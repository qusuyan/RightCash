# right_cash

[![Tests](https://github.com/qusuyan/auth_wal/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/qusuyan/auth_wal/actions/workflows/tests.yml)
[![Benchmark](https://github.com/qusuyan/auth_wal/actions/workflows/benchmark.yml/badge.svg?branch=main)](https://github.com/qusuyan/auth_wal/actions/workflows/benchmark.yml)

RightCash is an authenticated, bounded, in-memory set of recent writes that
sits between EVM validators and an authenticated storage engine such as
Firewood. A validator can opt in without modifying its underlying MPT
database (or any other storage engine that keeps entries sorted by key) and
without migrating its data to a different storage engine. 

- **Writes land in the set first.** A write to a key already in the set
  updates it in place, and reads check the set before the engine, so
  frequently rewritten keys stay in memory instead of reaching the engine
  every block.
- **Bounded by eviction.** Once the set exceeds its capacity, a pluggable
  eviction policy (LRU-write or FIFO) evicts entries down to a low watermark
  and sends them to the engine in storage proposals that commit with the
  block.
- **Authenticated.** A pluggable vector commitment (merkle-blake3, rehash,
  lattice hash or a sparse Merkle tree) covers the set. Each block's root
  combines it with the policy state, the engine's storage root, the height
  and the consensus parameters.
- **Revertible.** Every proposal is an immutable copy of its parent's set,
  so competing blocks can be proposed side by side; committing one makes it
  the head, and recent committed revisions stay readable.
- **Precomputed eviction.** With speculation on, prefetch hints let a
  background worker predict the victims and build their storage proposals
  ahead of time, which takes engine work off the propose path.
- **Clean-shutdown recovery.** The committed set can be checkpointed on
  close and recovered with the matching Firewood database.

See [here](https://www.qusuyan.com/byzantine%20fault%20tolerance/storage%20systems/2026/11/06/right-cash.html)
for my blog post. 

## Usage

Add the crate with the Firewood backend (rustc 1.94 or newer); the example
below also uses `tempfile` for its database directory:

```toml
right_cash = { git = "https://github.com/qusuyan/auth_wal", features = ["firewood"] }
tempfile = "3"
```

Open a `RightCash` in front of a fresh Firewood database, then hint, propose
and commit each block:

```rust
use right_cash::policy::LruWrite;
use right_cash::set::Snapshot;
use right_cash::storage::{FirewoodConfig, FirewoodStorage};
use right_cash::types::BatchOp;
use right_cash::vc::merkle_blake3::MerkleBlake3;
use right_cash::{Config, Consensus, Local, RightCash};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?; // any directory for the Firewood database

    // Capacity is a consensus parameter: it enters the root and must match on
    // every validator. `Local` settings may differ; speculation precomputes
    // evictions from prefetch hints.
    let config = Config {
        consensus: Consensus { capacity: 10_000 },
        local: Local { speculate: true, ..Local::default() },
    };

    // Firewood commits each speculative step as its own revision
    // (`COMMITS_CHAIN_LINKS_SEPARATELY`, hence `true`), so it must keep
    // `revisions × speculation_rounds` of them.
    let mut firewood = FirewoodConfig::new(dir.path().join("firewood"));
    firewood.revisions = config.local.engine_window(true);
    let storage = FirewoodStorage::new(firewood)?;

    // Above 10,000 entries, evict the least recently written down to 7,000.
    // The scheme, the policy and its low watermark enter the root too, so
    // they are consensus; the 9,000 trigger is local: it starts predicting
    // the victims once a block would reach it.
    let policy = LruWrite::new(7_000).with_trigger(9_000);
    let set = Snapshot::empty(MerkleBlake3::default(), policy, config.consensus.capacity)?;
    let mut cache: RightCash<Vec<u8>, Vec<u8>, _, _, _> = RightCash::new(config, storage, set)?;

    // One block on the committed head (`None`): hint the keys execution
    // writes, propose the block, and commit it once consensus accepts it.
    let key = b"account".to_vec();
    cache.prefetch(None, &[key.clone()]);
    let block = vec![BatchOp::Put { key: key.clone(), value: b"42".to_vec() }];
    let proposal = cache.propose(None, block, None)?;
    let root = proposal.root(); // the authenticated root of the new state
    assert_eq!(cache.commit(proposal)?, root);
    assert_eq!(cache.reader().get(&key)?, Some(b"42".to_vec()));

    cache.close()?;
    Ok(())
}
```

## Performance

Bare Firewood against RightCash in front of the same Firewood, with the
LRU-write policy, the merkle-blake3 commitment and precomputation on. `zipf`
updates Zipf-distributed keys; `tenkrandom` mixes a quarter inserts, a
quarter deletes and half updates over a moving window. Propose builds a
block's proposal (the critical path). Prefetch is one hint call: Firewood
warms the keys' trie paths, RightCash predicts evictions. Commit makes the
proposal the head; for RightCash that includes committing its storage
proposals to Firewood. The
[Benchmark workflow](.github/workflows/benchmark.yml) reruns this table on
every push to `main` that changes the code, with one short trial per row on
a fresh random seed, stated below the table;
[`docs/02-usage.md`](docs/02-usage.md#experiment-runner-and-plots) shows how
to run it locally.

<!-- BENCHMARK-TABLE:START -->
| Workload | Target | Propose mean | Propose p99 | Prefetch mean | Prefetch p99 | Commit mean | Commit p99 |
|---|---|--:|--:|--:|--:|--:|--:|
| `zipf` | Firewood | 5.93 ms | 14.4 ms | 264 µs | 813 µs | 213 µs | 729 µs |
| `zipf` | RightCash | 1.77 ms | 9.03 ms | 70 µs | 613 µs | 151 µs | 1.26 ms |
| `tenkrandom` | Firewood | 11.7 ms | 24.8 ms | 611 µs | 4.40 ms | 274 µs | 528 µs |
| `tenkrandom` | RightCash | 3.31 ms | 9.76 ms | 105 µs | 1.83 ms | 320 µs | 945 µs |

Seed 1582617319, the same for every row (rerun with `scripts/run_experiments.py --only readme --seed 1582617319`). 1,000,800 prefilled rows in firewood (`firewood/merkledb`); 2,400-op blocks, each hinted as 30 prefetch calls 1–5 ms apart; 20 warm-up and 200 measured blocks; 128 committed revisions kept; Zipf exponent 1.2. RightCash: capacity 10,000, high watermark 7,900, low watermark 3,701; merkle branching 16; 30 speculation rounds, so its Firewood keeps 3,840 revisions for the speculative steps.
Measured on 2026-11-15 at commit `4a229ee` on a local workstation (12th Gen Intel Core i7-12700KF, 20 threads, WSL2).
<!-- BENCHMARK-TABLE:END -->

## Documentation

- [`docs/00-project-description.md`](docs/00-project-description.md): the requirements.
- [`docs/01-design.md`](docs/01-design.md): the design (write set, eviction
  policy, vector commitment, proposal tree, speculative eviction, benchmarks).
- [`docs/02-usage.md`](docs/02-usage.md): the speculative path, initial sets
  and checkpoint recovery in detail, and how to run the benchmarks and tests.
- [`docs/TODO.md`](docs/TODO.md): progress so far and deferred work,
  including the Firewood-side changes that would remove the per-step commit
  and rehashing costs.
