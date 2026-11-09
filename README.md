# auth_wal

An authenticated, bounded, in-memory set of recent writes that sits between the
EVM validators (`Avax-pEVM`, `libevm`) and an authenticated storage engine
(`qmdb-ffi` or `firewood`), exposing the same Go `ffi` interface so validators
need no code changes.

- `docs/00-project-description.md`: the requirements as stated.
- `docs/01-design.md`: the design (write set, eviction policy, vector
  commitment, proposal tree, speculative eviction, benchmarks).
- `docs/TODO.md`: deferred work, including the firewood-side changes that
  would remove the per-step commit and rehashing costs.

The benchmark defaults to capacity **10,000**, low watermark **7,000 (70%)**,
and high watermark **9,000 (90%)**. The high watermark starts precomputation;
actual eviction waits until capacity is exceeded and drains to the low watermark.
Prediction starts with a fixed **3,000-key window** (capacity minus low, plus
any explicit margin). Later insertion hints do not enlarge it: FIFO ignores
later hints, while LRU replaces hinted predicted keys with the next eligible
candidates. Actual victims outside the window are handled at finalization.
Percentages scale with `--capacity` and round down; explicit watermark flags override them.
For Zipf, mixed-write, and single-key workloads, measurement requires set
occupancy at least the low watermark after prefill/warm-up. Reports include
`start_occupancy`; insufficient preparation fails with a configuration error.
The `create` workload intentionally measures growth from an empty database.

Status: milestones 0–2 done and reviewed through 2026-11-03 (design §13, §14;
test coverage includes the policies, the set and its commitment
oracles, the three schemes, the proposal tree and its lineage rules, the
revision window, differential tests against a reference map and against
the plain path, the engine contract on both backends, firewood smoke
tests). The benchmark runs through auth_wal (`WriteSet`). Before it,
milestone 0: the benchmark suite, the in-memory model engine and the
vanilla firewood backend. First release-mode numbers (design §14, milestone
1): on firewood's `zipf` auth_wal cuts storage ops per commit from 1000 to
100 but its own set work costs about 8 ms per 1000-op batch at 10k entries,
so throughput halves on a 50k-row database. On a populated trie
(clarification 28, `--prefill`) the picture inverts: with the set at 10k,
auth_wal is ahead of bare firewood from 100k rows on, by a third at 1M rows
on `zipf`, because firewood's per-batch cost grows with the trie and the
set's does not (design §12.13). Growing the set itself is another matter:
at capacity 100k the per-proposal copy costs about 28 ms per block and 3.4
GB for 128 revisions, so a larger set would need a different structure
(design §12.13, §12.15). Milestone 2 (design §9, §14) turns speculation on:
hints precompute a full eviction batch at a local high watermark, and
predictions outlive the block. Actual eviction only occurs above capacity
and drains to the low watermark; high watermark never enters the root.
A fuzz test shows hints never change the eviction. The following historical
measurements predate this watermark correction (design §6.2, R39). Against hinted blocks on a
2.4M-row firewood the final proposal shrinks from 330–2120 storage ops to
0–52 and the propose p99 from 115 to 18 ms. The per-phase latency
breakdown (clarification 31, `--breakdown`, design §12.16 and §14) then
shows where propose, the critical path, really goes: the set's own work
(copy, lattice updates, eviction, policy digest) is 8.4–10.5 ms per block
on `zipf` and 17.8–19.4 ms on `tenkrandom`, more than bare firewood's whole
propose (7.1 and 15.9 ms) in every configuration; the engine side is cold
puts of the evicted keys (17–22 µs each against 9 µs warm), which
speculation moves off the path entirely once a step is built per hint
(rounds 30, min_step 1: propose 10.2 ms on `zipf` and 20.7 ms on
`tenkrandom`, all of it the set's own work); the chain's 14–75 ms commit
is firewood's persistence backpressure (one unpersisted revision by
default), off the critical path. The whole-set `rehash` commitment scheme
(clarification 32, `--vc rehash`, design §7.2) then replaces the lattice's
2.7–2.8 µs per item with one hash over the set, 0.7 ms per block: propose
falls to 6.9 ms on `zipf` and 9.1 ms on `tenkrandom`, at or under bare
firewood's. Clarification 33 then applied the proposals one step at a
time (design §14): the policy digest from one buffer, the flat snapshot
(sorted arrays and a value arena rebuilt by merge passes instead of B-tree
copies) and the flat policy arrays take propose to 2.2 ms on `zipf` and
3.1 ms on `tenkrandom`, a third and a fifth of bare firewood's; a block
that evicts nothing now keeps the precomputed chain and the previous
block's storage root (R35, exercised with `--hint-noise`), which
halved the mean propose under the bulk rule with noisy hints on `zipf`, 16.4 to 9.0 ms (the 100 non-evicting blocks of 200 kept the chain and paid the set's work alone, about 2.5 ms, while the evicting half still pays about 15 ms of read-backs and reverts for the over-prediction itself, which only firewood-side changes remove, §12.10 (e)–(f)), and left `tenkrandom` unchanged, where every block evicts under either rule. The levers left are listed in `docs/TODO.md`. Design v19 (§14) makes every
proposal a full copy of its parent's set (R20): a node is its snapshot, its
pinned storage view and one weak parent link; there are no overlays, no chain
reads and no replay-through-chain policy queries, and the redundancy pass
that followed (design §14 v19, round 26) removed the link budget, the proof
stubs, the capacity hook, the engine's revision type, the Rust-side handle
count and a dozen smaller structures. Design v20 (R21) strips the derived
state from nodes and the registry: a node's storage ownership is handle
identity and statistics are atomics counted at `Propose`; the registry is one
deque of committed nodes in commit order (its newest entry is the head) and a
flat live index for `Revision(root)` on live proposals, nothing else; the
configuration and the closed flag are writer-owned. Design v21 (R22) keeps only what consensus and the interface
require: no lineage walk (`Commit` checks that the parent is the head,
`Propose` builds on the head for an equal root and otherwise on the node as
given, as firewood does), no ring clearing at close, no prefix-delete
support in the set (rejected as QMDB rejects it), and the policy state
carried in the set commitment's items instead of a second accumulator (its
review showed the adapter stages children by root, so equal roots must be
interchangeable parents). Design v22 (R23) makes the capacity a field of
the snapshot and how much to evict the policy's own rule (a low watermark is
the v0 policies' private parameter; the trait knows none), and drops the
chain-carrier test. Design v23 (R24) replaces the per-node chains of link
slots by one precomputation: the validator executes one block at a time, so
the worker builds every link on its latest view in queue order, nothing is
cancelled, and `Propose` waits for the queued links (R17 amended for
`Propose` alone). Design v24 (R25) moves the prediction into the eviction
policy: `writes` evicts and reconciles the policy's own marks, `hint_writes`
says what to send early and what to put back, `state_digest` makes the
policy state a root term again (a full hash per proposal), and the
speculation machinery shrinks to a pending queue, a worker and a flush
behind one lock. Thirty-seven review rounds (rounds 30–35 on
R22–R24, 36–37 on R25; round 36 kept the target identity apart from the
consumed working policy). Design v25 (R26) keeps the policy in the
snapshot and lets hints mark it in place, drops warm jobs and the working
copy, and calls the speculative proposals steps. Design v26 (R27) makes the
worker the owner of the speculative side of the engine: `submit` a batch,
`finish` a block, `reset` at a retarget; a step is the merged write set of
everything queued. Design v27 (R28) puts the worker on a `std::sync::mpsc`
channel; the current terminal `Finish` also carries final reconciliation
(see below). The chain state is the
thread's own. The milestone-1 and milestone-2 implementations were
reviewed on 2026-11-02 (clarification 34, design §14): a consensus hazard
in R35's chain handover was fixed (the child that keeps the chain becomes
the precomputation's target), the chain capped at `rounds` steps across
kept blocks, and the experiments' leftovers removed; round 2 made `finish`
answer nothing while submitted ops wait unbuilt on a full chain and the
firewood adapter's first link hold the committed handle that pins its base
revision, and pinned the consensus encodings with known answers; round 3
made both engines compare a chain's base with the committed state by
revision identity, never by root (firewood would free a node twice
otherwise). The
retraction of a speculative eviction (the final proposal puts back the
value read through the parent's pinned view, design §9.1) is kept as is by
decision; a firewood-side restore op is the recorded future change (design
§12.10 (e), `docs/TODO.md`). Next step is milestone 3 (the benchmark
sweep).

## Speculative proposal flow

`Propose` sends the final eviction refinement to the worker in one terminal
request. The worker merges it with pending hints before building at most one
final storage proposal. If its latest view is already exact and there is no
metadata, it returns that view directly. The validator does not build another
correction proposal. Retractions still read the chain's original storage view.

`--speculation-rounds` caps the complete chain, including its final proposal;
early work uses at most `rounds - 1` slots. The engine revision window is
`revisions × rounds` when the backend commits chain links separately. A
non-evicting block keeps the chain asynchronously. An evicting block consumes
it and starts the next prediction on the completed child, so competing
siblings cannot reuse child-specific corrections.

The library and benchmark default `speculation_min_step` to 256 distinct
pending keys. `--speculation-min-step` overrides it; block-end flushes and
finalization can build below this threshold.

FIFO refreshes rewritten predictions when keeping a chain. At margin zero,
its fixed oldest prefix needs no final retractions; nonzero margins can
still require restoration. LRU restores withdrawn predictions and uses a
forward candidate cursor, reset when writes rebuild its order. Its fixed
budget survives kept blocks; false hints are corrected at finalization.

Firewood's existing commit-permit setting is exposed as
`--firewood-commit-permits`. Benchmark defaults allow one complete speculative
chain (`--speculation-rounds`) to await persistence, or one revision for plain
and baseline runs. The value must be positive and smaller than the engine
revision window; CSV reports the resolved `firewood_commit_permits`. Direct
`FirewoodConfig` retains the backend default of one permit. Firewood's commit
behavior is unchanged; this configures its existing persistence queue.

The adapter supports upstream Firewood v0.9.0 through its public dynamic API.
`ethhash` selects Ethereum mode at database open; the default is MerkleDB.
The permit limit maps to `max_persistence_gap`. Firewood can absorb a no-op
commit, so adapter generations preserve the rule that each successful commit
supersedes the previous head. The API requires owned key/value batches.

## Initial set

`AuthWal::new(config, storage, set)` consumes the complete
`set::Snapshot<K, V, C, P>`: entries, policy state, commitment scheme and
capacity. It derives root parameters from that set, and rejects a capacity
mismatch with `config`. Storage must still be fresh.

Use `Snapshot::empty(vc, policy, capacity)?` for the existing empty-genesis
behavior, or `Snapshot::from_entries(vc, policy, capacity, entries)?` for a
populated initial set. Entries are `(key, Entry::Value(value))` or
`(key, Entry::Tombstone)`; repeated keys use the last entry. Supply a fresh
policy to the builder, which fills its key space through the normal write path.
The resulting set exposes no independent policy mutation.

A populated initial set is equivalent to one initial batch: height 1 and a
content-dependent root, with all initial entries retained in memory. An empty
set remains height 0 with `EMPTY_ROOT`. Initial entries do not count as runtime
writes in statistics. Use `AuthWal::recover` to restore a prior database and
its historical policy ordering.

## Clean-shutdown persistence and recovery

Set `config.local.snapshot_path = Some(path)` to enable a checkpoint on
`AuthWal::close()`. The file contains the **latest committed** set, including
tombstones, policy ages, height, capacity, metadata hash, and the corresponding
storage and combined roots. FIFO, LRU-write, and all three commitment schemes
support recovery. Checkpointing is disabled by default; the benchmarks retain
their existing behavior.

On startup, reopen Firewood without truncation and call
`AuthWal::recover(config, storage, vc, policy)`. Supply matching consensus
parameters; local settings such as the high watermark and speculation settings
may change. For example, with the `firewood` feature:

```rust
use auth_wal::{AuthWal, Config, Consensus, Local};
use auth_wal::policy::LruWrite;
use auth_wal::set::Snapshot;
use auth_wal::storage::{FirewoodConfig, FirewoodStorage};
use auth_wal::vc::rehash::Rehash;

let snapshot_path = std::path::PathBuf::from("data/auth-wal.snapshot");
let config = Config {
    consensus: Consensus { capacity: 10_000 },
    local: Local { snapshot_path: Some(snapshot_path.clone()), ..Local::default() },
};
std::fs::create_dir_all("data")?;
let engine = FirewoodStorage::new(FirewoodConfig::new("data/firewood"))?;
let policy = LruWrite::new(7_000).with_trigger(9_000);
let mut wal: AuthWal<Vec<u8>, Vec<u8>, _, _, _> = if snapshot_path.try_exists()? {
    AuthWal::recover(config, engine, Rehash, policy)?
} else {
    AuthWal::new(config, engine, Snapshot::empty(Rehash, policy, 10_000)?)?
};
// Propose and commit blocks normally.
wal.close()?;
drop(wal);
```

Close joins the worker, writes and syncs a temporary snapshot file, closes and
flushes storage, then atomically replaces the snapshot and syncs its directory
on Unix. I/O and outstanding-proposal errors are reported and may be retried;
a failed storage flush prevents publication. `new` refuses an existing snapshot
file. Recovery checks the file version, checksum, key/value decoding, policy
state, configuration, storage root, and reconstructed combined root.

Only the latest committed revision is recovered. Uncommitted proposals,
speculative marks, retained revision history, and counters are not saved.
Drop the old writer and all its handles/readers before reopening Firewood in
the same process, since pinned views can keep its file lock alive.
`MemoryStorage` does not persist evicted data; a set checkpoint alone cannot
restore a nonempty memory engine.

This supports recovery after a **successful close**. Commit and dropping the
writer do not checkpoint the set. Crash recovery, rollback to an older storage
revision, and state sync remain future work; a checkpoint/storage-root mismatch
is an error, never an implicit reset.

## Benchmarks

```bash
# Build in release mode (firewood is a path dependency; first build is slow).
cargo build --release -p auth_wal-benchmark

B=target/release/auth_wal-benchmark
# Vanilla firewood baseline: create a 10M-row database, then run the other
# workloads on it (create truncates; the others reopen what create left).
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 create
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 200 --warmup-batches 20 zipf
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 200 tenkrandom
$B --baseline --engine firewood --dbname /tmp/fwbench -b 1000 --iterations 500 single
# The in-memory firewood model needs no path.
$B --baseline --engine memory -b 10000 -n 100 create
# Emulate libevm prefetch hints (per-transaction chunks before each propose).
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 50 --hints zipf
```

Each run prints a CSV header and one record to stdout (progress to stderr),
so sweeps can be concatenated and plotted. By default each invocation chooses
a random seed, printed to stderr at startup and included in the CSV `seed`
column. Pass `--seed <reported-seed>` to reproduce the workload, hint gaps and
hint noise with the same configuration. Defaults follow requirement R16:
a 10k-entry write set that evicts down to 7,000 entries; typical blocks write under 2k
entries, so sweeps also run `-b 2000`. Without `--baseline` the workload
runs through auth_wal. `--help` lists every knob (capacity, policy, scheme,
speculation rounds/margin, hints and hint noise, synthetic storage latency,
seed, the per-phase breakdown).

### Experiment runner and plots

`scripts/run_experiments.py` runs the four experiment matrices one trial at
a time (Zipf exponent sweep; LRU-write vs FIFO and rehash vs merkle-blake3
on `tenkrandom` and `zipf` with precomputation, plus lthash and the sparse
merkle on `tenkrandom`; bare firewood vs auth_wal vs auth_wal with
precomputation on `zipf` and `tenkrandom`). Every trial uses 1M prefilled
rows, 2400-op blocks hinted as 30 × 80 keys with 1–5 ms gaps, 50 warm-up and
500 measured blocks, and the seeds 101, 202 and 303, shared across targets
and interleaved (seed 101 of every configuration, then 202, then 303), so
drift in the host's speed spreads over all configurations. The commitment
experiment retains 8 revisions for all schemes, and merkle measures 100
blocks, because the sparse Merkle reference scheme needs far more than 15 GB
at 128 revisions and proposes in about 2 s per block. It writes
`results.csv`, `breakdown.csv` and per-trial logs under
`benchmark/results/2026-11-07-interleaved/<experiment>/`, and skips trials
already recorded, so an interrupted run resumes. Experiments that run on
several workloads prefix their trial ids with the workload
(`zipf-lru-write-101`), so a workload added later runs into the same
directory and lands in the same figures. Firewood v0.9.0 needs rustc
1.94 or newer (`cargo +1.94 build --release -p auth_wal-benchmark`).

A fifth experiment, `--only watermarks watermarks_capacity` (run with `--out
benchmark/results/2026-11-07-watermarks`), varies capacity, high watermark
and low watermark on `tenkrandom` with precomputation, around capacity
10,000, high 7,900 and low 3,701. A block adds W = 2,100 new entries, so
capacity − high = W, high − low = 2W − 1 and capacity − low = 3W − 1:
eviction is strictly above capacity, so every third block evicts 3W with all
but one victim inside the fixed capacity − low prediction. `watermarks`
varies the high and the low watermark one at a time with the other two
fixed; `watermarks_capacity` varies capacity with both watermarks shifted
along, so the two gaps stay fixed and only the set size changes. These runs
pass `--blocks-csv`, which writes one row per measured block (latencies,
evicted keys, final storage ops, steps, phase times) for per-block analysis.
`plot_watermarks.py` writes one figure per knob
(`watermarks_{capacity,low_watermark,trigger}.png`), propose mean and p99
side by side against bare firewood.

`scripts/plot/plot_{zipf_sweep,policy,vc,baseline,watermarks}.py`
(matplotlib, pandas) read those directories and write PNG/PDF figures plus a
summary CSV to `<results>/plots/`; `--results` and `--out` override the paths.
The figures show propose latency (mean and p99), one panel per workload;
prefetch latency is in the summary CSVs. `--blog` renders the dark,
captionless PNGs for the blog post.

## Tests and checks

Library tests are grouped under [`src/tests/`](src/tests/mod.rs): `wal`,
`policy`, `speculation`, `faults`, `persist`, `storage`, and `encodings`.
Shared fixtures live in `src/tests/common.rs`. These are unit tests because
they also inspect crate-private snapshots, storage views, and worker state;
tests of private implementation helpers stay inline beside those helpers.

```bash
# Compile all library tests, without running the suite.
cargo test -p auth_wal --lib --no-default-features --no-run
cargo test -p auth_wal --lib --all-features --no-run
# Run just persistence/recovery tests, including the private file-write test.
cargo test -p auth_wal --lib --features firewood checkpoint_

cargo check --workspace --all-features            # lib + benchmark
cargo check -p auth_wal --features firewood        # firewood backend (path dependency on ../firewood); -p: the benchmark enables it
cargo test  --workspace --all-features             # the whole battery (firewood contract and smoke tests included)
cargo clippy --workspace --all-features --all-targets
cargo doc -p auth_wal --no-deps --all-features
cargo fmt --all -- --check                        # rustfmt.toml: max_width = 120
cargo build --release --workspace                 # the benchmark binary; the staticlib for the Go shim arrives with milestone 4
```
