# right_cash: usage and development notes

The details that used to live in the top-level `README.md`: how the
speculative path, the initial set and checkpointing behave, and how to run
the benchmarks and the tests. The design (`01-design.md`) is the reference
for the reasoning behind them.

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

`RightCash::new(config, storage, set)` consumes the complete
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
writes in statistics. Use `RightCash::recover` to restore a prior database and
its historical policy ordering.

## Clean-shutdown persistence and recovery

Set `config.local.snapshot_path = Some(path)` to enable a checkpoint on
`RightCash::close()`. The file contains the **latest committed** set, including
tombstones, policy ages, height, capacity, metadata hash, and the corresponding
storage and combined roots. FIFO, LRU-write, and all four commitment schemes
support recovery. Checkpointing is disabled by default; the benchmarks retain
their existing behavior.

On startup, reopen Firewood without truncation and call
`RightCash::recover(config, storage, vc, policy)`. Supply matching consensus
parameters; local settings such as the high watermark and speculation settings
may change. For example, with the `firewood` feature:

```rust
use right_cash::{RightCash, Config, Consensus, Local};
use right_cash::policy::LruWrite;
use right_cash::set::Snapshot;
use right_cash::storage::{FirewoodConfig, FirewoodStorage};
use right_cash::vc::rehash::Rehash;

let snapshot_path = std::path::PathBuf::from("data/right-cash.snapshot");
let config = Config {
    consensus: Consensus { capacity: 10_000 },
    local: Local { snapshot_path: Some(snapshot_path.clone()), ..Local::default() },
};
std::fs::create_dir_all("data")?;
let engine = FirewoodStorage::new(FirewoodConfig::new("data/firewood"))?;
let policy = LruWrite::new(7_000).with_trigger(9_000);
let mut cache: RightCash<Vec<u8>, Vec<u8>, _, _, _> = if snapshot_path.try_exists()? {
    RightCash::recover(config, engine, Rehash, policy)?
} else {
    RightCash::new(config, engine, Snapshot::empty(Rehash, policy, 10_000)?)?
};
// Propose and commit blocks normally.
cache.close()?;
drop(cache);
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
# Build in release mode (firewood is a git dependency; the first build is slow).
cargo build --release -p right_cash-benchmark

B=target/release/right_cash-benchmark
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
runs through right_cash. `--help` lists every knob (capacity, policy, scheme,
speculation rounds/margin, hints and hint noise, synthetic storage latency,
seed, the per-phase breakdown).

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

### Experiment runner and plots

`scripts/run_experiments.py` runs the four experiment matrices one trial at
a time (Zipf exponent sweep; LRU-write vs FIFO and rehash vs merkle-blake3
on `tenkrandom` and `zipf` with precomputation, plus lthash and the sparse
merkle on `tenkrandom`; bare firewood vs right_cash vs right_cash with
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
1.94 or newer (`cargo +1.94 build --release -p right_cash-benchmark`).

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

The README's performance table comes from one more experiment, `readme`,
which the default run leaves out. It runs bare firewood and right_cash with
LRU-write and merkle-blake3 (the commitment experiment's configuration) on
`zipf` and `tenkrandom`. It uses the same rows and hints, but one random
seed per run (shared by the four trials, so both targets see the same
workload), 20 warm-up + 200 measured blocks, and the default 128 revisions rather than
the commitment experiment's 8. With 8, right_cash's Firewood window (8 × 30
speculation rounds) fills around block 240, and from then on `tenkrandom`'s
propose p99 is 40–70 ms instead of 7 ms, nearly all of it reads. With 128,
every 100-block window of a full run gives the same mean and p99, so 200
blocks suffice. The four trials take about four minutes, most of it the
1M-row prefill:

```bash
scripts/run_experiments.py --only readme --out /tmp/readme-bench
scripts/readme_benchmark.py /tmp/readme-bench/readme/results.csv --readme README.md --host "<machine>"
```

The runner prints the seed it drew, and the table states it. Pass `--seed N`
to rerun a given run, for example a CI run. Use a fresh `--out`: the runner
skips trials already recorded there, and an interrupted run resumes with the
seed it recorded. The table script refuses a configuration that lacks the
others' seed.
The [Benchmark workflow](../.github/workflows/benchmark.yml) runs both on
every push to `main` that changes the code (or by hand, from the Actions
tab). It keeps the CSVs and trial logs as a run artifact and commits the
refreshed table to `README.md` as `github-actions[bot]`, so pull before your
next push to `main`. It leaves the README alone if `main` has newer code by
then; the run for that code updates it.

## Tests and checks

Library tests are grouped under [`src/tests/`](../src/tests/mod.rs): `cache`,
`policy`, `speculation`, `faults`, `persist`, `storage`, and `encodings`.
Shared fixtures live in `src/tests/common.rs`. These are unit tests because
they also inspect crate-private snapshots, storage views, and worker state;
tests of private implementation helpers stay inline beside those helpers.
The README's example runs as a doctest when the `firewood` feature is on.
The [Tests workflow](../.github/workflows/tests.yml) runs `cargo test` with
all features, the default features, and no default features on every push
to `main` and every pull request.

```bash
# Compile all library tests, without running the suite.
cargo test -p right_cash --lib --no-default-features --no-run
cargo test -p right_cash --lib --all-features --no-run
# Run just persistence/recovery tests, including the private file-write test.
cargo test -p right_cash --lib --features firewood checkpoint_

cargo check --workspace --all-features            # lib + benchmark
cargo check -p right_cash --features firewood      # firewood backend; -p: the benchmark enables it
cargo test  --workspace --all-features             # the whole battery (firewood contract and smoke tests included)
cargo clippy --workspace --all-features --all-targets
cargo doc -p right_cash --no-deps --all-features
cargo fmt --all -- --check                        # rustfmt.toml: max_width = 120
cargo build --release --workspace                 # the benchmark binary; the staticlib for the Go shim arrives with milestone 4
```
