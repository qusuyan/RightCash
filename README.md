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

Status: milestone 0 done (design §13). The benchmark suite, the in-memory
model engine and the vanilla firewood backend are implemented and tested;
the auth_wal core (write set, policies, schemes, proposal tree, speculation)
is a skeleton whose bodies are `todo!()`. Design v19 (§14) makes every
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
configuration is writer-owned and the closing flag doubles as the writer's
closed flag. Design v21 (R22) keeps only what consensus and the interface
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
behind one lock. The registry mutex, the worker's queue mutex and the firewood
handle lock are the only locks. Thirty-seven review rounds (rounds 30–35 on
R22–R24, 36–37 on R25; round 36 kept the target identity apart from the
consumed working policy). Design v25 (R26) keeps the policy in the
snapshot and lets hints mark it in place, drops warm jobs and the working
copy, and calls the speculative proposals steps. Design v26 (R27) makes the
worker the owner of the speculative side of the engine: `submit` a batch,
`finish` a block, `reset` at a retarget; a step is the merged write set of
everything queued. Design v27 (R28) puts the worker on a `std::sync::mpsc`
channel with `Submit`, `Reset` and `Finish` requests; the chain state is the
thread's own. v25–v27 await review. Next step is milestone 1. The
retraction of a speculative eviction (the final proposal puts back the
value read through the parent's pinned view, design §9.1) is kept as is by
decision; a firewood-side restore op is the recorded future change (design
§12.10 (e), `docs/TODO.md`). Next step is milestone 1.

## Benchmarks

```bash
# Build in release mode (firewood is a path dependency; first build is slow).
cargo build --release -p auth_wal-benchmark

B=target/release/auth_wal-benchmark
# Vanilla firewood baseline: create a 10M-row database, then run the other
# workloads on it (create truncates; the others reopen what create left).
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 create
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 200 --warmup-batches 20 zipf
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 200 ten-k-random
$B --baseline --engine firewood --dbname /tmp/fwbench -b 1000 --iterations 500 single
# The in-memory firewood model needs no path.
$B --baseline --engine memory -b 10000 -n 100 create
# Emulate libevm prefetch hints (per-transaction chunks before each propose).
$B --baseline --engine firewood --dbname /tmp/fwbench -b 10000 -n 1000 --iterations 50 --hints zipf
```

Each run prints a CSV header and one record to stdout (progress to stderr),
so sweeps can be concatenated and plotted. Defaults follow requirement R16:
a 10k-entry write set that evicts down to half; typical blocks write under 2k
entries, so sweeps also run `-b 2000`. Without `--baseline` the workload
runs through auth_wal, which lands with milestone 1. `--help` lists every
knob (capacity, policy, scheme, speculation rounds/margin, hints, synthetic
storage latency, seed).

```bash
cargo check --workspace --all-features            # lib + benchmark
cargo check -p auth_wal --features firewood        # firewood backend (path dependency on ../firewood); -p: the benchmark enables it
cargo test  --workspace --all-features             # lattice + engine contract tests (memory and firewood)
cargo clippy --workspace --all-features --all-targets
cargo doc -p auth_wal --no-deps --all-features
cargo fmt --all -- --check                        # rustfmt.toml: max_width = 120
cargo build --release                             # target/release/libauth_wal.a for the Go shim
cargo bench -p auth_wal --bench lattice           # micro-benchmarks: accumulator, per-proposal copy, sync primitives, engine propose
```
