# right_cash TODO

Progress so far, then the items deliberately deferred. Design references are
to `01-design.md`.

## Progress

Where the project stands (moved here from the README on 2026-11-15).

Status: milestones 0–2 done and reviewed through 2026-11-03 (design §13, §14;
test coverage includes the policies, the set and its commitment
oracles, the three schemes, the proposal tree and its lineage rules, the
revision window, differential tests against a reference map and against
the plain path, the engine contract on both backends, firewood smoke
tests). The benchmark runs through right_cash (`WriteSet`). Before it,
milestone 0: the benchmark suite, the in-memory model engine and the
vanilla firewood backend. First release-mode numbers (design §14, milestone
1): on firewood's `zipf` right_cash cuts storage ops per commit from 1000 to
100 but its own set work costs about 8 ms per 1000-op batch at 10k entries,
so throughput halves on a 50k-row database. On a populated trie
(clarification 28, `--prefill`) the picture inverts: with the set at 10k,
right_cash is ahead of bare firewood from 100k rows on, by a third at 1M rows
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
halved the mean propose under the bulk rule with noisy hints on `zipf`, 16.4 to 9.0 ms (the 100 non-evicting blocks of 200 kept the chain and paid the set's work alone, about 2.5 ms, while the evicting half still pays about 15 ms of read-backs and reverts for the over-prediction itself, which only firewood-side changes remove, §12.10 (e)–(f)), and left `tenkrandom` unchanged, where every block evicts under either rule. The levers left are listed below. Design v19 (§14) makes every
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
(see `02-usage.md`, "Speculative proposal flow"). The chain state is the
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
§12.10 (e), below). Next step is milestone 3 (the benchmark
sweep).

## Firewood-side changes (R15, design §12.10)

Confirmed against firewood's source: proposals can be built on uncommitted
proposals (`Proposal::propose` → `propose_with_parent`), but `manager.commit`
rejects a child whose parent is still a proposal (the parent-identity check
fails, giving `NotLatest`), and neither the
FFI nor the Go wrapper walks ancestors. Consequences accepted for now:

- one engine commit per speculative step, with a physical revision for each
  state-changing step (v0.9.0 absorbs no-op commits), so the engine
  window is `K × rounds` and a mid-chain commit failure poisons the
  instance (design §8, §9.2);
- intermediate trie hashes recomputed once per step that touches a path and
  once more when the final proposal reverts a key a step wrote; mitigated by
  pessimistic prediction (`speculation_margin`, design §9.2).

Future changes, in order of value:

1. **Cascade commit**: `commit` walks `Proposed` ancestors and commits them
   oldest-first inside the same critical section. Removes the caller-side
   ordering and the mid-chain failure window; still one revision per step.
2. **Squash commit**: merge a chain's node stores into one revision. Removes
   the window-sizing cost and the per-step revision records.
3. **Hashing shared across a chain**: defer or share path hashing so a path
   touched by several steps (or written then reverted) is hashed once.
4. **Native warm-path call** (no longer needed by right_cash's hints, R26;
   the benchmark baseline's read-through would use it) under
   `CacheReadStrategy::All` (design §9.4).
5. **Commit by reference** (`Proposal::commit(&self)`, or a handle that stays
   a readable view of its committed root): the adapter's per-handle
   read-write lock exists only because `commit(self)` consumes the proposal
   while the worker and readers may still hold the handle (design §10 (e));
   it would go with this change. Kept for now by decision (clarification 6,
   2026-10-27): it is the last lock on the storage path.
6. **Restore op** (`Restore(key)` in a proposal's batch): put back the
   value the proposal's base holds for `key`, or delete it, inside the
   engine. The final proposal's reverts (design §9.1) would carry it
   instead of a `Put(old)` read back through the parent's pinned view, so
   no caller-side read, no value copy across the FFI, and with item 3 the
   path is hashed once. The read-back revert is kept as is by decision
   (clarification 14, 2026-10-28, design §12.10 (e)); `put_back` in
   `speculate.rs`, used by the worker's `build_step` for both early
   withdrawals and final corrections, is the one place to change.
7. **A speculative proposal that survives a commit** (clarification 29,
   design §9.1, §12.10 (f)): rebase a proposal onto the new head, or let a
   commit keep a sibling proposal's hashed nodes reusable. The final
   proposal must put predicted-but-kept keys back (the committed root covers
   the storage root), so today only the prediction survives the block (R31:
   the kept marks are re-sent on the new base and their paths hashed again
   by the next chain's first step).

## right_cash

- The `ffi` feature, `src/ffi/` and the `staticlib` crate type arrive with
  milestone 4 (the stubs that stood for them were removed in the review of
  2026-11-02, design §13); the feature should also enable `firewood`,
  which design §3 names as `WithStorageEngine`'s default.
- The C ABI (milestone 4) keeps the writer behind a mutex and a `Reader`
  beside it (design §3 threading contract, R19): the Rust writer takes `&mut
  self`, which C cannot express; the Go package's `Proposal.Commit`,
  `Proposal.Propose` and every `Prefetch` become writer calls on the database
  handle, so the Go surface stays `qmdb-ffi`'s.
- Without the empty-root sentinel (R42, design §7.3, §12.6), right_cash
  never reports the Go `EmptyRoot`: the adapter port (milestone 4) needs its
  own `Initialized()` test. `Revision` of the last pre-right_cash block root
  (the engine's) already resolves to the initial node (R43), but the
  handle's `Root` is the initial node's combined root, not the root asked
  for; the port must not assume they are equal.

- **Warm-up ahead of the final root computation (clarification 31, R33).**
  `Propose` is the critical path, and its engine side is cold-path puts:
  the victims are the least recently written keys, so a put costs 17–22
  µs on `zipf` (13.5–15 on `tenkrandom`) against 9 (6.6) µs per hinted,
  warmed op in the baseline (design §12.16, §14 "Latency breakdown"). The
  steps warm the predicted victims' paths (R26: a step is the warming)
  and, with a step per hint (rounds 30, min_step 1), leave 0.1–0.7 ms on
  the path (design §14 follow-up), at the price of 17–55 ms of step
  hashing per block on the worker, an engine window of `K × 31` and a
  chain commit of 16–68 ms of persistence backpressure. Nothing warms the
  plain path's victims. Candidate to measure against the breakdown: a
  read-through warm of the predicted victims on the worker (the
  baseline's hint reads cost 5 µs per key off the path) followed by one
  warm final proposal and no chain, which removes the chain commit and
  the window; firewood's native warm-path call (firewood item 4) would
  make the warm cheaper.
- **The set's own work (clarifications 31–33, design §12.17, §14
  "Clarification 33").** After the steps of clarification 33 (the policy
  digest from one buffer, the flat entries with a value arena, the flat
  policy arrays) propose is 2.2 ms per block on `zipf` and 3.1 ms on
  `tenkrandom` against bare firewood's 7.1 and 15.9. What is left, by
  size: the policy's `writes` (0.5 / 1.3 ms: the written keys sorted and
  deduplicated, a binary search each, two merge passes, the marks
  reconciled; folding the index into the entries' height column, design
  §12.14, would save one pass), the rehash (0.4 ms, single-threaded: the
  rayon variant measured slower in situ; hashing without serialisation
  would need keys and values contiguous as items), the policy digest and
  the merge (0.25 ms each). `lthash` stays the default until the owner
  decides between the schemes (design §7.2: the lattice wins for a large
  set with small blocks).
- **Firewood's window under pinned bases (design §10).** A live first link
  pins the committed revision it was proposed on (the review of
  2026-11-02, round 2), so an orphan handle held for long stalls
  firewood's reaping at that revision: memory, not wrong data. The Go
  layer's keep-alive accounting (milestone 4) bounds it.
- **A kept chain's budget (R35, design §9.2).** The chain is capped at
  `rounds` steps however many blocks it spans, so after `rounds − 1` steps
  the predicted ops of further non-evicting blocks wait for the next tail,
  on the critical path of the evicting block, and a second block end on a
  chain the first child's tail filled takes the plain path (its `finish`
  answers nothing while ops wait unbuilt). If a workload shows long runs
  of non-evicting blocks with many predictions, or competing candidates
  after a tail, the alternative is to fold the chain into one step on its
  base when the budget is reached (one larger proposal on the worker, the
  merged ops kept in memory).
- **Pipeline depth (design §9.2).** `after_commit` and `prefetch` test the
  direct parent only: with blocks verified two or more ahead (head → C1 →
  C2 → C3, target C3), committing C1 resets the chain and hints on C3 are
  ignored until C2 commits. Correct, speculation lost for that depth; walk
  the parent chain (bounded by the live depth) if the adapter pipelines
  deeper than one block.
- **Firewood's persistence backpressure on a chain commit.** The adapter
  exposes `commit_permits`, mapped to upstream's `max_persistence_gap`.
  Speculative benchmarks default to one chain's worth; plain/baseline and
  direct adapter construction default to 1. The focused 32-permit trial
  reduced mean Commit below 0.22 ms but raised process peak RSS to about
  1,140 MiB, including prefill. Locate that peak and evaluate smaller permit
  limits with minimum step 256 (design §14).
- Milestones 3–5 as listed in design §13 (milestones 0–2 are done and
  were reviewed on 2026-11-02, design §14). Design §9.3 test 6 (the
  asynchrony timing assertions) is pending.
- **Left by the review of 2026-11-02 (design §14), the owner's call:**
  `fifo.rs` and `lru_write.rs` share about 110 lines (fields, builders,
  `eviction_count`, the mark plumbing, `validate`) that R26's "no shared
  watermark code" keeps apart; `Key: Borrow<[u8]>` exists only for the
  memory model's prefix-delete range scan and forces `::<K>` at the
  policies' set lookups (the bound could move to the memory engine);
  `Value::from_bytes` panics on a length mismatch for `[u8; N]` (an
  `Error::Storage` on the read path would be cleaner; the C ABI turns the
  panic into an error); the `Poisoned` path has no test because the
  memory engine commits a chain atomically (a fault-injection knob would
  model firewood's one-commit-per-step failure; the test engine of
  `tests::faults` injects propose and read failures, not mid-chain commit
  failures); `Prediction::refresh` and `withdraw` could be one list
  handled one way at the price design §9.1 weighed; `chains_kept` counts
  a kept block whose chain was already dead (a failed step leaves no
  latest view; the count says the block kept nothing).
- **Resolved upstream: parent identity (design §10 item 3).** Firewood
  v0.9.0 checks parent revision identity and absorbs same-root commits.
  The earlier hash-only check and double-free reproduction no longer
  describe upstream. Adapter generations retain right_cash's stricter rule
  that even a no-op commit supersedes the old head; contract tests cover it.
- **Set structure for a larger set (open, clarifications 27–28, design
  §12.13, §12.15).** At the default 10k the copy wins once the trie is
  large (right_cash ahead of bare firewood from 100k rows on), so R20
  stands. At capacity 100k the full copy per proposal costs about 28 ms per
  block before the first op and 3.4 GB for 128 revisions. If the set is to
  grow: persistent maps (keeps R20's properties, about 7 µs per op) or a
  mutable base with overlays and undo deltas (0.1–0.2 µs per op, brings
  back chain reads and a read lock; policies keep consensus state as
  per-key attributes in the entries). Either way the policy digest must
  become incremental (design §7.3).
- **Eviction p99.** Bulk eviction (design §6.2) makes propose p99 60–140
  ms on the eviction blocks against a 9–25 ms mean (clarification 28
  runs). Measured (design §14 "Latency breakdown"): the incremental rule
  (`--high-watermark` = low watermark, R30) brings the `zipf` p99 from 114
  to 20 ms plain and 15 ms speculative at a higher mean (17.8 and 12.6 ms
  against 14.5 ms plain bulk), because every block then pays 329 cold
  puts or a tail step. The default stays bulk until the milestone-3
  sweep decides (the set's own work and the tail step are now 2.2–3.1 ms
  and 0.1–0.7 ms, design §14 "Clarification 33").
- **10M-row experiments** (design §12.13): the gain over bare firewood
  narrows from a third at 1M rows to a tenth to a seventh at 10M with the
  default 192 MB node cache, because the evicted keys are the cold ones
  whose paths are uncached. Rerun at 10M with a larger
  `--cache-memory-limit`, and with speculation (milestone 2).
- `MemoryStorage`'s root costs one BLAKE3 hash plus a 2 KiB ChaCha8
  expansion per op; fine as a model, but it is slower than firewood's
  propose in release mode, so absolute memory-engine numbers are not a
  performance reference.
- Evaluate the speculation defaults in the milestone-3 sweep (design §9.2,
  §12.5). `min_step` is now 256 following the focused Zipf comparison;
  broader tuning of `rounds` and `margin` remains open.
- Crash recovery and state sync (design §12.6). Clean-shutdown checkpointing
  now restores the latest committed set and policy against matching storage.
  It does not provide per-commit durability or an atomic transaction between
  the checkpoint file and storage; unexpected termination still needs a WAL,
  replay, or storage rollback protocol.
- Milestone 1 target (design §12.13, decided R16/R18/R20): per-write
  overhead of the write set (entry insert, commitment update, policy update)
  near 0.5 µs and the per-proposal copy around 2 ms at 10k entries (1.9 ms
  FIFO, 2.2 ms LRU-write measured by the since-removed `cow_base`
  micro-benchmark, design §4, §12.13).
  Measured in situ (design §14 "Latency breakdown", "Clarification 32"):
  with the lattice 3.1–4.2 µs per op plus 3.5–4.9 µs per victim, all but
  0.3–0.4 µs of it lattice items at 2.7–2.8 µs each; with `rehash` the
  per-op work is 0.3–0.4 µs (the target met) plus 0.66–0.75 ms per block
  for the hash; the copy is 3.5–4.0 ms per proposal (the target missed by
  2×; the flat snapshot of design §12.17 (a) makes it a 0.14 ms memcpy).
  §12.14 lists the remaining snapshot refinements (policy heights in the
  entries; an atomic registry pointer); the item above lists the levers.
- Milestone 5 adds the proof methods removed in the R20 redundancy pass
  (`VectorCommitment::prove`/`verify`, `StorageEngine::prove`/`verify_proof`,
  `Handle::prove`, design §7.4) together with the KZG scheme (its stub module
  and feature were removed too).
