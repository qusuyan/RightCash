# auth_wal TODO

Items deliberately deferred. Design references are to `01-design.md`.

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
4. **Native warm-path call** (no longer needed by auth_wal's hints, R26;
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

## auth_wal

- The `ffi` feature, `src/ffi/` and the `staticlib` crate type arrive with
  milestone 4 (the stubs that stood for them were removed in the review of
  2026-11-02, design §13); the feature should also enable `firewood`,
  which design §3 names as `WithStorageEngine`'s default.
- The C ABI (milestone 4) keeps the writer behind a mutex and a `Reader`
  beside it (design §3 threading contract, R19): the Rust writer takes `&mut
  self`, which C cannot express; the Go package's `Proposal.Commit`,
  `Proposal.Propose` and every `Prefetch` become writer calls on the database
  handle, so the Go surface stays `qmdb-ffi`'s.

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
  describe upstream. Adapter generations retain auth_wal's stricter rule
  that even a no-op commit supersedes the old head; contract tests cover it.
- **Set structure for a larger set (open, clarifications 27–28, design
  §12.13, §12.15).** At the default 10k the copy wins once the trie is
  large (auth_wal ahead of bare firewood from 100k rows on), so R20
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
