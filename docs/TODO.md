# auth_wal TODO

Items deliberately deferred. Design references are to `01-design.md`.

## Firewood-side changes (R15, design §12.10)

Confirmed against firewood's source: proposals can be built on uncommitted
proposals (`Proposal::propose` → `propose_with_parent`), but `manager.commit`
rejects a child whose parent is still a proposal (`parent_hash_is` returns
false for `NodeStoreParent::Proposed`, giving `NotLatest`), and neither the
FFI nor the Go wrapper walks ancestors. Consequences accepted for now:

- one engine commit and one revision per speculative step, so the engine
  window is `K × (1 + rounds)` and a mid-chain commit failure poisons the
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
   (clarification 14, 2026-10-28, design §12.10 (e)); `build_link` in
   `speculate.rs` (retract ops) and `Propose` in `lib.rs` are the two places
   to change.

## auth_wal

- Feature `ffi` should also enable `firewood` once the Go layer exists
  (milestone 4): design §3 names `firewood` as `WithStorageEngine`'s default.
- The C ABI (milestone 4) keeps the writer behind a mutex and a `Reader`
  beside it (design §3 threading contract, R19): the Rust writer takes `&mut
  self`, which C cannot express; the Go package's `Proposal.Commit`,
  `Proposal.Propose` and every `Prefetch` become writer calls on the database
  handle, so the Go surface stays `qmdb-ffi`'s.

- Milestones 1–5 as listed in design §13 (milestone 0, the benchmark suite
  and the firewood backend, is done); the benchmark's auth_wal target comes
  with milestone 1.
- `MemoryStorage`'s root costs one 2 KiB BLAKE3 XOF per op; fine as a model,
  but it is slower than firewood's propose in release mode, so absolute
  memory-engine numbers are not a performance reference.
- Decide the provisional speculation defaults (`rounds`, `min_step`,
  `margin`) from the milestone-3 sweep (design §9.2, §12.5).
- Bootstrap/durability before any validator may restart (design §12.6).
- Milestone 1 target (design §12.13, decided R16/R18/R20): per-write
  overhead of the write set (entry insert, commitment update, policy update)
  near 0.5 µs and the per-proposal copy around 2 ms at 10k entries (1.9 ms
  FIFO, 2.2 ms LRU-write measured in `benches/lattice.rs`, `cow_base`);
  measure against the `zipf`/`create` baselines. §12.14 lists the snapshot
  refinements to measure then (policy heights in the entries, which is about
  half of the copy; an atomic registry pointer; the flat sorted array).
- Milestone 5 adds the proof methods removed in the R20 redundancy pass
  (`VectorCommitment::prove`/`verify`, `StorageEngine::prove`/`verify_proof`,
  `Handle::prove`, design §7.4) together with the KZG scheme (its stub module
  and feature were removed too).
