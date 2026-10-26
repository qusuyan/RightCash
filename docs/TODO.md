# auth_wal TODO

Items deliberately deferred. Design references are to `01-design.md`.

## Firewood-side changes (R15, design §12.10)

Confirmed against firewood's source: proposals can be built on uncommitted
proposals (`Proposal::propose` → `propose_with_parent`), but `manager.commit`
rejects a child whose parent is still a proposal (`parent_hash_is` returns
false for `NodeStoreParent::Proposed`, giving `NotLatest`), and neither the
FFI nor the Go wrapper walks ancestors. Consequences accepted for now:

- one engine commit and one revision per speculative link, so the engine
  window is `K × (1 + rounds)` and a mid-chain commit failure poisons the
  instance (design §8, §9.2);
- intermediate trie hashes recomputed once per link that touches a path and
  once more when the final proposal reverts a key a link wrote; mitigated by
  pessimistic prediction (`speculation_margin`, design §9.2).

Future changes, in order of value:

1. **Cascade commit**: `commit` walks `Proposed` ancestors and commits them
   oldest-first inside the same critical section. Removes the caller-side
   ordering and the mid-chain failure window; still one revision per link.
2. **Squash commit**: merge a chain's node stores into one revision. Removes
   the window-sizing cost and the per-link revision records.
3. **Hashing shared across a chain**: defer or share path hashing so a path
   touched by several links (or written then reverted) is hashed once.
4. **Native warm-path call** replacing read-through prefetch under
   `CacheReadStrategy::All` (design §9.4).

## auth_wal

- Milestones 1–6 as listed in design §13.
- Decide the provisional speculation defaults (`rounds`, `min_link`,
  `max_links_total`, `margin`) from the milestone-3 sweep (design §9.2, §12.5).
- Bootstrap/durability before any validator may restart (design §12.6).
