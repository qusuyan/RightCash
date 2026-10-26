# auth_wal design

Status: DESIGN v11 (2026-10-26). Derived from `00-project-description.md`
including its clarifications (R13–R15); v1 to v6 incorporate five review
rounds plus a spot-check (see §14); v7 reworked §9 and §10 for the
clarifications, v8 to v10 incorporate their reviews, which declared the design
ready for milestones 1–2; v11 records the pessimistic-prediction decision
(R15). The crate is a compiling skeleton whose bodies are `todo!()`.

## 1. Goal, non-goals, v0 limitations

`auth_wal` is an authenticated, bounded, in-memory set of recent writes placed
between the EVM validators (`Avax-pEVM`, `libevm`) and an authenticated storage
engine (QMDB via `qmdb-ffi`, or firewood). From the validator's point of view
it *is* the database: it exposes the same Go `ffi` package shape as `qmdb-ffi`.

**Target adapter.** The adapter port that consumes it is
`Avax-pEVM/graft/evm/qmdb/` (plus the `qmdb-ffi/docs/03-avax-integration.md`
patches to coreth), because that port already treats roots as opaque
commitments and translates them for block headers. The older
`graft/evm/firewood/` port assumes Ethereum MPT roots and can only be used
with the same patches applied. Neither port needs code changes beyond the
import (R1) and, for the prefetch hints of R14, one optional interface.

**Storage backend of record: firewood** (R13). Firewood's root is a Merkle
trie root, a function of the stored state alone, which is what lets
speculative eviction (§9) stay invisible to consensus. QMDB already groups
freshly written keys together and gains less from a write set in front; it
keeps interface-level support (its roots cover the op history, so speculation
is disabled on it, §9.3).

Goals

- Absorb recent writes in memory and update them in place, so hot keys cost one
  storage write per eviction rather than one per block.
- Keep every block's state root computable at propose time, before commit.
- Make three things pluggable behind traits: eviction policy, vector
  commitment scheme, storage engine.
- Let the eviction policy predict its next victims so their writes can be
  proposed to the storage engine ahead of the actual eviction.

Non-goals

- Root compatibility with Ethereum MPT roots, QMDB roots or firewood roots.
  Roots are opaque 32-byte commitments.
- Range iteration and range/change proofs (firewood-only features the adapter
  does not use on the consensus path).

v0 limitations (explicit, consensus-relevant)

- **Everything is in memory** (R10). The set contents *and* the policy state
  are consensus state: two validators with different set contents produce
  different roots. In v0 every validator starts fresh at genesis and never
  restarts with state; `New` rejects an engine that already has committed
  state (`EngineNotFresh`), because the adapter keys `Initialized()` on
  `Root() != EmptyRoot` and would otherwise re-run genesis over it. A restart or a state-sync is a consensus-state recovery
  problem (replay from genesis, or a replicated snapshot) and is deferred
  (§12.6).
- Roots are **history-bound**: they include the block height and the policy
  state, so the same key-value contents reached by different histories yield
  different roots. This is the same property QMDB has and the qmdb adapter
  port already relies on (§3, empty blocks).
- **Consensus-state rule.** Anything that can influence a future root is
  either digested into the current root (set contents, policy state, scheme,
  policy and engine parameters, §7.3) or a pure function of state that is.
  Nothing root-affecting may depend on the local `Propose` stream, on when
  blocks were accepted, on timing, or on configuration outside the consensus
  part of `Config`. Speculative eviction and prefetch hints (§9) are local by
  construction: on a state-function engine they change when storage work is
  done, never the resulting state.

## 2. Architecture

```
 Avax-pEVM / libevm adapter (unchanged, imports package `ffi`)
            │  New / Propose / Commit / Get / Root / Revision / Prefetch / Close
            ▼
 ┌──────────────────────────── auth_wal (Go `ffi` ← C ABI ← Rust) ───────────────────────────┐
 │  Proposal tree of copy-on-write WriteSets                                                 │
 │   ┌─────────────────────┐    ┌──────────────────────┐    ┌───────────────────────────┐    │
 │   │ WriteSet (persistent│    │ EvictionPolicy       │    │ VectorCommitment          │    │
 │   │ ordered map k→Entry)│    │  select_victims()    │    │  apply(key, old, new)     │    │
 │   │                     │    │  likely_victims()    │    │  commitment()             │    │
 │   │                     │    │  digest()            │    │                           │    │
 │   └─────────────────────┘    └──────────────────────┘    └───────────────────────────┘    │
 │                                   │ victims / predictions / prefetch hints                │
 │                                   ▼                                                        │
 │                      Evictor + Speculator ──► StorageEngine proposals (chained)           │
 └────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                              ▼
                              StorageEngine trait: Memory model (now) | firewood (M4) | QMDB (interface only)
```

Language split. The core is a Rust library crate, because the candidate
commitment schemes (lattice hash, KZG) and both storage engines live in Rust.
A thin C ABI (cbindgen) and a Go package named `ffi` mirror `qmdb-ffi`
one-for-one. A pure-Go layer wrapping the existing Go `ffi` packages was
rejected: it would force the commitment arithmetic into Go and double the FFI
crossings per block.

## 3. External interface (R1)

The Go package exports **every symbol `qmdb-ffi` exports**, with identical
names, so both adapter ports compile unchanged. The adapter ports use, between
them: `New`, `Database`, `Proposal`, `Revision`, `BatchOp`, `Put`, `Delete`,
`PrefixDelete`, `Hash`, `EmptyRoot`, `ErrRevisionNotFound`, `Option`,
`WithRevisions`, `WithNodeCacheSizeInBytes`, `WithTruncate`, `WithRootStore`,
`WithReadCacheStrategy`, `WithFreeListCacheEntries`, `WithExpensiveMetrics`,
`EthereumNodeHashing`, `CacheStrategy`, `CacheAllReads`, `StartMetrics`,
`Gatherer` (coreth `vm.go`), and `Revision.Drop`.

| Symbol | Behaviour in auth_wal |
|---|---|
| `New(dir, NodeHashAlgorithm, opts...)` | Opens the storage engine (all firewood/qmdb options pass through to it; for `MemoryStorage` the directory and hashing algorithm are accepted and ignored) and an empty WriteSet. New options: `WithSetCapacity(n)` and `WithLowWatermark(n)` (consensus), `WithEvictionPolicy(name)`, `WithCommitmentScheme(name)`, `WithStorageEngine(name)`, and the local `WithSpeculation(bool)`, `WithSpeculationRounds(n)`, `WithSpeculationMinLink(n)` (§9.2). `WithRevisions(K)` sizes auth_wal's K-ring; the engine is opened with a window of `K × (1 + rounds)` when speculation is on and the engine commits chain links separately (firewood), else `K` (§8, §10). The Rust core is statically generic (§13); the names select one of a fixed list of monomorphizations behind an enum, and an unknown combination is rejected at `New`. |
| `Update(batch)` / `UpdateWithMetadata` | `Propose` + `Commit` in one call. |
| `Propose(batch)` / `ProposeWithMetadata` | Builds a child WriteSet, evicts if over capacity, creates the storage proposal for the evicted writes, returns a `Proposal` whose `Root()` is known immediately (§8). `Root()` is a single 32-byte commitment combining the set commitment and the storage root (§7.3); the two are never exposed separately. Metadata is root-affecting (it enters the preimage; `None` and `Some(empty)` are distinct, as in `qmdb-ffi`) and is also carried by the node's own storage proposal when one exists (on the speculative path, the final proposal; speculative links never carry metadata, §9.1). |
| `Proposal.Get` | Pinned storage view after the WriteSet snapshot (§5). |
| `Proposal.Propose` / `ProposeWithMetadata` | Child of an uncommitted proposal (the adapter chains during block building). `ProposalOrphaned` if the parent's chain no longer leads to the committed state (§10); `qmdb-ffi` and firewood accept proposing on an orphan, auth_wal rejects it. |
| `Proposal.Commit` | Commits uncommitted ancestors in order, then this node; rejects proposals whose chain does not start at the committed state (§8). |
| `Proposal.Drop` | Frees the snapshot and drops the storage proposal. |
| `Prefetch(keys)` on `Database`, `Proposal`, `Revision` | **New (R14).** Hint that execution reading through this view is writing `keys`. Keys not in the set are forwarded to the engine to warm their trie paths; all keys refine the speculative eviction attached to this view's node (§9.4). Advisory: a wrong hint costs work, never correctness. |
| `Get` | Committed WriteSet, then committed storage. |
| `Root` | Combined root of the committed state (§7.3). `EmptyRoot` is the Ethereum empty-root constant, as in `qmdb-ffi`, reported exactly while no proposal has been committed, so the adapter's genesis idiom (`Initialized()`, `Revision(EmptyRootHash)`) keeps working. |
| `Revision(root)` / `LatestRevision` | Resolves live proposals and the last K committed snapshots. `ErrRevisionNotFound` for an unknown root; `ErrRevisionStale` from `Revision.Get` when the pinned storage view has left the engine's window. |
| `Prune`, `GatherMetrics`, `Gatherer`, `StartMetrics*` | Pass-through / composition. |
| `Close(ctx)` | Mirrors `qmdb-ffi`: waits for live `Proposal`/`Revision` handles until `ctx` expires, then returns `ErrActiveKeepAliveHandles`; otherwise sets the closing flag (hints become no-ops), waits for in-flight speculative links to finish (§9.2), drops every chain and the K-ring's engine views, closes the engine, and marks the instance closed. Idempotent; allowed on a poisoned instance. |
| `BatchOp`: `Put`, `Delete`, `PrefixDelete` | `Put`/`Delete` land in the set. `PrefixDelete` removes matching set entries (an ordered range scan, §4) and is forwarded to storage in the same proposal (§6.3); it fails if the backend rejects it (QMDB does). The qmdb adapter port never emits it. |
| Proofs (`ProveLatest` / `VerifyLatest`) | Scheme-dependent (§7.4). Not used by the adapter. |
| `NodeHashAlgorithm`, `CacheStrategy`, legacy options | Accepted; forwarded to a firewood backend, ignored otherwise, exactly like `qmdb-ffi`. |

**Empty blocks.** A block that writes nothing still gets a new root, because
the height is in the root preimage (§7.3). This matches QMDB's "every commit
changes the root" and is what the qmdb integration relies on: coreth's
`commitWithSnap` (`core/blockchain.go`, patched per `qmdb-ffi/docs/03` P3)
assumes a child's root never equals the parent's, so it always has a proposal
to commit, and the adapter port's `createProposals` de-duplicates candidate
proposals by root. Post-commit `Root()` equals the proposal's
`Root()`.

Deviations relative to firewood are the `qmdb-ffi` list plus: roots depend on
the set capacity, low watermark, eviction policy, commitment scheme and
storage engine, which are therefore consensus parameters and must be identical
on every validator; proposing on an orphaned proposal is rejected; `New`
requires a fresh engine (v0); `Prefetch` is an addition; on firewood the
engine's read-cache strategy is forced to cache all reads (§9.4), overriding
`WithReadCacheStrategy`. Speculation and its rounds are local settings.

## 4. Data model: the WriteSet (R2, R7)

```rust
pub trait Key:   Clone + Eq + Ord + Hash + AsRef<[u8]> + Send + Sync + 'static {}
pub trait Value: Clone + Eq + AsRef<[u8]> + Send + Sync + 'static {}

enum Entry<V> { Value(V), Tombstone }               // Tombstone = pending delete

struct WriteSet<K: Key, V: Value, C: VectorCommitment<K, V>, P: EvictionPolicy<K>> {
    entries:    PersistentMap<K, Entry<V>>,      // persistent ORDERED map
    len:        usize,                           // entries, including tombstones
    vc:         C,                               // scheme instance (parameters, e.g. KZG setup)
    commitment: C::State,                        // incremental commitment of `entries`
    policy:     P,                               // per-snapshot policy state
    height:     u64,                             // parent.height + 1 for every proposal
}
```

- **Persistent ordered map.** `entries` is a persistent B-tree
  (`im::OrdMap`), not a hash map: snapshots are a pointer copy, updates copy
  O(log n) nodes, iteration order is the key order (canonical on every node,
  no hasher to seed, no hash-flooding surface), and `PrefixDelete` is a range
  scan. The `PersistentMap` wrapper hides the backing crate. A HAMT could be
  swapped in later if point lookups dominate, but it would need a fixed,
  cross-version-stable hasher.
- **Copy on write.** A proposal that touches `w` keys costs O(w log n) extra
  memory and the parent stays readable and unchanged.
- **Update in place.** `Put` on a key already in the set replaces the entry
  and leaves `len` unchanged; this is the whole point of the layer.
- **Deletes are tombstones** until evicted; eviction turns a tombstone into a
  `Delete` batch op for storage. Tombstones count toward capacity.
- **Capacity is counted in entries**, not bytes, in v0. `len` exists so a
  byte-based or adaptive capacity can replace it later (§6.4).
- **Height as the clock.** Policies and the commitment never read wall-clock
  time. `height` is the only notion of time: the snapshot with nothing
  committed has height 0 and every proposal is `parent.height + 1`. It counts
  proposals since genesis (v0 always opens at genesis, §1; a later bootstrap
  must restore it, §12.6). The chain's genesis block is itself committed as a
  proposal, so under the v0 assumption (fresh start, one proposal per block)
  a block at chain height `h` sits at auth_wal height `h + 1`; the adapter's
  own height bookkeeping is Go-side and never reads this value. The `ffi`
  interface does not pass a height. Within one batch the policy observes
  writes in batch order; keys written at the same height tie-break by key
  bytes in eviction order (§6.1).
- **Generic key and value types.** The `ffi` interface carries both keys and
  values as plain byte strings. The core is generic over `K: Key` and
  `V: Value` (blanket-implemented for `Box<[u8]>`, `Vec<u8>`, `[u8; N]`) so
  typed callers, such as a Rust-side adapter with 32-byte keys or fixed-size
  slots for KZG, pay no boxing. The FFI layer instantiates
  `AuthWal<Box<[u8]>, Box<[u8]>, _, _, _>`. `PrefixDelete` carries a
  `Box<[u8]>` prefix, not a `K`, because a prefix is shorter than a key.

**Batch semantics.** Ops apply to the set in batch order, each exactly once:
`PrefixDelete(p)` followed by `Put(p‖s)` keeps the put; a key written twice in
one batch produces two `on_write` calls at the same height (the second is a
no-op for FIFO and LRU-write, and counts twice for a future LFU).

## 5. Reads (R5)

Every node of the proposal tree (committed state, live proposal, retained
revision) carries a **pinned storage view** (§8): the storage proposal created
by the nearest ancestor-or-self that evicted, otherwise the committed storage
revision that chain started from. Lookup order for any read:

1. The node's WriteSet snapshot. `Value` returns it; `Tombstone` returns
   absent and stops.
2. The node's pinned storage view.

The pinned view stays valid across commits: `StorageProposal::commit(&self)`
is specified to leave the handle readable at the committed root (the engine
adapter re-bases to a revision internally, exactly as `qmdb-ffi` does), so a
live child proposal keeps reading through the same handle after its parent
commits. If the engine's revision window has since dropped that root, the read
returns `RevisionStale`, never wrong data.

Reads never mutate policy state. RPC reads differ between validators, and a
read-aware policy would make eviction, and hence roots, non-deterministic. A
read-aware policy would need the adapter to feed only execution-time reads
(§12.3).

## 6. Eviction (R3, R4, R8)

### 6.1 Trait

```rust
pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {
    /// Called for every key written by a proposal, in batch order.
    fn on_write(&mut self, key: &K, height: u64);
    /// Called when a key leaves the set other than through `select_victims`
    /// (prefix delete). `select_victims` removes its own victims.
    fn on_remove(&mut self, key: &K);
    /// Choose up to `n` keys to evict now and remove them from policy state.
    /// Deterministic given identical call history; ties broken by key bytes.
    fn select_victims(&mut self, n: usize) -> Vec<K>;
    /// Non-mutating prediction of the next `min(n, len)` victims, best first.
    /// Local use only (speculative eviction, §9); never affects a root.
    fn likely_victims(&self, n: usize) -> Vec<K>;
    /// Prediction that skips keys execution is known to be writing this block
    /// (prefetch hints, §9.4), which under LRU-write will not be evicted.
    /// Default: `likely_victims(n + exclude.len())` minus `exclude`, first `n`.
    fn likely_victims_excluding(&self, n: usize, exclude: &BTreeSet<K>) -> Vec<K>;
    /// Digest of everything that determines future `select_victims` output.
    /// Enters the root preimage (§7.3). Incremental.
    fn digest(&self) -> Digest;
    fn name(&self) -> &'static str;
    /// Digest of the policy's fixed parameters (none for the v0 policies;
    /// e.g. a decay constant for a future LFU). Enters the root preimage.
    fn params_digest(&self) -> Digest;
}
```

`Clone` is required because policy state is part of each WriteSet snapshot:
sibling proposals must evict from identical policy state, and dropping a
proposal must drop its policy mutations. `select_victims` is
consensus-critical: it decides what leaves the set, so it may not use
randomness or call history, and `digest` must cover whatever determines it.
`likely_victims` only steers speculative eviction (§9), whose effect on
storage is always reverted where it was wrong, so it may be heuristic; it
should still be a cheap, deterministic function of the state so that
speculation is reproducible in tests. Invariant relied on by §9: the keys the
policy tracks are exactly the keys of the set. Policies keep their bookkeeping in
persistent structures (`im::OrdMap<(u64, K), ()>` for eviction order plus
`im::OrdMap<K, u64>` for the reverse index), so cloning is O(1) and updates
O(log n). `digest` is an order-independent incremental digest over the
`(key, height)` pairs the policy orders by. It uses the same lattice-hash
accumulator and item encoding as the `lthash` scheme (§7.2, with its own
domain tag), so the consensus-visible policy digest is no weaker than the set
commitment; it costs one hash per write.

v0 policies

| Name | Victim order | `likely_victims` quality |
|---|---|---|
| `fifo` | Oldest insertion `height` first | Exact unless a victim is re-inserted after eviction (updates do not move keys). **Must override `likely_victims_excluding` to ignore the exclusion**: a hinted in-set key is still evicted under FIFO, so excluding it would predict keys that stay and cause reverts. |
| `lru-write` | Oldest last-write `height` first | A hinted (written) key moves to the back, so the default exclusion is exact for it |
| `lfu-write` (later) | Fewest writes since insertion, then oldest | Noisier |

### 6.2 When and how much

Eviction runs inside `Propose`, after the batch is applied to the child
snapshot: if `len > capacity`, evict down to `low_watermark`. Both are
**explicit, fixed consensus parameters** in v0 and both enter the root
preimage. Default `low_watermark = capacity − capacity/8`. (An earlier draft
derived the watermark from the median batch size seen locally; that is
node-local hidden state and would split consensus, since validators see
different `Propose` streams.) Evicting in bulk amortises the storage proposal
cost; the benchmarks decide the cadence before parameters are fixed (§12.4).

### 6.3 Storage proposal for evicted writes

A storage proposal is created for a node iff its batch contained a
`PrefixDelete` or its eviction was non-empty. The storage batch is in
**canonical order**: the user's `PrefixDelete`s first, in batch order, then
the evicted entries sorted by key bytes (`Put` for values, `Delete` for
tombstones). Canonical order keeps the batch reproducible (and matters for
engines whose root covers the op sequence, §10). On the plain path the batch
is proposed directly on the **parent node's pinned storage view**; on the
speculative path a final proposal with reverts, prefix deletes and the batch is
proposed on the last link of the parent's speculative chain (§9.1). Either
way the resulting proposal becomes the child's pinned view and is committed or
dropped together with it (§8). Nodes without a storage proposal
inherit the parent's pinned view and `storage_root`, so a block that stays
inside the set never touches the engine.

### 6.4 Capacity control (future)

```rust
pub trait CapacityController { fn target(&mut self, stats: &SetStats, height: u64) -> usize; }
```

v0 ships `Fixed(n)` and `Config.capacity` is its initial value. Growing or
shrinking by workload is consensus-visible, so an adaptive controller may use
only committed-history statistics (occupancy, batch sizes, eviction counts),
never local timing or the local `Propose` stream. The hook is reserved;
`low_watermark` follows capacity by the same ratio (`Effective::retarget`).
Note for that future work: the effective parameters a child uses must be a
function of its *parent* node (e.g. carried in the parent's preimage), not of
the instance's committed node, or a child proposed on an uncommitted parent
would differ from one proposed after the parent committed (§9.3 test 1).
With `Fixed` the two coincide.

## 7. Authentication (R6)

### 7.1 Trait

```rust
pub trait VectorCommitment<K: Key, V: Value>: Clone + Send + Sync + 'static {
    type State: Clone + Send + Sync;
    fn scheme_id(&self) -> &'static str;
    /// Digest of the instance parameters (KZG setup hash and slot count, Merkle
    /// depth; zeros for lthash). Enters the root preimage (§7.3).
    fn params_digest(&self) -> Digest;
    /// The scheme instance carries its parameters (KZG setup, Merkle depth), hence `&self`.
    fn empty(&self) -> Self::State;
    /// Incremental update for one key; `None` = absent.
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>);
    fn commitment(&self, state: &Self::State) -> Digest;          // 32 bytes, enters the root
    fn supports_proofs(&self) -> bool;
    fn prove(&self, state: &Self::State, set: &PersistentMap<K, Entry<V>>, key: &K) -> Option<Proof>;
    fn verify(&self, commitment: &Digest, key: &K, entry: Option<&Entry<V>>, proof: &Proof) -> bool;
}
```

The trait is incremental by design: every candidate scheme updates its
commitment from `(old, new)` without touching the rest of the set, so
per-block cost is proportional to the batch rather than the set.

### 7.2 Schemes

| Scheme | State | Update cost | Membership proof | Notes |
|---|---|---|---|---|
| `lthash` (v0 default) | 2 KiB lattice-hash vector (1024 × u16 lanes) | one XOF + lane add/sub | none (set-level only) | Additively homomorphic: `C ← C − H(k,old) + H(k,new)`. Cheap, order-independent, authenticates the set as a whole. |
| `kzg` (next) | one G1 element (48 B, hashed to 32 for the root) + persistent `key → slot` map | one scalar mult | yes, constant size | Slot count must exceed `capacity` by a safety factor (a `capacity`-slot table is guaranteed to collide near full occupancy); collision policy is open (§12.2). Trusted setup of slot-count size. |
| `merkle` (reference) | whole sparse Merkle tree (persistent) over `SHA-256(key)`, depth 256, leaf = the entry item below | 256 hashes per update | yes | No slots, so no collisions; parameter-free. Slow but proof-friendly baseline for tests and benchmarks. |

**Lattice accumulator encoding** (shared by `lthash`, the policy digest and
`MemoryStorage`'s root). An item is the concatenation of its fields, each as
`u32 LE length ‖ bytes`, so moving a byte between key and value changes the
item; an entry item is `tag ‖ key ‖ value` with tag `0x00` for a tombstone
and `0x01` for a value, so tombstone, empty value and absent are distinct;
integers are `u64 LE`. An item's lane vector is the BLAKE3 keyed XOF
(`derive_key` with the use's domain tag) over the item, expanded to
2 × 1024 bytes read as `u16 LE` lanes; `add`/`remove` are lane-wise ± mod
2^16; the 32-byte digest is SHA-256 of the lane vector in `u16 LE`. The
parameter security of 1024 × u16 lanes is an open question (§12.11).

Each scheme is a feature-gated module under `src/vc/`. Tests run the same
suite against every enabled scheme.

### 7.3 Root composition (R12)

The validator sees exactly one commitment per proposal, revision and committed
state. It is computed when the node is built and is what `Root()` returns and
what `Revision(root)` is keyed by.

```
root(node) = EMPTY_ROOT                                         if node.height == 0
           = SHA-256( "auth_wal/v1"
                    ‖ len ‖ scheme_id ‖ len ‖ policy_name ‖ len ‖ engine_id   (u32 LE length-prefixed)
                    ‖ scheme.params_digest() ‖ policy.params_digest()
                    ‖ capacity ‖ low_watermark                   (u64 LE, u64 LE)
                    ‖ height                                     (u64 LE)
                    ‖ VC.commitment(set)                         set contents
                    ‖ policy.digest()                            policy state
                    ‖ storage_root                               pinned storage view
                    ‖ metadata_hash )                            None → 32 zero bytes; Some(m) → SHA-256(m)
```

Why each term: the parameters and parameter digests keep two networks with
different settings from agreeing on a root by accident; `height` makes every
block's root unique (empty blocks, §3); the **policy digest** makes the root
pin the policy state. Together with the consensus-state rule (§1), this gives:

> Two nodes with equal roots have identical reads and, fed identical future
> batches, produce identical future roots.

That is what the adapter's root-keyed lookups assume. Without the policy
digest, two siblings that both leave the set contents unchanged (one rewrote a
key with its existing value, one wrote nothing) would share a root but evict
differently later. `engine_id` is included because every engine reports
`EMPTY_ROOT` before its first commit, so without it two networks on different
engines would share roots and diverge later. `storage_root` is the
pinned storage view's root; a proposal without eviction reuses its parent's.
Both QMDB and firewood report a proposal's root at propose time, so the
combined root is available before commit. `EMPTY_ROOT` means "nothing
committed yet": the height-0 node; `Revision(EMPTY_ROOT)` resolves to it
while that node is in the K-ring (the adapter needs it only at genesis), and
returns `RevisionNotFound` once it is retired. Every node keeps its `metadata_hash` so the preimage can
be reproduced for proofs (§7.4) and tests.

### 7.4 Authenticated reads

A proof of `key = value` under `root` is the preimage components plus a set
proof (membership, or non-membership) from the scheme and, on non-membership,
the storage engine's proof under `storage_root`. `lthash` cannot produce a
per-key proof, so `ProveLatest` returns "unsupported" under it; `kzg` and
`merkle` can. This is the main reason the scheme is pluggable (§12.1).

## 8. Proposal tree and lifetime (R7, R8)

```
committed ──► P1 ──► P2        (chain: block building)
         └──► P1'              (sibling: competing block)
```

Node contents: the WriteSet snapshot, the pinned storage view (§5), the
storage proposal this node created (if any), how its eviction reached
storage (none / plain / speculative, with reused and reverted op counts for
§9.5), the speculative chain attached to it as a parent (local, §9.2),
`storage_root`, `metadata_hash`, `root`, a `committed` flag and a clearable
parent link.

- **Propose** creates a child node (`WriteSet::child`, height + 1), applies
  the batch, evicts, creates the storage proposal if needed, composes the root,
  and registers the node under its root. Live proposals are held weakly by the
  tree; the caller's `Proposal` handle owns the node.
- **Commit(P)** walks up from P collecting uncommitted ancestors until it
  reaches a committed node, which must be *the* committed node (pointer
  equality); otherwise `ProposalOrphaned` (a sibling committed first). It then
  commits each node oldest-first: commit its storage proposal if it created
  one (firewood sees one commit per storage proposal, in order: each link of
  a speculative chain, then the final proposal; the adapter only ever commits
  direct children of the committed state anyway), mark it committed,
  make it the committed node, push it on the K-ring (the ring holds the K
  most recent committed nodes *including* the head, so the oldest resolvable
  root is at most `(K − 1) × (1 + rounds)` engine commits behind, which is
  what makes an engine window of `K × (1 + rounds)` sufficient), and clear its
  parent link so freed snapshots are reclaimed. The persistent map shares structure, so only
  the nodes unique to the freed snapshots are released.
- **Drop** of a `Proposal` handle releases its strong reference and nothing
  else: the node (and its storage proposal) is freed when the last reference
  goes, which may be later if a child still links to it as parent or a
  `Revision` holds it. The root index holds weak links, so a dropped node
  simply stops resolving (sweep rule below). This lazy scheme has no drop-versus-commit
  ordering hazards: dropping P1's handle and then committing P2 (P1's child)
  commits and retains P1 normally, and `advance` registers idempotently.
  Sweep rule, stated once: a root's list is pruned of dead links when a node is
  registered under that root or retired from the ring, the whole index is
  additionally swept every K commits, and lookups never mutate. A
  handle collected by Go's GC (`runtime.AddCleanup`, as in `qmdb-ffi`) frees
  the same way; Go `Commit` disowns the keep-alive first, as `qmdb-ffi` does.
  Committing an already committed handle returns `ProposalConsumed`.
- **Root index.** `root → [Weak<Node>]`: equal roots are interchangeable
  (§7.3), but two distinct live nodes can still carry the same root, and the
  adapter may drop one of them. `Revision(root)` returns the first node that
  is still alive; the K-ring holds committed nodes strongly. Committed roots
  are pairwise distinct (height is in the preimage), so without retirement the
  index would grow by one entry per block; retiring a node removes its link
  (and the key once the list is empty), so a live orphan sibling sharing the
  root still resolves (sweep rule above).
- **Revision window.** K committed nodes are retained (`WithRevisions`). A
  `Revision` is just a node; `Revision.Get` reads through its pinned storage
  view and surfaces `RevisionStale` if the engine has dropped that root. The
  engine is therefore opened with a window of `K × (1 + speculation_rounds)`
  when speculation is on and the engine commits chain links separately
  (`COMMITS_CHAIN_LINKS_SEPARATELY`, §10: firewood and the in-memory model),
  else K. The Rust `new` checks the
  engine it is handed; the Go `New` opens the engine with that size. Note
  that firewood never reaps a revision that is still referenced, so a pinned
  firewood view does not go stale: its window stalls instead, and the K-ring
  (plus the adapter dropping revisions) is what bounds memory.

## 9. Speculative eviction and prefetch hints (R9, R14)

Goal: when `Propose` finally arrives with a block's batch and the victim set is
known, most of the storage engine's Merkle work for those victims is already
done. Three mechanisms:

1. Predicted victims are sent early, as a **chain of storage proposals**
   attached to the node the block is being executed on (its parent). The
   engine hashes their paths when each link is proposed, off the `Propose`
   path.
2. During execution the prediction is **refined** from prefetch hints (§9.4):
   as transactions dirty more new keys, more evictions become likely and
   further links are sent.
3. At `Propose` the **final proposal** chains on the last link: it **reverts**
   every predicted key that was not evicted (old value put back, or deleted if
   it was absent) and then applies the real batch, so the storage state after
   the final proposal is exactly what the plain path would produce. On an
   engine whose root is a function of state (firewood, §10) the storage root
   is therefore identical, and speculation is invisible to consensus: no
   parameter of it enters the preimage, and it may freely depend on timing,
   hints and local heuristics.

### 9.1 Plain and speculative paths

For a node `N` built on parent `P` from batch `B` with victims `V` (canonical
order, §6.3):

| path | storage proposals |
|---|---|
| **plain** (no chain on `P`, or speculation off) | one proposal on `P.storage_view` with the canonical batch: `PrefixDelete`s in batch order, then `V` in key order |
| **speculative** (`P`'s chain has at least one completed link, `L₁ → … → Lₖ`) | the final proposal `F` on `Lₖ`, with ops in this order: (1) **reverts** for every key in `sent ∖ V`, in key order: `Put(old)` where `old = P.storage_view.get(k)`, or `Delete` if absent; (2) the batch's `PrefixDelete`s in batch order; (3) `V` in key order, omitting the pairs identical in `sent` **unless** `B` contains a `PrefixDelete`, in which case all of `V` is sent |

where `sent` is the union of all `(key, entry)` pairs the links carried and
`sent ∖ V` is the key-wise difference (keys sent whose key is not in `V`).
Correctness: after (1) the storage state equals `P`'s storage state exactly
(every speculative write undone or confirmed-identical), after (2) and (3) it
equals the plain path's. The `PrefixDelete` rule exists because a link may
have written a key under a deleted prefix: omitting its identical pair would
let step (2) erase it while the batch re-puts it. Reverts need one storage
read per mispredicted key, warm because the link just wrote that path.

On the speculative path `F` is created even when `V` is empty (the reverts
still have to be applied) and even when it carries no ops at all, so the node
owns a distinct handle. A chain with no completed link takes the plain path:
no extra engine commit for a quiet block. `F` carries the node's metadata;
links never do.

### 9.2 Lifecycle of a speculative chain

A chain is **local state attached to a node acting as a parent**. Only two
kinds of node may carry one: the **current committed node** (the head) and a
**live, non-orphaned uncommitted proposal**. A chain is created by the first
prefetch hint for such a node, or for the head right after its commit (round
0: the next block most likely builds on it). A hint on any other node (a
retained revision that is no longer the head, an orphaned proposal) only warms
storage: proposing on such a view would route through the engine head and
violate the sole-writer invariant (§10 item 4). A chain is **dropped the
moment its node stops qualifying**: when a child commits, `advance` takes the
ex-head's chain; an orphaned proposal's chain is dropped at its next hint or
`Propose`, when the orphan check fails. Chains are shared by every child
proposed on the node and are never consumed by `Propose` (each child's `F`
chains on the current last *completed* link).

Round `r` sends link `Lᵣ` with the predicted victims not yet sent:

```
new      = hinted keys ∉ P.set                     (insertions this block)
expected = len(P.set) + max(new, estimate₀)        (estimate₀: running estimate of new keys per block, a floor)
E        = expected > capacity ? expected − low_watermark + margin : 0     (predicted eviction count)
pred     = first E of P.policy.likely_victims_excluding(E, hinted) with P.set values
Lᵣ       = pred ∖ sent (key-wise), in key order, proposed on Lᵣ₋₁ (or P.storage_view)
```

**Pessimistic prediction (R15).** `margin` (local knob `speculation_margin`)
deliberately over-predicts once the set is near capacity, so that the block's
later writes rarely push the victim set past what the links already carry and
no further link is needed. The price is more reverts in `F` for the keys that
were not evicted after all. This is the chosen trade-off against firewood's
per-proposal commits: every extra link, and every key written by a link and
reverted by `F`, makes firewood recompute the intermediate hashes on that path
once more (§12.10). Fewer, larger links cost less than many small ones.

A link is sent only when `|pred ∖ sent| ≥ min_link` and `r < rounds`, and
while the instance-wide budget `max_links_total` is not exhausted (local
knobs; defaults `rounds = 3`, `min_link = 1000`, `max_links_total = 16` are
provisional until the milestone-3 sweep, §12.5). Keys that leave the
prediction (a hinted key under LRU-write) are not retracted; they are
reverted in `F`. Each link costs the engine one proposal, which is the
precomputation.

**Where link hashing runs.** Never inside the tree lock's critical section,
and never where `Propose` has to wait for it. The hint path holds the tree
read lock only to resolve the node (clone its reference, snapshot the
effective parameters, retire the chain of a node that no longer qualifies)
and releases it before touching the engine. It then plans the link under the
node's chain mutex (cheap), releases it, proposes to the engine (hashing up to
`E` paths), re-takes the mutex and appends the link. Round 0 for the new head
runs after `Commit` has released the tree lock, in the committing thread in
v0 (a worker later).

**One in-flight link per chain.** Hints on one node may run concurrently, so
the chain records the link it is building (`in_flight`: its planned entries
and its planned parent, the current tail). While a link is in flight, further
hints only record their keys and warm storage; planning computes
`pred ∖ (sent ∪ in_flight)`; and `append` requires the tail to still be the
link's planned parent, otherwise the link is dropped. This keeps the chain
linear in the engine, so that `sent` is exactly what the tail's ancestry
wrote: with two links proposed on the same tail, `sent` would be a union that
neither branch carried and `F` would omit writes that never reached storage.
`F` is built from the completed links only; an in-flight link is not used
(if it completes later it is appended and a later sibling may use it).
Retirement is **sticky**: a node's chain slot has three states, absent, live
and retired, and a retired slot never becomes live again (a node that stopped
qualifying never re-qualifies), which both planning and `append` check. A
hint that resolved the node just before it was demoted therefore cannot
re-create a chain on it.

**Engine root check for round 0.** Links after the first are proposed on the
previous link's handle. The first link of a chain on the committed head is
proposed through the engine on its committed view, and a `Commit` may move
the head between planning and the engine call. The hint path therefore checks
`engine.root() == base root` before and after the call (sound on firewood,
where equal roots mean equal state) and drops the link, counting a
`link_failure`, if it does not hold; it never asserts.
`StorageProposal::propose` on a committed handle returns an error, never
panics.

**Budget.** A link's unit of `max_links_total` is an owned token, refunded
exactly once when the token is dropped: the hint task holds it from planning
through the engine call, and `append` moves it into the chain next to the
link, so retiring a chain refunds the completed links' tokens while an
in-flight link's token is still the task's and is refunded when the task
drops it (append refused, engine failure, slot retired). The budget is
checked and the token reserved in the **resolve step, under the tree read
lock and before any chain mutex**; if the budget is exhausted the sweep runs
there, taking one chain mutex at a time (never two: there is no order between
chain mutexes). The registry of chain-holding nodes (weak references) is kept
on the shared state behind its own leaf lock; it is pruned of dead and
retired nodes only in `advance`, under the tree write lock. In-flight links
are counted under the same read lock in which the closing flag is read, so
`Close` cannot miss one (§3). Engine memory lags one commit: firewood keeps a
dropped proposal until its next commit.

**Memory.** Live links are bounded by `max_links_total` across all chains,
each link by `E = expected − low_watermark` entries (which exceeds
`capacity − low_watermark` by the block's predicted insertions); dropping
chains on head change keeps retired links from pinning engine revisions for K
commits.

**Commit.** Committing a child commits `L₁ … Lₖ` then `F`, one engine commit
each on firewood (§10). A failure part-way through leaves the engine head at
a state no node owns, so it is fatal: `Commit` returns the error, the
instance is **poisoned** and refuses further `Propose`/`Commit`
(`Poisoned`), consistent with halting rather than diverging; reads keep
serving the committed node and `Close` is allowed. The plain path has no such
window (one commit): a node whose single commit failed is dead (the engine
consumed the handle) and a retry returns that error, not `ProposalConsumed`.

### 9.3 Consensus and gating

Speculation never changes the set, the policy state or the final storage
state, so on a state-function engine it never changes a root. The engine
declares the property with `ROOT_IS_STATE_FUNCTION` (§10); `New` rejects
`speculate = true` on an engine without it (QMDB, whose roots cover the op
history), so there is no configuration in which speculation is
consensus-visible.

Milestone-2 tests (on `MemoryStorage`, which models firewood):

1. Speculation on versus off, same block history: equal roots and equal
   storage roots at every height.
2. Arbitrary hints, including wrong, duplicate, late and **concurrent** ones
   (several threads hinting one node), and varying `rounds`/`min_link`: equal
   roots, live links never above `max_links_total`, and every budget unit
   refunded after `Close` (root equality cannot detect a budget leak).
3. Differential test: random batches including `Delete` and `PrefixDelete`
   (with predicted keys under the deleted prefix), reads compared against a
   reference map, and for every committed node `storage(k)` exact for every
   `k ∉ set`.
4. Two children of one parent, proposed in both orders, either committed:
   equal roots.
5. Policy purity: a cloned policy state yields identical `select_victims`
   sequences on both copies.

### 9.4 Prefetch hints (R14)

**Meaning.** A prefetch hint tells the storage engine which keys execution is
touching so it can bring their trie paths into memory before the final
hashing, and tells auth_wal which keys are being written so speculation can
be refined (§9.2).

**API.** `Prefetch(keys)` on `Database` (relative to the committed state),
`Proposal` and `Revision` (relative to that view), mirrored in the Rust core
as `prefetch(&self, keys)`. Advisory: a wrong hint costs work, never
correctness; a hint on a node that cannot carry a chain only warms storage
(§9.2); no-op after `Close`. On a hint, auth_wal:

1. forwards the keys **not in the set** to `StorageEngine::prefetch(view,
   keys)`, which warms their paths. Keys in the set need nothing: their writes
   land in the set, and the victims' paths are warmed by the links;
2. feeds every key to the node's speculative chain (§9.2).

**Firewood warming.** Firewood has no dedicated warm call; `prefetch` reads
each key through the view, which populates the node cache along the path
**only under `CacheReadStrategy::All`** (or `BranchReads`). That strategy is a
database-open setting whose default is `WritesOnly`, and the qmdb adapter port
does not set it; the firewood engine constructor (milestone 4) owns the
engine config, overrides the strategy to `All` (accepting `BranchReads`) and
logs that it did, a documented §3 deviation. This caches every read, RPC reads
included; and since execution's own reads also
go through the same views, the warming half of a hint is partly redundant on
firewood: the hint's main payload there is the speculation feed. A native
firewood warm call can replace the read-through later.

**libevm integration.** libevm's `StateDB` already collects exactly the right
stream: at each transaction's `Finalise`, `FinaliseHelper` gathers the dirtied
addresses (`addressesToPrefetch`) and `stateObject.finalise` the changed slots
(`slotsToPrefetch`). Today they feed the trie prefetcher, which cannot be used
here: it is created only when snapshots are enabled, coreth forces the
snapshot limit to zero for the QMDB and firewood schemes (because
`IntermediateRoot` swaps the trie for the prefetcher's copy, which these
adapters cannot tolerate), and its storage sub-fetcher opens storage tries in
a way the port rejects. So the hook is a direct one: an optional capability on
the trie, checked by type assertion exactly like the port's existing
`destructNotifier` (`Prefetch(addresses, slots)` or two calls), invoked from
`FinaliseHelper` / `stateObject.finalise` with the lists already built,
regardless of the `withPrefetch` flag the parallel executors pass (the hint is
advisory). The adapter hashes addresses and builds generation-qualified slot keys as it
already does for reads, and calls `Revision.Prefetch` (or `Proposal.Prefetch`)
on the view it reads through. Note that `journal.dirties` includes accounts
that were *touched* without being changed, so account hints mean "written or
touched" and slightly inflate `new` in §9.2; slot hints are exact (`value !=
origin`). Milestone 5.

### 9.5 Metrics

Per committed node (losing siblings are not counted): `plain`,
`speculative`, `links`, `reused_ops` (pairs omitted from `F`), `reverted_ops`.
Per instance: `links_sent`, `links_dropped_unused`, `link_failures`,
`fallbacks` (`F` fell back to the plain path after a revert read failed; the
plain path is always available, §10 item 5), `hint_keys`, `hint_new_keys`,
`prefetched_keys`. Figures of merit: `reused_ops / |V|`
(work moved off the `Propose` path) and `reverted_ops / sent` (waste); the
benchmark reports both per policy and per `rounds`.

## 10. Storage engine abstraction (R10, R13)

```rust
pub trait ReadView<K, V>: Send + Sync {
    fn root(&self) -> Hash;
    fn get(&self, key: &K) -> Result<Option<V>>;       // RevisionStale once outside the window
}
pub trait StorageProposal<K, V>: ReadView<K, V> + Sized {
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self>; // chaining
    fn commit(&self) -> Result<Hash>;                   // once-only; see contract below
    fn is_committed(&self) -> bool;                     // drives StorageView::propose (item 4)
}
pub trait StorageEngine<K, V>: Send + Sync + 'static {
    type Proposal: StorageProposal<K, V>;
    type Revision: ReadView<K, V>;
    const ENGINE_ID: &'static str;                      // in the root preimage (§7.3)
    const ROOT_IS_STATE_FUNCTION: bool;                 // required for speculation (§9.3)
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool;         // each proposal of a chain is its own commit (firewood)
    fn revisions(&self) -> usize;                       // engine window, for the §8 check
    fn root(&self) -> Hash;
    fn get(&self, key: &K) -> Result<Option<V>>;
    fn revision(&self, root: Hash) -> Result<Self::Revision>;
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self::Proposal>;
    fn prefetch(&self, view: ViewRef<'_, K, V, Self>, keys: &[K]) -> Result<()>; // §9.4; default: get per key, engine-typed view
    fn prune(&self) -> Result<()> { Ok(()) }            // pass-through hooks, default no-op
    fn close(&self) -> Result<()> { Ok(()) }
    fn prove(&self, root: Hash, key: &K) -> Result<Vec<u8>> { Err(Unsupported) }   // §7.4, milestone 6
    fn verify_proof(root: &Hash, key: &K, value: Option<&V>, proof: &[u8]) -> bool { false }
}
```

**Proposal handle contract.** Handles are shared (`Arc`) between the node
that created them, their children's pinned views and speculative chains,
hence `commit(&self)`; the adapter owns the once-only state. An adapter must
provide:

1. `commit` commits this proposal and any uncommitted ancestors, oldest
   first, and returns the committed root. A second `commit` returns an error.
   `COMMITS_CHAIN_LINKS_SEPARATELY` says whether those ancestors become their
   own revisions (firewood) or fold into one record (QMDB).
2. After `commit`, the handle `commit` was called on keeps working as a view
   of the committed root (`get`, `root`; the adapter re-bases to
   `engine.revision(root)` internally), and returns `RevisionStale` once the
   engine's window has moved past it. Implicitly committed ancestors (chain
   links) need not remain readable: reads through them may return an error,
   never wrong data.
3. Committing a child whose parent handle has already been committed
   succeeds (firewood: re-parenting onto the new head; `qmdb-ffi`: tip-child
   re-basing). The check an engine performs is therefore "the chain's base,
   after committing its uncommitted ancestors, is the committed state", not
   "this proposal's base is the committed state".
4. `propose` after `commit` is **not** required, but it must return an
   error, never panic: the hint path can race a commit between its
   `is_committed()` check and the call (§9.2). auth_wal never relies on it
   succeeding. The pinned-view dispatch (`StorageView::propose`)
   chains on a `Proposal` view with `!is_committed()` and routes a `Revision`
   view or a committed `Proposal` view through `engine.propose`. The decision
   uses `is_committed()`, never a root comparison: an uncommitted proposal can
   be a storage no-op whose root equals the engine's. Routing is valid because
   **auth_wal is the engine's sole writer**, so the committed node's
   `storage_root` always equals `engine.root()`; on the `Propose` path the
   dispatch returns `ProposalOrphaned` if it does not, on the hint path it
   drops the link (§9.2); it never panics.
5. `propose` is **side-effect free**: a proposal that is built and dropped
   leaves no trace on the committed state or on the root of any future
   proposal. Speculative links rely on it; milestone 4 verifies it on
   firewood.

**Firewood** (the backend of record). Its root is the Merkle trie root of
the stored state (`ROOT_IS_STATE_FUNCTION = true`); a proposal is an
in-memory hashed overlay on its parent, so proposing a link hashes the
predicted victims' paths immediately, which is the precomputation §9 counts
on. Committing a proposal whose parent is not the latest revision fails
(`NotLatest`), so a chain commits one proposal at a time
(`COMMITS_CHAIN_LINKS_SEPARATELY = true`) and firewood re-parents the
committed proposal's children onto the new head. Firewood never reaps a
revision that is still referenced: a pinned view does not go stale, the
window stalls instead, and the K-ring (plus the adapter dropping revisions)
bounds memory; the window is opened at `K × (1 + rounds)` (§8). There is no
dedicated warm-path call; `prefetch` reads each key, which warms only under
`CacheReadStrategy::All`, so `New` sets that strategy (§9.4). Adapter
mechanics the milestone-4 work must honour: (a) `Proposal<'db>` borrows the
database and `commit(self)` consumes it, so the adapter keeps a `'static`
database (leaked or pinned, as the `firewood-ffi` crate effectively does) and
owns the `Proposal` itself, exposing reads through the `'static` view handle
a proposal provides; (b) firewood forgets a proposal at the next commit
unless something holds it, and a proposal has no parent accessor, so the
adapter handle links to its parent handle to commit ancestors oldest-first;
(c) an uncommitted proposal refers to its base revision by hash only, so it
is the node's pinned view (`Arc` to the committed revision) that keeps the
base alive; (d) two consecutive revisions can share a root (a no-op `F`, an
identical-value eviction) and firewood indexes revisions by root, so the
adapter keeps its own `Arc`s and never relies on root lookup for a retained
revision. Milestone 4 also runs the §9.3 tests 1–3 against the real engine.

**QMDB** keeps interface-level support through `qmdb-ffi`'s Rust crate (which
owns the proposal tree and window): `ROOT_IS_STATE_FUNCTION = false`
(commit records and the ops MMR enter the root), `COMMITS_CHAIN_LINKS_SEPARATELY
= false`; speculation is rejected at `New`.

**v0 backend: `MemoryStorage`**, a model of firewood: a persistent map plus
an incremental state-function root (a lattice accumulator over the encoded
`(key, value)` items, §7.2, updated per op so a proposal costs O(batch), not
O(engine)), chained proposals committed one at a time
(`COMMITS_CHAIN_LINKS_SEPARATELY = true`), K revisions, `prefetch` as the
default read-through, and an optional **synthetic per-op latency** knob so
benchmarks can show what speculation buys against a slow engine. A fresh
engine reports `EMPTY_ROOT`.

**Concurrency.** One `RwLock` guards the tree, capacity controller and
statistics. `Propose` and `Commit` take the write lock (`Propose` mutates the
root index); `Get`, `Root` and `Revision` take the read lock; `Prefetch`
takes it only to resolve its target and releases it before any engine call
(§9.2), so a hint never makes `Propose` or `Commit` wait for hashing. Each
node's speculative chain sits behind its own mutex, held only to plan and to
append; `Propose` takes the parent's chain mutex to build `F`. Lock order is
tree lock → node chain mutex → node parent link → engine, and a thread never
holds two chain mutexes. The parent to propose on
is resolved under the lock: `Committed`, or a given node whose parent chain is
walked to a committed node exactly as `Commit` does; if that node is not the
current committed node the parent is orphaned and the proposal is rejected
with `ProposalOrphaned`, which keeps the sole-writer invariant of contract
item 4 a true invariant. No actor thread in v0; the C ABI keeps `qmdb-ffi`'s
handle/keep-alive conventions so one can be added without changing the Go
side.

## 11. Benchmarks (R11)

`auth_wal/benchmark/` is a binary crate modelled on `firewood/benchmark`:
same four workloads, same key derivation (`sha256(row_id)`), same CLI flags
where they apply, plus auth_wal knobs.

| Workload | From firewood | What it stresses here |
|---|---|---|
| `create` | insert N rows, 10k per commit | pure eviction throughput; every batch overflows the set |
| `tenkrandom` | 2.5k deletes, 2.5k inserts, 5k updates per batch over a moving window | mixed Put/Delete, tombstone handling |
| `zipf` | Zipf-distributed updates (exponent 1.2, seeded) | the target workload: hot keys should stay resident; hit rate vs capacity |
| `single` | tiny batch on the same keys every commit | pure in-set update path; storage should see zero writes |

Flags: `--capacity` (default 1M entries, i.e. 100 firewood batches),
`--low-watermark`, `--revisions` (K), `--policy {fifo,lru-write}`, `--vc {lthash,merkle}`,
`--speculate`, `--speculation-rounds`, `--speculation-min-link`, `--hints`
(emulate libevm: call `Prefetch` with the batch's keys in per-transaction
chunks before `Propose`), `--storage-latency-us`, `--zipf-exponent`,
`--seed`,
`--warmup-batches` (zipf), and `--baseline` to run the same workload straight
into the storage engine with no write set in front.

Reported per run: commits and elapsed time (throughput is derived), mean and
p99 propose and commit latency (speculation moves hashing from `Propose` into
the hint path and `Commit`), hint-path time, set occupancy, evictions per
commit, storage ops per commit, reused and reverted ops per commit (§9.5),
peak RSS.
The driver fills the configuration columns; a workload returns measurements
only. Output is a CSV header plus one record per run
so sweeps over capacity × policy × speculation rounds × hints can be plotted directly. A small
`criterion` suite (milestone 3) covers the micro-paths (VC `apply`,
persistent-map `Put`, `select_victims`).

## 12. Open questions

1. **Per-key proofs under lattice hash.** `lthash` authenticates the set, not
   individual entries. Either accept set-level authentication for v0 or move
   the default to a proof-capable scheme once `kzg` exists.
2. **KZG slot mapping.** Slot count vs capacity, and the collision policy
   (chain into the slot value, cuckoo-style displacement, or reject).
3. **Read-aware policies.** Ruled out in v0 for determinism; would need the
   adapter to signal execution-time reads separately from RPC reads.
4. **Eviction cadence.** Bulk eviction to a low watermark vs. a fixed count
   per block; the benchmarks decide before parameters are fixed.
5. **Round-0 estimate.** Without hints the first link relies on a running
   estimate of new keys per block (§9.2); its form (EWMA, median) and whether
   round 0 should wait for the first hint instead are benchmark decisions.
6. **Durability and bootstrap.** The set and policy state are consensus
   state. A real write-ahead log of committed batches replayed into the set
   (which is why the layer carries the name), or a replicated snapshot, is
   needed before any validator may restart or state-sync. Bootstrap must
   restore `height`, the set and the policy state so that `Root()` after a
   reopen equals the last committed root (as `qmdb-ffi`'s reopen test
   checks); until then `New` requires a fresh engine (§1).
7. **Policy digest cost.** One extra hash per write; acceptable for v0, but
   a scheme that already commits `(key, height)` could subsume it.
8. **Capacity controller state.** When an adaptive controller arrives, its
   inputs must be digested into the root or be a pure function of the
   committed node (consensus-state rule, §1). The effective capacity and
   watermark it produces are what `apply` and the preimage use.
9. **Speculation on QMDB.** Rejected in v0 because QMDB's root covers the
   op history, so reverts do not restore the root. A QMDB variant would need
   the structure to be a pure function of (parent, batch), as an earlier draft
   had; deferred with QMDB itself (R13).
10. **Firewood-side TODO** (R15; tracked in `docs/TODO.md`). Confirmed
   against the source (`manager.rs` `commit`, `nodestore` `parent_hash_is`):
   firewood lets proposals be built on uncommitted proposals but commits one
   proposal at a time; committing a child of an uncommitted proposal fails
   with `NotLatest`, so a chain costs one revision per link and firewood
   recomputes intermediate hashes along a path once per link that touches it
   and once more when `F` reverts a key a link wrote. Accepted for now with
   pessimistic prediction (§9.2). Future firewood changes, in order of value:
   (a) a cascade commit that commits `Proposed` ancestors oldest-first inside
   one critical section, removing the caller-side ordering and the mid-chain
   failure window; (b) a squash commit that merges a chain's node stores into
   one revision, removing the window-sizing cost; (c) hashing deferred or
   shared across a chain so a path touched by several links is hashed once;
   (d) a native warm-path call replacing read-through prefetch.
11. **Hint semantics.** Account hints mean "written or touched" (§9.4); if
   the inflation of `new` matters, `Prefetch` gains a read/write flag or the
   hook filters touched-only accounts.
12. **Lattice-hash parameters.** 1024 × u16 lanes with a BLAKE3 XOF (§7.2)
   follows published LtHash practice, but both the set commitment and the
   policy digest are consensus-critical; the collision-resistance margin of
   this parameter set should be reviewed before a network relies on it.

## 13. Layout and milestones

```
auth_wal/
  Cargo.toml                  workspace root; lib (rlib + staticlib), features: lthash, merkle, kzg, ffi
  src/lib.rs                  AuthWal<K, V, C, P, S> handle over Arc<Shared>, Config, Stats
  src/types.rs                Key/Value bounds, Hash, Digest, EMPTY_ROOT, BatchOp<K, V>, Error
  src/set.rs                  WriteSet, Entry<V>, PersistentMap<K, V> (ordered), Evicted
  src/policy/{mod,digest,fifo,lru_write}.rs
  src/capacity.rs             CapacityController, Fixed (§6.4)
  src/vc/{mod,lthash,merkle,kzg}.rs
  src/root.rs                 root composition (§7.3)
  src/lattice.rs              lattice-hash accumulator and item encoding shared by lthash, the policy digest and MemoryStorage
  src/proof.rs                AuthProof shape for authenticated reads (§7.4)
  src/storage/{mod,memory}.rs  (milestone 4 adds firewood.rs)
  src/proposal.rs             Node, StorageView, Proposal/Revision handles, ProposalTree
  src/speculate.rs            SpeculativeChain, Speculator (on_hint / after_commit / finish / record) (§9)
  src/ffi/                    C ABI, cbindgen -> auth_wal.h (names auth_wal_*), feature `ffi`
  *.go                        package ffi, mirrors qmdb-ffi files
  benchmark/                  workspace member; firewood-style driver
    src/{main,create,tenkrandom,zipf,single,report}.rs
  docs/                       this directory
```

Milestones

1. Core library: WriteSet, FIFO + LRU-write policies, lthash and the
   `merkle` reference scheme, MemoryStorage, proposal tree, plain path only,
   unit tests shared across policies and schemes.
2. Speculative eviction and prefetch hints (§9) with the §9.3 tests and §9.5
   metrics.
3. Benchmark crate: sweep over capacity × policy × speculation rounds × hints,
   with the engine-only baseline.
4. Firewood backend behind `StorageEngine` (§10), including the contract
   checks (items 1–5) and the §9.3 tests 1–3 against the real engine.
5. C ABI and Go `ffi` package with `Prefetch`; port `qmdb_test.go`; build
   Avax-pEVM against it with the import swapped in the qmdb adapter port; the
   libevm/adapter hint hook (§9.4).
6. KZG scheme and `ProveLatest`; QMDB backend (speculation off).

## 14. Review log

v1 (2026-10-25) addressed: target adapter and full symbol list; height in the
preimage for unique per-block roots; fixed explicit low watermark; policy
digest in the preimage; pinned storage views and `commit(&self)`; sibling
rejection by pointer equality; weak root index with `Drop`; `PrefixDelete`
semantics and prefix type; ordered persistent map; scheme instances carrying
parameters; speculative-eviction determinism invariant, `ROOT_IS_STATE_FUNCTION`,
relaxed hit rule and keep-across-commits; metadata handling; concurrency
model; benchmark baseline, seed and warm-up; restart/bootstrap as a v0
limitation.

v2 (2026-10-25) addressed: speculation consumed at `Propose` (now a pure
function of the committed node, replaced only at commit, shared by siblings);
`PrefixDelete` excluded from the chained path; parameter digests for scheme
and policy; the consensus-state rule; the engine handle contract (post-commit
reads, child-of-committed commits, no post-commit `propose`, sole-writer
invariant); `CHAIN_COMMIT_COST`; `Drop` of committed handles and index
pruning; length-prefixed preimage strings and `None`/`Some(empty)` metadata;
`height` definition; failed materialisation halts rather than diverges;
lock order and parent resolution under the lock.

v3 (2026-10-25) addressed: speculation anchored on the committed node was
still validator-local for nodes proposed on uncommitted parents; §9 now
defines the storage structure as a function of (parent, batch) and reduces
the prefetcher to a cache, with `chaining` as the consensus parameter and
`prefetch` local; early-copy invariant replaces the subset rule; `engine_id`
in the preimage; `Close` mirrors `qmdb-ffi`; engine window opened at
K × cost; firewood pinned-view and lifetime notes; lazy root index instead
of eager unregistration; incremental MemoryStorage root; per-node storage
path for metrics; failure of pre-materialisation is a cache miss, so no
poisoning.

v4 (2026-10-25) addressed: `likely_victims` made an explicit consensus-critical
pure function covered by `digest`; metadata never on proposal 1; chaining cost
bound and default-off; orphan detection on `Propose`; cache keep rule includes
the base root; `prefetch_stability` semantics; `revisions` flag; differential
and policy-purity tests; terminology (`S`, one commit per storage proposal,
window qualifier); §10 item 2 scoped to the committed handle; §12.5 reworded.

v5 (2026-10-25) addressed: fresh-engine guard at `New` and bootstrap
requirements; `propose` side-effect freedom as engine contract item 5;
lattice item encoding and endianness; K-ring includes the head; root-index
wording; `ProposalOrphaned` surfaced in §3; `commitWithSnap` attribution;
`on_write` per op; prefetcher lock-hold note; `Revision(EMPTY_ROOT)` scope;
`prove`/`verify_proof` in the §10 listing; effective parameters as a function
of the parent before adaptive capacity; lattice parameter security as an open
question; `merkle` in milestone 1. Both reviewers declared the document ready
for milestones 1–3 with no further full round warranted.

v6 spot-check (2026-10-25): `is_committed` on `StorageProposal` drives the
pinned-view dispatch (a root comparison cannot); `Key: Borrow<[u8]>` for the
prefix range scan; genesis preimage records the effective parameters; one
root-index sweep rule; `merkle` leaf addressing fixed (SHA-256(key), depth
256, no slots). Both spot-checks found nothing further; no more rounds.

v7 (2026-10-25), from the project owner's clarifications (00, R13, R14):
speculative eviction reworked around firewood: predicted victims go out as a
chain of links refined during execution, and the final proposal reverts
mispredictions so storage ends exactly as the plain path; speculation is
therefore local (no `chaining` consensus flag, nothing of it in the
preimage), gated on `ROOT_IS_STATE_FUNCTION`; `likely_victims` demoted to a
local heuristic with a hint-aware variant; "prefetch" now means the explicit
`Prefetch(keys)` hint API that warms trie paths and feeds speculation, with
its libevm integration point; firewood facts and QMDB status in §10;
`COMMITS_CHAIN_LINKS_SEPARATELY` replaces `CHAIN_COMMIT_COST`; milestones
reordered (firewood backend before the Go shim).

v8 (2026-10-25), from the v7 review: chains only on the head or live
non-orphaned proposals, dropped on head change or orphaning (hints elsewhere
only warm); link hashing outside the tree lock and never on the `Propose`
path, round 0 after the commit lock is released; prediction formula fixed and
FIFO's override made mandatory; `max_links_total` budget; mid-chain commit
failure poisons the instance; §9.4 integration rewritten around a direct
finalise-time hook (the trie prefetcher is disabled for these schemes) and
firewood's read-cache strategy forced at `New`; firewood adapter mechanics
(a)–(d); `fallbacks`/`link_failures` metrics; nits (key-wise difference,
diagram, §11 wording, §12 numbering, op-log model dropped).

v9 (2026-10-25), from the v8 targeted review: one in-flight link per chain
with append conditioned on an unchanged tail (concurrent hints would
otherwise fork the chain and make `F` omit writes); the hint path releases the
tree lock before any engine call and soft-checks the engine root for round 0;
budget reserved at plan and refunded on every drop, with a weak registry of
chain holders swept by `advance`; `Close` waits for in-flight links and is
allowed when poisoned; speculative path only with a completed link; firewood
constructor owns the read-cache override (§3 deviation); the finalise hook
fires regardless of `withPrefetch`; concurrent hints added to test 2.

v10 (2026-10-25), from the v9 spot-check: budget units are owned tokens
(refunded exactly once on drop; the chain owns completed links' tokens, the
hint task the in-flight one); the budget check and sweep happen in the
resolve step under the tree read lock, never inside a plan, one chain mutex
at a time; registry pruning only in `advance`; sticky retirement of chain
slots; in-flight counting under the read lock that reads the closing flag;
budget invariants in test 2; error-not-panic for `propose` on a committed
handle stated in §10 item 4; listing and wording nits. Both spot-checks
declared the design and skeleton ready for milestones 1–2.

v11 (2026-10-26), project owner's decision after the firewood commit check:
pessimistic prediction with a `margin` knob (R15, §9.2) and the firewood-side
TODO list (§12.10, `docs/TODO.md`).
