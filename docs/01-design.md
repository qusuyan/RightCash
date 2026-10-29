# auth_wal design

Status: DESIGN v27 (2026-10-29). Derived from `00-project-description.md`
including its clarifications (R13–R28); v1 to v6 incorporate five review
rounds plus a spot-check (see §14); v7 reworked §9 and §10 for the
clarifications, v8 to v10 incorporate their reviews, which declared the design
ready for milestones 1–2; v11 records the pessimistic-prediction decision
(R15); v12 records milestone 0 (benchmark suite, in-memory model and firewood
backend implemented); v13 records the first micro-benchmark findings (lattice
expansion changed to hash-then-ChaCha8); v14 rebuilt the data model on
in-place structures with a small default capacity (R16); v15 incorporates its
review (replay-equivalent policy queries, overlay invariant, undo-log reads
for retained and orphaned nodes); v16 makes precomputation asynchronous
(R17: a worker thread runs every speculative engine call, link proposals and
warming); v17 makes the committed set an immutable snapshot copied per
commit, drops the undo logs and takes every lock off the read path (R18);
v18 replaces the remaining coordination with ownership (R19: writer
operations take `&mut self`, link slots are filled once by the worker, nodes
are immutable and list their uncommitted ancestors), leaving the registry
mutex and the firewood handle lock as the only locks; v19 makes every
proposal a full copy of its parent's set (R20), which removes overlays, chain
reads, ancestor lists and the replay-through-chain policy queries, and strips
the code of what the simplification made redundant; v20 removes the derived
state the owner asked about (R21): the node's ownership flag and metrics
record, and the registry's head field, order list and revision count (the
K-ring is one deque in commit order, the live index a flat list); v21
(R22) keeps only what consensus and the interface require: no lineage walk
(`Commit` checks one link, `Propose` builds on the head for an equal root),
no ring clearing at close, no prefix-delete support in the set (rejected as
QMDB rejects it), and the policy state covered by the set commitment's items
instead of a second accumulator (the v21 review found that the adapter stages
children by root and needs equal roots to be interchangeable parents); v22
(R23) makes the capacity the snapshot's and how much to evict the policy's
rule (a low watermark is the v0 policies' parameter, unknown to the trait),
and drops the chain-carrier test; v23 (R24) replaces the per-node chains of
link slots by one precomputation: the worker builds every link on its latest
view in queue order, nothing is cancelled, and `Propose` waits for the queued
links (amending R17 for `Propose` alone); v24 (R25) moves the prediction
into the eviction policy (`writes` evicts and reconciles the policy's own
marks, `hint_writes` says what to send early and what to put back), makes
the policy state a root term again through `state_digest` (a full hash per
proposal), and reduces the speculation machinery to a pending queue, a
worker and a flush behind one lock; v25 (R26) keeps the policy in the
snapshot and lets hints mark it in place (no working copy, `apply` takes
only the batch), reduces the marks to the marked set, makes `writes` answer
the victims plus one last `Prediction`, drops warm jobs (a step is the
warming) and renames the speculative proposals steps; v26 (R27) makes the
worker the owner of the speculative side of the engine, with a queue of
submitted ops merged into one write set per step and an interface of
`submit`, `finish` (block end: wait for the queue and take the latest view)
and `reset` (a retarget); v27 (R28) puts the worker on a `std::sync::mpsc`
channel: the queue is the channel, the chain state is the thread's locals,
and no lock of ours is on the path.
The core (§4–§9) is still a skeleton whose bodies are `todo!()`.

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
- Roots are **history-bound**: they include the block height and the
  policy's state digest (its eviction order, by write height for the v0
  policies, §6.1, §7.3), so the same key-value contents reached by different
  histories yield different roots whenever the eviction order differs. This is the same property QMDB has and
  the qmdb adapter port relies on (§3, empty blocks; §7.3, staging by root).
- **Consensus-state rule.** Anything that can influence a future root is
  either digested into the current root (set contents, the policy's state
  digest, capacity, scheme, policy and engine parameters, §7.3) or a pure
  function of state that is.
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
 │  Proposal tree: one full copy of the set per proposal (snapshot), one weak parent link    │
 │   ┌─────────────────────┐    ┌──────────────────────┐    ┌───────────────────────────┐    │
 │   │ Snapshot (BTreeMap) │    │ EvictionPolicy       │    │ VectorCommitment          │    │
 │   │ + policy state      │    │  writes()            │    │  apply(key, old, new)     │    │
 │   │                     │    │  hint_writes()       │    │  commitment()             │    │
 │   │                     │    │  state_digest()      │    │                           │    │
 │   └─────────────────────┘    └──────────────────────┘    └───────────────────────────┘    │
 │                                   │ victims / predictions / prefetch hints                │
 │                                   ▼                                                        │
 │          eviction + precomputation (worker thread) ──────► StorageEngine proposals        │
 └────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                              ▼
                              StorageEngine trait: Memory model (M0) | firewood (M0) | QMDB (interface only)
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
| `New(dir, NodeHashAlgorithm, opts...)` | Opens the storage engine (all firewood/qmdb options pass through to it; for `MemoryStorage` the directory and hashing algorithm are accepted and ignored) and an empty write set. New options: `WithSetCapacity(n)` (consensus) and `WithLowWatermark(n)` (handed to the v0 policies, whose parameter it is; a policy without one ignores it, §6.2), `WithEvictionPolicy(name)` (default `lru-write`, §6.1), `WithCommitmentScheme(name)` (default `lthash`), `WithStorageEngine(name)` (default `firewood`), and the local `WithSpeculation(bool)`, `WithSpeculationRounds(n)`, `WithSpeculationMinStep(n)`, `WithSpeculationMargin(n)` (§9.2). `WithRevisions(K)` sizes auth_wal's K-ring; the engine is opened with a window of `K × (1 + rounds)` when speculation is on and the engine commits chain links separately (firewood), else `K` (§8, §10). The Rust core is statically generic (§13); the names select one of a fixed list of monomorphizations behind an enum, and an unknown combination is rejected at `New`, as are (`InvalidConfig`) `capacity = 0`, `revisions < 2` (§8), speculation on an engine whose root is not a state function (§9.3), `rounds = 0` or `min_step = 0` with speculation on, and an engine window smaller than the one above; `EngineNotFresh` if the engine already has committed state (§1); a policy that leaves the set over capacity (for the v0 policies `low_watermark > capacity`) is a contract violation caught at the first proposal that evicts (§6.2). Default `revisions` is 128. |
| `Update(batch)` / `UpdateWithMetadata` | `Propose` + `Commit` in one call. |
| `Propose(batch)` / `ProposeWithMetadata` | Copies the parent's set and applies the batch to the copy (§4), the policy evicts if over capacity (§6.1), creates the storage proposal for the evicted writes, returns a `Proposal` whose `Root()` is known immediately (§8). `Root()` is a single 32-byte commitment combining the set commitment, the policy digest and the storage root (§7.3); none of them is exposed separately. Metadata is root-affecting (it enters the preimage; `None` and `Some(empty)` are distinct, as in `qmdb-ffi`) and is also carried by the node's own storage proposal when one exists (on the speculative path, the final proposal; steps never carry metadata, §9.1). |
| `Proposal.Get` | Its snapshot, then its pinned storage view (§5); one lookup, no lock on the set side (the storage read takes the adapter's handle read guard, §10 (e)). |
| `Proposal.Propose` / `ProposeWithMetadata` | Child of a proposal (the adapter chains during block building); in Rust `AuthWal::propose(&mut self, Some(&parent), batch, metadata)`, a writer operation (threading contract below). A handle whose node has since committed behaves as the committed state while that node is the head (equal root, §8); any other node is proposed on as given, as `qmdb-ffi` and firewood accept, and a child of a node that can no longer be committed is itself never committable (`Commit` reports `ParentUncommitted` or `ProposalOrphaned`, §8). `ProposalOrphaned` can still come from the engine: a storage proposal on a committed handle whose root the engine has moved past (§10 item 4). |
| `Proposal.Commit` | In Rust `AuthWal::commit(&mut self, handle)`: it takes the writer exclusively and consumes the handle, on success and on failure alike, so neither a second commit of the same proposal nor two overlapping commits can be written (R19); after a storage failure the adapter re-proposes the block, and a retry of the same Go handle reports `ProposalConsumed` (§8). Requires the parent to be the head node by identity (§8; `ParentUncommitted` otherwise, as firewood's `NotLatest`, an equal-root twin of the head included; the adapter only ever commits a child of the committed tree root, and qmdb-ffi's cascade commit is not offered, R20); rejects a proposal whose parent was dropped uncommitted (`ProposalOrphaned`, §8). A storage failure leaves the head where it was, returns the error, and poisons the instance if the node's proposal chained on speculative steps (§9.2); the handle is consumed on every outcome, a pre-check failure included (§8). Retargets the precomputation to the new head, unless it already targets it or a live child of it (§9.2), planning no step (the first hint plans) and never waiting for the worker (R17); its own engine commit may wait for a step still being hashed on a handle it commits (§9.2). |
| `Proposal.Drop` | Releases the handle's reference; the node, its snapshot and its storage proposal are freed when the last reference goes (the K-ring may hold one, §8; the precomputation refers to its target weakly, and the worker's chain keeps the steps, and through them the target's pinned view, alive until the next retarget or `Close`, §9.2). |
| `Prefetch(keys)` on `Database`, `Proposal`, `Revision` | **New (R14).** Hint that execution reading through this view is writing `keys`. The keys go to the policy's `hint_writes` when the node is the precomputation's target, retarget it first when the node is a live child of the target (or, with no live target, the head or a live child of it), and otherwise do nothing (§9.2; no engine warming, R26: the steps are the warming, §9.4). Returns after the policy call: the engine work runs on the instance's worker thread (R17, §9.2). A writer operation in Rust (`AuthWal::prefetch(&mut self, view, keys)`, R19) and a writer call on the database handle in the C ABI, which takes the writer mutex (threading contract below): the hint writes the target's policy. Advisory: a wrong hint costs work, never correctness. |
| `Get` | The head's snapshot, then its pinned storage view (§5). The head is resolved under the registry lock, one hold of about 15 ns (§10); the set lookup takes no lock, the storage read the adapter's handle read guard (§10 (e)). |
| `Root` | Combined root of the committed state (§7.3). `EmptyRoot` is the Ethereum empty-root constant, as in `qmdb-ffi`, reported exactly while no proposal has been committed, so the adapter's genesis idiom (`Initialized()`, `Revision(EmptyRootHash)`) keeps working. |
| `Revision(root)` / `LatestRevision` | Resolves the last K committed revisions, then live proposals, under one registry hold; in Rust the result is the same `Handle` type a `Propose` returns (§8). `ErrRevisionNotFound` for an unknown root; `ErrRevisionStale` from `Revision.Get` only when its pinned storage view has left the engine's window: every node owns its snapshot, so set reads never go stale, and an orphan reads its own copy exactly, however many commits later (§5, §8). |
| `Prune`, `GatherMetrics`, `Gatherer`, `StartMetrics*` | Pass-through / composition. |
| `Close(ctx)` | Mirrors `qmdb-ffi`: the Go layer waits for its outstanding `Proposal`/`Revision` handles until `ctx` expires (its own keep-alive count, as in `qmdb-ffi`'s `keepalive.go`), then returns `ErrActiveKeepAliveHandles`; otherwise it calls the Rust `AuthWal::close(&mut self)`, a writer operation, which sets the closing flag (`Propose`, `Commit` and `Update` report `Closed` from then on; hints are no-ops), drops the worker's channel and joins it (the step being built finishes, queued requests are handled and nothing more is built, §9.2; the thread's chain, the only speculative engine proposals, drops with it; the writer holds none), and closes the engine through the writer's exclusive reference, which succeeds because committed proposals and revisions do not pin it and reports `ActiveHandles` if a Rust caller still holds a handle to an uncommitted proposal with its own storage proposal (§10 item 6). Reads through handles and `Reader`s keep working afterwards, retained revisions included (a snapshot is memory, a committed engine view stays readable; the Rust side keeps no closed flag for reads, R20; the registry is released with the instance, R22), and `Root` keeps answering with the last committed root. Idempotent; allowed on a poisoned instance. |
| `BatchOp`: `Put`, `Delete`, `PrefixDelete` | `Put`/`Delete` land in the set. `PrefixDelete` is rejected by `Propose` (`PrefixDeleteUnsupported`), as QMDB rejects it behind qmdb-ffi: a bounded set cannot apply a prefix delete to the keys it does not hold, forwarding it would need a storage proposal and ordering rules of its own, and the adapter never emits it (it bumps storage generations instead; R22). The engines keep supporting the op (§10). |
| Proofs (`ProveLatest` / `VerifyLatest`) | Milestone 5 (§7.4); nothing of it exists in v0 code. Not used by the adapter. |
| `NodeHashAlgorithm`, `CacheStrategy`, legacy options | Accepted; forwarded to a firewood backend, ignored otherwise, exactly like `qmdb-ffi`. |

**Threading contract (R19).** The Rust core separates the one thread that
changes state from the threads that read it, and lets the type system do the
serialising. `AuthWal` is the **writer**: `Propose`, `Proposal.Propose`,
`Commit`, `Update`, `Prefetch`, `Prune` and `Close` are methods taking `&mut
self`, so two of them can never overlap and no runtime lock is needed to keep
them apart; `Commit` also takes the `Proposal` by value, so a committed handle
cannot be committed twice or read as a proposal afterwards (the committed
state is read through `Get` or `LatestRevision`). `AuthWal::reader()` returns
a `Reader` (`Clone + Send + Sync`) for the RPC and mempool threads: `Get`,
`Root`, `Revision`, `LatestRevision` and `Stats`. Go's `Proposal` and
`Revision` are one Rust type, `Handle` (§8), `Send + Sync` and read-only
(`Get`, `Root`); handles are returned by the writer and resolved by
readers, and hold nothing but the node. `Prefetch` on any of the three Go
objects is a writer operation (planning writes the precomputation, §9.2),
so in Rust it is `AuthWal::prefetch(&mut self, view, keys)` with the handle
as `view` (`None` for the committed state). The writer has four state-changing
methods: `propose(parent, batch, metadata)`, `commit(handle)`,
`update(batch, metadata)` and `prefetch(view, keys)`, plus `prune` and
`close`; the Go names map onto them. The one shared mutable structure left is the
registry (§8), behind a mutex whose every hold is listed in §10. The C ABI
(milestone 4) cannot express `&mut`: it keeps the writer behind a mutex that
every writer call takes and a `Reader` beside it that reader calls use with
no lock, so the Go package keeps `qmdb-ffi`'s surface unchanged. `Propose`,
`Commit` and `Close` come from the one block-processing goroutine and never
contend; `Prefetch` can arrive from several threads (parallel executors
finalising transactions, a tracer finalising on an RPC thread, §9.4), each
hint taking the writer mutex for its policy call (microseconds to a few
milliseconds, §9.2) and waiting if a `Propose` or `Commit` holds it. Hints
are advisory, so that wait costs time, never correctness, and a hint on a
node other than the precomputation's target does nothing (§9.2), so the
wait only ever delays a target hint.

**Empty blocks.** A block that writes nothing still gets a new root, because
the height is in the root preimage (§7.3). This matches QMDB's "every commit
changes the root" and is what the qmdb integration relies on: coreth's
`commitWithSnap` (`core/blockchain.go`, patched per `qmdb-ffi/docs/03` P3)
assumes a child's root never equals the parent's, so it always has a proposal
to commit, and the adapter port's `createProposals` de-duplicates candidate
proposals by root. Post-commit `Root()` equals the proposal's
`Root()`.

Deviations relative to firewood are the `qmdb-ffi` list plus: roots depend on
the set capacity, the eviction policy with its parameters (the v0 policies'
low watermark), the commitment
scheme and the storage engine, which are therefore consensus parameters and must be identical
on every validator; proposing on a node that can no longer be committed is accepted, its child never commits; `New`
requires a fresh engine (v0); `Prefetch` is an addition; on firewood the
engine's read-cache strategy is forced to cache all reads (§9.4), overriding
`WithReadCacheStrategy`. Speculation and its rounds are local settings.

## 4. Data model (R2, R7, R16, R18, R20)

```rust
pub trait Key:   Clone + Eq + Ord + AsRef<[u8]> + Borrow<[u8]> + Send + Sync + 'static {}
pub trait Value: Clone + Eq + AsRef<[u8]> + Send + Sync + 'static { fn from_bytes(&[u8]) -> Self; }

enum Entry<V> { Value(V), Tombstone }               // Tombstone = pending delete

/// The whole write set at one node: entries, policy state and commitment at
/// `height`. Immutable once built. Every proposal is a fresh copy of its
/// parent's snapshot with the batch applied (R7 taken literally, R20); the
/// node that was committed owns the committed set; nothing is ever mutated
/// after it is shared.
struct Snapshot<K, V, C, P> {
    entries:    BTreeMap<K, Entry<V>>,              // ordered: canonical iteration
    policy:     Mutex<P>,                           // policy state as of this node (§6.1), copied with the entries; hints mark predictions in it (§9.2, R26), the one mutable thing in a snapshot
    policy_digest: Digest,                          // policy.state_digest() as of this node, a root term (§7.3)
    vc:         C,                                  // scheme instance (parameters, §7.1; a KZG setup belongs behind an Arc, §12.2)
    commitment: C::State,                           // incremental commitment of `entries`
    height:     u64,
    capacity:   usize,                              // in force at this node (§6.2; per-node state once it adapts, R4)
}
```

**Why a full copy per proposal (R16, R18, R20).** The structure was decided
three times, each time by the micro-benchmark (`benches/lattice.rs`, group
`cow_base`; the removed groups' figures are recorded in §12.13). At 500k entries a persistent
ordered map cost about 7 µs per insert into a clone, against firewood's own
propose at about 1.6 µs per op, so R16 capped the default capacity at 10k
entries. At 10k, an in-place committed map plus the undo record it needs for
reads at older heights cost 1.4 ms per block, a full clone of the map plus
the same 2k writes 0.94 ms with `Arc` values, so R18 made the committed set a
snapshot copied once per commit, with per-proposal overlays in between. The
overlays bought a cheaper `Propose` (a few thousand inserts instead of a
copy) at the price of chain reads, a base snapshot and an ancestor list on
every node, and the replay-equivalence machinery that let the eviction
policy answer through a chain of overlays what the committed state would
answer after the chain committed (v15 to v18, §14). R20 removes all of it:
**every proposal is a complete copy of its parent's set with the batch
applied directly**, policy state and commitment included. Measured per
proposal on a 10k set with 2k writes (1k updates, 1k inserts): clone the
entries and the FIFO state and apply the batch, 1.9 ms; with LRU-write state,
2.2 ms; the bulk eviction of half the set that follows an overflow, about
2.2 ms more, once every few blocks (§6.2). A commit copies nothing. Against
firewood's 30–90 ms commit (§11) that is noise, and it buys: one lookup per
read, no chain, no base, no ancestor list; eviction
computed on the proposal's own materialised state, exactly as they would be
on the committed state, so a block proposed on a live parent and the same
block proposed after the parent committed produce the same root by
construction (the v15 round-10 bug class cannot exist); and a `Commit` that
installs a pointer. The policy's two per-key maps are the larger half of the
copy; §12.14 lists the levers if the copy ever matters.

- **Ordered snapshot.** `BTreeMap` gives canonical iteration order on
  every node (no hasher to seed, no hash-flooding surface). At the default capacity of 10k entries a
  lookup is about 130 ns.
- **Update in place.** A write to a key already in the set replaces the entry
  and leaves the size unchanged; this is the whole point of the layer.
- **Deletes are tombstones** until evicted; eviction turns a tombstone into a
  `Delete` batch op for storage. Tombstones count toward capacity and are
  present entries for the policy: a `Delete` of an absent key inserts one
  (a write at this height for the policy), and a `Put` over a tombstone is a write to
  a present key (FIFO keeps its height, LRU-write moves it).
- **Capacity is counted in entries**, not bytes, in v0: `entries.len()`,
  so a byte-based capacity can replace it later (§12.8).
- **Height as the clock.** Policies and the commitment never read wall-clock
  time. `height` is the only notion of time: the genesis node (nothing
  committed) has height 0 and every proposal is `parent.height + 1`. It counts
  proposals since genesis (v0 always opens at genesis, §1; a later bootstrap
  must restore it, §12.6). The chain's genesis block is itself committed as a
  proposal, so under the v0 assumption (fresh start, one proposal per block)
  a block at chain height `h` sits at auth_wal height `h + 1`; the adapter's
  own height bookkeeping is Go-side and never reads this value. The `ffi`
  interface does not pass a height. Keys written at the same height
  tie-break by key bytes in eviction order (§6.1).
- **Generic key and value types.** The `ffi` interface carries both keys and
  values as plain byte strings. The core is generic over `K: Key` and
  `V: Value` (blanket-implemented for `Arc<[u8]>`, `Box<[u8]>`, `Vec<u8>`,
  `[u8; N]`; `Key` includes `Borrow<[u8]>` for the memory model's
  prefix-delete range scan (the set has none, R22), which makes `get(&K)` on
  the std maps ambiguous, so the code names the key type at those call
  sites; `Ord` must agree with the byte order of `as_ref()`,
  which the key-byte tie-breaks rely on and every blanket impl satisfies) so
  typed callers, such as a Rust-side adapter with 32-byte keys or fixed-size
  slots for KZG, pay no boxing. Both must be **cheap to clone**: every
  proposal copies every key and value of the set (above), so the FFI layer
  instantiates `AuthWal<Arc<[u8]>, Arc<[u8]>, _, _, _>` (one reference count
  per clone; `Box<[u8]>` values would add about 0.5 ms per copy) and the
  benchmark's auth_wal target (milestone 1) uses the same types.
  `Value::from_bytes` lets a storage adapter rebuild a
  value from the engine's bytes.

**Batch semantics.** Ops apply in order to the copy, each exactly once:
`Put(k, v)` and `Delete(k)` insert the entry (a tombstone for `Delete`), and
the commitment is updated per op with `apply(k, old, new)`, where `old` is
what the copy held before the op, never storage, so the commitment stays a
commitment to the set (R6); a batch containing a `PrefixDelete` is rejected
before anything is applied (§3, R22). The copy's policy is a clone of the parent's taken under its lock, marks
included (§9.2); after the batch it sees the batch's keys at once,
`writes(keys, height, capacity)` (§6.1), the victims it returns are removed
the same way (§6.2), and its `state_digest` is taken for the root (§7.3). This is plain sequential batch
application, as qmdb-ffi and firewood apply a batch, and it replaced the
earlier last-op-wins rule that an overlay needed; every validator sees the
same blocks, so it is deterministic.

## 5. Reads (R5)

Every node of the proposal tree (committed head, live proposal, retained
revision) carries two immutable things (§8): its **snapshot**, the whole set
as of that node (§4), and a **pinned storage view**, the storage proposal
created by the nearest ancestor-or-self that created one (§6.3), otherwise
the committed handle that chain started from (always a proposal handle, §10
item 7).
Neither changes after the node exists, so a read takes no lock of auth_wal's
(the storage read takes the adapter's handle read guard, §10 (e)). Lookup
order for a read on a node `N`:

1. `N`'s snapshot. `Value` is the value; `Tombstone` is absent and stops.
   One lookup, about 130 ns at 10k entries; the same for the head, a live
   proposal at any depth of chaining, and a retained revision. An orphan (a
   sibling committed instead, §8) keeps reading its own copy exactly, however
   many commits later, as `qmdb-ffi` and firewood do: the adapter's
   transaction pool and RPC read the preferred, not yet accepted, block
   between a competing commit and their own reset. Nothing on the set side
   ever goes stale. `Propose` on an orphan is accepted and its child is never
   committable (§8); reads are exact.
2. `N`'s pinned storage view, through the handle the node owns: stable across
   a concurrent commit (§10 item 2: `commit(&self)` leaves it readable and a
   dropped root reads `RevisionStale`, never wrong data). A storage read
   costs microseconds (§11), so the set lookup is never what a read waits
   for.

Reads never mutate policy state. RPC reads differ between validators, and a
read-aware policy would make eviction, and hence roots, non-deterministic. A
read-aware policy would need the adapter to feed only execution-time reads
(§12.3).

## 6. Eviction (R3, R4, R8)

### 6.1 Trait

Policy state lives on the snapshot (§4) and is copied with it: a proposal's
policy sees exactly the written keys of its own batch applied to exactly
its parent's state, and the eviction decision is taken on that materialised
state. There is no query "through a chain": the state a chained proposal
evicts from *is* the state its parent would have committed, so a block
proposed on a live parent and the same block proposed after the parent
committed select the same victims and reach the same root (R20; the
replay-equivalence definition of v15–v18, §14, is no longer needed).

```rust
/// What `hint_writes` answers, and what `writes` answers as its last refinement (§9.2). Local: never in a root.
pub struct Prediction<K> { pub evict: Vec<K>, pub retract: Vec<K> }
/// What `writes` answers (§9.1): `evicted` is the consensus output, `refinement` is local.
pub struct Eviction<K> { pub evicted: Vec<K>, pub refinement: Prediction<K> }

pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {   // Clone: copied with the snapshot (§4)
    /// Consensus-critical. The keys of one batch (batch order; a `Put` or `Delete` each, a write to a present key
    /// included) landed at `height`; evict by the policy's own rule from the set size (it tracks exactly the set's
    /// keys) and `capacity`, remove the victims from the policy's state and return them in eviction order (ties by
    /// key bytes). `evicted` is a function of the consensus state, `keys`, `height` and `capacity` alone. Also
    /// reconciles the marks into the last refinement (`evict`: victims storage lacks; `retract`: marked keys kept)
    /// and clears them.
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K>;
    /// Local (§9.2). A prefetch hint: execution is writing `keys`. Predicts the block's eviction count from the set
    /// size, `capacity`, this hint's insertions (keys absent from the set) and `margin` extra, marks that many likely
    /// victims and returns the newly marked (`evict`) and the marked keys the hint showed will stay, unmarked
    /// (`retract`). Never touches the consensus state.
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K>;
    /// Local (§9.2). Forget every mark: the precomputation was reset, nothing marked is in its storage.
    fn reset_hints(&mut self);
    /// Digest of the consensus state, a root term (§7.3); never covers the marks.
    fn state_digest(&self) -> Digest;
    fn name(&self) -> &'static str;
    fn params_digest(&self) -> Digest;          // the policy's own parameters (zeros if none); v0: SHA-256("auth_wal/policy-params" ‖ low_watermark u64 LE)
}
```

`writes` is consensus-critical: it decides what leaves the set, so it may
not use randomness, call history or the marks; the root commits to the
state it reads through `state_digest` (§7.3, R25). `hint_writes` only
steers speculative eviction (§9), whose mispredictions are always put back,
so it may be heuristic; it should still be a cheap, deterministic function
of the state and the hint sequence so that speculation is reproducible in
tests. Three invariants: the keys the policy tracks are exactly the keys of
the set (relied on by §9: a predicted victim's entry is looked up in the
set); the consensus state (what `writes` selects from and `state_digest`
covers) is disjoint from the prediction state (the marks, which `writes`
clears and `reset_hints` forgets), so a child's policy carries no
prediction and equal digests imply equal future `writes`; and `writes` and
`state_digest` read only that state, `keys`, `height`, `capacity` and the
policy's parameters, every one of which enters `params_digest` (zeros if
none), so the capacity term and `policy_params` cover everything the next
eviction depends on (§7.3). Since the policy also decides how many to
evict, its contract is that the set is within capacity after `writes`;
`Snapshot::apply` asserts it (a violated consensus invariant halts rather
than diverges, §6.2).

Why the policy owns the prediction (R25, clarifications 17–19): it holds
both ends, the likely victims and the actual ones, so reconciling the two at
`Propose` is one pass over its own marks; the writer keeps no sent set and
the worker no dedupe. The marks mean "a step carries this key's entry", so
a retraction a hint returns is executed as an op like an eviction (§9.2) and
the marks keep mirroring storage (§9.1). The marks live in the policy where
it lives, the snapshot (§4, R26): a hint mutates the target's policy in
place under its lock, `apply` clones it marks and all, and a retarget resets
it; nothing is copied aside.

v0 policies (consensus state: a `BTreeSet<(u64, K)>` order and a
`BTreeMap<K, u64>` reverse index; prediction state: the shared `Marks`
helper, the marked keys and nothing else (clarification 22); parameter: the
low watermark, each policy's own with its eviction rule and digest, unknown
to the trait and to the rest of the crate (clarification 23: the R16
default, half the capacity, is the benchmark's and the Go `New`'s
convention, §6.2); cloned with the snapshot, about 0.9 ms of the 2 ms per
proposal at 10k keys, §4):

| Name | Victim order | `hint_writes` behaviour |
|---|---|---|
| `fifo` | Oldest insertion `height` first (a write does not move a key) | Marks the first `E` of the order not yet marked; retracts nothing and excludes nothing, since a hinted in-set key is still evicted under FIFO. Exact for the keys it names once the count is right. |
| `lru-write` | Oldest last-write `height` first (a write moves the key to the back) | Skips this hint's keys (they move to the back) and retracts a marked key among them; exact for those keys, so a retraction is practically never wrong. |
| `lfu-write` (later) | Fewest writes since insertion, then oldest | Noisier |

`E` is the policy's own count: `n` victims for a set of the current size
plus this hint's insertions (its keys absent from the set), plus `margin`
when `n > 0` (§9.2). `state_digest` of both: BLAKE3
`derive_key("auth_wal/policy-state")` over the policy name and the order as
`(height, key)` items (the height fixed-width `u64 LE`, the key
length-prefixed), one pass per proposal (§7.3).

Milestone-1 tests, for both policies: FIFO and LRU-write semantics on small
sequences (a rewrite keeps or moves the key; a delete of an absent key
inserts a tombstone at this height and a put over it is a rewrite; eviction
order with height ties; a batch with a `PrefixDelete` is rejected untouched);
the equal-root property of §7.3 (a child proposed on two equal-root nodes,
the committed node and a live twin, or two live siblings from one batch,
gets one root; under `lru-write` a same-value rewrite and an empty batch
give different roots, under `fifo` the same); the prediction boundary (the
same batches with any hint sequence, including none, give the same
`evicted`, the same `state_digest` and the same roots; §9.3 test 1 is the
same statement end to end); and the same block sequence applied as chained
proposals and as proposals after each commit reaches the same roots (a
pipeline smoke test: the two paths run the same code on the same state).
Policy purity (a cloned state yields the same `writes` sequence) is §9.3
test 5.

### 6.2 When and how much

Eviction runs inside `Propose`, in the policy's `writes` after the batch is
applied to the copy: the policy evicts by its own rule (§6.1); the v0
policies evict down to a **low watermark**, their parameter, once the set is
over capacity; another policy may have no watermark at all. Both the
capacity and the policy's parameters are **explicit, fixed consensus
parameters**: the capacity is the snapshot's (a node term of the root
preimage, and the field a future adaptive capacity would change per node,
R4, §12.8), the policy's parameters enter through `params_digest`, the
preimage's `policy_params` term (R23). The policy's contract is that the set is within capacity after `writes`;
`Snapshot::apply` asserts it on every proposal (for the v0 policies it holds
iff `low_watermark ≤ capacity`), and its determinism is trusted (§6.1). Defaults (R16):
`capacity = 10_000` entries and `low_watermark = capacity / 2`; the default
policy is `lru-write` (its hint exclusion is exact, §6.1), as in the
benchmark. One eviction frees half the set in a single bulk storage proposal
(about 2 ms of set work at 10k, §4, plus the engine's propose), so with
typical blocks writing under 2k entries the next two or three blocks fit
without evicting, and speculation (§9) can precompute the bulk proposal
ahead of time. (An earlier draft derived the watermark from the median batch
size seen locally; that is node-local hidden state and would split
consensus.) The benchmarks sweep both parameters (§11).

### 6.3 Storage proposal for evicted writes

A storage proposal is created for a node iff its eviction was non-empty.
The storage batch is in **canonical order**: the evicted entries sorted by
key bytes (`Put` for values, `Delete` for tombstones). Canonical order keeps the batch reproducible (and matters for
engines whose root covers the op sequence, §10). On the plain path the batch
is proposed directly on the **parent node's pinned storage view**; on the
speculative path a final proposal with reverts and the batch is
proposed on the precomputation's latest view (§9.1). Either
way the resulting proposal becomes the child's pinned view and is committed or
dropped together with it (§8). Nodes without a storage proposal
inherit the parent's pinned view and `storage_root`, so a block that stays
inside the set never touches the engine.

### 6.4 Capacity control (deferred)

v0 has fixed parameters and no controller hook (R20 removed the unused
one). An adaptive capacity (R4, "in the future") is an open question (§12.8):
its effective capacity would have to be a function of the parent node and
enter the root as the node's capacity term already does (§7.3, R23).

## 7. Authentication (R6)

### 7.1 Trait

```rust
pub trait VectorCommitment<K: Key, V: Value>: Clone + Send + Sync + 'static {
    type State: Clone + Send + Sync;            // copied with the snapshot (§4): cheap to clone
    fn scheme_id(&self) -> &'static str;
    /// Digest of the instance parameters (KZG setup hash and slot count, Merkle
    /// depth; zeros for lthash). Enters the root preimage (§7.3).
    fn params_digest(&self) -> Digest;
    /// The scheme instance carries its parameters (KZG setup, Merkle depth), hence `&self`.
    fn empty(&self) -> Self::State;
    /// Incremental update for one key; `None` = absent.
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>);   // item = the entry under its key (§7.2)
    fn commitment(&self, state: &Self::State) -> Digest;          // 32 bytes, enters the root
}
```

The trait is incremental by design: every candidate scheme updates its
commitment from `(old, new)` without touching the rest of the set, so
per-block cost is proportional to the batch rather than the set. Proof
methods (`prove`, `verify`) are added with milestone 5 (§7.4); v0 code
carries none of them (R20).

### 7.2 Schemes

| Scheme | State | Update cost | Membership proof | Notes |
|---|---|---|---|---|
| `lthash` (v0 default) | 2 KiB lattice-hash vector (1024 × u16 lanes) | one XOF + lane add/sub | none (set-level only) | Additively homomorphic: `C ← C − H(k,old) + H(k,new)`. Cheap, order-independent, authenticates the set as a whole. |
| `kzg` (milestone 5) | one G1 element (48 B, hashed to 32 for the root) + `key → slot` map (a slot's value commits to the §7.2 entry item) | one scalar mult | yes, constant size | Slot count must exceed `capacity` by a safety factor (a `capacity`-slot table is guaranteed to collide near full occupancy); collision policy is open (§12.2). Trusted setup of slot-count size. The slot map is consensus state: it lives on the `Snapshot`, copied with the entries (one more per-key map in the per-proposal copy, §12.14), to be settled together with §12.2 in milestone 5. No code in v0. |
| `merkle` (reference) | whole sparse Merkle tree (persistent) over `SHA-256(key)`, depth 256, leaf = the entry item below | 256 hashes per update | yes | No slots, so no collisions; parameter-free. Slow but proof-friendly baseline for tests and benchmarks; its persistent tree is cloned with the snapshot in O(1). |

**Lattice accumulator encoding** (shared by `lthash` and `MemoryStorage`'s
root). An item is the concatenation of its fields, each as
`u32 LE length ‖ bytes`, so moving a byte between key and value changes the
item; an entry item is `tag ‖ key ‖ value`, three length-prefixed
fields: the tag field is one byte, `0x00` for a tombstone and `0x01` for a
value, and a tombstone's value field is empty, so tombstone, empty value and
absent are distinct (the policy state is a root term of its own, §7.3, R25;
`MemoryStorage` uses the same encoding); integers are `u64 LE`. The item hash is BLAKE3 `derive_key` (with the use's
domain tag) over the item, 32 bytes; the lane vector is the ChaCha8 keystream
keyed by that hash (nonce zero), 2 × 1024 bytes read as `u16 LE` lanes
(hash-then-PRG); `add`/`remove` are lane-wise ± mod 2^16, and two
accumulators over the same tag merge by lane-wise addition, so a batch can be
hashed in parallel chunks; the 32-byte digest is SHA-256 of the lane vector in
`u16 LE`. Measured (`cargo bench -p auth_wal --bench lattice`, Haswell with
AVX2 only): BLAKE3's own XOF output is vectorised only with AVX-512 and cost
2.5 µs per item, ChaCha8 expansion 0.65 µs (0.74 µs per add including the
hash), AES-NI counter mode 1.0 µs; a 16-way parallel accumulation reaches
0.08 µs per item. The parameter security of 1024 × u16 lanes and of the PRG
expansion is an open question (§12.12).

Each scheme is a module under `src/vc/`; a scheme with heavy dependencies
(KZG) will sit behind a feature. Tests run the same suite against every
scheme.

### 7.3 Root composition (R12)

The validator sees exactly one commitment per proposal, revision and committed
state. It is computed when the node is built and is what `Root()` returns and
what `Revision(root)` is keyed by.

```
root(node) = EMPTY_ROOT                                         if node.height == 0
           = SHA-256( "auth_wal/v1"                                   (raw bytes, no length prefix)
                    ‖ len ‖ scheme_id ‖ len ‖ policy_name ‖ len ‖ engine_id   (u32 LE length-prefixed)
                    ‖ scheme.params_digest() ‖ policy.params_digest()
                    ‖ capacity                                   (u64 LE: in force at the node, §6.2)
                    ‖ height                                     (u64 LE)
                    ‖ VC.commitment(set)                         set contents (§7.2)
                    ‖ policy.state_digest()                      the eviction order (§6.1)
                    ‖ storage_root                               pinned storage view
                    ‖ metadata_hash )                            None → 32 zero bytes; Some(m) → SHA-256(m)
```

Why each term: the parameters and parameter digests keep two networks with
different settings from agreeing on a root by accident; `height` makes every
block's root unique (empty blocks, §3). The policy state enters as a term of its
own, `policy_digest = state_digest()` (§6.1, R25), so equal roots imply
equal set contents, equal eviction order and equal storage state, and
therefore identical future evictions:
**two nodes with equal roots are interchangeable as parents**. The adapter
needs exactly that: a block's header root is this root, and
`createProposals` stages a child on the committed tree and on every live
proposal registered under the parent's root, returning one candidate's root,
so two live candidates with equal roots but different policy states (under
`lru-write`, one that rewrote a key with its existing value and one that
wrote nothing, a routine pair since the adapter puts every touched account)
would give a child two roots depending on arrival order and split the
validators (the v21 review). The term's history: v20 covered the state with
a second lattice accumulator over `(key, height)` pairs, as much hashing
again as the set commitment; v21 dropped it on the premise that policy state
is predictive, which that review refuted; v21–v23 carried a per-key datum
inside the set commitment's items at no extra hashing, which forced every
policy's state to be per-key; v24 (R25) makes the policy opaque
(`writes`/`hint_writes`, §6.1) and digests its whole state instead, one
hash of the order per proposal (BLAKE3 over 10k `(height, key)` items, a
few hundred microseconds against the 2 ms copy, to measure in milestone 1;
an incremental accumulator inside the policy replaces it without touching
the trait or the root if it ever matters, clarification 21). `engine_id` is included because every engine reports
`EMPTY_ROOT` before its first commit, so without it two networks on different
engines would share roots and diverge later. `storage_root` is the
pinned storage view's root; a proposal without a storage proposal of its own
reuses its parent's.
Both QMDB and firewood report a proposal's root at propose time, so the
combined root is available before commit. `EMPTY_ROOT` means "nothing
committed yet": the height-0 node; `Revision(EMPTY_ROOT)` resolves to it
while that node is in the K-ring (the adapter needs it only at genesis), and
returns `RevisionNotFound` once it is retired. Every node keeps its
`metadata_hash` so the preimage can be reproduced for proofs (§7.4) and
tests.

### 7.4 Authenticated reads (milestone 5)

A proof of `key = value` under `root` is the preimage components plus a set
proof (membership, or non-membership) from the scheme and, on non-membership,
the storage engine's proof under `storage_root`. `lthash` cannot produce a
per-key proof, so `ProveLatest` returns "unsupported" under it; `kzg` and
`merkle` can. This is the main reason the scheme is pluggable (§12.1).
Milestone 5 adds `prove`/`verify` to `VectorCommitment`, `prove`/`verify_proof`
to `StorageEngine`, and `prove` to the handle; the node already keeps
everything a proof needs (its `metadata_hash` and the writer's terms rebuild
the preimage, §7.3; its snapshot).

## 8. Proposal tree and lifetime (R7, R8)

```
committed ──► P1 ──► P2        (chain: block building)
         └──► P1'              (sibling: competing block)
```

A **node** is the immutable record of one proposal: one block's batch applied
on one parent. Its contents: the **snapshot** (§4, the whole set after the
batch), the **pinned storage view** (§5; the node's own storage proposal when
it created one, §6.3, else the parent's, the same handle), the number of
speculative steps that proposal chains on (§9.1; zero on the plain path, the
one bit `Commit`'s poison rule needs, §9.2), its `metadata_hash` and root
(§7.3), and one **weak link to its parent**. Statistics are counted at
`Propose` (§9.5), so a node carries none.
Nothing in a node changes after construction, so it is shared freely between
the writer, the worker and the reader threads and every read takes no lock
(§5, §10). Every handle the caller holds, every entry of the K-ring and the
precomputation refers to a node; committing a node changes nothing in it,
it makes the **registry** point at it.

The **registry** is the one structure the writer changes and readers read: the
**K-ring**, one deque of the last K committed nodes in commit order, whose
newest entry is the **head** (the committed set is the head's snapshot; there
is no separate head field, no order list and no copy of K, which `Commit`
passes in), the **live index** (a flat list of (root, height, weak link) triples
for the live proposals, Drop below), and nothing else (the statistics are
atomics beside the speculation counters, §9.5). It sits behind the registry
mutex (§10). The live index exists for one reason: `Revision(root)` must
resolve a live proposal's root, as `qmdb-ffi`'s `Database.Revision` and
firewood's `view` do, and the adapter's `Reader(root)` opens the state of
every unaccepted block that way (`translateRoots` lists live proposals
first); nothing else answers that lookup, and making the adapter read through
the handle it holds instead would be a validator change (R1). The hint path
needs it as well: the adapter calls `Prefetch` on the view it reads through
(§9.4), so for a block built on an unaccepted parent the live index is what
delivers the hint to that parent's node, by identity, which the
precomputation targets (§9.2); `qmdb-ffi`'s `qmdb_get_revision` resolves a live proposal the same
way. Everything else the writer owns outright and touches through `&mut
self` (§3 threading contract): the precomputation (§9.2), the worker
handle, the storage engine, the configuration and the poisoned flag.

- **Lineage.** Roots carry no identity and storage handles are identity-based
  (an equal-root sibling's engine proposal was never committed), so a node
  needs exactly one link: a `Weak` pointer to its parent, immutable and
  retaining nothing but the parent's inline allocation (a few KiB: the
  snapshot's heap is freed with the last strong reference); qmdb-ffi's own
  proposal tree holds parents weakly for the same reason. Two rules read it,
  each one compare (R22; the lineage walk of v20 is gone): (1) **`Commit`
  requires the parent to be the head node** (identity), else
  `ParentUncommitted` (a deeper pipelined node: commit the parent first, as
  the adapter always does; or a line that lost: never committable) or
  `ProposalOrphaned` if the link is dead (the parent was dropped
  uncommitted, or retired from the ring with no handle left); a node whose
  root is the head's reports `ProposalConsumed`
  (that state is committed). (2) **`Propose` on a node whose root is the
  head's builds on the registry's head node**: equal roots mean equal set,
  policy and storage state (§7.3), and the head's pinned view is the
  committed handle, so the child never chains on a twin's uncommitted engine
  proposal; the child's
  parent link points at the head node. Any other node is proposed on as
  given, whatever its lineage, as firewood and qmdb-ffi accept: a child of a
  node that can no longer be committed is itself never committable, and
  `Commit` says so; the adapter never proposes on such a node (it stages
  only from the committed tree and its bound live proposals, and losing
  branches leave those at commit cleanup). The precomputation
  targets one node at a time (§9.2, R24): `Commit` leaves it alone when it
  already targets the new head or a live child of it (by parent identity:
  the pipeline's next parent) and otherwise retargets it to the new head; a
  hint on a live child of the target retargets it, a hint on the target
  refines it, and a hint on any other node does nothing, so no test of a
  node's line is made (R23).
- **Propose** creates a child node: the parent's snapshot is cloned (the
  registry's head node for a parent whose root is the head's) and the batch applied
  to the copy (§4), whose policy is a clone of the parent's with its
  prediction marks (§9.2);
  the policy evicts if the copy is over capacity (§6.2),
  the storage proposal is created if needed on the parent's pinned view
  (§6.3, §9.1), the root composed, the node built with a weak link to the
  node it was built on (the registry's head node in that case) and registered in the live index. The registry is held twice, for
  nanoseconds: once to resolve the parent (one `Arc` clone and a root compare) and
  once to register the child (the batch's counts go to atomics with no
  lock, §9.5); nothing else of auth_wal's is locked, and nothing can commit
  meanwhile (`Propose` and `Commit` share `&mut self`).
  When the parent is the precomputation's target, `Propose` calls the
  worker's `finish` after applying the batch, between the two holds with
  nothing held (§9.2). The copy is the one cost, about 2 ms at 10k (§4). Live proposals are held
  weakly by the registry; the caller's handle owns the node.
- **Commit(P)** takes `&mut self` (only another writer call could ever wait
  on it, and the type system forbids one in Rust; through the C ABI a hint
  waits on the writer mutex, §3) and `P`'s handle by value, **whatever the
  outcome**: a failed storage commit consumes the handle too (the node is
  dead: on the plain path the engine consumed its storage proposal, on the
  speculative path the instance is poisoned, §9.2), the adapter re-proposes
  the block, and a retry of the same Go handle through the C ABI reports
  `ProposalConsumed`. On a pre-check failure (`ParentUncommitted`,
  `ProposalOrphaned`, `ProposalConsumed`, `Closed`, `Poisoned`) the node is
  untouched but the handle is consumed all the same, so the proposal is freed
  unless another handle holds it and must be re-proposed; this mirrors
  firewood's `commit(self)`, and the adapter never hits it (it commits in
  order). Under one registry hold it checks `P`'s parent link (rule 1
  above) and compares `P`'s pinned view with the head's by
  identity: a different handle is `P`'s own storage proposal (an inherited
  view is the head's). It then commits that proposal if there is one
  (firewood sees one commit per storage proposal, in order: each step the
  precomputation sent, then the final proposal; the adapter only ever commits
  direct children of the committed state anyway), with no lock held. Then one
  registry hold installs the node: pushed on the ring, it is the head
  (the ring holds the K most recent committed nodes *including* the head, so
  the oldest resolvable root is at most `(K − 1) × (1 + rounds)` engine
  commits behind, which is what makes an engine window of `K × (1 + rounds)`
  sufficient); it leaves the live index (the ring answers for its root now);
  the ring entry beyond K is taken out and handed back. The retired entry is
  dropped after the hold (freeing a snapshot or
  engine handles is not a nanosecond hold, §10); the writer then retargets
  the precomputation to the new head, unless it already targets the new head
  or a live child of it (by parent identity, §9.2), with nothing held; no
  step is planned until the first hint. Nothing is copied or replayed at commit: the node's snapshot *is*
  the committed set. A failed storage commit leaves the head where it was,
  returns the error, and poisons the instance if the node's proposal chained
  on speculative steps (§9.2); no retarget then.
- **Drop** of a handle releases its strong reference and nothing else: the
  node (and its storage proposal) is freed when the last reference goes (a
  ring entry may still hold one; the precomputation and the live index hold
  their node weakly, §9.2). Children hold only weak links, so dropping an
  uncommitted node leaves its children with a dead link and `Commit` reports
  `ProposalOrphaned` for them, which is right: their parent can no longer be
  committed. A dropped node stops resolving
  at once. Live-index pruning rule, stated once:
  the live index drops its dead links whenever a node is registered or
  leaves it at commit, and at commit also every entry K or more heights
  below the new head (one window for both lists, R22: the node, if still
  held, stays readable but no longer resolves by root), so it is bounded by
  the live proposals within the window; lookups never mutate. A handle collected by Go's GC (`runtime.AddCleanup`, as in
  `qmdb-ffi`) frees the same way; Go `Commit` disowns the keep-alive first, as
  `qmdb-ffi` does.
- **Lookup order.** `Revision(root)` scans the ring from its newest entry
  (committed roots are pairwise distinct, height is in the preimage; at most
  K root compares, each a node dereference: 141 ns for a miss over 128 with
  the nodes hot against 28 ns for the map lookup it replaces, `registry_ring`
  in `benches/lattice.rs`, up to one cache miss per entry cold; a `Revision`
  call happens once per block execution or RPC state open) and
  then the live index, comparing roots before upgrading and returning the
  first node still alive; so at the committed root it resolves to the
  committed node, never to a live sibling that shares the root. Equal roots
  are interchangeable (§7.3), but two distinct live nodes can still carry the
  same root and the adapter may drop one of them, hence triples rather than
  unique keys, and a flat list because it holds a few entries. Retiring a node from the ring removes its root, and the window rule
  retires an equal-root live twin at the same commit (sweep rule above).
- **Handles.** The Rust core has one handle type over a node, `Handle`,
  returned by `Propose` and by `Revision(root)` alike (the Go package keeps
  qmdb-ffi's two names, `Proposal` and `Revision`, over it): `Get`, `Root`,
  nothing else; the writer takes it as an argument to `Propose`
  (as parent), `Commit` (by value) and `Prefetch`. It is a plain newtype over
  the node's `Arc` and holds no other state: the Go layer counts outstanding
  handles and waits for them before `Close` (its keep-alive bookkeeping,
  `keepalive.go`), and the engine refuses to close while an uncommitted
  storage proposal is held (§10 item 6), so the Rust side keeps no handle
  count and no closed flag for reads (R20).
- **Revision window.** K committed nodes are retained (`WithRevisions`;
  `K ≥ 2`, because the adapter resolves the parent's root right after a
  commit, so the previous head must still resolve). A handle to a retained
  revision reads its node's snapshot (§5) and its pinned view, which surfaces
  `RevisionStale` if the engine has dropped that root.
  The engine is therefore opened with a window of `K × (1 +
  speculation_rounds)` when speculation is on and the engine commits chain
  links separately (`COMMITS_CHAIN_LINKS_SEPARATELY`, §10: firewood and the
  in-memory model), else K. The Rust `new` checks the engine it is handed;
  the Go `New` opens the engine with that size. Note that firewood never
  reaps a revision that is still referenced, so a pinned firewood view does
  not go stale: its window stalls instead, and the K-ring (plus the adapter
  dropping revisions) is what bounds memory.
- **Memory.** Each node owns a snapshot: the entries map (about 0.6 MB of
  map nodes at 10k entries with `Arc` keys and values, the values shared with
  every other snapshot that holds them) plus the policy's two per-key maps
  (§6.1, about 0.8 MB), leaf payload at 70 % fill plus about an eighth for
  internal nodes: about 1.4 MB per snapshot with `Arc` keys and about 2 MB
  with inline 32-byte keys. The ring keeps K of them, so K = 128 retains
  180–260 MB of snapshots, plus one per live proposal (a few at most: the
  blocks being verified ahead and competing candidates). `WithRevisions` is
  the knob, and the policy's maps (§12.14) the lever. The K-ring, not handles,
  bounds memory: a dropped handle to a committed node frees nothing until the
  node leaves the ring (one to an uncommitted node frees it at once), and a
  live orphan keeps its copy alive.

## 9. Speculative eviction and prefetch hints (R9, R14)

Goal: when `Propose` finally arrives with a block's batch and the victim set is
known, most of the storage engine's Merkle work for those victims is already
done. Three mechanisms:

1. Predicted victims are sent early in **steps**, storage proposals built one
   on top of the other from the pinned view of the node the block is being
   executed on (its parent), by the instance's background worker: off the
   caller's thread (R17).
2. During execution the prediction is **refined** from prefetch hints (§9.4):
   the policy, asked at every hint, names more likely victims as the block
   dirties more new keys and withdraws predictions a hint contradicts; both
   go out as further ops, each step built on the latest view whether or not
   the previous one has finished hashing (the worker runs them in order,
   R24).
3. At `Propose` the policy reconciles its predictions with the actual
   victims into one last refinement (§6.1) and the **final proposal** is
   built on the latest view: it **puts back** every predicted key that was
   not evicted (the parent's value, or a delete if the key was absent) and
   writes the victims the steps did not, so the storage state after the
   final proposal is exactly what the plain path would produce. On an engine
   whose root is a function of state (firewood, §10) the storage root is
   therefore identical, and speculation is invisible to consensus: no
   parameter of it enters the preimage, and it may freely depend on timing,
   hints and local heuristics.

The prediction is neither accurate nor complete by design (clarification
16): a step carries a best guess, and the final proposal corrects it in both
directions, retractions for the keys sent and kept, the remaining victims
for the keys never sent. What bounds how aggressive the prediction may be is
the cost of the retractions (`margin`, §9.2). The steps are also all the
warming there is (clarification 22): a hint no longer reads anything
through the engine, since the paths the final proposal touches are the
victims' and the retracted keys', which the steps have just written.

### 9.1 Plain and speculative paths

For a node `N` built on parent `P` from batch `B`, with the policy's answer
to `writes` (§6.1) being the victims `V` and its last refinement `(E, R)`:
`E ⊆ V` the victims whose entry storage does not hold yet, `R` the
retractions (canonical order, §6.3):

| path | storage proposals |
|---|---|
| **plain** (speculation off, the precomputation does not target `P`, no step was built, or a step failed) | one proposal on `P.storage_view` with the canonical batch: `V` in key order |
| **speculative** (the precomputation targets `P` and its steps `S₁ → … → Sₖ` were all built) | the final proposal `F` on `Sₖ`, the latest view, with ops in this order: (1) **retractions** for every key in `R`, in key order: `Put(old)` where `old = P.storage_view.get(k)`, or `Delete` if absent; (2) `E` in key order |

`E` and `R` come from the policy's marks (§6.1): a key is marked while a
step carries its entry, so storage, as of the latest view, holds it. A
marked key the block then wrote again stays in the set, is not in `V`, and
`F` puts storage back to what `P.storage_view` holds for it, the value from
before any step, or deletes it if storage had none (it is in `R`); a marked
key evicted after all is omitted if the block did not write it (the step's
entry is its final entry) and rewritten with its final entry otherwise
(under FIFO a rewritten key keeps its place and can be evicted; it is in
`E`). Correctness: after (1) storage differs from `P`'s state exactly on
`V ∖ E`, the marked victims the block did not write, whose step entry is
their final entry; after (2) it equals the plain path's. The invariant that
makes this sound is that **the marks mirror storage** whenever `finish`
answers a view: every key the policy marked was submitted as an op and
built before `finish` answered, and every retraction the policy returned
was executed, as an op during the block (§9.2) or in `F`; when a
submission was dropped or a step failed `finish` answers nothing and the
node takes the plain path on `P`'s view, where the marks are irrelevant.
Retractions need one storage read per key, warm because the step just
wrote that path; they run after `Propose` has waited for the queue (§9.2);
nothing here takes a lock across an engine call (§9.2, §10).

On the speculative path `F` is created even when `V` is empty and even when
it carries no ops at all (the node's view must be the reconciled one), so the
node owns a distinct handle. A precomputation that built no step takes the
plain path: no extra engine commit for a quiet block. If a retraction's read
through `P.storage_view` fails, `F` is abandoned and the node takes the
plain path on `P`'s pinned view (counted in `fallbacks`, §9.5; the plain
proposal reports its own error if the view is really gone). `F` carries the
node's metadata; steps never do.

**Kept as is (clarification 14, 2026-10-28), to change by extending
firewood.** auth_wal keeps no copy of a sent key's old value: the retraction
is a read-back through `P.storage_view` and an explicit `Put(old)` or
`Delete`, in a step or in `F`, which costs one read per key, one value copy
across the engine boundary and a second hashing of the key's path. The
recorded future change is a firewood-side restore op that undoes a
proposal's write for a key against the proposal's base (§12.10 (e),
`docs/TODO.md`): the ops would carry `Restore(k)` instead, with no read of
ours, and the engine would settle the absent case itself. The ops order, the
correctness argument above and the `reverted_ops` metric are unchanged by it.

### 9.2 The precomputation

**One at a time (R24).** The validator executes one block at a time, and a
block's hints cannot arrive before the previous block's root has been
computed, so at any moment there is **one precomputation**, for one node (the
one the next block builds on), and every step is built on the **latest
view**, finished or not. Nothing is cancelled and nothing is chained by hand:
the worker builds steps in the order their ops were submitted, and a step's
base is whatever the previous one left behind.

**Three owners (R25–R27).** The **policy** owns the prediction (§6.1):
asked at every hint, it answers which entries to send and which earlier
predictions to withdraw, and at `Propose` it reconciles its marks with the
actual victims. The marks live where the policy lives, in the target node's
snapshot, behind the policy's lock (§4): the only writers are hints, the
clone `apply` takes and a retarget's reset, all the writer's, serialised by
`&mut self`, so the lock is never contended and readers never touch it. The
**writer** owns the target's identity (a weak link to the node) and turns
the policy's answers into ops. The **worker** owns the speculative side of
the engine (clarification 24): a queue of submitted ops, the chain of steps
it has built from them, the chain's base (the target's pinned view, which
every retraction reads through) and its latest view. The writer makes three
requests of it over a `std::sync::mpsc` channel (clarification 25; the std
channel has been crossbeam's implementation since Rust 1.67, so it is the
existing high-performance queue with no dependency added): `submit(ops)`
sends a batch and returns at once; `finish()` sends a block end and blocks
on its reply, the latest view with the step count once every request queued
before it has been handled, the base `Propose` builds the final proposal
on; `reset(base)` starts a fresh chain on a new target's view at a
retarget. The chain state is the thread's own locals; nothing is shared but
the channel and the counters, and no lock of ours is on the path. This replaces v23's writer-side sent
set and hint sets, the worker's dedupe, the job channel with its four job
kinds and the in-flight atomic, v24's working copy of the policy and warm
jobs, v25's split of the worker's logic between a state struct and a
handle, and v26's hand-rolled queue under a mutex and condvar
(clarifications 15–18, 22, 24, 25).

**A hint** on the target (§9.4) is one call on the target's policy,
`hint_writes(keys, capacity, margin)` (§6.1), whose answer becomes ops:

```
pred  = P.snapshot.policy.hint_writes(keys, P.capacity, margin)   (§6.1: this hint's insertions and the margin
                                                                     decide E, the first E candidates not yet marked
                                                                     are marked and returned)
ops   = Evict(k, P.set[k]) for k in pred.evict  ++  Retract(k) for k in pred.retract
worker.submit(ops)                                                 (no wait; the worker builds when a step is due)
```

A marked key's entry is looked up in the target's snapshot (the policy
tracks exactly the set's keys, §6.1). A retraction is submitted like an
eviction: the worker reads the key through the base view and puts that
value back (or deletes), so the marks keep mirroring storage (§9.1); under
the v0 policies a hint-time retraction is practically never wrong (an
lru-write key the block writes moves to the back; FIFO retracts nothing).
The policy never returns a victim twice, so the only repeat the queue can
hold for a key is an eviction followed by its retraction, which the
worker's merge folds into the retraction alone (below). No count of
insertions is kept across hints (clarification 22): each hint predicts from
the set size, its own keys absent from the set and the margin, so with
per-transaction hints the margin also stands in for the earlier hints'
insertions, the first thing the milestone-3 sweep measures (§12.5).

**Pessimistic prediction (R15).** `margin` (local knob `speculation_margin`,
handed to `hint_writes`) deliberately over-predicts once a hint's insertions
would carry the set over capacity, so that the block's later writes rarely
push the victim set past what the steps already carry and no further step
is needed. The price is more retractions in `F` for the keys that were not
evicted after all. This is the chosen trade-off against firewood's
per-proposal commits: every extra step, and every key written by a step and
put back by `F`, makes firewood recompute the intermediate hashes on that
path once more (§12.10). Fewer, larger steps cost less than many small ones.

**The worker (R17, clarifications 24–25).** Step hashing runs on the
instance's **worker thread**, never on a caller's. Each turn it blocks for
one request (the sender dropped ends the loop), drains every request queued
behind it, and handles them in order: a `Submit` joins the batch (or is
dropped when there is no chain, no reset yet or a step failed,
`ops_dropped`); a `Reset` drops the chain and the batch and starts afresh on
its base (the old chain's steps count `steps_dropped` when no `finish` read
them); a `Finish` is noted. Then, if ops are waiting and a `Finish` is noted
or at least `min_step` ops wait and fewer than `rounds − 1` steps were built
on this chain (the finishing step is the last of `rounds`), it merges the
batch into one **combined write set**, the last op for a key winning (an
eviction then a retraction of the same key is one retraction, the reverse
one eviction), in key order, and builds the step: evictions become
`Put`/`Delete` of their entry, retractions `Put(old)`/`Delete` read through
the base view, the batch proposed with no metadata on the latest view (§10
item 4: a committed handle makes the before-and-after check, a proposal
handle chains); the result becomes the latest view and the step count
grows. A failed read or proposal logs, counts `step_failures`, counts the
chain's steps as `steps_dropped` and leaves no latest view, so later
batches are dropped (`ops_dropped`) and the next `Finish` answers nothing;
a panicking step is caught and counted (`worker_panics`) with the same
effect; a thread that dies anyway drops its receiver, so every later
request fails at the sender (`ops_dropped`; `finish` answers nothing),
which ends speculation for the instance's lifetime without affecting
correctness, and its chain drops with it. Finally it answers the noted
`Finish`. A reset received while a step was being built is applied right
after it, dropping the result: request order replaces any generation count,
busy flag or liveness flag, and the thread needs no wake-up protocol of
ours. The worker is one thread per instance in v0 (a pool would have to
keep the steps in order, §12.5); `New` fails with `WorkerUnavailable` if it
cannot be spawned, `Close` drops the sender and joins it.

**`Propose` waits (R24, amending R17).** `Propose` applies the batch to a
copy of the parent's snapshot (§4; the policy clone carries the parent's
marks, which `writes` reconciles into the refinement, while the worker keeps
hashing), then, on the precomputation's target, calls `finish()`: it sends the
block end and blocks on the reply, which comes once the thread has handled
every request queued before it and folded the tail, below `min_step` or
past the eager cap included, into one last step: the latest view with the
step count, or nothing when no step was built (nothing was submitted, a
step failed, or the thread is gone): then the plain path. The wait is bounded by the step being built plus the tail;
R17's "never wait" now holds for `Prefetch` and, its own engine commit
excepted ("Commits and steps" below), for `Commit`. The benchmark's propose
latency includes the wait and the worker's busy time is reported apart
(§9.5, §11). A step that cannot be stopped is not marked cancelled, it is
waited for or, after a reset, simply never used.

**Target (R24, R27).** The precomputation targets one node, named by
identity (a weak link on the writer; "child" below means a live node whose
parent link *is* the node in question, one pointer compare, R22). `Commit`
leaves it alone when it already targets the new head (its steps were built
on the node's own view, now committed, and stay valid bases) or a live child
of the new head (the pipeline's next parent, hinted while the head was
live); otherwise it retargets to the new head. No step is planned at a
commit: the first hint plans (round 0 of v16–v23 is gone, §12.5). A hint on
a live child of the target retargets to that child (the block being
executed moved on); a hint on the target feeds its policy; a hint with no
live target retargets only to the head or a live child of it; a hint on any
other node (the head while a live node is targeted, a retained revision, a
losing candidate) does nothing, so no test of a node's line is ever made
(R23). A retarget resets the new target's marks (`reset_hints`, §6.1:
nothing marked is in the new chain's storage) and the worker's chain
(`reset`): the new target's pinned view becomes the base and the latest
view, the batch queued for the old chain is dropped (`ops_dropped`), a step
being built when the reset arrives is dropped right after it, and the old
chain's steps count as `steps_dropped` when no `finish` read them (§9.5). The marks of a node the
target moved away from stay on it unread: the node is never the speculative
parent again without a retarget to it, and the plain path ignores the
refinement. Because the marks and the steps both belong to the target, a
second child of the same target (a competing block, or a re-propose) is
speculative too: its copy of the policy carries the same marks and
`finish` answers the same latest view, and its reconciliation is right for
it. A straggler hint on the target after a child was proposed (an RPC tracer
finalising on the head's state while the next block is pending, or a late
finalise) only adds marks and ops, at most `rounds` steps that the next
commit's retarget drops (`steps_dropped`, `ops_dropped`).

**Commits and steps.** `Commit` never waits for the worker. The committed
node's own precomputation was finished by its `Propose`, so none of its steps
is in flight; what can be in flight is the *next* block's step, when its
hints already retargeted the precomputation to the node being committed
(the pipelined flow): that step is being hashed on the handle the commit
takes (`F`, or the inherited view), so the engine commit waits on the
handle lock for that one step (§10 (e); the worker runs one at a time), and
a step proposed on the handle after its commit takes the committed path
with the right root (§10 item 4 passes; the step stays a valid base). The
before-and-after root check is a backstop that only a racing retarget could
trip (a base on a committed handle whose root moves before its first step
runs), which costs one refused step, never a wrong state.

**Bounds.** Steps are bounded by `rounds` per chain (eager steps while
fewer than `rounds − 1` are built, the finishing step last) and there is one
precomputation, so at most `rounds` step proposals exist for the block being
executed, plus those the final proposals of live speculative nodes still
chain on (`rounds × (1 + live speculative nodes)`; the engine window of §8
is sized for it); there is no instance-wide budget (R20); with `rounds = 1`
no eager step is built and the whole prediction is hashed inside `finish`,
the no-overlap baseline the sweep (§12.5) should read it as. Each step
carries what the policy predicted since the last, at most `E` entries in
all. `Close` drops the channel's sender and joins the worker (the step
being built finishes its engine call; queued requests are handled and
nothing more is built), whose chain, the only speculative engine handles
there were, drops with it, so no step outlives the instance and the engine
can close (§10 item 6). Engine memory lags one commit: firewood keeps a dropped proposal
until its next commit.

**Commit.** Committing a child commits `S₁ … Sₖ` then `F`, one engine commit
each on firewood (§10). A failure part-way through leaves the engine head at
a state no node owns, so it is fatal: `Commit` returns the error, the
instance is **poisoned** and refuses further `Propose`/`Commit`
(`Poisoned`), consistent with halting rather than diverging; reads keep
serving the committed node and `Close` is allowed. If the node's final
proposal chains on steps (the node records their number, §8) the instance
is poisoned, because its single `commit` call cascaded through the steps
and the engine does not say how far it got; on the plain path nothing moved
(one commit): the node is dead (the engine consumed its storage proposal; a
retried `commit` on that handle reports the stored failure, §10 item 1),
`Commit` consumed the auth_wal handle (§8), the instance stays intact and
the adapter re-proposes the block.

### 9.3 Consensus and gating

Speculation never changes the set, the policy's consensus state or the
final storage state, so on a state-function engine it never changes a root:
the marks live beside the consensus state and `state_digest` never covers
them (§6.1). The engine declares the property with `ROOT_IS_STATE_FUNCTION`
(§10); `New` rejects `speculate = true` on an engine without it (QMDB,
whose roots cover the op history), so there is no configuration in which
speculation is consensus-visible.

Milestone-2 tests (on `MemoryStorage`, which models firewood):

1. Speculation on versus off, same block history: equal roots, equal policy
   digests and equal storage roots at every height.
2. Arbitrary hints, including wrong, duplicate, late and interleaved ones
   (hints from several sources on one node, serialised like every writer
   operation, R19; several steps built in one generation; hints on nodes
   other than the target; hints that retract; several batches queued
   before one step, merged), and varying `rounds`/`min_step`/`margin`:
   equal roots, and no live step after `Close`.
3. Differential test: random batches including `Delete`, with predicted
   victims written again before the block ends (the retraction case of
   §9.1, by a hint during the block and by the batch alone: storage must
   hold their pre-step value afterwards), reads compared against a
   reference map, and for every committed node `storage(k)` exact for every
   `k ∉ set`.
4. Two children of one parent, proposed in both orders, with and without
   hints between the two proposes, either committed: equal roots (both
   children reconcile against the parent's marks and `finish` answers the
   same latest view, §9.2).
5. Policy purity: a cloned policy state yields identical `writes` sequences
   on both copies, hints or not (also a milestone-1 test, §6.1).
6. Asynchrony (R17, R24), on `MemoryStorage`, whose handle carries firewood's
   lock shape (§10 (e): a read guard across its latency sleep in `propose`
   and across `get`, the write guard across `commit`), with synthetic engine
   latency (`--storage-latency-us`): `Prefetch` returns in time independent
   of step hashing; `Propose` on the target waits for the step being built
   and the tail (its latency includes their remaining propose latency and
   nothing else, and nothing is queued or being built once it returns); `Commit`'s latency
   includes no step's propose latency in the sequential flow and at most one
   step's in the pipelined flow (§9.2); and `Close` joins the worker (the
   running step finishes, pending ops are dropped) before closing the
   engine. Hints are issued while no `Propose`/`Commit` is running, so every
   wait the test measures has one cause; the unit test sets
   `MemoryConfig::op_latency`, the benchmark `--storage-latency-us`;
   progress is observable via the `steps_built` counter. The same test on
   firewood uses real hashing on large steps (10k entries or more) for its
   timing assertions.

### 9.4 Prefetch hints (R14)

**Meaning.** A prefetch hint tells auth_wal which keys execution is writing
so the policy can refine its prediction (§9.2). It is the hint API R14 asked
for, kept as an interface; what it feeds is the precomputation. It warms
nothing through the engine (clarification 22): a hinted key not in the set
goes into the set, not into storage, so its trie path is of no use to this
block, and the paths the final proposal touches, the victims' and the
retracted keys', are warmed by the steps that wrote them.

**API.** `Prefetch(keys)` on `Database` (relative to the committed state),
`Proposal` and `Revision` (relative to that view), mirrored in the Rust core
as one writer operation (R19, §3), `AuthWal::prefetch(&mut self, view,
keys)` with `view` the handle or `None` for the committed state, because a
hint writes the target's policy. Advisory: a wrong hint costs work, never
correctness; a hint on a node other than the precomputation's target does
nothing, unless the node is a live child of the target (or, with no live
target, the head or a live child of it), which retargets (§9.2); no-op after
`Close` and with speculation off. On a hint, auth_wal resolves the node
(one registry hold), retargets if the rules say so, and feeds every key to
the target's policy (§9.2); the hint returns after the policy call (R17).

**Firewood cache strategy.** The firewood engine constructor (milestone 0)
forces `CacheReadStrategy::All`, so every read through a view populates the
node cache along the path (a documented §3 deviation; the Go `New` logs the
override when the caller asked for something else, milestone 4). The
read-through warming that first needed it is gone with R26; the setting
stays as a milestone-2 knob, since execution's own reads go through the same
views. `StorageProposal::prefetch`, the read-through, remains an engine
capability the benchmark's engine-only baseline uses to emulate a native
warm call (§11).

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
advisory). The adapter hashes addresses and builds generation-qualified slot
keys as it already does for reads, and calls `Revision.Prefetch` (or
`Proposal.Prefetch`) on the view it reads through. Note that `journal.dirties`
includes accounts that were *touched* without being changed, so account hints
mean "written or touched" and slightly inflate a hint's count of insertions
(§6.1); slot hints are exact (`value != origin`). The hook itself is
milestone 4 (§13).

### 9.5 Metrics

Per proposal, counted at `Propose` (a losing candidate counts too: competing
candidates are rare on Avalanche and absent in the benchmark, and counting
at commit would need a record on every node): `plain`, `speculative`,
`steps`, `reused_ops` (victims omitted from `F`: `V ∖ E`, §9.1),
`reverted_ops` (keys `F` put back: `R`); the set statistics (`puts`,
in-place updates, evictions, §11) likewise; every counter is an atomic (the
registry holds none), and `Stats` adds the head's height and occupancy, read
under one registry hold, and derives `storage_proposals = plain +
speculative` and `total_storage_ops = evictions − reused_ops +
reverted_ops` (saturating: the two counter sets are bumped at different
moments; a fallback counts `plain`). Per instance: `steps_built` (steps the
worker built), `steps_dropped` (built steps no `finish` read: the chain
was reset or closed before a `Propose` on its target, or a step failed
after them), `step_failures` (step proposals the engine rejected, or a
retraction's read that failed), `retractions` (retract ops the steps
carried), `ops_dropped` (ops dropped unbuilt: submitted with no chain to build on,
dropped by a reset, or refused because the worker is gone), `fallbacks` (`F`
fell back to the plain path after a retraction's read or `F`'s own proposal
failed; the plain path is always available, §10 item 5), `hint_keys` (every
key of every hint), `worker_time` (cumulative wall time per step on the
worker, §9.2: work moved off every caller's path, including time a step
waited on engine locks; the benchmark's worker column, §11),
`worker_panics` (steps that panicked on the worker, caught and logged; no
latest view remains, §9.2). Figures of merit: `reused_ops / |V|` (work moved
off the `Propose` path) and `reverted_ops / (reused_ops + reverted_ops)`
(waste); the benchmark reports both per policy and per `rounds`.

## 10. Storage engine abstraction (R10, R13)

```rust
pub trait StorageProposal<K, V>: Send + Sync + Sized {
    fn root(&self) -> Hash;
    fn get(&self, key: &K) -> Result<Option<V>>;       // RevisionStale once outside the window
    fn prefetch(&self, keys: &[K]) -> Result<()>;      // §9.4; default: one get per key
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self>; // chaining; on a committed handle, item 4
    fn commit(&self) -> Result<Hash>;                   // once-only; see contract below
}
pub trait StorageEngine<K, V>: Send + Sync + 'static {
    type Proposal: StorageProposal<K, V>;
    const ENGINE_ID: &'static str;                      // in the root preimage (§7.3)
    const ROOT_IS_STATE_FUNCTION: bool;                 // required for speculation (§9.3)
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool;         // each proposal of a chain is its own commit (firewood)
    fn revisions(&self) -> usize;                       // engine window, for the §8 check
    fn root(&self) -> Hash;                             // `New`'s freshness check (§1); the benchmark baseline
    fn head(&self) -> Result<Self::Proposal>;           // committed handle at the committed root (item 7); `head()?.propose(..)` proposes on the committed state
    fn prune(&self) -> Result<()> { Ok(()) }            // pass-through hook, default no-op
    fn close(&mut self) -> Result<()> { Ok(()) }        // exclusive: the writer owns the engine (R19)
}
```

Everything auth_wal reads from or proposes on storage goes through a
proposal handle (a node's pinned view), so the engine interface has no
revision type, no engine-level read, propose or revision lookup (R20 removed
them with the `is_committed` probe and the proof stubs; `head()?.propose(..)`
is the one way to propose on the committed state, and the benchmark baseline
uses it too; milestone 5 adds `prove` and `verify_proof`, §7.4). A node's pinned storage view is always a proposal handle (`Arc<S::Proposal>`,
committed or not; the genesis node's is `head()`), so there is one dispatch
for proposing on a view, the handle's own `propose`, and neither the worker
nor the readers hold a reference to the engine: the writer owns it and closes
it through `&mut self` (R19).

**Proposal handle contract.** Handles are shared (`Arc`) between the node
that created them, their children's pinned views and the worker's latest view,
hence `commit(&self)`; the adapter owns the once-only state. An adapter must
provide:

1. `commit` commits this proposal and any uncommitted ancestors, oldest
   first, and returns the committed root. A second `commit` returns
   `ProposalConsumed`. A failed `commit` consumes exactly the handle whose
   own engine commit failed (on `NotLatest`, the oldest uncommitted
   ancestor): that handle reports the failure on every later operation, a
   retried `commit` included (§9.2), and no longer pins the engine; a child's
   later `commit` reports the ancestor's failure, but the child itself is not
   consumed and stays live, readable and pinning, until dropped.
   `COMMITS_CHAIN_LINKS_SEPARATELY` says whether those ancestors become their
   own revisions (firewood) or fold into one record (QMDB).
2. After `commit`, the handle `commit` was called on keeps working as a view
   of the committed root (`get`, `root`; the adapter re-bases to the
   backend's own revision lookup internally, firewood's `Db::revision`), and
   returns `RevisionStale` once the
   engine's window has moved past it. Implicitly committed ancestors (the
   steps) need not remain readable: reads through them may return an error,
   never wrong data.
3. Committing a child whose parent handle has already been committed
   succeeds (firewood: re-parenting onto the new head; `qmdb-ffi`: tip-child
   re-basing). The check an engine performs is therefore "the chain's base,
   after committing its uncommitted ancestors, is the committed state", not
   "this proposal's base is the committed state".
4. `propose` on a **committed** handle proposes on the engine's committed
   state, provided the engine's committed root equals the handle's root both
   **before and after** the call (an adapter for an engine whose root is not a
   state function compares revision identity instead), and returns
   `ProposalOrphaned` otherwise; it never panics. auth_wal proposes on
   committed handles on two paths: the plain proposal of a child whose
   parent's pinned view is committed (a child of the head, or of a live node
   that inherited the head's view), on the writer thread, where the check
   cannot fail (nothing commits during a writer operation, §3), and the
   worker's first step after a retarget, on the target's committed handle;
   in the pipelined flow the next block's step may be hashing on the handle
   being committed, as a chained proposal and never on the committed path,
   and a step proposed on it after the commit takes the committed path with
   the right root (§9.2), so the before-and-after check is a backstop that
   keeps **auth_wal the engine's sole writer** a true invariant: no proposal
   on a stale view is accepted as a step. (`F`
   itself never takes the committed path: it chains on the latest view,
   §9.1.) The decision between chaining and the committed path is the
   adapter's own, from its handle state, never a root comparison on the
   caller's side: an uncommitted proposal can be a storage no-op whose root
   equals the engine's.
5. `propose` is **side-effect free**: a proposal that is built and dropped
   leaves no trace on the committed state or on the root of any future
   proposal, and a `Delete` of a key the state lacks leaves the root
   unchanged (a state function), which `F`'s `Delete`-if-absent retractions
   and tombstone evictions of never-stored keys rely on. Speculative steps
   rely on the first; the contract tests verify both on both backends
   (milestone 0).
6. Committed proposals do **not** pin the engine: `close` succeeds once no
   live proposal handle (uncommitted and not failed) is alive, reports
   `ActiveHandles` while one is, and is indifferent to how many committed
   proposal handles remain. The firewood adapter
   keeps its `Arc<Db>` only inside a proposal's live state and drops it at
   commit; the re-based committed view keeps a `Weak<Db>` for item 4, which
   does not pin. auth_wal's `Close` relies on this: the committed node keeps
   its pinned view for the instance's lifetime (§8), and `Close` only has to
   join the worker, whose latest view holds the steps (live proposals), first; the
   Go layer waits for its outstanding handles before calling `Close`, and a
   Rust caller still holding a handle to an uncommitted proposal with its own
   storage proposal gets `ActiveHandles` from the engine (§3). The database file
   stays open while any committed view lives, so reopening the same directory
   in-process needs the instance dropped (the Go `Close` drops it).
7. `head` returns a committed handle at the engine's committed root
   (`EMPTY_ROOT` on a fresh engine), readable like any committed proposal and
   proposable per item 4. It is the genesis node's pinned view, which is what
   makes every pinned view a proposal handle (above).

**Firewood** (the backend of record). Its root is the Merkle trie root of
the stored state (`ROOT_IS_STATE_FUNCTION = true`); a proposal is an
in-memory hashed overlay on its parent, so proposing a step hashes the
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
mechanics the backend honours (milestone 0): (a) `Proposal<'db>` borrows the
database and `commit(self)` consumes it, so the adapter keeps a `'static`
database (leaked or pinned, as the `firewood-ffi` crate effectively does) and
owns the `Proposal` itself, exposing reads through the `'static` view handle
a proposal provides; (b) firewood forgets a proposal at the next commit
unless something holds it, and a proposal has no parent accessor, so the
adapter handle keeps its parent handle inside its live state (dropped at
commit) to commit ancestors oldest-first;
(c) an uncommitted proposal refers to its base revision by hash only, so it
is the node's pinned view (`Arc` to the committed revision) that keeps the
base alive; (d) two consecutive revisions can share a root (a no-op `F`, an
identical-value eviction) and firewood indexes revisions by root, so the
adapter keeps its own `Arc`s and never relies on root lookup for a retained
revision. Milestone 2 also runs the §9.3 tests 1–3 against the real engine (§13).

**QMDB** keeps interface-level support through `qmdb-ffi`'s Rust crate (which
owns the proposal tree and window): `ROOT_IS_STATE_FUNCTION = false`
(commit records and the ops MMR enter the root), `COMMITS_CHAIN_LINKS_SEPARATELY
= false`; speculation is rejected at `New`.

**`MemoryStorage`** (implemented, milestone 0), a model of firewood: a
persistent map plus an incremental state-function root (a lattice accumulator
over the encoded `(key, value)` items, §7.2, updated per op so a proposal
costs O(batch), not O(engine)), chained proposals committed one at a time
(`COMMITS_CHAIN_LINKS_SEPARATELY = true`, each its own commit in the
sequence), a window of K revisions with an O(1) staleness check (arithmetic
on the commit sequence), `prefetch` as the default
read-through, the same handle shapes as the firewood adapter (the parent
link inside the handle's lock, the committed-handle `propose` with its
before-and-after check, `head`), and an optional **synthetic per-op
latency** knob so benchmarks can show what speculation buys against a slow
engine. A fresh engine reports
`EMPTY_ROOT`, and so does an emptied one (the root is a function of state).

**Firewood backend** (implemented, milestone 0, feature `firewood`;
`src/storage/firewood.rs`). The mechanics above are realised as: (a) the
`Db` lives in an `Arc` owned by the engine (a plain `Option`, taken by
`close(&mut self)`, R19) and each proposal is stored with its borrow lifetime
erased by a `transmute` next to an `Arc<Db>` clone that outlives it, the one
`unsafe` in the crate; the clone lives only in the proposal's live state and
is replaced at commit by a `Weak<Db>`, which lets the committed handle
propose on the committed state (item 4) without pinning the database, so
`close` succeeds once no live (uncommitted and not failed) proposal is alive
(`ActiveHandles` otherwise, contract item 6); (b) each live handle holds its parent handle
inside its guarded state and `commit` walks those links to commit ancestors
oldest-first; the link goes with the live state at commit; (c) after
`commit` the handle re-bases to the committed revision at its root
(`Db::revision`, committed revisions only, so a live sibling with the same
root is never mistaken for it), to an empty readable state when the commit
emptied the trie (under `ethhash`, to the indexed empty revision), or to a
stale state that reports `RevisionStale` if the
root cannot be looked up, while a failed commit leaves a state that reports
the failure on every operation (§9.2); (d) `propose` on a committed handle
upgrades the `Weak<Db>` (`Closed` if the engine is gone), compares the
committed root with the handle's before and after proposing (item 4,
`ProposalOrphaned` on a mismatch) and never panics; (e) a handle's state sits
behind a read-write lock: `get` and `propose` share it (firewood's `val` and
`propose` take `&self`), only `commit` is exclusive, so a step being hashed on
the worker never blocks `F`, a plain proposal or a read on the same handle
(§9.2, R17); a contract test exercises it with concurrent proposes on one
handle (results, not overlap: §9.3 test 6 measures the overlap), and the
memory model carries the same lock shape so that test runs on both engines;
(f) `head` wraps the committed revision, or the empty state of a fresh
engine, as a committed handle (item 7). The memory model also counts its
live uncommitted proposals so its `close` reports `ActiveHandles` like the
firewood adapter's (item 6), fails the oldest uncommitted handle of a chain
whose commit is `NotLatest` (the failure half of item 1: firewood commits
oldest-first and consumes only the proposal whose commit failed; the children
stay live until dropped, and committing one reports the ancestor's failure
without consuming the child), and its synthetic latency makes the *after*
half of item 4's check testable: a test races a committed-handle `propose`
against a sibling's commit and expects `ProposalOrphaned` with the committed
state untouched (item 5). The constructor forces
`CacheReadStrategy::All`; the node-hashing algorithm
follows the crate's `ethhash` feature, which forwards to firewood's. The
engine contract tests (items 1–7, plus the failed-commit rule of §9.2: a
handle whose commit failed reports that failure on every later operation)
and the state-function check run against both backends
(`src/storage/contract_tests.rs`), firewood in a temporary directory.

**Concurrency (R18, R19, R20).** Every lock is listed here with the reason
it exists and its holds; the rule is that a reader takes none, and that the
writer holds none across an engine call, a snapshot copy or a drop. Measured
on one thread (`benches/lattice.rs`, `sync_primitives`): a parking_lot mutex
hold 14 ns, a read-write lock read guard 19 ns, a set-once cell read 0.6 ns,
an atomic load 0.4 ns, an `Arc` clone and drop 12 ns.

- *Ownership first.* The writer (`AuthWal`, every state-changing method
  `&mut self`, §3) owns the working policy, the worker handle, the
  storage engine, the configuration and the poisoned flag; the type system
  serialises writer operations, so none of these needs a lock, and two
  commits, or a commit and a propose, cannot overlap. Nodes are immutable
  (§8). The worker thread shares only its channel and the counters (§9.2); its
  chain is its own locals. Readers share the registry. The shared state is
  two things: the counters and the closing flag, atomics (the writer's
  `Closed` checks read the closing flag, so there is no second closed
  flag), and the registry behind its mutex; handles hold only their node
  (§8).
- *No lock on reads.* A node's snapshot and pinned storage view are
  immutable (§8), so a handle's `Get` and the engine read behind it take no
  lock of auth_wal's and check no flag; a database-level `Get` first
  resolves the head under the registry lock.
- *The registry lock* (one mutex, §8) guards the K-ring (whose newest entry
  is the head) and the live index. It exists because the mempool and RPC
  threads resolve the head and revisions while the consensus thread commits:
  it protects those two lists, never a snapshot's contents, which are
  immutable. Its holds, each a few pointer operations or a scan of the ring
  (both lists are pre-sized at `New`, the live one to more entries than the
  adapter ever holds live, so neither a register nor an install allocates):
  `Propose` two (resolve the parent, one `Arc` clone and a root
  compare; register the child), `Commit` one to check the parent link and
  the view (the link upgraded before the hold and released after it, so a
  parent whose last other reference went meanwhile is freed outside the
  lock) and one to install (ring push and retirement, live-index removal);
  `Prefetch` one (resolve the node), `Close` none, and a reader's `Get`, `Root`, `Revision`, `LatestRevision` and `Stats` one each.
  Never held across an engine call, a snapshot copy or a drop: retired ring
  entries are handed back and dropped after the hold, and the precomputation
  is retargeted by the writer with nothing held. About 15 ns uncontended plus the work: the parent check is
  one `Weak` upgrade and two pointer compares, and a `Revision(root)` lookup
  reads each ring node's root, 141 ns for a miss over 128 hot and up to one
  cache miss per entry cold (§8). An atomic pointer
  swap of the registry would let readers resolve with no lock at all
  (§12.14).
- *The policy lock* (one mutex per snapshot, §4) guards the policy state of
  a node, because hints mark predictions in the target's policy while the
  snapshot is shared (§9.2, R26). Only the writer takes it (a hint's
  `hint_writes`, `apply`'s clone, a retarget's `reset_hints`), serialised by
  `&mut self`, so it is never contended; readers never touch the policy. The
  clone under it is the 0.9 ms policy copy of §4, blocking nobody.
- *The engine handle lock* (firewood adapter, (e) above): firewood's
  `Proposal::commit(self)` consumes the proposal, so a handle shared with the
  worker (a step being proposed on it) and with readers (children without a
  storage proposal of their own, RPC on the preferred block) must change
  state under a read-write lock when it commits; `get` and `propose` share
  it, and the parent link lives inside the guarded state, so it needs no cell
  of its own. Firewood itself takes its revision-manager locks on every
  `propose`, `commit`, `revision` and `root_hash`, so the storage path is
  never lock-free; this lock adds about 19 ns to a microsecond read. A
  firewood-side `commit(&self)` would remove it (`docs/TODO.md` item 5); kept
  for now by decision (clarification 6).
- *Lock-free atomics*: the closing flag, the counters and the firewood
  handle's committed flag. The worker's queue is a `std::sync::mpsc` channel (no
  lock of ours; `finish` blocks on a reply channel; `Close` drops the sender
  and joins).
- *Lifecycle without cells*: the worker is spawned after the shared state
  exists and holds a strong reference to it, so there is no attach step (the
  thread exits when its channel's sender is dropped, by `close` or by the
  writer's drop); the writer owns the join handle and the engine, so `shutdown` and
  `close` take `&mut self`; a node's committed status is registry membership
  and its lineage one weak link (§8), so no node field is ever written.

There is no lock order to state: auth_wal's locks, the registry's and a
snapshot's policy lock, are never held together, neither nests anything (no
engine call, no drop under them), and the engine's own locks are leaves taken
with nothing of ours held. The worker thread takes no lock of ours: it receives requests on its
channel and proposes on a handle (the adapter's lock). The parent to propose on is resolved under the registry lock
(§8); a `Propose` can never overlap a `Commit` (both are `&mut self`), and a
step in flight during a `Commit` (the pipelined flow) chains on the handle
being committed, whose lock the engine commit waits on (R24, §9.2), so no
race is left in the system; contract item 4's before-and-after root check
stays as the backstop. No actor thread in v0;
the C ABI keeps `qmdb-ffi`'s handle/keep-alive conventions and serialises
writer calls with a mutex of its own (§3), so one can be added without
changing the Go side.

## 11. Benchmarks (R11)

`auth_wal/benchmark/` is a binary crate modelled on `firewood/benchmark`:
same four workloads, same key derivation (`sha256(row_id)`), same CLI flags
where they apply, plus auth_wal knobs.

| Workload | From firewood | What it stresses here |
|---|---|---|
| `create` | insert N rows, 10k per commit | pure eviction throughput; every batch overflows the set (firewood's batch size; a typical block writes under 2k entries, so sweeps also run `-b 2000`) |
| `tenkrandom` | 2.5k deletes, 2.5k inserts, 5k updates per batch over a moving window | mixed Put/Delete, tombstone handling |
| `zipf` | Zipf-distributed updates (exponent 1.2, seeded) | the target workload: hot keys should stay resident; hit rate vs capacity |
| `single` | tiny batch on the same keys every commit | pure in-set update path; storage should see zero writes |

The suite is implemented (milestone 0, `benchmark/`). Targets: `--baseline`
runs a workload straight into an engine selected with `--engine
{memory,firewood}` (firewood is the vanilla comparison point; `--dbname` and
`--cache-memory-limit` as in firewood's benchmark, and `create` truncates
while the other workloads reopen the database `create` left); without
`--baseline` the workload runs through auth_wal (milestone 1). Run control:
`--iterations` (batches for tenkrandom/zipf/single; 0 = until
`--duration-minutes`, firewood's rule) and `--progress-every`. Flags:
`--capacity` (default 10k entries, R16; sweep from 1k to 1M),
`--low-watermark` (default `capacity / 2`, the policy's parameter), `--revisions` (K), `--policy {fifo,lru-write}`, `--vc {lthash,merkle}`,
`--speculate`, `--speculation-rounds`, `--speculation-min-step`,
`--speculation-margin`, `--hints`
(emulate libevm: call `Prefetch` with the batch's keys in per-transaction
chunks before `Propose`), `--hint-chunks`, `--storage-latency-us`, `--zipf-exponent`,
`--seed`,
`--warmup-batches` (zipf), and `--baseline` to run the same workload straight
into the storage engine with no write set in front.

Reported per run: commits, elapsed time and commits per second, mean and
p99 propose and commit latency (speculation moves hashing from `Propose`
onto the worker thread; `Propose` includes the flush wait, §9.2; `Commit` includes none in the benchmark's sequential flow and at most one step's hashing in the pipelined flow),
hint-path time, worker time (the background
step proposals, §9.2, R17: off every caller's path and reported
apart from propose and commit latency; the CSV column arrives with the
auth_wal target in milestone 1), set occupancy, evictions per
commit, storage ops per commit, in-place update ratio, reused and reverted ops
per commit and steps per commit (§9.5), peak RSS. Warm-up batches are
excluded from every column. The `target` column names the engine or
`auth_wal`.
The driver fills the configuration columns; a workload returns measurements
only. Output is a CSV header plus one record per run
so sweeps over capacity × policy × speculation rounds × hints can be plotted directly. A small
`criterion` suite covers the micro-paths; `benches/lattice.rs` exists
(accumulator add and parallel accumulation, the per-proposal copy, the
synchronisation primitives, memory engine propose; the benches of the decided
alternatives were removed with R20 and their figures are recorded in §4,
§7.2, §10 and §12.13) and milestone 1 adds VC `apply` and the policy's `writes`.

## 12. Open questions

1. **Per-key proofs under lattice hash.** `lthash` authenticates the set, not
   individual entries. Either accept set-level authentication for v0 or move
   the default to a proof-capable scheme once `kzg` exists.
2. **KZG slot mapping.** Slot count vs capacity, and the collision policy
   (chain into the slot value, cuckoo-style displacement, or reject).
3. **Read-aware policies.** Ruled out in v0 for determinism; would need the
   adapter to signal execution-time reads separately from RPC reads.
4. **Eviction cadence.** Decided (R16, §6.2): bulk eviction to a low
   watermark, half the set by default; the capacity/watermark ratio stays a
   §11 sweep dimension.
5. **Speculation knobs and the worker.** The knobs (`rounds`, `min_step`,
   `margin`) are provisional until the milestone-3 sweep, as are: whether
   the prediction should count a block's insertions across hints (the
   hinted set of v24, dropped by clarification 22: each hint predicts from
   its own keys and the margin, so with per-transaction hints the margin
   stands in for earlier hints' insertions; the sweep measures what that
   costs and a per-target counter would be the first refinement); whether
   to plan a step at a commit before any hint arrives (round 0 of v16–v23,
   dropped by R25); whether one worker thread per instance suffices (R17; a
   pool would have to keep the one precomputation's steps in order, §9.2);
   and whether auth_wal should spawn the thread at all (clarification 13):
   firewood spawns its own (a rayon pool of 16 for parallel proposal
   hashing, one thread per top-level child, and a persistence thread), and
   its FFI proposes on the calling goroutine's thread, so a step's hashing
   already runs on a Go-managed thread plus firewood's pool whoever drives
   it. The alternative with no thread of ours is a `drive()` entry point
   that runs the worker's next step on the caller's thread, called by the
   Go shim from a goroutine it spawns per hint, with `finish` draining the
   channel itself when no driver is active; the chain state is the thread's
   locals (R28), which a driver would own instead, and a Rust caller (the
   benchmark) would supply its own driver or run synchronously. To decide at milestone 2 with the
   benchmark.
6. **Durability and bootstrap.** The set and policy state are consensus
   state. A real write-ahead log of committed batches replayed into the set
   (which is why the layer carries the name), or a replicated snapshot, is
   needed before any validator may restart or state-sync. Bootstrap must
   restore `height`, the capacity, the set and the policy state (which the
   root checks through `policy_digest`, §7.3) so that `Root()`
   after a reopen equals the last committed root (as `qmdb-ffi`'s reopen test
   checks); until then `New` requires a fresh engine (§1).
7. **Policy digest.** A term of its own again (R25, §7.3): `state_digest`,
   a full hash of the eviction order per proposal, chosen over an
   incremental accumulator because policy state will change often across
   runs and the full hash needs no maintained invariant (clarification 21).
   The history is in §7.3; the choice is the policy's and milestone 1
   measures it.
8. **Adaptive capacity (R4, future).** v0 has fixed parameters and no
   controller hook (§6.4). When one arrives, its inputs must be digested into
   the root or be a pure function of the parent node (consensus-state rule,
   §1): the capacity is already a field of the snapshot and a node term of
   the preimage (R23), so a controller would change that field per node, and
   a child proposed on a live parent must use the same effective capacity as
   the same child proposed after the parent committed. A byte-based capacity
   belongs to the same change.
9. **Speculation on QMDB.** Rejected in v0 because QMDB's root covers the
   op history, so reverts do not restore the root. A QMDB variant would need
   the structure to be a pure function of (parent, batch), as an earlier draft
   had; deferred with QMDB itself (R13).
10. **Firewood-side TODO** (R15; tracked in `docs/TODO.md`). Confirmed
   against the source (`manager.rs` `commit`, `nodestore` `parent_hash_is`):
   firewood lets proposals be built on uncommitted proposals but commits one
   proposal at a time; committing a child of an uncommitted proposal fails
   with `NotLatest`, so a chain costs one revision per step and firewood
   recomputes intermediate hashes along a path once per step that touches it
   and once more when `F` reverts a key a step wrote. Accepted for now with
   pessimistic prediction (§9.2). Future firewood changes, in order of value:
   (a) a cascade commit that commits `Proposed` ancestors oldest-first inside
   one critical section, removing the caller-side ordering and the mid-chain
   failure window; (b) a squash commit that merges a chain's node stores into
   one revision, removing the window-sizing cost; (c) hashing deferred or
   shared across a chain so a path touched by several steps is hashed once;
   (d) a native warm-path call (no longer needed by auth_wal's hints, R26;
   the benchmark baseline's read-through would use it); (e) a
   restore op, `Restore(key)` in a proposal's batch, that puts back the
   value the proposal's base holds for `key` (or deletes it) without the
   caller reading it, so `F`'s reverts (§9.1) need no read through
   `P.storage_view` and no value copy; with (c) the path is hashed once.
   The read-back revert is kept as is by decision (clarification 14).
11. **Hint semantics.** Account hints mean "written or touched" (§9.4); if
   the inflation of `new` matters, `Prefetch` gains a read/write flag or the
   hook filters touched-only accounts.
12. **Lattice-hash parameters.** 1024 × u16 lanes follows published LtHash
   practice, and expanding a 256-bit item hash with a stream cipher (ChaCha8)
   instead of the hash's own XOF is the standard hash-then-PRG construction;
   the set commitment is consensus-critical, so
   the collision-resistance margin of the lane parameters and the PRG choice
   should be reviewed before a network relies on them.
13. **Write-set data structure cost: decided three times (R16, R18, R20; §4).** The
   first micro-benchmark round showed a persistent ordered map (`im::OrdMap`)
   at about 7 µs per insert into a clone of a 500k-entry map, a persistent
   HAMT at about 3 µs and a plain `BTreeMap` at about 0.5 µs at 500k, against
   firewood's propose at about 1.6 µs per op on a hot working set: hence the
   10k default capacity. The second (`cow_base`, per block at 10k with 2k
   writes) showed the in-place map's undo record costing as much as copying
   the whole map (1.4 ms either way), the copy with `Arc` values at 0.94 ms,
   the persistent map at 1.9 ms with 60 % slower lookups, and a flat sorted
   array at 0.45 ms with 25 % slower lookups: hence the full copy per commit.
   The third (`cow_base`, "full copy per proposal") showed the copy with the
   policy's maps and the batch applied at 1.9 ms (FIFO) and 2.2 ms
   (LRU-write), and the bulk eviction of half the set at 2.2 ms more: hence
   the full copy per proposal, which also removed the overlays. Remaining
   question: whether the per-write overhead (entry insert, commitment update,
   policy update) lands near the 0.5 µs target once milestone 1 exists; the
   `create` and `zipf` baselines are the yardstick.
14. **Snapshot refinements**, to measure in milestone 1 rather than decide
   now: the policy's reverse index duplicates what the set knows (a key's
   presence; `writes` could receive it per key) and, if the set stamped its
   entries with their heights, the order could be an index built lazily over
   them, one map to copy per proposal instead of three, about half of the
   2 ms;
   the registry could be published through an atomic pointer swap
   (`ArcSwap`) so readers resolve the head and revisions with no lock at all
   (today one uncontended mutex hold, about 15 ns, §10); the KZG slot map and setup (§7.2) must be designed for the copy (map in the
   entries, setup behind an `Arc`); the flat sorted array above if writes per
   block grow relative to reads.

## 13. Layout and milestones

```
auth_wal/
  Cargo.toml                  workspace root; lib (rlib + staticlib), features: ffi, firewood, ethhash
  src/lib.rs                  AuthWal<K, V, C, P, S> (the writer, &mut self) and Reader, both over Arc<Shared>; Config, Stats
  src/types.rs                Key/Value bounds, Hash, Digest, EMPTY_ROOT, BatchOp<K, V>, Error
  src/set.rs                  Snapshot (the whole set at one node) and its batch application, Entry<V>, Entries
  src/policy/{mod,fifo,lru_write}.rs   EvictionPolicy (writes / hint_writes / reset_hints / state_digest), Marks; the v0 policies, each with its own watermark
  src/vc/{mod,lthash,merkle}.rs
  src/root.rs                 root composition (§7.3)
  src/lattice.rs              lattice-hash accumulator and item encoding shared by lthash and MemoryStorage
  src/storage/{mod,memory,firewood,contract_tests}.rs
  src/proposal.rs             Node (immutable), Handle, Registry
  src/speculate.rs            Worker (submit / finish / reset over a channel), the thread (run, merge, build_step), SpecOp, Request (§9)
  src/ffi/                    C ABI, cbindgen -> auth_wal.h (names auth_wal_*), feature `ffi`
  *.go                        package ffi, mirrors qmdb-ffi files
  benchmark/                  workspace member; firewood-style driver (implemented)
    src/{main,target,workload,report,create,tenkrandom,zipf,single}.rs
  docs/                       this directory
```

Milestones

0. **Done (2026-10-26).** Benchmark suite (§11) with the engine-only
   baseline against the in-memory model and vanilla firewood; `MemoryStorage`
   and the firewood backend (§10) with the engine contract tests (items 1–7)
   and the state-function check; the lattice accumulator (§7.2). Every design
   change from here on is measured against the firewood baseline.
1. Core library: snapshots and batch application (§4, §5), FIFO + LRU-write
   policies with their tests (§6.1), lthash and the `merkle` reference
   scheme, registry and handles, plain path only, unit tests shared across
   policies and schemes; the benchmark's auth_wal target.
2. Speculative eviction and prefetch hints (§9) with the §9.3 tests (1–3 also
   against real firewood) and §9.5 metrics.
3. Benchmark sweep over capacity × policy × speculation rounds × hints; the
   provisional speculation defaults are decided here.
4. C ABI (the writer behind a mutex, a `Reader` beside it, §3) and Go `ffi`
   package with `Prefetch`; port `qmdb_test.go`; build
   Avax-pEVM against it with the import swapped in the qmdb adapter port; the
   libevm/adapter hint hook (§9.4).
5. KZG scheme and `ProveLatest` (the proof methods join the traits then,
   §7.4); QMDB backend (speculation off).

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

v12 (2026-10-26), milestone 0 implemented at the owner's request to measure
every design change: benchmark suite (§11, four workloads, baseline against
`memory` and `firewood`, CSV output), `MemoryStorage`, the firewood backend
(§10 mechanics (a)–(d) as built), lattice accumulator, engine contract tests;
`Value::from_bytes`; milestones renumbered.

v13 (2026-10-26), from the first micro-benchmarks (`benches/lattice.rs`):
lattice expansion changed from BLAKE3 XOF to hash-then-ChaCha8 (3.9× faster
per item on AVX2 hardware), `merge` for parallel accumulation; the persistent
map's per-insert cost recorded as open question §12.13.

v14 (2026-10-26), project owner's decision R16: data model rebuilt on
in-place structures (§4: committed `BTreeMap` mutated at commit, per-proposal
overlays, undo logs for the K revisions), reads through the overlay chain and
undo logs (§5), in-place policy state with overlay-aware queries and digest
deltas (§6.1), defaults capacity 10k and watermark capacity/2 with bulk
eviction (§6.2), commit as overlay merge with undo recording (§8), §12.13
closed.

v15 (2026-10-26), from the v14 review: the policy overlay carries each key's
event history and both queries are defined by replay equivalence (a FIFO key
evicted then rewritten in a chain, or written twice, otherwise reached a
different root than the same node proposed after its parent committed);
overlay invariant `writes ∩ removed = ∅` with last-op-wins and deltas from
the final overlay; orphan reads reconstruct the map as of their ancestor
through the undo logs, `RevisionStale` below the ring; memory bound restated;
planning under the read lock stated in §9.2/§10; commitment state kept on
nodes; `prove` over a `SetReader`; KZG slot map on `Committed`; terminology.

Round 11 (2026-10-26), targeted spot-check of v15 (design: §4, §5, §6.1,
§6.3, §8, §9.2, §10; skeleton: set, policy, proposal, lib, speculate). No
blocker: the reviewers confirmed by hand that the replay-equivalent policy
queries make a node proposed on an uncommitted parent reach the root of the
same node proposed after the parent committed, for both policies and every
chain tried. Fixed from their should-fix lists: removal deltas and `on_remove`
for a key the parent view lacks (no-ops, §4/§6.1); the per-op policy sentence
in §4 replaced by the one-effect-per-node rule, with the one-node vs two-node
`PrefixDelete` → `Put` asymmetry stated; engine reads of `Get` moved outside
the tree lock (§5, §10); per-node commit interleaving and the poisoning rule
made precise (§9.2); `Close` waits with no lock held; `Key`'s `Borrow<[u8]>`
note; test list widened (§6.1); `len` rule, cost notes, budget/E wording,
round-0 wait and benchmark column, lock-order leaves, KZG state note. The
skeleton gained a `Node` constructor and the accessors `commit`/`close` need,
a corrected prefix-scan formula, round-0 token reservation in the resolve
step, and terminology fixes.

Round 12 (2026-10-26), verification of the round-11 fixes. No blocker; the
design reviewer re-derived the consensus argument by hand for the six §6.1
chains and more, and confirmed it mechanically with a model of §4–§6.1/§8
over 4000 random two-node chains for both policies. Documentation fixes
applied: milestone numbers outside §13 corrected to the §13 numbering;
§12.4 marked decided; `New`'s validation rules and default `revisions`
stated in §3; "old through the parent view" pinned to the set entry (§4, §8);
§3 rows for `Commit` (partial failure), `Drop`, `Get`, `Revision`
(`RevisionStale` cases), `Propose` on a committed handle, `Close` (flag under
the write lock) and the two missing speculation options; §8 commit step list
(bucket order, failure); replay-order wording unified; §6.4 test pointer;
`E` wording; §12.5 widened to the knobs; §9.1 fallback rule; §5 step-3
wording; vocabulary, feature list, domain-tag and `Ord` notes. The skeleton
review (which also wrote throwaway bodies for the whole plain path and found
them to type-check) added: contract item 6, with the firewood adapter
dropping its database handle at commit so `Close` can close the engine while
the committed node keeps its view; round-0 chains registered for retirement
and `Close` retiring the head's slot; the speculation gate on round 0; and
documentation fixes (orphan slot retirement at `Propose`, warming for
non-qualifying nodes, `Evicted` bounds, undo prefix scan, `pred` reference,
sweep location).

Round 13 (2026-10-26), verification of round 12. No blocker. The firewood
adapter now keeps the failure of a consumed proposal, so a retried `commit`
reports it rather than `ProposalConsumed` (§9.2), re-bases a commit that
empties the trie to an empty readable state, and the contract tests assert
both plus item 5 (a built-and-dropped proposal leaves no trace on a later
root). Documentation: contract items "1–6"; `margin` default; eager orphan
retirement by `Commit`'s sweep; orphans forward all hint keys; reads after
`Close`; the lattice item's tag and tombstone fields pinned; default policy,
scheme and engine named; `round0_time` counter; milestone comments in the
skeleton aligned with §13. From the skeleton review: the post-commit re-base
and `Revision` look up committed revisions only (§10 (c)); `Prefetch` forwards
the not-in-set keys whether or not speculation is on (§9.4); the open-file
note on item 6. Nothing above nit level remained in either report once these
were applied, so the review loop stops here and milestone 1 starts. A final
round 14 over the adapter rework found no blocker and no code-level item;
its documentation point (the read side returns `Closed` after `Close`) and
small hardenings (re-base by the proposal's own root, single error prefix on
a retried failed commit, contract item 4's error-not-panic clause, round-0
timing in the commit path) were applied.

v16 (2026-10-26), R17 (clarification 4): precomputation is asynchronous. Every
engine call the hint path and the commit path used to make (link proposals,
hint warming) is a job for a worker thread owned by the instance; `Prefetch`
and `Commit` return after planning, `Propose` builds `F` on the links that
have completed, `Close` drains the queue and joins the worker (§3, §9.2,
§9.3 test 6, §9.4, §9.5 `worker_time`/`jobs_refused`, §10, §11, §12.5,
§13). Skeleton: `LinkWorker`/`WorkerJob` in `speculate.rs`, the
`InFlightGuard` travels in the job again, `Shared` owns the worker (spawned
first, then attached to a `Weak` reference).

Round 15 (2026-10-26), review of v16. One blocker: the firewood adapter
serialised every operation on a proposal handle behind a mutex, so a link
being hashed on the worker would have stalled `Propose` (building `F` on that
link) and `Commit` (committing it), and the test for the property ran only on
the memory model, which has no such lock. Fixed: the adapter's handle state
is a read-write lock (§10 (e), exercised by a concurrent-propose contract
test on both engines), a chain plans no links after a child built `F`, and
`Commit`'s one bounded wait (a link planned before `Propose`) is stated
(§9.2, §9.3 test 6 now on firewood too, §11). Should-fixes: `jobs_refused`
means queue full or worker gone (closing cannot refuse: `Close` waits for
in-flight jobs before stopping the worker), with a refused link job aborted;
the worker's queue is bounded for warm jobs, drops queued jobs once closing
began, skips a link job overtaken by a commit, and catches and counts a
panicking job (`worker_panics`); `New` fails with `WorkerUnavailable` if the
thread cannot start, the worker is spawned before the shared state and
attached to a `Weak` reference afterwards (no `Arc::new_cyclic`); the hint
path's guards are stated per branch and a `Reservation` pairs token and
guard; wording that still placed engine work on the hint path or in the
committing thread corrected (§9.2, §10, §11, skeleton).

Round 16 (2026-10-26), verification of round 15. No blocker. Applied: a
panicking link job retires its parent's chain (a chain with a half-updated
`sent` could otherwise drive a later `F`); the memory model's handle carries
firewood's read-write lock shape so §9.3 test 6 can run with synthetic
latency on both engines; the bounded `Commit` wait generalised to any handle
with a link in flight; `used` set only on the speculative outcome and the
second-child trade-off stated; closing semantics of dropped jobs, queue-bound
and overtaken-job wording, the worker's two chain-mutex acquisitions, extra
§9.5 counters, `jobs_in_flight` naming, and `shutdown` closing the attach
channel and resetting the queue depth.

Round 17 (2026-10-26), verification of round 16, with the memory model's
lock behaviour measured. No blocker. Applied: the `Commit` wait bound now
counts the node's own `F` handle too (a link planned by the next block's
hints before this node's commit), one in-flight link per handle committed;
`append` is ordered to be panic-safe because the worker's retire runs after
unwinding released the chain mutex; a failed engine root check or a
`ProposalOrphaned` on an overtaken job is dropped, not a failure; the memory
model's commit takes its handle guards before the engine lock so a bounded
wait no longer stalls engine-wide calls; test 6 preconditions and knobs
stated.

Round 18 (2026-10-26), verification of round 17. No blocker. Corrected the
panic-safety argument: `sent` also drives `F`'s reuse rule, so no ordering
of `append`'s mutations is safe on its own; phase 3 now runs `append` inside
`catch_unwind` under the slot's mutex and retires the slot before releasing
it, with the worker's retire covering phase-2 panics. Test 6's "no wait"
precondition includes the parent chain's link having completed; the bound is
stated per chain tail (two links per speculative node, one per plain node);
`run_link`'s root-check wording, the `Proposal::commit` doc, `link_failures`
glossed, the memory model's propose checks `committed` under its handle
guard, wording nits.

Round 19 (2026-10-26), verification of round 18: no blocker and no should-fix;
the panic paths, the per-tail wait bound and test 6 were walked and found
correct. Seven wording nits applied (the retire's mutex guard is taken
outside the `catch_unwind` closure; `append` takes `in_flight` before its
tail check; the `F`-handle wait may also come from a quiet child's chain;
`Propose` may wait for an append step; the `Commit` row and `round0` doc
scope "never waits" to round 0; `worker_panics` gloss; contract item 4 names
`StorageView::propose`'s check). The loop on R17 closes here; milestone 1 is
next.

v17 (2026-10-26), R18 (clarification 5): the project owner questioned the
number of locks under a blockchain workload and the need for an undo log when
writes are copy-on-write. The audit (§10) found the undo log and every lock on
the read path to be consequences of R16's in-place committed map, not of the
workload, and the micro-benchmark (`cow_base`) found the in-place map's undo
record to cost as much as copying the whole 10k-entry map. Decision: the
committed set is an immutable snapshot copied once per commit (§4), nodes
hold their base snapshot and overlays immutably (§8), reads take no lock
(§5), the undo logs, `UndoView` and the stale-by-undo cases are gone, the
tree `RwLock` becomes a registry mutex held for nanoseconds plus a commit
mutex only `Commit` takes, the in-flight count's close protocol is stated
without a lock, and every remaining lock is justified in §10. Skeleton:
`Snapshot` replaces `Committed`/`Undo`/`UndoView`, `Node` carries
`base`/`overlays`/`snapshot` (a set-once cell), `ProposalTree` loses
`read_path`/`undo_logs_above`, `Shared` gains `commit_lock` and an atomic
`closed`; `Arc<[u8]>` implements `Key` and `Value` for the FFI types.

Round 20 (2026-10-26), review of v17 (design and skeleton). One blocker in
the skeleton: `propose_on` read the parent's base and overlays with two
separate commit checks, so a parent committing in between could pair the
grandparent's snapshot with no overlays; `Node::child_base` now reads both in
one observation (§8). No blocker in the design. Should-fixes applied:
`EvictionPolicy: Clone` (the snapshot copy clones it, §6.1); `F`'s revert
reads moved out from under the chain mutex (`finish` returns the plan,
`Propose` reads and proposes with no lock, `used` is set at plan time; §9.1,
§9.2, §10); the budget-exhausted sweep given its criterion and place (the
hint path's registry hold; round 0 does not sweep; §9.2); the storage dispatch
re-checks the engine root after proposing (§10 item 4, closing a window in
which a race-built orphan could have read post-commit storage); `advance` and
`Close` hand snapshots, ring nodes and chains back to be dropped after the
registry hold, and §10's hold list says so; the memory count includes the
policy maps and the K + 1 retained snapshots (§8); the worker's queue sender
lost its mutex (a stop message replaces closing the channel); the adapter's
database-handle mutex and the handle parent cells listed honestly (§10); KZG
row and FFI types on `Snapshot`/`Arc<[u8]>`; close-protocol orderings stated
(`SeqCst`, handle counting). Nits: wording in §3, §4, §8, §9.2.

Round 21 (2026-10-26), verification of round 20 (design and skeleton). No
blocker; nothing changed the model. Applied: a handle-level `Prefetch` takes
the registry lock once to qualify its node (§9.2, §10); the chain-holder list
sits between the registry lock and the chain mutexes in the lock order and
both sweeps hand retired chains back for dropping after the hold (§10,
skeleton docs); a `Proposal`/`Revision` handle is built inside the registry
hold that checks `closed` (skeleton docs); `used` is set when the plan for `F`
is taken (§9.2); the round-0 link job goes through the storage dispatch's
root check rather than its own (§9.2, §10 item 4, `LinkJob` loses a flag);
`Close` may report `ActiveHandles` for a `Propose` still building (§3, §10
item 6); the memory count includes B-tree internal nodes (§8, 180–270 MB at
K = 128); `count_fallback` added for the caller-side fallback; the worker's
queue depth no longer reset at shutdown; type-alias and doc nits.

Round 22 (2026-10-26), spot-check of round 21. No blocker, nothing above
stale wording: the chain-holder list was still called a leaf lock in §9.2 and
§10 and the skeleton's lock-order header omitted it (both now place it between
the registry lock and the chain mutexes); §10 item 6 states the `ActiveHandles`
case; `links_dropped_unused` glossed once and the same way everywhere; the
adapter's database-handle count, the memory rounding, round-0 registration and
small doc nits. The loop on R18 closes here; milestone 1 is next.

v18 (2026-10-27), R19 (clarification 6): the project owner went through the
v17 lock list item by item: keep the registry mutex with small critical
sections; replace the commit mutex by a parameter type that forbids overlap;
the chain should not have two writers (the caller submits requests, the
worker marks a slot ready); keep the firewood handle lock with the TODO; and
why parent pointers at all. Decisions: writer operations take `&mut self`
and `Commit` consumes the handle, readers and handles are `Send + Sync`
(§3 threading contract; the C ABI keeps a mutex around the writer); a link
is a caller-allocated slot the worker fills once, chains are writer-owned,
the one-link-in-flight rule and the chain-holder list are gone (§9.2); nodes
are immutable and list their uncommitted ancestors, committed-ness is
registry membership, so there is no parent pointer and no committed flag
(§8); the engine is owned by the writer and closed through `&mut self`,
pinned views are always proposal handles (`head`, contract item 7), a
committed handle proposes on the committed state with the root check inside
the adapter (item 4), and `prefetch` moved to `ReadView`, so the worker holds
no engine reference (§10); the lock inventory is the registry mutex and the
firewood handle lock, with every hold listed (§10) and the primitives
measured (`sync_primitives`). Skeleton: `AuthWal` (writer) and `Reader`,
`Registry` replaces `ProposalTree`, `Node` loses `snapshot`, `parent` and
`speculation` and gains `ancestors`, `LinkSlot`/`Chains` replace
`ChainSlot`/`SpeculativeChain`'s mutex, the adapters implement `head` and
the committed-handle `propose`.

Round 23 (2026-10-27), review of v18 (design and skeleton). Two blockers in
the design's lineage rule, both fixed in §8: a child proposed on a live node
whose own ancestors had committed inherited the stale base and the whole
ancestor list, so under the adapter's pipelined flow the lists grew without
bound and, once an ancestor left the ring, a legitimate descendant of the
head was judged an orphan (now every child is re-based on the head's
snapshot with the uncommitted suffix as its ancestors); and lineage entries
were judged committed by root equality, which is unsound for storage
handles (an equal-root live sibling's engine proposal was never committed, so
committing its child would fail with `NotLatest`): lineage is now classified
by node identity, with root equality kept for the node itself (a sibling
sharing the head's root is treated as the head for everything, a node
sharing a retained root is superseded). Should-fixes: the classification
no longer counts a committed prefix (a chain deeper than K, an ancestor that
left the ring); `Prefetch` on a non-qualifying node warms only (§8 agrees
with §9.2/§9.4); the threading contract states that every `Prefetch` takes
the C ABI's writer mutex and may wait on block processing; a failed `Commit`
consumes the handle (§3, §8, §9.2). Nits: `worker_panics` gloss; "an `Arc`
clone of the head's snapshot"; `Close` takes every ring entry but the head;
the hint's one hold classifies the chains' parents; the first-link race is
settled by the chain drop, with the root check as backstop (§9.2, §10). In
the skeleton (no blocker): an equal-root live parent is handled as the head
for snapshot, pinned view and chain alike; `Close` checks the live-handle
count and sets the closed flag under one registry hold and a reader's
`Revision` re-checks the flag inside its own hold (a reader could otherwise
slip a handle past `Close`, §3, §10); the memory model counts live
uncommitted proposals so its `close` reports `ActiveHandles`, and a test
races a committed-handle `propose` against a commit for the after half of
item 4; `Speculator::reserve` gives both planning paths one reservation
step; `run_link` resolves its parent slot without blocking (a pool would
wait); `commit_node` sweeps and retargets on the error path too once a node
is installed; a dropped writer sets the closing flag so the detached worker
drains cheaply; `dropped_unused` keeps its §9.5 meaning; the contract list
names `ProposalConsumed` and the failed-commit rule; the memory model fails
the oldest handle of a `NotLatest` chain commit (item 1); wording nits.

Round 24 (2026-10-27), verification of round 23 (design and skeleton). No
blocker; the re-basing, the identity-based classification, the first-link
argument, the `Close` ordering and the hold counts were walked and found
correct. Applied: a chain is kept by the sweep only if its parent is the
head node by identity or descends from it, so an equal-root sibling never
keeps a chain after the commit (§9.2, skeleton `commit_node`,
`prefetch_node`, `propose_node`); the storage contract's item 4 names the
chain drop as the argument and the root check as the backstop (`storage/mod.rs`);
items 1 and 6 state that a failed handle no longer pins the engine and that
`close` reports `ActiveHandles` while a live one exists (§10, memory model
noted); case 4's gloss covers a node committed and retired; the memory count
adds one snapshot per level of pipelining; the first-link race is stated for
every chain whose pinned view is committed, with the reason no surviving
chain is affected; wording nits (§3 `Prefetch` row, §8 Commit and case 1
scope, §9.2 drop after the hold and the warm-only reason). Skeleton: the
memory model no longer consumes a child whose ancestor's commit failed
(firewood leaves the child live), and the contract test now tells the two
apart (`ActiveHandles` while the child lives); the sweep keeps a `Head`
verdict only for the head node itself; `settle` does not count
`dropped_unused`; the overtaken-propose test widens its timing margin; the
storage contract states idempotent `close`, post-close behaviour and
`revision(EMPTY_ROOT)` on a fresh engine; `commit_node`'s error path lists
what still runs; FFI doc and drop-order comment nits.

Round 25 (2026-10-27), spot check of round 24 (design and skeleton). No
blocker and nothing above stale wording: §8's node-contents sentence still
gave the pre-round-23 ancestor formula (now "the parent's uncommitted suffix
as classified at `Propose`"); §10 item 4 and the storage contract claimed
`F` takes the committed path (it chains on a completed link; the writer's
committed-path use is the plain proposal of a child whose parent's pinned
view is committed); the first-link enumeration names the equal-root sibling;
the Drop paragraph's superseded-ancestor clause is conditioned on the ring;
the firewood (a) and §5 pinned-view wording follow items 1, 6 and 7. In
the skeleton: a contract test for item 3 (a child proposed on an uncommitted
parent commits after the parent was committed by its own call) was missing
and is added; the failed-commit test also checks `propose`; the contract
names the error variant and says a `head` handle's `commit` is
`ProposalConsumed`; `finish`'s fallback wording and the FFI's writer-call
list follow §9.1 and §3. The loop on R19 closes here; milestone 1 is next.

v19 (2026-10-27), R20 (clarification 7): the project owner asked why a node
needs `base`, `ancestors` and `overlays` under copy-on-write, and why a
`Proposal` type exists beside `Node`. Decision: every proposal is a full
copy of its parent's set with the batch applied (§4; measured 1.9–2.2 ms per
proposal at 10k, the copy moves from `Commit` to `Propose` and `Commit`
copies nothing), so reads are one lookup (§5), the policy evicts on the
proposal's own materialised state and the replay-equivalence machinery of
§6.1 (overlay queries, `digest_through`, `PolicyOverlay`, `ChainOp`) is
gone, a node is its snapshot, its pinned view and one weak parent link (§8),
`Commit` requires the parent to be the head (the adapter never commits
otherwise; qmdb-ffi's cascade is dropped; `ParentUncommitted`), and the two
handle types collapse into one `Handle` newtype over the node with no shared
state (the Go layer counts keep-alives; the engine refuses to close while a
live storage proposal is held). The redundancy pass that followed removed:
the link budget (`max_links_total`, tokens, guards, reservations; links are
bounded by `rounds` × chains), the Rust-side live-handle count and closed
flag for reads, the capacity-controller hook (§6.4 → §12.8), the proof stubs
(`proof.rs`, `AuthProof`, `SetReader`, `Proof`, the `prove`/`verify`
methods of both traits; milestone 5 adds them back with the KZG scheme), the
`kzg` feature stub, the engine's `Revision` type with `revision()`/`get()`
and the `is_committed` probe (every storage read goes through a handle),
`Effective`/`SetStats`, the `Committed`/`Resolved`/`Chain` aliases, the
`HintView` enum and the duplicated read methods on the writer.

Round 26 (2026-10-27), review of v19 (design and skeleton). No blocker.
Design should-fixes applied: cascade-era wording in the `Commit` row and
§9.2 "Commit" (one node commits now; the head does not move on failure);
speculative chains refer to their node **weakly**, so a dropped hinted
proposal dies with its handle instead of living on until the next sweep (§8
Drop, §9.2, skeleton `SpeculativeChain`); the first-link argument restated
for one commit per `Commit` (the new head is the only node whose commit moves
the engine head); a child of a head-classified parent links to the registry's
head node; a pre-check failure of `Commit` consumes the handle too, stated;
`WithSpeculationMaxLinks` removed from §3; `Close`: hints are no-ops and
`Revision(root)` resolves only the head and live proposals afterwards. Nits:
lineage rule (3)'s terminal cases, what a weak link retains, the adapter's
read guard on the storage side of a read, "created a storage proposal" for
the pinned view, tombstones as present entries for the policy (§4, §6.1
tests), the shared-state list and the stop mechanism in §10, contract item
2's wording, the `K ≥ 2` reason in §8; and the engine-level `propose`
removed from the storage trait (`head()?.propose(..)` is the one way, used
by the benchmark baseline and the contract tests). The redundancy pass the
owner asked for (clarification 7) then removed, in the skeleton: the `Parent`
and `HintView` enums (`propose` and `prefetch` take `Option<&Handle>`), the
`Chains` and `Speculator` structs (a `Vec` of chains and the round-0 estimate
are fields of the writer; counters, in-flight count and closing flag are
fields of the shared state; `plan_link`/`finish`/`run_link` are functions),
`SpeculationConfig` (the writer reads `Local`), the `LinkFailure`/`LinkResult`
enums (a slot's cell holds the link or nothing; the reason is logged at fill),
`LinkJob` (the slot carries its base), the `ReadView` trait (folded into
`StorageProposal`), the engine-level `propose` (`head()?.propose(..)`), the
`Unsupported` error (nothing constructs it before milestone 5), the
`lthash`/`merkle` feature gates, the firewood config's dead
`cache_read_strategy` field, the memory model's `history` (its staleness
check is arithmetic on the commit sequence), `Stats.capacity`,
`AppliedSummary.deletes`, the `prefetched_keys` counter, the `Snapshot` and
`SpeculativeChain` getters nothing called, and the micro-benchmarks of the
decided alternatives (their figures stay recorded in this document); internal
types are `pub(crate)`. The five type parameters remain the largest
line-count lever (a bundle trait, to do at milestone 1 before bodies exist).

Round 27 (2026-10-27), spot check of round 26 (design and skeleton). No
blocker and nothing above stale wording: the `Drop` row still let a chain
keep a node alive; §9.3 test 6 named the removed `Speculator` for
`jobs_in_flight` (now `Reader::jobs_in_flight`, listed in §3); the "Slots"
paragraph described the removed link job and failure reasons; item 6, the
memory-model paragraph, §4's bench groups, the §2 diagram, a duplicated
plain-path sentence in §9.2 "Commit", the settle direction in "Bounds", §5's
read-lock sentence and the §14 round-26 wording. The loop on R20 closes
here; milestone 1 is next.

Addendum (2026-10-27), after the owner asked why a node tracks
`owns_storage` and a "storage path": the first is derivable (a node's own
storage proposal is a different handle from the head's; an inherited view is
the same `Arc`), the second was a misnamed metrics record. Both removed with
the node's batch summary: statistics are counted at `Propose` (§9.5), and
the one bit `Commit`'s poison rule needs, whether the final proposal chains
on speculative links, is the node's link count (§8, §9.2).

v20 (2026-10-27), R21 (clarification 8): the owner asked why the registry
keeps `head`, `live` and `revisions`. `head` was a cache of the newest ring
entry and `order` a second copy of the ring's keys: the K-ring is now one
deque in commit order whose back is the head (a lookup by root scans at most
K entries, 141 ns for a miss over 128 with the nodes hot against 28 ns for
the map it replaces, `registry_ring`). `revisions` was a copy of `Local.revisions` for
retirement: `advance` takes it. `live` stays, because `Revision(root)` must
resolve a live proposal's root (`qmdb-ffi`'s `Database.Revision` and
firewood's `view` do, and the adapter's `Reader(root)` depends on it, R1),
reduced from a map of lists to a flat list of (root, weak link) pairs with
one sweep rule. With the addendum above (the node's `owns_storage` and
metrics record), nothing in a node or the registry is derivable from
something else (R21).

Round 28 (2026-10-27), two reviewers on v20 (design; skeleton and
redundancy). No blocker. Design should-fixes: the `Close` row said the ring
is gone after close (it holds the head); the live-index justification now
names the hint path (§9.4), which needs the live node by identity; the live
list is pre-sized like the ring. Skeleton: the chain sweep upgraded weak
parents inside the registry hold, where a temporary could be the last
reference and free a snapshot under the lock (upgraded before the hold now,
released after); the registry's `Stats` duplicated the atomic counters
(removed: the set statistics are atomics beside the speculation counters,
and the registry is the ring and the live index, nothing else);
`Shared.config` had no reader on the shared path (the writer owns the
configuration); the writer's `closed` flag duplicated the closing flag (one
flag). Redundancy accepted: the `Superseded` lineage case is subsumed by the
walk (a node at a retained revision's root has a height below the head's, so
the walk never reaches the head; `Commit` of a retained revision handle
reports `ProposalOrphaned`, which the adapter never observes), and the walk
compares pointers only, no ring node dereferenced (67 ns for a scan of 128,
`registry_ring`); `register`'s idempotence
clause and scan and the `Node::parent` accessor are gone (the `LiveIndex`
alias stays: clippy's type-complexity lint wants one, and its doc now points
at the field). The scan figures are labelled hot (a cold ring scan is up to
one cache miss per entry). Nits applied.

Round 29 (2026-10-27), spot check of round 28 (design, skeleton, project
description). No blocker and nothing above wording: the height argument
said "below" where a twin of the head is at the head's height ("no higher
than" now, with the ancestors strictly below); one plural "flags" survived
the removed writer flag; the §9.2 first-link argument still named a
superseded class; "no node dereferenced" is true of the ring only; the
`Shared.registry` hold list lacked `close`'s clear; the live list's
pre-sizing is stated as a bound; the walk through handle-retained retired
ancestors is noted; the project description now names the hint path. The
loop on R21 closes here; milestone 1 is next.

v21 (2026-10-28), R22 (clarification 9): the owner asked why the registry
needs `classify` and `clear_ring`, whether the root needs a policy digest
beside `policy_name` and `policy_params`, and what `prefix_deletes` is for.
Applied: the lineage walk is gone; `Commit` checks that the parent link is
the head node (`ParentUncommitted` otherwise, `ProposalOrphaned` on a dead
link, `ProposalConsumed` for the head's root), `Propose` builds on the head
node for an equal root and otherwise on the node as given (as firewood and
qmdb-ffi do; the adapter never proposes on a finished line), and chains are
kept for the head and for live nodes above the head's height
(`Registry::is_current`). `clear_ring` is gone: nothing outlives the writer
and its readers in a real path, and committed views do not block the
engine's close, so `close` is shutdown the worker, drop the chains, close the
engine; `advance` also drops live entries K or more heights below the head
(one window for both lists). The policy digest is gone from the root
(§7.3): agreement on evictions follows from determinism, the digest only
detected a divergent implementation one eviction earlier, and it cost a
second lattice accumulator updated on every write; equal roots no longer
imply equal policy state, which nothing relied on. `PrefixDelete` is
rejected by `Propose` as QMDB rejects it: the set cannot apply it to keys it
does not hold, the adapter never emits it, and its support reached into the
batch semantics, the canonical storage order and the speculative final
proposal; the engines keep the op (`Key` keeps its `Borrow<[u8]>` bound for
the memory model's own range scan).

Round 30 (2026-10-28), two reviewers on v21 (design; skeleton and
redundancy). One design blocker: dropping the policy state from the root
broke "equal roots are interchangeable parents", which the adapter relies
on (it stages a child on every live proposal under the parent's root and
returns one candidate's root; under `lru-write` a same-value rewrite and an
empty batch share a root with different policy states, so a child's root
would depend on arrival order). Resolved in v22 by carrying each key's
policy datum in the set commitment's item (§7.2, §7.3): one accumulator,
cheaper than v20's digest, and the owner's premise (predictions are local)
holds for `likely_victims` while the per-key state that decides actual
evictions is committed. Skeleton: the preimage's static terms were
unreachable from a node (now `root::Terms` on the writer, built at `new`;
a node keeps only its `metadata_hash`, the one term not derivable from it);
`advance`'s window rule would have upgraded live entries under the hold
(the live index caches root and height); the chain helpers took `&mut
self` and would not have borrowed (over the field now, `chain_for` written);
two unreachable "retire the chain" clauses, the node's preimage copy, two
derivable summary counts, the `Evicted` newtype, `reused_ops`, per-impl zero
`params_digest`, `Handle::height`, `Snapshot::is_empty` and the writer's
`stats`/`config` accessors removed; `on_remove` is for tracked keys only;
`commit` upgrades the parent link before its hold. Stale wording fixed
(§2 diagram, §4 range scans, §6.1 prefix deletes, §8 sweep rule now stating
the live window, §12.6, §12.7).

v22 (2026-10-28), R23 (clarification 10), from two owner remarks during
round 30: `Snapshot::apply` takes only the batch, the capacity being the
snapshot's field (a node term of the preimage, the field a future adaptive
capacity changes per node) and the low watermark the policy's parameter
(`low_watermark`, `eviction_count`, in `policy_params`; `Consensus` keeps
the capacity only); and `Registry::is_current` is gone: a chain needs no
test of its node, `Commit` drops the ex-head's chain by identity and the
sweep drops dead nodes' chains with no registry hold, a chain on a finished
line dies at its first link (the engine refuses it, §10 item 4) or with its
node.

Round 31 (2026-10-28), two reviewers on v22. No blocker in the design; the
datum-in-item construction was walked for both policies (the round-30 pair,
two conflicting candidates, delete-then-put in one batch) and the first-link
race re-enumerated without the sweep test. Two skeleton blockers, one line
each: the policy module's header still carried v21's "the root does not
commit to the policy's state", and the planner's formula read a private
snapshot field (`Snapshot::eviction_count(len)` now serves `apply` and the
planner). Applied: `params_digest`'s default covers the watermark (an
override must keep it); `RootPreimage` was derivable (`Terms::combine`
hashes the node's terms directly); `Config::with_capacity` had no caller
(the watermark default has one Rust home, `policy::default_low_watermark`,
named in the ffi module doc); `storage_proposals` and `total_storage_ops`
are derived in `stats()` with the counting rule stated (a fallback counts
`plain`, a `None` plan nothing); the unused `Hash` bound left `Key`;
"dies at its first link" was an overclaim: when the commit moved nothing in
the engine a finished line's link is accepted on the unchanged state and
wasted, at most `rounds` links until the sweep after the node is dropped
(§3, §8, §9.2, §9.4, §10 item 4, the project description); the §7.1 trait
listing shows the datum in `apply`'s items; §4 states when the old and new
items are read; §8 Commit no longer claims a hold for the sweep; `Handle`
lists `Get` and `Root` only; the live index is triples; the equal-root
property joins the milestone-1 tests; the `params_digest` encoding is in
§6.1; hints on retained revisions and the chain bound count them.

Round 32 (2026-10-28), spot check of round 31. No blocker; two arithmetic
precisions in doc comments and stale wording: `reused_ops` is taken on the
plan's ops before the reverts are prepended (and `reverted_ops` is the
revert count); the derived `total_storage_ops` saturates, since the two
counter sets are bumped at different moments; §9.5 now states the
derivations; §8 and §7.4 said a node keeps its preimage (it keeps its
`metadata_hash`); §4's `Key` listing still had `Hash`; the `Handle` doc
listed `height`; the benchmark had its own `capacity / 2` (it calls
`policy::default_low_watermark`); `Terms` is not built from consensus
parameters; the sweep doc names the adapter's own commit call and the
unchanged-handle case; the R21 record says triples. The loop on R22 and R23
closes here; milestone 1 is next.

Clarification 11 (2026-10-28), the owner on the trait: the low watermark
need not be explicit; it is a policy parameter, and some policies have none.
Applied as a refinement of R23: `EvictionPolicy::low_watermark` is gone,
`eviction_count(len, capacity)` is the policy's own rule (required; at least
`len − capacity` victims once over capacity, so one eviction returns the set
to capacity; `New` checks the boundary instance `eviction_count(capacity + 1,
capacity) ≥ 1`), `params_digest` is required again (every policy states
its own parameters, zeros if none); the v0 policies keep their watermark
privately and share `watermark_params`; `WithLowWatermark` is handed to
them and ignored by a policy without one. Round 33, the spot check, found
the contract stated as "at least one victim once over capacity", which with
one eviction per proposal does not keep the set at capacity: corrected to
"at least `len − capacity`", `New`'s check being its boundary instance; §3's
`InvalidConfig` list names the policy check instead of the v0 instance;
§6.1 states the third invariant (`eviction_count` and `select_victims` read
only the committed state and digested parameters); `capacity + 1` is a
checked add. The loop closes here; milestone 1 is next.

v23 (2026-10-28), R24 (clarification 12): the owner found the chain of link
slots overly complicated and gave the ordering argument that removes it: the
validator executes one block at a time and a block's hints cannot arrive
before the previous block's root is computed, so there is one
precomputation at a time, every link builds on the latest view whether or
not the previous one has finished (the worker runs jobs in order), nothing
is cancelled (a firewood propose cannot be stopped, so a cancel flag meant
nothing), and `Propose` waits for the queued links, which amends R17 for
`Propose` alone (`Prefetch` and `Commit` still never wait). §9 is rewritten:
the writer owns one `Speculation` (target, `sent`, hints, link count), the
worker owns the latest view, the jobs are `Base`, `Link`, `Flush`, `Warm`;
the retarget rule (a commit retargets to the new head unless a live node
above it is targeted; a hint on a node above the target retargets; other
hints only warm) replaces the per-node chains, the sweep and the slot
cancellation; the committed node's own links are never in flight during its commit
(qualified in round 34: the next block's link may be, on the handle being
committed), so the first-link race of v18 to v22 no longer exists and
contract item 4 is a backstop.
Skeleton: `speculate.rs` rewritten (`Speculation`, `WorkerJob` with
`Flush`, `LinkWorker::flush`, `finish` returning the reverts and the ops);
`AuthWal` holds one `speculation` and a `retarget` helper; the chain sweep
and the chain helpers are gone.

Round 34 (2026-10-28), two reviewers on v23. No design blocker in the state
produced, one in the text and the skeleton: round 0 retargeted even when the
precomputation already targeted the new head (the pipelined case), throwing
away the next block's links, and "above" was height-based, which misfired
on competing lines. Now: the target is named by identity, `Commit` leaves a
precomputation alone when it targets the new head or a live child of it,
and a hint retargets only to a live child of the target (or, with no live
target, to the head or a live child of it). "No link in flight during a
commit" was an overclaim: in the pipelined flow the next block's link may
be hashing on the handle being committed, so the engine commit waits on the
handle lock for that one job (R17 qualified for `Commit`); a precomputation
a child built `F` on plans no further link (`used`). The flush queued
behind warm jobs: the worker now runs retargets, links and flushes before
warm jobs. Skeleton: `Speculation<K, V>` holds no node (the target is the
writer's), `finish` and `plan_link` need no engine type; `reset` is shared
by retarget and `close` (no double count); `mark_used` at the flush covers
`F` and the fallback; a failed `F` falls back to the plain path; `submit`
counts every refusal; the flush is counted done before its reply, so
`jobs_in_flight` is exact after `Propose`; `note_hints` returns the warm set
(one lookup per key); `Node::parent_is`. Stale chain wording in §3, §6.3,
§8, §10, §11 and §12.5 replaced; the live-index "sweep rule" is the pruning
rule; the R19 record notes its slots are superseded.

Round 35 (2026-10-28), spot check of round 34. No blocker; wording: two
§10 passages (contract item 4 and the lock-order paragraph) and the §8
Commit bullet still carried the "no link in flight" and height wording,
§9.2's R17 sentence lacked the `Commit` qualification, the project
description's R17 row and Clarification 12 likewise, and `round0`'s doc
named a `target()` accessor that did not exist (added). The owner's two
questions of the same day are recorded as clarification 13: the retraction
of a speculative eviction is the final proposal's revert (§9.1 now names
the case and §9.3 test 3 covers it), and the thread model is recorded as
the §12.5 option (firewood's own threads surveyed). The loop on R24 closes
here; milestone 1 is next.

Clarification 14 (2026-10-28), no round. The owner asked where the old
version of a change is kept for a revert; the answer (the parent's
immutable snapshot for the set, a read-back through the parent's pinned
view for a speculatively evicted key, no undo copy anywhere) stands as the
design. Decision: keep it as is, and record the future change as a firewood
API extension, the restore op of §12.10 (e); §9.1 carries the note and
`docs/TODO.md` the item.

v24 (2026-10-29, R25, clarifications 15–21). The owner judged the v23
speculation module overly complicated and, over seven exchanges, moved the
prediction into the policy: `on_write`, `on_remove`, `key_state`,
`eviction_count`, `select_victims` and `likely_victims` are replaced by
`writes` (consensus: a batch's keys in, the victims out, the marks
reconciled) and `hint_writes` (local: a hint in, what to send and what to
put back out); the policy keeps its own marks and never returns a victim
twice; the root commits to the policy state through `state_digest`, a full
hash of the order per proposal (the second accumulator of v20 and the
datum-in-item of v21–v23 are both gone; the digest is a term again, §7.3);
the writer keeps a working copy of the target's policy, consumed by the
first child; the pipeline is a pending queue, a worker and a flush behind
one mutex and condvar (no job channel, no sent set, no `used`, no round 0);
links carry retractions as well as evictions. Sections rewritten: §6.1, §9;
touched: §4, §6.2, §7.1–7.3, §8, §10, §11, §12.5–12.7, §12.14, §13.
Skeleton: `policy/{mod,fifo,lru_write}.rs`, `set.rs`, `vc/*`, `root.rs`,
`speculate.rs` and `lib.rs` rewritten; the battery is green before review.

Round 36 (2026-10-29), two reviewers on v24 (the design; the skeleton
against the design). No blocker. Design should-fixes: the target identity
outlived the working policy in v23 but not in v24's first text (consuming
the copy dropped the target, so a hint on the next block's node could only
warm and every commit retargeted): `Working { target, policy: Option<P> }`
keeps the target, and a hint on a consumed target retargets to it (§3, §8,
§9.2, §9.4, `lib.rs`); the §10 lock-free bullet still named the v23 channel
and in-flight count; §9.1's correctness sentence overstated what step (1)
restores, and the marks invariant is now qualified by "whenever the flush
answers a view". Skeleton should-fixes: views dropped under the pipeline
lock (retarget, clear, the install path) now leave the lock first (`Taken`,
a stale list); `links_dropped` counts the links of a failed generation and
a link built for a replaced generation, and `flushed` is set only when a
view is answered; a hint after `close` returns at the closing check and
`Worker::retarget` is a no-op once gone or closing; storage contract item
4's wording was v23's. Nits applied: the worker takes no warm job while a
flush is pending (and the flush wakes it afterwards); pushes are refused
once there is no latest view; `Delete` of an absent key as a root no-op
stated in contract item 5; `combine(capacity, height, …)` in preimage
order; `commit` returns `node.root()`; `worker_idle` qualified in test 6;
the margin wording; `rounds = 1`; the dedupe remark; the FIFO row; the
digest encoding; stale §1, §3 and §12.14 wording; clarifications 9 and 12
annotated; the R24 row completed.

Round 37 (2026-10-29), spot check of round 36. No blocker, no should-fix;
every round-36 item verified, the target-identity rule traced through the
sequential, pipelined, two-deep, competing-child and late-hint flows with
no loss of correctness. Wording: storage contract item 4 names the chained
proposal; §8's lineage and Propose bullets carry the consumed-target rule;
§9.2 and §9.3 test 4 cover a hint between two children of one target; §9.2
states the straggler-hint cost. The loop on R25 closes here; milestone 1 is
next.

v25 (2026-10-29, R26, clarification 22). The owner's five remarks on the
R25 shape: the watermark leaves the trait module (first into a
`policy::watermark` module, then, clarification 23, out of the crate's
shared code altogether: each v0 policy keeps its watermark, rule and digest
as its own, and the R16 default is the benchmark's and the Go `New`'s
convention);
`writes` answers the victims plus one last `Prediction` (no `confirmed`);
the marks are the marked set alone (no hinted set, no insertion count: each
hint predicts from its own keys and the margin, §12.5); the policy stays in
the snapshot and hints mark it in place under a per-snapshot lock, so
`apply` takes only the batch, the writer keeps no working copy and a second
child of the target is speculative too (round 36's consumed-target rule is
gone with the copy); a retarget resets the new target's marks
(`reset_hints`, the one method added); warm jobs are gone (a step is the
warming; `StorageProposal::prefetch` stays for the benchmark baseline;
firewood's forced cache strategy is a milestone-2 knob); the speculative
proposals are called steps (`SpecOp`, `Step`, `steps`, `min_step`). Not
reviewed: the owner asked to see the pipeline in this shape before deciding
its simplification.

v26 (2026-10-29, R27, clarification 24). The owner found the split between
the `Pipeline` state struct and the `Worker` handle misleading and restated
the model: the worker manages the speculative side of the engine and takes
two kinds of request, a batch of ops and a block end; the interface for
auth_wal is `submit` and `finish` (block-wait for the latest view); a step
is built from everything queued, merged into one write set. Applied: the
queue is plain data under the lock, `Worker` is the interface (`submit`,
`finish`, `reset`, `shutdown`), the thread body `run` holds the worker
logic (take all, `merge` by key with the last op winning, build, install
under the generation), and `Pipeline`, `next_step`, `Step` and `push` are
gone. `reset` stays as the third request because a retarget must reset the
marks and the chain together, and a child that inherits its parent's view
shares the same handle, so the worker cannot tell a retarget from a
continued chain by the base alone (§9.2). Not reviewed yet.

v27 (2026-10-29, R28, clarification 25). The owner asked whether v26's
`Queue` reimplemented an existing parallel queue and for a library: it
did, and the library is `std::sync::mpsc` (crossbeam's implementation since
Rust 1.67; crossbeam-channel, flume and the SPSC ring buffers add nothing
this path needs). Applied: the writer holds a `Sender<Request>` with three
kinds, `Submit`, `Reset`, `Finish(reply)`; the thread owns the chain (base,
latest view, step count, used flag, the batch) as locals; each turn blocks
for one request, drains the rest, handles them in order, builds a step when
one is due and answers a noted finish. Gone with nothing to replace them:
the `Queue` struct, its mutex and condvar, the generation, busy, finishing,
finished and alive flags, `Taken`, the exit guard, the drop-outside-the-lock
care and `Reader::worker_idle` (§9.3 test 6 observes `steps_built`). Not
reviewed yet; the loop on R26–R28 runs next.
