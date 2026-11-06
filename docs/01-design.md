# auth_wal design

Status: DESIGN v36 (2026-11-03). Clean-shutdown persistence and recovery
save the latest committed snapshot and restore it against matching storage
(R41, §12.6). Watermark semantics corrected by R39 (§6.2):
the high watermark is local and starts precomputation only; actual eviction
uses capacity and the low watermark. Historical benchmark sections describing
an incremental high-watermark eviction rule predate this correction.
The proposal-path refactor of 2026-11-03 makes worker finalization terminal:
final refinements merge with pending hints before at most one proposal, and
`rounds` includes that final proposal (§9). Earlier review and benchmark
entries retain their historical chain-budget and final-proposal terminology.
The fixed-window prediction update keeps the initial budget at
`capacity - low_watermark + margin`: projected overflow no longer enlarges
it. FIFO ignores subsequent hints; LRU replaces withdrawn candidates using
a forward cursor (§6.1–§6.2). Earlier measurements predate this update
and the benchmark's revised Firewood commit-permit default (§11). The latest
entries in §14 record the focused comparison, serialization experiments,
and upstream Firewood migration.
The Firewood adapter targets upstream v0.9.0 (`a1282b68`, 2026-11-03): its
public dynamic API, runtime hash-mode selection, and `max_persistence_gap`
configuration. Adapter generations preserve logical revision identity when
Firewood absorbs a same-root commit (§10).

Previously DESIGN v34 (2026-11-02). Derived from `00-project-description.md`
including its clarifications (R13–R37); v1 to v6 incorporate five review
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
and no lock of ours is on the path; v28 (R29–R32) records milestone 2,
speculation implemented and measured against hinted blocks: hints predict
what `writes` would do on the projected size (the block's new keys counted
across its hints), the v0 policies gain an eviction trigger (the low
watermark as trigger evicts as many as arrive), predictions outlive the
block (kept marks are re-sent on the new base), and the fuzz property that
hints never change the eviction; v29 (R33) adopts the owner's terms,
`Propose` the critical path and `Commit` off it, and adds the per-phase
latency breakdown of both calls (§9.5, §11) with what it found (§12.16,
§14); v30 (R34–R36) hands `commitment` the entries and adds the whole-set
`rehash` scheme (§7.1, §7.2, measured in §14), records that a block
evicting nothing keeps the precomputation and the parent's storage root
(§9.1, §9.2, §12.17: decided, to implement), and writes down the measured
levers for the set's own work as proposals (§12.17); v31 (R37) applies
them one step at a time with a measurement each (§14 "Clarification
33"): the flat snapshot (§4: sorted arrays and a value arena, merge passes
instead of a copy and per-op map work; §6.1: the policies' order and index
as arrays), the policy digest from one buffer, R35 implemented (§9.1,
§9.2) with the benchmark's `--hint-noise`, and the rehash kept
single-threaded after the parallel variant measured slower; v32 (R38) is
the review round that followed (§14 "Review of 2026-11-02"): a consensus
hazard in R35's chain handover fixed (the child that keeps the chain
becomes its target at `Propose`, §9.1, §9.2), the chain capped at `rounds`
steps across kept blocks so the engine window holds (§9.2), the policy's
parameters validated at `New` (§6.1), the victims check of the merge pass
made complete (§4), and the experiments' leftovers removed (the criterion
bench and the lattice API only it used, the `ffi` stub and `staticlib`,
the view-identity handover, the worker's unused type parameters, the
benchmark's mirror structs and trait; §13); v33 (R38, round 2) fixes
two more findings, a `finish` on a full chain answering a view that lacked
the queued ops (§9.2) and the firewood adapter's first link not pinning
its base revision (§10), pins the consensus encodings with known answers,
and folds the remaining duplication (§14); v34 (R38, round 3) makes both
engines compare a chain's base with the committed state by revision
identity, never by root (a same-root commit then a chain commit would
make firewood free a node twice, §10), adds a fault-injected differential
and a few more pins, and trims what the third round found (§14).
Milestones 0–2 are implemented and reviewed (§13).

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

- **The live set is in memory** (R10, extended by R41). The set contents *and* the policy state
  are consensus state: two validators with different set contents produce
  different roots. Every validator must supply the same initial set. An empty
  initial set starts at height 0; a seeded set starts at height 1, equivalent
  to one initial batch. `Close` can checkpoint the latest committed state to
  `Local::snapshot_path`; `Recover` restores it against the same storage
  (§12.6). Historical revisions and crash recovery remain unsupported.
  `New` rejects an engine that already has committed
  state (`EngineNotFresh`), because the adapter keys `Initialized()` on
  `Root() != EmptyRoot` and would otherwise re-run genesis over it. Reopening
  after a successful close uses `Recover`; crash recovery and state sync need
  a separate durability protocol (§12.6).
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
 │   │ Snapshot (arrays)   │    │ EvictionPolicy       │    │ VectorCommitment          │    │
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
| `New(dir, NodeHashAlgorithm, opts...)` | Opens the storage engine (all firewood/qmdb options pass through to it; for `MemoryStorage` the directory and hashing algorithm are accepted and ignored) and a write set. The Rust constructor is `AuthWal::new(config, storage, set)`; it takes the complete `Snapshot`, whose entries, policy, scheme and capacity stay together, and validates that the set capacity matches configuration. Empty or seeded initial sets are supported (§4). New options: `WithSetCapacity(n)` (consensus) and `WithLowWatermark(n)` (handed to the v0 policies, whose parameter it is; a policy without one ignores it, §6.2), `WithEvictionPolicy(name)` (default `lru-write`, §6.1), `WithCommitmentScheme(name)` (default `lthash`), `WithStorageEngine(name)` (default `firewood`), and the local `WithHighWatermark(n)` (precomputation threshold only, §6.2), `WithSpeculation(bool)`, `WithSpeculationRounds(n)`, `WithSpeculationMinStep(n)`, `WithSpeculationMargin(n)` (§9.2). `WithRevisions(K)` sizes auth_wal's K-ring; the engine is opened with a window of `K × rounds` when speculation is on and the engine commits chain links separately (firewood), else `K` (§8, §10). The Rust core is statically generic (§13); the names select one of a fixed list of monomorphizations behind an enum, and an unknown combination is rejected at `New`, as are (`InvalidConfig`) `capacity = 0`, `revisions < 2` (§8), speculation on an engine whose root is not a state function (§9.3), `rounds = 0` or `min_step = 0` with speculation on, and an engine window smaller than the one above; `EngineNotFresh` if the engine already has committed state (§1); a policy whose parameters could leave the set over capacity is refused (`InvalidConfig` from `EvictionPolicy::validate`, §6.1: for the v0 policies `low_watermark > capacity` or a high watermark outside `[low_watermark, capacity]`); the `apply` assert of §6.2 stays as the backstop for other policies. Default `revisions` is 128. |
| `Recover` (Rust) | `AuthWal::recover(config, storage, vc, policy)` restores `Local::snapshot_path` with matching, reopened storage. Validates the file and reconstructed root, installs the latest committed node, and starts fresh local speculation. See §12.6. |
| `Update(batch)` / `UpdateWithMetadata` | `Propose` + `Commit` in one call. |
| `Propose(batch)` / `ProposeWithMetadata` | Copies the parent's set and applies the batch to the copy (§4), the policy evicts if over capacity (§6.1), creates the storage proposal for the evicted writes, returns a `Proposal` whose `Root()` is known immediately (§8). `Root()` is a single 32-byte commitment combining the set commitment, the policy digest and the storage root (§7.3); none of them is exposed separately. Metadata is root-affecting (it enters the preimage; `None` and `Some(empty)` are distinct, as in `qmdb-ffi`) and is also carried by the node's own storage proposal when one exists (on the speculative path, the worker's final proposal; early steps never carry metadata, §9.1). |
| `Proposal.Get` | Its snapshot, then its pinned storage view (§5); one lookup, no lock on the set side (the storage read takes the adapter's handle read guard, §10 (e)). |
| `Proposal.Propose` / `ProposeWithMetadata` | Child of a proposal (the adapter chains during block building); in Rust `AuthWal::propose(&mut self, Some(&parent), batch, metadata)`, a writer operation (threading contract below). A handle whose node has since committed behaves as the committed state while that node is the head (equal root, §8); any other node is proposed on as given, as `qmdb-ffi` and firewood accept, and a child of a node that can no longer be committed is itself never committable (`Commit` reports `ParentUncommitted` or `ProposalOrphaned`, §8). `ProposalOrphaned` can still come from the engine: a storage proposal on a committed handle whose root the engine has moved past (§10 item 4). |
| `Proposal.Commit` | In Rust `AuthWal::commit(&mut self, handle)`: it takes the writer exclusively and consumes the handle, on success and on failure alike, so neither a second commit of the same proposal nor two overlapping commits can be written (R19); after a storage failure the adapter re-proposes the block, and a retry of the same Go handle reports `ProposalConsumed` (§8). Requires the parent to be the head node by identity (§8; `ParentUncommitted` otherwise, as firewood's `NotLatest`, an equal-root twin of the head included; the adapter only ever commits a child of the committed tree root, and qmdb-ffi's cascade commit is not offered, R20); rejects a proposal whose parent was dropped uncommitted (`ProposalOrphaned`, §8). A storage failure leaves the head where it was, returns the error, and poisons the instance if the node's proposal chained on speculative steps (§9.2); the handle is consumed on every outcome, a pre-check failure included (§8). Retargets the precomputation to the new head, unless it already targets it or a live child of it (§9.2), resending retained predictions without waiting for the worker (R17); its own engine commit may wait for a step still being hashed on a handle it commits (§9.2). |
| `Proposal.Drop` | Releases the handle's reference; the node, its snapshot and its storage proposal are freed when the last reference goes (the K-ring may hold one, §8; the precomputation refers to its target weakly, and the worker's chain keeps the steps, and through them the target's pinned view, alive until the next retarget or `Close`, §9.2). |
| `Prefetch(keys)` on `Database`, `Proposal`, `Revision` | **New (R14).** Hint that execution reading through this view is writing `keys`. The keys go to the policy's `hint_writes` when the node is the precomputation's target, retarget it first when the node is a live child of the target (or, with no live target, the head or a live child of it), and otherwise do nothing (§9.2; no engine warming, R26: the steps are the warming, §9.4). Returns after the policy call: the engine work runs on the instance's worker thread (R17, §9.2). A writer operation in Rust (`AuthWal::prefetch(&mut self, view, keys)`, R19) and a writer call on the database handle in the C ABI, which takes the writer mutex (threading contract below): the hint writes the target's policy. Advisory: a wrong hint costs work, never correctness. |
| `Get` | The head's snapshot, then its pinned storage view (§5). The head is resolved under the registry lock, one hold of about 15 ns (§10); the set lookup takes no lock, the storage read the adapter's handle read guard (§10 (e)). |
| `Root` | Combined root of the committed state (§7.3). `EmptyRoot` is the Ethereum empty-root constant, as in `qmdb-ffi`, reported for an empty height-zero initial set; a seeded set has an authenticated root immediately (§4), so the empty-start adapter's genesis idiom (`Initialized()`, `Revision(EmptyRootHash)`) keeps working. |
| `Revision(root)` / `LatestRevision` | Resolves the last K committed revisions, then live proposals, under one registry hold; in Rust the result is the same `Handle` type a `Propose` returns (§8). `ErrRevisionNotFound` for an unknown root; `ErrRevisionStale` from `Revision.Get` only when its pinned storage view has left the engine's window: every node owns its snapshot, so set reads never go stale, and an orphan reads its own copy exactly, however many commits later (§5, §8). |
| `Prune`, `GatherMetrics`, `Gatherer`, `StartMetrics*` | Pass-through / composition. |
| `Close(ctx)` | Mirrors `qmdb-ffi`: the Go layer waits for its outstanding `Proposal`/`Revision` handles until `ctx` expires (its own keep-alive count, as in `qmdb-ffi`'s `keepalive.go`), then returns `ErrActiveKeepAliveHandles`; otherwise it calls the Rust `AuthWal::close(&mut self)`, a writer operation, which sets the closing flag (`Propose`, `Commit` and `Update` report `Closed` from then on; hints are no-ops), drops the worker's channel and joins it (the step being built finishes, the requests it had already drained are handled, which may build one more step, and nothing is answered, §9.2; the thread's chain, the only speculative engine proposals, drops with it; the writer holds none), and closes the engine through the writer's exclusive reference, which succeeds because committed proposals and revisions do not pin it and reports `ActiveHandles` if a Rust caller still holds a handle to an uncommitted proposal with its own storage proposal (§10 item 6). Reads through handles and `Reader`s keep working afterwards, retained revisions included (a snapshot is memory, a committed engine view stays readable; the Rust side keeps no closed flag for reads, R20; the registry is released with the instance, R22), and `Root` keeps answering with the last committed root. With `Local::snapshot_path`, saves the committed head through the ordered checkpoint protocol in §12.6. Successful close is idempotent; a poisoned instance can close storage but cannot publish a checkpoint. |
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
requires a fresh engine, while `Recover` requires a matching checkpoint;
`Prefetch` is an addition; on firewood the
engine's read-cache strategy is forced to cache all reads (§9.4), overriding
`WithReadCacheStrategy`. Speculation and its rounds are local settings.

## 4. Data model (R2, R7, R16, R18, R20)

The public Rust `set::Snapshot` is the input to `AuthWal::new(config, storage,
set)`. `Snapshot::empty(vc, policy, capacity)` constructs an empty initial set;
`Snapshot::from_entries(vc, policy, capacity, entries)` builds a populated set
from `(K, Entry<V>)` pairs through normal policy writes and commitment updates.
These builders require a fresh policy with no tracked keys. They validate
capacity and policy parameters, deduplicate entries with last-value-wins, and
reject initial data that exceeds capacity or would be evicted. Thus no seed
entries disappear without a corresponding storage proposal.

A nonempty initial set has height 1 and the same authenticated root as one
initial batch with no metadata on fresh storage. Empty initialization keeps
height 0 and `EMPTY_ROOT`. The initial node has no parent; it enters the
revision registry directly. Initialization does not count as runtime writes
or eviction statistics. Root parameters come from the supplied set, and
prediction marks are cleared because no engine precomputation accompanies it.
Storage freshness remains required for `New`. `Recover(config, storage, vc,
policy)` reconstructs a saved set with its original height and policy state
instead of treating its entries as a new batch (§12.6).

```rust
pub trait Key:   Clone + Eq + Ord + AsRef<[u8]> + Borrow<[u8]> + Send + Sync + 'static {}
pub trait Value: Clone + Eq + AsRef<[u8]> + Send + Sync + 'static { fn from_bytes(&[u8]) -> Self; }

enum Entry<V> { Value(V), Tombstone }               // Tombstone = pending delete; what leaves the set and what a read returns
enum EntryRef<'a> { Value(&'a [u8]), Tombstone }    // an entry by reference to its bytes: what the commitment schemes see (§7.1)

/// The whole write set at one node: entries, policy state and commitment at
/// `height`. Immutable once built. Every proposal is a fresh copy of its
/// parent's snapshot with the batch applied (R7 taken literally, R20); the
/// node that was committed owns the committed set; nothing is ever mutated
/// after it is shared. The flat snapshot (clarification 33, §12.17 (a)):
/// the entries are one array of fixed-size records in key order whose value
/// bytes live in an arena of chunks shared between snapshots by `Arc`, and a
/// child is built by one merge pass over its parent's array (§4 "Batch
/// semantics"), so there is no per-entry allocation and no tree to walk.
struct Snapshot<K, V, C, P> {
    entries:    Vec<Record<K>>,                     // key order: canonical iteration, binary-search reads; Record = (key, Slot), Slot = a (chunk, offset, len) into the arena or Tombstone
    arena:      Arena,                              // Vec<Arc<[u8]>> chunks, one per block, plus live and total byte counts; compacted into one chunk when the dead bytes pass the live ones or the list passes 64 chunks
    policy:     Mutex<P>,                           // policy state as of this node (§6.1), copied with the entries; hints mark predictions in it (§9.2, R26), the one mutable thing in a snapshot
    policy_digest: Digest,                          // policy.state_digest() as of this node, a root term (§7.3)
    vc:         C,                                  // scheme instance (parameters, §7.1; a KZG setup belongs behind an Arc, §12.2)
    commitment: C::State,                           // incremental commitment of `entries` (nothing for `rehash`)
    commitment_digest: Digest,                      // vc.commitment(..) as of this node, a root term (§7.3), computed once
    height:     u64,
    capacity:   usize,                              // in force at this node (§6.2; per-node state once it adapts, R4)
}
```

**Why a full copy per proposal (R16, R18, R20).** The structure was decided
three times, each time by the micro-benchmark (the `cow_base` group of a
criterion suite since removed, §14 "Review of 2026-11-02"; its figures are
recorded here and in §12.13). At 500k entries a persistent
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
installs a pointer. The policy's two per-key maps were the larger half of
the copy until clarification 33 made every structure a sorted array
(below): the copy is now a chunk-list clone and the merge pass that builds
the child, 0.15–0.3 ms for the policy's arrays and about 0.25–0.5 ms for
the entries' pass on both workloads (§14 "Clarification 33").

**Measured at 100k (2026-10-29, clarification 27; §12.13, §14).** The copy
matters: every term above is O(n), and at capacity 100k a proposal pays
about 20 ms (entry map 9.5 ms, policy maps 2 ms, the §7.3 policy digest
7.4 ms) and a commit about 8 ms to drop the retired snapshot, before the
first op; 128 retained revisions hold 3.4 GB. At the default 10k the copy
pays for itself once the trie under the set is large (clarification 28:
auth_wal ahead of bare firewood from 100k rows on, by a third at 1M rows
on `zipf`; §12.13), so R20 stands at 10k. Whether it should hold for a
larger set is open: §12.13 has the measured alternatives, §12.15 the shape
a mutable base would take.

- **Ordered snapshot.** One array of records in key order gives canonical
  iteration on every node (no hasher to seed, no hash-flooding surface); a
  read is a binary search (about 0.15 µs hot, 0.46 cold at 10k, against a
  B-tree's 0.12 / 0.2; an Eytzinger layout or a sampled first level would
  close that if reads ever matter, §12.17 (a)). The B-tree it replaced
  (clarification 33) cost one cache miss per node and per value to clone in
  a fragmented heap, 1.7–2.5 ms per copy in situ (§12.17).
- **Values in an arena.** A record points into chunks of bytes shared
  between snapshots by `Arc`: a block's puts go into one new chunk, the
  parent's chunks are shared, a read rebuilds the value with
  `Value::from_bytes`, and the arena is compacted into one chunk of the
  live values (in record order) when its dead bytes pass the live ones or
  it holds more than 64 chunks (a set growing by small blocks would
  otherwise hand every child a long chunk list to clone). The layout is
  local: digests see bytes, never offsets. The merge pass holds the policy
  to its contract: a victim the set lacks, wherever it sorts, or one named
  twice, is `unreachable!`, and a set left over capacity is an `assert!`
  (§6.2: halt rather than diverge).
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
  proposals since genesis; a populated initial set is treated as the first
  batch and starts at height 1 (§1). Recovery must restore historical state
  separately (§12.6). The chain's genesis block is itself committed as a
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
before anything is applied (§3, R22). As one merge pass (clarification
33): the batch is sorted by key, stable, the last op of a key winning (a
later put of a repeated key is an in-place update, the key being present
by then), the puts' bytes go into the block's chunk, the policy answers
`writes(keys, height, capacity)` with the batch's keys in batch order
(§6.1), and one pass merges the parent's records with the ops into the
child's array, skipping the victims (sorted by key) and collecting them
with their final entries, applying `(old, new)` to the commitment for an op
on a present key, `(None, new)` for an insertion, `(old, None)` for a
victim and nothing for a key inserted and evicted in the same block; the
result equals the sequential application (intermediate states cancel in
every scheme). The copy's policy is a clone of the parent's taken under
its lock, marks included (§9.2), and its `state_digest` is taken for the
root (§7.3). This is plain sequential batch
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
pub struct Prediction<K> {
    // Send/resend current entries: worker instructions when keeping the chain,
    // or missing/stale actual victims when finalizing an evicting block.
    pub evict: Vec<K>,
    pub retract: Vec<K>,
}
/// What `writes` answers (§9.1): `all_evicted_keys` is the consensus output, `refinement` is local.
pub struct Eviction<K> {
    // Every actual eviction, including entries already sent by precomputation.
    pub all_evicted_keys: Vec<K>,
    pub refinement: Prediction<K>,
}

pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {   // Clone: copied with the snapshot (§4)
    /// Consensus-critical. The keys of one batch (batch order; a `Put` or `Delete` each, a write to a present key
    /// included) landed at `height`; evict by the policy's own rule from the set size (it tracks exactly the set's
    /// keys) and `capacity`, remove the victims from the policy's state and return them sorted by key (the storage
    /// batch's order, §6.3; ties in the order break by key bytes). `all_evicted_keys` is a function of the consensus state,
    /// `keys`, `height` and `capacity` alone. Also
    /// reconciles the marks into `evict`/`retract`: with victims, finish their writes and restore kept predictions;
    /// without victims, resend rewritten FIFO predictions or restore canceled LRU predictions.
    /// The policy removes victims' marks and cancels retained LRU rewrites before returning.
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K>;
    /// Local (§9.2). At the high watermark, start a fixed capacity - low_watermark
    /// prediction window plus margin. Projected insertions trigger it, never grow it.
    /// Return newly marked candidates and retractions; never change consensus state.
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K>;
    /// Local (§9.2, R31). The marked keys, in key order: what the chain carries or is about to; re-sent at a retarget.
    fn predicted(&self) -> Vec<K>;
    /// Local (§9.2). Forget every mark: the node took the plain path, nothing marked is in its storage.
    fn reset_hints(&mut self);
    /// The policy's parameters against the capacity (§6.2), checked once by `New`: `InvalidConfig` for a rule that
    /// could leave the set over capacity (v0: a low watermark above capacity, a high watermark outside [low, capacity]).
    fn validate(&self, capacity: usize) -> Result<()> { Ok(()) }
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
covers) is disjoint from the prediction state (the marks, of which `writes`
drops the victims' and canceled LRU rewrites' marks, retaining the rest,
R31, and `reset_hints` forgets), so a
child's policy carries only the prediction that outlives the block and equal
digests imply equal future `writes`; and `writes` and
`state_digest` read only that state, `keys`, `height`, `capacity` and the
policy's parameters, every one of which enters `params_digest` (zeros if
none), so the capacity term and `policy_params` cover everything the next
eviction depends on (§7.3). Since the policy also decides how many to
evict, its contract is that the set is within capacity after `writes`;
`Snapshot::apply` asserts it (a violated consensus invariant halts rather
than diverges, §6.2).

Why the policy owns the prediction (R25, clarifications 17–19): it holds
both ends, the likely victims and the actual ones. At `Propose` it
reconciles sorted written keys and actual victims against its own marks;
the writer keeps no sent-key set. The worker merges operations by key. The marks mean "a step carries this key's entry", so
a retraction a hint returns is executed as an op like an eviction (§9.2) and
the marks keep mirroring storage (§9.1). The marks live in the policy where
it lives, the snapshot (§4, R26): a hint mutates the target's policy in
place under its lock, `apply` clones it marks and all, a retarget re-sends
what the new target's copy marks (R31) and a fallback to the plain path
resets the node's copy; nothing is copied aside.

v0 policies (consensus state: an `Arc<Vec<(u64, K)>>` order and an
`Arc<Vec<(K, u64)>>` reverse index, both sorted. A policy clone shares these
immutable input arrays; `writes` builds the child's arrays once by merge
passes, replacing both references without mutating the parent (the
clarification-33 merge passes with their redundant initial copy removed: the
written keys' old places skipped, the block's run appended, the victims cut
from the front, the index merged with the new heights and without the
victims; `split_order` and `merge_index` in `policy/mod.rs`); prediction
state: each policy owns its marked-key set and hint history. FIFO tracks
distinct hinted insertions only until its first trigger, then records that
prediction has started and does no further hint work. LRU-write records all
hinted keys to exclude them from candidates, plus an insertion count needed
only before activation, a fixed prediction budget, and a forward queue
cursor. These are local fields, absent from the consensus digest.

Both policies prepare sorted, distinct written keys once per `writes` call
and reuse them in reconciliation. For a kept block, FIFO visits written keys
to resend those still marked. LRU visits written keys to remove marked ones
and return their retractions; it does not scan the whole marked set twice.
For an evicting block, missing or rewritten actual victims are returned in
`evict`, and retained predictions in `retract`. LRU captures these final
restorations before canceling rewritten retained marks. No writer-side
unmarking or shared `Marks` helper is needed.

| Name | Victim order | `hint_writes` behaviour |
|---|---|---|
| `fifo` | Oldest insertion height first; rewrites keep their position. | At the first trigger, marks the fixed oldest prefix. Later hints return no operations. |
| `lru-write` | Oldest last-write height first; rewrites move to the back. | At the trigger, fills a fixed window of eligible keys. Hints withdraw marked keys and fill vacancies from the next eligible queue positions. |
| `lfu-write` (later) | Fewest writes since insertion, then oldest. | Deferred. |

The budget is `capacity - low_watermark + margin`, selected at the first
trigger and capped by available candidates. It does not grow when hints
project more insertions. FIFO never excludes or retracts hinted keys. With
the configured margin zero, its predicted prefix is a subset of every
later overflow eviction, so finalization needs no FIFO retractions either;
a nonzero margin can predict retained entries, which still must be restored.

LRU processes the **entire hint batch** before choosing replacements, so it
cannot pick a key hinted later in the same batch. Successful mark removals
deduplicate retractions; the returned list is sorted. A cursor visits each
queue position at most once per block, skipping hinted and already marked
keys until the marked count reaches the fixed budget or the queue ends.
`writes` rebuilds the order, clears hint exclusions, and resets the cursor.
The budget and surviving marks persist across non-evicting blocks; only
vacancies are filled. False hints may leave younger surviving predictions,
so final reconciliation still restores nonvictims under LRU, even with
margin zero. Actual eviction clears activation for the next window;
`reset_hints` clears all prediction state.

Parameters are the consensus low watermark and local high watermark; the
benchmark defaults are 70% and 90% of capacity (§6.2). `state_digest` remains
BLAKE3 `derive_key("auth_wal/policy-state")` over the length-prefixed policy
name and eviction order as `(height u64 LE, length-prefixed key)` items.
The prediction update changes neither this encoding nor actual eviction.

Milestone-1 tests, for both policies: FIFO and LRU-write semantics on small
sequences (a rewrite keeps or moves the key; a delete of an absent key
inserts a tombstone at this height and a put over it is a rewrite; eviction
order with height ties; a batch with a `PrefixDelete` is rejected untouched);
the equal-root property of §7.3 (a child proposed on two equal-root nodes,
the committed node and a live twin, or two live siblings from one batch,
gets one root; under `lru-write` a same-value rewrite and an empty batch
give different roots, under `fifo` the same); the prediction boundary (the
same batches with any hint sequence, including none, give the same
`all_evicted_keys`, the same `state_digest` and the same roots; §9.3 test 1 is the
same statement end to end); and the same block sequence applied as chained
proposals and as proposals after each commit reaches the same roots (a
pipeline smoke test: the two paths run the same code on the same state).
Policy purity (a cloned state yields the same `writes` sequence) is §9.3
test 5, whose fuzz form is R32 (clarification 30): from one state, one to
six overlapping hints per block (keys hinted but not written, writes never
hinted) then `writes` of the union, against `writes` of the union on a copy
that saw no hint: equal victims, equal digests, and the marks plus the last
refinement leave storage holding exactly the victims, six blocks in a row
so the kept marks of R31 carry over, both policies, default and early prediction thresholds
(`hints_compose_to_the_plain_eviction` in `src/tests/`).

### 6.2 When and how much

Eviction runs inside `Propose`, after applying the batch to the copied set.
For the v0 policies, actual eviction occurs only when `len > capacity`, and
removes `len - low_watermark` entries. Reaching the high watermark does not
remove entries from the set or change the authoritative storage root.

The high watermark (`with_trigger`, `--high-watermark`) is a **local
precomputation threshold**. Before activation, projected occupancy is
resident entries plus distinct hinted keys absent from the set. Once it
reaches the high watermark, choose one fixed prediction budget:

```text
expected = resident + distinct_hinted_new_keys
if not_started and expected >= high_watermark:
    prediction_budget = capacity - low_watermark + speculation_margin
```

Capacity 10,000, low 7,000 and high 9,000 therefore start with 3,000 predicted
keys at margin zero. Later insertion hints do not enlarge this window.
FIFO's window stays unchanged; LRU replaces marked keys withdrawn by hints.
Only resident eligible entries can be sent ahead, so an exhausted queue may
produce fewer than the budget. Actual overflow to 10,001 evicts 3,001 keys;
the final refinement supplies the additional victim. A larger overflow is
handled the same way, by actual eviction and its final refinement.

FIFO refreshes marked entries rewritten by a non-evicting block. LRU
withdraws rewritten predictions and fills vacancies on later hints using its
rebuilt order. Activation and the fixed budget survive kept blocks, even if
false insertion hints triggered the window before actual occupancy reached
high. Marks and immutable engine proposals remain provisional under R35
(§9.1); final reconciliation corrects false hints and unhinted writes so
storage equals the plain path. Actual eviction ends the window and allows
the next one to trigger; `reset_hints` forgets it entirely.

Capacity and low watermark are consensus parameters. High watermark, margin,
and marks are local and cannot change victims or roots. The v0 `params_digest`
hashes only `"auth_wal/policy-params" || low_watermark (u64 LE)`. Removing the
old trigger field changes the root encoding relative to the earlier version;
old benchmark roots are not a compatibility reference. Validation requires
`0 <= low_watermark <= high_watermark <= capacity`. Benchmark defaults are
capacity 10,000, low watermark 70% (7,000), and high watermark 90% (9,000),
with percentages rounded down. Explicit CLI values override these defaults.
The library requires an explicit low watermark; callers choose the local high
watermark with `with_trigger` (an unconfigured policy still uses capacity).
`Snapshot::apply` still asserts the post-eviction capacity bound.

This supersedes R30's earlier interpretation that high watermark controlled
actual eviction. Setting high equal to low now starts prediction earlier;
it does not select an incremental eviction rule.

### 6.3 Storage proposal for evicted writes

A node that evicts entries receives a storage proposal view. On the plain
path, the evicted entries are proposed in **canonical key order** on the
parent's pinned storage view (`Put` for values, `Delete` for tombstones).
On the speculative path, the worker merges final corrections with pending
hints and returns the exact final view, reusing its latest proposal when no
work or metadata remains (§9.1). The view is committed or dropped with the
child (§8). A node that evicts nothing inherits its parent's view and
`storage_root`; background prediction may continue without an engine commit.

### 6.4 Capacity control (deferred)

v0 has fixed parameters and no controller hook (R20 removed the unused
one). An adaptive capacity (R4, "in the future") is an open question (§12.8):
its effective capacity would have to be a function of the parent node and
enter the root as the node's capacity term already does (§7.3, R23).

## 7. Authentication (R6)

### 7.1 Trait

```rust
pub trait VectorCommitment<K: Key>: Clone + Send + Sync + 'static {
    type State: Clone + Send + Sync;            // copied with the snapshot (§4): cheap to clone
    fn scheme_id(&self) -> &'static str;
    /// Digest of the instance parameters (KZG setup hash and slot count, Merkle
    /// depth). Enters the root preimage (§7.3). Zeros by default: every v0 scheme.
    fn params_digest(&self) -> Digest { [0u8; 32] }
    /// The scheme instance carries its parameters (KZG setup, Merkle depth), hence `&self`.
    fn empty(&self) -> Self::State;
    /// Incremental update for one key; `None` = absent.
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>);   // item = the entry under its key (§7.2); entries by reference to their bytes (§4)
    /// 32 bytes, enters the root: from `state`, or from `entries`, the whole set in key order (a whole-set scheme, R34).
    fn commitment<'a>(&self, state: &Self::State, entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>) -> Digest;
}
```

The trait is incremental by design: a scheme updates its commitment from
`(old, new)` without touching the rest of the set, so its per-block cost is
proportional to the batch rather than the set. `commitment` is also handed
the whole set in key order (R34, clarification 32): a whole-set scheme
keeps no state, ignores `apply` and hashes the entries there, once per
proposal, at a cost proportional to the set; `Snapshot::apply` calls
`commitment` once when it builds the child and keeps the digest, as it
keeps the policy digest (§4). Which kind is cheaper is set size against
block size (§7.2). Proof methods (`prove`, `verify`) are added with
milestone 5 (§7.4); v0 code carries none of them (R20).

### 7.2 Schemes

| Scheme | State | Update cost | Membership proof | Notes |
|---|---|---|---|---|
| `lthash` (v0 default) | 2 KiB lattice-hash vector (1024 × u16 lanes) | one XOF + lane add/sub | none (set-level only) | Additively homomorphic: `C ← C − H(k,old) + H(k,new)`. Cheap, order-independent, authenticates the set as a whole. |
| `kzg` (milestone 5) | one G1 element (48 B, hashed to 32 for the root) + `key → slot` map (a slot's value commits to the §7.2 entry item) | one scalar mult | yes, constant size | Slot count must exceed `capacity` by a safety factor (a `capacity`-slot table is guaranteed to collide near full occupancy); collision policy is open (§12.2). Trusted setup of slot-count size. The slot map is consensus state: it lives on the `Snapshot`, copied with the entries (one more per-key map in the per-proposal copy, §12.14), to be settled together with §12.2 in milestone 5. No code in v0. |
| `merkle` (reference) | whole sparse Merkle tree (persistent) over `SHA-256(key)`, depth 256, leaf = the entry item below | 256 hashes per update | yes | No slots, so no collisions; parameter-free. Slow but proof-friendly baseline for tests and benchmarks; its persistent tree is cloned with the snapshot in O(1). |
| `rehash` (R34, clarification 32) | none | nothing per op; one keyed BLAKE3 (`derive_key`, domain `auth_wal/rehash`) per proposal over `u64 LE` entry count ‖ every entry item in key order | none (set-level only) | Whole-set scheme. The entries are serialised into one buffer and hashed with one `update`: 0.36–0.45 ms at 10k entries of 32-byte keys and values (0.77 MB; the same hash as 30k small `update` calls costs 1.6 ms, `scratchpad` micro-benchmark), against 1.1 µs per lattice item. In situ it costs 0.66–0.75 ms per block at 10k while a lattice item costs 2.7–2.8 µs (§14 "Clarification 32"), so it wins while the block's items (inserts + 2 × rewrites + victims) exceed about a fortieth of the set: 250 items at 10k, far under both benchmark workloads (about 1570 on `zipf`, 4800 on `tenkrandom`), where it cuts propose from 10.5 to 6.9 ms and from 21.5 to 9.1 ms; at 100k (7.7 MB, several ms) the lattice wins for `zipf`-sized blocks. `lthash` stays the default until the owner decides. |

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
`u16 LE`. Measured (the since-removed `lattice` criterion bench, §14 "Review of
2026-11-02"; Haswell with AVX2 only): BLAKE3's own XOF output is vectorised only with AVX-512 and cost
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
the trait or the root if it ever matters, clarification 21).
`engine_id` includes the engine's consensus parameters: Firewood reports
`firewood/ethereum` with `ethhash`, otherwise `firewood/merkledb`. Every engine
reports `EMPTY_ROOT` before its first commit, so an identifier covering only
the engine name would let different hashing algorithms share roots until the
first eviction. These identifiers change Firewood-backed roots from the old
`firewood` identifier starting at the first non-genesis proposal; the root
encoding and MemoryStorage roots are unchanged. `storage_root` is the
pinned storage view's root; a proposal without a storage proposal of its own
reuses its parent's.
Both QMDB and firewood report a proposal's root at propose time, so the
combined root is available before commit. `EMPTY_ROOT` means "nothing
committed yet" on the empty-start path: the height-0 node. A populated
initial set starts at height 1 with the same root as one initial batch and
uses that root for revision lookup immediately. `Revision(EMPTY_ROOT)` resolves
to the empty initial node only when one was supplied,
while that node is in the K-ring (the adapter needs it only at genesis), and
returns `RevisionNotFound` once it is retired. A node keeps its root and
metadata hash so checkpoint recovery can verify the preimage (§12.6).
A proof (§7.4, milestone 5) still takes the metadata bytes from the caller
and recomputes the preimage with `Terms::combine`.

### 7.4 Authenticated reads (milestone 5)

A proof of `key = value` under `root` is the preimage components plus a set
proof (membership, or non-membership) from the scheme and, on non-membership,
the storage engine's proof under `storage_root`. `lthash` cannot produce a
per-key proof, so `ProveLatest` returns "unsupported" under it; `kzg` and
`merkle` can. This is the main reason the scheme is pluggable (§12.1).
Milestone 5 adds `prove`/`verify` to `VectorCommitment`, `prove`/`verify_proof`
to `StorageEngine`, and `prove` to the handle; the node keeps what a
proof needs but the metadata (its snapshot and its root; the writer's terms
and the caller's metadata rebuild the preimage, §7.3).

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
one bit `Commit`'s poison rule needs, §9.2), its root and metadata hash (§7.3,
§12.6), and one
**weak link to its parent**. Statistics are counted at
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
handle, the storage engine, the configuration, the poisoned flag and the
closed flag.

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
  worker's `finish` (or, evicting nothing, the reply-less block end of R35,
  §9.1) after applying the batch, between the two holds with nothing held
  (§9.2). The copy is the one cost, about 2 ms at 10k (§4). Live proposals are held
  weakly by the registry; the caller's handle owns the node.
- **Commit(P)** takes `&mut self` (only another writer call could ever wait
  on it, and the type system forbids one in Rust; through the C ABI a hint
  waits on the writer mutex, §3) and `P`'s handle by value, **whatever the
  outcome**: a failed storage commit consumes the handle too (the node is
  dead: the engine consumed its failed storage proposal; a chain with
  speculative ancestors also poisons the instance, §9.2), the adapter re-proposes
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
  (Firewood sees one commit per proposal in the completed chain, including
  its final view; the adapter only commits direct children of the committed
  state anyway), with no lock held. Then one
  registry hold installs the node: pushed on the ring, it is the head
  (the ring holds the K most recent committed nodes *including* the head, so
  the oldest resolvable root is at most `(K − 1) × rounds` engine
  commits behind, which is what makes an engine window of `K × rounds`
  sufficient); it leaves the live index (the ring answers for its root now);
  the ring entry beyond K is taken out and handed back. The retired entry is
  dropped after the hold (freeing a snapshot or
  engine handles is not a nanosecond hold, §10); the writer then retargets
  the precomputation to the new head, unless it already targets the new head
  or a live child of it (by parent identity, §9.2), with nothing held.
  Retained predictions are re-sent on a new base without waiting. The
  node's snapshot itself becomes the committed set; no batch is replayed. A failed storage commit leaves the head where it was,
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
  the nodes hot against 28 ns for the map lookup it replaces, the
  since-removed `registry_ring` micro-benchmark, up to one cache miss per
  entry cold; a `Revision`
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
  The engine is therefore opened with a window of `K × speculation_rounds` when speculation is on and the engine commits chain
  links separately (`COMMITS_CHAIN_LINKS_SEPARATELY`, §10: firewood and the
  in-memory model), else K. The Rust `new` checks the engine it is handed;
  the Go `New` opens the engine with that size. Note that firewood never
  reaps a revision that is still referenced, so a pinned firewood view does
  not go stale: its window stalls instead, and the K-ring (plus the adapter
  dropping revisions) is what bounds memory.
- **Memory.** Each node owns a snapshot: one record array (the key and a
  slot into the arena; 48 bytes per record with inline 32-byte keys, 32
  with `Arc` keys), the arena's chunks (one per block plus the compacted
  one, shared by `Arc` with every snapshot that holds the same chunk, so a
  child adds its own block's values only) and the policy's two arrays
  (`(height, key)` and `(key, height)`, §6.1; 80 bytes per entry with
  inline keys, 48 with `Arc` keys): about 1.3 MB per snapshot at 10k
  entries with inline 32-byte keys, plus up to twice the live value bytes
  in chunks (0.3 MB of 32-byte values, shared), about 0.8 MB with `Arc`
  keys. The ring keeps K of them, so K = 128 retains 100–200 MB of
  snapshots, plus one per live proposal (a few at most: the blocks being
  verified ahead and competing candidates). `WithRevisions` is the knob,
  and folding the policy's heights into the records (§12.14) the lever. The K-ring, not handles,
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
2. During execution, hints **refine** LRU's fixed window (§6.1): a hinted
   marked key is withdrawn and replaced by the next eligible candidate.
   FIFO needs no hint refinements after activation. Each resulting operation
   joins the worker's pending batch; steps are built sequentially on the
   latest view (R24).
3. At `Propose` the policy reconciles its predictions with actual victims
   into one last refinement (§6.1). The worker merges it with pending hints
   before finalizing the latest view: retained predictions are restored to
   their base values or absence, and missing or stale victims are written.
   No separate caller-side correction proposal is needed. The returned
   storage state is exactly what the plain path would produce. On an engine
   whose root is a function of state (firewood, §10) the storage root is
   therefore identical, and speculation is invisible to consensus: no
   parameter of it enters the preimage, and it may freely depend on timing,
   hints and local heuristics.

The prediction is neither accurate nor complete by design (clarification
16): a step carries a best guess, and the final proposal corrects it in both
directions, retractions for the keys sent and kept, the remaining victims
for the keys never sent. What bounds how aggressive the prediction may be is
the cost of the retractions (`margin`, §9.2). The steps are also all the
warming there is (clarification 22): a hint does no separate engine reads.
Early steps warm their victims' and retracted keys' paths; actual victims
outside the fixed window may still need cold-path work at finalization.

### 9.1 Plain and speculative paths

For a child `N` of parent `P`, the policy's `writes` returns the actual
victims `V` and a refinement `(E, R)`. When `V` is nonempty, `E ⊆ V`
contains victims whose final entries are missing or stale in the prediction;
`R` contains predicted keys retained in the set. When `V` is empty, the
same interface maintains the continuing prediction: FIFO resends rewritten
marked entries through `E`, while LRU-write withdraws rewritten predictions
through `R` and cancels their marks.

| path | resulting storage view |
|---|---|
| **no eviction, off the target** | Inherit `P.storage_view`; clear the child's local prediction marks. |
| **no eviction, on the target** | Inherit `P.storage_view`; submit the refinement and an asynchronous block end, retain the chain, and move its target to `N`. |
| **eviction, plain** | Propose all of `V`, in key order, on `P.storage_view`. |
| **eviction, on the target** | Send the final refinement and metadata in one terminal worker request. The worker merges them with pending hints and returns the reconciled storage view. A failure takes the plain path. |

The worker owns both early proposals and final reconciliation. Its pending
map keeps one operation per key, with the latest operation winning. Final
`Evict` operations carry the actual entries selected from `V`; final
`Retract` operations restore the original chain base's value, or delete the
key if that base lacked it. The base equals `P.storage_view`, including
when the chain spans several blocks that evict nothing. Retractions must
never read the speculative latest view, whose changes they are undoing.

Finalization merges `(E, R)` into the pending map **before** hashing it.
The worker builds at most one proposal for that merged batch, on its latest
view. If there are no pending operations and no metadata, the latest view
already has the exact final state and is returned directly. Otherwise the
new proposal carries the metadata, including `Some(empty)`. There is no
second proposal on the validator thread. Plain proposals retain canonical
victim order; speculative merged batches are also in key order.

Correctness follows from the prediction invariant: before finalization,
every marked key is represented either in the latest view or in the queued
operations, and every withdrawal is queued in order. Final refinement
replaces pending stale values, restores retained predictions, and adds the
missing actual victims. After it is applied, storage differs from the base
exactly by `V`, so it equals the plain path's state and root (§9.3). A failed
read, proposal, or worker prevents reuse of that chain; the writer clears
the child's marks and proposes all actual victims on `P.storage_view`.
No storage commit occurs during either path.

**Finalization consumes the chain.** Its correction is specific to one
child and cannot become a shared prediction for another child of `P`.
After building `N`, the writer starts a fresh chain on `N.storage_view` and
resends `N`'s surviving prediction marks. A sibling proposed on `P` is now
off the target and takes the plain path. This also makes late hints on
`P` harmless. A later commit that chooses a different node retargets to
that node (§9.2).

**Predictions outlive the block (R31).** Retained FIFO predictions and
retained LRU predictions not rewritten by the batch stay marked. After an
evicting block, their speculative storage writes have been restored, so
retargeting to the completed child resends their entries on its new base.
This works before or after the child's commit; immutable and committed
proposal handles are both valid bases (§10). Carrying the old hashed
prediction across an eviction-bearing commit without rebuilding it still
requires Firewood support (§12.10 (f)).

**A block that evicts nothing (R35).** `Propose` does not wait for the
worker and does not finalize the chain. It submits the policy refinement
and an asynchronous block end; the worker flushes pending work only while
an early-proposal slot remains. The child inherits the parent's storage
view and becomes the target immediately, with no reset. A rewritten FIFO
prediction therefore carries the child's new entry, and an LRU withdrawal
restores the base. A sibling on the old parent cannot inherit this chain
merely because its storage view is the same: target identity is the node's
identity. `Commit` of the kept child makes no engine commit. The prediction
can span any number of these blocks, but the proposal budget remains per
chain, with one slot always reserved for finalization.

**Retractions remain explicit.** auth_wal keeps no old-value cache: both
early and final retractions read the chain base and produce `Put(old)` or
`Delete`. This costs a read and another hashing of the path. A Firewood
restore operation could remove that cost (§12.10 (e), `docs/TODO.md`), but
is outside this refactor.

### 9.2 The precomputation

**Ownership.** The policy owns prediction marks in the target snapshot;
its consensus state and digest are unaffected by hints (§6.1). The writer
owns the weak target-node identity and converts policy answers into owned
operations. The worker owns the engine chain: its base and latest views,
proposal count, and the pending map. All writer operations take `&mut self`;
the worker receives ordered requests through `std::sync::mpsc`. No auth_wal
lock is held across a storage call or a worker wait.

**Requests.** `Submit` queues operations without waiting. An asynchronous
block end asks for a pending early step without consuming the chain.
`Reset(base)` discards the old chain and starts one on the new target's
pinned view. `Finish` contains the final refinement, owned metadata, and a
reply channel; it is a terminal barrier, not a request for a preliminary
image. The worker handles all preceding requests, merges the final
operations, completes or reuses the final view, answers once, and consumes
the chain. Requests after the barrier belong to a later reset and cannot
change its answer.

**Early work.** The worker merges submissions into a key-ordered map even
when its early-proposal budget is exhausted. Replaced values are dropped
on the worker, leaving the validator's submit path enqueue-only. This
bounds drained pending state to one operation per key; it does not bound
a producer backlog while hashing. A step is due when at least `min_step`
distinct keys wait, or an asynchronous block end requests a flush. It can
run only while fewer than `rounds − 1` proposals have been built. Evictions
become puts or deletes; retractions restore the base. Early proposals carry
no metadata and become the chain's latest view. Only one storage call runs
on the worker at a time. The library and benchmark default `min_step` to
256 distinct pending keys, following the focused Zipf comparison (§14).
Block-end flushes and terminal finalization can build below this threshold.

**Final work.** An evicting `Propose` applies its set batch while the
worker may still be hashing, constructs the final refinement, and calls
`finish(refinement, metadata)`. The request merges with whatever hints
remain pending, bypasses `min_step`, and can use the reserved last slot.
It therefore avoids a separate tail proposal followed by a correction
proposal. If the merged work is empty and metadata is absent, the worker
returns its latest proposal directly. The returned result includes the
storage view, ancestor-link count, and final read/proposal timings.
`Propose` waits for any in-flight early work plus this finalization; it
never waits for persistence. The plain path remains available if the
worker cannot supply a valid result.

**Failures and shutdown.** A failed read/proposal or a caught panic
invalidates the chain until reset, since its latest view may lack a write
described by a policy mark. Subsequent submissions are dropped and counted;
finalization reports the failure. An orphaned engine base is discarded
work, rather than a consensus failure. If the worker disconnects, the
writer takes the plain path. Reset releases pending values and obsolete
views; request ordering replaces generation numbers and cancellation
flags. `Close` drops the sender and joins the worker before closing the
engine, allowing in-flight work and queued requests to finish and releasing
all worker-owned proposal handles. Speculation-disabled instances create
no worker or channel; a worker-spawn failure makes `New` fail.

**Target.** The target is one node by pointer identity, initially the head.
A hint on it updates its policy and submits the refinement. A hint on a
live child retargets to that child; with no live target, a hint may retarget
to the head or a live child of the head. Other hints are ignored. Retargeting
resets the worker to the selected node's pinned view and resends its marked
entries, because policy marks alone do not install values on a fresh base.
A plain-path node has cleared marks.

When a targeted `Propose` evicts entries, it consumes the chain and retargets
to its completed child with a reset. When it evicts nothing, it moves the
target to the child while keeping the same chain. Thus a completed candidate
never leaves its parent targeted with child-specific engine state. Competing
siblings take the plain path; if one wins, `Commit` retargets to it. Commit
otherwise leaves a target alone when it is the new head or a live child of
that head, preserving pipelined work. The existing same-view test is never
used as a substitute for node identity.

**Commits and bounds.** `rounds` bounds **all** worker proposals in a chain,
including its final proposal, across any number of kept blocks. At most
`rounds − 1` are built early. With `rounds = 1`, all prediction and final
refinement merge into one proposal at `Propose`. Reusing an already exact
latest view needs no additional proposal. The returned node records only
ancestor links (`total proposals − 1`); the last proposal is its own storage
view. Firewood commits those links and that view in order, so one auth_wal
commit advances the engine by at most `rounds` revisions. An engine window
of `K × rounds` therefore covers the K retained auth_wal revisions (§8).

Firewood's persistence queue is a separate limit. `FirewoodConfig` exposes
`commit_permits` through the backend's `max_persistence_gap` option,
with adapter default 1. It must be positive and smaller than the engine
revision window. The benchmark defaults it to `speculation_rounds` for
speculative auth_wal runs and 1 otherwise, with an explicit override (§11).
More permits allow more committed revisions to await persistence; commits
still use Firewood's existing asynchronous path and can encounter
backpressure when the permits are exhausted. No extra auth_wal commit queue
or change to Firewood's behavior is introduced.

`Commit` does not wait on a worker request. Retargeting or pipelined hints
may already be building the next chain on the completed node, so the engine
handle lock can make its commit wait for a proposal on that handle. The
handle remains a valid base after commit (§10); revision-identity checks
reject work based on a committed revision the engine has moved past. A
failed commit of a node with speculative ancestors poisons the instance,
since some ancestors may already have committed. A single-proposal failure
leaves the instance usable, as on the plain path. Reads and `Close` remain
available on a poisoned instance.

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
   hints between the two proposes, either committed: equal roots. The first
   targeted proposal moves the target to its child; the sibling uses the
   plain path and cannot inherit that child's corrections (§9.2).
5. Policy purity: a cloned policy state yields identical `writes` sequences
   on both copies, hints or not (also a milestone-1 test, §6.1).
6. Asynchrony (R17, R24), on `MemoryStorage`, whose handle carries firewood's
   lock shape (§10 (e): a read guard across its latency sleep in `propose`
   and across `get`, the write guard across `commit`), with synthetic engine
   latency (`--storage-latency-us`): `Prefetch` returns in time independent
   of step hashing; `Propose` on the target waits for the step being built
   and final reconciliation; subsequent retargeting may already queue the
   next chain. `Commit` does not wait for a worker reply, but the engine's
   handle lock may wait for a next-chain proposal on that handle (§9.2).
   `Close` joins the worker before closing the engine. Hints are issued while no `Propose`/`Commit` is running, so every
   wait the test measures has one cause; the unit test sets
   `MemoryConfig::op_latency`, the benchmark `--storage-latency-us`;
   progress is observable via the `steps_built` counter. The same test on
   firewood uses real hashing on large steps (10k entries or more) for its
   timing assertions.

Status (milestone 2, 2026-10-29): tests 1–5 are in `src/tests/`
(`speculation_matches_the_plain_path` under both eviction rules, all
schemes and policies: forty random blocks with deletes, overlapping and
wrong hints, pauses for the worker, equal roots and storage roots at every
height, reads and storage exact, steps built and reused, no failure;
`two_children_of_the_target_either_committed`;
`hints_off_target_do_nothing_and_close_drops_the_chain`; the fuzz test of
R32; `speculation_on_firewood`). Test 6, the timing assertions, is pending.

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

Counters are local diagnostics and never enter a root. Per-proposal outcomes
include losing candidates: `plain`, `speculative`, and `steps` (ancestor
links of the returned storage view, excluding that view itself). A successful
worker finalization can have zero ancestor links, so the per-call
`speculative` flag identifies the path independently of `steps`.
`reused_ops = |V ∖ E|` counts victims already covered by the prediction;
`reverted_ops = |R|` counts retained predictions to restore. Some predicted
operations may still be pending when finalization arrives, so `reused_ops`
is not a count of operations already hashed before `Propose`.

The existing derived `storage_proposals = plain + speculative` counts
finalized evicting outcomes, **not engine calls**: finalization may reuse an
existing proposal. Likewise `total_storage_ops = evictions − reused_ops +
reverted_ops` is the logical reconciliation workload, not the merged final
batch size. These expressions saturate because the underlying counters are
updated separately; a fallback counts as plain. Set counters record puts,
in-place updates, and actual evictions, while `Stats` reads the head's height
and occupancy under the registry lock.

Worker counters describe actual work: `steps_built` includes successful
early **and final** proposals. `step_ops` and `worker_time_ns` include
attempted work even when a read or proposal fails, counting submitted
operations and wall time, including engine-lock waits. `tail_steps` and
`tail_time_ns` identify proposal attempts made by finalization;
reusing the latest view builds no tail proposal. `retractions` counts
restoration operations processed by the worker. `chains_kept` counts
targeted blocks that evicted nothing and continued their prediction.
`steps_dropped` counts completed worker proposals discarded before a
successful finalization; shutdown does not count remaining work.
`ops_dropped` counts discarded pending/submitted operations, including work
on an orphaned base. `step_failures` records other storage proposal or
restoration-read failures, `worker_panics` caught panics, and `fallbacks`
finalizations that could not be reused. `hint_keys` counts all keys reaching
the target policy, including duplicates; ignored hints count nothing.

The writer exposes its last successful call through `last_propose_timings`
and `last_commit_timings` (`src/timings.rs`). `Propose` records `copy`, `ops`,
`writes`, `merge`, `digest`, and `commitment` for set work. Worker finalization
returns its final batch preparation and restoration `reads` and final-proposal `storage` durations;
`finish` is the elapsed reply wait **minus those two durations**, so the
breakdown does not count them twice. It includes queueing, unfinished early
work, and reconciliation overhead. On the plain path, `storage` measures
the caller's storage proposal. `rest` covers root composition, node creation,
retargeting, counters, and registration; `total` measures the entire call.
The shape fields are `batch_ops`, `evicted`, `storage_ops` (the actual final
batch, including merged pending hints), `steps`, and `speculative`.
Returning an already exact latest view has zero final reads, storage time,
and storage operations.

`Commit` records `engine` (all required engine commits), `advance` (registry
advance and retiring the old snapshot), and `retarget`. Timing reads cost
tens of nanoseconds against millisecond phases, so instrumentation stays on.
The benchmark's `--breakdown` reports the mean, p99, and share of each phase,
plus worker work and failure counters; the CSV retains the failure fields
because the benchmark has no log sink. The worker totals overlap caller
waits and are reported separately from the per-call phase breakdown.

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
   "this proposal's base is the committed state"; and "is" means the same
   revision by identity, not an equal root: a same-root revision committed
   meanwhile (a no-op batch) is another revision, and a chain built on the
   superseded one is refused (the oldest link fails and is consumed, item
   1; its children stay live). Firewood's own check compares hashes and
   would accept such a chain, whereupon the no-op revision and the chain's
   first link both list the shared root node as deleted and the engine
   frees it twice (later allocations overwrite a live node), so the
   adapter refuses first.
4. `propose` on a **committed** handle proposes on the engine's committed
   state, provided the handle's revision is the engine's current one by
   identity both **before and after** the call, and returns
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
   on a stale view is accepted as a step. Finalization can also be the
   worker's first proposal, using the same committed-handle checks.
   The decision between chaining and the committed path is the
   adapter's own, from its handle state, never a root comparison on the
   caller's side: an uncommitted proposal can be a storage no-op whose root
   equals the engine's.
5. `propose` is **side-effect free**: a proposal that is built and dropped
   leaves no trace on the committed state or on the root of any future
   proposal, and a `Delete` of a key the state lacks leaves the root
   unchanged (a state function), which finalization's `Delete`-if-absent retractions
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
bounds memory; the window is opened at `K × rounds` (§8). There is no
dedicated warm-path call; `prefetch` reads each key, which warms only under
`CacheReadStrategy::All`, so `New` sets that strategy (§9.4). Adapter
mechanics the backend honours: (a) the public `DynProposal<'db>` borrows the
database and its boxed `commit` consumes it, so the adapter pins the database
behind an `Arc` and owns the proposal with its lifetime erased; the live
proposal drops before its database reference. It exposes reads through the
proposal or its committed view; (b) firewood forgets a proposal at the next commit
unless something holds it, and a proposal has no parent accessor, so the
adapter handle keeps its parent handle inside its live state (dropped at
commit) to commit ancestors oldest-first;
(c) an uncommitted proposal reads its base revision's nodes by address,
so the adapter's first link on a committed view
holds that committed handle as its parent: the handle's view (an `Arc` to
the committed revision) keeps the base from being reaped for as long as
the link lives, whatever became of the node that proposed it (an orphan
handle may be read long after, §5), and firewood's reaper stops at a
referenced revision, so the window stalls rather than freeing nodes a
live proposal still reads (before the review of 2026-11-02, round 2, the
first link had no parent and such a handle read reused addresses as
data); (d) Firewood checks parent identity and absorbs a same-root commit
without allocating a physical revision. The adapter still advances its
logical generation on every successful commit, so even a no-op supersedes
older committed handles under contract items 3 and 4. It retains its own
`Arc` views and never relies on root lookup for an already-held revision.
Milestone 2 also runs the §9.3 tests 1–3 against the real engine (§13).

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
oldest-first, stopping at a committed parent (a first link's parent is the
committed handle it was proposed on, the pin of (c) above); the link goes
with the live state at commit; (c) after
`commit` the handle re-bases to the committed revision at its root
(`Db::revision`, committed revisions only, so a live sibling with the same
root is never mistaken for it), to an empty readable state when the commit
emptied the trie (under `ethhash`, to the indexed empty revision), or to a
stale state that reports `RevisionStale` if the
root cannot be looked up, while a failed commit leaves a state that reports
the failure on every operation (§9.2); (d) `propose` on a committed handle
upgrades the `Weak<Db>` (`Closed` if the engine is gone), checks that the
handle's stored generation equals the adapter's current generation and its
pinned view matches the backend head before and after proposing (item 4,
`ProposalOrphaned` otherwise). The view check covers a backend commit that
has completed before the adapter publishes its new generation. It never
panics; `commit` makes the same check on the chain's base before its first
engine commit (item 3); (e) a handle's state sits
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
follows the crate's `ethhash` feature, which selects the runtime mode passed
to `firewood::db::open`; upstream no longer has an `ethhash` feature.
The public API returns `Box<dyn DynDb>` and takes an `OwnedBatch`, so the
adapter copies each key/value into owned byte slices before proposing.
`FirewoodConfig.commit_permits` maps to `max_persistence_gap`. Its free-list
entry limit is converted to KiB using Firewood's public entry-size estimate,
rounding up (40,000 entries becomes 1,875 KiB on this 64-bit host).
The
engine contract tests (items 1–7, plus the failed-commit rule of §9.2: a
handle whose commit failed reports that failure on every later operation)
and the state-function check run against both backends
(`src/tests/storage.rs`), firewood in a temporary directory.

**Concurrency (R18, R19, R20).** Every lock is listed here with the reason
it exists and its holds; the rule is that a reader takes none, and that the
writer holds none across an engine call, a snapshot copy or a drop. Measured
on one thread (the since-removed `sync_primitives` micro-benchmark): a parking_lot mutex
hold 14 ns, a read-write lock read guard 19 ns, a set-once cell read 0.6 ns,
an atomic load 0.4 ns, an `Arc` clone and drop 12 ns.

- *Ownership first.* The writer (`AuthWal`, every state-changing method
  `&mut self`, §3) owns the precomputation's target, the worker handle,
  the storage engine, the configuration, the root terms and the poisoned
  and closed flags; the type system
  serialises writer operations, so none of these needs a lock, and two
  commits, or a commit and a propose, cannot overlap. Nodes are immutable
  (§8). The worker thread shares only its channel and the counters (§9.2); its
  chain is its own locals. Readers share the registry. The shared state is
  two things: the counters, atomics (the speculation counters behind an
  `Arc` the worker holds too), and the registry behind its mutex; the
  closed flag is the writer's own plain field, since only writer operations
  read it (reads never check one, R20); handles hold only their node (§8).
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
  `Prefetch` one (resolve the node), `Close` one when checkpointing (clone the
  committed head; none otherwise), and a reader's `Get`, `Root`, `Revision`,
  `LatestRevision` and `Stats` one each.
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
  `hint_writes`, `apply`'s clone, a retarget's `predicted`, the plain
  path's `reset_hints`), serialised by
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
- *Lock-free atomics*: the counters and the firewood handle's committed
  flag. The worker's queue is a `std::sync::mpsc` channel (no
  lock of ours; `finish` blocks on a reply channel; `Close` drops the sender
  and joins).
- *Lifecycle without cells*: with speculation enabled the worker is spawned after the shared state
  exists and holds a strong reference to the counters, so there is no attach step (the
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
race is left in the system; contract item 4's before-and-after identity
check stays as the backstop. No actor thread in v0;
the C ABI keeps `qmdb-ffi`'s handle/keep-alive conventions and serialises
writer calls with a mutex of its own (§3), so one can be added without
changing the Go side.

## 11. Benchmarks (R11)

Every CSV row records the engine and its identifier (including the Firewood
hashing algorithm), row count and number of batches, Zipf exponent, cache
size, iteration/duration limit and
warm-up count alongside the write-set configuration. `target=auth_wal` alone
does not identify its backend.

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
`--duration-minutes`, firewood's rule) and `--progress-every`. For update
workloads, after prefill and the requested warm-up, auth_wal occupancy must
be at least the low watermark before the first measured block. Otherwise the
run fails and asks for sufficient prefill or warm-up. A run ending before
measurement starts also fails. CSV `start_occupancy` and the stderr measurement
marker record the boundary; bare-engine runs have no set and are exempt.
`create` deliberately measures growth from empty and remains exempt. Flags:
`--capacity` (default 10k entries, R16; sweep from 1k to 1M),
`--low-watermark` (default 70% of capacity, rounded down, a consensus parameter),
`--high-watermark` (local precomputation threshold, default 90% of capacity, rounded down;
actual eviction remains at capacity, §6.2), `--revisions` (K),
`--policy {fifo,lru-write}`, `--vc {lthash,merkle,rehash}`, `--speculate`,
`--speculation-rounds`, `--speculation-min-step` (default 256 distinct pending
keys), `--speculation-margin`,
`--firewood-commit-permits` (positive; defaults to rounds for speculative
auth_wal, 1 for plain/baseline; must be smaller than the engine revision
window; recorded as the resolved `firewood_commit_permits` CSV field),
`--hints` (emulate execution, clarification 29: the batch is cut into
`--hint-chunks` chunks, one per emulated transaction, each hinted with
`Prefetch` after a gap of `--hint-gap-min-us` plus a geometric number of
100 µs steps truncated at `--hint-gap-max-us`, and the block is proposed
as the union of the chunks, the last write of a key winning; the hints'
latency is recorded per call), `--hint-noise` (extra keys per hint chunk
as a fraction of its size, drawn uniformly from the workload's rows and
not written by the block: emulated reads that make the hints
over-predict, the way to exercise R35, clarification 33),
`--storage-latency-us`, `--zipf-exponent`,
`--seed` (random by default on each invocation; the resolved seed is printed
at startup and in every CSV record, and an explicit value reproduces the
Zipf draws, hint gaps and hint noise with the same configuration),
`--warmup-batches` (tenkrandom/zipf/single), `--prefill` (insert all `rows()` keys before the
workload, in-process and outside the measurement, through the write set when
there is one, so tenkrandom/zipf/single update a populated trie: the
experiment of clarification 28, where firewood's hashing cost grows with the
database while the set's work does not), `--breakdown` (print, before the
CSV record, where `Propose` and `Commit` spent their time phase by phase
over the measured blocks, §9.5, with the worker's step counts: the
instrument of clarification 31), and `--baseline` to run the same
workload straight into the storage engine with no write set in front.

Reported per run: commits, elapsed time and commits per second, mean and
p99 propose and commit latency (`Propose` includes terminal worker
finalization, §9.2; `Commit` has no worker request but can wait on engine
handle contention or persistence permits),
hint-path time with the per-call hint latency's mean and p99
(clarification 29: what the execution thread pays for sending hints),
worker proposal time (reported separately; final worker builds also lie
inside `Propose`, so worker and caller times are not additive), set occupancy,
evictions per commit, storage ops per commit, puts per commit (the block's
size after the hint-mode union), in-place update ratio, reused and reverted
ops per commit and steps per commit (§9.5), peak RSS, step failures and
fallbacks (§9.5). Latencies and counter deltas exclude prefill and warm-up;
peak RSS is the process high-water mark and includes both. Close timing is
reported separately as `[close] elapsed_s`, outside the measured workload,
and includes worker shutdown, final persistence drain, and resource release. With
`--hints` the block is proposed as the union of its chunks, so
`puts_per_commit` counts the distinct puts (786 of 2400 draws on `zipf`)
while an unhinted run of the same seed counts the draws: compare like with
like. The `target` column names the engine or
`auth_wal`. What counts (clarification 31, R33): the validator's critical
path is `Propose`, the new root, so propose latency is the figure of
merit; `Commit` marks the proposal committed, and firewood's `commit`
signals its persist worker and returns, so persistence is asynchronous.
Commit latency still matters when committing a speculative chain exhausts
the persistence permits; it measures queue acceptance, not durable storage.
The driver fills the configuration columns; a workload returns measurements
only. Output is a CSV header plus one record per run
so sweeps over capacity × policy × speculation rounds × hints can be plotted directly. The
criterion micro-benchmarks of the early rounds (the accumulator, the
per-proposal copy of the structures since replaced, the synchronisation
primitives) were removed in the review of 2026-11-02 (§14): their figures
are recorded in §4, §7.2, §10, §12.13 and §14, and the breakdown measures
the paths in situ.

## 12. Open questions

1. **Per-key proofs under lattice hash.** `lthash` authenticates the set, not
   individual entries. Either accept set-level authentication for v0 or move
   the default to a proof-capable scheme once `kzg` exists.
2. **KZG slot mapping.** Slot count vs capacity, and the collision policy
   (chain into the slot value, cuckoo-style displacement, or reject).
3. **Read-aware policies.** Ruled out in v0 for determinism; would need the
   adapter to signal execution-time reads separately from RPC reads.
4. **Eviction cadence.** Decided (R16, §6.2): bulk eviction to a low
   watermark, 70% of capacity by benchmark default; the capacity/watermark ratio stays a
   §11 sweep dimension.
5. **Speculation knobs and the worker.** The knobs (`rounds`, `min_step`,
   `margin`) are provisional until the milestone-3 sweep, as are: whether
   to plan a step at a commit before any hint arrives (round 0 of v16–v23,
   dropped by R25; R31's re-send of the kept marks at a retarget is the
   part of it that came back, driven by the policy's marks rather than a
   fresh prediction); whether one worker thread per instance suffices (R17; a
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
   benchmark. Measured in the latency breakdown (§14, clarification 31):
   `finish` on the critical path is the tail step, so `rounds` must cover
   the block's hints with `min_step` 1 (rounds 30: `finish` 0.66 ms on
   `zipf` and 72 µs on `tenkrandom`, against 2.7 and 28 ms at rounds 3 /
   min_step 100 and 5.7 and 24 ms at rounds 8 / min_step 1, whose eager
   steps are spent on the first hints); the chain's commit then costs
   16–68 ms of firewood's persistence backpressure, off the critical path
   (R33), and the engine window `K × rounds` grows with it (§8). The
   defaults stay provisional until the milestone-3 sweep confirms this on
   other block shapes.
6. **Clean-shutdown persistence and recovery (R41).**
   `Local::snapshot_path: Option<PathBuf>` selects a checkpoint file;
   `None` retains in-memory operation and is the benchmark default.
   `New(config, storage, snapshot)` still initializes fresh storage and refuses
   an existing checkpoint file. `Recover(config, storage, vc, policy)` opens
   the saved committed state after the caller reopens the matching engine
   without truncation. It never silently falls back to empty initialization.

   A checkpoint contains capacity, height, sorted keys and value/tombstone
   records, policy consensus state, scheme/policy/engine identifiers and
   parameter digests, storage root, combined root, and metadata hash. Nodes
   now retain the metadata hash so recovery can recompute the exact root;
   metadata bytes remain caller-owned. The policy's `encode_state(keys)` and
   `restore_state(bytes, keys, height)` methods handle consensus state only.
   Built-in policies encode one u64 write height per sorted snapshot key and
   reconstruct their order/index arrays. They validate one height per key in
   `[1, snapshot_height]`; hints, marks, and worker proposals are discarded.
   Custom policies may opt in through those methods; enabling a snapshot path
   for an unsupported policy fails at initialization.

   File format v1 is `AWALSNAP`, u32 LE version, three length-prefixed IDs,
   two 32-byte parameter digests, u64 LE capacity/height, three 32-byte hashes
   (combined root, storage root, metadata), u64 LE entry count, entries,
   policy-state bytes, and a BLAKE3 checksum of the preceding bytes. Byte
   fields use u32 LE lengths. Each entry is a key followed by tag 0 for a
   tombstone or tag 1 plus value bytes. The decoder bounds lengths/counts,
   rejects malformed or unsorted/duplicate entries, and reconstructs keys
   and values with `TryFrom<Vec<u8>>` (supported by all built-in byte types).
   It rebuilds the commitment state for any scheme and verifies the combined
   root, as well as checking the checksum, version, consensus parameters,
   capacity, and current storage root. Local settings may change on recovery.

   `Close` marks the writer closed and joins its worker as before. It clones
   the committed head under one brief registry hold, then performs all
   encoding and I/O outside that lock. It writes and syncs a temporary file
   in the destination directory, closes/flushes the engine, atomically
   replaces the checkpoint, and syncs the directory on Unix. Failed writes
   or engine closes leave the preceding checkpoint intact. I/O and
   `ActiveHandles` errors can be retried; a storage-close failure other than
   `ActiveHandles` poisons checkpoint publication, including subsequent
   attempts if the engine consumed its database before returning the error.
   A poisoned writer can close storage but cannot publish a checkpoint.
   A completed close is idempotent. Reads through existing handles retain
   their existing behavior; drop all old Firewood handles/readers and the
   writer before reopening its file in the same process.

   Recovery installs only the latest committed node, with fresh counters and
   a fresh worker. Historical revisions, uncommitted proposals, and predictions
   are not restored. `MemoryStorage` remains an in-memory model: recovering
   its set requires independently restoring matching engine state if any
   entries had been evicted. This protocol guarantees recovery after a
   successful close, not per-commit or crash durability. Storage and checkpoint
   publication are not one atomic transaction; mismatched storage is rejected,
   and matching roots alone cannot prove a checkpoint includes writes lost in
   a crash. WAL/replay, rollback, and state-sync support remain future work.
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
   The read-back revert is kept as is by decision (clarification 14). (f)
   A proposal that survives a commit of another proposal on the same base
   (a rebase onto the new head, or a commit that keeps a sibling's hashed
   nodes reusable), so the precomputed steps of predictions the block kept
   (R31) carry their hashing into the next block instead of being rebuilt
   on the new base; today only the prediction survives (§9.1).
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
   the full copy per proposal, which also removed the overlays. The remaining
   question, whether the per-write overhead (entry insert, commitment
   update, policy update) lands near the 0.5 µs target, is answered by
   milestone 1's first numbers (§14): about 6 µs per op plus 2 ms of copy
   per batch at 10k, ten times the target; profiling is next.
   The fourth measurement (2026-10-29, clarification 27, at capacity 100k)
   separates the O(n) terms of a proposal from the per-op work, one core,
   best of seven: cloning the entry map takes 0.64 / 9.4 / 190 ms at 10k /
   100k / 1M entries (`Vec<u8>` values; 0.48 / 5.1 / 184 ms with
   `Arc<[u8]>`), each of the policy's two maps 0.15 / 1.06 / 27 ms, the
   BLAKE3 digest of the policy order 0.72 / 7.3 / 81 ms, and dropping a
   retired 100k snapshot at commit about 8 ms; a 1-op proposal through the
   public API costs 1.3 ms at 10k and 20.4 ms at 100k, a 1000-op batch adds
   6–7 µs per op at either size, and an overflow evicting 51k entries takes
   370 ms (7 µs per victim, 5 of them the memory engine's own). End to end
   (1000-op batches, K = 128, lthash) auth_wal is the same on every engine
   and workload, because the fixed terms dominate: propose 26–30 ms and
   commit 6–12 ms, 23–30 commits/s, against bare firewood at 93 commits/s
   on `zipf` (propose 2.4 ms, commit 7.5 ms on the 500k-row database) and
   37 on `create`, and the memory model at 116–130; auth_wal keeps 95 % of
   `zipf` puts in place and sends 50 storage ops per commit instead of
   1000, and holds 2.8–3.4 GB against the baselines' 69–185 MB. The
   alternatives at 100k, per 1000 ops: a persistent ordered map
   (`im::OrdMap`) clone plus the batch 3.5 ms (1.1 at 10k, 11 at 1M), the
   policy's order and index as persistent structures 1.2 and 0.85 ms,
   lookups 0.33 µs against `BTreeMap`'s 0.2; a mutable `HashMap` with the
   old values pushed to an undo log 0.11 ms (0.15 at 1M), rollback
   included 0.18 ms, lookups 45 ns; the lattice's two expansions per write
   1.6 ms regardless of size. Reading: the full copy per proposal is viable
   only at about 10k; path copying removes the cliff and the K × n memory
   but leaves the set at about 7 µs per op, above firewood's own 2.4 µs
   per op; only a mutable base reaches the 0.5 µs target, and it brings
   back what R18 and R20 removed (§12.15). Decision open (clarification
   27).
   The fifth measurement (2026-10-29, clarification 28, the experiment the
   owner meant) keeps the set at 10k and grows the trie under it: firewood
   pre-populated in-process with 100k and 1M rows (`--prefill`), 1000-op
   batches, K = 128, lthash, `zipf` with 50 warm-up and 200 measured
   batches, `tenkrandom` 250 batches. Bare firewood slows with the trie:
   `zipf` propose + commit 2.7 + 8.1 ms per batch at 100k rows (85
   commits/s), 4.0 + 13.7 ms at 1M (54 commits/s, commit p99 90 ms);
   `tenkrandom` 7.5 + 15.0 ms (43 commits/s) and 11.8 + 24.7 ms (27
   commits/s). auth_wal's own cost does not: `zipf` propose 9.1 ms and
   commit 1.4 ms at 100k (89 commits/s with lru-write, 93 with fifo; 90 %
   of puts in place, 101 storage ops per commit), 10.9 + 1.6 ms at 1M (72
   commits/s, 86 % in place, 152 storage ops per commit); `tenkrandom`
   15.9 + 1.8 ms (55 commits/s) and 25.3 + 3.5 ms (33 commits/s) with only
   17 % of writes in place and 885 storage ops per commit, because the
   evictions reach firewood as 6000-op batches every six or seven commits,
   which hash shared paths once and commit once. Gains: `zipf` +5 % (fifo
   +10 %) at 100k rows and +33 % at 1M; `tenkrandom` +27 % and +24 %.
   Inserting the 1M rows took 30 s through firewood and 24 s through the
   set. Costs: propose p99 60–140 ms on the eviction blocks (§6.2's bulk
   eviction; a watermark nearer capacity or milestone 2's speculation
   spreads it), and 450–630 MB RSS against 170–200 MB (K × 10k copies).
   At 10M rows (prefill 400 s either way; the trie no longer fits the
   192 MB node cache) the gain holds but narrows: `zipf` baseline 5.2 +
   14.2 ms (49 commits/s) against auth_wal 13.7 + 2.7 ms (56 commits/s,
   84 % in place, 178 storage ops per commit), +14 %; `tenkrandom` 15.4 +
   27.9 ms (23 commits/s) against 23.9 + 14.8 ms (25 commits/s), +10 %,
   with auth_wal's propose p99 at 110–118 ms and its `tenkrandom` commit
   p99 at 105 ms. From 1M to 10M rows firewood's per-batch cost grew by a
   tenth to a fifth while auth_wal's grew by a third: the set keeps the hot
   keys, whose paths firewood would have cached anyway, and sends the cold
   ones, whose paths it must read from disk, in 6000-key eviction batches;
   fewer ops, but the expensive ones. That is the case for milestone 2's
   speculation (victims' paths hashed ahead, off the block's path) and for
   a larger node cache, both to measure at 10M.
   Reading: at the default capacity the layer pays for itself from about
   100k rows, by a third at 1M and a tenth to a seventh at 10M with the
   default cache, which is the premise of R16's small set; the 10k
   structure question is closed in favour of the copy, and the §12.15
   question is only about growing the set.
14. **Snapshot refinements** (the flat sorted array is now the structure,
   clarification 33, §4; the policy's index is still its own array, one
   merge pass per block that folding the height into the entries would
   save, about 0.1–0.3 ms; the rest stands as written), to measure in
   milestone 1 rather than decide
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
15. **A mutable base and the policies (open, clarification 27; moot at
   10k since the flat snapshot, clarification 33: the copy is a memcpy
   and the set's work 2.2–3.1 ms per block, §14).** What
   replacing R20's copy with undo logs would take, written down so the
   decision can weigh it. The set: one mutable committed base (a hash map;
   canonical order is not needed once every digest is a lattice hash, and
   the Merkle reference addresses leaves by key hash), a small overlay per
   proposal holding the batch and the evictions it caused (reads on a
   proposal walk its chain's overlays, then the base, as R16 had), and an
   undo delta per committed revision holding the old entries of the keys
   the commit changed or evicted, so a reader of revision `r` resolves a
   key through the deltas of `r + 1 ..= head`, then the base; readers of
   the head share the base with the committing writer, so §5's "no lock"
   becomes a read lock or a left-right pair. The policies, the owner's
   concern: a policy's consensus state becomes **per-key attributes stored
   in the entry** (insertion height for FIFO, last-write height for
   LRU-write, a counter for LFU, a queue tag for 2Q), so the overlay and
   the undo delta carry it with the value and no policy writes undo code;
   the policy keeps only a **local index over the base** (height buckets
   with lazy invalidation: a bucket member whose entry no longer carries
   that height is skipped), updated at commit and never undone; victim
   selection for a proposal walks the oldest buckets filtering through the
   chain's overlays (rewritten in the chain: not a victim; evicted earlier
   in the chain: gone; inserted in the chain: in the overlay and newest);
   `state_digest` (R25) becomes an incremental lattice over `(key,
   attribute)` items, O(1) per write in place of the O(n) BLAKE3 of §7.3
   (the owner anticipated the swap in clarification 21). A policy with a
   global scalar (ARC's target) keeps it as a copied value in the overlay;
   ghost lists are keyed by evicted keys and would be a second kind of
   entry. Roots stay the same function of entries and attributes, so the
   equal-roots rule of round 30 holds, and the replay-equivalence
   machinery of v15 (the policy answering through a chain what the
   committed state would answer) returns in this bucket-and-filter form.
   Measured floor (§12.13): 0.1–0.2 µs per write for the base and its undo
   record, after which the lattice's 1.6 µs per write dominates (§12.12).
16. **Where `Propose` spends its time (clarification 31, R33; measured,
   §14 "Latency breakdown").** `Propose` is the critical path and the
   figure of merit; `Commit` is off it (firewood's commit signals its
   persist worker and returns). Phase by phase on the milestone-2 setup
   (firewood with 2.4M rows, set 10k, hinted blocks of 2400 draws), the
   set's own work (`copy` + `ops` + `writes` + `remove` + `digest`) is
   8.4–10.5 ms per block on `zipf` (786 ops) and 17.8–19.4 ms on
   `tenkrandom` (2400 ops) in every configuration, plain or speculative:
   more than bare firewood's whole propose (7.1 and 15.9 ms). Per op the
   `ops` phase costs 4.2 µs for an update (two lattice items at 1.12 µs
   each in the since-removed micro-benchmark, plus the map) and 3.1 µs for an insert;
   the eviction 3.5–4.9 µs per victim with the LRU order updates; the copy
   2.1–4.4 ms per block in situ (0.28–0.46 µs per entry, growing with the
   resident count and 2–4× the hot-cache micro-benchmark's 1 ms); the
   digest 0.7–1.1 ms. The engine side of the critical path is cold-path
   puts: the victims are by construction the least recently written keys,
   so their paths are not in firewood's node cache, and a put costs 17–22
   µs (`zipf`) or 13.5–15 µs (`tenkrandom`) against 9 and 6.6 µs per
   hinted (warmed) op in the baseline. Speculation moves them to the worker
   except the **tail step**, the ops predicted after the last eager step,
   hashed inside `finish` on the critical path: 2.7 ms on `zipf` (111 ops)
   and 28 ms on `tenkrandom` (about 2000 ops) with rounds 3 and min_step
   100, where the two eager steps fire at the second hint and the other 28
   hints pile into the tail; an empty `F` costs 6–130 µs. With a step per
   hint (rounds 30, min_step 1) the tail is one hint's worth or nothing,
   `finish` costs 0.66 ms on `zipf` and 72 µs on `tenkrandom`, and propose
   is the set's own work alone: 10.2 ms against bare firewood's 7.1, and
   20.7 against 15.9 (§14 follow-up). The chain's
   commit (14–75 ms) is firewood's persistence backpressure
   (`deferred_persistence_commit_count`, default 1: the second commit of a
   chain waits for the first revision's persistence), not hashing: off the
   critical path under R33 and a knob. Levers, each to be measured against
   the breakdown: for the set's work the lattice (parallel accumulation
   over the batch by the homomorphism, §7.2), the digest (incremental,
   §12.15), the copy (values by reference, the §12.14 refinements, or the
   mutable base of §12.15) and the LRU updates; for the engine side, the
   knobs (measured: rounds covering the block's hints with min_step 1) or,
   without a chain and its commit backpressure, warming the victims' paths
   ahead of one final proposal (`docs/TODO.md`).
17. **The set's own work: measured and proposed (clarification 32,
   R34–R36; §14 "Clarification 32").** With a step per hint and the
   lattice, the set's work was 9.8 ms on `zipf` and 21.4 ms on
   `tenkrandom`; the `rehash` scheme (R34, §7.2) brings it to 6.2 and 9.0
   ms and propose to 6.9 and 9.1 ms, at or under bare firewood's 7.1 and
   15.9. What the rehash showed: in situ a lattice item costs 2.7–2.8 µs
   (1.12 in the hot micro-benchmark; the ratio is not explained), and the
   B-tree work of `ops` and `remove` is 0.3–0.4 µs per op and per victim,
   so §12.16's split of the per-op cost was wrong: the lattice was nearly
   all of it. What is left, by size: the two copies (1.7–2.0 ms each,
   44–56 % of the set's work), the policy's `writes` (0.7 ms on `zipf`,
   2.1 on `tenkrandom`), the policy digest (1.0 ms), the rehash (0.7 ms),
   `ops` plus `remove` (0.4–1.3 ms). Why the copies cost what they do
   (micro-benchmarks in the session's scratchpad, hot and after a 128 MB
   cache flush): the same 10k-entry B-tree map clones in 0.45 ms hot and
   1.15 ms cold in a fresh heap, and in 0.81 ms hot and 2.8 ms cold in a
   heap that has seen 300 blocks of the in-situ pattern (a ring of eight
   revisions, splits and removals, other allocations in between); the
   policy's two maps 0.37 and 1.5–2.1 ms there; dropping a retired
   revision 2.5–2.8 ms cold (the `advance` phase of `Commit`, 2.2–2.9 ms in
   situ). A clone of a B-tree is one cache miss per node and per value, on
   the source and on the freshly allocated target, and a fragmented heap
   scatters both. Ruled out: firewood's threads (the memory engine gives
   2.5 and 2.4 ms copies), the allocator's trimming (glibc with trimming
   off: 1.39 ms for the policy copy, 1.69 for the entries), the sleep
   between hints (a copy after a 3 ms sleep is 10–40 % slower on the mean,
   not 2–5×) and the second socket (pinned to one node: 1.45 ms for the
   policy copy, 1.67 for the entries). A contiguous array of the same
   entries copies in 0.04 ms hot and 0.14 ms cold in any heap.

   Proposals, each for the owner's decision before code changes (R36) and
   to be measured with the breakdown. **Status (clarification 33, R37,
   §14 "Clarification 33"):** (b) done as step 1; (a) done as steps 2 and
   3 (the index not folded into the entries, §12.14); (d) measured as
   step 4 and reverted (slower in situ); (c) done as step 5 with the
   benchmark's `--hint-noise`.

   (a) **Flat snapshot.** The entries as one sorted array of fixed-size
   records (key, value reference, height), the policy's order as a second
   sorted array of (height, key), the policy's index folded into the
   entries' height column (§12.14). A proposal builds its child in merge
   passes over sorted inputs: the batch sorted by key with the last op per
   key winning, the victims chosen first from the order's front, then one
   pass over the entries (copy, ops and removal in one: 0.25–0.3 ms cold
   in the micro-benchmark against today's 2.1–3.2 ms in situ for
   `copy_entries` + `ops` + `remove`) and one over the order (the written
   keys' old places skipped, the block's run appended, the victims cut
   from the front: with the index merge 0.5–1.1 ms cold against today's
   2.4–4.1 ms for `copy_policy` + `writes`). Both digests become one
   `update` over contiguous bytes: the policy order 0.13–0.15 ms (1.0
   today), the rehash 0.24 ms single-threaded with no serialisation (0.66
   today; 0.11 with `update_rayon`). Values: inline for a fixed-size `V`
   (the benchmark); for variable-size values (the FFI's `Arc<[u8]>`) an
   append-only arena of `Arc` chunks referenced by (chunk, offset,
   length), one chunk per block, compacted when the dead share passes a
   half, so the copy stays a memcpy and no reference count is touched.
   Reads: a binary search over 10k records is 0.46 µs cold against the
   B-tree's 0.2 µs; an Eytzinger layout or a sampled first level (every
   64th key in a 10 KB array) closes that. Projected set work from the
   cold micro-benchmarks: about 1.8 ms on `zipf` and 2.4 ms on
   `tenkrandom` (2.3 and 3.0 with the in-situ margin the rehash showed),
   against 6.2 and 9.0 today and bare firewood's 7.1 and 15.9 for its
   whole propose; the drop of a retired revision becomes three frees. R20
   stands (a copy per proposal, no chain, lock-free readers), and
   §12.15's mutable base becomes unnecessary at 10k. At 100k the memcpy is
   6.4 MB (about 1 ms) and the two whole-set hashes about 10 ms
   single-threaded, so a large set would take the lattice back for the
   entries (the trait allows both) or hash on several threads. Cost: a
   rewrite of `set.rs` and of the policies' state (`writes` taking and
   returning sorted lists; the then-shared `Marks` unchanged), the largest change
   proposed.

   (b) **Policy digest from one buffer.** `order_digest` serialises the
   order into a `Vec<u8>` (44 bytes per entry) and hashes it with one
   `update`: 1.0 ms to about 0.15 ms, a ten-line change independent of
   (a). This replaces the "incremental digest" of §12.15 and
   `docs/TODO.md`: a lattice over (height, key) items would cost two items
   per written key and one per victim at 2.8 µs each in situ, 5 ms on
   `zipf` and 19 ms on `tenkrandom`, far more than the full hash at 10k.

   (c) **R35, the no-eviction block** (§9.1; implemented in clarification
   33 step 5, corrected in the review of 2026-11-02): no `finish` wait,
   the parent's view inherited, the chain kept, `Commit` with no engine
   commit, the child made the target at `Propose` (§9.2); the policy's
   refinement for a non-evicting block narrows to the kept marked keys the
   block wrote (re-sent under FIFO, retracted under LRU-write) and `F` at
   an evicting block reverts `predicted()`; the budget `rounds` stays per
   chain however many blocks it spans (a block end without a reply builds
   only while there is room, the rest goes into the next tail), so the
   engine window of §8 holds. Saves, on such a block, `finish`, the
   read-backs, `F` with one put per marked key, the chain's commit (16–68
   ms of backpressure) and the re-send of every mark. Measured with the
   benchmark's `--hint-noise` (hinted keys the block does not write, as
   execution's reads would be; the exact hints never over-predicted): §14
   "Clarification 33", step 5.

   (d) **Rehash without serialisation or on several threads**: 0.66 ms to
   0.24 (hashing the flat entries of (a) directly) or 0.11 (`update_rayon`,
   contending with firewood's pool).

   Not a lever: the lattice's per-item cost is compute-bound; the scheme
   stays for large sets, where the whole-set hash loses.

## 13. Layout and milestones

```
auth_wal/
  Cargo.toml                  workspace root; lib (rlib; milestone 4 adds staticlib), features: firewood, ethhash
  src/lib.rs                  AuthWal<K, V, C, P, S> (the writer, &mut self) and Reader, both over Arc<Shared>; Config, Stats
  src/types.rs                Key/Value bounds, Hash, Digest, EMPTY_ROOT, BatchOp<K, V>, Error
  src/set.rs                  Snapshot (the whole set at one node: sorted records, value arena) and its one-pass batch application, Entry<V>, EntryRef, Entries
  src/policy/{mod,fifo,lru_write}.rs   EvictionPolicy (writes / hint_writes / predicted / reset_hints / validate / state_digest), per-policy prediction state and reconciliation, split_order, merge_index, order_digest, params_digest, validate_watermarks; the v0 policies on sorted arrays, each with its own watermarks
  src/vc/{mod,lthash,merkle,rehash}.rs   VectorCommitment (commitment is handed the entries, R34), the entry item's fields; lattice, sparse Merkle reference, whole-set rehash
  src/root.rs                 root composition (§7.3)
  src/lattice.rs              lattice-hash accumulator (add/remove fields, digest) shared by lthash and MemoryStorage
  src/storage/{mod,memory,firewood,contract_tests}.rs
  src/proposal.rs             Node (immutable), Handle, Registry
  src/speculate.rs            Worker<K, V, S> (submit / terminal finish / block_end / reset over a channel), chain construction and final reconciliation, SpecOp, Request, Counters (§9)
  src/timings.rs              ProposeTimings, CommitTimings: per-call phase timings (§9.5), the benchmark's --breakdown
  benchmark/                  workspace member; firewood-style driver (implemented)
    src/{main,target,workload,workloads,report}.rs   flags and targets; the shared loop, hint emulation and recorder; the four workloads; the CSV and the breakdown
  docs/                       this directory
```

Milestone 4 adds `src/ffi/` (the C ABI, cbindgen → `auth_wal.h`, names
`auth_wal_*`, behind a feature `ffi` that also enables `firewood`), the
`staticlib` crate type and the Go package `ffi` mirroring qmdb-ffi's files;
the stubs that stood for them were removed in the review of 2026-11-02.

Library tests are organized in `src/tests/`, with modules for policy rules,
WAL behavior, speculation, injected faults, persistence, storage contracts,
and consensus encodings. `common.rs` contains shared fixtures. The library
loads them through its `#[cfg(test)] mod tests`; private implementation tests
remain inline in their own modules. Test organization does not change the
public API or expose internals solely for testing.

Milestones

0. **Done (2026-10-26).** Benchmark suite (§11) with the engine-only
   baseline against the in-memory model and vanilla firewood; `MemoryStorage`
   and the firewood backend (§10) with the engine contract tests (items 1–7)
   and the state-function check; the lattice accumulator (§7.2). Every design
   change from here on is measured against the firewood baseline.
1. **Done (2026-10-29).** Core library: snapshots and batch application
   (§4, §5), FIFO + LRU-write policies with their tests (§6.1), lthash and
   the `merkle` reference scheme, registry and handles, plain path only,
   unit tests shared across policies and schemes (`src/tests/`: the §6.1
   list, the §8 lineage rules, the revision window, a differential test
   against a reference map, a firewood smoke test); the benchmark's
   auth_wal target (`WriteSet`). Reviewed on 2026-11-02 (§14).
2. **Done (2026-10-29, R29–R32).** Speculative eviction and prefetch hints
   (§9): `build_step`, `New` accepts `speculate`, predictions that outlive
   the block (R31), the local precomputation trigger (R39), §9.3 tests 1–5 (test 6
   pending), §9.5 metrics in the benchmark with the hinted-block emulation
   (§11). Reviewed on 2026-11-02 (§14), which found and fixed the R35
   chain-handover hazard and the full-chain `finish`.
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

Milestone 1 (2026-10-29, clarification 26). The owner called the v27 shape
good enough and asked for an initial implementation without
precomputation. The two reviewers launched on R26–R28 died on the session's
rate limit before reporting, so R26–R28 stand unreviewed by the owner's
call. Implemented: every `todo!()` but `speculate::build_step`; `New`
refuses `speculate = true` (`InvalidConfig`) until milestone 2; the sparse
Merkle reference scheme (depth 256, a defaults table, positions by level and
prefix); 23 tests in `src/tests/` (both policies' semantics and digests,
the then-shared `Marks::reconcile`, the plain-path suite for every scheme × policy on
`MemoryStorage`: fresh instance, reads and roots, evictions reaching
storage, the revision window, the §8 lineage rules, metadata and the
equal-root property, chained equals sequential, close and errors, a
differential test; one smoke test on firewood); the benchmark's `WriteSet`
target with the four monomorphizations and the engine window from
`engine_window`. The battery is green; the implementation's own review
round is pending. First release-mode numbers (milestone 1, 2026-10-29; 1000-op batches,
capacity 10k, watermark 5k, `lthash`, one machine, no profiling yet). On
the memory model engine, `create` (every key new, nothing can stay):
baseline propose 4.9 ms per batch, auth_wal 12.7 ms (p99 59 ms on the
eviction blocks); `zipf`: baseline 5.5 ms, auth_wal 7.6 ms with 91 % of
puts landing in place and 50 storage ops per commit instead of 1000. On
firewood, the engine of record: `create` baseline propose 4.0 ms and commit
11.7 ms (61 commits/s), auth_wal propose 11.0 ms (p99 61 ms) and commit 3.8
ms (66 commits/s: the work moves from commit to propose, four commits in
five carry no storage proposal); `zipf` from a fresh database with 50
warm-up batches: baseline propose 2.5 ms and commit 1.1 ms (219 commits/s),
auth_wal lru-write propose 8.9 ms (p99 32 ms) and commit 0.06 ms (103
commits/s) with 90 % in-place updates and 101 storage ops per commit, fifo
8.2 ms and 111 commits/s. Reading: the set's own cost per 1000-op batch at
10k entries is about 8 ms, roughly 2 ms of copy and 6 µs per op, ten times
the 0.5 µs per-write target, and larger than firewood's own propose cost
per op (2.5 µs), so on this workload auth_wal halves throughput even while
cutting storage ops tenfold. The per-op suspects, to profile before any
change: two lattice expansions per write (about 1.5 µs), three ordered-map
operations in the policy, the benchmark's `Vec<u8>` values cloned at every
copy (§4 says `Arc<[u8]>`), the full policy digest per proposal (§7.3), and
the key clones on the batch path. The §12.14 levers follow the profile.

Measurement at 100k (2026-10-29, clarification 27). Asked for runs with
many more keys and whether a bad result means the copy-on-write set should
give way to undo logs, with the worry that undo logs sit badly with the
policies. Ran the four end-to-end comparisons at capacity 100k (watermark
50k, 1000-op batches, K = 128, lthash; `zipf` over 500k rows with 1000
warm-up and 1000 measured batches; fresh firewood database per run) and a
micro-benchmark that isolates each O(n) term of a proposal and measures the
alternatives at 10k, 100k and 1M (§12.13). Result: auth_wal is identical on
every engine and workload, 26–30 ms propose and 6–12 ms commit, 23–30
commits/s, because the fixed terms of the copy dominate (20 ms in propose,
8 ms to drop the retired snapshot at commit, 28 ms before the first op; 1.4
ms at 10k); bare firewood runs 93 commits/s on `zipf` and 37 on `create`,
the memory model 116–130; RSS 2.8–3.4 GB against 69–185 MB. The per-op
work is unchanged at 6–7 µs. So the full copy of R20 is only viable at
about 10k, as R16 anticipated. The two ways out are measured and recorded
in §12.13, and §12.15 writes down the mutable-base shape, including how the
policies avoid undo code of their own (per-key attributes in the entries, a
local lazily-corrected index, an incremental lattice digest). §4 points to
the result. No design change: the decision is the owner's and R20 stands
until it is taken. The micro-benchmark lives in the session's scratchpad
(`k100/micro`), not in the crate; `benches/lattice.rs`'s `cow_base` group
is where a kept version belongs.

Prefilled trie (2026-10-29, clarification 28). The owner corrected the
reading: "100k" meant key-value pairs already in storage, with the set at
10k, the point being that firewood's Merkle root costs more as the trie
grows, so the set's lattice hash and copy may win. The benchmark driver
gained `--prefill` (§11): a workload's `rows()` keys are inserted
in-process before the measurement, through the write set when there is one
(v0 requires a fresh engine, §1), and the firewood target truncates when
prefilling. Results at 100k and 1M rows in §12.13: auth_wal is ahead of
bare firewood on both workloads at both sizes, by 5–10 % on `zipf` at 100k
rows and 33 % at 1M, by about a quarter on `tenkrandom`, and the margin
grows with the trie because firewood's per-batch cost grows while the
set's stays at about 8 ms. The 10k structure question is closed in favour
of the copy (R20 stands); §12.15 remains open only for a larger set. The
bulk eviction's propose p99 (60–140 ms) is the visible cost, for §6.2's
watermark or milestone 2 to spread. At 10M rows, beyond the node cache,
the gain narrows to +14 % on `zipf` and +10 % on `tenkrandom`: the set
sends firewood the cold keys, whose paths are the uncached ones, so the
remaining storage ops are the expensive ones (§12.13); the next
experiments are a larger `--cache-memory-limit` at 10M and, once
milestone 2 exists, speculation at 10M.

Milestone 2 (2026-10-29/30, clarifications 29–30, R29–R32). The owner asked
for speculation on, with the benchmark feeding each block as thirty hints
of about eighty writes at 1–5 ms geometric gaps and proposing their union,
the hint cost reported beside the final computation, a "95 % full"
heuristic for `hint_writes`, predictions that survive the block, and a fuzz
test that hints never change the eviction. Implemented: `build_step`
(evictions as `Put`/`Delete`, retractions read through the base), `New`
accepts `speculate`; the v0 policies gain an eviction trigger
(`with_trigger`, R30; the capacity by default, the low watermark for the
incremental rule) and `hint_writes` predicts what `writes` would evict on
the projected size, the block's new keys counted once across its hints and
every hinted key excluded under lru-write (the two hinted sets of v24 are
back, each for a measured reason: without the count thirty hints predict
only the first hint's share, and without the hinted set lru-write predicted
keys an earlier hint had shown written, 32 puts back per block on `zipf`
where fifo had none); `writes` keeps the marks of the kept keys and a
retarget re-sends their entries as the new chain's first batch (R31), while
a node that took the plain path resets its copy; `predicted` joins the
trait. Tests: 31, among them the fuzz test of R32 for both policies and
both rules over six blocks, speculation against the plain path (equal
roots and storage roots at every height, storage exact outside the set,
steps built and reused, no failure) under both rules, two children of one
target either committed, off-target hints and `close` with a chain, and
speculation on firewood; §9.3 test 6 is pending. The benchmark gained
`--high-watermark`, the hint gaps and the union proposal, and the hint
latency, worker time and puts-per-commit columns (§11). The battery is
green; the implementation is not reviewed.

Numbers (firewood prefilled with 2.4M rows, `rows() = 1000 × 2400`, a
correction of the "1M" first reported; set 10k, K = 8, blocks of 2400
draws hinted as 30 × 80 with 1–5 ms gaps, about 80 ms of emulated execution
per block, so every target runs 5–10 blocks/s; `zipf` 786 distinct puts
per block, 50 warm-up and 200 measured; `tenkrandom` 2400 ops, 250). On
`zipf`: bare firewood proposes in 6.3 ms and commits in 0.2 ms (the gaps
let its persistence overlap; 13.7 ms without them, §12.13), and its hint,
a read-through warm of 80 keys, costs 0.43 ms (p99 2.0 ms); the plain set
under the bulk rule proposes in 14.4 ms (p99 115 ms) and commits in 1.8 ms
with 366 storage ops per block; speculation under the bulk rule (rounds 8,
min_step 1) proposes in 9.7 ms (p99 38 ms) and commits in 20.2 ms (p99 218
ms), `F` carrying 50 ops (361 reused, 45 put back), 3.7 steps per block,
the hint costing 112 µs (p99 1.2 ms: the hint that crosses the capacity
clones the bulk prediction), the worker 6 ms per block off the path; the
incremental rule (trigger = watermark = 9500) proposes in 17.5 ms (p99 22
ms) plain with 329 ops per block, and with speculation (rounds 8) in 16.6
ms (p99 20.5 ms) with 52 ops in `F`, committing in 23.4 ms (p99 46 ms)
over 8 steps. The knob sweep under the incremental rule: rounds 3 with
min_step 100 is best, propose 14.0 ms (p99 18 ms); rounds 2, 16.5 ms;
rounds 1 (the whole prediction hashed inside `finish`), 19.4 ms; a margin
of 20 adds 19 puts back and nothing else; commit is 21–24 ms in every
case. Those runs still carried 45–49 puts back per block under lru-write;
with every hinted key excluded (the fix above) the rerun shows none: `F`
is empty under lru-write (329 of 329 victims reused) and carries only the
16.7 stale rewrites under fifo, and the best setting, lru-write with the
incremental rule, rounds 3 and min_step 100, proposes in 12.7 ms (p99 15.3
ms) and commits in 18.0 ms (p99 44 ms); under the bulk rule with the same
knobs 9.6 ms (p99 61 ms) and 14.4 ms (p99 198 ms), the chain existing only
on the eviction blocks (0.19 steps per block). On `tenkrandom`: baseline 16.1 + 1.6 ms (hint 1.3 ms, p99 13 ms:
cold paths), plain bulk 46.5 (p99 124) + 1.8 ms with 2120 ops, speculative
bulk 20.2 (p99 41) + 70.7 ms (p99 338 ms) with `F` empty (every victim
reused) and the worker at 32 ms per block, speculative incremental 44.5
(p99 50) + 76.6 ms over 8 steps with `F` empty, plain incremental 51.1 +
3.0 ms. Hints cost auth_wal 0.11–0.35 ms per 80 keys (1.4–4 µs per key:
the order walk and the entry clones under the policy lock), 3–10 ms per
block, below the baseline's warm reads.

Reading. (1) The prediction side works as §9 promised: `F` carries 0–52 ops
instead of 330–2120, 12 % of the sent entries are put back under the bulk
rule and almost none under the incremental rule once lru-write excludes
every hinted key (fifo's only `F` ops are stale rewrites of evicted keys),
the incremental rule removes the eviction p99 (115 → 18 ms), and the kept
marks carry over. (2) Propose does not fall in proportion: at 2400-op
blocks the set's own work (§12.13, about 6 µs per op) and the two chained
engine proposals left on the path, the finishing step and `F` on top of it,
dominate, so the best case is 12.7 against 14.4 ms plain (bulk) and 17.5
ms plain (incremental). (3) The extra engine commit cancels the gain: a
chain's commit costs 14–24 ms whatever its length, against 1.8–3.4 ms for
one proposal, because the second commit waits for the first one's
persistence (R15's one-commit-per-step, one disk wait per block); the best
speculative block therefore costs about 31 ms of propose plus commit
against 16 ms plain under the bulk rule and 21 ms under the incremental
one, so with firewood as it is speculation loses on total latency and wins
only on the propose side (mean and p99). The gating work is
firewood-side: the cascade or squash commit of §12.10 (a)/(b), and on our
side folding the tail into `F` (one proposal fewer per block); the
milestone-3 sweep should run on that. (4) Hint latency is fine.

Latency breakdown (2026-10-30, clarification 31, R33). On the milestone-2
report the owner set the terms: `Propose` computes the new root and is the
critical path; `Commit` marks the proposal committed and the engine persists
asynchronously, so commit latency is not on the critical path; computing a
Merkle root cold is still a problem, so warm-up goes to the TODO; and
where the time goes is to be measured, not guessed. Done: `src/timings.rs`
(`ProposeTimings`, `CommitTimings`), `Snapshot::apply` and `propose`/
`commit` timed phase by phase (§9.5), the worker counting tail steps and
step sizes, the benchmark's `--breakdown` (§11); battery green, 31 tests.
Also corrected: the milestone-2 runs' trie holds 2.4M rows (`rows() =
1000 × 2400`), not 1M.

Numbers (same setup as milestone 2: firewood prefilled with 2.4M rows,
set 10k, K = 8, blocks of 2400 draws hinted as 30 × 80 with 1–5 ms gaps,
the union proposed; `zipf` 786 distinct ops per block with 329 (incremental
rule) or 366 (bulk, 5000 every 14th block) victims, 200 measured blocks;
`tenkrandom` 2400 ops with 2101–2120 victims, 250 blocks; speculation
rounds 3, min_step 100; mean µs per block, p99 in brackets). `zipf`: bare
firewood 7113 (8681). Plain bulk: copy 2617, ops 3297, evict 1673 (17864:
the eviction blocks), digest 794, storage 6019 (87802: 5000 cold puts),
total 14505 (114064). Plain incremental: copy 4388, ops 3445, evict 1613,
digest 1021, storage 7221 (329 cold puts, 22 µs each), total 17771
(20328). Speculative incremental: copy 3695, ops 3385, evict 1615, digest
1130, finish 2741 (the tail step: 2612 µs for 111 ops), storage 34 (`F`
empty), total 12628 (15186); fifo the same with a 298 µs `F` of 16.7
stale rewrites. Speculative bulk: set work 8438, finish 806 (31531), total
9280 (62235), a chain on 7 % of the blocks. `tenkrandom`: bare firewood
15875 (18549). Plain bulk: copy 2092, ops 7438, evict 7534, digest 709,
storage 28672 (2120 cold ops, 13.5 µs each), total 46888 (123984). Plain
incremental: set work 19435, storage 31611, total 51449 (59428).
Speculative incremental: set work 19051, finish 28225 (tail step 27666
µs, about 2000 ops), storage 130, total 47435 (52314). Speculative bulk:
set work 19231, finish 992, total 20285 (41495). Commit: plain, engine
73–444 µs and advance 1.7–3.4 ms (the retired snapshot's drop);
speculative, engine 13.6–75 ms (p99 up to 244 ms) and advance 1.7–2.8 ms.
Micro-benchmarks (`benches/lattice.rs`, hot caches): one lattice item
1.12 µs; clone of 10k entries plus the policy state with 2k writes applied
1.98 ms (fifo), 2.18 ms (lru-write).

Reading. (1) The set's own work is the first problem, not the engine: 8.4–
10.5 ms per block on `zipf` and 17.8–19.4 ms on `tenkrandom`, the same in
every configuration, which is more than bare firewood's whole propose.
Per op, `ops` costs 4.2 µs for an update (two lattice items, 2.25 µs, plus
the map) and 3.1 µs for an insert; the eviction 4.9 (`zipf`) to 3.5 µs
(`tenkrandom`) per victim including the LRU order updates of the written
keys; the copy 2.1–4.4 ms, more at a constant 9500 residents (incremental
rule) than oscillating between 5000 and 10000 (bulk), and 2–4× the
micro-benchmark's hot-cache clone; the digest 0.7–1.1 ms. Milestone 1's
targets (§12.13: 0.5 µs per write, 2 ms per copy) are missed by 6–8× per
op. (2) The engine side of the critical path is cold-path puts: the
victims are the least recently written keys, so their paths are not in
the node cache; 17–22 µs per put on `zipf` and 13.5–15 µs on `tenkrandom`
against 9 and 6.6 µs per hinted op in the baseline, whose hints warm the
paths. Speculation takes them off the path except the tail step, the
predictions after the last eager step, hashed inside `finish`: 2.7 ms on
`zipf` but 28 ms on `tenkrandom`, where rounds 3 / min_step 100 fire both
eager steps at the second hint and leave 28 hints' predictions to the
tail. An empty `F` is free (6–130 µs). (3) The chain commit cost is
firewood's persistence backpressure: its `commit` is asynchronous but
`deferred_persistence_commit_count` defaults to 1, so each further commit
of a chain waits for the previous revision's persistence (15.8 ms for 4
commits on `zipf`, 75 ms on `tenkrandom`); off the critical path under
R33, and a configuration knob rather than the cascade/squash change of
§12.10. (4) Two earlier readings are corrected: milestone 2's "(2) the
set's work and the two chained engine proposals dominate" was half right,
the two proposals being the tail step and a free `F`, and its "(3) the
extra engine commit cancels the gain" is moot under R33; clarification
28's "auth_wal ahead of bare firewood by a third at 1M rows" held in the
no-gap setting, where firewood's persistence of one block contends with
the next block's propose (13.7 ms); with execution gaps and hints, bare
firewood proposes in 7.1 ms and the plain set in 14.5–17.8 ms. (5) Next,
in order of size: the set's own work (§12.16 levers: parallel lattice
accumulation, incremental digest, a cheaper copy, cheaper LRU updates),
then the tail step (steps spread over the block, the follow-up below) and
warming ahead of the final proposal (TODO).

Follow-up (2026-10-30, same day): the copy and the eviction split
(`copy_entries`/`copy_policy`, `writes`/`remove`), and the incremental rule
with steps spread over the block. `zipf`, plain: copy_entries 1760,
copy_policy 1791, ops 3713, writes 586, remove 1039, digest 970, storage
6198, total 16127 (20008; run-to-run variance about 1 ms against the
17771 above). Speculative, rounds 3 / min_step 100: finish 2485 (tail
2364 µs, 111 ops), total 11730 (14000). Rounds 8 / min_step 1: finish 5709
(tail 5519 µs), total 16248 (18249): with min_step 1 a step is built per
hint, so the seven eager steps are spent on the first seven hints (11 ops
each) and the other 23 hints' 253 ops pile into the tail. Rounds 30 /
min_step 1: finish 664 (tail 564 µs, 11 ops), storage 20, total 10249
(11852), the set's own work 9530 µs (93 %); 30 steps per block of 562 µs
each on the worker (16.9 ms per block off the path); engine commit 16070
(35124). `tenkrandom`, plain: copy 1278 + 1390, ops 8148, writes 1637,
remove 6217 (2101 victims, 3.0 µs each), digest 933, storage 31641, total
51570 (62248). Rounds 3 / min_step 100: finish 28501, total 49299 (53169).
Rounds 8 / min_step 1: finish 24242 (tail 23734 µs, about 1600 ops), total
45629 (51406). Rounds 30 / min_step 1: finish 72, storage 14, total 20726
(25468), the set's own work 20615 µs (99.5 %); 26.8 steps per block (the
worker merges two hints when a 2 ms step outlasts the gap) of 2066 µs,
one tail step in 250 blocks; engine commit 68299 (100439).

Reading. (1) The engine side of the critical path is solved by the knobs
alone: with a step per hint the tail is one hint's worth or nothing, and
`finish` plus `F` cost 0.1–0.7 ms. Propose is then the set's own work:
10.2 ms on `zipf` against bare firewood's 7.1, 20.7 ms on `tenkrandom`
against 15.9, every phase of it ours: the two copies (1.3–1.8 ms each; the
policy's as large as the entries'), the ops (3.7 / 8.4 ms), `writes`
(0.6 / 2.0 ms), `remove` (1.0 / 6.5 ms, 3.0–3.2 µs per victim), the digest
(1.0 ms). (2) The price is on the worker and in the engine: 17–55 ms of
step hashing per block off the path (small steps carry a fixed cost: 562
µs for 11 ops), an engine window of `K × 31`, and a chain commit of 16–68
ms of persistence backpressure, off the critical path under R33. The
alternative without a chain, warming the victims' paths and proposing once
(TODO), would trade the 0.1–0.7 ms for a warm `F` of 329–2101 ops; to
measure. (3) The milestone-3 sweep's question is settled for `rounds` and
`min_step` (cover the block's hints; the defaults of §9.2 stay provisional
until the sweep confirms it on other block shapes); what remains is the set
itself (§12.16 levers), the only part of propose that bare firewood does
not pay.

**Clarification 32 (2026-11-01, R34–R36): whole-set rehash, the
no-eviction block, the set's work dug into.** Setup as the breakdown
follow-up (firewood, 2.4M rows, set 10k, incremental rule, hinted blocks
of 2400 draws, rounds 30 / min_step 1), `lthash` against `rehash` in the
same session, mean µs (p99) per block.

`zipf` (786 ops: 456 rewrites, 330 inserts; 329 victims). `lthash`: copy
1669 + 1761, ops 3778, writes 614, remove 1038, digest 954, vc 14, finish
663, storage 22, total 10523 (13032); set work 9828. `rehash`: copy 1709 +
1760, ops 277, writes 655, remove 139, digest 1001, vc 659, finish 700,
storage 27, total 6938 (9041); set work 6200. Bare firewood: 7113 (8681).
`tenkrandom` (2400 ops: 299 rewrites, 2101 inserts; 2101 victims).
`lthash`: copy 2058 + 1872, ops 8279, writes 2055, remove 6201, digest 936,
vc 13, finish 60, total 21495 (24424); set work 21414. `rehash`: copy 1966
+ 1986, ops 769, writes 2087, remove 501, digest 935, vc 747, finish 57,
total 9071 (10665); set work 8991. Bare firewood: 15.9 ms. Chain commits
17.6–20.3 ms (`zipf`) and 69.9–71.3 ms (`tenkrandom`) in both, the
backpressure of the breakdown follow-up.

Experiments on the copies. Memory engine (no firewood threads), `zipf`
plain incremental: copy 2526 + 2356, ops 3820, writes 701, remove 1112,
digest 1050; `advance` 8785 (the engine's revision drop included). glibc
with trimming and mmap thresholds off (`MALLOC_TRIM_THRESHOLD_`,
`MALLOC_MMAP_THRESHOLD_`, `MALLOC_TOP_PAD_`), `zipf` rounds 30: copy 1686 +
1389, total 10003 (11951). Pinned to one socket and its memory (`numactl
--cpunodebind=0 --membind=0`), `zipf` rounds 30: copy 1668 + 1451, total
9993 (11133). Micro-benchmarks (scratchpad crate `micro`, bins `flat`,
`wake`, `churn`; "cold" after writing 128 MB): a 10k-entry `BTreeMap<K,
Vec<u8>>` clones in 447 µs hot / 1147 cold in a fresh heap and 814 / 2771
in a heap after 300 blocks of the in situ pattern (982 / 3156 for the
churned map itself); the policy's two maps 195 / 340 in the fresh heap,
367 / 1549 in the churned heap (531 / 2133 for the churned maps); dropping
a clone 55 / 323 fresh, 2531–2840 cold in the churned heap; the same set
as `Vec<(K, [u8; 32])>` 38–39 / 133–146 in either heap; a clone after a 3
ms sleep 457 µs min / 605 mean against 445 / 547 back to back. Flat merge
passes (cold): the entries with the block's ops and victims 245 µs
(`zipf`) and 292 (`tenkrandom`) against B-tree clone + ops + removals 1527
and 2142; the policy's order and index 531 and 1065 against the B-tree
pair's clone + writes 846 and 1874; 1000 point reads 205 µs B-tree / 458
binary search, cold. Whole-set hashes (770 KB): 30k small `update` calls
1630 µs, serialise + one `update` 359 / 451, the hash alone 241 / 274,
`update_rayon` 107 / 246; the policy order (400 KB) as 20k small updates
863 / 893, as one buffer 126 / 150.

Reading. (1) The owner's intuition holds at 10k: a lattice item costs
2.7–2.8 µs in situ and a block has 1570–4800 of them, the rehash 0.66–0.75
ms; `rehash` cuts propose by a third on `zipf` and by more than half on
`tenkrandom`, to 6.9 and 9.1 ms, at or under bare firewood's. The map work
behind the lattice was small all along (0.3–0.4 µs per op and per victim),
so §12.16's per-op split is corrected in §12.17. (2) The copies are now
the largest term, 3.5–4.0 ms, and the micro-benchmarks say why: a B-tree
clone in a fragmented heap is one cache miss per node and per value,
source and target, which no engine, allocator setting, wake-up or socket
pinning explains away, while a contiguous array copies in 0.14 ms cold in
the same heap. That is the case for the flat snapshot (§12.17 (a)),
projected at 1.8–2.4 ms of set work. (3) The policy digest as one buffer
is a ten-line 0.85 ms (§12.17 (b)); the incremental digest the TODO
carried would have cost more. (4) R35 is decided and designed (§9.1,
§12.17 (c)); the benchmark cannot exercise it until its hints can
over-predict. Nothing in (2)–(4) is implemented (R36). Battery green, 35
tests, the `rehash` suites and the fuzz property over `rehash` included.

**Clarification 33 (2026-11-01, R37): the proposals applied one step at a
time.** Setup as above (firewood, 2.4M rows, set 10k, incremental rule,
hinted blocks of 2400 draws, rounds 30 / min_step 1, `rehash`), one
benchmark pair per step, mean µs (p99) per block; step 0 is the `rehash`
run of clarification 32.

| step | zipf propose | tenkrandom propose |
|---|---|---|
| 0: rehash, clarification 32 | 6938 (9041) | 9071 (10665) |
| 1: policy digest from one buffer | 5238 (6923) | 7488 (9588) |
| 2: flat entries with the value arena | 3496 (4339) | 4626 (5477) |
| 3: flat policy arrays | 2249 (2612) | 3147 (3432) |
| 4: rehash on rayon's pool (reverted) | 2609 (3489) | 3432 (3851) |
| 5: R35 (same path as step 3, a control run) | 2599 (3100) | 3181 (3583) |
| bare firewood | 7113 (8681) | about 15900 |

Phases, `zipf` (copy_entries + copy_policy, ops, writes, remove or merge,
digest, vc, total): step 0: 1709 + 1760, 277, 655, 139, 1001, 659, 6938;
step 1: 1365 + 1411, 244, 593, 118, 279, 526, 5238; step 2: 0.7 + 1074,
188, 547, 276, 297, 444, 3496 (`advance` at commit 659, from 2200–2900);
step 3: 0.5 + 147, 124, 516, 230, 240, 402, 2249 (`advance` 66); step 4:
0.5 + 174, 140, 594, 267, 264, 539, 2609. `tenkrandom`: step 0: 1966 +
1986, 769, 2087, 501, 935, 747, 9071; step 1: 1468 + 1679, 711, 2017, 502,
351, 670, 7488; step 2: 1.4 + 899, 532, 1801, 626, 270, 394, 4626
(`advance` 978); step 3: 0.8 + 276, 408, 1301, 472, 232, 373, 3147
(`advance` 258); step 4: 0.8 + 288, 443, 1318, 484, 252, 510, 3432.

Step 5, R35, in its own setting: the bulk rule (`--low-watermark 9500`,
trigger the capacity, so about every other block evicts) with
`--hint-noise 1.0` (one emulated read per written key, drawn from the
rows, so the hints predict an eviction on every block). Before R35: `zipf`
total 16364 (24990) with reads 7473, storage 5740, finish 255, 2553
storage ops per block (nearly all reverts of predictions that did not
happen), 21.1 steps, commit 158728 (245113); `tenkrandom` 16761 (22409),
reads 6088, storage 5461, finish 822, 2393 storage ops, 17.5 steps,
commit 189718 (290626). After R35: `zipf` 8953 (17864), reads
3504, storage 2815, finish 27, 1195 storage
ops per block, 21.6 steps, 100 of 200 blocks kept the chain,
commit 85408 (215540); `tenkrandom` 17158 (22822), reads 6168,
storage 5455, finish 631, 2393 storage ops,
17.3 steps, 0 of 250 blocks kept the chain, commit
191412 (350042).

Reading. (1) Steps 1–3 took the set's work from 6.2 / 9.0 ms to 2.2–2.6 /
3.1–3.2 ms (three runs of the final path: steps 3, 4 and 5) and propose to
about a third of bare firewood's on `zipf` and a fifth on `tenkrandom`; the copies went from 3.5–4.0 ms to 0.15–0.3, `ops` plus
`merge` is 0.35–0.9 ms, and the retired snapshot's drop at commit from
2.2–2.9 ms to 0.07–0.26. What is left, by size: the policy's `writes`
(0.5 ms on `zipf`, 1.3 on `tenkrandom`: the sort and dedup of the written
keys, their binary searches, the two merges and the marks' reconciliation),
the rehash (0.4 ms), the policy digest and the merge (0.25 each). (2) The
run-to-run spread of the copy phases (1.4–1.8 ms for the same code in
steps 0 and 1) is as large as a small step's gain; the step deltas in the
phases the step touched are clean, the totals carry that noise. (3) Step 4
lost: `update_rayon` queues behind firewood's own pool work, 0.54 ms
against 0.40; the rehash stays single-threaded and the remaining lever is
to hash without serialising, which the arena layout does not give
(keys and values are not contiguous as items). (4) R35 halved the mean propose under the bulk rule with noisy hints on `zipf`, 16.4 to 9.0 ms (the 100 non-evicting blocks of 200 kept the chain and paid the set's work alone, about 2.5 ms, while the evicting half still pays about 15 ms of read-backs and reverts for the over-prediction itself, which only firewood-side changes remove, §12.10 (e)–(f)), and left `tenkrandom` unchanged, where every block evicts under either rule; it is
exercised only when hints over-predict, which the benchmark's exact hints
never did before `--hint-noise`. The chain's length across kept blocks is
uncapped (§9.1, `docs/TODO.md`). Battery green, 36 tests, the R35 test
included.

**Review of 2026-11-02 (clarification 34, R38, v32).** After clarification
33 the owner judged the code "probably a bit too complicated" and asked
for subagent reviews of two kinds, one for designs that could be
simplified and leftovers of experiments never reverted, one general,
repeated until the reviewers have no further comments. Round 1: four
reviewers (simplification of the core; simplification of the benchmark,
the dependencies and the stubs; general review of `lib`/`set`/`policy`/
`speculate`; general review of `storage`/`vc`/`root`/`proposal`/tests),
read-only, with reproductions in scratch copies.

*Correctness (two reviewers, independently, confirmed by executed
reproductions).* R35's chain handover was unsound: the parent stayed the
target after a child kept the chain, and `retarget` adopted the chain
(`take_marks`) at any node sharing the target's storage view, which every
non-evicting child of one parent does. A sibling then inherited marks
whose chain entries were another node's set: (a) under FIFO a rival that
evicted nothing re-sent its rewrite of a marked key (`refresh`) into the
shared chain, and the sibling that won the block omitted that key from
`F` as "already in storage", so storage held the rival's value and the
roots diverged; (b) a hinted rival that stayed alive lent its marks,
including its own inserts, to the committed sibling, whose next commit hit
the `unreachable!` of a marked key the set lacks; (c) a straggler hint on
the parent after a non-evicting child marked a key the child had
rewritten, with the parent's entry on the chain. All three are the §9.3
failure class (local hint timing deciding a root). Fix: the child that
keeps the chain becomes the target at `Propose`; siblings are off the
target (plain path) and their commit resets; stragglers on the parent are
ignored; `retarget` never adopts (§9.1, §9.2). `take_marks` and
`end_block` are gone from the trait, the snapshot and the policies. Also
confirmed: the chain's length across kept blocks was unbounded (the eager
budget was per block), so the engine window `K × (1 + rounds)` no longer
held and on the memory engine the previous head went stale after the
evicting block's commit; the budget is per chain again, the tail reserved
(§9.2 "Bounds"), and a test holds a 15-block kept chain to 3 steps. A
policy whose watermark exceeds the capacity panicked in `Propose` on the
first full block; `EvictionPolicy::validate(capacity)` runs at `New`
(`InvalidConfig`; the trigger's assert moved there too). The merge pass's
"victim the set lacks" check fired only for a victim above every key; it
now fires for any missing or repeated victim. Steps the engine refuses as
`ProposalOrphaned` on a dead line (a straggler's ops on the old head as a
competing child is committed) count `ops_dropped`, not `step_failures`;
the differential test with competing candidates exposed three such per
run. Low: `u32` length prefixes are checked (`len_prefix`); the memory
engine's latency arithmetic saturates. Tests added: the three
reproductions, the window test, competing candidates / stragglers /
pipelined hints in the differential test, the kept chain on firewood, an
incremental-against-fresh commitment oracle over 300 random blocks for
lthash and merkle (the roots had only ever been compared between two
instances of the same `apply`), the policy-contract halts, empty values
and implicitly committed ancestors in the engine contract tests, and the
two `InvalidConfig` cases. 44 tests.

*Simplification.* Removed: `benches/lattice.rs` with `criterion` and
`rayon` (47 packages off every test build; 60 % of it measured structures
since replaced, the figures are in §4, §7.2, §10, §12.13), the lattice's
bench-only API (`add`/`remove`/`merge`/`encode_item` and the const generic
with one instantiation; the Merkle leaf hashes the fields directly), the
`ffi` stub module, feature and `staticlib` (a 33 MB archive per build for
a shim that does not exist; back with milestone 4), `Node::metadata_hash`
(dead behind `allow(dead_code)`), `ApplyTimings` (a prefix copy of
`ProposeTimings`; `apply` fills the latter and the two copy phases are
one), the `closing` atomic on `Shared` (a writer-only `bool`), the
worker's `C, P` type parameters (it holds an `Arc<Counters>`), the
arena's 64 KiB compaction floor (compaction is now exercised by the test
battery; a 64-chunk cap keeps a child's chunk list short for small
blocks), the victims' second sort and clone (`writes` returns them sorted
by key, the storage batch's order), the `BTreeSet` in `final_proposal`
(a binary search), the stale milestone comments and the measurement
numbers in comments (replaced by § references), and the narrative doc
comments on `propose`, `commit`, `new`, `prefetch`, `retarget` and
`Snapshot::apply` (contracts plus § pointers). Shared: `item_fields` in
`vc`, `params_digest` in `policy`, `entry_op` in `speculate`,
`BatchOp::key`. Benchmark: `Report` holds `&GlobalOpts` and one
`(name, value)` column list serves the CSV header and record (they had
to be kept in step by eye); policy and scheme names come from clap;
`GlobalOpts::watermark()`/`trigger()` replace two copies of the R16
default; the `TestRunner` trait and four unit structs are `TestName`
methods over one `run_loop` (`workload.rs`) called from `workloads.rs`
(`--warmup-batches` now applies to all three update workloads); `TargetStats` is `auth_wal::Stats`;
the worker line prints the dropped steps and ops, the step failures, the
fallbacks and the panics, and the CSV gained `step_failures` and
`fallbacks` (`log` has no sink in the benchmark, so a run whose steps the
engine refused looked like a success). Listed, not done (requirement-bound
or the owner's call): the ~110 lines fifo and lru-write share (R26 forbids
shared watermark code), the marks living in the policy (R25), `Key:
Borrow<[u8]>` for the memory model's range scan, `Prediction::refresh`/
`withdraw` collapsing to one list (§9.1 chose the per-policy handling),
`Value::from_bytes` panicking on a length mismatch (`docs/TODO.md`).

*Measurement* (the incremental-rule pair of clarification 33, rehash,
rounds 30 / min_step 1, 2.4M rows; mean µs (p99) per block): `zipf`
2514 (3145) against the step-5 control's 2599 (3100), `tenkrandom` 2885
(3438) against 3181 (3583): within the run-to-run spread, a little lower
(one `Instant` pair and the victims' second sort and clone gone). Phases
`zipf` / `tenkrandom`: copy 163 / 197, ops 128 / 307, writes 567 / 1263,
merge 235 / 430, digest 246 / 233, vc 430 / 381, finish 710 / 54; 30.0 and
26.8 steps per block, 0 steps and 0 ops dropped, 0 step failures, 0
fallbacks.

Battery green (fmt, clippy `-D warnings` on all targets and features,
rustdoc `-D warnings`, 44 tests with all features, the firewood contract
tests). Round 2 below.

**Review of 2026-11-02, round 2 (R38, v33).** Five fresh reviewers: the
four scopes of round 1 and a docs-versus-code pass. Verdicts: the tree is
in good shape; two correctness findings, both confirmed by reproductions;
small duplication left; a few doc words that outlived the code.

*Correctness.* (1) Round 1's per-chain step budget opened a hole two
reviewers found independently: once a tail had filled the chain
(`steps == rounds`), a second `finish` on the same chain (a competing
candidate, a straggler hint then a sibling, a kept sibling after an
evicting one) found no room, built nothing, and still answered the
latest view while the policy's marks named keys whose ops sat unbuilt;
`F` then omitted them as "already in storage" and the root diverged. A
seeded differential run failed in 16 of 144 configurations at `rounds`
1–2 (it never fills a chain at the tests' `rounds` 4, which is why round
1's tests passed). Fix: `finish` answers nothing while anything submitted
waits unbuilt (`Chain::answer`), so that block takes the plain path; a
full chain with nothing waiting still answers a second child. Tests: the
two reproductions (`two_children_of_the_target_either_committed` at
`rounds` 1, `a_straggler_after_the_tail_does_not_poison_a_second_child`)
and the differential test at `rounds` 1 and 2. (2) The firewood adapter's
first link on a committed view had no parent link, and a firewood
proposal names its base revision by hash and reads its nodes by address;
once the base and its successor left the window, the reaper freed the
nodes, later commits reused the addresses, and a long-held orphan handle
(a rejected block the adapter may still read, §5) returned other
revisions' values, 400 of 400 wrong reads in the reproduction, no error.
Fix: the first link holds the committed handle as its parent, whose view
pins the base revision for as long as the link lives (firewood's reaper
stops at a referenced revision, so the window stalls instead, §10 (b),
(c)); `commit`'s chain walk stops at the committed parent as before. The
memory model's commit compared the chain's base with the head by `Arc`
identity where firewood compares by root, so a chain built before a
same-root (no-op) commit committed on firewood and failed on the model;
round 2 made the model compare roots, which round 3 reversed (below:
firewood's acceptance is the defect). Contract tests for both: a live
proposal on a reaped base (a two-revision window, no node cache on
firewood), a commit after a same-root commit. Also: known answers pinned for the four
consensus encodings (a lattice item as a value and as a tombstone, a
rehash digest, a Merkle root, a `Terms::combine` root), since every other
test compared two in-tree computations and a byte change would have
passed; a recursive sparse-Merkle oracle over the final entries for the
incremental Merkle update's shape; the arena's `u32` offsets and lengths
checked (a block or set over 4 GiB of values is refused rather than
aliased); `fw_err` maps every firewood error to `Storage`
(`RevisionNotFound` is the registry's word).

*Simplification.* `spec_ops` builds the ops of a prediction for the kept
chain, the hints and the retarget (three copies before; `Worker::submit`
ignores an empty batch); `put_back` reads a key back for a step's
retraction and for `F` (TODO item 6's two places are one); the worker's
chain is a `Chain` with `drop_chain` and `answer` (the two "chain goes"
blocks were one); `speculative_view` holds the speculative branch of
`propose`; `normalise` holds the batch half of `Snapshot::apply`;
`validate_watermarks` sits with the other shared v0 helpers in
`policy/mod.rs`. Tests: one `Rng`, `k`/`put`/`del`/`keys` and a
`TempDir` guard shared by `tests.rs`, `set.rs`, `merkle.rs` and the
contract tests; the kept-chain test is one body run on the memory engine
and on firewood; the obscured oracle of `evictions_reach_storage` is a
list. Benchmark: the workload label comes from `TestName::name`, the
one-user `Clock` is inlined into `run_loop`, the worker statistics live
once on `Measurements` (the breakdown renders them), one `micros`, the
redundant `[lib]`/`[[bin]]` manifest blocks are gone, unused derives
dropped, `tenkrandom` is the subcommand's name with `ten-k-random` kept
as an alias. Docs: the stale words the consistency pass listed (the node
keeping its `metadata_hash`, the `New` row and §6.2 on `validate`, the
removed `cargo bench`, `Close`'s "nothing more is built", the §8 Propose
bullet and the writer-owned list, the README's status lines) and a
clarification-number sweep in doc comments (R-rows and § in their place).
Left, the owner's call: a pipeline deeper than one live child resets the
chain at the intermediate commit (performance only; `after_commit` and
`prefetch` test the direct parent); `chains_kept` counts a kept block
whose chain was already dead; the items of round 1.

*Measurement* (the same pair): `zipf` 2523 (3111) against round
1's 2514 (3145) and the step-5 control's 2599 (3100), `tenkrandom` 2922
(3301) against 2885 (3438) and 3181 (3583): unchanged within the spread.
Phases `zipf` / `tenkrandom`: copy 178 / 241, ops 155 / 322, writes 565 /
1222, merge 265 / 421, digest 257 / 235, vc 443 / 388, finish 637 / 65;
30.0 and 26.6 steps per block, nothing dropped, no step failure, no
fallback.

Battery green, 48 tests with all features. Round 3 below.

**Review of 2026-11-02, round 3 (R38, v34).** Five fresh reviewers again
(interrupted once by the session's limit and resumed). Verdicts: the lib,
set, policy and worker reviewer, after tracing every ordering of the
brief and running its own seeded, fault-injected and pipelined
differentials (216 and 480 configurations, all green), reports no further
correctness comments; the simplification reviewers report the tree in
good shape with small items; the docs pass lists wording; the storage
reviewer found one real defect.

*Correctness.* Round 2 had made the memory model compare a chain's base
with the head by root "as firewood does". The storage reviewer showed, by
reproduction on firewood, that firewood's acceptance is the defect: every
proposal lists its base's root node as deleted, and the engine's commit
check compares parent hashes, so a chain built on revision B and
committed after a same-root no-op revision B' makes B' and the chain's
first link both free B's root node; later allocations reuse it and
overwrite a live node (37 wrong reads of 40 in the reproduction; the
control, the chain committed before the no-op, read right). auth_wal's
writer never commits such a chain (`after_commit` resets the chain after
any other commit, and a lost line fails `ParentUncommitted`), so
production was safe by the writer's flow while the contract, the model
and the adapter promised the corrupting behaviour. Fix: both engines
compare revisions by identity, never by root (§10 items 3 and 4): the
firewood adapter checks that a committed handle's pinned revision is the
`Arc` the engine holds under the current root (`is_current`) before and
after a `propose` on it, and `commit` checks the chain's base the same
way before its first engine commit, failing the oldest link (consumed,
item 1; its children stay live); the memory model is back to `Arc`
identity in both places. The contract test asserts the refusal on both
backends, with the child still readable and a fresh `head` proposing.
Also: the lattice known answer now goes through `LtHash`'s own `empty`,
`apply` and `commitment` (it had bypassed the scheme and its private
domain string, so a changed domain passed), and the memory engine's root
is pinned too (`ENGINE_ID` and the storage root enter auth_wal's root on
that engine); `in_place_updates` counts a present key the block rewrites
and then evicts (it was skipped by the victim branch; the benchmark's
hit rate only). Tests added from the reviewer's scratch work: a kept
child proposed on a full chain (both policies, both rules, chains of one
to three steps, with a hint on it and a second kept child), and a
differential against the plain reference under an engine that refuses
every n-th step proposal, retraction read and final proposal, with the
next block pipelined on the live candidate half the time (every injected
failure counted as `step_failures` or `fallbacks`, roots equal at every
block).

*Simplification.* The merge pass is a function (`merge`) over a named
`Op` (the crate's one `allow(clippy::type_complexity)` is gone); the
`split_order` fallback that re-sorted a run out of height order is an
assert (the order is consensus state; `apply` always runs at `parent.height
+ 1`); `propose`'s three-way view choice is one expression;
`speculative_view` takes `&self`; the `run` and `apply` doc comments and
the policies' `writes` docs keep their contracts and lose their
walk-throughs; "link" is "step" where it meant one. Tests share `fill`,
`agree` and `assert_no_failures`; the obscured oracle of
`evictions_reach_storage` is a list. Benchmark: `Stats::since` and
`SpeculationStats::since` (the counters' growth since warm-up) replace a
renamed mirror of the speculation statistics, the worker line prints the
retractions, `Report` holds the arguments and derives the workload and
target names, `Target` no longer names itself, the engine window comes
from each engine's `COMMITS_CHAIN_LINKS_SEPARATELY`, four help strings
say what the code does at any batch size, the policy and scheme flags
have doc lines, the README's example uses the subcommand's name. Docs:
the seventeen wording items of the consistency pass (the writer's
ownership list and the policy lock's takers in §10, `F`'s creation rule
in §9.1, `steps_dropped`/`ops_dropped`/`hint_keys` in §9.5, the trigger
clamp in §6.2, the layout lines, the README's lock sentence and status,
the TODO's restore-op places and the bulk-default sentence). Left, the
owner's call: a `Chain` whose three states live in two `Option`s; the
`Counters`/`SpeculationStats`/`snapshot` triple listing of eighteen
fields; a one-key-per-path hint (`Marks::hinted_new`) kept for O(hint)
notes; the items of rounds 1 and 2.

*Measurement* (the same pair): «MEASUREMENT»

Battery green, 50 tests with all features. Round 4 pending.


### Review fixes of 2026-11-03

The owner accepted the eight review findings, clarifying that merging must
stay on the dedicated worker (00 clarification 35). Applied:

- Pending steps use a worker-owned ordered map, updated as submissions are
  drained, even with no room for another step. Only the last op per key is
  retained; `submit` still only enqueues. The step threshold counts distinct
  pending keys. Superseded pending ops do not count as dropped work; a reset
  counts the merged pending ops it discards. The asynchronous channel can
  still accumulate a producer backlog while the worker hashes.
- Firewood's engine identifier includes its hashing algorithm
  (`firewood/ethereum` or `firewood/merkledb`). Firewood-backed combined roots
  therefore change from the first non-genesis proposal, even before an
  eviction. The storage trait documentation now requires revision identity
  for committed-handle proposals and chain-base checks on every engine.
- CSV records identify the actual backend and its engine identifier, the
  populated row count, Zipf exponent, cache size and run controls.
- FIFO and LRU-write clone their immutable array references, then construct
  the child's order and index once at `writes`. Previously both arrays were
  fully cloned and immediately rebuilt. Snapshot semantics and digests stay
  the same; only temporary copying is removed.
- A plain instance creates no speculation worker or channel. `Marks` counts
  new hinted keys using the insertion result of its existing hinted-key set,
  removing the second set. `VectorCommitment<K>` no longer has an unused
  value-type parameter; values reach schemes as `EntryRef` bytes.

The memory-growth reproduction (capacity 2, FIFO, one speculation round,
64 rewrites of one 1 MiB value) went from approximately 74 MiB RSS to 11 MiB,
matching the plain control while occupancy remained 2 and no step was built.
A regression test checks that superseded values are released before the
worker can build a step and that a later finish preserves last-op semantics
for both evictions and retractions. A second regression checks that the
Firewood hashing algorithm affects the root before any eviction.

A local release comparison, not the milestone-3 sweep: memory backend,
`rehash`, LRU-write, capacity 10k, 20k prefilled rows, 300 Zipf blocks of
1k ops with 50 warm-up blocks and no hints. Mean `copy` fell from 73.9 to
0.2 µs, `writes` from 455.0 to 360.6 µs, and total propose from 1739.8 to
1502.8 µs. This is one before/after run; the removed array copies account
for the clear phase-level reduction, while total latency includes runtime
variation and storage work.

Validation: all 52 tests passed with `cargo test --release --workspace
--all-features`; the debug suite also passed. The hashing-identifier
regression passed separately without `ethhash`. `cargo clippy --workspace
--all-features --all-targets -- -D warnings`, the workspace check without
default features, formatting and whitespace checks passed.


### Prediction interface simplification

`Prediction` now contains only `evict` and `retract`; `all_evicted_keys.is_empty()`
distinguishes continuing precomputation from finalizing actual eviction. The
former `refresh` and `withdraw` lists and the writer-side `withdraw` callback are
removed. FIFO and LRU-write own their reconciliation and cancel marks themselves.
Historical review entries above describe the older four-list interface.

The shared `Marks` wrapper is removed as well. Hint history remains private to
the policies because it serves prediction independently of reconciliation:
FIFO counts distinct expected insertions, and LRU additionally excludes all
previously hinted writes from later predictions. No prediction strategy or
consensus eviction rule changes in this refactor.


### Proposal finalization refactor (2026-11-03)

Final refinement now travels with the terminal worker request. The worker
merges it with pending hints before at most one proposal, returning the
latest view directly when no operations or metadata remain. This removes
the caller-side final proposal and its duplicate restoration path. The
completed child becomes the next target; siblings cannot reuse its final
corrections. The proposal budget includes finalization, and engine windows
therefore use `K × rounds`. Active §9 describes this protocol; earlier
review and performance entries describe the implementation measured then.


### Fixed prediction windows and persistence configuration (2026-11-03)

Prediction now selects `capacity - low_watermark + margin` entries at its
first trigger. FIFO ignores subsequent hints; LRU fills vacancies with a
forward cursor and records the complete hint batch before selecting any
replacement. The budget survives kept blocks and does not expand for
projected overflow. Final writes supply missing or refreshed actual victims;
restoration remains necessary for LRU false hints and nonzero FIFO margins.
Actual eviction, root encodings, and the worker protocol are unchanged.

Firewood commit permits are exposed through the existing backend option.
The adapter default remains 1; the benchmark selects rounds for speculative
auth_wal and 1 otherwise, supports an override, and reports the resolved
setting. Earlier measurements used one permit unless stated otherwise.

The focused Zipf comparison used one release trial per configuration, each
with a fresh independent seed, 1M prefilled rows, 50 warm-up blocks, and 500
measured blocks. Both used LRU-write, Rehash, capacity 10k, low/high 7k/9k,
30 rounds, and 32 commit permits. Each measured 42 eviction blocks with
no failures or fallbacks, starting above the low watermark.

| Configuration | Seed | Propose mean | Commit mean | Hint time/block | Proposals/eviction |
|---|---:|---:|---:|---:|---:|
| Minimum step 1 | 4845979189037642163 | 2.035 ms | 0.214 ms | 1.462 ms | 30.00 |
| Minimum step 256 | 16021042067665105071 | 1.832 ms | 0.189 ms | 1.456 ms | 5.95 |

The preceding speculative trial, with growing predictions and one permit,
averaged 2.046 ms Propose, 10.650 ms Commit, and 8.932 ms hints/block.
Policy and permit changes were combined, and these are independent single
trials rather than isolated causal estimates. Following this comparison,
the library and benchmark minimum-step defaults are set to 256.
Kept-block flushes can still build below that threshold.

Peak RSS increased from 223 MiB in the preceding speculative trial to about
1,140 MiB in both new trials. It includes prefill and warm-up; the measurements
do not locate the peak or attribute it solely to permits. Investigating that
peak and smaller permit limits with shorter chains remains future work.

Two standalone digest probes preserved the existing byte encodings and used
8,500 entries with 32-byte keys/values, 400 interleaved iterations per variant.
Streaming in 16 KiB chunks was 47–56% slower (seed 91882179538077321).
Reusing whole serialization buffers was effectively tied with fresh
allocations, within 0.3% (seed 16760059880781806585). These hot-cache probes
do not establish whole-app performance, but provide no reason to add scratch
ownership to snapshots or commitment traits. Production hashing is unchanged.

The [full report](../benchmark/results/2026-11-03-fixed-predictions/README.md)
records commands, raw results, phase breakdowns, source/binary hashes, and
probe code. Validation used 28 focused library tests and one benchmark CLI
test, Clippy, formatting, and two agent reviews; the full suite was not run.

### Upstream Firewood v0.9.0 migration (2026-11-03)

The dependency checkout advances from the local fork's
`1e1dc26d3b69f94198d15120712091c30ba90072` to upstream
[`a1282b68eb969e21e19aebf11e367a433896b5f9`](https://github.com/ava-labs/firewood/commit/a1282b68eb969e21e19aebf11e367a433896b5f9),
the `main` head resolved at the start of this experiment. A separate local
branch preserves the previous checkout's `main`; upstream sources are clean.

Compatibility changes use the public `DynDb`/`DynProposal` API and owned
batches, select the hash mode at open, map commit permits to
`max_persistence_gap`, and convert the free-list cache entry limit to KiB.
No speculative scheduling, eviction policy, or commitment encoding changes
are part of the migration. Firewood now checks parent revision identity and
absorbs same-root commits. Adapter generations retain logical commit identity,
while backend-view identity checks reject proposals racing the interval between
backend commit and generation publication. A deterministic regression test
covers that interval; seven focused tests pass in each hash mode.

The comparison repeats the earlier full matrix: Zipf and mixed writes,
bare Firewood/plain auth_wal/speculative auth_wal, three independent seeds
per combination. Each trial prefills 1M rows, warms up for 50 blocks, and
measures 500 blocks. Watermarks remain 70%/90%, minimum step is the new
default 256, the chain budget is 30, and speculation uses 32 permits.
Supplemental RSS sampling distinguishes premeasurement memory from memory
during measurement and close. The initial partial matrix is retained separately
and excluded after the adapter concurrency correction; final comparisons use
one corrected build and new seeds throughout.

All 18 final trials completed, totaling 9,000 measured blocks. Every auth_wal
trial started at or above the low watermark and evicted during measurement;
reported failures, fallbacks, panics, and dropped work were zero. The table
shows medians of three per-trial measurements, in milliseconds.

| Workload | Target | Propose mean | Propose p99 | Commit mean | API time/block |
|---|---|---:|---:|---:|---:|
| Zipf | Bare Firewood | 5.329 | 6.914 | 0.160 | 14.841 |
| Zipf | auth_wal plain | 3.722 | 31.064 | 0.070 | 3.804 |
| Zipf | auth_wal speculative | 1.793 | 6.006 | 0.197 | 3.476 |
| Mixed writes | Bare Firewood | 11.697 | 13.625 | 0.900 | 33.820 |
| Mixed writes | auth_wal plain | 11.741 | 24.197 | 0.741 | 12.660 |
| Mixed writes | auth_wal speculative | 5.250 | 43.000 | 0.606 | 8.179 |

API time includes hints, Propose, and Commit, but excludes artificial execution
gaps and close. Bare Firewood reads hinted paths; speculative auth_wal updates
predictions and submits worker operations, while plain auth_wal does no
speculative hint work. These costs therefore represent different hint behavior.
Commit still measures queue acceptance, including any backpressure, rather
than durable persistence. Independent seeds make these unpaired comparisons.

Speculation improves mean Propose in both workloads, but mixed-write p99
ranges from 31.773 to 54.895 ms across its three trials. Final storage work
averages 2.383 ms/block and worker finalization 0.429 ms/block there; per-block
tracing is needed to attribute the tails. Zipf's remaining policy writes
average 0.492 ms/block and digest plus commitment work 0.662 ms/block.

Speculative peak RSS remains approximately 1,140 MiB, versus 215–217 MiB
for plain auth_wal. Sampling shows most of that footprint already resident
when measurement starts. These measurements do not distinguish live
allocations from allocator retention or isolate the effect of 32 permits.
Memory profiling and a smaller-permit comparison remain future work.

The [full upstream report](../benchmark/results/2026-11-03-latest-firewood/README.md)
includes all seeds, commands, ranges, raw measurements, phase timings, memory
samples, and an archive of the tested sources. Seven focused tests passed in
each hash mode; Clippy, formatting, and artifact verification also passed.

### Clean-shutdown checkpoint recovery

R41 adds an optional snapshot file on `Close` and an explicit `Recover`
constructor (§12.6). Existing new/seeded initialization remains available.
The checkpoint contains the latest committed set and policy state; commitment
state is rebuilt and the saved combined root verified before exposing it.
Only the metadata hash is added to live nodes, leaving commitment encodings
and the proposal/commit protocol unchanged.

Eight focused persistence tests cover both policies and all three schemes,
identical future roots against uninterrupted execution, genesis and populated
sets, metadata, empty values and tombstones, fixed-size byte types, malformed
files, configuration/storage mismatches, and close retries and flush failures.
Firewood round trips pass with both MerkleDB and Ethereum hashing, including
two successive restarts and reads of evicted values. Existing close regressions
also pass. Clippy, formatting, and a build without Firewood were checked;
the full unit suite and performance experiments were not rerun.
