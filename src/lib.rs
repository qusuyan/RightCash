//! `auth_wal`: an authenticated, bounded, in-memory set of recent writes that
//! sits between the EVM validators and an authenticated storage engine.
//!
//! See `docs/01-design.md`. Module map (design §13):
//!
//! | module | design | role |
//! |---|---|---|
//! | [`types`]     | §3, §4 | `Key`/`Value` bounds, `Hash`, `Digest`, `BatchOp`, errors |
//! | [`set`]       | §4     | the snapshot (the whole set at one node) and its batch application |
//! | [`policy`]    | §6     | `EvictionPolicy` trait (eviction and prediction), the v0 policies |
//! | [`vc`]        | §7     | `VectorCommitment` trait and schemes |
//! | [`root`]      | §7.3   | combining the set commitment, the policy digest and the storage root |
//! | [`lattice`]   | §7.2, §10 | lattice-hash accumulator shared by `lthash` and `MemoryStorage` |
//! | [`storage`]   | §10    | `StorageEngine` abstraction and the in-memory model of firewood |
//! | [`proposal`]  | §8     | immutable nodes, the handle, the registry |
//! | [`speculate`] | §9     | the worker: a channel of requests, the thread that builds steps, `submit`/`finish`/`reset` |
//! | `ffi`         | §2     | C ABI consumed by the Go `ffi` package (feature `ffi`) |
//!
//! Threading (design §3, R19): [`AuthWal`] is the **writer**; every operation
//! that changes state takes `&mut self`, so the type system serialises them.
//! [`Reader`] and [`proposal::Handle`] are the `Send + Sync` read side. The
//! shared mutable structures are the registry behind one mutex and a
//! snapshot's policy behind its own; the worker is reached over a channel
//! (design §10 lists every hold).

// Skeleton phase: bodies are `todo!()`, so parameters are unused for now.
// Remove once milestone 1 lands.
#![allow(unused_variables, dead_code)]

pub mod lattice;
pub mod policy;
pub mod proposal;
pub mod root;
pub mod set;
pub mod speculate;
pub mod storage;
pub mod types;
pub mod vc;

#[cfg(feature = "ffi")]
pub mod ffi;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use parking_lot::Mutex;

use crate::policy::EvictionPolicy;
use crate::proposal::{Handle, NodeRef, Registry, WeakNode};
use crate::speculate::{Counters, SpeculationStats, Worker};
use crate::storage::StorageEngine;
use crate::types::{BatchOp, Hash, Key, Result, Value};
use crate::vc::VectorCommitment;

/// Consensus parameters: they enter the root preimage (design §7.3) and must
/// be identical on every validator. Deliberately no `Default`: a network
/// states them explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Consensus {
    /// Maximum number of entries (including tombstones) in the write set
    /// (design §6.2); how much to evict is the policy's rule (R23).
    pub capacity: usize,
}

/// Local parameters: may differ between validators and never reach a root.
#[derive(Clone, Debug)]
pub struct Local {
    /// Committed revisions kept for `revision()` (design §8); at least 2
    /// (`Config::validate`).
    pub revisions: usize,
    /// Speculative eviction (design §9): send predicted victims to the engine
    /// early. Requires `S::ROOT_IS_STATE_FUNCTION`.
    pub speculate: bool,
    /// Maximum steps per precomputation (design §9.2), the one `finish`
    /// builds included; sizes the engine window (`engine_window`).
    pub speculation_rounds: u32,
    /// Minimum queued ops before the worker builds a step ahead of `finish`
    /// (design §9.2).
    pub speculation_min_step: usize,
    /// Extra predicted victims beyond the strict count once a hint's
    /// insertions carry the set over capacity (design §9.2, R15: pessimistic
    /// prediction); handed to the policy's `hint_writes`.
    pub speculation_margin: usize,
}

impl Default for Local {
    fn default() -> Self {
        // Rounds and min_step are provisional until the milestone-3 sweep (design §9.2).
        Self {
            revisions: 128,
            speculate: false,
            speculation_rounds: 3,
            speculation_min_step: 1_000,
            speculation_margin: 0,
        }
    }
}

impl Local {
    /// The engine window these settings need (design §8): `revisions * (1 +
    /// rounds)` when speculating on an engine that commits chained proposals
    /// separately (`S::COMMITS_CHAIN_LINKS_SEPARATELY`), else `revisions`.
    /// One formula for `AuthWal::new`'s check, the benchmark's engine config
    /// and the Go `New`.
    pub fn engine_window(&self, commits_chain_links_separately: bool) -> usize {
        if self.speculate && commits_chain_links_separately {
            self.revisions * (1 + self.speculation_rounds as usize)
        } else {
            self.revisions
        }
    }
}

/// Configuration of an [`AuthWal`] instance (design §3). The split lets the
/// type system keep local settings out of the root preimage, whose terms
/// come from the scheme, the policy, the engine and the node (its capacity
/// among them), never from `Local`.
#[derive(Clone, Debug)]
pub struct Config {
    pub consensus: Consensus,
    pub local: Local,
}

impl Config {
    /// `capacity >= 1`, `revisions >= 2`
    /// (the adapter resolves the parent's root right after a commit, design
    /// §8), and when speculating `speculation_rounds >= 1` and
    /// `speculation_min_step >= 1` (0 would permit empty steps). Called first
    /// thing by `AuthWal::new`.
    pub fn validate(&self) -> Result<()> {
        todo!()
    }
}

/// Observable counters for benchmarks and metrics (design §11): the set
/// counters (`SetCounters`, atomics added at `Propose`; a losing candidate
/// counts too) and the speculation counters, plus `height` and `occupancy`,
/// the head's when `stats()` is called.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub height: u64,
    pub occupancy: usize,
    pub total_evictions: u64,
    /// Ops in the nodes' own storage proposals,
    /// `total_evictions.saturating_sub(reused_ops) + reverted_ops` (step ops
    /// are not counted; saturating because the two counter sets are bumped
    /// at different moments, so a concurrent read may briefly see `reused`
    /// ahead); derived in `stats()`.
    pub total_storage_ops: u64,
    /// `plain + speculative`, derived in `stats()`.
    pub storage_proposals: u64,
    /// All `Put` ops applied.
    pub puts: u64,
    /// Puts that replaced an entry already in the set (the layer's win; the
    /// `zipf` benchmark's hit rate is `in_place_updates / puts`).
    pub in_place_updates: u64,
    pub speculation: SpeculationStats,
}

/// Lock-free accumulators behind the set half of [`Stats`] (design §9.5):
/// `propose` adds to them with no lock held, `Reader::stats` loads them.
#[derive(Default)]
pub(crate) struct SetCounters {
    pub(crate) puts: AtomicU64,
    pub(crate) in_place_updates: AtomicU64,
    pub(crate) total_evictions: AtomicU64,
}

/// State shared between the writer, the readers and the worker thread
/// (design §8, §10): the counters (set and speculation) and the closing
/// flag, atomics; the registry behind one mutex. No configuration (the writer owns it),
/// no engine (the writer owns it, R19), no handle count and no closed flag
/// for reads (R20: the Go layer counts its outstanding handles; the engine
/// refuses to close while a live storage proposal is held). Handles hold
/// only their node.
pub(crate) struct Shared<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub(crate) set: SetCounters,
    pub(crate) counters: Counters,
    /// Set by `close`: the writer's operations report `Closed` from then on
    /// and hints are no-ops (reads never check it, R20). The worker needs no
    /// flag: dropping its channel's sender ends it (design §9.2).
    pub(crate) closing: AtomicBool,
    /// The registry (design §8), one of the locks (design §10): held for a
    /// few pointer operations or a scan of at most K roots by the writer
    /// (resolve the parent, register, check at commit, install, resolve at
    /// prefetch) and by readers (resolve the head or a root), never across
    /// an engine call, a snapshot copy or a drop.
    pub(crate) registry: Mutex<Registry<K, V, C, P, S>>,
}

/// The read side of an instance (design §3 threading contract): `Clone +
/// Send + Sync`, for the RPC and mempool threads. Each call takes the
/// registry lock once, for nanoseconds, to resolve the head or a root; the
/// read itself takes no lock (design §10). The writer holds one too.
pub struct Reader<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    shared: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Clone for Reader<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<K, V, C, P, S> Reader<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Reads `key` from the committed state (design §5): one registry hold
    /// clones the head (one `Arc`), then `head.get(key)` with no lock.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("let head = registry.lock().head().clone(); head.get(key)")
    }

    /// The single combined commitment of the committed state; `EMPTY_ROOT`
    /// until the first commit (design §7.3). One registry hold.
    pub fn root(&self) -> Hash {
        todo!("registry.lock().head().root()")
    }

    /// Read-only handle at `root`: one of the last K commits or a live
    /// proposal (design §8), resolved under one registry hold
    /// (`Registry::lookup`: the ring, then live proposals). `RevisionNotFound`
    /// otherwise.
    pub fn revision(&self, root: Hash) -> Result<Handle<K, V, C, P, S>> {
        todo!("registry.lock().lookup(root).map(Handle::new).ok_or(RevisionNotFound)")
    }

    /// A handle to the committed state, one registry hold.
    pub fn latest_revision(&self) -> Handle<K, V, C, P, S> {
        todo!("Handle::new(registry.lock().head().clone())")
    }

    /// One registry hold reads the head's height and occupancy; every
    /// counter is an atomic loaded outside it (`set`, then
    /// `counters.snapshot()`); `storage_proposals` and `total_storage_ops`
    /// are derived from them (design §9.5).
    pub fn stats(&self) -> Stats {
        todo!("hold: height/occupancy from head(); then relaxed loads of set and counters.snapshot()")
    }
}

/// The authenticated write set in front of a storage engine: the **writer**
/// (design §3 threading contract, R19). One per instance, not `Clone`;
/// `propose`, `commit`, `update`, `prefetch`, `prune` and `close` take `&mut
/// self`, so no two of them can overlap and none of the writer's state needs
/// a lock: the storage engine, the worker handle, the target, the
/// configuration and the poisoned flag are plain fields. The read side is
/// [`Reader`] (`reader()`) and the handles.
///
/// Type parameters follow design §4 and §13: `K`/`V` key and value types,
/// `C` the vector commitment scheme, `P` the eviction policy, `S` the storage
/// engine.
pub struct AuthWal<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    // Field order is drop order: the writer's reader (one `Arc<Shared>`; the
    // worker thread and cloned `Reader`s hold their own), then the worker
    // handle (dropping it drops the channel's sender, so a writer dropped
    // without `close` lets the thread exit and drop its chain),
    // then the engine (handles keep what they need alive).
    reader: Reader<K, V, C, P, S>,
    /// The writer's interface to the worker (design §9.2, R17): `submit`,
    /// `finish`, `reset`; spawned by `new` after the shared state exists,
    /// stopped and joined by `close`.
    worker: Worker<K, V, C, P, S>,
    /// The node the precomputation is for (design §9.2 "Target"): the node
    /// the next block builds on, whose snapshot's policy carries the marks.
    /// Weak: the caller's handle owns the node; dead means no target.
    target: WeakNode<K, V, C, P, S>,
    /// Owned by the writer (R19): closed through `&mut self`. The worker and
    /// the readers hold only handles (design §10 item 7).
    storage: S,
    /// Validated at `new`; read by the writer's operations only, so it is
    /// not shared.
    config: Config,
    /// The root preimage's static terms (design §7.3), computed at `new`
    /// from the scheme, the policy and the engine.
    terms: root::Terms,
    /// Set by a failed mid-chain engine commit (design §9.2); `propose` and
    /// `commit` return `Error::Poisoned` from then on.
    poisoned: Option<String>,
}

impl<K, V, C, P, S> AuthWal<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Opens an empty write set in front of `storage` (design §3 New).
    /// Runs `config.validate()` first, requires a fresh engine
    /// (`storage.root() == EMPTY_ROOT`, else `EngineNotFresh`; design §1,
    /// §12.6), rejects `speculate` unless `S::ROOT_IS_STATE_FUNCTION`
    /// (design §9.3, `InvalidConfig`), and checks the engine's window:
    /// `storage.revisions() >=
    /// local.engine_window(S::COMMITS_CHAIN_LINKS_SEPARATELY)` (design §8;
    /// the Go `New` and the benchmark open the engine with that size). The
    /// policy's eviction rule is checked where it acts, not here:
    /// `Snapshot::apply` asserts the set is within capacity after `writes`
    /// (design §6.2). Then computes the root terms (`root::Terms` from
    /// `vc.scheme_id()` and `params_digest()`, `policy.name()` and
    /// `params_digest()` and `S::ENGINE_ID`, design §7.3), builds the genesis
    /// node (`Node::genesis` over `Snapshot::genesis(vc, policy, capacity)`,
    /// whose policy digest is the empty order's, and `Arc::new(
    /// storage.head()?)`, design §10 item 7), the registry and the shared
    /// state (`Arc::new`), and spawns the worker
    /// with a clone of it (`Worker::spawn(shared, min_step, rounds)`,
    /// `WorkerUnavailable` on failure). The target starts dead.
    pub fn new(config: Config, storage: S, policy: P, vc: C) -> Result<Self> {
        todo!("design §3 New")
    }

    /// The read side (design §3 threading contract): a `Clone + Send + Sync`
    /// handle for other threads.
    pub fn reader(&self) -> Reader<K, V, C, P, S> {
        self.reader.clone()
    }

    /// Proposes `batch` on `parent`, the committed state when `None` (design
    /// §3 `Propose`/`Proposal.Propose`, §8 Propose). `metadata` is
    /// root-affecting (design §7.3). `Closed` (the shared closing flag),
    /// `Poisoned` and `PrefixDeleteUnsupported` (a batch containing one,
    /// design §4, R22) checks first. Under the registry lock (first hold):
    /// `head = registry.head().clone()`; the parent node is `head` for `None`
    /// and for a `Some(n)` whose root is the head's (equal set, policy and
    /// storage state, design §7.3: the head node's snapshot and pinned view
    /// are used, so a child never chains on a twin's uncommitted engine
    /// proposal), else `n` itself whatever its lineage, as firewood and
    /// qmdb-ffi accept (design §8: a child of a node that can no longer be
    /// committed is itself never committable and `Commit` says so; if its
    /// batch evicts, the engine may refuse the storage proposal with
    /// `ProposalOrphaned`, design §10 item 4, which this returns). Then with
    /// no lock held: `applied = parent.snapshot().apply(batch)` (the copy,
    /// design §4: the policy clone carries the parent's marks, which `writes`
    /// reconciles into `applied.refinement`; the worker keeps hashing
    /// meanwhile); then, if `self.targets(parent)`, `finished =
    /// self.worker.finish()` (design §9.2, R24: blocks until every submitted
    /// op reached the engine; `Some((latest, steps))` when at least one
    /// step was built).
    /// The storage view (design §9.1): on `Some((latest, steps))`, read the
    /// old value of every key of `refinement.retract` through the parent's
    /// pinned view (`Put(old)`, `Delete` if absent; the read-back is kept by
    /// decision pending a firewood restore op, design §12.10 (e)), append
    /// `entries_to_ops` of the entries of `evicted` whose key is in
    /// `refinement.evict` (both in key order: one merge), and `F =
    /// latest.propose(ops, metadata)` (built even with no op: the node's
    /// view must be the reconciled one); a failed read or a failed `F`:
    /// count `fallbacks` and take the plain path (always available, design
    /// §10 item 5). Otherwise (`None`: not the target, no step built, a step
    /// failed, the worker gone; `finish` is not called off the target), with
    /// nothing evicted inherit the parent's
    /// view, else the plain path, `parent.storage_view().propose(
    /// entries_to_ops(evicted), metadata)` (the handle's own dispatch,
    /// design §10 item 4; the refinement is ignored). `steps` is `finish`'s
    /// when `F` was built and 0 otherwise, the fallback included.
    /// Count the proposal with no lock held (`counters`, design §9.5:
    /// `plain` for a plain proposal, a fallback included; `speculative` with
    /// `steps`, `reused_ops = evicted.len() - refinement.evict.len()` and
    /// `reverted_ops = refinement.retract.len()` for `F`). `metadata_hash =
    /// root::metadata_hash(metadata)`, `root = self.terms.combine(
    /// snapshot.capacity(), snapshot.height(), snapshot.commitment(),
    /// snapshot.policy_digest(), view.root(), metadata_hash)`,
    /// `Node::new(snapshot, Arc::downgrade(parent), view, steps,
    /// metadata_hash, root)`, and add the batch counts to `set` (puts,
    /// in-place updates, evictions; relaxed atomics, no lock). Under the
    /// registry lock (second hold): `registry.register(&node)`; return
    /// `Handle::new(node)`. A second child of the same target reuses the
    /// same steps: the parent's marks are still there and `finish` answers
    /// the same latest view (design §9.2).
    pub fn propose(
        &mut self,
        parent: Option<&Handle<K, V, C, P, S>>,
        batch: Vec<BatchOp<K, V>>,
        metadata: Option<&[u8]>,
    ) -> Result<Handle<K, V, C, P, S>> {
        todo!("design §8 Propose")
    }

    /// Commits `handle`'s node (design §3 `Proposal.Commit`, §8 Commit);
    /// consumes the handle **on every outcome**, a pre-check failure included
    /// (design §8: the node is untouched then, but the proposal is freed
    /// unless another handle holds it; this mirrors firewood's `commit(self)`
    /// and the adapter never hits it). `Closed`/`Poisoned` checks first.
    /// Upgrade the node's parent link with no lock held (the temporary, a
    /// dropped parent's last reference at worst, is released after the hold,
    /// design §10), then under the registry lock (one hold), three checks in
    /// this order:
    /// `node.root() == head.root()` ->
    /// `ProposalConsumed` (the head itself or an equal-root twin: that state
    /// is committed); the parent link dead -> `ProposalOrphaned` (the parent
    /// was dropped uncommitted, so this node can never be committed); the
    /// parent not the head node (`Arc::ptr_eq`) -> `ParentUncommitted` (a
    /// deeper pipelined node, or a line that lost: commit the parent first,
    /// or never; design §8); in the same hold, `owns = !Arc::ptr_eq(
    /// node.storage_view(), head.storage_view())` (an inherited view is the
    /// head's, the same `Arc`). Then with no lock held: if `owns`, commit
    /// `node.storage_view()` (the engine commits a speculative node's steps
    /// first, design §10); on an error, set `poisoned` iff `node.steps() > 0`
    /// (the cascade may have moved the engine head to a state no node owns,
    /// design §9.2) and return the error (the head stays where it was; no
    /// retarget). Then under the registry lock (one hold): `retired =
    /// registry.advance(node.clone(), self.config.local.revisions)`; after
    /// the hold drop `retired` (a snapshot and engine handles: not a
    /// nanosecond hold, design §10). Then `self.after_commit(&node)` (design
    /// §9.2: leave the precomputation alone when it already targets the new
    /// head or a live child of it, else retarget it to the new head; nothing
    /// held). Returns `node.root()`, the combined root `Root()` reports from
    /// now on (design §3).
    /// `commit` never waits for the worker (R17); its engine commit may wait
    /// on the handle lock for the one step being hashed on the handle it
    /// commits, when the next block's hints already queued ops on this node
    /// (the pipelined flow, design §9.2).
    pub fn commit(&mut self, handle: Handle<K, V, C, P, S>) -> Result<Hash> {
        todo!("design §8 Commit, §9.2")
    }

    /// `propose` on the committed state followed by `commit` (design §3
    /// `Update`).
    pub fn update(&mut self, batch: Vec<BatchOp<K, V>>, metadata: Option<&[u8]>) -> Result<Hash> {
        let handle = self.propose(None, batch, metadata)?;
        self.commit(handle)
    }

    /// Prefetch hint (design §9.4, R14): execution reading through `view`
    /// (the committed state when `None`) is writing `keys`. Feeds the
    /// precomputation; the adapter's finalise-time hook lands here with the
    /// revision at the parent root as `view`. Advisory; a no-op after
    /// `close` (the closing flag is checked first) and with speculation off.
    /// Under the registry lock (one hold): resolve `view` as `propose` does
    /// (the head node for `None` and for a root equal to the head's),
    /// cloning the node and the head. Then with no lock of auth_wal's held
    /// but the policy's (design §9.2 "Target"): if the node is a live child
    /// of the target (`self.target().is_some_and(|t| node.parent_is(&t))`),
    /// or there is no live target and the node is the head or a live child
    /// of it (`node.parent_is(&head)`), `self.retarget(&node)`; then, if
    /// `self.targets(&node)`: `pred = node.snapshot().hint_writes(keys,
    /// local.speculation_margin)` (the policy lock, design §4, §6.1), `ops`
    /// = `SpecOp::Evict(k, entry)` for every key of `pred.evict` with its
    /// entry from the node's snapshot (the policy tracks exactly the set's
    /// keys; a missing one is a policy bug, `unreachable!`) then
    /// `SpecOp::Retract(k)` for every key of `pred.retract`, and
    /// `worker.submit(ops)` if any; count `hint_keys`. A hint on any other
    /// node does nothing (no warming: the steps are the warming, R26). The
    /// hint returns at once and never waits for the engine (R17). Hints are
    /// advisory: engine errors are counted on the worker, never returned.
    pub fn prefetch(&mut self, view: Option<&Handle<K, V, C, P, S>>, keys: &[K]) {
        todo!("design §9.4")
    }

    /// Pass-through to `StorageEngine::prune` (design §3).
    pub fn prune(&mut self) -> Result<()> {
        self.storage.prune()
    }

    /// Mirrors qmdb-ffi's `Close` (design §3), a writer operation: the Go
    /// layer has already waited for its outstanding handles.
    /// Sets the closing flag first (writer operations report `Closed` from
    /// now on and hints are no-ops), then `worker.shutdown()` (drops the
    /// channel's sender and joins the thread, whose step being built
    /// finishes and whose chain, the only speculative engine handles there
    /// were, drops with it, design §9.2); then `storage.close()` through the
    /// writer's exclusive reference, which succeeds because committed
    /// proposals do not pin the engine (design §10 item 6) and reports
    /// `ActiveHandles` if a Rust caller still holds a handle to an
    /// uncommitted proposal with its own storage proposal. Reads through
    /// handles and `Reader`s keep working afterwards, the retained revisions
    /// included: the registry is not touched, and the ring is freed with the
    /// instance (nothing outlives the writer and its readers in any real
    /// path, R22). The firewood file stays open (and locked) while the
    /// head's pinned view lives, so an in-process reopen of the same
    /// directory needs this `AuthWal` dropped first (the Go `Close` drops
    /// it). Idempotent and allowed on a poisoned instance.
    pub fn close(&mut self) -> Result<()> {
        todo!("closing.store(true); worker.shutdown(); storage.close()")
    }

    /// Whether `node` is the precomputation's target (pointer identity on the
    /// weak link; a dead target matches nothing).
    fn targets(&self, node: &NodeRef<K, V, C, P, S>) -> bool {
        std::ptr::eq(self.target.as_ptr(), Arc::as_ptr(node))
    }

    /// The target if still alive (one `Weak` upgrade, no lock).
    fn target(&self) -> Option<NodeRef<K, V, C, P, S>> {
        self.target.upgrade()
    }

    /// Retargets the precomputation to `node` (design §9.2 "Target"):
    /// `node.snapshot().reset_hints()` (the policy forgets its marks: nothing
    /// marked is in the new generation's storage), `self.target =
    /// Arc::downgrade(node)`, `self.worker.reset(node.storage_view()
    /// .clone())` (a fresh chain on the new base; the thread drops the old
    /// one and its queued ops, counting unread steps).
    /// No step is planned: the first hint plans (design §12.5).
    fn retarget(&mut self, node: &NodeRef<K, V, C, P, S>) {
        todo!("design §9.2 retarget")
    }

    /// After the `commit` that just installed `head` (design §9.2). No-op
    /// unless speculation is on. With no lock held: if the precomputation
    /// already targets `head` (its steps were built on this node's own view,
    /// now committed, and stay valid bases) or a live child of it
    /// (`target().is_some_and(|t| t.parent_is(head))`, the pipeline's next
    /// parent), leave it alone; otherwise `self.retarget(head)`. `commit`'s
    /// caller never waits for engine work here (R17).
    fn after_commit(&mut self, head: &NodeRef<K, V, C, P, S>) {
        todo!("design §9.2 after a commit")
    }
}

#[cfg(test)]
mod send_sync {
    //! The writer moves to the consensus thread, the reader and the handles
    //! cross threads in the adapter (design §3 threading contract, §10):
    //! assert it at compile time for the FFI's instantiation (design §4), so
    //! a non-`Send`/`Sync` field cannot slip in unnoticed.

    use crate::policy::Fifo;
    use crate::storage::MemoryStorage;
    use crate::vc::lthash::LtHash;

    type K = std::sync::Arc<[u8]>;
    type V = std::sync::Arc<[u8]>;
    type Db = crate::AuthWal<K, V, LtHash, Fifo<K>, MemoryStorage<K, V>>;
    type Rd = crate::Reader<K, V, LtHash, Fifo<K>, MemoryStorage<K, V>>;
    type Hd = crate::proposal::Handle<K, V, LtHash, Fifo<K>, MemoryStorage<K, V>>;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn writer_is_send_and_read_side_is_send_and_sync() {
        assert_send::<Db>();
        assert_send_sync::<Rd>();
        assert_send_sync::<Hd>();
    }
}
