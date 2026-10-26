//! Proposal tree, pinned storage views, commit/drop and the revision window
//! (design §5, §8).
//!
//! ```text
//! committed --> P1 --> P2        (chain: block building)
//!          \--> P1'              (sibling: competing block)
//! ```
//!
//! Lock order: the tree lock (`Shared.inner`) is taken first, then a node's
//! speculative-chain mutex, then a node's `parent` link, then the engine.
//! Dropping a `Proposal` or `Revision` handle takes no lock: it decrements
//! the shared live-handle counter (lock-free, for `close`) and releases its
//! reference. The root index holds weak links and sweeps dead ones when
//! nodes are registered or retired, never on lookup (design §8 Drop).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use crate::Shared;
use crate::policy::EvictionPolicy;
use crate::proof::AuthProof;
use crate::root::RootPreimage;
use crate::set::{Applied, WriteSet};
use crate::speculate::{ChainSlot, StoragePath};
use crate::storage::{ReadView, StorageEngine, ViewRef};
use crate::types::{BatchOp, Hash, Key, Result, Value};
use crate::vc::VectorCommitment;

pub type NodeRef<K, V, C, P, S> = Arc<Node<K, V, C, P, S>>;
pub type WeakNode<K, V, C, P, S> = Weak<Node<K, V, C, P, S>>;
/// A node's parent link; only touched under the tree lock.
pub type ParentLink<K, V, C, P, S> = Mutex<Option<NodeRef<K, V, C, P, S>>>;
/// Root -> live/retained nodes (design §8 root index).
pub type RootIndex<K, V, C, P, S> = HashMap<Hash, Vec<WeakNode<K, V, C, P, S>>>;
/// Uncommitted ancestors oldest-first, ending with the node to commit.
pub type Chain<K, V, C, P, S> = Vec<NodeRef<K, V, C, P, S>>;
/// The speculative chain slot of a node acting as a parent (design §9.2);
/// local state, behind its own mutex. `Retired` is terminal.
pub type Speculation<K, V, S> = Mutex<ChainSlot<K, V, S>>;

/// The storage view a node reads through after its own write set (design §5):
/// the proposal created by the nearest ancestor-or-self that evicted, or the
/// committed revision that chain started from. Stays valid across commits
/// because `StorageProposal::commit(&self)` leaves the handle readable.
pub enum StorageView<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    Proposal(Arc<S::Proposal>),
    Revision(Arc<S::Revision>),
}

impl<K, V, S> Clone for StorageView<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    fn clone(&self) -> Self {
        match self {
            Self::Proposal(p) => Self::Proposal(p.clone()),
            Self::Revision(r) => Self::Revision(r.clone()),
        }
    }
}

impl<K, V, S> ReadView<K, V> for StorageView<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    fn root(&self) -> Hash {
        match self {
            Self::Proposal(p) => p.root(),
            Self::Revision(r) => r.root(),
        }
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        match self {
            Self::Proposal(p) => p.get(key),
            Self::Revision(r) => r.get(key),
        }
    }
}

impl<K, V, S> StorageView<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    /// Proposes `batch` on this view (design §6.3, §10 contract item 4): a
    /// `Proposal` with `!is_committed()` chains on it; a `Revision` or a
    /// committed `Proposal` routes through `engine.propose` after checking
    /// `self.root() == engine.root()` (auth_wal is the engine's sole writer),
    /// returning `ProposalOrphaned` rather than panicking if it does not hold
    /// (the hint path treats that as a dropped link, design §9.2). The
    /// decision uses `is_committed`, never the root comparison, because an
    /// uncommitted proposal can be a storage no-op with the engine's root.
    pub fn propose(&self, engine: &S, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<S::Proposal> {
        todo!("design §10 contract item 4")
    }

    /// Borrowed, engine-typed form for `StorageEngine::prefetch`.
    pub fn as_ref(&self) -> ViewRef<'_, K, V, S> {
        match self {
            Self::Proposal(p) => ViewRef::Proposal(&**p),
            Self::Revision(r) => ViewRef::Revision(&**r),
        }
    }
}

/// The counts of `Applied` without the evicted entries (which went to storage).
#[derive(Clone, Copy, Debug, Default)]
pub struct AppliedSummary {
    pub puts: usize,
    pub new_keys: usize,
    pub in_place_updates: usize,
    pub deletes: usize,
    pub prefix_deletes: usize,
    pub evicted: usize,
}

impl<K, V> From<&Applied<K, V>> for AppliedSummary {
    fn from(a: &Applied<K, V>) -> Self {
        Self {
            puts: a.puts,
            new_keys: a.new_keys,
            in_place_updates: a.in_place_updates,
            deletes: a.deletes,
            prefix_deletes: a.prefix_deletes,
            evicted: a.evicted.entries.len(),
        }
    }
}

/// One node of the tree.
pub struct Node<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    set: WriteSet<K, V, C, P>,
    /// Pinned storage view (design §5).
    storage_view: StorageView<K, V, S>,
    /// The storage proposal this node itself created, if its batch evicted or
    /// prefix-deleted (design §6.3): the plain proposal, or the final
    /// proposal `F` of the speculative path, whose ancestors are the links.
    /// Committed when the node commits.
    own_storage: Option<Arc<S::Proposal>>,
    /// How the eviction reached storage; counted at commit (design §9.5).
    storage_path: StoragePath,
    /// Batch summary from `WriteSet::apply`, for `Stats` and the round-0
    /// speculation estimate at commit (design §9.2).
    applied: AppliedSummary,
    /// Speculative chain slot for children of this node (design §9.2). Local
    /// state: created by prefetch hints or round 0 after commit, shared by
    /// all children; retired by `ProposalTree::advance` when the node stops
    /// being the head or by a failed orphan check, dropped with the node
    /// (the chain's `Drop` releases the link budget either way).
    speculation: Speculation<K, V, S>,
    /// The exact preimage `root` was computed from (design §7.3): built from
    /// the *effective* consensus parameters at construction, so it is
    /// reproducible for proofs (§7.4) after a capacity-controller change, and
    /// never sees the local part of the configuration.
    preimage: RootPreimage,
    /// `root::combine(&preimage)`, cached.
    root: Hash,
    /// `Release` on commit, `Acquire` on read; checked under the tree lock
    /// where the answer matters (`chain_to_commit`).
    committed: AtomicBool,
    /// Cleared on commit so freed snapshots are reclaimed (design §8).
    /// Only touched under the tree lock.
    parent: ParentLink<K, V, C, P, S>,
}

impl<K, V, C, P, S> Node<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub fn root(&self) -> Hash {
        self.root
    }

    pub fn height(&self) -> u64 {
        self.set.height()
    }

    pub fn set(&self) -> &WriteSet<K, V, C, P> {
        &self.set
    }

    pub fn storage_view(&self) -> &StorageView<K, V, S> {
        &self.storage_view
    }

    pub fn storage_root(&self) -> Hash {
        self.preimage.storage_root
    }

    pub fn storage_path(&self) -> StoragePath {
        self.storage_path
    }

    pub fn applied(&self) -> AppliedSummary {
        self.applied
    }

    /// The speculative chain attached to this node as a parent (design §9.2).
    pub fn speculation(&self) -> &Speculation<K, V, S> {
        &self.speculation
    }

    /// Write set first, then the pinned storage view (design §5).
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("set.get -> Value/Tombstone, else storage_view.get")
    }

    /// The preimage this node's `root` was computed from (design §7.3).
    pub fn preimage(&self) -> &RootPreimage {
        &self.preimage
    }
}

/// Handle returned to callers; mirrors the `ffi` `Proposal`. Holds a strong
/// reference to its node; dropping the handle decrements the live-handle
/// counter and releases the reference (design §8 Drop): the node is freed
/// when the last reference goes.
pub struct Proposal<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    node: NodeRef<K, V, C, P, S>,
    db: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Proposal<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Only `Shared::propose_on` builds handles. Counts the handle for
    /// `close` (design §3); `Drop` undoes it.
    pub(crate) fn new(node: NodeRef<K, V, C, P, S>, db: Arc<Shared<K, V, C, P, S>>) -> Self {
        db.live_handles.fetch_add(1, Ordering::AcqRel);
        Self { node, db }
    }

    /// The single combined commitment, known before commit (design §7.3).
    pub fn root(&self) -> Hash {
        self.node.root
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.node.get(key)
    }

    /// Prefetch hint relative to this proposal (design §9.4, R14): execution
    /// building on it is writing `keys`. Advisory; an orphaned proposal only
    /// warms storage.
    pub fn prefetch(&self, keys: &[K]) {
        self.db.prefetch_on(crate::Parent::Node(&self.node), keys)
    }

    /// Child proposal on top of this uncommitted one (design §8 Propose).
    pub fn propose(&self, batch: Vec<BatchOp<K, V>>) -> Result<Self> {
        self.propose_with_metadata(batch, None)
    }

    pub fn propose_with_metadata(&self, batch: Vec<BatchOp<K, V>>, metadata: Option<&[u8]>) -> Result<Self> {
        todo!("db.propose_on(Parent::Node(&self.node), batch, metadata)")
    }

    /// Commits uncommitted ancestors oldest-first, then this node; consumes
    /// the handle. `ProposalOrphaned` if the chain does not start at the
    /// committed node; `ProposalConsumed` if already committed; `Poisoned`
    /// after a failed mid-chain engine commit (design §8, §9.2). Round 0 of
    /// speculation for the new head runs here, after `commit_node` has
    /// released the tree lock.
    pub fn commit(self) -> Result<Hash> {
        todo!("let (root, job) = db.commit_node(&self.node)?; db.round0(job); Ok(root)")
    }
}

impl<K, V, C, P, S> Drop for Proposal<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    fn drop(&mut self) {
        self.db.live_handles.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Read-only view at a root: a live proposal or one of the last K commits.
/// Just a node (design §8); `get` reads through the pinned storage view and
/// surfaces `RevisionStale` if the engine has dropped that root.
pub struct Revision<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    node: NodeRef<K, V, C, P, S>,
    db: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Revision<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Only `AuthWal::revision` builds handles. Counts the handle for
    /// `close` (design §3); `Drop` undoes it.
    pub(crate) fn new(node: NodeRef<K, V, C, P, S>, db: Arc<Shared<K, V, C, P, S>>) -> Self {
        db.live_handles.fetch_add(1, Ordering::AcqRel);
        Self { node, db }
    }

    pub fn root(&self) -> Hash {
        self.node.root
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.node.get(key)
    }

    /// Prefetch hint relative to this revision (design §9.4, R14): execution
    /// building on it is writing `keys`. This is where the adapter's
    /// finalise-time hook lands, since execution reads through a revision at
    /// the parent root. Advisory; a retained non-head revision only warms
    /// storage.
    pub fn prefetch(&self, keys: &[K]) {
        self.db.prefetch_on(crate::Parent::Node(&self.node), keys)
    }

    /// Authenticated read (design §7.4): `Unsupported` when the scheme has no
    /// per-key proofs (`lthash`). Milestone 6.
    pub fn prove(&self, key: &K) -> Result<AuthProof<V>> {
        todo!("design §7.4")
    }
}

impl<K, V, C, P, S> Drop for Revision<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    fn drop(&mut self) {
        self.db.live_handles.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The tree: committed node, K-ring of committed nodes (strong), and a
/// root index over live proposals (weak) and history.
pub struct ProposalTree<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Never empty: `close` clears `history` but the committed node (and its
    /// pinned engine view) lives until the instance is dropped.
    committed: NodeRef<K, V, C, P, S>,
    /// Last K committed nodes, newest last (includes `committed`).
    history: VecDeque<NodeRef<K, V, C, P, S>>,
    /// Root -> nodes. Equal roots are interchangeable (design §7.3) but two
    /// distinct live nodes may share one and either may be dropped, so this
    /// is a list of weak links; `lookup` returns the first still alive and
    /// dead links are swept by `register`/`advance`. Lookup-only: never
    /// iterated for anything consensus-visible.
    by_root: RootIndex<K, V, C, P, S>,
    revisions: usize,
}

impl<K, V, C, P, S> ProposalTree<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Genesis: the empty set at height 0 over the engine's committed state;
    /// root `EMPTY_ROOT`, but the genesis preimage still records `effective`
    /// so a child can read its parameters back from `parent.preimage`. The
    /// genesis node is pushed on `history` and registered, so
    /// `revision(EMPTY_ROOT)` resolves on a fresh instance (design §3, §7.3)
    /// until it leaves the K-ring.
    pub fn new(
        genesis: WriteSet<K, V, C, P>,
        storage: StorageView<K, V, S>,
        effective: crate::Effective,
        revisions: usize,
    ) -> Self {
        todo!()
    }

    pub fn committed(&self) -> &NodeRef<K, V, C, P, S> {
        &self.committed
    }

    /// First live node with this root, if any (design §8 root index).
    pub fn lookup(&self, root: Hash) -> Option<NodeRef<K, V, C, P, S>> {
        todo!("upgrade weak links in order; no mutation under the read lock")
    }

    /// Idempotent: a node already present (e.g. committed after its handle
    /// was dropped) is not duplicated. Sweeps dead links in the same bucket.
    pub fn register(&mut self, node: &NodeRef<K, V, C, P, S>) {
        todo!()
    }

    /// Uncommitted ancestors of `node` oldest-first, including `node`.
    /// `ProposalConsumed` if `node` is committed; `ProposalOrphaned` if the
    /// chain does not end at `committed` (pointer equality) (design §8).
    pub fn chain_to_commit(&self, node: &NodeRef<K, V, C, P, S>) -> Result<Chain<K, V, C, P, S>> {
        todo!()
    }

    /// Makes `node` the committed state: marks it, clears its parent link,
    /// (re)registers it, pushes it on the ring, and retires the oldest beyond
    /// K, removing the retired node from `by_root`. Every K commits it also
    /// sweeps the whole index of dead links (dropped, never-committed
    /// proposals have unique roots nothing else revisits); the index is
    /// bounded by K plus live proposals, so this is O(K) amortised (design §8).
    /// Retires the previous head's chain slot (`ChainSlot::retire`): an
    /// ex-head can never be a parent again and its links would otherwise pin
    /// engine revisions for K commits (design §9.2); the chain's `Drop`
    /// releases the budget and counts it unused if no child built on it.
    pub fn advance(&mut self, node: NodeRef<K, V, C, P, S>) {
        todo!()
    }
}
