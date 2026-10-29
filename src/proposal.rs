//! Nodes, handles and the registry (design §5, §8).
//!
//! ```text
//! committed --> P1 --> P2        (chain: block building)
//!          \--> P1'              (sibling: competing block)
//! ```
//!
//! A `Node` is the immutable record of one proposal (design §8, R20): its
//! snapshot (the whole set after its batch), its pinned storage view, the
//! number of speculative steps under it, its root with the metadata hash
//! that entered it, and one weak link to its parent. Nothing in it is written
//! after construction, so the writer, the worker and the reader threads
//! share it freely and every read is one lookup with no lock (design §5,
//! §10). Committed-ness is registry membership: the `Registry` is the K-ring
//! of committed nodes in commit order (its newest entry is the head) and the
//! live index, nothing else, behind the one mutex (`Shared.registry`); every
//! hold is a few pointer operations or a scan of the ring (design §10). A
//! [`Handle`] is the caller's newtype over a node and holds nothing else.

use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use crate::policy::EvictionPolicy;
use crate::set::Snapshot;
use crate::storage::StorageEngine;
use crate::types::{Digest, Hash, Key, Result, Value};
use crate::vc::VectorCommitment;

pub(crate) type NodeRef<K, V, C, P, S> = Arc<Node<K, V, C, P, S>>;
pub(crate) type WeakNode<K, V, C, P, S> = Weak<Node<K, V, C, P, S>>;
/// The storage view a node reads through after its own set (design §5): the
/// proposal created by the nearest ancestor-or-self that created one (design
/// §6.3, §9.1), otherwise the committed handle that chain started from
/// (`StorageEngine::head` for the genesis node). Always a proposal handle (design §10 item 7), so
/// proposing on it is one call whatever its state (item 4) and it stays
/// readable across commits (item 2).
pub(crate) type StorageView<K, V, S> = Arc<<S as StorageEngine<K, V>>::Proposal>;
/// The live index's entries (`Registry::live`): root and height cached
/// beside the weak link, so a lookup compares roots and a retirement
/// compares heights without upgrading (nothing is dropped under the hold,
/// design §10). An alias because the inline type trips clippy's
/// type-complexity lint.
pub(crate) type LiveIndex<K, V, C, P, S> = Vec<(Hash, u64, WeakNode<K, V, C, P, S>)>;

/// One node of the tree: one proposal, immutable from construction (design
/// §8, R20).
pub(crate) struct Node<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// The whole set after this node's batch (design §4): what reads look up
    /// and what a child copies.
    snapshot: Snapshot<K, V, C, P>,
    /// The parent this node was proposed on (design §8): the one link a node
    /// has. Weak, retaining nothing but the parent's inline allocation; dead
    /// once the parent was dropped uncommitted, after which this node can
    /// never be committed (`Commit` requires the parent to be the head).
    parent: WeakNode<K, V, C, P, S>,
    /// Pinned storage view (design §5): the proposal this node created (its
    /// batch evicted, design §6.3: the plain proposal, or
    /// the final proposal `F` of the speculative path), else the parent's,
    /// the same `Arc`. `Commit` tells the two apart by identity with the
    /// head's view (design §8).
    storage_view: StorageView<K, V, S>,
    /// Speculative steps the node's own storage proposal chains on (`F` on
    /// `S1..Sk`, design §9.1); 0 for a plain proposal or an inherited view.
    /// A failed engine commit poisons the instance iff this is non-zero
    /// (design §9.2).
    steps: u32,
    /// `SHA-256(metadata)` or zeros (design §7.3): the one preimage term not
    /// derivable from the node and the writer's `root::Terms`, kept so a
    /// proof (design §7.4) can rebuild the preimage.
    metadata_hash: Digest,
    /// `root::Terms::combine` over the node's terms, computed by `propose`.
    root: Hash,
}

impl<K, V, C, P, S> Node<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// A live node (design §8 Propose); `root` is `Terms::combine` of the
    /// writer's terms and this node's own, computed by `propose`.
    pub(crate) fn new(
        snapshot: Snapshot<K, V, C, P>,
        parent: WeakNode<K, V, C, P, S>,
        storage_view: StorageView<K, V, S>,
        steps: u32,
        metadata_hash: Digest,
        root: Hash,
    ) -> NodeRef<K, V, C, P, S> {
        todo!("design §8 Propose")
    }

    /// The genesis node (design §8, §7.3): the empty set at height 0, no
    /// parent (a dead weak link), the engine's `head()` as its view (design
    /// §10 item 7), no storage proposal, no steps, zero metadata hash, root
    /// `EMPTY_ROOT` (what `combine` gives any height-0 preimage).
    pub(crate) fn genesis(
        snapshot: Snapshot<K, V, C, P>,
        storage_view: StorageView<K, V, S>,
    ) -> NodeRef<K, V, C, P, S> {
        todo!("design §8 genesis; debug_assert!(snapshot.height() == 0 && snapshot.len() == 0)")
    }

    pub(crate) fn root(&self) -> Hash {
        self.root
    }

    pub(crate) fn height(&self) -> u64 {
        self.snapshot.height()
    }

    /// The whole set at this node (design §4).
    pub(crate) fn snapshot(&self) -> &Snapshot<K, V, C, P> {
        &self.snapshot
    }

    /// Design §5: the snapshot (`Value` is the value, `Tombstone` is absent
    /// and stops), then the pinned storage view, which returns
    /// `RevisionStale` once the engine dropped its root. No lock.
    pub(crate) fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("design §5")
    }

    /// Whether `node` is this node's parent (pointer identity on the weak
    /// link, no upgrade): the precomputation's retarget rule (design §9.2).
    pub(crate) fn parent_is(&self, node: &NodeRef<K, V, C, P, S>) -> bool {
        std::ptr::eq(self.parent.as_ptr(), Arc::as_ptr(node))
    }

    pub(crate) fn storage_view(&self) -> &StorageView<K, V, S> {
        &self.storage_view
    }

    /// Speculative steps under this node's storage proposal (design §9.1);
    /// 0 on the plain path or with an inherited view.
    pub(crate) fn steps(&self) -> u32 {
        self.steps
    }
}

/// The caller's handle over a node (design §8): what `Propose` returns and
/// what `Revision(root)` resolves to; qmdb-ffi's `Proposal` and `Revision`
/// are both this type. Read-only (`get`, `root`); committing it,
/// proposing on it and hinting through it are writer operations that take
/// the handle as an argument (`AuthWal::commit(&mut self, handle)` consumes
/// it, design §3, R19). A plain newtype: no counter, no flag (the Go layer
/// counts its outstanding handles, R20). `Send + Sync`; dropping it releases
/// the node, which is freed when no handle and no ring entry holds it (the
/// precomputation and the live index hold it weakly, design §8).
pub struct Handle<K, V, C, P, S>(NodeRef<K, V, C, P, S>)
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>;

impl<K, V, C, P, S> Handle<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub(crate) fn new(node: NodeRef<K, V, C, P, S>) -> Self {
        Self(node)
    }

    pub(crate) fn node(&self) -> &NodeRef<K, V, C, P, S> {
        &self.0
    }

    /// The single combined commitment (design §7.3), known before commit.
    pub fn root(&self) -> Hash {
        self.0.root
    }

    /// Design §5: one lookup in the node's snapshot, then its pinned storage
    /// view. No lock.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.0.get(key)
    }
}

/// The registry (design §8): the K-ring of committed nodes and the live
/// index, nothing else (the statistics are atomics on `Shared`). The one
/// structure the writer changes and readers read, behind `Shared.registry`
/// (design §10: every hold is a few pointer operations or a scan of the
/// ring, never an engine call, a snapshot copy or a drop; retired entries
/// are handed back to be dropped after the hold). The committed set is the
/// head's snapshot.
pub(crate) struct Registry<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// The last K committed nodes in commit order, oldest first (design §8
    /// revision window); the newest is the **head**, so there is no head
    /// field. Never empty: the head stays until the instance is dropped (its
    /// pinned view does not keep the engine from closing, design §10 item 6;
    /// `close` leaves the ring alone, R22). Committed roots are
    /// pairwise distinct (height is in the preimage); a lookup by root scans
    /// from the newest entry, at most K root compares (design §10).
    ring: VecDeque<NodeRef<K, V, C, P, S>>,
    /// The live index (design §8): the live proposals as (root, height, weak
    /// link) triples, so a lookup compares roots and a retirement compares
    /// heights without upgrading; a short list
    /// (the blocks being verified ahead and the competing candidates), weak
    /// because the caller's handle owns the node. It exists because
    /// `Revision(root)` must resolve a live proposal's root (qmdb-ffi's
    /// `Database.Revision` and firewood's `view` do; the adapter's
    /// `Reader(root)` opens the state of an unaccepted block this way, and
    /// its hints reach the precomputation through the same lookup, design
    /// §9.4), which nothing else answers. Equal roots are interchangeable
    /// (design §7.3) but two distinct live nodes may share one and either
    /// may be dropped, so `lookup` returns the first still alive; dead links
    /// are dropped by `register`/`advance`. Lookup-only: never iterated for
    /// anything consensus-visible.
    live: LiveIndex<K, V, C, P, S>,
}

impl<K, V, C, P, S> Registry<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Genesis (design §8, §7.3): `genesis` is `Node::genesis`, root
    /// `EMPTY_ROOT`; it enters the ring, so `revision(EMPTY_ROOT)` resolves
    /// on a fresh instance (design §3) until it leaves the K-ring. Both lists
    /// are pre-sized (the ring to `revisions + 1`, the live index to a few
    /// entries) so neither an install nor a register allocates; `revisions`
    /// itself is not kept (`advance` takes it).
    pub fn new(genesis: NodeRef<K, V, C, P, S>, revisions: usize) -> Self {
        todo!("ring: VecDeque::with_capacity(revisions + 1) holding genesis; live: Vec::with_capacity(16)")
    }

    /// The newest committed node (design §8): the committed set is its
    /// snapshot, `Propose(None)` builds on it and `Commit` requires it as the
    /// parent.
    pub fn head(&self) -> &NodeRef<K, V, C, P, S> {
        self.ring.back().expect("the ring always holds the head")
    }

    /// Design §8 lookup order: the ring first (the committed node, scanned
    /// newest first), then the first live node still alive under `root`. No
    /// mutation.
    pub fn lookup(&self, root: Hash) -> Option<NodeRef<K, V, C, P, S>> {
        todo!("ring.iter().rev().find(root), else upgrade the live triples with that root in order")
    }

    /// Adds a live node to the index (design §8 Propose, second hold; once
    /// per node, `propose` is the only caller) after dropping the dead
    /// links, so the list stays bounded by the live proposals.
    pub fn register(&mut self, node: &NodeRef<K, V, C, P, S>) {
        todo!("retain strong_count > 0; push (root, height, downgrade)")
    }

    /// Makes `node` the head (design §8 Commit): it is pushed on the ring and
    /// leaves the live index by identity, along with any dead link and any
    /// entry `revisions` or more heights below the new head (one window for
    /// both lists, R22, `h + revisions > head.height()` on the cached height,
    /// no upgrade: such a node, if still held, stays readable but no longer
    /// resolves by root; the ring answers for the committed root, so
    /// `lookup` there finds the committed node, not a live sibling sharing
    /// the root); with the ring
    /// over `revisions` entries the oldest is popped and **returned for the
    /// caller to drop after releasing the lock** (its snapshot and engine
    /// view go with it unless a handle still holds it; the precomputation
    /// holds its target weakly, design §8).
    #[must_use = "drop the retired node after releasing the registry lock"]
    pub fn advance(&mut self, node: NodeRef<K, V, C, P, S>, revisions: usize) -> Option<NodeRef<K, V, C, P, S>> {
        todo!(
            "ring.push_back(node); pop_front if len > revisions; live.retain(|(_, h, w)| w.strong_count() > 0 && !ptr_eq(w, node) && h + revisions > head.height())"
        )
    }
}
