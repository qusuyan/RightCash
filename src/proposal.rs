//! Nodes, handles and the registry (design §5, §8).
//!
//! ```text
//! committed --> P1 --> P2        (chain: block building)
//!          \--> P1'              (sibling: competing block)
//! ```
//!
//! A `Node` is the immutable record of one proposal (design §8, R20): its
//! snapshot (the whole set after its batch), its pinned storage view, the
//! number of speculative steps under it, its root, and one weak link to its
//! parent. Its fields are fixed after construction; the snapshot's local
//! prediction marks and the storage handle's commit state have their own
//! synchronization. Reads check the snapshot, then storage on a miss; they
//! never walk parent nodes. Storage adapters may acquire locks (design §5,
//! §10). Committed status is registry membership: the `Registry` is the K-ring
//! of committed nodes in commit order (its newest entry is the head) and the
//! live index, nothing else, behind the one mutex (`Shared.registry`); every
//! hold is a few pointer operations or a scan of the ring (design §10). A
//! [`Handle`] is the caller's newtype over a node and holds nothing else.

use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use crate::policy::EvictionPolicy;
use crate::set::{Entry, Snapshot};
use crate::storage::{StorageEngine, StorageProposal};
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
    C: VectorCommitment<K>,
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
    /// Speculative ancestors the node's storage view chains on (the final view on
    /// `S1..Sk`, design §9.1); 0 for a plain proposal or an inherited view.
    /// A failed engine commit poisons the instance iff this is non-zero
    /// (design §9.2).
    steps: u32,
    /// `root::Terms::combine` over the node's terms, computed by `propose`.
    root: Hash,
    /// Retained to verify the combined root when recovering a checkpoint.
    metadata_hash: Digest,
}

impl<K, V, C, P, S> Node<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Return an `Arc` owning a node with the supplied state. `snapshot` is its
    /// complete write set; `parent` is a weak link to the proposal parent (empty
    /// for initialization); `storage_view` is the storage state read on a set
    /// miss. `steps` counts speculative ancestors of this node's own storage
    /// proposal, or is zero for a plain proposal or an inherited view.
    /// `root` must already combine these states using the writer's root terms.
    /// This constructor stores the inputs; it does not validate or register them.
    pub(crate) fn new(
        snapshot: Snapshot<K, V, C, P>,
        parent: WeakNode<K, V, C, P, S>,
        storage_view: StorageView<K, V, S>,
        steps: u32,
        root: Hash,
        metadata_hash: Digest,
    ) -> NodeRef<K, V, C, P, S> {
        Arc::new(Self {
            snapshot,
            parent,
            storage_view,
            steps,
            root,
            metadata_hash,
        })
    }

    pub(crate) fn root(&self) -> Hash {
        self.root
    }

    pub(crate) fn metadata_hash(&self) -> Digest {
        self.metadata_hash
    }

    pub(crate) fn height(&self) -> u64 {
        self.snapshot.height()
    }

    /// The whole set at this node (design §4).
    pub(crate) fn snapshot(&self) -> &Snapshot<K, V, C, P> {
        &self.snapshot
    }

    /// Return the owned value for borrowed `key`, or `None` if absent/deleted.
    /// A snapshot tombstone suppresses any older storage value. Only a key
    /// absent from the snapshot is read through the pinned storage view, whose
    /// errors (including `RevisionStale`) propagate. The snapshot lookup takes
    /// no lock; the storage adapter may lock. No parent node is consulted.
    pub(crate) fn get(&self, key: &K) -> Result<Option<V>> {
        match self.snapshot.get(key) {
            Some(Entry::Value(value)) => Ok(Some(value)),
            // Falling through here would resurrect a value deleted by this snapshot.
            Some(Entry::Tombstone) => Ok(None),
            None => self.storage_view.get(key),
        }
    }

    /// The parent, if still alive (one `Weak` upgrade, no lock): `Commit`'s
    /// check (design §8). The temporary is the caller's to release after
    /// its registry hold.
    pub(crate) fn parent(&self) -> Option<NodeRef<K, V, C, P, S>> {
        self.parent.upgrade()
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
/// the handle as an argument (`RightCash::commit(&mut self, handle)` consumes
/// it, design §3, R19). A plain newtype: no counter, no flag (the Go layer
/// counts its outstanding handles, R20). `Send + Sync`; dropping it releases
/// the node, which is freed when no handle and no ring entry holds it (the
/// precomputation and the live index hold it weakly, design §8).
pub struct Handle<K, V, C, P, S>(NodeRef<K, V, C, P, S>)
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>;

impl<K, V, C, P, S> Handle<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub(crate) fn new(node: NodeRef<K, V, C, P, S>) -> Self {
        Self(node)
    }

    pub(crate) fn node(&self) -> &NodeRef<K, V, C, P, S> {
        &self.0
    }

    /// `Commit` takes the handle by value (design §8): the node it held.
    pub(crate) fn into_node(self) -> NodeRef<K, V, C, P, S> {
        self.0
    }

    /// The single combined commitment (design §7.3), known before commit.
    pub fn root(&self) -> Hash {
        self.0.root
    }

    /// Return the owned value for `key` at this handle's node, or `None` if
    /// absent/deleted. Consult storage only on a snapshot miss, propagating
    /// storage errors. Does not lock the registry; the storage adapter may lock.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        self.0.get(key)
    }
}

/// The registry (design §8): the K-ring of committed nodes and the live
/// index, nothing else (the statistics are atomics on `Shared`). The one
/// structure the writer changes and readers read, behind `Shared.registry`
/// (design §10: every hold is a few pointer operations or a scan of the
/// ring, never an engine call, a snapshot copy or final node destruction; retired entries
/// are handed back to be dropped after the hold). The committed set is the
/// head's snapshot.
pub(crate) struct Registry<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
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
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Return a registry whose only committed node is `genesis` and whose live
    /// index is empty. `genesis` may contain an initialized nonempty snapshot;
    /// the caller has already computed its root. `revisions` is the maximum
    /// retained committed-node count, validated by the caller.
    ///
    /// Reserve `revisions + 1` ring slots so `advance` can push before retiring
    /// an entry. Reserve 16 live-index slots initially; that vector can grow.
    /// The window size is not stored and must also be passed to `advance`.
    pub fn new(genesis: NodeRef<K, V, C, P, S>, revisions: usize) -> Self {
        let mut ring = VecDeque::with_capacity(revisions + 1);
        ring.push_back(genesis);
        Self {
            ring,
            live: Vec::with_capacity(16),
        }
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
        if let Some(node) = self.ring.iter().rev().find(|node| node.root() == root) {
            return Some(node.clone());
        }
        self.live
            .iter()
            .filter(|(r, _, _)| *r == root)
            .find_map(|(_, _, weak)| weak.upgrade())
    }

    /// Register borrowed `node` for lookup by root, after removing dead weak
    /// links. Stores only its root, height, and a weak reference; the caller
    /// must keep the node alive. Returns `()` and may grow the index. Call once
    /// per new node: duplicate registrations are not checked.
    pub fn register(&mut self, node: &NodeRef<K, V, C, P, S>) {
        self.live.retain(|(_, _, weak)| weak.strong_count() > 0);
        self.live.push((node.root(), node.height(), Arc::downgrade(node)));
    }

    /// Install owned `node` as the newest committed head. `revisions` is the
    /// retained-node limit, validated by the caller and unchanged since `new`.
    /// The caller must have checked parent eligibility and committed storage.
    ///
    /// Remove this node's live-index entry by pointer identity, plus dead links
    /// and entries at heights outside the new window. Append the node to the
    /// committed ring. If the ring exceeds the limit, return its oldest node;
    /// otherwise return `None`. Drop that returned node after releasing the
    /// registry lock so destruction does not block other registry users.
    /// Existing handles still own nodes removed from the registry, but lookup
    /// by root no longer finds them unless another retained/live entry matches.
    #[must_use = "drop the retired node after releasing the registry lock"]
    pub fn advance(&mut self, node: NodeRef<K, V, C, P, S>, revisions: usize) -> Option<NodeRef<K, V, C, P, S>> {
        let head_height = node.height();
        let window = revisions as u64;
        self.live.retain(|(_, height, weak)| {
            weak.strong_count() > 0 && !std::ptr::eq(weak.as_ptr(), Arc::as_ptr(&node)) && height + window > head_height
        });
        self.ring.push_back(node);
        if self.ring.len() > revisions {
            self.ring.pop_front()
        } else {
            None
        }
    }
}
