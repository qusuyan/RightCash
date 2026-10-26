//! `auth_wal`: an authenticated, bounded, in-memory set of recent writes that
//! sits between the EVM validators and an authenticated storage engine.
//!
//! See `docs/01-design.md`. Module map (design §13):
//!
//! | module | design | role |
//! |---|---|---|
//! | [`types`]     | §3, §4 | `Key`/`Value` bounds, `Hash`, `Digest`, `BatchOp`, errors |
//! | [`set`]       | §4     | `WriteSet`: persistent ordered map of entries plus commitment and policy state |
//! | [`policy`]    | §6     | `EvictionPolicy` trait, policy digest, the v0 policies |
//! | [`capacity`]  | §6.4   | `CapacityController` hook |
//! | [`vc`]        | §7     | `VectorCommitment` trait and schemes |
//! | [`root`]      | §7.3   | combining the set commitment, policy digest and storage root |
//! | [`lattice`]   | §6.1, §7.2 | lattice-hash accumulator shared by the policy digest, `lthash` and `MemoryStorage` |
//! | [`proof`]     | §7.4   | authenticated-read proof shape (`Revision::prove`) |
//! | [`storage`]   | §10    | `StorageEngine` abstraction and the in-memory model of firewood |
//! | [`proposal`]  | §8     | proposal tree, pinned storage views, commit/drop, revision window |
//! | [`speculate`] | §9     | speculative eviction chains and prefetch hints |
//! | `ffi`         | §2     | C ABI consumed by the Go `ffi` package (feature `ffi`) |

// Skeleton phase: bodies are `todo!()`, so parameters are unused for now.
// Remove once milestone 1 lands.
#![allow(unused_variables, dead_code)]

pub mod capacity;
pub mod lattice;
pub mod policy;
pub mod proof;
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
use std::sync::atomic::AtomicUsize;

use parking_lot::RwLock;

use crate::capacity::{CapacityController, SetStats};
use crate::policy::EvictionPolicy;
use crate::proposal::{Node, Proposal, ProposalTree, Revision};
use crate::speculate::{SpeculationConfig, SpeculationStats, Speculator};

/// Nodes that received a speculative chain (design §9.2 "Budget"): a leaf
/// lock on `Shared`, so the hint path can register a node after `on_hint`
/// returns and sweep it when the budget is exhausted without touching the
/// tree lock (taking the tree write lock inside a hint would invert the
/// tree → chain lock order against `propose_on`).
type ChainHolders<K, V, C, P, S> = parking_lot::Mutex<Vec<proposal::WeakNode<K, V, C, P, S>>>;
use crate::storage::StorageEngine;
use crate::types::{BatchOp, Hash, Key, Result, Value};
use crate::vc::VectorCommitment;

/// Consensus parameters: they enter the root preimage (design §7.3) and must
/// be identical on every validator. Deliberately no `Default`: a network
/// states them explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Consensus {
    /// Maximum number of entries (including tombstones) in the write set;
    /// initial value for the capacity controller (design §6.4).
    pub capacity: usize,
    /// Eviction stops once the set shrinks to this many entries (design §6.2).
    pub low_watermark: usize,
}

/// Local parameters: may differ between validators and never reach a root.
#[derive(Clone, Debug)]
pub struct Local {
    /// Committed snapshots kept for `revision()` (design §8).
    pub revisions: usize,
    /// Speculative eviction (design §9): send predicted victims to the engine
    /// early. Requires `S::ROOT_IS_STATE_FUNCTION`.
    pub speculate: bool,
    /// Maximum speculative links per parent node (design §9.2).
    pub speculation_rounds: u32,
    /// Minimum not-yet-sent predicted entries before a link is sent (design §9.2).
    pub speculation_min_link: usize,
    /// Instance-wide budget of live speculative links (design §9.2, memory bound).
    pub speculation_max_links_total: usize,
    /// Extra predicted victims beyond the strict count once the set is over
    /// capacity (design §9.2, R15: pessimistic prediction).
    pub speculation_margin: usize,
}

impl Default for Local {
    fn default() -> Self {
        // Rounds, min_link and the link budget are provisional until the milestone-3 sweep (design §9.2).
        Self {
            revisions: 128,
            speculate: false,
            speculation_rounds: 3,
            speculation_min_link: 1_000,
            speculation_max_links_total: 16,
            speculation_margin: 0,
        }
    }
}

impl Local {
    /// The engine window these settings need (design §8): `revisions * (1 +
    /// rounds)` when speculating on an engine that commits chain links
    /// separately, else `revisions`. One formula for `AuthWal::new`'s check,
    /// the benchmark's engine config and the Go `New`.
    pub fn engine_window<K: Key, V: Value, S: StorageEngine<K, V>>(&self) -> usize {
        if self.speculate && S::COMMITS_CHAIN_LINKS_SEPARATELY {
            self.revisions * (1 + self.speculation_rounds as usize)
        } else {
            self.revisions
        }
    }

    pub fn speculation(&self) -> SpeculationConfig {
        SpeculationConfig {
            enabled: self.speculate,
            rounds: self.speculation_rounds,
            min_link: self.speculation_min_link,
            max_links_total: self.speculation_max_links_total,
            margin: self.speculation_margin,
        }
    }
}

/// Configuration of an [`AuthWal`] instance (design §3). The split lets the
/// type system keep local settings out of `RootPreimage`/`Effective`, which
/// are built from `Consensus` only.
#[derive(Clone, Debug)]
pub struct Config {
    pub consensus: Consensus,
    pub local: Local,
}

impl Config {
    /// Capacity with the default watermark `capacity - capacity / 8` (design §6.2);
    /// local settings at their defaults.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            consensus: Consensus {
                capacity,
                low_watermark: capacity.saturating_sub(capacity / 8),
            },
            local: Local::default(),
        }
    }

    /// `capacity >= 1`, `low_watermark <= capacity` (equal still makes
    /// progress: eviction triggers at `len > capacity`), `revisions >= 2`,
    /// and when speculating `speculation_rounds >= 1` and
    /// `speculation_min_link >= 1` (0 would permit empty links). Called first
    /// thing by `AuthWal::new`.
    pub fn validate(&self) -> Result<()> {
        todo!()
    }
}

/// Observable counters for benchmarks and metrics (design §11).
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub height: u64,
    pub occupancy: usize,
    pub capacity: usize,
    pub total_evictions: u64,
    pub total_storage_ops: u64,
    pub storage_proposals: u64,
    /// All `Put` ops applied.
    pub puts: u64,
    /// Puts that replaced an entry already in the set (the layer's win; the
    /// `zipf` benchmark's hit rate is `in_place_updates / puts`).
    pub in_place_updates: u64,
    pub speculation: SpeculationStats,
}

/// Effective consensus parameters after the capacity controller (design
/// §6.4, §12.8): what `WriteSet::apply` and every node's `RootPreimage` use.
/// Built from `Consensus` only.
///
/// Determinism note: `Inner.effective` is keyed off the *committed* node. That
/// is exact for `Fixed` (the only v0 controller). Before an adaptive
/// controller lands, the effective parameters for a child must become a
/// function of its *parent* node (e.g. read back from `parent.preimage`),
/// otherwise a child proposed on an uncommitted evicting parent would use
/// different parameters than on a validator that committed the parent first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Effective {
    pub capacity: usize,
    pub low_watermark: usize,
}

impl From<&Consensus> for Effective {
    fn from(c: &Consensus) -> Self {
        Self {
            capacity: c.capacity,
            low_watermark: c.low_watermark,
        }
    }
}

impl Effective {
    /// New parameters for a controller target: the watermark keeps its ratio
    /// to the capacity (design §6.4).
    pub fn retarget(&self, capacity: usize) -> Self {
        todo!("low_watermark = capacity * self.low_watermark / self.capacity (integer, floor)")
    }
}

/// State shared between the database handle and every proposal/revision
/// handle (design §8: proposals need the tree, config and engine to commit).
pub(crate) struct Shared<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    config: Config,
    storage: S,
    /// Outside the tree lock (design §9): its state is atomic, so the hint
    /// path can use it without the tree lock and never serialises hints on
    /// different nodes.
    speculator: Speculator,
    /// See [`ChainHolders`]. Swept by `advance` (under the tree write lock,
    /// then per-node `retire()`) and by a budget-exhausted hint.
    chain_holders: ChainHolders<K, V, C, P, S>,
    /// Live `Proposal` + `Revision` handles, for `close` (design §3, §8).
    /// Incremented by `Proposal::new`/`Revision::new`, decremented lock-free
    /// in their `Drop` (both in `proposal.rs`).
    live_handles: AtomicUsize,
    inner: RwLock<Inner<K, V, C, P, S>>,
}

/// Guarded by the single lock (design §10, concurrency): `propose` and
/// `commit` take it for writing; reads and `prefetch` for reading (each
/// node's speculative chain has its own mutex).
struct Inner<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    tree: ProposalTree<K, V, C, P, S>,
    capacity: Box<dyn CapacityController>,
    effective: Effective,
    set_stats: SetStats,
    stats: Stats,
    closed: bool,
    /// Set by a failed mid-chain engine commit (design §9.2); `propose` and
    /// `commit` return `Error::Poisoned` from then on.
    poisoned: Option<String>,
}

/// What `Shared::round0` needs after `commit_node` released the lock
/// (design §9.2): the head that was just committed (not whatever is head by
/// the time round 0 runs), its batch's new-key count and the effective
/// parameters at commit.
pub(crate) struct Round0<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub head: Arc<Node<K, V, C, P, S>>,
    pub new_keys: usize,
    pub effective: Effective,
}

/// `commit_node`'s result: the committed root and the round-0 job to run
/// after the lock is released.
pub(crate) type Committed<K, V, C, P, S> = (Hash, Round0<K, V, C, P, S>);

/// The node a proposal is built on, resolved *under* the write lock so a
/// concurrent commit cannot slip between reading the committed node and
/// using it (design §10, concurrency).
pub(crate) enum Parent<'a, K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// The committed node at the time the lock is taken.
    Committed,
    /// An explicit node (`Proposal::propose`). Its parent chain is walked to
    /// a committed node exactly as `Commit` does; if that node is not the
    /// current committed node the parent is orphaned and the proposal is
    /// rejected with `ProposalOrphaned`, which keeps the sole-writer assertion
    /// in `StorageView::propose` a true invariant (design §10).
    Node(&'a Arc<Node<K, V, C, P, S>>),
}

impl<K, V, C, P, S> Shared<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// The one propose path (design §8 Propose), under the write lock:
    /// resolve `parent`, check `closed`, `child = parent.set.child()`,
    /// `applied = child.apply(batch, eff.capacity, eff.low_watermark)` with
    /// `eff = inner.effective`, then, holding the parent's chain mutex only
    /// while computing the plan, `speculator.finish(parent.storage_view,
    /// chain, applied.evicted)` (design §9.1): `None` -> inherit the parent's
    /// view; `Plain` -> `StorageView::propose` with the canonical batch and
    /// the metadata; `Speculative` -> `base.propose(ops, metadata)` after the
    /// mutex is released (`base` is an `Arc` clone of the last link). Build
    /// `RootPreimage` from `eff`, compose the root, register the node, bump
    /// `live_handles`.
    fn propose_on(
        self: &Arc<Self>,
        parent: Parent<'_, K, V, C, P, S>,
        batch: Vec<BatchOp<K, V>>,
        metadata: Option<&[u8]>,
    ) -> Result<Proposal<K, V, C, P, S>> {
        todo!("design §8 Propose")
    }

    /// The one commit path (design §8 Commit), used by `Proposal::commit`,
    /// under the write lock: check `poisoned`/`closed`, `chain_to_commit`,
    /// commit each node's `own_storage` oldest-first (the engine commits
    /// speculative links first, design §10; a failure after the first engine
    /// commit of a chain sets `poisoned` and returns the error), `advance`
    /// (which retires the ex-head's chain slot) plus a sweep of
    /// `chain_holders` for dead or orphaned nodes, `speculator.record(path)`
    /// and `Stats` from `node.applied()` per node,
    /// `effective = effective.retarget(capacity.target(&set_stats, height))`.
    /// Returns the committed root and the data round 0 needs; the caller runs
    /// `round0` after this guard is gone (design §9.2).
    fn commit_node(&self, node: &Arc<Node<K, V, C, P, S>>) -> Result<Committed<K, V, C, P, S>> {
        todo!("design §8 Commit, §9.2")
    }

    /// Round 0 of speculation for the head `commit_node` just installed,
    /// outside the tree lock (design §9.2):
    /// `speculator.after_commit(&job.head, job.new_keys, job.effective)`. If a
    /// child committed meanwhile the head's slot is `Retired` and only the
    /// estimate update runs.
    pub(crate) fn round0(&self, job: Round0<K, V, C, P, S>) {
        todo!("design §9.2 round 0")
    }

    /// The one prefetch path (design §9.4). Under one read guard: resolve
    /// `target` exactly as `propose_on` does (`Committed`, or a live proposal
    /// whose chain walk ends at the committed node), clone the `NodeRef`,
    /// snapshot `effective`, and for a node that no longer qualifies retire
    /// its chain slot; if it qualifies and speculation is on, enter an
    /// `InFlightGuard` (returns `None` once `begin_closing` ran: stop) and
    /// reserve a `LinkToken` from the budget, sweeping `chain_holders` first
    /// if the budget is exhausted (one chain mutex at a time, never two); then
    /// **release the guard**. Without any tree lock: forward the keys not in
    /// `node.set` to `storage.prefetch(node.storage_view.as_ref(), ..)`
    /// (counted via `count_prefetched`), and, if the node qualified,
    /// `speculator.on_hint(&node, keys, effective, token)`, which re-checks
    /// the slot (a demotion in between leaves it `Retired`). From its
    /// `HintOutcome`, with no chain mutex held, register the node in
    /// `chain_holders` on `chain_created`. No-op after `close`. Hints are
    /// advisory: engine errors are counted, never returned.
    pub(crate) fn prefetch_on(&self, target: Parent<'_, K, V, C, P, S>, keys: &[K]) {
        todo!("design §9.4")
    }
}

/// The authenticated write set in front of a storage engine. A thin,
/// cloneable handle over the shared state; proposals and revisions hold the
/// same `Arc`.
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
    shared: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Clone for AuthWal<K, V, C, P, S>
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
    /// `storage.revisions() >= local.engine_window::<K, V, S>()` (design §8;
    /// the Go `New` and the benchmark open the engine with that size).
    pub fn new(config: Config, storage: S, policy: P, vc: C) -> Result<Self> {
        todo!("design §3 New")
    }

    /// Replaces the capacity controller (default `Fixed(config.capacity)`).
    /// Consensus-visible: every validator must install the same controller
    /// at the same height (design §6.4, §1 consensus-state rule).
    pub fn set_capacity_controller(&self, controller: Box<dyn CapacityController>) {
        todo!("design §6.4")
    }

    /// Proposes `batch` on the committed state (design §3, §6, §8).
    pub fn propose(&self, batch: Vec<BatchOp<K, V>>) -> Result<Proposal<K, V, C, P, S>> {
        self.propose_with_metadata(batch, None)
    }

    /// `propose` with root-affecting commit metadata (design §3, §7.3).
    pub fn propose_with_metadata(
        &self,
        batch: Vec<BatchOp<K, V>>,
        metadata: Option<&[u8]>,
    ) -> Result<Proposal<K, V, C, P, S>> {
        self.shared.propose_on(Parent::Committed, batch, metadata)
    }

    /// `propose` followed by `commit` (design §3 Update).
    pub fn update(&self, batch: Vec<BatchOp<K, V>>) -> Result<Hash> {
        self.update_with_metadata(batch, None)
    }

    pub fn update_with_metadata(&self, batch: Vec<BatchOp<K, V>>, metadata: Option<&[u8]>) -> Result<Hash> {
        self.propose_with_metadata(batch, metadata)?.commit()
    }

    /// Reads `key` from the committed state: write set first, then storage (design §5).
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("design §5")
    }

    /// Prefetch hint relative to the committed state (design §9.4, R14):
    /// execution is writing `keys`. Warms their trie paths in the engine and
    /// refines the committed node's speculative chain. Advisory; no-op after
    /// `close`.
    pub fn prefetch(&self, keys: &[K]) {
        self.shared.prefetch_on(Parent::Committed, keys)
    }

    /// The single combined commitment of the committed state; `EMPTY_ROOT`
    /// until the first commit (design §7.3).
    pub fn root(&self) -> Hash {
        todo!("design §7.3")
    }

    /// Read-only view at `root`: a live proposal or one of the last K commits
    /// (design §8). `RevisionNotFound` otherwise.
    pub fn revision(&self, root: Hash) -> Result<Revision<K, V, C, P, S>> {
        todo!("design §8")
    }

    /// Revision at the committed root.
    pub fn latest_revision(&self) -> Result<Revision<K, V, C, P, S>> {
        self.revision(self.root())
    }

    /// Pass-through to `StorageEngine::prune` (design §3).
    pub fn prune(&self) -> Result<()> {
        self.shared.storage.prune()
    }

    /// Mirrors qmdb-ffi's `Close` (design §3): `ActiveHandles` if
    /// `live_handles != 0` (the FFI layer waits on a deadline before calling
    /// this); otherwise `speculator.begin_closing()`, waits for
    /// `links_in_flight() == 0`, retires every chain (via `chain_holders`),
    /// drops the K-ring's engine views, closes the engine and marks the
    /// instance closed. Idempotent and allowed on a poisoned instance.
    pub fn close(&self) -> Result<()> {
        todo!(
            "live_handles check; begin_closing; wait in_flight == 0; retire chains; history.clear(); storage.close(); closed = true"
        )
    }

    pub fn stats(&self) -> Stats {
        todo!()
    }

    pub fn config(&self) -> &Config {
        &self.shared.config
    }
}
