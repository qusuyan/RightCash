//! Speculative eviction (design §9): predicted victims are sent to the storage
//! engine early as a chain of proposals attached to the parent node, refined
//! from prefetch hints during block execution, and the final proposal reverts
//! every misprediction so the storage state equals the plain path's. On a
//! state-function engine (firewood, `MemoryStorage`) nothing here is
//! consensus-visible; `AuthWal::new` rejects speculation on other engines
//! (design §9.3).
//!
//! The [`Speculator`] lives in `Shared`, outside the tree lock, and keeps its
//! mutable state in atomics so the hint path can run without the tree lock;
//! each chain lives on its node behind the node's own mutex, in a tri-state
//! [`ChainSlot`] whose `Retired` state is sticky. Budget units are owned
//! [`LinkToken`]s, refunded exactly once when dropped (design §9.2 "Budget").

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use crate::Effective;
use crate::policy::EvictionPolicy;
use crate::proposal::{Node, StorageView};
use crate::set::{Entry, Evicted, WriteSet};
use crate::storage::StorageEngine;
use crate::types::{BatchOp, Key, Value};
use crate::vc::VectorCommitment;

/// Predicted victims with the parent's current entries, canonical (key)
/// order. Produced by `WriteSet::pred` / `WriteSet::pred_excluding`.
pub type Pred<K, V> = Vec<(K, Entry<V>)>;

/// The one encoding of entries as engine ops (`Put` for values, `Delete` for
/// tombstones, in the given order), used for `Evicted::into_batch`, links and
/// the final proposal.
pub fn entries_to_ops<K: Key, V: Value>(entries: &[(K, Entry<V>)]) -> Vec<BatchOp<K, V>> {
    todo!()
}

/// Local knobs (design §9.2). None of them is consensus-visible. The defaults
/// in `Local` are provisional until the milestone-3 sweep.
#[derive(Clone, Copy, Debug)]
pub struct SpeculationConfig {
    pub enabled: bool,
    /// Maximum links per parent node.
    pub rounds: u32,
    /// Minimum number of not-yet-sent predicted entries before a link is sent (>= 1).
    pub min_link: usize,
    /// Instance-wide budget of live links across all chains (memory bound).
    pub max_links_total: usize,
    /// Pessimistic prediction (design §9.2, R15): extra predicted victims
    /// added to `E` once the set is over capacity, so a block's later writes
    /// rarely need another link. Costs reverts in `F`.
    pub margin: usize,
}

/// Instance-wide link accounting shared by the [`Speculator`], every
/// [`LinkToken`] and every [`SpeculativeChain`] (design §9.2 "Budget", §9.5).
#[derive(Default)]
pub struct LinkBudget {
    /// Reserved units: one per live [`LinkToken`] (completed links, held by
    /// their chains, and in-flight links, held by their hint tasks).
    pub live: AtomicUsize,
    /// Hint tasks between the closing check and the end of their engine call;
    /// counted under the tree read lock in which `closing` is read, so
    /// `close` (which sets `closing` then waits for zero) cannot miss one.
    pub in_flight: AtomicUsize,
    /// Set by `close` (`SeqCst`) before it waits for `in_flight == 0`.
    pub closing: AtomicBool,
    /// Links of chains dropped before any child built `F` on them.
    pub dropped_unused: AtomicU64,
}

impl LinkBudget {
    /// Reserves one unit against `max`, as an owned token; `None` leaves the
    /// budget untouched. Called in the resolve step under the tree read lock,
    /// before any chain mutex (design §9.2 "Budget").
    pub fn try_reserve(self: &Arc<Self>, max: usize) -> Option<LinkToken> {
        todo!("compare-and-swap loop on `live`, then a LinkToken holding a clone of self")
    }
}

/// One reserved unit of the link budget. Refunded exactly once, in `Drop`:
/// the hint task holds it from planning through the engine call, `append`
/// moves it into the chain next to the link, and the chain drops it with the
/// link. Nothing else ever touches `live`.
pub struct LinkToken {
    budget: Arc<LinkBudget>,
}

impl Drop for LinkToken {
    fn drop(&mut self) {
        let prev = self.budget.live.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev >= 1, "link budget refunded more than reserved");
    }
}

/// Counts a hint task as in flight for the duration of its engine work
/// (design §3 Close). Created under the tree read lock right after the
/// closing flag was read; `Drop` decrements.
pub struct InFlightGuard {
    budget: Arc<LinkBudget>,
}

impl InFlightGuard {
    /// `None` if the instance is closing (the hint then does nothing).
    pub fn enter(budget: &Arc<LinkBudget>) -> Option<Self> {
        budget.in_flight.fetch_add(1, Ordering::SeqCst);
        if budget.closing.load(Ordering::SeqCst) {
            budget.in_flight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Self { budget: budget.clone() })
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.budget.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The link a chain is building (design §9.2, "One in-flight link per
/// chain"): its planned entries and the tail it was planned on, so `append`
/// can refuse it if the tail moved and planning can exclude its keys. The
/// budget token travels with the hint task, not with this record.
pub struct InFlight<K, V> {
    pub entries: Pred<K, V>,
    /// `links.len()` when planned: the link's parent is the tail at that time.
    pub parent_len: usize,
}

/// The chain slot on a node (`Node::speculation`). `Retired` is terminal and
/// sticky: set when the node stops qualifying as a parent (`advance` demotes
/// the head; an orphan check fails), checked at plan and at append, so a hint
/// that resolved the node just before its demotion, or a stale round 0, can
/// never recreate a chain on it (design §9.2).
pub enum ChainSlot<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    Absent,
    Live(SpeculativeChain<K, V, S>),
    Retired,
}

impl<K, V, S> ChainSlot<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    /// Retires the slot, dropping any chain (its links' tokens refund themselves).
    pub fn retire(&mut self) {
        *self = Self::Retired;
    }

    pub fn live(&self) -> Option<&SpeculativeChain<K, V, S>> {
        match self {
            Self::Live(c) => Some(c),
            _ => None,
        }
    }

    pub fn live_mut(&mut self) -> Option<&mut SpeculativeChain<K, V, S>> {
        match self {
            Self::Live(c) => Some(c),
            _ => None,
        }
    }
}

/// Speculative chain attached to a node acting as a parent (design §9.2):
/// links `L1 -> ... -> Lk` (each with its budget token) proposed on the
/// node's pinned storage view, the union of what they carried, and the hints
/// seen for this node. Shared by every child proposed on the node; never
/// consumed by `Propose`; dropped when the node stops qualifying or is freed.
pub struct SpeculativeChain<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    links: Vec<(Arc<S::Proposal>, LinkToken)>,
    /// Union of `(key, entry)` pairs the links carried.
    sent: BTreeMap<K, Entry<V>>,
    /// Keys hinted for this node so far (design §9.4).
    hinted: BTreeSet<K>,
    /// Hinted keys that are not in the parent's set (predicted insertions).
    hinted_new: usize,
    /// The one link being proposed right now, if any (design §9.2).
    in_flight: Option<InFlight<K, V>>,
    /// Set when a child built its final proposal on the chain.
    used: bool,
    budget: Arc<LinkBudget>,
}

impl<K, V, S> SpeculativeChain<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    pub fn new(budget: Arc<LinkBudget>) -> Self {
        Self {
            links: Vec::new(),
            sent: BTreeMap::new(),
            hinted: BTreeSet::new(),
            hinted_new: 0,
            in_flight: None,
            used: false,
            budget,
        }
    }

    /// Last link, the base of a child's final proposal; `None` before round 0.
    pub fn last(&self) -> Option<&Arc<S::Proposal>> {
        self.links.last().map(|(link, _)| link)
    }

    pub fn rounds(&self) -> u32 {
        self.links.len() as u32
    }

    pub fn is_used(&self) -> bool {
        self.used
    }

    pub fn sent_ops(&self) -> usize {
        self.sent.len()
    }

    pub fn in_flight(&self) -> Option<&InFlight<K, V>> {
        self.in_flight.as_ref()
    }

    /// Phase 1 of a link (design §9.2): records the planned entries and the
    /// current tail as its parent. Returns `false` if a link is already in
    /// flight (the hint then only records its keys and warms storage, and its
    /// token drops).
    pub fn begin_link(&mut self, entries: Pred<K, V>) -> bool {
        todo!("if in_flight.is_some() then false else set InFlight with parent_len = links.len()")
    }

    /// Phase 3 after an engine failure, slot still `Live`: clears `in_flight`.
    /// The task's token refunds itself when dropped. Not needed for a
    /// `Retired` slot: there is no chain to call it on.
    pub fn abort_link(&mut self) {
        todo!("in_flight = None")
    }

    /// Phase 3 of a link (design §9.2): appends the completed `link` with its
    /// token if the tail is still the parent it was planned on
    /// (`in_flight.parent_len == links.len()`), moving the in-flight entries
    /// into `sent`; otherwise drops both (the token refunds itself). Returns
    /// whether it was appended. Keeps the chain linear in the engine, so
    /// `sent` is exactly what the tail's ancestry wrote.
    pub fn append(&mut self, link: Arc<S::Proposal>, token: LinkToken) -> bool {
        todo!("check parent_len; links.push((link, token)); sent.extend(in_flight.entries); in_flight = None")
    }

    /// Records hints (design §9.2). Only keys not already hinted count, so
    /// duplicate hints (design §9.3 test 2) do not inflate `hinted_new`;
    /// `in_set` tells whether a key is in the parent's set.
    pub fn note_hints(&mut self, keys: &[K], in_set: impl Fn(&K) -> bool) {
        todo!("for each key not yet hinted: hinted.insert; count hinted_new when !in_set(k)")
    }
}

impl<K, V, S> Drop for SpeculativeChain<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    /// Counts the links unused if no child ever built on the chain (design
    /// §9.5 `links_dropped_unused`); the tokens refund themselves.
    fn drop(&mut self) {
        if !self.used {
            self.budget
                .dropped_unused
                .fetch_add(self.links.len() as u64, Ordering::Relaxed);
        }
    }
}

/// What `on_hint` tells its caller, with no chain mutex held.
#[derive(Clone, Copy, Debug, Default)]
pub struct HintOutcome {
    /// The node got its first chain: register it in `chain_holders`.
    pub chain_created: bool,
    /// A link was proposed and appended.
    pub link_sent: bool,
}

/// How a node's eviction reaches storage (design §9.1 table).
pub enum FinalPlan<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    /// No eviction, no prefix delete, and no completed link on the parent's
    /// chain: inherit the parent's pinned view. (With links, an empty batch
    /// still needs `F` for the reverts.)
    None,
    /// One proposal with the canonical batch on the parent's pinned view.
    Plain(Evicted<K, V>),
    /// Final proposal `F` on the chain's last completed link: reverts, then
    /// prefix deletes, then the batch (design §9.1 ordering).
    Speculative {
        base: Arc<S::Proposal>,
        ops: Vec<BatchOp<K, V>>,
        links: u32,
        /// Pairs omitted from `F` because a link already carried them.
        reused_ops: usize,
        /// Reverts emitted for predicted keys that were not evicted.
        reverted_ops: usize,
    },
}

/// Recorded on the node at propose time and counted at commit (design §9.5),
/// so losing siblings are not counted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoragePath {
    None,
    Plain,
    Speculative {
        links: u32,
        reused_ops: usize,
        reverted_ops: usize,
    },
}

/// Snapshot of the counters (design §9.5).
#[derive(Clone, Copy, Debug, Default)]
pub struct SpeculationStats {
    // per committed node
    pub plain: u64,
    pub speculative: u64,
    pub links: u64,
    pub reused_ops: u64,
    pub reverted_ops: u64,
    // per instance
    pub links_sent: u64,
    pub links_live: u64,
    pub links_dropped_unused: u64,
    pub link_failures: u64,
    /// `finish` fell back to the plain path after a revert read failed.
    pub fallbacks: u64,
    pub hint_keys: u64,
    pub hint_new_keys: u64,
    pub prefetched_keys: u64,
    /// Engine `prefetch` calls that returned an error (the warming is advisory).
    pub prefetch_failures: u64,
}

/// Lock-free counters behind [`SpeculationStats`], so the hint path (no tree
/// lock) and the commit path (write lock) can both record.
#[derive(Default)]
struct Counters {
    plain: AtomicU64,
    speculative: AtomicU64,
    links: AtomicU64,
    reused_ops: AtomicU64,
    reverted_ops: AtomicU64,
    links_sent: AtomicU64,
    link_failures: AtomicU64,
    fallbacks: AtomicU64,
    hint_keys: AtomicU64,
    hint_new_keys: AtomicU64,
    prefetched_keys: AtomicU64,
    prefetch_failures: AtomicU64,
}

/// Drives speculative chains (design §9.2) and prefetch hints (design §9.4).
/// Lives in `Shared`; holds only local heuristics and statistics in atomics
/// (`&self` everywhere, so hints on different nodes never serialise on it).
/// Chains live on their nodes and share the [`LinkBudget`].
pub struct Speculator {
    config: SpeculationConfig,
    /// Running estimate of new keys per block, for round 0 (design §12.5).
    estimate_new_keys: AtomicUsize,
    budget: Arc<LinkBudget>,
    counters: Counters,
}

impl Speculator {
    pub fn new(config: SpeculationConfig) -> Self {
        Self {
            config,
            estimate_new_keys: AtomicUsize::new(0),
            budget: Arc::new(LinkBudget::default()),
            counters: Counters::default(),
        }
    }

    pub fn config(&self) -> SpeculationConfig {
        self.config
    }

    /// Shared with every chain and token created by this speculator. The
    /// caller of `on_hint` reserves a token from it in the resolve step and
    /// enters an `InFlightGuard` there (design §9.2 "Budget").
    pub fn budget(&self) -> &Arc<LinkBudget> {
        &self.budget
    }

    /// Pure planning step (design §9.2), unit-testable without an engine:
    /// given the parent's set, the chain so far and the effective parameters,
    /// the entries of the next link, or `None` when a link is already in
    /// flight, fewer than `min_link` new predictions exist, or `rounds` is
    /// exhausted. (The budget was reserved by the caller before any mutex.)
    ///
    /// ```text
    /// new      = max(chain.hinted_new, estimate_new_keys)     (estimate is a floor)
    /// expected = set.len() + new
    /// E        = expected > capacity ? expected - low_watermark + margin : 0
    /// pred     = set.pred_excluding(E, chain.hinted)
    /// link     = pred \ (chain.sent ∪ chain.in_flight)   (key-wise, key order)
    /// ```
    pub fn plan_link<K, V, C, P, S>(
        &self,
        set: &WriteSet<K, V, C, P>,
        chain: &SpeculativeChain<K, V, S>,
        eff: Effective,
    ) -> Option<Pred<K, V>>
    where
        K: Key,
        V: Value,
        C: VectorCommitment<K, V>,
        P: EvictionPolicy<K>,
        S: StorageEngine<K, V>,
    {
        todo!("design §9.2")
    }

    /// A prefetch hint for `keys` on `parent`, called with **no tree lock
    /// held**: the caller resolved `parent` under a read guard, checked that
    /// it may carry a chain, read the closing flag and entered the
    /// `InFlightGuard`, reserved `token` (or not, if the budget was exhausted,
    /// in which case it swept the registry), cloned the `NodeRef`, snapshotted
    /// `eff`, and released the guard. Three phases so engine hashing never
    /// runs under a lock `Propose` waits for:
    /// 1. under `parent.speculation()`: bail if `Retired`; create the chain if
    ///    `Absent`; `note_hints`; stop if a link is in flight or `token` is
    ///    `None` (the hint is recorded; the caller still warms storage);
    ///    `plan_link`; `begin_link`; remember the base (`last()` or the
    ///    parent's pinned view) and the entries; release;
    /// 2. outside any lock: propose the link on the base with
    ///    `entries_to_ops`, no metadata. For the first link of a head's chain,
    ///    which goes through the engine on the committed view, check
    ///    `engine.root() == base root` before and after the call and treat a
    ///    mismatch as a failure (design §9.2 "Engine root check");
    /// 3. under the mutex again: if the slot is still `Live`, `append(link,
    ///    token)` (which refuses, dropping both, if the tail moved) or, on an
    ///    engine error, `abort_link` and count `link_failures`; if the slot is
    ///    `Retired`, drop link and token.
    ///
    /// Errors are never returned: hints are advisory. The outcome lets the
    /// caller register the node as a chain holder.
    pub fn on_hint<K, V, C, P, S>(
        &self,
        engine: &S,
        parent: &Node<K, V, C, P, S>,
        keys: &[K],
        eff: Effective,
        token: Option<LinkToken>,
    ) -> HintOutcome
    where
        K: Key,
        V: Value,
        C: VectorCommitment<K, V>,
        P: EvictionPolicy<K>,
        S: StorageEngine<K, V>,
    {
        todo!("design §9.2")
    }

    /// Round 0 for the newly committed node (design §9.2), called by
    /// `Proposal::commit` **after** `commit_node` has released the tree lock,
    /// with the head it committed (not whatever is head now): updates
    /// `estimate_new_keys` from `new_keys_in_committed_batch`, then, unless
    /// the slot is already `Retired` (a child committed meanwhile), reserves a
    /// token and enters an `InFlightGuard` itself (no tree lock is needed for
    /// either) and runs the same three phases as `on_hint` with no hints.
    /// Engine hashing on the accept path, but outside every lock.
    pub fn after_commit<K, V, C, P, S>(
        &self,
        engine: &S,
        committed: &Node<K, V, C, P, S>,
        new_keys_in_committed_batch: usize,
        eff: Effective,
    ) -> HintOutcome
    where
        K: Key,
        V: Value,
        C: VectorCommitment<K, V>,
        P: EvictionPolicy<K>,
        S: StorageEngine<K, V>,
    {
        todo!("design §9.2 round 0")
    }

    /// Design §9.1 at `Propose`, with the parent's chain mutex held by the
    /// caller: with no live chain or no completed link, the plain path (or
    /// `None` if nothing to send); otherwise `F`'s ops in order: reverts for
    /// `sent \ V` (key-wise; old value read through `parent_view`, `Delete`
    /// if absent), the batch's prefix deletes, then `V` omitting pairs
    /// identical in `sent` unless the batch has a prefix delete. Marks the
    /// chain used. A failed revert read degrades to `Plain` (the plain path is
    /// always available and correct, design §10 item 5) and counts
    /// `fallbacks`; it never fails the block.
    pub fn finish<K, V, S>(
        &self,
        parent_view: &StorageView<K, V, S>,
        chain: Option<&mut SpeculativeChain<K, V, S>>,
        evicted: Evicted<K, V>,
    ) -> FinalPlan<K, V, S>
    where
        K: Key,
        V: Value,
        S: StorageEngine<K, V>,
    {
        todo!("design §9.1")
    }

    /// Design §9.5: counts a committed node's path.
    pub fn record(&self, path: StoragePath) {
        todo!()
    }

    /// Live links (completed + in flight) across all chains.
    pub fn links_live(&self) -> usize {
        self.budget.live.load(Ordering::Acquire)
    }

    /// Hint tasks currently between the closing check and the end of their
    /// engine work; `close` waits for zero.
    pub fn links_in_flight(&self) -> usize {
        self.budget.in_flight.load(Ordering::SeqCst)
    }

    /// `close` step 1 (design §3): after this, `InFlightGuard::enter` refuses,
    /// so no new engine work starts; `close` then waits for the count to
    /// reach zero.
    pub fn begin_closing(&self) {
        self.budget.closing.store(true, Ordering::SeqCst);
    }

    pub fn count_prefetched(&self, keys: usize, failed: bool) {
        self.counters.prefetched_keys.fetch_add(keys as u64, Ordering::Relaxed);
        if failed {
            self.counters.prefetch_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn stats(&self) -> SpeculationStats {
        todo!("snapshot of `counters` and `budget`")
    }
}
