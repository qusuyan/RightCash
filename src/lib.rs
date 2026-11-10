//! `right_cash`: an authenticated, bounded, in-memory set of recent writes that
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
//! | [`speculate`] | §9     | the worker: a channel of requests, the thread that builds steps, `submit`/`finish`/`block_end`/`reset` |
//! | [`timings`]   | §9.5, §11 | per-call phase timings of `propose` and `commit`, read by the benchmark's `--breakdown` |
//! | `persist`     | §12.6 | clean-shutdown checkpoint encoding, validation, and atomic publication |
//!
//! Threading (design §3, R19): [`RightCash`] is the **writer**; every operation
//! that changes state takes `&mut self`, so the type system serialises them.
//! [`Reader`] and [`proposal::Handle`] are the `Send + Sync` read side. The
//! shared mutable structures are the registry behind one mutex and a
//! snapshot's policy behind its own; the worker is reached over a channel
//! (design §10 lists every hold).

pub mod lattice;
mod persist;
pub mod policy;
pub mod proposal;
pub mod root;
pub mod set;
pub mod speculate;
pub mod storage;
pub mod timings;
pub mod types;
pub mod vc;

#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use parking_lot::Mutex;

use crate::policy::{EvictionPolicy, Prediction};
use crate::proposal::{Handle, Node, NodeRef, Registry, StorageView, WeakNode};
use crate::set::{Entries, Snapshot};
use crate::speculate::{Counters, SpecOp, SpeculationStats, Worker, entries_to_ops};
use crate::storage::{StorageEngine, StorageProposal};
use crate::timings::{CommitTimings, ProposeTimings};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash, Key, Result, Value};
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
    /// Clean-shutdown checkpoint file. `close` saves the latest committed
    /// snapshot here; `RightCash::recover` reads it with matching storage.
    /// None disables checkpointing (the default for in-memory/benchmark use).
    pub snapshot_path: Option<PathBuf>,
    /// Committed revisions kept for `revision()` (design §8); at least 2
    /// (`Config::validate`).
    pub revisions: usize,
    /// Speculative eviction (design §9): send predicted victims to the engine
    /// early. Requires `S::ROOT_IS_STATE_FUNCTION`.
    pub speculate: bool,
    /// Maximum storage proposals on one chain (design §9.2), including
    /// the final refinement, however many blocks the chain spans (R35); sizes the
    /// engine window (`engine_window`).
    pub speculation_rounds: u32,
    /// Minimum distinct queued keys before the worker builds a step ahead of `finish`
    /// (design §9.2).
    pub speculation_min_step: usize,
    /// Extra predicted victims beyond the strict count once a hint's
    /// insertions reach the high watermark (design §9.2, R15: pessimistic
    /// prediction); handed to the policy's `hint_writes`.
    pub speculation_margin: usize,
}

impl Default for Local {
    fn default() -> Self {
        // Rounds remain provisional; min_step follows the focused Zipf comparison (design §9.2, §14).
        Self {
            snapshot_path: None,
            revisions: 128,
            speculate: false,
            speculation_rounds: 3,
            speculation_min_step: 256,
            speculation_margin: 0,
        }
    }
}

impl Local {
    /// The engine window these settings need (design §8): `revisions * rounds`
    /// when speculating on an engine that commits chained proposals separately (`S::COMMITS_CHAIN_LINKS_SEPARATELY`), else `revisions`.
    /// One formula for `RightCash::new`'s check, the benchmark's engine config
    /// and the adapter's `New` (milestone 4).
    pub fn engine_window(&self, commits_chain_links_separately: bool) -> usize {
        if self.speculate && commits_chain_links_separately {
            self.revisions * self.speculation_rounds as usize
        } else {
            self.revisions
        }
    }
}

/// Configuration of an [`RightCash`] instance (design §3). The split lets the
/// type system keep local settings out of the root preimage, whose terms
/// come from the scheme, the policy, the engine and the node (its capacity
/// among them), never from `Local`.
#[derive(Clone, Debug)]
pub struct Config {
    pub consensus: Consensus,
    pub local: Local,
}

impl Config {
    /// Return `Ok(())` if capacity is at least 1 and revisions at least 2,
    /// plus positive rounds and min-step when speculation is enabled.
    /// Otherwise return `InvalidConfig`. Does not mutate configuration or
    /// validate the supplied snapshot, policy, or storage engine; `new` does
    /// those checks separately.
    pub fn validate(&self) -> Result<()> {
        if let Some(path) = &self.local.snapshot_path
            && path.file_name().is_none()
        {
            return Err(Error::InvalidConfig("snapshot_path must name a file".into()));
        }
        if self.consensus.capacity == 0 {
            return Err(Error::InvalidConfig("capacity must be at least 1".into()));
        }
        if self.local.revisions < 2 {
            return Err(Error::InvalidConfig("revisions must be at least 2 (design §8)".into()));
        }
        if self.local.speculate {
            if self.local.speculation_rounds == 0 {
                return Err(Error::InvalidConfig("speculation_rounds must be at least 1".into()));
            }
            if self.local.speculation_min_step == 0 {
                return Err(Error::InvalidConfig("speculation_min_step must be at least 1".into()));
            }
        }
        Ok(())
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
    /// Logical reconciliation ops: evictions not already predicted, plus
    /// withdrawals. Excludes queued hint ops merged into finalization;
    /// `last_propose_timings().storage_ops` measures the actual final batch.
    /// Derived as `total_evictions.saturating_sub(reused_ops) + reverted_ops`;
    /// saturation handles counters sampled between independent updates.
    pub total_storage_ops: u64,
    /// Evicting proposal outcomes, `plain + speculative`. Includes reuse
    /// of an exact worker view; worker `steps_built` counts materializations.
    pub storage_proposals: u64,
    /// All `Put` ops applied.
    pub puts: u64,
    /// Puts that replaced an entry already in the set (the layer's win; the
    /// `zipf` benchmark's hit rate is `in_place_updates / puts`).
    pub in_place_updates: u64,
    pub speculation: SpeculationStats,
}

impl Stats {
    /// The counters' growth since `base` (saturating: the counters are
    /// bumped at different moments); `height` and `occupancy` are this
    /// reading's. The benchmark's warm-up subtraction (design §11).
    pub fn since(&self, base: &Stats) -> Stats {
        Stats {
            height: self.height,
            occupancy: self.occupancy,
            total_evictions: self.total_evictions.saturating_sub(base.total_evictions),
            total_storage_ops: self.total_storage_ops.saturating_sub(base.total_storage_ops),
            storage_proposals: self.storage_proposals.saturating_sub(base.storage_proposals),
            puts: self.puts.saturating_sub(base.puts),
            in_place_updates: self.in_place_updates.saturating_sub(base.in_place_updates),
            speculation: self.speculation.since(&base.speculation),
        }
    }
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
/// (design §8, §10): the counters (set and speculation), atomics; the
/// registry behind one mutex. No configuration (the writer owns it), no
/// engine (the writer owns it, R19), no handle count and no closed flag
/// (the writer keeps its own; reads never check one, R20: the Go layer
/// counts its outstanding handles; the engine refuses to close while a live
/// storage proposal is held). Handles hold only their node.
pub(crate) struct Shared<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    pub(crate) set: SetCounters,
    /// Shared with the worker thread by `Arc`: all it needs of this state.
    pub(crate) counters: Arc<Counters>,
    /// The registry (design §8), one of the locks (design §10): held for a
    /// few pointer operations or a scan of at most K roots by the writer
    /// (resolve the parent, register, check at commit, install, resolve at
    /// prefetch) and by readers (resolve the head or a root), never across
    /// an engine call, a snapshot copy or a drop.
    pub(crate) registry: Mutex<Registry<K, V, C, P, S>>,
}

/// The read side of an instance (design §3 threading contract): `Clone +
/// Send + Sync`, for the RPC and mempool threads. Each call takes the
/// registry lock briefly to resolve the head or a root, then releases it
/// before reading values. Storage adapters may take their own locks.
/// The writer holds one too.
pub struct Reader<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    shared: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Clone for Reader<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
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
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Return the owned value for `key`, or `None` if absent/deleted, from the
    /// head selected at the start of this call. Clone that node under the
    /// registry lock, then release the lock before reading. Storage errors
    /// propagate; a concurrent commit does not change the selected snapshot.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let head = self.shared.registry.lock().head().clone();
        head.get(key)
    }

    /// Return the committed head's combined root under one registry hold.
    /// Initially `EMPTY_ROOT` for an empty height-zero set, or the supplied
    /// snapshot's combined root when initialized with existing entries.
    pub fn root(&self) -> Hash {
        self.shared.registry.lock().head().root()
    }

    /// Read-only handle at `root`: one of the last K commits or a live
    /// proposal (design §8), resolved under one registry hold
    /// (`Registry::lookup`: the ring, then live proposals). `RevisionNotFound`
    /// otherwise.
    pub fn revision(&self, root: Hash) -> Result<Handle<K, V, C, P, S>> {
        self.shared
            .registry
            .lock()
            .lookup(root)
            .map(Handle::new)
            .ok_or(Error::RevisionNotFound)
    }

    /// A handle to the committed state, one registry hold.
    pub fn latest_revision(&self) -> Handle<K, V, C, P, S> {
        Handle::new(self.shared.registry.lock().head().clone())
    }

    /// One registry hold reads the head's height and occupancy; every
    /// counter is an atomic loaded outside it (`set`, then
    /// `counters.snapshot()`); `storage_proposals` and `total_storage_ops`
    /// are derived from them (design §9.5).
    pub fn stats(&self) -> Stats {
        let (height, occupancy) = {
            let registry = self.shared.registry.lock();
            let head = registry.head();
            (head.height(), head.snapshot().len())
        };
        let set = &self.shared.set;
        let total_evictions = set.total_evictions.load(Ordering::Relaxed);
        let speculation = self.shared.counters.snapshot();
        Stats {
            height,
            occupancy,
            total_evictions,
            total_storage_ops: total_evictions.saturating_sub(speculation.reused_ops) + speculation.reverted_ops,
            storage_proposals: speculation.plain + speculation.speculative,
            puts: set.puts.load(Ordering::Relaxed),
            in_place_updates: set.in_place_updates.load(Ordering::Relaxed),
            speculation,
        }
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
pub struct RightCash<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
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
    /// `finish`, `block_end`, `reset`; spawned by `new` only with speculation
    /// enabled, stopped and joined by `close`. The plain path has no worker.
    worker: Option<Worker<K, V, S>>,
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
    /// Set by a failed mid-chain engine commit (design §9.2), or a failed
    /// storage flush during checkpointing. Prevents further writes and
    /// publication of a checkpoint whose storage durability is uncertain.
    poisoned: Option<String>,
    /// Set by `close`: the writer's operations report `Closed` from then on
    /// and hints are no-ops; reads never check it (R20). The worker needs
    /// no flag: dropping its channel's sender ends it (design §9.2).
    closed: bool,
    /// Engine closure and optional checkpoint publication both succeeded.
    close_complete: bool,
    /// Where the last `propose` and the last successful `commit` spent their
    /// time (design §9.5): read by `last_propose_timings` and
    /// `last_commit_timings`.
    last_propose: ProposeTimings,
    last_commit: CommitTimings,
}

impl<K, V, C, P, S> RightCash<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Opens the supplied set in front of fresh `storage` (design §3 New).
    /// The set owns its entries, policy, commitment scheme and capacity;
    /// its capacity must match `config.consensus.capacity`. A populated set
    /// starts at its own height and authenticated root; an empty height-zero
    /// set retains `EMPTY_ROOT`. Use `recover` to reopen an existing checkpoint.
    /// Validates configuration, set policy, engine freshness and revision
    /// window. The target starts dead, with no speculative proposals.
    pub fn new(config: Config, storage: S, snapshot: Snapshot<K, V, C, P>) -> Result<Self> {
        if storage.root() != EMPTY_ROOT {
            return Err(Error::EngineNotFresh);
        }
        if let Some(path) = &config.local.snapshot_path {
            if path.try_exists()? {
                return Err(Error::InvalidConfig(
                    "snapshot file already exists; use recover or a new path".into(),
                ));
            }
            // Refuse unsupported policies at startup, not at shutdown.
            snapshot.encode_policy()?;
        }
        Self::from_snapshot(config, storage, snapshot, root::metadata_hash(None))
    }

    /// Recover the latest committed state from `config.local.snapshot_path`.
    /// Reopen the matching persistent engine first, without truncating it.
    /// Consensus capacity, scheme, policy parameters, and storage root must
    /// match the checkpoint; local settings may change. Only the latest
    /// revision is restored. Counters, hints, and worker proposals start fresh.
    ///
    /// All built-in byte containers support the decoding bounds below;
    /// fixed-size keys/values reject incorrect lengths without panicking.
    /// Missing/corrupt files return an error, never silently create an empty DB.
    pub fn recover(config: Config, storage: S, vc: C, policy: P) -> Result<Self>
    where
        K: TryFrom<Vec<u8>>,
        V: TryFrom<Vec<u8>>,
    {
        config.validate()?;
        let path = config
            .local
            .snapshot_path
            .as_ref()
            .ok_or_else(|| Error::InvalidConfig("recover requires snapshot_path".into()))?;
        let (snapshot, metadata_hash) = persist::load(
            path,
            vc,
            policy,
            config.consensus.capacity,
            S::ENGINE_ID,
            storage.root(),
        )?;
        Self::from_snapshot(config, storage, snapshot, metadata_hash)
    }

    fn from_snapshot(
        config: Config,
        storage: S,
        snapshot: Snapshot<K, V, C, P>,
        metadata_hash: types::Digest,
    ) -> Result<Self> {
        config.validate()?;
        snapshot.validate(config.consensus.capacity)?;
        if config.local.speculate && !S::ROOT_IS_STATE_FUNCTION {
            return Err(Error::InvalidConfig(
                "speculative eviction requires an engine whose root is a function of state (design §9.3)".into(),
            ));
        }
        let window = config.local.engine_window(S::COMMITS_CHAIN_LINKS_SEPARATELY);
        if storage.revisions() < window {
            return Err(Error::InvalidConfig(format!(
                "the engine keeps {} revisions, these settings need {window} (design §8)",
                storage.revisions()
            )));
        }
        let terms = snapshot.terms(S::ENGINE_ID);
        // A supplied set has no engine precomputation associated with it.
        snapshot.reset_hints();
        let head = Arc::new(storage.head()?);
        let initial_root = terms.combine(
            snapshot.capacity(),
            snapshot.height(),
            snapshot.commitment(),
            snapshot.policy_digest(),
            head.root(),
            metadata_hash,
        );
        let genesis = Node::new(snapshot, Weak::new(), head.clone(), 0, initial_root, metadata_hash);
        // The worker starts on this node's view, so its first submissions need
        // no reset. Plain instances have no speculation target.
        let target = if config.local.speculate {
            Arc::downgrade(&genesis)
        } else {
            Weak::new()
        };
        let shared = Arc::new(Shared {
            set: SetCounters::default(),
            counters: Arc::new(Counters::default()),
            registry: Mutex::new(Registry::new(genesis, config.local.revisions)),
        });
        let worker = if config.local.speculate {
            Some(
                Worker::spawn(
                    head,
                    shared.counters.clone(),
                    config.local.speculation_min_step,
                    config.local.speculation_rounds,
                )
                .map_err(|e| Error::WorkerUnavailable(e.to_string()))?,
            )
        } else {
            None
        };
        Ok(Self {
            reader: Reader { shared },
            worker,
            target,
            storage,
            config,
            terms,
            poisoned: None,
            closed: false,
            close_complete: false,
            last_propose: ProposeTimings::default(),
            last_commit: CommitTimings::default(),
        })
    }

    /// Return timings from the last successful `propose`, for the benchmark's
    /// `--breakdown`. All zero before the first success; failed calls leave the
    /// previous reading unchanged.
    pub fn last_propose_timings(&self) -> ProposeTimings {
        self.last_propose
    }

    /// Where the last successful `commit` spent its time (design §9.5).
    pub fn last_commit_timings(&self) -> CommitTimings {
        self.last_commit
    }

    /// The read side (design §3 threading contract): a `Clone + Send + Sync`
    /// handle for other threads.
    pub fn reader(&self) -> Reader<K, V, C, P, S> {
        self.reader.clone()
    }

    /// `Closed` once `close` began, `Poisoned` after a failed mid-chain
    /// commit (design §8, §9.2): the first checks of `propose` and `commit`.
    fn check_open(&self) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        if let Some(reason) = &self.poisoned {
            return Err(Error::Poisoned(reason.clone()));
        }
        Ok(())
    }

    /// Return `(node, head)` for a borrowed optional `view`. `None` selects the
    /// committed head. A handle with the same combined root as the head also
    /// selects the actual head node; any other handle selects its own node.
    /// Both returned values are owning `Arc` clones. Locks the registry only
    /// while resolving them, and does not check whether `node` can be committed.
    fn resolve(&self, view: Option<&Handle<K, V, C, P, S>>) -> Resolved<K, V, C, P, S> {
        let registry = self.reader.shared.registry.lock();
        let head = registry.head().clone();
        let node = match view {
            None => head.clone(),
            Some(handle) if handle.root() == head.root() => head.clone(),
            Some(handle) => handle.node().clone(),
        };
        (node, head)
    }

    /// Build and register an uncommitted child; return its readable handle and
    /// make its combined root available immediately through `Handle::root`.
    ///
    /// - `parent`: borrowed handle to build on; `None` selects the committed head.
    ///   A handle whose root equals the head's is resolved to the actual head.
    /// - `batch`: owned puts/deletes in execution order. Repeated keys are allowed;
    ///   the last operation wins. Prefix deletes are rejected before application.
    /// - `metadata`: optional bytes hashed into the combined root, and forwarded to
    ///   storage if a storage proposal is built. The node does not retain the bytes.
    ///
    /// Builds a fresh snapshot, sends evicted entries to a new storage proposal
    /// (or reuses speculative work), and registers the node for root lookup.
    /// When nothing is evicted, the child inherits the parent's storage view.
    /// Updates proposal counters and `last_propose` on success; speculative worker
    /// work and counters may already have changed if a later storage call fails.
    /// Does not commit storage or advance the committed head.
    ///
    /// Returns `Closed`, `Poisoned`, `PrefixDeleteUnsupported`, or a storage error.
    /// A successful proposal does not guarantee commit eligibility: `commit` still
    /// requires its parent to be the current head.
    pub fn propose(
        &mut self,
        parent: Option<&Handle<K, V, C, P, S>>,
        batch: Vec<BatchOp<K, V>>,
        metadata: Option<&[u8]>,
    ) -> Result<Handle<K, V, C, P, S>> {
        let started = Instant::now();
        self.check_open()?;
        if batch.iter().any(|op| matches!(op, BatchOp::PrefixDelete { .. })) {
            return Err(Error::PrefixDeleteUnsupported);
        }
        let batch_ops = batch.len();
        let (parent, _head) = self.resolve(parent);
        let set::Applied {
            snapshot,
            evicted,
            refinement,
            summary,
            mut timings,
        } = parent.snapshot().apply(batch);
        let evictions = evicted.len();
        timings.batch_ops = batch_ops;
        timings.evicted = evictions;
        let counters = self.reader.shared.counters.clone();

        // Keep predictions provisional until actual eviction. On the target,
        // the worker finalizes evictions; other branches and failures use the
        // plain proposal on the parent's storage view.
        let speculative = self.targets(&parent);
        let keep_chain = speculative && evictions == 0;
        let (view, steps) = if keep_chain {
            // The worker's early writes are still provisional. Keep them off
            // this node's pinned view until a later block actually evicts keys.
            // R35 (design §9.1): the chain is kept and this node becomes its
            // target once built (below). What the block owes the chain goes
            // down as ops, then the block end (no reply) so the worker
            // builds what is pending.
            let worker = self.worker.as_ref().expect("a targeted node has a worker");
            worker.block_end(spec_ops(&snapshot, refinement.evict, refinement.retract));
            counters.chains_kept.fetch_add(1, Ordering::Relaxed);
            (parent.storage_view().clone(), 0)
        } else if speculative && let Some(found) = self.finish_speculation(&evicted, refinement, metadata, &mut timings)
        {
            found
        } else {
            // The plain path (design §9.2): nothing marked is in this node's
            // storage, whatever the parent's marks said.
            snapshot.reset_hints();
            if evicted.is_empty() {
                (parent.storage_view().clone(), 0)
            } else {
                counters.plain.fetch_add(1, Ordering::Relaxed);
                let ops = entries_to_ops(evicted);
                let mark = Instant::now();
                let proposal = parent.storage_view().propose(&ops, metadata)?;
                timings.storage = mark.elapsed();
                timings.storage_ops = ops.len();
                (Arc::new(proposal), 0)
            }
        };

        let mark = Instant::now();
        let metadata_hash = root::metadata_hash(metadata);
        let root = self.terms.combine(
            snapshot.capacity(),
            snapshot.height(),
            snapshot.commitment(),
            snapshot.policy_digest(),
            view.root(),
            metadata_hash,
        );
        let node = Node::new(snapshot, Arc::downgrade(&parent), view, steps, root, metadata_hash);
        if keep_chain {
            self.target = Arc::downgrade(&node);
        } else if speculative {
            // Finalization consumed the parent's chain. Start prediction on
            // this child immediately, including before its parent commits.
            self.retarget(&node);
        }
        let set = &self.reader.shared.set;
        set.puts.fetch_add(summary.puts as u64, Ordering::Relaxed);
        set.in_place_updates
            .fetch_add(summary.in_place_updates as u64, Ordering::Relaxed);
        set.total_evictions.fetch_add(evictions as u64, Ordering::Relaxed);
        self.reader.shared.registry.lock().register(&node);
        timings.rest = mark.elapsed();
        timings.steps = steps;
        timings.total = started.elapsed();
        self.last_propose = timings;
        Ok(Handle::new(node))
    }

    /// Consume `handle` on every outcome and, on success, return its combined
    /// root after installing that node as the new committed head.
    ///
    /// Rejects a closed/poisoned writer, a root already equal to the head's
    /// (`ProposalConsumed`), a dropped parent (`ProposalOrphaned`), or a parent
    /// that is not the current head node (`ParentUncommitted`). This method does
    /// not commit parent right_cash nodes automatically.
    ///
    /// If the node owns a different storage view from the head, commit that view
    /// and its speculative storage ancestors first. On failure, leave the registry
    /// head unchanged; if speculative steps were involved, poison the writer
    /// because some storage ancestors may already have committed.
    ///
    /// On success, advance the registry, release the retired revision outside its
    /// lock, update the speculation target, and record `last_commit`. Registry
    /// locks are never held during storage commit or worker communication.
    pub fn commit(&mut self, handle: Handle<K, V, C, P, S>) -> Result<Hash> {
        let started = Instant::now();
        let node = handle.into_node();
        self.check_open()?;
        let parent = node.parent();
        let owns = {
            let registry = self.reader.shared.registry.lock();
            let head = registry.head();
            if node.root() == head.root() {
                return Err(Error::ProposalConsumed);
            }
            match &parent {
                None => return Err(Error::ProposalOrphaned),
                Some(parent) if !Arc::ptr_eq(parent, head) => return Err(Error::ParentUncommitted),
                Some(_) => {}
            }
            // An inherited view is already committed. Equal root bytes are
            // insufficient: a new no-op storage proposal still needs commit.
            !Arc::ptr_eq(node.storage_view(), head.storage_view())
        };
        drop(parent);
        let mut timings = CommitTimings::default();
        if owns {
            let mark = Instant::now();
            if let Err(e) = node.storage_view().commit() {
                if node.steps() > 0 {
                    // The storage chain can fail after earlier links commit;
                    // the unchanged right_cash head would then lag storage.
                    self.poisoned = Some(e.to_string());
                }
                return Err(e);
            }
            timings.engine = mark.elapsed();
        }
        let mark = Instant::now();
        let retired = self
            .reader
            .shared
            .registry
            .lock()
            .advance(node.clone(), self.config.local.revisions);
        // Release potentially large snapshots and storage handles after the
        // temporary registry guard above has been dropped.
        drop(retired);
        timings.advance = mark.elapsed();
        let mark = Instant::now();
        self.after_commit(&node);
        timings.retarget = mark.elapsed();
        timings.total = started.elapsed();
        self.last_commit = timings;
        Ok(node.root())
    }

    /// Finalize the target's chain on the worker. The final refinement is
    /// merged with queued hints before hashing, so no separate caller-side
    /// proposal is needed. Consumes the chain even on failure: its corrections
    /// belong to this candidate and cannot be reused by a sibling.
    fn finish_speculation(
        &mut self,
        evicted: &Entries<K, V>,
        refinement: Prediction<K>,
        metadata: Option<&[u8]>,
        timings: &mut ProposeTimings,
    ) -> Option<(StorageView<K, V, S>, u32)> {
        self.target = Weak::new();
        let reused = evicted.len() - refinement.evict.len();
        let reverted = refinement.retract.len();
        // Both lists are sorted. Select final values in one pass; values of
        // actual victims are no longer in the child's retained snapshot.
        let mut entries = evicted.iter();
        let mut ops = Vec::with_capacity(refinement.evict.len() + reverted);
        for key in refinement.evict {
            let (_, value) = entries
                .find(|(candidate, _)| candidate == &key)
                .expect("final refinement must name an actual victim");
            ops.push(SpecOp::Evict(key, value.clone()));
        }
        ops.extend(refinement.retract.into_iter().map(SpecOp::Retract));
        let mark = Instant::now();
        let finished = self.worker.as_ref()?.finish(ops, metadata);
        timings.finish = mark.elapsed();
        let counters = &self.reader.shared.counters;
        match finished {
            Ok(finished) => {
                timings.speculative = true;
                timings.reads = finished.timings.reads;
                timings.storage = finished.timings.storage;
                timings.storage_ops = finished.timings.ops;
                // These worker phases ran inside the wait. Keep the public
                // breakdown additive rather than counting them twice.
                timings.finish = timings.finish.saturating_sub(timings.reads + timings.storage);
                counters.speculative.fetch_add(1, Ordering::Relaxed);
                counters.steps.fetch_add(u64::from(finished.steps), Ordering::Relaxed);
                counters.reused_ops.fetch_add(reused as u64, Ordering::Relaxed);
                counters.reverted_ops.fetch_add(reverted as u64, Ordering::Relaxed);
                Some((finished.view, finished.steps))
            }
            Err(error) => {
                log::debug!("right_cash: {error}; taking the plain path");
                counters.fallbacks.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Apply owned `batch` to the committed head and immediately commit it.
    /// `metadata` has the same meaning as in `propose`. Returns the new combined
    /// root or the first error from `propose`/`commit`; their side effects and
    /// failure rules also apply. Does not build on a supplied live proposal.
    pub fn update(&mut self, batch: Vec<BatchOp<K, V>>, metadata: Option<&[u8]>) -> Result<Hash> {
        let handle = self.propose(None, batch, metadata)?;
        self.commit(handle)
    }

    /// Hint that execution based on `view` expects to write borrowed `keys`.
    /// `None` selects the committed head; other views resolve as in `propose`.
    /// Duplicate keys and repeated hints are allowed.
    ///
    /// Returns `()` without waiting for engine work. With speculation disabled
    /// or the writer closed, does nothing. Otherwise it may select a new target:
    /// a child of the current target, or (with no live target) the head or its child.
    /// Hints for other nodes are ignored.
    ///
    /// For the selected target, updates local prediction marks and queues entries
    /// or retractions for the worker. Does not read storage to warm paths directly,
    /// change consensus state, or return worker errors. A policy naming an eviction
    /// candidate absent from the target snapshot causes a panic.
    pub fn prefetch(&mut self, view: Option<&Handle<K, V, C, P, S>>, keys: &[K]) {
        if self.closed || self.worker.is_none() {
            return;
        }
        let (node, head) = self.resolve(view);
        let retarget = match self.target() {
            Some(target) => node.parent_is(&target),
            None => Arc::ptr_eq(&node, &head) || node.parent_is(&head),
        };
        if retarget {
            self.retarget(&node);
        }
        if !self.targets(&node) {
            return;
        }
        self.reader
            .shared
            .counters
            .hint_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        let prediction = node.snapshot().hint_writes(keys, self.config.local.speculation_margin);
        self.worker
            .as_ref()
            .expect("a targeted node has a worker")
            .submit(spec_ops(node.snapshot(), prediction.evict, prediction.retract));
    }

    /// Pass-through to `StorageEngine::prune` (design §3).
    pub fn prune(&mut self) -> Result<()> {
        self.storage.prune()
    }

    /// Stop writer use and request engine closure. Returns `Ok(())` once the
    /// engine closes, or its error (including `ActiveHandles` while uncommitted
    /// storage proposals remain alive).
    ///
    /// Sets `closed` before stopping and joining the worker, then calls the engine.
    /// Even if the engine refuses closure, `propose`, `commit`, and `update` remain
    /// closed and hints remain no-ops; drop outstanding proposals and retry `close`.
    /// `prune` is a direct engine hook and does not check this flag.
    ///
    /// With `Local::snapshot_path`, sync a temporary file containing the latest
    /// committed snapshot, close/flush storage, then atomically publish the file.
    /// I/O and active-handle errors can be retried. A storage-close error other
    /// than `ActiveHandles` poisons the writer and prevents checkpoint publication.
    /// A poisoned writer may close storage but cannot save a recoverable snapshot.
    /// Dropping an instance without `close` does not save a snapshot.
    ///
    /// Retained handles and readers can still read available revisions. Committed
    /// storage handles do not prevent closure. Repeated calls are allowed, including
    /// on a poisoned writer.
    pub fn close(&mut self) -> Result<()> {
        if self.close_complete {
            return Ok(());
        }
        self.closed = true;
        if let Some(mut worker) = self.worker.take() {
            worker.shutdown();
        }
        if let Some(path) = self.config.local.snapshot_path.clone() {
            if let Some(reason) = &self.poisoned {
                self.storage.close()?;
                return Err(Error::Poisoned(reason.clone()));
            }
            let head = self.reader.shared.registry.lock().head().clone();
            let bytes = persist::encode(
                head.snapshot(),
                S::ENGINE_ID,
                head.root(),
                head.storage_view().root(),
                head.metadata_hash(),
            )?;
            persist::write(&path, &bytes, || {
                let result = self.storage.close();
                if let Err(error) = &result
                    && !matches!(error, Error::ActiveHandles)
                {
                    // Some engines consume their database even when flushing
                    // fails. A subsequent no-op close must not publish a file.
                    self.poisoned = Some(format!("storage close failed: {error}"));
                }
                result
            })?;
        } else {
            self.storage.close()?;
        }
        self.close_complete = true;
        Ok(())
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

    /// Select borrowed `node` as the speculation target and return `()`.
    /// With no worker, does nothing. Otherwise stores a weak node reference,
    /// queues a reset to its storage view, then queues the snapshot values of all
    /// still-marked keys. It does not wait for the worker or clear the node's marks.
    ///
    /// Resending matters because a prediction mark survives across blocks, but its
    /// old speculative writes are not automatically present on the new base.
    /// Only `propose`'s no-eviction path keeps an existing chain without a reset.
    fn retarget(&mut self, node: &NodeRef<K, V, C, P, S>) {
        let Some(worker) = &self.worker else { return };
        self.target = Arc::downgrade(node);
        worker.reset(node.storage_view().clone());
        let snapshot = node.snapshot();
        worker.submit(spec_ops(snapshot, snapshot.predicted(), std::iter::empty()));
    }

    /// After the `commit` that just installed `head` (design §9.2). No-op
    /// unless speculation is on. With no lock held: if the precomputation
    /// already targets `head` (its steps were built on this node's own view,
    /// now committed, and stay valid bases) or a live child of it
    /// (`target().is_some_and(|t| t.parent_is(head))`, the pipeline's next
    /// parent), leave it alone; otherwise `self.retarget(head)`. `commit`'s
    /// caller never waits for engine work here (R17).
    fn after_commit(&mut self, head: &NodeRef<K, V, C, P, S>) {
        if self.worker.is_none() {
            return;
        }
        if self.targets(head) || self.target().is_some_and(|target| target.parent_is(head)) {
            return;
        }
        self.retarget(head);
    }
}

/// What `RightCash::resolve` answers: the node a call is about and the head.
type Resolved<K, V, C, P, S> = (NodeRef<K, V, C, P, S>, NodeRef<K, V, C, P, S>);

/// Return worker operations from borrowed `snapshot` and two owned-key iterators:
/// `evict` keys become `SpecOp::Evict` with their current snapshot entries;
/// `retract` keys become `SpecOp::Retract` without a snapshot lookup.
/// All evictions precede all retractions, preserving each iterator's order;
/// there is no sorting or deduplication. Panics if an eviction key is absent.
/// Does not change the snapshot, marks, or worker.
fn spec_ops<K, V, C, P>(
    snapshot: &Snapshot<K, V, C, P>,
    evict: impl IntoIterator<Item = K>,
    retract: impl IntoIterator<Item = K>,
) -> Vec<SpecOp<K, V>>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let evict = evict.into_iter().map(|key| {
        let Some(entry) = snapshot.get(&key) else {
            unreachable!("the eviction policy named a key the set lacks (design §6.1 invariant)");
        };
        SpecOp::Evict(key, entry)
    });
    evict.chain(retract.into_iter().map(SpecOp::Retract)).collect()
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
    type Db = crate::RightCash<K, V, LtHash, Fifo<K>, MemoryStorage<K, V>>;
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
