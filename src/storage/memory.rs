//! In-memory storage engine (design §10), a model of firewood: a persistent
//! map with an incremental state-function root (lattice accumulator over the
//! encoded `(key, value)` item, see `lattice`, updated per op so a proposal
//! costs O(batch)), chained proposals committed one at a time (each its own
//! revision, like firewood), a window of K revisions, the firewood adapter's
//! handle shapes (the parent link inside the handle's lock, the
//! committed-handle `propose` with its before-and-after root check, `head`,
//! consumed handles after a failed commit, `ActiveHandles` from `close`), and
//! an optional synthetic per-op latency so benchmarks and tests can open a
//! window the real engine's hashing time would.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::RwLock;

use crate::lattice::{LANES, LatticeAccumulator};
use crate::storage::{StorageEngine, StorageProposal};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash, Key, Result, Value};

const TAG: &str = "auth_wal/memory-storage";

#[derive(Clone, Debug)]
pub struct MemoryConfig {
    /// Committed revisions readable through committed handles before they go
    /// `RevisionStale`.
    pub revisions: usize,
    /// Artificial delay added per op in `propose` and per `commit`; zero
    /// disables it. Affects latency only, never roots.
    pub op_latency: Duration,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            revisions: 128,
            op_latency: Duration::ZERO,
        }
    }
}

fn sleep(latency: Duration, ops: usize) {
    if !latency.is_zero() {
        std::thread::sleep(latency * ops.max(1) as u32);
    }
}

/// Persistent (structurally shared) ordered map: the engine's revisions are
/// whole states that share structure, like firewood's node stores. O(1)
/// snapshot, O(log n) update; iteration in key order; range scans for prefix
/// deletes.
#[derive(Clone)]
struct PersistentMap<K, V> {
    inner: im::OrdMap<K, V>,
}

impl<K: Key, V: Clone> PersistentMap<K, V> {
    fn new() -> Self {
        Self {
            inner: im::OrdMap::new(),
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get::<K>(key)
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove::<K>(key)
    }

    /// Keys whose bytes start with `prefix`, in key order: a range scan from
    /// the first key `>= prefix` while keys still start with it.
    fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<K> {
        // `(Bound<&T>, Bound<&T>)` is the `RangeBounds` impl that allows an
        // unsized `T`, which `prefix..` does not.
        self.inner
            .range::<_, [u8]>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded))
            .take_while(|(k, _)| k.as_ref().starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }
}

/// One materialised state (committed or proposed) with its root.
struct State<K, V> {
    map: PersistentMap<K, V>,
    /// Incremental digest of `map`; `root` is derived from it.
    acc: LatticeAccumulator<LANES>,
    root: Hash,
    /// Commit sequence number, set once at commit (the `State` is shared
    /// behind `Arc` with child proposals, hence write-once interior
    /// mutability); the window check is arithmetic on it.
    seq: OnceLock<u64>,
}

impl<K: Key, V: Value> State<K, V> {
    fn empty() -> Self {
        Self {
            map: PersistentMap::new(),
            acc: LatticeAccumulator::new(TAG),
            root: EMPTY_ROOT,
            seq: OnceLock::new(),
        }
    }

    /// Applies `batch` to a copy of this state; the root is a function of the
    /// resulting map alone (`EMPTY_ROOT` when empty).
    fn apply(&self, batch: &[BatchOp<K, V>]) -> Self {
        let mut map = self.map.clone();
        let mut acc = self.acc.clone();
        for op in batch {
            match op {
                BatchOp::Put { key, value } => {
                    if let Some(old) = map.insert(key.clone(), value.clone()) {
                        acc.remove_fields(&[key.as_ref(), old.as_ref()]);
                    }
                    acc.add_fields(&[key.as_ref(), value.as_ref()]);
                }
                BatchOp::Delete { key } => {
                    if let Some(old) = map.remove(key) {
                        acc.remove_fields(&[key.as_ref(), old.as_ref()]);
                    }
                }
                BatchOp::PrefixDelete { prefix } => {
                    for key in map.keys_with_prefix(prefix) {
                        if let Some(old) = map.remove(&key) {
                            acc.remove_fields(&[key.as_ref(), old.as_ref()]);
                        }
                    }
                }
            }
        }
        let root = if acc.is_zero() { EMPTY_ROOT } else { Hash(acc.digest()) };
        Self {
            map,
            acc,
            root,
            seq: OnceLock::new(),
        }
    }
}

struct EngineInner<K, V> {
    committed: Arc<State<K, V>>,
    /// Sequence number the next commit gets; one per commit, so the oldest
    /// readable revision is `next_seq - revisions`.
    next_seq: u64,
    revisions: u64,
    /// Set by `close`: models firewood, whose committed handles can no longer
    /// reach the database afterwards (`Closed` from `propose`).
    closed: bool,
}

impl<K: Key, V: Value> EngineInner<K, V> {
    /// Installs `state` as the committed head, one revision per call (models
    /// firewood: a chain commits one proposal at a time).
    fn push(&mut self, state: Arc<State<K, V>>) {
        let _ = state.seq.set(self.next_seq);
        self.next_seq += 1;
        self.committed = state;
    }

    /// Committed, and more than `revisions` commits ago.
    fn is_stale(&self, state: &State<K, V>) -> bool {
        state.seq.get().is_some_and(|seq| seq + self.revisions < self.next_seq)
    }
}

pub struct MemoryStorage<K: Key, V: Value> {
    op_latency: Duration,
    engine: Arc<RwLock<EngineInner<K, V>>>,
    /// Live uncommitted proposals (neither committed nor failed), so `close`
    /// reports `ActiveHandles` like the firewood adapter's (contract item 6).
    live: Arc<AtomicUsize>,
}

/// Shared body of a proposal handle (firewood's thin-handle pattern): the
/// handle is cheap to clone and `commit(&self)` flips `committed`, after which
/// the handle reads as a revision at `state.root` and proposes on the
/// committed state (contract item 4).
struct ProposalInner<K, V> {
    /// Models firewood's per-handle read-write lock (design §10 (e)) and, like
    /// the firewood adapter's live state, guards the parent link (the
    /// uncommitted parent if chained; `None` once committed): the read guard
    /// is held across `propose`'s latency sleep and across `get`, the write
    /// guard across this handle's commit, so `op_latency` reproduces both the
    /// non-blocking `Propose` and `Commit`'s one bounded wait of design §9.2
    /// (§9.3 test 6). Never nested inside the engine lock on the read side:
    /// `get` drops its engine guard before taking it.
    handle: RwLock<Option<Arc<ProposalInner<K, V>>>>,
    state: Arc<State<K, V>>,
    /// The committed state this chain was proposed on. `commit` checks that
    /// the chain's base, after committing uncommitted ancestors, is the
    /// committed state (contract item 3): `head = parent.state or base`.
    base: Arc<State<K, V>>,
    committed: AtomicBool,
    /// Set once when this handle's **own** engine commit failed (models
    /// firewood consuming exactly that proposal, contract item 1): every later
    /// operation reports it and the handle no longer counts as live. A child
    /// of a failed handle is not marked: it stays live until dropped.
    failed: OnceLock<String>,
    engine: Arc<RwLock<EngineInner<K, V>>>,
    /// The engine's live count; this handle is in it while neither committed
    /// nor failed.
    live: Arc<AtomicUsize>,
}

impl<K, V> ProposalInner<K, V> {
    fn new(
        parent: Option<Arc<Self>>,
        state: Arc<State<K, V>>,
        base: Arc<State<K, V>>,
        committed: bool,
        engine: Arc<RwLock<EngineInner<K, V>>>,
        live: Arc<AtomicUsize>,
    ) -> Self {
        if !committed {
            live.fetch_add(1, Ordering::AcqRel);
        }
        Self {
            handle: RwLock::new(parent),
            state,
            base,
            committed: AtomicBool::new(committed),
            failed: OnceLock::new(),
            engine,
            live,
        }
    }

    fn is_live(&self) -> bool {
        !self.committed.load(Ordering::Acquire) && self.failed.get().is_none()
    }

    /// Records a failed commit (once): the handle stops counting as live.
    fn fail(&self, msg: &str) {
        if self.is_live() && self.failed.set(msg.to_owned()).is_ok() {
            self.live.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn check_failed(&self) -> Result<()> {
        match self.failed.get() {
            Some(msg) => Err(Error::Storage(msg.clone())),
            None => Ok(()),
        }
    }
}

impl<K, V> Drop for ProposalInner<K, V> {
    /// A dropped uncommitted proposal leaves the live count (firewood: its
    /// `Arc<Db>` goes).
    fn drop(&mut self) {
        if self.is_live() {
            self.live.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub struct MemoryProposal<K: Key, V: Value> {
    inner: Arc<ProposalInner<K, V>>,
    op_latency: Duration,
}

impl<K: Key, V: Value> MemoryStorage<K, V> {
    pub fn new(config: MemoryConfig) -> Self {
        let genesis = Arc::new(State::empty());
        let mut inner = EngineInner {
            committed: genesis.clone(),
            next_seq: 0,
            revisions: config.revisions.max(1) as u64,
            closed: false,
        };
        inner.push(genesis);
        Self {
            op_latency: config.op_latency,
            engine: Arc::new(RwLock::new(inner)),
            live: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<K: Key, V: Value> StorageEngine<K, V> for MemoryStorage<K, V> {
    type Proposal = MemoryProposal<K, V>;

    const ENGINE_ID: &'static str = "memory";
    const ROOT_IS_STATE_FUNCTION: bool = true;
    /// Models firewood: every proposal of a chain becomes its own revision.
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool = true;

    fn revisions(&self) -> usize {
        self.engine.read().revisions as usize
    }

    fn root(&self) -> Hash {
        self.engine.read().committed.root
    }

    /// Contract item 7: a committed handle at the committed state.
    fn head(&self) -> Result<Self::Proposal> {
        let engine = self.engine.read();
        if engine.closed {
            return Err(Error::Closed);
        }
        let committed = engine.committed.clone();
        drop(engine);
        Ok(MemoryProposal {
            inner: Arc::new(ProposalInner::new(
                None,
                committed.clone(),
                committed,
                true,
                self.engine.clone(),
                self.live.clone(),
            )),
            op_latency: self.op_latency,
        })
    }

    /// Models firewood's `close` (contract item 6): `ActiveHandles` while an
    /// uncommitted proposal is alive; afterwards committed handles keep
    /// reading, but a committed handle can no longer propose (`Closed`), as
    /// the firewood adapter's `Weak<Db>` no longer upgrades. Idempotent.
    fn close(&mut self) -> Result<()> {
        if self.live.load(Ordering::Acquire) > 0 {
            return Err(Error::ActiveHandles);
        }
        self.engine.write().closed = true;
        Ok(())
    }
}

impl<K: Key, V: Value> MemoryProposal<K, V> {
    fn child(&self, state: Arc<State<K, V>>, base: Arc<State<K, V>>, parent: Option<Arc<ProposalInner<K, V>>>) -> Self {
        Self {
            inner: Arc::new(ProposalInner::new(
                parent,
                state,
                base,
                false,
                self.inner.engine.clone(),
                self.inner.live.clone(),
            )),
            op_latency: self.op_latency,
        }
    }
}

impl<K: Key, V: Value> StorageProposal<K, V> for MemoryProposal<K, V> {
    fn root(&self) -> Hash {
        self.inner.state.root
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        self.inner.check_failed()?;
        if self.inner.committed.load(Ordering::Acquire) && self.inner.engine.read().is_stale(&self.inner.state) {
            return Err(Error::RevisionStale);
        }
        let _shared = self.inner.handle.read();
        Ok(self.inner.state.map.get(key).cloned())
    }

    fn propose(&self, batch: &[BatchOp<K, V>], _metadata: Option<&[u8]>) -> Result<Self> {
        // Firewood's lock shape (design §10 (e)): proposes and reads on one
        // handle share it; only this handle's commit is exclusive. The
        // committed check runs under the guard, as the firewood adapter's
        // does, so a propose parked behind a commit sees the committed state.
        let _shared = self.inner.handle.read();
        self.inner.check_failed()?;
        if self.inner.committed.load(Ordering::Acquire) {
            // Contract item 4: on the committed state, provided this handle's
            // root is the committed root before and after (equal roots mean
            // equal state here, as on firewood); `ProposalOrphaned` otherwise;
            // `Closed` once the engine is closed.
            let head = {
                let engine = self.inner.engine.read();
                if engine.closed {
                    return Err(Error::Closed);
                }
                engine.committed.clone()
            };
            if head.root != self.inner.state.root {
                return Err(Error::ProposalOrphaned);
            }
            let state = Arc::new(head.apply(batch));
            sleep(self.op_latency, batch.len());
            if self.inner.engine.read().committed.root != self.inner.state.root {
                return Err(Error::ProposalOrphaned);
            }
            return Ok(self.child(state, head, None));
        }
        let state = Arc::new(self.inner.state.apply(batch));
        sleep(self.op_latency, batch.len());
        Ok(self.child(state, self.inner.base.clone(), Some(self.inner.clone())))
    }

    /// Commits uncommitted ancestors oldest-first, each as its own revision
    /// (models firewood), then this proposal. Requires the chain's base to be
    /// the committed head (firewood's `NotLatest` otherwise).
    fn commit(&self) -> Result<Hash> {
        self.inner.check_failed()?;
        if self.inner.committed.load(Ordering::Acquire) {
            return Err(Error::ProposalConsumed);
        }
        // Collect the uncommitted chain, oldest first.
        let mut chain = vec![self.inner.clone()];
        loop {
            let parent = chain.last().expect("non-empty").handle.read().clone();
            match parent {
                Some(p) if !p.committed.load(Ordering::Acquire) => chain.push(p),
                _ => break,
            }
        }
        chain.reverse();
        // A failed ancestor fails this commit too (firewood commits oldest
        // first and stops at the consumed parent) but consumes nothing more:
        // this handle stays live, readable and pinning until dropped, as the
        // firewood adapter's does (contract item 1).
        if let Some(msg) = chain.iter().find_map(|p| p.failed.get()) {
            return Err(Error::Storage(format!("an ancestor's commit failed: {msg}")));
        }
        // Exclusive on each handle first (design §10 (e)): waits for a propose
        // in flight on it, as firewood's commit does, without holding the
        // engine lock meanwhile (handle -> engine is the only nesting; readers
        // release their engine guard before taking a handle guard).
        let mut exclusive: Vec<_> = chain.iter().map(|p| p.handle.write()).collect();
        let mut engine = self.inner.engine.write();
        if chain.iter().any(|p| p.committed.load(Ordering::Acquire)) {
            return Err(Error::Storage(
                "an ancestor was committed concurrently (auth_wal serialises commits)".to_owned(),
            ));
        }
        let oldest = chain.first().expect("non-empty");
        let head = match &*exclusive[0] {
            Some(p) => p.state.clone(),
            None => oldest.base.clone(),
        };
        if !Arc::ptr_eq(&head, &engine.committed) {
            // Firewood commits oldest-first and consumes the proposal whose
            // commit failed: the oldest handle is dead from now on (contract
            // item 1), its children stay live until dropped.
            let msg = format!(
                "proposal parent is not the latest committed revision (NotLatest): parent {:?}, head {:?}",
                head.root, engine.committed.root
            );
            oldest.fail(&msg);
            return Err(Error::Storage(msg));
        }
        for (p, guard) in chain.iter().zip(exclusive.iter_mut()) {
            engine.push(p.state.clone());
            p.committed.store(true, Ordering::Release);
            p.live.fetch_sub(1, Ordering::AcqRel);
            // The parent link goes with the commit, as the firewood adapter's
            // does with its live state.
            **guard = None;
            sleep(self.op_latency, 1);
        }
        Ok(self.inner.state.root)
    }
}
