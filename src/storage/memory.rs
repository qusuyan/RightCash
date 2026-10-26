//! In-memory storage engine (design §10), a model of firewood: a persistent
//! map with an incremental state-function root (lattice accumulator over the
//! encoded `(key, value)` item, see `lattice`,
//! updated per op so a proposal costs O(batch)), chained proposals,
//! K revisions, and an optional synthetic per-op latency so benchmarks can
//! show what speculation buys against a slow engine.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use parking_lot::RwLock;

use crate::lattice::{LANES, LatticeAccumulator};
use crate::set::PersistentMap;
use crate::storage::{ReadView, StorageEngine, StorageProposal};
use crate::types::{BatchOp, Hash, Key, Result, Value};

#[derive(Clone, Debug)]
pub struct MemoryConfig {
    /// Committed roots kept for `revision()`.
    pub revisions: usize,
    /// Artificial delay added to every propose/commit op; zero disables it.
    /// Affects latency only, never roots.
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

/// One materialised state (committed or proposed) with its root.
struct State<K, V> {
    map: PersistentMap<K, V>,
    /// Incremental digest of `map`; `root` is derived from it.
    acc: LatticeAccumulator<LANES>,
    root: Hash,
    /// Commit sequence number, set once at commit (the `State` is shared
    /// behind `Arc` with child proposals, hence write-once interior
    /// mutability); with `EngineInner.oldest_seq` the window check is O(1).
    seq: OnceLock<u64>,
}

struct EngineInner<K, V> {
    committed: Arc<State<K, V>>,
    /// Last K committed states, newest last.
    history: VecDeque<Arc<State<K, V>>>,
    /// `seq` of the oldest state still in `history`.
    oldest_seq: u64,
}

pub struct MemoryStorage<K: Key, V: Value> {
    config: MemoryConfig,
    engine: Arc<RwLock<EngineInner<K, V>>>,
}

/// Shared body of a proposal handle (firewood's thin-handle pattern): the
/// handle is cheap to clone and `commit(&self)` flips `committed`, after which
/// the handle reads as a revision at `state.root`.
struct ProposalInner<K, V> {
    state: Arc<State<K, V>>,
    /// The committed state this chain was proposed on. `commit` checks that
    /// the chain's base, after committing uncommitted ancestors, is the
    /// committed state (contract item 3): `head = parent.state or base`.
    base: Arc<State<K, V>>,
    /// Uncommitted parent, if chained; cleared on commit.
    parent: parking_lot::Mutex<Option<Arc<ProposalInner<K, V>>>>,
    committed: AtomicBool,
    engine: Arc<RwLock<EngineInner<K, V>>>,
    op_latency: Duration,
}

pub struct MemoryProposal<K: Key, V: Value> {
    inner: Arc<ProposalInner<K, V>>,
}

pub struct MemoryRevision<K: Key, V: Value> {
    state: Arc<State<K, V>>,
    engine: Arc<RwLock<EngineInner<K, V>>>,
}

impl<K: Key, V: Value> MemoryStorage<K, V> {
    pub fn new(config: MemoryConfig) -> Self {
        todo!("committed = empty map at EMPTY_ROOT")
    }
}

impl<K: Key, V: Value> StorageEngine<K, V> for MemoryStorage<K, V> {
    type Proposal = MemoryProposal<K, V>;
    type Revision = MemoryRevision<K, V>;

    const ENGINE_ID: &'static str = "memory";
    const ROOT_IS_STATE_FUNCTION: bool = true;
    /// Models firewood: every proposal of a chain becomes its own revision.
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool = true;

    fn revisions(&self) -> usize {
        self.config.revisions
    }

    fn root(&self) -> Hash {
        todo!()
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        todo!()
    }

    fn revision(&self, root: Hash) -> Result<Self::Revision> {
        todo!("search history; Error::RevisionNotFound otherwise")
    }

    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self::Proposal> {
        todo!("apply batch to committed map, update acc incrementally, derive root, sleep op_latency; metadata ignored")
    }

    fn prove(&self, root: Hash, key: &K) -> Result<Vec<u8>> {
        todo!("milestone 6: needs a Merkle variant of the state root; the lattice accumulator has no inclusion proofs")
    }

    fn verify_proof(root: &Hash, key: &K, value: Option<&V>, proof: &[u8]) -> bool {
        todo!("milestone 6")
    }
}

impl<K: Key, V: Value> ReadView<K, V> for MemoryProposal<K, V> {
    fn root(&self) -> Hash {
        self.inner.state.root
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("if committed and state.seq < engine.oldest_seq -> RevisionStale, else state.map.get")
    }
}

impl<K: Key, V: Value> StorageProposal<K, V> for MemoryProposal<K, V> {
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self> {
        todo!("child with parent = Some(self.inner.clone())")
    }

    fn is_committed(&self) -> bool {
        self.inner.committed.load(std::sync::atomic::Ordering::Acquire)
    }

    fn commit(&self) -> Result<Hash> {
        todo!(
            "second commit -> error; commit uncommitted ancestors first, each its own history record (models firewood); then require Arc::ptr_eq(parent.state or base, committed); push this record"
        )
    }
}

impl<K: Key, V: Value> ReadView<K, V> for MemoryRevision<K, V> {
    fn root(&self) -> Hash {
        self.state.root
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        todo!("RevisionStale if state.seq < engine.oldest_seq, else state.map.get")
    }
}
