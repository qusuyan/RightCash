//! Firewood storage backend (design §10, R13): the engine of record, adapted
//! directly from the `firewood` crate.
//!
//! Firewood facts this adapter honours (verified against the source):
//! - `Proposal<'db>` borrows the `Db` and `commit(self)` consumes it. The
//!   adapter keeps the `Db` in an `Arc` and owns each `Proposal` with its
//!   lifetime erased, holding an `Arc<Db>` next to it so the database always
//!   outlives the proposal (the only `unsafe` in this crate; see `FwLive`).
//! - A chain commits one proposal at a time: committing a child whose parent
//!   is uncommitted fails with `ParentNotLatest`, so `commit` walks the
//!   adapter-side parent links (kept inside each live state) and commits
//!   oldest-first (`COMMITS_CHAIN_LINKS_SEPARATELY = true`).
//! - After `commit` the handle re-bases to the committed view so children
//!   pinned to it keep reading (contract item 2); firewood never reaps a
//!   referenced revision, so such a view does not go stale. The committed
//!   state keeps a `Weak<Db>` so the handle can still propose on the
//!   committed state while it is the head (item 4) without pinning the
//!   database (item 6).
//! - Two consecutive revisions can share a root; the adapter never relies on
//!   root lookup for a view it already holds.
//! - The constructor forces `CacheReadStrategy::All` (a documented
//!   deviation) so that reads through views populate the node cache; the
//!   read-through warming that first needed it is no longer used by
//!   auth_wal's hints (design §9.4, R26), so the setting is a milestone-2
//!   knob.
//!
//! The engine is owned by auth_wal's writer (R19): `close` takes `&mut self`
//! and the database slot is a plain `Option`, so the adapter has no lock of
//! its own but the per-handle read-write lock of design §10 (e).

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use firewood::api::{Db as _, DbView as _, Proposal as _};
use firewood::db::{BatchOp as FwBatchOp, Db, DbConfig, Proposal as FwRawProposal};
use firewood::manager::{CacheReadStrategy, RevisionManagerConfig};
use firewood_storage::{NodeHashAlgorithm, TrieHash};
use parking_lot::RwLock;

use crate::storage::{StorageEngine, StorageProposal};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash, Key, Result, Value};

#[derive(Clone, Debug)]
pub struct FirewoodConfig {
    pub path: PathBuf,
    /// Destroy any existing database at `path` first.
    pub truncate: bool,
    /// Revision window (`max_revisions`); design §8 sizes it as
    /// `K × (1 + rounds)` when speculating.
    pub revisions: usize,
    pub node_cache_bytes: NonZeroUsize,
    pub free_list_cache_entries: NonZeroUsize,
}

impl FirewoodConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            truncate: false,
            revisions: 128,
            node_cache_bytes: NonZeroUsize::new(192_000_000).expect("non-zero"),
            free_list_cache_entries: NonZeroUsize::new(40_000).expect("non-zero"),
        }
    }
}

pub struct FirewoodStorage {
    /// `None` after `close` (which takes `&mut self`: the writer owns the
    /// engine, R19).
    db: Option<Arc<Db>>,
    revisions: usize,
}

/// A `firewood::db::Proposal` with its borrow of the database erased.
///
/// # Safety
///
/// `Proposal<'db>` only requires the `Db` to outlive it. [`FwLive`] holds
/// an `Arc<Db>` and declares `raw` before `db`, so Rust drops the proposal
/// before the database reference, and the `Db` lives on the heap behind the
/// `Arc`, so it never moves. `FirewoodStorage::close` only consumes the `Db`
/// once it is the sole owner, which cannot happen while a live proposal
/// exists. The `Arc<Db>` lives *only* here: a committed or dead proposal
/// holds at most a `Weak<Db>` (contract item 6), so `close` succeeds once
/// every uncommitted proposal is gone, however many committed handles and
/// revision views remain.
struct FwLive {
    raw: FwRawProposal<'static>,
    db: Arc<Db>,
    /// Adapter-side parent link so `commit` can commit ancestors oldest-first
    /// (firewood keeps no parent accessor); goes with the live state at
    /// commit (design §10 (b)), so no cell of its own is needed.
    parent: Option<Arc<FwProposal>>,
}

enum FwState {
    Live(FwLive),
    /// Re-based after commit (contract item 2). Holds a `Weak<Db>`, never an
    /// `Arc`: a committed proposal never blocks `close` (item 6) but can
    /// still propose on the committed state while it is the head (item 4).
    Committed {
        view: firewood::api::ArcDynDbView,
        db: Weak<Db>,
    },
    /// Committed, and the trie is empty: without `ethhash` firewood has no
    /// root hash and no view for an empty trie (with it the indexed empty
    /// revision is used instead), so every key reads as absent. Proposable
    /// like `Committed` (a fresh engine's `head`).
    Empty {
        db: Weak<Db>,
    },
    /// Committed, but the root could not be looked up (the window already
    /// moved past a same-root revision): every read reports `RevisionStale`.
    Stale,
    /// `commit` failed after firewood consumed the proposal: every operation,
    /// including a retried `commit`, reports that failure (design §9.2: "a
    /// retry returns that error, not `ProposalConsumed`").
    Failed(String),
}

/// One firewood proposal handle. Its state sits behind a read-write lock:
/// `get` and `propose` share it (firewood's `val` and `propose` take `&self`),
/// only `commit_one` is exclusive, so a speculative link being hashed on the
/// worker never blocks the final proposal, a plain proposal or a read on the
/// same handle (design §9.2 R17, §10 (e)). The parent link lives inside the
/// guarded live state.
pub struct FwProposal {
    state: RwLock<FwState>,
    root: Hash,
    committed: AtomicBool,
}

fn hash_from(h: Option<TrieHash>) -> Hash {
    match h {
        Some(h) => {
            let mut out = [0u8; 32];
            out.copy_from_slice(h.as_ref());
            Hash(out)
        }
        None => EMPTY_ROOT,
    }
}

fn trie_hash(h: Hash) -> TrieHash {
    TrieHash::from(h.0)
}

fn fw_err(e: firewood::api::Error) -> Error {
    match e {
        firewood::api::Error::RevisionNotFound { .. } => Error::RevisionNotFound,
        other => Error::Storage(other.to_string()),
    }
}

fn fw_batch<K: Key, V: Value>(batch: &[BatchOp<K, V>]) -> Vec<FwBatchOp<&[u8], &[u8]>> {
    batch
        .iter()
        .map(|op| match op {
            BatchOp::Put { key, value } => FwBatchOp::Put {
                key: key.as_ref(),
                value: value.as_ref(),
            },
            BatchOp::Delete { key } => FwBatchOp::Delete { key: key.as_ref() },
            BatchOp::PrefixDelete { prefix } => FwBatchOp::DeleteRange {
                prefix: prefix.as_ref(),
            },
        })
        .collect()
}

fn erase<'db>(p: FwRawProposal<'db>) -> FwRawProposal<'static> {
    // SAFETY: see `FwLive`. The struct layout does not depend on the lifetime;
    // the holder keeps an `Arc<Db>` alive for as long as the proposal exists.
    unsafe { std::mem::transmute::<FwRawProposal<'db>, FwRawProposal<'static>>(p) }
}

impl FirewoodStorage {
    pub fn new(config: FirewoodConfig) -> Result<Self> {
        // Reads through views populate the node cache only under `All`:
        // the strategy is forced, a documented §3 deviation (a milestone-2
        // knob now that hints do not read through, design §9.4).
        let manager = RevisionManagerConfig::builder()
            .max_revisions(config.revisions.max(2))
            .node_cache_memory_limit(config.node_cache_bytes)
            .free_list_cache_size(config.free_list_cache_entries)
            .cache_read_strategy(CacheReadStrategy::All)
            .build();
        // Must match firewood's compiled hashing feature; our `ethhash` feature
        // forwards to firewood's, so the two agree by construction.
        let algorithm = if cfg!(feature = "ethhash") {
            NodeHashAlgorithm::Ethereum
        } else {
            NodeHashAlgorithm::MerkleDB
        };
        let cfg = DbConfig::builder()
            .node_hash_algorithm(algorithm)
            .truncate(config.truncate)
            .manager(manager)
            .build();
        let db = Db::new(&config.path, cfg).map_err(fw_err)?;
        Ok(Self {
            db: Some(Arc::new(db)),
            // What the engine was opened with (firewood needs at least 2).
            revisions: config.revisions.max(2),
        })
    }

    fn db(&self) -> Result<&Arc<Db>> {
        self.db.as_ref().ok_or(Error::Closed)
    }

    fn wrap(db: Arc<Db>, raw: FwRawProposal<'static>, parent: Option<Arc<FwProposal>>) -> FwProposal {
        let root = hash_from(raw.root_hash());
        FwProposal {
            state: RwLock::new(FwState::Live(FwLive { raw, db, parent })),
            root,
            committed: AtomicBool::new(false),
        }
    }

    /// Contract item 4: a proposal on the committed state through a committed
    /// handle at `root`. The database is upgraded from the handle's weak
    /// reference (`Closed` if the engine is gone) and the committed root is
    /// compared with `root` before and after proposing (`ProposalOrphaned` on
    /// a mismatch: a `Commit` moved the head meanwhile; the built proposal is
    /// dropped, item 5). The new live proposal has no parent link: its parent
    /// is the committed state.
    fn propose_committed<K: Key, V: Value>(
        db: &Weak<Db>,
        root: Hash,
        batch: &[BatchOp<K, V>],
    ) -> Result<Arc<FwProposal>> {
        let db = db.upgrade().ok_or(Error::Closed)?;
        if hash_from(db.root_hash()) != root {
            return Err(Error::ProposalOrphaned);
        }
        let raw = erase(db.propose(fw_batch(batch)).map_err(fw_err)?);
        if hash_from(db.root_hash()) != root {
            // `raw` drops before `db` (declared later), as `FwLive` requires.
            return Err(Error::ProposalOrphaned);
        }
        Ok(Arc::new(Self::wrap(db, raw, None)))
    }
}

impl<K: Key, V: Value> StorageEngine<K, V> for FirewoodStorage {
    type Proposal = Arc<FwProposal>;

    const ENGINE_ID: &'static str = "firewood";
    const ROOT_IS_STATE_FUNCTION: bool = true;
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool = true;

    fn revisions(&self) -> usize {
        self.revisions
    }

    fn root(&self) -> Hash {
        self.db
            .as_ref()
            .map(|db| hash_from(db.root_hash()))
            .unwrap_or(EMPTY_ROOT)
    }

    /// Contract item 7: the committed revision (or the empty state of a fresh
    /// engine) wrapped as a committed handle, holding a `Weak<Db>` only.
    fn head(&self) -> Result<Self::Proposal> {
        let db = self.db()?;
        let weak = Arc::downgrade(db);
        let (state, root) = match db.root_hash() {
            None => (FwState::Empty { db: weak }, EMPTY_ROOT),
            Some(h) => {
                let root = hash_from(Some(h.clone()));
                let view: firewood::api::ArcDynDbView = firewood::api::Db::revision(&**db, h).map_err(fw_err)?;
                (FwState::Committed { view, db: weak }, root)
            }
        };
        Ok(Arc::new(FwProposal {
            state: RwLock::new(state),
            root,
            committed: AtomicBool::new(true),
        }))
    }

    fn close(&mut self) -> Result<()> {
        let Some(db) = self.db.take() else { return Ok(()) };
        match Arc::try_unwrap(db) {
            Ok(db) => db.close().map_err(fw_err),
            Err(db) => {
                // Live proposals still reference the database; put it back.
                self.db = Some(db);
                Err(Error::ActiveHandles)
            }
        }
    }
}

impl<K: Key, V: Value> StorageProposal<K, V> for Arc<FwProposal> {
    fn root(&self) -> Hash {
        self.as_ref().root
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        let state = self.state.read();
        let value = match &*state {
            FwState::Live(live) => live.raw.val(key.as_ref()).map_err(fw_err)?,
            FwState::Committed { view, .. } => view.val(key.as_ref()).map_err(fw_err)?,
            FwState::Empty { .. } => None,
            FwState::Stale => return Err(Error::RevisionStale),
            FwState::Failed(msg) => return Err(Error::Storage(msg.clone())),
        };
        Ok(value.map(|b| V::from_bytes(&b)))
    }

    fn propose(&self, batch: &[BatchOp<K, V>], _metadata: Option<&[u8]>) -> Result<Self> {
        let state = self.state.read();
        match &*state {
            FwState::Live(live) => {
                let raw = live.raw.propose(fw_batch(batch)).map_err(fw_err)?;
                let raw = erase(raw);
                Ok(Arc::new(FirewoodStorage::wrap(
                    live.db.clone(),
                    raw,
                    Some(self.clone()),
                )))
            }
            // Contract item 4: on the committed state, root-checked before and after.
            FwState::Committed { db, .. } | FwState::Empty { db } => {
                FirewoodStorage::propose_committed(db, self.root, batch)
            }
            FwState::Stale => Err(Error::RevisionStale),
            FwState::Failed(msg) => Err(Error::Storage(msg.clone())),
        }
    }

    /// Commits uncommitted ancestors oldest-first (firewood rejects a child
    /// of an uncommitted proposal with `ParentNotLatest`), then this
    /// proposal, re-basing each handle to its committed view.
    fn commit(&self) -> Result<Hash> {
        if self.committed.load(Ordering::Acquire) {
            return Err(Error::ProposalConsumed);
        }
        let mut chain = vec![self.clone()];
        loop {
            let parent = match &*chain.last().expect("non-empty").state.read() {
                FwState::Live(live) => live.parent.clone(),
                _ => None,
            };
            match parent {
                Some(p) if !p.committed.load(Ordering::Acquire) => chain.push(p),
                _ => break,
            }
        }
        for p in chain.iter().rev() {
            p.commit_one()?;
        }
        Ok(self.root)
    }
}

impl FwProposal {
    fn commit_one(&self) -> Result<()> {
        let mut state = self.state.write();
        match &*state {
            FwState::Live(_) => {}
            FwState::Failed(msg) => return Err(Error::Storage(msg.clone())),
            FwState::Committed { .. } | FwState::Empty { .. } | FwState::Stale => {
                return Err(Error::ProposalConsumed);
            }
        }
        // Take the live proposal out so `commit(self)` can consume it; on
        // failure the handle keeps the error (firewood consumed the proposal)
        // and every later operation, including a retried commit, reports it.
        let placeholder = FwState::Failed("commit in progress".to_owned());
        let FwState::Live(live) = std::mem::replace(&mut *state, placeholder) else {
            unreachable!()
        };
        let FwLive { raw, db, parent } = live;
        // The parent link goes with the live state (design §10 (b)).
        drop(parent);
        if let Err(e) = raw.commit() {
            // Keep firewood's own message: a retry reports it once, under
            // `Error::Storage`'s prefix.
            *state = FwState::Failed(e.to_string());
            return Err(fw_err(e));
        }
        // Re-base (contract item 2) to *this proposal's* root among the
        // committed revisions only (`Db::revision`, not `view`, which would
        // resolve a live proposal with the same root first and not pin the
        // committed base; design §10 (c)); under auth_wal's serialised commits
        // that is also the engine head. An emptied trie has no root hash
        // without `ethhash` and reads as absent; a lookup can fail only if the
        // window already moved past a same-root revision, and then reads
        // through this handle report RevisionStale. The committed state keeps
        // only a `Weak<Db>` (items 4 and 6).
        let weak = Arc::downgrade(&db);
        *state = if db.root_hash().is_none() {
            FwState::Empty { db: weak }
        } else {
            match firewood::api::Db::revision(&*db, trie_hash(self.root)) {
                Ok(rev) => {
                    let view: firewood::api::ArcDynDbView = rev;
                    FwState::Committed { view, db: weak }
                }
                Err(_) => FwState::Stale,
            }
        };
        self.committed.store(true, Ordering::Release);
        Ok(())
    }
}
