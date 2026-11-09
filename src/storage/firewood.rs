//! Firewood storage backend (design §10, R13): the engine of record, adapted
//! directly from the `firewood` crate.
//!
//! Firewood facts this adapter honours (verified against the source):
//! - `DynProposal<'db>` borrows the database and `commit(self)` consumes it. The
//!   adapter keeps the `Db` in an `Arc` and owns each `Proposal` with its
//!   lifetime erased, holding an `Arc<Db>` next to it so the database always
//!   outlives the proposal (one of two `unsafe` blocks in this crate, with
//!   the AVX2 entry in `vc::blake3_lanes`; see `FwLive`).
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
//! - A Firewood proposal reads its base revision's nodes by address, so a
//!   live proposal whose base was reaped would read
//!   reused addresses as data. The first link of a chain therefore holds
//!   the committed handle it was proposed on as its parent, which pins the
//!   base revision (its view) for as long as the link lives; the window
//!   stalls at a pinned revision instead (design §10 (b), (c)).
//! - Firewood checks parent revision identity and absorbs same-root commits
//!   without creating a new physical revision. The adapter assigns each
//!   successful commit a generation, preserving its stricter contract that
//!   even a no-op commit supersedes the previous head (items 3 and 4).
//!   Pinned views remain readable; generations only gate proposing and committing.
//! - The constructor forces `CacheReadStrategy::All` (a documented
//!   deviation) so that reads through views populate the node cache; the
//!   read-through warming that first needed it is no longer used by
//!   auth_wal's hints (design §9.4, R26), so the setting is a milestone-2
//!   knob.
//!
//! The engine is owned by auth_wal's writer (R19): `close` takes `&mut self`
//! and the database slot is a plain `Option`, so the adapter has no lock of
//! its own but the per-handle read-write lock of design §10 (e).

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use firewood::api::{BatchOp as FwBatchOp, DynDb, DynProposal, OwnedBatch};
use firewood::db::DbConfig;
use firewood::manager::{CacheReadStrategy, RevisionManagerConfig};
use firewood_storage::{NodeHashAlgorithm, TrieHash};
use parking_lot::RwLock;

use crate::storage::{StorageEngine, StorageProposal};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash, Key, Result, Value};

struct Db {
    inner: Box<dyn DynDb>,
    // auth_wal serializes commits; readers and the speculative worker observe
    // this counter to reject bases superseded even by a physical no-op.
    generation: AtomicU64,
}

impl std::ops::Deref for Db {
    type Target = dyn DynDb;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

type FwRawProposal<'db> = Box<dyn DynProposal<'db> + 'db>;

#[derive(Clone, Debug)]
pub struct FirewoodConfig {
    pub path: PathBuf,
    /// Destroy any existing database at `path` first.
    pub truncate: bool,
    /// Revision window (`max_revisions`); design §8 sizes it as
    /// `K × rounds` when speculating.
    pub revisions: usize,
    /// Maximum committed revisions awaiting persistence. Must be smaller
    /// than the engine's revision window. Defaults to Firewood's single permit.
    pub commit_permits: NonZeroU64,
    pub node_cache_bytes: NonZeroUsize,
    /// Converted to Firewood's KiB budget, rounding up using its entry-size estimate.
    pub free_list_cache_entries: NonZeroUsize,
}

impl FirewoodConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            truncate: false,
            revisions: 128,
            commit_permits: NonZeroU64::MIN,
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

/// A boxed `firewood::api::DynProposal` with its database lifetime erased.
///
/// # Safety
///
/// `DynProposal<'db>` requires its database to outlive it. [`FwLive`] holds
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
    /// (firewood keeps no parent accessor), and the pin of a chain's base:
    /// the first link holds the committed handle it was proposed on, whose
    /// view keeps the base revision from being reaped while the link reads
    /// it by address. Goes with the live state at commit (design §10 (b)).
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
        generation: u64,
    },
    /// Committed, and the trie is empty: without `ethhash` firewood has no
    /// root hash and no view for an empty trie (with it the indexed empty
    /// revision is used instead), so every key reads as absent. Proposable
    /// like `Committed` (a fresh engine's `head`).
    Empty {
        db: Weak<Db>,
        generation: u64,
    },
    /// Committed, but the root could not be looked up (the window already
    /// moved past a same-root revision): every read reports `RevisionStale`.
    Stale,
    /// A base check or engine commit failed. Later reads, proposals, and commits
    /// return the stored error, including retries; `root` still returns its
    /// cached hash. This state owns no live proposal or database reference.
    Failed(String),
}

/// One Firewood proposal handle with state behind a read-write lock.
/// `get` and `propose` acquire shared guards; committing or marking a failed
/// base check requires an exclusive guard. Reads and proposals can share the
/// handle while a step is built, but may wait for a writer or backend locks.
/// The parent link belongs to the guarded live state.
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

/// Every firewood error is an engine error here: `RevisionNotFound` is the
/// registry's word for a root no live proposal or retained revision has
/// (design §8), not an engine's.
fn fw_err(e: firewood::api::Error) -> Error {
    Error::Storage(e.to_string())
}

/// Return whether borrowed committed handle `state` still names `db`'s head.
/// Generations distinguish adapter commits even when Firewood absorbs a no-op.
/// Return false for other handle states; mutate neither.
fn is_current(state: &FwState, db: &Db) -> bool {
    match state {
        FwState::Committed { view, generation, .. } => {
            // The engine can publish a state-changing commit before the
            // adapter publishes its generation. Check both identities.
            *generation == db.generation.load(Ordering::Acquire)
                && db.root_hash().is_some_and(|hash| {
                    db.revision(hash)
                        .is_ok_and(|current| std::ptr::addr_eq(Arc::as_ptr(view), Arc::as_ptr(&current)))
                })
        }
        FwState::Empty { generation, .. } => {
            *generation == db.generation.load(Ordering::Acquire) && db.root_hash().is_none()
        }
        _ => false,
    }
}

/// Copy operations in input order into Firewood's owned public batch format.
/// PrefixDelete maps to Firewood's prefix-based DeleteRange.
fn fw_batch<K: Key, V: Value>(batch: &[BatchOp<K, V>]) -> OwnedBatch {
    batch
        .iter()
        .map(|op| match op {
            BatchOp::Put { key, value } => FwBatchOp::Put {
                key: key.as_ref().into(),
                value: value.as_ref().into(),
            },
            BatchOp::Delete { key } => FwBatchOp::Delete {
                key: key.as_ref().into(),
            },
            BatchOp::PrefixDelete { prefix } => FwBatchOp::DeleteRange {
                prefix: prefix.as_ref().into(),
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
        // Check before opening the path, especially when truncate is requested.
        let revisions = config.revisions.max(2);
        if u128::from(config.commit_permits.get()) >= revisions as u128 {
            return Err(Error::InvalidConfig(format!(
                "firewood commit permits ({}) must be less than the engine revision window ({revisions})",
                config.commit_permits
            )));
        }
        let freelist_kb = config
            .free_list_cache_entries
            .get()
            .checked_mul(firewood_storage::FREE_LIST_CACHE_ENTRY_SIZE)
            .map(|bytes| bytes.div_ceil(1024))
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| Error::InvalidConfig("firewood free-list cache size overflows usize".into()))?;
        // Reads through views populate the node cache only under `All`:
        // the strategy is forced, a documented §3 deviation (a milestone-2
        // knob now that hints do not read through, design §9.4).
        let manager = RevisionManagerConfig::builder()
            .max_revisions(revisions)
            .max_persistence_gap(config.commit_permits)
            .node_cache_memory_limit(config.node_cache_bytes)
            .freelist_memory_limit_kb(freelist_kb)
            .cache_read_strategy(CacheReadStrategy::All)
            .build();
        // Keep auth_wal's feature-selected engine identifier aligned with the
        // runtime hash mode passed to Firewood's public open function.
        let algorithm = if cfg!(feature = "ethhash") {
            NodeHashAlgorithm::Ethereum
        } else {
            NodeHashAlgorithm::MerkleDB
        };
        let cfg = DbConfig::builder().truncate(config.truncate).manager(manager).build();
        let db = firewood::db::open(&config.path, algorithm, cfg).map_err(fw_err)?;
        Ok(Self {
            db: Some(Arc::new(Db {
                inner: db,
                generation: AtomicU64::new(0),
            })),
            // What the engine was opened with (firewood needs at least 2).
            revisions,
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

    /// Contract item 4: a proposal on the committed state through the
    /// committed handle `parent`, whose `state` the caller holds. The
    /// database is upgraded from the state's weak reference (`Closed` if the
    /// engine is gone) and the handle's revision is checked to be the
    /// engine's current one, by identity, before and after proposing
    /// (`ProposalOrphaned` otherwise: a `Commit` moved the head meanwhile,
    /// a same-root one included, module doc; the built proposal is dropped,
    /// item 5). The new live proposal keeps `parent` as its parent link: a
    /// committed handle, so `commit` stops there, and the pin of the base
    /// revision the proposal reads by address (module doc).
    fn propose_committed<K: Key, V: Value>(
        parent: &Arc<FwProposal>,
        state: &FwState,
        batch: &[BatchOp<K, V>],
    ) -> Result<Arc<FwProposal>> {
        let db = match state {
            FwState::Committed { db, .. } | FwState::Empty { db, .. } => db.upgrade().ok_or(Error::Closed)?,
            _ => unreachable!("propose_committed takes a committed state"),
        };
        if !is_current(state, &db) {
            return Err(Error::ProposalOrphaned);
        }
        let raw = erase(db.propose(fw_batch(batch)).map_err(fw_err)?);
        if !is_current(state, &db) {
            // Locals drop in reverse declaration order: `raw` was declared
            // after `db`, so the proposal drops while its database is still alive.
            return Err(Error::ProposalOrphaned);
        }
        Ok(Arc::new(Self::wrap(db, raw, Some(parent.clone()))))
    }
}

impl<K: Key, V: Value> StorageEngine<K, V> for FirewoodStorage {
    type Proposal = Arc<FwProposal>;

    // The selected hash affects future storage roots even while every write
    // still fits in the set, so it must distinguish the combined roots now.
    const ENGINE_ID: &'static str = if cfg!(feature = "ethhash") {
        "firewood/ethereum"
    } else {
        "firewood/merkledb"
    };
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
        let generation = db.generation.load(Ordering::Acquire);
        let (state, root) = match db.root_hash() {
            None => (FwState::Empty { db: weak, generation }, EMPTY_ROOT),
            Some(h) => {
                let root = hash_from(Some(h.clone()));
                let view = db.revision(h).map_err(fw_err)?;
                (
                    FwState::Committed {
                        view,
                        db: weak,
                        generation,
                    },
                    root,
                )
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
            Ok(db) => db.inner.close().map_err(fw_err),
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
            // Contract item 4: on the committed state, its revision checked
            // to be current before and after.
            FwState::Committed { .. } | FwState::Empty { .. } => {
                FirewoodStorage::propose_committed(self, &state, batch)
            }
            FwState::Stale => Err(Error::RevisionStale),
            FwState::Failed(msg) => Err(Error::Storage(msg.clone())),
        }
    }

    /// Commits uncommitted ancestors oldest-first (firewood rejects a child
    /// of an uncommitted proposal with `ParentNotLatest`), then this
    /// proposal, re-basing each handle to its committed view. First the
    /// chain's base, the committed handle the oldest link was proposed on,
    /// must still be the adapter's current generation (module doc);
    /// otherwise the oldest link fails
    /// (`Storage`, consumed as a refused commit is, contract item 1) and its
    /// children stay live.
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
        let oldest = chain.last().expect("non-empty");
        oldest.check_base_current()?;
        // Each state-changing link becomes a real revision. A later error does
        // not roll those commits back; auth_wal handles that as a poisoned chain.
        for p in chain.iter().rev() {
            p.commit_one()?;
        }
        Ok(self.root)
    }
}

impl FwProposal {
    /// Check the committed parent of this chain's oldest live link against the
    /// engine head by revision identity. Return `Ok(())` if it is current, or if
    /// there is no live link with a committed parent to check.
    /// On mismatch, replace this handle's live state with Failed (releasing its
    /// proposal and database reference) and return `Error::Storage`. This mirrors
    /// a failed oldest-link commit; children are not consumed by this check.
    fn check_base_current(&self) -> Result<()> {
        let current = match &*self.state.read() {
            FwState::Live(live) => match &live.parent {
                Some(base) if base.committed.load(Ordering::Acquire) => is_current(&base.state.read(), &live.db),
                _ => true,
            },
            _ => true,
        };
        if current {
            return Ok(());
        }
        let msg = "the chain's base revision is no longer the committed one (NotLatest)".to_owned();
        *self.state.write() = FwState::Failed(msg.clone());
        Err(Error::Storage(msg))
    }

    /// Commit this handle's live Firewood proposal, with ancestors already
    /// committed and the chain base checked by the caller. Returns `Ok(())`
    /// after replacing the live state with a committed view (or a stale marker
    /// if revision lookup fails). A Firewood commit error is stored as Failed
    /// and returned; subsequent reads/proposals/commits report that error.
    /// Holds the handle's write lock throughout the transition.
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
        // and later reads/proposals/commits report it.
        let placeholder = FwState::Failed("commit in progress".to_owned());
        // The write guard prevents readers from observing this placeholder
        // during a normal commit; every normal exit installs the final state.
        let FwState::Live(live) = std::mem::replace(&mut *state, placeholder) else {
            unreachable!()
        };
        let FwLive { raw, db, parent } = live;
        // The parent link goes with the live state (design §10 (b)): a
        // committed parent's pin on the base revision is released here.
        drop(parent);
        if let Err(e) = raw.commit() {
            // Keep firewood's own message: a retry reports it once, under
            // `Error::Storage`'s prefix.
            *state = FwState::Failed(e.to_string());
            return Err(fw_err(e));
        }
        let generation = db.generation.fetch_add(1, Ordering::AcqRel) + 1;
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
            FwState::Empty { db: weak, generation }
        } else {
            match db.revision(trie_hash(self.root)) {
                Ok(rev) => {
                    let view: firewood::api::ArcDynDbView = rev;
                    FwState::Committed {
                        view,
                        db: weak,
                        generation,
                    }
                }
                Err(_) => FwState::Stale,
            }
        };
        self.committed.store(true, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::common::TempDir;

    #[test]
    fn firewood_rejects_head_change_before_generation_publication() {
        type K = Vec<u8>;
        type V = Vec<u8>;
        let dir = TempDir::new("generation-publication");
        let mut engine = FirewoodStorage::new(FirewoodConfig::new(dir.db())).unwrap();
        let head = <FirewoodStorage as StorageEngine<K, V>>::head(&engine).unwrap();
        let put = |value: u8| BatchOp::Put {
            key: b"key".to_vec(),
            value: vec![value],
        };
        StorageProposal::<K, V>::commit(&head.propose(&[put(1)], None).unwrap()).unwrap();
        let old_head = <FirewoodStorage as StorageEngine<K, V>>::head(&engine).unwrap();

        // Stop at the point between Firewood accepting a commit and the
        // adapter publishing its generation, without relying on scheduling.
        engine
            .db()
            .unwrap()
            .propose(fw_batch(&[put(2)]))
            .unwrap()
            .commit()
            .unwrap();
        assert!(matches!(
            old_head.propose(&[put(3)], None),
            Err(Error::ProposalOrphaned)
        ));
        assert_eq!(
            StorageProposal::<K, V>::get(&old_head, &b"key".to_vec()).unwrap(),
            Some(vec![1])
        );
        <FirewoodStorage as StorageEngine<K, V>>::close(&mut engine).unwrap();
    }

    #[test]
    fn firewood_commit_permits_validate_before_truncate_and_persist_on_close() {
        type K = Vec<u8>;
        type V = Vec<u8>;
        let dir = TempDir::new("commit-permits");
        let mut config = FirewoodConfig::new(dir.db());
        assert_eq!(config.commit_permits.get(), 1);
        config.revisions = 33;
        config.commit_permits = NonZeroU64::new(32).unwrap();
        let mut engine = FirewoodStorage::new(config.clone()).unwrap();
        let mut view = <FirewoodStorage as StorageEngine<K, V>>::head(&engine).unwrap();
        let key = b"key".to_vec();
        for value in 0..3u8 {
            view = view
                .propose(
                    &[BatchOp::Put {
                        key: key.clone(),
                        value: vec![value],
                    }],
                    None,
                )
                .unwrap();
        }
        let root = StorageProposal::<K, V>::commit(&view).unwrap();
        drop(view);
        <FirewoodStorage as StorageEngine<K, V>>::close(&mut engine).unwrap();

        let mut invalid = config.clone();
        invalid.truncate = true;
        invalid.revisions = 32;
        assert!(matches!(FirewoodStorage::new(invalid), Err(Error::InvalidConfig(_))));

        // The refused truncate must preserve the database. Close must have
        // persisted the chain even though it never exhausted its 32 permits.
        let mut reopened = FirewoodStorage::new(config).unwrap();
        assert_eq!(<FirewoodStorage as StorageEngine<K, V>>::root(&reopened), root);
        let head = <FirewoodStorage as StorageEngine<K, V>>::head(&reopened).unwrap();
        assert_eq!(StorageProposal::<K, V>::get(&head, &key).unwrap(), Some(vec![2]));
        <FirewoodStorage as StorageEngine<K, V>>::close(&mut reopened).unwrap();
    }
}
