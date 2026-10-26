//! Storage engine abstraction (design §10). Firewood is the backend of record
//! (R13); v0 ships `MemoryStorage`, a model of firewood. QMDB keeps
//! interface-level support with speculation disabled. The Go layer never sees
//! which backend is in use.

pub mod memory;

pub use memory::{MemoryConfig, MemoryStorage};

use crate::types::{BatchOp, Hash, Key, Result, Value};

/// Read-only view at some storage root (committed revision or proposal).
pub trait ReadView<K: Key, V: Value>: Send + Sync {
    fn root(&self) -> Hash;
    /// `RevisionStale` once the engine has dropped this root from its window.
    fn get(&self, key: &K) -> Result<Option<V>>;
}

/// An uncommitted, chainable set of changes with a known root. Handles are
/// shared (`Arc`) between the node that created them, their children's pinned
/// views and speculative chains, hence `&self` everywhere.
///
/// Adapter contract (design §10):
/// 1. `commit` commits this proposal and any uncommitted ancestors, oldest
///    first; a second `commit` returns an error.
/// 2. After `commit`, `get`/`root` keep working as a view of the committed
///    root (re-based to a revision internally) and return `RevisionStale`
///    once the engine's window has moved past it. Implicitly committed
///    ancestors may error, never return wrong data.
/// 3. Committing a child whose parent handle was already committed succeeds:
///    the check is "the chain's base, after committing its uncommitted
///    ancestors, is the committed state", not "this proposal's base is".
/// 4. `propose` after `commit` is not required; auth_wal never does it
///    (`StorageView::propose` routes through the engine instead, deciding
///    with `is_committed`).
/// 5. `propose` is side-effect free: a proposal that is built and dropped
///    leaves no trace on the committed state or on the root of any future
///    proposal. Speculative links rely on it; milestone 4 verifies it on
///    firewood.
pub trait StorageProposal<K: Key, V: Value>: ReadView<K, V> + Sized {
    /// Child proposal on top of this one (chaining; design §9).
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self>;

    /// Commits this proposal and any uncommitted ancestors, oldest first.
    /// Once-only; see the contract above.
    fn commit(&self) -> Result<Hash>;

    /// Whether `commit` has succeeded on this handle. `StorageView::propose`
    /// uses it to decide between chaining on the proposal and routing through
    /// the engine (design §10 item 4); a root comparison cannot, because an
    /// uncommitted proposal can be a storage no-op with `root() == engine.root()`.
    fn is_committed(&self) -> bool;
}

/// A borrowed engine view, typed so an engine override of `prefetch` can use
/// its own proposal/revision types (a `&dyn ReadView` could only call `get`).
pub enum ViewRef<'a, K: Key, V: Value, S: StorageEngine<K, V> + ?Sized> {
    Proposal(&'a S::Proposal),
    Revision(&'a S::Revision),
}

impl<K: Key, V: Value, S: StorageEngine<K, V> + ?Sized> ViewRef<'_, K, V, S> {
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        match self {
            Self::Proposal(p) => p.get(key),
            Self::Revision(r) => r.get(key),
        }
    }
}

pub trait StorageEngine<K: Key, V: Value>: Send + Sync + 'static {
    type Proposal: StorageProposal<K, V> + 'static;
    type Revision: ReadView<K, V> + 'static;

    /// Stable identifier hashed into the root preimage (design §7.3): every
    /// engine reports `EMPTY_ROOT` before its first commit, so without it two
    /// networks on different engines would share roots.
    const ENGINE_ID: &'static str;

    /// Whether the root is a function of the stored state alone (firewood,
    /// `MemoryStorage`: true; QMDB: false, its root covers the op history).
    /// Speculative eviction requires `true` (design §9.3); `AuthWal::new`
    /// rejects `speculate` otherwise.
    const ROOT_IS_STATE_FUNCTION: bool;

    /// Whether each proposal of a committed chain becomes its own revision
    /// (firewood, `MemoryStorage`) rather than folding into one record
    /// (QMDB). Sizes the engine window: `K * (1 + rounds)` when speculating
    /// (design §8).
    const COMMITS_CHAIN_LINKS_SEPARATELY: bool;

    /// Size of the engine's revision window, checked against design §8.
    fn revisions(&self) -> usize;

    /// Committed root. A fresh engine reports `EMPTY_ROOT`.
    fn root(&self) -> Hash;

    /// Read from the committed state.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Read-only view at a committed root within the engine's window.
    fn revision(&self, root: Hash) -> Result<Self::Revision>;

    /// Proposal on the committed state.
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self::Proposal>;

    /// Prefetch hint (design §9.4): warm the trie paths of `keys` as read
    /// through `view`. Default: one `view.get` per key, which on firewood
    /// under `CacheReadStrategy::All` populates the node cache along the
    /// path; it continues past errors and returns the first. An engine with
    /// a native warm call overrides it.
    fn prefetch(&self, view: ViewRef<'_, K, V, Self>, keys: &[K]) -> Result<()> {
        let mut first_err = None;
        for key in keys {
            if let Err(e) = view.get(key) {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Pass-through hooks for `AuthWal::prune`/`close`; no-ops by default.
    fn prune(&self) -> Result<()> {
        Ok(())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Engine proof that `key` has its value under `root` (design §7.4;
    /// milestone 6). Default: `Unsupported`.
    fn prove(&self, root: Hash, key: &K) -> Result<Vec<u8>> {
        Err(crate::types::Error::Unsupported("storage engine has no proofs"))
    }

    /// Stateless verification of `prove`'s output; needs only the engine
    /// type. Default: rejects everything.
    fn verify_proof(root: &Hash, key: &K, value: Option<&V>, proof: &[u8]) -> bool {
        false
    }
}
