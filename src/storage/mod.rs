//! Storage engine abstraction (design §10). Firewood is the backend of record
//! (R13); v0 ships `MemoryStorage`, a model of firewood. QMDB keeps
//! interface-level support with speculation disabled. The Go layer never sees
//! which backend is in use.
//!
//! Ownership (R19, R20): the engine is owned by the writer (`AuthWal`), which
//! closes it through `&mut self`; everything auth_wal reads from or proposes
//! on storage goes through a proposal handle (a node's pinned view,
//! `StorageEngine::head` for the genesis node), so there is no revision type,
//! no engine-level read, propose or revision lookup here. Proof methods arrive
//! with milestone 5 (design §7.4).

pub mod memory;

#[cfg(test)]
mod contract_tests;
#[cfg(feature = "firewood")]
pub mod firewood;

#[cfg(feature = "firewood")]
pub use firewood::{FirewoodConfig, FirewoodStorage};
pub use memory::{MemoryConfig, MemoryStorage};

use crate::types::{BatchOp, Hash, Key, Result, Value};

/// A storage proposal handle: an uncommitted, chainable set of changes with a
/// known root, or, once committed (by `commit` or from
/// [`StorageEngine::head`]), a readable view of its committed root that can
/// still be proposed on. Handles are shared (`Arc`) between the node that
/// created them, their children's pinned views and speculative steps, hence
/// `&self` everywhere.
///
/// Adapter contract (design §10):
/// 1. `commit` commits this proposal and any uncommitted ancestors, oldest
///    first; `commit` of a handle that is already committed (a second
///    `commit`, or a `head` handle) returns `ProposalConsumed`. A failed
///    `commit` consumes exactly the handle whose own engine commit failed (on
///    `NotLatest`, the oldest uncommitted ancestor): that handle reports the
///    failure, as `Error::Storage`, on every later operation, a retried
///    `commit` included (design §9.2), and no longer pins the engine; a
///    child's later `commit` reports the ancestor's failure, but the child
///    itself is not consumed and stays live, readable and pinning, until
///    dropped.
/// 2. After `commit`, `get`/`root` keep working as a view of the committed
///    root (re-based to the backend's own revision lookup internally) and
///    return `RevisionStale` once the engine's window has moved past it.
///    Implicitly committed ancestors may error, never return wrong data.
/// 3. Committing a child whose parent handle was already committed succeeds:
///    the check is "the chain's base, after committing its uncommitted
///    ancestors, is the committed state", not "this proposal's base is".
/// 4. `propose` on a **committed** handle proposes on the engine's committed
///    state, provided the engine's committed root equals this handle's root
///    both before and after the call (an engine whose root is not a state
///    function compares revision identity instead), and returns
///    `ProposalOrphaned` otherwise; it never panics. auth_wal's writer
///    proposes on committed handles for the plain proposal of a child whose
///    parent's pinned view is committed (a child of the head, or of a live
///    node that inherited the head's view; `F` never does, it chains on the
///    latest view), where the check cannot fail, and the worker's first
///    step after a retarget, on the target's committed handle: in the
///    pipelined flow that step may be hashing on the handle being committed
///    (a chained proposal, not the committed path; the engine commit waits
///    on the handle lock), and a step proposed
///    after the commit takes the committed path with the right root; a
///    commit that moves the root under a step's proposal costs one refused
///    step, never a wrong state (design §9.2, §10).
/// 5. `propose` is side-effect free: a proposal that is built and dropped
///    leaves no trace on the committed state or on the root of any future
///    proposal, and a `Delete` of a key the state lacks is a no-op on the
///    root (a state-function root is unchanged by it), which the final
///    proposal's `Delete`-if-absent retractions and tombstone evictions of
///    never-stored keys rely on. Speculative steps rely on the first; the
///    contract tests verify both on both backends.
/// 6. Committed proposals do not pin the engine: `close` succeeds once no
///    live proposal handle (uncommitted and not failed) is alive, however
///    many committed proposal handles remain (`AuthWal::close` relies on it:
///    the committed node keeps its pinned view), and reports `ActiveHandles`
///    while one is (both v0 adapters count them). `close` is idempotent;
///    afterwards reads through committed handles keep answering, and
///    `propose` on a committed handle returns `Closed`, never panics.
/// 7. [`StorageEngine::head`] returns a committed handle at the committed
///    root (`EMPTY_ROOT` on a fresh engine), readable like any committed
///    proposal and proposable per item 4: `head()?.propose(..)` is the one
///    way to propose on the committed state.
pub trait StorageProposal<K: Key, V: Value>: Send + Sync + Sized {
    fn root(&self) -> Hash;

    /// `RevisionStale` once the engine has dropped this root from its window.
    fn get(&self, key: &K) -> Result<Option<V>>;

    /// Read-through warming of the trie paths of `keys` through this handle:
    /// one `get` per key, which on firewood under `CacheReadStrategy::All`
    /// populates the node cache along the path; it continues past errors
    /// and returns the first. auth_wal's hints do not call it (design §9.4,
    /// R26: the steps are the warming); the benchmark's engine-only baseline
    /// uses it to emulate a native warm call, and the contract tests
    /// exercise it.
    fn prefetch(&self, keys: &[K]) -> Result<()> {
        let mut first_err = None;
        for key in keys {
            if let Err(e) = self.get(key) {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Child proposal on top of this one (chaining; design §9), or, on a
    /// committed handle, on the committed state (item 4).
    fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self>;

    /// Commits this proposal and any uncommitted ancestors, oldest first.
    /// Once-only; see the contract above.
    fn commit(&self) -> Result<Hash>;
}

pub trait StorageEngine<K: Key, V: Value>: Send + Sync + 'static {
    type Proposal: StorageProposal<K, V> + 'static;

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

    /// Committed root. A fresh engine reports `EMPTY_ROOT` (`AuthWal::new`'s
    /// freshness check, design §1; the benchmark baseline).
    fn root(&self) -> Hash;

    /// A committed handle at the committed root (contract item 7): the
    /// genesis node's pinned view, and the one way to propose on the
    /// committed state, so neither the worker nor a reader ever needs the
    /// engine.
    fn head(&self) -> Result<Self::Proposal>;

    /// Pass-through hook for `AuthWal::prune`; no-op by default.
    fn prune(&self) -> Result<()> {
        Ok(())
    }

    /// Releases the engine (`AuthWal::close`). Exclusive: the writer owns the
    /// engine (R19), so an adapter keeps its database in a plain field and
    /// needs no lock to take it out. `ActiveHandles` while a live proposal
    /// handle is alive (item 6; both v0 adapters count them); idempotent. The
    /// default is a no-op for an engine with nothing to release.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
