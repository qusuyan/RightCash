//! Shared bounds and plain data types (design §3, §4).

use std::borrow::Borrow;
use std::hash::Hash as StdHash;

/// Bound for key types. The set treats keys as opaque and orders them by
/// `Ord` (canonical iteration, prefix scans, eviction tie-breaks); `Ord` must
/// agree with the lexicographic order of `as_ref()` bytes, which holds for
/// every blanket impl below. `Borrow<[u8]>` lets the ordered map position a
/// range scan at a byte prefix (`PersistentMap::keys_with_prefix`). Only the
/// commitment scheme needs the bytes. Blanket-implemented for the byte
/// containers the FFI layer and the benchmarks use (design §4, "Generic key
/// and value types").
pub trait Key: Clone + Eq + Ord + StdHash + AsRef<[u8]> + Borrow<[u8]> + Send + Sync + 'static {}

/// Bound for value types; see [`Key`].
pub trait Value: Clone + Eq + AsRef<[u8]> + Send + Sync + 'static {}

impl Key for Box<[u8]> {}
impl Key for Vec<u8> {}
impl<const N: usize> Key for [u8; N] {}

impl Value for Box<[u8]> {}
impl Value for Vec<u8> {}
impl<const N: usize> Value for [u8; N] {}

/// Length of a root in bytes.
pub const ROOT_LENGTH: usize = 32;

/// A 32-byte digest that is *not* a root: a set commitment, a policy digest,
/// a metadata hash (design §7.3).
pub type Digest = [u8; ROOT_LENGTH];

/// An opaque 32-byte combined root (design §7.3). Never comparable with
/// Ethereum MPT roots, QMDB roots or firewood roots. Deliberately not
/// `Default`: there is no meaningful zero root.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, StdHash, Debug)]
pub struct Hash(pub [u8; ROOT_LENGTH]);

/// Root reported while nothing has been committed: Ethereum's empty-root
/// constant `keccak256(rlp(""))`, matching `qmdb-ffi` so the adapter's genesis
/// idiom holds (design §3, §7.3).
pub const EMPTY_ROOT: Hash = Hash([
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6, 0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e, 0x5b, 0x48, 0xe0,
    0x1b, 0x99, 0x6c, 0xad, 0xc0, 0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
]);

/// One operation of a batch, mirroring the `ffi` `BatchOp` (design §3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchOp<K, V> {
    Put {
        key: K,
        value: V,
    },
    Delete {
        key: K,
    },
    /// Applied to matching set entries (range scan) and forwarded to storage
    /// in the same proposal; fails if the backend rejects it (QMDB does). The
    /// prefix is raw bytes, not a `K`: a prefix is shorter than a key.
    PrefixDelete {
        prefix: Box<[u8]>,
    },
}

/// Errors surfaced through the public API and the C ABI. Mirrors the
/// `qmdb-ffi` error vocabulary where one exists.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("no live proposal or retained revision has the requested root")]
    RevisionNotFound,
    #[error("the revision's storage view has left the engine's window")]
    RevisionStale,
    #[error("proposal has been committed or dropped")]
    ProposalConsumed,
    #[error("proposal's chain does not start at the committed state")]
    ProposalOrphaned,
    #[error("database is closed")]
    Closed,
    /// v0 opens only over an engine with nothing committed (design §1, §12.6).
    #[error("storage engine already has committed state; v0 requires a fresh engine")]
    EngineNotFresh,
    /// A speculative chain failed part-way through its one-at-a-time engine
    /// commits, leaving the engine head at a state no node owns (design §9.2).
    /// `Propose`/`Commit` refuse from then on: halt rather than diverge.
    #[error("instance poisoned by a failed mid-chain commit: {0}")]
    Poisoned(String),
    /// `close` was asked to wait for live handles and the deadline passed
    /// (mirrors qmdb-ffi's `ErrActiveKeepAliveHandles`, design §3).
    #[error("live proposal or revision handles outstanding")]
    ActiveHandles,
    #[error("operation not supported by this configuration: {0}")]
    Unsupported(&'static str),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("storage engine error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;
