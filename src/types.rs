//! Shared bounds and plain data types (design §3, §4).

use std::borrow::Borrow;
use std::hash::Hash as StdHash;
use std::sync::Arc;

/// Bound for key types. The set treats keys as opaque and orders them by
/// `Ord` (canonical iteration, eviction tie-breaks); `Ord` must agree with
/// the lexicographic order of `as_ref()` bytes, which holds for every
/// blanket impl below. `Borrow<[u8]>` lets the memory model's ordered map
/// position its prefix-delete range scan (`MemoryStorage`; the set itself
/// has no range scan, R22); it makes `get(&K)` on the std maps ambiguous, so
/// lookups by `&K` name the key type (`map.get::<K>(key)`). Only the
/// commitment scheme needs the bytes. Both
/// keys and values should be **cheap to clone**: every proposal copies the
/// whole set (design §4, R20), so the FFI layer uses `Arc<[u8]>` for both
/// (one reference count per clone) and fixed-size arrays are inline.
/// Blanket-implemented for the byte containers the FFI layer and the
/// benchmarks use (design §4, "Generic key and value types").
pub trait Key: Clone + Eq + Ord + AsRef<[u8]> + Borrow<[u8]> + Send + Sync + 'static {}

/// Bound for value types; see [`Key`]. `from_bytes` lets a storage adapter
/// rebuild a value from the engine's bytes; for a fixed-size array a length
/// mismatch is corrupted data and panics.
pub trait Value: Clone + Eq + AsRef<[u8]> + Send + Sync + 'static {
    fn from_bytes(bytes: &[u8]) -> Self;
}

impl Key for Arc<[u8]> {}
impl Key for Box<[u8]> {}
impl Key for Vec<u8> {}
impl<const N: usize> Key for [u8; N] {}

impl Value for Arc<[u8]> {
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.into()
    }
}
impl Value for Box<[u8]> {
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.into()
    }
}
impl Value for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}
impl<const N: usize> Value for [u8; N] {
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.try_into().expect("stored value has the fixed length of its type")
    }
}

/// Length of a root in bytes.
pub const ROOT_LENGTH: usize = 32;

/// A 32-byte digest that is *not* a root: a set commitment, a parameter
/// digest, a metadata hash (design §7.3).
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
    /// Part of the `ffi` vocabulary. The engines apply it (firewood, the
    /// memory model); the write set does not: `propose` rejects a batch that
    /// contains one (`PrefixDeleteUnsupported`), as QMDB does behind
    /// qmdb-ffi, because a bounded set cannot apply a prefix delete to the
    /// keys it does not hold and the adapter never issues one (it bumps
    /// storage generations instead; design §4, R22). The prefix is raw
    /// bytes, not a `K`.
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
    /// `Commit` of a node whose parent was dropped before it was committed
    /// (design §8); also an engine's own answer when a storage proposal is
    /// attempted on a committed handle whose root the engine has moved past
    /// (design §10 item 4), surfaced through `propose`.
    #[error("proposal's chain does not start at the committed state")]
    ProposalOrphaned,
    /// `Commit` of a proposal whose parent is not the head: a deeper
    /// pipelined node (commit the parent first, as the adapter always does),
    /// a node on a line that lost, or a child of a retained revision that is
    /// no longer the head (both never committable); firewood's
    /// `NotLatest` (design §8; qmdb-ffi's cascade commit is not offered,
    /// R20).
    #[error("proposal's parent is not the committed state; commit it first")]
    ParentUncommitted,
    /// `Propose` of a batch containing `PrefixDelete` (design §4, R22).
    #[error("PrefixDelete is not supported by the write set (as by QMDB); the adapter never issues one")]
    PrefixDeleteUnsupported,
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
    /// The storage engine refused to close because a live (uncommitted, not
    /// failed) storage proposal is still held (design §10 item 6): a Rust
    /// caller still holds a handle to an uncommitted proposal that owns a
    /// storage proposal. The keep-alive wait itself is Go-side
    /// (`ErrActiveKeepAliveHandles`, design §3).
    #[error("live storage proposal handles outstanding")]
    ActiveHandles,
    /// `New` could not spawn the instance's worker thread (design §9.2, R17).
    #[error("speculation worker unavailable: {0}")]
    WorkerUnavailable(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("storage engine error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;
