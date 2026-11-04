//! Vector commitment schemes over the write set (design §7).
//!
//! The trait supports incremental and whole-set implementations. `apply`
//! describes one key's transition, including eviction; incremental schemes
//! update their state from it. Cost depends on the scheme and value sizes
//! (Merkle updates a path; LtHash hashes changed entries). `rehash` ignores
//! transitions and hashes all entries supplied to `commitment` instead.
//! Methods take `&self` because a scheme instance carries its parameters
//! (KZG trusted setup, Merkle depth). Proof methods arrive with milestone 5
//! (design §7.4).

pub mod lthash;
pub mod merkle;
pub mod rehash;

use crate::set::EntryRef;
use crate::types::{Digest, Key};

/// Return three borrowed byte slices encoding `entry` under `key`: tag
/// (`0x00` tombstone, `0x01` value), key bytes, and value bytes (empty for a
/// tombstone). Does not allocate or add length prefixes; each scheme does its
/// own hashing. The tag distinguishes a tombstone from an empty value.
pub(crate) fn item_fields<'a, K: Key>(key: &'a K, entry: EntryRef<'a>) -> [&'a [u8]; 3] {
    match entry {
        EntryRef::Value(value) => [&[0x01], key.as_ref(), value],
        EntryRef::Tombstone => [&[0x00], key.as_ref(), &[]],
    }
}

pub trait VectorCommitment<K: Key>: Clone + Send + Sync + 'static {
    /// Commitment state of the set at a node; copied with the snapshot at
    /// every proposal (design §4), so it must be cheap to clone.
    type State: Clone + Send + Sync;

    /// Identifier hashed into the root preimage (design §7.3).
    fn scheme_id(&self) -> &'static str;

    /// Digest of the instance parameters (a KZG setup hash and slot count,
    /// later). Enters the root preimage (design §7.3). Zeros for a
    /// parameter-free scheme, the default (every v0 scheme).
    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }

    /// Return fresh state representing no entries, including no tombstones.
    fn empty(&self) -> Self::State;

    /// Update mutable `state` for borrowed `key` changing from `old` to `new`.
    /// `old` must describe the entry currently represented by `state`; `new`
    /// describes the retained entry afterward. `None` means no entry, whereas
    /// `Some(Tombstone)` is a present, authenticated delete marker.
    ///
    /// Returns `()`. Borrowed entry bytes need live only for this call. An
    /// incremental scheme updates its state; a whole-set scheme may ignore
    /// this call and recompute from `commitment`'s complete entry iterator.
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>);

    /// Return the 32-byte digest of the set, without mutating `state`.
    /// `state` is the result of applying all transitions; `entries` iterates
    /// every retained entry, tombstones included, in ascending distinct-key
    /// order. Both inputs must describe the same set.
    ///
    /// Incremental schemes use `state` and may ignore the iterator. Whole-set
    /// schemes consume `entries`; they do not sort or deduplicate it. The
    /// resulting digest is one term of the combined auth_wal root.
    fn commitment<'a>(
        &self,
        state: &Self::State,
        entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>,
    ) -> Digest;
}
