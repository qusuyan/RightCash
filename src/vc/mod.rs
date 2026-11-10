//! Vector commitment schemes over the write set (design §7).
//!
//! The trait supports incremental and whole-set implementations. `apply`
//! describes one key's transition, including eviction; incremental schemes
//! update their state from it. Cost depends on the scheme and value sizes
//! (Merkle updates a path; LtHash hashes changed entries). `rehash` ignores
//! transitions and hashes all entries supplied to `commitment` instead.
//! `merkle-blake3` also builds over all entries there, keeping its tree for
//! as long as the snapshot lives, but reuses the parent's leaf
//! hashes for the entries `apply` did not report, so it relies on every
//! change being reported.
//! A scheme instance carries its parameters (KZG trusted setup, Merkle
//! branching) and the commitment state of one snapshot's set, so `apply` and
//! `commitment` take `&mut self`. Proof methods arrive with milestone 5
//! (design §7.4).

pub(crate) mod blake3_lanes;
pub mod lthash;
pub mod merkle;
pub mod merkle_blake3;
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

/// A commitment scheme instance: its parameters together with the
/// commitment state of one set. Each snapshot owns one, a clone of its
/// parent's carried through the transitions between them (design §4, §7.1),
/// so a clone must be cheap: large parameters (a KZG setup) sit behind an
/// `Arc`.
pub trait VectorCommitment<K: Key>: Clone + Send + Sync + 'static {
    /// Identifier hashed into the root preimage (design §7.3).
    fn scheme_id(&self) -> &'static str;

    /// Digest of the instance parameters (a KZG setup hash and slot count,
    /// later; `merkle-blake3`'s branching factor). Enters the root preimage
    /// (design §7.3). Zeros for a parameter-free scheme, the default.
    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }

    /// The same scheme and parameters over no entries, including no
    /// tombstones. The snapshot constructors start from this, whatever the
    /// instance they are handed has seen.
    fn empty(&self) -> Self;

    /// Record borrowed `key` changing from `old` to `new`. `old` must
    /// describe the entry this instance currently represents; `new`
    /// describes the retained entry afterward. `None` means no entry,
    /// whereas `Some(Tombstone)` is a present, authenticated delete marker.
    ///
    /// Returns `()`. Borrowed entry bytes need live only for this call. An
    /// incremental scheme updates its state; a whole-set scheme may ignore
    /// this call and recompute from `commitment`'s complete entry iterator
    /// (`rehash`), or use it to reuse work kept from the parent
    /// (`merkle-blake3`'s leaf hashes). The caller (`Snapshot::apply`'s
    /// merge, design §4) therefore reports every key whose entry differs
    /// between parent and child, exactly once, in ascending key order, on a
    /// clone of the parent's committed instance, and then calls
    /// `commitment` on that same instance.
    fn apply(&mut self, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>);

    /// Return the 32-byte digest of the set. The instance has applied all
    /// transitions; `entries` iterates every retained entry, tombstones
    /// included, in ascending distinct-key order. Both must describe the
    /// same set. Called once per snapshot, when it is built (design §7.1); a
    /// scheme may keep what it derives here (a whole-set Merkle tree), which
    /// the snapshot then owns, but the digest must depend on the set alone.
    ///
    /// Incremental schemes use their state and may ignore the iterator.
    /// Whole-set schemes consume `entries`; they do not sort or deduplicate
    /// it. The resulting digest is one term of the combined right_cash root.
    fn commitment<'a>(&mut self, entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>) -> Digest;
}
