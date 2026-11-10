//! Whole-set rehash (design §7.2, R34): no incremental state. The
//! commitment is one domain-separated BLAKE3 hash over every entry of the set in key
//! order, each as the §7.2 entry item (`tag ‖ key ‖ value`, fields `u32 LE`
//! length-prefixed), behind a `u64 LE` entry count. Its cost is
//! proportional to the set, not to the batch: the entries are serialised
//! into one buffer and hashed with one `update`, since BLAKE3 runs near
//! memory speed over a large input and at call overhead over many small
//! ones. Design §7.2 and §14 record the measurements against the lattice,
//! which wins only for a large set with small blocks.

use crate::set::EntryRef;
use crate::types::{Digest, Key, len_prefix};
use crate::vc::{VectorCommitment, item_fields};

const DOMAIN: &str = "right_cash/rehash";

/// Bytes reserved per entry before serialising: three length prefixes, the
/// tag, a 32-byte key and a 32-byte value; the buffer grows past it.
const ESTIMATE_PER_ENTRY: usize = 3 * 4 + 1 + 32 + 32;

/// Parameter-free scheme.
#[derive(Clone, Default)]
pub struct Rehash;

impl Rehash {
    pub fn new() -> Self {
        Self
    }
}

impl<K: Key> VectorCommitment<K> for Rehash {
    fn scheme_id(&self) -> &'static str {
        "rehash"
    }

    fn empty(&self) -> Self {
        Self
    }

    /// Intentionally ignore individual transitions: `commitment` receives
    /// the complete final set, so this scheme has no incremental state to update.
    fn apply(&mut self, _key: &K, _old: Option<EntryRef<'_>>, _new: Option<EntryRef<'_>>) {}

    fn commitment<'a>(&mut self, entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>) -> Digest {
        let count = entries.len();
        // Serialize the iterator's order as supplied. The snapshot guarantees
        // distinct ascending keys; this scheme does not canonicalize them here.
        let mut buf = Vec::with_capacity(8 + count * ESTIMATE_PER_ENTRY);
        buf.extend_from_slice(&(count as u64).to_le_bytes());
        for (key, entry) in entries {
            for field in item_fields(key, entry) {
                buf.extend_from_slice(&len_prefix(field.len()));
                buf.extend_from_slice(field);
            }
        }
        let mut hasher = blake3::Hasher::new_derive_key(DOMAIN);
        // Single-threaded by decision: the parallel variant measured slower
        // in situ (design §14).
        hasher.update(&buf);
        *hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::set::Entry;

    type K = Vec<u8>;
    type V = Vec<u8>;

    fn digest(entries: &BTreeMap<K, Entry<V>>) -> Digest {
        VectorCommitment::<K>::commitment(&mut Rehash, entries.iter().map(|(k, e)| (k, e.as_ref())))
    }

    #[test]
    fn covers_every_entry_and_distinguishes_tombstones() {
        let mut a = BTreeMap::new();
        a.insert(b"k1".to_vec(), Entry::Value(b"v1".to_vec()));
        a.insert(b"k2".to_vec(), Entry::Value(b"v2".to_vec()));
        let mut b = a.clone();
        assert_eq!(digest(&a), digest(&b));
        b.insert(b"k2".to_vec(), Entry::Value(b"v3".to_vec()));
        assert_ne!(digest(&a), digest(&b));
        b.insert(b"k2".to_vec(), Entry::Value(Vec::new()));
        let empty_value = digest(&b);
        b.insert(b"k2".to_vec(), Entry::Tombstone);
        assert_ne!(empty_value, digest(&b));
        b.remove(&b"k2"[..]);
        assert_ne!(digest(&a), digest(&b));
        assert_ne!(digest(&BTreeMap::new()), digest(&b));
    }

    #[test]
    fn fields_are_delimited() {
        let mut a = BTreeMap::new();
        a.insert(b"ab".to_vec(), Entry::Value(b"c".to_vec()));
        let mut b = BTreeMap::new();
        b.insert(b"a".to_vec(), Entry::Value(b"bc".to_vec()));
        assert_ne!(digest(&a), digest(&b));
    }
}
