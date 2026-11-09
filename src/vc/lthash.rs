//! Lattice hash (design §7.2, v0 default): additively homomorphic,
//! `C <- C - H(k, old) + H(k, new)` over 1024 u16 lanes, an item being the
//! §7.2 entry item (`tag || key || value`). Authenticates the set as a whole;
//! no per-key proofs.

use crate::lattice::LatticeAccumulator;
use crate::set::EntryRef;
use crate::types::{Digest, Key};
use crate::vc::{VectorCommitment, item_fields};

const TAG: &str = "auth_wal/lthash";

/// Parameter-free scheme; the state is the accumulator of the set's items.
#[derive(Clone)]
pub struct LtHash {
    acc: LatticeAccumulator,
}

impl LtHash {
    /// The scheme over no entries.
    pub fn new() -> Self {
        Self {
            acc: LatticeAccumulator::new(TAG),
        }
    }
}

impl Default for LtHash {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key> VectorCommitment<K> for LtHash {
    fn scheme_id(&self) -> &'static str {
        "lthash"
    }

    fn empty(&self) -> Self {
        Self::new()
    }

    fn apply(&mut self, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>) {
        if let Some(entry) = old {
            self.acc.remove_fields(&item_fields(key, entry));
        }
        if let Some(entry) = new {
            self.acc.add_fields(&item_fields(key, entry));
        }
    }

    fn commitment<'a>(&mut self, _entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>) -> Digest {
        self.acc.digest()
    }
}
