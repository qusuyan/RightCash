//! Lattice hash (design §7.2, v0 default): additively homomorphic,
//! `C <- C - H(k, old) + H(k, new)` over 1024 u16 lanes, an item being the
//! §7.2 entry item (`tag || key || value`). Authenticates the set as a whole;
//! no per-key proofs.

use crate::lattice::LatticeAccumulator;
use crate::set::EntryRef;
use crate::types::{Digest, Key};
use crate::vc::{VectorCommitment, item_fields};

const TAG: &str = "auth_wal/lthash";

/// Parameter-free scheme.
#[derive(Clone, Default)]
pub struct LtHash;

#[derive(Clone)]
pub struct LtHashState {
    acc: LatticeAccumulator,
}

impl LtHash {
    pub fn new() -> Self {
        Self
    }
}

impl<K: Key> VectorCommitment<K> for LtHash {
    type State = LtHashState;

    fn scheme_id(&self) -> &'static str {
        "lthash"
    }

    fn empty(&self) -> Self::State {
        LtHashState {
            acc: LatticeAccumulator::new(TAG),
        }
    }

    fn apply(&self, state: &mut Self::State, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>) {
        if let Some(entry) = old {
            state.acc.remove_fields(&item_fields(key, entry));
        }
        if let Some(entry) = new {
            state.acc.add_fields(&item_fields(key, entry));
        }
    }

    fn commitment<'a>(
        &self,
        state: &Self::State,
        _entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>,
    ) -> Digest {
        state.acc.digest()
    }
}
