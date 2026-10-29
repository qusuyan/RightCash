//! Vector commitment schemes over the write set (design §7).
//!
//! The trait is incremental: every scheme updates its state from
//! `(old, new)` for one key, so per-block cost is proportional to the batch,
//! not the set. Methods take `&self` because a scheme instance carries its
//! parameters (KZG trusted setup, Merkle depth). Proof methods arrive with
//! milestone 5 (design §7.4).

pub mod lthash;
pub mod merkle;

use crate::set::Entry;
use crate::types::{Digest, Key, Value};

pub trait VectorCommitment<K: Key, V: Value>: Clone + Send + Sync + 'static {
    /// Commitment state of the set at a node; copied with the snapshot at
    /// every proposal (design §4), so it must be cheap to clone.
    type State: Clone + Send + Sync;

    /// Identifier hashed into the root preimage (design §7.3).
    fn scheme_id(&self) -> &'static str;

    /// Digest of the instance parameters (a KZG setup hash and slot count,
    /// later). Enters the root preimage (design §7.3). Zeros for a
    /// parameter-free scheme, the default (both v0 schemes).
    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }

    fn empty(&self) -> Self::State;

    /// Incremental update for one key. An item is the entry under its key
    /// (design §7.2); the policy state is a root term of its own (design
    /// §7.3, R25). `None` means absent; `old` is the item the set held
    /// before the op (design §4 batch semantics).
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>);

    /// 32-byte digest of the state, used in the root.
    fn commitment(&self, state: &Self::State) -> Digest;
}
