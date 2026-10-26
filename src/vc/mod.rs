//! Vector commitment schemes over the write set (design §7).
//!
//! The trait is incremental: every scheme updates its state from
//! `(old, new)` for one key, so per-block cost is proportional to the batch,
//! not the set. Methods take `&self` because a scheme instance carries its
//! parameters (KZG trusted setup, Merkle depth).

#[cfg(feature = "kzg")]
pub mod kzg;
#[cfg(feature = "lthash")]
pub mod lthash;
#[cfg(feature = "merkle")]
pub mod merkle;

use crate::set::{Entry, PersistentMap};
use crate::types::{Digest, Key, Value};

/// Opaque, scheme-specific proof bytes (design §7.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proof(pub Vec<u8>);

pub trait VectorCommitment<K: Key, V: Value>: Clone + Send + Sync + 'static {
    /// Commitment state carried by every snapshot; must be cheap to clone.
    type State: Clone + Send + Sync;

    /// Identifier hashed into the root preimage (design §7.3).
    fn scheme_id(&self) -> &'static str;

    /// Digest of the instance parameters (KZG setup hash and slot count,
    /// Merkle depth; zeros for lthash). Enters the root preimage (design §7.3).
    fn params_digest(&self) -> Digest;

    fn empty(&self) -> Self::State;

    /// Incremental update for one key. `None` means absent.
    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>);

    /// 32-byte digest of the state, used in the root.
    fn commitment(&self, state: &Self::State) -> Digest;

    /// Whether `prove` can return `Some` (design §7.4).
    fn supports_proofs(&self) -> bool;

    /// Membership / non-membership proof, if the scheme supports one.
    fn prove(&self, state: &Self::State, set: &PersistentMap<K, Entry<V>>, key: &K) -> Option<Proof>;

    fn verify(&self, commitment: &Digest, key: &K, entry: Option<&Entry<V>>, proof: &Proof) -> bool;
}
