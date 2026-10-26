//! Authenticated reads (design §7.4): a proof that `key` has a given status
//! under a combined root. Milestone 6 fills it in; the shape exists now so
//! `Node` keeps everything the preimage needs.

use crate::root::RootPreimage;
use crate::storage::StorageEngine;
use crate::types::{Hash, Key, Value};
use crate::vc::{Proof, VectorCommitment};

/// What the proof claims about the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetClaim<V> {
    /// In the set with this value.
    Value(V),
    /// In the set as a tombstone: the key is deleted regardless of storage.
    Tombstone,
    /// Not in the set; `storage_value` is what storage holds under
    /// `preimage.storage_root` (`None` = absent), proven by `storage_proof`.
    Absent { storage_value: Option<V> },
}

/// The preimage (so the root can be recomputed), the scheme's set proof and,
/// for `SetClaim::Absent`, the storage engine's proof under
/// `preimage.storage_root`.
pub struct AuthProof<V> {
    pub preimage: RootPreimage,
    pub claim: SetClaim<V>,
    /// Always present: under a scheme without per-key proofs (`lthash`)
    /// `Revision::prove` returns `Unsupported` instead of a proof.
    pub set_proof: Proof,
    /// Present iff `claim` is `Absent`.
    pub storage_proof: Option<Vec<u8>>,
}

impl<V: Value> AuthProof<V> {
    /// Binding checks first: `root::combine(&preimage) == *root`,
    /// `preimage.scheme_id == vc.scheme_id()`,
    /// `preimage.scheme_params == vc.params_digest()`,
    /// `preimage.engine_id == S::ENGINE_ID`. Then the set proof with `vc`
    /// (the verifier's own instance of the scheme) against
    /// `preimage.set_commitment` and, for `Absent`, the storage proof with
    /// `S::verify_proof` against `preimage.storage_root`. The verifier needs
    /// no engine instance, only its type (turbofish at the call site).
    pub fn verify<K, C, S>(&self, vc: &C, root: &Hash, key: &K) -> bool
    where
        K: Key,
        C: VectorCommitment<K, V>,
        S: StorageEngine<K, V>,
    {
        todo!("design §7.4")
    }
}
