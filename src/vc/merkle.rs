//! Sparse Merkle tree reference scheme (design §7.2): slow but proof-friendly
//! baseline for tests and benchmarks. Leaves are addressed by `SHA-256(key)`
//! at depth 256 (no slots, no collisions, parameter-free); a leaf hashes the
//! §7.2 entry item. The state is the whole (persistent) tree, since
//! `apply(old, new)` must recompute a path.

use crate::set::{Entry, PersistentMap};
use crate::types::{Digest, Key, Value};
use crate::vc::{Proof, VectorCommitment};

/// Parameter-free: depth is fixed at 256.
#[derive(Clone, Default)]
pub struct Merkle;

pub const DEPTH: u32 = 256;

#[derive(Clone)]
pub struct MerkleState {
    root: Digest,
    /// Persistent map of occupied node positions `(level, path prefix)` to
    /// hashes; absent = default (empty-subtree) hash for that level.
    nodes: im::OrdMap<(u32, Digest), Digest>,
}

impl Merkle {
    pub fn new() -> Self {
        Self
    }
}

impl<K: Key, V: Value> VectorCommitment<K, V> for Merkle {
    type State = MerkleState;

    fn scheme_id(&self) -> &'static str {
        "merkle"
    }

    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }

    fn empty(&self) -> Self::State {
        todo!()
    }

    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>) {
        todo!("path = SHA-256(key); leaf = H(entry item); rehash the 256-node path")
    }

    fn commitment(&self, state: &Self::State) -> Digest {
        state.root
    }

    fn supports_proofs(&self) -> bool {
        true
    }

    fn prove(&self, state: &Self::State, set: &PersistentMap<K, Entry<V>>, key: &K) -> Option<Proof> {
        todo!("membership and non-membership paths")
    }

    fn verify(&self, commitment: &Digest, key: &K, entry: Option<&Entry<V>>, proof: &Proof) -> bool {
        todo!()
    }
}
