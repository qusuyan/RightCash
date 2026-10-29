//! Sparse Merkle tree reference scheme (design §7.2): slow but proof-friendly
//! baseline for tests and benchmarks. Leaves are addressed by `SHA-256(key)`
//! at depth 256 (no slots, no collisions, parameter-free); a leaf hashes the
//! §7.2 entry item. The state is the whole (persistent) tree, cloned with the
//! snapshot in O(1), since `apply(old, new)` must recompute a path.

use crate::set::Entry;
use crate::types::{Digest, Key, Value};
use crate::vc::VectorCommitment;

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

    fn empty(&self) -> Self::State {
        todo!()
    }

    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>) {
        todo!("path = SHA-256(key); leaf = H(entry item); rehash the 256-node path")
    }

    fn commitment(&self, state: &Self::State) -> Digest {
        state.root
    }
}
