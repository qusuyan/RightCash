//! Lattice hash (design §7.2, v0 default): additively homomorphic,
//! `C <- C - H(k, old) + H(k, new)` over 1024 u16 lanes. Authenticates the set
//! as a whole; no per-key proofs.

use crate::lattice::{LANES, LatticeAccumulator};
use crate::set::{Entry, PersistentMap};
use crate::types::{Digest, Key, Value};
use crate::vc::{Proof, VectorCommitment};

const TAG: &[u8] = b"auth_wal/lthash";

/// Parameter-free scheme.
#[derive(Clone, Default)]
pub struct LtHash;

#[derive(Clone)]
pub struct LtHashState {
    acc: LatticeAccumulator<LANES>,
}

impl LtHash {
    pub fn new() -> Self {
        Self
    }
}

impl<K: Key, V: Value> VectorCommitment<K, V> for LtHash {
    type State = LtHashState;

    fn scheme_id(&self) -> &'static str {
        "lthash"
    }

    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }

    fn empty(&self) -> Self::State {
        LtHashState {
            acc: LatticeAccumulator::new(TAG),
        }
    }

    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>) {
        todo!("acc.remove(encode(key, old)); acc.add(encode(key, new)); tombstones encoded distinctly from values")
    }

    fn commitment(&self, state: &Self::State) -> Digest {
        state.acc.digest()
    }

    fn supports_proofs(&self) -> bool {
        false
    }

    fn prove(&self, state: &Self::State, set: &PersistentMap<K, Entry<V>>, key: &K) -> Option<Proof> {
        None
    }

    fn verify(&self, commitment: &Digest, key: &K, entry: Option<&Entry<V>>, proof: &Proof) -> bool {
        false
    }
}
