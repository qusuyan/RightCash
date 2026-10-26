//! KZG polynomial commitment (design §7.2, next): one G1 element plus a
//! persistent `key -> slot` map; constant-size membership proofs. Needs a
//! trusted setup with at least `slots` powers. Open questions §12.2
//! (slot count vs capacity, collision policy).

use std::sync::Arc;

use crate::set::{Entry, PersistentMap};
use crate::types::{Digest, Key, Value};
use crate::vc::{Proof, VectorCommitment};

/// Trusted-setup powers; loaded once and shared by every snapshot.
pub struct Setup {
    // g1_powers: Vec<G1>, g2_tau: G2, lagrange basis for `slots`
    pub slots: usize,
}

#[derive(Clone)]
pub struct Kzg {
    setup: Arc<Setup>,
}

#[derive(Clone)]
pub struct KzgState {
    // commitment: G1 (48 bytes compressed; hashed to 32 for the root)
    // slots: im::OrdMap<K, usize> occupancy map
}

impl Kzg {
    pub fn new(setup: Arc<Setup>) -> Self {
        Self { setup }
    }
}

impl<K: Key, V: Value> VectorCommitment<K, V> for Kzg {
    type State = KzgState;

    fn scheme_id(&self) -> &'static str {
        "kzg"
    }

    fn params_digest(&self) -> Digest {
        todo!("sha256(setup hash || slots)")
    }

    fn empty(&self) -> Self::State {
        todo!()
    }

    fn apply(&self, state: &mut Self::State, key: &K, old: Option<&Entry<V>>, new: Option<&Entry<V>>) {
        todo!("C += (f(new) - f(old)) * L_slot(key)")
    }

    fn commitment(&self, state: &Self::State) -> Digest {
        todo!("sha256 of the compressed G1 point")
    }

    fn supports_proofs(&self) -> bool {
        true
    }

    fn prove(&self, state: &Self::State, set: &PersistentMap<K, Entry<V>>, key: &K) -> Option<Proof> {
        todo!()
    }

    fn verify(&self, commitment: &Digest, key: &K, entry: Option<&Entry<V>>, proof: &Proof) -> bool {
        todo!()
    }
}
