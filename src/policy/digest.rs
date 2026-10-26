//! Order-independent incremental digest over `(key, height)` pairs, shared by
//! the v0 policies to implement `EvictionPolicy::digest` (design §6.1, §7.3).
//! Same accumulator and lane count as the `lthash` set commitment, under its
//! own domain tag.

use crate::lattice::{LANES, LatticeAccumulator};
use crate::types::Digest;

const TAG: &[u8] = b"auth_wal/policy-digest";

#[derive(Clone)]
pub struct HeightDigest {
    acc: LatticeAccumulator<LANES>,
}

impl HeightDigest {
    pub fn new() -> Self {
        Self {
            acc: LatticeAccumulator::new(TAG),
        }
    }

    pub fn add(&mut self, key: &[u8], height: u64) {
        todo!("acc.add(key || height LE)")
    }

    pub fn remove(&mut self, key: &[u8], height: u64) {
        todo!("acc.remove(key || height LE)")
    }

    pub fn digest(&self) -> Digest {
        self.acc.digest()
    }
}

impl Default for HeightDigest {
    fn default() -> Self {
        Self::new()
    }
}
