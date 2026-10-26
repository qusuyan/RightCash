//! Lattice-hash accumulator shared by the `lthash` scheme (design §7.2), the
//! policy digest (design §6.1) and `MemoryStorage`'s root (design §10): an
//! order-independent, additively homomorphic digest over a multiset of items.
//!
//! Encoding (design §7.2, fixed for all three uses):
//! - an item is `encode_item(fields)`: each field as `u32 LE length || bytes`,
//!   so moving a byte between fields changes the item; entries carry a leading
//!   tag field (`0x00` tombstone, `0x01` value) and integers are `u64 LE`;
//! - the lane vector of an item is `BLAKE3::derive_key(tag)` keyed-XOF over
//!   the item, expanded to `2 * LANES` bytes read as `u16 LE` lanes;
//! - `add`/`remove` are lane-wise addition/subtraction mod 2^16;
//! - `digest` is `SHA-256` of the lane vector serialised as `u16 LE`.
//!
//! Callers pick a distinct domain `tag` per use so the digests never collide.

use crate::types::Digest;

#[derive(Clone)]
pub struct LatticeAccumulator<const LANES: usize> {
    tag: &'static [u8],
    lanes: Box<[u16; LANES]>,
}

impl<const LANES: usize> LatticeAccumulator<LANES> {
    pub fn new(tag: &'static [u8]) -> Self {
        todo!()
    }

    pub fn add(&mut self, item: &[u8]) {
        todo!("lanes += expand(item)")
    }

    pub fn remove(&mut self, item: &[u8]) {
        todo!("lanes -= expand(item)")
    }

    /// `u32 LE length || bytes` per field (see module doc).
    pub fn encode_item(fields: &[&[u8]]) -> Vec<u8> {
        todo!()
    }

    /// SHA-256 of the lane vector.
    pub fn digest(&self) -> Digest {
        todo!()
    }
}

/// Lane count used by both consensus-visible digests (design §6.1).
pub const LANES: usize = 1024;
