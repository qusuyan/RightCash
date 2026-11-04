//! Lattice-hash accumulator shared by the `lthash` scheme (design §7.2) and
//! `MemoryStorage`'s root (design §10): an order-independent, additively
//! homomorphic digest over a multiset of items.
//!
//! Encoding (design §7.2, fixed for both uses):
//! - an item is its fields, each as `u32 LE length || bytes`, so moving a
//!   byte between fields changes the item; entries carry a leading tag field
//!   (`0x00` tombstone, `0x01` value) and integers are `u64 LE` (the policy
//!   state is a root term of its own, design §7.3, R25);
//! - the item hash is `BLAKE3::derive_key(tag)` over the item (32 bytes);
//! - the lane vector is the ChaCha8 keystream keyed by the item hash (nonce
//!   zero), `2 * LANES` bytes read as `u16 LE` lanes: hash-then-PRG
//!   expansion (design §7.2 records the alternatives measured);
//! - adding and removing an item are lane-wise addition and subtraction
//!   mod 2^16;
//! - `digest` is `SHA-256` of the lane vector serialised as `u16 LE`.
//!
//! Callers pick distinct domain tags to separate item encodings across uses.
//! This is hash domain separation, not a guarantee that collisions are impossible.

use chacha20::cipher::{KeyIvInit, StreamCipher};
use sha2::{Digest as _, Sha256};

use crate::types::{Digest, len_prefix};

/// Lane count of every lattice digest (design §7.2, §10).
pub const LANES: usize = 1024;

#[derive(Clone)]
pub struct LatticeAccumulator {
    /// `Hasher::new_derive_key(tag)`, computed once and cloned per item:
    /// deriving the key hashes the tag, which would otherwise be paid on
    /// every add/remove.
    base: blake3::Hasher,
    lanes: Box<[u16; LANES]>,
}

impl LatticeAccumulator {
    /// Return an accumulator with zero lanes and the supplied static `tag` as
    /// its item-hash domain. Use the same tag for states intended to be compared.
    pub fn new(tag: &'static str) -> Self {
        Self {
            base: blake3::Hasher::new_derive_key(tag),
            lanes: Box::new([0u16; LANES]),
        }
    }

    /// Hashes `fields` length-prefixed in order, expands the hash with
    /// ChaCha8 to `2 * LANES` bytes on the stack, and folds the lanes into
    /// `self` with `fold` (wrapping add or sub). No allocation.
    #[inline]
    fn apply(&mut self, fields: &[&[u8]], fold: fn(u16, u16) -> u16) {
        let mut hasher = self.base.clone();
        for field in fields {
            hasher.update(&len_prefix(field.len()));
            hasher.update(field);
        }
        let mut bytes = [0u8; 2 * LANES];
        let mut prg = chacha20::ChaCha8::new(hasher.finalize().as_bytes().into(), &[0u8; 12].into());
        prg.apply_keystream(&mut bytes);
        for (lane, chunk) in self.lanes.iter_mut().zip(bytes.chunks_exact(2)) {
            *lane = fold(*lane, u16::from_le_bytes([chunk[0], chunk[1]]));
        }
    }

    /// Add one item whose borrowed `fields` are hashed in the supplied order,
    /// each with a length prefix. Returns `()` and changes the lanes only.
    /// Repeated additions count repeatedly; this accumulator does not deduplicate.
    pub fn add_fields(&mut self, fields: &[&[u8]]) {
        self.apply(fields, u16::wrapping_add);
    }

    /// Subtract one item encoded by borrowed `fields`, reversing `add_fields`
    /// for the same fields and domain. Returns `()`; membership is not checked,
    /// so the caller must ensure its adds/removes represent the intended multiset.
    pub fn remove_fields(&mut self, fields: &[&[u8]]) {
        self.apply(fields, u16::wrapping_sub);
    }

    /// SHA-256 of the lane vector in `u16 LE`.
    pub fn digest(&self) -> Digest {
        let mut hasher = Sha256::new();
        for lane in self.lanes.iter() {
            hasher.update(lane.to_le_bytes());
        }
        hasher.finalize().into()
    }

    /// Return whether all lanes are zero; this does not count or track items.
    /// Adding then removing the same items restores zero. `MemoryStorage` uses
    /// this condition to select `EMPTY_ROOT`.
    pub fn is_zero(&self) -> bool {
        self.lanes.iter().all(|l| *l == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_independent_and_invertible() {
        let a: [&[u8]; 2] = [b"k1", b"v1"];
        let b: [&[u8]; 2] = [b"k2", b"v2"];
        let mut x = LatticeAccumulator::new("test");
        let mut y = LatticeAccumulator::new("test");
        x.add_fields(&a);
        x.add_fields(&b);
        y.add_fields(&b);
        y.add_fields(&a);
        assert_eq!(x.digest(), y.digest());
        x.remove_fields(&b);
        let mut z = LatticeAccumulator::new("test");
        z.add_fields(&a);
        assert_eq!(x.digest(), z.digest());
        x.remove_fields(&a);
        assert!(x.is_zero());
    }

    #[test]
    fn encoding_separates_fields_and_tags() {
        let mut ab = LatticeAccumulator::new("tag-a");
        let mut a_bc = LatticeAccumulator::new("tag-a");
        ab.add_fields(&[b"ab", b"c"]);
        a_bc.add_fields(&[b"a", b"bc"]);
        assert_ne!(ab.digest(), a_bc.digest());
        let mut other_tag = LatticeAccumulator::new("tag-b");
        other_tag.add_fields(&[b"ab", b"c"]);
        assert_ne!(ab.digest(), other_tag.digest());
    }
}
