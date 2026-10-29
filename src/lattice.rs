//! Lattice-hash accumulator shared by the `lthash` scheme (design §7.2) and
//! `MemoryStorage`'s root (design §10): an
//! order-independent, additively homomorphic digest over a multiset of items.
//!
//! Encoding (design §7.2, fixed for both uses):
//! - an item is `encode_item(fields)`: each field as `u32 LE length || bytes`,
//!   so moving a byte between fields changes the item; entries carry a leading
//!   tag field (`0x00` tombstone, `0x01` value) and integers are `u64 LE`
//!   (the policy state is a root term of its own, design §7.3, R25);
//! - the item hash is `BLAKE3::derive_key(tag)` over the item (32 bytes);
//! - the lane vector is the ChaCha8 keystream keyed by the item hash (nonce
//!   zero), `2 * LANES` bytes read as `u16 LE` lanes: hash-then-PRG expansion.
//!   Measured on an AVX2-only Haswell (`benches/lattice.rs`): blake3's own
//!   XOF output is only vectorised with AVX-512 and cost 2.5 µs per item,
//!   ChaCha8 expansion 0.65 µs, AES-NI counter mode 1.0 µs;
//! - `add`/`remove` are lane-wise addition/subtraction mod 2^16; `merge`
//!   sums two accumulators, so a batch can be hashed in parallel chunks;
//! - `digest` is `SHA-256` of the lane vector serialised as `u16 LE`.
//!
//! Callers pick a distinct domain `tag` per use so the digests never collide.

use chacha20::cipher::{KeyIvInit, StreamCipher};
use sha2::{Digest as _, Sha256};

use crate::types::Digest;

/// Lane count used by every lattice digest (design §7.2, §10).
pub const LANES: usize = 1024;

/// Largest lane vector the stack scratch buffer covers.
const MAX_LANES: usize = 1024;

#[derive(Clone)]
pub struct LatticeAccumulator<const N: usize> {
    /// `Hasher::new_derive_key(tag)`, computed once and cloned per item:
    /// deriving the key hashes the tag, which would otherwise be paid on
    /// every add/remove.
    base: blake3::Hasher,
    lanes: Box<[u16; N]>,
}

impl<const N: usize> LatticeAccumulator<N> {
    pub fn new(tag: &'static str) -> Self {
        assert!(N <= MAX_LANES, "lattice lane count exceeds the scratch buffer");
        Self {
            base: blake3::Hasher::new_derive_key(tag),
            lanes: Box::new([0u16; N]),
        }
    }

    /// Hashes `fields` as `encode_item` would encode them (length-prefixed,
    /// in order), expands the hash with ChaCha8 to `2 * N` bytes on the stack,
    /// and folds the lanes into `self` with `fold` (wrapping add or sub). No
    /// allocation.
    #[inline]
    fn apply(&mut self, fields: &[&[u8]], fold: fn(u16, u16) -> u16) {
        let mut hasher = self.base.clone();
        for field in fields {
            hasher.update(&(field.len() as u32).to_le_bytes());
            hasher.update(field);
        }
        self.fold_expanded(hasher.finalize(), fold);
    }

    /// Hash-then-PRG expansion of one item hash into the lanes.
    #[inline]
    fn fold_expanded(&mut self, item_hash: blake3::Hash, fold: fn(u16, u16) -> u16) {
        let mut buf = [0u8; 2 * MAX_LANES];
        let bytes = &mut buf[..2 * N];
        let mut prg = chacha20::ChaCha8::new(item_hash.as_bytes().into(), &[0u8; 12].into());
        prg.apply_keystream(bytes);
        for (lane, chunk) in self.lanes.iter_mut().zip(bytes.chunks_exact(2)) {
            *lane = fold(*lane, u16::from_le_bytes([chunk[0], chunk[1]]));
        }
    }

    /// Adds an item given as its fields (what `encode_item` would join).
    pub fn add_fields(&mut self, fields: &[&[u8]]) {
        self.apply(fields, u16::wrapping_add);
    }

    pub fn remove_fields(&mut self, fields: &[&[u8]]) {
        self.apply(fields, u16::wrapping_sub);
    }

    /// Adds an already encoded item (`encode_item` output, or any bytes the
    /// caller treats as one opaque field sequence).
    pub fn add(&mut self, item: &[u8]) {
        self.apply_raw(item, u16::wrapping_add);
    }

    pub fn remove(&mut self, item: &[u8]) {
        self.apply_raw(item, u16::wrapping_sub);
    }

    #[inline]
    fn apply_raw(&mut self, item: &[u8], fold: fn(u16, u16) -> u16) {
        let mut hasher = self.base.clone();
        hasher.update(item);
        self.fold_expanded(hasher.finalize(), fold);
    }

    /// Lane-wise sum with another accumulator over the same tag: the
    /// homomorphism that lets a batch be accumulated in parallel chunks and
    /// reduced (design §7.2).
    pub fn merge(&mut self, other: &Self) {
        for (lane, o) in self.lanes.iter_mut().zip(other.lanes.iter()) {
            *lane = lane.wrapping_add(*o);
        }
    }

    /// SHA-256 of the lane vector in `u16 LE`.
    pub fn digest(&self) -> Digest {
        let mut hasher = Sha256::new();
        for lane in self.lanes.iter() {
            hasher.update(lane.to_le_bytes());
        }
        hasher.finalize().into()
    }

    /// `u32 LE length || bytes` per field (see module doc). `add(&encode_item(f))`
    /// equals `add_fields(f)`; the latter avoids the allocation.
    pub fn encode_item(fields: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::with_capacity(fields.iter().map(|f| f.len() + 4).sum());
        for field in fields {
            out.extend_from_slice(&(field.len() as u32).to_le_bytes());
            out.extend_from_slice(field);
        }
        out
    }

    /// Whether no item is accumulated (all lanes zero). Used by
    /// `MemoryStorage` to report `EMPTY_ROOT` for an empty state.
    pub fn is_zero(&self) -> bool {
        self.lanes.iter().all(|l| *l == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_independent_and_invertible() {
        let a = LatticeAccumulator::<LANES>::encode_item(&[b"k1", b"v1"]);
        let b = LatticeAccumulator::<LANES>::encode_item(&[b"k2", b"v2"]);
        let mut x = LatticeAccumulator::<LANES>::new("test");
        let mut y = LatticeAccumulator::<LANES>::new("test");
        x.add(&a);
        x.add(&b);
        y.add(&b);
        y.add(&a);
        assert_eq!(x.digest(), y.digest());
        x.remove(&b);
        let mut z = LatticeAccumulator::<LANES>::new("test");
        z.add(&a);
        assert_eq!(x.digest(), z.digest());
        x.remove(&a);
        assert!(x.is_zero());
    }

    #[test]
    fn fields_match_encoded_item() {
        let mut x = LatticeAccumulator::<LANES>::new("t");
        let mut y = LatticeAccumulator::<LANES>::new("t");
        x.add_fields(&[b"key", b"value"]);
        y.add(&LatticeAccumulator::<LANES>::encode_item(&[b"key", b"value"]));
        assert_eq!(x.digest(), y.digest());
        x.remove_fields(&[b"key", b"value"]);
        assert!(x.is_zero());
    }

    #[test]
    fn encoding_separates_fields_and_tags() {
        let ab = LatticeAccumulator::<LANES>::encode_item(&[b"ab", b"c"]);
        let a_bc = LatticeAccumulator::<LANES>::encode_item(&[b"a", b"bc"]);
        assert_ne!(ab, a_bc);
        let mut x = LatticeAccumulator::<LANES>::new("tag-a");
        let mut y = LatticeAccumulator::<LANES>::new("tag-b");
        x.add(&ab);
        y.add(&ab);
        assert_ne!(x.digest(), y.digest());
    }
}
