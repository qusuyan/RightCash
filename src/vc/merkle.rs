//! Sparse Merkle tree reference scheme (design §7.2): slow but proof-friendly
//! baseline for tests and benchmarks. Leaves are addressed by `SHA-256(key)`
//! at depth 256 (parameter-free, assuming no SHA-256 address collisions);
//! distinct keys with the same address are not handled separately. A leaf hashes the
//! §7.2 entry item. The state is the whole (persistent) tree, cloned with the
//! snapshot in O(1), since `apply(old, new)` must recompute a path.
//!
//! Encoding: a leaf is `SHA-256(0x00 || item)`, the item's fields (`tag`,
//! `key`, `value`) `u32 LE` length-prefixed as in the lattice, an empty leaf
//! 32 zero bytes, an inner node `SHA-256(0x01 || left || right)`;
//! the default hash of a level is the hash of a subtree of empty leaves, and
//! a node equal to its level's default is not stored. Levels count from the
//! root (0) to the leaves (`DEPTH`); a node's position is its level and its
//! path, the leaf address with the bits below the level cleared.

use std::sync::OnceLock;

use sha2::{Digest as _, Sha256};

use crate::set::EntryRef;
use crate::types::{Digest, Key, len_prefix};
use crate::vc::{VectorCommitment, item_fields};

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

/// `defaults()[level]`: the hash of an all-empty subtree rooted at `level`.
fn defaults() -> &'static [Digest; DEPTH as usize + 1] {
    static DEFAULTS: OnceLock<[Digest; DEPTH as usize + 1]> = OnceLock::new();
    DEFAULTS.get_or_init(|| {
        let mut table = [[0u8; 32]; DEPTH as usize + 1];
        for level in (0..DEPTH as usize).rev() {
            table[level] = inner(&table[level + 1], &table[level + 1]);
        }
        table
    })
}

fn inner(left: &Digest, right: &Digest) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update([0x01]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

fn leaf<K: Key>(key: &K, entry: EntryRef<'_>) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update([0x00]);
    for field in item_fields(key, entry) {
        hasher.update(len_prefix(field.len()));
        hasher.update(field);
    }
    hasher.finalize().into()
}

/// Return bit `i` of borrowed 256-bit `path`, numbered most-significant first.
/// The caller must supply `i < DEPTH`.
fn bit(path: &Digest, i: u32) -> bool {
    path[(i / 8) as usize] & (0x80 >> (i % 8)) != 0
}

/// Return the first `level` bits of `path` with all remaining bits zeroed.
/// Requires `level <= DEPTH`; zero identifies the root, DEPTH preserves the
/// complete leaf address. Does not change the input path.
fn prefix(path: &Digest, level: u32) -> Digest {
    let mut out = [0u8; 32];
    let full = (level / 8) as usize;
    out[..full].copy_from_slice(&path[..full]);
    if !level.is_multiple_of(8) {
        out[full] = path[full] & (0xFFu8 << (8 - level % 8));
    }
    out
}

impl<K: Key> VectorCommitment<K> for Merkle {
    type State = MerkleState;

    fn scheme_id(&self) -> &'static str {
        "merkle"
    }

    fn empty(&self) -> Self::State {
        MerkleState {
            root: defaults()[0],
            nodes: im::OrdMap::new(),
        }
    }

    /// Replace `key`'s leaf in mutable `state` with the hash of `new`, or the
    /// empty leaf for `None`, then recompute its ancestors through the root.
    /// `_old` is unused because the tree already stores the path's hashes.
    /// Returns `()`; other paths remain structurally shared with cloned states.
    fn apply(&self, state: &mut Self::State, key: &K, _old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>) {
        let defaults = defaults();
        let path: Digest = Sha256::digest(key.as_ref()).into();
        let mut hash = match new {
            Some(entry) => leaf(key, entry),
            None => defaults[DEPTH as usize],
        };
        let mut level = DEPTH;
        loop {
            let position = (level, prefix(&path, level));
            // Missing map entries mean the default empty-subtree hash. Remove
            // nodes that return to that hash so deletes do not leave empty paths.
            if hash == defaults[level as usize] {
                state.nodes.remove(&position);
            } else {
                state.nodes.insert(position, hash);
            }
            if level == 0 {
                break;
            }
            // Toggle the last bit of this node's prefix to address its sibling;
            // bits below the current level are already zero.
            let mut sibling = position.1;
            let byte = ((level - 1) / 8) as usize;
            sibling[byte] ^= 0x80 >> ((level - 1) % 8);
            let sibling_hash = state
                .nodes
                .get(&(level, sibling))
                .copied()
                .unwrap_or(defaults[level as usize]);
            // A set path bit puts our node on the right. Preserve left/right
            // order when hashing the parent; concatenation is not commutative.
            hash = if bit(&path, level - 1) {
                inner(&sibling_hash, &hash)
            } else {
                inner(&hash, &sibling_hash)
            };
            level -= 1;
        }
        state.root = hash;
    }

    fn commitment<'a>(
        &self,
        state: &Self::State,
        _entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>,
    ) -> Digest {
        state.root
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::set::Entry;
    use crate::tests::common::Rng;

    type K = Vec<u8>;
    type V = Vec<u8>;

    /// The root over `leaves` (path, leaf hash; sorted by path) by recursion
    /// on the path bits: an oracle for the incremental update's shape (the
    /// sibling's side, the prefix mask, the default subtrees), which the
    /// incremental-against-fresh comparison of `set::tests` cannot see.
    fn oracle(leaves: &[(Digest, Digest)], level: u32) -> Digest {
        if leaves.is_empty() {
            return defaults()[level as usize];
        }
        if level == DEPTH {
            assert_eq!(leaves.len(), 1, "two leaves on one path");
            return leaves[0].1;
        }
        let split = leaves.partition_point(|(path, _)| !bit(path, level));
        inner(
            &oracle(&leaves[..split], level + 1),
            &oracle(&leaves[split..], level + 1),
        )
    }

    #[test]
    fn incremental_root_matches_the_recursive_oracle() {
        let scheme = Merkle::new();
        let mut state = VectorCommitment::<K>::empty(&scheme);
        let mut entries: BTreeMap<K, Entry<V>> = BTreeMap::new();
        let mut rng = Rng(11);
        for round in 0..200u32 {
            let key = format!("k{}", rng.below(50)).into_bytes();
            let new = match rng.below(4) {
                0 => None,
                1 => Some(Entry::Tombstone),
                _ => Some(Entry::Value(format!("v{round}").into_bytes())),
            };
            let old = entries.get(&key).map(|entry| entry.as_ref());
            VectorCommitment::<K>::apply(&scheme, &mut state, &key, old, new.as_ref().map(|e| e.as_ref()));
            match new {
                None => entries.remove(&key),
                Some(entry) => entries.insert(key, entry),
            };
            let mut leaves: Vec<(Digest, Digest)> = entries
                .iter()
                .map(|(key, entry)| (Sha256::digest(key).into(), leaf(key, entry.as_ref())))
                .collect();
            leaves.sort();
            let root = VectorCommitment::<K>::commitment(&scheme, &state, std::iter::empty::<(&K, EntryRef<'_>)>());
            assert_eq!(root, oracle(&leaves, 0), "round {round}");
        }
    }
}
