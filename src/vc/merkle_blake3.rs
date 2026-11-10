//! Bottom-up BLAKE3 Merkle tree (design §7.2): a whole-set scheme that
//! builds a `b`-ary Merkle tree over the set in key order at every
//! `commitment` and keeps it in the snapshot's scheme instance, so the tree lives
//! exactly as long as the set copy it describes (and can later serve
//! membership proofs, design §7.4).
//!
//! Construction, for branching factor `b >= 2`:
//! - level 0 holds one leaf hash per entry, in key order: keyed BLAKE3 under
//!   `LEAF` over the §7.2 entry item (`tag ‖ key ‖ value`, each field
//!   `u32 LE` length-prefixed);
//! - each further level groups the one below into runs of `b` consecutive
//!   hashes (the last run may be shorter) and hashes each run's
//!   concatenation with keyed BLAKE3 under `NODE`;
//! - the build stops at the first level with a single hash, the top. A set
//!   of one entry has its leaf as the top; the empty set has no levels.
//!   (Stopping instead once a level has fewer than `b` hashes and hashing
//!   that level into the top would add a single-child node whenever a level
//!   has exactly one or exactly `b` hashes; this is the usual rule without it.)
//!
//! The commitment is BLAKE3 `derive_key` (domain `right_cash/merkle-blake3`)
//! over `u64 LE` entry count ‖ top (32 zero bytes for the empty set). The
//! distinct leaf and node keys keep a leaf from being read as a node; the
//! count fixes the shape, and `b` enters the root through `params_digest`.
//!
//! Computation (the result is the full build's, whatever the history):
//! - `apply` records which keys changed. A child's instance starts as a clone
//!   of its parent's, holding the parent's tree, so `commitment` copies the
//!   parent's leaf hash for every entry no transition touched (a leaf
//!   depends on its entry alone) and hashes only the rest; the levels above
//!   are rebuilt, since an insert or removal shifts every later run. Without
//!   a parent tree (genesis, a restored checkpoint) every leaf is hashed.
//! - Leaves and nodes are hashed eight at a time, one per SIMD lane
//!   (`blake3_lanes`): every leaf of a short entry and every node with
//!   `b` ≤ 32 fits in one 1 KiB BLAKE3 chunk.
//!
//! Memory: a tree holds about `32 · n · b / (b − 1)` bytes for `n` entries
//! (0.34 MB at 10k entries, `b` = 16), once per retained snapshot.

use std::sync::{Arc, OnceLock};

use crate::set::EntryRef;
use crate::types::{Digest, Key, len_prefix};
use crate::vc::blake3_lanes::{LANES, MAX_LEN, keyed_hash_lanes};
use crate::vc::{VectorCommitment, item_fields};

const DOMAIN: &str = "right_cash/merkle-blake3";
const LEAF_DOMAIN: &str = "right_cash/merkle-blake3 leaf";
const NODE_DOMAIN: &str = "right_cash/merkle-blake3 node";
const PARAMS_DOMAIN: &str = "right_cash/merkle-blake3 params";

/// Default branching factor: a full node hashes 16 × 32 = 512 bytes, eight
/// 64-byte blocks, half of one 1 KiB BLAKE3 chunk (any `b` up to 32 keeps a
/// node within one chunk).
pub const DEFAULT_BRANCHING: usize = 16;

/// Keys of the leaf and node hashes, derived once from their domains.
fn keys() -> &'static ([u8; 32], [u8; 32]) {
    static KEYS: OnceLock<([u8; 32], [u8; 32])> = OnceLock::new();
    KEYS.get_or_init(|| {
        (
            blake3::derive_key(LEAF_DOMAIN, &[]),
            blake3::derive_key(NODE_DOMAIN, &[]),
        )
    })
}

/// Scheme instance: the branching factor (its one parameter), the tree of
/// the snapshot that owns the instance, and the transitions `apply`
/// reported since that tree was built. A child's instance is a clone of its
/// parent's, so it starts from the parent's tree: its `commitment` reuses
/// the parent's leaf hashes for the entries no transition touched, then
/// replaces the tree with its own, after which the parent's tree is held
/// only by the parent and is freed with it.
#[derive(Clone)]
pub struct MerkleBlake3<K> {
    branching: usize,
    tree: Option<Arc<Tree>>,
    /// `(key, in the parent, in the child)` per `apply`, in call order
    /// (ascending keys, one call per key: design §7.1). Emptied by
    /// `commitment`.
    changes: Vec<(K, bool, bool)>,
}

impl<K> MerkleBlake3<K> {
    /// The scheme with branching factor `branching`, over no entries.
    /// Panics below 2, where the levels would not shrink.
    pub fn new(branching: usize) -> Self {
        assert!(
            branching >= 2,
            "the Merkle branching factor must be at least 2, got {branching}"
        );
        Self {
            branching,
            tree: None,
            changes: Vec::new(),
        }
    }

    pub fn branching(&self) -> usize {
        self.branching
    }

    /// The tree, once `commitment` has run.
    pub fn tree(&self) -> Option<&Tree> {
        self.tree.as_deref()
    }

    /// The shared tree itself (tests watch its lifetime).
    #[cfg(test)]
    pub(crate) fn tree_handle(&self) -> Option<&Arc<Tree>> {
        self.tree.as_ref()
    }
}

impl<K> Default for MerkleBlake3<K> {
    fn default() -> Self {
        Self::new(DEFAULT_BRANCHING)
    }
}

/// Sizes of the levels of a tree over `leaves` entries with branching
/// factor `branching`, leaves first, ending with the level of one hash; none
/// for the empty set.
fn level_sizes(leaves: usize, branching: usize) -> impl Iterator<Item = usize> {
    std::iter::successors((leaves > 0).then_some(leaves), move |&size| {
        (size > 1).then(|| size.div_ceil(branching))
    })
}

/// One snapshot's tree: every level in one buffer (a single allocation per
/// snapshot), the leaf hashes first in key order, then each level's node
/// hashes, ending with the single top hash; empty for the empty set.
#[derive(Debug, PartialEq, Eq)]
pub struct Tree {
    branching: usize,
    leaves: usize,
    nodes: Vec<Digest>,
}

impl Tree {
    /// The levels, leaves first, the top last.
    pub fn levels(&self) -> impl Iterator<Item = &[Digest]> {
        let mut rest = &self.nodes[..];
        level_sizes(self.leaves, self.branching).map(move |size| {
            let (level, tail) = rest.split_at(size);
            rest = tail;
            level
        })
    }

    /// The leaf hashes, in key order.
    pub fn leaves(&self) -> &[Digest] {
        &self.nodes[..self.leaves]
    }

    /// The top hash, or `None` for the empty set.
    pub fn top(&self) -> Option<&Digest> {
        self.nodes.last()
    }

    /// Number of leaves (entries).
    pub fn len(&self) -> usize {
        self.leaves
    }

    pub fn is_empty(&self) -> bool {
        self.leaves == 0
    }
}

/// Leaves waiting to be hashed together, one per lane: each encoded entry
/// item (fields `u32 LE` length-prefixed) in its lane's buffer, and the leaf
/// it is for. On the stack: no allocation per leaf or per snapshot.
struct LeafLanes {
    items: [[u8; MAX_LEN]; LANES],
    lens: [usize; LANES],
    targets: [usize; LANES],
    len: usize,
}

impl LeafLanes {
    fn new() -> Self {
        Self {
            items: [[0; MAX_LEN]; LANES],
            lens: [0; LANES],
            targets: [0; LANES],
            len: 0,
        }
    }

    /// Queue the leaf of `(key, entry)` for `leaves[target]`; hash the
    /// queue when every lane is taken. An item longer than one chunk is
    /// hashed on its own.
    fn push<K: Key>(&mut self, target: usize, key: &K, entry: EntryRef<'_>, leaves: &mut [Digest]) {
        let fields = item_fields(key, entry);
        let len: usize = fields.iter().map(|field| 4 + field.len()).sum();
        if len > MAX_LEN {
            let mut item = Vec::with_capacity(len);
            for field in fields {
                item.extend_from_slice(&len_prefix(field.len()));
                item.extend_from_slice(field);
            }
            leaves[target] = *blake3::keyed_hash(&keys().0, &item).as_bytes();
            return;
        }
        let item = &mut self.items[self.len];
        let mut at = 0;
        for field in fields {
            item[at..at + 4].copy_from_slice(&len_prefix(field.len()));
            item[at + 4..at + 4 + field.len()].copy_from_slice(field);
            at += 4 + field.len();
        }
        self.lens[self.len] = len;
        self.targets[self.len] = target;
        self.len += 1;
        if self.len == LANES {
            self.flush(leaves);
        }
    }

    fn flush(&mut self, leaves: &mut [Digest]) {
        let msgs: [&[u8]; LANES] = std::array::from_fn(|l| &self.items[l][..self.lens[l]]);
        let mut out = [[0u8; 32]; LANES];
        keyed_hash_lanes(&keys().0, &msgs[..self.len], &mut out);
        for (&target, hash) in self.targets[..self.len].iter().zip(out) {
            leaves[target] = hash;
        }
        self.len = 0;
    }
}

/// Fill `leaves` with the leaf hashes of `entries`. With the parent's leaves
/// and the transitions since them, an entry no transition touched takes its
/// parent's leaf: it is the parent's entry at position `u + r`, `u` counting
/// the untouched entries before it and `r` the transitions before it that
/// removed or replaced a parent entry (both are in key order). The rest are
/// hashed, eight at a time. Returns how many were hashed, or `None`, with
/// `leaves` incomplete, if the transitions do not describe `entries` against
/// the parent.
fn fill_leaves<K: Key>(
    entries: &[(&K, EntryRef<'_>)],
    parent: Option<&[Digest]>,
    changes: &[(K, bool, bool)],
    leaves: &mut [Digest],
) -> Option<usize> {
    let mut lanes = LeafLanes::new();
    let (mut untouched, mut replaced, mut next) = (0, 0, 0);
    for (i, &(key, entry)) in entries.iter().enumerate() {
        let Some(parent) = parent else {
            lanes.push(i, key, entry, leaves);
            continue;
        };
        // Transitions before this key removed parent entries (a removal
        // that left the key in the child would be an entry we skipped).
        while let Some((changed, in_parent, in_child)) = changes.get(next)
            && changed < key
        {
            if *in_child {
                return None;
            }
            replaced += usize::from(*in_parent);
            next += 1;
        }
        match changes.get(next) {
            Some((changed, in_parent, in_child)) if changed == key => {
                if !in_child {
                    return None;
                }
                replaced += usize::from(*in_parent);
                next += 1;
                lanes.push(i, key, entry, leaves);
            }
            _ => {
                leaves[i] = *parent.get(untouched + replaced)?;
                untouched += 1;
            }
        }
    }
    lanes.flush(leaves);
    if let Some(parent) = parent {
        for (_, in_parent, in_child) in &changes[next..] {
            if *in_child {
                return None;
            }
            replaced += usize::from(*in_parent);
        }
        if untouched + replaced != parent.len() {
            return None;
        }
    }
    Some(entries.len() - untouched)
}

/// Fill `above` with the node hashes over `below`, runs of `branching`,
/// eight runs at a time while a run fits in one chunk.
fn fill_level(branching: usize, below: &[Digest], above: &mut [Digest]) {
    if branching <= MAX_LEN / 32 {
        let key = &keys().1;
        for (runs, out) in below.chunks(branching * LANES).zip(above.chunks_mut(LANES)) {
            let mut msgs: [&[u8]; LANES] = [&[]; LANES];
            let mut count = 0;
            for (msg, run) in msgs.iter_mut().zip(runs.chunks(branching)) {
                *msg = run.as_flattened();
                count += 1;
            }
            keyed_hash_lanes(key, &msgs[..count], out);
        }
    } else {
        for (run, out) in below.chunks(branching).zip(above.iter_mut()) {
            *out = node(run);
        }
    }
}

/// Build the tree over `entries` (in the order given) with branching factor
/// `branching`, reusing `parent`'s leaves for the entries `changes` does
/// not name. Allocates the tree's one buffer and a list of the entries.
fn build<'a, K: Key + 'a>(
    branching: usize,
    entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>,
    parent: Option<&Tree>,
    changes: &[(K, bool, bool)],
) -> Tree {
    let entries: Vec<_> = entries.collect();
    let n = entries.len();
    let mut nodes = vec![[0u8; 32]; level_sizes(n, branching).sum()];
    let (leaves, mut rest) = nodes.split_at_mut(n);
    #[cfg_attr(
        not(any(test, debug_assertions)),
        expect(unused_variables, reason = "read by debug and test builds")
    )]
    let hashed = match fill_leaves(&entries, parent.map(Tree::leaves), changes, leaves) {
        Some(hashed) => hashed,
        None => {
            debug_assert!(
                false,
                "the transitions do not describe the set against the parent's tree"
            );
            fill_leaves(&entries, None, &[], leaves).expect("a build without a parent always succeeds")
        }
    };
    // A change `apply` never reported leaves the counts balanced and reuses
    // a stale leaf; debug builds check every reused leaf against its hash.
    #[cfg(debug_assertions)]
    if hashed < n {
        let mut fresh = vec![[0u8; 32]; n];
        fill_leaves(&entries, None, &[], &mut fresh);
        assert!(
            fresh == leaves,
            "a reused leaf differs from its entry's hash: a change was not reported to `apply`"
        );
    }
    #[cfg(test)]
    tests::LEAVES_HASHED.set(hashed);
    let mut below: &[Digest] = leaves;
    for size in level_sizes(n, branching).skip(1) {
        let (above, tail) = std::mem::take(&mut rest).split_at_mut(size);
        fill_level(branching, below, above);
        below = above;
        rest = tail;
    }
    Tree {
        branching,
        leaves: n,
        nodes,
    }
}

/// Keyed BLAKE3 under the node key over the concatenated children (a
/// run too long for one lane, and the tests).
fn node(children: &[Digest]) -> Digest {
    *blake3::keyed_hash(&keys().1, children.as_flattened()).as_bytes()
}

/// The commitment of a tree: `derive_key` over `u64 LE` leaf count ‖ top.
fn digest(tree: &Tree) -> Digest {
    let mut hasher = blake3::Hasher::new_derive_key(DOMAIN);
    hasher.update(&(tree.len() as u64).to_le_bytes());
    hasher.update(tree.top().unwrap_or(&[0u8; 32]));
    *hasher.finalize().as_bytes()
}

impl<K: Key> VectorCommitment<K> for MerkleBlake3<K> {
    fn scheme_id(&self) -> &'static str {
        "merkle-blake3"
    }

    /// `derive_key` over the branching factor as `u64 LE`.
    fn params_digest(&self) -> Digest {
        blake3::derive_key(PARAMS_DOMAIN, &(self.branching as u64).to_le_bytes())
    }

    fn empty(&self) -> Self {
        Self::new(self.branching)
    }

    /// Record that `key` changed, for `commitment` to rehash its leaf.
    fn apply(&mut self, key: &K, old: Option<EntryRef<'_>>, new: Option<EntryRef<'_>>) {
        self.changes.push((key.clone(), old.is_some(), new.is_some()));
    }

    /// Build the tree over `entries`, reusing the leaves of the tree this
    /// instance holds (the parent's) for untouched entries; keep the new
    /// tree, releasing the parent's, and return its commitment.
    fn commitment<'a>(&mut self, entries: impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)>) -> Digest {
        let parent = self.tree.take();
        let changes = std::mem::take(&mut self.changes);
        let tree = build(self.branching, entries, parent.as_deref(), &changes);
        drop(parent);
        let digest = digest(&tree);
        self.tree = Some(Arc::new(tree));
        digest
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;

    use super::*;
    use crate::set::Entry;

    type K = Vec<u8>;
    type V = Vec<u8>;

    thread_local! {
        /// Leaves the last `build` on this thread hashed rather than reused.
        pub(super) static LEAVES_HASHED: Cell<usize> = const { Cell::new(0) };
    }

    /// Entries with tombstones and, now and then, an item longer than one
    /// BLAKE3 chunk (hashed outside the lanes).
    fn entries(n: usize) -> BTreeMap<K, Entry<V>> {
        (0..n)
            .map(|i| {
                let key = format!("key{i:05}").into_bytes();
                let entry = if i % 7 == 3 {
                    Entry::Tombstone
                } else if i % 11 == 5 {
                    Entry::Value(vec![i as u8; 1100])
                } else {
                    Entry::Value(format!("v{i}").into_bytes())
                };
                (key, entry)
            })
            .collect()
    }

    /// A fresh instance with `scheme`'s parameters, committed over `set`.
    fn commit(scheme: &MerkleBlake3<K>, set: &BTreeMap<K, Entry<V>>) -> (Digest, MerkleBlake3<K>) {
        let mut fresh = scheme.empty();
        let digest = fresh.commitment(set.iter().map(|(k, e)| (k, e.as_ref())));
        (digest, fresh)
    }

    /// A node hash from the blake3 crate's public API directly.
    fn reference_node(children: &[Digest]) -> Digest {
        let bytes: Vec<u8> = children.iter().flatten().copied().collect();
        *blake3::keyed_hash(&blake3::derive_key(NODE_DOMAIN, &[]), &bytes).as_bytes()
    }

    /// The top by recursion over the leaves, written independently of
    /// `build`: the tree has height `h`, the smallest with `b^h >= n`, and
    /// a node at height `h` splits its leaves into runs of `b^(h-1)`. A run
    /// shorter than a full subtree still sits at its height, so a lone last
    /// leaf is wrapped by one single-child node per level above it.
    fn oracle_top(leaves: &[Digest], b: usize) -> Digest {
        fn subtree(leaves: &[Digest], b: usize, height: u32) -> Digest {
            if height == 0 {
                assert_eq!(leaves.len(), 1);
                return leaves[0];
            }
            let span = b.pow(height - 1);
            let children: Vec<Digest> = leaves.chunks(span).map(|part| subtree(part, b, height - 1)).collect();
            reference_node(&children)
        }
        let (mut height, mut cap) = (0, 1);
        while cap < leaves.len() {
            cap *= b;
            height += 1;
        }
        subtree(leaves, b, height)
    }

    /// The leaf hash from the blake3 crate's public API directly.
    fn reference_leaf(key: &[u8], entry: &Entry<V>) -> Digest {
        let (tag, value): (u8, &[u8]) = match entry {
            Entry::Value(v) => (1, v),
            Entry::Tombstone => (0, &[]),
        };
        let mut item = Vec::new();
        for field in [&[tag][..], key, value] {
            item.extend_from_slice(&(field.len() as u32).to_le_bytes());
            item.extend_from_slice(field);
        }
        *blake3::keyed_hash(&blake3::derive_key(LEAF_DOMAIN, &[]), &item).as_bytes()
    }

    #[test]
    fn tree_matches_the_recursive_oracle_at_every_shape() {
        // 32: a full node is exactly one chunk; 33: nodes outside the lanes.
        for b in [2, 3, 4, 16, 32, 33] {
            let scheme = MerkleBlake3::new(b);
            for n in [
                0,
                1,
                2,
                b - 1,
                b,
                b + 1,
                b * b - 1,
                b * b,
                b * b + 1,
                b * b * b + 2,
                1000,
            ] {
                let set = entries(n);
                let (digest, state) = commit(&scheme, &set);
                let tree = state.tree().expect("commitment keeps the tree");
                let leaves: Vec<Digest> = set.iter().map(|(k, e)| reference_leaf(k, e)).collect();
                assert_eq!(tree.len(), n, "b {b} n {n}");
                // The commitment: count ‖ top, 32 zero bytes standing in for
                // the empty set's missing top.
                let mut expected = blake3::Hasher::new_derive_key(DOMAIN);
                expected.update(&(n as u64).to_le_bytes());
                expected.update(tree.top().unwrap_or(&[0u8; 32]));
                assert_eq!(digest, *expected.finalize().as_bytes(), "b {b} n {n}");
                if n == 0 {
                    assert!(tree.is_empty() && tree.top().is_none());
                    continue;
                }
                assert_eq!(tree.leaves(), leaves, "b {b} n {n}");
                assert_eq!(*tree.top().unwrap(), oracle_top(&leaves, b), "b {b} n {n}");
                // Every level but the top has more than one hash and each
                // level is the ceiling of the one below over b.
                let levels: Vec<&[Digest]> = tree.levels().collect();
                for pair in levels.windows(2) {
                    assert!(pair[0].len() > 1);
                    assert_eq!(pair[1].len(), pair[0].len().div_ceil(b));
                }
                assert_eq!(levels.last().unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn covers_every_entry_and_distinguishes_tombstones() {
        let scheme = MerkleBlake3::default();
        let a = entries(40);
        let (base, _) = commit(&scheme, &a);
        let key = b"key00011".to_vec(); // a value in `entries` (11 % 7 != 3)
        let mut b = a.clone();
        b.insert(key.clone(), Entry::Value(b"other".to_vec()));
        let (changed, _) = commit(&scheme, &b);
        b.insert(key.clone(), Entry::Value(Vec::new()));
        let (empty_value, _) = commit(&scheme, &b);
        b.insert(key.clone(), Entry::Tombstone);
        let (tombstone, _) = commit(&scheme, &b);
        b.remove(&key);
        let (absent, _) = commit(&scheme, &b);
        let all = [
            base,
            changed,
            empty_value,
            tombstone,
            absent,
            commit(&scheme, &BTreeMap::new()).0,
        ];
        for i in 0..all.len() {
            for j in i + 1..all.len() {
                assert_ne!(all[i], all[j], "{i} {j}");
            }
        }
    }

    #[test]
    fn fields_are_delimited_and_the_branching_factor_is_a_parameter() {
        let mut a = BTreeMap::new();
        a.insert(b"ab".to_vec(), Entry::Value(b"c".to_vec()));
        let mut b = BTreeMap::new();
        b.insert(b"a".to_vec(), Entry::Value(b"bc".to_vec()));
        let scheme = MerkleBlake3::default();
        assert_ne!(commit(&scheme, &a).0, commit(&scheme, &b).0);
        // The same set under two factors: different trees above the leaves
        // and different parameter digests.
        let set = entries(100);
        let (two, three): (MerkleBlake3<K>, MerkleBlake3<K>) = (MerkleBlake3::new(2), MerkleBlake3::new(3));
        assert_ne!(commit(&two, &set).0, commit(&three, &set).0);
        assert_ne!(
            VectorCommitment::<K>::params_digest(&two),
            VectorCommitment::<K>::params_digest(&three)
        );
    }

    #[test]
    fn a_leaf_is_not_a_node() {
        // Leaves and nodes are hashed under different keys, so a one-entry
        // set's top (its leaf) differs from a node over that leaf.
        assert_ne!(keys().0, keys().1);
        let set = entries(1);
        let (_, state) = commit(&MerkleBlake3::new(2), &set);
        let top = *state.tree().unwrap().top().unwrap();
        assert_eq!(top, state.tree().unwrap().leaves()[0]);
        assert_ne!(top, node(&[top]));
    }

    #[test]
    fn a_clone_shares_the_tree_until_its_own_commitment_replaces_it() {
        let scheme = MerkleBlake3::new(4);
        let (_, parent) = commit(&scheme, &entries(30));
        let mut child = parent.clone();
        let parent_tree = parent.tree_handle().unwrap().clone();
        assert!(Arc::ptr_eq(&parent_tree, child.tree_handle().unwrap()));
        let set = entries(31);
        let (added, entry) = set.last_key_value().unwrap();
        child.apply(added, None, Some(entry.as_ref()));
        child.commitment(set.iter().map(|(k, e)| (k, e.as_ref())));
        assert!(!Arc::ptr_eq(&parent_tree, child.tree_handle().unwrap()));
        assert_eq!(child.tree().unwrap().len(), 31);
        assert_eq!(parent.tree().unwrap().len(), 30);
        // Only the parent and this handle hold the parent's tree.
        assert_eq!(Arc::strong_count(&parent_tree), 2);
    }

    /// A child built from its parent's instance and the transitions between
    /// them: inserts, updates (to a value, an empty value, a tombstone) and
    /// removals, anywhere in key order, including emptying the set and
    /// growing it from empty. Its tree must equal a fresh build's.
    #[test]
    fn reused_leaves_give_the_full_build() {
        use crate::tests::common::Rng;
        let mut rng = Rng(17);
        for b in [2, 3, 16] {
            let scheme = MerkleBlake3::new(b);
            let mut set: BTreeMap<K, Entry<V>> = BTreeMap::new();
            let mut state = scheme.empty();
            state.commitment(std::iter::empty());
            for round in 0..400u32 {
                let mut child = set.clone();
                let mut child_state = state.clone();
                // Mostly small changes; now and then empty the set.
                let wipe = round % 97 == 50;
                let mut changed: BTreeMap<K, Option<Entry<V>>> = BTreeMap::new();
                if wipe {
                    for key in set.keys() {
                        changed.insert(key.clone(), None);
                    }
                } else {
                    for _ in 0..rng.below(12) {
                        let key = format!("k{:03}", rng.below(120)).into_bytes();
                        let entry = match rng.below(6) {
                            0 => None,
                            1 => Some(Entry::Tombstone),
                            2 => Some(Entry::Value(Vec::new())),
                            _ => Some(Entry::Value(format!("v{round}").into_bytes())),
                        };
                        changed.insert(key, entry);
                    }
                }
                // Transitions in key order, one per key, as the set's merge
                // reports them.
                let mut reported_present = 0;
                for (key, new) in &changed {
                    let old = set.get(key).map(|e| e.as_ref());
                    if old.is_none() && new.is_none() {
                        continue;
                    }
                    reported_present += usize::from(new.is_some());
                    child_state.apply(key, old, new.as_ref().map(|e| e.as_ref()));
                    match new {
                        Some(entry) => child.insert(key.clone(), entry.clone()),
                        None => child.remove(key),
                    };
                }
                let digest = child_state.commitment(child.iter().map(|(k, e)| (k, e.as_ref())));
                // Exactly the reported entries were hashed: everything else
                // came from the parent's tree.
                assert_eq!(LEAVES_HASHED.get(), reported_present, "b {b} round {round}");
                let (fresh_digest, fresh) = commit(&scheme, &child);
                assert_eq!(digest, fresh_digest, "b {b} round {round}");
                assert_eq!(child_state.tree(), fresh.tree(), "b {b} round {round}");
                set = child;
                state = child_state;
            }
        }
    }

    /// A commitment over a set its transitions do not describe against the
    /// parent's tree (here: an entry added without `apply`) is caught.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "do not describe the set")]
    fn a_commitment_without_its_transitions_is_caught() {
        let scheme = MerkleBlake3::new(4);
        let (_, parent) = commit(&scheme, &entries(30));
        let mut child = parent.clone();
        let set = entries(31);
        child.commitment(set.iter().map(|(k, e)| (k, e.as_ref())));
    }

    /// A value changed without `apply`: the counts still add up, so only
    /// the debug check of reused leaves can see it.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a change was not reported")]
    fn an_unreported_change_is_caught() {
        let scheme = MerkleBlake3::new(4);
        let mut set = entries(30);
        let (_, parent) = commit(&scheme, &set);
        let mut child = parent.clone();
        set.insert(b"key00001".to_vec(), Entry::Value(b"changed".to_vec()));
        child.commitment(set.iter().map(|(k, e)| (k, e.as_ref())));
    }

    #[test]
    #[should_panic(expected = "at least 2")]
    fn rejects_a_branching_factor_below_two() {
        MerkleBlake3::<K>::new(1);
    }
}
