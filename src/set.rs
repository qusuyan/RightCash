//! The write set: a copy-on-write ordered map of recent writes with its
//! commitment and eviction-policy state (design §4).

use crate::policy::EvictionPolicy;
use crate::speculate::Pred;
use crate::types::{BatchOp, Digest, Key, Value};
use crate::vc::VectorCommitment;

/// A set entry. Deletes stay as tombstones until evicted, when they become a
/// `BatchOp::Delete` for storage. Tombstones count toward capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry<V> {
    Value(V),
    Tombstone,
}

/// Persistent (structurally shared) **ordered** map: O(1) snapshot, O(log n)
/// update, iteration in key order on every node, range scans for prefix
/// deletes. Wraps the backing crate so it can be swapped without touching
/// callers (design §4).
#[derive(Clone)]
pub struct PersistentMap<K, V> {
    inner: im::OrdMap<K, V>,
}

impl<K: Key, V: Clone> PersistentMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: im::OrdMap::new(),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get::<K>(key)
    }

    /// Returns the previous value, if any.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove::<K>(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    /// Keys whose bytes start with `prefix`, in key order (design §3 PrefixDelete).
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<K> {
        todo!("range scan from the first key >= prefix while key.as_ref().starts_with(prefix)")
    }
}

impl<K: Key, V: Clone> Default for PersistentMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// One snapshot of the write set. Cloning is cheap (persistent map, small
/// commitment state, persistent policy state) and is how proposals copy on
/// write (design §4, §8).
#[derive(Clone)]
pub struct WriteSet<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
{
    entries: PersistentMap<K, Entry<V>>,
    /// Entries including tombstones; kept separately so a byte-based or
    /// adaptive capacity can replace it later (design §6.4).
    len: usize,
    /// Scheme instance: carries parameters such as a KZG setup (design §7.1).
    vc: C,
    /// Incremental commitment of `entries`.
    commitment: C::State,
    /// Per-snapshot policy state.
    policy: P,
    /// Block height: 0 for genesis, `parent.height + 1` for every proposal.
    /// The only notion of time in the set (design §4).
    height: u64,
}

/// Result of `WriteSet::apply`: what left the set plus the batch summary the
/// node keeps for statistics and for the round-0 speculation estimate
/// (design §9.2, §11).
pub struct Applied<K, V> {
    pub evicted: Evicted<K, V>,
    pub puts: usize,
    /// Puts whose key was not in the set before the batch.
    pub new_keys: usize,
    /// Puts that replaced an entry already in the set.
    pub in_place_updates: usize,
    pub deletes: usize,
    pub prefix_deletes: usize,
}

/// What left the set while applying one batch, in the canonical storage-batch
/// order of design §6.3: prefix deletes first (batch order), then evicted
/// entries sorted by key. Empty on both counts means no storage proposal.
pub struct Evicted<K, V> {
    pub prefix_deletes: Vec<Box<[u8]>>,
    pub entries: Vec<(K, Entry<V>)>,
}

impl<K, V> Evicted<K, V> {
    pub fn is_empty(&self) -> bool {
        self.prefix_deletes.is_empty() && self.entries.is_empty()
    }

    /// The storage batch in canonical order (design §6.3): `PrefixDelete`
    /// ops, then `speculate::entries_to_ops(entries)`.
    pub fn into_batch(self) -> Vec<BatchOp<K, V>> {
        todo!("PrefixDelete ops, then entries_to_ops")
    }
}

impl<K, V, C, P> WriteSet<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
{
    /// Empty set at height 0.
    pub fn genesis(vc: C, policy: P) -> Self {
        todo!("design §4")
    }

    /// Copy-on-write child at `height + 1` (design §8 Propose).
    pub fn child(&self) -> Self {
        todo!("clone + height += 1")
    }

    /// Applies `batch` in order, then evicts down to `low_watermark` if the
    /// set exceeds `capacity`. The single entry point used by both
    /// `AuthWal::propose` and `Proposal::propose` (design §4, §6.2).
    pub fn apply(&mut self, batch: Vec<BatchOp<K, V>>, capacity: usize, low_watermark: usize) -> Applied<K, V> {
        todo!("put/delete/prefix_delete in order, counting, then evict")
    }

    /// Looks a key up in this snapshot only (design §5 step 1).
    pub fn get(&self, key: &K) -> Option<&Entry<V>> {
        self.entries.get(key)
    }

    /// Predicted victims (design §9.2): `policy.likely_victims(n)` with this
    /// snapshot's entries, sorted by key. Costs `n` lookups.
    pub fn pred(&self, n: usize) -> Pred<K, V> {
        todo!("design §9.2")
    }

    /// `pred` skipping keys execution is known to be writing this block
    /// (`policy.likely_victims_excluding`, design §9.2/§9.4).
    pub fn pred_excluding(&self, n: usize, exclude: &std::collections::BTreeSet<K>) -> Pred<K, V> {
        todo!("design §9.2")
    }

    /// Inserts or updates in place, feeding the commitment and the policy.
    fn put(&mut self, key: K, value: V) {
        todo!("design §4 update in place; §7.1 apply; §6.1 on_write")
    }

    /// Records a tombstone (counts toward capacity).
    fn delete(&mut self, key: K) {
        todo!("design §4 tombstones")
    }

    /// Removes matching entries via a range scan; returns them so the caller
    /// can account for them. The prefix itself is forwarded to storage.
    fn prefix_delete(&mut self, prefix: &[u8]) -> Vec<(K, Entry<V>)> {
        todo!("keys_with_prefix, remove each, vc.apply(old -> None), policy.on_remove")
    }

    /// Evicts down to `low_watermark` if `len > capacity`; victims come from
    /// `policy.select_victims`, which removes them from policy state itself.
    fn evict(&mut self, capacity: usize, low_watermark: usize) -> Vec<(K, Entry<V>)> {
        todo!("design §6.2; result sorted by key")
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    /// Set commitment, one of the root preimage terms (design §7.3).
    pub fn commitment(&self) -> Digest {
        self.vc.commitment(&self.commitment)
    }

    /// Policy digest, one of the root preimage terms (design §7.3).
    pub fn policy_digest(&self) -> Digest {
        self.policy.digest()
    }

    pub fn policy(&self) -> &P {
        &self.policy
    }

    pub fn vc(&self) -> &C {
        &self.vc
    }

    pub fn entries(&self) -> &PersistentMap<K, Entry<V>> {
        &self.entries
    }
}
