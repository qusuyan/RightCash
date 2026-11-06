//! LRU by write: evict the key whose last write is oldest (design §6.1). A
//! write moves the key to the back. Hints exclude expected written keys from
//! predictions and retract earlier predictions for them. Actual eviction may
//! still reach those keys if too few older keys remain.
//!
//! State as sorted arrays (design §12.17 (a)): cloning shares immutable
//! input arrays through `Arc`; `writes` builds the child's arrays once by
//! merge passes. No intermediate full-array copy is needed.

use std::collections::BTreeSet;

use std::sync::Arc;

use crate::policy::{
    Eviction, EvictionPolicy, Prediction, merge_index, order_digest, params_digest, split_order, validate_watermarks,
};
use crate::types::{Digest, Key, Result};

#[derive(Clone)]
pub struct LruWrite<K: Key> {
    /// Consensus state: (last-write height, key) in eviction order, sorted;
    /// shared immutably by a clone until `writes` builds the child's array.
    /// What `state_digest` covers (design §4).
    order: Arc<Vec<(u64, K)>>,
    /// One `(key, last-write height)` per tracked key, sorted by key. Binary
    /// search recovers the old height used to remove a rewritten key from order.
    index: Arc<Vec<(K, u64)>>,
    /// Eviction target (design §6.2), this policy's own parameter: enters
    /// `params_digest`. Another policy may have none.
    low_watermark: usize,
    /// Local precomputation threshold, defaulting to capacity. Never used
    /// by actual eviction or included in `params_digest`.
    trigger: Option<usize>,
    /// Outstanding predictions: keys sent or queued for precomputation.
    /// Local only; does not affect actual eviction or the state digest.
    marked: BTreeSet<K>,
    /// Expected writes across all hints this block; excludes earlier hinted
    /// keys from later predictions because LRU writes move them to the back.
    hinted_keys: BTreeSet<K>,
    /// Distinct hinted keys absent from the snapshot, cached to avoid recounting
    /// all earlier hints. Reset with `hinted_keys` at writes/reset_hints.
    hinted_new_count: usize,
    /// Fixed prediction budget chosen on the first trigger. Retained across
    /// kept blocks; None after actual eviction or reset.
    prediction_size: Option<usize>,
    /// Next queue position to consider. Each position is scanned at most once
    /// per block; writes rebuild the order and reset this cursor.
    next_candidate: usize,
}

impl<K: Key> LruWrite<K> {
    pub fn new(low_watermark: usize) -> Self {
        Self {
            order: Arc::new(Vec::new()),
            index: Arc::new(Vec::new()),
            low_watermark,
            trigger: None,
            marked: BTreeSet::new(),
            hinted_keys: BTreeSet::new(),
            hinted_new_count: 0,
            prediction_size: None,
            next_candidate: 0,
        }
    }

    /// Start precomputation when projected occupancy reaches `trigger`.
    /// Actual eviction still waits until capacity is exceeded. The trigger
    /// must be between the low watermark and capacity (inclusive).
    pub fn with_trigger(mut self, trigger: usize) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// Return how many keys to evict from post-write occupancy `len` given
    /// the entry limit `capacity`: zero if within capacity, otherwise enough
    /// to reach this policy's low watermark. Does not mutate policy state.
    fn eviction_count(&self, len: usize, capacity: usize) -> usize {
        if len > capacity {
            len.saturating_sub(self.low_watermark)
        } else {
            0
        }
    }

    /// Reconcile sorted, distinct actual victims and written keys. A kept
    /// block withdraws rewritten predictions by visiting only written keys.
    /// Finalization supplies missing/refreshed victims and restores all marks
    /// retained in the set, including predictions displaced by false hints.
    fn reconcile(&mut self, victims: &[K], written: &[K]) -> Prediction<K> {
        let evict = victims
            .iter()
            .filter(|key| {
                let predicted = self.marked.remove::<K>(*key);
                !predicted || written.binary_search(key).is_ok()
            })
            .cloned()
            .collect();
        let mut retract = if victims.is_empty() {
            Vec::new()
        } else {
            self.prediction_size = None;
            self.marked.iter().cloned().collect()
        };
        for key in written {
            if self.marked.remove::<K>(key) && victims.is_empty() {
                retract.push(key.clone());
            }
        }
        self.hinted_keys.clear();
        self.hinted_new_count = 0;
        self.next_candidate = 0;
        Prediction { evict, retract }
    }

    fn present(&self, key: &K) -> Option<u64> {
        self.index
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|i| self.index[i].1)
    }
}

impl<K: Key> EvictionPolicy<K> for LruWrite<K> {
    fn encode_state(&self, keys: &[K]) -> Result<Vec<u8>> {
        super::encode_heights(&self.index, keys)
    }

    fn restore_state(&mut self, bytes: &[u8], keys: &[K], height: u64) -> Result<()> {
        let index = super::decode_heights(bytes, keys, height)?;
        let mut order: Vec<_> = index.iter().map(|(key, height)| (*height, key.clone())).collect();
        order.sort_unstable();
        self.index = Arc::new(index);
        self.order = Arc::new(order);
        self.reset_hints();
        Ok(())
    }

    /// Apply one batch's writes to this policy and return its eviction decision.
    /// `keys` contains one key per put/delete in execution order; duplicates
    /// are allowed and tombstones count as present. `height` is the new block's
    /// height, greater than all tracked heights. `capacity` limits tracked keys.
    ///
    /// Every written key receives last-write height `height`, moving existing
    /// keys to the back. Ties at the same height are broken by key.
    /// If occupancy after adding new keys exceeds capacity, remove the oldest
    /// keys until only `low_watermark` remain; otherwise remove none.
    ///
    /// Updates both `order` and `index`. Returns `Eviction` with distinct
    /// `all_evicted_keys` keys sorted by key and a local prediction `refinement` from
    /// reconciling marks with the actual victims and written keys. Marks are
    /// updated but never influence the victim selection (design §6.1).
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K> {
        let mut written_keys: Vec<K> = keys.to_vec();
        written_keys.sort_unstable();
        written_keys.dedup();
        let mut removed_entries: Vec<(u64, K)> = Vec::with_capacity(written_keys.len());
        let mut new_key_count = 0usize;
        for key in &written_keys {
            match self.present(key) {
                Some(previous_height) => removed_entries.push((previous_height, key.clone())),
                None => new_key_count += 1,
            }
        }
        removed_entries.sort_unstable();
        let appended_entries: Vec<(u64, K)> = written_keys.iter().map(|key| (height, key.clone())).collect();
        let mut order = split_order(&self.order, &removed_entries, &appended_entries);
        let eviction_count = self.eviction_count(self.index.len() + new_key_count, capacity);
        let mut all_evicted_keys: Vec<K> = order
            .drain(..eviction_count.min(order.len()))
            .map(|(_, key)| key)
            .collect();
        all_evicted_keys.sort_unstable();
        let refinement = self.reconcile(&all_evicted_keys, &written_keys);
        let height_updates: Vec<(K, u64)> = written_keys.into_iter().map(|key| (key, height)).collect();
        self.index = Arc::new(merge_index(&self.index, &height_updates, &all_evicted_keys));
        self.order = Arc::new(order);
        Eviction {
            all_evicted_keys,
            refinement,
        }
    }

    /// Start a fixed capacity-minus-low prediction window at the trigger,
    /// plus margin. Exclude hinted writes and replace withdrawn marked keys
    /// with the next eligible queue entries. The cursor only moves forward
    /// within this immutable order, avoiding a prefix scan for every hint.
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K> {
        let mut retract = Vec::new();
        // Record the complete hint before choosing replacements: a later key
        // in this same hint must never become a new eviction candidate.
        for key in keys {
            if self.hinted_keys.insert(key.clone()) && self.prediction_size.is_none() && self.present(key).is_none() {
                self.hinted_new_count += 1;
            }
            if self.marked.remove::<K>(key) {
                retract.push(key.clone());
            }
        }
        retract.sort_unstable();
        if self.prediction_size.is_none()
            && self.index.len() + self.hinted_new_count >= self.trigger.unwrap_or(capacity)
        {
            self.prediction_size = Some(capacity.saturating_sub(self.low_watermark).saturating_add(margin));
        }
        let mut evict = Vec::new();
        if let Some(count) = self.prediction_size {
            while self.marked.len() < count && self.next_candidate < self.order.len() {
                let key = &self.order[self.next_candidate].1;
                self.next_candidate += 1;
                if !self.hinted_keys.contains::<K>(key) && self.marked.insert(key.clone()) {
                    evict.push(key.clone());
                }
            }
        }
        Prediction { evict, retract }
    }

    fn predicted(&self) -> Vec<K> {
        self.marked.iter().cloned().collect()
    }

    fn reset_hints(&mut self) {
        self.marked.clear();
        self.hinted_keys.clear();
        self.hinted_new_count = 0;
        self.prediction_size = None;
        self.next_candidate = 0;
    }

    fn validate(&self, capacity: usize) -> Result<()> {
        validate_watermarks(self.low_watermark, self.trigger, capacity)
    }

    fn state_digest(&self) -> Digest {
        order_digest(self.name(), self.order.iter())
    }

    fn name(&self) -> &'static str {
        "lru-write"
    }

    /// `policy::params_digest` (design §6.1, §7.3): the consensus low watermark only.
    fn params_digest(&self) -> Digest {
        params_digest(self.low_watermark)
    }
}
