//! FIFO: evict in order of first insertion (design §6.1). A write to a key
//! already in the set does not move it, so a hint never retracts a mark and
//! never excludes a key from the prediction: a hinted existing key remains
//! eligible at its original position in the eviction order.
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
pub struct Fifo<K: Key> {
    /// Consensus state: (insertion height, key) in eviction order, sorted;
    /// shared immutably by a clone until `writes` builds the child's array.
    /// What `state_digest` covers (design §4).
    order: Arc<Vec<(u64, K)>>,
    /// One `(key, insertion height)` per tracked key, sorted by key. Binary
    /// search checks membership; heights describe order positions, not offsets.
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
    /// Distinct expected insertions across hints this block. Existing keys
    /// need no hint history under FIFO because rewrites keep their positions.
    hinted_new_keys: BTreeSet<K>,
    /// A chain selects its FIFO prefix once, even if it selected no keys.
    prediction_started: bool,
}

impl<K: Key> Fifo<K> {
    pub fn new(low_watermark: usize) -> Self {
        Self {
            order: Arc::new(Vec::new()),
            index: Arc::new(Vec::new()),
            low_watermark,
            trigger: None,
            marked: BTreeSet::new(),
            hinted_new_keys: BTreeSet::new(),
            prediction_started: false,
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

    /// Reconcile final victims with predictions. Written keys and victims
    /// are sorted and distinct. A kept block only refreshes rewritten marks;
    /// an evicting block also supplies unpredicted victims and restores any
    /// surplus predictions (possible with a nonzero speculation margin).
    fn reconcile(&mut self, victims: &[K], written: &[K]) -> Prediction<K> {
        self.hinted_new_keys.clear();
        if victims.is_empty() {
            return Prediction {
                evict: written
                    .iter()
                    .filter(|key| self.marked.contains::<K>(*key))
                    .cloned()
                    .collect(),
                retract: Vec::new(),
            };
        }
        self.prediction_started = false;
        let evict = victims
            .iter()
            .filter(|key| {
                let predicted = self.marked.remove::<K>(*key);
                !predicted || written.binary_search(key).is_ok()
            })
            .cloned()
            .collect();
        Prediction {
            evict,
            retract: self.marked.iter().cloned().collect(),
        }
    }

    fn contains(&self, key: &K) -> bool {
        self.index.binary_search_by(|(k, _)| k.cmp(key)).is_ok()
    }
}

impl<K: Key> EvictionPolicy<K> for Fifo<K> {
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
    /// New keys receive insertion height `height`; existing keys keep their
    /// positions. Ties at the same height are broken by key.
    /// If occupancy after adding new keys exceeds capacity, remove the oldest
    /// keys until only `low_watermark` remain; otherwise remove none.
    ///
    /// Updates both `order` and `index`. Returns `Eviction` with distinct
    /// `all_evicted_keys` keys sorted by key and a local prediction `refinement` from
    /// reconciling marks with the actual victims and written keys. Marks are
    /// updated but never influence the victim selection (design §6.1).
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K> {
        let mut written: Vec<K> = keys.to_vec();
        written.sort_unstable();
        written.dedup();
        let new_keys: Vec<K> = written.iter().filter(|key| !self.contains(key)).cloned().collect();
        let appended_entries: Vec<(u64, K)> = new_keys.iter().map(|key| (height, key.clone())).collect();
        let mut order = split_order(&self.order, &[], &appended_entries);
        let eviction_count = self.eviction_count(self.index.len() + new_keys.len(), capacity);
        let mut all_evicted_keys: Vec<K> = order
            .drain(..eviction_count.min(order.len()))
            .map(|(_, key)| key)
            .collect();
        all_evicted_keys.sort_unstable();
        let height_updates: Vec<(K, u64)> = new_keys.into_iter().map(|key| (key, height)).collect();
        self.index = Arc::new(merge_index(&self.index, &height_updates, &all_evicted_keys));
        self.order = Arc::new(order);
        let refinement = self.reconcile(&all_evicted_keys, &written);
        Eviction {
            all_evicted_keys,
            refinement,
        }
    }

    /// On the first trigger, predict the oldest capacity-minus-low keys,
    /// plus the configured margin. Later hints do nothing: FIFO writes never
    /// change queue positions, and projected overflow is handled by writes.
    /// Only the first trigger needs to track expected insertions. Hints never
    /// retract FIFO predictions; with margin zero neither does finalization.
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K> {
        let mut evict = Vec::new();
        if !self.prediction_started {
            for key in keys {
                if !self.contains(key) {
                    self.hinted_new_keys.insert(key.clone());
                }
            }
            if self.index.len() + self.hinted_new_keys.len() >= self.trigger.unwrap_or(capacity) {
                self.prediction_started = true;
                let count = capacity.saturating_sub(self.low_watermark).saturating_add(margin);
                for (_, key) in self.order.iter().take(count) {
                    if self.marked.insert(key.clone()) {
                        evict.push(key.clone());
                    }
                }
                self.hinted_new_keys.clear();
            }
        }
        Prediction {
            evict,
            retract: Vec::new(),
        }
    }

    fn predicted(&self) -> Vec<K> {
        self.marked.iter().cloned().collect()
    }

    fn reset_hints(&mut self) {
        self.marked.clear();
        self.hinted_new_keys.clear();
        self.prediction_started = false;
    }

    fn validate(&self, capacity: usize) -> Result<()> {
        validate_watermarks(self.low_watermark, self.trigger, capacity)
    }

    fn state_digest(&self) -> Digest {
        order_digest(self.name(), self.order.iter())
    }

    fn name(&self) -> &'static str {
        "fifo"
    }

    /// SHA-256 of `"auth_wal/policy-params" || low_watermark (u64 LE)`;
    /// no local prediction parameters (design §6.1, §7.3).
    fn params_digest(&self) -> Digest {
        params_digest(self.low_watermark)
    }
}
