//! FIFO: evict in order of first insertion (design §6.1). A write to a key
//! already in the set does not move it, so a hint never retracts a mark and
//! never excludes a key from the prediction: a hinted in-set key is still
//! the next victim.

use std::collections::{BTreeMap, BTreeSet};

use crate::policy::{Eviction, EvictionPolicy, Marks, Prediction, order_digest};
use crate::types::{Digest, Key};

#[derive(Clone)]
pub struct Fifo<K: Key> {
    /// Consensus state: (insertion height, key) in eviction order; copied
    /// with the snapshot (design §4). What `state_digest` covers.
    order: BTreeSet<(u64, K)>,
    /// Consensus state, derived: key -> insertion height, so a write and a
    /// removal find the key's place in `order`.
    index: BTreeMap<K, u64>,
    /// Eviction target (design §6.2), this policy's own parameter: enters
    /// `params_digest`. Another policy may have none.
    low_watermark: usize,
    /// Prediction state (design §9.2), local: never read by the victim
    /// selection or by `state_digest`.
    marks: Marks<K>,
}

impl<K: Key> Fifo<K> {
    pub fn new(low_watermark: usize) -> Self {
        Self {
            order: BTreeSet::new(),
            index: BTreeMap::new(),
            low_watermark,
            marks: Marks::default(),
        }
    }

    /// The v0 rule (design §6.2): nothing while `len <= capacity`, down to
    /// the watermark once over it. Shared by `writes` and `hint_writes`.
    fn eviction_count(&self, len: usize, capacity: usize) -> usize {
        if len > capacity {
            len.saturating_sub(self.low_watermark)
        } else {
            0
        }
    }
}

impl<K: Key> EvictionPolicy<K> for Fifo<K> {
    /// Design §6.1: every key absent from `index` is inserted at `(height,
    /// key)` (a present key keeps its place); `n =
    /// eviction_count(index.len(), capacity)`; the
    /// first `n` of `order` are the victims, removed from `order` and
    /// `index`, returned in that order; `refinement =
    /// marks.reconcile(&victims, keys)`, which also clears the marks.
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K> {
        todo!("design §6.1 fifo writes")
    }

    /// Design §9.2: `new` = the keys of this hint absent from `index`
    /// (counted once each); `expected = index.len() + new`; `n =
    /// eviction_count(expected, capacity)`; `E = n
    /// > 0 ? n + margin : 0`; walk `order` from the front over the first `E`
    /// keys and return those not yet marked, marking them. Nothing is
    /// retracted (a rewrite keeps the key's place). Reads nothing but
    /// `order`, `index` and the marks; changes the marks only.
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K> {
        todo!("design §9.2 fifo hint_writes")
    }

    fn reset_hints(&mut self) {
        self.marks.clear();
    }

    fn state_digest(&self) -> Digest {
        order_digest(self.name(), self.order.iter())
    }

    fn name(&self) -> &'static str {
        "fifo"
    }

    /// SHA-256 of `"auth_wal/policy-params" || low_watermark (u64 LE)`
    /// (design §6.1, §7.3).
    fn params_digest(&self) -> Digest {
        todo!("sha256(domain || low_watermark LE)")
    }
}
