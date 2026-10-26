//! FIFO: evict in order of first insertion (design §6.1). Updates do not
//! change position, so `likely_victims` is exact unless a victim is
//! re-inserted after eviction.

use crate::policy::{EvictionPolicy, HeightDigest};
use crate::types::{Digest, Key};

#[derive(Clone)]
pub struct Fifo<K: Key> {
    /// (insertion height, key) in eviction order.
    order: im::OrdSet<(u64, K)>,
    /// Reverse index: key -> insertion height.
    index: im::OrdMap<K, u64>,
    digest: HeightDigest,
}

impl<K: Key> Fifo<K> {
    pub fn new() -> Self {
        todo!("design §6.1 fifo")
    }

    /// Shared removal path for `select_victims` and `on_remove`.
    fn remove(&mut self, key: &K) {
        todo!("index.remove -> order.remove((h, key)) -> digest.remove")
    }
}

impl<K: Key> Default for Fifo<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key> EvictionPolicy<K> for Fifo<K> {
    fn on_write(&mut self, key: &K, height: u64) {
        todo!("insert only if absent; digest.add")
    }

    fn on_remove(&mut self, key: &K) {
        self.remove(key)
    }

    fn select_victims(&mut self, n: usize) -> Vec<K> {
        todo!("first n of `order`, removing each")
    }

    fn likely_victims(&self, n: usize) -> Vec<K> {
        todo!("first n of `order`, non-mutating")
    }

    /// FIFO ignores the exclusion (design §6.1 table): an update does not
    /// move a key, so a hinted in-set key is still the next victim; excluding
    /// it would predict keys that stay and cause reverts.
    fn likely_victims_excluding(&self, n: usize, _exclude: &std::collections::BTreeSet<K>) -> Vec<K> {
        self.likely_victims(n)
    }

    fn digest(&self) -> Digest {
        self.digest.digest()
    }

    fn name(&self) -> &'static str {
        "fifo"
    }

    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }
}
