//! LRU by write: evict the key whose last write is oldest (design §6.1).
//! A hint is invalidated only if the key is written again before eviction.

use crate::policy::{EvictionPolicy, HeightDigest};
use crate::types::{Digest, Key};

#[derive(Clone)]
pub struct LruWrite<K: Key> {
    /// (last-write height, key) in eviction order.
    order: im::OrdSet<(u64, K)>,
    /// Reverse index: key -> last-write height.
    index: im::OrdMap<K, u64>,
    digest: HeightDigest,
}

impl<K: Key> LruWrite<K> {
    pub fn new() -> Self {
        todo!("design §6.1 lru-write")
    }

    /// Shared removal path for `select_victims` and `on_remove`.
    fn remove(&mut self, key: &K) {
        todo!()
    }
}

impl<K: Key> Default for LruWrite<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key> EvictionPolicy<K> for LruWrite<K> {
    fn on_write(&mut self, key: &K, height: u64) {
        todo!("move key to (height, key); digest.remove(old) + digest.add(new)")
    }

    fn on_remove(&mut self, key: &K) {
        self.remove(key)
    }

    fn select_victims(&mut self, n: usize) -> Vec<K> {
        todo!()
    }

    fn likely_victims(&self, n: usize) -> Vec<K> {
        todo!()
    }

    fn digest(&self) -> Digest {
        self.digest.digest()
    }

    fn name(&self) -> &'static str {
        "lru-write"
    }

    fn params_digest(&self) -> Digest {
        [0u8; 32]
    }
}
