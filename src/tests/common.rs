//! Shared fixtures for the library's unit and cross-module tests.

use crate::policy::EvictionPolicy;
use crate::storage::{MemoryConfig, MemoryStorage, StorageEngine};
use crate::types::{BatchOp, Hash};
use crate::vc::VectorCommitment;
use crate::{Config, Consensus, Local, RightCash};

pub(super) type K = Vec<u8>;
pub(super) type V = Vec<u8>;

pub(crate) fn k(s: &str) -> K {
    s.as_bytes().to_vec()
}

pub(crate) fn put(key: &str, value: &str) -> BatchOp<K, V> {
    BatchOp::Put {
        key: k(key),
        value: k(value),
    }
}

pub(crate) fn del(key: &str) -> BatchOp<K, V> {
    BatchOp::Delete { key: k(key) }
}

pub(crate) fn keys(names: &[&str]) -> Vec<K> {
    names.iter().map(|n| k(n)).collect()
}

/// xorshift64*: deterministic randomness for the fuzz tests, no dependency.
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// A fresh directory under the system temp dir, removed when dropped (on
/// success and on panic); firewood databases live in it.
#[cfg(feature = "firewood")]
pub(crate) struct TempDir(std::path::PathBuf);

#[cfg(feature = "firewood")]
impl TempDir {
    pub(crate) fn new(tag: &str) -> Self {
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "right_cash-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// The database path inside the directory.
    pub(crate) fn db(&self) -> std::path::PathBuf {
        self.0.join("db")
    }
}

#[cfg(feature = "firewood")]
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) type Cache<C, P> = RightCash<K, V, C, P, MemoryStorage<K, V>>;

// Empty-start fixtures keep their policy/scheme convenience at the test boundary.
pub(super) fn new_empty<C, P, S>(
    config: Config,
    storage: S,
    policy: P,
    vc: C,
) -> crate::types::Result<RightCash<K, V, C, P, S>>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    let set = crate::set::Snapshot::genesis(vc, policy, config.consensus.capacity);
    RightCash::new(config, storage, set)
}

pub(super) fn config(capacity: usize, revisions: usize) -> Config {
    Config {
        consensus: Consensus { capacity },
        local: Local {
            revisions,
            ..Local::default()
        },
    }
}

pub(super) fn open<C, P>(capacity: usize, revisions: usize, policy: P, vc: C) -> Cache<C, P>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let engine = MemoryStorage::new(MemoryConfig {
        revisions,
        ..MemoryConfig::default()
    });
    new_empty(config(capacity, revisions), engine, policy, vc).unwrap()
}

pub(super) fn get<C, P>(cache: &Cache<C, P>, key: &str) -> Option<V>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    cache.reader().get(&k(key)).unwrap()
}

pub(super) fn speculating(capacity: usize, revisions: usize, rounds: u32, min_step: usize, margin: usize) -> Config {
    Config {
        consensus: Consensus { capacity },
        local: Local {
            revisions,
            speculate: true,
            speculation_rounds: rounds,
            speculation_min_step: min_step,
            speculation_margin: margin,
            snapshot_path: None,
        },
    }
}

pub(super) fn open_with<C, P>(config: Config, policy: P, vc: C) -> Cache<C, P>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let engine = MemoryStorage::new(MemoryConfig {
        revisions: config.local.engine_window(true),
        ..MemoryConfig::default()
    });
    new_empty(config, engine, policy, vc).unwrap()
}

/// `k0..kn` with value "0" on both instances: the set every scenario starts
/// from.
pub(super) fn fill<C, P, S>(plain: &mut RightCash<K, V, C, P, S>, spec: &mut RightCash<K, V, C, P, S>, n: usize)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    plain.update(fill_batch(n), None).unwrap();
    spec.update(fill_batch(n), None).unwrap();
}

pub(super) fn fill_batch(n: usize) -> Vec<BatchOp<K, V>> {
    (0..n).map(|i| put(&format!("k{i}"), "0")).collect()
}

/// The same block on both instances; their roots must agree.
pub(super) fn agree<C, P, S>(
    plain: &mut RightCash<K, V, C, P, S>,
    spec: &mut RightCash<K, V, C, P, S>,
    batch: Vec<BatchOp<K, V>>,
) -> Hash
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    let expected = plain.update(batch.clone(), None).unwrap();
    assert_eq!(spec.update(batch, None).unwrap(), expected);
    expected
}

/// No step the engine refused, no fallback to the plain path, no panic on
/// the worker.
pub(super) fn assert_no_failures(s: &crate::speculate::SpeculationStats) {
    assert_eq!((s.fallbacks, s.step_failures, s.worker_panics), (0, 0, 0), "{s:?}");
}
