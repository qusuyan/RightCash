//! Eviction policies (design §6).
//!
//! Policy state is part of every write-set snapshot, so a policy must be
//! cheaply `Clone` (use persistent structures) and fully deterministic: it may
//! observe only writes and block heights, never reads or wall-clock time.
//! `select_victims` is consensus-critical and `digest` must cover everything
//! that determines it, so equal roots imply equal future behaviour (design
//! §7.3). `likely_victims` only steers speculative eviction (design §9), whose
//! mispredictions are always reverted, so it is a local heuristic.

pub mod digest;
pub mod fifo;
pub mod lru_write;

pub use digest::HeightDigest;
pub use fifo::Fifo;
pub use lru_write::LruWrite;

use crate::types::{Digest, Key};

pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {
    /// Called for every key written by a proposal, in batch order.
    fn on_write(&mut self, key: &K, height: u64);

    /// Called when a key leaves the set other than through `select_victims`
    /// (prefix delete). `select_victims` removes its own victims.
    fn on_remove(&mut self, key: &K);

    /// Chooses up to `n` keys to evict now and removes them from policy
    /// state. Must be deterministic given identical call history; ties are
    /// broken by key bytes.
    fn select_victims(&mut self, n: usize) -> Vec<K>;

    /// Non-mutating prediction of the next `min(n, len)` victims, best first.
    /// Local use only (speculative eviction, design §9); never affects a root.
    /// Should be a cheap, deterministic function of the state so speculation
    /// is reproducible in tests.
    fn likely_victims(&self, n: usize) -> Vec<K>;

    /// Prediction that skips keys execution is known to be writing this block
    /// (prefetch hints, design §9.4), which under LRU-write will not be
    /// evicted. Default: `likely_victims(n + exclude.len())` minus `exclude`,
    /// first `n`. FIFO may override to keep hinted keys (updates do not move
    /// them).
    fn likely_victims_excluding(&self, n: usize, exclude: &std::collections::BTreeSet<K>) -> Vec<K> {
        let mut out: Vec<K> = self
            .likely_victims(n + exclude.len())
            .into_iter()
            .filter(|k| !exclude.contains::<K>(k))
            .collect();
        out.truncate(n);
        out
    }

    /// Digest of everything that determines future `select_victims` output.
    /// Maintained incrementally; enters the root preimage (design §7.3).
    fn digest(&self) -> Digest;

    /// Stable identifier, hashed into the root preimage and used in
    /// configuration and benchmark output.
    fn name(&self) -> &'static str;

    /// Digest of the policy's fixed parameters (zeros for the v0 policies;
    /// e.g. a decay constant for a future LFU). Enters the root preimage.
    fn params_digest(&self) -> Digest;
}
