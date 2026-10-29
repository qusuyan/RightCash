//! Eviction policies (design §6).
//!
//! Policy state lives on the snapshot (design §4) and is copied with it: a
//! proposal's policy sees exactly the written keys of its own batch applied
//! to exactly its parent's state, and evicts from that materialised state.
//! There is no query through a chain of overlays (R20): a block proposed on
//! a live parent and the same block proposed after the parent committed run
//! the same code on the same state and reach the same root by construction.
//!
//! Two entry points (R25, design §6.1). `writes` is consensus-critical: it
//! takes a batch's written keys, evicts by the policy's own rule and returns
//! the victims, a deterministic function of the state the policy carries,
//! the keys, the height and the capacity. `hint_writes` is local: it takes
//! a prefetch hint and answers which likely victims to send to storage early
//! and which earlier predictions to put back (design §9.2). The policy
//! keeps the prediction state itself, the keys it marked as sent, and
//! `writes` reconciles the marks with the actual victims into one last
//! `Prediction`: what still has to be sent and what must be put back.
//! `state_digest` covers the consensus state only and is a root term
//! (design §7.3), so equal roots imply equal eviction order; the marks never
//! reach a root. Hints mutate the policy inside the target node's snapshot
//! (design §4, R26), the one thing in a snapshot that changes after it is
//! built.
//!
//! How many victims an eviction removes is the policy's rule inside
//! `writes`: the capacity is the snapshot's and is passed in (design §4,
//! R23). The v0 policies evict down to a low watermark, a parameter each
//! keeps as its own, unknown to the trait (design §6.2): some policies have
//! none.

pub mod fifo;
pub mod lru_write;

use std::collections::BTreeSet;

pub use fifo::Fifo;
pub use lru_write::LruWrite;

use crate::types::{Digest, Key};

/// Domain of [`order_digest`].
const STATE_DOMAIN: &str = "auth_wal/policy-state";

/// `state_digest` of the v0 policies (design §7.3): BLAKE3 `derive_key`
/// under `STATE_DOMAIN` over the policy's name (`u32 LE` length-prefixed)
/// and its eviction order as `(height u64 LE, u32 LE key length, key bytes)`
/// in order, one pass over the state per call (a full hash, clarification
/// 21; an incremental accumulator replaces it if it measures as too slow).
/// Shared by both policies; a policy with another order structure writes its
/// own.
pub(crate) fn order_digest<'a, K: Key + 'a>(name: &str, order: impl Iterator<Item = &'a (u64, K)>) -> Digest {
    let mut hasher = blake3::Hasher::new_derive_key(STATE_DOMAIN);
    hasher.update(&(name.len() as u32).to_le_bytes());
    hasher.update(name.as_bytes());
    for (height, key) in order {
        let bytes = key.as_ref();
        hasher.update(&height.to_le_bytes());
        hasher.update(&(bytes.len() as u32).to_le_bytes());
        hasher.update(bytes);
    }
    *hasher.finalize().as_bytes()
}

/// The prediction state the v0 policies share (design §9.2): the keys
/// `hint_writes` returned as likely victims and has not retracted, so what
/// the steps carry and what storage holds (design §9.1 invariant). Embedded
/// beside the consensus state and never read by the victim selection or by
/// `state_digest`; `reconcile` clears it at `writes` and `clear` at a
/// retarget (design §9.2).
#[derive(Clone)]
pub(crate) struct Marks<K: Key> {
    marked: BTreeSet<K>,
}

impl<K: Key> Default for Marks<K> {
    fn default() -> Self {
        Self {
            marked: BTreeSet::new(),
        }
    }
}

impl<K: Key> Marks<K> {
    pub(crate) fn is_marked(&self, key: &K) -> bool {
        self.marked.contains::<K>(key)
    }

    /// Marks a key as sent; returns whether it was newly marked.
    pub(crate) fn mark(&mut self, key: &K) -> bool {
        self.marked.insert(key.clone())
    }

    /// Withdraws a mark; returns whether the key was marked.
    pub(crate) fn unmark(&mut self, key: &K) -> bool {
        self.marked.remove::<K>(key)
    }

    pub(crate) fn clear(&mut self) {
        self.marked.clear();
    }

    /// At `writes` (design §9.1), the last refinement: `evict` are the
    /// victims whose current entry storage lacks (not marked, or marked and
    /// written by the batch, whose sent entry is stale), in the victims'
    /// order; `retract` are the marked keys not evicted after all, in key
    /// order. A marked victim the batch did not write is in neither list:
    /// the step that carried it wrote its final entry. Clears the marks.
    pub(crate) fn reconcile(&mut self, victims: &[K], written: &[K]) -> Prediction<K> {
        let written: BTreeSet<&K> = written.iter().collect();
        let marked = std::mem::take(&mut self.marked);
        let evict = victims
            .iter()
            .filter(|key| !marked.contains::<K>(key) || written.contains(key))
            .cloned()
            .collect();
        let victims: BTreeSet<&K> = victims.iter().collect();
        let retract = marked.into_iter().filter(|key| !victims.contains(key)).collect();
        Prediction { evict, retract }
    }
}

/// What `hint_writes` answers, and what `writes` answers as its last
/// refinement (design §6.1, §9.2). Local: never in a root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prediction<K> {
    /// Keys to send to storage with their current entries: from a hint, the
    /// keys newly marked as likely victims; from `writes`, the victims whose
    /// entry storage does not hold yet.
    pub evict: Vec<K>,
    /// Keys marked earlier that stay in the set, now unmarked: storage gets
    /// its own value back.
    pub retract: Vec<K>,
}

/// What `writes` answers (design §6.1, §9.1): the victims, which the set
/// removes, and the last refinement, which the final proposal applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Eviction<K> {
    /// Every victim, in eviction order: the consensus output.
    pub evicted: Vec<K>,
    /// Local: `evict` is the subset of `evicted` storage lacks, `retract`
    /// the marked keys kept.
    pub refinement: Prediction<K>,
}

/// `Clone`: the state is copied with the snapshot at every proposal (design
/// §4, about 0.9 ms of the 2 ms per proposal at 10k keys for the v0
/// policies).
pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {
    /// Consensus-critical (design §6.1). The keys of one batch, in batch
    /// order (a `Put` or `Delete` each, a write to a present key included),
    /// landed in the set at `height`, and the eviction that follows (design
    /// §6.2): the policy updates its order, decides how many to evict from
    /// the set size (it tracks exactly the set's keys) and `capacity` by its
    /// own rule, removes the victims from its own state and returns them.
    /// `evicted` is a function of the consensus state, `keys`, `height` and
    /// `capacity` alone, never of the marks, with ties broken by key bytes.
    /// The call also reconciles the marks with the victims into the last
    /// `refinement` (design §9.1) and clears them, so the child's policy
    /// state carries no prediction.
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K>;

    /// Local (design §9.2). Execution is writing `keys` (a prefetch hint;
    /// duplicates and keys hinted before are allowed). The policy predicts
    /// how many evictions the block will cause from the set size, `capacity`,
    /// this hint's insertions (its keys absent from the set) and `margin`
    /// extra victims (R15, pessimistic prediction; no count is kept across
    /// hints, clarification 22, so the margin also stands in for earlier
    /// hints' insertions), marks that many likely victims and returns the
    /// newly marked ones in `evict` (so a victim is returned once per
    /// block), and returns in `retract` the marked keys the hint showed will
    /// stay, unmarking them. Must not touch the consensus state;
    /// deterministic given the state and the hint sequence (design §9.3
    /// tests).
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K>;

    /// Forgets every mark (design §9.2): the precomputation was reset, so
    /// nothing marked is in its storage any more. Consensus state untouched.
    fn reset_hints(&mut self);

    /// Digest of the consensus state (the eviction order), a root preimage
    /// term (design §7.3): equal digests mean identical future `writes`.
    /// Never covers the marks. Computed once per proposal by
    /// `Snapshot::apply`; the v0 policies hash their whole order
    /// (`order_digest`), a choice to revisit with the benchmark.
    fn state_digest(&self) -> Digest;

    /// Stable identifier, hashed into the root preimage and used in
    /// configuration and benchmark output.
    fn name(&self) -> &'static str;

    /// Digest of the policy's fixed parameters, a root preimage term (design
    /// §7.3): every policy states its own (zeros if it has none; the v0
    /// policies digest their low watermark), so two networks differing only
    /// in a parameter never share a root.
    fn params_digest(&self) -> Digest;
}
