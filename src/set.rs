//! The write set (design §4, R20): every proposal owns a complete
//! `Snapshot`, a copy of its parent's with the batch applied, policy state
//! and commitment included. Nothing is mutated once shared, except the
//! policy's prediction marks behind their lock (design §9.2, R26), so reads
//! take no lock and need no chain (design §5).

use std::collections::BTreeMap;

use parking_lot::Mutex;

use crate::policy::{EvictionPolicy, Prediction};
use crate::types::{BatchOp, Digest, Key, Value};
use crate::vc::VectorCommitment;

/// A set entry. Deletes stay as tombstones until evicted, when they become a
/// `BatchOp::Delete` for storage. Tombstones count toward capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry<V> {
    Value(V),
    Tombstone,
}

/// Entries in key order: the evicted entries of `Snapshot::apply` (design
/// §6.3); `speculate::entries_to_ops` turns them into a storage batch.
pub(crate) type Entries<K, V> = Vec<(K, Entry<V>)>;

/// The whole write set at one node (design §4): entries, policy state and
/// commitment at `height`. Immutable once built and shared by `Arc` through
/// its node (design §8); `Snapshot::apply` builds a child's as a copy with
/// the batch applied. The one exception is the policy's prediction marks,
/// which hints write through `hint_writes` (design §9.2).
pub(crate) struct Snapshot<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
{
    /// Ordered: canonical iteration, no hasher.
    entries: BTreeMap<K, Entry<V>>,
    /// Policy state as of this node (design §6.1), copied with the entries.
    /// Behind a mutex because hints mark predictions in it while the node is
    /// the precomputation's target (design §9.2, R26): the writer alone
    /// takes it (hints, `apply`'s clone, a retarget's reset), serialised by
    /// `&mut self`, so it is never contended; readers never touch it.
    policy: Mutex<P>,
    /// `policy.state_digest()` as of this node, a root preimage term (design
    /// §7.3), computed once when the snapshot is built.
    policy_digest: Digest,
    /// Scheme instance (parameters) and the incremental commitment of `entries`.
    vc: C,
    commitment: C::State,
    height: u64,
    /// Maximum number of entries, tombstones included (design §6.2): the
    /// capacity in force at this node, a consensus parameter today and
    /// per-node state once it adapts (R4, design §12.8); a root preimage
    /// term of the node (design §7.3, R23).
    capacity: usize,
}

impl<K, V, C, P> Snapshot<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
{
    /// Empty set at height 0 with `capacity` in force.
    pub fn genesis(vc: C, policy: P, capacity: usize) -> Self {
        let commitment = vc.empty();
        let policy_digest = policy.state_digest();
        Self {
            entries: BTreeMap::new(),
            policy: Mutex::new(policy),
            policy_digest,
            vc,
            commitment,
            height: 0,
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    /// The capacity in force at this node (design §6.2).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Design §5 step 1: one lookup.
    pub fn get(&self, key: &K) -> Option<&Entry<V>> {
        self.entries.get::<K>(key)
    }

    /// Set commitment, a root preimage term (design §7.3).
    pub fn commitment(&self) -> Digest {
        self.vc.commitment(&self.commitment)
    }

    /// The policy's state digest, a root preimage term (design §7.3).
    pub fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// A prefetch hint on this node (design §9.2): `policy.hint_writes(keys,
    /// capacity, margin)` under the policy lock. Local; the consensus state
    /// is untouched.
    pub fn hint_writes(&self, keys: &[K], margin: usize) -> Prediction<K> {
        self.policy.lock().hint_writes(keys, self.capacity, margin)
    }

    /// A retarget to this node (design §9.2): the policy forgets its marks,
    /// since nothing marked is in the new precomputation's storage.
    pub fn reset_hints(&self) {
        self.policy.lock().reset_hints();
    }

    /// Design §8 Propose and §4 batch semantics: the child's snapshot is a
    /// copy of this one (entries, policy, commitment, capacity; about 2 ms
    /// at 10k, design §4; the policy is cloned under its lock and carries
    /// this node's marks, design §9.2) at `height + 1` with `batch` applied
    /// **in order**: `Put`/`Delete` insert the entry (a tombstone for
    /// `Delete`) and update the commitment with `vc.apply(k, old,
    /// Some(new))`, `old` what the copy held before the op (design §7.2);
    /// `batch` holds no `PrefixDelete` (`propose` rejected it, design §4;
    /// one here is a caller bug, `unreachable!`). Then the policy: `ev =
    /// policy.writes(&keys, height, capacity)` with the batch's keys in
    /// batch order (design §6.1), after which every key of `ev.evicted` is
    /// removed from the copy (`vc.apply(k, Some(old), None)`; a key the
    /// policy names that the set lacks is a policy bug, `unreachable!`) and
    /// collected into `evicted`, sorted by key (design §6.3), and
    /// `assert!(entries.len() <= capacity)` holds the policy to its contract
    /// (design §6.2: a violation halts rather than diverges). `refinement`
    /// is `ev.refinement` with both lists sorted by key (design §9.1).
    /// Finally `policy_digest = policy.state_digest()` (one hash over the
    /// order, design §7.3). The summary counts puts and puts of present keys
    /// (`in_place_updates`, design §11).
    pub fn apply(&self, batch: Vec<BatchOp<K, V>>) -> Applied<K, V, C, P> {
        todo!("design §4 batch semantics, §6.1 writes, §6.2 eviction")
    }
}

/// Counts of one batch application, consumed by `propose` for the
/// statistics (design §9.5, §11).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AppliedSummary {
    pub puts: usize,
    /// Puts of a key present at the time of the op (a key put twice in one
    /// batch counts here the second time).
    pub in_place_updates: usize,
}

/// Result of `Snapshot::apply`: the child's snapshot, what left the set,
/// the policy's last refinement of its prediction (design §9.1), and the
/// counts.
pub(crate) struct Applied<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
{
    pub snapshot: Snapshot<K, V, C, P>,
    /// What left the set, in key order (design §6.3); empty means no storage
    /// proposal on the plain path.
    pub evicted: Entries<K, V>,
    /// `evict`: the keys of `evicted` storage does not hold yet (the final
    /// proposal writes these and omits the rest); `retract`: keys a step
    /// wrote that stayed in the set (the final proposal puts their storage
    /// value back). Both in key order; the plain path ignores it.
    pub refinement: Prediction<K>,
    pub summary: AppliedSummary,
}
