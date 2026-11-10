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
//! `Prediction`: what still has to be sent and what must be put back. A
//! prediction can outlive the block (R31): retained marks survive unless the
//! policy cancels them (LRU cancels rewritten keys). The writer sends marked
//! entries again when starting the next chain (`predicted`).
//! `state_digest` covers the consensus state only and is a root term
//! (design §7.3), so equal roots imply equal eviction order; the marks never
//! reach a root. Hints mutate the policy inside the target node's snapshot
//! (design §4, R26), the one thing in a snapshot that changes after it is
//! built.
//!
//! How many victims an eviction removes is the policy's rule inside
//! `writes`: the capacity is the snapshot's and is passed in (design §4,
//! R23). The v0 policies evict down to a consensus low watermark only
//! when capacity is exceeded. A separate local high watermark starts
//! prediction of a full capacity-to-low batch; it never changes eviction.

pub mod fifo;
pub mod lru_write;

pub use fifo::Fifo;
pub use lru_write::LruWrite;
use sha2::{Digest as _, Sha256};

use crate::types::{Digest, Error, Key, Result, len_prefix};

/// Return SHA-256 of `"right_cash/policy-params"` followed by `low_watermark`
/// encoded as a little-endian `u64`. The input is the number of entries to
/// retain after eviction; this digest excludes local prediction settings.
/// The high watermark only schedules precomputation and is not hashed.
pub(crate) fn params_digest(low_watermark: usize) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"right_cash/policy-params");
    hasher.update((low_watermark as u64).to_le_bytes());
    hasher.finalize().into()
}

/// Snapshot encoding shared by FIFO and LRU: one u64 LE height for each
/// snapshot key. Keys are stored once, in the snapshot's entry section.
fn encode_heights<K: Key>(index: &[(K, u64)], keys: &[K]) -> Result<Vec<u8>> {
    if index.len() != keys.len() || index.iter().zip(keys).any(|((key, _), expected)| key != expected) {
        return Err(Error::InvalidSnapshot("policy and snapshot key sets differ".into()));
    }
    Ok(index.iter().flat_map(|(_, height)| height.to_le_bytes()).collect())
}

/// Decode and validate heights, returning the key-sorted index. Callers
/// reconstruct eviction order by sorting (height, key) pairs.
fn decode_heights<K: Key>(bytes: &[u8], keys: &[K], height: u64) -> Result<Vec<(K, u64)>> {
    if bytes.len() / 8 != keys.len() || !bytes.len().is_multiple_of(8) {
        return Err(Error::InvalidSnapshot(
            "policy height count differs from entry count".into(),
        ));
    }
    keys.iter()
        .zip(bytes.chunks_exact(8))
        .map(|(key, bytes)| {
            let written = u64::from_le_bytes(bytes.try_into().expect("eight-byte chunk"));
            if written == 0 || written > height {
                return Err(Error::InvalidSnapshot(
                    "policy height is outside the snapshot's history".into(),
                ));
            }
            Ok((key.clone(), written))
        })
        .collect()
}

/// Check entry-count parameters without mutating them: `low_watermark` is the
/// target occupancy after eviction, `trigger` is an optional prediction-start
/// occupancy, and `capacity` is the maximum retained entry count.
/// Return `Ok(())` when `low_watermark <= capacity` and any explicit trigger
/// is within `[low_watermark, capacity]`; otherwise return `InvalidConfig`.
pub(crate) fn validate_watermarks(low_watermark: usize, trigger: Option<usize>, capacity: usize) -> Result<()> {
    if low_watermark > capacity {
        return Err(Error::InvalidConfig(format!(
            "the low watermark ({low_watermark}) is above the capacity ({capacity})"
        )));
    }
    if let Some(trigger) = trigger
        && (trigger < low_watermark || trigger > capacity)
    {
        return Err(Error::InvalidConfig(format!(
            "the speculation high watermark ({trigger}) must be between the low watermark ({low_watermark}) and capacity ({capacity})"
        )));
    }
    Ok(())
}

/// Domain of [`order_digest`].
const STATE_DOMAIN: &str = "right_cash/policy-state";

/// Return the consensus-state digest for policy identifier `name` and its
/// eviction `order`, an iterator of borrowed `(height, key)` pairs in eviction
/// order. Consumes the iterator without changing the pairs or sorting them.
///
/// Encoding (design §7.3): BLAKE3 `derive_key`
/// under `STATE_DOMAIN` over the policy's name (`u32 LE` length-prefixed)
/// and its eviction order as `(height u64 LE, u32 LE key length, key bytes)`
/// in order, one pass over the state per call (a full hash per proposal,
/// R25). The order is serialised into one buffer and hashed with one
/// `update` (design §12.17 (b)): BLAKE3 runs near memory speed over a large
/// input and at call overhead over tens of thousands of small ones. An
/// incremental accumulator would cost more at 10k (two items per written
/// key). Shared by both policies; a policy with another order structure
/// writes its own.
pub(crate) fn order_digest<'a, K: Key + 'a>(name: &str, order: impl ExactSizeIterator<Item = &'a (u64, K)>) -> Digest {
    let mut buf = Vec::with_capacity(4 + name.len() + order.len() * (8 + 4 + 32));
    buf.extend_from_slice(&len_prefix(name.len()));
    buf.extend_from_slice(name.as_bytes());
    for (height, key) in order {
        let bytes = key.as_ref();
        buf.extend_from_slice(&height.to_le_bytes());
        buf.extend_from_slice(&len_prefix(bytes.len()));
        buf.extend_from_slice(bytes);
    }
    let mut hasher = blake3::Hasher::new_derive_key(STATE_DOMAIN);
    hasher.update(&buf);
    *hasher.finalize().as_bytes()
}

/// Return a new eviction order: `old_order` minus `removed_entries`, followed
/// by `appended_entries`. Inputs are borrowed and left unchanged. This helper
/// repositions keys; the caller selects and removes eviction victims afterward.
///
/// - `old_order`: one `(height, key)` pair per tracked key, sorted by the
///   tuple (oldest height first, then key to break ties).
/// - `removed_entries`: distinct pairs from `old_order`, in the same tuple
///   order. LRU-write supplies the old positions of rewritten keys; FIFO
///   supplies an empty slice because rewrites keep their positions.
/// - `appended_entries`: one pair per new or repositioned key, sorted by key,
///   all at the current block's height. That height must exceed every old
///   height, and these keys must not also occur in the retained old entries.
///
/// The result has one pair per key and remains sorted by `(height, key)`.
/// A scan skips removed pairs, then appends the new pairs without sorting.
/// A debug assertion checks that all removals were found; a runtime assertion
/// checks the boundary between retained and appended entries (design §12.17).
pub(crate) fn split_order<K: Key>(
    old_order: &[(u64, K)],
    removed_entries: &[(u64, K)],
    appended_entries: &[(u64, K)],
) -> Vec<(u64, K)> {
    let mut out: Vec<(u64, K)> = Vec::with_capacity(old_order.len() - removed_entries.len() + appended_entries.len());
    let mut removed_index = 0usize;
    for pair in old_order {
        if removed_index < removed_entries.len() && removed_entries[removed_index] == *pair {
            removed_index += 1;
            continue;
        }
        out.push(pair.clone());
    }
    debug_assert!(
        removed_index == removed_entries.len(),
        "a moved key was not in the order (design §6.1 invariant)"
    );
    let in_order = match (out.last(), appended_entries.first()) {
        (Some(last), Some(first)) => *last < *first,
        _ => true,
    };
    assert!(
        in_order,
        "a block's height is not above the order's (design §6.1 invariant)"
    );
    out.extend_from_slice(appended_entries);
    out
}

/// Return the updated key-to-height index, sorted by key with no duplicates.
/// All inputs are borrowed and left unchanged:
///
/// - `index`: the parent's `(key, height)` pairs, sorted by distinct key.
/// - `heights`: `(key, new_height)` pairs to insert or replace, sorted by
///   distinct key. FIFO supplies new keys; LRU-write supplies all written keys.
/// - `victims`: distinct evicted keys, sorted by key. These keys are omitted
///   even if they also appear in `heights`; absent keys have no effect.
///
/// Heights come from `heights` when a key occurs in both inputs, otherwise
/// from `index`. This updates the lookup index, not the eviction order.
pub(crate) fn merge_index<K: Key>(index: &[(K, u64)], heights: &[(K, u64)], victims: &[K]) -> Vec<(K, u64)> {
    let mut out: Vec<(K, u64)> = Vec::with_capacity(index.len() + heights.len());
    let (mut index_position, mut update_position, mut victim_position) = (0usize, 0usize, 0usize);
    while index_position < index.len() || update_position < heights.len() {
        let take_update = update_position < heights.len()
            && (index_position >= index.len() || heights[update_position].0 <= index[index_position].0);
        let (key, height) = if take_update {
            let (key, height) = &heights[update_position];
            update_position += 1;
            if index_position < index.len() && index[index_position].0 == *key {
                index_position += 1;
            }
            (key, *height)
        } else {
            let (key, height) = &index[index_position];
            index_position += 1;
            (key, *height)
        };
        while victim_position < victims.len() && victims[victim_position] < *key {
            victim_position += 1;
        }
        if victim_position < victims.len() && victims[victim_position] == *key {
            victim_position += 1;
            continue;
        }
        out.push((key.clone(), height));
    }
    out
}

/// Storage work needed to maintain or finalize precomputation. Contains keys
/// only: `evict` sends current snapshot entries (Put or Delete for a tombstone),
/// and `retract` restores storage values from before precomputation.
/// Neither list decides which keys leave the write set; `Eviction::all_evicted_keys`
/// is the complete actual eviction decision.
///
/// The caller chooses how to apply the lists from the context:
/// - `hint_writes`: queue both lists for the worker; all evictions are predictions.
/// - `writes` with no actual evictions: queue both lists if keeping the chain.
///   FIFO resends rewritten predictions; LRU cancels rewritten predictions.
/// - `writes` with actual evictions: apply both lists in the final storage
///   proposal on a usable chain. Otherwise ignore them and write all victims
///   through the plain path.
///
/// Policies manage their own marks before returning. A restoration may cancel
/// a prediction (LRU rewrite) or leave it marked for a later block (a retained
/// prediction restored during finalization). The caller does no unmarking.
/// Built-in policies return distinct keys within each list; hint evictions are
/// in eviction order, and retractions and both `writes` lists are in key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prediction<K> {
    /// Entries to send or resend to storage. From hints, these are newly marked
    /// candidates. From `writes` with no actual evictions, these are retained
    /// FIFO predictions rewritten by the batch; read their values from the child
    /// snapshot. With actual evictions, these are the subset of `all_evicted_keys`
    /// whose final entries precomputation cannot supply: unmarked or rewritten
    /// victims. Read their values from the collected evicted entries.
    /// Rewrites are resent even if the bytes are unchanged.
    pub evict: Vec<K>,
    /// Speculative writes to undo: restore the chain base's storage value (Put),
    /// or Delete if absent. This does not remove entries from the snapshot.
    /// From hints or a non-evicting `writes`, these are canceled LRU predictions.
    /// From an evicting `writes`, these are all predicted keys retained in the
    /// snapshot; the final proposal reads their old storage values through the
    /// parent's view. Policy marks have already been updated for either case.
    pub retract: Vec<K>,
}

/// A batch's actual eviction decision and the storage work needed to reconcile
/// precomputation. Returned by `EvictionPolicy::writes`. Only `all_evicted_keys`
/// affects consensus; `refinement` must realize the same final state as writing
/// every actual victim through the plain path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Eviction<K> {
    /// Every key leaving the bounded write set, including keys already sent
    /// through precomputation. Distinct and sorted by key, identical with or
    /// without hints. Each key must exist in the parent or be written by this
    /// batch. The policy has already removed these keys from its own state;
    /// `merge` removes their records and collects their final entries for storage.
    /// Evicting a value moves it to storage; a tombstone becomes a storage delete.
    pub all_evicted_keys: Vec<K>,
    /// Storage operations for this outcome. When `all_evicted_keys` is empty,
    /// maintain a kept chain: resend rewritten FIFO predictions or restore
    /// canceled LRU predictions. Otherwise finalize a chain: write missing/stale
    /// actual victims and restore predictions that stayed in the snapshot.
    /// Thus `refinement.evict` is a subset of `all_evicted_keys` only when actual
    /// eviction occurs. Ignored on the plain path; the policy owns unmarking.
    pub refinement: Prediction<K>,
}

/// `Clone`: a proposal gets independent policy state (design §4). The v0
/// policies share immutable input arrays when cloned, then build the child's
/// arrays once in `writes` (design §12.17 (a)); prediction marks are copied.
pub trait EvictionPolicy<K: Key>: Clone + Send + Sync + 'static {
    /// Encode consensus state for clean-shutdown recovery. `keys` is the
    /// snapshot's complete key set in sorted order. Exclude prediction state.
    /// Custom policies must implement this and `restore_state` to enable
    /// `Local::snapshot_path`; in-memory operation needs neither method.
    fn encode_state(&self, _keys: &[K]) -> Result<Vec<u8>> {
        Err(Error::InvalidConfig(format!(
            "policy {} does not support snapshots",
            self.name()
        )))
    }

    /// Replace consensus state from `encode_state`, retaining this instance's
    /// parameters. Validate that the state describes exactly `keys` at `height`.
    /// Clear all hints and predictions. The caller separately checks parameters
    /// and recomputes the combined root before exposing the recovered instance.
    fn restore_state(&mut self, _bytes: &[u8], _keys: &[K], _height: u64) -> Result<()> {
        Err(Error::InvalidConfig(format!(
            "policy {} does not support snapshots",
            self.name()
        )))
    }

    /// Update policy state for one batch and return its eviction decision.
    ///
    /// - `keys`: one key for each put or delete, in batch execution order,
    ///   including repeated keys and writes to existing keys. A delete leaves
    ///   a tombstone, so its key is tracked until eviction.
    /// - `height`: the new block's height, greater than the parent's height.
    /// - `capacity`: the maximum number of retained keys, including tombstones.
    ///
    /// Returns `Eviction`: `all_evicted_keys` contains distinct keys sorted by key;
    /// `refinement` describes storage work needed to reconcile predictions with
    /// this decision. Every victim must be a parent key or occur in `keys`,
    /// and removing the victims must leave the set within capacity.
    ///
    /// Updates the tracked keys and their eviction order, removes victims from
    /// policy state, and reconciles local marks. Victims depend only on the
    /// consensus state and these inputs, never on prediction marks. Victims'
    /// marks are removed. Retained FIFO predictions survive; LRU also cancels
    /// retained predictions rewritten by the batch. Return restoration ops
    /// before discarding the information needed to undo their storage writes.
    fn writes(&mut self, keys: &[K], height: u64, capacity: usize) -> Eviction<K>;

    /// Predict storage work from expected writes, without changing consensus
    /// state. `keys` are expected written keys (duplicates and repeated hints
    /// allowed), `capacity` is the entry limit, and `margin` is an extra number
    /// of candidates requested beyond the policy's usual prediction size.
    ///
    /// Returns a `Prediction`: newly marked candidates in
    /// `evict`, and canceled candidates in `retract`.
    /// These are provisional, not actual evictions. The policy updates its
    /// marks itself; the caller only queues the returned storage operations.
    ///
    /// May update local marks and hint bookkeeping only. The built-in policies
    /// count distinct hinted insertions until the high watermark starts a fixed
    /// capacity-minus-low window (plus margin). FIFO keeps that prefix; LRU
    /// replaces hinted candidates by advancing through the queue. Results must
    /// be deterministic for the same
    /// state and hint sequence (design §9.2, §9.3).
    fn hint_writes(&mut self, keys: &[K], capacity: usize, margin: usize) -> Prediction<K>;

    /// The keys currently marked, in key order (design §9.2, R31): what the
    /// precomputation's storage holds or is about to; the writer sends their
    /// entries again when the chain moves to a new base. Local.
    fn predicted(&self) -> Vec<K>;

    /// Forgets every mark (design §9.2): the proposal took the plain path,
    /// so nothing marked is in its storage. Consensus state untouched.
    fn reset_hints(&mut self);

    /// The policy's own parameters against the set's `capacity` (design
    /// §6.2), checked once by `RightCash::new`: `InvalidConfig` for a rule
    /// that could leave the set over capacity (the v0 policies: a low
    /// watermark above capacity, or a high watermark outside [low, capacity]).
    /// Nothing to check by default.
    fn validate(&self, _capacity: usize) -> Result<()> {
        Ok(())
    }

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
    /// policies digest only their low watermark). Local prediction settings
    /// must never enter this digest.
    fn params_digest(&self) -> Digest;
}
