//! Immutable snapshots of the recent-write set (design §4, §12.17).
//!
//! Each proposal builds a new sorted array of records from its parent's
//! records and the batch. Records own their keys but refer to value bytes
//! in shared arena chunks. Building a child clones keys and chunk pointers;
//! unchanged value bytes are shared unless the child compacts its arena.
//! New batch values are packed together rather than allocated per record.
//!
//! Each snapshot also owns its policy state and commitment. Once shared,
//! only local prediction marks may change, under the policy mutex. Value
//! reads search this snapshot directly without locking or visiting ancestors.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use crate::policy::{EvictionPolicy, Prediction};
use crate::timings::ProposeTimings;
use crate::types::{BatchOp, Digest, Error, Key, Result, Value};
use crate::vc::VectorCommitment;

/// A set entry by value: what leaves the set (`Entries`), what a step
/// carries and what a read returns. Deletes stay as tombstones until
/// evicted, when they become a `BatchOp::Delete` for storage. Tombstones
/// count toward capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry<V> {
    Value(V),
    Tombstone,
}

/// A set entry by reference, as the commitment schemes see it (design
/// §7.2): the value's bytes, or a tombstone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryRef<'a> {
    Value(&'a [u8]),
    Tombstone,
}

impl<V: Value> Entry<V> {
    pub fn as_ref(&self) -> EntryRef<'_> {
        match self {
            Entry::Value(value) => EntryRef::Value(value.as_ref()),
            Entry::Tombstone => EntryRef::Tombstone,
        }
    }
}

impl EntryRef<'_> {
    /// The entry by value: `Value::from_bytes` for a value (one allocation
    /// for a heap `V`).
    pub fn to_entry<V: Value>(self) -> Entry<V> {
        match self {
            EntryRef::Value(bytes) => Entry::Value(V::from_bytes(bytes)),
            EntryRef::Tombstone => Entry::Tombstone,
        }
    }
}

/// Entries in key order: the evicted entries of `Snapshot::apply` (design
/// §6.3); `speculate::entries_to_ops` turns them into a storage batch.
pub(crate) type Entries<K, V> = Vec<(K, Entry<V>)>;

/// A non-owning location within an arena: `chunk` indexes `Arena::chunks`,
/// and `offset..offset + len` selects bytes in that chunk. Copying or dropping
/// this descriptor does not change any reference count. Digests cover the
/// referenced bytes, not these storage coordinates.
#[derive(Clone, Copy, Debug)]
struct ValueRef {
    chunk: u32,
    offset: u32,
    len: u32,
}

#[derive(Clone, Copy, Debug)]
enum Slot {
    Value(ValueRef),
    Tombstone,
}

/// One entry of the array: the key and where its value is (a tombstone
/// has none).
#[derive(Clone, Debug)]
struct Record<K> {
    key: K,
    slot: Slot,
}

/// Value storage owned by one snapshot, with immutable chunks shared by `Arc`.
/// Cloning copies the chunk-pointer vector and increments one reference count
/// per chunk; it does not copy value bytes. Each batch containing a put adds
/// one chunk, until compaction replaces this arena's chunks with one new chunk.
///
/// Dropping this arena drops its chunk references. A chunk is freed only when
/// its last owning `Arc` is dropped, even if no record references its bytes.
/// Other snapshots can therefore keep old chunks alive after compaction.
#[derive(Clone)]
struct Arena {
    chunks: Vec<Arc<[u8]>>,
    /// Bytes referenced by this snapshot's records, maintained by `merge`.
    live: usize,
    /// Sum of chunk lengths, including bytes no longer referenced here.
    total: usize,
}

/// Chunks past which the arena is compacted whatever its dead bytes: a
/// child copies the chunk list, one reference count per chunk, so the list
/// is kept short for a set that grows by small blocks.
const MAX_CHUNKS: usize = 64;

impl Arena {
    fn empty() -> Self {
        Self {
            chunks: Vec::new(),
            live: 0,
            total: 0,
        }
    }

    fn bytes(&self, r: ValueRef) -> &[u8] {
        let start = r.offset as usize;
        &self.chunks[r.chunk as usize][start..start + r.len as usize]
    }

    fn entry(&self, slot: Slot) -> EntryRef<'_> {
        match slot {
            Slot::Value(r) => EntryRef::Value(self.bytes(r)),
            Slot::Tombstone => EntryRef::Tombstone,
        }
    }

    fn len(slot: Slot) -> usize {
        match slot {
            Slot::Value(r) => r.len as usize,
            Slot::Tombstone => 0,
        }
    }

    /// Return true when unreferenced bytes exceed live bytes, or this arena
    /// holds more than `MAX_CHUNKS` chunks. Call after updating `live`.
    fn wants_compaction(&self) -> bool {
        self.total > 2 * self.live || self.chunks.len() > MAX_CHUNKS
    }

    /// Compact this arena using `records`, the complete list of this snapshot's
    /// retained records. Every value slot must refer to this arena.
    ///
    /// Returns `()`. Copies those values into one new chunk in record order,
    /// rewrites their slots, leaves keys and tombstones unchanged, and sets
    /// `live` and `total` to the new chunk's length. Drops this arena's references
    /// to the old chunks; other snapshots' references and records are unchanged.
    fn compact<K: Key>(&mut self, records: &mut [Record<K>]) {
        let mut chunk: Vec<u8> = Vec::with_capacity(self.live);
        for record in records.iter_mut() {
            if let Slot::Value(r) = record.slot {
                let offset = offset_u32(chunk.len());
                chunk.extend_from_slice(self.bytes(r));
                record.slot = Slot::Value(ValueRef {
                    chunk: 0,
                    offset,
                    len: r.len,
                });
            }
        }
        self.total = chunk.len();
        self.live = chunk.len();
        self.chunks = vec![Arc::from(chunk)];
    }
}

/// The whole write set at one node (design §4): entries, policy state and
/// commitment at `height`. Immutable once built and shared by `Arc` through
/// its node (design §8); `Snapshot::apply` builds a child's by one merge
/// pass over this one with the batch applied. The one exception is the
/// policy's prediction marks, which hints write through `hint_writes`
/// (design §9.2).
pub struct Snapshot<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    /// In key order: canonical iteration, binary-search reads, no hasher.
    entries: Vec<Record<K>>,
    /// The entries' value bytes.
    arena: Arena,
    /// Policy state as of this node (design §6.1). A clone copies the marks
    /// and shares immutable v0 policy arrays until `writes` builds new ones.
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
    /// `vc.commitment(&commitment, entries)` as of this node, a root
    /// preimage term (design §7.3), computed once when the snapshot is
    /// built: the lattice's digest of its lanes, or a whole-set scheme's
    /// hash over every entry (design §7.2).
    commitment_digest: Digest,
    height: u64,
    /// Maximum number of entries, tombstones included (design §6.2): the
    /// capacity in force at this node, a consensus parameter today and
    /// per-node state once it adapts (R4, design §12.8); a root preimage
    /// term of the node (design §7.3, R23).
    capacity: usize,
    _value: PhantomData<fn() -> V>,
}

/// A byte offset or length as the arena's `u32`: a set or a block whose
/// values pass 4 GiB is refused rather than aliased.
fn offset_u32(n: usize) -> u32 {
    u32::try_from(n).expect("a block's or a set's value bytes fit in 4 GiB")
}

/// Borrow keys and values from `records` and their backing `arena` without
/// copying bytes. Every value slot must refer to `arena`. Returns an iterator
/// of `(&K, EntryRef)` in the input record order; it does not sort the records.
fn entry_refs<'a, K: Key>(
    records: &'a [Record<K>],
    arena: &'a Arena,
) -> impl ExactSizeIterator<Item = (&'a K, EntryRef<'a>)> {
    records
        .iter()
        .map(move |record| (&record.key, arena.entry(record.slot)))
}

impl<K, V, C, P> Snapshot<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    /// Build an empty initial set. Supply a fresh policy with no tracked keys.
    /// Entries, policy state, and commitment subsequently stay owned together.
    pub fn empty(vc: C, policy: P, capacity: usize) -> Result<Self> {
        Self::from_entries(vc, policy, capacity, std::iter::empty())
    }

    /// Build an initial set from entries using a fresh policy with no tracked
    /// keys. Repeated keys use their last entry, including tombstones. The
    /// policy and commitment are populated through the normal write path.
    /// A nonempty set has height 1, equivalent to one initial write batch;
    /// an empty set has height 0. No entries may be evicted during initialization.
    /// This initializes in-memory state, not a recovered database snapshot.
    pub fn from_entries(
        vc: C,
        policy: P,
        capacity: usize,
        entries: impl IntoIterator<Item = (K, Entry<V>)>,
    ) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::InvalidConfig("capacity must be at least 1".into()));
        }
        policy.validate(capacity)?;
        let entries: BTreeMap<K, Entry<V>> = entries.into_iter().collect();
        if entries.len() > capacity {
            return Err(Error::InvalidConfig("initial set exceeds capacity".into()));
        }
        let snapshot = Self::genesis(vc, policy, capacity);
        if entries.is_empty() {
            return Ok(snapshot);
        }
        let batch = entries
            .into_iter()
            .map(|(key, entry)| match entry {
                Entry::Value(value) => BatchOp::Put { key, value },
                Entry::Tombstone => BatchOp::Delete { key },
            })
            .collect();
        let applied = snapshot.apply(batch);
        if !applied.evicted.is_empty() {
            return Err(Error::InvalidConfig(
                "initial set policy must retain all entries".into(),
            ));
        }
        Ok(applied.snapshot)
    }

    /// Empty unchecked set for internal construction and contract tests.
    pub(crate) fn genesis(vc: C, policy: P, capacity: usize) -> Self {
        let entries: Vec<Record<K>> = Vec::new();
        let arena = Arena::empty();
        let commitment = vc.empty();
        let commitment_digest = vc.commitment(&commitment, entry_refs(&entries, &arena));
        let policy_digest = policy.state_digest();
        Self {
            entries,
            arena,
            policy: Mutex::new(policy),
            policy_digest,
            vc,
            commitment,
            commitment_digest,
            height: 0,
            capacity,
            _value: PhantomData,
        }
    }

    /// Validate an owned set against the writer's consensus configuration.
    pub(crate) fn validate(&self, capacity: usize) -> Result<()> {
        if self.capacity != capacity {
            return Err(Error::InvalidConfig(format!(
                "initial set capacity {} differs from configured capacity {capacity}",
                self.capacity
            )));
        }
        self.policy.lock().validate(capacity)
    }

    /// Root parameters come from the scheme and policy bound to this set.
    pub(crate) fn terms(&self, engine_id: &'static str) -> crate::root::Terms {
        let policy = self.policy.lock();
        crate::root::Terms {
            scheme_id: self.vc.scheme_id(),
            policy_name: policy.name(),
            engine_id,
            scheme_params: self.vc.params_digest(),
            policy_params: policy.params_digest(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

    /// Design §5 step 1: one binary search over the records, the entry
    /// rebuilt from its bytes.
    pub fn get(&self, key: &K) -> Option<Entry<V>> {
        self.entries
            .binary_search_by(|record| record.key.cmp(key))
            .ok()
            .map(|i| self.arena.entry(self.entries[i].slot).to_entry())
    }

    /// Set commitment, a root preimage term (design §7.3), computed when
    /// the snapshot was built.
    pub fn commitment(&self) -> Digest {
        self.commitment_digest
    }

    /// The policy's state digest, a root preimage term (design §7.3).
    pub fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// A prefetch hint on this node (design §9.2): `policy.hint_writes(keys,
    /// capacity, margin)` under the policy lock. Local; the consensus state
    /// is untouched.
    pub(crate) fn hint_writes(&self, keys: &[K], margin: usize) -> Prediction<K> {
        self.policy.lock().hint_writes(keys, self.capacity, margin)
    }

    /// The policy's current prediction (design §9.2, R31): the marked keys,
    /// whose entries the writer sends again when the chain moves to a new
    /// base. Under the policy lock.
    pub fn predicted(&self) -> Vec<K> {
        self.policy.lock().predicted()
    }

    /// The proposal took the plain path (design §9.2): the policy forgets
    /// its marks, since nothing marked is in this node's storage.
    pub(crate) fn reset_hints(&self) {
        self.policy.lock().reset_hints();
    }

    /// Build a child at `self.height + 1` from this parent and an owned `batch`
    /// of puts/deletes in execution order. Repeated keys are allowed; their
    /// last operation wins. The caller must reject prefix deletes beforehand.
    ///
    /// Returns `Applied`: the child snapshot (same capacity as the parent),
    /// evicted final entries sorted by key, the policy's prediction refinement,
    /// operation counts, and phase timings. The parent is left unchanged.
    ///
    /// Clones the arena's chunk pointers, policy, and commitment; normalises
    /// the batch; asks the cloned policy for victims; and builds the child's
    /// records with `merge`. Compacts the child's arena if needed, then computes
    /// its digests. Panics if the policy returns invalid victims or leaves the
    /// child over capacity (design §4, §6.2, §8).
    pub(crate) fn apply(&self, batch: Vec<BatchOp<K, V>>) -> Applied<K, V, C, P> {
        let mut timings = ProposeTimings::default();
        let mark = Instant::now();
        let mut arena = self.arena.clone();
        let mut policy = self.policy.lock().clone();
        let vc = self.vc.clone();
        let mut commitment = self.commitment.clone();
        timings.copy = mark.elapsed();

        let mark = Instant::now();
        let height = self.height + 1;
        let capacity = self.capacity;
        let (keys, ops, mut summary) = normalise(batch, &mut arena);
        timings.ops = mark.elapsed();

        let mark = Instant::now();
        let eviction = policy.writes(&keys, height, capacity);
        timings.writes = mark.elapsed();

        let mark = Instant::now();
        let (mut entries, evicted) = merge(
            &self.entries,
            &ops,
            &eviction.all_evicted_keys,
            &mut arena,
            &vc,
            &mut commitment,
            &mut summary,
        );
        assert!(
            entries.len() <= capacity,
            "the eviction policy left the set over capacity: {} entries for {capacity} (design §6.2)",
            entries.len()
        );
        if arena.wants_compaction() {
            arena.compact(&mut entries);
        }
        timings.merge = mark.elapsed();

        let mark = Instant::now();
        let policy_digest = policy.state_digest();
        timings.digest = mark.elapsed();
        let mark = Instant::now();
        let commitment_digest = vc.commitment(&commitment, entry_refs(&entries, &arena));
        timings.commitment = mark.elapsed();
        Applied {
            snapshot: Snapshot {
                entries,
                arena,
                policy: Mutex::new(policy),
                policy_digest,
                vc,
                commitment,
                commitment_digest,
                height,
                capacity,
                _value: PhantomData,
            },
            evicted,
            refinement: eviction.refinement,
            summary,
            timings,
        }
    }
}

/// A batch operation represented by an arena reference or tombstone.
/// During collection each `Op` represents one input operation; after
/// deduplication it represents all operations for one key.
struct Op<K> {
    key: K,
    /// The last operation's result after deduplication.
    slot: Slot,
    /// Whether the first operation for this key was a put. Used only to count
    /// in-place updates when `merge` checks whether the parent contains the key.
    first_op_is_put: bool,
}

/// Prepare an owned `batch` of puts/deletes in execution order for `merge`.
/// `arena` is the child's clone of the parent's arena; existing references
/// must keep their chunk indices. Prefix deletes must already be rejected.
///
/// Returns `(keys, ops, summary)`:
/// - `keys`: one key per input operation, in original order, including
///   duplicates and deletes. Passed to the eviction policy.
/// - `ops`: one operation per distinct key, sorted by key. Its slot contains
///   the last operation's result; its flag describes the first operation.
/// - `summary`: counts every put and every put preceded by another operation
///   on that key in this batch. `merge` counts initial puts to existing keys.
///
/// Copies all put bytes into one new arena chunk, including superseded values,
/// and increases `arena.total`. Adds no chunk if there are no puts. Leaves
/// `arena.live` unchanged: `merge` updates it once retained records are known.
fn normalise<K: Key, V: Value>(batch: Vec<BatchOp<K, V>>, arena: &mut Arena) -> (Vec<K>, Vec<Op<K>>, AppliedSummary) {
    let mut summary = AppliedSummary::default();
    let mut keys: Vec<K> = Vec::with_capacity(batch.len());
    let mut ops: Vec<Op<K>> = Vec::with_capacity(batch.len());
    let chunk_index = offset_u32(arena.chunks.len());
    let mut chunk: Vec<u8> = Vec::new();
    let mut had_put = false;
    for op in batch {
        match op {
            BatchOp::Put { key, value } => {
                summary.puts += 1;
                had_put = true;
                let bytes = value.as_ref();
                let slot = Slot::Value(ValueRef {
                    chunk: chunk_index,
                    offset: offset_u32(chunk.len()),
                    len: offset_u32(bytes.len()),
                });
                chunk.extend_from_slice(bytes);
                keys.push(key.clone());
                ops.push(Op {
                    key,
                    slot,
                    first_op_is_put: true,
                });
            }
            BatchOp::Delete { key } => {
                keys.push(key.clone());
                ops.push(Op {
                    key,
                    slot: Slot::Tombstone,
                    first_op_is_put: false,
                });
            }
            BatchOp::PrefixDelete { .. } => unreachable!("propose rejects PrefixDelete before apply (design §4)"),
        }
    }
    if had_put {
        arena.total += chunk.len();
        arena.chunks.push(Arc::from(chunk));
    }
    // Stable sorting preserves execution order among operations on the same key.
    ops.sort_by(|a, b| a.key.cmp(&b.key));
    let mut deduped: Vec<Op<K>> = Vec::with_capacity(ops.len());
    for op in ops {
        match deduped.last_mut() {
            Some(last) if last.key == op.key => {
                if op.first_op_is_put {
                    summary.in_place_updates += 1;
                }
                // Replace the result but preserve the first operation's flag.
                last.slot = op.slot;
            }
            _ => deduped.push(op),
        }
    }
    (keys, deduped, summary)
}

/// Apply normalised operations and evictions in one pass over sorted keys.
///
/// Inputs:
/// - `parent`: the parent's records, sorted by distinct key.
/// - `ops`: `normalise`'s final operations, sorted by distinct key. Deletes
///   are tombstones, which remain records unless selected for eviction.
/// - `victims`: distinct keys to evict, sorted by key. Each must occur in
///   `parent` or `ops`; otherwise this function panics.
/// - `arena`: the child's arena after `normalise`. It must resolve all slots
///   in `parent` and `ops`, with `live` still equal to the parent's live bytes.
/// - `vc`: the commitment scheme used by the parent and child.
/// - `commitment`: a mutable clone of the parent's commitment state.
/// - `summary`: the counts returned by `normalise`.
///
/// Returns `(entries, evicted)`, both sorted by distinct key. `entries` holds
/// the child's retained records, reusing arena references. `evicted` holds
/// owned final entries for storage: a batch operation overrides the parent
/// even when that key is evicted. The input slices remain unchanged.
///
/// Updates `arena.live` and `commitment` to describe only retained records,
/// and adds initial puts to parent-present keys to `summary.in_place_updates`.
/// A key inserted and evicted in this batch never enters the commitment.
/// Does not compact the arena or enforce the capacity limit; `apply` does.
fn merge<K, V, C>(
    parent: &[Record<K>],
    ops: &[Op<K>],
    victims: &[K],
    arena: &mut Arena,
    vc: &C,
    commitment: &mut C::State,
    summary: &mut AppliedSummary,
) -> (Vec<Record<K>>, Entries<K, V>)
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
{
    let mut entries: Vec<Record<K>> = Vec::with_capacity(parent.len() + ops.len());
    let mut evicted: Entries<K, V> = Vec::with_capacity(victims.len());
    let (mut parent_index, mut op_index, mut victim_index) = (0usize, 0usize, 0usize);
    while parent_index < parent.len() || op_index < ops.len() {
        // Take the smaller next key. Equal keys consume both inputs so the batch wins.
        let take_batch_op =
            op_index < ops.len() && (parent_index >= parent.len() || ops[op_index].key <= parent[parent_index].key);
        let (key, parent_slot, batch_slot, first_op_is_put) = if take_batch_op {
            let op = &ops[op_index];
            op_index += 1;
            let parent_slot = if parent_index < parent.len() && parent[parent_index].key == op.key {
                parent_index += 1;
                Some(parent[parent_index - 1].slot)
            } else {
                None
            };
            (&op.key, parent_slot, Some(op.slot), op.first_op_is_put)
        } else {
            let record = &parent[parent_index];
            parent_index += 1;
            (&record.key, Some(record.slot), None, false)
        };
        if first_op_is_put && parent_slot.is_some() {
            summary.in_place_updates += 1;
        }

        if victim_index < victims.len() && victims[victim_index] < *key {
            unreachable!("the eviction policy named a key the set lacks, or one twice (design §6.1 invariant)");
        }
        // Omit this key from the child; return its final value or tombstone for storage.
        if victim_index < victims.len() && victims[victim_index] == *key {
            victim_index += 1;
            if let Some(parent_slot) = parent_slot {
                vc.apply(commitment, key, Some(arena.entry(parent_slot)), None);
                arena.live -= Arena::len(parent_slot);
            }
            let final_slot = batch_slot
                .or(parent_slot)
                .expect("a merged key comes from the parent or the batch");
            evicted.push((key.clone(), arena.entry(final_slot).to_entry()));
            continue;
        }
        // Retain the batch result if supplied, otherwise retain the parent slot.
        let slot = match (parent_slot, batch_slot) {
            (Some(parent_slot), Some(batch_slot)) => {
                vc.apply(
                    commitment,
                    key,
                    Some(arena.entry(parent_slot)),
                    Some(arena.entry(batch_slot)),
                );
                arena.live = arena.live + Arena::len(batch_slot) - Arena::len(parent_slot);
                batch_slot
            }
            (None, Some(batch_slot)) => {
                vc.apply(commitment, key, None, Some(arena.entry(batch_slot)));
                arena.live += Arena::len(batch_slot);
                batch_slot
            }
            (Some(parent_slot), None) => parent_slot,
            (None, None) => unreachable!("a merged key comes from the parent or the batch"),
        };
        entries.push(Record { key: key.clone(), slot });
    }
    if victim_index != victims.len() {
        unreachable!("the eviction policy named a key the set lacks (design §6.1 invariant)");
    }
    (entries, evicted)
}

/// Counts of one batch application, consumed by `propose` for the
/// statistics (design §9.5, §11).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AppliedSummary {
    pub puts: usize,
    /// Puts of a key present at the time of the op (a key put twice in one
    /// batch counts here the second time; a present key the block rewrites
    /// and then evicts counts too).
    pub in_place_updates: usize,
}

/// Result of `Snapshot::apply`: the child's snapshot, what left the set,
/// the policy's last refinement of its prediction (design §9.1), and the
/// counts.
pub(crate) struct Applied<K, V, C, P>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    pub snapshot: Snapshot<K, V, C, P>,
    /// What left the set, in key order (design §6.3); empty means no storage
    /// proposal on the plain path.
    pub evicted: Entries<K, V>,
    /// The policy's last refinement of its prediction (`Prediction`, design
    /// §9.1), every list in key order; the plain path ignores it.
    pub refinement: Prediction<K>,
    pub summary: AppliedSummary,
    /// The phases' durations so far (design §9.5); `propose` fills the rest.
    pub timings: ProposeTimings,
}

#[cfg(test)]
mod tests {
    //! The commitment maintained by the merge pass against a fresh one over
    //! the final entries (an oracle independent of `apply`'s bookkeeping),
    //! and the policy contract `apply` enforces.

    use super::*;
    use crate::policy::{Eviction, Fifo, LruWrite, Prediction};
    use crate::tests::Rng;
    use crate::vc::lthash::LtHash;
    use crate::vc::merkle::Merkle;
    use crate::vc::rehash::Rehash;

    type K = Vec<u8>;
    type V = Vec<u8>;

    fn fresh_digest<C, P>(vc: &C, snapshot: &Snapshot<K, V, C, P>) -> Digest
    where
        C: VectorCommitment<K>,
        P: EvictionPolicy<K>,
    {
        let mut state = vc.empty();
        for (key, entry) in entry_refs(&snapshot.entries, &snapshot.arena) {
            vc.apply(&mut state, key, None, Some(entry));
        }
        vc.commitment(&state, entry_refs(&snapshot.entries, &snapshot.arena))
    }

    /// Random blocks (deletes, empty values, repeated keys, evictions,
    /// compactions) against the oracle, the key order and the arena's live
    /// count.
    fn incremental_matches_fresh<C, P>(vc: C, policy: P, seed: u64)
    where
        C: VectorCommitment<K>,
        P: EvictionPolicy<K>,
    {
        let capacity = 16;
        let mut snapshot = Snapshot::genesis(vc.clone(), policy, capacity);
        let mut rng = Rng(seed);
        let mut evicted = 0;
        for block in 0..300 {
            let n = rng.below(14) as usize;
            let batch: Vec<BatchOp<K, V>> = (0..n)
                .map(|_| {
                    let key = format!("k{:02}", rng.below(40)).into_bytes();
                    match rng.below(5) {
                        0 => BatchOp::Delete { key },
                        1 => BatchOp::Put { key, value: Vec::new() },
                        _ => BatchOp::Put {
                            key,
                            value: format!("v{}", rng.below(100)).into_bytes(),
                        },
                    }
                })
                .collect();
            let applied = snapshot.apply(batch);
            evicted += applied.evicted.len();
            snapshot = applied.snapshot;
            assert!(
                snapshot.entries.windows(2).all(|w| w[0].key < w[1].key),
                "block {block}: keys out of order or repeated"
            );
            assert!(snapshot.entries.len() <= capacity);
            let live: usize = snapshot.entries.iter().map(|r| Arena::len(r.slot)).sum();
            assert_eq!(snapshot.arena.live, live, "block {block}: arena.live drifted");
            assert_eq!(snapshot.commitment(), fresh_digest(&vc, &snapshot), "block {block}");
        }
        assert!(evicted > 0);
    }

    #[test]
    fn lthash_incremental_matches_fresh() {
        for seed in 1..6 {
            incremental_matches_fresh(LtHash::new(), Fifo::<K>::new(8), seed);
            incremental_matches_fresh(LtHash::new(), LruWrite::<K>::new(8), seed);
            incremental_matches_fresh(LtHash::new(), Fifo::<K>::new(14).with_trigger(14), seed);
            incremental_matches_fresh(LtHash::new(), LruWrite::<K>::new(14).with_trigger(14), seed);
        }
    }

    #[test]
    fn merkle_incremental_matches_fresh() {
        for seed in 1..4 {
            incremental_matches_fresh(Merkle::new(), Fifo::<K>::new(8), seed);
            incremental_matches_fresh(Merkle::new(), LruWrite::<K>::new(14).with_trigger(14), seed);
        }
    }

    #[test]
    fn rehash_matches_fresh() {
        incremental_matches_fresh(Rehash::new(), LruWrite::<K>::new(8), 3);
    }

    /// A policy that names whatever victims it is told to.
    #[derive(Clone)]
    struct BadPolicy {
        victims: Vec<K>,
    }

    impl EvictionPolicy<K> for BadPolicy {
        fn writes(&mut self, _keys: &[K], _height: u64, _capacity: usize) -> Eviction<K> {
            Eviction {
                all_evicted_keys: self.victims.clone(),
                refinement: Prediction {
                    evict: Vec::new(),
                    retract: Vec::new(),
                },
            }
        }
        fn hint_writes(&mut self, _: &[K], _: usize, _: usize) -> Prediction<K> {
            unreachable!()
        }
        fn predicted(&self) -> Vec<K> {
            Vec::new()
        }
        fn reset_hints(&mut self) {}
        fn state_digest(&self) -> Digest {
            [0; 32]
        }
        fn name(&self) -> &'static str {
            "bad"
        }
        fn params_digest(&self) -> Digest {
            [0; 32]
        }
    }

    /// Design §6.1 invariant: a victim the set lacks, wherever it sorts, and
    /// a victim named twice halt `apply`.
    #[test]
    fn a_policy_naming_a_key_the_set_lacks_halts() {
        let put = |key: &str| BatchOp::Put {
            key: key.as_bytes().to_vec(),
            value: b"1".to_vec(),
        };
        let genesis = Snapshot::<K, V, LtHash, BadPolicy>::genesis(LtHash::new(), BadPolicy { victims: Vec::new() }, 4);
        let snapshot = genesis.apply(vec![put("b")]).snapshot;
        let halts = |victims: &[&str], batch: Vec<BatchOp<K, V>>| {
            *snapshot.policy.lock() = BadPolicy {
                victims: victims.iter().map(|k| k.as_bytes().to_vec()).collect(),
            };
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| snapshot.apply(batch))).is_err()
        };
        assert!(halts(&["a"], Vec::new()), "a victim below every key");
        assert!(halts(&["b", "b"], vec![put("c")]), "a victim named twice");
        assert!(halts(&["z"], Vec::new()), "a victim above every key");
        assert!(!halts(&["b"], vec![put("c")]), "a victim the set holds");
    }
}
