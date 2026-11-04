//! Milestone-1 and milestone-2 tests (design §6.1, §9.3, §13): the policies
//! on their own (the fuzz test of R32 included), the plain path
//! end to end on `MemoryStorage` for every policy and scheme, speculation
//! against the plain path, and smoke tests on firewood behind its feature.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::policy::{EvictionPolicy, Fifo, LruWrite, Prediction};
use crate::set::Entry;
use crate::storage::{MemoryConfig, MemoryStorage, StorageEngine, StorageProposal};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash};
use crate::vc::VectorCommitment;
use crate::vc::lthash::LtHash;
use crate::vc::merkle::Merkle;
use crate::vc::rehash::Rehash;
use crate::{AuthWal, Config, Consensus, Local};

type K = Vec<u8>;
type V = Vec<u8>;

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

// ---- policies (design §6.1) ----

#[test]
fn fifo_keeps_a_rewritten_key_in_place_and_evicts_the_oldest() {
    let mut p = Fifo::<K>::new(2);
    assert!(p.writes(&keys(&["a", "b"]), 1, 3).all_evicted_keys.is_empty());
    // `a` rewritten at height 2 keeps its place; `c` is new.
    assert!(p.writes(&keys(&["a", "c"]), 2, 3).all_evicted_keys.is_empty());
    // Four entries over a capacity of three: down to the watermark, oldest first.
    let ev = p.writes(&keys(&["d"]), 3, 3);
    assert_eq!(ev.all_evicted_keys, keys(&["a", "b"]));
    assert_eq!(ev.refinement.evict, keys(&["a", "b"]));
    assert!(ev.refinement.retract.is_empty());
}

#[test]
fn lru_write_moves_a_rewritten_key_to_the_back() {
    let mut p = LruWrite::<K>::new(2);
    assert!(p.writes(&keys(&["a", "b"]), 1, 3).all_evicted_keys.is_empty());
    assert!(p.writes(&keys(&["a", "c"]), 2, 3).all_evicted_keys.is_empty());
    // Order: b@1, a@2, c@2, d@3; the two oldest go, returned sorted by key.
    let ev = p.writes(&keys(&["d"]), 3, 3);
    assert_eq!(ev.all_evicted_keys, keys(&["a", "b"]));
}

#[test]
fn height_ties_break_by_key_bytes() {
    let mut p = Fifo::<K>::new(1);
    let ev = p.writes(&keys(&["z", "y", "x"]), 1, 2);
    assert_eq!(ev.all_evicted_keys, keys(&["x", "y"]));
}

#[test]
fn the_same_batch_sequence_yields_the_same_digest_whatever_the_hints() {
    let mut plain = LruWrite::<K>::new(2);
    let mut hinted = LruWrite::<K>::new(2);
    for (height, batch) in [keys(&["a", "b"]), keys(&["c", "a"]), keys(&["d", "e"])]
        .into_iter()
        .enumerate()
    {
        let height = height as u64 + 1;
        hinted.hint_writes(&batch, 3, 1);
        hinted.hint_writes(&keys(&["zzz"]), 3, 0);
        let e1 = plain.writes(&batch, height, 3);
        let e2 = hinted.writes(&batch, height, 3);
        assert_eq!(e1.all_evicted_keys, e2.all_evicted_keys);
        assert_eq!(plain.state_digest(), hinted.state_digest());
    }
}

#[test]
fn fifo_refinement_resends_rewrites_only_when_keeping_the_chain() {
    let mut parent = Fifo::<K>::new(4);
    parent.writes(&keys(&["a", "b", "c", "d", "e", "f"]), 1, 6);
    assert_eq!(parent.hint_writes(&[], 6, 3).evict, keys(&["a", "b", "c", "d", "e"]));

    let mut kept = parent.clone();
    let result = kept.writes(&keys(&["b", "b"]), 2, 6);
    assert!(result.all_evicted_keys.is_empty());
    assert_eq!(result.refinement.evict, keys(&["b"]));
    assert!(result.refinement.retract.is_empty());
    assert_eq!(kept.predicted(), parent.predicted());

    // The same rewrite in an overflowing batch must not send retained d/e.
    // Only rewritten victim b needs a write; precomputed a/c can be reused.
    let result = parent.writes(&keys(&["b", "g"]), 2, 6);
    assert_eq!(result.all_evicted_keys, keys(&["a", "b", "c"]));
    assert_eq!(result.refinement.evict, keys(&["b"]));
    assert_eq!(result.refinement.retract, keys(&["d", "e"]));
    assert_eq!(parent.predicted(), keys(&["d", "e"]));
}

#[test]
fn lru_refinement_cancels_unhinted_rewrites_without_caller_unmarking() {
    let mut parent = LruWrite::<K>::new(4);
    parent.writes(&keys(&["a", "b", "c", "d", "e", "f"]), 1, 6);
    assert_eq!(parent.hint_writes(&[], 6, 3).evict, keys(&["a", "b", "c", "d", "e"]));

    let mut kept = parent.clone();
    let result = kept.writes(&keys(&["b", "b"]), 2, 6);
    assert!(result.all_evicted_keys.is_empty() && result.refinement.evict.is_empty());
    assert_eq!(result.refinement.retract, keys(&["b"]));
    assert_eq!(kept.predicted(), keys(&["a", "c", "d", "e"]));
    assert_eq!(parent.predicted(), keys(&["a", "b", "c", "d", "e"]));

    // Finalization restores b even though its mark is canceled, and also e,
    // whose mark survives for the next block. No writer callback is needed.
    let result = parent.writes(&keys(&["b", "g"]), 2, 6);
    assert_eq!(result.all_evicted_keys, keys(&["a", "c", "d"]));
    assert!(result.refinement.evict.is_empty());
    assert_eq!(result.refinement.retract, keys(&["b", "e"]));
    assert_eq!(parent.predicted(), keys(&["e"]));
}

fn rewritten_victims_need_final_writes<P: EvictionPolicy<K>>(mut policy: P) {
    policy.writes(&keys(&["a", "b"]), 1, 2);
    assert_eq!(policy.hint_writes(&[], 2, 0).evict, keys(&["a", "b"]));
    let result = policy.writes(&keys(&["a", "c"]), 2, 2);
    assert_eq!(result.all_evicted_keys, keys(&["a", "b", "c"]));
    assert_eq!(result.refinement.evict, keys(&["a", "c"]));
    assert!(result.refinement.retract.is_empty() && policy.predicted().is_empty());
}

#[test]
fn refinement_includes_rewritten_and_unpredicted_actual_victims() {
    rewritten_victims_need_final_writes(Fifo::new(0));
    rewritten_victims_need_final_writes(LruWrite::new(0));
}

#[test]
fn lru_write_hints_predict_the_oldest_and_retract_hinted_marks() {
    let mut p = LruWrite::<K>::new(2);
    p.writes(&keys(&["a", "b", "c"]), 1, 4);
    // The fixed two-key window excludes every hinted write.
    let pred = p.hint_writes(&keys(&["x", "y", "c"]), 4, 0);
    assert_eq!(pred.evict, keys(&["a", "b"]));
    assert!(pred.retract.is_empty());
    let pred = p.hint_writes(&keys(&["a", "a"]), 4, 0);
    assert_eq!(pred.retract, keys(&["a"]));
    assert!(pred.evict.is_empty(), "all remaining candidates were already hinted");
    assert_eq!(p.predicted(), keys(&["b"]));

    p.reset_hints();
    assert_eq!(p.hint_writes(&keys(&["x", "y"]), 4, 0).evict, keys(&["a", "b"]));
    // A kept block changes the queue. Refill uses its new order and includes
    // the entire hint before choosing a replacement: hinted c must be skipped.
    p.writes(&keys(&["d"]), 2, 4);
    let pred = p.hint_writes(&keys(&["b", "c", "b"]), 4, 0);
    assert_eq!(pred.retract, keys(&["b"]));
    assert_eq!(pred.evict, keys(&["d"]));
    assert_eq!(p.predicted(), keys(&["a", "d"]));
}

#[test]
fn watermark_predictions_use_a_fixed_window_despite_projected_overflow() {
    let mut p = Fifo::<K>::new(3).with_trigger(3);
    assert!(p.writes(&keys(&["a", "b", "c"]), 1, 4).all_evicted_keys.is_empty());
    assert_eq!(p.hint_writes(&keys(&["d", "e"]), 4, 0).evict, keys(&["a"]));
    assert!(p.hint_writes(&keys(&["d", "e", "f"]), 4, 0).evict.is_empty());
    assert_eq!(p.predicted(), keys(&["a"]));
    let ev = p.writes(&keys(&["d", "e"]), 2, 4);
    assert_eq!(ev.all_evicted_keys, keys(&["a", "b"]));
    assert_eq!(ev.refinement.evict, keys(&["b"]));
    assert!(ev.refinement.retract.is_empty() && p.predicted().is_empty());
    // A new chain gets a new fixed prefix, never more predictions from
    // later insertion hints on that chain.
    assert_eq!(p.hint_writes(&keys(&["g", "h"]), 4, 0).evict, keys(&["c"]));
    let ev = p.writes(&keys(&["g", "h"]), 3, 4);
    assert_eq!(ev.all_evicted_keys, keys(&["c", "d"]));
    assert_eq!(ev.refinement.evict, keys(&["d"]));
    assert!(ev.refinement.retract.is_empty());
}

#[test]
fn lru_fixed_window_survives_false_hints_and_queue_changes() {
    let mut p = LruWrite::<K>::new(4).with_trigger(5);
    p.writes(&keys(&["a", "b", "c", "d", "e"]), 1, 6);
    assert_eq!(p.hint_writes(&[], 6, 0).evict, keys(&["a", "b"]));
    let pred = p.hint_writes(&keys(&["a", "c", "a"]), 6, 0);
    assert_eq!(pred.retract, keys(&["a"]));
    assert_eq!(pred.evict, keys(&["d"]));
    // Neither hint becomes an actual write. Retained marks stay bounded;
    // rebuilding the order makes the earlier a eligible again for replacement.
    p.writes(&[], 2, 6);
    assert!(p.hint_writes(&keys(&["x", "y", "z"]), 6, 0).evict.is_empty());
    assert_eq!(p.predicted(), keys(&["b", "d"]));
    let pred = p.hint_writes(&keys(&["b"]), 6, 0);
    assert_eq!(pred.retract, keys(&["b"]));
    assert_eq!(pred.evict, keys(&["a"]));
    assert_eq!(p.predicted(), keys(&["a", "d"]));
    let ev = p.writes(&keys(&["x", "y"]), 3, 6);
    assert_eq!(ev.all_evicted_keys, keys(&["a", "b", "c"]));
    assert_eq!(ev.refinement.evict, keys(&["b", "c"]));
    assert_eq!(ev.refinement.retract, keys(&["d"]));
}

#[test]
fn fixed_predictions_handle_empty_windows_and_exhausted_queues() {
    for mut p in [Fifo::<K>::new(4), Fifo::<K>::new(4).with_trigger(4)] {
        p.writes(&keys(&["a", "b", "c", "d"]), 1, 4);
        assert!(p.hint_writes(&keys(&["x", "y"]), 4, 0).evict.is_empty());
        assert!(p.hint_writes(&keys(&["a", "z"]), 4, 0).evict.is_empty());
        assert_eq!(p.writes(&keys(&["x"]), 2, 4).refinement.evict, keys(&["a"]));
    }
    let mut p = LruWrite::<K>::new(4);
    p.writes(&keys(&["a", "b", "c", "d"]), 1, 4);
    assert!(p.hint_writes(&keys(&["x"]), 4, 0).evict.is_empty());
    assert!(p.hint_writes(&keys(&["a", "x"]), 4, 0).evict.is_empty());

    let mut p = LruWrite::<K>::new(0).with_trigger(0);
    p.writes(&keys(&["a", "b"]), 1, 4);
    assert_eq!(p.hint_writes(&keys(&["b"]), 4, 0).evict, keys(&["a"]));
    let pred = p.hint_writes(&keys(&["a", "a"]), 4, 0);
    assert_eq!(pred.retract, keys(&["a"]));
    assert!(pred.evict.is_empty());
    // Activation survives this kept block. The cursor must reset even though
    // the former queue was exhausted, so new entries can fill the window.
    p.writes(&keys(&["c"]), 2, 4);
    assert_eq!(p.hint_writes(&[], 4, 0).evict, keys(&["a", "b", "c"]));
}

// The high watermark schedules a full future eviction, without removing
// entries or changing consensus roots at the prediction threshold.
fn watermark_boundaries<P: EvictionPolicy<K>>(mut policy: P) {
    let key = |i: usize| format!("k{i:05}").into_bytes();
    let resident: Vec<K> = (0..8999).map(key).collect();
    assert!(policy.writes(&resident, 1, 10_000).all_evicted_keys.is_empty());
    assert!(policy.hint_writes(&[], 10_000, 0).evict.is_empty());
    let digest = policy.state_digest();
    let prediction = policy.hint_writes(&[key(8999)], 10_000, 0);
    assert_eq!(prediction.evict, (0..3000).map(key).collect::<Vec<_>>());
    assert_eq!(policy.state_digest(), digest);
    assert!(policy.hint_writes(&[key(8999), key(8999)], 10_000, 0).evict.is_empty());
    assert!(policy.writes(&[key(8999)], 2, 10_000).all_evicted_keys.is_empty());
    assert_eq!(policy.predicted().len(), 3000);
    let more: Vec<K> = (9000..10_000).map(key).collect();
    assert!(policy.writes(&more, 3, 10_000).all_evicted_keys.is_empty());
    // One actual overflow key means 3001 evictions to leave exactly 7000.
    let eviction = policy.writes(&[key(10_000)], 4, 10_000);
    assert_eq!(eviction.all_evicted_keys, (0..3001).map(key).collect::<Vec<_>>());
    assert_eq!(eviction.refinement.evict, vec![key(3000)]);
}

#[test]
fn watermark_high_only_starts_precomputation() {
    watermark_boundaries(Fifo::new(7000).with_trigger(9000));
    watermark_boundaries(LruWrite::new(7000).with_trigger(9000));
}

#[test]
fn watermark_lru_refines_the_same_future_batch() {
    let mut policy = LruWrite::new(7).with_trigger(9);
    policy.writes(&keys(&["a", "b", "c", "d", "e", "f", "g", "h", "i"]), 1, 10);
    assert_eq!(policy.hint_writes(&[], 10, 0).evict, keys(&["a", "b", "c"]));
    let prediction = policy.hint_writes(&keys(&["a"]), 10, 0);
    assert_eq!(prediction.retract, keys(&["a"]));
    assert_eq!(prediction.evict, keys(&["d"]));
    assert_eq!(policy.predicted(), keys(&["b", "c", "d"]));
    assert!(policy.hint_writes(&keys(&["a"]), 10, 0).evict.is_empty());
    // A false hint must be corrected by the final proposal too.
    let eviction = policy.writes(&keys(&["j", "k"]), 2, 10);
    assert_eq!(eviction.all_evicted_keys, keys(&["a", "b", "c", "d"]));
    assert_eq!(eviction.refinement.evict, keys(&["a"]));
}

fn watermark_kept_chain<P: EvictionPolicy<K>>(plain_policy: P, early_policy: P) {
    let mut plain = open(10, 4, plain_policy, Rehash::new());
    let mut spec = open_with(speculating(10, 4, 16, 1, 0), early_policy, Rehash::new());
    agree(&mut plain, &mut spec, fill_batch(9));
    let reader = spec.reader();
    let storage_root = reader.latest_revision().node().storage_view().root();
    spec.prefetch(None, &keys(&["k8"]));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while reader.stats().speculation.steps_built == 0 {
        assert!(std::time::Instant::now() < deadline, "early precomputation did not run");
        std::thread::sleep(Duration::from_millis(1));
    }
    // Rewrite a predicted entry: refresh it under FIFO, withdraw it under LRU.
    agree(&mut plain, &mut spec, vec![put("k0", "updated")]);
    spec.prefetch(None, &keys(&["x"]));
    agree(&mut plain, &mut spec, vec![put("x", "new")]);
    assert_eq!(reader.stats().occupancy, 10);
    assert_eq!(reader.stats().total_evictions, 0);
    assert_eq!(reader.latest_revision().node().storage_view().root(), storage_root);
    assert_eq!(reader.stats().speculation.steps_dropped, 0);
    // Actual overflow, including one unhinted write, reconciles the kept proposal.
    agree(&mut plain, &mut spec, vec![put("y", "new")]);
    assert_eq!(reader.stats().occupancy, 7);
    assert_eq!(reader.stats().total_evictions, 4);
    assert!(reader.stats().speculation.reused_ops > 0);
    assert_eq!(reader.get(&k("k0")).unwrap(), Some(k("updated")));
    assert_no_failures(&reader.stats().speculation);
    spec.close().unwrap();
    plain.close().unwrap();
}

#[test]
fn watermark_keeps_proposals_across_blocks_and_preserves_roots() {
    // Different high watermarks, with and without hints/speculation, must agree.
    watermark_kept_chain(Fifo::new(7), Fifo::new(7).with_trigger(9));
    watermark_kept_chain(LruWrite::new(7), LruWrite::new(7).with_trigger(9));
}

#[test]
fn watermark_validation_and_consensus_parameters() {
    for high in [7, 9, 10] {
        let fifo = Fifo::<K>::new(7).with_trigger(high);
        let lru = LruWrite::<K>::new(7).with_trigger(high);
        fifo.validate(10).unwrap();
        lru.validate(10).unwrap();
        assert_eq!(fifo.params_digest(), Fifo::<K>::new(7).params_digest());
        assert_eq!(lru.params_digest(), LruWrite::<K>::new(7).params_digest());
    }
    for high in [6, 11] {
        assert!(Fifo::<K>::new(7).with_trigger(high).validate(10).is_err());
        assert!(LruWrite::<K>::new(7).with_trigger(high).validate(10).is_err());
    }
    assert_ne!(Fifo::<K>::new(7).params_digest(), Fifo::<K>::new(6).params_digest());
}

// ---- hints compose to the plain eviction (R32, design §9.3) ----

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
            "auth_wal-{tag}-{}-{}",
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

/// From one policy state, a block of several hints (overlapping, with keys
/// that will not be written and writes that were never hinted) followed by
/// `writes` of the union must evict exactly what `writes` of the union
/// evicts on a copy that saw no hint, with equal digests; and the steps'
/// contents plus the refinement must hold exactly the victims on eviction,
/// or the continuing predictions when the chain is kept. Several blocks, so the
/// marks that outlive a block (R31) are exercised; default and early
/// prediction thresholds alternate.
fn hints_compose_to_the_plain_eviction<P: EvictionPolicy<K>>(make: impl Fn(usize, Option<usize>) -> P) {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let key = |i: u64| format!("k{i:05}").into_bytes();
    for round in 0..40u32 {
        let capacity = 16 + rng.below(64) as usize;
        let low = capacity / 2 + rng.below((capacity / 2) as u64) as usize;
        let trigger = (round % 2 == 1).then(|| low + rng.below((capacity - low + 1) as u64) as usize);
        let universe = 3 * capacity as u64;
        let mut plain = make(low, trigger);
        let mut height = 0u64;
        for _ in 0..3 {
            height += 1;
            let batch: Vec<K> = (0..capacity as u64 / 2).map(|_| key(rng.below(universe))).collect();
            plain.writes(&batch, height, capacity);
        }
        let mut hinted = plain.clone();
        assert!(hinted.predicted().is_empty());
        for _block in 0..6 {
            height += 1;
            let mut predicted: BTreeSet<K> = hinted.predicted().into_iter().collect();
            let mut union: Vec<K> = Vec::new();
            let hints = 1 + rng.below(6);
            for _ in 0..hints {
                let n = 1 + rng.below(capacity as u64 / 4) as usize;
                let chunk: Vec<K> = (0..n).map(|_| key(rng.below(universe))).collect();
                let margin = rng.below(3) as usize;
                let pred = hinted.hint_writes(&chunk, capacity, margin);
                for key in &pred.retract {
                    assert!(predicted.remove(key), "retracted a key that was not marked");
                }
                for key in &pred.evict {
                    assert!(predicted.insert(key.clone()), "predicted a marked key again");
                }
                // Hinted keys are likely writes, not certain ones; a few
                // writes were never hinted.
                for key in chunk {
                    if rng.below(4) != 0 {
                        union.push(key);
                    }
                }
                if rng.below(3) == 0 {
                    union.push(key(rng.below(universe)));
                }
            }
            assert_eq!(hinted.predicted().into_iter().collect::<BTreeSet<_>>(), predicted);
            let a = hinted.writes(&union, height, capacity);
            let b = plain.writes(&union, height, capacity);
            // Consensus: the hints changed nothing.
            assert_eq!(a.all_evicted_keys, b.all_evicted_keys, "round {round}, height {height}");
            assert_eq!(hinted.state_digest(), plain.state_digest());
            assert_eq!(
                b.refinement,
                Prediction {
                    evict: b.all_evicted_keys.clone(),
                    retract: Vec::new(),
                }
            );
            // Apply the returned instructions to the precomputed key set.
            let evicted: BTreeSet<K> = a.all_evicted_keys.iter().cloned().collect();
            let retract: BTreeSet<K> = a.refinement.retract.iter().cloned().collect();
            let evict: BTreeSet<K> = a.refinement.evict.iter().cloned().collect();
            let written: BTreeSet<K> = union.iter().cloned().collect();
            assert!(retract.is_subset(&predicted) && retract.is_disjoint(&evicted));
            let storage: BTreeSet<K> = predicted
                .difference(&retract)
                .cloned()
                .chain(evict.iter().cloned())
                .collect();
            let next_predictions: BTreeSet<K> = hinted.predicted().into_iter().collect();
            if evicted.is_empty() {
                // A kept chain still holds useful predictions, not an empty
                // storage state. Only rewritten entries can need resending.
                assert!(evict.is_subset(&predicted) && evict.is_subset(&written));
                assert_eq!(storage, next_predictions, "kept chain, round {round}, height {height}");
            } else {
                assert!(evict.is_subset(&evicted));
                assert_eq!(storage, evicted, "final chain, round {round}, height {height}");
                // FIFO retains all restored predictions. LRU cancels rewritten
                // ones before returning, while still returning their restoration.
                let expected_marks = if hinted.name() == "lru-write" {
                    retract.difference(&written).cloned().collect()
                } else {
                    retract
                };
                assert_eq!(next_predictions, expected_marks);
            }
        }
    }
}

#[test]
fn fifo_hints_compose_to_the_plain_eviction() {
    hints_compose_to_the_plain_eviction(|low, trigger| match trigger {
        Some(trigger) => Fifo::<K>::new(low).with_trigger(trigger),
        None => Fifo::new(low),
    });
}

#[test]
fn lru_write_hints_compose_to_the_plain_eviction() {
    hints_compose_to_the_plain_eviction(|low, trigger| match trigger {
        Some(trigger) => LruWrite::<K>::new(low).with_trigger(trigger),
        None => LruWrite::new(low),
    });
}

#[test]
fn params_digest_covers_the_watermark() {
    let a: Fifo<K> = Fifo::new(1);
    let b: Fifo<K> = Fifo::new(2);
    assert_ne!(a.params_digest(), b.params_digest());
    assert_eq!(a.params_digest(), LruWrite::<K>::new(1).params_digest());
}

// ---- the plain path end to end (design §4, §5, §7.3, §8, §13) ----

type Wal<C, P> = AuthWal<K, V, C, P, MemoryStorage<K, V>>;

// Empty-start fixtures keep their policy/scheme convenience at the test boundary.
fn new_empty<C, P, S>(config: Config, storage: S, policy: P, vc: C) -> crate::types::Result<AuthWal<K, V, C, P, S>>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    let set = crate::set::Snapshot::genesis(vc, policy, config.consensus.capacity);
    AuthWal::new(config, storage, set)
}

fn config(capacity: usize, revisions: usize) -> Config {
    Config {
        consensus: Consensus { capacity },
        local: Local {
            revisions,
            ..Local::default()
        },
    }
}

fn open<C, P>(capacity: usize, revisions: usize, policy: P, vc: C) -> Wal<C, P>
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

fn get<C, P>(wal: &Wal<C, P>, key: &str) -> Option<V>
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    wal.reader().get(&k(key)).unwrap()
}

fn suite<C, P>(policy: impl Fn() -> P, vc: impl Fn() -> C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    fresh_instance(policy(), vc());
    reads_and_roots(policy(), vc());
    evictions_reach_storage(policy(), vc());
    revision_window(policy(), vc());
    lineage_rules(policy(), vc());
    metadata_and_equal_roots(policy(), vc());
    chained_equals_sequential(policy(), vc());
    close_and_errors(policy(), vc());
    differential(policy(), vc());
}

fn fresh_instance<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    assert_eq!(reader.root(), EMPTY_ROOT);
    assert_eq!(reader.get(&k("a")).unwrap(), None);
    // Genesis resolves while it is in the ring (design §3, §8).
    assert_eq!(reader.revision(EMPTY_ROOT).unwrap().root(), EMPTY_ROOT);
    assert_eq!(reader.latest_revision().root(), EMPTY_ROOT);
    let stats = reader.stats();
    assert_eq!((stats.height, stats.occupancy, stats.puts), (0, 0, 0));
}

fn reads_and_roots<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    let p1 = wal.propose(None, vec![put("a", "1"), put("b", "2")], None).unwrap();
    let r1 = p1.root();
    assert_ne!(r1, EMPTY_ROOT);
    // A live proposal reads its own set and resolves by root (design §5, §8).
    assert_eq!(p1.get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(reader.get(&k("a")).unwrap(), None);
    assert_eq!(reader.revision(r1).unwrap().get(&k("b")).unwrap(), Some(k("2")));
    assert_eq!(wal.commit(p1).unwrap(), r1);
    assert_eq!(reader.root(), r1);
    assert_eq!(get(&wal, "a"), Some(k("1")));
    // A rewrite and a delete: in-place update, tombstone reads as absent, a
    // put over the tombstone is a write to a present key.
    let r2 = wal.update(vec![put("a", "3"), del("b"), del("c")], None).unwrap();
    assert_ne!(r2, r1);
    assert_eq!(get(&wal, "a"), Some(k("3")));
    assert_eq!(get(&wal, "b"), None);
    assert_eq!(get(&wal, "c"), None);
    let stats = reader.stats();
    assert_eq!(stats.height, 2);
    assert_eq!(stats.occupancy, 3);
    assert_eq!(stats.puts, 3);
    assert_eq!(stats.in_place_updates, 1);
    assert_eq!(stats.total_evictions, 0);
    assert_eq!(stats.storage_proposals, 0);
    wal.update(vec![put("c", "4")], None).unwrap();
    assert_eq!(reader.stats().in_place_updates, 2);
    assert_eq!(get(&wal, "c"), Some(k("4")));
    // An empty block still gets a new root (height is in the preimage).
    let r3 = reader.root();
    let r4 = wal.update(vec![], None).unwrap();
    assert_ne!(r3, r4);
}

fn evictions_reach_storage<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    // Capacity 4, watermark 2: the fifth entry evicts two.
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    wal.update(vec![put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")], None)
        .unwrap();
    assert_eq!(reader.stats().occupancy, 4);
    let p = wal.propose(None, vec![put("e", "5"), del("f")], None).unwrap();
    // Six entries: the policy evicts down to two; the node owns a storage
    // proposal and reads through it for the evicted keys.
    let expected = [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")];
    for (key, value) in expected {
        assert_eq!(p.get(&k(key)).unwrap(), Some(k(value)));
    }
    assert_eq!(p.get(&k("f")).unwrap(), None);
    wal.commit(p).unwrap();
    let stats = reader.stats();
    assert_eq!(stats.occupancy, 2);
    assert_eq!(stats.total_evictions, 4);
    assert_eq!(stats.storage_proposals, 1);
    assert_eq!(stats.total_storage_ops, 4);
    for (key, value) in expected {
        assert_eq!(get(&wal, key), Some(k(value)));
    }
    assert_eq!(get(&wal, "f"), None);
    // The committed state stays readable through a retained revision after
    // further commits, exactly as it was.
    let r = reader.root();
    wal.update(vec![put("a", "9")], None).unwrap();
    assert_eq!(reader.revision(r).unwrap().get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(get(&wal, "a"), Some(k("9")));
}

fn revision_window<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    let mut roots = vec![EMPTY_ROOT];
    for i in 0..5 {
        roots.push(wal.update(vec![put("k", &i.to_string())], None).unwrap());
    }
    // K = 3: the last three committed roots resolve, the older ones do not.
    for root in &roots[..3] {
        assert!(matches!(reader.revision(*root), Err(Error::RevisionNotFound)));
    }
    for (i, root) in roots[3..].iter().enumerate() {
        let handle = reader.revision(*root).unwrap();
        assert_eq!(handle.get(&k("k")).unwrap(), Some(k(&(i + 2).to_string())));
    }
    // A live proposal resolves while its handle is held, not after.
    let p = wal.propose(None, vec![put("x", "1")], None).unwrap();
    let live = p.root();
    assert_eq!(reader.revision(live).unwrap().get(&k("x")).unwrap(), Some(k("1")));
    drop(p);
    assert!(matches!(reader.revision(live), Err(Error::RevisionNotFound)));
}

fn lineage_rules<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    // A deeper pipelined node: commit the parent first.
    let p1 = wal.propose(None, vec![put("a", "1")], None).unwrap();
    let p2 = wal.propose(Some(&p1), vec![put("b", "2")], None).unwrap();
    assert_eq!(p2.get(&k("a")).unwrap(), Some(k("1")));
    assert!(matches!(wal.commit(p2), Err(Error::ParentUncommitted)));
    let p2 = wal.propose(Some(&p1), vec![put("b", "2")], None).unwrap();
    wal.commit(p1).unwrap();
    wal.commit(p2).unwrap();
    assert_eq!(get(&wal, "b"), Some(k("2")));
    // An orphan: the parent dropped uncommitted.
    let p3 = wal.propose(None, vec![put("c", "3")], None).unwrap();
    let p4 = wal.propose(Some(&p3), vec![put("d", "4")], None).unwrap();
    drop(p3);
    assert!(matches!(wal.commit(p4), Err(Error::ProposalOrphaned)));
    // A twin of the head: that state is committed already.
    let p5 = wal.propose(None, vec![put("e", "5")], None).unwrap();
    let p5b = wal.propose(None, vec![put("e", "5")], None).unwrap();
    assert_eq!(p5.root(), p5b.root());
    wal.commit(p5).unwrap();
    assert!(matches!(wal.commit(p5b), Err(Error::ProposalConsumed)));
    // Proposing on a handle whose root is the head's builds on the head.
    let head = wal.reader().latest_revision();
    let p6 = wal.propose(Some(&head), vec![put("f", "6")], None).unwrap();
    wal.commit(p6).unwrap();
    assert_eq!(get(&wal, "f"), Some(k("6")));
}

fn metadata_and_equal_roots<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let batch = || vec![put("a", "1")];
    let none = wal.propose(None, batch(), None).unwrap().root();
    let empty = wal.propose(None, batch(), Some(b"")).unwrap().root();
    let some = wal.propose(None, batch(), Some(b"m")).unwrap().root();
    let same = wal.propose(None, batch(), Some(b"m")).unwrap().root();
    assert_ne!(none, empty);
    assert_ne!(empty, some);
    assert_eq!(some, same);
    // The equal-root property (design §7.3): a child proposed on the
    // committed node and on a live twin of it gets one root.
    let p1 = wal.propose(None, batch(), None).unwrap();
    let twin = wal.propose(None, batch(), None).unwrap();
    wal.commit(p1).unwrap();
    let on_head = wal.propose(None, vec![put("b", "2")], None).unwrap();
    let on_twin = wal.propose(Some(&twin), vec![put("b", "2")], None).unwrap();
    assert_eq!(on_head.root(), on_twin.root());
}

fn chained_equals_sequential<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let blocks = |i: u64| vec![put(&format!("k{}", i % 5), &i.to_string()), put(&format!("n{i}"), "x")];
    // Sequential: propose, commit, propose, commit.
    let mut a = open(4, 8, policy.clone(), vc.clone());
    let mut sequential = Vec::new();
    for i in 0..6 {
        sequential.push(a.update(blocks(i), None).unwrap());
    }
    // Chained: every block proposed on the previous live one, committed in order.
    let mut b = open(4, 8, policy, vc);
    let mut handles = Vec::new();
    let mut parent = None;
    for i in 0..6 {
        let h = b.propose(parent.as_ref(), blocks(i), None).unwrap();
        handles.push(h.root());
        parent = Some(b.reader().revision(h.root()).unwrap());
        drop(h);
    }
    drop(parent);
    assert_eq!(handles, sequential);
}

fn close_and_errors<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    wal.update(vec![put("a", "1")], None).unwrap();
    assert!(matches!(
        wal.propose(
            None,
            vec![BatchOp::PrefixDelete {
                prefix: Box::new([0u8])
            }],
            None
        ),
        Err(Error::PrefixDeleteUnsupported)
    ));
    wal.close().unwrap();
    wal.close().unwrap();
    assert!(matches!(wal.propose(None, vec![], None), Err(Error::Closed)));
    // Reads keep working after close (design §3).
    assert_eq!(reader.get(&k("a")).unwrap(), Some(k("1")));
    assert_ne!(reader.root(), EMPTY_ROOT);
}

/// Random-ish blocks of puts and deletes against a reference map: every read
/// through the committed state, and through each live proposal, matches.
fn differential<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(16, 4, policy, vc);
    let reader = wal.reader();
    let mut reference: BTreeMap<K, V> = BTreeMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut next = || rng.next();
    for _ in 0..60 {
        let mut batch = Vec::new();
        let mut pending: BTreeMap<K, Option<V>> = BTreeMap::new();
        for _ in 0..(next() % 12) {
            let key = k(&format!("k{}", next() % 40));
            if next() % 4 == 0 {
                batch.push(BatchOp::Delete { key: key.clone() });
                pending.insert(key, None);
            } else {
                let value = k(&format!("v{}", next() % 1000));
                batch.push(BatchOp::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
                pending.insert(key, Some(value));
            }
        }
        let proposal = wal.propose(None, batch, None).unwrap();
        for (key, value) in pending {
            match value {
                Some(value) => {
                    reference.insert(key, value);
                }
                None => {
                    reference.remove(&key);
                }
            }
        }
        for i in 0..40 {
            let key = k(&format!("k{i}"));
            assert_eq!(proposal.get(&key).unwrap(), reference.get(&key).cloned());
        }
        wal.commit(proposal).unwrap();
        for i in 0..40 {
            let key = k(&format!("k{i}"));
            assert_eq!(reader.get(&key).unwrap(), reference.get(&key).cloned());
        }
        assert!(reader.stats().occupancy <= 16);
    }
    assert!(reader.stats().total_evictions > 0);
}

#[test]
fn plain_path_lthash_fifo() {
    suite(|| Fifo::new(2), LtHash::new);
}

#[test]
fn plain_path_lthash_lru_write() {
    suite(|| LruWrite::new(2), LtHash::new);
}

#[test]
fn plain_path_merkle_fifo() {
    suite(|| Fifo::new(2), Merkle::new);
}

#[test]
fn plain_path_merkle_lru_write() {
    suite(|| LruWrite::new(2), Merkle::new);
}

#[test]
fn plain_path_rehash_fifo() {
    suite(|| Fifo::new(2), Rehash::new);
}

#[test]
fn plain_path_rehash_lru_write() {
    suite(|| LruWrite::new(2), Rehash::new);
}

#[test]
fn lru_write_and_fifo_differ_on_a_same_value_rewrite() {
    // Under lru-write a same-value rewrite moves the key, so the root differs
    // from an empty block's; under fifo the two blocks share a root (design
    // §6.1 tests, §7.3).
    fn roots<P: EvictionPolicy<K>>(policy: impl Fn() -> P) -> (Hash, Hash) {
        let mut wal = open(4, 3, policy(), LtHash::new());
        wal.update(vec![put("a", "1")], None).unwrap();
        let rewrite = wal.propose(None, vec![put("a", "1")], None).unwrap().root();
        let empty = wal.propose(None, vec![], None).unwrap().root();
        (rewrite, empty)
    }
    let (rewrite, empty) = roots(|| LruWrite::new(2));
    assert_ne!(rewrite, empty);
    let (rewrite, empty) = roots(|| Fifo::new(2));
    assert_eq!(rewrite, empty);
}

#[test]
fn schemes_and_policies_give_distinct_roots() {
    let block = vec![put("a", "1")];
    let r1 = open(4, 3, Fifo::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r2 = open(4, 3, Fifo::new(2), Merkle::new())
        .update(block.clone(), None)
        .unwrap();
    let r3 = open(4, 3, LruWrite::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r4 = open(4, 3, Fifo::new(1), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r5 = open(5, 3, Fifo::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r6 = open(4, 3, Fifo::new(2), Rehash::new()).update(block, None).unwrap();
    let roots = [r1, r2, r3, r4, r5, r6];
    for (i, a) in roots.iter().enumerate() {
        for b in &roots[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

fn new_with_populated_set<C, P>(vc: C, policy: P)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    use crate::set::Snapshot;
    let initial = vec![
        (k("a"), Entry::Value(k("old"))),
        (k("b"), Entry::Tombstone),
        (k("c"), Entry::Value(k("3"))),
        (k("a"), Entry::Value(k("1"))),
    ];
    let set = Snapshot::from_entries(vc.clone(), policy.clone(), 4, initial).unwrap();
    assert_eq!((set.len(), set.height()), (3, 1));
    let mut reference = open(4, 4, policy, vc);
    let root = reference
        .update(vec![put("a", "old"), del("b"), put("c", "3"), put("a", "1")], None)
        .unwrap();
    let engine = MemoryStorage::new(MemoryConfig::default());
    let mut wal = AuthWal::new(speculating(4, 4, 4, 1, 0), engine, set).unwrap();
    let reader = wal.reader();
    assert_eq!(reader.root(), root);
    assert_ne!(root, EMPTY_ROOT);
    assert_eq!(reader.revision(root).unwrap().root(), root);
    assert_eq!(reader.latest_revision().node().storage_view().root(), EMPTY_ROOT);
    assert_eq!((reader.stats().height, reader.stats().occupancy), (1, 3));
    assert_eq!(reader.stats().puts, 0, "initialization is not measured as a write");
    assert_eq!(reader.get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(reader.get(&k("b")).unwrap(), None);
    for batch in [
        vec![put("d", "4")],
        vec![put("e", "5"), put("a", "new")],
        vec![del("c"), put("f", "6")],
    ] {
        let hints: Vec<_> = batch.iter().filter_map(|op| op.key().cloned()).collect();
        wal.prefetch(None, &hints);
        agree(&mut reference, &mut wal, batch);
        for key in ["a", "b", "c", "d", "e", "f"] {
            assert_eq!(reader.get(&k(key)).unwrap(), reference.reader().get(&k(key)).unwrap());
        }
    }
    assert!(reader.stats().total_evictions > 0);
    assert_no_failures(&reader.stats().speculation);
    wal.close().unwrap();
    reference.close().unwrap();
}

#[test]
fn new_accepts_a_populated_set_with_matching_policy_and_root() {
    new_with_populated_set(LtHash::new(), Fifo::new(2).with_trigger(3));
    new_with_populated_set(Merkle::new(), LruWrite::new(2).with_trigger(3));
    new_with_populated_set(Rehash::new(), LruWrite::new(2).with_trigger(3));
}

#[test]
fn new_validates_supplied_set_capacity_and_empty_genesis() {
    use crate::set::Snapshot;
    let set = Snapshot::empty(LtHash::new(), Fifo::<K>::new(2), 4).unwrap();
    let mut wal = AuthWal::new(config(4, 3), MemoryStorage::<K, V>::new(MemoryConfig::default()), set).unwrap();
    assert_eq!(wal.reader().root(), EMPTY_ROOT);
    assert_eq!(wal.reader().stats().occupancy, 0);
    wal.close().unwrap();

    let set = Snapshot::empty(LtHash::new(), Fifo::<K>::new(2), 4).unwrap();
    let result = AuthWal::new(config(5, 3), MemoryStorage::<K, V>::new(MemoryConfig::default()), set);
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    assert!(Snapshot::<K, V, _, _>::empty(LtHash::new(), Fifo::new(0), 0).is_err());
    let too_large = Snapshot::from_entries(
        LtHash::new(),
        Fifo::new(1),
        2,
        keys(&["a", "b", "c"])
            .into_iter()
            .map(|key| (key, Entry::Value(k("v")))),
    );
    assert!(matches!(too_large, Err(Error::InvalidConfig(_))));
}

#[test]
fn new_rejects_bad_configuration_and_a_used_engine() {
    let engine = || MemoryStorage::<K, V>::new(MemoryConfig::default());
    let bad = |config: Config| new_empty(config, engine(), Fifo::new(2), LtHash::new()).err();
    assert!(matches!(bad(config(0, 3)), Some(Error::InvalidConfig(_))));
    assert!(matches!(bad(config(4, 1)), Some(Error::InvalidConfig(_))));
    let mut speculating = config(4, 3);
    speculating.local.speculate = true;
    speculating.local.speculation_rounds = 0;
    assert!(matches!(bad(speculating.clone()), Some(Error::InvalidConfig(_))));
    speculating.local.speculation_rounds = 3;
    speculating.local.speculation_min_step = 0;
    assert!(matches!(bad(speculating.clone()), Some(Error::InvalidConfig(_))));
    speculating.local.speculation_min_step = 1;
    assert!(bad(speculating).is_none());
    // The policy's own parameters against the capacity (design §6.2).
    assert!(matches!(
        new_empty(config(4, 3), engine(), Fifo::new(10), LtHash::new()).err(),
        Some(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        new_empty(config(8, 3), engine(), LruWrite::new(6).with_trigger(4), LtHash::new()).err(),
        Some(Error::InvalidConfig(_))
    ));
    // The engine keeps fewer revisions than the window needs.
    assert!(matches!(bad(config(4, 1_000)), Some(Error::InvalidConfig(_))));
    // An engine with committed state.
    let used = engine();
    {
        use crate::storage::{StorageEngine, StorageProposal};
        used.head()
            .unwrap()
            .propose(&[put("a", "1")], None)
            .unwrap()
            .commit()
            .unwrap();
    }
    assert!(matches!(
        new_empty(config(4, 3), used, Fifo::new(2), LtHash::new()).err(),
        Some(Error::EngineNotFresh)
    ));
}

#[cfg(feature = "firewood")]
#[test]
fn firewood_hashing_algorithm_is_committed_before_eviction() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = TempDir::new("hashing-id");
    let mut wal = new_empty(
        config(4, 3),
        FirewoodStorage::new(FirewoodConfig::new(dir.db())).unwrap(),
        Fifo::new(2),
        Rehash,
    )
    .unwrap();
    let node = wal.propose(None, vec![put("a", "1")], None).unwrap();
    let snapshot = node.node().snapshot();
    assert_eq!(StorageProposal::<K, V>::root(&**node.node().storage_view()), EMPTY_ROOT);
    let mut terms = wal.terms.clone();
    let expected_id = if cfg!(feature = "ethhash") {
        "firewood/ethereum"
    } else {
        "firewood/merkledb"
    };
    assert_eq!(terms.engine_id, expected_id);
    let root_for = |terms: &crate::root::Terms| {
        terms.combine(
            snapshot.capacity(),
            snapshot.height(),
            snapshot.commitment(),
            snapshot.policy_digest(),
            EMPTY_ROOT,
            crate::root::metadata_hash(None),
        )
    };
    assert_eq!(node.root(), root_for(&terms));
    terms.engine_id = if cfg!(feature = "ethhash") {
        "firewood/merkledb"
    } else {
        "firewood/ethereum"
    };
    assert_ne!(node.root(), root_for(&terms));
    wal.close().unwrap();
}

#[cfg(feature = "firewood")]
#[test]
fn plain_path_on_firewood() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = TempDir::new("m1");
    let mut cfg = FirewoodConfig::new(dir.db());
    cfg.truncate = true;
    cfg.revisions = 8;
    let engine = FirewoodStorage::new(cfg).unwrap();
    let mut wal: AuthWal<K, V, LtHash, LruWrite<K>, FirewoodStorage> =
        new_empty(config(4, 3), engine, LruWrite::new(2), LtHash::new()).unwrap();
    let reader = wal.reader();
    for i in 0..10u32 {
        wal.update(
            vec![put(&format!("k{i}"), &format!("v{i}")), put("hot", &i.to_string())],
            None,
        )
        .unwrap();
    }
    for i in 0..10u32 {
        assert_eq!(reader.get(&k(&format!("k{i}"))).unwrap(), Some(k(&format!("v{i}"))));
    }
    assert_eq!(reader.get(&k("hot")).unwrap(), Some(k("9")));
    let stats = reader.stats();
    assert!(stats.total_evictions > 0 && stats.occupancy <= 4);
    wal.close().unwrap();
}

// ---- speculation against the plain path (design §9.3, milestone 2) ----

fn speculating(capacity: usize, revisions: usize, rounds: u32, min_step: usize, margin: usize) -> Config {
    Config {
        consensus: Consensus { capacity },
        local: Local {
            revisions,
            speculate: true,
            speculation_rounds: rounds,
            speculation_min_step: min_step,
            speculation_margin: margin,
        },
    }
}

fn open_with<C, P>(config: Config, policy: P, vc: C) -> Wal<C, P>
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
fn fill<C, P, S>(plain: &mut AuthWal<K, V, C, P, S>, spec: &mut AuthWal<K, V, C, P, S>, n: usize)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    plain.update(fill_batch(n), None).unwrap();
    spec.update(fill_batch(n), None).unwrap();
}

fn fill_batch(n: usize) -> Vec<BatchOp<K, V>> {
    (0..n).map(|i| put(&format!("k{i}"), "0")).collect()
}

/// The same block on both instances; their roots must agree.
fn agree<C, P, S>(
    plain: &mut AuthWal<K, V, C, P, S>,
    spec: &mut AuthWal<K, V, C, P, S>,
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
fn assert_no_failures(s: &crate::speculate::SpeculationStats) {
    assert_eq!((s.fallbacks, s.step_failures, s.worker_panics), (0, 0, 0), "{s:?}");
}

/// §9.3 tests 1 to 3: the same random block history, with deletes, on a
/// plain instance and on a speculating one fed overlapping, partly wrong,
/// partly duplicate hints (sometimes with time for the worker to build steps
/// in between), with competing candidates on the same parent (sometimes
/// hinted, so the target moves to them; sometimes alive across the commit),
/// straggler hints on the parent after the block was proposed and hints on
/// the candidate before its commit: equal roots and equal storage roots at
/// every height, reads exact against a reference map, storage exact for
/// every key outside the set, steps built and reused, no failure or
/// fallback.
fn speculation_matches_the_plain_path<C, P>(policy: impl Fn() -> P, vc: impl Fn() -> C, rounds: u32)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let capacity = 24;
    let mut plain = open(capacity, 4, policy(), vc());
    let mut spec = open_with(speculating(capacity, 4, rounds, 1, 1), policy(), vc());
    let universe = 60u64;
    let key = |i: u64| format!("k{i:03}").into_bytes();
    let random_batch = |rng: &mut Rng, tag: u64| -> Vec<BatchOp<K, V>> {
        let n = 4 + rng.below(10) as usize;
        (0..n)
            .map(|_| {
                let key = key(rng.below(universe));
                if rng.below(5) == 0 {
                    BatchOp::Delete { key }
                } else {
                    BatchOp::Put {
                        key,
                        value: format!("v{tag}").into_bytes(),
                    }
                }
            })
            .collect()
    };
    let mut rng = Rng(7);
    let mut reference: BTreeMap<K, Option<V>> = BTreeMap::new();
    for block in 0..40u64 {
        let batch = random_batch(&mut rng, block);
        let written: Vec<K> = batch.iter().filter_map(|op| op.key().cloned()).collect();
        for chunk in written.chunks(3) {
            let mut hint = chunk.to_vec();
            if rng.below(2) == 0 {
                hint.push(key(rng.below(universe)));
            }
            spec.prefetch(None, &hint);
            if rng.below(3) == 0 {
                std::thread::sleep(Duration::from_micros(300));
            }
        }
        for op in &batch {
            match op {
                BatchOp::Put { key, value } => reference.insert(key.clone(), Some(value.clone())),
                BatchOp::Delete { key } => reference.insert(key.clone(), None),
                BatchOp::PrefixDelete { .. } => unreachable!(),
            };
        }
        // A competing candidate on the same parent (design §7.3), sometimes
        // hinted (the pipeline moving the target to it), sometimes alive
        // across the commit.
        let mut rival = None;
        if rng.below(4) == 0 {
            let other = random_batch(&mut rng, 1000 + block);
            let handle = spec.propose(None, other, None).unwrap();
            if rng.below(2) == 0 {
                spec.prefetch(Some(&handle), &[key(rng.below(universe)), key(rng.below(universe))]);
            }
            rival = Some(handle);
        }
        let r1 = plain.update(batch.clone(), None).unwrap();
        let candidate = spec.propose(None, batch, None).unwrap();
        assert_eq!(candidate.root(), r1, "block {block}");
        // A straggler hint on the parent; a hint on the candidate.
        if rng.below(3) == 0 {
            spec.prefetch(None, &[key(rng.below(universe))]);
        }
        if rng.below(2) == 0 {
            spec.prefetch(Some(&candidate), &[key(rng.below(universe)), key(rng.below(universe))]);
        }
        if rng.below(2) == 0 {
            rival = None;
        }
        let r2 = spec.commit(candidate).unwrap();
        drop(rival);
        assert_eq!(r1, r2, "block {block}");
        let h1 = plain.reader().latest_revision();
        let h2 = spec.reader().latest_revision();
        assert_eq!(h1.node().storage_view().root(), h2.node().storage_view().root());
        for i in 0..universe {
            let key = key(i);
            let expected = reference.get(&key).cloned().flatten();
            assert_eq!(spec.reader().get(&key).unwrap(), expected, "block {block}, key {i}");
            if h2.node().snapshot().get(&key).is_none() {
                assert_eq!(
                    h2.node().storage_view().get(&key).unwrap(),
                    expected,
                    "storage, block {block}"
                );
            }
        }
    }
    let s = spec.reader().stats().speculation;
    assert!(s.steps_built > 0 && s.speculative > 0 && s.reused_ops > 0, "{s:?}");
    assert_eq!((s.step_failures, s.fallbacks, s.worker_panics), (0, 0, 0), "{s:?}");
    spec.close().unwrap();
    plain.close().unwrap();
}

#[test]
fn speculation_matches_the_plain_path_bulk_rule() {
    speculation_matches_the_plain_path(|| Fifo::<K>::new(12), LtHash::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(12), LtHash::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(12), Merkle::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(12), Rehash::new, 4);
}

#[test]
fn watermark_speculation_matches_the_plain_path() {
    speculation_matches_the_plain_path(|| Fifo::<K>::new(20).with_trigger(20), LtHash::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(20).with_trigger(20), LtHash::new, 4);
    speculation_matches_the_plain_path(|| Fifo::<K>::new(20).with_trigger(20), Merkle::new, 4);
}

/// Small chains still fold every pending hint and final correction into
/// their reserved final proposal. Competing candidates must not inherit
/// the consumed chain's corrections.
#[test]
fn speculation_matches_the_plain_path_with_small_chains() {
    for rounds in [1, 2] {
        speculation_matches_the_plain_path(|| Fifo::<K>::new(12), LtHash::new, rounds);
        speculation_matches_the_plain_path(|| LruWrite::<K>::new(12), LtHash::new, rounds);
        speculation_matches_the_plain_path(|| Fifo::<K>::new(20).with_trigger(20), LtHash::new, rounds);
    }
}

/// §9.3 test 4: two children of one parent, hints between the two proposes,
/// either committed: the committed root is the plain path's, and so is the
/// next block's. Finalizing the first child consumes the prediction chain;
/// its sibling uses the plain path, including with a single available round.
#[test]
fn two_children_of_the_target_either_committed() {
    for (order, rounds) in [(0, 3), (1, 3), (0, 1), (1, 1)] {
        let mut plain = open(8, 3, LruWrite::<K>::new(4), LtHash::new());
        let mut spec = open_with(speculating(8, 3, rounds, 1, 0), LruWrite::<K>::new(4), LtHash::new());
        fill(&mut plain, &mut spec, 8);
        let a = vec![put("x", "1"), put("y", "1")];
        let b = vec![put("z", "2"), put("k0", "2"), put("w", "2")];
        spec.prefetch(None, &keys(&["x", "y"]));
        let pa = spec.propose(None, a.clone(), None).unwrap();
        spec.prefetch(None, &keys(&["z", "k0", "w"]));
        let pb = spec.propose(None, b.clone(), None).unwrap();
        let (chosen, other, batch) = if order == 0 { (pa, pb, a) } else { (pb, pa, b) };
        drop(other);
        assert_eq!(
            spec.commit(chosen).unwrap(),
            plain.update(batch, None).unwrap(),
            "order {order}, rounds {rounds}"
        );
        let next = vec![put("q", "3"), del("k1")];
        spec.prefetch(None, &keys(&["q", "k1"]));
        agree(&mut plain, &mut spec, next);
        spec.close().unwrap();
    }
}

/// A final refinement consumes its chain and moves the target to the child.
/// A straggler hint on the old parent cannot alter that finished view, and
/// a sibling must build from its own parent's storage state.
#[test]
fn a_finalized_child_does_not_poison_a_sibling() {
    let mut plain = open(8, 3, Fifo::<K>::new(4), LtHash::new());
    let mut spec = open_with(speculating(8, 3, 3, 1, 0), Fifo::<K>::new(4), LtHash::new());
    let reader = spec.reader();
    fill(&mut plain, &mut spec, 8);
    // FIFO predicts its fixed prefix on the first hint; later hints do no work.
    for key in ["x", "y", "q"] {
        spec.prefetch(None, &keys(&[key]));
        std::thread::sleep(Duration::from_millis(20));
    }
    let a = vec![put("x", "1"), put("y", "1"), put("q", "1")];
    let pa = spec.propose(None, a, None).unwrap();
    assert_eq!(reader.stats().speculation.steps, 1);
    let hinted = reader.stats().speculation.hint_keys;
    // The first child is now targeted, so this old-parent hint is ignored.
    spec.prefetch(None, &keys(&["z", "w"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted);
    std::thread::sleep(Duration::from_millis(20));
    let b = vec![put("z", "2"), put("w", "2"), put("v", "2"), put("u", "2")];
    let pb = spec.propose(None, b.clone(), None).unwrap();
    drop(pa);
    assert_eq!(spec.commit(pb).unwrap(), plain.update(b, None).unwrap());
    assert_eq!(reader.get(&k("k7")).unwrap(), plain.reader().get(&k("k7")).unwrap());
    let s = reader.stats().speculation;
    assert_eq!(
        (s.speculative, s.plain),
        (1, 1),
        "the second child took the plain path, {s:?}"
    );
    assert_eq!((s.fallbacks, s.step_failures, s.worker_panics), (0, 0, 0), "{s:?}");
    spec.close().unwrap();
}

/// Finalizing a proposal on a live parent must keep the next block eligible
/// for hints before that parent commits, even two levels beyond the head.
#[test]
fn finalization_keeps_hints_for_the_next_pipelined_block() {
    let mut plain = open(8, 3, Fifo::<K>::new(4), LtHash::new());
    let mut spec = open_with(speculating(8, 3, 3, 1, 0), Fifo::<K>::new(4), LtHash::new());
    let reader = spec.reader();
    fill(&mut plain, &mut spec, 8);

    let first = vec![put("x", "1"), put("y", "1")];
    spec.prefetch(None, &keys(&["x", "y"]));
    let first_spec = spec.propose(None, first.clone(), None).unwrap();
    let first_plain = plain.propose(None, first, None).unwrap();
    assert_eq!(first_spec.root(), first_plain.root());

    let second = vec![
        put("q", "2"),
        put("r", "2"),
        put("s", "2"),
        put("t", "2"),
        put("u", "2"),
    ];
    spec.prefetch(Some(&first_spec), &keys(&["q", "r", "s", "t", "u"]));
    let second_spec = spec.propose(Some(&first_spec), second.clone(), None).unwrap();
    let second_plain = plain.propose(Some(&first_plain), second, None).unwrap();
    assert_eq!(second_spec.root(), second_plain.root());

    let hinted = reader.stats().speculation.hint_keys;
    spec.prefetch(Some(&second_spec), &keys(&["v", "w", "z"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted + 3);
    assert_eq!(spec.commit(first_spec).unwrap(), plain.commit(first_plain).unwrap());
    let hinted = reader.stats().speculation.hint_keys;
    spec.prefetch(Some(&second_spec), &keys(&["v"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted + 1);
    assert_eq!(spec.commit(second_spec).unwrap(), plain.commit(second_plain).unwrap());
    agree(&mut plain, &mut spec, vec![put("v", "3"), put("w", "3"), put("z", "3")]);
    assert_no_failures(&reader.stats().speculation);
    spec.close().unwrap();
    plain.close().unwrap();
}

/// R35 (design §9.1): a block whose hints predicted an eviction but which
/// evicts nothing keeps the chain: no wait, the parent's view inherited,
/// nothing dropped, the node the target from then on (a hint on it counts,
/// a straggler on the old parent is ignored); the next, evicting block is
/// speculative on the kept chain and its root is the plain path's. Run on
/// the memory engine and on firewood.
fn a_block_that_evicts_nothing_keeps_the_chain_on<S: StorageEngine<K, V>>(
    mut plain: AuthWal<K, V, LtHash, LruWrite<K>, S>,
    mut spec: AuthWal<K, V, LtHash, LruWrite<K>, S>,
) {
    let reader = spec.reader();
    fill(&mut plain, &mut spec, 8);
    // Hints trigger the fixed prediction window, excluding k0. The block
    // writes k0 alone and evicts nothing, so the prediction stays provisional.
    spec.prefetch(None, &keys(&["x", "y", "k0"]));
    std::thread::sleep(Duration::from_millis(200));
    let before = reader.stats().speculation;
    assert!(before.steps_built > 0, "{before:?}");
    let block = vec![put("k0", "1")];
    let handle = spec.propose(None, block.clone(), None).unwrap();
    let head = reader.latest_revision();
    assert!(std::sync::Arc::ptr_eq(
        handle.node().storage_view(),
        head.node().storage_view()
    ));
    let after = reader.stats().speculation;
    assert_eq!(after.chains_kept, before.chains_kept + 1);
    assert_eq!(after.steps_dropped, before.steps_dropped);
    // The node is the target now: a hint on it counts, a straggler on the
    // old parent is ignored (its entries would be the parent's, design §9.1).
    let hinted = reader.stats().speculation.hint_keys;
    spec.prefetch(None, &keys(&["z"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted);
    spec.prefetch(Some(&handle), &keys(&["z"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted + 1);
    assert_eq!(spec.commit(handle).unwrap(), plain.update(block, None).unwrap());
    assert_eq!(reader.stats().speculation.steps_dropped, before.steps_dropped);
    // The next block inserts x, y and z: three victims, on the kept chain.
    let next = vec![put("x", "1"), put("y", "1"), put("z", "1")];
    spec.prefetch(None, &keys(&["x", "y", "z"]));
    agree(&mut plain, &mut spec, next);
    for key in ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "x", "y", "z"] {
        assert_eq!(
            reader.get(&k(key)).unwrap(),
            plain.reader().get(&k(key)).unwrap(),
            "{key}"
        );
    }
    let stats = reader.stats().speculation;
    assert!(stats.speculative >= 1 && stats.reused_ops > 0, "{stats:?}");
    assert_eq!(
        (stats.fallbacks, stats.step_failures, stats.steps_dropped),
        (0, 0, before.steps_dropped),
        "{stats:?}"
    );
    spec.close().unwrap();
    plain.close().unwrap();
}

#[test]
fn a_block_that_evicts_nothing_keeps_the_chain() {
    let plain = open(8, 3, LruWrite::<K>::new(4), LtHash::new());
    let spec = open_with(speculating(8, 3, 3, 1, 0), LruWrite::<K>::new(4), LtHash::new());
    a_block_that_evicts_nothing_keeps_the_chain_on(plain, spec);
}

/// Design §9.1: a competing candidate that evicted nothing keeps the chain
/// and becomes its target, with its own rewrite of a marked key on the
/// chain; the sibling that wins the block must not inherit that chain (its
/// set holds the parent's entry), so its commit resets the precomputation
/// and the next, evicting block agrees with the plain path. Under FIFO the
/// rewritten key keeps its place and is the next victim, the case that
/// leaked the rival's value into storage; lru-write moves it to the back.
fn a_rival_that_kept_the_chain_loses<P: EvictionPolicy<K>>(policy: impl Fn() -> P) {
    for hinted in [false, true] {
        let mut plain = open(8, 3, policy(), LtHash::new());
        let mut spec = open_with(speculating(8, 3, 3, 1, 0), policy(), LtHash::new());
        fill(&mut plain, &mut spec, 8);
        // Two insertions predicted: k0..k5 sent with "0".
        spec.prefetch(None, &keys(&["x", "y"]));
        std::thread::sleep(Duration::from_millis(20));
        // The rival rewrites marked k0 and evicts nothing: the chain is
        // kept, k0 = "A" goes down it, and the rival is the target.
        let rival = spec.propose(None, vec![put("k0", "A")], None).unwrap();
        if hinted {
            spec.prefetch(Some(&rival), &keys(&["k7"]));
        }
        // The block that wins rewrites k7 and evicts nothing either.
        let block = vec![put("k7", "B")];
        let winner = spec.propose(None, block.clone(), None).unwrap();
        assert_eq!(spec.commit(winner).unwrap(), plain.update(block, None).unwrap());
        drop(rival);
        // The next block evicts k0..k6: k0 reaches storage as "0".
        let next = vec![put("x", "1"), put("y", "1"), put("z", "1")];
        spec.prefetch(None, &keys(&["x", "y", "z"]));
        agree(&mut plain, &mut spec, next);
        assert_eq!(spec.reader().get(&k("k0")).unwrap(), Some(k("0")));
        assert_no_failures(&spec.reader().stats().speculation);
        spec.close().unwrap();
    }
}

#[test]
fn a_rival_that_kept_the_chain_loses_under_both_policies() {
    a_rival_that_kept_the_chain_loses(|| Fifo::<K>::new(4));
    a_rival_that_kept_the_chain_loses(|| LruWrite::<K>::new(4));
}

/// Design §9.1, §9.2: the target is a live rival of the committed block,
/// hinted, so its marks name entries of its own set (one the winner's set
/// lacks). The commit resets the chain instead of taking the rival's
/// marks; the next blocks agree with the plain path and nothing panics.
#[test]
fn a_live_rival_target_does_not_lend_its_marks() {
    let mut plain = open(10, 3, Fifo::<K>::new(5), LtHash::new());
    let mut spec = open_with(speculating(10, 3, 3, 1, 2), Fifo::<K>::new(5), LtHash::new());
    fill(&mut plain, &mut spec, 8);
    // The rival inserts k8 and rewrites k1, evicting nothing; a hint on it
    // marks k0..k8 with the rival's entries.
    let rival = spec.propose(None, vec![put("k8", "T"), put("k1", "T")], None).unwrap();
    spec.prefetch(Some(&rival), &keys(&["x", "y", "z"]));
    std::thread::sleep(Duration::from_millis(20));
    let block = vec![put("k7", "B")];
    let winner = spec.propose(None, block.clone(), None).unwrap();
    assert_eq!(spec.commit(winner).unwrap(), plain.update(block, None).unwrap());
    let next = vec![put("x", "1"), put("y", "1"), put("z", "1")];
    spec.prefetch(None, &keys(&["x", "y", "z"]));
    agree(&mut plain, &mut spec, next);
    assert_eq!(spec.reader().get(&k("k1")).unwrap(), Some(k("0")));
    drop(rival);
    let more = vec![put("w", "1")];
    agree(&mut plain, &mut spec, more);
    assert_no_failures(&spec.reader().stats().speculation);
    spec.close().unwrap();
}

/// Design §9.1: a straggler hint on the parent after a block that kept the
/// chain lands on a non-target and is ignored, since the entries it would
/// send are the parent's, not the pending block's (FIFO keeps a rewritten
/// key in place, so the stale entry would be the next victim).
#[test]
fn a_straggler_hint_on_the_old_parent_is_ignored() {
    let mut plain = open(8, 3, Fifo::<K>::new(4), LtHash::new());
    let mut spec = open_with(speculating(8, 3, 3, 1, 0), Fifo::<K>::new(4), LtHash::new());
    let reader = spec.reader();
    fill(&mut plain, &mut spec, 8);
    let block = vec![put("k0", "C")];
    let candidate = spec.propose(None, block.clone(), None).unwrap();
    let hinted = reader.stats().speculation.hint_keys;
    spec.prefetch(None, &keys(&["x", "y"]));
    assert_eq!(reader.stats().speculation.hint_keys, hinted);
    assert_eq!(spec.commit(candidate).unwrap(), plain.update(block, None).unwrap());
    let next = vec![put("x", "1"), put("y", "1"), put("z", "1")];
    spec.prefetch(None, &keys(&["x", "y", "z"]));
    agree(&mut plain, &mut spec, next);
    assert_eq!(reader.get(&k("k0")).unwrap(), Some(k("C")));
    spec.close().unwrap();
}

/// Design §9.2: a chain kept across many blocks holds at most `rounds`
/// steps, so the engine window `Local::engine_window` sizes holds and the
/// previous head stays readable after the evicting block's commit.
#[test]
fn a_kept_chain_stays_within_the_engine_window() {
    let mut spec = open_with(speculating(8, 3, 3, 1, 0), Fifo::<K>::new(4), LtHash::new());
    let reader = spec.reader();
    spec.update(fill_batch(8), None).unwrap();
    spec.prefetch(None, &keys(&["x", "y"]));
    // Fifteen blocks rewriting marked k0 and evicting nothing: an Evict
    // resend each, and more steps than the window allows if nothing capped them.
    for i in 0..15 {
        spec.update(vec![put("k0", &i.to_string())], None).unwrap();
    }
    let previous = reader.root();
    spec.prefetch(None, &keys(&["x", "y", "z"]));
    spec.update(vec![put("x", "1"), put("y", "1"), put("z", "1")], None)
        .unwrap();
    let s = reader.stats().speculation;
    // The initial fill also keeps the chain: the worker targets genesis at creation.
    assert_eq!(s.chains_kept, 16, "{s:?}");
    assert!(s.speculative == 1 && s.steps <= 2, "{s:?}");
    let parent = reader.revision(previous).unwrap();
    assert_eq!(parent.get(&k("k0")).unwrap(), Some(k("14")));
    assert_eq!(reader.get(&k("k0")).unwrap(), Some(k("14")));
    spec.close().unwrap();
}

/// An evicting candidate consumes the original chain. A non-evicting sibling
/// cannot keep that chain; committing it starts fresh speculation from its
/// own state. Rewrites and later evictions must agree with the plain path.
fn sibling_after_finalization<P: EvictionPolicy<K>>(
    policy: impl Fn() -> P,
    rounds: u32,
    hint_on_kept: bool,
    second_kept: bool,
) {
    let mut plain = open(8, 3, policy(), LtHash::new());
    let mut spec = open_with(speculating(8, 3, rounds, 1, 0), policy(), LtHash::new());
    let reader = spec.reader();
    fill(&mut plain, &mut spec, 8);
    // Two insertions predicted: k0..k5 marked and sent.
    spec.prefetch(None, &keys(&["x", "y"]));
    std::thread::sleep(Duration::from_millis(20));
    // The first child evicts and consumes the original chain; it stays
    // alive as a competing candidate and becomes the new target.
    let first = spec.propose(None, vec![put("x", "1"), put("y", "1")], None).unwrap();
    let before = reader.stats().speculation.chains_kept;
    // Its sibling rewrites k0 and k1 and evicts nothing, on the plain path.
    let block = vec![put("k0", "B"), put("k1", "B")];
    let kept = spec.propose(None, block.clone(), None).unwrap();
    assert_eq!(reader.stats().speculation.chains_kept, before);
    if hint_on_kept {
        spec.prefetch(Some(&kept), &keys(&["z", "w", "k2"]));
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(first);
    assert_eq!(spec.commit(kept).unwrap(), plain.update(block, None).unwrap());
    if second_kept {
        spec.prefetch(None, &keys(&["k3"]));
        agree(&mut plain, &mut spec, vec![put("k3", "C"), put("k0", "C2")]);
    }
    spec.prefetch(None, &keys(&["z", "w", "v"]));
    agree(&mut plain, &mut spec, vec![put("z", "1"), put("w", "1"), put("v", "1")]);
    let (head, expected) = (reader.latest_revision(), plain.reader().latest_revision());
    assert_eq!(head.node().storage_view().root(), expected.node().storage_view().root());
    for key in ["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "x", "y", "z", "w", "v"] {
        assert_eq!(head.get(&k(key)).unwrap(), expected.get(&k(key)).unwrap(), "{key}");
    }
    spec.prefetch(None, &keys(&["u", "t", "s"]));
    agree(&mut plain, &mut spec, vec![put("u", "1"), put("t", "1"), put("s", "1")]);
    assert_no_failures(&reader.stats().speculation);
    spec.close().unwrap();
}

#[test]
fn a_sibling_after_finalization_starts_a_fresh_chain() {
    for rounds in [1, 2, 3] {
        for (hint, second) in [(false, false), (true, false), (false, true), (true, true)] {
            sibling_after_finalization(|| Fifo::<K>::new(4), rounds, hint, second);
            sibling_after_finalization(|| LruWrite::<K>::new(4), rounds, hint, second);
            sibling_after_finalization(|| Fifo::<K>::new(8).with_trigger(8), rounds, hint, second);
            sibling_after_finalization(|| LruWrite::<K>::new(8).with_trigger(8), rounds, hint, second);
        }
    }
}

/// Hints on a node other than the target do nothing; `close` with a chain
/// pending joins the worker and the engine closes; after it hints are no-ops
/// and the writer reports `Closed`.
#[test]
fn hints_off_target_do_nothing_and_close_drops_the_chain() {
    let mut spec = open_with(speculating(6, 3, 3, 1, 0), Fifo::<K>::new(3), LtHash::new());
    let reader = spec.reader();
    spec.update((0..6).map(|i| put(&format!("k{i}"), "0")).collect(), None)
        .unwrap();
    let old = reader.latest_revision();
    spec.update(vec![put("k0", "1")], None).unwrap();
    let before = reader.stats().speculation.hint_keys;
    spec.prefetch(Some(&old), &keys(&["a", "b"]));
    assert_eq!(reader.stats().speculation.hint_keys, before);
    spec.prefetch(None, &keys(&["a", "b", "c"]));
    assert_eq!(reader.stats().speculation.hint_keys, before + 3);
    spec.close().unwrap();
    spec.prefetch(None, &keys(&["d"]));
    assert!(matches!(
        spec.propose(None, vec![put("d", "1")], None),
        Err(Error::Closed)
    ));
}

#[cfg(feature = "firewood")]
#[test]
fn speculation_on_firewood() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = TempDir::new("m2");
    let config = speculating(8, 3, 3, 1, 0);
    let mut cfg = FirewoodConfig::new(dir.db());
    cfg.truncate = true;
    cfg.revisions = config.local.engine_window(true);
    let engine = FirewoodStorage::new(cfg).unwrap();
    let mut wal: AuthWal<K, V, LtHash, LruWrite<K>, FirewoodStorage> =
        new_empty(config, engine, LruWrite::new(6).with_trigger(6), LtHash::new()).unwrap();
    let reader = wal.reader();
    let mut reference: BTreeMap<K, V> = BTreeMap::new();
    for i in 0..30u32 {
        let batch = vec![put(&format!("k{i}"), &format!("v{i}")), put("hot", &i.to_string())];
        wal.prefetch(None, &keys(&[&format!("k{i}"), "hot", "noise"]));
        for op in &batch {
            if let BatchOp::Put { key, value } = op {
                reference.insert(key.clone(), value.clone());
            }
        }
        wal.update(batch, None).unwrap();
    }
    for (key, value) in &reference {
        assert_eq!(reader.get(key).unwrap(), Some(value.clone()));
    }
    let stats = reader.stats();
    let s = stats.speculation;
    assert!(stats.total_evictions > 0 && stats.occupancy <= 8);
    assert!(s.steps_built > 0 && s.speculative > 0 && s.reused_ops > 0, "{s:?}");
    assert_eq!((s.step_failures, s.fallbacks, s.worker_panics), (0, 0, 0), "{s:?}");
    wal.close().unwrap();
}

/// R35 on the real engine: `a_block_that_evicts_nothing_keeps_the_chain`
/// with both instances on firewood.
#[cfg(feature = "firewood")]
#[test]
fn kept_chain_on_firewood() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dirs = (TempDir::new("kept-plain"), TempDir::new("kept-spec"));
    let open_firewood = |dir: &TempDir, config: &Config| {
        let mut cfg = FirewoodConfig::new(dir.db());
        cfg.truncate = true;
        cfg.revisions = config.local.engine_window(true).max(8);
        FirewoodStorage::new(cfg).unwrap()
    };
    let plain_config = config(8, 3);
    let spec_config = speculating(8, 3, 3, 1, 0);
    let plain_engine = open_firewood(&dirs.0, &plain_config);
    let spec_engine = open_firewood(&dirs.1, &spec_config);
    let plain: AuthWal<K, V, LtHash, LruWrite<K>, FirewoodStorage> =
        new_empty(plain_config, plain_engine, LruWrite::new(4), LtHash::new()).unwrap();
    let spec: AuthWal<K, V, LtHash, LruWrite<K>, FirewoodStorage> =
        new_empty(spec_config, spec_engine, LruWrite::new(4), LtHash::new()).unwrap();
    a_block_that_evicts_nothing_keeps_the_chain_on(plain, spec);
}

/// Known answers for the consensus encodings (design §7.2, §7.3): a change
/// to an item's bytes or domain, to the Merkle leaf or inner hash, or to
/// the preimage's order changes every root, and the other tests, which
/// compare two in-tree computations, would not notice.
#[test]
fn known_answers_for_the_encodings() {
    use crate::root::{Terms, metadata_hash};
    use crate::set::EntryRef;
    fn hex(d: &[u8]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }
    let key = k("k");
    let lthash = LtHash::new();
    let mut state = VectorCommitment::<K>::empty(&lthash);
    let no_entries = std::iter::empty::<(&K, EntryRef<'_>)>;
    VectorCommitment::<K>::apply(&lthash, &mut state, &key, None, Some(EntryRef::Value(b"vv")));
    let lattice_value = hex(&VectorCommitment::<K>::commitment(&lthash, &state, no_entries()));
    VectorCommitment::<K>::apply(
        &lthash,
        &mut state,
        &key,
        Some(EntryRef::Value(b"vv")),
        Some(EntryRef::Tombstone),
    );
    let lattice_tombstone = hex(&VectorCommitment::<K>::commitment(&lthash, &state, no_entries()));
    let memory_root = {
        use crate::storage::StorageEngine;
        let memory = MemoryStorage::<K, V>::new(MemoryConfig::default());
        let proposal = memory
            .head()
            .unwrap()
            .propose(&[put("a", "1"), del("b")], None)
            .unwrap();
        hex(&proposal.commit().unwrap().0)
    };
    let entries = [(k("a"), Entry::Value(k("1"))), (k("b"), Entry::Tombstone)];
    let items = || entries.iter().map(|(key, entry)| (key, entry.as_ref()));
    let rehash = hex(&VectorCommitment::<K>::commitment(&Rehash, &(), items()));
    let merkle = Merkle::new();
    let mut state = VectorCommitment::<K>::empty(&merkle);
    for (key, entry) in &entries {
        VectorCommitment::<K>::apply(&merkle, &mut state, key, None, Some(entry.as_ref()));
    }
    let merkle = hex(&VectorCommitment::<K>::commitment(&merkle, &state, items()));
    let terms = Terms {
        scheme_id: "lthash",
        policy_name: "fifo",
        engine_id: "memory",
        scheme_params: [1; 32],
        policy_params: [2; 32],
    };
    let root = hex(&terms
        .combine(10, 3, [3; 32], [4; 32], Hash([5; 32]), metadata_hash(Some(b"m")))
        .0);
    let actual = [
        ("lattice value", lattice_value.as_str()),
        ("lattice tombstone", lattice_tombstone.as_str()),
        ("rehash", rehash.as_str()),
        ("merkle", merkle.as_str()),
        ("root", root.as_str()),
        ("memory root", memory_root.as_str()),
    ];
    let expected = [
        (
            "lattice value",
            "a6ee9510061e24bf1ce9102373303a3b57a34b3a334391f4f40997dd9beb2a66",
        ),
        (
            "lattice tombstone",
            "1b786b4d7483232acaac8768c4bdd65e7c1f949e52c27d0eef810d077c56ac1a",
        ),
        (
            "rehash",
            "c0f53ba41d38411087747723b70c9d3891cf1bc9df93839bc8b4d3a0dafeccd0",
        ),
        (
            "merkle",
            "dbddcbc409901b07370f96e94c3d582031f62b769d1b5f6946337a921eb62d2b",
        ),
        (
            "root",
            "2700bf8ef876785482b3cd8d5f450bc5e5f59c2873cf49130322b558b8e56c13",
        ),
        (
            "memory root",
            "171488d0abe82d0401050bbab94c5c5ea9448d058718f9f4c520fddc87535706",
        ),
    ];
    assert_eq!(actual, expected);
}

/// Design §9.3 tests 1 to 3 under failure: an engine over the memory model
/// that refuses every n-th eager proposal, every n-th retraction read and
/// every n-th final proposal, so the worker's failure paths and the
/// writer's fallback to the plain path run against the plain reference,
/// with the next block pipelined on the live candidate half the time.
mod faulty {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use super::*;
    use crate::proposal::Handle;
    use crate::types::Result;

    const FINAL_METADATA: &[u8] = b"auth_wal test finalization";

    /// Every `step`-th worker-thread propose without metadata, every `read`-th
    /// worker-thread read, and every `f`-th worker-thread propose with the
    /// finalization marker fail; zero disables one. The marker distinguishes
    /// final requests from eager steps without depending on the proposal's base.
    pub struct Faults {
        step: u64,
        read: u64,
        f: u64,
        counts: [AtomicU64; 3],
        pub injected: AtomicU64,
        panic_next_step: AtomicBool,
        worker_proposals: AtomicU64,
        caller_proposals: AtomicU64,
        metadata: Mutex<Vec<Vec<u8>>>,
    }

    impl Faults {
        pub fn new(step: u64, read: u64, f: u64) -> Arc<Self> {
            Arc::new(Self {
                step,
                read,
                f,
                counts: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
                injected: AtomicU64::new(0),
                panic_next_step: AtomicBool::new(false),
                worker_proposals: AtomicU64::new(0),
                caller_proposals: AtomicU64::new(0),
                metadata: Mutex::new(Vec::new()),
            })
        }

        fn fire(&self, which: usize, every: u64) -> bool {
            if every == 0 {
                return false;
            }
            let n = self.counts[which].fetch_add(1, Ordering::Relaxed) + 1;
            let fire = n.is_multiple_of(every);
            if fire {
                self.injected.fetch_add(1, Ordering::Relaxed);
            }
            fire
        }
    }

    fn on_worker() -> bool {
        std::thread::current().name() == Some("auth_wal-worker")
    }

    pub struct Flaky {
        inner: MemoryStorage<K, V>,
        faults: Arc<Faults>,
    }

    pub struct FlakyProposal {
        inner: <MemoryStorage<K, V> as StorageEngine<K, V>>::Proposal,
        faults: Arc<Faults>,
    }

    impl StorageProposal<K, V> for FlakyProposal {
        fn root(&self) -> Hash {
            self.inner.root()
        }

        fn get(&self, key: &K) -> Result<Option<V>> {
            if on_worker() && self.faults.fire(1, self.faults.read) {
                return Err(Error::Storage("injected read failure".into()));
            }
            self.inner.get(key)
        }

        fn propose(&self, batch: &[BatchOp<K, V>], metadata: Option<&[u8]>) -> Result<Self> {
            let worker = on_worker();
            let calls = if worker {
                &self.faults.worker_proposals
            } else {
                &self.faults.caller_proposals
            };
            calls.fetch_add(1, Ordering::Relaxed);
            if let Some(metadata) = metadata {
                self.faults.metadata.lock().unwrap().push(metadata.to_vec());
            }
            if worker && self.faults.panic_next_step.swap(false, Ordering::Relaxed) {
                panic!("injected step panic");
            }
            if worker && metadata.is_none() && self.faults.fire(0, self.faults.step) {
                return Err(Error::Storage("injected step failure".into()));
            }
            if worker && metadata == Some(FINAL_METADATA) && self.faults.fire(2, self.faults.f) {
                return Err(Error::Storage("injected final-proposal failure".into()));
            }
            Ok(FlakyProposal {
                inner: self.inner.propose(batch, metadata)?,
                faults: self.faults.clone(),
            })
        }

        fn commit(&self) -> Result<Hash> {
            self.inner.commit()
        }
    }

    impl StorageEngine<K, V> for Flaky {
        type Proposal = FlakyProposal;
        const ENGINE_ID: &'static str = "memory";
        const ROOT_IS_STATE_FUNCTION: bool = true;
        const COMMITS_CHAIN_LINKS_SEPARATELY: bool = true;

        fn revisions(&self) -> usize {
            StorageEngine::<K, V>::revisions(&self.inner)
        }

        fn root(&self) -> Hash {
            StorageEngine::<K, V>::root(&self.inner)
        }

        fn head(&self) -> Result<Self::Proposal> {
            Ok(FlakyProposal {
                inner: StorageEngine::<K, V>::head(&self.inner)?,
                faults: self.faults.clone(),
            })
        }

        fn close(&mut self) -> Result<()> {
            StorageEngine::<K, V>::close(&mut self.inner)
        }
    }

    fn wait_for_steps(counters: &crate::speculate::Counters, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while counters.steps_built.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(counters.steps_built.load(Ordering::Relaxed), expected);
    }

    #[test]
    fn worker_finalization_failure_requires_reset_with_a_new_base() {
        use crate::speculate::{Counters, SpecOp, Worker, WorkerError};

        // Each failure follows a successful step, so losing that step must
        // prevent later deltas from producing a seemingly complete chain.
        for failure in ["proposal", "read", "panic"] {
            let faults = Faults::new(u64::from(failure == "proposal") * 2, u64::from(failure == "read"), 0);
            let engine = Flaky {
                inner: MemoryStorage::new(MemoryConfig::default()),
                faults: faults.clone(),
            };
            let base = Arc::new(engine.head().unwrap());
            let counters = Arc::new(Counters::default());
            let mut worker = Worker::<K, V, Flaky>::spawn(base.clone(), counters.clone(), 100, 4).unwrap();
            worker.block_end(vec![SpecOp::Evict(k("a"), Entry::Value(k("first")))]);
            wait_for_steps(&counters, 1);
            assert_eq!(base.get(&k("a")).unwrap(), None);

            faults.panic_next_step.store(failure == "panic", Ordering::Relaxed);
            let op = if failure == "read" {
                SpecOp::Retract(k("a"))
            } else {
                SpecOp::Evict(k("b"), Entry::Value(k("failed")))
            };
            let error = worker
                .finish(vec![op], None)
                .err()
                .expect("a failed finalization must return an error");
            match (&error, failure) {
                (WorkerError::Panicked, "panic") => {}
                (WorkerError::Storage(error), "read" | "proposal") => {
                    assert!(matches!(&**error, Error::Storage(_)));
                }
                _ => panic!("unexpected {failure} error: {error}"),
            }
            worker.submit(vec![SpecOp::Evict(k("c"), Entry::Value(k("ignored")))]);
            assert!(matches!(worker.finish(Vec::new(), None), Err(WorkerError::NoChain)));
            assert_eq!(counters.ops_dropped.load(Ordering::Relaxed), 1);
            assert_eq!(counters.steps_built.load(Ordering::Relaxed), 1);

            // Change both the base contents and the fault configuration. A
            // retraction must now read this base, not the original empty view.
            let new_base = Arc::new(FlakyProposal {
                inner: engine
                    .inner
                    .head()
                    .unwrap()
                    .propose(&[put("a", "new base")], None)
                    .unwrap(),
                faults: Faults::new(0, 0, 0),
            });
            worker.reset(new_base);
            let finalized = worker
                .finish(
                    vec![
                        SpecOp::Retract(k("a")),
                        SpecOp::Evict(k("c"), Entry::Value(k("resent"))),
                    ],
                    None,
                )
                .unwrap();
            assert_eq!(finalized.steps, 0);
            let view = finalized.view;
            assert_eq!(view.get(&k("a")).unwrap(), Some(k("new base")));
            assert_eq!(view.get(&k("b")).unwrap(), None);
            assert_eq!(view.get(&k("c")).unwrap(), Some(k("resent")));
            worker.shutdown();
            assert!(matches!(
                worker.finish(Vec::new(), None),
                Err(WorkerError::Disconnected)
            ));
        }
    }

    #[test]
    fn final_refinement_builds_one_worker_proposal() {
        let faults = Faults::new(0, 0, 0);
        let config = speculating(8, 3, 1, 100, 0);
        let engine = Flaky {
            inner: MemoryStorage::new(MemoryConfig {
                revisions: config.local.engine_window(true),
                ..MemoryConfig::default()
            }),
            faults: faults.clone(),
        };
        let mut spec = new_empty(config, engine, Fifo::<K>::new(4), LtHash::new()).unwrap();
        let mut plain = open(8, 3, Fifo::<K>::new(4), LtHash::new());
        assert_eq!(
            spec.update(fill_batch(8), None).unwrap(),
            plain.update(fill_batch(8), None).unwrap()
        );

        // Four pending FIFO predictions; the block actually evicts five,
        // including k0 with a new value. The worker must overwrite k0 and
        // add k4 while building the same single storage proposal.
        spec.prefetch(None, &keys(&["x", "y", "z"]));
        let batch = vec![put("k0", "changed"), put("x", "1")];
        let candidate = spec.propose(None, batch.clone(), None).unwrap();
        let expected = plain.propose(None, batch, None).unwrap();
        assert_eq!(candidate.root(), expected.root());
        assert_eq!(
            candidate.node().storage_view().get(&k("k0")).unwrap(),
            Some(k("changed"))
        );
        assert_eq!(candidate.node().storage_view().get(&k("k5")).unwrap(), None);
        assert_eq!(candidate.node().storage_view().get(&k("k6")).unwrap(), None);
        assert_eq!(faults.worker_proposals.load(Ordering::Relaxed), 1);
        assert_eq!(faults.caller_proposals.load(Ordering::Relaxed), 0);
        assert_eq!(candidate.node().steps(), 0);
        assert_eq!(spec.commit(candidate).unwrap(), plain.commit(expected).unwrap());
        assert_no_failures(&spec.reader().stats().speculation);
        spec.close().unwrap();
        plain.close().unwrap();
    }

    #[test]
    fn exact_latest_is_reused_unless_final_metadata_needs_a_proposal() {
        use crate::speculate::{Counters, SpecOp, Worker, WorkerError};

        for metadata in [None, Some(FINAL_METADATA)] {
            let faults = Faults::new(0, 0, 0);
            let engine = Flaky {
                inner: MemoryStorage::new(MemoryConfig::default()),
                faults: faults.clone(),
            };
            let base = Arc::new(engine.head().unwrap());
            let counters = Arc::new(Counters::default());
            let mut worker = Worker::<K, V, Flaky>::spawn(base, counters.clone(), 100, 2).unwrap();
            worker.block_end(vec![SpecOp::Evict(k("a"), Entry::Value(k("value")))]);
            wait_for_steps(&counters, 1);

            let finalized = worker.finish(Vec::new(), metadata).unwrap();
            assert_eq!(finalized.view.get(&k("a")).unwrap(), Some(k("value")));
            assert_eq!(finalized.steps, u32::from(metadata.is_some()));
            assert_eq!(
                faults.worker_proposals.load(Ordering::Relaxed),
                1 + u64::from(metadata.is_some())
            );
            assert_eq!(faults.caller_proposals.load(Ordering::Relaxed), 0);
            let recorded = faults.metadata.lock().unwrap();
            let expected: Vec<_> = metadata.into_iter().map(<[u8]>::to_vec).collect();
            assert_eq!(*recorded, expected);
            assert!(matches!(worker.finish(Vec::new(), None), Err(WorkerError::NoChain)));
            worker.shutdown();
        }
    }

    type Pending<P> = (
        Handle<K, V, LtHash, P, Flaky>,
        Handle<K, V, LtHash, P, MemoryStorage<K, V>>,
    );

    pub fn differential<P: EvictionPolicy<K>>(policy: impl Fn() -> P, rounds: u32, seed: u64, faults: Arc<Faults>) {
        let capacity = 24;
        let config = speculating(capacity, 4, rounds, 1, 1);
        let engine = Flaky {
            inner: MemoryStorage::new(MemoryConfig {
                revisions: config.local.engine_window(true),
                ..MemoryConfig::default()
            }),
            faults: faults.clone(),
        };
        let mut spec: AuthWal<K, V, LtHash, P, Flaky> = new_empty(config, engine, policy(), LtHash::new()).unwrap();
        let mut plain = open(capacity, 4, policy(), LtHash::new());
        let (universe, hot) = (60u64, 14u64);
        let key = |i: u64| format!("k{i:03}").into_bytes();
        let random_batch = |rng: &mut Rng, tag: u64| -> Vec<BatchOp<K, V>> {
            (0..1 + rng.below(9))
                .map(|_| {
                    let key = key(if rng.below(3) < 2 {
                        rng.below(hot)
                    } else {
                        rng.below(universe)
                    });
                    if rng.below(6) == 0 {
                        BatchOp::Delete { key }
                    } else {
                        BatchOp::Put {
                            key,
                            value: format!("v{tag}").into_bytes(),
                        }
                    }
                })
                .collect()
        };
        let mut rng = Rng(seed);
        let mut reference: BTreeMap<K, Option<V>> = BTreeMap::new();
        // The live candidate the next block builds on when pipelined.
        let mut pending: Option<Pending<P>> = None;
        for block in 0..80u64 {
            let batch = random_batch(&mut rng, block);
            let written: Vec<K> = batch.iter().filter_map(|op| op.key().cloned()).collect();
            let parent = pending.as_ref().map(|(s, _)| s);
            for chunk in written.chunks(2) {
                let mut hint = chunk.to_vec();
                if rng.below(2) == 0 {
                    hint.push(key(rng.below(universe)));
                }
                spec.prefetch(parent, &hint);
                if rng.below(3) == 0 {
                    std::thread::sleep(Duration::from_micros(300));
                }
            }
            for op in &batch {
                match op {
                    BatchOp::Put { key, value } => reference.insert(key.clone(), Some(value.clone())),
                    BatchOp::Delete { key } => reference.insert(key.clone(), None),
                    BatchOp::PrefixDelete { .. } => unreachable!(),
                };
            }
            let mut rival = None;
            if rng.below(3) == 0 {
                let handle = spec
                    .propose(parent, random_batch(&mut rng, 1000 + block), Some(FINAL_METADATA))
                    .unwrap();
                if rng.below(2) == 0 {
                    spec.prefetch(Some(&handle), &[key(rng.below(universe)), key(rng.below(hot))]);
                }
                rival = Some(handle);
            }
            let plain_parent = pending.as_ref().map(|(_, p)| p);
            let expected = plain
                .propose(plain_parent, batch.clone(), Some(FINAL_METADATA))
                .unwrap();
            let candidate = spec.propose(parent, batch, Some(FINAL_METADATA)).unwrap();
            assert_eq!(
                candidate.root(),
                expected.root(),
                "seed {seed} rounds {rounds} block {block}"
            );
            if rng.below(3) == 0 {
                spec.prefetch(parent, &[key(rng.below(universe))]);
            }
            if rng.below(2) == 0 {
                spec.prefetch(Some(&candidate), &[key(rng.below(universe)), key(rng.below(hot))]);
            }
            if rng.below(2) == 0 {
                rival = None;
            }
            if let Some((s, p)) = pending.take() {
                assert_eq!(
                    spec.commit(s).unwrap(),
                    plain.commit(p).unwrap(),
                    "seed {seed} block {block}"
                );
            }
            if rng.below(2) == 0 {
                pending = Some((candidate, expected));
            } else {
                assert_eq!(
                    spec.commit(candidate).unwrap(),
                    plain.commit(expected).unwrap(),
                    "seed {seed} block {block}"
                );
            }
            drop(rival);
            let (h1, h2) = (plain.reader().latest_revision(), spec.reader().latest_revision());
            assert_eq!(h1.root(), h2.root(), "seed {seed} block {block}");
            assert_eq!(h1.node().storage_view().root(), h2.node().storage_view().root());
            for i in 0..universe {
                let key = key(i);
                assert_eq!(
                    h2.get(&key).unwrap(),
                    h1.get(&key).unwrap(),
                    "seed {seed} block {block}, key {i}"
                );
                if pending.is_none() {
                    assert_eq!(h2.get(&key).unwrap(), reference.get(&key).cloned().flatten());
                }
            }
        }
        if let Some((s, p)) = pending.take() {
            assert_eq!(spec.commit(s).unwrap(), plain.commit(p).unwrap());
        }
        let s = spec.reader().stats().speculation;
        assert_eq!(s.worker_panics, 0, "{s:?}");
        assert_eq!(
            faults.injected.load(Ordering::Relaxed),
            s.step_failures,
            "every injected failure is counted on the worker, {s:?}"
        );
        assert!(
            s.fallbacks <= s.step_failures,
            "every fallback follows a recorded worker failure, {s:?}"
        );
        spec.close().unwrap();
        plain.close().unwrap();
    }
}

#[test]
fn speculation_matches_the_plain_path_under_engine_failures() {
    for seed in [1, 2] {
        for rounds in [1, 3] {
            let faults = faulty::Faults::new(6, 9, 5);
            faulty::differential(|| Fifo::<K>::new(12), rounds, seed, faults.clone());
            assert!(faults.injected.load(std::sync::atomic::Ordering::Relaxed) > 0);
            let faults = faulty::Faults::new(6, 9, 5);
            faulty::differential(|| LruWrite::<K>::new(20).with_trigger(20), rounds, seed, faults.clone());
            assert!(faults.injected.load(std::sync::atomic::Ordering::Relaxed) > 0);
        }
    }
}
