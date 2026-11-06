//! Eviction rules, watermark predictions, and hint reconciliation.

use super::common::*;
use crate::policy::{EvictionPolicy, Fifo, LruWrite, Prediction};
use crate::storage::StorageProposal;
use crate::vc::rehash::Rehash;
use std::collections::BTreeSet;
use std::time::Duration;

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
