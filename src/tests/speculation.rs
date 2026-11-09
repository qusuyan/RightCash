//! Speculative execution compared with the plain path, including pipelining.

use super::common::*;
use crate::AuthWal;
use crate::policy::{EvictionPolicy, Fifo, LruWrite};
use crate::storage::{StorageEngine, StorageProposal};
use crate::types::{BatchOp, Error};
use crate::vc::{VectorCommitment, lthash::LtHash, merkle::Merkle, merkle_blake3::MerkleBlake3, rehash::Rehash};
use std::collections::BTreeMap;
use std::time::Duration;

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
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(12), || MerkleBlake3::new(3), 4);
}

#[test]
fn watermark_speculation_matches_the_plain_path() {
    speculation_matches_the_plain_path(|| Fifo::<K>::new(20).with_trigger(20), LtHash::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(20).with_trigger(20), LtHash::new, 4);
    speculation_matches_the_plain_path(|| Fifo::<K>::new(20).with_trigger(20), Merkle::new, 4);
    speculation_matches_the_plain_path(|| LruWrite::<K>::new(20).with_trigger(20), MerkleBlake3::default, 4);
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
    let open_firewood = |dir: &TempDir, config: &crate::Config| {
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
