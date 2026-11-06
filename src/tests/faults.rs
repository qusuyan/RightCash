//! Injected worker and storage failures, checked against uninterrupted execution.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::common::*;
use crate::AuthWal;
use crate::policy::{EvictionPolicy, Fifo, LruWrite};
use crate::proposal::Handle;
use crate::set::Entry;
use crate::storage::{MemoryConfig, MemoryStorage, StorageEngine, StorageProposal};
use crate::types::{BatchOp, Error, Hash, Result};
use crate::vc::lthash::LtHash;

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
#[test]
fn speculation_matches_the_plain_path_under_engine_failures() {
    for seed in [1, 2] {
        for rounds in [1, 3] {
            let faults = Faults::new(6, 9, 5);
            differential(|| Fifo::<K>::new(12), rounds, seed, faults.clone());
            assert!(faults.injected.load(std::sync::atomic::Ordering::Relaxed) > 0);
            let faults = Faults::new(6, 9, 5);
            differential(|| LruWrite::<K>::new(20).with_trigger(20), rounds, seed, faults.clone());
            assert!(faults.injected.load(std::sync::atomic::Ordering::Relaxed) > 0);
        }
    }
}
