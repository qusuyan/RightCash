//! Speculative eviction (design §9): predicted victims are sent to the storage
//! engine early in **steps**, storage proposals built one on the other from
//! the pinned view of the node the next block builds on, refined from
//! prefetch hints during block execution; the final proposal puts back every
//! misprediction so the storage state equals the plain path's. On a
//! state-function engine (firewood, `MemoryStorage`) nothing here is
//! consensus-visible; `AuthWal::new` rejects speculation on other engines
//! (design §9.3).
//!
//! The policy chooses predictions and final corrections; the writer supplies
//! their values. The worker owns the pending map, base and latest storage
//! views. Submit refines predictions, Reset starts a new chain, and Finish
//! merges the final correction before returning its reconciled view. Finish
//! consumes the chain, so competing candidates cannot reuse its corrections.
//! Non-evicting blocks only flush predictions and keep the chain alive.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::proposal::StorageView;
use crate::set::Entry;
use crate::storage::{StorageEngine, StorageProposal};
use crate::types::{BatchOp, Error, Key, Result, Value};

/// The one encoding of entries as engine ops (`Put` for values, `Delete` for
/// tombstones, in the given order), used for the plain proposal's batch, the
/// steps' evictions and the final proposal.
pub(crate) fn entries_to_ops<K: Key, V: Value>(entries: impl IntoIterator<Item = (K, Entry<V>)>) -> Vec<BatchOp<K, V>> {
    entries.into_iter().map(|(key, entry)| entry_op(key, entry)).collect()
}

/// One entry as an engine op: `Put` for a value, `Delete` for a tombstone.
fn entry_op<K: Key, V: Value>(key: K, entry: Entry<V>) -> BatchOp<K, V> {
    match entry {
        Entry::Value(value) => BatchOp::Put { key, value },
        Entry::Tombstone => BatchOp::Delete { key },
    }
}

/// Read owned `key` through borrowed storage `view` and return an operation
/// restoring that view's value: `Put` when present, `Delete` when absent.
/// Propagate any read error. This constructs an operation; it does not apply it.
/// The caller must supply the chain base, before speculative changes.
fn put_back<K: Key, V: Value, P: StorageProposal<K, V>>(view: &P, key: K) -> Result<BatchOp<K, V>> {
    Ok(match view.get(&key)? {
        Some(value) => BatchOp::Put { key, value },
        None => BatchOp::Delete { key },
    })
}

/// One op of a step (design §9.2): the policy's answer to a hint.
pub(crate) enum SpecOp<K, V> {
    /// Write the entry to storage early: a predicted victim with its entry
    /// in the target's set.
    Evict(K, Entry<V>),
    /// Put back what the target's pinned view holds for the key, or delete
    /// it: a prediction the policy withdrew (design §9.2).
    Retract(K),
}

impl<K, V> SpecOp<K, V> {
    fn key(&self) -> &K {
        match self {
            SpecOp::Evict(key, _) | SpecOp::Retract(key) => key,
        }
    }
}

/// A failure that prevents reuse of speculative work.
#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum WorkerError {
    #[error("speculative storage operation failed: {0}")]
    Storage(Arc<Error>),
    #[error("speculative storage operation panicked")]
    Panicked,
    #[error("speculation worker disconnected")]
    Disconnected,
    #[error("speculative chain was already finalized; reset before reuse")]
    NoChain,
}

/// Final storage work, measured on the worker and reported within Propose.
#[derive(Default)]
pub(crate) struct StorageTimings {
    pub(crate) reads: Duration,
    pub(crate) storage: Duration,
    pub(crate) ops: usize,
}

/// A fully reconciled view ready for the candidate node. `steps` counts its
/// speculative ancestors, excluding the returned proposal itself.
pub(crate) struct Finalized<K: Key, V: Value, S: StorageEngine<K, V>> {
    pub(crate) view: StorageView<K, V, S>,
    pub(crate) steps: u32,
    pub(crate) timings: StorageTimings,
}

type Finished<K, V, S> = std::result::Result<Finalized<K, V, S>, WorkerError>;

/// Ordered requests from the sole writer. Finish is a terminal barrier:
/// corrections and metadata belong to one candidate, never its siblings.
enum Request<K: Key, V: Value, S: StorageEngine<K, V>> {
    Submit {
        ops: Vec<SpecOp<K, V>>,
        flush: bool,
    },
    Reset(StorageView<K, V, S>),
    Finish {
        ops: Vec<SpecOp<K, V>>,
        metadata: Option<Vec<u8>>,
        reply: SyncSender<Finished<K, V, S>>,
    },
}

/// The writer only queues work or waits for finalization. All speculative
/// merging, restoration reads and storage proposals belong to the worker.
pub(crate) struct Worker<K: Key, V: Value, S: StorageEngine<K, V>> {
    tx: Option<Sender<Request<K, V, S>>>,
    handle: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
}

impl<K: Key, V: Value, S: StorageEngine<K, V>> Worker<K, V, S> {
    /// Start on the initial target's storage view. `rounds` caps all proposals
    /// on a chain, reserving one slot for its final refinement. Both limits
    /// must be positive (validated by AuthWal::new).
    pub(crate) fn spawn(
        base: StorageView<K, V, S>,
        counters: Arc<Counters>,
        min_step: usize,
        rounds: u32,
    ) -> std::io::Result<Self> {
        let (tx, rx) = channel();
        let thread_counters = counters.clone();
        let handle = std::thread::Builder::new()
            .name("auth_wal-worker".to_owned())
            .spawn(move || run(base, rx, thread_counters, min_step, rounds))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            counters,
        })
    }

    fn send(&self, request: Request<K, V, S>) -> bool {
        self.tx.as_ref().is_some_and(|tx| tx.send(request).is_ok())
    }

    /// Start a fresh chain, dropping old predictions and any recorded error.
    /// Does not wait for engine work; requests are processed in order.
    pub(crate) fn reset(&self, base: StorageView<K, V, S>) {
        self.send(Request::Reset(base));
    }

    /// Queue hints without waiting. Merging and releasing overwritten values
    /// happen on the worker, even after the eager step budget is exhausted.
    pub(crate) fn submit(&self, ops: Vec<SpecOp<K, V>>) {
        if !ops.is_empty() {
            self.enqueue(ops, false);
        }
    }

    /// Reconcile a non-evicting block and keep the chain, without waiting.
    /// A flush may build below min_step, but cannot use the final slot.
    pub(crate) fn block_end(&self, ops: Vec<SpecOp<K, V>>) {
        self.enqueue(ops, true);
    }

    fn enqueue(&self, ops: Vec<SpecOp<K, V>>, flush: bool) {
        let n = ops.len() as u64;
        if !self.send(Request::Submit { ops, flush }) {
            self.counters.ops_dropped.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Merge the final refinement into pending hints and return the exact
    /// final view. At most one more proposal is built, including metadata.
    /// With no pending ops and no metadata, reuse latest directly. Consumes
    /// the chain on success or failure; reset is required before further work.
    pub(crate) fn finish(&self, ops: Vec<SpecOp<K, V>>, metadata: Option<&[u8]>) -> Finished<K, V, S> {
        let (reply, wait) = sync_channel(1);
        let n = ops.len() as u64;
        if !self.send(Request::Finish {
            ops,
            metadata: metadata.map(<[u8]>::to_vec),
            reply,
        }) {
            self.counters.ops_dropped.fetch_add(n, Ordering::Relaxed);
            return Err(WorkerError::Disconnected);
        }
        wait.recv().unwrap_or(Err(WorkerError::Disconnected))
    }

    /// Drain queued requests and release speculative handles before storage
    /// closes. Safe to call repeatedly; ignores a panic returned by join.
    pub(crate) fn shutdown(&mut self) {
        self.tx = None;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Worker-owned prediction state. The base is unchanged across kept blocks;
/// retractions always restore it, never a speculatively modified latest view.
struct Chain<K: Key, V: Value, S: StorageEngine<K, V>> {
    base: StorageView<K, V, S>,
    latest: StorageView<K, V, S>,
    status: std::result::Result<(), WorkerError>,
    steps: u32,
    batch: BTreeMap<K, SpecOp<K, V>>,
}

impl<K: Key, V: Value, S: StorageEngine<K, V>> Chain<K, V, S> {
    fn new(base: StorageView<K, V, S>) -> Self {
        Self {
            latest: base.clone(),
            base,
            status: Ok(()),
            steps: 0,
            batch: BTreeMap::new(),
        }
    }

    fn merge(&mut self, ops: Vec<SpecOp<K, V>>, counters: &Counters) {
        if self.status.is_err() {
            counters.ops_dropped.fetch_add(ops.len() as u64, Ordering::Relaxed);
            return;
        }
        for op in ops {
            self.batch.insert(op.key().clone(), op);
        }
    }

    fn discard(&self, counters: &Counters) {
        counters
            .ops_dropped
            .fetch_add(self.batch.len() as u64, Ordering::Relaxed);
        counters
            .steps_dropped
            .fetch_add(u64::from(self.steps), Ordering::Relaxed);
    }

    /// Build once, with the same restoration and failure handling for eager
    /// predictions and final refinements. A failed chain stays unusable.
    fn build(
        &mut self,
        metadata: Option<&[u8]>,
        counters: &Counters,
        tail: bool,
    ) -> std::result::Result<StorageTimings, WorkerError> {
        self.status.clone()?;
        let ops: Vec<_> = std::mem::take(&mut self.batch).into_values().collect();
        counters.step_ops.fetch_add(ops.len() as u64, Ordering::Relaxed);
        let started = Instant::now();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            build_step::<K, V, S>(counters, &self.base, &self.latest, ops, metadata)
        }))
        .map_err(|_| {
            counters.worker_panics.fetch_add(1, Ordering::Relaxed);
            log::error!("auth_wal worker: a proposal panicked; the chain is disabled until reset");
            WorkerError::Panicked
        })
        .and_then(|result| result.map_err(|error| WorkerError::Storage(Arc::new(error))));
        let elapsed = started.elapsed().as_nanos() as u64;
        counters.worker_time_ns.fetch_add(elapsed, Ordering::Relaxed);
        if tail {
            counters.tail_steps.fetch_add(1, Ordering::Relaxed);
            counters.tail_time_ns.fetch_add(elapsed, Ordering::Relaxed);
        }
        match outcome {
            Ok((view, timings)) => {
                self.latest = view;
                self.steps += 1;
                Ok(timings)
            }
            Err(error) => {
                self.status = Err(error.clone());
                Err(error)
            }
        }
    }

    fn finish(mut self, ops: Vec<SpecOp<K, V>>, metadata: Option<&[u8]>, counters: &Counters) -> Finished<K, V, S> {
        self.merge(ops, counters);
        let result = if !self.batch.is_empty() || metadata.is_some() || self.steps == 0 {
            self.build(metadata, counters, true)
        } else {
            self.status.clone().map(|()| StorageTimings::default())
        };
        match result {
            Ok(timings) => Ok(Finalized {
                view: self.latest,
                steps: self.steps - 1,
                timings,
            }),
            Err(error) => {
                self.discard(counters);
                Err(error)
            }
        }
    }
}

/// Drain requests in order and coalesce hints before eager work. A Finish
/// stops the drain, consumes the chain, and answers before later requests.
/// Only a subsequent Reset makes the worker ready for another candidate.
fn run<K: Key, V: Value, S: StorageEngine<K, V>>(
    base: StorageView<K, V, S>,
    rx: Receiver<Request<K, V, S>>,
    counters: Arc<Counters>,
    min_step: usize,
    rounds: u32,
) {
    let counters = &*counters;
    let mut chain = Some(Chain::<K, V, S>::new(base));
    while let Ok(first) = rx.recv() {
        let mut flush = false;
        for request in std::iter::once(first).chain(rx.try_iter()) {
            match request {
                Request::Submit { ops, flush: block_end } => {
                    if let Some(chain) = &mut chain {
                        chain.merge(ops, counters);
                        flush |= block_end;
                    } else {
                        counters.ops_dropped.fetch_add(ops.len() as u64, Ordering::Relaxed);
                    }
                }
                Request::Reset(base) => {
                    if let Some(old) = chain.replace(Chain::new(base)) {
                        old.discard(counters);
                    }
                    flush = false;
                }
                Request::Finish { ops, metadata, reply } => {
                    let result = match chain.take() {
                        Some(chain) => chain.finish(ops, metadata.as_deref(), counters),
                        None => {
                            counters.ops_dropped.fetch_add(ops.len() as u64, Ordering::Relaxed);
                            Err(WorkerError::NoChain)
                        }
                    };
                    let _ = reply.send(result);
                    break;
                }
            }
        }
        if let Some(chain) = &mut chain
            && chain.status.is_ok()
            && chain.steps < rounds - 1
            && !chain.batch.is_empty()
            && (flush || chain.batch.len() >= min_step)
        {
            let _ = chain.build(None, counters, false);
        }
    }
}

/// Build one storage proposal. Hint withdrawals and final corrections use the
/// same base reads; metadata is supplied only by finalization.
fn build_step<K: Key, V: Value, S: StorageEngine<K, V>>(
    counters: &Counters,
    base: &StorageView<K, V, S>,
    view: &StorageView<K, V, S>,
    ops: Vec<SpecOp<K, V>>,
    metadata: Option<&[u8]>,
) -> Result<(StorageView<K, V, S>, StorageTimings)> {
    let mut timings = StorageTimings {
        ops: ops.len(),
        ..StorageTimings::default()
    };
    let mut batch = Vec::with_capacity(ops.len());
    let started = Instant::now();
    for op in ops {
        match op {
            SpecOp::Evict(key, entry) => batch.push(entry_op(key, entry)),
            SpecOp::Retract(key) => {
                counters.retractions.fetch_add(1, Ordering::Relaxed);
                match put_back(&**base, key) {
                    Ok(op) => batch.push(op),
                    Err(e) => {
                        counters.step_failures.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                }
            }
        }
    }
    timings.reads = started.elapsed();
    let started = Instant::now();
    let proposal = view.propose(&batch, metadata);
    timings.storage = started.elapsed();
    match proposal {
        Ok(step) => {
            counters.steps_built.fetch_add(1, Ordering::Relaxed);
            Ok((Arc::new(step), timings))
        }
        Err(Error::ProposalOrphaned) => {
            counters.ops_dropped.fetch_add(batch.len() as u64, Ordering::Relaxed);
            Err(Error::ProposalOrphaned)
        }
        Err(e) => {
            counters.step_failures.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
    }
}

/// Snapshot of the counters (design §9.5).
#[derive(Clone, Copy, Debug, Default)]
pub struct SpeculationStats {
    // per proposal, counted at `Propose` (a losing candidate counts too:
    // competing candidates are rare on Avalanche and absent in the benchmark)
    pub plain: u64,
    pub speculative: u64,
    /// Ancestor proposals beneath the returned final views (excluding each final view).
    pub steps: u64,
    /// Victims supplied by the prediction stream rather than the final
    /// refinement. They may still be pending when finalization merges them.
    pub reused_ops: u64,
    /// Keys the final proposals put back (design §9.1).
    pub reverted_ops: u64,
    // per instance
    /// Storage proposals the worker built, including final refinements (design §9.5).
    pub steps_built: u64,
    /// Unused proposals discarded on reset or failed finalization.
    /// Shutdown does not count remaining proposals.
    pub steps_dropped: u64,
    /// Step proposals the engine rejected, or a retraction's read that failed
    /// (design §9.2); not a step on a line the engine had committed past,
    /// which counts its ops as `ops_dropped`.
    pub step_failures: u64,
    /// Retractions processed by worker proposals, from hints or final
    /// reconciliation (design §9.2).
    pub retractions: u64,
    /// Ops dropped unbuilt: submitted after a step failed and before reset,
    /// dropped by a reset, built on a line the
    /// engine had committed past (`ProposalOrphaned`, a reset follows), or
    /// refused because the thread is gone (design §9.2).
    pub ops_dropped: u64,
    /// Finalizations that failed and required the plain path (design §9.1),
    /// including earlier step errors, caught panics and disconnection.
    pub fallbacks: u64,
    /// Every key of every hint that reached the target's policy, duplicates
    /// included; an ignored hint counts nothing (design §9.5).
    pub hint_keys: u64,
    /// Cumulative wall time per step on the worker thread (design §9.2,
    /// §9.5, §11, R17), including engine-lock waits. Finalization runs on
    /// this thread too, while Propose waits for its result.
    pub worker_time_ns: u64,
    /// Worker proposals that panicked; caught and logged. The chain cannot
    /// be reused, but the thread stays alive and accepts a reset.
    pub worker_panics: u64,
    /// Final proposals attempted by Finish (design §9.2), excluding reuse
    /// of an already exact latest view, and their
    /// cumulative wall time; the rest of `worker_time_ns` is eager steps.
    pub tail_steps: u64,
    pub tail_time_ns: u64,
    /// Ops the steps carried after the merge (the engine ops of every step
    /// built, failed ones included), for the mean step size.
    pub step_ops: u64,
    /// Blocks that evicted nothing on the target and kept the chain (R35,
    /// design §9.1): no wait, the parent's view inherited, no reset, the
    /// node the target from then on.
    pub chains_kept: u64,
}

impl SpeculationStats {
    /// The counters' growth since `base`, saturating (design §9.5, §11).
    pub fn since(&self, base: &Self) -> Self {
        let d = |now: u64, before: u64| now.saturating_sub(before);
        Self {
            plain: d(self.plain, base.plain),
            speculative: d(self.speculative, base.speculative),
            steps: d(self.steps, base.steps),
            reused_ops: d(self.reused_ops, base.reused_ops),
            reverted_ops: d(self.reverted_ops, base.reverted_ops),
            steps_built: d(self.steps_built, base.steps_built),
            steps_dropped: d(self.steps_dropped, base.steps_dropped),
            step_failures: d(self.step_failures, base.step_failures),
            retractions: d(self.retractions, base.retractions),
            ops_dropped: d(self.ops_dropped, base.ops_dropped),
            fallbacks: d(self.fallbacks, base.fallbacks),
            hint_keys: d(self.hint_keys, base.hint_keys),
            worker_time_ns: d(self.worker_time_ns, base.worker_time_ns),
            worker_panics: d(self.worker_panics, base.worker_panics),
            tail_steps: d(self.tail_steps, base.tail_steps),
            tail_time_ns: d(self.tail_time_ns, base.tail_time_ns),
            step_ops: d(self.step_ops, base.step_ops),
            chains_kept: d(self.chains_kept, base.chains_kept),
        }
    }
}

/// Lock-free counters behind [`SpeculationStats`], behind an `Arc` on the
/// shared state and in the worker, so the writer, the worker thread and
/// `stats()` can all touch them.
#[derive(Default)]
pub(crate) struct Counters {
    pub(crate) plain: AtomicU64,
    pub(crate) speculative: AtomicU64,
    pub(crate) steps: AtomicU64,
    pub(crate) reused_ops: AtomicU64,
    pub(crate) reverted_ops: AtomicU64,
    pub(crate) steps_built: AtomicU64,
    pub(crate) steps_dropped: AtomicU64,
    pub(crate) step_failures: AtomicU64,
    pub(crate) retractions: AtomicU64,
    pub(crate) ops_dropped: AtomicU64,
    pub(crate) fallbacks: AtomicU64,
    pub(crate) hint_keys: AtomicU64,
    pub(crate) worker_time_ns: AtomicU64,
    pub(crate) worker_panics: AtomicU64,
    pub(crate) tail_steps: AtomicU64,
    pub(crate) tail_time_ns: AtomicU64,
    pub(crate) step_ops: AtomicU64,
    pub(crate) chains_kept: AtomicU64,
}

impl Counters {
    pub(crate) fn snapshot(&self) -> SpeculationStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        SpeculationStats {
            plain: load(&self.plain),
            speculative: load(&self.speculative),
            steps: load(&self.steps),
            reused_ops: load(&self.reused_ops),
            reverted_ops: load(&self.reverted_ops),
            steps_built: load(&self.steps_built),
            steps_dropped: load(&self.steps_dropped),
            step_failures: load(&self.step_failures),
            retractions: load(&self.retractions),
            ops_dropped: load(&self.ops_dropped),
            fallbacks: load(&self.fallbacks),
            hint_keys: load(&self.hint_keys),
            worker_time_ns: load(&self.worker_time_ns),
            worker_panics: load(&self.worker_panics),
            tail_steps: load(&self.tail_steps),
            tail_time_ns: load(&self.tail_time_ns),
            step_ops: load(&self.step_ops),
            chains_kept: load(&self.chains_kept),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MemoryConfig, MemoryStorage};
    use std::time::Duration;

    #[test]
    fn finish_is_a_barrier_before_a_queued_reset() {
        type S = MemoryStorage<Vec<u8>, Vec<u8>>;
        let storage = S::new(MemoryConfig::default());
        let base = Arc::new(storage.head().unwrap());
        let reset_base = Arc::new(
            base.propose(
                &[BatchOp::Put {
                    key: b"a".to_vec(),
                    value: b"reset".to_vec(),
                }],
                None,
            )
            .unwrap(),
        );
        let counters = Arc::new(Counters::default());
        let (tx, rx) = channel();
        let (first_reply, first_wait) = sync_channel(1);
        let (second_reply, second_wait) = sync_channel(1);
        // Queue everything before running the worker: draining past Finish
        // would mix the two candidates or answer from the wrong base.
        tx.send(Request::Submit {
            ops: vec![SpecOp::Evict(b"a".to_vec(), Entry::Value(b"hint".to_vec()))],
            flush: false,
        })
        .unwrap();
        tx.send(Request::Finish {
            ops: vec![SpecOp::Evict(b"a".to_vec(), Entry::Value(b"final".to_vec()))],
            metadata: None,
            reply: first_reply,
        })
        .unwrap();
        tx.send(Request::Reset(reset_base)).unwrap();
        tx.send(Request::Finish {
            ops: Vec::new(),
            metadata: None,
            reply: second_reply,
        })
        .unwrap();
        drop(tx);
        run::<Vec<u8>, Vec<u8>, S>(base, rx, counters.clone(), 1, 1);
        let first = first_wait.recv().unwrap().unwrap();
        let second = second_wait.recv().unwrap().unwrap();
        assert_eq!(first.view.get(&b"a".to_vec()).unwrap(), Some(b"final".to_vec()));
        assert_eq!(second.view.get(&b"a".to_vec()).unwrap(), Some(b"reset".to_vec()));
        assert_eq!((first.steps, second.steps), (0, 0));
        assert_eq!(counters.steps_built.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn worker_releases_superseded_values_without_building_a_step() {
        type V = Arc<[u8]>;
        type S = MemoryStorage<Vec<u8>, V>;
        let storage = S::new(MemoryConfig::default());
        let counters = Arc::new(Counters::default());
        // With one round, only a blocking finish may build a step. Kept
        // blocks must still release obsolete pending values on the worker.
        let mut worker =
            Worker::<Vec<u8>, V, S>::spawn(Arc::new(storage.head().unwrap()), counters.clone(), 1, 1).unwrap();
        let old: V = vec![1; 1024 * 1024].into();
        let old_ref = Arc::downgrade(&old);
        worker.submit(vec![SpecOp::Evict(b"a".to_vec(), Entry::Value(old))]);
        worker.block_end(Vec::new());
        let final_value: V = b"latest".as_slice().into();
        worker.submit(vec![
            SpecOp::Retract(b"a".to_vec()),
            SpecOp::Evict(b"a".to_vec(), Entry::Value(final_value.clone())),
            SpecOp::Evict(b"b".to_vec(), Entry::Value(final_value.clone())),
            SpecOp::Retract(b"b".to_vec()),
        ]);
        worker.block_end(Vec::new());
        let deadline = Instant::now() + Duration::from_secs(5);
        while old_ref.strong_count() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(old_ref.strong_count(), 0, "pending history survived the worker's drain");
        assert_eq!(counters.steps_built.load(Ordering::Relaxed), 0);
        let finalized = worker.finish(Vec::new(), None).unwrap();
        assert_eq!(finalized.steps, 0);
        let view = finalized.view;
        assert_eq!(view.get(&b"a".to_vec()).unwrap(), Some(final_value));
        assert_eq!(view.get(&b"b".to_vec()).unwrap(), None);
        worker.shutdown();
    }
}
