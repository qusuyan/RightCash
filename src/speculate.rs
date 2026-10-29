//! Speculative eviction (design §9): predicted victims are sent to the storage
//! engine early in **steps**, storage proposals built one on the other from
//! the pinned view of the node the next block builds on, refined from
//! prefetch hints during block execution; the final proposal puts back every
//! misprediction so the storage state equals the plain path's. On a
//! state-function engine (firewood, `MemoryStorage`) nothing here is
//! consensus-visible; `AuthWal::new` rejects speculation on other engines
//! (design §9.3).
//!
//! Division of labour (R25–R28, design §9.2). The **policy** owns the
//! prediction: `hint_writes` says which entries to send early and which
//! earlier predictions to put back, `writes` reconciles at `Propose`
//! (design §6.1); its marks live in the target node's snapshot. The
//! **writer** turns the policy's answers into `SpecOp`s. The **worker**
//! owns the speculative side of the engine (clarification 24): the chain of
//! steps built on its latest view, one at a time (R17), each step the
//! merged write set of everything submitted since the last. The writer
//! reaches it over a `std::sync::mpsc` channel (clarification 25) with
//! three requests, `Submit` a batch, `Finish` a block (answered with the
//! latest view, R24) and `Reset` the chain at a retarget; the thread's
//! chain state is its own locals, so there is no lock of ours anywhere on
//! this path, and the thread's loop body is one step any thread could run
//! (design §12.5).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, channel, sync_channel};
use std::thread::JoinHandle;
use std::time::Instant;

use crate::Shared;
use crate::policy::EvictionPolicy;
use crate::proposal::StorageView;
use crate::set::Entry;
use crate::storage::StorageEngine;
use crate::types::{BatchOp, Key, Value};
use crate::vc::VectorCommitment;

/// The one encoding of entries as engine ops (`Put` for values, `Delete` for
/// tombstones, in the given order), used for the plain proposal's batch, the
/// steps' evictions and the final proposal.
pub(crate) fn entries_to_ops<K: Key, V: Value>(entries: impl IntoIterator<Item = (K, Entry<V>)>) -> Vec<BatchOp<K, V>> {
    entries
        .into_iter()
        .map(|(key, entry)| match entry {
            Entry::Value(value) => BatchOp::Put { key, value },
            Entry::Tombstone => BatchOp::Delete { key },
        })
        .collect()
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

/// What `Finish` answers: the latest view and the chain's step count, or
/// nothing (no step built, a step failed, the thread gone).
type Finished<K, V, S> = Option<(StorageView<K, V, S>, u32)>;

/// A request to the worker (design §9.2, clarification 24), sent down the
/// channel by the writer.
enum Request<K, V, S>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    /// A batch of ops for the chain; queued, never waited for.
    Submit(Vec<SpecOp<K, V>>),
    /// A retarget: a fresh chain on this base, the new target's pinned view.
    Reset(StorageView<K, V, S>),
    /// Block end: build the tail, then answer on the reply channel.
    Finish(SyncSender<Finished<K, V, S>>),
}

/// The writer's interface to the worker (design §9.2, clarifications 24–25):
/// `submit` a batch of ops, `finish` a block and take the latest view,
/// `reset` the chain at a retarget, `shutdown` at `close`. It holds the
/// channel's sender and the thread's join handle; the thread holds a strong
/// reference to the shared state (counters) and exits when the sender is
/// dropped, so it never keeps an instance alive on its own, and its chain,
/// the only speculative engine handles there are, drops with it.
pub(crate) struct Worker<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// `None` after `shutdown`: dropping the sender ends the thread's loop.
    tx: Option<Sender<Request<K, V, S>>>,
    /// Taken by `shutdown` (`&mut self`: the writer owns the worker).
    handle: Option<JoinHandle<()>>,
    shared: Arc<Shared<K, V, C, P, S>>,
}

impl<K, V, C, P, S> Worker<K, V, C, P, S>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    /// Spawns the thread with its reference to the shared state, which
    /// `AuthWal::new` builds first (no attach step, design §10), and the two
    /// local knobs it batches by. A spawn failure is an `io::Error` the
    /// caller maps to `Error::WorkerUnavailable`.
    pub(crate) fn spawn(shared: Arc<Shared<K, V, C, P, S>>, min_step: usize, rounds: u32) -> std::io::Result<Self> {
        let (tx, rx) = channel::<Request<K, V, S>>();
        let thread_shared = shared.clone();
        let handle = std::thread::Builder::new()
            .name("auth_wal-worker".to_owned())
            .spawn(move || run(rx, thread_shared, min_step, rounds))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            shared,
        })
    }

    /// Sends a request; `false` when the thread is gone (its receiver
    /// dropped, by `shutdown` or by the thread's death), which ends
    /// speculation for the instance's lifetime without affecting correctness.
    fn send(&self, request: Request<K, V, S>) -> bool {
        match &self.tx {
            Some(tx) => tx.send(request).is_ok(),
            None => false,
        }
    }

    /// A retarget (design §9.2 "Target"): a fresh chain on `base`, the new
    /// target's pinned view. The thread drops the old chain and the ops
    /// queued for it (`ops_dropped`), counting its steps as `steps_dropped`
    /// when no `finish` read them, in request order, so a step it is still
    /// building when the reset arrives is dropped right after. No wait; a
    /// no-op once the thread is gone.
    pub(crate) fn reset(&self, base: StorageView<K, V, S>) {
        self.send(Request::Reset(base));
    }

    /// Queues a batch of ops for the chain (design §9.2); no wait. Dropped
    /// and counted `ops_dropped` when the thread is gone (the thread itself
    /// drops a batch that arrives with no chain to build on: no reset yet,
    /// or a step failed), after which `finish` answers nothing and `Propose`
    /// takes the plain path.
    pub(crate) fn submit(&self, ops: Vec<SpecOp<K, V>>) {
        let n = ops.len() as u64;
        if !self.send(Request::Submit(ops)) {
            self.shared.counters.ops_dropped.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Block end (design §9.2, R24): sends `Finish` and blocks on its reply,
    /// which the thread sends once every request queued before it has been
    /// handled and the tail, below `min_step` or past the eager cap
    /// included, has been built into one last step (at most `rounds` in
    /// all): the latest view with the chain's step count, the base `Propose`
    /// builds the final proposal on; or `None` when no step was built on
    /// this chain (nothing was submitted: the plain path, no extra engine
    /// commit for a quiet block), a step failed, or the thread is gone (the
    /// reply channel then closes). The wait is bounded by the step being
    /// built plus the tail.
    pub(crate) fn finish(&self) -> Finished<K, V, S> {
        let (reply, wait) = sync_channel(1);
        if !self.send(Request::Finish(reply)) {
            return None;
        }
        wait.recv().ok().flatten()
    }

    /// `close` (design §3, §9.2): drops the sender, which ends the thread's
    /// loop once it has handled what was queued (a `Submit` is built only
    /// if a `Finish` follows it, so nothing is hashed for nobody; the step
    /// being built finishes its engine call), and joins it; the thread's
    /// chain, the only speculative engine handles there were, drops with it,
    /// so the engine can close (design §10 item 6). A thread that died
    /// earlier is simply joined; a later `submit`, `reset` or `finish` is
    /// refused. Idempotent.
    pub(crate) fn shutdown(&mut self) {
        self.tx = None;
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The worker thread (design §9.2, clarification 24). Each turn: block for
/// one request (the sender dropped ends the loop), drain every request
/// queued behind it, and handle them in order: a `Submit` joins the batch
/// (or is dropped when there is no chain, `ops_dropped`), a `Reset` drops
/// the chain and the batch and starts afresh on its base, a `Finish` is
/// noted. Then, if ops are waiting and a `Finish` is noted or at least
/// `min_step` ops wait and fewer than `rounds - 1` steps were built on this
/// chain (the finishing step is the last of `rounds`), merge the batch into
/// one write set and build a step on the latest view; the result becomes
/// the latest view, a failed or panicking step leaves none (its steps count
/// `steps_dropped`; later batches are dropped and the next `Finish` answers
/// nothing). Finally answer the noted `Finish` with the latest view and the
/// step count, or nothing. The chain state is the thread's own: no lock of
/// ours on this path, and a reset received while a step was being built is
/// applied right after it, dropping the result.
fn run<K, V, C, P, S>(rx: Receiver<Request<K, V, S>>, shared: Arc<Shared<K, V, C, P, S>>, min_step: usize, rounds: u32)
where
    K: Key,
    V: Value,
    C: VectorCommitment<K, V>,
    P: EvictionPolicy<K>,
    S: StorageEngine<K, V>,
{
    let counters = &shared.counters;
    let mut base: Option<StorageView<K, V, S>> = None;
    let mut latest: Option<StorageView<K, V, S>> = None;
    let mut steps: u32 = 0;
    let mut used = false;
    let mut batch: Vec<SpecOp<K, V>> = Vec::new();
    loop {
        // 1. Requests: block for one, drain the rest.
        let Ok(first) = rx.recv() else { break };
        let mut finish = None;
        for request in std::iter::once(first).chain(rx.try_iter()) {
            match request {
                Request::Submit(ops) => {
                    if latest.is_some() {
                        batch.extend(ops);
                    } else {
                        counters.ops_dropped.fetch_add(ops.len() as u64, Ordering::Relaxed);
                    }
                }
                Request::Reset(view) => {
                    counters.ops_dropped.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    batch.clear();
                    if steps > 0 && !used {
                        counters.steps_dropped.fetch_add(u64::from(steps), Ordering::Relaxed);
                    }
                    steps = 0;
                    used = false;
                    base = Some(view.clone());
                    latest = Some(view);
                }
                Request::Finish(reply) => finish = Some(reply),
            }
        }
        // 2. A step, when one is due.
        if !batch.is_empty() {
            let eager = batch.len() >= min_step && steps + 1 < rounds;
            if let (Some(b), Some(view), true) = (&base, &latest, finish.is_some() || eager) {
                let ops = merge(std::mem::take(&mut batch));
                let started = Instant::now();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    build_step::<K, V, S>(counters, b, view, ops)
                }));
                counters
                    .worker_time_ns
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if outcome.is_err() {
                    counters.worker_panics.fetch_add(1, Ordering::Relaxed);
                    log::error!("auth_wal worker: a step panicked; the chain is dropped, continuing");
                }
                match outcome.ok().flatten() {
                    Some(next) => {
                        latest = Some(next);
                        steps += 1;
                    }
                    None => {
                        counters.steps_dropped.fetch_add(u64::from(steps), Ordering::Relaxed);
                        steps = 0;
                        latest = None;
                    }
                }
            }
        }
        // 3. The answer.
        if let Some(reply) = finish {
            let answer = if steps > 0 {
                latest.clone().map(|view| (view, steps))
            } else {
                None
            };
            if answer.is_some() {
                used = true;
            }
            let _ = reply.send(answer);
        }
    }
}

/// The combined write set of one step (design §9.2, clarification 24): the
/// batch folded by key, the last op for a key winning (an eviction then a
/// retraction of the same key is one retraction, the reverse one eviction),
/// in key order.
fn merge<K: Key, V: Value>(ops: Vec<SpecOp<K, V>>) -> Vec<SpecOp<K, V>> {
    let mut merged: BTreeMap<K, SpecOp<K, V>> = BTreeMap::new();
    for op in ops {
        merged.insert(op.key().clone(), op);
    }
    merged.into_values().collect()
}

/// One step (design §9.2), on the worker thread: `Evict` ops become
/// `Put`/`Delete` of their entry (`entries_to_ops`), `Retract` ops read the
/// key through `base` (the target's pinned view: the value from before any
/// step, design §9.1) and become `Put(old)` or `Delete`, counted
/// `retractions`; the batch is proposed with no metadata on `view` (design
/// §10 item 4: a committed handle makes the before-and-after root check, a
/// proposal handle chains). `Some(step)` counts `steps_built` and is the new
/// latest view; a failed read or a failed propose logs the message, counts
/// `step_failures` and returns `None`. Errors are never returned: hints are
/// advisory.
fn build_step<K, V, S>(
    counters: &Counters,
    base: &StorageView<K, V, S>,
    view: &StorageView<K, V, S>,
    ops: Vec<SpecOp<K, V>>,
) -> Option<StorageView<K, V, S>>
where
    K: Key,
    V: Value,
    S: StorageEngine<K, V>,
{
    todo!("design §9.2 build_step")
}

/// Snapshot of the counters (design §9.5).
#[derive(Clone, Copy, Debug, Default)]
pub struct SpeculationStats {
    // per proposal, counted at `Propose` (a losing candidate counts too:
    // competing candidates are rare on Avalanche and absent in the benchmark)
    pub plain: u64,
    pub speculative: u64,
    /// Steps the final proposals chained on.
    pub steps: u64,
    /// Victims omitted from the final proposals: a step had written them
    /// (design §9.1).
    pub reused_ops: u64,
    /// Keys the final proposals put back (design §9.1).
    pub reverted_ops: u64,
    // per instance
    /// Steps the worker built (design §9.5).
    pub steps_built: u64,
    /// Built steps no `finish` read: the chain was reset before a `Propose`
    /// on its target, or a step failed after them (design §9.5).
    pub steps_dropped: u64,
    /// Step proposals the engine rejected, or a retraction's read that failed
    /// (design §9.2).
    pub step_failures: u64,
    /// Retract ops the steps carried: predictions withdrawn by a later hint
    /// (design §9.2).
    pub retractions: u64,
    /// Ops dropped unbuilt: submitted with no chain to build on (no reset
    /// yet, or a step failed), dropped by a reset, or refused because the
    /// thread is gone (design §9.2).
    pub ops_dropped: u64,
    /// `Propose` fell back to the plain path after a retraction's read or
    /// `F`'s own proposal failed (design §9.1).
    pub fallbacks: u64,
    /// Every key of every hint, duplicates included (design §9.5).
    pub hint_keys: u64,
    /// Cumulative wall time per step on the worker thread (design §9.2,
    /// §9.5, §11, R17): work off every caller's path; includes time the step
    /// spent waiting on engine locks.
    pub worker_time_ns: u64,
    /// Steps that panicked on the worker; caught, logged (no latest view
    /// remains), speculation continues.
    pub worker_panics: u64,
}

/// Lock-free counters behind [`SpeculationStats`], a field of the shared
/// state, so the writer, the worker and `stats()` can all touch them.
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
}

impl Counters {
    pub(crate) fn snapshot(&self) -> SpeculationStats {
        todo!("one Relaxed load per field")
    }
}
