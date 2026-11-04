# auth_wal: project description (as given)

Recorded 2026-10-25. This is the problem statement as provided by the project
owner; `01-design.md` is the design derived from it. When the two disagree,
this file is the requirement and the design is what needs fixing.

## Context

`auth_wal/` is an **authenticated set of recent writes**. It sits in the middle
of the blockchain validators (`Avax-pEVM/` and `libevm/`) and the storage layer
(`qmdb-ffi/` or `firewood/`).

## Statement

> `auth_wal/` will use the same interface as `qmdb-ffi/` and `firewood/` so that
> the validators do not require additional code changes. The idea is simple: we
> keep a set of recently written key-value pairs in `auth_wal/`. If the
> validator writes a key, it first adds the value into the set. If the key is
> already in the set, it simply updates the value. When the set is full, some
> key-value pairs will be evicted and written to the storage layer. Your design
> should make it generic to choose which keys are evicted. The set size is
> determined by a parameter. In the future, we might want to make it
> automatically grow/shrink based on workload. Because the set contains states,
> all reads will first query entries in the set before forwarding to the storage
> engine. To allow authenticated reads, this set also needs to be authenticated
> using vector commitment schemes. Which exact scheme to use here should also be
> flexible so that we can try different tools like lattice hash, KZG polynomial
> commitment, etc. Writes should be revertible within a small period, so the set
> should be copied on writes. Older sets can be freed when later sets are
> committed. Similarly, during eviction, the set of evicted writes are sent to
> the storage engine as a proposal that commits when the set is committed.
> Finally, and perhaps the most interesting design, is that the eviction policy
> should provide a set of keys that are likely to be evicted next, so that we
> can send the writes to storage engine early for pre-computation. Please
> remember to add an API for the set of likely evicted keys to the eviction
> policy and the mechanism to send these writes as proposals to the storage
> engine. For now, everything only needs to stay in memory. Please also add
> simple benchmarks (based on what's in `firewood`) for simple evaluations.

Added emphasis (2026-10-25):

> Please also remember to combine the commitment for the set with the
> commitment from the storage layer at the end so that you only return one
> commitment at the end of a proposal.

Clarifications (2026-10-25, after the first design and review rounds):

> 1. I think we do want to see the chained path. The whole point of
> precomputation is that, when we are finally confirmed on the set of keys to
> evict (at the end of block execution when we receive a new `propose`),
> hopefully most of the Merkle storage has already been updated. The estimation
> would differ from the actual write set, so we should drop precomputations for
> keys that are not yet evicted (e.g., by sending the old value back?). During
> the execution for one block, it is also possible that we would send predicted
> eviction keys to storage multiple times to refine the prediction better
> (e.g., more keys evicted as transactions run).
> 2. Let's focus on firewood as the storage engine backend since QMDB already
> puts freshly written keys together.
> 3. What is prefetching in this context? I thought prefetching is what happens
> when the validator tries to write to a key during execution so that the
> storage will bring its parent nodes to memory to accelerate Merkle
> computation. By the way, let's add this prefetch interface as well so that
> libevm can hint the storage engine early.

Decision (2026-10-26), after confirming that firewood commits one proposal
at a time and does not cascade a chain commit:

> One predominant side effect is that firewood would compute some
> intermediate hashes multiple times if 1. a key is written and reverted, or
> 2. if multiple children are changed in different revisions. For now, this is
> fine -- the eviction policy can be a bit pessimistic and give multiple
> likely to be evicted keys when the set grows too big so that next few writes
> do not cause further evictions. But do put firewood level changes as a todo
> for future.

Decision (2026-10-26), after the first micro-benchmarks showed persistent
structures costing more per write than firewood's own hot path:

> What is the current set size you used for testing? What if we limit it to
> 10k entries and all evicted entries go to firewood?
>
> Let's do that. Per-block eviction is expected, but it should not matter too
> much since 1. we can precompute some entries evicted to firewood in the
> future, and 2. the eviction policy can evict half of the entries on one
> eviction to make room for possibly writes from future blocks as well. Notice
> that a typical block only writes less than 2k entries.

(The "that" agreed to: a small default capacity, 10k entries, with capacity a
benchmark sweep dimension; and the write set built on in-place structures,
the committed map mutated at commit, per-proposal overlays, and an undo log
for retained revisions, instead of a persistent map per snapshot. The
structure part was revisited in clarification 5, R18: a full copy per commit
and no undo log.)

Terminology fixed by clarification 3: **prefetch** means warming the storage
engine's trie paths for keys that execution is touching, via an explicit hint
API; the early sending of predicted evictions is **speculative eviction**.

Clarification 4 (2026-10-26), on the mechanics of speculative eviction:

> By the way, please also remember to make precomputation asynchronous so
> that we do not wait for its completion -- we only need it at the end of the
> block.

(Precomputation here is the speculative-eviction work of R9/R15, proposing
predicted victims to the storage engine ahead of `Propose`, and the trie
warming of R14. Both run on a background worker owned by the instance;
`Commit`, `Prefetch` and `Propose` never wait for engine work (amended by R24: `Propose` waits for the queued links), and `Propose`,
at the end of the block, uses whatever links have completed by then.)

Clarification 5 (2026-10-26), on locks and the undo log:

> I have seen quite a few mutex in your implementations. Why are they needed
> under a blockchain workload? Recall that during block execution, a validator
> would only read certain states and send write prefetch requests which should
> only affect eviction policy (hence precomputation may happen along the way
> as well), while writes with actual values arrive much later and applied with
> a copy-on-write fashion. It does not strike to me as we need a lock on
> overlay, the undo log, or etc. Or am I missing something? Please validate
> your lock uses. By the way, why do you need an undo log in the first place
> when all writes are copy-on-write?

and, after the lock audit and the micro-benchmark of the alternatives:

> Let's start with full copy and removing undo logs. I need a bit more
> explanations to be convinced that we need the other locks too.

(Decided: the committed set is an immutable snapshot copied in full at each
commit, so there are no undo logs and reads take no lock; each remaining lock
is justified individually in design §10. This supersedes the structure clause
of R16.)

Clarification 6 (2026-10-27), on the remaining locks, item by item (the
numbering refers to the lock list the design's §10 gave at the time: 1 the
registry mutex, 2 the commit mutex, 3 the per-node chain mutex, 4 the
firewood per-handle read-write lock, 5 the cells and lifecycle fields):

> For 1: it seems that the lock is mainly for hashmap allocation but not
> subsequent reads and writes to a snapshot. You can keep it, but make sure to
> keep the critical section small. For 2: Not sure where the code is exactly,
> but I would argue that you should change the parameter type to avoid reuse
> (e.g., Commit takes a mutable reference to it, etc). For 3: What does the
> chain contain and why we have two writers? My understanding is that the user
> chain only submits precomputation requests, while the background worker
> chain would update the already-in-place slot to indicate that it is ready. I
> am sure there are designs to avoid the coordination here. By the way, why do
> we have node here? Does each node correspond to a certain proposal? For 4:
> Let's keep it for now and only leave the problem in `docs/TODO.md` is fine.
> For 5: not sure if all locks here exists for a reason. For one, do we even
> need the parent pointers? I do not see a case where we need to get the
> parent of the current set to answer any queries.

(Decided, R19: the writer operations `Propose`, `Commit`, `Prefetch` and
`Close` take the instance by `&mut self`, so the type system serialises them
and the commit mutex goes; reads go through `Send + Sync` reader and
revision handles. A speculative link is a slot the caller allocates when it
plans the link and the worker fills exactly once; the chain of slots is owned
by the writer, so no chain mutex exists and the one-link-in-flight rule is
gone. A node records its uncommitted ancestors immutably at propose time, so
there is no parent pointer and nothing to clear; commit and the
descends-from-head check read that list. The registry mutex stays, with its
critical sections listed, and so does the firewood handle lock (TODO item 5);
the storage engine is owned by the writer and closed through `&mut self`, so
the adapter's database-handle mutex goes too.)

Clarification 7 (2026-10-27), on the remaining per-node structure:

> I will continue to press for a simpler design: why do you need `base`,
> `ancestors`, and `overlays` in `Node`? It seems to me that if we take the
> CoW approach, you do not need to keep any of these. By the way, why do you
> have `Proposal` in addition to `Node`? What is shared and why do you need
> it?

and, after the answer (full copy per proposal removes all three and the
replay-equivalence machinery of §6.1; one weak parent link remains for
identity; the adapter never commits a proposal whose parent is uncommitted;
the Rust-side handle count and closed flag duplicate the Go layer's
keep-alive bookkeeping):

> Let's do that. Then, go through your code again and find anything that
> seems redundant to you at this point. I suspect that you have a lot of
> over-engineering that can be greatly simplified.

(Decided, R20: every proposal is a full copy of its parent's set with the
batch applied, so a node is its snapshot, its pinned storage view and one
weak link to its parent; there are no overlays, no base, no ancestor list and
no replay-through-chain policy queries. `Commit` requires the parent to be
the head, as firewood does and as the adapter always does; the cascade commit
of qmdb-ffi is dropped. Handles are plain newtypes over the node with no
shared state; the Rust side keeps no handle count and no closed flag. The
redundancy pass that followed is recorded in design §14 v19.)

Clarification 8 (2026-10-27), on per-node and registry state, two questions
asked after the redundancy pass:

> Why does a Node need to keep track of `owns_storage`? And why is the
> reference to the storage engine stored as `storage_path`? For a storage
> engine like `firewood`, all revisions are stored in the same directory. It
> does not make sense to keep track of `storage_path` at this level.

> For `Registry`, why do you need a `head` in the first place? And why do you
> need `live` and `revisions`? It is not clear to me why you keep these 3
> fields around.

(Decided, R21: a node and the registry hold nothing derivable. Whether a
node owns a storage proposal is handle identity at commit (an inherited view
is the head's `Arc`); the misnamed `storage_path` was a per-node metrics
record and went with the node's batch summary, statistics being counted at
`Propose`; the node keeps its speculative link count for the poison rule.
The registry's K-ring is one deque in commit order whose newest entry is the
head, so there is no head field and no separate order; the revision count is
not copied into the registry (`advance` takes it); the live index stays,
because `Revision(root)` must resolve a live proposal's root as qmdb-ffi's
`Database.Revision` and firewood's `view` do and as the adapter's
`Reader(root)` relies on (R1), and hints on an unaccepted block reach its
chain through the same lookup (01 §9.4), reduced to a flat list of (root,
weak link) pairs (height joined the pair in round 30). The review that followed, design §14 round 28, removed
what else was derivable: the registry's counters, the shared configuration,
the writer's second closed flag and the superseded lineage case.)

Clarification 9 (2026-10-27 and 2026-10-28), on what the registry, the root
and the batch must carry. After the registry answer the owner asked:

> I guess my real question is if we really need to manage committed vs
> uncommitted revisions separately. If we have multiple competing revisions
> at the same height, we will probably want to retire them together at a
> later depth, regardless if they are committed or not.

(answered: retirement and lookup can be uniform, ownership cannot, because a
strong reference to an abandoned candidate would pin its engine proposal and
stall firewood's reaper for the whole window), then:

> I see. Then, why does a registry need `classify()` and `clear_ring()`? Why
> are these two methods useful?

and, agreeing to remove both:

> Let's do that, and I will add a little more: 1. in `root.rs`,
> `RootPreimage` already contains `policy_name` and `policy_params`. Are
> there particular reasons you need `policy_digest` as well? Remember that
> keys that are likely to be evicted are merely predictions that does not
> require consensus agreements.

> And 2. what is `prefix_deletes` in `Evicted<K, V>`? Why do you need it?

(Decided, R22: the lineage walk is replaced by two compares (`Commit`
requires the parent link to be the head node; `Propose` on a node whose root
is the head's builds on the head node, any other node is proposed on as
given, as firewood and qmdb-ffi accept); a chain was kept for the head and
for live nodes above the head's height (a test R23 then dropped); `close` does not touch the registry;
live entries retire with the same window as the ring. The root does not
commit to the policy's state: validators agree on evictions because each
applies the same batches in the same order, so the digest only detected a
divergent implementation one eviction earlier at the cost of a second
accumulator per write; `policy_name` and `policy_params` stay. `PrefixDelete`
is rejected by `Propose`, as QMDB rejects it behind qmdb-ffi: the set cannot
apply it to keys it does not hold and the adapter never issues one. The
review of that design (round 30) found the root clause unsound: the adapter
stages a child on every live proposal registered under the parent's root
and returns one candidate's root, so two live candidates with equal roots
but different policy states would give a child two roots depending on
arrival order. The decision stands in its cheaper form: no digest of its
own, but every set commitment item carries its key's policy datum, so equal
roots imply equal policy state at no extra hashing beyond a same-value
rewrite under `lru-write`; the owner's premise holds for predictions
(`likely_victims`), which stay local. Superseded by R25, clarification 20:
the policy state is a term of its own again, `state_digest`, and the items
carry no datum.)

Clarification 10 (2026-10-28), two remarks while round 30 was running:

> By the way, I think `Snapshot::apply` should not have `capacity` and
> `low_watermark` as parameters. `capacity` should be a field of `Snapshot`,
> while `low_watermark` should be determined by `policy`.

> Also, is `Registry::is_current` needed? When is it ever used?

(Decided, R23: the capacity is a field of the snapshot and a node term of
the root preimage (the field a future adaptive capacity changes per node,
R4); the low watermark is the policy's parameter, entering `policy_params`,
and the policy decides the eviction count. `is_current` served `Prefetch`
and the chain sweep and is gone: a chain lives with its node, `Commit`
drops the ex-head's chain by identity, the sweep drops dead nodes' chains,
and a chain on a finished line is refused at its first link or wasted,
never read by a node that can commit.)

Clarification 11 (2026-10-28), on the trait:

> I don't think `low_watermark` needs to be explicit. It should simply be a
> policy parameter. Some policies may not have a low watermark at all.

(Decided, R23 refined: the trait asks a policy only how many to evict
(`eviction_count(len, capacity)`, at least `len − capacity` once over
capacity, so one eviction returns the set to capacity) and for a
digest of its own parameters; the low watermark is the v0 policies' private
parameter, and `WithLowWatermark` is handed to them.)

Clarification 12 (2026-10-28), on the speculation design:

> I think the linked list design for speculative precomputation is overly
> complicated. Since the validator can process one block at a time, newer
> precomputations will always be built on top of the last precomputed view,
> regardless if it has finished or not. In this way, you do not need a
> linked list at all -- only a latest view and the combined set of evicted
> writes that needs to be sent to the storage next. Note that there will not
> be two concurrent precomputations with different base views either since
> the previous block must have finished computing Merkle root before the
> next block can start, and during the Merkle root computation all previous
> precomputations must have completed. By the way, if we cannot stop a
> precomputation on firewood, it makes no sense to mark it cancelled. We
> should remove that as well.

(Decided, R24: one precomputation per instance; the writer keeps its target
node, the combined set of entries sent and the link count, the worker keeps
the latest view and builds every link on it in queue order; nothing is
cancelled; `Propose` on the target waits for the queued links and builds the
final proposal on the latest view, which amends R17 for `Propose` alone; a
commit retargets the precomputation to the new head unless it already
targets the new head or a live child of it; a hint on a live child of the
target retargets it (with no live target, a hint on the head or a live child
of it); other hints only warm. Superseded in part by R25: the sent set is
the policy's marks, the latest view and the link count are the pipeline's,
and the target is the writer's working copy.)

Clarification 13 (2026-10-28), two questions on the speculation mechanism:

> 1. since keys evicted speculatively may be wrong (e.g., the key is later
> written again so it should not be evicted), we need a way to retract an
> eviction by setting the value back to the original value. Please make sure
> you can do that with the current shape.

> 2. currently `auth_wal` launches new threads to track precomputations. Do
> we have to work with new threads? Can you take a look at `firewood/`,
> particularly how it computes Merkle hashes in parallel and how its
> background persisting worker is implemented, to see if it is possible to
> use the threads the validator's go runtime already manages?

(Answered: the retraction is the final proposal's revert of §9.1, which puts
back the value the parent's pinned view holds, from before any link, or
deletes the key, and the differential test now names the case. Firewood
spawns its own threads (a rayon pool for parallel proposal hashing and a
persistence thread) and its FFI proposes on the calling goroutine's thread;
auth_wal's one worker thread only drives the sequence of link proposals,
whose hashing runs inside firewood. A driver model with no thread of
auth_wal's is recorded in 01 §12.5 as an option to decide at milestone 2.)

Clarification 14 (2026-10-28), after a walk through where the old version
of a change is kept for a revert (the parent's immutable snapshot for the
set; a read-back through the parent's pinned storage view for a key a link
sent to storage; no undo copy anywhere):

> Let's keep it as is for now. Remember to add a note to change this design
> in the future by extending firewood APIs.

(Decided: the read-back revert of 01 §9.1 stays. The future change is
recorded as a firewood-side restore op, 01 §12.10 (e) and `docs/TODO.md`
item 6: the final proposal would carry `Restore(key)` for a retracted
eviction instead of a value read back and put again. No requirement row:
nothing in v0 changes.)

Clarification 15 (2026-10-28), on the speculation module:

> I think the design for `speculate` is overly complicated. First of all,
> how does `sent`, `hinted`, and `hinted_new` relate with each other? My
> understanding is simply that the upper layer may call `record_link` to
> give it a list of key-value pairs evicted speculatively. It simply records
> them in a pending (not yet sent to storage) table -- if the same key is
> evicted more than once, only the last value is kept. Then, a background
> thread will consume all entries in the pending table into a write batch
> and send it to storage. It will not send a next batch until the previous
> batch has returned, and we can have some more heuristics like it only
> sends a new batch if it is sufficiently large. What problems are there in
> this simplified design? If you follow this design, I do not see why you
> need `hinted`, `hinted_new`, `links`, `used`, or `note_hints`, nor all the
> special `WorkerJob` types.

(Answered: the pending table needs a base view and a target identity, a
record of what reached storage for the final proposal's reverts, a wait at
`Propose` and a generation check; `hinted`/`hinted_new` were the planner's
inputs, `used` and the job enum were droppable. Decided with the following
clarifications.)

Clarification 16 (2026-10-29):

> There are a few nuances. 1. the batch sent speculatively may not
> necessarily be the full set of keys that will be evicted, just a set of
> very likely to-be-evicted key-value pairs. This set is neither 100%
> accurate nor complete. Because of this, reverting is inevitable, which in
> the current design is done by writing again with the old value. 2. I do
> not see a reason to keep the keys that have already been sent to the
> storage layer. There is nothing wrong with such an implementation, but I
> think a channel of pending key-value pairs with the worker may be a more
> simplified design.

(Recorded in 01 §9: the prediction is neither accurate nor complete by
design and the final proposal corrects both directions. The sent record
moved out of the writer; where it lives was settled by clarification 17.)

Clarification 17 (2026-10-29):

> I think it should be the policy who keeps track of sent keys. At the end
> of propose, it should check which keys it has already marked as likely to
> evicted and which ones should actually be evicted. Note that the policy
> should already track which keys it has already returned in
> `likely_victims` so that it does not return the same set of likely victims
> multiple times.

Clarification 18 (2026-10-29):

> I think this deserves further discussion: 1. I agree that
> `likely_victims` need to change the policy state and require mutable
> reference, which I do not consider as a huge problem -- if there are
> multiple threads sending write hints, you need them to update the policy
> to keep track of policy specific states anyways. 2. The consensus boundary
> should remain the same. The set of likely victims are predictive and
> should not be part of the consensus. 3. this is where I think we need two
> APIs: one for `hint_writes` and returns a set of likely victims, and one
> for `writes` and returns a set of actual victims. The first API is used
> when the validator sends write hints during execution, while the second
> is used when the validators have finished execution and we have a
> finalized write set. Both APIs will return a new set of keys to be evicted
> (or likely to be evicted) and a set of keys that were previously marked as
> evicted but needs to be retracted. Critically, the final set of evicted
> keys only depends on the written keys in the second API, and that is what
> is needed at consensus level. All the intermediate predicted key evictions
> do not require consensus at all.

Clarification 19 (2026-10-29):

> I think `on_remove` can go as well, and I do not think `key_state` is
> necessary. Since the policy controls eviction, it can remove its keys
> first and the set should remove the same keys when it receives the
> eviction list. `evict` is needed as part of the returned values of
> `writes(keys, height, capacity)` mainly because we need it to modify the
> actual set, not because we need to return it back to validators for
> consensus.

(Answered: `on_remove` goes; `key_state` had one consensus job, the root's
commitment to the eviction order, which the round-30 finding requires
because the adapter stages children by root; a replacement was needed.)

Clarification 20 (2026-10-29):

> I see the problem, and maybe `state_digest` instead of `key_state` is
> what we really need?

Clarification 21 (2026-10-29):

> Let's start with full hash. Unlike the set, policy state will probably
> change frequently across runs. We can try to replace with lattice hash or
> others later as needed.

(Decided, R25, from clarifications 15–21: the policy owns the prediction.
`writes(keys, height, capacity)` is the consensus entry point (the victims,
which the set removes; the marks reconciled into `confirmed` and
`retract`); `hint_writes(keys, capacity, margin)` the local one (new likely
victims to send early, predictions to retract, never a victim twice); the
policy's consensus state enters the root through `state_digest`, a full
hash of the eviction order per proposal, to be replaced by an incremental
digest inside the policy if it measures as too slow; `on_write`,
`on_remove`, `key_state`, `eviction_count`, `select_victims` and
`likely_victims` leave the trait; the writer keeps a working copy of the
target's policy, where the marks live, consumed by the first child; the
speculation machinery is a pending queue of link ops, a worker and a flush
behind one mutex and condvar, and links carry retractions as well as
evictions. 01 §6.1, §7.3 and §9 are the design.)

Clarification 22 (2026-10-29), on the R25 shape:

> 1. watermark is internal to policy and not all policy would have a
> watermark, consider removing it from `policy/mod.rs`. 2. `Eviction<K>`
> should simply be the full evicted list plus a final `Prediction<K>`
> refinement. Returning `confirmed` and `retract` only complicates the code
> logic. 3. The design of `Marks` require further discussion -- personally
> speaking I do not see why you need `hinted` and `hinted_new`. So let's
> remove it for now. 3. `policy` is part of the `Snapshot`, so I do not see
> why you need to pass it in as a parameter again in `apply`. 4. For the
> Pipeline's `Job`, you only need to worry about `Link`, and `Warm` is part
> of the `Link` precomputation. By the way, I think `Link` is a bad name
> given that we no longer have the linked list design.
> Also, the `Pipeline` design is almost certainly an over-engineering, but
> I want to first see what it would look like when `warm` is gone.

(Decided, R26: the watermark lives with the v0 policies in
`policy::watermark`; `writes` returns `Eviction { all_evicted_keys, refinement:
Prediction }`; the marks are the marked keys only, each hint predicting
from its own insertions and the margin (01 §12.5 keeps the per-block count
as the first refinement to measure); hints mark the policy inside the
target's snapshot, behind a per-snapshot lock, so `apply` takes only the
batch and there is no working copy; `reset_hints` forgets the marks at a
retarget; the pipeline has one kind of work, a step, and no warm jobs (the
steps are the warming); "link" is "step" throughout. The pipeline is shown
in this shape for the owner's next decision, before any review round.)

Clarification 23 (2026-10-29), on the `policy::watermark` module of R26:

> Let's just take out `watermark` completely. The watermark will be a part
> of the policy params and some policies may not have a `watermark` at all.

(Decided, R26 amended: no shared watermark code anywhere; each v0 policy
keeps its low watermark as its own parameter, with its own eviction rule
and `params_digest`; the R16 default, half the capacity, is the benchmark's
and the Go `New`'s convention, 01 §6.2.)

Clarification 24 (2026-10-29), on `speculate.rs` without warm jobs:

> I think the share of responsibility between `Worker` and `Pipeline`
> looks very weird. I will refer to them as the worker and the interface
> (for `auth_wal`) since I think the current name is misleading. Per my
> understanding, the worker should be managing the storage engine. It
> receives two types of requests: speculative eviction (a batch of
> `SpecOp`), and a commit message indicating that we have reached a block
> end, so just apply all the evictions and return the final `base` for
> `auth_wal` to process the next block. Correspondingly, the interface will
> also only provide these two interfaces: one to submit a batch of `SpecOp`
> to the worker's queue, and one that block-waits for the final `base` to
> be ready.

> Note that the worker may have multiple batches in the pending queue. When
> the previous batch has finished, before sending the next batch, it should
> take everything in the pending queue, merge them together into a combined
> write set, then send to the storage engine.

(Decided, R27: the worker owns the speculative side of the engine; the
interface is `submit(ops)` and `finish() -> latest view`, plus `reset(base)`
at a retarget, which the two cannot express: a retarget resets the marks and
the chain together, and a child that inherits its parent's view shares the
same engine handle, so the worker cannot tell a retarget from a continued
chain from the base alone (01 §9.2). The thread takes everything queued at
once and merges it by key, the last op winning, into one write set per
step. The queue is plain data; the worker logic is the thread's. Not yet
reviewed.)

Clarification 25 (2026-10-29), on `speculate.rs` with the worker's queue:

> It seems that this `Queue` is a reimplementation of existing parallel
> queues. Can you find some existing high performance library for this?

and, on the proposal to use `std::sync::mpsc` with `Submit`, `Finish` and
`Reset` requests and the chain state as the thread's locals:

> I think that would be a much more clear design.

(Decided, R28: the worker's queue is a `std::sync::mpsc` channel (crossbeam's
implementation since Rust 1.67, no dependency added); the writer sends
`Submit`, `Reset` and `Finish` requests and blocks on `Finish`'s reply; the
thread owns the chain as locals and needs no lock, generation, busy or
liveness flag of ours; `Reader::worker_idle` is gone (01 §9.2, §10).)

Clarification 26 (2026-10-29), after the channel design of R28:

> I think this looks good enough. Let's start with an initial
> implementation without precomputation (yet).

(Done: milestone 1, the plain path end to end; `New` refuses speculation
until milestone 2 and the worker's step builder stays `todo!()`. The two
reviewers of R26–R28 had died on the session's rate limit; by the owner's
call those revisions stand unreviewed. 01 §13, §14.)

Clarification 27 (2026-10-29), after the milestone-1 numbers at 10k:

> Can you run some more experiments with much more keys (say 100k)? If the
> numbers still don't look good, then CoW may be a bad idea and we need to
> work with undo logs. My concern with undo logs is that they do not work
> well with the various policies :/

(Open. Measured at capacity 100k (01 §12.13, §14): the full copy per
proposal of R20 costs about 28 ms per block before the first op (entry and
policy map clones 11.5 ms, the BLAKE3 policy digest 7.4 ms, the drop of
the retired snapshot 8 ms at commit) and 3.4 GB for 128 revisions, so
auth_wal runs at about a quarter of bare firewood's rate on every workload;
at 10k the same terms were 1.4 ms. Two ways out are measured: path-copying
persistent maps keep R20's properties and remove the O(n) terms but stay
near 7 µs per op; a mutable base with undo records reaches 0.1–0.2 µs per
op and needs overlays, undo deltas and a read lock back. 01 §12.15 sketches
how the policies live with undo: consensus state as per-key attributes in
the entries, a local lazily-corrected index, an incremental lattice digest.
Decision pending; R20 stands until it is taken. The reading of "100k" as
the set's capacity was corrected in clarification 28; the capacity sweep
stays recorded as what a larger set would cost.)

Clarification 28 (2026-10-29), correcting the reading of clarification 27:

> I need to correct this: I want to experiment where there are 100k
> key-value pairs already in the storage. The `auth_wal` capacity stays at
> 10k. The key thing is computing the merkle root is expensive when the
> storage contains a lot of keys. In this setup, replacing Merkle updates
> with lattice hash and copying may actually lead to performance gains.

(The benchmark driver gained `--prefill` (01 §11): the workload's `rows()`
keys are inserted in-process before the measurement, through the write set
when there is one, since v0 requires a fresh engine (01 §1, §12.6). Results
in 01 §12.13 and §14.)

Clarification 29 (2026-10-29), after the prefilled-trie results:

> Let's try running with speculative precomputation enabled. This time,
> your write set will be a union of smaller write sets. Instead of simply
> given the unioned write set, you will periodically hint a small write set
> (say, every 1~5ms, geometrically distributed) 30 times (with ~80 writes
> each). Then, submit a union of these writes to disk as a new proposal.
> This time, we are interested in not only the time spent on the final
> computation, but also the time spent on sending the write hints. The
> workloads will remain the same. By the way, as a heuristic, `hint_writes`
> may start to return speculative evictions as long as the new writes when
> the pool is 95% full. You can refine the set of evicted keys as later
> hints are sent. And, the storage view with precomputation should not be
> gone when the current block is committed and we enter a new base view,
> since the set of evicted keys will most likely be the same in the next
> block proposal if they are not evicted in the current block proposal.

(Decided, R29–R31; milestone 2 implemented. R29: speculation is on
(`build_step` implemented, `New` accepts `speculate`), and the benchmark's
`--hints` mode emulates execution: a block of `-b` writes is cut into
`--hint-chunks` hints (30 × 80 for a block of 2400), each hinted after a
gap of `--hint-gap-min-us` plus a geometric number of 100 µs steps
truncated at `--hint-gap-max-us` (1–5 ms), and the block is proposed as the
union of the chunks, the last write of a key winning; per-hint latency is
reported beside propose and commit (01 §11). R30: "as many as the new
writes once 95 % full" is realised as "predict what `writes` would evict if
the block ended now": `hint_writes` projects the set size plus the block's
new keys, counted once across its hints (the per-block hinted sets
clarification 22 had dropped come back: the count, because 30 hints per
block saturate at the first hint's count without it, and the hinted keys,
because lru-write must exclude every key the block writes, whichever hint
named it, or it predicts keys an earlier hint showed written and `F` puts
them back: 32–49 per block on `zipf` before, none after), and the v0
policies gain an eviction trigger
(`with_trigger`, `--high-watermark`; default the capacity, R16's bulk
rule); `trigger = low_watermark = 0.95 × capacity` is the heuristic as
stated, and makes the actual eviction incremental too, which is what lets
the predictions be exact (01 §6.1, §6.2). R31: a prediction outlives the
block: `writes` keeps the marks of the keys it did not evict, and a
retarget re-sends their entries as the new chain's first batch, so the next
block's steps carry them before any hint arrives. What cannot be kept is
the engine-side view itself: the committed root covers the storage root,
so the final proposal must still put the kept keys back (01 §9.1, §9.3) and
firewood cannot rebase a proposal onto the new head; carrying the
precomputed proposal across the commit is firewood-side TODO item 7 (01
§12.10 (f)).)

Clarification 30 (2026-10-29), same message:

> Also, as a fuzz test, please construct a similar workload and make sure
> that, from the same policy state, the union of evicted keys from multiple
> `hinted_writes` + a final `writes` is the same as the evicted keys you
> get if you directly give the final unioned keys through `writes`.

(Done, R32: `src/tests.rs` `hints_compose_to_the_plain_eviction`, for both
policies under both eviction rules, forty random configurations, six
blocks each with one to six overlapping hints (keys hinted but not written,
writes never hinted): the actual eviction and the state digest equal the
hint-free copy's, and the marks plus the final refinement leave storage
holding exactly the victims; the kept marks carry into the next block. 01
§6.1, §9.3.)

Clarification 31 (2026-10-30), on the milestone-2 report:

> Your definition of propose and commit may be different from mine, but I
> see propose as the step to compute the new Merkle root while commit as
> the step to mark a proposal as committed so that the storage engine can
> persist to disk asynchronously. In this terminology, commit latency does
> not matter since it does not happen on the critical path of blockchain
> systems. Regardless, I do see that computing a new Merkle root without
> warm up ahead of time can be problematic. So we probably still need to
> add warming up. Let's put that into the todo, then do a detailed latency
> breakdown to see what the problem really is instead of guessing.

(Decided, R33. The terminology is adopted: `Propose` (the new root) is the
critical path and the figure of merit; `Commit` marks the proposal
committed, and firewood's own commit signals its persist worker and
returns, so the engine's persistence is asynchronous and commit latency is
reported but not optimised (01 §11). Warm-up ahead of the final root
computation is a TODO item (`docs/TODO.md`, auth_wal section). The
breakdown: `AuthWal::last_propose_timings` and `last_commit_timings`
return where the last call spent its time phase by phase (the entry and
policy copies, the ops, the policy's `writes`, the victims' removal, the
policy digest, the wait for the worker,
the read-backs, the engine proposal, the rest; the engine commit, the
registry advance with the retired snapshot's drop, the retarget), the
worker counts its tail steps and step sizes, and the benchmark's
`--breakdown` prints the mean, p99 and share of every phase over the
measured blocks (01 §9.5, §11). What it found (01 §12.16, §14): the set's
own work is 8.4–10.5 ms per block on `zipf` and 17.8–19.4 ms on
`tenkrandom`, more than bare firewood's whole propose, in every
configuration; the engine side of the critical path is the cold-path puts
of the evicted keys, which speculation moves off the path except the tail
step; and the chain's commit cost is firewood's persistence backpressure,
a knob, off the critical path. With a step per hint (rounds 30, min_step
1) the engine side of propose falls to 0.1–0.7 ms and propose is the set's
own work alone: 10.2 ms on `zipf` against bare firewood's 7.1, 20.7 ms on
`tenkrandom` against 15.9. The levers are in `docs/TODO.md`.)

Clarification 32 (2026-11-01), on the set-work breakdown:

> It does strike to me that a full rehash can be more efficient than
> lattice hash. Please try to add such a simply vc scheme and see if it
> performs better than it currently does. By the way, because precomputed
> roots are triggered by evictions we estimate to happen at the end of the
> block, it is very likely that we simply end up not evict any. In this
> case, the precomputed root of the storage engine should be preserved, but
> we should be ok to return without waiting for it to finish, and we can
> use the same storage root as from the previous block. Please also
> continue digging into how we could improve performance based on your
> measurements. Focus more on the slowest parts and propose solutions
> before making any code changes.

(Decided, R34–R36. The whole-set `rehash` scheme is implemented and
measured (R34): `VectorCommitment::commitment` is handed the entries in key
order, `rehash` keeps no state and hashes them once per proposal from one
buffer, and the breakdown gained a `vc` phase for the commitment's digest
(01 §7.1, §7.2, §9.5). In situ, with a step per hint, it cuts propose from
10.5 to 6.9 ms on `zipf` and from 21.5 to 9.1 ms on `tenkrandom`: a
lattice item costs 2.7–2.8 µs in situ (two per rewrite, one per insert and
per victim), the rehash 0.66–0.75 ms per block at 10k entries, and the map
work the lattice hid is 0.3–0.4 µs per op; propose is at or under bare
firewood's on both workloads (6.9 against 7.1, 9.1 against 15.9). `lthash`
stays the default until the owner decides; at 10k `rehash` wins whenever a
block's items exceed about a fortieth of the set, and the lattice remains
the scheme for a large set with small blocks (01 §7.2, §14). The
no-eviction behaviour is decided (R35) and designed, not yet implemented
(01 §9.1, §9.2, §12.17 (c)): a block whose `writes` evicts nothing returns
without waiting for the worker, inherits the parent's storage view (the
previous block's storage root) and keeps the precomputed chain for the
next block; the benchmark's hints are exact and never over-predict, so
measuring it needs a hint-noise knob. The digging (R36, 01 §12.17, §14):
after the rehash the set's work is 6.2 ms on `zipf` and 9.0 ms on
`tenkrandom`, of which the two copies are 3.5–4.0 ms, the policy's
`writes` 0.7–2.1 ms, the policy digest 1.0 ms, the rehash 0.7 ms; the
copies are slow because a B-tree clone in a fragmented heap is one cache
miss per node and per value (micro-benchmarks: 2.8 ms cold for the same
clone that takes 1.15 ms in a fresh heap, while a contiguous array copies
in 0.14 ms in any heap), not firewood's threads, the allocator's trimming,
the sleep between hints or the second socket. Proposed, in order of size,
for the owner's decision before any code change: a flat snapshot (sorted
arrays rebuilt by merge passes, 1.8–2.4 ms of set work projected), the
policy digest hashed from one buffer (1.0 to 0.15 ms), R35, and a rehash
without serialisation or on several threads.)

Clarification 33 (2026-11-01), on the proposals of clarification 32:

> I agree with these proposals. Please apply these proposals one by one
> and see how much performance gain you can get from each step.

(Decided, R37; done. The proposals were applied one at a time, each
measured on both workloads with the breakdown (01 §14 "Clarification
33"; a step per hint, incremental rule, `rehash`, 2.4M-row firewood):
propose on `zipf` went 6.9 → 5.2 → 3.5 → 2.25 ms and on `tenkrandom`
9.1 → 7.5 → 4.6 → 3.15 ms through the policy digest from one buffer
(step 1), the flat entries with a value arena (step 2) and the flat policy
arrays (step 3), against bare firewood's 7.1 and 15.9 ms; the rehash on
rayon's pool (step 4) measured slower in situ and was reverted; R35 (step
5) is implemented with a `--hint-noise` knob to exercise it: R35 halved
the mean propose under the bulk rule with one emulated read per written key
on `zipf`, 16.4 to 9.0 ms (the 100 non-evicting blocks of 200 kept the chain and paid the set's work alone, about 2.5 ms, while the evicting half still pays about 15 ms of read-backs and reverts for the over-prediction itself, which only firewood-side changes remove, §12.10 (e)–(f)), and left `tenkrandom` unchanged, where every block evicts under either rule. Design
v31 (01 §4, §6.1, §7.1, §9.1, §9.2, §9.5, §11, §12.17, §13).)

Clarification 34 (2026-11-02), after the step-by-step report:

> I think the code is probably a bit too complicated at this point. Please
> launch subagents for code reviews, targeting on designs that could have
> been simplified or useless code from previous experiments that have not
> been reverted. Please also launch subagents for general code reviews.
> Address their comments and repeat again until they do not have further
> comments.

(Decided, R38; rounds 1 to 3 done, round 4 pending. Four to five
read-only reviewers (two on simplification and leftovers, two general,
one on docs against code) per round; every finding is applied or
recorded with its reason (01 §14 "Review of 2026-11-02", three entries).
Round 1 found a consensus hazard in R35's implementation,
confirmed by reproductions: a sibling sharing the target's storage view
adopted the chain and its marks at commit without having been reconciled
against them, so a rival's rewrite, a straggler hint's parent-side entry
or a hinted rival's inserts could leave storage holding the wrong value
while the final proposal omitted the key. Fixed by making the child that
keeps the chain the target at `Propose` (01 §9.1, §9.2); the chain is
capped at `rounds` steps across kept blocks so the engine window holds;
the policy's parameters are validated at `New`; the merge pass's victim
check is complete. The leftovers removed: the criterion bench and the
lattice API only it used, the `ffi` stub and `staticlib`, the
view-identity handover, the worker's unused type parameters, the
benchmark's mirror structs and runner trait, dead fields and stale
comments. Round 2 found two more, both with reproductions: a `finish` on
a chain filled by a first child's tail answered the chain's view while a
second child's ops waited unbuilt (the per-chain budget of round 1 opened
it; `finish` now answers nothing while anything waits, and that block
takes the plain path), and the firewood adapter's first link did not pin
the base revision it reads by address, so a long-held orphan handle could
read other revisions' values after the base left the window (the link
now holds the committed handle, whose view pins the base). Known answers
pin the consensus encodings. Propose on the clarification-33 pair after
round 1: `zipf` 2514 (3145) µs against the step-5 control's 2599 (3100),
`tenkrandom` 2885 (3438) against 3181 (3583); after round 2: `zipf` 2523 (3111), `tenkrandom` 2922 (3301), unchanged within the spread.
Round 3 found that round 2's "compare a chain's base by root, as
firewood does" matched a firewood defect: a chain committed after a
same-root no-op commit makes firewood free the shared root node twice
(reproduced: later revisions corrupted); both engines now compare
revisions by identity and refuse such a chain (01 §10 items 3 and 4), the
lattice known answer goes through the scheme's own path, and a
fault-injected, pipelined differential joins the tests. The remaining
findings were small simplifications and wording. After round 3:
«MEASUREMENT».)

Clarification 35 (2026-11-03), on the next code review:

> I agree with everything you mentioned here except the first point.
> Merging the sets is a potentially expensive operation that I think would
> be better to hand to the dedicated worker, instead of on the critical
> path of blockchain validators.

(The accepted simplifications are recorded in 01 §14 "Review fixes of
2026-11-03". Pending speculative operations are coalesced by the dedicated
worker while draining submissions, including when its step budget is
exhausted. The validator-side `submit` only enqueues a batch; merging and
dropping superseded values stay on the worker.)

Clarification 36 (2026-11-03), on the benchmark's watermark semantics:

> The high watermark only exists as a trigger for precomputation, which
> should speculative mark 3000 (that is capacity - low watermark) keys as
> evicted and send them to firewood for precomputation. The precomputed root
> is kept and gradually adjusted as more hints arrive if it turns out the
> 3000 speculatively evicted keys differ from what would actually be evicted.
> Note that these immutable proposals are not committed so they may be
> discarded freely.

R39 corrects the interpretation recorded under R30: high watermark affects
prediction only, never actual eviction or consensus roots. At high 9,000,
capacity 10,000 and low 7,000, predict 3,000 eligible resident victims; refine
those candidates with later hints and include projected overflow if any.
Actual eviction remains `len > capacity`, draining to low. Non-evicting blocks
retain the speculative proposals under R35. High watermark is excluded from
`params_digest`, which covers the low watermark only. This corrects the old
root encoding as well as the eviction rule. The historical incremental-rule
benchmark results below/within the design describe the previous implementation.
Firewood's commit/persistence behavior is deferred and unchanged in this fix.
The subsequent default-configuration decision sets the benchmark low watermark
to 70% of capacity and high watermark to 90%, rounded down; both retain explicit
overrides. The library continues to require explicit consensus parameters.

Clarification 37 (2026-11-03), on constructor ownership:

> On `new()`, `auth_wal` should take the entire set since the set and the
> policy should match key space.

The owner also requested support for populated initial sets. R40: the Rust
constructor consumes one complete `Snapshot`, including entries, policy,
commitment scheme and capacity. Builders populate the policy and commitment
together with the entries; the writer derives root parameters from that set.
An empty set retains height-zero genesis; a populated set is equivalent to
one initial batch at height 1 over fresh storage, with a content-dependent
root. This does not implement recovery of an existing database.

## Requirements checklist (extracted)

| # | Requirement | Design section |
|---|---|---|
| R1 | Same external interface as `qmdb-ffi` / `firewood` Go `ffi` packages; no validator changes | 01 §3 |
| R2 | Set of recent writes; write inserts or updates in place | 01 §4 |
| R3 | Eviction to storage when full; **pluggable eviction policy** | 01 §6 |
| R4 | Set size is a parameter; future automatic grow/shrink | 01 §6.4 |
| R5 | Reads consult the set first, then the storage engine | 01 §5 |
| R6 | Set is authenticated with a **pluggable vector commitment** (lattice hash, KZG, ...) | 01 §7 |
| R7 | Writes revertible for a short period: copy-on-write sets; free old sets after later commits | 01 §4, §8 |
| R8 | Evicted writes go to storage as a **proposal** that commits when the set commits | 01 §6.3, §8 |
| R9 | Policy exposes **likely-next victims**; mechanism sends them to storage early as speculative proposals, refined during block execution, with mispredictions reverted so the final storage state equals a plain eviction | 01 §9 |
| R10 | Everything in memory for now | 01 §2, §10 |
| R11 | Simple benchmarks modelled on `firewood/benchmark` | 01 §11 |
| R12 | A proposal returns **one** commitment: the set commitment combined with the storage layer's commitment | 01 §7.3 |
| R13 | **Firewood** is the storage backend of record; QMDB keeps interface-level support only | 01 §1, §10 |
| R14 | A **prefetch hint API** lets libevm tell the storage engine early which keys execution touches, so trie paths are warmed before hashing | 01 §3, §9.4 |
| R15 | Accept firewood's per-proposal commits and repeated intermediate hashing for now; predict **pessimistically** (extra likely victims once the set is near capacity) so a block's later writes rarely need further links; firewood-side improvements are a recorded TODO | 01 §9.2, §12.10, `docs/TODO.md` |
| R16 | **Small set, cheap structures.** Default capacity 10k entries (a sweep dimension); evicted entries go to firewood; eviction frees **half** the set at once so the next blocks (typically < 2k writes each) fit without evicting; the write set uses in-place structures (committed map mutated at commit, per-proposal overlays, undo log for the K revisions), not a persistent map per snapshot; the structure clause is superseded by R18 | 01 §4, §5, §6.2, §8 |
| R17 | **Asynchronous precomputation.** Speculative links and hint warming run on a background worker; `Prefetch` never waits; `Commit` never waits for the queue (its engine commit may wait on the handle lock for one link in the pipelined flow, 01 §9.2); `Propose` waits for the queued links and builds on the latest view (R24 amended the original "never wait") | 01 §3, §9.2, §9.4, §10 |
| R18 | **Full copy per commit, no undo logs.** The committed set is an immutable snapshot copied and updated at each commit; every node holds the snapshot it was proposed on; reads take no lock; every remaining lock is individually justified | 01 §4, §5, §8, §10 |
| R19 | **Ownership instead of locks.** Writer operations take `&mut self`; readers and handles are `Send + Sync`; link slots are allocated by the caller and filled once by the worker (superseded by R24: one precomputation, no slots); nodes carry their uncommitted ancestors immutably; the registry mutex (small critical sections) and the firewood handle lock are the only locks | 01 §3, §8, §9.2, §10 |
| R20 | **Full copy per proposal.** A proposal is a complete copy of its parent's set with the batch applied (R7 taken literally); a node holds that snapshot, its pinned storage view and a weak parent link, nothing else about lineage; `Commit` requires the parent to be the head (no cascade); handles are plain newtypes; whatever the simplification made redundant is removed | 01 §4, §5, §6, §8, §10, §14 |
| R21 | **No derived state.** A node and the registry hold only what nothing else gives: storage-proposal ownership is handle identity, statistics are counted at `Propose`, the K-ring in commit order is the head, configuration is not copied; the live index stays for `Revision(root)` on live proposals (R1) and the hint path (01 §9.4) | 01 §8, §9.5, §10, §14 |
| R22 | **Only what consensus and the interface require.** The root commits to the set contents with their per-key policy data (through the set commitment's items, no digest of its own; superseded by R25: a digest of its own, `state_digest`), the storage root, the height and the parameters; `PrefixDelete` is rejected as QMDB rejects it; lineage is two compares and `Propose` accepts any node as firewood does; `close` releases nothing the drop would not | 01 §3, §4, §6.1, §7.2, §7.3, §8, §9.2, §10, §12.7, §14 |
| R23 | **Capacity is the snapshot's, how much to evict the policy's.** `Snapshot::apply` takes only the batch; the capacity is a snapshot field and a node term of the root; the eviction count is the policy's own rule and its parameters enter `policy_params` (a low watermark is the v0 policies' parameter, unknown to the trait); chains need no carrier test | 01 §4, §6.1, §6.2, §7.3, §8, §9.2, §14 |
| R24 | **One precomputation, no chain.** The validator executes one block at a time, so every link builds on the latest view in queue order, nothing is cancelled, and `Propose` waits for the queued links (amending R17 for `Propose`); the writer keeps the target, the sent set and the link count, the worker the latest view (R25 moved the sent set into the policy's marks, the latest view and the link count into the pipeline, and the target into the writer's working copy) | 01 §3, §8, §9, §10, §12.5, §13, §14 |
| R25 | **The policy owns the prediction.** Two entry points: `writes(keys, height, capacity)` (consensus: the victims, which the set removes, plus the marks reconciled) and `hint_writes(keys, capacity, margin)` (local: new likely victims to send early and predictions to retract; never a victim twice); the root commits to the policy state through `state_digest`, a full hash for now; `on_write`, `on_remove`, `key_state`, `eviction_count`, `select_victims` and `likely_victims` leave the trait; the writer keeps a working copy of the target's policy; the speculation machinery is a pending queue, a worker and a flush behind one lock, and links carry retractions too | 01 §4, §6.1, §6.2, §7.3, §8, §9, §10, §12.5, §12.7, §13, §14 |
| R26 | **Policy in place, no warming.** The watermark is each v0 policy's own parameter, with no shared code (clarification 23); `writes` returns the victims plus one last `Prediction`; the marks are the marked keys only; hints mark the policy inside the target's snapshot (behind its lock), so `apply` takes only the batch and the writer keeps no working copy; one kind of pipeline work, a step (no warm jobs: the steps are the warming); "link" is renamed "step"; the pipeline is to be simplified further once seen in this shape | 01 §4, §6.1, §8, §9, §10, §12.5, §13, §14 |
| R27 | **The worker owns the speculative engine side.** Two requests, a batch of ops and a block end, behind an interface of `submit` and `finish` (block-wait for the latest view), plus `reset` at a retarget; every step is the merged write set of everything queued since the last, the last op per key winning; the queue is plain data and the worker logic is the thread's | 01 §3, §9, §10, §12.5, §13, §14 |
| R28 | **The queue is a channel.** The worker is reached over `std::sync::mpsc` with three requests (`Submit`, `Reset`, `Finish` with a reply); the chain state is the thread's locals; no lock, generation or flag of ours on the speculative path | 01 §3, §9.2, §9.3, §10, §12.5, §13, §14 |
| R29 | **Speculation on, hinted blocks.** Milestone 2 implemented; the benchmark emulates execution: a block's writes arrive as 30 hints of about 80 writes with 1–5 ms geometric gaps, the union is proposed, and the time spent sending hints is reported beside the final computation | 01 §9, §11, §13, §14 |
| R30 | **Historical; watermark interpretation superseded by R39.** **Predict what `writes` would do.** `hint_writes` projects the set size plus the block's new keys (counted once across its hints) and predicts the eviction `writes` would make; the v0 policies gain an eviction trigger (default the capacity; at the low watermark: evict as many as arrive), so "once 95 % full, as many victims as the new writes" is `trigger = low = 0.95 × capacity` | 01 §6.1, §6.2, §9.2 |
| R31 | **Predictions outlive the block.** `writes` keeps the marks of the keys it did not evict; a retarget re-sends their entries as the new chain's first batch; the final proposal still puts them back in the committed storage (the root covers the storage root); keeping the engine-side proposal across the commit is a firewood change (TODO item 7) | 01 §9.1, §9.2, §12.10 (f) |
| R32 | **Hints never change the eviction (fuzz).** From one policy state, hints then `writes(union)` evicts exactly what `writes(union)` evicts without hints, with equal digests, and the marks plus the refinement leave storage holding exactly the victims; both policies, both rules, several blocks | 01 §6.1, §9.3 |
| R33 | **Propose is the critical path.** `Propose` computes the new root and is what the validator waits for; `Commit` marks the proposal committed and the engine persists asynchronously, so commit latency is reported but not optimised; warming the engine ahead of the final root computation is a TODO; changes are judged on a measured per-phase latency breakdown of `Propose` and `Commit` (`--breakdown`), not on guesses | 01 §9.5, §11, §12.16, §14 |
| R34 | **Whole-set rehash as a scheme.** `commitment` is handed the set's entries in key order; the `rehash` scheme keeps no state, ignores `apply` and hashes every entry once per proposal, a cost proportional to the set rather than the batch; chosen against the lattice by measurement (the breakdown's `vc` phase) | 01 §7.1, §7.2, §9.5, §14 |
| R35 | **A block that evicts nothing keeps the precomputation.** The prediction estimates an eviction at the block's end that may not happen; then `Propose` returns without waiting for the worker, the node's storage root is the previous block's (the parent's view, inherited), and the precomputed engine proposals are preserved for the next block instead of reverted, committed and rebuilt | 01 §9.1, §9.2, §12.17 |
| R36 | **Propose before changing.** Improvements to the set's own work are proposed with measurements (the breakdown, micro-benchmarks) and decided by the owner before code changes, the slowest parts first | 01 §12.17, §14, `docs/TODO.md` |
| R37 | **One step at a time, each measured.** The clarification-32 proposals are applied one by one, each step measured with the breakdown on both workloads against the previous step | 01 §12.17, §14 |
| R38 | **Review rounds until clean.** Independent reviews of two kinds, simplification (designs that could be simpler, leftovers of experiments never reverted) and general correctness, repeated until the reviewers have no further comments; every finding applied, or recorded with the reason (a requirement it would contradict, or the owner's call) | 01 §14 "Review of 2026-11-02", `docs/TODO.md` |
| R39 | **High watermark starts precomputation only.** Predict a capacity-to-low batch at the high watermark, refine it with hints, retain immutable proposals across non-evicting blocks, and evict only above capacity. High watermark is local and excluded from roots; low watermark remains consensus. | 01 §6.1, §6.2, §9.1 |
| R40 | **Construct from the complete set.** `AuthWal::new(config, storage, set)` owns a coherent set/policy/commitment snapshot, supports seeded initialization with an authenticated root, and validates capacity agreement. | 01 §3, §4, §7.3 |
