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

Terminology fixed by clarification 3: **prefetch** means warming the storage
engine's trie paths for keys that execution is touching, via an explicit hint
API; the early sending of predicted evictions is **speculative eviction**.

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
