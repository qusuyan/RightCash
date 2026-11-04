//! Per-call latency breakdown of the writer's `propose` and `commit`
//! (design §9.5, §11): where the time of one call went, phase by phase,
//! taken with `Instant` reads around each phase (tens of nanoseconds against
//! milliseconds) and kept as the writer's last values, which the benchmark
//! reads after every block (`--breakdown`). Local diagnostics: never in a
//! root. `propose` builds a state and computes its root; `commit` commits any
//! storage proposals and advances the registry. Persistence work depends on
//! the backend, so proposal time and commit time are reported separately.

use std::time::Duration;

/// Where one `propose` spent its time (design §8 Propose, §9.1). The
/// phase fields describe the recorded work; `total` measures the whole call.
/// They need not sum to `total`: checks and scheduling add overhead, and a
/// failed speculative attempt remains in `finish` if the plain fallback
/// builds another proposal. Stored by the writer on success only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProposeTimings {
    /// Cloning the parent's arena chunk list, policy (marks copied, immutable
    /// v0 arrays shared until `writes`) and commitment state (design §4, §6.1).
    /// The records and policy arrays are built once by their merge passes.
    pub copy: Duration,
    /// The batch: keys collected, ops sorted by key with the last op per
    /// key, values appended to the block's chunk (design §4).
    pub ops: Duration,
    /// The policy's `writes`: its order updated, the victims chosen, the
    /// marks reconciled (design §6).
    pub writes: Duration,
    /// The merge pass: the parent's records and the ops into the child's
    /// array, victims skipped and collected, every change applied to the
    /// commitment, the arena compacted when due (design §6.3, §7.2, §12.17
    /// (a)).
    pub merge: Duration,
    /// The policy's state digest (design §7.3).
    pub digest: Duration,
    /// The set commitment's digest (design §7.2): the lattice's SHA-256 over
    /// its lanes, or a whole-set scheme's hash over every entry.
    pub commitment: Duration,
    /// Worker finalization wait, excluding the successful final build's
    /// `reads` and `storage` phases below. Includes queued or in-flight eager
    /// work and scheduling. On failure, includes the entire failed wait.
    pub finish: Duration,
    /// Preparing the final worker batch, including restoration reads through
    /// the chain base. Zero when latest is reused or on the plain path.
    pub reads: Duration,
    /// The final worker proposal or the plain proposal on the caller.
    /// Zero when the view is inherited or an exact latest view is reused.
    pub storage: Duration,
    /// Root composition, node construction, counting and registration.
    pub rest: Duration,
    /// The whole call.
    pub total: Duration,
    /// Ops in the batch.
    pub batch_ops: usize,
    /// Keys the batch evicted (design §6.3).
    pub evicted: usize,
    /// Ops in the final storage proposal, including pending hints merged
    /// with corrections. Zero when a view is reused.
    pub storage_ops: usize,
    /// Ancestor proposals under the final view; zero on the plain path or
    /// when the worker produces a single proposal.
    pub steps: u32,
    /// Whether the worker successfully returned this candidate's final view.
    pub speculative: bool,
}

/// Where one `commit` spent its time (design §8 Commit). Recorded on
/// success only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitTimings {
    /// The engine's commit of the node's storage view, the steps first
    /// (design §10); zero for an inherited view.
    pub engine: Duration,
    /// Advancing the registry and dropping the retired revision, its
    /// snapshot included (design §8).
    pub advance: Duration,
    /// `after_commit`: a retarget, which resets the worker's chain and
    /// re-sends the kept prediction (design §9.2, R31), or nothing.
    pub retarget: Duration,
    /// The whole call.
    pub total: Duration,
}
