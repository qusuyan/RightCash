//! C ABI consumed by the Go `ffi` package (design §2, §3; milestone 4).
//!
//! Mirrors `qmdb-ffi`'s `qmdb_*` functions as `auth_wal_*`, with the same
//! handle, keep-alive and result conventions. The Rust core is statically
//! generic, so the Go-side options `WithEvictionPolicy(name)`,
//! `WithCommitmentScheme(name)` and `WithStorageEngine(name)` select one of a
//! fixed list of monomorphizations behind an enum (`AnyAuthWal`), all with
//! `K = V = Arc<[u8]>` (design §4: every proposal copies the set, so a clone
//! must be a reference count); an unknown combination is rejected at open.
//! `WithSetCapacity(n)` is the consensus capacity; `WithLowWatermark(n)` is
//! handed to the v0 policies, whose parameter it is
//! (`capacity / 2` when absent, the R16 convention, design §6.2); a
//! policy without one ignores it.
//! The Rust writer takes `&mut self` (design §3 threading contract, R19),
//! which C cannot express: the database handle keeps the writer behind a
//! mutex that every writer call (`Propose`, `Proposal.Propose`, `Commit`,
//! `Update`, `Prefetch`, `Prune`, `Close`) takes, and a `Reader` beside it
//! that `Get`, `Root`, `Revision` and `LatestRevision` use with no lock. Go's
//! `Proposal` and `Revision` both wrap the one Rust `Handle` (design §8); the
//! Go `Proposal.Commit` becomes a writer call with the handle as its
//! argument, which takes the Rust handle out of the Go object whatever the
//! outcome, so a later `Commit`, `Get` or `Propose` on that Go handle reports
//! `ProposalConsumed` (design §3, §8). Keep-alive counting stays on the Go
//! side, as in qmdb-ffi (`Close` waits for outstanding handles there; the
//! Rust side keeps no count, R20). Panics are converted to errors at the
//! boundary, which is why the release profile keeps `panic = "unwind"`.
//!
//! Exports are generated into `auth_wal.h` by cbindgen. Nothing is exported yet.
