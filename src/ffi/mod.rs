//! C ABI consumed by the Go `ffi` package (design §2, §3; milestone 5).
//!
//! Mirrors `qmdb-ffi`'s `qmdb_*` functions as `auth_wal_*`, with the same
//! handle, keep-alive and result conventions. The Rust core is statically
//! generic, so the Go-side options `WithEvictionPolicy(name)`,
//! `WithCommitmentScheme(name)` and `WithStorageEngine(name)` select one of a
//! fixed list of monomorphizations behind an enum (`AnyAuthWal`), all with
//! `K = V = Box<[u8]>`; an unknown combination is rejected at open. Panics
//! are converted to errors at the boundary, which is why the release profile
//! keeps `panic = "unwind"`.
//!
//! Exports are generated into `auth_wal.h` by cbindgen. Nothing is exported yet.
