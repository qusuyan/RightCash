//! Library tests grouped by subject. These remain unit tests because they
//! verify crate-private snapshots, storage views, and worker state as well
//! as public behavior. Implementation-local tests stay inline in their modules.

mod cache;
pub(crate) mod common;
mod encodings;
mod faults;
mod persist;
mod policy;
mod speculation;
mod storage;
