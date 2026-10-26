//! Root composition (design §7.3, R12): the validator sees exactly one
//! commitment per proposal, revision and committed state.
//!
//! ```text
//! root = EMPTY_ROOT                                           if height == 0
//!      = SHA-256( DOMAIN
//!               || len || scheme_id || len || policy_name || len || engine_id   (u32 LE length-prefixed)
//!               || scheme_params || policy_params              (digests)
//!               || capacity || low_watermark                   (u64 LE, u64 LE)
//!               || height                                      (u64 LE)
//!               || set_commitment || policy_digest || storage_root
//!               || metadata_hash )                             (None -> zeros; Some(m) -> SHA-256(m))
//! ```
//!
//! Nothing about speculative eviction or prefetch hints enters the preimage
//! (design §9.3); the type system keeps `Local` settings out of
//! `RootPreimage` by construction.

use crate::types::{Digest, Hash};

pub const DOMAIN: &[u8] = b"auth_wal/v1";

/// Everything that enters the preimage, owned, in preimage order. Built by the
/// proposal tree for every node, consumed by [`combine`], and embedded
/// verbatim in an `AuthProof` (design §7.4) so the two can never drift.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootPreimage {
    pub scheme_id: &'static str,
    pub policy_name: &'static str,
    pub engine_id: &'static str,
    pub scheme_params: Digest,
    pub policy_params: Digest,
    pub capacity: u64,
    pub low_watermark: u64,
    pub height: u64,
    pub set_commitment: Digest,
    pub policy_digest: Digest,
    pub storage_root: Hash,
    pub metadata_hash: Digest,
}

/// Combines the terms into the single root (design §7.3).
pub fn combine(preimage: &RootPreimage) -> Hash {
    todo!("design §7.3; height == 0 -> EMPTY_ROOT")
}

/// `None` -> 32 zero bytes; `Some(m)` -> SHA-256(m). `Some(empty)` is distinct
/// from `None`, as in qmdb-ffi (design §7.3).
pub fn metadata_hash(metadata: Option<&[u8]>) -> Digest {
    todo!()
}
