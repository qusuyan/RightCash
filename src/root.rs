//! Root composition (design §7.3, R12): the validator sees exactly one
//! commitment per proposal, revision and committed state.
//!
//! ```text
//! root = EMPTY_ROOT                                           if height == 0
//!      = SHA-256( DOMAIN
//!               || len || scheme_id || len || policy_name || len || engine_id   (u32 LE length-prefixed)
//!               || scheme_params || policy_params              (digests)
//!               || capacity                                   (u64 LE: in force at the node, §6.2)
//!               || height                                      (u64 LE)
//!               || set_commitment || policy_digest || storage_root
//!               || metadata_hash )                             (None -> zeros; Some(m) -> SHA-256(m))
//! ```
//!
//! The policy's state enters as a term of its own, `policy_digest =
//! EvictionPolicy::state_digest()` (design §6.1, §7.3, R25), so equal roots
//! imply equal eviction order and two nodes with equal roots are
//! interchangeable as parents; `policy_params` covers the policy's own
//! parameters, the low watermark for the v0 policies (R23). Nothing about
//! speculative eviction or prefetch hints enters the preimage (design §9.3:
//! the digest covers the consensus state, never the prediction marks); the
//! type system keeps `Local` settings out of `Terms` by construction.

use crate::types::{Digest, Hash};

pub const DOMAIN: &[u8] = b"auth_wal/v1";

/// The preimage's static terms (design §7.3): computed once at `new` from
/// the scheme, the policy and the engine and kept on the writer; no node
/// changes them. [`Terms::combine`] adds a node's own
/// terms and hashes. A node keeps only its `metadata_hash`, the one term not
/// derivable from it (design §8).
#[derive(Clone, Debug)]
pub struct Terms {
    pub scheme_id: &'static str,
    pub policy_name: &'static str,
    pub engine_id: &'static str,
    pub scheme_params: Digest,
    pub policy_params: Digest,
}

impl Terms {
    /// The root of one node (design §7.3): `EMPTY_ROOT` at height 0, else
    /// SHA-256 over the preimage above, the node's own terms in the
    /// preimage's order. Nothing is kept: a proof (design §7.4) calls this again.
    pub fn combine(
        &self,
        capacity: usize,
        height: u64,
        set_commitment: Digest,
        policy_digest: Digest,
        storage_root: Hash,
        metadata_hash: Digest,
    ) -> Hash {
        todo!("design §7.3; height == 0 -> EMPTY_ROOT; capacity as u64 LE")
    }
}

/// `None` -> 32 zero bytes; `Some(m)` -> SHA-256(m). `Some(empty)` is distinct
/// from `None`, as in qmdb-ffi (design §7.3).
pub fn metadata_hash(metadata: Option<&[u8]>) -> Digest {
    todo!()
}
