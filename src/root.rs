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
//! EvictionPolicy::state_digest()` (design §6.1, §7.3, R25). Subject to the
//! hash schemes' collision resistance, equal non-genesis roots authenticate
//! the same state, including eviction order. `policy_params` covers the policy's own
//! parameters, the low watermark for the v0 policies (R23). Nothing about
//! speculative eviction or prefetch hints enters the preimage (design §9.3:
//! the digest covers the consensus state, never the prediction marks); the
//! type system keeps `Local` settings out of `Terms` by construction.

use sha2::{Digest as _, Sha256};

use crate::types::{Digest, EMPTY_ROOT, Hash, len_prefix};

pub const DOMAIN: &[u8] = b"auth_wal/v1";

/// The preimage's static terms (design §7.3): computed once at `new` from
/// the scheme, the policy and the engine and kept on the writer; no node
/// changes them. [`Terms::combine`] adds a node's own
/// terms and hashes. A node keeps its root only: a proof (milestone 5) takes
/// the metadata from the caller and rebuilds the preimage with `combine`
/// (design §7.4, §8).
#[derive(Clone, Debug)]
pub struct Terms {
    pub scheme_id: &'static str,
    pub policy_name: &'static str,
    pub engine_id: &'static str,
    pub scheme_params: Digest,
    pub policy_params: Digest,
}

impl Terms {
    /// Return a node's combined root from these static terms and its state:
    ///
    /// - `capacity`: the node's maximum write-set entry count, tombstones included.
    /// - `height`: the node's block height; zero selects the genesis sentinel.
    /// - `set_commitment`: digest of the node's retained write-set entries.
    /// - `policy_digest`: digest of its consensus eviction-policy state.
    /// - `storage_root`: root of its pinned storage view, including evicted entries.
    /// - `metadata_hash`: already-hashed metadata, as returned by `metadata_hash`.
    ///
    /// At height zero, returns `EMPTY_ROOT` without checking any other input;
    /// callers must ensure that height zero represents the empty initial state.
    /// Otherwise returns SHA-256 of the encoding above. Stores no inputs and
    /// does not validate that the supplied digests describe the same node.
    pub fn combine(
        &self,
        capacity: usize,
        height: u64,
        set_commitment: Digest,
        policy_digest: Digest,
        storage_root: Hash,
        metadata_hash: Digest,
    ) -> Hash {
        if height == 0 {
            return EMPTY_ROOT;
        }
        let mut hasher = Sha256::new();
        hasher.update(DOMAIN);
        for term in [self.scheme_id, self.policy_name, self.engine_id] {
            hasher.update(len_prefix(term.len()));
            hasher.update(term.as_bytes());
        }
        hasher.update(self.scheme_params);
        hasher.update(self.policy_params);
        hasher.update((capacity as u64).to_le_bytes());
        hasher.update(height.to_le_bytes());
        hasher.update(set_commitment);
        hasher.update(policy_digest);
        hasher.update(storage_root.0);
        hasher.update(metadata_hash);
        Hash(hasher.finalize().into())
    }
}

/// `None` -> 32 zero bytes; `Some(m)` -> SHA-256(m). `Some(empty)` is distinct
/// from `None`, as in qmdb-ffi (design §7.3).
pub fn metadata_hash(metadata: Option<&[u8]>) -> Digest {
    match metadata {
        None => [0u8; 32],
        Some(bytes) => Sha256::digest(bytes).into(),
    }
}
