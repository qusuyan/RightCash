//! Up to eight independent keyed BLAKE3 hashes at once, one message per
//! 32-bit lane. `merkle_blake3` hashes thousands of short, independent
//! messages per proposal (a leaf per entry, a node per run of children);
//! the blake3 crate parallelises the chunks of one long input, not separate
//! inputs, so it compresses these one block at a time.
//!
//! Only single-chunk messages (at most `MAX_LEN` = 1024 bytes) are taken:
//! each is compressed block by block from the key, with `CHUNK_START` on its
//! first block and `CHUNK_END | ROOT` on its last, its final partial block
//! zero-padded with its true length as the block length, exactly as BLAKE3
//! hashes a one-chunk input, so a lane's output equals
//! `blake3::keyed_hash(key, message)`. Lanes are processed in lockstep for
//! the longest message; a shorter message's hash is taken after its last
//! block and the lane's later compressions are discarded.
//!
//! The lanes are AVX2 256-bit vectors (x86_64, checked at run time): message
//! blocks are loaded per lane and transposed so that a vector holds one word
//! of every lane, and the outputs transposed back. The intrinsics used are
//! safe to call inside AVX2-enabled functions; the one `unsafe` is entering
//! them after the check. Without AVX2 each message goes to
//! `blake3::keyed_hash` on its own: a portable lane loop measured about 0.55x
//! that (the compiler does not vectorise it), so none is kept.

use crate::types::Digest;

/// Messages hashed together.
pub(crate) const LANES: usize = 8;
/// Longest message: one BLAKE3 chunk.
pub(crate) const MAX_LEN: usize = 1024;

/// `out[i] = blake3::keyed_hash(key, msgs[i])` for every `i < msgs.len()`.
/// Panics if there are more than `LANES` messages, fewer outputs than
/// messages, or a message longer than `MAX_LEN`.
pub(crate) fn keyed_hash_lanes(key: &[u8; 32], msgs: &[&[u8]], out: &mut [Digest]) {
    assert!(
        msgs.len() <= LANES && out.len() >= msgs.len(),
        "at most {LANES} messages, one output each"
    );
    assert!(
        msgs.iter().all(|m| m.len() <= MAX_LEN),
        "a lane message fits in one chunk"
    );
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        // Safety: the CPU supports AVX2, the only requirement of `avx2::hash`.
        return unsafe { avx2::hash(key, msgs, out) };
    }
    for (msg, digest) in msgs.iter().zip(out.iter_mut()) {
        *digest = *blake3::keyed_hash(key, msg).as_bytes();
    }
}

/// The lanes as 256-bit AVX2 vectors. The intrinsics used are safe to call
/// inside these AVX2-enabled functions; `hash` is entered only after a
/// run-time check that the CPU supports AVX2.
#[cfg(target_arch = "x86_64")]
mod avx2 {
    use std::arch::x86_64::*;

    use super::{Digest, LANES};

    const BLOCK_LEN: usize = 64;
    const IV: [u32; 8] = [
        0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A, 0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
    ];
    const MSG_SCHEDULE: [[usize; 16]; 7] = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        [2, 6, 3, 10, 7, 0, 4, 13, 1, 11, 12, 5, 9, 14, 15, 8],
        [3, 4, 10, 12, 13, 2, 7, 14, 6, 5, 9, 0, 11, 15, 8, 1],
        [10, 7, 12, 9, 14, 3, 13, 15, 4, 0, 11, 2, 5, 8, 1, 6],
        [12, 13, 9, 11, 15, 10, 14, 8, 7, 2, 5, 3, 0, 1, 6, 4],
        [9, 14, 11, 5, 8, 12, 15, 1, 13, 3, 0, 10, 2, 6, 4, 7],
        [11, 15, 5, 0, 1, 9, 8, 6, 14, 10, 2, 12, 3, 4, 7, 13],
    ];
    const CHUNK_START: u32 = 1 << 0;
    const CHUNK_END: u32 = 1 << 1;
    const ROOT: u32 = 1 << 3;
    const KEYED_HASH: u32 = 1 << 4;

    type Lanes = [u32; LANES];

    /// Blocks per lane: an empty message is one empty block; 0 for an unused lane.
    fn block_counts(msgs: &[&[u8]]) -> [usize; LANES] {
        std::array::from_fn(|l| msgs.get(l).map_or(0, |m| m.len().div_ceil(BLOCK_LEN).max(1)))
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn add(a: __m256i, b: __m256i) -> __m256i {
        _mm256_add_epi32(a, b)
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn rot16(x: __m256i) -> __m256i {
        _mm256_shuffle_epi8(
            x,
            _mm256_set_epi8(
                13, 12, 15, 14, 9, 8, 11, 10, 5, 4, 7, 6, 1, 0, 3, 2, 13, 12, 15, 14, 9, 8, 11, 10, 5, 4, 7, 6, 1, 0,
                3, 2,
            ),
        )
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn rot12(x: __m256i) -> __m256i {
        _mm256_or_si256(_mm256_srli_epi32::<12>(x), _mm256_slli_epi32::<20>(x))
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn rot8(x: __m256i) -> __m256i {
        _mm256_shuffle_epi8(
            x,
            _mm256_set_epi8(
                12, 15, 14, 13, 8, 11, 10, 9, 4, 7, 6, 5, 0, 3, 2, 1, 12, 15, 14, 13, 8, 11, 10, 9, 4, 7, 6, 5, 0, 3,
                2, 1,
            ),
        )
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn rot7(x: __m256i) -> __m256i {
        _mm256_or_si256(_mm256_srli_epi32::<7>(x), _mm256_slli_epi32::<25>(x))
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn load(x: &Lanes) -> __m256i {
        _mm256_setr_epi32(
            x[0] as i32,
            x[1] as i32,
            x[2] as i32,
            x[3] as i32,
            x[4] as i32,
            x[5] as i32,
            x[6] as i32,
            x[7] as i32,
        )
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn g(v: &mut [__m256i; 16], a: usize, b: usize, c: usize, d: usize, x: __m256i, y: __m256i) {
        v[a] = add(add(v[a], v[b]), x);
        v[d] = rot16(_mm256_xor_si256(v[d], v[a]));
        v[c] = add(v[c], v[d]);
        v[b] = rot12(_mm256_xor_si256(v[b], v[c]));
        v[a] = add(add(v[a], v[b]), y);
        v[d] = rot8(_mm256_xor_si256(v[d], v[a]));
        v[c] = add(v[c], v[d]);
        v[b] = rot7(_mm256_xor_si256(v[b], v[c]));
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    fn compress(cv: &mut [__m256i; 8], m: &[__m256i; 16], block_len: __m256i, flags: __m256i) {
        let zero = _mm256_setzero_si256();
        let mut v = [
            cv[0],
            cv[1],
            cv[2],
            cv[3],
            cv[4],
            cv[5],
            cv[6],
            cv[7],
            _mm256_set1_epi32(IV[0] as i32),
            _mm256_set1_epi32(IV[1] as i32),
            _mm256_set1_epi32(IV[2] as i32),
            _mm256_set1_epi32(IV[3] as i32),
            zero,
            zero,
            block_len,
            flags,
        ];
        macro_rules! round {
            ($r:expr) => {{
                const S: [usize; 16] = MSG_SCHEDULE[$r];
                g(&mut v, 0, 4, 8, 12, m[S[0]], m[S[1]]);
                g(&mut v, 1, 5, 9, 13, m[S[2]], m[S[3]]);
                g(&mut v, 2, 6, 10, 14, m[S[4]], m[S[5]]);
                g(&mut v, 3, 7, 11, 15, m[S[6]], m[S[7]]);
                g(&mut v, 0, 5, 10, 15, m[S[8]], m[S[9]]);
                g(&mut v, 1, 6, 11, 12, m[S[10]], m[S[11]]);
                g(&mut v, 2, 7, 8, 13, m[S[12]], m[S[13]]);
                g(&mut v, 3, 4, 9, 14, m[S[14]], m[S[15]]);
            }};
        }
        round!(0);
        round!(1);
        round!(2);
        round!(3);
        round!(4);
        round!(5);
        round!(6);
        for i in 0..8 {
            cv[i] = _mm256_xor_si256(v[i], v[i + 8]);
        }
    }

    /// Eight rows of eight words to eight columns: `out[w][l] = rows[l][w]`.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn transpose(r: [__m256i; 8]) -> [__m256i; 8] {
        let t0 = _mm256_unpacklo_epi32(r[0], r[1]);
        let t1 = _mm256_unpackhi_epi32(r[0], r[1]);
        let t2 = _mm256_unpacklo_epi32(r[2], r[3]);
        let t3 = _mm256_unpackhi_epi32(r[2], r[3]);
        let t4 = _mm256_unpacklo_epi32(r[4], r[5]);
        let t5 = _mm256_unpackhi_epi32(r[4], r[5]);
        let t6 = _mm256_unpacklo_epi32(r[6], r[7]);
        let t7 = _mm256_unpackhi_epi32(r[6], r[7]);
        let u0 = _mm256_unpacklo_epi64(t0, t2);
        let u1 = _mm256_unpackhi_epi64(t0, t2);
        let u2 = _mm256_unpacklo_epi64(t1, t3);
        let u3 = _mm256_unpackhi_epi64(t1, t3);
        let u4 = _mm256_unpacklo_epi64(t4, t6);
        let u5 = _mm256_unpackhi_epi64(t4, t6);
        let u6 = _mm256_unpacklo_epi64(t5, t7);
        let u7 = _mm256_unpackhi_epi64(t5, t7);
        [
            _mm256_permute2x128_si256::<0x20>(u0, u4),
            _mm256_permute2x128_si256::<0x20>(u1, u5),
            _mm256_permute2x128_si256::<0x20>(u2, u6),
            _mm256_permute2x128_si256::<0x20>(u3, u7),
            _mm256_permute2x128_si256::<0x31>(u0, u4),
            _mm256_permute2x128_si256::<0x31>(u1, u5),
            _mm256_permute2x128_si256::<0x31>(u2, u6),
            _mm256_permute2x128_si256::<0x31>(u3, u7),
        ]
    }

    /// Eight little-endian words of a 32-byte slice as one vector.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn load_bytes(b: &[u8; 32]) -> __m256i {
        let w = |i: usize| i32::from_le_bytes([b[4 * i], b[4 * i + 1], b[4 * i + 2], b[4 * i + 3]]);
        _mm256_setr_epi32(w(0), w(1), w(2), w(3), w(4), w(5), w(6), w(7))
    }

    /// One vector's eight words as 32 little-endian bytes.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn store_bytes(x: __m256i) -> Digest {
        let mut out = [0u8; 32];
        out[0..8].copy_from_slice(&_mm256_extract_epi64::<0>(x).to_le_bytes());
        out[8..16].copy_from_slice(&_mm256_extract_epi64::<1>(x).to_le_bytes());
        out[16..24].copy_from_slice(&_mm256_extract_epi64::<2>(x).to_le_bytes());
        out[24..32].copy_from_slice(&_mm256_extract_epi64::<3>(x).to_le_bytes());
        out
    }

    #[target_feature(enable = "avx2")]
    pub(super) fn hash(key: &[u8; 32], msgs: &[&[u8]], out: &mut [Digest]) {
        let blocks = block_counts(msgs);
        let rounds = blocks.iter().copied().max().unwrap_or(0);
        let mut cv: [__m256i; 8] =
            std::array::from_fn(|i| _mm256_set1_epi32(i32::from_le_bytes(key[4 * i..4 * i + 4].try_into().unwrap())));
        let zero = [0u8; BLOCK_LEN];
        for b in 0..rounds {
            // Each lane's block (zero-padded when partial, zeros when the
            // lane is done or unused) as two rows of eight words, then
            // transposed so vector `w` holds word `w` of every lane.
            let mut padded = [[0u8; BLOCK_LEN]; LANES];
            let mut lo = [_mm256_setzero_si256(); LANES];
            let mut hi = [_mm256_setzero_si256(); LANES];
            let mut block_len = [0u32; LANES];
            let mut flags = [KEYED_HASH; LANES];
            for (l, msg) in msgs.iter().enumerate() {
                let start = b * BLOCK_LEN;
                let block: &[u8; BLOCK_LEN] = if b >= blocks[l] {
                    &zero
                } else {
                    let bytes = &msg[start..msg.len().min(start + BLOCK_LEN)];
                    block_len[l] = bytes.len() as u32;
                    if b == 0 {
                        flags[l] |= CHUNK_START;
                    }
                    if b + 1 == blocks[l] {
                        flags[l] |= CHUNK_END | ROOT;
                    }
                    match bytes.try_into() {
                        Ok(full) => full,
                        Err(_) => {
                            padded[l][..bytes.len()].copy_from_slice(bytes);
                            &padded[l]
                        }
                    }
                };
                lo[l] = load_bytes(block[..32].try_into().unwrap());
                hi[l] = load_bytes(block[32..].try_into().unwrap());
            }
            let lo = transpose(lo);
            let hi = transpose(hi);
            let m: [__m256i; 16] = std::array::from_fn(|w| if w < 8 { lo[w] } else { hi[w - 8] });
            compress(&mut cv, &m, load(&block_len), load(&flags));
            if (0..msgs.len()).any(|l| b + 1 == blocks[l]) {
                let rows = transpose(cv);
                for (l, digest) in out.iter_mut().enumerate().take(msgs.len()) {
                    if b + 1 == blocks[l] {
                        *digest = store_bytes(rows[l]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::common::Rng;

    fn check(key: &[u8; 32], msgs: &[&[u8]]) {
        let mut out = [[0u8; 32]; LANES];
        keyed_hash_lanes(key, msgs, &mut out);
        for (l, msg) in msgs.iter().enumerate() {
            let expected = *blake3::keyed_hash(key, msg).as_bytes();
            assert_eq!(out[l], expected, "lane {l} of {}, length {}", msgs.len(), msg.len());
        }
        // Nothing written past the last message's output.
        assert!(out[msgs.len()..].iter().all(|digest| *digest == [0u8; 32]));
    }

    /// Checks the AVX2 lanes where the CPU has AVX2 (elsewhere the lanes are
    /// the blake3 crate itself, and this only checks the plumbing).
    #[test]
    fn every_lane_matches_the_blake3_crate() {
        let data: Vec<u8> = (0..MAX_LEN * LANES).map(|i| (i * 31 + 7) as u8).collect();
        let key = blake3::derive_key("auth_wal/blake3_lanes test", &[]);
        // Every length around the block boundaries, alone and in a full set
        // of lanes of one length.
        for len in [0, 1, 4, 31, 63, 64, 65, 77, 127, 128, 129, 512, 1000, 1023, 1024] {
            check(&key, &[&data[..len]]);
            let msgs: Vec<&[u8]> = (0..LANES).map(|l| &data[l * MAX_LEN..l * MAX_LEN + len]).collect();
            check(&key, &msgs);
        }
        // Mixed lengths and lane counts, including lanes finishing early.
        let mut rng = Rng(5);
        for _ in 0..300 {
            let count = 1 + rng.below(LANES as u64) as usize;
            let msgs: Vec<&[u8]> = (0..count)
                .map(|l| {
                    let len = rng.below(MAX_LEN as u64 + 1) as usize;
                    &data[l * MAX_LEN..l * MAX_LEN + len]
                })
                .collect();
            check(&key, &msgs);
        }
        check(&key, &[]);
    }

    #[test]
    #[should_panic(expected = "one chunk")]
    fn rejects_a_message_longer_than_a_chunk() {
        let long = vec![0u8; MAX_LEN + 1];
        keyed_hash_lanes(&[0; 32], &[&long], &mut [[0; 32]]);
    }
}
