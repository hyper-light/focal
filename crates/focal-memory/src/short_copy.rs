//! A copy of a few bytes without a call into the platform's `memcpy`.
//!
//! A copy whose length the compiler cannot see — `copy_from_slice` or `extend_from_slice` of a
//! slice — is a call to the C library's `memcpy`. The release links musl, whose x86-64 `memcpy`
//! moves an unaligned head and the tail a byte at a time (`movsb`) and the rest with `rep movsq`,
//! whose start-up costs tens of cycles; for the one- to thirty-two-byte fields an encoder writes
//! one at a time, the call is the whole cost. On the comparison's nodes `memcpy` took 12–23% of
//! the CPU, and 42% of a checkpoint's encoding, nearly all of it in copies of single fields
//! (cluster profile run 38187112271).
//!
//! A short copy here is a pair of fixed-size moves that overlap where the length falls between
//! two sizes — the technique of glibc's and LLVM libc's `memcpy` for small sizes — which the
//! compiler emits as plain loads and stores. Anything longer is `copy_from_slice`'s, where the
//! call is paid once for many bytes.

/// The longest copy made of fixed-size moves.
pub const SHORT_COPY: usize = 32;

/// Copies `src` into `dst`. Refused (`false`, `dst` unchanged) unless they are as long.
#[inline]
pub fn copy(dst: &mut [u8], src: &[u8]) -> bool {
    let n = src.len();
    if dst.len() != n {
        return false;
    }
    match n {
        0 => true,
        1..=3 => {
            // The first, middle and last bytes cover a length of one to three.
            let middle = n / 2;
            let last = n.saturating_sub(1);
            match (
                take::<1>(src, 0),
                take::<1>(src, middle),
                take::<1>(src, last),
            ) {
                (Some(first), Some(mid), Some(end)) => {
                    put(dst, 0, first) && put(dst, middle, mid) && put(dst, last, end)
                }
                _ => false,
            }
        }
        4..=7 => overlapping::<4>(dst, src, n),
        8..=15 => overlapping::<8>(dst, src, n),
        16..=SHORT_COPY => overlapping::<16>(dst, src, n),
        _ => {
            dst.copy_from_slice(src);
            true
        }
    }
}

/// `N` bytes at `at` and `N` ending at `n`: together every byte of a copy of `N` to `2N`.
#[inline]
fn overlapping<const N: usize>(dst: &mut [u8], src: &[u8], n: usize) -> bool {
    let Some(tail) = n.checked_sub(N) else {
        return false;
    };
    match (take::<N>(src, 0), take::<N>(src, tail)) {
        (Some(head), Some(end)) => put(dst, 0, head) && put(dst, tail, end),
        _ => false,
    }
}

#[inline]
fn take<const N: usize>(src: &[u8], at: usize) -> Option<[u8; N]> {
    src.get(at..)?.get(..N)?.try_into().ok()
}

#[inline]
fn put<const N: usize>(dst: &mut [u8], at: usize, value: [u8; N]) -> bool {
    match dst
        .get_mut(at..)
        .and_then(|rest| rest.get_mut(..N))
        .and_then(|room| <&mut [u8; N]>::try_from(room).ok())
    {
        Some(room) => {
            *room = value;
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_length_to_twice_the_short_bound_copies_exactly() {
        let source: Vec<u8> = (1..=80u8).collect();
        for n in 0..=2 * SHORT_COPY + 3 {
            let mut target = vec![0xAA; n];
            assert!(copy(&mut target, &source[..n]), "length {n}");
            assert_eq!(target, &source[..n], "length {n}");
        }
    }

    #[test]
    fn a_copy_between_unequal_lengths_is_refused_and_changes_nothing() {
        let mut target = [7u8; 5];
        assert!(!copy(&mut target, &[1, 2, 3]));
        assert_eq!(target, [7; 5]);
    }
}
