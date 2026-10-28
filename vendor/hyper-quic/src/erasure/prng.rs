//! TinyMT32 (RFC 8682), the generator RFC 8681 draws its coding coefficients from, with the
//! parameters RFC 8682 §2.1 makes mandatory, and RFC 8681's coefficients for GF(2^8) at the
//! largest density (§3.6, `DT` 15): every coefficient a nonzero byte.

/// RFC 8682 §2.1's mandatory parameter set, its `mat1`.
const MAT1: u32 = 0x8f70_11ee;
/// RFC 8682 §2.1's `mat2`.
const MAT2: u32 = 0xfc78_ff1f;
/// RFC 8682 §2.1's `tmat`.
const TMAT: u32 = 0x3793_fdff;
/// RFC 8682 Figure 1's `TINYMT32_SH0`.
const SH0: u32 = 1;
/// RFC 8682 Figure 1's `TINYMT32_SH1`.
const SH1: u32 = 10;
/// RFC 8682 Figure 1's `TINYMT32_SH8`.
const SH8: u32 = 8;
/// RFC 8682 Figure 1's `TINYMT32_MASK`.
const MASK: u32 = 0x7fff_ffff;
/// RFC 8682 Figure 1's `MIN_LOOP`: the rounds of the seeding loop.
const MIN_LOOP: u32 = 8;
/// RFC 8682 Figure 1's `PRE_LOOP`: the rounds of the state's after it.
const PRE_LOOP: u32 = 8;
/// The seeding loop's multiplier (RFC 8682 Figure 1, Knuth's from MT19937's initialization).
const MULTIPLIER: u32 = 1_812_433_253;

/// TinyMT32's state.
pub(crate) struct TinyMt32 {
    status: [u32; 4],
}

impl TinyMt32 {
    /// The generator seeded with `seed` (RFC 8682's `tinymt32_init`).
    pub(crate) fn new(seed: u32) -> Self {
        let mut status = [seed, MAT1, MAT2, TMAT];
        for i in 1..MIN_LOOP {
            let previous = word(&status, i.wrapping_sub(1));
            let mixed = i.wrapping_add(MULTIPLIER.wrapping_mul(previous ^ (previous >> 30)));
            if let Some(slot) = status.get_mut(usize::try_from(i & 3).unwrap_or(0)) {
                *slot ^= mixed;
            }
        }
        // Period certification: the all-zero state is the one state the recurrence never leaves.
        if let [first, second, third, fourth] = &mut status
            && *first & MASK == 0
            && *second == 0
            && *third == 0
            && *fourth == 0
        {
            *first = u32::from(b'T');
            *second = u32::from(b'I');
            *third = u32::from(b'N');
            *fourth = u32::from(b'Y');
        }
        let mut generator = Self { status };
        for _ in 0..PRE_LOOP {
            generator.next_state();
        }
        generator
    }

    fn next_state(&mut self) {
        let [first, second, third, fourth] = &mut self.status;
        let mut y = *fourth;
        let mut x = (*first & MASK) ^ *second ^ *third;
        x ^= x << SH0;
        y ^= (y >> SH0) ^ x;
        *first = *second;
        *second = *third;
        *third = x ^ (y << SH1);
        *fourth = y;
        if y & 1 != 0 {
            *second ^= MAT1;
            *third ^= MAT2;
        }
    }

    /// The next 32 bits (RFC 8682's `tinymt32_generate_uint32`).
    pub(crate) fn next_u32(&mut self) -> u32 {
        self.next_state();
        let [first, _, third, fourth] = &self.status;
        let mut t0 = *fourth;
        let t1 = first.wrapping_add(*third >> SH8);
        t0 ^= t1;
        if t1 & 1 != 0 {
            t0 ^= TMAT;
        }
        t0
    }

    /// The next byte: the 8 least significant bits (RFC 8681 §3.5, `tinymt32_rand256`).
    fn next_byte(&mut self) -> u8 {
        u8::try_from(self.next_u32() & 0xff).unwrap_or(0)
    }
}

fn word(status: &[u32; 4], i: u32) -> u32 {
    status
        .get(usize::try_from(i & 3).unwrap_or(0))
        .copied()
        .unwrap_or(0)
}

/// The most draws a coefficient takes: RFC 8681 draws again while a byte is zero, a loop that ends
/// with probability one; past this many (2^-2048 of the time) the coefficient is one, at both
/// ends alike.
const DRAWS: u32 = 256;

/// RFC 8681's coding coefficients for `key` at GF(2^8) and density 15 (§3.6,
/// `generate_coding_coefficients`): `count` nonzero bytes, written over `into`; false, and
/// `into` empty, when the memory for them could not be had.
pub(crate) fn coefficients(key: u16, count: usize, into: &mut Vec<u8>) -> bool {
    into.clear();
    if into.try_reserve_exact(count).is_err() {
        return false;
    }
    let mut generator = TinyMt32::new(u32::from(key));
    for _ in 0..count {
        let mut coefficient = 1;
        for _ in 0..DRAWS {
            let drawn = generator.next_byte();
            if drawn != 0 {
                coefficient = drawn;
                break;
            }
        }
        into.push(coefficient);
    }
    true
}
