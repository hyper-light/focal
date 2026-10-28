use super::field::{EXP, LOG, POLYNOMIAL, add_scaled, inverse, mul, scale};
use super::prng::{TinyMt32, coefficients};
use super::{Decoder, Encoder};
use crate::Instant;

/// A product by shift and reduction, the field's definition, against which the tables are held.
fn slow_mul(a: u8, b: u8) -> u8 {
    let (mut a, mut b, mut product) = (u16::from(a), b, 0u16);
    while b != 0 {
        if b & 1 != 0 {
            product ^= a;
        }
        a <<= 1;
        if a & 0x100 != 0 {
            a ^= POLYNOMIAL;
        }
        b >>= 1;
    }
    u8::try_from(product).unwrap()
}

#[test]
fn the_tables_are_the_powers_of_two_modulo_the_polynomial() {
    let mut power = 1u8;
    for (i, value) in EXP.iter().enumerate() {
        assert_eq!(*value, power, "2^{i}");
        if i < 255 {
            assert_eq!(usize::from(LOG[usize::from(power)]), i);
        }
        power = slow_mul(power, 2);
    }
    // Two generates the group: its powers below its order are distinct.
    let mut seen = [false; 256];
    for value in &EXP[..255] {
        assert!(!seen[usize::from(*value)]);
        seen[usize::from(*value)] = true;
    }
}

#[test]
fn every_product_and_inverse_is_the_fields() {
    for a in 0..=255u8 {
        for b in 0..=255u8 {
            assert_eq!(mul(a, b), slow_mul(a, b), "{a} · {b}");
        }
        if a != 0 {
            assert_eq!(mul(a, inverse(a)), 1, "{a}⁻¹");
        }
    }
    let from: Vec<u8> = (0..=255).collect();
    for factor in [0u8, 1, 2, 0x8e, 0xff] {
        let mut into: Vec<u8> = (0..=255u8).rev().collect();
        let expected: Vec<u8> = into
            .iter()
            .zip(&from)
            .map(|(to, by)| to ^ slow_mul(factor, *by))
            .collect();
        add_scaled(&mut into, &from, factor);
        assert_eq!(into, expected, "factor {factor}");
        let mut scaled = from.clone();
        scale(&mut scaled, factor);
        assert_eq!(
            scaled,
            from.iter()
                .map(|by| slow_mul(factor, *by))
                .collect::<Vec<_>>()
        );
    }
}

/// RFC 8682 §2.3's validation: the first 50 outputs of `tinymt32_generate_uint32` for seed 1.
#[test]
fn tinymt32_gives_rfc_8682s_validation_outputs() {
    const OUTPUTS: [u32; 50] = [
        2545341989, 981918433, 3715302833, 2387538352, 3591001365, 3820442102, 2114400566,
        2196103051, 2783359912, 764534509, 643179475, 1822416315, 881558334, 4207026366,
        3690273640, 3240535687, 2921447122, 3984931427, 4092394160, 44209675, 2188315343,
        2908663843, 1834519336, 3774670961, 3019990707, 4065554902, 1239765502, 4035716197,
        3412127188, 552822483, 161364450, 353727785, 140085994, 149132008, 2547770827, 4064042525,
        4078297538, 2057335507, 622384752, 2041665899, 2193913817, 1080849512, 33160901, 662956935,
        642999063, 3384709977, 1723175122, 3866752252, 521822317, 2292524454,
    ];
    let mut generator = TinyMt32::new(1);
    for (i, expected) in OUTPUTS.iter().enumerate() {
        assert_eq!(generator.next_u32(), *expected, "output {i}");
    }
}

#[test]
fn coefficients_are_nonzero_and_the_keys_own() {
    let mut first = Vec::new();
    let mut again = Vec::new();
    for key in [0u16, 1, 7, u16::MAX] {
        assert!(coefficients(key, 300, &mut first));
        assert!(coefficients(key, 300, &mut again));
        assert_eq!(first.len(), 300);
        assert_eq!(first, again);
        assert!(first.iter().all(|coefficient| *coefficient != 0));
    }
    assert!(coefficients(1, 64, &mut first));
    assert!(coefficients(2, 64, &mut again));
    assert_ne!(first, again);
    // RFC 8681 draws a byte and keeps the nonzero ones: the first coefficient of key 1 is the low
    // byte of RFC 8682's first output for seed 1, 2545341989 = 0x97B6_3B25.
    assert!(coefficients(1, 1, &mut first));
    assert_eq!(first, [0x25]);
}

/// A deterministic stream of bytes for the tests' payloads and losses (xorshift64*).
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
    fn payload(&mut self, longest: usize) -> Vec<u8> {
        let length = 1 + usize::try_from(self.below(u64::try_from(longest).unwrap())).unwrap();
        (0..length)
            .map(|_| u8::try_from(self.next() & 0xff).unwrap())
            .collect()
    }
}

/// `rebuilt`, a payload at a repair's length, is `original` followed by zeros.
fn is_rebuilt_from(rebuilt: &[u8], original: &[u8]) -> bool {
    rebuilt.len() >= original.len()
        && rebuilt[..original.len()] == *original
        && rebuilt[original.len()..].iter().all(|byte| *byte == 0)
}

#[test]
fn one_repair_rebuilds_the_one_source_lost_in_its_span() {
    let mut draws = Draws(7);
    let mut encoder = Encoder::new(16);
    let mut decoder = Decoder::new(16);
    let sources: Vec<Vec<u8>> = (0..8).map(|_| draws.payload(1_200)).collect();
    let mut rebuilt = Vec::new();
    for (id, payload) in sources.iter().enumerate() {
        assert!(encoder.sent(payload, 0, Instant::now()));
        if id != 5 {
            decoder.received(u64::try_from(id).unwrap(), payload, &mut rebuilt);
        }
    }
    assert!(rebuilt.is_empty());
    let mut repair = Vec::new();
    let span = encoder.repair(8, &mut repair).unwrap();
    assert_eq!((span.first, span.count), (0, 8));
    decoder.repair(span, &repair, &mut rebuilt);
    assert_eq!(rebuilt.len(), 1);
    assert!(is_rebuilt_from(&rebuilt[0], &sources[5]));
    assert_eq!(decoder.pending(), 0);
}

#[test]
fn repairs_rebuild_as_many_losses_as_they_number() {
    // Over many seeds, a span of 32 sources losing `lost` of them is rebuilt by `lost` repairs
    // over it, or by one more when the first are dependent: the coefficients are random, and a
    // random square system over GF(2^8) is singular about once in 255.
    let mut short = 0;
    for seed in 1..=200u64 {
        let mut draws = Draws(seed);
        let mut encoder = Encoder::new(64);
        let mut decoder = Decoder::new(64);
        let sources: Vec<Vec<u8>> = (0..32).map(|_| draws.payload(1_400)).collect();
        let lost_count = 1 + usize::try_from(draws.below(6)).unwrap();
        let mut lost = Vec::new();
        while lost.len() < lost_count {
            let id = usize::try_from(draws.below(32)).unwrap();
            if !lost.contains(&id) {
                lost.push(id);
            }
        }
        let mut rebuilt = Vec::new();
        for (id, payload) in sources.iter().enumerate() {
            assert!(encoder.sent(payload, 0, Instant::now()));
            if !lost.contains(&id) {
                decoder.received(u64::try_from(id).unwrap(), payload, &mut rebuilt);
            }
        }
        let mut repairs = 0;
        while rebuilt.len() < lost_count && repairs < lost_count + 2 {
            let mut repair = Vec::new();
            let span = encoder.repair(32, &mut repair).unwrap();
            decoder.repair(span, &repair, &mut rebuilt);
            repairs += 1;
        }
        assert_eq!(rebuilt.len(), lost_count, "seed {seed}: {lost:?}");
        if repairs > lost_count {
            short += 1;
        }
        for payload in &rebuilt {
            assert!(
                lost.iter()
                    .any(|id| is_rebuilt_from(payload, &sources[*id])),
                "seed {seed}: a rebuilt source is none of those lost"
            );
        }
        assert_eq!(decoder.pending(), 0);
    }
    assert!(short <= 5, "{short} of 200 needed a repair more");
}

#[test]
fn a_source_that_arrives_after_its_repair_is_substituted() {
    // Two sources missing when the first repair comes: nothing rebuilds. One arrives late, and
    // the other is rebuilt from the same equation.
    let mut draws = Draws(3);
    let mut encoder = Encoder::new(16);
    let mut decoder = Decoder::new(16);
    let sources: Vec<Vec<u8>> = (0..6).map(|_| draws.payload(900)).collect();
    let mut rebuilt = Vec::new();
    for (id, payload) in sources.iter().enumerate() {
        assert!(encoder.sent(payload, 0, Instant::now()));
        if id != 1 && id != 4 {
            decoder.received(u64::try_from(id).unwrap(), payload, &mut rebuilt);
        }
    }
    let mut repair = Vec::new();
    let span = encoder.repair(6, &mut repair).unwrap();
    decoder.repair(span, &repair, &mut rebuilt);
    assert!(rebuilt.is_empty());
    assert_eq!(decoder.pending(), 1);
    decoder.received(4, &sources[4], &mut rebuilt);
    assert_eq!(rebuilt.len(), 1);
    assert!(is_rebuilt_from(&rebuilt[0], &sources[1]));
    assert_eq!(decoder.pending(), 0);
}

#[test]
fn sliding_repairs_rebuild_losses_across_overlapping_spans() {
    // A stream of 400 sources, 5% lost at random, a repair over the last 24 after every 8th:
    // the overlapping equations rebuild every loss but those of the last span's tail.
    let mut draws = Draws(11);
    let mut encoder = Encoder::new(64);
    let mut decoder = Decoder::new(64);
    let mut sources = Vec::new();
    let mut lost = Vec::new();
    let mut rebuilt = Vec::new();
    for id in 0..400u64 {
        let payload = draws.payload(1_300);
        assert!(encoder.sent(&payload, 0, Instant::now()));
        if draws.below(100) < 5 {
            lost.push(id);
        } else {
            decoder.received(id, &payload, &mut rebuilt);
        }
        sources.push(payload);
        if id % 8 == 7 {
            let mut repair = Vec::new();
            let span = encoder.repair(24, &mut repair).unwrap();
            // Repairs are lost as sources are
            if draws.below(100) >= 5 {
                decoder.repair(span, &repair, &mut rebuilt);
            }
        }
    }
    let found = rebuilt
        .iter()
        .filter(|payload| {
            lost.iter()
                .any(|id| is_rebuilt_from(payload, &sources[usize::try_from(*id).unwrap()]))
        })
        .count();
    assert_eq!(found, rebuilt.len());
    assert!(
        found + 2 >= lost.len(),
        "{found} of {} losses rebuilt",
        lost.len()
    );
}

#[test]
fn the_decoder_holds_no_more_than_its_bound_and_forgets_what_falls_below() {
    let mut draws = Draws(5);
    let mut encoder = Encoder::new(8);
    let mut decoder = Decoder::new(8);
    let mut rebuilt = Vec::new();
    for id in 0..100u64 {
        let payload = draws.payload(1_000);
        assert!(encoder.sent(&payload, 0, Instant::now()));
        assert!(encoder.held() <= 8);
        // Every third lost and never repaired: the equations over them go with the bound
        if id % 3 != 0 {
            decoder.received(id, &payload, &mut rebuilt);
        }
        if id % 5 == 4 {
            let mut repair = Vec::new();
            let span = encoder.repair(8, &mut repair).unwrap();
            // Only half the repairs arrive, so some losses stay
            if id % 10 == 4 {
                decoder.repair(span, &repair, &mut rebuilt);
            }
        }
        assert!(decoder.known.len() <= 8);
        assert!(decoder.rows.len() <= 8);
        assert!(decoder.known.keys().all(|known| *known >= decoder.floor));
    }
    // A repair over sources now forgotten is dropped
    let mut repair = Vec::new();
    let span = super::Span {
        first: 0,
        count: 8,
        key: 3,
    };
    repair.resize(100, 1);
    let before = decoder.pending();
    decoder.repair(span, &repair, &mut rebuilt);
    assert_eq!(decoder.pending(), before);
}
