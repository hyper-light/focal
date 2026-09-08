use super::*;
use crate::native::input_codec::{encode, frame_tests};
use focal_model::{RequestEpoch, RequestId};

fn input(command: NativeCommand) -> NativeInput {
    NativeInput {
        request: RequestKey {
            principal: ParticipantId::from_u128(13),
            epoch: RequestEpoch(0x0102_0304_0506_0708),
            id: RequestId::from_u128(15),
        },
        command,
    }
}
fn encoded(input: &NativeInput) -> Vec<u8> {
    let profile = if matches!(input.command, NativeCommand::CreateAuthored { .. }) {
        NativeContentProfile::AuthoredV1
    } else {
        NativeContentProfile::ProjectionOnly
    };
    let plan = EncodingPlan::prepare(
        frame_tests::frame(input, profile),
        EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 24,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn limits() -> NativeDecodeLimits {
    NativeDecodeLimits::for_native(
        NativeLimits::default(),
        1 << 20,
        DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        },
    )
    .unwrap()
}
fn plus(a: DecodeWork, b: DecodeWork) -> DecodeWork {
    DecodeWork {
        parse: a.parse + b.parse,
        source: a.source + b.source,
        model: a.model + b.model,
        acceptance: a.acceptance + b.acceptance,
        native: a.native + b.native,
    }
}

#[test]
fn unified_actor_decoder_preserves_all_twenty_eight_frames_at_exact_cumulative_work_limits() {
    for command in frame_tests::commands() {
        let tag = encode::tag(&command);
        let input = input(command);
        let bytes = encoded(&input);
        let quote = limits()
            .with_request(NativeLimits::default(), &bytes, |plan, quote| {
                assert_eq!(
                    plan.intent_fingerprint(),
                    crate::native::intent::fingerprint(plan.header().ledger, &input).unwrap()
                );
                assert_eq!(encoded(&plan.build().unwrap()), bytes, "command {tag}");
                quote
            })
            .unwrap();
        let exact = NativeDecodeLimits {
            work: plus(quote.preparation, quote.construction),
            ..limits()
        };
        exact
            .with_request(NativeLimits::default(), &bytes, |plan, repeated| {
                assert_eq!(repeated, quote);
                assert_eq!(
                    encoded(&plan.build().unwrap()),
                    bytes,
                    "exact command {tag}"
                );
            })
            .unwrap();
    }
}

#[test]
fn cumulative_domains_cannot_replenish_each_other_or_forget_initial_inspection() {
    for command in frame_tests::commands() {
        let tag = encode::tag(&command);
        let bytes = encoded(&input(command));
        let quote = limits()
            .with_request(NativeLimits::default(), &bytes, |_, quote| quote)
            .unwrap();
        for domain in 0..5 {
            let mut work = plus(quote.preparation, quote.construction);
            let field = match domain {
                0 => &mut work.parse,
                1 => &mut work.source,
                2 => &mut work.model,
                3 => &mut work.acceptance,
                _ => &mut work.native,
            };
            if *field == 0 {
                continue;
            }
            *field -= 1;
            // Plenty of another domain cannot pay the exhausted one.
            if domain == 4 {
                work.model += 100_000;
            } else {
                work.native += 100_000;
            }
            let result = NativeDecodeLimits { work, ..limits() }.with_request(
                NativeLimits::default(),
                &bytes,
                |plan, _| plan.build(),
            );
            assert!(
                !matches!(result, Ok(Ok(_))),
                "command {tag}, domain {domain}"
            );
        }
    }
}

#[test]
fn malformed_or_overflowing_input_never_reaches_the_request_callback() {
    let bytes = encoded(&input(frame_tests::commands().remove(9)));
    for length in 0..bytes.len() {
        let mut called = false;
        let result = limits().with_request(NativeLimits::default(), &bytes[..length], |_, _| {
            called = true;
        });
        assert!(result.is_err(), "prefix {length}");
        assert!(!called);
    }
    let mut called = false;
    let mut invalid = limits();
    invalid.work.parse = usize::MAX;
    assert!(
        invalid
            .with_request(NativeLimits::default(), &bytes, |_, _| {
                called = true;
            })
            .is_err()
    );
    assert!(!called);
    let mut trailing = bytes;
    trailing.push(0);
    assert!(
        limits()
            .with_request(NativeLimits::default(), &trailing, |_, _| {
                called = true;
            })
            .is_err()
    );
    assert!(!called);
}
