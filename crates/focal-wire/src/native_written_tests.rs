use super::*;

#[test]
fn written_state_is_explicitly_negotiated_without_changing_peer_lanes() {
    let limits = WireLimits::default();
    let hello = |versions| Hello {
        versions,
        max_frame_bytes: limits.max_frame_bytes,
        max_items: limits.max_items,
    };
    let peers = limits
        .negotiate_laned(&hello(OFFERED_PROTOCOLS.to_vec()), [true; 5])
        .unwrap();
    assert_eq!(peers.protocol, LANED_PROTOCOL_VERSION);
    assert!(peers.laned());
    assert!(!OFFERED_PROTOCOLS.contains(&NATIVE_WRITTEN_PROTOCOL_VERSION));

    let native = limits
        .negotiate_laned(
            &hello(NATIVE_WRITTEN_PROTOCOLS.to_vec()),
            [true, true, true, false, false],
        )
        .unwrap();
    assert_eq!(native.protocol, NATIVE_WRITTEN_PROTOCOL_VERSION);
    assert!(!native.laned());
    assert!(native.accepts_protocol(NATIVE_PROTOCOL_VERSION));
    assert!(native.accepts_protocol(NATIVE_WRITTEN_PROTOCOL_VERSION));
    assert!(!native.accepts_protocol(ORDERED_PROTOCOL_VERSION));
    assert!(!native.accepts_protocol(LANED_PROTOCOL_VERSION));
    let old = limits
        .negotiate_laned(
            &hello(vec![NATIVE_PROTOCOL_VERSION]),
            [true, true, true, false, false],
        )
        .unwrap();
    assert_eq!(old.protocol, NATIVE_PROTOCOL_VERSION);
    assert!(!old.accepts_protocol(NATIVE_WRITTEN_PROTOCOL_VERSION));
}

#[test]
fn a_written_reply_requires_its_own_receipt_sequence_and_the_requested_profile() {
    let limits = WireLimits::default();
    let mut request = envelope(
        NATIVE_WRITTEN_PROTOCOL_VERSION,
        Operation::Native {
            frame: frame(0, 0, ledger(), key(), 0),
        },
    );
    assert!(
        verify_request(
            peer(PeerRole::Actor, key().principal),
            request.clone(),
            &limits
        )
        .is_ok()
    );
    let receipt = NativeReceipt {
        invocation: NativeInvocationRef::Request(key()),
        sequence: SessionSeq(1),
        logical_time: 9,
        operation: NativeOperationKind::Create,
        intent: ContentHash([3; 32]),
        counts: NativeOutcomeCounts {
            created: 1,
            changed: 0,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 1,
        },
    };
    let written = NativeWrittenState {
        sequence: receipt.sequence,
        claim: Some(NativeWrittenClaim {
            binding: NativeBinding {
                object: ObjectId::from_u128(5),
                content: ContentHash([7; 32]),
                revision: ObjectRevision(1),
            },
            issuer: key().principal,
            subject: ParticipantId::from_u128(22),
            status: ClaimStatus::Generated,
            receipt: None,
            response_count: 0,
            latest_response: None,
        }),
        response: None,
    };
    let extended =
        |written| Response::Native(NativeMutationReply::CommittedWithWritten { receipt, written });
    let response = request.reply(extended(written));
    assert!(validate_response(&request, &response, Some(key().principal), &limits).is_ok());
    let wrong_sequence = request.reply(extended(NativeWrittenState {
        sequence: SessionSeq(2),
        ..written
    }));
    assert!(validate_response(&request, &wrong_sequence, Some(key().principal), &limits).is_err());
    let empty = request.reply(extended(NativeWrittenState {
        claim: None,
        ..written
    }));
    assert!(validate_response(&request, &empty, Some(key().principal), &limits).is_err());
    let plain = request.reply(Response::Native(NativeMutationReply::Committed(receipt)));
    assert!(validate_response(&request, &plain, Some(key().principal), &limits).is_ok());
    request.protocol = NATIVE_PROTOCOL_VERSION;
    let response = request.reply(extended(written));
    assert!(validate_response(&request, &response, Some(key().principal), &limits).is_err());
    let plain = request.reply(Response::Native(NativeMutationReply::Committed(receipt)));
    assert!(validate_response(&request, &plain, Some(key().principal), &limits).is_ok());
}
