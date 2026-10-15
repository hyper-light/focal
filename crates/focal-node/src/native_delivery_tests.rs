//! Delivery queries must visit the recipient's messages, independently of unrelated traffic.
use super::*;

fn post(h: &mut Harness, subject: ParticipantId) -> (ClaimId, SessionSeq) {
    let (compiled, _) = h.run(
        ISSUER,
        &parse(
            "claim.submit",
            json!({
                "description": "A directed delivery.",
                "target": hex(&subject.0),
                "validations": [{"kind": "receipt", "description": "Record delivery.",
                                 "deadline": {"at": FAR}}]
            }),
        ),
    );
    let claim = ClaimId(compiled.created[0].id);
    let (_, outcome) = h.run(
        ISSUER,
        &parse("claim.post", json!({"claim": hex(&claim.0)})),
    );
    (claim, outcome.sequence)
}

#[test]
fn delivery_tail_visits_only_its_recipient() {
    let mut h = Harness::new();
    let mut expected = Vec::new();
    // Two messages for this recipient, interleaved with 30 for other recipients.
    for round in 0..2 {
        for recipient in 2..18 {
            let (claim, sequence) = post(&mut h, ParticipantId::from_u128(recipient));
            if recipient == 2 {
                expected.push((claim, sequence));
            }
        }
        assert_eq!(expected.len(), round + 1);
    }
    let page = h
        .list(&NativeListRequest {
            filter: NativeListFilter::Deliveries {
                recipient: SUBJECT,
                after: None,
            },
            cursor: None,
            max_items: 256,
            max_visits: 4096,
        })
        .unwrap();
    eprintln!(
        "delivery lookup: {} visits, {} returned objects, {} relevant deliveries",
        page.visited,
        page.objects.len(),
        expected.len()
    );
    assert_eq!(
        page.visited as usize,
        expected.len(),
        "delivery work must depend on this recipient, not the ledger's event count"
    );
    let got: Vec<_> = page
        .objects
        .iter()
        .map(|object| {
            let NativeObject::Event(event) = object else {
                panic!("a delivery event")
            };
            let NativeFactRecord::Claim(claim) = event.fact else {
                panic!("a posted claim")
            };
            assert_eq!(claim.status, ClaimStatus::Posted);
            (ClaimId(claim.after.object.0), event.sequence)
        })
        .collect();
    assert_eq!(got, expected);
}

#[test]
fn delivery_pages_resume_exclusively_and_bind_the_recipient_and_principal() {
    let mut h = Harness::new();
    let first = post(&mut h, SUBJECT);
    post(&mut h, ParticipantId::from_u128(3));
    let second = post(&mut h, SUBJECT);
    let request = |recipient, cursor| NativeListRequest {
        filter: NativeListFilter::Deliveries {
            recipient,
            after: None,
        },
        cursor,
        max_items: 1,
        max_visits: 1,
    };
    let page = h.list(&request(SUBJECT, None)).unwrap();
    assert_eq!(page.visited, 1);
    assert_eq!(page.objects.len(), 1);
    let NativeObject::Event(event) = &page.objects[0] else {
        panic!("an event")
    };
    assert_eq!(event.sequence, first.1);
    let cursor = page.next.unwrap();
    assert!(matches!(
        h.list(&request(ParticipantId::from_u128(3), Some(cursor.clone()))),
        Err(AccessError::InvalidRequest)
    ));
    let mut reader = h.reader();
    reader.principal = SUBJECT;
    assert!(matches!(
        native_lists::serve(
            &reader,
            &LIST_KEY,
            &request(SUBJECT, Some(cursor.clone())),
            &mut read_charge()
        ),
        Err(AccessError::InvalidRequest)
    ));
    // Growing the ledger while a cursor is held neither replays the first delivery nor
    // visits the intervening recipient's event. A newly posted delivery is visible too.
    let third = post(&mut h, SUBJECT);
    let mut got = Vec::new();
    let mut next = Some(cursor);
    for _ in 0..4 {
        let page = h.list(&request(SUBJECT, next.take())).unwrap();
        assert!(page.visited <= 1);
        got.extend(page.objects.into_iter().map(|object| {
            let NativeObject::Event(event) = object else {
                panic!("an event")
            };
            event.sequence
        }));
        next = page.next;
        if next.is_none() {
            break;
        }
    }
    assert!(next.is_none());
    assert_eq!(got, [second.1, third.1]);
    let page = h
        .list(&NativeListRequest {
            filter: NativeListFilter::Deliveries {
                recipient: SUBJECT,
                after: Some((second.1, u32::MAX)),
            },
            cursor: None,
            max_items: 256,
            max_visits: 256,
        })
        .unwrap();
    assert_eq!(page.visited, 1);
    assert_eq!(page.objects.len(), 1);
}

#[test]
fn a_posted_testament_is_delivered_to_its_claimant_once() {
    let mut h = Harness::new();
    let (claim, _) = post(&mut h, SUBJECT);
    let claim_hex = hex(&claim.0);
    h.run(
        SUBJECT,
        &parse("receipt.acquire", json!({"claim": claim_hex})),
    );
    let (compiled, _) = h.run(
        SUBJECT,
        &parse(
            "testament.submit",
            json!({
                "claim": claim_hex, "summary": "The delivery completed.",
                "confidence": "committed", "outcome": "complete", "manifest": []
            }),
        ),
    );
    let response = hex(&compiled.created[0].id);
    let (_, posted) = h.run(
        SUBJECT,
        &parse(
            "testament.post",
            json!({
                "claim": claim_hex, "testament": response
            }),
        ),
    );
    h.run(
        ISSUER,
        &parse(
            "testament.receive",
            json!({
                "claim": claim_hex, "testament": response
            }),
        ),
    );
    let page = h
        .list(&NativeListRequest {
            filter: NativeListFilter::Deliveries {
                recipient: ISSUER,
                after: None,
            },
            cursor: None,
            max_items: 256,
            max_visits: 256,
        })
        .unwrap();
    assert_eq!(
        page.visited, 1,
        "neither submission nor receipt re-delivers a testament"
    );
    assert_eq!(page.objects.len(), 1);
    let NativeObject::Event(event) = &page.objects[0] else {
        panic!("an event")
    };
    assert_eq!(event.sequence, posted.sequence);
    assert!(matches!(event.fact, NativeFactRecord::Response {
        claim: got, state: NativeResponseState::Posted, ..
    } if got == claim));
}
