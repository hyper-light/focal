use super::*;

fn deliver_empty_response(
    owner: &mut NativeOwner,
    claim: ClaimId,
    response: TestamentId,
    request: u128,
) {
    let binding = owner.effective().claim(claim).unwrap().binding();
    commit(
        owner,
        close(binding, u128::from_be_bytes(response.0), request),
    );
    for (offset, actor, receive) in [(1, f::SUBJECT, false), (2, f::ISSUER, true)] {
        let claim = owner.effective().claim(claim).unwrap().binding();
        let expected = owner
            .effective()
            .response(response)
            .unwrap()
            .identity()
            .binding;
        let command = if receive {
            NativeCommand::ReceiveResponse { claim, expected }
        } else {
            NativeCommand::PostResponse { claim, expected }
        };
        commit(
            owner,
            NativeInput {
                request: f::request(actor, request + offset),
                command,
            },
        );
    }
    let claim = owner.effective().claim(claim).unwrap().binding();
    let expected = owner
        .effective()
        .response(response)
        .unwrap()
        .identity()
        .binding;
    commit(
        owner,
        NativeInput {
            request: f::request(f::ISSUER, request + 3),
            command: NativeCommand::EnterWholeWork { claim, expected },
        },
    );
}

fn complete_claim(owner: &mut NativeOwner, id: u128, response: u128, request: u128) {
    commit(owner, f::creation(request, id, &[], None));
    let claim = ClaimId::from_u128(id);
    let binding = owner.effective().claim(claim).unwrap().binding();
    commit(owner, f::post(request + 1, binding));
    let expected = owner.effective().claim(claim).unwrap().binding();
    commit(
        owner,
        NativeInput {
            request: f::request(f::SUBJECT, request + 2),
            command: NativeCommand::AcquireReceipt {
                expected,
                receipt: ReceiptId::from_u128(700 + id),
            },
        },
    );
    deliver_empty_response(owner, claim, TestamentId::from_u128(response), request + 3);
    let state = owner.effective().claim(claim).unwrap();
    assert!(state.local_complete());
    assert_eq!(state.status(), ClaimStatus::Satisfied);
}

#[test]
fn posting_uses_frozen_bundle_after_another_claim_completes_and_publishes_a_separate_audit() {
    // Runtime blocking-scope ingress is not implemented yet. This actual owner
    // history qualifies publication after independent graph and audit progress;
    // it does not claim to qualify a locally complete parent waiting on a scope.
    let mut core = f::core();
    core.limits.plan_edges = 65_536;
    let mut owner = NativeOwner::new(core).unwrap();
    complete_claim(&mut owner, 1, 900, 1400);
    let original = frozen_claim(&owner);
    let generation = commit(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 1410),
    );
    let bundle = owner.committed().result_testament(BUNDLE).unwrap();
    let binding = bundle.testament().binding();
    let captured = bundle.captured_at();
    let results = bundle.testament().results().to_vec();
    let publications = bundle.publications().to_vec();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].phase(), validation::Phase::Delivery);

    complete_claim(&mut owner, 2, 901, 1420);
    let second = ClaimId::from_u128(2);
    let second_id = TestamentId::from_u128(951);
    let second_binding = owner.effective().claim(second).unwrap().binding();
    commit(
        &mut owner,
        generate(second_binding, second_id, f::ISSUER, 1430),
    );
    let second_bundle = owner.effective().claim_result_testament(second).unwrap();
    assert_eq!(second_bundle.testament().binding().object.0, second_id.0);
    let second_results = second_bundle.testament().results();
    assert_eq!(second_results.len(), 1);
    assert_ne!(second_results[0], results[0]);
    assert!(second_bundle.publications()[0].position.sequence > generation.sequence);
    assert_claim_unchanged(&owner, &original);

    let posting = commit(&mut owner, post(binding, f::ISSUER, 1440));
    let bundle = owner.committed().result_testament(BUNDLE).unwrap();
    assert_eq!(bundle.testament().binding(), binding.next().unwrap());
    assert_eq!(bundle.testament().state(), ResultTestamentState::Posted);
    assert_eq!(bundle.testament().results(), results);
    assert_eq!(bundle.publications(), publications);
    assert_eq!(bundle.captured_at(), captured);
    assert_eq!(bundle.generated_at().sequence, generation.sequence);
    assert_eq!(bundle.posted_at().unwrap().sequence, posting.sequence);
    assert_eq!(
        owner
            .committed()
            .claim_result_testament(CLAIM)
            .unwrap()
            .testament()
            .binding(),
        binding.next().unwrap()
    );
    assert_eq!(
        owner
            .committed()
            .claim_result_testament(second)
            .unwrap()
            .testament()
            .state(),
        ResultTestamentState::Generated
    );
    assert_claim_unchanged(&owner, &original);
    retry(
        &mut owner,
        generate(original.binding(), BUNDLE, f::ISSUER, 1410),
        generation,
        None,
    );
}
