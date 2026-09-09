//! Disk-backed two-party workflows through the durable native Session: the
//! claimant issues and posts; the respondent takes responsibility, performs work
//! outside Focal, supplies its artifacts and its own testimony (success and
//! failure); the claimant receives it; the designated evaluator reports; the
//! ledger derives acceptance; a checkpoint, restart and new term preserve it all.
use super::tests::{lead, ledger, open_dir};
use super::*;
use focal_core::native::fixtures as fx;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::lifecycle::evidence::{EvidenceFailure, Parent, ResponseState, WorkArtifactState};
use focal_model::lifecycle::{Binding, validation};
use focal_model::{
    ArtifactId, ClaimId, ClaimStatus, Confidence, OutcomeKind, ParticipantId, RequestKey,
    TestamentId, ValidationId, ValidationMode, VerdictValue,
};

const PARTIES: fx::Parties = fx::Parties::numbered(61);
const SLOT_VALIDATION: u128 = 301;
const DELIVERY_VALIDATION: u128 = 300;

struct Harness {
    session: NativeSession<BuiltinNativeSchemas>,
    clock: u64,
    serial: u128,
}
impl Harness {
    fn commit(&mut self, actor: ParticipantId, input: NativeInput, step: &str) -> NativeOutcome {
        self.clock = self.clock.saturating_add(1);
        let request = input.request;
        let submission = self
            .session
            .propose(fx::context(actor, self.clock), input)
            .unwrap_or_else(|error| panic!("{step}: {error:?}"));
        let outcome = match submission {
            NativeSubmission::Committed(outcome) => return outcome,
            NativeSubmission::Pending { outcome, .. } => outcome,
        };
        for _ in 0..16 {
            let events = self
                .session
                .poll()
                .unwrap_or_else(|error| panic!("{step} poll: {error:?}"));
            if events
                .committed
                .iter()
                .any(|commit| commit.outcome == outcome)
            {
                assert_eq!(
                    self.session.outcome(request).unwrap(),
                    Some(outcome),
                    "{step}"
                );
                return outcome;
            }
        }
        panic!("{step}: candidate never committed");
    }
    fn next(&mut self, actor: ParticipantId) -> RequestKey {
        self.serial = self.serial.saturating_add(1);
        fx::request(actor, 1, self.serial)
    }
    fn core(&self) -> &Core<NativeState> {
        self.session.committed_core().unwrap()
    }
    fn claim(&self, id: u128) -> Binding {
        self.core()
            .native_claim(ClaimId::from_u128(id))
            .unwrap()
            .binding()
    }
    fn status(&self, id: u128) -> ClaimStatus {
        self.core()
            .native_claim(ClaimId::from_u128(id))
            .unwrap()
            .status()
    }
    fn parent(&self, id: u128) -> Parent {
        Parent::from_claim(self.core().native_claim(ClaimId::from_u128(id)).unwrap()).unwrap()
    }
    fn response(&self, id: u128) -> Binding {
        self.core()
            .native_response_record(TestamentId::from_u128(id))
            .unwrap()
            .response()
            .identity()
            .binding
    }
    fn response_state(&self, id: u128) -> ResponseState {
        self.core()
            .native_response_record(TestamentId::from_u128(id))
            .unwrap()
            .response()
            .state()
    }
    fn work_state(&self, id: u128) -> WorkArtifactState {
        self.core()
            .native_work(ArtifactId::from_u128(id))
            .unwrap()
            .state
            .state()
    }
}

fn declarations(claim: u128) -> Vec<validation::Declaration> {
    vec![
        fx::delivery_declaration(
            ledger(),
            PARTIES,
            claim,
            claim * 1000 + DELIVERY_VALIDATION,
            1000,
        )
        .unwrap(),
        fx::slot_declaration(
            ledger(),
            PARTIES,
            claim,
            claim * 1000 + SLOT_VALIDATION,
            1,
            0,
            ValidationMode::Required,
            false,
            1000,
        )
        .unwrap(),
    ]
}
fn slots(claim: u128) -> Vec<fx::Slot> {
    vec![
        fx::Slot {
            slot: 0,
            missing_declaration_index: 20,
            mode: ValidationMode::Required,
            checks: vec![focal_model::lifecycle::aggregation::CheckPolicy {
                declaration_index: 1,
                validation: ValidationId::from_u128(claim * 1000 + SLOT_VALIDATION),
                mode: ValidationMode::Required,
            }],
        },
        fx::Slot {
            slot: 1,
            missing_declaration_index: 21,
            mode: ValidationMode::Required,
            checks: vec![],
        },
    ]
}
fn issue_and_accept(h: &mut Harness, claim: u128, receipt: u128) {
    let request = h.next(PARTIES.issuer);
    h.commit(
        PARTIES.issuer,
        fx::creation(
            ledger(),
            PARTIES,
            request,
            claim,
            declarations(claim),
            &slots(claim),
        )
        .unwrap(),
        "create",
    );
    assert_eq!(h.status(claim), ClaimStatus::Generated);
    let request = h.next(PARTIES.issuer);
    let expected = h.claim(claim);
    h.commit(PARTIES.issuer, fx::post(request, expected), "post");
    assert_eq!(h.status(claim), ClaimStatus::Posted);
    // Receiving the claim creates responsibility, never testimony.
    let request = h.next(PARTIES.subject);
    let expected = h.claim(claim);
    h.commit(
        PARTIES.subject,
        fx::acquire_receipt(request, expected, receipt),
        "acquire receipt",
    );
    assert_eq!(h.status(claim), ClaimStatus::Received);
    assert!(
        h.core()
            .native_response_record(TestamentId::from_u128(claim * 10))
            .is_none()
    );
}

#[test]
fn successful_and_failed_testimony_derive_acceptance_and_survive_checkpoint_restart_and_new_term() {
    let dir = tempfile::tempdir().unwrap();
    let parent_budget = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut h = Harness {
        session: open_dir(dir.path(), &parent_budget),
        clock: 0,
        serial: 0,
    };
    lead(&mut h.session);

    // --- Claim 1: work succeeds; the respondent's testimony is checked by the evaluator.
    issue_and_accept(&mut h, 1, 701);
    let parent = h.parent(1);
    let (first, first_slot) = fx::work_artifact(ledger(), 801, &parent, 0, fx::PROOF).unwrap();
    let (second, second_slot) = fx::work_artifact(ledger(), 802, &parent, 1, fx::PROOF).unwrap();
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::submit_work(request, claim, 0, first),
        "submit work 0",
    );
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::submit_work(request, claim, 1, second),
        "submit work 1",
    );
    assert_eq!(h.work_state(801), WorkArtifactState::Generated);
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            10,
            "All requested tests pass.",
            Confidence::Committed,
            OutcomeKind::Complete,
            vec![first_slot, second_slot],
            vec![],
        ),
        "close response",
    );
    assert_eq!(h.response_state(10), ResponseState::Generated);
    assert_eq!(h.work_state(801), WorkArtifactState::Attached);
    assert_eq!(h.status(1), ClaimStatus::TestamentGenerated);
    let (request, claim, response) = (h.next(PARTIES.subject), h.claim(1), h.response(10));
    h.commit(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post response",
    );
    assert_eq!(h.response_state(10), ResponseState::Posted);
    let (request, claim, response) = (h.next(PARTIES.issuer), h.claim(1), h.response(10));
    h.commit(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive response",
    );
    assert_eq!(h.response_state(10), ResponseState::Received);
    assert_eq!(h.status(1), ClaimStatus::TestamentAcknowledged);
    // The designated evaluator checks the exact attached artifact in its own environment and reports.
    let key = fx::work_key(
        1,
        1000 + SLOT_VALIDATION,
        10,
        0,
        ArtifactId::from_u128(801),
        1,
    );
    let expected = h.core().native_evaluation(key).unwrap().binding();
    let (request, claim) = (h.next(PARTIES.evaluator), h.claim(1));
    h.commit(
        PARTIES.evaluator,
        fx::begin_work(request, claim, key, expected),
        "begin work",
    );
    assert_eq!(h.status(1), ClaimStatus::Validating);
    assert_eq!(h.response_state(10), ResponseState::Validating);
    let report = {
        let core = h.core();
        let state = core.native_evaluation(key).unwrap();
        let definition = core.native_definition(key.validation).unwrap();
        fx::report_work(
            ledger(),
            fx::request(PARTIES.evaluator, 1, 5001),
            851,
            h.claim(1),
            key,
            state,
            definition,
            VerdictValue::Pass,
            fx::PROOF,
        )
        .unwrap()
    };
    let accepted = h.commit(PARTIES.evaluator, report, "report work");
    assert_eq!(
        h.status(1),
        ClaimStatus::Satisfied,
        "derived acceptance after the required checks"
    );
    assert_eq!(h.response_state(10), ResponseState::Validated);
    assert_eq!(h.work_state(801), WorkArtifactState::Validated);
    assert!(
        h.core()
            .native_artifact(ArtifactId::from_u128(851))
            .is_some(),
        "evaluator proof retained"
    );
    let a_wrong_evaluator = {
        let core = h.core();
        let state = core.native_evaluation(key).unwrap();
        let definition = core.native_definition(key.validation).unwrap();
        fx::report_work(
            ledger(),
            fx::request(PARTIES.subject, 1, 5002),
            852,
            h.claim(1),
            key,
            state,
            definition,
            VerdictValue::Pass,
            fx::PROOF,
        )
    };
    assert!(
        a_wrong_evaluator.is_err(),
        "the respondent cannot author the evaluator's report"
    );

    // --- Claim 2: work fails; the respondent supplies its diagnostic and a Failed testament.
    issue_and_accept(&mut h, 2, 702);
    let parent = h.parent(2);
    let (diagnostic, diagnostic_ref) = fx::diagnostic_artifact(
        ledger(),
        821,
        &parent,
        EvidenceFailure::Work,
        fx::WORK_DIAGNOSTIC,
    )
    .unwrap();
    let (request, claim) = (h.next(PARTIES.subject), h.claim(2));
    h.commit(
        PARTIES.subject,
        fx::submit_diagnostic(request, claim, EvidenceFailure::Work, diagnostic),
        "submit diagnostic",
    );
    let (request, claim) = (h.next(PARTIES.subject), h.claim(2));
    h.commit(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            20,
            "The tests could not be made to pass.",
            Confidence::Tentative,
            OutcomeKind::Failed,
            vec![],
            vec![diagnostic_ref],
        ),
        "close failed response",
    );
    let (request, claim, response) = (h.next(PARTIES.subject), h.claim(2), h.response(20));
    h.commit(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post failed response",
    );
    let (request, claim, response) = (h.next(PARTIES.issuer), h.claim(2), h.response(20));
    h.commit(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive failed response",
    );
    assert_eq!(h.status(2), ClaimStatus::TestamentAcknowledged);
    let (request, claim, response) = (h.next(PARTIES.issuer), h.claim(2), h.response(20));
    h.commit(
        PARTIES.issuer,
        fx::enter_whole_work(request, claim, response),
        "enter whole work",
    );
    assert_eq!(
        h.status(2),
        ClaimStatus::ValidationIncomplete,
        "failed work never satisfies the requirements"
    );
    assert_eq!(h.response_state(20), ResponseState::ValidationIncomplete);
    let record = h
        .core()
        .native_response_record(TestamentId::from_u128(20))
        .unwrap();
    assert_eq!(record.response().reported_outcome(), OutcomeKind::Failed);
    assert_eq!(
        record.response().diagnostics().len(),
        1,
        "the respondent's diagnostic is frozen into its testimony"
    );
    let _ = diagnostic_ref;
    assert!(
        h.core()
            .native_diagnostic(ArtifactId::from_u128(821))
            .is_some(),
        "diagnostic evidence stays inspectable"
    );

    // --- Checkpoint, restart, new term: identities, histories and outcomes are the same.
    let sequence = h.session.sequence().unwrap();
    h.session.begin_checkpoint().unwrap();
    let _ = h.session.poll().unwrap();
    let (request, claim) = (h.next(PARTIES.issuer), h.claim(2));
    let cancelled = h.commit(
        PARTIES.issuer,
        fx::cancel(request, claim),
        "cancel after checkpoint",
    );
    let _ = cancelled;
    assert_eq!(
        h.status(2),
        ClaimStatus::ValidationIncomplete,
        "a terminal claim keeps its original cut"
    );
    let Harness {
        session,
        clock,
        serial,
    } = h;
    let before: Vec<Option<NativeOutcome>> = (1..=serial)
        .map(|id| session.outcome(fx::request(PARTIES.issuer, 1, id)).unwrap())
        .collect();
    drop(session);
    let mut h = Harness {
        session: open_dir(dir.path(), &parent_budget),
        clock,
        serial,
    };
    assert!(
        h.session.sequence().unwrap() > sequence,
        "the tail beyond the checkpoint replayed"
    );
    assert_eq!(h.status(1), ClaimStatus::Satisfied);
    assert_eq!(h.status(2), ClaimStatus::ValidationIncomplete);
    assert_eq!(h.response_state(10), ResponseState::Validated);
    assert_eq!(h.response_state(20), ResponseState::ValidationIncomplete);
    assert_eq!(h.work_state(801), WorkArtifactState::Validated);
    assert!(
        h.core()
            .native_artifact(ArtifactId::from_u128(851))
            .is_some()
    );
    assert!(
        h.core()
            .native_diagnostic(ArtifactId::from_u128(821))
            .is_some()
    );
    let after: Vec<Option<NativeOutcome>> = (1..=serial)
        .map(|id| {
            h.session
                .outcome(fx::request(PARTIES.issuer, 1, id))
                .unwrap()
        })
        .collect();
    assert_eq!(before, after);
    assert_eq!(
        h.session
            .outcome(fx::request(PARTIES.evaluator, 1, 5001))
            .unwrap(),
        Some(accepted)
    );
    lead(&mut h.session);
    // Exact retries after restart return the original outcomes; new work continues.
    let original_post = h
        .session
        .outcome(fx::request(PARTIES.issuer, 1, 2))
        .unwrap()
        .expect("post committed before restart");
    assert_eq!(
        h.session
            .propose(
                fx::context(PARTIES.issuer, 1_000),
                fx::post(fx::request(PARTIES.issuer, 1, 2), fx::binding(ledger(), 1))
            )
            .unwrap(),
        NativeSubmission::Committed(original_post),
        "the exact original intent is answered from the restored root"
    );
    let substituted = Binding {
        revision: focal_model::ObjectRevision(2),
        ..fx::binding(ledger(), 1)
    };
    assert_eq!(
        h.session
            .propose(
                fx::context(PARTIES.issuer, 1_000),
                fx::post(fx::request(PARTIES.issuer, 1, 2), substituted)
            )
            .unwrap_err()
            .class(),
        FailureClass::Request,
        "a substituted intent under the same request key conflicts"
    );
    issue_and_accept(&mut h, 3, 703);
    assert_eq!(h.status(3), ClaimStatus::Received);
}

/// Every node hosts its native engine under `NativeSessionLimits::standard`;
/// the complete cycle, including the completion-class admissions whose record
/// buffers are funded from these limits, must fit them.
#[test]
fn the_standard_limits_admit_the_complete_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let parent_budget = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let limits = NativeSessionLimits::standard(focal_model::ContentDomainId::from_u128(1));
    let mut h = Harness {
        session: super::tests::open_with(dir.path(), &parent_budget, limits).unwrap(),
        clock: 0,
        serial: 0,
    };
    lead(&mut h.session);
    issue_and_accept(&mut h, 1, 701);
    let parent = h.parent(1);
    let (first, first_slot) = fx::work_artifact(ledger(), 801, &parent, 0, fx::PROOF).unwrap();
    let (second, second_slot) = fx::work_artifact(ledger(), 802, &parent, 1, fx::PROOF).unwrap();
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::submit_work(request, claim, 0, first),
        "submit work 0",
    );
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::submit_work(request, claim, 1, second),
        "submit work 1",
    );
    let (request, claim) = (h.next(PARTIES.subject), h.claim(1));
    h.commit(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            10,
            "All requested tests pass.",
            Confidence::Committed,
            OutcomeKind::Complete,
            vec![first_slot, second_slot],
            vec![],
        ),
        "close response",
    );
    let (request, claim, response) = (h.next(PARTIES.subject), h.claim(1), h.response(10));
    h.commit(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post response",
    );
    let (request, claim, response) = (h.next(PARTIES.issuer), h.claim(1), h.response(10));
    h.commit(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive response",
    );
    let key = fx::work_key(
        1,
        1000 + SLOT_VALIDATION,
        10,
        0,
        ArtifactId::from_u128(801),
        1,
    );
    let expected = h.core().native_evaluation(key).unwrap().binding();
    let (request, claim) = (h.next(PARTIES.evaluator), h.claim(1));
    h.commit(
        PARTIES.evaluator,
        fx::begin_work(request, claim, key, expected),
        "begin work",
    );
    let report = {
        let core = h.core();
        let state = core.native_evaluation(key).unwrap();
        let definition = core.native_definition(key.validation).unwrap();
        fx::report_work(
            ledger(),
            fx::request(PARTIES.evaluator, 1, 5001),
            851,
            h.claim(1),
            key,
            state,
            definition,
            VerdictValue::Pass,
            fx::PROOF,
        )
        .unwrap()
    };
    h.commit(PARTIES.evaluator, report, "report work");
    assert_eq!(h.status(1), ClaimStatus::Satisfied);
}
