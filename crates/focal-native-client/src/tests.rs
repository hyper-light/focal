use super::*;
use focal_client::input::{BuildContext, InputError};
use focal_client::native_store::{NativeIdentity, NativeIdentityKind};
use focal_client::operations::{NativeAuthoredOperation, parse_native_json};
use focal_core::Core;
use focal_core::native::{
    EvaluationKey, EvaluationTarget, NativeCommand, NativeContentProfile, NativeContext,
    NativeLimits, NativeOutcome, NativeOwner, NativeStaging, NativeView,
    input_codec::{DecodeWork, NativeDecodeLimits},
};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{MemoryBudget, RangeId};
use focal_model::lifecycle::{Binding, Principal};
use focal_model::*;
use focal_wire::inspect_native_frame;
use serde_json::{Value, json};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(11),
        session: SessionId::from_u128(12),
    }
}
fn context(actor: ParticipantId) -> BuildContext {
    BuildContext {
        ledger: ledger(),
        actor,
        root: RootCommandId::from_u128(900),
        policy_revision: 1,
    }
}
fn hex(value: u128) -> String {
    format!("{value:032x}")
}
fn hash(value: u8) -> String {
    ContentHash([value; 32])
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}
fn ids(seed: u128) -> impl FnMut() -> Result<[u8; 16], InputError> {
    let mut next = seed;
    move || {
        next += 1;
        Ok(next.to_be_bytes())
    }
}
fn parse(name: &str, value: Value) -> NativeAuthoredOperation {
    parse_native_json(name, &serde_json::to_vec(&value).unwrap()).unwrap()
}
fn claim_document() -> Value {
    json!({
        "description": "Inspect the module and report.",
        "target": hex(2),
        "scopes": [{"kind": "file", "key": "src/lib.rs"}, {"kind": "api", "key": "focal::native"}],
        "validations": [
            {"kind": "receipt", "description": "Record delivery of the testimony.", "deadline": {"at": 10_000}},
            {"kind": "test", "description": "Run the suite on the primary slot.",
             "target": {"type": "slot", "index": 0, "name": "primary"},
             "evaluator": hex(3), "handlers": [{"id": hex(77), "version": hash(77), "attempts": 2}],
             "deadline": {"at": 10_000}}
        ],
        "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
    })
}
const PROOF: &str = r#"{"passed":3,"failed":0,"skipped":0}"#;
const DIAGNOSTIC: &str = r#"{"code":"build","message":"the build failed"}"#;

/// One in-process authored owner with a content store for artifact custody.
/// Frames are admitted exactly as the node admits them: the owner decodes the
/// journaled bytes under its decode limits and verifies custody itself.
struct Harness {
    owner: NativeOwner,
    store: ContentStore,
    _root: tempfile::TempDir,
    time: u64,
}
impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let store = ContentStore::open(
            root.path(),
            StoreLimits {
                max_content_bytes: 4 * 1024 * 1024,
                max_staging_bytes: 8 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 4096,
                max_manifest_bytes: 128 * 1024,
            },
        )
        .unwrap();
        let core = Core::new_native_authored(
            ledger(),
            RangeId(1),
            NativeLimits::default(),
            MemoryBudget::new(512 << 20, 128 << 20).unwrap(),
        )
        .unwrap();
        Self {
            owner: NativeOwner::new(core).unwrap(),
            store,
            _root: root,
            time: 100,
        }
    }
    fn view(&self) -> NativeView<'_> {
        self.owner.committed()
    }
    fn resolved(
        &self,
        claim: ClaimId,
        response: Option<TestamentId>,
        evaluation: Option<EvaluationKey>,
    ) -> Resolved {
        let view = self.view();
        let mut resolved = Resolved::default();
        if let Some(state) = view.claim(claim) {
            resolved.claims.push(ResolvedClaim {
                binding: state.binding(),
                issuer: state.issuer(),
                subject: state.subject(),
                status: state.status(),
                receipt: state
                    .receipt()
                    .map(|receipt| (receipt.holder, receipt.fence)),
                response_count: u32::try_from(state.response_count()).unwrap(),
                latest_response: state.latest_response().map(|link| link.testament),
            });
        }
        if let Some(response) = response.and_then(|id| view.response(id)) {
            let identity = response.identity();
            resolved.responses.push(ResolvedResponse {
                binding: identity.binding,
                claim: identity.claim,
                cycle: identity.cycle,
            });
        }
        if let Some(key) = evaluation
            && let Some(state) = view.evaluation(key)
        {
            let definition = view.definition(key.validation).unwrap();
            let bound = (*state).bind(definition).unwrap();
            resolved.evaluations.push(ResolvedEvaluation {
                binding: state.binding(),
                key,
                target: state.target(),
                receipt: state.receipt(),
                evaluator: bound.evaluator().ok(),
                attempt: bound.current_attempt().ok(),
                has_begun: state.has_begun(),
                terminal: state.state().is_terminal(),
            });
        }
        resolved
    }
    /// Compile, encode, fingerprint, and admit the frame bytes through the
    /// owner as the node would.
    fn run(
        &mut self,
        actor: ParticipantId,
        request: u128,
        operation: &NativeAuthoredOperation,
        resolved: &Resolved,
    ) -> (Compiled, Vec<u8>, NativeOutcome) {
        let limits = CompileLimits::default();
        let mut generator = ids(request * 1000);
        let compiled = compile(
            operation,
            &context(actor),
            RequestId::from_u128(request),
            &mut generator,
            resolved,
            &limits,
        )
        .unwrap();
        let frame = encode_frame(
            ledger(),
            NativeContentProfile::AuthoredV1,
            &compiled.input,
            limits.encoding(),
        )
        .unwrap();
        let header = inspect_native_frame(&frame).unwrap();
        assert_eq!(header.key, compiled.input.request);
        assert_eq!(header.ledger, ledger());
        let fingerprint = fingerprint(&frame, limits.native, limits.frame).unwrap();
        // Identical documents and identities compile to identical bytes.
        let again = compile(
            operation,
            &context(actor),
            RequestId::from_u128(request),
            &mut ids(request * 1000),
            resolved,
            &limits,
        )
        .unwrap();
        assert_eq!(
            encode_frame(
                ledger(),
                NativeContentProfile::AuthoredV1,
                &again.input,
                limits.encoding()
            )
            .unwrap(),
            frame
        );
        let outcome = self.publish(operation.name(), actor, &frame);
        assert_eq!(
            outcome.intent, fingerprint,
            "receipt intent must equal the client fingerprint"
        );
        (compiled, frame, outcome)
    }
    /// Compiles and submits like `run`, but returns the owner's refusal as
    /// its debug rendering instead of panicking.
    fn attempt(
        &mut self,
        actor: ParticipantId,
        request: u128,
        operation: &NativeAuthoredOperation,
        resolved: &Resolved,
    ) -> Result<(Compiled, NativeOutcome), String> {
        let limits = CompileLimits::default();
        let compiled = compile(
            operation,
            &context(actor),
            RequestId::from_u128(request),
            &mut ids(request * 1000),
            resolved,
            &limits,
        )
        .map_err(|error| format!("compile: {error}"))?;
        let frame = encode_frame(
            ledger(),
            NativeContentProfile::AuthoredV1,
            &compiled.input,
            limits.encoding(),
        )
        .unwrap();
        self.time += 1;
        let context = NativeContext {
            principal: Principal::Actor(actor),
            logical_time: self.time,
        };
        let work = DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        };
        let decode =
            NativeDecodeLimits::for_native(limits.native, limits.frame.bytes, work).unwrap();
        let staging = self
            .owner
            .prepare_frame_with_custody(
                context,
                &frame,
                decode,
                &mut self.store,
                ContentDomainId::from_u128(93),
                &BuiltinNativeSchemas,
            )
            .map_err(|error| format!("{error:?}"))?;
        match staging {
            NativeStaging::Prepared { candidate, outcome } => {
                let published = self.owner.publish_after_durable(candidate).unwrap();
                assert_eq!(published, outcome);
                Ok((compiled, outcome))
            }
            NativeStaging::Existing { .. } => Err("existing".into()),
        }
    }
    fn publish(&mut self, name: &str, actor: ParticipantId, frame: &[u8]) -> NativeOutcome {
        self.time += 1;
        let context = NativeContext {
            principal: Principal::Actor(actor),
            logical_time: self.time,
        };
        let limits = CompileLimits::default();
        let work = DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        };
        let decode =
            NativeDecodeLimits::for_native(limits.native, limits.frame.bytes, work).unwrap();
        let staging = self
            .owner
            .prepare_frame_with_custody(
                context,
                frame,
                decode,
                &mut self.store,
                ContentDomainId::from_u128(93),
                &BuiltinNativeSchemas,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        match staging {
            NativeStaging::Prepared { candidate, outcome } => {
                let published = self.owner.publish_after_durable(candidate).unwrap();
                assert_eq!(published, outcome);
                outcome
            }
            NativeStaging::Existing { .. } => panic!("{name}: unexpected exact retry"),
        }
    }
}
#[test]
fn the_two_party_workflow_compiles_from_documents_and_commits_through_the_owner() {
    let mut h = Harness::new();
    let submit = parse("claim.submit", claim_document());
    assert_eq!(requirements(&submit).unwrap(), Vec::new());
    let (compiled, _, outcome) = h.run(ISSUER, 1, &submit, &Resolved::default());
    assert_eq!((outcome.created, outcome.definitions), (1, 2));
    let claim = ClaimId(compiled.created[0].id);
    assert_eq!(compiled.created[0].kind, NativeIdentityKind::Claim);
    assert_eq!(compiled.created.len(), 3);
    let validation = ValidationId(compiled.created[2].id);
    assert_eq!(
        h.view().claim(claim).unwrap().status(),
        ClaimStatus::Generated
    );
    {
        let view = h.view();
        let content = view.claim_content(claim).unwrap();
        assert_eq!(content.issuer(), ISSUER);
        assert_eq!(content.subject(), SUBJECT);
        assert_eq!(content.scopes().len(), 2);
        assert_eq!(content.requirements().len(), 2);
    }

    let post = parse("claim.post", json!({"claim": hex_of(claim.0)}));
    assert_eq!(
        requirements(&post).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(claim)
        ])]
    );
    h.run(ISSUER, 2, &post, &h.resolved(claim, None, None));
    assert_eq!(h.view().claim(claim).unwrap().status(), ClaimStatus::Posted);

    let acquire = parse("receipt.acquire", json!({"claim": hex_of(claim.0)}));
    let (compiled, _, _) = h.run(SUBJECT, 3, &acquire, &h.resolved(claim, None, None));
    assert_eq!(compiled.created[0].kind, NativeIdentityKind::Receipt);
    let state = h.view().claim(claim).unwrap();
    assert_eq!(state.status(), ClaimStatus::Received);
    assert_eq!(state.receipt().unwrap().holder, SUBJECT);

    // The issuer holds no receipt, so it cannot author work.
    let work = parse(
        "artifact.submit",
        json!({"claim": hex_of(claim.0), "slot": 0, "payload": {"type": "text", "text": PROOF}}),
    );
    let resolved = h.resolved(claim, None, None);
    assert!(matches!(
        compile(
            &work,
            &context(ISSUER),
            RequestId::from_u128(4),
            &mut ids(4000),
            &resolved,
            &CompileLimits::default()
        ),
        Err(CompileError::Unsupported(_))
    ));
    let (compiled, _, outcome) = h.run(SUBJECT, 4, &work, &resolved);
    assert_eq!(outcome.artifacts, 1);
    let artifact = ArtifactId(compiled.created[0].id);
    let recorded = h.view().artifact(artifact).unwrap();
    assert_eq!(recorded.descriptor().producer(), SUBJECT);
    assert_eq!(recorded.descriptor().work_provenance().unwrap().cycle, 1);
    let artifact_hash = recorded.descriptor().content_hash();

    // Every diagnostic of the cycle must be cited by the closing testimony.
    let diagnostic = parse(
        "artifact.diagnostic",
        json!({"claim": hex_of(claim.0), "reason": "work", "payload": {"type": "text", "text": DIAGNOSTIC}}),
    );
    let (compiled, _, _) = h.run(SUBJECT, 5, &diagnostic, &h.resolved(claim, None, None));
    let diagnostic_id = ArtifactId(compiled.created[0].id);
    let diagnostic_hash = h
        .view()
        .diagnostic(diagnostic_id)
        .unwrap()
        .diagnostic
        .artifact()
        .hash;

    let testament = parse(
        "testament.submit",
        json!({
            "claim": hex_of(claim.0), "summary": "All tests pass.", "confidence": "committed", "outcome": "complete",
            "manifest": [{"slot": 0, "artifact": {"id": hex_of(artifact.0), "hash": hex_bytes(&artifact_hash.0)}}],
            "diagnostics": [{"id": hex_of(diagnostic_id.0), "hash": hex_bytes(&diagnostic_hash.0)}]
        }),
    );
    let (compiled, _, outcome) = h.run(SUBJECT, 6, &testament, &h.resolved(claim, None, None));
    assert_eq!(outcome.responses, 1);
    let response = TestamentId(compiled.created[0].id);
    assert_eq!(compiled.created[0].kind, NativeIdentityKind::Response);
    assert!(h.view().response(response).is_some());

    let post_response = parse(
        "testament.post",
        json!({"claim": hex_of(claim.0), "testament": hex_of(response.0)}),
    );
    assert_eq!(
        requirements(&post_response).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(claim),
            focal_wire::NativeObjectRef::Response(response)
        ])]
    );
    h.run(
        SUBJECT,
        7,
        &post_response,
        &h.resolved(claim, Some(response), None),
    );
    let receive = parse(
        "testament.receive",
        json!({"claim": hex_of(claim.0), "testament": hex_of(response.0)}),
    );
    let (_, _, outcome) = h.run(
        ISSUER,
        8,
        &receive,
        &h.resolved(claim, Some(response), None),
    );
    assert!(outcome.evaluations >= 1, "{outcome:?}");

    let key = EvaluationKey {
        claim,
        validation,
        target: EvaluationTarget::Work {
            response,
            slot: 0,
            artifact,
        },
        generation: 1,
    };
    assert!(h.view().evaluation(key).is_some());
    let begin = parse(
        "validation.begin",
        json!({"claim": hex_of(claim.0), "validation": hex_of(validation.0)}),
    );
    assert_eq!(
        requirements(&begin).unwrap(),
        vec![
            Requirement::Objects(vec![focal_wire::NativeObjectRef::Claim(claim)]),
            Requirement::Evaluations { validation }
        ]
    );
    let resolved = h.resolved(claim, None, Some(key));
    // The subject is not the designated evaluator: the owner refuses it, the
    // compiler only checks ownership facts it can see.
    h.run(EVALUATOR, 9, &begin, &resolved);
    assert!(h.view().evaluation(key).unwrap().has_begun());
    assert!(matches!(
        compile(
            &begin,
            &context(EVALUATOR),
            RequestId::from_u128(10),
            &mut ids(10_000),
            &h.resolved(claim, None, Some(key)),
            &CompileLimits::default()
        ),
        Err(CompileError::Unsupported(_))
    ));

    let report = parse(
        "validation.report",
        json!({"claim": hex_of(claim.0), "validation": hex_of(validation.0), "verdict": "pass", "payload": {"type": "text", "text": PROOF}}),
    );
    let resolved = h.resolved(claim, None, Some(key));
    assert!(matches!(
        compile(
            &report,
            &context(SUBJECT),
            RequestId::from_u128(10),
            &mut ids(10_000),
            &resolved,
            &CompileLimits::default()
        ),
        Err(CompileError::Unsupported(_))
    ));
    let (_, _, outcome) = h.run(EVALUATOR, 10, &report, &resolved);
    assert_eq!(outcome.results, 1);
    let evaluation = h.view().evaluation(key).unwrap();
    assert!(evaluation.state().is_terminal());
    assert_eq!(
        evaluation.state(),
        focal_model::lifecycle::validation::State::Validated
    );
    assert_eq!(
        h.view().claim(claim).unwrap().status(),
        ClaimStatus::Satisfied
    );

    // Cancellation after satisfaction is refused by the owner, and the
    // compiled frame for it still binds the exact committed revision.
    let cancel = parse("claim.cancel", json!({"claim": hex_of(claim.0)}));
    let resolved = h.resolved(claim, None, None);
    let compiled = compile(
        &cancel,
        &context(ISSUER),
        RequestId::from_u128(11),
        &mut ids(11_000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::Cancel { expected } = compiled.input.command else {
        panic!()
    };
    assert_eq!(expected, h.view().claim(claim).unwrap().binding());

    // The terminal claim releases its scope once, then its audit is
    // generated and posted; each verb binds the committed revision it read.
    let release = parse("claim.release_scope", json!({"claim": hex_of(claim.0)}));
    assert_eq!(
        requirements(&release).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(claim)
        ])]
    );
    h.run(ISSUER, 12, &release, &h.resolved(claim, None, None));
    assert!(h.view().claim(claim).unwrap().scopes().released());
    let generate = parse("audit.generate", json!({"claim": hex_of(claim.0)}));
    let (compiled, _, outcome) = h.run(ISSUER, 13, &generate, &h.resolved(claim, None, None));
    assert_eq!(
        compiled.created[0].kind,
        NativeIdentityKind::ResultTestament
    );
    let audit = TestamentId(compiled.created[0].id);
    assert_eq!(outcome.result_testaments, 1);
    let generated = h.view().claim_result_testament(claim).unwrap();
    assert_eq!(generated.testament().binding().object.0, audit.0);
    let post = parse("audit.post", json!({"testament": hex_of(audit.0)}));
    assert_eq!(
        requirements(&post).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::ResultTestament(audit)
        ])]
    );
    let mut resolved = Resolved::default();
    resolved.result_testaments.push(ResolvedResultTestament {
        binding: generated.testament().binding(),
        claim,
        posted: false,
    });
    h.run(ISSUER, 14, &post, &resolved);
    assert_eq!(
        h.view()
            .result_testament(audit)
            .unwrap()
            .testament()
            .state(),
        focal_model::lifecycle::audit::ResultTestamentState::Posted
    );
    resolved.result_testaments[0].posted = true;
    assert!(matches!(
        compile(
            &post,
            &context(ISSUER),
            RequestId::from_u128(15),
            &mut ids(15_000),
            &resolved,
            &CompileLimits::default()
        ),
        Err(CompileError::Unsupported(_))
    ));
}

#[test]
fn the_remaining_verbs_name_their_reads_and_bind_committed_objects() {
    let claim = ClaimId::from_u128(10);
    let other = ClaimId::from_u128(11);
    let artifact = ArtifactId::from_u128(15);
    let testament = TestamentId::from_u128(16);
    use focal_wire::NativeObjectRef as R;
    let cases: [(&str, serde_json::Value, Vec<Requirement>); 12] = [
        (
            "claim.release_scope",
            json!({"claim": hex(10)}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
        (
            "receipt.adopt",
            json!({"claim": hex(10), "holder": "self"}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
        (
            "artifact.fail",
            json!({"claim": hex(10), "slot": 1, "diagnostic": hex(15)}),
            vec![Requirement::Objects(vec![
                R::Claim(claim),
                R::Diagnostic(artifact),
            ])],
        ),
        (
            "artifact.receive",
            json!({"claim": hex(10), "artifact": hex(15)}),
            vec![Requirement::Objects(vec![
                R::Claim(claim),
                R::Work(artifact),
            ])],
        ),
        (
            "artifact.reject",
            json!({"claim": hex(10), "artifact": hex(15), "reason": "structure", "payload": {"type": "text", "text": "{}"}}),
            vec![Requirement::Objects(vec![
                R::Claim(claim),
                R::Work(artifact),
                R::Artifact(artifact),
            ])],
        ),
        (
            "validation.seal_increments",
            json!({"claim": hex(10)}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
        (
            "validation.enter_whole_work",
            json!({"claim": hex(10), "testament": hex(16)}),
            vec![Requirement::Objects(vec![
                R::Claim(claim),
                R::Response(testament),
            ])],
        ),
        (
            "audit.generate",
            json!({"claim": hex(10)}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
        (
            "audit.post",
            json!({"testament": hex(16)}),
            vec![Requirement::Objects(vec![R::ResultTestament(testament)])],
        ),
        (
            "monitor.register",
            json!({"claim": hex(10), "roots": [{"predicate": "satisfied", "claim": hex(11)}], "deadline": {"at": 10}}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
        (
            "monitor.rebind",
            json!({"claim": hex(10), "monitor": hex(18), "predecessor": hex(11), "successor": hex(12)}),
            vec![Requirement::Objects(vec![
                R::Claim(claim),
                R::Claim(other),
                R::Claim(ClaimId::from_u128(12)),
            ])],
        ),
        (
            "monitor.cancel",
            json!({"claim": hex(10), "monitor": hex(18)}),
            vec![Requirement::Objects(vec![R::Claim(claim)])],
        ),
    ];
    for (name, document, expected) in cases {
        let operation = parse(name, document);
        assert_eq!(requirements(&operation).unwrap(), expected, "{name}");
        // Nothing compiles without the objects it binds to.
        assert!(
            matches!(
                compile(
                    &operation,
                    &context(ISSUER),
                    RequestId::from_u128(1),
                    &mut ids(1_000),
                    &Resolved::default(),
                    &CompileLimits::default()
                ),
                Err(CompileError::Missing(_)),
            ),
            "{name}"
        );
    }

    // A claim with a current receipt: adoption fences it, monitors carry it.
    let binding = Binding {
        ledger: ledger(),
        object: ObjectId(claim.0),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(3),
    };
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(40),
        epoch: 2,
    };
    let mut resolved = Resolved::default();
    resolved.claims.push(ResolvedClaim {
        binding,
        issuer: ISSUER,
        subject: SUBJECT,
        status: ClaimStatus::Received,
        receipt: Some((SUBJECT, fence)),
        response_count: 0,
        latest_response: None,
    });
    let adopt = parse(
        "receipt.adopt",
        json!({"claim": hex(10), "holder": hex_bytes(&EVALUATOR.0)}),
    );
    let compiled = compile(
        &adopt,
        &context(ISSUER),
        RequestId::from_u128(2),
        &mut ids(2_000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::AdoptReceipt {
        expected,
        previous,
        receipt,
        holder,
    } = compiled.input.command
    else {
        panic!()
    };
    assert_eq!((expected, previous, holder), (binding, fence, EVALUATOR));
    assert_eq!(
        compiled.created,
        vec![NativeIdentity {
            kind: NativeIdentityKind::Receipt,
            id: receipt.0
        }]
    );
    let register = parse(
        "monitor.register",
        json!({"claim": hex(10), "roots": [{"predicate": "released", "claim": hex(11)}, {"predicate": "satisfied", "claim": hex(11)}], "deadline": {"at": 10}}),
    );
    let compiled = compile(
        &register,
        &context(ISSUER),
        RequestId::from_u128(3),
        &mut ids(3_000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::RegisterMonitor {
        expected,
        receipt,
        id,
        roots,
        deadline,
    } = compiled.input.command
    else {
        panic!()
    };
    assert_eq!((expected, receipt), (binding, Some(fence)));
    assert_eq!(
        roots,
        vec![
            WaitPredicate::Released(other),
            WaitPredicate::Satisfied(other)
        ]
    );
    assert_eq!((deadline.generation, deadline.at), (1, 10));
    assert_eq!(
        compiled.created,
        vec![NativeIdentity {
            kind: NativeIdentityKind::Monitor,
            id: id.0
        }]
    );
    // Duplicate roots, unknown predicates and self-succession are refused.
    for document in [
        json!({"claim": hex(10), "roots": [{"predicate": "satisfied", "claim": hex(11)}, {"predicate": "satisfied", "claim": hex(11)}], "deadline": {"at": 10}}),
        json!({"claim": hex(10), "roots": [{"predicate": "done", "claim": hex(11)}], "deadline": {"at": 10}}),
        json!({"claim": hex(10), "roots": [], "deadline": {"at": 10}}),
    ] {
        assert!(matches!(
            compile(
                &parse("monitor.register", document),
                &context(ISSUER),
                RequestId::from_u128(4),
                &mut ids(4_000),
                &resolved,
                &CompileLimits::default()
            ),
            Err(CompileError::Input(_))
        ));
    }
    resolved.claims.push(ResolvedClaim {
        binding: Binding {
            object: ObjectId(other.0),
            ..binding
        },
        ..resolved.claims[0]
    });
    let rebind = parse(
        "monitor.rebind",
        json!({"claim": hex(10), "monitor": hex(18), "predecessor": hex(11), "successor": hex(11)}),
    );
    assert!(matches!(
        compile(
            &rebind,
            &context(ISSUER),
            RequestId::from_u128(5),
            &mut ids(5_000),
            &resolved,
            &CompileLimits::default()
        ),
        Err(CompileError::Input(_))
    ));
}
fn hex_of(id: [u8; 16]) -> String {
    hex(u128::from_be_bytes(id))
}
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn creation_rules_are_checked_before_any_identity_leaves_the_compiler() {
    let limits = CompileLimits::default();
    let refused = |document: Value, expected: &str| {
        let operation = parse("claim.submit", document);
        let error = compile(
            &operation,
            &context(ISSUER),
            RequestId::from_u128(1),
            &mut ids(1),
            &Resolved::default(),
            &limits,
        )
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{expected}: {error}");
    };
    let mut document = claim_document();
    document["validations"].as_array_mut().unwrap().remove(0);
    document["slots"][0]["checks"][0]["declaration"] = json!(0);
    refused(document, "receipt (delivery) declaration");
    let mut document = claim_document();
    document["relations"] = json!([{"kind": "issuer", "target": format!("claim:{}", hex(5))}]);
    refused(document, "are derived");
    let mut document = claim_document();
    document["relations"] = json!([{"kind": "reviews", "target": format!("artifact:{}", hex(5))}]);
    refused(document, "artifact:ID@HASH");
    let mut document = claim_document();
    document["relations"] =
        json!([{"kind": "depends_on", "target": format!("artifact:{}@{}", hex(5), hash(5))}]);
    refused(document, "only reviews and derived_from");
    let mut document = claim_document();
    document["relations"] = json!([{"kind": "depends_on", "target": hex(5)}]);
    refused(document, "claim:ID");
    let mut document = claim_document();
    document["slots"] = json!([{"slot": 0, "checks": [{"declaration": 9}]}]);
    refused(document, "declaration index outside");
    let mut document = claim_document();
    document["validations"][1]["handlers"] = json!([]);
    refused(document, "at least one handler");
    let mut document = claim_document();
    document["validations"][1]["phase"] = json!("admission");
    refused(document, "phase does not match");
    let mut document = claim_document();
    document["target"] = json!(hex(1));
    refused(document, "contract");
    let mut document = claim_document();
    document["scopes"] = json!([{"kind": "file", "key": "a"}, {"kind": "file", "key": "a"}]);
    refused(document, "duplicate scope");
    // Zero request identity and a missing parent are refused too.
    let operation = parse("claim.submit", claim_document());
    assert!(
        compile(
            &operation,
            &context(ISSUER),
            RequestId::from_u128(0),
            &mut ids(1),
            &Resolved::default(),
            &limits
        )
        .is_err()
    );
    let mut document = claim_document();
    document["parent"] = json!(hex(55));
    let operation = parse("claim.submit", document);
    assert_eq!(
        requirements(&operation).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(ClaimId::from_u128(55))
        ])]
    );
    assert!(matches!(
        compile(
            &operation,
            &context(ISSUER),
            RequestId::from_u128(1),
            &mut ids(1),
            &Resolved::default(),
            &limits
        ),
        Err(CompileError::Missing("claim"))
    ));
}

#[test]
fn responses_need_diagnostics_for_failure_and_bind_content_to_the_authored_report() {
    let mut h = Harness::new();
    let (compiled, _, _) = h.run(
        ISSUER,
        1,
        &parse("claim.submit", claim_document()),
        &Resolved::default(),
    );
    let claim = ClaimId(compiled.created[0].id);
    h.run(
        ISSUER,
        2,
        &parse("claim.post", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    h.run(
        SUBJECT,
        3,
        &parse("receipt.acquire", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    let resolved = h.resolved(claim, None, None);
    let failed = parse(
        "testament.submit",
        json!({"claim": hex_of(claim.0), "summary": "Could not build.", "confidence": "committed", "outcome": "failed"}),
    );
    assert!(matches!(
        compile(
            &failed,
            &context(SUBJECT),
            RequestId::from_u128(4),
            &mut ids(4000),
            &resolved,
            &CompileLimits::default()
        ),
        Err(CompileError::Input(InputError::Invalid(_)))
    ));
    let (compiled, _, _) = h.run(SUBJECT, 4, &parse("artifact.diagnostic", json!({"claim": hex_of(claim.0), "reason": "production", "payload": {"type": "text", "text": DIAGNOSTIC}})), &resolved);
    let diagnostic = h
        .view()
        .artifact(ArtifactId(compiled.created[0].id))
        .unwrap()
        .descriptor()
        .content_hash();
    let cited = parse(
        "testament.submit",
        json!({"claim": hex_of(claim.0), "summary": "Could not build.", "confidence": "committed", "outcome": "failed",
        "diagnostics": [{"id": hex_of(compiled.created[0].id), "hash": hex_bytes(&diagnostic.0)}]}),
    );
    let first = compile(
        &cited,
        &context(SUBJECT),
        RequestId::from_u128(5),
        &mut ids(5000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::CloseResponse {
        response, report, ..
    } = &first.input.command
    else {
        panic!()
    };
    assert_eq!(response.revision, ObjectRevision(1));
    assert_ne!(response.content.0, [0; 32]);
    assert_eq!(report.diagnostics.len(), 1);
    // Different testimony under the same identity binds different content.
    let mut other = cited.clone();
    if let NativeAuthoredOperation::TestamentSubmit(document) = &mut other {
        document.summary = "Could not link.".into();
    }
    let second = compile(
        &other,
        &context(SUBJECT),
        RequestId::from_u128(5),
        &mut ids(5000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::CloseResponse {
        response: other_response,
        ..
    } = &second.input.command
    else {
        panic!()
    };
    assert_eq!(other_response.object, response.object);
    assert_ne!(other_response.content, response.content);
    let (_, _, outcome) = h.run(SUBJECT, 5, &cited, &resolved);
    assert_eq!(outcome.responses, 1);
}

#[test]
fn resolution_reads_wire_objects_and_selects_the_current_evaluation() {
    use focal_wire::*;
    let ledger = ledger();
    let binding = |object: u128, revision: u64| NativeBinding {
        object: ObjectId::from_u128(object),
        content: ContentHash([9; 32]),
        revision: ObjectRevision(revision),
    };
    let evaluation = |generation: u64, slot: u32, state: NativeValidationState, begun: bool| {
        NativeObject::Evaluation(Box::new(NativeEvaluation {
            binding: binding(500 + generation as u128, 1),
            key: NativeEvaluationKey {
                claim: ClaimId::from_u128(10),
                validation: ValidationId::from_u128(11),
                target: NativeEvaluationTarget::Work {
                    response: TestamentId::from_u128(16),
                    slot,
                    artifact: ArtifactId::from_u128(15),
                },
                generation,
            },
            target: NativeTarget::Artifact {
                response: binding(16, 1),
                slot,
                artifact: binding(15, 1),
            },
            state,
            phase: NativePhase::Programmatic,
            declared_phase: ValidationPhase::WholeWork,
            declaration_index: 1,
            issuer: ISSUER,
            evaluator: Some(EVALUATOR),
            mode: ValidationMode::Required,
            receipt: Some(ReceiptFence {
                receipt: ReceiptId::from_u128(13),
                epoch: 1,
            }),
            has_begun: begun,
            attempt_index: begun.then_some(0),
            attempt_bound: 2,
            fence: None,
            suppression: None,
            last_result: None,
            sealed: None,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 10_000,
            },
            current_attempt: begun.then_some(NativeAttempt {
                phase: NativePhase::Programmatic,
                index: 0,
                handler: ValidatorId::from_u128(77),
                version: ContentHash([77; 32]),
                evaluator: EVALUATOR,
                definition: ContentHash([78; 32]),
            }),
        }))
    };
    let objects = vec![
        NativeObject::Missing(NativeObjectRef::Claim(ClaimId::from_u128(99))),
        NativeObject::Claim(Box::new(NativeClaim {
            binding: binding(10, 3),
            issuer: ISSUER,
            subject: SUBJECT,
            created: SessionSeq(1),
            deadline: None,
            status: ClaimStatus::Received,
            origin: NativeClaimOrigin::Native,
            released: false,
            receipt: Some(NativeEntitlement {
                holder: SUBJECT,
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(13),
                    epoch: 1,
                },
            }),
            local_complete: false,
            local_sealed_at: None,
            terminal: None,
            latest_response: None,
            response_count: 2,
            max_responses: 4,
            obligations: Vec::new(),
            cause: Cause::Root(RootCommandId::from_u128(900)),
            corrections: Vec::new(),
            acceptance: Vec::new(),
            scopes: None,
            content: None,
        })),
        NativeObject::Response(Box::new(NativeResponse {
            binding: binding(16, 1),
            claim: ClaimId::from_u128(10),
            receipt: ReceiptFence {
                receipt: ReceiptId::from_u128(13),
                epoch: 1,
            },
            cycle: 2,
            prior: None,
            respondent: SUBJECT,
            state: NativeResponseState::Posted,
            summary: "done".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            manifest: Vec::new(),
            failed_work: Vec::new(),
            diagnostics: Vec::new(),
            terminal: None,
        })),
        evaluation(1, 0, NativeValidationState::Validated, true),
        evaluation(2, 0, NativeValidationState::Validating, true),
        evaluation(2, 1, NativeValidationState::Ready, false),
    ];
    let resolved = Resolved::from_objects(ledger, &objects).unwrap();
    let claim = resolved.claim(ClaimId::from_u128(10)).unwrap();
    assert_eq!(claim.binding.revision, ObjectRevision(3));
    assert_eq!(claim.receipt.unwrap().0, SUBJECT);
    assert_eq!(claim.response_count, 2);
    assert!(resolved.claim(ClaimId::from_u128(99)).is_err());
    assert_eq!(
        resolved.response(TestamentId::from_u128(16)).unwrap().cycle,
        2
    );
    // Generation two on slot zero is current; the terminal generation one is
    // skipped and the slot-one evaluation needs the slot named.
    assert!(
        resolved
            .evaluation(
                ClaimId::from_u128(10),
                ValidationId::from_u128(11),
                EvaluationSelector::WholeWork { slot: None },
            )
            .is_err()
    );
    let current = resolved
        .evaluation(
            ClaimId::from_u128(10),
            ValidationId::from_u128(11),
            EvaluationSelector::WholeWork { slot: Some(0) },
        )
        .unwrap();
    assert_eq!(current.key.generation, 2);
    assert!(current.has_begun && !current.terminal);
    assert_eq!(current.attempt.unwrap().evaluator, EVALUATOR);
    let idle = resolved
        .evaluation(
            ClaimId::from_u128(10),
            ValidationId::from_u128(11),
            EvaluationSelector::WholeWork { slot: Some(1) },
        )
        .unwrap();
    assert!(!idle.has_begun && idle.attempt.is_none());
    // Compiling begin/report against these bindings uses the exact wire values.
    let begin = parse(
        "validation.begin",
        json!({"claim": hex(10), "validation": hex(11), "slot": 1}),
    );
    let compiled = compile(
        &begin,
        &context(EVALUATOR),
        RequestId::from_u128(1),
        &mut ids(1),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::BeginWork {
        claim,
        key,
        expected,
    } = compiled.input.command
    else {
        panic!()
    };
    assert_eq!(claim.revision, ObjectRevision(3));
    assert_eq!(key.generation, 2);
    assert_eq!(expected.object, ObjectId::from_u128(502));
    let report = parse(
        "validation.report",
        json!({"claim": hex(10), "validation": hex(11), "slot": 0, "verdict": "error", "payload": {"type": "text", "text": DIAGNOSTIC}}),
    );
    let compiled = compile(
        &report,
        &context(EVALUATOR),
        RequestId::from_u128(2),
        &mut ids(2),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::ReportWork {
        report, artifact, ..
    } = &compiled.input.command
    else {
        panic!()
    };
    assert_eq!(report.value, VerdictValue::Error);
    assert_eq!(artifact.get().unwrap().kind(), "error");
    assert_eq!(
        artifact.get().unwrap().result_provenance().unwrap().attempt,
        report.attempt
    );
    let frame = encode_frame(
        ledger,
        NativeContentProfile::AuthoredV1,
        &compiled.input,
        CompileLimits::default().encoding(),
    )
    .unwrap();
    assert_eq!(inspect_native_frame(&frame).unwrap().command, 19);
    assert_eq!(
        focal_client::operations::frame_tags(focal_wire::NativeOperationKind::ReportWork),
        &[inspect_native_frame(&frame).unwrap().command]
    );
}

#[test]
fn a_challenge_cites_exact_committed_evidence_and_carries_its_policy_through_the_owner() {
    let mut h = Harness::new();
    let (compiled, _, _) = h.run(
        ISSUER,
        1,
        &parse("claim.submit", claim_document()),
        &Resolved::default(),
    );
    let claim = ClaimId(compiled.created[0].id);
    h.run(
        ISSUER,
        2,
        &parse("claim.post", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    h.run(
        SUBJECT,
        3,
        &parse("receipt.acquire", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    let work = parse(
        "artifact.submit",
        json!({"claim": hex_of(claim.0), "slot": 0, "payload": {"type": "text", "text": PROOF}}),
    );
    let (compiled, _, _) = h.run(SUBJECT, 4, &work, &h.resolved(claim, None, None));
    let artifact = ArtifactId(compiled.created[0].id);
    let artifact_hash = h
        .view()
        .artifact(artifact)
        .unwrap()
        .descriptor()
        .content_hash();
    let hash_hex = artifact_hash
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    // A challenge reviewing the exact artifact, with its follow-up policy,
    // compiles to descriptor schema 2 and commits.
    let mut challenge = claim_document();
    challenge["action"] = json!("challenge");
    challenge["relations"] = json!([{"kind": "reviews", "target": format!("artifact:{}@{hash_hex}", hex_of(artifact.0))}]);
    challenge["policy"] = json!({"corrective_allowed": true, "max_follow_ups": 1, "single_issuer": true, "escalation": "evaluator"});
    let (compiled, _, _) = h.run(
        ISSUER,
        5,
        &parse("claim.submit", challenge.clone()),
        &Resolved::default(),
    );
    let challenge_id = ClaimId(compiled.created[0].id);
    {
        let view = h.view();
        let content = view.claim_content(challenge_id).unwrap();
        assert_eq!(content.schema(), 2);
        assert_eq!(
            content.policy(),
            Some(PeerPolicy {
                corrective_allowed: true,
                max_follow_ups: 1,
                single_issuer: true,
                escalation: Escalation::Evaluator,
            })
        );
        assert!(content.relations().iter().any(|relation| {
            relation.kind == RelationKind::Reviews
                && relation.target
                    == RelationTarget::Evidence(ArtifactRef {
                        id: artifact,
                        hash: artifact_hash,
                    })
        }));
        // An ordinary claim keeps schema 1.
        assert_eq!(view.claim_content(claim).unwrap().schema(), 1);
    }

    // The wrong hash and an unknown artifact are refused by the owner; a
    // dependency on evidence is refused before any frame exists.
    for target in [
        format!("artifact:{}@{}", hex_of(artifact.0), hash(9)),
        format!("artifact:{}@{hash_hex}", hex(0xdead)),
    ] {
        let mut stale = claim_document();
        stale["relations"] = json!([{"kind": "reviews", "target": target}]);
        let compiled = compile(
            &parse("claim.submit", stale),
            &context(ISSUER),
            RequestId::from_u128(6),
            &mut ids(6000),
            &Resolved::default(),
            &CompileLimits::default(),
        )
        .unwrap();
        let limits = CompileLimits::default();
        let frame = encode_frame(
            ledger(),
            NativeContentProfile::AuthoredV1,
            &compiled.input,
            limits.encoding(),
        )
        .unwrap();
        h.time += 1;
        let context = NativeContext {
            principal: Principal::Actor(ISSUER),
            logical_time: h.time,
        };
        let work = DecodeWork {
            parse: 1 << 24,
            source: 1 << 24,
            model: 1 << 24,
            acceptance: 1 << 24,
            native: 1 << 24,
        };
        let decode =
            NativeDecodeLimits::for_native(limits.native, limits.frame.bytes, work).unwrap();
        assert!(
            h.owner
                .prepare_frame_with_custody(
                    context,
                    &frame,
                    decode,
                    &mut h.store,
                    ContentDomainId::from_u128(93),
                    &BuiltinNativeSchemas,
                )
                .is_err()
        );
    }
    let mut dependent = claim_document();
    dependent["relations"] = json!([{"kind": "depends_on", "target": format!("artifact:{}@{hash_hex}", hex_of(artifact.0))}]);
    assert!(matches!(
        compile(
            &parse("claim.submit", dependent),
            &context(ISSUER),
            RequestId::from_u128(7),
            &mut ids(7000),
            &Resolved::default(),
            &CompileLimits::default()
        ),
        Err(CompileError::Input(InputError::Invalid(_)))
    ));
}

const STRANGER: ParticipantId = ParticipantId::from_u128(9);
const FAILED_PROOF: &str = r#"{"passed":0,"failed":1,"skipped":0}"#;

/// Drives one challenge through its two-party cycle to a terminal verdict and
/// returns the challenge, its verdict report artifact and that report's hash.
fn challenged(
    h: &mut Harness,
    base: u128,
    policy: Option<Value>,
    verdict: &str,
) -> (ClaimId, ArtifactId, ArtifactId, String) {
    let mut challenge = claim_document();
    challenge["action"] = json!("challenge");
    if let Some(policy) = policy {
        challenge["policy"] = policy;
    }
    let (compiled, _, _) = h.run(
        ISSUER,
        base,
        &parse("claim.submit", challenge),
        &Resolved::default(),
    );
    let claim = ClaimId(compiled.created[0].id);
    let validation = ValidationId(compiled.created[2].id);
    h.run(
        ISSUER,
        base + 1,
        &parse("claim.post", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    h.run(
        SUBJECT,
        base + 2,
        &parse("receipt.acquire", json!({"claim": hex_of(claim.0)})),
        &h.resolved(claim, None, None),
    );
    let work = parse(
        "artifact.submit",
        json!({"claim": hex_of(claim.0), "slot": 0, "payload": {"type": "text", "text": PROOF}}),
    );
    let (compiled, _, _) = h.run(SUBJECT, base + 3, &work, &h.resolved(claim, None, None));
    let artifact = ArtifactId(compiled.created[0].id);
    let artifact_hash = h
        .view()
        .artifact(artifact)
        .unwrap()
        .descriptor()
        .content_hash();
    let testament = parse(
        "testament.submit",
        json!({
            "claim": hex_of(claim.0), "summary": "Done.", "confidence": "committed", "outcome": "complete",
            "manifest": [{"slot": 0, "artifact": {"id": hex_of(artifact.0), "hash": hex_bytes(&artifact_hash.0)}}]
        }),
    );
    let (compiled, _, _) = h.run(
        SUBJECT,
        base + 4,
        &testament,
        &h.resolved(claim, None, None),
    );
    let response = TestamentId(compiled.created[0].id);
    h.run(
        SUBJECT,
        base + 5,
        &parse(
            "testament.post",
            json!({"claim": hex_of(claim.0), "testament": hex_of(response.0)}),
        ),
        &h.resolved(claim, Some(response), None),
    );
    h.run(
        ISSUER,
        base + 6,
        &parse(
            "testament.receive",
            json!({"claim": hex_of(claim.0), "testament": hex_of(response.0)}),
        ),
        &h.resolved(claim, Some(response), None),
    );
    let key = EvaluationKey {
        claim,
        validation,
        target: EvaluationTarget::Work {
            response,
            slot: 0,
            artifact,
        },
        generation: 1,
    };
    h.run(
        EVALUATOR,
        base + 7,
        &parse(
            "validation.begin",
            json!({"claim": hex_of(claim.0), "validation": hex_of(validation.0)}),
        ),
        &h.resolved(claim, None, Some(key)),
    );
    let payload = if verdict == "pass" {
        PROOF
    } else {
        FAILED_PROOF
    };
    let (compiled, _, _) = h.run(
        EVALUATOR,
        base + 8,
        &parse(
            "validation.report",
            json!({"claim": hex_of(claim.0), "validation": hex_of(validation.0), "verdict": verdict, "payload": {"type": "text", "text": payload}}),
        ),
        &h.resolved(claim, None, Some(key)),
    );
    let report = ArtifactId(compiled.created[0].id);
    assert!(h.view().evaluation(key).unwrap().state().is_terminal());
    let report_hash = hex_bytes(
        &h.view()
            .artifact(report)
            .unwrap()
            .descriptor()
            .content_hash()
            .0,
    );
    (claim, artifact, report, report_hash)
}

fn correction_of(challenge: ClaimId, artifact: ArtifactId, hash: &str) -> Value {
    let mut document = claim_document();
    document["action"] = json!("correction");
    document["description"] = json!("Redo the inspection with the missing cases.");
    document["relations"] = json!([
        {"kind": "invalidates", "target": format!("claim:{}", hex_of(challenge.0))},
        {"kind": "reviews", "target": format!("artifact:{}@{hash}", hex_of(artifact.0))}
    ]);
    document
}

#[test]
fn a_correction_rests_on_the_challenge_s_failed_verdict_under_its_authored_policy() {
    let mut h = Harness::new();
    let policy = json!({"corrective_allowed": true, "max_follow_ups": 0, "single_issuer": true, "escalation": "evaluator"});
    let (challenge, work_artifact, report, hash) = challenged(&mut h, 100, Some(policy), "fail");
    assert!(h.view().claim(challenge).unwrap().status().is_terminal());
    let submit = |document: Value| parse("claim.submit", document);
    let refusal = |h: &mut Harness, actor: ParticipantId, request: u128, document: Value| {
        h.attempt(actor, request, &submit(document), &Resolved::default())
            .expect_err("the owner must refuse")
    };

    // Nobody outside the challenge's issuer, holder and reporting evaluator.
    let error = refusal(
        &mut h,
        STRANGER,
        200,
        correction_of(challenge, report, &hash),
    );
    assert!(error.contains("WrongActor"), "{error}");
    // The failed verdict must be cited by its report, not by the work it judged.
    let work_hash = hex_bytes(
        &h.view()
            .artifact(work_artifact)
            .unwrap()
            .descriptor()
            .content_hash()
            .0,
    );
    let error = refusal(
        &mut h,
        ISSUER,
        201,
        correction_of(challenge, work_artifact, &work_hash),
    );
    assert!(error.contains("MissingEvidence"), "{error}");
    // Only a challenge can be invalidated, and only one that allows it.
    let (plain, _, _) = h.run(ISSUER, 202, &submit(claim_document()), &Resolved::default());
    let plain = ClaimId(plain.created[0].id);
    let error = refusal(&mut h, ISSUER, 203, correction_of(plain, report, &hash));
    assert!(error.contains("InvalidTarget"), "{error}");
    let (bare, _, bare_report, bare_hash) = challenged(&mut h, 300, None, "fail");
    let error = refusal(
        &mut h,
        ISSUER,
        400,
        correction_of(bare, bare_report, &bare_hash),
    );
    assert!(error.contains("InvalidPolicy"), "{error}");
    // A passing verdict is no ground for a correction.
    let allowed = json!({"corrective_allowed": true, "max_follow_ups": 0, "single_issuer": false, "escalation": "holder"});
    let (passed, _, pass_report, pass_hash) = challenged(&mut h, 500, Some(allowed), "pass");
    let error = refusal(
        &mut h,
        ISSUER,
        600,
        correction_of(passed, pass_report, &pass_hash),
    );
    assert!(error.contains("InvalidTransition"), "{error}");
    // A correction is not a plain claim: the compiler refuses the wrong shape
    // before any identity is spent.
    let mut plain_invalidating = claim_document();
    plain_invalidating["relations"] =
        json!([{"kind": "invalidates", "target": format!("claim:{}", hex_of(challenge.0))}]);
    let error = refusal(&mut h, ISSUER, 601, plain_invalidating);
    assert!(
        error.contains("only a correction may invalidate"),
        "{error}"
    );
    let mut unreviewed = correction_of(challenge, report, &hash);
    unreviewed["relations"] =
        json!([{"kind": "invalidates", "target": format!("claim:{}", hex_of(challenge.0))}]);
    let error = refusal(&mut h, ISSUER, 602, unreviewed);
    assert!(error.contains("exactly one verdict artifact"), "{error}");

    // The reporting evaluator corrects under `escalation: evaluator`; the
    // correction is schema 2 and records what it invalidates and reviews.
    let (compiled, _) = h
        .attempt(
            EVALUATOR,
            603,
            &submit(correction_of(challenge, report, &hash)),
            &Resolved::default(),
        )
        .unwrap();
    let correction = ClaimId(compiled.created[0].id);
    {
        let view = h.view();
        let content = view.claim_content(correction).unwrap();
        assert_eq!(content.schema(), 2);
        assert_eq!(content.action(), ActionType::Correction);
        assert_eq!(content.issuer(), EVALUATOR);
        assert!(content.relations().iter().any(|relation| relation.kind
            == RelationKind::Invalidates
            && relation.target == RelationTarget::Object(ObjectRef::claim(ledger(), challenge))));
        assert!(content.relations().iter().any(|relation| relation.kind == RelationKind::Reviews
            && matches!(relation.target, RelationTarget::Evidence(evidence) if evidence.id == report)));
        // The challenge itself is untouched: terminal, same revision.
        assert!(view.claim(challenge).unwrap().status().is_terminal());
    }
    // `single_issuer`: the issuer's and the holder's later corrections conflict.
    let error = refusal(&mut h, ISSUER, 604, correction_of(challenge, report, &hash));
    assert!(error.contains("ConflictingCause"), "{error}");
    // (The holder addresses its correction to the issuer: a claim never
    // targets its own author.)
    let mut by_holder = correction_of(challenge, report, &hash);
    by_holder["target"] = json!(hex(1));
    let error = refusal(&mut h, SUBJECT, 605, by_holder);
    assert!(error.contains("ConflictingCause"), "{error}");
    // Corrections are readable by what they invalidate.
    assert_eq!(
        h.view()
            .related_claims(RelationKind::Invalidates, challenge)
            .collect::<Vec<_>>(),
        vec![correction]
    );
}

#[test]
fn consult_follow_ups_refine_their_parent_within_its_authored_policy() {
    let mut h = Harness::new();
    let mut consult = claim_document();
    consult["action"] = json!("consultation");
    consult["policy"] = json!({"corrective_allowed": false, "max_follow_ups": 1, "single_issuer": false, "escalation": "none"});
    let (compiled, _, _) = h.run(
        ISSUER,
        1,
        &parse("claim.submit", consult),
        &Resolved::default(),
    );
    let parent = ClaimId(compiled.created[0].id);
    // A follow-up addressed to `target` (a claim never targets its author).
    let follow_up = |parent: ClaimId, target: u128| {
        let mut document = claim_document();
        document["action"] = json!("consultation");
        document["target"] = json!(hex(target));
        document["description"] = json!("And what about the edge cases?");
        document["relations"] =
            json!([{"kind": "refines", "target": format!("claim:{}", hex_of(parent.0))}]);
        parse("claim.submit", document)
    };
    // `escalation: none` reserves follow-ups to the issuer.
    let error = h
        .attempt(SUBJECT, 2, &follow_up(parent, 1), &Resolved::default())
        .expect_err("the subject is not the issuer");
    assert!(error.contains("WrongActor"), "{error}");
    let (compiled, _) = h
        .attempt(ISSUER, 3, &follow_up(parent, 2), &Resolved::default())
        .unwrap();
    let first = ClaimId(compiled.created[0].id);
    assert_eq!(
        h.view().claim_content(first).unwrap().action(),
        ActionType::Consultation
    );
    // `max_follow_ups: 1` is exhausted; the refusal cites the policy.
    let error = h
        .attempt(ISSUER, 4, &follow_up(parent, 2), &Resolved::default())
        .expect_err("the follow-up bound is exhausted");
    assert!(error.contains("InvalidPolicy"), "{error}");
    // A consultation without a policy bounds nobody (schema-1 behaviour).
    let mut open = claim_document();
    open["action"] = json!("consultation");
    let (compiled, _, _) = h.run(
        ISSUER,
        5,
        &parse("claim.submit", open),
        &Resolved::default(),
    );
    let open = ClaimId(compiled.created[0].id);
    h.attempt(SUBJECT, 6, &follow_up(open, 1), &Resolved::default())
        .unwrap();
    h.attempt(STRANGER, 7, &follow_up(open, 2), &Resolved::default())
        .unwrap();
    assert_eq!(
        h.view().related_claims(RelationKind::Refines, open).count(),
        2
    );
}

#[test]
fn peer_verbs_are_authored_shapes_of_claim_submit_with_derived_identities() {
    let mut h = Harness::new();
    let policy = json!({"corrective_allowed": true, "max_follow_ups": 1, "single_issuer": true, "escalation": "evaluator"});
    let (challenge, work, report, report_hash) = challenged(&mut h, 100, Some(policy), "fail");
    let work_hash = h.view().artifact(work).unwrap().descriptor().content_hash();

    // claim.challenge disputes one exact artifact; an omitted hash is read
    // from the ledger before the frame is compiled.
    let mut document = claim_document();
    document["artifact"] = json!(hex_of(work.0));
    document["policy"] = json!({"corrective_allowed": false, "max_follow_ups": 0, "single_issuer": false, "escalation": "none"});
    let operation = parse("claim.challenge", document);
    assert_eq!(
        requirements(&operation).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Artifact(work)
        ])]
    );
    let mut resolved = Resolved::default();
    resolved.artifacts.push(ResolvedArtifact {
        id: work,
        content_hash: work_hash,
        visibility: Vec::new(),
    });
    let (compiled, _) = h.attempt(ISSUER, 200, &operation, &resolved).unwrap();
    let disputing = ClaimId(compiled.created[0].id);
    {
        let view = h.view();
        let content = view.claim_content(disputing).unwrap();
        assert_eq!(content.action(), ActionType::Challenge);
        assert_eq!(content.schema(), 2);
        assert!(content.relations().iter().any(|relation| {
            relation.kind == RelationKind::Reviews
                && relation.target
                    == RelationTarget::Evidence(ArtifactRef {
                        id: work,
                        hash: work_hash,
                    })
        }));
        assert_eq!(content.policy().unwrap().escalation, Escalation::None);
    }

    // claim.correct by the reporting evaluator: the challenge's subject is
    // the default target, the verdict hash is read from the ledger, and the
    // occurrence derives from the facts, so a second delivery with a fresh
    // request identity resolves to the committed correction.
    let correction = parse(
        "claim.correct",
        json!({
            "challenge": hex_of(challenge.0),
            "verdict": hex_of(report.0),
            "description": "Redo the inspection with the missing cases.",
            "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}]
        }),
    );
    assert_eq!(
        requirements(&correction).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(challenge),
            focal_wire::NativeObjectRef::Artifact(report),
        ])]
    );
    let mut resolved = h.resolved(challenge, None, None);
    resolved.artifacts.push(ResolvedArtifact {
        id: report,
        content_hash: ContentHash(hex_bytes_to_array(&report_hash)),
        visibility: Vec::new(),
    });
    let (compiled, outcome) = h.attempt(EVALUATOR, 300, &correction, &resolved).unwrap();
    assert_eq!(outcome.created, 1);
    let corrected = ClaimId(compiled.created[0].id);
    {
        let view = h.view();
        let content = view.claim_content(corrected).unwrap();
        assert_eq!(content.action(), ActionType::Correction);
        assert_eq!(content.subject(), SUBJECT);
        assert!(content.relations().iter().any(|relation| {
            relation.kind == RelationKind::Invalidates
                && relation.target == RelationTarget::Object(ObjectRef::claim(ledger(), challenge))
        }));
    }
    let (again, outcome) = h.attempt(EVALUATOR, 301, &correction, &resolved).unwrap();
    assert_eq!(
        outcome.created, 0,
        "the same facts resolve to one correction"
    );
    // The identities derive from the facts too, so both deliveries name
    // the same claim before the owner ever sees them.
    assert_eq!(again.created[0].id, compiled.created[0].id);
    assert_eq!(
        h.view()
            .related_claims(RelationKind::Invalidates, challenge)
            .collect::<Vec<_>>(),
        vec![corrected]
    );
    // A different description is a different correction, refused under
    // `single_issuer`.
    let mut other = correction.clone();
    let NativeAuthoredOperation::ClaimCorrect(document) = &mut other else {
        panic!()
    };
    document.description = "Another correction.".into();
    let error = h
        .attempt(EVALUATOR, 302, &other, &resolved)
        .expect_err("single issuer");
    assert!(error.contains("ConflictingCause"), "{error}");

    // claim.consult and claim.follow_up: the follow-up refines the
    // consultation and addresses its subject by default.
    let consult = parse(
        "claim.consult",
        json!({
            "description": "Which cases does the parser leave undefined?",
            "target": hex(2),
            "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}],
            "policy": {"max_follow_ups": 1, "escalation": "none"}
        }),
    );
    let (compiled, _) = h
        .attempt(ISSUER, 400, &consult, &Resolved::default())
        .unwrap();
    let consulted = ClaimId(compiled.created[0].id);
    let follow_up = parse(
        "claim.follow_up",
        json!({
            "refines": hex_of(consulted.0),
            "description": "And the unicode cases?",
            "validations": [{"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}]
        }),
    );
    assert_eq!(
        requirements(&follow_up).unwrap(),
        vec![Requirement::Objects(vec![
            focal_wire::NativeObjectRef::Claim(consulted)
        ])]
    );
    let resolved = h.resolved(consulted, None, None);
    let (compiled, _) = h.attempt(ISSUER, 401, &follow_up, &resolved).unwrap();
    let followed = ClaimId(compiled.created[0].id);
    {
        let view = h.view();
        let content = view.claim_content(followed).unwrap();
        assert_eq!(content.action(), ActionType::Consultation);
        assert_eq!(content.subject(), SUBJECT);
        assert!(content.relations().iter().any(|relation| {
            relation.kind == RelationKind::Refines
                && relation.target == RelationTarget::Object(ObjectRef::claim(ledger(), consulted))
        }));
    }
    let (_, outcome) = h.attempt(ISSUER, 402, &follow_up, &resolved).unwrap();
    assert_eq!(outcome.created, 0, "the same follow-up is one claim");
    // The subject may not file one under `escalation: none` (addressing it
    // to the issuer, since a claim never targets its author).
    let mut by_subject = follow_up.clone();
    let NativeAuthoredOperation::ClaimFollowUp(document) = &mut by_subject else {
        panic!()
    };
    document.target = Some(hex(1));
    let error = h
        .attempt(SUBJECT, 403, &by_subject, &resolved)
        .expect_err("escalation none");
    assert!(error.contains("WrongActor"), "{error}");
}

fn hex_bytes_to_array(text: &str) -> [u8; 32] {
    let mut out = [0; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).unwrap();
    }
    out
}

fn synthetic_claim(
    id: ClaimId,
    status: ClaimStatus,
    revision: u64,
    released: bool,
    cause: Cause,
    sequence: u64,
) -> focal_wire::NativeReadPage {
    use focal_wire::*;
    NativeReadPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(sequence),
            route_epoch: RouteEpoch(1),
        },
        native_sequence: SessionSeq(sequence),
        logical_time: 1,
        objects: vec![NativeObject::Claim(Box::new(NativeClaim {
            binding: NativeBinding {
                object: ObjectId(id.0),
                content: ContentHash([1; 32]),
                revision: ObjectRevision(revision),
            },
            issuer: ISSUER,
            subject: SUBJECT,
            created: SessionSeq(1),
            deadline: None,
            status,
            origin: NativeClaimOrigin::Native,
            released,
            receipt: None,
            local_complete: false,
            local_sealed_at: None,
            terminal: None,
            latest_response: None,
            response_count: 0,
            max_responses: 4,
            obligations: Vec::new(),
            cause,
            corrections: Vec::new(),
            acceptance: Vec::new(),
            scopes: None,
            content: None,
        }))],
        next: None,
        visited: 1,
    }
}

#[test]
fn the_wait_observer_and_the_lineage_read_compose_bounded_exact_reads() {
    use crate::NativeReadOutcome;
    use focal_client::claim_wait::ClaimWaitCondition;
    use focal_client::operations::{
        NativeObjectDocument, NativeReadOperation, NativeWaitDocument, NativeWaitUntil,
    };
    use focal_wire::*;
    let claim = ClaimId::from_u128(10);
    let root = Cause::Root(RootCommandId::from_u128(1));
    // Waiting: pending, then met, with one pause between the probes.
    let mut probes = 0u64;
    let mut reads = |request: NativeReadRequest| {
        let NativeReadQuery::Claim { id, .. } = request.query else {
            panic!("{request:?}");
        };
        probes += 1;
        Ok(match probes {
            1 => synthetic_claim(id, ClaimStatus::Received, 3, false, root.clone(), 5),
            _ => synthetic_claim(
                id,
                ClaimStatus::TestamentAcknowledged,
                4,
                false,
                root.clone(),
                6,
            ),
        })
    };
    let mut lists = |_: NativeListRequest| panic!("the wait lists nothing");
    let mut pauses = Vec::new();
    let mut pause = |duration: std::time::Duration| {
        pauses.push(duration);
        Ok(())
    };
    let outcome = crate::read(
        &NativeReadOperation::ClaimWait(NativeWaitDocument {
            claim: hex_of(claim.0),
            until: NativeWaitUntil::Testament,
            timeout_ms: 30_000,
        }),
        &context(ISSUER),
        &mut reads,
        &mut lists,
        &mut pause,
    )
    .unwrap();
    let NativeReadOutcome::Wait(result) = outcome else {
        panic!()
    };
    assert_eq!(result.condition, ClaimWaitCondition::Met);
    assert_eq!(result.probes, 2);
    assert_eq!(
        result.observation.status,
        ClaimStatus::TestamentAcknowledged
    );
    assert_eq!(result.observation.revision, ObjectRevision(4));
    assert_eq!(pauses, vec![std::time::Duration::from_secs(1)]);
    // A terminal claim can no longer deliver a testament: Unmet at once.
    let mut reads = |request: NativeReadRequest| {
        let NativeReadQuery::Claim { id, .. } = request.query else {
            panic!()
        };
        Ok(synthetic_claim(
            id,
            ClaimStatus::Cancelled,
            5,
            false,
            root.clone(),
            7,
        ))
    };
    let mut pause = |_: std::time::Duration| panic!("no pause after a settled predicate");
    let NativeReadOutcome::Wait(result) = crate::read(
        &NativeReadOperation::ClaimWait(NativeWaitDocument {
            claim: hex_of(claim.0),
            until: NativeWaitUntil::Testament,
            timeout_ms: 1_000,
        }),
        &context(ISSUER),
        &mut reads,
        &mut lists,
        &mut pause,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(result.condition, ClaimWaitCondition::Unmet);
    assert_eq!(result.probes, 1);
    // An observation that moves backwards is an invalid response.
    let mut probes = 0u64;
    let mut reads = |request: NativeReadRequest| {
        let NativeReadQuery::Claim { id, .. } = request.query else {
            panic!()
        };
        probes += 1;
        Ok(match probes {
            1 => synthetic_claim(id, ClaimStatus::Received, 3, false, root.clone(), 9),
            _ => synthetic_claim(id, ClaimStatus::Received, 2, false, root.clone(), 8),
        })
    };
    let mut pause = |_: std::time::Duration| Ok(());
    let error = crate::read(
        &NativeReadOperation::ClaimWait(NativeWaitDocument {
            claim: hex_of(claim.0),
            until: NativeWaitUntil::Satisfied,
            timeout_ms: 30_000,
        }),
        &context(ISSUER),
        &mut reads,
        &mut lists,
        &mut pause,
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            crate::DriveError::Client(focal_client::ClientError::InvalidResponse)
        ),
        "{error:?}"
    );

    // Lineage: the claim, its ancestor, then a correction and a child, all
    // at least at the first read's token.
    let parent = ClaimId::from_u128(11);
    let correction = ClaimId::from_u128(12);
    let child = ClaimId::from_u128(13);
    let mut tokens = Vec::new();
    let mut reads = |request: NativeReadRequest| {
        let NativeReadQuery::Claim { id, .. } = request.query else {
            panic!()
        };
        tokens.push(request.consistency);
        Ok(match id {
            id if id == claim => synthetic_claim(
                id,
                ClaimStatus::Satisfied,
                2,
                false,
                Cause::Claim(parent),
                20,
            ),
            id if id == parent => {
                synthetic_claim(id, ClaimStatus::Satisfied, 2, false, root.clone(), 20)
            }
            id => synthetic_claim(id, ClaimStatus::Posted, 1, false, Cause::Claim(claim), 20),
        })
    };
    let mut kinds = Vec::new();
    let mut lists = |request: NativeListRequest| {
        let NativeListFilter::Claims {
            relation: Some(relation),
            ..
        } = &request.filter
        else {
            panic!("{request:?}");
        };
        kinds.push(relation.kind);
        let page = match relation.kind {
            RelationKind::Invalidates => {
                synthetic_claim(correction, ClaimStatus::Posted, 1, false, root.clone(), 20)
            }
            RelationKind::CausedBy => synthetic_claim(
                child,
                ClaimStatus::Posted,
                1,
                false,
                Cause::Claim(claim),
                20,
            ),
            _ => synthetic_claim(claim, ClaimStatus::Posted, 1, false, root.clone(), 20),
        };
        Ok(NativeListPage {
            token: page.token,
            native_sequence: page.native_sequence,
            objects: if relation.kind == RelationKind::Refines {
                Vec::new()
            } else {
                page.objects
            },
            next: None,
            visited: 1,
        })
    };
    let mut pause = |_: std::time::Duration| panic!("a lineage read never pauses");
    let NativeReadOutcome::Page(page) = crate::read(
        &NativeReadOperation::ClaimLineage(NativeObjectDocument {
            id: hex_of(claim.0),
        }),
        &context(ISSUER),
        &mut reads,
        &mut lists,
        &mut pause,
    )
    .unwrap() else {
        panic!()
    };
    let ids: Vec<_> = page
        .objects
        .iter()
        .map(|object| match object {
            NativeObject::Claim(claim) => ClaimId(claim.binding.object.0),
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![claim, parent, correction, child]);
    assert_eq!(page.token.sequence, SessionSeq(20));
    assert_eq!(page.visited, 7);
    assert_eq!(
        kinds,
        vec![
            RelationKind::Invalidates,
            RelationKind::Refines,
            RelationKind::CausedBy
        ]
    );
    assert_eq!(tokens[0], ReadConsistency::Linearizable);
    assert!(tokens[1..].iter().all(|consistency| matches!(consistency, ReadConsistency::AtLeast(token) if token.sequence == SessionSeq(20))));
}
