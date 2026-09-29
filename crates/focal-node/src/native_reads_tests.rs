//! The native read page over a populated owner, served exactly as the host
//! serves it (the audit's F07 and F08): an evaluation page in key order whose
//! continuation is the last key it consumed, so pages of any size concatenate
//! to the whole span at one prefix; the owner's selection of the current
//! evaluation over the declaration's whole span, past any page; a claim
//! expansion that continues where it filled; and the responses list that
//! reaches the end of a long chain a row per page. The client's driver runs
//! against the same page: `validation.get` follows the pages to the end and
//! `validation.begin` binds the evaluation the owner selected.
use crate::native_lists;
use crate::native_reads::{Reader, check_consistency, page};
use focal_client::ClientError;
use focal_client::input::{BuildContext, InputError};
use focal_client::operations::{
    NativeAuthoredOperation, NativeContextDocument, NativeObjectDocument, NativeReadOperation,
    parse_native_json,
};
use focal_core::Core;
use focal_core::native::{
    EvaluationKey, EvaluationTarget, NativeCommand, NativeContentProfile, NativeContext,
    NativeLimits, NativeOutcome, NativeOwner, NativeStaging, NativeState,
    event_record::evaluation_key_of,
    input_codec::{DecodeWork, NativeDecodeLimits},
};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{MemoryBudget, RangeId};
use focal_model::lifecycle::Principal;
use focal_model::*;
use focal_native_client::{
    CompileError, CompileLimits, Compiled, DriveError, NativeReadOutcome, compile, encode_frame,
    fingerprint, read, resolve,
};
use focal_wire::*;
use serde_json::{Value, json};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);
/// Work artifacts per response cycle and response cycles: 16 × 17 = 272
/// increment evaluations of one declaration, more than the client's page of
/// 256, so the current ones lie past the first page in key order.
const SLOTS: u32 = 16;
const CYCLES: u32 = 17;
const FAR: u64 = 4_102_444_800_000;
const LIST_KEY: [u8; 32] = [7; 32];

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(11),
        session: SessionId::from_u128(12),
    }
}
fn build(actor: ParticipantId) -> BuildContext {
    BuildContext {
        ledger: ledger(),
        actor,
        root: RootCommandId::from_u128(900),
        policy_revision: 1,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
fn access(error: AccessError) -> DriveError {
    DriveError::Client(ClientError::Access(error))
}
fn keys(page: &NativeReadPage) -> Vec<EvaluationKey> {
    page.objects
        .iter()
        .map(|object| match object {
            NativeObject::Evaluation(evaluation) => evaluation_key_of(evaluation.key),
            other => panic!("an evaluation page holds evaluations only: {other:?}"),
        })
        .collect()
}

/// One in-process authored owner behind the node's own read page: frames are
/// admitted as the node admits them and reads are served as the host serves
/// them, after the wire validation and the consistency check.
struct Harness {
    owner: NativeOwner,
    store: ContentStore,
    _root: tempfile::TempDir,
    time: u64,
    requests: u128,
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
            MemoryBudget::new(1024 << 20, 256 << 20).unwrap(),
        )
        .unwrap();
        Self {
            owner: NativeOwner::new(core).unwrap(),
            store,
            _root: root,
            time: 100,
            requests: 0,
        }
    }
    fn core(&self) -> &Core<NativeState> {
        self.owner.committed_core()
    }
    fn reader(&self) -> Reader<'_> {
        Reader {
            core: self.core(),
            ledger: ledger(),
            profile: NativeProfile::AuthoredV1,
            principal: ISSUER,
            role: NativePeerRole::Actor,
            route: RouteEpoch(1),
        }
    }
    fn token(&self) -> ReadToken {
        ReadToken {
            ledger: ledger(),
            sequence: self.core().native_sequence(),
            route_epoch: RouteEpoch(1),
        }
    }
    /// The node's read page, exactly as the host serves it.
    fn page(&self, request: &NativeReadRequest) -> Result<NativeReadPage, AccessError> {
        request.validate(&WireLimits::default())?;
        check_consistency(self.core(), ledger(), &request.consistency)?;
        page(&self.reader(), request)
    }
    /// The node's list page, exactly as the host serves it.
    fn list(&self, request: &NativeListRequest) -> Result<NativeListPage, AccessError> {
        request.validate(&WireLimits::default())?;
        native_lists::serve(&self.reader(), &LIST_KEY, request)
    }
    /// The client's driver over this owner's pages.
    fn read(&self, operation: &NativeReadOperation) -> Result<NativeReadPage, DriveError> {
        let mut reads = |request: NativeReadRequest| self.page(&request).map_err(access);
        let mut lists = |request: NativeListRequest| self.list(&request).map_err(access);
        let mut pause = |_: std::time::Duration| Ok(());
        match read(
            operation,
            &build(ISSUER),
            &mut reads,
            &mut lists,
            &mut pause,
        )? {
            NativeReadOutcome::Page(page) => Ok(page),
            NativeReadOutcome::Wait(wait) => panic!("a plain read: {wait:?}"),
        }
    }
    fn resolve(
        &self,
        operation: &NativeAuthoredOperation,
    ) -> Result<focal_native_client::Resolved, DriveError> {
        let mut reads = |request: NativeReadRequest| self.page(&request).map_err(access);
        resolve(ledger(), operation, &mut reads)
    }
    /// Resolve through the node's page, compile, encode and admit the frame
    /// as the node does.
    fn attempt(
        &mut self,
        actor: ParticipantId,
        operation: &NativeAuthoredOperation,
    ) -> Result<(Compiled, NativeOutcome), DriveError> {
        self.requests += 1;
        let request = self.requests;
        let resolved = self.resolve(operation)?;
        let limits = CompileLimits::default();
        let compiled = compile(
            operation,
            &build(actor),
            RequestId::from_u128(request),
            &mut ids(request * 1000),
            &resolved,
            &limits,
        )?;
        let frame = encode_frame(
            ledger(),
            NativeContentProfile::AuthoredV1,
            &compiled.input,
            limits.encoding(),
        )?;
        let expected = fingerprint(&frame, limits.native, limits.frame)?;
        let outcome = self.publish(operation.name(), actor, &frame);
        assert_eq!(outcome.intent, expected);
        Ok((compiled, outcome))
    }
    fn run(
        &mut self,
        actor: ParticipantId,
        operation: &NativeAuthoredOperation,
    ) -> (Compiled, NativeOutcome) {
        self.attempt(actor, operation)
            .unwrap_or_else(|error| panic!("{}: {error}", operation.name()))
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
    /// The declaration's evaluations in key order, as the core scans them.
    fn span(&self, claim: ClaimId, validation: ValidationId) -> Vec<EvaluationKey> {
        self.core()
            .native_declaration_evaluations_from(claim, validation, None)
            .collect()
    }
    /// What the owner must select: of the declaration's evaluations the
    /// selector names, at the named generation when there is one, live when
    /// asked, the ones at the highest generation.
    fn selection(&self, query: &NativeSelectionQuery) -> Vec<EvaluationKey> {
        let view = self.owner.committed();
        let mut best = Vec::new();
        let mut generation = None;
        for key in self.span(query.claim, query.validation) {
            if !query
                .selector
                .selects(focal_core::native::event_record::evaluation_target(
                    key.target,
                ))
                || query
                    .generation
                    .is_some_and(|wanted| key.generation != wanted)
            {
                continue;
            }
            let Some(state) = view.evaluation(key) else {
                continue;
            };
            if query.live && state.state().is_terminal() {
                continue;
            }
            match generation {
                Some(seen) if key.generation < seen => continue,
                Some(seen) if key.generation == seen => {}
                _ => {
                    generation = Some(key.generation);
                    best.clear();
                }
            }
            best.push(key);
        }
        best
    }
}

/// A received claim with an increment declaration (the issuer its evaluator)
/// and a whole-work declaration on slot zero, after `CYCLES` response cycles
/// of `SLOTS` work artifacts each: every artifact registered one increment
/// evaluation, so the increment declaration spans `SLOTS * CYCLES`
/// evaluations, the latest cycle's at the end of the key order.
struct Populated {
    h: Harness,
    claim: ClaimId,
    increment: ValidationId,
    artifacts: Vec<Vec<ArtifactId>>,
}
fn populated() -> Populated {
    let mut h = Harness::new();
    let handler = |id: u128| json!({"id": format!("{id:032x}"), "version": format!("{id:064x}")});
    let slots: Vec<Value> = (0..SLOTS)
        .map(|slot| {
            if slot == 0 {
                json!({"slot": slot, "checks": [{"declaration": 2}]})
            } else {
                json!({"slot": slot})
            }
        })
        .collect();
    let document = json!({
        "description": "Produce the report in every slot; each increment is checked.",
        "target": hex(&SUBJECT.0),
        "max_responses": CYCLES,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": FAR}},
            {"kind": "test", "description": "Each increment passes.", "target": {"type": "increment"},
             "phase": "increment", "evaluator": "self", "handlers": [handler(72)], "deadline": {"at": FAR}},
            {"kind": "test", "description": "The report passes.",
             "target": {"type": "slot", "index": 0, "name": "report"}, "phase": "whole_work",
             "evaluator": "self", "handlers": [handler(73)], "deadline": {"at": FAR}}
        ],
        "slots": slots
    });
    let (compiled, _) = h.run(ISSUER, &parse("claim.submit", document));
    let claim = ClaimId(compiled.created[0].id);
    let increment = ValidationId(compiled.created[2].id);
    let claim_hex = hex(&claim.0);
    h.run(ISSUER, &parse("claim.post", json!({"claim": claim_hex})));
    h.run(
        SUBJECT,
        &parse("receipt.acquire", json!({"claim": claim_hex})),
    );
    let mut artifacts = Vec::new();
    for cycle in 0..CYCLES {
        let mut produced = Vec::new();
        for slot in 0..SLOTS {
            let text = format!(
                r#"{{"passed":{},"failed":0,"skipped":0}}"#,
                cycle * SLOTS + slot + 1
            );
            let (compiled, outcome) = h.run(
                SUBJECT,
                &parse(
                    "artifact.submit",
                    json!({"claim": claim_hex, "slot": slot, "payload": {"type": "text", "text": text}}),
                ),
            );
            assert_eq!(
                outcome.evaluations, 1,
                "one increment evaluation per artifact"
            );
            produced.push(ArtifactId(compiled.created[0].id));
        }
        let manifest: Vec<Value> = produced
            .iter()
            .enumerate()
            .map(|(slot, artifact)| {
                let hash = h
                    .owner
                    .committed()
                    .artifact(*artifact)
                    .unwrap()
                    .descriptor()
                    .content_hash();
                json!({"slot": slot, "artifact": {"id": hex(&artifact.0), "hash": hex(&hash.0)}})
            })
            .collect();
        let (compiled, _) = h.run(
            SUBJECT,
            &parse(
                "testament.submit",
                json!({"claim": claim_hex, "summary": format!("Cycle {cycle} complete."),
                       "confidence": "committed", "outcome": "complete", "manifest": manifest}),
            ),
        );
        let response = hex(&compiled.created[0].id);
        h.run(
            SUBJECT,
            &parse(
                "testament.post",
                json!({"claim": claim_hex, "testament": response}),
            ),
        );
        h.run(
            ISSUER,
            &parse(
                "testament.receive",
                json!({"claim": claim_hex, "testament": response}),
            ),
        );
        artifacts.push(produced);
    }
    Populated {
        h,
        claim,
        increment,
        artifacts,
    }
}

#[test]
fn evaluation_pages_of_every_size_concatenate_to_the_whole_span_at_one_prefix() {
    let p = populated();
    let h = &p.h;
    let whole = h.span(p.claim, p.increment);
    assert_eq!(whole.len(), (SLOTS * CYCLES) as usize);
    assert!(whole.windows(2).all(|pair| pair[0] < pair[1]));
    let token = h.token();
    let limit = WireLimits::default().max_items;
    let request = |after: Option<NativeEvaluationKey>, max_items: u32| NativeReadRequest {
        consistency: if after.is_none() {
            ReadConsistency::AtLeast(token)
        } else {
            ReadConsistency::Exact(token)
        },
        query: NativeReadQuery::Evaluations {
            claim: p.claim,
            validation: p.increment,
            after,
        },
        max_items,
    };
    // Pages of every size, exact boundaries included, concatenate to the
    // whole span without a skip or a repeat; the continuation is the last
    // key the page consumed, and a page that holds the last row is final.
    for size in [1u32, 2, 3, 5, 17, 64, 255, 256, 257, 271, 272, 273, limit] {
        if size > limit {
            continue;
        }
        let mut after = None;
        let mut seen = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = h.page(&request(after, size)).unwrap();
            pages += 1;
            assert!(page.objects.len() <= size as usize, "size {size}");
            assert_eq!(page.token, token);
            let consumed = keys(&page);
            let last = consumed.last().copied();
            seen.extend(consumed);
            match page.next {
                Some(NativeContinuation::Evaluations(next)) => {
                    assert_eq!(
                        Some(evaluation_key_of(next)),
                        last,
                        "the continuation is the last key the page consumed (size {size})"
                    );
                    after = Some(next);
                }
                Some(other) => panic!("size {size}: {other:?}"),
                None => break,
            }
            assert!(pages <= whole.len(), "size {size}: the pages must end");
        }
        assert_eq!(seen, whole, "size {size}");
        assert_eq!(pages, whole.len().div_ceil(size as usize), "size {size}");
    }
    // A cursor no row has still positions the scan: before the first key it
    // yields the span, after the last it yields nothing, and one strictly
    // between two keys yields what follows it.
    let wire = focal_core::native::event_record::evaluation_key;
    let before = EvaluationKey {
        generation: 0,
        ..whole[0]
    };
    let page = h.page(&request(Some(wire(before)), limit)).unwrap();
    assert_eq!(keys(&page), whole);
    let page = h
        .page(&request(Some(wire(*whole.last().unwrap())), limit))
        .unwrap();
    assert!(page.objects.is_empty() && page.next.is_none());
    let between = EvaluationKey {
        generation: whole[3].generation + 1,
        ..whole[3]
    };
    assert!(!whole.contains(&between));
    let page = h.page(&request(Some(wire(between)), limit)).unwrap();
    assert_eq!(keys(&page), whole[4..]);
    // A continuation is a position in one exact prefix and in this
    // declaration: a resumed page that is not exact, a cursor of another
    // claim or declaration, and a declaration under another claim are refused.
    assert!(matches!(
        h.page(&NativeReadRequest {
            consistency: ReadConsistency::AtLeast(token),
            ..request(Some(wire(whole[0])), limit)
        }),
        Err(AccessError::InvalidRequest)
    ));
    for foreign in [
        EvaluationKey {
            claim: ClaimId::from_u128(77),
            ..whole[0]
        },
        EvaluationKey {
            validation: ValidationId::from_u128(78),
            ..whole[0]
        },
    ] {
        assert!(matches!(
            h.page(&request(Some(wire(foreign)), limit)),
            Err(AccessError::InvalidRequest)
        ));
    }
    assert!(matches!(
        h.page(&NativeReadRequest {
            query: NativeReadQuery::Evaluations {
                claim: ClaimId::from_u128(77),
                validation: p.increment,
                after: None,
            },
            ..request(None, limit)
        }),
        Err(AccessError::InvalidRequest)
    ));
    // A declaration the owner never had answers with its absence, final.
    let missing = ValidationId::from_u128(4242);
    let page = h
        .page(&NativeReadRequest {
            query: NativeReadQuery::Evaluations {
                claim: p.claim,
                validation: missing,
                after: None,
            },
            ..request(None, limit)
        })
        .unwrap();
    assert_eq!(
        page.objects,
        vec![NativeObject::Missing(NativeObjectRef::Definition(missing))]
    );
    assert!(page.next.is_none());
}

#[test]
fn the_owner_selects_the_current_evaluation_over_the_whole_span_and_the_client_binds_it() {
    let mut p = populated();
    let whole = p.h.span(p.claim, p.increment);
    let latest = p.artifacts.last().unwrap();
    let target = latest[7];
    let limit = WireLimits::default().max_items;
    let query = |selector, generation, live| NativeSelectionQuery {
        claim: p.claim,
        validation: p.increment,
        selector,
        generation,
        live,
    };
    let select = |h: &Harness, query: NativeSelectionQuery, max_items| {
        h.page(&NativeReadRequest {
            consistency: ReadConsistency::AtLeast(h.token()),
            query: NativeReadQuery::SelectEvaluation(query),
            max_items,
        })
    };
    // The current increment evaluation of one artifact is unique, at the
    // latest cycle's generation, and lies past the first page of 256.
    let one = query(
        NativeEvaluationSelector::Increment {
            artifact: Some(target),
        },
        None,
        true,
    );
    let page = select(&p.h, one, limit).unwrap();
    let expected = p.h.selection(&one);
    assert_eq!(keys(&page), expected);
    assert_eq!(expected.len(), 1, "{expected:?}");
    let current = expected[0];
    assert_eq!(current.generation, u64::from(CYCLES));
    assert_eq!(
        current.target,
        EvaluationTarget::Increment { artifact: target }
    );
    let position = whole.iter().position(|key| *key == current).unwrap();
    assert!(position >= 256, "the current evaluation is item {position}");
    assert!(page.next.is_none());
    // Every current increment: the tie set at the highest generation, whole
    // when it fits and refused, never cut, when it does not.
    let any = query(
        NativeEvaluationSelector::Increment { artifact: None },
        None,
        true,
    );
    let page = select(&p.h, any, limit).unwrap();
    let tie = p.h.selection(&any);
    assert_eq!(keys(&page), tie);
    assert_eq!(tie.len(), SLOTS as usize);
    assert!(tie.iter().all(|key| key.generation == u64::from(CYCLES)));
    assert!(matches!(
        select(&p.h, any, SLOTS - 1),
        Err(AccessError::Capacity)
    ));
    // A named generation selects that generation's evaluations of any state.
    let first = query(
        NativeEvaluationSelector::Increment { artifact: None },
        Some(1),
        false,
    );
    let page = select(&p.h, first, limit).unwrap();
    let cycle_one: Vec<EvaluationKey> = whole
        .iter()
        .copied()
        .filter(|key| key.generation == 1)
        .collect();
    assert_eq!(keys(&page), cycle_one);
    assert_eq!(cycle_one.len(), SLOTS as usize);
    assert_eq!(p.h.selection(&first), cycle_one);
    // Selectors this declaration has no evaluations for select nothing.
    for selector in [
        NativeEvaluationSelector::WholeWork { slot: None },
        NativeEvaluationSelector::WholeWork { slot: Some(0) },
        NativeEvaluationSelector::Admission,
    ] {
        let page = select(&p.h, query(selector, None, true), limit).unwrap();
        assert!(
            page.objects.is_empty() && page.next.is_none(),
            "{selector:?}"
        );
    }
    // A declaration under another claim is refused; one the owner never had
    // answers with its absence.
    assert!(matches!(
        select(
            &p.h,
            NativeSelectionQuery {
                claim: ClaimId::from_u128(77),
                ..one
            },
            limit
        ),
        Err(AccessError::InvalidRequest)
    ));
    let missing = ValidationId::from_u128(4242);
    let page = select(
        &p.h,
        NativeSelectionQuery {
            validation: missing,
            ..one
        },
        limit,
    )
    .unwrap();
    assert_eq!(
        page.objects,
        vec![NativeObject::Missing(NativeObjectRef::Definition(missing))]
    );

    // The client binds what the owner selected: `validation.begin` naming
    // the artifact compiles to the current evaluation past the first page,
    // and without the artifact the tie set is an ambiguity it reports.
    let claim_hex = hex(&p.claim.0);
    let validation_hex = hex(&p.increment.0);
    let begin = parse(
        "validation.begin",
        json!({"claim": claim_hex, "validation": validation_hex, "phase": "increment", "target": hex(&target.0)}),
    );
    let resolved = p.h.resolve(&begin).unwrap();
    let selected = resolved
        .evaluation(
            p.claim,
            p.increment,
            NativeEvaluationSelector::Increment {
                artifact: Some(target),
            },
        )
        .unwrap();
    assert_eq!(selected.key, current);
    let compiled = compile(
        &begin,
        &build(ISSUER),
        RequestId::from_u128(9_000),
        &mut ids(9_000_000),
        &resolved,
        &CompileLimits::default(),
    )
    .unwrap();
    let NativeCommand::BeginIncrement { key, .. } = compiled.input.command else {
        panic!("{:?}", compiled.input.command)
    };
    assert_eq!(key, current);
    let ambiguous = parse(
        "validation.begin",
        json!({"claim": claim_hex, "validation": validation_hex, "phase": "increment"}),
    );
    let resolved = p.h.resolve(&ambiguous).unwrap();
    assert!(matches!(
        resolved.evaluation(
            p.claim,
            p.increment,
            NativeEvaluationSelector::Increment { artifact: None }
        ),
        Err(CompileError::Unsupported(_))
    ));
    assert!(matches!(
        compile(
            &ambiguous,
            &build(ISSUER),
            RequestId::from_u128(9_001),
            &mut ids(9_001_000),
            &resolved,
            &CompileLimits::default(),
        ),
        Err(CompileError::Unsupported(_))
    ));
    // The owner admits the begin the client compiled.
    p.h.run(ISSUER, &begin);
    assert!(
        p.h.owner
            .committed()
            .evaluation(current)
            .unwrap()
            .has_begun()
    );
    // The context read selects the same way: the named artifact's current
    // evaluation, an ambiguity without it, a named generation of any state.
    let context = |target: Option<ArtifactId>, generation| {
        p.h.read(&NativeReadOperation::ValidationContext(
            NativeContextDocument {
                validation: validation_hex.clone(),
                phase: "increment".into(),
                slot: None,
                target: target.map(|artifact| hex(&artifact.0)),
                generation,
                results_after: None,
                limit: 16,
            },
        ))
    };
    let page = context(Some(target), None).unwrap();
    let NativeObject::Context(composed) = &page.objects[0] else {
        panic!("{page:?}")
    };
    assert_eq!(
        evaluation_key_of(composed.evaluation.as_ref().unwrap().key),
        current
    );
    assert!(composed.evaluation.as_ref().unwrap().has_begun);
    assert!(matches!(
        context(None, None),
        Err(DriveError::Compile(CompileError::Unsupported(_)))
    ));
    let early = p.artifacts[0][7];
    let page = context(Some(early), Some(1)).unwrap();
    let NativeObject::Context(composed) = &page.objects[0] else {
        panic!("{page:?}")
    };
    let key = evaluation_key_of(composed.evaluation.as_ref().unwrap().key);
    assert_eq!(
        (key.target, key.generation),
        (EvaluationTarget::Increment { artifact: early }, 1)
    );
}

#[test]
fn validation_get_follows_the_evaluation_pages_to_the_end() {
    let p = populated();
    let h = &p.h;
    let whole = h.span(p.claim, p.increment);
    let mut sent = Vec::new();
    let mut reads = |request: NativeReadRequest| {
        sent.push((
            request.consistency.clone(),
            matches!(request.query, NativeReadQuery::Evaluations { .. }),
        ));
        h.page(&request).map_err(access)
    };
    let mut lists = |request: NativeListRequest| h.list(&request).map_err(access);
    let mut pause = |_: std::time::Duration| Ok(());
    let page = match read(
        &NativeReadOperation::ValidationGet(NativeObjectDocument {
            id: hex(&p.increment.0),
        }),
        &build(ISSUER),
        &mut reads,
        &mut lists,
        &mut pause,
    )
    .unwrap()
    {
        NativeReadOutcome::Page(page) => page,
        NativeReadOutcome::Wait(wait) => panic!("{wait:?}"),
    };
    // The definition, then every evaluation of its span in key order, from
    // three requests: the definition, a first page of 256 at least at its
    // prefix, and the rest at exactly that prefix.
    let (definition, evaluations) = page.objects.split_at(1);
    assert!(
        matches!(definition, [NativeObject::Definition(_)]),
        "{page:?}"
    );
    let seen: Vec<EvaluationKey> = evaluations
        .iter()
        .map(|object| match object {
            NativeObject::Evaluation(evaluation) => evaluation_key_of(evaluation.key),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(seen, whole);
    assert!(page.next.is_none());
    assert!(page.visited >= SLOTS * CYCLES, "{}", page.visited);
    assert_eq!(sent.len(), 3, "{sent:?}");
    assert!(!sent[0].1 && sent[1].1 && sent[2].1, "{sent:?}");
    assert!(matches!(sent[1].0, ReadConsistency::AtLeast(_)));
    assert_eq!(sent[2].0, ReadConsistency::Exact(page.token));
}

#[test]
fn a_claim_expansion_continues_where_its_page_filled_and_never_repeats_the_claim() {
    let p = populated();
    let h = &p.h;
    let token = h.token();
    let limit = WireLimits::default().max_items;
    let expand = NativeClaimExpand {
        content: false,
        scopes: false,
        responses: true,
        evaluations: true,
        history: false,
    };
    let request = |after: Option<NativeContinuation>, max_items: u32| NativeReadRequest {
        consistency: if after.is_none() {
            ReadConsistency::AtLeast(token)
        } else {
            ReadConsistency::Exact(token)
        },
        query: NativeReadQuery::Claim {
            id: p.claim,
            expand,
            after,
        },
        max_items,
    };
    let collect = |size: u32| {
        let mut after = None;
        let mut objects = Vec::new();
        let mut pages = 0usize;
        loop {
            let page = h.page(&request(after, size)).unwrap();
            pages += 1;
            assert!(page.objects.len() <= size as usize, "size {size}");
            if after.is_some() {
                assert!(
                    !page
                        .objects
                        .iter()
                        .any(|object| matches!(object, NativeObject::Claim(_))),
                    "a resumed page never repeats the claim (size {size})"
                );
            }
            objects.extend(page.objects);
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
            assert!(pages <= 4096, "size {size}: the pages must end");
        }
        (objects, pages)
    };
    let (reference, _) = collect(limit);
    // The expansion: the claim, the responses from the latest cycle back,
    // then the evaluations in key order, every one of the claim's.
    assert!(matches!(reference[0], NativeObject::Claim(_)));
    let cycles: Vec<u32> = reference
        .iter()
        .filter_map(|object| match object {
            NativeObject::Response(response) => Some(response.cycle),
            _ => None,
        })
        .collect();
    assert_eq!(cycles, (1..=CYCLES).rev().collect::<Vec<u32>>());
    let evaluations: Vec<EvaluationKey> = reference
        .iter()
        .filter_map(|object| match object {
            NativeObject::Evaluation(evaluation) => Some(evaluation_key_of(evaluation.key)),
            _ => None,
        })
        .collect();
    let all: Vec<EvaluationKey> = h
        .core()
        .native_claim_evaluations_from(p.claim, None)
        .collect();
    assert_eq!(evaluations, all);
    assert!(evaluations.len() > (SLOTS * CYCLES) as usize);
    assert_eq!(reference.len(), 1 + cycles.len() + evaluations.len());
    assert!(reference.len() > 256);
    // Pages of every size concatenate to the same expansion.
    for size in [1u32, 2, 3, 7, 64, 255, 256] {
        let (objects, pages) = collect(size);
        assert_eq!(objects, reference, "size {size}");
        assert!(
            pages >= reference.len().div_ceil(size as usize),
            "size {size}"
        );
    }
    // A continuation of another kind, of another claim, or under a read that
    // is not exact is refused.
    let foreign = focal_core::native::event_record::evaluation_key(EvaluationKey {
        claim: ClaimId::from_u128(77),
        ..all[0]
    });
    for bad in [
        request(Some(NativeContinuation::Results(ObjectRevision(1))), limit),
        request(Some(NativeContinuation::Evaluations(foreign)), limit),
        NativeReadRequest {
            consistency: ReadConsistency::AtLeast(token),
            ..request(Some(NativeContinuation::Responses { cycle: 3 }), limit)
        },
    ] {
        assert!(matches!(h.page(&bad), Err(AccessError::InvalidRequest)));
    }
}

#[test]
fn the_responses_list_reaches_the_end_of_a_long_chain_one_row_per_page() {
    let p = populated();
    let h = &p.h;
    let mut cursor = None;
    let mut cycles = Vec::new();
    let mut pages = 0usize;
    // One row and one visit per page: the rows above a resumed page's
    // cursor are the way to it, not its work, so every page advances.
    loop {
        let page = h
            .list(&NativeListRequest {
                filter: NativeListFilter::Responses { claim: p.claim },
                cursor: cursor.clone(),
                max_items: 1,
                max_visits: 1,
            })
            .unwrap();
        pages += 1;
        for object in &page.objects {
            let NativeObject::Response(response) = object else {
                panic!("{object:?}")
            };
            cycles.push(response.cycle);
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
        assert!(
            pages <= CYCLES as usize + 1,
            "the list stalled after {cycles:?}"
        );
    }
    assert_eq!(cycles, (1..=CYCLES).rev().collect::<Vec<u32>>());
}
