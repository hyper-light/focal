use super::*;
use crate::{ReceiptId, SessionId, TenantId, ValidatorId};
use std::cell::Cell;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}

fn limits() -> Limits {
    Limits {
        kind_bytes: 32,
        metadata_bytes: 128,
        inline_bytes: 128,
        inputs: 8,
        visibility_labels: 8,
        visibility_label_bytes: 32,
        construction_bytes: 16 * 1024,
    }
}

fn inputs() -> [ObjectRef; 2] {
    [20, 30].map(|id| ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Claim,
        id: ObjectId::from_u128(id),
    })
}

fn spec(inputs: &[ObjectRef]) -> ArtifactSpec<'_> {
    ArtifactSpec {
        ledger: ledger(),
        id: ArtifactId::from_u128(3),
        schema: 1,
        kind: "error",
        schema_hash: ContentHash([4; 32]),
        metadata: &[0, 255, 7],
        payload: PayloadSpec::Inline(b"details"),
        producer: ParticipantId::from_u128(5),
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(6),
            epoch: 7,
        }),
        result: None,
        work: None,
        inputs,
        visibility: &["internal", "tenant/one"],
    }
}

fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([17; 32]),
        revision: ObjectRevision(18),
    }
}

fn result() -> ResultProvenance {
    ResultProvenance {
        claim: ClaimId::from_u128(10),
        validation: ValidationId::from_u128(11),
        target: Target::Artifact {
            response: binding(12),
            slot: 13,
            artifact: binding(14),
        },
        generation: 15,
        attempt: Attempt {
            phase: Phase::Quality,
            index: 16,
            handler: ValidatorId::from_u128(19),
            version: ContentHash([20; 32]),
            evaluator: ParticipantId::from_u128(5),
            definition: ContentHash([21; 32]),
        },
        value: VerdictValue::Incomplete,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Mode {
    #[default]
    Valid,
    MissingInput,
    ExtraInput,
    InputError,
    DuplicateInput,
    UnsortedInput,
    ForeignInput,
    ZeroInput,
    MissingLabel,
    ExtraLabel,
    LabelError,
    DuplicateLabel,
    UnsortedLabel,
    Id,
    Producer,
    SchemaHash,
    Metadata,
    Payload,
    Input,
    Label,
    LongLabel,
}

#[derive(Debug, Default)]
struct Controls {
    mode: Cell<Mode>,
    input_count: Cell<Option<usize>>,
    visibility_count: Cell<Option<usize>>,
    mutate_input: Cell<bool>,
    mutate_label: Cell<bool>,
    input_steps: Cell<usize>,
    label_steps: Cell<usize>,
}

#[derive(Debug)]
struct Source<'a> {
    spec: ArtifactSpec<'a>,
    controls: &'a Controls,
}

struct Inputs<'a> {
    rows: &'a [ObjectRef],
    controls: &'a Controls,
    position: usize,
}

impl Iterator for Inputs<'_> {
    type Item = Result<ObjectRef, ContractError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.controls
            .input_steps
            .set(self.controls.input_steps.get() + 1);
        if self.controls.mutate_input.replace(false) {
            self.controls.mode.set(Mode::Input);
        }
        let position = self.position;
        self.position += 1;
        let mode = self.controls.mode.get();
        if mode == Mode::InputError && position == 0 {
            return Some(Err(ContractError::StaleEvaluation));
        }
        if mode == Mode::MissingInput && position == 1 {
            return None;
        }
        if mode == Mode::ExtraInput && position == self.rows.len() {
            return Some(Ok(ObjectRef {
                ledger: ledger(),
                kind: ObjectKind::Claim,
                id: ObjectId::from_u128(40),
            }));
        }
        let index = match (mode, position) {
            (Mode::DuplicateInput, 1) | (Mode::UnsortedInput, 1) => 0,
            (Mode::UnsortedInput, 0) => 1,
            _ => position,
        };
        let mut value = *self.rows.get(index)?;
        if position == 0 {
            match mode {
                Mode::ForeignInput => value.ledger.tenant = TenantId::from_u128(99),
                Mode::ZeroInput => value.id = ObjectId::from_u128(0),
                Mode::Input => value.id = ObjectId::from_u128(21),
                _ => {}
            }
        }
        Some(Ok(value))
    }

    // The seam must validate its explicit count and not rely on this hint.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

struct Visibility<'a> {
    rows: &'a [&'a str],
    controls: &'a Controls,
    position: usize,
}

impl<'a> Iterator for Visibility<'a> {
    type Item = Result<&'a str, ContractError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.controls
            .label_steps
            .set(self.controls.label_steps.get() + 1);
        if self.controls.mutate_label.replace(false) {
            self.controls.mode.set(Mode::Label);
        }
        let position = self.position;
        self.position += 1;
        let mode = self.controls.mode.get();
        if mode == Mode::LabelError && position == 0 {
            return Some(Err(ContractError::StaleReceipt));
        }
        if mode == Mode::MissingLabel && position == 1 {
            return None;
        }
        if mode == Mode::ExtraLabel && position == self.rows.len() {
            return Some(Ok("z-extra"));
        }
        let index = match (mode, position) {
            (Mode::DuplicateLabel, 1) | (Mode::UnsortedLabel, 1) => 0,
            (Mode::UnsortedLabel, 0) => 1,
            _ => position,
        };
        let mut value = *self.rows.get(index)?;
        if position == 0 {
            value = match mode {
                Mode::Label => "altering",
                Mode::LongLabel => "a-label-larger-than-the-configured-thirty-two-byte-limit",
                _ => value,
            };
        }
        Some(Ok(value))
    }
}

impl<'a> ArtifactSource<'a> for Source<'a> {
    type Inputs<'s>
        = Inputs<'a>
    where
        Self: 's;
    type Visibility<'s>
        = Visibility<'a>
    where
        Self: 's;

    fn fields(&self) -> ArtifactFields<'a> {
        let mut fields = self.spec.fields();
        match self.controls.mode.get() {
            Mode::Id => fields.id = ArtifactId::from_u128(99),
            Mode::Producer => fields.producer = ParticipantId::from_u128(99),
            Mode::SchemaHash => fields.schema_hash = ContentHash([99; 32]),
            Mode::Metadata => fields.metadata = &[0, 254, 7],
            Mode::Payload => fields.payload = PayloadSpec::Inline(b"changed"),
            _ => {}
        }
        fields
    }

    fn input_count(&self) -> usize {
        self.controls
            .input_count
            .get()
            .unwrap_or(self.spec.inputs.len())
    }

    fn visibility_count(&self) -> usize {
        self.controls
            .visibility_count
            .get()
            .unwrap_or(self.spec.visibility.len())
    }

    fn inputs(&self) -> Self::Inputs<'_> {
        Inputs {
            rows: self.spec.inputs,
            controls: self.controls,
            position: 0,
        }
    }

    fn visibility(&self) -> Self::Visibility<'_> {
        Visibility {
            rows: self.spec.visibility,
            controls: self.controls,
            position: 0,
        }
    }
}

#[test]
fn repeatable_source_matches_slice_api_for_payloads_and_complete_provenance() {
    let inputs = inputs();
    let roles = [
        WorkRole::Output { slot: 23 },
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
        WorkRole::ReceiptRejection {
            artifact: ArtifactRef {
                id: ArtifactId::from_u128(24),
                hash: ContentHash([25; 32]),
            },
            reason: EvidenceFailure::Metadata,
        },
    ];
    let provenances = [
        (None, None),
        (Some(result()), None),
        (None, Some(roles[0])),
        (None, Some(roles[1])),
        (None, Some(roles[2])),
    ];
    for payload in [
        PayloadSpec::Inline(b"details"),
        PayloadSpec::Content(ContentPointer {
            domain: ContentDomainId::from_u128(26),
            root: ContentHash([27; 32]),
            length: 8192,
            class: ContentClass::Evidence,
        }),
    ] {
        for (result, work) in provenances {
            let spec = ArtifactSpec {
                payload,
                result,
                work: work.map(|role| WorkProvenance {
                    claim: ClaimId::from_u128(10),
                    cycle: 22,
                    role,
                }),
                ..spec(&inputs)
            };
            let controls = Controls::default();
            let source = Source {
                spec,
                controls: &controls,
            };
            let expected = ArtifactDescriptor::prepare(spec, limits()).unwrap();
            let plan = bytes::fail_after(0, || {
                ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX)
            })
            .unwrap();
            assert_eq!(plan.fields(), spec.fields());
            assert_eq!(plan.content_hash(), expected.content_hash());
            assert_eq!(plan.intent_fingerprint(), expected.intent_fingerprint());
            assert_eq!(plan.construction_charge(), expected.construction_charge());
            assert_eq!(
                plan.construction_heap_bytes(),
                expected.construction_heap_bytes()
            );
            assert_eq!(
                plan.construction_heap_allocations(),
                expected.construction_heap_allocations()
            );
            let charge = plan.construction_charge();
            let visits = plan.build_visits();
            let built = plan.build(charge, visits).unwrap();
            assert_eq!(built, expected.build().unwrap());
            assert_eq!(built.retained_bytes().unwrap(), charge);
            assert_ne!(built.inputs().as_ptr(), inputs.as_ptr());
            assert_ne!(built.metadata().as_ptr(), spec.metadata.as_ptr());
            assert_ne!(built.kind().as_ptr(), spec.kind.as_ptr());
            assert_eq!(built.result_provenance(), result);
            assert_eq!(built.work_provenance(), spec.work);
        }
    }
}

#[test]
fn declared_counts_fallible_values_and_canonical_order_are_checked_without_allocation() {
    let inputs = inputs();
    let controls = Controls::default();
    let source = Source {
        spec: spec(&inputs),
        controls: &controls,
    };
    for (mode, error) in [
        (Mode::MissingInput, ContractError::InvalidManifest),
        (Mode::ExtraInput, ContractError::InvalidManifest),
        (Mode::InputError, ContractError::StaleEvaluation),
        (Mode::DuplicateInput, ContractError::InvalidManifest),
        (Mode::UnsortedInput, ContractError::InvalidManifest),
        (Mode::ForeignInput, ContractError::WrongLedger),
        (Mode::ZeroInput, ContractError::InvalidTarget),
        (Mode::MissingLabel, ContractError::InvalidManifest),
        (Mode::ExtraLabel, ContractError::InvalidManifest),
        (Mode::LabelError, ContractError::StaleReceipt),
        (Mode::DuplicateLabel, ContractError::InvalidManifest),
        (Mode::UnsortedLabel, ContractError::InvalidManifest),
        (Mode::LongLabel, ContractError::Capacity),
    ] {
        controls.mode.set(mode);
        let refused = bytes::fail_after(0, || {
            ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX)
        });
        assert_eq!(refused.unwrap_err(), error, "{mode:?}");
    }
    controls.mode.set(Mode::Valid);
    for count in [0, 1, 3] {
        for override_count in [&controls.input_count, &controls.visibility_count] {
            override_count.set(Some(count));
            assert!(matches!(
                bytes::fail_after(0, || ArtifactDescriptor::prepare_source(
                    &source,
                    limits(),
                    usize::MAX
                )),
                Err(ContractError::InvalidManifest)
            ));
            override_count.set(None);
        }
    }
    for override_count in [&controls.input_count, &controls.visibility_count] {
        override_count.set(Some(usize::MAX));
        controls.input_steps.set(0);
        controls.label_steps.set(0);
        assert!(matches!(
            bytes::fail_after(0, || ArtifactDescriptor::prepare_source(
                &source,
                limits(),
                usize::MAX
            )),
            Err(ContractError::Capacity)
        ));
        assert_eq!(controls.input_steps.get(), 0);
        assert_eq!(controls.label_steps.get(), 0);
        override_count.set(None);
    }
}

#[test]
fn exact_inspection_construction_bytes_and_build_visits_are_precharged() {
    let inputs = inputs();
    let controls = Controls::default();
    let source = Source {
        spec: spec(&inputs),
        controls: &controls,
    };
    let quote = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
    let inspection = quote.inspection_visits();
    let build_visits = quote.build_visits();
    let charge = quote.construction_charge();
    let allocations = quote.construction_heap_allocations();
    assert!(inspection > 0);
    assert!(build_visits >= inspection);
    assert_eq!(
        charge,
        size_of::<ArtifactDescriptor>() + quote.construction_heap_bytes()
    );
    let exact_limits = Limits {
        construction_bytes: charge,
        ..limits()
    };
    bytes::fail_after(0, || {
        assert!(ArtifactDescriptor::prepare_source(&source, exact_limits, inspection).is_ok());
        assert!(matches!(
            ArtifactDescriptor::prepare_source(&source, exact_limits, inspection - 1),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            ArtifactDescriptor::prepare_source(
                &source,
                Limits {
                    construction_bytes: charge - 1,
                    ..limits()
                },
                inspection
            ),
            Err(ContractError::Capacity)
        ));
    });
    for (bytes_limit, visits_limit) in [(charge - 1, build_visits), (charge, build_visits - 1)] {
        let plan = ArtifactDescriptor::prepare_source(&source, exact_limits, inspection).unwrap();
        controls.input_steps.set(0);
        controls.label_steps.set(0);
        bytes::fail_after(allocations, || {
            assert!(matches!(
                plan.build(bytes_limit, visits_limit),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(allocations));
        });
        assert_eq!(controls.input_steps.get(), 0);
        assert_eq!(controls.label_steps.get(), 0);
    }
    let plan = ArtifactDescriptor::prepare_source(&source, exact_limits, inspection).unwrap();
    let built = bytes::fail_after(allocations, || {
        let built = plan.build(charge, build_visits).unwrap();
        assert_eq!(bytes::remaining_allocations(), Some(0));
        built
    });
    assert_eq!(built.retained_bytes().unwrap(), charge);
    assert_eq!(built.heap_allocations().unwrap(), allocations);
}

#[test]
fn every_partial_allocation_failure_allows_an_identical_retry() {
    let inputs = inputs();
    let controls = Controls::default();
    let source = Source {
        spec: spec(&inputs),
        controls: &controls,
    };
    let original = ArtifactDescriptor::prepare(source.spec, limits())
        .unwrap()
        .build()
        .unwrap();
    let hash = original.content_hash();
    let intent = original.intent_fingerprint();
    let original_inputs = original.inputs().as_ptr();
    let original_metadata = original.metadata().as_ptr();
    let quote = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
    let charge = quote.construction_charge();
    let allocations = quote.construction_heap_allocations();
    let visits = quote.build_visits();
    assert!(allocations >= 6);
    for after in 0..allocations {
        let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        bytes::fail_after(after, || {
            assert!(
                matches!(plan.build(charge, visits), Err(ContractError::Capacity)),
                "allocation {after}"
            );
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        assert_eq!(original.inputs().as_ptr(), original_inputs);
        assert_eq!(original.metadata().as_ptr(), original_metadata);
        assert_eq!(original.content_hash(), hash);
        assert_eq!(original.intent_fingerprint(), intent);
        let retry = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        assert_eq!(retry.content_hash(), hash);
        assert_eq!(retry.intent_fingerprint(), intent);
        let rebuilt = bytes::fail_after(allocations, || retry.build(charge, visits)).unwrap();
        assert_eq!(rebuilt, original);
        assert_ne!(rebuilt.inputs().as_ptr(), original_inputs);
        assert_ne!(rebuilt.metadata().as_ptr(), original_metadata);
    }
}

#[test]
fn mutations_between_prepare_and_build_cannot_change_the_reserved_artifact() {
    let inputs = inputs();
    let controls = Controls::default();
    let source = Source {
        spec: spec(&inputs),
        controls: &controls,
    };
    let original = ArtifactDescriptor::prepare(source.spec, limits())
        .unwrap()
        .build()
        .unwrap();
    for mode in [
        Mode::Id,
        Mode::Producer,
        Mode::SchemaHash,
        Mode::Metadata,
        Mode::Payload,
        Mode::Input,
        Mode::Label,
    ] {
        controls.mode.set(Mode::Valid);
        let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        controls.mode.set(mode);
        assert!(
            matches!(
                plan.build(charge, visits),
                Err(ContractError::ContentConflict)
            ),
            "{mode:?}"
        );
        controls.mode.set(Mode::Valid);
        let retry = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        assert_eq!(retry.build(charge, visits).unwrap(), original);
    }
    for override_count in [&controls.input_count, &controls.visibility_count] {
        let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        override_count.set(Some(3));
        assert!(plan.build(charge, visits).is_err());
        override_count.set(None);
    }
    for (mode, error) in [
        (Mode::MissingInput, ContractError::InvalidManifest),
        (Mode::ExtraInput, ContractError::InvalidManifest),
        (Mode::InputError, ContractError::StaleEvaluation),
        (Mode::ForeignInput, ContractError::WrongLedger),
        (Mode::ZeroInput, ContractError::InvalidTarget),
        (Mode::DuplicateInput, ContractError::InvalidManifest),
        (Mode::UnsortedInput, ContractError::InvalidManifest),
        (Mode::MissingLabel, ContractError::InvalidManifest),
        (Mode::ExtraLabel, ContractError::InvalidManifest),
        (Mode::LabelError, ContractError::StaleReceipt),
        (Mode::DuplicateLabel, ContractError::InvalidManifest),
        (Mode::UnsortedLabel, ContractError::InvalidManifest),
    ] {
        let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        controls.mode.set(mode);
        assert_eq!(plan.build(charge, visits).unwrap_err(), error, "{mode:?}");
        controls.mode.set(Mode::Valid);
    }
    let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
    let charge = plan.construction_charge();
    let visits = plan.build_visits();
    controls.mode.set(Mode::LongLabel);
    assert!(matches!(
        plan.build(charge, visits),
        Err(ContractError::Capacity)
    ));
}

#[test]
fn a_source_that_changes_values_while_build_is_reading_is_refused() {
    let inputs = inputs();
    let controls = Controls::default();
    let source = Source {
        spec: spec(&inputs),
        controls: &controls,
    };
    for mutation in [&controls.mutate_input, &controls.mutate_label] {
        controls.mode.set(Mode::Valid);
        let plan = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        let hash = plan.content_hash();
        let intent = plan.intent_fingerprint();
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        mutation.set(true);
        assert!(matches!(
            plan.build(charge, visits),
            Err(ContractError::ContentConflict)
        ));
        assert!(!mutation.get());
        controls.mode.set(Mode::Valid);
        let retry = ArtifactDescriptor::prepare_source(&source, limits(), usize::MAX).unwrap();
        let built = retry.build(charge, visits).unwrap();
        assert_eq!(built.content_hash(), hash);
        assert_eq!(built.intent_fingerprint(), intent);
        assert_eq!(built.inputs(), inputs);
        assert_eq!(
            built.visibility().collect::<Vec<_>>(),
            source.spec.visibility
        );
    }
}
