use super::record_codec::recovery;
use super::record_codec::recovery::tests as ckpt;
use super::report_tests as f;
use super::retirement::*;
use super::*;
use focal_evidence::BuiltinNativeSchemas;
use focal_model::lifecycle::{creation::Owner, succession::Lineage};
use focal_model::{Cause, ValidationMode, VerdictValue};

fn id(value: u128) -> ClaimId {
    ClaimId::from_u128(value)
}
fn binding_of(core: &Core<NativeState>, claim: u128) -> Binding {
    core.native_claim(id(claim)).unwrap().binding()
}
fn cancel(core: &mut Core<NativeState>, request: u128, claim: u128, time: u64) {
    let expected = binding_of(core, claim);
    f::publish(
        core,
        time,
        NativeInput {
            request: f::request(f::ISSUER, request),
            command: NativeCommand::Cancel { expected },
        },
    );
}
fn release(core: &mut Core<NativeState>, request: u128, claim: u128, time: u64) {
    let expected = binding_of(core, claim);
    f::publish(
        core,
        time,
        NativeInput {
            request: f::request(f::ISSUER, request),
            command: NativeCommand::ReleaseScope { expected },
        },
    );
}
fn limits() -> record_codec::EncodingLimits {
    record_codec::EncodingLimits {
        bytes: 1 << 20,
        visits: 1 << 24,
        rows: 1 << 16,
    }
}
/// Create, cancel and release one claim: a family of one, three outcomes.
fn finished(core: &mut Core<NativeState>, claim: u128, request: u128, time: u64) {
    f::publish(core, time, f::creation(request, claim, &[], None));
    cancel(core, request + 1, claim, time + 10);
    release(core, request + 2, claim, time + 20);
}
/// The family rooted at `root`, its bundle's digest and length, and the
/// prefix it claims.
fn bundle_of(
    core: &Core<NativeState>,
    root: ClaimId,
) -> (RetirementFamily, ContentHash, u64, SessionSeq) {
    let family = core.retirement_family(root).unwrap();
    let through = core.native_sequence();
    let quote = core
        .archive_family_quote(&family, through, limits())
        .unwrap();
    let mut bytes = vec![0; quote.bytes];
    let hash = core
        .archive_family_into(&family, through, &mut bytes, quote.visits)
        .unwrap();
    (family, hash, bytes.len() as u64, through)
}
fn outcomes_of(core: &Core<NativeState>) -> usize {
    match core.state.rows.get(&Key::Meta) {
        Some(Row::Meta(meta)) => meta.outcomes,
        _ => panic!("meta row"),
    }
}
fn events_of(core: &Core<NativeState>, claim: ClaimId) -> usize {
    core.state
        .rows
        .entries_from(&Key::Event(SessionSeq(0), 0), false)
        .take_while(|entry| matches!(entry.key, Key::Event(..)))
        .filter(|entry| {
            let Row::Event(stored) = &entry.value else {
                return false;
            };
            let event = stored.get().unwrap().expand(core.state.ledger);
            record_codec::event_object(event) == Key::Claim(claim)
        })
        .count()
}

/// A family leaves the core only when every member is terminal and
/// released and nothing outside refers to it (26 §4); its rows go to the
/// bundle and a continuation stays, outcomes stay for exact retries, the
/// checkpoint restores and validates, and the core keeps working.
#[test]
fn a_family_leaves_the_core_only_when_terminal_released_and_unreferenced() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let mut core = f::core();
    f::publish(&mut core, 10, f::creation(1, 1, &[], None));
    assert_eq!(
        core.retirement_family(id(1)).unwrap_err(),
        RetirementRefusal::NotTerminal(id(1))
    );
    assert_eq!(
        core.retirement_family(id(9)).unwrap_err(),
        RetirementRefusal::Unknown(id(9))
    );
    cancel(&mut core, 2, 1, 20);
    assert!(core.native_claim(id(1)).unwrap().is_terminal());
    assert_eq!(
        core.retirement_family(id(1)).unwrap_err(),
        RetirementRefusal::NotReleased(id(1))
    );
    release(&mut core, 3, 1, 30);
    assert!(core.native_claim(id(1)).unwrap().released());
    let family = core.retirement_family(id(1)).unwrap();
    assert_eq!(family.root, id(1));
    assert_eq!(family.members, vec![id(1)]);
    assert!(family.rows() >= 4, "{}", family.rows());
    assert!(events_of(&core, id(1)) > 0);
    // The family's last event is the release; a bundle must claim at least
    // that much. The candidate walk offers the claim from the status index
    // and resumes exactly where its visit bound stopped it.
    assert_eq!(family.through, core.native_sequence());
    let family_events = core
        .state
        .rows
        .entries_from(&Key::Event(SessionSeq(0), 0), false)
        .take_while(|entry| matches!(entry.key, Key::Event(..)))
        .filter(|entry| {
            let Row::Event(stored) = &entry.value else {
                return false;
            };
            let event = stored.get().unwrap().expand(core.state.ledger);
            family.keys.contains(&record_codec::event_object(event))
        })
        .count();
    assert!(family_events > events_of(&core, id(1)));
    assert_eq!(family.events.iter().sum::<u32>() as usize, family_events);
    let walk = core.retirement_candidates(None, 64).unwrap();
    assert_eq!(walk.claims, vec![id(1)]);
    assert_eq!(walk.next, None);
    let short = core.retirement_candidates(None, 0).unwrap();
    assert!(short.claims.is_empty());
    let cursor = short.next.unwrap();
    assert_eq!(cursor.after, None);
    let mut found = Vec::new();
    let mut resume = Some(cursor);
    for _ in 0..16 {
        let Some(cursor) = resume else { break };
        let step = core.retirement_candidates(Some(cursor), 1).unwrap();
        found.extend(step.claims);
        resume = step.next;
    }
    assert_eq!(found, vec![id(1)]);
    assert_eq!(resume, None);
    assert!(
        core.retire_native_family(
            &family,
            ContentHash([7; 32]),
            1,
            SessionSeq(family.through.0 - 1)
        )
        .is_err()
    );
    let through = core.native_sequence();
    let quote = core
        .archive_family_quote(&family, through, limits())
        .unwrap();
    let mut bytes = vec![0; quote.bytes];
    let hash = core
        .archive_family_into(&family, through, &mut bytes, quote.visits)
        .unwrap();
    assert_eq!(hash, quote.hash);
    assert!(bytes.starts_with(b"FCNARCHV"));
    // The same family again quotes the same bundle.
    assert_eq!(
        core.archive_family_quote(&family, through, limits())
            .unwrap()
            .hash,
        hash
    );
    // A zero bundle, a zero prefix or a prefix past publication is refused.
    assert!(
        core.retire_native_family(&family, ContentHash([0; 32]), 1, through)
            .is_err()
    );
    assert!(
        core.retire_native_family(&family, hash, 1, SessionSeq(0))
            .is_err()
    );
    assert!(
        core.retire_native_family(&family, hash, 1, SessionSeq(through.0 + 1))
            .is_err()
    );
    // A zero length is refused too.
    assert!(
        core.retire_native_family(&family, hash, 0, through)
            .is_err()
    );
    // The bundle reads back structurally: header, digest, rows, and every
    // corruption of it is refused.
    let inspection = record_codec::InspectionLimits {
        bytes: 1 << 20,
        visits: 1 << 24,
        rows: 1 << 16,
        row_bytes: 1 << 16,
    };
    let archive = record_codec::StructuralArchive::inspect(&bytes, inspection).unwrap();
    assert_eq!(archive.digest(), hash);
    assert_eq!(archive.header().root, id(1));
    assert_eq!(archive.header().members, vec![id(1)]);
    assert_eq!(archive.header().through, through);
    assert_eq!(archive.header().content, family.content);
    assert_eq!(archive.header().inline, family.inline);
    assert!(family.content.is_empty());
    assert!(family.inline.is_empty());
    assert_eq!(archive.header().count, family.rows());
    assert_eq!(archive.header().ledger, core.state.ledger);
    let counted: usize = archive
        .family_counts()
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum();
    assert_eq!(counted, family.rows());
    for index in (0..bytes.len()).step_by(7).chain([bytes.len() - 1]) {
        let mut flipped = bytes.clone();
        flipped[index] ^= 0x20;
        assert!(
            record_codec::StructuralArchive::inspect(&flipped, inspection).is_err(),
            "byte {index}"
        );
    }
    assert!(
        record_codec::StructuralArchive::inspect(&bytes[..bytes.len() - 1], inspection).is_err()
    );
    let issuer_request = f::request(f::ISSUER, 1);
    let outcomes_before = core.native_stats().entries;
    let rows = core
        .retire_native_family(&family, hash, bytes.len() as u64, through)
        .unwrap();
    assert_eq!(rows, family.rows());
    assert_eq!(core.native_sequence(), SessionSeq(through.0 + 1));
    assert!(core.native_claim(id(1)).is_none());
    assert_eq!(events_of(&core, id(1)), 0);
    let Some(Row::Retired(retired)) = core.state.rows.get(&Key::Retired(id(1))) else {
        panic!("a continuation stays");
    };
    assert_eq!(retired.bundle, hash);
    assert_eq!(retired.through, through);
    assert_eq!(retired.retired_at, SessionSeq(through.0 + 1));
    assert!(retired.status.is_terminal());
    assert_eq!(retired.binding.object.0, id(1).0);
    assert!(matches!(
        core.state
            .rows
            .get(&Key::Outcome(NativeInvocation::Retirement(id(1)))),
        Some(Row::Outcome(outcome)) if outcome.operation == NativeOperation::Retire
            && outcome.sequence == SessionSeq(through.0 + 1)
    ));
    assert!(
        core.state
            .rows
            .get(&Key::Outcome(NativeInvocation::Request(issuer_request)))
            .is_some()
    );
    assert!(core.native_stats().entries < outcomes_before);
    assert_eq!(
        core.retirement_family(id(1)).unwrap_err(),
        RetirementRefusal::Unknown(id(1))
    );
    // The checkpoint restores and validates with the continuation in place.
    let encoded = ckpt::encode(&core);
    let restored = recovery::restore(
        &ckpt::inspect(&encoded),
        RangeId(777),
        ckpt::limits(core.limits),
        ckpt::budget(),
        &store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    ckpt::compare(&core, &restored);
    assert!(matches!(
        restored.state.rows.get(&Key::Retired(id(1))),
        Some(Row::Retired(value)) if value.bundle == hash
    ));
    // The core keeps admitting, and the walk no longer offers the retired
    // claim.
    f::publish(&mut core, 40, f::creation(4, 4, &[], None));
    assert!(core.native_claim(id(4)).is_some());
    assert!(core.native_claim(id(1)).is_none());
    assert!(
        core.retirement_candidates(None, 64)
            .unwrap()
            .claims
            .is_empty()
    );
}

/// A claim owned by a live parent stays; a parent takes every owned claim
/// with it, and all of them must be terminal and released.
#[test]
fn an_owned_child_keeps_its_parent_and_a_parent_takes_its_children() {
    let mut core = f::core();
    core.limits.plan_edges = 65_536;
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for claim in 1..=2u128 {
        let NativeCommand::Create {
            claims: mut next,
            declarations: mut definitions,
        } = f::creation(claim, claim, &[], None).command
        else {
            panic!("create")
        };
        if claim > 1 {
            let row = &mut next[0];
            row.definition.lineage =
                Lineage::new(row.definition.binding, Cause::Claim(id(claim - 1)), &[], 0).unwrap();
            row.owner = Some(Owner {
                expected: f::binding(claim - 1),
                receipt: None,
            });
        }
        claims.append(&mut next);
        declarations.append(&mut definitions);
    }
    f::publish(
        &mut core,
        10,
        NativeInput {
            request: f::request(f::ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    assert_eq!(
        core.native_claim(id(1)).unwrap().scopes().children().len(),
        1
    );
    // Cancelling the parent cancels the tree; each scope is released
    // bottom-up.
    cancel(&mut core, 2, 1, 20);
    assert!(core.native_claim(id(1)).unwrap().is_terminal());
    assert!(core.native_claim(id(2)).unwrap().is_terminal());
    release(&mut core, 3, 2, 30);
    // The child is owned by a live parent: it never leaves alone, and the
    // parent cannot leave while it is not released.
    assert_eq!(
        core.retirement_family(id(2)).unwrap_err(),
        RetirementRefusal::LiveParent(id(1))
    );
    assert_eq!(
        core.retirement_family(id(1)).unwrap_err(),
        RetirementRefusal::NotReleased(id(1))
    );
    release(&mut core, 4, 1, 40);
    let family = core.retirement_family(id(1)).unwrap();
    assert_eq!(family.members, vec![id(1), id(2)]);
    let through = core.native_sequence();
    let quote = core
        .archive_family_quote(&family, through, limits())
        .unwrap();
    let mut bytes = vec![0; quote.bytes];
    let hash = core
        .archive_family_into(&family, through, &mut bytes, quote.visits)
        .unwrap();
    core.retire_native_family(&family, hash, bytes.len() as u64, through)
        .unwrap();
    assert!(core.native_claim(id(1)).is_none());
    assert!(core.native_claim(id(2)).is_none());
    assert!(matches!(
        core.state.rows.get(&Key::Retired(id(2))),
        Some(Row::Retired(_))
    ));
    assert_eq!(events_of(&core, id(2)), 0);
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let encoded = ckpt::encode(&core);
    let restored = recovery::restore(
        &ckpt::inspect(&encoded),
        RangeId(778),
        ckpt::limits(core.limits),
        ckpt::budget(),
        &store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    ckpt::compare(&core, &restored);
}

/// Under the authored profile a family's content identities and creation
/// results are cross-linked: the identities leave with the family, the
/// creation result stays and its retired entries are vouched for by the
/// continuation, so the owner reconstructs over the retired core exactly
/// as the checkpoint validators accept it.
#[test]
fn an_authored_family_leaves_with_its_identities_and_the_owner_reconstructs() {
    use super::authored::tests as a;
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let mut core = a::core();
    core.limits.plan_edges = 65_536;
    a::publish(
        &mut core,
        a::create(1, vec![a::proposal(1, 11), a::proposal(2, 12)]),
    );
    let expected = binding_of(&core, 1);
    a::publish(&mut core, a::input(2, NativeCommand::Cancel { expected }));
    let expected = binding_of(&core, 1);
    a::publish(
        &mut core,
        a::input(3, NativeCommand::ReleaseScope { expected }),
    );
    let family = core.retirement_family(id(1)).unwrap();
    assert_eq!(family.members, vec![id(1)]);
    let identities = |core: &Core<NativeState>, claim: u128| {
        let Some(Row::ClaimContent(content)) = core.state.rows.get(&Key::ClaimContent(id(claim)))
        else {
            return None;
        };
        let content = content.get()?;
        core.state
            .rows
            .get(&Key::ClaimIdentity(
                content.schema(),
                content.content_hash(),
            ))
            .map(|_| ())
    };
    assert!(identities(&core, 1).is_some());
    assert!(
        core.state
            .rows
            .get(&Key::Definition(ValidationId::from_u128(11)))
            .is_some()
    );
    let through = core.native_sequence();
    let quote = core
        .archive_family_quote(&family, through, limits())
        .unwrap();
    let mut bytes = vec![0; quote.bytes];
    let hash = core
        .archive_family_into(&family, through, &mut bytes, quote.visits)
        .unwrap();
    core.retire_native_family(&family, hash, bytes.len() as u64, through)
        .unwrap();
    // The claim, its content, its identity and its definitions left; the
    // sibling of the same creation and the creation result stay.
    assert!(core.native_claim(id(1)).is_none());
    assert!(core.state.rows.get(&Key::ClaimContent(id(1))).is_none());
    assert!(identities(&core, 1).is_none());
    assert!(
        core.state
            .rows
            .get(&Key::Definition(ValidationId::from_u128(11)))
            .is_none()
    );
    assert!(core.native_claim(id(2)).is_some());
    assert!(identities(&core, 2).is_some());
    assert!(
        core.state
            .rows
            .get(&Key::Definition(ValidationId::from_u128(12)))
            .is_some()
    );
    assert!(
        core.state
            .rows
            .get(&Key::CreationResult(NativeInvocation::Request(a::key(1))))
            .is_some()
    );
    // The owner reconstructs over the retired core, as an authority does
    // after applying the record; the checkpoint restores and validates.
    let encoded = ckpt::encode(&core);
    let restored = recovery::restore(
        &ckpt::inspect(&encoded),
        RangeId(778),
        ckpt::limits(core.limits),
        ckpt::budget(),
        &store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    ckpt::compare(&core, &restored);
    let owner =
        NativeOwner::with_record_buffers(restored, &BuiltinNativeSchemas, limits()).unwrap();
    assert!(owner.committed_core().native_retired(id(1)).is_some());
    // The core keeps admitting authored claims.
    a::publish(&mut core, a::create(4, vec![a::proposal(3, 13)]));
    assert!(core.native_claim(id(3)).is_some());
}

fn restore_with(
    core: &Core<NativeState>,
    limits: NativeLimits,
    store: &focal_evidence::ContentStore,
) -> Result<Core<NativeState>, NativeError> {
    let encoded = ckpt::encode(core);
    recovery::restore(
        &ckpt::inspect(&encoded),
        RangeId(779),
        ckpt::limits(limits),
        ckpt::budget(),
        store,
        &BuiltinNativeSchemas,
    )
}

/// One under the outcome bound (three outcomes under a bound of four) the
/// retirement fits: it publishes the fourth outcome at the fourth prefix,
/// the checkpoint restores and the owner rebuilds under the same bound, an
/// exact retry of the retired creation is still answered from its outcome,
/// and a fresh request meets the bound itself.
#[test]
fn a_retirement_that_fits_the_outcome_bound_restores_under_it() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let mut core = f::core();
    core.limits.outcomes = 4;
    finished(&mut core, 1, 1, 10);
    let (family, hash, length, through) = bundle_of(&core, id(1));
    core.retire_native_family(&family, hash, length, through)
        .unwrap();
    assert_eq!(core.native_sequence(), SessionSeq(4));
    assert_eq!(outcomes_of(&core), 4);
    let restored = restore_with(&core, core.limits, &store).unwrap();
    ckpt::compare(&core, &restored);
    let owner =
        NativeOwner::with_record_buffers(restored, &BuiltinNativeSchemas, limits()).unwrap();
    assert!(owner.committed_core().native_retired(id(1)).is_some());
    let core = owner.into_committed_core().unwrap();
    assert!(matches!(
        core.prepare_native(f::context(f::ISSUER, 50), f::creation(1, 1, &[], None), &[]),
        Ok(NativePreparation::Existing {
            committed: true,
            ..
        })
    ));
    assert!(matches!(
        core.prepare_native(f::context(f::ISSUER, 50), f::creation(9, 9, &[], None), &[]),
        Err(NativeError::Capacity("outcomes"))
    ));
}

/// At the outcome bound (three outcomes under a bound of three) the
/// retirement is refused before anything changes: the derivation refuses
/// the family (`OutcomeCapacity`), and a family derived under a wider
/// bound is refused at publication with the same `Capacity` ordinary
/// admission answers, the sequence, the rows and the budget untouched.
#[test]
fn a_retirement_at_the_outcome_bound_is_refused_before_anything_changes() {
    let mut core = f::core();
    core.limits.outcomes = 3;
    finished(&mut core, 1, 1, 10);
    assert_eq!(outcomes_of(&core), 3);
    assert_eq!(core.native_sequence(), SessionSeq(3));
    assert_eq!(
        core.retirement_family(id(1)).unwrap_err(),
        RetirementRefusal::OutcomeCapacity
    );
    core.limits.outcomes = 4;
    let (family, hash, length, through) = bundle_of(&core, id(1));
    core.limits.outcomes = 3;
    let sequence = core.native_sequence();
    let stats = core.native_stats();
    let budget = core.state.budget.stats();
    assert!(matches!(
        core.retire_native_family(&family, hash, length, through),
        Err(NativeError::Capacity("outcomes"))
    ));
    assert_eq!(core.native_sequence(), sequence);
    assert_eq!(core.native_stats(), stats);
    assert_eq!(core.state.budget.stats(), budget);
    assert_eq!(outcomes_of(&core), 3);
    assert!(core.native_claim(id(1)).is_some());
    assert!(core.native_retired(id(1)).is_none());
}

/// Why both checks exist: a retirement allowed under a bound of four makes
/// four outcomes, which a bound of three cannot restore (`Contract(Capacity)`
/// from the checkpoint) nor rebuild an owner over (`Capacity`) — the state
/// the unchecked retirement used to make under the bound of three itself.
#[test]
fn a_retirement_past_the_bound_is_what_both_checks_refuse() {
    let directory = tempfile::tempdir().unwrap();
    let store = ckpt::store(directory.path());
    let mut core = f::core();
    core.limits.outcomes = 4;
    finished(&mut core, 1, 1, 10);
    let (family, hash, length, through) = bundle_of(&core, id(1));
    core.retire_native_family(&family, hash, length, through)
        .unwrap();
    assert_eq!(outcomes_of(&core), 4);
    let mut lowered = core.limits;
    lowered.outcomes = 3;
    assert!(matches!(
        restore_with(&core, lowered, &store),
        Err(NativeError::Contract(ContractError::Capacity))
    ));
    core.limits = lowered;
    let refused = NativeOwner::with_record_buffers(core, &BuiltinNativeSchemas, limits())
        .err()
        .unwrap();
    assert!(
        matches!(
            refused.error,
            NativeOwnerError::Native(NativeError::Capacity(_))
        ),
        "{:?}",
        refused.error
    );
}

/// Three finished families under a bound with two outcomes to spare:
/// exactly two retire, each at the next prefix, and the third is refused
/// at derivation with nothing changed.
#[test]
fn families_retire_while_outcomes_remain_and_no_further() {
    let mut core = f::core();
    core.limits.outcomes = 11;
    for claim in 1..=3u128 {
        finished(&mut core, claim, claim * 10, claim as u64 * 100);
    }
    assert_eq!(outcomes_of(&core), 9);
    for claim in 1..=2u128 {
        let (family, hash, length, through) = bundle_of(&core, id(claim));
        core.retire_native_family(&family, hash, length, through)
            .unwrap();
        assert_eq!(core.native_sequence(), SessionSeq(9 + claim as u64));
        assert!(core.native_retired(id(claim)).is_some());
    }
    assert_eq!(outcomes_of(&core), 11);
    assert_eq!(
        core.retirement_family(id(3)).unwrap_err(),
        RetirementRefusal::OutcomeCapacity
    );
    assert!(core.native_claim(id(3)).is_some());
    assert_eq!(core.native_sequence(), SessionSeq(11));
}

/// The smallest outcome bound under which an owner rebuilds over `core`:
/// the outcomes published plus those promised to its live reports and the
/// one control.
fn smallest_owner_bound(mut core: Core<NativeState>) -> (usize, Core<NativeState>) {
    for bound in outcomes_of(&core).. {
        core.limits.outcomes = bound;
        match NativeOwner::new(core) {
            Ok(owner) => return (bound, owner.into_committed_core().unwrap()),
            Err(refused) => core = refused.core,
        }
    }
    panic!("no bound rebuilds the owner");
}
fn reserved_core() -> Core<NativeState> {
    let mut core = f::running(&[(ValidationMode::Required, false)]);
    finished(&mut core, 2, 20, 40);
    core
}

/// The owner holds the outcomes it promised to live reports and one
/// control back from a retirement, as it holds them back from every fresh
/// candidate: at the smallest bound the owner rebuilds under, the core's
/// own check passes (the bound has room for one more outcome) and the owner
/// refuses (`OutcomesReserved` at the session); retiring through the core
/// regardless leaves a state no owner rebuilds over — the deadlock the
/// unchecked retirement used to make. One above that bound the retirement
/// is allowed, the owner rebuilds, and the promised report and a deadline
/// control are admitted after it.
#[test]
fn a_retirement_never_takes_an_outcome_promised_to_a_live_report() {
    let (bound, mut core) = smallest_owner_bound(reserved_core());
    assert!(bound > outcomes_of(&core) + 1, "reports are promised");
    core.limits.outcomes = bound;
    assert!(core.retirement_family(id(2)).is_ok());
    let owner = NativeOwner::new(core).unwrap();
    assert!(matches!(
        owner.check_retirement(),
        Err(NativeOwnerError::Native(NativeError::Capacity(_)))
    ));
    let mut core = owner.into_committed_core().unwrap();
    let (family, hash, length, through) = bundle_of(&core, id(2));
    core.retire_native_family(&family, hash, length, through)
        .unwrap();
    let refused = NativeOwner::new(core).err().unwrap();
    assert!(
        matches!(
            refused.error,
            NativeOwnerError::Native(NativeError::Capacity(_))
        ),
        "{:?}",
        refused.error
    );
    // One above: allowed, rebuilt, and what was promised is admitted.
    let (_, mut core) = smallest_owner_bound(reserved_core());
    core.limits.outcomes = bound + 1;
    let owner = NativeOwner::new(core).unwrap();
    owner.check_retirement().unwrap();
    let mut core = owner.into_committed_core().unwrap();
    let (family, hash, length, through) = bundle_of(&core, id(2));
    core.retire_native_family(&family, hash, length, through)
        .unwrap();
    let mut owner = NativeOwner::new(core).unwrap();
    let mut custody = f::Custody::new();
    let report = f::report_for(
        owner.committed_core(),
        None,
        30_001,
        1,
        VerdictValue::Pass,
        f::descriptor(f::artifact_spec(30_001, f::EVALUATOR, VerdictValue::Pass)),
    );
    let evidence = f::verified(&mut custody, &report);
    assert!(matches!(
        owner.prepare(f::context(f::EVALUATOR, 100), report, Some(&evidence)),
        Ok(NativeStaging::Prepared { .. })
    ));
    owner.discard_all();
    let view = owner.effective();
    let deadline = view
        .evaluation(f::key(1))
        .unwrap()
        .bind(view.definition(f::key(1).validation).unwrap())
        .unwrap()
        .deadline();
    let input = NativeDeadlineInput {
        evaluation: f::key(1),
        deadline,
    };
    assert!(matches!(
        owner.prepare_evaluation_deadline(input, deadline.at),
        Ok(NativeStaging::Prepared { .. })
    ));
}
