//! Exact retained object identities, immutable authoring and lifecycle chains.
use super::super::read_validate_attempts::AttemptCursor;
use super::super::read_validate_evidence::HistoryIndex;
use super::*;
use focal_model::lifecycle::scope::RegistrySnapshotSource;
use focal_model::lifecycle::{
    graph::{Kind, Obligation},
    validation::Target,
};
use focal_model::{Cause, ObjectKind, ObjectRevision, RelationTarget};

fn pinned(expected: Binding, actual: Binding) -> Result<(), NativeError> {
    if expected.ledger != actual.ledger
        || expected.object != actual.object
        || expected.content != actual.content
        || expected.revision.0 == 0
        || expected.revision > actual.revision
    {
        return Err(invalid());
    }
    Ok(())
}
fn model(error: NativeError) -> ContractError {
    match error {
        NativeError::Contract(error) => error,
        NativeError::Memory(_) | NativeError::Capacity(_) => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}
pub(super) fn claim(
    id: ClaimId,
    owned: &OwnedClaim,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
    counts: &mut Counts,
) -> Result<(), NativeError> {
    let claim = owned.claim().ok_or_else(invalid)?;
    let binding = claim.binding();
    if binding.object.0 != id.0
        || binding.ledger != read.ledger
        || claim.created().0 == 0
        || claim.created() > read.prefix
        || claim.local_sealed_at().is_some_and(|at| at > read.prefix)
    {
        return Err(invalid());
    }
    if let Some(receipt) = claim.receipt() {
        let Row::Receipt(actual) = read.require(Key::Receipt(receipt.fence.receipt))? else {
            return Err(invalid());
        };
        if actual.claim != id || actual.fence != receipt.fence || actual.holder != receipt.holder {
            return Err(invalid());
        }
        counts.receipt_epochs = sum(
            counts.receipt_epochs,
            usize::try_from(receipt.fence.epoch).map_err(|_| ContractError::Capacity)?,
        )?;
    }
    super::read_validate_index::require_claim(id, claim, read)?;
    let declarations = claim.acceptance().declarations();
    counts.declared_definitions = sum(counts.declared_definitions, declarations.len())?;
    read.charge(sum(declarations.len(), 1)?)?;
    for record in declarations {
        let definition = read.definition(ValidationId(record.binding().object.0))?;
        read.charge(const { (usize::BITS as usize + 1) * 16 })?;
        claim.acceptance().check_declaration(definition)?;
    }
    let obligations = claim.graph().obligations();
    read.charge(sum(obligations.len(), 1)?)?;
    for obligation in obligations {
        read.claim(obligation.target)?;
        if !matches!(
            read.require(Key::IncomingLink(obligation.target, id))?,
            Row::IncomingLink(_)
        ) {
            return Err(invalid());
        }
        read.charge(const { (usize::BITS as usize + 1) * 4 })?;
        if obligation.kind != Kind::Awaits
            || obligations
                .binary_search(&Obligation {
                    kind: Kind::DependsOn,
                    target: obligation.target,
                })
                .is_err()
        {
            increment(&mut counts.declared_links)?;
        }
    }
    match *claim.lineage().cause() {
        Cause::Claim(parent) => {
            read.claim(parent)?;
            let event = history.child(id, read)?.ok_or_else(invalid)?;
            let NativeFact::Claim(recorded) = event.fact else {
                return Err(invalid());
            };
            let child = recorded.owned_child.ok_or_else(invalid)?;
            pinned(child, binding)?;
            if recorded.after.object.0 != parent.0 || event.sequence != claim.created() {
                return Err(invalid());
            }
        }
        Cause::Root(id) if id.is_zero() => return Err(invalid()),
        Cause::Root(_) => (),
    }
    read.charge(sum(claim.lineage().corrections().len(), 1)?)?;
    for correction in claim.lineage().corrections() {
        if correction.predecessor.ledger != read.ledger
            || correction.predecessor.kind != ObjectKind::Claim
        {
            return Err(invalid());
        }
        read.claim(ClaimId(correction.predecessor.id.0))?;
    }
    let mut scopes = claim.scopes().iter();
    loop {
        read.charge(1)?;
        let Some(scope) = scopes.next() else {
            break;
        };
        increment(&mut counts.scope_monitors)?;
        let Row::Monitor(allocation) = read.require(Key::Monitor(scope.id()))? else {
            return Err(invalid());
        };
        if allocation.owner.object.0 != id.0
            || allocation.registered != scope.registered()
            || scope.registered() > read.prefix
        {
            return Err(invalid());
        }
        let roots = scope.roots();
        read.charge(sum(roots.len(), 1)?)?;
        for (index, root) in roots.iter().enumerate() {
            let target = links::target(*root);
            read.claim(target)?;
            if scope.active() {
                let Row::MonitorLink(Some(link)) =
                    read.require(Key::MonitorLink(target, scope.id()))?
                else {
                    return Err(invalid());
                };
                if link.owner != id || link.registered != scope.registered() {
                    return Err(invalid());
                }
                read.charge(const { (usize::BITS as usize + 1) * 12 })?;
                let prior = roots.get(..index).ok_or_else(invalid)?;
                if [
                    WaitPredicate::Satisfied(target),
                    WaitPredicate::Terminal(target),
                    WaitPredicate::Released(target),
                ]
                .iter()
                .all(|value| prior.binary_search(value).is_err())
                {
                    increment(&mut counts.active_roots)?;
                }
            }
        }
    }
    read.charge(sum(claim.scopes().children().len(), 1)?)?;
    for child in claim.scopes().children() {
        let actual = read.claim(child.id())?;
        pinned(child.binding(), actual.binding())?;
        if actual.lineage().cause() != &Cause::Claim(id)
            || child.registered() != actual.created()
            || child.registered() > read.prefix
        {
            return Err(invalid());
        }
        let event = history.child(child.id(), read)?.ok_or_else(invalid)?;
        let NativeFact::Claim(recorded) = event.fact else {
            return Err(invalid());
        };
        if recorded.kind != NativeEventKind::ChildRegistered
            || recorded.after.object.0 != id.0
            || recorded.owned_child != Some(child.binding())
            || event.sequence != child.registered()
        {
            return Err(invalid());
        }
    }
    let registrations = owned.registrations().ok_or_else(invalid)?;
    counts.registrations = sum(counts.registrations, registrations.rows().len())?;
    if registrations.rows().len() > read.limits.evaluations_per_claim {
        return Err(ContractError::Capacity.into());
    }
    read.charge(sum(registrations.rows().len(), 1)?)?;
    for registration in registrations.rows() {
        let key = EvaluationKey {
            claim: id,
            validation: ValidationId(registration.binding().object.0),
            target: EvaluationTarget::of(registration.target()),
            generation: registration.generation(),
        };
        let Row::Evaluation(value) = read.require(Key::Evaluation(key))? else {
            return Err(invalid());
        };
        let value = value.get().ok_or_else(invalid)?;
        pinned(registration.binding(), value.binding())?;
        if registration.target() != value.target() || registration.receipt() != value.receipt() {
            return Err(invalid());
        }
        super::read_validate_index::require_evaluation(key, value, read)?;
    }
    let mut last_registration = None;
    history.registrations(id, read, |event| {
        read.charge(64)?;
        let NativeFact::Registrations { claim: recorded } = event.fact else {
            return Err(invalid());
        };
        pinned(recorded, binding)?;
        if event.sequence < claim.created() {
            return Err(invalid());
        }
        last_registration = Some(recorded);
        Ok(())
    })?;
    let registration_header = registrations.snapshot_v1();
    if (registration_header.sealed || registration_header.increments_sealed)
        && last_registration.is_none()
    {
        return Err(invalid());
    }
    let (mut last, mut status, mut position, mut children, mut monitors, mut local_seal) =
        (None, None, None, 0usize, 0usize, None);
    let (mut terminal_at, mut owner_released) = (None, None);
    let mut imported_open = false;
    history.events(Key::Claim(id), read, |event| {
        read.charge(128)?;
        let NativeFact::Claim(recorded) = event.fact else {
            return Err(invalid());
        };
        match last {
            None => {
                let opening = match recorded.kind {
                    NativeEventKind::Created => true,
                    NativeEventKind::Imported(_) => event.invocation == NativeInvocation::Import,
                    _ => false,
                };
                imported_open = matches!(recorded.kind, NativeEventKind::Imported(_));
                if recorded.before.is_some()
                    || !opening
                    || recorded.after.revision != ObjectRevision(1)
                    || event.sequence != claim.created()
                {
                    return Err(invalid());
                }
            }
            Some(binding) => {
                if recorded.before != Some(binding) || recorded.after != binding.next()? {
                    return Err(invalid());
                }
            }
        }
        let current = (event.sequence, event.ordinal);
        if position.is_some_and(|prior| prior >= current) {
            return Err(invalid());
        }
        match recorded.kind {
            NativeEventKind::ChildRegistered => {
                let child = recorded.owned_child.ok_or_else(invalid)?;
                pinned(child, read.claim(ClaimId(child.object.0))?.binding())?;
                increment(&mut children)?;
            }
            NativeEventKind::Monitor(monitor) => {
                read.charge(const { (usize::BITS as usize + 1) * 4 })?;
                let actual = claim.scopes().monitor(monitor.id()).ok_or_else(invalid)?;
                if monitor.cut().position != event.sequence {
                    return Err(invalid());
                }
                if matches!(monitor, NativeMonitorEvent::Registered { .. }) {
                    if actual.registered() != event.sequence {
                        return Err(invalid());
                    }
                    increment(&mut monitors)?;
                }
            }
            NativeEventKind::LocallyComplete => {
                if local_seal.replace(event.sequence).is_some() {
                    return Err(invalid());
                }
            }
            NativeEventKind::OwnerReleased => {
                if owner_released.replace(event.sequence).is_some() {
                    return Err(invalid());
                }
            }
            NativeEventKind::Imported(legacy) => {
                if event.invocation != NativeInvocation::Import || legacy.0 == 0 {
                    return Err(invalid());
                }
            }
            _ => (),
        }
        if recorded.status.is_terminal() && terminal_at.is_none() {
            terminal_at = Some(event.sequence);
            if local_seal.is_none() {
                local_seal = Some(event.sequence);
            }
        }
        last = Some(recorded.after);
        status = Some(recorded.status);
        position = Some(current);
        Ok(())
    })?;
    read.charge(1)?;
    let scopes = claim.scopes().snapshot_v1().fields();
    if last != Some(binding)
        || status != Some(claim.status())
        || children != claim.scopes().children().len()
        || monitors != scopes.scopes
        || local_seal != claim.local_sealed_at()
        || owner_released != claim.scopes().release_cut().map(|cut| cut.position)
        || scopes.last_cut > read.prefix
    {
        return Err(invalid());
    }
    // Origin is proven by the chain, the empty policy and the import position.
    let legacy = claim.origin() == focal_model::lifecycle::claim::ClaimOrigin::Legacy;
    if legacy != imported_open
        || legacy
            && (claim.acceptance().slot_count() != 0
                || !claim.acceptance().declarations().is_empty()
                || claim.created() != SessionSeq(1))
    {
        return Err(invalid());
    }
    terminal(claim, terminal_at, history, read)?;
    if read.profile == NativeContentProfile::AuthoredV1 {
        let Row::ClaimContent(content) = read.require(Key::ClaimContent(id))? else {
            return Err(invalid());
        };
        let body = content.get().ok_or_else(invalid)?;
        read.meter.budget(|visits| {
            authored::check_recorded_claim(
                body,
                content.profile().ok_or(ContractError::InvalidManifest)?,
                claim,
                visits,
            )
            .map_err(model)
        })?;
    }
    admission::validate(owned, read)?;
    super::super::read_validate_evidence::validate_claim(owned, read)?;
    Ok(())
}

pub(super) fn definition(
    id: ValidationId,
    value: &OwnedDeclaration,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let declaration = value.get().ok_or_else(invalid)?;
    if declaration.binding().object.0 != id.0 || declaration.binding().ledger != read.ledger {
        return Err(invalid());
    }
    let claim = read.claim(declaration.claim())?;
    read.charge(const { (usize::BITS as usize + 1) * 16 })?;
    claim.acceptance().check_declaration(declaration)?;
    super::read_validate_index::require_definition(declaration, read)?;
    let mut found = false;
    history.events(Key::Definition(id), read, |event| {
        read.charge(64)?;
        let NativeFact::Definition {
            binding,
            claim: owner,
            index,
            intent,
        } = event.fact
        else {
            return Err(invalid());
        };
        if found
            || binding != declaration.binding()
            || owner != declaration.claim()
            || index != declaration.declaration_index()
            || intent != declaration.intent_fingerprint()
            || event.sequence != claim.created()
        {
            return Err(invalid());
        }
        found = true;
        Ok(())
    })?;
    if !found {
        return Err(invalid());
    }
    match (read.profile, value.descriptor()) {
        (NativeContentProfile::ProjectionOnly, None) => (),
        (NativeContentProfile::AuthoredV1, Some(descriptor)) => {
            let Row::ClaimContent(body) = read.require(Key::ClaimContent(declaration.claim()))?
            else {
                return Err(invalid());
            };
            let body = body.get().ok_or_else(invalid)?;
            read.meter.budget(|visits| {
                authored::check_recorded_declaration(body, claim, descriptor, visits).map_err(model)
            })?;
            if !matches!(read.require(Key::DefinitionIdentity(descriptor.schema(), descriptor.content_hash()))?, Row::DefinitionIdentity(actual) if *actual == id)
            {
                return Err(invalid());
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

/// Legacy rows exist only in an imported ledger, name an imported claim and
/// keep their frozen bytes within the configured bound (23 §5.2). Their bodies
/// are decoded with the frozen legacy codec and never reinterpreted.
pub(super) fn legacy(
    key: Key,
    row: &Row,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    read.charge(64)?;
    let bytes = match (key, row) {
        (Key::LegacyTestament(id), Row::LegacyTestament(value)) if !id.is_zero() => value.bytes(),
        (Key::LegacyEvidenceSet(id), Row::LegacyEvidenceSet(value)) if !id.is_zero() => {
            value.bytes()
        }
        (Key::LegacyRun(id, _), Row::LegacyRun(value)) if !id.is_zero() => value.bytes(),
        (Key::LegacyDefinition(id), Row::LegacyDefinition(value)) if !id.is_zero() => value.bytes(),
        _ => return Err(invalid()),
    };
    if bytes.is_empty() || bytes.len() > read.limits.legacy_row_bytes {
        return Err(invalid());
    }
    read.charge(bytes.len())?;
    let Row::Outcome(outcome) = read.require(Key::Outcome(NativeInvocation::Import))? else {
        return Err(invalid());
    };
    if outcome.operation != NativeOperation::Import || outcome.ledger != read.ledger {
        return Err(invalid());
    }
    let claim = match key {
        Key::LegacyTestament(_) => {
            let testament: focal_model::Testament =
                focal_model::durable_v1::decode(bytes).map_err(|_| invalid())?;
            let content = testament.content();
            if content.ledger != read.ledger {
                return Err(invalid());
            }
            content.claim
        }
        Key::LegacyEvidenceSet(id) => {
            let set: focal_model::EvidenceSet =
                focal_model::durable_v1::decode(bytes).map_err(|_| invalid())?;
            if set.id != id {
                return Err(invalid());
            }
            set.claim
        }
        Key::LegacyRun(id, _) => {
            let run: focal_model::ValidationRun =
                focal_model::durable_v1::decode(bytes).map_err(|_| invalid())?;
            if run.id.validation != id
                || !matches!(
                    read.require(Key::LegacyDefinition(id))?,
                    Row::LegacyDefinition(_)
                )
            {
                return Err(invalid());
            }
            run.claim
        }
        Key::LegacyDefinition(_) => {
            let validation: focal_model::Validation =
                focal_model::durable_v1::decode(bytes).map_err(|_| invalid())?;
            let content = validation.content();
            if content.ledger != read.ledger {
                return Err(invalid());
            }
            content.claim
        }
        _ => return Err(invalid()),
    };
    read.claim(claim)?;
    Ok(())
}

pub(super) fn authored(
    key: Key,
    row: &Row,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    if read.profile != NativeContentProfile::AuthoredV1 {
        return Err(invalid());
    }
    match (key, row) {
        (Key::ClaimContent(id), Row::ClaimContent(value)) => {
            let body = value.get().ok_or_else(invalid)?;
            if body.id() != id || body.ledger() != read.ledger {
                return Err(invalid());
            }
            pinned(body.binding(), read.claim(id)?.binding())?;
            if !matches!(read.require(Key::ClaimIdentity(body.schema(), body.content_hash()))?, Row::ClaimIdentity(actual) if *actual == id)
            {
                return Err(invalid());
            }
            read.charge(sum(body.requirements().len(), 1)?)?;
            for requirement in body.requirements() {
                read.definition(requirement.id)?;
            }
            read.charge(sum(body.relations().len(), 1)?)?;
            for relation in body.relations() {
                if let RelationTarget::Object(target) = relation.target {
                    if target.ledger != read.ledger || target.kind != ObjectKind::Claim {
                        return Err(invalid());
                    }
                    read.claim(ClaimId(target.id.0))?;
                }
            }
        }
        (Key::ClaimIdentity(schema, hash), Row::ClaimIdentity(id)) => {
            let Row::ClaimContent(value) = read.require(Key::ClaimContent(*id))? else {
                return Err(invalid());
            };
            let body = value.get().ok_or_else(invalid)?;
            if body.schema() != schema || body.content_hash() != hash {
                return Err(invalid());
            }
        }
        (Key::DefinitionIdentity(schema, hash), Row::DefinitionIdentity(id)) => {
            let Row::Definition(value) = read.require(Key::Definition(*id))? else {
                return Err(invalid());
            };
            let body = value.descriptor().ok_or_else(invalid)?;
            if body.schema() != schema || body.content_hash() != hash {
                return Err(invalid());
            }
        }
        (Key::CreationResult(invocation), Row::CreationResult(value)) => {
            let Row::Outcome(outcome) = read.require(Key::Outcome(invocation))? else {
                return Err(invalid());
            };
            if outcome.operation != NativeOperation::Create {
                return Err(invalid());
            }
            read.charge(sum(value.get().entries().len(), 1)?)?;
            // A created claim that retired (26 §4) left its identity with
            // the family; its continuation vouches for the entry, and the
            // definitions created beside it left with their claim.
            let mut retired = false;
            for object in value.get().entries() {
                if object.family == NativeCreatedFamily::Claim
                    && matches!(
                        read.get(Key::Retired(ClaimId(object.resolved.0)))?,
                        Some(Row::Retired(_))
                    )
                {
                    retired = true;
                }
            }
            for object in value.get().entries() {
                let resolved = match object.family {
                    NativeCreatedFamily::Claim => {
                        match read.get(Key::ClaimIdentity(object.schema, object.content))? {
                            Some(Row::ClaimIdentity(id)) => id.0,
                            None if matches!(
                                read.get(Key::Retired(ClaimId(object.resolved.0)))?,
                                Some(Row::Retired(_))
                            ) =>
                            {
                                object.resolved.0
                            }
                            _ => return Err(invalid()),
                        }
                    }
                    NativeCreatedFamily::Validation => {
                        match read.get(Key::DefinitionIdentity(object.schema, object.content))? {
                            Some(Row::DefinitionIdentity(id)) => id.0,
                            None if retired
                                && read
                                    .get(Key::Definition(ValidationId(object.resolved.0)))?
                                    .is_none() =>
                            {
                                object.resolved.0
                            }
                            _ => return Err(invalid()),
                        }
                    }
                };
                if resolved != object.resolved.0
                    || outcome.created != 0 && object.requested != object.resolved
                {
                    return Err(invalid());
                }
            }
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

pub(super) fn evaluation(
    key: EvaluationKey,
    owned: &OwnedEvaluation,
    read: &ValidationRead<'_, '_>,
    history: &HistoryIndex,
) -> Result<(), NativeError> {
    let value = owned.get().ok_or_else(invalid)?;
    let declaration = read.definition(key.validation)?;
    if EvaluationKey::of(key.claim, value) != key
        || declaration.claim() != key.claim
        || value.binding().ledger != read.ledger
    {
        return Err(invalid());
    }
    let claim = read.claim(key.claim)?;
    target(key, value, read)?;
    let mut cursor = AttemptCursor::new(declaration);
    let (mut last, mut state, mut phase, mut position, mut last_result) =
        (None, None, None, None, None);
    history.events(Key::Evaluation(key), read, |event| {
        read.charge(128)?;
        let NativeFact::Evaluation {
            kind,
            key: recorded,
            before,
            after,
            state: next,
            phase: step,
            attempt,
            fence,
        } = event.fact
        else {
            return Err(invalid());
        };
        if recorded != key
            || event.sequence < claim.created()
            || (kind == NativeEvaluationEventKind::Sealed
                && claim.local_sealed_at() != Some(event.sequence))
        {
            return Err(invalid());
        }
        if last.is_some()
            && before != last
            && matches!(key.target, EvaluationTarget::Delivery { .. })
        {
            delivery(
                key,
                read,
                &mut last,
                &mut state,
                &mut phase,
                &mut position,
                &mut last_result,
                &mut cursor,
            )?;
        }
        match last {
            None => {
                if before.is_some()
                    || after != declaration.binding()
                    || kind != NativeEvaluationEventKind::Materialized
                {
                    return Err(invalid());
                }
            }
            Some(previous) => {
                if before != Some(previous) || after != previous.next()? {
                    return Err(invalid());
                }
            }
        }
        let at = (event.sequence, event.ordinal);
        if position.is_some_and(|prior| prior >= at) {
            return Err(invalid());
        }
        let mut accepted = None;
        if kind == NativeEvaluationEventKind::Reported
            || kind == NativeEvaluationEventKind::MissingTarget
        {
            let result_key = NativeResultKey {
                evaluation: key,
                revision: after.revision,
            };
            let result = result(read, result_key)?;
            if let Some(result) = result {
                if result.binding() != after || result.resulting_state() != next {
                    return Err(invalid());
                }
                last_result = Some(result);
                accepted = Some(result);
            } else if kind == NativeEvaluationEventKind::Reported {
                return Err(invalid());
            }
        }
        cursor.event(kind, attempt, next, step, fence, accepted, read)?;
        last = Some(after);
        state = Some(next);
        if step != validation::Phase::MissingTarget {
            phase = Some(step);
        }
        position = Some(at);
        Ok(())
    })?;
    if last != Some(value.binding()) && matches!(key.target, EvaluationTarget::Delivery { .. }) {
        delivery(
            key,
            read,
            &mut last,
            &mut state,
            &mut phase,
            &mut position,
            &mut last_result,
            &mut cursor,
        )?;
    }
    if last != Some(value.binding())
        || state != Some(value.state())
        || phase != Some(value.phase())
        || last_result != value.last_result()
    {
        return Err(invalid());
    }
    cursor.finish(value, read)?;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
fn delivery(
    key: EvaluationKey,
    read: &ValidationRead<'_, '_>,
    binding: &mut Option<Binding>,
    state: &mut Option<validation::State>,
    phase: &mut Option<validation::Phase>,
    position: &mut Option<(SessionSeq, u32)>,
    last_result: &mut Option<validation::AcceptedResult>,
    cursor: &mut AttemptCursor<'_>,
) -> Result<(), NativeError> {
    read.charge(128)?;
    let previous = binding.ok_or_else(invalid)?;
    let next = previous.next()?;
    let key = NativeResultKey {
        evaluation: key,
        revision: next.revision,
    };
    let Row::DeliveryResult(value) = read.require(Key::DeliveryResult(key))? else {
        return Err(invalid());
    };
    let value = value.get().ok_or_else(invalid)?;
    let result = value.result();
    let event = read.event(value.sequence(), value.ordinal())?;
    let at = (event.sequence, event.ordinal);
    if *state != Some(validation::State::Ready)
        || *phase != Some(validation::Phase::Delivery)
        || event.fact != (NativeFact::Delivery { key })
        || result.binding() != next
        || position.is_none_or(|prior| prior >= at)
        || last_result.is_some()
    {
        return Err(invalid());
    }
    cursor.delivery(result, read)?;
    *binding = Some(next);
    *state = Some(result.resulting_state());
    *phase = Some(validation::Phase::Delivery);
    *position = Some(at);
    *last_result = Some(result);
    Ok(())
}
fn result(
    read: &ValidationRead<'_, '_>,
    key: NativeResultKey,
) -> Result<Option<validation::AcceptedResult>, NativeError> {
    let mut found = None;
    for key in [
        Key::Accepted(key),
        Key::DeliveryResult(key),
        Key::MissingResult(key),
    ] {
        let result = match read.get(key)? {
            Some(Row::Accepted(value)) => Some(value.get().ok_or_else(invalid)?.result()),
            Some(Row::DeliveryResult(value)) => Some(value.get().ok_or_else(invalid)?.result()),
            Some(Row::MissingResult(value)) => Some(value.get().ok_or_else(invalid)?.result()),
            None => None,
            _ => return Err(invalid()),
        };
        if let Some(result) = result
            && found.replace(result).is_some()
        {
            return Err(invalid());
        }
    }
    Ok(found)
}
fn target(
    key: EvaluationKey,
    value: &validation::EvaluationState,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    match value.target() {
        Target::Admission { claim } => {
            if claim.object.0 != key.claim.0 {
                return Err(invalid());
            }
            pinned(claim, read.claim(key.claim)?.binding())?;
        }
        Target::Increment { claim, artifact } => {
            if claim.object.0 != key.claim.0 {
                return Err(invalid());
            }
            pinned(claim, read.claim(key.claim)?.binding())?;
            pinned(
                artifact,
                read.artifact(ArtifactId(artifact.object.0))?
                    .descriptor()
                    .binding(),
            )?;
        }
        Target::Artifact {
            response,
            artifact,
            slot,
        } => {
            let Row::Response(actual) =
                read.require(Key::Response(TestamentId(response.object.0)))?
            else {
                return Err(invalid());
            };
            let actual = actual.get().ok_or_else(invalid)?;
            pinned(response, actual.identity().binding)?;
            let Row::Work(work) = read.require(Key::Work(ArtifactId(artifact.object.0)))? else {
                return Err(invalid());
            };
            let work = work.get().ok_or_else(invalid)?;
            pinned(artifact, work.state.binding())?;
            if actual.identity().claim != key.claim
                || work.state.claim() != key.claim
                || work.state.slot() != slot
                || work.state.attachment() != Some(TestamentId(response.object.0))
                || value.receipt() != Some(actual.identity().receipt)
            {
                return Err(invalid());
            }
        }
        Target::MissingSlot { response, .. } | Target::Delivery { response } => {
            let Row::Response(actual) =
                read.require(Key::Response(TestamentId(response.object.0)))?
            else {
                return Err(invalid());
            };
            let actual = actual.get().ok_or_else(invalid)?;
            pinned(response, actual.identity().binding)?;
            if actual.identity().claim != key.claim
                || value.receipt() != Some(actual.identity().receipt)
            {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

fn terminal(
    claim: &ClaimState,
    observed: Option<SessionSeq>,
    history: &HistoryIndex,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    use focal_model::lifecycle::claim::ClaimTerminalCut;
    let Some(cut) = claim.terminal_cut() else {
        return if observed.is_none() {
            Ok(())
        } else {
            Err(invalid())
        };
    };
    let sequence = match cut {
        ClaimTerminalCut::Explicit(cut) => cut.position,
        ClaimTerminalCut::Required(cut) => cut.sequence(),
        ClaimTerminalCut::Graph(cut) => cut.sequence(),
    };
    if observed != Some(sequence) || sequence > read.prefix {
        return Err(invalid());
    }
    match cut {
        ClaimTerminalCut::Explicit(_) => (),
        ClaimTerminalCut::Required(cut) => blocking(claim, cut.snapshot_v1(), read)?,
        ClaimTerminalCut::Graph(cut) => {
            let value = cut.snapshot_v1();
            let origin = read.claim(ClaimId(value.origin.binding.object.0))?;
            pinned(value.origin.binding, origin.binding())?;
            if value.origin.created != origin.created() {
                return Err(invalid());
            }
            let binding = history
                .revision(
                    Key::Claim(ClaimId(value.origin.binding.object.0)),
                    value.origin.binding.revision.0,
                    read,
                )?
                .ok_or_else(invalid)?;
            let NativeFact::Claim(binding) = binding.fact else {
                return Err(invalid());
            };
            if binding.after != value.origin.binding {
                return Err(invalid());
            }
            let event = history
                .at_or_before(
                    Key::Claim(ClaimId(value.origin.binding.object.0)),
                    value.origin.terminal,
                    read,
                )?
                .ok_or_else(invalid)?;
            let NativeFact::Claim(terminal) = event.fact else {
                return Err(invalid());
            };
            if !terminal.status.is_terminal()
                || terminal.status == ClaimStatus::Satisfied
                || value.kind == focal_model::lifecycle::graph::FailureKind::Deadlocked
                    && terminal.status != ClaimStatus::Deadlocked
            {
                return Err(invalid());
            }
            if let (Some(deadline), Some(fired)) = (value.deadline, value.fired_at) {
                let event = history
                    .at_or_before(
                        Key::Claim(ClaimId(claim.binding().object.0)),
                        sequence,
                        read,
                    )?
                    .ok_or_else(invalid)?;
                let actual = match event.invocation {
                    NativeInvocation::ClaimDeadline(key) => {
                        if key.timer != deadline.timer || key.generation != deadline.generation {
                            return Err(invalid());
                        }
                        read.claim(key.claim)?.deadline().ok_or_else(invalid)?
                    }
                    NativeInvocation::MonitorDeadline(key) => {
                        if key.timer != deadline.timer || key.generation != deadline.generation {
                            return Err(invalid());
                        }
                        let Row::Monitor(monitor) = read.require(Key::Monitor(key.monitor))? else {
                            return Err(invalid());
                        };
                        if monitor.owner.object.0 != key.claim.0 {
                            return Err(invalid());
                        }
                        monitor.deadline
                    }
                    _ => return Err(invalid()),
                };
                let Row::Outcome(outcome) = read.require(Key::Outcome(event.invocation))? else {
                    return Err(invalid());
                };
                if actual != deadline || outcome.logical_time != fired || fired < deadline.at {
                    return Err(invalid());
                }
            }
        }
    }
    Ok(())
}

fn blocking(
    claim: &ClaimState,
    cut: focal_model::lifecycle::aggregation::TerminalCutSnapshotV1,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    use focal_model::lifecycle::aggregation::{BlockingKind, CausePhase, CauseTarget};
    let cause = cut.cause;
    if cause.key.phase == CausePhase::MissingTarget {
        let CauseTarget::Response(id) = cause.key.target else {
            return Err(invalid());
        };
        let slot = cause.slot.ok_or_else(invalid)?;
        let Row::Response(response) = read.require(Key::Response(id))? else {
            return Err(invalid());
        };
        let record = response.record().ok_or_else(invalid)?;
        let response = record.response();
        if response.identity().claim.0 != claim.binding().object.0
            || record
                .entered()
                .is_none_or(|position| position.sequence > cut.sequence)
        {
            return Err(invalid());
        }
        read.charge(const { (usize::BITS as usize + 1) * 8 })?;
        if response
            .manifest()
            .binary_search_by_key(&slot, |value| value.slot)
            .is_ok()
        {
            return Err(invalid());
        }
        let mut slots = claim.acceptance().slots();
        let mut found = false;
        loop {
            read.charge(1)?;
            let Some(policy) = slots.next() else {
                break;
            };
            if policy.slot == slot {
                if policy.mode != cause.slot_mode {
                    return Err(invalid());
                }
                if policy.missing_declaration_index != cause.key.declaration_index {
                    // A declared MissingSlot check has its own immutable index.
                    // Its result is distinct from the synthetic slot-presence
                    // cause, which is available even when no checks exist.
                    read.charge(sum(policy.checks.len(), 1)?)?;
                    let check = policy
                        .checks
                        .iter()
                        .find(|check| check.declaration_index == cause.key.declaration_index)
                        .ok_or_else(invalid)?;
                    if check.mode != cause.mode {
                        return Err(invalid());
                    }
                    missing_result(claim, response, cut, read)?;
                }
                found = true;
            }
        }
        return if found { Ok(()) } else { Err(invalid()) };
    }
    read.charge(sum(claim.acceptance().declarations().len(), 1)?)?;
    let declaration = claim
        .acceptance()
        .declarations()
        .iter()
        .find(|value| value.index() == cause.key.declaration_index)
        .ok_or_else(invalid)?;
    let validation = ValidationId(declaration.binding().object.0);
    let target = match cause.key.target {
        CauseTarget::Admission => EvaluationTarget::Admission,
        CauseTarget::Increment { artifact, .. } => EvaluationTarget::Increment { artifact },
        CauseTarget::Response(response) => EvaluationTarget::Work {
            response,
            slot: cause.slot.ok_or_else(invalid)?,
            artifact: cause.artifact.ok_or_else(invalid)?.id,
        },
    };
    let key = EvaluationKey {
        claim: ClaimId(claim.binding().object.0),
        validation,
        target,
        generation: cause.key.generation.ok_or_else(invalid)?,
    };
    let start = Key::Accepted(NativeResultKey {
        evaluation: key,
        revision: ObjectRevision(0),
    });
    read.charge(const { (usize::BITS as usize + 1) * 64 })?;
    let mut results = read.root.entries_from(&start, false);
    let mut found = false;
    loop {
        read.charge(128)?;
        let Some(entry) = results.next() else {
            break;
        };
        let Key::Accepted(actual_key) = entry.key else {
            break;
        };
        if actual_key.evaluation != key {
            break;
        }
        let Row::Accepted(actual) = &entry.value else {
            return Err(invalid());
        };
        let actual = actual.get().ok_or_else(invalid)?;
        let result = actual.result();
        let phase = match cause.key.phase {
            CausePhase::Programmatic => validation::Phase::Programmatic,
            CausePhase::Quality => validation::Phase::Quality,
            _ => return Err(invalid()),
        };
        if result.attempt() != cause.key.attempt || result.phase() != phase {
            continue;
        }
        let kind = match result.verdict() {
            focal_model::VerdictValue::Fail => BlockingKind::Failed,
            focal_model::VerdictValue::Incomplete => BlockingKind::Incomplete,
            focal_model::VerdictValue::Error => BlockingKind::Errored,
            focal_model::VerdictValue::Pass => return Err(invalid()),
        };
        if found
            || kind != cause.kind
            || result.mode() != cause.mode
            || result.evidence() != cause.evidence
            || actual.sequence() > cut.sequence
        {
            return Err(invalid());
        }
        found = true;
    }
    if !found {
        return Err(invalid());
    }
    Ok(())
}

/// The terminal cause omits a fabricated worker generation for MissingTarget,
/// but the native evaluation still belongs to the response's recorded cycle.
/// Resolve that exact evaluation/result instead of treating every missing cause
/// as the separate zero-check slot-presence obligation.
fn missing_result(
    claim: &ClaimState,
    response: &focal_model::lifecycle::evidence::Response,
    cut: focal_model::lifecycle::aggregation::TerminalCutSnapshotV1,
    read: &ValidationRead<'_, '_>,
) -> Result<(), NativeError> {
    use focal_model::lifecycle::aggregation::BlockingKind;
    let cause = cut.cause;
    read.charge(sum(claim.acceptance().declarations().len(), 1)?)?;
    let declaration = claim
        .acceptance()
        .declarations()
        .iter()
        .find(|declaration| declaration.index() == cause.key.declaration_index)
        .ok_or_else(invalid)?;
    let key = EvaluationKey {
        claim: ClaimId(claim.binding().object.0),
        validation: ValidationId(declaration.binding().object.0),
        target: EvaluationTarget::MissingSlot {
            response: TestamentId(response.identity().binding.object.0),
            slot: cause.slot.ok_or_else(invalid)?,
        },
        generation: u64::from(response.identity().cycle),
    };
    let Row::Evaluation(evaluation) = read.require(Key::Evaluation(key))? else {
        return Err(invalid());
    };
    let evaluation = evaluation.get().ok_or_else(invalid)?;
    let result = evaluation.last_result().ok_or_else(invalid)?;
    let result_key = NativeResultKey::of(result);
    let Row::MissingResult(actual) = read.require(Key::MissingResult(result_key))? else {
        return Err(invalid());
    };
    let actual = actual.get().ok_or_else(invalid)?;
    if result_key.evaluation != key
        || actual.result() != result
        || actual.sequence() > cut.sequence
        || result.declaration_index() != cause.key.declaration_index
        || result.mode() != cause.mode
        || result.phase() != validation::Phase::MissingTarget
        || result.verdict() != focal_model::VerdictValue::Incomplete
        || result.resulting_state() != validation::State::ValidationIncomplete
        || result.attempt() != cause.key.attempt
        || result.evidence() != cause.evidence
        || cause.kind != BlockingKind::Incomplete
        || cause.artifact.is_some()
        || cause.key.generation.is_some()
    {
        return Err(invalid());
    }
    Ok(())
}
