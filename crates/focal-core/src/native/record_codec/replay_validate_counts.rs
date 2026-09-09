use super::*;

#[derive(Default)]
struct Counts {
    added: Meta,
    writes: Meta,
    meta: bool,
    outcome: bool,
}
fn row(meta: &mut Meta, value: &Row) -> Result<(), NativeError> {
    let count = match value {
        Row::Claim(_) => &mut meta.claims,
        Row::Definition(_) => &mut meta.definitions,
        Row::Evaluation(_) => &mut meta.evaluations,
        Row::Artifact(_) => &mut meta.artifacts,
        Row::Accepted(_) | Row::DeliveryResult(_) | Row::MissingResult(_) => &mut meta.results,
        Row::Receipt(_) => &mut meta.receipts,
        Row::Response(_) => &mut meta.responses,
        Row::ResultTestament(_) => &mut meta.result_testaments,
        Row::Monitor(_) => &mut meta.monitors,
        Row::MonitorLink(_) => &mut meta.monitor_links,
        Row::CreationResult(_) => &mut meta.creation_results,
        Row::Outcome(_) => &mut meta.outcomes,
        Row::Event(_) => &mut meta.events,
        _ => return Ok(()),
    };
    *count = add(*count, 1)?;
    Ok(())
}

pub(super) fn immutable(key: Key) -> bool {
    matches!(
        key,
        Key::IncomingLink(..)
            | Key::Monitor(_)
            | Key::Definition(_)
            | Key::Artifact(_)
            | Key::ArtifactIdentity(_)
            | Key::Accepted(_)
            | Key::DeliveryResult(_)
            | Key::MissingResult(_)
            | Key::Receipt(_)
            | Key::RetiredCycle(_)
            | Key::WorkSlot(..)
            | Key::Diagnostic(_)
            | Key::ClaimResultTestament(_)
            | Key::Outcome(_)
            | Key::Event(..)
            | Key::ClaimContent(_)
            | Key::ClaimIdentity(..)
            | Key::DefinitionIdentity(..)
            | Key::CreationResult(_)
            | Key::LegacyTestament(_)
            | Key::LegacyEvidenceSet(_)
            | Key::LegacyRun(..)
            | Key::LegacyDefinition(_)
            | Key::ByIssuer(..)
            | Key::BySubject(..)
            | Key::ByAction(..)
            | Key::ByScope(..)
            | Key::ByRelation(..)
            | Key::ByProducer(..)
            | Key::ByArtifactKind(..)
            | Key::BySchema(..)
            | Key::ArtifactInput(..)
            | Key::ByEvaluator(..)
            | Key::ByVerdict(..)
            | Key::ByCreated(..)
    )
}

pub(super) fn validate<O: Overlay>(read: &ReplayRead<'_, '_, O>) -> Result<(), NativeError> {
    require(
        read.outcome.ledger == read.ledger
            && read.base.0.checked_add(1) == Some(read.outcome.sequence.0),
    )?;
    let previous = match read.before(Key::Meta)? {
        Some(Row::Meta(value)) if read.base.0 != 0 => *value,
        None if read.base.0 == 0 => Meta::default(),
        _ => return Err(invalid()),
    };
    require(
        u64::try_from(previous.outcomes).map_err(|_| ContractError::Capacity)? == read.base.0
            && read.outcome.logical_time >= previous.logical_time,
    )?;
    let mut counts = Counts::default();
    let mut last = None;
    let changes = read.overlay.changes();
    read.charge(mul(add(changes.len(), 1)?, 256)?)?;
    require(changes.len() != 0)?;
    for (key, value) in changes {
        require(key != Key::End && last.is_none_or(|old| old < key))?;
        last = Some(key);
        // All native row families retain allocation/history identities. Live
        // removal of an active monitor writes a tombstone, not a key deletion.
        // The only key deletion any native command has authority over is the
        // status index row a claim transition leaves behind (doc 22 §7); no
        // retained row or derived membership head is ever erased.
        let Some(value) = value else {
            require(matches!(key, Key::ByStatus(..) | Key::DueTimer(..)))?;
            require(matches!(read.before(key)?, Some(Row::Index)))?;
            continue;
        };
        mutation::check_family(key, value)?;
        let old = read.before(key)?;
        if let Some(old) = old {
            mutation::check_family(key, old)?;
            require(!immutable(key))?;
        } else {
            row(&mut counts.added, value)?;
        }
        row(&mut counts.writes, value)?;
        match (key, value) {
            (Key::Meta, Row::Meta(_)) => counts.meta = true,
            (Key::Outcome(invocation), Row::Outcome(value)) => {
                require(invocation == read.outcome.invocation && *value == read.outcome)?;
                counts.outcome = true;
            }
            (Key::Event(sequence, ordinal), Row::Event(value)) => {
                require(sequence == read.outcome.sequence && ordinal < read.outcome.events)?;
                let value = value.get().ok_or_else(invalid)?.expand(read.ledger);
                require(
                    value.sequence == sequence
                        && value.ordinal == ordinal
                        && value.invocation == read.outcome.invocation,
                )?;
            }
            _ => (),
        }
    }
    require(counts.meta && counts.outcome && counts.added.outcomes == 1)?;
    if read.profile == NativeContentProfile::AuthoredV1
        && read.outcome.operation == NativeOperation::Create
    {
        require(counts.added.creation_results == 1)?;
        read.require(Key::CreationResult(read.outcome.invocation))?;
    }
    let Row::Meta(next) = read.require(Key::Meta)? else {
        return Err(invalid());
    };
    require(next.logical_time == read.outcome.logical_time)?;
    let pairs = [
        (
            previous.claims,
            counts.added.claims,
            next.claims,
            read.limits.claims,
        ),
        (
            previous.outcomes,
            counts.added.outcomes,
            next.outcomes,
            read.limits.outcomes,
        ),
        (
            previous.events,
            counts.added.events,
            next.events,
            read.limits.events,
        ),
        (
            previous.definitions,
            counts.added.definitions,
            next.definitions,
            read.limits.definitions,
        ),
        (
            previous.evaluations,
            counts.added.evaluations,
            next.evaluations,
            read.limits.evaluations,
        ),
        (
            previous.artifacts,
            counts.added.artifacts,
            next.artifacts,
            read.limits.artifacts,
        ),
        (
            previous.results,
            counts.added.results,
            next.results,
            read.limits.results,
        ),
        (
            previous.receipts,
            counts.added.receipts,
            next.receipts,
            read.limits.receipts,
        ),
        (
            previous.responses,
            counts.added.responses,
            next.responses,
            read.limits.responses,
        ),
        (
            previous.result_testaments,
            counts.added.result_testaments,
            next.result_testaments,
            read.limits.claims,
        ),
        (
            previous.monitors,
            counts.added.monitors,
            next.monitors,
            read.limits.monitors,
        ),
        (
            previous.monitor_links,
            counts.added.monitor_links,
            next.monitor_links,
            read.limits.monitor_links,
        ),
        (
            previous.creation_results,
            counts.added.creation_results,
            next.creation_results,
            read.limits.outcomes,
        ),
    ];
    read.charge(add(pairs.len(), 1)?)?;
    for (old, added, actual, maximum) in pairs {
        require(add(old, added)? == actual)?;
        if actual > maximum {
            return Err(ContractError::Capacity.into());
        }
    }
    let outcome = read.outcome;
    let pairs = [
        (counts.added.claims, outcome.created),
        (counts.writes.claims, outcome.changed),
        (counts.writes.definitions, outcome.definitions),
        (counts.writes.evaluations, outcome.evaluations),
        (counts.writes.artifacts, outcome.artifacts),
        (counts.writes.results, outcome.results),
        (counts.writes.receipts, outcome.receipts),
        (counts.writes.responses, outcome.responses),
        (counts.writes.result_testaments, outcome.result_testaments),
        (counts.writes.events, outcome.events),
    ];
    for (actual, expected) in pairs {
        read.charge(8)?;
        require(actual == usize::try_from(expected).map_err(|_| ContractError::Capacity)?)?;
    }
    // Every ordinal must be represented exactly once by the canonical new key
    // set; old history cannot satisfy this lookup because all keys use base+1.
    read.charge(add(
        usize::try_from(outcome.events).map_err(|_| ContractError::Capacity)?,
        1,
    )?)?;
    for ordinal in 0..outcome.events {
        read.charge(128)?;
        operation::check(outcome.operation, read.event(ordinal)?.fact)?;
    }
    Ok(())
}
