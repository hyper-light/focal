//! The peer verbs are authored shapes of `claim.submit`: a challenge, a
//! consultation, a correction and a follow-up consultation each lower to one
//! complete claim document before the shared claim compiler runs, so they
//! produce the same frame, `n1:` identity and receipt as a hand-written
//! claim with the same content. A correction's and a follow-up's occurrence
//! identity derives from what they follow, so a repeated delivery resolves
//! to one claim through the owner's content identity.
use crate::{CompileError, Resolved};
use focal_client::input::{BuildContext, InputError, parse_id};
use focal_client::operations::{
    NativeChallengeDocument, NativeClaimDocument, NativeConsultDocument, NativeCorrectionDocument,
    NativeFollowUpDocument, NativeRelationDocument,
};
use focal_model::{ArtifactId, ClaimId, ContentHash};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `ID` or `ID@HASH`: the exact artifact reference, pinned at its committed
/// descriptor hash from the resolved read when the document omits it.
fn evidence(reference: &str, resolved: &Resolved) -> Result<String, CompileError> {
    let (id, hash) = match reference.split_once('@') {
        Some((id, hash)) => (id, Some(hash)),
        None => (reference, None),
    };
    let artifact = ArtifactId(parse_id(id)?);
    let hash = match hash {
        Some(hash) => {
            let parsed: ContentHash = focal_client::input::parse_hash(hash)?;
            hex(&parsed.0)
        }
        None => hex(&resolved.artifact(artifact)?.content_hash.0),
    };
    Ok(format!("artifact:{}@{hash}", hex(&artifact.0)))
}

/// The subject a follow-up addresses when the document names none: the
/// subject of the claim it follows.
fn subject_of(claim: &str, resolved: &Resolved) -> Result<String, CompileError> {
    let claim = ClaimId(parse_id(claim)?);
    Ok(hex(&resolved.claim(claim)?.subject.0))
}

/// A deterministic occurrence identity: sixteen bytes of a keyed BLAKE3
/// digest over the ledger, the author and the facts the follow-up rests on.
fn occurrence(
    label: &str,
    context: &BuildContext,
    parts: &[&[u8]],
) -> Result<String, CompileError> {
    let mut hash = blake3::Hasher::new_derive_key(label);
    hash.update(&context.ledger.tenant.0);
    hash.update(&context.ledger.session.0);
    hash.update(&context.actor.0);
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    let digest = hash.finalize();
    let bytes = digest
        .as_bytes()
        .get(..16)
        .ok_or(InputError::Invalid("occurrence digest"))?;
    Ok(hex(bytes))
}

/// Every object identity of a follow-up whose occurrence is derived is
/// derived with it (the claim, then each validation lacking an explicit
/// id), so the same facts produce the same descriptor and the owner resolves
/// a repeated delivery to the committed claim instead of minting a second.
fn derive_identities(
    document: &mut NativeClaimDocument,
    label: &str,
    context: &BuildContext,
    parts: &[&[u8]],
) -> Result<(), CompileError> {
    if document.id.is_none() {
        document.id = Some(occurrence(label, context, &[parts, &[b"claim"]].concat())?);
    }
    if let Some(deadline) = &mut document.deadline
        && deadline.timer.is_none()
    {
        deadline.timer = Some(occurrence(label, context, &[parts, &[b"timer"]].concat())?);
    }
    for (index, validation) in document.validations.iter_mut().enumerate() {
        let ordinal = u32::try_from(index)
            .map_err(|_| InputError::Capacity)?
            .to_le_bytes();
        if validation.id.is_none() {
            validation.id = Some(occurrence(
                label,
                context,
                &[parts, &[b"validation", &ordinal]].concat(),
            )?);
        }
        if validation.deadline.timer.is_none() {
            validation.deadline.timer = Some(occurrence(
                label,
                context,
                &[parts, &[b"validation-timer", &ordinal]].concat(),
            )?);
        }
    }
    Ok(())
}

pub(crate) fn challenge(
    document: &NativeChallengeDocument,
    resolved: &Resolved,
) -> Result<NativeClaimDocument, CompileError> {
    let mut relations = document.relations.clone();
    if let Some(artifact) = &document.artifact {
        relations
            .try_reserve_exact(1)
            .map_err(|_| InputError::Capacity)?;
        relations.push(NativeRelationDocument {
            kind: "reviews".into(),
            target: evidence(artifact, resolved)?,
        });
    }
    Ok(NativeClaimDocument {
        id: document.id.clone(),
        occurrence: document.occurrence.clone(),
        description: document.description.clone(),
        target: document.target.clone(),
        action: "challenge".into(),
        scopes: document.scopes.clone(),
        relations,
        deadline: document.deadline.clone(),
        validations: document.validations.clone(),
        slots: document.slots.clone(),
        max_responses: document.max_responses,
        scope_limits: document.scope_limits,
        parent: document.parent.clone(),
        policy: Some(document.policy.clone()),
    })
}

pub(crate) fn consult(document: &NativeConsultDocument) -> NativeClaimDocument {
    NativeClaimDocument {
        id: document.id.clone(),
        occurrence: document.occurrence.clone(),
        description: document.description.clone(),
        target: document.target.clone(),
        action: "consultation".into(),
        scopes: document.scopes.clone(),
        relations: document.relations.clone(),
        deadline: document.deadline.clone(),
        validations: document.validations.clone(),
        slots: document.slots.clone(),
        max_responses: document.max_responses,
        scope_limits: document.scope_limits,
        parent: document.parent.clone(),
        policy: document.policy.clone(),
    }
}

pub(crate) fn correction(
    document: &NativeCorrectionDocument,
    context: &BuildContext,
    resolved: &Resolved,
) -> Result<NativeClaimDocument, CompileError> {
    let challenge = ClaimId(parse_id(&document.challenge)?);
    let verdict = evidence(&document.verdict, resolved)?;
    let mut relations = document.relations.clone();
    relations
        .try_reserve_exact(2)
        .map_err(|_| InputError::Capacity)?;
    relations.push(NativeRelationDocument {
        kind: "invalidates".into(),
        target: format!("claim:{}", hex(&challenge.0)),
    });
    relations.push(NativeRelationDocument {
        kind: "reviews".into(),
        target: verdict.clone(),
    });
    let derived = document.occurrence.is_none();
    let parts: [&[u8]; 3] = [
        &challenge.0,
        verdict.as_bytes(),
        document.description.as_bytes(),
    ];
    let occurrence = match &document.occurrence {
        Some(occurrence) => occurrence.clone(),
        None => occurrence("focal.peer.correction.v1", context, &parts)?,
    };
    let target = match &document.target {
        Some(target) => target.clone(),
        None => subject_of(&document.challenge, resolved)?,
    };
    let mut lowered = NativeClaimDocument {
        id: document.id.clone(),
        occurrence: Some(occurrence),
        description: document.description.clone(),
        target,
        action: "correction".into(),
        scopes: document.scopes.clone(),
        relations,
        deadline: document.deadline.clone(),
        validations: document.validations.clone(),
        slots: document.slots.clone(),
        max_responses: document.max_responses,
        scope_limits: document.scope_limits,
        parent: document.parent.clone(),
        policy: document.policy.clone(),
    };
    if derived {
        derive_identities(&mut lowered, "focal.peer.correction.v1", context, &parts)?;
    }
    Ok(lowered)
}

pub(crate) fn follow_up(
    document: &NativeFollowUpDocument,
    context: &BuildContext,
    resolved: &Resolved,
) -> Result<NativeClaimDocument, CompileError> {
    let refined = ClaimId(parse_id(&document.refines)?);
    let mut relations = document.relations.clone();
    relations
        .try_reserve_exact(1)
        .map_err(|_| InputError::Capacity)?;
    relations.push(NativeRelationDocument {
        kind: "refines".into(),
        target: format!("claim:{}", hex(&refined.0)),
    });
    let derived = document.occurrence.is_none();
    let parts: [&[u8]; 2] = [&refined.0, document.description.as_bytes()];
    let occurrence = match &document.occurrence {
        Some(occurrence) => occurrence.clone(),
        None => occurrence("focal.peer.follow-up.v1", context, &parts)?,
    };
    let target = match &document.target {
        Some(target) => target.clone(),
        None => subject_of(&document.refines, resolved)?,
    };
    let mut lowered = NativeClaimDocument {
        id: document.id.clone(),
        occurrence: Some(occurrence),
        description: document.description.clone(),
        target,
        action: "consultation".into(),
        scopes: document.scopes.clone(),
        relations,
        deadline: document.deadline.clone(),
        validations: document.validations.clone(),
        slots: document.slots.clone(),
        max_responses: document.max_responses,
        scope_limits: document.scope_limits,
        parent: document.parent.clone(),
        policy: document.policy.clone(),
    };
    if derived {
        derive_identities(&mut lowered, "focal.peer.follow-up.v1", context, &parts)?;
    }
    Ok(lowered)
}
