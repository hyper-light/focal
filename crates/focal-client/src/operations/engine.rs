//! One engine selection for every surface. The V1 and native catalogues
//! share names; which one serves a name is decided here, the same way for
//! every host. Offline (discovery: list, get, example, shape-only
//! validation, completion) an explicit request wins, else the only catalogue
//! that has the name, else V1, a fresh ledger's engine. Online (a mutation,
//! `status`, context-backed validation) the standing probe every host
//! performs once decides; an explicit request that contradicts it is
//! refused, and an engine assumed because the owner was unreachable is
//! reported as assumed. The examples of both engines are generated from the
//! contracts the mutation path decodes with, so what discovery prints is
//! what the ledger accepts.
use super::{
    AuthoredOperation, LimitedJson, NativeAuthoredOperation, NativeListOperation,
    NativeReadOperation, OperationDescriptor, ResultKind, WireProfile, descriptors, find,
    find_native, native_descriptors, parse_json, parse_native_json, parse_native_list_json,
    parse_native_read_json,
};
use crate::input::{IdGenerator, InputError};
use crate::{Client, ClientError, ClientTransport};
use focal_model::{LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::{
    NativeReadQuery, NativeReadRequest, NativeStanding, Operation, ReadConsistency, RequestEnvelope,
};

/// The application catalogue one engine serves.
pub fn application(wire: WireProfile) -> &'static [OperationDescriptor] {
    match wire {
        WireProfile::V1 => descriptors(),
        WireProfile::Native => native_descriptors(),
    }
}
/// One engine's descriptor of `name`.
pub fn find_application(wire: WireProfile, name: &str) -> Option<&'static OperationDescriptor> {
    match wire {
        WireProfile::V1 => find(name),
        WireProfile::Native => find_native(name),
    }
}

/// An application document decoded by the engine that serves it: the V1
/// authored operation, or the native mutation, read or list a native
/// descriptor's result kind selects, as the native hosts decode it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplicationDocument {
    V1(AuthoredOperation),
    Native(NativeAuthoredOperation),
    NativeRead(NativeReadOperation),
    NativeList(NativeListOperation),
}
impl ApplicationDocument {
    pub fn wire(&self) -> WireProfile {
        match self {
            Self::V1(_) => WireProfile::V1,
            Self::Native(_) | Self::NativeRead(_) | Self::NativeList(_) => WireProfile::Native,
        }
    }
    pub fn descriptor(&self) -> &'static OperationDescriptor {
        match self {
            Self::V1(operation) => operation.descriptor(),
            Self::Native(operation) => operation.descriptor(),
            Self::NativeRead(operation) => operation.descriptor(),
            Self::NativeList(operation) => operation.descriptor(),
        }
    }
    pub fn name(&self) -> &'static str {
        self.descriptor().name
    }
    /// `{"operation": NAME, "input": {...}}` with stable field order and
    /// every serde default expanded; no identity generation.
    pub fn canonical_intent(&self) -> Result<Vec<u8>, InputError> {
        match self {
            Self::V1(operation) => operation.canonical_intent(),
            Self::Native(operation) => operation.canonical_intent(),
            Self::NativeRead(operation) => canonical(operation),
            Self::NativeList(operation) => canonical(operation),
        }
    }
}
fn canonical(value: &impl serde::Serialize) -> Result<Vec<u8>, InputError> {
    let mut output = LimitedJson(Vec::new());
    serde_json::to_writer(&mut output, value).map_err(|_| InputError::Capacity)?;
    Ok(output.0)
}
/// Decode one application document through the engine that serves `name`.
/// A native name decodes by its descriptor's result kind, exactly as the
/// native hosts dispatch it; an unknown name refuses.
pub fn decode_application(
    wire: WireProfile,
    name: &str,
    bytes: &[u8],
) -> Result<ApplicationDocument, InputError> {
    match wire {
        WireProfile::V1 => parse_json(name, bytes).map(ApplicationDocument::V1),
        WireProfile::Native => {
            let descriptor = find_native(name).ok_or(InputError::Invalid(
                "unknown or unexposed native application operation",
            ))?;
            match descriptor.result_kind {
                ResultKind::List => {
                    parse_native_list_json(name, bytes).map(ApplicationDocument::NativeList)
                }
                ResultKind::Read => {
                    parse_native_read_json(name, bytes).map(ApplicationDocument::NativeRead)
                }
                ResultKind::Mutation | ResultKind::Reconcile => {
                    parse_native_json(name, bytes).map(ApplicationDocument::Native)
                }
            }
        }
    }
}

/// The normalized example of `name` on `wire`: the engine's authored example
/// decoded by the same decoder the mutation path uses and re-serialized with
/// every default expanded, so the printed document is exactly what the
/// engine accepts. Illustrative object identities are not expanded and no
/// referenced object is claimed to exist.
pub fn example(wire: WireProfile, name: &str) -> Result<serde_json::Value, InputError> {
    let raw = match wire {
        WireProfile::V1 => super::examples::v1_raw_example(name),
        WireProfile::Native => super::native_examples::native_raw_example(name),
    }
    .ok_or(InputError::Invalid(
        "no authored example is available for this released operation",
    ))?;
    let expanded;
    let raw = if raw.contains("SCHEMA") {
        expanded = raw.replace("SCHEMA", &focal_evidence::test_report_schema().to_string());
        expanded.as_str()
    } else {
        raw
    };
    let document = decode_application(wire, name, raw.as_bytes())?;
    let mut normalized: serde_json::Value =
        serde_json::from_slice(&document.canonical_intent()?)
            .map_err(|_| InputError::Invalid("invalid normalized authored example"))?;
    normalized
        .as_object_mut()
        .and_then(|value| value.remove("input"))
        .ok_or(InputError::Invalid("normalized authored input is absent"))
}

/// Why a requested engine cannot serve a surface.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// A V1-only name was requested on the native engine.
    #[error(
        "{0} is not available on the native engine yet; it arrives with the native index families"
    )]
    NotNative(String),
    #[error("{0} is not a released application operation of either engine")]
    Unknown(String),
    /// The probe answered V1 and the native engine was requested.
    #[error("the selected ledger answers the V1 engine; --native contradicts it")]
    Contradicted,
    /// The owner could not be reached and the native engine was requested:
    /// nothing confirms it, and nothing native can be checked without it.
    #[error(
        "the selected ledger's engine could not be probed (the owner is unreachable); --native cannot be confirmed"
    )]
    Unprobed,
}

/// Which engine serves `name` offline: an explicit request wins, else the
/// only catalogue that has the name, else V1 (a fresh ledger's engine). A
/// V1-only name under an explicit native request is refused by name.
pub fn select_offline(native: bool, name: &str) -> Result<WireProfile, EngineError> {
    let (v1, native_catalogue) = (find(name).is_some(), find_native(name).is_some());
    match (native, v1, native_catalogue) {
        (true, _, true) => Ok(WireProfile::Native),
        (true, true, false) => Err(EngineError::NotNative(name.into())),
        (false, true, _) => Ok(WireProfile::V1),
        (false, false, true) => Ok(WireProfile::Native),
        (_, false, false) => Err(EngineError::Unknown(name.into())),
    }
}

/// The engine of a live ledger, as the standing probe found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// The owner answered the standing read under the native profile.
    Native(NativeStanding),
    /// The owner answered under the legacy profile, or (`assumed`) could not
    /// be reached and no native journal of this host proves otherwise.
    V1 { assumed: bool },
}
impl Engine {
    pub fn wire(&self) -> WireProfile {
        match self {
            Self::Native(_) => WireProfile::Native,
            Self::V1 { .. } => WireProfile::V1,
        }
    }
    pub fn standing(&self) -> Option<&NativeStanding> {
        match self {
            Self::Native(standing) => Some(standing),
            Self::V1 { .. } => None,
        }
    }
    /// Whether the engine was assumed rather than answered.
    pub fn assumed(&self) -> bool {
        matches!(self, Self::V1 { assumed: true })
    }
}
/// Resolve one standing read's answer into the engine a host runs on.
/// Without a reachable owner the engine is unknown: a host whose native
/// journal already exists has proof the ledger is native and refuses to run
/// as V1 (it would mint identities in the wrong namespace), any other host
/// keeps the V1 behaviour, whose local validation and journaling never
/// needed the network. A standing for another principal is a misconfigured
/// context, never this host's engine.
pub fn resolve_probe(
    answer: Result<Option<NativeStanding>, ClientError>,
    journal_initialized: bool,
    principal: ParticipantId,
) -> Result<Engine, ClientError> {
    match answer {
        Ok(Some(standing)) => {
            if standing.principal != principal {
                return Err(ClientError::Configuration);
            }
            Ok(Engine::Native(standing))
        }
        Ok(None) => Ok(Engine::V1 { assumed: false }),
        Err(ClientError::Transport) if !journal_initialized => Ok(Engine::V1 { assumed: true }),
        Err(error) => Err(error),
    }
}
/// The engine probe every host performs once per connection: one
/// linearizable standing read under the native profile. A node without the
/// native engine refuses the profile at negotiation before any frame is
/// seen; the answer is resolved by [`resolve_probe`].
pub async fn probe<T: ClientTransport>(
    client: &Client<T>,
    ledger: LedgerId,
    request_id: RequestId,
    journal_initialized: bool,
    principal: ParticipantId,
) -> Result<Engine, ClientError> {
    let operation = Operation::NativeRead(NativeReadRequest {
        consistency: ReadConsistency::Linearizable,
        query: NativeReadQuery::Standing,
        max_items: 1,
    });
    let request = RequestEnvelope {
        protocol: focal_wire::participant_protocol(&operation),
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id,
        operation,
    };
    resolve_probe(
        client.native_standing(request).await,
        journal_initialized,
        principal,
    )
}
/// Which engine serves an online surface: the probe's, unless an explicit
/// native request contradicts a V1 answer or asks for what an unreachable
/// owner cannot confirm.
pub fn select_online(engine: &Engine, native: bool) -> Result<WireProfile, EngineError> {
    match (engine, native) {
        (Engine::Native(_), _) => Ok(WireProfile::Native),
        (Engine::V1 { assumed: false }, true) => Err(EngineError::Contradicted),
        (Engine::V1 { assumed: true }, true) => Err(EngineError::Unprobed),
        (Engine::V1 { .. }, false) => Ok(WireProfile::V1),
    }
}

/// Identities for a compilation whose result is discarded (local validation
/// before any journal exists): sequential, never zero, and never one of the
/// exact hexadecimal identities the document itself names, so a synthetic
/// identity cannot create a false self-reference or duplicate.
pub struct ThrowawayIds {
    next: u128,
    excluded: Vec<[u8; 16]>,
}
impl ThrowawayIds {
    /// `canonical` is the document's canonical intent.
    pub fn excluding(canonical: &[u8]) -> Result<Self, InputError> {
        Ok(Self {
            next: 0,
            excluded: super::authored_ids(canonical)?,
        })
    }
}
impl IdGenerator for ThrowawayIds {
    fn next_id(&mut self) -> Result<[u8; 16], InputError> {
        // The excluded set is finite: at most one step past its size finds
        // an identity it does not hold.
        for _ in 0..=self.excluded.len() {
            self.next = self.next.checked_add(1).ok_or(InputError::Capacity)?;
            let bytes = self.next.to_be_bytes();
            if self.excluded.binary_search(&bytes).is_err() {
                return Ok(bytes);
            }
        }
        Err(InputError::Capacity)
    }
}
