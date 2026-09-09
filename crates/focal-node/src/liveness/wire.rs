//! The probe and its acknowledgement: bounded postcard payloads inside
//! `Operation::Probe` / `Response::Probe`, validated on decode so a peer's
//! bytes never reach the driver unchecked.
use super::{
    coordinates::{NetworkCoordinate, VivaldiConfig},
    gossip::{LivenessUpdate, MAX_PIGGYBACK},
    health::{LocalHealth, MAX_SCORE},
};
use focal_wire::MAX_PROBE_BYTES;
use serde::{Deserialize, Serialize};

pub const PROBE_SCHEMA: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeKind {
    /// Answer for yourself.
    Direct,
    /// Probe `target` on my behalf and relay whether it answered.
    Indirect { target: u64 },
}
/// A host under load asks the accuser for more time: its incarnation, a
/// progress witness it cannot fake while stuck, and whether its admission is
/// refusing capacity (then it is healed, never extended).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionRequest {
    pub incarnation: u64,
    pub witness: u64,
    pub overloaded: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtensionOutcome {
    Granted { millis: u64 },
    Denied,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeRequest {
    pub schema: u16,
    pub kind: ProbeKind,
    /// The probing node; must equal the authenticated peer's node identity.
    pub sender: u64,
    pub generation: u64,
    pub sequence: u64,
    pub incarnation: u64,
    pub coordinate: NetworkCoordinate,
    pub health: LocalHealth,
    pub extension: Option<ExtensionRequest>,
    pub updates: Vec<LivenessUpdate>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeOutcome {
    Ack,
    /// The relay's own probe of `target`, with the target's incarnation when
    /// it answered.
    Relayed {
        target: u64,
        acknowledged: Option<u64>,
    },
    Refused,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeReply {
    pub schema: u16,
    pub outcome: ProbeOutcome,
    /// The answering node and the sequence it answers.
    pub node: u64,
    pub generation: u64,
    pub sequence: u64,
    pub incarnation: u64,
    pub coordinate: NetworkCoordinate,
    pub health: LocalHealth,
    pub extension: Option<ExtensionOutcome>,
    pub updates: Vec<LivenessUpdate>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProbeCodecError {
    #[error("probe payload is malformed")]
    Invalid,
    #[error("probe payload exceeds its bound")]
    Capacity,
}
fn valid_updates(updates: &[LivenessUpdate]) -> bool {
    updates.len() <= MAX_PIGGYBACK
        && updates
            .iter()
            .all(|update| update.node != 0 && update.generation != 0 && update.origin != 0)
}
fn valid_state(
    node: u64,
    generation: u64,
    coordinate: &NetworkCoordinate,
    health: LocalHealth,
    updates: &[LivenessUpdate],
    config: &VivaldiConfig,
) -> bool {
    node != 0
        && generation != 0
        && coordinate.is_valid(config)
        && health.score <= MAX_SCORE
        && valid_updates(updates)
}
impl ProbeRequest {
    pub fn validate(&self, config: &VivaldiConfig) -> Result<(), ProbeCodecError> {
        let target_ok = match self.kind {
            ProbeKind::Direct => true,
            ProbeKind::Indirect { target } => target != 0 && target != self.sender,
        };
        if self.schema != PROBE_SCHEMA
            || !target_ok
            || self.incarnation == 0
            || !valid_state(
                self.sender,
                self.generation,
                &self.coordinate,
                self.health,
                &self.updates,
                config,
            )
            || self
                .extension
                .is_some_and(|extension| extension.incarnation == 0)
        {
            return Err(ProbeCodecError::Invalid);
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, ProbeCodecError> {
        encode(self)
    }
    pub fn decode(bytes: &[u8], config: &VivaldiConfig) -> Result<Self, ProbeCodecError> {
        let request: Self = decode(bytes)?;
        request.validate(config)?;
        Ok(request)
    }
}
impl ProbeReply {
    pub fn validate(&self, config: &VivaldiConfig) -> Result<(), ProbeCodecError> {
        let outcome_ok = match self.outcome {
            ProbeOutcome::Ack | ProbeOutcome::Refused => true,
            ProbeOutcome::Relayed {
                target,
                acknowledged,
            } => target != 0 && target != self.node && acknowledged != Some(0),
        };
        if self.schema != PROBE_SCHEMA
            || !outcome_ok
            || self.incarnation == 0
            || !valid_state(
                self.node,
                self.generation,
                &self.coordinate,
                self.health,
                &self.updates,
                config,
            )
        {
            return Err(ProbeCodecError::Invalid);
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, ProbeCodecError> {
        encode(self)
    }
    pub fn decode(bytes: &[u8], config: &VivaldiConfig) -> Result<Self, ProbeCodecError> {
        let reply: Self = decode(bytes)?;
        reply.validate(config)?;
        Ok(reply)
    }
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ProbeCodecError> {
    let bytes = postcard::to_stdvec(value).map_err(|_| ProbeCodecError::Capacity)?;
    if bytes.is_empty() || bytes.len() > MAX_PROBE_BYTES {
        return Err(ProbeCodecError::Capacity);
    }
    Ok(bytes)
}
fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, ProbeCodecError> {
    if bytes.is_empty() || bytes.len() > MAX_PROBE_BYTES {
        return Err(ProbeCodecError::Capacity);
    }
    let (value, rest) =
        postcard::take_from_bytes::<T>(bytes).map_err(|_| ProbeCodecError::Invalid)?;
    if !rest.is_empty() {
        return Err(ProbeCodecError::Invalid);
    }
    Ok(value)
}
