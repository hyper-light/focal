//! Offline native activation of a single-node ledger. The data directory is
//! opened exclusively, the activation record commits under the node's own
//! authority and the checkpoint retains it before the command returns.
use crate::{
    config::Settings,
    embedded::{EmbeddedNode, NodeError},
};
use focal_consensus::ConsensusError;
use focal_ledger::{LedgerActivation, LedgerError, NativeContentProfile};
use std::time::Duration;

/// Bounded driving of a single-node commit; each round polls the session.
const ROUNDS: usize = 512;
const ROUND_PAUSE: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalActivation {
    pub ledger: focal_model::LedgerId,
    pub group: [u8; 16],
    pub activation: LedgerActivation,
    /// False when the ledger was already native and nothing was proposed.
    pub proposed: bool,
}

/// Activate native history on an embedded node that is not running. A
/// populated legacy prefix is imported with the projection profile; an empty
/// ledger starts with the requested profile.
pub fn activate_local(
    settings: &Settings,
    profile: NativeContentProfile,
) -> Result<LocalActivation, NodeError> {
    let mut node = EmbeddedNode::open(settings)?;
    let ledger = node.identity.ledger;
    let group = node.session.group_id();
    if node.session.activation().is_native() {
        return Ok(LocalActivation {
            ledger,
            group,
            activation: node.session.activation(),
            proposed: false,
        });
    }
    let mut proposed = false;
    for _ in 0..ROUNDS {
        let attempt = if node.session.legacy_populated() {
            let (chunk, manifest) = (
                node.content.upload_chunk_bytes(),
                node.content.max_manifest_bytes(),
            );
            let now = crate::native_ingress::logical_time(&node.session)
                .map_err(|_| NodeError::Domain("system clock unavailable".into()))?;
            node.session.propose_native_import(
                NativeContentProfile::ProjectionOnly,
                Some(&mut node.content),
                now,
                chunk,
                manifest,
            )
        } else {
            node.session.propose_native_activation(profile)
        };
        match attempt {
            Ok(()) => {
                proposed = true;
                break;
            }
            Err(LedgerError::Consensus(ConsensusError::PersistencePending))
            | Err(LedgerError::NotReady { .. })
            | Err(LedgerError::Retry) => {
                let _ = node.session.poll()?;
                std::thread::sleep(ROUND_PAUSE);
            }
            Err(error) => return Err(error.into()),
        }
    }
    if !proposed {
        return Err(NodeError::Domain(
            "native activation could not be proposed; the node never became authoritative".into(),
        ));
    }
    for _ in 0..ROUNDS {
        match node.session.poll() {
            Ok(_) | Err(LedgerError::Retry) => {}
            Err(error) => return Err(error.into()),
        }
        if node.session.native_authoritative() {
            node.checkpoint()?;
            return Ok(LocalActivation {
                ledger,
                group,
                activation: node.session.activation(),
                proposed: true,
            });
        }
        std::thread::sleep(ROUND_PAUSE);
    }
    Err(NodeError::Domain(
        "native activation was proposed but did not become authoritative in time".into(),
    ))
}
