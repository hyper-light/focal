//! The catalogue an adapter serves on a native ledger: version 2 of the shared
//! verb names from the native descriptor table, filtered by the principal's
//! standing, plus the four recovery tools of the `n1:` journal. V1 tools are
//! not offered because the owner refuses their wire profile; the catalogue is
//! decided once per connection by the engine probe.
use crate::{ProtocolError, Tool, catalog};
use focal_client::operations::{OperationDescriptor, native_descriptors};
use focal_wire::{NativeProfile, NativeStanding};
use serde_json::{Map, Value};

/// `request.inspect`, `request.retry`, `request.pending`, `request.acknowledge`.
pub(crate) const RECOVERY_TOOLS: usize = 4;
/// The per-tool output schema identity of native results (`schema_version` 2).
pub(crate) const OUTPUT_VERSION: u16 = 2;

/// Whether the descriptor is offered under this standing. A projection-only
/// ledger carries no authored content profile, so claim creation is withheld
/// rather than advertised and refused; every other refusal is the owner's
/// per call, and the server still authorizes a hidden tool invoked directly.
pub(crate) fn permitted(standing: &NativeStanding, descriptor: &OperationDescriptor) -> bool {
    // A projection-only ledger authors no claims: neither claim.submit nor
    // the peer shapes that compile to it.
    !(standing.profile == NativeProfile::ProjectionOnly
        && (descriptor.name == "claim.submit"
            || focal_client::operations::authored_shape(descriptor).is_some()))
}

pub(crate) fn tool_count(standing: &NativeStanding) -> Result<usize, ProtocolError> {
    native_descriptors()
        .iter()
        .filter(|descriptor| permitted(standing, descriptor))
        .count()
        .checked_add(RECOVERY_TOOLS)
        .ok_or(ProtocolError::Capacity)
}

pub(crate) fn catalog(standing: &NativeStanding) -> Result<Vec<Tool>, ProtocolError> {
    let mut tools = Vec::new();
    tools
        .try_reserve_exact(tool_count(standing)?)
        .map_err(|_| ProtocolError::Capacity)?;
    for descriptor in native_descriptors()
        .iter()
        .filter(|descriptor| permitted(standing, descriptor))
    {
        let mut input = descriptor
            .input_schema()
            .map_err(|_| ProtocolError::Limits)?;
        catalog::schema::prune_definitions(&mut input)?;
        let kinds: &[&str] = if descriptor.mutation {
            let properties = input
                .as_object_mut()
                .and_then(|object| object.get_mut("properties"))
                .and_then(Value::as_object_mut)
                .ok_or(ProtocolError::Limits)?;
            properties.insert("operation_id".into(), native_id_schema(false));
            &["native", "native_refused", "error"]
        } else if descriptor.result_kind == focal_client::operations::ResultKind::List {
            &["native_list", "error"]
        } else if descriptor.name == "claim.wait" {
            &["native_wait", "error"]
        } else {
            &["native_read", "error"]
        };
        tools.push(Tool {
            name: descriptor.name.into(),
            description: descriptor.description.into(),
            input_schema: input,
            output_schema: catalog::specialized(descriptor.name, kinds, OUTPUT_VERSION)?,
            read_only: descriptor.read_only(),
            destructive: descriptor.destructive,
            idempotent: true,
        });
    }
    for (name, description, read_only, destructive, takes_id, kinds) in [
        (
            "request.inspect",
            "Inspect one journaled native operation by its n1: reference: the committed receipt, the recorded refusal, or Pending while no receipt is bound. remote:true instead reads the owner's committed outcome for the same request key without touching the journal, which also observes an operation another adapter journaled under this context.",
            true,
            false,
            true,
            &[
                "native",
                "native_refused",
                "managed_request",
                "native_read",
                "error",
            ][..],
        ),
        (
            "request.retry",
            "Resend the exact journaled frame of one n1: reference until the owner commits it, or return the already bound receipt without a send. The owner resolves the request identity before admission, so a retry never executes twice.",
            false,
            true,
            true,
            &["native", "native_refused", "error"][..],
        ),
        (
            "request.pending",
            "List this adapter's journaled native operations whose results have not been acknowledged, including ones whose reply was lost. Listing sends nothing.",
            true,
            false,
            false,
            &["managed_requests", "error"][..],
        ),
        (
            "request.acknowledge",
            "Confirm you have consumed a committed native operation's result; it leaves request.pending. The receipt stays readable through request.inspect. Only a committed operation can be acknowledged.",
            false,
            true,
            true,
            &["managed_request", "error"][..],
        ),
    ] {
        let mut properties = Map::new();
        if takes_id {
            properties.insert("operation_id".into(), native_id_schema(true));
        }
        if name == "request.inspect" {
            properties.insert(
                "remote".into(),
                serde_json::json!({"type":"boolean","default":false}),
            );
        }
        let required = if takes_id {
            vec!["operation_id"]
        } else {
            Vec::new()
        };
        tools.push(Tool {
            name: name.into(),
            description: description.into(),
            input_schema: serde_json::json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":properties,"required":required,"additionalProperties":false}),
            output_schema: catalog::specialized(name, kinds, OUTPUT_VERSION)?,
            read_only,
            destructive,
            idempotent: true,
        });
    }
    Ok(tools)
}

/// An `n1:` reference: the request identity the journal claims before any
/// frame is sent. Mutations accept it optionally so an agent may choose the
/// identity of a retry; the recovery tools require it.
fn native_id_schema(required: bool) -> Value {
    let description = if required {
        "The n1: reference of a journaled native operation, as returned in operation_id."
    } else {
        "Optional n1: reference to claim for this operation. Omit it and the adapter mints one and returns it in operation_id; supply the same reference with the same input to resume an exact retry. A reference already bound to different input is refused."
    };
    serde_json::json!({"type":"string","minLength":35,"maxLength":35,
        "pattern":"^n1:[0-9a-f]{32}$",
        "not":{"const":"n1:00000000000000000000000000000000"},
        "description":description})
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
    use focal_model::{ParticipantId, SessionSeq};
    use focal_wire::NativePeerRole;
    use std::collections::BTreeSet;

    fn standing(profile: NativeProfile) -> NativeStanding {
        NativeStanding {
            principal: ParticipantId::from_u128(1),
            role: NativePeerRole::Actor,
            profile,
            native_sequence: SessionSeq(1),
            logical_time: 1,
        }
    }

    #[test]
    fn native_catalogue_is_exact_versioned_and_withholds_creation_on_projection_only_ledgers() {
        let full = focal_client::operations::descriptors()[0]
            .output_schema()
            .unwrap();
        let original = full["properties"]["result"]["oneOf"].as_array().unwrap();
        let tools = catalog(&standing(NativeProfile::AuthoredV1)).unwrap();
        assert_eq!(
            tools.len(),
            native_descriptors().len() + RECOVERY_TOOLS,
            "{tools:?}"
        );
        assert_eq!(
            tools.len(),
            tool_count(&standing(NativeProfile::AuthoredV1)).unwrap()
        );
        let mut ids = BTreeSet::new();
        let mut v1 = BTreeSet::new();
        for tool in &crate::catalog::catalog().unwrap() {
            v1.insert(tool.output_schema["$id"].as_str().unwrap().to_string());
        }
        for tool in &tools {
            let id = tool.output_schema["$id"].as_str().unwrap();
            assert_eq!(
                id,
                format!("urn:focal:mcp:{}:output:{OUTPUT_VERSION}", tool.name)
            );
            assert!(ids.insert(id.to_string()));
            // A native tool never shares an output identity with its V1 namesake.
            assert!(!v1.contains(id), "{id}");
            let branches = tool.output_schema["properties"]["result"]["oneOf"]
                .as_array()
                .unwrap();
            assert!(branches.len() < original.len(), "{}", tool.name);
            assert!(branches.iter().all(|branch| original.contains(branch)));
            // Branches keep the released union's order; compare as sets.
            let kinds: BTreeSet<&str> = branches
                .iter()
                .map(|branch| branch["properties"]["kind"]["const"].as_str().unwrap())
                .collect();
            assert!(kinds.contains(&"error"), "{}", tool.name);
            assert_eq!(tool.input_schema["additionalProperties"], false);
            match focal_client::operations::find_native(&tool.name) {
                Some(descriptor) => {
                    assert_eq!(tool.read_only, !descriptor.mutation);
                    assert_eq!(tool.destructive, descriptor.destructive);
                    assert_eq!(
                        tool.input_schema["$id"],
                        format!("urn:focal:operation:{}:input:2", tool.name)
                    );
                    let operation_id = tool.input_schema["properties"].get("operation_id");
                    if descriptor.mutation {
                        assert_eq!(kinds, ["native", "native_refused", "error"].into());
                        let schema = operation_id.unwrap();
                        assert_eq!(schema["pattern"], "^n1:[0-9a-f]{32}$");
                        assert!(
                            !tool.input_schema["required"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|field| field == "operation_id"),
                            "{}",
                            tool.name
                        );
                    } else if descriptor.result_kind == focal_client::operations::ResultKind::List {
                        assert_eq!(kinds, ["native_list", "error"].into());
                        assert!(operation_id.is_none());
                    } else if descriptor.name == "claim.wait" {
                        assert_eq!(kinds, ["native_wait", "error"].into());
                        assert!(operation_id.is_none());
                    } else {
                        assert_eq!(kinds, ["native_read", "error"].into());
                        assert!(operation_id.is_none());
                    }
                }
                None => {
                    assert!(tool.name.starts_with("request."), "{}", tool.name);
                    let required = tool.input_schema["required"].as_array().unwrap();
                    let takes_id = tool.name != "request.pending";
                    assert_eq!(!required.is_empty(), takes_id, "{}", tool.name);
                    assert_eq!(
                        tool.read_only,
                        matches!(tool.name.as_str(), "request.inspect" | "request.pending")
                    );
                    assert_eq!(tool.destructive, !tool.read_only);
                    assert!(tool.idempotent);
                }
            }
        }
        // No V1-only, managed, transfer or watch tool is offered.
        for name in [
            "request.reserve",
            "request.seal",
            "request.status",
            "upload.begin",
            "watch.open",
        ] {
            assert!(!tools.iter().any(|tool| tool.name == name), "{name}");
        }
        // The V1 context tool is replaced by its version 2 namesake.
        assert!(tools.iter().any(|tool| tool.name == "validation.context"));
        // A projection-only ledger authors nothing: claim.submit and the four
        // peer shapes that compile to it are withheld together.
        let projection = catalog(&standing(NativeProfile::ProjectionOnly)).unwrap();
        assert_eq!(projection.len(), tools.len() - 5);
        for withheld in [
            "claim.submit",
            "claim.challenge",
            "claim.consult",
            "claim.correct",
            "claim.follow_up",
        ] {
            assert!(
                !projection.iter().any(|tool| tool.name == withheld),
                "{withheld}"
            );
        }
        assert!(projection.iter().any(|tool| tool.name == "claim.post"));
        assert_eq!(
            projection.len(),
            tool_count(&standing(NativeProfile::ProjectionOnly)).unwrap()
        );
    }

    #[test]
    fn native_catalogue_with_administration_leaves_room_for_a_real_first_job() {
        const MIB: usize = 1024 * 1024;
        // The production stdio server's envelope (160 MiB, 80 MiB reserved).
        let budget = MemoryBudget::new(160 * MIB, 80 * MIB).unwrap();
        let standing = standing(NativeProfile::AuthoredV1);
        let count = tool_count(&standing).unwrap() + crate::catalog_admin::TOOL_COUNT;
        let construction = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Ordinary,
                (count + 1) * 512 * 1024,
            )
            .unwrap()
            .commit();
        let mut tools = catalog(&standing).unwrap();
        crate::catalog_admin::append(&mut tools).unwrap();
        let limits = crate::Limits {
            max_frame_bytes: 278_528,
            max_response_bytes: 16 * MIB,
            max_active_calls: 1,
            ..crate::Limits::default()
        };
        let protocol = crate::Protocol::new(
            limits,
            budget.clone(),
            crate::ServerInfo {
                name: "focal".into(),
                version: "1".into(),
            },
            tools,
        )
        .unwrap();
        drop(construction);
        let catalog = budget.stats();
        let _queues = budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024)
            .unwrap()
            .commit();
        let _decoder = crate::FrameDecoder::new(limits.max_frame_bytes, budget.clone()).unwrap();
        let result = budget.reserve(BudgetKind::Control, BudgetLane::Ordinary, 32 * MIB);
        assert!(
            result.is_ok(),
            "catalogue={catalog:?}, before job={:?}, result={result:?}",
            budget.stats()
        );
        drop(protocol);
    }
}
