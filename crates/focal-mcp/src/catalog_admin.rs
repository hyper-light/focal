use crate::{ProtocolError, Tool};
use serde_json::json;
pub(crate) const TOOL_COUNT: usize = 32;
pub(crate) fn append(tools: &mut Vec<Tool>) -> Result<(), ProtocolError> {
    tools
        .try_reserve_exact(TOOL_COUNT)
        .map_err(|_| ProtocolError::Capacity)?;
    let output = crate::catalog::output_schema("cluster.invite")?;
    for (name, description) in [
        (
            "cluster.node.identity",
            "Read the authenticated physical owner's immutable identity. No participant or administration authority is transferred.",
        ),
        (
            "cluster.node.health",
            "Observe local root and fleet owner progress. This diagnostic is not a quorum, placement or complete service health guarantee.",
        ),
        (
            "cluster.node.config",
            "Read the running physical node's validated saved listener and root namespace configuration. No desired placement policy or achieved guarantee is inferred.",
        ),
        (
            "cluster.replicas.diagnostics",
            "Observe an installed application owner's commit/applied prefix, pending checkpoint and immutable decoder floor. No checkpoint, activation or upgrade is initiated.",
        ),
    ] {
        let properties = if name == "cluster.replicas.diagnostics" {
            json!({"session":{"type":"string","pattern":"^[0-9a-fA-F]{32}$"}})
        } else {
            json!({})
        };
        tools.push(Tool{name:name.into(),description:description.into(),input_schema:json!({"type":"object","additionalProperties":false,"properties":properties}),output_schema:output.clone(),read_only:true,destructive:false,idempotent:true});
    }
    tools.push(Tool{name:"cluster.invite".into(),description:"Create or recover the exact named private Node invitation through the local founder signer. This grants enrollment only, not voting or placement.".into(),input_schema:json!({"type":"object","additionalProperties":false,"required":["node","output"],"properties":{"node":{"type":"string","minLength":1,"maxLength":63,"pattern":"^[A-Za-z0-9_.-]+$"},"output":{"type":"string","minLength":1,"maxLength":4096}}}),output_schema:output.clone(),read_only:false,destructive:false,idempotent:true});
    for (name, description, read, node, session, retry) in [
        (
            "cluster.replicas.transfer",
            "Initiate application-group leadership transfer under an exact owner-checked configuration fence. Success does not assert election completion.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.replicas.list",
            "Observe a bounded live page of locally installed application replicas. Management sequence is process-local; this is not a placement or quorum snapshot.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.replicas.show",
            "Quorum-read an installed application replica's committed configuration. Omitting session selects the physical node's original application ledger.",
            true,
            false,
            true,
            false,
        ),
        (
            "cluster.replicas.add_learner",
            "Journal exact application-group learner admission, preserving managed decoder-floor and candidate checks. This does not install a remote replica or activate durability.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.replicas.promote",
            "Journal application-group promotion; commit requires actual catch-up and existing decoder compatibility guards.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.replicas.remove",
            "Journal exact application-group removal; this does not drain or reassign placement.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.replicas.leave_joint",
            "Commit exit from an existing joint application configuration.",
            false,
            false,
            true,
            false,
        ),
        (
            "cluster.replicas.request.inspect",
            "Read the retained local r1 operation reference and known exact membership receipt.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.replicas.request.retry",
            "Retry only the retained exact r1 membership request. No new intent or replacement replica is created.",
            false,
            false,
            false,
            true,
        ),
        (
            "cluster.replicas.request.reconcile",
            "Recover the exact latest membership receipt or preserve FencedOutcomeUnknown when a newer configuration makes the old request permanently inadmissible.",
            false,
            false,
            false,
            true,
        ),
    ] {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();
        if session {
            properties.insert(
                "session".into(),
                json!({"type":"string","pattern":"^[0-9a-f]{32}$"}),
            );
        }
        if node {
            properties.insert("node".into(), json!({"type":"integer","minimum":1}));
            required.push("node");
        }
        if !read && session {
            properties.insert(
                "expected_configuration_index".into(),
                json!({"type":"integer","minimum":0}),
            );
        }
        if name.ends_with(".list") {
            properties.insert(
                "after".into(),
                json!({"type":"string","pattern":"^[0-9a-f]{32}$"}),
            );
            properties.insert(
                "limit".into(),
                json!({"type":"integer","minimum":1,"maximum":64,"default":32}),
            );
        }
        if retry {
            properties.insert("operation_id".into(),json!({"type":"string","pattern":"^r1:[0-9a-f]{16}:[0-9a-f]{32}$","minLength":52,"maxLength":52}));
            required.push("operation_id");
        }
        tools.push(Tool{name:name.into(),description:description.into(),input_schema:json!({"type":"object","additionalProperties":false,"required":required,"properties":properties}),output_schema:output.clone(),read_only:read,destructive:!read,idempotent:read||retry});
    }
    for (name, description, read, node, fence, retry) in [
        (
            "cluster.status",
            "Quorum-read the local root-group leader and voters. A contact is not membership or placement.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.membership.show",
            "Read the committed exact root configuration and its applied fence.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.nodes.list",
            "Read committed node contact announcements. This does not assert active credentials, voter membership, or achieved durability.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.membership.add_learner",
            "Journal and commit a root learner admission. On a lost response use cluster.request.inspect and retry that exact reference.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.membership.promote",
            "Commit promotion only after existing consensus catch-up and configuration checks. Lost outcomes remain journaled.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.membership.remove",
            "Commit root-group member removal under its exact configuration. This is not a data-placement drain.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.membership.leave_joint",
            "Commit completion of an existing joint root configuration.",
            false,
            false,
            true,
            false,
        ),
        (
            "cluster.leader.transfer",
            "Initiate a transfer to an eligible root voter. Success is initiation, not a committed or observed leadership change.",
            false,
            true,
            true,
            false,
        ),
        (
            "cluster.request.inspect",
            "Read the latest local admin journal reference and committed receipt, if known. No request is sent.",
            true,
            false,
            false,
            false,
        ),
        (
            "cluster.request.reconcile",
            "Persist a known receipt, or a same-prefix proof that an absent request precondition was superseded. Absence alone never releases an unknown intent.",
            false,
            false,
            false,
            true,
        ),
        (
            "cluster.request.retry",
            "Retry only the exact retained latest admin request. An older reference never creates new work.",
            false,
            false,
            false,
            true,
        ),
    ] {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();
        if node {
            properties.insert("node".into(), json!({"type":"integer","minimum":1}));
            required.push("node");
        }
        if fence {
            properties.insert(
                "expected_configuration_index".into(),
                json!({"type":"integer","minimum":0}),
            );
        }
        if retry {
            properties.insert("operation_id".into(),json!({"type":"string","pattern":"^a1:[0-9a-f]{16}:[0-9a-f]{16}$","minLength":36,"maxLength":36}));
            required.push("operation_id");
        }
        tools.push(Tool {name:name.into(),description:description.into(),input_schema:json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":properties,"required":required,"additionalProperties":false}),output_schema:output.clone(),read_only:read,destructive:!read,idempotent:read||retry});
    }
    for (name, read, page) in [
        ("cluster.invitations.list", true, true),
        ("cluster.invitations.get", true, false),
        ("cluster.credentials.get", true, false),
        ("cluster.invitations.revoke", false, false),
        ("cluster.credentials.revoke", false, false),
    ] {
        let mut properties = serde_json::Map::new();
        let id = json!({"type":"string","pattern":"^[0-9a-f]{32}$","minLength":32,"maxLength":32});
        let mut required = Vec::new();
        if page {
            properties.insert("after".into(), id);
            properties.insert(
                "limit".into(),
                json!({"type":"integer","minimum":1,"maximum":64,"default":32}),
            );
        } else {
            properties.insert("id".into(), id);
            required.push("id");
        }
        if page || !read {
            properties.insert(
                "expected_revision".into(),
                json!({"type":"integer","minimum":0}),
            );
        }
        tools.push(Tool {name:name.into(),description:if read {"Read redacted committed invitation and issued-credential metadata, never secrets. Page with expected_revision to reject a changing registry."} else {"Journal and commit revocation of this invitation and any credential it issued. This does not drain data or remove consensus membership. Lost replies use cluster.request.inspect/retry."}.into(),input_schema:json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":properties,"required":required,"additionalProperties":false}),output_schema:output.clone(),read_only:read,destructive:!read,idempotent:read});
    }
    tools.push(Tool {name:"cluster.client.invite".into(),description:"Create or recover an exact named Client-role invitation and atomically write its private file without overwriting another invitation. No secret appears in the result; the client is not a node or Runtime.".into(),input_schema:json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["name","output"],"properties":{"name":{"type":"string","minLength":1,"maxLength":63,"pattern":"^[a-zA-Z0-9_.-]+$"},"output":{"type":"string","minLength":1,"maxLength":4096}}}),output_schema:output,read_only:false,destructive:false,idempotent:true});
    Ok(())
}
