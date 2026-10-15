//! focal's grammar: `focal ACTION THING [ARG...]`, as ../shards says its commands.
//!
//! One table names every command a person types: the section of the help it is listed
//! in, its action, the thing it acts on, and the command of the derived tree
//! (`command_tree`) that does it. The parser people use is built from this table: each
//! `ACTION THING` is that command's own parser, its arguments, values and checks, under
//! the new name, so help, usage, errors and completions all say what is typed. A parsed
//! command is then said as its derived command, whose handler runs unchanged. The old
//! thing-first words are not accepted: the derived tree is internal.

use clap::{Arg, ArgMatches, Command};
use std::ffi::{OsStr, OsString};

/// A command as it is typed: `focal {action} {thing}`, listed under `section`, done by
/// the derived command at `path`. `about` replaces the derived command's own where that
/// says nothing or says it in the old words.
pub(crate) struct Use {
    pub section: &'static str,
    pub action: &'static str,
    pub thing: &'static str,
    pub path: &'static [&'static str],
    pub about: Option<&'static str>,
}

const fn of(
    section: &'static str,
    action: &'static str,
    thing: &'static str,
    path: &'static [&'static str],
) -> Use {
    Use {
        section,
        action,
        thing,
        path,
        about: None,
    }
}

const fn said(
    section: &'static str,
    action: &'static str,
    thing: &'static str,
    path: &'static [&'static str],
    about: &'static str,
) -> Use {
    Use {
        section,
        action,
        thing,
        path,
        about: Some(about),
    }
}

/// The sections of the help, in order.
pub(crate) const SECTIONS: [&str; 12] = [
    "run", "connect", "claims", "work", "validate", "read", "requests", "node", "cluster", "data",
    "deploy", "schemas",
];

/// Every command, by section, in the order the help lists them.
pub(crate) const USES: &[Use] = &[
    // Run focal, alone or as a cluster's node, and its agent tools.
    said(
        "run",
        "start",
        "node",
        &["start"],
        "Run the durable service, using saved network settings on restart",
    ),
    said(
        "run",
        "join",
        "cluster",
        &["join"],
        "Persist a pinned enrollment and physical identity from an invitation, then exit",
    ),
    said(
        "run",
        "prepare",
        "volume",
        &["prepare-volume"],
        "Give the data directory to the node's user: create it, set its owner and mode 0700, then exit",
    ),
    said(
        "run",
        "run",
        "demo",
        &["demo"],
        "Run or resume the real claim, testament and validator example with exclusive local ownership",
    ),
    said(
        "run",
        "serve",
        "mcp",
        &["mcp", "serve"],
        "Serve agent tools over a bounded, durable stdio MCP connection",
    ),
    said(
        "run",
        "run",
        "code",
        &["code", "run"],
        "Run a JavaScript program that calls focal's tools; print what it returns",
    ),
    said(
        "run",
        "search",
        "tools",
        &["code", "search"],
        "Run a program over the tools you may call; print what it selects",
    ),
    // Connections to a ledger, apart from any node's enrollment.
    of("connect", "add", "context", &["context", "add"]),
    of("connect", "enroll", "context", &["context", "enroll"]),
    of("connect", "list", "contexts", &["context", "list"]),
    of("connect", "inspect", "context", &["context", "show"]),
    of("connect", "use", "context", &["context", "use"]),
    of("connect", "remove", "context", &["context", "remove"]),
    // Claims: work asked for, its lifecycle and its lineage.
    said(
        "claims",
        "submit",
        "claim",
        &["submit", "claim"],
        "Generate an immutable claim; `post claim` makes it actionable",
    ),
    of("claims", "submit", "claims", &["submit", "claims"]),
    said(
        "claims",
        "post",
        "claim",
        &["claim", "post"],
        "Make a claim actionable for its subject",
    ),
    of("claims", "wait", "claim", &["claim", "wait"]),
    said(
        "claims",
        "report",
        "progress",
        &["claim", "progress"],
        "Report progress on a claim you hold",
    ),
    said(
        "claims",
        "cancel",
        "claim",
        &["claim", "cancel"],
        "Cancel a claim you issued",
    ),
    of("claims", "supersede", "claim", &["claim", "supersede"]),
    said(
        "claims",
        "release",
        "scope",
        &["claim", "release-scope"],
        "Release the owned scope of your terminal claim",
    ),
    said(
        "claims",
        "trace",
        "lineage",
        &["claim", "lineage"],
        "Read a claim's cause ancestors, corrections, refinements and children at one prefix",
    ),
    said(
        "claims",
        "challenge",
        "participant",
        &["claim", "challenge"],
        "Challenge a participant to prove or redo stated work, with an immutable follow-up policy",
    ),
    said(
        "claims",
        "consult",
        "participant",
        &["claim", "consult"],
        "Consult a participant for work answering a query",
    ),
    said(
        "claims",
        "correct",
        "challenge",
        &["claim", "correct"],
        "Correct a failed challenge on the exact report of its verdict",
    ),
    said(
        "claims",
        "refine",
        "consultation",
        &["claim", "follow-up"],
        "File a follow-up consultation refining a committed consultation",
    ),
    // Work: taking a claim, its evidence, artifacts and testament.
    said(
        "work",
        "acquire",
        "receipt",
        &["receipt", "acquire"],
        "Acquire a receipt as the authenticated subject of the claim",
    ),
    said(
        "work",
        "adopt",
        "receipt",
        &["receipt", "adopt"],
        "As the issuer, replace the claim's current holder",
    ),
    said(
        "work",
        "begin",
        "evidence",
        &["evidence", "begin"],
        "Begin a receipt-fenced evidence set",
    ),
    of("work", "submit", "artifact", &["submit", "artifact"]),
    of("work", "register", "artifact", &["artifact", "register"]),
    said(
        "work",
        "submit",
        "diagnostic",
        &["artifact", "diagnostic"],
        "Submit a diagnostic for failed or impossible work",
    ),
    said(
        "work",
        "fail",
        "slot",
        &["artifact", "fail"],
        "As the holder, record a slot as failed, citing your committed production diagnostic",
    ),
    said(
        "work",
        "receive",
        "artifact",
        &["artifact", "receive"],
        "As the issuer, receive one generated work artifact",
    ),
    said(
        "work",
        "reject",
        "artifact",
        &["artifact", "reject"],
        "As the issuer, reject a work artifact for a structure or metadata failure with your diagnostic",
    ),
    said(
        "work",
        "inspect",
        "upload",
        &["artifact", "upload", "inspect"],
        "Inspect saved artifact transfer progress without sending or resuming any request",
    ),
    said(
        "work",
        "cancel",
        "upload",
        &["artifact", "upload", "cancel"],
        "Retire a saved upload and remove server staging; committed content stays",
    ),
    of("work", "submit", "testament", &["submit", "testament"]),
    said(
        "work",
        "post",
        "testament",
        &["testament", "post"],
        "Post your closed testament to the issuer",
    ),
    of("work", "receive", "testament", &["testament", "receive"]),
    // Validation: evaluations of work, their verdicts, audits and monitors.
    of("validate", "begin", "validation", &["validation", "begin"]),
    of(
        "validate",
        "begin",
        "increment",
        &["validation", "begin-increment"],
    ),
    of(
        "validate",
        "submit",
        "validation",
        &["submit", "validation"],
    ),
    said(
        "validate",
        "report",
        "verdict",
        &["validation", "report"],
        "Report the begun attempt's verdict with its evidence",
    ),
    of(
        "validate",
        "complete",
        "validation",
        &["validation", "complete"],
    ),
    said(
        "validate",
        "seal",
        "increments",
        &["validation", "seal-increments"],
        "As the issuer, seal the claim's increment targets",
    ),
    said(
        "validate",
        "enter",
        "whole-work",
        &["validation", "enter-whole-work"],
        "As the issuer, close the increment cohort of the received testament and enter whole-work evaluation",
    ),
    said(
        "validate",
        "generate",
        "audit",
        &["audit", "generate"],
        "Generate the result testament of your closed claim",
    ),
    said(
        "validate",
        "post",
        "audit",
        &["audit", "post"],
        "Post a generated result testament",
    ),
    of("validate", "register", "monitor", &["monitor", "register"]),
    of("validate", "get", "monitor", &["monitor", "get"]),
    said(
        "validate",
        "rebind",
        "monitor",
        &["monitor", "rebind"],
        "Rebind one root of your monitor from a predecessor claim to its successor",
    ),
    said(
        "validate",
        "cancel",
        "monitor",
        &["monitor", "cancel"],
        "Cancel one of your claim's monitors",
    ),
    // Reading the ledger: objects, lists, watches and pages of its graph.
    said(
        "read",
        "get",
        "claim",
        &["get", "claim"],
        "Read one claim at the authoritative prefix",
    ),
    said(
        "read",
        "get",
        "testament",
        &["get", "testament"],
        "Read one testament at the authoritative prefix",
    ),
    of("read", "get", "validation", &["get", "validation"]),
    said(
        "read",
        "get",
        "artifact",
        &["get", "artifact"],
        "Read one artifact at the authoritative prefix",
    ),
    said(
        "read",
        "get",
        "archived",
        &["get", "archived"],
        "One object of a claim's family wherever the family is: the ledger while it is live, its archive bundle once it retired",
    ),
    of("read", "list", "claims", &["list", "claims"]),
    of("read", "list", "testaments", &["list", "testaments"]),
    of("read", "list", "artifacts", &["list", "artifacts"]),
    of("read", "list", "validations", &["list", "validations"]),
    said(
        "read",
        "list",
        "evaluations",
        &["list", "evaluations"],
        "Select current evaluations by claim, validation, evaluator and verdict",
    ),
    said(
        "read",
        "list",
        "receipts",
        &["list", "receipts"],
        "Select receipts by holder and claim",
    ),
    said(
        "read",
        "list",
        "monitors",
        &["list", "monitors"],
        "The monitors registered on one claim",
    ),
    said(
        "read",
        "list",
        "events",
        &["list", "events"],
        "The publication history after an event position",
    ),
    of("read", "watch", "claims", &["watch", "claims"]),
    said(
        "read",
        "watch",
        "testaments",
        &["watch", "testaments"],
        "Watch testament facts as they commit",
    ),
    said(
        "read",
        "watch",
        "artifacts",
        &["watch", "artifacts"],
        "Watch artifact facts as they commit",
    ),
    said(
        "read",
        "watch",
        "validations",
        &["watch", "validations"],
        "Watch validation facts as they commit",
    ),
    said(
        "read",
        "watch",
        "everything",
        &["watch", "all"],
        "Watch every fact of the ledger as it commits",
    ),
    of("read", "resume", "watch", &["watch", "resume"]),
    of("read", "inspect", "watch", &["watch", "inspect"]),
    said(
        "read",
        "inspect",
        "ledger",
        &["ledger", "summary"],
        "Read bounded scalar counts of the ledger at a fresh quorum prefix",
    ),
    of("read", "traverse", "ledger", &["ledger", "traverse"]),
    said(
        "read",
        "inspect",
        "prefix",
        &["status"],
        "Read the running service's authoritative published prefix",
    ),
    said(
        "read",
        "list",
        "validators",
        &["validator", "list"],
        "List the validator contracts claims recorded; handlers execute outside focal",
    ),
    said(
        "read",
        "get",
        "validator",
        &["validator", "get"],
        "Inspect the requirement bindings of one exact immutable handler version",
    ),
    // Requests saved for recovery: build, send, retry, inspect and settle them.
    of("requests", "build", "request", &["request", "build"]),
    of("requests", "check", "request", &["request", "check"]),
    of("requests", "send", "request", &["request", "send"]),
    of("requests", "retry", "request", &["request", "retry"]),
    of("requests", "inspect", "request", &["request", "inspect"]),
    of("requests", "reserve", "request", &["request", "reserve"]),
    said(
        "requests",
        "list",
        "requests",
        &["request", "pending"],
        "List bounded outstanding CLI and MCP operations that still need recovery",
    ),
    of(
        "requests",
        "acknowledge",
        "request",
        &["request", "acknowledge"],
    ),
    of("requests", "seal", "request", &["request", "seal"]),
    said(
        "requests",
        "inspect",
        "mutation",
        &["request", "status"],
        "Query your retained mutation receipt; a missing receipt stays unknown",
    ),
    of("requests", "inspect", "epoch", &["request", "epoch"]),
    // This node and the replicas it hosts.
    said(
        "node",
        "inspect",
        "node",
        &["diagnose", "node"],
        "What this node reports about itself, read through its local socket; nothing changes",
    ),
    said(
        "node",
        "inspect",
        "replicas",
        &["diagnose", "cluster"],
        "What the replicas this node hosts report, read through its local socket; nothing changes",
    ),
    said(
        "node",
        "inspect",
        "identity",
        &["identity"],
        "Show identity metadata without opening or modifying the ledger",
    ),
    of("node", "list", "replicas", &["cluster", "replicas", "list"]),
    of(
        "node",
        "inspect",
        "replica",
        &["cluster", "replicas", "show"],
    ),
    said(
        "node",
        "transfer",
        "replica-leader",
        &["cluster", "replicas", "transfer"],
        "Move a replica's leadership under an exact application configuration fence",
    ),
    said(
        "node",
        "activate",
        "native",
        &["cluster", "replicas", "activate-native"],
        "Propose committed activation of native history on an empty ledger; every voter must already promise the native decoder",
    ),
    said(
        "node",
        "checkpoint",
        "replica",
        &["cluster", "replicas", "checkpoint"],
        "Checkpoint one installed replica's applied prefix now and compact its log",
    ),
    said(
        "node",
        "list",
        "ranges",
        &["cluster", "replicas", "ranges", "list"],
        "The members of a native session's range group and their holders",
    ),
    said(
        "node",
        "move",
        "range",
        &["cluster", "replicas", "ranges", "move"],
        "Move one member of a native session's range group to a node",
    ),
    said(
        "node",
        "inspect",
        "replica-membership",
        &["cluster", "replicas", "membership", "show"],
        "Quorum-read one replica's exact committed membership",
    ),
    said(
        "node",
        "add",
        "replica-learner",
        &["cluster", "replicas", "membership", "add-learner"],
        "Add a learner to one replica's membership",
    ),
    said(
        "node",
        "promote",
        "replica-learner",
        &["cluster", "replicas", "membership", "promote"],
        "Promote a caught-up learner of one replica to a voter",
    ),
    said(
        "node",
        "remove",
        "replica-member",
        &["cluster", "replicas", "membership", "remove"],
        "Remove a member from one replica's membership",
    ),
    said(
        "node",
        "leave",
        "replica-joint",
        &["cluster", "replicas", "membership", "leave-joint"],
        "Leave one replica's joint configuration",
    ),
    said(
        "node",
        "inspect",
        "replica-request",
        &["cluster", "replicas", "request", "inspect"],
        "Inspect the exact latest durable replica admin intent",
    ),
    said(
        "node",
        "retry",
        "replica-request",
        &["cluster", "replicas", "request", "retry"],
        "Retry the exact latest durable replica admin intent",
    ),
    said(
        "node",
        "reconcile",
        "replica-request",
        &["cluster", "replicas", "request", "reconcile"],
        "Reconcile the latest replica admin intent with what committed",
    ),
    // The cluster: its nodes, invitations, credentials, tenants, sessions and groups.
    said(
        "cluster",
        "inspect",
        "cluster",
        &["cluster", "status"],
        "Quorum-read the root group's leader and voting membership",
    ),
    said(
        "cluster",
        "invite",
        "node",
        &["cluster", "invite"],
        "Write a private one-node invitation; asking again under the same name is exact while it is live",
    ),
    said(
        "cluster",
        "invite",
        "client",
        &["cluster", "client", "invite"],
        "Invite an authenticated client principal, without any physical node role",
    ),
    said(
        "cluster",
        "list",
        "nodes",
        &["cluster", "nodes", "list"],
        "Inspect committed contact announcements",
    ),
    of("cluster", "drain", "node", &["cluster", "nodes", "drain"]),
    of(
        "cluster",
        "undrain",
        "node",
        &["cluster", "nodes", "undrain"],
    ),
    of("cluster", "remove", "node", &["cluster", "nodes", "remove"]),
    of(
        "cluster",
        "replace",
        "node",
        &["cluster", "nodes", "replace"],
    ),
    said(
        "cluster",
        "list",
        "invitations",
        &["cluster", "invitations", "list"],
        "Inspect redacted committed invitation metadata",
    ),
    said(
        "cluster",
        "get",
        "invitation",
        &["cluster", "invitations", "get"],
        "Inspect one redacted committed invitation",
    ),
    said(
        "cluster",
        "revoke",
        "invitation",
        &["cluster", "invitations", "revoke"],
        "Revoke an invitation and any credential issued through it; placement is not drained",
    ),
    said(
        "cluster",
        "get",
        "credential",
        &["cluster", "credentials", "get"],
        "Inspect the credential issued by an invitation",
    ),
    said(
        "cluster",
        "revoke",
        "credential",
        &["cluster", "credentials", "revoke"],
        "Disable the issuing invitation and credential; consensus membership is unchanged",
    ),
    said(
        "cluster",
        "renew",
        "credential",
        &["cluster", "credentials", "renew"],
        "Renew this node's own credential now: the same key under a fresh certificate and lifetime",
    ),
    said(
        "cluster",
        "rotate",
        "credential",
        &["cluster", "credentials", "rotate"],
        "Rotate this node's own credential to a fresh key under the same identity",
    ),
    said(
        "cluster",
        "list",
        "issuers",
        &["cluster", "credentials", "issuers"],
        "The issuers the cluster's credentials chain to, as committed",
    ),
    said(
        "cluster",
        "rotate",
        "issuer",
        &["cluster", "credentials", "rotate-issuer"],
        "Stage the issuer's successor (founder only), then activate it",
    ),
    of(
        "cluster",
        "admit",
        "tenant",
        &["cluster", "tenants", "admit"],
    ),
    of(
        "cluster",
        "list",
        "tenants",
        &["cluster", "tenants", "list"],
    ),
    of(
        "cluster",
        "create",
        "session",
        &["cluster", "sessions", "create"],
    ),
    of(
        "cluster",
        "plan",
        "session",
        &["cluster", "sessions", "plan"],
    ),
    said(
        "cluster",
        "inspect",
        "placement",
        &["cluster", "placement"],
        "Every directory partition this node acts on: nodes, sessions, their desired and achieved guarantee and what blocks it",
    ),
    said(
        "cluster",
        "plan",
        "placement",
        &["cluster", "plan"],
        "The bounded next actions the placement controller would take",
    ),
    said(
        "cluster",
        "inspect",
        "membership",
        &["cluster", "membership", "show"],
        "Inspect the root metadata group's configuration",
    ),
    said(
        "cluster",
        "add",
        "learner",
        &["cluster", "membership", "add-learner"],
        "Add a learner to the root metadata group",
    ),
    said(
        "cluster",
        "promote",
        "learner",
        &["cluster", "membership", "promote"],
        "Promote a caught-up learner of the root metadata group to a voter",
    ),
    said(
        "cluster",
        "remove",
        "member",
        &["cluster", "membership", "remove"],
        "Remove a member from the root metadata group",
    ),
    said(
        "cluster",
        "leave",
        "joint",
        &["cluster", "membership", "leave-joint"],
        "Leave the root metadata group's joint configuration",
    ),
    said(
        "cluster",
        "transfer",
        "leader",
        &["cluster", "leader", "transfer"],
        "Ask the root group's leader to hand over; success does not mean the target leads yet",
    ),
    said(
        "cluster",
        "inspect",
        "partition",
        &["cluster", "partitions", "show"],
        "Inspect a directory partition group's configuration",
    ),
    said(
        "cluster",
        "add",
        "partition-learner",
        &["cluster", "partitions", "add-learner"],
        "Add a learner to a directory partition group",
    ),
    said(
        "cluster",
        "promote",
        "partition-learner",
        &["cluster", "partitions", "promote"],
        "Promote a caught-up learner of a directory partition group",
    ),
    said(
        "cluster",
        "remove",
        "partition-member",
        &["cluster", "partitions", "remove"],
        "Remove a member from a directory partition group",
    ),
    said(
        "cluster",
        "transfer",
        "partition-leader",
        &["cluster", "partitions", "transfer"],
        "Hand a directory partition group's leadership on",
    ),
    said(
        "cluster",
        "inspect",
        "admin-request",
        &["cluster", "request", "inspect"],
        "Inspect the exact latest durable local admin intent",
    ),
    said(
        "cluster",
        "retry",
        "admin-request",
        &["cluster", "request", "retry"],
        "Retry the exact latest durable local admin intent",
    ),
    said(
        "cluster",
        "reconcile",
        "admin-request",
        &["cluster", "request", "reconcile"],
        "Reconcile the latest local admin intent with what committed",
    ),
    // Data: archives, backups, restores, repair and the upgrade fence.
    of(
        "data",
        "inspect",
        "archive",
        &["cluster", "archive", "show"],
    ),
    said(
        "data",
        "restore",
        "object",
        &["cluster", "gc", "restore"],
        "Bring a quarantined content object back by its domain and root, while its quarantine round has not expired",
    ),
    of("data", "create", "backup", &["cluster", "backup", "create"]),
    of("data", "verify", "backup", &["cluster", "backup", "verify"]),
    said(
        "data",
        "restore",
        "session",
        &["cluster", "restore"],
        "Restore a session from a verified backup onto this node; an unfenced source needs `--new-incarnation`",
    ),
    said(
        "data",
        "repair",
        "session",
        &["cluster", "repair"],
        "Repair a hosted session's custody on this node: re-verify, recopy what is missing, complete the other copies",
    ),
    said(
        "data",
        "inspect",
        "upgrade",
        &["cluster", "upgrade", "status"],
        "The committed fence, this binary's level and every node's reported capability",
    ),
    said(
        "data",
        "activate",
        "upgrade",
        &["cluster", "upgrade", "activate"],
        "Raise the fence to a level (founder only) once every node reports it; the fence never lowers",
    ),
    // Deployment policy, plans and packaging.
    of(
        "deploy",
        "explain",
        "deployment",
        &["deployment", "explain"],
    ),
    of("deploy", "plan", "deployment", &["deployment", "plan"]),
    of("deploy", "apply", "deployment", &["deployment", "apply"]),
    said(
        "deploy",
        "inspect",
        "deployment",
        &["deployment", "status"],
        "Progress of applied plans, re-checked against the directory",
    ),
    said(
        "deploy",
        "create",
        "root-key",
        &["create", "root-key"],
        "Make a node's root key: 32 random bytes in a new owner-only file, for the deployment's secret store",
    ),
    said(
        "deploy",
        "render",
        "kubernetes",
        &["deployment", "render", "kubernetes"],
        "Render Kubernetes packaging for the requested configuration; no cluster or node is touched",
    ),
    said(
        "deploy",
        "render",
        "systemd",
        &["deployment", "render", "systemd"],
        "Render systemd packaging for the requested configuration; no node is touched",
    ),
    said(
        "deploy",
        "get",
        "deployment-schema",
        &["deployment", "schema"],
        "Emit the machine-readable deployment schema",
    ),
    // Contracts, without opening a ledger.
    of("schemas", "list", "schemas", &["schema", "list"]),
    of("schemas", "get", "schema", &["schema", "get"]),
    said(
        "schemas",
        "inspect",
        "coverage",
        &["schema", "coverage"],
        "The native operation coverage table: every owner operation with its frame tags, actor, descriptor, command and exposure",
    ),
    said(
        "schemas",
        "validate",
        "document",
        &["schema", "validate"],
        "Validate an authored document locally; no request or journal is created",
    ),
    said(
        "schemas",
        "get",
        "example",
        &["schema", "example"],
        "Print a normalized authored input; replace its illustrative existing-object IDs",
    ),
    said(
        "schemas",
        "generate",
        "completion",
        &["completion"],
        "Generate shell completions from this binary's actual command tree",
    ),
];

/// What an action does to the several things it takes in one section, for the help's
/// one row of it; an action that takes one thing there is said by that thing's command.
pub(crate) const GROUP_ABOUTS: &[(&str, &str, &str)] = &[
    (
        "run",
        "run",
        "Run the claim, testament and validator demo, or a JavaScript program against focal's tools",
    ),
    (
        "claims",
        "submit",
        "Generate an immutable claim, or an atomic batch of them",
    ),
    (
        "work",
        "submit",
        "Attach a work artifact, report a diagnostic, or close the work cycle with a testament",
    ),
    (
        "work",
        "receive",
        "As the issuer, receive a work artifact or the closing testament",
    ),
    (
        "validate",
        "begin",
        "Begin a whole-work validation, or an incremental check of an attached artifact",
    ),
    (
        "read",
        "get",
        "Read one object at the authoritative prefix, an archived one, or a validator contract",
    ),
    (
        "read",
        "list",
        "List the objects that match a filter, a bounded page at a time",
    ),
    (
        "read",
        "watch",
        "Follow ledger facts as they commit; acknowledge only flushed output",
    ),
    (
        "read",
        "inspect",
        "A saved watch, the ledger's counts, or the authoritative published prefix",
    ),
    (
        "requests",
        "inspect",
        "A saved request's recovery state, your retained mutation receipt, or an epoch's admission",
    ),
    (
        "node",
        "inspect",
        "This node, its identity, the replicas it hosts, or one replica's membership or admin intent",
    ),
    (
        "node",
        "list",
        "The replicas this node hosts, or a native session's range group",
    ),
    (
        "cluster",
        "inspect",
        "The root group, placement, membership, a partition group, or the latest admin intent",
    ),
    (
        "cluster",
        "invite",
        "Write a private invitation for a node, or for a client principal",
    ),
    (
        "cluster",
        "list",
        "Nodes, invitations, credential issuers or tenants, as committed",
    ),
    (
        "cluster",
        "remove",
        "Remove a drained node, or a member of the root or a partition group",
    ),
    (
        "cluster",
        "get",
        "One redacted invitation, or the credential it issued",
    ),
    (
        "cluster",
        "revoke",
        "Revoke an invitation or a credential; placement is not drained",
    ),
    (
        "cluster",
        "rotate",
        "Rotate this node's credential to a fresh key, or stage the issuer's successor",
    ),
    (
        "cluster",
        "plan",
        "Plan a session's placement, or show the controller's next actions",
    ),
    (
        "cluster",
        "add",
        "Add a learner to the root group or to a partition group",
    ),
    (
        "cluster",
        "promote",
        "Promote a caught-up learner of the root group or of a partition group",
    ),
    (
        "cluster",
        "transfer",
        "Hand the root group's or a partition group's leadership on",
    ),
    (
        "data",
        "inspect",
        "A retired claim's archive bundle, or the upgrade fence",
    ),
    (
        "data",
        "restore",
        "A quarantined content object, or a session from a verified backup",
    ),
    (
        "deploy",
        "render",
        "Render Kubernetes or systemd packaging; no cluster or node is touched",
    ),
    (
        "schemas",
        "get",
        "Print an operation's schema, or a normalized example of its input",
    ),
];

/// One row of the help: an action, the things it takes in a section, and what it does.
pub(crate) struct Group {
    pub action: &'static str,
    pub things: Vec<&'static str>,
    pub about: String,
}

/// The rows of `section`: one for each action, its things alphabetical, the actions
/// alphabetical.
pub(crate) fn groups(section: &str, tree: &Command) -> Vec<Group> {
    let mut rows: Vec<Group> = Vec::new();
    for u in USES.iter().filter(|u| u.section == section) {
        match rows.iter_mut().find(|g| g.action == u.action) {
            Some(group) => group.things.push(u.thing),
            None => rows.push(Group {
                action: u.action,
                things: vec![u.thing],
                about: about(u, tree),
            }),
        }
    }
    for group in &mut rows {
        group.things.sort_unstable();
        if group.things.len() > 1 {
            group.about = GROUP_ABOUTS
                .iter()
                .find(|(s, a, _)| *s == section && *a == group.action)
                .map(|(_, _, about)| (*about).to_string())
                .unwrap_or_default();
        }
    }
    rows.sort_by(|a, b| a.action.cmp(b.action));
    rows
}

/// What focal is, as its help's head says.
pub(crate) const ABOUT: &str =
    "Durable claims, evidence and validation for agents that work together";

/// The global options: taken before or after the action and its thing.
const GLOBALS_WITH_VALUE: [&str; 4] =
    ["--config", "--data-dir", "--client-context", "--trace-file"];

/// The command of `tree` at `path`.
pub(crate) fn find<'a>(tree: &'a Command, path: &[&str]) -> Option<&'a Command> {
    path.iter().try_fold(tree, |node, segment| {
        node.get_subcommands()
            .find(|sub| sub.get_name() == *segment)
    })
}

/// The use typed as `action thing`.
pub(crate) fn lookup(action: &str, thing: &str) -> Option<&'static Use> {
    USES.iter().find(|u| u.action == action && u.thing == thing)
}

/// The actions, each once, in the order the table first says them.
pub(crate) fn actions() -> impl Iterator<Item = &'static str> {
    USES.iter().enumerate().filter_map(|(i, u)| {
        let first = USES
            .iter()
            .take(i)
            .all(|earlier| earlier.action != u.action);
        first.then_some(u.action)
    })
}

/// What a use does: its own words, or its derived command's, without the engine's
/// prefix the derived tree still carries.
pub(crate) fn about(u: &Use, tree: &Command) -> String {
    let own = u.about.map(str::to_string).or_else(|| {
        find(tree, u.path)
            .and_then(Command::get_about)
            .map(|a| a.to_string())
    });
    let text = own.unwrap_or_default();
    let text = text.strip_prefix("Native engine: ").unwrap_or(&text);
    let text = text.trim_end_matches('.');
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The parser people use, built from [`USES`] over the derived `tree`: an action for
/// each action, a thing for each use, each thing its derived command's own parser.
pub(crate) fn command_from(tree: &Command) -> Command {
    let mut root = Command::new("focal")
        .version(env!("CARGO_PKG_VERSION"))
        .about(ABOUT)
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand_value_name("ACTION")
        .args(globals(tree));
    for action in actions() {
        let mut verb = Command::new(action)
            .subcommand_required(true)
            .disable_help_subcommand(true)
            .subcommand_value_name("THING");
        for u in USES.iter().filter(|u| u.action == action) {
            if let Some(leaf) = find(tree, u.path) {
                // The options of the commands on its way (`--session` of a replica's
                // membership) are the leaf's own here: one action, one thing.
                let lifted: Vec<Arg> = lifted(tree, u.path)
                    .into_iter()
                    .filter(|a| leaf.get_arguments().all(|own| own.get_id() != a.get_id()))
                    .collect();
                verb = verb.subcommand(
                    leaf.clone()
                        .name(u.thing)
                        .about(about(u, tree))
                        .args(lifted),
                );
            }
        }
        root = root.subcommand(verb);
    }
    root
}

/// The options of the derived commands between the root and the leaf at `path`: what
/// the derived tree takes before a leaf's own words. Each is an option with a value.
fn lifted(tree: &Command, path: &[&str]) -> Vec<Arg> {
    let inner = path.len().saturating_sub(1);
    (1..inner.saturating_add(1))
        .filter_map(|depth| path.get(..depth))
        .filter(|prefix| prefix.len() < path.len())
        .filter_map(|prefix| find(tree, prefix))
        .flat_map(|node| {
            node.get_arguments()
                .filter(|a| !a.is_global_set() && !a.is_positional())
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The derived tree's global options, for the parser people use.
fn globals(tree: &Command) -> Vec<Arg> {
    tree.get_arguments()
        .filter(|a| a.is_global_set())
        .cloned()
        .collect()
}

/// The parser people use.
pub(crate) fn command() -> Command {
    command_from(&super::command_tree::command())
}

/// The parser completions are generated from: the same, over the derived tree's
/// completion projection.
pub(crate) fn completion_command() -> Command {
    command_from(&super::command_tree::completion_command())
}

/// Where the action and the thing are in `args` (the program's name first): the first
/// two words that are neither an option nor a global option's value. Only global
/// options can come before the action or between it and its thing, since an action
/// takes nothing of its own.
pub(crate) fn words(args: &[OsString]) -> (Option<usize>, Option<usize>) {
    let mut found: [Option<usize>; 2] = [None, None];
    let mut want = 0usize;
    let mut skip = false;
    for (i, arg) in args.iter().enumerate().skip(1) {
        if skip {
            skip = false;
            continue;
        }
        let Some(text) = arg.to_str() else {
            break;
        };
        if text == "--" {
            break;
        }
        if GLOBALS_WITH_VALUE.contains(&text) {
            skip = true;
            continue;
        }
        if text.starts_with('-') {
            continue;
        }
        match found.get_mut(want) {
            Some(slot) => *slot = Some(i),
            None => break,
        }
        want = want.saturating_add(1);
        if want >= 2 {
            break;
        }
    }
    let [action, thing] = found;
    (action, thing)
}

/// `args` said as the derived command at `path` of `tree`: the action and the thing
/// replaced by the path's words, an intermediate command's options moved to follow its
/// word (where the derived tree takes them), everything else where it was.
pub(crate) fn internal(tree: &Command, args: &[OsString], path: &[&str]) -> Vec<OsString> {
    let (action, thing) = words(args);
    // Which long options belong to which word of the path.
    let mut owners: Vec<(usize, String)> = Vec::new();
    let leaf = find(tree, path);
    for depth in 1..path.len() {
        let Some(node) = path.get(..depth).and_then(|prefix| find(tree, prefix)) else {
            continue;
        };
        for a in node
            .get_arguments()
            .filter(|a| !a.is_global_set() && !a.is_positional())
        {
            let owned_by_leaf =
                leaf.is_some_and(|l| l.get_arguments().any(|own| own.get_id() == a.get_id()));
            if let (Some(long), false) = (a.get_long(), owned_by_leaf) {
                owners.push((depth, long.to_string()));
            }
        }
    }
    let mut moved: Vec<Vec<OsString>> = vec![Vec::new(); path.len()];
    let mut kept: Vec<(usize, OsString)> = Vec::with_capacity(args.len());
    let mut iter = args.iter().enumerate();
    let mut ended = false;
    while let Some((i, arg)) = iter.next() {
        let text = arg.to_str().unwrap_or_default();
        if text == "--" {
            ended = true;
        }
        let owner = if ended || Some(i) == action || Some(i) == thing {
            None
        } else {
            owners.iter().find(|(_, long)| {
                text.strip_prefix("--").is_some_and(|t| {
                    t == long
                        || t.strip_prefix(long.as_str())
                            .is_some_and(|r| r.starts_with('='))
                })
            })
        };
        match owner {
            Some((depth, long)) => {
                let mut taken = vec![arg.clone()];
                if text.strip_prefix("--") == Some(long.as_str())
                    && let Some((_, value)) = iter.next()
                {
                    taken.push(value.clone());
                }
                if let Some(slot) = moved.get_mut(*depth) {
                    slot.extend(taken);
                }
            }
            None => kept.push((i, arg.clone())),
        }
    }
    let mut out = Vec::with_capacity(args.len().saturating_add(path.len()));
    for (i, arg) in kept {
        if Some(i) == action {
            for (depth, word) in path.iter().enumerate() {
                out.push(OsString::from(word));
                if let Some(slot) = moved.get(depth.saturating_add(1)) {
                    out.extend(slot.iter().cloned());
                }
            }
        } else if Some(i) != thing {
            out.push(arg);
        }
    }
    out
}

/// The use `matches` (of the parser people use) chose.
pub(crate) fn chosen(matches: &ArgMatches) -> Option<&'static Use> {
    let (action, sub) = matches.subcommand()?;
    let (thing, _) = sub.subcommand()?;
    lookup(action, thing)
}

/// What a person asked for, read before any parsing: a page of help, or a command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Asked {
    /// `focal`, `focal help`, `focal -h`: every command.
    Top,
    /// `focal ACTION`, `focal ACTION -h`, `focal help ACTION`: what it acts on.
    Action(&'static str),
    /// `focal ACTION THING -h`, `focal help ACTION THING`: one command's page.
    Command(&'static Use),
    /// A command to parse and run, or a mistake for the parser to name.
    Run,
}

impl std::fmt::Debug for Use {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.action, self.thing)
    }
}

impl PartialEq for Use {
    fn eq(&self, other: &Self) -> bool {
        self.action == other.action && self.thing == other.thing
    }
}
impl Eq for Use {}

/// What `args` asks for.
pub(crate) fn asked(args: &[OsString]) -> Asked {
    let help = |a: &OsString| a == OsStr::new("-h") || a == OsStr::new("--help");
    // Options after `--` are a program's, not focal's.
    let before_end = args
        .iter()
        .skip(1)
        .take_while(|a| a.as_os_str() != OsStr::new("--"));
    let wants_help = before_end.clone().any(help);
    let (action, thing) = words(args);
    let word = |at: Option<usize>| at.and_then(|i| args.get(i)).and_then(|a| a.to_str());
    let (first, second) = (word(action), word(thing));
    if first == Some("help") {
        // `focal help ACTION [THING]`: the words after `help`.
        let rest: Vec<&str> = args
            .iter()
            .skip(1)
            .filter_map(|a| a.to_str())
            .filter(|a| !a.starts_with('-') && *a != "help")
            .collect();
        return match rest.as_slice() {
            [] => Asked::Top,
            [a] => known_action(a).map_or(Asked::Run, Asked::Action),
            [a, t, ..] => lookup(a, t).map_or(Asked::Run, Asked::Command),
        };
    }
    match (first, second) {
        (None, _) => {
            // No action: help, or the version, which the parser answers.
            let version = before_end.clone().any(|a| a == "-V" || a == "--version");
            if version { Asked::Run } else { Asked::Top }
        }
        (Some(a), None) => known_action(a).map_or(Asked::Run, Asked::Action),
        (Some(a), Some(t)) => match lookup(a, t) {
            Some(u) if wants_help => Asked::Command(u),
            Some(_) => Asked::Run,
            None if wants_help => known_action(a).map_or(Asked::Run, Asked::Action),
            None => Asked::Run,
        },
    }
}

fn known_action(word: &str) -> Option<&'static str> {
    actions().find(|a| *a == word)
}

#[cfg(test)]
#[path = "grammar_tests.rs"]
mod tests;
