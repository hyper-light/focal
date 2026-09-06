//! One derived command tree for parsing, help and completion discovery.
use clap::{Command, CommandFactory};
use focal_model::ObjectKind;

pub(crate) fn command() -> Command {
    crate::Args::command().mut_subcommands(|command| match command.get_name() {
        "list" => command.mut_subcommands(|command| {
            let family = match command.get_name() {
                "claims" => Some(ObjectKind::Claim),
                "testaments" => Some(ObjectKind::Testament),
                "artifacts" => Some(ObjectKind::Artifact),
                "validations" => Some(ObjectKind::Validation),
                _ => None,
            };
            match family {
                Some(family) => filter_help(command, family),
                None => command,
            }
        }),
        "get" => command.mut_subcommands(|command| {
            if command.get_name() == "claim" {
                filter_help(command, ObjectKind::Claim)
            } else {
                command
            }
        }),
        _ => command,
    })
}

fn filter_help(command: Command, family: ObjectKind) -> Command {
    command.mut_args(|arg| {
        let supported = match arg.get_id().as_str() {
            "source" | "target" | "status" | "action" | "scopes" | "relations" | "caused_by" => {
                family == ObjectKind::Claim
            }
            "testament" | "producer" | "schema_hash" | "inputs" => family == ObjectKind::Artifact,
            "kind" => matches!(family, ObjectKind::Artifact | ObjectKind::Validation),
            "evaluator" | "phase" | "mode" => family == ObjectKind::Validation,
            "outcome" | "confidence" => family == ObjectKind::Testament,
            _ => true,
        };
        // Keep the full parser: unsupported flags reach the shared authored
        // validator and retain its typed diagnostic, including JSON/YAML output.
        if supported { arg } else { arg.hide(true) }
    })
}

pub(super) fn completion_command() -> Command {
    // Pinned clap_complete ignores Arg::hide. Project only the visible arguments
    // of these five leaves from the actual parser tree. There is no second flag
    // inventory: names, aliases, descriptions, value parsers and arity are cloned
    // from the same arguments used by main and help.
    command().mut_subcommands(|command| match command.get_name() {
        "list" => command.mut_subcommands(|command| match command.get_name() {
            "claims" => visible_leaf(command, "claims"),
            "testaments" => visible_leaf(command, "testaments"),
            "artifacts" => visible_leaf(command, "artifacts"),
            "validations" => visible_leaf(command, "validations"),
            _ => command,
        }),
        "get" => command.mut_subcommands(|command| {
            if command.get_name() == "claim" {
                visible_leaf(command, "claim")
            } else {
                command
            }
        }),
        _ => command,
    })
}

fn visible_leaf(command: Command, name: &'static str) -> Command {
    // These flat leaves have no command aliases, required/exclusive groups or
    // cross-argument constraints; tests guard that projection boundary. Their
    // derive-only groups reference hidden arguments and are irrelevant to
    // completion, so they are not carried into this visible projection.
    let mut visible = Command::new(name).args(
        command
            .get_arguments()
            .filter(|arg| !arg.is_hide_set())
            .cloned(),
    );
    if let Some(about) = command.get_about() {
        visible = visible.about(about.clone());
    }
    if let Some(about) = command.get_long_about() {
        visible = visible.long_about(about.clone());
    }
    visible
}

#[cfg(test)]
#[path = "command_tree_tests.rs"]
mod tests;
