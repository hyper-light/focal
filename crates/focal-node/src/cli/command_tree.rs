//! One derived command tree for parsing, help and completion discovery.
use clap::{Command, CommandFactory};
use focal_model::ObjectKind;

pub(crate) fn command() -> Command {
    crate::Args::command().mut_subcommands(|command| match command.get_name() {
        "list" => command.mut_subcommands(|command| {
            let family = match command.get_name() {
                "claims" => Some(ListFamily::Shared(ObjectKind::Claim)),
                "testaments" => Some(ListFamily::Shared(ObjectKind::Testament)),
                "artifacts" => Some(ListFamily::Shared(ObjectKind::Artifact)),
                "validations" => Some(ListFamily::Shared(ObjectKind::Validation)),
                "evaluations" => Some(ListFamily::Evaluations),
                "receipts" => Some(ListFamily::Receipts),
                "monitors" => Some(ListFamily::Monitors),
                "events" => Some(ListFamily::Events),
                _ => None,
            };
            match family {
                Some(family) => filter_help(command, family),
                None => command,
            }
        }),
        "get" => command.mut_subcommands(|command| {
            if command.get_name() == "claim" {
                filter_help(command, ListFamily::Shared(ObjectKind::Claim))
            } else {
                command
            }
        }),
        _ => command,
    })
}

/// The list leaves: the four families both engines serve and the four the
/// native engine adds (doc 22 §7).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ListFamily {
    Shared(ObjectKind),
    Evaluations,
    Receipts,
    Monitors,
    Events,
}

fn filter_help(command: Command, family: ListFamily) -> Command {
    command.mut_args(|arg| {
        let shared = match family {
            ListFamily::Shared(kind) => Some(kind),
            _ => None,
        };
        let supported = match arg.get_id().as_str() {
            "source" | "target" | "status" | "action" | "scopes" | "relations" | "caused_by" => {
                shared == Some(ObjectKind::Claim)
            }
            "created_after" | "created_through" => shared.is_some(),
            "testament" | "producer" | "schema_hash" | "inputs" => {
                shared == Some(ObjectKind::Artifact)
            }
            "kind" => matches!(shared, Some(ObjectKind::Artifact | ObjectKind::Validation)),
            "evaluator" => {
                shared == Some(ObjectKind::Validation) || family == ListFamily::Evaluations
            }
            "phase" | "mode" => shared == Some(ObjectKind::Validation),
            "outcome" | "confidence" => shared == Some(ObjectKind::Testament),
            "claim" => {
                shared.is_some()
                    || matches!(
                        family,
                        ListFamily::Evaluations | ListFamily::Receipts | ListFamily::Monitors
                    )
            }
            "validation" | "verdict" => family == ListFamily::Evaluations,
            "holder" => family == ListFamily::Receipts,
            "after" => family == ListFamily::Events,
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
            "evaluations" => visible_leaf(command, "evaluations"),
            "receipts" => visible_leaf(command, "receipts"),
            "monitors" => visible_leaf(command, "monitors"),
            "events" => visible_leaf(command, "events"),
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
