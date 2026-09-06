use super::*;
use clap::{FromArgMatches, ValueEnum};
use clap_complete::{Generator, Shell};
use focal_client::input::BuildContext;
use focal_model::{LedgerId, ParticipantId, RootCommandId, SessionId, TenantId};

const ID: &str = "00000000000000000000000000000001";
const HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn context() -> BuildContext {
    BuildContext {
        ledger: LedgerId {
            tenant: TenantId([1; 16]),
            session: SessionId([2; 16]),
        },
        actor: ParticipantId([3; 16]),
        root: RootCommandId([4; 16]),
        policy_revision: 1,
    }
}

fn samples() -> [(&'static str, &'static str, &'static str); 20] {
    [
        ("claim", "claim", ID),
        ("testament", "testament", ID),
        ("source", "source", "self"),
        ("target", "target", "self"),
        ("status", "status", "posted"),
        ("action", "action", "work"),
        ("producer", "producer", "self"),
        ("kind", "kind", "test"),
        ("schema_hash", "schema-hash", HASH),
        ("evaluator", "evaluator", "self"),
        ("phase", "phase", "whole_work"),
        ("mode", "mode", "required"),
        ("scopes", "scope", "file:src/lib.rs"),
        ("relations", "relation", "issuer=participant:self"),
        (
            "caused_by",
            "caused-by",
            "root:00000000000000000000000000000001",
        ),
        (
            "inputs",
            "input",
            "artifact:00000000000000000000000000000001",
        ),
        ("outcome", "outcome", "complete"),
        ("confidence", "confidence", "committed"),
        ("created_after", "created-after", "1"),
        ("created_through", "created-through", "10"),
    ]
}

#[test]
fn family_help_and_completions_match_shared_filter_admission() {
    command().debug_assert();
    completion_command().debug_assert();
    for (name, kind) in [
        ("claims", ObjectKind::Claim),
        ("testaments", ObjectKind::Testament),
        ("artifacts", ObjectKind::Artifact),
        ("validations", ObjectKind::Validation),
    ] {
        for (id, flag, value) in samples() {
            // Unsupported filters must still parse, then fail through the same
            // authored builder used by CLI, SDK and MCP documents.
            let mut matches = command()
                .try_get_matches_from(["focal", "list", name, &format!("--{flag}"), value])
                .unwrap();
            let parsed = crate::Args::from_arg_matches_mut(&mut matches).unwrap();
            let crate::Commands::Manual(manual) = parsed.command else {
                panic!("manual list command expected");
            };
            let super::super::args::Commands::List { command: list } = *manual else {
                panic!("list command expected");
            };
            use super::super::args::ListCommand;
            let args = match list {
                ListCommand::Claims(args)
                | ListCommand::Testaments(args)
                | ListCommand::Artifacts(args)
                | ListCommand::Validations(args) => args,
            };
            let accepted = super::super::authored::filters(args.filters)
                .build_operation(kind, &context())
                .is_ok();
            let tree = command();
            let leaf = tree
                .find_subcommand("list")
                .unwrap()
                .find_subcommand(name)
                .unwrap();
            let argument = leaf.get_arguments().find(|arg| arg.get_id() == id).unwrap();
            assert_eq!(!argument.is_hide_set(), accepted, "{name} --{flag}");
            assert_eq!(
                leaf.clone()
                    .render_long_help()
                    .to_string()
                    .contains(&format!("--{flag} ")),
                accepted,
                "help: {name} --{flag}",
            );
            let completion = completion_command();
            let projected = completion
                .find_subcommand("list")
                .unwrap()
                .find_subcommand(name)
                .unwrap();
            assert_eq!(
                projected.get_arguments().any(|arg| arg.get_id() == id),
                accepted,
                "completion: {name} --{flag}",
            );
        }
    }
}

#[test]
fn all_shells_omit_hidden_family_filters_and_preserve_claim_selection() {
    for (parent, name) in [
        ("list", "claims"),
        ("list", "testaments"),
        ("list", "artifacts"),
        ("list", "validations"),
        ("get", "claim"),
    ] {
        let tree = command();
        let leaf = tree
            .find_subcommand(parent)
            .unwrap()
            .find_subcommand(name)
            .unwrap();
        // A future command alias or cross-argument completion constraint must
        // extend the visible projection before it can silently disappear.
        assert_eq!(leaf.get_all_aliases().count(), 0);
        assert_eq!(leaf.get_subcommands().count(), 0);
        assert!(leaf.get_short_flag().is_none());
        assert!(leaf.get_long_flag().is_none());
        for mut group in leaf.get_groups().cloned() {
            assert!(!group.is_required_set());
            assert!(group.is_multiple());
        }
        for argument in leaf.get_arguments() {
            assert!(leaf.get_arg_conflicts_with(argument).is_empty());
        }
        let completion = completion_command();
        let projected = completion
            .find_subcommand(parent)
            .unwrap()
            .find_subcommand(name)
            .unwrap();
        for shell in Shell::value_variants() {
            // Use the same visible leaf and generator as the released full-tree
            // command, isolated so flags from other families cannot mask errors.
            let mut projected = projected.clone().bin_name("focal");
            projected.build();
            let mut bytes = Vec::new();
            shell.try_generate(&projected, &mut bytes).unwrap();
            let script = String::from_utf8(bytes).unwrap();
            for (id, flag, _) in samples() {
                let argument = leaf.get_arguments().find(|arg| arg.get_id() == id).unwrap();
                let suggested = script.contains(&format!("--{flag}"))
                    || script.contains(&format!("-l {flag} "));
                assert_eq!(
                    suggested,
                    !argument.is_hide_set(),
                    "{shell:?} {parent} {name} --{flag}"
                );
            }
        }
    }
}
