use super::*;
use std::collections::BTreeSet;

/// The derived commands no `ACTION THING` says: the thing-first aliases of `submit
/// artifact` and `submit testament`, which the grammar says once.
const UNSAID: [&[&str]; 2] = [&["artifact", "submit"], &["testament", "submit"]];

fn leaves(node: &Command, path: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    let mut any = false;
    for sub in node.get_subcommands() {
        any = true;
        path.push(sub.get_name().to_string());
        leaves(sub, path, out);
        path.pop();
    }
    if !any {
        out.push(path.clone());
    }
}

#[test]
fn every_derived_command_is_said_exactly_once() {
    let tree = super::super::command_tree::command();
    let mut all = Vec::new();
    leaves(&tree, &mut Vec::new(), &mut all);
    let said: Vec<Vec<String>> = USES
        .iter()
        .map(|u| u.path.iter().map(|s| s.to_string()).collect())
        .collect();
    for leaf in &all {
        let unsaid = UNSAID.iter().any(|p| p.iter().eq(leaf.iter()));
        let count = said.iter().filter(|p| *p == leaf).count();
        if unsaid {
            assert_eq!(count, 0, "{leaf:?} is an alias the grammar does not say");
        } else {
            assert_eq!(count, 1, "{leaf:?} is said {count} times");
        }
    }
    for u in USES {
        let node = find(&tree, u.path).unwrap_or_else(|| panic!("{u:?}: no {:?}", u.path));
        assert!(
            node.get_subcommands().next().is_none(),
            "{u:?}: {:?} is not a leaf",
            u.path
        );
    }
}

#[test]
fn every_command_has_one_name_a_section_and_words() {
    let mut names = BTreeSet::new();
    let tree = super::super::command_tree::command();
    for u in USES {
        assert!(names.insert((u.action, u.thing)), "{u:?} twice");
        assert!(
            SECTIONS.contains(&u.section),
            "{u:?}: section {}",
            u.section
        );
        assert!(!about(u, &tree).is_empty(), "{u:?} says nothing");
        for word in [u.action, u.thing] {
            assert!(
                word.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
                "{u:?}: {word}"
            );
        }
    }
    for section in SECTIONS {
        assert!(
            USES.iter().any(|u| u.section == section),
            "{section} is empty"
        );
    }
}

#[test]
fn the_parser_is_well_formed() {
    let mut command = command();
    command.set_bin_name("focal");
    command.build();
    command.debug_assert();
    let mut completion = completion_command();
    completion.build();
    completion.debug_assert();
}

fn tree() -> Command {
    super::super::command_tree::command()
}

fn os(words: &[&str]) -> Vec<OsString> {
    words.iter().map(OsString::from).collect()
}

#[test]
fn a_command_is_said_as_its_derived_command() {
    let args = os(&[
        "focal",
        "--data-dir",
        "post",
        "post",
        "--config",
        "c.yaml",
        "claim",
        "C1",
        "--format",
        "json",
    ]);
    assert_eq!(words(&args), (Some(3), Some(6)));
    assert_eq!(
        internal(&tree(), &args, &["claim", "post"]),
        os(&[
            "focal",
            "--data-dir",
            "post",
            "claim",
            "post",
            "--config",
            "c.yaml",
            "C1",
            "--format",
            "json"
        ])
    );
    let args = os(&["focal", "start", "node", "--listen", "127.0.0.1:1"]);
    assert_eq!(
        internal(&tree(), &args, &["start"]),
        os(&["focal", "start", "--listen", "127.0.0.1:1"])
    );
}

#[test]
fn both_parsers_agree_on_what_was_typed() {
    let cases: [&[&str]; 5] = [
        &["focal", "post", "claim", "--help"],
        &["focal", "inspect", "node", "--probe", "authoritative"],
        &[
            "focal",
            "--data-dir",
            "/d",
            "invite",
            "node",
            "--node",
            "n2",
            "--output",
            "o",
        ],
        &["focal", "list", "claims", "--format", "json"],
        &["focal", "generate", "completion", "bash"],
    ];
    for case in cases {
        let args = os(case);
        let Ok(matches) = command().try_get_matches_from(&args) else {
            // A help request: answered as a page, before the parsers.
            assert!(case.contains(&"--help"), "{case:?}");
            continue;
        };
        let u = chosen(&matches).unwrap();
        let derived = internal(&tree(), &args, u.path);
        super::super::command_tree::command()
            .try_get_matches_from(&derived)
            .unwrap_or_else(|e| panic!("{case:?} → {derived:?}: {e}"));
    }
}

#[test]
fn the_old_words_are_refused() {
    for case in [
        &["focal", "claim", "post", "C1"][..],
        &["focal", "diagnose", "node"],
        &["focal", "mcp", "serve"],
        &["focal", "cluster", "invite", "--node", "n2"],
    ] {
        assert!(
            command().try_get_matches_from(os(case)).is_err(),
            "{case:?}"
        );
    }
}

#[test]
fn help_is_read_before_parsing() {
    assert_eq!(asked(&os(&["focal"])), Asked::Top);
    assert_eq!(asked(&os(&["focal", "-h"])), Asked::Top);
    assert_eq!(asked(&os(&["focal", "help"])), Asked::Top);
    assert_eq!(asked(&os(&["focal", "post"])), Asked::Action("post"));
    assert_eq!(
        asked(&os(&["focal", "post", "--help"])),
        Asked::Action("post")
    );
    assert_eq!(
        asked(&os(&["focal", "help", "post"])),
        Asked::Action("post")
    );
    let post = lookup("post", "claim").unwrap();
    assert_eq!(
        asked(&os(&["focal", "post", "claim", "-h"])),
        Asked::Command(post)
    );
    assert_eq!(
        asked(&os(&["focal", "help", "post", "claim"])),
        Asked::Command(post)
    );
    assert_eq!(asked(&os(&["focal", "post", "claim", "C1"])), Asked::Run);
    assert_eq!(asked(&os(&["focal", "--version"])), Asked::Run);
    assert_eq!(asked(&os(&["focal", "nonsense"])), Asked::Run);
    // A program's own `--help`, after `--`, is not focal's.
    assert_eq!(
        asked(&os(&["focal", "run", "code", "--", "--help"])),
        Asked::Run
    );
}

#[test]
fn every_group_of_several_things_says_what_it_does() {
    let tree = super::super::command_tree::command();
    for section in SECTIONS {
        let rows = groups(section, &tree);
        let mut actions: Vec<&str> = rows.iter().map(|g| g.action).collect();
        let sorted = {
            let mut a = actions.clone();
            a.sort_unstable();
            a
        };
        assert_eq!(actions, sorted, "{section} is alphabetical");
        actions.dedup();
        assert_eq!(actions.len(), rows.len(), "{section}: one row an action");
        for group in rows {
            assert!(!group.about.is_empty(), "{section} {}", group.action);
        }
    }
    for (section, action, _) in GROUP_ABOUTS {
        let things = USES
            .iter()
            .filter(|u| u.section == *section && u.action == *action)
            .count();
        assert!(things > 1, "{section} {action}: a summary for one thing");
    }
}

#[test]
fn an_intermediate_commands_option_is_the_leafs_and_moves_to_its_word() {
    // `--session` belongs to the derived `cluster replicas membership`: typed after
    // `add replica-learner`, it is said after `membership`, before `add-learner`.
    let args = os(&[
        "focal",
        "add",
        "replica-learner",
        "--node",
        "7",
        "--session",
        "ab",
        "--expected-configuration-index",
        "0",
    ]);
    let typed = command().try_get_matches_from(&args).unwrap();
    let u = chosen(&typed).unwrap();
    let derived = internal(&tree(), &args, u.path);
    assert_eq!(
        derived,
        os(&[
            "focal",
            "cluster",
            "replicas",
            "membership",
            "--session",
            "ab",
            "add-learner",
            "--node",
            "7",
            "--expected-configuration-index",
            "0"
        ])
    );
    tree().try_get_matches_from(&derived).unwrap();
    let args = os(&["focal", "list", "ranges", "--session=cd"]);
    let typed = command().try_get_matches_from(&args).unwrap();
    let derived = internal(&tree(), &args, chosen(&typed).unwrap().path);
    assert_eq!(
        derived,
        os(&[
            "focal",
            "cluster",
            "replicas",
            "ranges",
            "--session=cd",
            "list"
        ])
    );
    tree().try_get_matches_from(&derived).unwrap();
}
