use super::*;
use std::path::Path;

fn yaml(text: &str) -> (Settings, resolve::FilePresence) {
    (
        Settings::from_yaml(text).unwrap(),
        resolve::FilePresence::of(text).unwrap(),
    )
}
fn write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    std::fs::write(path, bytes)
}

#[test]
fn an_unknown_key_fails_by_its_full_path_before_the_typed_parse() {
    assert!(check_unknown_keys("version: 1\nnode:\n  advertise: a:1").is_ok());
    for (text, path) in [
        ("version: 1\nshards: 3", "shards"),
        ("version: 1\nnode:\n  shards: 3", "node.shards"),
        ("version: 1\ndurability:\n  survive: node\n  replicas: 3", "durability.replicas"),
        ("version: 1\nplacement:\n  regions: [a]", "placement.regions"),
    ] {
        match check_unknown_keys(text) {
            Err(ConfigError::UnknownKey { path: found }) => assert_eq!(found, path, "{text}"),
            other => panic!("{text}: {other:?}"),
        }
    }
}

#[test]
fn the_schema_and_the_typed_settings_declare_the_same_fields() {
    let default = serde_json::to_value(Settings::default()).unwrap();
    let root: Vec<String> = default.as_object().unwrap().keys().cloned().collect();
    assert_eq!(schema::properties(&[]).unwrap(), root);
    for section in ["node", "topology", "durability", "placement"] {
        let fields: Vec<String> = default[section]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let declared = schema::properties(&[section]).unwrap();
        let mut sorted = fields.clone();
        sorted.sort();
        let mut declared_sorted = declared.clone();
        declared_sorted.sort();
        assert_eq!(declared_sorted, sorted, "{section}");
    }
}

#[test]
fn precedence_is_command_line_then_file_then_creation_default_and_every_value_names_its_source() {
    let (file, presence) = yaml("version: 1\nnode:\n  advertise: a:1\ndurability:\n  max_failures: 1\ntopology:\n  zone: z");
    let overrides = CliOverrides {
        data_dir: Some(Path::new("/tmp/x").to_owned()),
        advertise: Some("b:2".into()),
        listen: None,
    };
    let resolved = resolve(&overrides, Some((&file, &presence)), None).unwrap();
    assert_eq!(resolved.settings.node.advertise.as_deref(), Some("b:2"));
    assert_eq!(resolved.settings.node.data_dir.as_deref(), Some(Path::new("/tmp/x")));
    assert_eq!(resolved.settings.durability.max_failures, 1);
    assert_eq!(resolved.sources["node.advertise"], ConfigSource::CommandLine);
    assert_eq!(resolved.sources["node.data_dir"], ConfigSource::CommandLine);
    assert_eq!(resolved.sources["durability.max_failures"], ConfigSource::File);
    assert_eq!(resolved.sources["topology.zone"], ConfigSource::File);
    assert_eq!(resolved.sources["durability.survive"], ConfigSource::CreationDefault);
    assert_eq!(resolved.sources["placement.residency"], ConfigSource::CreationDefault);
    assert!(resolved.committed.is_none());
}

#[test]
fn a_committed_policy_wins_over_omitted_fields_and_refuses_a_conflicting_file_by_name() {
    let committed = CommittedPolicy {
        revision: PolicyRevision(3),
        intent: PolicyIntent {
            durability: Durability {
                survive: FailureDomain::Zone,
                max_failures: 1,
            },
            placement: Placement {
                home_regions: vec!["a".into()],
                residency: vec!["a".into(), "b".into()],
            },
        },
        hash: [0; 32],
    };
    // Omitted policy fields keep the committed values, never the creation
    // defaults, and say so.
    let (file, presence) = yaml("version: 1\nnode:\n  advertise: a:1");
    let resolved = resolve(&CliOverrides::default(), Some((&file, &presence)), Some(&committed))
        .unwrap();
    assert_eq!(resolved.settings.durability, committed.intent.durability);
    assert_eq!(resolved.settings.placement, committed.intent.placement);
    assert_eq!(resolved.sources["durability.survive"], ConfigSource::Committed(3));
    assert_eq!(resolved.sources["placement.residency"], ConfigSource::Committed(3));
    // The same value restated is fine; a different one is refused by field.
    let (same, presence) = yaml("version: 1\ndurability:\n  survive: zone\n  max_failures: 1\nplacement:\n  home_regions: [a]\n  residency: [a, b]");
    let resolved = resolve(&CliOverrides::default(), Some((&same, &presence)), Some(&committed))
        .unwrap();
    assert_eq!(resolved.sources["durability.survive"], ConfigSource::File);
    let (changed, presence) = yaml("version: 1\ndurability:\n  survive: zone\n  max_failures: 2");
    assert!(matches!(
        resolve(&CliOverrides::default(), Some((&changed, &presence)), Some(&committed)),
        Err(ConfigError::CommittedPolicyChange {
            field: "durability.max_failures"
        })
    ));
    let (changed, presence) = yaml("version: 1\nplacement:\n  residency: [a]");
    assert!(matches!(
        resolve(&CliOverrides::default(), Some((&changed, &presence)), Some(&committed)),
        Err(ConfigError::CommittedPolicyChange {
            field: "placement.residency"
        })
    ));
    // A policy request keeps the file's values as the request and still
    // fills what it omits from the committed policy.
    let requested = resolve_request(
        &CliOverrides::default(),
        Some((&changed, &presence)),
        Some(&committed),
    )
    .unwrap();
    assert_eq!(requested.settings.placement.residency, vec!["a".to_owned()]);
    assert_eq!(requested.settings.placement.home_regions, vec!["a".to_owned()]);
    assert_eq!(requested.settings.durability, committed.intent.durability);
    assert_eq!(requested.sources["placement.residency"], ConfigSource::File);
    assert_eq!(requested.sources["durability.survive"], ConfigSource::Committed(3));
}

#[test]
fn the_committed_policy_file_keeps_its_original_form_and_advances_by_revision() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    assert_eq!(policy::read_committed(root).unwrap(), None);
    let intent = Settings::default().policy_intent();
    let first = policy::install_or_check(root, &intent, write).unwrap();
    assert_eq!(first.revision, PolicyRevision(1));
    assert_eq!(first.hash, intent.hash().unwrap());
    assert!(root.join("POLICY.initialized").is_file());
    assert_eq!(policy::read_committed(root).unwrap(), Some(first.clone()));
    // The same intent checks; a differing one is refused and not written.
    let bytes = std::fs::read(root.join("POLICY")).unwrap();
    assert_eq!(policy::install_or_check(root, &intent, write).unwrap(), first);
    let mut other = intent.clone();
    other.placement.residency = vec!["a".into()];
    assert!(matches!(
        policy::install_or_check(root, &other, write),
        Err(ConfigError::CommittedPolicyChange {
            field: "placement.residency"
        })
    ));
    assert_eq!(std::fs::read(root.join("POLICY")).unwrap(), bytes);
    // Plan and apply commit the next revision under the expected one.
    assert!(policy::commit(root, &other, PolicyRevision(2), write).is_err());
    let second = policy::commit(root, &other, PolicyRevision(1), write).unwrap();
    assert_eq!(second.revision, PolicyRevision(2));
    assert_eq!(second.intent, other);
    assert_eq!(policy::read_committed(root).unwrap(), Some(second.clone()));
    assert_eq!(policy::install_or_check(root, &other, write).unwrap(), second);
    // The original bare pair reads as revision 1 with the same hash rule.
    let legacy = postcard::to_stdvec(&(&intent.durability, &intent.placement)).unwrap();
    let decoded = CommittedPolicy::decode(&legacy).unwrap();
    assert_eq!(decoded.revision, PolicyRevision(1));
    assert_eq!(decoded.intent, intent);
    assert_eq!(decoded.hash, intent.hash().unwrap());
    // A lost policy beside an existing store is never recreated.
    std::fs::remove_file(root.join("POLICY")).unwrap();
    assert!(matches!(
        policy::read_committed(root),
        Err(ConfigError::PolicyMissing)
    ));
    let fresh = tempfile::tempdir().unwrap();
    std::fs::create_dir(fresh.path().join("wal")).unwrap();
    assert!(matches!(
        policy::install_or_check(fresh.path(), &intent, write),
        Err(ConfigError::PolicyMissing)
    ));
}

#[test]
fn metrics_listen_is_loopback_only() {
    assert!(Settings::from_yaml("version: 1\nnode:\n  metrics_listen: 127.0.0.1:9100").is_ok());
    assert!(Settings::from_yaml("version: 1\nnode:\n  metrics_listen: 0.0.0.0:9100").is_err());
}
