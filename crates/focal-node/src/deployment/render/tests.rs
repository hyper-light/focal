use super::{
    MissingInput,
    kubernetes::{self, KubernetesRequest},
    systemd::{self, SystemdRequest},
};
use crate::config::{FailureDomain, Settings};

fn settings(survive: FailureDomain, max_failures: u16) -> Settings {
    let mut settings = Settings::default();
    settings.durability.survive = survive;
    settings.durability.max_failures = max_failures;
    settings.topology.region = Some("eu-a".into());
    settings
}
fn file<'a>(assets: &'a super::RenderedAssets, name: &str) -> &'a str {
    &assets
        .files
        .iter()
        .find(|file| file.name == name)
        .unwrap_or_else(|| panic!("{name} rendered"))
        .content
}

#[test]
fn node_survival_renders_a_founder_and_a_host_set_and_names_what_is_missing() {
    let settings = settings(FailureDomain::Node, 1);
    let assets = kubernetes::render(&settings, &KubernetesRequest::default(), "0.1.0").unwrap();
    let names: Vec<&str> = assets.files.iter().map(|file| file.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "configmap.yaml",
            "service.yaml",
            "statefulset-focal-founder.yaml",
            "statefulset-focal-hosts.yaml",
            "pdb.yaml",
            "kustomization.yaml",
            "invitations.sh"
        ]
    );
    assert!(
        matches!(
            assets.missing.as_slice(),
            [
                MissingInput::Image { placeholder },
                MissingInput::StorageClass,
                MissingInput::InvitationSecret { secret, invitations, .. }
            ] if placeholder == "focal:0.1.0" && secret == "focal-invitations" && invitations == &["focal-hosts-0", "focal-hosts-1"]
        ),
        "{:?}",
        assets.missing
    );
    let hosts = file(&assets, "statefulset-focal-hosts.yaml");
    assert!(hosts.contains("replicas: 2"));
    assert!(hosts.contains("--invite-file"));
    assert!(hosts.contains("secretName: focal-invitations\n            defaultMode: 288\n"));
    assert!(hosts.contains("topologySpreadConstraints"));
    assert!(!hosts.contains("storageClassName"));
    assert!(hosts.contains("image: focal:0.1.0"));
    let founder = file(&assets, "statefulset-focal-founder.yaml");
    assert!(founder.contains("replicas: 1"));
    assert!(!founder.contains("--invite-file"));
    assert!(founder.contains("\"$(POD_NAME).focal.$(POD_NAMESPACE).svc.cluster.local:7443\""));
    assert!(founder.contains("prepare-volume"));
    assert!(founder.contains("\"--check\", \"alive\""));
    assert!(file(&assets, "pdb.yaml").contains("maxUnavailable: 1"));
    assert!(file(&assets, "service.yaml").contains("clusterIP: None"));
    assert!(file(&assets, "service.yaml").contains("protocol: UDP"));
    assert!(file(&assets, "invitations.sh").contains("--output - > \"$host.invite\""));
    // The configuration a set ships is the requested policy and reads back.
    let map = file(&assets, "configmap.yaml");
    assert!(map.contains("  focal-hosts.yaml: |\n    version: 1\n    topology:\n      region: \"eu-a\"\n    durability:\n      survive: node\n      max_failures: 1\n"));
    let shipped = Settings::from_yaml(
        "version: 1\ntopology:\n  region: \"eu-a\"\ndurability:\n  survive: node\n  max_failures: 1\n",
    )
    .unwrap();
    assert_eq!(shipped.policy_intent(), settings.policy_intent());
    // The same inputs render the same bytes.
    let again = kubernetes::render(&settings, &KubernetesRequest::default(), "0.1.0").unwrap();
    assert_eq!(again, assets);
}

#[test]
fn zone_survival_needs_its_zones_then_places_one_set_per_zone_with_affinity() {
    let settings = settings(FailureDomain::Zone, 1);
    let short = kubernetes::render(
        &settings,
        &KubernetesRequest {
            zones: vec!["a".into(), "b".into()],
            ..KubernetesRequest::default()
        },
        "0.1.0",
    )
    .unwrap();
    assert!(short.files.is_empty());
    assert_eq!(
        short.missing,
        vec![MissingInput::Zones {
            needed: 3,
            given: 2
        }]
    );
    let assets = kubernetes::render(
        &settings,
        &KubernetesRequest {
            zones: vec!["a".into(), "b".into(), "c".into()],
            image: Some("registry.example/focal:0.1.0".into()),
            storage_class: Some("fast".into()),
            secret: Some("invites".into()),
            nodes: Some(4),
            ..KubernetesRequest::default()
        },
        "0.1.0",
    )
    .unwrap();
    assert!(assets.missing.is_empty(), "{:?}", assets.missing);
    let names: Vec<&str> = assets.files.iter().map(|file| file.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "configmap.yaml",
            "service.yaml",
            "statefulset-focal-founder.yaml",
            "statefulset-focal-a.yaml",
            "statefulset-focal-b.yaml",
            "statefulset-focal-c.yaml",
            "pdb.yaml",
            "kustomization.yaml",
            "invitations.sh"
        ]
    );
    // Four nodes over three zones: the founder plus one host in `a`.
    assert!(file(&assets, "statefulset-focal-a.yaml").contains("replicas: 1"));
    assert!(file(&assets, "statefulset-focal-founder.yaml").contains("- \"a\""));
    assert!(file(&assets, "statefulset-focal-b.yaml").contains("topology.kubernetes.io/zone"));
    assert!(file(&assets, "statefulset-focal-b.yaml").contains("storageClassName: fast"));
    assert!(file(&assets, "statefulset-focal-b.yaml").contains("secretName: invites"));
    assert!(file(&assets, "configmap.yaml").contains("  focal-c.yaml: |\n    version: 1\n    topology:\n      region: \"eu-a\"\n      zone: \"c\"\n"));
    assert!(file(&assets, "invitations.sh").contains("HOSTS=\"focal-a-0 focal-b-0 focal-c-0\""));
}

#[test]
fn region_survival_and_bad_inputs_are_refused_by_name() {
    let region = kubernetes::render(
        &settings(FailureDomain::Region, 1),
        &KubernetesRequest::default(),
        "0.1.0",
    )
    .unwrap_err();
    assert!(region.to_string().contains("one deployment per region"));
    for (request, needle) in [
        (
            KubernetesRequest {
                namespace: "Focal".into(),
                ..KubernetesRequest::default()
            },
            "--namespace",
        ),
        (
            KubernetesRequest {
                nodes: Some(2),
                ..KubernetesRequest::default()
            },
            "at least 3 nodes",
        ),
        (
            KubernetesRequest {
                zones: vec!["a".into()],
                ..KubernetesRequest::default()
            },
            "--zone applies",
        ),
        (
            KubernetesRequest {
                volume: "20 Gi".into(),
                ..KubernetesRequest::default()
            },
            "--volume",
        ),
    ] {
        let error =
            kubernetes::render(&settings(FailureDomain::Node, 1), &request, "0.1.0").unwrap_err();
        assert!(error.to_string().contains(needle), "{error}");
    }
}

#[test]
fn the_systemd_unit_runs_the_node_under_its_user_and_ships_the_configuration() {
    let mut settings = settings(FailureDomain::Node, 0);
    let no_address = systemd::render(&settings, &SystemdRequest::default()).unwrap();
    assert_eq!(no_address.missing, vec![MissingInput::Advertise]);
    settings.node.advertise = Some("host-1.example:7443".into());
    settings.node.metrics_listen = Some("127.0.0.1:9464".parse().unwrap());
    let assets = systemd::render(
        &settings,
        &SystemdRequest {
            invite_file: Some("/etc/focal/join.invite".into()),
            ..SystemdRequest::default()
        },
    )
    .unwrap();
    assert!(assets.missing.is_empty());
    let unit = file(&assets, "focal.service");
    assert!(unit.contains(
        "ExecStart=/usr/local/bin/focal --config /etc/focal/focal.yaml --data-dir /var/lib/focal start --invite-file /etc/focal/join.invite\n"
    ));
    assert!(unit.contains("User=focal\nGroup=focal\n"));
    assert!(unit.contains("TimeoutStopSec=45\n"));
    assert!(unit.contains("StateDirectory=focal\n"));
    assert!(unit.contains("ConfigurationDirectory=focal\n"));
    assert!(unit.contains("CapabilityBoundingSet=\n"));
    let config = file(&assets, "focal.yaml");
    assert_eq!(
        config,
        "version: 1\nnode:\n  advertise: \"host-1.example:7443\"\n  metrics_listen: \"127.0.0.1:9464\"\ntopology:\n  region: \"eu-a\"\ndurability:\n  survive: node\n  max_failures: 0\n"
    );
    let shipped = Settings::from_yaml(config).unwrap();
    assert_eq!(
        shipped.node.advertise.as_deref(),
        Some("host-1.example:7443")
    );
    assert_eq!(shipped.policy_intent(), settings.policy_intent());
    // A data directory outside /var/lib is a read-write path with a note.
    let custom = systemd::render(
        &settings,
        &SystemdRequest {
            data_dir: "/srv/focal".into(),
            ..SystemdRequest::default()
        },
    )
    .unwrap();
    assert!(file(&custom, "focal.service").contains("ReadWritePaths=/srv/focal\n"));
    assert!(
        custom
            .notes
            .iter()
            .any(|note| note.contains("prepare-volume"))
    );
    let bad = systemd::render(
        &settings,
        &SystemdRequest {
            user: "Focal User".into(),
            ..SystemdRequest::default()
        },
    )
    .unwrap_err();
    assert!(bad.to_string().contains("--user"));
}
