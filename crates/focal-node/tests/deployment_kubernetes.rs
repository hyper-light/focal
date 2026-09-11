#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Stage 3, Kubernetes (08 §5, DC07, DC08): the render command emits plain
//! manifests from a one-node and a fleet configuration with no operator or
//! CRD prerequisite, a durable volume per identity, and secret references
//! rather than plaintext credentials, naming the facts it lacks; and a
//! VM-to-Kubernetes migration (local processes standing in for pods) adds a
//! member, catches it up and keeps the application demo and its receipts
//! unchanged. Built on the fleet stage; it adds the render command and its
//! inputs. `helm template` and a real cluster run are recorded as not
//! executed.
use serde_json::Value;
use std::time::Duration;

#[path = "support/fleet.rs"]
mod fleet;
#[path = "support/journey.rs"]
mod journey;
use fleet::*;
use journey::*;

/// Render into a fresh directory from a config; the result and the output dir.
fn render(
    journey: &mut Journey,
    node: &Node,
    concept: &str,
    inputs: &[&str],
    args: &[&str],
) -> (Value, std::path::PathBuf) {
    let output = node.root().join("rendered");
    let mut full: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    full.push("--output".into());
    full.push(output.to_str().unwrap().to_string());
    let full_ref: Vec<&str> = full.iter().map(String::as_str).collect();
    let value = journey.admin_bare(node, concept, inputs, &full_ref);
    (value, output)
}

#[test]
fn the_kubernetes_stage_renders_plain_manifests_and_migrates_without_rewriting_history() {
    let mut journey =
        Journey::building_on("kubernetes", "deployment_kubernetes", &["laptop", "fleet"]);
    // ---- DC08: render fleet manifests (three zones). Plain manifests, a
    // volume per identity, secret references, only the storage class left to
    // the cluster.
    let render_node = Node::new("render");
    let config = render_node.root().join("kubernetes.yaml");
    std::fs::write(
        &config,
        "version: 1\ndurability:\n  survive: zone\n  max_failures: 1\n",
    )
    .unwrap();
    let (result, output) = render(
        &mut journey,
        &render_node,
        "render kubernetes",
        &["config", "namespace", "image", "invitation secret", "zones"],
        &[
            "--config",
            config.to_str().unwrap(),
            "deployment",
            "render",
            "kubernetes",
            "--namespace",
            "focal",
            "--image",
            "focal:0.1.0",
            "--secret",
            "focal-invitations",
            "--zone",
            "a",
            "--zone",
            "b",
            "--zone",
            "c",
        ],
    );
    assert_eq!(result["result"]["target"], "kubernetes", "{result}");
    assert_eq!(
        result["result"]["missing"],
        serde_json::json!([{"input": "storage_class"}]),
        "only the storage class is left to the cluster: {result}"
    );
    // Plain manifests: StatefulSets, a headless Service, a disruption budget,
    // a ConfigMap and the invitation script — no CustomResourceDefinition.
    let files: Vec<String> = std::fs::read_dir(&output)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        files.iter().any(|f| f.starts_with("statefulset-")),
        "{files:?}"
    );
    assert!(files.contains(&"service.yaml".to_string()), "{files:?}");
    assert!(files.contains(&"pdb.yaml".to_string()), "{files:?}");
    let all: String = files
        .iter()
        .map(|f| std::fs::read_to_string(output.join(f)).unwrap())
        .collect();
    assert!(
        !all.contains("CustomResourceDefinition"),
        "no CRD prerequisite"
    );
    assert!(
        all.contains("volumeClaimTemplates"),
        "a volume per identity"
    );
    assert!(
        all.contains("secretName: focal-invitations"),
        "secret references"
    );
    // No plaintext credential: an invitation token is base64url with no
    // padding; the manifests carry only the secret's name. The invitation
    // script installs the secret at deploy time.
    assert!(
        !all.contains("kind: Secret") || !all.contains("token:"),
        "no plaintext token: {output:?}"
    );
    journey.manual(
        "render kubernetes",
        &["config", "namespace", "image", "invitation secret", "zones"],
        &[
            "focal",
            "--config",
            "<config>",
            "deployment",
            "render",
            "kubernetes",
            "--namespace",
            "focal",
            "--image",
            "<image>",
            "--secret",
            "<secret>",
            "--zone",
            "<zone>",
            "--output",
            "<dir>",
        ],
    );
    // ---- DC07: a migration standing in with local processes. The founder
    // serves the demo; a new member (a "pod") joins, catches up, and the
    // demo and its receipts are unchanged. Killing the joiner mid-catch-up
    // and restarting it resumes without rewriting history.
    let founder = Node::new("founder");
    let host = Node::new("host");
    let addresses: Vec<String> = (0..3).map(|_| address()).collect();
    journey.admin(
        &founder,
        "native engine",
        &["data directory"],
        &["cluster", "replicas", "activate-native"],
    );
    let _founder_server = journey.start(
        &founder,
        "start",
        &["data directory", "address"],
        &["--advertise", &addresses[0]],
    );
    let (founder_node, _tenant, ledger) = identity(&founder);
    let demo = journey.enroll(&founder, "alice");
    let token = Journey::client_token(&demo);
    let before = journey.demo(&founder, &demo, "before the migration");
    // A joiner is killed mid-catch-up, then restarted: it resumes.
    let invite = journey.invite(&founder, &host, "host");
    let (mut child, _rx) = fleet::spawn(
        &host,
        &[
            "--advertise",
            &addresses[1],
            "--invite-file",
            invite.to_str().unwrap(),
        ],
        &[],
    );
    std::thread::sleep(Duration::from_millis(400));
    child.kill().unwrap();
    child.wait().unwrap();
    journey.manual("crash", &[], &["kill", "-KILL", "<joining pod>"]);
    let (_server, node) = {
        let server = journey.start(
            &host,
            "start",
            &["invitation file", "address"],
            &[
                "--advertise",
                &addresses[1],
                "--invite-file",
                invite.to_str().unwrap(),
            ],
        );
        let id = fleet::identity(&host).0;
        (server, id)
    };
    wait_for(
        &founder,
        "the pod caught up",
        Duration::from_secs(120),
        |view| settled(view, &ledger, &[founder_node, node], 0),
    );
    // The demo's receipts are unchanged after the migration.
    journey.same(&founder, &before);
    let after = journey.demo(&founder, &demo, "after the migration");
    assert_ne!(after.claim, before.claim);
    journey.assert_redacted(&[&token]);
    journey.not_executed(
        "a run on a real Kubernetes cluster and `helm template`: neither a cluster nor helm is available here; the manifests are byte-checked against the renderer in deployment_render, and pods stand in with local processes",
    );
    journey.finish();
}
