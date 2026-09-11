#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::disallowed_macros
)]
//! Packaging goldens (doc 08 §3, §5; 24 §24): the checked-in Kubernetes
//! manifests, systemd unit and container image are exactly what the binary
//! renders from `deploy/config`, the Helm chart names the same objects, and
//! the Dockerfile builds with the release's pinned musl image.
use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}
fn render(args: &[&str], output: &Path) -> serde_json::Value {
    let result = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(args)
        .args(["--output", output.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn compare(rendered: &Path, golden: &Path) {
    let mut names: Vec<String> = std::fs::read_dir(rendered)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut expected: Vec<String> = std::fs::read_dir(golden)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    expected.sort();
    assert_eq!(names, expected, "{} lists other files", golden.display());
    for name in &names {
        let ours = std::fs::read_to_string(rendered.join(name)).unwrap();
        let theirs = std::fs::read_to_string(golden.join(name)).unwrap();
        assert!(
            ours == theirs,
            "{} drifted from the renderer; regenerate it with `focal deployment render`",
            golden.join(name).display()
        );
    }
}

#[test]
fn the_checked_in_kubernetes_manifests_are_the_renderer_output() {
    let repo = repo();
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("kubernetes");
    let config = repo.join("deploy/config/kubernetes.yaml");
    let result = render(
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
        &output,
    );
    assert_eq!(result["result"]["target"], "kubernetes");
    // Only the storage class is left to the cluster.
    assert_eq!(
        result["result"]["missing"],
        serde_json::json!([{"input": "storage_class"}])
    );
    compare(&output, &repo.join("deploy/kubernetes"));
    // Every object the manifests declare, the chart templates too.
    let templates = std::fs::read_to_string(repo.join("deploy/helm/focal/templates/_helpers.tpl"))
        .unwrap()
        + &std::fs::read_to_string(repo.join("deploy/helm/focal/templates/statefulsets.yaml"))
            .unwrap()
        + &std::fs::read_to_string(repo.join("deploy/helm/focal/templates/service.yaml")).unwrap()
        + &std::fs::read_to_string(repo.join("deploy/helm/focal/templates/pdb.yaml")).unwrap()
        + &std::fs::read_to_string(repo.join("deploy/helm/focal/templates/configmap.yaml"))
            .unwrap();
    for needle in [
        "kind: StatefulSet",
        "kind: Service",
        "kind: PodDisruptionBudget",
        "kind: ConfigMap",
        "prepare-volume",
        "\"--invite-file\"",
        "publishNotReadyAddresses: true",
        "\"--check\", \"alive\"",
        "topology.kubernetes.io/zone",
        "terminationGracePeriodSeconds: 45",
    ] {
        assert!(templates.contains(needle), "chart lacks {needle}");
    }
    // A rendered file is never overwritten.
    let again = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "deployment",
            "render",
            "kubernetes",
            "--namespace",
            "focal",
            "--zone",
            "a",
            "--zone",
            "b",
            "--zone",
            "c",
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!again.status.success());
    // `helm template` agrees on the object names when helm is installed;
    // otherwise this lane records that it did not run.
    if let Ok(helm) = Command::new("helm")
        .args([
            "template",
            "focal",
            repo.join("deploy/helm/focal").to_str().unwrap(),
            "--namespace",
            "focal",
        ])
        .output()
    {
        assert!(
            helm.status.success(),
            "{}",
            String::from_utf8_lossy(&helm.stderr)
        );
        let text = String::from_utf8_lossy(&helm.stdout);
        for name in [
            "focal-founder",
            "focal-b",
            "focal-c",
            "focal-config",
            "focal-hosts",
        ] {
            assert!(
                text.contains(&format!("name: {name}")),
                "helm template lacks {name}"
            );
        }
        assert!(text.contains("- \"b\""));
    } else {
        eprintln!("helm is not installed: chart rendering not executed");
    }
}

#[test]
fn the_checked_in_unit_is_the_renderer_output_and_the_image_pins_the_release_toolchain() {
    let repo = repo();
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("systemd");
    let config = repo.join("deploy/config/systemd.yaml");
    let result = render(
        &[
            "--config",
            config.to_str().unwrap(),
            "deployment",
            "render",
            "systemd",
            "--invite-file",
            "/etc/focal/join.invite",
        ],
        &output,
    );
    assert_eq!(result["result"]["target"], "systemd");
    assert_eq!(result["result"]["missing"], serde_json::json!([]));
    compare(&output, &repo.join("deploy/systemd"));
    let dockerfile = std::fs::read_to_string(repo.join("deploy/container/Dockerfile")).unwrap();
    let platforms: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(repo.join("scripts/release/platforms.json")).unwrap(),
    )
    .unwrap();
    let musl = platforms["musl_image"].as_str().unwrap();
    assert!(dockerfile.contains(&format!("FROM {musl} AS build")));
    assert!(dockerfile.contains("FROM scratch"));
    assert!(dockerfile.contains("USER 65532:65532"));
    assert!(dockerfile.contains("STOPSIGNAL SIGTERM"));
    assert!(dockerfile.contains("target-feature=+crt-static"));
    assert!(!dockerfile.contains("--offline"));
    // The image build needs a container runtime and the network: it is not
    // executed here.
    eprintln!("container image build not executed by this test");
}
