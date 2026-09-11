//! Kubernetes as packaging (doc 08 §5; 24 §24): a headless service for
//! stable names, one founder StatefulSet and one host StatefulSet per
//! failure domain, each pod with its own volume and its own enrollment, a
//! disruption budget per role, probes that ask the node rather than the
//! socket, and a configuration per set. The manifests carry secret
//! references, never invitations; the invitations are issued by the running
//! founder and installed through the operator's own tooling.
use super::{ConfigNode, MissingInput, RenderedAssets, config_yaml, valid_label, yaml_string};
use crate::{
    config::{FailureDomain, Settings},
    deployment::DeploymentError,
};
use std::fmt::Write as _;

/// What a Kubernetes render needs beyond the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubernetesRequest {
    pub namespace: String,
    /// The image every pod runs; absent, the local tag is a placeholder.
    pub image: Option<String>,
    pub storage_class: Option<String>,
    /// The secret holding one invitation per host pod (`<pod>.invite`).
    pub secret: Option<String>,
    /// The zones a zone-surviving deployment spreads over, in order; the
    /// founder takes the first.
    pub zones: Vec<String>,
    /// Nodes in total; default the fewest the durability needs (`2f+1`).
    pub nodes: Option<u32>,
    /// Each pod's volume request.
    pub volume: String,
    /// The QUIC port every pod listens and advertises on.
    pub port: u16,
}
impl Default for KubernetesRequest {
    fn default() -> Self {
        Self {
            namespace: "focal".into(),
            image: None,
            storage_class: None,
            secret: None,
            zones: Vec::new(),
            nodes: None,
            volume: "20Gi".into(),
            port: 7443,
        }
    }
}
/// One StatefulSet the render places.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SetPlan {
    name: String,
    founder: bool,
    replicas: u32,
    zone: Option<String>,
}
const USER: u32 = 65532;
const DATA_DIR: &str = "/var/lib/focal";
const CONFIG_MAP: &str = "focal-config";
const SERVICE: &str = "focal";
const DEFAULT_SECRET: &str = "focal-invitations";
/// The node's shutdown bound is 30 s; the pod waits longer before SIGKILL.
const GRACE_SECONDS: u32 = 45;

pub fn render(
    settings: &Settings,
    request: &KubernetesRequest,
    version: &str,
) -> Result<RenderedAssets, DeploymentError> {
    settings.validate()?;
    if !valid_label(&request.namespace) {
        return Err(DeploymentError::Render(
            "--namespace must be a DNS label of at most 63 bytes".into(),
        ));
    }
    if request.port == 0 {
        return Err(DeploymentError::Render("--port must be nonzero".into()));
    }
    if request.volume.is_empty()
        || request.volume.len() > 32
        || !request
            .volume
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
    {
        return Err(DeploymentError::Render(
            "--volume must be a Kubernetes quantity such as 20Gi".into(),
        ));
    }
    for zone in &request.zones {
        if !valid_label(zone) || zone.len() > 40 {
            return Err(DeploymentError::Render(format!(
                "--zone {zone:?} must be a DNS label of at most 40 bytes"
            )));
        }
    }
    if request
        .zones
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != request.zones.len()
    {
        return Err(DeploymentError::Render(
            "--zone names must be distinct".into(),
        ));
    }
    if let Some(secret) = &request.secret
        && !valid_label(secret)
    {
        return Err(DeploymentError::Render(
            "--secret must be a DNS label of at most 63 bytes".into(),
        ));
    }
    if let Some(class) = &request.storage_class
        && !valid_label(class)
    {
        return Err(DeploymentError::Render(
            "--storage-class must be a DNS label of at most 63 bytes".into(),
        ));
    }
    if let Some(image) = &request.image
        && (image.is_empty() || image.len() > 255 || image.contains([' ', '\n', '"']))
    {
        return Err(DeploymentError::Render(
            "--image is not an image reference".into(),
        ));
    }
    let mut assets = RenderedAssets {
        files: Vec::new(),
        missing: Vec::new(),
        notes: Vec::new(),
    };
    let failures = usize::from(settings.durability.max_failures);
    let minimum = failures
        .checked_mul(2)
        .and_then(|twice| twice.checked_add(1))
        .ok_or(DeploymentError::Capacity)?;
    let nodes = match request.nodes {
        Some(nodes) => usize::try_from(nodes).map_err(|_| DeploymentError::Capacity)?,
        None => minimum,
    };
    if nodes < minimum || nodes > 1024 {
        return Err(DeploymentError::Render(format!(
            "surviving {failures} {} failure(s) needs at least {minimum} nodes (at most 1024)",
            crate::deployment::survive_name(settings.durability.survive)
        )));
    }
    let sets = match settings.durability.survive {
        FailureDomain::Region => {
            return Err(DeploymentError::Render(
                "a Kubernetes cluster spans zones; region survival is one deployment per region joined by invitation (08 §7)".into(),
            ));
        }
        FailureDomain::Node => {
            if !request.zones.is_empty() {
                return Err(DeploymentError::Render(
                    "--zone applies to zone survival; node survival spreads by the scheduler"
                        .into(),
                ));
            }
            let mut sets = vec![SetPlan {
                name: "focal-founder".into(),
                founder: true,
                replicas: 1,
                zone: settings.topology.zone.clone(),
            }];
            if nodes > 1 {
                sets.push(SetPlan {
                    name: "focal-hosts".into(),
                    founder: false,
                    replicas: u32::try_from(nodes.saturating_sub(1))
                        .map_err(|_| DeploymentError::Capacity)?,
                    zone: settings.topology.zone.clone(),
                });
            }
            sets
        }
        FailureDomain::Zone => {
            if request.zones.len() < minimum {
                assets.missing.push(MissingInput::Zones {
                    needed: minimum,
                    given: request.zones.len(),
                });
                assets.notes.push(format!(
                    "Zone survival of {failures} failure(s) needs {minimum} zones named with --zone; nothing was rendered."
                ));
                return Ok(assets);
            }
            let mut sets = Vec::new();
            for (index, zone) in request.zones.iter().enumerate() {
                // Nodes spread round-robin; the founder is the first zone's
                // first node.
                let share = nodes
                    .checked_div(request.zones.len())
                    .unwrap_or(0)
                    .saturating_add(usize::from(
                        index < nodes.checked_rem(request.zones.len()).unwrap_or(0),
                    ));
                if index == 0 {
                    sets.push(SetPlan {
                        name: "focal-founder".into(),
                        founder: true,
                        replicas: 1,
                        zone: Some(zone.clone()),
                    });
                    if share > 1 {
                        sets.push(SetPlan {
                            name: format!("focal-{zone}"),
                            founder: false,
                            replicas: u32::try_from(share.saturating_sub(1))
                                .map_err(|_| DeploymentError::Capacity)?,
                            zone: Some(zone.clone()),
                        });
                    }
                } else if share > 0 {
                    sets.push(SetPlan {
                        name: format!("focal-{zone}"),
                        founder: false,
                        replicas: u32::try_from(share).map_err(|_| DeploymentError::Capacity)?,
                        zone: Some(zone.clone()),
                    });
                }
            }
            sets
        }
    };
    let image = match &request.image {
        Some(image) => image.clone(),
        None => {
            let placeholder = format!("focal:{version}");
            assets.missing.push(MissingInput::Image {
                placeholder: placeholder.clone(),
            });
            placeholder
        }
    };
    if request.storage_class.is_none() {
        assets.missing.push(MissingInput::StorageClass);
    }
    let secret = request
        .secret
        .clone()
        .unwrap_or_else(|| DEFAULT_SECRET.to_owned());
    let hosts: Vec<String> = sets
        .iter()
        .filter(|set| !set.founder)
        .flat_map(|set| (0..set.replicas).map(move |ordinal| format!("{}-{ordinal}", set.name)))
        .collect();
    let script = invitations_script(&request.namespace, &secret, &hosts);
    if request.secret.is_none() {
        assets.missing.push(MissingInput::InvitationSecret {
            secret: secret.clone(),
            invitations: hosts.clone(),
            script: "invitations.sh".into(),
        });
    }
    // The configuration per set: the requested policy, the set's zone.
    let mut config_map = format!(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {CONFIG_MAP}\n  namespace: {}\n  labels:\n    app.kubernetes.io/name: focal\n    app.kubernetes.io/managed-by: focal-deployment-render\ndata:\n",
        request.namespace
    );
    for set in &sets {
        let config = config_yaml(
            settings,
            &ConfigNode {
                listen: None,
                advertise: None,
                metrics_listen: None,
                region: settings.topology.region.as_deref(),
                zone: set.zone.as_deref(),
            },
        );
        let _ = writeln!(config_map, "  {}.yaml: |", set.name);
        for line in config.lines() {
            let _ = writeln!(config_map, "    {line}");
        }
    }
    assets.push("configmap.yaml", config_map);
    assets.push("service.yaml", service(&request.namespace, request.port));
    let mut resources = vec![
        "configmap.yaml".to_owned(),
        "service.yaml".to_owned(),
        "pdb.yaml".to_owned(),
    ];
    for set in &sets {
        let file = format!("statefulset-{}.yaml", set.name);
        assets.push(
            &file,
            stateful_set(set, request, &image, &secret, settings.durability.survive),
        );
        resources.push(file);
    }
    assets.push("pdb.yaml", budgets(&request.namespace, failures));
    let mut kustomization = format!("namespace: {}\nresources:\n", request.namespace);
    for resource in &resources {
        let _ = writeln!(kustomization, "  - {resource}");
    }
    assets.push("kustomization.yaml", kustomization);
    assets.push("invitations.sh", script);
    assets.notes.push(format!(
        "Bootstrap allocation, unqualified: each pod requests 500m CPU and 1Gi memory (limit 2Gi) and a {} volume; measure and raise them for your workload (08 §5).",
        request.volume
    ));
    assets.notes.push(
        "Apply the founder first (`kubectl apply -k .` applies everything; host pods wait for their invitation), run invitations.sh once focal-founder-0 is Ready to issue and install the invitations, then the hosts enroll and start.".into(),
    );
    assets.notes.push(
        "Probes ask the node: startup, liveness and readiness are `cluster node probe --check alive`; `catching-up`, `authoritative` and `policy` are for inspection (`kubectl exec`) and never gate restarts, so a healthy node is not restarted for a missing quorum.".into(),
    );
    Ok(assets)
}
fn service(namespace: &str, port: u16) -> String {
    format!(
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: {SERVICE}\n  namespace: {namespace}\n  labels:\n    app.kubernetes.io/name: focal\n    app.kubernetes.io/managed-by: focal-deployment-render\nspec:\n  clusterIP: None\n  publishNotReadyAddresses: true\n  selector:\n    app.kubernetes.io/name: focal\n  ports:\n    - name: peer\n      port: {port}\n      targetPort: peer\n      protocol: UDP\n"
    )
}
fn budgets(namespace: &str, failures: usize) -> String {
    format!(
        "apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: focal-founder\n  namespace: {namespace}\n  labels:\n    app.kubernetes.io/name: focal\n    app.kubernetes.io/managed-by: focal-deployment-render\nspec:\n  maxUnavailable: 0\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: focal\n      app.kubernetes.io/component: founder\n---\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: focal-hosts\n  namespace: {namespace}\n  labels:\n    app.kubernetes.io/name: focal\n    app.kubernetes.io/managed-by: focal-deployment-render\nspec:\n  maxUnavailable: {failures}\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: focal\n      app.kubernetes.io/component: host\n"
    )
}
fn invitations_script(namespace: &str, secret: &str, hosts: &[String]) -> String {
    let mut out = String::from(
        "#!/bin/sh\n# Issue one invitation per host pod from the running founder and install\n# them as the secret the host StatefulSets mount (24 §24). Run where\n# kubectl reaches the cluster, once focal-founder-0 is Ready. Invitations\n# are one-use and expire after an hour; rerun for a pod that never joined.\nset -eu\n",
    );
    let _ = writeln!(out, "NAMESPACE=\"{namespace}\"\nSECRET=\"{secret}\"");
    if hosts.is_empty() {
        out.push_str("echo \"no host pods to invite\"\nexit 0\n");
        return out;
    }
    out.push_str("HOSTS=\"");
    out.push_str(&hosts.join(" "));
    out.push_str("\"\nFILES=\"\"\nfor host in $HOSTS; do\n  kubectl -n \"$NAMESPACE\" exec focal-founder-0 -c focal -- /focal --data-dir /var/lib/focal cluster invite --node \"$host\" --output - > \"$host.invite\"\n  FILES=\"$FILES --from-file=$host.invite=$host.invite\"\ndone\nkubectl -n \"$NAMESPACE\" delete secret \"$SECRET\" --ignore-not-found\n# shellcheck disable=SC2086\nkubectl -n \"$NAMESPACE\" create secret generic \"$SECRET\" $FILES\nrm -f -- *.invite\n");
    out
}
fn stateful_set(
    set: &SetPlan,
    request: &KubernetesRequest,
    image: &str,
    secret: &str,
    survive: FailureDomain,
) -> String {
    let component = if set.founder { "founder" } else { "host" };
    let port = request.port;
    let mut out = format!(
        "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: {name}\n  namespace: {namespace}\n  labels:\n    app.kubernetes.io/name: focal\n    app.kubernetes.io/component: {component}\n    app.kubernetes.io/managed-by: focal-deployment-render\n    focal.dev/set: {name}\n  annotations:\n    focal.dev/qualification: unqualified-bootstrap-allocation\nspec:\n  serviceName: {SERVICE}\n  replicas: {replicas}\n  podManagementPolicy: OrderedReady\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: focal\n      focal.dev/set: {name}\n  template:\n    metadata:\n      labels:\n        app.kubernetes.io/name: focal\n        app.kubernetes.io/component: {component}\n        focal.dev/set: {name}\n    spec:\n      terminationGracePeriodSeconds: {GRACE_SECONDS}\n      securityContext:\n        runAsNonRoot: true\n        runAsUser: {USER}\n        runAsGroup: {USER}\n        fsGroup: {USER}\n        seccompProfile:\n          type: RuntimeDefault\n",
        name = set.name,
        namespace = request.namespace,
        replicas = set.replicas,
    );
    if let Some(zone) = &set.zone {
        let _ = writeln!(
            out,
            "      affinity:\n        nodeAffinity:\n          requiredDuringSchedulingIgnoredDuringExecution:\n            nodeSelectorTerms:\n              - matchExpressions:\n                  - key: topology.kubernetes.io/zone\n                    operator: In\n                    values:\n                      - {}",
            yaml_string(zone)
        );
    }
    if survive == FailureDomain::Node && set.replicas > 1 {
        out.push_str("      topologySpreadConstraints:\n        - maxSkew: 1\n          topologyKey: kubernetes.io/hostname\n          whenUnsatisfiable: DoNotSchedule\n          labelSelector:\n            matchLabels:\n              app.kubernetes.io/name: focal\n");
    }
    // The volume is given to the node's user by the same image, privileged
    // only for that step; the node itself never runs as root.
    let _ = write!(
        out,
        "      initContainers:\n        - name: prepare-volume\n          image: {image}\n          command: [\"/focal\"]\n          args: [\"--data-dir\", \"{DATA_DIR}\", \"prepare-volume\", \"--owner\", \"{USER}:{USER}\"]\n          securityContext:\n            runAsUser: 0\n            runAsNonRoot: false\n            allowPrivilegeEscalation: false\n            readOnlyRootFilesystem: true\n            capabilities:\n              drop: [\"ALL\"]\n              add: [\"CHOWN\", \"FOWNER\", \"DAC_OVERRIDE\"]\n          volumeMounts:\n            - name: data\n              mountPath: {DATA_DIR}\n      containers:\n        - name: focal\n          image: {image}\n          command: [\"/focal\"]\n          args:\n            - \"--config\"\n            - \"/etc/focal/focal.yaml\"\n            - \"--data-dir\"\n            - \"{DATA_DIR}\"\n            - \"start\"\n            - \"--listen\"\n            - \"0.0.0.0:{port}\"\n            - \"--advertise\"\n            - \"$(POD_NAME).{SERVICE}.$(POD_NAMESPACE).svc.cluster.local:{port}\"\n"
    );
    if !set.founder {
        out.push_str("            - \"--invite-file\"\n            - \"/etc/focal/invitations/$(POD_NAME).invite\"\n");
    }
    let probe = format!(
        "            exec:\n              command: [\"/focal\", \"--data-dir\", \"{DATA_DIR}\", \"cluster\", \"node\", \"probe\", \"--check\", \"alive\"]\n"
    );
    let _ = write!(
        out,
        "          env:\n            - name: POD_NAME\n              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.name\n            - name: POD_NAMESPACE\n              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.namespace\n          ports:\n            - name: peer\n              containerPort: {port}\n              protocol: UDP\n          securityContext:\n            allowPrivilegeEscalation: false\n            readOnlyRootFilesystem: true\n            capabilities:\n              drop: [\"ALL\"]\n          resources:\n            requests:\n              cpu: 500m\n              memory: 1Gi\n            limits:\n              memory: 2Gi\n          startupProbe:\n{probe}            periodSeconds: 5\n            failureThreshold: 60\n          livenessProbe:\n{probe}            periodSeconds: 10\n            failureThreshold: 6\n          readinessProbe:\n{probe}            periodSeconds: 5\n            failureThreshold: 3\n          volumeMounts:\n            - name: data\n              mountPath: {DATA_DIR}\n            - name: config\n              mountPath: /etc/focal/focal.yaml\n              subPath: focal.yaml\n              readOnly: true\n"
    );
    if !set.founder {
        out.push_str("            - name: invitations\n              mountPath: /etc/focal/invitations\n              readOnly: true\n");
    }
    let _ = write!(
        out,
        "      volumes:\n        - name: config\n          configMap:\n            name: {CONFIG_MAP}\n            items:\n              - key: {name}.yaml\n                path: focal.yaml\n",
        name = set.name
    );
    if !set.founder {
        let _ = write!(
            out,
            "        - name: invitations\n          secret:\n            secretName: {secret}\n            defaultMode: 288\n"
        );
    }
    let _ = write!(
        out,
        "  volumeClaimTemplates:\n    - metadata:\n        name: data\n      spec:\n        accessModes: [\"ReadWriteOnce\"]\n"
    );
    if let Some(class) = &request.storage_class {
        let _ = writeln!(out, "        storageClassName: {class}");
    }
    let _ = write!(
        out,
        "        resources:\n          requests:\n            storage: {}\n",
        request.volume
    );
    out
}
