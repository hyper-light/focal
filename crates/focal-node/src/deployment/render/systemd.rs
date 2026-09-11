//! A supervised host: one unit that starts the node under its own user with
//! the hardening a state-only service can carry, stops it with SIGTERM
//! inside the node's own cleanup bound, and restarts it on failure (doc 08
//! §3; 24 §24).
use super::{ConfigNode, MissingInput, RenderedAssets, config_yaml, valid_label, yaml_string};
use crate::{config::Settings, deployment::DeploymentError};
use std::fmt::Write as _;

/// What a systemd render needs beyond the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemdRequest {
    /// The installed binary.
    pub binary: String,
    /// The service user and group; the data directory is theirs.
    pub user: String,
    /// The data directory; `/var/lib/focal` is managed as a state directory.
    pub data_dir: String,
    /// Where the rendered configuration is installed.
    pub config_path: String,
    /// A host's invitation, redeemed at the first start (24 §24).
    pub invite_file: Option<String>,
}
impl Default for SystemdRequest {
    fn default() -> Self {
        Self {
            binary: "/usr/local/bin/focal".into(),
            user: "focal".into(),
            data_dir: "/var/lib/focal".into(),
            config_path: "/etc/focal/focal.yaml".into(),
            invite_file: None,
        }
    }
}
/// The node's shutdown bound is 30 s (`main.rs`); the supervisor waits
/// longer before it kills.
const STOP_TIMEOUT_SECONDS: u32 = 45;

pub fn render(
    settings: &Settings,
    request: &SystemdRequest,
) -> Result<RenderedAssets, DeploymentError> {
    settings.validate()?;
    for (field, value) in [
        ("--binary", &request.binary),
        ("--state-dir", &request.data_dir),
        ("--config-path", &request.config_path),
    ] {
        if !value.starts_with('/') || value.contains(['\n', ' ']) {
            return Err(DeploymentError::Render(format!(
                "{field} must be an absolute path without spaces"
            )));
        }
    }
    if !valid_label(&request.user) {
        return Err(DeploymentError::Render(
            "--user must be a lowercase name of at most 63 bytes".into(),
        ));
    }
    if request
        .invite_file
        .as_deref()
        .is_some_and(|path| !path.starts_with('/') || path.contains(['\n', ' ']))
    {
        return Err(DeploymentError::Render(
            "--invite-file must be an absolute path without spaces".into(),
        ));
    }
    let mut assets = RenderedAssets {
        files: Vec::new(),
        missing: Vec::new(),
        notes: Vec::new(),
    };
    if settings.node.advertise.is_none() {
        assets.missing.push(MissingInput::Advertise);
    }
    let listen = settings.node.listen.map(|address| address.to_string());
    let metrics = settings
        .node
        .metrics_listen
        .map(|address| address.to_string());
    let config = config_yaml(
        settings,
        &ConfigNode {
            listen: listen.as_deref(),
            advertise: settings.node.advertise.as_deref(),
            metrics_listen: metrics.as_deref(),
            region: settings.topology.region.as_deref(),
            zone: settings.topology.zone.as_deref(),
        },
    );
    assets.push("focal.yaml", config);
    let mut unit = String::new();
    let _ = writeln!(
        unit,
        "[Unit]\nDescription=Focal node\nDocumentation=https://github.com/hyper-light/focal\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=exec\nUser={user}\nGroup={user}",
        user = request.user
    );
    let mut exec = format!(
        "{} --config {} --data-dir {} start",
        request.binary, request.config_path, request.data_dir
    );
    if let Some(invite) = &request.invite_file {
        let _ = write!(exec, " --invite-file {invite}");
    }
    let _ = writeln!(unit, "ExecStart={exec}");
    let _ = writeln!(
        unit,
        "KillSignal=SIGTERM\nKillMode=mixed\nTimeoutStopSec={STOP_TIMEOUT_SECONDS}\nRestart=on-failure\nRestartSec=2\nLimitNOFILE=65536\nUMask=0077"
    );
    if request.data_dir == "/var/lib/focal" {
        unit.push_str("StateDirectory=focal\nStateDirectoryMode=0700\n");
    } else {
        let _ = writeln!(unit, "ReadWritePaths={}", request.data_dir);
        assets.notes.push(format!(
            "{} is outside /var/lib: create it owned by {} with mode 0700 before the first start (`focal --data-dir {} prepare-volume --owner UID:GID`).",
            request.data_dir, request.user, request.data_dir
        ));
    }
    if request.config_path.starts_with("/etc/focal/") {
        unit.push_str("ConfigurationDirectory=focal\n");
    }
    unit.push_str(
        "NoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\nPrivateDevices=yes\nProtectKernelTunables=yes\nProtectKernelModules=yes\nProtectKernelLogs=yes\nProtectControlGroups=yes\nProtectClock=yes\nProtectHostname=yes\nRestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\nRestrictNamespaces=yes\nRestrictRealtime=yes\nRestrictSUIDSGID=yes\nLockPersonality=yes\nMemoryDenyWriteExecute=yes\nSystemCallArchitectures=native\nSystemCallFilter=@system-service\nSystemCallFilter=~@privileged @resources\nCapabilityBoundingSet=\nAmbientCapabilities=\n\n[Install]\nWantedBy=multi-user.target\n",
    );
    assets.push("focal.service", unit);
    assets.notes.push(format!(
        "Install {} as the unit, {} as the configuration ({}), then `systemctl enable --now focal`. The node stops on SIGTERM within its 30 s cleanup bound; the unit waits {STOP_TIMEOUT_SECONDS} s before killing.",
        yaml_string("focal.service"),
        yaml_string("focal.yaml"),
        request.config_path
    ));
    assets.notes.push(
        "Readiness is `focal cluster node probe --check alive|catching-up|authoritative|policy` over the data directory's admin socket; a listening process is not authoritative because it listens.".into(),
    );
    Ok(assets)
}
