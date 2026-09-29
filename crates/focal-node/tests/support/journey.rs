//! A recorded operator journey (08 §11, DC20): every command one stage's
//! test runs against the real binary, the concept the operator needs for
//! it and the inputs it takes, kept as a transcript, compared with the
//! stage's entry in `tests/deployment/concepts.json` (the concepts a stage
//! introduces over the stages it builds on are the file's claim; a new
//! one fails the test until the file, and so the design, records it), and
//! searched for secrets (DC17). The including test declares `mod fleet`
//! first; this module records what that harness runs.
#![allow(dead_code)]
use super::fleet::{self, Node, Server};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Output,
};

/// One recorded command: the concept it needs, the inputs it takes and
/// the command with every machine-specific value replaced by its kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub concept: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    pub command: Vec<String>,
}
/// One stage as `concepts.json` records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    /// The stages whose concepts an operator already has at this one.
    #[serde(default)]
    pub builds_on: Vec<String>,
    /// The concepts this stage adds over those, in the order they appear.
    pub introduces: Vec<String>,
    /// Every input the stage's commands take, in the order they appear.
    pub inputs: Vec<String>,
    /// The test that walked the stage and the demonstrations it covers.
    pub executed_by: String,
    /// Claimed demonstrations the test does not run here, each with why.
    #[serde(default)]
    pub not_executed: Vec<String>,
    pub steps: Vec<Step>,
}
pub type Concepts = BTreeMap<String, Stage>;

pub fn concepts_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/deployment/concepts.json")
}
pub fn load_concepts() -> Concepts {
    let text = std::fs::read_to_string(concepts_path()).unwrap();
    serde_json::from_str(&text).unwrap()
}
/// The concepts a stage's operator holds: its own and those of every
/// stage it builds on, transitively.
pub fn known_concepts(concepts: &Concepts, stage: &str) -> BTreeSet<String> {
    let mut known = BTreeSet::new();
    let mut pending = vec![stage.to_owned()];
    while let Some(name) = pending.pop() {
        let Some(entry) = concepts.get(&name) else {
            panic!("concepts.json names no stage {name}");
        };
        for concept in &entry.introduces {
            known.insert(concept.clone());
        }
        for base in &entry.builds_on {
            if !known.contains(&format!("stage:{base}")) {
                known.insert(format!("stage:{base}"));
                pending.push(base.clone());
            }
        }
    }
    known.retain(|concept| !concept.starts_with("stage:"));
    known
}

/// The recorder for one stage.
pub struct Journey {
    stage: &'static str,
    executed_by: &'static str,
    builds_on: Vec<String>,
    steps: Vec<Step>,
    /// Every command's stdout and stderr, for the redaction corpus.
    outputs: Vec<(Vec<String>, String)>,
    not_executed: Vec<String>,
}
impl Journey {
    pub fn new(stage: &'static str, executed_by: &'static str) -> Self {
        Self::building_on(stage, executed_by, &[])
    }
    /// A recorder for a stage that builds on earlier stages: it introduces
    /// only the concepts those stages did not.
    pub fn building_on(stage: &'static str, executed_by: &'static str, builds_on: &[&str]) -> Self {
        Self {
            stage,
            executed_by,
            builds_on: builds_on.iter().map(|name| (*name).to_owned()).collect(),
            steps: Vec::new(),
            outputs: Vec::new(),
            not_executed: Vec::new(),
        }
    }
    /// A claimed demonstration this test does not run, and why.
    pub fn not_executed(&mut self, what: &str) {
        self.not_executed.push(what.to_owned());
    }
    fn record(
        &mut self,
        node: &Node,
        context: Option<&str>,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
    ) -> Vec<String> {
        let mut command = vec!["focal".to_owned()];
        if let Some(config) = &node.config {
            command.push("--config".to_owned());
            command.push(normalize(config.to_str().unwrap()));
        }
        command.push("--data-dir".to_owned());
        command.push(normalize(node.root().to_str().unwrap()));
        if let Some(context) = context {
            command.push("--client-context".to_owned());
            command.push(context.to_owned());
        }
        command.extend(args.iter().map(|arg| normalize(arg)));
        self.steps.push(Step {
            concept: concept.to_owned(),
            inputs: inputs.iter().map(|input| (*input).to_owned()).collect(),
            command: command.clone(),
        });
        command
    }
    fn keep(&mut self, command: Vec<String>, output: &Output) {
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push('\n');
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        self.outputs.push((command, text));
    }
    /// Run and record a command, whatever it returns.
    pub fn run(
        &mut self,
        node: &Node,
        context: Option<&str>,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
    ) -> Output {
        let command = self.record(node, context, concept, inputs, args);
        // A workload command run while the cluster reconfigures (a leader
        // election, a placement change) can briefly see the authoritative
        // service unavailable or at capacity (exit 6); resend a few times so
        // the transcript is not flaked by a moment of reconfiguration. A
        // definite error is returned at once. The command is recorded once.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let output = loop {
            let output = fleet::run(node, context, args);
            if output.status.success()
                || output.status.code() != Some(6)
                || std::time::Instant::now() >= deadline
            {
                break output;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        };
        self.keep(command, &output);
        output
    }
    /// An operator command that must succeed; its JSON.
    pub fn admin(&mut self, node: &Node, concept: &str, inputs: &[&str], args: &[&str]) -> Value {
        let output = self.run(node, None, concept, inputs, args);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{args:?}: {error}: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }
    /// An offline command that carries its own `--config` (a `deployment
    /// render`), run without the node's own config; records one step and
    /// returns its JSON. Must succeed.
    pub fn admin_bare(
        &mut self,
        node: &Node,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
    ) -> Value {
        let mut recorded = vec![
            "focal".to_owned(),
            "--data-dir".to_owned(),
            normalize(node.root().to_str().unwrap()),
        ];
        recorded.extend(args.iter().map(|arg| normalize(arg)));
        self.steps.push(Step {
            concept: concept.to_owned(),
            inputs: inputs.iter().map(|input| (*input).to_owned()).collect(),
            command: recorded.clone(),
        });
        let output = fleet::run_bare(node, args);
        self.keep(recorded, &output);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{args:?}: {error}: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }
    /// A participant command (`--format json` added) that must succeed.
    pub fn cli(
        &mut self,
        node: &Node,
        context: Option<&str>,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
    ) -> Value {
        let mut args = args.to_vec();
        args.extend(["--format", "json"]);
        let output = self.run(node, context, concept, inputs, &args);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{args:?}: {error}: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }
    /// A command that must fail: its exit code and stderr.
    pub fn failure(
        &mut self,
        node: &Node,
        context: Option<&str>,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
    ) -> (i32, String) {
        let output = self.run(node, context, concept, inputs, args);
        assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }
    /// Start a node (recorded as `start ...`) and wait for its readiness.
    pub fn start(&mut self, node: &Node, concept: &str, inputs: &[&str], args: &[&str]) -> Server {
        let mut recorded = vec!["start"];
        recorded.extend_from_slice(args);
        self.record(node, None, concept, inputs, &recorded);
        fleet::start(node, args)
    }
    /// Start with environment (recorded with the variables as inputs).
    pub fn start_with(
        &mut self,
        node: &Node,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
        envs: &[(&str, &str)],
    ) -> Server {
        let mut recorded = vec!["start"];
        recorded.extend_from_slice(args);
        let env_inputs: Vec<String> = envs.iter().map(|(name, _)| format!("env {name}")).collect();
        let mut all: Vec<&str> = inputs.to_vec();
        all.extend(env_inputs.iter().map(String::as_str));
        self.record(node, None, concept, &all, &recorded);
        fleet::start_with(node, args, envs)
    }
    /// Start a node that is expected to refuse and exit (a binary below the
    /// upgrade fence, say): its exit code and stderr, recorded as a `start`
    /// step. Waits up to 60 s for the process to exit.
    pub fn start_refused(
        &mut self,
        node: &Node,
        concept: &str,
        inputs: &[&str],
        args: &[&str],
        envs: &[(&str, &str)],
    ) -> (i32, String) {
        let mut recorded = vec!["start"];
        recorded.extend_from_slice(args);
        let env_inputs: Vec<String> = envs.iter().map(|(name, _)| format!("env {name}")).collect();
        let mut all: Vec<&str> = inputs.to_vec();
        all.extend(env_inputs.iter().map(String::as_str));
        self.record(node, None, concept, &all, &recorded);
        let (mut child, _receive) = fleet::spawn(node, args, envs);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                // The spawned node inherits stderr (its refusal prints to the
                // test output); the exit code is the assertion.
                return (status.code().unwrap_or(-1), String::new());
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the node did not exit; it was expected to refuse"
            );
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    /// A manual action outside the binary (a kill, a file written, a
    /// permission changed): recorded as a step so the study counts it.
    pub fn manual(&mut self, concept: &str, inputs: &[&str], description: &[&str]) {
        self.steps.push(Step {
            concept: concept.to_owned(),
            inputs: inputs.iter().map(|input| (*input).to_owned()).collect(),
            command: description.iter().map(|part| normalize(part)).collect(),
        });
    }
    /// No recorded output contains any of `secrets` (DC17).
    pub fn assert_redacted(&self, secrets: &[&str]) {
        for (command, text) in &self.outputs {
            for secret in secrets {
                assert!(
                    !secret.is_empty() && !text.contains(secret),
                    "{command:?} exposed a secret"
                );
            }
            assert!(!text.contains("panicked at"), "{command:?} panicked");
        }
    }
    /// The recorded stage.
    pub fn stage(&self, concepts: &Concepts) -> Stage {
        let builds_on = self.builds_on.clone();
        let mut known = BTreeSet::new();
        for base in &builds_on {
            known.extend(known_concepts(concepts, base));
        }
        let mut introduces = Vec::new();
        let mut inputs = Vec::new();
        for step in &self.steps {
            if !known.contains(&step.concept) && !introduces.contains(&step.concept) {
                introduces.push(step.concept.clone());
            }
            for input in &step.inputs {
                if !inputs.contains(input) {
                    inputs.push(input.clone());
                }
            }
        }
        Stage {
            builds_on,
            introduces,
            inputs,
            executed_by: self.executed_by.to_owned(),
            not_executed: self.not_executed.clone(),
            steps: self.steps.clone(),
        }
    }
    /// Compare the recording with `concepts.json`; on a difference write the
    /// recording beside the target directory and fail naming it.
    pub fn finish(self) {
        let concepts = load_concepts();
        let recorded = self.stage(&concepts);
        let expected = concepts.get(self.stage).cloned();
        if expected.as_ref() != Some(&recorded) {
            let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("qualification");
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join(format!("{}.json", self.stage));
            std::fs::write(&path, serde_json::to_string_pretty(&recorded).unwrap()).unwrap();
            let expected_text = expected
                .map(|stage| serde_json::to_string_pretty(&stage).unwrap())
                .unwrap_or_else(|| "(no entry)".to_owned());
            let recorded_text = serde_json::to_string_pretty(&recorded).unwrap();
            let mut first = None;
            for (number, (left, right)) in
                expected_text.lines().zip(recorded_text.lines()).enumerate()
            {
                if left != right {
                    first = Some((number + 1, left.to_owned(), right.to_owned()));
                    break;
                }
            }
            panic!(
                "stage {} differs from tests/deployment/concepts.json (recording written to {}); first difference at line {:?}",
                self.stage,
                path.display(),
                first
            );
        }
    }
}

/// Replace what varies between runs (directories, ports, identities,
/// documents) with its kind, so a transcript is the same on every machine.
pub fn normalize(arg: &str) -> String {
    for prefix in ["/private/tmp/focal-", "/tmp/focal-"] {
        if let Some(rest) = arg.strip_prefix(prefix) {
            let (dir, tail) = rest
                .split_once('/')
                .map_or((rest, ""), |(dir, tail)| (dir, tail));
            let name = dir.rsplit_once('-').map_or(dir, |(name, _)| name);
            return if tail.is_empty() {
                format!("<{name}>")
            } else {
                format!("<{name}>/{}", normalize_tail(tail))
            };
        }
    }
    if arg.starts_with('{') || arg.starts_with('[') && arg.ends_with(']') && !arg.contains(':') {
        return "<document>".to_owned();
    }
    if let Some(port) = arg
        .strip_prefix("localhost:")
        .filter(|port| port.parse::<u16>().is_ok())
    {
        let _ = port;
        return "localhost:<port>".to_owned();
    }
    if arg.parse::<std::net::SocketAddr>().is_ok() {
        return "<address>".to_owned();
    }
    if arg.len() >= 16 && arg.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return "<id>".to_owned();
    }
    if let Ok(number) = arg.parse::<u64>()
        && number >= 1_000
    {
        return "<n>".to_owned();
    }
    if arg.len() > 64 {
        return "<text>".to_owned();
    }
    arg.to_owned()
}
fn normalize_tail(tail: &str) -> String {
    tail.split('/').map(normalize).collect::<Vec<_>>().join("/")
}

/// The claims demo every stage runs unchanged (DC13): a claim written and
/// posted by the requester, the respondent's receipt (and, with a
/// participant, its artifact) against it, the claim read back. On the
/// laptop both parties are the node's own local principal over its socket:
/// the manual's self-addressed handoff with a receipt requirement, the
/// only legal self-addressed claim; from the VM stage on the respondent is
/// a participant enrolled from the founder and connected through
/// `--client-context`, and the claim carries a work slot its artifact
/// fills. The commands are the same; only the connection changes.
/// One run of the demo: what it wrote and how it read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoRun {
    pub claim: String,
    pub artifact: Option<String>,
    pub claim_object: Value,
    pub artifact_object: Option<Value>,
}
pub struct Demo {
    /// The directory the respondent's commands run from.
    pub client: Node,
    /// The respondent's client context, `None` for the local principal.
    pub context: Option<String>,
    /// The respondent's principal, the claim's target (`self` locally).
    pub principal: String,
    /// The claim's action: a `handoff` to oneself on the laptop (the only
    /// legal self-addressed claim), `work` for a participant.
    pub action: &'static str,
}
impl Journey {
    /// The node's own local principal as the respondent.
    pub fn local_demo(&mut self, node: &Node) -> Demo {
        let standing = self.admin(node, "standing", &[], &["status"]);
        assert!(
            fleet::objects(&standing)[0]["Standing"]["principal"].is_array()
                || fleet::objects(&standing)[0]["Standing"]["principal"].is_string(),
            "{standing}"
        );
        Demo {
            client: Node::new("client"),
            context: None,
            principal: "self".to_owned(),
            action: "handoff",
        }
    }
    /// A participant enrolled from `founder` into a client directory of its
    /// own (recorded: the invitation, the enrollment, the standing).
    pub fn enroll(&mut self, founder: &Node, name: &str) -> Demo {
        let client = Node::new("client");
        let invitation = client.root().join(format!("{name}.invite"));
        self.admin(
            founder,
            "participant",
            &["participant name"],
            &[
                "cluster",
                "client",
                "invite",
                "--name",
                name,
                "--output",
                invitation.to_str().unwrap(),
            ],
        );
        let enrolled = self.run(
            &client,
            None,
            "participant",
            &["invitation file"],
            &[
                "context",
                "enroll",
                name,
                "--invite-file",
                invitation.to_str().unwrap(),
            ],
        );
        assert!(
            enrolled.status.success(),
            "{}",
            String::from_utf8_lossy(&enrolled.stderr)
        );
        let standing = self.admin(
            &client,
            "standing",
            &[],
            &["--client-context", name, "status"],
        );
        let principal = fleet::hex_hash(&fleet::objects(&standing)[0]["Standing"]["principal"]);
        Demo {
            client,
            context: Some(name.to_owned()),
            principal,
            action: "work",
        }
    }
    /// The invitation token a participant was enrolled with: a secret no
    /// output may carry.
    pub fn client_token(demo: &Demo) -> String {
        let name = demo.context.as_deref().expect("an enrolled participant");
        let path = demo.client.root().join(format!("{name}.invite"));
        focal_node::network_join::ClientInvitation::load(&path)
            .unwrap()
            .invitation()
            .expose_token()
            .unwrap()
            .to_string()
    }
    /// Write and post a claim addressed to the respondent; its id.
    pub fn write(&mut self, node: &Node, demo: &Demo, what: &str) -> String {
        let document = if demo.context.is_some() {
            fleet::claim_document(&demo.principal, what)
        } else {
            serde_json::json!({
                "target": "self",
                "action": demo.action,
                "description": what,
                "validations": [{"kind": "receipt", "description": "Receive the report testament",
                    "deadline": {"at": fleet::FAR}}]
            })
        };
        let result = fleet::committed(&self.cli(
            node,
            None,
            "claim",
            &["claim document"],
            &["submit", "claim", "--json", &document.to_string()],
        ));
        let claim = fleet::created(&result, "Claim").remove(0);
        fleet::committed(&self.cli(node, None, "claim", &[], &["claim", "post", &claim]));
        claim
    }
    /// The respondent acquires the receipt and, as a participant, delivers
    /// its artifact into the claim's slot.
    pub fn deliver(&mut self, node: &Node, demo: &Demo, claim: &str) -> Option<String> {
        let (at, context) = match &demo.context {
            Some(context) => (&demo.client, Some(context.as_str())),
            None => (node, None),
        };
        fleet::committed(&self.cli(at, context, "receipt", &[], &["receipt", "acquire", claim]));
        context?;
        let result = fleet::committed(&self.cli(
            at,
            context,
            "artifact",
            &["artifact content"],
            &[
                "artifact",
                "submit",
                "--claim",
                claim,
                "--slot",
                "0",
                "--text",
                fleet::PROOF,
            ],
        ));
        Some(fleet::created(&result, "Artifact").remove(0))
    }
    /// Read one object back through `node`.
    pub fn read(&mut self, node: &Node, kind: &str, id: &str) -> Value {
        fleet::objects(&self.cli(node, None, "read", &[], &["get", kind, id]))[0].clone()
    }
    /// The whole demo against `node`: the claim, its artifact (with a
    /// participant), and both as read back.
    pub fn demo(&mut self, node: &Node, demo: &Demo, what: &str) -> DemoRun {
        let claim = self.write(node, demo, what);
        let artifact = self.deliver(node, demo, &claim);
        let claim_object = self.read(node, "claim", &claim);
        let artifact_object = artifact
            .as_deref()
            .map(|artifact| self.read(node, "artifact", artifact));
        assert_eq!(fleet::claim_id(&claim_object), claim);
        DemoRun {
            claim,
            artifact,
            claim_object,
            artifact_object,
        }
    }
    /// The records of an earlier run read back the same through the
    /// participant's own connection (routed to wherever the session's
    /// leader is: the connection, not the commands, finds the fleet).
    pub fn same_via(&mut self, demo: &Demo, run: &DemoRun) {
        let context = demo.context.as_deref();
        let claim = fleet::objects(&self.cli(
            &demo.client,
            context,
            "read",
            &[],
            &["get", "claim", &run.claim],
        ))[0]
            .clone();
        assert_eq!(claim, run.claim_object);
        if let Some(artifact) = &run.artifact {
            let object = fleet::objects(&self.cli(
                &demo.client,
                context,
                "read",
                &[],
                &["get", "artifact", artifact],
            ))[0]
                .clone();
            assert_eq!(Some(object), run.artifact_object);
        }
    }
    /// The records of an earlier run read back the same through `node`.
    pub fn same(&mut self, node: &Node, run: &DemoRun) {
        assert_eq!(self.read(node, "claim", &run.claim), run.claim_object);
        if let Some(artifact) = &run.artifact {
            assert_eq!(
                Some(self.read(node, "artifact", artifact)),
                run.artifact_object
            );
        }
    }
}

impl Journey {
    /// Invite a host from the founder (the invitation through a pipe into
    /// the host's directory) and start it in one command; its node id.
    pub fn join(
        &mut self,
        founder: &Node,
        host: &Node,
        name: &str,
        advertise: &str,
    ) -> (Server, u64) {
        let invite = self.invite(founder, host, name);
        let server = self.start(
            host,
            "join",
            &["invitation file", "address"],
            &[
                "--advertise",
                advertise,
                "--invite-file",
                invite.to_str().unwrap(),
            ],
        );
        let id = fleet::identity(host).0;
        (server, id)
    }
    /// An invitation for `name`, written from the founder's pipe into the
    /// host's directory; its path.
    pub fn invite(&mut self, founder: &Node, host: &Node, name: &str) -> PathBuf {
        let output = self.run(
            founder,
            None,
            "invitation",
            &["host name"],
            &["cluster", "invite", "--node", name, "--output", "-"],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let path = host.root().join(format!("{name}.invite"));
        std::fs::write(&path, &output.stdout).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        // The invitation's bytes are a secret: never in any later output.
        self.outputs.pop();
        path
    }
    /// Record a node removal as one operator step (the test may have
    /// retried it while copies drained; the transcript shows one command).
    pub fn steps_record_remove(&mut self, node: &Node, _id: u64) {
        self.steps.push(Step {
            concept: "remove".to_owned(),
            inputs: vec!["node id".to_owned()],
            command: vec![
                "focal".to_owned(),
                "--data-dir".to_owned(),
                normalize(node.root().to_str().unwrap()),
                "cluster".to_owned(),
                "nodes".to_owned(),
                "remove".to_owned(),
                "--node".to_owned(),
                "<n>".to_owned(),
            ],
        });
    }
    /// The token inside a node invitation file.
    pub fn node_token(path: &Path) -> String {
        focal_node::network_join::NodeInvitation::load(path)
            .unwrap()
            .invitation()
            .expose_token()
            .unwrap()
            .to_string()
    }
    /// Write a policy file beside a node and plan from it; the result. The
    /// plan process carries its own `--config`, so it runs without the
    /// node's, which the node already committed at its first start.
    pub fn plan(&mut self, node: &Node, name: &str, yaml: &str, output: Option<&Path>) -> Value {
        let policy = node.root().join(format!("{name}.yaml"));
        std::fs::write(&policy, yaml).unwrap();
        let mut args = vec!["--config", policy.to_str().unwrap(), "deployment", "plan"];
        let inputs: Vec<&str> = match output {
            Some(path) => {
                args.extend(["--output", path.to_str().unwrap()]);
                vec!["policy file", "plan file"]
            }
            None => {
                args.push("--dry-run");
                vec!["policy file"]
            }
        };
        let mut recorded = vec![
            "focal".to_owned(),
            "--data-dir".to_owned(),
            normalize(node.root().to_str().unwrap()),
        ];
        recorded.extend(args.iter().map(|arg| normalize(arg)));
        self.steps.push(Step {
            concept: "plan".to_owned(),
            inputs: inputs.iter().map(|input| (*input).to_owned()).collect(),
            command: recorded.clone(),
        });
        let output = fleet::run_bare(node, &args);
        self.keep(recorded, &output);
        assert!(
            output.status.success(),
            "{args:?}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "{args:?}: {error}: {}",
                String::from_utf8_lossy(&output.stdout)
            )
        })
    }
}
