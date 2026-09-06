use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Register and inspect durable owner-authorized wait predicates.
    Monitor {
        #[command(subcommand)]
        command: super::monitor::MonitorCommand,
    },
    /// Follow durable ledger changes; acknowledge only flushed output.
    Watch {
        #[command(subcommand)]
        command: super::watch::WatchCommand,
    },
    /// Inspect pinned external validator contracts recorded by claims.
    Validator {
        #[command(subcommand)]
        command: super::validators::ValidatorCommand,
    },
    /// Explore bounded graph pages at an immutable ledger prefix.
    Ledger {
        #[command(subcommand)]
        command: super::graph::LedgerCommand,
    },
    /// Submit authored work or evidence using flags, JSON, or YAML.
    Submit {
        #[command(subcommand)]
        command: SubmitCommand,
    },
    /// Read an authoritative object from the running service.
    Get {
        #[command(subcommand)]
        command: GetCommand,
    },
    /// List matching objects with optional filters and bounded pages.
    List {
        #[command(subcommand)]
        command: ListCommand,
    },
    /// Post, report progress, wait for, or cancel a claim.
    Claim {
        #[command(subcommand)]
        command: ClaimCommand,
    },
    /// Acquire a receipt as the authenticated subject of the claim.
    Receipt {
        #[command(subcommand)]
        command: ReceiptCommand,
    },
    /// Begin a receipt-fenced evidence set.
    Evidence {
        #[command(subcommand)]
        command: EvidenceCommand,
    },
    /// Acknowledge a response as its claimant; this does not establish quality.
    Testament {
        #[command(subcommand)]
        command: TestamentCommand,
    },
    /// Record evaluation lifecycle facts; invoke tools outside Focal.
    Validation {
        #[command(subcommand)]
        command: ValidationCommand,
    },
    /// Register immutable evidence or inspect and cancel durable payload uploads.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum OutputFormat {
    Table,
    Json,
    Yaml,
}
#[derive(Args)]
pub(crate) struct OutputOptions {
    /// Human table or the complete structured JSON/YAML result.
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}
#[derive(Args)]
pub(crate) struct MutationOptions {
    /// Private journal directory. Existing operations use `request retry`.
    #[arg(long)]
    pub operation: Option<PathBuf>,
    /// Resume an ID previously returned by `request reserve` or pending recovery.
    #[arg(long, conflicts_with = "operation")]
    pub operation_id: Option<String>,
    /// Reject a mutation if the object's revision has changed.
    #[arg(long)]
    pub expected_revision: Option<u64>,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Format {
    Json,
    Yaml,
}
#[derive(Args, Default)]
pub(crate) struct DocumentInput {
    /// Inline authored JSON document; mutually exclusive with field flags.
    #[arg(long, conflicts_with_all = ["yaml", "file"])]
    pub json: Option<String>,
    /// Inline authored YAML document; mutually exclusive with field flags.
    #[arg(long, conflicts_with_all = ["json", "file"])]
    pub yaml: Option<String>,
    /// Authored document path, or - for stdin.
    #[arg(long, conflicts_with_all = ["json", "yaml"])]
    pub file: Option<PathBuf>,
    /// Required for stdin or a file with no .json/.yaml/.yml extension.
    #[arg(long, value_enum, requires = "file")]
    pub input_format: Option<Format>,
}
#[derive(Subcommand)]
pub(crate) enum SubmitCommand {
    /// Generate an immutable claim. Use `claim post` to make it actionable.
    Claim(ClaimArgs),
    /// Generate an atomic batch; each claim retains its own pinned requirements.
    Claims(ClaimBatchArgs),
    /// Report completed or failed work with your exact artifact manifest.
    Testament(TestamentArgs),
    /// Attach an immutable artifact to an open, receipt-fenced evidence set.
    Artifact(ArtifactArgs),
    /// Record your designated evaluator verdict against the exact committed run.
    Validation(Box<ValidationArgs>),
}
#[derive(Args)]
pub(crate) struct ClaimBatchArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    /// Repeat a complete authored claim JSON document.
    #[arg(long)]
    pub claim_json: Vec<String>,
    /// Repeat authored claim files; JSON/YAML follows the file extension.
    #[arg(long)]
    pub claim_file: Vec<PathBuf>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct ClaimArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub occurrence: Option<String>,
    #[arg(long)]
    pub description: Option<String>,
    /// Exact subject participant ID, not a network address.
    #[arg(long)]
    pub target: Option<String>,
    /// Defaults to work; explicit consultation/challenge still needs its contract.
    #[arg(long)]
    pub action: Option<String>,
    /// Repeat KIND:KEY, for example file:src/lib.rs.
    #[arg(long)]
    pub scope: Vec<String>,
    /// Repeat KIND:CLAIM_ID; issuer, subject, action and cause are derived.
    #[arg(long)]
    pub relation: Vec<String>,
    /// Repeat a JSON/YAML validation specification file.
    #[arg(long)]
    pub validation_file: Vec<PathBuf>,
    /// Repeat an inline JSON validation specification.
    #[arg(long)]
    pub validation_json: Vec<String>,
    /// Exact timer/generation/time document; does not set the server clock.
    #[arg(long)]
    pub deadline_json: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct TestamentArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub claim: Option<String>,
    #[arg(long)]
    pub receipt: Option<String>,
    #[arg(long)]
    pub receipt_epoch: Option<u64>,
    #[arg(long)]
    pub evidence_set: Option<String>,
    /// Repeat artifact ID:descriptor hash in manifest order; include errors for unsuccessful work.
    #[arg(long)]
    pub artifact: Vec<String>,
    #[arg(long, conflicts_with = "artifact")]
    pub manifest_file: Option<PathBuf>,
    #[arg(long)]
    pub summary: Option<String>,
    #[arg(long)]
    pub confidence: Option<String>,
    /// complete, partial, refused, impossible, interrupted, or failed. Non-complete requires error evidence.
    #[arg(long)]
    pub outcome: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct ArtifactArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub claim: Option<String>,
    #[arg(long)]
    pub receipt: Option<String>,
    #[arg(long)]
    pub receipt_epoch: Option<u64>,
    #[arg(long)]
    pub evidence_set: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
    #[arg(long)]
    pub schema_hash: Option<String>,
    /// Upload file bytes through durable custody, then attach their immutable reference.
    #[arg(long, conflicts_with = "text")]
    pub payload_file: Option<PathBuf>,
    #[arg(long, conflicts_with = "payload_file")]
    pub text: Option<String>,
    /// Opaque metadata bytes from a bounded file.
    #[arg(long)]
    pub metadata_file: Option<PathBuf>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}

#[derive(Args, Default)]
pub(crate) struct Filters {
    /// Exact claim ID, or the claim associated with another object family.
    #[arg(long)]
    pub claim: Option<String>,
    /// Artifacts included in this testament's committed manifest.
    #[arg(long)]
    pub testament: Option<String>,
    /// Claim issuer. Does not alter authentication.
    #[arg(long, alias = "issuer")]
    pub source: Option<String>,
    /// Claim subject.
    #[arg(long, alias = "subject")]
    pub target: Option<String>,
    /// Exact claim lifecycle status, for example posted, satisfied or cancelled.
    #[arg(long)]
    pub status: Option<String>,
    /// Exact claim action, for example work, consultation or challenge.
    #[arg(long)]
    pub action: Option<String>,
    /// Participant that registered the artifact.
    #[arg(long)]
    pub producer: Option<String>,
    /// Artifact kind or validation requirement kind, according to this family.
    #[arg(long)]
    pub kind: Option<String>,
    /// Exact artifact payload schema hash (64 hexadecimal characters).
    #[arg(long)]
    pub schema_hash: Option<String>,
    /// Designated evaluator participant for the validation requirement.
    #[arg(long)]
    pub evaluator: Option<String>,
    /// Validation phase: admission, increment or whole_work.
    #[arg(long)]
    pub phase: Option<String>,
    /// Validation requirement mode: required or observe.
    #[arg(long)]
    pub mode: Option<String>,
    /// Required claim scope, repeatable as KIND:KEY.
    #[arg(long = "scope", value_parser = super::authored::scope_filter)]
    pub scopes: Vec<focal_client::input::ScopeDocument>,
    /// Required claim relation, repeatable as KIND=TYPE:TARGET.
    #[arg(long = "relation", value_parser = super::authored::relation_filter)]
    pub relations: Vec<focal_client::input::ClaimRelationDocument>,
    /// Exact cause, claim:ID or root:ID.
    #[arg(long)]
    pub caused_by: Option<String>,
    /// Required artifact input, repeatable as KIND:ID.
    #[arg(long = "input", value_parser = super::authored::input_filter)]
    pub inputs: Vec<focal_client::input::ObjectReferenceDocument>,
    /// Testament outcome: complete, partial, refused, impossible, interrupted or failed.
    #[arg(long)]
    pub outcome: Option<String>,
    /// Testament confidence: hint, tentative, committed or consensus.
    #[arg(long)]
    pub confidence: Option<String>,
    /// Exclusive creation SessionSeq lower bound.
    #[arg(long)]
    pub created_after: Option<u64>,
    /// Inclusive creation SessionSeq upper bound.
    #[arg(long)]
    pub created_through: Option<u64>,
}
#[derive(Args)]
pub(crate) struct ListArgs {
    /// Stream all pages at one read prefix; JSON is one page per line.
    #[arg(long)]
    pub all: bool,
    #[command(flatten)]
    pub filters: Filters,
    /// Maximum results in this page; the service also bounds visited records.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub limit: u32,
    /// Maximum records examined in this page, including nonmatching records.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=1024))]
    pub max_visits: u32,
    /// Opaque continuation printed by the previous page. Keep filters unchanged.
    #[arg(long)]
    pub cursor: Option<String>,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Subcommand)]
pub(crate) enum ListCommand {
    /// Select claims by issuer, subject, lifecycle, scopes and typed lineage.
    Claims(ListArgs),
    /// Select testaments by claim, outcome and confidence.
    Testaments(ListArgs),
    /// Select artifacts by claim, testament, producer, kind, schema and inputs.
    Artifacts(ListArgs),
    /// Select validation requirements by claim, evaluator, kind, phase and mode.
    Validations(ListArgs),
}
#[derive(Args)]
pub(crate) struct GetArgs {
    pub id: String,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Args)]
pub(crate) struct GetClaimArgs {
    /// Exact claim ID, or select exactly one claim using filters.
    pub id: Option<String>,
    #[command(flatten)]
    pub filters: Filters,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Args)]
pub(crate) struct GetArtifactArgs {
    pub id: String,
    /// Atomically write payload bytes to a new file after complete retrieval.
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[command(flatten)]
    pub display: OutputOptions,
}
#[derive(Subcommand)]
pub(crate) enum GetCommand {
    Claim(Box<GetClaimArgs>),
    Testament(GetArgs),
    /// Read the requirement and a page of recorded runs and verdict attempts.
    Validation(GetValidationArgs),
    Artifact(GetArtifactArgs),
}
#[derive(Args)]
pub(crate) struct GetValidationArgs {
    pub id: String,
    /// Include the owning claim and current testament at the same ledger snapshot.
    #[arg(long)]
    pub context: bool,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub limit: u32,
    /// Resume the same requirement at the exact previous ledger prefix.
    #[arg(long)]
    pub cursor: Option<String>,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Args)]
pub(crate) struct ClaimIdArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub id: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct ProgressArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub id: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub receipt: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub receipt_epoch: Option<u64>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub message: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct CancelArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub id: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub reason: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ClaimCommand {
    /// Observe a claim without creating work or a monitor (at most 30 seconds).
    Wait(super::claim_wait::WaitArgs),
    Post(ClaimIdArgs),
    Progress(ProgressArgs),
    Cancel(CancelArgs),
    /// Create a new immutable successor; preserve the predecessor's history.
    Supersede(Box<SupersedeArgs>),
}
#[derive(Args)]
pub(crate) struct SupersedeArgs {
    /// Existing claim being superseded; --id, if supplied, names the new successor.
    pub predecessor: String,
    #[command(flatten)]
    pub successor: ClaimArgs,
}

#[derive(Subcommand)]
pub(crate) enum TestamentCommand {
    /// Receive this exact closing testament as its claim's issuer.
    Receive(ReceiveTestamentArgs),
}
#[derive(Args)]
pub(crate) struct ReceiveTestamentArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub id: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub claim: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ValidationCommand {
    /// Pin eligible whole-work evaluations. Focal does not launch a validator.
    Begin(ValidationClaimArgs),
    /// Begin an incremental check of an attached artifact and its evidence manifest.
    BeginIncrement(IncrementValidationArgs),
    /// Apply the recorded required outcomes and graph conditions, not a caller verdict.
    Complete(ValidationClaimArgs),
}
#[derive(Args)]
pub(crate) struct ValidationClaimArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub claim: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct IncrementValidationArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub claim: Option<String>,
    #[arg(long)]
    pub validation: Option<String>,
    #[arg(long)]
    pub target_hash: Option<String>,
    #[arg(long)]
    pub manifest: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct ValidationArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub validation: Option<String>,
    #[arg(long)]
    pub target_hash: Option<String>,
    #[arg(long)]
    pub phase: Option<String>,
    #[arg(long)]
    pub epoch: Option<u64>,
    #[arg(long)]
    pub handler: Option<String>,
    #[arg(long)]
    pub handler_version: Option<String>,
    /// Must match the pinned handler; this flag does not confer authority.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub agentic: Option<bool>,
    #[arg(long)]
    pub attempt: Option<u32>,
    #[arg(long)]
    pub manifest: Option<String>,
    #[arg(long)]
    pub receipt: Option<String>,
    #[arg(long)]
    pub receipt_epoch: Option<u64>,
    /// pass, fail, incomplete, or error; recording fail can be a successful mutation.
    #[arg(long)]
    pub value: Option<String>,
    /// Repeat RESULT_ARTIFACT_ID:DESCRIPTOR_HASH, using committed proof artifacts.
    #[arg(long)]
    pub evidence: Vec<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ArtifactCommand {
    /// Inspect or cancel an existing artifact upload in this client context.
    Upload {
        #[command(subcommand)]
        command: super::upload_control::UploadCommand,
    },
    /// Register evidence as yourself; does not attach it to a respondent's manifest.
    Register(Box<RegisterArtifactArgs>),
}
#[derive(Args)]
pub(crate) struct RegisterArtifactArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    pub id: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
    #[arg(long)]
    pub schema_hash: Option<String>,
    #[arg(long, conflicts_with = "text")]
    pub payload_file: Option<PathBuf>,
    #[arg(long, conflicts_with = "payload_file")]
    pub text: Option<String>,
    #[arg(long)]
    pub metadata_file: Option<PathBuf>,
    /// Repeat a typed input reference JSON object; cannot change the producer.
    #[arg(long)]
    pub input_json: Vec<String>,
    #[arg(long)]
    pub visibility: Vec<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct AcquireArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub claim: Option<String>,
    /// New receipt identity; generated once and journaled if omitted.
    #[arg(long)]
    pub id: Option<String>,
    /// Expected initial receipt generation (defaults to 1); adoption is a separate runtime action.
    #[arg(long)]
    pub epoch: Option<u64>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ReceiptCommand {
    Acquire(AcquireArgs),
}
#[derive(Args)]
pub(crate) struct BeginEvidenceArgs {
    #[command(flatten)]
    pub input: DocumentInput,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub claim: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub receipt: Option<String>,
    #[arg(long)]
    #[arg(required_unless_present_any = ["json", "yaml", "file"])]
    pub receipt_epoch: Option<u64>,
    #[arg(long)]
    pub id: Option<String>,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum EvidenceCommand {
    Begin(BeginEvidenceArgs),
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true, subcommand_negates_reqs = true)]
pub(crate) struct RequestArgs {
    /// Existing complete JSON wire envelope; resend the same file after uncertainty.
    #[arg(required = true)]
    pub file: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Option<RequestCommand>,
}
#[derive(Subcommand)]
pub(crate) enum RequestCommand {
    /// Save a complete, non-submitted legacy wire request with frozen generated IDs.
    Build(super::request_files::BuildArgs),
    /// Check a raw JSON envelope locally; does not prove server acceptance.
    Check { file: PathBuf },
    /// Send a complete saved JSON envelope without rebuilding or changing IDs.
    Send { file: PathBuf },
    /// Resume the exact saved operation, including epoch admission.
    Retry {
        #[arg(
            required_unless_present = "operation_id",
            conflicts_with = "operation_id"
        )]
        operation: Option<PathBuf>,
        #[arg(long)]
        operation_id: Option<String>,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Inspect saved recovery state, optionally querying the owner's retained receipt.
    Inspect {
        #[arg(
            required_unless_present = "operation_id",
            conflicts_with = "operation_id"
        )]
        operation: Option<PathBuf>,
        #[arg(long)]
        operation_id: Option<String>,
        /// Query the owner at a fresh quorum barrier and verify the saved command.
        #[arg(long)]
        remote: bool,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Reserve a recovery ID before a later mutation; no business work is sent.
    Reserve {
        #[command(flatten)]
        output: OutputOptions,
    },
    /// List bounded outstanding CLI and MCP operations that still need recovery.
    Pending {
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Acknowledge that you consumed this operation's saved result.
    Acknowledge {
        #[arg(long)]
        operation_id: String,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Fence an unresolved request, returning its result if it already committed.
    #[command(alias = "abandon")]
    Seal {
        #[arg(long)]
        operation_id: String,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Query your retained mutation receipt. Missing receipts remain unknown.
    Status {
        #[arg(long)]
        request_id: String,
        #[arg(long, default_value_t = 1)]
        epoch: u64,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Observe your requested epoch's admission and committed retention floor.
    Epoch {
        #[arg(long, default_value_t = 1)]
        epoch: u64,
        #[command(flatten)]
        output: OutputOptions,
    },
}
