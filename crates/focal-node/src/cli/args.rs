use clap::{Args, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum Commands {
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
    /// Read one bounded page; every relationship filter is optional.
    List {
        #[command(subcommand)]
        command: ListCommand,
    },
    /// Post, report progress, or cancel an existing claim.
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
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum OutputFormat {
    Table,
    Json,
}
#[derive(Args)]
pub(crate) struct OutputOptions {
    /// Human table or the complete structured result.
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}
#[derive(Args)]
pub(crate) struct MutationOptions {
    /// Private journal directory. Existing operations use `request retry`.
    #[arg(long)]
    pub operation: Option<PathBuf>,
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
    /// Close an existing evidence set with its exact artifact manifest.
    Testament(TestamentArgs),
    /// Attach an immutable artifact to an open, receipt-fenced evidence set.
    Artifact(ArtifactArgs),
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
    /// Repeat artifact ID:descriptor hash in manifest order.
    #[arg(long)]
    pub artifact: Vec<String>,
    #[arg(long, conflicts_with = "artifact")]
    pub manifest_file: Option<PathBuf>,
    #[arg(long)]
    pub summary: Option<String>,
    #[arg(long)]
    pub confidence: Option<String>,
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
    /// Inline artifact payload, subject to the ledger's inline size limit.
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
    #[arg(long)]
    pub claim: Option<String>,
    #[arg(long)]
    pub testament: Option<String>,
    /// Claim issuer. Does not alter authentication.
    #[arg(long, alias = "issuer")]
    pub source: Option<String>,
    /// Claim subject.
    #[arg(long, alias = "subject")]
    pub target: Option<String>,
    #[arg(long)]
    pub status: Option<String>,
    #[arg(long)]
    pub action: Option<String>,
    #[arg(long)]
    pub producer: Option<String>,
    #[arg(long)]
    pub kind: Option<String>,
    #[arg(long)]
    pub schema_hash: Option<String>,
    #[arg(long)]
    pub evaluator: Option<String>,
    #[arg(long)]
    pub phase: Option<String>,
    #[arg(long)]
    pub mode: Option<String>,
}
#[derive(Args)]
pub(crate) struct ListArgs {
    #[command(flatten)]
    pub filters: Filters,
    /// Maximum results in this page; the service also bounds visited records.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=256))]
    pub limit: u32,
    /// Opaque continuation printed by the previous page. Keep filters unchanged.
    #[arg(long)]
    pub cursor: Option<String>,
    #[command(flatten)]
    pub output: OutputOptions,
}
#[derive(Subcommand)]
pub(crate) enum ListCommand {
    Claims(ListArgs),
    Testaments(ListArgs),
    Artifacts(ListArgs),
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
    pub id: String,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct ProgressArgs {
    pub id: String,
    #[arg(long)]
    pub receipt: String,
    #[arg(long)]
    pub receipt_epoch: u64,
    #[arg(long)]
    pub message: String,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Args)]
pub(crate) struct CancelArgs {
    pub id: String,
    #[arg(long)]
    pub reason: String,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ClaimCommand {
    Post(ClaimIdArgs),
    Progress(ProgressArgs),
    Cancel(CancelArgs),
}
#[derive(Args)]
pub(crate) struct AcquireArgs {
    pub claim: String,
    /// New receipt identity; generated once and journaled if omitted.
    #[arg(long)]
    pub id: Option<String>,
    /// Expected initial receipt generation; adoption is a separate runtime action.
    #[arg(long, default_value_t = 1)]
    pub epoch: u64,
    #[command(flatten)]
    pub mutation: MutationOptions,
}
#[derive(Subcommand)]
pub(crate) enum ReceiptCommand {
    Acquire(AcquireArgs),
}
#[derive(Args)]
pub(crate) struct BeginEvidenceArgs {
    #[arg(long)]
    pub claim: String,
    #[arg(long)]
    pub receipt: String,
    #[arg(long)]
    pub receipt_epoch: u64,
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
    /// Resume the exact saved operation, including epoch admission.
    Retry {
        operation: PathBuf,
        #[command(flatten)]
        output: OutputOptions,
    },
    /// Inspect saved recovery state, optionally querying the owner's retained receipt.
    Inspect {
        operation: PathBuf,
        /// Query the owner at a fresh quorum barrier and verify the saved command.
        #[arg(long)]
        remote: bool,
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
