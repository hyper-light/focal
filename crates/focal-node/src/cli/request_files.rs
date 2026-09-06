//! Offline authored expansion into a single immutable legacy wire envelope.
use super::{CliError, Context, Result, args::DocumentInput};
use clap::Args;
use focal_client::{
    input::*,
    operations::{self, AuthoredOperation},
};
use focal_model::{ObjectRevision, RequestEpoch, RequestId, RouteEpoch};
use focal_node::config::Settings;
use focal_wire::{RequestEnvelope, WireLimits};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Args)]
pub(crate) struct BuildArgs {
    #[arg(value_parser = super::discovery::operation_names())]
    operation: String,
    #[command(flatten)]
    input: DocumentInput,
    /// New private JSON file; existing paths are never replaced.
    #[arg(long)]
    output: PathBuf,
    /// Required for mutations; the raw sender must have admitted this legacy epoch.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    request_epoch: Option<u64>,
    /// Optional nonzero 32-hex request ID; generated once when omitted.
    #[arg(long)]
    request_id: Option<String>,
    /// Exact object revision; required by revision-fenced participant commands.
    #[arg(long)]
    expected_revision: Option<u64>,
}

pub(super) fn authored(name: &str, input: DocumentInput) -> Result<AuthoredOperation> {
    let value: serde_json::Value = input
        .load(false)?
        .ok_or(InputError::Invalid("provide --json, --yaml, or --file"))?;
    let mut bytes = BoundedBytes(Vec::new());
    serde_json::to_writer(&mut bytes, &value).map_err(|_| InputError::Capacity)?;
    Ok(operations::parse_json(name, &bytes.0)?)
}

pub(super) fn build(settings: &Settings, selection: Option<&str>, args: BuildArgs) -> Result<()> {
    let authored = authored(&args.operation, args.input)?;
    if authored.descriptor().mutation && args.request_epoch.is_none() {
        return Err(InputError::Invalid(
            "mutations require --request-epoch; admit that legacy epoch before sending",
        )
        .into());
    }
    if authored.revision_claim()?.is_some() && args.expected_revision.is_none() {
        return Err(InputError::Invalid(
            "this raw participant request requires --expected-revision from an observed claim",
        )
        .into());
    }
    let context = Context::open(settings, selection)?;
    authored.preflight(&context.build)?;
    // The reusable same-directory installer checks no-clobber before expansion,
    // and repeats it atomically at publication. It never changes parent modes.
    let mut temporary = super::download::Temporary::create(&args.output)?;
    let operation = authored
        .build(&context.build, &mut super::random_id)?
        .into_wire(args.expected_revision.map(ObjectRevision))?;
    let request = RequestEnvelope {
        protocol: focal_wire::participant_protocol(&operation),
        ledger: context.operation.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(args.request_epoch.unwrap_or(1)),
        request_id: RequestId(
            args.request_id
                .as_deref()
                .map(parse_id)
                .transpose()?
                .map_or_else(super::random_id, Ok)?,
        ),
        operation,
    };
    let hash = checked_hash(&request)?;
    let mut bytes = BoundedBytes(Vec::new());
    serde_json::to_writer(&mut bytes, &request).map_err(|_| InputError::Capacity)?;
    // Ensure our strict offline reader can consume the file before publishing.
    let _: RequestEnvelope = parse_document(&bytes.0, InputFormat::Json)?;
    temporary.file.write_all(&bytes.0)?;
    temporary.file.write_all(b"\n")?;
    temporary.install(&args.output)?;
    report(&args.output, &request, hash, "built; not submitted")
}
fn read(path: &Path) -> Result<RequestEnvelope> {
    let bytes = super::documents::read_bytes(path, MAX_INPUT_BYTES)?;
    let request: RequestEnvelope = parse_document(&bytes, InputFormat::Json)?;
    let original: serde_json::Value = parse_document(&bytes, InputFormat::Json)?;
    let mut canonical = BoundedBytes(Vec::new());
    serde_json::to_writer(&mut canonical, &request).map_err(|_| InputError::Capacity)?;
    let canonical: serde_json::Value = parse_document(&canonical.0, InputFormat::Json)?;
    if original != canonical {
        return Err(InputError::Invalid(
            "raw request must contain exactly the complete typed envelope fields",
        )
        .into());
    }
    Ok(request)
}
fn checked_hash(request: &RequestEnvelope) -> Result<blake3::Hash> {
    let limits = WireLimits::default();
    focal_wire::check_request_shape(request, &limits)
        .map_err(|e| CliError::Input(e.to_string()))?;
    let bytes = focal_wire::encode_payload(request, limits.max_frame_bytes)
        .map_err(|e| CliError::Input(e.to_string()))?;
    Ok(blake3::hash(&bytes))
}
pub(super) fn check(path: &Path) -> Result<()> {
    let request = read(path)?;
    let hash = checked_hash(&request)?;
    report(
        path,
        &request,
        hash,
        "locally checked; authentication, authority, epoch admission and server acceptance unchecked",
    )
}
pub(super) fn send(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    selection: Option<&str>,
    file: &Path,
) -> Result<()> {
    // Preserve the original expert `request FILE` transport path, including its
    // 1 MiB JSON bound and complete wire operation surface. `check` is a separate
    // stricter offline tool; it is not an authority gate on existing raw clients.
    let request: RequestEnvelope =
        serde_json::from_slice(&super::documents::read_bytes(file, 1024 * 1024)?)
            .map_err(|error| CliError::Input(error.to_string()))?;
    let context = Context::open(settings, selection)?;
    if request.ledger != context.operation.ledger {
        return Err(InputError::Invalid(
            "wire request ledger differs from selected client context",
        )
        .into());
    }
    let response = runtime.block_on(context.client.request(request))?;
    super::super::output_response(response).map_err(CliError::Other)
}
fn report(
    path: &Path,
    request: &RequestEnvelope,
    hash: blake3::Hash,
    condition: &str,
) -> Result<()> {
    let mut output = io::stdout().lock();
    writeln!(
        output,
        "{condition}: {:?}\nrequest {} epoch {} protocol {}\nwire BLAKE3 {hash}",
        path, request.request_id, request.request_epoch.0, request.protocol
    )?;
    output.flush()?;
    Ok(())
}
/// A hard cap for authored conversion and saved human-reviewable JSON. The
/// complete wire encoder separately enforces its negotiated frame policy.
struct BoundedBytes(Vec<u8>);
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .0
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("request input capacity"))?;
        if size >= MAX_INPUT_BYTES {
            return Err(io::Error::other("request input capacity"));
        }
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| io::Error::other("request input capacity"))?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
