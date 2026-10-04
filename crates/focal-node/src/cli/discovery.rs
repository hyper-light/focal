//! Offline discovery from the released authored registries and actual Clap
//! tree. No node state, client journal, network connection or async runtime
//! is opened. Both engines' catalogues are served, selected the way the
//! mutation path selects them (`focal_client::operations::select_offline`):
//! `--native` wins, else the only catalogue that has the name, else V1.
use super::{CliError, Result, args::OutputFormat};
use clap::{Subcommand, ValueEnum, builder::PossibleValuesParser};
use clap_complete::{Generator, Shell};
use focal_client::operations::{
    self, ApplicationDocument, Capability, OperationDescriptor, ResultKind, WireProfile,
};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use std::io::{self, Write};

// A local output policy; all inputs are compiled descriptors, never ledger data.
const MAX_DISCOVERY_BYTES: usize = 1024 * 1024;

#[derive(Subcommand)]
pub(crate) enum SchemaCommand {
    /// List built-in payload schemas and released authored operation contracts.
    List {
        #[arg(long, value_enum, default_value = "table")]
        format: OutputFormat,
        /// List the native engine's catalogue (version 2) instead of the V1
        /// catalogue a fresh ledger runs.
        #[arg(long)]
        native: bool,
    },
    /// Print a payload contract or an operation's authored input/result schema.
    Get {
        #[arg(value_parser = schema_names())]
        name: String,
        /// Applies to operation schemas, not built-in payload contracts.
        #[arg(long, value_enum)]
        direction: Option<Direction>,
        /// Select the native engine's descriptor (version 2) for a shared name.
        #[arg(long)]
        native: bool,
    },
    /// The native operation coverage table: every owner operation with its
    /// frame tags, actor, descriptor, CLI path, exposure and example.
    Coverage {
        #[arg(long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Validate an authored document locally; no request or journal is created.
    Validate {
        #[arg(value_parser = operation_names())]
        operation: String,
        #[command(flatten)]
        input: super::args::DocumentInput,
        /// Check bounded DTO shape only, without loading any node/client identity.
        #[arg(long)]
        shape_only: bool,
        /// Validate against the native engine's contract (version 2). With a
        /// selected context the ledger's engine is probed and must agree.
        #[arg(long)]
        native: bool,
    },
    /// Print a normalized authored input; replace illustrative existing-object IDs.
    Example {
        #[arg(value_parser = operation_names())]
        operation: String,
        /// Print the native engine's example (version 2) for a shared name.
        #[arg(long)]
        native: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Direction {
    Input,
    Output,
}
/// Every application operation of either engine, for `example` and `validate`.
pub(crate) fn operation_names() -> PossibleValuesParser {
    PossibleValuesParser::new(all_operation_names())
}
/// The V1 catalogue alone: `request build` writes a legacy raw envelope.
pub(crate) fn legacy_operation_names() -> PossibleValuesParser {
    PossibleValuesParser::new(operations::descriptors().iter().map(|value| value.name))
}
/// The union of both catalogues in name order, each name once.
fn all_operation_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = operations::descriptors()
        .iter()
        .chain(operations::native_descriptors())
        .map(|value| value.name)
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}
fn schema_names() -> PossibleValuesParser {
    PossibleValuesParser::new(
        ["test-report", "error-report", "domain-registry"]
            .into_iter()
            .chain(all_operation_names()),
    )
}
fn wire_of(native: bool) -> WireProfile {
    if native {
        WireProfile::Native
    } else {
        WireProfile::V1
    }
}
fn engine_name(wire: WireProfile) -> &'static str {
    match wire {
        WireProfile::V1 => "v1",
        WireProfile::Native => "native",
    }
}

pub(crate) fn schema(command: SchemaCommand) -> Result<()> {
    let mut output = LimitedWriter::new(io::stdout().lock(), MAX_DISCOVERY_BYTES);
    match command {
        SchemaCommand::Validate {
            operation,
            input,
            shape_only: true,
            native,
        } => {
            validate_offline(&operation, input, native)?;
        }
        SchemaCommand::Validate { .. } => {
            return Err(CliError::Input(
                "schema validation requires selected client context or --shape-only".into(),
            ));
        }
        SchemaCommand::List { format, native } => list(wire_of(native), format, &mut output)?,
        SchemaCommand::Coverage { format } => coverage(format, &mut output)?,
        SchemaCommand::Get {
            name,
            direction,
            native,
        } => get(&name, direction, native, &mut output)?,
        SchemaCommand::Example { operation, native } => {
            json(&mut output, &example(native, &operation)?)?;
        }
    }
    output.flush()?;
    Ok(())
}

pub(crate) fn completion(shell: Shell) -> Result<()> {
    let mut output = LimitedWriter::new(io::stdout().lock(), MAX_DISCOVERY_BYTES);
    completion_to(shell, &mut output)?;
    output.flush()?;
    Ok(())
}
fn completion_to(shell: Shell, output: &mut dyn Write) -> Result<()> {
    // Fish::try_generate in pinned clap_complete 4.6.6 still expects I/O success.
    // Save the first write error, then discard subsequent writes without
    // allocation to avoid that dependency panic. Contain static tree assertions
    // separately without changing the global panic hook.
    let mut deferred = DeferredError {
        output,
        error: None,
    };
    let generated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut command = super::command_tree::completion_command();
        command.set_bin_name("focal");
        command.build();
        shell.try_generate(&command, &mut deferred)
    }));
    if let Some(error) = deferred.error {
        return Err(error.into());
    }
    generated.map_err(|_| {
        CliError::Input("completion generator could not build the CLI tree".into())
    })??;
    Ok(())
}

struct DeferredError<'a> {
    output: &'a mut dyn Write,
    error: Option<io::Error>,
}
impl Write for DeferredError<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.error.is_none() {
            self.error = self.output.write_all(bytes).err();
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.error.is_none() {
            self.error = self.output.flush().err();
        }
        Ok(())
    }
}

fn coverage(format: OutputFormat, output: &mut dyn Write) -> Result<()> {
    use focal_client::operations::{NativeActor, NativeExposure, native_coverage_table};
    #[derive(Serialize)]
    struct Row {
        operation: &'static str,
        frame_tags: &'static [u8],
        actor: &'static str,
        descriptor: Option<&'static str>,
        cli: &'static str,
        exposure: &'static str,
        result: &'static str,
        reads: &'static str,
        /// Whether `schema example DESCRIPTOR --native` prints a document.
        example: bool,
    }
    let rows: Vec<Row> = native_coverage_table()
        .into_iter()
        .map(|row| Row {
            operation: row.operation.name(),
            frame_tags: row.tags,
            actor: match row.actor {
                NativeActor::Issuer => "issuer",
                NativeActor::Subject => "subject",
                NativeActor::Evaluator => "evaluator",
                NativeActor::Owner => "owner",
                NativeActor::Internal => "internal",
            },
            descriptor: row.name,
            cli: row.cli,
            exposure: match row.exposure {
                NativeExposure::AuthoredTool => "authored_tool",
                NativeExposure::InternalTimer => "internal_timer",
                NativeExposure::WireOnly => "wire_only",
                NativeExposure::Activation => "activation",
                NativeExposure::Retirement => "retirement",
                NativeExposure::Seal => "seal",
                NativeExposure::ClientProtocol => "client_protocol",
            },
            result: row.result,
            reads: row.reads,
            example: row
                .name
                .is_some_and(|name| operations::example(WireProfile::Native, name).is_ok()),
        })
        .collect();
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct Table {
                schema_version: u16,
                retry: &'static str,
                operations: Vec<Row>,
            }
            super::output::structured_to(
                output,
                &Table {
                    schema_version: 1,
                    retry: focal_client::operations::NATIVE_RETRY,
                    operations: rows,
                },
                format,
            )
        }
        OutputFormat::Table => {
            writeln!(
                output,
                "OPERATION                 TAGS     ACTOR      EXPOSURE       DESCRIPTOR                  EXAMPLE  CLI"
            )?;
            for row in rows {
                writeln!(
                    output,
                    "{:<25} {:<8} {:<10} {:<14} {:<27} {:<8} {}",
                    row.operation,
                    row.frame_tags
                        .iter()
                        .map(|tag| tag.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    row.actor,
                    row.exposure,
                    row.descriptor.unwrap_or("-"),
                    if row.example { "yes" } else { "-" },
                    if row.cli.is_empty() { "-" } else { row.cli },
                )?;
            }
            Ok(())
        }
    }
}
fn get(
    name: &str,
    direction: Option<Direction>,
    native: bool,
    output: &mut dyn Write,
) -> Result<()> {
    let builtin = matches!(name, "test-report" | "error-report" | "domain-registry");
    if !builtin {
        let wire = operations::select_offline(native, name)?;
        let descriptor = operations::find_application(wire, name)
            .ok_or_else(|| CliError::Input("unknown released schema name".into()))?;
        let value = match direction.unwrap_or(Direction::Input) {
            Direction::Input => descriptor.input_schema()?,
            Direction::Output => descriptor.output_schema()?,
        };
        return json(output, &value);
    }
    if direction.is_some() {
        return Err(CliError::Input(
            "--direction applies only to authored operation schemas".into(),
        ));
    }
    if native {
        return Err(CliError::Input(
            "--native applies only to authored operation schemas; a built-in payload contract has no engine".into(),
        ));
    }
    match name {
        "test-report" => {
            #[derive(Serialize)]
            struct Report<'a> {
                schema_version: u16,
                name: &'a str,
                hash: String,
                descriptor: &'a str,
                example: ReportExample,
            }
            #[derive(Serialize)]
            struct ReportExample {
                passed: u64,
                failed: u64,
                skipped: u64,
            }
            let descriptor = std::str::from_utf8(focal_evidence::TEST_REPORT_SCHEMA)
                .map_err(|_| CliError::Input("invalid built-in test-report schema".into()))?;
            json(
                output,
                &Report {
                    schema_version: 1,
                    name: "focal.test_report.v1",
                    hash: focal_evidence::test_report_schema().to_string(),
                    descriptor,
                    example: ReportExample {
                        passed: 1,
                        failed: 0,
                        skipped: 0,
                    },
                },
            )
        }
        "domain-registry" => {
            writeln!(
                output,
                "{}",
                include_str!("../../../../config/schema/domain-registry-v1.json")
            )?;
            Ok(())
        }
        "error-report" => {
            #[derive(Serialize)]
            struct Report<'a> {
                schema_version: u16,
                name: &'a str,
                hash: String,
                descriptor: &'a str,
                max_payload_bytes: usize,
                example: ReportExample<'a>,
            }
            #[derive(Serialize)]
            struct ReportExample<'a> {
                code: &'a str,
                message: &'a str,
                details: &'a str,
            }
            let descriptor = std::str::from_utf8(focal_evidence::ERROR_REPORT_SCHEMA)
                .map_err(|_| CliError::Input("invalid built-in error-report schema".into()))?;
            json(
                output,
                &Report {
                    schema_version: 1,
                    name: "focal.error_report.v1",
                    hash: focal_evidence::error_report_schema().to_string(),
                    descriptor,
                    max_payload_bytes: focal_evidence::ERROR_REPORT_MAX_BYTES,
                    example: ReportExample {
                        code: "tool_unavailable",
                        message: "The required tool could not run",
                        details: "No test result was produced",
                    },
                },
            )
        }
        _ => Err(CliError::Input("unknown released schema name".into())),
    }
}

#[derive(Serialize)]
struct OperationSummary<'a> {
    name: &'a str,
    /// The engine whose catalogue this row belongs to (`v1` or `native`).
    engine: &'static str,
    version: u16,
    description: &'a str,
    capability: &'static str,
    mutation: bool,
    destructive: bool,
    result_kind: &'static str,
    max_input_bytes: usize,
    example_available: bool,
}
fn summary(value: &OperationDescriptor) -> OperationSummary<'_> {
    OperationSummary {
        name: value.name,
        engine: engine_name(value.wire),
        version: value.version,
        description: value.description,
        capability: match value.capability {
            Capability::Actor => "actor",
            Capability::Evaluator => "evaluator",
            Capability::Runtime => "runtime",
            Capability::Node => "node",
            Capability::FounderNode => "founder_node",
        },
        mutation: value.mutation,
        destructive: value.destructive,
        result_kind: match value.result_kind {
            ResultKind::Mutation => "mutation",
            ResultKind::Read => "read",
            ResultKind::List => "list",
            ResultKind::Reconcile => "reconcile",
        },
        max_input_bytes: value.max_input_bytes,
        example_available: operations::example(value.wire, value.name).is_ok(),
    }
}
struct Catalog(WireProfile);
impl Serialize for Catalog {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let descriptors = operations::application(self.0);
        let mut sequence = serializer.serialize_seq(Some(descriptors.len()))?;
        for descriptor in descriptors {
            sequence.serialize_element(&summary(descriptor))?;
        }
        sequence.end()
    }
}
fn list(wire: WireProfile, format: OutputFormat, output: &mut dyn Write) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct Inventory {
                schema_version: u16,
                engine: &'static str,
                builtins: [&'static str; 3],
                operations: Catalog,
            }
            super::output::structured_to(
                output,
                &Inventory {
                    schema_version: 1,
                    engine: engine_name(wire),
                    builtins: ["test-report", "error-report", "domain-registry"],
                    operations: Catalog(wire),
                },
                format,
            )
        }
        OutputFormat::Table => {
            writeln!(
                output,
                "BUILT-IN SCHEMAS\ntest-report\nerror-report\ndomain-registry"
            )?;
            writeln!(
                output,
                "\nOPERATION                   ENGINE  VERSION  MODE      EXAMPLE  DESCRIPTION"
            )?;
            for descriptor in operations::application(wire) {
                let value = summary(descriptor);
                writeln!(
                    output,
                    "{:<27} {:<7} {:<8} {:<9} {:<8} {}",
                    value.name,
                    value.engine,
                    value.version,
                    if value.mutation { "mutation" } else { "read" },
                    if value.example_available { "yes" } else { "no" },
                    value.description,
                )?;
            }
            Ok(())
        }
    }
}

/// The normalized example of `name` on the engine offline selection picks:
/// the engine's own authored example, decoded by the decoder the mutation
/// path uses and re-serialized with every default expanded.
fn example(native: bool, name: &str) -> Result<serde_json::Value> {
    let wire = operations::select_offline(native, name)?;
    Ok(operations::example(wire, name)?)
}
fn json(output: &mut dyn Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer_pretty(&mut *output, value)
        .map_err(|error| CliError::Other(Box::new(error)))?;
    writeln!(output)?;
    Ok(())
}

struct LimitedWriter<W> {
    inner: W,
    written: usize,
    limit: usize,
}
impl<W: Write> LimitedWriter<W> {
    fn new(inner: W, limit: usize) -> Self {
        Self {
            inner,
            written: 0,
            limit,
        }
    }
}
impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .written
            .checked_add(bytes.len())
            .filter(|end| *end <= self.limit)
            .ok_or_else(|| io::Error::other("discovery output exceeds its byte limit"))?;
        let written = self.inner.write(bytes)?;
        if written == bytes.len() {
            self.written = end;
        } else {
            self.written = self
                .written
                .checked_add(written)
                .ok_or_else(|| io::Error::other("discovery output length overflow"))?;
        }
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;

/// `schema validate --shape-only`: the document decodes through the engine
/// offline selection picks; nothing is loaded, journaled or sent.
pub(super) fn validate_offline(
    operation: &str,
    input: super::args::DocumentInput,
    native: bool,
) -> Result<()> {
    let wire = operations::select_offline(native, operation)?;
    let bytes = super::request_files::document_bytes(input)?;
    let document = operations::decode_application(wire, operation, &bytes)?;
    report_valid(
        operation,
        wire,
        None,
        "document shape only; identity and domain semantics unchecked",
    )?;
    drop(document);
    Ok(())
}

/// `schema validate` under a selected context: the ledger's engine is probed
/// the way every mutation probes it, an explicit `--native` must agree, and
/// the document is checked the way that engine's mutation path checks it
/// before it journals anything. V1 runs the shared builder's preflight; the
/// native engine reads the committed bindings the verb needs at a fixed
/// prefix, compiles the exact frame with throwaway identities, encodes and
/// fingerprints it. No request, journal identity or mutation is created.
pub(super) fn validate_online(
    runtime: &tokio::runtime::Runtime,
    context: &super::Context,
    operation: &str,
    input: super::args::DocumentInput,
    native: bool,
) -> Result<()> {
    use focal_client::operations::ThrowawayIds;
    use focal_model::RequestId;
    let engine = super::native::detect(runtime, context)?;
    let wire = operations::select_online(&engine, native)?;
    let bytes = super::request_files::document_bytes(input)?;
    let document = operations::decode_application(wire, operation, &bytes)?;
    let checked = match document {
        ApplicationDocument::V1(authored) => {
            authored.preflight(&context.build)?;
            "authored input and selected-identity preflight; server acceptance unchecked"
        }
        ApplicationDocument::Native(authored) => {
            let standing = engine.standing().ok_or(CliError::InvalidResponse)?;
            let profile = super::native::profile(standing);
            focal_native_client::admissible(profile, &authored)?;
            let resolved = focal_native_client::resolve(
                context.build.ledger,
                &authored,
                &mut super::native::reads(runtime, context),
            )?;
            let limits = focal_native_client::CompileLimits::default();
            let mut ids = ThrowawayIds::excluding(&authored.canonical_intent()?)?;
            let compiled = focal_native_client::compile(
                &authored,
                &context.build,
                focal_model::RequestKey {
                    principal: context.build.actor,
                    epoch: focal_model::RequestEpoch(1),
                    id: RequestId(super::random_id()?),
                },
                &mut ids,
                &resolved,
                &limits,
            )?;
            let frame = focal_native_client::encode_frame(
                context.build.ledger,
                profile,
                &compiled.input,
                limits.encoding(),
            )?;
            focal_native_client::fingerprint(&frame, limits.native, limits.frame)?;
            "authored input compiled against the ledger's committed bindings into its exact frame; nothing journaled or sent; server acceptance unchecked"
        }
        ApplicationDocument::NativeList(list) => {
            focal_native_client::list_request(&list, &context.build)?;
            "authored list resolved to its wire request; nothing sent"
        }
        ApplicationDocument::NativeRead(_) => "authored read decoded; nothing sent",
    };
    report_valid(operation, wire, Some(&engine), checked)
}
fn report_valid(
    operation: &str,
    wire: WireProfile,
    engine: Option<&operations::Engine>,
    checked: &str,
) -> Result<()> {
    let mut output = io::stdout().lock();
    let assumed = if engine.is_some_and(operations::Engine::assumed) {
        ", assumed: the owner was unreachable"
    } else {
        ""
    };
    writeln!(
        output,
        "valid {operation} ({} engine{assumed}; {checked})",
        engine_name(wire)
    )?;
    output.flush()?;
    Ok(())
}
