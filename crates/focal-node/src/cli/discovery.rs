//! Offline discovery from the released authored registry and actual Clap tree.
//! No node state, client journal, network connection or async runtime is opened.
use super::{CliError, Result, args::OutputFormat};
use clap::{Subcommand, ValueEnum, builder::PossibleValuesParser};
use clap_complete::{Generator, Shell};
use focal_client::operations::{self, Capability, OperationDescriptor, ResultKind};
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
    /// frame tags, actor, descriptor, CLI path and exposure.
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
    },
    /// Print a normalized authored input; replace illustrative existing-object IDs.
    Example {
        #[arg(value_parser = operation_names())]
        operation: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Direction {
    Input,
    Output,
}
pub(crate) fn operation_names() -> PossibleValuesParser {
    PossibleValuesParser::new(operations::descriptors().iter().map(|value| value.name))
}
fn native_only_names() -> impl Iterator<Item = &'static str> {
    operations::native_descriptors()
        .iter()
        .map(|value| value.name)
        .filter(|name| operations::find(name).is_none())
}
fn schema_names() -> PossibleValuesParser {
    PossibleValuesParser::new(
        ["test-report", "error-report", "domain-registry"]
            .into_iter()
            .chain(operations::descriptors().iter().map(|value| value.name))
            .chain(native_only_names()),
    )
}

pub(crate) fn schema(command: SchemaCommand) -> Result<()> {
    let mut output = LimitedWriter::new(io::stdout().lock(), MAX_DISCOVERY_BYTES);
    match command {
        SchemaCommand::Validate {
            operation,
            input,
            shape_only: true,
        } => {
            validate(&operation, input, None)?;
        }
        SchemaCommand::Validate { .. } => {
            return Err(CliError::Input(
                "schema validation requires selected client context or --shape-only".into(),
            ));
        }
        SchemaCommand::List { format } => list(format, &mut output)?,
        SchemaCommand::Coverage { format } => coverage(format, &mut output)?,
        SchemaCommand::Get {
            name,
            direction,
            native,
        } => get(&name, direction, native, &mut output)?,
        SchemaCommand::Example { operation } => {
            json(&mut output, &example(&operation)?)?;
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
            },
            result: row.result,
            reads: row.reads,
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
                "OPERATION                 TAGS     ACTOR      EXPOSURE       DESCRIPTOR           CLI"
            )?;
            for row in rows {
                writeln!(
                    output,
                    "{:<25} {:<8} {:<10} {:<14} {:<20} {}",
                    row.operation,
                    row.frame_tags
                        .iter()
                        .map(|tag| tag.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    row.actor,
                    row.exposure,
                    row.descriptor.unwrap_or("-"),
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
    let descriptor = if native || operations::find(name).is_none() {
        operations::find_native(name)
    } else {
        operations::find(name)
    };
    if let Some(descriptor) = descriptor {
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
        example_available: raw_example(value.name).is_some(),
    }
}
struct Catalog;
impl Serialize for Catalog {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let descriptors = operations::descriptors();
        let mut sequence = serializer.serialize_seq(Some(descriptors.len()))?;
        for descriptor in descriptors {
            sequence.serialize_element(&summary(descriptor))?;
        }
        sequence.end()
    }
}
fn list(format: OutputFormat, output: &mut dyn Write) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct Inventory {
                schema_version: u16,
                builtins: [&'static str; 3],
                operations: Catalog,
            }
            super::output::structured_to(
                output,
                &Inventory {
                    schema_version: 1,
                    builtins: ["test-report", "error-report", "domain-registry"],
                    operations: Catalog,
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
                "\nOPERATION                 MODE      EXAMPLE  DESCRIPTION"
            )?;
            for descriptor in operations::descriptors() {
                let value = summary(descriptor);
                writeln!(
                    output,
                    "{:<25} {:<9} {:<8} {}",
                    value.name,
                    if value.mutation { "mutation" } else { "read" },
                    if value.example_available { "yes" } else { "no" },
                    value.description,
                )?;
            }
            Ok(())
        }
    }
}

// These are authored inputs, not fabricated server outcomes. Object references
// are valid illustrative IDs that the caller must replace with actual results.
// A newly released descriptor without an example is explicitly unavailable.
fn raw_example(name: &str) -> Option<&'static str> {
    match name {
        "monitor.register" => Some(
            r#"{"owner":"00000000000000000000000000000001","roots":[{"predicate":"satisfied","claim":"00000000000000000000000000000002"}],"deadline":{"timer":"00000000000000000000000000000003","generation":1,"at":4102444800}}"#,
        ),
        "claim.wait" => Some(
            r#"{"claim":"00000000000000000000000000000001","until":"satisfied","timeout_ms":1000}"#,
        ),
        "monitor.get" => Some(r#"{"id":"00000000000000000000000000000004"}"#),
        "claim.submit" => Some(
            r#"{"target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}"#,
        ),
        "claim.submit_batch" => Some(
            r#"{"claims":[{"target":"self","action":"handoff","description":"Deliver the checked report","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the report testament","evaluator":"self"}]}]}"#,
        ),
        "claim.post" | "validation.begin" | "validation.complete" => {
            Some(r#"{"claim":"00000000000000000000000000000001"}"#)
        }
        "validation.begin_increment" => Some(
            r#"{"claim":"00000000000000000000000000000010","validation":"00000000000000000000000000000012","target_hash":"1111111111111111111111111111111111111111111111111111111111111111","manifest":"2222222222222222222222222222222222222222222222222222222222222222"}"#,
        ),
        "claim.cancel" => Some(
            r#"{"claim":"00000000000000000000000000000001","reason":"Work is no longer needed"}"#,
        ),
        "claim.progress" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"message":"Report prepared"}"#,
        ),
        "receipt.acquire" => Some(r#"{"claim":"00000000000000000000000000000001","epoch":1}"#),
        "evidence.begin" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1}}"#,
        ),
        "testament.submit" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"evidence_set":"00000000000000000000000000000003","manifest":[],"summary":"Closing the response; supply the actual manifest when evidence is required","confidence":"committed","outcome":"complete"}"#,
        ),
        "artifact.submit" => Some(
            r#"{"claim":"00000000000000000000000000000001","receipt":{"id":"00000000000000000000000000000002","epoch":1},"evidence_set":"00000000000000000000000000000003","kind":"test-report","schema_hash":"SCHEMA","payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}"#,
        ),
        "artifact.register" => Some(
            r#"{"id":"00000000000000000000000000000004","kind":"test-report","schema_hash":"SCHEMA","payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}"#,
        ),
        "testament.receive" => Some(
            r#"{"claim":"00000000000000000000000000000001","testament":"00000000000000000000000000000004"}"#,
        ),
        "validation.submit" => Some(
            r#"{"validation":"00000000000000000000000000000005","target_hash":"1111111111111111111111111111111111111111111111111111111111111111","phase":"whole_work","epoch":1,"handler":{"id":"00000000000000000000000000000006","version":"2222222222222222222222222222222222222222222222222222222222222222","agentic":false},"attempt":0,"manifest":"3333333333333333333333333333333333333333333333333333333333333333","receipt":{"id":"00000000000000000000000000000002","epoch":1},"value":"pass","evidence":[{"id":"00000000000000000000000000000007","hash":"4444444444444444444444444444444444444444444444444444444444444444"}]}"#,
        ),
        "claim.supersede" => Some(
            r#"{"predecessor":"00000000000000000000000000000001","successor":{"id":"00000000000000000000000000000005","occurrence":"00000000000000000000000000000006","target":"self","action":"handoff","description":"Deliver the corrected report","validations":[{"id":"00000000000000000000000000000007","kind":"receipt","phase":"whole_work","mode":"required","description":"Receive the corrected report testament","evaluator":"self"}]}}"#,
        ),
        "claim.get" | "testament.get" | "artifact.get" | "validation.get"
        | "validation.context" => Some(r#"{"id":"00000000000000000000000000000001"}"#),
        "validator.get" => Some(
            r#"{"id":"00000000000000000000000000000006","version":"2222222222222222222222222222222222222222222222222222222222222222"}"#,
        ),
        "validator.list" | "ledger.summary" => Some("{}"),
        "ledger.traverse" => Some(
            r#"{"roots":["claim:00000000000000000000000000000001"],"edges":["requirement"],"depth":1,"limit":32}"#,
        ),
        "claim.list" | "testament.list" | "artifact.list" | "validation.list" => Some("{}"),
        "request.epoch" => Some(r#"{"epoch":1}"#),
        "request.status" => Some(r#"{"epoch":1,"request_id":"00000000000000000000000000000001"}"#),
        _ => None,
    }
}
fn example(name: &str) -> Result<serde_json::Value> {
    let source = raw_example(name).ok_or_else(|| {
        CliError::Input("no authored example is available for this released operation".into())
    })?;
    let expanded;
    let source = if matches!(name, "artifact.submit" | "artifact.register") {
        expanded = source.replace("SCHEMA", &focal_evidence::test_report_schema().to_string());
        expanded.as_str()
    } else {
        source
    };
    let authored = operations::parse_json(name, source.as_bytes())?;
    // Normalize through the actual typed decoder/serializer without expanding
    // random IDs or claiming that the illustrative referenced objects exist.
    let mut normalized: serde_json::Value =
        serde_json::from_slice(&authored.canonical_intent()?)
            .map_err(|_| CliError::Input("invalid normalized authored example".into()))?;
    normalized
        .as_object_mut()
        .and_then(|value| value.remove("input"))
        .ok_or_else(|| CliError::Input("normalized authored input is absent".into()))
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

pub(super) fn validate(
    operation: &str,
    input: super::args::DocumentInput,
    context: Option<&super::Context>,
) -> Result<()> {
    let authored = super::request_files::authored(operation, input)?;
    if let Some(context) = context {
        authored.preflight(&context.build)?;
    }
    let mut output = io::stdout().lock();
    writeln!(
        output,
        "valid {} ({})",
        operation,
        if context.is_some() {
            "authored input and selected-identity preflight; server acceptance unchecked"
        } else {
            "document shape only; identity and domain semantics unchecked"
        }
    )?;
    output.flush()?;
    Ok(())
}
