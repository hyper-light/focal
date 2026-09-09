use super::{CliError, args::OutputFormat};
use focal_client::failure::{self, Failure};
use serde::Serialize;
use std::{error::Error, io::Write};

pub(crate) fn classification(error: &(dyn Error + 'static)) -> Failure {
    if let Some(error) = error.downcast_ref::<CliError>() {
        return match error {
            CliError::Input(_) => Failure::error("invalid_input", 2),
            CliError::Document(error) => failure::input(error),
            CliError::Client(error) => failure::client(error),
            CliError::Pending(error) => failure::pending(error),
            CliError::Managed(error) => failure::managed_store(error),
            CliError::NotFound => Failure::error("not_found", 4),
            CliError::Ambiguous => Failure::error("ambiguous", 5),
            CliError::WaitUnfinished(condition) => Failure {
                condition: condition.as_str(),
                code: match condition {
                    focal_client::claim_wait::ClaimWaitCondition::Pending => "wait_pending",
                    _ => "wait_unmet",
                },
                exit_code: 6,
            },
            CliError::Incomplete => Failure {
                condition: "Incomplete",
                code: "incomplete",
                exit_code: 6,
            },
            CliError::InvalidResponse => Failure::error("invalid_response", 1),
            CliError::Domain(_) => Failure {
                condition: "DomainOutcome",
                code: "domain",
                exit_code: 5,
            },
            CliError::Unconfirmed => Failure::outcome_unknown(),
            CliError::NativeRefused(refusal) => failure::native(refusal),
            CliError::NativeStore(error) => failure::native_store(error),
            CliError::NativeCompile(error) => match error {
                focal_native_client::CompileError::Input(error) => failure::input(error),
                focal_native_client::CompileError::Contract(_) => {
                    Failure::error("invalid_input", 2)
                }
                focal_native_client::CompileError::Capacity(_) => Failure::error("capacity", 6),
                focal_native_client::CompileError::Codec(_) => Failure::error("native_frame", 1),
                focal_native_client::CompileError::Missing(_) => Failure::error("not_found", 4),
                focal_native_client::CompileError::Unsupported(_) => {
                    Failure::error("operation_conflict", 5)
                }
            },
            CliError::Other(error) => classification(error.as_ref()),
            CliError::Io(error) => io(error),
        };
    }
    if let Some(value) = failure::classify(error) {
        return value;
    }
    if let Some(error) = error.downcast_ref::<focal_node::cluster_admin::ClusterAdminError>() {
        return error.classification();
    }
    if let Some(error) = error.downcast_ref::<focal_node::placement::PlacementError>() {
        use focal_node::placement::PlacementError;
        return match error {
            PlacementError::Capacity => Failure::error("capacity", 6),
            PlacementError::Identity => Failure::error("invalid_input", 2),
            _ => Failure::error("guarantee_unsatisfied", 9),
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return io(error);
    }
    if let Some(error) = error.downcast_ref::<serde_json::Error>() {
        return error
            .io_error_kind()
            .map(io_kind)
            .unwrap_or(Failure::error("serialization", 1));
    }
    if let Some(error) = error.downcast_ref::<serde_saphyr::SerializeError>() {
        return match error {
            serde_saphyr::SerializeError::IO { error } => io(error),
            _ => Failure::error("serialization", 1),
        };
    }
    Failure::error("operation_failed", 1)
}
fn io(error: &std::io::Error) -> Failure {
    io_kind(error.kind())
}
fn io_kind(kind: std::io::ErrorKind) -> Failure {
    match kind {
        std::io::ErrorKind::BrokenPipe => Failure::error("broken_pipe", 1),
        std::io::ErrorKind::Interrupted => Failure::cancelled(),
        std::io::ErrorKind::WouldBlock => Failure::error("busy", 6),
        _ => Failure::error("io", 1),
    }
}

/// Read the chosen output format from Clap's parsed command, not by scanning
/// argv (where a document or message may legitimately contain `--format`).
pub(crate) fn format(matches: &clap::ArgMatches) -> OutputFormat {
    let mut current = matches;
    let mut format = OutputFormat::Table;
    loop {
        if let Ok(Some(value)) = current.try_get_one::<OutputFormat>("format") {
            format = *value;
        }
        match current.subcommand() {
            Some((_, child)) => current = child,
            None => return format,
        }
    }
}

/// Final diagnostics go to stderr. Existing partial output and exact recovery
/// references stay where the operation emitted them; no second stdout result.
pub(crate) fn report(
    error: &(dyn Error + 'static),
    format: OutputFormat,
    output: &mut impl Write,
) -> std::io::Result<()> {
    let failure = classification(error);
    match format {
        OutputFormat::Table => {
            writeln!(output, "focal: [{}] {error}", failure.code)?;
            let mut source = error.source();
            // A foreign Error implementation may expose a cyclic source chain.
            for _ in 0..16 {
                let Some(cause) = source else {
                    break;
                };
                writeln!(output, "  caused by: {cause}")?;
                source = cause.source();
            }
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml => {
            #[derive(Serialize)]
            struct Diagnostic<'a> {
                schema_version: u16,
                condition: &'a str,
                error: ErrorBody<'a>,
            }
            #[derive(Serialize)]
            struct ErrorBody<'a> {
                code: &'a str,
                exit_code: i32,
                message: String,
            }
            let value = Diagnostic {
                schema_version: 1,
                condition: failure.condition,
                error: ErrorBody {
                    code: failure.code,
                    exit_code: failure.exit_code,
                    message: bounded_message(error),
                },
            };
            match format {
                OutputFormat::Json => {
                    serde_json::to_writer(&mut *output, &value).map_err(std::io::Error::other)?;
                    writeln!(output)
                }
                OutputFormat::Yaml => {
                    writeln!(output, "---")?;
                    serde_saphyr::to_io_writer(output, &value).map_err(std::io::Error::other)
                }
                OutputFormat::Table => Ok(()),
            }
        }
    }
}
fn bounded_message(error: &(dyn Error + 'static)) -> String {
    use std::fmt::Write;
    struct Limited(String);
    impl std::fmt::Write for Limited {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let left = (16 * 1024usize).saturating_sub(self.0.len());
            if value.len() > left {
                let end = value
                    .char_indices()
                    .map(|(index, _)| index)
                    .take_while(|index| *index <= left)
                    .last()
                    .unwrap_or(0);
                self.0.push_str(value.get(..end).ok_or(std::fmt::Error)?);
                return Err(std::fmt::Error);
            }
            self.0.push_str(value);
            Ok(())
        }
    }
    let mut value = Limited(String::new());
    if value.0.try_reserve_exact(16 * 1024).is_err() {
        return String::new();
    }
    let _ = write!(&mut value, "{error}");
    value.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn parsed_format_never_interprets_authored_document_as_an_option() {
        let matches = crate::Args::command()
            .try_get_matches_from([
                "focal",
                "submit",
                "claim",
                "--json",
                r#"{"description":"--format yaml"}"#,
                "--format",
                "json",
            ])
            .unwrap();
        assert!(matches!(format(&matches), OutputFormat::Json));
        let matches = crate::Args::command()
            .try_get_matches_from([
                "focal",
                "submit",
                "claim",
                "--json",
                r#"{"description":"--format json"}"#,
            ])
            .unwrap();
        assert!(matches!(format(&matches), OutputFormat::Table));
    }
    #[test]
    fn boxed_coordinator_capacity_and_expired_view_have_distinct_codes() {
        let error = CliError::Other(Box::new(
            focal_client::managed_requests::ManagedRequestsError::Remote(
                focal_wire::AccessError::SnapshotExpired,
            ),
        ));
        assert_eq!(
            classification(&error),
            Failure::error("snapshot_expired", 8)
        );
        let error = CliError::Other(Box::new(focal_client::watch::WatchError::Managed(
            focal_client::managed_store::ManagedStoreError::Capacity,
        )));
        assert_eq!(classification(&error), Failure::error("capacity", 6));
    }
    #[test]
    fn structured_errors_preserve_unicode_bounds_and_failed_stderr_is_fallible() {
        let error = CliError::Input("é".repeat(20_000));
        for format in [OutputFormat::Json, OutputFormat::Yaml] {
            let mut bytes = vec![];
            report(&error, format, &mut bytes).unwrap();
            let value: serde_json::Value = match format {
                OutputFormat::Json => serde_json::from_slice(&bytes).unwrap(),
                _ => serde_saphyr::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap(),
            };
            assert_eq!(value["error"]["code"], "invalid_input");
            assert_eq!(value["error"]["exit_code"], 2);
            assert!(value["error"]["message"].as_str().unwrap().len() <= 16 * 1024);
        }
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        assert!(report(&error, OutputFormat::Json, &mut Closed).is_err());
        for format in [OutputFormat::Json, OutputFormat::Yaml] {
            let failed = super::super::output::structured_to(
                Closed,
                &serde_json::json!({"value": 1}),
                format,
            )
            .unwrap_err();
            assert_eq!(classification(&failed), Failure::error("broken_pipe", 1));
        }
    }
}
