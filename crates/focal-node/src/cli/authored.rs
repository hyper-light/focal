//! Flag adaptation only: the shared client registry validates and compiles all
//! authored domain input. Journal identity and presentation remain in the CLI.
use super::{CliError, Result, args::*, documents, lifecycle};
use focal_client::operations::*;

pub(super) fn mutation(command: Commands) -> Result<(AuthoredOperation, MutationOptions)> {
    Ok(match command {
        Commands::Monitor {
            command: super::monitor::MonitorCommand::Register(args),
        } => super::monitor::authored(*args)?,
        Commands::Monitor {
            command: super::monitor::MonitorCommand::Get(_),
        } => return Err(CliError::Input("read passed to mutation builder".into())),
        Commands::Submit { command } => match command {
            SubmitCommand::Claim(args) => {
                let (document, options) = documents::claim(args)?;
                (AuthoredOperation::ClaimSubmit(document), options)
            }
            SubmitCommand::Claims(args) => {
                let (document, options) = documents::claim_batch(args)?;
                (AuthoredOperation::ClaimSubmitBatch(document), options)
            }
            SubmitCommand::Testament(args) => {
                let (document, options) = documents::testament(args)?;
                (AuthoredOperation::TestamentSubmit(document), options)
            }
            SubmitCommand::Artifact(args) => {
                let (document, options) = documents::artifact(args)?;
                (AuthoredOperation::ArtifactSubmit(document), options)
            }
            SubmitCommand::Validation(args) => lifecycle::validation(*args)?,
        },
        Commands::Claim { command } => match command {
            ClaimCommand::Wait(_) => {
                return Err(CliError::Input("read passed to mutation builder".into()));
            }
            ClaimCommand::Post(args) => lifecycle::claim_post(args)?,
            ClaimCommand::Progress(args) => lifecycle::claim_progress(args)?,
            ClaimCommand::Cancel(args) => lifecycle::claim_cancel(args)?,
            ClaimCommand::Supersede(args) => {
                let (successor, options) = documents::claim(args.successor)?;
                (
                    AuthoredOperation::ClaimSupersede(SupersedeDocument {
                        predecessor: args.predecessor,
                        successor,
                    }),
                    options,
                )
            }
        },
        Commands::Receipt {
            command: ReceiptCommand::Acquire(args),
        } => lifecycle::receipt(args)?,
        Commands::Evidence {
            command: EvidenceCommand::Begin(args),
        } => lifecycle::evidence(args)?,
        Commands::Testament {
            command: TestamentCommand::Receive(args),
        } => lifecycle::receive(args)?,
        Commands::Validation { command } => match command {
            ValidationCommand::Begin(args) => lifecycle::validation_claim(args, false)?,
            ValidationCommand::BeginIncrement(args) => {
                let fields = args.claim.is_some()
                    || args.validation.is_some()
                    || args.target_hash.is_some()
                    || args.manifest.is_some();
                let document = match args.input.load(fields)? {
                    Some(document) => document,
                    None => IncrementValidationDocument {
                        claim: documents::required(args.claim, "claim")?,
                        validation: documents::required(args.validation, "validation")?,
                        target_hash: documents::required(args.target_hash, "target-hash")?,
                        manifest: documents::required(args.manifest, "manifest")?,
                    },
                };
                (
                    AuthoredOperation::ValidationBeginIncrement(document),
                    args.mutation,
                )
            }
            ValidationCommand::Complete(args) => lifecycle::validation_claim(args, true)?,
        },
        Commands::Artifact {
            command: ArtifactCommand::Register(args),
        } => lifecycle::register(*args)?,
        Commands::Artifact {
            command: ArtifactCommand::Upload { .. },
        }
        | Commands::Get { .. }
        | Commands::List { .. }
        | Commands::Ledger { .. }
        | Commands::Validator { .. }
        | Commands::Watch { .. } => {
            return Err(CliError::Input("read passed to mutation builder".into()));
        }
    })
}
pub(super) fn filters(args: Filters) -> ListDocument {
    ListDocument {
        claim: args.claim,
        testament: args.testament,
        source: args.source,
        target: args.target,
        status: args.status,
        action: args.action,
        producer: args.producer,
        kind: args.kind,
        schema_hash: args.schema_hash,
        evaluator: args.evaluator,
        phase: args.phase,
        mode: args.mode,
        scopes: args.scopes,
        relations: args.relations,
        caused_by: args.caused_by,
        inputs: args.inputs,
        outcome: args.outcome,
        confidence: args.confidence,
        created_after: args.created_after,
        created_through: args.created_through,
        ..ListDocument::default()
    }
}

pub(super) fn scope_filter(
    value: &str,
) -> std::result::Result<focal_client::input::ScopeDocument, String> {
    let (kind, key) = value.split_once(':').ok_or("scope requires KIND:KEY")?;
    Ok(focal_client::input::ScopeDocument {
        kind: kind.into(),
        key: key.into(),
    })
}
pub(super) fn relation_filter(
    value: &str,
) -> std::result::Result<focal_client::input::ClaimRelationDocument, String> {
    let (kind, target) = value
        .split_once('=')
        .ok_or("relation requires KIND=TYPE:TARGET")?;
    Ok(focal_client::input::ClaimRelationDocument {
        kind: kind.into(),
        target: target.into(),
    })
}
pub(super) fn input_filter(
    value: &str,
) -> std::result::Result<focal_client::input::ObjectReferenceDocument, String> {
    let (kind, id) = value.split_once(':').ok_or("input requires KIND:ID")?;
    Ok(focal_client::input::ObjectReferenceDocument {
        kind: kind.into(),
        id: id.into(),
    })
}
