//! Flag adaptation only: the shared client registry validates and compiles all
//! authored domain input. Journal identity and presentation remain in the CLI.
use super::{CliError, Result, args::*, documents};
use focal_client::{input::ReceiptDocument, operations::*};

pub(super) fn mutation(command: Commands) -> Result<(AuthoredOperation, MutationOptions)> {
    Ok(match command {
        Commands::Submit { command } => match command {
            SubmitCommand::Claim(args) => {
                let (document, options) = documents::claim(args)?;
                (AuthoredOperation::ClaimSubmit(document), options)
            }
            SubmitCommand::Testament(args) => {
                let (document, options) = documents::testament(args)?;
                (AuthoredOperation::TestamentSubmit(document), options)
            }
            SubmitCommand::Artifact(args) => {
                let (document, options) = documents::artifact(args)?;
                (AuthoredOperation::ArtifactSubmit(document), options)
            }
        },
        Commands::Claim { command } => match command {
            ClaimCommand::Post(args) => (
                AuthoredOperation::ClaimPost(ClaimIdDocument { claim: args.id }),
                args.mutation,
            ),
            ClaimCommand::Progress(args) => (
                AuthoredOperation::ClaimProgress(ProgressDocument {
                    claim: args.id,
                    receipt: ReceiptDocument {
                        id: args.receipt,
                        epoch: args.receipt_epoch,
                    },
                    message: args.message,
                }),
                args.mutation,
            ),
            ClaimCommand::Cancel(args) => (
                AuthoredOperation::ClaimCancel(CancelDocument {
                    claim: args.id,
                    reason: args.reason,
                }),
                args.mutation,
            ),
        },
        Commands::Receipt {
            command: ReceiptCommand::Acquire(args),
        } => (
            AuthoredOperation::ReceiptAcquire(AcquireReceiptDocument {
                claim: args.claim,
                id: args.id,
                epoch: args.epoch,
            }),
            args.mutation,
        ),
        Commands::Evidence {
            command: EvidenceCommand::Begin(args),
        } => (
            AuthoredOperation::EvidenceBegin(BeginEvidenceDocument {
                claim: args.claim,
                receipt: ReceiptDocument {
                    id: args.receipt,
                    epoch: args.receipt_epoch,
                },
                id: args.id,
            }),
            args.mutation,
        ),
        Commands::Get { .. } | Commands::List { .. } => {
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
        ..ListDocument::default()
    }
}
