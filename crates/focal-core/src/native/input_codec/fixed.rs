//! Allocation-free typed construction for the fixed-size subset of the dormant
//! input format. A decoded actor or timer still requires the existing owner
//! checks. This module supplies neither semantic approval nor trusted delivery.
use super::bytes::{Cursor, Error};
use super::*;
use focal_model::{
    ArtifactRef, ObjectId, ObjectRevision, RequestEpoch, RequestId, SessionId, TenantId,
};

#[cfg(test)]
#[path = "fixed_tests.rs"]
mod tests;

/// Complete typed fixed input. Timer firing time and authenticated actor/node
/// context remain outside these authored fields, exactly as in `InputFrame`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Keep typed input on the stack before owner memory admission.
pub enum FixedFrame {
    Request {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeInput,
    },
    EvaluationDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeDeadlineInput,
    },
    ClaimDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeClaimDeadlineInput,
    },
    MonitorDeadline {
        ledger: LedgerId,
        profile: NativeContentProfile,
        input: NativeMonitorDeadlineInput,
    },
}

impl FixedFrame {
    pub fn as_frame(&self) -> InputFrame<'_> {
        match self {
            Self::Request {
                ledger,
                profile,
                input,
            } => InputFrame::Request {
                ledger: *ledger,
                profile: *profile,
                input,
            },
            Self::EvaluationDeadline {
                ledger,
                profile,
                input,
            } => InputFrame::EvaluationDeadline {
                ledger: *ledger,
                profile: *profile,
                input: *input,
            },
            Self::ClaimDeadline {
                ledger,
                profile,
                input,
            } => InputFrame::ClaimDeadline {
                ledger: *ledger,
                profile: *profile,
                input: *input,
            },
            Self::MonitorDeadline {
                ledger,
                profile,
                input,
            } => InputFrame::MonitorDeadline {
                ledger: *ledger,
                profile: *profile,
                input: *input,
            },
        }
    }
}

impl StructuralInput<'_> {
    /// Reparse the complete fixed frame with a separate finite visit allowance.
    /// Dynamic commands return `None` without a second traversal; their complete
    /// structural inspection has already succeeded. No heap is allocated, and
    /// successful typed construction does not certify semantic validity. For a
    /// fixed frame, `self.quote().visits` is the exact typed-pass allowance: both
    /// passes read the same primitive boundaries without collections or text.
    pub fn decode_fixed(&self, max_visits: usize) -> Result<Option<FixedFrame>, CodecError> {
        let expected = self.header();
        if matches!(
            expected.kind,
            FrameKind::Request {
                command: 0 | 4 | 6 | 7 | 9 | 13 | 15 | 19 | 24 | 27
            }
        ) {
            return Ok(None);
        }
        let bytes = self.bytes();
        let mut cursor = Cursor::new(bytes, bytes.len(), max_visits)?;
        if cursor.fixed::<8>()? != MAGIC || cursor.u16()? != VERSION {
            return Err(Error::InvalidTag("input format"));
        }
        let profile = match cursor.u8()? {
            0 => NativeContentProfile::ProjectionOnly,
            1 => NativeContentProfile::AuthoredV1,
            _ => return Err(Error::InvalidTag("content profile")),
        };
        let namespace = cursor.u8()?;
        let ledger = ledger(&mut cursor)?;
        if ledger != expected.ledger || profile != expected.profile {
            return Err(Error::InvalidTag("input header"));
        }
        let frame = match (namespace, expected.kind) {
            (
                0,
                FrameKind::Request {
                    command: expected_tag,
                },
            ) => {
                let request = RequestKey {
                    principal: ParticipantId(cursor.fixed()?),
                    epoch: RequestEpoch(cursor.u64()?),
                    id: RequestId(cursor.fixed()?),
                };
                let tag = cursor.u8()?;
                if tag != expected_tag || Some(request) != expected.request {
                    return Err(Error::InvalidTag("input header"));
                }
                FixedFrame::Request {
                    ledger,
                    profile,
                    input: NativeInput {
                        request,
                        command: command(&mut cursor, tag)?,
                    },
                }
            }
            (1, FrameKind::EvaluationDeadline) => FixedFrame::EvaluationDeadline {
                ledger,
                profile,
                input: NativeDeadlineInput {
                    evaluation: evaluation(&mut cursor)?,
                    deadline: deadline(&mut cursor)?,
                },
            },
            (2, FrameKind::ClaimDeadline) => FixedFrame::ClaimDeadline {
                ledger,
                profile,
                input: NativeClaimDeadlineInput {
                    claim: ClaimId(cursor.fixed()?),
                    deadline: deadline(&mut cursor)?,
                },
            },
            (3, FrameKind::MonitorDeadline) => FixedFrame::MonitorDeadline {
                ledger,
                profile,
                input: NativeMonitorDeadlineInput {
                    claim: ClaimId(cursor.fixed()?),
                    monitor: MonitorId(cursor.fixed()?),
                    deadline: deadline(&mut cursor)?,
                },
            },
            _ => return Err(Error::InvalidTag("input namespace")),
        };
        cursor.finish()?;
        Ok(Some(frame))
    }
}

pub(super) fn ledger(cursor: &mut Cursor<'_>) -> Result<LedgerId, Error> {
    Ok(LedgerId {
        tenant: TenantId(cursor.fixed()?),
        session: SessionId(cursor.fixed()?),
    })
}

pub(super) fn binding(cursor: &mut Cursor<'_>) -> Result<Binding, Error> {
    Ok(Binding {
        ledger: ledger(cursor)?,
        object: ObjectId(cursor.fixed()?),
        content: ContentHash(cursor.fixed()?),
        revision: ObjectRevision(cursor.u64()?),
    })
}

pub(super) fn deadline(cursor: &mut Cursor<'_>) -> Result<Deadline, Error> {
    Ok(Deadline {
        timer: TimerId(cursor.fixed()?),
        generation: cursor.u64()?,
        at: cursor.u64()?,
    })
}

pub(super) fn receipt(cursor: &mut Cursor<'_>) -> Result<ReceiptFence, Error> {
    Ok(ReceiptFence {
        receipt: ReceiptId(cursor.fixed()?),
        epoch: cursor.u64()?,
    })
}

pub(super) fn optional_receipt(cursor: &mut Cursor<'_>) -> Result<Option<ReceiptFence>, Error> {
    match cursor.u8()? {
        0 => Ok(None),
        1 => Ok(Some(receipt(cursor)?)),
        _ => Err(Error::InvalidTag("option")),
    }
}

pub(super) fn artifact_ref(cursor: &mut Cursor<'_>) -> Result<ArtifactRef, Error> {
    Ok(ArtifactRef {
        id: ArtifactId(cursor.fixed()?),
        hash: ContentHash(cursor.fixed()?),
    })
}

pub(super) fn evaluation(cursor: &mut Cursor<'_>) -> Result<EvaluationKey, Error> {
    let claim = ClaimId(cursor.fixed()?);
    let validation = ValidationId(cursor.fixed()?);
    let generation = cursor.u64()?;
    let target = match cursor.u8()? {
        0 => EvaluationTarget::Admission,
        1 => EvaluationTarget::Increment {
            artifact: ArtifactId(cursor.fixed()?),
        },
        2 => EvaluationTarget::Work {
            response: TestamentId(cursor.fixed()?),
            slot: cursor.u32()?,
            artifact: ArtifactId(cursor.fixed()?),
        },
        3 => EvaluationTarget::MissingSlot {
            response: TestamentId(cursor.fixed()?),
            slot: cursor.u32()?,
        },
        4 => EvaluationTarget::Delivery {
            response: TestamentId(cursor.fixed()?),
        },
        _ => return Err(Error::InvalidTag("evaluation target")),
    };
    Ok(EvaluationKey {
        claim,
        validation,
        target,
        generation,
    })
}

fn command(cursor: &mut Cursor<'_>, tag: u8) -> Result<NativeCommand, Error> {
    match tag {
        1 => Ok(NativeCommand::Cancel {
            expected: binding(cursor)?,
        }),
        2 => Ok(NativeCommand::Post {
            expected: binding(cursor)?,
        }),
        3 => Ok(NativeCommand::BeginAdmission {
            claim: binding(cursor)?,
            key: evaluation(cursor)?,
            expected: binding(cursor)?,
        }),
        5 => Ok(NativeCommand::AcquireReceipt {
            expected: binding(cursor)?,
            receipt: ReceiptId(cursor.fixed()?),
        }),
        8 => Ok(NativeCommand::ReceiveWork {
            claim: binding(cursor)?,
            expected: binding(cursor)?,
        }),
        10 => Ok(NativeCommand::PostResponse {
            claim: binding(cursor)?,
            expected: binding(cursor)?,
        }),
        11 => Ok(NativeCommand::ReceiveResponse {
            claim: binding(cursor)?,
            expected: binding(cursor)?,
        }),
        12 => Ok(NativeCommand::FailWorkProduction {
            claim: binding(cursor)?,
            slot: cursor.u32()?,
            diagnostic: artifact_ref(cursor)?,
        }),
        14 => Ok(NativeCommand::BeginIncrement {
            claim: binding(cursor)?,
            key: evaluation(cursor)?,
            expected: binding(cursor)?,
        }),
        16 => Ok(NativeCommand::SealIncrementTargets {
            claim: binding(cursor)?,
        }),
        17 => Ok(NativeCommand::EnterWholeWork {
            claim: binding(cursor)?,
            expected: binding(cursor)?,
        }),
        18 => Ok(NativeCommand::BeginWork {
            claim: binding(cursor)?,
            key: evaluation(cursor)?,
            expected: binding(cursor)?,
        }),
        20 => Ok(NativeCommand::GenerateResultTestament {
            claim: binding(cursor)?,
            id: TestamentId(cursor.fixed()?),
        }),
        21 => Ok(NativeCommand::PostResultTestament {
            expected: binding(cursor)?,
        }),
        22 => Ok(NativeCommand::AdoptReceipt {
            expected: binding(cursor)?,
            previous: receipt(cursor)?,
            receipt: ReceiptId(cursor.fixed()?),
            holder: ParticipantId(cursor.fixed()?),
        }),
        23 => Ok(NativeCommand::ReleaseScope {
            expected: binding(cursor)?,
        }),
        25 => Ok(NativeCommand::RebindMonitor {
            expected: binding(cursor)?,
            receipt: optional_receipt(cursor)?,
            id: MonitorId(cursor.fixed()?),
            predecessor: binding(cursor)?,
            successor: binding(cursor)?,
        }),
        26 => Ok(NativeCommand::CancelMonitor {
            expected: binding(cursor)?,
            receipt: optional_receipt(cursor)?,
            id: MonitorId(cursor.fixed()?),
        }),
        _ => Err(Error::InvalidTag("fixed command")),
    }
}
