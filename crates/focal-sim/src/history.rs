//! Checks an instrumented linearization witness against caller-visible history.
//! This is not a black-box search for an arbitrary valid linearization: the
//! simulator supplies ordered publication events from the actual state owner.
use focal_model::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub enum Request {
    Mutation {
        ledger: LedgerId,
        key: RequestKey,
        command_hash: ContentHash,
    },
    Read {
        ledger: LedgerId,
        consistency: Consistency,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum Consistency {
    Linearizable,
    AtLeast(SessionSeq),
    Exact(SessionSeq),
}
#[derive(Debug, Clone)]
pub enum Outcome {
    Committed(MutationReceipt),
    Read {
        sequence: SessionSeq,
        state_hash: ContentHash,
    },
    Refused,
    Unknown,
}
#[derive(Debug, Clone)]
pub enum Event {
    Invoke {
        call: u64,
        request: Request,
    },
    /// Must occur only after durable commitment and complete graph publication.
    Publish {
        receipt: MutationReceipt,
        state_hash: ContentHash,
    },
    Complete {
        call: u64,
        outcome: Outcome,
    },
}
#[derive(Debug, Clone, Copy)]
pub struct Initial {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub state_hash: ContentHash,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub publications: usize,
    pub reads: usize,
    pub retries: usize,
    pub unknown: usize,
    pub pending: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HistoryError {
    #[error("history exceeds its declared event budget")]
    Capacity,
    #[error("duplicate/missing invocation or initial ledger")]
    Identity,
    #[error("publication is not the next contiguous domain sequence")]
    Prefix,
    #[error("a request committed more than once or changed its receipt")]
    DuplicateCommit,
    #[error("success was reported before its durable publication")]
    PrematureSuccess,
    #[error("response belongs to another request or command")]
    ResponseMismatch,
    #[error("read violates its consistency contract")]
    StaleRead,
    #[error("read contents do not match their claimed committed prefix")]
    StateMismatch,
}

/// Event order is the simulator/driver observation order, independent of remote
/// machine wall clocks. Per-prefix hashes must also be checked against Core's
/// serial oracle by the driving workload; this checker verifies their visibility.
pub fn check(
    initial: &[Initial],
    events: &[Event],
    max_events: usize,
) -> Result<Report, HistoryError> {
    if events.len() > max_events || initial.len() > max_events {
        return Err(HistoryError::Capacity);
    }
    let mut current = BTreeMap::new();
    let mut states = BTreeMap::new();
    for base in initial {
        if current.insert(base.ledger, base.sequence).is_some() {
            return Err(HistoryError::Identity);
        }
        states.insert((base.ledger, base.sequence), base.state_hash);
    }
    let mut active = BTreeMap::new();
    let mut seen_calls = std::collections::BTreeSet::new();
    let mut receipts = BTreeMap::<(LedgerId, RequestKey), (usize, MutationReceipt)>::new();
    let mut report = Report::default();
    for (position, event) in events.iter().enumerate() {
        match event {
            Event::Invoke { call, request } => {
                if !seen_calls.insert(*call) {
                    return Err(HistoryError::Identity);
                }
                let ledger = match request {
                    Request::Mutation { ledger, .. } | Request::Read { ledger, .. } => ledger,
                };
                let prefix = *current.get(ledger).ok_or(HistoryError::Identity)?;
                active.insert(*call, (position, prefix, request));
            }
            Event::Publish {
                receipt,
                state_hash,
            } => {
                let prefix = current
                    .get_mut(&receipt.ledger)
                    .ok_or(HistoryError::Identity)?;
                if prefix.0.checked_add(1) != Some(receipt.sequence.0) {
                    return Err(HistoryError::Prefix);
                }
                if receipts
                    .insert((receipt.ledger, receipt.key), (position, receipt.clone()))
                    .is_some()
                {
                    return Err(HistoryError::DuplicateCommit);
                }
                *prefix = receipt.sequence;
                states.insert((receipt.ledger, receipt.sequence), *state_hash);
                report.publications = report
                    .publications
                    .checked_add(1)
                    .ok_or(HistoryError::Capacity)?;
            }
            Event::Complete { call, outcome } => {
                let (invoked, prefix, request) =
                    active.remove(call).ok_or(HistoryError::Identity)?;
                match (request, outcome) {
                    (_, Outcome::Unknown) => {
                        report.unknown = report
                            .unknown
                            .checked_add(1)
                            .ok_or(HistoryError::Capacity)?;
                    }
                    (_, Outcome::Refused) => {}
                    (
                        Request::Mutation {
                            ledger,
                            key,
                            command_hash,
                        },
                        Outcome::Committed(receipt),
                    ) => {
                        if receipt.ledger != *ledger
                            || receipt.key != *key
                            || receipt.command_hash != *command_hash
                        {
                            return Err(HistoryError::ResponseMismatch);
                        }
                        let (published, known) = receipts
                            .get(&(*ledger, *key))
                            .ok_or(HistoryError::PrematureSuccess)?;
                        if known != receipt {
                            return Err(HistoryError::ResponseMismatch);
                        }
                        if *published < invoked {
                            report.retries = report
                                .retries
                                .checked_add(1)
                                .ok_or(HistoryError::Capacity)?;
                        }
                    }
                    (
                        Request::Read {
                            ledger,
                            consistency,
                        },
                        Outcome::Read {
                            sequence,
                            state_hash,
                        },
                    ) => {
                        let latest = current.get(ledger).ok_or(HistoryError::Identity)?;
                        let valid = match consistency {
                            Consistency::Linearizable => *sequence >= prefix && sequence <= latest,
                            Consistency::AtLeast(minimum) => {
                                sequence >= minimum && sequence <= latest
                            }
                            Consistency::Exact(exact) => sequence == exact && sequence <= latest,
                        };
                        if !valid {
                            return Err(HistoryError::StaleRead);
                        }
                        if states.get(&(*ledger, *sequence)) != Some(state_hash) {
                            return Err(HistoryError::StateMismatch);
                        }
                        report.reads = report.reads.checked_add(1).ok_or(HistoryError::Capacity)?;
                    }
                    _ => return Err(HistoryError::ResponseMismatch),
                }
            }
        }
    }
    report.pending = active.len();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn initial() -> Initial {
        Initial {
            ledger: LedgerId {
                tenant: TenantId::from_u128(1),
                session: SessionId::from_u128(2),
            },
            sequence: SessionSeq(0),
            state_hash: ContentHash([0; 32]),
        }
    }
    fn receipt() -> MutationReceipt {
        MutationReceipt {
            ledger: initial().ledger,
            key: RequestKey {
                principal: ParticipantId::from_u128(1),
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(1),
            },
            sequence: SessionSeq(1),
            command_hash: ContentHash([1; 32]),
            outcome: CommandResult::EpochAdmitted(RequestEpoch(1)),
        }
    }
    fn invocation(call: u64) -> Event {
        let r = receipt();
        Event::Invoke {
            call,
            request: Request::Mutation {
                ledger: r.ledger,
                key: r.key,
                command_hash: r.command_hash,
            },
        }
    }
    #[test]
    fn lost_reply_retry_is_one_commit_and_concurrent_read_can_observe_earlier_prefix() {
        let events = vec![
            invocation(1),
            Event::Invoke {
                call: 2,
                request: Request::Read {
                    ledger: initial().ledger,
                    consistency: Consistency::Linearizable,
                },
            },
            Event::Publish {
                receipt: receipt(),
                state_hash: ContentHash([2; 32]),
            },
            Event::Complete {
                call: 1,
                outcome: Outcome::Unknown,
            },
            Event::Complete {
                call: 2,
                outcome: Outcome::Read {
                    sequence: SessionSeq(0),
                    state_hash: initial().state_hash,
                },
            },
            invocation(3),
            Event::Complete {
                call: 3,
                outcome: Outcome::Committed(receipt()),
            },
        ];
        assert_eq!(
            check(&[initial()], &events, 20).unwrap(),
            Report {
                publications: 1,
                reads: 1,
                retries: 1,
                unknown: 1,
                pending: 0
            }
        );
    }
    #[test]
    fn speculative_success_and_stale_linearizable_read_are_rejected() {
        assert_eq!(
            check(
                &[initial()],
                &[
                    invocation(1),
                    Event::Complete {
                        call: 1,
                        outcome: Outcome::Committed(receipt())
                    }
                ],
                10
            ),
            Err(HistoryError::PrematureSuccess)
        );
        let events = [
            Event::Publish {
                receipt: receipt(),
                state_hash: ContentHash([2; 32]),
            },
            Event::Invoke {
                call: 1,
                request: Request::Read {
                    ledger: initial().ledger,
                    consistency: Consistency::Linearizable,
                },
            },
            Event::Complete {
                call: 1,
                outcome: Outcome::Read {
                    sequence: SessionSeq(0),
                    state_hash: initial().state_hash,
                },
            },
        ];
        assert_eq!(
            check(&[initial()], &events, 10),
            Err(HistoryError::StaleRead)
        );
    }
}
