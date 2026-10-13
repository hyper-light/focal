//! Runs a [`WorkloadShape`] end to end and measures it. `concurrency` workers
//! — one OS thread each, with its own client and its own current-thread
//! runtime, so they are N independent callers as N processes would be —
//! submit native claim creations, then linearizable claim reads, over the
//! real `focal_client::Client` path: against an `EmbeddedNode` opened in this
//! process (the embedded transport), or against a running `focal start node` node
//! over its Unix socket. Every request is timed from send to reply. In
//! embedded mode `reopen: true` then closes the node, reopens its directory
//! and times that to the first linearizable read: single-node recovery at
//! exactly this run's retained size.
use crate::authored;
use crate::error::LoadError;
use crate::generations::{Finished, Generations};
use crate::measurements::{
    self, RefusalCause, WriteMeasurements, WriteOutcome, WriteSample, WriteSamples,
};
use crate::native;
use crate::report::{self, Latency, Report};
use crate::shape::{Lifecycle, Profile, Transport, WorkloadShape};
use focal_client::operations::NativeAuthoredOperation;
use focal_client::{
    Client, ClientError, EmbeddedTransport, LocalTransport, QuicTransport, RetryPolicy,
};
use focal_ledger::NativeContentProfile;
use focal_model::{
    ClaimId, LedgerId, ParticipantId, RequestEpoch, RequestId, RequestKey, RootCommandId,
    RouteEpoch, SessionSeq,
};
use focal_node::{
    config::Settings,
    embedded::{EmbeddedNode, decode_identity},
    host::LocalHost,
};
use focal_wire::{
    AccessError, AuthenticatedPeer, NativeClaimExpand, NativeErrorCode, NativeMutationReply,
    NativeObject, NativeObjectRef, NativeReadQuery, NativeReadRequest, NativeRefusalKind,
    Operation, PeerGrant, PeerRole, QuicConnector, ReadConsistency, ReadToken, RequestEnvelope,
    Response, ResponseEnvelope, WireLimits,
};
use hyper_rt::combine::{Either, race2};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The seed sits above this bit of every identity the run uses.
const SEED_SHIFT: u32 = 40;
/// The worker index sits above this bit, under the seed.
const WORKER_SHIFT: u32 = 32;
/// Claim ids start here within a worker's space; request ids start at 1.
const CLAIM_OFFSET: u128 = 0x4000_0000;
/// Read request ids start here within a worker's space.
const READ_OFFSET: u128 = 0x8000_0000;
/// The request ids of a worker's floor advances and window reads (the
/// audit's F12) start here within its space.
const PROTOCOL_OFFSET: u128 = 0xC000_0000;
/// Request ids a write may take: the request, and its re-issue when the
/// owner refused it by name.
const ATTEMPTS: u128 = 2;
/// The worker index the reopen probe uses; above every real worker.
const PROBE_WORKER: u128 = 0xFF;
/// Distinct refusal reasons kept per run, the last place for those past the others
/// ([`OTHER_REASONS`]).
const MAX_REFUSALS: usize = 8;
/// What the refusals past the kept reasons are counted under: the reasons sum to the
/// refusals they name, however many kinds there were.
const OTHER_REASONS: &str = "(other reasons)";
/// Characters kept of one refusal reason.
const MAX_REASON_CHARS: usize = 200;
/// Directory entries the size walk visits before it refuses to count further.
const MAX_WALK_ENTRIES: usize = 100_000;
/// Directory depth the size walk descends to.
const MAX_WALK_DEPTH: usize = 16;

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

/// What the run's connections are opened to.
enum Connector {
    Embedded(LocalHost),
    Unix(PathBuf),
    /// An enrolled client's journal: each connection is dialed with a TLS
    /// configuration made for it from the journal's credential, as a
    /// connector takes the one it is bound with.
    Enrolled(Box<focal_node::network_join::PendingClientJoin>),
}

/// The identities every frame names.
#[derive(Clone, Copy)]
struct Names {
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    /// The ledger's root command, which an authored creation cites as its cause.
    root: RootCommandId,
    profile: NativeContentProfile,
}

enum Conn {
    Embedded(Client<EmbeddedTransport<LocalHost>>),
    Unix(Client<LocalTransport>),
    Enrolled(Client<QuicTransport>),
}
impl Conn {
    fn open(connector: &Connector, names: &Names, limits: &WireLimits) -> Result<Self, LoadError> {
        match connector {
            Connector::Embedded(host) => {
                let peer = AuthenticatedPeer::local(PeerGrant {
                    principal: names.issuer,
                    tenants: BTreeSet::from([names.ledger.tenant]),
                    role: PeerRole::Runtime,
                })
                .map_err(|error| LoadError::Fixture(format!("peer grant: {error:?}")))?;
                let transport = EmbeddedTransport::new(peer, host.clone(), limits.clone())?;
                Ok(Self::Embedded(Client::new(
                    transport,
                    RetryPolicy::default(),
                    limits.clone(),
                    1,
                )?))
            }
            Connector::Unix(socket) => {
                let transport = LocalTransport::connect(socket, limits.clone())?;
                Ok(Self::Unix(Client::new(
                    transport,
                    RetryPolicy::default(),
                    limits.clone(),
                    1,
                )?))
            }
            Connector::Enrolled(pending) => {
                let client = pending.remote_client(unix_now()?, limits)?;
                let initial = client.initial;
                let address = if initial.endpoint.starts_with('[') {
                    "[::]:0"
                } else {
                    "0.0.0.0:0"
                }
                .parse()
                .map_err(|_| LoadError::Shape("local address".into()))?;
                // The connection's endpoint is its own owner.
                let connector =
                    QuicConnector::bind_standalone(address, client.tls, limits.clone())?;
                // One route, one connection: the transport's bound is the
                // routes a client may hold, as the CLI's enrolled context.
                let transport = QuicTransport::new(connector, initial, 16)?;
                Ok(Self::Enrolled(Client::new(
                    transport,
                    RetryPolicy::default(),
                    limits.clone(),
                    1,
                )?))
            }
        }
    }
    async fn request(&self, envelope: RequestEnvelope) -> Result<ResponseEnvelope, ClientError> {
        match self {
            Self::Embedded(client) => client.request(envelope).await,
            Self::Unix(client) => client.request(envelope).await,
            Self::Enrolled(client) => client.request(envelope).await,
        }
    }
}

/// One worker's share of a phase, everything it needs owned.
struct Job {
    index: u16,
    /// Write phase: the most of this worker's writes under way at once.
    inflight: u16,
    /// The callers of the phase, among which `index` is one.
    callers: u16,
    /// Writes to submit, or reads to issue.
    count: u64,
    /// The first identity of this worker's space.
    base: u128,
    names: Names,
    limits: WireLimits,
    /// Read phase: the claims this worker created, cycled through.
    created: Vec<u128>,
    /// Write phase: what becomes of each claim after its creation.
    lifecycle: Lifecycle,
    /// Write phase: the most of this worker's lifecycle reads under way at once.
    lifecycle_reads: u16,
    /// Write phase, paced (the shape's `rate`): the interval between this
    /// worker's intended starts.
    pace: Option<Duration>,
}

#[derive(Clone, Copy)]
enum Phase {
    Write,
    Read,
}

/// What a worker measured: `(start offset, latency)` in nanoseconds per
/// request, the counts, and the claims it created.
#[derive(Default)]
struct Outcome {
    samples: Vec<(u128, u128)>,
    writes: WriteSamples,
    committed: u64,
    refused: u64,
    unknown: u64,
    hits: u64,
    created: Vec<u128>,
    refusals: Vec<(String, u64)>,
    /// Claims whose every lifecycle step committed.
    lifecycles: u64,
    /// Lifecycle steps never sent: the read of the claim they bind to
    /// failed.
    abandoned: u64,
}

/// Counts one refusal of `reason`: a reason already seen counts once more,
/// and a new one is kept while fewer than `MAX_REFUSALS` are (each count is
/// what decides a fix: a refusal at the session's door costs the network a
/// round, one after the owner prepared costs the owner too).
fn note(refusals: &mut Vec<(String, u64)>, reason: String) {
    note_count(refusals, reason, 1);
}
/// A reason as it is counted: each run of digits read as `#`, so a refusal that names its own
/// figures (a memory refusal's request and room, an epoch) is counted with the others of its
/// kind.
fn reason_shape(reason: &str) -> String {
    let mut shaped = String::new();
    let mut digits = false;
    for character in reason.chars().take(MAX_REASON_CHARS) {
        if character.is_ascii_digit() {
            if !digits {
                shaped.push('#');
            }
            digits = true;
        } else {
            shaped.push(character);
            digits = false;
        }
    }
    shaped
}
fn note_count(refusals: &mut Vec<(String, u64)>, reason: String, count: u64) {
    let reason = reason_shape(&reason);
    if let Some((_, seen)) = refusals.iter_mut().find(|(seen, _)| *seen == reason) {
        *seen = seen.saturating_add(count);
        return;
    }
    if refusals.len() < MAX_REFUSALS.saturating_sub(1) {
        refusals.push((reason, count));
        return;
    }
    match refusals.iter_mut().find(|(seen, _)| seen == OTHER_REASONS) {
        Some((_, seen)) => *seen = seen.saturating_add(count),
        None => refusals.push((OTHER_REASONS.to_owned(), count)),
    }
}

fn read_envelope(names: &Names, request: u128, claim: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
        ledger: names.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: NativeReadQuery::Claim {
                id: ClaimId::from_u128(claim),
                expand: NativeClaimExpand::default(),
                after: None,
            },
            max_items: 1,
        }),
    }
}

fn read_hit(result: &Result<ResponseEnvelope, ClientError>) -> bool {
    matches!(result, Ok(envelope) if matches!(&envelope.result, Response::NativeRead(page) if !page.objects.is_empty()))
}

/// How the owner answered a write.
enum Reply {
    /// Committed at this native sequence.
    Committed(SessionSeq),
    /// Refused by name: the request's generation is closed, or not admitted
    /// yet (the audit's F12); never executed.
    Expired,
    Refused(String, RefusalCause),
    Unknown(String),
}

/// The cause of a refusal the node's access layer made.
fn access(error: &AccessError) -> RefusalCause {
    match error {
        AccessError::Capacity => RefusalCause::IngressCapacity,
        AccessError::Unavailable => RefusalCause::Unavailable,
        _ => RefusalCause::Other,
    }
}

fn classify(result: Result<ResponseEnvelope, ClientError>) -> Reply {
    match result {
        Ok(envelope) => match envelope.result {
            Response::Native(NativeMutationReply::Committed(receipt)) => {
                Reply::Committed(receipt.sequence)
            }
            Response::Native(NativeMutationReply::Refused(refusal))
                if matches!(
                    refusal.kind,
                    NativeRefusalKind::Refused(
                        NativeErrorCode::RequestHistoryExpired | NativeErrorCode::EpochNotAdmitted
                    )
                ) =>
            {
                Reply::Expired
            }
            Response::Native(NativeMutationReply::Refused(refusal)) => {
                // The owner names the bound that refused (its memory, its pending
                // candidates): a full session is refused for its memory.
                let cause = match refusal.kind {
                    NativeRefusalKind::Capacity
                        if refusal.detail.starts_with("native owner memory") =>
                    {
                        RefusalCause::OwnerMemory
                    }
                    NativeRefusalKind::Capacity => RefusalCause::OwnerBound,
                    _ => RefusalCause::Other,
                };
                Reply::Refused(format!("refused: {refusal:?}"), cause)
            }
            Response::Native(NativeMutationReply::Pending(_)) => {
                Reply::Unknown("pending: ticket, not a receipt".into())
            }
            Response::Error(error) => Reply::Refused(format!("access: {error:?}"), access(&error)),
            _ => Reply::Refused("unexpected reply kind".into(), RefusalCause::Other),
        },
        // The client gave up on a refusal the node made (capacity, after
        // its bounded retries): admitted nothing, so refused, not unknown.
        Err(ClientError::Access(error)) => {
            Reply::Refused(format!("access: {error:?}"), access(&error))
        }
        Err(error) => Reply::Unknown(format!("client: {error}")),
    }
}

fn window_envelope(names: &Names, request: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
        ledger: names.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: NativeReadQuery::Epochs(names.issuer),
            max_items: 1,
        }),
    }
}

/// The write `offset` of this worker: a structural creation names its claim
/// id; a compiled one mints its identities from a span of the same space.
fn build(
    job: &Job,
    offset: u128,
    request: RequestKey,
) -> Result<(RequestEnvelope, u128), LoadError> {
    match job.names.profile {
        NativeContentProfile::ProjectionOnly => {
            let claim = job
                .base
                .checked_add(CLAIM_OFFSET)
                .and_then(|id| id.checked_add(offset))
                .ok_or(LoadError::Bound("claim id space"))?;
            let envelope = native::create_envelope(
                job.names.ledger,
                job.names.issuer,
                job.names.worker,
                job.names.profile,
                request,
                claim,
            )?;
            Ok((envelope, claim))
        }
        NativeContentProfile::AuthoredV1 => {
            let first_id = offset
                .checked_mul(authored::IDS_PER_CREATION)
                .and_then(|span| job.base.checked_add(CLAIM_OFFSET)?.checked_add(span))
                .ok_or(LoadError::Bound("claim id space"))?;
            let created = authored::create_envelope(
                job.names.ledger,
                job.names.issuer,
                job.names.worker,
                job.names.root,
                request,
                first_id,
            )?;
            Ok((created.envelope, created.claim))
        }
    }
}

/// The protocol requests of one worker — floor advances and window reads —
/// numbered in their own part of its id space.
struct Protocol {
    next: u128,
}
impl Protocol {
    fn request(&mut self, job: &Job) -> Result<u128, LoadError> {
        let id = job
            .base
            .checked_add(PROTOCOL_OFFSET)
            .and_then(|first| first.checked_add(self.next))
            .filter(|_| self.next < READ_OFFSET)
            .ok_or(LoadError::Bound("protocol request id space"))?;
        self.next = self
            .next
            .checked_add(1)
            .ok_or(LoadError::Bound("protocol request id space"))?;
        Ok(id)
    }
}

/// What one worker's writes under way share on its thread: its job, client
/// and the run's generations, its protocol request ids and its outcome.
/// Nothing is held across a wait.
struct Shared<'a> {
    job: &'a Job,
    conn: &'a Conn,
    generations: &'a Generations,
    /// The worker's lifecycle reads under way, at most `job.lifecycle_reads`.
    reads: hyper_rt::sync::Semaphore,
    run_start: Instant,
    protocol: std::cell::Cell<u128>,
    outcome: std::cell::RefCell<Outcome>,
}
impl Shared<'_> {
    fn protocol_request(&self) -> Result<u128, LoadError> {
        let mut protocol = Protocol {
            next: self.protocol.get(),
        };
        let id = protocol.request(self.job)?;
        self.protocol.set(protocol.next);
        Ok(id)
    }
    fn note(&self, reason: String) {
        note(&mut self.outcome.borrow_mut().refusals, reason);
    }
    /// The owner's window, read after a refusal by name: the run issues
    /// where the owner admits. `false` when the window could not be read.
    async fn learn(&self) -> Result<bool, LoadError> {
        let request = self.protocol_request()?;
        match self
            .conn
            .request(window_envelope(&self.job.names, request))
            .await
        {
            Ok(envelope) => {
                if let Response::NativeRead(page) = &envelope.result
                    && let Some(NativeObject::Epochs(window)) = page.objects.first()
                {
                    let open: Vec<RequestEpoch> =
                        window.open.iter().map(|open| open.epoch).collect();
                    self.generations.learn(window.floor, &open)?;
                    return Ok(true);
                }
                self.note(format!("window: unexpected reply {:?}", envelope.result));
            }
            Err(error) => self.note(format!("window: client: {error}")),
        }
        Ok(false)
    }
    /// The floor advance the journal sends once the generation below
    /// drained: committed, the floor stands at `minimum`; refused by name,
    /// the window is learned; otherwise the next reply sends it again.
    async fn advance(&self, epoch: RequestEpoch, minimum: RequestEpoch) -> Result<(), LoadError> {
        let request = RequestKey {
            principal: self.job.names.issuer,
            epoch,
            id: RequestId::from_u128(self.protocol_request()?),
        };
        let names = &self.job.names;
        let envelope = native::advance_envelope(names.ledger, names.profile, request, minimum)?;
        match classify(self.conn.request(envelope).await) {
            Reply::Committed(_) => self.generations.advanced(minimum),
            Reply::Expired => {
                self.generations.advance_failed()?;
                self.learn().await.map(|_| ())
            }
            Reply::Refused(reason, _) | Reply::Unknown(reason) => {
                self.generations.advance_failed()?;
                self.note(format!("floor advance: {reason}"));
                Ok(())
            }
        }
    }
    /// Lifecycle `i`, from its intended start `started`: the claim's
    /// creation and, as the shape asks, each later step once the step before
    /// it committed. A later step is timed from its own send: an agent's
    /// next step follows its last answer, not the schedule. A step that did
    /// not commit ends the lifecycle there.
    async fn write(&self, i: u64, started: Instant) -> Result<(), LoadError> {
        let Some((claim, mut written)) = self.step(i, 0, started, None).await? else {
            return Ok(());
        };
        if self.job.lifecycle == Lifecycle::Create {
            return Ok(());
        }
        let later = [
            NativeAuthoredOperation::ClaimCancel(authored::claim_target(claim)),
            NativeAuthoredOperation::ClaimReleaseScope(authored::claim_target(claim)),
        ];
        for (step, operation) in (1u8..).zip(later.iter()) {
            let Some(objects) = self.read_claim(claim, written).await? else {
                let mut outcome = self.outcome.borrow_mut();
                outcome.abandoned = outcome.abandoned.saturating_add(1);
                return Ok(());
            };
            let Some((_, sequence)) = self
                .step(i, step, Instant::now(), Some((operation, &objects)))
                .await?
            else {
                return Ok(());
            };
            written = sequence;
        }
        let mut outcome = self.outcome.borrow_mut();
        outcome.lifecycles = outcome.lifecycles.saturating_add(1);
        Ok(())
    }
    /// The claim as this caller's own last write left it, read at least at
    /// that write's sequence (read-your-writes, no read barrier), for the
    /// next step to bind to; `None`, with the reason noted, when it could
    /// not be read.
    async fn read_claim(
        &self,
        claim: u128,
        written: SessionSeq,
    ) -> Result<Option<Vec<NativeObject>>, LoadError> {
        let Ok(_permit) = self.reads.acquire().await else {
            return Err(LoadError::Bound("lifecycle reads"));
        };
        let request = self.protocol_request()?;
        let envelope = RequestEnvelope {
            protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
            ledger: self.job.names.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(request),
            operation: Operation::NativeRead(NativeReadRequest {
                consistency: ReadConsistency::AtLeast(ReadToken {
                    ledger: self.job.names.ledger,
                    sequence: written,
                    route_epoch: RouteEpoch(1),
                }),
                query: NativeReadQuery::Objects(vec![NativeObjectRef::Claim(ClaimId::from_u128(
                    claim,
                ))]),
                max_items: 1,
            }),
        };
        match self.conn.request(envelope).await {
            Ok(envelope) => match envelope.result {
                Response::NativeRead(page) if !page.objects.is_empty() => Ok(Some(page.objects)),
                other => {
                    self.note(format!("lifecycle read: unexpected reply {other:?}"));
                    Ok(None)
                }
            },
            Err(error) => {
                self.note(format!("lifecycle read: client: {error}"));
                Ok(None)
            }
        }
    }
    /// Step `step` of lifecycle `i`, from `started`: its creation (`later`
    /// none) or a later step compiled against the claim as read. Issued in
    /// the generation the run's journal stands at; one the owner refused by
    /// name is issued once more, with the next request id, in the generation
    /// the owner admits — never executed twice. The claim and the sequence
    /// it committed at, once committed.
    async fn step(
        &self,
        i: u64,
        step: u8,
        started: Instant,
        later: Option<(&NativeAuthoredOperation, &[NativeObject])>,
    ) -> Result<Option<(u128, SessionSeq)>, LoadError> {
        let offset = u128::from(i);
        let steps = u128::from(self.job.lifecycle.steps());
        let mut attempt = 0u128;
        loop {
            let request = offset
                .checked_mul(steps)
                .and_then(|slot| slot.checked_add(u128::from(step)))
                .and_then(|slot| slot.checked_mul(ATTEMPTS))
                .and_then(|slot| slot.checked_add(attempt))
                .and_then(|slot| slot.checked_add(1))
                .filter(|slot| *slot < CLAIM_OFFSET)
                .and_then(|slot| self.job.base.checked_add(slot))
                .ok_or(LoadError::Bound("request id space"))?;
            let minted = self.generations.mint()?;
            let key = RequestKey {
                principal: self.job.names.issuer,
                epoch: minted.epoch,
                id: RequestId::from_u128(request),
            };
            let (envelope, claim) = match later {
                None => build(self.job, offset, key)?,
                Some((operation, objects)) => {
                    let names = &self.job.names;
                    let envelope = authored::step_envelope(
                        names.ledger,
                        names.profile,
                        names.issuer,
                        names.root,
                        key,
                        operation,
                        objects,
                    )?;
                    (envelope, 0)
                }
            };
            let sent = Instant::now();
            let response = self.conn.request(envelope).await;
            let finished = Instant::now();
            let reply = classify(response);
            self.outcome.borrow_mut().writes.record(WriteSample {
                start_ns: started.saturating_duration_since(self.run_start).as_nanos(),
                sent_ns: sent.saturating_duration_since(self.run_start).as_nanos(),
                finished_ns: finished
                    .saturating_duration_since(self.run_start)
                    .as_nanos(),
                worker: self.job.index,
                write: i,
                attempt,
                epoch: minted.epoch,
                request,
                outcome: match &reply {
                    Reply::Committed(_) => WriteOutcome::Committed,
                    Reply::Expired => WriteOutcome::Expired,
                    Reply::Refused(..) => WriteOutcome::Refused,
                    Reply::Unknown(_) => WriteOutcome::Unknown,
                },
                refusal: match &reply {
                    Reply::Refused(_, cause) => *cause,
                    _ => RefusalCause::None,
                },
                step,
            })?;
            if let Finished::Advance { epoch, minimum } = self.generations.finish(minted.epoch)? {
                self.advance(epoch, minimum).await?;
            }
            match reply {
                Reply::Committed(sequence) => {
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.committed = outcome.committed.saturating_add(1);
                    if later.is_none() {
                        outcome.created.push(claim);
                    }
                    return Ok(Some((claim, sequence)));
                }
                Reply::Expired => {
                    attempt = attempt.saturating_add(1);
                    if attempt < ATTEMPTS && self.learn().await? {
                        continue;
                    }
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.refused = outcome.refused.saturating_add(1);
                    note(
                        &mut outcome.refusals,
                        "refused by name in the generation the owner named".into(),
                    );
                }
                Reply::Refused(reason, _) => {
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.refused = outcome.refused.saturating_add(1);
                    note(&mut outcome.refusals, reason);
                }
                Reply::Unknown(reason) => {
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.unknown = outcome.unknown.saturating_add(1);
                    note(&mut outcome.refusals, reason);
                }
            }
            return Ok(None);
        }
    }
}

/// A worker's writes, up to `inflight` under way at once: paced, each is sent
/// at its place on the schedule whatever its earlier writes are doing (wrk2's
/// constant throughput), and one due while all are out waits for the first
/// to end, its latency still counted from its place; unpaced, `inflight` are
/// kept under way. At one, a caller waits for each answer, as it always has.
async fn write_open_loop(shared: &Shared<'_>, inflight: u16) -> Result<(), LoadError> {
    let job = shared.job;
    let inflight = usize::from(inflight.max(1));
    // The writes under way, in this task: a wake polls each of them, at most
    // `inflight`.
    let mut flying =
        focal_rt::Concurrent::new(inflight).map_err(|_| LoadError::Bound("writes in flight"))?;
    let mut i = 0u64;
    loop {
        // The next write's place, while one is left: on the schedule, or now.
        let due = if i < job.count {
            Some(match job.pace {
                Some(interval) => place(shared.run_start, interval, job, i)?,
                None => Instant::now(),
            })
        } else {
            None
        };
        if due.is_none() && flying.is_empty() {
            return Ok(());
        }
        match due.filter(|_| flying.len() < inflight) {
            // A write is due and has room: it is sent at its place, unless
            // one under way ends first.
            Some(at) => {
                let wait = focal_rt::time::sleep(at.saturating_duration_since(Instant::now()));
                let ended = if flying.is_empty() {
                    wait.await.map_err(LoadError::Runtime)?;
                    None
                } else {
                    match race2(wait, flying.join_next()).await {
                        Either::First(slept) => {
                            slept.map_err(LoadError::Runtime)?;
                            None
                        }
                        Either::Second(written) => Some(written),
                    }
                };
                match ended {
                    Some(Some(written)) => written?,
                    Some(None) => {}
                    None => {
                        flying
                            .push(shared.write(i, at))
                            .map_err(|_| LoadError::Bound("writes in flight"))?;
                        i = i.saturating_add(1);
                    }
                }
            }
            None => {
                if let Some(written) = flying.join_next().await {
                    written?;
                }
            }
        }
    }
}

/// Write `i`'s place on a paced worker's schedule: the callers' schedules
/// spread over the interval, caller `index` of `callers` at its share of it,
/// so the offered rate arrives evenly, never as every caller's request at one
/// instant.
fn place(run_start: Instant, interval: Duration, job: &Job, i: u64) -> Result<Instant, LoadError> {
    let phase = interval
        .checked_mul(u32::from(job.index))
        .and_then(|spread| spread.checked_div(u32::from(job.callers)))
        .ok_or(LoadError::Bound("schedule"))?;
    u32::try_from(i)
        .ok()
        .and_then(|i| interval.checked_mul(i))
        .and_then(|offset| offset.checked_add(phase))
        .and_then(|offset| run_start.checked_add(offset))
        .ok_or(LoadError::Bound("schedule"))
}

async fn worker(
    job: Job,
    phase: Phase,
    run_start: Instant,
    generations: &Generations,
    conn: &Conn,
) -> Result<Outcome, LoadError> {
    let count = usize::try_from(job.count).map_err(|_| LoadError::Bound("worker count"))?;
    // The shape bounds lifecycles at MAX_CLAIMS; each takes its steps, each
    // step at most ATTEMPTS samples. The complete trace is funded before
    // sending work.
    let write_limit = match phase {
        Phase::Write => count
            .checked_mul(usize::from(job.lifecycle.steps()))
            .and_then(|writes| writes.checked_mul(usize::try_from(ATTEMPTS).ok()?))
            .ok_or(LoadError::Bound("write samples"))?,
        Phase::Read => 0,
    };
    let mut outcome = Outcome {
        samples: Vec::with_capacity(match phase {
            Phase::Write => 0,
            Phase::Read => count,
        }),
        writes: WriteSamples::reserve(write_limit)?,
        ..Outcome::default()
    };
    match phase {
        Phase::Write => {
            outcome.created = Vec::with_capacity(count);
            let shared = Shared {
                job: &job,
                conn,
                generations,
                reads: hyper_rt::sync::Semaphore::new(
                    u64::from(job.lifecycle_reads),
                    usize::from(job.inflight.max(1)),
                )
                .map_err(LoadError::Runtime)?,
                run_start,
                protocol: std::cell::Cell::new(0),
                outcome: std::cell::RefCell::new(outcome),
            };
            write_open_loop(&shared, job.inflight).await?;
            outcome = shared.outcome.into_inner();
        }
        Phase::Read => {
            let mut cycle = job.created.iter().cycle();
            for i in 0..job.count {
                let claim = *cycle
                    .next()
                    .ok_or(LoadError::Bound("reads without claims"))?;
                let request = job
                    .base
                    .checked_add(READ_OFFSET)
                    .and_then(|id| id.checked_add(u128::from(i)))
                    .ok_or(LoadError::Bound("read request id space"))?;
                let envelope = read_envelope(&job.names, request, claim);
                let started = Instant::now();
                let result = conn.request(envelope).await;
                outcome.samples.push((
                    started.saturating_duration_since(run_start).as_nanos(),
                    started.elapsed().as_nanos(),
                ));
                if read_hit(&result) {
                    outcome.hits = outcome.hits.saturating_add(1);
                }
            }
        }
    }
    Ok(outcome)
}

/// Runs one phase on every job at once and waits for all of them; the wall
/// time is the phase's, from the first caller's start to the last's end.
///
/// The callers run together in this task, on this shard, with the run's
/// connections and generations: a client's requests from several callers
/// are carried on its one connection together. A wake polls each caller
/// still running; more throughput than one shard drives comes from more
/// processes, as the generations already model one participant's.
async fn run_phase(
    jobs: Vec<Job>,
    phase: Phase,
    generations: &Generations,
    connector: &Connector,
    connections: u16,
) -> Result<(Vec<Outcome>, u128, u128), LoadError> {
    let Some(first) = jobs.first() else {
        return Err(LoadError::Bound("zero workers"));
    };
    let conns = (0..connections.max(1))
        .map(|_| Conn::open(connector, &first.names, &first.limits))
        .collect::<Result<Vec<_>, _>>()?;
    let run_start_epoch_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LoadError::Clock)?
        .as_nanos();
    let run_start = Instant::now();
    let mut callers =
        focal_rt::Concurrent::new(jobs.len()).map_err(|_| LoadError::Bound("callers"))?;
    for job in jobs {
        let conn = conns
            .get(
                usize::from(job.index)
                    .checked_rem(conns.len())
                    .ok_or(LoadError::Bound("connections"))?,
            )
            .ok_or(LoadError::Bound("connections"))?;
        let index = job.index;
        callers
            .push(async move {
                (
                    index,
                    worker(job, phase, run_start, generations, conn).await,
                )
            })
            .map_err(|_| LoadError::Bound("callers"))?;
    }
    let mut outcomes = Vec::with_capacity(callers.len());
    while let Some((index, outcome)) = callers.join_next().await {
        outcomes.push((index, outcome?));
    }
    // In the callers' order, whichever ended first.
    outcomes.sort_unstable_by_key(|(index, _)| *index);
    Ok((
        outcomes.into_iter().map(|(_, outcome)| outcome).collect(),
        run_start.elapsed().as_nanos(),
        run_start_epoch_ns,
    ))
}

/// The wall clock in seconds since the epoch, for a credential's validity.
fn unix_now() -> Result<i64, LoadError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LoadError::Clock)?
            .as_secs(),
    )
    .map_err(|_| LoadError::Clock)
}

/// `total` split evenly over `parts`; the first `total % parts` parts get one more.
fn share(total: u64, parts: u16, index: u16) -> Result<u64, LoadError> {
    let parts = u64::from(parts);
    let each = total
        .checked_div(parts)
        .ok_or(LoadError::Bound("zero workers"))?;
    let extra = total
        .checked_rem(parts)
        .ok_or(LoadError::Bound("zero workers"))?;
    Ok(if u64::from(index) < extra {
        each.saturating_add(1)
    } else {
        each
    })
}

fn worker_base(seed: u64, index: u128) -> Result<u128, LoadError> {
    let seed = u128::from(seed)
        .checked_shl(SEED_SHIFT)
        .ok_or(LoadError::Bound("seed space"))?;
    let worker = index
        .checked_shl(WORKER_SHIFT)
        .ok_or(LoadError::Bound("worker space"))?;
    seed.checked_add(worker)
        .ok_or(LoadError::Bound("identity space"))
}

/// Bytes of every regular file under `root`, to a stated number of entries.
fn directory_bytes(root: &Path) -> Result<u64, LoadError> {
    let mut total = 0u64;
    let mut visited = 0usize;
    let mut pending: Vec<(PathBuf, usize)> = vec![(root.to_path_buf(), 0)];
    while let Some((directory, depth)) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            visited = visited.saturating_add(1);
            if visited > MAX_WALK_ENTRIES {
                return Err(LoadError::Bound("data directory walk"));
            }
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                if depth < MAX_WALK_DEPTH {
                    pending.push((entry.path(), depth.saturating_add(1)));
                }
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

fn content_profile(profile: Profile) -> NativeContentProfile {
    match profile {
        Profile::ProjectionOnly => NativeContentProfile::ProjectionOnly,
        Profile::AuthoredV1 => NativeContentProfile::AuthoredV1,
    }
}

/// An embedded node opened on `root` (activated on first open) and hosted on
/// its owner thread; the host handle is what workers clone.
struct Hosted {
    host: LocalHost,
    owner: focal_node::host::HostOwner,
}
impl Hosted {
    fn open(settings: &Settings, limits: &WireLimits) -> Result<Self, LoadError> {
        let node = EmbeddedNode::open(settings)?;
        let (host, owner) = LocalHost::spawn(node, limits.clone())?;
        Ok(Self { host, owner })
    }
    fn close(self) -> Result<(), LoadError> {
        drop(self.host);
        self.owner.join()?;
        Ok(())
    }
}

/// Every worker's samples and counts of one phase, summed.
#[derive(Default)]
struct Merged {
    samples: Vec<(u128, u128)>,
    committed: u64,
    refused: u64,
    unknown: u64,
    hits: u64,
}

fn merged(outcomes: &[Outcome]) -> Merged {
    let mut total = Merged::default();
    for outcome in outcomes {
        total.samples.extend_from_slice(&outcome.samples);
        total.samples.extend(
            outcome
                .writes
                .iter()
                .map(|sample| (sample.start_ns, sample.latency_ns())),
        );
        total.committed = total.committed.saturating_add(outcome.committed);
        total.refused = total.refused.saturating_add(outcome.refused);
        total.unknown = total.unknown.saturating_add(outcome.unknown);
        total.hits = total.hits.saturating_add(outcome.hits);
    }
    total
}

/// Percentiles of all samples, and of the halves by start time.
fn halves(samples: &mut [(u128, u128)]) -> Result<(Latency, Latency, Latency), LoadError> {
    samples.sort_unstable_by_key(|(start, _)| *start);
    let middle = samples
        .len()
        .checked_div(2)
        .ok_or(LoadError::Bound("halves"))?;
    let (first, second) = samples.split_at(middle.min(samples.len()));
    let mut all: Vec<u128> = samples.iter().map(|(_, latency)| *latency).collect();
    let mut first: Vec<u128> = first.iter().map(|(_, latency)| *latency).collect();
    let mut second: Vec<u128> = second.iter().map(|(_, latency)| *latency).collect();
    Ok((
        report::latency(&mut all)?,
        report::latency(&mut first)?,
        report::latency(&mut second)?,
    ))
}

/// The run on one shard of this thread, its root owning the node, the
/// connections and the generations every caller shares.
pub fn run(shape: WorkloadShape) -> Result<Report, LoadError> {
    focal_rt::block_on(run_on_shard(shape)).map_err(LoadError::Runtime)?
}
async fn run_on_shard(shape: WorkloadShape) -> Result<Report, LoadError> {
    let limits = WireLimits::default();
    let profile = content_profile(shape.profile());

    // The node: opened here, or already running behind its socket.
    let mut kept_temp = None;
    let mut settings = Settings::default();
    let (connector, names, root) = match shape.transport {
        Transport::Embedded => {
            let root = match &shape.data_dir {
                Some(directory) => {
                    std::fs::create_dir_all(directory)?;
                    directory.clone()
                }
                None => {
                    let temporary = tempfile::tempdir()?;
                    let path = temporary.path().to_path_buf();
                    kept_temp = Some(temporary);
                    path
                }
            };
            settings.node.data_dir = Some(root.clone());
            focal_node::native_activation::activate_local(&settings, profile)?;
            let hosted = Hosted::open(&settings, &limits)?;
            let identity = decode_identity(&root.join("IDENTITY"))?;
            let names = Names {
                ledger: identity.ledger,
                issuer: identity.issuer,
                worker: identity.worker,
                root: identity.root,
                profile,
            };
            (
                Connector::Embedded(hosted.host.clone()),
                names,
                Some((root, hosted)),
            )
        }
        Transport::Unix => {
            let root = shape
                .data_dir
                .clone()
                .ok_or_else(|| LoadError::Shape("transport unix needs data_dir".into()))?;
            let identity = decode_identity(&root.join("IDENTITY"))?;
            let now = i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| LoadError::Clock)?
                    .as_secs(),
            )
            .map_err(|_| LoadError::Clock)?;
            let actor = focal_node::network_join::local_unix_principal(&root, &identity, now)?;
            let names = Names {
                ledger: identity.ledger,
                issuer: actor,
                worker: identity.worker,
                root: identity.root,
                profile,
            };
            (Connector::Unix(root.join("focal.sock")), names, None)
        }
        Transport::Enrolled => {
            let enrollment = shape
                .enrollment
                .clone()
                .ok_or_else(|| LoadError::Shape("transport enrolled needs enrollment".into()))?;
            let worker = shape
                .worker
                .as_deref()
                .ok_or_else(|| LoadError::Shape("transport enrolled needs worker".into()))
                .and_then(|text| {
                    focal_client::input::parse_id(text)
                        .map_err(|_| LoadError::Shape("worker is 32 hex digits".into()))
                })?;
            let now = i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| LoadError::Clock)?
                    .as_secs(),
            )
            .map_err(|_| LoadError::Clock)?;
            let pending = focal_node::network_join::PendingClientJoin::resume_shared(&enrollment)?;
            let client = pending.remote_client(now, &limits)?;
            let names = Names {
                ledger: client.build.ledger,
                issuer: client.build.actor,
                worker: ParticipantId(worker),
                root: client.build.root,
                profile,
            };
            (Connector::Enrolled(Box::new(pending)), names, None)
        }
    };
    // Paced writes: each worker's share of the offered rate, as a fixed
    // interval between its intended starts.
    let pace = match shape.rate {
        Some(rate) => {
            let per_worker = rate
                .checked_div(u64::from(shape.concurrency))
                .filter(|share| *share > 0)
                .ok_or(LoadError::Bound("rate below one a second per worker"))?;
            Some(
                Duration::from_secs(1)
                    .checked_div(u32::try_from(per_worker).map_err(|_| LoadError::Bound("rate"))?)
                    .ok_or(LoadError::Bound("rate"))?,
            )
        }
        None => None,
    };

    let connections = shape.connections.unwrap_or(shape.concurrency);
    // Writes: every worker its share of the claims, in its own id space.
    let mut jobs = Vec::with_capacity(usize::from(shape.concurrency));
    for index in 0..shape.concurrency {
        jobs.push(Job {
            index,
            inflight: shape.inflight,
            callers: shape.concurrency,
            count: share(shape.claims, shape.concurrency, index)?,
            base: worker_base(shape.seed, u128::from(index))?,
            names,
            limits: limits.clone(),
            created: Vec::new(),
            lifecycle: shape.lifecycle,
            lifecycle_reads: shape.lifecycle_reads,
            pace,
        });
    }
    // One journal's generations for every caller, as N processes of one
    // participant share one (the audit's F12).
    let generations = Generations::new();
    let (write_outcomes, write_nanos, write_phase_start_epoch_ns) =
        run_phase(jobs, Phase::Write, &generations, &connector, connections).await?;
    let (floors_advanced, expired) = generations.counts()?;
    let mut writes = merged(&write_outcomes);
    let (committed, refused, unknown) = (writes.committed, writes.refused, writes.unknown);
    let lifecycles = write_outcomes.iter().fold(0u64, |total, outcome| {
        total.saturating_add(outcome.lifecycles)
    });
    let abandoned = write_outcomes.iter().fold(0u64, |total, outcome| {
        total.saturating_add(outcome.abandoned)
    });
    // Requests whose intended start falls in the warm-up are sent, never measured.
    let warmup = u128::from(shape.warmup_ms).saturating_mul(1_000_000);
    writes.samples.retain(|(start, _)| *start >= warmup);
    let (latency_ns, first_half, second_half) = halves(&mut writes.samples)?;
    let measured_writes = WriteMeasurements::measure(
        write_outcomes
            .iter()
            .flat_map(|outcome| outcome.writes.iter()),
        warmup,
    )?;
    measurements::write_samples(
        write_outcomes
            .iter()
            .flat_map(|outcome| outcome.writes.iter()),
        warmup,
        write_phase_start_epoch_ns,
    )?;
    let mut refusals = Vec::new();
    for outcome in &write_outcomes {
        for (reason, count) in &outcome.refusals {
            note_count(&mut refusals, reason.clone(), *count);
        }
    }

    // Reads: the workers that created claims cycle through their own.
    let readers: Vec<&Outcome> = write_outcomes
        .iter()
        .filter(|outcome| !outcome.created.is_empty())
        .collect();
    let reader_count = u16::try_from(readers.len()).map_err(|_| LoadError::Bound("readers"))?;
    let (reads, read_hits, read_nanos, read_latency_ns) = if shape.reads > 0 && reader_count > 0 {
        let mut jobs = Vec::with_capacity(readers.len());
        for (slot, outcome) in readers.iter().enumerate() {
            let index = u16::try_from(slot).map_err(|_| LoadError::Bound("readers"))?;
            jobs.push(Job {
                index,
                inflight: 1,
                callers: reader_count,
                count: share(shape.reads, reader_count, index)?,
                base: worker_base(shape.seed, u128::from(index))?,
                names,
                limits: limits.clone(),
                created: outcome.created.clone(),
                lifecycle: Lifecycle::Create,
                lifecycle_reads: 1,
                // Reads are not paced: they measure the read path closed-loop.
                pace: None,
            });
        }
        let (read_outcomes, read_nanos, _) = run_phase(
            jobs,
            Phase::Read,
            &generations,
            &connector,
            reader_count.min(connections),
        )
        .await?;
        let reads = merged(&read_outcomes);
        let issued = u64::try_from(reads.samples.len()).map_err(|_| LoadError::Bound("reads"))?;
        let mut latencies: Vec<u128> = reads.samples.iter().map(|(_, latency)| *latency).collect();
        (
            issued,
            reads.hits,
            read_nanos,
            report::latency(&mut latencies)?,
        )
    } else {
        (0, 0, 0, Latency::default())
    };
    drop(connector);

    // The embedded node: its size on disk, and its recovery when asked.
    let mut data_dir_bytes = None;
    let (mut reopen_ms, mut first_read_after_reopen_ns, mut reopen_read_hit) = (None, None, None);
    if let Some((root, hosted)) = root {
        hosted.close()?;
        data_dir_bytes = Some(directory_bytes(&root)?);
        if shape.reopen {
            let started = Instant::now();
            let hosted = Hosted::open(&settings, &limits)?;
            reopen_ms = Some(started.elapsed().as_millis());
            if let Some(claim) = write_outcomes
                .iter()
                .rev()
                .find_map(|outcome| outcome.created.last().copied())
            {
                let probe = Connector::Embedded(hosted.host.clone());
                let conn = Conn::open(&probe, &names, &limits)?;
                let request = worker_base(shape.seed, PROBE_WORKER)?
                    .checked_add(READ_OFFSET)
                    .ok_or(LoadError::Bound("probe id space"))?;
                let envelope = read_envelope(&names, request, claim);
                let started = Instant::now();
                let result = conn.request(envelope).await;
                first_read_after_reopen_ns = Some(started.elapsed().as_nanos());
                reopen_read_hit = Some(read_hit(&result));
                drop(conn);
                drop(probe);
            }
            hosted.close()?;
        }
    }
    drop(kept_temp);

    Ok(Report {
        committed,
        refused,
        unknown,
        expired,
        lifecycles,
        abandoned,
        floors_advanced,
        wall_ms: write_nanos.checked_div(1_000_000).unwrap_or(0),
        throughput_ops_per_s: report::per_second(committed, write_nanos),
        latency_ns,
        latency_ns_first_half: first_half,
        latency_ns_second_half: second_half,
        measured_writes,
        write_phase_start_epoch_ns,
        reads,
        read_hits,
        read_wall_ms: read_nanos.checked_div(1_000_000).unwrap_or(0),
        read_throughput_ops_per_s: report::per_second(read_hits, read_nanos),
        read_latency_ns,
        workers: shape.concurrency,
        refusals,
        reopen_ms,
        first_read_after_reopen_ns,
        reopen_read_hit,
        data_dir_bytes,
        shape,
    })
}
