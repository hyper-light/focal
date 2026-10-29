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
    self, Deliveries, Delivery, DeliveryMeasurements, LifecycleCounts, RefusalCause,
    WriteMeasurements, WriteOutcome, WriteSample, WriteSamples,
};
use crate::native;
use crate::progress::{PROGRESS, Stage};
use crate::report::{self, Latency, LedgerReport, Report};
use crate::seats::{self, Seat, share};
use crate::shape::{Home, Lifecycle, Profile, Transport, WorkloadShape};
use crate::watch::{self, Side, TailCounts, Watch};
use focal_client::native_store::{NativeIdentity, NativeIdentityKind};
use focal_client::operations::NativeAuthoredOperation;
use focal_client::{
    Client, ClientError, EmbeddedTransport, LocalTransport, QuicTransport, ReadRoute, RetryPolicy,
};
use focal_ledger::NativeContentProfile;
use focal_model::{
    ClaimId, LedgerId, ParticipantId, RequestEpoch, RequestId, RequestKey, RootCommandId,
    RouteEpoch, SessionId, SessionSeq,
};
use focal_node::{config::Settings, embedded::EmbeddedNode, host::LocalHost};
use focal_wire::{
    AccessError, AuthenticatedPeer, NativeClaimExpand, NativeErrorCode, NativeMutationReply,
    NativeObject, NativeReadQuery, NativeReadRequest, NativeRefusalKind, Operation, PeerGrant,
    PeerRole, QuicConnector, ReadConsistency, ReadToken, RequestEnvelope, Response,
    ResponseEnvelope, RouteHint, WireLimits,
};
use hyper_rt::combine::{Either, race2};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    rc::Rc,
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
/// The worker indexes a `deliver` run's event tails number their pages under; above every real
/// worker, below the probe.
const CONSUMER_TAIL_WORKER: u128 = 0xFE;
const ISSUER_TAIL_WORKER: u128 = 0xFD;
/// The worker index every agent's tail numbers its pages under in a run among agents: the pages
/// are reads, each named by its agent's principal beside its id, so the agents share the space.
const AGENT_TAIL_WORKER: u128 = 0xFC;
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

pub(crate) enum Conn {
    Embedded(Client<EmbeddedTransport<LocalHost>>),
    Unix(Client<LocalTransport>),
    Enrolled(Client<QuicTransport>),
}
impl Conn {
    /// A connection through `connector`; on the embedded transport, as `principal` (an
    /// enrolled client is the principal its credential names).
    fn open(
        connector: &Connector,
        principal: ParticipantId,
        names: &Names,
        limits: &WireLimits,
    ) -> Result<Self, LoadError> {
        Self::open_homed(connector, principal, names, limits, &[])
    }
    /// [`Conn::open`], an enrolled client homed on the first of `homes` where any are given: it
    /// dials its home first, reads there and writes at the ledger's leader ([`ReadRoute::Home`]),
    /// and sends a request to the next of `homes` in turn once the one it was sent on gave no
    /// answer (`QuicTransport::with_routes`), as many as the transport holds connections.
    fn open_homed(
        connector: &Connector,
        principal: ParticipantId,
        names: &Names,
        limits: &WireLimits,
        homes: &[&Home],
    ) -> Result<Self, LoadError> {
        match connector {
            Connector::Embedded(host) => {
                let peer = AuthenticatedPeer::local(PeerGrant {
                    principal,
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
                let mut routes = Vec::new();
                routes
                    .try_reserve_exact(homes.len().max(1))
                    .map_err(|_| LoadError::Bound("homes"))?;
                for home in homes {
                    routes.push(RouteHint {
                        epoch: client.initial.epoch,
                        endpoint: home.endpoint.clone(),
                        server_name: home.server_name.clone(),
                    });
                }
                let reads = if routes.is_empty() {
                    routes.push(client.initial);
                    ReadRoute::Leader
                } else {
                    ReadRoute::Home
                };
                let first = routes.first().ok_or(LoadError::Bound("homes"))?;
                let address = if first.endpoint.starts_with('[') {
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
                let transport = QuicTransport::with_routes(connector, routes, 16)?;
                Ok(Self::Enrolled(
                    Client::new(transport, RetryPolicy::default(), limits.clone(), 1)?
                        .with_read_route(reads),
                ))
            }
        }
    }
    pub(crate) async fn request(
        &self,
        envelope: RequestEnvelope,
    ) -> Result<ResponseEnvelope, ClientError> {
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
    /// Where the caller sits among the phase's: the ledger it drives, and its seat among that
    /// ledger's callers ([`Seat`]).
    seat: Seat,
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
    /// Write phase: the aggregate offered rate. Every caller takes its interleaved places on
    /// that one schedule, including a rate's remainder across callers.
    pace: Option<u64>,
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
    /// The ledger the worker drove.
    ledger: u16,
    samples: Vec<(u128, u128)>,
    writes: WriteSamples,
    /// What its lifecycles did, and its writes committed: a refused write, or one whose outcome
    /// is unknown, ends its lifecycle, so each counts one write and one lifecycle.
    counts: LifecycleCounts,
    hits: u64,
    created: Vec<u128>,
    refusals: Vec<(String, u64)>,
    /// `deliver`: the messages delivered end to end.
    deliveries: Deliveries,
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

/// The point query asks for one exact claim. Missing/retired markers and unrelated objects
/// must not inflate successful-read throughput, including the probe after reopening.
fn read_hit(result: &Result<ResponseEnvelope, ClientError>, claim: u128) -> bool {
    let Ok(envelope) = result else {
        return false;
    };
    let Response::NativeRead(page) = &envelope.result else {
        return false;
    };
    matches!(page.objects.as_slice(), [NativeObject::Claim(object)] if object.binding.object.0 == claim.to_be_bytes())
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

fn window_envelope(names: &Names, principal: ParticipantId, request: u128) -> RequestEnvelope {
    RequestEnvelope {
        protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
        ledger: names.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: NativeReadQuery::Epochs(principal),
            max_items: 1,
        }),
    }
}

/// The write `offset` of this worker: a structural creation names its claim
/// id; a compiled one mints its identities from a span of the same space.
/// Objects one evaluation selection's page holds at most: the evaluation the owner selects and
/// what it binds (its definition and results), as the native client reads it (`READ_ITEMS`).
const LIFECYCLE_SELECTION_ITEMS: u32 = focal_native_client::READ_ITEMS;

/// The creation of lifecycle `offset`'s claim, by `request`'s principal for `subject`, judged by
/// `evaluator` where it names one (a work claim).
fn build(
    job: &Job,
    offset: u128,
    request: RequestKey,
    subject: ParticipantId,
    evaluator: Option<ParticipantId>,
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
                request.principal,
                subject,
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
                request.principal,
                subject,
                evaluator,
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

/// One principal's side of a worker: who it is, the client it sends on, the journal
/// generations every worker of it shares (one participant's, as N processes of one participant
/// share one), and its protocol request ids. The issuer creates every claim; a `deliver` run's
/// consumer completes them.
struct Actor<'a> {
    principal: ParticipantId,
    conn: &'a Conn,
    generations: &'a Generations,
    protocol: std::cell::Cell<u128>,
}

/// What one worker's writes under way share on its thread: its job, its
/// principals' clients and generations, its protocol request ids and its
/// outcome. Nothing is held across a wait.
struct Shared<'a> {
    job: &'a Job,
    /// The run's issuer, in a run with no agents: it creates every claim.
    issuer: Option<Actor<'a>>,
    /// A `deliver` run's consumer, and what both sides watch for.
    consumer: Option<(Actor<'a>, &'a Watch)>,
    /// A run among agents: lifecycle `n`'s issuer is the agent at `n` and its consumer the
    /// next around the ring ([`Shared::sides`]); `issuer` and `consumer` then act for none.
    agents: Option<(Vec<Actor<'a>>, &'a Watch)>,
    /// The worker's lifecycle reads under way, at most `job.lifecycle_reads`.
    reads: hyper_rt::sync::Semaphore,
    run_start: Instant,
    outcome: std::cell::RefCell<Outcome>,
}
/// A lifecycle's issuer, and its consumer with what both watch for ([`Shared::sides`]).
type Sides<'s> = (&'s Actor<'s>, Option<(&'s Actor<'s>, &'s Watch)>);

impl Shared<'_> {
    /// Lifecycle `i`'s issuer, and its consumer with what both watch: in a run among agents,
    /// those of its place among its ledger's lifecycles, the ledger's callers' places interleaved
    /// (`n = i · seats + seat`, [`Seat::local`]), so every caller's lifecycles go round every
    /// agent of its ledger.
    fn sides(&self, i: u64) -> Result<Sides<'_>, LoadError> {
        let Some((agents, watch)) = &self.agents else {
            let issuer = self.issuer.as_ref().ok_or(LoadError::Shape(
                "a run without agents writes as its issuer".into(),
            ))?;
            return Ok((
                issuer,
                self.consumer.as_ref().map(|(actor, watch)| (actor, *watch)),
            ));
        };
        let count = u64::try_from(agents.len()).map_err(|_| LoadError::Bound("agents"))?;
        let place = self.job.seat.local(i)?;
        let at = |n: u64| -> Result<&Actor<'_>, LoadError> {
            let index = n
                .checked_rem(count)
                .and_then(|index| usize::try_from(index).ok())
                .ok_or(LoadError::Bound("agents"))?;
            agents.get(index).ok_or(LoadError::Bound("agents"))
        };
        Ok((
            at(place)?,
            Some((
                at(place.checked_add(1).ok_or(LoadError::Bound("agents"))?)?,
                *watch,
            )),
        ))
    }
    fn protocol_request(&self, actor: &Actor<'_>) -> Result<u128, LoadError> {
        let mut protocol = Protocol {
            next: actor.protocol.get(),
        };
        let id = protocol.request(self.job)?;
        actor.protocol.set(protocol.next);
        Ok(id)
    }
    fn note(&self, reason: String) {
        note(&mut self.outcome.borrow_mut().refusals, reason);
    }
    /// The worker's counts, as `change` makes them.
    fn count(&self, change: impl FnOnce(&mut LifecycleCounts)) {
        change(&mut self.outcome.borrow_mut().counts);
    }
    /// A lifecycle ended whole: every write committed.
    fn written(&self) {
        PROGRESS.lifecycle();
        self.count(|counts| counts.written = counts.written.saturating_add(1));
    }
    /// A lifecycle ended at a step never sent: the read of what it binds to failed.
    fn abandoned(&self) {
        self.count(|counts| counts.abandoned = counts.abandoned.saturating_add(1));
    }
    /// A lifecycle's claim settled satisfied, at `satisfied`: delivered, its handing in at
    /// `handled`, both measured from its place on the schedule at `started`.
    fn judged(
        &self,
        started: Instant,
        handled: Instant,
        satisfied: Instant,
    ) -> Result<(), LoadError> {
        let mut outcome = self.outcome.borrow_mut();
        outcome.counts.judged = outcome.counts.judged.saturating_add(1);
        outcome.deliveries.push(Delivery {
            start_ns: started.saturating_duration_since(self.run_start).as_nanos(),
            handled_ns: handled.saturating_duration_since(self.run_start).as_nanos(),
            satisfied_ns: satisfied
                .saturating_duration_since(self.run_start)
                .as_nanos(),
        })
    }
    /// The owner's window for `actor`, read after a refusal by name: the run
    /// issues where the owner admits. `false` when the window could not be read.
    async fn learn(&self, actor: &Actor<'_>) -> Result<bool, LoadError> {
        let request = self.protocol_request(actor)?;
        match actor
            .conn
            .request(window_envelope(&self.job.names, actor.principal, request))
            .await
        {
            Ok(envelope) => {
                if let Response::NativeRead(page) = &envelope.result
                    && let Some(NativeObject::Epochs(window)) = page.objects.first()
                {
                    let open: Vec<RequestEpoch> =
                        window.open.iter().map(|open| open.epoch).collect();
                    actor.generations.learn(window.floor, &open)?;
                    return Ok(true);
                }
                self.note(format!("window: unexpected reply {:?}", envelope.result));
            }
            Err(error) => self.note(format!("window: client: {error}")),
        }
        Ok(false)
    }
    /// The floor advance `actor`'s journal sends once the generation below
    /// drained: committed, the floor stands at `minimum`; refused by name,
    /// the window is learned; otherwise the next reply sends it again.
    async fn advance(
        &self,
        actor: &Actor<'_>,
        epoch: RequestEpoch,
        minimum: RequestEpoch,
    ) -> Result<(), LoadError> {
        let request = RequestKey {
            principal: actor.principal,
            epoch,
            id: RequestId::from_u128(self.protocol_request(actor)?),
        };
        let names = &self.job.names;
        let envelope = native::advance_envelope(names.ledger, names.profile, request, minimum)?;
        match classify(actor.conn.request(envelope).await) {
            Reply::Committed(_) => actor.generations.advanced(minimum),
            Reply::Expired => {
                actor.generations.advance_failed()?;
                self.learn(actor).await.map(|_| ())
            }
            Reply::Refused(reason, _) | Reply::Unknown(reason) => {
                actor.generations.advance_failed()?;
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
        let (issuer, _) = self.sides(i)?;
        PROGRESS.started();
        self.count(|counts| counts.started = counts.started.saturating_add(1));
        let creating = PROGRESS.at(Stage::Create);
        let Some((claim, _, _)) = self.step(issuer, i, 0, started, None).await? else {
            return Ok(());
        };
        drop(creating);
        match self.job.lifecycle {
            // The creation is the lifecycle's one write.
            Lifecycle::Create => {
                self.written();
                Ok(())
            }
            Lifecycle::CancelRelease => self.cancel_release(i, claim).await,
            Lifecycle::Deliver => self.deliver(i, claim, started).await,
            Lifecycle::Work => self.work(i, claim, started).await,
        }
    }
    /// The issuer cancels the claim it created, then releases its scope.
    async fn cancel_release(&self, i: u64, claim: u128) -> Result<(), LoadError> {
        let later = [
            NativeAuthoredOperation::ClaimCancel(authored::claim_target(claim)),
            NativeAuthoredOperation::ClaimReleaseScope(authored::claim_target(claim)),
        ];
        let (issuer, _) = self.sides(i)?;
        for (step, operation) in (1u8..).zip(later.iter()) {
            if self
                .later(issuer, i, step, operation, 0..0)
                .await?
                .is_none()
            {
                return Ok(());
            }
        }
        self.written();
        Ok(())
    }
    /// The claim the issuer created, delivered: posted by the issuer, found by the
    /// consumer in the ledger's events, acquired, answered with a testament and the testament
    /// posted; the testament found by the issuer in the events, received, its whole-work
    /// validation entered, which satisfies the claim, and the claim's scope released, which ends
    /// its lifecycle so its family may retire. Each side waits on the events it polls
    /// for the other's step, registered before the write that makes it is sent. Recorded: when
    /// the consumer's testament was posted (the message handled) and when the claim was
    /// satisfied, each from the message's place on the schedule.
    async fn deliver(&self, i: u64, claim: u128, started: Instant) -> Result<(), LoadError> {
        let (issuer, consumer) = self.sides(i)?;
        let Some((consumer, watch)) = consumer else {
            return Err(LoadError::Shape("deliver without a consumer".into()));
        };
        // The receipt and the testament mint from the upper half of the claim's own creation
        // span, which its creation leaves unused.
        let span = self.creation_span(i)?;
        let half = authored::IDS_PER_CREATION / 2;
        let quarter = authored::IDS_PER_CREATION / 4;
        let receipt_ids = span
            .start
            .checked_add(half)
            .zip(span.start.checked_add(half + quarter))
            .map(|(start, end)| start..end)
            .ok_or(LoadError::Bound("receipt id space"))?;
        let testament_ids = span
            .start
            .checked_add(half + quarter)
            .map(|start| start..span.end)
            .ok_or(LoadError::Bound("testament id space"))?;
        let posted = watch.expect_posted(claim)?;
        let post = NativeAuthoredOperation::ClaimPost(authored::claim_target(claim));
        let at = PROGRESS.at(Stage::Post);
        if self.later(issuer, i, 1, &post, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let at = PROGRESS.at(Stage::WaitPosted);
        let Some(()) = watch.wait(posted).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        let acquire = NativeAuthoredOperation::ReceiptAcquire(authored::receipt(claim));
        let at = PROGRESS.at(Stage::Acquire);
        if self
            .later(consumer, i, 2, &acquire, receipt_ids)
            .await?
            .is_none()
        {
            return Ok(());
        }
        drop(at);
        let submit = NativeAuthoredOperation::TestamentSubmit(authored::testament(claim));
        let at = PROGRESS.at(Stage::Submit);
        let Some((_, minted)) = self.later(consumer, i, 3, &submit, testament_ids).await? else {
            return Ok(());
        };
        drop(at);
        let testament = minted
            .iter()
            .find(|identity| identity.kind == NativeIdentityKind::Response)
            .map(|identity| u128::from_be_bytes(identity.id))
            .ok_or_else(|| LoadError::Fixture("the testament minted no response".into()))?;
        let answered = watch.expect_testament(claim)?;
        let post =
            NativeAuthoredOperation::TestamentPost(authored::testament_target(claim, testament));
        let at = PROGRESS.at(Stage::PostTestament);
        if self.later(consumer, i, 4, &post, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let handled = Instant::now();
        let at = PROGRESS.at(Stage::WaitAnswered);
        let Some(posted) = watch.wait(answered).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        if posted != testament {
            self.note(format!(
                "the testament the issuer saw posted for {claim:032x} is not the consumer's"
            ));
        }
        let target = authored::testament_target(claim, testament);
        let receive = NativeAuthoredOperation::TestamentReceive(target.clone());
        let at = PROGRESS.at(Stage::Receive);
        if self.later(issuer, i, 5, &receive, 0..0).await?.is_none() {
            return Ok(());
        }
        drop(at);
        let enter = NativeAuthoredOperation::ValidationEnterWholeWork(target);
        let at = PROGRESS.at(Stage::Enter);
        if self.later(issuer, i, 6, &enter, 0..0).await?.is_none() {
            return Ok(());
        }
        drop(at);
        // Delivered once satisfied, whether or not the release that ends its lifecycle commits:
        // a message delivered and a lifecycle completed are counted apart.
        self.judged(started, handled, Instant::now())?;
        let release = NativeAuthoredOperation::ClaimReleaseScope(authored::claim_target(claim));
        let at = PROGRESS.at(Stage::Release);
        if self.later(issuer, i, 7, &release, 0..0).await?.is_none() {
            return Ok(());
        }
        drop(at);
        self.written();
        Ok(())
    }

    /// The work claim the issuer created, done and judged among three agents (02 §3,
    /// §6): posted by the issuer; found by the worker in its events, its receipt acquired, a work
    /// artifact submitted for the claim's slot, read back for the reference a manifest binds,
    /// and a testament citing it submitted and posted; the testament found by the issuer in its
    /// events, received, and its whole-work validation entered; the evaluation found due by the
    /// evaluator in its events, begun and reported a pass with its result artifact; the claim
    /// found settled by the issuer in its events, and its scope released. Every hand-off is
    /// learned from the recipient's own events, each waiter registered before the write that
    /// answers it. Recorded: when the testament was posted (the work handed in) and when the
    /// claim was seen satisfied, from the claim's place on the schedule.
    async fn work(&self, i: u64, claim: u128, started: Instant) -> Result<(), LoadError> {
        let (issuer, worker) = self.sides(i)?;
        let Some((worker, watch)) = worker else {
            return Err(LoadError::Shape("work without a worker".into()));
        };
        let evaluator = self.evaluator(i)?;
        // The identities each step mints, from the upper half of the claim's own creation span,
        // which its creation leaves unused: an eighth each for the receipt, the work artifact,
        // the testament and the report's result artifact.
        let span = self.creation_span(i)?;
        let eighth = authored::IDS_PER_CREATION / 8;
        let part = |n: u128| -> Result<std::ops::Range<u128>, LoadError> {
            let start = n
                .checked_add(4)
                .and_then(|eighths| eighth.checked_mul(eighths))
                .and_then(|offset| span.start.checked_add(offset))
                .ok_or(LoadError::Bound("work id space"))?;
            let end = start
                .checked_add(eighth)
                .filter(|end| *end <= span.end)
                .ok_or(LoadError::Bound("work id space"))?;
            Ok(start..end)
        };
        let (receipt_ids, artifact_ids, testament_ids, report_ids) =
            (part(0)?, part(1)?, part(2)?, part(3)?);
        // The issuer posts; the worker finds the claim in its events.
        let posted = watch.expect_posted(claim)?;
        let post = NativeAuthoredOperation::ClaimPost(authored::claim_target(claim));
        let at = PROGRESS.at(Stage::Post);
        if self.later(issuer, i, 1, &post, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let at = PROGRESS.at(Stage::WaitPosted);
        let Some(()) = watch.wait(posted).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        // The worker takes the claim and does its work.
        let acquire = NativeAuthoredOperation::ReceiptAcquire(authored::receipt(claim));
        let at = PROGRESS.at(Stage::Acquire);
        if self
            .later(worker, i, 2, &acquire, receipt_ids)
            .await?
            .is_none()
        {
            return Ok(());
        }
        drop(at);
        let artifact = NativeAuthoredOperation::ArtifactSubmit(authored::work_artifact(claim));
        let at = PROGRESS.at(Stage::Artifact);
        let Some((written, minted)) = self.later(worker, i, 3, &artifact, artifact_ids).await?
        else {
            return Ok(());
        };
        drop(at);
        let artifact = minted
            .iter()
            .find(|identity| identity.kind == NativeIdentityKind::Artifact)
            .map(|identity| u128::from_be_bytes(identity.id))
            .ok_or_else(|| LoadError::Fixture("the work artifact minted no artifact".into()))?;
        let Some(reference) = self.work_reference(worker, artifact, written).await? else {
            self.abandoned();
            return Ok(());
        };
        let submit =
            NativeAuthoredOperation::TestamentSubmit(authored::work_testament(claim, reference));
        let at = PROGRESS.at(Stage::Submit);
        let Some((_, minted)) = self.later(worker, i, 4, &submit, testament_ids).await? else {
            return Ok(());
        };
        drop(at);
        let testament = minted
            .iter()
            .find(|identity| identity.kind == NativeIdentityKind::Response)
            .map(|identity| u128::from_be_bytes(identity.id))
            .ok_or_else(|| LoadError::Fixture("the testament minted no response".into()))?;
        // The worker posts its testament; the issuer finds it in its events. Every later waiter is
        // registered here too, before the post: what materializes the evaluation, or settles the
        // claim, may follow any write after it, and an event that comes before its waiter is
        // passed over.
        let answered = watch.expect_testament(claim)?;
        let due = watch.expect_evaluation(claim)?;
        let settled = watch.expect_settled(claim)?;
        let post =
            NativeAuthoredOperation::TestamentPost(authored::testament_target(claim, testament));
        let at = PROGRESS.at(Stage::PostTestament);
        if self.later(worker, i, 5, &post, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let handled = Instant::now();
        let at = PROGRESS.at(Stage::WaitAnswered);
        let Some(posted) = watch.wait(answered).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        if posted != testament {
            self.note(format!(
                "the testament the issuer saw posted for {claim:032x} is not the worker's"
            ));
        }
        // The issuer receives the testament and enters its whole-work validation; the evaluator
        // finds its evaluation due, and the issuer will find the claim settled.
        let target = authored::testament_target(claim, testament);
        let receive = NativeAuthoredOperation::TestamentReceive(target.clone());
        let at = PROGRESS.at(Stage::Receive);
        if self.later(issuer, i, 6, &receive, 0..0).await?.is_none() {
            return Ok(());
        }
        drop(at);
        let enter = NativeAuthoredOperation::ValidationEnterWholeWork(target);
        let at = PROGRESS.at(Stage::Enter);
        if self.later(issuer, i, 7, &enter, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let at = PROGRESS.at(Stage::WaitEvaluation);
        let Some(validation) = watch.wait(due).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        // The evaluator judges the work.
        let begin = NativeAuthoredOperation::ValidationBegin(authored::begin(claim, validation));
        let at = PROGRESS.at(Stage::Begin);
        if self.later(evaluator, i, 8, &begin, 0..0).await?.is_none() {
            watch.forget(claim);
            return Ok(());
        }
        drop(at);
        let report = NativeAuthoredOperation::ValidationReport(authored::report(claim, validation));
        let at = PROGRESS.at(Stage::Report);
        let Some((_, _)) = self.later(evaluator, i, 9, &report, report_ids).await? else {
            watch.forget(claim);
            return Ok(());
        };
        drop(at);
        // The issuer finds the claim settled by the evaluator's verdict.
        let at = PROGRESS.at(Stage::WaitSettled);
        let Some(status) = watch.wait(settled).await? else {
            return self.undelivered(claim, watch);
        };
        drop(at);
        if status != focal_model::ClaimStatus::Satisfied {
            self.note(format!(
                "the work claim {claim:032x} settled {status:?}, not satisfied"
            ));
            return self.undelivered(claim, watch);
        }
        self.judged(started, handled, Instant::now())?;
        let release = NativeAuthoredOperation::ClaimReleaseScope(authored::claim_target(claim));
        let at = PROGRESS.at(Stage::Release);
        if self.later(issuer, i, 10, &release, 0..0).await?.is_none() {
            return Ok(());
        }
        drop(at);
        self.written();
        Ok(())
    }

    /// A step the other side never showed in the events within the wait's bound: counted, its
    /// waiter let go.
    fn undelivered(&self, claim: u128, watch: &Watch) -> Result<(), LoadError> {
        watch.forget(claim);
        PROGRESS.undelivered();
        self.count(|counts| counts.undelivered = counts.undelivered.saturating_add(1));
        Ok(())
    }
    /// The identities lifecycle `i`'s creation mints from.
    fn creation_span(&self, i: u64) -> Result<std::ops::Range<u128>, LoadError> {
        let start = u128::from(i)
            .checked_mul(authored::IDS_PER_CREATION)
            .and_then(|span| self.job.base.checked_add(CLAIM_OFFSET)?.checked_add(span))
            .ok_or(LoadError::Bound("claim id space"))?;
        let end = start
            .checked_add(authored::IDS_PER_CREATION)
            .ok_or(LoadError::Bound("claim id space"))?;
        Ok(start..end)
    }
    /// A later step of lifecycle `i`: what `operation` binds to read by `actor`, compiled against
    /// it and sent, minting from `ids`. The sequence it committed at and the identities it
    /// minted; `None` when the read failed (the step abandoned, the reason noted) or the step did
    /// not commit.
    async fn later(
        &self,
        actor: &Actor<'_>,
        i: u64,
        step: u8,
        operation: &NativeAuthoredOperation,
        ids: std::ops::Range<u128>,
    ) -> Result<Option<(SessionSeq, Vec<NativeIdentity>)>, LoadError> {
        let Some(objects) = self.read_for(actor, operation).await? else {
            self.abandoned();
            return Ok(None);
        };
        Ok(self
            .step(
                actor,
                i,
                step,
                Instant::now(),
                Some((operation, &objects, ids)),
            )
            .await?
            .map(|(_, sequence, minted)| (sequence, minted)))
    }
    /// The objects `operation` binds to (`requirements`), as `actor` reads them linearizable: the
    /// objects it names in one page, and each evaluation it binds to selected by the owner in a
    /// page of its own, as the native client resolves them (its driver's `resolve`); `None`, with
    /// the reason noted, when they could not be read. Linearizable, as `resolve` reads, since what
    /// a step binds another participant may have changed since the actor's last write, and a
    /// copy that has applied only that write would serve the revision it replaced: at least at
    /// the actor's own write, the evaluator's begin read the claim at a follower behind the
    /// leader and was refused its stale revision.
    async fn read_for(
        &self,
        actor: &Actor<'_>,
        operation: &NativeAuthoredOperation,
    ) -> Result<Option<Vec<NativeObject>>, LoadError> {
        let mut references = Vec::new();
        let mut queries = Vec::new();
        for requirement in focal_native_client::requirements(operation)
            .map_err(|error| LoadError::Fixture(format!("requirements: {error}")))?
        {
            match requirement {
                focal_native_client::Requirement::Objects(objects) => {
                    references.extend(objects);
                }
                focal_native_client::Requirement::Evaluation {
                    claim,
                    validation,
                    selector,
                } => queries.push((
                    NativeReadQuery::SelectEvaluation(focal_wire::NativeSelectionQuery {
                        claim,
                        validation,
                        selector,
                        generation: None,
                        live: true,
                    }),
                    LIFECYCLE_SELECTION_ITEMS,
                )),
            }
        }
        if !references.is_empty() {
            let wanted =
                u32::try_from(references.len()).map_err(|_| LoadError::Bound("lifecycle read"))?;
            queries.push((NativeReadQuery::Objects(references), wanted));
        }
        let mut objects = Vec::new();
        for (query, wanted) in queries {
            let Some(page) = self
                .read_page(actor, query, wanted, ReadConsistency::Linearizable)
                .await?
            else {
                return Ok(None);
            };
            objects.extend(page);
        }
        Ok(Some(objects))
    }
    /// One page of `query`, as `actor` reads it at `consistency`, at most `wanted` objects;
    /// `None`, with the reason noted, when it could not be read or held nothing. A read its
    /// client could not have answered, no node reached or none serving its ledger, is asked again
    /// until its ledger answers it or a hand-off's wait has passed since it was first asked, as an
    /// agent asks again: a linearizable read needs its session's leader, and one asked of a
    /// session that lost its quorum failed at its client's 30 s clock however soon the voters
    /// returned, its lifecycle abandoned (twelve of 33 nodes killed for 30 s, 2026-10-29). Each
    /// pause between is the client's own longest pause between resends.
    async fn read_page(
        &self,
        actor: &Actor<'_>,
        query: NativeReadQuery,
        wanted: u32,
        consistency: ReadConsistency,
    ) -> Result<Option<Vec<NativeObject>>, LoadError> {
        let Ok(_permit) = self.reads.acquire().await else {
            return Err(LoadError::Bound("lifecycle reads"));
        };
        let asked = Instant::now();
        loop {
            let request = self.protocol_request(actor)?;
            let envelope = RequestEnvelope {
                protocol: focal_wire::NATIVE_PROTOCOL_VERSION,
                ledger: self.job.names.ledger,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId::from_u128(request),
                operation: Operation::NativeRead(NativeReadRequest {
                    consistency: consistency.clone(),
                    query: query.clone(),
                    max_items: wanted,
                }),
            };
            return match actor.conn.request(envelope).await {
                Ok(envelope) => match envelope.result {
                    Response::NativeRead(page) if !page.objects.is_empty() => {
                        Ok(Some(page.objects))
                    }
                    other => {
                        self.note(format!("lifecycle read: unexpected reply {other:?}"));
                        Ok(None)
                    }
                },
                Err(ClientError::Transport | ClientError::Access(AccessError::Unavailable))
                    if asked.elapsed() < watch::WAIT =>
                {
                    focal_rt::time::sleep(RetryPolicy::default().max_backoff)
                        .await
                        .map_err(LoadError::Runtime)?;
                    continue;
                }
                Err(error) => {
                    self.note(format!("lifecycle read: client: {error}"));
                    Ok(None)
                }
            };
        }
    }
    /// The work artifact `artifact` as `actor` reads it at least at `written`, the write that
    /// submitted it (an artifact is never changed): its reference (its identity and content
    /// hash), which a testament's manifest binds; `None`, noted, when it could not be read.
    async fn work_reference(
        &self,
        actor: &Actor<'_>,
        artifact: u128,
        written: SessionSeq,
    ) -> Result<Option<focal_model::ArtifactRef>, LoadError> {
        let query = NativeReadQuery::Objects(vec![focal_wire::NativeObjectRef::Work(
            focal_model::ArtifactId::from_u128(artifact),
        )]);
        let written = ReadConsistency::AtLeast(ReadToken {
            ledger: self.job.names.ledger,
            sequence: written,
            route_epoch: RouteEpoch(1),
        });
        let Some(objects) = self.read_page(actor, query, 1, written).await? else {
            return Ok(None);
        };
        for object in objects {
            if let NativeObject::Work(work) = object {
                return Ok(Some(work.reference));
            }
        }
        self.note(format!(
            "the work artifact {artifact:032x} read as something else"
        ));
        Ok(None)
    }
    /// Step `step` of lifecycle `i`, from `started`: its creation (`later`
    /// none) or a later step compiled against the claim as read. Issued in
    /// the generation the run's journal stands at; one the owner refused by
    /// name is issued once more, with the next request id, in the generation
    /// the owner admits — never executed twice. The claim and the sequence
    /// it committed at, once committed.
    /// Lifecycle `i`'s evaluator in a run among agents: the agent after its consumer around the
    /// ring, so a work claim's issuer, worker and evaluator are three principals of their own.
    fn evaluator(&self, i: u64) -> Result<&Actor<'_>, LoadError> {
        let Some((agents, _)) = &self.agents else {
            return Err(LoadError::Shape("work goes among agents".into()));
        };
        let count = u64::try_from(agents.len()).map_err(|_| LoadError::Bound("agents"))?;
        let place = self
            .job
            .seat
            .local(i)?
            .checked_add(2)
            .ok_or(LoadError::Bound("lifecycle places"))?;
        let index = place
            .checked_rem(count)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or(LoadError::Bound("agents"))?;
        agents.get(index).ok_or(LoadError::Bound("agents"))
    }
    async fn step(
        &self,
        actor: &Actor<'_>,
        i: u64,
        step: u8,
        started: Instant,
        later: Option<(
            &NativeAuthoredOperation,
            &[NativeObject],
            std::ops::Range<u128>,
        )>,
    ) -> Result<Option<(u128, SessionSeq, Vec<NativeIdentity>)>, LoadError> {
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
            let minted = actor.generations.mint()?;
            let key = RequestKey {
                principal: actor.principal,
                epoch: minted.epoch,
                id: RequestId::from_u128(request),
            };
            let (envelope, claim, identities) = match &later {
                None => {
                    // The claim targets its lifecycle's consumer: the run's worker, or in a run
                    // among agents the agent after its issuer.
                    let subject = match self.sides(i)? {
                        (_, Some((consumer, _))) => consumer.principal,
                        (_, None) => self.job.names.worker,
                    };
                    // A work claim names its evaluator: the agent after its worker.
                    let evaluator = match self.job.lifecycle {
                        Lifecycle::Work => Some(self.evaluator(i)?.principal),
                        _ => None,
                    };
                    let (envelope, claim) = build(self.job, offset, key, subject, evaluator)?;
                    (envelope, claim, Vec::new())
                }
                Some((operation, objects, ids)) => {
                    let names = &self.job.names;
                    let (envelope, identities) = authored::step_envelope(
                        names.ledger,
                        names.profile,
                        actor.principal,
                        names.root,
                        key,
                        operation,
                        objects,
                        ids.clone(),
                    )?;
                    (envelope, 0, identities)
                }
            };
            let sent = Instant::now();
            let mut response = actor.conn.request(envelope).await;
            // A write whose client could not learn its outcome within its clock is resent under its
            // own identity, its bytes and its generation (held open, so the floor never passes
            // it), until its ledger answers it or a hand-off's wait has passed since it was first
            // sent, as the client says to (`retry the same identity`) and as an agent does: the
            // ledger answers it once whichever time it reaches it, from the request's history
            // where it committed. A session that lost its quorum decides nothing until its voters
            // return, and a write sent into it was reported unknown at its client's 30 s clock
            // however soon they returned after it (twelve of 33 nodes killed for 30 s,
            // 2026-10-29). Each pause between is the client's own longest pause between resends.
            let mut resent = false;
            let response = loop {
                match response {
                    Err(ClientError::OutcomeUnknown { request })
                        if sent.elapsed() < watch::WAIT =>
                    {
                        resent = true;
                        focal_rt::time::sleep(RetryPolicy::default().max_backoff)
                            .await
                            .map_err(LoadError::Runtime)?;
                        response = actor.conn.request(*request).await;
                    }
                    answered => break answered,
                }
            };
            if resent {
                let mut outcome = self.outcome.borrow_mut();
                outcome.counts.resent = outcome.counts.resent.saturating_add(1);
            }
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
            if let Finished::Advance { epoch, minimum } = actor.generations.finish(minted.epoch)? {
                self.advance(actor, epoch, minimum).await?;
            }
            match reply {
                Reply::Committed(sequence) => {
                    PROGRESS.committed();
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.counts.committed = outcome.counts.committed.saturating_add(1);
                    if later.is_none() {
                        outcome.created.push(claim);
                    }
                    return Ok(Some((claim, sequence, identities)));
                }
                Reply::Expired => {
                    attempt = attempt.saturating_add(1);
                    if attempt < ATTEMPTS && self.learn(actor).await? {
                        continue;
                    }
                    PROGRESS.refused();
                    let mut outcome = self.outcome.borrow_mut();
                    let counts = &mut outcome.counts;
                    counts.refused = counts.refused.saturating_add(1);
                    counts.refused_by_cause.expired =
                        counts.refused_by_cause.expired.saturating_add(1);
                    note(
                        &mut outcome.refusals,
                        "refused by name in the generation the owner named".into(),
                    );
                }
                Reply::Refused(reason, cause) => {
                    PROGRESS.refused();
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.counts.refused = outcome.counts.refused.saturating_add(1);
                    outcome.counts.refused_by_cause.count(cause);
                    note(&mut outcome.refusals, reason);
                }
                Reply::Unknown(reason) => {
                    PROGRESS.unknown();
                    let mut outcome = self.outcome.borrow_mut();
                    outcome.counts.unknown = outcome.counts.unknown.saturating_add(1);
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
                Some(rate) => place(shared.run_start, rate, job, i)?,
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

/// Write `i`'s place in the aggregate schedule: its seat's ([`Seat::global`]), every `seats`th
/// of its ledger's places, the ledgers' interleaved. Divide only after scaling its global
/// ordinal, so neither a remainder across callers nor rounding a recurring interval changes the
/// aggregate offered rate.
fn place(run_start: Instant, rate: u64, job: &Job, i: u64) -> Result<Instant, LoadError> {
    let ordinal = job.seat.global(i)?;
    let nanos = u128::from(ordinal)
        .checked_mul(1_000_000_000)
        .and_then(|scaled| scaled.checked_div(u128::from(rate)))
        .and_then(|nanos| u64::try_from(nanos).ok())
        .ok_or(LoadError::Bound("schedule"))?;
    run_start
        .checked_add(Duration::from_nanos(nanos))
        .ok_or(LoadError::Bound("schedule"))
}

/// A `deliver` run's consumer as one worker sends: the client it sends on, its principal's
/// generations, and the watch both sides wait on.
#[derive(Clone, Copy)]
struct ConsumerSide<'a> {
    principal: ParticipantId,
    conn: &'a Conn,
    generations: &'a Generations,
    watch: &'a Watch,
}

/// One agent of a run among agents: its principal, the one connection it writes and reads its
/// inbox on, and its journal's request generations.
struct Agent {
    principal: ParticipantId,
    conn: Conn,
    generations: Generations,
}

/// One ledger of a run among agents, as its callers and tails share it: the ledger, its agents,
/// the watch every one of their tails tells, and how the tails read. Owned through `Rc`, so each
/// caller and each tail runs as a task of its own ([`run_phase`]).
#[derive(Clone)]
struct Agents {
    ledger: LedgerId,
    agents: Rc<[Agent]>,
    watch: Rc<Watch>,
    poll: Duration,
    wait_ms: u32,
    seed: u64,
}

/// One caller's share of a phase: the write phase's lifecycles, as the run's issuer (`conn`,
/// with a `deliver` run's consumer) or among its ledger's agents, or the read phase's reads on
/// `conn`.
async fn worker(
    job: Job,
    phase: Phase,
    run_start: Instant,
    generations: &Generations,
    conn: Option<&Conn>,
    (consumer, agents): (Option<ConsumerSide<'_>>, Option<&Agents>),
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
    let delivered = match (phase, job.lifecycle) {
        (Phase::Write, lifecycle) if lifecycle.among_principals() => count,
        _ => 0,
    };
    let mut outcome = Outcome {
        ledger: job.seat.ledger,
        samples: Vec::with_capacity(match phase {
            Phase::Write => 0,
            Phase::Read => count,
        }),
        writes: WriteSamples::reserve(write_limit)?,
        deliveries: Deliveries::reserve(delivered)?,
        ..Outcome::default()
    };
    match phase {
        Phase::Write => {
            outcome.created = Vec::with_capacity(count);
            let shared = Shared {
                job: &job,
                issuer: conn.map(|conn| Actor {
                    principal: job.names.issuer,
                    conn,
                    generations,
                    protocol: std::cell::Cell::new(0),
                }),
                consumer: consumer.map(|side| {
                    (
                        Actor {
                            principal: side.principal,
                            conn: side.conn,
                            generations: side.generations,
                            protocol: std::cell::Cell::new(0),
                        },
                        side.watch,
                    )
                }),
                agents: agents.map(|run| {
                    (
                        run.agents
                            .iter()
                            .map(|agent| Actor {
                                principal: agent.principal,
                                conn: &agent.conn,
                                generations: &agent.generations,
                                protocol: std::cell::Cell::new(0),
                            })
                            .collect(),
                        &*run.watch,
                    )
                }),
                reads: hyper_rt::sync::Semaphore::new(
                    u64::from(job.lifecycle_reads),
                    usize::from(job.inflight.max(1)),
                )
                .map_err(LoadError::Runtime)?,
                run_start,
                outcome: std::cell::RefCell::new(outcome),
            };
            write_open_loop(&shared, job.inflight).await?;
            outcome = shared.outcome.into_inner();
        }
        Phase::Read => {
            let conn = conn.ok_or(LoadError::Bound("read connections"))?;
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
                if read_hit(&result, claim) {
                    outcome.hits = outcome.hits.saturating_add(1);
                }
            }
        }
    }
    Ok(outcome)
}

/// A `deliver` run's consumer: how its connections are opened and as whom, its principal's
/// generations, the watch both sides wait on, and how its tails poll.
struct Consumer<'a> {
    connector: &'a Connector,
    principal: ParticipantId,
    generations: Rc<Generations>,
    watch: Rc<Watch>,
    poll: Duration,
    /// How long an inbox read is asked to wait at the node; zero polls.
    wait_ms: u32,
    seed: u64,
}

/// Request ids of one event tail's pages: the read space of the tail's own worker index.
fn tail_ids(seed: u64, worker: u128) -> impl FnMut() -> Result<u128, LoadError> {
    let mut next = 0u128;
    move || {
        let id = worker_base(seed, worker)?
            .checked_add(READ_OFFSET)
            .and_then(|first| first.checked_add(next))
            .filter(|_| next < PROTOCOL_OFFSET.saturating_sub(READ_OFFSET))
            .ok_or(LoadError::Bound("tail request id space"))?;
        next = next
            .checked_add(1)
            .ok_or(LoadError::Bound("tail request id space"))?;
        Ok(id)
    }
}

/// Runs one phase on every job at once and waits for all of them; the wall
/// time is the phase's, from the first caller's start to the last's end.
///
/// Each caller, and each agent's inbox tail, runs as a task of its own on this
/// shard, with the run's connections and generations (`Rc`, one thread): a
/// client's requests from several callers are carried on its one connection
/// together, and a wake polls the task it was for. A caller's lifecycles share
/// its task (`focal_rt::Concurrent`), so a wake of one polls the caller's
/// others; more throughput than one shard drives comes from more processes,
/// as the generations already model one participant's.
///
/// The run's issuer sends on `connections` connections of its own through `connector`, a
/// `deliver` run's consumer on as many through its own; a run among agents opens none of them,
/// each ledger's agents sending on their own (`agents`, one a ledger in the ledgers' order, each
/// caller among its seat's ledger's).
async fn run_phase(
    jobs: Vec<Job>,
    phase: Phase,
    generations: Rc<Generations>,
    connector: &Connector,
    connections: u16,
    (consumer, agents): (Option<&Consumer<'_>>, &[Agents]),
) -> Result<(Vec<Outcome>, u128, u128), LoadError> {
    let Some(first) = jobs.first() else {
        return Err(LoadError::Bound("zero workers"));
    };
    let names = first.names;
    let limits = first.limits.clone();
    // The issuer's connections, where it writes: a run among agents writes as its agents alone.
    let conns: Rc<[Conn]> = if agents.is_empty() {
        (0..connections.max(1))
            .map(|_| Conn::open(connector, names.issuer, &names, &limits))
            .collect::<Result<Vec<_>, _>>()?
            .into()
    } else {
        Rc::from(Vec::new())
    };
    // A `deliver` run's consumer sends on connections of its own, as a second agent would, and
    // each side's tail reads on one more.
    let consumer_conns: Rc<[Conn]> = match consumer {
        Some(side) => (0..connections.max(1))
            .map(|_| Conn::open(side.connector, side.principal, &names, &limits))
            .collect::<Result<Vec<_>, _>>()?
            .into(),
        None => Rc::from(Vec::new()),
    };
    let tail_conns = match consumer {
        Some(side) => Some((
            Conn::open(side.connector, side.principal, &names, &limits)?,
            Conn::open(connector, names.issuer, &names, &limits)?,
        )),
        None => None,
    };
    // The tails the run reads its sides' inboxes with, by the watch they take their places in: a
    // `deliver` run's two sides in its one, each ledger's agents in the ledger's.
    let mut positions: Vec<(&Watch, usize)> = Vec::new();
    positions
        .try_reserve_exact(agents.len().max(1))
        .map_err(|_| LoadError::Bound("watches"))?;
    match consumer {
        Some(side) => positions.push((&*side.watch, 2)),
        None => {
            for run in agents {
                positions.push((&*run.watch, run.agents.len()));
            }
        }
    }
    for (watch, tails) in &positions {
        watch.reserve_tails(*tails)?;
    }
    let stop = || {
        for (watch, _) in &positions {
            watch.stop();
        }
    };
    // Every caller to its end, the first failure kept; the tails stopped either way, so none
    // outlives the callers it serves. The callers begin, and the run's clock with them, once
    // every tail of every ledger has taken its place: a claim posted before its recipient's tail
    // had was posted behind where that tail reads from (`Watch::all_positioned`).
    let collect = async {
        for (watch, tails) in &positions {
            if let Err(error) = watch.all_positioned(*tails).await {
                stop();
                return Err(error);
            }
        }
        let run_start_epoch_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => since.as_nanos(),
            Err(_) => {
                stop();
                return Err(LoadError::Clock);
            }
        };
        let run_start = Instant::now();
        // Each caller a task of its own: a wake polls the task it was for, where a set polled
        // every caller and each caller's lifecycles at every wake of any (`focal_rt::Concurrent`),
        // and the 500 agents' generator polled so in one task spent 12.8% of its samples reading
        // one-shot replies that had not come (compare CI run 40540102366).
        let mut callers = Vec::new();
        if callers.try_reserve_exact(jobs.len()).is_err() {
            stop();
            return Err(LoadError::Bound("callers"));
        }
        for job in jobs {
            let index = job.index;
            // The issuer's connection the caller sends on, where the run opened them.
            let slot = usize::from(index).checked_rem(conns.len());
            if slot.is_none() && agents.is_empty() {
                abort_all(&callers);
                stop();
                return Err(LoadError::Bound("connections"));
            }
            // The agents of the ledger the caller drives, in a run among agents.
            let ledger = match agents.get(usize::from(job.seat.ledger)) {
                Some(run) => Some(run.clone()),
                None if agents.is_empty() => None,
                None => {
                    abort_all(&callers);
                    stop();
                    return Err(LoadError::Bound("ledgers"));
                }
            };
            let generations = Rc::clone(&generations);
            let conns = Rc::clone(&conns);
            let consumer_conns = Rc::clone(&consumer_conns);
            let side = consumer.map(|side| {
                (
                    side.principal,
                    Rc::clone(&side.generations),
                    Rc::clone(&side.watch),
                )
            });
            let spawned = focal_rt::spawn(async move {
                let conn = match slot {
                    Some(slot) => Some(conns.get(slot).ok_or(LoadError::Bound("connections"))?),
                    None => None,
                };
                let side = match &side {
                    Some((principal, generations, watch)) => Some(ConsumerSide {
                        principal: *principal,
                        conn: slot
                            .and_then(|slot| consumer_conns.get(slot))
                            .ok_or(LoadError::Bound("consumer connections"))?,
                        generations,
                        watch,
                    }),
                    None => None,
                };
                worker(
                    job,
                    phase,
                    run_start,
                    &generations,
                    conn,
                    (side, ledger.as_ref()),
                )
                .await
            });
            match spawned {
                Ok(handle) => callers.push((index, handle)),
                Err(error) => {
                    abort_all(&callers);
                    stop();
                    return Err(LoadError::Runtime(error));
                }
            }
        }
        let mut outcomes = Vec::with_capacity(callers.len());
        let mut failed = None;
        for (index, handle) in callers {
            match handle.await {
                Ok(Ok(outcome)) => outcomes.push((index, outcome)),
                Ok(Err(error)) => {
                    failed.get_or_insert(error);
                }
                Err(_) => {
                    failed.get_or_insert(LoadError::Worker(
                        "a caller's task ended without its outcome",
                    ));
                }
            }
        }
        stop();
        match failed {
            Some(error) => Err(error),
            // The run's time is its callers': the tails it stops after them are not its writes.
            None => Ok((outcomes, run_start.elapsed().as_nanos(), run_start_epoch_ns)),
        }
    };
    let (mut outcomes, wall_ns, run_start_epoch_ns) = match (consumer, &tail_conns) {
        (Some(side), Some((consumer_tail, issuer_tail))) => {
            let (outcomes, consumer_tailed, issuer_tailed) = hyper_rt::combine::join3(
                collect,
                watch::tail(
                    &side.watch,
                    Side::Consumer,
                    (side.poll, side.wait_ms),
                    consumer_tail,
                    names.ledger,
                    side.principal,
                    tail_ids(side.seed, CONSUMER_TAIL_WORKER),
                ),
                watch::tail(
                    &side.watch,
                    Side::Issuer,
                    (side.poll, side.wait_ms),
                    issuer_tail,
                    names.ledger,
                    names.issuer,
                    tail_ids(side.seed, ISSUER_TAIL_WORKER),
                ),
            )
            .await;
            consumer_tailed?;
            issuer_tailed?;
            outcomes?
        }
        _ if agents.is_empty() => collect.await?,
        // Every agent's inbox read beside the callers, each on the agent's one connection, in
        // the agent's ledger.
        _ => {
            // Each agent's tail a task of its own, as each caller is: a delivery to one agent
            // wakes its tail alone.
            let count = agents
                .iter()
                .try_fold(0usize, |count, run| count.checked_add(run.agents.len()))
                .ok_or(LoadError::Bound("agent tails"))?;
            let mut tails = Vec::new();
            if tails.try_reserve_exact(count).is_err() {
                stop();
                return Err(LoadError::Bound("agent tails"));
            }
            for run in agents {
                for index in 0..run.agents.len() {
                    let run = run.clone();
                    let spawned = focal_rt::spawn(async move {
                        let agent = run
                            .agents
                            .get(index)
                            .ok_or(LoadError::Bound("agent tails"))?;
                        watch::tail(
                            &run.watch,
                            Side::Agent,
                            (run.poll, run.wait_ms),
                            &agent.conn,
                            run.ledger,
                            agent.principal,
                            tail_ids(run.seed, AGENT_TAIL_WORKER),
                        )
                        .await
                    });
                    match spawned {
                        Ok(handle) => tails.push(handle),
                        Err(error) => {
                            for tail in &tails {
                                tail.abort();
                            }
                            stop();
                            return Err(LoadError::Runtime(error));
                        }
                    }
                }
            }
            let tailed = async {
                let mut failed = None;
                for tail in tails {
                    match tail.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            failed.get_or_insert(error);
                        }
                        Err(_) => {
                            failed.get_or_insert(LoadError::Worker(
                                "an agent's tail ended without its outcome",
                            ));
                        }
                    }
                }
                failed.map_or(Ok(()), Err)
            };
            let (outcomes, tailed) = hyper_rt::combine::join2(collect, tailed).await;
            tailed?;
            outcomes?
        }
    };
    // In the callers' order, whichever ended first.
    outcomes.sort_unstable_by_key(|(index, _)| *index);
    Ok((
        outcomes.into_iter().map(|(_, outcome)| outcome).collect(),
        wall_ns,
        run_start_epoch_ns,
    ))
}

/// Cancels every task in `handles`: a phase that could not start them all runs none of them on.
fn abort_all<T>(handles: &[(u16, focal_rt::JoinHandle<T>)]) {
    for (_, handle) in handles {
        handle.abort();
    }
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

/// The enrolled clients of `count` agents under `directory`: each agent a client data directory of
/// its own (`agent-*`, as `focal --data-dir <directory>/<agent> enroll context <agent>` leaves it),
/// taken as its one context's enrollment (`CLIENT.contexts/enrollment-<agent>`), in their order,
/// the first `count`; fewer is refused. The entries are listed to a stated bound, the agents' and a
/// few others.
fn enrolled_agents(directory: &Path, count: u16) -> Result<Vec<PathBuf>, LoadError> {
    let most = usize::from(count).saturating_add(64);
    let mut found = Vec::new();
    for entry in std::fs::read_dir(directory)?.take(most.saturating_add(1)) {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !entry.file_type()?.is_dir() || !name.starts_with("agent-") {
            continue;
        }
        // An agent whose enrollment never finished has no context: it is not one of them.
        let enrollment = entry
            .path()
            .join("CLIENT.contexts")
            .join(format!("enrollment-{name}"));
        if enrollment.is_dir() {
            found.push(enrollment);
        }
    }
    found.sort();
    if found.len() < usize::from(count) {
        return Err(LoadError::Shape(format!(
            "{} holds {} agent enrollments of the {count} asked",
            directory.display(),
            found.len()
        )));
    }
    found.truncate(usize::from(count));
    Ok(found)
}

/// The keys of the client data directory an enrollment lies in (`<data-dir>/CLIENT.contexts/
/// enrollment-<name>`, as `focal enroll context` leaves it), found as the CLI that enrolled it
/// found them (29 §2): its journal and key are sealed under them.
fn client_keys(enrollment: &Path) -> Result<focal_seal::DataKeys, LoadError> {
    let data_dir = enrollment.parent().and_then(Path::parent).ok_or_else(|| {
        LoadError::Shape(format!(
            "{} is no context's enrollment",
            enrollment.display()
        ))
    })?;
    Ok(focal_node::config::Settings::default().data_keys(data_dir)?)
}

/// The principal of agent `agent` of a run among agents: the top bit set, then the run's seed and
/// the agent's index, so no two agents share one and none is another run's.
fn agent_principal(seed: u64, agent: u16) -> Result<ParticipantId, LoadError> {
    u128::from(seed)
        .checked_shl(16)
        .map(|seed| seed | u128::from(agent) | (1u128 << 127))
        .map(ParticipantId::from_u128)
        .ok_or(LoadError::Bound("agent identity space"))
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
        total.committed = total.committed.saturating_add(outcome.counts.committed);
        total.refused = total.refused.saturating_add(outcome.counts.refused);
        total.unknown = total.unknown.saturating_add(outcome.counts.unknown);
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

/// The run on one shard of this thread, its root owning the nodes, the
/// connections and the generations every caller shares.
pub fn run(shape: WorkloadShape) -> Result<Report, LoadError> {
    focal_rt::block_on(run_on_shard(shape)).map_err(LoadError::Runtime)?
}

/// An embedded node the run opened, a ledger of its own: the directory it keeps its data in (a
/// temporary one the run removes, unless the shape named one), the settings it was opened with,
/// and its host.
struct Node {
    root: PathBuf,
    settings: Settings,
    hosted: Hosted,
    temporary: Option<tempfile::TempDir>,
}
impl Node {
    /// A node on the shape's `data_dir`, or on a fresh temporary directory, activated natively
    /// as `profile` on its first open; and the names its identity gives the ledger.
    fn open(
        shape: &WorkloadShape,
        profile: NativeContentProfile,
        limits: &WireLimits,
    ) -> Result<(Self, Names), LoadError> {
        let (root, temporary) = match &shape.data_dir {
            Some(directory) => {
                std::fs::create_dir_all(directory)?;
                (directory.clone(), None)
            }
            None => {
                let temporary = tempfile::tempdir()?;
                (temporary.path().to_path_buf(), Some(temporary))
            }
        };
        let mut settings = Settings::default();
        settings.node.data_dir = Some(root.clone());
        focal_node::native_activation::activate_local(&settings, profile)?;
        let hosted = Hosted::open(&settings, limits)?;
        let identity = focal_node::node_files::read_identity(&settings, &root)?;
        let names = Names {
            ledger: identity.ledger,
            issuer: identity.issuer,
            worker: identity.worker,
            root: identity.root,
            profile,
        };
        Ok((
            Self {
                root,
                settings,
                hosted,
                temporary,
            },
            names,
        ))
    }
}

/// The ledgers a run drives, in the shape's order: each one's names, and what connections to it
/// are opened through — on the embedded transport its own node, otherwise the run's one
/// connector, which reaches every ledger the run drives (a running node's socket, the
/// enrollment's cluster).
struct Ledgers {
    names: Vec<Names>,
    connectors: Vec<Connector>,
}
impl Ledgers {
    fn names(&self, ledger: u16) -> Result<&Names, LoadError> {
        self.names
            .get(usize::from(ledger))
            .ok_or(LoadError::Bound("ledgers"))
    }
    fn connector(&self, ledger: u16) -> Result<&Connector, LoadError> {
        match self.connectors.as_slice() {
            [one] => Ok(one),
            each => each
                .get(usize::from(ledger))
                .ok_or(LoadError::Bound("ledgers")),
        }
    }
}

/// Floors advanced and requests refused by name, summed over `journals`: every principal's.
fn journal_counts<'a>(
    journals: impl Iterator<Item = &'a Generations>,
) -> Result<(u64, u64), LoadError> {
    let (mut advanced, mut expired) = (0u64, 0u64);
    for journal in journals {
        let (floors, refused) = journal.counts()?;
        advanced = advanced.saturating_add(floors);
        expired = expired.saturating_add(refused);
    }
    Ok((advanced, expired))
}

/// What every tail `watch` tells read: a `deliver` run's two sides, or a ledger's agents.
fn tail_totals(watch: &Watch) -> TailCounts {
    let mut total = TailCounts::default();
    for side in [Side::Consumer, Side::Issuer, Side::Agent] {
        total.add(&watch.counts(side));
    }
    total
}

async fn run_on_shard(shape: WorkloadShape) -> Result<Report, LoadError> {
    let limits = WireLimits::default();
    let profile = content_profile(shape.profile());
    let ledger_count = shape.ledger_count().map_err(LoadError::Shape)?;

    // The nodes: opened here, one a ledger, or already running behind a socket or a cluster.
    let mut nodes = Vec::new();
    // The enrolled issuer's founder ledger, which every enrolled principal of the run names.
    let mut founder = None;
    let mut ledgers =
        match shape.transport {
            Transport::Embedded => {
                let mut ledgers = Ledgers {
                    names: Vec::with_capacity(usize::from(ledger_count)),
                    connectors: Vec::with_capacity(usize::from(ledger_count)),
                };
                nodes.reserve_exact(usize::from(ledger_count));
                for _ in 0..ledger_count {
                    let (node, names) = Node::open(&shape, profile, &limits)?;
                    ledgers
                        .connectors
                        .push(Connector::Embedded(node.hosted.host.clone()));
                    ledgers.names.push(names);
                    nodes.push(node);
                }
                ledgers
            }
            Transport::Unix => {
                let root = shape
                    .data_dir
                    .clone()
                    .ok_or_else(|| LoadError::Shape("transport unix needs data_dir".into()))?;
                let settings = Settings::default();
                let identity = focal_node::node_files::read_identity(&settings, &root)?;
                let now = i64::try_from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| LoadError::Clock)?
                        .as_secs(),
                )
                .map_err(|_| LoadError::Clock)?;
                let keys = settings.found_data_keys(&root)?;
                let actor = focal_node::network_join::local_unix_principal(
                    &root,
                    keys.as_ref(),
                    &identity,
                    now,
                )?;
                Ledgers {
                    names: vec![Names {
                        ledger: identity.ledger,
                        issuer: actor,
                        worker: identity.worker,
                        root: identity.root,
                        profile,
                    }],
                    connectors: vec![Connector::Unix(root.join("focal.sock"))],
                }
            }
            Transport::Enrolled => {
                let enrollment = shape.enrollment.clone().ok_or_else(|| {
                    LoadError::Shape("transport enrolled needs enrollment".into())
                })?;
                // A `deliver` run's claims target its consumer, named below.
                let worker = match shape.lifecycle {
                    Lifecycle::Deliver | Lifecycle::Work => [0; 16],
                    _ => shape
                        .worker
                        .as_deref()
                        .ok_or_else(|| LoadError::Shape("transport enrolled needs worker".into()))
                        .and_then(|text| {
                            focal_client::input::parse_id(text)
                                .map_err(|_| LoadError::Shape("worker is 32 hex digits".into()))
                        })?,
                };
                let pending = focal_node::network_join::PendingClientJoin::resume_shared(
                    &enrollment,
                    &client_keys(&enrollment)?,
                )?;
                let client = pending.remote_client(unix_now()?, &limits)?;
                let enrolled = Names {
                    ledger: client.build.ledger,
                    issuer: client.build.actor,
                    worker: ParticipantId(worker),
                    root: client.build.root,
                    profile,
                };
                founder = Some(enrolled.ledger);
                // Each ledger a session of the enrollment's tenant, addressed as an enrolled context
                // addresses one (`focal context add --enrolled-as … --session …`): the tenant's grant
                // reaches it, and its creations cite the root the enrollment names.
                let names = match &shape.ledgers {
                    Some(entries) => entries
                        .iter()
                        .map(|entry| {
                            let session = entry
                                .session_id()
                                .map_err(LoadError::Shape)?
                                .ok_or_else(|| {
                                    LoadError::Shape(
                                        "on enrolled each ledger names its session".into(),
                                    )
                                })?;
                            Ok(Names {
                                ledger: LedgerId {
                                    tenant: enrolled.ledger.tenant,
                                    session: SessionId(session),
                                },
                                ..enrolled
                            })
                        })
                        .collect::<Result<Vec<_>, LoadError>>()?,
                    None => vec![enrolled],
                };
                Ledgers {
                    names,
                    connectors: vec![Connector::Enrolled(Box::new(pending))],
                }
            }
        };
    // A `deliver` run's consumer: a second enrolled client, or on the embedded node its worker
    // identity; every claim targets it.
    let consumer_connector = match (shape.lifecycle, shape.transport) {
        (Lifecycle::Deliver, Transport::Enrolled) if shape.agents.is_none() => {
            let directory = shape.consumer_enrollment.clone().ok_or_else(|| {
                LoadError::Shape("deliver over enrolled needs consumer_enrollment".into())
            })?;
            let pending = focal_node::network_join::PendingClientJoin::resume_shared(
                &directory,
                &client_keys(&directory)?,
            )?;
            let client = pending.remote_client(unix_now()?, &limits)?;
            let names = ledgers
                .names
                .first_mut()
                .ok_or(LoadError::Bound("ledgers"))?;
            if client.build.ledger != names.ledger {
                return Err(LoadError::Shape(
                    "the consumer is enrolled with another ledger".into(),
                ));
            }
            names.worker = client.build.actor;
            Some(Connector::Enrolled(Box::new(pending)))
        }
        // A run among agents has no consumer of its own: each agent is the next one's.
        (Lifecycle::Deliver, Transport::Embedded) if shape.agents.is_none() => {
            match ledgers.connector(0)? {
                Connector::Embedded(host) => Some(Connector::Embedded(host.clone())),
                _ => None,
            }
        }
        _ => None,
    };
    // The first ledger's names: a run of one ledger's, which a run without agents drives.
    let names = *ledgers.names(0)?;
    if shape.lifecycle == Lifecycle::Deliver && names.worker == names.issuer {
        return Err(LoadError::Shape(
            "deliver's consumer must be a principal of its own".into(),
        ));
    }
    let consumer_generations = Rc::new(Generations::new());
    // What each ledger's tails tell its lifecycles, at most its callers' writes under way.
    let mut watches = Vec::with_capacity(usize::from(ledger_count));
    for ledger in 0..ledger_count {
        let bound = share(u64::from(shape.concurrency), ledger_count, ledger)?
            .checked_mul(u64::from(shape.inflight))
            .and_then(|bound| usize::try_from(bound).ok())
            .ok_or(LoadError::Bound("messages under way"))?;
        watches.push(Rc::new(Watch::new(bound)));
    }
    // A run among agents: each agent a principal of its own on its ledger (agent `a` on ledger
    // `a mod ledgers`, `seats::agent_seat`), with its connection and its journal's generations.
    PROGRESS.agents(u64::from(shape.agents.unwrap_or(0)));
    let mut groups: Vec<Vec<Agent>> = Vec::with_capacity(usize::from(ledger_count));
    for ledger in 0..ledger_count {
        let its = usize::try_from(share(
            u64::from(shape.agents.unwrap_or(0)),
            ledger_count,
            ledger,
        )?)
        .map_err(|_| LoadError::Bound("agents"))?;
        let mut group = Vec::new();
        group
            .try_reserve_exact(its)
            .map_err(|_| LoadError::Bound("agents"))?;
        groups.push(group);
    }
    match (shape.agents, &shape.agent_enrollments) {
        // Each an enrolled client of its own: the principal its credential names, on an
        // endpoint and a connection of its own.
        (Some(count), Some(directory)) => {
            for (agent, enrollment) in (0..count).zip(enrolled_agents(directory, count)?) {
                let seat = seats::agent_seat(agent, ledger_count)?;
                let pending = focal_node::network_join::PendingClientJoin::resume_shared(
                    &enrollment,
                    &client_keys(&enrollment)?,
                )?;
                let client = pending.remote_client(unix_now()?, &limits)?;
                // Enrolled in the run's cluster, as the run's enrollment is: what each invitation
                // names is the founder's ledger, and its tenant the ledgers'.
                if Some(client.build.ledger) != founder {
                    return Err(LoadError::Shape(format!(
                        "{} is enrolled with another ledger",
                        enrollment.display()
                    )));
                }
                let principal = client.build.actor;
                let connector = Connector::Enrolled(Box::new(pending));
                // A ledger's agent `n` on its home `n mod homes`, the ledger's agents spread over
                // the nodes holding its replica, and the ledger's other homes after it in turn:
                // the nodes it asks once its own gives no answer.
                let homes = shape.homes_of(seat.ledger).unwrap_or(&[]);
                let first = usize::from(seat.index)
                    .checked_rem(homes.len())
                    .unwrap_or(0);
                let mut routes = Vec::new();
                routes
                    .try_reserve_exact(homes.len())
                    .map_err(|_| LoadError::Bound("homes"))?;
                routes.extend(homes.iter().cycle().skip(first).take(homes.len()));
                let conn = Conn::open_homed(
                    &connector,
                    principal,
                    ledgers.names(seat.ledger)?,
                    &limits,
                    &routes,
                )?;
                PROGRESS.agent_opened();
                groups
                    .get_mut(usize::from(seat.ledger))
                    .ok_or(LoadError::Bound("ledgers"))?
                    .push(Agent {
                        principal,
                        conn,
                        generations: Generations::new(),
                    });
            }
        }
        (Some(count), None) => {
            for agent in 0..count {
                let seat = seats::agent_seat(agent, ledger_count)?;
                let principal = agent_principal(shape.seed, agent)?;
                let conn = Conn::open(
                    ledgers.connector(seat.ledger)?,
                    principal,
                    ledgers.names(seat.ledger)?,
                    &limits,
                )?;
                PROGRESS.agent_opened();
                groups
                    .get_mut(usize::from(seat.ledger))
                    .ok_or(LoadError::Bound("ledgers"))?
                    .push(Agent {
                        principal,
                        conn,
                        generations: Generations::new(),
                    });
            }
        }
        (None, _) => {}
    }
    let agents: Vec<Agents> = match shape.agents {
        Some(_) => (0..ledger_count)
            .zip(groups)
            .map(|(ledger, group)| {
                Ok(Agents {
                    ledger: ledgers.names(ledger)?.ledger,
                    agents: Rc::from(group),
                    watch: Rc::clone(
                        watches
                            .get(usize::from(ledger))
                            .ok_or(LoadError::Bound("ledgers"))?,
                    ),
                    poll: Duration::from_millis(u64::from(shape.poll_ms)),
                    wait_ms: shape.inbox_wait_ms,
                    seed: shape.seed,
                })
            })
            .collect::<Result<_, LoadError>>()?,
        None => Vec::new(),
    };
    let consumer = match &consumer_connector {
        Some(consumer_connector) => Some(Consumer {
            connector: consumer_connector,
            principal: names.worker,
            generations: Rc::clone(&consumer_generations),
            watch: Rc::clone(watches.first().ok_or(LoadError::Bound("ledgers"))?),
            poll: Duration::from_millis(u64::from(shape.poll_ms)),
            wait_ms: shape.inbox_wait_ms,
            seed: shape.seed,
        }),
        None => None,
    };
    let pace = shape.rate;

    let connections = shape.connections.unwrap_or(shape.concurrency);
    // Writes: every caller its seat's share of its ledger's lifecycles, in its own id space.
    let mut jobs = Vec::with_capacity(usize::from(shape.concurrency));
    for index in 0..shape.concurrency {
        let seat = Seat::of(index, shape.concurrency, ledger_count)?;
        jobs.push(Job {
            index,
            inflight: shape.inflight,
            seat,
            count: seat.lifecycles(shape.claims)?,
            base: worker_base(shape.seed, u128::from(index))?,
            names: *ledgers.names(seat.ledger)?,
            limits: limits.clone(),
            created: Vec::new(),
            lifecycle: shape.lifecycle,
            lifecycle_reads: shape.lifecycle_reads(),
            pace,
        });
    }
    // One journal's generations for every caller, as N processes of one
    // participant share one (the audit's F12).
    let generations = Rc::new(Generations::new());
    // The write phase, its progress said on stderr as it goes (`progress`): a run that stalls
    // says where, though its report never comes.
    let written = race2(
        run_phase(
            jobs,
            Phase::Write,
            Rc::clone(&generations),
            ledgers.connector(0)?,
            connections,
            (consumer.as_ref(), &agents),
        ),
        crate::progress::report(Instant::now()),
    )
    .await;
    let (write_outcomes, write_nanos, write_phase_start_epoch_ns) = match written {
        Either::First(written) => written?,
        Either::Second(never) => match never {},
    };
    // Every principal's journal: the issuer's, a consumer's, and each agent's.
    let (floors_advanced, expired) = journal_counts(
        [&*generations, &*consumer_generations].into_iter().chain(
            agents
                .iter()
                .flat_map(|run| run.agents.iter().map(|agent| &agent.generations)),
        ),
    )?;
    let mut writes = merged(&write_outcomes);
    let (committed, refused, unknown) = (writes.committed, writes.refused, writes.unknown);
    // Each ledger's lifecycles counted apart, its callers' outcomes together, and every ledger's
    // summed.
    let mut rows = Vec::with_capacity(usize::from(ledger_count));
    let mut totals = LifecycleCounts::default();
    for ledger in 0..ledger_count {
        let mut counts = LifecycleCounts::default();
        let mut refusals = Vec::new();
        for outcome in write_outcomes
            .iter()
            .filter(|outcome| outcome.ledger == ledger)
        {
            counts.add(&outcome.counts);
            for (reason, count) in &outcome.refusals {
                note_count(&mut refusals, reason.clone(), *count);
            }
        }
        totals.add(&counts);
        let tails = watches
            .get(usize::from(ledger))
            .map(|watch| tail_totals(watch))
            .unwrap_or_default();
        rows.push(LedgerReport {
            ledger: ledgers.names(ledger)?.ledger.into(),
            callers: u16::try_from(share(u64::from(shape.concurrency), ledger_count, ledger)?)
                .map_err(|_| LoadError::Bound("callers"))?,
            agents: agents
                .get(usize::from(ledger))
                .map_or(Ok(0), |run| u16::try_from(run.agents.len()))
                .map_err(|_| LoadError::Bound("agents"))?,
            counts,
            tail_events: tails.events,
            tail_failures: tails.failed,
            refusals,
        });
    }
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
    let deliver = if consumer.is_some() || !agents.is_empty() {
        let mut measured = DeliveryMeasurements::measure(
            write_outcomes
                .iter()
                .flat_map(|outcome| outcome.deliveries.iter()),
            warmup,
        )?;
        measured.undelivered = totals.undelivered;
        // Every ledger's tails together.
        let (mut consumer_tail, mut issuer_tail, mut agents_tail) = (
            TailCounts::default(),
            TailCounts::default(),
            TailCounts::default(),
        );
        for watch in &watches {
            consumer_tail.add(&watch.counts(Side::Consumer));
            issuer_tail.add(&watch.counts(Side::Issuer));
            agents_tail.add(&watch.counts(Side::Agent));
        }
        measured.agents = shape.agents.unwrap_or(0);
        measured.agent_polls = agents_tail.polls;
        measured.agent_events = agents_tail.events;
        measured.agent_waits = agents_tail.waited;
        measured.consumer_polls = consumer_tail.polls;
        measured.consumer_events = consumer_tail.events;
        measured.consumer_waits = consumer_tail.waited;
        measured.issuer_polls = issuer_tail.polls;
        measured.issuer_events = issuer_tail.events;
        measured.issuer_waits = issuer_tail.waited;
        measured.tail_failures = consumer_tail
            .failed
            .saturating_add(issuer_tail.failed)
            .saturating_add(agents_tail.failed);
        Some(measured)
    } else {
        None
    };
    let mut refusals = Vec::new();
    for outcome in &write_outcomes {
        for (reason, count) in &outcome.refusals {
            note_count(&mut refusals, reason.clone(), *count);
        }
    }

    // Reads: the workers that created claims cycle through their own, in a run of one ledger.
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
                seat: Seat::of(index, reader_count, 1)?,
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
            Rc::clone(&generations),
            ledgers.connector(0)?,
            reader_count.min(connections),
            (None, &[]),
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
    // Every handle to an embedded host goes before it closes: its owner joins once the last does,
    // each agent's connection among them, held by the run's agents (the phases' tasks, joined,
    // hold none), and each ledger's connector.
    drop(agents);
    drop(consumer);
    drop(consumer_connector);
    drop(ledgers);

    // The embedded nodes: their size on disk, and the node's recovery when asked.
    let mut data_dir_bytes = None;
    let (mut reopen_ms, mut first_read_after_reopen_ns, mut reopen_read_hit) = (None, None, None);
    for node in nodes {
        node.hosted.close()?;
        let bytes = directory_bytes(&node.root)?;
        data_dir_bytes = Some(data_dir_bytes.unwrap_or(0u64).saturating_add(bytes));
        // A run of one ledger, one node (`reopen` is refused over ledgers).
        if shape.reopen {
            let started = Instant::now();
            let hosted = Hosted::open(&node.settings, &limits)?;
            reopen_ms = Some(started.elapsed().as_millis());
            if let Some(claim) = write_outcomes
                .iter()
                .rev()
                .find_map(|outcome| outcome.created.last().copied())
            {
                let probe = Connector::Embedded(hosted.host.clone());
                let conn = Conn::open(&probe, names.issuer, &names, &limits)?;
                let request = worker_base(shape.seed, PROBE_WORKER)?
                    .checked_add(READ_OFFSET)
                    .ok_or(LoadError::Bound("probe id space"))?;
                let envelope = read_envelope(&names, request, claim);
                let started = Instant::now();
                let result = conn.request(envelope).await;
                first_read_after_reopen_ns = Some(started.elapsed().as_nanos());
                reopen_read_hit = Some(read_hit(&result, claim));
                drop(conn);
                drop(probe);
            }
            hosted.close()?;
        }
        drop(node.temporary);
    }

    Ok(Report {
        committed,
        refused,
        unknown,
        expired,
        lifecycles: totals.written,
        abandoned: totals.abandoned,
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
        deliver,
        ledgers: rows,
        totals,
        shape,
    })
}
