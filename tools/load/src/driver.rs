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
use crate::measurements::{self, WriteMeasurements, WriteOutcome, WriteSample, WriteSamples};
use crate::native;
use crate::report::{self, Latency, Report};
use crate::shape::{Profile, Transport, WorkloadShape};
use focal_client::{
    Client, ClientError, EmbeddedTransport, QuicTransport, RetryPolicy, UnixTransport,
};
use focal_ledger::NativeContentProfile;
use focal_model::{
    ClaimId, LedgerId, ParticipantId, RequestEpoch, RequestId, RequestKey, RootCommandId,
    RouteEpoch,
};
use focal_node::{
    config::Settings,
    embedded::{EmbeddedNode, decode_identity},
    host::LocalHost,
};
use focal_wire::{
    AuthenticatedPeer, NativeClaimExpand, NativeErrorCode, NativeMutationReply, NativeObject,
    NativeReadQuery, NativeReadRequest, NativeRefusalKind, Operation, PeerGrant, PeerRole,
    QuicConnector, ReadConsistency, RequestEnvelope, Response, ResponseEnvelope, RouteHint,
    WireLimits,
};
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
/// Distinct refusal reasons kept per run.
const MAX_REFUSALS: usize = 8;
/// Characters kept of one refusal reason.
const MAX_REASON_CHARS: usize = 200;
/// Directory entries the size walk visits before it refuses to count further.
const MAX_WALK_ENTRIES: usize = 100_000;
/// Directory depth the size walk descends to.
const MAX_WALK_DEPTH: usize = 16;

/// What a worker opens its connection to; owned, so every worker thread
/// carries its own and nothing is shared across threads.
#[derive(Clone)]
enum Connector {
    Embedded(LocalHost),
    Unix(PathBuf),
    /// An enrolled client's TLS and first route; each worker dials its own
    /// QUIC connection with them.
    Enrolled {
        tls: quinn::ClientConfig,
        initial: RouteHint,
    },
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
    Unix(Client<UnixTransport>),
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
                let transport = UnixTransport::connect(socket, limits.clone())?;
                Ok(Self::Unix(Client::new(
                    transport,
                    RetryPolicy::default(),
                    limits.clone(),
                    1,
                )?))
            }
            Connector::Enrolled { tls, initial } => {
                let address = if initial.endpoint.starts_with('[') {
                    "[::]:0"
                } else {
                    "0.0.0.0:0"
                }
                .parse()
                .map_err(|_| LoadError::Shape("local address".into()))?;
                let connector = QuicConnector::bind(address, tls.clone(), limits.clone())?;
                // One route, one connection: the transport's bound is the
                // routes a client may hold, as the CLI's enrolled context.
                let transport = QuicTransport::new(connector, initial.clone(), 16)?;
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
    /// The callers of the phase, among which `index` is one.
    callers: u16,
    /// Writes to submit, or reads to issue.
    count: u64,
    /// The first identity of this worker's space.
    base: u128,
    connector: Connector,
    names: Names,
    limits: WireLimits,
    /// Read phase: the claims this worker created, cycled through.
    created: Vec<u128>,
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
    refusals: Vec<String>,
}

fn note(refusals: &mut Vec<String>, reason: String) {
    let reason: String = reason.chars().take(MAX_REASON_CHARS).collect();
    if refusals.len() < MAX_REFUSALS && !refusals.contains(&reason) {
        refusals.push(reason);
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
    Committed,
    /// Refused by name: the request's generation is closed, or not admitted
    /// yet (the audit's F12); never executed.
    Expired,
    Refused(String),
    Unknown(String),
}

fn classify(result: Result<ResponseEnvelope, ClientError>) -> Reply {
    match result {
        Ok(envelope) => match envelope.result {
            Response::Native(NativeMutationReply::Committed(_)) => Reply::Committed,
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
                Reply::Refused(format!("refused: {refusal:?}"))
            }
            Response::Native(NativeMutationReply::Pending(_)) => {
                Reply::Unknown("pending: ticket, not a receipt".into())
            }
            Response::Error(error) => Reply::Refused(format!("access: {error:?}")),
            _ => Reply::Refused("unexpected reply kind".into()),
        },
        // The client gave up on a refusal the node made (capacity, after
        // its bounded retries): admitted nothing, so refused, not unknown.
        Err(ClientError::Access(error)) => Reply::Refused(format!("access: {error:?}")),
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

/// One worker's means of speaking to the owner: its runtime and client,
/// its job, the run's generations and its own protocol request ids.
struct Caller<'a> {
    runtime: &'a tokio::runtime::Runtime,
    conn: &'a Conn,
    job: &'a Job,
    generations: &'a Generations,
    protocol: Protocol,
}
impl Caller<'_> {
    fn send(&self, envelope: RequestEnvelope) -> Result<ResponseEnvelope, ClientError> {
        self.runtime.block_on(self.conn.request(envelope))
    }
    /// The owner's window, read after a refusal by name: the run issues
    /// where the owner admits. `false` when the window could not be read.
    fn learn(&mut self, outcome: &mut Outcome) -> Result<bool, LoadError> {
        let request = self.protocol.request(self.job)?;
        match self.send(window_envelope(&self.job.names, request)) {
            Ok(envelope) => {
                if let Response::NativeRead(page) = &envelope.result
                    && let Some(NativeObject::Epochs(window)) = page.objects.first()
                {
                    let open: Vec<RequestEpoch> =
                        window.open.iter().map(|open| open.epoch).collect();
                    self.generations.learn(window.floor, &open)?;
                    return Ok(true);
                }
                note(
                    &mut outcome.refusals,
                    format!("window: unexpected reply {:?}", envelope.result),
                );
            }
            Err(error) => note(&mut outcome.refusals, format!("window: client: {error}")),
        }
        Ok(false)
    }
    /// The floor advance the journal sends once the generation below
    /// drained: committed, the floor stands at `minimum`; refused by name,
    /// the window is learned; otherwise the next reply sends it again.
    fn advance(
        &mut self,
        outcome: &mut Outcome,
        epoch: RequestEpoch,
        minimum: RequestEpoch,
    ) -> Result<(), LoadError> {
        let request = RequestKey {
            principal: self.job.names.issuer,
            epoch,
            id: RequestId::from_u128(self.protocol.request(self.job)?),
        };
        let names = &self.job.names;
        let envelope = native::advance_envelope(names.ledger, names.profile, request, minimum)?;
        match classify(self.send(envelope)) {
            Reply::Committed => self.generations.advanced(minimum),
            Reply::Expired => {
                self.generations.advance_failed()?;
                self.learn(outcome).map(|_| ())
            }
            Reply::Refused(reason) | Reply::Unknown(reason) => {
                self.generations.advance_failed()?;
                note(&mut outcome.refusals, format!("floor advance: {reason}"));
                Ok(())
            }
        }
    }
}

fn worker(
    job: Job,
    phase: Phase,
    run_start: Instant,
    generations: &Generations,
    runtime: &tokio::runtime::Runtime,
    conn: &Conn,
) -> Result<Outcome, LoadError> {
    let count = usize::try_from(job.count).map_err(|_| LoadError::Bound("worker count"))?;
    // The shape bounds logical writes at MAX_CLAIMS; each has at most
    // ATTEMPTS samples. The complete trace is funded before sending work.
    let write_limit = match phase {
        Phase::Write => count
            .checked_mul(usize::try_from(ATTEMPTS).map_err(|_| LoadError::Bound("write attempts"))?)
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
    let mut caller = Caller {
        runtime,
        conn,
        job: &job,
        generations,
        protocol: Protocol { next: 0 },
    };
    match phase {
        Phase::Write => {
            outcome.created = Vec::with_capacity(count);
            for i in 0..job.count {
                let offset = u128::from(i);
                // Every write is issued in the generation the run's
                // journal stands at; one the owner refused by name is
                // issued once more, with the next request id, in the
                // generation the owner admits — never executed twice.
                let mut attempt = 0u128;
                loop {
                    let request = offset
                        .checked_mul(ATTEMPTS)
                        .and_then(|slot| slot.checked_add(attempt))
                        .and_then(|slot| slot.checked_add(1))
                        .filter(|slot| *slot < CLAIM_OFFSET)
                        .and_then(|slot| job.base.checked_add(slot))
                        .ok_or(LoadError::Bound("request id space"))?;
                    let minted = generations.mint()?;
                    let key = RequestKey {
                        principal: job.names.issuer,
                        epoch: minted.epoch,
                        id: RequestId::from_u128(request),
                    };
                    let (envelope, claim) = build(&job, offset, key)?;
                    // Paced, a request starts at its place on the schedule
                    // and its latency counts from there, however late the
                    // worker reached it (wrk2); unpaced, from its send.
                    let started = match job.pace {
                        Some(interval) => {
                            // The callers' schedules are spread over the
                            // interval, caller `index` of `concurrency` at its
                            // share of it: the offered rate arrives evenly, as
                            // the other systems' generator sends it, never as
                            // every caller's request at one instant.
                            let phase = interval
                                .checked_mul(u32::from(job.index))
                                .and_then(|spread| spread.checked_div(u32::from(job.callers)))
                                .ok_or(LoadError::Bound("schedule"))?;
                            let place = u32::try_from(i)
                                .ok()
                                .and_then(|i| interval.checked_mul(i))
                                .and_then(|offset| offset.checked_add(phase))
                                .and_then(|offset| run_start.checked_add(offset))
                                .ok_or(LoadError::Bound("schedule"))?;
                            if let Some(wait) = place.checked_duration_since(Instant::now()) {
                                std::thread::sleep(wait);
                            }
                            place
                        }
                        None => Instant::now(),
                    };
                    let sent = Instant::now();
                    let response = caller.send(envelope);
                    let finished = Instant::now();
                    let reply = classify(response);
                    outcome.writes.record(WriteSample {
                        start_ns: started.saturating_duration_since(run_start).as_nanos(),
                        sent_ns: sent.saturating_duration_since(run_start).as_nanos(),
                        finished_ns: finished.saturating_duration_since(run_start).as_nanos(),
                        worker: job.index,
                        write: i,
                        attempt,
                        epoch: minted.epoch,
                        request,
                        outcome: match &reply {
                            Reply::Committed => WriteOutcome::Committed,
                            Reply::Expired => WriteOutcome::Expired,
                            Reply::Refused(_) => WriteOutcome::Refused,
                            Reply::Unknown(_) => WriteOutcome::Unknown,
                        },
                    })?;
                    if let Finished::Advance { epoch, minimum } =
                        generations.finish(minted.epoch)?
                    {
                        caller.advance(&mut outcome, epoch, minimum)?;
                    }
                    match reply {
                        Reply::Committed => {
                            outcome.committed = outcome.committed.saturating_add(1);
                            outcome.created.push(claim);
                        }
                        Reply::Expired => {
                            attempt = attempt.saturating_add(1);
                            if attempt < ATTEMPTS && caller.learn(&mut outcome)? {
                                continue;
                            }
                            outcome.refused = outcome.refused.saturating_add(1);
                            note(
                                &mut outcome.refusals,
                                "refused by name in the generation the owner named".into(),
                            );
                        }
                        Reply::Refused(reason) => {
                            outcome.refused = outcome.refused.saturating_add(1);
                            note(&mut outcome.refusals, reason);
                        }
                        Reply::Unknown(reason) => {
                            outcome.unknown = outcome.unknown.saturating_add(1);
                            note(&mut outcome.refusals, reason);
                        }
                    }
                    break;
                }
            }
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
                let result = runtime.block_on(conn.request(envelope));
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
/// time is the phase's, from the first spawn to the last join.
fn run_phase(
    jobs: Vec<Job>,
    phase: Phase,
    generations: &Generations,
    connections: u16,
) -> Result<(Vec<Outcome>, u128, u128), LoadError> {
    // The callers' clients, on one runtime they all drive: a QUIC endpoint
    // binds to the runtime it is made in, and a client's requests from
    // several callers are carried on its one connection together.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(usize::from(connections.clamp(1, 4)))
        .enable_all()
        .build()?;
    let Some(first) = jobs.first() else {
        return Err(LoadError::Bound("zero workers"));
    };
    let conns = {
        let _entered = runtime.enter();
        (0..connections.max(1))
            .map(|_| Conn::open(&first.connector, &first.names, &first.limits))
            .collect::<Result<Vec<_>, _>>()?
    };
    let run_start_epoch_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LoadError::Clock)?
        .as_nanos();
    let run_start = Instant::now();
    let outcomes = std::thread::scope(|scope| -> Result<Vec<Outcome>, LoadError> {
        let mut handles = Vec::with_capacity(jobs.len());
        for job in jobs {
            let conn = conns
                .get(
                    usize::from(job.index)
                        .checked_rem(conns.len())
                        .ok_or(LoadError::Bound("connections"))?,
                )
                .ok_or(LoadError::Bound("connections"))?;
            let runtime = &runtime;
            let handle = std::thread::Builder::new()
                .name(format!("focal-load-{}", job.index))
                .spawn_scoped(scope, move || {
                    worker(job, phase, run_start, generations, runtime, conn)
                })?;
            handles.push(handle);
        }
        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            outcomes.push(
                handle
                    .join()
                    .map_err(|_| LoadError::Worker("a worker thread ended without a result"))??,
            );
        }
        Ok(outcomes)
    })?;
    Ok((outcomes, run_start.elapsed().as_nanos(), run_start_epoch_ns))
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

pub fn run(shape: WorkloadShape) -> Result<Report, LoadError> {
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
            (
                Connector::Enrolled {
                    tls: client.tls,
                    initial: client.initial,
                },
                names,
                None,
            )
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
            callers: shape.concurrency,
            count: share(shape.claims, shape.concurrency, index)?,
            base: worker_base(shape.seed, u128::from(index))?,
            connector: connector.clone(),
            names,
            limits: limits.clone(),
            created: Vec::new(),
            pace,
        });
    }
    // One journal's generations for every caller, as N processes of one
    // participant share one (the audit's F12).
    let generations = Generations::new();
    let (write_outcomes, write_nanos, write_phase_start_epoch_ns) =
        run_phase(jobs, Phase::Write, &generations, connections)?;
    let (floors_advanced, expired) = generations.counts()?;
    let mut writes = merged(&write_outcomes);
    let (committed, refused, unknown) = (writes.committed, writes.refused, writes.unknown);
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
        for reason in &outcome.refusals {
            note(&mut refusals, reason.clone());
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
                callers: reader_count,
                count: share(shape.reads, reader_count, index)?,
                base: worker_base(shape.seed, u128::from(index))?,
                connector: connector.clone(),
                names,
                limits: limits.clone(),
                created: outcome.created.clone(),
                // Reads are not paced: they measure the read path closed-loop.
                pace: None,
            });
        }
        let (read_outcomes, read_nanos, _) = run_phase(
            jobs,
            Phase::Read,
            &generations,
            reader_count.min(connections),
        )?;
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
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let probe = Connector::Embedded(hosted.host.clone());
                let conn = Conn::open(&probe, &names, &limits)?;
                let request = worker_base(shape.seed, PROBE_WORKER)?
                    .checked_add(READ_OFFSET)
                    .ok_or(LoadError::Bound("probe id space"))?;
                let envelope = read_envelope(&names, request, claim);
                let started = Instant::now();
                let result = runtime.block_on(conn.request(envelope));
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
