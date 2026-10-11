//! Foreground process transport. Each blocking resource has one owned thread.
use crate::{
    Action, Backend, CallToken, EncodedFrame, FrameDecoder, InputFrame, Limits, Protocol,
    ProtocolError, ServerInfo, ToolCall,
};
use focal_client::{
    ClientTransport,
    operations::{ApplicationResult, OperationOutput},
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_rt::sync::{ChannelReceiver, OneshotSender, Sender};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    sync::mpsc::{self, Receiver, SyncSender, TrySendError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const MIB: usize = 1024 * 1024;
/// The client frames that wait while a cancelled call ends: as many as the
/// input owner's channel to the coordinator holds (`serve`).
const HELD_INPUTS: usize = 8;
/// The input and output owners' stack: one blocking read or write and the frame codec.
const THREAD_STACK: usize = MIB;
/// The ledger worker's stack. It runs a hyper-rt shard whose root task makes each call: the call's
/// poll chain, the backend's waits and the synchronous work between them (a journal, the native
/// compiler beneath the driver's waits), and code mode's engine, bounded at half of it
/// ([`crate::code::CodeLimits::for_adapter`]). The deepest call of the adapter's suite measured
/// under 1.5 MiB in a debug build (the native example's compile, 2026-10-10), whose frames keep
/// every temporary; a release build's are a fraction of that.
pub(crate) const WORKER_STACK: usize = 2 * MIB;
const JOB_BYTES: usize = 32 * MIB;
const SHUTDOWN: Duration = Duration::from_secs(3);

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Memory(#[from] focal_memory::MemoryError),
    #[error("MCP transport I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("MCP output consumer exceeded bounded backpressure")]
    Backpressure,
    #[error("MCP transport owner stopped unexpectedly")]
    Owner,
    #[error("MCP transport shutdown deadline exceeded; saved operations remain recoverable")]
    Shutdown,
    #[error("MCP engine probe failed before any tool was served: {0}")]
    Probe(String),
    #[error("MCP foreground startup requires a blocking process owner, not a runtime's task")]
    Nested,
}

struct Job {
    call: ToolCall,
    cancel: crate::Cancel,
    allocation: Allocation,
}
/// What the worker hands the coordinator once its shard probed the engine and built the
/// catalogue: what the protocol serves, and the construction admission it releases once the
/// protocol holds its own charge.
struct Started {
    tools: Vec<crate::Tool>,
    skills: crate::skills::Skills,
    admission: Allocation,
}
struct Completion {
    call: ToolCall,
    result: ApplicationResult,
    _allocation: Allocation,
}
enum Event {
    Input(InputFrame),
    InputEnd,
    Complete(Box<Completion>),
    WorkerEnd,
    WriterEnd,
    Failed(ServeError),
}

/// Serve one stdio MCP connection. This is a foreground-process boundary:
/// return means the host must exit, releasing any OS I/O still blocked at the
/// bounded shutdown deadline. It is not a reusable embedded thread pool.
pub fn serve<
    T: ClientTransport + Send + 'static,
    R: Read + Send + 'static,
    W: Write + Send + 'static,
>(
    backend: Backend<T>,
    reader: R,
    writer: W,
) -> Result<(), ServeError> {
    // The worker owns a shard of its own; this thread waits on its owners and must not be one.
    if hyper_rt::futures::shard_id().is_some() {
        return Err(ServeError::Nested);
    }
    let Adapter {
        limits,
        code_limits,
        budget,
    } = Adapter::new()?;
    let (events, receive) = mpsc::sync_channel(HELD_INPUTS);
    let (jobs, work) = hyper_rt::sync::channel(1).map_err(|_| ServeError::Owner)?;
    let (ready, started) = mpsc::sync_channel(1);
    let worker_budget = budget.clone();
    // The worker's shard probes the engine and builds the catalogue before anything is served: a
    // remote transport binds its endpoint to the shard that first drives it, and every later call
    // is driven there.
    let worker = owner(
        "focal-mcp-ledger",
        WORKER_STACK,
        &budget,
        events.clone(),
        move |events| worker(backend, worker_budget, code_limits, work, events, ready),
    )?;
    let Started {
        tools,
        skills,
        admission: catalog_admission,
    } = match started.recv() {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            // The worker ends as it reports: nothing else was started.
            let _ = worker.join();
            return Err(error);
        }
        Err(_) => return Err(ServeError::Owner),
    };
    let protocol = Protocol::new(
        limits,
        budget.clone(),
        ServerInfo {
            name: "focal".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        tools,
    )?;
    let mut protocol = protocol.with_skills(skills)?;
    drop(catalog_admission);
    let decoder = FrameDecoder::new(limits.max_frame_bytes, budget.clone())?;
    // Includes bounded channel slots, synchronization bookkeeping, owner
    // handles, fixed input scratch, and the coordinator's small state.
    let _queues = budget
        .reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024)?
        .commit();
    let (frames, output) = mpsc::sync_channel(2);
    let writer = owner(
        "focal-mcp-output",
        THREAD_STACK,
        &budget,
        events.clone(),
        move |events| write_output(writer, output, events),
    )?;
    let input = owner(
        "focal-mcp-input",
        THREAD_STACK,
        &budget,
        events.clone(),
        move |events| read_input(reader, decoder, events),
    )?;
    drop(events);
    let mut jobs = Some(jobs);
    let mut frames = Some(frames);
    let mut active: Option<(CallToken, OneshotSender<()>)> = None;
    let result = coordinate(
        &mut protocol,
        &budget,
        &receive,
        &mut jobs,
        &mut frames,
        &mut active,
    );
    if let Some((_, cancel)) = active.take() {
        let _ = cancel.send(());
    }
    drop(jobs);
    drop(frames);
    // Never let joining an OS read/write or filesystem stall defeat the process
    // shutdown bound. Normal EOF has already received both owner-end events.
    for handle in [input, worker, writer] {
        if handle.is_finished() && handle.join().is_err() && result.is_ok() {
            return Err(ServeError::Owner);
        }
    }
    result
}

/// One client frame: answered, started as the worker's call, or a cancel sent
/// to the running call, whose token is returned: the frames after it wait
/// until that call has ended (`coordinate`).
fn input(
    frame: InputFrame,
    protocol: &mut Protocol,
    budget: &MemoryBudget,
    jobs: &Option<Sender<Job>>,
    frames: &mut Option<SyncSender<EncodedFrame>>,
    active: &mut Option<(CallToken, OneshotSender<()>)>,
) -> Result<Option<CallToken>, ServeError> {
    match protocol.receive(frame.as_bytes())? {
        Action::Reply(frame) => send_frame(frames, frame)?,
        Action::Call(call) => {
            if active.is_some() {
                return Err(ServeError::Owner);
            }
            let allocation =
                match budget.reserve(BudgetKind::Control, BudgetLane::Ordinary, JOB_BYTES) {
                    Ok(reservation) => reservation.commit(),
                    Err(_) => {
                        if let Some(frame) = protocol
                            .fail(call.token, "Operation capacity exhausted; retry later")?
                        {
                            send_frame(frames, frame)?;
                        }
                        return Ok(None);
                    }
                };
            let (cancel, cancelled) = hyper_rt::sync::oneshot().map_err(|_| ServeError::Owner)?;
            let token = call.token;
            // The worker still finishing a cancelled call holds the
            // queue's place: this call is refused, typed, to retry.
            match jobs.as_ref().ok_or(ServeError::Owner)?.try_send(Job {
                call,
                cancel: cancelled,
                allocation,
            }) {
                Ok(()) => *active = Some((token, cancel)),
                Err(hyper_rt::sync::SyncError::Full(_)) => {
                    if let Some(frame) =
                        protocol.fail(token, "Operation capacity exhausted; retry later")?
                    {
                        send_frame(frames, frame)?;
                    }
                }
                Err(_) => return Err(ServeError::Owner),
            }
        }
        Action::Cancel(token) => {
            if active
                .as_ref()
                .is_some_and(|(current, _)| *current == token)
                && let Some((_, cancel)) = active.take()
            {
                let _ = cancel.send(());
                return Ok(Some(token));
            }
        }
        Action::NoReply => {}
    }
    Ok(None)
}

fn coordinate(
    protocol: &mut Protocol,
    budget: &MemoryBudget,
    events: &Receiver<Event>,
    jobs: &mut Option<Sender<Job>>,
    frames: &mut Option<SyncSender<EncodedFrame>>,
    active: &mut Option<(CallToken, OneshotSender<()>)>,
) -> Result<(), ServeError> {
    let mut ended = None;
    let mut worker_ended = false;
    let mut writer_ended = false;
    // The cancelled call being torn down, and the frames waiting for it.
    let mut draining: Option<CallToken> = None;
    let mut held: VecDeque<InputFrame> = VecDeque::new();
    loop {
        if ended.is_some_and(|start: Instant| start.elapsed() >= SHUTDOWN) {
            return Err(ServeError::Shutdown);
        }
        if ended.is_some() && worker_ended && writer_ended {
            return Ok(());
        }
        let event = match events.recv_timeout(Duration::from_millis(20)) {
            Ok(event) => event,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err(ServeError::Owner),
        };
        match event {
            // A frame after a cancellation waits until the cancelled call has
            // ended and given its slot back, so a call that follows a cancel
            // finds it free instead of being refused while the cancel is
            // still on its way (the MCP claim-wait cancellation test on macOS
            // CI). As many wait as the input channel holds; past that a frame
            // is taken at once.
            Event::Input(frame) => {
                if draining.is_some() && held.len() < HELD_INPUTS {
                    held.push_back(frame);
                } else if let Some(token) = input(frame, protocol, budget, jobs, frames, active)? {
                    draining = Some(token);
                }
            }
            Event::Complete(completion) => {
                completion
                    .result
                    .validate_metadata()
                    .map_err(|_| ProtocolError::Encode)?;
                if active
                    .as_ref()
                    .is_some_and(|(token, _)| *token == completion.call.token)
                {
                    active.take();
                }
                // Inspecting a saved pending identity succeeded. Pending is
                // still explicit in the structured result and implies no
                // durable acceptance; it is an error only for a submit/retry.
                let inspected_pending = completion.call.tool == "request.inspect"
                    && matches!(
                        &completion.result.result,
                        OperationOutput::Mutation {
                            reply: focal_wire::MutationReply::Pending(_)
                        }
                    );
                if ended.is_none()
                    && let Some(frame) = protocol.complete(
                        completion.call.token,
                        &completion.result,
                        completion.result.is_error() && !inspected_pending,
                    )?
                {
                    send_frame(frames, frame)?;
                }
                // The cancelled call has ended: the frames that waited for it
                // are taken in order, until one cancels a call again.
                if draining == Some(completion.call.token) {
                    draining = None;
                    while draining.is_none()
                        && let Some(frame) = held.pop_front()
                    {
                        draining = input(frame, protocol, budget, jobs, frames, active)?;
                    }
                }
            }
            Event::InputEnd => {
                // What waited for a cancelled call is taken as it stands: the
                // client is gone, and its last frames are answered first.
                draining = None;
                while let Some(frame) = held.pop_front() {
                    input(frame, protocol, budget, jobs, frames, active)?;
                }
                ended = Some(Instant::now());
                if let Some((_, cancel)) = active.take() {
                    let _ = cancel.send(());
                }
                jobs.take();
                frames.take();
            }
            Event::WorkerEnd => {
                if ended.is_none() {
                    return Err(ServeError::Owner);
                }
                worker_ended = true;
            }
            Event::WriterEnd => {
                if ended.is_none() {
                    return Err(ServeError::Owner);
                }
                writer_ended = true;
            }
            Event::Failed(error) => return Err(error),
        }
    }
}
/// The adapter's bounds and budget, the same for a served connection and for
/// one program run from the CLI.
struct Adapter {
    limits: Limits,
    code_limits: crate::code::CodeLimits,
    budget: MemoryBudget,
}
impl Adapter {
    fn new() -> Result<Self, ServeError> {
        let limits = Limits {
            max_frame_bytes: 278_528,
            max_response_bytes: 16 * MIB,
            max_active_calls: 1,
            ..Limits::default()
        };
        // One code-mode run at a time (the worker is one owner): its sandbox
        // and the text it hands across are added to the budget. Of the rest,
        // 80 MiB of ordinary headroom: the catalogue's construction admission
        // (half a mebibyte per tool) and its measured resident tree (about
        // 7.5 MiB for the 47-tool native catalogue) are held together before
        // the admission is released, beside the frame decoder and journals.
        let code_limits = crate::code::CodeLimits::for_adapter(limits, WORKER_STACK)?;
        let budget = MemoryBudget::new(
            (160 * MIB)
                .checked_add(code_limits.reservation()?)
                .ok_or(ProtocolError::Capacity)?,
            80 * MIB,
        )?;
        Ok(Self {
            limits,
            code_limits,
            budget,
        })
    }
}

/// The served tools, what code mode may call among them, and their charges:
/// the construction admission (released once the protocol holds its own)
/// and the registry's, held for the adapter's life.
struct Catalogue {
    tools: Vec<crate::Tool>,
    registry: crate::code::Registry,
    skills: crate::skills::Skills,
    admission: Allocation,
    _registry: Allocation,
}
impl Catalogue {
    fn new<T: ClientTransport>(
        backend: &Backend<T>,
        budget: &MemoryBudget,
        code_limits: crate::code::CodeLimits,
    ) -> Result<Self, ServeError> {
        // Reserve before constructing serde schema trees. Protocol takes its
        // own measured resident charge before this admission is released.
        let application = match backend.native_standing() {
            Some(standing) => {
                crate::catalog_native::tool_count(standing)?.checked_add(if backend.has_watches() {
                    crate::catalog_watch::TOOL_COUNT
                } else {
                    0
                })
            }
            None => {
                focal_client::operations::application(focal_client::operations::WireProfile::V1)
                    .len()
                    .checked_add(6)
                    .and_then(|n| {
                        n.checked_add(if backend.has_uploads() {
                            crate::catalog_transfer::TOOL_COUNT
                        } else {
                            0
                        })
                    })
                    .and_then(|n| {
                        n.checked_add(if backend.has_watches() {
                            crate::catalog_watch::TOOL_COUNT
                        } else {
                            0
                        })
                    })
            }
        }
        .ok_or(ProtocolError::Capacity)?;
        let tool_count = application
            .checked_add(if backend.has_admin() {
                crate::catalog_admin::TOOL_COUNT
            } else {
                0
            })
            .and_then(|n| n.checked_add(2))
            .ok_or(ProtocolError::Capacity)?;
        // Covers each retained pruned input/output tree and one temporary
        // shared definition tree. The composition test measures the whole.
        let construction_bytes = tool_count
            .checked_add(1)
            .and_then(|n| n.checked_mul(512 * 1024))
            .ok_or(ProtocolError::Capacity)?;
        let admission = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Ordinary,
                construction_bytes,
            )?
            .commit();
        let mut tools = match backend.native_standing() {
            Some(standing) => crate::catalog_native::catalog(standing)?,
            None => crate::catalog::catalog()?,
        };
        if backend.has_admin() {
            crate::catalog_admin::append(&mut tools)?;
        }
        if !backend.has_native() && backend.has_uploads() {
            crate::catalog_transfer::append(&mut tools)?;
        }
        if backend.has_watches() {
            crate::catalog_watch::append(&mut tools)?;
        }
        // What code mode may call is what is served, less its own two tools;
        // its search sees the served skills beside them.
        let skills = crate::skills::Skills::embedded()?;
        let registry = crate::code::Registry::new(&tools, &skills)?;
        let registry_charge = budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, registry.bytes())?
            .commit();
        tools
            .try_reserve_exact(2)
            .map_err(|_| ProtocolError::Capacity)?;
        tools.extend(crate::code::tools(code_limits)?);
        Ok(Self {
            tools,
            registry,
            skills,
            admission,
            _registry: registry_charge,
        })
    }
}

/// Run one code-mode program (`code.run` or `code.search`) against the
/// backend outside a served connection: the CLI's `focal code`. The program
/// sees and calls exactly what `serve` would offer the same backend, through
/// the same journal, so a run begun over MCP resumes here and the reverse.
pub fn run_code<T: ClientTransport + 'static>(
    mut backend: Backend<T>,
    tool: &str,
    arguments: serde_json::Map<String, serde_json::Value>,
) -> Result<ApplicationResult, ServeError> {
    if !crate::code::is_code(tool) {
        return Err(ServeError::Protocol(ProtocolError::Limits));
    }
    // The run owns a shard on this thread for its life; a thread that already runs one cannot.
    if hyper_rt::futures::shard_id().is_some() {
        return Err(ServeError::Nested);
    }
    let Adapter {
        limits,
        code_limits,
        budget,
    } = Adapter::new()?;
    let tool = tool.to_owned();
    focal_rt::block_on(async move {
        backend.detect().await.map_err(ServeError::Probe)?;
        let Catalogue {
            tools: _,
            registry,
            skills: _,
            admission,
            _registry,
        } = Catalogue::new(&backend, &budget, code_limits)?;
        drop(admission);
        let allocation = budget
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Ordinary,
                limits.workspace()?,
            )?
            .commit();
        let mut call = crate::ToolCall::nested(tool, arguments, allocation);
        let (_signal, mut cancel) = hyper_rt::sync::oneshot().map_err(|_| ServeError::Owner)?;
        Ok(crate::code::execute(
            crate::code::Host {
                backend: &mut backend,
                budget: &budget,
                registry: &registry,
                limits: code_limits,
            },
            &mut call,
            &mut cancel,
        )
        .await)
    })
    .map_err(|_| ServeError::Owner)?
}

fn send_frame(
    sender: &Option<SyncSender<EncodedFrame>>,
    frame: EncodedFrame,
) -> Result<(), ServeError> {
    match sender.as_ref().ok_or(ServeError::Owner)?.try_send(frame) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ServeError::Backpressure),
        Err(TrySendError::Disconnected(_)) => Err(ServeError::Owner),
    }
}
/// A thread of the adapter's, its `stack` and bookkeeping charged to the budget for its life.
fn owner(
    name: &str,
    stack: usize,
    budget: &MemoryBudget,
    events: SyncSender<Event>,
    run: impl FnOnce(&SyncSender<Event>) -> Result<(), ServeError> + Send + 'static,
) -> Result<JoinHandle<()>, ServeError> {
    let charged = stack
        .checked_add(64 * 1024)
        .ok_or(ProtocolError::Capacity)?;
    let allocation = budget
        .reserve(BudgetKind::Control, BudgetLane::Ordinary, charged)?
        .commit();
    Ok(thread::Builder::new()
        .name(name.into())
        .stack_size(stack)
        .spawn(move || {
            let _allocation = allocation;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&events)))
                .unwrap_or(Err(ServeError::Owner));
            if let Err(error) = result {
                let _ = events.send(Event::Failed(error));
            }
        })?)
}
/// The ledger worker: one shard on its thread, whose root task probes the engine, builds the
/// catalogue and hands it to the coordinator, then serves the jobs it is given one at a time
/// until their queue closes. The shard runs between jobs, so a connection the backend holds keeps
/// its keepalives and acknowledgements while the adapter waits for a call.
fn worker<T: ClientTransport + 'static>(
    backend: Backend<T>,
    budget: MemoryBudget,
    code_limits: crate::code::CodeLimits,
    mut jobs: ChannelReceiver<Job>,
    events: &SyncSender<Event>,
    ready: SyncSender<Result<Started, ServeError>>,
) -> Result<(), ServeError> {
    let events = events.clone();
    focal_rt::block_on(async move {
        let mut backend = backend;
        if let Err(detail) = backend.detect().await {
            let _ = ready.send(Err(ServeError::Probe(detail)));
            return Ok(());
        }
        let Catalogue {
            tools,
            registry,
            skills,
            admission,
            _registry: _registry_charge,
        } = match Catalogue::new(&backend, &budget, code_limits) {
            Ok(catalogue) => catalogue,
            Err(error) => {
                let _ = ready.send(Err(error));
                return Ok(());
            }
        };
        ready
            .send(Ok(Started {
                tools,
                skills,
                admission,
            }))
            .map_err(|_| ServeError::Owner)?;
        while let Ok(mut job) = jobs.recv().await {
            let result = if crate::code::is_code(&job.call.tool) {
                crate::code::execute(
                    crate::code::Host {
                        backend: &mut backend,
                        budget: &budget,
                        registry: &registry,
                        limits: code_limits,
                    },
                    &mut job.call,
                    &mut job.cancel,
                )
                .await
            } else {
                backend.execute(&mut job.call, &mut job.cancel).await
            };
            events
                .send(Event::Complete(Box::new(Completion {
                    call: job.call,
                    result,
                    _allocation: job.allocation,
                })))
                .map_err(|_| ServeError::Owner)?;
        }
        events.send(Event::WorkerEnd).map_err(|_| ServeError::Owner)
    })
    .map_err(|_| ServeError::Owner)?
}
fn read_input(
    mut reader: impl Read,
    mut decoder: FrameDecoder,
    events: &SyncSender<Event>,
) -> Result<(), ServeError> {
    let mut scratch = [0; 8192];
    loop {
        let read = match reader.read(&mut scratch) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if read == 0 {
            decoder.finish()?;
            events
                .send(Event::InputEnd)
                .map_err(|_| ServeError::Owner)?;
            return Ok(());
        }
        let mut remaining = scratch.get(..read).ok_or(ServeError::Owner)?;
        while !remaining.is_empty() {
            let (consumed, frame) = decoder.push(remaining)?;
            if consumed == 0 {
                return Err(ServeError::Owner);
            }
            remaining = remaining.get(consumed..).ok_or(ServeError::Owner)?;
            if let Some(frame) = frame {
                events
                    .send(Event::Input(frame))
                    .map_err(|_| ServeError::Owner)?;
            }
        }
    }
}
fn write_output(
    mut writer: impl Write,
    frames: Receiver<EncodedFrame>,
    events: &SyncSender<Event>,
) -> Result<(), ServeError> {
    while let Ok(frame) = frames.recv() {
        writer.write_all(frame.as_bytes())?;
        writer.flush()?;
    }
    events.send(Event::WriterEnd).map_err(|_| ServeError::Owner)
}
