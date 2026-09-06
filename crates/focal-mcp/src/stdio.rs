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
use std::{
    io::{Read, Write},
    sync::mpsc::{self, Receiver, SyncSender, TrySendError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const MIB: usize = 1024 * 1024;
const THREAD_STACK: usize = MIB;
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
}

struct Job {
    call: ToolCall,
    cancel: oneshot::Receiver<()>,
    allocation: Allocation,
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
pub fn serve<T: ClientTransport + 'static, R: Read + Send + 'static, W: Write + Send + 'static>(
    backend: Backend<T>,
    reader: R,
    writer: W,
) -> Result<(), ServeError> {
    let budget = MemoryBudget::new(128 * MIB, 80 * MIB)?;
    let limits = Limits {
        max_frame_bytes: 278_528,
        max_response_bytes: 16 * MIB,
        max_active_calls: 1,
        ..Limits::default()
    };
    // Reserve before constructing serde schema trees. Protocol takes its own
    // measured resident charge before this temporary admission is released.
    let catalog_admission = budget
        .reserve(BudgetKind::Control, BudgetLane::Ordinary, 4 * MIB)?
        .commit();
    let mut protocol = Protocol::new(
        limits,
        budget.clone(),
        ServerInfo {
            name: "focal".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
        crate::catalog::catalog()?,
    )?;
    drop(catalog_admission);
    let decoder = FrameDecoder::new(limits.max_frame_bytes, budget.clone())?;
    // Includes bounded channel slots, synchronization bookkeeping, owner
    // handles, fixed input scratch, and the coordinator's small state.
    let _queues = budget
        .reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024)?
        .commit();
    let (events, receive) = mpsc::sync_channel(8);
    let (jobs, work) = mpsc::sync_channel(1);
    let (frames, output) = mpsc::sync_channel(2);
    let worker = owner("focal-mcp-ledger", &budget, events.clone(), move |events| {
        worker(backend, work, events)
    })?;
    let writer = owner("focal-mcp-output", &budget, events.clone(), move |events| {
        write_output(writer, output, events)
    })?;
    let input = owner("focal-mcp-input", &budget, events.clone(), move |events| {
        read_input(reader, decoder, events)
    })?;
    drop(events);
    let mut jobs = Some(jobs);
    let mut frames = Some(frames);
    let mut active: Option<(CallToken, oneshot::Sender<()>)> = None;
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

fn coordinate(
    protocol: &mut Protocol,
    budget: &MemoryBudget,
    events: &Receiver<Event>,
    jobs: &mut Option<SyncSender<Job>>,
    frames: &mut Option<SyncSender<EncodedFrame>>,
    active: &mut Option<(CallToken, oneshot::Sender<()>)>,
) -> Result<(), ServeError> {
    let mut ended = None;
    let mut worker_ended = false;
    let mut writer_ended = false;
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
            Event::Input(frame) => match protocol.receive(frame.as_bytes())? {
                Action::Reply(frame) => send_frame(frames, frame)?,
                Action::Call(call) => {
                    if active.is_some() {
                        return Err(ServeError::Owner);
                    }
                    let allocation = match budget.reserve(
                        BudgetKind::Control,
                        BudgetLane::Ordinary,
                        JOB_BYTES,
                    ) {
                        Ok(reservation) => reservation.commit(),
                        Err(_) => {
                            if let Some(frame) = protocol
                                .fail(call.token, "Operation capacity exhausted; retry later")?
                            {
                                send_frame(frames, frame)?;
                            }
                            continue;
                        }
                    };
                    let (cancel, cancelled) = oneshot::channel();
                    let token = call.token;
                    jobs.as_ref()
                        .ok_or(ServeError::Owner)?
                        .try_send(Job {
                            call,
                            cancel: cancelled,
                            allocation,
                        })
                        .map_err(|_| ServeError::Owner)?;
                    *active = Some((token, cancel));
                }
                Action::Cancel(token) => {
                    if active
                        .as_ref()
                        .is_some_and(|(current, _)| *current == token)
                        && let Some((_, cancel)) = active.take()
                    {
                        let _ = cancel.send(());
                    }
                }
                Action::NoReply => {}
            },
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
            }
            Event::InputEnd => {
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
fn owner(
    name: &str,
    budget: &MemoryBudget,
    events: SyncSender<Event>,
    run: impl FnOnce(&SyncSender<Event>) -> Result<(), ServeError> + Send + 'static,
) -> Result<JoinHandle<()>, ServeError> {
    let allocation = budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Ordinary,
            THREAD_STACK + 64 * 1024,
        )?
        .commit();
    Ok(thread::Builder::new()
        .name(name.into())
        .stack_size(THREAD_STACK)
        .spawn(move || {
            let _allocation = allocation;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&events)))
                .unwrap_or(Err(ServeError::Owner));
            if let Err(error) = result {
                let _ = events.send(Event::Failed(error));
            }
        })?)
}
fn worker<T: ClientTransport>(
    mut backend: Backend<T>,
    jobs: Receiver<Job>,
    events: &SyncSender<Event>,
) -> Result<(), ServeError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    while let Ok(mut job) = jobs.recv() {
        let result = backend.execute(&runtime, &mut job.call, &mut job.cancel);
        events
            .send(Event::Complete(Box::new(Completion {
                call: job.call,
                result,
                _allocation: job.allocation,
            })))
            .map_err(|_| ServeError::Owner)?;
    }
    runtime.shutdown_background();
    events.send(Event::WorkerEnd).map_err(|_| ServeError::Owner)
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
