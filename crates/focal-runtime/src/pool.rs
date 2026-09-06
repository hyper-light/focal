use crate::*;
use focal_memory::Allocation;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread,
};

pub(crate) struct Work {
    pub task: Task,
    pub cancellation: Cancellation,
    pub charge: Allocation,
}
pub(crate) struct Finished {
    pub id: ArtifactId,
    pub outcome: Option<WorkerOutcome>,
    pub completed_at: u64,
    pub reconciled: bool,
    pub _charge: Allocation,
}
pub(crate) struct Pool {
    senders: Vec<SyncSender<Work>>,
    receiver: Receiver<Finished>,
    handles: Vec<thread::JoinHandle<()>>,
}
impl Pool {
    pub fn new(
        config: &RuntimeConfig,
        executor: Arc<dyn Executor>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, RuntimeError> {
        // Each worker owns its receiver. No shared receiver mutex, poisoning, or
        // shared queue lifetime is needed; Runtime bounds total outstanding work.
        let mut senders = Vec::new();
        let (finished, done) = mpsc::sync_channel(config.max_inflight);
        let mut handles = Vec::new();
        for index in 0..config.workers {
            let (sender, receiver) = mpsc::sync_channel::<Work>(1);
            let executor = Arc::clone(&executor);
            let clock = Arc::clone(&clock);
            let finished = finished.clone();
            let max_reason = config.max_reason_bytes;
            let handle = thread::Builder::new()
                .name(format!("focal-validator-{index}"))
                .spawn(move || {
                    loop {
                        let work = receiver.recv();
                        let Ok(work) = work else {
                            return;
                        };
                        let external = work.task.assignment.policy.as_ref().is_some_and(|p| p.retry == RetryContract::Reconcile);
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            if work.cancellation.is_cancelled() { return (None, false); }
                            if external {
                                // Query with the logical run/attempt identity even
                                // on the first local assignment or after adoption.
                                match executor.reconcile(&work.task, &work.cancellation) {
                                    Reconciliation::Completed(outcome) => return (Some(outcome), true),
                                    Reconciliation::Indeterminate => return (None, false),
                                    Reconciliation::NotStarted if clock.now_ms() >= work.task.assignment.deadline => return (Some(WorkerOutcome::error(DiagnosticCode::TimedOut, "reconciliation proved no effect before the expired attempt")), true),
                                    Reconciliation::NotStarted => {}
                                }
                            }
                            (Some(executor.execute(&work.task, &work.cancellation)), false)
                        }));
                        let (mut outcome, reconciled) = result.unwrap_or_else(|_| {
                            if external { (None, false) } else { (Some(WorkerOutcome::error(DiagnosticCode::Panicked, "handler panicked")), false) }
                        });
                        if let Some(result) = &mut outcome {
                            let mut length = result.reason.len().min(max_reason);
                            while !result.reason.is_char_boundary(length) {
                                length = length.saturating_sub(1);
                            }
                            result.reason.truncate(length);
                        }
                        if finished
                            .send(Finished {
                                id: work.task.id,
                                outcome,
                                completed_at: clock.now_ms(),
                                reconciled,
                                _charge: work.charge,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                })
                .map_err(|_| RuntimeError::Capacity)?;
            senders.push(sender);
            handles.push(handle);
        }
        Ok(Self {
            senders,
            receiver: done,
            handles,
        })
    }
    pub fn submit(&self, mut work: Work) -> Result<(), RuntimeError> {
        let mut live = false;
        for sender in &self.senders {
            match sender.try_send(work) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => {
                    work = returned;
                    live = true;
                }
                Err(TrySendError::Disconnected(returned)) => work = returned,
            }
        }
        Err(if live {
            RuntimeError::Capacity
        } else {
            RuntimeError::Stopped
        })
    }
    pub fn receive(&self) -> Result<Option<Finished>, RuntimeError> {
        match self.receiver.try_recv() {
            Ok(value) => Ok(Some(value)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(RuntimeError::Stopped),
        }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.senders.clear();
        // Rust cannot forcibly preempt a synchronous callback. Join only workers
        // already finished; running workers retain their bounded allocation and
        // exit when their callback returns and the queue/sender closes.
        for handle in self.handles.drain(..) {
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}
