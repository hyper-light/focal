//! Bounded egress from the session owner to authenticated peer connections.
use crate::control_host::{ControlReplicationFrame, DirectoryReplication};
use crate::fleet::{FleetReplication, ReplicationFrame};
use focal_wire::{PeerConnectionPool, PeerSendError};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::panic::AssertUnwindSafe;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicationReport {
    pub attempted: u64,
    /// Accepted by remote ingress; this is never a Raft/quorum acknowledgment.
    pub accepted: u64,
    pub lost: u64,
    pub saturated: u64,
    pub peak_inflight: usize,
}
#[derive(Debug, thiserror::Error)]
pub enum ReplicationDriverError {
    #[error("replication driver concurrency must be between one and 1024")]
    Configuration,
    #[error("replication send task failed")]
    Worker,
}

/// Drain the host's bounded channel with at most `max_inflight` send futures.
/// Full task capacity pauses receiving, leaving the host's existing queue/drop
/// policy in control. There is no second queue and no task spawning.
/// Closing the receiver's sender drains accepted frames; cancelling this future
/// drops the owned futures and releases their frame allocations immediately.
/// The pool is borrowed; concurrent sends need no additional Arc or spawned task.
pub async fn drive_replication(
    receiver: mpsc::Receiver<ReplicationFrame>,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Single(receiver), pool, max_inflight).await
}

/// The grouped receiver retains its channel accounting through all outstanding
/// sends, including after every logical session has stopped. The pool remains
/// borrowed; this adapter adds no task, queue, or ownership wrapper.
pub async fn drive_fleet_replication(
    receiver: FleetReplication,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Fleet(receiver), pool, max_inflight).await
}

pub async fn drive_control_replication(
    receiver: mpsc::Receiver<ControlReplicationFrame>,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Control(receiver), pool, max_inflight).await
}

/// Directory startup retains its fixed channel allowance through this owned
/// receiver and all outstanding sends, without an extra shared wrapper.
pub async fn drive_directory_replication(
    receiver: DirectoryReplication,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    drive(Receiver::Directory(receiver), pool, max_inflight).await
}

enum Frame {
    Session(ReplicationFrame),
    Control(ControlReplicationFrame),
}
impl Frame {
    fn target(&self) -> u64 {
        match self {
            Self::Session(frame) => frame.target,
            Self::Control(frame) => frame.target,
        }
    }
    fn complete(&mut self, accepted: bool) {
        match self {
            Self::Session(frame) => frame.report_snapshot(accepted),
            Self::Control(frame) => frame.report_snapshot(accepted),
        }
    }
    fn request(&self) -> &focal_wire::RequestEnvelope {
        match self {
            Self::Session(frame) => &frame.request,
            Self::Control(frame) => &frame.request,
        }
    }
}

enum Receiver {
    Single(mpsc::Receiver<ReplicationFrame>),
    Fleet(FleetReplication),
    Control(mpsc::Receiver<ControlReplicationFrame>),
    Directory(DirectoryReplication),
}
impl Receiver {
    async fn recv(&mut self) -> Option<Frame> {
        match self {
            Self::Single(receiver) => receiver.recv().await.map(Frame::Session),
            Self::Fleet(receiver) => receiver.recv().await.map(Frame::Session),
            Self::Control(receiver) => receiver.recv().await.map(Frame::Control),
            Self::Directory(receiver) => receiver.recv().await.map(Frame::Control),
        }
    }
}

async fn drive(
    mut receiver: Receiver,
    pool: &PeerConnectionPool,
    max_inflight: usize,
) -> Result<ReplicationReport, ReplicationDriverError> {
    if !(1..=1024).contains(&max_inflight) {
        return Err(ReplicationDriverError::Configuration);
    }
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(ReplicationDriverError::Worker);
    }
    let mut tasks = FuturesUnordered::new();
    let mut receiving = true;
    let mut report = ReplicationReport::default();
    while receiving || !tasks.is_empty() {
        tokio::select! {
            frame = receiver.recv(), if receiving && tasks.len() < max_inflight => {
                if let Some(mut frame) = frame {
                    report.attempted = report.attempted.saturating_add(1);
                    tasks.push(AssertUnwindSafe(async move {
                        let result = pool.send(frame.target(), frame.request()).await;
                        frame.complete(result.is_ok());
                        // Keep the whole frame, especially its Allocation, alive
                        // across connection setup, retries and response receipt.
                        drop(frame);
                        result
                    }).catch_unwind());
                    report.peak_inflight = report.peak_inflight.max(tasks.len());
                } else { receiving = false; }
            }
            completed = tasks.next(), if !tasks.is_empty() => {
                match completed.ok_or(ReplicationDriverError::Worker)?.map_err(|_| ReplicationDriverError::Worker)? {
                    Ok(()) => report.accepted = report.accepted.saturating_add(1),
                    Err(PeerSendError::Busy) => report.saturated = report.saturated.saturating_add(1),
                    Err(_) => report.lost = report.lost.saturating_add(1),
                }
            }
        }
    }
    Ok(report)
}
