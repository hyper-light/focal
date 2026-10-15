//! The node log's growth, admitted against the node's disk (27 §15.11): the `hyper_log::Growth`
//! a node's start hands its log, over the `DiskBudget` every durable writer of its volume draws
//! from. The log asks before a frame opens a slot past the file's end; a refusal reads as the
//! file's bound reached (`Full`: groups compact, sweeps run), never as a fence.
//!
//! Each admission is a `DiskKind::Wal` reservation on the completion lane: the log carries every
//! group's writes, a completion's among them, so its room is the floor completions stand on, and
//! fresh ordinary work is refused upstream, at the native session's headroom
//! (`NativeSessionLimits::disk_headroom_bytes`). The volume is sampled when the budget says a
//! sample is due. Admitted slots are committed when the log says they are durable and released
//! when it says they never will be; a part of one is split off when the log finishes less than a
//! whole admission.
use std::collections::VecDeque;
use std::path::PathBuf;

use focal_memory::{BudgetLane, DiskBudget, DiskKind, DiskReservation, MemoryError};

/// Admissions outstanding at once, at most: the log keeps two slots admitted ahead of need
/// (hyper-log `growth::AHEAD`) and a new log admits its persist area and first slot once beside
/// them. One past it is refused, read as the bound reached.
const MOST_ADMITTED: usize = 3;

/// The node log's admission to grow, over the node's disk.
pub struct DiskGrowth {
    disk: DiskBudget,
    /// A directory on the log's volume, sampled for its free space.
    volume: PathBuf,
    /// How the volume's free space is read: the platform's, but where a test states it.
    probe: fn(&std::path::Path) -> Option<u64>,
    /// What was admitted and is not yet durable, oldest first.
    admitted: VecDeque<DiskReservation>,
    /// What the file took when it was opened, as the log told it: on the volume already, so in
    /// its free space, and never reserved again.
    held: u64,
}

impl std::fmt::Debug for DiskGrowth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskGrowth")
            .field("volume", &self.volume)
            .field("admitted", &self.admitted.len())
            .field("held", &self.held)
            .finish()
    }
}

impl DiskGrowth {
    /// The admission over `disk` for a log whose file lives on the volume of `volume`.
    pub fn new(disk: DiskBudget, volume: PathBuf) -> Result<Self, MemoryError> {
        Self::with_probe(disk, volume, focal_platform::available_space)
    }

    /// As [`DiskGrowth::new`], the volume's free space read by `probe`.
    pub fn with_probe(
        disk: DiskBudget,
        volume: PathBuf,
        probe: fn(&std::path::Path) -> Option<u64>,
    ) -> Result<Self, MemoryError> {
        let mut admitted = VecDeque::new();
        admitted
            .try_reserve_exact(MOST_ADMITTED)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok(Self {
            disk,
            volume,
            probe,
            admitted,
            held: 0,
        })
    }

    /// What the file took when it was opened.
    pub fn held_bytes(&self) -> u64 {
        self.held
    }

    /// Bytes admitted and not yet durable.
    pub fn admitted_bytes(&self) -> u64 {
        self.admitted.iter().fold(0u64, |sum, reservation| {
            sum.saturating_add(reservation.bytes())
        })
    }

    /// Finishes `bytes` of what was admitted, oldest first: committed to the volume, or given
    /// back. A part of an admission is split off; bytes past what was admitted are none of this
    /// gate's, and nothing is done for them.
    fn finish(&mut self, mut bytes: u64, durable: bool) {
        while bytes > 0 {
            let Some(oldest) = self.admitted.front_mut() else {
                return;
            };
            let part = if oldest.bytes() <= bytes {
                self.admitted.pop_front()
            } else {
                oldest.split_off(bytes).ok()
            };
            let Some(part) = part else {
                return;
            };
            bytes = bytes.saturating_sub(part.bytes());
            if durable {
                part.commit();
            } else {
                drop(part);
            }
        }
    }
}

impl hyper_log::Growth for DiskGrowth {
    fn held(&mut self, bytes: u64) {
        self.held = bytes;
    }

    fn admit(&mut self, bytes: u64) -> bool {
        if self.admitted.len() >= MOST_ADMITTED {
            return false;
        }
        let (volume, probe) = (&self.volume, self.probe);
        self.disk.refresh_with(|| probe(volume));
        match self
            .disk
            .reserve(DiskKind::Wal, BudgetLane::Completion, bytes)
        {
            Ok(reservation) => {
                self.admitted.push_back(reservation);
                true
            }
            Err(_) => false,
        }
    }

    fn commit(&mut self, bytes: u64) {
        self.finish(bytes, true);
    }

    fn release(&mut self, bytes: u64) {
        self.finish(bytes, false);
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
#[path = "disk_growth_tests.rs"]
mod tests;
