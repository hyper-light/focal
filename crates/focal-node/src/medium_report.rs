//! What a node says of the medium under its data directory (`focal_platform::medium`) and of its
//! log's write path on it: the storage view's `medium` and `write_path` (`inspect node
//! --storage`), and the one line a node writes when its flushes cannot be taken to reach stable
//! media (memory, another host's storage, a USB attachment; mantle
//! `docs/research/29-device-classes.md` §1, findings 4 and 5): what it writes is then as durable as
//! its replicas make it, and an operator who meant otherwise should know at once.
use std::io::Write;

use focal_client::admin::{AdminLogCounts, AdminMedium, AdminWritePath};
use focal_consensus::LogMetrics;
use focal_consensus::storage_open::WritePath;
use focal_platform::medium::Medium;

/// Writes the node's line about its medium once its description is known, where its flushes
/// cannot be taken to reach stable media; nothing otherwise.
pub(crate) fn announce(medium: &Medium) {
    if medium.flushes_reach_stable_media() {
        return;
    }
    let _ = writeln!(
        std::io::stderr().lock(),
        "focal: the data directory is on {} ({}, {}): a flush may not reach stable media there, \
         so what this node writes is as durable as its replicas make it",
        medium.class.as_str(),
        medium.interconnect.as_str(),
        medium
            .file_system_name
            .as_deref()
            .unwrap_or(medium.file_system.as_str()),
    );
}

/// The storage view's `medium`.
pub(crate) fn admin_medium(medium: &Medium) -> AdminMedium {
    AdminMedium {
        class: medium.class.as_str().to_owned(),
        interconnect: medium.interconnect.as_str().to_owned(),
        rotational: medium.rotational,
        file_system: medium.file_system.as_str().to_owned(),
        file_system_name: medium.file_system_name.clone(),
        logical_block: medium.logical_block,
        physical_block: medium.physical_block,
        preferred_write: medium.preferred_write,
        optimal_io: medium.optimal_io,
        write_cache: medium.write_cache.as_str().to_owned(),
        fua: medium.fua,
        discard: medium.discard,
        removable: medium.removable,
        device: medium.device.clone(),
        model: medium.model.clone(),
        notes: medium.notes.clone(),
        flushes_reach_stable_media: medium.flushes_reach_stable_media(),
    }
}

/// The storage view's `write_path`: what the log's file took, and the log's counts as its owner
/// answered them for this view.
pub(crate) fn admin_write_path(path: WritePath, log: Option<&LogMetrics>) -> AdminWritePath {
    AdminWritePath {
        direct: path.direct,
        layout_block: u64::try_from(path.layout_block).unwrap_or(u64::MAX),
        zero_fill: path.zero_fill,
        written_in_place: path.written_in_place,
        counts: log.map(|log| AdminLogCounts {
            frames: log.frames,
            updates: log.updates,
            bytes: log.bytes,
            flushes: log.flushes,
            durable_writes: log.durable_writes,
            durable_fallbacks: log.durable_fallbacks,
            flushed_confirmations: log.flushed_confirmations,
            zeroed_bytes: log.zeroed_bytes,
            swept_bytes: log.swept_bytes,
        }),
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
#[path = "medium_report_tests.rs"]
mod tests;
