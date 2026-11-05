use super::*;

/// The storage view says what the system said of the medium, field for field, and whether a flush
/// reaches stable media there. Do: describe this machine's temporary directory; map it. Expect:
/// every field as the description has it, by name.
#[test]
fn the_storage_view_says_what_the_system_said_of_the_medium() {
    let dir = tempfile::tempdir().unwrap();
    let medium = focal_platform::medium::medium(dir.path());
    let shown = admin_medium(&medium);
    assert_eq!(shown.class, medium.class.as_str());
    assert_eq!(shown.interconnect, medium.interconnect.as_str());
    assert_eq!(shown.file_system, medium.file_system.as_str());
    assert_eq!(shown.file_system_name, medium.file_system_name);
    assert_eq!(shown.rotational, medium.rotational);
    assert_eq!(shown.logical_block, medium.logical_block);
    assert_eq!(shown.physical_block, medium.physical_block);
    assert_eq!(shown.preferred_write, medium.preferred_write);
    assert_eq!(shown.optimal_io, medium.optimal_io);
    assert_eq!(shown.write_cache, medium.write_cache.as_str());
    assert_eq!(shown.fua, medium.fua);
    assert_eq!(shown.discard, medium.discard);
    assert_eq!(shown.removable, medium.removable);
    assert_eq!(shown.device, medium.device);
    assert_eq!(shown.model, medium.model);
    assert_eq!(shown.notes, medium.notes);
    assert_eq!(
        shown.flushes_reach_stable_media,
        medium.flushes_reach_stable_media()
    );
}

/// The write path says what the log's file took and the log's counts at the last sample, and no
/// counts before one.
#[test]
fn the_write_path_says_what_the_log_took_and_counted() {
    let path = WritePath {
        direct: true,
        layout_block: 4096,
        zero_fill: false,
        written_in_place: Some(true),
    };
    let unsampled = admin_write_path(path, None);
    assert!(unsampled.direct && !unsampled.zero_fill);
    assert_eq!(unsampled.written_in_place, Some(true));
    assert_eq!(unsampled.layout_block, 4096);
    assert_eq!(unsampled.counts, None);
    let log = LogMetrics {
        frames: 3,
        updates: 9,
        bytes: 24_576,
        flushes: 2,
        durable_writes: 1,
        durable_fallbacks: 0,
        flushed_confirmations: 1,
        zeroed_bytes: 8_192,
        swept_bytes: 4_096,
        ..LogMetrics::default()
    };
    let counts = admin_write_path(path, Some(&log)).counts.unwrap();
    assert_eq!(
        (counts.frames, counts.updates, counts.bytes, counts.flushes),
        (3, 9, 24_576, 2)
    );
    assert_eq!(
        (
            counts.durable_writes,
            counts.durable_fallbacks,
            counts.flushed_confirmations
        ),
        (1, 0, 1)
    );
    assert_eq!((counts.zeroed_bytes, counts.swept_bytes), (8_192, 4_096));
}
