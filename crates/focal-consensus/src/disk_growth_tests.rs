//! The node log's growth over the node's disk: admitted while the volume holds it above the
//! watermark, committed to the volume when durable, given back when not, bounded, and never
//! counting what the file already took.
use super::*;
use focal_memory::DiskBudgetConfig;
use hyper_log::Growth;

const MIB: u64 = 1024 * 1024;

fn disk(free: u64, headroom: u64) -> DiskBudget {
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    disk.observe(free);
    disk
}

/// A volume of 100 MiB free, as its sample reads it.
fn hundred(_: &std::path::Path) -> Option<u64> {
    Some(100 * MIB)
}

/// A volume whose free space cannot be read.
fn unread(_: &std::path::Path) -> Option<u64> {
    None
}

fn growth(disk: &DiskBudget) -> DiskGrowth {
    DiskGrowth::with_probe(disk.clone(), std::env::temp_dir(), hundred).unwrap()
}

/// Slots are admitted while the volume holds them above the watermark, and the one that would
/// pass it is refused: the bound reached, with nothing reserved for it.
#[test]
fn slots_are_admitted_until_the_watermark() {
    let disk = disk(100 * MIB, 40 * MIB);
    let mut gate = growth(&disk);
    assert!(gate.admit(32 * MIB));
    assert!(gate.admit(16 * MIB));
    assert!(!gate.admit(16 * MIB), "60 MiB free less 48 admitted is 12");
    assert_eq!(gate.admitted_bytes(), 48 * MIB);
    assert_eq!(disk.stats().outstanding, 48 * MIB);
}

/// A slot made durable is charged to the volume; one that never will be is given back; a part
/// of an admission is finished alone, oldest first.
#[test]
fn durable_slots_are_charged_and_others_given_back() {
    let disk = disk(200 * MIB, 0);
    let mut gate = growth(&disk);
    assert!(gate.admit(16 * MIB));
    assert!(gate.admit(16 * MIB));
    gate.commit(16 * MIB);
    assert_eq!(
        disk.stats().free,
        Some(184 * MIB),
        "the first slot is on the volume"
    );
    assert_eq!(gate.admitted_bytes(), 16 * MIB);
    gate.release(8 * MIB);
    assert_eq!(
        gate.admitted_bytes(),
        8 * MIB,
        "half of the second given back"
    );
    gate.commit(8 * MIB);
    let stats = disk.stats();
    assert_eq!(stats.outstanding, 0);
    assert_eq!(stats.free, Some(176 * MIB));
    // Past what was admitted: none of this gate's, nothing done.
    gate.commit(64 * MIB);
    assert_eq!(disk.stats().free, Some(176 * MIB));
}

/// Admissions outstanding at once are bounded by what the log keeps ahead; past it the slot is
/// refused, read as the bound reached, until one finishes.
#[test]
fn admissions_outstanding_are_bounded() {
    let disk = disk(1 << 40, 0);
    let mut gate = growth(&disk);
    for _ in 0..MOST_ADMITTED {
        assert!(gate.admit(MIB));
    }
    assert!(!gate.admit(MIB));
    gate.commit(MIB);
    assert!(gate.admit(MIB));
}

/// What the file took when it was opened is on the volume already: told, never reserved.
#[test]
fn what_the_file_took_at_open_is_never_reserved() {
    let disk = disk(100 * MIB, 0);
    let mut gate = growth(&disk);
    gate.held(64 * MIB);
    assert_eq!(gate.held_bytes(), 64 * MIB);
    assert_eq!(disk.stats().outstanding, 0);
}

/// A gate dropped with slots admitted gives them back: the log ended before using them.
#[test]
fn a_gate_dropped_gives_back_what_it_admitted() {
    let disk = disk(100 * MIB, 0);
    let mut gate = growth(&disk);
    assert!(gate.admit(16 * MIB));
    drop(gate);
    let stats = disk.stats();
    assert_eq!(stats.outstanding, 0);
    assert_eq!(stats.free, Some(100 * MIB));
}

/// A volume whose free space cannot be read admits nothing once a sample is due (as it is near
/// the watermark): the estimate is forgotten, and an unknown free space is no room.
#[test]
fn a_volume_that_cannot_be_read_admits_nothing_once_sampled() {
    let disk = disk(100 * MIB, 40 * MIB);
    let mut gate = DiskGrowth::with_probe(disk.clone(), std::env::temp_dir(), unread).unwrap();
    // 100 free less nothing outstanding is past twice the 40 MiB watermark: no sample due.
    assert!(gate.admit(32 * MIB));
    // 100 less 32 is within twice the watermark: sampled, unread, forgotten, refused.
    assert!(!gate.admit(16 * MIB));
    assert_eq!(disk.stats().free, None);
}
