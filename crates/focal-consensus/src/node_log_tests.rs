//! The node's log configuration from its facts: a frame holds the largest entry any group may take,
//! a group retains what its core and its owners' cadence allow, and facts that cannot hold the log
//! are refused, typed.
use super::*;

fn node(disk_bytes: u64, max_groups: usize) -> NodeLog {
    NodeLog {
        block: 4096,
        disk_bytes,
        max_groups,
        cadence_entries: 4096,
        cache_bytes: 64 << 20,
    }
}

fn groups() -> NodeConfig {
    NodeConfig::single(1, [1; 16], [2; 16])
}

#[test]
fn a_node_log_holds_the_largest_entry_and_retains_two_cadences_and_the_uncommitted() {
    let groups = groups();
    let config = config(&groups, &node(8 << 30, 1024)).unwrap();
    // A frame of the log holds the largest entry any group may take, as the shell writes it: the
    // check `DurableNode::open_on_shell` makes at open passes for every group.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log");
    let file = hyper_block::file::DeviceFile::open(
        &path,
        true,
        hyper_block::file::CachingRequest::PreferDirect,
        Alignment::new(4096).unwrap(),
    )
    .unwrap();
    let log = hyper_log::Log::create(file, config, 1).unwrap();
    assert!(log.entry_room().unwrap() >= MAX_ENTRY_BYTES + hyper_durable::ENTRY_OVERHEAD);
    drop(log.close().unwrap());
    assert_eq!(config.max_groups, 1024);
    let uncommitted = groups.max_uncommitted_bytes + groups.max_entry_bytes as u64;
    assert_eq!(
        config.group_entries,
        2 * 4096 + uncommitted / ENTRY_FIXED_BYTES as u64
    );
    assert_eq!(
        config.group_bytes,
        2 * 4096 * (MAX_ENTRY_BYTES + ENTRY_FIXED_BYTES) as u64 + uncommitted
    );
    assert_eq!(config.group_cache, (64 << 20) / 1024);
}

#[test]
fn facts_that_cannot_hold_the_node_log_are_refused_typed() {
    // A disk budget under the persist area and three segments of a frame of 8 MiB.
    assert!(matches!(
        config(&groups(), &node(16 << 20, 64)),
        Err(ConsensusError::LogFacts(hyper_log::Unfit::Disk { .. }))
    ));
    // No groups admitted.
    assert!(matches!(
        config(&groups(), &node(8 << 30, 0)),
        Err(ConsensusError::LogFacts(hyper_log::Unfit::Zero(_)))
    ));
    // A block that is not a power of two is no device's.
    let mut odd = node(8 << 30, 64);
    odd.block = 3000;
    assert!(matches!(
        config(&groups(), &odd),
        Err(ConsensusError::Configuration(_))
    ));
}

#[test]
fn the_device_block_is_what_the_system_reports_for_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log");
    std::fs::write(&path, b"").unwrap();
    let block = device_block(&path).unwrap();
    assert!(block.is_power_of_two(), "{block}");
    assert!(device_block(&dir.path().join("missing")).is_err());
}
