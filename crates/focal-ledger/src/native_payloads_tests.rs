use super::*;
use focal_memory::DiskBudgetConfig;

fn source(root: &Path, disk: DiskBudget) -> PayloadSource {
    PayloadSource {
        root: root.join("payloads"),
        disk,
    }
}

fn unbounded() -> DiskBudget {
    DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap()
}

fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| seed.wrapping_add(index as u8))
        .collect()
}

fn read(log: &dyn PayloadRead, at: PayloadRef) -> Result<Vec<u8>, PayloadError> {
    let mut bytes = vec![0; at.len()];
    log.read(at, &mut bytes).map(|()| bytes)
}

#[test]
fn payloads_read_back_across_segments_and_a_reader_keeps_what_a_sweep_removes() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), 1_000).unwrap();
    let mut written = Vec::new();
    for seed in 0..40u8 {
        let bytes = payload(seed, 100 + usize::from(seed));
        let at = log.append(&bytes).unwrap();
        written.push((at, bytes));
    }
    assert!(log.segments() > 4, "{}", log.segments());
    for (at, bytes) in &written {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
    }
    let reader = log.reader().unwrap();
    // Only the last payload's segment is live: every sealed one before it goes.
    let (last, _) = written.last().unwrap();
    let live = BTreeSet::from([last.segment()]);
    assert!(log.sweep(&live) > 0);
    assert_eq!(log.segments(), 1);
    for (at, bytes) in &written {
        if at.segment() == last.segment() {
            assert_eq!(read(&log, *at).unwrap(), *bytes);
        } else {
            assert_eq!(read(&log, *at), Err(PayloadError::Unavailable));
        }
        // The reader made before the sweep reads every one through its own handles.
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
}

#[test]
fn a_payload_that_reads_back_wrong_is_refused_and_a_full_volume_refuses_the_next_segment() {
    let directory = tempfile::tempdir().unwrap();
    // Room for one segment past the volume's protected bytes, never a second.
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        ..DiskBudgetConfig::unbounded()
    })
    .unwrap();
    disk.observe(1_500);
    let log = FilePayloads::open_with(&source(directory.path(), disk), 1_000).unwrap();
    let first = log.append(&payload(1, 600)).unwrap();
    assert_eq!(log.append(&payload(2, 600)), None, "no second segment");
    assert_eq!(read(&log, first).unwrap(), payload(1, 600));
    // A segment's bytes changed under the log are caught by the reference's check.
    let path = segment_path(&directory.path().join("payloads"), first.segment());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(read(&log, first), Err(PayloadError::Corrupt));
}

#[test]
fn opening_a_log_lets_go_of_what_an_earlier_run_left() {
    let directory = tempfile::tempdir().unwrap();
    let at = {
        let log = FilePayloads::open_with(&source(directory.path(), unbounded()), 1_000).unwrap();
        for seed in 0..12u8 {
            log.append(&payload(seed, 200)).unwrap();
        }
        assert!(log.segments() > 1);
        log.append(&payload(99, 10)).unwrap()
    };
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), 1_000).unwrap();
    assert_eq!(log.segments(), 1);
    let segments = std::fs::read_dir(directory.path().join("payloads"))
        .unwrap()
        .count();
    assert_eq!(segments, 1);
    // What the earlier run wrote is gone: a reference to it reads nothing.
    assert!(read(&log, at).is_err());
}
