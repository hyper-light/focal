use super::*;
use focal_memory::DiskBudgetConfig;

fn source(root: &Path, disk: DiskBudget) -> PayloadSource {
    PayloadSource {
        root: root.join("payloads"),
        disk,
    }
}

fn memory() -> MemoryBudget {
    MemoryBudget::new(1 << 20, 0).unwrap()
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
    let log =
        FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1_000).unwrap();
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
    let log = FilePayloads::open_with(&source(directory.path(), disk), &memory(), 1_000).unwrap();
    let first = log.append(&payload(1, 600)).unwrap();
    assert_eq!(log.append(&payload(2, 600)), None, "no second segment");
    assert_eq!(read(&log, first).unwrap(), payload(1, 600));
    // A reader reads the file once the writer has written it; a segment's bytes changed under
    // the log are caught by the reference's check. The log's own window still holds them.
    drop(log.reader().unwrap());
    let reader = log.reader().unwrap();
    assert_eq!(read(reader.as_ref(), first).unwrap(), payload(1, 600));
    let path = segment_path(&directory.path().join("payloads"), first.segment());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(read(reader.as_ref(), first), Err(PayloadError::Corrupt));
    assert_eq!(read(&log, first).unwrap(), payload(1, 600));
}

#[test]
fn the_window_serves_the_newest_payloads_and_a_full_window_refuses_rather_than_waits() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1_000).unwrap();
    let mut written = Vec::new();
    for seed in 0..200u8 {
        let bytes = payload(seed, 300);
        let Some(at) = log.append(&bytes) else {
            // Every chunk the writer still holds: refused, the row keeps its bytes.
            continue;
        };
        written.push((at, bytes));
    }
    assert!(written.len() > 100, "{}", written.len());
    for (at, bytes) in &written {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
    }
    // Every byte reaches the files: a reader behind the barrier reads them all.
    let reader = log.reader().unwrap();
    for (at, bytes) in &written {
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
}

#[test]
fn opening_a_log_lets_go_of_what_an_earlier_run_left() {
    let directory = tempfile::tempdir().unwrap();
    let at = {
        let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1_000)
            .unwrap();
        for seed in 0..12u8 {
            log.append(&payload(seed, 200)).unwrap();
        }
        assert!(log.segments() > 1);
        log.append(&payload(99, 10)).unwrap()
    };
    let log =
        FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1_000).unwrap();
    assert_eq!(log.segments(), 1);
    let segments = std::fs::read_dir(directory.path().join("payloads"))
        .unwrap()
        .count();
    assert_eq!(segments, 1);
    // What the earlier run wrote is gone: a reference to it reads nothing.
    assert!(read(&log, at).is_err());
}

/// Measurement, not a check: what an append and a read of a spilled row's size cost, one at a
/// time and as one run (doc 31 M3).
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn measure_payload_append_and_read_cost() {
    let directory = tempfile::tempdir().unwrap();
    // A session's share as a node gives it: the window is its eighth.
    let memory = MemoryBudget::new(512 << 20, 0).unwrap();
    let log = FilePayloads::open(&source(directory.path(), unbounded()), &memory).unwrap();
    const ROWS: usize = 20_000;
    for size in [96usize, 300, 600] {
        let bytes = payload(7, size);
        let started = std::time::Instant::now();
        let mut refs = Vec::with_capacity(ROWS);
        let mut refused = 0usize;
        for _ in 0..ROWS {
            match log.append(&bytes) {
                Some(at) => refs.push(at),
                None => refused += 1,
            }
        }
        let append = started.elapsed().as_nanos() as f64 / ROWS as f64;
        let mut into = vec![0u8; size];
        let started = std::time::Instant::now();
        for at in &refs {
            log.read(*at, &mut into).unwrap();
        }
        let read = started.elapsed().as_nanos() as f64 / refs.len().max(1) as f64;
        let started = std::time::Instant::now();
        for _ in 0..ROWS {
            std::hint::black_box(PayloadRef::at(0, 0, &bytes).unwrap());
        }
        let check = started.elapsed().as_nanos() as f64 / ROWS as f64;
        println!(
            "{size:>4} B: append {append:.0} ns ({refused} refused), read {read:.0} ns (check alone {check:.0} ns)"
        );
    }
}
