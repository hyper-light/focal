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
    assert!(log.sweep(&live, u32::MAX) > 0);
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

/// The handle of `segment` the writer is handed with each write, replaced: one that cannot write,
/// so each write through it fails as on a failing volume, or one that can.
fn writable(log: &FilePayloads, segment: u32, can: bool) {
    let path = segment_path(&log.root, segment);
    let file = if can {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    } else {
        File::open(&path).unwrap()
    };
    let mut writer = log.writer.lock().unwrap();
    writer.segments.get_mut(&segment).unwrap().file = file;
}

/// Every write handed so far done by the writer and given back, not yet taken in.
fn drained(log: &FilePayloads) {
    let (reply, barrier) = sync_channel(1);
    let writer = log.writer.lock().unwrap();
    writer
        .jobs
        .as_ref()
        .unwrap()
        .try_send(Job::Barrier(reply))
        .unwrap();
    drop(writer);
    barrier.recv_timeout(Duration::from_secs(30)).unwrap();
}

/// A write that failed leaves its bytes, and every byte after them in its chunk, to be handed
/// again: a write of the chunk's suffix that succeeded after it does not count past the hole, and
/// the window keeps the chunk, refusing payloads rather than let it go, until the whole of it is
/// in the file. Then it is let go and what it held reads back from the file.
#[test]
fn a_chunk_whose_prefix_failed_is_kept_though_its_suffix_was_written() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    let (prefix, suffix) = (payload(1, 1000), payload(2, 1000));
    let first = log.append(&prefix).unwrap();
    let segment = first.segment();
    // The prefix handed through a handle that cannot write; the suffix, appended before that
    // failure is taken in, through one that can.
    writable(&log, segment, false);
    assert!(log.writer.lock().unwrap().hand(true, false));
    writable(&log, segment, true);
    {
        let mut writer = log.writer.lock().unwrap();
        let chunk = writer.window.back_mut().unwrap();
        chunk.bytes.extend_from_slice(&suffix);
        writer.tail += suffix.len() as u64;
        assert!(writer.hand(true, false));
    }
    let second = PayloadRef::at(segment, 1000, &suffix).unwrap();
    drained(&log);
    // In the file: the suffix, after a hole where the prefix failed.
    let file = std::fs::read(segment_path(&log.root, segment)).unwrap();
    assert_eq!(file.get(1000..2000), Some(&suffix[..]));
    assert!(
        file.get(..1000)
            .is_none_or(|hole| hole.iter().all(|byte| *byte == 0))
    );
    {
        let mut writer = log.writer.lock().unwrap();
        writer.settle();
        assert_eq!(writer.failures_seen, 1);
        let chunk = writer.window.back().unwrap();
        assert_eq!((chunk.sent, chunk.written, chunk.bytes.len()), (0, 0, 2000));
        assert_eq!(writer.unsent_from, 0);
    }
    // While every write fails, the window keeps the chunk: the payloads that need its buffer are
    // refused, and what it holds is read from it.
    writable(&log, segment, false);
    let mut written = vec![(first, prefix.clone()), (second, suffix.clone())];
    let mut refused = 0;
    for seed in 3..40u8 {
        let bytes = payload(seed, 30_000);
        match log.append(&bytes) {
            Some(at) => written.push((at, bytes)),
            None => refused += 1,
        }
    }
    assert!(refused > 30, "{refused}");
    for (at, bytes) in &written {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
    }
    // Once writes succeed, a reader behind the writer's barrier reads every byte from the files.
    drained(&log);
    log.writer.lock().unwrap().settle();
    writable(&log, segment, true);
    let reader = log.reader().unwrap();
    for (at, bytes) in &written {
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
    // The chunk is let go for the next payloads, and what it held is read from the file.
    for seed in 40..60u8 {
        let bytes = payload(seed, 30_000);
        if let Some(at) = log.append(&bytes) {
            written.push((at, bytes));
        }
    }
    assert!(
        log.writer
            .lock()
            .unwrap()
            .window
            .iter()
            .all(|chunk| (chunk.segment, chunk.start) != (segment, 0))
    );
    for (at, bytes) in &written {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
    }
}

/// Bytes appended to a chunk after a reader handed what it held are handed with it: the chunk
/// appends go to is looked at by every hand, so it is written whole and let go when the window
/// needs it, and appends past a full window are not refused for it.
#[test]
fn bytes_appended_after_a_reader_handed_their_chunk_are_written_and_the_chunk_let_go() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    let mut written = Vec::new();
    for seed in 0..60u8 {
        let bytes = payload(seed, 20_000);
        let at = log.append(&bytes).unwrap();
        written.push((at, bytes));
        // A reader made between appends to the same chunk, as a checkpoint makes one.
        let reader = log.reader().unwrap();
        let (at, bytes) = written.last().unwrap();
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
    let reader = log.reader().unwrap();
    for (at, bytes) in &written {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
}

/// A log let go while its writer holds work, after a panic poisoned its lock and with what the
/// writer gave back not taken in: dropping it closes the writer's queue all the same and takes what
/// comes back, so the writer ends and the drop returns rather than waits for ever.
#[test]
fn a_log_dropped_after_a_panic_under_its_lock_lets_its_writer_end() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    for seed in 0..4u8 {
        log.append(&payload(seed, 30_000)).unwrap();
    }
    assert!(log.writer.lock().unwrap().hand(true, true));
    let poisoned = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let _writer = log.writer.lock().unwrap();
                panic!("poisons the writer's lock");
            })
            .join()
    });
    assert!(poisoned.is_err());
    assert!(log.writer.lock().is_err());
    // Dropped on a thread of its own, so a drop that never returns fails the test, not hangs it.
    let (done, finished) = sync_channel(1);
    let dropping = std::thread::spawn(move || {
        drop(log);
        let _ = done.send(());
    });
    finished.recv_timeout(Duration::from_secs(30)).unwrap();
    dropping.join().unwrap();
}

/// A log whose writer has not come back from a write by the close's deadline lets the writer go,
/// but not what the writer holds: the buffer handed to it and its queues stay charged until it
/// ends, and only the log's own window and pool go at the close.
#[test]
fn a_writer_let_go_at_the_close_deadline_stays_charged_for_what_it_holds_until_it_ends() {
    let directory = tempfile::tempdir().unwrap();
    let memory = memory();
    let used = || memory.stats().used_by(BudgetKind::Payload);
    let (open, gate) = sync_channel::<()>(0);
    let mut log = FilePayloads::open_gated(
        &source(directory.path(), unbounded()),
        &memory,
        1 << 20,
        Some(gate),
    )
    .unwrap();
    log.close_wait = Duration::from_millis(50);
    let opened = used();
    log.append(&payload(1, 1000)).unwrap();
    // One write handed: the writer takes it and is held before writing it.
    assert!(log.writer.lock().unwrap().hand(true, false));
    let (chunk_bytes, slots) = {
        let writer = log.writer.lock().unwrap();
        let depth = writer.max_chunks * 2 + BARRIERS;
        (
            writer.chunk_bytes,
            depth * (size_of::<Job>() + size_of::<Written>()),
        )
    };
    assert_eq!(used(), opened);
    let writer = log.thread.take().unwrap();
    drop(log);
    assert_eq!(used(), chunk_bytes + slots);
    // Let on, the writer finds no one to take what it gives back, and ends: nothing is charged.
    drop(open);
    writer.join().unwrap();
    assert_eq!(used(), 0);
}

/// A reader whose bytes did not all reach the files refuses to read rather than read a hole: the
/// writer's barrier reports a failure the log had not seen when the reader was made, and the
/// reader stays shut. The next reader, made once the failure is taken in and the bytes handed
/// again, reads them from the file.
#[test]
fn a_reader_behind_a_failed_write_refuses_and_the_next_reads_the_bytes_handed_again() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    let bytes = payload(1, 1000);
    let at = log.append(&bytes).unwrap();
    writable(&log, at.segment(), false);
    let refused = log.reader().unwrap();
    assert_eq!(read(refused.as_ref(), at), Err(PayloadError::Unavailable));
    assert_eq!(read(refused.as_ref(), at), Err(PayloadError::Unavailable));
    writable(&log, at.segment(), true);
    let reader = log.reader().unwrap();
    assert_eq!(read(reader.as_ref(), at).unwrap(), bytes);
    let file = std::fs::read(segment_path(&log.root, at.segment())).unwrap();
    assert_eq!(file.get(..1000), Some(&bytes[..]));
    assert_eq!(read(refused.as_ref(), at), Err(PayloadError::Unavailable));
}

/// A buffer made beyond the pool for a reader's barrier is charged before it is allocated and for
/// as long as the writer holds it, then let go with its charge: the log's charge is the chunks and
/// buffers it holds, never fewer bytes, and never more once the buffer is back.
#[test]
fn a_buffer_made_beyond_the_pool_is_charged_while_the_writer_holds_it() {
    let directory = tempfile::tempdir().unwrap();
    let memory = memory();
    let log =
        FilePayloads::open_with(&source(directory.path(), unbounded()), &memory, 1 << 20).unwrap();
    let bytes = payload(1, 1000);
    let at = log.append(&bytes).unwrap();
    let used = || memory.stats().used_by(BudgetKind::Payload);
    let (pool, before) = {
        let mut writer = log.writer.lock().unwrap();
        // The pool's buffers are out: the next hand has none of its own.
        (std::mem::take(&mut writer.buffers), used())
    };
    {
        let mut writer = log.writer.lock().unwrap();
        assert!(!writer.hand(true, false), "no buffer, none made");
        assert!(writer.hand(true, true));
        let made = used() - before;
        assert!(made >= bytes.len(), "{made}");
    }
    drained(&log);
    {
        let mut writer = log.writer.lock().unwrap();
        writer.settle();
        assert_eq!(used(), before);
        assert!(writer.buffers.is_empty(), "the made buffer is let go");
        writer.buffers = pool;
    }
    let reader = log.reader().unwrap();
    assert_eq!(read(reader.as_ref(), at).unwrap(), bytes);
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

/// The bytes of segment `segment` in its file now.
fn on_disk(log: &FilePayloads, segment: u32) -> u64 {
    std::fs::metadata(segment_path(&log.root, segment)).map_or(0, |metadata| metadata.len())
}

/// What a hold holds back is never handed to the writer, even as its chunk fills and a reader's
/// barrier asks for every byte, and giving it back takes the log back to where it began: the
/// next append goes where the first held one went, and what was appended before reads as it did.
/// A hold that no longer stands changes nothing.
#[test]
fn a_held_append_is_never_written_and_giving_it_back_takes_the_log_back_to_where_it_began() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    let before: Vec<_> = (0..3u8)
        .map(|seed| {
            let bytes = payload(seed, 10_000);
            (log.append(&bytes).unwrap(), bytes)
        })
        .collect();
    let kept = log.bytes();
    let hold = log.hold().unwrap();
    // Past the window's chunk (64 KiB here): the filled chunk is handed only up to the hold.
    let held: Vec<_> = (10..18u8)
        .map(|seed| log.append(&payload(seed, 10_000)).unwrap())
        .collect();
    assert_eq!(log.bytes(), kept + 80_000);
    drop(log.reader().unwrap());
    drained(&log);
    assert_eq!(on_disk(&log, 0), kept, "a held byte reached the file");
    for at in &held {
        assert!(
            read(&log, *at).is_ok(),
            "the window serves what it holds back"
        );
    }
    assert!(log.give_back(hold));
    assert_eq!(log.bytes(), kept);
    assert!(!log.give_back(hold), "a hold given back no longer stands");
    let next = log.append(&payload(99, 500)).unwrap();
    assert_eq!(u64::from(next.offset()), kept);
    assert_eq!(read(&log, next).unwrap(), payload(99, 500));
    let reader = log.reader().unwrap();
    for (at, bytes) in &before {
        assert_eq!(read(&log, *at).unwrap(), *bytes);
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
    assert_eq!(read(reader.as_ref(), next).unwrap(), payload(99, 500));
}

/// A hold kept is the log's: what it held back is written as any append is, and a hold that no
/// longer stands gives nothing back.
#[test]
fn a_kept_hold_is_written_as_any_append() {
    let directory = tempfile::tempdir().unwrap();
    let log = FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1 << 20)
        .unwrap();
    let hold = log.hold().unwrap();
    let held: Vec<_> = (0..8u8)
        .map(|seed| {
            let bytes = payload(seed, 10_000);
            (log.append(&bytes).unwrap(), bytes)
        })
        .collect();
    log.keep(hold);
    assert!(!log.give_back(hold));
    let reader = log.reader().unwrap();
    for (at, bytes) in &held {
        assert_eq!(read(reader.as_ref(), *at).unwrap(), *bytes);
    }
    assert_eq!(on_disk(&log, 0), 80_000);
}

/// A hold whose appends opened new segments gives them back as well: the segment it began in is
/// sealed where it began, those opened since hold nothing (a sweep removes them, no row naming
/// them), appends go on from the newest's first byte, and nothing held was written anywhere.
#[test]
fn a_hold_given_back_across_new_segments_seals_where_it_began() {
    let directory = tempfile::tempdir().unwrap();
    let log =
        FilePayloads::open_with(&source(directory.path(), unbounded()), &memory(), 1_000).unwrap();
    let first = log.append(&payload(1, 700)).unwrap();
    let hold = log.hold().unwrap();
    for seed in 2..6u8 {
        log.append(&payload(seed, 700)).unwrap();
    }
    assert!(log.segments() >= 4, "{}", log.segments());
    drop(log.reader().unwrap());
    drained(&log);
    for segment in 1..u32::try_from(log.segments()).unwrap() {
        assert_eq!(
            on_disk(&log, segment),
            0,
            "segment {segment} holds held bytes"
        );
    }
    assert!(log.give_back(hold));
    assert_eq!(log.bytes(), 700);
    let next = log.append(&payload(9, 300)).unwrap();
    assert_eq!(next.offset(), 0);
    assert!(next.segment() > first.segment());
    assert_eq!(read(&log, first).unwrap(), payload(1, 700));
    assert_eq!(read(&log, next).unwrap(), payload(9, 300));
    // Only the first payload's segment and the newest are live: the sealed empty ones go.
    let live = BTreeSet::from([first.segment(), next.segment()]);
    log.sweep(&live, u32::MAX);
    assert_eq!(log.segments(), 2);
    let reader = log.reader().unwrap();
    assert_eq!(read(reader.as_ref(), first).unwrap(), payload(1, 700));
    assert_eq!(read(reader.as_ref(), next).unwrap(), payload(9, 300));
}
