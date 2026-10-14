use super::*;

/// Chunks of 64 bytes, a manifest chunk every 4 of them: the production cut, scaled down.
const SMALL: ColdGeometry = ColdGeometry {
    chunk_bytes: 64,
    manifest_names: 4,
};

fn disk() -> DiskBudget {
    DiskBudget::new(DiskBudgetConfig::default()).unwrap()
}
fn memory() -> MemoryBudget {
    MemoryBudget::new(4 << 20, 1 << 20).unwrap()
}
/// A volume whose free bytes the test sets: no headroom, never sampled.
fn volume(free: u64) -> DiskBudget {
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    disk.observe(free);
    disk
}
/// Append `bytes` within the room promised for them.
fn put(store: &mut ColdStore, bytes: &[u8]) {
    let room = store.room(bytes.len()).unwrap();
    store.append(bytes, room).unwrap();
}
fn stream(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}
fn store(root: &Path, state: Option<&ColdState>) -> ColdStore {
    ColdStore::open(root, disk(), &memory(), SMALL, state).unwrap()
}
fn files(root: &Path, extension: &str) -> usize {
    fs::read_dir(root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|found| found == extension)
        })
        .count()
}

/// Every replica cuts the same chunks from the same bytes, however its appends divide them: the
/// names, the manifest chunks and the open chunk are the stream's, not its appends'.
#[test]
fn the_stream_is_cut_alike_however_it_is_appended() {
    let bytes = stream(64 * 9 + 17);
    let whole = tempfile::tempdir().unwrap();
    let mut one = store(whole.path(), None);
    put(&mut one, &bytes);
    let pieces = tempfile::tempdir().unwrap();
    let mut many = store(pieces.path(), None);
    let mut rest = &bytes[..];
    let mut size = 1;
    while !rest.is_empty() {
        let (head, tail) = rest.split_at(size.min(rest.len()));
        put(&mut many, head);
        rest = tail;
        size = size % 70 + 7;
    }
    let (state, pending) = one.capture().unwrap();
    assert_eq!(many.capture().unwrap(), (state.clone(), pending.clone()));
    assert_eq!(state.length, bytes.len() as u64);
    assert_eq!(
        state.manifests.len(),
        2,
        "eight whole chunks, two manifests"
    );
    assert_eq!(state.names.len(), 1);
    assert_eq!(state.open, &bytes[64 * 9..]);
    // Nine data chunks and two manifest chunks wait for a checkpoint.
    assert_eq!(pending.len(), 11);
    assert_eq!(files(whole.path(), "pending"), 11);
    state.check(SMALL).unwrap();
}

/// A checkpoint makes the chunks it names durable (renamed under their hashes, the directory
/// synced), and a second commit of the same chunks is done already. A crash after it leaves
/// pending chunks and an open chunk no checkpoint names: reopened from the checkpoint's state,
/// they are gone, and appending what replay appends cuts exactly what was cut before.
#[test]
fn a_checkpoint_makes_its_chunks_durable_and_a_reopen_restores_the_stream() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let bytes = stream(64 * 6 + 30);
    let (first, later) = bytes.split_at(64 * 2 + 10);
    let mut cold = store(root, None);
    put(&mut cold, first);
    let (checkpoint, pending) = cold.capture().unwrap();
    assert_eq!(pending.len(), 2);
    ColdStore::make_durable(root, &pending).unwrap();
    ColdStore::make_durable(root, &pending).unwrap();
    let (chunks, manifests) = checkpoint.named(SMALL).unwrap();
    cold.made_durable(chunks, manifests);
    assert_eq!(cold.pending(), 0);
    assert_eq!(files(root, "seed"), 2);
    assert_eq!(files(root, "pending"), 0);
    put(&mut cold, later);
    let before_crash = cold.capture().unwrap().0;
    assert!(files(root, "pending") > 0);
    drop(cold);
    let mut reopened = store(root, Some(&checkpoint));
    assert_eq!(
        files(root, "pending"),
        0,
        "what no checkpoint names is gone"
    );
    assert_eq!(
        reopened.capture().unwrap(),
        (checkpoint.clone(), Vec::new())
    );
    put(&mut reopened, later);
    assert_eq!(reopened.capture().unwrap().0, before_crash);
    // The durable chunks read back by their hashes, the stream's first bytes.
    let reader = reopened.reader();
    let chunks = before_crash.chunks(SMALL, &reader).unwrap();
    let read: Vec<u8> = chunks
        .iter()
        .flat_map(|hash| reader.read(*hash, 64).unwrap())
        .collect();
    assert_eq!(read, &bytes[..read.len()]);
    // A chunk a commit cannot find, pending or durable, fails it.
    assert!(matches!(
        ColdStore::make_durable(root, &[ContentHash([5; 32])]),
        Err(ContentError::Failed)
    ));
}

/// What a state names is listed through its manifest chunks: an empty store lacks the manifest
/// chunks and the last names first, then, once a manifest chunk is installed, the chunks behind
/// it; once everything is installed by its hash, nothing. A chunk that is not what its name says
/// is refused, on install and on read.
#[test]
fn an_install_pulls_what_a_state_names_through_its_manifests() {
    let source = tempfile::tempdir().unwrap();
    let bytes = stream(64 * 5 + 3);
    let mut cold = store(source.path(), None);
    put(&mut cold, &bytes);
    let (state, pending) = cold.capture().unwrap();
    ColdStore::make_durable(source.path(), &pending).unwrap();
    let from = cold.reader();
    let target = tempfile::tempdir().unwrap();
    let mut installing = store(target.path(), None);
    let reader = installing.reader();
    let missing = state.missing(SMALL, &reader).unwrap();
    assert_eq!(missing, vec![state.manifests[0], state.names[0]]);
    installing
        .install_as(
            state.manifests[0],
            &from.read(state.manifests[0], 1 << 20).unwrap(),
        )
        .unwrap();
    let behind = state.missing(SMALL, &reader).unwrap();
    assert_eq!(
        behind.len(),
        5,
        "four chunks behind the manifest, and the last name"
    );
    for hash in &behind {
        installing
            .install_as(*hash, &from.read(*hash, 1 << 20).unwrap())
            .unwrap();
    }
    assert!(state.missing(SMALL, &reader).unwrap().is_empty());
    assert_eq!(
        state.chunks(SMALL, &reader).unwrap(),
        state.chunks(SMALL, &from).unwrap()
    );
    assert!(matches!(
        installing.install_as(state.names[0], b"not the chunk"),
        Err(ContentError::Corrupt)
    ));
    std::fs::write(chunk_path(target.path(), state.names[0]), b"tampered").unwrap();
    assert!(matches!(
        reader.read(state.names[0], 64),
        Err(ContentError::Corrupt)
    ));
}

/// A state whose parts do not add up to its length, or that names a whole manifest's worth of
/// names or a whole chunk as open, is refused, by a store and by a check alike; so is a geometry
/// past what a chunk may hold.
#[test]
fn a_state_that_does_not_add_up_is_refused() {
    let good = ColdState {
        length: 64 + 3,
        manifests: Vec::new(),
        names: vec![ContentHash([1; 32])],
        open: vec![0; 3],
    };
    good.check(SMALL).unwrap();
    let short = ColdState {
        length: 64,
        ..good.clone()
    };
    let full = ColdState {
        length: 64 * 4,
        names: vec![ContentHash([1; 32]); 4],
        open: Vec::new(),
        ..good.clone()
    };
    let whole_open = ColdState {
        length: 64 * 2,
        open: vec![0; 64],
        ..good.clone()
    };
    for state in [&short, &full, &whole_open] {
        assert!(matches!(state.check(SMALL), Err(ContentError::Corrupt)));
        let directory = tempfile::tempdir().unwrap();
        assert!(ColdStore::open(directory.path(), disk(), &memory(), SMALL, Some(state)).is_err());
    }
    let directory = tempfile::tempdir().unwrap();
    for geometry in [
        ColdGeometry {
            chunk_bytes: 0,
            manifest_names: 4,
        },
        ColdGeometry {
            chunk_bytes: SEED_CHUNK_BYTES + 1,
            manifest_names: 4,
        },
        ColdGeometry {
            chunk_bytes: 64,
            manifest_names: SEED_CHUNK_BYTES,
        },
    ] {
        assert!(matches!(
            ColdStore::open(directory.path(), disk(), &memory(), geometry, None),
            Err(ContentError::Invalid)
        ));
    }
    ColdGeometry::STANDARD.valid().unwrap();
}

/// Any range of the stream reads back as it was appended, whether it lies in whole chunks
/// (behind a manifest chunk or among the last names), in the open chunk, or across them; a
/// range past the stream is refused.
#[test]
fn any_range_of_the_stream_reads_as_it_was_appended() {
    let directory = tempfile::tempdir().unwrap();
    let bytes = stream(64 * 6 + 40);
    let mut cold = store(directory.path(), None);
    put(&mut cold, &bytes);
    for (offset, length) in [
        (0, 1),
        (0, 64 * 4),
        (60, 10),
        (64 * 3 + 5, 64 * 2),
        (64 * 5 + 63, 2),
        (64 * 6, 40),
        (64 * 6 + 39, 1),
        (0, bytes.len()),
    ] {
        assert_eq!(
            cold.read(offset as u64, length).unwrap(),
            &bytes[offset..offset + length],
            "{offset}..+{length}"
        );
    }
    assert!(matches!(
        cold.read(bytes.len() as u64 - 1, 2),
        Err(ContentError::Invalid)
    ));
}

/// What an append takes is promised before it begins, and an append takes nothing else: a room
/// the volume cannot hold, or memory cannot name, is refused and leaves the stream, its files and
/// both budgets as they were; one granted is the volume of exactly the chunks and the manifest
/// chunk the append cuts, so the append is never refused for room once it has begun. A room is
/// for its own bytes at its own place: one promised for other bytes, or before another append,
/// is refused unused. The names are charged as they grow, a manifest's worth at most.
#[test]
fn an_append_takes_only_the_room_promised_before_it() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    // The open chunk's volume, and nothing past it.
    let disk = volume(64);
    let memory = memory();
    let mut cold = ColdStore::open(root, disk.clone(), &memory, SMALL, None).unwrap();
    assert_eq!(disk.stats().outstanding, 64, "the open chunk is promised");
    let bytes = stream(64 * 5 + 10);
    let names_before = memory.stats().used_by(BudgetKind::Index);
    assert!(matches!(
        cold.room(bytes.len()),
        Err(ContentError::Capacity)
    ));
    assert_eq!(disk.stats().outstanding, 64, "a refusal promises nothing");
    assert_eq!(memory.stats().used_by(BudgetKind::Index), names_before);
    assert_eq!(cold.length(), 0);
    assert_eq!(files(root, "pending"), 0);
    // Within the open chunk an append cuts nothing and needs no volume.
    let room = cold.room(10).unwrap();
    cold.append(&bytes[..10], room).unwrap();
    // Five chunks are cut from here (the first completes the open chunk), and one manifest
    // chunk after the fourth: five chunks' volume and a manifest chunk's, and no more.
    let rest = &bytes[10..];
    disk.observe(64 + 64 * 5 + 32 * 4);
    let room = cold.room(rest.len()).unwrap();
    assert_eq!(disk.stats().outstanding, 64 + 64 * 5 + 32 * 4);
    assert!(
        memory.stats().used_by(BudgetKind::Index) > names_before,
        "the names are charged"
    );
    let held = memory.stats().used_by(BudgetKind::Index);
    let wrong = cold.room(3).unwrap();
    assert!(matches!(
        cold.append(&rest[..3], room),
        Err(ContentError::Invalid)
    ));
    assert_eq!(
        cold.length(),
        10,
        "a room for other bytes is refused unused"
    );
    cold.append(&rest[..3], wrong).unwrap();
    let room = cold.room(rest.len() - 3).unwrap();
    let stale = cold.room(1).unwrap();
    cold.append(&rest[3..], room).unwrap();
    assert!(
        matches!(cold.append(&rest[..1], stale), Err(ContentError::Invalid)),
        "a room promised before another append is refused"
    );
    assert_eq!(cold.length(), bytes.len() as u64);
    let (state, pending) = cold.capture().unwrap();
    assert_eq!(state.manifests.len(), 1);
    assert_eq!(state.names.len(), 1);
    assert_eq!(pending.len(), 6, "five chunks and a manifest chunk");
    // Every cut took its volume from the room: the cut chunks, the manifest chunk and the next
    // open chunk's promise, with nothing left over once the rooms are spent.
    assert_eq!(disk.stats().outstanding, 64);
    assert!(memory.stats().used_by(BudgetKind::Index) <= held + 64);
    assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes);
    drop(cold);
    assert_eq!(
        memory.stats().used_by(BudgetKind::Index),
        0,
        "the names are given back"
    );
}
