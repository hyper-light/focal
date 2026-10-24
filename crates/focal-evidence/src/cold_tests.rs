use super::*;

/// Chunks of 64 bytes, a manifest chunk every 4 of them: the production cut, scaled down.
const SMALL: ColdGeometry = ColdGeometry {
    chunk_bytes: 64,
    manifest_names: 4,
};

const FORMS: [ContentForm; 2] = [ContentForm::Clear, ContentForm::Sealed];

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
/// Bytes no stored byte may repeat: patterned, so a copy of any 16 of them shows.
fn plaintext(seed: u8, length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| (index.wrapping_mul(31) ^ (index >> 8)) as u8 ^ seed)
        .collect()
}
fn keys(data: &Path) -> focal_seal::DataKeys {
    crate::store::test_keys(data)
}
/// The cold directory `name` of the data directory `data`, opened in `form` at `state`.
fn store(
    data: &Path,
    name: &str,
    state: Option<&ColdState>,
    form: ContentForm,
) -> (PathBuf, ColdStore) {
    let root = data.join(name);
    let cold = ColdStore::open(&root, disk(), &memory(), SMALL, state, &keys(data), form).unwrap();
    (root, cold)
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
/// The chunks named in the clear a directory holds: a hash and `.seed`.
fn clear_names(root: &Path) -> usize {
    fs::read_dir(root)
        .unwrap()
        .filter(|entry| {
            let path = entry.as_ref().unwrap().path();
            chunk_convert::clear_seed(&path).unwrap().is_some()
        })
        .count()
}

/// Every replica cuts the same chunks from the same bytes, however its appends divide them and in
/// whichever form it keeps them: the names, the manifest chunks and the open chunk are the
/// stream's, not its appends' or its form's, so a sealed replica's chunk hashes are a clear one's.
#[test]
fn the_stream_is_cut_alike_however_it_is_appended() {
    let mut states = Vec::new();
    for form in FORMS {
        let bytes = stream(64 * 9 + 17);
        let directory = tempfile::tempdir().unwrap();
        let (whole, mut one) = store(directory.path(), "whole", None, form);
        put(&mut one, &bytes);
        let (_, mut many) = store(directory.path(), "pieces", None, form);
        let mut rest = &bytes[..];
        let mut size = 1;
        while !rest.is_empty() {
            let (head, tail) = rest.split_at(size.min(rest.len()));
            put(&mut many, head);
            rest = tail;
            size = size % 70 + 7;
        }
        let (state, pending) = one.capture().unwrap();
        let (other, other_pending) = many.capture().unwrap();
        assert_eq!(other, state, "{form:?}");
        assert_eq!(other_pending.len(), pending.len(), "{form:?}");
        if form == ContentForm::Clear {
            // In the clear, the pending chunks are named by their hashes alike.
            assert_eq!(other_pending, pending);
        }
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
        assert_eq!(files(&whole, "pending"), 11, "{form:?}");
        state.check(SMALL).unwrap();
        states.push(state);
    }
    let [clear, sealed] = &states[..] else {
        panic!("a state in each form");
    };
    assert_eq!(sealed, clear, "a sealed replica cuts what a clear one cuts");
}

/// A checkpoint makes the chunks it names durable (renamed under their names, the directory
/// synced), and a second commit of the same chunks is done already. A crash after it leaves
/// pending chunks and an open chunk no checkpoint names: reopened from the checkpoint's state,
/// they are gone, and appending what replay appends cuts exactly what was cut before.
#[test]
fn a_checkpoint_makes_its_chunks_durable_and_a_reopen_restores_the_stream() {
    for form in FORMS {
        let directory = tempfile::tempdir().unwrap();
        let bytes = stream(64 * 6 + 30);
        let (first, later) = bytes.split_at(64 * 2 + 10);
        let (root, mut cold) = store(directory.path(), "cold", None, form);
        put(&mut cold, first);
        let (checkpoint, pending) = cold.capture().unwrap();
        assert_eq!(pending.len(), 2);
        ColdStore::make_durable(&root, &pending).unwrap();
        ColdStore::make_durable(&root, &pending).unwrap();
        let (chunks, manifests) = checkpoint.named(SMALL).unwrap();
        cold.made_durable(chunks, manifests);
        assert_eq!(cold.pending(), 0);
        assert_eq!(files(&root, "seed"), 2, "{form:?}");
        assert_eq!(files(&root, "pending"), 0);
        put(&mut cold, later);
        let before_crash = cold.capture().unwrap().0;
        assert!(files(&root, "pending") > 0);
        drop(cold);
        let (_, mut reopened) = store(directory.path(), "cold", Some(&checkpoint), form);
        assert_eq!(
            files(&root, "pending"),
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
            ColdStore::make_durable(
                &root,
                &[ColdPending {
                    stem: ContentHash([5; 32]).to_string()
                }]
            ),
            Err(ContentError::Failed)
        ));
    }
}

/// What a state names is listed through its manifest chunks: an empty store lacks the manifest
/// chunks and the last names first, then, once a manifest chunk is installed, the chunks behind
/// it; once everything is installed by its hash, nothing. A chunk that is not what its name says
/// is refused, on install and on read; a chunk installed without a writer lies in the form the
/// directory's writer keeps.
#[test]
fn an_install_pulls_what_a_state_names_through_its_manifests() {
    for form in FORMS {
        let directory = tempfile::tempdir().unwrap();
        let bytes = stream(64 * 5 + 3);
        let (source, mut cold) = store(directory.path(), "source", None, form);
        put(&mut cold, &bytes);
        let (state, pending) = cold.capture().unwrap();
        ColdStore::make_durable(&source, &pending).unwrap();
        let from = cold.reader();
        let (_, mut installing) = store(directory.path(), "target", None, form);
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
        for hash in &behind[1..] {
            installing
                .install_as(*hash, &from.read(*hash, 1 << 20).unwrap())
                .unwrap();
        }
        // One without the writer, as an installing replica takes a chunk before its writer opens.
        install_cold_chunk(
            &directory.path().join("target"),
            &disk(),
            &keys(directory.path()),
            form,
            behind[0],
            &from.read(behind[0], 1 << 20).unwrap(),
        )
        .unwrap();
        assert!(state.missing(SMALL, &reader).unwrap().is_empty());
        assert_eq!(
            state.chunks(SMALL, &reader).unwrap(),
            state.chunks(SMALL, &from).unwrap()
        );
        assert!(matches!(
            installing.install_as(state.names[0], b"not the chunk"),
            Err(ContentError::Corrupt)
        ));
        std::fs::write(reader.file_of(state.names[0]).unwrap(), b"tampered").unwrap();
        assert!(matches!(
            reader.read(state.names[0], 64),
            Err(ContentError::Corrupt)
        ));
    }
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
    let directory = tempfile::tempdir().unwrap();
    let data_keys = keys(directory.path());
    for state in [&short, &full, &whole_open] {
        assert!(matches!(state.check(SMALL), Err(ContentError::Corrupt)));
        for form in FORMS {
            assert!(
                ColdStore::open(
                    directory.path().join("cold"),
                    disk(),
                    &memory(),
                    SMALL,
                    Some(state),
                    &data_keys,
                    form,
                )
                .is_err()
            );
        }
    }
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
            ColdStore::open(
                directory.path().join("cold"),
                disk(),
                &memory(),
                geometry,
                None,
                &data_keys,
                ContentForm::Clear,
            ),
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
    for form in FORMS {
        let directory = tempfile::tempdir().unwrap();
        let bytes = stream(64 * 6 + 40);
        let (_, mut cold) = store(directory.path(), "cold", None, form);
        // In uneven appends, so the open chunk holds several records.
        for piece in bytes.chunks(13) {
            put(&mut cold, piece);
        }
        for (offset, length) in [
            (0, 1),
            (0, 64 * 4),
            (60, 10),
            (64 * 3 + 5, 64 * 2),
            (64 * 5 + 63, 2),
            (64 * 6, 40),
            (64 * 6 + 7, 20),
            (64 * 6 + 39, 1),
            (0, bytes.len()),
        ] {
            assert_eq!(
                cold.read(offset as u64, length).unwrap(),
                &bytes[offset..offset + length],
                "{form:?} {offset}..+{length}"
            );
        }
        assert!(matches!(
            cold.read(bytes.len() as u64 - 1, 2),
            Err(ContentError::Invalid)
        ));
    }
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
    let root = directory.path().join("cold");
    // The open chunk's volume, and nothing past it.
    let disk = volume(64);
    let memory = memory();
    let mut cold = ColdStore::open(
        &root,
        disk.clone(),
        &memory,
        SMALL,
        None,
        &keys(directory.path()),
        ContentForm::Clear,
    )
    .unwrap();
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
    assert_eq!(files(&root, "pending"), 0);
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

/// Sealed, an append's room holds its records' overhead and the sealed manifest chunk's bound
/// besides, and the appends spend it exactly: what is promised once the rooms are spent is the
/// open chunk's (a record file's header and a chunk's plaintext).
#[test]
fn a_sealed_append_spends_exactly_what_its_room_promised() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("cold");
    let disk = volume(u64::MAX / 4);
    let memory = memory();
    let mut cold = ColdStore::open(
        &root,
        disk.clone(),
        &memory,
        SMALL,
        None,
        &keys(directory.path()),
        ContentForm::Sealed,
    )
    .unwrap();
    let open = 64 + cold_records::HEADER as u64;
    assert_eq!(disk.stats().outstanding, open, "the open chunk is promised");
    let bytes = stream(64 * 5 + 10);
    for piece in bytes.chunks(23) {
        put(&mut cold, piece);
        assert_eq!(disk.stats().outstanding, open, "every room spent");
    }
    assert!(files(&root, "pending") > 0);
    assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes);
}

/// Sealed, no name or byte of the directory holds a chunk's plaintext or hash, and every change
/// an attacker can make is refused: a flipped byte in a whole chunk or in the open chunk, a record
/// moved within the open chunk, another stream's open chunk put in its place, a whole chunk under
/// another chunk's name; a directory erased by its key's record reads no more. What a crash left
/// unnamed is gone at the next open, which writes its open chunk under a key of its own.
#[test]
fn a_sealed_stream_holds_no_plaintext_and_refuses_what_was_changed() {
    let directory = tempfile::tempdir().unwrap();
    let bytes = plaintext(7, 64 * 5 + 50);
    let (root, mut cold) = store(directory.path(), "cold", None, ContentForm::Sealed);
    for piece in bytes.chunks(17) {
        put(&mut cold, piece);
    }
    let (state, pending) = cold.capture().unwrap();
    ColdStore::make_durable(&root, &pending).unwrap();
    // No name or byte holds a hash or the plaintext.
    let hashes: Vec<ContentHash> = state
        .manifests
        .iter()
        .chain(&state.names)
        .copied()
        .collect();
    for entry in fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let stored = crate::store::stored_bytes(&path);
        for hash in &hashes {
            assert!(!name.contains(&hash.to_string()), "{name} names a hash");
            assert!(
                !stored.windows(32).any(|window| window == hash.0),
                "{name} holds a hash"
            );
        }
        for probe in bytes.chunks(16).filter(|probe| probe.len() == 16) {
            assert!(
                !stored.windows(16).any(|window| window == probe),
                "{name} holds plaintext"
            );
        }
    }
    let reader = cold.reader();
    let whole = state.names[0];
    let file = reader.file_of(whole).unwrap();
    let original = fs::read(&file).unwrap();
    // A flipped byte in a whole chunk: in its header, a length block and its bytes.
    for at in [3, cold_records::HEADER + 2, original.len() - 3] {
        let mut flipped = original.clone();
        flipped[at] ^= 1;
        fs::write(&file, &flipped).unwrap();
        assert!(
            matches!(reader.read(whole, 64), Err(ContentError::Corrupt)),
            "flipped at {at}"
        );
    }
    fs::write(&file, &original).unwrap();
    assert_eq!(reader.read(whole, 64).unwrap(), &bytes[64 * 4..64 * 5]);
    // A whole chunk under another chunk's name.
    let other = reader.file_of(state.manifests[0]).unwrap();
    let other_bytes = fs::read(&other).unwrap();
    fs::copy(&file, &other).unwrap();
    assert!(matches!(
        reader.read(state.manifests[0], 1 << 20),
        Err(ContentError::Corrupt)
    ));
    fs::write(&other, &other_bytes).unwrap();
    // The open chunk: a flipped byte, then its first two records swapped.
    let open = root.join(format!(
        "open.{}",
        state.manifests.len() * 4 + state.names.len()
    ));
    let open_bytes = fs::read(&open).unwrap();
    let tail = 64 * 5;
    let mut flipped = open_bytes.clone();
    *flipped.last_mut().unwrap() ^= 1;
    fs::write(&open, &flipped).unwrap();
    assert!(cold.read(tail as u64, 50).is_err());
    let record = cold_records::RECORD_OVERHEAD + 17;
    let first = cold_records::HEADER;
    let mut swapped = open_bytes.clone();
    assert!(swapped.len() >= first + 2 * record);
    let (left, right) = swapped[first..first + 2 * record].split_at_mut(record);
    left.swap_with_slice(right);
    fs::write(&open, &swapped).unwrap();
    assert!(cold.read(tail as u64, 30).is_err(), "records swapped");
    fs::write(&open, &open_bytes).unwrap();
    assert_eq!(cold.read(tail as u64, 50).unwrap(), &bytes[tail..]);
    // Another stream's open chunk in its place.
    let (other_root, mut stranger) = store(directory.path(), "stranger", None, ContentForm::Sealed);
    put(&mut stranger, &bytes[..50]);
    fs::copy(other_root.join("open.0"), &open).unwrap();
    assert!(cold.read(tail as u64, 50).is_err());
    fs::write(&open, &open_bytes).unwrap();
    // Reopened, the open chunk is written under a key of its own; what a crash left is gone.
    let id = |path: &Path| fs::read(path).unwrap()[8..24].to_vec();
    let before = id(&open);
    drop(cold);
    fs::write(root.join("stray.pending"), b"left by a crash").unwrap();
    let (_, cold) = store(directory.path(), "cold", Some(&state), ContentForm::Sealed);
    assert!(!root.join("stray.pending").exists());
    assert_ne!(id(&open), before, "a new key for the open chunk");
    assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes);
    // The directory's record destroyed: its chunks read no more, another stream's still do.
    fs::remove_file(root.join(sealing::DOMAIN_KEY)).unwrap();
    assert!(cold.reader().read(whole, 64).is_err());
    assert_eq!(stranger.read(0, 50).unwrap(), &bytes[..50]);
}

/// No key and nonce seal two plaintexts: a record file's key is its ID's and a block's nonce its
/// place, so each ID must name one run of bytes that only ever grows. Whatever the stream does
/// (appends, cuts, a checkpoint, then a crash whose reopen appends other bytes at the same offsets
/// of the stream), every record file met under an ID holds what was met under it before and
/// perhaps more, never other bytes where it held some.
#[test]
fn no_record_files_key_and_nonce_seal_two_plaintexts() {
    use std::collections::BTreeSet;
    /// Every record file of `root` (its magic first), by its ID.
    fn record_files(root: &Path) -> Vec<([u8; 16], Vec<u8>)> {
        let mut found = Vec::new();
        for entry in fs::read_dir(root).unwrap() {
            let stored = crate::store::stored_bytes(&entry.unwrap().path());
            if stored.starts_with(cold_records::MAGIC) {
                found.push((stored[8..24].try_into().unwrap(), stored));
            }
        }
        found
    }
    /// Each ID's longest bytes met so far: a file met again under an ID it was met under must
    /// begin with them, or they with it.
    fn meet(met: &mut BTreeMap<[u8; 16], Vec<u8>>, root: &Path, at: &str) {
        let mut ids = BTreeSet::new();
        for (id, stored) in record_files(root) {
            assert!(ids.insert(id), "{at}: two files of one ID");
            let longest = met.entry(id).or_default();
            let shared = longest.len().min(stored.len());
            assert_eq!(
                longest[..shared],
                stored[..shared],
                "{at}: an ID's bytes changed where it held some"
            );
            if stored.len() > longest.len() {
                *longest = stored;
            }
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let mut met = BTreeMap::new();
    let (root, mut cold) = store(directory.path(), "cold", None, ContentForm::Sealed);
    for piece in plaintext(1, 64 * 2 + 40).chunks(16) {
        put(&mut cold, piece);
        meet(&mut met, &root, "appending");
    }
    let (state, pending) = cold.capture().unwrap();
    ColdStore::make_durable(&root, &pending).unwrap();
    meet(&mut met, &root, "checkpointed");
    // Past the checkpoint, a cut and part of the next chunk; then a crash.
    for piece in plaintext(2, 64 + 30).chunks(16) {
        put(&mut cold, piece);
        meet(&mut met, &root, "past the checkpoint");
    }
    drop(cold);
    // Reopened at the checkpoint, the same offsets of the stream take other bytes.
    let (_, mut cold) = store(directory.path(), "cold", Some(&state), ContentForm::Sealed);
    meet(&mut met, &root, "reopened");
    for piece in plaintext(3, 64 * 3 + 20).chunks(16) {
        put(&mut cold, piece);
        meet(&mut met, &root, "appending again");
    }
    // Every file the stream wrote was met, one ID a file: the chunks cut before the checkpoint,
    // the open chunk before it, both runs past it, each reopen's open chunk.
    assert!(met.len() >= 2 + 2 + 3, "{} record files", met.len());
    let mut expected = plaintext(1, 64 * 2 + 40);
    expected.extend_from_slice(&plaintext(3, 64 * 3 + 20));
    assert_eq!(cold.read(0, expected.len()).unwrap(), expected);
}

/// A whole chunk of many small records (a marks stream's: 65,536 of 16 bytes) reads back as it was
/// appended, opened in place in its own buffer, which holds the chunk and a record's tag's room
/// and no more, as `read_bytes` charges; a capture holds the open chunk's bytes and that room, as
/// `captured_bytes` says.
#[test]
fn a_chunk_of_small_records_is_read_in_place_within_its_charge() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("marks");
    let geometry = ColdGeometry::STANDARD;
    let mut cold = ColdStore::open(
        &root,
        disk(),
        &memory(),
        geometry,
        None,
        &keys(directory.path()),
        ContentForm::Sealed,
    )
    .unwrap();
    let marks = plaintext(5, geometry.chunk_bytes + 16 * 3);
    for mark in marks.chunks(16) {
        put(&mut cold, mark);
    }
    let (state, pending) = cold.capture().unwrap();
    assert_eq!(state.open, &marks[geometry.chunk_bytes..]);
    assert!(state.open.capacity() <= state.open.len() + hyper_seal::TAG);
    assert_eq!(
        cold.captured_bytes().unwrap(),
        names_bytes(0, 1, 0).unwrap()
            + size_of::<ColdPending>()
            + state.open.len()
            + hyper_seal::TAG
    );
    ColdStore::make_durable(&root, &pending).unwrap();
    let whole = cold
        .reader()
        .read(state.names[0], geometry.chunk_bytes)
        .unwrap();
    assert_eq!(whole, &marks[..geometry.chunk_bytes]);
    assert!(whole.capacity() <= geometry.chunk_bytes + hyper_seal::TAG);
    assert!(geometry.read_bytes(16).unwrap() >= 16 + whole.capacity());
    assert_eq!(cold.read(0, marks.len()).unwrap(), marks);
}

/// An idle sealed stream holds, beyond its names, its open chunk's key and where reads begin:
/// less than a kibibyte, charged; a stream in the clear holds nothing more.
#[test]
fn an_idle_streams_writer_holds_less_than_a_kibibyte() {
    for (form, most) in [(ContentForm::Clear, 0), (ContentForm::Sealed, 1024)] {
        let directory = tempfile::tempdir().unwrap();
        let memory = memory();
        let mut cold = ColdStore::open(
            directory.path().join("cold"),
            disk(),
            &memory,
            ColdGeometry::STANDARD,
            None,
            &keys(directory.path()),
            form,
        )
        .unwrap();
        put(&mut cold, b"a family retired");
        let names = names_bytes(
            cold.manifests.capacity(),
            cold.names.capacity(),
            cold.pending.capacity(),
        )
        .unwrap();
        let held = memory.stats().used_by(BudgetKind::Index);
        assert!(held >= names);
        assert!(held - names <= most, "{form:?}: {} bytes", held - names);
        assert_eq!(held - names, cold.writer_bytes());
    }
}

/// A directory in the clear asked to seal converts its whole chunks and manifest chunks while the
/// stream goes on: every range reads at every step, the stream appends and cuts meanwhile, and it
/// ends marked sealed, every chunk counted converted and nothing named in the clear.
#[test]
fn a_cold_directory_in_the_clear_is_converted_while_the_stream_goes_on() {
    let directory = tempfile::tempdir().unwrap();
    let mut bytes = stream(64 * 9 + 20);
    let (root, mut cold) = store(directory.path(), "cold", None, ContentForm::Clear);
    put(&mut cold, &bytes);
    let (state, pending) = cold.capture().unwrap();
    ColdStore::make_durable(&root, &pending).unwrap();
    drop(cold);
    let clear = clear_names(&root);
    assert_eq!(clear, 11, "nine chunks and two manifests");
    let (_, mut cold) = store(directory.path(), "cold", Some(&state), ContentForm::Sealed);
    assert!(root.join(sealing::SEALING).exists());
    let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
    let mut steps = 0;
    loop {
        assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes, "step {steps}");
        if steps == 2 {
            let more = stream(64 * 3 + 5);
            put(&mut cold, &more);
            bytes.extend_from_slice(&more);
        }
        if cold.convert_step(&mut scratch, 2).unwrap() == Conversion::Done {
            break;
        }
        steps += 1;
        assert!(steps < 1000, "the conversion ended");
    }
    let status = cold.conversion_status();
    assert_eq!(status.form, crate::StoreForm::Sealed);
    assert_eq!(status.counted.files, clear as u64);
    assert_eq!(status.converted.files, clear as u64);
    assert_eq!(status.left().files, 0);
    assert!(root.join(sealing::SEALED).exists());
    assert_eq!(clear_names(&root), 0);
    assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes);
    // A checkpoint now names every chunk, read back sealed by a reader made after.
    let (state, pending) = cold.capture().unwrap();
    ColdStore::make_durable(&root, &pending).unwrap();
    let reader = ColdReader::open(&root, keys(directory.path())).unwrap();
    let read: Vec<u8> = state
        .chunks(SMALL, &reader)
        .unwrap()
        .iter()
        .flat_map(|hash| reader.read(*hash, 64).unwrap())
        .collect();
    assert_eq!(read, &bytes[..read.len()]);
}

/// A cold directory's conversion cut after any step resumes at the next open, whatever form it
/// asks, and loses nothing.
#[test]
fn a_cold_conversion_cut_at_any_step_resumes_and_loses_nothing() {
    let mut scratch = vec![0u8; SEED_CHUNK_BYTES];
    let mut cut = 0;
    loop {
        let directory = tempfile::tempdir().unwrap();
        let bytes = stream(64 * 6 + 9);
        let (root, mut cold) = store(directory.path(), "cold", None, ContentForm::Clear);
        put(&mut cold, &bytes);
        let (state, pending) = cold.capture().unwrap();
        ColdStore::make_durable(&root, &pending).unwrap();
        drop(cold);
        let (_, mut cold) = store(directory.path(), "cold", Some(&state), ContentForm::Sealed);
        let mut finished = false;
        for _ in 0..cut {
            if cold.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                finished = true;
                break;
            }
        }
        drop(cold);
        if finished {
            break;
        }
        let (_, mut cold) = store(directory.path(), "cold", Some(&state), ContentForm::Clear);
        for _ in 0..1000 {
            if cold.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                break;
            }
        }
        assert_eq!(
            cold.conversion_status().form,
            crate::StoreForm::Sealed,
            "cut {cut}"
        );
        assert_eq!(clear_names(&root), 0, "cut {cut}");
        assert_eq!(cold.read(0, bytes.len()).unwrap(), bytes, "cut {cut}");
        cut += 1;
        assert!(cut < 200, "a conversion of eight chunks ended");
    }
}
