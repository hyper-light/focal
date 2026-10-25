use super::*;
use crate::{Conversion, StoreForm};
use focal_memory::{DiskBudget, DiskBudgetConfig};
use focal_model::ContentClass;

/// Chunks longer than a record, so an append is several records.
const CHUNK: usize = record_file::RECORD_BYTES + 4321;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 4 * CHUNK as u64,
        max_staging_bytes: 8 * CHUNK as u64,
        max_uploads: 4,
        chunk_bytes: CHUNK,
        max_manifest_bytes: 4096,
    }
}
fn open(root: &Path, form: ContentForm) -> ContentStore {
    ContentStore::open_with_disk(
        root,
        limits(),
        DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
        &crate::store::test_keys(root),
        form,
    )
    .unwrap()
}
fn reopen(root: &Path, form: ContentForm) -> Result<ContentStore, ContentError> {
    ContentStore::open_with_disk(
        root,
        limits(),
        DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
        &crate::store::test_keys(root),
        form,
    )
}
/// Bytes no stored byte repeats by chance: 16 of them anywhere in a sealed file would show.
fn payload(seed: u8, length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| (index.wrapping_mul(37) ^ (index >> 7)) as u8 ^ seed)
        .collect()
}
fn begin(store: &mut ContentStore, id: UploadId, length: usize) {
    begin_in(store, id, length, ContentDomainId([9; 16]));
}
fn begin_in(store: &mut ContentStore, id: UploadId, length: usize, domain: ContentDomainId) {
    store
        .begin(id, domain, ContentClass::Evidence, length as u64, None)
        .unwrap();
}
/// Append `bytes` from `from` in chunks, as a client does.
fn append(store: &mut ContentStore, id: UploadId, bytes: &[u8], from: usize) {
    let mut offset = from;
    for piece in bytes[from..].chunks(CHUNK) {
        store.append(id, offset as u64, piece).unwrap();
        offset += piece.len();
    }
}
/// Staging's files in the clear: nothing whose name is an upload's identity in hex, nothing that
/// holds 16 bytes of `plaintext` or an upload's identity.
fn assert_nothing_in_the_clear(root: &Path, ids: &[UploadId], plaintext: &[u8]) {
    for entry in fs::read_dir(root.join("staging")).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let stored = crate::store::stored_bytes(&path);
        for id in ids {
            assert!(!name.contains(&hex(&id.0)), "{name} names an upload");
            assert!(
                !stored.windows(16).any(|window| window == id.0),
                "{name} holds an upload's identity"
            );
        }
        for probe in plaintext.chunks(4096).map(|piece| &piece[..16]) {
            assert!(
                !stored.windows(16).any(|window| window == probe),
                "{name} holds plaintext"
            );
        }
    }
}

/// Do: stage an upload sealed, stop the store halfway, open it again, finish the upload. Expect:
/// it goes on at the bytes it held, seals to what was sent, and at every point no file of staging
/// is named by the upload or holds its bytes or its identity; finished, its terminal record fences
/// it across a restart.
#[test]
fn an_upload_staged_sealed_resumes_after_a_restart_and_nothing_lies_in_the_clear() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let id = UploadId([0x41; 16]);
    let bytes = payload(1, 3 * CHUNK + 1000);
    let mut store = open(root, ContentForm::Sealed);
    begin(&mut store, id, bytes.len());
    append(&mut store, id, &bytes[..2 * CHUNK], 0);
    assert_nothing_in_the_clear(root, &[id], &bytes);
    drop(store);
    let mut store = open(root, ContentForm::Sealed);
    assert_eq!(store.offset(id).unwrap(), 2 * CHUNK as u64);
    // A retry of bytes already held is answered with the offset; other bytes there are refused.
    assert_eq!(
        store
            .append(id, CHUNK as u64, &bytes[CHUNK..2 * CHUNK])
            .unwrap(),
        2 * CHUNK as u64
    );
    assert!(matches!(
        store.append(id, CHUNK as u64, &payload(2, CHUNK)),
        Err(ContentError::Corrupt)
    ));
    append(&mut store, id, &bytes, 2 * CHUNK);
    let reference = store.seal(id).unwrap();
    assert_eq!(store.read_bytes(&reference, bytes.len()).unwrap(), bytes);
    assert_nothing_in_the_clear(root, &[id], &bytes);
    store.finish(id).unwrap();
    assert!(matches!(
        store.staged_record_of(id),
        Some(Staged::Terminal(found)) if found == id
    ));
    drop(store);
    let mut store = open(root, ContentForm::Sealed);
    assert!(matches!(
        store.begin(
            id,
            ContentDomainId([9; 16]),
            ContentClass::Evidence,
            4,
            None
        ),
        Err(ContentError::FinishedUpload)
    ));
    assert_nothing_in_the_clear(root, &[id], &bytes);
}

/// Do: cut a part's last append short (its last record half written, as a crash between write and
/// flush leaves it), then open the store. Expect: the upload goes on at its whole records' bytes,
/// its part written again under a new key, and seals to what was sent.
#[test]
fn a_part_whose_last_append_was_cut_short_goes_on_under_a_new_key() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let id = UploadId([0x42; 16]);
    let bytes = payload(3, 2 * CHUNK + 77);
    let mut store = open(root, ContentForm::Sealed);
    begin(&mut store, id, bytes.len());
    append(&mut store, id, &bytes[..2 * CHUNK], 0);
    let part = store.part_path(id);
    drop(store);
    let header = |path: &Path| fs::read(path).unwrap()[8..24].to_vec();
    let before = header(&part);
    let length = fs::metadata(&part).unwrap().len();
    // The second append's last record, half there.
    OpenOptions::new()
        .write(true)
        .open(&part)
        .unwrap()
        .set_len(length - 2000)
        .unwrap();
    let mut store = open(root, ContentForm::Sealed);
    let held = store.offset(id).unwrap();
    assert!(held >= CHUNK as u64 && held < 2 * CHUNK as u64, "{held}");
    assert_ne!(header(&store.part_path(id)), before, "a new key");
    append(&mut store, id, &bytes, held as usize);
    let reference = store.seal(id).unwrap();
    assert_eq!(store.read_bytes(&reference, bytes.len()).unwrap(), bytes);
}

/// Do: change staging's sealed files as an attacker can: flip a byte of a part behind its last
/// append, move a part under another upload's name, put one upload's metadata under another's.
/// Expect: each refused when the store opens.
#[test]
fn a_changed_or_moved_staging_file_is_refused() {
    let stage = |root: &Path| {
        let mut store = open(root, ContentForm::Sealed);
        let (first, second) = (UploadId([0x51; 16]), UploadId([0x52; 16]));
        // Each in a domain of its own: a domain's staging allowance is half the whole.
        for (id, seed) in [(first, 5), (second, 6)] {
            let bytes = payload(seed, 3 * CHUNK);
            begin_in(&mut store, id, bytes.len(), ContentDomainId([seed; 16]));
            append(&mut store, id, &bytes, 0);
        }
        let paths = (
            (store.part_path(first), store.meta_path(first)),
            (store.part_path(second), store.meta_path(second)),
        );
        drop(store);
        paths
    };
    // A byte flipped in the first record: two appends lie past it.
    let directory = tempfile::tempdir().unwrap();
    let ((part, _), _) = stage(directory.path());
    let mut stored = fs::read(&part).unwrap();
    stored[record_file::PART_HEADER + 30] ^= 1;
    fs::write(&part, &stored).unwrap();
    assert!(matches!(
        reopen(directory.path(), ContentForm::Sealed),
        Err(ContentError::Corrupt)
    ));
    // One upload's part under the other's name: kept for its own upload, it opens to nothing.
    let directory = tempfile::tempdir().unwrap();
    let ((first_part, _), (second_part, _)) = stage(directory.path());
    fs::copy(&first_part, &second_part).unwrap();
    assert!(reopen(directory.path(), ContentForm::Sealed).is_err());
    // One upload's metadata under the other's name.
    let directory = tempfile::tempdir().unwrap();
    let ((_, first_meta), (_, second_meta)) = stage(directory.path());
    fs::copy(&first_meta, &second_meta).unwrap();
    assert!(matches!(
        reopen(directory.path(), ContentForm::Sealed),
        Err(ContentError::Corrupt)
    ));
}

/// Do: leave what a crash leaves of a sealed terminal record's write: its temporary synced and not
/// renamed, then the record renamed into place. Expect: the temporary goes at the next open and the
/// upload is still staged; once renamed, its identity is fenced.
#[test]
fn a_sealed_terminal_records_crash_windows_recover() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let id = UploadId([0x61; 16]);
    let mut store = open(root, ContentForm::Sealed);
    begin(&mut store, id, 100);
    store.append(id, 0, &payload(7, 60)).unwrap();
    let meta = store.meta_path(id);
    // The terminal record written beside the metadata as the sealed install writes it, never
    // renamed.
    let temporary = meta.with_extension("install");
    store
        .staging_key(true)
        .unwrap()
        .install(&temporary, &terminal::encode(id).unwrap())
        .unwrap();
    assert!(temporary.exists());
    drop(store);
    let mut store = open(root, ContentForm::Sealed);
    assert!(!temporary.exists(), "the temporary went");
    assert_eq!(store.offset(id).unwrap(), 60);
    store.finish(id).unwrap();
    drop(store);
    let mut store = open(root, ContentForm::Sealed);
    assert!(matches!(
        store.begin(
            id,
            ContentDomainId([9; 16]),
            ContentClass::Evidence,
            100,
            None
        ),
        Err(ContentError::FinishedUpload)
    ));
}

/// What a store in the clear holds in staging: an upload half sent, and one finished.
fn stage_in_the_clear(root: &Path) -> (UploadId, Vec<u8>, UploadId) {
    let mut store = open(root, ContentForm::Clear);
    let (active, done) = (UploadId([0x71; 16]), UploadId([0x72; 16]));
    let bytes = payload(8, 3 * CHUNK + 10);
    begin(&mut store, active, bytes.len());
    append(&mut store, active, &bytes[..CHUNK + 5], 0);
    let finished = payload(9, 50);
    begin(&mut store, done, finished.len());
    store.append(done, 0, &finished).unwrap();
    store.seal(done).unwrap();
    store.finish(done).unwrap();
    drop(store);
    (active, bytes, done)
}

/// The files in the clear the conversion counts and their bytes: the objects' chunks and manifests
/// named by their hashes, and staging's records and parts.
fn clear_files(root: &Path) -> crate::Tally {
    let mut tally = fs::read_dir(root.join("staging"))
        .unwrap()
        .fold(crate::Tally::default(), |tally, entry| {
            tally.plus(counted_staging(&entry.unwrap().path()).unwrap())
        });
    let mut stack = vec![root.join("objects")];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                stack.push(path);
                continue;
            }
            let extension = path.extension().and_then(|x| x.to_str()).unwrap_or("");
            let stem = path.file_stem().unwrap().to_str().unwrap();
            if (extension == "chunk" || extension == "manifest") && stem.len() == 64 {
                tally.add(entry.metadata().unwrap().len());
            }
        }
    }
    tally
}

/// Do: open a store in the clear with an upload half sent and one finished as sealing, and convert
/// it while the upload goes on between the steps. Expect: every look at the tally agrees with
/// staging's files in the clear on the volume; it ends sealed, every file counted converted, the
/// upload goes on in its sealed part and seals to what was sent, and nothing in staging lies in the
/// clear.
#[test]
fn staging_in_the_clear_is_converted_while_its_upload_goes_on() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let (active, bytes, done) = stage_in_the_clear(root);
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0u8; crate::MAX_TRANSFER_CHUNK_BYTES];
    let mut sent = CHUNK + 5;
    for _ in 0..10_000 {
        let step = store.convert_step(&mut scratch, 1).unwrap();
        let status = store.conversion_status();
        if status.form == StoreForm::Converting {
            assert_eq!(status.left(), clear_files(root), "{status:?}");
        }
        if step == Conversion::Done {
            break;
        }
        assert_ne!(step, Conversion::Held);
        // The upload goes on between the steps, a chunk at a time.
        if sent < 2 * CHUNK + 5 {
            store
                .append(active, sent as u64, &bytes[sent..sent + CHUNK])
                .unwrap();
            sent += CHUNK;
        }
    }
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.left(), crate::Tally::default());
    assert_eq!(status.converted.plus(status.gone), status.counted);
    assert!(matches!(
        store.uploads.get(&active).unwrap().part,
        Part::Sealed(_)
    ));
    append(&mut store, active, &bytes, sent);
    let reference = store.seal(active).unwrap();
    assert_eq!(store.read_bytes(&reference, bytes.len()).unwrap(), bytes);
    assert_nothing_in_the_clear(root, &[active, done], &bytes);
    drop(store);
    let mut store = open(root, ContentForm::Sealed);
    assert!(matches!(
        store.begin(
            done,
            ContentDomainId([9; 16]),
            ContentClass::Evidence,
            50,
            None
        ),
        Err(ContentError::FinishedUpload)
    ));
}

/// Do: cut staging's conversion after each step in turn (the store dropped and opened again).
/// Expect: each resumes, ends sealed, and the upload staged in the clear is whole and goes on.
#[test]
fn staging_conversion_cut_at_any_step_resumes() {
    let mut scratch = vec![0u8; crate::MAX_TRANSFER_CHUNK_BYTES];
    // Every cut until one falls after the conversion's last step: the loop ends there, not at its
    // bound.
    let mut covered = None;
    for cut in 0..200 {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let (active, bytes, done) = stage_in_the_clear(root);
        let mut store = open(root, ContentForm::Sealed);
        let mut finished = false;
        for _ in 0..cut {
            if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                finished = true;
                break;
            }
        }
        drop(store);
        let mut store = open(root, ContentForm::Clear);
        for _ in 0..10_000 {
            if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
                break;
            }
        }
        assert_eq!(
            store.conversion_status().form,
            StoreForm::Sealed,
            "cut {cut}"
        );
        assert_eq!(
            store.offset(active).unwrap(),
            (CHUNK + 5) as u64,
            "cut {cut}"
        );
        append(&mut store, active, &bytes, CHUNK + 5);
        let reference = store.seal(active).unwrap();
        assert_eq!(
            store.read_bytes(&reference, bytes.len()).unwrap(),
            bytes,
            "cut {cut}"
        );
        assert_nothing_in_the_clear(root, &[active, done], &bytes);
        if finished {
            covered = Some(cut);
            break;
        }
    }
    assert!(
        covered.is_some(),
        "the conversion did not end within the cuts tried"
    );
}
