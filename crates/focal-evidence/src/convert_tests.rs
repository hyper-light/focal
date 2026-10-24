use super::super::hex;
use super::*;
use crate::{CollectorConfig, ContentForm, ContentReader, ProtectionSet, StoreLimits, UploadId};
use focal_memory::{DiskBudget, DiskBudgetConfig};
use focal_model::{ContentClass, ContentHash, ContentRef};
use std::time::{Duration, SystemTime};

const DAY: u64 = 24 * 60 * 60 * 1000;
const CHUNK: usize = 64;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 4096,
        max_staging_bytes: 8192,
        max_uploads: 4,
        chunk_bytes: CHUNK,
        max_manifest_bytes: 4096,
    }
}
fn open(root: &Path, form: ContentForm) -> ContentStore {
    open_on(
        root,
        form,
        DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
    )
}
fn open_on(root: &Path, form: ContentForm, disk: DiskBudget) -> ContentStore {
    ContentStore::open_with_disk(root, limits(), disk, &crate::store::test_keys(root), form)
        .unwrap()
}
fn domain(byte: u8) -> ContentDomainId {
    ContentDomainId([byte; 16])
}
/// Bytes no ciphertext repeats by chance: 16 of them anywhere in a sealed file would show.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(29).wrapping_add(seed) ^ ((i >> 5) as u8))
        .collect()
}
fn seal(store: &mut ContentStore, id: u8, domain: ContentDomainId, bytes: &[u8]) -> ContentRef {
    let upload = UploadId([id; 16]);
    store
        .begin(
            upload,
            domain,
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    for (index, part) in bytes.chunks(CHUNK).enumerate() {
        store.append(upload, (index * CHUNK) as u64, part).unwrap();
    }
    let reference = store.seal(upload).unwrap();
    store.finish(upload).unwrap();
    reference
}
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
/// Every file under `root` last touched two days ago.
fn age(root: &Path) {
    let when = SystemTime::now() - Duration::from_millis(2 * DAY);
    let mut stack = vec![root.to_path_buf()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                stack.push(entry.path());
            } else {
                File::options()
                    .write(true)
                    .open(entry.path())
                    .unwrap()
                    .set_modified(when)
                    .unwrap();
            }
        }
    }
}
fn collect(store: &mut ContentStore, protection: &ProtectionSet) -> crate::CollectorReport {
    let config = CollectorConfig {
        grace_ms: DAY,
        quarantine_ms: 7 * DAY,
        terminal_grace_ms: 7 * DAY,
        keep_records: 2,
        max_marks: 1 << 16,
    };
    for _ in 0..10_000 {
        let report = store
            .collect_step(protection, config, now_ms(), 64)
            .unwrap();
        if report.complete {
            return report;
        }
    }
    panic!("the pass never completed");
}
/// Every object file under the store's objects and its quarantine: its path, stem and bytes.
fn object_files(root: &Path) -> Vec<(PathBuf, String, Vec<u8>)> {
    let mut found = Vec::new();
    let mut stack = vec![root.join("objects"), root.join("quarantine")];
    while let Some(directory) = stack.pop() {
        let Ok(listing) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in listing {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                stack.push(path);
                continue;
            }
            let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
            if extension == "chunk" || extension == "manifest" {
                let stem = path.file_stem().unwrap().to_str().unwrap().to_owned();
                let bytes = fs::read(&path).unwrap();
                found.push((path, stem, bytes));
            }
        }
    }
    found
}
fn copy_tree(from: &Path, to: &Path) {
    let mut stack = vec![(from.to_path_buf(), to.to_path_buf())];
    while let Some((source, target)) = stack.pop() {
        fs::create_dir_all(&target).unwrap();
        for entry in fs::read_dir(&source).unwrap() {
            let entry = entry.unwrap();
            let into = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                stack.push((entry.path(), into));
            } else if entry.file_name() != "LOCK" {
                fs::copy(entry.path(), into).unwrap();
            }
        }
    }
}
/// The files in the clear under the store's objects and quarantine, as the conversion counts them,
/// and their bytes.
fn clear_tally(root: &Path) -> Tally {
    let mut tally = Tally::default();
    for (_, stem, bytes) in object_files(root) {
        if stem.len() == 64 {
            tally.add(bytes.len() as u64);
        }
    }
    tally
}
/// The same, under the quarantine alone.
fn clear_in_quarantine(root: &Path) -> Tally {
    let quarantine = root.join("quarantine");
    let mut tally = Tally::default();
    for (path, stem, bytes) in object_files(root) {
        if stem.len() == 64 && path.starts_with(&quarantine) {
            tally.add(bytes.len() as u64);
        }
    }
    tally
}
/// Where the count stands, as a test asks.
type CountAt = fn(&Count) -> bool;
/// Count steps of one visit until the count stands where `at` says.
fn count_until(store: &mut ContentStore, scratch: &mut [u8], at: CountAt) {
    for _ in 0..10_000 {
        let count = store
            .conversion
            .as_ref()
            .and_then(|state| state.count.as_ref())
            .expect("the count ended before it got there");
        if at(count) {
            return;
        }
        assert_eq!(store.convert_step(scratch, 1).unwrap(), Conversion::Visited);
    }
    panic!("the count never got there");
}
/// Count steps until the count is whole.
fn count_all(store: &mut ContentStore, scratch: &mut [u8]) {
    for _ in 0..10_000 {
        if store.conversion_status().form != StoreForm::Counting {
            return;
        }
        assert_eq!(store.convert_step(scratch, 1).unwrap(), Conversion::Visited);
    }
    panic!("the count never ended");
}
fn collect_at(
    store: &mut ContentStore,
    protection: &ProtectionSet,
    now: u64,
) -> crate::CollectorReport {
    let config = CollectorConfig {
        grace_ms: DAY,
        quarantine_ms: 7 * DAY,
        terminal_grace_ms: 7 * DAY,
        keep_records: 2,
        max_marks: 1 << 16,
    };
    for _ in 0..10_000 {
        let report = store.collect_step(protection, config, now, 64).unwrap();
        if report.complete {
            return report;
        }
    }
    panic!("the pass never completed");
}
fn convert_all(store: &mut ContentStore) -> usize {
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    for steps in 0..10_000 {
        match store.convert_step(&mut scratch, 1).unwrap() {
            Conversion::Done => return steps,
            Conversion::Held => panic!("held with room"),
            Conversion::Converted | Conversion::Visited => {}
        }
    }
    panic!("the conversion never ended");
}

/// What a store below the fence holds: three objects in two domains, one of them quarantined by
/// the collector, each written in the clear.
struct Laid {
    live: Vec<(ContentRef, Vec<u8>)>,
    quarantined: (ContentRef, Vec<u8>),
}
fn lay_out_clear(root: &Path) -> Laid {
    let mut store = open(root, ContentForm::Clear);
    assert!(!store.converting());
    let one = payload(1, 3 * CHUNK + 9);
    let two = payload(2, 2 * CHUNK);
    let three = payload(3, CHUNK + 1);
    let one_ref = seal(&mut store, 1, domain(1), &one);
    let two_ref = seal(&mut store, 2, domain(2), &two);
    let three_ref = seal(&mut store, 3, domain(1), &three);
    let mut protection = ProtectionSet::new();
    protection.protect_object(domain(1), one_ref.root).unwrap();
    protection.protect_object(domain(2), two_ref.root).unwrap();
    protection.finish();
    age(&root.join("objects"));
    let report = collect(&mut store, &protection);
    assert_eq!(report.objects_quarantined, 1, "{report:?}");
    // The layout every earlier binary wrote and reads: each file in the clear, named by its hash,
    // its bytes the bytes it names; no key record, no marker.
    let files = object_files(root);
    assert!(!files.is_empty());
    for (path, stem, bytes) in &files {
        assert_eq!(stem.len(), 64, "{path:?}");
        assert_eq!(
            ContentHash(*blake3::hash(bytes).as_bytes()).to_string(),
            *stem
        );
    }
    assert!(!root.join(sealing::SEALING).exists());
    assert!(!root.join(sealing::SEALED).exists());
    assert!(
        !root
            .join("objects")
            .join(hex(&domain(1).0))
            .join(sealing::DOMAIN_KEY)
            .exists()
    );
    Laid {
        live: vec![(one_ref, one), (two_ref, two)],
        quarantined: (three_ref, three),
    }
}
/// Nothing under the store's objects or quarantine holds a name in the clear or 16 bytes of any
/// object's plaintext.
fn assert_sealed_through(root: &Path, plaintexts: &[&[u8]]) {
    assert!(root.join(sealing::SEALED).exists());
    assert!(!root.join(sealing::SEALING).exists());
    for (path, stem, bytes) in object_files(root) {
        assert_eq!(stem.len(), 32, "{path:?} is named in the clear");
        for plaintext in plaintexts {
            for window in plaintext.chunks(16).filter(|window| window.len() == 16) {
                assert!(
                    !bytes.windows(16).any(|stored| stored == window),
                    "{path:?} holds plaintext"
                );
            }
        }
    }
}

/// A store in the clear, asked to seal, is marked and converted a file a step while it serves:
/// every object reads at every step through the store, a reader made before the conversion and
/// one made at that step; an object sealed meanwhile is sealed at once; the collector leaves the
/// objects alone until the conversion ends and collects again after; the quarantined object comes
/// back sealed; and the store opened again in the clear stays sealed.
#[test]
fn a_store_in_the_clear_is_converted_while_it_serves() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let Laid {
        live,
        quarantined: (gone, gone_bytes),
    } = lay_out_clear(root);
    let keys = crate::store::test_keys(root);
    let on_disk = clear_tally(root);
    let mut store = open(root, ContentForm::Sealed);
    assert!(store.converting());
    assert!(root.join(sealing::SEALING).exists());
    let before = store.reader().unwrap();
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    let mut late = None;
    let mut protection = ProtectionSet::new();
    protection.finish();
    for step in 0..10_000 {
        for (reference, bytes) in &live {
            assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes, "{step}");
            assert_eq!(&before.read_bytes(reference, 4096).unwrap(), bytes);
            let now = ContentReader::open(root, keys.clone()).unwrap();
            assert_eq!(&now.read_bytes(reference, 4096).unwrap(), bytes);
            assert_eq!(
                now.manifest_length(reference).unwrap(),
                store.manifest_length(reference).unwrap()
            );
        }
        if step == 3 {
            // Sealed as it is written, under the domain's keyed names.
            let bytes = payload(4, 2 * CHUNK + 3);
            let reference = seal(&mut store, 4, domain(2), &bytes);
            let manifest = store.manifest_file(domain(2), reference.root);
            assert_eq!(manifest.file_stem().unwrap().len(), 32);
            // The collector passes over every domain while the store converts.
            age(&root.join("objects"));
            let report = collect(&mut store, &protection);
            assert_eq!(report.objects_quarantined, 0, "{report:?}");
            assert_eq!(report.chunks_quarantined, 0);
            late = Some((reference, bytes));
        }
        match store.convert_step(&mut scratch, 1).unwrap() {
            Conversion::Done => break,
            Conversion::Held => panic!("held with room"),
            Conversion::Converted | Conversion::Visited => {}
        }
    }
    let (late, late_bytes) = late.unwrap();
    assert!(!store.converting());
    // An object sealed while the store converts is not one to convert: the tally closes on what
    // lay in the clear when it began.
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.counted, on_disk);
    assert_eq!(status.converted, on_disk);
    assert_eq!(status.left(), Tally::default());
    assert_eq!(
        store.convert_step(&mut scratch, 1).unwrap(),
        Conversion::Done
    );
    let mut plaintexts: Vec<&[u8]> = live.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    plaintexts.push(&gone_bytes);
    plaintexts.push(&late_bytes);
    assert_sealed_through(root, &plaintexts);
    for (reference, bytes) in live
        .iter()
        .chain([(late.clone(), late_bytes.clone())].iter())
    {
        assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes);
        assert_eq!(&before.read_bytes(reference, 4096).unwrap(), bytes);
    }
    // Quarantined in the clear, converted where it lay, and back whole.
    assert!(store.restore_quarantined(domain(1), gone.root).unwrap());
    assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes);
    // The collector collects again: with nothing protected, every aged object leaves.
    age(&root.join("objects"));
    let report = collect(&mut store, &protection);
    assert_eq!(report.objects_quarantined, 4, "{report:?}");
    drop(store);
    // Marked sealed, the store stays sealed whatever a start asks.
    let mut store = open(root, ContentForm::Clear);
    assert!(!store.converting());
    assert!(store.restore_quarantined(domain(2), late.root).unwrap());
    assert_eq!(store.read_bytes(&late, 4096).unwrap(), late_bytes);
    let more = payload(5, CHUNK);
    let reference = seal(&mut store, 5, domain(3), &more);
    assert_eq!(
        store
            .manifest_file(domain(3), reference.root)
            .file_stem()
            .unwrap()
            .len(),
        32
    );
    assert_sealed_through(root, &[&more, &late_bytes]);
}

/// A conversion cut at any step resumes and loses nothing: cut between files; cut inside one, its
/// sealed form written and its name in the clear still there; and with a sealed form damaged
/// beside the bytes in the clear, which are sealed over it. The start after the cut asks for the
/// clear, and the conversion begun finishes anyway; every object reads before it resumes.
#[test]
fn a_conversion_cut_at_any_step_resumes_and_loses_nothing() {
    let pristine = tempfile::tempdir().unwrap();
    let Laid {
        live,
        quarantined: (gone, gone_bytes),
    } = lay_out_clear(pristine.path());
    let clear_files = object_files(pristine.path());
    let steps = {
        let whole = tempfile::tempdir().unwrap();
        copy_tree(pristine.path(), whole.path());
        let mut store = open(whole.path(), ContentForm::Sealed);
        convert_all(&mut store)
    };
    assert!(steps > clear_files.len(), "{steps} steps");
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    for cut in 0..=steps {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path();
        copy_tree(pristine.path(), root);
        let mut store = open(root, ContentForm::Sealed);
        for _ in 0..cut {
            store.convert_step(&mut scratch, 1).unwrap();
        }
        drop(store);
        if cut % 2 == 1 {
            // Cut inside each file converted so far: its name in the clear back beside its sealed
            // form, as a crash between the seal and the removal leaves it.
            for (path, _, bytes) in &clear_files {
                let at = root.join(path.strip_prefix(pristine.path()).unwrap());
                if !at.exists() {
                    fs::write(&at, bytes).unwrap();
                }
            }
        }
        if cut % 3 == 2 {
            // A sealed form damaged beside its bytes in the clear.
            if let Some((sealed, _, bytes)) = object_files(root)
                .into_iter()
                .find(|(_, stem, _)| stem.len() == 32)
            {
                let mut damaged = bytes.clone();
                let at = damaged.len() / 2;
                damaged[at] ^= 1;
                fs::write(&sealed, &damaged).unwrap();
                for (path, _, bytes) in &clear_files {
                    let at = root.join(path.strip_prefix(pristine.path()).unwrap());
                    if !at.exists() {
                        fs::write(&at, bytes).unwrap();
                    }
                }
            }
        }
        let at_reopen = clear_tally(root);
        let mut store = open(root, ContentForm::Clear);
        assert!(store.converting() || cut == steps, "cut {cut}");
        for (reference, bytes) in &live {
            assert_eq!(
                &store.read_bytes(reference, 4096).unwrap(),
                bytes,
                "cut {cut}"
            );
        }
        convert_all(&mut store);
        // A start that resumes counts what is left in the clear and converts exactly that.
        let status = store.conversion_status();
        assert_eq!(status.form, StoreForm::Sealed, "cut {cut}");
        if cut < steps {
            assert!(status.resumed, "cut {cut}");
            assert_eq!(status.counted, at_reopen, "cut {cut}");
            assert_eq!(status.converted, at_reopen, "cut {cut}");
        }
        assert_eq!(status.left(), Tally::default(), "cut {cut}");
        for (reference, bytes) in &live {
            assert_eq!(
                &store.read_bytes(reference, 4096).unwrap(),
                bytes,
                "cut {cut}"
            );
        }
        let mut plaintexts: Vec<&[u8]> = live.iter().map(|(_, bytes)| bytes.as_slice()).collect();
        plaintexts.push(&gone_bytes);
        assert_sealed_through(root, &plaintexts);
        assert!(store.restore_quarantined(domain(1), gone.root).unwrap());
        assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes);
    }
}

/// A file whose sealed form the volume has no room for holds the conversion: nothing written, the
/// same file taken first once room comes, and the store reads throughout.
#[test]
fn a_conversion_without_room_holds_and_takes_the_same_file_next() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let Laid { live, .. } = lay_out_clear(root);
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: 64,
    })
    .unwrap();
    disk.observe(0);
    let mut store = open_on(root, ContentForm::Sealed, disk.clone());
    assert!(store.converting());
    let before = object_files(root);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    let mut held = 0;
    for _ in 0..64 {
        match store.convert_step(&mut scratch, 1).unwrap() {
            Conversion::Held => held += 1,
            Conversion::Visited => {}
            other => panic!("{other:?} without room"),
        }
    }
    assert!(held > 0);
    let status = store.conversion_status();
    assert!(status.held);
    assert_eq!(status.held_steps, held);
    assert_eq!(status.converted, Tally::default());
    let now = object_files(root);
    assert_eq!(now.len(), before.len());
    assert!(now.iter().all(|(_, stem, _)| stem.len() == 64));
    assert_eq!(disk.stats().outstanding, 0);
    for (reference, bytes) in &live {
        assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes);
    }
    disk.observe(u64::MAX / 4);
    assert_eq!(
        store.convert_step(&mut scratch, 1).unwrap(),
        Conversion::Converted
    );
    let status = store.conversion_status();
    assert!(!status.held);
    assert_eq!(status.held_steps, held);
    assert_eq!(status.converted.files, 1);
    convert_all(&mut store);
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.left(), Tally::default());
    let plaintexts: Vec<&[u8]> = live.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    assert_sealed_through(root, &plaintexts);
    for (reference, bytes) in &live {
        assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes);
    }
}

/// A store that holds nothing is sealed as it opens; one opened in the clear makes no marker and
/// no key record, and its files open with a binary that knows only the clear.
#[test]
fn an_empty_store_is_sealed_at_once_and_one_in_the_clear_stays_so() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let store = open(root, ContentForm::Sealed);
    assert!(!store.converting());
    assert!(root.join(sealing::SEALED).exists());
    assert!(!root.join(sealing::SEALING).exists());
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.counted, Tally::default());
    assert!(status.finished_ms >= status.started_ms && status.started_ms > 0);
    drop(store);
    let clear = tempfile::tempdir().unwrap();
    let mut store = open(clear.path(), ContentForm::Clear);
    assert!(!store.converting());
    assert_eq!(store.conversion_status().form, StoreForm::Clear);
    let bytes = payload(9, CHUNK * 2);
    let reference = seal(&mut store, 9, domain(9), &bytes);
    drop(store);
    let mut store = open(clear.path(), ContentForm::Clear);
    assert!(!clear.path().join(sealing::SEALED).exists());
    assert!(!clear.path().join(sealing::SEALING).exists());
    assert_eq!(store.read_bytes(&reference, 4096).unwrap(), bytes);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    assert_eq!(
        store.convert_step(&mut scratch, 4).unwrap(),
        Conversion::Done
    );
    // What a binary below the content level reads: the manifest by its root, in the clear.
    let manifest = clear
        .path()
        .join("objects")
        .join(hex(&domain(9).0))
        .join(format!("{}.manifest", reference.root));
    let encoded = fs::read(manifest).unwrap();
    assert_eq!(
        ContentHash(*blake3::hash(&encoded).as_bytes()),
        reference.root
    );
}

/// Do: a store laid out in the clear opened to seal, counted then converted a visit a step.
/// Expect: the form reads clear below the fence, then counting, converting and sealed; the count
/// only grows and ends at exactly the files and bytes in the clear on disk; each step converts at
/// most one file, and the conversion ends with every counted file converted, none left.
#[test]
fn a_conversion_counts_what_it_must_convert_and_tallies_each_file_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let Laid { live, .. } = lay_out_clear(root);
    let clear = open(root, ContentForm::Clear);
    let status = clear.conversion_status();
    assert_eq!(status.form, StoreForm::Clear);
    assert_eq!(status.counted, Tally::default());
    drop(clear);
    let on_disk = clear_tally(root);
    assert!(on_disk.files > 0);
    let mut store = open(root, ContentForm::Sealed);
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Counting);
    assert!(!status.resumed);
    assert!(status.started_ms > 0);
    assert_eq!(status.finished_ms, 0);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    let mut last = Tally::default();
    while store.conversion_status().form == StoreForm::Counting {
        assert_eq!(
            store.convert_step(&mut scratch, 1).unwrap(),
            Conversion::Visited
        );
        let counted = store.conversion_status().counted;
        assert!(counted.files >= last.files && counted.bytes >= last.bytes);
        last = counted;
    }
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Converting);
    assert_eq!(status.counted, on_disk);
    assert_eq!(status.left(), on_disk);
    loop {
        let before = store.conversion_status();
        let step = store.convert_step(&mut scratch, 1).unwrap();
        let after = store.conversion_status();
        match step {
            Conversion::Done => break,
            Conversion::Converted => assert_eq!(after.converted.files, before.converted.files + 1),
            Conversion::Visited => assert_eq!(after.converted, before.converted),
            Conversion::Held => panic!("held with room"),
        }
        assert_eq!(after.left(), on_disk.less(after.converted));
    }
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.converted, on_disk);
    assert_eq!(status.gone, Tally::default());
    assert_eq!(status.left(), Tally::default());
    assert!(status.finished_ms >= status.started_ms);
    assert_eq!(status.stopped, None);
    assert!(!status.held);
    for (reference, bytes) in &live {
        assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes);
    }
    drop(store);
    let store = open(root, ContentForm::Sealed);
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.counted, Tally::default());
}

/// Do: while the count runs, restore the quarantined object at three places: while its round's
/// domain is listed, once its round is counted and the store's own objects are not, and while the
/// domain of the store's own it comes back to is listed (POSIX leaves a listing under change
/// unspecified). Expect: each file counted once, the count whole at exactly the files in the clear
/// on disk, and every counted file converted.
#[test]
fn a_restore_while_the_count_runs_leaves_every_file_counted_once() {
    let pristine = tempfile::tempdir().unwrap();
    let Laid {
        quarantined: (gone, gone_bytes),
        ..
    } = lay_out_clear(pristine.path());
    let on_disk = clear_tally(pristine.path());
    let cases: [(&str, CountAt); 3] = [
        ("its round's domain listed", |count| {
            count.tree == 0
                && count
                    .listing
                    .as_ref()
                    .is_some_and(|listing| listing.partial.files > 0)
        }),
        ("its round counted, the store's own ahead", |count| {
            count.tree == count.rounds.len() && count.domains.is_none()
        }),
        ("the store's domain listed", |count| {
            count.tree == count.rounds.len()
                && count
                    .domains
                    .as_ref()
                    .and_then(|domains| domains.get(count.domain))
                    == Some(&domain(1))
                && count
                    .listing
                    .as_ref()
                    .is_some_and(|listing| listing.partial.files > 0)
        }),
    ];
    for (case, at) in cases {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path();
        copy_tree(pristine.path(), root);
        let mut store = open(root, ContentForm::Sealed);
        let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
        count_until(&mut store, &mut scratch, at);
        assert!(
            store.restore_quarantined(domain(1), gone.root).unwrap(),
            "{case}"
        );
        count_all(&mut store, &mut scratch);
        assert_eq!(store.conversion_status().counted, on_disk, "{case}");
        convert_all(&mut store);
        let status = store.conversion_status();
        assert_eq!(status.form, StoreForm::Sealed, "{case}");
        assert_eq!(status.converted, on_disk, "{case}");
        assert_eq!(status.gone, Tally::default(), "{case}");
        assert_eq!(status.left(), Tally::default(), "{case}");
        assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes, "{case}");
    }
}

/// Do: run the collector past the quarantine's grace while the store counts, then again once it
/// has counted. Expect: no round deleted while it counts; once it has, the expired round goes and
/// its files in the clear are tallied as gone, and the conversion ends with every counted file
/// converted or gone, none left.
#[test]
fn a_round_that_expires_during_the_conversion_is_tallied_as_gone() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let Laid { live, .. } = lay_out_clear(root);
    let on_disk = clear_tally(root);
    let in_round = clear_in_quarantine(root);
    assert!(in_round.files > 0);
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    assert_eq!(
        store.convert_step(&mut scratch, 1).unwrap(),
        Conversion::Visited
    );
    assert_eq!(store.conversion_status().form, StoreForm::Counting);
    let mut protection = ProtectionSet::new();
    protection.finish();
    let later = now_ms() + 8 * DAY;
    let report = collect_at(&mut store, &protection, later);
    assert_eq!(report.deleted, 0, "{report:?}");
    assert_eq!(clear_in_quarantine(root), in_round);
    count_all(&mut store, &mut scratch);
    assert_eq!(store.conversion_status().counted, on_disk);
    let report = collect_at(&mut store, &protection, later);
    assert!(report.deleted >= in_round.files, "{report:?}");
    assert_eq!(clear_in_quarantine(root), Tally::default());
    assert_eq!(store.conversion_status().gone, in_round);
    convert_all(&mut store);
    let status = store.conversion_status();
    assert_eq!(status.form, StoreForm::Sealed);
    assert_eq!(status.converted, on_disk.less(in_round));
    assert_eq!(status.gone, in_round);
    assert_eq!(status.left(), Tally::default());
    for (reference, bytes) in &live {
        assert_eq!(&store.read_bytes(reference, 4096).unwrap(), bytes);
    }
}

/// Do: damage a domain's key record before the store converts its first file of the domain.
/// Expect: the step that meets it fails, and the status keeps its cause, the form converting and
/// files left to convert; the store reports no conversion running.
#[test]
fn a_conversion_that_stops_keeps_its_cause_in_the_status() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    lay_out_clear(root);
    fs::write(
        root.join("objects")
            .join(hex(&domain(1).0))
            .join(sealing::DOMAIN_KEY),
        b"no key record",
    )
    .unwrap();
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    let error = (0..10_000)
        .find_map(|_| match store.convert_step(&mut scratch, 1) {
            Ok(Conversion::Done) => panic!("the conversion ended past a damaged record"),
            Ok(_) => None,
            Err(error) => Some(error),
        })
        .expect("no step failed");
    let status = store.conversion_status();
    assert_eq!(status.stopped, Some(error.kind()));
    assert_eq!(status.form, StoreForm::Converting);
    assert!(status.left().files > 0, "{status:?}");
    assert!(!store.converting());
}

/// Do: once the count is whole, restore the quarantined object while the conversion visits its
/// round. Expect: the files the restore moves in the clear are converted where they land, each
/// counted file converted once, none gone or left.
#[test]
fn a_restore_while_the_conversion_visits_the_rounds_converts_each_file_once() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let Laid {
        quarantined: (gone, gone_bytes),
        ..
    } = lay_out_clear(root);
    let on_disk = clear_tally(root);
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    count_all(&mut store, &mut scratch);
    // Into the round's tree, before any of its files is converted.
    for _ in 0..10_000 {
        let state = store.conversion.as_ref().unwrap();
        if state.domain.is_some() {
            break;
        }
        assert_eq!(
            store.convert_step(&mut scratch, 1).unwrap(),
            Conversion::Visited
        );
    }
    let state = store.conversion.as_ref().unwrap();
    assert!(!state.own);
    assert!(
        state
            .domain
            .as_ref()
            .unwrap()
            .directory
            .starts_with(root.join("quarantine"))
    );
    assert_eq!(store.conversion_status().converted, Tally::default());
    assert!(store.restore_quarantined(domain(1), gone.root).unwrap());
    convert_all(&mut store);
    let status = store.conversion_status();
    assert_eq!(status.converted, on_disk);
    assert_eq!(status.gone, Tally::default());
    assert_eq!(status.left(), Tally::default());
    assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes);
}

/// Do: upload the quarantined object's bytes again below the fence, so the store's own objects
/// hold a copy in the clear under the very names its round holds; then restore it from the round
/// (its chunks stand already; its manifest replaces the copy's) while the count has passed that
/// domain, and in another copy of the store once the count is whole. Expect: the replaced manifest
/// leaves the count while it runs, so the count matches the files on disk, and is tallied as gone
/// once it has counted; every counted file is converted or gone, none left.
#[test]
fn a_restore_over_a_copy_in_the_clear_tallies_the_copy_it_replaces() {
    let pristine = tempfile::tempdir().unwrap();
    let Laid {
        quarantined: (gone, gone_bytes),
        ..
    } = lay_out_clear(pristine.path());
    {
        let mut store = open(pristine.path(), ContentForm::Clear);
        let again = seal(&mut store, 9, domain(1), &gone_bytes);
        assert_eq!(again.root, gone.root);
    }
    let manifest = |root: &Path| -> u64 {
        fs::metadata(
            root.join("objects")
                .join(hex(&domain(1).0))
                .join(format!("{}.manifest", gone.root)),
        )
        .unwrap()
        .len()
    };
    let on_disk = clear_tally(pristine.path());
    // While the count runs, past the store's own domain the copy lies in.
    {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path();
        copy_tree(pristine.path(), root);
        let mut store = open(root, ContentForm::Sealed);
        let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
        count_until(&mut store, &mut scratch, |count| {
            count.tree == count.rounds.len()
                && count
                    .domains
                    .as_ref()
                    .and_then(|domains| domains.get(count.domain))
                    .is_some_and(|listed| *listed > domain(1))
        });
        assert!(store.restore_quarantined(domain(1), gone.root).unwrap());
        count_all(&mut store, &mut scratch);
        let after = clear_tally(root);
        assert_eq!(after.files, on_disk.files - 1);
        assert_eq!(store.conversion_status().counted, after);
        convert_all(&mut store);
        let status = store.conversion_status();
        assert_eq!(status.converted, after);
        assert_eq!(status.gone, Tally::default());
        assert_eq!(status.left(), Tally::default());
        assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes);
    }
    // Once the count is whole.
    {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path();
        copy_tree(pristine.path(), root);
        let replaced = manifest(root);
        let mut store = open(root, ContentForm::Sealed);
        let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
        count_all(&mut store, &mut scratch);
        assert_eq!(store.conversion_status().counted, on_disk);
        assert!(store.restore_quarantined(domain(1), gone.root).unwrap());
        assert_eq!(
            store.conversion_status().gone,
            Tally {
                files: 1,
                bytes: replaced
            }
        );
        convert_all(&mut store);
        let status = store.conversion_status();
        assert_eq!(status.converted.files, on_disk.files - 1);
        assert_eq!(status.left(), Tally::default());
        assert_eq!(store.read_bytes(&gone, 4096).unwrap(), gone_bytes);
    }
}

/// Do: plant a link named as a chunk in the clear beside a domain's objects, pointing at one of
/// them. Expect: the count passes over it as the conversion does (it is not a file of the store's),
/// so the conversion ends with every counted file converted and none left, the link as it was.
#[cfg(unix)]
#[test]
fn a_link_named_like_an_object_is_neither_counted_nor_converted() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    lay_out_clear(root);
    let on_disk = clear_tally(root);
    let own = root.join("objects").join(hex(&domain(2).0));
    let target = fs::read_dir(&own)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "chunk")
        })
        .unwrap();
    let link = own.join(format!("{}.chunk", "ab".repeat(32)));
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    count_all(&mut store, &mut scratch);
    assert_eq!(store.conversion_status().counted, on_disk);
    convert_all(&mut store);
    let status = store.conversion_status();
    assert_eq!(status.converted, on_disk);
    assert_eq!(status.left(), Tally::default());
    assert!(
        fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

/// Do: take away the read permission of a chunk in the clear before the conversion reaches it.
/// Expect: the step that opens it fails with the I/O error, which stops the store as any write's
/// does, and the status keeps the cause (an I/O error of that kind) with files left. A test run
/// as a user that reads every file anyway (root) has nothing to inject, and says so.
#[cfg(unix)]
#[test]
fn an_io_failure_in_a_step_stops_the_conversion_with_its_cause() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    lay_out_clear(root);
    let own = root.join("objects").join(hex(&domain(2).0));
    let chunk = fs::read_dir(&own)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "chunk")
        })
        .unwrap();
    fs::set_permissions(&chunk, fs::Permissions::from_mode(0o000)).unwrap();
    if File::open(&chunk).is_ok() {
        eprintln!("this user reads a file without its permission: no failure to inject");
        return;
    }
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0; crate::MAX_TRANSFER_CHUNK_BYTES];
    let error = (0..10_000)
        .find_map(|_| match store.convert_step(&mut scratch, 1) {
            Ok(Conversion::Done) => panic!("the conversion ended past an unreadable chunk"),
            Ok(_) => None,
            Err(error) => Some(error),
        })
        .expect("no step failed");
    assert!(matches!(error, ContentError::Io(_)), "{error:?}");
    let status = store.conversion_status();
    assert_eq!(
        status.stopped,
        Some(ContentErrorKind::Io(std::io::ErrorKind::PermissionDenied))
    );
    assert_eq!(status.form, StoreForm::Converting);
    assert!(status.left().files > 0, "{status:?}");
    assert!(!store.converting());
}
