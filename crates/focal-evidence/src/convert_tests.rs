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
    convert_all(&mut store);
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
    drop(store);
    let clear = tempfile::tempdir().unwrap();
    let mut store = open(clear.path(), ContentForm::Clear);
    assert!(!store.converting());
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
