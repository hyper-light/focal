//! Custody records at rest (29 §12.7): sealed under their kind's key at keyed names, read in either
//! form, refused when changed or carried, converted while the store serves.
use super::*;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 1 << 20,
        max_staging_bytes: 2 << 20,
        max_uploads: 4,
        chunk_bytes: 64 * 1024,
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
/// Bytes no stored byte repeats by chance.
fn payload(seed: u8, length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| (index.wrapping_mul(61) ^ (index >> 4)) as u8 ^ seed)
        .collect()
}
/// The records' directories: nothing named by a hash in `names`, nothing holding 16 bytes of any of
/// `plaintexts`.
fn assert_records_sealed(root: &Path, names: &[ContentHash], plaintexts: &[&[u8]]) {
    for kind in CustodyRecordKind::ALL {
        for entry in fs::read_dir(root.join(kind.directory())).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let stored = crate::store::stored_bytes(&path);
            for hash in names {
                assert!(!name.contains(&hash.to_string()), "{name} names a hash");
                assert!(
                    !stored.windows(32).any(|w| w == hash.0),
                    "{name} holds a hash"
                );
            }
            for plaintext in plaintexts {
                for probe in plaintext.chunks(1024).filter(|probe| probe.len() >= 16) {
                    assert!(
                        !stored.windows(16).any(|w| w == &probe[..16]),
                        "{name} holds plaintext"
                    );
                }
            }
        }
    }
}

/// Do: install a checkpoint and a manifest record and replace a named record twice in a store that
/// seals; reopen it. Expect: each reads back (the named record its newest bytes), installing the
/// same bytes again is kept, and no file of the records' directories is named by a hash or holds
/// one or a record's bytes.
#[test]
fn custody_records_are_sealed_under_keyed_names() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root, ContentForm::Sealed);
    let checkpoint = payload(1, 3 << 20 >> 2);
    let manifest = payload(2, 900);
    let first = payload(3, 120);
    let second = payload(4, 140);
    let checkpoint_hash = store
        .install_custody_record(CustodyRecordKind::Checkpoint, &checkpoint)
        .unwrap();
    let manifest_hash = store
        .install_custody_record(CustodyRecordKind::Manifest, &manifest)
        .unwrap();
    assert_eq!(
        store
            .install_custody_record(CustodyRecordKind::Manifest, &manifest)
            .unwrap(),
        manifest_hash
    );
    let name = ContentHash([0x5a; 32]);
    store
        .replace_named_custody_record(CustodyRecordKind::Receipt, name, &first)
        .unwrap();
    store
        .replace_named_custody_record(CustodyRecordKind::Receipt, name, &second)
        .unwrap();
    drop(store);
    let store = open(root, ContentForm::Sealed);
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, checkpoint_hash, 8 << 20)
            .unwrap(),
        checkpoint
    );
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Manifest, manifest_hash, 4096)
            .unwrap(),
        manifest
    );
    assert_eq!(
        store
            .read_named_custody_record(CustodyRecordKind::Receipt, name, 4096)
            .unwrap()
            .unwrap(),
        second
    );
    // A reader of the store reads them too.
    assert_eq!(
        store
            .reader()
            .unwrap()
            .read_custody_record(CustodyRecordKind::Manifest, manifest_hash, 4096)
            .unwrap(),
        manifest
    );
    assert_records_sealed(
        root,
        &[checkpoint_hash, manifest_hash, name],
        &[&checkpoint, &manifest, &first, &second],
    );
}

/// Do: flip a byte of a sealed record; carry a record into another kind's directory under the name
/// that kind would file it by. Expect: each refused.
#[test]
fn a_changed_or_carried_record_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root, ContentForm::Sealed);
    let bytes = payload(5, 500);
    let hash = store
        .install_custody_record(CustodyRecordKind::Checkpoint, &bytes)
        .unwrap();
    // The manifests' key directory made, so the carried record lies under a name it files by.
    store
        .install_custody_record(CustodyRecordKind::Manifest, &payload(6, 50))
        .unwrap();
    let path = store.record_path(CustodyRecordKind::Checkpoint, hash);
    let original = fs::read(&path).unwrap();
    let carried = store.record_path(CustodyRecordKind::Manifest, hash);
    fs::copy(&path, &carried).unwrap();
    assert!(
        store
            .read_custody_record(CustodyRecordKind::Manifest, hash, 4096)
            .is_err()
    );
    let mut flipped = original.clone();
    let at = flipped.len() / 2;
    flipped[at] ^= 1;
    fs::write(&path, &flipped).unwrap();
    assert!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, hash, 4096)
            .is_err()
    );
    fs::write(&path, &original).unwrap();
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, hash, 4096)
            .unwrap(),
        bytes
    );
}

/// Do: open a store in the clear holding records of each kind (a checkpoint longer than the
/// conversion's scratch among them) as sealing, replace the named record halfway through the
/// conversion, and convert it to the end. Expect: the tally agrees with the records in the clear on
/// the volume at every step, the replaced one tallied gone; it ends sealed, every record reads back
/// (the named one its newest bytes), and none lies in the clear.
#[test]
fn records_in_the_clear_are_converted_while_the_store_serves() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut clear = open(root, ContentForm::Clear);
    let checkpoint = payload(7, MAX_TRANSFER_CHUNK_BYTES + 12_345);
    let manifest = payload(8, 700);
    let first = payload(9, 100);
    let checkpoint_hash = clear
        .install_custody_record(CustodyRecordKind::Checkpoint, &checkpoint)
        .unwrap();
    let manifest_hash = clear
        .install_custody_record(CustodyRecordKind::Manifest, &manifest)
        .unwrap();
    let name = ContentHash([0x6b; 32]);
    clear
        .replace_named_custody_record(CustodyRecordKind::Receipt, name, &first)
        .unwrap();
    drop(clear);
    let clear_records = |root: &Path| {
        let mut tally = crate::Tally::default();
        for kind in CustodyRecordKind::ALL {
            for entry in fs::read_dir(root.join(kind.directory())).unwrap() {
                let path = entry.unwrap().path();
                let stem = path.file_stem().unwrap().to_str().unwrap().to_owned();
                if path.extension() == Some("record".as_ref()) && stem.len() == 64 {
                    tally.add(fs::metadata(&path).unwrap().len());
                }
            }
        }
        tally
    };
    let mut store = open(root, ContentForm::Sealed);
    let mut scratch = vec![0u8; MAX_TRANSFER_CHUNK_BYTES];
    let second = payload(10, 130);
    let mut replaced = false;
    for steps in 0..10_000 {
        let step = store.convert_step(&mut scratch, 1).unwrap();
        let status = store.conversion_status();
        if status.form == convert::StoreForm::Converting {
            assert_eq!(
                status.left(),
                clear_records(root),
                "step {steps}: {status:?}"
            );
            if !replaced {
                store
                    .replace_named_custody_record(CustodyRecordKind::Receipt, name, &second)
                    .unwrap();
                replaced = true;
                let status = store.conversion_status();
                assert_eq!(status.left(), clear_records(root), "{status:?}");
            }
        }
        if step == Conversion::Done {
            break;
        }
        assert_ne!(step, Conversion::Held);
    }
    let status = store.conversion_status();
    assert_eq!(status.form, convert::StoreForm::Sealed);
    assert_eq!(status.left(), crate::Tally::default());
    assert_eq!(status.converted.plus(status.gone), status.counted);
    assert!(status.gone.files >= 1, "the replaced record: {status:?}");
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, checkpoint_hash, 8 << 20)
            .unwrap(),
        checkpoint
    );
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Manifest, manifest_hash, 4096)
            .unwrap(),
        manifest
    );
    assert_eq!(
        store
            .read_named_custody_record(CustodyRecordKind::Receipt, name, 4096)
            .unwrap()
            .unwrap(),
        second
    );
    assert_records_sealed(
        root,
        &[checkpoint_hash, manifest_hash, name],
        &[&checkpoint, &manifest, &first, &second],
    );
}

/// Do: open a store whose objects seal (the content level) while its files beside them do not (below
/// the at-rest level), stage an upload and finish another, install and replace records; then open
/// it at the at-rest level and convert it. Expect: below the level every staging and record file is
/// as every earlier binary wrote it, named in the clear and holding its bytes, and the store holds
/// no marker of theirs; at the level they convert alone (the objects sealed already) and end sealed
/// under their keys, every record and upload as it was.
#[test]
fn below_the_at_rest_level_the_files_beside_the_objects_stay_as_earlier_binaries_wrote_them() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let open_at = |rest: ContentForm| {
        ContentStore::open_forms(
            root,
            limits(),
            DiskBudget::new(DiskBudgetConfig::unbounded()).unwrap(),
            &crate::store::test_keys(root),
            ContentForm::Sealed,
            rest,
        )
        .unwrap()
    };
    let mut store = open_at(ContentForm::Clear);
    let checkpoint = payload(11, 300);
    let checkpoint_hash = store
        .install_custody_record(CustodyRecordKind::Checkpoint, &checkpoint)
        .unwrap();
    let name = ContentHash([0x7c; 32]);
    let receipt = payload(12, 90);
    store
        .replace_named_custody_record(CustodyRecordKind::Receipt, name, &receipt)
        .unwrap();
    let (live, done) = (UploadId([0x81; 16]), UploadId([0x82; 16]));
    let bytes = payload(13, 5000);
    store
        .begin(
            live,
            ContentDomainId([4; 16]),
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    store.append(live, 0, &bytes[..2000]).unwrap();
    store
        .begin(
            done,
            ContentDomainId([4; 16]),
            ContentClass::Evidence,
            10,
            None,
        )
        .unwrap();
    store.append(done, 0, &bytes[..10]).unwrap();
    let object = store.seal(done).unwrap();
    store.finish(done).unwrap();
    // As an earlier binary wrote them: named in the clear, holding their bytes.
    let staging = root.join("staging");
    assert_eq!(
        fs::read(staging.join(format!("{}.part", hex(&live.0)))).unwrap(),
        &bytes[..2000]
    );
    assert!(staging.join(format!("{}.meta", hex(&done.0))).is_file());
    assert_eq!(
        fs::read(
            root.join("checkpoints")
                .join(format!("{checkpoint_hash}.record"))
        )
        .unwrap(),
        checkpoint
    );
    assert_eq!(
        fs::read(root.join("receipts").join(format!("{name}.record"))).unwrap(),
        receipt
    );
    assert!(!root.join(sealing::REST_SEALING).exists());
    assert!(!root.join(sealing::REST_SEALED).exists());
    assert!(root.join(sealing::SEALED).exists(), "the objects sealed");
    drop(store);
    // At the at-rest level: they convert, the objects sealed already.
    let mut store = open_at(ContentForm::Sealed);
    assert!(root.join(sealing::REST_SEALING).exists());
    let mut scratch = vec![0u8; MAX_TRANSFER_CHUNK_BYTES];
    for _ in 0..10_000 {
        if store.convert_step(&mut scratch, 1).unwrap() == Conversion::Done {
            break;
        }
    }
    let status = store.conversion_status();
    assert_eq!(status.form, convert::StoreForm::Sealed);
    assert_eq!(status.left(), crate::Tally::default());
    assert!(root.join(sealing::REST_SEALED).exists());
    assert!(!root.join(sealing::REST_SEALING).exists());
    assert_eq!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, checkpoint_hash, 4096)
            .unwrap(),
        checkpoint
    );
    assert_eq!(
        store
            .read_named_custody_record(CustodyRecordKind::Receipt, name, 4096)
            .unwrap()
            .unwrap(),
        receipt
    );
    assert_eq!(store.offset(live).unwrap(), 2000);
    store.append(live, 2000, &bytes[2000..]).unwrap();
    let reference = store.seal(live).unwrap();
    assert_eq!(store.read_bytes(&reference, bytes.len()).unwrap(), bytes);
    assert_eq!(store.read_bytes(&object, 10).unwrap(), &bytes[..10]);
    for entry in fs::read_dir(&staging).unwrap() {
        let file = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(
            !file.contains(&hex(&live.0)) && !file.contains(&hex(&done.0)),
            "{file}"
        );
    }
    assert_records_sealed(root, &[checkpoint_hash, name], &[&checkpoint, &receipt]);
}

/// What a store below the at-rest level holds beside its objects: an upload half sent and one
/// finished, a chunk half received in parts, and a record of each kind.
struct Beside {
    live: UploadId,
    bytes: Vec<u8>,
    transfer: TransferManifest,
    chunk: Vec<u8>,
    checkpoint: (ContentHash, Vec<u8>),
    manifest: (ContentHash, Vec<u8>),
    receipt: (ContentHash, Vec<u8>),
}

fn lay_out_beside(root: &Path) -> Beside {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = open(source_dir.path(), ContentForm::Sealed);
    let object = payload(20, 50_000);
    let upload = UploadId([0x90; 16]);
    source
        .begin(
            upload,
            ContentDomainId([5; 16]),
            ContentClass::Evidence,
            object.len() as u64,
            None,
        )
        .unwrap();
    source.append(upload, 0, &object).unwrap();
    let reference = source.seal(upload).unwrap();
    let exported = source.export_manifest(&reference).unwrap();
    let chunk = source.read_transfer_chunk(&exported, 0).unwrap();
    let mut store = open(root, ContentForm::Clear);
    let transfer = store
        .prepare_import(reference, exported.encoded().to_vec())
        .unwrap();
    store
        .import_chunk_part(&transfer, 0, 0, &chunk[..20_000])
        .unwrap();
    let (live, done) = (UploadId([0x91; 16]), UploadId([0x92; 16]));
    let bytes = payload(21, 30_000);
    store
        .begin(
            live,
            ContentDomainId([6; 16]),
            ContentClass::Evidence,
            bytes.len() as u64,
            None,
        )
        .unwrap();
    store.append(live, 0, &bytes[..12_000]).unwrap();
    store
        .begin(
            done,
            ContentDomainId([6; 16]),
            ContentClass::Evidence,
            20,
            None,
        )
        .unwrap();
    store.append(done, 0, &bytes[..20]).unwrap();
    store.seal(done).unwrap();
    store.finish(done).unwrap();
    let checkpoint = payload(22, 600);
    let manifest = payload(23, 300);
    let receipt = payload(24, 80);
    let checkpoint_hash = store
        .install_custody_record(CustodyRecordKind::Checkpoint, &checkpoint)
        .unwrap();
    let manifest_hash = store
        .install_custody_record(CustodyRecordKind::Manifest, &manifest)
        .unwrap();
    let name = ContentHash([0x93; 32]);
    store
        .replace_named_custody_record(CustodyRecordKind::Receipt, name, &receipt)
        .unwrap();
    drop(store);
    Beside {
        live,
        bytes,
        transfer,
        chunk,
        checkpoint: (checkpoint_hash, checkpoint),
        manifest: (manifest_hash, manifest),
        receipt: (name, receipt),
    }
}

/// Do: lay out a store in the clear with an upload half sent and one finished, a chunk half
/// received, and a record of each kind; open it sealing and cut its conversion after each step in
/// turn, then open it again (asking for the clear) and let it finish. Expect: every cut resumes and
/// ends sealed, its tally whole (counted equals converted with what went), every record reads back,
/// the upload and the transfer go on to whole objects, and nothing beside the objects lies in the
/// clear.
#[test]
fn the_files_beside_the_objects_convert_through_a_cut_at_any_step() {
    let mut scratch = vec![0u8; MAX_TRANSFER_CHUNK_BYTES];
    // Every cut until one falls after the conversion's last step: the loop ends there, not at its
    // bound.
    let mut covered = None;
    for cut in 0..400 {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let beside = lay_out_beside(root);
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
        let status = store.conversion_status();
        assert_eq!(status.form, convert::StoreForm::Sealed, "cut {cut}");
        assert_eq!(
            status.left(),
            crate::Tally::default(),
            "cut {cut}: {status:?}"
        );
        for (kind, (hash, bytes)) in [
            (CustodyRecordKind::Checkpoint, &beside.checkpoint),
            (CustodyRecordKind::Manifest, &beside.manifest),
        ] {
            assert_eq!(
                &store.read_custody_record(kind, *hash, 4096).unwrap(),
                bytes,
                "cut {cut}"
            );
        }
        assert_eq!(
            store
                .read_named_custody_record(CustodyRecordKind::Receipt, beside.receipt.0, 4096)
                .unwrap()
                .unwrap(),
            beside.receipt.1,
            "cut {cut}"
        );
        assert_eq!(store.offset(beside.live).unwrap(), 12_000, "cut {cut}");
        store
            .append(beside.live, 12_000, &beside.bytes[12_000..])
            .unwrap();
        let reference = store.seal(beside.live).unwrap();
        assert_eq!(
            store.read_bytes(&reference, beside.bytes.len()).unwrap(),
            beside.bytes
        );
        assert_eq!(
            store.staged_chunk_bytes(&beside.transfer, 0).unwrap(),
            20_000,
            "cut {cut}"
        );
        store
            .import_chunk_part(&beside.transfer, 0, 20_000, &beside.chunk[20_000..])
            .unwrap();
        assert_eq!(
            store.read_transfer_chunk(&beside.transfer, 0).unwrap(),
            beside.chunk
        );
        assert_records_sealed(
            root,
            &[beside.checkpoint.0, beside.manifest.0, beside.receipt.0],
            &[&beside.checkpoint.1, &beside.manifest.1, &beside.receipt.1],
        );
        for entry in fs::read_dir(root.join("staging")).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(!name.contains(&hex(&beside.live.0)), "cut {cut}: {name}");
        }
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
