use super::*;
use crate::CustodyReceipt;
use focal_memory::DiskBudgetConfig;
use focal_model::{ContentClass, LedgerId, RouteEpoch, SessionId, TenantId};
use std::time::{Duration, SystemTime};

const DAY: u64 = 24 * 60 * 60 * 1000;

fn limits() -> StoreLimits {
    StoreLimits {
        max_content_bytes: 64,
        max_staging_bytes: 256,
        max_uploads: 4,
        chunk_bytes: 4,
        max_manifest_bytes: 2048,
    }
}
fn open(root: &Path) -> ContentStore {
    ContentStore::open(root, limits(), &crate::store::test_keys(root)).unwrap()
}
fn domain(byte: u8) -> ContentDomainId {
    ContentDomainId([byte; 16])
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
    for (index, chunk) in bytes.chunks(4).enumerate() {
        store.append(upload, (index * 4) as u64, chunk).unwrap();
    }
    let reference = store.seal(upload).unwrap();
    store.finish(upload).unwrap();
    reference
}
/// Every file under `root` was last touched `age_ms` ago.
fn age_everything(root: &Path, age_ms: u64) {
    let when = SystemTime::now() - Duration::from_millis(age_ms);
    fn walk(path: &Path, when: SystemTime) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                walk(&path, when);
            } else {
                File::options()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_modified(when)
                    .unwrap();
            }
        }
    }
    walk(root, when);
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
fn config() -> CollectorConfig {
    CollectorConfig {
        grace_ms: DAY,
        quarantine_ms: 7 * DAY,
        terminal_grace_ms: 7 * DAY,
        keep_records: 2,
        max_marks: 1 << 16,
    }
}
fn pass(
    store: &mut ContentStore,
    protection: &ProtectionSet,
    config: CollectorConfig,
    now: u64,
    max_items: usize,
) -> CollectorReport {
    for _ in 0..10_000 {
        let report = store
            .collect_step(protection, config, now, max_items)
            .unwrap();
        if report.complete {
            return report;
        }
    }
    panic!("the pass never completed");
}
fn count(path: &Path, extension: &str) -> usize {
    fs::read_dir(path)
        .map(|entries| {
            entries
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .and_then(|value| value.to_str())
                        == Some(extension)
                })
                .count()
        })
        .unwrap_or(0)
}
fn quarantine_rounds(root: &Path) -> Vec<PathBuf> {
    fs::read_dir(root.join("quarantine"))
        .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
        .unwrap_or_default()
}

#[test]
fn an_empty_store_completes_a_pass_doing_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = open(directory.path());
    let mut protection = ProtectionSet::new();
    assert!(matches!(
        store.collect_step(&protection, config(), now_ms(), 8),
        Err(ContentError::Invalid)
    ));
    protection.finish();
    let report = pass(&mut store, &protection, config(), now_ms(), 8);
    assert!(report.complete);
    assert_eq!(report.objects_quarantined, 0);
    assert_eq!(report.deleted, 0);
}

/// Unreferenced objects past the grace leave through quarantine with their
/// receipts and their unshared chunks; protected and young objects stay
/// whole, a chunk they share stays with them, and a quarantined object can
/// be brought back until its round expires and is deleted.
#[test]
fn unreferenced_objects_are_quarantined_restored_and_finally_deleted() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root);
    let tenant = domain(2);
    let kept = seal(&mut store, 1, tenant, b"AAAABBBB");
    let dropped = seal(&mut store, 2, tenant, b"AAAACCCC");
    let ledger = LedgerId {
        tenant: TenantId([2; 16]),
        session: SessionId([3; 16]),
    };
    for reference in [&kept, &dropped] {
        let receipt = CustodyReceipt::new(
            ledger,
            tenant,
            reference.root,
            reference.length,
            7,
            RouteEpoch(1),
            1,
            5,
        );
        store
            .replace_named_custody_record(
                CustodyRecordKind::Receipt,
                CustodyReceipt::name(ledger, reference.root, 7),
                &receipt.encode(),
            )
            .unwrap();
    }
    let objects = root.join("objects").join(hex(&tenant.0));
    assert_eq!(count(&objects, "manifest"), 2);
    // Chunks "AAAA", "BBBB", "CCCC": the first is shared.
    assert_eq!(count(&objects, "chunk"), 3);
    let mut protection = ProtectionSet::new();
    protection.protect_object(tenant, kept.root).unwrap();
    protection.finish();
    // Nothing is young enough to leave yet.
    let now = now_ms();
    let report = pass(&mut store, &protection, config(), now, 3);
    assert_eq!(report.objects_quarantined, 0);
    assert_eq!(count(&objects, "manifest"), 2);
    // Two days later the unreferenced object, its unshared chunk and its
    // receipt are quarantined; the kept object still reads whole.
    age_everything(root, 2 * DAY);
    let report = pass(&mut store, &protection, config(), now, 3);
    assert_eq!(report.objects_quarantined, 1, "{report:?}");
    assert_eq!(report.chunks_quarantined, 1);
    assert_eq!(report.receipts_quarantined, 1);
    assert_eq!(report.deleted, 0);
    assert_eq!(count(&objects, "manifest"), 1);
    assert_eq!(count(&objects, "chunk"), 2);
    assert_eq!(store.read_bytes(&kept, 64).unwrap(), b"AAAABBBB");
    assert!(store.read_bytes(&dropped, 64).is_err());
    assert_eq!(quarantine_rounds(root).len(), 1);
    // The quarantined object comes back whole, including the shared chunk
    // it never lost, and a later pass takes it again.
    assert!(store.restore_quarantined(tenant, dropped.root).unwrap());
    assert_eq!(store.read_bytes(&dropped, 64).unwrap(), b"AAAACCCC");
    assert!(!store.restore_quarantined(tenant, dropped.root).unwrap());
    age_everything(root, 2 * DAY);
    let report = pass(&mut store, &protection, config(), now, 3);
    assert_eq!(report.objects_quarantined, 1);
    assert!(store.read_bytes(&dropped, 64).is_err());
    // The store reopens with its quarantine intact; a round older than the
    // quarantine grace is deleted, bytes and all.
    drop(store);
    let mut store = open(root);
    // Both passes ran at the same clock, so they share one round.
    assert_eq!(quarantine_rounds(root).len(), 1);
    let later = now + 8 * DAY;
    let report = pass(&mut store, &protection, config(), later, 2);
    assert!(report.deleted >= 2, "{report:?}");
    assert!(report.bytes_deleted > 0);
    assert!(quarantine_rounds(root).is_empty());
    assert!(!store.restore_quarantined(tenant, dropped.root).unwrap());
    assert_eq!(store.read_bytes(&kept, 64).unwrap(), b"AAAABBBB");
}

/// An opaque domain is never collected; a domain whose chunk marks exceed
/// the bound keeps its chunks and reports the deferral.
#[test]
fn opaque_domains_and_mark_overflow_leave_bytes_in_place() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root);
    let hidden = domain(5);
    let known = domain(6);
    let orphan = seal(&mut store, 1, hidden, b"XXXXYYYY");
    let kept = seal(&mut store, 2, known, b"XXXXYYYYZZZZ");
    let gone = seal(&mut store, 3, known, b"QQQQ");
    age_everything(root, 2 * DAY);
    let mut protection = ProtectionSet::new();
    protection.opaque_domain(hidden).unwrap();
    protection.protect_object(known, kept.root).unwrap();
    protection.finish();
    let tight = CollectorConfig {
        max_marks: 2,
        ..config()
    };
    let report = pass(&mut store, &protection, tight, now_ms(), 4);
    assert_eq!(report.opaque_domains, 1);
    assert_eq!(report.objects_quarantined, 1);
    assert_eq!(report.chunks_deferred, 1);
    assert_eq!(report.chunks_quarantined, 0);
    assert_eq!(store.read_bytes(&orphan, 64).unwrap(), b"XXXXYYYY");
    assert_eq!(store.read_bytes(&kept, 64).unwrap(), b"XXXXYYYYZZZZ");
    assert!(store.read_bytes(&gone, 64).is_err());
    assert_eq!(count(&root.join("objects").join(hex(&known.0)), "chunk"), 4);
}

/// A staged upload untouched past the grace is finished as abandoned: its
/// identity is fenced exactly as an explicit finish fences it, and the fence
/// itself is released after the terminal grace.
#[test]
fn abandoned_uploads_expire_and_their_fences_are_released_later() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root);
    let tenant = domain(9);
    let id = UploadId([4; 16]);
    store
        .begin(id, tenant, ContentClass::Evidence, 8, None)
        .unwrap();
    store.append(id, 0, b"ABCD").unwrap();
    let protection = {
        let mut set = ProtectionSet::new();
        set.finish();
        set
    };
    let now = now_ms();
    assert_eq!(
        pass(&mut store, &protection, config(), now, 4).uploads_expired,
        0
    );
    age_everything(root, 2 * DAY);
    let report = pass(&mut store, &protection, config(), now, 4);
    assert_eq!(report.uploads_expired, 1, "{report:?}");
    assert!(matches!(
        store.begin(id, tenant, ContentClass::Evidence, 8, None),
        Err(ContentError::FinishedUpload)
    ));
    assert_eq!(count(&root.join("staging"), "part"), 0);
    // Past the terminal grace the identity may begin afresh.
    age_everything(root, 8 * DAY);
    let report = pass(&mut store, &protection, config(), now, 4);
    assert_eq!(report.terminals_released, 1, "{report:?}");
    assert_eq!(
        store
            .begin(id, tenant, ContentClass::Evidence, 8, None)
            .unwrap(),
        0
    );
}

/// Custody records keep the newest few and every protected one; older
/// ones past the grace are quarantined.
#[test]
fn custody_records_keep_the_newest_and_the_protected() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root);
    let mut hashes = Vec::new();
    for index in 0..5u8 {
        let hash = store
            .install_custody_record(CustodyRecordKind::Checkpoint, &[index; 40])
            .unwrap();
        hashes.push(hash);
        // Distinct ages, oldest first.
        let path = root.join("checkpoints").join(format!("{hash}.record"));
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_millis((10 - u64::from(index)) * DAY))
            .unwrap();
    }
    let mut protection = ProtectionSet::new();
    protection
        .protect_record(CustodyRecordKind::Checkpoint, hashes[0])
        .unwrap();
    protection.finish();
    let report = pass(&mut store, &protection, config(), now_ms(), 3);
    // Five records: the two newest stay, the oldest is protected, two go.
    assert_eq!(report.records_quarantined, 2, "{report:?}");
    assert!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, hashes[0], 64)
            .is_ok()
    );
    assert!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, hashes[4], 64)
            .is_ok()
    );
    assert!(
        store
            .read_custody_record(CustodyRecordKind::Checkpoint, hashes[1], 64)
            .is_err()
    );
}

/// Seeds nothing protects leave once past the grace; a sweep resumes
/// across bounded calls.
#[test]
fn unprotected_seeds_are_removed_past_the_grace() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("seeds");
    let mut seeds =
        SeedStore::open(&root, DiskBudget::new(DiskBudgetConfig::default()).unwrap()).unwrap();
    let keep = seeds.install(b"keep").unwrap();
    let old = seeds.install(b"old").unwrap();
    let young = seeds.install(b"young").unwrap();
    for hash in [keep, old] {
        File::options()
            .write(true)
            .open(root.join(format!("{hash}.seed")))
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_millis(2 * DAY))
            .unwrap();
    }
    let protected = [keep];
    let mut removed = 0;
    let mut complete = false;
    for _ in 0..16 {
        let report = seeds.collect(&protected, DAY, now_ms(), 1).unwrap();
        removed += report.removed;
        if report.complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(removed, 1);
    assert!(seeds.contains(keep));
    assert!(!seeds.contains(old));
    assert!(seeds.contains(young));
}

/// What a crash left unnamed (an install's or a batch's unsynced file) leaves
/// past the grace too; a young one, as a live batch's would be, stays.
#[test]
fn unnamed_files_a_crash_left_are_removed_past_the_grace() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("seeds");
    let mut seeds =
        SeedStore::open(&root, DiskBudget::new(DiskBudgetConfig::default()).unwrap()).unwrap();
    let stale_install = root.join(format!("{}.install", ContentHash([3; 32])));
    let stale_batch = root.join(format!("{}.7.batch", ContentHash([4; 32])));
    let young_batch = root.join(format!("{}.8.batch", ContentHash([5; 32])));
    for path in [&stale_install, &stale_batch, &young_batch] {
        std::fs::write(path, b"partial").unwrap();
    }
    for path in [&stale_install, &stale_batch] {
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_millis(2 * DAY))
            .unwrap();
    }
    let mut complete = false;
    for _ in 0..16 {
        if seeds.collect(&[], DAY, now_ms(), 4).unwrap().complete {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert!(!stale_install.exists());
    assert!(!stale_batch.exists());
    assert!(young_batch.exists());
}

/// A seed sweep walks the directory across calls while a checkpoint's commit, on another thread,
/// renames its batch's files under their hashes: an entry the walk listed that is gone when the
/// sweep looks at it was renamed or removed, not an I/O failure, and the store goes on serving.
/// Taken for one, the sweep failed the store and the session's next checkpoint stopped it.
#[test]
fn a_seed_the_sweep_listed_that_is_gone_when_it_looks_fails_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("seeds");
    let mut seeds =
        SeedStore::open(&root, DiskBudget::new(DiskBudgetConfig::default()).unwrap()).unwrap();
    let name = |n: u32| {
        let mut hash = [0u8; 32];
        hash[..4].copy_from_slice(&n.to_be_bytes());
        ContentHash(hash)
    };
    for n in 0..256 {
        std::fs::write(root.join(format!("{}.9.batch", name(n))), b"chunk").unwrap();
    }
    // The sweep looks at one entry and keeps its walk for the next call.
    assert!(!seeds.collect(&[], DAY, now_ms(), 1).unwrap().complete);
    // The batch commits: every file is renamed under its hash.
    for n in 0..256 {
        std::fs::rename(
            root.join(format!("{}.9.batch", name(n))),
            root.join(format!("{}.seed", name(n))),
        )
        .unwrap();
    }
    let report = seeds.collect(&[], DAY, now_ms(), 4096).unwrap();
    assert!(report.complete);
    assert_eq!(report.removed, 0, "young seeds stay");
    assert!(seeds.batch().is_ok(), "the store still serves");
    for n in 0..256 {
        assert!(root.join(format!("{}.seed", name(n))).exists());
    }
}

/// A collector's walk resumes across steps while the store's writer renames and removes what it
/// walks: files gone when the walk reaches them are passed over, the pass completes, and the store
/// goes on serving. Taken for an I/O failure, a vanished file failed the store.
#[test]
fn a_file_gone_since_the_walk_listed_it_is_passed_over() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut store = open(root);
    let tenant = domain(2);
    let mut sealed = Vec::new();
    for id in 1..=64u8 {
        sealed.push(seal(&mut store, id, tenant, &[id, id, id, id, 0, 0, 0, id]));
    }
    let objects = root.join("objects").join(hex(&tenant.0));
    let mut protection = ProtectionSet::new();
    protection.protect_object(tenant, sealed[0].root).unwrap();
    protection.finish();
    age_everything(root, 2 * DAY);
    let now = now_ms();
    // Step until the walk has listed the domain's manifests, and taken one.
    let mut steps = 0;
    while !matches!(store.collector.phase, Phase::Manifests { .. }) {
        assert!(
            !store
                .collect_step(&protection, config(), now, 1)
                .unwrap()
                .complete
        );
        steps += 1;
        assert!(steps < 1_000, "the walk never reached the manifests");
    }
    // Every other object's files go: the walk's listing names what is no longer there. The first
    // object's manifest stays under its keyed name, and the domain's key record with it.
    let kept = store.manifest_file(sealed[0].domain, sealed[0].root);
    for entry in fs::read_dir(&objects).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if path != kept && !name.ends_with(".chunk") && name != crate::store::sealing::DOMAIN_KEY {
            fs::remove_file(&path).unwrap();
        }
    }
    let report = pass(&mut store, &protection, config(), now, 1);
    assert!(report.complete);
    assert_eq!(
        store.read_bytes(&sealed[0], 64).unwrap(),
        [1, 1, 1, 1, 0, 0, 0, 1]
    );
    seal(&mut store, 99, tenant, b"STILLSERVES");
}
