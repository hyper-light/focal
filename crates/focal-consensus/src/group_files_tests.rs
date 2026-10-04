use super::*;
use focal_platform::fs::FileMedium;
use focal_sim::disk::Disk;

fn records(floor: Option<[u8; 32]>) -> GroupRecords {
    GroupRecords {
        identity: NodeConfig {
            node_id: 3,
            cluster_id: [1; 16],
            group_id: [2; 16],
            voters: vec![1, 2, 3],
            learners: vec![4],
            election_tick: 10,
            heartbeat_tick: 2,
            max_entry_bytes: 1 << 20,
            max_uncommitted_bytes: 1 << 24,
            max_inflight_messages: 32,
            fast: false,
            election_seed: None,
        },
        fast: true,
        decoder_floor: floor,
        decoder_transition: floor.map(|f| (f, [9; 32])),
    }
}

fn point(index: u64) -> ImagePoint {
    ImagePoint {
        index,
        term: 2,
        configuration: ConfState {
            voters: vec![1, 2, 3],
            learners: vec![4],
            voters_outgoing: vec![1, 2, 5],
            learners_next: vec![5],
            auto_leave: true,
        },
    }
}

const BOUND: usize = 4096;
const GROUP: [u8; 16] = [0xab; 16];

#[test]
fn a_groups_directory_is_named_by_its_id_in_hex_under_raft_groups() {
    let dir = group_dir(Path::new("/data"), [0x0f; 16]);
    assert_eq!(
        dir,
        Path::new("/data/raft/groups/0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f")
    );
}

#[test]
fn records_and_images_read_back_as_written_on_the_filesystem() {
    let root = tempfile::tempdir().unwrap();
    let mut medium = FileMedium;
    let dir = create(&mut medium, root.path(), GROUP).unwrap();
    assert_eq!(read_records(&medium, &dir).unwrap(), None);
    assert_eq!(read_image(&medium, &dir, BOUND).unwrap(), None);
    write_records(&mut medium, &dir, &records(Some([7; 32]))).unwrap();
    write_image(&mut medium, &dir, &point(9), b"state at nine", BOUND).unwrap();
    assert_eq!(
        read_records(&medium, &dir).unwrap(),
        Some(records(Some([7; 32])))
    );
    assert_eq!(
        read_image(&medium, &dir, BOUND).unwrap(),
        Some((point(9), b"state at nine".to_vec()))
    );
    // A rewrite replaces the whole file and touches the other one not at all.
    write_image(&mut medium, &dir, &point(12), b"state at twelve", BOUND).unwrap();
    assert_eq!(
        read_image(&medium, &dir, BOUND).unwrap(),
        Some((point(12), b"state at twelve".to_vec()))
    );
    assert_eq!(
        read_records(&medium, &dir).unwrap(),
        Some(records(Some([7; 32])))
    );
}

/// Every byte of each file is covered: a flipped bit anywhere, a missing tail, another file's
/// magic, or a version this binary does not know is refused, naming the file, and never read as
/// no file or as another version.
#[test]
fn a_file_that_does_not_read_whole_is_refused_naming_it() {
    let mut disk = Disk::new(1 << 20);
    let dir = PathBuf::from("/data/raft/groups/g");
    write_records(&mut disk, &dir, &records(None)).unwrap();
    write_image(&mut disk, &dir, &point(4), b"image", BOUND).unwrap();
    for (name, len) in [
        (META_FILE, disk.read(dir.join(META_FILE)).unwrap().len()),
        (IMAGE_FILE, disk.read(dir.join(IMAGE_FILE)).unwrap().len()),
    ] {
        let path = dir.join(name);
        for at in 0..len {
            let mut flipped = disk.clone();
            let byte = flipped.read(&path).unwrap()[at];
            flipped.write(&path, at, &[byte ^ 0x01]).unwrap();
            let read = if name == META_FILE {
                read_records(&flipped, &dir).map(drop)
            } else {
                read_image(&flipped, &dir, BOUND).map(drop)
            };
            assert!(
                matches!(read, Err(GroupFileError::Corrupt { file, .. }) if file == name),
                "{name}: byte {at} flipped read as {read:?}"
            );
        }
        // A tail that never reached the disk.
        let whole = disk.read(&path).unwrap().to_vec();
        let mut short = disk.clone();
        short.rename(&path, dir.join("gone")).unwrap();
        short.create(&path).unwrap();
        short.write(&path, 0, &whole[..whole.len() - 1]).unwrap();
        let read = if name == META_FILE {
            read_records(&short, &dir).map(drop)
        } else {
            read_image(&short, &dir, BOUND).map(drop)
        };
        assert!(
            matches!(read, Err(GroupFileError::Corrupt { .. })),
            "{name}: {read:?}"
        );
    }
    // One file's bytes under the other's name.
    let meta = disk.read(dir.join(META_FILE)).unwrap().to_vec();
    let mut swapped = disk.clone();
    swapped
        .rename(dir.join(IMAGE_FILE), dir.join("gone"))
        .unwrap();
    swapped.create(dir.join(IMAGE_FILE)).unwrap();
    swapped.write(dir.join(IMAGE_FILE), 0, &meta).unwrap();
    assert!(matches!(
        read_image(&swapped, &dir, BOUND),
        Err(GroupFileError::Corrupt {
            reason: "another file's magic",
            ..
        })
    ));
    // A version this binary does not know, summed as if a later binary wrote it.
    let mut later = meta.clone();
    later[8] = 2;
    let body = later.len() - 4;
    let sum = crc32fast::hash(&later[..body]);
    later[body..].copy_from_slice(&sum.to_le_bytes());
    let mut newer = disk.clone();
    newer.rename(dir.join(META_FILE), dir.join("gone")).unwrap();
    newer.create(dir.join(META_FILE)).unwrap();
    newer.write(dir.join(META_FILE), 0, &later).unwrap();
    assert!(matches!(
        read_records(&newer, &dir),
        Err(GroupFileError::Corrupt {
            reason: "a version this binary does not know",
            ..
        })
    ));
}

/// The power cut at every operation of a rewrite, the disk keeping what was durable: the file
/// reads as the write before it or as the rewrite, whole, never anything else; and once the
/// rewrite returned, as the rewrite.
#[test]
fn a_crash_at_every_operation_of_a_rewrite_leaves_the_old_file_or_the_new() {
    let dir = PathBuf::from("/data/raft/groups/g");
    let mut base = Disk::new(1 << 20);
    write_records(&mut base, &dir, &records(None)).unwrap();
    write_image(&mut base, &dir, &point(4), b"old", BOUND).unwrap();
    // Every operation of each rewrite, counted on a disk that does not fail.
    let mut probe = base.clone();
    let start = probe.operations();
    write_records(&mut probe, &dir, &records(Some([3; 32]))).unwrap();
    write_image(&mut probe, &dir, &point(8), b"new", BOUND).unwrap();
    let operations = probe.operations() - start;
    assert_eq!(operations, 10, "two installs of five operations each");
    for cut in 1..=operations {
        let mut disk = base.clone();
        disk.fail_before(Some(start + cut));
        let wrote = write_records(&mut disk, &dir, &records(Some([3; 32])))
            .and_then(|()| write_image(&mut disk, &dir, &point(8), b"new", BOUND));
        assert!(wrote.is_err(), "cut {cut} fails the rewrite");
        // The records' install is the first five operations: past them, it returned.
        let records_returned = cut > 5;
        disk.crash();
        let meta = read_records(&disk, &dir).unwrap().unwrap();
        let image = read_image(&disk, &dir, BOUND).unwrap().unwrap();
        assert!(
            meta == records(None) || meta == records(Some([3; 32])),
            "cut {cut}: {meta:?}"
        );
        if records_returned {
            assert_eq!(
                meta,
                records(Some([3; 32])),
                "cut {cut}: a returned write is kept"
            );
        }
        assert!(
            image == (point(4), b"old".to_vec()) || image == (point(8), b"new".to_vec()),
            "cut {cut}: {image:?}"
        );
    }
}

#[test]
fn what_exceeds_its_bound_is_refused_written_or_read() {
    let mut disk = Disk::new(1 << 20);
    let dir = PathBuf::from("/data/raft/groups/g");
    let big = vec![0u8; BOUND + 1];
    assert!(matches!(
        write_image(&mut disk, &dir, &point(1), &big, BOUND),
        Err(GroupFileError::Bound { .. })
    ));
    write_image(&mut disk, &dir, &point(1), &big[..BOUND], BOUND).unwrap();
    assert!(matches!(
        read_image(&disk, &dir, BOUND - 1),
        Err(GroupFileError::Bound { .. } | GroupFileError::Io(_))
    ));
    // An identity with more members than any configuration holds encodes past the records' bound.
    let mut many = records(None);
    many.identity.voters = vec![u64::MAX; 3 * hyper_raft::MAX_MEMBERS];
    assert!(matches!(
        write_records(&mut disk, &dir, &many),
        Err(GroupFileError::Bound { .. })
    ));
}

/// A medium that cannot tell whether a path is there.
struct Unreadable(Disk);
impl Medium for Unreadable {
    fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        Medium::create_dir(&mut self.0, path)
    }
    fn create(&mut self, path: &Path) -> io::Result<()> {
        Medium::create(&mut self.0, path)
    }
    fn write(&mut self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        Medium::write(&mut self.0, path, bytes)
    }
    fn sync_file(&mut self, path: &Path) -> io::Result<()> {
        Medium::sync_file(&mut self.0, path)
    }
    fn sync_dir(&mut self, path: &Path) -> io::Result<()> {
        Medium::sync_dir(&mut self.0, path)
    }
    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        Medium::rename(&mut self.0, from, to)
    }
    fn exists(&self, _: &Path) -> io::Result<bool> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }
    fn read(&self, path: &Path, limit: usize) -> io::Result<Vec<u8>> {
        Medium::read(&self.0, path, limit)
    }
}

/// A group's records that cannot be read are an error, never no records: a log without records
/// is corruption, and must not open as a fresh group (27 §15.5, O1).
#[test]
fn records_that_cannot_be_read_are_an_error_never_none() {
    let medium = Unreadable(Disk::new(1 << 20));
    let dir = PathBuf::from("/data/raft/groups/g");
    assert!(matches!(
        read_records(&medium, &dir),
        Err(GroupFileError::Io(_))
    ));
    assert!(matches!(
        read_image(&medium, &dir, BOUND),
        Err(GroupFileError::Io(_))
    ));
}
