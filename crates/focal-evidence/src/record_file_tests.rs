use super::*;

/// A key of its own for every file ID, as a directory's key derives one.
fn key_of(id: &[u8; 16]) -> Result<VersionKey, ContentError> {
    let secret = hyper_seal::Secret32::from_bytes(blake3::hash(id).as_bytes())
        .map_err(|_| ContentError::Corrupt)?;
    VersionKey::new(&secret, *id).map_err(|_| ContentError::Corrupt)
}

/// Bytes no stored byte may repeat by chance.
fn plaintext(seed: u8, length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| (index.wrapping_mul(131) ^ (index >> 9)) as u8 ^ seed)
        .collect()
}

/// A part of `appends` appended and synced one at a time, as an upload's are.
fn part(path: &Path, appends: &[Vec<u8>], most: usize) -> RecordWriter {
    let _keys = focal_seal::test_keys(path.parent().unwrap()).unwrap();
    let id = [7u8; 16];
    let mut writer =
        RecordWriter::create(path, PART_MAGIC, id, key_of(&id).unwrap(), most).unwrap();
    for bytes in appends {
        writer.append(bytes).unwrap();
        writer.sync().unwrap();
    }
    writer
}

fn header_id(path: &Path) -> [u8; 16] {
    fs::read(path).unwrap()[8..24].try_into().unwrap()
}

/// A part opened again after a clean stop is read through and goes on where it stopped, under the
/// key it was written under; every range reads back.
#[test]
fn a_part_opened_again_goes_on_where_it_stopped() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("upload.part");
    let first = plaintext(1, RECORD_BYTES * 2 + 100);
    let second = plaintext(2, 777);
    let most = 4 * RECORD_BYTES;
    drop(part(&path, &[first.clone(), second.clone()], most));
    let torn = records_bytes(RECORD_BYTES * 3).unwrap();
    let mut writer =
        RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All).unwrap();
    assert_eq!(
        header_id(&path),
        [7u8; 16],
        "the same file, under its own key"
    );
    let mut whole = first.clone();
    whole.extend_from_slice(&second);
    assert_eq!(writer.plain(), whole.len() as u64);
    let third = plaintext(3, RECORD_BYTES + 9);
    writer.append(&third).unwrap();
    writer.sync().unwrap();
    whole.extend_from_slice(&third);
    let mut read = vec![0u8; whole.len()];
    writer.read(&path, 0, &mut read).unwrap();
    assert_eq!(read, whole);
    // A range from inside the second record of the first append.
    let mut range = vec![0u8; 5000];
    writer
        .read(&path, RECORD_BYTES as u32 + 17, &mut range)
        .unwrap();
    assert_eq!(range, whole[RECORD_BYTES + 17..RECORD_BYTES + 17 + 5000]);
    // Opened again, the same.
    drop(writer);
    let writer = RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All).unwrap();
    assert_eq!(writer.plain(), whole.len() as u64);
}

/// A part whose last append a crash cut short (its last record half written, or the file run on in
/// zeros) is written again whole under a new ID before anything is appended: its whole records,
/// and nothing of the cut append's blocks sealed over under the old key.
#[test]
fn a_part_cut_short_is_written_again_under_a_new_key() {
    let most = 4 * RECORD_BYTES;
    let torn = records_bytes(RECORD_BYTES * 2).unwrap();
    let first = plaintext(4, RECORD_BYTES + 300);
    let cut = plaintext(5, RECORD_BYTES * 2);
    for damage in ["half a record", "zeros"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("upload.part");
        drop(part(&path, std::slice::from_ref(&first), most));
        let whole_length = fs::metadata(&path).unwrap().len();
        // The cut append: its first record whole, the next half written; or the file run on. Each
        // through a handle that may do it: Windows refuses to truncate through a handle that may
        // only append (`SetEndOfFile` needs `FILE_WRITE_DATA`).
        match damage {
            "half a record" => {
                let mut writer =
                    RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All)
                        .unwrap();
                writer.append(&cut).unwrap();
                drop(writer);
                let full = fs::metadata(&path).unwrap().len();
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(full - 4000)
                    .unwrap();
            }
            _ => OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(&vec![0u8; 3000])
                .unwrap(),
        }
        let before = header_id(&path);
        let mut writer =
            RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All).unwrap();
        assert_ne!(
            header_id(&path),
            before,
            "{damage}: written again under a new ID"
        );
        let expected = match damage {
            // The cut append's first record was whole and stays.
            "half a record" => [first.clone(), cut[..RECORD_BYTES].to_vec()].concat(),
            _ => first.clone(),
        };
        assert_eq!(writer.plain(), expected.len() as u64, "{damage}");
        assert!(fs::metadata(&path).unwrap().len() >= whole_length);
        assert!(!directory.path().join("upload.resume").exists());
        let more = plaintext(6, 1234);
        writer.append(&more).unwrap();
        writer.sync().unwrap();
        let all = [expected, more].concat();
        let mut read = vec![0u8; all.len()];
        writer.read(&path, 0, &mut read).unwrap();
        assert_eq!(read, all, "{damage}");
    }
}

/// A record that does not open further from the end than one append could have been cut is no
/// crash's: refused, as is a part of another kind or one holding more than it may.
#[test]
fn damage_behind_the_last_append_and_another_kinds_file_are_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("upload.part");
    let most = 8 * RECORD_BYTES;
    let appends = [
        plaintext(7, RECORD_BYTES),
        plaintext(8, RECORD_BYTES),
        plaintext(9, RECORD_BYTES),
    ];
    drop(part(&path, &appends, most));
    let torn = records_bytes(RECORD_BYTES).unwrap();
    let original = fs::read(&path).unwrap();
    // A byte flipped in the first record: two appends' worth lie past it.
    let mut flipped = original.clone();
    flipped[PART_HEADER + LENGTH_BLOCK + 10] ^= 1;
    fs::write(&path, &flipped).unwrap();
    assert!(matches!(
        RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All),
        Err(ContentError::Corrupt)
    ));
    // The first two records swapped.
    let record = RECORD_BYTES + RECORD_OVERHEAD;
    let mut swapped = original.clone();
    let (left, right) = swapped[PART_HEADER..PART_HEADER + 2 * record].split_at_mut(record);
    left.swap_with_slice(right);
    fs::write(&path, &swapped).unwrap();
    assert!(matches!(
        RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All),
        Err(ContentError::Corrupt)
    ));
    // Another kind's file, and more than the part may hold.
    fs::write(&path, &original).unwrap();
    assert!(matches!(
        RecordWriter::resume(&path, COLD_MAGIC, &key_of, most, torn, Verify::All),
        Err(ContentError::Corrupt)
    ));
    assert!(matches!(
        RecordWriter::resume(
            &path,
            PART_MAGIC,
            &key_of,
            2 * RECORD_BYTES,
            torn,
            Verify::All
        ),
        Err(ContentError::Corrupt)
    ));
    // Whole again, it opens.
    let writer = RecordWriter::resume(&path, PART_MAGIC, &key_of, most, torn, Verify::All).unwrap();
    assert_eq!(writer.plain(), 3 * RECORD_BYTES as u64);
}
