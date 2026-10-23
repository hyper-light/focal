use super::*;
use std::fs;

fn content_key() -> WrappingKey {
    hyper_seal::lock_keys(focal_seal::key_slots()).unwrap();
    WrappingKey::generate(0).unwrap()
}

fn domain_dir(root: &Path, domain: ContentDomainId) -> PathBuf {
    let directory = root.join(hex(&domain.0));
    fs::create_dir_all(&directory).unwrap();
    directory
}

/// Plaintext no stored byte may repeat: long and patterned, so a copy of any 16 bytes of it shows.
fn plaintext(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 8)) as u8)
        .collect()
}

fn hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

/// A converting store's writer keys: they seal, and a file may still lie in the clear.
fn converting(content: WrappingKey) -> ContentKeys<'static> {
    ContentKeys {
        content: Content::Held(content),
        clear: true,
    }
}

/// A chunk installed opens to its bytes, under the name its domain gives its hash, and installs to
/// the same file again; its domain's key is made once and found by a second opening.
#[test]
fn a_chunk_installed_opens_to_its_bytes_and_installs_to_the_same_file() {
    let root = tempfile::tempdir().unwrap();
    let keys = ContentKeys::new(content_key());
    let domain = ContentDomainId::from_u128(7);
    let directory = domain_dir(root.path(), domain);
    let bytes = plaintext(200_000);
    let key = keys.domain(&directory, domain, true).unwrap();
    let path = key.path(&directory, hash(&bytes), Kind::Chunk).unwrap();
    key.install(&path, &bytes).unwrap();
    let first = fs::read(&path).unwrap();
    let mut scratch = vec![0; bytes.len()];
    assert_eq!(
        key.open_into(
            &directory,
            hash(&bytes),
            Kind::Chunk,
            bytes.len(),
            &mut scratch
        )
        .unwrap(),
        &bytes[..]
    );
    assert_eq!(
        key.plain_length(&directory, hash(&bytes), Kind::Chunk)
            .unwrap(),
        bytes.len() as u64
    );
    // Opened again from its record, the domain's key and names are the same.
    let again = keys.domain(&directory, domain, false).unwrap();
    assert_eq!(
        again.path(&directory, hash(&bytes), Kind::Chunk).unwrap(),
        path
    );
    again.install(&path, &bytes).unwrap();
    assert_eq!(
        fs::read(&path).unwrap(),
        first,
        "the same bytes, the same file"
    );
    assert_eq!(fs::read(directory.join(DOMAIN_KEY)).unwrap().len(), RECORD);
}

/// What an attacker can do to a stored chunk is refused: a flipped byte, a cut, a file put under
/// another chunk's name, and a chunk carried into another domain's directory.
#[test]
fn a_changed_cut_renamed_or_moved_chunk_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let keys = ContentKeys::new(content_key());
    let one = ContentDomainId::from_u128(1);
    let two = ContentDomainId::from_u128(2);
    let (dir_one, dir_two) = (domain_dir(root.path(), one), domain_dir(root.path(), two));
    let key_one = keys.domain(&dir_one, one, true).unwrap();
    let key_two = keys.domain(&dir_two, two, true).unwrap();
    let a = plaintext(70_000);
    let b = plaintext(70_001);
    let path_a = key_one.path(&dir_one, hash(&a), Kind::Chunk).unwrap();
    let path_b = key_one.path(&dir_one, hash(&b), Kind::Chunk).unwrap();
    key_one.install(&path_a, &a).unwrap();
    let sealed = fs::read(&path_a).unwrap();
    let mut scratch = vec![0; 70_001];
    for at in [0, 100, sealed.len() / 2, sealed.len() - 9] {
        let mut flipped = sealed.clone();
        flipped[at] ^= 1;
        fs::write(&path_a, &flipped).unwrap();
        assert!(
            key_one
                .open_into(&dir_one, hash(&a), Kind::Chunk, a.len(), &mut scratch)
                .is_err(),
            "flipped at {at}"
        );
    }
    fs::write(&path_a, &sealed[..sealed.len() - 1]).unwrap();
    assert!(
        key_one
            .open_into(&dir_one, hash(&a), Kind::Chunk, a.len(), &mut scratch)
            .is_err()
    );
    // A's file under B's name opens, but to bytes that are not B's.
    fs::write(&path_b, &sealed).unwrap();
    assert!(matches!(
        key_one.open_into(&dir_one, hash(&b), Kind::Chunk, b.len(), &mut scratch),
        Err(ContentError::Corrupt)
    ));
    // A's file carried into domain two opens under none of its keys.
    let moved = key_two.path(&dir_two, hash(&a), Kind::Chunk).unwrap();
    fs::write(&moved, &sealed).unwrap();
    assert!(
        key_two
            .open_into(&dir_two, hash(&a), Kind::Chunk, a.len(), &mut scratch)
            .is_err()
    );
}

/// No stored name holds a chunk's hash and no stored byte its plaintext: a listing and the files'
/// bytes say nothing of what the domain holds.
#[test]
fn no_stored_name_or_byte_holds_a_hash_or_plaintext() {
    let root = tempfile::tempdir().unwrap();
    let keys = ContentKeys::new(content_key());
    let domain = ContentDomainId::from_u128(3);
    let directory = domain_dir(root.path(), domain);
    let key = keys.domain(&directory, domain, true).unwrap();
    let bytes = plaintext(150_000);
    let path = key.path(&directory, hash(&bytes), Kind::Chunk).unwrap();
    key.install(&path, &bytes).unwrap();
    let names: Vec<String> = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let hashed = hash(&bytes).0;
    let named = hex(&hashed);
    assert!(
        names.iter().all(|name| !name.contains(&named[..16])),
        "{names:?}"
    );
    for name in &names {
        let stored = fs::read(directory.join(name)).unwrap();
        for window in bytes.chunks(4096).take(8) {
            let probe = &window[..16];
            assert!(
                !stored.windows(16).any(|w| w == probe),
                "{name} holds plaintext"
            );
        }
        assert!(
            !stored.windows(32).any(|w| w == hashed),
            "{name} holds the hash"
        );
    }
}

/// A domain's record destroyed, its chunks open no more while another domain's still do: the
/// erase 29 §11 asks of a domain.
#[test]
fn a_domain_erased_by_its_record_reads_no_more_while_another_does() {
    let root = tempfile::tempdir().unwrap();
    let keys = ContentKeys::new(content_key());
    let (gone, kept) = (ContentDomainId::from_u128(4), ContentDomainId::from_u128(5));
    let (dir_gone, dir_kept) = (domain_dir(root.path(), gone), domain_dir(root.path(), kept));
    let bytes = plaintext(40_000);
    let mut scratch = vec![0; bytes.len()];
    let mut paths = Vec::new();
    for (directory, domain) in [(&dir_gone, gone), (&dir_kept, kept)] {
        let key = keys.domain(directory, domain, true).unwrap();
        let path = key.path(directory, hash(&bytes), Kind::Chunk).unwrap();
        key.install(&path, &bytes).unwrap();
        paths.push(path);
    }
    fs::remove_file(dir_gone.join(DOMAIN_KEY)).unwrap();
    let erased = keys.domain(&dir_gone, gone, false).unwrap();
    assert!(erased.holds_nothing());
    assert!(matches!(
        erased.open_into(&dir_gone, hash(&bytes), Kind::Chunk, bytes.len(), &mut scratch),
        Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    // Made again, the domain's key is another: the erased chunk does not open under it, at its own
    // name or at the new key's.
    let fresh = keys.domain(&dir_gone, gone, true).unwrap();
    let renamed = fresh.path(&dir_gone, hash(&bytes), Kind::Chunk).unwrap();
    assert_ne!(renamed, paths[0]);
    fs::copy(&paths[0], &renamed).unwrap();
    assert!(
        fresh
            .open_into(
                &dir_gone,
                hash(&bytes),
                Kind::Chunk,
                bytes.len(),
                &mut scratch
            )
            .is_err()
    );
    let key = keys.domain(&dir_kept, kept, false).unwrap();
    assert_eq!(
        key.open_into(
            &dir_kept,
            hash(&bytes),
            Kind::Chunk,
            bytes.len(),
            &mut scratch
        )
        .unwrap(),
        &bytes[..]
    );
    assert_eq!(
        key.path(&dir_kept, hash(&bytes), Kind::Chunk).unwrap(),
        paths[1]
    );
}

/// A domain's record that does not unwrap under the content key is corrupt, never a new key.
#[test]
fn a_domain_record_under_another_key_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let domain = ContentDomainId::from_u128(6);
    let directory = domain_dir(root.path(), domain);
    ContentKeys::new(content_key())
        .domain(&directory, domain, true)
        .unwrap();
    let other = ContentKeys::new(content_key());
    assert!(matches!(
        other.domain(&directory, domain, true),
        Err(ContentError::Corrupt)
    ));
}

/// Below the fence the writer keeps the layout every earlier binary wrote and reads: each file in
/// the clear, byte for byte its bytes, named by its hash, and no key record made.
#[test]
fn the_clear_form_writes_the_layout_earlier_binaries_read() {
    let root = tempfile::tempdir().unwrap();
    let keys = ContentKeys::clear();
    let domain = ContentDomainId::from_u128(8);
    let directory = domain_dir(root.path(), domain);
    let key = keys.domain(&directory, domain, true).unwrap();
    assert!(!key.holds_nothing());
    assert_eq!(key.record_bytes(), 0);
    assert_eq!(key.file_bytes(1000).unwrap(), 1000);
    let bytes = plaintext(30_000);
    let path = key.path(&directory, hash(&bytes), Kind::Chunk).unwrap();
    assert_eq!(path, directory.join(format!("{}.chunk", hash(&bytes))));
    key.install(&path, &bytes).unwrap();
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert!(!directory.join(DOMAIN_KEY).exists());
    let mut scratch = vec![0; bytes.len()];
    assert_eq!(
        key.open_into(
            &directory,
            hash(&bytes),
            Kind::Chunk,
            bytes.len(),
            &mut scratch
        )
        .unwrap(),
        &bytes[..]
    );
    // A listed manifest in the clear is named by its root, read as it is.
    let manifest = plaintext(500);
    let listed = key
        .path(&directory, hash(&manifest), Kind::Manifest)
        .unwrap();
    key.install(&listed, &manifest).unwrap();
    assert_eq!(
        key.open_listed(&listed, 1 << 20).unwrap(),
        Some((manifest.clone(), hash(&manifest)))
    );
    assert!(key.is_name(&hash(&manifest).to_string()));
    assert!(!key.is_name(&hash(&manifest).to_string()[..32]));
}

/// While a store converts, a file is found in either form, the clear looked at first: one still in
/// the clear, one sealed by the writer since, and one whose bytes in the clear were damaged but
/// sealed whole over them; a file in neither is not found. Marked sealed, the clear is looked at no
/// more.
#[test]
fn a_converting_store_finds_each_file_in_either_form() {
    let root = tempfile::tempdir().unwrap();
    let domain = ContentDomainId::from_u128(9);
    let directory = domain_dir(root.path(), domain);
    let clear = ContentKeys::clear()
        .domain(&directory, domain, true)
        .unwrap();
    let old = plaintext(20_000);
    let old_clear = clear.path(&directory, hash(&old), Kind::Chunk).unwrap();
    clear.install(&old_clear, &old).unwrap();
    let mut keys = converting(content_key());
    // No record yet: what lies in the clear is found, and a write makes the record first.
    let before = keys.domain(&directory, domain, false).unwrap();
    assert!(!before.holds_nothing());
    assert_eq!(before.record_bytes(), RECORD as u64);
    let mut scratch = vec![0; 40_000];
    assert_eq!(
        before
            .open_into(&directory, hash(&old), Kind::Chunk, old.len(), &mut scratch)
            .unwrap(),
        &old[..]
    );
    let key = keys.writable(&directory, domain, before).unwrap();
    assert_eq!(key.record_bytes(), 0);
    let new = plaintext(30_000);
    let new_sealed = key.path(&directory, hash(&new), Kind::Chunk).unwrap();
    key.install(&new_sealed, &new).unwrap();
    assert_eq!(
        new_sealed.file_stem().unwrap().to_str().unwrap().len(),
        KEYED_NAME
    );
    assert_eq!(
        key.open_into(&directory, hash(&new), Kind::Chunk, new.len(), &mut scratch)
            .unwrap(),
        &new[..]
    );
    assert_eq!(
        key.plain_length(&directory, hash(&old), Kind::Chunk)
            .unwrap(),
        old.len() as u64
    );
    // The bytes in the clear damaged, their sealed form installed over them serves.
    let mut bytes = fs::read(&old_clear).unwrap();
    bytes[7] ^= 1;
    fs::write(&old_clear, &bytes).unwrap();
    assert!(matches!(
        key.open_into(&directory, hash(&old), Kind::Chunk, old.len(), &mut scratch),
        Err(ContentError::Corrupt)
    ));
    let old_sealed = key.path(&directory, hash(&old), Kind::Chunk).unwrap();
    key.install(&old_sealed, &old).unwrap();
    let (path, opened) = key
        .opened(&directory, hash(&old), Kind::Chunk, old.len(), &mut scratch)
        .unwrap();
    assert_eq!(opened, &old[..]);
    assert_eq!(path, old_sealed);
    let absent = hash(b"absent");
    assert!(!key.exists(&directory, absent, Kind::Chunk).unwrap());
    assert!(matches!(
        key.open_into(&directory, absent, Kind::Chunk, 6, &mut scratch),
        Err(ContentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    // Marked sealed, the clear is looked at no more: a file only in the clear is not found.
    keys.mark_sealed();
    fs::remove_file(&new_sealed).unwrap();
    fs::write(
        clear.path(&directory, hash(&new), Kind::Chunk).unwrap(),
        &new,
    )
    .unwrap();
    let sealed = keys.domain(&directory, domain, false).unwrap();
    assert!(!sealed.exists(&directory, hash(&new), Kind::Chunk).unwrap());
    assert!(sealed.exists(&directory, hash(&old), Kind::Chunk).unwrap());
}

/// The collector's mark of a file is what it reads back from the file's name, in either form, and
/// a name the writer does not file with has none: a chunk's mark costs 32 bytes, as a hash did.
#[test]
fn a_mark_is_read_back_from_the_name_it_marks() {
    let root = tempfile::tempdir().unwrap();
    let domain = ContentDomainId::from_u128(10);
    let directory = domain_dir(root.path(), domain);
    let bytes = plaintext(1000);
    let sealing = ContentKeys::new(content_key());
    let sealed = sealing.domain(&directory, domain, true).unwrap();
    let clear_keys = ContentKeys::clear();
    let clear = clear_keys.domain(&directory, domain, true).unwrap();
    for (keys, key) in [(&sealing, &sealed), (&clear_keys, &clear)] {
        let path = key.path(&directory, hash(&bytes), Kind::Chunk).unwrap();
        let stem = path.file_stem().unwrap().to_str().unwrap();
        assert_eq!(keys.mark_of(stem), Some(key.mark(hash(&bytes)).unwrap()));
    }
    assert_ne!(
        sealed.mark(hash(&bytes)).unwrap(),
        clear.mark(hash(&bytes)).unwrap()
    );
    assert_eq!(sealing.mark_of(&hash(&bytes).to_string()), None);
    assert_eq!(
        clear_keys.mark_of(&sealed.name(hash(&bytes)).unwrap()),
        None
    );
    assert_eq!(sealing.mark_of("DOMAIN"), None);
    assert_eq!(size_of::<Mark>(), size_of::<ContentHash>());
}
