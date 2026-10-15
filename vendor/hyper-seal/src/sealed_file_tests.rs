//! A sealed file reads back as written at every size and alignment, and every change an attacker
//! can make to its bytes is refused, typed: a flipped byte anywhere, a cut at or between segment
//! boundaries, an extension, a segment spliced from another file, two segments swapped, the wrong
//! parent, a header pointed at another key, a forged segment size, padding or a footer changed.
use super::*;
use crate::stream::MIN_SEGMENT;
use hyper_block::sim::SimFile;

/// The smallest segment the construction takes, so a few KiB cross many segment boundaries.
const S: u32 = MIN_SEGMENT;
/// The same, as a length.
const SU: usize = S as usize;

fn align(n: usize) -> Alignment {
    Alignment::new(n).unwrap()
}

fn sim(a: usize) -> SimFile {
    SimFile::new(align(a), align(a.min(512)), 7).unwrap()
}

fn plaintext(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(31) ^ (i >> 8)) as u8)
        .collect()
}

/// The file's bytes after a writer of `plain` in `a`-aligned blocks finished it.
fn sealed(parent: &WrappingKey, plain: &[u8], a: usize) -> Vec<u8> {
    let mut writer = SealedWriter::new(sim(a), parent, S).unwrap();
    // In uneven pieces, so a segment is filled across writes.
    for piece in plain.chunks(97) {
        writer.write(piece).unwrap();
    }
    writer.finish().unwrap().durable_image().unwrap()
}

/// `bytes` in a buffer at alignment `a`, as a direct transfer needs.
fn aligned(bytes: &[u8], a: usize) -> AlignedBuf {
    let mut buf = AlignedBuf::zeroed(bytes.len().max(a), align(a)).unwrap();
    buf.as_mut_capacity()[..bytes.len()].copy_from_slice(bytes);
    buf.set_len(bytes.len()).unwrap();
    buf
}

/// A file holding exactly `bytes`, `a`-aligned.
fn file_of(bytes: &[u8], a: usize) -> SimFile {
    let file = sim(a);
    if !bytes.is_empty() {
        file.write_all_at(aligned(bytes, a).as_slice(), 0).unwrap();
    }
    file
}

fn open(bytes: &[u8], parent: &WrappingKey, a: usize) -> Result<Vec<u8>, SealedFileError> {
    let mut reader = SealedReader::open(file_of(bytes, a), parent, S)?;
    let mut out =
        vec![0; usize::try_from(reader.len()).map_err(|_| SealedFileError::Bound("len"))?];
    reader.read_at(&mut out, 0)?;
    Ok(out)
}

/// The stream of a file: its bytes before the padding, by its footer.
fn stream_of(file: &[u8]) -> Vec<u8> {
    let stated = u64::from_le_bytes(file[file.len() - 8..].try_into().unwrap());
    file[..usize::try_from(stated).unwrap()].to_vec()
}

/// A stream laid out as a writer lays it: padded and footed for alignment `a`.
fn file_from_stream(stream: &[u8], a: usize) -> Vec<u8> {
    let end = (stream.len() + 8).div_ceil(a) * a;
    let mut out = stream.to_vec();
    out.resize(end - 8, 0);
    out.extend_from_slice(&(stream.len() as u64).to_le_bytes());
    out
}

/// Where segment `i`'s stored bytes begin in a stream.
fn seg(i: usize) -> usize {
    HEADER + i * (SU + TAG)
}

fn parent() -> WrappingKey {
    WrappingKey::generate(0).unwrap()
}

fn refused_open(result: Result<Vec<u8>, SealedFileError>) -> bool {
    matches!(result, Err(SealedFileError::Seal(SealError::Open)))
}

#[test]
fn every_size_and_alignment_reads_back() {
    let key = parent();
    for a in [1, 512, 4096] {
        for len in [0, 1, SU - 1, SU, SU + 1, 3 * SU, 3 * SU + 7, 9 * SU - 1] {
            let plain = plaintext(len);
            let bytes = sealed(&key, &plain, a);
            assert_eq!(bytes.len() % a, 0);
            assert_eq!(
                stream_of(&bytes).len() as u64,
                crate::stream::sealed_len(len as u64, S).unwrap(),
                "the stream is STREAM's, len {len}"
            );
            assert_eq!(open(&bytes, &key, a).unwrap(), plain, "len {len} align {a}");
        }
    }
}

#[test]
fn any_range_reads_back_opening_only_its_segments() {
    let key = parent();
    let plain = plaintext(5 * SU + 33);
    let bytes = sealed(&key, &plain, 4096);
    let mut reader = SealedReader::open(file_of(&bytes, 4096), &key, S).unwrap();
    for (from, len) in [
        (0, 1),
        (SU - 1, 2),
        (SU, SU),
        (3 * SU + 5, 2 * SU + 28),
        (5 * SU, 33),
    ] {
        let mut out = vec![0; len];
        reader.read_at(&mut out, from as u64).unwrap();
        assert_eq!(out, plain[from..from + len]);
    }
    let mut past = [0u8; 2];
    assert!(matches!(
        reader.read_at(&mut past, plain.len() as u64 - 1),
        Err(SealedFileError::Bound(_))
    ));
}

#[test]
fn a_flipped_byte_anywhere_is_refused() {
    let key = parent();
    let plain = plaintext(3 * SU + 10);
    for a in [1, 512] {
        let bytes = sealed(&key, &plain, a);
        let stream_len = stream_of(&bytes).len();
        for at in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[at] ^= 0x40;
            let result = open(&bad, &key, a);
            assert!(result.is_err(), "a flip at {at} of {} opened", bytes.len());
            if (HEADER..stream_len).contains(&at) {
                assert!(refused_open(result), "a flip in a segment or tag at {at}");
            }
        }
    }
}

#[test]
fn a_cut_at_or_between_segment_boundaries_is_refused() {
    let key = parent();
    let a = 512;
    let stream = stream_of(&sealed(&key, &plaintext(4 * SU), a));
    // At each boundary: whole segments remain, but the new last was not sealed last.
    for k in 1..4 {
        let cut = file_from_stream(&stream[..seg(k)], a);
        assert!(refused_open(open(&cut, &key, a)), "cut after segment {k}");
    }
    // Between boundaries: a tag lands where none was sealed.
    for at in [seg(1) + 1, seg(2) + SU / 2, stream.len() - 1] {
        let cut = file_from_stream(&stream[..at], a);
        assert!(open(&cut, &key, a).is_err(), "cut at {at}");
    }
    // The file cut without its footer restated: its length is not the footer's.
    let file = sealed(&key, &plaintext(4 * SU), a);
    assert!(matches!(
        open(&file[..file.len() - a], &key, a),
        Err(SealedFileError::Layout(_))
    ));
}

#[test]
fn an_extended_file_is_refused() {
    let key = parent();
    let a = 512;
    let stream = stream_of(&sealed(&key, &plaintext(2 * SU + 3), a));
    let mut longer = stream.clone();
    longer.extend_from_slice(&vec![0xAB; SU + TAG]);
    assert!(open(&file_from_stream(&longer, a), &key, a).is_err());
    // Its last segment's stored bytes repeated after it.
    let mut again = stream.clone();
    again.extend_from_slice(&stream[seg(2)..]);
    assert!(open(&file_from_stream(&again, a), &key, a).is_err());
}

#[test]
fn a_segment_spliced_from_another_file_under_the_same_key_is_refused() {
    let key = parent();
    let a = 512;
    let one = stream_of(&sealed(&key, &plaintext(3 * SU), a));
    let two = stream_of(&sealed(&key, &plaintext(3 * SU), a));
    let mut spliced = one.clone();
    spliced[seg(1)..seg(2)].copy_from_slice(&two[seg(1)..seg(2)]);
    assert!(refused_open(open(&file_from_stream(&spliced, a), &key, a)));
}

#[test]
fn two_segments_swapped_are_refused() {
    let key = parent();
    let a = 512;
    let stream = stream_of(&sealed(&key, &plaintext(4 * SU), a));
    let mut swapped = stream.clone();
    swapped[seg(1)..seg(2)].copy_from_slice(&stream[seg(2)..seg(3)]);
    swapped[seg(2)..seg(3)].copy_from_slice(&stream[seg(1)..seg(2)]);
    assert!(refused_open(open(&file_from_stream(&swapped, a), &key, a)));
}

#[test]
fn the_wrong_parent_is_refused() {
    let a = 512;
    let bytes = sealed(&parent(), &plaintext(SU + 1), a);
    assert!(matches!(
        open(&bytes, &parent(), a),
        Err(SealedFileError::Seal(SealError::Unwrap))
    ));
}

#[test]
fn a_header_pointed_at_another_key_is_refused() {
    let key = parent();
    let a = 512;
    let mine = stream_of(&sealed(&key, &plaintext(2 * SU), a));
    let other = stream_of(&sealed(&key, &plaintext(2 * SU), a));
    // The other file's key record, which unwraps under the same parent: the commitment refuses it.
    let record = 1 + 4 + 16;
    let mut bad = mine.clone();
    bad[record..record + crate::keys::RECORD]
        .copy_from_slice(&other[record..record + crate::keys::RECORD]);
    assert!(refused_open(open(&file_from_stream(&bad, a), &key, a)));
    // Its key record and commitment both: segment 0 binds the commitment and the file ID.
    let mut both = mine.clone();
    both[record..HEADER].copy_from_slice(&other[record..HEADER]);
    assert!(refused_open(open(&file_from_stream(&both, a), &key, a)));
}

#[test]
fn a_forged_segment_size_is_refused_before_its_buffer_is_made() {
    let key = parent();
    let a = 512;
    let mut stream = stream_of(&sealed(&key, &plaintext(SU), a));
    stream[1..5].copy_from_slice(&(1u32 << 29).to_le_bytes());
    assert!(matches!(
        open(&file_from_stream(&stream, a), &key, a),
        Err(SealedFileError::Layout(_))
    ));
}

#[test]
fn padding_and_footer_are_checked() {
    let key = parent();
    let a = 512;
    let file = sealed(&key, &plaintext(SU + 9), a);
    let stream = stream_of(&file).len();
    let mut padded = file.clone();
    padded[stream] = 1;
    assert!(matches!(
        open(&padded, &key, a),
        Err(SealedFileError::Layout(_))
    ));
    let mut footer = file.clone();
    let at = footer.len() - 8;
    footer[at..].copy_from_slice(&(stream as u64 + 1).to_le_bytes());
    assert!(open(&footer, &key, a).is_err());
    assert!(matches!(
        open(&[], &key, a),
        Err(SealedFileError::Layout(_))
    ));
}

#[test]
fn a_file_with_bytes_is_never_written_over() {
    let file = sim(512);
    file.write_all_at(aligned(&[1; 512], 512).as_slice(), 0)
        .unwrap();
    assert!(matches!(
        SealedWriter::new(file, &parent(), S),
        Err(SealedFileError::Bound(_))
    ));
}

#[test]
fn a_crash_before_finish_leaves_nothing_a_reader_takes() {
    let key = parent();
    let mut writer = SealedWriter::new(sim(512), &key, S).unwrap();
    writer.write(&plaintext(4 * SU)).unwrap();
    // Dropped unfinished: segments may be on the device, but no footer and no flush.
    let image = {
        let file = writer.file_for_test();
        file.durable_image().unwrap()
    };
    assert!(open(&image, &key, 512).is_err());
}

#[test]
fn a_file_on_a_real_disk_is_renamed_into_place_and_reads_back() {
    use hyper_block::file::{CachingRequest, DeviceFile, rename_durable};
    let dir = tempfile::tempdir().unwrap();
    let key = parent();
    let plain = plaintext(64 * 1024 + 123);
    let temporary = dir.path().join("image.new");
    let path = dir.path().join("image");
    let file =
        DeviceFile::open(&temporary, true, CachingRequest::PreferDirect, align(4096)).unwrap();
    let mut writer = SealedWriter::new(file, &key, 4096).unwrap();
    writer.write(&plain).unwrap();
    drop(writer.finish().unwrap());
    rename_durable(&temporary, &path).unwrap();
    let file = DeviceFile::open(&path, false, CachingRequest::PreferDirect, align(4096)).unwrap();
    let mut reader = SealedReader::open(file, &key, 4096).unwrap();
    let mut out = vec![0; plain.len()];
    reader.read_at(&mut out, 0).unwrap();
    assert_eq!(out, plain);
    assert!(!temporary.exists());
}
