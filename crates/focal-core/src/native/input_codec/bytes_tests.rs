use super::*;

fn fields(sink: &mut (impl Sink + ?Sized)) -> Result<(), Error> {
    write_u8(sink, 0xa5)?;
    write_u16(sink, 0x1234)?;
    write_u32(sink, 0x12345678)?;
    write_u64(sink, 0x0123456789abcdef)?;
    write_raw(sink, &[9, 8, 7])?;
    write_count(sink, 7)?;
    write_text(sink, "é🦀")?;
    write_raw(sink, &[])
}

fn read_fields(cursor: &mut Cursor<'_>) -> Result<(), Error> {
    assert_eq!(cursor.u8()?, 0xa5);
    assert_eq!(cursor.u16()?, 0x1234);
    assert_eq!(cursor.u32()?, 0x12345678);
    assert_eq!(cursor.u64()?, 0x0123456789abcdef);
    assert_eq!(cursor.fixed::<3>()?, [9, 8, 7]);
    assert_eq!(cursor.count(7)?, 7);
    assert_eq!(cursor.text(6)?, "é🦀");
    assert!(cursor.take(0)?.is_empty());
    Ok(())
}

#[test]
fn counting_and_unaligned_writes_match_an_independent_little_endian_vector() {
    let expected = [
        0xa5, 0x34, 0x12, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
        9, 8, 7, 7, 0, 0, 0, 6, 0, 0, 0, 0xc3, 0xa9, 0xf0, 0x9f, 0xa6, 0x80,
    ];
    let mut count = CountingSink::new(usize::MAX, usize::MAX);
    fields(&mut count).unwrap();
    assert_eq!(count.len(), expected.len());
    // Nine writes, including the text prefix/body and the empty raw field.
    assert_eq!(count.visits_used(), expected.len() + 9);
    let mut bytes = [0xcc; 40];
    let mut sink = SliceSink::new(&mut bytes[1..1 + expected.len()], count.visits_used());
    fields(&mut sink).unwrap();
    assert_eq!(sink.len(), count.len());
    assert_eq!(sink.visits_used(), count.visits_used());
    sink.finish().unwrap();
    assert_eq!(&bytes[1..1 + expected.len()], &expected);
    assert_eq!(bytes[0], 0xcc);
    assert!(bytes[1 + expected.len()..].iter().all(|byte| *byte == 0xcc));

    let mut cursor =
        Cursor::new(&bytes[1..1 + expected.len()], expected.len(), usize::MAX).unwrap();
    read_fields(&mut cursor).unwrap();
    assert_eq!(cursor.offset(), expected.len());
    assert_eq!(cursor.remaining(), 0);
    // Two checked counts and one additional UTF-8 pass over six bytes.
    assert_eq!(cursor.visits_used(), count.visits_used() + 2 + 7);
    cursor.finish().unwrap();
}

#[test]
fn every_truncated_prefix_refuses_without_reading_beyond_the_supplied_slice() {
    let mut bytes = [0u8; 32];
    let mut sink = SliceSink::new(&mut bytes, usize::MAX);
    fields(&mut sink).unwrap();
    sink.finish().unwrap();
    for cut in 0..bytes.len() {
        let mut cursor = Cursor::new(&bytes[..cut], cut, usize::MAX).unwrap();
        assert_eq!(
            read_fields(&mut cursor),
            Err(Error::Truncated),
            "prefix {cut}"
        );
        assert!(cursor.offset() <= cut);
        assert_eq!(cursor.offset() + cursor.remaining(), cut);
    }
    let mut trailing = Cursor::new(&[1, 2], 2, 2).unwrap();
    assert_eq!(trailing.u8().unwrap(), 1);
    assert_eq!(trailing.finish(), Err(Error::TrailingBytes));
    assert!(matches!(
        Cursor::new(&[0], 0, usize::MAX),
        Err(Error::Capacity)
    ));
}

#[test]
fn text_lengths_utf8_and_borrowing_are_checked_independently() {
    let malformed = [2, 0, 0, 0, 0xc3, 0x28];
    let mut cursor = Cursor::new(&malformed, malformed.len(), 12).unwrap();
    assert_eq!(cursor.text(2), Err(Error::InvalidUtf8));
    assert_eq!(cursor.offset(), malformed.len());
    assert_eq!(cursor.visits_used(), 12);

    let bytes = [3, 0, 0, 0, 0xc3, 0xa9, 0];
    let mut cursor = Cursor::new(&bytes, bytes.len(), 14).unwrap();
    let text = cursor.text(3).unwrap();
    assert_eq!(text, "é\0");
    assert_eq!(text.as_ptr(), bytes[4..].as_ptr());
    assert_eq!(cursor.visits_used(), 14);
    cursor.finish().unwrap();

    let maximum_count = u32::MAX.to_le_bytes();
    let mut oversized = Cursor::new(&maximum_count, 4, usize::MAX).unwrap();
    assert_eq!(oversized.count(3), Err(Error::Capacity));
    assert_eq!(oversized.offset(), 4);
    assert_eq!(oversized.visits_used(), 6);
    let mut unknown = Cursor::new(&[8, 0, 0, 0, 1, 2, 3], 7, usize::MAX).unwrap();
    assert_eq!(unknown.text(8), Err(Error::Truncated));
    assert_eq!(unknown.offset(), 4);
    assert_eq!(unknown.remaining(), 3);
}

#[test]
fn exact_reader_visit_bounds_include_empty_operations_and_intentional_checks() {
    let mut empty = Cursor::new(&[], 0, 0).unwrap();
    assert_eq!(empty.take(0), Err(Error::Capacity));
    assert_eq!(empty.visits_used(), 0);
    empty.finish().unwrap();
    let mut empty = Cursor::new(&[], 0, 1).unwrap();
    assert!(empty.fixed::<0>().unwrap().is_empty());
    assert_eq!(empty.visits_used(), 1);
    empty.finish().unwrap();

    for limit in 0..5 {
        let mut cursor = Cursor::new(&[1, 2, 3, 4], 4, limit).unwrap();
        assert_eq!(cursor.u32(), Err(Error::Capacity));
        assert_eq!(cursor.offset(), 0);
        assert_eq!(cursor.visits_used(), 0);
    }
    let mut cursor = Cursor::new(&[1, 2, 3, 4], 4, 5).unwrap();
    assert_eq!(cursor.u32().unwrap(), 0x04030201);
    assert_eq!(cursor.visits_used(), 5);
    cursor.finish().unwrap();

    let mut count = Cursor::new(&[1, 0, 0, 0], 4, 5).unwrap();
    assert_eq!(count.count(1), Err(Error::Capacity));
    assert_eq!(count.offset(), 4);
    assert_eq!(count.visits_used(), 5);
    let mut count = Cursor::new(&[1, 0, 0, 0], 4, 6).unwrap();
    assert_eq!(count.count(1), Ok(1));
    assert_eq!(count.visits_used(), 6);

    for limit in 0..8 {
        let mut text = Cursor::new(&[0, 0, 0, 0], 4, limit).unwrap();
        assert_eq!(text.text(0), Err(Error::Capacity));
        assert!(text.visits_used() <= limit);
    }
    let mut text = Cursor::new(&[0, 0, 0, 0], 4, 8).unwrap();
    assert_eq!(text.text(0), Ok(""));
    assert_eq!(text.visits_used(), 8);
    text.finish().unwrap();
}

#[test]
fn sink_refusals_preserve_written_bytes_counts_and_exact_fill_requirements() {
    let mut count = CountingSink::new(2, 3);
    assert_eq!(count.write(&[1, 2, 3]), Err(Error::Capacity));
    assert_eq!((count.len(), count.visits_used()), (0, 0));
    count.write(&[1, 2]).unwrap();
    assert_eq!((count.len(), count.visits_used()), (2, 3));
    assert_eq!(count.write(&[]), Err(Error::Capacity));
    assert_eq!((count.len(), count.visits_used()), (2, 3));

    let mut bytes = [0xcc; 3];
    let mut sink = SliceSink::new(&mut bytes, 4);
    sink.write(&[1]).unwrap();
    assert_eq!(sink.write(&[2, 3, 4]), Err(Error::Capacity));
    assert_eq!((sink.len(), sink.visits_used()), (1, 2));
    sink.write(&[2]).unwrap();
    assert_eq!(sink.write(&[3]), Err(Error::Capacity));
    assert_eq!((sink.len(), sink.visits_used()), (2, 4));
    assert_eq!(sink.finish(), Err(Error::Truncated));
    assert_eq!(bytes, [1, 2, 0xcc]);

    let mut empty = CountingSink::new(0, 1);
    empty.write(&[]).unwrap();
    assert_eq!((empty.len(), empty.visits_used()), (0, 1));
    let mut buffer = [];
    let mut empty = SliceSink::new(&mut buffer, 1);
    empty.write(&[]).unwrap();
    assert_eq!(empty.visits_used(), 1);
    empty.finish().unwrap();
}

#[test]
fn arithmetic_and_wire_count_overflow_refuse_without_hidden_wrap_or_allocation() {
    let mut cursor = Cursor::new(&[1], 1, usize::MAX).unwrap();
    assert_eq!(cursor.take(usize::MAX), Err(Error::Capacity));
    assert_eq!((cursor.offset(), cursor.visits_used()), (0, 0));
    cursor.visits.used = usize::MAX;
    assert_eq!(cursor.take(0), Err(Error::Capacity));
    assert_eq!(cursor.offset(), 0);

    let mut count = CountingSink::new(usize::MAX, usize::MAX);
    count.len = usize::MAX;
    assert_eq!(count.write(&[1]), Err(Error::Capacity));
    assert_eq!((count.len(), count.visits_used()), (usize::MAX, 0));
    count.len = 0;
    count.visits.used = usize::MAX;
    assert_eq!(count.write(&[]), Err(Error::Capacity));
    assert_eq!(count.len(), 0);

    if let Ok(too_large) = usize::try_from(u64::from(u32::MAX) + 1) {
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        assert_eq!(write_count(&mut count, too_large), Err(Error::Capacity));
        assert_eq!((count.len(), count.visits_used()), (0, 0));
    }
    let mut count = CountingSink::new(4, 5);
    write_u32(&mut count, u32::MAX).unwrap();
    assert_eq!((count.len(), count.visits_used()), (4, 5));
}

#[test]
fn explicit_non_byte_visits_have_identical_atomic_costs_in_both_sinks() {
    let mut count = CountingSink::new(1, 6);
    let mut bytes = [0xcc; 1];
    let mut sink = SliceSink::new(&mut bytes, 6);
    count.visit(0).unwrap();
    sink.visit(0).unwrap();
    assert_eq!((count.len(), count.visits_used()), (0, 0));
    assert_eq!((sink.len(), sink.visits_used()), (0, 0));
    count.visit(4).unwrap();
    sink.visit(4).unwrap();
    assert_eq!(count.visit(3), Err(Error::Capacity));
    assert_eq!(sink.visit(3), Err(Error::Capacity));
    assert_eq!((count.len(), count.visits_used()), (0, 4));
    assert_eq!((sink.len(), sink.visits_used()), (0, 4));
    write_u8(&mut count, 42).unwrap();
    write_u8(&mut sink, 42).unwrap();
    assert_eq!((count.len(), count.visits_used()), (1, 6));
    assert_eq!((sink.len(), sink.visits_used()), (1, 6));
    assert_eq!(count.visit(usize::MAX), Err(Error::Capacity));
    assert_eq!(sink.visit(usize::MAX), Err(Error::Capacity));
    assert_eq!(count.visits_used(), 6);
    assert_eq!(sink.visits_used(), 6);
    sink.finish().unwrap();
    assert_eq!(bytes, [42]);

    let mut bytes = [0xcc; 1];
    let mut sink = SliceSink::new(&mut bytes, usize::MAX);
    assert_eq!(sink.write(&[1, 2]), Err(Error::Capacity));
    assert_eq!((sink.len(), sink.visits_used()), (0, 0));
    assert_eq!(sink.finish(), Err(Error::Truncated));
    assert_eq!(bytes, [0xcc]);
}
