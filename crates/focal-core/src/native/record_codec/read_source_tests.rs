use super::*;

#[test]
fn nested_decoder_cannot_spend_the_allowance_held_by_an_outer_cursor() {
    let meter = Meter::new(4);
    let reads = Cell::new(0);
    let result = meter
        .read(&[7, 8], |cursor| {
            assert_eq!(cursor.u8()?, 7);
            reads.set(reads.get() + 1);
            // Two visits remain in the cursor. A second cursor must not receive
            // those visits while the first one is still entitled to spend them.
            assert_eq!(
                meter.read(&[9], |nested| {
                    let value = nested.u8()?;
                    reads.set(reads.get() + 1);
                    Ok(value)
                }),
                Err(CodecError::Capacity)
            );
            let value = cursor.u8()?;
            reads.set(reads.get() + 1);
            Ok(value)
        })
        .unwrap();
    assert_eq!(result, (8, 2));
    assert_eq!(reads.get(), 2);
    assert_eq!(meter.remaining(), 0);
}

#[test]
fn truncated_source_keeps_consumed_prefix_work_charged_and_refunds_only_unused_work() {
    let meter = Meter::new(10);
    assert_eq!(
        meter.read(&[1], |cursor| {
            cursor.u8()?;
            cursor.u64()
        }),
        Err(CodecError::Capacity)
    );
    assert_eq!(meter.remaining(), 8);
    assert_eq!(meter.read(&[], Cursor::u8), Err(CodecError::Truncated));
    assert_eq!(meter.remaining(), 8);
    assert_eq!(meter.read(&[2, 3], Cursor::u16).unwrap(), (770, 2));
    assert_eq!(meter.remaining(), 5);
}

#[test]
fn opaque_model_inspection_and_graph_work_cannot_reenter_their_own_budget() {
    let meter = Meter::new(10);
    meter
        .model(|allowance| {
            assert_eq!(allowance, 10);
            assert_eq!(meter.charge(1), Err(CodecError::Capacity));
            Ok(((), 4))
        })
        .unwrap();
    assert_eq!(meter.remaining(), 6);
    assert_eq!(
        meter.budget(|visits| {
            visits.charge(2)?;
            assert_eq!(meter.charge(1), Err(CodecError::Capacity));
            Err::<(), _>(ContractError::InvalidManifest)
        }),
        Err(ContractError::InvalidManifest)
    );
    assert_eq!(meter.remaining(), 4);
    assert_eq!(
        meter.model(|allowance| {
            assert_eq!(allowance, 4);
            Err::<((), usize), _>(ContractError::InvalidManifest)
        }),
        Err(ContractError::InvalidManifest)
    );
    // Opaque failure provides no trustworthy work quote; its entire loan is
    // consumed instead of silently giving another attempt the same allowance.
    assert_eq!(meter.remaining(), 0);
}
