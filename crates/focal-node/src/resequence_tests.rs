use super::*;

fn drain(sequencer: &mut Resequencer<u64>) -> Vec<u64> {
    let mut out = Vec::new();
    while let Some(what) = sequencer.take_due() {
        out.push(what);
    }
    out
}
fn ready(sequencer: &mut Resequencer<u64>, source: u64) -> Vec<u64> {
    let mut out = Vec::new();
    while let Some(what) = sequencer.step_ready(source) {
        out.push(what);
    }
    out
}

/// Frames 1, 3, 2 from one source: 3 is held until 2 comes, then both
/// step in their order; a frame behind what was stepped is stale.
#[test]
fn a_frame_that_overtook_the_one_before_it_waits_for_it_and_steps_in_order() {
    let mut sequencer = Resequencer::new(4, 8);
    assert_eq!(sequencer.admit(7, 1, 1), Ok(Admission::Step));
    assert_eq!(sequencer.admit(7, 1, 3), Ok(Admission::Hold));
    sequencer.hold(7, 3, 3, 100).unwrap();
    assert_eq!(sequencer.held(), 1);
    assert!(ready(&mut sequencer, 7).is_empty());
    assert_eq!(sequencer.admit(7, 1, 2), Ok(Admission::Step));
    assert_eq!(ready(&mut sequencer, 7), vec![3]);
    assert_eq!(sequencer.admit(7, 1, 4), Ok(Admission::Step));
    assert_eq!(sequencer.admit(7, 1, 2), Ok(Admission::Stale));
    assert_eq!(sequencer.held(), 0);
    assert!(drain(&mut sequencer).is_empty());
}

/// A lane that is full lets everything it held go, in order, the frame
/// that came with it; the sequence expected next is past them all.
#[test]
fn a_full_lane_lets_what_it_held_go_in_order() {
    let mut sequencer = Resequencer::new(2, 8);
    assert_eq!(sequencer.admit(7, 1, 1), Ok(Admission::Step));
    assert_eq!(sequencer.admit(7, 1, 4), Ok(Admission::Hold));
    sequencer.hold(7, 4, 4, 100).unwrap();
    assert_eq!(sequencer.admit(7, 1, 3), Ok(Admission::Hold));
    sequencer.hold(7, 3, 3, 100).unwrap();
    assert_eq!(sequencer.admit(7, 1, 6), Ok(Admission::Hold));
    sequencer.hold(7, 6, 6, 100).unwrap();
    assert_eq!(drain(&mut sequencer), vec![3, 4, 6]);
    assert_eq!(sequencer.held(), 0);
    // 2 is behind what went; 7 is next.
    assert_eq!(sequencer.admit(7, 1, 2), Ok(Admission::Stale));
    assert_eq!(sequencer.admit(7, 1, 7), Ok(Admission::Step));
}

/// A frame held past its patience goes, and what was held consecutively
/// behind it with it: the one before it was lost.
#[test]
fn a_frame_held_past_its_patience_goes_with_what_waited_behind_it() {
    let mut sequencer = Resequencer::new(8, 8);
    assert_eq!(sequencer.admit(7, 1, 1), Ok(Admission::Step));
    for sequence in [3, 4, 6] {
        assert_eq!(sequencer.admit(7, 1, sequence), Ok(Admission::Hold));
        sequencer
            .hold(7, sequence, sequence, 10 + sequence)
            .unwrap();
    }
    sequencer.expire(12).unwrap();
    assert!(
        drain(&mut sequencer).is_empty(),
        "nothing is past its patience at 12"
    );
    sequencer.expire(13).unwrap();
    // 3 passed; 4 was consecutive behind it; 6 waits for 5 still.
    assert_eq!(drain(&mut sequencer), vec![3, 4]);
    assert_eq!(sequencer.held(), 1);
    assert_eq!(sequencer.admit(7, 1, 5), Ok(Admission::Step));
    assert_eq!(ready(&mut sequencer, 7), vec![6]);
}

/// A newer epoch from a source — it started over — resets its lane: what
/// the older epoch held goes first, in its order, and the older epoch's
/// late frames are stale.
#[test]
fn a_newer_epoch_resets_the_lane_and_the_older_is_stale() {
    let mut sequencer = Resequencer::new(8, 8);
    assert_eq!(sequencer.admit(7, 1, 1), Ok(Admission::Step));
    assert_eq!(sequencer.admit(7, 1, 3), Ok(Admission::Hold));
    sequencer.hold(7, 3, 3, 100).unwrap();
    assert_eq!(sequencer.admit(7, 2, 1), Ok(Admission::Step));
    assert_eq!(drain(&mut sequencer), vec![3]);
    assert_eq!(sequencer.admit(7, 1, 2), Ok(Admission::Stale));
    assert_eq!(sequencer.admit(7, 2, 2), Ok(Admission::Step));
}

/// Sources are bounded, and a source that left the configuration gives
/// back what it held.
#[test]
fn sources_are_bounded_and_a_departed_source_gives_back_what_it_held() {
    let mut sequencer = Resequencer::new(8, 2);
    assert_eq!(sequencer.admit(1, 1, 1), Ok(Admission::Step));
    assert_eq!(sequencer.admit(2, 1, 1), Ok(Admission::Step));
    assert_eq!(sequencer.admit(3, 1, 1), Err(Capacity));
    assert_eq!(sequencer.admit(2, 1, 3), Ok(Admission::Hold));
    sequencer.hold(2, 3, 23, 100).unwrap();
    let mut gone = Vec::new();
    sequencer.prune(|source| source != 2, &mut gone).unwrap();
    assert_eq!(gone, vec![23]);
    assert_eq!(sequencer.sources(), 1);
    assert_eq!(sequencer.admit(3, 1, 1), Ok(Admission::Step));
}
