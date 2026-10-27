use super::*;

fn budget() -> MemoryBudget {
    MemoryBudget::new(1 << 20, 0).unwrap()
}
const SAMPLE: usize = size_of::<(SessionSeq, u64)>();

/// A record is dated by the first sample at or past it, which bounds from above when it settled
/// (logical time never falls along the sequence); none before a sample is taken there.
#[test]
fn a_record_is_dated_by_the_first_sample_at_or_past_it() {
    let budget = budget();
    let mut settled = Settled::default();
    assert_eq!(settled.by(SessionSeq(1)), None);
    settled.sample(&budget, SessionSeq(10), 100, 100, 1_000);
    settled.sample(&budget, SessionSeq(20), 103, 103, 1_000);
    assert_eq!(settled.by(SessionSeq(1)), Some(100));
    assert_eq!(settled.by(SessionSeq(10)), Some(100));
    assert_eq!(settled.by(SessionSeq(11)), Some(103));
    assert_eq!(settled.by(SessionSeq(20)), Some(103));
    assert_eq!(settled.by(SessionSeq(21)), None);
    // No record since the last sample, or one sooner than the spacing, adds none.
    settled.sample(&budget, SessionSeq(20), 900, 900, 1_000);
    assert_eq!(settled.samples.len(), 2);
    settled.sample(&budget, SessionSeq(30), 103, 103, 1_000);
    assert_eq!(settled.samples.len(), 2);
    assert_eq!(settled.by(SessionSeq(30)), None);
}

/// Samples are spaced by the grace and let go once the newest at or before `now - grace` dates
/// every record before it as due: a family settled long ago is still dated, as due.
#[test]
fn samples_are_spaced_by_the_grace_and_let_go_past_it() {
    let budget = budget();
    let mut settled = Settled::default();
    let grace = 512_000;
    // Spaced a thousandth of the grace's half apart: 1,000 ms.
    for (index, time) in [1u64, 501, 1_001, 2_000, 2_001].into_iter().enumerate() {
        settled.sample(&budget, SessionSeq(index as u64 + 1), time, time, grace);
    }
    let kept: Vec<_> = settled.samples.iter().map(|(_, time)| *time).collect();
    assert_eq!(kept, [1, 1_001, 2_001]);
    // Long after: the samples at or before the cut go but the newest of them, which dates every
    // record before it as due.
    let now = 2_001 + grace + 10_000;
    settled.sample(&budget, SessionSeq(100), now, now, grace);
    let kept: Vec<_> = settled.samples.iter().map(|(_, time)| *time).collect();
    assert_eq!(kept, [2_001, now]);
    assert_eq!(settled.by(SessionSeq(1)), Some(2_001));
    assert!(now - settled.by(SessionSeq(1)).unwrap() >= grace);
}

/// Under one grace the spacing and the cut hold half the bound and a little more; a grace that
/// grows keeps what a smaller one sampled densely, and the bound lets the oldest go first, which
/// only dates a record later. What the buffer holds is what it is charged, and a sample the
/// budget cannot fund is not taken.
#[test]
fn the_samples_are_bounded_and_charged_as_their_buffer_grows() {
    let budget = budget();
    let mut settled = Settled::default();
    let mut most = 0;
    let mut check = |settled: &Settled| {
        assert!(settled.samples.len() <= SETTLE_SAMPLES);
        assert!(settled.samples.capacity() <= SETTLE_SAMPLES);
        assert_eq!(
            settled.charge.as_ref().unwrap().bytes(),
            settled.samples.capacity() * SAMPLE,
        );
        most = most.max(settled.samples.len());
    };
    // A grace of 512 ms: a sample a millisecond, the last 512 ms of them kept.
    let mut sequence = 0;
    for time in 1..=2_000u64 {
        sequence += 1;
        settled.sample(&budget, SessionSeq(sequence), time, time, 512);
        check(&settled);
    }
    assert!(
        settled.samples.len() <= SETTLE_SAMPLES / 2 + 2,
        "{}",
        settled.samples.len()
    );
    let first = settled.samples.front().unwrap().0;
    // A grace a million times as long keeps them and samples a thousand seconds apart.
    let grace = 512_000_000;
    for step in 1..=1_000u64 {
        sequence += 1;
        let time = 2_000 + step * 1_000_000;
        settled.sample(&budget, SessionSeq(sequence), time, time, grace);
        check(&settled);
    }
    assert_eq!(most, SETTLE_SAMPLES);
    // The densest went first: the record they dated is dated by a later sample, never earlier.
    assert!(settled.by(first).unwrap() > 2_000);
    let tight = MemoryBudget::new(15 * SAMPLE, 0).unwrap();
    let mut unfunded = Settled::default();
    unfunded.sample(&tight, SessionSeq(1), 1, 1, 0);
    assert!(unfunded.samples.is_empty());
    assert_eq!(unfunded.by(SessionSeq(1)), None);
}

/// A family is due exactly when its last event is at or before the last record past its grace:
/// `due_through` agrees with the rule [`Settled::by`] dates by, at every record and at every
/// time, grace 0 among them (the newest sample past its grace too).
#[test]
fn due_through_agrees_with_the_first_sample_rule() {
    let budget = budget();
    for grace in [0u64, 1, 5, 1_000] {
        let mut settled = Settled::default();
        assert_eq!(settled.due_through(10_000, grace), None);
        let mut sequence = 0;
        for time in [10u64, 12, 15, 40, 41, 900, 1_500, 4_000] {
            sequence += 3;
            settled.sample(&budget, SessionSeq(sequence), time, time, grace);
            for now in time..time + 2_500 {
                let through = settled.due_through(now, grace);
                for record in 0..=sequence + 1 {
                    let due = settled
                        .by(SessionSeq(record))
                        .is_some_and(|at| now.saturating_sub(at) >= grace);
                    assert_eq!(
                        due,
                        through.is_some_and(|through| SessionSeq(record) <= through),
                        "grace {grace}, now {now}, record {record}: {through:?}",
                    );
                }
            }
        }
    }
}

/// A core that holds no logical time yet (no record has set one) is not sampled: when its records
/// settled is unknown, not the epoch. Sampled as the epoch, it stayed the oldest sample for good
/// and dated every record before it a grace old, so no walk of a session activated before its first
/// record could end early (`due_through` never none). Its first record's time is sampled as ever.
#[test]
fn a_core_with_no_logical_time_is_not_sampled() {
    let budget = budget();
    let mut settled = Settled::default();
    let grace = 86_400_000;
    let now = 1_800_000_000_000;
    settled.sample(&budget, SessionSeq(3), 0, now, grace);
    assert!(settled.samples.is_empty());
    assert_eq!(settled.due_through(now, grace), None);
    settled.sample(&budget, SessionSeq(9), now, now, grace);
    assert_eq!(settled.samples.len(), 1);
    assert_eq!(settled.due_through(now + 1_000, grace), None);
    assert_eq!(settled.due_through(now + grace, grace), Some(SessionSeq(9)));
}
