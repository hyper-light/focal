use super::*;

fn read(n: u8) -> ReadCorrelation {
    ReadCorrelation([n; 16])
}

/// The barriers asked, by their first read's byte.
#[derive(Default)]
struct Asked(Vec<u8>);
impl Asked {
    fn ask(&mut self) -> impl FnMut(ReadCorrelation) -> Result<(), ()> + '_ {
        |barrier| {
            self.0.push(barrier.0[0]);
            Ok(())
        }
    }
    fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0)
    }
}

fn refused(_: ReadCorrelation) -> Result<(), ()> {
    Err(())
}

fn served(rounds: &mut ReadRounds, applied: u64) -> Vec<(u8, u64)> {
    let mut out = Vec::new();
    rounds.take_served(applied, usize::MAX, |read, index| {
        out.push((read.0[0], index))
    });
    out
}

/// Short of its window a copy asks a barrier for each read as it is asked; at it, the reads
/// asked meanwhile wait and are asked for by one barrier once one comes back, which confirms them
/// all; none is served by a barrier asked before it was held.
#[test]
fn reads_past_the_window_wait_and_share_the_next_barrier() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    rounds.hold(read(1), 2, asked.ask()).unwrap();
    rounds.hold(read(2), 2, asked.ask()).unwrap();
    assert_eq!(
        asked.take(),
        vec![1, 2],
        "a barrier each, short of the window"
    );
    rounds.hold(read(3), 2, asked.ask()).unwrap();
    rounds.hold(read(4), 2, asked.ask()).unwrap();
    assert_eq!(asked.take(), Vec::<u8>::new(), "at the window they wait");
    assert_eq!(rounds.held(), 4);
    // The first answer serves its read and asks one barrier for the two that waited.
    assert_eq!(rounds.answered(read(1), 10, 2, asked.ask()), 0);
    assert_eq!(asked.take(), vec![3]);
    assert_eq!(served(&mut rounds, 10), vec![(1, 10)]);
    // Answered out of order, each round is served once applied.
    assert_eq!(rounds.answered(read(3), 12, 2, asked.ask()), 0);
    assert_eq!(served(&mut rounds, 11), Vec::new(), "not yet applied");
    assert_eq!(served(&mut rounds, 12), vec![(3, 12), (4, 12)]);
    assert_eq!(rounds.answered(read(2), 11, 2, asked.ask()), 0);
    assert_eq!(served(&mut rounds, 12), vec![(2, 11)]);
    assert_eq!(asked.take(), Vec::<u8>::new());
    assert_eq!(rounds.held(), 0);
}

/// An answer to a barrier not out serves nothing: one asked again and answered twice, or one
/// never asked.
#[test]
fn an_answer_to_a_barrier_not_out_serves_nothing() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    rounds.hold(read(1), 1, asked.ask()).unwrap();
    assert_eq!(rounds.answered(read(9), 5, 1, asked.ask()), 0);
    assert_eq!(served(&mut rounds, 9), Vec::new());
    assert_eq!(rounds.answered(read(1), 5, 1, asked.ask()), 0);
    assert_eq!(
        rounds.answered(read(1), 6, 1, asked.ask()),
        0,
        "answered twice"
    );
    assert_eq!(served(&mut rounds, 6), vec![(1, 5)]);
}

/// A barrier refused as a read is held holds nothing; refused for the reads that waited, it
/// gives them up.
#[test]
fn a_barrier_refused_holds_nothing_and_gives_up_the_reads_that_wanted_it() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    assert!(rounds.hold(read(1), 1, refused).is_err());
    assert_eq!(rounds.held(), 0);
    rounds.hold(read(1), 1, asked.ask()).unwrap();
    rounds.hold(read(2), 1, asked.ask()).unwrap();
    rounds.hold(read(3), 1, asked.ask()).unwrap();
    assert_eq!(rounds.answered(read(1), 4, 1, refused), 2);
    assert_eq!(served(&mut rounds, 4), vec![(1, 4)]);
    assert_eq!(rounds.held(), 0);
}

/// A barrier out unanswered is asked again after the patience in ticks and given up with its
/// reads after twice it, which frees its place for the reads waiting; a late answer to it serves
/// nothing.
#[test]
fn a_barrier_unanswered_is_asked_again_then_given_up_for_the_reads_waiting() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    rounds.hold(read(1), 1, asked.ask()).unwrap();
    rounds.hold(read(2), 1, asked.ask()).unwrap();
    assert_eq!(asked.take(), vec![1]);
    for _ in 0..2 {
        assert_eq!(rounds.tick(3, 1, asked.ask()), 0);
    }
    assert_eq!(rounds.tick(3, 1, asked.ask()), 0);
    assert_eq!(asked.take(), vec![1], "asked again at the patience");
    for _ in 0..2 {
        assert_eq!(rounds.tick(3, 1, asked.ask()), 0);
    }
    assert_eq!(rounds.tick(3, 1, asked.ask()), 1, "given up at twice it");
    assert_eq!(asked.take(), vec![2], "the read waiting is asked for");
    assert_eq!(rounds.held(), 1);
    assert_eq!(rounds.answered(read(1), 7, 1, asked.ask()), 0);
    assert_eq!(rounds.answered(read(2), 8, 1, asked.ask()), 0);
    assert_eq!(served(&mut rounds, 8), vec![(2, 8)]);
    assert_eq!(rounds.tick(3, 1, asked.ask()), 0, "nothing out");
    assert_eq!(asked.take(), Vec::<u8>::new());
}

/// A barrier given up takes only its own reads, around an older one answered and waiting to be
/// applied and the reads waiting behind it.
#[test]
fn a_barrier_given_up_takes_only_its_own_reads() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    rounds.hold(read(1), 2, asked.ask()).unwrap();
    rounds.hold(read(2), 2, asked.ask()).unwrap();
    rounds.hold(read(3), 2, asked.ask()).unwrap();
    assert_eq!(rounds.answered(read(1), 30, 2, asked.ask()), 0);
    assert_eq!(asked.take(), vec![1, 2, 3]);
    rounds.hold(read(4), 2, asked.ask()).unwrap();
    assert_eq!(asked.take(), Vec::<u8>::new(), "two out");
    assert_eq!(rounds.tick(1, 2, asked.ask()), 0);
    assert_eq!(asked.take(), vec![2, 3], "both asked again");
    assert_eq!(rounds.answered(read(3), 31, 2, asked.ask()), 0);
    assert_eq!(asked.take(), vec![4]);
    assert_eq!(
        rounds.tick(1, 2, asked.ask()),
        1,
        "the second, unanswered, given up"
    );
    assert_eq!(rounds.answered(read(4), 32, 2, asked.ask()), 0);
    assert_eq!(served(&mut rounds, 32), vec![(1, 30), (3, 31), (4, 32)]);
    assert_eq!(rounds.held(), 0);
}

/// Served into less room than a round holds, the round keeps the rest for the next call; a new
/// term gives up every read.
#[test]
fn a_round_served_in_part_keeps_the_rest_and_a_new_term_ends_every_read() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(8));
    rounds.hold(read(1), 1, asked.ask()).unwrap();
    rounds.hold(read(2), 1, asked.ask()).unwrap();
    rounds.hold(read(3), 1, asked.ask()).unwrap();
    assert_eq!(rounds.answered(read(1), 3, 1, asked.ask()), 0);
    assert_eq!(rounds.answered(read(2), 3, 1, asked.ask()), 0);
    let mut out = Vec::new();
    rounds.take_served(3, 2, |read, index| out.push((read.0[0], index)));
    assert_eq!(out, vec![(1, 3), (2, 3)]);
    assert_eq!(rounds.held(), 1);
    assert_eq!(served(&mut rounds, 3), vec![(3, 3)]);
    rounds.hold(read(5), 1, asked.ask()).unwrap();
    rounds.hold(read(6), 1, asked.ask()).unwrap();
    assert_eq!(rounds.begin_term(0), 0, "the same term keeps them");
    assert_eq!(rounds.begin_term(2), 2);
    assert_eq!(rounds.held(), 0);
}

/// Room for the reads a copy may hold is taken once, and holding up to it allocates no more.
#[test]
fn holding_up_to_the_room_reserved_allocates_nothing_more() {
    let mut rounds = ReadRounds::default();
    let mut asked = Asked::default();
    assert!(rounds.reserve(4));
    let (reads, round_slots) = (rounds.reads.capacity(), rounds.rounds.capacity());
    for n in 1..=4 {
        rounds.hold(read(n), 2, asked.ask()).unwrap();
    }
    assert_eq!(rounds.answered(read(1), 1, 2, asked.ask()), 0);
    assert_eq!(rounds.reads.capacity(), reads);
    assert_eq!(rounds.rounds.capacity(), round_slots);
    assert!(ReadRounds::bytes_for(4) >= 4 * std::mem::size_of::<ReadCorrelation>());
}
