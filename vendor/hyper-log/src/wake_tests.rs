//! The owner's wake under the interleavings preempted threads make (`wake.rs`), each schedule
//! fixed by pausing the threads that do the jobs where the OS may stop them (`device::pause`).

use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{Scope, ScopedJoinHandle};

use hyper_block::buf::Alignment;
use hyper_block::sim::SimFile;

use crate::device::pause::{self, At};
use crate::{Config, HardState, Log, LogError, Update, Waits};

const ID: u128 = 0x7761_6b65;
const BLOCK: usize = 4096;

fn config() -> Config {
    Config {
        segment_bytes: 16 * BLOCK as u64,
        max_segments: 8,
        max_groups: 64,
        group_entries: 1 << 16,
        group_bytes: 1 << 24,
        group_cache: 1 << 10,
        queue_submissions: 256,
        // Nothing written ahead while the log idles: the jobs are the test's.
        waits: Waits::Never,
    }
}

fn sim(seed: u64) -> SimFile {
    SimFile::new(
        Alignment::new(BLOCK).unwrap(),
        Alignment::new(512).unwrap(),
        seed,
    )
    .unwrap()
}

fn hard(term: u64) -> Update {
    Update {
        hard_state: Some(HardState {
            term,
            vote: 1,
            commit: 0,
            held: None,
        }),
        ..Update::default()
    }
}

/// A thread that writes `term` to `group`, doing its frame's I/O as a blocking writer does, and
/// pauses its job `at`: its handle, once it has paused, and the sender that lets it go on.
fn paused<'s>(
    scope: &'s Scope<'s, '_>,
    log: &'s Log<SimFile>,
    at: At,
    group: u128,
    term: u64,
) -> (ScopedJoinHandle<'s, Result<(), LogError>>, SyncSender<()>) {
    let (paused, heard) = sync_channel(1);
    let (go, gone) = sync_channel(1);
    let writer = scope.spawn(move || {
        pause::next(at, paused, gone);
        log.write(group, hard(term))
    });
    heard.recv().unwrap();
    (writer, go)
}

/// Do: two writers each do their frame's I/O and stop between giving the job back and waking the
/// owner, as threads the OS preempts there do; a look then holds the I/O thread while a
/// submission waits on the device; the writers go on, then the look. Expect: the submission is
/// written and answered. The token channel the wake replaced hung here for good, as on
/// linux/arm64 CI (`wake.rs`): the writers' tokens filled it after the owner had last emptied it,
/// and the I/O thread, done with the look, blocked on its own token with the owner asleep and
/// the channel's receiver waiting in the I/O thread's own queue.
#[test]
fn late_threads_and_a_look_on_the_io_thread_leave_the_owner_its_wake() {
    let log = Log::create(sim(60), config(), ID).unwrap();
    std::thread::scope(|s| {
        let (first, go_first) = paused(s, &log, At::Back, 1, 1);
        let (second, go_second) = paused(s, &log, At::Back, 1, 2);
        let (looking, look_runs) = sync_channel(1);
        let (let_go, held) = sync_channel::<()>(1);
        let look = s.spawn(|| {
            log.with_file(move |_| {
                let _ = looking.send(());
                let _ = held.recv();
            })
        });
        look_runs.recv().unwrap();
        let waiting = log.submit(2, hard(1)).unwrap();
        go_first.send(()).unwrap();
        first.join().unwrap().unwrap();
        go_second.send(()).unwrap();
        second.join().unwrap().unwrap();
        let_go.send(()).unwrap();
        look.join().unwrap().unwrap();
        waiting.wait().unwrap();
    });
    let view = log.view(2).unwrap().unwrap();
    assert_eq!(view.hard_state.map(|h| h.term), Some(1));
}

/// Do: two writers stop between giving their jobs back and waking the owner; a third does its
/// frame and stops before giving it back, while a submission waits on it; the first two go on,
/// each claiming the wake the owner opened for the third, then the third, whose confirmation held,
/// so that it wakes the owner only through the wake. Expect: the submission is written and
/// answered, the owner opening its wake again after each claim it did not need.
#[test]
fn late_claims_never_take_the_wake_of_the_job_out() {
    let log = Log::create(sim(61), config(), ID).unwrap();
    std::thread::scope(|s| {
        let (first, go_first) = paused(s, &log, At::Back, 1, 1);
        let (second, go_second) = paused(s, &log, At::Back, 1, 2);
        let (third, go_third) = paused(s, &log, At::Done, 1, 3);
        let waiting = log.submit(2, hard(1)).unwrap();
        go_first.send(()).unwrap();
        first.join().unwrap().unwrap();
        go_second.send(()).unwrap();
        second.join().unwrap().unwrap();
        go_third.send(()).unwrap();
        third.join().unwrap().unwrap();
        waiting.wait().unwrap();
    });
    assert_eq!(
        log.view(1).unwrap().unwrap().hard_state.map(|h| h.term),
        Some(3)
    );
    assert_eq!(
        log.view(2).unwrap().unwrap().hard_state.map(|h| h.term),
        Some(1)
    );
}
