use super::*;
use std::collections::VecDeque;

/// A pipeline of `capacity` writes a second, one at a time, each answered no
/// sooner than `base_ns` after it was admitted: a write admitted at `t` is
/// answered at `max(t + base, the last answer + 1/capacity)`. Its
/// bandwidth-delay product is `capacity × base`.
struct Pipeline {
    service_ns: u64,
    base_ns: u64,
    last_answer: u64,
    out: VecDeque<(u64, Admitted)>,
}
impl Pipeline {
    fn new(capacity: u64, base_ns: u64) -> Self {
        Self {
            service_ns: 1_000_000_000 / capacity,
            base_ns,
            last_answer: 0,
            out: VecDeque::new(),
        }
    }
    fn admit(&mut self, window: &WriteWindow, now: u64) {
        let answer = (now + self.base_ns).max(self.last_answer + self.service_ns);
        self.last_answer = answer;
        self.out.push_back((
            answer,
            Admitted {
                at_ns: now,
                delivered: window.delivered(),
            },
        ));
    }
    fn inflight(&self) -> u64 {
        self.out.len() as u64
    }
}

/// What a run saw: each answered write's latency and the window after it.
#[derive(Default)]
struct Run {
    latencies: Vec<u64>,
    windows: Vec<u64>,
    refused: u64,
    admitted: u64,
    end_ns: u64,
}

/// Drives `pipeline` until `until_ns`: writes offered every `every_ns`
/// (zero: as fast as the window takes them), each refused when the window is
/// full, as the host's ingress refuses it.
fn drive(
    window: &mut WriteWindow,
    pipeline: &mut Pipeline,
    start: u64,
    until_ns: u64,
    every_ns: u64,
) -> Run {
    let mut run = Run::default();
    let mut now = start;
    let mut next_offer = start;
    loop {
        // Offer what is due now.
        if every_ns == 0 {
            while pipeline.inflight() < window.window() {
                pipeline.admit(window, now);
                run.admitted += 1;
            }
            window.filled(pipeline.inflight());
        } else {
            while next_offer <= now {
                if pipeline.inflight() < window.window() {
                    pipeline.admit(window, next_offer.max(now));
                    run.admitted += 1;
                } else {
                    run.refused += 1;
                    window.filled(pipeline.inflight());
                }
                next_offer += every_ns;
            }
        }
        // Advance to the next answer or the next offer.
        let answer = pipeline.out.front().map(|(at, _)| *at);
        let next = match (answer, every_ns) {
            (Some(at), 0) => at,
            (Some(at), _) => at.min(next_offer),
            (None, 0) => break,
            (None, _) => next_offer,
        };
        if next > until_ns {
            break;
        }
        now = next;
        while let Some(&(at, admitted)) = pipeline.out.front() {
            if at > now {
                break;
            }
            pipeline.out.pop_front();
            window.answered(admitted, now, pipeline.inflight());
            run.latencies.push(now - admitted.at_ns);
            run.windows.push(window.window());
        }
    }
    run.end_ns = now;
    run
}

fn median(values: &[u64]) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

const MS: u64 = 1_000_000;

/// Under a load that always fills it, the window settles about twice the
/// pipeline's bandwidth-delay product (BBR's window gain), the probe cycle
/// swinging it by a quarter, and a write waits about twice the base latency:
/// the pipeline's throughput, without the queue a fixed bound of hundreds
/// would hold.
#[test]
fn a_full_pipeline_settles_the_window_near_twice_its_bandwidth_delay_product() {
    // 10,000 writes a second over 2 ms: a product of 20 writes.
    let mut window = WriteWindow::new(100_000, 0);
    let mut pipeline = Pipeline::new(10_000, 2 * MS);
    let run = drive(&mut window, &mut pipeline, 0, 2_000 * MS, 0);
    let settled = &run.windows[run.windows.len() / 2..];
    let typical = median(settled);
    assert!(
        (30..=50).contains(&typical),
        "settled at {typical}, product 20"
    );
    assert!(
        settled.iter().all(|w| *w <= 60),
        "probes never past 5/4 of 2 × 20 and a margin"
    );
    let waited = median(&run.latencies[run.latencies.len() / 2..]);
    assert!(
        waited <= 5 * MS,
        "a write waited {waited} ns against a 2 ms base"
    );
    // And it delivered the pipeline's throughput.
    let rate = run.latencies.len() as u64 * 1_000 / (run.end_ns / MS);
    assert!(rate >= 9_000, "{rate} writes a second of 10,000");
}

/// Starting from RFC 6928's ten, the window grows by the startup gain each
/// round while the rate grows, and leaves its start once three rounds grew it
/// by less than a quarter.
#[test]
fn startup_grows_the_window_until_the_rate_stops_growing_and_then_leaves() {
    let mut window = WriteWindow::new(100_000, 0);
    assert_eq!(window.window(), INITIAL_WINDOW);
    let mut pipeline = Pipeline::new(100_000, 2 * MS);
    // A product of 200 writes: startup must grow past its first window.
    let run = drive(&mut window, &mut pipeline, 0, 40 * MS, 0);
    let most = run.windows.iter().copied().max().unwrap_or(0);
    assert!(most >= 200, "startup reached {most}");
    let run = drive(&mut window, &mut pipeline, run.end_ns, 400 * MS, 0);
    assert!(
        matches!(window.phase, Phase::Probe { .. } | Phase::Latency { .. }),
        "left its start: {:?}",
        window.phase
    );
    let typical = median(&run.windows[run.windows.len() / 2..]);
    assert!(
        (300..=500).contains(&typical),
        "settled at {typical}, product 200"
    );
}

/// When the pipeline's capacity halves, the window follows within the rate
/// filter's span and a probe cycle: the rounds of the old rate age out.
#[test]
fn a_pipeline_that_slows_shrinks_the_window_within_the_rate_span() {
    let mut window = WriteWindow::new(100_000, 0);
    let mut pipeline = Pipeline::new(10_000, 2 * MS);
    let run = drive(&mut window, &mut pipeline, 0, 1_000 * MS, 0);
    let before = median(&run.windows[run.windows.len() / 2..]);
    pipeline.service_ns *= 2;
    let run = drive(&mut window, &mut pipeline, run.end_ns, 2_000 * MS, 0);
    let after = median(&run.windows[run.windows.len() / 2..]);
    assert!(
        after * 3 <= before * 2,
        "half the capacity, the window {before} → {after}"
    );
    assert!((15..=30).contains(&after), "product 10 now, window {after}");
}

/// A load the pipeline carries with room to spare is never refused: the
/// window holds more than the load keeps admitted, and a round its load
/// limited lowers its rate no further than a full one would.
#[test]
fn a_light_load_is_never_refused() {
    let mut window = WriteWindow::new(100_000, 0);
    let mut pipeline = Pipeline::new(10_000, 2 * MS);
    // 1,000 writes a second: two admitted at a time.
    let run = drive(&mut window, &mut pipeline, 0, 3_000 * MS, MS);
    assert_eq!(run.refused, 0);
    assert!(run.admitted >= 2_900);
    assert!(window.window() >= LEAST_WINDOW);
}

/// A window that stays full hides the base latency behind its own queue, so
/// it holds the least window one round in about fifty and measures it again
/// (BBR's ProbeRTT duty).
#[test]
fn a_full_window_measures_its_base_latency_again_one_round_in_fifty() {
    let mut window = WriteWindow::new(100_000, 0);
    let mut pipeline = Pipeline::new(10_000, 2 * MS);
    let run = drive(&mut window, &mut pipeline, 0, 500 * MS, 0);
    let rounds_before = window.round;
    let mut probes = 0u64;
    let mut was_probing = false;
    let mut end = run.end_ns;
    for _ in 0..40 {
        let run = drive(&mut window, &mut pipeline, end, end + 50 * MS, 0);
        end = run.end_ns;
        let probing =
            matches!(window.phase, Phase::Latency { .. }) || run.windows.contains(&LEAST_WINDOW);
        if probing && !was_probing {
            probes += 1;
        }
        was_probing = probing;
    }
    let rounds = window.round - rounds_before;
    assert!(probes >= 1, "no probe in {rounds} rounds");
    assert!(
        probes <= rounds / 40 + 1,
        "{probes} probes in {rounds} rounds"
    );
    // The base latency it holds is the pipeline's, not its queue's.
    assert!(window.latency_ns <= 3 * MS, "{} ns", window.latency_ns);
}

/// The window holds no more than its ceiling, and no fewer than the least
/// window unless the ceiling is under it.
#[test]
fn the_ceiling_and_the_least_window_bound_the_window() {
    let mut window = WriteWindow::new(8, 0);
    let mut pipeline = Pipeline::new(100_000, 2 * MS);
    let run = drive(&mut window, &mut pipeline, 0, 200 * MS, 0);
    assert!(run.windows.iter().all(|w| *w <= 8));
    assert_eq!(WriteWindow::new(2, 0).window(), 2);
    assert_eq!(WriteWindow::new(0, 0).window(), 1, "a window never closes");
    let mut window = WriteWindow::new(1_000, 0);
    let mut pipeline = Pipeline::new(100, 2 * MS);
    drive(&mut window, &mut pipeline, 0, 2_000 * MS, 0);
    assert!(window.window() >= LEAST_WINDOW, "{}", window.window());
}

/// Answers out of the clock's order, at the clock's ends and in numbers past
/// any run are taken without a panic and leave a window within its bounds.
#[test]
fn extreme_answers_leave_the_window_within_its_bounds() {
    let mut window = WriteWindow::new(64, u64::MAX - 10);
    for at in [0, u64::MAX, 5, u64::MAX / 2] {
        window.answered(
            Admitted {
                at_ns: at,
                delivered: u64::MAX,
            },
            at.wrapping_sub(7),
            u64::MAX,
        );
        assert!((1..=64).contains(&window.window()));
    }
    window.filled(u64::MAX);
    window.answered(
        Admitted {
            at_ns: 0,
            delivered: 0,
        },
        0,
        0,
    );
    assert!((1..=64).contains(&window.window()));
}

/// The windowed filter keeps the most of the last ten rounds: a larger value
/// replaces it at once, and one that aged out gives way to the best since.
#[test]
fn the_rate_filter_keeps_the_most_of_the_last_ten_rounds() {
    let mut filter = MaxFilter::default();
    filter.update(1, 100);
    filter.update(2, 50);
    assert_eq!(filter.get(), 100);
    filter.update(3, 150);
    assert_eq!(filter.get(), 150);
    for round in 4..=13 {
        filter.update(round, 80);
    }
    assert_eq!(filter.get(), 150, "round 3 is ten rounds old at 13");
    filter.update(14, 70);
    filter.update(15, 70);
    assert!(filter.get() <= 80, "round 3 aged out: {}", filter.get());
}

/// The window states the length of its last round, the interval a full window
/// holds a burst within at the ingress; none before a round has ended.
#[test]
fn the_window_states_the_length_of_its_last_round() {
    let mut window = WriteWindow::new(128, 0);
    assert_eq!(window.round_ns(), 0);
    // The first write, admitted with nothing delivered and answered 5 ms
    // later, ends the first round
    window.answered(
        Admitted {
            at_ns: 0,
            delivered: 0,
        },
        5_000_000,
        0,
    );
    assert_eq!(window.round_ns(), 5_000_000);
    // A write admitted before the round's start ends no round
    window.answered(
        Admitted {
            at_ns: 1_000_000,
            delivered: 0,
        },
        9_000_000,
        0,
    );
    assert_eq!(window.round_ns(), 5_000_000);
}
