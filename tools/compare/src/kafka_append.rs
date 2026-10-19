//! Kafka's side of `append` at its client's own cost: one thread produces through librdkafka, and
//! librdkafka's polling thread hands each delivery report to it with the time it came, as Kafka's
//! own `producer-perf-test` and librdkafka's `rdkafka_performance` send, a callback a record. The
//! other systems' generator spawns a task and awaits a future a record; at Kafka's ceiling that
//! was the generator's own CPU (81% of its one on CI's runner, 72ddad03), which measured the
//! generator, not the brokers. Records are timed as theirs are: from the intended send on the
//! schedule, or from the send in a closed loop (a rate of zero), to the report.

use crate::timeline::Timeline;
use crate::{CompareError, HIGHEST_NS, Tally, schedule};
use hdrhistogram::Histogram;
use rdkafka::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant};

/// One `append` run's terms against Kafka.
pub struct Run {
    /// The producer's configuration (`target::kafka_config`): the brokers, `acks`, idempotence.
    pub config: ClientConfig,
    pub topic: &'static str,
    /// Records a second on the schedule; zero, a closed loop.
    pub rate: u32,
    pub warmup: Duration,
    pub total: Duration,
    pub payload: usize,
    pub inflight: usize,
}

/// A delivery report as the polling thread hands it over: the record's slot, when the report
/// came, and its error, if any.
type Report = (usize, Instant, Option<String>);

/// The producer's context: each report goes to the generator's thread, at most `inflight` of
/// them waiting (each is a record in flight, which the window bounds).
struct Reports {
    sender: Mutex<SyncSender<Report>>,
}

impl ClientContext for Reports {}

impl ProducerContext for Reports {
    type DeliveryOpaque = usize;

    fn delivery(&self, result: &DeliveryResult<'_>, slot: usize) {
        let came = Instant::now();
        let error = result.as_ref().err().map(|(error, _)| error.to_string());
        if let Ok(sender) = self.sender.lock() {
            // The channel holds a report for every record in flight: it has room for this one
            // unless the generator is gone, whose run is over.
            match sender.try_send((slot, came, error)) {
                Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                Err(TrySendError::Full(report)) => {
                    let _ = sender.send(report);
                }
            }
        }
    }
}

/// A record in flight: when it was intended and whether it is measured.
#[derive(Clone, Copy)]
struct Flying {
    intended: Instant,
    measured: bool,
}

/// Runs the schedule, or the closed loop; each acknowledged record's latency in the histogram.
pub fn run(run: Run) -> Result<(Histogram<u64>, Tally), CompareError> {
    let fail = |what: String| CompareError::Target(format!("kafka: {what}"));
    let (sender, receiver) = sync_channel::<Report>(run.inflight);
    let producer: ThreadedProducer<Reports> = run
        .config
        .create_with_context(Reports {
            sender: Mutex::new(sender),
        })
        .map_err(|error| fail(error.to_string()))?;
    let interval = match run.rate {
        0 => None,
        rate => Some(
            Duration::from_secs(1)
                .checked_div(rate)
                .filter(|interval| !interval.is_zero())
                .ok_or(CompareError::Argument("rate"))?,
        ),
    };
    let payload = vec![0x5a_u8; run.payload];
    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
        .map_err(|error| CompareError::Histogram(error.to_string()))?;
    let mut tally = Tally::default();
    // Each record in flight holds a slot until its report comes: the window, and nothing more.
    let mut slots: Vec<Option<Flying>> = vec![None; run.inflight];
    let mut free: Vec<usize> = (0..run.inflight).rev().collect();
    let began = Instant::now();
    tally.timeline = Timeline::new(run.total)?;
    let mut next_due = Duration::ZERO;
    let mut closed: Option<Instant> = None;
    let mut last_answer = began;
    // The client's own deadline for a report is the histogram's highest value
    // (`message.timeout.ms`), so a run whose last records never answer ends that long after its
    // schedule, its records failed.
    let patience = Duration::from_nanos(HIGHEST_NS);
    loop {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(began);
        let open = match interval {
            Some(_) => next_due < run.total,
            None => elapsed < run.total,
        };
        if !open && closed.is_none() {
            closed = Some(now);
        }
        let flying = run.inflight.saturating_sub(free.len());
        if !open && flying == 0 {
            break;
        }
        if closed.is_some_and(|closed| now.saturating_duration_since(closed) > patience) {
            for flying in slots.iter().flatten() {
                if flying.measured {
                    tally.failed = tally.failed.saturating_add(1);
                }
            }
            tally
                .first_error
                .get_or_insert_with(|| "no report within the client's deadline".to_owned());
            break;
        }
        // What this turn sends: on the schedule, the records due while the window has room, and
        // once it is full every one due counted refused at once; in a closed loop, as many as the
        // window has room for, each sent now. Bounded: each pass takes a slot or ends the turn.
        for _ in 0..=run.inflight {
            if !open || (interval.is_some() && (next_due > elapsed || next_due >= run.total)) {
                break;
            }
            let Some(slot) = free.pop() else {
                if let Some(interval) = interval {
                    let skipped =
                        schedule::overdue(next_due, elapsed, interval, run.warmup, run.total)?;
                    tally.refused = tally.refused.saturating_add(skipped.measured);
                    next_due = skipped.next;
                }
                break;
            };
            // The record's place on the schedule is taken only once librdkafka takes it.
            let (intended, measured, after) = match interval {
                None => (now, elapsed >= run.warmup, next_due),
                Some(interval) => (
                    began
                        .checked_add(next_due)
                        .ok_or(CompareError::Argument("schedule"))?,
                    next_due >= run.warmup,
                    next_due
                        .checked_add(interval)
                        .ok_or(CompareError::Argument("schedule"))?,
                ),
            };
            let key = tally.sent.saturating_add(1).to_be_bytes();
            let record = BaseRecord::with_opaque_to(run.topic, slot)
                .key(&key[..])
                .payload(&payload[..]);
            match producer.send(record) {
                Ok(()) => {
                    tally.sent = tally.sent.saturating_add(1);
                    next_due = after;
                    if let Some(place) = slots.get_mut(slot) {
                        *place = Some(Flying { intended, measured });
                    }
                }
                // librdkafka's own queue is full (`queue.buffering.max.messages`, 100,000 by
                // default, past a window that large): the record waits for its reports, as a
                // window that is full does, and is sent from the same place.
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                    free.push(slot);
                    break;
                }
                Err((error, _)) => return Err(fail(error.to_string())),
            }
        }
        // The reports until the next record is due, or at most a tick while the window is full;
        // in a closed loop with room, none waited for.
        let room = !free.is_empty();
        let wait = match interval {
            Some(_) if open => began
                .checked_add(next_due)
                .map(|due| due.saturating_duration_since(Instant::now()))
                .unwrap_or_default(),
            None if open && room => Duration::ZERO,
            _ => Duration::from_millis(1),
        }
        .min(Duration::from_millis(1));
        let first = match receiver.recv_timeout(wait) {
            Ok(report) => Some(report),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(fail("the producer's polling thread ended".to_owned()));
            }
        };
        // Bounded: the reports waiting, at most one a record in flight.
        for report in first.into_iter().chain(drain(&receiver, run.inflight)) {
            let (slot, came, error) = report;
            let Some(flying) = slots.get_mut(slot).and_then(Option::take) else {
                continue;
            };
            free.push(slot);
            last_answer = came;
            let latency = came.saturating_duration_since(flying.intended);
            let result = match error {
                None => {
                    tally.produced = tally.produced.saturating_add(1);
                    tally
                        .timeline
                        .record(flying.intended.saturating_duration_since(began), latency)?;
                    Ok(())
                }
                Some(error) => Err(error),
            };
            if flying.measured {
                tally.record(&mut histogram, latency, result)?;
            }
        }
    }
    tally.drain = closed.map_or(Duration::ZERO, |closed| {
        last_answer.saturating_duration_since(closed)
    });
    // Whatever librdkafka still holds is given its own deadline to go before the producer ends.
    let _ = producer.flush(Duration::from_secs(1));
    Ok((histogram, tally))
}

/// The reports already waiting, at most `most`.
fn drain(receiver: &Receiver<Report>, most: usize) -> impl Iterator<Item = Report> + '_ {
    std::iter::from_fn(|| receiver.try_recv().ok()).take(most)
}
