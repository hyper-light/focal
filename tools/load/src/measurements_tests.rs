use super::*;

fn sample(start: u128, delay: u128, latency: u128, outcome: WriteOutcome) -> WriteSample {
    WriteSample {
        start_ns: start,
        sent_ns: start + delay,
        finished_ns: start + latency,
        worker: 0,
        write: 1,
        attempt: 0,
        epoch: RequestEpoch(1),
        request: start,
        outcome,
    }
}

#[test]
fn fast_refusals_and_warmup_do_not_lower_success_latency() {
    let samples = [
        sample(0, 0, 900, WriteOutcome::Committed),
        sample(100, 30, 100, WriteOutcome::Committed),
        sample(110, 0, 5, WriteOutcome::Refused),
        sample(120, 0, 6, WriteOutcome::Refused),
        sample(130, 2, 7, WriteOutcome::Unknown),
        sample(140, 3, 9, WriteOutcome::Expired),
    ];
    let report = WriteMeasurements::measure(samples.iter(), 100).unwrap();
    assert_eq!(
        (
            report.committed,
            report.refused,
            report.unknown,
            report.expired
        ),
        (1, 2, 1, 1)
    );
    assert_eq!(report.committed_latency_ns.p50, 100);
    assert_eq!(report.refused_latency_ns.max, 6);
    assert_eq!(report.unknown_latency_ns.max, 7);
    assert_eq!(report.expired_latency_ns.max, 9);
    assert_eq!(report.schedule_delay_ns.max, 30);
    assert_eq!(report.client_request_latency_ns.max, 70);
}

#[test]
fn a_full_trace_refuses_another_sample() {
    let mut samples = WriteSamples::reserve(1).unwrap();
    let first = sample(0, 0, 1, WriteOutcome::Committed);
    samples.record(first).unwrap();
    assert!(matches!(
        samples.record(first),
        Err(LoadError::Bound("write measurement samples"))
    ));
    assert_eq!(samples.iter().count(), 1);
}

#[test]
fn csv_preserves_identity_outcome_and_both_time_origins() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("trace.csv");
    let mut later = sample(120, 10, 20, WriteOutcome::Refused);
    later.worker = 2;
    later.write = 3;
    later.attempt = 1;
    later.epoch = RequestEpoch(4);
    later.request = 5;
    let samples = [
        later,
        sample(100, 5, 25, WriteOutcome::Committed),
        sample(0, 0, 900, WriteOutcome::Unknown),
    ];
    write_csv(&path, samples.iter(), 100, 1_000_000).unwrap();
    let output = std::fs::read_to_string(path).unwrap();
    let lines: Vec<_> = output.lines().collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(
        lines[1],
        "0,25,5,25,0,1,0,1,00000000000000000000000000000064,committed,100,1000000"
    );
    assert_eq!(
        lines[2],
        "20,20,30,40,2,3,1,4,00000000000000000000000000000005,refused,120,1000000"
    );
}
