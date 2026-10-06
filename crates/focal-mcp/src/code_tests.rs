use super::*;

#[test]
fn a_reference_is_the_runs_and_the_ordinals_and_nothing_else() {
    let first = reference("run-a", 0, true).unwrap();
    assert_eq!(first.len(), 35);
    assert!(first.starts_with("n1:"));
    assert!(
        first[3..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
    // A retry of the same run derives the same reference at the same ordinal.
    assert_eq!(reference("run-a", 0, true).unwrap(), first);
    // Another ordinal, another run, or a run that is a prefix of another all
    // derive another reference (the run's length is part of the input).
    assert_ne!(reference("run-a", 1, true).unwrap(), first);
    assert_ne!(reference("run-b", 0, true).unwrap(), first);
    assert_ne!(reference("run-a\0", 0, true).unwrap(), first);
    // A V1 ledger takes the same digits as its legacy identity.
    assert_eq!(reference("run-a", 0, false).unwrap(), first[3..]);
}

#[test]
fn the_bounds_are_derived_from_the_adapters_and_hold_together() {
    let limits = Limits {
        max_frame_bytes: 278_528,
        max_response_bytes: 16 * 1024 * 1024,
        max_active_calls: 1,
        ..Limits::default()
    };
    let code = CodeLimits::for_adapter(limits, 1024 * 1024).unwrap();
    assert_eq!(code.heap, 32 * 1024 * 1024);
    assert_eq!(code.stack, 512 * 1024);
    assert_eq!(code.calls, limits.max_nodes / 4);
    // The value is carried twice in the response, once escaped (six bytes a
    // byte at most): both together stay within the response bound.
    assert!(code.result * 7 < limits.max_response_bytes);
    assert_eq!(code.program, limits.max_frame_bytes);
    assert!(code.reservation().unwrap() > code.heap);
}

#[test]
#[ignore = "measurement"]
fn measure_interrupt_rate() {
    for program in [
        "for (;;) globalThis.n++;",
        "function f() { globalThis.n++; } for (;;) f();",
        "const a = []; for (;;) { a.sort(); globalThis.n++; }",
    ] {
        let engine = Runtime::new().unwrap();
        let calls = std::rc::Rc::new(std::cell::Cell::new(0u64));
        let seen = calls.clone();
        let limit = 20_000u64;
        engine.set_interrupt_handler(Some(Box::new(move || {
            seen.set(seen.get() + 1);
            seen.get() > limit
        })));
        let context = Context::full(&engine).unwrap();
        let started = std::time::Instant::now();
        let n = context.with(|ctx| {
            let _ = ctx.eval::<(), _>(format!("globalThis.n = 0; {program}"));
            ctx.globals().get::<_, f64>("n").unwrap()
        });
        let elapsed = started.elapsed();
        eprintln!(
            "{program}: {} hook calls, {n} iterations ({:.0}/call), {:?}, {:.0} calls/s",
            calls.get(),
            n / calls.get() as f64,
            elapsed,
            calls.get() as f64 / elapsed.as_secs_f64()
        );
    }
}
