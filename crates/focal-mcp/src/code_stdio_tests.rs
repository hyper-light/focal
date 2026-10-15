//! Code mode through the real stdio server (19 §Code mode): what a program
//! may see and call, its determinism, every bound as a typed end, and a
//! retried run's mutations exactly once.
use super::*;
use focal_client::operations::CodeOutcome;

fn code(runner: &mut Running, id: u64, arguments: Value) -> ApplicationResult {
    let response = runner.tool_response(id, "code.run", arguments);
    application(&response)
}
fn run(runner: &mut Running, id: u64, run: &str, program: &str) -> ApplicationResult {
    code(runner, id, json!({"run": run, "program": program}))
}
fn returned(result: &ApplicationResult) -> &Value {
    match &result.result {
        OperationOutput::Code {
            outcome: CodeOutcome::Returned { value },
            ..
        } => value,
        other => panic!("the program did not return: {other:?}"),
    }
}
fn ended(result: &ApplicationResult) -> String {
    match &result.result {
        OperationOutput::Code {
            outcome: CodeOutcome::Failed { code, .. },
            ..
        } => code.clone(),
        other => panic!("the program did not fail: {other:?}"),
    }
}

#[test]
fn code_mode_is_listed_and_a_search_returns_only_what_its_program_selects() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    runner.rpc(1, "tools/list", json!({}));
    let listed = runner.response(1);
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"code.run") && names.contains(&"code.search"));
    let search = |runner: &mut Running, id: u64, program: &str| {
        application(&runner.tool_response(id, "code.search", json!({"program": program})))
    };
    let found = search(
        &mut runner,
        2,
        r#"return registry.filter(t => t.name === "claim.submit").map(t => [t.name, t.read_only, typeof t.input]);"#,
    );
    assert_eq!(
        returned(&found),
        &json!([["claim.submit", false, "object"]])
    );
    // The registry is what the caller may call: code mode's own tools are not in it.
    let own = search(
        &mut runner,
        3,
        r#"return registry.filter(t => t.name.startsWith("code.")).length;"#,
    );
    assert_eq!(returned(&own), &json!(0));
    // A search makes no calls.
    let calling = search(&mut runner, 4, r#"return await focal.ledger.summary({});"#);
    assert_eq!(ended(&calling), "argument");
    runner.stop();
}

#[test]
fn a_program_has_one_clock_and_one_seed_so_a_replay_computes_the_same() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    let program = "return [Date.now(), new Date().getTime(), Math.random(), Math.random()];";
    let arguments = |run: &str| json!({"run": run, "program": program, "now_ms": 1_234});
    let first = code(&mut runner, 1, arguments("r1"));
    let again = code(&mut runner, 2, arguments("r1"));
    let other = code(&mut runner, 3, arguments("r2"));
    let [now, date, a, b] = returned(&first).as_array().unwrap().as_slice() else {
        panic!("four values")
    };
    assert_eq!((now, date), (&json!(1_234), &json!(1_234)));
    assert_ne!(a, b, "a seeded generator still moves");
    assert_eq!(returned(&first), returned(&again));
    assert_ne!(
        returned(&first),
        returned(&other),
        "another run, another seed"
    );
    runner.stop();
}

#[test]
fn every_bound_ends_the_program_with_its_name_and_the_server_keeps_serving() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    let cases = [
        ("while (true) {}", "work"),
        (
            "const a = []; for (;;) a.push(String(a.length).repeat(1 << 20));",
            "heap",
        ),
        ("function f() { return f() + 1; } return f();", "stack"),
        ("return \"x\".repeat(3 * 1024 * 1024);", "result"),
        ("return 1n;", "result"),
        ("const o = {}; o.o = o; return o;", "result"),
        ("return await new Promise(() => {});", "unsettled"),
        ("this is not javascript", "program"),
        ("throw new Error(\"mine\");", "exception"),
        // The driver's entry points cannot be replaced by the program.
        (
            "__focal_next = () => \"[0, \\\"ledger.summary\\\", {}]\"; return 1;",
            "exception",
        ),
    ];
    for (id, (program, bound)) in (1..).zip(cases) {
        let result = run(&mut runner, id, "bounds", program);
        assert!(result.is_error(), "{program}");
        assert_eq!(ended(&result), bound, "{program}");
    }
    // Each run's sandbox and its charge are gone: the next one runs whole.
    let after = run(&mut runner, 99, "after", "return 6 * 7;");
    assert_eq!(returned(&after), &json!(42));
    runner.stop();
}

#[test]
fn a_program_cannot_call_a_tool_its_caller_may_not_list_nor_code_mode_itself() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    let program = r#"
        const conditions = [];
        for (const name of ["code.run", "code.search", "no.such.tool"]) {
            try { await focal.call(name, { run: "x", program: "return 1" }); }
            catch (e) { conditions.push(e.condition); }
        }
        try { await focal.no.such.tool({}); } catch (e) { conditions.push(e.condition); }
        return conditions;"#;
    let result = run(&mut runner, 1, "unlisted", program);
    assert_eq!(
        returned(&result),
        &json!(["Unlisted", "Unlisted", "Unlisted", "Unlisted"])
    );
    // Refused calls never reach a backend, so none is listed as made.
    let OperationOutput::Code { calls, .. } = &result.result else {
        panic!("code result")
    };
    assert_eq!(
        calls.len(),
        0,
        "an unlisted call is not dispatched: {calls:?}"
    );
    runner.stop();
}

#[test]
fn a_retried_run_resumes_its_mutation_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    // Every request the backend sends is answered by one core, on its own
    // thread, so the test does not decide how many a retry sends.
    let (_unused, empty) = mpsc::sync_channel(1);
    let requests = std::mem::replace(&mut runner.requests, empty);
    let core = thread::spawn(move || {
        let mut core = Core::new(context().ledger, CoreLimits::default());
        while let Ok(observed) = requests.recv() {
            let reply = apply(&mut core, &observed.request);
            answer(observed, reply);
        }
        core
    });
    let mut claim = claim();
    claim.as_object_mut().unwrap().remove("operation_id");
    let program = "const r = await focal.claim.submit(input.claim); return r.condition;";
    let arguments = json!({"run": "exactly-once", "program": program, "input": {"claim": claim}});
    let first = code(&mut runner, 1, arguments.clone());
    let second = code(&mut runner, 2, arguments);
    assert_eq!(
        returned(&first),
        returned(&second),
        "{first:?} / {second:?}"
    );
    let reference = |result: &ApplicationResult| match &result.result {
        OperationOutput::Code { calls, .. } => calls[0].operation_id.clone(),
        other => panic!("{other:?}"),
    };
    let derived = crate::code::reference("exactly-once", 0, false).unwrap();
    assert_eq!(reference(&first).as_deref(), Some(derived.as_str()));
    assert_eq!(reference(&second), reference(&first));
    // Another program under the same run reaches the same reference with
    // other input: refused, never a second claim.
    let mut other = claim.clone();
    other["description"] = json!("a different occurrence");
    let changed = code(
        &mut runner,
        3,
        json!({"run": "exactly-once", "program": program, "input": {"claim": other}}),
    );
    assert!(changed.is_error(), "{changed:?}");
    runner.stop();
    let core = core.join().unwrap();
    // The epoch and the one claim.
    assert_eq!(core.sequence(), SessionSeq(2));
}

#[test]
fn a_cancelled_program_is_stopped_and_answers_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let mut runner = Running::start(&temp.path().join("operations"), false);
    runner.tool(
        1,
        "code.run",
        json!({"run": "spin", "program": "for (;;) {}"}),
    );
    runner
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}));
    // The cancelled call's answer is suppressed. Its slot frees when the
    // program stops at its next hook call; until then a call is refused for
    // capacity, never answered with the cancelled one's result.
    let deadline = Instant::now() + FROZEN;
    let after = (2..).find_map(|id| {
        assert!(
            Instant::now() < deadline,
            "the cancelled program held its slot"
        );
        let response = runner.tool_response(
            id,
            "code.run",
            json!({"run": "after", "program": "return \"served\";"}),
        );
        response["result"]
            .get("structuredContent")
            .map(|_| application(&response))
    });
    assert_eq!(returned(&after.unwrap()), &json!("served"));
    runner.stop();
}
