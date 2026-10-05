//! Code mode (19 §Code mode, decision F59): an agent's program runs in a
//! bounded QuickJS-NG sandbox, and every call it makes is a `ToolCall`
//! dispatched through the same backend as a direct tool call.
//!
//! The sandbox holds no host function. A program's calls queue inside its own
//! heap (bounded by the heap limit), and the driver takes them one at a time:
//! it executes each through [`Backend::execute`], then settles the call's
//! promise with the result. Every bound is counted, none is timed: the heap
//! limit, the interrupt hook's calls (each one 10,000 of the engine's polls at
//! function calls, loop back-edges and long built-in loops), the stack limit,
//! the number of calls and the returned value's size and tree.
use crate::backend::Backend;
use crate::{Limits, ProtocolError, Tool, ToolCall};
use focal_client::ClientTransport;
use focal_client::operations::{ApplicationResult, CodeCall, CodeOutcome, OperationOutput};
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use rquickjs::{Context, Function, Runtime};
use serde_json::{Map, Value};
use std::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) const RUN: &str = "code.run";
pub(crate) const SEARCH: &str = "code.search";
/// The longest `run` identity a caller may give.
const MAX_RUN_BYTES: usize = 64;
/// The derivation context of a program's mutation references.
const REFERENCE_CONTEXT: &str = "focal 2026-10-04 code.run mutation reference v1";

/// One run's bounds, every one derived from the adapter's own (`for_adapter`).
#[derive(Clone, Copy, Debug)]
pub(crate) struct CodeLimits {
    /// Bytes the engine may allocate.
    pub heap: usize,
    /// Bytes of native stack the engine may use.
    pub stack: usize,
    /// Interrupt-hook calls before the program is stopped.
    pub work: u64,
    /// Calls one program may make.
    pub calls: usize,
    /// UTF-16 units of the returned value's JSON text.
    pub result: usize,
    /// Bytes of program text.
    pub program: usize,
    /// The tree bound a call's input and the returned value are held to.
    pub tree: Limits,
}
impl CodeLimits {
    /// - heap: one call's largest result, as the text handed in and as the
    ///   value it parses to, at once: twice the response bound.
    /// - stack: half the owner thread's stack, the rest for the frames under
    ///   the engine's entry.
    /// - result: the response carries the value twice, as structured content
    ///   and as escaped text (at most six bytes a byte, `\u00XX`), with room
    ///   for the envelope: an eighth of the response bound.
    /// - calls: the listing of calls (four JSON values each) stays within the
    ///   protocol's tree bound.
    /// - work: measured, see `WORK`.
    pub(crate) fn for_adapter(limits: Limits, thread_stack: usize) -> Result<Self, ProtocolError> {
        Ok(Self {
            heap: limits
                .max_response_bytes
                .checked_mul(2)
                .ok_or(ProtocolError::Limits)?,
            stack: thread_stack.checked_div(2).ok_or(ProtocolError::Limits)?,
            work: WORK,
            calls: limits
                .max_nodes
                .checked_div(4)
                .ok_or(ProtocolError::Limits)?,
            result: limits
                .max_response_bytes
                .checked_div(8)
                .ok_or(ProtocolError::Limits)?,
            program: limits.max_frame_bytes,
            tree: limits,
        })
    }
    /// The memory a run holds outside the engine's heap at most: the text of
    /// one call's result handed in, and the returned value's text.
    pub(crate) fn reservation(&self) -> Result<usize, ProtocolError> {
        self.heap
            .checked_add(self.tree.max_response_bytes)
            .and_then(|n| n.checked_add(self.result.checked_mul(3)?))
            .ok_or(ProtocolError::Limits)
    }
}
/// Interrupt-hook calls a run may take. A program holds the adapter's one
/// worker while it computes, so it may hold it no longer than the longest
/// wait one call already may (`claim.wait`'s 30 s), at the slowest hook rate
/// measured (`measure_interrupt_rate`, release, Apple M-series, 2026-10-04):
/// 7,147 calls a second for a loop of built-in sorts, against 13,723 for a
/// loop of property writes. Faster hosts end sooner; the count, not a clock,
/// is the bound.
const WORK: u64 = 30 * 7_147;

/// What `code.search` searches and `code.run` may call: the served tools
/// other than code mode's own, as JSON text the sandbox parses.
pub(crate) struct Registry {
    listing: String,
    /// The served skills as the search sees them (`skills`).
    skills: String,
    names: Vec<String>,
    /// The mutations that take an `operation_id`, which a program's call
    /// gets derived when it names none.
    referenced: Vec<String>,
}
impl Registry {
    pub(crate) fn new(
        tools: &[Tool],
        skills: &crate::skills::Skills,
    ) -> Result<Self, ProtocolError> {
        let mut names = Vec::new();
        names
            .try_reserve_exact(tools.len())
            .map_err(|_| ProtocolError::Capacity)?;
        let mut referenced = Vec::new();
        referenced
            .try_reserve_exact(tools.len())
            .map_err(|_| ProtocolError::Capacity)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(tools.len())
            .map_err(|_| ProtocolError::Capacity)?;
        for tool in tools.iter().filter(|tool| !is_code(&tool.name)) {
            names.push(tool.name.clone());
            if !tool.read_only
                && tool
                    .input_schema
                    .get("properties")
                    .and_then(|p| p.get("operation_id"))
                    .is_some()
            {
                referenced.push(tool.name.clone());
            }
            entries.push(serde_json::json!({
                "name": tool.name,
                "description": tool.description,
                "input": tool.input_schema,
                "output": tool.output_schema,
                "read_only": tool.read_only,
                "destructive": tool.destructive,
            }));
        }
        let listing = serde_json::to_string(&entries).map_err(|_| ProtocolError::Encode)?;
        Ok(Self {
            listing,
            skills: skills.search_listing()?,
            names,
            referenced,
        })
    }
    /// The bytes the registry holds.
    pub(crate) fn bytes(&self) -> usize {
        self.names
            .iter()
            .chain(self.referenced.iter())
            .map(String::capacity)
            .fold(
                self.listing
                    .capacity()
                    .saturating_add(self.skills.capacity()),
                usize::saturating_add,
            )
    }
    fn listed(&self, name: &str) -> bool {
        self.names.iter().any(|listed| listed == name)
    }
    fn takes_reference(&self, name: &str) -> bool {
        self.referenced.iter().any(|listed| listed == name)
    }
}

/// The two code-mode tools, appended to the catalogue the adapter serves.
pub(crate) fn tools(limits: CodeLimits) -> Result<[Tool; 2], ProtocolError> {
    let program = serde_json::json!({"type":"string","minLength":1,"maxLength":limits.program,
        "description":"JavaScript (ES2023), the body of an async function: use await and return a JSON value."});
    Ok([
        Tool {
            name: SEARCH.into(),
            description: "Search focal's tools and skills with code. The program sees `registry`, an array of {name, description, input, output, read_only, destructive} for every tool you may call, and `skills`, an array of {uri, name, description, files: [{uri, text}]} for every skill served, and returns only what you need, e.g. `return registry.filter(t => t.name.startsWith(\"claim.\")).map(t => ({name: t.name, input: t.input}))`. Makes no calls.".into(),
            input_schema: serde_json::json!({"type":"object","additionalProperties":false,"required":["program"],
                "properties":{"program":program}}),
            output_schema: crate::catalog::output_schema(SEARCH)?,
            read_only: true,
            destructive: false,
            idempotent: true,
        },
        Tool {
            name: RUN.into(),
            description: "Run a program that calls focal's tools and returns only its result. `await focal.claim.submit({...})` (or `focal.call(\"claim.submit\", {...})`) calls a tool and resolves to its structured result; a refused or failed call throws an Error with `condition` and `result`. Mutations get an operation_id derived from `run` and their order, so sending the same run and program again after a lost reply resumes each call exactly once. No clock (Date.now() is now_ms), no randomness but a seeded Math.random, no I/O but focal; bounded heap, work, stack and calls.".into(),
            input_schema: serde_json::json!({"type":"object","additionalProperties":false,"required":["run","program"],
                "properties":{
                    "run":{"type":"string","minLength":1,"maxLength":MAX_RUN_BYTES,"description":"Your identity for this program; reuse it only to retry the same program."},
                    "program":program,
                    "input":{"type":"object","description":"Data the program reads as `input`."},
                    "now_ms":{"type":"integer","minimum":0,"description":"What Date.now() returns; part of the run, so give the same value on a retry."}}}),
            output_schema: crate::catalog::output_schema(RUN)?,
            read_only: false,
            destructive: false,
            idempotent: true,
        },
    ])
}

pub(crate) fn is_code(name: &str) -> bool {
    name == RUN || name == SEARCH
}

/// What a run executes against: the adapter's backend and runtime, the
/// budget its sandbox and calls are charged to, and what it may call.
pub(crate) struct Host<'a, T: ClientTransport> {
    pub backend: &'a mut Backend<T>,
    pub runtime: &'a tokio::runtime::Runtime,
    pub budget: &'a MemoryBudget,
    pub registry: &'a Registry,
    pub limits: CodeLimits,
}

/// Run `code.run` or `code.search`. `cancel` is the call's own signal: it is
/// moved into the interrupt hook, and the calls the program makes run with a
/// signal of their own that never fires (cancellation stops the program at
/// its next hook call, after the call in flight ends at its own bound).
pub(crate) fn execute<T: ClientTransport>(
    host: Host<'_, T>,
    call: &mut ToolCall,
    cancel: &mut oneshot::Receiver<()>,
) -> ApplicationResult {
    let limits = host.limits;
    let (_quiet, quiet) = oneshot::channel();
    let signal = std::mem::replace(cancel, quiet);
    let mut calls = Vec::new();
    let outcome = match Request::parse(&call.tool, &mut call.arguments, limits) {
        Err(detail) => failed("argument", detail),
        Ok(request) => match host.budget.reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            match limits.reservation() {
                Ok(bytes) => bytes,
                Err(_) => return result(failed("limits", "bounds overflow"), calls),
            },
        ) {
            Err(_) => failed("capacity", "no memory for a sandbox"),
            Ok(reservation) => {
                let _reservation = reservation.commit();
                let driven = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut driver = Driver {
                        host,
                        request: &request,
                        calls: &mut calls,
                    };
                    driver.run(signal)
                }));
                driven.unwrap_or_else(|_| failed("dependency", "the engine unwound"))
            }
        },
    };
    result(outcome, calls)
}

fn result(outcome: CodeOutcome, calls: Vec<CodeCall>) -> ApplicationResult {
    let condition = match &outcome {
        CodeOutcome::Returned { .. } => "Returned",
        CodeOutcome::Failed { .. } => "Failed",
    };
    ApplicationResult {
        schema_version: 2,
        operation_id: None,
        condition: condition.into(),
        result: OperationOutput::Code { outcome, calls },
    }
}

fn failed(code: &str, detail: impl Into<String>) -> CodeOutcome {
    CodeOutcome::Failed {
        code: code.into(),
        detail: detail.into(),
    }
}

enum Mode {
    Run {
        run: String,
        input: Value,
        now_ms: u64,
    },
    Search,
}
struct Request {
    mode: Mode,
    program: String,
}
impl Request {
    fn parse(
        tool: &str,
        arguments: &mut Map<String, Value>,
        limits: CodeLimits,
    ) -> Result<Self, &'static str> {
        let Some(Value::String(program)) = arguments.remove("program") else {
            return Err("program must be a string");
        };
        if program.len() > limits.program {
            return Err("program is longer than the frame bound");
        }
        let mode = if tool == SEARCH {
            Mode::Search
        } else {
            let Some(Value::String(run)) = arguments.remove("run") else {
                return Err("run must be a string");
            };
            if run.is_empty() || run.len() > MAX_RUN_BYTES {
                return Err("run must be 1 to 64 bytes");
            }
            let input = arguments
                .remove("input")
                .unwrap_or(Value::Object(Map::new()));
            if !input.is_object() {
                return Err("input must be an object");
            }
            let now_ms = match arguments.remove("now_ms") {
                None => 0,
                Some(value) => value.as_u64().ok_or("now_ms must be a whole number")?,
            };
            Mode::Run { run, input, now_ms }
        };
        if !arguments.is_empty() {
            return Err("unknown argument");
        }
        Ok(Self { mode, program })
    }
}

/// Why the interrupt hook stopped a program.
#[derive(Clone, Copy, Debug)]
enum Stop {
    Work,
    Cancelled,
}

struct Driver<'a, T: ClientTransport> {
    host: Host<'a, T>,
    request: &'a Request,
    calls: &'a mut Vec<CodeCall>,
}
impl<T: ClientTransport> Driver<'_, T> {
    fn run(&mut self, mut signal: oneshot::Receiver<()>) -> CodeOutcome {
        let Ok(engine) = Runtime::new() else {
            return failed("dependency", "no engine");
        };
        engine.set_memory_limit(self.host.limits.heap);
        engine.set_max_stack_size(self.host.limits.stack);
        let (stops, stopped) = mpsc::sync_channel::<Stop>(1);
        let mut left = self.host.limits.work;
        engine.set_interrupt_handler(Some(Box::new(move || {
            if !matches!(signal.try_recv(), Err(oneshot::error::TryRecvError::Empty)) {
                let _ = stops.try_send(Stop::Cancelled);
                return true;
            }
            match left.checked_sub(1) {
                Some(next) => {
                    left = next;
                    false
                }
                None => {
                    let _ = stops.try_send(Stop::Work);
                    true
                }
            }
        })));
        let Ok(context) = Context::full(&engine) else {
            return failed("heap", "no memory for a context");
        };
        let stop = |stopped: &mpsc::Receiver<Stop>, otherwise: CodeOutcome| match stopped.try_recv()
        {
            Ok(Stop::Work) => failed("work", "the program's work bound was spent"),
            Ok(Stop::Cancelled) => failed("cancelled", "the call was cancelled"),
            Err(_) => otherwise,
        };
        if let Err(detail) = self.start(&context) {
            return stop(&stopped, failed("program", detail));
        }
        loop {
            loop {
                match engine.execute_pending_job() {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(_) => return stop(&stopped, failed("program", "a job threw")),
                }
            }
            let next = context.with(|ctx| ctx.eval::<Option<String>, _>("__focal_next()"));
            let next = match next {
                Ok(next) => next,
                Err(_) => return stop(&stopped, failed("heap", "the call queue failed")),
            };
            let Some(next) = next else { break };
            if let Err(outcome) = self.call(&context, &next) {
                return stop(&stopped, outcome);
            }
        }
        self.finish(&context)
    }

    fn start(&self, context: &Context) -> Result<(), String> {
        let prelude = match &self.request.mode {
            Mode::Run { run, input, now_ms } => {
                let seed = blake3::derive_key(REFERENCE_CONTEXT, run.as_bytes());
                let words: Vec<u32> = seed
                    .chunks_exact(4)
                    .take(4)
                    .filter_map(|w| <[u8; 4]>::try_from(w).ok().map(u32::from_le_bytes))
                    .collect();
                let input = serde_json::to_string(input).map_err(|e| e.to_string())?;
                format!(
                    "{PRELUDE}\n__focal_fix({now_ms}, [{}], {});",
                    words
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                    input
                )
            }
            Mode::Search => format!(
                "{PRELUDE}\n__focal_fix(0, [1,2,3,4], {{}});\nconst registry = {};\nconst skills = {};",
                self.host.registry.listing, self.host.registry.skills
            ),
        };
        let source = format!(
            "{prelude}\n__focal_main((async () => {{{}\n}})());",
            self.request.program
        );
        context.with(|ctx| {
            ctx.eval::<(), _>(source)
                .map_err(|_| match ctx.catch().into_exception() {
                    Some(exception) => format!(
                        "{}: {}",
                        exception.message().unwrap_or_default(),
                        exception.stack().unwrap_or_default()
                    ),
                    None => "the program could not start".into(),
                })
        })
    }

    /// One queued call: `[id, name, input]` as JSON text.
    fn call(&mut self, context: &Context, next: &str) -> Result<(), CodeOutcome> {
        let parsed = crate::json::parse(next.as_bytes(), self.host.limits.tree)
            .map_err(|_| failed("argument", "a call's input passes the tree bound"))?;
        let Value::Array(parts) = parsed else {
            return Err(failed("dependency", "malformed call queue"));
        };
        let mut parts = parts.into_iter();
        let (Some(Value::Number(id)), Some(Value::String(name)), Some(input)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(failed("dependency", "malformed call queue"));
        };
        let id = id
            .as_u64()
            .and_then(|id| u32::try_from(id).ok())
            .ok_or(failed("dependency", "malformed call id"))?;
        if matches!(self.request.mode, Mode::Search) {
            return Err(failed("argument", "code.search makes no calls"));
        }
        if self.calls.len() >= self.host.limits.calls {
            return Err(failed("calls", "the program's call bound was reached"));
        }
        let settled = if !self.host.registry.listed(&name) {
            settlement(
                false,
                "Unlisted",
                &format!("{name} is not a tool this caller may use"),
            )
        } else {
            let Value::Object(mut arguments) = input else {
                return Err(failed("argument", "a call's input must be an object"));
            };
            if self.host.registry.takes_reference(&name)
                && !arguments.contains_key("operation_id")
                && let Mode::Run { run, .. } = &self.request.mode
            {
                // The ordinal is the driver's count, never an id the sandbox
                // chose: a program that forges queue entries can only reuse a
                // reference, which the journal refuses for different input.
                let ordinal = u64::try_from(self.calls.len())
                    .map_err(|_| failed("limits", "call count overflow"))?;
                let reference = reference(run, ordinal, self.host.backend.has_native())
                    .ok_or(failed("dependency", "a derived reference was zero"))?;
                arguments.insert("operation_id".into(), Value::String(reference));
            }
            let allocation = self
                .host
                .budget
                .reserve(
                    BudgetKind::Pending,
                    BudgetLane::Ordinary,
                    self.host
                        .limits
                        .tree
                        .workspace()
                        .map_err(|_| failed("limits", "bounds overflow"))?,
                )
                .map_err(|_| failed("capacity", "no memory for a call"))?
                .commit();
            let mut inner = ToolCall::nested(name.clone(), arguments, allocation);
            let (_quiet, mut quiet) = oneshot::channel();
            let result = self
                .host
                .backend
                .execute(self.host.runtime, &mut inner, &mut quiet);
            self.calls.push(CodeCall {
                tool: name,
                condition: result.condition.clone(),
                operation_id: result.operation_id.clone(),
            });
            let text = serde_json::to_string(&result)
                .map_err(|_| failed("dependency", "a result did not encode"))?;
            if text.len() > self.host.limits.tree.max_response_bytes {
                return Err(failed(
                    "result",
                    "a call's result passes the response bound",
                ));
            }
            (!result.is_error(), text)
        };
        let (ok, text) = settled;
        context
            .with(|ctx| {
                let settle: Function = ctx.globals().get("__focal_settle")?;
                settle.call::<_, ()>((id, ok, text))
            })
            .map_err(|_| failed("heap", "a result did not fit the heap"))
    }

    fn finish(&self, context: &Context) -> CodeOutcome {
        let done = context.with(|ctx| {
            ctx.eval::<Option<String>, _>(format!("__focal_done({})", self.host.limits.result))
        });
        // `__focal_done` answers "1" or "0" (returned or failed), then the text.
        let done = done.map(|done| {
            done.and_then(|text| {
                let ok = text.starts_with('1');
                text.get(1..).map(|text| (ok, text.to_owned()))
            })
        });
        match done {
            Ok(Some((true, text))) => {
                match crate::json::parse(text.as_bytes(), self.host.limits.tree) {
                    Ok(value) => CodeOutcome::Returned { value },
                    Err(_) => failed("result", "the returned value passes the tree bound"),
                }
            }
            Ok(Some((false, text))) => {
                let (code, detail) = match serde_json::from_str::<Value>(&text) {
                    Ok(Value::Object(error)) => (
                        error
                            .get("code")
                            .and_then(Value::as_str)
                            .unwrap_or("exception")
                            .to_owned(),
                        error
                            .get("detail")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    ),
                    _ => ("exception".to_owned(), text),
                };
                CodeOutcome::Failed { code, detail }
            }
            Ok(None) => failed(
                "unsettled",
                "the program awaited something that never settles",
            ),
            Err(_) => failed("heap", "the result could not be read"),
        }
    }
}

fn settlement(ok: bool, condition: &str, detail: &str) -> (bool, String) {
    let text = serde_json::json!({
        "schema_version": 2,
        "operation_id": null,
        "condition": condition,
        "result": {"kind": "error", "code": condition, "detail": detail},
    })
    .to_string();
    (ok, text)
}

/// `n1:` and the first 16 bytes of the key derived from (`run`, ordinal); on
/// a V1 ledger the same 32 hex digits. `None` for the all-zero value the
/// reference grammar refuses.
pub(crate) fn reference(run: &str, ordinal: u64, native: bool) -> Option<String> {
    let mut hasher = blake3::Hasher::new_derive_key(REFERENCE_CONTEXT);
    let length = u64::try_from(run.len()).ok()?;
    hasher.update(&length.to_le_bytes());
    hasher.update(run.as_bytes());
    hasher.update(&ordinal.to_le_bytes());
    let digest = hasher.finalize();
    let bytes = digest.as_bytes().get(..16)?;
    if bytes.iter().all(|b| *b == 0) {
        return None;
    }
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Some(if native { format!("n1:{hex}") } else { hex })
}

/// The sandbox's own definitions: the fixed clock and seeded randomness that
/// keep a replay identical, the `focal` call object whose calls queue in the
/// heap, and the settlement and completion entry points the driver uses.
const PRELUDE: &str = r#"
"use strict";
(() => {
  const J = { parse: JSON.parse, stringify: JSON.stringify };
  const RealDate = Date, RealMap = Map, RealPromise = Promise, RealProxy = Proxy, RealError = Error;
  const IE = InternalError, RE = RangeError, TE = TypeError;
  const queue = [], waiting = new RealMap();
  let nextId = 0, end;
  const fixed = (name, value) => Object.defineProperty(globalThis, name, { value, writable: false, configurable: false, enumerable: false });
  fixed("__focal_fix", (now, seed, input) => {
    function FixedDate(...a) {
      if (!new.target) return new RealDate(now).toString();
      return a.length ? new RealDate(...a) : new RealDate(now);
    }
    FixedDate.prototype = RealDate.prototype;
    FixedDate.now = () => now;
    FixedDate.parse = RealDate.parse;
    FixedDate.UTC = RealDate.UTC;
    globalThis.Date = FixedDate;
    let [a, b, c, d] = seed.map(x => x >>> 0);
    Math.random = () => {
      const t = (a + b) >>> 0;
      a = (b ^ (b >>> 9)) >>> 0;
      b = (c + (c << 3)) >>> 0;
      c = ((c << 21) | (c >>> 11)) >>> 0;
      d = (d + 1) >>> 0;
      const r = (t + d) >>> 0;
      c = (c + r) >>> 0;
      return r / 4294967296;
    };
    fixed("input", input);
  });
  const call = (name, input) => {
    if (typeof name !== "string") throw new TE("focal.call: the tool name is a string");
    const id = nextId++;
    const text = J.stringify([id, name, input === undefined ? {} : input]);
    return new RealPromise((resolve, reject) => {
      waiting.set(id, { resolve, reject });
      queue.push(text);
    });
  };
  const path = prefix => new RealProxy(function () {}, {
    get(_, key) {
      if (typeof key !== "string" || key === "then") return undefined;
      if (prefix === "" && key === "call") return call;
      return path(prefix === "" ? key : prefix + "." + key);
    },
    apply(_, __, args) {
      if (prefix === "") throw new TE("focal is not a tool");
      return call(prefix, args[0]);
    },
  });
  fixed("focal", path(""));
  fixed("__focal_next", () => queue.length ? queue.shift() : null);
  fixed("__focal_settle", (id, ok, text) => {
    const w = waiting.get(id);
    if (w === undefined) return;
    waiting.delete(id);
    const result = J.parse(text);
    if (ok) {
      w.resolve(result);
    } else {
      const error = new RealError(result.result && result.result.detail || result.condition);
      error.condition = result.condition;
      error.result = result;
      w.reject(error);
    }
  });
  const failure = e => {
    if (e instanceof IE && e.message === "out of memory") return { code: "heap", detail: e.message };
    if (e instanceof RE && e.message === "Maximum call stack size exceeded") return { code: "stack", detail: e.message };
    if (e && typeof e.condition === "string") return { code: e.condition, detail: String(e.message) };
    return { code: "exception", detail: e && e.stack ? String(e) + "\n" + e.stack : String(e) };
  };
  fixed("__focal_main", promise => {
    promise.then(v => { end = [true, v === undefined ? null : v]; }, e => { end = [false, failure(e)]; });
  });
  fixed("__focal_done", limit => {
    if (end === undefined) return null;
    let text;
    try {
      text = J.stringify(end[1]);
    } catch (e) {
      return "0" + J.stringify({ code: "result", detail: "the returned value is not JSON: " + String(e) });
    }
    if (text === undefined) text = "null";
    if (text.length > limit) {
      return "0" + J.stringify({ code: "result", detail: "the returned value is " + text.length + " characters; the bound is " + limit });
    }
    return (end[0] ? "1" : "0") + text;
  });
})();
"#;

#[cfg(test)]
#[path = "code_tests.rs"]
mod tests;
