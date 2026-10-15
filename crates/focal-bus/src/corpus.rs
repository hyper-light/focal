//! Dense JSON as agents send it: the payloads every measurement of the bus runs on.
//!
//! A claim names its task, inputs, constraints and acceptance checks; a testament its steps,
//! metrics, evidence and the artifacts it references; a progress event its phase and usage. Each is
//! one JSON object of many short fields, nested, with identifiers that share their time prefix,
//! timestamps, hashes, numbers, enumerations and natural-language text drawn from a Zipf-weighted
//! vocabulary, the distribution word frequencies follow (Zipf 1949; Piantadosi 2014, "Zipf's word
//! frequency law in natural language", Psychonomic Bulletin & Review 21(5)). A fourth shape carries a
//! base64 blob of random bytes, the case no codec can shrink, so no measurement rests on compression
//! alone.
//!
//! Every message is a function of the seed and its position: two corpora of one seed are the same
//! bytes, and a dictionary trained on one seed is measured on another's messages. The generator is
//! SplitMix64 (Steele, Lea and Flood, "Fast Splittable Pseudorandom Number Generators", OOPSLA 2014),
//! the one hyper-measure's open loop and hyper-sim draw from.

use std::fmt::Write as _;

/// The kinds of message the corpus writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// A progress event: a claim's phase, its usage so far and a sentence, 0.4 to 1 KiB.
    Event,
    /// A claim: its issuer, respondent pool, task, inputs, constraints, acceptance checks, artifact
    /// references, labels and trace, 1.5 to 6 KiB.
    Claim,
    /// A testament: its respondent, steps with their output tails, metrics, evidence and artifact
    /// references, 3 to 20 KiB.
    Testament,
    /// A base64 blob of random bytes in a small envelope, about 4 KiB: incompressible.
    Entropy,
}

impl Shape {
    /// Every shape, in the order a report lists them.
    pub const ALL: [Self; 4] = [Self::Event, Self::Claim, Self::Testament, Self::Entropy];

    /// The shape's name, as a report labels it.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Claim => "claim",
            Self::Testament => "testament",
            Self::Entropy => "entropy",
        }
    }
}

/// Why a message could not be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CorpusError {
    /// Formatting into the message's buffer failed (it never does into a `String`; the error is
    /// typed rather than assumed away).
    Format,
}

impl std::fmt::Display for CorpusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format => f.write_str("a corpus message could not be formatted"),
        }
    }
}

impl std::error::Error for CorpusError {}

impl From<std::fmt::Error> for CorpusError {
    fn from(_: std::fmt::Error) -> Self {
        Self::Format
    }
}

/// SplitMix64: one 64-bit state, advanced by the golden-ratio increment and mixed by two
/// multiply-xorshift rounds (Steele, Lea and Flood 2014, Figure 1).
#[derive(Clone, Copy, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(Self::GAMMA);
        let mut z = self.state;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ z.wrapping_shr(31)
    }

    /// A draw uniform on `[0, bound)` by Lemire's multiply-shift ("Fast Random Integer Generation
    /// in an Interval", ACM TOMACS 29(1), 2019) without the rejection step: the bias is below
    /// `bound / 2^64`, nothing a payload's statistics can show. Zero for an empty range.
    fn below(&mut self, bound: u64) -> u64 {
        let wide = u128::from(self.next()).wrapping_mul(u128::from(bound));
        u64::try_from(wide.wrapping_shr(64)).unwrap_or(0)
    }

    /// A draw uniform on `[low, high]`, `low` for an empty range.
    fn between(&mut self, low: u64, high: u64) -> u64 {
        let span = high.saturating_sub(low).saturating_add(1);
        low.saturating_add(self.below(span))
    }

    /// One of `items`, uniformly; the first for a draw past the end, which `below` never makes.
    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        let at = usize::try_from(self.below(items.len() as u64)).unwrap_or(0);
        items
            .get(at)
            .or_else(|| items.first())
            .copied()
            .unwrap_or("")
    }

    /// True with probability `numerator / denominator`.
    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }
}

/// The vocabulary text is drawn from, most frequent first: common English words and the terms of
/// the work agents do (building, testing, deploying, reviewing).
const WORDS: [&str; 243] = [
    "the",
    "to",
    "of",
    "and",
    "a",
    "in",
    "is",
    "for",
    "that",
    "on",
    "with",
    "it",
    "this",
    "as",
    "be",
    "by",
    "not",
    "are",
    "from",
    "at",
    "or",
    "an",
    "was",
    "we",
    "can",
    "build",
    "test",
    "if",
    "when",
    "all",
    "will",
    "has",
    "one",
    "each",
    "which",
    "after",
    "before",
    "new",
    "file",
    "check",
    "run",
    "pass",
    "error",
    "change",
    "into",
    "then",
    "only",
    "its",
    "now",
    "also",
    "no",
    "more",
    "should",
    "write",
    "read",
    "node",
    "cluster",
    "service",
    "request",
    "update",
    "state",
    "value",
    "config",
    "deploy",
    "pod",
    "image",
    "container",
    "release",
    "commit",
    "branch",
    "merge",
    "review",
    "patch",
    "fix",
    "issue",
    "failure",
    "retry",
    "timeout",
    "latency",
    "memory",
    "disk",
    "network",
    "queue",
    "message",
    "claim",
    "testament",
    "artifact",
    "agent",
    "task",
    "step",
    "result",
    "output",
    "input",
    "schema",
    "field",
    "type",
    "function",
    "module",
    "crate",
    "trait",
    "struct",
    "method",
    "call",
    "return",
    "loop",
    "bound",
    "limit",
    "budget",
    "token",
    "model",
    "prompt",
    "context",
    "window",
    "cache",
    "index",
    "key",
    "range",
    "page",
    "log",
    "entry",
    "record",
    "batch",
    "stream",
    "shard",
    "leader",
    "follower",
    "replica",
    "quorum",
    "term",
    "vote",
    "election",
    "snapshot",
    "compaction",
    "flush",
    "sync",
    "lock",
    "thread",
    "future",
    "await",
    "poll",
    "wake",
    "timer",
    "deadline",
    "clock",
    "metric",
    "trace",
    "span",
    "event",
    "handler",
    "route",
    "path",
    "endpoint",
    "client",
    "server",
    "connection",
    "socket",
    "packet",
    "frame",
    "header",
    "body",
    "payload",
    "bytes",
    "size",
    "count",
    "rate",
    "load",
    "scale",
    "region",
    "zone",
    "host",
    "machine",
    "core",
    "worker",
    "pool",
    "job",
    "schedule",
    "controller",
    "operator",
    "resource",
    "manifest",
    "namespace",
    "volume",
    "secret",
    "policy",
    "role",
    "account",
    "principal",
    "identity",
    "certificate",
    "signature",
    "hash",
    "digest",
    "encrypt",
    "decrypt",
    "seal",
    "verify",
    "valid",
    "invalid",
    "missing",
    "stale",
    "fresh",
    "slow",
    "fast",
    "small",
    "large",
    "first",
    "last",
    "next",
    "previous",
    "every",
    "some",
    "most",
    "least",
    "between",
    "within",
    "without",
    "across",
    "through",
    "against",
    "under",
    "over",
    "because",
    "since",
    "while",
    "until",
    "unless",
    "although",
    "therefore",
    "however",
    "instead",
    "again",
    "already",
    "still",
    "never",
    "always",
    "often",
    "rarely",
    "kubernetes",
    "rust",
    "cargo",
    "clippy",
    "tests",
    "suite",
];

/// Cumulative Zipf weights over [`WORDS`]: rank `r` (from 1) weighs `K / r`.
struct Zipf {
    cumulative: [u32; WORDS.len()],
    total: u32,
}

impl Zipf {
    const K: u32 = 1 << 20;

    fn new() -> Self {
        let mut cumulative = [0_u32; WORDS.len()];
        let mut total = 0_u32;
        let mut rank = 1_u32;
        for slot in &mut cumulative {
            total = total.saturating_add(Self::K.checked_div(rank).unwrap_or(0));
            *slot = total;
            rank = rank.saturating_add(1);
        }
        Self { cumulative, total }
    }

    fn word(&self, rng: &mut SplitMix64) -> &'static str {
        let draw = u32::try_from(rng.below(u64::from(self.total))).unwrap_or(0);
        let at = self.cumulative.partition_point(|&edge| edge <= draw);
        WORDS
            .get(at)
            .or_else(|| WORDS.first())
            .copied()
            .unwrap_or("")
    }
}

const REGIONS: [&str; 8] = [
    "us-east-1",
    "us-east-2",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
    "sa-east-1",
];
const MODELS: [&str; 6] = [
    "claude-opus-5-5",
    "claude-sonnet-5-5",
    "claude-haiku-4-5",
    "gpt-5.5",
    "gemini-3-pro",
    "llama-5-405b",
];
const POOLS: [&str; 8] = [
    "builders.rust",
    "builders.go",
    "reviewers.security",
    "reviewers.style",
    "testers.integration",
    "operators.kubernetes",
    "writers.docs",
    "planners.design",
];
const CAPABILITIES: [&str; 12] = [
    "rust",
    "go",
    "kubernetes",
    "terraform",
    "testing",
    "fuzzing",
    "review",
    "security",
    "docs",
    "profiling",
    "networking",
    "storage",
];
const TOOLS: [&str; 10] = [
    "cargo", "git", "kubectl", "helm", "docker", "rg", "perf", "gh", "make", "python3",
];
const TASKS: [&str; 7] = [
    "implement",
    "review",
    "test",
    "deploy",
    "investigate",
    "refactor",
    "document",
];
const PHASES: [&str; 7] = [
    "planning",
    "reading",
    "editing",
    "building",
    "testing",
    "reviewing",
    "reporting",
];
const STATUSES: [&str; 3] = ["succeeded", "failed", "partial"];
const EXPECTS: [&str; 3] = ["pass", "no-diff", "exit-zero"];
const SANDBOXES: [&str; 3] = ["container", "microvm", "process"];
const ENVS: [&str; 3] = ["prod", "staging", "dev"];
const MEDIA: [&str; 6] = [
    "application/zstd",
    "text/markdown",
    "application/json",
    "video/mp4",
    "audio/ogg",
    "application/x-tar",
];
const EXTENSIONS: [&str; 6] = ["zst", "md", "json", "mp4", "ogg", "tar"];
const COMMANDS: [&str; 6] = [
    "cargo test -p",
    "cargo clippy -p",
    "kubectl apply -f",
    "git diff --stat",
    "make check",
    "helm upgrade --install",
];
const LANGUAGES: [&str; 4] = ["rust", "go", "yaml", "markdown"];

/// Crockford's base32 alphabet, which ULIDs are written in.
const BASE32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const HEX: &[u8; 16] = b"0123456789abcdef";
const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// The first message's clock: 2026-10-15T00:00:00Z in milliseconds since the Unix epoch.
const EPOCH_MS: u64 = 1_792_022_400_000;
const DAY_MS: u64 = 86_400_000;

/// A number in `[0, 1)`: its decimal digits and how many places they fill.
struct Fraction {
    digits: u64,
    places: usize,
}

impl std::fmt::Display for Fraction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0.{:0width$}", self.digits, width = self.places)
    }
}

/// A dollar amount with four decimals.
struct Money {
    whole: u64,
    ten_thousandths: u64,
}

impl std::fmt::Display for Money {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{:04}", self.whole, self.ten_thousandths)
    }
}

/// A source of dense JSON messages, a function of its seed.
pub struct Corpus {
    rng: SplitMix64,
    zipf: Zipf,
    /// Milliseconds since [`EPOCH_MS`]: each message advances it, as a fleet's clocks do, so
    /// identifiers and timestamps of neighbouring messages share their leading digits.
    clock_ms: u64,
    /// Messages written: a claim's sequence numbers and progress counts follow it.
    written: u64,
}

impl Corpus {
    /// A corpus whose messages are a function of `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            zipf: Zipf::new(),
            clock_ms: 0,
            written: 0,
        }
    }

    /// Writes the next message of `shape` into `out`, which it clears first, keeping its capacity.
    pub fn next(&mut self, shape: Shape, out: &mut String) -> Result<(), CorpusError> {
        out.clear();
        self.clock_ms = self.clock_ms.saturating_add(self.rng.between(0, 40));
        self.written = self.written.saturating_add(1);
        match shape {
            Shape::Event => self.event(out),
            Shape::Claim => self.claim(out),
            Shape::Testament => self.testament(out),
            Shape::Entropy => self.entropy(out),
        }
    }

    fn event(&mut self, out: &mut String) -> Result<(), CorpusError> {
        out.push_str("{\"kind\":\"progress\",\"id\":");
        self.ulid(out, "evt_");
        out.push_str(",\"claim\":");
        self.ulid(out, "clm_");
        out.push_str(",\"agent\":");
        self.ulid(out, "agt_");
        write!(
            out,
            ",\"seq\":{},\"at\":",
            self.written.checked_rem(1_000).unwrap_or(0)
        )?;
        self.timestamp(out, 0)?;
        out.push_str(",\"phase\":");
        push_string(out, self.rng.pick(&PHASES));
        write!(out, ",\"progress\":{},\"message\":", self.fraction(4))?;
        self.sentence(out, 8, 22);
        out.push_str(",\"usage\":{");
        write!(
            out,
            "\"input_tokens\":{},\"output_tokens\":{},\"cached_tokens\":{},\"cost_usd\":{}",
            self.rng.between(1_000, 400_000),
            self.rng.between(10, 40_000),
            self.rng.between(0, 300_000),
            self.money(0, 20)
        )?;
        out.push_str("},\"host\":{\"region\":");
        let region = self.rng.pick(&REGIONS);
        push_string(out, region);
        write!(
            out,
            ",\"zone\":\"{region}{}\",\"node\":\"node-{}\"}},\"tags\":",
            char::from(b'a'.saturating_add(u8::try_from(self.rng.below(3)).unwrap_or(0))),
            self.rng.between(1, 64)
        )?;
        self.words_array(out, &CAPABILITIES, 1, 3);
        out.push('}');
        Ok(())
    }

    fn claim(&mut self, out: &mut String) -> Result<(), CorpusError> {
        out.push_str("{\"kind\":\"claim\",\"id\":");
        self.ulid(out, "clm_");
        write!(
            out,
            ",\"version\":{},\"issued_at\":",
            self.rng.between(1, 4)
        )?;
        self.timestamp(out, 0)?;
        write!(
            out,
            ",\"deadline_ms\":{},\"priority\":{},\"attempt\":{},\"issuer\":{{\"agent\":",
            self.rng.between(10, 3_600).saturating_mul(1_000),
            self.rng.between(0, 9),
            self.rng.between(1, 3)
        )?;
        self.ulid(out, "agt_");
        out.push_str(",\"principal\":");
        self.ulid(out, "prn_");
        out.push_str(",\"session\":");
        self.ulid(out, "ses_");
        out.push_str(",\"model\":");
        push_string(out, self.rng.pick(&MODELS));
        out.push_str(",\"region\":");
        push_string(out, self.rng.pick(&REGIONS));
        out.push_str("},\"respondent\":{\"pool\":");
        push_string(out, self.rng.pick(&POOLS));
        out.push_str(",\"agent\":");
        if self.rng.chance(1, 3) {
            self.ulid(out, "agt_");
        } else {
            out.push_str("null");
        }
        out.push_str(",\"capabilities\":");
        self.words_array(out, &CAPABILITIES, 2, 5);
        out.push_str("},\"task\":{\"type\":");
        push_string(out, self.rng.pick(&TASKS));
        out.push_str(",\"title\":");
        self.sentence(out, 6, 14);
        out.push_str(",\"description\":");
        self.paragraph(out, 3, 7);
        out.push_str(",\"repository\":{\"url\":\"https://github.com/");
        out.push_str(self.zipf.word(&mut self.rng));
        out.push('-');
        out.push_str(self.zipf.word(&mut self.rng));
        out.push('/');
        out.push_str(self.zipf.word(&mut self.rng));
        out.push_str("\",\"commit\":\"");
        self.hex(out, 40);
        out.push_str("\",\"branch\":\"");
        out.push_str(self.rng.pick(&TASKS));
        out.push('/');
        out.push_str(self.zipf.word(&mut self.rng));
        out.push('-');
        out.push_str(self.zipf.word(&mut self.rng));
        out.push_str("\"},\"inputs\":[");
        let inputs = self.rng.between(4, 16);
        for at in 0..inputs {
            if at > 0 {
                out.push(',');
            }
            out.push_str("{\"path\":");
            self.source_path(out);
            out.push_str(",\"sha256\":\"");
            self.hex(out, 64);
            write!(
                out,
                "\",\"bytes\":{},\"language\":",
                self.rng.between(200, 120_000)
            )?;
            push_string(out, self.rng.pick(&LANGUAGES));
            out.push('}');
        }
        write!(
            out,
            "],\"constraints\":{{\"timeout_s\":{},\"max_tokens\":{},\"network\":{},\"tools\":",
            self.rng.between(60, 3_600),
            self.rng.between(8, 400).saturating_mul(1_000),
            self.rng.chance(1, 4)
        )?;
        self.words_array(out, &TOOLS, 3, 6);
        write!(out, ",\"budget_usd\":{},\"sandbox\":", self.money(1, 40))?;
        push_string(out, self.rng.pick(&SANDBOXES));
        out.push_str("},\"acceptance\":[");
        let checks = self.rng.between(2, 5);
        for at in 0..checks {
            if at > 0 {
                out.push(',');
            }
            out.push_str("{\"check\":\"");
            out.push_str(self.rng.pick(&COMMANDS));
            out.push(' ');
            out.push_str(self.zipf.word(&mut self.rng));
            out.push_str("\",\"expect\":");
            push_string(out, self.rng.pick(&EXPECTS));
            write!(out, ",\"weight\":{}}}", self.fraction(2))?;
        }
        out.push_str("],\"context\":{\"parent\":");
        self.ulid(out, "clm_");
        write!(out, ",\"depth\":{},\"related\":[", self.rng.between(0, 6))?;
        let related = self.rng.between(0, 4);
        for at in 0..related {
            if at > 0 {
                out.push(',');
            }
            self.ulid(out, "clm_");
        }
        out.push_str("]}},\"artifacts\":");
        self.artifacts(out, 0, 4)?;
        out.push_str(",\"labels\":{\"team\":");
        push_string(out, self.zipf.word(&mut self.rng));
        out.push_str(",\"service\":");
        push_string(out, self.zipf.word(&mut self.rng));
        out.push_str(",\"env\":");
        push_string(out, self.rng.pick(&ENVS));
        write!(
            out,
            ",\"cost_center\":\"cc-{}\"}},\"trace\":",
            self.rng.between(100, 999)
        )?;
        self.trace(out);
        out.push('}');
        Ok(())
    }

    fn testament(&mut self, out: &mut String) -> Result<(), CorpusError> {
        out.push_str("{\"kind\":\"testament\",\"id\":");
        self.ulid(out, "tst_");
        out.push_str(",\"claim\":");
        self.ulid(out, "clm_");
        out.push_str(",\"respondent\":{\"agent\":");
        self.ulid(out, "agt_");
        out.push_str(",\"model\":");
        push_string(out, self.rng.pick(&MODELS));
        out.push_str(",\"region\":");
        push_string(out, self.rng.pick(&REGIONS));
        out.push_str("},\"status\":");
        push_string(out, self.rng.pick(&STATUSES));
        out.push_str(",\"started_at\":");
        let duration = self.rng.between(2_000, 1_800_000);
        self.timestamp(out, 0)?;
        out.push_str(",\"finished_at\":");
        self.timestamp(out, duration)?;
        write!(out, ",\"duration_ms\":{duration},\"summary\":")?;
        self.paragraph(out, 2, 6);
        out.push_str(",\"steps\":[");
        let steps = self.rng.between(3, 12);
        for at in 0..steps {
            if at > 0 {
                out.push(',');
            }
            out.push_str("{\"name\":\"");
            out.push_str(self.zipf.word(&mut self.rng));
            out.push(' ');
            out.push_str(self.zipf.word(&mut self.rng));
            out.push_str("\",\"command\":\"");
            out.push_str(self.rng.pick(&COMMANDS));
            out.push(' ');
            out.push_str(self.zipf.word(&mut self.rng));
            out.push_str("\",\"started_at\":");
            let offset = self.rng.below(duration.max(1));
            self.timestamp(out, offset)?;
            write!(
                out,
                ",\"duration_ms\":{},\"exit_code\":{},\"stdout_tail\":",
                self.rng.between(5, 600_000),
                if self.rng.chance(1, 8) { 1 } else { 0 }
            )?;
            self.paragraph(out, 2, 4);
            out.push_str(",\"files_touched\":[");
            let touched = self.rng.between(0, 5);
            for file in 0..touched {
                if file > 0 {
                    out.push(',');
                }
                self.source_path(out);
            }
            out.push_str("]}");
        }
        write!(
            out,
            "],\"metrics\":{{\"tests_passed\":{},\"tests_failed\":{},\"coverage\":{},\"lines_added\":{},\"lines_removed\":{},",
            self.rng.between(0, 4_000),
            self.rng.between(0, 12),
            self.fraction(4),
            self.rng.between(0, 3_000),
            self.rng.between(0, 2_000)
        )?;
        write!(
            out,
            "\"tokens\":{{\"input\":{},\"output\":{},\"cached\":{}}},\"cost_usd\":{},\"latency_ms\":{{\"p50\":{},\"p99\":{}}}}}",
            self.rng.between(1_000, 900_000),
            self.rng.between(100, 90_000),
            self.rng.between(0, 800_000),
            self.money(0, 60),
            self.rng.between(1, 400),
            self.rng.between(400, 9_000)
        )?;
        out.push_str(",\"artifacts\":");
        self.artifacts(out, 1, 6)?;
        out.push_str(",\"evidence\":{\"commit\":\"");
        self.hex(out, 40);
        write!(
            out,
            "\",\"diff_stat\":{{\"files\":{},\"insertions\":{},\"deletions\":{}}},\"checks\":[",
            self.rng.between(1, 60),
            self.rng.between(0, 3_000),
            self.rng.between(0, 2_000)
        )?;
        let checks = self.rng.between(2, 6);
        for at in 0..checks {
            if at > 0 {
                out.push(',');
            }
            out.push_str("{\"name\":\"");
            out.push_str(self.rng.pick(&COMMANDS));
            out.push_str("\",\"status\":");
            push_string(out, self.rng.pick(&EXPECTS));
            write!(out, ",\"duration_ms\":{}}}", self.rng.between(50, 900_000))?;
        }
        out.push_str("],\"digest\":\"b3:");
        self.hex(out, 64);
        out.push_str("\"},\"trace\":");
        self.trace(out);
        out.push('}');
        Ok(())
    }

    fn entropy(&mut self, out: &mut String) -> Result<(), CorpusError> {
        out.push_str("{\"kind\":\"blob\",\"id\":");
        self.ulid(out, "blb_");
        out.push_str(",\"encoding\":\"base64\",\"data\":\"");
        let bytes = self.rng.between(2_400, 3_600);
        let mut left = bytes;
        while left > 0 {
            let word = self.rng.next();
            for shift in [0_u32, 6, 12, 18] {
                let at = usize::try_from(word.wrapping_shr(shift) & 63).unwrap_or(0);
                out.push(char::from(BASE64.get(at).copied().unwrap_or(b'A')));
            }
            left = left.saturating_sub(3);
        }
        out.push_str("\"}");
        Ok(())
    }

    fn artifacts(&mut self, out: &mut String, low: u64, high: u64) -> Result<(), CorpusError> {
        out.push('[');
        let count = self.rng.between(low, high);
        for at in 0..count {
            if at > 0 {
                out.push(',');
            }
            out.push_str("{\"ref\":\"b3:");
            self.hex(out, 64);
            let kind = usize::try_from(self.rng.below(u64::try_from(MEDIA.len()).unwrap_or(1)))
                .unwrap_or(0);
            write!(
                out,
                "\",\"bytes\":{},\"media_type\":",
                self.rng.between(100, 2_000_000_000)
            )?;
            push_string(out, MEDIA.get(kind).copied().unwrap_or("application/json"));
            out.push_str(",\"name\":\"");
            out.push_str(self.zipf.word(&mut self.rng));
            out.push('.');
            out.push_str(EXTENSIONS.get(kind).copied().unwrap_or("json"));
            out.push_str("\"}");
        }
        out.push(']');
        Ok(())
    }

    fn trace(&mut self, out: &mut String) {
        out.push_str("{\"trace_id\":\"");
        self.hex(out, 32);
        out.push_str("\",\"span_id\":\"");
        self.hex(out, 16);
        out.push_str("\",\"parent_span\":\"");
        self.hex(out, 16);
        out.push_str("\"}");
    }

    fn source_path(&mut self, out: &mut String) {
        out.push_str("\"crates/");
        out.push_str(self.zipf.word(&mut self.rng));
        out.push_str("/src/");
        out.push_str(self.zipf.word(&mut self.rng));
        out.push_str(".rs\"");
    }

    /// A ULID with `prefix` as a JSON string: 10 characters of the clock, then 16 random.
    fn ulid(&mut self, out: &mut String, prefix: &str) {
        out.push('"');
        out.push_str(prefix);
        let time = EPOCH_MS.saturating_add(self.clock_ms);
        for at in (0..10_u32).rev() {
            let digit = usize::try_from(time.wrapping_shr(at.saturating_mul(5)) & 31).unwrap_or(0);
            out.push(char::from(BASE32.get(digit).copied().unwrap_or(b'0')));
        }
        let mut random = self.rng.next();
        for _ in 0..16 {
            let digit = usize::try_from(random & 31).unwrap_or(0);
            out.push(char::from(BASE32.get(digit).copied().unwrap_or(b'0')));
            random = random.wrapping_shr(4);
        }
        out.push('"');
    }

    /// The message's clock plus `later_ms` as an RFC 3339 timestamp with microseconds.
    fn timestamp(&mut self, out: &mut String, later_ms: u64) -> Result<(), CorpusError> {
        let at = self.clock_ms.saturating_add(later_ms);
        let day = at.checked_div(DAY_MS).unwrap_or(0);
        let of_day = at.checked_rem(DAY_MS).unwrap_or(0);
        let micros = of_day
            .checked_rem(1_000)
            .unwrap_or(0)
            .saturating_mul(1_000)
            .saturating_add(self.rng.below(1_000));
        let seconds = of_day.checked_div(1_000).unwrap_or(0);
        write!(
            out,
            "\"2026-10-{:02}T{:02}:{:02}:{:02}.{:06}Z\"",
            15_u64.saturating_add(day).min(31),
            seconds.checked_div(3_600).unwrap_or(0),
            seconds
                .checked_div(60)
                .unwrap_or(0)
                .checked_rem(60)
                .unwrap_or(0),
            seconds.checked_rem(60).unwrap_or(0),
            micros
        )?;
        Ok(())
    }

    fn hex(&mut self, out: &mut String, digits: u32) {
        let mut left = digits;
        while left > 0 {
            let mut word = self.rng.next();
            for _ in 0..left.min(16) {
                let at = usize::try_from(word & 15).unwrap_or(0);
                out.push(char::from(HEX.get(at).copied().unwrap_or(b'0')));
                word = word.wrapping_shr(4);
            }
            left = left.saturating_sub(16);
        }
    }

    /// A number in `[0, 1)` with `places` decimals, as its digits.
    fn fraction(&mut self, places: u32) -> Fraction {
        Fraction {
            digits: self.rng.below(10_u64.saturating_pow(places)),
            places: usize::try_from(places).unwrap_or(0),
        }
    }

    /// A dollar amount in `[low, high]` with four decimals.
    fn money(&mut self, low: u64, high: u64) -> Money {
        Money {
            whole: self.rng.between(low, high),
            ten_thousandths: self.rng.below(10_000),
        }
    }

    fn words_array(&mut self, out: &mut String, items: &[&str], low: u64, high: u64) {
        out.push('[');
        let count = self.rng.between(low, high);
        for at in 0..count {
            if at > 0 {
                out.push(',');
            }
            let item = self.rng.pick(items);
            push_string(out, item);
        }
        out.push(']');
    }

    /// A sentence of `low` to `high` Zipf-drawn words as a JSON string.
    fn sentence(&mut self, out: &mut String, low: u64, high: u64) {
        out.push('"');
        self.sentence_body(out, low, high);
        out.push('"');
    }

    fn sentence_body(&mut self, out: &mut String, low: u64, high: u64) {
        let count = self.rng.between(low, high);
        for at in 0..count {
            let word = self.zipf.word(&mut self.rng);
            if at == 0 {
                let mut chars = word.chars();
                if let Some(first) = chars.next() {
                    out.extend(first.to_uppercase());
                    out.push_str(chars.as_str());
                }
            } else {
                out.push(' ');
                out.push_str(word);
            }
        }
        out.push('.');
    }

    /// `low` to `high` sentences as one JSON string.
    fn paragraph(&mut self, out: &mut String, low: u64, high: u64) {
        out.push('"');
        let count = self.rng.between(low, high);
        for at in 0..count {
            if at > 0 {
                out.push(' ');
            }
            self.sentence_body(out, 6, 24);
        }
        out.push('"');
    }
}

/// `text` as a JSON string, escaped (RFC 8259 §7).
fn push_string(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_writes_the_same_messages_and_another_seed_others() {
        let mut first = Corpus::new(7);
        let mut again = Corpus::new(7);
        let mut other = Corpus::new(8);
        let (mut a, mut b, mut c) = (String::new(), String::new(), String::new());
        for shape in Shape::ALL {
            first.next(shape, &mut a).unwrap();
            again.next(shape, &mut b).unwrap();
            other.next(shape, &mut c).unwrap();
            assert_eq!(a, b);
            assert_ne!(a, c);
        }
    }

    #[test]
    fn every_message_is_one_json_object_within_its_shapes_size() {
        let mut corpus = Corpus::new(1);
        let mut out = String::new();
        for shape in Shape::ALL {
            let (low, high) = match shape {
                Shape::Event => (300, 1_200),
                Shape::Claim => (1_200, 7_000),
                Shape::Testament => (2_000, 24_000),
                Shape::Entropy => (3_000, 5_200),
            };
            for _ in 0..500 {
                corpus.next(shape, &mut out).unwrap();
                let value: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert!(value.is_object(), "{shape:?}: {out}");
                assert!(
                    (low..=high).contains(&out.len()),
                    "{shape:?} of {} bytes: {out}",
                    out.len()
                );
            }
        }
    }
}
