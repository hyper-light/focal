//! Optional client request tracing to a file (the hidden `--trace-file` flag),
//! the capture point for the R11 §1 black-box history harness. Off unless the
//! flag is given; a trace that fails to serialize or write is dropped so
//! tracing never changes what the command does or returns.

use focal_client::{TraceEntry, TraceSink};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// Set once from the parsed `--trace-file` argument before any client is built.
static TRACE_FILE: OnceLock<Option<PathBuf>> = OnceLock::new();

/// Record the trace destination for this invocation (the parsed flag value).
pub(crate) fn configure(path: Option<PathBuf>) {
    let _ = TRACE_FILE.set(path);
}

/// A trace sink for a client, if `--trace-file` was given. Every client in the
/// invocation appends to the same file; entries carry per-client call numbers
/// and timestamps, so a merge step orders them.
pub(super) fn sink() -> Option<Box<dyn TraceSink>> {
    let path = TRACE_FILE.get()?.as_ref()?;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()?;
    Some(Box::new(FileTraceSink {
        file: Mutex::new(file),
    }))
}

/// Appends each completed exchange as one JSON line. Best-effort: any error is
/// dropped rather than surfaced, so a diagnostic trace cannot perturb the run.
struct FileTraceSink {
    file: Mutex<File>,
}
impl TraceSink for FileTraceSink {
    fn record(&self, entry: TraceEntry) {
        let Ok(mut line) = serde_json::to_vec(&entry) else {
            return;
        };
        line.push(b'\n');
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(&line);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_client::TraceOutcome;
    use focal_model::{
        ContentHash, LedgerId, RequestEpoch, RequestId, SessionId, SessionSeq, TenantId,
    };

    fn entry(call: u64, id: u128) -> TraceEntry {
        TraceEntry {
            call,
            invoked_nanos: 10,
            completed_nanos: 20,
            ledger: LedgerId {
                tenant: TenantId::from_u128(1),
                session: SessionId::from_u128(2),
            },
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            mutation: true,
            outcome: TraceOutcome::Committed {
                sequence: SessionSeq(call + 1),
                command_hash: ContentHash([7; 32]),
            },
        }
    }

    #[test]
    fn file_sink_appends_one_json_line_per_exchange_that_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        {
            let sink = FileTraceSink {
                file: std::sync::Mutex::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .unwrap(),
                ),
            };
            sink.record(entry(0, 100));
            sink.record(entry(1, 101));
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON line per exchange");
        let decoded: Vec<TraceEntry> = lines
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(decoded[0], entry(0, 100));
        assert_eq!(decoded[1], entry(1, 101));
    }
}
