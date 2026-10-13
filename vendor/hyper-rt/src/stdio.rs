//! Standard input and output without blocking a shard (docs/runtime.md §6.2). Readiness serves a pipe, a
//! socket or a terminal on Unix but not a regular file (epoll refuses one with `EPERM`), and on Windows a
//! console or an anonymous pipe cannot be polled at all. One mechanism serves every OS and every kind of
//! handle: **one reader thread and one writer thread for the process**, each moving bytes between the
//! handle and a task through §8's bounded channel.
//!
//! **Bounded, and allocated once**: each direction holds `depth` buffers of `chunk` bytes (the consumer's;
//! a page is the natural chunk), allocated at the start and passed back and forth — full ones one way,
//! empty ones back — so moving bytes allocates nothing. `depth × chunk` bytes each way, two threads
//! (DERIVED).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::RtError;
use crate::sync::{ChannelReceiver, OneshotSender, Sender, channel, oneshot};

static STDIN_TAKEN: AtomicBool = AtomicBool::new(false);
static STDOUT_TAKEN: AtomicBool = AtomicBool::new(false);

/// Starts a named stdio thread.
fn thread(name: &str, body: impl FnOnce() + Send + 'static) -> Result<(), RtError> {
    #[allow(
        clippy::disallowed_methods,
        reason = "the process's stdio threads (docs/runtime.md §6.2): one reader, one writer"
    )]
    let spawned = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(body);
    spawned.map(|_| ()).map_err(|_| RtError::BadConfig {
        what: "the OS refused a stdio thread",
    })
}

/// A buffer pool's two ends: where empty buffers go, and where they come from.
type Pool = (Sender<Vec<u8>>, ChannelReceiver<Vec<u8>>);

/// `depth` empty buffers of `chunk` bytes' capacity, on a channel, and the channel's other end.
fn pool(depth: usize, chunk: usize) -> Result<Pool, RtError> {
    if depth == 0 || chunk == 0 {
        return Err(RtError::BadConfig {
            what: "stdio with no buffers or empty ones",
        });
    }
    let (empty_tx, empty_rx) = channel(depth)?;
    for _ in 0..depth {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(chunk)
            .map_err(|_| RtError::Capacity {
                what: "stdio buffer bytes",
                bound: chunk,
            })?;
        empty_tx.try_send(buffer).map_err(|_| RtError::BadConfig {
            what: "the stdio buffer pool closed while filling",
        })?;
    }
    Ok((empty_tx, empty_rx))
}

/// The process's standard input, read by a task.
#[derive(Debug)]
pub struct Stdin {
    full: ChannelReceiver<Vec<u8>>,
    empty: Sender<Vec<u8>>,
    current: Option<Vec<u8>>,
    at: usize,
}

/// Takes the process's standard input: `depth` buffers of `chunk` bytes. Refused `BadConfig` when taken
/// already.
pub fn stdin(depth: usize, chunk: usize) -> Result<Stdin, RtError> {
    if STDIN_TAKEN.swap(true, Ordering::AcqRel) {
        return Err(RtError::BadConfig {
            what: "standard input taken twice",
        });
    }
    let (empty_tx, mut empty_rx) = pool(depth, chunk)?;
    let (full_tx, full_rx) = channel(depth)?;
    thread("hyper-rt-stdin", move || {
        let mut input = std::io::stdin().lock();
        // Each empty buffer comes back from the task; the end of input (or an error, which ends it too)
        // closes the full side, which the task reads as end of stream.
        while let Ok(mut buffer) = empty_rx.blocking_recv() {
            buffer.resize(buffer.capacity(), 0);
            match input.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    buffer.truncate(n);
                    if full_tx.blocking_send(buffer).is_err() {
                        return;
                    }
                }
            }
        }
    })?;
    Ok(Stdin {
        full: full_rx,
        empty: empty_tx,
        current: None,
        at: 0,
    })
}

impl Stdin {
    /// Reads into `buf`, awaiting input: the byte count, zero at end of input.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, RtError> {
        loop {
            if let Some(current) = &self.current {
                let rest = current.get(self.at..).unwrap_or(&[]);
                if !rest.is_empty() {
                    let n = rest.len().min(buf.len());
                    if let (Some(into), Some(from)) = (buf.get_mut(..n), rest.get(..n)) {
                        into.copy_from_slice(from);
                    }
                    self.at = self.at.saturating_add(n);
                    return Ok(n);
                }
            }
            if let Some(mut done) = self.current.take() {
                done.clear();
                // Back to the reader; its slot is the one this buffer left.
                let _ = self.empty.try_send(done);
            }
            match self.full.recv().await {
                Ok(next) => {
                    self.current = Some(next);
                    self.at = 0;
                }
                Err(_) => return Ok(0),
            }
        }
    }
}

/// The process's standard output, written by a task.
#[derive(Debug)]
pub struct Stdout {
    out: Sender<OutMessage>,
    empty: ChannelReceiver<Vec<u8>>,
}

/// What the writer thread is handed.
#[derive(Debug)]
struct OutMessage(OutInner);

#[derive(Debug)]
enum OutInner {
    Bytes(Vec<u8>),
    Flush(OneshotSender<bool>),
}

/// Takes the process's standard output: `depth` buffers of `chunk` bytes. Refused `BadConfig` when taken
/// already.
pub fn stdout(depth: usize, chunk: usize) -> Result<Stdout, RtError> {
    if STDOUT_TAKEN.swap(true, Ordering::AcqRel) {
        return Err(RtError::BadConfig {
            what: "standard output taken twice",
        });
    }
    let (empty_tx, empty_rx) = pool(depth, chunk)?;
    // One more than the buffers, for a flush queued behind a full set of them.
    let (out_tx, mut out_rx) = channel::<OutMessage>(depth.saturating_add(1))?;
    thread("hyper-rt-stdout", move || {
        // Locked per message, not for the thread's life, so other writers in the process (a log line)
        // interleave between chunks rather than wait forever.
        let output = std::io::stdout();
        let mut failed = false;
        while let Ok(OutMessage(message)) = out_rx.blocking_recv() {
            match message {
                OutInner::Bytes(mut buffer) => {
                    failed |= output.lock().write_all(&buffer).is_err();
                    buffer.clear();
                    if empty_tx.blocking_send(buffer).is_err() {
                        return;
                    }
                }
                OutInner::Flush(done) => {
                    failed |= output.lock().flush().is_err();
                    let _ = done.send(!failed);
                }
            }
        }
    })?;
    Ok(Stdout {
        out: out_tx,
        empty: empty_rx,
    })
}

impl Stdout {
    /// Writes all of `buf`, a chunk at a time, awaiting a free buffer while the writer is behind.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> Result<(), RtError> {
        while !buf.is_empty() {
            let mut buffer = self.empty.recv().await.map_err(|_| closed())?;
            let n = buffer.capacity().min(buf.len());
            let (now, rest) = buf.split_at(n);
            buffer.extend_from_slice(now);
            buf = rest;
            self.out
                .send(OutMessage(OutInner::Bytes(buffer)))
                .await
                .map_err(|_| closed())?;
        }
        Ok(())
    }

    /// Waits until everything written so far is flushed to the handle; refused when a write failed.
    pub async fn flush(&mut self) -> Result<(), RtError> {
        let (done, flushed) = oneshot()?;
        self.out
            .send(OutMessage(OutInner::Flush(done)))
            .await
            .map_err(|_| closed())?;
        match flushed.await {
            Ok(true) => Ok(()),
            _ => Err(closed()),
        }
    }
}

fn closed() -> RtError {
    RtError::DriverRefused {
        call: "write(stdout)",
        code: None,
    }
}
