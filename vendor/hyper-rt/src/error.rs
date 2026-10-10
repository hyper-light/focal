//! The closed refusal taxonomy of the runtime (§4.3, failure matrix).

use std::fmt;

use crate::mem::MemError;

/// A typed refusal from the runtime; never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtError {
    /// The shard's task arena is full; admission refused.
    TooManyTasks {
        /// The arena's capacity in tasks.
        capacity: usize,
    },
    /// The task id names a slot whose generation moved on, or never existed.
    StaleTask {
        /// The slot.
        slot: u32,
        /// The generation the id carried.
        generation: u32,
    },
    /// The process has registered every shard id it may.
    TooManyShards {
        /// The bound.
        max: u16,
    },
    /// The OS refused a driver call.
    DriverRefused {
        /// The call.
        call: &'static str,
        /// The OS error code, when one exists.
        code: Option<i32>,
    },
    /// The driver died while the shard waited on it (a simulation injection, or the OS closing
    /// the descriptor beneath us); the shard cancels its tasks and exits.
    DriverLost,
    /// A call that needs the current shard was made from a thread that runs none.
    NotOnShardThread,
    /// A configuration value the runtime cannot honour.
    BadConfig {
        /// Which value.
        what: &'static str,
    },
    /// A shard's control channel is full: its admission limit of spawns is pending.
    ControlFull {
        /// The shard.
        shard: u16,
    },
    /// The shard named no longer runs (or never existed).
    ShardGone {
        /// The shard.
        shard: u16,
    },
    /// A memory refusal beneath the runtime.
    Mem(MemError),
    /// The shard's kept values are borrowed — a keep inside a `Kept::with` on the same shard, or inside
    /// another keep's build — so a value cannot be added now (AUD-29-08).
    KeptInUse {
        /// The shard.
        shard: u16,
    },
    /// A ring half the shard must own alone — its foreign wake ring's consumer, a pair ring's producer or
    /// consumer — was already handed out (AUD-29-33: each ring has exactly one of each).
    RingClaimed {
        /// The shard.
        shard: u16,
    },
    /// A shard's worker thread ended without a result: it panicked (AUD-29-12). Its slot was still given
    /// back and its siblings still stopped and joined.
    WorkerFailed {
        /// The shard.
        shard: u16,
    },
    /// A timer id that is not in the state the call needs: armed when it was to be armed, or not armed when
    /// it was to be disarmed (fired, never armed, or past the wheel's capacity).
    TimerState {
        /// The timer.
        id: u32,
        /// Whether it was armed.
        armed: bool,
    },
    /// A bounded table or queue of the shard's is at its bound (docs/runtime.md §2): the request is
    /// refused, never queued without a bound.
    Capacity {
        /// What is full.
        what: &'static str,
        /// Its bound.
        bound: usize,
    },
    /// The OS had no room for a non-blocking call now (`EAGAIN`, `WSAEWOULDBLOCK`): local pressure, not a
    /// failure of the socket. Nothing was sent; the call is retried once the socket is writable (AUD-29-61).
    WouldBlock {
        /// The call.
        call: &'static str,
    },
}

impl fmt::Display for RtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyTasks { capacity } => {
                write!(f, "too many tasks: the arena holds {capacity}")
            }
            Self::StaleTask { slot, generation } => {
                write!(f, "stale task: slot {slot} generation {generation}")
            }
            Self::TooManyShards { max } => write!(f, "too many shards: at most {max} per process"),
            Self::DriverRefused { call, code } => {
                write!(f, "the OS refused {call} (code {code:?})")
            }
            Self::DriverLost => f.write_str("the driver was lost while waiting"),
            Self::NotOnShardThread => f.write_str("not on a shard thread"),
            Self::BadConfig { what } => write!(f, "bad configuration: {what}"),
            Self::ControlFull { shard } => write!(f, "shard {shard}'s control channel is full"),
            Self::ShardGone { shard } => write!(f, "shard {shard} is gone"),
            Self::Mem(e) => write!(f, "memory: {e}"),
            Self::KeptInUse { shard } => write!(f, "shard {shard}'s kept values are borrowed"),
            Self::RingClaimed { shard } => write!(
                f,
                "a ring half shard {shard} must own was already handed out"
            ),
            Self::WorkerFailed { shard } => {
                write!(f, "shard {shard}'s worker ended without a result")
            }
            Self::TimerState { id, armed } => write!(
                f,
                "timer {id} is {}",
                if *armed { "already armed" } else { "not armed" }
            ),
            Self::Capacity { what, bound } => write!(f, "{what} is full at its bound of {bound}"),
            Self::WouldBlock { call } => {
                write!(f, "{call} would block: the OS has no room for it now")
            }
        }
    }
}

impl std::error::Error for RtError {}

impl From<MemError> for RtError {
    fn from(e: MemError) -> Self {
        Self::Mem(e)
    }
}

impl RtError {
    /// Captures the current OS error for a refused driver call.
    pub fn os(call: &'static str) -> Self {
        Self::DriverRefused {
            call,
            code: std::io::Error::last_os_error().raw_os_error(),
        }
    }

    /// The refusal a send too large for its interface gets (`EMSGSIZE`; Winsock's `WSAEMSGSIZE`) — what the
    /// simulation's modelled interface returns, so a caller handles it as it handles the real one.
    pub fn message_too_large(call: &'static str) -> Self {
        Self::DriverRefused {
            call,
            code: Some(MESSAGE_TOO_LARGE),
        }
    }

    /// Whether this is a send the local stack refused as too large for a datagram (`EMSGSIZE`; Winsock's
    /// `WSAEMSGSIZE`): past the interface's MTU with don't-fragment set, or past the host's UDP datagram cap —
    /// the answer a path-MTU probe that is too large for this host gets at once (RFC 8899 §4.4).
    pub fn is_message_too_large(&self) -> bool {
        matches!(self, Self::DriverRefused { code: Some(code), .. } if *code == MESSAGE_TOO_LARGE)
    }
}

/// Format: the OS error code of a datagram too large to send — `EMSGSIZE` on Unix.
#[cfg(unix)]
const MESSAGE_TOO_LARGE: i32 = libc::EMSGSIZE;
/// Format: the OS error code of a datagram too large to send — `WSAEMSGSIZE` on Windows.
#[cfg(windows)]
const MESSAGE_TOO_LARGE: i32 = windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE;
