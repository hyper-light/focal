//! The closed refusal taxonomy of the machine profile (§4.1): `ProfileUnavailable`,
//! `ProfileStale`, `MeasurementTimeout`, plus the OS refusing a query. An uncategorized refusal
//! is a bug, so there is no catch-all variant.

use std::fmt;

/// A typed refusal from the machine profile; never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineError {
    /// The cached profile segment cannot be read (absent, wrong magic, wrong version, torn).
    ProfileUnavailable {
        /// Why, in plain English.
        reason: String,
    },
    /// The cached profile was measured on different hardware or OS build than this one.
    ProfileStale {
        /// The identity the cache was measured under.
        cached: String,
        /// The identity of this machine now.
        current: String,
    },
    /// A microbenchmark hit its wall-time bound before its interval converged.
    MeasurementTimeout {
        /// The probe that timed out.
        probe: &'static str,
    },
    /// The operating system refused a query or a probe; the profile records the degraded value.
    OsRefused {
        /// The call that was refused.
        call: &'static str,
        /// The OS error code, when one exists.
        code: Option<i32>,
    },
}

impl fmt::Display for MachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProfileUnavailable { reason } => write!(f, "profile unavailable: {reason}"),
            Self::ProfileStale { cached, current } => {
                write!(
                    f,
                    "profile stale: measured on `{cached}`, this machine is `{current}`"
                )
            }
            Self::MeasurementTimeout { probe } => write!(f, "measurement timeout: {probe}"),
            Self::OsRefused { call, code } => match code {
                Some(code) => write!(f, "the OS refused {call} (code {code})"),
                None => write!(f, "the OS refused {call}"),
            },
        }
    }
}

impl std::error::Error for MachineError {}

impl MachineError {
    /// Captures the current OS error for a refused call.
    pub fn os(call: &'static str) -> Self {
        Self::OsRefused {
            call,
            code: std::io::Error::last_os_error().raw_os_error(),
        }
    }
}
