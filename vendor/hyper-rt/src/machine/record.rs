//! The calibration record (docs/runtime.md §10.2): what [`Calibration::measure`] measured — the wake and
//! the null system call, the probes that cost their wall budget — in a fixed, versioned, checksummed
//! record the consumer stores (focal in its data directory), so a short command reuses it instead of paying
//! the probes. The facts are queried afresh every time (they cost no budget) and name the record's key: the
//! machine's identity line (CPU model, OS build, architecture, cores, memory, page) and its power source. A
//! record is reused while its key matches the machine now and its age is under the consumer's bound;
//! otherwise it is measured again and a new record handed back. A power-source change changes the key, so
//! it re-measures.
//!
//! Layout, little-endian: magic (8) | version (u16) | key length (u16) | key | measured at, Unix seconds (u64)
//! | wake: mean, mean lower, mean upper, p50, p99, sd (u64 × 6), samples, same-CPU samples, rounds (u32 × 3),
//! rounds agree (u8: 0 no, 1 yes, 2 unknown), placement (u8), asleep confirmed (u8), quick (u8) | system
//! call: median, lower, upper, p99, min (u64 × 5), samples, batch (u32 × 2), quick (u8) | CRC-32C of
//! everything before it (u32).
//!
//! Time is an input: the consumer passes the wall clock's Unix seconds (the workspace's wall keeps
//! `SystemTime::now` out of library code).

use std::time::Duration;

use crate::machine::bench::Measurement;
use crate::machine::calibration::Calibration;
use crate::machine::error::MachineError;
use crate::machine::facts::{Facts, PowerState};
use crate::machine::probes::Pinning;
use crate::machine::stats::Interval;
use crate::machine::wake::WakeLatency;

/// Format: the record's magic.
const MAGIC: [u8; 8] = *b"HRTCAL\0\x01";
/// Format: the layout's version; a record of another is measured again.
const VERSION: u16 = 1;
/// Format: the longest key a record carries (the identity line is well under it; a longer one is refused,
/// so a record never grows with a hostile CPU string).
pub const MAX_KEY: usize = 1_024;

/// Why a stored record was not used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stale {
    /// It does not decode: truncated, a wrong magic or checksum, a field out of range.
    Corrupt,
    /// Another layout version.
    Version,
    /// Another machine, or the same one on another power source.
    Key,
    /// Older than the consumer's bound (or from the future, a clock that stepped back).
    Age,
}

/// The record's key for `facts`.
fn key(facts: &Facts) -> String {
    let power = match facts.power {
        PowerState::Mains => "mains",
        PowerState::Battery => "battery",
        PowerState::Unknown => "unknown",
    };
    format!("{} | {power}", facts.identity.line())
}

fn pinning_byte(pinning: Pinning) -> u8 {
    match pinning {
        Pinning::Pinned => 0,
        Pinning::Hint => 1,
        Pinning::Scheduled => 2,
        Pinning::Refused => 3,
    }
}

fn pinning_of(byte: u8) -> Option<Pinning> {
    match byte {
        0 => Some(Pinning::Pinned),
        1 => Some(Pinning::Hint),
        2 => Some(Pinning::Scheduled),
        3 => Some(Pinning::Refused),
        _ => None,
    }
}

/// A reader over a record that refuses to read past its end.
struct Cursor<'a> {
    rest: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.rest.split_at_checked(n)?;
        self.rest = rest;
        Some(head)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[byte]| byte)
    }

    fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(u64::from_le_bytes)
    }

    fn flag(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
}

/// The wake measurement's fields.
fn read_wake(at: &mut Cursor<'_>) -> Option<WakeLatency> {
    Some(WakeLatency {
        mean_ns: at.u64()?,
        mean_lower_ns: at.u64()?,
        mean_upper_ns: at.u64()?,
        p50_ns: at.u64()?,
        p99_ns: at.u64()?,
        sd_ns: at.u64()?,
        samples: at.u32()?,
        same_cpu_samples: at.u32()?,
        rounds: at.u32()?,
        rounds_agree: match at.u8()? {
            0 => Some(false),
            1 => Some(true),
            2 => None,
            _ => return None,
        },
        placement: pinning_of(at.u8()?)?,
        asleep_confirmed: at.flag()?,
        quick: at.flag()?,
    })
}

/// The system call's measurement's fields.
fn read_syscall(at: &mut Cursor<'_>) -> Option<Measurement> {
    Some(Measurement {
        interval: Interval {
            median: at.u64()?,
            lower: at.u64()?,
            upper: at.u64()?,
        },
        p99_ns: at.u64()?,
        min_ns: at.u64()?,
        samples: at.u32()?,
        batch: at.u32()?,
        quick: at.flag()?,
    })
}

impl Calibration {
    /// The record of this calibration, measured at `measured_at_unix_s`. `None` when the machine's key is
    /// longer than [`MAX_KEY`].
    pub fn encode(&self, measured_at_unix_s: u64) -> Option<Vec<u8>> {
        let key = key(&self.facts);
        if key.len() > MAX_KEY {
            return None;
        }
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&u16::try_from(key.len()).ok()?.to_le_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&measured_at_unix_s.to_le_bytes());
        let wake = &self.wake;
        for value in [
            wake.mean_ns,
            wake.mean_lower_ns,
            wake.mean_upper_ns,
            wake.p50_ns,
            wake.p99_ns,
            wake.sd_ns,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        for value in [wake.samples, wake.same_cpu_samples, wake.rounds] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.push(match wake.rounds_agree {
            Some(false) => 0,
            Some(true) => 1,
            None => 2,
        });
        out.push(pinning_byte(wake.placement));
        out.push(u8::from(wake.asleep_confirmed));
        out.push(u8::from(wake.quick));
        let syscall = &self.syscall;
        for value in [
            syscall.interval.median,
            syscall.interval.lower,
            syscall.interval.upper,
            syscall.p99_ns,
            syscall.min_ns,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&syscall.samples.to_le_bytes());
        out.extend_from_slice(&syscall.batch.to_le_bytes());
        out.push(u8::from(syscall.quick));
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Some(out)
    }

    /// The calibration a stored `record` holds, with the machine's `facts` now, when its key matches them
    /// and it is no older than `max_age_s` at `now_unix_s`.
    pub fn decode(
        record: &[u8],
        facts: Facts,
        now_unix_s: u64,
        max_age_s: u64,
    ) -> Result<Calibration, Stale> {
        let (body, crc) = record.split_last_chunk::<4>().ok_or(Stale::Corrupt)?;
        if crc32c::crc32c(body) != u32::from_le_bytes(*crc) {
            return Err(Stale::Corrupt);
        }
        let mut at = Cursor { rest: body };
        if at.array::<8>().ok_or(Stale::Corrupt)? != MAGIC {
            return Err(Stale::Corrupt);
        }
        if at.u16().ok_or(Stale::Corrupt)? != VERSION {
            return Err(Stale::Version);
        }
        let key_len = usize::from(at.u16().ok_or(Stale::Corrupt)?);
        if key_len > MAX_KEY {
            return Err(Stale::Corrupt);
        }
        let stored_key = at.take(key_len).ok_or(Stale::Corrupt)?;
        if stored_key != key(&facts).as_bytes() {
            return Err(Stale::Key);
        }
        let measured_at = at.u64().ok_or(Stale::Corrupt)?;
        let age = now_unix_s.checked_sub(measured_at).ok_or(Stale::Age)?;
        if age > max_age_s {
            return Err(Stale::Age);
        }
        let wake = read_wake(&mut at).ok_or(Stale::Corrupt)?;
        let syscall = read_syscall(&mut at).ok_or(Stale::Corrupt)?;
        if !at.rest.is_empty() {
            return Err(Stale::Corrupt);
        }
        Ok(Calibration {
            facts,
            syscall,
            wake,
        })
    }

    /// The stored `record`'s calibration when it is still good for this machine, else a fresh measurement
    /// (each probe within `budget`) and the record to store in place of the old one.
    pub fn load_or_measure(
        record: Option<&[u8]>,
        now_unix_s: u64,
        max_age_s: u64,
        budget: Duration,
        reserved_cores: u16,
    ) -> Result<(Calibration, Option<Vec<u8>>), MachineError> {
        if let Some(record) = record
            && let Ok(calibration) =
                Calibration::decode(record, Facts::query(), now_unix_s, max_age_s)
        {
            return Ok((calibration, None));
        }
        let calibration = Calibration::measure(budget, reserved_cores)?;
        let fresh = calibration.encode(now_unix_s);
        Ok((calibration, fresh))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured() -> Calibration {
        Calibration::measure(Duration::from_millis(40), 0).expect("the machine calibrates")
    }

    /// Do: encode a measurement, decode it with the same facts. Expect: the same calibration.
    #[test]
    fn a_record_round_trips() {
        let calibration = measured();
        let record = calibration.encode(1_000).unwrap();
        let back = Calibration::decode(&record, calibration.facts.clone(), 1_500, 3_600).unwrap();
        assert_eq!(back, calibration);
    }

    /// Do: a record past its age, from another machine, on another power source, from a clock that
    /// stepped back, of another version, and every one-byte corruption and truncation. Expect: each refused
    /// with its reason, none decoded, none panicking.
    #[test]
    fn a_stale_or_damaged_record_is_measured_again() {
        let calibration = measured();
        let facts = calibration.facts.clone();
        let record = calibration.encode(1_000).unwrap();
        assert_eq!(
            Calibration::decode(&record, facts.clone(), 5_000, 3_600),
            Err(Stale::Age)
        );
        assert_eq!(
            Calibration::decode(&record, facts.clone(), 999, 3_600),
            Err(Stale::Age)
        );
        let mut other = facts.clone();
        other.identity.cores += 1;
        assert_eq!(
            Calibration::decode(&record, other, 1_000, 3_600),
            Err(Stale::Key)
        );
        let mut unplugged = facts.clone();
        unplugged.power = match facts.power {
            PowerState::Battery => PowerState::Mains,
            _ => PowerState::Battery,
        };
        assert_eq!(
            Calibration::decode(&record, unplugged, 1_000, 3_600),
            Err(Stale::Key)
        );
        let mut versioned = record.clone();
        versioned[8] = 2;
        let body = versioned.len() - 4;
        let crc = crc32c::crc32c(&versioned[..body]);
        versioned[body..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            Calibration::decode(&versioned, facts.clone(), 1_000, 3_600),
            Err(Stale::Version)
        );
        for at in 0..record.len() {
            let mut flipped = record.clone();
            flipped[at] ^= 0x40;
            assert!(Calibration::decode(&flipped, facts.clone(), 1_000, 3_600).is_err());
            assert!(Calibration::decode(&record[..at], facts.clone(), 1_000, 3_600).is_err());
        }
    }

    /// Do: load with a good record, then with none. Expect: the record is reused (no new one handed back),
    /// and without one the machine is measured and a record handed back.
    #[test]
    fn a_good_record_spares_the_probes() {
        let calibration = measured();
        let record = calibration.encode(1_000).unwrap();
        let (reused, fresh) =
            Calibration::load_or_measure(Some(&record), 1_000, 3_600, Duration::from_millis(40), 0)
                .unwrap();
        assert!(fresh.is_none(), "the stored record served");
        assert_eq!(reused.wake, calibration.wake);
        let (_, fresh) =
            Calibration::load_or_measure(None, 1_000, 3_600, Duration::from_millis(40), 0).unwrap();
        assert!(fresh.is_some(), "measured, and a record to store");
    }
}
