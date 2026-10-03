//! The heartbeat's wire form: one plane message (`hyper-datagram`), little-endian, fixed fields.
//!
//! ```text
//! kind (1) version (1) run (8) seq (8) interval (8) floor (8) ask (8) sent (8) late (8)
//! flushes (8) flush_age (8) echo flag (1) [echo sent (8) echo late (8) echo hold (8)]
//! ```
//!
//! The plane seals, authenticates and checksums every datagram (CRC-32C under AES-256-GCM), so the
//! message carries no checksum of its own. Times are nanoseconds on the sender's clock, never
//! compared with the receiver's except through the echo, whose clock offsets cancel
//! (RFC 5905 §8; the receiver-report fields LSR and DLSR of RFC 3550 §6.4.1 are the same design).

use crate::Refusal;

/// Format: the first byte of a liveness message, so an owner multiplexing the plane among Raft
/// control, SWIM and liveness tells this one apart (`'L'`).
pub const KIND: u8 = 0x4c;
/// Format: the wire version this build reads and writes. Version 2 orders runs: its `run` is a
/// count the sender raises at every start, where version 1's `boot` was any value its node never
/// reused, which a receiver could not order; a version 1 heartbeat is refused, not misread.
pub const VERSION: u8 = 2;
/// The bytes before the echo: kind, version, nine eight-byte fields and the echo flag.
const HEAD_BYTES: usize = 2 + 9 * 8 + 1;
/// The echo's three eight-byte fields.
const ECHO_BYTES: usize = 3 * 8;
/// The longest message: a heartbeat with its echo.
pub const MAX_BYTES: usize = HEAD_BYTES + ECHO_BYTES;

/// The latest heartbeat the sender received from the receiver, sent back so the receiver can
/// measure the round trip on its own clock: NTP's originate timestamp and RFC 3550's last-report
/// delay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Echo {
    /// When the receiver sent the echoed heartbeat, on the receiver's clock (its `sent`).
    pub sent_ns: u64,
    /// How late past its schedule the receiver sent it (its `late`).
    pub late_ns: u64,
    /// From that heartbeat's arrival at the sender to this heartbeat's send, on the sender's clock.
    pub hold_ns: u64,
}

/// One heartbeat of a node pair's stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Heartbeat {
    /// The sender's run: a count its node keeps durably and raises at every start
    /// (`Settings::run`), so a later run's is greater. A later one than the receiver last took
    /// starts the stream again; an earlier one is a superseded run's.
    pub run: u64,
    /// The heartbeat's number in the run, from zero.
    pub seq: u64,
    /// The interval `η` the sender schedules at: heartbeat `seq` was due `η` after `seq − 1`.
    pub interval_ns: u64,
    /// The sender's stability floor `E[flush] + G` (`docs/timing.md` §2.6): it cannot keep a
    /// shorter interval, so the receiver configures no shorter one. Zero before it is measured.
    pub floor_ns: u64,
    /// The interval the sender asks the receiver to send at: its configurator's best for the
    /// receiver's stream to it. Zero while it asks nothing.
    pub ask_ns: u64,
    /// When it was sent, on the sender's clock.
    pub sent_ns: u64,
    /// How late past its schedule: `sent − σ`, the sender's wake and flush.
    pub late_ns: u64,
    /// The proof of a durable flush: the log writes the sender has seen made durable. Each
    /// heartbeat of a run counts more than the one before it.
    pub flushes: u64,
    /// From the latest of them becoming durable to the send: at most `late + interval`, so the
    /// flush came after the previous heartbeat was due (`docs/durable.md` §8).
    pub flush_age_ns: u64,
    /// The latest heartbeat the sender had from the receiver, if any.
    pub echo: Option<Echo>,
}

fn put(out: &mut [u8; MAX_BYTES], at: &mut usize, value: u64) {
    if let Some(slot) = at.checked_add(8).and_then(|end| out.get_mut(*at..end)) {
        slot.copy_from_slice(&value.to_le_bytes());
    }
    *at = at.saturating_add(8);
}

fn take(bytes: &mut &[u8]) -> Result<u64, Refusal> {
    let (word, rest) = bytes.split_first_chunk::<8>().ok_or(Refusal::Truncated)?;
    *bytes = rest;
    Ok(u64::from_le_bytes(*word))
}

impl Heartbeat {
    /// Writes the heartbeat into `out` and returns the bytes written, a prefix of it.
    pub fn encode<'a>(&self, out: &'a mut [u8; MAX_BYTES]) -> &'a [u8] {
        let mut at = 2usize;
        if let Some(head) = out.first_chunk_mut::<2>() {
            *head = [KIND, VERSION];
        }
        for value in [
            self.run,
            self.seq,
            self.interval_ns,
            self.floor_ns,
            self.ask_ns,
            self.sent_ns,
            self.late_ns,
            self.flushes,
            self.flush_age_ns,
        ] {
            put(out, &mut at, value);
        }
        if let Some(flag) = out.get_mut(at) {
            *flag = u8::from(self.echo.is_some());
        }
        at = at.saturating_add(1);
        if let Some(echo) = self.echo {
            for value in [echo.sent_ns, echo.late_ns, echo.hold_ns] {
                put(out, &mut at, value);
            }
        }
        out.get(..at).unwrap_or(&[])
    }

    /// Reads a heartbeat; a message of another kind, another version or the wrong length is
    /// refused.
    pub fn decode(bytes: &[u8]) -> Result<Self, Refusal> {
        let (header, mut rest) = bytes.split_first_chunk::<2>().ok_or(Refusal::Truncated)?;
        let [kind, version] = *header;
        if kind != KIND {
            return Err(Refusal::NotLiveness);
        }
        if version != VERSION {
            return Err(Refusal::BadVersion);
        }
        let mut fields = [0u64; 9];
        for field in &mut fields {
            *field = take(&mut rest)?;
        }
        let [
            run,
            seq,
            interval_ns,
            floor_ns,
            ask_ns,
            sent_ns,
            late_ns,
            flushes,
            flush_age_ns,
        ] = fields;
        let (flag, mut rest) = rest.split_first().ok_or(Refusal::Truncated)?;
        let echo = match flag {
            0 => None,
            1 => Some(Echo {
                sent_ns: take(&mut rest)?,
                late_ns: take(&mut rest)?,
                hold_ns: take(&mut rest)?,
            }),
            _ => return Err(Refusal::Malformed),
        };
        if !rest.is_empty() {
            return Err(Refusal::Malformed);
        }
        Ok(Self {
            run,
            seq,
            interval_ns,
            floor_ns,
            ask_ns,
            sent_ns,
            late_ns,
            flushes,
            flush_age_ns,
            echo,
        })
    }
}

/// Whether `message`, taken from the plane, is a liveness message.
pub fn is_liveness(message: &[u8]) -> bool {
    message.first() == Some(&KIND)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn heartbeat() -> impl Strategy<Value = Heartbeat> {
        (
            prop::array::uniform9(any::<u64>()),
            prop::option::of(prop::array::uniform3(any::<u64>())),
        )
            .prop_map(|(f, echo)| Heartbeat {
                run: f[0],
                seq: f[1],
                interval_ns: f[2],
                floor_ns: f[3],
                ask_ns: f[4],
                sent_ns: f[5],
                late_ns: f[6],
                flushes: f[7],
                flush_age_ns: f[8],
                echo: echo.map(|e| Echo {
                    sent_ns: e[0],
                    late_ns: e[1],
                    hold_ns: e[2],
                }),
            })
    }

    proptest! {
        /// Every heartbeat reads back as written, at the length its echo gives it.
        #[test]
        fn a_heartbeat_reads_back_as_written(beat in heartbeat()) {
            let mut out = [0u8; MAX_BYTES];
            let bytes = beat.encode(&mut out).to_vec();
            prop_assert_eq!(bytes.len(), if beat.echo.is_some() { MAX_BYTES } else { HEAD_BYTES });
            prop_assert!(is_liveness(&bytes));
            prop_assert_eq!(Heartbeat::decode(&bytes), Ok(beat));
        }

        /// No prefix, extension or other kind of a heartbeat is read as one.
        #[test]
        fn a_damaged_heartbeat_is_refused(beat in heartbeat(), cut in 0usize..MAX_BYTES, extra in any::<u8>()) {
            let mut out = [0u8; MAX_BYTES];
            let bytes = beat.encode(&mut out).to_vec();
            if cut < bytes.len() {
                prop_assert!(Heartbeat::decode(&bytes[..cut]).is_err());
            }
            let mut longer = bytes.clone();
            longer.push(extra);
            prop_assert_eq!(Heartbeat::decode(&longer), Err(Refusal::Malformed));
            let mut other = bytes.clone();
            other[0] = KIND.wrapping_add(1);
            prop_assert_eq!(Heartbeat::decode(&other), Err(Refusal::NotLiveness));
            let mut version = bytes;
            version[1] = VERSION.wrapping_add(1);
            prop_assert_eq!(Heartbeat::decode(&version), Err(Refusal::BadVersion));
        }
    }

    #[test]
    fn an_echo_flag_other_than_zero_or_one_is_malformed() {
        let beat = Heartbeat {
            run: 1,
            seq: 2,
            interval_ns: 3,
            floor_ns: 4,
            ask_ns: 5,
            sent_ns: 6,
            late_ns: 7,
            flushes: 8,
            flush_age_ns: 9,
            echo: None,
        };
        let mut out = [0u8; MAX_BYTES];
        let mut bytes = beat.encode(&mut out).to_vec();
        *bytes.last_mut().unwrap() = 2;
        assert_eq!(Heartbeat::decode(&bytes), Err(Refusal::Malformed));
    }
}
