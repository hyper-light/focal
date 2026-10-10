use crate::Instant;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum Timer {
    /// When to send an ack-eliciting probe packet or declare unacked packets lost
    LossDetection = 0,
    /// When to close the connection after no activity
    Idle = 1,
    /// When the close timer expires, the connection has been gracefully terminated.
    Close = 2,
    /// When keys are discarded because they should not be needed anymore
    KeyDiscard = 3,
    /// When to give up on validating a new path to the peer
    PathValidation = 4,
    /// When to send a `PING` frame to keep the connection alive
    KeepAlive = 5,
    /// When pacing will allow us to send a packet
    Pacing = 6,
    /// When to invalidate old CID and proactively push new one via NEW_CONNECTION_ID frame
    PushNewCid = 7,
    /// When to send an immediate ACK if there are unacked ack-eliciting packets of the peer
    MaxAckDelay = 8,
    /// When a copy of the handshake's flights is due behind its original
    Copies = 9,
}

impl Timer {
    /// Every timer
    pub(crate) const VALUES: [Self; 10] = [
        Self::LossDetection,
        Self::Idle,
        Self::Close,
        Self::KeyDiscard,
        Self::PathValidation,
        Self::KeepAlive,
        Self::Pacing,
        Self::PushNewCid,
        Self::MaxAckDelay,
        Self::Copies,
    ];
}

/// A table of data associated with each distinct kind of `Timer`
#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct TimerTable {
    data: [Option<Instant>; 10],
}

impl TimerTable {
    pub(super) fn set(&mut self, timer: Timer, time: Instant) {
        *self.slot_mut(timer) = Some(time);
    }

    pub(super) fn get(&self, timer: Timer) -> Option<Instant> {
        let [a, b, c, d, e, f, g, h, i, j] = &self.data;
        *match timer {
            Timer::LossDetection => a,
            Timer::Idle => b,
            Timer::Close => c,
            Timer::KeyDiscard => d,
            Timer::PathValidation => e,
            Timer::KeepAlive => f,
            Timer::Pacing => g,
            Timer::PushNewCid => h,
            Timer::MaxAckDelay => i,
            Timer::Copies => j,
        }
    }

    pub(super) fn stop(&mut self, timer: Timer) {
        *self.slot_mut(timer) = None;
    }

    /// Each timer's slot, found by destructuring so that no lookup can miss
    fn slot_mut(&mut self, timer: Timer) -> &mut Option<Instant> {
        let [a, b, c, d, e, f, g, h, i, j] = &mut self.data;
        match timer {
            Timer::LossDetection => a,
            Timer::Idle => b,
            Timer::Close => c,
            Timer::KeyDiscard => d,
            Timer::PathValidation => e,
            Timer::KeepAlive => f,
            Timer::Pacing => g,
            Timer::PushNewCid => h,
            Timer::MaxAckDelay => i,
            Timer::Copies => j,
        }
    }

    pub(super) fn next_timeout(&self) -> Option<Instant> {
        self.data.iter().filter_map(|&x| x).min()
    }

    pub(super) fn is_expired(&self, timer: Timer, after: Instant) -> bool {
        self.get(timer).is_some_and(|x| x <= after)
    }
}
