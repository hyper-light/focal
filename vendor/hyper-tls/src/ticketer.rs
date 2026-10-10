use alloc::boxed::Box;
use alloc::vec::Vec;
use core::mem;

use pki_types::UnixTime;

use crate::server::ProducesTickets;
use crate::{rand, Error};

#[derive(Debug)]
pub(crate) struct TicketSwitcherState {
    next: Option<Box<dyn ProducesTickets>>,
    current: Box<dyn ProducesTickets>,
    previous: Option<Box<dyn ProducesTickets>>,
    next_switch_time: u64,
}

/// A ticketer that has a 'current' sub-ticketer and a single
/// 'previous' ticketer.  It creates a new ticketer every so
/// often, demoting the current ticketer.
#[derive(Debug)]
pub struct TicketSwitcher {
    pub(crate) generator: fn() -> Result<Box<dyn ProducesTickets>, rand::GetRandomFailed>,
    lifetime: u32,
    state: TicketSwitcherState,
}

impl TicketSwitcher {
    /// Creates a new `TicketSwitcher`, which rotates through sub-ticketers
    /// based on the passage of time.
    ///
    /// `lifetime` is in seconds, and is how long the current ticketer
    /// is used to generate new tickets.  Tickets are accepted for no
    /// longer than twice this duration.  `generator` produces a new
    /// `ProducesTickets` implementation.
    #[deprecated(note = "use TicketRotator instead")]
    pub fn new(
        lifetime: u32,
        generator: fn() -> Result<Box<dyn ProducesTickets>, rand::GetRandomFailed>,
    ) -> Result<Self, Error> {
        Ok(Self {
            generator,
            lifetime,
            state: TicketSwitcherState {
                next: Some(generator()?),
                current: generator()?,
                previous: None,
                next_switch_time: UnixTime::now()
                    .as_secs()
                    .saturating_add(u64::from(lifetime)),
            },
        })
    }

    /// If it's time, demote the `current` ticketer to `previous` (so it
    /// does no new encryptions but can do decryption) and use next for a
    /// new `current` ticketer.
    ///
    /// Calling this regularly will ensure timely key erasure.  Otherwise,
    /// key erasure will be delayed until the next encrypt/decrypt call.
    ///
    /// The ticketer is owned by its configuration and reached through `&mut`, so the switch
    /// needs no lock. If `next` is missing because an earlier generation failed, both a new
    /// `next` and a new `current` are generated, and the time is checked again before switching.
    pub(crate) fn maybe_roll(&mut self, now: UnixTime) -> Option<&mut TicketSwitcherState> {
        let now = now.as_secs();
        let generator = self.generator;
        let lifetime = self.lifetime;
        let state = &mut self.state;

        // Fast path in case we do not need to switch to the next ticketer yet
        if now <= state.next_switch_time {
            return Some(state);
        }

        // Make the switch, or mark for recovery if not possible
        let are_recovering = match state.next.take() {
            Some(next) => {
                state.previous = Some(mem::replace(&mut state.current, next));
                state.next_switch_time = now.saturating_add(u64::from(lifetime));
                false
            }
            None => true,
        };

        // We always need a next, so generate it now
        let next = generator().ok()?;
        if !are_recovering {
            state.next = Some(next);
            return Some(state);
        }

        // Recovering: also generate a new current ticketer, and redo the time check, otherwise
        // this might result in very rapid switching of ticketers.
        let new_current = generator().ok()?;
        state.next = Some(next);
        if now > state.next_switch_time {
            state.previous = Some(mem::replace(&mut state.current, new_current));
            state.next_switch_time = now.saturating_add(u64::from(lifetime));
        }
        Some(state)
    }
}

impl ProducesTickets for TicketSwitcher {
    fn lifetime(&self) -> u32 {
        // Two generations of keys are live; a lifetime hint past u32::MAX saturates.
        self.lifetime.saturating_mul(2)
    }

    fn enabled(&self) -> bool {
        true
    }

    fn encrypt(&mut self, message: &[u8]) -> Option<Vec<u8>> {
        let now = UnixTime::now();

        self.maybe_roll(now)?.current.encrypt(message)
    }

    fn decrypt(&mut self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        let now = UnixTime::now();

        let state = self.maybe_roll(now)?;

        // Decrypt with the current key; if that fails, try with the previous.
        state.current.decrypt(ciphertext).or_else(|| {
            state
                .previous
                .as_mut()
                .and_then(|previous| previous.decrypt(ciphertext))
        })
    }
}

#[derive(Debug)]
pub(crate) struct TicketRotatorState {
    current: Box<dyn ProducesTickets>,
    previous: Option<Box<dyn ProducesTickets>>,
    next_switch_time: u64,
}

/// A ticketer that has a 'current' sub-ticketer and a single
/// 'previous' ticketer.  It creates a new ticketer every so
/// often, demoting the current ticketer.
pub struct TicketRotator {
    pub(crate) generator: fn() -> Result<Box<dyn ProducesTickets>, rand::GetRandomFailed>,
    lifetime: u32,
    state: TicketRotatorState,
}

impl TicketRotator {
    /// Creates a new `TicketRotator`, which rotates through sub-ticketers
    /// based on the passage of time.
    ///
    /// `lifetime` is in seconds, and is how long the current ticketer
    /// is used to generate new tickets.  Tickets are accepted for no
    /// longer than twice this duration.  `generator` produces a new
    /// `ProducesTickets` implementation.
    pub fn new(
        lifetime: u32,
        generator: fn() -> Result<Box<dyn ProducesTickets>, rand::GetRandomFailed>,
    ) -> Result<Self, Error> {
        Ok(Self {
            generator,
            lifetime,
            state: TicketRotatorState {
                current: generator()?,
                previous: None,
                next_switch_time: UnixTime::now()
                    .as_secs()
                    .saturating_add(u64::from(lifetime)),
            },
        })
    }

    /// If it's time, demote the `current` ticketer to `previous` (so it
    /// does no new encryptions but can do decryption) and replace it
    /// with a new one.
    ///
    /// Calling this regularly will ensure timely key erasure.  Otherwise,
    /// key erasure will be delayed until the next encrypt/decrypt call.
    ///
    /// The ticketer is owned by its configuration and reached through `&mut`, so the rotation
    /// needs no lock.
    pub(crate) fn maybe_roll(&mut self, now: UnixTime) -> Option<&mut TicketRotatorState> {
        let now = now.as_secs();

        // Fast, common path in case we do not need to switch to the next ticketer yet
        if now <= self.state.next_switch_time {
            return Some(&mut self.state);
        }

        // We need to switch ticketers, and make a new one.
        let next = (self.generator)().ok()?;
        self.state.previous = Some(mem::replace(&mut self.state.current, next));
        self.state.next_switch_time = now.saturating_add(u64::from(self.lifetime));
        Some(&mut self.state)
    }
}

impl ProducesTickets for TicketRotator {
    fn lifetime(&self) -> u32 {
        // Two generations of keys are live; a lifetime hint past u32::MAX saturates.
        self.lifetime.saturating_mul(2)
    }

    fn enabled(&self) -> bool {
        true
    }

    fn encrypt(&mut self, message: &[u8]) -> Option<Vec<u8>> {
        self.maybe_roll(UnixTime::now())?.current.encrypt(message)
    }

    fn decrypt(&mut self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        let state = self.maybe_roll(UnixTime::now())?;

        // Decrypt with the current key; if that fails, try with the previous.
        state.current.decrypt(ciphertext).or_else(|| {
            state
                .previous
                .as_mut()
                .and_then(|previous| previous.decrypt(ciphertext))
        })
    }
}

impl core::fmt::Debug for TicketRotator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TicketRotator").finish_non_exhaustive()
    }
}
