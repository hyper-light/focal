//! A principal's request generations as its journal keeps them (the audit's
//! F12, [21 §3](../../../docs/archictecutre/21-native-input-format.md)),
//! shared by every worker of a run as N processes of one participant share
//! one journal: the generation fresh requests are issued in, the floor the
//! owner stands at as this run advanced or learned it, and what is in flight
//! in the generation being drained. The next generation opens once as many
//! requests were issued in the current one as the CLI's journal issues
//! before it rotates — half its capacity — while the floor stands at the
//! current one; the floor advances, by the protocol operation the journal
//! sends, once the generation below has drained. A refusal by name — the
//! owner closed the generation under pressure, or does not admit it yet —
//! teaches the run the owner's window, and it issues where the owner admits.
use crate::error::LoadError;
use focal_client::native_store::NativeStoreLimits;
use focal_model::RequestEpoch;
use std::sync::Mutex;

/// Requests issued in one generation before the next opens: what the CLI's
/// journal issues before it rotates, half its capacity.
fn rotation() -> u32 {
    NativeStoreLimits::default()
        .max_operations
        .checked_div(2)
        .unwrap_or(1)
        .max(1)
}

struct State {
    /// The generation fresh requests are issued in.
    epoch: u64,
    /// The owner's floor as this run advanced or learned it.
    floor: u64,
    /// Requests issued in `epoch`.
    issued: u32,
    /// Requests in flight in `epoch`, and in the generation below it.
    filling: u32,
    draining: u32,
    /// A worker is sending the floor advance.
    advancing: bool,
    rotation: u32,
    advances: u64,
    expired: u64,
}

/// The generation a request is minted in.
#[derive(Debug, Clone, Copy)]
pub struct Minted {
    pub epoch: RequestEpoch,
}

/// What follows a request's reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finished {
    Idle,
    /// The generation below the current one has drained and the floor stands
    /// under it: this worker sends the floor advance to `minimum`, in `epoch`.
    Advance {
        epoch: RequestEpoch,
        minimum: RequestEpoch,
    },
}

pub struct Generations {
    state: Mutex<State>,
}

impl Default for Generations {
    fn default() -> Self {
        Self::new()
    }
}

impl Generations {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                epoch: 1,
                floor: 1,
                issued: 0,
                filling: 0,
                draining: 0,
                advancing: false,
                rotation: rotation(),
                advances: 0,
                expired: 0,
            }),
        }
    }
    fn locked(&self) -> Result<std::sync::MutexGuard<'_, State>, LoadError> {
        self.state
            .lock()
            .map_err(|_| LoadError::Worker("the generation lock was poisoned"))
    }
    /// The generation of the next request: the current one, or the next
    /// once the rotation count was issued here, the floor stands here and
    /// the generation below has drained (two are open at most).
    pub fn mint(&self) -> Result<Minted, LoadError> {
        let mut state = self.locked()?;
        if state.issued >= state.rotation && state.floor == state.epoch && state.draining == 0 {
            state.epoch = state
                .epoch
                .checked_add(1)
                .ok_or(LoadError::Bound("request generations"))?;
            state.issued = 0;
            state.draining = state.filling;
            state.filling = 0;
        }
        state.issued = state.issued.saturating_add(1);
        state.filling = state.filling.saturating_add(1);
        Ok(Minted {
            epoch: RequestEpoch(state.epoch),
        })
    }
    /// A request of `epoch` was answered, committed or refused: it is no
    /// longer in flight; when the generation below the current one has
    /// drained and the floor stands under it, the floor advances.
    pub fn finish(&self, epoch: RequestEpoch) -> Result<Finished, LoadError> {
        let mut state = self.locked()?;
        if epoch.0 == state.epoch {
            state.filling = state.filling.saturating_sub(1);
        } else if epoch.0.checked_add(1) == Some(state.epoch) {
            state.draining = state.draining.saturating_sub(1);
        }
        if state.floor < state.epoch && state.draining == 0 && !state.advancing {
            state.advancing = true;
            return Ok(Finished::Advance {
                epoch: RequestEpoch(state.epoch),
                minimum: RequestEpoch(state.epoch),
            });
        }
        Ok(Finished::Idle)
    }
    /// The floor advance to `minimum` committed.
    pub fn advanced(&self, minimum: RequestEpoch) -> Result<(), LoadError> {
        let mut state = self.locked()?;
        state.floor = state.floor.max(minimum.0);
        state.advancing = false;
        state.advances = state.advances.saturating_add(1);
        Ok(())
    }
    /// The floor advance was refused or lost: the next reply may send it.
    pub fn advance_failed(&self) -> Result<(), LoadError> {
        self.locked()?.advancing = false;
        Ok(())
    }
    /// The owner's window as a refusal by name taught it: the floor it
    /// stands at and the generations it holds open; fresh requests go to
    /// the last open one, or open the next while fewer than two are.
    pub fn learn(&self, floor: RequestEpoch, open: &[RequestEpoch]) -> Result<(), LoadError> {
        let mut state = self.locked()?;
        let next = floor
            .0
            .checked_add(
                u64::try_from(open.len()).map_err(|_| LoadError::Bound("open generations"))?,
            )
            .ok_or(LoadError::Bound("request generations"))?;
        state.floor = floor.0.max(state.floor);
        state.epoch = match open.last() {
            Some(last) if open.len() >= 2 => last.0,
            _ => next,
        }
        .max(state.floor);
        state.issued = 0;
        state.filling = 0;
        state.draining = 0;
        state.advancing = false;
        state.expired = state.expired.saturating_add(1);
        Ok(())
    }
    /// Floors this run advanced, and requests the owner refused by name
    /// (re-issued in the generation it admits).
    pub fn counts(&self) -> Result<(u64, u64), LoadError> {
        let state = self.locked()?;
        Ok((state.advances, state.expired))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_rotate_at_the_journals_pace_and_advance_once_the_old_one_drains() {
        let generations = Generations::new();
        let rotation = rotation();
        for _ in 0..rotation {
            let minted = generations.mint().unwrap();
            assert_eq!(minted.epoch, RequestEpoch(1));
            assert_eq!(generations.finish(minted.epoch).unwrap(), Finished::Idle);
        }
        // The rotation count issued and the floor here: the next opens, the
        // one below drained already (every request answered) — the first
        // reply in it sends the floor advance.
        let two = generations.mint().unwrap();
        assert_eq!(two.epoch, RequestEpoch(2));
        assert_eq!(
            generations.finish(two.epoch).unwrap(),
            Finished::Advance {
                epoch: RequestEpoch(2),
                minimum: RequestEpoch(2)
            }
        );
        // While one worker advances, no other does; a failed advance is
        // sent again by the next reply; a committed one moves the floor.
        let more = generations.mint().unwrap();
        assert_eq!(generations.finish(more.epoch).unwrap(), Finished::Idle);
        generations.advance_failed().unwrap();
        let again = generations.mint().unwrap();
        assert!(matches!(
            generations.finish(again.epoch).unwrap(),
            Finished::Advance { .. }
        ));
        generations.advanced(RequestEpoch(2)).unwrap();
        assert_eq!(generations.counts().unwrap(), (1, 0));
        // Three issued in generation two so far; up to the rotation count,
        // then a request still in flight when the next opens: it holds the
        // advance, never the rotation, until it is answered.
        for _ in 0..rotation.saturating_sub(4) {
            let minted = generations.mint().unwrap();
            assert_eq!(minted.epoch, RequestEpoch(2));
            generations.finish(minted.epoch).unwrap();
        }
        let held = generations.mint().unwrap();
        assert_eq!(held.epoch, RequestEpoch(2));
        let three = generations.mint().unwrap();
        assert_eq!(three.epoch, RequestEpoch(3));
        assert_eq!(generations.finish(three.epoch).unwrap(), Finished::Idle);
        assert_eq!(
            generations.finish(held.epoch).unwrap(),
            Finished::Advance {
                epoch: RequestEpoch(3),
                minimum: RequestEpoch(3)
            }
        );
    }

    #[test]
    fn a_refusal_by_name_teaches_the_owners_window() {
        let generations = Generations::new();
        generations
            .learn(RequestEpoch(5), &[RequestEpoch(5)])
            .unwrap();
        // One open under the floor: the next opens.
        assert_eq!(generations.mint().unwrap().epoch, RequestEpoch(6));
        generations
            .learn(RequestEpoch(7), &[RequestEpoch(7), RequestEpoch(8)])
            .unwrap();
        // Two open: the last of them.
        assert_eq!(generations.mint().unwrap().epoch, RequestEpoch(8));
        generations.learn(RequestEpoch(9), &[]).unwrap();
        assert_eq!(generations.mint().unwrap().epoch, RequestEpoch(9));
        assert_eq!(generations.counts().unwrap(), (0, 3));
    }
}
