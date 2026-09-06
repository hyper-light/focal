use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery<M> {
    pub from: u64,
    pub to: u64,
    pub message: M,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NetworkError {
    #[error("the link is partitioned")]
    Partitioned,
    #[error("simulation network capacity exhausted")]
    Capacity,
    #[error("simulation sequence or clock overflow")]
    Overflow,
    #[error("logical time cannot move backwards")]
    BackwardsTime,
}

/// An explicitly scheduled message network. Equal-time delivery is FIFO by send ordinal.
/// Tests can choose any delays, drop a delivery, or retransmit it with a new ordinal.
#[derive(Debug)]
pub struct Network<M> {
    now: u64,
    ordinal: u64,
    bytes: usize,
    max_bytes: usize,
    max_messages: usize,
    queue: BTreeMap<(u64, u64), (usize, Delivery<M>)>,
    blocked: BTreeSet<(u64, u64)>,
}

impl<M> Network<M> {
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            now: 0,
            ordinal: 0,
            bytes: 0,
            max_bytes,
            max_messages,
            queue: BTreeMap::new(),
            blocked: BTreeSet::new(),
        }
    }

    pub fn partition(&mut self, from: u64, to: u64, blocked: bool) {
        if blocked {
            self.blocked.insert((from, to));
        } else {
            self.blocked.remove(&(from, to));
        }
    }

    pub fn send(
        &mut self,
        delivery: Delivery<M>,
        encoded_bytes: usize,
        delay: u64,
    ) -> Result<(), NetworkError> {
        if self.blocked.contains(&(delivery.from, delivery.to)) {
            return Err(NetworkError::Partitioned);
        }
        let bytes = self
            .bytes
            .checked_add(encoded_bytes)
            .ok_or(NetworkError::Overflow)?;
        if bytes > self.max_bytes || self.queue.len() >= self.max_messages {
            return Err(NetworkError::Capacity);
        }
        let at = self.now.checked_add(delay).ok_or(NetworkError::Overflow)?;
        let next = self.ordinal.checked_add(1).ok_or(NetworkError::Overflow)?;
        self.queue
            .insert((at, self.ordinal), (encoded_bytes, delivery));
        self.ordinal = next;
        self.bytes = bytes;
        Ok(())
    }

    pub fn advance_to(&mut self, now: u64) -> Result<(), NetworkError> {
        if now < self.now {
            return Err(NetworkError::BackwardsTime);
        }
        self.now = now;
        Ok(())
    }

    /// Removes one due message. A link partition at delivery time drops the frame.
    /// A caller must treat `None` as no deliverable frame now, not as session completion.
    pub fn receive(&mut self) -> Option<Delivery<M>> {
        while let Some((&key, _)) = self.queue.first_key_value() {
            if key.0 > self.now {
                return None;
            }
            let (_, (charge, delivery)) = self.queue.pop_first()?;
            self.bytes = self.bytes.checked_sub(charge)?;
            if !self.blocked.contains(&(delivery.from, delivery.to)) {
                return Some(delivery);
            }
        }
        None
    }

    pub fn pending(&self) -> (usize, usize) {
        (self.queue.len(), self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_reordering_and_capacity_have_explicit_semantics() {
        let mut n = Network::new(2, 4);
        n.send(
            Delivery {
                from: 1,
                to: 2,
                message: "later",
            },
            2,
            5,
        )
        .unwrap();
        n.send(
            Delivery {
                from: 2,
                to: 1,
                message: "first",
            },
            2,
            1,
        )
        .unwrap();
        assert_eq!(
            n.send(
                Delivery {
                    from: 1,
                    to: 2,
                    message: "overflow"
                },
                1,
                0
            ),
            Err(NetworkError::Capacity)
        );
        n.advance_to(1).unwrap();
        assert_eq!(n.receive().unwrap().message, "first");
        n.partition(1, 2, true);
        n.advance_to(5).unwrap();
        assert_eq!(n.receive(), None);
        assert_eq!(n.pending(), (0, 0));
        assert_eq!(n.advance_to(4), Err(NetworkError::BackwardsTime));
    }
}
