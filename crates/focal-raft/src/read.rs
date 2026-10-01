//! Reads that wait for a quorum to confirm the leader (Ongaro's thesis
//! §6.4): the leader notes its commit, asks a quorum whether it still
//! leads, and answers with that commit. No clock is trusted.
use std::collections::VecDeque;

use crate::{
    NodeId,
    error::{Error, Result},
};

/// A read whose index may be served once it is applied.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadState {
    pub index: u64,
    pub request_ctx: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRead {
    /// What the asker calls the read.
    pub context: Vec<u8>,
    /// Who asked, in the order they asked: zero or the leader itself for a
    /// read asked here, and every other member that asked under the same
    /// context while it waited. A read asked twice is one read, and each
    /// asker is answered.
    origins: Vec<NodeId>,
    /// The leader's commit when it was asked.
    pub index: u64,
    /// Who confirmed the leader since, in order.
    acks: Vec<NodeId>,
}
impl PendingRead {
    pub fn acks(&self) -> &[NodeId] {
        &self.acks
    }
    pub fn origins(&self) -> &[NodeId] {
        &self.origins
    }
    /// The askers, the index and the context, to answer each asker.
    pub fn into_parts(self) -> (Vec<NodeId>, u64, Vec<u8>) {
        (self.origins, self.index, self.context)
    }
}

/// Reads in the order asked, at most `limit` of them.
#[derive(Clone, Debug)]
pub struct ReadOnly {
    queue: VecDeque<PendingRead>,
    limit: usize,
    /// How many of them, from the first, a round that was sent asks for:
    /// its heartbeat carried the context of the last of them, and a quorum
    /// that answers it confirms them all. Those behind were asked after it
    /// was sent, and it proves nothing for them.
    asked: usize,
}
impl ReadOnly {
    pub fn new(limit: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            limit,
            asked: 0,
        }
    }
    /// Whether a read waits that no round sent asks for.
    pub fn unasked(&self) -> bool {
        self.asked < self.queue.len()
    }
    /// A round was sent with the context of the last read: it asks for
    /// every read that waits.
    pub fn asked(&mut self) {
        self.asked = self.queue.len();
    }
    pub fn len(&self) -> usize {
        self.queue.len()
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn clear(&mut self) {
        self.queue = VecDeque::new();
        self.asked = 0;
    }
    fn position(&self, context: &[u8]) -> Option<usize> {
        self.queue
            .iter()
            .position(|read| read.context.as_slice() == context)
    }
    /// A read asked twice is one read; a second asker of a read that waits
    /// is answered with it, never lost to the first.
    pub fn add(
        &mut self,
        index: u64,
        context: Vec<u8>,
        from: NodeId,
        leader: NodeId,
    ) -> Result<()> {
        if let Some(position) = self.position(&context) {
            let Some(read) = self.queue.get_mut(position) else {
                return Ok(());
            };
            if !read.origins.contains(&from) {
                if read.origins.len() >= crate::MAX_MEMBERS {
                    return Err(Error::Capacity("members that ask one read"));
                }
                read.origins.try_reserve(1).map_err(|_| Error::Memory)?;
                read.origins.push(from);
            }
            return Ok(());
        }
        if self.queue.len() >= self.limit {
            return Err(Error::Capacity("reads that wait for their quorum"));
        }
        self.queue
            .try_reserve(1)
            .map_err(|_| Error::Capacity("reads that wait for their quorum"))?;
        let mut acks = Vec::new();
        acks.try_reserve(1)
            .map_err(|_| Error::Capacity("reads that wait for their quorum"))?;
        acks.push(leader);
        let mut origins = Vec::new();
        origins
            .try_reserve(1)
            .map_err(|_| Error::Capacity("reads that wait for their quorum"))?;
        origins.push(from);
        self.queue.push_back(PendingRead {
            context,
            origins,
            index,
            acks,
        });
        Ok(())
    }
    /// `member` confirmed the leader for the read `context`; who has so
    /// far, when the read waits.
    pub fn ack(&mut self, member: NodeId, context: &[u8]) -> Result<Option<&[NodeId]>> {
        let Some(position) = self.position(context) else {
            return Ok(None);
        };
        let Some(read) = self.queue.get_mut(position) else {
            return Ok(None);
        };
        if let Err(at) = read.acks.binary_search(&member) {
            if read.acks.len() >= crate::MAX_MEMBERS {
                return Err(Error::Capacity("members that confirm a read"));
            }
            read.acks.try_reserve(1).map_err(|_| Error::Memory)?;
            read.acks.insert(at, member);
        }
        Ok(Some(read.acks.as_slice()))
    }
    /// The read `context` is confirmed, and so is every read asked before
    /// it: they leave in the order asked.
    pub fn advance(&mut self, context: &[u8]) -> impl Iterator<Item = PendingRead> + use<> {
        let count = self
            .position(context)
            .map_or(0, |position| position.saturating_add(1));
        self.asked = self.asked.saturating_sub(count);
        if count == self.queue.len() {
            // All of them: the queue is given up with them, so that a
            // member that rests holds what it held before it was asked.
            return std::mem::take(&mut self.queue).into_iter().take(count);
        }
        let mut rest = self.queue.split_off(count);
        std::mem::swap(&mut rest, &mut self.queue);
        rest.into_iter().take(count)
    }
    /// What the last read asked is called: a heartbeat that carries it
    /// confirms every read before it too.
    pub fn last_context(&self) -> Option<&[u8]> {
        self.queue.back().map(|read| read.context.as_slice())
    }
    pub fn resident_bytes(&self) -> usize {
        let slots = self
            .queue
            .capacity()
            .saturating_mul(std::mem::size_of::<PendingRead>());
        self.queue.iter().fold(slots, |bytes, read| {
            bytes
                .saturating_add(read.context.capacity())
                .saturating_add(
                    read.origins
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NodeId>()),
                )
                .saturating_add(
                    read.acks
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NodeId>()),
                )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_leave_in_the_order_asked_once_one_is_confirmed() {
        let mut reads = ReadOnly::new(3);
        reads.add(5, b"a".to_vec(), 0, 1).unwrap();
        reads.add(6, b"b".to_vec(), 2, 1).unwrap();
        reads.add(9, b"a".to_vec(), 3, 1).unwrap();
        assert_eq!(reads.len(), 2);
        reads.add(7, b"c".to_vec(), 0, 1).unwrap();
        assert_eq!(
            reads.add(8, b"d".to_vec(), 0, 1),
            Err(Error::Capacity("reads that wait for their quorum"))
        );
        assert_eq!(reads.last_context(), Some(b"c".as_slice()));
        // A round sent now asks for the three; what is asked after it is
        // asked for by none, and a confirmation takes what it confirms
        // from those asked for.
        assert!(reads.unasked());
        reads.asked();
        assert!(!reads.unasked());
        assert_eq!(reads.ack(3, b"b").unwrap(), Some([1, 3].as_slice()));
        assert_eq!(reads.ack(2, b"b").unwrap(), Some([1, 2, 3].as_slice()));
        assert_eq!(reads.ack(2, b"b").unwrap(), Some([1, 2, 3].as_slice()));
        assert_eq!(reads.ack(2, b"z").unwrap(), None);
        assert!(reads.resident_bytes() > 0);
        let confirmed: Vec<_> = reads.advance(b"b").collect();
        assert_eq!(
            confirmed
                .iter()
                .map(|read| (read.index, read.origins().to_vec()))
                .collect::<Vec<_>>(),
            // The second asker of "a" (member 3) is answered with it.
            vec![(5, vec![0, 3]), (6, vec![2])]
        );
        assert_eq!(confirmed[1].acks(), [1, 2, 3]);
        assert_eq!(reads.advance(b"b").count(), 0);
        assert_eq!(reads.len(), 1);
        assert!(!reads.unasked());
        reads.add(8, b"d".to_vec(), 0, 1).unwrap();
        assert!(reads.unasked());
        reads.clear();
        assert!(reads.is_empty() && reads.last_context().is_none());
    }
}
