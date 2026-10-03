//! Questions about the log's state, answered as they come: the groups, a group's view, a term,
//! and entries fetched into the caller's reservation by ticket (mantle note 32 §3.9, L-2). Entries
//! in memory are copied at once; those that are not are read from the file on the device, a run
//! whose blocks touch in one read, and the fetch is answered when they come back.

use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;

use super::Owner;
use crate::device::{Job, Reads, Run, Wanted};
use crate::state::Group;
use crate::ticket::{Answer, Ticket};
use crate::{Fetched, LogError, Proposal, View};

/// What a caller asks of the log's state.
pub(crate) enum Query {
    Groups,
    View(u128),
    Term(u128, u64),
    Entries {
        group: u128,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: Fetched,
    },
    Flushed,
    Fenced,
}

/// Entries to fetch, waiting for their read from the file.
pub(super) struct Fetch {
    reads: Reads,
    ticket: Ticket,
    /// Reads made of entries that missed, which a second miss ends.
    retries: u32,
}

/// Reads an entry that missed its place is given before it is called damaged: mantle-log looks
/// an entry up again once, should a sweep have moved it since its place was taken.
const RETRIES: u32 = 2;

impl<F: BlockFile + 'static> Owner<F> {
    pub(super) fn query(&mut self, query: Query, mut ticket: Ticket) {
        let answer = match query {
            Query::Groups => Ok(Answer::Groups(self.state.groups.keys().copied().collect())),
            Query::View(group) => self.view(group).map(Answer::View),
            Query::Term(group, index) => self.term(group, index).map(Answer::Term),
            Query::Entries {
                group,
                low,
                high,
                max_bytes,
                into,
            } => {
                self.fetch(group, (low, high, max_bytes), into, ticket);
                return;
            }
            Query::Flushed => Ok(Answer::Flushed(self.schedule.frames, self.schedule.updates)),
            Query::Fenced => Ok(Answer::Fenced(self.fenced)),
        };
        ticket.answer(answer);
    }

    fn view(&self, group: u128) -> Result<Option<View>, LogError> {
        if self.state.damaged.contains_key(&group) {
            return Err(LogError::Damaged(
                "the group's acknowledged records are damaged; it recovers from its peers",
            ));
        }
        let Some(g) = self.state.groups.get(&group) else {
            return Ok(None);
        };
        Ok(Some(View {
            start: g.start,
            last: g.last().ok_or(LogError::Damaged("an index past u64"))?,
            hard_state: g.hard.map(|(h, _)| h),
            proposals: g
                .proposals
                .iter()
                .map(|(&index, p)| Proposal {
                    index,
                    term: p.term,
                    bytes: p.bytes.clone(),
                })
                .collect(),
            uncertain: g.uncertain.map(|(mark, _)| mark),
        }))
    }

    fn term(&self, group: u128, index: u64) -> Result<u64, LogError> {
        let g = self
            .state
            .groups
            .get(&group)
            .ok_or(LogError::Unavailable { group, index })?;
        if index < g.start.index {
            return Err(LogError::Compacted {
                group,
                first: g.start.index,
            });
        }
        g.term(index).ok_or(LogError::Unavailable { group, index })
    }

    /// The entries of `[low, high)`, as many as `max_bytes` of payload admit and one at least:
    /// those in memory copied into the caller's reservation at once, those no longer in memory
    /// read from the file on the device, a run whose blocks touch in one read, as an update's
    /// entries lie together in its frame and a replica catching up asks for runs of them (mantle
    /// audit P07).
    fn fetch(
        &mut self,
        group: u128,
        range: (u64, u64, u64),
        mut into: Fetched,
        mut ticket: Ticket,
    ) {
        into.clear();
        let mut reads = std::mem::replace(&mut self.spare_reads, Reads::none());
        reads.group = group;
        reads.runs.clear();
        reads.wanted.clear();
        reads.missed.clear();
        reads.result = Ok(());
        if let Err(e) = self.plan(group, range, &mut into, &mut reads) {
            self.spare_reads = reads;
            ticket.answer(Err(e));
            return;
        }
        if reads.runs.is_empty() {
            self.spare_reads = reads;
            ticket.answer(Ok(Answer::Entries(into)));
            return;
        }
        if self.fetches.len() >= self.p.config.max_groups {
            self.spare_reads = reads;
            ticket.answer(Err(LogError::Busy));
            return;
        }
        reads.into = into;
        self.fetches.push_back(Fetch {
            reads,
            ticket,
            retries: 0,
        });
        if self.fetches.len() == 1 {
            self.read_next();
        }
    }

    /// What to read of `[low, high)`: entries in memory go into `into` now, the others leave a
    /// place there and are grouped into runs whose blocks touch.
    fn plan(
        &self,
        group: u128,
        (low, high, max_bytes): (u64, u64, u64),
        into: &mut Fetched,
        reads: &mut Reads,
    ) -> Result<(), LogError> {
        let g = self
            .state
            .groups
            .get(&group)
            .ok_or(LogError::Unavailable { group, index: low })?;
        let first = g.first().ok_or(LogError::Damaged("an index past u64"))?;
        if low < first {
            return Err(LogError::Compacted { group, first });
        }
        let mut total = 0u64;
        for index in low..high {
            let slot = g
                .slot(index)
                .ok_or(LogError::Unavailable { group, index })?;
            total = total.saturating_add(u64::from(slot.len));
            if !into.is_empty() && total > max_bytes {
                break;
            }
            plan_one(g, index, self.p.align, into, reads)
                .ok_or(LogError::Corrupt { group, index })?;
        }
        Ok(())
    }

    /// Hands the device the first fetch's reads.
    fn read_next(&mut self) {
        let Some(fetch) = self.fetches.front_mut() else {
            return;
        };
        let reads = std::mem::replace(&mut fetch.reads, Reads::none());
        self.io.push_back((Job::Read(reads), None));
    }

    /// A fetch's reads are back: entries that missed their place are looked up again, once, as
    /// one moved by a sweep since its place was taken; the fetch is answered when none is left.
    pub(super) fn read(&mut self, mut reads: Reads) {
        let Some(mut fetch) = self.fetches.pop_front() else {
            return;
        };
        let outcome = std::mem::replace(&mut reads.result, Ok(()))
            .and_then(|()| self.again(&mut reads, fetch.retries));
        match outcome {
            Ok(()) if reads.runs.is_empty() => {
                let into = std::mem::take(&mut reads.into);
                self.spare_reads = reads;
                fetch.ticket.answer(Ok(Answer::Entries(into)));
            }
            Ok(()) => {
                fetch.reads = reads;
                fetch.retries = fetch.retries.saturating_add(1);
                self.fetches.push_front(fetch);
            }
            Err(e) => fetch.ticket.answer(Err(e)),
        }
        self.read_next();
    }

    /// Replaces `reads`' runs with reads of the entries that missed: each where the group now
    /// holds it. One where it was is damaged, as is one that missed past the retries; one the
    /// group no longer holds is unavailable.
    fn again(&self, reads: &mut Reads, retries: u32) -> Result<(), LogError> {
        reads.runs.clear();
        reads.wanted.clear();
        let group = reads.group;
        for wanted in reads.missed.drain(..) {
            let corrupt = LogError::Corrupt {
                group,
                index: wanted.index,
            };
            let now = self
                .state
                .groups
                .get(&group)
                .and_then(|g| g.slot(wanted.index))
                .map(|s| s.place)
                .ok_or(LogError::Unavailable {
                    group,
                    index: wanted.index,
                })?;
            if now.offset == wanted.offset || retries.saturating_add(1) >= RETRIES {
                return Err(corrupt);
            }
            let moved = Wanted {
                offset: now.offset,
                ..wanted
            };
            let (begin, end) =
                crate::device::span(self.p.align, moved.offset, moved.len).ok_or(corrupt)?;
            reads.runs.push(Run {
                begin,
                end,
                slot: now.slot,
                contiguous: moved.at.saturating_add(1),
                first: reads.wanted.len(),
                count: 1,
            });
            reads.wanted.push(moved);
        }
        Ok(())
    }
}

/// Plans one entry of a fetch: copied at once if in memory, a place kept and a read planned if
/// not. `None` when its span is past the file's offsets.
fn plan_one(
    g: &Group,
    index: u64,
    align: Alignment,
    into: &mut Fetched,
    reads: &mut Reads,
) -> Option<()> {
    let slot = g.slot(index)?;
    if let Some(bytes) = &slot.cached {
        into.push(slot.term, bytes);
        return Some(());
    }
    let wanted = Wanted {
        at: into.reserve(slot.term),
        index,
        term: slot.term,
        offset: slot.place.offset,
        len: slot.len,
    };
    add_to_runs(reads, align, slot.place.slot, wanted)
}

/// Adds `wanted` to the last run when its blocks touch that run's, in the same segment, and it
/// follows the run's last entry among those asked for; to a new run otherwise.
fn add_to_runs(reads: &mut Reads, align: Alignment, slot: u32, wanted: Wanted) -> Option<()> {
    let (b, e) = crate::device::span(align, wanted.offset, wanted.len)?;
    let next = wanted.at.saturating_add(1);
    let first = reads.wanted.len();
    reads.wanted.push(wanted);
    if let Some(run) = reads.runs.last_mut()
        && run.slot == slot
        && b >= run.begin
        && b <= run.end
        && run.contiguous == wanted.at
    {
        run.end = run.end.max(e);
        run.contiguous = next;
        run.count = run.count.saturating_add(1);
        return Some(());
    }
    reads.runs.push(Run {
        begin: b,
        end: e,
        slot,
        contiguous: next,
        first,
        count: 1,
    });
    Some(())
}
