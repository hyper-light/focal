//! The shared shell's side of `DurableNode` ([27] §15.7, option (B)): what lets focal's owners run
//! over hyper-durable's replica unchanged.
//!
//! - [`HandOver`] is the replica's state machine. It applies nothing itself: each committed entry,
//!   change of configuration and installed image waits for the owner's next drain as
//!   [`NodeEvents`], which the owner applies as it does over focal-log. What a restart opens at
//!   is the group's durable image, in the group's own files (`group_files`): written when the
//!   owner checkpoints and when the replica installs a leader's image, and read back, verified,
//!   when the replica serves it.
//! - [`FloorStore`] is the replica's log store around hyper-log's. It holds a write that carries
//!   an entry needing a decoder the group's records do not state durable ([27] §15.5, O2). The
//!   shell stalls the replica whole on the hold (`Fault::Held`), and the owner releases it once
//!   its records are durable.
//!
//! [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
use std::path::PathBuf;
use std::task::Waker;

use focal_platform::fs::Medium;
use hyper_durable::{EntryRef, Fatal, Fault, LogStore, Point, StateMachine, StoreView, Write};
use hyper_raft::StorageError;
use hyper_raft::proto::{ConfChangeV2, ConfState, Entry, EntryType};

use crate::group_files::{self, GroupFileError, ImagePoint};
use crate::{
    AppliedMembership, AppliedSnapshot, CommittedEntry, ConsensusError, MembershipConfiguration,
    NodeEvents,
};

/// The replica's state machine over the shell: what was committed, waiting for the owner's next
/// drain, and the point of the group's durable image, which a restart opens at.
pub(crate) struct HandOver<M> {
    medium: M,
    /// The group's own directory (`group_files::group_dir`).
    dir: PathBuf,
    /// Every entry of the group is acted on at its members' next start (`apply_on_written_commit`,
    /// focal's control groups): the shell applies none before its durable commit covers it.
    control: bool,
    /// The most an image of the group holds: the owner's checkpoint bound.
    image_bound: usize,
    /// Entries committed and handed over, waiting for the owner's next drain, in index order.
    committed: Vec<CommittedEntry>,
    /// Changes of configuration applied, waiting likewise.
    membership: Vec<AppliedMembership>,
    /// An image installed from the group's leader, waiting likewise. An install replaces what
    /// was handed over before it, so one at most waits.
    snapshot: Option<AppliedSnapshot>,
    /// The last entry handed over.
    applied: Point,
    /// The configuration as of `applied`.
    configuration: ConfState,
    /// The point of the group's durable image: none before the group's first checkpoint or
    /// install.
    imaged: Option<Point>,
    /// The bytes of the group's durable image: none before its first checkpoint or install.
    image_bytes: Option<u64>,
    /// The configuration the image holds, at its point.
    image_configuration: ConfState,
}

impl<M: Medium> HandOver<M> {
    /// The machine of the group whose files are in `dir`, opened at its durable image, if it has
    /// one, which waits for the owner's first drain as the state it restores; `founding` is the
    /// group's configuration where it has none.
    pub(crate) fn open(
        medium: M,
        dir: PathBuf,
        control: bool,
        image_bound: usize,
        founding: ConfState,
    ) -> Result<Self, GroupFileError> {
        let (applied, configuration, imaged, image_bytes, snapshot) =
            match group_files::read_image(&medium, &dir, image_bound)? {
                Some((point, data)) => {
                    let at = Point {
                        index: point.index,
                        term: point.term,
                    };
                    let bytes = u64::try_from(data.len()).ok();
                    let snapshot = AppliedSnapshot {
                        index: at.index,
                        term: at.term,
                        data,
                        configuration: MembershipConfiguration::from_conf(&point.configuration),
                    };
                    (at, point.configuration, Some(at), bytes, Some(snapshot))
                }
                None => (Point::default(), founding, None, None, None),
            };
        Ok(Self {
            medium,
            dir,
            control,
            image_bound,
            committed: Vec::new(),
            membership: Vec::new(),
            snapshot,
            applied,
            image_configuration: configuration.clone(),
            configuration,
            imaged,
            image_bytes,
        })
    }

    /// Moves what was handed over since the last take into `events`, in order. An image
    /// installed since supersedes what `events` gathered before it: the owner restores from the
    /// image, which holds it. The applied index is the last entry handed over, Raft's own empty
    /// entries among them.
    pub(crate) fn take(&mut self, events: &mut NodeEvents) -> Result<(), ConsensusError> {
        if let Some(snapshot) = self.snapshot.take() {
            events.committed.clear();
            events.membership.clear();
            events.snapshot = Some(snapshot);
        }
        hand(&mut self.committed, &mut events.committed)?;
        hand(&mut self.membership, &mut events.membership)?;
        events.applied_index = self.applied.index;
        Ok(())
    }

    /// Whether anything waits for the owner's next take.
    pub(crate) fn holds_events(&self) -> bool {
        !self.committed.is_empty() || !self.membership.is_empty() || self.snapshot.is_some()
    }

    /// Every entry of the group is acted on at its members' next start from here on
    /// (`apply_on_written_commit`).
    pub(crate) fn act_at_start(&mut self) {
        self.control = true;
    }

    /// The owner's checkpoint: its state at `at`, under `configuration`, made the group's durable
    /// image. Written whole before it returns; a restart opens here from then on, and the log may
    /// be compacted through it.
    pub(crate) fn checkpoint(
        &mut self,
        at: Point,
        configuration: &ConfState,
        image: &[u8],
    ) -> Result<(), GroupFileError> {
        let point = ImagePoint {
            index: at.index,
            term: at.term,
            configuration: configuration.clone(),
        };
        group_files::write_image(&mut self.medium, &self.dir, &point, image, self.image_bound)?;
        self.imaged = Some(at);
        self.image_bytes = u64::try_from(image.len()).ok();
        self.image_configuration = point.configuration;
        Ok(())
    }

    /// Whether the group's image names every member of `current`, the configuration applied: a
    /// member it does not name is seeded only by a later image. Without an image the log is
    /// complete from its first entry, which seeds anyone.
    pub(crate) fn image_names_every_member(&self, current: &ConfState) -> bool {
        self.imaged.is_none()
            || crate::core_state::names_every_member(&self.image_configuration, current)
    }

    /// The last entry handed over.
    pub(crate) fn applied(&self) -> Point {
        self.applied
    }
}

impl<M: Medium> StateMachine for HandOver<M> {
    type Answer = ();

    fn apply(&mut self, entry: &EntryRef<'_>, _answers: &mut Vec<()>) -> Result<(), Fatal> {
        // An empty entry is a leader's own: it moves what was applied and hands nothing over,
        // as over focal-log.
        if entry.kind == EntryType::EntryNormal && !entry.data.is_empty() {
            let mut data = Vec::new();
            data.try_reserve_exact(entry.data.len())
                .map_err(|_| Fatal("an entry handed over"))?;
            data.extend_from_slice(entry.data);
            self.committed
                .try_reserve(1)
                .map_err(|_| Fatal("an entry handed over"))?;
            self.committed.push(CommittedEntry {
                index: entry.index,
                term: entry.term,
                data,
            });
        }
        self.applied = Point {
            index: entry.index,
            term: entry.term,
        };
        Ok(())
    }

    fn apply_change(
        &mut self,
        at: Point,
        change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal> {
        let mut context = Vec::new();
        context
            .try_reserve_exact(change.context.len())
            .map_err(|_| Fatal("a change handed over"))?;
        context.extend_from_slice(&change.context);
        self.membership
            .try_reserve(1)
            .map_err(|_| Fatal("a change handed over"))?;
        self.membership.push(AppliedMembership {
            index: at.index,
            term: at.term,
            context,
            before: MembershipConfiguration::from_conf(&self.configuration),
            after: MembershipConfiguration::from_conf(configuration),
        });
        self.configuration = configuration.clone();
        self.applied = at;
        Ok(())
    }

    fn durable(&self) -> Point {
        self.imaged.unwrap_or_default()
    }

    fn configuration(&self) -> &ConfState {
        &self.configuration
    }

    fn acts_at_start(&self, _entry: &EntryRef<'_>) -> bool {
        self.control
    }

    /// The group's durable image, read from its file and verified there: what a leader sends is
    /// what a restart would open at, and the replica's prepared snapshot is the one copy kept.
    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        let read = group_files::read_image(&self.medium, &self.dir, self.image_bound)
            .map_err(|_| Fatal("the group's image file"))?;
        let Some((point, bytes)) = read else {
            return Err(Fatal("no image: the group has not checkpointed"));
        };
        *into = bytes;
        let at = Point {
            index: point.index,
            term: point.term,
        };
        Ok((at, point.configuration))
    }

    /// The bytes of the group's durable image, which `image` sends: none before the group's first
    /// checkpoint, when the log is weighed against nothing (hyper-durable's compaction rule).
    fn image_bytes(&self) -> Option<u64> {
        self.image_bytes
    }

    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        // Durable before the replica moves the log's start to it (O3, hyper-durable's I8).
        self.checkpoint(at, configuration, image)
            .map_err(|_| Fatal("the group's image file"))?;
        let mut data = Vec::new();
        data.try_reserve_exact(image.len())
            .map_err(|_| Fatal("an image handed over"))?;
        data.extend_from_slice(image);
        // The owner restores from the image: what was handed over before it is behind it.
        self.committed.clear();
        self.membership.clear();
        self.snapshot = Some(AppliedSnapshot {
            index: at.index,
            term: at.term,
            data,
            configuration: MembershipConfiguration::from_conf(configuration),
        });
        self.configuration = configuration.clone();
        self.applied = at;
        Ok(())
    }

    fn persist(&mut self) -> Result<(), Fatal> {
        // The owner's checkpoint is durable before it asks to compact; nothing else here is
        // the machine's to make durable.
        Ok(())
    }
}

/// Moves `queued` to the end of `into`: its buffer itself when `into` holds nothing, so a drain
/// that takes once copies nothing; else after `into` has room for all of it, or nothing moves.
fn hand<T>(queued: &mut Vec<T>, into: &mut Vec<T>) -> Result<(), ConsensusError> {
    if into.is_empty() {
        std::mem::swap(queued, into);
        return Ok(());
    }
    into.try_reserve(queued.len())
        .map_err(|_| ConsensusError::Capacity)?;
    into.append(queued);
    Ok(())
}

/// The decoder an entry's data needs, when it needs one the group's baseline does not give: the
/// owner's word on its own entries (the ledger's managed formats).
pub(crate) type Needs = fn(&[u8]) -> Option<[u8; 32]>;

/// The replica's log store around another. It holds a write that carries an entry needing a
/// decoder the group's records do not state durable ([27] §15.5, O2), and refuses every write
/// submitted behind a held one, as hyper-log refuses a handle's writes sent behind a refused one.
///
/// The records state two decoders at most, the floor and its one successor ([27] §15.2), and so
/// does this store: a third released is never taken for durable, and a write that needs it stays
/// held.
///
/// [27]: ../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md
pub(crate) struct FloorStore<L> {
    inner: L,
    needs: Needs,
    /// The decoders the group's durable records state: the floor, then its successor.
    durable: [Option<[u8; 32]>; 2],
    /// The decoder the held write waits for.
    holding: Option<[u8; 32]>,
}

impl<L: LogStore> FloorStore<L> {
    /// The store `inner`, holding writes by `needs` against the floor and successor the group's
    /// records state durable.
    pub(crate) fn new(
        inner: L,
        needs: Needs,
        floor: Option<[u8; 32]>,
        successor: Option<[u8; 32]>,
    ) -> Self {
        Self {
            inner,
            needs,
            durable: [floor, successor],
            holding: None,
        }
    }

    /// The store underneath, for its owner's own use of it.
    pub(crate) fn inner_mut(&mut self) -> &mut L {
        &mut self.inner
    }

    fn is_durable(&self, decoder: &[u8; 32]) -> bool {
        self.durable.contains(&Some(*decoder))
    }

    /// The first decoder `write`'s entries need that the group's records do not state durable.
    fn missing(&self, write: &Write<'_>) -> Option<[u8; 32]> {
        write
            .entries
            .iter()
            .flat_map(|entries| entries.entries)
            .chain(write.proposals)
            .filter_map(|entry| (self.needs)(&entry.data))
            .find(|decoder| !self.is_durable(decoder))
    }
}

impl<L: LogStore> LogStore for FloorStore<L> {
    type Hold = [u8; 32];

    fn held(&self) -> Option<&[u8; 32]> {
        self.holding.as_ref()
    }

    fn release(&mut self, met: &[u8; 32]) {
        if !self.is_durable(met)
            && let Some(slot) = self.durable.iter_mut().find(|slot| slot.is_none())
        {
            *slot = Some(*met);
        }
        if self
            .holding
            .is_some_and(|holding| self.is_durable(&holding))
        {
            self.holding = None;
        }
    }

    fn depth(&self) -> usize {
        self.inner.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        self.inner.view()
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        self.inner.bounds()
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        self.inner.term(index)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        self.inner.entries(low, high, max_bytes, into)
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        self.inner.visit(low, high, page, visit)
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        self.inner.proposals(into)
    }

    fn room(&self) -> bool {
        self.inner.room()
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        if self.holding.is_some() {
            return Err(Fault::Behind);
        }
        if let Some(decoder) = self.missing(write) {
            self.holding = Some(decoder);
            return Err(Fault::Held);
        }
        self.inner.submit(write, waker)
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        self.inner.poll()
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        // Opening's own writes repair what is durable; they carry no entry of the owner's.
        self.inner.write_now(write)
    }
}

#[cfg(test)]
#[path = "shell_tests.rs"]
mod tests;
