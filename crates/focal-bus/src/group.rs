//! A part its group replicates (doc 34): the part as the state machine of a hyper-durable replica,
//! its batches the group's entries (doc 32 D4).
//!
//! Applying a committed entry appends its batch to the part in its session's order, or recognises a
//! batch its producer sent again, or refuses one past a gap in its session (doc 34 P6), and answers
//! which, for the leader to acknowledge it and every node to deliver it (P4, P5). The leader states
//! with each entry the identity that published it (the entry's context), whose sessions judge it. Everything an apply decides from is the same at every node of the group: the
//! entries, in their order, and the part's bounds, which the group states. What one node alone
//! decides, which batches its memory retains for subscribers that start behind, never changes what
//! an entry does: the window lets batches go oldest first, so the part's floors are its state as of
//! the newest entry let go, at every node that let it go.
//!
//! **The image.** The part keeps nothing of its own on disk but its image: each stream's floor, the
//! producing sessions as of it, and the configuration, as of the newest entry the window let go
//! (`Node::released`). The group's log is compacted to that point and no further
//! (`StateMachine::durable`), so the log holds what the window holds: a node that restarts opens
//! on its image and the log replays the window into it, offsets and batches alike (doc 34 P10). A
//! member that lacks entries its leader's log no longer holds is given the leader's image, and the
//! entries after it.

use std::cell::Cell;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;

use focal_platform::fs::{FileMedium, Medium};
use hyper_durable::{EntryRef, Fatal, Point, StateMachine};
use hyper_raft::proto::{ConfChangeV2, ConfState};
use hyper_raft::wire::Record;

use crate::BusError;
use crate::batch::{self, Batch};
use crate::node::Node;
use crate::part::Taken;
use crate::producer::{Entry, SESSIONS_PER_IDENTITY, Session};

/// What applying an entry answered: the batch, by its stream, producer and sequence, and what the
/// part did with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
    /// The identity that published it, as its entry states; zero where none is stated.
    pub owner: u64,
    /// The batch's stream.
    pub stream: u64,
    /// Its producer.
    pub producer: u64,
    /// Its producer's sequence of its first record.
    pub sequence: u32,
    /// Its records.
    pub count: u16,
    /// What the part did with it.
    pub outcome: Outcome,
}

/// What the part did with a committed batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Appended, its first record at this offset.
    Appended {
        /// The offset of its first record.
        base: u64,
    },
    /// Its producer sent it again; the part took it before, its first record at this offset where
    /// the part still holds it.
    Again {
        /// The offset its first record took, where the part still says.
        base: Option<u64>,
    },
    /// Past a gap in its session's order: not appended, its publisher to send again from the
    /// sequence its session waits for.
    OutOfOrder {
        /// The sequence its session's next batch must begin with.
        expected: u64,
    },
    /// Refused alike at every node: the entry is no batch the part takes, or its stream is past
    /// the part's bound.
    Refused(&'static str),
}

/// Format: the image's first bytes.
const MAGIC: [u8; 4] = *b"FBPI";
/// Format: the image's layout.
const VERSION: u8 = 2;
/// Format: an image's bytes before its configuration: the magic, the version, the point's index
/// and term, and the configuration's length.
const HEAD_BYTES: usize = 4 + 1 + 8 + 8 + 4;
/// Format: a stream's bytes in an image: its identity and its floor.
const STREAM_IMAGE_BYTES: usize = 8 + 8;
/// Format: a session's bytes in an image: its producer, the sequence its next batch takes, its
/// newest batch's first sequence and first offset. A place no session holds is all zeros: a
/// session's next sequence is past its first batch, never zero.
const SESSION_IMAGE_BYTES: usize = 8 + 8 + 8 + 8;
/// Format: an identity's sessions in a stream, in an image: the stream, the identity, and each of
/// its places, most recently used first.
const PRODUCER_IMAGE_BYTES: usize = 8 + 8 + SESSIONS_PER_IDENTITY * SESSION_IMAGE_BYTES;
/// Format: the counts of streams and of producer entries, and the CRC-32C over everything before
/// it at the end.
const TAIL_BYTES: usize = 8 + 8 + 4;

/// The largest configuration of a group of at most `members` members, as an image holds it: every
/// list of members full, which no configuration reaches (its voters and learners together are at
/// most `members`), so its bytes bound any configuration's.
fn largest_configuration(members: usize) -> Result<ConfState, BusError> {
    let ids = || -> Result<Vec<u64>, BusError> {
        let mut ids = Vec::new();
        ids.try_reserve_exact(members)
            .map_err(|_| BusError::Configuration("a configuration's members"))?;
        ids.extend((1..).take(members));
        Ok(ids)
    };
    Ok(ConfState {
        voters: ids()?,
        learners: ids()?,
        voters_outgoing: ids()?,
        learners_next: ids()?,
        auto_leave: true,
    })
}

/// The bytes an image of `streams` streams and `producers` producer entries under `configuration`
/// takes.
fn image_len(streams: usize, producers: usize, configuration: &ConfState) -> Option<usize> {
    HEAD_BYTES
        .checked_add(configuration.encoded_len())?
        .checked_add(streams.checked_mul(STREAM_IMAGE_BYTES)?)?
        .checked_add(producers.checked_mul(PRODUCER_IMAGE_BYTES)?)?
        .checked_add(TAIL_BYTES)
}

/// A part, its window and its subscribers, as its group's state machine.
#[derive(Debug)]
pub struct PartMachine {
    node: Node,
    /// The last entry applied.
    applied: Point,
    /// The configuration as of the last entry applied.
    configuration: ConfState,
    /// Configurations applied past the image's point, each with its entry's index, oldest first.
    /// Each is an entry of the log past the image's point, which the log holds until the window
    /// lets go of the entries before it.
    changes: VecDeque<(u64, ConfState)>,
    /// The configuration as of the image's point.
    floor_configuration: ConfState,
    /// The point of the image last made durable: what a restart opens at. The writer moves it as
    /// it answers.
    durable: Cell<Point>,
    /// Makes the part's images durable off the shard that applies.
    writer: ImageWriter,
    /// Where the image is kept.
    path: PathBuf,
    /// The most bytes an image of this part takes: its stream and producer bounds'.
    image_bound: usize,
    /// Whether an apply let the window give memory back since the owner last asked
    /// ([`PartMachine::take_relieved`]).
    relieved: bool,
}

impl PartMachine {
    /// The part of `node`, opened on the image at `path` where one is kept, and otherwise empty
    /// under `founding`, the configuration its group was formed with; a configuration of the group
    /// names at most `members` members (hyper-raft's `Stated::members`). A node opened on an image
    /// holds every stream the image names, each from its floor, and nothing else: the group's log
    /// replays the rest into it.
    pub fn open(
        mut node: Node,
        path: PathBuf,
        founding: ConfState,
        members: usize,
    ) -> Result<Self, BusError> {
        let limits = node.part_limits();
        let image_bound = image_len(
            limits.streams,
            limits.producers,
            &largest_configuration(members)?,
        )
        .ok_or(BusError::Configuration("an image of the part's streams"))?;
        let medium = FileMedium;
        let exists = medium
            .exists(&path)
            .map_err(|_| BusError::Configuration("the part's image could not be looked for"))?;
        let (point, configuration) = if exists {
            let bytes = medium
                .read(&path, image_bound)
                .map_err(|_| BusError::Configuration("the part's image could not be read"))?;
            let image = Image::decode(&bytes)?;
            node.restore(
                image.floors(),
                &image.producers,
                (image.point.index, image.point.term),
            )?;
            (image.point, image.configuration)
        } else {
            (Point::default(), founding)
        };
        Ok(Self {
            node,
            applied: point,
            configuration: configuration.clone(),
            changes: VecDeque::new(),
            floor_configuration: configuration,
            durable: Cell::new(point),
            writer: ImageWriter::start(path.clone())?,
            path,
            image_bound,
            relieved: false,
        })
    }

    /// The node: its part, its window and its subscribers.
    pub const fn node(&self) -> &Node {
        &self.node
    }

    /// The node, to change: its subscribers are served through it.
    pub const fn node_mut(&mut self) -> &mut Node {
        &mut self.node
    }

    /// Whether an apply let the window give memory back since the last ask: the owner asks the
    /// transport's waiting frames again (`Endpoint::retry_reserving`).
    pub const fn take_relieved(&mut self) -> bool {
        let relieved = self.relieved;
        self.relieved = false;
        relieved
    }

    /// Waits for the image its writer has out, if any: the tests' fact that an image handed is
    /// durable.
    #[cfg(test)]
    pub(crate) fn settle_image(&self) -> Result<(), Fatal> {
        self.writer.settle(&self.durable)
    }

    /// The point of the newest image made durable, the writer's answers taken first.
    fn imaged(&self) -> Point {
        // A failure is the next persist's to report: it is what fences the part.
        let _ = self.writer.take(&self.durable);
        self.durable.get()
    }

    /// The point the part's floors are its state as of: the newest entry the window let go, or
    /// the image's it opened on.
    fn floor_point(&self) -> Point {
        let (index, term) = self.node.released();
        let released = Point { index, term };
        released.max(self.imaged())
    }

    /// Moves the configuration the image states up to the floor's point.
    fn settle_floor(&mut self) {
        let floor = self.floor_point().index;
        // Bounded: each change is taken out once.
        while let Some((index, _)) = self.changes.front() {
            if *index > floor {
                break;
            }
            if let Some((_, configuration)) = self.changes.pop_front() {
                self.floor_configuration = configuration;
            }
        }
    }

    /// The image of the part as of its floor's point, written into `into`.
    fn write_image(&self, into: &mut Vec<u8>) -> Result<Point, Fatal> {
        let point = self.floor_point();
        let image = Image {
            point,
            configuration: self.floor_configuration.clone(),
            streams: self.node.part().floors().collect(),
            producers: self.node.part().floor_producers().copied().collect(),
        };
        image
            .encode(into)
            .map_err(|_| Fatal("the part's image could not be laid out"))?;
        Ok(point)
    }
}

impl StateMachine for PartMachine {
    type Answer = Applied;

    fn apply(&mut self, entry: &EntryRef<'_>, answers: &mut Vec<Applied>) -> Result<(), Fatal> {
        self.applied = Point {
            index: entry.index,
            term: entry.term,
        };
        // A leader's first entry of its term carries nothing.
        if entry.data.is_empty() {
            return Ok(());
        }
        // The identity the leader stated published it: the sessions its batch is judged by.
        let Ok(owner) = <[u8; 8]>::try_from(entry.context).map(u64::from_le_bytes) else {
            answers.push(Applied {
                owner: 0,
                stream: 0,
                producer: 0,
                sequence: 0,
                count: 0,
                outcome: Outcome::Refused("an entry that names no publisher"),
            });
            return Ok(());
        };
        let Ok(header) = Batch::parse_verified(entry.data, entry.data.len()) else {
            answers.push(Applied {
                owner,
                stream: 0,
                producer: 0,
                sequence: 0,
                count: 0,
                outcome: Outcome::Refused("an entry that is no batch"),
            });
            return Ok(());
        };
        let (stream, producer, sequence, count) = (
            header.stream(),
            header.producer(),
            header.sequence(),
            header.count(),
        );
        let outcome = match self
            .node
            .apply_committed(entry.data, (entry.index, entry.term), owner)
        {
            Ok((taken, relieved)) => {
                self.relieved |= relieved;
                match taken {
                    Taken::Appended(appended) => Outcome::Appended {
                        base: appended.base,
                    },
                    Taken::Again { base, .. } => Outcome::Again { base },
                    Taken::OutOfOrder { expected, .. } => Outcome::OutOfOrder { expected },
                }
            }
            // What the entries and the part's bounds decide, alike at every node.
            Err(BusError::Batch(_)) => Outcome::Refused("a batch the part does not take"),
            Err(BusError::Capacity(bound)) => Outcome::Refused(bound),
            // Memory is the node's own: a committed batch it cannot hold leaves its part behind
            // its group, and the replica reopens from what is durable.
            Err(BusError::Memory(_)) => {
                return Err(Fatal("the node has no memory for a committed batch"));
            }
            Err(_) => return Err(Fatal("a committed batch could not be applied")),
        };
        answers.push(Applied {
            owner,
            stream,
            producer,
            sequence,
            count,
            outcome,
        });
        self.settle_floor();
        Ok(())
    }

    fn apply_change(
        &mut self,
        at: Point,
        _change: &ConfChangeV2,
        configuration: &ConfState,
    ) -> Result<(), Fatal> {
        self.applied = at;
        self.configuration = configuration.clone();
        self.changes.push_back((at.index, configuration.clone()));
        self.settle_floor();
        Ok(())
    }

    fn durable(&self) -> Point {
        self.imaged()
    }

    fn configuration(&self) -> &ConfState {
        &self.configuration
    }

    /// Nothing: a part acts on no entry at its start, before its group tells it anything (I5). It
    /// delivers what its group committed, and a restart replays that.
    fn acts_at_start(&self, _entry: &EntryRef<'_>) -> bool {
        false
    }

    fn image(&mut self, into: &mut Vec<u8>) -> Result<(Point, ConfState), Fatal> {
        self.settle_floor();
        into.clear();
        let point = self.write_image(into)?;
        Ok((point, self.floor_configuration.clone()))
    }

    fn image_bytes(&self) -> Option<u64> {
        let part = self.node.part();
        let len = image_len(
            part.streams(),
            part.floor_producers().count(),
            &self.floor_configuration,
        )?;
        u64::try_from(len).ok()
    }

    fn install(&mut self, image: &[u8], at: Point, configuration: &ConfState) -> Result<(), Fatal> {
        if image.len() > self.image_bound {
            return Err(Fatal("an image past the part's bound"));
        }
        let decoded = Image::decode(image).map_err(|_| Fatal("an image that does not decode"))?;
        if decoded.point != at {
            return Err(Fatal("an image of another point"));
        }
        // An image the writer has out would land after this one, the older over the newer: its
        // answer comes first.
        self.writer.settle(&self.durable)?;
        let mut medium = FileMedium;
        focal_platform::fs::install(&mut medium, &self.path, image)
            .map_err(|_| Fatal("the part's image could not be made durable"))?;
        self.node
            .restore(decoded.floors(), &decoded.producers, (at.index, at.term))
            .map_err(|_| Fatal("the part could not hold the image's streams"))?;
        self.applied = at;
        self.durable.set(at);
        self.configuration = configuration.clone();
        self.floor_configuration = configuration.clone();
        self.changes.clear();
        Ok(())
    }

    /// Hands the image of the part's floor to its writer, which makes it durable off the shard:
    /// the durable point moves as it answers (`StateMachine::durable`), and a compaction asked
    /// meanwhile compacts to the image made durable before, as the ledger's machine compacts to
    /// its owner's last checkpoint. One image is out at a time; with one out, the next waits.
    fn persist(&mut self) -> Result<(), Fatal> {
        self.writer.take(&self.durable)?;
        if self.writer.out.get() {
            return Ok(());
        }
        self.settle_floor();
        let mut bytes = Vec::new();
        let point = self.write_image(&mut bytes)?;
        if point == self.durable.get() {
            return Ok(());
        }
        self.writer.hand(bytes, point)
    }
}

/// Makes the part's images durable off the shard that applies. An image is an atomic replace of its
/// file (`focal_platform::fs::install`: a temporary file and its sync, the rename, the directory's
/// sync), which the shard waited for at every compaction: 20 to 156 ms an image on fedora's busy
/// disk, and 4.4 to 4.6 s of each node's 40-second closed loop at a window of 32,768 (2026-10-19),
/// what its heartbeats and answers waited behind. One thread of its own, which writes one image at
/// a time and answers each; the part hands it the next only once it has answered.
#[derive(Debug)]
struct ImageWriter {
    /// Images to make durable, each with its point; one at most out. Closed as the part goes.
    images: Option<SyncSender<(Vec<u8>, Point)>>,
    /// Each image's answer: its point once durable, or the failure that fences the part.
    answers: Receiver<Result<Point, ()>>,
    /// Whether an image is out, its answer not yet taken.
    out: Cell<bool>,
    /// A failed image, reported to the part at its next persist.
    failed: Cell<bool>,
    thread: Option<JoinHandle<()>>,
}

impl ImageWriter {
    /// The writer of the image at `path`, its thread started.
    fn start(path: PathBuf) -> Result<Self, BusError> {
        // One image out at most, and its one answer.
        let (images, requests) = sync_channel::<(Vec<u8>, Point)>(1);
        let (answering, answers) = sync_channel::<Result<Point, ()>>(1);
        let thread = std::thread::Builder::new()
            .name("focal-bus-image".into())
            .spawn(move || {
                let mut medium = FileMedium;
                // Bounded: one write an image handed, until the part closes its end.
                while let Ok((bytes, point)) = requests.recv() {
                    let answer = focal_platform::fs::install(&mut medium, &path, &bytes)
                        .map(|()| point)
                        .map_err(|_| ());
                    if answering.send(answer).is_err() {
                        return;
                    }
                }
            })
            .map_err(|_| BusError::Configuration("the part's image writer could not start"))?;
        Ok(Self {
            images: Some(images),
            answers,
            out: Cell::new(false),
            failed: Cell::new(false),
            thread: Some(thread),
        })
    }

    /// Takes the writer's answer, if it came: `durable` moves to the image's point, or the failure
    /// is kept and reported.
    fn take(&self, durable: &Cell<Point>) -> Result<(), Fatal> {
        if let Ok(answer) = self.answers.try_recv() {
            self.out.set(false);
            match answer {
                Ok(point) => durable.set(durable.get().max(point)),
                Err(()) => self.failed.set(true),
            }
        }
        if self.failed.get() {
            return Err(Fatal("the part's image could not be made durable"));
        }
        Ok(())
    }

    /// Waits for the image out, if any, and takes its answer.
    fn settle(&self, durable: &Cell<Point>) -> Result<(), Fatal> {
        if self.out.get() {
            match self.answers.recv() {
                Ok(Ok(point)) => durable.set(durable.get().max(point)),
                Ok(Err(())) => self.failed.set(true),
                Err(_) => return Err(Fatal("the part's image writer ended")),
            }
            self.out.set(false);
        }
        self.take(durable)
    }

    /// Hands the writer an image of `point`: none is out.
    fn hand(&self, bytes: Vec<u8>, point: Point) -> Result<(), Fatal> {
        let Some(images) = &self.images else {
            return Err(Fatal("the part's image writer ended"));
        };
        match images.try_send((bytes, point)) {
            Ok(()) => {
                self.out.set(true);
                Ok(())
            }
            // None out, so a full queue is a writer gone wrong as surely as a closed one.
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                Err(Fatal("the part's image writer ended"))
            }
        }
    }
}

impl Drop for ImageWriter {
    /// The thread ends once it has answered the image it has out: its end of the queue closes.
    fn drop(&mut self) {
        self.images = None;
        // Its answer, if any, is let go: the queue holds one at most.
        let _ = self.answers.try_recv();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A part's image, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Image {
    point: Point,
    configuration: ConfState,
    /// Each stream's identity and floor.
    streams: Vec<(u64, u64)>,
    /// Each identity's sessions in each stream, as of the floors.
    producers: Vec<Entry>,
}

impl Image {
    fn floors(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.streams.iter().copied()
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), BusError> {
        let configuration = self.configuration.encode_to_vec();
        let length = u32::try_from(configuration.len())
            .map_err(|_| BusError::Capacity("an image's configuration"))?;
        let count = u64::try_from(self.streams.len())
            .map_err(|_| BusError::Capacity("an image's streams"))?;
        let producers = u64::try_from(self.producers.len())
            .map_err(|_| BusError::Capacity("an image's producers"))?;
        let start = out.len();
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&self.point.index.to_le_bytes());
        out.extend_from_slice(&self.point.term.to_le_bytes());
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(&configuration);
        for (id, floor) in &self.streams {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&floor.to_le_bytes());
        }
        for entry in &self.producers {
            out.extend_from_slice(&entry.stream.to_le_bytes());
            out.extend_from_slice(&entry.owner.to_le_bytes());
            for place in &entry.sessions {
                let session = place.unwrap_or_default();
                out.extend_from_slice(&session.producer.to_le_bytes());
                out.extend_from_slice(&session.next.to_le_bytes());
                out.extend_from_slice(&session.newest.to_le_bytes());
                out.extend_from_slice(&session.base.to_le_bytes());
            }
        }
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&producers.to_le_bytes());
        let crc = batch::crc32c(out.get(start..).unwrap_or(&[]));
        out.extend_from_slice(&crc.to_le_bytes());
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<Self, BusError> {
        let refused = || BusError::Protocol("a part's image");
        let (covered, crc) = bytes.split_last_chunk::<4>().ok_or_else(refused)?;
        if batch::crc32c(covered) != u32::from_le_bytes(*crc) {
            return Err(BusError::Protocol("a part's image fails its CRC"));
        }
        let mut reader = Reader { bytes: covered };
        if reader.take(4)? != MAGIC {
            return Err(refused());
        }
        if reader.take(1)? != [VERSION] {
            return Err(BusError::Protocol("a part's image of another version"));
        }
        let point = Point {
            index: reader.u64()?,
            term: reader.u64()?,
        };
        let length = usize::try_from(reader.u32()?).map_err(|_| refused())?;
        let configuration = ConfState::decode(reader.take(length)?).map_err(|_| refused())?;
        let (body, counts) = reader.bytes.split_last_chunk::<16>().ok_or_else(refused)?;
        let (count, producers) = counts.split_at(8);
        let count = <[u8; 8]>::try_from(count)
            .map(u64::from_le_bytes)
            .ok()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(refused)?;
        let producers = <[u8; 8]>::try_from(producers)
            .map(u64::from_le_bytes)
            .ok()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(refused)?;
        let length = count
            .checked_mul(STREAM_IMAGE_BYTES)
            .zip(producers.checked_mul(PRODUCER_IMAGE_BYTES))
            .and_then(|(streams, producers)| streams.checked_add(producers))
            .ok_or_else(refused)?;
        if body.len() != length {
            return Err(refused());
        }
        let mut streams = Vec::new();
        streams.try_reserve_exact(count).map_err(|_| refused())?;
        let mut rows = Reader { bytes: body };
        // Bounded: the counts the length checked above.
        for _ in 0..count {
            let id = rows.u64()?;
            let floor = rows.u64()?;
            streams.push((id, floor));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(producers)
            .map_err(|_| refused())?;
        for _ in 0..producers {
            let stream = rows.u64()?;
            let owner = rows.u64()?;
            let mut sessions = [None; SESSIONS_PER_IDENTITY];
            for place in &mut sessions {
                let session = Session {
                    producer: rows.u64()?,
                    next: rows.u64()?,
                    newest: rows.u64()?,
                    base: rows.u64()?,
                };
                *place = (session.next != 0).then_some(session);
            }
            entries.push(Entry {
                stream,
                owner,
                sessions,
            });
        }
        Ok(Self {
            point,
            configuration,
            streams,
            producers: entries,
        })
    }
}

/// Bytes read from the front.
struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], BusError> {
        let (taken, rest) = self
            .bytes
            .split_at_checked(n)
            .ok_or(BusError::Protocol("a part's image is short"))?;
        self.bytes = rest;
        Ok(taken)
    }

    fn u64(&mut self) -> Result<u64, BusError> {
        let bytes = self.take(8)?;
        bytes
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| BusError::Protocol("a part's image is short"))
    }

    fn u32(&mut self) -> Result<u32, BusError> {
        let bytes = self.take(4)?;
        bytes
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| BusError::Protocol("a part's image is short"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BatchBuilder, Producing};
    use crate::node::NodeConfig;
    use focal_memory::MemoryBudget;
    use hyper_raft::proto::EntryType;

    /// A node whose memory holds `segments` segments besides its tables.
    fn node(segments: usize) -> Node {
        let memory = NodeConfig::SEGMENT_BYTES * segments + (1 << 20);
        Node::new(
            NodeConfig {
                segment_bytes: NodeConfig::SEGMENT_BYTES,
                streams: 64,
                subscriptions: 4,
                producers: 64,
            },
            MemoryBudget::new(memory, 0).unwrap(),
        )
        .unwrap()
    }

    fn founding() -> ConfState {
        ConfState {
            voters: vec![1, 2, 3],
            ..ConfState::default()
        }
    }

    fn machine(segments: usize, dir: &std::path::Path) -> PartMachine {
        PartMachine::open(node(segments), dir.join("part.image"), founding(), 3).unwrap()
    }

    /// A batch of `records` records of `payload` bytes each.
    fn batch(stream: u64, producer: u64, sequence: u32, records: u16, payload: usize) -> Vec<u8> {
        let mut builder = BatchBuilder::new();
        builder.begin(&Producing {
            stream,
            producer,
            sequence,
            flags: 0,
            timestamp: 1_792_022_400_000_000,
        });
        for _ in 0..records {
            builder
                .push(1_792_022_400_000_000, &vec![9u8; payload])
                .unwrap();
        }
        builder.finish().unwrap().to_vec()
    }

    /// The identity every test batch is published by, as the leader states it with the entry.
    const OWNER: [u8; 8] = 5_u64.to_le_bytes();

    fn entry(index: u64, term: u64, data: &[u8]) -> EntryRef<'_> {
        EntryRef {
            index,
            term,
            kind: EntryType::EntryNormal,
            context: &OWNER,
            data,
        }
    }

    fn apply(machine: &mut PartMachine, index: u64, data: &[u8]) -> Option<Applied> {
        let mut answers = Vec::new();
        machine.apply(&entry(index, 1, data), &mut answers).unwrap();
        assert!(answers.len() <= 1);
        answers.pop()
    }

    #[test]
    fn committed_batches_are_appended_in_order_and_one_sent_again_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let mut machine = machine(4, dir.path());
        // A leader's first entry of its term carries nothing, and answers nothing.
        assert_eq!(apply(&mut machine, 1, &[]), None);
        let first = batch(7, 3, 0, 2, 16);
        let applied = apply(&mut machine, 2, &first).unwrap();
        assert_eq!(applied.outcome, Outcome::Appended { base: 0 });
        assert_eq!(
            (applied.stream, applied.producer, applied.sequence),
            (7, 3, 0)
        );
        let second = batch(7, 3, 2, 3, 16);
        assert_eq!(
            apply(&mut machine, 3, &second).unwrap().outcome,
            Outcome::Appended { base: 2 }
        );
        // The newest batch sent again is acknowledged with the offsets it took; an older one is
        // known taken, its offsets no longer said.
        assert_eq!(
            apply(&mut machine, 4, &second).unwrap().outcome,
            Outcome::Again { base: Some(2) }
        );
        assert_eq!(
            apply(&mut machine, 5, &first).unwrap().outcome,
            Outcome::Again { base: None }
        );
        assert_eq!(machine.node().part().bounds(7), Some((0, 5)));
        // Another session's batch is appended; the first session's sent again after it is still
        // known taken, with its offsets: each session is kept.
        assert_eq!(
            apply(&mut machine, 6, &batch(7, 4, 0, 1, 16))
                .unwrap()
                .outcome,
            Outcome::Appended { base: 5 }
        );
        assert_eq!(
            apply(&mut machine, 7, &second).unwrap().outcome,
            Outcome::Again { base: Some(2) }
        );
        // A batch past a gap in its session is refused, naming the sequence the session waits for,
        // and changes nothing; the batch it waits for is then taken.
        assert_eq!(
            apply(&mut machine, 8, &batch(7, 3, 6, 1, 16))
                .unwrap()
                .outcome,
            Outcome::OutOfOrder { expected: 5 }
        );
        assert_eq!(
            apply(&mut machine, 9, &batch(7, 3, 5, 1, 16))
                .unwrap()
                .outcome,
            Outcome::Appended { base: 6 }
        );
        // An entry that names no publisher is refused alike at every node.
        let mut answers = Vec::new();
        let nameless = batch(7, 3, 6, 1, 16);
        let unnamed = EntryRef {
            context: &[],
            ..entry(10, 1, &nameless)
        };
        machine.apply(&unnamed, &mut answers).unwrap();
        assert!(matches!(
            answers.pop().map(|applied| applied.outcome),
            Some(Outcome::Refused(_))
        ));
        // An entry that is no batch is refused alike at every node, and changes nothing. (Its
        // producer's CRC is not what decides: the transport checked it at the leader, the group's
        // records and the log carry their own, and every subscriber checks it end to end.)
        let mut short = batch(7, 3, 9, 1, 16);
        short.pop();
        assert!(matches!(
            apply(&mut machine, 11, &short).unwrap().outcome,
            Outcome::Refused(_)
        ));
        assert_eq!(machine.node().part().bounds(7), Some((0, 7)));
        assert_eq!(machine.applied.index, 11);
    }

    #[test]
    fn the_image_is_the_part_as_of_the_newest_entry_its_window_let_go() {
        let dir = tempfile::tempdir().unwrap();
        // Two segments' memory, each 5 MiB: batches of 400 KiB let the oldest go after a dozen.
        let mut machine = machine(2, dir.path());
        let mut index = 0;
        for at in 0..40u32 {
            index += 1;
            let stream = u64::from(at % 3);
            // Each stream's session takes one record a batch, in order.
            let applied =
                apply(&mut machine, index, &batch(stream, 1, at / 3, 1, 400 << 10)).unwrap();
            assert!(matches!(applied.outcome, Outcome::Appended { .. }));
        }
        let (released, _) = machine.node().released();
        assert!(released > 0, "the window let batches go");
        assert!(released < index, "the window holds the newest batches");
        let mut image = Vec::new();
        let (point, configuration) = machine.image(&mut image).unwrap();
        assert_eq!(point.index, released);
        assert_eq!(configuration, founding());
        assert_eq!(machine.image_bytes(), u64::try_from(image.len()).ok());
        // Each stream's floor is its next offset as of the image's point, every batch of it at or
        // before the point, and its session's next sequence is the same.
        let decoded = Image::decode(&image).unwrap();
        for (stream, floor) in decoded.floors() {
            let taken_by_then = (1..=released).filter(|i| (*i - 1) % 3 == stream).count();
            assert_eq!(floor, taken_by_then as u64, "stream {stream}");
            let session = decoded
                .producers
                .iter()
                .find(|entry| entry.stream == stream && entry.owner == 5)
                .and_then(|entry| entry.sessions[0]);
            assert_eq!(
                session.map(|session| (session.producer, session.next)),
                (floor > 0).then_some((1, floor)),
                "stream {stream}"
            );
        }
        // A damaged image is refused.
        let mut bad = image.clone();
        bad[HEAD_BYTES] ^= 1;
        assert!(Image::decode(&bad).is_err());
    }

    #[test]
    fn a_part_opened_on_its_image_replays_to_the_state_it_had() {
        let dir = tempfile::tempdir().unwrap();
        let mut machine = machine(2, dir.path());
        let mut entries = Vec::new();
        for at in 0..30u32 {
            let stream = u64::from(at % 4);
            // Each stream's session takes two records a batch, in order.
            entries.push(batch(stream, 2, at / 4 * 2, 2, 300 << 10));
        }
        for (i, data) in entries.iter().enumerate() {
            apply(&mut machine, i as u64 + 1, data).unwrap();
        }
        // A change of configuration past the floor stays the machine's; the image states the
        // configuration as of its point.
        let grown = ConfState {
            voters: vec![1, 2, 3, 4],
            ..ConfState::default()
        };
        let change_at = Point {
            index: entries.len() as u64 + 1,
            term: 1,
        };
        machine
            .apply_change(change_at, &ConfChangeV2::default(), &grown)
            .unwrap();
        machine.persist().unwrap();
        machine.settle_image().unwrap();
        let durable = machine.durable();
        assert!(durable.index > 0 && durable.index < change_at.index);
        assert_eq!(machine.configuration(), &grown);

        // A node restarted: opened on the image, and the log replays every entry after it.
        let mut reopened =
            PartMachine::open(node(2), dir.path().join("part.image"), founding(), 3).unwrap();
        assert_eq!(reopened.durable(), durable);
        assert_eq!(reopened.configuration(), &founding());
        for (i, data) in entries.iter().enumerate() {
            let index = i as u64 + 1;
            if index > durable.index {
                apply(&mut reopened, index, data).unwrap();
            }
        }
        reopened
            .apply_change(change_at, &ConfChangeV2::default(), &grown)
            .unwrap();
        for stream in 0..4 {
            assert_eq!(
                reopened.node().part().bounds(stream).map(|(_, next)| next),
                machine.node().part().bounds(stream).map(|(_, next)| next),
                "stream {stream}"
            );
        }
        assert_eq!(reopened.configuration(), &grown);
        // A batch sent again after the restart is known taken, as before it.
        let last = entries.last().unwrap();
        assert!(matches!(
            apply(&mut reopened, 99, last).unwrap().outcome,
            Outcome::Again { .. }
        ));
    }

    /// Do: apply batches past the window's floor, persist, and before the image's answer is taken
    /// read the durable point and persist again; then wait for the image. Expect: the durable point
    /// is the image's once its answer comes and never past it before, the second persist hands
    /// nothing while one is out, and a part reopened on the file opens at that point: the shard
    /// hands its image to the writer and goes on, where it waited for the file's sync.
    #[test]
    fn an_image_is_made_durable_off_the_shard_and_the_durable_point_follows_its_answer() {
        let dir = tempfile::tempdir().unwrap();
        let mut machine = machine(2, dir.path());
        for at in 0..30u32 {
            let data = batch(u64::from(at % 4), 2, at / 4 * 2, 2, 300 << 10);
            apply(&mut machine, u64::from(at) + 1, &data).unwrap();
        }
        let floor = machine.floor_point();
        assert!(floor.index > 0, "the window let batches go");
        machine.persist().unwrap();
        // Before its answer is taken the durable point is the opening's or the image's, never
        // past it; a second persist with one out hands nothing.
        assert!(machine.durable() <= floor);
        machine.persist().unwrap();
        machine.settle_image().unwrap();
        assert_eq!(machine.durable(), floor);
        let reopened =
            PartMachine::open(node(2), dir.path().join("part.image"), founding(), 3).unwrap();
        assert_eq!(reopened.durable(), floor);
    }

    /// Do: hand the writer an image of the floor, and at once install an image of a later point.
    /// Expect: a part reopened on the file opens at the installed point: an install waits for the
    /// image out, so the older never lands over the newer.
    #[test]
    fn an_install_waits_for_the_image_out_so_the_older_never_lands_over_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut machine = machine(2, dir.path());
        for at in 0..30u32 {
            let data = batch(u64::from(at % 4), 2, at / 4 * 2, 2, 300 << 10);
            apply(&mut machine, u64::from(at) + 1, &data).unwrap();
        }
        machine.persist().unwrap();
        let mut leader =
            PartMachine::open(node(2), dir.path().join("leader.image"), founding(), 3).unwrap();
        for at in 0..60u32 {
            let data = batch(u64::from(at % 4), 2, at / 4 * 2, 2, 300 << 10);
            apply(&mut leader, u64::from(at) + 1, &data).unwrap();
        }
        let mut image = Vec::new();
        let (at, configuration) = leader.image(&mut image).unwrap();
        assert!(at > machine.floor_point(), "the leader's image is later");
        machine.install(&image, at, &configuration).unwrap();
        assert_eq!(machine.durable(), at);
        let reopened =
            PartMachine::open(node(2), dir.path().join("part.image"), founding(), 3).unwrap();
        assert_eq!(reopened.durable(), at);
    }

    #[test]
    fn an_image_installed_replaces_the_part_and_is_durable() {
        let leader_dir = tempfile::tempdir().unwrap();
        let mut leader = machine(2, leader_dir.path());
        for at in 0..30u32 {
            apply(
                &mut leader,
                u64::from(at) + 1,
                &batch(u64::from(at % 2), 5, at / 2, 1, 400 << 10),
            )
            .unwrap();
        }
        let mut image = Vec::new();
        let (point, configuration) = leader.image(&mut image).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let mut follower = machine(2, dir.path());
        apply(&mut follower, 1, &batch(9, 1, 0, 1, 16)).unwrap();
        follower.install(&image, point, &configuration).unwrap();
        assert_eq!(follower.durable(), point);
        // Its old stream is gone; the leader's streams hold nothing, each from its floor.
        assert_eq!(follower.node().part().bounds(9), None);
        for stream in 0..2 {
            let floor = leader.node().part().bounds(stream).map(|(floor, _)| floor);
            assert_eq!(
                follower.node().part().bounds(stream),
                floor.map(|floor| (floor, floor))
            );
        }
        // An image of another point is refused, and changes nothing.
        let other = Point {
            index: point.index + 1,
            term: point.term,
        };
        assert!(follower.install(&image, other, &configuration).is_err());
        // It was made durable: a node reopened on it holds the same.
        let reopened =
            PartMachine::open(node(2), dir.path().join("part.image"), founding(), 3).unwrap();
        assert_eq!(reopened.durable(), point);
    }
}
