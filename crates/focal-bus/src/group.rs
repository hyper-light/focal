//! A part its group replicates (doc 34): the part as the state machine of a hyper-durable replica,
//! its batches the group's entries (doc 32 D4).
//!
//! Applying a committed entry appends its batch to the part, or recognises a batch its producer sent
//! again (doc 34 P6), and answers which, for the leader to acknowledge it and every node to deliver
//! it (P4, P5). Everything an apply decides from is the same at every node of the group: the
//! entries, in their order, and the part's bounds, which the group states. What one node alone
//! decides, which batches its memory retains for subscribers that start behind, never changes what
//! an entry does: the window lets batches go oldest first, so the part's floors are its state as of
//! the newest entry let go, at every node that let it go.
//!
//! **The image.** The part keeps nothing of its own on disk but its image: each stream's floor and
//! what its newest batch there stated, and the configuration, as of the newest entry the window let
//! go (`Node::released`). The group's log is compacted to that point and no further
//! (`StateMachine::durable`), so the log holds what the window holds: a node that restarts opens
//! on its image and the log replays the window into it, offsets and batches alike (doc 34 P10). A
//! member that lacks entries its leader's log no longer holds is given the leader's image, and the
//! entries after it.

use std::collections::VecDeque;
use std::path::PathBuf;

use focal_platform::fs::{FileMedium, Medium};
use hyper_durable::{EntryRef, Fatal, Point, StateMachine};
use hyper_raft::proto::{ConfChangeV2, ConfState};
use hyper_raft::wire::Record;

use crate::BusError;
use crate::batch::{self, Batch};
use crate::node::Node;
use crate::part::Taken;
use crate::stream::Produced;

/// What applying an entry answered: the batch, by its stream, producer and sequence, and what the
/// part did with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Applied {
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
    /// Refused alike at every node: the entry is no batch the part takes, or its stream is past
    /// the part's bound.
    Refused(&'static str),
}

/// Format: the image's first bytes.
const MAGIC: [u8; 4] = *b"FBPI";
/// Format: the image's layout.
const VERSION: u8 = 1;
/// Format: an image's bytes before its configuration: the magic, the version, the point's index
/// and term, and the configuration's length.
const HEAD_BYTES: usize = 4 + 1 + 8 + 8 + 4;
/// Format: a stream's bytes in an image: its identity, its floor, and what its newest batch at the
/// floor stated (the producer and its next sequence).
const STREAM_IMAGE_BYTES: usize = 8 + 8 + 8 + 8;
/// Format: the count of streams, and the CRC-32C over everything before it at the end.
const TAIL_BYTES: usize = 8 + 4;

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

/// The bytes an image of `streams` streams under `configuration` takes.
fn image_len(streams: usize, configuration: &ConfState) -> Option<usize> {
    HEAD_BYTES
        .checked_add(configuration.encoded_len())?
        .checked_add(streams.checked_mul(STREAM_IMAGE_BYTES)?)?
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
    /// The point of the image last made durable: what a restart opens at.
    durable: Point,
    /// Where the image is kept.
    path: PathBuf,
    /// The most bytes an image of this part takes: its stream bound's.
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
        let streams = node.part_limits().streams;
        let image_bound = image_len(streams, &largest_configuration(members)?)
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
            node.restore(image.floors(), (image.point.index, image.point.term))?;
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
            durable: point,
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

    /// The point the part's floors are its state as of: the newest entry the window let go, or
    /// the image's it opened on.
    fn floor_point(&self) -> Point {
        let (index, term) = self.node.released();
        let released = Point { index, term };
        released.max(self.durable)
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
        let Ok(header) = Batch::parse_verified(entry.data, entry.data.len()) else {
            answers.push(Applied {
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
            .apply_committed(entry.data, (entry.index, entry.term))
        {
            Ok((taken, relieved)) => {
                self.relieved |= relieved;
                match taken {
                    Taken::Appended(appended) => Outcome::Appended {
                        base: appended.base,
                    },
                    Taken::Again { base, .. } => Outcome::Again { base },
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
        self.durable
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
        let len = image_len(self.node.part().streams(), &self.floor_configuration)?;
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
        let mut medium = FileMedium;
        focal_platform::fs::install(&mut medium, &self.path, image)
            .map_err(|_| Fatal("the part's image could not be made durable"))?;
        self.node
            .restore(decoded.floors(), (at.index, at.term))
            .map_err(|_| Fatal("the part could not hold the image's streams"))?;
        self.applied = at;
        self.durable = at;
        self.configuration = configuration.clone();
        self.floor_configuration = configuration.clone();
        self.changes.clear();
        Ok(())
    }

    fn persist(&mut self) -> Result<(), Fatal> {
        self.settle_floor();
        let mut bytes = Vec::new();
        let point = self.write_image(&mut bytes)?;
        if point == self.durable {
            return Ok(());
        }
        let mut medium = FileMedium;
        focal_platform::fs::install(&mut medium, &self.path, &bytes)
            .map_err(|_| Fatal("the part's image could not be made durable"))?;
        self.durable = point;
        Ok(())
    }
}

/// A part's image, decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Image {
    point: Point,
    configuration: ConfState,
    /// Each stream's identity, floor, and what its newest batch at the floor stated.
    streams: Vec<(u64, u64, Produced)>,
}

impl Image {
    fn floors(&self) -> impl Iterator<Item = (u64, u64, Produced)> + '_ {
        self.streams.iter().copied()
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), BusError> {
        let configuration = self.configuration.encode_to_vec();
        let length = u32::try_from(configuration.len())
            .map_err(|_| BusError::Capacity("an image's configuration"))?;
        let count = u64::try_from(self.streams.len())
            .map_err(|_| BusError::Capacity("an image's streams"))?;
        let start = out.len();
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&self.point.index.to_le_bytes());
        out.extend_from_slice(&self.point.term.to_le_bytes());
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(&configuration);
        for (id, floor, tail) in &self.streams {
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&floor.to_le_bytes());
            out.extend_from_slice(&tail.producer.to_le_bytes());
            out.extend_from_slice(&tail.next_sequence.to_le_bytes());
        }
        out.extend_from_slice(&count.to_le_bytes());
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
        let (body, count) = reader.bytes.split_last_chunk::<8>().ok_or_else(refused)?;
        let count = usize::try_from(u64::from_le_bytes(*count)).map_err(|_| refused())?;
        if body.len() != count.checked_mul(STREAM_IMAGE_BYTES).ok_or_else(refused)? {
            return Err(refused());
        }
        let mut streams = Vec::new();
        streams.try_reserve_exact(count).map_err(|_| refused())?;
        let mut rows = Reader { bytes: body };
        // Bounded: the count the length checked above.
        for _ in 0..count {
            let id = rows.u64()?;
            let floor = rows.u64()?;
            let producer = rows.u64()?;
            let next_sequence = rows.u64()?;
            streams.push((
                id,
                floor,
                Produced {
                    producer,
                    next_sequence,
                },
            ));
        }
        Ok(Self {
            point,
            configuration,
            streams,
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

    fn entry(index: u64, term: u64, data: &[u8]) -> EntryRef<'_> {
        EntryRef {
            index,
            term,
            kind: EntryType::EntryNormal,
            context: &[],
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
        // Another producer's batch is appended; the first producer's after it is too (at least
        // once: the stream keeps only its newest batch's producer).
        assert_eq!(
            apply(&mut machine, 6, &batch(7, 4, 0, 1, 16))
                .unwrap()
                .outcome,
            Outcome::Appended { base: 5 }
        );
        assert_eq!(
            apply(&mut machine, 7, &second).unwrap().outcome,
            Outcome::Appended { base: 6 }
        );
        // An entry that is no batch is refused alike at every node, and changes nothing. (Its
        // producer's CRC is not what decides: the transport checked it at the leader, the group's
        // records and the log carry their own, and every subscriber checks it end to end.)
        let mut short = batch(7, 3, 9, 1, 16);
        short.pop();
        assert!(matches!(
            apply(&mut machine, 8, &short).unwrap().outcome,
            Outcome::Refused(_)
        ));
        assert_eq!(machine.node().part().bounds(7), Some((0, 9)));
        assert_eq!(machine.applied.index, 8);
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
            let applied = apply(&mut machine, index, &batch(stream, 1, at, 1, 400 << 10)).unwrap();
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
        // Each stream's floor is its next offset as of the image's point: every batch of it at or
        // before the point.
        let decoded = Image::decode(&image).unwrap();
        for (stream, floor, tail) in decoded.floors() {
            let taken_by_then = (1..=released).filter(|i| (*i - 1) % 3 == stream).count();
            assert_eq!(floor, taken_by_then as u64, "stream {stream}");
            assert_eq!(tail.producer, 1);
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
            entries.push(batch(stream, 2, at, 2, 300 << 10));
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

    #[test]
    fn an_image_installed_replaces_the_part_and_is_durable() {
        let leader_dir = tempfile::tempdir().unwrap();
        let mut leader = machine(2, leader_dir.path());
        for at in 0..30u32 {
            apply(
                &mut leader,
                u64::from(at) + 1,
                &batch(u64::from(at % 2), 5, at, 1, 400 << 10),
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
