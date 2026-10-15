//! The world: virtual time, the nodes and their clocks, the pending events, the streams, the
//! trace and the digest, in one owned value (`docs/sim.md` §3.3).
//!
//! A harness owns its processes and drives the world in a loop: it asks [`World::next`] for the
//! next event, hands the event to the process it is for, and schedules what the process asked for.
//! Every choice the loop's run makes — which enabled event runs next, every fate drawn — is made
//! through the world, so the run is its seed, or its trace.

use std::time::Instant;

use crate::clock::{Anchor, Clock, PPM};
use crate::error::SimError;
use crate::queue::{Discipline, Key, Queue};
use crate::trace::{Chooser, Digest, Trace};
use crate::wakes::{Armed, DISARMED, Wakes};

/// A node of the world, by its place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

/// A named stream of draws, by its place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(pub u32);

/// Where a run's decisions come from.
#[derive(Clone, Debug)]
pub enum Source {
    /// Drawn from the named streams of this seed, and recorded.
    Seed(u64),
    /// Read from a recorded trace: the run it records, reproduced without its seed.
    Trace(Trace),
}

/// The bounds a run states (`docs/sim.md` §7): each is the harness's, derived from its schedule,
/// and reaching one is a typed refusal ([`SimError::Full`]) or, for steps, [`Step::Spent`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Events pending at once: messages in flight, plus timers and completions held as events.
    pub events: usize,
    /// Nodes.
    pub nodes: usize,
    /// Named streams, the world's own included: one for the schedule and one a node.
    pub streams: usize,
    /// Steps the run may take.
    pub steps: u64,
    /// Words of trace: the steps times the decisions a step makes, as the harness counts them.
    /// Reserved when the world is made, so it is a bound the host can hold.
    pub trace_words: usize,
}

/// What the world gives the harness next.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<E> {
    /// An event, for `node`.
    Event {
        /// The node it is for.
        node: NodeId,
        /// What the harness scheduled.
        event: E,
    },
    /// `node`'s timer fired.
    Wake {
        /// The node.
        node: NodeId,
    },
    /// Nothing is pending: no event and no armed timer.
    Idle,
    /// The run's step budget is spent.
    Spent,
}

/// An enabled event or timer at a choice point, as a strategy sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Candidate {
    /// A scheduled event: when it is due, the node it is for, and its scheduling ordinal.
    Event {
        /// When it is due, in virtual time.
        at: u64,
        /// The node it is for.
        node: NodeId,
        /// The order it was scheduled in, over the whole run.
        seq: u64,
    },
    /// A node's timer: when it fires, and the node.
    Wake {
        /// When it fires, in virtual time.
        at: u64,
        /// The node.
        node: NodeId,
    },
}

impl Candidate {
    /// When it is due.
    pub fn at(&self) -> u64 {
        match self {
            Self::Event { at, .. } | Self::Wake { at, .. } => *at,
        }
    }
    /// The node it is for.
    pub fn node(&self) -> NodeId {
        match self {
            Self::Event { node, .. } | Self::Wake { node, .. } => *node,
        }
    }
}

/// The enabled candidates at a choice point: the events, then the timers.
pub struct Choice<'a> {
    now: u64,
    events: &'a [Key],
    wakes: &'a [Armed],
}

impl Choice<'_> {
    /// The world's present.
    pub fn now(&self) -> u64 {
        self.now
    }
    /// How many candidates.
    pub fn len(&self) -> usize {
        self.events.len().saturating_add(self.wakes.len())
    }
    /// Whether there are none (never, when a strategy is asked).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Candidate `index`.
    pub fn get(&self, index: usize) -> Option<Candidate> {
        match self.events.get(index) {
            Some(key) => Some(Candidate::Event {
                at: key.at,
                node: NodeId(key.node),
                seq: key.seq,
            }),
            None => {
                let armed = self.wakes.get(index.checked_sub(self.events.len())?)?;
                Some(Candidate::Wake {
                    at: armed.at,
                    node: NodeId(armed.node),
                })
            }
        }
    }
}

/// The draws a strategy makes, from the world's schedule stream, recorded in the trace.
pub struct Draw<'a> {
    chooser: &'a mut Chooser,
    stream: u32,
}

impl Draw<'_> {
    /// Uniform in `[0, bound)`.
    pub fn below(&mut self, bound: u64) -> Result<u64, SimError> {
        self.chooser.below(self.stream, bound)
    }
}

/// What chooses at a choice point (`docs/sim.md` §3.3, §4.5). It is asked only when two or more
/// candidates are enabled, and draws only through [`Draw`], so any run it makes replays.
pub trait Strategy {
    /// The index of the candidate to run.
    fn pick(&mut self, choice: &Choice<'_>, draw: &mut Draw<'_>) -> Result<usize, SimError>;
}

/// Uniform among the enabled: under the free discipline any order, under the ordered one any
/// order of a tie.
#[derive(Clone, Copy, Debug, Default)]
pub struct Random;

impl Strategy for Random {
    fn pick(&mut self, choice: &Choice<'_>, draw: &mut Draw<'_>) -> Result<usize, SimError> {
        let len = u64::try_from(choice.len()).map_err(|_| SimError::Pick {
            picked: 0,
            candidates: choice.len(),
        })?;
        let picked = draw.below(len)?;
        usize::try_from(picked).map_err(|_| SimError::Pick {
            picked: usize::MAX,
            candidates: choice.len(),
        })
    }
}

/// The earliest due, events before timers, then the earliest scheduled or the lowest node: no
/// draw. Under the ordered discipline it is focal's network (equal times delivered in send
/// order), for directed tests that fix their schedule.
#[derive(Clone, Copy, Debug, Default)]
pub struct Fifo;

impl Strategy for Fifo {
    fn pick(&mut self, choice: &Choice<'_>, _draw: &mut Draw<'_>) -> Result<usize, SimError> {
        let rank = |candidate: Candidate| match candidate {
            Candidate::Event { at, seq, .. } => (at, 0u8, seq),
            Candidate::Wake { at, node } => (at, 1, u64::from(node.0)),
        };
        (0..choice.len())
            .filter_map(|index| choice.get(index).map(|c| (rank(c), index)))
            .min()
            .map(|(_, index)| index)
            .ok_or(SimError::Pick {
                picked: 0,
                candidates: 0,
            })
    }
}

/// A finished run: its digest, its trace, how many steps it took and where its clock ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    /// Every decision, event and observation, folded.
    pub digest: Digest,
    /// Every decision, replayable.
    pub trace: Trace,
    /// Steps taken.
    pub steps: u64,
    /// Virtual time at the end.
    pub now: u64,
}

#[derive(Clone, Debug)]
struct Node {
    clock: Clock,
    /// The sum of the wall clock's steps.
    stepped_ns: i64,
    /// The timer as the node set it, on its monotonic clock.
    deadline: Option<u64>,
    lateness: u32,
}

/// The tag of an event folded into the digest.
const EVENT: u64 = 1;
/// The tag of a timer folded into the digest.
const WAKE: u64 = 2;
/// The tag of an observation folded into the digest.
const OBSERVED: u64 = 3;

/// The world. `E` is what the harness schedules: its messages, ticks, completions.
#[derive(Clone, Debug)]
pub struct World<E> {
    now: u64,
    steps: u64,
    discipline: Discipline,
    limits: Limits,
    queue: Queue<E>,
    wakes: Wakes,
    nodes: Vec<Node>,
    chooser: Chooser,
    schedule: u32,
    /// The one host clock read a run makes, for crates whose `now` is an `Instant`
    /// (`docs/sim.md` §3.2): only differences of the instants it gives may enter a decision.
    anchor: Anchor,
    tie_events: Vec<Key>,
    tie_wakes: Vec<Armed>,
    stack: Vec<usize>,
}

impl<E> World<E> {
    /// A world drawing from `source`, under `discipline`, within `limits`.
    pub fn new(source: Source, discipline: Discipline, limits: Limits) -> Result<Self, SimError> {
        let mut chooser = match source {
            Source::Seed(seed) => Chooser::recording(seed, limits.streams, limits.trace_words)?,
            Source::Trace(trace) => Chooser::replaying(trace, limits.streams),
        };
        let schedule = chooser.stream("schedule", &[])?;
        Ok(Self {
            now: 0,
            steps: 0,
            discipline,
            limits,
            queue: Queue::new(discipline, limits.events),
            wakes: Wakes::default(),
            nodes: Vec::new(),
            chooser,
            schedule,
            anchor: Anchor::new(),
            tie_events: Vec::new(),
            tie_wakes: Vec::new(),
            stack: Vec::new(),
        })
    }

    /// The present, in virtual nanoseconds from the world's start.
    pub fn now(&self) -> u64 {
        self.now
    }
    /// Steps taken.
    pub fn steps(&self) -> u64 {
        self.steps
    }
    /// The discipline in force.
    pub fn discipline(&self) -> Discipline {
        self.discipline
    }
    /// Events pending.
    pub fn pending(&self) -> usize {
        self.queue.len()
    }
    /// The digest of the events and observations so far (the decisions join it when the run
    /// finishes).
    pub fn digest(&self) -> Digest {
        self.chooser.digest
    }

    /// The words of trace so far: the decisions made.
    pub fn decisions(&self) -> usize {
        self.chooser.words()
    }

    /// The discipline changed, as a run moves from its safety phase to its liveness phase
    /// (`docs/sim.md` §3.8). Every pending event stays pending.
    pub fn set_discipline(&mut self, discipline: Discipline) {
        self.discipline = discipline;
        self.queue.switch(discipline);
    }

    /// A new stream named `label` and `parts`: a link's `("link", &[from, to])`, a node's own
    /// `("node", &[id])`. Its draws depend on the seed and the name alone.
    pub fn stream(&mut self, label: &'static str, parts: &[u64]) -> Result<StreamId, SimError> {
        self.chooser.stream(label, parts).map(StreamId)
    }

    /// Uniform in `[0, bound)` from `stream`: a decision, recorded.
    pub fn below(&mut self, stream: StreamId, bound: u64) -> Result<u64, SimError> {
        self.chooser.below(stream.0, bound)
    }

    /// True with probability `ppm` parts per million, from `stream`.
    pub fn chance(&mut self, stream: StreamId, ppm: u32) -> Result<bool, SimError> {
        Ok(self.below(stream, u64::from(PPM))? < u64::from(ppm))
    }

    /// An observation folded into the digest: what the harness will judge, so a run that
    /// observes differently digests differently.
    pub fn observe(&mut self, word: u64) {
        // As an event's two words, the tag in the low bits of the second.
        self.chooser.digest.fold(word);
        self.chooser.digest.fold(OBSERVED);
    }

    /// A new node with `clock`, its timer disarmed, and its own stream for its timers' lateness.
    pub fn node(&mut self, clock: Clock) -> Result<NodeId, SimError> {
        clock.check()?;
        let full = SimError::Full {
            what: "nodes",
            bound: self.limits.nodes,
        };
        if self.nodes.len() >= self.limits.nodes {
            return Err(full);
        }
        let id = u32::try_from(self.nodes.len()).map_err(|_| full)?;
        let lateness = self.chooser.stream("lateness", &[u64::from(id)])?;
        self.nodes.push(Node {
            clock,
            stepped_ns: 0,
            deadline: None,
            lateness,
        });
        self.wakes.add_node();
        Ok(NodeId(id))
    }

    fn held(&self, node: NodeId) -> Result<&Node, SimError> {
        usize::try_from(node.0)
            .ok()
            .and_then(|index| self.nodes.get(index))
            .ok_or(SimError::UnknownNode(node.0))
    }

    /// `node`'s monotonic clock now.
    pub fn monotonic(&self, node: NodeId) -> Result<u64, SimError> {
        self.held(node)?.clock.monotonic(self.now)
    }

    /// `node`'s wall clock now.
    pub fn wall(&self, node: NodeId) -> Result<u64, SimError> {
        let held = self.held(node)?;
        held.clock.wall(self.now, held.stepped_ns)
    }

    /// `node`'s monotonic clock now as an `Instant`: the world's anchor plus the reading.
    pub fn instant(&self, node: NodeId) -> Result<Instant, SimError> {
        self.anchor.instant(self.monotonic(node)?)
    }

    /// `node`'s wall clock stepped by `by_ns`, forward or back.
    pub fn step_wall(&mut self, node: NodeId, by_ns: i64) -> Result<(), SimError> {
        let now = self.now;
        let held = usize::try_from(node.0)
            .ok()
            .and_then(|index| self.nodes.get_mut(index))
            .ok_or(SimError::UnknownNode(node.0))?;
        let stepped = held
            .stepped_ns
            .checked_add(by_ns)
            .ok_or(SimError::TimeOverflow)?;
        held.clock.wall(now, stepped)?;
        held.stepped_ns = stepped;
        Ok(())
    }

    /// `node`'s timer set to fire when its monotonic clock reads `deadline`, late by a draw from
    /// its lateness; or disarmed. Setting the deadline it already has changes nothing and draws
    /// nothing, so a harness may set it after every poll.
    pub fn wake(&mut self, node: NodeId, deadline: Option<u64>) -> Result<(), SimError> {
        let now = self.now;
        let index = usize::try_from(node.0).map_err(|_| SimError::UnknownNode(node.0))?;
        let held = self
            .nodes
            .get_mut(index)
            .ok_or(SimError::UnknownNode(node.0))?;
        if held.deadline == deadline {
            return Ok(());
        }
        held.deadline = deadline;
        let (clock, lateness) = (held.clock, held.lateness);
        let at = match deadline {
            None => None,
            Some(local) => {
                let due = clock.virtual_at(local)?.max(now);
                let late = clock
                    .lateness
                    .floor_ns
                    .checked_add(self.chooser.below(lateness, clock.lateness.spread_ns)?);
                Some(
                    late.and_then(|late| due.checked_add(late))
                        .filter(|at| *at != DISARMED)
                        .ok_or(SimError::TimeOverflow)?,
                )
            }
        };
        self.wakes.set(node.0, at);
        Ok(())
    }

    /// `event` for `node`, due at virtual time `at`.
    pub fn schedule(&mut self, at: u64, node: NodeId, event: E) -> Result<(), SimError> {
        self.held(node)?;
        if at < self.now {
            return Err(SimError::InThePast { at, now: self.now });
        }
        self.queue.push(at, node.0, event)
    }

    /// `event` for `node`, due `delay_ns` from now.
    pub fn after(&mut self, delay_ns: u64, node: NodeId, event: E) -> Result<(), SimError> {
        let at = self
            .now
            .checked_add(delay_ns)
            .ok_or(SimError::TimeOverflow)?;
        self.schedule(at, node, event)
    }

    /// When the earliest pending event or armed timer is due: what a harness that runs until a
    /// time asks before each step. Constant time under the ordered discipline; under the free one
    /// a scan of the pending.
    pub fn earliest(&self) -> Option<u64> {
        let events = match self.discipline {
            Discipline::Ordered => self.queue.earliest(),
            Discipline::Free => self.queue.dense().iter().map(|key| key.at).min(),
        };
        events.into_iter().chain(self.wakes.min()).min()
    }

    /// The clock moved forward to `to` with nothing run: an idle stretch. Refused under the
    /// ordered discipline if something is due before `to`, which the jump would skip.
    pub fn advance(&mut self, to: u64) -> Result<(), SimError> {
        if self.discipline == Discipline::Ordered
            && let Some(due) = self.earliest()
            && due < to
        {
            return Err(SimError::Skips { due, to });
        }
        self.now = self.now.max(to);
        Ok(())
    }

    /// The next event or timer, as the discipline enables and `strategy` chooses; the clock moves
    /// to it (never backward).
    pub fn next<S: Strategy>(&mut self, strategy: &mut S) -> Result<Step<E>, SimError> {
        if self.steps >= self.limits.steps {
            return Ok(Step::Spent);
        }
        let taken = match self.discipline {
            Discipline::Ordered => self.next_ordered(strategy)?,
            Discipline::Free => self.next_free(strategy)?,
        };
        let Some(taken) = taken else {
            return Ok(Step::Idle);
        };
        self.steps = self.steps.saturating_add(1);
        let (at, node, tag) = match &taken {
            Taken::Event(key) => (key.at, key.node, EVENT),
            Taken::Wake(armed) => (armed.at, armed.node, WAKE),
        };
        self.now = self.now.max(at);
        let digest = &mut self.chooser.digest;
        digest.fold(at);
        digest.fold((u64::from(node) << 2) | tag);
        match taken {
            Taken::Event(key) => {
                let event = self.queue.take(key).ok_or(SimError::Pick {
                    picked: 0,
                    candidates: 0,
                })?;
                Ok(Step::Event {
                    node: NodeId(node),
                    event,
                })
            }
            Taken::Wake(armed) => {
                if let Some(held) = usize::try_from(armed.node)
                    .ok()
                    .and_then(|index| self.nodes.get_mut(index))
                {
                    held.deadline = None;
                }
                self.wakes.set(armed.node, None);
                Ok(Step::Wake { node: NodeId(node) })
            }
        }
    }

    fn next_ordered<S: Strategy>(&mut self, strategy: &mut S) -> Result<Option<Taken>, SimError> {
        let (event_at, wake_at) = (self.queue.earliest(), self.wakes.min());
        let Some(at) = event_at.into_iter().chain(wake_at).min() else {
            return Ok(None);
        };
        // Most steps have no tie: one event or one timer due alone needs no choice.
        if wake_at != Some(at)
            && let Some(key) = self.queue.pop_alone(at)
        {
            return Ok(Some(Taken::Event(key)));
        }
        if event_at != Some(at)
            && let Some(armed) = self.wakes.alone_at(at)
        {
            return Ok(Some(Taken::Wake(armed)));
        }
        self.tie_events.clear();
        self.tie_wakes.clear();
        if event_at == Some(at) {
            self.queue.pop_at(at, &mut self.tie_events);
        }
        if wake_at == Some(at) {
            self.wakes
                .collect_at(at, &mut self.tie_wakes, &mut self.stack);
        }
        let choice = Choice {
            now: self.now,
            events: &self.tie_events,
            wakes: &self.tie_wakes,
        };
        let picked = match pick(strategy, &choice, &mut self.chooser, self.schedule) {
            Ok(picked) => picked,
            Err(error) => {
                // Nothing is taken: the ties go back among the pending.
                for key in &self.tie_events {
                    self.queue.insert(*key);
                }
                return Err(error);
            }
        };
        let mut taken = None;
        for (index, key) in self.tie_events.iter().enumerate() {
            if index == picked {
                taken = Some(Taken::Event(*key));
            } else {
                self.queue.insert(*key);
            }
        }
        if taken.is_none() {
            let armed = picked
                .checked_sub(self.tie_events.len())
                .and_then(|index| self.tie_wakes.get(index));
            taken = armed.map(|armed| Taken::Wake(*armed));
        }
        Ok(taken)
    }

    fn next_free<S: Strategy>(&mut self, strategy: &mut S) -> Result<Option<Taken>, SimError> {
        self.tie_wakes.clear();
        self.tie_wakes.extend(self.wakes.armed());
        self.tie_wakes.sort_unstable_by_key(|armed| armed.node);
        let choice = Choice {
            now: self.now,
            events: self.queue.dense(),
            wakes: &self.tie_wakes,
        };
        if choice.is_empty() {
            return Ok(None);
        }
        let picked = pick(strategy, &choice, &mut self.chooser, self.schedule)?;
        let events = self.queue.dense().len();
        if picked < events {
            return Ok(self.queue.remove_dense(picked).map(Taken::Event));
        }
        Ok(picked
            .checked_sub(events)
            .and_then(|index| self.tie_wakes.get(index))
            .map(|armed| Taken::Wake(*armed)))
    }

    /// The run's record: its digest, its trace, its steps and its clock.
    pub fn finish(self) -> Record {
        let (trace, digest) = self.chooser.finish();
        Record {
            digest,
            steps: self.steps,
            now: self.now,
            trace,
        }
    }
}

enum Taken {
    Event(Key),
    Wake(Armed),
}

/// The strategy's pick among two or more, checked; the only one without asking.
fn pick<S: Strategy>(
    strategy: &mut S,
    choice: &Choice<'_>,
    chooser: &mut Chooser,
    schedule: u32,
) -> Result<usize, SimError> {
    let candidates = choice.len();
    if candidates <= 1 {
        return Ok(0);
    }
    let mut draw = Draw {
        chooser,
        stream: schedule,
    };
    let picked = strategy.pick(choice, &mut draw)?;
    if picked >= candidates {
        return Err(SimError::Pick { picked, candidates });
    }
    Ok(picked)
}
