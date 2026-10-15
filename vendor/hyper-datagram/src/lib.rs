//! The sealed datagram plane: consensus control and membership messages on a UDP socket of their
//! own, beside the QUIC transport (mantle `docs/design/node.md` §3.4; note 32 §3.5).
//!
//! **Why a separate socket.** QUIC's unreliable datagrams share the connection's congestion
//! window (RFC 9221 §5), so a vote sent that way waits behind bulk transfers. The plane escapes
//! that, and takes on what a transport would have done (RFC 8085):
//! - at most one packed datagram to a peer per [`Plane::flush`], which the owner calls once a
//!   heartbeat, so the plane originates no more than one datagram per round trip;
//! - nothing retransmitted, since Raft retries and SWIM probes again;
//! - no datagram larger than the path allows ([`Plane::set_path`]), and before a path is
//!   measured no larger than RFC 8085's IPv4 fallback, 576 bytes.
//!
//! **Keys.** Each epoch is one QUIC connection to the peer. Its keys are expanded from that
//! connection's TLS exporter ([`schedule`]), one per direction, so a new connection always brings
//! new keys and a counter can never repeat under a key.
//!
//! **The wire.**
//! - A cleartext prologue holds the version, the sender, the epoch and the counter. It is the
//!   AEAD's associated data, and the counter is its nonce.
//! - Then the AES-256-GCM sealed body: a CRC-32C of the messages (every payload carries its
//!   checksum, mantle CLAUDE.md §6), then the messages, each with a two-byte length.
//!
//! **Acceptance order** ([`Plane::open`]), from slates' codec and RFC 4303:
//! 1. length;
//! 2. prologue;
//! 3. a known sender and epoch, and the owner's fence;
//! 4. the replay window;
//! 5. the key's integrity limit;
//! 6. the tag;
//! 7. only then the window moves, the checksum is checked and the messages are read.
//!
//! An unknown sender, a fenced epoch or a replay is refused before any cryptography.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cast_possible_truncation,
        clippy::cognitive_complexity
    )
)]

mod schedule;
mod window;

use std::collections::{HashMap, VecDeque};

use aws_lc_rs::aead::{Aad, LessSafeKey, NONCE_LEN, Nonce};

pub use schedule::{EXPORTER_LABEL, ExporterSecret, Role, SECRET_BYTES};
use schedule::{EpochKeys, epoch_keys};
pub use window::DEFAULT_WIDTH;
use window::ReplayWindow;

/// A peer's identity, as the owner names it.
pub type PeerId = u64;
/// A connection epoch: the counter of QUIC connections to one peer, rising with each.
pub type Epoch = u32;

/// The wire format's version. Format.
pub const VERSION: u8 = 1;
/// The cleartext prologue: version, sender, epoch, counter.
pub const PROLOGUE_BYTES: usize = 1 + 8 + 4 + 8;
/// The AES-256-GCM tag (RFC 5116 §5.3; NIST SP 800-38D).
pub const TAG_BYTES: usize = 16;
/// The CRC-32C at the head of the sealed body.
const CHECKSUM_BYTES: usize = 4;
/// Each message's length prefix.
pub const LENGTH_BYTES: usize = 2;
/// What every datagram spends besides its messages.
pub const OVERHEAD_BYTES: usize = PROLOGUE_BYTES + CHECKSUM_BYTES + TAG_BYTES;
/// RFC 8085 §3.2's fallback when a path is unmeasured: IPv4's 576-byte minimum reassembly size,
/// which IPv6's 1,280-byte minimum link MTU also carries. A datagram is never larger before
/// [`Plane::set_path`] reports the path.
pub const UNMEASURED_DATAGRAM_BYTES: usize = 576;
/// The largest UDP payload over IPv6 (RFC 2675 aside): 65,535 less the 8-byte UDP header.
pub const MAX_DATAGRAM_BYTES: usize = 65_527;

/// RFC 9001 §6.6 and Appendix B.1.1: AEAD_AES_256_GCM's confidentiality limit, the most
/// datagrams one key may seal (2^23). Past it the plane refuses until a new epoch.
pub const CONFIDENTIALITY_LIMIT: u64 = 1 << 23;
/// RFC 9001 §6.6 and Appendix B.1.2: AEAD_AES_256_GCM's integrity limit, the most failed opens
/// one key may suffer (2^52). Past it the plane refuses everything under the key.
pub const INTEGRITY_LIMIT: u64 = 1 << 52;

/// Why the plane refused: every failure is one of these, never a panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A limit given to [`Plane::new`] is zero.
    Limits,
    /// The plane already holds [`PlaneLimits::max_peers`] peers.
    TooManyPeers,
    /// The peer has no epoch installed.
    UnknownPeer,
    /// The epoch is not newer than the peer's newest: epochs only rise.
    EpochNotNewer,
    /// The key schedule could not expand the secret.
    KeySchedule,
    /// The message can never fit one datagram on this path.
    TooLarge,
    /// The peer's next datagram is full: flush, then queue again.
    Busy,
    /// The datagram is shorter than a prologue, a checksum and a tag.
    Truncated,
    /// The version is not one this build reads.
    BadVersion,
    /// The datagram claims to be from this node.
    FromSelf,
    /// The sender is known but not under this epoch.
    UnknownEpoch,
    /// The owner's fence refuses this sender's epoch.
    Fenced,
    /// The counter is left of the replay window.
    Stale,
    /// The counter was already accepted.
    Replay,
    /// The epoch's key has sealed its confidentiality limit.
    CounterExhausted,
    /// The epoch's key has failed its integrity limit of opens.
    IntegrityExhausted,
    /// The tag did not verify: a forgery, a corruption or the wrong key.
    BadSeal,
    /// The tag verified but the checksum did not: corruption after sealing or after opening, a
    /// fault to report, not a forgery.
    Corrupt,
    /// The tag and checksum verified but the messages do not parse.
    Malformed,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, formatter)
    }
}

impl std::error::Error for Refusal {}

/// The plane's bounds.
#[derive(Clone, Copy, Debug)]
pub struct PlaneLimits {
    /// The most peers the plane keeps keys for.
    pub max_peers: usize,
    /// The most epochs one peer keeps; installing past it retires the oldest. Two let a new
    /// connection's datagrams and the old one's last overlap for a handshake round trip.
    pub epochs_per_peer: usize,
    /// The widest a replay window may grow, in counters.
    pub window_limit: usize,
}

/// The owner's view of who may send: the sender's current epoch, and any term it fences.
pub trait Fence {
    /// Whether datagrams from `sender` under `epoch` are admitted.
    fn admits(&self, sender: PeerId, epoch: Epoch) -> bool;
}

/// A fence that admits every installed epoch.
pub struct AdmitAll;

impl Fence for AdmitAll {
    fn admits(&self, _: PeerId, _: Epoch) -> bool {
        true
    }
}

struct EpochState {
    epoch: Epoch,
    seal: LessSafeKey,
    open: LessSafeKey,
    /// The next counter this node seals with.
    next: u64,
    window: ReplayWindow,
    failures: u64,
}

struct Peer {
    /// Oldest first; sealing uses the newest.
    epochs: VecDeque<EpochState>,
    max_datagram: usize,
    /// The messages queued for the next datagram, each with its length prefix.
    pending: Vec<u8>,
}

impl Peer {
    fn epoch_mut(&mut self, epoch: Epoch) -> Option<&mut EpochState> {
        self.epochs.iter_mut().find(|state| state.epoch == epoch)
    }

    /// The room the messages of one datagram have on this path.
    fn message_room(&self) -> usize {
        self.max_datagram.saturating_sub(OVERHEAD_BYTES)
    }
}

/// An opened datagram: its sender, its epoch, and its messages.
#[derive(Debug)]
pub struct Opened<'a> {
    /// Who sealed it.
    pub sender: PeerId,
    /// Under which epoch.
    pub epoch: Epoch,
    body: &'a [u8],
}

impl<'a> Opened<'a> {
    /// The messages, in the order they were queued.
    pub fn messages(&self) -> Messages<'a> {
        Messages { rest: self.body }
    }
}

/// The messages of an [`Opened`] datagram, validated when it was opened.
#[derive(Debug)]
pub struct Messages<'a> {
    rest: &'a [u8],
}

impl<'a> Iterator for Messages<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let (message, rest) = split_message(self.rest)?;
        self.rest = rest;
        Some(message)
    }
}

/// The first length-prefixed message of `bytes` and what follows it, or `None` if `bytes` is
/// empty or ends inside a message.
fn split_message(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (length, rest) = bytes.split_first_chunk::<LENGTH_BYTES>()?;
    let length = usize::from(u16::from_le_bytes(*length));
    if rest.len() < length {
        return None;
    }
    Some(rest.split_at(length))
}

/// The plane: per-peer epochs, keys, replay windows and pending messages.
pub struct Plane {
    local: PeerId,
    limits: PlaneLimits,
    peers: HashMap<PeerId, Peer>,
    /// The datagram being sealed, reused across flushes.
    scratch: Vec<u8>,
}

impl Plane {
    /// A plane for `local` within `limits`.
    pub fn new(local: PeerId, limits: PlaneLimits) -> Result<Self, Refusal> {
        if limits.max_peers == 0 || limits.epochs_per_peer == 0 || limits.window_limit == 0 {
            return Err(Refusal::Limits);
        }
        Ok(Self {
            local,
            limits,
            peers: HashMap::new(),
            scratch: Vec::new(),
        })
    }

    /// Installs the epoch a new QUIC connection to `peer` gives, with keys from `secret` for this
    /// node's `role` on that connection. Past [`PlaneLimits::epochs_per_peer`] the oldest epoch is
    /// retired.
    pub fn install_epoch(
        &mut self,
        peer: PeerId,
        epoch: Epoch,
        secret: &ExporterSecret,
        role: Role,
    ) -> Result<(), Refusal> {
        if !self.peers.contains_key(&peer) && self.peers.len() >= self.limits.max_peers {
            return Err(Refusal::TooManyPeers);
        }
        let EpochKeys { seal, open } = epoch_keys(secret, role)?;
        let window_limit = self.limits.window_limit;
        let epochs_per_peer = self.limits.epochs_per_peer;
        let state = self.peers.entry(peer).or_insert_with(|| Peer {
            epochs: VecDeque::with_capacity(epochs_per_peer),
            max_datagram: UNMEASURED_DATAGRAM_BYTES,
            pending: Vec::new(),
        });
        if state
            .epochs
            .back()
            .is_some_and(|newest| newest.epoch >= epoch)
        {
            return Err(Refusal::EpochNotNewer);
        }
        if state.epochs.len() >= epochs_per_peer {
            state.epochs.pop_front();
        }
        state.epochs.push_back(EpochState {
            epoch,
            seal,
            open,
            next: 0,
            window: ReplayWindow::new(window_limit),
            failures: 0,
        });
        Ok(())
    }

    /// Retires one epoch of `peer`: its datagrams are refused from now on.
    pub fn retire(&mut self, peer: PeerId, epoch: Epoch) {
        if let Some(state) = self.peers.get_mut(&peer) {
            state.epochs.retain(|held| held.epoch != epoch);
        }
    }

    /// Forgets `peer` entirely, with its pending messages.
    pub fn remove_peer(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
    }

    /// The largest datagram the path to `peer` carries, from the transport's measurement of it,
    /// clamped to what UDP can carry at most and what the plane's overhead needs at least.
    pub fn set_path(&mut self, peer: PeerId, max_datagram: usize) -> Result<(), Refusal> {
        let state = self.peers.get_mut(&peer).ok_or(Refusal::UnknownPeer)?;
        state.max_datagram = max_datagram.clamp(
            OVERHEAD_BYTES.saturating_add(LENGTH_BYTES),
            MAX_DATAGRAM_BYTES,
        );
        Ok(())
    }

    /// Queues `message` for `peer`'s next datagram. Refused with [`Refusal::TooLarge`] if it can
    /// never fit one, and with [`Refusal::Busy`] if it does not fit beside what is queued.
    pub fn queue(&mut self, peer: PeerId, message: &[u8]) -> Result<(), Refusal> {
        let state = self.peers.get_mut(&peer).ok_or(Refusal::UnknownPeer)?;
        if state.epochs.is_empty() {
            return Err(Refusal::UnknownPeer);
        }
        let length = u16::try_from(message.len()).map_err(|_| Refusal::TooLarge)?;
        let framed = message
            .len()
            .checked_add(LENGTH_BYTES)
            .ok_or(Refusal::TooLarge)?;
        let room = state.message_room();
        if framed > room {
            return Err(Refusal::TooLarge);
        }
        if state.pending.len().saturating_add(framed) > room {
            return Err(Refusal::Busy);
        }
        if state.pending.capacity() < room {
            state
                .pending
                .reserve_exact(room.saturating_sub(state.pending.len()));
        }
        state.pending.extend_from_slice(&length.to_le_bytes());
        state.pending.extend_from_slice(message);
        Ok(())
    }

    /// Seals each peer's queued messages into one datagram and hands it to `out`, or hands the
    /// refusal that stopped it; the messages are dropped either way, since nothing is
    /// retransmitted. At most one datagram per peer per call.
    pub fn flush<F: FnMut(PeerId, Result<&[u8], Refusal>)>(&mut self, mut out: F) {
        let local = self.local;
        for (&peer, state) in &mut self.peers {
            if state.pending.is_empty() {
                continue;
            }
            let sealed = seal(local, state, &mut self.scratch);
            state.pending.clear();
            match sealed {
                Ok(()) => out(peer, Ok(&self.scratch)),
                Err(refusal) => out(peer, Err(refusal)),
            }
        }
    }

    /// Opens `datagram` in place, in the acceptance order, and returns its messages.
    pub fn open<'a>(
        &mut self,
        datagram: &'a mut [u8],
        fence: &dyn Fence,
    ) -> Result<Opened<'a>, Refusal> {
        if datagram.len() < OVERHEAD_BYTES {
            return Err(Refusal::Truncated);
        }
        let (prologue, sealed) = datagram
            .split_at_mut_checked(PROLOGUE_BYTES)
            .ok_or(Refusal::Truncated)?;
        let Prologue {
            sender,
            epoch,
            counter,
        } = Prologue::read(prologue)?;
        if sender == self.local {
            return Err(Refusal::FromSelf);
        }
        let peer = self.peers.get_mut(&sender).ok_or(Refusal::UnknownPeer)?;
        let state = peer.epoch_mut(epoch).ok_or(Refusal::UnknownEpoch)?;
        if !fence.admits(sender, epoch) {
            return Err(Refusal::Fenced);
        }
        state.window.check(counter)?;
        if state.failures >= INTEGRITY_LIMIT {
            return Err(Refusal::IntegrityExhausted);
        }
        let opened = state
            .open
            .open_in_place(nonce(counter), Aad::from(&*prologue), sealed);
        let Ok(plain) = opened else {
            state.failures = state.failures.saturating_add(1);
            return Err(Refusal::BadSeal);
        };
        state.window.accept(counter);
        let (checksum, body) = plain
            .split_first_chunk::<CHECKSUM_BYTES>()
            .ok_or(Refusal::Truncated)?;
        if u32::from_le_bytes(*checksum) != crc32c::crc32c(body) {
            return Err(Refusal::Corrupt);
        }
        let mut rest: &[u8] = body;
        while !rest.is_empty() {
            let (_, after) = split_message(rest).ok_or(Refusal::Malformed)?;
            rest = after;
        }
        Ok(Opened {
            sender,
            epoch,
            body,
        })
    }

    /// The replay window's current width for `peer` under `epoch`, in counters.
    pub fn window_width(&self, peer: PeerId, epoch: Epoch) -> Option<usize> {
        self.peers
            .get(&peer)?
            .epochs
            .iter()
            .find(|state| state.epoch == epoch)
            .map(|state| state.window.width())
    }
}

/// The cleartext prologue.
struct Prologue {
    sender: PeerId,
    epoch: Epoch,
    counter: u64,
}

impl Prologue {
    fn write(&self, out: &mut Vec<u8>) {
        out.push(VERSION);
        out.extend_from_slice(&self.sender.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.counter.to_le_bytes());
    }

    fn read(bytes: &[u8]) -> Result<Self, Refusal> {
        let (version, rest) = bytes.split_first().ok_or(Refusal::Truncated)?;
        if *version != VERSION {
            return Err(Refusal::BadVersion);
        }
        let (sender, rest) = rest.split_first_chunk::<8>().ok_or(Refusal::Truncated)?;
        let (epoch, rest) = rest.split_first_chunk::<4>().ok_or(Refusal::Truncated)?;
        let (counter, _) = rest.split_first_chunk::<8>().ok_or(Refusal::Truncated)?;
        Ok(Self {
            sender: u64::from_le_bytes(*sender),
            epoch: u32::from_le_bytes(*epoch),
            counter: u64::from_le_bytes(*counter),
        })
    }
}

/// The 96-bit nonce for `counter`: the counter little-endian, then zeros. Each direction of each
/// epoch has its own key, so the counter alone is unique per key.
fn nonce(counter: u64) -> Nonce {
    let mut bytes = [0u8; NONCE_LEN];
    if let Some(head) = bytes.first_chunk_mut::<8>() {
        *head = counter.to_le_bytes();
    }
    Nonce::assume_unique_for_key(bytes)
}

/// Seals `state`'s pending messages under its newest epoch into `out`.
fn seal(local: PeerId, state: &mut Peer, out: &mut Vec<u8>) -> Result<(), Refusal> {
    let epoch = state.epochs.back_mut().ok_or(Refusal::UnknownPeer)?;
    let counter = epoch.next;
    if counter >= CONFIDENTIALITY_LIMIT {
        return Err(Refusal::CounterExhausted);
    }
    out.clear();
    out.reserve(OVERHEAD_BYTES.saturating_add(state.pending.len()));
    Prologue {
        sender: local,
        epoch: epoch.epoch,
        counter,
    }
    .write(out);
    out.extend_from_slice(&crc32c::crc32c(&state.pending).to_le_bytes());
    out.extend_from_slice(&state.pending);
    let (prologue, body) = out
        .split_at_mut_checked(PROLOGUE_BYTES)
        .ok_or(Refusal::Truncated)?;
    let tag = epoch
        .seal
        .seal_in_place_separate_tag(nonce(counter), Aad::from(&*prologue), body)
        .map_err(|_| Refusal::BadSeal)?;
    out.extend_from_slice(tag.as_ref());
    epoch.next = counter.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: PlaneLimits = PlaneLimits {
        max_peers: 4,
        epochs_per_peer: 2,
        window_limit: 256,
    };
    const A: PeerId = 1;
    const B: PeerId = 2;

    fn secret(byte: u8) -> ExporterSecret {
        ExporterSecret::new([byte; SECRET_BYTES])
    }

    /// Two planes joined by one connection's epoch: `a` initiated it, `b` accepted it.
    fn pair(epoch: Epoch) -> (Plane, Plane) {
        let mut a = Plane::new(A, LIMITS).unwrap();
        let mut b = Plane::new(B, LIMITS).unwrap();
        a.install_epoch(B, epoch, &secret(7), Role::Initiator)
            .unwrap();
        b.install_epoch(A, epoch, &secret(7), Role::Acceptor)
            .unwrap();
        (a, b)
    }

    fn flushed(plane: &mut Plane) -> Vec<(PeerId, Vec<u8>)> {
        let mut out = Vec::new();
        plane.flush(|peer, datagram: Result<&[u8], Refusal>| {
            out.push((peer, datagram.unwrap().to_vec()));
        });
        out
    }

    fn messages(plane: &mut Plane, datagram: &mut [u8]) -> Result<Vec<Vec<u8>>, Refusal> {
        let opened = plane.open(datagram, &AdmitAll)?;
        Ok(opened.messages().map(<[u8]>::to_vec).collect())
    }

    #[test]
    fn queued_messages_arrive_in_one_datagram_in_order() {
        let (mut a, mut b) = pair(1);
        a.queue(B, b"vote").unwrap();
        a.queue(B, b"").unwrap();
        a.queue(B, b"heartbeat").unwrap();
        let mut out = flushed(&mut a);
        assert_eq!(out.len(), 1, "one datagram per peer per flush");
        let (peer, datagram) = out.pop().unwrap();
        assert_eq!(peer, B);
        assert_eq!(datagram.len(), OVERHEAD_BYTES + 3 * 2 + 4 + 9);
        let mut datagram = datagram;
        let opened = b.open(&mut datagram, &AdmitAll).unwrap();
        assert_eq!((opened.sender, opened.epoch), (A, 1));
        let got: Vec<&[u8]> = opened.messages().collect();
        assert_eq!(got, vec![&b"vote"[..], b"", b"heartbeat"]);
        assert!(flushed(&mut a).is_empty(), "nothing is retransmitted");
    }

    #[test]
    fn both_directions_use_their_own_keys() {
        let (mut a, mut b) = pair(1);
        b.queue(A, b"reply").unwrap();
        let (_, mut datagram) = flushed(&mut b).pop().unwrap();
        assert_eq!(
            messages(&mut a, &mut datagram).unwrap(),
            vec![b"reply".to_vec()]
        );
        // A datagram reflected back at its sender is from itself: refused before any key is used.
        a.queue(B, b"x").unwrap();
        let (_, mut sent) = flushed(&mut a).pop().unwrap();
        assert_eq!(messages(&mut a, &mut sent.clone()), Err(Refusal::FromSelf));
        assert!(messages(&mut b, &mut sent).is_ok());
    }

    #[test]
    fn a_replayed_datagram_is_refused_before_the_tag() {
        let (mut a, mut b) = pair(1);
        a.queue(B, b"once").unwrap();
        let (_, datagram) = flushed(&mut a).pop().unwrap();
        assert!(messages(&mut b, &mut datagram.clone()).is_ok());
        assert_eq!(
            messages(&mut b, &mut datagram.clone()),
            Err(Refusal::Replay)
        );
    }

    #[test]
    fn reordered_datagrams_inside_the_window_are_each_accepted_once() {
        let (mut a, mut b) = pair(1);
        let mut sent = Vec::new();
        for i in 0u8..10 {
            a.queue(B, &[i]).unwrap();
            sent.push(flushed(&mut a).pop().unwrap().1);
        }
        for i in [9usize, 3, 0, 7, 1, 2, 8, 4, 6, 5] {
            let mut datagram = sent[i].clone();
            assert_eq!(
                messages(&mut b, &mut datagram).unwrap(),
                vec![vec![i as u8]]
            );
        }
        for datagram in &sent {
            assert_eq!(
                messages(&mut b, &mut datagram.clone()),
                Err(Refusal::Replay)
            );
        }
    }

    #[test]
    fn a_tampered_prologue_or_body_fails_the_tag_and_does_not_move_the_window() {
        let (mut a, mut b) = pair(1);
        a.queue(B, b"payload").unwrap();
        let (_, datagram) = flushed(&mut a).pop().unwrap();
        // A forged high counter: the prologue is the AAD, so the tag fails.
        let mut forged = datagram.clone();
        forged[1 + 8 + 4..PROLOGUE_BYTES].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(messages(&mut b, &mut forged), Err(Refusal::BadSeal));
        let mut flipped = datagram.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 1;
        assert_eq!(messages(&mut b, &mut flipped), Err(Refusal::BadSeal));
        // The honest datagram still opens: the forgeries moved nothing.
        assert!(messages(&mut b, &mut datagram.clone()).is_ok());
    }

    #[test]
    fn the_acceptance_order_refuses_cheaply_first() {
        let (mut a, mut b) = pair(1);
        a.queue(B, b"m").unwrap();
        let (_, datagram) = flushed(&mut a).pop().unwrap();
        assert_eq!(
            messages(&mut b, &mut datagram[..10].to_vec()),
            Err(Refusal::Truncated)
        );
        let mut version = datagram.clone();
        version[0] = VERSION + 1;
        assert_eq!(messages(&mut b, &mut version), Err(Refusal::BadVersion));
        let mut stranger = datagram.clone();
        stranger[1..9].copy_from_slice(&99u64.to_le_bytes());
        assert_eq!(messages(&mut b, &mut stranger), Err(Refusal::UnknownPeer));
        let mut other_epoch = datagram.clone();
        other_epoch[9..13].copy_from_slice(&9u32.to_le_bytes());
        assert_eq!(
            messages(&mut b, &mut other_epoch),
            Err(Refusal::UnknownEpoch)
        );
        struct Fenced;
        impl Fence for Fenced {
            fn admits(&self, _: PeerId, _: Epoch) -> bool {
                false
            }
        }
        assert_eq!(
            b.open(&mut datagram.clone(), &Fenced).map(|_| ()),
            Err(Refusal::Fenced)
        );
    }

    #[test]
    fn a_new_epoch_seals_under_new_keys_and_the_old_one_overlaps_until_retired() {
        let (mut a, mut b) = pair(1);
        a.queue(B, b"old").unwrap();
        let (_, old) = flushed(&mut a).pop().unwrap();
        // A restart: a new connection, a new epoch, the counter at zero again under new keys.
        a.install_epoch(B, 2, &secret(8), Role::Initiator).unwrap();
        b.install_epoch(A, 2, &secret(8), Role::Acceptor).unwrap();
        a.queue(B, b"new").unwrap();
        let (_, new) = flushed(&mut a).pop().unwrap();
        assert_eq!(new[9..13], 2u32.to_le_bytes());
        assert_eq!(new[13..21], 0u64.to_le_bytes(), "the counter restarts");
        assert_ne!(old[PROLOGUE_BYTES..], new[PROLOGUE_BYTES..]);
        assert!(messages(&mut b, &mut new.clone()).is_ok());
        assert!(
            messages(&mut b, &mut old.clone()).is_ok(),
            "the old epoch overlaps"
        );
        b.retire(A, 1);
        assert_eq!(
            messages(&mut b, &mut old.clone()),
            Err(Refusal::UnknownEpoch)
        );
        assert_eq!(
            a.install_epoch(B, 2, &secret(9), Role::Initiator),
            Err(Refusal::EpochNotNewer)
        );
    }

    #[test]
    fn epochs_past_the_limit_retire_the_oldest() {
        let (mut a, _) = pair(1);
        a.install_epoch(B, 2, &secret(2), Role::Initiator).unwrap();
        a.install_epoch(B, 3, &secret(3), Role::Initiator).unwrap();
        assert!(a.window_width(B, 1).is_none());
        assert!(a.window_width(B, 2).is_some());
        assert!(a.window_width(B, 3).is_some());
    }

    #[test]
    fn peers_and_datagrams_are_bounded() {
        let mut a = Plane::new(A, LIMITS).unwrap();
        for peer in 10..14 {
            a.install_epoch(peer, 1, &secret(1), Role::Initiator)
                .unwrap();
        }
        assert_eq!(
            a.install_epoch(99, 1, &secret(1), Role::Initiator),
            Err(Refusal::TooManyPeers)
        );
        let room = UNMEASURED_DATAGRAM_BYTES - OVERHEAD_BYTES - 2;
        assert_eq!(a.queue(10, &vec![0; room + 1]), Err(Refusal::TooLarge));
        a.queue(10, &vec![0; room]).unwrap();
        assert_eq!(a.queue(10, b""), Err(Refusal::Busy));
        let out = flushed(&mut a);
        assert_eq!(
            out[0].1.len(),
            UNMEASURED_DATAGRAM_BYTES,
            "exactly the unmeasured size"
        );
        a.set_path(10, 1_200).unwrap();
        a.queue(10, &vec![0; 1_200 - OVERHEAD_BYTES - 2]).unwrap();
        assert_eq!(a.queue(99, b"x"), Err(Refusal::UnknownPeer));
        assert_eq!(
            Plane::new(
                A,
                PlaneLimits {
                    max_peers: 0,
                    ..LIMITS
                }
            )
            .map(|_| ()),
            Err(Refusal::Limits)
        );
    }

    #[test]
    fn a_key_refuses_past_its_confidentiality_limit() {
        let (mut a, _) = pair(1);
        a.peers.get_mut(&B).unwrap().epochs.back_mut().unwrap().next = CONFIDENTIALITY_LIMIT;
        a.queue(B, b"x").unwrap();
        let mut refusals = Vec::new();
        a.flush(|_, datagram: Result<&[u8], Refusal>| refusals.push(datagram.err()));
        assert_eq!(refusals, vec![Some(Refusal::CounterExhausted)]);
    }

    #[test]
    fn corruption_after_sealing_is_named_corruption_not_forgery() {
        let (mut a, mut b) = pair(1);
        // The checksum covers the message as queued; one byte changes before the seal, as memory
        // corruption between checksum and seal would change it. The tag verifies, the checksum
        // does not.
        let queued = [4, 0, b'd', b'a', b't', b'a'];
        let mut forged = Vec::new();
        Prologue {
            sender: A,
            epoch: 1,
            counter: 0,
        }
        .write(&mut forged);
        forged.extend_from_slice(&crc32c::crc32c(&queued).to_le_bytes());
        forged.extend_from_slice(&[4, 0, b'd', b'a', b't', b'X']);
        let epoch = a.peers.get_mut(&B).unwrap().epochs.back_mut().unwrap();
        let (prologue, body) = forged.split_at_mut(PROLOGUE_BYTES);
        let tag = epoch
            .seal
            .seal_in_place_separate_tag(nonce(0), Aad::from(&*prologue), body)
            .unwrap();
        forged.extend_from_slice(tag.as_ref());
        assert_eq!(messages(&mut b, &mut forged), Err(Refusal::Corrupt));
    }

    /// The wire, pinned: a fixed secret, role, epoch and message seal to these exact bytes. A change
    /// to the prologue, the key schedule, the nonce, the checksum or the framing fails this test.
    /// The bytes were computed independently, from RFC 8446 §7.1, RFC 5116 and the CRC-32C
    /// definition, with Python's `cryptography` package (HKDFExpand, AESGCM) and a bitwise CRC-32C
    /// that reproduces its check value 0xE3069283, and they matched this implementation exactly.
    #[test]
    fn a_sealed_datagram_matches_its_golden_vector() {
        let (mut a, _) = pair(1);
        a.queue(B, b"golden").unwrap();
        let (_, datagram) = flushed(&mut a).pop().unwrap();
        let hex: String = datagram.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, GOLDEN);
    }

    const GOLDEN: &str = "0101000000000000000100000000000000000000009923f0f82a6b9c2b8988cd234fdf5c49c078dbb0121203e6f1579011";
}
