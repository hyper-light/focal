use std::{
    collections::{BTreeMap, VecDeque},
    mem,
    ops::{Bound, Range},
};

use rand::{Rng, RngExt};
use rustc_hash::FxHashSet;
use tracing::trace;

use super::assembler::Assembler;
use crate::{
    Dir, Duration, Instant, PerDir, SocketAddr, StreamId, TransportError, VarInt,
    cid_queue::CidQueue, connection::StreamsState, crypto::Keys, frame, packet::SpaceId,
    range_set::ArrayRangeSet, shared::IssuedCid,
};

pub(super) struct PacketSpace {
    pub(super) crypto: Option<Keys>,
    pub(super) dedup: Dedup,
    /// Highest received packet number
    pub(super) rx_packet: u64,

    /// Data to send
    pub(super) pending: Retransmits,
    /// Packet numbers to acknowledge
    pub(super) pending_acks: PendingAcks,

    /// The packet number of the next packet that will be sent, if any. In the Data space, the
    /// packet number stored here is sometimes skipped by [`PacketNumberFilter`] logic.
    pub(super) next_packet_number: u64,
    /// The largest packet number the remote peer acknowledged in an ACK frame.
    pub(super) largest_acked_packet: Option<u64>,
    pub(super) largest_acked_packet_sent: Instant,
    /// The highest-numbered ACK-eliciting packet we've sent
    pub(super) largest_ack_eliciting_sent: u64,
    /// Number of packets in `sent_packets` with numbers above `largest_ack_eliciting_sent`
    pub(super) unacked_non_ack_eliciting_tail: u64,
    /// Transmitted but not acked
    // We use a BTreeMap here so we can efficiently query by range on ACK and for loss detection
    pub(super) sent_packets: BTreeMap<u64, SentPacket>,
    /// Number of explicit congestion notification codepoints seen on incoming packets
    pub(super) ecn_counters: frame::EcnCounts,
    /// Recent ECN counters sent by the peer in ACK frames
    ///
    /// Updated (and inspected) whenever we receive an ACK with a new highest acked packet
    /// number. Stored per-space to simplify verification, which would otherwise have difficulty
    /// distinguishing between ECN bleaching and counts having been updated by a near-simultaneous
    /// ACK already processed in another space.
    pub(super) ecn_feedback: frame::EcnCounts,

    /// Incoming cryptographic handshake stream
    pub(super) crypto_stream: Assembler,
    /// Current offset of outgoing cryptographic handshake stream
    pub(super) crypto_offset: u64,

    /// The time the most recently sent retransmittable packet was sent.
    pub(super) time_of_last_ack_eliciting_packet: Option<Instant>,
    /// The time at which the earliest sent packet in this space will be considered lost based on
    /// exceeding the reordering window in time. Only set for packets numbered prior to a packet
    /// that has been acknowledged.
    pub(super) loss_time: Option<Instant>,
    /// Number of tail loss probes to send
    pub(super) loss_probes: u32,
    /// Whether the packets sent now carry copies ([`PacketSpace::queue_copies`]), which are not
    /// copied again
    pub(super) sending_copies: bool,
    pub(super) ping_pending: bool,
    pub(super) immediate_ack_pending: bool,
    /// Number of packets sent in the current key phase
    pub(super) sent_with_keys: u64,
}

impl PacketSpace {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            crypto: None,
            dedup: Dedup::new(),
            rx_packet: 0,

            pending: Retransmits::default(),
            pending_acks: PendingAcks::new(),

            next_packet_number: 0,
            largest_acked_packet: None,
            largest_acked_packet_sent: now,
            largest_ack_eliciting_sent: 0,
            unacked_non_ack_eliciting_tail: 0,
            sent_packets: BTreeMap::new(),
            ecn_counters: frame::EcnCounts::ZERO,
            ecn_feedback: frame::EcnCounts::ZERO,

            crypto_stream: Assembler::new(),
            crypto_offset: 0,

            time_of_last_ack_eliciting_packet: None,
            loss_time: None,
            loss_probes: 0,
            sending_copies: false,
            ping_pending: false,
            immediate_ack_pending: false,
            sent_with_keys: 0,
        }
    }

    /// Queue data for a tail loss probe (or anti-amplification deadlock prevention) packet
    ///
    /// Probes are sent similarly to normal packets when an expected ACK has not arrived. We never
    /// deem a packet lost until we receive an ACK that should have included it, but if a trailing
    /// run of packets (or their ACKs) are lost, this might not happen in a timely fashion. We send
    /// probe packets to force an ACK, and exempt them from congestion control to prevent a deadlock
    /// when the congestion window is filled with lost tail packets.
    ///
    /// We prefer to send new data, to make the most efficient use of bandwidth. If there's no data
    /// waiting to be sent, then we retransmit in-flight data to reduce odds of loss. If there's no
    /// in-flight data either, we're probably a client guarding against a handshake
    /// anti-amplification deadlock and we just make something up.
    ///
    /// The in-flight data retransmitted is the oldest packet's, its STREAM frames included, as
    /// Chromium's probe retransmits the oldest outstanding data (`MaybeSendProbePacket`) and
    /// RFC 9002 §6.2.4 permits ("the sender MAY retransmit unacknowledged data"). Upstream took
    /// only the control frames, so a lost reply was resent only once the probe's acknowledgement
    /// declared it lost, a round trip after the probe.
    pub(super) fn maybe_queue_probe(
        &mut self,
        request_immediate_ack: bool,
        streams: &mut StreamsState,
    ) {
        if self.loss_probes == 0 {
            return;
        }

        if request_immediate_ack {
            // The probe should be ACKed without delay (should only be used in the Data space and
            // when the peer supports the acknowledgement frequency extension)
            self.immediate_ack_pending = true;
        }

        if !self.pending.is_empty(streams) {
            // There's real data to send here, no need to make something up
            return;
        }

        // Retransmit the data of the oldest in-flight packet
        for packet in self.sent_packets.values_mut() {
            if !packet.retransmits.is_empty(streams) || !packet.stream_frames.is_empty() {
                // Remove retransmitted data from the old packet so we don't end up retransmitting
                // it *again* even if the copy we're sending now gets acknowledged. Its STREAM
                // frames move likewise, so one sent copy of each range is tracked, as after a
                // loss: the probe's acknowledgement or loss settles them.
                self.pending |= mem::take(&mut packet.retransmits);
                for frame in mem::take(&mut packet.stream_frames) {
                    streams.retransmit(frame);
                }
                return;
            }
        }

        // Nothing new to send and nothing to retransmit, so fall back on a ping. This should only
        // happen in rare cases during the handshake when the server becomes blocked by
        // anti-amplification.
        if !self.immediate_ack_pending {
            self.ping_pending = true;
        }
    }

    /// Queues a copy of the frames of each packet of the handshake's flights in flight and not yet
    /// copied ([`SentPacket::handshake_flight`]), once this space has nothing new to send and no
    /// probe is due
    ///
    /// RFC 9265 §4's FEC within the transport, a repetition for flights of a packet or a few:
    /// "repair symbols are sent within what the congestion window or calculated rate allows", "when
    /// there is no more data to transmit". The copy goes in packets of its own numbers, under the
    /// congestion window, pacing and the anti-amplification limit like any other, so a lost original
    /// is still declared lost and answered: FEC "should not hide congestion signals" (RFC 9265). The
    /// originals keep their frames: whichever packet is acknowledged first delivers them, and data
    /// acknowledged through one is not sent again for the other's loss (`SendBuffer::retransmit`).
    ///
    /// A copy waits `spacing` behind its original ([`copy_spacing`](super::copy_spacing)): losses
    /// come in bursts, and a copy sent with its original is lost with it in one
    /// (`docs/research/burst-loss.md`). Nothing is copied while the space is `sending` new data
    /// its pending frames do not show (the Data space's streams). Returns when the earliest copy
    /// still waiting is due.
    pub(super) fn queue_copies(
        &mut self,
        streams: &mut StreamsState,
        now: Instant,
        spacing: Duration,
        sending: bool,
    ) -> Option<Instant> {
        let mut ready = !sending && self.loss_probes == 0 && self.pending.is_empty(streams);
        let mut queued = false;
        let mut waiting: Option<Instant> = None;
        for packet in self.sent_packets.values_mut() {
            if packet.copied
                || !packet.handshake_flight
                || !packet.ack_eliciting
                || (packet.retransmits.is_empty(streams) && packet.stream_frames.is_empty())
            {
                continue;
            }
            // A deadline past the clock's range is never reached: the copy waits for nothing
            let due = packet
                .time_sent
                .checked_add(spacing)
                .unwrap_or(packet.time_sent);
            if due > now {
                waiting = Some(waiting.map_or(due, |earliest| earliest.min(due)));
                continue;
            }
            // Due, and held until the space has nothing new to send: the transmission that sends
            // the new data polls again
            if !ready {
                continue;
            }
            packet.copied = true;
            if let Some(retransmits) = packet.retransmits.get() {
                self.pending |= retransmits.clone();
            }
            for frame in packet.stream_frames.iter() {
                streams.retransmit(frame.clone());
            }
            queued = true;
            // One original at a time, so its copy is a packet of its own frames alone. Queued
            // together, the retransmission queue merges their frames and packs them anew: a
            // ClientHello in two packets went as [1141, 1928) with [0, 349), then [349, 1141), so
            // one lost copy left both halves without theirs. The next original is copied once this
            // copy is sent, at the next transmission.
            ready = false;
        }
        self.sending_copies |= queued;
        waiting
    }

    /// Get the next outgoing packet number in this space
    ///
    /// In the Data space, the connection's [`PacketNumberFilter`] must be used rather than calling
    /// this directly.
    ///
    /// `None` once the space's packet numbers (below 2^62) are spent, where upstream asserted:
    /// the connection must then close without sending anything further (RFC 9000 §12.3).
    pub(super) fn get_tx_number(&mut self) -> Option<u64> {
        let x = self.next_packet_number;
        if x >= MAX_PACKET_NUMBER {
            return None;
        }
        self.next_packet_number = x.checked_add(1)?;
        // At most one per packet number, so below 2^62
        self.sent_with_keys = self.sent_with_keys.saturating_add(1);
        Some(x)
    }

    pub(super) fn can_send(&self, streams: &StreamsState) -> SendableFrames {
        let acks = self.pending_acks.can_send();
        let other =
            !self.pending.is_empty(streams) || self.ping_pending || self.immediate_ack_pending;

        SendableFrames { acks, other }
    }

    /// Verifies sanity of an ECN block and returns whether congestion was encountered.
    pub(super) fn detect_ecn(
        &mut self,
        newly_acked: u64,
        ecn: frame::EcnCounts,
    ) -> Result<bool, &'static str> {
        let ect0_increase = ecn
            .ect0
            .checked_sub(self.ecn_feedback.ect0)
            .ok_or("peer ECT(0) count regression")?;
        let ect1_increase = ecn
            .ect1
            .checked_sub(self.ecn_feedback.ect1)
            .ok_or("peer ECT(1) count regression")?;
        let ce_increase = ecn
            .ce
            .checked_sub(self.ecn_feedback.ce)
            .ok_or("peer CE count regression")?;
        // Each count is a varint (below 2^62), so neither sum can overflow
        let ect0_ce_increase = ect0_increase
            .checked_add(ce_increase)
            .ok_or("ECN counts overflow")?;
        let total_increase = ect0_ce_increase
            .checked_add(ect1_increase)
            .ok_or("ECN counts overflow")?;
        if total_increase < newly_acked {
            return Err("ECN bleaching");
        }
        if ect0_ce_increase < newly_acked || ect1_increase != 0 {
            return Err("ECN corruption");
        }
        // If total_increase > newly_acked (which happens when ACKs are lost), this is required by
        // the draft so that long-term drift does not occur. If =, then the only question is whether
        // to count CE packets as CE or ECT0. Recording them as CE is more consistent and keeps the
        // congestion check obvious.
        self.ecn_feedback = ecn;
        Ok(ce_increase != 0)
    }

    /// Stop tracking sent packet `number`, and return what we knew about it
    pub(super) fn take(&mut self, number: u64) -> Option<SentPacket> {
        let packet = self.sent_packets.remove(&number)?;
        if !packet.ack_eliciting && number > self.largest_ack_eliciting_sent {
            // The packet taken is one of those counted, so the count is at least one
            self.unacked_non_ack_eliciting_tail =
                self.unacked_non_ack_eliciting_tail.saturating_sub(1);
        }
        Some(packet)
    }

    /// May return a packet that should be forgotten
    pub(super) fn sent(&mut self, number: u64, packet: SentPacket) -> Option<SentPacket> {
        // Retain state for at most this many non-ACK-eliciting packets sent after the most recently
        // sent ACK-eliciting packet. We're never guaranteed to receive an ACK for those, and we
        // can't judge them as lost without an ACK, so to limit memory in applications which receive
        // packets but don't send ACK-eliciting data for long periods use we must eventually start
        // forgetting about them, although it might also be reasonable to just kill the connection
        // due to weird peer behavior.
        /// Non-ACK-eliciting packets kept after the last ACK-eliciting one (upstream's value)
        const MAX_UNACKED_NON_ACK_ELICTING_TAIL: u64 = 1_000;

        let mut forgotten = None;
        if packet.ack_eliciting {
            // This packet is the neighbour of the last ack-eliciting one still held
            if let Some(previous) = self.sent_packets.get_mut(&self.largest_ack_eliciting_sent)
                && previous.ack_eliciting
                && self.largest_ack_eliciting_sent < number
            {
                previous.next = Some((number, packet.time_sent));
            }
            self.unacked_non_ack_eliciting_tail = 0;
            self.largest_ack_eliciting_sent = number;
        } else if self.unacked_non_ack_eliciting_tail > MAX_UNACKED_NON_ACK_ELICTING_TAIL {
            // The count is of packets sent after the most recent ACK-eliciting one, so there is
            // an oldest among them.
            // Per https://www.rfc-editor.org/rfc/rfc9000.html#name-frames-and-frame-types,
            // non-ACK-eliciting packets must only contain PADDING, ACK, and CONNECTION_CLOSE
            // frames, which require no special handling on ACK or loss beyond removal from
            // in-flight counters if padded.
            forgotten = self
                .sent_packets
                .range((
                    Bound::Excluded(self.largest_ack_eliciting_sent),
                    Bound::Unbounded,
                ))
                .next()
                .map(|(&oldest, _)| oldest)
                .and_then(|oldest| self.sent_packets.remove(&oldest));
        } else {
            // Bounded by the limit just above
            self.unacked_non_ack_eliciting_tail =
                self.unacked_non_ack_eliciting_tail.saturating_add(1);
        }

        self.sent_packets.insert(number, packet);
        forgotten
    }

    /// Whether any congestion-controlled packets in this space are not yet acknowledged or lost
    pub(super) fn has_in_flight(&self) -> bool {
        // The number of non-congestion-controlled (i.e. size == 0) packets in flight at a time
        // should be small, since otherwise congestion control wouldn't be effective. Therefore,
        // this shouldn't need to visit many packets before finishing one way or another.
        self.sent_packets.values().any(|x| x.size != 0)
    }
}

/// The three packet number spaces, each reached by its [`SpaceId`] so that no lookup can miss
pub(super) struct Spaces {
    initial: PacketSpace,
    handshake: PacketSpace,
    data: PacketSpace,
}

impl Spaces {
    pub(super) fn new(initial: PacketSpace, handshake: PacketSpace, data: PacketSpace) -> Self {
        Self {
            initial,
            handshake,
            data,
        }
    }

    pub(super) fn get(&self, space: SpaceId) -> &PacketSpace {
        match space {
            SpaceId::Initial => &self.initial,
            SpaceId::Handshake => &self.handshake,
            SpaceId::Data => &self.data,
        }
    }

    pub(super) fn get_mut(&mut self, space: SpaceId) -> &mut PacketSpace {
        match space {
            SpaceId::Initial => &mut self.initial,
            SpaceId::Handshake => &mut self.handshake,
            SpaceId::Data => &mut self.data,
        }
    }
}

/// Represents one or more packets subject to retransmission
#[derive(Debug, Clone)]
pub(super) struct SentPacket {
    /// [`PathData::generation`](super::PathData::generation) of the path on which this packet was sent
    pub(super) path_generation: u64,
    /// The time the packet was sent.
    pub(super) time_sent: Instant,
    /// The number of bytes sent in the packet, not including UDP or IP overhead, but including QUIC
    /// framing overhead. Zero if this packet is not counted towards congestion control, i.e. not an
    /// "in flight" packet.
    pub(super) size: u16,
    /// Whether an acknowledgement is expected directly in response to this packet.
    pub(super) ack_eliciting: bool,
    /// The largest packet number acknowledged by this packet
    pub(super) largest_acked: Option<u64>,
    /// Data which needs to be retransmitted in case the packet is lost.
    /// The data is boxed to minimize `SentPacket` size for the typical case of
    /// packets only containing ACKs and STREAM frames.
    pub(super) retransmits: ThinRetransmits,
    /// Metadata for stream frames in a packet
    ///
    /// The actual application data is stored with the stream state.
    pub(super) stream_frames: frame::StreamMetaVec,
    /// Whether this packet's frames went, or are queued to go, in a second packet
    /// ([`PacketSpace::queue_copies`]), or it is that second packet
    pub(super) copied: bool,
    /// Whether the packet is of the handshake's flights: an Initial or Handshake packet, one sent
    /// before this endpoint's handshake completed (0-RTT and 0.5-RTT data), or one in a datagram
    /// with a Handshake packet (the client's first 1-RTT data, beside its Finished)
    pub(super) handshake_flight: bool,
    /// The next ack-eliciting packet this space sent after this one, if this one is ack-eliciting:
    /// its number and when it went, the neighbour whose fate says whether a loss of this one came
    /// in a burst (`losses.rs`)
    pub(super) next: Option<(u64, Instant)>,
}

/// Retransmittable data queue
#[allow(unreachable_pub)] // fuzzing only
#[derive(Debug, Default, Clone)]
pub struct Retransmits {
    pub(super) max_data: bool,
    pub(super) data_blocked: bool,
    pub(super) max_stream_id: PerDir<bool>,
    pub(super) reset_stream: Vec<(StreamId, VarInt)>,
    pub(super) stop_sending: Vec<frame::StopSending>,
    pub(super) max_stream_data: FxHashSet<StreamId>,
    pub(super) stream_data_blocked: FxHashSet<StreamId>,
    pub(super) crypto: VecDeque<frame::Crypto>,
    pub(super) new_cids: Vec<IssuedCid>,
    pub(super) retire_cids: Vec<u64>,
    pub(super) ack_frequency: bool,
    pub(super) handshake_done: bool,
    /// For each enqueued NEW_TOKEN frame, a copy of the path's remote address
    ///
    /// There are 2 reasons this is unusual:
    ///
    /// - If the path changes, NEW_TOKEN frames bound for the old path are not retransmitted on the
    ///   new path. That is why this field stores the remote address: so that ones for old paths
    ///   can be filtered out.
    /// - If a token is lost, a new randomly generated token is re-transmitted, rather than the
    ///   original. This is so that if both transmissions are received, the client won't risk
    ///   sending the same token twice. That is why this field does _not_ store any actual token.
    ///
    /// It is true that a QUIC endpoint will only want to effectively have NEW_TOKEN frames
    /// enqueued for its current path at a given point in time. Based on that, we could conceivably
    /// change this from a vector to an `Option<(SocketAddr, usize)>` or just a `usize` or
    /// something. However, due to the architecture of Quinn, it is considerably simpler to not do
    /// that; consider what such a change would mean for implementing `BitOrAssign` on Self.
    pub(super) new_tokens: Vec<SocketAddr>,
}

impl Retransmits {
    pub(super) fn retire_cids(&mut self, cids: Range<u64>) -> Result<(), TransportError> {
        // We don't bother counting in-flight frames because those are bounded by congestion control.
        let num = cids.end.saturating_sub(cids.start);
        if (self.retire_cids.len() as u64).saturating_add(num) > Self::MAX_PENDING_RETIRED_CIDS {
            return Err(TransportError::CONNECTION_ID_LIMIT_ERROR(
                "queued too many retired CIDs",
            ));
        }

        self.retire_cids.extend(cids);
        Ok(())
    }

    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        !self.max_data
            && !(self.data_blocked && streams.can_send_data_blocked())
            && !self.max_stream_id.iter().any(|&x| x)
            && self.reset_stream.is_empty()
            && self.stop_sending.is_empty()
            && self
                .max_stream_data
                .iter()
                .all(|&id| !streams.can_send_flow_control(id))
            && self
                .stream_data_blocked
                .iter()
                .all(|&id| streams.stream_data_blocked_limit(id).is_none())
            && self.crypto.is_empty()
            && self.new_cids.is_empty()
            && self.retire_cids.is_empty()
            && !self.ack_frequency
            && !self.handshake_done
            && self.new_tokens.is_empty()
    }

    /// Ensure `pending_retired` cannot grow without bound
    ///
    /// Limit is somewhat arbitrary but very permissive.
    const MAX_PENDING_RETIRED_CIDS: u64 = CidQueue::LEN as u64 * 10;
}

impl ::std::ops::BitOrAssign for Retransmits {
    fn bitor_assign(&mut self, rhs: Self) {
        // We reduce in-stream head-of-line blocking by queueing retransmits before other data for
        // STREAM and CRYPTO frames.
        self.max_data |= rhs.max_data;
        self.data_blocked |= rhs.data_blocked;
        for dir in Dir::iter() {
            *self.max_stream_id.get_mut(dir) |= *rhs.max_stream_id.get(dir);
        }
        self.reset_stream.extend_from_slice(&rhs.reset_stream);
        self.stop_sending.extend_from_slice(&rhs.stop_sending);
        self.max_stream_data.extend(&rhs.max_stream_data);
        self.stream_data_blocked.extend(&rhs.stream_data_blocked);
        for crypto in rhs.crypto.into_iter().rev() {
            self.crypto.push_front(crypto);
        }
        self.new_cids.extend(&rhs.new_cids);
        self.retire_cids.extend(rhs.retire_cids);
        self.ack_frequency |= rhs.ack_frequency;
        self.handshake_done |= rhs.handshake_done;
        self.new_tokens.extend_from_slice(&rhs.new_tokens);
    }
}

impl ::std::ops::BitOrAssign<ThinRetransmits> for Retransmits {
    fn bitor_assign(&mut self, rhs: ThinRetransmits) {
        if let Some(retransmits) = rhs.retransmits {
            self.bitor_assign(*retransmits)
        }
    }
}

impl ::std::iter::FromIterator<Self> for Retransmits {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Self>,
    {
        let mut result = Self::default();
        for packet in iter {
            result |= packet;
        }
        result
    }
}

/// A variant of `Retransmits` which only allocates storage when required
#[derive(Debug, Default, Clone)]
pub(super) struct ThinRetransmits {
    retransmits: Option<Box<Retransmits>>,
}

impl ThinRetransmits {
    /// Returns `true` if no retransmits are necessary
    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        match &self.retransmits {
            Some(retransmits) => retransmits.is_empty(streams),
            None => true,
        }
    }

    /// Returns a reference to the retransmits stored in this box
    pub(super) fn get(&self) -> Option<&Retransmits> {
        self.retransmits.as_deref()
    }

    /// Returns a mutable reference to the stored retransmits
    ///
    /// This function will allocate a backing storage if required.
    pub(super) fn get_or_create(&mut self) -> &mut Retransmits {
        self.retransmits.get_or_insert_with(Box::default)
    }
}

/// RFC4303-style sliding window packet number deduplicator.
///
/// A contiguous bitfield, where each bit corresponds to a packet number and the rightmost bit is
/// always set. A set bit represents a packet that has been successfully authenticated. Bits left of
/// the window are assumed to be set.
///
/// ```text
/// ...xxxxxxxxx 1 0
///     ^        ^ ^
/// window highest next
/// ```
pub(super) struct Dedup {
    window: Window,
    /// Lowest packet number higher than all yet authenticated.
    next: u64,
}

/// Inner bitfield type.
///
/// Because QUIC never reuses packet numbers, this only needs to be large enough to deal with
/// packets that are reordered but still delivered in a timely manner.
type Window = u128;

/// Number of packets tracked by `Dedup`.
const WINDOW_SIZE: u64 = 1 + mem::size_of::<Window>() as u64 * 8;

impl Dedup {
    /// Construct an empty window positioned at the start.
    pub(super) fn new() -> Self {
        Self { window: 0, next: 0 }
    }

    /// Highest packet number authenticated, if any.
    fn highest(&self) -> Option<u64> {
        self.next.checked_sub(1)
    }

    /// Record a newly authenticated packet number.
    ///
    /// Returns whether the packet might be a duplicate.
    pub(super) fn insert(&mut self, packet: u64) -> bool {
        match self
            .highest()
            .and_then(|highest| highest.checked_sub(packet))
        {
            None => {
                // Right of window: no packet yet, or past the highest
                let diff = packet.saturating_sub(self.next);
                self.window = ((self.window << 1) | 1)
                    .checked_shl(u32::try_from(diff).unwrap_or(u32::MAX))
                    .unwrap_or(0);
                // Packet numbers are below 2^62
                self.next = packet.saturating_add(1);
                false
            }
            Some(behind) if behind < WINDOW_SIZE => {
                // Within window
                if let Some(bit) = behind.checked_sub(1) {
                    // < highest
                    let mask = 1 << bit;
                    let duplicate = self.window & mask != 0;
                    self.window |= mask;
                    duplicate
                } else {
                    // == highest
                    true
                }
            }
            // Left of window
            Some(_) => true,
        }
    }

    /// Returns the packet number of the smallest packet missing between the provided interval
    ///
    /// If there are no missing packets, returns `None`
    fn smallest_missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> Option<u64> {
        /// Bits in the window
        const BITFIELD_SIZE: u64 = (mem::size_of::<Window>() * 8) as u64;
        // Both bounds were received, so there is a highest packet at or above them
        let highest = self.highest()?;

        // Since we already know the packets at the boundaries have been received, we only need to
        // check those in between them (this removes the necessity of extra logic to deal with the
        // highest packet, which is stored outside the bitfield)
        let lower_bound = lower_bound.checked_add(1)?;
        let upper_bound = upper_bound.saturating_sub(1);

        // Note: the offsets are counted from the right
        // The highest packet is not included in the bitfield, so we subtract 1 to account for that
        let start_offset = highest.checked_sub(upper_bound)?.saturating_sub(1);
        if start_offset >= BITFIELD_SIZE {
            // The start offset is outside of the window. All packets outside of the window are
            // considered to be received.
            return None;
        }

        let end_offset_exclusive = highest.saturating_sub(lower_bound);

        // The range is clamped at the edge of the window, because any earlier packets are
        // considered to be received
        let range_len = end_offset_exclusive
            .saturating_sub(start_offset)
            .min(BITFIELD_SIZE);
        if range_len == 0 {
            return None;
        }

        // Ensure the shift is within bounds (we already know start_offset < BITFIELD_SIZE,
        // because of the early return)
        let mask = if range_len == BITFIELD_SIZE {
            u128::MAX
        } else {
            // `range_len` is at least one here, so the low bits are not empty
            (1u128 << range_len).checked_sub(1)? << start_offset
        };
        let gaps = !self.window & mask;

        let smallest_missing_offset = u64::from(Window::BITS.checked_sub(gaps.leading_zeros())?);
        let smallest_missing_packet = highest.checked_sub(smallest_missing_offset)?;

        if smallest_missing_packet <= upper_bound {
            Some(smallest_missing_packet)
        } else {
            None
        }
    }

    /// Returns true if there are any missing packets between the provided interval
    ///
    /// The provided packet numbers must have been received before calling this function
    fn missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> bool {
        self.smallest_missing_in_interval(lower_bound, upper_bound)
            .is_some()
    }
}

/// Indicates which data is available for sending
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct SendableFrames {
    pub(super) acks: bool,
    pub(super) other: bool,
}

impl SendableFrames {
    /// Returns that no data is available for sending
    pub(super) fn empty() -> Self {
        Self {
            acks: false,
            other: false,
        }
    }

    /// Whether no data is sendable
    pub(super) fn is_empty(&self) -> bool {
        !self.acks && !self.other
    }
}

#[derive(Debug)]
pub(super) struct PendingAcks {
    /// Whether we should send an ACK immediately, even if that means sending an ACK-only packet
    ///
    /// When `immediate_ack_required` is false, the normal behavior is to send ACK frames only when
    /// there is other data to send, or when the `MaxAckDelay` timer expires.
    immediate_ack_required: bool,
    /// The number of ack-eliciting packets received since the last ACK frame was sent
    ///
    /// Once the count _exceeds_ `ack_eliciting_threshold`, an immediate ACK is required
    ack_eliciting_since_last_ack_sent: u64,
    non_ack_eliciting_since_last_ack_sent: u64,
    ack_eliciting_threshold: u64,
    /// The reordering threshold, controlling how we respond to out-of-order ack-eliciting packets
    ///
    /// Different values enable different behavior:
    ///
    /// * `0`: no special action is taken
    /// * `1`: an ACK is immediately sent if it is out-of-order according to RFC 9000
    /// * `>1`: an ACK is immediately sent if it is out-of-order according to the ACK frequency draft
    reordering_threshold: u64,
    /// The earliest ack-eliciting packet since the last ACK was sent, used to calculate the moment
    /// upon which `max_ack_delay` elapses
    earliest_ack_eliciting_since_last_ack_sent: Option<Instant>,
    /// The packet number ranges of ack-eliciting packets the peer hasn't confirmed receipt of ACKs
    /// for
    ranges: ArrayRangeSet,
    /// The packet with the largest packet number, and the time upon which it was received (used to
    /// calculate ACK delay in [`PendingAcks::ack_delay`])
    largest_packet: Option<(u64, Instant)>,
    /// The ack-eliciting packet we have received with the largest packet number
    largest_ack_eliciting_packet: Option<u64>,
    /// The largest acknowledged packet number sent in an ACK frame
    largest_acked: Option<u64>,
}

impl PendingAcks {
    fn new() -> Self {
        Self {
            immediate_ack_required: false,
            ack_eliciting_since_last_ack_sent: 0,
            non_ack_eliciting_since_last_ack_sent: 0,
            ack_eliciting_threshold: 1,
            reordering_threshold: 1,
            earliest_ack_eliciting_since_last_ack_sent: None,
            ranges: ArrayRangeSet::default(),
            largest_packet: None,
            largest_ack_eliciting_packet: None,
            largest_acked: None,
        }
    }

    pub(super) fn set_ack_frequency_params(&mut self, frame: &frame::AckFrequency) {
        self.ack_eliciting_threshold = frame.ack_eliciting_threshold.into_inner();
        self.reordering_threshold = frame.reordering_threshold.into_inner();
    }

    pub(super) fn set_immediate_ack_required(&mut self) {
        self.immediate_ack_required = true;
    }

    pub(super) fn on_max_ack_delay_timeout(&mut self) {
        self.immediate_ack_required = self.ack_eliciting_since_last_ack_sent > 0;
    }

    pub(super) fn max_ack_delay_timeout(&self, max_ack_delay: Duration) -> Option<Instant> {
        self.earliest_ack_eliciting_since_last_ack_sent
            .and_then(|earliest_unacked| earliest_unacked.checked_add(max_ack_delay))
    }

    /// Whether any ACK frames can be sent even if doing so requires a dedicated packet
    pub(super) fn can_send(&self) -> bool {
        self.immediate_ack_required && !self.ranges.is_empty()
    }

    /// Whether any ACK frames can be sent in data-initiated packets
    pub(super) fn can_send_with_other_frames(&self) -> bool {
        !self.ranges.is_empty()
    }

    /// Returns the delay since the packet with the largest packet number was received
    pub(super) fn ack_delay(&self, now: Instant) -> Duration {
        self.largest_packet
            .map_or(Duration::default(), |(_, received)| {
                now.saturating_duration_since(received)
            })
    }

    /// Handle receipt of a new packet
    ///
    /// Returns true if the max ack delay timer should be armed
    pub(super) fn packet_received(
        &mut self,
        now: Instant,
        packet_number: u64,
        ack_eliciting: bool,
        dedup: &Dedup,
    ) -> bool {
        // Both counts are statistics reset by each ACK sent; they saturate rather than wrap
        if !ack_eliciting {
            self.non_ack_eliciting_since_last_ack_sent =
                self.non_ack_eliciting_since_last_ack_sent.saturating_add(1);
            return false;
        }

        let prev_largest_ack_eliciting = self.largest_ack_eliciting_packet.unwrap_or(0);

        // Track largest ack-eliciting packet
        self.largest_ack_eliciting_packet = self
            .largest_ack_eliciting_packet
            .map(|pn| pn.max(packet_number))
            .or(Some(packet_number));

        // Handle ack_eliciting_threshold
        self.ack_eliciting_since_last_ack_sent =
            self.ack_eliciting_since_last_ack_sent.saturating_add(1);
        self.immediate_ack_required |=
            self.ack_eliciting_since_last_ack_sent > self.ack_eliciting_threshold;

        // Handle out-of-order packets
        self.immediate_ack_required |=
            self.is_out_of_order(packet_number, prev_largest_ack_eliciting, dedup);

        // Arm max_ack_delay timer if necessary
        if self.earliest_ack_eliciting_since_last_ack_sent.is_none() && !self.can_send() {
            self.earliest_ack_eliciting_since_last_ack_sent = Some(now);
            return true;
        }

        false
    }

    fn is_out_of_order(
        &self,
        packet_number: u64,
        prev_largest_ack_eliciting: u64,
        dedup: &Dedup,
    ) -> bool {
        match self.reordering_threshold {
            0 => false,
            1 => {
                // From https://www.rfc-editor.org/rfc/rfc9000#section-13.2.1-7
                packet_number < prev_largest_ack_eliciting
                    || dedup.missing_in_interval(prev_largest_ack_eliciting, packet_number)
            }
            _ => {
                // From acknowledgement frequency draft, section 6.1: send an ACK immediately if
                // doing so would cause the sender to detect a new packet loss
                let Some((largest_acked, largest_unacked)) =
                    self.largest_acked.zip(self.largest_ack_eliciting_packet)
                else {
                    return false;
                };
                // The largest packet number that could be declared lost without a new ACK being
                // sent; none while the threshold exceeds the largest acknowledged
                let Some(largest_reported) = largest_acked
                    .checked_sub(self.reordering_threshold)
                    .and_then(|below| below.checked_add(1))
                else {
                    return false;
                };
                dedup
                    .smallest_missing_in_interval(largest_reported, largest_unacked)
                    .and_then(|smallest| largest_unacked.checked_sub(smallest))
                    .is_some_and(|gap| gap >= self.reordering_threshold)
            }
        }
    }

    /// Should be called whenever ACKs have been sent
    ///
    /// This will suppress sending further ACKs until additional ACK eliciting frames arrive
    pub(super) fn acks_sent(&mut self) {
        // It is possible (though unlikely) that the ACKs we just sent do not cover all the
        // ACK-eliciting packets we have received (e.g. if there is not enough room in the packet to
        // fit all the ranges). To keep things simple, however, we assume they do. If there are
        // indeed some ACKs that weren't covered, the packets might be ACKed later anyway, because
        // they are still contained in `self.ranges`. If we somehow fail to send the ACKs at a later
        // moment, the peer will assume the packets got lost and will retransmit their frames in a
        // new packet, which is suboptimal, because we already received them. Our assumption here is
        // that simplicity results in code that is more performant, even in the presence of
        // occasional redundant retransmits.
        self.immediate_ack_required = false;
        self.ack_eliciting_since_last_ack_sent = 0;
        self.non_ack_eliciting_since_last_ack_sent = 0;
        self.earliest_ack_eliciting_since_last_ack_sent = None;
        self.largest_acked = self.largest_ack_eliciting_packet;
    }

    /// Insert one packet that needs to be acknowledged
    pub(super) fn insert_one(&mut self, packet: u64, now: Instant) {
        self.ranges.insert_one(packet);

        if self.largest_packet.is_none_or(|(pn, _)| packet > pn) {
            self.largest_packet = Some((packet, now));
        }

        if self.ranges.len() > MAX_ACK_BLOCKS {
            self.ranges.pop_min();
        }
    }

    /// Remove ACKs of packets numbered at or below `max` from the set of pending ACKs
    pub(super) fn subtract_below(&mut self, max: u64) {
        // Packet numbers are below 2^62
        self.ranges.remove(0..max.saturating_add(1));
    }

    /// Returns the set of currently pending ACK ranges
    pub(super) fn ranges(&self) -> &ArrayRangeSet {
        &self.ranges
    }

    /// Queue an ACK if a significant number of non-ACK-eliciting packets have not yet been
    /// acknowledged
    ///
    /// Should be called immediately before a non-probing packet is composed, when we've already
    /// committed to sending a packet regardless.
    pub(super) fn maybe_ack_non_eliciting(&mut self) {
        // If we're going to send a packet anyway, and we've received a significant number of
        // non-ACK-eliciting packets, then include an ACK to help the peer perform timely loss
        // detection even if they're not sending any ACK-eliciting packets themselves. Exact
        // threshold chosen somewhat arbitrarily.
        /// Unacknowledged non-ACK-eliciting packets that warrant an ACK (upstream's value)
        const LAZY_ACK_THRESHOLD: u64 = 10;
        if self.non_ack_eliciting_since_last_ack_sent > LAZY_ACK_THRESHOLD {
            self.immediate_ack_required = true;
        }
    }
}

/// Helper for mitigating [optimistic ACK attacks]
///
/// A malicious peer could prompt the local application to begin a large data transfer, and then
/// send ACKs without first waiting for data to be received. This could defeat congestion control,
/// allowing the connection to consume disproportionate resources. We therefore occasionally skip
/// packet numbers, and classify any ACK referencing a skipped packet number as a transport error.
///
/// Skipped packet numbers occur only in the application data space (where costly transfers might
/// take place) and are distributed exponentially to reflect the reduced likelihood and impact of
/// bad behavior from a peer that has been well-behaved for an extended period.
///
/// ACKs for packet numbers that have not yet been allocated are also a transport error, but an
/// attacker with knowledge of the congestion control algorithm in use could time falsified ACKs to
/// arrive after the packets they reference are sent.
///
/// [optimistic ACK attacks]: https://www.rfc-editor.org/rfc/rfc9000.html#name-optimistic-ack-attack
pub(super) struct PacketNumberFilter {
    /// Next outgoing packet number to skip
    next_skipped_packet_number: u64,
    /// Most recently skipped packet number
    prev_skipped_packet_number: Option<u64>,
    /// Next packet number to skip is randomly selected from 2^n..2^n+1
    exponent: u32,
}

impl PacketNumberFilter {
    pub(super) fn new(rng: &mut (impl Rng + ?Sized)) -> Self {
        // First skipped PN is in 0..64
        let exponent = 6;
        Self {
            next_skipped_packet_number: rng.random_range(0..2u64.saturating_pow(exponent)),
            prev_skipped_packet_number: None,
            exponent,
        }
    }

    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self {
            next_skipped_packet_number: u64::MAX,
            prev_skipped_packet_number: None,
            exponent: u32::MAX,
        }
    }

    pub(super) fn peek(&self, space: &PacketSpace) -> u64 {
        let n = space.next_packet_number;
        if n != self.next_skipped_packet_number {
            return n;
        }
        // Packet numbers are below 2^62
        n.saturating_add(1)
    }

    /// The next packet number to send in the Data space, `None` once they are spent
    pub(super) fn allocate(
        &mut self,
        rng: &mut (impl Rng + ?Sized),
        space: &mut PacketSpace,
    ) -> Option<u64> {
        let n = space.get_tx_number()?;
        if n != self.next_skipped_packet_number {
            return Some(n);
        }

        trace!("skipping pn {n}");
        // Skip this packet number, and choose the next one to skip
        self.prev_skipped_packet_number = Some(self.next_skipped_packet_number);
        let next_exponent = self.exponent.saturating_add(1);
        let (low, high) = (
            2u64.saturating_pow(self.exponent),
            2u64.saturating_pow(next_exponent),
        );
        // Once the exponent saturates the range is empty, which `random_range` would panic on;
        // no packet number that high is ever sent, so none is skipped again
        self.next_skipped_packet_number = if low < high {
            rng.random_range(low..high)
        } else {
            u64::MAX
        };
        self.exponent = next_exponent;

        space.get_tx_number()
    }

    pub(super) fn check_ack(
        &self,
        space_id: SpaceId,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<(), TransportError> {
        if space_id == SpaceId::Data
            && self
                .prev_skipped_packet_number
                .is_some_and(|x| range.contains(&x))
        {
            return Err(TransportError::PROTOCOL_VIOLATION("unsent packet acked"));
        }
        Ok(())
    }
}

/// Packet numbers are below 2^62 (RFC 9000 §12.3)
const MAX_PACKET_NUMBER: u64 = 1 << 62;

/// Ensures we can always fit all our ACKs in a single minimum-MTU packet with room to spare
const MAX_ACK_BLOCKS: usize = 64;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sanity() {
        let mut dedup = Dedup::new();
        assert!(!dedup.insert(0));
        assert_eq!(dedup.next, 1);
        assert_eq!(dedup.window, 0b1);
        assert!(dedup.insert(0));
        assert_eq!(dedup.next, 1);
        assert_eq!(dedup.window, 0b1);
        assert!(!dedup.insert(1));
        assert_eq!(dedup.next, 2);
        assert_eq!(dedup.window, 0b11);
        assert!(!dedup.insert(2));
        assert_eq!(dedup.next, 3);
        assert_eq!(dedup.window, 0b111);
        assert!(!dedup.insert(4));
        assert_eq!(dedup.next, 5);
        assert_eq!(dedup.window, 0b11110);
        assert!(!dedup.insert(7));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_0100);
        assert!(dedup.insert(4));
        assert!(!dedup.insert(3));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1100);
        assert!(!dedup.insert(6));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1101);
        assert!(!dedup.insert(5));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1111);
    }

    #[test]
    fn happypath() {
        let mut dedup = Dedup::new();
        for i in 0..(2 * WINDOW_SIZE) {
            assert!(!dedup.insert(i));
            for j in 0..=i {
                assert!(dedup.insert(j));
            }
        }
    }

    #[test]
    fn jump() {
        let mut dedup = Dedup::new();
        dedup.insert(2 * WINDOW_SIZE);
        assert!(dedup.insert(WINDOW_SIZE));
        assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
        assert_eq!(dedup.window, 0);
        assert!(!dedup.insert(WINDOW_SIZE + 1));
        assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
        assert_eq!(dedup.window, 1 << (WINDOW_SIZE - 2));
    }

    #[test]
    fn dedup_has_missing() {
        let mut dedup = Dedup::new();

        dedup.insert(0);
        assert!(!dedup.missing_in_interval(0, 0));

        dedup.insert(1);
        assert!(!dedup.missing_in_interval(0, 1));

        dedup.insert(3);
        assert!(dedup.missing_in_interval(1, 3));

        dedup.insert(4);
        assert!(!dedup.missing_in_interval(3, 4));
        assert!(dedup.missing_in_interval(0, 4));

        dedup.insert(2);
        assert!(!dedup.missing_in_interval(0, 4));
    }

    #[test]
    fn dedup_outside_of_window_has_missing() {
        let mut dedup = Dedup::new();

        for i in 0..140 {
            dedup.insert(i);
        }

        // 0 and 4 are outside of the window
        assert!(!dedup.missing_in_interval(0, 4));
        dedup.insert(160);
        assert!(!dedup.missing_in_interval(0, 4));
        assert!(!dedup.missing_in_interval(0, 140));
        assert!(dedup.missing_in_interval(0, 160));
    }

    #[test]
    fn dedup_smallest_missing() {
        let mut dedup = Dedup::new();

        dedup.insert(0);
        assert_eq!(dedup.smallest_missing_in_interval(0, 0), None);

        dedup.insert(1);
        assert_eq!(dedup.smallest_missing_in_interval(0, 1), None);

        dedup.insert(5);
        dedup.insert(7);
        assert_eq!(dedup.smallest_missing_in_interval(0, 7), Some(2));
        assert_eq!(dedup.smallest_missing_in_interval(5, 7), Some(6));

        dedup.insert(2);
        assert_eq!(dedup.smallest_missing_in_interval(1, 7), Some(3));

        dedup.insert(170);
        dedup.insert(172);
        dedup.insert(300);
        assert_eq!(dedup.smallest_missing_in_interval(170, 172), None);

        dedup.insert(500);
        assert_eq!(dedup.smallest_missing_in_interval(0, 500), Some(372));
        assert_eq!(dedup.smallest_missing_in_interval(0, 373), Some(372));
        assert_eq!(dedup.smallest_missing_in_interval(0, 372), None);
    }

    #[test]
    fn pending_acks_first_packet_is_not_considered_reordered() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();
        dedup.insert(0);
        acks.packet_received(Instant::now(), 0, true, &dedup);
        assert!(!acks.immediate_ack_required);
    }

    #[test]
    fn pending_acks_after_immediate_ack_set() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();

        // Receive ack-eliciting packet
        dedup.insert(0);
        let now = Instant::now();
        acks.insert_one(0, now);
        acks.packet_received(now, 0, true, &dedup);

        // Sanity check
        assert!(!acks.ranges.is_empty());
        assert!(!acks.can_send());

        // Can send ACK after max_ack_delay exceeded
        acks.set_immediate_ack_required();
        assert!(acks.can_send());
    }

    #[test]
    fn pending_acks_ack_delay() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();

        let t1 = Instant::now();
        let t2 = t1 + Duration::from_millis(2);
        let t3 = t2 + Duration::from_millis(5);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(0));

        // In-order packet
        dedup.insert(0);
        acks.insert_one(0, t1);
        acks.packet_received(t1, 0, true, &dedup);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(2));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(7));

        // Out of order (higher than expected)
        dedup.insert(3);
        acks.insert_one(3, t2);
        acks.packet_received(t2, 3, true, &dedup);
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));

        // Out of order (lower than expected, so previous instant is kept)
        dedup.insert(2);
        acks.insert_one(2, t3);
        acks.packet_received(t3, 2, true, &dedup);
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));
    }

    #[test]
    fn sent_packet_size() {
        // The tracking state of sent packets should be minimal, and not grow
        // over time.
        assert!(std::mem::size_of::<SentPacket>() <= 128);
    }
}
