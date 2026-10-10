use bytes::Bytes;
use rand::RngExt;
use tracing::{debug, trace, trace_span};

use super::{Connection, SentFrames, spaces::SentPacket};
use crate::{
    ConnectionId, Instant, TransportError, TransportErrorCode,
    connection::ConnectionSide,
    crypto::{HeaderKey, PacketKey},
    frame::{self, Close},
    packet::{FIXED_BIT, Header, InitialHeader, LongType, PacketNumber, PartialEncode, SpaceId},
};

pub(super) struct PacketBuilder {
    pub(super) datagram_start: usize,
    pub(super) space: SpaceId,
    pub(super) partial_encode: PartialEncode,
    pub(super) ack_eliciting: bool,
    pub(super) exact_number: u64,
    pub(super) short_header: bool,
    /// Smallest absolute position in the associated buffer that must be occupied by this packet's
    /// frames
    pub(super) min_size: usize,
    /// Largest absolute position in the associated buffer that may be occupied by this packet's
    /// frames
    pub(super) max_size: usize,
    pub(super) tag_len: usize,
    pub(super) _span: tracing::span::EnteredSpan,
}

impl PacketBuilder {
    /// Write a new packet header to `buffer` and determine the packet's properties
    ///
    /// Marks the connection drained and returns `None` if the confidentiality limit would be
    /// violated, if the space's packet numbers are spent (RFC 9000 §12.3: the connection closes
    /// without sending anything further), or if the space has no keys to send with (an internal
    /// error, where upstream panicked).
    pub(super) fn new(
        now: Instant,
        space_id: SpaceId,
        dst_cid: ConnectionId,
        buffer: &mut Vec<u8>,
        buffer_capacity: usize,
        datagram_start: usize,
        ack_eliciting: bool,
        conn: &mut Connection,
    ) -> Option<Self> {
        let version = conn.version;
        let Some((sample_size, tag_len, confidentiality_limit)) =
            local_keys(conn, space_id).map(|(header, packet)| {
                (
                    header.sample_size(),
                    packet.tag_len(),
                    packet.confidentiality_limit(),
                )
            })
        else {
            conn.kill(TransportError::INTERNAL_ERROR("no keys to send with").into());
            return None;
        };
        if !keep_within_confidentiality_limit(conn, now, space_id, confidentiality_limit) {
            return None;
        }

        let space = conn.spaces.get_mut(space_id);
        let allocated = match space_id {
            SpaceId::Data => conn.packet_number_filter.allocate(&mut conn.rng, space),
            _ => space.get_tx_number(),
        };
        let Some(exact_number) = allocated else {
            conn.kill(TransportError::INTERNAL_ERROR("packet numbers exhausted").into());
            return None;
        };
        let space = conn.spaces.get(space_id);

        let span = trace_span!("send", space = ?space_id, pn = exact_number).entered();

        let Some(number) = PacketNumber::new(exact_number, space.largest_acked_packet.unwrap_or(0))
        else {
            conn.kill(
                TransportError::INTERNAL_ERROR("packet number too far past the largest acked")
                    .into(),
            );
            return None;
        };
        let header = match space_id {
            SpaceId::Data if space.crypto.is_some() => Header::Short {
                dst_cid,
                number,
                spin: if conn.spin_enabled {
                    conn.spin
                } else {
                    conn.rng.random()
                },
                key_phase: conn.key_phase,
            },
            SpaceId::Data => Header::Long {
                ty: LongType::ZeroRtt,
                src_cid: conn.handshake_cid,
                dst_cid,
                number,
                version,
            },
            SpaceId::Handshake => Header::Long {
                ty: LongType::Handshake,
                src_cid: conn.handshake_cid,
                dst_cid,
                number,
                version,
            },
            SpaceId::Initial => Header::Initial(InitialHeader {
                src_cid: conn.handshake_cid,
                dst_cid,
                token: match &conn.side {
                    ConnectionSide::Client { token, .. } => token.clone(),
                    ConnectionSide::Server { .. } => Bytes::new(),
                },
                number,
                version,
            }),
        };
        let partial_encode = header.encode(buffer);
        if conn.peer_params.grease_quic_bit
            && conn.rng.random()
            && let Some(first) = buffer.get_mut(partial_encode.start)
        {
            *first ^= FIXED_BIT;
        }

        // Each packet must be large enough for header protection sampling, i.e. the combined
        // lengths of the encoded packet number and protected payload must be at least 4 bytes
        // longer than the sample required for header protection. Further, each packet should be at
        // least tag_len + 6 bytes larger than the destination CID on incoming packets so that the
        // peer may send stateless resets that are indistinguishable from regular traffic.

        // pn_len + payload_len + tag_len >= sample_size + 4
        // payload_len >= sample_size + 4 - pn_len - tag_len
        // Each sum is a required size, so saturating can only over-state it
        let min_size = Ord::max(
            buffer.len().saturating_add(
                sample_size
                    .saturating_add(4)
                    .saturating_sub(number.len().saturating_add(tag_len)),
            ),
            partial_encode
                .start
                .saturating_add(dst_cid.len())
                .saturating_add(6),
        );
        // The caller leaves room for a packet (`MIN_PACKET_SPACE`); a buffer without it cannot
        // carry one
        // A long header's length field caps what follows it (`PartialEncode::length_limit`)
        let length_limit = partial_encode
            .length_limit()
            .map_or(usize::MAX, |limit| limit.saturating_sub(tag_len));
        let Some(max_size) = buffer_capacity
            .checked_sub(tag_len)
            .map(|max_size| max_size.min(length_limit))
            .filter(|&max_size| max_size >= min_size)
        else {
            buffer.truncate(partial_encode.start);
            conn.kill(TransportError::INTERNAL_ERROR("no room for a packet").into());
            return None;
        };

        Some(Self {
            datagram_start,
            space: space_id,
            partial_encode,
            exact_number,
            short_header: header.is_short(),
            min_size,
            max_size,
            tag_len,
            ack_eliciting,
            _span: span,
        })
    }

    /// Append the minimum amount of padding to the packet such that, after encryption, the
    /// enclosing datagram will occupy at least `min_size` bytes
    pub(super) fn pad_to(&mut self, min_size: u16) {
        // The datagram might already have a larger minimum size than the caller is requesting, if
        // e.g. we're coalescing packets and have populated more than `min_size` bytes with packets
        // already.
        // A required size: saturating can only over-state it
        self.min_size = Ord::max(
            self.min_size,
            self.datagram_start
                .saturating_add(usize::from(min_size))
                .saturating_sub(self.tag_len),
        );
    }

    pub(super) fn finish_and_track(
        self,
        now: Instant,
        conn: &mut Connection,
        sent: Option<SentFrames>,
        buffer: &mut Vec<u8>,
    ) {
        let ack_eliciting = self.ack_eliciting;
        let exact_number = self.exact_number;
        let space_id = self.space;
        let Some((size, padded)) = self.finish(conn, now, buffer) else {
            return;
        };
        let sent = match sent {
            Some(sent) => sent,
            None => return,
        };

        // A packet is no larger than its datagram, whose size is a u16
        let size = match padded || ack_eliciting {
            true => u16::try_from(size).unwrap_or(u16::MAX),
            false => 0,
        };

        // Of the handshake's flights: an Initial or Handshake packet, one sent while this endpoint
        // handshakes (0-RTT and 0.5-RTT data), or, on a connection without 0-RTT, one beside the
        // client's Finished: the application's first data, which could go no sooner. With 0-RTT
        // that data went in it, and what goes beside the Finished is the traffic after. And the end
        // of a message in the application's first round trip: a packet that finishes a stream,
        // sent from the handshake's completion until the peer acknowledges a packet of this space.
        // A server's first reply goes then, the dial's last round trip; lost, it waited a probe
        // timeout (`docs/research/burst-loss.md` §9). A bulk transfer's first window ends no
        // stream and is not copied.
        let first_round = conn.application_from.is_some_and(|from| {
            conn.spaces
                .get(space_id)
                .largest_acked_packet
                .is_none_or(|acked| acked < from)
        }) && sent.stream_frames.iter().any(|frame| frame.fin);
        let handshake_flight = space_id != SpaceId::Data
            || conn.state.is_handshake()
            || (conn.handshake_datagram && !conn.zero_rtt_enabled)
            || first_round;
        let packet = SentPacket {
            path_generation: conn.path.generation(),
            largest_acked: sent.largest_acked,
            time_sent: now,
            size,
            ack_eliciting,
            retransmits: sent.retransmits,
            stream_frames: sent.stream_frames,
            copied: conn.spaces.get(space_id).sending_copies,
            handshake_flight,
            next: None,
        };

        conn.path
            .sent(exact_number, packet, conn.spaces.get_mut(space_id));
        conn.stats.path.sent_packets = conn.stats.path.sent_packets.saturating_add(1);
        conn.reset_keep_alive(now);
        if size != 0 {
            if ack_eliciting {
                conn.spaces
                    .get_mut(space_id)
                    .time_of_last_ack_eliciting_packet = Some(now);
                if conn.permit_idle_reset {
                    conn.reset_idle_timeout(now, space_id);
                }
                conn.permit_idle_reset = false;
            }
            conn.set_loss_detection_timer(now);
            conn.path.pacing.on_transmit(size);
        }
    }

    /// Encrypt packet, returning the length of the packet and whether padding was added
    ///
    /// The keys the packet was begun with are still there unless the connection lost them while
    /// the packet was built, where upstream panicked: then the packet is taken back out of the
    /// buffer, unsent, the connection is marked drained, and `None` is returned.
    pub(super) fn finish(
        self,
        conn: &mut Connection,
        now: Instant,
        buffer: &mut Vec<u8>,
    ) -> Option<(usize, bool)> {
        let encode_start = self.partial_encode.start;
        let Some((header_crypto, packet_crypto)) = local_keys(conn, self.space) else {
            buffer.truncate(encode_start);
            conn.kill(TransportError::INTERNAL_ERROR("no keys to send with").into());
            return None;
        };

        let pad = buffer.len() < self.min_size;
        if pad {
            trace!("PADDING * {}", self.min_size.saturating_sub(buffer.len()));
            buffer.resize(self.min_size, 0);
        }

        // In-memory: the buffer's length plus a tag of a few bytes
        buffer.resize(buffer.len().saturating_add(packet_crypto.tag_len()), 0);
        let packet_buf = buffer.get_mut(encode_start..).unwrap_or_default();
        // The builder pads every packet to its header protection sample (`min_size`); one the
        // keys refuse is taken back out, unsent, where upstream panicked
        if self
            .partial_encode
            .finish(
                packet_buf,
                header_crypto,
                Some((self.exact_number, packet_crypto)),
            )
            .is_err()
        {
            buffer.truncate(encode_start);
            conn.kill(TransportError::INTERNAL_ERROR("packet protection failed").into());
            return None;
        }

        let len = buffer.len().saturating_sub(encode_start);
        conn.qlog.emit_packet_sent(
            self.exact_number,
            len,
            self.space,
            self.space == SpaceId::Data && conn.spaces.get(SpaceId::Data).crypto.is_none(),
            now,
            conn.orig_rem_cid,
        );

        Some((len, pad))
    }
}

/// Initiates a key update as the Data space nears its confidentiality limit, and closes the
/// connection as another space reaches it; false once the limit is passed and the connection is
/// killed
fn keep_within_confidentiality_limit(
    conn: &mut Connection,
    now: Instant,
    space_id: SpaceId,
    confidentiality_limit: u64,
) -> bool {
    let sent_with_keys = conn.spaces.get(space_id).sent_with_keys;
    if space_id == SpaceId::Data {
        if sent_with_keys >= conn.key_phase_size {
            debug!("routine key update due to phase exhaustion");
            conn.force_key_update();
        }
    } else if sent_with_keys.saturating_add(1) == confidentiality_limit {
        // We still have time to attempt a graceful close
        conn.close_inner(
            now,
            Close::Connection(frame::ConnectionClose {
                error_code: TransportErrorCode::AEAD_LIMIT_REACHED,
                frame_type: None,
                reason: Bytes::from_static(b"confidentiality limit reached"),
            }),
        )
    } else if sent_with_keys > confidentiality_limit {
        // Confidentiality limited violated and there's nothing we can do
        conn.kill(TransportError::AEAD_LIMIT_REACHED("confidentiality limit reached").into());
        return false;
    }
    true
}

/// The keys this endpoint sends with in `space`: the space's own, or the 0-RTT keys in the Data
/// space before its 1-RTT keys are installed
pub(super) fn local_keys(
    conn: &Connection,
    space: SpaceId,
) -> Option<(&dyn HeaderKey, &dyn PacketKey)> {
    match &conn.spaces.get(space).crypto {
        Some(crypto) => Some((&*crypto.header.local, &*crypto.packet.local)),
        None if space == SpaceId::Data => conn
            .zero_rtt_crypto
            .as_ref()
            .map(|zero_rtt| (&*zero_rtt.header, &*zero_rtt.packet)),
        None => None,
    }
}
