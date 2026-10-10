//! Connection statistics

use crate::{Dir, Duration, frame::Frame};

/// Statistics about UDP datagrams transmitted or received on a connection
///
/// All QUIC packets are carried by UDP datagrams. Hence, these statistics cover all traffic on a connection.
#[derive(Default, Debug, Copy, Clone)]
#[non_exhaustive]
pub struct UdpStats {
    /// The amount of UDP datagrams observed
    pub datagrams: u64,
    /// The total amount of bytes which have been transferred inside UDP datagrams
    pub bytes: u64,
    /// The amount of I/O operations executed
    ///
    /// Can be less than `datagrams` when GSO, GRO, and/or batched system calls are in use.
    pub ios: u64,
}

// Statistics saturate: a count at u64::MAX stays there rather than wrapping to a small number.
impl UdpStats {
    pub(crate) fn on_sent(&mut self, datagrams: u64, bytes: usize) {
        self.datagrams = self.datagrams.saturating_add(datagrams);
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        self.ios = self.ios.saturating_add(1);
    }
}

/// Number of frames transmitted or received of each frame type
#[derive(Default, Copy, Clone)]
#[non_exhaustive]
pub struct FrameStats {
    /// ACK frames
    pub acks: u64,
    /// ACK_FREQUENCY frames
    pub ack_frequency: u64,
    /// CRYPTO frames
    pub crypto: u64,
    /// CONNECTION_CLOSE frames
    pub connection_close: u64,
    /// DATA_BLOCKED frames
    pub data_blocked: u64,
    /// DATAGRAM frames
    pub datagram: u64,
    /// HANDSHAKE_DONE frames, typically sent once
    pub handshake_done: u8,
    /// IMMEDIATE_ACK frames
    pub immediate_ack: u64,
    /// MAX_DATA frames
    pub max_data: u64,
    /// MAX_STREAM_DATA frames
    pub max_stream_data: u64,
    /// MAX_STREAMS frames for bidirectional streams
    pub max_streams_bidi: u64,
    /// MAX_STREAMS frames for unidirectional streams
    pub max_streams_uni: u64,
    /// NEW_CONNECTION_ID frames
    pub new_connection_id: u64,
    /// NEW_TOKEN frames
    pub new_token: u64,
    /// PATH_CHALLENGE frames
    pub path_challenge: u64,
    /// PATH_RESPONSE frames
    pub path_response: u64,
    /// PING frames
    pub ping: u64,
    /// RESET_STREAM frames
    pub reset_stream: u64,
    /// RETIRE_CONNECTION_ID frames
    pub retire_connection_id: u64,
    /// STREAM_DATA_BLOCKED frames
    pub stream_data_blocked: u64,
    /// STREAMS_BLOCKED frames for bidirectional streams
    pub streams_blocked_bidi: u64,
    /// STREAMS_BLOCKED frames for unidirectional streams
    pub streams_blocked_uni: u64,
    /// STOP_SENDING frames
    pub stop_sending: u64,
    /// STREAM frames
    pub stream: u64,
}

impl FrameStats {
    pub(crate) fn record(&mut self, frame: &Frame) {
        match frame {
            Frame::Padding => {}
            Frame::Ping => self.ping = self.ping.saturating_add(1),
            Frame::Ack(_) => self.acks = self.acks.saturating_add(1),
            Frame::ResetStream(_) => self.reset_stream = self.reset_stream.saturating_add(1),
            Frame::StopSending(_) => self.stop_sending = self.stop_sending.saturating_add(1),
            Frame::Crypto(_) => self.crypto = self.crypto.saturating_add(1),
            Frame::Datagram(_) => self.datagram = self.datagram.saturating_add(1),
            Frame::NewToken(_) => self.new_token = self.new_token.saturating_add(1),
            Frame::MaxData(_) => self.max_data = self.max_data.saturating_add(1),
            Frame::MaxStreamData { .. } => {
                self.max_stream_data = self.max_stream_data.saturating_add(1)
            }
            Frame::MaxStreams { dir, .. } => {
                if *dir == Dir::Bi {
                    self.max_streams_bidi = self.max_streams_bidi.saturating_add(1);
                } else {
                    self.max_streams_uni = self.max_streams_uni.saturating_add(1);
                }
            }
            Frame::DataBlocked { .. } => self.data_blocked = self.data_blocked.saturating_add(1),
            Frame::Stream(_) => self.stream = self.stream.saturating_add(1),
            Frame::StreamDataBlocked { .. } => {
                self.stream_data_blocked = self.stream_data_blocked.saturating_add(1)
            }
            Frame::StreamsBlocked { dir, .. } => {
                if *dir == Dir::Bi {
                    self.streams_blocked_bidi = self.streams_blocked_bidi.saturating_add(1);
                } else {
                    self.streams_blocked_uni = self.streams_blocked_uni.saturating_add(1);
                }
            }
            Frame::NewConnectionId(_) => {
                self.new_connection_id = self.new_connection_id.saturating_add(1)
            }
            Frame::RetireConnectionId { .. } => {
                self.retire_connection_id = self.retire_connection_id.saturating_add(1)
            }
            Frame::PathChallenge(_) => self.path_challenge = self.path_challenge.saturating_add(1),
            Frame::PathResponse(_) => self.path_response = self.path_response.saturating_add(1),
            Frame::Close(_) => self.connection_close = self.connection_close.saturating_add(1),
            Frame::AckFrequency(_) => self.ack_frequency = self.ack_frequency.saturating_add(1),
            Frame::ImmediateAck => self.immediate_ack = self.immediate_ack.saturating_add(1),
            Frame::HandshakeDone => self.handshake_done = self.handshake_done.saturating_add(1),
        }
    }
}

impl std::fmt::Debug for FrameStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameStats")
            .field("ACK", &self.acks)
            .field("ACK_FREQUENCY", &self.ack_frequency)
            .field("CONNECTION_CLOSE", &self.connection_close)
            .field("CRYPTO", &self.crypto)
            .field("DATA_BLOCKED", &self.data_blocked)
            .field("DATAGRAM", &self.datagram)
            .field("HANDSHAKE_DONE", &self.handshake_done)
            .field("IMMEDIATE_ACK", &self.immediate_ack)
            .field("MAX_DATA", &self.max_data)
            .field("MAX_STREAM_DATA", &self.max_stream_data)
            .field("MAX_STREAMS_BIDI", &self.max_streams_bidi)
            .field("MAX_STREAMS_UNI", &self.max_streams_uni)
            .field("NEW_CONNECTION_ID", &self.new_connection_id)
            .field("NEW_TOKEN", &self.new_token)
            .field("PATH_CHALLENGE", &self.path_challenge)
            .field("PATH_RESPONSE", &self.path_response)
            .field("PING", &self.ping)
            .field("RESET_STREAM", &self.reset_stream)
            .field("RETIRE_CONNECTION_ID", &self.retire_connection_id)
            .field("STREAM_DATA_BLOCKED", &self.stream_data_blocked)
            .field("STREAMS_BLOCKED_BIDI", &self.streams_blocked_bidi)
            .field("STREAMS_BLOCKED_UNI", &self.streams_blocked_uni)
            .field("STOP_SENDING", &self.stop_sending)
            .field("STREAM", &self.stream)
            .finish()
    }
}

/// Statistics related to a transmission path
#[derive(Debug, Default, Copy, Clone)]
#[non_exhaustive]
pub struct PathStats {
    /// Current best estimate of this connection's latency (round-trip-time)
    pub rtt: Duration,
    /// Minimum RTT seen on this path, ignoring ack delay
    pub min_rtt: Duration,
    /// Current congestion window of the connection
    pub cwnd: u64,
    /// Congestion events on the connection
    pub congestion_events: u64,
    /// The amount of packets lost on this path
    pub lost_packets: u64,
    /// The amount of bytes lost on this path
    pub lost_bytes: u64,
    /// The amount of packets sent on this path
    pub sent_packets: u64,
    /// The amount of PLPMTUD probe packets sent on this path (also counted by `sent_packets`)
    pub sent_plpmtud_probes: u64,
    /// The amount of PLPMTUD probe packets lost on this path (ignored by `lost_packets` and
    /// `lost_bytes`)
    pub lost_plpmtud_probes: u64,
    /// The packets the path's warm-up sent to measure it for Careful Resume (also counted by
    /// `sent_packets`)
    pub sent_warm_up_packets: u64,
    /// The number of times a black hole was detected in the path
    pub black_holes_detected: u64,
    /// Largest UDP payload size the path currently supports
    pub current_mtu: u16,
}

/// Connection statistics
#[derive(Debug, Default, Copy, Clone)]
#[non_exhaustive]
pub struct ConnectionStats {
    /// Statistics about UDP datagrams transmitted on a connection
    pub udp_tx: UdpStats,
    /// Statistics about UDP datagrams received on a connection
    pub udp_rx: UdpStats,
    /// Statistics about frames transmitted on a connection
    pub frame_tx: FrameStats,
    /// Statistics about frames received on a connection
    pub frame_rx: FrameStats,
    /// Statistics related to the current transmission path
    pub path: PathStats,
}
