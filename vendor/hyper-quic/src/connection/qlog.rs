use std::time::Duration;

use qlog::{
    events::{
        Event, EventData,
        quic::{
            PacketHeader, PacketLost, PacketLostTrigger, PacketReceived, PacketSent, PacketType,
        },
    },
    streamer::QlogStreamer,
};
use tracing::warn;

use crate::{
    ConnectionId, Instant,
    connection::{PathData, SentPacket},
    packet::SpaceId,
};

/// One connection's qlog output stream, owned by that connection
pub struct QlogStream(pub(crate) QlogStreamer);

impl QlogStream {
    fn emit_event(&mut self, orig_rem_cid: ConnectionId, event: EventData, now: Instant) {
        // Time will be overwritten by `add_event_with_instant`
        let mut event = Event::with_time(0.0, event);
        event.group_id = Some(orig_rem_cid.to_string());

        if let Err(e) = self.0.add_event_with_instant(event, now) {
            warn!("could not emit qlog event: {e}");
        }
    }
}

/// A connection's [`QlogStream`], if it has one
#[derive(Default)]
pub(crate) struct QlogSink {
    stream: Option<QlogStream>,
}

impl QlogSink {
    pub(super) fn emit_recovery_metrics(
        &mut self,
        pto_count: u32,
        path: &mut PathData,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };

            let Some(metrics) = path.qlog_recovery_metrics(pto_count) else {
                return;
            };

            stream.emit_event(orig_rem_cid, EventData::MetricsUpdated(metrics), now);
        }
    }

    pub(super) fn emit_packet_lost(
        &mut self,
        pn: u64,
        info: &SentPacket,
        loss_delay: Duration,
        space: SpaceId,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };

            let event = PacketLost {
                header: Some(PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, false),
                    length: Some(info.size),
                    ..Default::default()
                }),
                frames: None,
                trigger: Some(
                    match info.time_sent.saturating_duration_since(now) >= loss_delay {
                        true => PacketLostTrigger::TimeThreshold,
                        false => PacketLostTrigger::ReorderingThreshold,
                    },
                ),
            };

            stream.emit_event(orig_rem_cid, EventData::PacketLost(event), now);
        }
    }

    pub(super) fn emit_packet_sent(
        &mut self,
        pn: u64,
        len: usize,
        space: SpaceId,
        is_0rtt: bool,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };

            let event = PacketSent {
                header: PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, is_0rtt),
                    length: u16::try_from(len).ok(),
                    ..Default::default()
                },
                ..Default::default()
            };

            stream.emit_event(orig_rem_cid, EventData::PacketSent(event), now);
        }
    }

    pub(super) fn emit_packet_received(
        &mut self,
        pn: u64,
        space: SpaceId,
        is_0rtt: bool,
        now: Instant,
        orig_rem_cid: ConnectionId,
    ) {
        {
            let Some(stream) = self.stream.as_mut() else {
                return;
            };

            let event = PacketReceived {
                header: PacketHeader {
                    packet_number: Some(pn),
                    packet_type: packet_type(space, is_0rtt),
                    ..Default::default()
                },
                ..Default::default()
            };

            stream.emit_event(orig_rem_cid, EventData::PacketReceived(event), now);
        }
    }
}

impl From<Option<QlogStream>> for QlogSink {
    fn from(stream: Option<QlogStream>) -> Self {
        Self { stream }
    }
}

fn packet_type(space: SpaceId, is_0rtt: bool) -> PacketType {
    match space {
        SpaceId::Initial => PacketType::Initial,
        SpaceId::Handshake => PacketType::Handshake,
        SpaceId::Data if is_0rtt => PacketType::ZeroRtt,
        SpaceId::Data => PacketType::OneRtt,
    }
}
