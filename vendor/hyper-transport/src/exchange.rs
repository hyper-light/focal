//! One exchange's state, and the stream reads and writes every exchange and lane is made of.
//!
//! An exchange is one bidirectional QUIC stream: a message each way (node.md §3.2, focal's
//! exchanges). Each message is a prefix, a head and an optional body ([`crate::frame`]). The
//! outgoing message's prefix and head are one contiguous buffer, reserved from the budget and
//! written to QUIC whole when credit allows; its body is written by the owner as it has it. The
//! incoming message's prefix is read into a fixed buffer, its head into a reservation taken only
//! after the prefix's lengths were checked, and its body by the owner, into reservations of its own,
//! so QUIC's flow control holds the sender back while the owner has nowhere to put the bytes.

use std::time::Instant;

use hyper_quic::{Connection, ReadError, StreamId, VarInt, WriteError};

use crate::frame::{BodySum, PREFIX_BYTES, Prefix, TRAILER_BYTES};
use crate::progress::Carry;
use crate::{PeerId, Reservation};

fn length(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// How a read from a stream ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    /// More may arrive.
    Open,
    /// Nothing more has arrived yet.
    Blocked,
    /// The peer finished the stream and everything it sent was read.
    Finished,
    /// The peer reset the stream with this code.
    Reset(u64),
    /// The stream is gone.
    Closed,
}

/// Read at most `max` bytes of stream `id`, handing each piece to `sink`.
pub(crate) fn pull(
    connection: &mut Connection,
    id: StreamId,
    max: usize,
    mut sink: impl FnMut(&[u8]),
) -> (usize, End) {
    let mut recv = connection.recv_stream(id);
    let Ok(mut chunks) = recv.read(true) else {
        return (0, End::Closed);
    };
    let mut got = 0usize;
    let end = loop {
        let want = max.saturating_sub(got);
        if want == 0 {
            break End::Open;
        }
        match chunks.next(want) {
            Ok(Some(chunk)) => {
                sink(&chunk.bytes);
                got = got.saturating_add(chunk.bytes.len());
            }
            Ok(None) => break End::Finished,
            Err(ReadError::Blocked) => break End::Blocked,
            Err(ReadError::Reset(code)) => break End::Reset(code.into_inner()),
        }
    };
    let _ = chunks.finalize();
    (got, end)
}

/// How a write to a stream ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Pushed {
    /// This many bytes were taken; fewer than offered when credit ran out.
    Took(usize),
    /// The peer stopped the stream with this code.
    Stopped(u64),
    /// The stream is gone.
    Closed,
}

/// Write `bytes` to stream `id`, no more than `allowed`.
pub(crate) fn push(
    connection: &mut Connection,
    id: StreamId,
    bytes: &[u8],
    allowed: u64,
) -> Pushed {
    let allowed = usize::try_from(allowed).unwrap_or(usize::MAX);
    let Some(bytes) = bytes.get(..bytes.len().min(allowed)) else {
        return Pushed::Took(0);
    };
    if bytes.is_empty() {
        return Pushed::Took(0);
    }
    match connection.send_stream(id).write(bytes) {
        Ok(took) => Pushed::Took(took),
        Err(WriteError::Blocked) => Pushed::Took(0),
        Err(WriteError::Stopped(code)) => Pushed::Stopped(code.into_inner()),
        Err(WriteError::ClosedStream) => Pushed::Closed,
    }
}

/// Reset the sending half and stop the receiving half of stream `id`, each with `code`; a half that
/// already ended is left as it is.
pub(crate) fn abandon(connection: &mut Connection, id: StreamId, code: u32) {
    let code = VarInt::from_u32(code);
    let _ = connection.send_stream(id).reset(code);
    let _ = connection.recv_stream(id).stop(code);
}

/// Where the outgoing message stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Out {
    /// Nothing to send yet: a served exchange before its reply.
    Nothing,
    /// The prefix and head are being written.
    Head,
    /// The body is being written by the owner.
    Body,
    /// The body's checksum is being written.
    Trailer,
    /// Everything was written and the stream finished.
    Finished,
}

/// The outgoing message.
#[derive(Debug)]
pub(crate) struct Outgoing {
    pub(crate) state: Out,
    /// The prefix and the head, contiguous.
    pub(crate) message: Option<Reservation>,
    pub(crate) written: usize,
    /// The body still to write.
    pub(crate) left: u64,
    pub(crate) has_body: bool,
    pub(crate) sum: BodySum,
    pub(crate) trailer_written: usize,
}

impl Outgoing {
    pub(crate) fn nothing() -> Self {
        Self {
            state: Out::Nothing,
            message: None,
            written: 0,
            left: 0,
            has_body: false,
            sum: BodySum::default(),
            trailer_written: 0,
        }
    }
    /// The bytes still to be written: what the progress deadline charges the connection with.
    pub(crate) fn pending(&self) -> u64 {
        let message = self
            .message
            .as_ref()
            .map_or(0, |message| message.bytes().len());
        let message = u64::try_from(message.saturating_sub(self.written)).unwrap_or(u64::MAX);
        let trailer = if self.has_body && self.state != Out::Finished {
            u64::try_from(TRAILER_BYTES.saturating_sub(self.trailer_written)).unwrap_or(0)
        } else {
            0
        };
        message.saturating_add(self.left).saturating_add(trailer)
    }
}

/// Where the incoming message stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum In {
    /// The prefix is arriving.
    Prefix,
    /// The head is arriving into its reservation.
    Head,
    /// The body is the owner's to read.
    Body,
    /// The body's checksum is arriving.
    Trailer,
    /// The message is whole; the stream's end is still to be read.
    Whole,
    /// The stream's end was read.
    Finished,
}

/// The incoming message.
#[derive(Debug)]
pub(crate) struct Incoming {
    pub(crate) state: In,
    pub(crate) prefix: [u8; PREFIX_BYTES],
    pub(crate) filled: usize,
    pub(crate) decoded: Option<Prefix>,
    pub(crate) head: Option<Reservation>,
    pub(crate) left: u64,
    pub(crate) sum: BodySum,
    pub(crate) trailer: [u8; TRAILER_BYTES],
    pub(crate) trailer_filled: usize,
}

impl Incoming {
    pub(crate) fn new() -> Self {
        Self {
            state: In::Prefix,
            prefix: [0; PREFIX_BYTES],
            filled: 0,
            decoded: None,
            head: None,
            left: 0,
            sum: BodySum::default(),
            trailer: [0; TRAILER_BYTES],
            trailer_filled: 0,
        }
    }
    /// What of the message is declared and has still to arrive: its head, body and trailer once
    /// its prefix is in; nothing before.
    pub(crate) fn owed(&self) -> u64 {
        let trailer = |filled: usize| length(TRAILER_BYTES.saturating_sub(filled));
        match self.state {
            In::Prefix | In::Whole | In::Finished => 0,
            In::Head => {
                let head = self.head.as_ref().map_or(0, Reservation::room);
                let body = self.decoded.and_then(|prefix| prefix.body);
                body.map_or(head, |body| {
                    head.saturating_add(body)
                        .saturating_add(trailer(self.trailer_filled))
                })
            }
            In::Body => self.left.saturating_add(trailer(self.trailer_filled)),
            In::Trailer => trailer(self.trailer_filled),
        }
    }
    /// Whether the message, its body's checksum included, has been read and verified.
    pub(crate) fn whole(&self) -> bool {
        matches!(self.state, In::Whole | In::Finished)
    }
}

/// One exchange.
#[derive(Debug)]
pub(crate) struct Exchange<K> {
    /// The endpoint's key for the connection it runs on.
    pub(crate) connection: usize,
    pub(crate) peer: PeerId,
    /// Its stream, once one was opened or accepted.
    pub(crate) stream: Option<StreamId>,
    /// Whether this side asked.
    pub(crate) opened: bool,
    /// Its class, once known: at once for an exchange this side opens, from the request's prefix
    /// for one a peer opened.
    pub(crate) class: Option<K>,
    pub(crate) rank: u8,
    pub(crate) out: Outgoing,
    pub(crate) incoming: Incoming,
    pub(crate) carry: Carry,
    /// For an exchange queued for a stream, the connection's last credit-queue advance its wait was last judged on
    /// (policy C): a further advance restarts its wait; none for a whole period refuses it as stalled.
    pub(crate) credit_seen: Option<Instant>,
    /// Its share of its connection's sums as last counted (`tally::Table::settle`).
    pub(crate) counted: crate::tally::Counted,
    /// It changed since its share was last counted: it is in the table's touched list.
    pub(crate) touched: bool,
    /// Its place in the order exchanges were made, the endpoint's whole life: the order its
    /// connection's exchanges are visited in.
    pub(crate) order: u64,
    /// It is in its connection's set of exchanges a progress pass visits (`tally::visits`).
    pub(crate) active: bool,
    /// Its index in its connection's list of exchanges.
    pub(crate) at: usize,
    /// The due time its connection's deadline heap holds for it, if any (`tally::Table`).
    pub(crate) deadline: Option<Instant>,
    pub(crate) began: Instant,
    /// Whether the peer's message has begun (a reply's prefix and head arrived).
    pub(crate) answered: bool,
    /// A [`crate::Event::BodyReady`] is outstanding.
    pub(crate) ready_sent: bool,
    /// A [`crate::Event::BodyReady`] waits in the queue, unpolled: another says nothing more.
    pub(crate) ready_waiting: bool,
    /// The owner was told it could not write all it offered; it hears [`crate::Event::Writable`]
    /// when it can write more.
    pub(crate) wants_write: bool,
    /// A [`crate::Event::Writable`] is outstanding; the owner's next write answers it.
    pub(crate) writable_sent: bool,
    /// A [`crate::Event::Writable`] waits in the queue, unpolled: another says nothing more.
    pub(crate) writable_waiting: bool,
    /// QUIC refused a write the connection's credit allowed: the stream's own window is spent,
    /// and only QUIC's `Writable` for the stream says it can take more.
    pub(crate) stream_blocked: bool,
    /// The owner's last read of the body wanted more than had arrived: only then is a period that
    /// brought nothing evidence against the sender.
    pub(crate) starved: bool,
    /// The owner ended it while only this side's trailer was left to send: it stays, unseen by
    /// the owner, until the trailer is sent, and then goes.
    pub(crate) ended: bool,
}
