//! A peer that speaks the transport but drives its endpoint itself, on the test's task: what a
//! test needs to send what focal's connector never would — a request's body in pieces, a request
//! held half sent, a reply left unread — and to see how its connection ends and what it was told.
use super::*;
use crate::net::{Driver, FocalClasses, HELLO_BYTES, Kind, NetDirectory};
use crate::transport::endpoint_driver;
use hyper_transport::tls::Tls;
use hyper_transport::{Epoch, Event, ExchangeId, PeerId, Progress, Refusal, Reservation};

pub(super) struct RawPeer {
    driver: Driver,
    pub(super) peer: PeerId,
    pub(super) epoch: Epoch,
    /// The class of each exchange this peer opened that is still open, for its reply's reservation.
    classes: Vec<(ExchangeId, TrafficClass)>,
    /// The replies begun and not yet read whole.
    reading: Vec<(ExchangeId, Reservation)>,
}

impl RawPeer {
    /// A connection to `to` under `tls`, greeted as focal's connector greets with the base profile.
    pub(super) async fn connect(tls: ClientTls, to: std::net::SocketAddr) -> Self {
        let mut raw = Self::dial(tls, to).await;
        let reply = raw
            .greet(&Hello {
                versions: vec![PROTOCOL_VERSION],
                max_frame_bytes: limits().max_frame_bytes,
                max_items: limits().max_items,
            })
            .await;
        assert!(matches!(reply, HelloReply::Accepted(_)), "{reply:?}");
        raw
    }

    /// A connection to `to` under `tls`, up and not greeted.
    pub(super) async fn dial(tls: ClientTls, to: std::net::SocketAddr) -> Self {
        let role = tls.role();
        let mut driver = endpoint_driver(
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            (
                Tls {
                    server: None,
                    client: tls.into_config(),
                },
                role,
            ),
            NetDirectory::dialing(4),
            &limits(),
            &AdmissionLimits::for_connections(4),
            budget(),
        )
        .unwrap();
        let peer = driver
            .endpoint()
            .directory_mut()
            .dial("localhost", None)
            .unwrap();
        let now = driver.clock().unwrap();
        driver.endpoint().connect(now, peer, to).unwrap();
        let mut raw = Self {
            driver,
            peer,
            epoch: 0,
            classes: Vec::new(),
            reading: Vec::new(),
        };
        raw.epoch = loop {
            match raw.event().await {
                Event::Connected { epoch, .. } => break epoch,
                Event::Unreachable { refusal, .. } | Event::Closed { refusal, .. } => {
                    panic!("the raw peer's dial failed: {refusal:?}")
                }
                _ => {}
            }
        };
        raw
    }

    /// Greets the peer with `hello`, and what it answered.
    pub(super) async fn greet(&mut self, hello: &Hello) -> HelloReply {
        let hello = encode_payload(hello, HELLO_BYTES).unwrap();
        let exchange = self.open(Kind::Hello, TrafficClass::Control, hello.len() as u64);
        assert_eq!(self.write(exchange, &hello), hello.len());
        let reply = self.answer(exchange).await.unwrap();
        decode_payload::<HelloReply>(&reply).unwrap()
    }

    /// Opens an exchange of `kind` declaring a body of `length` bytes, none of it written yet,
    /// and sends what that puts on the wire: its prefix, the peer's `Request`.
    pub(super) fn open(&mut self, kind: Kind, class: TrafficClass, length: u64) -> ExchangeId {
        let now = self.driver.clock().unwrap();
        let progress = Progress::new(limits().request_timeout).unwrap();
        let exchange = self
            .driver
            .endpoint()
            .open(now, self.peer, kind, &[], Some(length), progress)
            .unwrap();
        self.classes.push((exchange, class));
        self.driver.flush().unwrap();
        exchange
    }

    /// Opens the exchange `request` is carried in, declaring a body of `length` bytes.
    pub(super) fn ask(&mut self, request: &RequestEnvelope, length: usize) -> ExchangeId {
        self.open(
            Kind::Operation(request.operation.registered_tag()),
            request.operation.class(),
            length as u64,
        )
    }

    /// Writes what the endpoint takes of `bytes` now, and sends it.
    pub(super) fn write(&mut self, exchange: ExchangeId, bytes: &[u8]) -> usize {
        self.try_write(exchange, bytes).unwrap()
    }

    /// [`Self::write`], or why the endpoint took none of it.
    pub(super) fn try_write(
        &mut self,
        exchange: ExchangeId,
        bytes: &[u8],
    ) -> Result<usize, Refusal> {
        let taken = self.driver.endpoint().write_body(exchange, bytes);
        self.driver.flush().unwrap();
        taken
    }

    /// Writes all of `bytes`, waiting for the peer's credit as it runs out.
    pub(super) async fn write_all(
        &mut self,
        exchange: ExchangeId,
        bytes: &[u8],
    ) -> Result<(), Refusal> {
        let mut written = 0;
        while written < bytes.len() {
            written += self.try_write(exchange, &bytes[written..])?;
            if written < bytes.len() {
                loop {
                    match self.event().await {
                        Event::Writable { exchange: of } if of == exchange => break,
                        Event::Refused {
                            exchange: of,
                            refusal,
                            ..
                        } if of == exchange => return Err(refusal),
                        Event::Closed { refusal, .. } => {
                            return Err(refusal.unwrap_or(Refusal::Closed));
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Closes the connection, its exchanges with it.
    pub(super) fn disconnect(&mut self) {
        let now = self.driver.clock().unwrap();
        self.driver.endpoint().disconnect(now, self.peer);
        self.driver.flush().unwrap();
    }

    /// The next event, what the endpoint has to send sent first.
    pub(super) async fn event(&mut self) -> Event<FocalClasses> {
        loop {
            self.driver.flush().unwrap();
            match self.driver.event().await.unwrap() {
                Event::Frame { frame, .. } => self.driver.endpoint().release(frame),
                event => return event,
            }
        }
    }

    /// Waits for the reply to `exchange` to begin — its peer's write begun — and reserves its body,
    /// reading none of it: its declared length.
    pub(super) async fn replied(&mut self, exchange: ExchangeId) -> Result<u64, Refusal> {
        loop {
            match self.event().await {
                Event::Reply { exchange: of, body } if of == exchange => {
                    let length = body.unwrap_or(0);
                    let class = self.class_of(exchange);
                    let reservation = self.driver.endpoint().reserve(class, length)?;
                    self.reading.push((exchange, reservation));
                    return Ok(length);
                }
                Event::Refused {
                    exchange: of,
                    refusal,
                    ..
                } if of == exchange => {
                    self.classes.retain(|(open, _)| *open != exchange);
                    return Err(refusal);
                }
                Event::Closed { refusal, .. } | Event::Unreachable { refusal, .. } => {
                    return Err(refusal.unwrap_or(Refusal::Closed));
                }
                _ => {}
            }
        }
    }

    /// The reply to `exchange`, read whole, or why it never came; the events about others are
    /// passed over.
    pub(super) async fn answer(&mut self, exchange: ExchangeId) -> Result<Vec<u8>, Refusal> {
        if !self.reading.iter().any(|(of, _)| *of == exchange) {
            self.replied(exchange).await?;
        }
        loop {
            if let Some(body) = self.read(exchange)? {
                return Ok(body);
            }
            match self.event().await {
                Event::BodyReady { exchange: of } if of == exchange => {}
                Event::Refused {
                    exchange: of,
                    refusal,
                    ..
                } if of == exchange => {
                    self.forget(exchange);
                    return Err(refusal);
                }
                Event::Closed { refusal, .. } => {
                    self.forget(exchange);
                    return Err(refusal.unwrap_or(Refusal::Closed));
                }
                _ => {}
            }
        }
    }

    /// A message of the enrollment protocol of kind `tag`, carried whole, and its answer.
    pub(super) async fn enrollment(&mut self, tag: u8, body: &[u8]) -> Result<Vec<u8>, Refusal> {
        let exchange = self.open(
            Kind::Enrollment(tag),
            TrafficClass::Control,
            body.len() as u64,
        );
        self.write_all(exchange, body).await?;
        self.answer(exchange).await
    }

    /// `request`, carried whole, and its response.
    pub(super) async fn request(
        &mut self,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, Refusal> {
        let body = encode_payload(request, limits().max_frame_bytes).unwrap();
        let exchange = self.ask(request, body.len());
        self.write_all(exchange, &body).await?;
        let answer = self.answer(exchange).await?;
        Ok(decode_payload(&answer).unwrap())
    }

    /// How the connection ended: what its peer or its handshake said, if anything.
    pub(super) async fn closed(&mut self) -> Option<Refusal> {
        loop {
            if let Event::Closed { refusal, .. } = self.event().await {
                return refusal;
            }
        }
    }

    fn class_of(&self, exchange: ExchangeId) -> TrafficClass {
        self.classes
            .iter()
            .find(|(open, _)| *open == exchange)
            .map(|(_, class)| *class)
            .unwrap()
    }

    /// What has arrived of the reply to `exchange`, read into its reservation; the body once it is
    /// whole.
    fn read(&mut self, exchange: ExchangeId) -> Result<Option<Vec<u8>>, Refusal> {
        let at = self
            .reading
            .iter()
            .position(|(of, _)| *of == exchange)
            .unwrap();
        let (_, into) = &mut self.reading[at];
        self.driver.endpoint().read_body(exchange, into)?;
        if !self.driver.endpoint().body_complete(exchange) {
            return Ok(None);
        }
        self.driver.endpoint().end(exchange);
        self.classes.retain(|(open, _)| *open != exchange);
        let (_, reservation) = self.reading.remove(at);
        let (granted, tag) = (reservation.granted(), reservation.tag());
        let body = reservation.into_buffer();
        self.driver
            .endpoint()
            .release(Reservation::tagged(granted, Vec::new(), tag));
        Ok(Some(body))
    }

    /// An exchange that ended: its reservation, if its reply began, goes back.
    fn forget(&mut self, exchange: ExchangeId) {
        self.classes.retain(|(open, _)| *open != exchange);
        if let Some(at) = self.reading.iter().position(|(of, _)| *of == exchange) {
            let (_, reservation) = self.reading.remove(at);
            self.driver.endpoint().release(reservation);
        }
    }
}

/// A server that speaks the transport but answers as no focal server would, driven on a task of
/// the test's: it greets as focal's listener greets and accepts every request whose body is no
/// longer than its bound, and never reads a longer one, whose stream then holds its sender to
/// what flow control lets it send.
pub(super) struct RawServer {
    driver: Driver,
    limits: WireLimits,
    /// The requests being read, with their kind.
    reading: Vec<(ExchangeId, Kind, Reservation)>,
    /// The replies being written, and how much of each is.
    replying: Vec<(ExchangeId, Vec<u8>, usize)>,
}

impl RawServer {
    /// A listener on a port of its own presenting `tls`, its peers named by `registry`.
    pub(super) fn listen(tls: ServerTls, registry: PeerRegistry, limits: WireLimits) -> Self {
        let driver = endpoint_driver(
            "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
            (
                Tls {
                    server: Some(tls.into_config()),
                    client: crate::transport::listener_client().unwrap(),
                },
                crate::net::Sender::Node,
            ),
            NetDirectory::serving(registry, None, 16),
            &limits,
            &AdmissionLimits::for_connections(4),
            budget(),
        )
        .unwrap();
        Self {
            driver,
            limits,
            reading: Vec::new(),
            replying: Vec::new(),
        }
    }

    pub(super) fn local_addr(&self) -> std::net::SocketAddr {
        self.driver.local_addr().unwrap()
    }

    /// Serves until its task is cancelled; a request whose body is longer than `read_up_to` is
    /// never read.
    pub(super) async fn serve(mut self, read_up_to: u64) {
        loop {
            self.driver.flush().unwrap();
            let Ok(event) = self.driver.event().await else {
                return;
            };
            match event {
                Event::Request {
                    exchange,
                    kind,
                    class,
                    body,
                    ..
                } => {
                    let length = body.unwrap_or(0);
                    if kind != Kind::Hello && length > read_up_to {
                        continue;
                    }
                    let Ok(reservation) = self.driver.endpoint().reserve(class, length) else {
                        self.driver.endpoint().end(exchange);
                        continue;
                    };
                    self.reading.push((exchange, kind, reservation));
                    self.read(exchange);
                }
                Event::BodyReady { exchange } => self.read(exchange),
                Event::Writable { exchange } => self.write(exchange),
                Event::Frame { frame, .. } => self.driver.endpoint().release(frame),
                _ => {}
            }
        }
    }

    fn read(&mut self, exchange: ExchangeId) {
        let Some(at) = self.reading.iter().position(|(of, _, _)| *of == exchange) else {
            return;
        };
        let (_, kind, into) = &mut self.reading[at];
        let kind = *kind;
        if self.driver.endpoint().read_body(exchange, into).is_err() {
            let (_, _, reservation) = self.reading.remove(at);
            self.driver.endpoint().release(reservation);
            self.driver.endpoint().end(exchange);
            return;
        }
        if !self.driver.endpoint().body_complete(exchange) {
            return;
        }
        let (_, _, reservation) = self.reading.remove(at);
        let (granted, tag) = (reservation.granted(), reservation.tag());
        let body = reservation.into_buffer();
        self.driver
            .endpoint()
            .release(Reservation::tagged(granted, Vec::new(), tag));
        let reply = if kind == Kind::Hello {
            let hello: Hello = decode_payload(&body).unwrap();
            let reply = match self.limits.negotiate(&hello) {
                Ok(negotiated) => HelloReply::Accepted(negotiated),
                Err(error) => HelloReply::Rejected(error),
            };
            encode_payload(&reply, HELLO_BYTES).unwrap()
        } else {
            let asked: RequestEnvelope = decode_payload(&body).unwrap();
            encode_payload(
                &asked.reply(Response::PeerAccepted),
                self.limits.max_frame_bytes,
            )
            .unwrap()
        };
        if self
            .driver
            .endpoint()
            .reply(exchange, &[], Some(reply.len() as u64))
            .is_err()
        {
            self.driver.endpoint().end(exchange);
            return;
        }
        self.replying.push((exchange, reply, 0));
        self.write(exchange);
    }

    fn write(&mut self, exchange: ExchangeId) {
        let Some(at) = self.replying.iter().position(|(of, _, _)| *of == exchange) else {
            return;
        };
        let (_, reply, written) = &mut self.replying[at];
        let done = match self
            .driver
            .endpoint()
            .write_body(exchange, &reply[*written..])
        {
            Ok(taken) => {
                *written += taken;
                *written >= reply.len()
            }
            Err(_) => true,
        };
        if done {
            self.replying.remove(at);
            self.driver.endpoint().end(exchange);
        }
    }
}
