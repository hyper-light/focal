//! Unbuffered connection API

use alloc::vec::Vec;
use core::error::Error as StdError;
use core::num::NonZeroUsize;
use core::{fmt, mem};

use super::{SideData, UnbufferedConnectionCommon};
use crate::client::{ClientConfig, ClientConnectionData};
use crate::msgs::deframer::buffers::{BufferProgress, DeframerSliceBuffer};
use crate::server::{ServerConfig, ServerConnectionData};
use crate::Error;

impl UnbufferedConnectionCommon<ClientConnectionData> {
    /// Processes the TLS records in `incoming_tls` buffer until a new [`UnbufferedStatus`] is
    /// reached.
    ///
    /// `config` is the configuration this connection was made with; the connection holds none.
    pub fn process_tls_records<'c, 'i>(
        &'c mut self,
        config: &mut ClientConfig,
        incoming_tls: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, ClientConnectionData> {
        // A client never receives early data.
        self.process_tls_records_common(config, incoming_tls, |_| false, None)
    }
}

impl UnbufferedConnectionCommon<ServerConnectionData> {
    /// Processes the TLS records in `incoming_tls` buffer until a new [`UnbufferedStatus`] is
    /// reached.
    ///
    /// `config` is the configuration this connection was made with; the connection holds none.
    pub fn process_tls_records<'c, 'i>(
        &'c mut self,
        config: &mut ServerConfig,
        incoming_tls: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, ServerConnectionData> {
        self.process_tls_records_common(
            config,
            incoming_tls,
            |conn| conn.peek_early_data().is_some(),
            Some(|conn, incoming_tls| ReadEarlyData::new(conn, incoming_tls).into()),
        )
    }
}

/// Makes the state that hands received early data to the caller.
type EarlyDataState<'c, 'i, Data> =
    fn(&'c mut UnbufferedConnectionCommon<Data>, &'i mut [u8]) -> ConnectionState<'c, 'i, Data>;

impl<Data: SideData> UnbufferedConnectionCommon<Data> {
    fn process_tls_records_common<'c, 'i>(
        &'c mut self,
        config: &mut Data::Config,
        incoming_tls: &'i mut [u8],
        mut early_data_available: impl FnMut(&mut Self) -> bool,
        early_data_state: Option<EarlyDataState<'c, 'i, Data>>,
    ) -> UnbufferedStatus<'c, 'i, Data> {
        let mut buffer = DeframerSliceBuffer::new(incoming_tls);
        let mut buffer_progress = self.core.hs_deframer.progress();

        let (discard, state) = loop {
            if let (true, Some(early_data_state)) = (early_data_available(self), early_data_state) {
                break (
                    buffer.pending_discard(),
                    early_data_state(self, incoming_tls),
                );
            }

            if !self.core.common_state.received_plaintext.is_empty() {
                break (
                    buffer.pending_discard(),
                    ReadTraffic::new(self, incoming_tls).into(),
                );
            }

            if let Some(chunk) = self.core.common_state.sendable_tls.pop() {
                break (
                    buffer.pending_discard(),
                    EncodeTlsData::new(self, chunk).into(),
                );
            }

            match self.process_next_message(config, &mut buffer, &mut buffer_progress) {
                Ok(true) => {
                    buffer.queue_discard(buffer_progress.take_discard());
                    continue;
                }
                Ok(false) => {}
                Err(err) => {
                    buffer.queue_discard(buffer_progress.take_discard());
                    return UnbufferedStatus {
                        discard: buffer.pending_discard(),
                        state: Err(err),
                    };
                }
            }

            break (buffer.pending_discard(), self.idle_state());
        };

        UnbufferedStatus {
            discard,
            state: Ok(state),
        }
    }

    /// Deframes the next message and runs the handshake on it: `Ok(false)` when there is none.
    /// A failure of the handshake is kept as the connection's state.
    fn process_next_message(
        &mut self,
        config: &mut Data::Config,
        buffer: &mut DeframerSliceBuffer<'_>,
        buffer_progress: &mut BufferProgress,
    ) -> Result<bool, Error> {
        if self.core.common_state.has_received_close_notify {
            return Ok(false);
        }
        let Some(msg) = self.core.deframe(buffer.filled_mut(), buffer_progress)? else {
            return Ok(false);
        };

        let state = match mem::replace(&mut self.core.state, Err(Error::HandshakeNotComplete)) {
            Ok(state) => state,
            Err(e) => {
                self.core.state = Err(e.clone());
                return Err(e);
            }
        };

        match self
            .core
            .process_msg(msg, state, None, config)
            .and_then(|new| self.core.common_state.take_encrypt_failure().map(|()| new))
        {
            Ok(new) => {
                self.core.state = Ok(new);
                Ok(true)
            }
            Err(e) => {
                self.core.state = Err(e.clone());
                Err(e)
            }
        }
    }

    /// The state to report once there is nothing to read, encode or process.
    fn idle_state<'c, 'i>(&'c mut self) -> ConnectionState<'c, 'i, Data> {
        let common = &self.core.common_state;
        if self.wants_write {
            TransmitTlsData { conn: self }.into()
        } else if common.has_received_close_notify && !self.emitted_peer_closed_state {
            self.emitted_peer_closed_state = true;
            ConnectionState::PeerClosed
        } else if common.has_received_close_notify && common.has_sent_close_notify {
            ConnectionState::Closed
        } else if common.may_send_application_data {
            ConnectionState::WriteTraffic(WriteTraffic { conn: self })
        } else {
            ConnectionState::BlockedHandshake
        }
    }
}

/// The current status of the `UnbufferedConnection*`
#[must_use]
#[derive(Debug)]
pub struct UnbufferedStatus<'c, 'i, Data> {
    /// Number of bytes to discard
    ///
    /// After the `state` field of this object has been handled, `discard` bytes must be
    /// removed from the *front* of the `incoming_tls` buffer that was passed to
    /// the [`UnbufferedConnectionCommon::process_tls_records`] call that returned this object.
    ///
    /// This discard operation MUST happen *before*
    /// [`UnbufferedConnectionCommon::process_tls_records`] is called again.
    pub discard: usize,

    /// The current state of the handshake process
    ///
    /// This value MUST be handled prior to calling
    /// [`UnbufferedConnectionCommon::process_tls_records`] again. See the documentation on the
    /// variants of [`ConnectionState`] for more details.
    pub state: Result<ConnectionState<'c, 'i, Data>, Error>,
}

/// The state of the [`UnbufferedConnectionCommon`] object
#[non_exhaustive] // for forwards compatibility; to support caller-side certificate verification
pub enum ConnectionState<'c, 'i, Data> {
    /// One, or more, application data records are available
    ///
    /// See [`ReadTraffic`] for more details on how to use the enclosed object to access
    /// the received data.
    ReadTraffic(ReadTraffic<'c, 'i, Data>),

    /// Connection has been cleanly closed by the peer.
    ///
    /// This state is encountered at most once by each connection -- it is
    /// "edge" triggered, rather than "level" triggered.
    ///
    /// It delimits the data received from the peer, meaning you can be sure you
    /// have received all the data the peer sent.
    ///
    /// No further application data will be received from the peer, so no further
    /// `ReadTraffic` states will be produced.
    ///
    /// However, it is possible to _send_ further application data via `WriteTraffic`
    /// states, or close the connection cleanly by calling
    /// [`WriteTraffic::queue_close_notify()`].
    PeerClosed,

    /// Connection has been cleanly closed by both us and the peer.
    ///
    /// This is a terminal state.  No other states will be produced for this
    /// connection.
    Closed,

    /// One, or more, early (RTT-0) data records are available
    ReadEarlyData(ReadEarlyData<'c, 'i, Data>),

    /// A Handshake record is ready for encoding
    ///
    /// Call [`EncodeTlsData::encode`] on the enclosed object, providing an `outgoing_tls`
    /// buffer to store the encoding
    EncodeTlsData(EncodeTlsData<'c, Data>),

    /// Previously encoded handshake records need to be transmitted
    ///
    /// Transmit the contents of the `outgoing_tls` buffer that was passed to previous
    /// [`EncodeTlsData::encode`] calls to the peer.
    ///
    /// After transmitting the contents, call [`TransmitTlsData::done`] on the enclosed object.
    /// The transmitted contents MUST not be sent to the peer more than once so they SHOULD be
    /// discarded at this point.
    ///
    /// At some stages of the handshake process, it's possible to send application-data alongside
    /// handshake records. Call [`TransmitTlsData::may_encrypt_app_data`] on the enclosed
    /// object to probe if that's allowed.
    TransmitTlsData(TransmitTlsData<'c, Data>),

    /// More TLS data is needed to continue with the handshake
    ///
    /// Request more data from the peer and append the contents to the `incoming_tls` buffer that
    /// was passed to [`UnbufferedConnectionCommon::process_tls_records`].
    BlockedHandshake,

    /// The handshake process has been completed.
    ///
    /// [`WriteTraffic::encrypt`] can be called on the enclosed object to encrypt application
    /// data into an `outgoing_tls` buffer. Similarly, [`WriteTraffic::queue_close_notify`] can
    /// be used to encrypt a close_notify alert message into a buffer to signal the peer that the
    /// connection is being closed. Data written into `outgoing_buffer` by either method MAY be
    /// transmitted to the peer during this state.
    ///
    /// Once this state has been reached, data MAY be requested from the peer and appended to an
    /// `incoming_tls` buffer that will be passed to a future
    /// [`UnbufferedConnectionCommon::process_tls_records`] invocation. When enough data has been
    /// appended to `incoming_tls`, [`UnbufferedConnectionCommon::process_tls_records`] will yield
    /// the [`ConnectionState::ReadTraffic`] state.
    WriteTraffic(WriteTraffic<'c, Data>),
}

impl<'c, 'i, Data> From<ReadTraffic<'c, 'i, Data>> for ConnectionState<'c, 'i, Data> {
    fn from(v: ReadTraffic<'c, 'i, Data>) -> Self {
        Self::ReadTraffic(v)
    }
}

impl<'c, 'i, Data> From<ReadEarlyData<'c, 'i, Data>> for ConnectionState<'c, 'i, Data> {
    fn from(v: ReadEarlyData<'c, 'i, Data>) -> Self {
        Self::ReadEarlyData(v)
    }
}

impl<'c, Data> From<EncodeTlsData<'c, Data>> for ConnectionState<'c, '_, Data> {
    fn from(v: EncodeTlsData<'c, Data>) -> Self {
        Self::EncodeTlsData(v)
    }
}

impl<'c, Data> From<TransmitTlsData<'c, Data>> for ConnectionState<'c, '_, Data> {
    fn from(v: TransmitTlsData<'c, Data>) -> Self {
        Self::TransmitTlsData(v)
    }
}

impl<Data> fmt::Debug for ConnectionState<'_, '_, Data> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadTraffic(..) => f.debug_tuple("ReadTraffic").finish(),

            Self::PeerClosed => write!(f, "PeerClosed"),

            Self::Closed => write!(f, "Closed"),

            Self::ReadEarlyData(..) => f.debug_tuple("ReadEarlyData").finish(),

            Self::EncodeTlsData(..) => f.debug_tuple("EncodeTlsData").finish(),

            Self::TransmitTlsData(..) => f.debug_tuple("TransmitTlsData").finish(),

            Self::BlockedHandshake => f.debug_tuple("BlockedHandshake").finish(),

            Self::WriteTraffic(..) => f.debug_tuple("WriteTraffic").finish(),
        }
    }
}

/// Application data is available
pub struct ReadTraffic<'c, 'i, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,
    // for forwards compatibility; to support in-place decryption in the future
    _incoming_tls: &'i mut [u8],

    // owner of the latest chunk obtained in `next_record`, as borrowed by
    // `AppDataRecord`
    chunk: Option<Vec<u8>>,
}

impl<'c, 'i, Data> ReadTraffic<'c, 'i, Data> {
    fn new(conn: &'c mut UnbufferedConnectionCommon<Data>, _incoming_tls: &'i mut [u8]) -> Self {
        Self {
            conn,
            _incoming_tls,
            chunk: None,
        }
    }

    /// Decrypts and returns the next available app-data record
    // TODO deprecate in favor of `Iterator` implementation, which requires in-place decryption
    pub fn next_record(&mut self) -> Option<Result<AppDataRecord<'_>, Error>> {
        self.chunk = self.conn.core.common_state.received_plaintext.pop();
        self.chunk.as_ref().map(|chunk| {
            Ok(AppDataRecord {
                discard: 0,
                payload: chunk,
            })
        })
    }

    /// Returns the payload size of the next app-data record *without* decrypting it
    ///
    /// Returns `None` if there are no more app-data records
    pub fn peek_len(&self) -> Option<NonZeroUsize> {
        self.conn
            .core
            .common_state
            .received_plaintext
            .peek()
            .and_then(|ch| NonZeroUsize::new(ch.len()))
    }
}

/// Early application-data is available.
pub struct ReadEarlyData<'c, 'i, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,

    // for forwards compatibility; to support in-place decryption in the future
    _incoming_tls: &'i mut [u8],

    // owner of the latest chunk obtained in `next_record`, as borrowed by
    // `AppDataRecord`
    chunk: Option<Vec<u8>>,
}

impl<'c, 'i> ReadEarlyData<'c, 'i, ServerConnectionData> {
    fn new(
        conn: &'c mut UnbufferedConnectionCommon<ServerConnectionData>,
        _incoming_tls: &'i mut [u8],
    ) -> Self {
        Self {
            conn,
            _incoming_tls,
            chunk: None,
        }
    }

    /// decrypts and returns the next available app-data record
    // TODO deprecate in favor of `Iterator` implementation, which requires in-place decryption
    pub fn next_record(&mut self) -> Option<Result<AppDataRecord<'_>, Error>> {
        self.chunk = self.conn.pop_early_data();
        self.chunk.as_ref().map(|chunk| {
            Ok(AppDataRecord {
                discard: 0,
                payload: chunk,
            })
        })
    }

    /// returns the payload size of the next app-data record *without* decrypting it
    ///
    /// returns `None` if there are no more app-data records
    pub fn peek_len(&self) -> Option<NonZeroUsize> {
        self.conn
            .peek_early_data()
            .and_then(|ch| NonZeroUsize::new(ch.len()))
    }
}

/// A decrypted application-data record
pub struct AppDataRecord<'i> {
    /// Number of additional bytes to discard
    ///
    /// This number MUST be added to the value of [`UnbufferedStatus::discard`] *prior* to the
    /// discard operation. See [`UnbufferedStatus::discard`] for more details
    pub discard: usize,

    /// The payload of the app-data record
    pub payload: &'i [u8],
}

/// Allows encrypting app-data
pub struct WriteTraffic<'c, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,
}

impl<Data> WriteTraffic<'_, Data> {
    /// Encrypts `application_data` into the `outgoing_tls` buffer
    ///
    /// Returns the number of bytes that were written into `outgoing_tls`, or an error if
    /// the provided buffer is too small. In the error case, `outgoing_tls` is not modified
    pub fn encrypt(
        &mut self,
        application_data: &[u8],
        outgoing_tls: &mut [u8],
    ) -> Result<usize, EncryptError> {
        self.conn.core.maybe_refresh_traffic_keys();
        self.conn
            .core
            .common_state
            .write_plaintext(application_data.into(), outgoing_tls)
    }

    /// Encrypts a close_notify warning alert in `outgoing_tls`
    ///
    /// Returns the number of bytes that were written into `outgoing_tls`, or an error if
    /// the provided buffer is too small. In the error case, `outgoing_tls` is not modified
    pub fn queue_close_notify(&mut self, outgoing_tls: &mut [u8]) -> Result<usize, EncryptError> {
        self.conn
            .core
            .common_state
            .eager_send_close_notify(outgoing_tls)
    }

    /// Arranges for a TLS1.3 `key_update` to be sent.
    ///
    /// This consumes the `WriteTraffic` state:  to actually send the message,
    /// call [`UnbufferedConnectionCommon::process_tls_records`] again which will
    /// return a `ConnectionState::EncodeTlsData` that emits the `key_update`
    /// message.
    ///
    /// See [`ConnectionCommon::refresh_traffic_keys()`] for full documentation,
    /// including why you might call this and in what circumstances it will fail.
    ///
    /// [`ConnectionCommon::refresh_traffic_keys()`]: crate::ConnectionCommon::refresh_traffic_keys
    pub fn refresh_traffic_keys(self) -> Result<(), Error> {
        self.conn.core.refresh_traffic_keys()
    }
}

/// A handshake record must be encoded
pub struct EncodeTlsData<'c, Data> {
    conn: &'c mut UnbufferedConnectionCommon<Data>,
    chunk: Option<Vec<u8>>,
}

impl<'c, Data> EncodeTlsData<'c, Data> {
    fn new(conn: &'c mut UnbufferedConnectionCommon<Data>, chunk: Vec<u8>) -> Self {
        Self {
            conn,
            chunk: Some(chunk),
        }
    }

    /// Encodes a handshake record into the `outgoing_tls` buffer
    ///
    /// Returns the number of bytes that were written into `outgoing_tls`, or an error if
    /// the provided buffer is too small. In the error case, `outgoing_tls` is not modified
    pub fn encode(&mut self, outgoing_tls: &mut [u8]) -> Result<usize, EncodeError> {
        let Some(chunk) = self.chunk.take() else {
            return Err(EncodeError::AlreadyEncoded);
        };

        let required_size = chunk.len();

        match outgoing_tls.get_mut(..required_size) {
            None => {
                self.chunk = Some(chunk);
                Err(InsufficientSizeError { required_size }.into())
            }
            Some(dst) => {
                dst.copy_from_slice(&chunk);

                self.conn.wants_write = true;

                Ok(required_size)
            }
        }
    }
}

/// Previously encoded TLS data must be transmitted
pub struct TransmitTlsData<'c, Data> {
    pub(crate) conn: &'c mut UnbufferedConnectionCommon<Data>,
}

impl<Data> TransmitTlsData<'_, Data> {
    /// Signals that the previously encoded TLS data has been transmitted
    pub fn done(self) {
        self.conn.wants_write = false;
    }

    /// Returns an adapter that allows encrypting application data
    ///
    /// If allowed at this stage of the handshake process
    pub fn may_encrypt_app_data(&mut self) -> Option<WriteTraffic<'_, Data>> {
        if self.conn.core.common_state.may_send_application_data {
            Some(WriteTraffic { conn: self.conn })
        } else {
            None
        }
    }
}

/// Errors that may arise when encoding a handshake record
#[derive(Debug)]
pub enum EncodeError {
    /// Provided buffer was too small
    InsufficientSize(InsufficientSizeError),

    /// The handshake record has already been encoded; do not call `encode` again
    AlreadyEncoded,
}

impl From<InsufficientSizeError> for EncodeError {
    fn from(v: InsufficientSizeError) -> Self {
        Self::InsufficientSize(v)
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientSize(InsufficientSizeError { required_size }) => write!(
                f,
                "cannot encode due to insufficient size, {required_size} bytes are required"
            ),
            Self::AlreadyEncoded => "cannot encode, data has already been encoded".fmt(f),
        }
    }
}

impl StdError for EncodeError {}

/// Errors that may arise when encrypting application data
#[derive(Debug)]
pub enum EncryptError {
    /// Provided buffer was too small
    InsufficientSize(InsufficientSizeError),

    /// Encrypter has been exhausted
    EncryptExhausted,
}

impl From<InsufficientSizeError> for EncryptError {
    fn from(v: InsufficientSizeError) -> Self {
        Self::InsufficientSize(v)
    }
}

impl fmt::Display for EncryptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientSize(InsufficientSizeError { required_size }) => write!(
                f,
                "cannot encrypt due to insufficient size, {required_size} bytes are required"
            ),
            Self::EncryptExhausted => f.write_str("encrypter has been exhausted"),
        }
    }
}

impl StdError for EncryptError {}

/// Provided buffer was too small
#[derive(Clone, Copy, Debug)]
pub struct InsufficientSizeError {
    /// buffer must be at least this size
    pub required_size: usize,
}
