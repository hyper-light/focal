use core::borrow::BorrowMut;
use core::ops::{Deref, DerefMut};
use std::io::{BufRead, IoSlice, Read, Result, Write};

use crate::conn::{ConnectionCommon, SideData};

/// This type implements `io::Read` and `io::Write`, encapsulating
/// a Connection `C`, the configuration `F` it was made with, and an underlying transport `T`,
/// such as a socket.
///
/// Relies on [`ConnectionCommon::complete_io()`] to perform the necessary I/O. A connection holds
/// no configuration, so the stream borrows it for the calls that may advance the handshake.
///
/// This allows you to use a rustls Connection like a normal stream.
#[derive(Debug)]
pub struct Stream<'a, C: 'a + ?Sized, T: 'a + Read + Write + ?Sized, F: 'a + ?Sized> {
    /// Our TLS connection
    pub conn: &'a mut C,

    /// The configuration our TLS connection was made with
    pub config: &'a mut F,

    /// The underlying transport, like a socket
    pub sock: &'a mut T,
}

impl<'a, C, T, S> Stream<'a, C, T, S::Config>
where
    C: 'a + DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: 'a + Read + Write,
    S: SideData,
{
    /// Make a new Stream using the Connection `conn`, the configuration `config` it was made
    /// with, and socket-like object `sock`.  This does not fail and does no IO.
    pub fn new(conn: &'a mut C, config: &'a mut S::Config, sock: &'a mut T) -> Self {
        Self { conn, config, sock }
    }

    /// If we're handshaking, complete all the IO for that.
    /// If we have data to write, write it all.
    fn complete_prior_io(&mut self) -> Result<()> {
        if self.conn.is_handshaking() {
            self.conn.complete_io(self.sock, self.config)?;
        }

        if self.conn.wants_write() {
            self.conn.complete_io(self.sock, self.config)?;
        }

        Ok(())
    }

    fn prepare_read(&mut self) -> Result<()> {
        self.complete_prior_io()?;

        // We call complete_io() in a loop since a single call may read only
        // a partial packet from the underlying transport. A full packet is
        // needed to get more plaintext, which we must do if EOF has not been
        // hit.
        while self.conn.wants_read() {
            if self.conn.complete_io(self.sock, self.config)?.0 == 0 {
                break;
            }
        }

        Ok(())
    }

    // Implements `BufRead::fill_buf` but with more flexible lifetimes, so StreamOwned can reuse it
    fn fill_buf(mut self) -> Result<&'a [u8]>
    where
        S: 'a,
    {
        self.prepare_read()?;
        self.conn.reader().into_first_chunk()
    }
}

impl<'a, C, T, S> Read for Stream<'a, C, T, S::Config>
where
    C: 'a + DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: 'a + Read + Write,
    S: SideData,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.prepare_read()?;
        self.conn.reader().read(buf)
    }
}

impl<'a, C, T, S> BufRead for Stream<'a, C, T, S::Config>
where
    C: 'a + DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: 'a + Read + Write,
    S: 'a + SideData,
{
    fn fill_buf(&mut self) -> Result<&[u8]> {
        // reborrow to get an owned `Stream`
        Stream {
            conn: self.conn,
            config: self.config,
            sock: self.sock,
        }
        .fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.conn.reader().consume(amt)
    }
}

impl<'a, C, T, S> Write for Stream<'a, C, T, S::Config>
where
    C: 'a + DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: 'a + Read + Write,
    S: SideData,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.complete_prior_io()?;

        let len = self.conn.writer().write(buf)?;

        // Try to write the underlying transport here, but don't let
        // any errors mask the fact we've consumed `len` bytes.
        // Callers will learn of permanent errors on the next call.
        let _ = self.conn.complete_io(self.sock, self.config);

        Ok(len)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.complete_prior_io()?;

        let len = self.conn.writer().write_vectored(bufs)?;

        // Try to write the underlying transport here, but don't let
        // any errors mask the fact we've consumed `len` bytes.
        // Callers will learn of permanent errors on the next call.
        let _ = self.conn.complete_io(self.sock, self.config);

        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        self.complete_prior_io()?;

        self.conn.writer().flush()?;
        if self.conn.wants_write() {
            self.conn.complete_io(self.sock, self.config)?;
        }
        Ok(())
    }
}

/// This type implements `io::Read` and `io::Write`, encapsulating
/// and owning a Connection `C`, the configuration `F` it is driven with, and an underlying
/// transport `T`, such as a socket.
///
/// `F` is the configuration itself, or a mutable borrow of one that other connections share.
///
/// Relies on [`ConnectionCommon::complete_io()`] to perform the necessary I/O.
///
/// This allows you to use a rustls Connection like a normal stream.
#[derive(Debug)]
pub struct StreamOwned<C: Sized, T: Read + Write + Sized, F: Sized> {
    /// Our connection
    pub conn: C,

    /// The configuration our connection was made with, or a borrow of it
    pub config: F,

    /// The underlying transport, like a socket
    pub sock: T,
}

impl<C, T, F, S> StreamOwned<C, T, F>
where
    C: DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: Read + Write,
    F: BorrowMut<S::Config>,
    S: SideData,
{
    /// Make a new StreamOwned taking the Connection `conn`, its configuration `config` and
    /// socket-like object `sock`.  This does not fail and does no IO.
    ///
    /// This is the same as `Stream::new` except `conn`, `config` and `sock` are
    /// moved into the StreamOwned.
    pub fn new(conn: C, config: F, sock: T) -> Self {
        Self { conn, config, sock }
    }

    /// Get a reference to the underlying socket
    pub fn get_ref(&self) -> &T {
        &self.sock
    }

    /// Get a mutable reference to the underlying socket
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.sock
    }

    /// Extract the `conn`, `config` and `sock` parts from the `StreamOwned`
    pub fn into_parts(self) -> (C, F, T) {
        (self.conn, self.config, self.sock)
    }
}

impl<'a, C, T, F, S> StreamOwned<C, T, F>
where
    C: DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: Read + Write,
    F: BorrowMut<S::Config>,
    S: SideData,
{
    fn as_stream(&'a mut self) -> Stream<'a, C, T, S::Config> {
        Stream {
            conn: &mut self.conn,
            config: self.config.borrow_mut(),
            sock: &mut self.sock,
        }
    }
}

impl<C, T, F, S> Read for StreamOwned<C, T, F>
where
    C: DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: Read + Write,
    F: BorrowMut<S::Config>,
    S: SideData,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.as_stream().read(buf)
    }
}

impl<C, T, F, S> BufRead for StreamOwned<C, T, F>
where
    C: DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: Read + Write,
    F: BorrowMut<S::Config>,
    S: 'static + SideData,
{
    fn fill_buf(&mut self) -> Result<&[u8]> {
        self.as_stream().fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.as_stream().consume(amt)
    }
}

impl<C, T, F, S> Write for StreamOwned<C, T, F>
where
    C: DerefMut + Deref<Target = ConnectionCommon<S>>,
    T: Read + Write,
    F: BorrowMut<S::Config>,
    S: SideData,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.as_stream().write(buf)
    }

    fn flush(&mut self) -> Result<()> {
        self.as_stream().flush()
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;

    use super::{Stream, StreamOwned};
    use crate::client::{ClientConfig, ClientConnection};
    use crate::server::{ServerConfig, ServerConnection};

    #[test]
    fn stream_can_be_created_for_connection_and_tcpstream() {
        type _Test<'a> = Stream<'a, ClientConnection, TcpStream, ClientConfig>;
    }

    #[test]
    fn streamowned_can_be_created_for_client_and_tcpstream() {
        type _Test = StreamOwned<ClientConnection, TcpStream, ClientConfig>;
    }

    #[test]
    fn streamowned_can_be_created_for_server_and_tcpstream() {
        type _Test = StreamOwned<ServerConnection, TcpStream, ServerConfig>;
    }
}
