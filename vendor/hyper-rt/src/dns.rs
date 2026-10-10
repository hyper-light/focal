//! Name resolution (docs/runtime.md §5.4) through the platform resolver — `getaddrinfo` on Unix,
//! `GetAddrInfoW` on Windows, by way of std's `ToSocketAddrs` — so `/etc/hosts`, `nsswitch`, mDNS and
//! enterprise resolvers behave as every other program on the host sees them. The call blocks, so it runs
//! on the bounded blocking pool ([`crate::blocking`]) under the share named [`SHARE`], which the consumer
//! declares with the limit it wants for resolution (rare: a context's endpoint, a join).

use std::net::{SocketAddr, ToSocketAddrs};

use crate::blocking;
use crate::error::RtError;

/// Format: the blocking pool share resolution runs under.
pub const SHARE: &str = "dns";

/// Resolves `host` and `port` to at most `max` addresses, in the resolver's order. A failed lookup is
/// `DriverRefused` with the OS error, where the resolver gives one; a pool without the [`SHARE`] share, or at
/// its limit, refuses as [`blocking::run`] does.
pub async fn resolve(host: &str, port: u16, max: usize) -> Result<Vec<SocketAddr>, RtError> {
    let host = host.to_owned();
    let lookup = blocking::run(SHARE, move || {
        (host.as_str(), port)
            .to_socket_addrs()
            .map(|addrs| addrs.take(max).collect::<Vec<_>>())
            .map_err(|error| error.raw_os_error())
    })?;
    match lookup.await {
        Ok(Ok(addrs)) => Ok(addrs),
        Ok(Err(code)) => Err(RtError::DriverRefused {
            call: "getaddrinfo",
            code,
        }),
        Err(_) => Err(RtError::DriverRefused {
            call: "getaddrinfo (the pool dropped the lookup)",
            code: None,
        }),
    }
}
