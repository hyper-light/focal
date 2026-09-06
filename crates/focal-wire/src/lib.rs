#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Bounded authenticated protocol shared by embedded and QUIC adapters.
//! Async transport entry points require a Tokio runtime with IO and time drivers.
//! Missing runtime context returns a typed transport error before dispatch.
mod auth;
mod frame;
mod handler;
mod message;
mod peers;
mod transport;
#[cfg(unix)]
mod unix;
pub use auth::*;
pub use frame::*;
pub use handler::*;
pub use message::*;
pub use peers::*;
pub use transport::*;
#[cfg(unix)]
pub use unix::*;
#[cfg(test)]
mod tests;

pub(crate) fn require_runtime() -> Result<(), WireError> {
    tokio::runtime::Handle::try_current()
        .map(|_| ())
        .map_err(|_| WireError::Connection)
}

/// Check the time driver once before setup can spawn endpoint tasks. IO driver
/// registration happens inside the same containment boundary. This is used by
/// bind constructors, never on the per-message path.
pub(crate) fn transport_setup<T>(
    build: impl FnOnce() -> Result<T, WireError>,
) -> Result<T, WireError> {
    require_runtime()?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(tokio::time::sleep(std::time::Duration::ZERO));
        build()
    }))
    .map_err(|_| WireError::Connection)?
}

/// A handle may be used from a different runtime than the one that bound it.
/// Contain the exchange itself, including timer construction, without adding a
/// timer probe per request. An unwound exchange is dropped and never repolled.
pub(crate) async fn transport_exchange<T>(
    exchange: impl std::future::Future<Output = Result<T, WireError>>,
) -> Result<T, WireError> {
    require_runtime()?;
    let mut exchange = std::pin::pin!(exchange);
    std::future::poll_fn(|context| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            exchange.as_mut().poll(context)
        }))
        .unwrap_or_else(|_| std::task::Poll::Ready(Err(WireError::Connection)))
    })
    .await
}
