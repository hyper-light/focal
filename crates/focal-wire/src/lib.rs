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
//! Async transport entry points run on a hyper-rt shard (27 §15); off one,
//! they return a typed transport error before dispatch.
mod admission;
mod auth;
mod crypto;
pub use crypto::crypto_provider;
mod frame;
mod handler;
mod list;
mod local;
mod managed;
mod message;
mod native;
mod peer_mutations;
mod peers;
mod reconcile;
mod round;
mod transport;
mod traversal;
mod trust;
mod validators;
pub use admission::*;
pub use auth::*;
pub use frame::*;
pub use handler::*;
pub use list::*;
pub use local::{LocalGrant, LocalRemote, LocalServer};
pub use managed::*;
pub use message::*;
pub use native::*;
pub use peer_mutations::*;
pub use peers::*;
pub use reconcile::*;
pub use round::*;
// Historic names kept so the ~25 call sites do not churn across the port.
pub use local::{LocalRemote as UnixRemote, LocalServer as UnixServer};
pub use transport::*;
pub use traversal::*;
pub use trust::*;
pub use validators::*;
#[cfg(test)]
#[path = "net_tests.rs"]
mod net_tests;

/// Whether this thread is a hyper-rt shard's: what every async entry point
/// needs, its timers and sockets the shard's.
pub(crate) fn require_runtime() -> Result<(), WireError> {
    hyper_rt::futures::shard_id()
        .map(|_| ())
        .ok_or(WireError::Connection)
}

/// An exchange contained by an unwind boundary: a dependency that unwinds
/// ends the exchange, typed, and it is dropped and never polled again
/// (CLAUDE.md §1: the boundary is the last fence, never the fix).
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

mod monitor;
pub mod net;
mod summary;
pub use monitor::{MAX_MONITOR_ROOTS, MonitorPage};
pub use summary::LedgerSummary;

mod selection;
pub use selection::*;
