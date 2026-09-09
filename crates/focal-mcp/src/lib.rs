#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Bounded owned MCP protocol state; transport and business execution are supplied by the host.
mod admin;
mod backend;
mod catalog_admin;
mod catalog_native;
mod catalog_transfer;
mod catalog_watch;
pub use admin::{AdminAction, AdminBackend, AdminChange, AdminError};
mod catalog;
mod codec;
mod json;
mod protocol;
#[cfg(test)]
mod skill_contract_tests;
mod stdio;
#[cfg(all(test, unix))]
mod stdio_tests;
#[cfg(test)]
mod tests;

pub use backend::{Backend, JournalError, NativeJournal};
pub use codec::{EncodedFrame, FrameDecoder, InputFrame};
pub use protocol::{Action, CallToken, Protocol, ServerInfo, Tool, ToolCall};
pub use stdio::{ServeError, serve};

pub const MODERN_VERSION: &str = "2026-07-28";
pub const LEGACY_VERSION: &str = "2025-11-25";

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_response_bytes: usize,
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_id_bytes: usize,
    pub max_active_calls: usize,
    pub max_tools: usize,
    pub tools_per_page: usize,
    pub cursor_ttl_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 262_144,
            max_response_bytes: 1_048_576,
            max_depth: 24,
            max_nodes: 16_384,
            max_id_bytes: 128,
            max_active_calls: 8,
            max_tools: 128,
            tools_per_page: 16,
            cursor_ttl_ms: 30_000,
        }
    }
}
impl Limits {
    pub(crate) fn validate(self) -> Result<Self, ProtocolError> {
        if self.max_frame_bytes == 0
            || self.max_response_bytes < 512
            || self.max_depth == 0
            || self.max_depth > 64
            || self.max_nodes == 0
            || self.max_id_bytes == 0
            || self.max_active_calls == 0
            || self.max_tools == 0
            || self.tools_per_page == 0
            || self.cursor_ttl_ms == 0
        {
            return Err(ProtocolError::Limits);
        }
        Ok(self)
    }
    pub(crate) fn workspace(self) -> Result<usize, ProtocolError> {
        self.max_frame_bytes
            .checked_mul(8)
            .and_then(|n| {
                self.max_nodes
                    .checked_mul(128)
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or(ProtocolError::Capacity)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("invalid MCP limits or catalog")]
    Limits,
    #[error("MCP capacity exhausted")]
    Capacity,
    #[error("invalid or oversized MCP frame")]
    Frame,
    #[error("MCP stream is closed")]
    Closed,
    #[error("duplicate active JSON-RPC ID")]
    DuplicateId,
    #[error("MCP response encoding failed")]
    Encode,
    #[error("MCP dependency failed")]
    Dependency,
    #[error("MCP allocation failed: {0}")]
    Memory(#[from] focal_memory::MemoryError),
}
