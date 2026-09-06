//! Caller-owned durable watches. A network reply is retained before delivery;
//! only explicit sink acknowledgment advances the saved source cursor.
mod journal;
mod store;
use crate::{
    managed_requests::ManagedRequestsError, managed_store::ManagedStoreError,
    operation_store::StoreError, *,
};
use focal_model::*;
pub use journal::{WatchAction, WatchJournal, WatchRequest};
use serde::{Deserialize, Serialize};
pub use store::WatchStore;

pub const MAX_WATCHES: usize = 16;
pub const MAX_WATCH_PAGE_BYTES: usize = 64 * 1024;
pub(crate) const RECORD_BYTES: usize = 256 * 1024;
pub(crate) const MAGIC: &[u8; 8] = b"FCLWAT01";
#[derive(Debug, thiserror::Error)]
pub enum WatchError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Managed(#[from] ManagedStoreError),
    #[error(transparent)]
    Maintenance(#[from] ManagedRequestsError),
    #[error(transparent)]
    Input(#[from] input::InputError),
    #[error("watch options or authenticated context are invalid")]
    Invalid,
    #[error("watch name is already bound to different options")]
    Conflict,
    #[error("watch state is missing or corrupt")]
    Corrupt,
    #[error("watch does not exist")]
    Missing,
    #[error("watch exceeds its bounded capacity")]
    Capacity,
    #[error("watch acknowledgment does not name the retained delivery")]
    DeliveryMismatch,
    #[error("watch reply does not match the exact pending request")]
    InvalidResponse,
    #[error("watch remote request failed: {0:?}")]
    Remote(AccessError),
    #[error("watch had an ambiguous write; reopen before continuing")]
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchOptions {
    #[serde(default)]
    pub claims: Vec<ClaimId>,
    #[serde(default)]
    pub family: Option<ObjectKind>,
    #[serde(default = "yes")]
    pub seed: bool,
    #[serde(default = "items")]
    pub max_items: u32,
    #[serde(default = "bytes")]
    pub max_bytes: u32,
}
fn yes() -> bool {
    true
}
fn items() -> u32 {
    64
}
fn bytes() -> u32 {
    MAX_WATCH_PAGE_BYTES as u32
}
impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            claims: vec![],
            family: None,
            seed: true,
            max_items: items(),
            max_bytes: bytes(),
        }
    }
}
impl WatchOptions {
    fn validate(&self) -> Result<(), WatchError> {
        if self.claims.len() > 256
            || self.claims.iter().any(|id| id.is_zero())
            || self.claims.windows(2).any(|p| matches!(p,[a,b] if a>=b))
            || self.max_items == 0
            || self.max_items > 256
            || self.max_bytes < 4096
            || self.max_bytes as usize > MAX_WATCH_PAGE_BYTES
        {
            return Err(WatchError::Invalid);
        }
        Ok(())
    }
    fn filter(&self) -> DeltaFilter {
        if self.claims.is_empty() {
            DeltaFilter::All
        } else {
            DeltaFilter::Claims(self.claims.iter().copied().collect())
        }
    }
    fn credits(&self) -> Credits {
        Credits {
            items: self.max_items,
            // The source counts data bytes separately from its cursor/control
            // reply wrapper. Reserve that wrapper inside the saved page bound.
            bytes: self.max_bytes.saturating_sub(1024),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchPage {
    Seed { page: ReadPage },
    Events { page: Box<StreamReply> },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchDelivery {
    pub id: ContentHash,
    pub number: u64,
    pub page: WatchPage,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchStatus {
    pub name: String,
    pub options: WatchOptions,
    pub delivered: u64,
    pub acknowledged: u64,
    pub pending: bool,
    pub delivery: Option<ContentHash>,
    pub seeding: bool,
}
pub(crate) fn record_limit(name: &str) -> Option<usize> {
    name.strip_suffix(".watch-owner")
        .filter(|name| valid_name(name))
        .map(|_| RECORD_BYTES)
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 96
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        && name != "."
        && name != ".."
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, WatchError> {
    let size = postcard::experimental::serialized_size(value).map_err(|_| WatchError::Corrupt)?;
    if size > RECORD_BYTES {
        return Err(WatchError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| WatchError::Capacity)?;
    bytes.resize(size, 0);
    postcard::to_slice(value, &mut bytes).map_err(|_| WatchError::Corrupt)?;
    Ok(bytes)
}
fn fresh(ids: &mut impl input::IdGenerator) -> Result<RequestId, WatchError> {
    let id = RequestId(ids.next_id()?);
    if id.is_zero() {
        Err(WatchError::Invalid)
    } else {
        Ok(id)
    }
}
