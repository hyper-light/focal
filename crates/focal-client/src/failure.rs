//! Adapter-neutral failure categories. These describe a response, never proof
//! that an earlier mutation did not commit or permission to allocate a new ID.
use crate::{
    AccessError, ClientError, NativeErrorCode, NativeMutationReply, NativeRefusal,
    NativeRefusalKind, artifact_transfer::TransferError, input::InputError,
    managed_requests::ManagedRequestsError, managed_store::ManagedStoreError,
    native_store::NativeStoreError, operation_store::StoreError, pending::PendingError,
    watch::WatchError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    pub condition: &'static str,
    pub code: &'static str,
    pub exit_code: i32,
}
impl Failure {
    pub const fn error(code: &'static str, exit_code: i32) -> Self {
        Self {
            condition: "Error",
            code,
            exit_code,
        }
    }
    pub const fn outcome_unknown() -> Self {
        Self {
            condition: "OutcomeUnknown",
            code: "outcome_unknown",
            exit_code: 7,
        }
    }
    pub const fn cancelled() -> Self {
        Self {
            condition: "Cancelled",
            code: "cancelled",
            exit_code: 130,
        }
    }
}
pub fn access(error: &AccessError) -> Failure {
    match error {
        AccessError::Unauthorized => Failure::error("unauthorized", 3),
        AccessError::UnsupportedProtocol => Failure::error("unsupported_protocol", 2),
        AccessError::InvalidRequest => Failure::error("invalid_input", 2),
        AccessError::Capacity => Failure::error("capacity", 6),
        AccessError::Unavailable => Failure::error("unavailable", 6),
        AccessError::OutcomeUnknown => Failure::outcome_unknown(),
        AccessError::RouteChanged(_) => Failure::error("route_changed", 6),
        AccessError::Behind { .. } => Failure::error("behind", 6),
        AccessError::SnapshotExpired => Failure::error("snapshot_expired", 8),
        AccessError::ResyncRequired { .. } => Failure::error("resync_required", 8),
        AccessError::UnsupportedOperation => Failure::error("unsupported_operation", 2),
        AccessError::ManagedRetired { .. } => Failure {
            condition: "Retired",
            code: "managed_retired",
            exit_code: 5,
        },
        AccessError::ManagedClosed { .. } => Failure::error("managed_closed", 5),
        AccessError::ManagedConflict => Failure::error("managed_conflict", 5),
        AccessError::ManagedNotRegistered => Failure::error("managed_not_registered", 5),
    }
}
/// Closed native refusal categories map to the same exit classes as V1:
/// invalid input 2, unauthorized 3, not found 4, conflict/stale 5, capacity 6.
pub fn native(refusal: &NativeRefusal) -> Failure {
    match &refusal.kind {
        NativeRefusalKind::InvalidInput => Failure::error("invalid_input", 2),
        NativeRefusalKind::Unauthorized => Failure::error("unauthorized", 3),
        NativeRefusalKind::NotFound => Failure::error("not_found", 4),
        NativeRefusalKind::Stale { .. } => Failure {
            condition: "Stale",
            code: "stale_binding",
            exit_code: 5,
        },
        NativeRefusalKind::Conflict => Failure::error("operation_conflict", 5),
        NativeRefusalKind::Capacity => Failure::error("capacity", 6),
        NativeRefusalKind::Refused(code) => match code {
            NativeErrorCode::WrongActor => Failure::error("unauthorized", 3),
            NativeErrorCode::WrongLedger => Failure::error("wrong_ledger", 2),
            NativeErrorCode::WrongObject => Failure::error("wrong_object", 2),
            NativeErrorCode::ContentConflict => Failure::error("content_conflict", 5),
            NativeErrorCode::StaleRevision => Failure::error("stale_revision", 5),
            NativeErrorCode::StaleReceipt => Failure::error("stale_receipt", 5),
            NativeErrorCode::StaleEvaluation => Failure::error("stale_evaluation", 5),
            NativeErrorCode::InvalidTransition => Failure::error("invalid_transition", 5),
            NativeErrorCode::InvalidTarget => Failure::error("invalid_target", 2),
            NativeErrorCode::InvalidManifest => Failure::error("invalid_manifest", 2),
            NativeErrorCode::MissingEvidence => Failure::error("missing_evidence", 2),
            NativeErrorCode::InvalidPolicy => Failure::error("invalid_policy", 2),
            NativeErrorCode::Capacity => Failure::error("capacity", 6),
            NativeErrorCode::ConflictingCause => Failure::error("conflicting_cause", 5),
            NativeErrorCode::InvalidCut => Failure::error("invalid_cut", 5),
            NativeErrorCode::Legacy => Failure::error("legacy_engine", 2),
            NativeErrorCode::Unsupported => Failure::error("unsupported_operation", 2),
        },
    }
}
/// A native reply that is not a committed receipt: pending outcomes stay
/// unknown; refusals classify by category.
pub fn native_reply(reply: &NativeMutationReply) -> Option<Failure> {
    match reply {
        NativeMutationReply::Committed(_) => None,
        NativeMutationReply::Pending(_) => Some(Failure::outcome_unknown()),
        NativeMutationReply::Refused(refusal) => Some(native(refusal)),
    }
}
pub fn native_store(error: &NativeStoreError) -> Failure {
    match error {
        NativeStoreError::Store(error) => store(error),
        NativeStoreError::InvalidId
        | NativeStoreError::InvalidRequest
        | NativeStoreError::Expansion(_) => Failure::error("invalid_input", 2),
        NativeStoreError::Corrupt => Failure::error("native_store", 1),
        NativeStoreError::Capacity => Failure::error("capacity", 6),
        NativeStoreError::ContextMismatch
        | NativeStoreError::IntentConflict
        | NativeStoreError::ReceiptMismatch => Failure::error("operation_conflict", 5),
        NativeStoreError::MissingOperation => Failure::error("not_found", 4),
        NativeStoreError::Incomplete => Failure::error("operation_conflict", 5),
        NativeStoreError::NotCommitted => Failure::outcome_unknown(),
    }
}
pub fn client(error: &ClientError) -> Failure {
    match error {
        ClientError::Access(error) => access(error),
        ClientError::Unauthenticated => Failure::error("unauthenticated", 3),
        ClientError::OutcomeUnknown { .. } => Failure::outcome_unknown(),
        ClientError::Configuration => Failure::error("configuration", 2),
        ClientError::InvalidResponse => Failure::error("invalid_response", 1),
        ClientError::Transport => Failure::error("transport", 1),
    }
}
pub fn input(error: &InputError) -> Failure {
    match error {
        InputError::Capacity => Failure::error("capacity", 2),
        _ => Failure::error("invalid_input", 2),
    }
}
pub fn pending(error: &PendingError) -> Failure {
    match error {
        PendingError::Locked => Failure::error("busy", 6),
        PendingError::Capacity => Failure::error("capacity", 6),
        PendingError::ContextMismatch | PendingError::Exists => {
            Failure::error("operation_conflict", 5)
        }
        PendingError::EpochPolicy => Failure::error("invalid_input", 2),
        // An ambiguous local write is a journal recovery problem. It does not
        // assert whether a business request was transmitted or committed.
        _ => Failure::error("operation_journal", 1),
    }
}
pub fn store(error: &StoreError) -> Failure {
    match error {
        StoreError::IntentConflict | StoreError::ContextMismatch => {
            Failure::error("operation_conflict", 5)
        }
        StoreError::Locked => Failure::error("busy", 6),
        StoreError::Capacity => Failure::error("capacity", 6),
        StoreError::MissingOperation => Failure::error("not_found", 4),
        StoreError::InvalidId | StoreError::InvalidIntent => Failure::error("invalid_input", 2),
        StoreError::Expansion(error) => input(error),
        StoreError::Pending(error) => pending(error),
        _ => Failure::error("operation_store", 1),
    }
}
pub fn managed_store(error: &ManagedStoreError) -> Failure {
    match error {
        ManagedStoreError::Store(error) => store(error),
        ManagedStoreError::Retired => Failure {
            condition: "Retired",
            code: "managed_retired",
            exit_code: 5,
        },
        ManagedStoreError::Missing => Failure::error("not_found", 4),
        ManagedStoreError::Conflict | ManagedStoreError::Context => {
            Failure::error("operation_conflict", 5)
        }
        ManagedStoreError::Capacity => Failure::error("capacity", 6),
        ManagedStoreError::InvalidId => Failure::error("invalid_input", 2),
        _ => Failure::error("managed_store", 1),
    }
}
pub fn managed(error: &ManagedRequestsError) -> Failure {
    match error {
        ManagedRequestsError::Managed(error) => managed_store(error),
        ManagedRequestsError::Store(error) => store(error),
        ManagedRequestsError::Identity(error) => input(error),
        ManagedRequestsError::Exhausted => Failure::error("capacity", 6),
        ManagedRequestsError::Missing => Failure::error("not_found", 4),
        ManagedRequestsError::Context => Failure::error("operation_conflict", 5),
        ManagedRequestsError::Remote(error) => access(error),
        _ => Failure::error("managed_requests", 1),
    }
}
pub fn transfer(error: &TransferError) -> Failure {
    match error {
        TransferError::Client(error) => client(error),
        TransferError::Locked => Failure::error("busy", 6),
        TransferError::Conflict | TransferError::Exists => Failure::error("operation_conflict", 5),
        TransferError::Capacity => Failure::error("capacity", 6),
        TransferError::Invalid => Failure::error("invalid_input", 2),
        TransferError::Incomplete => Failure {
            condition: "Pending",
            code: "upload_incomplete",
            exit_code: 6,
        },
        TransferError::Cancelled => Failure::cancelled(),
        _ => Failure::error("upload_journal", 1),
    }
}
pub fn watch(error: &WatchError) -> Failure {
    match error {
        WatchError::Remote(error) => access(error),
        WatchError::Missing => Failure::error("not_found", 4),
        WatchError::Capacity => Failure::error("capacity", 6),
        WatchError::Conflict => Failure::error("operation_conflict", 5),
        WatchError::Invalid | WatchError::DeliveryMismatch => Failure::error("invalid_input", 2),
        WatchError::Store(error) => store(error),
        WatchError::Managed(error) => managed_store(error),
        WatchError::Maintenance(error) => managed(error),
        WatchError::Input(error) => input(error),
        _ => Failure::error("watch_journal", 1),
    }
}

/// Recognize concrete SDK errors even when a caller preserved them in a boxed
/// error boundary. No string parsing, allocation, retry or journal mutation.
pub fn classify(error: &(dyn std::error::Error + 'static)) -> Option<Failure> {
    macro_rules! known {
        ($($ty:ty => $classify:ident),+ $(,)?) => { $(
            if let Some(value) = error.downcast_ref::<$ty>() { return Some($classify(value)); }
        )+ };
    }
    known! {
        ClientError => client, AccessError => access, InputError => input,
        PendingError => pending, StoreError => store, ManagedStoreError => managed_store,
        ManagedRequestsError => managed, TransferError => transfer, WatchError => watch,
        NativeStoreError => native_store
    }
    None
}
