//! Unique response ownership plus original claimant-receipt and WholeWork-entry
//! publication positions. Copies retain history; current status cannot replace it.
use super::prepare::ALLOCATION;
use super::{ContractError, MemoryError, NativeError, Response};
use focal_model::lifecycle::{
    aggregation::PublicationPosition,
    evidence::{ResponseState, ResponseTransition},
};

/// Retained owner facts, read at the same prefix as the response. Participants
/// cannot assign publication positions through authored testimony.
#[derive(Debug)]
pub struct NativeResponseRecord {
    response: Response,
    received: Option<PublicationPosition>,
    entered: Option<PublicationPosition>,
}
impl NativeResponseRecord {
    pub fn response(&self) -> &Response {
        &self.response
    }
    pub fn received(&self) -> Option<PublicationPosition> {
        self.received
    }
    pub fn entered(&self) -> Option<PublicationPosition> {
        self.entered
    }

    fn check(&self) -> Result<(), ContractError> {
        if self.received.is_some_and(|at| at.sequence.0 == 0)
            || self.entered.is_some_and(|at| at.sequence.0 == 0)
        {
            return Err(ContractError::InvalidCut);
        }
        match (self.response.state(), self.received, self.entered) {
            (ResponseState::Generated | ResponseState::Posted, None, None)
            | (ResponseState::Received, Some(_), None) => Ok(()),
            (
                ResponseState::Validating
                | ResponseState::Validated
                | ResponseState::ValidationIncomplete
                | ResponseState::ValidationFailed
                | ResponseState::ValidationErrored,
                Some(received),
                Some(entered),
            ) if after(entered, received) => Ok(()),
            _ => Err(ContractError::InvalidCut),
        }
    }
}
fn after(a: PublicationPosition, b: PublicationPosition) -> bool {
    (a.sequence, a.ordinal) > (b.sequence, b.ordinal)
}

#[derive(Debug)]
pub(super) struct OwnedResponse(Vec<NativeResponseRecord>);
const RESPONSE_CONTAINER: usize = size_of::<NativeResponseRecord>() + ALLOCATION;

fn add(a: usize, b: usize) -> Result<usize, MemoryError> {
    a.checked_add(b).ok_or(MemoryError::AllocationFailed)
}
impl OwnedResponse {
    pub(super) const fn container_charge() -> usize {
        RESPONSE_CONTAINER
    }

    pub(super) fn new(response: Response) -> Result<Self, NativeError> {
        if response.state() != ResponseState::Generated {
            return Err(ContractError::InvalidTransition.into());
        }
        Ok(Self::wrap(NativeResponseRecord {
            response,
            received: None,
            entered: None,
        })?)
    }
    fn wrap(record: NativeResponseRecord) -> Result<Self, MemoryError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if rows.capacity() != 1 {
            return Err(MemoryError::AllocationFailed);
        }
        rows.push(record);
        Ok(Self(rows))
    }
    pub(super) fn record(&self) -> Option<&NativeResponseRecord> {
        match self.0.as_slice() {
            [record] => Some(record),
            _ => None,
        }
    }
    pub(super) fn get(&self) -> Option<&Response> {
        self.record().map(|row| &row.response)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        let response = self.get().ok_or(MemoryError::MissingKey)?;
        add(Self::container_charge(), response_heap(response)?)
    }

    /// The caller precharges the complete old row before this fallible copy.
    /// Model transition capabilities enforce the actual source and authority;
    /// the sole owner supplies the new event's committed-prefix coordinates.
    pub(super) fn transition(
        &self,
        transition: ResponseTransition,
        position: PublicationPosition,
    ) -> Result<Self, NativeError> {
        let source = self.record().ok_or(MemoryError::MissingKey)?;
        source.check()?;
        if position.sequence.0 == 0
            || source.received.is_some_and(|at| !after(position, at))
            || source.entered.is_some_and(|at| !after(position, at))
        {
            return Err(ContractError::InvalidCut.into());
        }
        let mut response = source
            .response
            .try_copy(source.response.retained_bytes()?)?;
        response.apply(transition)?;
        let received = if source.response.state() == ResponseState::Posted
            && response.state() == ResponseState::Received
        {
            Some(position)
        } else {
            source.received
        };
        let entered = if source.response.state() == ResponseState::Received
            && response.state() == ResponseState::Validating
        {
            Some(position)
        } else {
            source.entered
        };
        let record = NativeResponseRecord {
            response,
            received,
            entered,
        };
        record.check()?;
        let copied = Self::wrap(record)?;
        if copied.heap_charge()? > self.heap_charge()? {
            return Err(MemoryError::AllocationFailed.into());
        }
        Ok(copied)
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let source = self.record().ok_or(MemoryError::MissingKey)?;
        source.check().map_err(|_| MemoryError::InvalidNeighbors)?;
        let response = source
            .response
            .try_copy(
                source
                    .response
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        let row = Self::wrap(NativeResponseRecord {
            response,
            received: source.received,
            entered: source.entered,
        })?;
        if row.heap_charge()? > self.heap_charge()? {
            return Err(MemoryError::AllocationFailed);
        }
        Ok(row)
    }
}
pub(super) fn response_heap(response: &Response) -> Result<usize, MemoryError> {
    add(
        response
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        response
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?
            .checked_mul(ALLOCATION)
            .ok_or(MemoryError::AllocationFailed)?,
    )
}
