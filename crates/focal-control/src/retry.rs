use crate::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClientWindow {
    floor: u64,
    highest: u64,
    receipts: BTreeMap<u64, ControlReceipt>,
}
pub(crate) type RetryCheckpoint = BTreeMap<[u8; 16], ClientWindow>;
pub(crate) struct RetryState {
    pub checkpoint: RetryCheckpoint,
    _allocation: Allocation,
}
impl RetryState {
    pub fn restore(
        checkpoint: RetryCheckpoint,
        limits: &ControlLimits,
        budget: &MemoryBudget,
        applied_index: u64,
    ) -> Result<Self, ControlError> {
        if checkpoint.len() > limits.max_clients {
            return Err(ControlError::Capacity);
        }
        for (client, window) in &checkpoint {
            if *client == [0; 16]
                || window.floor > window.highest
                || window.receipts.len() > limits.max_receipts_per_client
                || window.highest.saturating_sub(window.floor) != window.receipts.len() as u64
            {
                return Err(ControlError::Corrupt("client retry window"));
            }
            for (offset, (sequence, receipt)) in window.receipts.iter().enumerate() {
                if Some(*sequence)
                    != window
                        .floor
                        .checked_add(offset as u64)
                        .and_then(|value| value.checked_add(1))
                    || receipt.request.client != *client
                    || receipt.request.sequence != *sequence
                    || receipt.committed_index == 0
                    || receipt.committed_index > applied_index
                    || receipt.committed_term == 0
                {
                    return Err(ControlError::Corrupt("client receipt"));
                }
            }
        }
        let allocation = budget
            .reserve(
                BudgetKind::Dedup,
                BudgetLane::Completion,
                retry_charge(&checkpoint)?,
            )?
            .commit();
        Ok(Self {
            checkpoint,
            _allocation: allocation,
        })
    }
    pub fn existing(
        &self,
        request: &ControlRequest,
        digest: [u8; 32],
    ) -> Result<Option<ControlReceipt>, ControlError> {
        if request.id.client == [0; 16] || request.id.sequence == 0 {
            return Err(ControlError::Invalid);
        }
        let Some(window) = self.checkpoint.get(&request.id.client) else {
            if request.id.sequence != 1 || request.acknowledged_through != 0 {
                return Err(ControlError::RetryOrder);
            }
            return Ok(None);
        };
        if request.id.sequence <= window.floor {
            return Err(ControlError::RetryExpired);
        }
        if let Some(receipt) = window.receipts.get(&request.id.sequence) {
            return if receipt.request_hash == digest {
                Ok(Some(*receipt))
            } else {
                Err(ControlError::RetryConflict)
            };
        }
        if window.highest.checked_add(1) != Some(request.id.sequence)
            || request.acknowledged_through < window.floor
            || request.acknowledged_through > window.highest
        {
            return Err(ControlError::RetryOrder);
        }
        Ok(None)
    }
    pub fn prepare(
        &self,
        request: &ControlRequest,
        digest: [u8; 32],
        limits: &ControlLimits,
        budget: &MemoryBudget,
    ) -> Result<Self, ControlError> {
        if self.existing(request, digest)?.is_some() {
            return Err(ControlError::RetryConflict);
        }
        if !self.checkpoint.contains_key(&request.id.client)
            && self.checkpoint.len() == limits.max_clients
        {
            return Err(ControlError::Capacity);
        }
        let allocation = budget
            .reserve(
                BudgetKind::Dedup,
                BudgetLane::Completion,
                retry_charge(&self.checkpoint)?
                    .checked_add(
                        size_of::<ClientWindow>()
                            .saturating_add(size_of::<ControlReceipt>())
                            .saturating_add(128)
                            .saturating_mul(16),
                    )
                    .ok_or(ControlError::Capacity)?,
            )?
            .commit();
        let mut checkpoint = self.checkpoint.clone();
        let window = checkpoint
            .entry(request.id.client)
            .or_insert_with(|| ClientWindow {
                floor: 0,
                highest: 0,
                receipts: BTreeMap::new(),
            });
        window
            .receipts
            .retain(|sequence, _| *sequence > request.acknowledged_through);
        window.floor = request.acknowledged_through;
        if window.receipts.len() == limits.max_receipts_per_client {
            return Err(ControlError::Capacity);
        }
        window.highest = request.id.sequence;
        window.receipts.insert(
            request.id.sequence,
            ControlReceipt {
                request: request.id,
                request_hash: digest,
                committed_index: 0,
                committed_term: 0,
                revisions: ControlRevisions::default(),
            },
        );
        Ok(Self {
            checkpoint,
            _allocation: allocation,
        })
    }
    pub fn complete(
        &mut self,
        id: ControlRequestId,
        index: u64,
        term: u64,
        revisions: ControlRevisions,
    ) -> Result<ControlReceipt, ControlError> {
        let receipt = self
            .checkpoint
            .get_mut(&id.client)
            .and_then(|window| window.receipts.get_mut(&id.sequence))
            .ok_or(ControlError::Corrupt("missing prepared receipt"))?;
        receipt.committed_index = index;
        receipt.committed_term = term;
        receipt.revisions = revisions;
        Ok(*receipt)
    }
    pub fn lookup(&self, id: ControlRequestId) -> Result<Option<ControlReceipt>, ControlError> {
        let Some(window) = self.checkpoint.get(&id.client) else {
            return Ok(None);
        };
        if id.sequence <= window.floor {
            return Err(ControlError::RetryExpired);
        }
        Ok(window.receipts.get(&id.sequence).copied())
    }
    pub fn latest(&self, client: [u8; 16]) -> Result<Option<ControlReceipt>, ControlError> {
        let Some(window) = self.checkpoint.get(&client) else {
            return Ok(None);
        };
        let receipt = window
            .receipts
            .get(&window.highest)
            .ok_or(ControlError::Corrupt("client window lost latest receipt"))?;
        Ok(Some(*receipt))
    }
}
fn retry_charge(state: &RetryCheckpoint) -> Result<usize, ControlError> {
    let receipts = state.values().try_fold(0usize, |sum, window| {
        sum.checked_add(window.receipts.len())
            .ok_or(ControlError::Capacity)
    })?;
    256usize
        .checked_add(charge(
            state.len(),
            size_of::<ClientWindow>()
                .saturating_add(64)
                .saturating_mul(16),
        )?)
        .and_then(|n| {
            n.checked_add(
                receipts.checked_mul(
                    size_of::<ControlReceipt>()
                        .saturating_add(64)
                        .saturating_mul(16),
                )?,
            )
        })
        .ok_or(ControlError::Capacity)
}
