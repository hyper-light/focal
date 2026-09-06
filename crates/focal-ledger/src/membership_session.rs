pub use focal_consensus::{MembershipChange, MembershipConfiguration};

const MEMBERSHIP_MAGIC: &[u8] = b"FOCALMC1";

/// Trusted placement intent. The expected index fences ABA configurations;
/// the full expected configuration prevents admission against a stale route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMembershipRequest {
    pub id: [u8; 16],
    pub expected_index: u64,
    pub expected: MembershipConfiguration,
    pub change: MembershipChange,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMembershipReceipt {
    pub id: [u8; 16],
    pub request_hash: [u8; 32],
    pub index: u64,
    pub term: u64,
    pub configuration: MembershipConfiguration,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    pub configuration_index: u64,
    pub configuration: MembershipConfiguration,
    /// Only the latest configuration operation is retained. Older retries must
    /// obtain a current view and reconcile their intended placement.
    pub latest: Option<SessionMembershipReceipt>,
}
#[derive(Default, Serialize, Deserialize)]
struct MembershipState {
    configuration_index: u64,
    latest: Option<SessionMembershipReceipt>,
}
#[derive(Serialize, Deserialize)]
struct MembershipContext {
    ledger: LedgerId,
    id: [u8; 16],
    expected_index: u64,
    change: MembershipChange,
    hash: [u8; 32],
}
struct PendingMembership {
    id: [u8; 16],
    hash: [u8; 32],
    charge: Allocation,
}
impl SessionMembershipRequest {
    pub fn validate(&self) -> Result<(), LedgerError> {
        if self.id == [0; 16] {
            return Err(LedgerError::MembershipConflict);
        }
        self.expected.validate()?;
        Ok(())
    }
    fn hash(&self) -> Result<[u8; 32], LedgerError> {
        self.validate()?;
        Ok(*blake3::hash(&postcard::to_stdvec(self)?).as_bytes())
    }
}
impl Session {
    /// Local published view; hosts use ReadIndex before returning it as current.
    /// A retained Ready can have staged Raft configuration changes that the
    /// application has not published yet, so no mixed-prefix view is exposed.
    pub fn membership(&self) -> Result<MembershipView, LedgerError> {
        self.check()?;
        if self.persistence_pending() {
            return Err(ConsensusError::PersistencePending.into());
        }
        Ok(MembershipView {
            configuration_index: self.membership_state.configuration_index,
            configuration: self.consensus.membership_configuration(),
            latest: self.membership_state.latest.clone(),
        })
    }
    pub fn membership_receipt(
        &self,
        request: &SessionMembershipRequest,
    ) -> Result<Option<&SessionMembershipReceipt>, LedgerError> {
        let _scratch = self.budget.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            request
                .expected
                .charged_bytes()?
                .checked_mul(2)
                .ok_or(LedgerError::Capacity)?
                .checked_add(512)
                .ok_or(LedgerError::Capacity)?,
        )?;
        let hash = request.hash()?;
        if let Some(receipt) = &self.membership_state.latest
            && receipt.id == request.id
        {
            if receipt.request_hash != hash {
                return Err(LedgerError::MembershipConflict);
            }
            return Ok(Some(receipt));
        }
        Ok(None)
    }
    /// Admission only. Caller must wait for the applied receipt and a fresh
    /// current-term ReadIndex before reporting authoritative completion.
    pub fn propose_membership(
        &mut self,
        request: &SessionMembershipRequest,
    ) -> Result<(), LedgerError> {
        self.check()?;
        if !self.is_authoritative() {
            return Err(LedgerError::NotReady {
                leader: self.status().leader_id,
            });
        }
        let _scratch = self.budget.reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            request
                .expected
                .charged_bytes()?
                .checked_mul(4)
                .and_then(|n| n.checked_add(2048))
                .ok_or(LedgerError::Capacity)?,
        )?;
        if self.membership_receipt(request)?.is_some() {
            return Ok(());
        }
        if self.pending_placement.is_some() || self.placement_state.paused() {
            return Err(LedgerError::Capacity);
        }
        if request.expected_index != self.membership_state.configuration_index {
            return Err(LedgerError::MembershipConflict);
        }
        let hash = request.hash()?;
        if let Some(pending) = &self.pending_membership {
            return if pending.id == request.id && pending.hash == hash {
                Ok(())
            } else if pending.id == request.id {
                Err(LedgerError::MembershipConflict)
            } else {
                Err(LedgerError::Capacity)
            };
        }
        let next = request.change.apply_to(&request.expected)?;
        let bytes = next
            .charged_bytes()?
            .checked_add(512)
            .ok_or(LedgerError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
            .commit();
        let context = MembershipContext {
            ledger: self.ledger,
            id: request.id,
            expected_index: request.expected_index,
            change: request.change,
            hash,
        };
        let mut encoded = MEMBERSHIP_MAGIC.to_vec();
        encoded.extend(postcard::to_stdvec(&context)?);
        self.consensus
            .propose_membership(&request.expected, request.change, encoded)?;
        self.pending_membership = Some(PendingMembership {
            id: request.id,
            hash,
            charge,
        });
        Ok(())
    }
    fn apply_membership(
        &mut self,
        applied: focal_consensus::AppliedMembership,
    ) -> Result<(), LedgerError> {
        if applied.term == 0 || applied.index <= self.membership_state.configuration_index {
            return Err(LedgerError::Corrupt);
        }
        let Some(encoded) = applied.context.strip_prefix(MEMBERSHIP_MAGIC) else {
            // Bootstrap and compatibility Raft changes still fence later intents.
            self.membership_state = MembershipState {
                configuration_index: applied.index,
                latest: None,
            };
            self.membership_charge = None;
            self.pending_membership = None;
            return Ok(());
        };
        let (context, remaining): (MembershipContext, _) = postcard::take_from_bytes(encoded)?;
        if !remaining.is_empty() {
            return Err(LedgerError::Corrupt);
        }
        let request = SessionMembershipRequest {
            id: context.id,
            expected_index: context.expected_index,
            expected: applied.before,
            change: context.change,
        };
        if context.ledger != self.ledger
            || context.expected_index != self.membership_state.configuration_index
            || request.hash()? != context.hash
            || request.change.apply_to(&request.expected)? != applied.after
        {
            return Err(LedgerError::Corrupt);
        }
        let bytes = applied
            .after
            .charged_bytes()?
            .checked_add(512)
            .ok_or(LedgerError::Capacity)?;
        let charge = match self.pending_membership.take() {
            Some(pending)
                if pending.id == context.id
                    && pending.hash == context.hash
                    && pending.charge.bytes() >= bytes =>
            {
                pending.charge
            }
            _ => self
                .budget
                .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
                .commit(),
        };
        self.membership_state = MembershipState {
            configuration_index: applied.index,
            latest: Some(SessionMembershipReceipt {
                id: context.id,
                request_hash: context.hash,
                index: applied.index,
                term: applied.term,
                configuration: applied.after,
            }),
        };
        self.membership_charge = Some(charge);
        Ok(())
    }
}
