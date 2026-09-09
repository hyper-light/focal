//! One bounded export of the facts a directory registration needs from a hosted
//! session: its identities, membership and current placement fence. Read on
//! the owner thread so the values describe one applied prefix.
use super::*;
use crate::session_registration::HostedSessionFacts;

const REGISTRATION_BYTES: usize = 64 * 1024;

pub struct RegistrationFactsReply {
    value: HostedSessionFacts,
    _charge: Allocation,
}
impl RegistrationFactsReply {
    pub fn value(&self) -> &HostedSessionFacts {
        &self.value
    }
}
impl ReplicaHost {
    /// Trusted local read, absent from the wire API.
    pub async fn registration_facts(&self) -> Result<RegistrationFactsReply, LedgerError> {
        let charge = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                REGISTRATION_BYTES,
            )?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Registration(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
}
impl Owner {
    pub(super) fn registration_facts(
        &self,
        charge: Allocation,
    ) -> Result<RegistrationFactsReply, LedgerError> {
        let value =
            HostedSessionFacts::from_session(&self.session).map_err(|error| match error {
                crate::session_registration::SessionRegistrationError::Ledger(error) => error,
                _ => LedgerError::Capacity,
            })?;
        Ok(RegistrationFactsReply {
            value,
            _charge: charge,
        })
    }
}
