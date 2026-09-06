//! Local physical-owner observations. These are not quorum-read capabilities.
use super::*;
use focal_client::admin::AdminReplicaDiagnostics;

// Includes transient status membership copies (up to 1024 entries per list),
// the result's bounded strings, channel bookkeeping and retained delivery.
const DIAGNOSTICS_BYTES: usize = 64 * 1024;

pub struct ReplicaDiagnosticsReply {
    value: AdminReplicaDiagnostics,
    _charge: Allocation,
}
impl ReplicaDiagnosticsReply {
    pub fn value(&self) -> &AdminReplicaDiagnostics {
        &self.value
    }
}
impl ReplicaHost {
    pub async fn diagnostics(&self) -> Result<ReplicaDiagnosticsReply, LedgerError> {
        let charge = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                DIAGNOSTICS_BYTES,
            )?
            .commit();
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Diagnostics(send, charge))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::Failed)?
    }
}
impl Owner {
    pub(super) fn diagnostics(&self, charge: Allocation) -> ReplicaDiagnosticsReply {
        let status = self.session.status();
        let value = AdminReplicaDiagnostics {
            node: status.node_id,
            cluster: hex(&self.session.cluster_id()),
            session: self.session.ledger().session.to_string(),
            group: hex(&self.session.group_id()),
            leader: status.leader_id,
            term: status.term,
            committed_index: status.committed_index,
            applied_index: status.applied_index,
            sequence: self.session.sequence().0,
            pending: self.session.pending_count(),
            authoritative: self.session.is_authoritative(),
            persistence_pending: self.session.persistence_pending(),
            checkpoint_pending: self.session.checkpoint_in_flight(),
            compiled_managed_decoder: hex(&Session::managed_decoder_hash()),
            required_decoder: self.session.required_decoder().map(|hash| hex(&hash)),
            managed_active: self.session.managed_protocol_active(),
        };
        ReplicaDiagnosticsReply {
            value,
            _charge: charge,
        }
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 15])
        .filter_map(|nibble| char::from_digit(u32::from(nibble), 16))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_observation_does_not_install_a_floor_and_owns_its_delivered_bytes() {
        let disk = tempfile::tempdir().unwrap();
        let ledger = LedgerId {
            tenant: TenantId::from_u128(501),
            session: SessionId::from_u128(502),
        };
        let session = Session::open(
            disk.path(),
            ledger,
            focal_consensus::NodeConfig::single(1, [51; 16], ledger.session.0),
            focal_ledger::SessionLimits::default(),
        )
        .unwrap();
        let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
        let (sender, _receiver) = mpsc::sync_channel(4);
        let (outbound, _outgoing) = async_mpsc::channel(16);
        let (_host, mut owner) = ReplicaHost::assemble(
            session,
            ReplicaConfig::new(RootCommandId::from_u128(503)),
            ReplicaHost::wire_limits(),
            None,
            budget.clone(),
            HostSender::Direct(sender),
            outbound,
        )
        .unwrap();
        assert!(!owner.session.managed_support_demanded());
        let baseline = budget.stats().used;
        let (send, receive) = oneshot::channel();
        let charge = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                DIAGNOSTICS_BYTES,
            )
            .unwrap()
            .commit();
        owner.accept(Work::Diagnostics(send, charge)).unwrap();
        let reply = receive.blocking_recv().unwrap().unwrap();
        assert!(!owner.session.managed_support_demanded());
        assert_eq!(reply.value().required_decoder, None);
        assert_eq!(reply.value().sequence, 0);
        assert_eq!(budget.stats().used, baseline + DIAGNOSTICS_BYTES);
        drop(reply);
        assert_eq!(budget.stats().used, baseline);
        owner.session.begin_managed_support().unwrap();
        for _ in 0..4 {
            owner.session.poll().unwrap();
        }
        let charge = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                DIAGNOSTICS_BYTES,
            )
            .unwrap()
            .commit();
        let reply = owner.diagnostics(charge);
        assert_eq!(
            reply.value().required_decoder.as_ref(),
            Some(&reply.value().compiled_managed_decoder)
        );
        assert!(
            !reply.value().managed_active,
            "a local durable decoder floor alone does not activate managed admission"
        );
    }
}
