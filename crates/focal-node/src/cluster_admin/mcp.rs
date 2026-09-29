use super::*;
use focal_mcp::{AdminAction, AdminBackend, AdminChange, AdminError};
impl AdminBackend for ClusterAdmin {
    fn execute(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        action: AdminAction,
        cancel: &mut tokio::sync::oneshot::Receiver<()>,
    ) -> std::result::Result<AdminResult, AdminError> {
        let work = async {
            match action {
                AdminAction::NodeIdentity => {
                    self.operator(crate::network_admin::OperatorRead::Identity)
                        .await
                }
                AdminAction::NodeReadiness => {
                    self.operator(crate::network_admin::OperatorRead::Readiness)
                        .await
                }
                AdminAction::NodeMetrics => {
                    self.operator(crate::network_admin::OperatorRead::Metrics)
                        .await
                }
                AdminAction::NodeHealth => {
                    self.operator(crate::network_admin::OperatorRead::Health)
                        .await
                }
                AdminAction::NodeConfiguration => {
                    self.operator(crate::network_admin::OperatorRead::Configuration)
                        .await
                }
                AdminAction::ReplicaDiagnostics { session } => {
                    self.operator(crate::network_admin::OperatorRead::Replica {
                        session: session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                    })
                    .await
                }
                AdminAction::RetentionShow { session } => {
                    self.retention(
                        session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                    )
                    .await
                }
                AdminAction::GcShow => self.gc().await,
                AdminAction::StorageShow => self.storage().await,
                AdminAction::BackupCreate {
                    tenant,
                    session,
                    output,
                } => {
                    self.backup_create(
                        focal_model::LedgerId {
                            tenant: tenant
                                .map(focal_model::TenantId)
                                .unwrap_or(self.identity.ledger.tenant),
                            session: session
                                .map(focal_model::SessionId)
                                .unwrap_or(self.identity.ledger.session),
                        },
                        std::path::Path::new(&output),
                    )
                    .await
                }
                AdminAction::BackupVerify { input } => {
                    ClusterAdmin::backup_verify(std::path::Path::new(&input))
                }
                AdminAction::Repair {
                    tenant,
                    session,
                    after,
                    limit,
                } => {
                    self.repair(
                        focal_model::LedgerId {
                            tenant: tenant
                                .map(focal_model::TenantId)
                                .unwrap_or(self.identity.ledger.tenant),
                            session: session
                                .map(focal_model::SessionId)
                                .unwrap_or(self.identity.ledger.session),
                        },
                        after,
                        limit,
                    )
                    .await
                }
                AdminAction::Restore {
                    input,
                    new_incarnation,
                } => {
                    self.restore(std::path::Path::new(&input), new_incarnation)
                        .await
                }
                AdminAction::GcRestore { domain, root } => {
                    self.gc_restore(
                        focal_model::ContentDomainId(domain),
                        focal_model::ContentHash(root),
                    )
                    .await
                }
                AdminAction::ArchiveShow { session, claim } => {
                    self.archive(
                        session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                        focal_model::ClaimId(claim),
                    )
                    .await
                }
                AdminAction::ReplicaTransfer {
                    session,
                    node,
                    expected_configuration_index,
                } => {
                    self.replica_transfer(
                        session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                        node,
                        expected_configuration_index,
                    )
                    .await
                }
                AdminAction::InviteNode { name, output } => {
                    self.invite_node(&name, Path::new(&output)).await
                }
                AdminAction::ReplicaList { after, limit } => {
                    self.replica_list(after.map(focal_model::SessionId), limit)
                        .await
                }
                AdminAction::ReplicaShow { session } => {
                    self.replica_show(
                        session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                    )
                    .await
                }
                AdminAction::ReplicaChange {
                    session,
                    change,
                    expected_configuration_index,
                } => {
                    self.replica_change(
                        session
                            .map(focal_model::SessionId)
                            .unwrap_or(self.identity.ledger.session),
                        match change {
                            AdminChange::AddLearner(node) => MembershipChange::AddLearner { node },
                            AdminChange::Promote(node) => MembershipChange::Promote { node },
                            AdminChange::Remove(node) => MembershipChange::Remove { node },
                            AdminChange::LeaveJoint => MembershipChange::LeaveJoint,
                        },
                        expected_configuration_index,
                    )
                    .await
                }
                AdminAction::ReplicaInspect => self.replica_inspect(),
                AdminAction::ReplicaRetry {
                    operation_id,
                    reconcile,
                } => {
                    if reconcile {
                        self.replica_reconcile(&operation_id).await
                    } else {
                        self.replica_retry(&operation_id).await
                    }
                }
                AdminAction::InviteClient { name, output } => {
                    self.invite_client(&name, Path::new(&output)).await
                }
                AdminAction::Invitations {
                    after,
                    limit,
                    expected_revision,
                } => {
                    self.read(AdminRead::Invitations {
                        after,
                        limit,
                        expected_revision,
                    })
                    .await
                }
                AdminAction::Invitation { id } => self.read(AdminRead::Invitation { id }).await,
                AdminAction::Revoke {
                    id,
                    expected_revision,
                } => self.revoke(id, expected_revision).await,
                AdminAction::RenewCredential => self.renew_credential().await,
                AdminAction::RotateCredential => self.rotate_credential().await,
                AdminAction::Placement => self.placement().await,
                AdminAction::Plan => self.plan().await,
                AdminAction::AdmitTenant { tenant } => self.admit_tenant(tenant).await,
                AdminAction::Tenants => self.tenants().await,
                AdminAction::UpgradeStatus => self.upgrade_status().await,
                AdminAction::ActivateFence { level } => self.activate_fence(level).await,
                AdminAction::CreateSession { tenant, name } => {
                    self.create_session(tenant, &name).await
                }
                AdminAction::PlanSession {
                    tenant,
                    session,
                    survive,
                    max_failures,
                    dry_run,
                } => {
                    self.plan_session(tenant, session, &survive, max_failures, dry_run)
                        .await
                }
                AdminAction::Status => self.read(AdminRead::Membership).await,
                AdminAction::Configuration => self.read(AdminRead::Configuration).await,
                AdminAction::Contacts => self.read(AdminRead::Contacts).await,
                AdminAction::Membership {
                    change,
                    expected_configuration_index,
                } => {
                    self.membership(
                        match change {
                            AdminChange::AddLearner(node) => MembershipChange::AddLearner { node },
                            AdminChange::Promote(node) => MembershipChange::Promote { node },
                            AdminChange::Remove(node) => MembershipChange::Remove { node },
                            AdminChange::LeaveJoint => MembershipChange::LeaveJoint,
                        },
                        expected_configuration_index,
                    )
                    .await
                }
                AdminAction::Transfer {
                    node,
                    expected_configuration_index,
                } => self.transfer(node, expected_configuration_index).await,
                AdminAction::NodeEligibility { node, eligible } => {
                    self.node_eligibility(node, eligible).await
                }
                AdminAction::RemoveNode { node } => self.remove_node(node).await,
                AdminAction::ReplaceNode { node, replacement } => {
                    self.replace_node(node, replacement).await
                }
                AdminAction::Inspect => self.inspect(),
                AdminAction::Retry { operation_id } => self.retry(&operation_id).await,
                AdminAction::Reconcile { operation_id } => self.reconcile(&operation_id).await,
            }
        };
        runtime.block_on(async {
            tokio::select! {
                result=work=>result.map_err(error),
                _=cancel=>Err(AdminError { code:"cancelled",condition:"Cancelled",detail:"Admin wait cancelled; inspect the latest journal before issuing a new mutation.".into() }),
            }
        })
    }
}
fn error(error: ClusterAdminError) -> AdminError {
    let classification = error.classification();
    AdminError {
        code: classification.code,
        condition: classification.condition,
        detail: error.to_string(),
    }
}
