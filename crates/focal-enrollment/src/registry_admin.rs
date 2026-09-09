//! Redacted operator observations; no enrollment secret or token hash is exported.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvitationStatus {
    pub id: InvitationId,
    pub role: EnrollmentRole,
    pub expires_at: i64,
    pub revoked: bool,
    pub enrollment: Option<EnrolledCredentialStatus>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrolledCredentialStatus {
    pub node: Option<u64>,
    pub principal: [u8; 16],
    pub issued_at: i64,
    pub expires_at: i64,
    pub revision: u64,
    pub certificate_fingerprint: Fingerprint,
}
impl EnrollmentCommand {
    /// Identifies only the existing exact revocation mutation. A local admin
    /// transport must reject invitation issuance and credential consumption.
    pub fn revoked_invitation(&self) -> Option<InvitationId> {
        match self.change {
            Change::Revoke { invitation } => Some(invitation),
            _ => None,
        }
    }
    /// Identifies a renewal, which only the enrollment host may commit.
    pub fn renewed_invitation(&self) -> Option<InvitationId> {
        match self.change {
            Change::Renew { invitation, .. } => Some(invitation),
            _ => None,
        }
    }
}
impl EnrollmentRegistry {
    pub fn invitation_status(&self, id: InvitationId) -> Option<InvitationStatus> {
        self.records.get(&id).map(status)
    }
    pub fn invitation_page(
        &self,
        after: Option<InvitationId>,
        limit: u16,
    ) -> Result<(Vec<InvitationStatus>, Option<InvitationId>), EnrollmentError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if limit == 0 || limit > 64 {
            return Err(EnrollmentError::Capacity);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(usize::from(limit))
            .map_err(|_| EnrollmentError::Capacity)?;
        let mut rows = self
            .records
            .range((after.map_or(Unbounded, Excluded), Unbounded));
        for _ in 0..limit {
            let Some((_, record)) = rows.next() else {
                return Ok((entries, None));
            };
            entries.push(status(record));
        }
        let next = if rows.next().is_some() {
            entries.last().map(|record| record.id)
        } else {
            None
        };
        Ok((entries, next))
    }
}
fn status(record: &InviteMetadata) -> InvitationStatus {
    InvitationStatus {
        id: record.id,
        role: record.role,
        expires_at: record.expires_at,
        revoked: record.revoked,
        enrollment: record
            .receipt
            .as_ref()
            .map(|receipt| EnrolledCredentialStatus {
                node: receipt.identity.node_id,
                principal: receipt.identity.principal,
                issued_at: receipt.issued_at,
                expires_at: receipt.expires_at,
                revision: receipt.revision,
                certificate_fingerprint: server_fingerprint(&receipt.certificate),
            }),
    }
}
