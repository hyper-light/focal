use crate::*;
pub use focal_consensus::{MembershipChange, MembershipConfiguration};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMembershipCommand {
    pub expected_configuration_index: u64,
    pub expected: MembershipConfiguration,
    pub change: MembershipChange,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlConfiguration {
    pub identity: ControlIdentity,
    pub applied_index: u64,
    pub configuration_index: u64,
    pub configuration: MembershipConfiguration,
}
/// The committed entry that last changed a group's configuration, as every
/// replica applied it: what the group's voters attest when the root's grant
/// follows the log (24 §13; the audit's F24), the same on each of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlMembershipRecord {
    pub index: u64,
    pub term: u64,
    /// The digest of the request that made the change; for a change the log
    /// made without one (a joint configuration leaving), the entry's own.
    pub request_hash: [u8; 32],
    pub configuration: MembershipConfiguration,
}
impl ControlMembershipRecord {
    /// The record of a configuration entry that carried no request.
    pub fn of_entry(
        index: u64,
        term: u64,
        configuration: MembershipConfiguration,
    ) -> Result<Self, ControlError> {
        let encoded = postcard::to_stdvec(&(index, term, &configuration))
            .map_err(|_| ControlError::Capacity)?;
        Ok(Self {
            index,
            term,
            request_hash: blake3::derive_key("focal.control.membership-entry.v1", &encoded),
            configuration,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlTransfer {
    pub expected_configuration_index: u64,
    pub expected: MembershipConfiguration,
    pub target: u64,
}
