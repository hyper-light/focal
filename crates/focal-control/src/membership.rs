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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlTransfer {
    pub expected_configuration_index: u64,
    pub expected: MembershipConfiguration,
    pub target: u64,
}
