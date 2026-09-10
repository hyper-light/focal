//! The node-local facts of a configuration: identity storage, addresses,
//! bounds and topology. A startup may override eligible fields from the
//! command line; none of them is a cluster or session policy.
use super::{NodeSettings, Topology};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalFacts {
    pub node: NodeSettings,
    pub topology: Topology,
}
