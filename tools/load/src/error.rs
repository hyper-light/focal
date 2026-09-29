//! The tool's typed failures. Every one is returned to `main`, which prints a
//! line and exits non-zero; nothing in this crate unwinds (CLAUDE.md §1 holds
//! for the measurement tools too).
use std::fmt;

#[derive(Debug)]
pub enum LoadError {
    /// Reading the shape file, writing the report, walking the data directory
    /// or building a runtime.
    Io(std::io::Error),
    /// The shape did not parse or failed its bounds.
    Shape(String),
    /// Opening, activating or reopening the embedded node, or reading a
    /// running node's identity.
    Node(focal_node::embedded::NodeError),
    /// The running node's Unix-socket participant could not be established.
    Join(focal_node::network_join::JoinError),
    /// A request that could not be sent or answered at all.
    Client(focal_client::ClientError),
    /// A transport that could not be opened.
    Wire(focal_wire::WireError),
    /// A request fixture that could not be built: a defect of this tool, never
    /// of the node.
    Fixture(String),
    /// A worker thread ended without a result.
    Worker(&'static str),
    /// The wall clock is before the epoch or past `i64`.
    Clock,
    /// An arithmetic or capacity bound of the tool itself.
    Bound(&'static str),
    /// The report could not be serialized.
    Report(serde_json::Error),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "i/o: {error}"),
            Self::Shape(reason) => write!(f, "invalid shape: {reason}"),
            Self::Node(error) => write!(f, "node: {error}"),
            Self::Join(error) => write!(f, "local node client context: {error}"),
            Self::Client(error) => write!(f, "client: {error}"),
            Self::Wire(error) => write!(f, "transport: {error}"),
            Self::Fixture(reason) => write!(f, "request fixture: {reason}"),
            Self::Worker(reason) => write!(f, "worker: {reason}"),
            Self::Clock => f.write_str("system clock out of range"),
            Self::Bound(what) => write!(f, "bound exceeded: {what}"),
            Self::Report(error) => write!(f, "report: {error}"),
        }
    }
}

impl std::error::Error for LoadError {}

impl From<std::io::Error> for LoadError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<focal_node::embedded::NodeError> for LoadError {
    fn from(error: focal_node::embedded::NodeError) -> Self {
        Self::Node(error)
    }
}
impl From<focal_node::network_join::JoinError> for LoadError {
    fn from(error: focal_node::network_join::JoinError) -> Self {
        Self::Join(error)
    }
}
impl From<focal_client::ClientError> for LoadError {
    fn from(error: focal_client::ClientError) -> Self {
        Self::Client(error)
    }
}
impl From<focal_wire::WireError> for LoadError {
    fn from(error: focal_wire::WireError) -> Self {
        Self::Wire(error)
    }
}
impl From<serde_json::Error> for LoadError {
    fn from(error: serde_json::Error) -> Self {
        Self::Report(error)
    }
}
impl From<crate::native::FixtureError> for LoadError {
    fn from(error: crate::native::FixtureError) -> Self {
        Self::Fixture(error.0)
    }
}
