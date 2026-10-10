//! What the runtime reads from the machine, from slates' `machine` (ORIGIN.md): the cores the process
//! may run on and its CPU budget, the base page, the wake latency and its online estimate, the
//! stopping rule the probes measure under, and the placement of shards on cores.

pub mod bench;
pub mod calibration;
pub mod clock;
pub mod derived;
pub mod error;
pub mod facts;
pub mod placement;
pub mod probes;
pub mod record;
pub mod stats;
pub mod wake;

pub use derived::Derived;
pub use error::MachineError;
