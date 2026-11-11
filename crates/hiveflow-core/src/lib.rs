//! hiveflow-core — canonical types, error taxonomy, and shared models.
//!
//! Every crate in the hiveflow-engine workspace imports this crate.
//! No business logic lives here; only data structures and error kinds.

pub mod error;
pub mod models;
pub mod types;
pub mod constants;

pub use error::HiveFlowError;
pub use models::{MetadataRow, TablePolicy, JobScopeClaim, RunScope};
