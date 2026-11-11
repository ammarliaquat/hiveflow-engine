//! hiveflow-connections — Connection management and dialect pooling.
//!
//! Enforces Single Source of Truth for database endpoints and secrets.

pub mod manager;

pub use manager::{ConnectionManager, map_sqlx_connection_error};
