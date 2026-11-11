//! ConnectionManager implementation.

use hiveflow_core::error::HiveFlowError;
use sqlx::{AnyPool, Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Authoritative Connection Manager for all source and target databases.
#[derive(Debug, Clone, Default)]
pub struct ConnectionManager {
    // Dynamic connection pools indexed by endpoint tag
    pools: Arc<RwLock<HashMap<String, String>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

