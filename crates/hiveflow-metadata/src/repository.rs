//! Metadata repository queries.

use hiveflow_connections::ConnectionManager;
use hiveflow_core::models::MetadataRow;
use hiveflow_core::error::HiveFlowError;

#[derive(Debug, Clone)]
pub struct MetadataRepository {
    connection_manager: ConnectionManager,
}

impl MetadataRepository {
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self { connection_manager }
    }
}
