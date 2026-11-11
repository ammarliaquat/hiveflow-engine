//! Authoritative domain models and contract entities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Execution Run Scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RunScope {
    Full,
    Bronze,
    Silver,
    Gold,
}

impl Default for RunScope {
    fn default() -> Self {
        Self::Full
    }
}

/// Authoritative row definition from `metadata.db_tables_etl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataRow {
    pub table_id: i64,
    pub source_tag: String,
    pub source_table: String,
    pub target_tag: Option<String>,
    pub target_table: Option<String>,
    pub priority: i32,
    pub is_active: bool,
    pub is_cdc: bool,
    pub cdc_strategy: Option<String>,
    pub columns_unique_key: Option<String>,
    pub execution_authority_mode: Option<String>,
    pub last_success_date: Option<String>,
    pub last_success_timestamp: Option<String>,
    pub landing_mode: Option<String>,
}

/// Runtime configuration and constraints for a table processing pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePolicy {
    pub max_parallelism: usize,
    pub batch_chunk_size: usize,
    pub stage_timeout_secs: u64,
    pub max_retries: usize,
}

impl Default for TablePolicy {
    fn default() -> Self {
        Self {
            max_parallelism: 4,
            batch_chunk_size: 50_000,
            stage_timeout_secs: 600,
            max_retries: 3,
        }
    }
}

/// Worker lease claim in `metadata.gui_job_registry`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobScopeClaim {
    pub claim_id: Uuid,
    pub run_id: String,
    pub source_tag: String,
    pub table_name: String,
    pub worker_id: String,
    pub lease_expires_at: DateTime<Utc>,
    pub status: String,
}
