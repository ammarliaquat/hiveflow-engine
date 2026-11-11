//! Canonical constants across the HiveFlow ecosystem.

pub const SCHEMA_METADATA: &str = "metadata";
pub const TABLE_DB_TABLES_ETL: &str = "metadata.db_tables_etl";
pub const TABLE_DB_COLUMNS_ETL: &str = "metadata.db_columns_etl";
pub const TABLE_BATCH_LOG: &str = "metadata.batch_log";
pub const TABLE_BATCH_SUMMARY: &str = "metadata.batch_summary";
pub const TABLE_JOB_REGISTRY: &str = "metadata.gui_job_registry";
pub const TABLE_CONNECTIONS: &str = "metadata.connections";

pub const DEFAULT_MAX_PARALLELISM: usize = 4;
pub const DEFAULT_BATCH_CHUNK_SIZE: usize = 50_000;
pub const DEFAULT_STAGE_TIMEOUT_SECS: u64 = 600;

pub const DEFAULT_ENVIRONMENT: &str = "PROD";
