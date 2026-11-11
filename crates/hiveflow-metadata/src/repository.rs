//! Metadata repository queries.

use hiveflow_connections::{map_sqlx_connection_error, ConnectionManager};
use hiveflow_core::error::HiveFlowError;
use hiveflow_core::models::MetadataRow;
use sqlx::Row;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct MetadataRepository {
    connection_manager: ConnectionManager,
}

impl MetadataRepository {
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self { connection_manager }
    }

    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    /// Query active pipelines from metadata.db_tables_etl
    pub async fn get_active_tables(
        &self,
        meta_db_url: &str,
        source_tag: Option<&str>,
    ) -> Result<Vec<MetadataRow>, HiveFlowError> {
        let pool = self.connection_manager.get_pg_pool(meta_db_url, 4).await?;

        let mut query = String::from(
            "SELECT table_id, source_tag, source_table, target_tag, target_table, \
             priority, is_active, is_cdc, cdc_strategy, columns_unique_key, \
             execution_authority_mode, last_success_date, last_success_timestamp, landing_mode \
             FROM metadata.db_tables_etl WHERE LOWER(is_active) IN ('y', 'true', '1')",
        );

        if let Some(tag) = source_tag {
            if tag.to_lowercase() != "all" {
                query.push_str(&format!(
                    " AND LOWER(source_tag) = LOWER('{}')",
                    tag.replace('\'', "''")
                ));
            }
        }

        query.push_str(" ORDER BY priority ASC, table_id ASC");

        let rows = sqlx::query(&query).fetch_all(&pool).await.map_err(|e| {
            map_sqlx_connection_error(e, "Fetching active tables from metadata")
        })?;

        let mut result = Vec::with_capacity(rows.len());
        for r in rows {
            let is_active_str: String = r.try_get("is_active").unwrap_or_else(|_| "y".to_string());
            let is_active = matches!(is_active_str.to_lowercase().as_str(), "y" | "true" | "1");
            let is_cdc_str: String = r.try_get("is_cdc").unwrap_or_else(|_| "n".to_string());
            let is_cdc = matches!(is_cdc_str.to_lowercase().as_str(), "y" | "true" | "1");

            result.push(MetadataRow {
                table_id: r.try_get("table_id").unwrap_or(0),
                source_tag: r.try_get("source_tag").unwrap_or_default(),
                source_table: r.try_get("source_table").unwrap_or_default(),
                target_tag: r.try_get("target_tag").ok(),
                target_table: r.try_get("target_table").ok(),
                priority: r.try_get("priority").unwrap_or(10),
                is_active,
                is_cdc,
                cdc_strategy: r.try_get("cdc_strategy").ok(),
                columns_unique_key: r.try_get("columns_unique_key").ok(),
                execution_authority_mode: r.try_get("execution_authority_mode").ok(),
                last_success_date: r.try_get("last_success_date").ok(),
                last_success_timestamp: r.try_get("last_success_timestamp").ok(),
                landing_mode: r.try_get("landing_mode").ok(),
            });
        }

        debug!(
            "Retrieved {} active tables from metadata repository",
            result.len()
        );
        Ok(result)
    }
}

