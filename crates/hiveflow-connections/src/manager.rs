//! ConnectionManager implementation.
//!
//! Provides authoritative connection pooling and dialect safety with machine-classified
//! error mapping (P0 invariant: zero hardcoded credentials, connection errors preserved).

use hiveflow_core::error::HiveFlowError;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info};

pub fn map_sqlx_connection_error(err: sqlx::Error, context_msg: &str) -> HiveFlowError {
    match &err {
        sqlx::Error::PoolTimedOut => {
            HiveFlowError::TargetConnectionTimeout(format!("{}: pool timed out ({})", context_msg, err))
        }
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().unwrap_or_default();
            // Postgres: 28P01 (invalid password), 28000 (invalid authorization spec)
            // MySQL: 1045 (access denied)
            if code == "28P01" || code == "28000" || code == "1045" {
                HiveFlowError::TargetAuthenticationFailed(format!(
                    "{}: authentication failed: {}",
                    context_msg,
                    db_err.message()
                ))
            } else if code == "42P01" || code == "1146" {
                HiveFlowError::TargetTableNotFound(format!(
                    "{}: table not found: {}",
                    context_msg,
                    db_err.message()
                ))
            } else {
                HiveFlowError::Database(err)
            }
        }
        sqlx::Error::Io(io_err) => {
            use std::io::ErrorKind;
            match io_err.kind() {
                ErrorKind::ConnectionRefused => {
                    HiveFlowError::TargetConnectionRefused(format!("{}: connection refused", context_msg))
                }
                ErrorKind::TimedOut => {
                    HiveFlowError::TargetConnectionTimeout(format!("{}: connection timed out", context_msg))
                }
                _ => HiveFlowError::TargetUnreachable(format!("{}: {}", context_msg, io_err)),
            }
        }
        _ => HiveFlowError::Database(err),
    }
}

/// Authoritative Connection Manager for all source and target databases.
#[derive(Debug, Clone, Default)]
pub struct ConnectionManager {
    pg_pools: Arc<RwLock<HashMap<String, PgPool>>>,
    mysql_pools: Arc<RwLock<HashMap<String, MySqlPool>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            pg_pools: Arc::new(RwLock::new(HashMap::new())),
            mysql_pools: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Obtain or create a pooled PostgreSQL connection.
    pub async fn get_pg_pool(&self, url: &str, max_connections: u32) -> Result<PgPool, HiveFlowError> {
        {
            let pools = self.pg_pools.read().await;
            if let Some(pool) = pools.get(url) {
                return Ok(pool.clone());
            }
        }

        let mut pools = self.pg_pools.write().await;
        if let Some(pool) = pools.get(url) {
            return Ok(pool.clone());
        }

        info!("Creating new PostgreSQL connection pool (max={})", max_connections);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300))
            .connect(url)
            .await
            .map_err(|e| map_sqlx_connection_error(e, "PostgreSQL connection initialization"))?;

        pools.insert(url.to_string(), pool.clone());
        Ok(pool)
    }

    /// Obtain or create a pooled MySQL connection.
    pub async fn get_mysql_pool(&self, url: &str, max_connections: u32) -> Result<MySqlPool, HiveFlowError> {
        {
            let pools = self.mysql_pools.read().await;
            if let Some(pool) = pools.get(url) {
                return Ok(pool.clone());
            }
        }

        let mut pools = self.mysql_pools.write().await;
        if let Some(pool) = pools.get(url) {
            return Ok(pool.clone());
        }

        info!("Creating new MySQL connection pool (max={})", max_connections);
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300))
            .connect(url)
            .await
            .map_err(|e| map_sqlx_connection_error(e, "MySQL connection initialization"))?;

        pools.insert(url.to_string(), pool.clone());
        Ok(pool)
    }

    /// Test a connection string and return latency in milliseconds.
    pub async fn test_pg_connection(&self, url: &str) -> Result<u64, HiveFlowError> {
        let start = std::time::Instant::now();
        let pool = self.get_pg_pool(url, 2).await?;
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| map_sqlx_connection_error(e, "PostgreSQL ping"))?;
        let elapsed = start.elapsed().as_millis() as u64;
        debug!("PostgreSQL ping successful in {} ms", elapsed);
        Ok(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    #[test]
    fn test_error_mapping_connection_refused() {
        let io_err = Error::new(ErrorKind::ConnectionRefused, "connection refused");
        let sqlx_err = sqlx::Error::Io(io_err);
        let mapped = map_sqlx_connection_error(sqlx_err, "Target DB");
        match mapped {
            HiveFlowError::TargetConnectionRefused(msg) => {
                assert!(msg.contains("Target DB: connection refused"));
            }
            other => panic!("Expected TargetConnectionRefused, got {:?}", other),
        }
    }

    #[test]
    fn test_error_mapping_connection_timeout() {
        let io_err = Error::new(ErrorKind::TimedOut, "timed out");
        let sqlx_err = sqlx::Error::Io(io_err);
        let mapped = map_sqlx_connection_error(sqlx_err, "Source DB");
        match mapped {
            HiveFlowError::TargetConnectionTimeout(msg) => {
                assert!(msg.contains("Source DB: connection timed out"));
            }
            other => panic!("Expected TargetConnectionTimeout, got {:?}", other),
        }
    }

    #[test]
    fn test_error_mapping_pool_timeout() {
        let sqlx_err = sqlx::Error::PoolTimedOut;
        let mapped = map_sqlx_connection_error(sqlx_err, "Target Pool");
        match mapped {
            HiveFlowError::TargetConnectionTimeout(msg) => {
                assert!(msg.contains("Target Pool: pool timed out"));
            }
            other => panic!("Expected TargetConnectionTimeout, got {:?}", other),
        }
    }
}
