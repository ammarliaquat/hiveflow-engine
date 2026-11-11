use thiserror::Error;

/// Machine-classified error taxonomy.
///
/// Every variant maps 1-to-1 with a persisted `metadata.gui_job_registry.error_code`.
/// Never collapse a connection error into a table-not-found error.
#[derive(Debug, Error)]
pub enum HiveFlowError {
    // ── Source / connection layer ────────────────────────────────────────────
    #[error("SOURCE_CONNECTION_ENDPOINT_RESOLUTION_FAILED: {0}")]
    SourceEndpointResolutionFailed(String),

    #[error("TARGET_CONNECTION_TIMEOUT: {0}")]
    TargetConnectionTimeout(String),

    #[error("TARGET_CONNECTION_REFUSED: {0}")]
    TargetConnectionRefused(String),

    #[error("TARGET_AUTHENTICATION_FAILED: {0}")]
    TargetAuthenticationFailed(String),

    #[error("TARGET_UNREACHABLE: {0}")]
    TargetUnreachable(String),

    // ── Preflight / schema layer ─────────────────────────────────────────────
    #[error("TARGET_PREFLIGHT_FAILED: {0}")]
    TargetPreflightFailed(String),

    #[error("TARGET_TABLE_NOT_FOUND: {0}")]
    TargetTableNotFound(String),

    // ── Metadata / governance layer ──────────────────────────────────────────
    #[error("TARGET_RESOLUTION_FAILED: {0}")]
    TargetResolutionFailed(String),

    #[error("SOURCE_BUSINESS_KEY_DUPLICATE: table={table}, key={key}")]
    SourceBusinessKeyDuplicate { table: String, key: String },

    // ── ETL execution layer ──────────────────────────────────────────────────
    #[error("GOLD_TRANSFORM_FAILED: {0}")]
    GoldTransformFailed(String),

    #[error("GOLD_WRITE_FAILED: {0}")]
    GoldWriteFailed(String),

    #[error("RECONCILIATION_FAILED: {0}")]
    ReconciliationFailed(String),

    // ── Security layer ───────────────────────────────────────────────────────
    #[error("AUTHENTICATION_FAILED: invalid credentials for user '{0}'")]
    AuthenticationFailed(String),

    #[error("AUTHORIZATION_DENIED: user '{user}' lacks permission '{permission}'")]
    AuthorizationDenied { user: String, permission: String },

    #[error("SELF_DROP_DENIED: admin cannot delete their own account")]
    SelfDropDenied,

    // ── Infrastructure ───────────────────────────────────────────────────────
    #[error("DATABASE_ERROR: {0}")]
    Database(#[from] sqlx::Error),

    #[error("ARROW_ERROR: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO_ERROR: {0}")]
    Io(#[from] std::io::Error),

    #[error("CONFIG_ERROR: {0}")]
    Config(String),

    #[error("SERIALIZATION_ERROR: {0}")]
    Serialization(#[from] serde_json::Error),
}
