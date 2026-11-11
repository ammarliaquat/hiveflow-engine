//! Foundational domain enums, state machine phases, and type aliases.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Medallion architectural layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MedallionLayer {
    Bronze,
    Silver,
    Gold,
}

impl fmt::Display for MedallionLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bronze => write!(f, "BRONZE"),
            Self::Silver => write!(f, "SILVER"),
            Self::Gold => write!(f, "GOLD"),
        }
    }
}

/// Execution phase state machine.
/// RESOLVING_TARGET -> CONNECTING_TARGET -> TARGET_PREFLIGHT -> GOLD_TRANSFORM -> GOLD_WRITE -> COMPLETED
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionPhase {
    ResolvingTarget,
    ConnectingTarget,
    TargetPreflight,
    BronzeExtract,
    BronzeWrite,
    SilverTransform,
    SilverWrite,
    GoldTransform,
    GoldWrite,
    Reconciliation,
    Completed,
    Failed,
}

impl fmt::Display for ExecutionPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// CDC Strategy enum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CdcStrategy {
    TimestampHighWatermark,
    IdHighWatermark,
    FullOverwrite,
    AuditLogReconciliation,
}

/// Execution Authority Mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionAuthorityMode {
    Governed,
    Autonomous,
}
