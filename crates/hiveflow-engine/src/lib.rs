//! hiveflow-engine — Execution pipeline: Arrow RecordBatches, Parquet storage, and Medallion stages.

pub mod bronze;
pub mod silver;
pub mod gold;

pub use bronze::BronzeStage;
pub use silver::SilverStage;
pub use gold::GoldStage;
