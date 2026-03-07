use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("unsupported query operator `{0}`")]
    UnsupportedOperator(String),
    #[error("invalid query structure")]
    InvalidStructure,
    #[error("projection mixes include and exclude fields")]
    MixedProjection,
    #[error("invalid update document")]
    InvalidUpdate,
    #[error("unsupported aggregation stage `{0}`")]
    UnsupportedStage(String),
    #[error("invalid aggregation stage")]
    InvalidStage,
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    BsonObjectTooLarge(String),
    #[error("{0}")]
    ChangeStreamFatalError(String),
    #[error("expected numeric value")]
    ExpectedNumeric,
    #[error("aggregation expression did not evaluate to a document")]
    ExpectedDocument,
}
