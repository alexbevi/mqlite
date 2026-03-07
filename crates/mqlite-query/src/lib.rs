mod capabilities;
mod error;
mod expression;
mod filter;
mod pipeline;
mod projection;
mod types;
mod update;

pub use capabilities::{
    SUPPORTED_AGGREGATION_ACCUMULATORS, SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS,
    SUPPORTED_AGGREGATION_STAGES, SUPPORTED_AGGREGATION_WINDOW_OPERATORS,
    SUPPORTED_QUERY_OPERATORS,
};
pub use error::QueryError;
pub use filter::{document_matches, document_matches_expression, parse_filter};
pub use pipeline::{CollectionResolver, run_pipeline, run_pipeline_with_resolver};
pub use projection::apply_projection;
pub use types::{BitTestMode, MatchExpr, TypeSet, UpdateModifier, UpdateSpec};
pub use update::{apply_update, parse_update, parse_update_value, upsert_seed_from_query};

#[cfg(test)]
mod tests;
