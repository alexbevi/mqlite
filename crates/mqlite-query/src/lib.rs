use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path, lookup_path_owned, remove_path, set_path};
use regex::{Regex as RustRegex, RegexBuilder};
use thiserror::Error;

pub const SUPPORTED_QUERY_OPERATORS: &[&str] = &[
    "$and",
    "$or",
    "$nor",
    "$eq",
    "$ne",
    "$gt",
    "$gte",
    "$lt",
    "$lte",
    "$in",
    "$nin",
    "$exists",
    "$size",
    "$mod",
    "$all",
    "$not",
    "$type",
    "$regex",
    "$options",
    "$elemMatch",
];

pub const SUPPORTED_AGGREGATION_STAGES: &[&str] = &[
    "$match",
    "$project",
    "$set",
    "$addFields",
    "$unset",
    "$limit",
    "$skip",
    "$sort",
    "$count",
    "$unwind",
    "$group",
    "$replaceRoot",
];

pub const SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS: &[&str] = &["$literal"];

pub const SUPPORTED_AGGREGATION_ACCUMULATORS: &[&str] = &["$sum", "$first", "$push", "$avg"];

pub const SUPPORTED_AGGREGATION_WINDOW_OPERATORS: &[&str] = &[];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSet {
    pub all_numbers: bool,
    pub codes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchExpr {
    And(Vec<MatchExpr>),
    Or(Vec<MatchExpr>),
    Nor(Vec<MatchExpr>),
    Not(Box<MatchExpr>),
    Eq {
        path: String,
        value: Bson,
    },
    Ne {
        path: String,
        value: Bson,
    },
    Gt {
        path: String,
        value: Bson,
    },
    Gte {
        path: String,
        value: Bson,
    },
    Lt {
        path: String,
        value: Bson,
    },
    Lte {
        path: String,
        value: Bson,
    },
    In {
        path: String,
        values: Vec<Bson>,
    },
    Nin {
        path: String,
        values: Vec<Bson>,
    },
    All {
        path: String,
        values: Vec<Bson>,
    },
    Exists {
        path: String,
        exists: bool,
    },
    Type {
        path: String,
        type_set: TypeSet,
    },
    ElemMatch {
        path: String,
        spec: Document,
        value_case: bool,
    },
    Regex {
        path: String,
        pattern: String,
        options: String,
    },
    Size {
        path: String,
        size: usize,
    },
    Mod {
        path: String,
        divisor: i64,
        remainder: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateSpec {
    Replacement(Document),
    Modifiers(Vec<UpdateModifier>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateModifier {
    Set(String, Bson),
    Unset(String),
    Inc(String, Bson),
}

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
    #[error("expected numeric value")]
    ExpectedNumeric,
    #[error("aggregation expression did not evaluate to a document")]
    ExpectedDocument,
}

pub fn parse_filter(document: &Document) -> Result<MatchExpr, QueryError> {
    let mut expressions = Vec::new();

    for (key, value) in document {
        match key.as_str() {
            "$and" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| document.and_then(parse_filter))
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::And(parsed));
            }
            "$or" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| document.and_then(parse_filter))
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::Or(parsed));
            }
            "$nor" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| document.and_then(parse_filter))
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::Nor(parsed));
            }
            other if other.starts_with('$') => {
                return Err(QueryError::UnsupportedOperator(other.to_string()));
            }
            _ => expressions.push(parse_field_expression(key, value)?),
        }
    }

    Ok(match expressions.len() {
        0 => MatchExpr::And(Vec::new()),
        1 => expressions.remove(0),
        _ => MatchExpr::And(expressions),
    })
}

pub fn document_matches(document: &Document, filter: &Document) -> Result<bool, QueryError> {
    let expression = parse_filter(filter)?;
    Ok(matches_expression(document, &expression))
}

pub fn document_matches_expression(document: &Document, expression: &MatchExpr) -> bool {
    matches_expression(document, expression)
}

pub fn apply_projection(
    document: &Document,
    projection: Option<&Document>,
) -> Result<Document, QueryError> {
    let Some(projection) = projection else {
        return Ok(document.clone());
    };

    let mut include_mode = None;
    for (field, value) in projection {
        if field == "_id" {
            continue;
        }
        if let Some(flag) = projection_flag(value) {
            include_mode = match include_mode {
                None => Some(flag),
                Some(existing) if existing == flag => Some(existing),
                Some(_) => return Err(QueryError::MixedProjection),
            };
        } else {
            include_mode = Some(true);
        }
    }

    let include_mode = include_mode.unwrap_or_else(|| {
        projection
            .get("_id")
            .and_then(projection_flag)
            .unwrap_or(true)
    });

    if include_mode {
        let mut projected = Document::new();
        let include_id = projection
            .get("_id")
            .and_then(projection_flag)
            .unwrap_or(true);
        if include_id {
            if let Some(id) = document.get("_id") {
                projected.insert("_id", id.clone());
            }
        }

        for (field, value) in projection {
            if field == "_id" {
                continue;
            }

            match projection_flag(value) {
                Some(true) => {
                    if let Some(existing) = lookup_path(document, field) {
                        set_path(&mut projected, field, existing.clone())
                            .map_err(|_| QueryError::InvalidStructure)?;
                    }
                }
                Some(false) => {}
                None => {
                    projected.insert(field, eval_expression(document, value)?);
                }
            }
        }

        return Ok(projected);
    }

    let mut projected = document.clone();
    for (field, value) in projection {
        if projection_flag(value) == Some(false) {
            remove_path(&mut projected, field).map_err(|_| QueryError::InvalidStructure)?;
        }
    }
    Ok(projected)
}

pub fn parse_update(update: &Document) -> Result<UpdateSpec, QueryError> {
    let Some((first_key, _)) = update.iter().next() else {
        return Err(QueryError::InvalidUpdate);
    };

    if !first_key.starts_with('$') {
        return Ok(UpdateSpec::Replacement(update.clone()));
    }

    let mut modifiers = Vec::new();
    for (operator, spec) in update {
        let spec_document = spec.as_document().ok_or(QueryError::InvalidUpdate)?;
        match operator.as_str() {
            "$set" => {
                for (path, value) in spec_document {
                    modifiers.push(UpdateModifier::Set(path.clone(), value.clone()));
                }
            }
            "$unset" => {
                for (path, _) in spec_document {
                    modifiers.push(UpdateModifier::Unset(path.clone()));
                }
            }
            "$inc" => {
                for (path, value) in spec_document {
                    modifiers.push(UpdateModifier::Inc(path.clone(), value.clone()));
                }
            }
            other => return Err(QueryError::UnsupportedOperator(other.to_string())),
        }
    }

    Ok(UpdateSpec::Modifiers(modifiers))
}

pub fn apply_update(document: &mut Document, update: &UpdateSpec) -> Result<(), QueryError> {
    match update {
        UpdateSpec::Replacement(replacement) => {
            let id = document.get("_id").cloned();
            *document = replacement.clone();
            if let Some(id) = id {
                document.entry("_id".to_string()).or_insert(id);
            }
            Ok(())
        }
        UpdateSpec::Modifiers(modifiers) => {
            for modifier in modifiers {
                match modifier {
                    UpdateModifier::Set(path, value) => {
                        set_path(document, path, value.clone())
                            .map_err(|_| QueryError::InvalidStructure)?;
                    }
                    UpdateModifier::Unset(path) => {
                        remove_path(document, path).map_err(|_| QueryError::InvalidStructure)?;
                    }
                    UpdateModifier::Inc(path, value) => {
                        let current = lookup_path_owned(document, path).unwrap_or(Bson::Int32(0));
                        let next = increment_value(&current, value)?;
                        set_path(document, path, next).map_err(|_| QueryError::InvalidStructure)?;
                    }
                }
            }
            Ok(())
        }
    }
}

pub fn upsert_seed_from_query(filter: &Document) -> Document {
    let mut seed = Document::new();
    for (key, value) in filter {
        if key.starts_with('$') {
            continue;
        }

        match value {
            Bson::Document(document) if document.keys().all(|field| field.starts_with('$')) => {}
            _ => {
                let _ = set_path(&mut seed, key, value.clone());
            }
        }
    }
    seed
}

pub fn run_pipeline(
    documents: Vec<Document>,
    pipeline: &[Document],
) -> Result<Vec<Document>, QueryError> {
    let mut current = documents;

    for stage in pipeline {
        if stage.len() != 1 {
            return Err(QueryError::InvalidStage);
        }

        let (stage_name, stage_spec) = stage.iter().next().expect("single stage");
        current = match stage_name.as_str() {
            "$match" => {
                let filter = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|document| {
                        document_matches(&document, filter).map(|matches| (document, matches))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter_map(|(document, matches)| matches.then_some(document))
                    .collect()
            }
            "$project" => {
                let projection = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|document| apply_projection(&document, Some(projection)))
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$set" | "$addFields" => {
                let spec = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current
                    .into_iter()
                    .map(|mut document| {
                        for (field, expr) in spec {
                            let value = eval_expression(&document, expr)?;
                            set_path(&mut document, field, value)
                                .map_err(|_| QueryError::InvalidStructure)?;
                        }
                        Ok(document)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$unset" => {
                let fields = parse_unset(stage_spec)?;
                current
                    .into_iter()
                    .map(|mut document| {
                        for field in &fields {
                            remove_path(&mut document, field)
                                .map_err(|_| QueryError::InvalidStructure)?;
                        }
                        Ok(document)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "$limit" => {
                let limit = integer_value(stage_spec).ok_or(QueryError::InvalidStage)?;
                current.into_iter().take(limit.max(0) as usize).collect()
            }
            "$skip" => {
                let skip = integer_value(stage_spec).ok_or(QueryError::InvalidStage)?;
                current.into_iter().skip(skip.max(0) as usize).collect()
            }
            "$sort" => {
                let sort = stage_spec.as_document().ok_or(QueryError::InvalidStage)?;
                current.sort_by(|left, right| compare_documents_by_sort(left, right, sort));
                current
            }
            "$count" => {
                let field = stage_spec.as_str().ok_or(QueryError::InvalidStage)?;
                let mut counted = Document::new();
                counted.insert(field, current.len() as i64);
                vec![counted]
            }
            "$unwind" => unwind_documents(current, stage_spec)?,
            "$group" => group_documents(
                current,
                stage_spec.as_document().ok_or(QueryError::InvalidStage)?,
            )?,
            "$replaceRoot" => replace_root(
                current,
                stage_spec.as_document().ok_or(QueryError::InvalidStage)?,
            )?,
            other => return Err(QueryError::UnsupportedStage(other.to_string())),
        };
    }

    Ok(current)
}

fn matches_expression(document: &Document, expression: &MatchExpr) -> bool {
    match expression {
        MatchExpr::And(items) => items.iter().all(|item| matches_expression(document, item)),
        MatchExpr::Or(items) => items.iter().any(|item| matches_expression(document, item)),
        MatchExpr::Nor(items) => items.iter().all(|item| !matches_expression(document, item)),
        MatchExpr::Not(expression) => !matches_expression(document, expression),
        MatchExpr::Eq { path, value } => path_candidates(document, path)
            .into_iter()
            .any(|existing| matches_equality(existing, value)),
        MatchExpr::Ne { path, value } => path_candidates(document, path)
            .into_iter()
            .all(|existing| !matches_equality(existing, value)),
        MatchExpr::Gt { path, value } => path_candidates(document, path)
            .into_iter()
            .any(|existing| matches_comparison(existing, value, |ordering| ordering.is_gt())),
        MatchExpr::Gte { path, value } => {
            path_candidates(document, path).into_iter().any(|existing| {
                matches_comparison(existing, value, |ordering| {
                    matches!(
                        ordering,
                        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
                    )
                })
            })
        }
        MatchExpr::Lt { path, value } => path_candidates(document, path)
            .into_iter()
            .any(|existing| matches_comparison(existing, value, |ordering| ordering.is_lt())),
        MatchExpr::Lte { path, value } => {
            path_candidates(document, path).into_iter().any(|existing| {
                matches_comparison(existing, value, |ordering| {
                    matches!(
                        ordering,
                        std::cmp::Ordering::Equal | std::cmp::Ordering::Less
                    )
                })
            })
        }
        MatchExpr::In { path, values } => path_candidates(document, path)
            .into_iter()
            .any(|existing| values.iter().any(|value| matches_equality(existing, value))),
        MatchExpr::Nin { path, values } => {
            path_candidates(document, path).into_iter().all(|existing| {
                values
                    .iter()
                    .all(|value| !matches_equality(existing, value))
            })
        }
        MatchExpr::All { path, values } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_all(value, values)),
        MatchExpr::Exists { path, exists } => path_candidates(document, path).is_empty() != *exists,
        MatchExpr::Type { path, type_set } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_type(value, type_set)),
        MatchExpr::ElemMatch {
            path,
            spec,
            value_case,
        } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_elem_match(value, spec, *value_case)),
        MatchExpr::Regex {
            path,
            pattern,
            options,
        } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_regex(value, pattern, options)),
        MatchExpr::Size { path, size } => path_candidates(document, path)
            .into_iter()
            .any(|value| value.as_array().is_some_and(|values| values.len() == *size)),
        MatchExpr::Mod {
            path,
            divisor,
            remainder,
        } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_mod(value, *divisor, *remainder)),
    }
}

fn parse_field_expression(path: &str, value: &Bson) -> Result<MatchExpr, QueryError> {
    match value {
        Bson::RegularExpression(regex) => {
            let _ = compile_regex(&regex.pattern, &regex.options)?;
            Ok(MatchExpr::Regex {
                path: path.to_string(),
                pattern: regex.pattern.clone(),
                options: regex.options.clone(),
            })
        }
        Bson::Document(document) if document.keys().all(|key| key.starts_with('$')) => {
            let mut expressions = Vec::new();
            if document.contains_key("$regex") || document.contains_key("$options") {
                expressions.push(parse_regex_expression(path, document)?);
            }
            for (operator, operator_value) in document {
                expressions.push(match operator.as_str() {
                    "$regex" | "$options" => continue,
                    "$eq" => MatchExpr::Eq {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$ne" => MatchExpr::Ne {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$gt" => MatchExpr::Gt {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$gte" => MatchExpr::Gte {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$lt" => MatchExpr::Lt {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$lte" => MatchExpr::Lte {
                        path: path.to_string(),
                        value: operator_value.clone(),
                    },
                    "$in" => MatchExpr::In {
                        path: path.to_string(),
                        values: operator_value
                            .as_array()
                            .ok_or(QueryError::InvalidStructure)?
                            .clone(),
                    },
                    "$nin" => MatchExpr::Nin {
                        path: path.to_string(),
                        values: operator_value
                            .as_array()
                            .ok_or(QueryError::InvalidStructure)?
                            .clone(),
                    },
                    "$all" => {
                        let values = operator_value
                            .as_array()
                            .ok_or(QueryError::InvalidStructure)?
                            .clone();
                        if values.iter().any(all_value_uses_expression) {
                            return Err(QueryError::InvalidStructure);
                        }
                        MatchExpr::All {
                            path: path.to_string(),
                            values,
                        }
                    }
                    "$not" => MatchExpr::Not(Box::new(parse_not_expression(path, operator_value)?)),
                    "$exists" => MatchExpr::Exists {
                        path: path.to_string(),
                        exists: operator_value
                            .as_bool()
                            .ok_or(QueryError::InvalidStructure)?,
                    },
                    "$type" => MatchExpr::Type {
                        path: path.to_string(),
                        type_set: parse_type_set(operator_value)?,
                    },
                    "$elemMatch" => {
                        let spec = operator_value
                            .as_document()
                            .ok_or(QueryError::InvalidStructure)?
                            .clone();
                        let value_case = is_elem_match_value_case(&spec);
                        validate_elem_match_spec(&spec, value_case)?;
                        MatchExpr::ElemMatch {
                            path: path.to_string(),
                            spec,
                            value_case,
                        }
                    }
                    "$size" => MatchExpr::Size {
                        path: path.to_string(),
                        size: usize::try_from(
                            integer_value(operator_value)
                                .filter(|value| *value >= 0)
                                .ok_or(QueryError::InvalidStructure)?,
                        )
                        .map_err(|_| QueryError::InvalidStructure)?,
                    },
                    "$mod" => {
                        let values = operator_value
                            .as_array()
                            .ok_or(QueryError::InvalidStructure)?;
                        if values.len() != 2 {
                            return Err(QueryError::InvalidStructure);
                        }
                        let divisor =
                            coerce_to_i64(&values[0]).ok_or(QueryError::InvalidStructure)?;
                        let remainder =
                            coerce_to_i64(&values[1]).ok_or(QueryError::InvalidStructure)?;
                        if divisor == 0 {
                            return Err(QueryError::InvalidStructure);
                        }
                        MatchExpr::Mod {
                            path: path.to_string(),
                            divisor,
                            remainder,
                        }
                    }
                    other => return Err(QueryError::UnsupportedOperator(other.to_string())),
                });
            }

            Ok(match expressions.len() {
                0 => MatchExpr::And(Vec::new()),
                1 => expressions.remove(0),
                _ => MatchExpr::And(expressions),
            })
        }
        _ => Ok(MatchExpr::Eq {
            path: path.to_string(),
            value: value.clone(),
        }),
    }
}

fn path_candidates<'a>(document: &'a Document, path: &str) -> Vec<&'a Bson> {
    let segments = path.split('.').collect::<Vec<_>>();
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        return Vec::new();
    }
    path_candidates_in_document(document, &segments)
}

fn path_candidates_in_document<'a>(document: &'a Document, segments: &[&str]) -> Vec<&'a Bson> {
    let Some((first, rest)) = segments.split_first() else {
        return Vec::new();
    };
    document
        .get(*first)
        .into_iter()
        .flat_map(|value| path_candidates_in_value(value, rest))
        .collect()
}

fn path_candidates_in_value<'a>(value: &'a Bson, segments: &[&str]) -> Vec<&'a Bson> {
    if segments.is_empty() {
        return vec![value];
    }

    match value {
        Bson::Document(document) => path_candidates_in_document(document, segments),
        Bson::Array(items) => items
            .iter()
            .flat_map(|item| path_candidates_in_value(item, segments))
            .collect(),
        _ => Vec::new(),
    }
}

fn matches_equality(existing: &Bson, value: &Bson) -> bool {
    match existing {
        Bson::Array(items) => {
            compare_bson(existing, value).is_eq()
                || items.iter().any(|item| matches_equality(item, value))
        }
        _ => compare_bson(existing, value).is_eq(),
    }
}

fn matches_comparison(
    existing: &Bson,
    value: &Bson,
    predicate: impl Fn(std::cmp::Ordering) -> bool + Copy,
) -> bool {
    match existing {
        Bson::Array(items) => items
            .iter()
            .any(|item| matches_comparison(item, value, predicate)),
        _ => predicate(compare_bson(existing, value)),
    }
}

fn projection_flag(value: &Bson) -> Option<bool> {
    match value {
        Bson::Boolean(value) => Some(*value),
        Bson::Int32(value) => Some(*value != 0),
        Bson::Int64(value) => Some(*value != 0),
        _ => None,
    }
}

fn parse_not_expression(path: &str, value: &Bson) -> Result<MatchExpr, QueryError> {
    match value {
        Bson::RegularExpression(regex) => {
            let _ = compile_regex(&regex.pattern, &regex.options)?;
            Ok(MatchExpr::Regex {
                path: path.to_string(),
                pattern: regex.pattern.clone(),
                options: regex.options.clone(),
            })
        }
        Bson::Document(document) if document.keys().all(|key| key.starts_with('$')) => {
            if document.is_empty() {
                return Err(QueryError::InvalidStructure);
            }
            parse_field_expression(path, value)
        }
        _ => Err(QueryError::InvalidStructure),
    }
}

fn is_elem_match_value_case(spec: &Document) -> bool {
    let Some((first, _)) = spec.iter().next() else {
        return false;
    };
    spec.keys().all(|key| key.starts_with('$'))
        && !matches!(first.as_str(), "$and" | "$or" | "$nor" | "$expr")
}

fn validate_elem_match_spec(spec: &Document, value_case: bool) -> Result<(), QueryError> {
    if value_case {
        let mut filter = Document::new();
        filter.insert("_elem", Bson::Document(spec.clone()));
        parse_filter(&filter).map(|_| ())
    } else {
        parse_filter(spec).map(|_| ())
    }
}

fn parse_regex_expression(path: &str, document: &Document) -> Result<MatchExpr, QueryError> {
    let regex_value = document.get("$regex");
    let options_value = document.get("$options");
    let Some(regex_value) = regex_value else {
        return Err(QueryError::InvalidStructure);
    };

    let (pattern, mut options) = match regex_value {
        Bson::String(pattern) => (pattern.clone(), String::new()),
        Bson::RegularExpression(regex) => (regex.pattern.clone(), regex.options.clone()),
        _ => return Err(QueryError::InvalidStructure),
    };

    if let Some(options_value) = options_value {
        let extra = options_value.as_str().ok_or(QueryError::InvalidStructure)?;
        if !options.is_empty() {
            return Err(QueryError::InvalidStructure);
        }
        options = extra.to_string();
    }

    let _ = compile_regex(&pattern, &options)?;
    Ok(MatchExpr::Regex {
        path: path.to_string(),
        pattern,
        options,
    })
}

fn parse_type_set(value: &Bson) -> Result<TypeSet, QueryError> {
    let mut type_set = TypeSet {
        all_numbers: false,
        codes: Vec::new(),
    };

    match value {
        Bson::Array(values) => {
            for item in values {
                add_type_spec(&mut type_set, item)?;
            }
        }
        _ => add_type_spec(&mut type_set, value)?,
    }

    type_set.codes.sort_unstable();
    type_set.codes.dedup();
    Ok(type_set)
}

fn add_type_spec(type_set: &mut TypeSet, value: &Bson) -> Result<(), QueryError> {
    if let Some(alias) = value.as_str() {
        if alias == "number" {
            type_set.all_numbers = true;
            return Ok(());
        }

        let code = type_alias_code(alias).ok_or(QueryError::InvalidStructure)?;
        type_set.codes.push(code);
        return Ok(());
    }

    let code = parse_type_code(value).ok_or(QueryError::InvalidStructure)?;
    if code == 0 {
        return Err(QueryError::InvalidStructure);
    }
    type_set.codes.push(code);
    Ok(())
}

fn increment_value(current: &Bson, increment: &Bson) -> Result<Bson, QueryError> {
    let current_number = numeric_value(current)?;
    let increment_number = numeric_value(increment)?;
    let sum = current_number + increment_number;
    Ok(number_bson(sum))
}

fn numeric_value(value: &Bson) -> Result<f64, QueryError> {
    match value {
        Bson::Int32(value) => Ok(*value as f64),
        Bson::Int64(value) => Ok(*value as f64),
        Bson::Double(value) => Ok(*value),
        _ => Err(QueryError::ExpectedNumeric),
    }
}

fn integer_value(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i64),
        _ => None,
    }
}

fn coerce_to_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.is_finite() => truncate_f64_to_i64(*value),
        Bson::Decimal128(value) => truncate_f64_to_i64(value.to_string().parse::<f64>().ok()?),
        _ => None,
    }
}

fn truncate_f64_to_i64(value: f64) -> Option<i64> {
    let truncated = value.trunc();
    ((i64::MIN as f64)..=(i64::MAX as f64))
        .contains(&truncated)
        .then_some(truncated as i64)
}

fn matches_mod(value: &Bson, divisor: i64, remainder: i64) -> bool {
    match value {
        Bson::Array(items) => items
            .iter()
            .any(|item| matches_mod(item, divisor, remainder)),
        _ => coerce_to_i64(value).is_some_and(|coerced| coerced % divisor == remainder),
    }
}

fn all_value_uses_expression(value: &Bson) -> bool {
    value
        .as_document()
        .and_then(|document| document.iter().next())
        .is_some_and(|(field, _)| field.starts_with('$'))
}

fn matches_all(value: &Bson, expected_values: &[Bson]) -> bool {
    let values = dedup_values(expected_values);
    !values.is_empty()
        && values
            .iter()
            .all(|expected| matches_all_term(value, expected))
}

fn matches_all_term(value: &Bson, expected: &Bson) -> bool {
    match value {
        Bson::Array(items) => {
            compare_bson(value, expected).is_eq()
                || items
                    .iter()
                    .any(|item| compare_bson(item, expected).is_eq())
        }
        _ => compare_bson(value, expected).is_eq(),
    }
}

fn dedup_values(values: &[Bson]) -> Vec<Bson> {
    let mut unique = values.to_vec();
    unique.sort_by(compare_bson);
    unique.dedup_by(|left, right| compare_bson(left, right).is_eq());
    unique
}

fn matches_type(value: &Bson, type_set: &TypeSet) -> bool {
    (type_set.all_numbers && is_numeric_bson(value))
        || type_set
            .codes
            .iter()
            .any(|code| bson_type_code(value) == *code)
        || match value {
            Bson::Array(items) => items.iter().any(|item| matches_type(item, type_set)),
            _ => false,
        }
}

fn matches_elem_match(value: &Bson, spec: &Document, value_case: bool) -> bool {
    let Some(items) = value.as_array() else {
        return false;
    };

    items.iter().any(|item| {
        if value_case {
            let mut document = Document::new();
            document.insert("_elem", item.clone());
            let mut filter = Document::new();
            filter.insert("_elem", Bson::Document(spec.clone()));
            return document_matches(&document, &filter).unwrap_or(false);
        }

        match item {
            Bson::Document(document) => document_matches(document, spec).unwrap_or(false),
            Bson::Array(items) => {
                let document = array_as_document(items);
                document_matches(&document, spec).unwrap_or(false)
            }
            _ => false,
        }
    })
}

fn array_as_document(items: &[Bson]) -> Document {
    let mut document = Document::new();
    for (index, item) in items.iter().enumerate() {
        document.insert(index.to_string(), item.clone());
    }
    document
}

fn matches_regex(value: &Bson, pattern: &str, options: &str) -> bool {
    let Ok(regex) = compile_regex(pattern, options) else {
        return false;
    };
    matches_regex_compiled(value, &regex)
}

fn matches_regex_compiled(value: &Bson, regex: &RustRegex) -> bool {
    match value {
        Bson::String(value) | Bson::Symbol(value) => regex.is_match(value),
        Bson::Array(items) => items.iter().any(|item| matches_regex_compiled(item, regex)),
        _ => false,
    }
}

fn compile_regex(pattern: &str, options: &str) -> Result<RustRegex, QueryError> {
    let mut builder = RegexBuilder::new(pattern);
    for option in options.chars() {
        match option {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            'u' => {}
            _ => return Err(QueryError::InvalidStructure),
        }
    }
    builder.build().map_err(|_| QueryError::InvalidStructure)
}

fn type_alias_code(alias: &str) -> Option<i32> {
    match alias {
        "double" => Some(1),
        "string" => Some(2),
        "object" => Some(3),
        "array" => Some(4),
        "binData" => Some(5),
        "undefined" => Some(6),
        "objectId" => Some(7),
        "bool" | "boolean" => Some(8),
        "date" => Some(9),
        "null" => Some(10),
        "regex" => Some(11),
        "dbPointer" => Some(12),
        "javascript" => Some(13),
        "symbol" => Some(14),
        "javascriptWithScope" => Some(15),
        "int" => Some(16),
        "timestamp" => Some(17),
        "long" => Some(18),
        "decimal" => Some(19),
        "minKey" => Some(-1),
        "maxKey" => Some(127),
        _ => None,
    }
}

fn parse_type_code(value: &Bson) -> Option<i32> {
    let code = match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.is_finite() && value.fract() == 0.0 => Some(*value as i64),
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            (parsed.is_finite() && parsed.fract() == 0.0).then_some(parsed as i64)
        }
        _ => None,
    }?;
    i32::try_from(code).ok()
}

fn bson_type_code(value: &Bson) -> i32 {
    match value {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

fn is_numeric_bson(value: &Bson) -> bool {
    matches!(
        value,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

fn number_bson(value: f64) -> Bson {
    if value.fract() == 0.0 {
        Bson::Int64(value as i64)
    } else {
        Bson::Double(value)
    }
}

fn eval_expression(document: &Document, expression: &Bson) -> Result<Bson, QueryError> {
    match expression {
        Bson::String(path) if path.starts_with('$') => {
            Ok(lookup_path_owned(document, &path[1..]).unwrap_or(Bson::Null))
        }
        Bson::Document(spec) if spec.len() == 1 && spec.contains_key("$literal") => {
            Ok(spec.get("$literal").cloned().unwrap_or(Bson::Null))
        }
        Bson::Document(spec) if spec.len() == 1 => {
            let (field, _) = spec.iter().next().expect("single field");
            if field.starts_with('$') {
                return Err(QueryError::UnsupportedOperator(field.to_string()));
            }

            let mut evaluated = Document::new();
            for (field, value) in spec {
                evaluated.insert(field, eval_expression(document, value)?);
            }
            Ok(Bson::Document(evaluated))
        }
        Bson::Document(spec) => {
            let mut evaluated = Document::new();
            for (field, value) in spec {
                evaluated.insert(field, eval_expression(document, value)?);
            }
            Ok(Bson::Document(evaluated))
        }
        Bson::Array(items) => Ok(Bson::Array(
            items
                .iter()
                .map(|item| eval_expression(document, item))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Ok(expression.clone()),
    }
}

fn parse_unset(spec: &Bson) -> Result<Vec<String>, QueryError> {
    match spec {
        Bson::String(value) => Ok(vec![value.clone()]),
        Bson::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or(QueryError::InvalidStage)
            })
            .collect(),
        _ => Err(QueryError::InvalidStage),
    }
}

fn compare_documents_by_sort(
    left: &Document,
    right: &Document,
    sort: &Document,
) -> std::cmp::Ordering {
    for (field, direction) in sort {
        let left_value = lookup_path(left, field).unwrap_or(&Bson::Null);
        let right_value = lookup_path(right, field).unwrap_or(&Bson::Null);
        let mut ordering = compare_bson(left_value, right_value);
        if integer_value(direction).unwrap_or(1) < 0 {
            ordering = ordering.reverse();
        }
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn unwind_documents(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let (path, preserve) = match spec {
        Bson::String(path) => (path.trim_start_matches('$').to_string(), false),
        Bson::Document(spec) => (
            spec.get_str("path")
                .map_err(|_| QueryError::InvalidStage)?
                .trim_start_matches('$')
                .to_string(),
            spec.get_bool("preserveNullAndEmptyArrays").unwrap_or(false),
        ),
        _ => return Err(QueryError::InvalidStage),
    };

    let mut unwound = Vec::new();
    for document in documents {
        match lookup_path(&document, &path) {
            Some(Bson::Array(items)) if !items.is_empty() => {
                for item in items {
                    let mut clone = document.clone();
                    set_path(&mut clone, &path, item.clone())
                        .map_err(|_| QueryError::InvalidStructure)?;
                    unwound.push(clone);
                }
            }
            Some(Bson::Array(_)) if preserve => {
                let mut clone = document;
                remove_path(&mut clone, &path).map_err(|_| QueryError::InvalidStructure)?;
                unwound.push(clone);
            }
            Some(_) if preserve => unwound.push(document),
            None if preserve => unwound.push(document),
            _ => {}
        }
    }
    Ok(unwound)
}

fn group_documents(documents: Vec<Document>, spec: &Document) -> Result<Vec<Document>, QueryError> {
    let id_expression = spec.get("_id").cloned().unwrap_or(Bson::Null);
    let accumulator_specs = spec
        .iter()
        .filter(|(field, _)| field.as_str() != "_id")
        .map(|(field, value)| Ok((field.clone(), parse_accumulator(value)?)))
        .collect::<Result<Vec<_>, QueryError>>()?;

    let mut groups: BTreeMap<Vec<u8>, (Bson, BTreeMap<String, GroupAccumulatorState>)> =
        BTreeMap::new();

    for document in documents {
        let group_key = eval_expression(&document, &id_expression)?;
        let group_id = bson::to_vec(&doc! { "_id": group_key.clone() })
            .map_err(|_| QueryError::InvalidStage)?;
        let entry = groups.entry(group_id).or_insert_with(|| {
            let states = accumulator_specs
                .iter()
                .map(|(field, spec)| (field.clone(), GroupAccumulatorState::from_spec(spec)))
                .collect::<BTreeMap<_, _>>();
            (group_key.clone(), states)
        });

        for (field, accumulator) in &accumulator_specs {
            let state = entry.1.get_mut(field).expect("state exists");
            state.apply(&document, accumulator)?;
        }
    }

    groups
        .into_values()
        .map(|(group_key, states)| {
            let mut output = Document::new();
            output.insert("_id", group_key);
            for (field, state) in states {
                output.insert(field, state.finish());
            }
            Ok(output)
        })
        .collect()
}

fn replace_root(documents: Vec<Document>, spec: &Document) -> Result<Vec<Document>, QueryError> {
    let new_root = spec.get("newRoot").ok_or(QueryError::InvalidStage)?;
    documents
        .into_iter()
        .map(|document| match eval_expression(&document, new_root)? {
            Bson::Document(document) => Ok(document),
            _ => Err(QueryError::ExpectedDocument),
        })
        .collect()
}

fn parse_accumulator(value: &Bson) -> Result<GroupAccumulatorSpec, QueryError> {
    let document = value.as_document().ok_or(QueryError::InvalidStage)?;
    if document.len() != 1 {
        return Err(QueryError::InvalidStage);
    }
    let (name, expression) = document.iter().next().expect("single accumulator");
    match name.as_str() {
        "$sum" => Ok(GroupAccumulatorSpec::Sum(expression.clone())),
        "$first" => Ok(GroupAccumulatorSpec::First(expression.clone())),
        "$push" => Ok(GroupAccumulatorSpec::Push(expression.clone())),
        "$avg" => Ok(GroupAccumulatorSpec::Avg(expression.clone())),
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

fn as_document(value: &Bson) -> Result<&Document, QueryError> {
    value.as_document().ok_or(QueryError::InvalidStructure)
}

enum GroupAccumulatorSpec {
    Sum(Bson),
    First(Bson),
    Push(Bson),
    Avg(Bson),
}

enum GroupAccumulatorState {
    Sum(f64),
    First(Option<Bson>),
    Push(Vec<Bson>),
    Avg { sum: f64, count: u64 },
}

impl GroupAccumulatorState {
    fn from_spec(spec: &GroupAccumulatorSpec) -> Self {
        match spec {
            GroupAccumulatorSpec::Sum(_) => Self::Sum(0.0),
            GroupAccumulatorSpec::First(_) => Self::First(None),
            GroupAccumulatorSpec::Push(_) => Self::Push(Vec::new()),
            GroupAccumulatorSpec::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn apply(
        &mut self,
        document: &Document,
        spec: &GroupAccumulatorSpec,
    ) -> Result<(), QueryError> {
        match (self, spec) {
            (Self::Sum(total), GroupAccumulatorSpec::Sum(expression)) => {
                *total += numeric_value(&eval_expression(document, expression)?)?;
                Ok(())
            }
            (Self::First(current), GroupAccumulatorSpec::First(expression)) => {
                if current.is_none() {
                    *current = Some(eval_expression(document, expression)?);
                }
                Ok(())
            }
            (Self::Push(values), GroupAccumulatorSpec::Push(expression)) => {
                values.push(eval_expression(document, expression)?);
                Ok(())
            }
            (Self::Avg { sum, count }, GroupAccumulatorSpec::Avg(expression)) => {
                *sum += numeric_value(&eval_expression(document, expression)?)?;
                *count += 1;
                Ok(())
            }
            _ => Err(QueryError::InvalidStage),
        }
    }

    fn finish(self) -> Bson {
        match self {
            Self::Sum(total) => number_bson(total),
            Self::First(value) => value.unwrap_or(Bson::Null),
            Self::Push(values) => Bson::Array(values),
            Self::Avg { sum, count } => {
                if count == 0 {
                    Bson::Null
                } else {
                    Bson::Double(sum / count as f64)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bson::{Bson, DateTime, Decimal128, Document, Timestamp, doc, oid::ObjectId};
    use pretty_assertions::assert_eq;

    use super::{
        QueryError, apply_projection, apply_update, document_matches, parse_update, run_pipeline,
    };

    // These cases are grounded in MongoDB matcher and pipeline tests such as
    // expression_leaf_test.cpp, expression_tree_test.cpp, document_source_project_test.cpp,
    // document_source_add_fields_test.cpp, document_source_unwind_test.cpp,
    // document_source_group_test.cpp, document_source_replace_root_test.cpp, and
    // document_source_sort_test.cpp.

    fn run_pipeline_ok(documents: Vec<Document>, pipeline: &[Document]) -> Vec<Document> {
        run_pipeline(documents, pipeline).expect("pipeline")
    }

    fn assert_filter(document: &Document, filter: Document, expected: bool) {
        assert_eq!(
            document_matches(document, &filter).expect("match"),
            expected,
            "filter: {:?}",
            filter
        );
    }

    #[test]
    fn matches_basic_filters() {
        let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };
        assert!(document_matches(&document, &doc! { "sku": "abc" }).expect("match"));
        assert!(document_matches(&document, &doc! { "qty": { "$gte": 3 } }).expect("match"));
        assert!(
            document_matches(
                &document,
                &doc! { "$or": [ { "qty": { "$gt": 9 } }, { "meta.enabled": true } ] }
            )
            .expect("match")
        );
    }

    #[test]
    fn supports_all_comparison_query_operators() {
        let document = doc! { "sku": "abc", "qty": 5, "meta": { "score": 9 } };

        for (filter, expected) in [
            (doc! { "sku": "abc" }, true),
            (doc! { "qty": { "$eq": 5 } }, true),
            (doc! { "qty": { "$eq": 4 } }, false),
            (doc! { "qty": { "$ne": 4 } }, true),
            (doc! { "qty": { "$ne": 5 } }, false),
            (doc! { "qty": { "$gt": 4 } }, true),
            (doc! { "qty": { "$gt": 5 } }, false),
            (doc! { "qty": { "$gte": 5 } }, true),
            (doc! { "qty": { "$gte": 6 } }, false),
            (doc! { "qty": { "$lt": 6 } }, true),
            (doc! { "qty": { "$lt": 5 } }, false),
            (doc! { "meta.score": { "$lte": 9 } }, true),
            (doc! { "meta.score": { "$lte": 8 } }, false),
        ] {
            assert_filter(&document, filter, expected);
        }
    }

    #[test]
    fn supports_in_exists_and_implicit_and_filters() {
        let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

        assert_filter(
            &document,
            doc! { "sku": { "$in": ["def", "abc"] }, "qty": { "$gte": 5, "$lte": 5 } },
            true,
        );
        assert_filter(&document, doc! { "sku": { "$in": ["def"] } }, false);
        assert_filter(
            &document,
            doc! { "meta.enabled": { "$exists": true } },
            true,
        );
        assert_filter(
            &document,
            doc! { "meta.enabled": { "$exists": false } },
            false,
        );
        assert_filter(
            &document,
            doc! { "meta.missing": { "$exists": false } },
            true,
        );
        assert_filter(
            &document,
            doc! { "meta.missing": { "$exists": true } },
            false,
        );
    }

    #[test]
    fn supports_nin_filters() {
        let document = doc! { "sku": "abc", "qty": 5 };

        assert_filter(&document, doc! { "sku": { "$nin": ["def", "ghi"] } }, true);
        assert_filter(&document, doc! { "sku": { "$nin": ["abc", "ghi"] } }, false);
        assert_filter(&document, doc! { "missing": { "$nin": ["abc"] } }, true);
    }

    #[test]
    fn supports_size_filters() {
        let document = doc! { "tags": ["red", "blue"], "meta": { "values": [1] } };

        assert_filter(&document, doc! { "tags": { "$size": 2 } }, true);
        assert_filter(&document, doc! { "tags": { "$size": 1 } }, false);
        assert_filter(&document, doc! { "meta.values": { "$size": 1 } }, true);
        assert_filter(&document, doc! { "missing": { "$size": 0 } }, false);
    }

    #[test]
    fn supports_mod_filters() {
        let document = doc! { "qty": 12, "score": 4.7, "values": [1, 8] };

        assert_filter(&document, doc! { "qty": { "$mod": [5, 2] } }, true);
        assert_filter(&document, doc! { "qty": { "$mod": [5, 1] } }, false);
        assert_filter(&document, doc! { "score": { "$mod": [4, 0] } }, true);
        assert_filter(&document, doc! { "values": { "$mod": [4, 0] } }, true);
        assert_filter(&document, doc! { "missing": { "$mod": [4, 0] } }, false);
    }

    #[test]
    fn rejects_malformed_mod_filters() {
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": [5] } }),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": [0, 0] } }),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": ["x", 0] } }),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_all_filters() {
        let document = doc! { "tags": ["red", "blue", "green"], "status": "red" };

        assert_filter(
            &document,
            doc! { "tags": { "$all": ["red", "blue"] } },
            true,
        );
        assert_filter(
            &document,
            doc! { "tags": { "$all": ["red", "missing"] } },
            false,
        );
        assert_filter(
            &document,
            doc! { "tags": { "$all": ["blue", "blue"] } },
            true,
        );
        assert_filter(&document, doc! { "tags": { "$all": [] } }, false);
        assert_filter(&document, doc! { "status": { "$all": ["red"] } }, true);
    }

    #[test]
    fn rejects_expression_values_inside_all_filters() {
        assert!(matches!(
            document_matches(
                &doc! { "tags": ["red"] },
                &doc! { "tags": { "$all": [{ "$elemMatch": { "$eq": "red" } }] } }
            ),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_not_filters() {
        let document = doc! { "qty": 12, "tags": ["red", "blue"] };

        assert_filter(
            &document,
            doc! { "qty": { "$not": { "$mod": [5, 1] } } },
            true,
        );
        assert_filter(&document, doc! { "qty": { "$not": { "$gt": 5 } } }, false);
        assert_filter(
            &document,
            doc! { "tags": { "$not": { "$all": ["red", "green"] } } },
            true,
        );
    }

    #[test]
    fn rejects_malformed_not_filters() {
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$not": 5 } }),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_type_filters() {
        let document = doc! {
            "name": "Ada",
            "count": 5_i32,
            "big": Bson::Int64(7),
            "price": 4.5,
            "meta": { "enabled": true },
            "tags": ["red", "blue"],
            "when": DateTime::now(),
            "id": ObjectId::new(),
            "stamp": Timestamp { time: 1, increment: 2 },
            "dec": Bson::Decimal128(Decimal128::from_str("1.5").expect("decimal")),
        };

        assert_filter(&document, doc! { "name": { "$type": "string" } }, true);
        assert_filter(&document, doc! { "count": { "$type": "int" } }, true);
        assert_filter(&document, doc! { "big": { "$type": "long" } }, true);
        assert_filter(&document, doc! { "price": { "$type": "double" } }, true);
        assert_filter(&document, doc! { "price": { "$type": "number" } }, true);
        assert_filter(&document, doc! { "meta": { "$type": "object" } }, true);
        assert_filter(&document, doc! { "tags": { "$type": "array" } }, true);
        assert_filter(&document, doc! { "when": { "$type": "date" } }, true);
        assert_filter(&document, doc! { "id": { "$type": "objectId" } }, true);
        assert_filter(&document, doc! { "stamp": { "$type": "timestamp" } }, true);
        assert_filter(&document, doc! { "dec": { "$type": "decimal" } }, true);
        assert_filter(
            &document,
            doc! { "name": { "$type": ["int", "string"] } },
            true,
        );
        assert_filter(&document, doc! { "missing": { "$type": "string" } }, false);
    }

    #[test]
    fn rejects_malformed_type_filters() {
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$type": "missing" } }),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$type": 0 } }),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_regex_filters() {
        let document = doc! { "name": "Ada", "tags": ["beta", "Gamma"] };

        assert_filter(&document, doc! { "name": { "$regex": "^A" } }, true);
        assert_filter(
            &document,
            doc! { "name": { "$regex": "^a", "$options": "i" } },
            true,
        );
        assert_filter(
            &document,
            doc! { "tags": { "$regex": "^g", "$options": "i" } },
            true,
        );
        assert!(
            document_matches(
                &document,
                &doc! {
                    "name": Bson::RegularExpression(bson::Regex {
                        pattern: "^A".to_string(),
                        options: "".to_string(),
                    })
                }
            )
            .expect("match")
        );
    }

    #[test]
    fn rejects_malformed_regex_filters() {
        assert!(matches!(
            document_matches(
                &doc! { "name": "Ada" },
                &doc! { "name": { "$options": "i" } }
            ),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(
                &doc! { "name": "Ada" },
                &doc! { "name": { "$regex": "^A", "$options": 1 } }
            ),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(
                &doc! { "name": "Ada" },
                &doc! { "name": { "$regex": "[", "$options": "i" } }
            ),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(
                &doc! { "name": "Ada" },
                &doc! { "name": { "$regex": "^A", "$options": "q" } }
            ),
            Err(QueryError::InvalidStructure)
        ));
        assert!(matches!(
            document_matches(&doc! { "name": "Ada" }, &doc! { "name": { "$not": {} } }),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_elem_match_filters() {
        assert_filter(
            &doc! { "a": [3, 5, 7] },
            doc! { "a": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
            true,
        );
        assert_filter(
            &doc! { "a": [3, 7] },
            doc! { "a": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
            false,
        );
        assert_filter(
            &doc! { "a": [[5]] },
            doc! { "a": { "$elemMatch": { "$elemMatch": { "$lt": 6, "$gt": 4 } } } },
            true,
        );
        assert_filter(
            &doc! { "a": [{ "b": 2, "c": 3 }, { "b": 1, "c": 4 }] },
            doc! { "a": { "$elemMatch": { "b": 1, "c": 4 } } },
            true,
        );
        assert_filter(
            &doc! { "a": [{ "b": [12, 2], "c": [13, 3] }] },
            doc! { "a": { "$elemMatch": { "b": 2 } } },
            true,
        );
        assert_filter(
            &doc! { "a": [{ "b": [5] }] },
            doc! { "a.b": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
            true,
        );
    }

    #[test]
    fn rejects_malformed_elem_match_filters() {
        assert!(matches!(
            document_matches(&doc! { "a": [1, 2] }, &doc! { "a": { "$elemMatch": 1 } }),
            Err(QueryError::InvalidStructure)
        ));
    }

    #[test]
    fn supports_logical_and_or_query_filters() {
        let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

        assert_filter(
            &document,
            doc! {
                "$and": [
                    { "qty": { "$gte": 5 } },
                    { "$or": [ { "sku": "missing" }, { "meta.enabled": true } ] }
                ]
            },
            true,
        );
        assert_filter(
            &document,
            doc! {
                "$and": [
                    { "qty": { "$gt": 5 } },
                    { "$or": [ { "sku": "abc" }, { "meta.enabled": true } ] }
                ]
            },
            false,
        );
    }

    #[test]
    fn supports_nor_query_filters() {
        let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

        assert_filter(
            &document,
            doc! { "$nor": [{ "qty": { "$lt": 0 } }, { "sku": "missing" }] },
            true,
        );
        assert_filter(
            &document,
            doc! { "$nor": [{ "qty": { "$gte": 5 } }, { "sku": "missing" }] },
            false,
        );
    }

    #[test]
    fn applies_modifier_updates() {
        let mut document = doc! { "_id": 1, "qty": 2, "meta": { "enabled": true } };
        let update = parse_update(&doc! {
            "$set": { "meta.flag": "beta" },
            "$inc": { "qty": 3 },
            "$unset": { "meta.enabled": "" }
        })
        .expect("parse update");

        apply_update(&mut document, &update).expect("apply");
        assert_eq!(document.get_i64("qty").expect("qty"), 5);
        assert_eq!(
            document
                .get_document("meta")
                .expect("meta")
                .get_str("flag")
                .expect("flag"),
            "beta"
        );
        assert!(
            document
                .get_document("meta")
                .expect("meta")
                .get("enabled")
                .is_none()
        );
    }

    #[test]
    fn projection_only_excludes_id() {
        let document = doc! { "_id": 1, "sku": "abc", "qty": 5 };
        let projected =
            apply_projection(&document, Some(&doc! { "_id": 0 })).expect("apply projection");

        assert_eq!(projected, doc! { "sku": "abc", "qty": 5 });
    }

    #[test]
    fn projection_supports_nested_inclusion_and_computed_fields() {
        let document = doc! { "_id": 1, "sku": "abc", "meta": { "enabled": true, "flag": "beta" } };
        let projected = apply_projection(
            &document,
            Some(&doc! {
                "sku": 1,
                "meta.enabled": 1,
                "copiedSku": "$sku",
                "answer": { "$literal": 42 }
            }),
        )
        .expect("apply projection");

        assert_eq!(
            projected,
            doc! {
                "_id": 1,
                "sku": "abc",
                "meta": { "enabled": true },
                "copiedSku": "abc",
                "answer": 42
            }
        );
    }

    #[test]
    fn projection_supports_nested_exclusion_paths() {
        let document =
            doc! { "_id": 1, "sku": "abc", "qty": 5, "meta": { "enabled": true, "flag": "beta" } };
        let projected = apply_projection(
            &document,
            Some(&doc! { "_id": 0, "qty": 0, "meta.enabled": 0 }),
        )
        .expect("apply projection");

        assert_eq!(projected, doc! { "sku": "abc", "meta": { "flag": "beta" } });
    }

    #[test]
    fn runs_match_group_and_sort_pipeline() {
        let documents = vec![
            doc! { "team": "red", "points": 10 },
            doc! { "team": "red", "points": 4 },
            doc! { "team": "blue", "points": 7 },
        ];
        let results = run_pipeline(
            documents,
            &[
                doc! { "$match": { "points": { "$gte": 4 } } },
                doc! { "$group": { "_id": "$team", "total": { "$sum": "$points" } } },
                doc! { "$sort": { "total": -1 } },
            ],
        )
        .expect("pipeline");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_str("_id").expect("team"), "red");
        assert_eq!(results[0].get("total"), Some(&Bson::Int64(14)));
    }

    #[test]
    fn match_stage_supports_supported_query_operators() {
        let results = run_pipeline_ok(
            vec![
                doc! { "_id": 1, "qty": 5, "sku": "abc", "meta": { "enabled": true } },
                doc! { "_id": 2, "qty": 1, "sku": "xyz" },
            ],
            &[doc! {
                "$match": {
                    "$and": [
                        { "qty": { "$gte": 5 } },
                        { "$or": [
                            { "sku": { "$in": ["def", "abc"] } },
                            { "meta.enabled": { "$exists": true } }
                        ] }
                    ]
                }
            }],
        );

        assert_eq!(
            results,
            vec![doc! { "_id": 1, "qty": 5, "sku": "abc", "meta": { "enabled": true } }]
        );
    }

    #[test]
    fn set_and_add_fields_stages_preserve_fields_and_support_literals() {
        let results = run_pipeline_ok(
            vec![doc! { "_id": 1, "sku": "abc", "qty": 2, "meta": { "enabled": true } }],
            &[
                doc! { "$set": { "qty": 5, "meta.flag": { "$literal": "beta" } } },
                doc! { "$addFields": { "copiedSku": "$sku" } },
            ],
        );

        assert_eq!(
            results,
            vec![doc! {
                "_id": 1,
                "sku": "abc",
                "qty": 5,
                "meta": { "enabled": true, "flag": "beta" },
                "copiedSku": "abc"
            }]
        );
    }

    #[test]
    fn unset_stage_supports_string_and_array_syntax() {
        let input = vec![doc! {
            "_id": 1,
            "sku": "abc",
            "qty": 5,
            "meta": { "enabled": true, "flag": "beta" }
        }];

        let single = run_pipeline_ok(input.clone(), &[doc! { "$unset": "qty" }]);
        assert_eq!(
            single,
            vec![doc! { "_id": 1, "sku": "abc", "meta": { "enabled": true, "flag": "beta" } }]
        );

        let multiple = run_pipeline_ok(input, &[doc! { "$unset": ["qty", "meta.enabled"] }]);
        assert_eq!(
            multiple,
            vec![doc! { "_id": 1, "sku": "abc", "meta": { "flag": "beta" } }]
        );
    }

    #[test]
    fn skip_and_limit_trim_pipeline_results() {
        let results = run_pipeline_ok(
            vec![
                doc! { "_id": 1 },
                doc! { "_id": 2 },
                doc! { "_id": 3 },
                doc! { "_id": 4 },
            ],
            &[doc! { "$skip": 1 }, doc! { "$limit": 2 }],
        );

        assert_eq!(results, vec![doc! { "_id": 2 }, doc! { "_id": 3 }]);
    }

    #[test]
    fn count_stage_uses_dynamic_output_field() {
        let results = run_pipeline(
            vec![doc! { "value": 1 }, doc! { "value": 2 }],
            &[doc! { "$count": "total" }],
        )
        .expect("pipeline");

        assert_eq!(results, vec![doc! { "total": 2_i64 }]);
    }

    #[test]
    fn count_stage_returns_zero_for_empty_input() {
        let results = run_pipeline_ok(Vec::new(), &[doc! { "$count": "total" }]);
        assert_eq!(results, vec![doc! { "total": 0_i64 }]);
    }

    #[test]
    fn sort_stage_supports_dotted_compound_and_descending_keys() {
        let dotted = run_pipeline_ok(
            vec![
                doc! { "_id": 0, "a": { "b": 2 } },
                doc! { "_id": 1, "a": { "b": 1 } },
            ],
            &[doc! { "$sort": { "a.b": 1 } }],
        );
        assert_eq!(
            dotted,
            vec![
                doc! { "_id": 1, "a": { "b": 1 } },
                doc! { "_id": 0, "a": { "b": 2 } }
            ]
        );

        let compound = run_pipeline_ok(
            vec![
                doc! { "_id": 0, "a": 1, "b": 3 },
                doc! { "_id": 1, "a": 1, "b": 2 },
                doc! { "_id": 2, "a": 0, "b": 4 },
            ],
            &[doc! { "$sort": { "a": 1, "b": -1 } }],
        );
        assert_eq!(
            compound,
            vec![
                doc! { "_id": 2, "a": 0, "b": 4 },
                doc! { "_id": 0, "a": 1, "b": 3 },
                doc! { "_id": 1, "a": 1, "b": 2 }
            ]
        );
    }

    #[test]
    fn sort_stage_orders_missing_and_null_before_numbers() {
        let missing = run_pipeline_ok(
            vec![doc! { "_id": 0, "a": 1 }, doc! { "_id": 1 }],
            &[doc! { "$sort": { "a": 1 } }],
        );
        assert_eq!(missing, vec![doc! { "_id": 1 }, doc! { "_id": 0, "a": 1 }]);

        let null = run_pipeline_ok(
            vec![
                doc! { "_id": 0, "a": 1 },
                doc! { "_id": 1, "a": Bson::Null },
            ],
            &[doc! { "$sort": { "a": 1 } }],
        );
        assert_eq!(
            null,
            vec![
                doc! { "_id": 1, "a": Bson::Null },
                doc! { "_id": 0, "a": 1 }
            ]
        );
    }

    #[test]
    fn unwind_stage_supports_nested_arrays() {
        let results = run_pipeline_ok(
            vec![doc! { "_id": 0, "a": { "b": [1, 2], "c": 3 } }],
            &[doc! { "$unwind": "$a.b" }],
        );

        assert_eq!(
            results,
            vec![
                doc! { "_id": 0, "a": { "b": 1, "c": 3 } },
                doc! { "_id": 0, "a": { "b": 2, "c": 3 } }
            ]
        );
    }

    #[test]
    fn unwind_stage_preserves_null_missing_and_empty_arrays_when_requested() {
        let results = run_pipeline_ok(
            vec![
                doc! { "_id": 0, "tags": [] },
                doc! { "_id": 1 },
                doc! { "_id": 2, "tags": Bson::Null },
                doc! { "_id": 3, "tags": ["a", "b"] },
            ],
            &[doc! { "$unwind": { "path": "$tags", "preserveNullAndEmptyArrays": true } }],
        );

        assert_eq!(
            results,
            vec![
                doc! { "_id": 0 },
                doc! { "_id": 1 },
                doc! { "_id": 2, "tags": Bson::Null },
                doc! { "_id": 3, "tags": "a" },
                doc! { "_id": 3, "tags": "b" }
            ]
        );
    }

    #[test]
    fn group_stage_supports_sum_first_push_and_avg_accumulators() {
        let results = run_pipeline_ok(
            vec![
                doc! { "team": "blue", "sku": "b1", "qty": 2 },
                doc! { "team": "red", "sku": "r1", "qty": 1 },
                doc! { "team": "red", "sku": "r2", "qty": 3 },
            ],
            &[
                doc! {
                    "$group": {
                        "_id": "$team",
                        "total": { "$sum": "$qty" },
                        "firstSku": { "$first": "$sku" },
                        "skus": { "$push": "$sku" },
                        "avgQty": { "$avg": "$qty" }
                    }
                },
                doc! { "$sort": { "_id": 1 } },
            ],
        );

        assert_eq!(
            results,
            vec![
                doc! {
                    "_id": "blue",
                    "total": 2_i64,
                    "firstSku": "b1",
                    "skus": ["b1"],
                    "avgQty": 2.0
                },
                doc! {
                    "_id": "red",
                    "total": 4_i64,
                    "firstSku": "r1",
                    "skus": ["r1", "r2"],
                    "avgQty": 2.0
                }
            ]
        );
    }

    #[test]
    fn replace_root_promotes_subdocuments_and_expression_objects() {
        let promoted = run_pipeline_ok(
            vec![doc! { "wrapper": { "name": "alpha", "qty": 1 } }],
            &[doc! { "$replaceRoot": { "newRoot": "$wrapper" } }],
        );
        assert_eq!(promoted, vec![doc! { "name": "alpha", "qty": 1 }]);

        let expression_object = run_pipeline_ok(
            vec![doc! { "name": "alpha" }],
            &[
                doc! { "$replaceRoot": { "newRoot": { "name": "$name", "wrapped": { "$literal": true } } } },
            ],
        );
        assert_eq!(
            expression_object,
            vec![doc! { "name": "alpha", "wrapped": true }]
        );
    }

    #[test]
    fn replace_root_errors_when_new_root_is_not_a_document() {
        let error = run_pipeline(
            vec![doc! { "value": 5 }],
            &[doc! { "$replaceRoot": { "newRoot": "$value" } }],
        )
        .expect_err("replaceRoot should reject scalars");

        assert!(matches!(error, QueryError::ExpectedDocument));
    }

    #[test]
    fn match_stage_propagates_invalid_operator_errors() {
        let error = run_pipeline(
            vec![doc! { "qty": 1 }],
            &[doc! { "$match": { "qty": { "$unknown": 1 } } }],
        )
        .expect_err("invalid operator");

        assert!(matches!(
            error,
            super::QueryError::UnsupportedOperator(operator) if operator == "$unknown"
        ));
    }

    #[test]
    fn pipeline_rejects_unsupported_stage() {
        let error = run_pipeline(
            vec![doc! { "qty": 1 }],
            &[doc! { "$lookup": { "from": "other" } }],
        )
        .expect_err("unsupported stage");

        assert!(matches!(
            error,
            super::QueryError::UnsupportedStage(stage) if stage == "$lookup"
        ));
    }

    #[test]
    fn projection_rejects_unsupported_expression_operator() {
        let error = apply_projection(
            &doc! { "_id": 1, "value": 2 },
            Some(&doc! { "out": { "$add": [1, 2] } }),
        )
        .expect_err("unsupported expression");

        assert!(matches!(
            error,
            super::QueryError::UnsupportedOperator(operator) if operator == "$add"
        ));
    }
}
