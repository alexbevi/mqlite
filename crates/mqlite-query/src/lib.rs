use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path, lookup_path_owned, remove_path, set_path};
use thiserror::Error;

pub const SUPPORTED_QUERY_OPERATORS: &[&str] = &[
    "$and", "$or", "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$exists",
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

#[derive(Debug, Clone, PartialEq)]
pub enum MatchExpr {
    And(Vec<MatchExpr>),
    Or(Vec<MatchExpr>),
    Eq { path: String, value: Bson },
    Ne { path: String, value: Bson },
    Gt { path: String, value: Bson },
    Gte { path: String, value: Bson },
    Lt { path: String, value: Bson },
    Lte { path: String, value: Bson },
    In { path: String, values: Vec<Bson> },
    Exists { path: String, exists: bool },
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
        MatchExpr::Eq { path, value } => lookup_path(document, path)
            .is_some_and(|existing| compare_bson(existing, value).is_eq()),
        MatchExpr::Ne { path, value } => lookup_path(document, path)
            .is_none_or(|existing| !compare_bson(existing, value).is_eq()),
        MatchExpr::Gt { path, value } => lookup_path(document, path)
            .is_some_and(|existing| compare_bson(existing, value).is_gt()),
        MatchExpr::Gte { path, value } => lookup_path(document, path).is_some_and(|existing| {
            matches!(
                compare_bson(existing, value),
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater
            )
        }),
        MatchExpr::Lt { path, value } => lookup_path(document, path)
            .is_some_and(|existing| compare_bson(existing, value).is_lt()),
        MatchExpr::Lte { path, value } => lookup_path(document, path).is_some_and(|existing| {
            matches!(
                compare_bson(existing, value),
                std::cmp::Ordering::Equal | std::cmp::Ordering::Less
            )
        }),
        MatchExpr::In { path, values } => lookup_path(document, path).is_some_and(|existing| {
            values
                .iter()
                .any(|value| compare_bson(existing, value).is_eq())
        }),
        MatchExpr::Exists { path, exists } => lookup_path(document, path).is_some() == *exists,
    }
}

fn parse_field_expression(path: &str, value: &Bson) -> Result<MatchExpr, QueryError> {
    match value {
        Bson::Document(document) if document.keys().all(|key| key.starts_with('$')) => {
            let mut expressions = Vec::new();
            for (operator, operator_value) in document {
                expressions.push(match operator.as_str() {
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
                    "$exists" => MatchExpr::Exists {
                        path: path.to_string(),
                        exists: operator_value
                            .as_bool()
                            .ok_or(QueryError::InvalidStructure)?,
                    },
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

fn projection_flag(value: &Bson) -> Option<bool> {
    match value {
        Bson::Boolean(value) => Some(*value),
        Bson::Int32(value) => Some(*value != 0),
        Bson::Int64(value) => Some(*value != 0),
        _ => None,
    }
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
    use bson::{Bson, Document, doc};
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
