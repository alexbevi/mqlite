use std::{
    collections::{BTreeMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path, remove_path, set_path};

use crate::{
    QueryError,
    expression::{eval_expression, integer_value, number_bson, numeric_value},
    filter::document_matches,
    projection::apply_projection,
};

pub fn run_pipeline(
    documents: Vec<Document>,
    pipeline: &[Document],
) -> Result<Vec<Document>, QueryError> {
    let mut current = documents;

    for (stage_index, stage) in pipeline.iter().enumerate() {
        if stage.len() != 1 {
            return Err(QueryError::InvalidStage);
        }

        let (stage_name, stage_spec) = stage.iter().next().expect("single stage");
        current = match stage_name.as_str() {
            "$documents" => documents_stage(stage_index, stage_spec)?,
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
            "$sample" => sample_documents(current, stage_spec)?,
            "$skip" => {
                let skip = integer_value(stage_spec).ok_or(QueryError::InvalidStage)?;
                current.into_iter().skip(skip.max(0) as usize).collect()
            }
            "$sortByCount" => sort_by_count(current, stage_spec)?,
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
            "$replaceWith" => replace_with(current, stage_spec)?,
            other => return Err(QueryError::UnsupportedStage(other.to_string())),
        };
    }

    Ok(current)
}

fn documents_stage(stage_index: usize, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    if stage_index != 0 {
        return Err(QueryError::InvalidStage);
    }

    let documents = spec.as_array().ok_or(QueryError::InvalidStage)?;
    documents
        .iter()
        .map(|value| {
            value
                .as_document()
                .cloned()
                .ok_or(QueryError::ExpectedDocument)
        })
        .collect()
}

fn sample_documents(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let spec = spec.as_document().ok_or(QueryError::InvalidStage)?;
    if spec.len() != 1 {
        return Err(QueryError::InvalidStage);
    }

    let size = match spec.get("size") {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) if value.fract() == 0.0 => *value as i64,
        _ => return Err(QueryError::InvalidStage),
    };

    if size <= 0 {
        return Err(QueryError::InvalidStage);
    }
    if size as usize >= documents.len() {
        return Ok(documents);
    }

    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut scored = documents
        .into_iter()
        .enumerate()
        .map(|(position, document)| {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            position.hash(&mut hasher);
            bson::to_vec(&document)
                .unwrap_or_default()
                .hash(&mut hasher);
            (hasher.finish(), position, document)
        })
        .collect::<Vec<_>>();
    scored.sort_by_key(|(score, position, _)| (*score, *position));
    Ok(scored
        .into_iter()
        .take(size as usize)
        .map(|(_, _, document)| document)
        .collect())
}

fn sort_by_count(documents: Vec<Document>, spec: &Bson) -> Result<Vec<Document>, QueryError> {
    let expression = match spec {
        Bson::String(path) if path.starts_with('$') && path.len() > 1 => Bson::String(path.clone()),
        Bson::Document(document)
            if document
                .iter()
                .next()
                .is_some_and(|(field, _)| field.starts_with('$')) =>
        {
            Bson::Document(document.clone())
        }
        _ => return Err(QueryError::InvalidStage),
    };

    let group_spec = doc! {
        "_id": expression,
        "count": { "$sum": 1 },
    };
    let mut results = group_documents(documents, &group_spec)?;
    let sort_spec = doc! { "count": -1 };
    results.sort_by(|left, right| compare_documents_by_sort(left, right, &sort_spec));
    Ok(results)
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

fn replace_with(documents: Vec<Document>, expression: &Bson) -> Result<Vec<Document>, QueryError> {
    documents
        .into_iter()
        .map(|document| match eval_expression(&document, expression)? {
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
