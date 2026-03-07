use bson::{Bson, Document};
use mqlite_bson::{lookup_path_owned, remove_path, set_path};

use crate::{
    QueryError,
    expression::{number_bson, numeric_value},
    pipeline::run_pipeline,
    types::{UpdateModifier, UpdateSpec},
};

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

pub fn parse_update_value(update: &Bson) -> Result<UpdateSpec, QueryError> {
    match update {
        Bson::Document(document) => parse_update(document),
        Bson::Array(stages) => {
            let pipeline = stages
                .iter()
                .map(|stage| stage.as_document().cloned().ok_or(QueryError::InvalidUpdate))
                .collect::<Result<Vec<_>, _>>()?;
            if pipeline.is_empty() {
                return Err(QueryError::InvalidUpdate);
            }
            Ok(UpdateSpec::Pipeline(pipeline))
        }
        _ => Err(QueryError::InvalidUpdate),
    }
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
        UpdateSpec::Pipeline(pipeline) => {
            let original_id = document.get("_id").cloned();
            let mut updated = run_pipeline(vec![document.clone()], pipeline)?;
            if updated.len() != 1 {
                return Err(QueryError::InvalidUpdate);
            }
            *document = updated.pop().expect("single document");
            if let Some(id) = original_id {
                document.entry("_id".to_string()).or_insert(id);
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

fn increment_value(current: &Bson, increment: &Bson) -> Result<Bson, QueryError> {
    let current_number = numeric_value(current)?;
    let increment_number = numeric_value(increment)?;
    let sum = current_number + increment_number;
    Ok(number_bson(sum))
}
