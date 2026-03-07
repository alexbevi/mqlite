use bson::{Bson, Document};
use mqlite_bson::{lookup_path, remove_path, set_path};

use crate::{QueryError, expression::eval_expression};

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

pub(crate) fn projection_flag(value: &Bson) -> Option<bool> {
    match value {
        Bson::Boolean(value) => Some(*value),
        Bson::Int32(value) => Some(*value != 0),
        Bson::Int64(value) => Some(*value != 0),
        _ => None,
    }
}
