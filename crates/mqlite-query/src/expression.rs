use std::collections::BTreeMap;

use bson::{Bson, Document};
use mqlite_bson::{compare_bson, lookup_path_owned};

use crate::QueryError;

pub(crate) fn eval_expression(document: &Document, expression: &Bson) -> Result<Bson, QueryError> {
    eval_expression_with_variables(document, expression, &BTreeMap::new())
}

pub(crate) fn eval_expression_with_variables(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Bson, QueryError> {
    match expression {
        Bson::String(path) if path.starts_with("$$") => {
            Ok(variable_value(document, path, variables))
        }
        Bson::String(path) if path.starts_with('$') => {
            Ok(lookup_path_owned(document, &path[1..]).unwrap_or(Bson::Null))
        }
        Bson::Document(spec) if spec.len() == 1 => {
            let (field, value) = spec.iter().next().expect("single field");
            if field.starts_with('$') {
                return eval_expression_operator(document, field, value, variables);
            }

            let mut evaluated = Document::new();
            for (field, value) in spec {
                evaluated.insert(
                    field,
                    eval_expression_with_variables(document, value, variables)?,
                );
            }
            Ok(Bson::Document(evaluated))
        }
        Bson::Document(spec) => {
            let mut evaluated = Document::new();
            for (field, value) in spec {
                evaluated.insert(
                    field,
                    eval_expression_with_variables(document, value, variables)?,
                );
            }
            Ok(Bson::Document(evaluated))
        }
        Bson::Array(items) => Ok(Bson::Array(
            items
                .iter()
                .map(|item| eval_expression_with_variables(document, item, variables))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Ok(expression.clone()),
    }
}

pub(crate) fn validate_expression(expression: &Bson) -> Result<(), QueryError> {
    eval_expression(&Document::new(), expression).map(|_| ())
}

fn eval_expression_operator(
    document: &Document,
    operator: &str,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Bson, QueryError> {
    match operator {
        "$literal" => Ok(value.clone()),
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {
            let [left, right] = expression_arguments::<2>(value)?;
            let left = eval_expression_with_variables(document, left, variables)?;
            let right = eval_expression_with_variables(document, right, variables)?;
            let ordering = compare_bson(&left, &right);
            Ok(Bson::Boolean(match operator {
                "$eq" => ordering.is_eq(),
                "$ne" => !ordering.is_eq(),
                "$gt" => ordering.is_gt(),
                "$gte" => matches!(
                    ordering,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                ),
                "$lt" => ordering.is_lt(),
                "$lte" => matches!(
                    ordering,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                ),
                _ => unreachable!("comparison operator"),
            }))
        }
        "$and" => {
            let arguments = expression_argument_slice(value)?;
            Ok(Bson::Boolean(arguments.iter().try_fold(
                true,
                |result, argument| {
                    Ok::<_, QueryError>(
                        result
                            && expression_truthy(&eval_expression_with_variables(
                                document, argument, variables,
                            )?),
                    )
                },
            )?))
        }
        "$or" => {
            let arguments = expression_argument_slice(value)?;
            Ok(Bson::Boolean(arguments.iter().try_fold(
                false,
                |result, argument| {
                    Ok::<_, QueryError>(
                        result
                            || expression_truthy(&eval_expression_with_variables(
                                document, argument, variables,
                            )?),
                    )
                },
            )?))
        }
        "$not" => {
            let [argument] = expression_arguments::<1>(value)?;
            Ok(Bson::Boolean(!expression_truthy(
                &eval_expression_with_variables(document, argument, variables)?,
            )))
        }
        "$in" => {
            let [needle, haystack] = expression_arguments::<2>(value)?;
            let needle = eval_expression_with_variables(document, needle, variables)?;
            let haystack = eval_expression_with_variables(document, haystack, variables)?;
            let values = haystack.as_array().ok_or(QueryError::InvalidStructure)?;
            Ok(Bson::Boolean(
                values
                    .iter()
                    .any(|candidate| compare_bson(&needle, candidate).is_eq()),
            ))
        }
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

fn variable_value(document: &Document, path: &str, variables: &BTreeMap<String, Bson>) -> Bson {
    let mut segments = path[2..].splitn(2, '.');
    let name = segments.next().unwrap_or_default();
    let remainder = segments.next();

    let source = match name {
        "ROOT" | "CURRENT" => Bson::Document(document.clone()),
        _ => variables.get(name).cloned().unwrap_or(Bson::Null),
    };

    match remainder {
        Some(path) => match source {
            Bson::Document(document) => lookup_path_owned(&document, path).unwrap_or(Bson::Null),
            _ => Bson::Null,
        },
        None => source,
    }
}

fn expression_argument_slice(value: &Bson) -> Result<&[Bson], QueryError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or(QueryError::InvalidStructure)
}

fn expression_arguments<const N: usize>(value: &Bson) -> Result<[&Bson; N], QueryError> {
    let arguments = expression_argument_slice(value)?;
    if arguments.len() != N {
        return Err(QueryError::InvalidStructure);
    }
    Ok(std::array::from_fn(|index| &arguments[index]))
}

pub(crate) fn expression_truthy(value: &Bson) -> bool {
    match value {
        Bson::Boolean(value) => *value,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(value) => *value != 0,
        Bson::Int64(value) => *value != 0,
        Bson::Double(value) => *value != 0.0 && !value.is_nan(),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .map(|value| value != 0.0 && !value.is_nan())
            .unwrap_or(true),
        _ => true,
    }
}

pub(crate) fn numeric_value(value: &Bson) -> Result<f64, QueryError> {
    match value {
        Bson::Int32(value) => Ok(*value as f64),
        Bson::Int64(value) => Ok(*value as f64),
        Bson::Double(value) => Ok(*value),
        _ => Err(QueryError::ExpectedNumeric),
    }
}

pub(crate) fn integer_value(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i64),
        _ => None,
    }
}

pub(crate) fn number_bson(value: f64) -> Bson {
    if value.fract() == 0.0 {
        Bson::Int64(value as i64)
    } else {
        Bson::Double(value)
    }
}
