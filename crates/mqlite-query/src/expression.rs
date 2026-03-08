use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path_owned};

use crate::{QueryError, filter::bson_type_alias};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EvaluatedExpression {
    Missing,
    Value(Bson),
}

impl EvaluatedExpression {
    fn into_bson_or_null(self) -> Bson {
        match self {
            Self::Missing => Bson::Null,
            Self::Value(value) => value,
        }
    }

    fn is_nullish(&self) -> bool {
        matches!(
            self,
            Self::Missing | Self::Value(Bson::Null | Bson::Undefined)
        )
    }
}

pub(crate) fn eval_expression(document: &Document, expression: &Bson) -> Result<Bson, QueryError> {
    eval_expression_with_variables(document, expression, &BTreeMap::new())
}

pub(crate) fn eval_expression_with_variables(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Bson, QueryError> {
    Ok(eval_expression_result_with_variables(document, expression, variables)?.into_bson_or_null())
}

pub(crate) fn eval_expression_result_with_variables(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    match expression {
        Bson::String(path) if path.starts_with("$$") => {
            Ok(variable_value(document, path, variables))
        }
        Bson::String(path) if path.starts_with('$') => Ok(lookup_path_owned(document, &path[1..])
            .map(EvaluatedExpression::Value)
            .unwrap_or(EvaluatedExpression::Missing)),
        Bson::Document(spec) if spec.len() == 1 => {
            let (field, value) = spec.iter().next().expect("single field");
            if field.starts_with('$') {
                return eval_expression_operator(document, field, value, variables);
            }

            let mut evaluated = Document::new();
            for (field, value) in spec {
                match eval_expression_result_with_variables(document, value, variables)? {
                    EvaluatedExpression::Missing => {}
                    EvaluatedExpression::Value(value) => {
                        evaluated.insert(field, value);
                    }
                }
            }
            Ok(EvaluatedExpression::Value(Bson::Document(evaluated)))
        }
        Bson::Document(spec) => {
            let mut evaluated = Document::new();
            for (field, value) in spec {
                match eval_expression_result_with_variables(document, value, variables)? {
                    EvaluatedExpression::Missing => {}
                    EvaluatedExpression::Value(value) => {
                        evaluated.insert(field, value);
                    }
                }
            }
            Ok(EvaluatedExpression::Value(Bson::Document(evaluated)))
        }
        Bson::Array(items) => Ok(EvaluatedExpression::Value(Bson::Array(
            items
                .iter()
                .map(|item| {
                    eval_expression_result_with_variables(document, item, variables)
                        .map(EvaluatedExpression::into_bson_or_null)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        _ => Ok(EvaluatedExpression::Value(expression.clone())),
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
) -> Result<EvaluatedExpression, QueryError> {
    match operator {
        "$const" | "$literal" => Ok(EvaluatedExpression::Value(value.clone())),
        "$expr" => eval_expression_result_with_variables(document, value, variables),
        "$cond" => eval_cond_expression(document, value, variables),
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$cmp" => {
            let [left, right] = expression_arguments::<2>(value)?;
            let left = eval_expression_with_variables(document, left, variables)?;
            let right = eval_expression_with_variables(document, right, variables)?;
            let ordering = compare_bson(&left, &right);
            match operator {
                "$cmp" => Ok(EvaluatedExpression::Value(Bson::Int32(match ordering {
                    std::cmp::Ordering::Less => -1,
                    std::cmp::Ordering::Equal => 0,
                    std::cmp::Ordering::Greater => 1,
                }))),
                _ => Ok(EvaluatedExpression::Value(Bson::Boolean(match operator {
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
                }))),
            }
        }
        "$add" => eval_add_expression(document, value, variables),
        "$subtract" => eval_binary_numeric_expression(document, value, variables, |left, right| {
            Ok(EvaluatedExpression::Value(number_bson(left - right)))
        }),
        "$multiply" => {
            let arguments = expression_argument_slice(value)?;
            if arguments.is_empty() {
                return Err(QueryError::InvalidStructure);
            }
            let mut total = 1.0;
            for argument in arguments {
                total *= eval_numeric_expression(document, argument, variables)?;
            }
            Ok(EvaluatedExpression::Value(number_bson(total)))
        }
        "$divide" => eval_binary_numeric_expression(document, value, variables, |left, right| {
            if right == 0.0 {
                return Err(QueryError::InvalidArgument(
                    "cannot divide by zero".to_string(),
                ));
            }
            Ok(EvaluatedExpression::Value(number_bson(left / right)))
        }),
        "$mod" => {
            let [left, right] = expression_arguments::<2>(value)?;
            let left = eval_expression_with_variables(document, left, variables)?;
            let right = eval_expression_with_variables(document, right, variables)?;
            let left = coerce_to_i64(&left).ok_or(QueryError::ExpectedNumeric)?;
            let right = coerce_to_i64(&right).ok_or(QueryError::ExpectedNumeric)?;
            if right == 0 {
                return Err(QueryError::InvalidArgument(
                    "cannot divide by zero".to_string(),
                ));
            }
            Ok(EvaluatedExpression::Value(Bson::Int64(left % right)))
        }
        "$abs" => eval_unary_numeric_expression(document, value, variables, |number| {
            Ok(EvaluatedExpression::Value(number_bson(number.abs())))
        }),
        "$ceil" => eval_unary_numeric_expression(document, value, variables, |number| {
            Ok(EvaluatedExpression::Value(number_bson(number.ceil())))
        }),
        "$floor" => eval_unary_numeric_expression(document, value, variables, |number| {
            Ok(EvaluatedExpression::Value(number_bson(number.floor())))
        }),
        "$allElementsTrue" => eval_array_truth_expression(document, value, variables, true),
        "$anyElementTrue" => eval_array_truth_expression(document, value, variables, false),
        "$concat" => eval_concat_expression(document, value, variables),
        "$isNumber" => Ok(EvaluatedExpression::Value(Bson::Boolean(matches!(
            eval_expression_result_with_variables(document, unary_expression_operand(value), variables)?,
            EvaluatedExpression::Value(Bson::Int32(_)
                | Bson::Int64(_)
                | Bson::Double(_)
                | Bson::Decimal128(_))
        )))),
        "$type" => Ok(EvaluatedExpression::Value(Bson::String(
            match eval_expression_result_with_variables(
                document,
                unary_expression_operand(value),
                variables,
            )? {
                EvaluatedExpression::Missing => "missing".to_string(),
                EvaluatedExpression::Value(value) => bson_type_alias(&value).to_string(),
            },
        ))),
        "$round" => eval_rounding_expression(document, value, variables, f64::round),
        "$trunc" => eval_rounding_expression(document, value, variables, f64::trunc),
        "$ifNull" => eval_if_null_expression(document, value, variables),
        "$arrayElemAt" => eval_array_elem_at_expression(document, value, variables),
        "$arrayToObject" => eval_array_to_object_expression(document, value, variables),
        "$concatArrays" => eval_concat_arrays_expression(document, value, variables),
        "$first" => eval_first_last_expression(document, value, variables, true),
        "$isArray" => Ok(EvaluatedExpression::Value(Bson::Boolean(matches!(
            eval_expression_result_with_variables(document, value, variables)?,
            EvaluatedExpression::Value(Bson::Array(_))
        )))),
        "$last" => eval_first_last_expression(document, value, variables, false),
        "$mergeObjects" => eval_merge_objects_expression(document, value, variables),
        "$objectToArray" => eval_object_to_array_expression(document, value, variables),
        "$size" => eval_size_expression(document, value, variables),
        "$and" => {
            let arguments = expression_argument_slice(value)?;
            Ok(EvaluatedExpression::Value(Bson::Boolean(
                arguments.iter().try_fold(true, |result, argument| {
                    Ok::<_, QueryError>(
                        result
                            && expression_truthy(&eval_expression_with_variables(
                                document, argument, variables,
                            )?),
                    )
                })?,
            )))
        }
        "$or" => {
            let arguments = expression_argument_slice(value)?;
            Ok(EvaluatedExpression::Value(Bson::Boolean(
                arguments.iter().try_fold(false, |result, argument| {
                    Ok::<_, QueryError>(
                        result
                            || expression_truthy(&eval_expression_with_variables(
                                document, argument, variables,
                            )?),
                    )
                })?,
            )))
        }
        "$not" => {
            let [argument] = expression_arguments::<1>(value)?;
            Ok(EvaluatedExpression::Value(Bson::Boolean(
                !expression_truthy(&eval_expression_with_variables(
                    document, argument, variables,
                )?),
            )))
        }
        "$in" => {
            let [needle, haystack] = expression_arguments::<2>(value)?;
            let needle = eval_expression_with_variables(document, needle, variables)?;
            let haystack = eval_expression_with_variables(document, haystack, variables)?;
            let values = haystack.as_array().ok_or(QueryError::InvalidStructure)?;
            Ok(EvaluatedExpression::Value(Bson::Boolean(
                values
                    .iter()
                    .any(|candidate| compare_bson(&needle, candidate).is_eq()),
            )))
        }
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

fn variable_value(
    document: &Document,
    path: &str,
    variables: &BTreeMap<String, Bson>,
) -> EvaluatedExpression {
    let mut segments = path[2..].splitn(2, '.');
    let name = segments.next().unwrap_or_default();
    let remainder = segments.next();

    let source = match name {
        "ROOT" => variables
            .get("ROOT")
            .cloned()
            .map(EvaluatedExpression::Value)
            .unwrap_or_else(|| EvaluatedExpression::Value(Bson::Document(document.clone()))),
        "CURRENT" => variables
            .get("CURRENT")
            .cloned()
            .map(EvaluatedExpression::Value)
            .unwrap_or_else(|| EvaluatedExpression::Value(Bson::Document(document.clone()))),
        _ => variables
            .get(name)
            .cloned()
            .map(EvaluatedExpression::Value)
            .unwrap_or(EvaluatedExpression::Missing),
    };

    match remainder {
        Some(path) => match source {
            EvaluatedExpression::Value(Bson::Document(document)) => {
                lookup_path_owned(&document, path)
                    .map(EvaluatedExpression::Value)
                    .unwrap_or(EvaluatedExpression::Missing)
            }
            _ => EvaluatedExpression::Missing,
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

fn unary_expression_operand(value: &Bson) -> &Bson {
    match value {
        Bson::Array(arguments) if arguments.len() == 1 => &arguments[0],
        _ => value,
    }
}

fn eval_add_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if arguments.is_empty() {
        return Err(QueryError::InvalidStructure);
    }
    let mut total = 0.0;
    for argument in arguments {
        total += eval_numeric_expression(document, argument, variables)?;
    }
    Ok(EvaluatedExpression::Value(number_bson(total)))
}

fn eval_binary_numeric_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operation: impl FnOnce(f64, f64) -> Result<EvaluatedExpression, QueryError>,
) -> Result<EvaluatedExpression, QueryError> {
    let [left, right] = expression_arguments::<2>(value)?;
    let left = eval_numeric_expression(document, left, variables)?;
    let right = eval_numeric_expression(document, right, variables)?;
    operation(left, right)
}

fn eval_unary_numeric_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operation: impl FnOnce(f64) -> Result<EvaluatedExpression, QueryError>,
) -> Result<EvaluatedExpression, QueryError> {
    let number = eval_numeric_expression(document, value, variables)?;
    operation(number)
}

fn eval_numeric_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<f64, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    numeric_value(&value)
}

fn eval_rounding_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    rounder: impl Fn(f64) -> f64,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if !(1..=2).contains(&arguments.len()) {
        return Err(QueryError::InvalidStructure);
    }

    let number = eval_numeric_expression(document, &arguments[0], variables)?;
    let place = match arguments.get(1) {
        Some(place) => {
            let value = eval_expression_with_variables(document, place, variables)
                .and_then(|value| integer_value(&value).ok_or(QueryError::InvalidStructure))?;
            i32::try_from(value).map_err(|_| QueryError::InvalidStructure)?
        }
        None => 0,
    };

    let result = if place == 0 {
        rounder(number)
    } else {
        let factor = 10_f64.powi(place);
        rounder(number * factor) / factor
    };
    Ok(EvaluatedExpression::Value(number_bson(result)))
}

fn eval_if_null_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if arguments.len() < 2 {
        return Err(QueryError::InvalidStructure);
    }

    let mut last_value = EvaluatedExpression::Value(Bson::Null);
    for argument in arguments {
        let evaluated = eval_expression_result_with_variables(document, argument, variables)?;
        if !evaluated.is_nullish() {
            return Ok(evaluated);
        }
        last_value = evaluated;
    }
    Ok(EvaluatedExpression::Value(last_value.into_bson_or_null()))
}

fn eval_cond_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (condition, on_true, on_false) = match value {
        Bson::Array(_) => {
            let [condition, on_true, on_false] = expression_arguments::<3>(value)?;
            (condition, on_true, on_false)
        }
        Bson::Document(spec) => {
            if spec.len() != 3 {
                return Err(QueryError::InvalidStructure);
            }
            let condition = spec.get("if").ok_or(QueryError::InvalidStructure)?;
            let on_true = spec.get("then").ok_or(QueryError::InvalidStructure)?;
            let on_false = spec.get("else").ok_or(QueryError::InvalidStructure)?;
            (condition, on_true, on_false)
        }
        _ => return Err(QueryError::InvalidStructure),
    };

    let condition = eval_expression_with_variables(document, condition, variables)?;
    if expression_truthy(&condition) {
        eval_expression_result_with_variables(document, on_true, variables)
    } else {
        eval_expression_result_with_variables(document, on_false, variables)
    }
}

fn eval_size_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    match eval_expression_result_with_variables(document, value, variables)? {
        EvaluatedExpression::Value(value) => {
            let size = array_length(&value).ok_or_else(|| {
                QueryError::InvalidArgument("$size requires an array input".to_string())
            })?;
            Ok(EvaluatedExpression::Value(Bson::Int64(size as i64)))
        }
        EvaluatedExpression::Missing => Err(QueryError::InvalidArgument(
            "$size requires an array input".to_string(),
        )),
    }
}

fn eval_array_elem_at_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let [array, index] = expression_arguments::<2>(value)?;
    let array = eval_expression_result_with_variables(document, array, variables)?;
    if array.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(Bson::Array(items)) = array else {
        return Err(QueryError::InvalidArgument(
            "$arrayElemAt requires an array as the first argument".to_string(),
        ));
    };

    let index = eval_expression_result_with_variables(document, index, variables)?;
    if index.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(index) = index else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let index = integral_numeric_i64(&index).ok_or_else(|| {
        QueryError::InvalidArgument("$arrayElemAt requires an integral numeric index".to_string())
    })?;

    let index = if index < 0 {
        items.len() as i64 + index
    } else {
        index
    };
    if !(0..items.len() as i64).contains(&index) {
        return Ok(EvaluatedExpression::Missing);
    }

    Ok(EvaluatedExpression::Value(items[index as usize].clone()))
}

fn eval_first_last_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    first: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let evaluated = eval_expression_result_with_variables(document, value, variables)?;
    if evaluated.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let EvaluatedExpression::Value(Bson::Array(items)) = evaluated else {
        return Err(QueryError::InvalidArgument(
            if first {
                "$first requires an array input"
            } else {
                "$last requires an array input"
            }
            .to_string(),
        ));
    };

    let value = if first { items.first() } else { items.last() };
    Ok(value
        .cloned()
        .map(EvaluatedExpression::Value)
        .unwrap_or(EvaluatedExpression::Missing))
}

fn eval_concat_arrays_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = match value {
        Bson::Array(arguments) => arguments.as_slice(),
        _ => std::slice::from_ref(value),
    };

    let mut concatenated = Vec::new();
    for argument in arguments {
        match eval_expression_result_with_variables(document, argument, variables)? {
            EvaluatedExpression::Missing
            | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            EvaluatedExpression::Value(Bson::Array(items)) => concatenated.extend(items),
            EvaluatedExpression::Value(_) => {
                return Err(QueryError::InvalidArgument(
                    "$concatArrays requires array inputs".to_string(),
                ));
            }
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Array(concatenated)))
}

fn eval_object_to_array_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    match eval_expression_result_with_variables(document, value, variables)? {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Document(document)) => {
            Ok(EvaluatedExpression::Value(Bson::Array(
                document
                    .into_iter()
                    .map(|(key, value)| Bson::Document(doc! { "k": key, "v": value }))
                    .collect(),
            )))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$objectToArray requires an object input".to_string(),
        )),
    }
}

fn eval_array_to_object_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    match eval_expression_result_with_variables(document, value, variables)? {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Array(items)) => Ok(EvaluatedExpression::Value(
            Bson::Document(array_to_object(items)?),
        )),
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$arrayToObject requires an array input".to_string(),
        )),
    }
}

fn eval_merge_objects_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = match value {
        Bson::Array(arguments) => arguments.as_slice(),
        _ => std::slice::from_ref(value),
    };

    let mut merged = Document::new();
    if arguments.len() == 1 {
        match eval_expression_result_with_variables(document, &arguments[0], variables)? {
            EvaluatedExpression::Missing
            | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
                return Ok(EvaluatedExpression::Value(Bson::Document(merged)));
            }
            EvaluatedExpression::Value(Bson::Array(items)) => {
                for item in items {
                    merge_object_input(item, &mut merged)?;
                }
                return Ok(EvaluatedExpression::Value(Bson::Document(merged)));
            }
            EvaluatedExpression::Value(Bson::Document(document)) => {
                for (key, value) in document {
                    merged.insert(key, value);
                }
                return Ok(EvaluatedExpression::Value(Bson::Document(merged)));
            }
            EvaluatedExpression::Value(_) => {
                return Err(QueryError::InvalidArgument(
                    "$mergeObjects requires object inputs".to_string(),
                ));
            }
        }
    }

    for argument in arguments {
        match eval_expression_result_with_variables(document, argument, variables)? {
            EvaluatedExpression::Missing
            | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {}
            EvaluatedExpression::Value(Bson::Document(document)) => {
                for (key, value) in document {
                    merged.insert(key, value);
                }
            }
            EvaluatedExpression::Value(_) => {
                return Err(QueryError::InvalidArgument(
                    "$mergeObjects requires object inputs".to_string(),
                ));
            }
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Document(merged)))
}

fn eval_array_truth_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    require_all: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let evaluated =
        eval_expression_result_with_variables(document, unary_expression_operand(value), variables)?;
    let EvaluatedExpression::Value(Bson::Array(items)) = evaluated else {
        return Err(QueryError::InvalidArgument(
            if require_all {
                "$allElementsTrue requires an array input"
            } else {
                "$anyElementTrue requires an array input"
            }
            .to_string(),
        ));
    };

    let result = if require_all {
        items.iter().all(expression_truthy)
    } else {
        items.iter().any(expression_truthy)
    };
    Ok(EvaluatedExpression::Value(Bson::Boolean(result)))
}

fn eval_concat_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = match value {
        Bson::Array(arguments) => arguments.as_slice(),
        _ => std::slice::from_ref(value),
    };

    let mut output = String::new();
    for argument in arguments {
        match eval_expression_result_with_variables(document, argument, variables)? {
            EvaluatedExpression::Missing
            | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            EvaluatedExpression::Value(Bson::String(value) | Bson::Symbol(value)) => {
                output.push_str(&value);
            }
            EvaluatedExpression::Value(_) => {
                return Err(QueryError::InvalidArgument(
                    "$concat requires string inputs".to_string(),
                ));
            }
        }
    }

    Ok(EvaluatedExpression::Value(Bson::String(output)))
}

fn merge_object_input(value: Bson, merged: &mut Document) -> Result<(), QueryError> {
    match value {
        Bson::Document(document) => {
            for (key, value) in document {
                merged.insert(key, value);
            }
            Ok(())
        }
        Bson::Null | Bson::Undefined => Ok(()),
        _ => Err(QueryError::InvalidArgument(
            "$mergeObjects requires object inputs".to_string(),
        )),
    }
}

fn array_to_object(items: Vec<Bson>) -> Result<Document, QueryError> {
    let mut object = Document::new();
    enum EntryKind {
        PairArray,
        KeyValueDocument,
    }

    let mut entry_kind = None;
    for item in items {
        match item {
            Bson::Array(values) => {
                if values.len() != 2 {
                    return Err(QueryError::InvalidArgument(
                        "$arrayToObject array entries must have exactly two elements".to_string(),
                    ));
                }
                if matches!(entry_kind, Some(EntryKind::KeyValueDocument)) {
                    return Err(QueryError::InvalidArgument(
                        "$arrayToObject requires a consistent input shape".to_string(),
                    ));
                }
                entry_kind = Some(EntryKind::PairArray);

                let key = values[0].as_str().ok_or_else(|| {
                    QueryError::InvalidArgument(
                        "$arrayToObject entry keys must be strings".to_string(),
                    )
                })?;
                validate_object_key(key)?;
                object.insert(key, values[1].clone());
            }
            Bson::Document(document) => {
                if matches!(entry_kind, Some(EntryKind::PairArray)) {
                    return Err(QueryError::InvalidArgument(
                        "$arrayToObject requires a consistent input shape".to_string(),
                    ));
                }
                entry_kind = Some(EntryKind::KeyValueDocument);
                if document.len() != 2 || !document.contains_key("k") || !document.contains_key("v")
                {
                    return Err(QueryError::InvalidArgument(
                        "$arrayToObject document entries must contain only `k` and `v`".to_string(),
                    ));
                }
                let key = document.get_str("k").map_err(|_| {
                    QueryError::InvalidArgument(
                        "$arrayToObject entry keys must be strings".to_string(),
                    )
                })?;
                validate_object_key(key)?;
                object.insert(key, document.get("v").cloned().unwrap_or(Bson::Null));
            }
            _ => {
                return Err(QueryError::InvalidArgument(
                    "$arrayToObject requires array entries or {k, v} documents".to_string(),
                ));
            }
        }
    }

    Ok(object)
}

fn validate_object_key(key: &str) -> Result<(), QueryError> {
    if key.contains('\0') {
        return Err(QueryError::InvalidArgument(
            "object keys cannot contain null bytes".to_string(),
        ));
    }
    Ok(())
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
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .map_err(|_| QueryError::ExpectedNumeric),
        _ => Err(QueryError::ExpectedNumeric),
    }
}

pub(crate) fn integer_value(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i64),
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            (parsed.fract() == 0.0).then_some(parsed as i64)
        }
        _ => None,
    }
}

pub(crate) fn integral_numeric_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.is_finite() && value.fract() == 0.0 => {
            truncate_f64_to_i64(*value)
        }
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            (parsed.is_finite() && parsed.fract() == 0.0).then(|| truncate_f64_to_i64(parsed))?
        }
        _ => None,
    }
}

pub(crate) fn coerce_to_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.is_finite() => truncate_f64_to_i64(*value),
        Bson::Decimal128(value) => truncate_f64_to_i64(value.to_string().parse::<f64>().ok()?),
        _ => None,
    }
}

pub(crate) fn truncate_f64_to_i64(value: f64) -> Option<i64> {
    let truncated = value.trunc();
    ((i64::MIN as f64)..=(i64::MAX as f64))
        .contains(&truncated)
        .then_some(truncated as i64)
}

pub(crate) fn array_length(value: &Bson) -> Option<usize> {
    value.as_array().map(Vec::len)
}

pub(crate) fn number_bson(value: f64) -> Bson {
    if value.fract() == 0.0 {
        Bson::Int64(value as i64)
    } else {
        Bson::Double(value)
    }
}
