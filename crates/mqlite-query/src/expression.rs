use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use bson::{Bson, Decimal128, Document, doc, oid::ObjectId};
use chrono::{DateTime, SecondsFormat, Utc};
use mqlite_bson::{compare_bson, lookup_path_owned};

use crate::{
    QueryError,
    filter::bson_type_alias,
    pipeline::{compare_documents_by_sort, validate_sort_spec},
};

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
        Bson::String(path) if path.starts_with("$$") => variable_value(document, path, variables),
        Bson::String(path) if path.starts_with('$') => {
            Ok(field_path_value(document, path, variables))
        }
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
    let scope = default_validation_scope();
    validate_expression_with_scope(expression, &scope)
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
        "$bitAnd" => {
            eval_bitwise_expression(document, value, variables, |left, right| left & right, -1)
        }
        "$bitNot" => eval_bit_not_expression(document, value, variables),
        "$bitOr" => {
            eval_bitwise_expression(document, value, variables, |left, right| left | right, 0)
        }
        "$bitXor" => {
            eval_bitwise_expression(document, value, variables, |left, right| left ^ right, 0)
        }
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
            eval_expression_result_with_variables(
                document,
                unary_expression_operand(value),
                variables
            )?,
            EvaluatedExpression::Value(
                Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
            )
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
        "$strcasecmp" => eval_strcasecmp_expression(document, value, variables),
        "$trunc" => eval_rounding_expression(document, value, variables, f64::trunc),
        "$ifNull" => eval_if_null_expression(document, value, variables),
        "$let" => eval_let_expression(document, value, variables),
        "$switch" => eval_switch_expression(document, value, variables),
        "$arrayElemAt" => eval_array_elem_at_expression(document, value, variables),
        "$arrayToObject" => eval_array_to_object_expression(document, value, variables),
        "$binarySize" => eval_binary_size_expression(document, value, variables),
        "$bsonSize" => eval_bson_size_expression(document, value, variables),
        "$concatArrays" => eval_concat_arrays_expression(document, value, variables),
        "$convert" => eval_convert_expression(document, value, variables),
        "$filter" => eval_filter_expression(document, value, variables),
        "$first" => eval_first_last_expression(document, value, variables, true),
        "$getField" => eval_get_field_expression(document, value, variables),
        "$rand" => eval_rand_expression(value),
        "$exp" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.exp())
        }),
        "$ln" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number > 0.0 {
                Ok(number.ln())
            } else {
                Err(QueryError::InvalidArgument(
                    "$ln's argument must be a positive number".to_string(),
                ))
            }
        }),
        "$log10" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number > 0.0 {
                Ok(number.log10())
            } else {
                Err(QueryError::InvalidArgument(
                    "$log10's argument must be a positive number".to_string(),
                ))
            }
        }),
        "$sqrt" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number >= 0.0 {
                Ok(number.sqrt())
            } else {
                Err(QueryError::InvalidArgument(
                    "$sqrt's argument must be greater than or equal to 0".to_string(),
                ))
            }
        }),
        "$degreesToRadians" => {
            eval_nullable_unary_math_expression(document, value, variables, |number| {
                Ok(number * (std::f64::consts::PI / 180.0))
            })
        }
        "$radiansToDegrees" => {
            eval_nullable_unary_math_expression(document, value, variables, |number| {
                Ok(number * (180.0 / std::f64::consts::PI))
            })
        }
        "$log" => eval_log_expression(document, value, variables),
        "$pow" => eval_pow_expression(document, value, variables),
        "$indexOfBytes" => eval_index_of_string_expression(document, value, variables, false),
        "$indexOfCP" => eval_index_of_string_expression(document, value, variables, true),
        "$indexOfArray" => eval_index_of_array_expression(document, value, variables),
        "$isArray" => Ok(EvaluatedExpression::Value(Bson::Boolean(matches!(
            eval_expression_result_with_variables(document, value, variables)?,
            EvaluatedExpression::Value(Bson::Array(_))
        )))),
        "$last" => eval_first_last_expression(document, value, variables, false),
        "$map" => eval_map_expression(document, value, variables),
        "$mergeObjects" => eval_merge_objects_expression(document, value, variables),
        "$objectToArray" => eval_object_to_array_expression(document, value, variables),
        "$range" => eval_range_expression(document, value, variables),
        "$reduce" => eval_reduce_expression(document, value, variables),
        "$replaceAll" => eval_replace_expression(document, value, variables, true),
        "$replaceOne" => eval_replace_expression(document, value, variables, false),
        "$reverseArray" => eval_reverse_array_expression(document, value, variables),
        "$sortArray" => eval_sort_array_expression(document, value, variables),
        "$slice" => eval_slice_expression(document, value, variables),
        "$toBool" => eval_convert_alias_expression(document, value, variables, ConvertTarget::Bool),
        "$toDate" => eval_convert_alias_expression(document, value, variables, ConvertTarget::Date),
        "$toDecimal" => {
            eval_convert_alias_expression(document, value, variables, ConvertTarget::Decimal)
        }
        "$toDouble" => {
            eval_convert_alias_expression(document, value, variables, ConvertTarget::Double)
        }
        "$toInt" => eval_convert_alias_expression(document, value, variables, ConvertTarget::Int),
        "$toLong" => eval_convert_alias_expression(document, value, variables, ConvertTarget::Long),
        "$toObjectId" => {
            eval_convert_alias_expression(document, value, variables, ConvertTarget::ObjectId)
        }
        "$toString" => {
            eval_convert_alias_expression(document, value, variables, ConvertTarget::String)
        }
        "$acos" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || (-1.0..=1.0).contains(&number) {
                Ok(number.acos())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $acos to values outside [-1, 1]".to_string(),
                ))
            }
        }),
        "$asin" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || (-1.0..=1.0).contains(&number) {
                Ok(number.asin())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $asin to values outside [-1, 1]".to_string(),
                ))
            }
        }),
        "$atan" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.atan())
        }),
        "$atan2" => {
            eval_nullable_binary_math_expression(document, value, variables, |left, right| {
                Ok(left.atan2(right))
            })
        }
        "$atanh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || (-1.0..=1.0).contains(&number) {
                Ok(number.atanh())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $atanh to values outside [-1, 1]".to_string(),
                ))
            }
        }),
        "$acosh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number >= 1.0 {
                Ok(number.acosh())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $acosh to values below 1".to_string(),
                ))
            }
        }),
        "$asinh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.asinh())
        }),
        "$cos" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number.is_finite() {
                Ok(number.cos())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $cos to infinite values".to_string(),
                ))
            }
        }),
        "$cosh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.cosh())
        }),
        "$sin" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number.is_finite() {
                Ok(number.sin())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $sin to infinite values".to_string(),
                ))
            }
        }),
        "$sinh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.sinh())
        }),
        "$tan" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            if number.is_nan() || number.is_finite() {
                Ok(number.tan())
            } else {
                Err(QueryError::InvalidArgument(
                    "cannot apply $tan to infinite values".to_string(),
                ))
            }
        }),
        "$tanh" => eval_nullable_unary_math_expression(document, value, variables, |number| {
            Ok(number.tanh())
        }),
        "$setDifference" => eval_set_difference_expression(document, value, variables),
        "$setEquals" => eval_set_equals_expression(document, value, variables),
        "$setIntersection" => eval_set_intersection_expression(document, value, variables),
        "$setIsSubset" => eval_set_is_subset_expression(document, value, variables),
        "$setUnion" => eval_set_union_expression(document, value, variables),
        "$setField" => eval_set_field_expression(document, value, variables, false),
        "$size" => eval_size_expression(document, value, variables),
        "$split" => eval_split_expression(document, value, variables),
        "$strLenBytes" => eval_string_length_expression(document, value, variables, false),
        "$strLenCP" => eval_string_length_expression(document, value, variables, true),
        "$substr" | "$substrBytes" => eval_substring_expression(document, value, variables, false),
        "$substrCP" => eval_substring_expression(document, value, variables, true),
        "$tsIncrement" => eval_timestamp_part_expression(document, value, variables, false),
        "$tsSecond" => eval_timestamp_part_expression(document, value, variables, true),
        "$trim" => eval_trim_expression(document, value, variables, TrimType::Both),
        "$ltrim" => eval_trim_expression(document, value, variables, TrimType::Left),
        "$rtrim" => eval_trim_expression(document, value, variables, TrimType::Right),
        "$zip" => eval_zip_expression(document, value, variables),
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
        "$toLower" => eval_case_fold_expression(document, value, variables, false),
        "$toUpper" => eval_case_fold_expression(document, value, variables, true),
        "$unsetField" => eval_set_field_expression(document, value, variables, true),
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

fn default_validation_scope() -> BTreeSet<String> {
    ["CURRENT", "DESCEND", "KEEP", "PRUNE", "ROOT"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn validate_expression_with_scope(
    expression: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match expression {
        Bson::String(path) if path.starts_with("$$") => validate_variable_reference(path, scope),
        Bson::Document(spec) if spec.len() == 1 => {
            let (field, value) = spec.iter().next().expect("single field");
            if field.starts_with('$') {
                return validate_expression_operator(field, value, scope);
            }

            for value in spec.values() {
                validate_expression_with_scope(value, scope)?;
            }
            Ok(())
        }
        Bson::Document(spec) => {
            for value in spec.values() {
                validate_expression_with_scope(value, scope)?;
            }
            Ok(())
        }
        Bson::Array(items) => {
            for item in items {
                validate_expression_with_scope(item, scope)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_expression_operator(
    operator: &str,
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    match operator {
        "$const" | "$literal" => Ok(()),
        "$expr" | "$abs" | "$acos" | "$acosh" | "$asin" | "$asinh" | "$atan" | "$atanh"
        | "$ceil" | "$cos" | "$cosh" | "$degreesToRadians" | "$exp" | "$first" | "$floor"
        | "$isArray" | "$isNumber" | "$last" | "$ln" | "$objectToArray" | "$radiansToDegrees"
        | "$sin" | "$sinh" | "$size" | "$sqrt" | "$tan" | "$tanh" | "$tsIncrement"
        | "$tsSecond" | "$type" | "$log10" => {
            validate_expression_with_scope(unary_expression_operand(value), scope)
        }
        "$binarySize" | "$bsonSize" => {
            validate_expression_with_scope(single_expression_operand(value)?, scope)
        }
        "$add" | "$allElementsTrue" | "$and" | "$anyElementTrue" | "$arrayToObject" | "$concat"
        | "$concatArrays" | "$eq" | "$gt" | "$gte" | "$in" | "$lt" | "$lte" | "$mergeObjects"
        | "$mod" | "$multiply" | "$ne" | "$not" | "$or" | "$round" | "$subtract" | "$trunc" => {
            let arguments = match value {
                Bson::Array(arguments) => arguments.as_slice(),
                _ => std::slice::from_ref(value),
            };
            if arguments.is_empty() {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$arrayElemAt" | "$atan2" | "$cmp" | "$divide" | "$log" | "$pow" => {
            for argument in expression_arguments::<2>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$bitNot" => validate_expression_with_scope(single_expression_operand(value)?, scope),
        "$bitAnd" | "$bitOr" | "$bitXor" => {
            let arguments = match value {
                Bson::Array(arguments) => arguments.as_slice(),
                _ => std::slice::from_ref(value),
            };
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$strLenBytes" | "$strLenCP" => {
            validate_expression_with_scope(single_expression_operand(value)?, scope)
        }
        "$substr" | "$substrBytes" | "$substrCP" => {
            for argument in expression_arguments::<3>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$trim" | "$ltrim" | "$rtrim" => validate_trim_expression(value, scope),
        "$strcasecmp" => {
            for argument in expression_arguments::<2>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$cond" => validate_cond_expression(value, scope),
        "$filter" => validate_filter_expression(value, scope),
        "$convert" => validate_convert_expression(value, scope),
        "$getField" => {
            let (field, input) = parse_get_field_spec(value)?;
            validate_expression_with_scope(field, scope)?;
            if let Some(input) = input {
                validate_expression_with_scope(input, scope)?;
            }
            Ok(())
        }
        "$indexOfArray" => {
            let arguments = expression_argument_slice(value)?;
            if !(2..=4).contains(&arguments.len()) {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$indexOfBytes" | "$indexOfCP" => {
            let arguments = expression_argument_slice(value)?;
            if !(2..=4).contains(&arguments.len()) {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$ifNull" => {
            let arguments = expression_argument_slice(value)?;
            if arguments.len() < 2 {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$let" => validate_let_expression(value, scope),
        "$map" => validate_map_expression(value, scope),
        "$rand" => validate_rand_expression(value),
        "$switch" => validate_switch_expression(value, scope),
        "$range" => {
            let arguments = expression_argument_slice(value)?;
            if !(2..=3).contains(&arguments.len()) {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$reduce" => validate_reduce_expression(value, scope),
        "$replaceAll" | "$replaceOne" => validate_replace_expression(value, scope),
        "$reverseArray" => validate_expression_with_scope(unary_expression_operand(value), scope),
        "$sortArray" => validate_sort_array_expression(value, scope),
        "$toBool" | "$toDate" | "$toDecimal" | "$toDouble" | "$toInt" | "$toLong"
        | "$toObjectId" | "$toString" => {
            validate_expression_with_scope(single_expression_operand(value)?, scope)
        }
        "$toLower" | "$toUpper" => {
            validate_expression_with_scope(single_expression_operand(value)?, scope)
        }
        "$setDifference" | "$setIsSubset" => {
            for argument in expression_arguments::<2>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$setEquals" => {
            let arguments = expression_argument_slice(value)?;
            if arguments.len() < 2 {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$setIntersection" | "$setUnion" => {
            for argument in expression_argument_slice(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$setField" => validate_set_field_expression(value, scope, false),
        "$slice" => {
            let arguments = expression_argument_slice(value)?;
            if !(2..=3).contains(&arguments.len()) {
                return Err(QueryError::InvalidStructure);
            }
            for argument in arguments {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$split" => {
            for argument in expression_arguments::<2>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
            Ok(())
        }
        "$zip" => validate_zip_expression(value, scope),
        "$unsetField" => validate_set_field_expression(value, scope, true),
        other => Err(QueryError::UnsupportedOperator(other.to_string())),
    }
}

fn variable_value(
    document: &Document,
    path: &str,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let mut segments = path[2..].splitn(2, '.');
    let name = segments.next().unwrap_or_default();
    let remainder = segments.next();
    validate_user_variable_read_name(name)?;

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
            .ok_or_else(|| QueryError::InvalidArgument(format!("undefined variable `{name}`")))?,
    };

    match remainder {
        Some(path) => match source {
            EvaluatedExpression::Value(Bson::Document(document)) => {
                Ok(lookup_path_owned(&document, path)
                    .map(EvaluatedExpression::Value)
                    .unwrap_or(EvaluatedExpression::Missing))
            }
            _ => Ok(EvaluatedExpression::Missing),
        },
        None => Ok(source),
    }
}

fn field_path_value(
    document: &Document,
    path: &str,
    variables: &BTreeMap<String, Bson>,
) -> EvaluatedExpression {
    let current = variables
        .get("CURRENT")
        .cloned()
        .unwrap_or_else(|| Bson::Document(document.clone()));

    match current {
        Bson::Document(current) => lookup_path_owned(&current, &path[1..])
            .map(EvaluatedExpression::Value)
            .unwrap_or(EvaluatedExpression::Missing),
        _ => EvaluatedExpression::Missing,
    }
}

fn validate_variable_reference(path: &str, _scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let mut segments = path[2..].splitn(2, '.');
    let name = segments.next().unwrap_or_default();
    validate_user_variable_read_name(name)?;
    Ok(())
}

fn validate_user_variable_write_name(name: &str) -> Result<(), QueryError> {
    if name == "CURRENT" {
        return Ok(());
    }
    if !valid_user_variable_write_name(name) {
        return Err(QueryError::InvalidArgument(format!(
            "invalid variable name `{name}`"
        )));
    }
    Ok(())
}

fn validate_user_variable_read_name(name: &str) -> Result<(), QueryError> {
    if !valid_user_variable_read_name(name) {
        return Err(QueryError::InvalidArgument(format!(
            "invalid variable name `{name}`"
        )));
    }
    Ok(())
}

fn valid_user_variable_write_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_lowercase() || !first.is_ascii()) && chars.all(valid_variable_tail_char)
}

fn valid_user_variable_read_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || !first.is_ascii()) && chars.all(valid_variable_tail_char)
}

fn valid_variable_tail_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii()
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

fn single_expression_operand(value: &Bson) -> Result<&Bson, QueryError> {
    match value {
        Bson::Array(arguments) if arguments.len() == 1 => Ok(&arguments[0]),
        Bson::Array(_) => Err(QueryError::InvalidStructure),
        _ => Ok(value),
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

fn eval_nullable_unary_math_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operation: impl FnOnce(f64) -> Result<f64, QueryError>,
) -> Result<EvaluatedExpression, QueryError> {
    let Some(number) = eval_nullable_numeric_expression(document, value, variables)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    Ok(EvaluatedExpression::Value(number_bson(operation(number)?)))
}

fn eval_nullable_binary_math_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operation: impl FnOnce(f64, f64) -> Result<f64, QueryError>,
) -> Result<EvaluatedExpression, QueryError> {
    let [left, right] = expression_arguments::<2>(value)?;
    let Some(left) = eval_nullable_numeric_operand(document, left, variables)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let Some(right) = eval_nullable_numeric_operand(document, right, variables)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    Ok(EvaluatedExpression::Value(number_bson(operation(
        left, right,
    )?)))
}

fn eval_numeric_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<f64, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    numeric_value(&value)
}

fn eval_nullable_numeric_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<f64>, QueryError> {
    let operand = single_expression_operand(value)?;
    eval_nullable_numeric_operand(document, operand, variables)
}

fn eval_nullable_numeric_operand(
    document: &Document,
    operand: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<Option<f64>, QueryError> {
    let value = eval_expression_result_with_variables(document, operand, variables)?;
    match value {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(None)
        }
        EvaluatedExpression::Value(value) => numeric_value(&value).map(Some),
    }
}

fn eval_log_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    eval_nullable_binary_math_expression(document, value, variables, |argument, base| {
        if !(argument.is_nan() || argument > 0.0) {
            return Err(QueryError::InvalidArgument(
                "$log's argument must be a positive number".to_string(),
            ));
        }
        if !(base.is_nan() || (base > 0.0 && base != 1.0)) {
            return Err(QueryError::InvalidArgument(
                "$log's base must be a positive number not equal to 1".to_string(),
            ));
        }
        Ok(argument.log(base))
    })
}

fn eval_pow_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    eval_nullable_binary_math_expression(document, value, variables, |base, exponent| {
        if base == 0.0 && exponent < 0.0 {
            return Err(QueryError::InvalidArgument(
                "$pow cannot take a base of 0 and a negative exponent".to_string(),
            ));
        }
        Ok(base.powf(exponent))
    })
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

fn eval_let_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut vars_spec = None;
    let mut in_expression = None;

    for (field, value) in spec {
        match field.as_str() {
            "vars" => vars_spec = Some(value.as_document().ok_or(QueryError::InvalidStructure)?),
            "in" => in_expression = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let vars_spec = vars_spec.ok_or(QueryError::InvalidStructure)?;
    let in_expression = in_expression.ok_or(QueryError::InvalidStructure)?;

    let mut scoped = variables.clone();
    for (name, expression) in vars_spec {
        validate_user_variable_write_name(name)?;
        let value = eval_expression_result_with_variables(document, expression, variables)?;
        scoped.insert(name.clone(), materialize_variable_value(value));
    }

    eval_expression_result_with_variables(document, in_expression, &scoped)
}

fn eval_map_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut var_name = None;
    let mut in_expression = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "as" => var_name = Some(value.as_str().ok_or(QueryError::InvalidStructure)?),
            "in" => in_expression = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let input = input.ok_or(QueryError::InvalidStructure)?;
    let in_expression = in_expression.ok_or(QueryError::InvalidStructure)?;
    let var_name = var_name.unwrap_or("this");
    validate_user_variable_write_name(var_name)?;

    let input = eval_expression_result_with_variables(document, input, variables)?;
    if input.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(Bson::Array(items)) = input else {
        return Err(QueryError::InvalidArgument(
            "$map input must evaluate to an array".to_string(),
        ));
    };

    let mut mapped = Vec::with_capacity(items.len());
    for item in items {
        let mut scoped = variables.clone();
        scoped.insert(var_name.to_string(), item.clone());
        let value = eval_expression_result_with_variables(document, in_expression, &scoped)?;
        mapped.push(value.into_bson_or_null());
    }

    Ok(EvaluatedExpression::Value(Bson::Array(mapped)))
}

fn eval_filter_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut var_name = None;
    let mut condition = None;
    let mut limit = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "as" => var_name = Some(value.as_str().ok_or(QueryError::InvalidStructure)?),
            "cond" => condition = Some(value),
            "limit" => limit = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let input = input.ok_or(QueryError::InvalidStructure)?;
    let condition = condition.ok_or(QueryError::InvalidStructure)?;
    let var_name = var_name.unwrap_or("this");
    validate_user_variable_write_name(var_name)?;

    let limit = match limit {
        Some(limit) => Some(parse_filter_limit(document, limit, variables)?),
        None => None,
    };

    let input = eval_expression_result_with_variables(document, input, variables)?;
    if input.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(Bson::Array(items)) = input else {
        return Err(QueryError::InvalidArgument(
            "$filter input must evaluate to an array".to_string(),
        ));
    };

    let mut filtered = Vec::new();
    for item in items {
        let mut scoped = variables.clone();
        scoped.insert(var_name.to_string(), item.clone());
        let include = eval_expression_result_with_variables(document, condition, &scoped)?
            .into_bson_or_null();
        if expression_truthy(&include) {
            filtered.push(item);
            if limit.is_some_and(|limit| filtered.len() >= limit) {
                break;
            }
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Array(filtered)))
}

fn eval_get_field_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (field, input) = parse_get_field_spec(value)?;
    let field = eval_expression_result_with_variables(document, field, variables)?;
    let field = match field {
        EvaluatedExpression::Value(Bson::String(field) | Bson::Symbol(field)) => field,
        _ => {
            return Err(QueryError::InvalidArgument(
                "$getField requires `field` to evaluate to a string".to_string(),
            ));
        }
    };

    let input = match input {
        Some(input) => eval_expression_result_with_variables(document, input, variables)?,
        None => EvaluatedExpression::Value(
            variables
                .get("CURRENT")
                .cloned()
                .unwrap_or_else(|| Bson::Document(document.clone())),
        ),
    };

    match input {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Document(document)) => Ok(document
            .get(&field)
            .cloned()
            .map(EvaluatedExpression::Value)
            .unwrap_or(EvaluatedExpression::Missing)),
        EvaluatedExpression::Value(_) => Ok(EvaluatedExpression::Missing),
    }
}

fn eval_set_field_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    unset: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let (field, input, assigned) = parse_set_field_spec(value, unset)?;
    let field = constant_field_name(field)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;

    match input {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Document(mut input)) => {
            let remove = unset || assigned.is_some_and(is_remove_expression);
            if remove {
                input.remove(&field);
            } else {
                let value = eval_expression_with_variables(
                    document,
                    assigned.expect("setField value is present"),
                    variables,
                )?;
                input.insert(field, value);
            }
            Ok(EvaluatedExpression::Value(Bson::Document(input)))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$setField input must evaluate to an object or null".to_string(),
        )),
    }
}

fn eval_index_of_array_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if !(2..=4).contains(&arguments.len()) {
        return Err(QueryError::InvalidStructure);
    }

    let array = eval_expression_result_with_variables(document, &arguments[0], variables)?;
    if array.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(Bson::Array(items)) = array else {
        return Err(QueryError::InvalidArgument(
            "$indexOfArray requires an array input".to_string(),
        ));
    };

    let needle = eval_expression_with_variables(document, &arguments[1], variables)?;
    let start = match arguments.get(2) {
        Some(start) => parse_non_negative_index(document, start, variables, "$indexOfArray")?,
        None => 0,
    };
    let end = match arguments.get(3) {
        Some(end) => parse_non_negative_index(document, end, variables, "$indexOfArray")?,
        None => items.len(),
    };

    if start >= items.len() || start >= end {
        return Ok(EvaluatedExpression::Value(Bson::Int64(-1)));
    }

    let end = end.min(items.len());
    for (index, item) in items[start..end].iter().enumerate() {
        if compare_bson(item, &needle).is_eq() {
            return Ok(EvaluatedExpression::Value(Bson::Int64(
                (start + index) as i64,
            )));
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Int64(-1)))
}

fn eval_index_of_string_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    code_points: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if code_points {
        "$indexOfCP"
    } else {
        "$indexOfBytes"
    };
    let arguments = expression_argument_slice(value)?;
    if !(2..=4).contains(&arguments.len()) {
        return Err(QueryError::InvalidStructure);
    }

    let input = eval_expression_result_with_variables(document, &arguments[0], variables)?;
    let input = match input {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            return Ok(EvaluatedExpression::Value(Bson::Null));
        }
        EvaluatedExpression::Value(Bson::String(value) | Bson::Symbol(value)) => value,
        EvaluatedExpression::Value(_) => {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires a string as the first argument"
            )));
        }
    };

    let token = eval_expression_result_with_variables(document, &arguments[1], variables)?;
    let token = match token {
        EvaluatedExpression::Value(Bson::String(value) | Bson::Symbol(value)) => value,
        _ => {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires a string as the second argument"
            )));
        }
    };

    let start = match arguments.get(2) {
        Some(start) => parse_non_negative_index(document, start, variables, operator)?,
        None => 0,
    };
    let end = match arguments.get(3) {
        Some(end) => Some(parse_non_negative_index(
            document, end, variables, operator,
        )?),
        None => None,
    };

    let index = if code_points {
        index_of_code_points(&input, &token, start, end)
    } else {
        index_of_bytes(&input, &token, start, end)
    };
    Ok(EvaluatedExpression::Value(Bson::Int64(index)))
}

fn eval_range_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if !(2..=3).contains(&arguments.len()) {
        return Err(QueryError::InvalidStructure);
    }

    let start = parse_i32_bound(document, &arguments[0], variables, "$range")?;
    let end = parse_i32_bound(document, &arguments[1], variables, "$range")?;
    let step = match arguments.get(2) {
        Some(step) => parse_i32_bound(document, step, variables, "$range")?,
        None => 1,
    };
    if step == 0 {
        return Err(QueryError::InvalidArgument(
            "$range requires a non-zero step".to_string(),
        ));
    }

    let mut values = Vec::new();
    let mut current = start;
    if step > 0 {
        while current < end {
            values.push(Bson::Int32(current));
            current = current.checked_add(step).ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$range overflowed while materializing output".to_string(),
                )
            })?;
        }
    } else {
        while current > end {
            values.push(Bson::Int32(current));
            current = current.checked_add(step).ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$range overflowed while materializing output".to_string(),
                )
            })?;
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Array(values)))
}

fn eval_reduce_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (input, initial_value, in_expression) = parse_reduce_spec(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    if input.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let EvaluatedExpression::Value(Bson::Array(items)) = input else {
        return Err(QueryError::InvalidArgument(
            "$reduce input must evaluate to an array".to_string(),
        ));
    };

    let mut accumulator = eval_expression_with_variables(document, initial_value, variables)?;
    for item in items {
        let mut scoped = variables.clone();
        scoped.insert("this".to_string(), item);
        scoped.insert("value".to_string(), accumulator);
        accumulator = eval_expression_with_variables(document, in_expression, &scoped)?;
    }

    Ok(EvaluatedExpression::Value(accumulator))
}

fn eval_bitwise_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    op: fn(i64, i64) -> i64,
    identity: i64,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = match value {
        Bson::Array(arguments) => arguments.as_slice(),
        _ => std::slice::from_ref(value),
    };

    let mut result = identity;
    for argument in arguments {
        let operand = eval_expression_with_variables(document, argument, variables)?;
        let operand = bitwise_i64(&operand)?;
        result = op(result, operand);
    }

    Ok(EvaluatedExpression::Value(Bson::Int64(result)))
}

fn eval_bit_not_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let operand = single_expression_operand(value)?;
    let operand = eval_expression_with_variables(document, operand, variables)?;
    Ok(EvaluatedExpression::Value(Bson::Int64(!bitwise_i64(
        &operand,
    )?)))
}

fn eval_set_difference_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let [left, right] = expression_arguments::<2>(value)?;
    let Some(left) = eval_set_operand(document, left, variables, "$setDifference", true)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let Some(right) = eval_set_operand(document, right, variables, "$setDifference", true)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    let right = unique_bson_values(&right);
    let mut result = Vec::new();
    for item in unique_bson_values(&left) {
        if !bson_array_contains(&right, &item) {
            result.push(item);
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Array(result)))
}

fn eval_set_equals_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if arguments.len() < 2 {
        return Err(QueryError::InvalidStructure);
    }

    let mut sets = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let values = eval_set_operand(document, argument, variables, "$setEquals", false)?
            .expect("non-nullish set operand");
        sets.push(unique_bson_values(&values));
    }

    let first = &sets[0];
    let equals = sets[1..].iter().all(|candidate| {
        first.len() == candidate.len()
            && first
                .iter()
                .all(|value| bson_array_contains(candidate, value))
    });
    Ok(EvaluatedExpression::Value(Bson::Boolean(equals)))
}

fn eval_set_intersection_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if arguments.is_empty() {
        return Ok(EvaluatedExpression::Value(Bson::Array(Vec::new())));
    }

    let mut sets = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let Some(values) =
            eval_set_operand(document, argument, variables, "$setIntersection", true)?
        else {
            return Ok(EvaluatedExpression::Value(Bson::Null));
        };
        sets.push(unique_bson_values(&values));
    }

    let mut result = Vec::new();
    for item in &sets[0] {
        if sets[1..]
            .iter()
            .all(|candidate| bson_array_contains(candidate, item))
        {
            result.push(item.clone());
        }
    }
    Ok(EvaluatedExpression::Value(Bson::Array(result)))
}

fn eval_set_is_subset_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let [left, right] = expression_arguments::<2>(value)?;
    let left = eval_set_operand(document, left, variables, "$setIsSubset", false)?
        .expect("non-nullish set operand");
    let right = eval_set_operand(document, right, variables, "$setIsSubset", false)?
        .expect("non-nullish set operand");

    let left = unique_bson_values(&left);
    let right = unique_bson_values(&right);
    Ok(EvaluatedExpression::Value(Bson::Boolean(
        left.iter().all(|value| bson_array_contains(&right, value)),
    )))
}

fn eval_set_union_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    let mut result = Vec::new();
    for argument in arguments {
        let Some(values) = eval_set_operand(document, argument, variables, "$setUnion", true)?
        else {
            return Ok(EvaluatedExpression::Value(Bson::Null));
        };
        for item in values {
            push_unique_bson(&mut result, item);
        }
    }
    Ok(EvaluatedExpression::Value(Bson::Array(result)))
}

fn eval_reverse_array_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    match eval_expression_result_with_variables(
        document,
        unary_expression_operand(value),
        variables,
    )? {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Array(mut items)) => {
            items.reverse();
            Ok(EvaluatedExpression::Value(Bson::Array(items)))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$reverseArray requires an array input".to_string(),
        )),
    }
}

fn eval_slice_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let arguments = expression_argument_slice(value)?;
    if !(2..=3).contains(&arguments.len()) {
        return Err(QueryError::InvalidStructure);
    }

    let array = eval_expression_result_with_variables(document, &arguments[0], variables)?;
    if array.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let EvaluatedExpression::Value(Bson::Array(items)) = array else {
        return Err(QueryError::InvalidArgument(
            "$slice requires an array input".to_string(),
        ));
    };

    if arguments.len() == 2 {
        let count = parse_i32_bound(document, &arguments[1], variables, "$slice")?;
        let len = items.len() as i32;
        let (start, end) = if count >= 0 {
            (0, count.min(len))
        } else {
            let count = count.unsigned_abs() as i32;
            ((len - count).max(0), len)
        };
        return Ok(EvaluatedExpression::Value(Bson::Array(
            items[start as usize..end as usize].to_vec(),
        )));
    }

    let position = parse_i32_bound(document, &arguments[1], variables, "$slice")?;
    let count = parse_i32_bound(document, &arguments[2], variables, "$slice")?;
    if count <= 0 {
        return Err(QueryError::InvalidArgument(
            "$slice requires a positive count in the three-argument form".to_string(),
        ));
    }

    let len = items.len() as i32;
    let start = if position >= 0 {
        position.min(len)
    } else {
        (len + position).max(0)
    };
    let end = (start + count).min(len);
    Ok(EvaluatedExpression::Value(Bson::Array(
        items[start as usize..end as usize].to_vec(),
    )))
}

fn eval_split_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let [input, separator] = expression_arguments::<2>(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    let separator = eval_expression_result_with_variables(document, separator, variables)?;

    if input.is_nullish() || separator.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let input = require_string_operand(input, "$split")?;
    let separator = require_string_operand(separator, "$split")?;
    if separator.is_empty() {
        return Err(QueryError::InvalidArgument(
            "$split requires a non-empty separator".to_string(),
        ));
    }

    Ok(EvaluatedExpression::Value(Bson::Array(
        input
            .split(&separator)
            .map(|segment| Bson::String(segment.to_string()))
            .collect(),
    )))
}

fn eval_replace_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    replace_all: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if replace_all {
        "$replaceAll"
    } else {
        "$replaceOne"
    };
    let (input, find, replacement) = parse_replace_spec(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    let find = eval_expression_result_with_variables(document, find, variables)?;
    let replacement = eval_expression_result_with_variables(document, replacement, variables)?;

    if input.is_nullish() || find.is_nullish() || replacement.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let input = require_named_string_operand(input, operator, "input")?;
    let find = require_named_string_operand(find, operator, "find")?;
    let replacement = require_named_string_operand(replacement, operator, "replacement")?;

    let result = if replace_all {
        input.replace(&find, &replacement)
    } else {
        input.replacen(&find, &replacement, 1)
    };
    Ok(EvaluatedExpression::Value(Bson::String(result)))
}

fn eval_rand_expression(value: &Bson) -> Result<EvaluatedExpression, QueryError> {
    validate_rand_expression(value)?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    Ok(EvaluatedExpression::Value(Bson::Double(
        nanos as f64 / 1_000_000_000.0,
    )))
}

#[derive(Clone, Copy)]
enum ConvertTarget {
    Bool,
    Date,
    Decimal,
    Double,
    Int,
    Long,
    ObjectId,
    String,
}

struct ConvertSpec<'a> {
    input: &'a Bson,
    to: &'a Bson,
    on_error: Option<&'a Bson>,
    on_null: Option<&'a Bson>,
}

fn eval_convert_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = parse_convert_spec(value)?;
    eval_convert_with_target_expression(
        document,
        spec.input,
        spec.to,
        spec.on_error,
        spec.on_null,
        variables,
    )
}

fn eval_convert_alias_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    target: ConvertTarget,
) -> Result<EvaluatedExpression, QueryError> {
    let input = single_expression_operand(value)?;
    eval_convert_with_static_target(document, input, target, None, None, variables)
}

fn eval_convert_with_target_expression(
    document: &Document,
    input_expression: &Bson,
    target_expression: &Bson,
    on_error: Option<&Bson>,
    on_null: Option<&Bson>,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let input = eval_expression_result_with_variables(document, input_expression, variables)?;
    if input.is_nullish() {
        return match on_null {
            Some(on_null) => eval_expression_result_with_variables(document, on_null, variables),
            None => Ok(EvaluatedExpression::Value(Bson::Null)),
        };
    }

    let target = eval_expression_result_with_variables(document, target_expression, variables)?;
    let Some(target) = parse_convert_target_expression(target)? else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    eval_convert_result(document, input, target, on_error, variables)
}

fn eval_convert_with_static_target(
    document: &Document,
    input_expression: &Bson,
    target: ConvertTarget,
    on_error: Option<&Bson>,
    on_null: Option<&Bson>,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let input = eval_expression_result_with_variables(document, input_expression, variables)?;
    if input.is_nullish() {
        return match on_null {
            Some(on_null) => eval_expression_result_with_variables(document, on_null, variables),
            None => Ok(EvaluatedExpression::Value(Bson::Null)),
        };
    }

    eval_convert_result(document, input, target, on_error, variables)
}

fn eval_convert_result(
    document: &Document,
    input: EvaluatedExpression,
    target: ConvertTarget,
    on_error: Option<&Bson>,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let EvaluatedExpression::Value(input) = input else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    match convert_bson_value(&input, target) {
        Ok(value) => Ok(EvaluatedExpression::Value(value)),
        Err(error) => match on_error {
            Some(on_error) => eval_expression_result_with_variables(document, on_error, variables),
            None => Err(error),
        },
    }
}

fn parse_convert_target_expression(
    target: EvaluatedExpression,
) -> Result<Option<ConvertTarget>, QueryError> {
    match target {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(None)
        }
        EvaluatedExpression::Value(value) => parse_convert_target_value(&value).map(Some),
    }
}

fn parse_convert_target_value(value: &Bson) -> Result<ConvertTarget, QueryError> {
    match value {
        Bson::String(value) | Bson::Symbol(value) => match value.as_str() {
            "bool" | "boolean" => Ok(ConvertTarget::Bool),
            "date" => Ok(ConvertTarget::Date),
            "decimal" => Ok(ConvertTarget::Decimal),
            "double" => Ok(ConvertTarget::Double),
            "int" => Ok(ConvertTarget::Int),
            "long" => Ok(ConvertTarget::Long),
            "objectId" => Ok(ConvertTarget::ObjectId),
            "string" => Ok(ConvertTarget::String),
            other => Err(QueryError::InvalidArgument(format!(
                "$convert target type `{other}` is not supported"
            ))),
        },
        Bson::Document(spec) => {
            let Some(target_type) = spec.get("type") else {
                return Err(QueryError::InvalidStructure);
            };
            if spec.keys().any(|key| key != "type") {
                return Err(QueryError::InvalidArgument(
                    "$convert target objects only support `type` in mqlite".to_string(),
                ));
            }
            parse_convert_target_value(target_type)
        }
        _ => {
            let Some(code) = integer_value(value) else {
                return Err(QueryError::InvalidArgument(
                    "$convert requires `to` to evaluate to a string, integer type code, or { type } document"
                        .to_string(),
                ));
            };
            match code {
                1 => Ok(ConvertTarget::Double),
                2 => Ok(ConvertTarget::String),
                7 => Ok(ConvertTarget::ObjectId),
                8 => Ok(ConvertTarget::Bool),
                9 => Ok(ConvertTarget::Date),
                16 => Ok(ConvertTarget::Int),
                18 => Ok(ConvertTarget::Long),
                19 => Ok(ConvertTarget::Decimal),
                _ => Err(QueryError::InvalidArgument(format!(
                    "$convert target type code `{code}` is not supported"
                ))),
            }
        }
    }
}

fn convert_bson_value(value: &Bson, target: ConvertTarget) -> Result<Bson, QueryError> {
    match target {
        ConvertTarget::Bool => Ok(Bson::Boolean(convert_to_bool(value))),
        ConvertTarget::Date => Ok(Bson::DateTime(convert_to_date(value)?)),
        ConvertTarget::Decimal => Ok(Bson::Decimal128(convert_to_decimal128(value)?)),
        ConvertTarget::Double => Ok(Bson::Double(convert_to_double(value)?)),
        ConvertTarget::Int => Ok(Bson::Int32(convert_to_i32(value)?)),
        ConvertTarget::Long => Ok(Bson::Int64(convert_to_i64(value)?)),
        ConvertTarget::ObjectId => Ok(Bson::ObjectId(convert_to_object_id(value)?)),
        ConvertTarget::String => Ok(Bson::String(convert_to_string(value)?)),
    }
}

fn convert_to_bool(value: &Bson) -> bool {
    match value {
        Bson::Boolean(value) => *value,
        Bson::Int32(value) => *value != 0,
        Bson::Int64(value) => *value != 0,
        Bson::Double(value) => *value != 0.0 || value.is_nan(),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .map(|value| value != 0.0 || value.is_nan())
            .unwrap_or(true),
        _ => true,
    }
}

fn convert_to_date(value: &Bson) -> Result<bson::DateTime, QueryError> {
    match value {
        Bson::DateTime(value) => Ok(value.to_owned()),
        Bson::Int32(value) => Ok(bson::DateTime::from_millis(*value as i64)),
        Bson::Int64(value) => Ok(bson::DateTime::from_millis(*value)),
        Bson::Double(value) => Ok(bson::DateTime::from_millis(truncate_f64_for_convert(
            *value, "date",
        )?)),
        Bson::Decimal128(value) => Ok(bson::DateTime::from_millis(truncate_decimal_for_convert(
            value, "date",
        )?)),
        Bson::String(value) | Bson::Symbol(value) => {
            bson::DateTime::parse_rfc3339_str(value).map_err(|_| conversion_failure("date"))
        }
        Bson::ObjectId(value) => Ok(value.timestamp()),
        Bson::Timestamp(value) => Ok(bson::DateTime::from_millis((value.time as i64) * 1_000)),
        _ => Err(conversion_failure("date")),
    }
}

fn convert_to_decimal128(value: &Bson) -> Result<Decimal128, QueryError> {
    let text = match value {
        Bson::Decimal128(value) => return Ok(value.to_owned()),
        Bson::Int32(value) => value.to_string(),
        Bson::Int64(value) => value.to_string(),
        Bson::Double(value) => format_f64_for_conversion(*value),
        Bson::Boolean(value) => {
            if *value {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Bson::DateTime(value) => value.timestamp_millis().to_string(),
        Bson::String(value) | Bson::Symbol(value) => value.clone(),
        _ => return Err(conversion_failure("decimal")),
    };
    Decimal128::from_str(&text).map_err(|_| conversion_failure("decimal"))
}

fn convert_to_double(value: &Bson) -> Result<f64, QueryError> {
    match value {
        Bson::Double(value) => Ok(*value),
        Bson::Int32(value) => Ok(*value as f64),
        Bson::Int64(value) => Ok(*value as f64),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .map_err(|_| conversion_failure("double")),
        Bson::Boolean(value) => Ok(if *value { 1.0 } else { 0.0 }),
        Bson::DateTime(value) => Ok(value.timestamp_millis() as f64),
        Bson::String(value) | Bson::Symbol(value) => {
            parse_f64_for_conversion(value).ok_or_else(|| conversion_failure("double"))
        }
        _ => Err(conversion_failure("double")),
    }
}

fn convert_to_i32(value: &Bson) -> Result<i32, QueryError> {
    match value {
        Bson::Int32(value) => Ok(*value),
        Bson::Int64(value) => i32::try_from(*value).map_err(|_| conversion_failure("int")),
        Bson::Double(value) => i32::try_from(truncate_f64_for_convert(*value, "int")?)
            .map_err(|_| conversion_failure("int")),
        Bson::Decimal128(value) => i32::try_from(truncate_decimal_for_convert(value, "int")?)
            .map_err(|_| conversion_failure("int")),
        Bson::Boolean(value) => Ok(if *value { 1 } else { 0 }),
        Bson::String(value) | Bson::Symbol(value) => {
            value.parse::<i32>().map_err(|_| conversion_failure("int"))
        }
        _ => Err(conversion_failure("int")),
    }
}

fn convert_to_i64(value: &Bson) -> Result<i64, QueryError> {
    match value {
        Bson::Int32(value) => Ok(*value as i64),
        Bson::Int64(value) => Ok(*value),
        Bson::Double(value) => truncate_f64_for_convert(*value, "long"),
        Bson::Decimal128(value) => truncate_decimal_for_convert(value, "long"),
        Bson::Boolean(value) => Ok(if *value { 1 } else { 0 }),
        Bson::DateTime(value) => Ok(value.timestamp_millis()),
        Bson::String(value) | Bson::Symbol(value) => {
            value.parse::<i64>().map_err(|_| conversion_failure("long"))
        }
        _ => Err(conversion_failure("long")),
    }
}

fn convert_to_object_id(value: &Bson) -> Result<ObjectId, QueryError> {
    match value {
        Bson::ObjectId(value) => Ok(value.to_owned()),
        Bson::String(value) | Bson::Symbol(value) => {
            ObjectId::parse_str(value).map_err(|_| conversion_failure("objectId"))
        }
        _ => Err(conversion_failure("objectId")),
    }
}

fn convert_to_string(value: &Bson) -> Result<String, QueryError> {
    match value {
        Bson::Document(_) | Bson::Array(_) => stringify_nested_json(value),
        _ => stringify_scalar_for_conversion(value),
    }
}

fn truncate_f64_for_convert(value: f64, target: &str) -> Result<i64, QueryError> {
    if value.is_nan() || value.is_infinite() {
        return Err(conversion_failure(target));
    }
    truncate_f64_to_i64(value).ok_or_else(|| conversion_failure(target))
}

fn truncate_decimal_for_convert(value: &Decimal128, target: &str) -> Result<i64, QueryError> {
    let parsed = value
        .to_string()
        .parse::<f64>()
        .map_err(|_| conversion_failure(target))?;
    truncate_f64_for_convert(parsed, target)
}

fn parse_f64_for_conversion(value: &str) -> Option<f64> {
    match value {
        "Infinity" | "+Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        "NaN" | "+NaN" | "-NaN" => Some(f64::NAN),
        _ => value.parse::<f64>().ok(),
    }
}

fn format_f64_for_conversion(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        }
    } else {
        value.to_string()
    }
}

fn conversion_failure(target: &str) -> QueryError {
    QueryError::InvalidArgument(format!("$convert failed to convert input to {target}"))
}

fn stringify_scalar_for_conversion(value: &Bson) -> Result<String, QueryError> {
    match value {
        Bson::Null | Bson::Undefined => Ok("null".to_string()),
        Bson::Boolean(value) => Ok(if *value { "true" } else { "false" }.to_string()),
        Bson::Int32(value) => Ok(value.to_string()),
        Bson::Int64(value) => Ok(value.to_string()),
        Bson::Double(value) => Ok(format_f64_for_conversion(*value)),
        Bson::Decimal128(value) => Ok(value.to_string()),
        Bson::String(value) | Bson::Symbol(value) => Ok(value.clone()),
        Bson::DateTime(value) => {
            let value = DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
                .ok_or_else(|| conversion_failure("string"))?;
            Ok(value.to_rfc3339_opts(SecondsFormat::Millis, true))
        }
        Bson::ObjectId(value) => Ok(value.to_hex()),
        Bson::Timestamp(value) => Ok(value.to_string()),
        Bson::RegularExpression(value) => Ok(format!("/{}/{}", value.pattern, value.options)),
        Bson::JavaScriptCode(value) => Ok(value.clone()),
        Bson::JavaScriptCodeWithScope(value) => Ok(value.code.clone()),
        Bson::MinKey => Ok("MinKey".to_string()),
        Bson::MaxKey => Ok("MaxKey".to_string()),
        Bson::Binary(value) => Ok(format!("{value:?}")),
        Bson::DbPointer(value) => Ok(format!("{value:?}")),
        Bson::Document(_) | Bson::Array(_) => Err(conversion_failure("string")),
    }
}

fn stringify_nested_json(value: &Bson) -> Result<String, QueryError> {
    let mut output = String::new();
    append_nested_json(&mut output, value)?;
    Ok(output)
}

fn append_nested_json(output: &mut String, value: &Bson) -> Result<(), QueryError> {
    match value {
        Bson::Document(document) => {
            output.push('{');
            for (index, (key, value)) in document.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).expect("json string"));
                output.push(':');
                append_nested_json(output, value)?;
            }
            output.push('}');
            Ok(())
        }
        Bson::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                append_nested_json(output, value)?;
            }
            output.push(']');
            Ok(())
        }
        Bson::Null | Bson::Undefined => {
            output.push_str("null");
            Ok(())
        }
        Bson::Boolean(value) => {
            output.push_str(if *value { "true" } else { "false" });
            Ok(())
        }
        Bson::Int32(value) => {
            output.push_str(&value.to_string());
            Ok(())
        }
        Bson::Int64(value) => {
            output.push_str(&value.to_string());
            Ok(())
        }
        Bson::Double(value) => {
            if value.is_finite() {
                output.push_str(&value.to_string());
            } else {
                output.push_str(
                    &serde_json::to_string(&format_f64_for_conversion(*value))
                        .expect("json string"),
                );
            }
            Ok(())
        }
        Bson::Decimal128(value) => {
            let rendered = value.to_string();
            if rendered.parse::<f64>().is_ok_and(f64::is_finite) {
                output.push_str(&rendered);
            } else {
                output.push_str(&serde_json::to_string(&rendered).expect("json string"));
            }
            Ok(())
        }
        _ => {
            let rendered = stringify_scalar_for_conversion(value)?;
            output.push_str(&serde_json::to_string(&rendered).expect("json string"));
            Ok(())
        }
    }
}

fn parse_convert_spec(value: &Bson) -> Result<ConvertSpec<'_>, QueryError> {
    let Bson::Document(spec) = value else {
        return Err(QueryError::InvalidStructure);
    };

    let mut input = None;
    let mut to = None;
    let mut on_error = None;
    let mut on_null = None;
    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "to" => to = Some(value),
            "onError" => on_error = Some(value),
            "onNull" => on_null = Some(value),
            "base" | "format" | "byteOrder" => {
                return Err(QueryError::InvalidArgument(format!(
                    "$convert field `{field}` is not supported in mqlite"
                )));
            }
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok(ConvertSpec {
        input: input.ok_or(QueryError::InvalidStructure)?,
        to: to.ok_or(QueryError::InvalidStructure)?,
        on_error,
        on_null,
    })
}

fn eval_sort_array_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (input, sort_by) = parse_sort_array_spec(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    if input.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let EvaluatedExpression::Value(Bson::Array(mut items)) = input else {
        return Err(QueryError::InvalidArgument(
            "$sortArray requires an array input".to_string(),
        ));
    };
    if items.len() < 2 {
        return Ok(EvaluatedExpression::Value(Bson::Array(items)));
    }

    match parse_sort_array_order(sort_by)? {
        SortArrayOrder::Scalar(direction) => {
            items.sort_by(|left, right| {
                let ordering = compare_bson(left, right);
                if direction < 0 {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
        }
        SortArrayOrder::Document(sort) => {
            if !items.iter().all(|item| matches!(item, Bson::Document(_))) {
                return Err(QueryError::InvalidArgument(
                    "$sortArray requires document array elements when sortBy is an object"
                        .to_string(),
                ));
            }
            items.sort_by(|left, right| {
                let left = left.as_document().expect("document item");
                let right = right.as_document().expect("document item");
                compare_documents_by_sort(left, right, &sort)
            });
        }
    }

    Ok(EvaluatedExpression::Value(Bson::Array(items)))
}

fn eval_timestamp_part_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    seconds: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if seconds { "$tsSecond" } else { "$tsIncrement" };
    let operand = single_expression_operand(value)?;
    match eval_expression_result_with_variables(document, operand, variables)? {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Timestamp(timestamp)) => {
            let value = if seconds {
                timestamp.time as i64
            } else {
                timestamp.increment as i64
            };
            Ok(EvaluatedExpression::Value(Bson::Int64(value)))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(format!(
            "{operator} requires a timestamp input"
        ))),
    }
}

fn eval_zip_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (inputs, defaults, use_longest_length) = parse_zip_spec(value)?;

    let mut input_values = Vec::with_capacity(inputs.len());
    let mut min_array_size = 0usize;
    let mut max_array_size = 0usize;
    for (index, input) in inputs.iter().enumerate() {
        let input = eval_expression_result_with_variables(document, input, variables)?;
        if input.is_nullish() {
            return Ok(EvaluatedExpression::Value(Bson::Null));
        }

        let EvaluatedExpression::Value(Bson::Array(values)) = input else {
            return Err(QueryError::InvalidArgument(
                "$zip requires array inputs".to_string(),
            ));
        };

        let array_size = values.len();
        if index == 0 {
            min_array_size = array_size;
            max_array_size = array_size;
        } else {
            min_array_size = min_array_size.min(array_size);
            max_array_size = max_array_size.max(array_size);
        }
        input_values.push(values);
    }

    let mut evaluated_defaults = vec![Bson::Null; inputs.len()];
    if min_array_size != max_array_size {
        if let Some(defaults) = defaults {
            for (index, default) in defaults.iter().enumerate() {
                evaluated_defaults[index] =
                    eval_expression_with_variables(document, default, variables)?;
            }
        }
    }

    let output_length = if use_longest_length {
        max_array_size
    } else {
        min_array_size
    };
    let mut output = Vec::with_capacity(output_length);
    for row in 0..output_length {
        let mut zipped = Vec::with_capacity(inputs.len());
        for (column, values) in input_values.iter().enumerate() {
            if let Some(value) = values.get(row) {
                zipped.push(value.clone());
            } else {
                zipped.push(evaluated_defaults[column].clone());
            }
        }
        output.push(Bson::Array(zipped));
    }

    Ok(EvaluatedExpression::Value(Bson::Array(output)))
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

fn eval_switch_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (branches, default) = parse_switch_spec(value)?;
    for (condition, then_expression) in branches {
        let condition = eval_expression_with_variables(document, condition, variables)?;
        if expression_truthy(&condition) {
            return eval_expression_result_with_variables(document, then_expression, variables);
        }
    }

    match default {
        Some(default) => eval_expression_result_with_variables(document, default, variables),
        None => Err(QueryError::InvalidArgument(
            "$switch matched no branch and no default was provided".to_string(),
        )),
    }
}

fn eval_strcasecmp_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let [left, right] = expression_arguments::<2>(value)?;
    let left = coerce_case_string(
        eval_expression_result_with_variables(document, left, variables)?,
        "$strcasecmp",
    )?;
    let right = coerce_case_string(
        eval_expression_result_with_variables(document, right, variables)?,
        "$strcasecmp",
    )?;
    let ordering = left.to_ascii_lowercase().cmp(&right.to_ascii_lowercase());
    Ok(EvaluatedExpression::Value(Bson::Int32(match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    })))
}

fn eval_case_fold_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    uppercase: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if uppercase { "$toUpper" } else { "$toLower" };
    let operand = single_expression_operand(value)?;
    let value = eval_expression_result_with_variables(document, operand, variables)?;
    let value = coerce_case_string(value, operator)?;
    let value = if uppercase {
        value.to_ascii_uppercase()
    } else {
        value.to_ascii_lowercase()
    };
    Ok(EvaluatedExpression::Value(Bson::String(value)))
}

#[derive(Clone, Copy)]
enum TrimType {
    Left,
    Right,
    Both,
}

fn eval_substring_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    code_points: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if code_points {
        "$substrCP"
    } else {
        "$substrBytes"
    };
    let [input, start, length] = expression_arguments::<3>(value)?;

    let input = eval_expression_result_with_variables(document, input, variables)?;
    let input = coerce_substring_input(input)?;

    if code_points {
        let start = parse_non_negative_i32_bound(document, start, variables, operator)?;
        let length = parse_non_negative_i32_bound(document, length, variables, operator)?;
        let boundaries = code_point_boundaries(&input);
        let code_point_len = boundaries.len() - 1;
        if start as usize >= code_point_len {
            return Ok(EvaluatedExpression::Value(Bson::String(String::new())));
        }

        let start = start as usize;
        let end = (start + length as usize).min(code_point_len);
        let substring = input[boundaries[start]..boundaries[end]].to_string();
        return Ok(EvaluatedExpression::Value(Bson::String(substring)));
    }

    let start = parse_non_negative_truncating_index(document, start, variables, operator)?;
    let length = parse_truncating_length(document, length, variables, operator)?;
    if start >= input.len() {
        return Ok(EvaluatedExpression::Value(Bson::String(String::new())));
    }
    validate_utf8_byte_boundary(&input, start, operator, true)?;

    let end = match length {
        Some(length) => start.saturating_add(length).min(input.len()),
        None => input.len(),
    };
    validate_utf8_byte_boundary(&input, end, operator, false)?;
    Ok(EvaluatedExpression::Value(Bson::String(
        input[start..end].to_string(),
    )))
}

fn eval_trim_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    trim_type: TrimType,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = match trim_type {
        TrimType::Left => "$ltrim",
        TrimType::Right => "$rtrim",
        TrimType::Both => "$trim",
    };
    let (input, chars) = parse_trim_spec(value)?;

    let input = eval_expression_result_with_variables(document, input, variables)?;
    if input.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let input = require_trim_string(input, operator, "input")?;

    let trimmed = match chars {
        Some(chars) => {
            let chars = eval_expression_result_with_variables(document, chars, variables)?;
            if chars.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            let chars = require_trim_string(chars, operator, "chars")?;
            if chars.len() > 4096 {
                return Err(QueryError::InvalidArgument(format!(
                    "{operator} requires `chars` to be at most 4096 bytes"
                )));
            }
            let characters = chars.chars().collect::<BTreeSet<_>>();
            trim_with_characters(&input, &characters, trim_type).to_string()
        }
        None => trim_whitespace(&input, trim_type).to_string(),
    };

    Ok(EvaluatedExpression::Value(Bson::String(trimmed)))
}

fn validate_replace_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (input, find, replacement) = parse_replace_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    validate_expression_with_scope(find, scope)?;
    validate_expression_with_scope(replacement, scope)
}

fn validate_sort_array_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let (input, sort_by) = parse_sort_array_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    match sort_by {
        Bson::Document(sort) => validate_sort_spec(sort).map_err(|_| QueryError::InvalidStructure),
        _ if integer_value(sort_by).is_some_and(|direction| matches!(direction, 1 | -1)) => Ok(()),
        _ => Err(QueryError::InvalidStructure),
    }
}

fn validate_zip_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (inputs, defaults, _) = parse_zip_spec(value)?;
    for input in inputs {
        validate_expression_with_scope(input, scope)?;
    }
    if let Some(defaults) = defaults {
        for default in defaults {
            validate_expression_with_scope(default, scope)?;
        }
    }
    Ok(())
}

fn validate_rand_expression(value: &Bson) -> Result<(), QueryError> {
    match value {
        Bson::Document(spec) if spec.is_empty() => Ok(()),
        Bson::Array(arguments) if arguments.is_empty() => Ok(()),
        _ => Err(QueryError::InvalidStructure),
    }
}

fn validate_convert_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let spec = parse_convert_spec(value)?;
    validate_expression_with_scope(spec.input, scope)?;
    validate_expression_with_scope(spec.to, scope)?;
    if let Some(on_error) = spec.on_error {
        validate_expression_with_scope(on_error, scope)?;
    }
    if let Some(on_null) = spec.on_null {
        validate_expression_with_scope(on_null, scope)?;
    }
    Ok(())
}

fn index_of_bytes(input: &str, token: &str, start: usize, end: Option<usize>) -> i64 {
    let haystack = input.as_bytes();
    let needle = token.as_bytes();
    let end = end.unwrap_or(haystack.len()).min(haystack.len());
    if start > haystack.len() || end < start {
        return -1;
    }
    if needle.is_empty() {
        return start as i64;
    }
    if needle.len() > end.saturating_sub(start) {
        return -1;
    }

    for byte_index in start..=end - needle.len() {
        if &haystack[byte_index..byte_index + needle.len()] == needle {
            return byte_index as i64;
        }
    }
    -1
}

fn index_of_code_points(input: &str, token: &str, start: usize, end: Option<usize>) -> i64 {
    let boundaries = code_point_boundaries(input);
    let code_point_len = boundaries.len() - 1;
    if start > code_point_len {
        return -1;
    }

    let end = end.unwrap_or(code_point_len).min(code_point_len);
    if end < start {
        return -1;
    }
    if start == 0 && input.is_empty() && token.is_empty() {
        return 0;
    }

    let needle = token.as_bytes();
    let haystack = input.as_bytes();
    for (code_point_index, byte_index) in boundaries.iter().enumerate().take(end).skip(start) {
        let byte_index = *byte_index;
        if haystack[byte_index..].starts_with(needle) {
            return code_point_index as i64;
        }
    }
    -1
}

fn code_point_boundaries(input: &str) -> Vec<usize> {
    let mut boundaries = input
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    boundaries.push(input.len());
    boundaries
}

fn eval_string_length_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    code_points: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = if code_points {
        "$strLenCP"
    } else {
        "$strLenBytes"
    };
    let operand = single_expression_operand(value)?;
    let value = eval_expression_result_with_variables(document, operand, variables)?;
    let string = require_string_operand(value, operator)?;
    let length = if code_points {
        string.chars().count() as i64
    } else {
        string.len() as i64
    };
    Ok(EvaluatedExpression::Value(Bson::Int64(length)))
}

fn validate_trim_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (input, chars) = parse_trim_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    if let Some(chars) = chars {
        validate_expression_with_scope(chars, scope)?;
    }
    Ok(())
}

fn validate_cond_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    match value {
        Bson::Array(_) => {
            for argument in expression_arguments::<3>(value)? {
                validate_expression_with_scope(argument, scope)?;
            }
        }
        Bson::Document(spec) => {
            if spec.len() != 3 {
                return Err(QueryError::InvalidStructure);
            }
            let condition = spec.get("if").ok_or(QueryError::InvalidStructure)?;
            let on_true = spec.get("then").ok_or(QueryError::InvalidStructure)?;
            let on_false = spec.get("else").ok_or(QueryError::InvalidStructure)?;
            validate_expression_with_scope(condition, scope)?;
            validate_expression_with_scope(on_true, scope)?;
            validate_expression_with_scope(on_false, scope)?;
        }
        _ => return Err(QueryError::InvalidStructure),
    }

    Ok(())
}

fn validate_switch_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (branches, default) = parse_switch_spec(value)?;
    for (condition, then_expression) in branches {
        validate_expression_with_scope(condition, scope)?;
        validate_expression_with_scope(then_expression, scope)?;
    }
    if let Some(default) = default {
        validate_expression_with_scope(default, scope)?;
    }
    Ok(())
}

fn validate_let_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut vars_spec = None;
    let mut in_expression = None;

    for (field, value) in spec {
        match field.as_str() {
            "vars" => vars_spec = Some(value.as_document().ok_or(QueryError::InvalidStructure)?),
            "in" => in_expression = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let vars_spec = vars_spec.ok_or(QueryError::InvalidStructure)?;
    let in_expression = in_expression.ok_or(QueryError::InvalidStructure)?;
    let mut inner_scope = scope.clone();

    for (name, expression) in vars_spec {
        validate_user_variable_write_name(name)?;
        validate_expression_with_scope(expression, scope)?;
        inner_scope.insert(name.clone());
    }

    validate_expression_with_scope(in_expression, &inner_scope)
}

fn validate_map_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut var_name = None;
    let mut in_expression = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "as" => var_name = Some(value.as_str().ok_or(QueryError::InvalidStructure)?),
            "in" => in_expression = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let input = input.ok_or(QueryError::InvalidStructure)?;
    let in_expression = in_expression.ok_or(QueryError::InvalidStructure)?;
    let var_name = var_name.unwrap_or("this");
    validate_user_variable_write_name(var_name)?;
    validate_expression_with_scope(input, scope)?;

    let mut inner_scope = scope.clone();
    inner_scope.insert(var_name.to_string());
    validate_expression_with_scope(in_expression, &inner_scope)
}

fn validate_filter_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut var_name = None;
    let mut condition = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "as" => var_name = Some(value.as_str().ok_or(QueryError::InvalidStructure)?),
            "cond" => condition = Some(value),
            "limit" => validate_expression_with_scope(value, scope)?,
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let input = input.ok_or(QueryError::InvalidStructure)?;
    let condition = condition.ok_or(QueryError::InvalidStructure)?;
    let var_name = var_name.unwrap_or("this");
    validate_user_variable_write_name(var_name)?;
    validate_expression_with_scope(input, scope)?;

    let mut inner_scope = scope.clone();
    inner_scope.insert(var_name.to_string());
    validate_expression_with_scope(condition, &inner_scope)
}

fn validate_set_field_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
    unset: bool,
) -> Result<(), QueryError> {
    let (field, input, assigned) = parse_set_field_spec(value, unset)?;
    constant_field_name(field)?;
    validate_expression_with_scope(input, scope)?;
    if let Some(assigned) = assigned {
        if !is_remove_expression(assigned) {
            validate_expression_with_scope(assigned, scope)?;
        }
    }
    Ok(())
}

fn validate_reduce_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (input, initial_value, in_expression) = parse_reduce_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    validate_expression_with_scope(initial_value, scope)?;

    let mut inner_scope = scope.clone();
    inner_scope.insert("this".to_string());
    inner_scope.insert("value".to_string());
    validate_expression_with_scope(in_expression, &inner_scope)
}

fn parse_get_field_spec(value: &Bson) -> Result<(&Bson, Option<&Bson>), QueryError> {
    match value {
        Bson::Document(spec) => {
            let mut field = None;
            let mut input = None;

            if spec.len() == 1
                && spec
                    .iter()
                    .next()
                    .is_some_and(|(name, _)| name.starts_with('$'))
            {
                return Ok((value, None));
            }

            for (name, value) in spec {
                match name.as_str() {
                    "field" => field = Some(value),
                    "input" => input = Some(value),
                    _ => return Err(QueryError::InvalidStructure),
                }
            }

            Ok((
                field.ok_or(QueryError::InvalidStructure)?,
                Some(input.ok_or(QueryError::InvalidStructure)?),
            ))
        }
        _ => Ok((value, None)),
    }
}

fn parse_set_field_spec(
    value: &Bson,
    unset: bool,
) -> Result<(&Bson, &Bson, Option<&Bson>), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut field = None;
    let mut input = None;
    let mut assigned = None;

    for (name, value) in spec {
        match name.as_str() {
            "field" => field = Some(value),
            "input" => input = Some(value),
            "value" if !unset => assigned = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        field.ok_or(QueryError::InvalidStructure)?,
        input.ok_or(QueryError::InvalidStructure)?,
        if unset {
            None
        } else {
            Some(assigned.ok_or(QueryError::InvalidStructure)?)
        },
    ))
}

fn parse_reduce_spec(value: &Bson) -> Result<(&Bson, &Bson, &Bson), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut initial_value = None;
    let mut in_expression = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "initialValue" => initial_value = Some(value),
            "in" => in_expression = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        input.ok_or(QueryError::InvalidStructure)?,
        initial_value.ok_or(QueryError::InvalidStructure)?,
        in_expression.ok_or(QueryError::InvalidStructure)?,
    ))
}

fn parse_trim_spec(value: &Bson) -> Result<(&Bson, Option<&Bson>), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut chars = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "chars" => chars = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((input.ok_or(QueryError::InvalidStructure)?, chars))
}

fn parse_replace_spec(value: &Bson) -> Result<(&Bson, &Bson, &Bson), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut find = None;
    let mut replacement = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "find" => find = Some(value),
            "replacement" => replacement = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        input.ok_or(QueryError::InvalidStructure)?,
        find.ok_or(QueryError::InvalidStructure)?,
        replacement.ok_or(QueryError::InvalidStructure)?,
    ))
}

enum SortArrayOrder {
    Scalar(i64),
    Document(Document),
}

fn parse_sort_array_spec(value: &Bson) -> Result<(&Bson, &Bson), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut sort_by = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "sortBy" => sort_by = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        input.ok_or(QueryError::InvalidStructure)?,
        sort_by.ok_or(QueryError::InvalidStructure)?,
    ))
}

fn parse_sort_array_order(value: &Bson) -> Result<SortArrayOrder, QueryError> {
    if let Some(direction) = integer_value(value) {
        if matches!(direction, 1 | -1) {
            return Ok(SortArrayOrder::Scalar(direction));
        }
    }

    let sort = value.as_document().ok_or(QueryError::InvalidStructure)?;
    validate_sort_spec(sort).map_err(|_| QueryError::InvalidStructure)?;
    Ok(SortArrayOrder::Document(sort.clone()))
}

type ZipSpec<'a> = (&'a [Bson], Option<&'a [Bson]>, bool);

fn parse_zip_spec(value: &Bson) -> Result<ZipSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut inputs = None;
    let mut defaults = None;
    let mut use_longest_length = false;

    for (field, value) in spec {
        match field.as_str() {
            "inputs" => inputs = Some(value.as_array().ok_or(QueryError::InvalidStructure)?),
            "defaults" => defaults = Some(value.as_array().ok_or(QueryError::InvalidStructure)?),
            "useLongestLength" => {
                use_longest_length = value.as_bool().ok_or(QueryError::InvalidStructure)?
            }
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let inputs = inputs.ok_or(QueryError::InvalidStructure)?;
    if inputs.is_empty() {
        return Err(QueryError::InvalidStructure);
    }
    if defaults.is_some() && !use_longest_length {
        return Err(QueryError::InvalidStructure);
    }
    if let Some(defaults) = defaults
        && defaults.len() != inputs.len()
    {
        return Err(QueryError::InvalidStructure);
    }

    Ok((
        inputs.as_slice(),
        defaults.map(Vec::as_slice),
        use_longest_length,
    ))
}

type SwitchBranches<'a> = Vec<(&'a Bson, &'a Bson)>;

fn parse_switch_spec(value: &Bson) -> Result<(SwitchBranches<'_>, Option<&Bson>), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut branches = None;
    let mut default = None;

    for (field, value) in spec {
        match field.as_str() {
            "branches" => {
                let values = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let mut parsed = Vec::with_capacity(values.len());
                for branch in values {
                    let branch = branch.as_document().ok_or(QueryError::InvalidStructure)?;
                    let mut condition = None;
                    let mut then_expression = None;
                    for (field, value) in branch {
                        match field.as_str() {
                            "case" => condition = Some(value),
                            "then" => then_expression = Some(value),
                            _ => return Err(QueryError::InvalidStructure),
                        }
                    }
                    parsed.push((
                        condition.ok_or(QueryError::InvalidStructure)?,
                        then_expression.ok_or(QueryError::InvalidStructure)?,
                    ));
                }
                branches = Some(parsed);
            }
            "default" => default = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    let branches = branches.ok_or(QueryError::InvalidStructure)?;
    if branches.is_empty() {
        return Err(QueryError::InvalidStructure);
    }
    Ok((branches, default))
}

fn constant_field_name(expression: &Bson) -> Result<String, QueryError> {
    let value = match expression {
        Bson::String(value) if value.starts_with('$') => {
            return Err(QueryError::InvalidArgument(
                "$setField requires `field` to be a constant string".to_string(),
            ));
        }
        Bson::String(value) => value.clone(),
        Bson::Document(spec) if spec.len() == 1 => {
            let (operator, value) = spec.iter().next().expect("single field");
            match operator.as_str() {
                "$const" | "$literal" => match value {
                    Bson::String(value) => value.clone(),
                    _ => {
                        return Err(QueryError::InvalidArgument(
                            "$setField requires `field` to be a constant string".to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(QueryError::InvalidArgument(
                        "$setField requires `field` to be a constant string".to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(QueryError::InvalidArgument(
                "$setField requires `field` to be a constant string".to_string(),
            ));
        }
    };

    validate_object_key(&value)?;
    Ok(value)
}

fn eval_set_operand(
    document: &Document,
    expression: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
    nullish_returns_null: bool,
) -> Result<Option<Vec<Bson>>, QueryError> {
    let value = eval_expression_result_with_variables(document, expression, variables)?;
    if value.is_nullish() {
        return if nullish_returns_null {
            Ok(None)
        } else {
            Err(QueryError::InvalidArgument(format!(
                "{operator} requires array inputs"
            )))
        };
    }
    match value {
        EvaluatedExpression::Value(Bson::Array(values)) => Ok(Some(values)),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires array inputs"
        ))),
    }
}

fn coerce_case_string(value: EvaluatedExpression, operator: &str) -> Result<String, QueryError> {
    let value = match value {
        EvaluatedExpression::Missing => return Ok(String::new()),
        EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => return Ok(String::new()),
        EvaluatedExpression::Value(value) => value,
    };

    match value {
        Bson::String(value) => Ok(value),
        Bson::Symbol(value) => Ok(value),
        Bson::Int32(value) => Ok(value.to_string()),
        Bson::Int64(value) => Ok(value.to_string()),
        Bson::Double(value) => Ok(value.to_string()),
        Bson::Decimal128(value) => Ok(value.to_string()),
        Bson::DateTime(value) => {
            let value = DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
                .ok_or_else(|| {
                    QueryError::InvalidArgument(format!(
                        "{operator} requires a string-compatible input"
                    ))
                })?;
            Ok(value.to_rfc3339_opts(SecondsFormat::Millis, true))
        }
        Bson::ObjectId(value) => Ok(value.to_hex()),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires a string-compatible input"
        ))),
    }
}

fn coerce_substring_input(value: EvaluatedExpression) -> Result<String, QueryError> {
    match value {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(String::new())
        }
        EvaluatedExpression::Value(Bson::String(value))
        | EvaluatedExpression::Value(Bson::Symbol(value))
        | EvaluatedExpression::Value(Bson::JavaScriptCode(value)) => Ok(value),
        EvaluatedExpression::Value(Bson::JavaScriptCodeWithScope(value)) => Ok(value.code),
        EvaluatedExpression::Value(Bson::Int32(value)) => Ok(value.to_string()),
        EvaluatedExpression::Value(Bson::Int64(value)) => Ok(value.to_string()),
        EvaluatedExpression::Value(Bson::Double(value)) => Ok(value.to_string()),
        EvaluatedExpression::Value(Bson::Decimal128(value)) => Ok(value.to_string()),
        EvaluatedExpression::Value(Bson::Timestamp(value)) => Ok(value.to_string()),
        EvaluatedExpression::Value(Bson::DateTime(value)) => {
            let value = DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
                .ok_or_else(|| {
                    QueryError::InvalidArgument(
                        "substring expressions require a string-compatible input".to_string(),
                    )
                })?;
            Ok(value.to_rfc3339_opts(SecondsFormat::Millis, true))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "substring expressions require a string-compatible input".to_string(),
        )),
    }
}

fn require_string_operand(
    value: EvaluatedExpression,
    operator: &str,
) -> Result<String, QueryError> {
    match value {
        EvaluatedExpression::Value(Bson::String(value)) => Ok(value),
        EvaluatedExpression::Value(Bson::Symbol(value)) => Ok(value),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires a string argument"
        ))),
    }
}

fn require_named_string_operand(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<String, QueryError> {
    match value {
        EvaluatedExpression::Value(Bson::String(value)) => Ok(value),
        EvaluatedExpression::Value(Bson::Symbol(value)) => Ok(value),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires `{field}` to evaluate to a string"
        ))),
    }
}

fn require_trim_string(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<String, QueryError> {
    match value {
        EvaluatedExpression::Value(Bson::String(value)) => Ok(value),
        EvaluatedExpression::Value(Bson::Symbol(value)) => Ok(value),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires `{field}` to evaluate to a string"
        ))),
    }
}

fn trim_whitespace(input: &str, trim_type: TrimType) -> &str {
    match trim_type {
        TrimType::Left => input.trim_start_matches(char::is_whitespace),
        TrimType::Right => input.trim_end_matches(char::is_whitespace),
        TrimType::Both => input.trim_matches(char::is_whitespace),
    }
}

fn trim_with_characters<'a>(
    input: &'a str,
    characters: &BTreeSet<char>,
    trim_type: TrimType,
) -> &'a str {
    let matcher = |character| characters.contains(&character);
    match trim_type {
        TrimType::Left => input.trim_start_matches(matcher),
        TrimType::Right => input.trim_end_matches(matcher),
        TrimType::Both => input.trim_matches(matcher),
    }
}

fn bitwise_i64(value: &Bson) -> Result<i64, QueryError> {
    match value {
        Bson::Int32(value) => Ok(*value as i64),
        Bson::Int64(value) => Ok(*value),
        _ => Err(QueryError::InvalidArgument(
            "bitwise expressions require integer inputs".to_string(),
        )),
    }
}

fn unique_bson_values(values: &[Bson]) -> Vec<Bson> {
    let mut unique = Vec::new();
    for value in values {
        push_unique_bson(&mut unique, value.clone());
    }
    unique
}

fn push_unique_bson(values: &mut Vec<Bson>, candidate: Bson) {
    if !bson_array_contains(values, &candidate) {
        values.push(candidate);
    }
}

fn bson_array_contains(values: &[Bson], candidate: &Bson) -> bool {
    values
        .iter()
        .any(|value| compare_bson(value, candidate).is_eq())
}

fn is_remove_expression(expression: &Bson) -> bool {
    matches!(expression, Bson::String(value) if value == "$$REMOVE")
}

fn parse_filter_limit(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<usize, QueryError> {
    let limit = eval_expression_result_with_variables(document, value, variables)?;
    if limit.is_nullish() {
        return Ok(usize::MAX);
    }

    let EvaluatedExpression::Value(limit) = limit else {
        return Ok(usize::MAX);
    };
    let limit = integer_value(&limit).ok_or_else(|| {
        QueryError::InvalidArgument("$filter limit must evaluate to a positive integer".to_string())
    })?;
    if limit <= 0 {
        return Err(QueryError::InvalidArgument(
            "$filter limit must evaluate to a positive integer".to_string(),
        ));
    }

    usize::try_from(limit).map_err(|_| {
        QueryError::InvalidArgument("$filter limit must evaluate to a positive integer".to_string())
    })
}

fn parse_i32_bound(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<i32, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    let numeric = numeric_value(&value).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} requires numeric integral arguments"))
    })?;
    if numeric.fract() != 0.0 {
        return Err(QueryError::InvalidArgument(format!(
            "{operator} requires numeric integral arguments"
        )));
    }

    i32::try_from(numeric as i64).map_err(|_| {
        QueryError::InvalidArgument(format!(
            "{operator} requires arguments representable as 32-bit integers"
        ))
    })
}

fn parse_non_negative_index(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<usize, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    let numeric = numeric_value(&value).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} requires non-negative integral bounds"))
    })?;
    if numeric.fract() != 0.0 || numeric < 0.0 {
        return Err(QueryError::InvalidArgument(format!(
            "{operator} requires non-negative integral bounds"
        )));
    }

    usize::try_from(numeric as u64).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} requires non-negative integral bounds"))
    })
}

fn parse_non_negative_i32_bound(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<i32, QueryError> {
    let value = parse_i32_bound(document, value, variables, operator)?;
    if value < 0 {
        return Err(QueryError::InvalidArgument(format!(
            "{operator} requires non-negative integral arguments"
        )));
    }
    Ok(value)
}

fn parse_non_negative_truncating_index(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<usize, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    let numeric = numeric_value(&value).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} requires numeric arguments"))
    })?;
    let truncated = numeric.trunc();
    if truncated < 0.0 {
        return Err(QueryError::InvalidArgument(format!(
            "{operator} requires a non-negative starting index"
        )));
    }
    usize::try_from(truncated as u128)
        .map_err(|_| QueryError::InvalidArgument(format!("{operator} requires numeric arguments")))
}

fn parse_truncating_length(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<Option<usize>, QueryError> {
    let value = eval_expression_with_variables(document, value, variables)?;
    let numeric = numeric_value(&value).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} requires numeric arguments"))
    })?;
    let truncated = numeric.trunc();
    if truncated < 0.0 {
        return Ok(None);
    }
    usize::try_from(truncated as u128)
        .map(Some)
        .map_err(|_| QueryError::InvalidArgument(format!("{operator} requires numeric arguments")))
}

fn validate_utf8_byte_boundary(
    input: &str,
    index: usize,
    operator: &str,
    starting: bool,
) -> Result<(), QueryError> {
    if index >= input.len() {
        return Ok(());
    }
    if is_utf8_continuation_byte(input.as_bytes()[index]) {
        let part = if starting { "starting" } else { "ending" };
        return Err(QueryError::InvalidArgument(format!(
            "{operator} has an invalid UTF-8 {part} boundary"
        )));
    }
    Ok(())
}

fn is_utf8_continuation_byte(byte: u8) -> bool {
    (byte & 0b1100_0000) == 0b1000_0000
}

fn materialize_variable_value(value: EvaluatedExpression) -> Bson {
    match value {
        EvaluatedExpression::Missing => Bson::Null,
        EvaluatedExpression::Value(value) => value,
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

fn eval_binary_size_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let evaluated = eval_expression_result_with_variables(
        document,
        single_expression_operand(value)?,
        variables,
    )?;
    match evaluated {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::String(value)) => {
            Ok(EvaluatedExpression::Value(Bson::Int64(value.len() as i64)))
        }
        EvaluatedExpression::Value(Bson::Binary(binary)) => Ok(EvaluatedExpression::Value(
            Bson::Int64(binary.bytes.len() as i64),
        )),
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$binarySize requires a string or BinData input".to_string(),
        )),
    }
}

fn eval_bson_size_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let evaluated = eval_expression_result_with_variables(
        document,
        single_expression_operand(value)?,
        variables,
    )?;
    match evaluated {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            Ok(EvaluatedExpression::Value(Bson::Null))
        }
        EvaluatedExpression::Value(Bson::Document(document)) => {
            let size = bson::to_vec(&document).map_err(|error| {
                QueryError::InvalidArgument(format!("$bsonSize failed to encode document: {error}"))
            })?;
            Ok(EvaluatedExpression::Value(Bson::Int64(size.len() as i64)))
        }
        EvaluatedExpression::Value(_) => Err(QueryError::InvalidArgument(
            "$bsonSize requires a document input".to_string(),
        )),
    }
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
    let evaluated = eval_expression_result_with_variables(
        document,
        unary_expression_operand(value),
        variables,
    )?;
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
