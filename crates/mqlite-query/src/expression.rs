use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use bson::{Bson, Decimal128, Document, doc, oid::ObjectId};
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset,
    SecondsFormat, TimeZone, Timelike, Utc, Weekday,
};
use chrono_tz::Tz;
use mqlite_bson::{compare_bson, lookup_path_owned};
use regex::Regex as RustRegex;

use crate::{
    QueryError,
    filter::{bson_type_alias, compile_regex},
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
        "$dateAdd" => eval_date_arithmetic_expression(
            document,
            value,
            variables,
            DateArithmeticExpressionMode::Add,
        ),
        "$dateDiff" => eval_date_diff_expression(document, value, variables),
        "$dateFromString" => eval_date_from_string_expression(document, value, variables),
        "$dateFromParts" => eval_date_from_parts_expression(document, value, variables),
        "$dateSubtract" => eval_date_arithmetic_expression(
            document,
            value,
            variables,
            DateArithmeticExpressionMode::Subtract,
        ),
        "$dateToString" => eval_date_to_string_expression(document, value, variables),
        "$dateToParts" => eval_date_to_parts_expression(document, value, variables),
        "$dateTrunc" => eval_date_trunc_expression(document, value, variables),
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
        "$avg" => {
            eval_expression_accumulator(document, value, variables, ExpressionAccumulator::Avg)
        }
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
        "$dayOfMonth" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::DayOfMonth,
        ),
        "$dayOfWeek" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::DayOfWeek,
        ),
        "$dayOfYear" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::DayOfYear,
        ),
        "$filter" => eval_filter_expression(document, value, variables),
        "$first" => eval_first_last_expression(document, value, variables, true),
        "$firstN" => eval_n_expression(document, value, variables, NExpressionMode::First),
        "$getField" => eval_get_field_expression(document, value, variables),
        "$hour" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Hour)
        }
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
        "$isoDayOfWeek" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::IsoDayOfWeek,
        ),
        "$isoWeek" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::IsoWeek)
        }
        "$isoWeekYear" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::IsoWeekYear,
        ),
        "$isArray" => Ok(EvaluatedExpression::Value(Bson::Boolean(matches!(
            eval_expression_result_with_variables(document, value, variables)?,
            EvaluatedExpression::Value(Bson::Array(_))
        )))),
        "$last" => eval_first_last_expression(document, value, variables, false),
        "$lastN" => eval_n_expression(document, value, variables, NExpressionMode::Last),
        "$map" => eval_map_expression(document, value, variables),
        "$max" => {
            eval_expression_accumulator(document, value, variables, ExpressionAccumulator::Max)
        }
        "$maxN" => eval_n_expression(document, value, variables, NExpressionMode::Max),
        "$mergeObjects" => eval_merge_objects_expression(document, value, variables),
        "$millisecond" => eval_date_part_expression(
            document,
            value,
            variables,
            DatePartExpressionMode::Millisecond,
        ),
        "$min" => {
            eval_expression_accumulator(document, value, variables, ExpressionAccumulator::Min)
        }
        "$minN" => eval_n_expression(document, value, variables, NExpressionMode::Min),
        "$minute" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Minute)
        }
        "$month" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Month)
        }
        "$objectToArray" => eval_object_to_array_expression(document, value, variables),
        "$range" => eval_range_expression(document, value, variables),
        "$reduce" => eval_reduce_expression(document, value, variables),
        "$regexFind" => {
            eval_regex_expression(document, value, variables, RegexExpressionMode::Find)
        }
        "$regexFindAll" => {
            eval_regex_expression(document, value, variables, RegexExpressionMode::FindAll)
        }
        "$regexMatch" => {
            eval_regex_expression(document, value, variables, RegexExpressionMode::Match)
        }
        "$replaceAll" => eval_replace_expression(document, value, variables, true),
        "$replaceOne" => eval_replace_expression(document, value, variables, false),
        "$reverseArray" => eval_reverse_array_expression(document, value, variables),
        "$second" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Second)
        }
        "$sortArray" => eval_sort_array_expression(document, value, variables),
        "$slice" => eval_slice_expression(document, value, variables),
        "$sum" => {
            eval_expression_accumulator(document, value, variables, ExpressionAccumulator::Sum)
        }
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
        "$week" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Week)
        }
        "$year" => {
            eval_date_part_expression(document, value, variables, DatePartExpressionMode::Year)
        }
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
        "$avg" | "$max" | "$min" | "$sum" => {
            if let Bson::Array(arguments) = value {
                for argument in arguments {
                    validate_expression_with_scope(argument, scope)?;
                }
                Ok(())
            } else {
                validate_expression_with_scope(value, scope)
            }
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
        "$dayOfMonth" | "$dayOfWeek" | "$dayOfYear" | "$hour" | "$isoDayOfWeek" | "$isoWeek"
        | "$isoWeekYear" | "$millisecond" | "$minute" | "$month" | "$second" | "$week"
        | "$year" => validate_date_part_expression(value, scope),
        "$dateAdd" | "$dateSubtract" => validate_date_arithmetic_expression(value, scope, operator),
        "$dateDiff" => validate_date_diff_expression(value, scope),
        "$dateFromString" => validate_date_from_string_expression(value, scope),
        "$dateFromParts" => validate_date_from_parts_expression(value, scope),
        "$dateToString" => validate_date_to_string_expression(value, scope),
        "$dateToParts" => validate_date_to_parts_expression(value, scope),
        "$dateTrunc" => validate_date_trunc_expression(value, scope),
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
        "$firstN" | "$lastN" | "$maxN" | "$minN" => validate_n_expression(value, scope),
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
        "$regexFind" | "$regexFindAll" | "$regexMatch" => validate_regex_expression(value, scope),
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

fn eval_regex_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    mode: RegexExpressionMode,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = match mode {
        RegexExpressionMode::Find => "$regexFind",
        RegexExpressionMode::FindAll => "$regexFindAll",
        RegexExpressionMode::Match => "$regexMatch",
    };
    let (input, regex, options) = parse_regex_expression_spec(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    let regex = eval_expression_result_with_variables(document, regex, variables)?;
    let options = options
        .map(|options| eval_expression_result_with_variables(document, options, variables))
        .transpose()?;
    let input = if input.is_nullish() {
        None
    } else {
        Some(require_named_string_operand(input, operator, "input")?)
    };
    let compiled = compile_expression_regex(operator, regex, options, value)?;

    if input.is_none() || compiled.is_none() {
        return Ok(match mode {
            RegexExpressionMode::Find => EvaluatedExpression::Value(Bson::Null),
            RegexExpressionMode::FindAll => EvaluatedExpression::Value(Bson::Array(Vec::new())),
            RegexExpressionMode::Match => EvaluatedExpression::Value(Bson::Boolean(false)),
        });
    }

    let input = input.expect("validated input");
    let compiled = compiled.expect("validated regex");

    Ok(match mode {
        RegexExpressionMode::Find => regex_match_documents(&input, &compiled)
            .into_iter()
            .next()
            .map(Bson::Document)
            .map(EvaluatedExpression::Value)
            .unwrap_or(EvaluatedExpression::Value(Bson::Null)),
        RegexExpressionMode::FindAll => EvaluatedExpression::Value(Bson::Array(
            regex_match_documents(&input, &compiled)
                .into_iter()
                .map(Bson::Document)
                .collect(),
        )),
        RegexExpressionMode::Match => {
            EvaluatedExpression::Value(Bson::Boolean(compiled.is_match(&input)))
        }
    })
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

#[derive(Clone, Copy)]
enum ExpressionAccumulator {
    Avg,
    Max,
    Min,
    Sum,
}

#[derive(Clone, Copy)]
enum RegexExpressionMode {
    Find,
    FindAll,
    Match,
}

#[derive(Clone, Copy)]
enum NExpressionMode {
    First,
    Last,
    Max,
    Min,
}

#[derive(Clone, Copy)]
enum DatePartExpressionMode {
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    Hour,
    IsoDayOfWeek,
    IsoWeek,
    IsoWeekYear,
    Millisecond,
    Minute,
    Month,
    Second,
    Week,
    Year,
}

impl DatePartExpressionMode {
    fn name(self) -> &'static str {
        match self {
            Self::DayOfMonth => "$dayOfMonth",
            Self::DayOfWeek => "$dayOfWeek",
            Self::DayOfYear => "$dayOfYear",
            Self::Hour => "$hour",
            Self::IsoDayOfWeek => "$isoDayOfWeek",
            Self::IsoWeek => "$isoWeek",
            Self::IsoWeekYear => "$isoWeekYear",
            Self::Millisecond => "$millisecond",
            Self::Minute => "$minute",
            Self::Month => "$month",
            Self::Second => "$second",
            Self::Week => "$week",
            Self::Year => "$year",
        }
    }
}

#[derive(Clone, Copy)]
enum DateArithmeticExpressionMode {
    Add,
    Subtract,
}

impl DateArithmeticExpressionMode {
    fn name(self) -> &'static str {
        match self {
            Self::Add => "$dateAdd",
            Self::Subtract => "$dateSubtract",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DateUnit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn eval_expression_accumulator(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    accumulator: ExpressionAccumulator,
) -> Result<EvaluatedExpression, QueryError> {
    match value {
        Bson::Array(arguments) if arguments.len() != 1 => {
            let mut evaluated = Vec::with_capacity(arguments.len());
            for argument in arguments {
                evaluated.push(eval_expression_result_with_variables(
                    document, argument, variables,
                )?);
            }
            evaluate_expression_accumulator_values(evaluated, accumulator, false)
        }
        _ => {
            let evaluated = eval_expression_result_with_variables(
                document,
                unary_expression_operand(value),
                variables,
            )?;
            evaluate_expression_accumulator_values(vec![evaluated], accumulator, true)
        }
    }
}

fn evaluate_expression_accumulator_values(
    values: Vec<EvaluatedExpression>,
    accumulator: ExpressionAccumulator,
    single_argument: bool,
) -> Result<EvaluatedExpression, QueryError> {
    match accumulator {
        ExpressionAccumulator::Avg => evaluate_expression_avg(values, single_argument),
        ExpressionAccumulator::Max => evaluate_expression_extrema(values, single_argument, true),
        ExpressionAccumulator::Min => evaluate_expression_extrema(values, single_argument, false),
        ExpressionAccumulator::Sum => evaluate_expression_sum(values, single_argument),
    }
}

fn evaluate_expression_avg(
    values: Vec<EvaluatedExpression>,
    single_argument: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let mut sum = 0.0;
    let mut count = 0usize;
    for value in expression_accumulator_items(values, single_argument) {
        if let Some(number) = numeric_value_or_none(&value)? {
            sum += number;
            count += 1;
        }
    }

    Ok(if count == 0 {
        EvaluatedExpression::Value(Bson::Null)
    } else {
        EvaluatedExpression::Value(Bson::Double(sum / count as f64))
    })
}

fn evaluate_expression_sum(
    values: Vec<EvaluatedExpression>,
    single_argument: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let mut sum = 0.0;
    for value in expression_accumulator_items(values, single_argument) {
        if let Some(number) = numeric_value_or_none(&value)? {
            sum += number;
        }
    }
    Ok(EvaluatedExpression::Value(number_bson(sum)))
}

fn evaluate_expression_extrema(
    values: Vec<EvaluatedExpression>,
    single_argument: bool,
    max: bool,
) -> Result<EvaluatedExpression, QueryError> {
    let mut best = None;
    for value in expression_accumulator_items(values, single_argument) {
        if matches!(value, Bson::Null | Bson::Undefined) {
            continue;
        }

        match &best {
            None => best = Some(value),
            Some(current) if extrema_prefers_candidate(current, &value, max) => best = Some(value),
            _ => {}
        }
    }

    Ok(best
        .map(EvaluatedExpression::Value)
        .unwrap_or(EvaluatedExpression::Value(Bson::Null)))
}

fn expression_accumulator_items(
    values: Vec<EvaluatedExpression>,
    single_argument: bool,
) -> Vec<Bson> {
    if single_argument {
        match values
            .into_iter()
            .next()
            .unwrap_or(EvaluatedExpression::Missing)
        {
            EvaluatedExpression::Missing => vec![Bson::Null],
            EvaluatedExpression::Value(Bson::Array(items)) => items,
            EvaluatedExpression::Value(value) => vec![value],
        }
    } else {
        values
            .into_iter()
            .map(|value| match value {
                EvaluatedExpression::Missing => Bson::Null,
                EvaluatedExpression::Value(value) => value,
            })
            .collect()
    }
}

fn numeric_value_or_none(value: &Bson) -> Result<Option<f64>, QueryError> {
    match value {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            numeric_value(value).map(Some)
        }
        _ => Ok(None),
    }
}

fn extrema_prefers_candidate(current: &Bson, candidate: &Bson, max: bool) -> bool {
    let current_nan = bson_is_nan(current);
    let candidate_nan = bson_is_nan(candidate);
    if max {
        if candidate_nan {
            return false;
        }
        if current_nan {
            return true;
        }
        compare_bson(candidate, current).is_gt()
    } else {
        if candidate_nan {
            return true;
        }
        if current_nan {
            return false;
        }
        compare_bson(candidate, current).is_lt()
    }
}

fn bson_is_nan(value: &Bson) -> bool {
    match value {
        Bson::Double(value) => value.is_nan(),
        Bson::Decimal128(value) => value
            .to_string()
            .parse::<f64>()
            .map(f64::is_nan)
            .unwrap_or(false),
        _ => false,
    }
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

fn validate_regex_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (input, regex, options) = parse_regex_expression_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    validate_expression_with_scope(regex, scope)?;
    if let Some(options) = options {
        validate_expression_with_scope(options, scope)?;
    }
    Ok(())
}

fn validate_n_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (input, n) = parse_n_expression_spec(value)?;
    validate_expression_with_scope(input, scope)?;
    validate_expression_with_scope(n, scope)?;
    Ok(())
}

fn validate_date_part_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (date, timezone) = parse_date_part_expression_spec(value)?;
    validate_expression_with_scope(date, scope)?;
    if let Some(timezone) = timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    Ok(())
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

fn parse_regex_expression_spec(value: &Bson) -> Result<(&Bson, &Bson, Option<&Bson>), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut regex = None;
    let mut options = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "regex" => regex = Some(value),
            "options" => options = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        input.ok_or(QueryError::InvalidStructure)?,
        regex.ok_or(QueryError::InvalidStructure)?,
        options,
    ))
}

fn parse_n_expression_spec(value: &Bson) -> Result<(&Bson, &Bson), QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut input = None;
    let mut n = None;

    for (field, value) in spec {
        match field.as_str() {
            "input" => input = Some(value),
            "n" => n = Some(value),
            _ => return Err(QueryError::InvalidStructure),
        }
    }

    Ok((
        input.ok_or(QueryError::InvalidStructure)?,
        n.ok_or(QueryError::InvalidStructure)?,
    ))
}

fn parse_date_part_expression_spec(value: &Bson) -> Result<(&Bson, Option<&Bson>), QueryError> {
    match value {
        Bson::Document(spec) => {
            let mut date = None;
            let mut timezone = None;
            for (field, value) in spec {
                match field.as_str() {
                    "date" => date = Some(value),
                    "timezone" => timezone = Some(value),
                    _ => return Err(QueryError::InvalidStructure),
                }
            }
            Ok((date.ok_or(QueryError::InvalidStructure)?, timezone))
        }
        Bson::Array(arguments) => {
            if arguments.len() != 1 {
                return Err(QueryError::InvalidStructure);
            }
            Ok((&arguments[0], None))
        }
        _ => Ok((value, None)),
    }
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

fn compile_expression_regex(
    operator: &str,
    regex: EvaluatedExpression,
    options: Option<EvaluatedExpression>,
    spec: &Bson,
) -> Result<Option<RustRegex>, QueryError> {
    let options_present = parse_regex_expression_spec(spec)?.2.is_some();
    let explicit_options = match options {
        Some(
            EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined),
        ) => None,
        Some(EvaluatedExpression::Value(Bson::String(options))) => Some(options),
        Some(_) => {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires `options` to evaluate to a string"
            )));
        }
        None => None,
    };

    let (pattern, embedded_options) = match regex {
        EvaluatedExpression::Missing | EvaluatedExpression::Value(Bson::Null | Bson::Undefined) => {
            return Ok(None);
        }
        EvaluatedExpression::Value(Bson::String(pattern)) => (pattern, String::new()),
        EvaluatedExpression::Value(Bson::RegularExpression(regex)) => {
            if options_present && !regex.options.is_empty() {
                return Err(QueryError::InvalidArgument(format!(
                    "{operator} cannot specify regex options in both `regex` and `options`"
                )));
            }
            (regex.pattern, regex.options)
        }
        _ => {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires `regex` to evaluate to a string or regex"
            )));
        }
    };

    let options = match explicit_options {
        Some(options) => {
            if !embedded_options.is_empty() {
                return Err(QueryError::InvalidArgument(format!(
                    "{operator} cannot specify regex options in both `regex` and `options`"
                )));
            }
            options
        }
        None => embedded_options,
    };

    Ok(Some(compile_regex(&pattern, &options)?))
}

fn regex_match_documents(input: &str, regex: &RustRegex) -> Vec<Document> {
    let mut results = Vec::new();
    let mut search_start = 0usize;

    loop {
        let Some(captures) = regex.captures_at(input, search_start) else {
            break;
        };
        let matched = captures.get(0).expect("captures always include group zero");
        if matched.start() == input.len() && matched.end() == input.len() && !input.is_empty() {
            break;
        }

        results.push(regex_match_document(input, &captures));

        if matched.end() > matched.start() {
            search_start = matched.end();
            continue;
        }

        let Some(next) = next_char_boundary(input, matched.start()) else {
            break;
        };
        search_start = next;
    }

    results
}

fn regex_match_document(input: &str, captures: &regex::Captures<'_>) -> Document {
    let matched = captures.get(0).expect("captures always include group zero");
    let mut document = Document::new();
    document.insert("match", matched.as_str());
    document.insert("idx", regex_code_point_index(input, matched.start()));
    document.insert(
        "captures",
        Bson::Array(
            (1..captures.len())
                .map(|index| match captures.get(index) {
                    Some(capture) => Bson::String(capture.as_str().to_string()),
                    None => Bson::Null,
                })
                .collect(),
        ),
    );
    document
}

fn regex_code_point_index(input: &str, byte_index: usize) -> Bson {
    let count = input[..byte_index].chars().count();
    i32::try_from(count)
        .map(Bson::Int32)
        .unwrap_or_else(|_| Bson::Int64(count as i64))
}

fn next_char_boundary(input: &str, start: usize) -> Option<usize> {
    input[start..]
        .chars()
        .next()
        .map(|character| start + character.len_utf8())
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

fn eval_n_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    mode: NExpressionMode,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = match mode {
        NExpressionMode::First => "$firstN",
        NExpressionMode::Last => "$lastN",
        NExpressionMode::Max => "$maxN",
        NExpressionMode::Min => "$minN",
    };
    let (input, n) = parse_n_expression_spec(value)?;
    let input = eval_expression_result_with_variables(document, input, variables)?;
    let n = eval_expression_with_variables(document, n, variables)?;

    let EvaluatedExpression::Value(Bson::Array(items)) = input else {
        return Err(QueryError::InvalidArgument(format!(
            "{operator} requires `input` to evaluate to an array"
        )));
    };

    let n = exact_positive_usize(&n).ok_or_else(|| {
        QueryError::InvalidArgument(format!(
            "{operator} requires `n` to evaluate to a positive integral value"
        ))
    })?;

    let result = match mode {
        NExpressionMode::First => items.into_iter().take(n).collect(),
        NExpressionMode::Last => {
            let start = items.len().saturating_sub(n);
            items.into_iter().skip(start).collect()
        }
        NExpressionMode::Min => {
            let mut values = items
                .into_iter()
                .filter(|value| !matches!(value, Bson::Null | Bson::Undefined))
                .collect::<Vec<_>>();
            values.sort_by(compare_bson);
            values.truncate(n);
            values
        }
        NExpressionMode::Max => {
            let mut values = items
                .into_iter()
                .filter(|value| !matches!(value, Bson::Null | Bson::Undefined))
                .collect::<Vec<_>>();
            values.sort_by(|left, right| compare_bson(right, left));
            values.truncate(n);
            values
        }
    };

    Ok(EvaluatedExpression::Value(Bson::Array(result)))
}

fn eval_date_arithmetic_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    mode: DateArithmeticExpressionMode,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = mode.name();
    let (start_date, unit, amount, timezone) =
        parse_date_arithmetic_expression_spec(value, operator)?;
    let start_date = eval_expression_result_with_variables(document, start_date, variables)?;
    if start_date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let unit = eval_expression_result_with_variables(document, unit, variables)?;
    if unit.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let amount = eval_expression_result_with_variables(document, amount, variables)?;
    if amount.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let timezone = resolve_timezone_expression(document, timezone, variables, operator)?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let start_date = coerce_date_expression_value(start_date, operator, "startDate")?;
    let unit = parse_date_unit_operand(unit, operator)?;
    let amount = require_integral_i64_operand(amount, operator, "amount")?;

    if matches!(mode, DateArithmeticExpressionMode::Subtract) && amount == i64::MIN {
        return Err(QueryError::InvalidArgument(format!(
            "invalid {operator} 'amount' parameter value: {amount}"
        )));
    }

    let amount = match mode {
        DateArithmeticExpressionMode::Add => amount,
        DateArithmeticExpressionMode::Subtract => -amount,
    };
    let result = date_add_units(start_date, unit, amount, &timezone, operator)?;
    Ok(EvaluatedExpression::Value(Bson::DateTime(
        bson::DateTime::from_millis(result.timestamp_millis()),
    )))
}

fn eval_date_diff_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (start_date, end_date, unit, timezone, start_of_week) =
        parse_date_diff_expression_spec(value)?;
    let start_date = eval_expression_result_with_variables(document, start_date, variables)?;
    if start_date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let end_date = eval_expression_result_with_variables(document, end_date, variables)?;
    if end_date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let unit = eval_expression_result_with_variables(document, unit, variables)?;
    if unit.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let timezone = resolve_timezone_expression(document, timezone, variables, "$dateDiff")?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    let start_date = coerce_date_expression_value(start_date, "$dateDiff", "startDate")?;
    let end_date = coerce_date_expression_value(end_date, "$dateDiff", "endDate")?;
    let unit = parse_date_unit_operand(unit, "$dateDiff")?;
    let start_of_week = if unit == DateUnit::Week {
        match start_of_week {
            Some(start_of_week) => {
                let value =
                    eval_expression_result_with_variables(document, start_of_week, variables)?;
                if value.is_nullish() {
                    return Ok(EvaluatedExpression::Value(Bson::Null));
                }
                parse_start_of_week_operand(value, "$dateDiff", "startOfWeek")?
            }
            None => Weekday::Sun,
        }
    } else {
        Weekday::Sun
    };

    Ok(EvaluatedExpression::Value(Bson::Int64(date_diff_units(
        start_date,
        end_date,
        unit,
        &timezone,
        start_of_week,
    )?)))
}

fn eval_date_trunc_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let (date, unit, bin_size, timezone, start_of_week) = parse_date_trunc_expression_spec(value)?;
    let date = eval_expression_result_with_variables(document, date, variables)?;
    if date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }

    let unit = eval_expression_result_with_variables(document, unit, variables)?;
    if unit.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let unit = parse_date_unit_operand(unit, "$dateTrunc")?;

    let bin_size = match bin_size {
        Some(bin_size) => {
            let value = eval_expression_result_with_variables(document, bin_size, variables)?;
            if value.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            let value = require_integral_i64_operand(value, "$dateTrunc", "binSize")?;
            if value <= 0 {
                return Err(QueryError::InvalidArgument(
                    "$dateTrunc requires `binSize` to evaluate to a positive integral value"
                        .to_string(),
                ));
            }
            value
        }
        None => 1,
    };

    let timezone = resolve_timezone_expression(document, timezone, variables, "$dateTrunc")?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let date = coerce_date_expression_value(date, "$dateTrunc", "date")?;

    let start_of_week = if unit == DateUnit::Week {
        match start_of_week {
            Some(start_of_week) => {
                let value =
                    eval_expression_result_with_variables(document, start_of_week, variables)?;
                if value.is_nullish() {
                    return Ok(EvaluatedExpression::Value(Bson::Null));
                }
                parse_start_of_week_operand(value, "$dateTrunc", "startOfWeek")?
            }
            None => Weekday::Sun,
        }
    } else {
        Weekday::Sun
    };

    let result = truncate_date_unit(date, unit, bin_size, &timezone, start_of_week, "$dateTrunc")?;
    Ok(EvaluatedExpression::Value(Bson::DateTime(
        bson::DateTime::from_millis(result.timestamp_millis()),
    )))
}

fn eval_date_from_parts_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = parse_date_from_parts_expression_spec(value)?;

    let hour = match eval_date_from_parts_component(
        document,
        spec.hour,
        variables,
        "$dateFromParts",
        "hour",
        0,
        Some((0, 23)),
    )? {
        Some(hour) => hour,
        None => return Ok(EvaluatedExpression::Value(Bson::Null)),
    };
    let minute = match eval_date_from_parts_component(
        document,
        spec.minute,
        variables,
        "$dateFromParts",
        "minute",
        0,
        Some((0, 59)),
    )? {
        Some(minute) => minute,
        None => return Ok(EvaluatedExpression::Value(Bson::Null)),
    };
    let second = match eval_date_from_parts_component(
        document,
        spec.second,
        variables,
        "$dateFromParts",
        "second",
        0,
        Some((0, 59)),
    )? {
        Some(second) => second,
        None => return Ok(EvaluatedExpression::Value(Bson::Null)),
    };
    let millisecond = match eval_date_from_parts_component(
        document,
        spec.millisecond,
        variables,
        "$dateFromParts",
        "millisecond",
        0,
        Some((0, 999)),
    )? {
        Some(millisecond) => millisecond,
        None => return Ok(EvaluatedExpression::Value(Bson::Null)),
    };

    let timezone =
        resolve_timezone_expression(document, spec.timezone, variables, "$dateFromParts")?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    let local = if let Some(year) = spec.year {
        let year = match eval_date_from_parts_component(
            document,
            Some(year),
            variables,
            "$dateFromParts",
            "year",
            1970,
            Some((1, 9999)),
        )? {
            Some(year) => year,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        let month = match eval_date_from_parts_component(
            document,
            spec.month,
            variables,
            "$dateFromParts",
            "month",
            1,
            Some((1, 12)),
        )? {
            Some(month) => month,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        let day = match eval_date_from_parts_component(
            document,
            spec.day,
            variables,
            "$dateFromParts",
            "day",
            1,
            Some((1, 31)),
        )? {
            Some(day) => day,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
            .and_then(|date| {
                date.and_hms_milli_opt(
                    hour as u32,
                    minute as u32,
                    second as u32,
                    millisecond as u32,
                )
            })
            .ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$dateFromParts produced an invalid calendar date".to_string(),
                )
            })?
    } else {
        let iso_week_year = match eval_date_from_parts_component(
            document,
            spec.iso_week_year,
            variables,
            "$dateFromParts",
            "isoWeekYear",
            1970,
            Some((1, 9999)),
        )? {
            Some(year) => year,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        let iso_week = match eval_date_from_parts_component(
            document,
            spec.iso_week,
            variables,
            "$dateFromParts",
            "isoWeek",
            1,
            Some((1, 53)),
        )? {
            Some(week) => week,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        let iso_day_of_week = match eval_date_from_parts_component(
            document,
            spec.iso_day_of_week,
            variables,
            "$dateFromParts",
            "isoDayOfWeek",
            1,
            Some((1, 7)),
        )? {
            Some(day) => day,
            None => return Ok(EvaluatedExpression::Value(Bson::Null)),
        };
        let weekday = iso_weekday(iso_day_of_week as u32)?;
        NaiveDate::from_isoywd_opt(iso_week_year as i32, iso_week as u32, weekday)
            .and_then(|date| {
                date.and_hms_milli_opt(
                    hour as u32,
                    minute as u32,
                    second as u32,
                    millisecond as u32,
                )
            })
            .ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$dateFromParts produced an invalid ISO-8601 date".to_string(),
                )
            })?
    };

    let result = resolve_local_datetime(&timezone, local, "$dateFromParts")?;
    Ok(EvaluatedExpression::Value(Bson::DateTime(
        bson::DateTime::from_millis(result.timestamp_millis()),
    )))
}

fn eval_date_from_string_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = parse_date_from_string_expression_spec(value)?;
    let date_string = eval_expression_result_with_variables(document, spec.date_string, variables)?;
    if date_string.is_nullish() {
        return match spec.on_null {
            Some(on_null) => eval_expression_result_with_variables(document, on_null, variables),
            None => Ok(EvaluatedExpression::Value(Bson::Null)),
        };
    }

    let date_string = match date_string {
        EvaluatedExpression::Value(Bson::String(value)) => value,
        _ => {
            return match spec.on_error {
                Some(on_error) => {
                    eval_expression_result_with_variables(document, on_error, variables)
                }
                None => Err(QueryError::InvalidArgument(
                    "$dateFromString requires `dateString` to evaluate to a string".to_string(),
                )),
            };
        }
    };

    let format = match spec.format {
        Some(format) => {
            let format = eval_expression_result_with_variables(document, format, variables)?;
            if format.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            Some(require_named_string_operand(
                format,
                "$dateFromString",
                "format",
            )?)
        }
        None => None,
    };

    let timezone = match spec.timezone {
        Some(timezone) => {
            let timezone = eval_expression_result_with_variables(document, timezone, variables)?;
            if timezone.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            let timezone = require_named_string_operand(timezone, "$dateFromString", "timezone")?;
            Some(parse_timezone(&timezone).ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$dateFromString requires a valid timezone string".to_string(),
                )
            })?)
        }
        None => None,
    };

    let result =
        match parse_date_from_string_input(&date_string, format.as_deref(), timezone.as_ref()) {
            Ok(result) => result,
            Err(error) => {
                return match spec.on_error {
                    Some(on_error) => {
                        eval_expression_result_with_variables(document, on_error, variables)
                    }
                    None => Err(error),
                };
            }
        };

    Ok(EvaluatedExpression::Value(Bson::DateTime(
        bson::DateTime::from_millis(result.timestamp_millis()),
    )))
}

fn eval_date_to_string_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = parse_date_to_string_expression_spec(value)?;
    let date = eval_expression_result_with_variables(document, spec.date, variables)?;
    if date.is_nullish() {
        return match spec.on_null {
            Some(on_null) => eval_expression_result_with_variables(document, on_null, variables),
            None => Ok(EvaluatedExpression::Value(Bson::Null)),
        };
    }

    let format = match spec.format {
        Some(format) => {
            let format = eval_expression_result_with_variables(document, format, variables)?;
            if format.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            Some(require_named_string_operand(
                format,
                "$dateToString",
                "format",
            )?)
        }
        None => None,
    };

    let timezone = match spec.timezone {
        Some(timezone) => {
            let timezone = eval_expression_result_with_variables(document, timezone, variables)?;
            if timezone.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            let timezone = require_named_string_operand(timezone, "$dateToString", "timezone")?;
            parse_timezone(&timezone).ok_or_else(|| {
                QueryError::InvalidArgument(
                    "$dateToString requires a valid timezone string".to_string(),
                )
            })?
        }
        None => ResolvedTimeZone::Utc,
    };

    let date = coerce_date_expression_value(date, "$dateToString", "date")?;
    let rendered = match format.as_deref() {
        Some(format) => format_date_to_string(date, &timezone, format)?,
        None => default_date_to_string(date, &timezone),
    };
    Ok(EvaluatedExpression::Value(Bson::String(rendered)))
}

fn eval_date_to_parts_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
) -> Result<EvaluatedExpression, QueryError> {
    let spec = parse_date_to_parts_expression_spec(value)?;
    let timezone = resolve_timezone_expression(document, spec.timezone, variables, "$dateToParts")?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };

    let iso8601 = match spec.iso8601 {
        Some(iso8601) => {
            let iso8601 = eval_expression_result_with_variables(document, iso8601, variables)?;
            if iso8601.is_nullish() {
                return Ok(EvaluatedExpression::Value(Bson::Null));
            }
            match iso8601 {
                EvaluatedExpression::Value(Bson::Boolean(value)) => value,
                _ => {
                    return Err(QueryError::InvalidArgument(
                        "$dateToParts requires `iso8601` to evaluate to a boolean".to_string(),
                    ));
                }
            }
        }
        None => false,
    };

    let date = eval_expression_result_with_variables(document, spec.date, variables)?;
    if date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let date = coerce_date_expression_value(date, "$dateToParts", "date")?;
    let parts = date_parts_in_timezone(date, &timezone);
    let mut result = Document::new();
    if iso8601 {
        result.insert("isoWeekYear", parts.iso_week_year);
        result.insert("isoWeek", parts.iso_week as i32);
        result.insert("isoDayOfWeek", parts.iso_day_of_week as i32);
    } else {
        result.insert("year", parts.year);
        result.insert("month", parts.month as i32);
        result.insert("day", parts.day_of_month as i32);
    }
    result.insert("hour", parts.hour as i32);
    result.insert("minute", parts.minute as i32);
    result.insert("second", parts.second as i32);
    result.insert("millisecond", parts.millisecond as i32);
    Ok(EvaluatedExpression::Value(Bson::Document(result)))
}

fn eval_date_part_expression(
    document: &Document,
    value: &Bson,
    variables: &BTreeMap<String, Bson>,
    mode: DatePartExpressionMode,
) -> Result<EvaluatedExpression, QueryError> {
    let operator = mode.name();
    let (date, timezone) = parse_date_part_expression_spec(value)?;
    let date = eval_expression_result_with_variables(document, date, variables)?;
    if date.is_nullish() {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    }
    let date = coerce_date_part_value(date, operator)?;
    let timezone = resolve_timezone_expression(document, timezone, variables, operator)?;
    let Some(timezone) = timezone else {
        return Ok(EvaluatedExpression::Value(Bson::Null));
    };
    let parts = date_parts_in_timezone(date, &timezone);

    Ok(EvaluatedExpression::Value(match mode {
        DatePartExpressionMode::DayOfMonth => Bson::Int32(parts.day_of_month as i32),
        DatePartExpressionMode::DayOfWeek => Bson::Int32(parts.day_of_week as i32),
        DatePartExpressionMode::DayOfYear => Bson::Int32(parts.day_of_year as i32),
        DatePartExpressionMode::Hour => Bson::Int32(parts.hour as i32),
        DatePartExpressionMode::IsoDayOfWeek => Bson::Int32(parts.iso_day_of_week as i32),
        DatePartExpressionMode::IsoWeek => Bson::Int32(parts.iso_week as i32),
        DatePartExpressionMode::IsoWeekYear => Bson::Int32(parts.iso_week_year),
        DatePartExpressionMode::Millisecond => Bson::Int32(parts.millisecond as i32),
        DatePartExpressionMode::Minute => Bson::Int32(parts.minute as i32),
        DatePartExpressionMode::Month => Bson::Int32(parts.month as i32),
        DatePartExpressionMode::Second => Bson::Int32(parts.second as i32),
        DatePartExpressionMode::Week => Bson::Int32(parts.week as i32),
        DatePartExpressionMode::Year => Bson::Int32(parts.year),
    }))
}

#[derive(Clone)]
enum ResolvedTimeZone {
    Utc,
    Fixed(FixedOffset),
    Named(Tz),
}

#[derive(Clone, Copy)]
struct DateParts {
    year: i32,
    month: u32,
    day_of_month: u32,
    day_of_week: u32,
    day_of_year: u32,
    hour: u32,
    iso_day_of_week: u32,
    iso_week: u32,
    iso_week_year: i32,
    millisecond: u32,
    minute: u32,
    second: u32,
    week: u32,
}

type DateArithmeticExpressionSpec<'a> = (&'a Bson, &'a Bson, &'a Bson, Option<&'a Bson>);
type DateDiffExpressionSpec<'a> = (
    &'a Bson,
    &'a Bson,
    &'a Bson,
    Option<&'a Bson>,
    Option<&'a Bson>,
);
type DateTruncExpressionSpec<'a> = (
    &'a Bson,
    &'a Bson,
    Option<&'a Bson>,
    Option<&'a Bson>,
    Option<&'a Bson>,
);

struct DateFromPartsSpec<'a> {
    year: Option<&'a Bson>,
    month: Option<&'a Bson>,
    day: Option<&'a Bson>,
    hour: Option<&'a Bson>,
    minute: Option<&'a Bson>,
    second: Option<&'a Bson>,
    millisecond: Option<&'a Bson>,
    iso_week_year: Option<&'a Bson>,
    iso_week: Option<&'a Bson>,
    iso_day_of_week: Option<&'a Bson>,
    timezone: Option<&'a Bson>,
}

struct DateFromStringSpec<'a> {
    date_string: &'a Bson,
    timezone: Option<&'a Bson>,
    format: Option<&'a Bson>,
    on_null: Option<&'a Bson>,
    on_error: Option<&'a Bson>,
}

struct DateToStringSpec<'a> {
    date: &'a Bson,
    format: Option<&'a Bson>,
    timezone: Option<&'a Bson>,
    on_null: Option<&'a Bson>,
}

struct DateToPartsSpec<'a> {
    date: &'a Bson,
    timezone: Option<&'a Bson>,
    iso8601: Option<&'a Bson>,
}

fn resolve_timezone_expression(
    document: &Document,
    timezone: Option<&Bson>,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
) -> Result<Option<ResolvedTimeZone>, QueryError> {
    let Some(timezone) = timezone else {
        return Ok(Some(ResolvedTimeZone::Utc));
    };
    let timezone = eval_expression_result_with_variables(document, timezone, variables)?;
    if timezone.is_nullish() {
        return Ok(None);
    }
    let timezone = require_named_string_operand(timezone, operator, "timezone")?;
    parse_timezone(&timezone).map(Some).ok_or_else(|| {
        QueryError::InvalidArgument(format!("{operator} requires a valid timezone string"))
    })
}

fn parse_date_arithmetic_expression_spec<'a>(
    value: &'a Bson,
    operator: &str,
) -> Result<DateArithmeticExpressionSpec<'a>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut start_date = None;
    let mut unit = None;
    let mut amount = None;
    let mut timezone = None;

    for (field, value) in spec {
        match field.as_str() {
            "startDate" => start_date = Some(value),
            "unit" => unit = Some(value),
            "amount" => amount = Some(value),
            "timezone" => timezone = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to {operator}: {field}"
                )));
            }
        }
    }

    Ok((
        start_date.ok_or(QueryError::InvalidStructure)?,
        unit.ok_or(QueryError::InvalidStructure)?,
        amount.ok_or(QueryError::InvalidStructure)?,
        timezone,
    ))
}

fn parse_date_diff_expression_spec(value: &Bson) -> Result<DateDiffExpressionSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut start_date = None;
    let mut end_date = None;
    let mut unit = None;
    let mut timezone = None;
    let mut start_of_week = None;

    for (field, value) in spec {
        match field.as_str() {
            "startDate" => start_date = Some(value),
            "endDate" => end_date = Some(value),
            "unit" => unit = Some(value),
            "timezone" => timezone = Some(value),
            "startOfWeek" => start_of_week = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateDiff: {field}"
                )));
            }
        }
    }

    Ok((
        start_date.ok_or(QueryError::InvalidStructure)?,
        end_date.ok_or(QueryError::InvalidStructure)?,
        unit.ok_or(QueryError::InvalidStructure)?,
        timezone,
        start_of_week,
    ))
}

fn parse_date_trunc_expression_spec(
    value: &Bson,
) -> Result<DateTruncExpressionSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut date = None;
    let mut unit = None;
    let mut bin_size = None;
    let mut timezone = None;
    let mut start_of_week = None;

    for (field, value) in spec {
        match field.as_str() {
            "date" => date = Some(value),
            "unit" => unit = Some(value),
            "binSize" => bin_size = Some(value),
            "timezone" => timezone = Some(value),
            "startOfWeek" => start_of_week = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateTrunc: {field}"
                )));
            }
        }
    }

    Ok((
        date.ok_or(QueryError::InvalidStructure)?,
        unit.ok_or(QueryError::InvalidStructure)?,
        bin_size,
        timezone,
        start_of_week,
    ))
}

fn parse_date_from_parts_expression_spec(
    value: &Bson,
) -> Result<DateFromPartsSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut parsed = DateFromPartsSpec {
        year: None,
        month: None,
        day: None,
        hour: None,
        minute: None,
        second: None,
        millisecond: None,
        iso_week_year: None,
        iso_week: None,
        iso_day_of_week: None,
        timezone: None,
    };

    for (field, value) in spec {
        match field.as_str() {
            "year" => parsed.year = Some(value),
            "month" => parsed.month = Some(value),
            "day" => parsed.day = Some(value),
            "hour" => parsed.hour = Some(value),
            "minute" => parsed.minute = Some(value),
            "second" => parsed.second = Some(value),
            "millisecond" => parsed.millisecond = Some(value),
            "isoWeekYear" => parsed.iso_week_year = Some(value),
            "isoWeek" => parsed.iso_week = Some(value),
            "isoDayOfWeek" => parsed.iso_day_of_week = Some(value),
            "timezone" => parsed.timezone = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateFromParts: {field}"
                )));
            }
        }
    }

    if parsed.year.is_none() && parsed.iso_week_year.is_none() {
        return Err(QueryError::InvalidStructure);
    }
    if parsed.year.is_some()
        && (parsed.iso_week_year.is_some()
            || parsed.iso_week.is_some()
            || parsed.iso_day_of_week.is_some())
    {
        return Err(QueryError::InvalidStructure);
    }
    if parsed.iso_week_year.is_some() && (parsed.month.is_some() || parsed.day.is_some()) {
        return Err(QueryError::InvalidStructure);
    }

    Ok(parsed)
}

fn parse_date_from_string_expression_spec(
    value: &Bson,
) -> Result<DateFromStringSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut date_string = None;
    let mut timezone = None;
    let mut format = None;
    let mut on_null = None;
    let mut on_error = None;

    for (field, value) in spec {
        match field.as_str() {
            "dateString" => date_string = Some(value),
            "timezone" => timezone = Some(value),
            "format" => format = Some(value),
            "onNull" => on_null = Some(value),
            "onError" => on_error = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateFromString: {field}"
                )));
            }
        }
    }

    Ok(DateFromStringSpec {
        date_string: date_string.ok_or(QueryError::InvalidStructure)?,
        timezone,
        format,
        on_null,
        on_error,
    })
}

fn parse_date_to_string_expression_spec(value: &Bson) -> Result<DateToStringSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut date = None;
    let mut format = None;
    let mut timezone = None;
    let mut on_null = None;

    for (field, value) in spec {
        match field.as_str() {
            "date" => date = Some(value),
            "format" => format = Some(value),
            "timezone" => timezone = Some(value),
            "onNull" => on_null = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateToString: {field}"
                )));
            }
        }
    }

    Ok(DateToStringSpec {
        date: date.ok_or(QueryError::InvalidStructure)?,
        format,
        timezone,
        on_null,
    })
}

fn parse_date_to_parts_expression_spec(value: &Bson) -> Result<DateToPartsSpec<'_>, QueryError> {
    let spec = value.as_document().ok_or(QueryError::InvalidStructure)?;
    let mut date = None;
    let mut timezone = None;
    let mut iso8601 = None;

    for (field, value) in spec {
        match field.as_str() {
            "date" => date = Some(value),
            "timezone" => timezone = Some(value),
            "iso8601" => iso8601 = Some(value),
            _ => {
                return Err(QueryError::InvalidArgument(format!(
                    "Unrecognized argument to $dateToParts: {field}"
                )));
            }
        }
    }

    Ok(DateToPartsSpec {
        date: date.ok_or(QueryError::InvalidStructure)?,
        timezone,
        iso8601,
    })
}

fn validate_date_arithmetic_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
    operator: &str,
) -> Result<(), QueryError> {
    let (start_date, unit, amount, timezone) =
        parse_date_arithmetic_expression_spec(value, operator)?;
    validate_expression_with_scope(start_date, scope)?;
    validate_expression_with_scope(unit, scope)?;
    validate_expression_with_scope(amount, scope)?;
    if let Some(timezone) = timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    Ok(())
}

fn validate_date_diff_expression(value: &Bson, scope: &BTreeSet<String>) -> Result<(), QueryError> {
    let (start_date, end_date, unit, timezone, start_of_week) =
        parse_date_diff_expression_spec(value)?;
    validate_expression_with_scope(start_date, scope)?;
    validate_expression_with_scope(end_date, scope)?;
    validate_expression_with_scope(unit, scope)?;
    if let Some(timezone) = timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    if let Some(start_of_week) = start_of_week {
        validate_expression_with_scope(start_of_week, scope)?;
    }
    Ok(())
}

fn validate_date_from_parts_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let spec = parse_date_from_parts_expression_spec(value)?;
    for value in [
        spec.year,
        spec.month,
        spec.day,
        spec.hour,
        spec.minute,
        spec.second,
        spec.millisecond,
        spec.iso_week_year,
        spec.iso_week,
        spec.iso_day_of_week,
        spec.timezone,
    ]
    .into_iter()
    .flatten()
    {
        validate_expression_with_scope(value, scope)?;
    }
    Ok(())
}

fn validate_date_from_string_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let spec = parse_date_from_string_expression_spec(value)?;
    validate_expression_with_scope(spec.date_string, scope)?;
    if let Some(timezone) = spec.timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    if let Some(format) = spec.format {
        validate_expression_with_scope(format, scope)?;
    }
    if let Some(on_null) = spec.on_null {
        validate_expression_with_scope(on_null, scope)?;
    }
    if let Some(on_error) = spec.on_error {
        validate_expression_with_scope(on_error, scope)?;
    }
    Ok(())
}

fn validate_date_to_string_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let spec = parse_date_to_string_expression_spec(value)?;
    validate_expression_with_scope(spec.date, scope)?;
    if let Some(format) = spec.format {
        validate_expression_with_scope(format, scope)?;
    }
    if let Some(timezone) = spec.timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    if let Some(on_null) = spec.on_null {
        validate_expression_with_scope(on_null, scope)?;
    }
    Ok(())
}

fn validate_date_to_parts_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let spec = parse_date_to_parts_expression_spec(value)?;
    validate_expression_with_scope(spec.date, scope)?;
    if let Some(timezone) = spec.timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    if let Some(iso8601) = spec.iso8601 {
        validate_expression_with_scope(iso8601, scope)?;
    }
    Ok(())
}

fn validate_date_trunc_expression(
    value: &Bson,
    scope: &BTreeSet<String>,
) -> Result<(), QueryError> {
    let (date, unit, bin_size, timezone, start_of_week) = parse_date_trunc_expression_spec(value)?;
    validate_expression_with_scope(date, scope)?;
    validate_expression_with_scope(unit, scope)?;
    if let Some(bin_size) = bin_size {
        validate_expression_with_scope(bin_size, scope)?;
    }
    if let Some(timezone) = timezone {
        validate_expression_with_scope(timezone, scope)?;
    }
    if let Some(start_of_week) = start_of_week {
        validate_expression_with_scope(start_of_week, scope)?;
    }
    Ok(())
}

fn parse_timezone(value: &str) -> Option<ResolvedTimeZone> {
    if matches!(value, "UTC" | "GMT" | "Z") {
        return Some(ResolvedTimeZone::Utc);
    }
    if let Some(offset) = parse_fixed_offset(value) {
        return Some(ResolvedTimeZone::Fixed(offset));
    }
    value.parse::<Tz>().ok().map(ResolvedTimeZone::Named)
}

fn parse_date_from_string_input(
    input: &str,
    format: Option<&str>,
    timezone: Option<&ResolvedTimeZone>,
) -> Result<DateTime<Utc>, QueryError> {
    match format {
        Some(format) => parse_date_from_string_with_format(input, format, timezone),
        None => parse_date_from_string_default(input, timezone),
    }
}

fn parse_date_from_string_default(
    input: &str,
    timezone: Option<&ResolvedTimeZone>,
) -> Result<DateTime<Utc>, QueryError> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(input) {
        if timezone.is_some() {
            return Err(QueryError::InvalidArgument(
                "$dateFromString cannot use a timezone argument when dateString contains timezone information".to_string(),
            ));
        }
        return Ok(parsed.with_timezone(&Utc));
    }

    for pattern in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(input, pattern) {
            let timezone = timezone.cloned().unwrap_or(ResolvedTimeZone::Utc);
            return resolve_local_datetime(&timezone, parsed, "$dateFromString");
        }
    }

    if let Ok(parsed) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let timezone = timezone.cloned().unwrap_or(ResolvedTimeZone::Utc);
        let local = parsed.and_hms_milli_opt(0, 0, 0, 0).expect("midnight");
        return resolve_local_datetime(&timezone, local, "$dateFromString");
    }

    Err(QueryError::InvalidArgument(
        "$dateFromString failed to parse the provided date string".to_string(),
    ))
}

fn parse_date_from_string_with_format(
    input: &str,
    format: &str,
    timezone: Option<&ResolvedTimeZone>,
) -> Result<DateTime<Utc>, QueryError> {
    let translated = translate_mongo_datetime_parse_format(format)?;
    if translated.has_timezone && timezone.is_some() {
        return Err(QueryError::InvalidArgument(
            "$dateFromString cannot use a timezone argument when the format includes timezone information".to_string(),
        ));
    }

    if translated.has_timezone {
        return DateTime::parse_from_str(input, &translated.chrono_format)
            .map(|parsed| parsed.with_timezone(&Utc))
            .map_err(|_| {
                QueryError::InvalidArgument(
                    "$dateFromString failed to parse the provided date string".to_string(),
                )
            });
    }

    if let Ok(parsed) = NaiveDateTime::parse_from_str(input, &translated.chrono_format) {
        let timezone = timezone.cloned().unwrap_or(ResolvedTimeZone::Utc);
        return resolve_local_datetime(&timezone, parsed, "$dateFromString");
    }

    NaiveDate::parse_from_str(input, &translated.chrono_format)
        .map_err(|_| {
            QueryError::InvalidArgument(
                "$dateFromString failed to parse the provided date string".to_string(),
            )
        })
        .and_then(|parsed| {
            let timezone = timezone.cloned().unwrap_or(ResolvedTimeZone::Utc);
            let local = parsed.and_hms_milli_opt(0, 0, 0, 0).expect("midnight");
            resolve_local_datetime(&timezone, local, "$dateFromString")
        })
}

struct MongoDateParseFormat {
    chrono_format: String,
    has_timezone: bool,
}

fn translate_mongo_datetime_parse_format(format: &str) -> Result<MongoDateParseFormat, QueryError> {
    let mut translated = String::new();
    let mut chars = format.chars().peekable();
    let mut has_timezone = false;
    while let Some(ch) = chars.next() {
        if ch != '%' {
            translated.push(ch);
            continue;
        }

        let directive = chars.next().ok_or_else(|| {
            QueryError::InvalidArgument("Invalid trailing '%' in format string".to_string())
        })?;
        match directive {
            '%' => translated.push_str("%%"),
            'Y' | 'm' | 'd' | 'H' | 'M' | 'S' | 'G' | 'V' | 'u' | 'j' | 'b' | 'B' => {
                translated.push('%');
                translated.push(directive);
            }
            'L' => {
                if translated.ends_with('.') {
                    translated.pop();
                    translated.push_str("%.3f");
                } else {
                    translated.push_str("%3f");
                }
            }
            'z' => {
                has_timezone = true;
                translated.push_str("%z");
            }
            other => {
                return Err(QueryError::InvalidArgument(format!(
                    "Invalid format character '%{other}' in format string"
                )));
            }
        }
    }

    Ok(MongoDateParseFormat {
        chrono_format: translated,
        has_timezone,
    })
}

fn default_date_to_string(date: DateTime<Utc>, timezone: &ResolvedTimeZone) -> String {
    match timezone {
        ResolvedTimeZone::Utc => format_date_to_string(date, timezone, "%Y-%m-%dT%H:%M:%S.%LZ")
            .expect("default UTC format"),
        _ => format_date_to_string(date, timezone, "%Y-%m-%dT%H:%M:%S.%L")
            .expect("default local format"),
    }
}

fn format_date_to_string(
    date: DateTime<Utc>,
    timezone: &ResolvedTimeZone,
    format: &str,
) -> Result<String, QueryError> {
    let offset_minutes = timezone_offset_minutes(date, timezone);
    let parts = date_parts_in_timezone(date, timezone);
    let mut rendered = String::new();
    let mut chars = format.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            rendered.push(ch);
            continue;
        }

        let directive = chars.next().ok_or_else(|| {
            QueryError::InvalidArgument("Invalid trailing '%' in format string".to_string())
        })?;
        match directive {
            '%' => rendered.push('%'),
            'Y' => rendered.push_str(&format!("{:04}", parts.year)),
            'm' => rendered.push_str(&format!("{:02}", parts.month)),
            'd' => rendered.push_str(&format!("{:02}", parts.day_of_month)),
            'H' => rendered.push_str(&format!("{:02}", parts.hour)),
            'M' => rendered.push_str(&format!("{:02}", parts.minute)),
            'S' => rendered.push_str(&format!("{:02}", parts.second)),
            'L' => rendered.push_str(&format!("{:03}", parts.millisecond)),
            'z' => {
                let sign = if offset_minutes < 0 { '-' } else { '+' };
                let total = offset_minutes.abs();
                rendered.push_str(&format!("{sign}{:02}{:02}", total / 60, total % 60));
            }
            'Z' => rendered.push_str(&offset_minutes.to_string()),
            'G' => rendered.push_str(&format!("{:04}", parts.iso_week_year)),
            'V' => rendered.push_str(&format!("{:02}", parts.iso_week)),
            'u' => rendered.push_str(&parts.iso_day_of_week.to_string()),
            'U' => rendered.push_str(&format!("{:02}", parts.week)),
            'w' => rendered.push_str(&parts.day_of_week.to_string()),
            'j' => rendered.push_str(&format!("{:03}", parts.day_of_year)),
            'b' => rendered.push_str(month_name(parts.month, true)),
            'B' => rendered.push_str(month_name(parts.month, false)),
            other => {
                return Err(QueryError::InvalidArgument(format!(
                    "Invalid format character '%{other}' in format string"
                )));
            }
        }
    }

    Ok(rendered)
}

fn timezone_offset_minutes(date: DateTime<Utc>, timezone: &ResolvedTimeZone) -> i32 {
    match timezone {
        ResolvedTimeZone::Utc => 0,
        ResolvedTimeZone::Fixed(offset) => offset.local_minus_utc() / 60,
        ResolvedTimeZone::Named(timezone) => {
            date.with_timezone(timezone)
                .offset()
                .fix()
                .local_minus_utc()
                / 60
        }
    }
}

fn month_name(month: u32, abbreviated: bool) -> &'static str {
    const SHORT: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    const LONG: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];

    let names = if abbreviated { &SHORT } else { &LONG };
    month
        .checked_sub(1)
        .and_then(|index| names.get(index as usize))
        .copied()
        .unwrap_or("")
}

fn parse_fixed_offset(value: &str) -> Option<FixedOffset> {
    let (sign, rest) = match value.as_bytes().first().copied() {
        Some(b'+') => (1, &value[1..]),
        Some(b'-') => (-1, &value[1..]),
        _ => return None,
    };

    let (hours, minutes) = if let Some((hours, minutes)) = rest.split_once(':') {
        (hours, minutes)
    } else if rest.len() == 2 {
        (rest, "00")
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else {
        return None;
    };

    let hours = hours.parse::<i32>().ok()?;
    let minutes = minutes.parse::<i32>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }

    let total_seconds = sign * ((hours * 60 + minutes) * 60);
    FixedOffset::east_opt(total_seconds)
}

fn parse_date_unit_operand(
    value: EvaluatedExpression,
    operator: &str,
) -> Result<DateUnit, QueryError> {
    let value = require_named_string_operand(value, operator, "unit")?;
    parse_date_unit(&value).ok_or_else(|| {
        QueryError::InvalidArgument(format!("{operator} requires a valid time unit string"))
    })
}

fn parse_date_unit(value: &str) -> Option<DateUnit> {
    match value {
        "millisecond" => Some(DateUnit::Millisecond),
        "second" => Some(DateUnit::Second),
        "minute" => Some(DateUnit::Minute),
        "hour" => Some(DateUnit::Hour),
        "day" => Some(DateUnit::Day),
        "week" => Some(DateUnit::Week),
        "month" => Some(DateUnit::Month),
        "quarter" => Some(DateUnit::Quarter),
        "year" => Some(DateUnit::Year),
        _ => None,
    }
}

fn parse_start_of_week_operand(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<Weekday, QueryError> {
    let value = require_named_string_operand(value, operator, field)?;
    parse_start_of_week(&value).ok_or_else(|| {
        QueryError::InvalidArgument(format!(
            "{operator} requires `{field}` to evaluate to a valid day of week string"
        ))
    })
}

fn parse_start_of_week(value: &str) -> Option<Weekday> {
    match value.to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Some(Weekday::Sun),
        "mon" | "monday" => Some(Weekday::Mon),
        "tue" | "tuesday" => Some(Weekday::Tue),
        "wed" | "wednesday" => Some(Weekday::Wed),
        "thu" | "thursday" => Some(Weekday::Thu),
        "fri" | "friday" => Some(Weekday::Fri),
        "sat" | "saturday" => Some(Weekday::Sat),
        _ => None,
    }
}

fn require_integral_i64_operand(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<i64, QueryError> {
    let value = match value {
        EvaluatedExpression::Value(value) => value,
        EvaluatedExpression::Missing => {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires `{field}` to evaluate to an integral numeric value"
            )));
        }
    };

    integral_numeric_i64(&value).ok_or_else(|| {
        QueryError::InvalidArgument(format!(
            "{operator} requires `{field}` to evaluate to an integral numeric value"
        ))
    })
}

fn eval_date_from_parts_component(
    document: &Document,
    expression: Option<&Bson>,
    variables: &BTreeMap<String, Bson>,
    operator: &str,
    field: &str,
    default: i64,
    bounds: Option<(i64, i64)>,
) -> Result<Option<i64>, QueryError> {
    let Some(expression) = expression else {
        return Ok(Some(default));
    };
    let value = eval_expression_result_with_variables(document, expression, variables)?;
    if value.is_nullish() {
        return Ok(None);
    }
    let value = require_integral_i64_operand(value, operator, field)?;
    if let Some((lower, upper)) = bounds {
        if value < lower || value > upper {
            return Err(QueryError::InvalidArgument(format!(
                "{operator} requires `{field}` to evaluate to an integer in the range {lower} to {upper}"
            )));
        }
    }
    Ok(Some(value))
}

fn iso_weekday(value: u32) -> Result<Weekday, QueryError> {
    match value {
        1 => Ok(Weekday::Mon),
        2 => Ok(Weekday::Tue),
        3 => Ok(Weekday::Wed),
        4 => Ok(Weekday::Thu),
        5 => Ok(Weekday::Fri),
        6 => Ok(Weekday::Sat),
        7 => Ok(Weekday::Sun),
        _ => Err(QueryError::InvalidArgument(
            "$dateFromParts requires `isoDayOfWeek` to evaluate to an integer in the range 1 to 7"
                .to_string(),
        )),
    }
}

fn coerce_date_part_value(
    value: EvaluatedExpression,
    operator: &str,
) -> Result<DateTime<Utc>, QueryError> {
    coerce_date_expression_value_with_field(value, operator, "date")
}

fn coerce_date_expression_value_with_field(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<DateTime<Utc>, QueryError> {
    let value = match value {
        EvaluatedExpression::Value(value) => value,
        EvaluatedExpression::Missing => return Ok(DateTime::<Utc>::UNIX_EPOCH),
    };

    match value {
        Bson::DateTime(value) => DateTime::<Utc>::from_timestamp_millis(value.timestamp_millis())
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "{operator} requires `{field}` to evaluate to a date, timestamp, or objectId input"
                ))
            }),
        Bson::Timestamp(value) => {
            DateTime::<Utc>::from_timestamp(value.time as i64, 0).ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "{operator} requires `{field}` to evaluate to a date, timestamp, or objectId input"
                ))
            })
        }
        Bson::ObjectId(value) => DateTime::<Utc>::from_timestamp_millis(
            value.timestamp().timestamp_millis(),
        )
        .ok_or_else(|| {
            QueryError::InvalidArgument(format!(
                "{operator} requires `{field}` to evaluate to a date, timestamp, or objectId input"
            ))
        }),
        _ => Err(QueryError::InvalidArgument(format!(
            "{operator} requires `{field}` to evaluate to a date, timestamp, or objectId input"
        ))),
    }
}

fn coerce_date_expression_value(
    value: EvaluatedExpression,
    operator: &str,
    field: &str,
) -> Result<DateTime<Utc>, QueryError> {
    coerce_date_expression_value_with_field(value, operator, field)
}

fn date_parts_in_timezone(date: DateTime<Utc>, timezone: &ResolvedTimeZone) -> DateParts {
    match timezone {
        ResolvedTimeZone::Utc => extract_date_parts(date),
        ResolvedTimeZone::Fixed(offset) => extract_date_parts(date.with_timezone(offset)),
        ResolvedTimeZone::Named(timezone) => extract_date_parts(date.with_timezone(timezone)),
    }
}

fn naive_in_timezone(date: DateTime<Utc>, timezone: &ResolvedTimeZone) -> NaiveDateTime {
    match timezone {
        ResolvedTimeZone::Utc => date.naive_utc(),
        ResolvedTimeZone::Fixed(offset) => date.with_timezone(offset).naive_local(),
        ResolvedTimeZone::Named(timezone) => date.with_timezone(timezone).naive_local(),
    }
}

fn resolve_local_datetime(
    timezone: &ResolvedTimeZone,
    naive: NaiveDateTime,
    operator: &str,
) -> Result<DateTime<Utc>, QueryError> {
    let resolved = match timezone {
        ResolvedTimeZone::Utc => DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc),
        ResolvedTimeZone::Fixed(offset) => match offset.from_local_datetime(&naive) {
            LocalResult::Single(date) => return Ok(date.with_timezone(&Utc)),
            LocalResult::Ambiguous(date, _) => return Ok(date.with_timezone(&Utc)),
            LocalResult::None => {
                return Err(QueryError::InvalidArgument(format!(
                    "{operator} produced an invalid local datetime"
                )));
            }
        },
        ResolvedTimeZone::Named(timezone) => match timezone.from_local_datetime(&naive) {
            LocalResult::Single(date) => return Ok(date.with_timezone(&Utc)),
            LocalResult::Ambiguous(date, _) => return Ok(date.with_timezone(&Utc)),
            LocalResult::None => {
                return Err(QueryError::InvalidArgument(format!(
                    "{operator} produced an invalid local datetime"
                )));
            }
        },
    };
    Ok(resolved)
}

fn date_add_units(
    date: DateTime<Utc>,
    unit: DateUnit,
    amount: i64,
    timezone: &ResolvedTimeZone,
    operator: &str,
) -> Result<DateTime<Utc>, QueryError> {
    let local = naive_in_timezone(date, timezone);
    let shifted = match unit {
        DateUnit::Millisecond => local + Duration::milliseconds(amount),
        DateUnit::Second => local + Duration::seconds(amount),
        DateUnit::Minute => local + Duration::minutes(amount),
        DateUnit::Hour => local + Duration::hours(amount),
        DateUnit::Day => local + Duration::days(amount),
        DateUnit::Week => local + Duration::weeks(amount),
        DateUnit::Month => add_months_to_local_datetime(local, amount, operator)?,
        DateUnit::Quarter => add_months_to_local_datetime(
            local,
            amount.checked_mul(3).ok_or_else(|| {
                QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
            })?,
            operator,
        )?,
        DateUnit::Year => add_months_to_local_datetime(
            local,
            amount.checked_mul(12).ok_or_else(|| {
                QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
            })?,
            operator,
        )?,
    };
    resolve_local_datetime(timezone, shifted, operator)
}

fn add_months_to_local_datetime(
    local: NaiveDateTime,
    months: i64,
    operator: &str,
) -> Result<NaiveDateTime, QueryError> {
    let year_month = local.year().saturating_mul(12) + local.month0() as i32;
    let target_month_index = (year_month as i64).checked_add(months).ok_or_else(|| {
        QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
    })?;
    let target_year = target_month_index.div_euclid(12);
    let target_month0 = target_month_index.rem_euclid(12);
    let target_year = i32::try_from(target_year).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
    })?;
    let target_month = u32::try_from(target_month0 + 1).map_err(|_| {
        QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
    })?;
    let day = local
        .day()
        .min(last_day_of_month(target_year, target_month));
    let date = NaiveDate::from_ymd_opt(target_year, target_month, day).ok_or_else(|| {
        QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
    })?;
    date.and_hms_milli_opt(
        local.hour(),
        local.minute(),
        local.second(),
        local.and_utc().timestamp_subsec_millis(),
    )
    .ok_or_else(|| {
        QueryError::InvalidArgument(format!("{operator} overflowed during date arithmetic"))
    })
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    for day in (28..=31).rev() {
        if NaiveDate::from_ymd_opt(year, month, day).is_some() {
            return day;
        }
    }
    28
}

fn date_diff_units(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    unit: DateUnit,
    timezone: &ResolvedTimeZone,
    start_of_week: Weekday,
) -> Result<i64, QueryError> {
    let start = naive_in_timezone(start_date, timezone);
    let end = naive_in_timezone(end_date, timezone);
    Ok(match unit {
        DateUnit::Millisecond => (end - start).num_milliseconds(),
        DateUnit::Second => (end - start).num_seconds(),
        DateUnit::Minute => (end - start).num_minutes(),
        DateUnit::Hour => (end - start).num_hours(),
        DateUnit::Day => end.date().signed_duration_since(start.date()).num_days(),
        DateUnit::Week => {
            let start = start_of_week_date(start.date(), start_of_week);
            let end = start_of_week_date(end.date(), start_of_week);
            end.signed_duration_since(start).num_days() / 7
        }
        DateUnit::Month => calendar_month_difference(start, end)?,
        DateUnit::Quarter => calendar_month_difference(start, end)? / 3,
        DateUnit::Year => calendar_month_difference(start, end)? / 12,
    })
}

fn calendar_month_difference(start: NaiveDateTime, end: NaiveDateTime) -> Result<i64, QueryError> {
    let mut months =
        ((end.year() - start.year()) as i64) * 12 + end.month0() as i64 - start.month0() as i64;
    if months > 0 && add_months_to_local_datetime(start, months, "$dateDiff")? > end {
        months -= 1;
    } else if months < 0 && add_months_to_local_datetime(start, months, "$dateDiff")? < end {
        months += 1;
    }
    Ok(months)
}

fn truncate_date_unit(
    date: DateTime<Utc>,
    unit: DateUnit,
    bin_size: i64,
    timezone: &ResolvedTimeZone,
    start_of_week: Weekday,
    operator: &str,
) -> Result<DateTime<Utc>, QueryError> {
    let local = naive_in_timezone(date, timezone);
    let truncated = match unit {
        DateUnit::Millisecond => {
            let millisecond =
                (local.and_utc().timestamp_subsec_millis() as i64 / bin_size) * bin_size;
            local
                .with_nanosecond((millisecond as u32) * 1_000_000)
                .ok_or_else(|| {
                    QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
                })?
        }
        DateUnit::Second => local
            .with_second(((local.second() as i64 / bin_size) * bin_size) as u32)
            .and_then(|value| value.with_nanosecond(0))
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
            })?,
        DateUnit::Minute => local
            .with_minute(((local.minute() as i64 / bin_size) * bin_size) as u32)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
            })?,
        DateUnit::Hour => local
            .with_hour(((local.hour() as i64 / bin_size) * bin_size) as u32)
            .and_then(|value| value.with_minute(0))
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
            })?,
        DateUnit::Day => {
            let day = (((local.day() as i64) - 1) / bin_size) * bin_size + 1;
            NaiveDate::from_ymd_opt(local.year(), local.month(), day as u32)
                .and_then(|date| date.and_hms_milli_opt(0, 0, 0, 0))
                .ok_or_else(|| {
                    QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
                })?
        }
        DateUnit::Week => {
            let current_week = start_of_week_date(local.date(), start_of_week);
            let anchor = start_of_week_date(
                NaiveDate::from_ymd_opt(1970, 1, 4).expect("anchor"),
                start_of_week,
            );
            let weeks_since = current_week.signed_duration_since(anchor).num_days() / 7;
            let floored_weeks = weeks_since.div_euclid(bin_size) * bin_size;
            anchor
                .and_hms_milli_opt(0, 0, 0, 0)
                .expect("anchor datetime")
                + Duration::weeks(floored_weeks)
        }
        DateUnit::Month => truncate_month_like(local, bin_size, 1, operator)?,
        DateUnit::Quarter => truncate_month_like(local, bin_size * 3, 1, operator)?,
        DateUnit::Year => {
            let year = (i64::from(local.year() - 1)).div_euclid(bin_size) * bin_size + 1;
            NaiveDate::from_ymd_opt(year as i32, 1, 1)
                .and_then(|date| date.and_hms_milli_opt(0, 0, 0, 0))
                .ok_or_else(|| {
                    QueryError::InvalidArgument(format!("{operator} failed to truncate date"))
                })?
        }
    };
    resolve_local_datetime(timezone, truncated, operator)
}

fn truncate_month_like(
    local: NaiveDateTime,
    bin_size_months: i64,
    anchor_month: u32,
    operator: &str,
) -> Result<NaiveDateTime, QueryError> {
    let total_months = i64::from(local.year() - 1) * 12 + i64::from(local.month() - anchor_month);
    let floored_months = total_months.div_euclid(bin_size_months) * bin_size_months;
    let year = floored_months.div_euclid(12) + 1;
    let month = floored_months.rem_euclid(12) + i64::from(anchor_month);
    let carry = (month - 1).div_euclid(12);
    let month = (month - 1).rem_euclid(12) + 1;
    NaiveDate::from_ymd_opt((year + carry) as i32, month as u32, 1)
        .and_then(|date| date.and_hms_milli_opt(0, 0, 0, 0))
        .ok_or_else(|| QueryError::InvalidArgument(format!("{operator} failed to truncate date")))
}

fn start_of_week_date(date: NaiveDate, start_of_week: Weekday) -> NaiveDate {
    let weekday = date.weekday().num_days_from_sunday() as i64;
    let start = start_of_week.num_days_from_sunday() as i64;
    let delta = (7 + weekday - start) % 7;
    date - Duration::days(delta)
}

fn extract_date_parts<Tz: chrono::TimeZone>(date: DateTime<Tz>) -> DateParts {
    DateParts {
        year: date.year(),
        month: date.month(),
        day_of_month: date.day(),
        day_of_week: date.weekday().num_days_from_sunday() + 1,
        day_of_year: date.ordinal(),
        hour: date.hour(),
        iso_day_of_week: date.weekday().num_days_from_monday() + 1,
        iso_week: date.iso_week().week(),
        iso_week_year: date.iso_week().year(),
        millisecond: date.timestamp_subsec_millis(),
        minute: date.minute(),
        second: date.second(),
        week: date
            .naive_local()
            .format("%U")
            .to_string()
            .parse::<u32>()
            .unwrap_or(0),
    }
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

fn exact_positive_usize(value: &Bson) -> Option<usize> {
    let integer = match value {
        Bson::Int32(value) if *value > 0 => *value as i64,
        Bson::Int64(value) if *value > 0 => *value,
        Bson::Double(value) if value.is_finite() && value.fract() == 0.0 && *value > 0.0 => {
            *value as i64
        }
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            if !(parsed.is_finite() && parsed.fract() == 0.0 && parsed > 0.0) {
                return None;
            }
            parsed as i64
        }
        _ => return None,
    };
    usize::try_from(integer).ok()
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
