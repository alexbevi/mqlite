use std::{
    collections::BTreeMap,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use bson::{Bson, Document};
use mqlite_bson::compare_bson;
use mqlite_debug::{Component, span};
use regex::{Regex as RustRegex, RegexBuilder};

use crate::{
    QueryError,
    expression::{
        array_length, coerce_to_i64, eval_expression_with_variables, expression_truthy,
        truncate_f64_to_i64, validate_expression,
    },
    types::{BitTestMode, MatchExpr, TypeSet},
};

pub fn parse_filter(document: &Document) -> Result<MatchExpr, QueryError> {
    let _span = span(Component::Query, "parse_filter");
    parse_filter_with_context(document, true)
}

fn parse_filter_with_context(
    document: &Document,
    allow_expr: bool,
) -> Result<MatchExpr, QueryError> {
    let mut expressions = Vec::new();

    for (key, value) in document {
        match key.as_str() {
            "$alwaysFalse" => {
                parse_always_boolean(value)?;
                expressions.push(MatchExpr::AlwaysFalse);
            }
            "$alwaysTrue" => {
                parse_always_boolean(value)?;
                expressions.push(MatchExpr::AlwaysTrue);
            }
            "$sampleRate" => expressions.push(parse_sample_rate(value)?),
            "$and" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| {
                        document
                            .and_then(|document| parse_filter_with_context(document, allow_expr))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::And(parsed));
            }
            "$or" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| {
                        document
                            .and_then(|document| parse_filter_with_context(document, allow_expr))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::Or(parsed));
            }
            "$nor" => {
                let items = value.as_array().ok_or(QueryError::InvalidStructure)?;
                let parsed = items
                    .iter()
                    .map(as_document)
                    .map(|document| {
                        document
                            .and_then(|document| parse_filter_with_context(document, allow_expr))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                expressions.push(MatchExpr::Nor(parsed));
            }
            "$expr" => {
                if !allow_expr {
                    return Err(QueryError::InvalidStructure);
                }
                validate_expression(value)?;
                expressions.push(MatchExpr::Expr(value.clone()));
            }
            "$comment" => continue,
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
    document_matches_with_variables(document, filter, &BTreeMap::new())
}

pub(crate) fn document_matches_with_variables(
    document: &Document,
    filter: &Document,
    variables: &BTreeMap<String, Bson>,
) -> Result<bool, QueryError> {
    let expression = parse_filter(filter)?;
    Ok(matches_expression(document, &expression, variables))
}

pub fn document_matches_expression(document: &Document, expression: &MatchExpr) -> bool {
    matches_expression(document, expression, &BTreeMap::new())
}

fn matches_expression(
    document: &Document,
    expression: &MatchExpr,
    variables: &BTreeMap<String, Bson>,
) -> bool {
    match expression {
        MatchExpr::AlwaysFalse => false,
        MatchExpr::AlwaysTrue => true,
        MatchExpr::SampleRate { rate, seed } => sample_rate_matches(document, *rate, *seed),
        MatchExpr::And(items) => items
            .iter()
            .all(|item| matches_expression(document, item, variables)),
        MatchExpr::Or(items) => items
            .iter()
            .any(|item| matches_expression(document, item, variables)),
        MatchExpr::Nor(items) => items
            .iter()
            .all(|item| !matches_expression(document, item, variables)),
        MatchExpr::Not(expression) => !matches_expression(document, expression, variables),
        MatchExpr::Expr(expression) => {
            eval_expression_with_variables(document, expression, variables)
                .map(|value| expression_truthy(&value))
                .unwrap_or(false)
        }
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
            .any(|value| matches_elem_match(value, spec, *value_case, variables)),
        MatchExpr::Regex {
            path,
            pattern,
            options,
        } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_regex(value, pattern, options)),
        MatchExpr::Size { path, size } => path_candidates(document, path)
            .into_iter()
            .any(|value| array_length(value).is_some_and(|length| length == *size)),
        MatchExpr::BitTest {
            path,
            mode,
            positions,
        } => path_candidates(document, path)
            .into_iter()
            .any(|value| matches_bit_test(value, *mode, positions)),
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
                    "$bitsAllSet" => MatchExpr::BitTest {
                        path: path.to_string(),
                        mode: BitTestMode::AllSet,
                        positions: parse_bit_positions(operator_value)?,
                    },
                    "$bitsAllClear" => MatchExpr::BitTest {
                        path: path.to_string(),
                        mode: BitTestMode::AllClear,
                        positions: parse_bit_positions(operator_value)?,
                    },
                    "$bitsAnySet" => MatchExpr::BitTest {
                        path: path.to_string(),
                        mode: BitTestMode::AnySet,
                        positions: parse_bit_positions(operator_value)?,
                    },
                    "$bitsAnyClear" => MatchExpr::BitTest {
                        path: path.to_string(),
                        mode: BitTestMode::AnyClear,
                        positions: parse_bit_positions(operator_value)?,
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

fn parse_always_boolean(value: &Bson) -> Result<(), QueryError> {
    match integer_value(value) {
        Some(1) => Ok(()),
        _ => Err(QueryError::InvalidStructure),
    }
}

fn parse_sample_rate(value: &Bson) -> Result<MatchExpr, QueryError> {
    let rate = numeric_probability(value).ok_or(QueryError::InvalidStructure)?;
    if !(0.0..=1.0).contains(&rate) {
        return Err(QueryError::InvalidStructure);
    }
    if rate == 0.0 {
        return Ok(MatchExpr::AlwaysFalse);
    }
    if rate == 1.0 {
        return Ok(MatchExpr::AlwaysTrue);
    }
    Ok(MatchExpr::SampleRate {
        rate,
        seed: next_sample_seed(),
    })
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
        parse_filter_with_context(&filter, false).map(|_| ())
    } else {
        parse_filter_with_context(spec, false).map(|_| ())
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

fn integer_value(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(value) => Some(*value as i64),
        Bson::Int64(value) => Some(*value),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i64),
        _ => None,
    }
}

fn matches_mod(value: &Bson, divisor: i64, remainder: i64) -> bool {
    match value {
        Bson::Array(items) => items
            .iter()
            .any(|item| matches_mod(item, divisor, remainder)),
        _ => coerce_to_i64(value).is_some_and(|coerced| coerced % divisor == remainder),
    }
}

fn parse_bit_positions(value: &Bson) -> Result<Vec<u32>, QueryError> {
    let mut positions = match value {
        Bson::Array(values) => values
            .iter()
            .map(bit_position_from_bson)
            .collect::<Result<Vec<_>, _>>()?,
        Bson::Binary(binary) => bit_positions_from_bytes(&binary.bytes),
        _ => bit_positions_from_numeric_value(value)
            .map(bit_positions_from_mask)
            .ok_or(QueryError::InvalidStructure)?,
    };
    positions.sort_unstable();
    positions.dedup();
    Ok(positions)
}

fn bit_position_from_bson(value: &Bson) -> Result<u32, QueryError> {
    let Some(position) = integer_value(value) else {
        return Err(QueryError::InvalidStructure);
    };
    if position < 0 {
        return Err(QueryError::InvalidStructure);
    }
    u32::try_from(position).map_err(|_| QueryError::InvalidStructure)
}

fn bit_positions_from_numeric_value(value: &Bson) -> Option<u64> {
    match value {
        Bson::Int32(value) if *value >= 0 => Some(*value as u64),
        Bson::Int64(value) if *value >= 0 => Some(*value as u64),
        Bson::Double(value) if value.is_finite() && *value >= 0.0 && value.fract() == 0.0 => {
            (*value <= u64::MAX as f64).then_some(*value as u64)
        }
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            (parsed.is_finite()
                && parsed >= 0.0
                && parsed.fract() == 0.0
                && parsed <= u64::MAX as f64)
                .then_some(parsed as u64)
        }
        _ => None,
    }
}

fn numeric_probability(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(value) => Some(*value as f64),
        Bson::Int64(value) => Some(*value as f64),
        Bson::Double(value) if value.is_finite() => Some(*value),
        Bson::Decimal128(value) => {
            let parsed = value.to_string().parse::<f64>().ok()?;
            parsed.is_finite().then_some(parsed)
        }
        _ => None,
    }
}

fn bit_positions_from_mask(mask: u64) -> Vec<u32> {
    (0_u32..64)
        .filter(|position| ((mask >> position) & 1) == 1)
        .collect()
}

fn bit_positions_from_bytes(bytes: &[u8]) -> Vec<u32> {
    let mut positions = Vec::new();
    for (byte_index, byte) in bytes.iter().enumerate() {
        for bit_index in 0..8_u32 {
            if (byte & (1_u8 << bit_index)) != 0 {
                positions.push((byte_index as u32 * 8) + bit_index);
            }
        }
    }
    positions
}

fn matches_bit_test(value: &Bson, mode: BitTestMode, positions: &[u32]) -> bool {
    match value {
        Bson::Array(items) => items
            .iter()
            .any(|item| matches_bit_test(item, mode, positions)),
        Bson::Binary(binary) => evaluate_bit_test(mode, positions, |position| {
            let byte_index = (position / 8) as usize;
            let bit_index = position % 8;
            binary
                .bytes
                .get(byte_index)
                .is_some_and(|byte| (byte & (1_u8 << bit_index)) != 0)
        }),
        _ => numeric_bits(value).is_some_and(|bits| {
            evaluate_bit_test(mode, positions, |position| {
                position < 64 && ((bits >> position) & 1) == 1
            })
        }),
    }
}

fn evaluate_bit_test(
    mode: BitTestMode,
    positions: &[u32],
    bit_is_set: impl Fn(u32) -> bool,
) -> bool {
    match mode {
        BitTestMode::AllSet => positions.iter().copied().all(bit_is_set),
        BitTestMode::AllClear => positions
            .iter()
            .copied()
            .all(|position| !bit_is_set(position)),
        BitTestMode::AnySet => positions.iter().copied().any(bit_is_set),
        BitTestMode::AnyClear => positions
            .iter()
            .copied()
            .any(|position| !bit_is_set(position)),
    }
}

fn numeric_bits(value: &Bson) -> Option<u64> {
    match value {
        Bson::Int32(value) => Some(*value as i64 as u64),
        Bson::Int64(value) => Some(*value as u64),
        Bson::Double(value) if value.is_finite() && value.fract() == 0.0 => {
            truncate_f64_to_i64(*value).map(|value| value as u64)
        }
        Bson::Decimal128(value) => {
            truncate_f64_to_i64(value.to_string().parse::<f64>().ok()?).map(|value| value as u64)
        }
        _ => None,
    }
}

fn next_sample_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn sample_rate_matches(document: &Document, rate: f64, seed: u64) -> bool {
    let bytes = bson::to_vec(document).unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    bytes.hash(&mut hasher);
    let bucket = hasher.finish() as f64 / u64::MAX as f64;
    bucket < rate
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

fn matches_elem_match(
    value: &Bson,
    spec: &Document,
    value_case: bool,
    variables: &BTreeMap<String, Bson>,
) -> bool {
    let Some(items) = value.as_array() else {
        return false;
    };

    items.iter().any(|item| {
        if value_case {
            let mut document = Document::new();
            document.insert("_elem", item.clone());
            let mut filter = Document::new();
            filter.insert("_elem", Bson::Document(spec.clone()));
            return document_matches_with_variables(&document, &filter, variables).unwrap_or(false);
        }

        match item {
            Bson::Document(document) => {
                document_matches_with_variables(document, spec, variables).unwrap_or(false)
            }
            Bson::Array(items) => {
                let document = array_as_document(items);
                document_matches_with_variables(&document, spec, variables).unwrap_or(false)
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

pub(crate) fn compile_regex(pattern: &str, options: &str) -> Result<RustRegex, QueryError> {
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

pub(crate) fn type_alias_code(alias: &str) -> Option<i32> {
    match alias {
        "double" => Some(1),
        "string" => Some(2),
        "object" => Some(3),
        "array" => Some(4),
        "binData" => Some(5),
        "undefined" => Some(6),
        "objectId" => Some(7),
        "bool" => Some(8),
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
    match value {
        Bson::Int32(value) => Some(*value),
        Bson::Int64(value) => i32::try_from(*value).ok(),
        Bson::Double(value) if value.fract() == 0.0 => Some(*value as i32),
        _ => None,
    }
}

pub(crate) fn bson_type_code(value: &Bson) -> i32 {
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

pub(crate) fn is_numeric_bson(value: &Bson) -> bool {
    matches!(
        value,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_)
    )
}

pub(crate) fn bson_type_alias(value: &Bson) -> &'static str {
    match bson_type_code(value) {
        1 => "double",
        2 => "string",
        3 => "object",
        4 => "array",
        5 => "binData",
        6 => "undefined",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        10 => "null",
        11 => "regex",
        12 => "dbPointer",
        13 => "javascript",
        14 => "symbol",
        15 => "javascriptWithScope",
        16 => "int",
        17 => "timestamp",
        18 => "long",
        19 => "decimal",
        -1 => "minKey",
        127 => "maxKey",
        _ => "unknown",
    }
}

fn as_document(value: &Bson) -> Result<&Document, QueryError> {
    value.as_document().ok_or(QueryError::InvalidStructure)
}
