use std::cmp::Ordering;

use anyhow::Result;
use bson::Bson;
use bson::Document;
const TAG_MIN_KEY: u8 = 0x01;
const TAG_NULL: u8 = 0x02;
const TAG_BOOL: u8 = 0x03;
const TAG_NUMBER: u8 = 0x04;
const TAG_STRING: u8 = 0x05;
const TAG_DOCUMENT: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_BINARY_DEBUG: u8 = 0x08;
const TAG_OBJECT_ID: u8 = 0x09;
const TAG_DATETIME: u8 = 0x0A;
const TAG_TIMESTAMP: u8 = 0x0B;
const TAG_REGEX_DEBUG: u8 = 0x0C;
const TAG_CODE_DEBUG: u8 = 0x0D;
const TAG_SYMBOL_DEBUG: u8 = 0x0E;
const TAG_DECIMAL_DEBUG: u8 = 0x0F;
const TAG_MAX_KEY: u8 = 0x10;
const TAG_FALLBACK_DEBUG: u8 = 0x11;

pub(crate) fn encode_index_key(key: &Document, key_pattern: &Document) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for (field, direction) in key_pattern {
        let mut field_bytes = encode_value_for_direction(key.get(field).unwrap_or(&Bson::Null))?;
        if key_direction(direction) < 0 {
            invert_bytes(&mut field_bytes);
        }
        bytes.extend_from_slice(&field_bytes);
    }
    Ok(bytes)
}

pub(crate) fn compare_encoded_index_keys(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

fn encode_value_for_direction(value: &Bson) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    encode_bson_sort_key(value, &mut bytes)?;
    Ok(bytes)
}

fn encode_bson_sort_key(value: &Bson, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Bson::MinKey => out.push(TAG_MIN_KEY),
        Bson::Null => out.push(TAG_NULL),
        Bson::Boolean(flag) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*flag));
        }
        Bson::Int32(number) => {
            out.push(TAG_NUMBER);
            out.extend_from_slice(&encode_f64_sortable(*number as f64));
        }
        Bson::Int64(number) => {
            out.push(TAG_NUMBER);
            out.extend_from_slice(&encode_f64_sortable(*number as f64));
        }
        Bson::Double(number) => {
            out.push(TAG_NUMBER);
            out.extend_from_slice(&encode_f64_sortable(*number));
        }
        Bson::String(text) => {
            out.push(TAG_STRING);
            encode_escaped_bytes(text.as_bytes(), out);
        }
        Bson::Document(document) => {
            out.push(TAG_DOCUMENT);
            for (field, nested) in document {
                encode_escaped_bytes(field.as_bytes(), out);
                encode_bson_sort_key(nested, out)?;
            }
            out.push(0);
        }
        Bson::Array(values) => {
            out.push(TAG_ARRAY);
            for nested in values {
                encode_bson_sort_key(nested, out)?;
            }
            out.push(0);
        }
        Bson::ObjectId(object_id) => {
            out.push(TAG_OBJECT_ID);
            out.extend_from_slice(&object_id.bytes());
        }
        Bson::DateTime(date_time) => {
            out.push(TAG_DATETIME);
            out.extend_from_slice(&encode_i64_sortable(date_time.timestamp_millis()));
        }
        Bson::Timestamp(timestamp) => {
            out.push(TAG_TIMESTAMP);
            out.extend_from_slice(&timestamp.time.to_be_bytes());
            out.extend_from_slice(&timestamp.increment.to_be_bytes());
        }
        Bson::MaxKey => out.push(TAG_MAX_KEY),
        Bson::Binary(binary) => {
            out.push(TAG_BINARY_DEBUG);
            encode_escaped_bytes(format!("{binary:?}").as_bytes(), out);
        }
        Bson::RegularExpression(regex) => {
            out.push(TAG_REGEX_DEBUG);
            encode_escaped_bytes(format!("{regex:?}").as_bytes(), out);
        }
        Bson::JavaScriptCode(code) => {
            out.push(TAG_CODE_DEBUG);
            encode_escaped_bytes(code.as_bytes(), out);
        }
        Bson::JavaScriptCodeWithScope(code) => {
            out.push(TAG_CODE_DEBUG);
            encode_escaped_bytes(format!("{code:?}").as_bytes(), out);
        }
        Bson::Symbol(symbol) => {
            out.push(TAG_SYMBOL_DEBUG);
            encode_escaped_bytes(symbol.as_bytes(), out);
        }
        Bson::Decimal128(decimal) => {
            out.push(TAG_DECIMAL_DEBUG);
            encode_escaped_bytes(format!("{decimal:?}").as_bytes(), out);
        }
        other => {
            out.push(TAG_FALLBACK_DEBUG);
            encode_escaped_bytes(format!("{other:?}").as_bytes(), out);
        }
    }
    Ok(())
}

fn encode_i64_sortable(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1_u64 << 63)).to_be_bytes()
}

fn encode_f64_sortable(value: f64) -> [u8; 8] {
    let bits = value.to_bits();
    let sortable = if bits >> 63 == 0 {
        bits ^ (1_u64 << 63)
    } else {
        !bits
    };
    sortable.to_be_bytes()
}

fn encode_escaped_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(*byte);
        }
    }
    out.push(0);
    out.push(0);
}

fn invert_bytes(bytes: &mut [u8]) {
    for byte in bytes {
        *byte = !*byte;
    }
}

fn key_direction(value: &Bson) -> i32 {
    match value {
        Bson::Int32(direction) if *direction < 0 => -1,
        Bson::Int64(direction) if *direction < 0 => -1,
        Bson::Double(direction) if *direction < 0.0 => -1,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use bson::{Bson, Document, doc};

    use super::{compare_encoded_index_keys, encode_index_key};
    use mqlite_bson::compare_bson;

    fn compare_index_keys(left: &Document, right: &Document, key_pattern: &Document) -> Ordering {
        for (field, direction) in key_pattern {
            let left_value = left.get(field).unwrap_or(&Bson::Null);
            let right_value = right.get(field).unwrap_or(&Bson::Null);
            let mut ordering = compare_bson(left_value, right_value);
            if matches!(direction, Bson::Int32(value) if *value < 0)
                || matches!(direction, Bson::Int64(value) if *value < 0)
                || matches!(direction, Bson::Double(value) if *value < 0.0)
            {
                ordering = ordering.reverse();
            }
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }

    #[test]
    fn encoded_keys_match_compound_index_ordering() {
        let key_pattern = doc! { "sku": 1, "qty": -1 };
        let samples = vec![
            doc! { "sku": "alpha", "qty": 9 },
            doc! { "sku": "alpha", "qty": 3 },
            doc! { "sku": "beta", "qty": 5 },
            doc! { "sku": "beta", "qty": Bson::Null },
        ];

        for left in &samples {
            for right in &samples {
                let expected = compare_index_keys(left, right, &key_pattern);
                let actual = compare_encoded_index_keys(
                    &encode_index_key(left, &key_pattern).expect("encode left"),
                    &encode_index_key(right, &key_pattern).expect("encode right"),
                );
                assert_eq!(actual, expected, "left={left:?} right={right:?}");
            }
        }
    }

    #[test]
    fn encoded_keys_handle_nested_documents_and_arrays() {
        let key_pattern = doc! { "meta": 1, "tags": 1 };
        let left = doc! {
            "meta": { "sku": "alpha", "qty": 2 },
            "tags": ["a", 1]
        };
        let right = doc! {
            "meta": { "sku": "alpha", "qty": 3 },
            "tags": ["a", 1]
        };

        assert_eq!(
            compare_encoded_index_keys(
                &encode_index_key(&left, &key_pattern).expect("encode left"),
                &encode_index_key(&right, &key_pattern).expect("encode right"),
            ),
            Ordering::Less
        );
    }

    #[test]
    fn encoded_keys_canonicalize_numeric_types() {
        let key_pattern = doc! { "qty": 1 };
        let int_key = doc! { "qty": 5_i32 };
        let long_key = doc! { "qty": 5_i64 };
        let double_key = doc! { "qty": 5.0_f64 };

        let encoded_int = encode_index_key(&int_key, &key_pattern).expect("encode int");
        let encoded_long = encode_index_key(&long_key, &key_pattern).expect("encode long");
        let encoded_double = encode_index_key(&double_key, &key_pattern).expect("encode double");

        assert_eq!(encoded_int, encoded_long);
        assert_eq!(encoded_long, encoded_double);
    }

    #[test]
    fn encoded_keys_support_descending_segments() {
        let key_pattern = doc! { "sku": -1 };
        let left = encode_index_key(&doc! { "sku": "alpha" }, &key_pattern).expect("encode left");
        let right = encode_index_key(&doc! { "sku": "beta" }, &key_pattern).expect("encode right");

        assert_eq!(compare_encoded_index_keys(&left, &right), Ordering::Greater);
    }
}
