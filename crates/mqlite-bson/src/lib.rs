use std::cmp::Ordering;

use bson::{Bson, Document, oid::ObjectId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BsonToolsError {
    #[error("invalid dotted path `{0}`")]
    InvalidPath(String),
}

pub fn serialize_document(document: &Document) -> bson::ser::Result<Vec<u8>> {
    bson::to_vec(document)
}

pub fn deserialize_document(bytes: &[u8]) -> bson::de::Result<Document> {
    bson::from_slice(bytes)
}

pub fn lookup_path<'a>(document: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut current = document;
    let mut segments = path.split('.').peekable();

    while let Some(segment) = segments.next() {
        let value = current.get(segment)?;
        if segments.peek().is_none() {
            return Some(value);
        }

        match value {
            Bson::Document(child) => current = child,
            _ => return None,
        }
    }

    None
}

pub fn lookup_path_owned(document: &Document, path: &str) -> Option<Bson> {
    lookup_path(document, path).cloned()
}

pub fn set_path(document: &mut Document, path: &str, value: Bson) -> Result<(), BsonToolsError> {
    let segments = path_segments(path)?;
    set_segments(document, &segments, value);
    Ok(())
}

pub fn remove_path(document: &mut Document, path: &str) -> Result<(), BsonToolsError> {
    let segments = path_segments(path)?;
    remove_segments(document, &segments);
    Ok(())
}

pub fn ensure_object_id(document: &mut Document) -> Bson {
    if let Some(existing) = document.get("_id") {
        return existing.clone();
    }

    let generated = ObjectId::new();
    document.insert("_id", Bson::ObjectId(generated));
    Bson::ObjectId(generated)
}

pub fn compare_bson(left: &Bson, right: &Bson) -> Ordering {
    let left_rank = bson_rank(left);
    let right_rank = bson_rank(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }

    match (left, right) {
        (Bson::Null, Bson::Null) => Ordering::Equal,
        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),
        (Bson::String(a), Bson::String(b)) => a.cmp(b),
        (Bson::Int32(a), Bson::Int32(b)) => a.cmp(b),
        (Bson::Int64(a), Bson::Int64(b)) => a.cmp(b),
        (Bson::Double(a), Bson::Double(b)) => a.total_cmp(b),
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a.bytes().cmp(&b.bytes()),
        (Bson::DateTime(a), Bson::DateTime(b)) => a.cmp(b),
        (Bson::Timestamp(a), Bson::Timestamp(b)) => {
            (a.time, a.increment).cmp(&(b.time, b.increment))
        }
        (Bson::Array(a), Bson::Array(b)) => compare_slices(a, b),
        (Bson::Document(a), Bson::Document(b)) => compare_documents(a, b),
        _ if is_numeric(left) && is_numeric(right) => numeric_value(left)
            .unwrap_or_default()
            .total_cmp(&numeric_value(right).unwrap_or_default()),
        _ => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

pub fn compare_documents(left: &Document, right: &Document) -> Ordering {
    let left_items = left.iter().collect::<Vec<_>>();
    let right_items = right.iter().collect::<Vec<_>>();

    for ((left_key, left_value), (right_key, right_value)) in
        left_items.iter().zip(right_items.iter())
    {
        let key_cmp = left_key.cmp(right_key);
        if key_cmp != Ordering::Equal {
            return key_cmp;
        }

        let value_cmp = compare_bson(left_value, right_value);
        if value_cmp != Ordering::Equal {
            return value_cmp;
        }
    }

    left_items.len().cmp(&right_items.len())
}

fn compare_slices(left: &[Bson], right: &[Bson]) -> Ordering {
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let value_cmp = compare_bson(left_value, right_value);
        if value_cmp != Ordering::Equal {
            return value_cmp;
        }
    }

    left.len().cmp(&right.len())
}

fn bson_rank(value: &Bson) -> u8 {
    match value {
        Bson::MinKey => 0,
        Bson::Null => 1,
        Bson::Boolean(_) => 2,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => 3,
        Bson::String(_) => 4,
        Bson::Document(_) => 5,
        Bson::Array(_) => 6,
        Bson::Binary(_) => 7,
        Bson::ObjectId(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Timestamp(_) => 10,
        Bson::RegularExpression(_) => 11,
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => 12,
        Bson::Symbol(_) => 13,
        Bson::Decimal128(_) => 14,
        Bson::MaxKey => 15,
        _ => 16,
    }
}

fn is_numeric(value: &Bson) -> bool {
    matches!(value, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))
}

fn numeric_value(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(number) => Some(*number as f64),
        Bson::Int64(number) => Some(*number as f64),
        Bson::Double(number) => Some(*number),
        _ => None,
    }
}

fn path_segments(path: &str) -> Result<Vec<&str>, BsonToolsError> {
    let segments = path.split('.').collect::<Vec<_>>();
    if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
        return Err(BsonToolsError::InvalidPath(path.to_string()));
    }

    Ok(segments)
}

fn set_segments(current: &mut Document, segments: &[&str], value: Bson) {
    if let Some((first, rest)) = segments.split_first() {
        if rest.is_empty() {
            current.insert(*first, value);
            return;
        }

        let entry = current
            .entry((*first).to_string())
            .or_insert_with(|| Bson::Document(Document::new()));

        if !matches!(entry, Bson::Document(_)) {
            *entry = Bson::Document(Document::new());
        }

        if let Bson::Document(document) = entry {
            set_segments(document, rest, value);
        }
    }
}

fn remove_segments(current: &mut Document, segments: &[&str]) {
    if let Some((first, rest)) = segments.split_first() {
        if rest.is_empty() {
            current.remove(*first);
            return;
        }

        if let Some(Bson::Document(document)) = current.get_mut(*first) {
            remove_segments(document, rest);
        }
    }
}

#[cfg(test)]
mod tests {
    use bson::{Bson, Document, doc};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    use super::{
        compare_bson, deserialize_document, ensure_object_id, lookup_path, remove_path,
        serialize_document, set_path,
    };

    fn simple_bson_strategy() -> impl Strategy<Value = Bson> {
        prop_oneof![
            any::<i32>().prop_map(Bson::Int32),
            any::<i64>().prop_map(Bson::Int64),
            any::<bool>().prop_map(Bson::Boolean),
            ".*".prop_map(Bson::String),
        ]
    }

    fn simple_document_strategy() -> impl Strategy<Value = Document> {
        prop::collection::btree_map("[a-z]{1,8}", simple_bson_strategy(), 0..8).prop_map(
            |entries| {
                let mut document = Document::new();
                for (key, value) in entries {
                    document.insert(key, value);
                }
                document
            },
        )
    }

    proptest! {
        #[test]
        fn round_trips_documents(document in simple_document_strategy()) {
            let encoded = serialize_document(&document).expect("serialize");
            let decoded = deserialize_document(&encoded).expect("deserialize");
            prop_assert_eq!(decoded, document);
        }
    }

    #[test]
    fn supports_dotted_lookup_and_mutation() {
        let mut document = doc! {
            "profile": {
                "name": "Ada",
                "flags": { "admin": true }
            }
        };

        assert_eq!(
            lookup_path(&document, "profile.name"),
            Some(&Bson::String("Ada".to_string()))
        );

        set_path(&mut document, "profile.flags.beta", Bson::Boolean(false)).expect("set path");
        assert_eq!(
            lookup_path(&document, "profile.flags.beta"),
            Some(&Bson::Boolean(false))
        );

        remove_path(&mut document, "profile.flags.admin").expect("remove path");
        assert_eq!(lookup_path(&document, "profile.flags.admin"), None);
    }

    #[test]
    fn compares_numeric_types_by_value() {
        assert_eq!(
            compare_bson(&Bson::Int32(5), &Bson::Int64(5)),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_bson(&Bson::Double(4.0), &Bson::Int32(5)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_bson(
                &Bson::String("a".to_string()),
                &Bson::String("b".to_string())
            ),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn preserves_existing_object_id() {
        let id = bson::oid::ObjectId::new();
        let mut document = doc! { "_id": id };
        assert_eq!(ensure_object_id(&mut document), Bson::ObjectId(id));

        let mut scalar_id = doc! { "_id": 1, "name": "Ada" };
        assert_eq!(ensure_object_id(&mut scalar_id), Bson::Int32(1));

        let mut without_id = doc! { "name": "Grace" };
        let created = ensure_object_id(&mut without_id);
        assert_eq!(lookup_path(&without_id, "_id"), Some(&created));
    }
}
