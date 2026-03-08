use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use bson::{
    Binary, Bson, DateTime, Decimal128, Document, Timestamp, doc, oid::ObjectId,
    spec::BinarySubtype,
};
use pretty_assertions::assert_eq;

use crate::{
    CollectionResolver, QueryError, apply_projection, apply_update, document_matches,
    document_matches_expression, parse_filter, parse_update, parse_update_value, run_pipeline,
    run_pipeline_with_resolver,
};

// These cases are grounded in MongoDB matcher and pipeline tests such as
// expression_leaf_test.cpp, expression_tree_test.cpp, document_source_project_test.cpp,
// document_source_add_fields_test.cpp, document_source_unwind_test.cpp,
// document_source_group_test.cpp, document_source_replace_root_test.cpp, and
// document_source_sort_test.cpp.

fn run_pipeline_ok(documents: Vec<Document>, pipeline: &[Document]) -> Vec<Document> {
    run_pipeline(documents, pipeline).expect("pipeline")
}

#[derive(Default)]
struct StaticResolver {
    collections: BTreeMap<(String, String), Vec<Document>>,
    change_events: Vec<Document>,
}

impl StaticResolver {
    fn with_collection(
        mut self,
        database: &str,
        collection: &str,
        documents: Vec<Document>,
    ) -> Self {
        self.collections
            .insert((database.to_string(), collection.to_string()), documents);
        self
    }

    fn with_change_events(mut self, change_events: Vec<Document>) -> Self {
        self.change_events = change_events;
        self
    }
}

impl CollectionResolver for StaticResolver {
    fn resolve_collection(&self, database: &str, collection: &str) -> Vec<Document> {
        self.collections
            .get(&(database.to_string(), collection.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn resolve_change_events(&self) -> Vec<Document> {
        self.change_events.clone()
    }
}

fn run_pipeline_with_static_resolver(
    documents: Vec<Document>,
    pipeline: &[Document],
    resolver: &StaticResolver,
) -> Vec<Document> {
    run_pipeline_with_resolver(documents, pipeline, "app", None, resolver).expect("pipeline")
}

#[allow(clippy::too_many_arguments)]
fn change_event(
    sequence: i64,
    database: &str,
    collection: Option<&str>,
    operation_type: &str,
    document_key: Option<Document>,
    full_document: Option<Document>,
    full_document_before_change: Option<Document>,
    update_description: Option<Document>,
    expanded: bool,
    extra_fields: Document,
) -> Document {
    let mut document = doc! {
        "token": { "sequence": sequence },
        "clusterTime": Timestamp { time: sequence as u32, increment: 0 },
        "wallTime": DateTime::from_millis(sequence),
        "database": database,
        "operationType": operation_type,
        "expanded": expanded,
        "extraFields": extra_fields,
    };
    if let Some(collection) = collection {
        document.insert("collection", collection);
    }
    if let Some(document_key) = document_key {
        document.insert("documentKey", Bson::Document(document_key));
    }
    if let Some(full_document) = full_document {
        document.insert("fullDocument", Bson::Document(full_document));
    }
    if let Some(full_document_before_change) = full_document_before_change {
        document.insert(
            "fullDocumentBeforeChange",
            Bson::Document(full_document_before_change),
        );
    }
    if let Some(update_description) = update_description {
        document.insert("updateDescription", Bson::Document(update_description));
    }
    document
}

fn large_update_change_event(sequence: i64, payload: &str) -> Document {
    change_event(
        sequence,
        "app",
        Some("widgets"),
        "update",
        Some(doc! { "_id": sequence }),
        Some(doc! { "_id": sequence, "payload": payload }),
        Some(doc! { "_id": sequence, "payload": payload }),
        Some(doc! { "updatedFields": { "payload": payload }, "removedFields": [] }),
        false,
        Document::new(),
    )
}

fn merge_split_fragments(fragments: &[Document]) -> Document {
    let mut merged = Document::new();
    for fragment in fragments {
        for (field, value) in fragment {
            if field != "_id" && field != "splitEvent" {
                merged.insert(field.clone(), value.clone());
            }
        }
    }
    merged
}

fn assert_filter(document: &Document, filter: Document, expected: bool) {
    assert_eq!(
        document_matches(document, &filter).expect("match"),
        expected,
        "filter: {:?}",
        filter
    );
}

#[test]
fn matches_basic_filters() {
    let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };
    assert!(document_matches(&document, &doc! { "sku": "abc" }).expect("match"));
    assert!(document_matches(&document, &doc! { "qty": { "$gte": 3 } }).expect("match"));
    assert!(
        document_matches(
            &document,
            &doc! { "$or": [ { "qty": { "$gt": 9 } }, { "meta.enabled": true } ] }
        )
        .expect("match")
    );
}

#[test]
fn supports_always_true_and_always_false_filters() {
    let document = doc! { "a": false, "b": 1 };

    assert_filter(&document, doc! { "$alwaysTrue": 1 }, true);
    assert_filter(&document, doc! { "$alwaysFalse": 1 }, false);
    assert_filter(
        &document,
        doc! { "$and": [{ "a": false }, { "$alwaysTrue": 1 }, { "$alwaysTrue": 1 }] },
        true,
    );
    assert_filter(
        &document,
        doc! { "$and": [{ "a": false }, { "$alwaysTrue": 1 }, { "$alwaysFalse": 1 }] },
        false,
    );
    assert_filter(
        &document,
        doc! { "$or": [{ "b": 1 }, { "$alwaysFalse": 1 }] },
        true,
    );
    assert_filter(&document, doc! { "$nor": [{ "$alwaysFalse": 1 }] }, true);
}

#[test]
fn supports_all_comparison_query_operators() {
    let document = doc! { "sku": "abc", "qty": 5, "meta": { "score": 9 } };

    for (filter, expected) in [
        (doc! { "sku": "abc" }, true),
        (doc! { "qty": { "$eq": 5 } }, true),
        (doc! { "qty": { "$eq": 4 } }, false),
        (doc! { "qty": { "$ne": 4 } }, true),
        (doc! { "qty": { "$ne": 5 } }, false),
        (doc! { "qty": { "$gt": 4 } }, true),
        (doc! { "qty": { "$gt": 5 } }, false),
        (doc! { "qty": { "$gte": 5 } }, true),
        (doc! { "qty": { "$gte": 6 } }, false),
        (doc! { "qty": { "$lt": 6 } }, true),
        (doc! { "qty": { "$lt": 5 } }, false),
        (doc! { "meta.score": { "$lte": 9 } }, true),
        (doc! { "meta.score": { "$lte": 8 } }, false),
    ] {
        assert_filter(&document, filter, expected);
    }
}

#[test]
fn supports_in_exists_and_implicit_and_filters() {
    let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

    assert_filter(
        &document,
        doc! { "sku": { "$in": ["def", "abc"] }, "qty": { "$gte": 5, "$lte": 5 } },
        true,
    );
    assert_filter(&document, doc! { "sku": { "$in": ["def"] } }, false);
    assert_filter(
        &document,
        doc! { "meta.enabled": { "$exists": true } },
        true,
    );
    assert_filter(
        &document,
        doc! { "meta.enabled": { "$exists": false } },
        false,
    );
    assert_filter(
        &document,
        doc! { "meta.missing": { "$exists": false } },
        true,
    );
    assert_filter(
        &document,
        doc! { "meta.missing": { "$exists": true } },
        false,
    );
}

#[test]
fn supports_nin_filters() {
    let document = doc! { "sku": "abc", "qty": 5 };

    assert_filter(&document, doc! { "sku": { "$nin": ["def", "ghi"] } }, true);
    assert_filter(&document, doc! { "sku": { "$nin": ["abc", "ghi"] } }, false);
    assert_filter(&document, doc! { "missing": { "$nin": ["abc"] } }, true);
}

#[test]
fn supports_size_filters() {
    let document = doc! { "tags": ["red", "blue"], "meta": { "values": [1] } };

    assert_filter(&document, doc! { "tags": { "$size": 2 } }, true);
    assert_filter(&document, doc! { "tags": { "$size": 1 } }, false);
    assert_filter(&document, doc! { "meta.values": { "$size": 1 } }, true);
    assert_filter(&document, doc! { "missing": { "$size": 0 } }, false);
}

#[test]
fn supports_bit_test_filters() {
    let numeric = doc! { "qty": 54, "values": [1.1, 54], "negative": -1_i32 };
    let binary = doc! {
        "payload": Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![0b0000_0011],
        }),
    };

    assert_filter(&numeric, doc! { "qty": { "$bitsAllSet": 54 } }, true);
    assert_filter(&numeric, doc! { "qty": { "$bitsAllSet": 55 } }, false);
    assert_filter(&numeric, doc! { "qty": { "$bitsAllClear": 129 } }, true);
    assert_filter(&numeric, doc! { "qty": { "$bitsAnySet": [1, 4] } }, true);
    assert_filter(&numeric, doc! { "qty": { "$bitsAnyClear": [0, 3] } }, true);
    assert_filter(&numeric, doc! { "values": { "$bitsAllSet": 54 } }, true);
    assert_filter(
        &numeric,
        doc! { "negative": { "$bitsAllSet": [0, 63] } },
        true,
    );
    assert_filter(
        &binary,
        doc! {
            "payload": {
                "$bitsAllSet": Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: vec![0b0000_0001],
                })
            }
        },
        true,
    );
    assert_filter(
        &binary,
        doc! {
            "payload": {
                "$bitsAnyClear": Bson::Binary(Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: vec![0b0000_0110],
                })
            }
        },
        true,
    );
}

#[test]
fn supports_mod_filters() {
    let document = doc! { "qty": 12, "score": 4.7, "values": [1, 8] };

    assert_filter(&document, doc! { "qty": { "$mod": [5, 2] } }, true);
    assert_filter(&document, doc! { "qty": { "$mod": [5, 1] } }, false);
    assert_filter(&document, doc! { "score": { "$mod": [4, 0] } }, true);
    assert_filter(&document, doc! { "values": { "$mod": [4, 0] } }, true);
    assert_filter(&document, doc! { "missing": { "$mod": [4, 0] } }, false);
}

#[test]
fn rejects_malformed_mod_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": [5] } }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": [0, 0] } }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$mod": ["x", 0] } }),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn rejects_malformed_bit_test_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$bitsAllSet": -1 } }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(
            &doc! { "qty": 12 },
            &doc! { "qty": { "$bitsAnySet": ["x"] } }
        ),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(
            &doc! { "qty": 12 },
            &doc! { "qty": { "$bitsAnyClear": "x" } }
        ),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn supports_all_filters() {
    let document = doc! { "tags": ["red", "blue", "green"], "status": "red" };

    assert_filter(
        &document,
        doc! { "tags": { "$all": ["red", "blue"] } },
        true,
    );
    assert_filter(
        &document,
        doc! { "tags": { "$all": ["red", "missing"] } },
        false,
    );
    assert_filter(
        &document,
        doc! { "tags": { "$all": ["blue", "blue"] } },
        true,
    );
    assert_filter(&document, doc! { "tags": { "$all": [] } }, false);
    assert_filter(&document, doc! { "status": { "$all": ["red"] } }, true);
}

#[test]
fn supports_top_level_comment_filters() {
    let document = doc! { "sku": "abc", "qty": 5 };

    assert_filter(
        &document,
        doc! { "sku": "abc", "$comment": "keep this for profiler parity" },
        true,
    );
    assert_filter(&document, doc! { "$comment": { "trace": 1 } }, true);
    assert_filter(
        &document,
        doc! { "qty": { "$gt": 7 }, "$comment": "ignored metadata" },
        false,
    );
}

#[test]
fn supports_sample_rate_filters() {
    let filter = doc! { "$sampleRate": 0.5 };
    let expression = parse_filter(&filter).expect("sample rate filter");
    let documents = (0..20)
        .map(|value| doc! { "_id": value, "qty": value })
        .collect::<Vec<_>>();
    let matches = documents
        .iter()
        .filter(|document| document_matches_expression(document, &expression))
        .count();

    assert_filter(&doc! { "qty": 5 }, doc! { "$sampleRate": 1.0 }, true);
    assert_filter(&doc! { "qty": 5 }, doc! { "$sampleRate": 0.0 }, false);
    assert!((1..documents.len()).contains(&matches));
}

#[test]
fn rejects_expression_values_inside_all_filters() {
    assert!(matches!(
        document_matches(
            &doc! { "tags": ["red"] },
            &doc! { "tags": { "$all": [{ "$elemMatch": { "$eq": "red" } }] } }
        ),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn rejects_malformed_sample_rate_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "$sampleRate": -1 }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "$sampleRate": 2.0 }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "$sampleRate": "x" }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$sampleRate": 0.5 } }),
        Err(QueryError::UnsupportedOperator(operator)) if operator == "$sampleRate"
    ));
}

#[test]
fn rejects_invalid_always_boolean_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "$alwaysTrue": 0 }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "$alwaysFalse": 0 }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$alwaysFalse": 1 } }),
        Err(QueryError::UnsupportedOperator(operator)) if operator == "$alwaysFalse"
    ));
}

#[test]
fn supports_not_filters() {
    let document = doc! { "qty": 12, "tags": ["red", "blue"] };

    assert_filter(
        &document,
        doc! { "qty": { "$not": { "$mod": [5, 1] } } },
        true,
    );
    assert_filter(&document, doc! { "qty": { "$not": { "$gt": 5 } } }, false);
    assert_filter(
        &document,
        doc! { "tags": { "$not": { "$all": ["red", "green"] } } },
        true,
    );
}

#[test]
fn rejects_malformed_not_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$not": 5 } }),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn rejects_comment_as_field_operator() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$comment": "invalid" } }),
        Err(QueryError::UnsupportedOperator(operator)) if operator == "$comment"
    ));
}

#[test]
fn supports_type_filters() {
    let document = doc! {
        "name": "Ada",
        "count": 5_i32,
        "big": Bson::Int64(7),
        "price": 4.5,
        "meta": { "enabled": true },
        "tags": ["red", "blue"],
        "when": DateTime::now(),
        "id": ObjectId::new(),
        "stamp": Timestamp { time: 1, increment: 2 },
        "dec": Bson::Decimal128(Decimal128::from_str("1.5").expect("decimal")),
    };

    assert_filter(&document, doc! { "name": { "$type": "string" } }, true);
    assert_filter(&document, doc! { "count": { "$type": "int" } }, true);
    assert_filter(&document, doc! { "big": { "$type": "long" } }, true);
    assert_filter(&document, doc! { "price": { "$type": "double" } }, true);
    assert_filter(&document, doc! { "price": { "$type": "number" } }, true);
    assert_filter(&document, doc! { "meta": { "$type": "object" } }, true);
    assert_filter(&document, doc! { "tags": { "$type": "array" } }, true);
    assert_filter(&document, doc! { "when": { "$type": "date" } }, true);
    assert_filter(&document, doc! { "id": { "$type": "objectId" } }, true);
    assert_filter(&document, doc! { "stamp": { "$type": "timestamp" } }, true);
    assert_filter(&document, doc! { "dec": { "$type": "decimal" } }, true);
    assert_filter(
        &document,
        doc! { "name": { "$type": ["int", "string"] } },
        true,
    );
    assert_filter(&document, doc! { "missing": { "$type": "string" } }, false);
}

#[test]
fn rejects_malformed_type_filters() {
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$type": "missing" } }),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "qty": 12 }, &doc! { "qty": { "$type": 0 } }),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn supports_regex_filters() {
    let document = doc! { "name": "Ada", "tags": ["beta", "Gamma"] };

    assert_filter(&document, doc! { "name": { "$regex": "^A" } }, true);
    assert_filter(
        &document,
        doc! { "name": { "$regex": "^a", "$options": "i" } },
        true,
    );
    assert_filter(
        &document,
        doc! { "tags": { "$regex": "^g", "$options": "i" } },
        true,
    );
    assert!(
        document_matches(
            &document,
            &doc! {
                "name": Bson::RegularExpression(bson::Regex {
                    pattern: "^A".to_string(),
                    options: "".to_string(),
                })
            }
        )
        .expect("match")
    );
}

#[test]
fn rejects_malformed_regex_filters() {
    assert!(matches!(
        document_matches(
            &doc! { "name": "Ada" },
            &doc! { "name": { "$options": "i" } }
        ),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(
            &doc! { "name": "Ada" },
            &doc! { "name": { "$regex": "^A", "$options": 1 } }
        ),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(
            &doc! { "name": "Ada" },
            &doc! { "name": { "$regex": "[", "$options": "i" } }
        ),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(
            &doc! { "name": "Ada" },
            &doc! { "name": { "$regex": "^A", "$options": "q" } }
        ),
        Err(QueryError::InvalidStructure)
    ));
    assert!(matches!(
        document_matches(&doc! { "name": "Ada" }, &doc! { "name": { "$not": {} } }),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn supports_elem_match_filters() {
    assert_filter(
        &doc! { "a": [3, 5, 7] },
        doc! { "a": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
        true,
    );
    assert_filter(
        &doc! { "a": [3, 7] },
        doc! { "a": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
        false,
    );
    assert_filter(
        &doc! { "a": [[5]] },
        doc! { "a": { "$elemMatch": { "$elemMatch": { "$lt": 6, "$gt": 4 } } } },
        true,
    );
    assert_filter(
        &doc! { "a": [{ "b": 2, "c": 3 }, { "b": 1, "c": 4 }] },
        doc! { "a": { "$elemMatch": { "b": 1, "c": 4 } } },
        true,
    );
    assert_filter(
        &doc! { "a": [{ "b": [12, 2], "c": [13, 3] }] },
        doc! { "a": { "$elemMatch": { "b": 2 } } },
        true,
    );
    assert_filter(
        &doc! { "a": [{ "b": [5] }] },
        doc! { "a.b": { "$elemMatch": { "$lt": 6, "$gt": 4 } } },
        true,
    );
}

#[test]
fn rejects_malformed_elem_match_filters() {
    assert!(matches!(
        document_matches(&doc! { "a": [1, 2] }, &doc! { "a": { "$elemMatch": 1 } }),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn supports_expr_filters() {
    let document = doc! { "qty": 5, "limit": 4, "sku": "abc", "tags": ["red", "blue"] };

    assert_filter(
        &document,
        doc! { "$expr": { "$gt": ["$qty", "$limit"] } },
        true,
    );
    assert_filter(
        &document,
        doc! {
            "$or": [
                { "$expr": { "$eq": ["$sku", "missing"] } },
                { "$expr": { "$in": ["$sku", ["abc", "def"]] } }
            ]
        },
        true,
    );
    assert_filter(
        &document,
        doc! { "$expr": { "$and": [{ "$eq": ["$qty", 5] }, { "$not": [{ "$lt": ["$limit", 4] }] }] } },
        true,
    );
    assert_filter(
        &document,
        doc! { "$expr": { "$lte": ["$qty", "$limit"] } },
        false,
    );
}

#[test]
fn rejects_expr_in_subdocuments() {
    assert!(matches!(
        document_matches(
            &doc! { "a": [{ "qty": 1 }] },
            &doc! { "a": { "$elemMatch": { "$expr": { "$eq": ["$qty", 1] } } } }
        ),
        Err(QueryError::InvalidStructure)
    ));
}

#[test]
fn supports_logical_and_or_query_filters() {
    let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

    assert_filter(
        &document,
        doc! {
            "$and": [
                { "qty": { "$gte": 5 } },
                { "$or": [ { "sku": "missing" }, { "meta.enabled": true } ] }
            ]
        },
        true,
    );
    assert_filter(
        &document,
        doc! {
            "$and": [
                { "qty": { "$gt": 5 } },
                { "$or": [ { "sku": "abc" }, { "meta.enabled": true } ] }
            ]
        },
        false,
    );
}

#[test]
fn supports_nor_query_filters() {
    let document = doc! { "sku": "abc", "qty": 5, "meta": { "enabled": true } };

    assert_filter(
        &document,
        doc! { "$nor": [{ "qty": { "$lt": 0 } }, { "sku": "missing" }] },
        true,
    );
    assert_filter(
        &document,
        doc! { "$nor": [{ "qty": { "$gte": 5 } }, { "sku": "missing" }] },
        false,
    );
}

#[test]
fn applies_modifier_updates() {
    let mut document = doc! { "_id": 1, "qty": 2, "meta": { "enabled": true } };
    let update = parse_update(&doc! {
        "$set": { "meta.flag": "beta" },
        "$inc": { "qty": 3 },
        "$unset": { "meta.enabled": "" }
    })
    .expect("parse update");

    apply_update(&mut document, &update).expect("apply");
    assert_eq!(document.get_i64("qty").expect("qty"), 5);
    assert_eq!(
        document
            .get_document("meta")
            .expect("meta")
            .get_str("flag")
            .expect("flag"),
        "beta"
    );
    assert!(
        document
            .get_document("meta")
            .expect("meta")
            .get("enabled")
            .is_none()
    );
}

#[test]
fn applies_pipeline_updates() {
    let mut document = doc! { "_id": 1, "qty": 2 };
    let update = parse_update_value(&Bson::Array(vec![
        Bson::Document(doc! { "$set": { "qty": 5 } }),
        Bson::Document(doc! { "$set": { "flag": "beta" } }),
    ]))
    .expect("parse update");

    apply_update(&mut document, &update).expect("apply");
    assert_eq!(document, doc! { "_id": 1, "qty": 5, "flag": "beta" });
}

#[test]
fn projection_only_excludes_id() {
    let document = doc! { "_id": 1, "sku": "abc", "qty": 5 };
    let projected =
        apply_projection(&document, Some(&doc! { "_id": 0 })).expect("apply projection");

    assert_eq!(projected, doc! { "sku": "abc", "qty": 5 });
}

#[test]
fn projection_supports_nested_inclusion_and_computed_fields() {
    let document = doc! { "_id": 1, "sku": "abc", "meta": { "enabled": true, "flag": "beta" } };
    let projected = apply_projection(
        &document,
        Some(&doc! {
            "sku": 1,
            "meta.enabled": 1,
            "copiedSku": "$sku",
            "answer": { "$literal": 42 }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "sku": "abc",
            "meta": { "enabled": true },
            "copiedSku": "abc",
            "answer": 42
        }
    );
}

#[test]
fn projection_supports_expression_operators() {
    let document = doc! {
        "_id": 1,
        "left": 5,
        "right": 3,
        "sku": "abc",
        "array": [1, 2, 3],
        "empty": [],
        "object": { "a": 1, "b": 2 },
        "pairs": [["price", 24], ["item", "apple"]]
    };
    let mut projection = Document::new();
    projection.insert("abs", doc! { "$abs": -5 });
    projection.insert("add", doc! { "$add": ["$left", "$right", 2] });
    projection.insert(
        "allElementsTrue",
        doc! { "$allElementsTrue": [true, 1, "ok"] },
    );
    projection.insert("eq", doc! { "$eq": ["$left", 5] });
    projection.insert("ne", doc! { "$ne": ["$left", "$right"] });
    projection.insert("gt", doc! { "$gt": ["$left", "$right"] });
    projection.insert("gte", doc! { "$gte": ["$left", 5] });
    projection.insert("lt", doc! { "$lt": ["$right", "$left"] });
    projection.insert("lte", doc! { "$lte": ["$right", 3] });
    projection.insert(
        "anyElementTrue",
        doc! { "$anyElementTrue": [0, false, "ok"] },
    );
    projection.insert("arrayElemAt", doc! { "$arrayElemAt": ["$array", -1] });
    projection.insert("arrayToObject", doc! { "$arrayToObject": "$pairs" });
    projection.insert("cmp", doc! { "$cmp": ["$left", "$right"] });
    projection.insert("concat", doc! { "$concat": ["prefix-", "$sku"] });
    projection.insert("concatArrays", doc! { "$concatArrays": ["$array", [4, 5]] });
    projection.insert("and", doc! { "$and": [true, { "$eq": ["$left", 5] }] });
    projection.insert("or", doc! { "$or": [false, { "$eq": ["$sku", "abc"] }] });
    projection.insert("not", doc! { "$not": [{ "$eq": ["$right", 5] }] });
    projection.insert("in", doc! { "$in": ["$sku", ["def", "abc"]] });
    projection.insert("const", doc! { "$const": "fixed" });
    projection.insert("divide", doc! { "$divide": [7, 2] });
    projection.insert("expr", doc! { "$expr": { "$eq": ["$left", 5] } });
    projection.insert("first", doc! { "$first": "$array" });
    projection.insert("floor", doc! { "$floor": 2.8 });
    projection.insert("ceil", doc! { "$ceil": 2.2 });
    projection.insert("ifNull", doc! { "$ifNull": [null, "$left"] });
    projection.insert("isArray", doc! { "$isArray": "$array" });
    projection.insert("isNumber", doc! { "$isNumber": "$left" });
    projection.insert("last", doc! { "$last": "$array" });
    projection.insert("mod", doc! { "$mod": [17, 5] });
    projection.insert(
        "mergeObjects",
        doc! { "$mergeObjects": ["$object", { "b": 9, "c": 3 }] },
    );
    projection.insert("multiply", doc! { "$multiply": ["$left", 2] });
    projection.insert("objectToArray", doc! { "$objectToArray": "$object" });
    projection.insert("round", doc! { "$round": [2.65, 1] });
    projection.insert("size", doc! { "$size": "$array" });
    projection.insert("subtract", doc! { "$subtract": ["$left", "$right"] });
    projection.insert("type", doc! { "$type": "$left" });
    projection.insert("trunc", doc! { "$trunc": [2.65, 1] });
    projection.insert("literal", doc! { "$literal": { "nested": true } });
    let projected = apply_projection(&document, Some(&projection)).expect("apply projection");

    let mut expected = Document::new();
    expected.insert("_id", 1);
    expected.insert("abs", 5_i64);
    expected.insert("add", 10_i64);
    expected.insert("allElementsTrue", true);
    expected.insert("eq", true);
    expected.insert("ne", true);
    expected.insert("gt", true);
    expected.insert("gte", true);
    expected.insert("lt", true);
    expected.insert("lte", true);
    expected.insert("anyElementTrue", true);
    expected.insert("arrayElemAt", 3);
    expected.insert("arrayToObject", doc! { "price": 24, "item": "apple" });
    expected.insert("cmp", 1);
    expected.insert("concat", "prefix-abc");
    expected.insert("concatArrays", vec![1, 2, 3, 4, 5]);
    expected.insert("and", true);
    expected.insert("or", true);
    expected.insert("not", true);
    expected.insert("in", true);
    expected.insert("const", "fixed");
    expected.insert("divide", 3.5);
    expected.insert("expr", true);
    expected.insert("first", 1);
    expected.insert("floor", 2_i64);
    expected.insert("ceil", 3_i64);
    expected.insert("ifNull", 5);
    expected.insert("isArray", true);
    expected.insert("isNumber", true);
    expected.insert("last", 3);
    expected.insert("mod", 2_i64);
    expected.insert("mergeObjects", doc! { "a": 1, "b": 9, "c": 3 });
    expected.insert("multiply", 10_i64);
    expected.insert(
        "objectToArray",
        vec![doc! { "k": "a", "v": 1 }, doc! { "k": "b", "v": 2 }],
    );
    expected.insert("round", 2.7);
    expected.insert("size", 3_i64);
    expected.insert("subtract", 2_i64);
    expected.insert("type", "int");
    expected.insert("trunc", 2.6);
    expected.insert("literal", doc! { "nested": true });

    assert_eq!(projected, expected);
}

#[test]
fn projection_supports_scoped_and_field_access_expressions() {
    let document = doc! {
        "_id": 1,
        "simple": [1, 2, 3, 4],
        "nested": [{ "a": 1 }, { "a": 2 }],
        "mixed": [{ "a": 1 }, {}, { "a": 2 }, { "a": Bson::Null }],
        "nestedDoc": { "four": 4 },
        "lookupField": "a.b",
        "a.b": "literal",
        "special": { "$price": 5 }
    };
    let projected = apply_projection(
        &document,
        Some(&doc! {
            "mapped": { "$map": { "input": "$simple", "as": "outer", "in": { "$add": [10, "$$outer"] } } },
            "mappedCurrent": { "$map": { "input": "$nested", "as": "CURRENT", "in": "$a" } },
            "mappedMixed": { "$map": { "input": "$mixed", "as": "item", "in": "$$item.a" } },
            "filtered": { "$filter": { "input": "$simple", "as": "value", "cond": { "$gt": ["$$value", 2] } } },
            "filteredDefault": { "$filter": { "input": "$simple", "cond": { "$eq": [2, "$$this"] }, "limit": { "$literal": 1 } } },
            "letValue": {
                "$let": {
                    "vars": { "CURRENT": "$nestedDoc", "factor": 10 },
                    "in": { "$add": ["$four", "$$factor"] }
                }
            },
            "swapped": {
                "$let": {
                    "vars": { "x": 6, "y": 10 },
                    "in": {
                        "$let": {
                            "vars": { "x": "$$y", "y": "$$x" },
                            "in": { "$subtract": ["$$x", "$$y"] }
                        }
                    }
                }
            },
            "getFieldDynamic": { "$getField": "$lookupField" },
            "getFieldObject": { "$getField": { "field": { "$const": "$price" }, "input": "$special" } }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "mapped": [11_i64, 12_i64, 13_i64, 14_i64],
            "mappedCurrent": [1, 2],
            "mappedMixed": [1, Bson::Null, 2, Bson::Null],
            "filtered": [3, 4],
            "filteredDefault": [2],
            "letValue": 14_i64,
            "swapped": 4_i64,
            "getFieldDynamic": "literal",
            "getFieldObject": 5
        }
    );
}

#[test]
fn projection_preserves_missing_results_for_scoped_and_field_access_expressions() {
    let projected = apply_projection(
        &doc! { "_id": 1, "maybeArray": Bson::Null, "special": { "present": 1 } },
        Some(&doc! {
            "nullMap": { "$map": { "input": "$maybeArray", "in": "$$this" } },
            "nullFilter": { "$filter": { "input": "$missing", "cond": true } },
            "missingField": { "$getField": { "field": "missing", "input": "$special" } }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! { "_id": 1, "nullMap": Bson::Null, "nullFilter": Bson::Null }
    );
}

#[test]
fn projection_supports_field_mutation_expressions() {
    let projected = apply_projection(
        &doc! { "_id": 1, "base": { "keep": true } },
        Some(&doc! {
            "setSimple": { "$setField": { "field": "status", "input": { "a": 1 }, "value": 24 } },
            "unsetSimple": { "$unsetField": { "field": "a", "input": { "a": 1, "b": 2 } } },
            "removeWithSetField": { "$setField": { "field": "a", "input": { "a": 1, "b": 2 }, "value": "$$REMOVE" } },
            "literalDot": { "$setField": { "field": { "$const": "a.b" }, "input": { "$const": { "a.b": 5 } }, "value": 12345 } },
            "literalDollar": { "$setField": { "field": { "$const": "$price" }, "input": { "$const": { "$price": 5 } }, "value": 9 } },
            "nullInput": { "$unsetField": { "field": "a", "input": Bson::Null } },
            "nestedGet": {
                "$getField": {
                    "field": "foo",
                    "input": { "$setField": { "field": "foo", "input": "$$ROOT", "value": 1234 } }
                }
            }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "setSimple": { "a": 1, "status": 24 },
            "unsetSimple": { "b": 2 },
            "removeWithSetField": { "b": 2 },
            "literalDot": { "a.b": 12345 },
            "literalDollar": { "$price": 9 },
            "nullInput": Bson::Null,
            "nestedGet": 1234
        }
    );
}

#[test]
fn projection_supports_array_sequence_expressions() {
    let projected = apply_projection(
        &doc! { "_id": 1, "array": [1, 2, 3, 2, 1], "seq": [1, 2, 3], "nullish": Bson::Null },
        Some(&doc! {
            "indexOfArray": { "$indexOfArray": ["$array", 2] },
            "indexOfArrayFrom": { "$indexOfArray": ["$array", 2, 2] },
            "range": { "$range": [0, 5, 2] },
            "reverseArray": { "$reverseArray": "$seq" },
            "sliceCount": { "$slice": ["$array", 2] },
            "sliceWindow": { "$slice": ["$array", 1, 2] },
            "nullIndex": { "$indexOfArray": [Bson::Null, 2] },
            "nullReverse": { "$reverseArray": "$missing" }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "indexOfArray": 1_i64,
            "indexOfArrayFrom": 3_i64,
            "range": [0, 2, 4],
            "reverseArray": [3, 2, 1],
            "sliceCount": [1, 2],
            "sliceWindow": [2, 3],
            "nullIndex": Bson::Null,
            "nullReverse": Bson::Null
        }
    );
}

#[test]
fn projection_supports_reduce_expression() {
    let projected = apply_projection(
        &doc! {
            "_id": 1,
            "array": [1, 2, 3],
            "nested": [[1, 2, 3], [4, 5]],
            "matrix": [[0, 1], [2, 3]]
        },
        Some(&doc! {
            "sum": {
                "$reduce": {
                    "input": "$array",
                    "initialValue": { "$literal": 0 },
                    "in": { "$add": ["$$value", "$$this"] }
                }
            },
            "empty": {
                "$reduce": {
                    "input": [],
                    "initialValue": { "$literal": 0 },
                    "in": 10
                }
            },
            "concat": {
                "$reduce": {
                    "input": "$array",
                    "initialValue": [],
                    "in": { "$concatArrays": ["$$value", ["$$this"]] }
                }
            },
            "nestedReduce": {
                "$reduce": {
                    "input": "$nested",
                    "initialValue": 1,
                    "in": {
                        "$multiply": [
                            "$$value",
                            {
                                "$reduce": {
                                    "input": "$$this",
                                    "initialValue": 0,
                                    "in": { "$add": ["$$value", "$$this"] }
                                }
                            }
                        ]
                    }
                }
            },
            "nestedLet": {
                "$reduce": {
                    "input": "$matrix",
                    "initialValue": { "allElements": [], "sumOfInner": { "$literal": 0 } },
                    "in": {
                        "$let": {
                            "vars": { "outerValue": "$$value", "innerArray": "$$this" },
                            "in": {
                                "$reduce": {
                                    "input": "$$innerArray",
                                    "initialValue": "$$outerValue",
                                    "in": {
                                        "allElements": {
                                            "$concatArrays": ["$$value.allElements", ["$$this"]]
                                        },
                                        "sumOfInner": { "$add": ["$$value.sumOfInner", "$$this"] }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "sum": 6_i64,
            "empty": 0,
            "concat": [1, 2, 3],
            "nestedReduce": 54_i64,
            "nestedLet": { "allElements": [0, 1, 2, 3], "sumOfInner": 6_i64 }
        }
    );
}

#[test]
fn projection_preserves_nullish_reduce_input() {
    let projected = apply_projection(
        &doc! { "_id": 1, "items": Bson::Null },
        Some(&doc! {
            "nullInput": { "$reduce": { "input": "$items", "initialValue": 0, "in": 5 } },
            "missingInput": { "$reduce": { "input": "$missing", "initialValue": 0, "in": 5 } }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! { "_id": 1, "nullInput": Bson::Null, "missingInput": Bson::Null }
    );
}

#[test]
fn projection_supports_switch_expression() {
    let projected = apply_projection(
        &doc! { "_id": 1, "flag": true, "qty": 2 },
        Some(&doc! {
            "firstMatch": {
                "$switch": {
                    "branches": [
                        { "case": { "$eq": [1, 1] }, "then": "one is equal to one!" },
                        { "case": { "$eq": [2, 2] }, "then": "two is equal to two!" }
                    ]
                }
            },
            "defaulted": {
                "$switch": {
                    "branches": [{ "case": { "$eq": [1, 2] }, "then": "one is equal to two!" }],
                    "default": "no case matched."
                }
            },
            "nullCase": {
                "$switch": {
                    "branches": [{ "case": Bson::Null, "then": "Null was true!" }],
                    "default": "No case matched."
                }
            },
            "missingCase": {
                "$switch": {
                    "branches": [{ "case": "$missingField", "then": "Missing was true!" }],
                    "default": "No case matched."
                }
            },
            "nullThen": {
                "$switch": {
                    "branches": [{ "case": true, "then": Bson::Null }],
                    "default": false
                }
            },
            "nullDefault": {
                "$switch": {
                    "branches": [{ "case": Bson::Null, "then": false }],
                    "default": Bson::Null
                }
            }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "firstMatch": "one is equal to one!",
            "defaulted": "no case matched.",
            "nullCase": "No case matched.",
            "missingCase": "No case matched.",
            "nullThen": Bson::Null,
            "nullDefault": Bson::Null
        }
    );
}

#[test]
fn projection_preserves_missing_switch_results() {
    let projected = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "missingThen": {
                "$switch": {
                    "branches": [{ "case": true, "then": "$missingField" }],
                    "default": false
                }
            },
            "missingDefault": {
                "$switch": {
                    "branches": [{ "case": Bson::Null, "then": false }],
                    "default": "$missingField"
                }
            }
        }),
    )
    .expect("apply projection");

    assert_eq!(projected, doc! { "_id": 1 });
}

#[test]
fn switch_expression_rejects_invalid_arguments() {
    let non_object = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": "not an object" }
        }),
    )
    .expect_err("switch requires object");
    assert!(matches!(non_object, QueryError::InvalidStructure));

    let branches_not_array = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": "not an array" } }
        }),
    )
    .expect_err("branches must be array");
    assert!(matches!(branches_not_array, QueryError::InvalidStructure));

    let branch_not_object = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": ["not an object"] } }
        }),
    )
    .expect_err("branch must be object");
    assert!(matches!(branch_not_object, QueryError::InvalidStructure));

    let missing_case = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": [{}] } }
        }),
    )
    .expect_err("branch case required");
    assert!(matches!(missing_case, QueryError::InvalidStructure));

    let missing_then = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": [{ "case": 1 }] } }
        }),
    )
    .expect_err("branch then required");
    assert!(matches!(missing_then, QueryError::InvalidStructure));

    let branch_unknown = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": [{ "case": true, "then": false, "badKey": 1 }] } }
        }),
    )
    .expect_err("unknown branch key");
    assert!(matches!(branch_unknown, QueryError::InvalidStructure));

    let unknown_argument = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "notAnArgument": 1 } }
        }),
    )
    .expect_err("unknown argument");
    assert!(matches!(unknown_argument, QueryError::InvalidStructure));

    let empty_branches = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": [] } }
        }),
    )
    .expect_err("requires at least one branch");
    assert!(matches!(empty_branches, QueryError::InvalidStructure));

    let no_default_match = apply_projection(
        &doc! { "_id": 1, "x": 1 },
        Some(&doc! {
            "out": { "$switch": { "branches": [{ "case": { "$eq": ["$x", 0] }, "then": 1 }] } }
        }),
    )
    .expect_err("missing default should fail");
    assert!(matches!(no_default_match, QueryError::InvalidArgument(_)));
}

#[test]
fn projection_supports_set_expressions() {
    let projected = apply_projection(
        &doc! {
            "_id": 1,
            "arr1": [1, 2, 3, 2, 1],
            "arr2": [2, 3, 4, 3],
            "nested": [[1], [1], [2]]
        },
        Some(&doc! {
            "union": { "$setUnion": ["$arr1", "$arr2"] },
            "intersection": { "$setIntersection": ["$arr1", "$arr2"] },
            "difference": { "$setDifference": ["$arr1", "$arr2"] },
            "equals": { "$setEquals": ["$arr1", [1, 2, 3, 2]] },
            "isSubset": { "$setIsSubset": [[2, 3], "$arr2"] },
            "nestedUnion": { "$setUnion": ["$nested", [[2], [3]]] },
            "emptyUnion": { "$setUnion": [] },
            "emptyIntersection": { "$setIntersection": [] }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "union": [1, 2, 3, 4],
            "intersection": [2, 3],
            "difference": [1],
            "equals": true,
            "isSubset": true,
            "nestedUnion": [[1], [2], [3]],
            "emptyUnion": Bson::Array(Vec::new()),
            "emptyIntersection": Bson::Array(Vec::new())
        }
    );
}

#[test]
fn set_expressions_handle_nullish_and_invalid_inputs() {
    let projected = apply_projection(
        &doc! { "_id": 1, "arr": Bson::Null },
        Some(&doc! {
            "union": { "$setUnion": ["$arr", [1, 2, 3]] },
            "intersection": { "$setIntersection": ["$arr", [1, 2, 3]] },
            "difference": { "$setDifference": ["$arr", [1, 2, 3]] }
        }),
    )
    .expect("nullish null-result operators");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "union": Bson::Null,
            "intersection": Bson::Null,
            "difference": Bson::Null
        }
    );

    let equals_null = apply_projection(
        &doc! { "_id": 1, "arr": Bson::Null },
        Some(&doc! {
            "out": { "$setEquals": ["$arr", [1, 2, 3]] }
        }),
    )
    .expect_err("setEquals rejects nullish arrays");
    assert!(matches!(equals_null, QueryError::InvalidArgument(_)));

    let subset_missing = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$setIsSubset": [[1], "$missing"] }
        }),
    )
    .expect_err("setIsSubset rejects missing arrays");
    assert!(matches!(subset_missing, QueryError::InvalidArgument(_)));

    let union_non_array = apply_projection(
        &doc! { "_id": 1, "arr": "nope" },
        Some(&doc! {
            "out": { "$setUnion": ["$arr", [1, 2, 3]] }
        }),
    )
    .expect_err("setUnion requires arrays");
    assert!(matches!(union_non_array, QueryError::InvalidArgument(_)));

    let equals_too_few = apply_projection(
        &doc! { "_id": 1, "arr": [1, 2] },
        Some(&doc! {
            "out": { "$setEquals": ["$arr"] }
        }),
    )
    .expect_err("setEquals requires two operands");
    assert!(matches!(equals_too_few, QueryError::InvalidStructure));
}

#[test]
fn projection_supports_case_expression_operators() {
    let projected = apply_projection(
        &doc! {
            "_id": 1,
            "text": "aBz",
            "number": 555.5,
            "when": DateTime::from_millis(0),
            "nested": { "str": "hello world" },
            "unicode": "\u{0080}D€"
        },
        Some(&doc! {
            "upper": { "$toUpper": "$text" },
            "lower": { "$toLower": ["$text"] },
            "numberLower": { "$toLower": "$number" },
            "dateLower": { "$toLower": "$when" },
            "nullUpper": { "$toUpper": Bson::Null },
            "fieldUpper": { "$toUpper": "$nested.str" },
            "strcasecmpEqual": { "$strcasecmp": ["Ab", "aB"] },
            "strcasecmpNumeric": { "$strcasecmp": ["1.23", 1.23] },
            "strcasecmpAccent": { "$strcasecmp": ["ó", "Ó"] },
            "unicodeUpper": { "$toUpper": "$unicode" },
            "unicodeLower": { "$toLower": "$unicode" }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "upper": "ABZ",
            "lower": "abz",
            "numberLower": "555.5",
            "dateLower": "1970-01-01t00:00:00.000z",
            "nullUpper": "",
            "fieldUpper": "HELLO WORLD",
            "strcasecmpEqual": 0,
            "strcasecmpNumeric": 0,
            "strcasecmpAccent": 1,
            "unicodeUpper": "\u{0080}D€",
            "unicodeLower": "\u{0080}d€"
        }
    );
}

#[test]
fn projection_supports_bitwise_expression_operators() {
    let projected = apply_projection(
        &doc! {
            "_id": 1,
            "a": 3,
            "b": 5,
            "arr": [1, 2]
        },
        Some(&doc! {
            "bitAnd": { "$bitAnd": ["$a", "$b"] },
            "bitOr": { "$bitOr": ["$a", "$b"] },
            "bitXor": { "$bitXor": ["$a", "$b"] },
            "bitNot": { "$bitNot": "$a" },
            "identityAnd": { "$bitAnd": [] },
            "identityOr": { "$bitOr": [] },
            "identityXor": { "$bitXor": [] }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "bitAnd": 1_i64,
            "bitOr": 7_i64,
            "bitXor": 6_i64,
            "bitNot": -4_i64,
            "identityAnd": -1_i64,
            "identityOr": 0_i64,
            "identityXor": 0_i64
        }
    );
}

#[test]
fn case_expression_operators_reject_invalid_inputs() {
    let wrong_arity = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$toUpper": ["a", "b"] }
        }),
    )
    .expect_err("toUpper rejects multiple arguments");
    assert!(matches!(wrong_arity, QueryError::InvalidStructure));

    let invalid_upper = apply_projection(
        &doc! { "_id": 1, "flag": true },
        Some(&doc! {
            "out": { "$toUpper": "$flag" }
        }),
    )
    .expect_err("toUpper requires string-compatible input");
    assert!(matches!(invalid_upper, QueryError::InvalidArgument(_)));

    let invalid_lower = apply_projection(
        &doc! { "_id": 1, "items": ["a"] },
        Some(&doc! {
            "out": { "$toLower": "$items" }
        }),
    )
    .expect_err("toLower rejects arrays");
    assert!(matches!(invalid_lower, QueryError::InvalidArgument(_)));

    let invalid_strcasecmp = apply_projection(
        &doc! { "_id": 1, "value": { "a": 1 } },
        Some(&doc! {
            "out": { "$strcasecmp": ["$value", "abc"] }
        }),
    )
    .expect_err("strcasecmp rejects documents");
    assert!(matches!(invalid_strcasecmp, QueryError::InvalidArgument(_)));

    let invalid_strcasecmp_arity = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$strcasecmp": ["a"] }
        }),
    )
    .expect_err("strcasecmp requires two arguments");
    assert!(matches!(
        invalid_strcasecmp_arity,
        QueryError::InvalidStructure
    ));
}

#[test]
fn bitwise_expression_operators_reject_invalid_inputs() {
    let invalid_double = apply_projection(
        &doc! { "_id": 1, "value": 12.0 },
        Some(&doc! {
            "out": { "$bitNot": "$value" }
        }),
    )
    .expect_err("bitNot rejects doubles");
    assert!(matches!(invalid_double, QueryError::InvalidArgument(_)));

    let invalid_decimal = apply_projection(
        &doc! { "_id": 1, "value": Decimal128::from_str("12").expect("decimal") },
        Some(&doc! {
            "out": { "$bitAnd": [1, "$value"] }
        }),
    )
    .expect_err("bitAnd rejects decimals");
    assert!(matches!(invalid_decimal, QueryError::InvalidArgument(_)));

    let invalid_array = apply_projection(
        &doc! { "_id": 1, "arr": [1, 2] },
        Some(&doc! {
            "out": { "$bitOr": "$arr" }
        }),
    )
    .expect_err("bitOr rejects arrays");
    assert!(matches!(invalid_array, QueryError::InvalidArgument(_)));

    let invalid_unary_arity = apply_projection(
        &doc! { "_id": 1, "a": 1, "b": 2 },
        Some(&doc! {
            "out": { "$bitNot": ["$a", "$b"] }
        }),
    )
    .expect_err("bitNot requires one operand");
    assert!(matches!(invalid_unary_arity, QueryError::InvalidStructure));
}

#[test]
fn reduce_expression_rejects_invalid_arguments() {
    let non_object = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$reduce": 0 }
        }),
    )
    .expect_err("reduce requires object");
    assert!(matches!(non_object, QueryError::InvalidStructure));

    let missing_field = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$reduce": { "input": "$items", "initialValue": 0 } }
        }),
    )
    .expect_err("missing in");
    assert!(matches!(missing_field, QueryError::InvalidStructure));

    let unknown_field = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$reduce": { "input": "$items", "initialValue": 0, "in": "$$value", "notAField": 1 } }
        }),
    )
    .expect_err("unknown field");
    assert!(matches!(unknown_field, QueryError::InvalidStructure));

    let non_array_input = apply_projection(
        &doc! { "_id": 1, "items": 5 },
        Some(&doc! {
            "out": { "$reduce": { "input": "$items", "initialValue": 0, "in": "$$value" } }
        }),
    )
    .expect_err("non array input");
    assert!(matches!(non_array_input, QueryError::InvalidArgument(_)));

    let undefined_value = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$reduce": { "input": "$$value", "initialValue": [], "in": [] } }
        }),
    )
    .expect_err("undefined $$value outside in");
    assert!(matches!(undefined_value, QueryError::InvalidArgument(_)));

    let undefined_this = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$reduce": { "input": "$$this", "initialValue": [], "in": [] } }
        }),
    )
    .expect_err("undefined $$this outside in");
    assert!(matches!(undefined_this, QueryError::InvalidArgument(_)));
}

#[test]
fn projection_preserves_missing_expression_results() {
    let projected = apply_projection(
        &doc! { "_id": 1, "array": [1, 2, 3], "object": { "a": 1 } },
        Some(&doc! {
            "outOfBounds": { "$arrayElemAt": ["$array", 5] },
            "firstEmpty": { "$first": [] },
            "lastEmpty": { "$last": "$missingArray" },
            "merged": { "$mergeObjects": ["$object", { "b": "$missingField" }] }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! { "_id": 1, "lastEmpty": Bson::Null, "merged": { "a": 1 } }
    );
}

#[test]
fn projection_supports_type_expression_for_missing_and_present_values() {
    let projected = apply_projection(
        &doc! {
            "_id": 1,
            "intValue": 5,
            "text": "abc",
            "items": [1, 2],
            "nested": { "ok": true }
        },
        Some(&doc! {
            "intType": { "$type": "$intValue" },
            "stringType": { "$type": "$text" },
            "arrayType": { "$type": "$items" },
            "objectType": { "$type": "$nested" },
            "missingType": { "$type": "$missing" }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "intType": "int",
            "stringType": "string",
            "arrayType": "array",
            "objectType": "object",
            "missingType": "missing"
        }
    );
}

#[test]
fn scoped_expressions_reject_invalid_variables_and_shapes() {
    let invalid_name = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$let": { "vars": { "ROOT": 1 }, "in": "$$ROOT" } }
        }),
    )
    .expect_err("invalid variable name");
    assert!(matches!(invalid_name, QueryError::InvalidArgument(_)));

    let undefined_default = apply_projection(
        &doc! { "_id": 1, "items": [1, 2] },
        Some(&doc! {
            "out": { "$map": { "input": "$items", "as": "value", "in": "$$this" } }
        }),
    )
    .expect_err("undefined variable");
    assert!(matches!(undefined_default, QueryError::InvalidArgument(_)));

    let invalid_get_field = apply_projection(
        &doc! { "_id": 1, "value": 2 },
        Some(&doc! {
            "out": { "$getField": { "field": 5, "input": { "a": 1 } } }
        }),
    )
    .expect_err("non-string getField");
    assert!(matches!(invalid_get_field, QueryError::InvalidArgument(_)));
}

#[test]
fn field_mutation_expressions_reject_invalid_arguments() {
    let field_path = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$setField": { "field": "$field_path", "input": {}, "value": 0 } }
        }),
    )
    .expect_err("field path is not a literal");
    assert!(matches!(field_path, QueryError::InvalidArgument(_)));

    let dynamic_field = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$setField": { "field": { "$concat": ["a", "b"] }, "input": {}, "value": 0 } }
        }),
    )
    .expect_err("dynamic field expression");
    assert!(matches!(dynamic_field, QueryError::InvalidArgument(_)));

    let invalid_input = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$setField": { "field": "a", "input": true, "value": 0 } }
        }),
    )
    .expect_err("non-object input");
    assert!(matches!(invalid_input, QueryError::InvalidArgument(_)));

    let unexpected_unset_value = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$unsetField": { "field": "a", "input": {}, "value": 0 } }
        }),
    )
    .expect_err("unexpected unsetField value");
    assert!(matches!(
        unexpected_unset_value,
        QueryError::InvalidStructure
    ));
}

#[test]
fn array_sequence_expressions_reject_invalid_arguments() {
    let invalid_index = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$indexOfArray": ["string", "s"] }
        }),
    )
    .expect_err("non-array input");
    assert!(matches!(invalid_index, QueryError::InvalidArgument(_)));

    let invalid_range = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$range": [1, 3, 0] }
        }),
    )
    .expect_err("zero step");
    assert!(matches!(invalid_range, QueryError::InvalidArgument(_)));

    let invalid_reverse = apply_projection(
        &doc! { "_id": 1, "value": 1 },
        Some(&doc! {
            "out": { "$reverseArray": "$value" }
        }),
    )
    .expect_err("scalar reverse");
    assert!(matches!(invalid_reverse, QueryError::InvalidArgument(_)));

    let invalid_slice = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! {
            "out": { "$slice": [[1, 2], 0, 0] }
        }),
    )
    .expect_err("non-positive count");
    assert!(matches!(invalid_slice, QueryError::InvalidArgument(_)));
}

#[test]
fn expression_operators_reject_invalid_numeric_forms() {
    let divide_by_zero = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! { "value": { "$divide": [1, 0] } }),
    )
    .expect_err("divide by zero");
    assert!(matches!(divide_by_zero, QueryError::InvalidArgument(_)));

    let if_null_requires_at_least_two_arguments = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! { "value": { "$ifNull": ["$missing"] } }),
    )
    .expect_err("ifNull arity");
    assert!(matches!(
        if_null_requires_at_least_two_arguments,
        QueryError::InvalidStructure
    ));

    let round_requires_integral_place = apply_projection(
        &doc! { "_id": 1 },
        Some(&doc! { "value": { "$round": [2.7, 1.5] } }),
    )
    .expect_err("round place");
    assert!(matches!(
        round_requires_integral_place,
        QueryError::InvalidStructure
    ));

    let add_requires_numeric_operands = apply_projection(
        &doc! { "_id": 1, "text": "abc" },
        Some(&doc! { "value": { "$add": ["$text", 1] } }),
    )
    .expect_err("numeric add");
    assert!(matches!(
        add_requires_numeric_operands,
        QueryError::ExpectedNumeric
    ));
}

#[test]
fn expression_operators_reject_invalid_array_and_object_forms() {
    let array_elem_at_requires_an_array = apply_projection(
        &doc! { "_id": 1, "value": "text" },
        Some(&doc! { "value": { "$arrayElemAt": ["$value", 0] } }),
    )
    .expect_err("arrayElemAt array");
    assert!(matches!(
        array_elem_at_requires_an_array,
        QueryError::InvalidArgument(_)
    ));

    let array_elem_at_requires_an_integral_index = apply_projection(
        &doc! { "_id": 1, "array": [1, 2, 3] },
        Some(&doc! { "value": { "$arrayElemAt": ["$array", 1.5] } }),
    )
    .expect_err("arrayElemAt index");
    assert!(matches!(
        array_elem_at_requires_an_integral_index,
        QueryError::InvalidArgument(_)
    ));

    let size_requires_array_input = apply_projection(
        &doc! { "_id": 1, "value": 3 },
        Some(&doc! { "value": { "$size": "$value" } }),
    )
    .expect_err("size input");
    assert!(matches!(
        size_requires_array_input,
        QueryError::InvalidArgument(_)
    ));

    let array_to_object_rejects_mixed_entry_shapes = apply_projection(
        &doc! { "_id": 1, "entries": [["price", 24], { "k": "item", "v": "apple" }] },
        Some(&doc! { "value": { "$arrayToObject": "$entries" } }),
    )
    .expect_err("arrayToObject mixed");
    assert!(matches!(
        array_to_object_rejects_mixed_entry_shapes,
        QueryError::InvalidArgument(_)
    ));

    let merge_objects_rejects_non_documents = apply_projection(
        &doc! { "_id": 1, "value": "text" },
        Some(&doc! { "value": { "$mergeObjects": ["$value", { "ok": true }] } }),
    )
    .expect_err("mergeObjects non document");
    assert!(matches!(
        merge_objects_rejects_non_documents,
        QueryError::InvalidArgument(_)
    ));

    let object_to_array_rejects_non_documents = apply_projection(
        &doc! { "_id": 1, "value": [1, 2, 3] },
        Some(&doc! { "value": { "$objectToArray": "$value" } }),
    )
    .expect_err("objectToArray non document");
    assert!(matches!(
        object_to_array_rejects_non_documents,
        QueryError::InvalidArgument(_)
    ));

    let concat_requires_string_inputs = apply_projection(
        &doc! { "_id": 1, "value": 3 },
        Some(&doc! { "value": { "$concat": ["prefix-", "$value"] } }),
    )
    .expect_err("concat string");
    assert!(matches!(
        concat_requires_string_inputs,
        QueryError::InvalidArgument(_)
    ));

    let any_elements_true_requires_array_input = apply_projection(
        &doc! { "_id": 1, "value": 3 },
        Some(&doc! { "value": { "$anyElementTrue": "$value" } }),
    )
    .expect_err("anyElementTrue array");
    assert!(matches!(
        any_elements_true_requires_array_input,
        QueryError::InvalidArgument(_)
    ));
}

#[test]
fn projection_supports_nested_exclusion_paths() {
    let document =
        doc! { "_id": 1, "sku": "abc", "qty": 5, "meta": { "enabled": true, "flag": "beta" } };
    let projected = apply_projection(
        &document,
        Some(&doc! { "_id": 0, "qty": 0, "meta.enabled": 0 }),
    )
    .expect("apply projection");

    assert_eq!(projected, doc! { "sku": "abc", "meta": { "flag": "beta" } });
}

#[test]
fn runs_match_group_and_sort_pipeline() {
    let documents = vec![
        doc! { "team": "red", "points": 10 },
        doc! { "team": "red", "points": 4 },
        doc! { "team": "blue", "points": 7 },
    ];
    let results = run_pipeline(
        documents,
        &[
            doc! { "$match": { "points": { "$gte": 4 } } },
            doc! { "$group": { "_id": "$team", "total": { "$sum": "$points" } } },
            doc! { "$sort": { "total": -1 } },
        ],
    )
    .expect("pipeline");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_str("_id").expect("team"), "red");
    assert_eq!(results[0].get("total"), Some(&Bson::Int64(14)));
}

#[test]
fn match_stage_supports_supported_query_operators() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "qty": 5, "sku": "abc", "meta": { "enabled": true } },
            doc! { "_id": 2, "qty": 1, "sku": "xyz" },
        ],
        &[doc! {
            "$match": {
                "$and": [
                    { "qty": { "$gte": 5 } },
                    { "$or": [
                        { "sku": { "$in": ["def", "abc"] } },
                        { "meta.enabled": { "$exists": true } }
                    ] }
                ]
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! { "_id": 1, "qty": 5, "sku": "abc", "meta": { "enabled": true } }]
    );
}

#[test]
fn match_stage_supports_expr_filters() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "qty": 5, "limit": 4 },
            doc! { "_id": 2, "qty": 1, "limit": 4 },
        ],
        &[doc! { "$match": { "$expr": { "$gt": ["$qty", "$limit"] } } }],
    );

    assert_eq!(results, vec![doc! { "_id": 1, "qty": 5, "limit": 4 }]);
}

#[test]
fn set_and_add_fields_stages_preserve_fields_and_support_literals() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 1, "sku": "abc", "qty": 2, "meta": { "enabled": true } }],
        &[
            doc! { "$set": { "qty": 5, "meta.flag": { "$literal": "beta" } } },
            doc! { "$addFields": { "copiedSku": "$sku" } },
        ],
    );

    assert_eq!(
        results,
        vec![doc! {
            "_id": 1,
            "sku": "abc",
            "qty": 5,
            "meta": { "enabled": true, "flag": "beta" },
            "copiedSku": "abc"
        }]
    );
}

#[test]
fn unset_stage_supports_string_and_array_syntax() {
    let input = vec![doc! {
        "_id": 1,
        "sku": "abc",
        "qty": 5,
        "meta": { "enabled": true, "flag": "beta" }
    }];

    let single = run_pipeline_ok(input.clone(), &[doc! { "$unset": "qty" }]);
    assert_eq!(
        single,
        vec![doc! { "_id": 1, "sku": "abc", "meta": { "enabled": true, "flag": "beta" } }]
    );

    let multiple = run_pipeline_ok(input, &[doc! { "$unset": ["qty", "meta.enabled"] }]);
    assert_eq!(
        multiple,
        vec![doc! { "_id": 1, "sku": "abc", "meta": { "flag": "beta" } }]
    );
}

#[test]
fn skip_and_limit_trim_pipeline_results() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1 },
            doc! { "_id": 2 },
            doc! { "_id": 3 },
            doc! { "_id": 4 },
        ],
        &[doc! { "$skip": 1 }, doc! { "$limit": 2 }],
    );

    assert_eq!(results, vec![doc! { "_id": 2 }, doc! { "_id": 3 }]);
}

#[test]
fn sample_stage_returns_a_unique_subset_or_all_documents() {
    let input = vec![doc! { "_id": 1 }, doc! { "_id": 2 }, doc! { "_id": 3 }];
    let sampled = run_pipeline_ok(input.clone(), &[doc! { "$sample": { "size": 2 } }]);
    assert_eq!(sampled.len(), 2);
    let ids = sampled
        .iter()
        .map(|document| document.get_i32("_id").expect("_id"))
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), 2);
    assert!(ids.iter().all(|id| (1..=3).contains(id)));

    let all = run_pipeline_ok(input, &[doc! { "$sample": { "size": 10 } }]);
    assert_eq!(all.len(), 3);
}

#[test]
fn sample_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$sample": "string" },
        doc! { "$sample": {} },
        doc! { "$sample": { "size": "two" } },
        doc! { "$sample": { "size": 0 } },
        doc! { "$sample": { "size": -1 } },
        doc! { "$sample": { "size": 1, "unknownOpt": true } },
    ] {
        let error = run_pipeline(vec![doc! { "_id": 1 }], &[stage]).expect_err("invalid sample");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn multiple_sample_stages_are_allowed() {
    let input = vec![doc! { "_id": 1 }, doc! { "_id": 2 }, doc! { "_id": 3 }];
    let results = run_pipeline_ok(
        input,
        &[
            doc! { "$sample": { "size": 3 } },
            doc! { "$sample": { "size": 1 } },
        ],
    );
    assert_eq!(results.len(), 1);
}

#[test]
fn sort_by_count_groups_and_sorts_descending() {
    let results = run_pipeline_ok(
        vec![
            doc! { "team": "red" },
            doc! { "team": "blue" },
            doc! { "team": "blue" },
            doc! { "team": "green" },
            doc! { "team": "green" },
            doc! { "team": "green" },
        ],
        &[doc! { "$sortByCount": "$team" }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": "green", "count": 3_i64 },
            doc! { "_id": "blue", "count": 2_i64 },
            doc! { "_id": "red", "count": 1_i64 },
        ]
    );
}

#[test]
fn sort_by_count_accepts_expression_objects() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 1 }, doc! { "_id": 2 }],
        &[doc! { "$sortByCount": { "$literal": "all" } }],
    );

    assert_eq!(results, vec![doc! { "_id": "all", "count": 2_i64 }]);
}

#[test]
fn sort_by_count_rejects_invalid_specs() {
    for stage in [
        doc! { "$sortByCount": "team" },
        doc! { "$sortByCount": "" },
        doc! { "$sortByCount": 1 },
        doc! { "$sortByCount": {} },
        doc! { "$sortByCount": { "team": "$team" } },
    ] {
        let error =
            run_pipeline(vec![doc! { "team": "red" }], &[stage]).expect_err("invalid stage");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn count_stage_uses_dynamic_output_field() {
    let results = run_pipeline(
        vec![doc! { "value": 1 }, doc! { "value": 2 }],
        &[doc! { "$count": "total" }],
    )
    .expect("pipeline");

    assert_eq!(results, vec![doc! { "total": 2_i64 }]);
}

#[test]
fn count_stage_returns_zero_for_empty_input() {
    let results = run_pipeline_ok(Vec::new(), &[doc! { "$count": "total" }]);
    assert_eq!(results, vec![doc! { "total": 0_i64 }]);
}

#[test]
fn sort_stage_supports_dotted_compound_and_descending_keys() {
    let dotted = run_pipeline_ok(
        vec![
            doc! { "_id": 0, "a": { "b": 2 } },
            doc! { "_id": 1, "a": { "b": 1 } },
        ],
        &[doc! { "$sort": { "a.b": 1 } }],
    );
    assert_eq!(
        dotted,
        vec![
            doc! { "_id": 1, "a": { "b": 1 } },
            doc! { "_id": 0, "a": { "b": 2 } }
        ]
    );

    let compound = run_pipeline_ok(
        vec![
            doc! { "_id": 0, "a": 1, "b": 3 },
            doc! { "_id": 1, "a": 1, "b": 2 },
            doc! { "_id": 2, "a": 0, "b": 4 },
        ],
        &[doc! { "$sort": { "a": 1, "b": -1 } }],
    );
    assert_eq!(
        compound,
        vec![
            doc! { "_id": 2, "a": 0, "b": 4 },
            doc! { "_id": 0, "a": 1, "b": 3 },
            doc! { "_id": 1, "a": 1, "b": 2 }
        ]
    );
}

#[test]
fn sort_stage_orders_missing_and_null_before_numbers() {
    let missing = run_pipeline_ok(
        vec![doc! { "_id": 0, "a": 1 }, doc! { "_id": 1 }],
        &[doc! { "$sort": { "a": 1 } }],
    );
    assert_eq!(missing, vec![doc! { "_id": 1 }, doc! { "_id": 0, "a": 1 }]);

    let null = run_pipeline_ok(
        vec![
            doc! { "_id": 0, "a": 1 },
            doc! { "_id": 1, "a": Bson::Null },
        ],
        &[doc! { "$sort": { "a": 1 } }],
    );
    assert_eq!(
        null,
        vec![
            doc! { "_id": 1, "a": Bson::Null },
            doc! { "_id": 0, "a": 1 }
        ]
    );
}

#[test]
fn unwind_stage_supports_nested_arrays() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 0, "a": { "b": [1, 2], "c": 3 } }],
        &[doc! { "$unwind": "$a.b" }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 0, "a": { "b": 1, "c": 3 } },
            doc! { "_id": 0, "a": { "b": 2, "c": 3 } }
        ]
    );
}

#[test]
fn unwind_stage_preserves_null_missing_and_empty_arrays_when_requested() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 0, "tags": [] },
            doc! { "_id": 1 },
            doc! { "_id": 2, "tags": Bson::Null },
            doc! { "_id": 3, "tags": ["a", "b"] },
        ],
        &[doc! { "$unwind": { "path": "$tags", "preserveNullAndEmptyArrays": true } }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 0 },
            doc! { "_id": 1 },
            doc! { "_id": 2, "tags": Bson::Null },
            doc! { "_id": 3, "tags": "a" },
            doc! { "_id": 3, "tags": "b" }
        ]
    );
}

#[test]
fn group_stage_supports_sum_first_push_add_to_set_and_avg_accumulators() {
    let results = run_pipeline_ok(
        vec![
            doc! { "team": "blue", "sku": "b1", "qty": 2 },
            doc! { "team": "red", "sku": "r1", "qty": 1 },
            doc! { "team": "red", "sku": "r2", "qty": 3 },
            doc! { "team": "red", "sku": "r1", "qty": 4 },
        ],
        &[
            doc! {
                "$group": {
                    "_id": "$team",
                    "total": { "$sum": "$qty" },
                    "firstSku": { "$first": "$sku" },
                    "skus": { "$push": "$sku" },
                    "uniqueSkus": { "$addToSet": "$sku" },
                    "avgQty": { "$avg": "$qty" }
                }
            },
            doc! { "$sort": { "_id": 1 } },
        ],
    );

    assert_eq!(
        results,
        vec![
            doc! {
                "_id": "blue",
                "total": 2_i64,
                "firstSku": "b1",
                "skus": ["b1"],
                "uniqueSkus": ["b1"],
                "avgQty": 2.0
            },
            doc! {
                "_id": "red",
                "total": 8_i64,
                "firstSku": "r1",
                "skus": ["r1", "r2", "r1"],
                "uniqueSkus": ["r1", "r2"],
                "avgQty": 8.0 / 3.0
            }
        ]
    );
}

#[test]
fn replace_root_promotes_subdocuments_and_expression_objects() {
    let promoted = run_pipeline_ok(
        vec![doc! { "wrapper": { "name": "alpha", "qty": 1 } }],
        &[doc! { "$replaceRoot": { "newRoot": "$wrapper" } }],
    );
    assert_eq!(promoted, vec![doc! { "name": "alpha", "qty": 1 }]);

    let expression_object = run_pipeline_ok(
        vec![doc! { "name": "alpha" }],
        &[
            doc! { "$replaceRoot": { "newRoot": { "name": "$name", "wrapped": { "$literal": true } } } },
        ],
    );
    assert_eq!(
        expression_object,
        vec![doc! { "name": "alpha", "wrapped": true }]
    );
}

#[test]
fn replace_with_alias_matches_replace_root_behavior() {
    let promoted = run_pipeline_ok(
        vec![doc! { "wrapper": { "name": "alpha", "qty": 1 } }],
        &[doc! { "$replaceWith": "$wrapper" }],
    );
    assert_eq!(promoted, vec![doc! { "name": "alpha", "qty": 1 }]);

    let expression_object = run_pipeline_ok(
        vec![doc! { "name": "alpha" }],
        &[doc! { "$replaceWith": { "name": "$name", "wrapped": { "$literal": true } } }],
    );
    assert_eq!(
        expression_object,
        vec![doc! { "name": "alpha", "wrapped": true }]
    );
}

#[test]
fn cond_expression_supports_array_and_object_forms() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 1, "qty": 2 }],
        &[doc! {
            "$project": {
                "_id": 0,
                "arrayForm": { "$cond": [{ "$lte": ["$qty", 2] }, "low", "high"] },
                "objectForm": {
                    "$cond": {
                        "if": { "$gt": ["$qty", 2] },
                        "then": "high",
                        "else": "low"
                    }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! { "arrayForm": "low", "objectForm": "low" }]
    );
}

#[test]
fn redact_stage_descends_and_prunes_nested_documents() {
    let results = run_pipeline_ok(
        vec![
            doc! {
                "_id": 1,
                "level": 1,
                "nested": {
                    "level": 2,
                    "keep": true,
                    "deeper": { "level": 3, "secret": true }
                },
                "items": [
                    { "level": 2, "visible": true },
                    { "level": 4, "hidden": true },
                    "plain"
                ]
            },
            doc! { "_id": 2, "level": 3, "drop": true },
        ],
        &[doc! {
            "$redact": {
                "$cond": [
                    { "$lte": ["$level", 2] },
                    "$$DESCEND",
                    "$$PRUNE"
                ]
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! {
            "_id": 1,
            "level": 1,
            "nested": { "level": 2, "keep": true },
            "items": [
                { "level": 2, "visible": true },
                "plain"
            ]
        }]
    );
}

#[test]
fn redact_stage_supports_keep_prune_and_descend_variables() {
    let input = vec![doc! { "_id": 1, "nested": { "value": 1 } }];

    assert_eq!(
        run_pipeline_ok(input.clone(), &[doc! { "$redact": "$$KEEP" }]),
        input
    );
    assert!(run_pipeline_ok(input.clone(), &[doc! { "$redact": "$$PRUNE" }]).is_empty());
    assert_eq!(
        run_pipeline_ok(input.clone(), &[doc! { "$redact": "$$DESCEND" }]),
        input
    );
}

#[test]
fn redact_stage_rejects_invalid_decisions() {
    let error = run_pipeline(vec![doc! { "_id": 1 }], &[doc! { "$redact": "KEEP" }])
        .expect_err("invalid redact decision");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn densify_stage_supports_explicit_numeric_bounds() {
    let results = run_pipeline_ok(
        vec![doc! { "val": 0 }, doc! { "val": 4 }, doc! { "val": 9 }],
        &[doc! {
            "$densify": {
                "field": "val",
                "range": { "step": 2, "bounds": [0, 10] }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "val": 0 },
            doc! { "val": 2_i64 },
            doc! { "val": 4 },
            doc! { "val": 6_i64 },
            doc! { "val": 8_i64 },
            doc! { "val": 9 },
        ]
    );
}

#[test]
fn densify_stage_supports_partition_bounds() {
    let results = run_pipeline_ok(
        vec![
            doc! { "val": 0, "part": "a" },
            doc! { "val": 2, "part": "a" },
            doc! { "val": 0, "part": "b" },
            doc! { "val": 2, "part": "b" },
        ],
        &[doc! {
            "$densify": {
                "field": "val",
                "partitionByFields": ["part"],
                "range": { "step": 1, "bounds": "partition" }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "val": 0, "part": "a" },
            doc! { "val": 1_i64, "part": "a" },
            doc! { "val": 2, "part": "a" },
            doc! { "val": 0, "part": "b" },
            doc! { "val": 1_i64, "part": "b" },
            doc! { "val": 2, "part": "b" },
        ]
    );
}

#[test]
fn densify_stage_supports_date_ranges() {
    let day0 = DateTime::from_millis(0);
    let day1 = DateTime::from_millis(86_400_000);
    let day2 = DateTime::from_millis(172_800_000);
    let day3 = DateTime::from_millis(259_200_000);

    let results = run_pipeline_ok(
        vec![doc! { "when": day0 }, doc! { "when": day3 }],
        &[doc! {
            "$densify": {
                "field": "when",
                "range": {
                    "step": 1,
                    "unit": "day",
                    "bounds": [Bson::DateTime(day0), Bson::DateTime(day3)]
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "when": day0 },
            doc! { "when": day1 },
            doc! { "when": day2 },
            doc! { "when": day3 },
        ]
    );
}

#[test]
fn densify_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$densify": { "field": "val", "range": { "step": 1 } } },
        doc! { "$densify": { "field": "$val", "range": { "step": 1, "bounds": "full" } } },
        doc! { "$densify": { "field": "val", "range": { "step": 1, "bounds": "partition" } } },
        doc! { "$densify": { "field": "val", "partitionByFields": ["val"], "range": { "step": 1, "bounds": "full" } } },
        doc! { "$densify": { "field": "val", "range": { "step": 1, "unit": "day", "bounds": [0, 10] } } },
        doc! { "$densify": { "field": "val", "range": { "step": 1.5, "unit": "day", "bounds": [Bson::DateTime(DateTime::from_millis(0)), Bson::DateTime(DateTime::from_millis(86_400_000))] } } },
    ] {
        let error = run_pipeline(vec![doc! { "val": 0 }], &[stage]).expect_err("invalid densify");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn fill_stage_supports_partitioned_locf_and_linear_methods() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "part": 1, "linear": 1, "other": 1 },
            doc! { "_id": 2, "part": 2, "linear": 1, "other": 1 },
            doc! { "_id": 3, "part": 1, "linear": Bson::Null, "other": Bson::Null },
            doc! { "_id": 4, "part": 2, "linear": Bson::Null, "other": Bson::Null },
            doc! { "_id": 5, "part": 1, "linear": 5, "other": 10 },
            doc! { "_id": 6, "part": 2, "linear": 6, "other": 2 },
            doc! { "_id": 7, "part": 1, "linear": Bson::Null, "other": Bson::Null },
            doc! { "_id": 8, "part": 2, "linear": 3, "other": 5 },
            doc! { "_id": 9, "part": 1, "linear": 7, "other": 15 },
            doc! { "_id": 10, "part": 2, "linear": Bson::Null, "other": Bson::Null },
        ],
        &[doc! {
            "$fill": {
                "sortBy": { "_id": 1 },
                "partitionBy": "$part",
                "output": {
                    "linear": { "method": "linear" },
                    "other": { "method": "locf" }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 1, "part": 1, "linear": 1, "other": 1 },
            doc! { "_id": 3, "part": 1, "linear": 3_i64, "other": 1 },
            doc! { "_id": 5, "part": 1, "linear": 5, "other": 10 },
            doc! { "_id": 7, "part": 1, "linear": 6_i64, "other": 10 },
            doc! { "_id": 9, "part": 1, "linear": 7, "other": 15 },
            doc! { "_id": 2, "part": 2, "linear": 1, "other": 1 },
            doc! { "_id": 4, "part": 2, "linear": 3.5, "other": 1 },
            doc! { "_id": 6, "part": 2, "linear": 6, "other": 2 },
            doc! { "_id": 8, "part": 2, "linear": 3, "other": 5 },
            doc! { "_id": 10, "part": 2, "linear": Bson::Null, "other": 5 },
        ]
    );
}

#[test]
fn fill_stage_supports_value_outputs_after_methods() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "part": 1, "other": Bson::Null, "linear": 1 },
            doc! { "_id": 2, "part": 1, "other": 10, "linear": Bson::Null },
        ],
        &[doc! {
            "$fill": {
                "sortBy": { "_id": 1 },
                "output": {
                    "linear": { "method": "locf" },
                    "other": { "value": "$_id" }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 1, "part": 1, "other": 1, "linear": 1 },
            doc! { "_id": 2, "part": 1, "other": 10, "linear": 1 },
        ]
    );
}

#[test]
fn fill_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$fill": {} },
        doc! { "$fill": { "output": {} } },
        doc! { "$fill": { "output": { "qty": "bad" } } },
        doc! { "$fill": { "sortBy": { "$bad": 1 }, "output": { "qty": { "value": 1 } } } },
        doc! { "$fill": { "partitionBy": "$part", "partitionByFields": ["part"], "output": { "qty": { "method": "locf" } } } },
        doc! { "$fill": { "output": { "qty": { "method": "linear" } } } },
    ] {
        let error =
            run_pipeline(vec![doc! { "_id": 1, "qty": 1 }], &[stage]).expect_err("invalid fill");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn set_window_fields_stage_supports_partitioned_document_windows() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 3, "team": "a", "seq": 2, "qty": 3 },
            doc! { "_id": 1, "team": "a", "seq": 0, "qty": 1 },
            doc! { "_id": 5, "team": "b", "seq": 1, "qty": 5 },
            doc! { "_id": 2, "team": "a", "seq": 1, "qty": 2 },
            doc! { "_id": 4, "team": "b", "seq": 0, "qty": 4 },
        ],
        &[doc! {
            "$setWindowFields": {
                "partitionBy": "$team",
                "sortBy": { "seq": 1 },
                "output": {
                    "runningQty": { "$sum": "$qty", "window": { "documents": ["unbounded", "current"] } },
                    "trailingQty": { "$sum": "$qty", "window": { "documents": [-1, "current"] } },
                    "docNo": { "$documentNumber": {} }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 1, "team": "a", "seq": 0, "qty": 1, "runningQty": 1_i64, "trailingQty": 1_i64, "docNo": 1_i64 },
            doc! { "_id": 2, "team": "a", "seq": 1, "qty": 2, "runningQty": 3_i64, "trailingQty": 3_i64, "docNo": 2_i64 },
            doc! { "_id": 3, "team": "a", "seq": 2, "qty": 3, "runningQty": 6_i64, "trailingQty": 5_i64, "docNo": 3_i64 },
            doc! { "_id": 4, "team": "b", "seq": 0, "qty": 4, "runningQty": 4_i64, "trailingQty": 4_i64, "docNo": 1_i64 },
            doc! { "_id": 5, "team": "b", "seq": 1, "qty": 5, "runningQty": 9_i64, "trailingQty": 9_i64, "docNo": 2_i64 },
        ]
    );
}

#[test]
fn set_window_fields_stage_supports_range_shift_and_rank_functions() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "score": 10, "label": "a" },
            doc! { "_id": 2, "score": 10, "label": "b" },
            doc! { "_id": 3, "score": 11, "label": "c" },
            doc! { "_id": 4, "score": 13, "label": "d" },
        ],
        &[doc! {
            "$setWindowFields": {
                "sortBy": { "score": 1 },
                "output": {
                    "nearbyCount": { "$count": {}, "window": { "range": [-1, 0] } },
                    "prevLabel": { "$shift": { "output": "$label", "by": -1, "default": "start" } },
                    "rank": { "$rank": {} },
                    "dense": { "$denseRank": {} }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 1, "score": 10, "label": "a", "nearbyCount": 2_i64, "prevLabel": "start", "rank": 1_i64, "dense": 1_i64 },
            doc! { "_id": 2, "score": 10, "label": "b", "nearbyCount": 2_i64, "prevLabel": "a", "rank": 1_i64, "dense": 1_i64 },
            doc! { "_id": 3, "score": 11, "label": "c", "nearbyCount": 3_i64, "prevLabel": "b", "rank": 3_i64, "dense": 2_i64 },
            doc! { "_id": 4, "score": 13, "label": "d", "nearbyCount": 1_i64, "prevLabel": "c", "rank": 4_i64, "dense": 3_i64 },
        ]
    );
}

#[test]
fn set_window_fields_stage_supports_locf_and_linear_fill() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 1, "seq": 1, "carry": 1, "interp": 1 },
            doc! { "_id": 2, "seq": 2, "carry": Bson::Null, "interp": Bson::Null },
            doc! { "_id": 3, "seq": 3, "carry": 2, "interp": 5 },
            doc! { "_id": 4, "seq": 4, "carry": Bson::Null, "interp": Bson::Null },
            doc! { "_id": 5, "seq": 5, "carry": 4, "interp": 9 },
        ],
        &[doc! {
            "$setWindowFields": {
                "sortBy": { "seq": 1 },
                "output": {
                    "carry": { "$locf": "$carry" },
                    "interp": { "$linearFill": "$interp" }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 1, "seq": 1, "carry": 1, "interp": 1_i64 },
            doc! { "_id": 2, "seq": 2, "carry": 1, "interp": 3_i64 },
            doc! { "_id": 3, "seq": 3, "carry": 2, "interp": 5_i64 },
            doc! { "_id": 4, "seq": 4, "carry": 2, "interp": 7_i64 },
            doc! { "_id": 5, "seq": 5, "carry": 4, "interp": 9_i64 },
        ]
    );
}

#[test]
fn set_window_fields_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$setWindowFields": 1 },
        doc! { "$setWindowFields": { "output": { "total": {} } } },
        doc! { "$setWindowFields": { "output": { "total": { "sum": "$qty" } } } },
        doc! { "$setWindowFields": { "output": { "rank": { "$rank": {} } } } },
        doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "shifted": { "$shift": { "output": "$qty" } } } } },
        doc! { "$setWindowFields": { "output": { "total": { "$sum": "$qty", "window": { "documents": ["unbounded", "current"] } } } } },
        doc! { "$setWindowFields": { "sortBy": { "seq": 1, "qty": 1 }, "output": { "counted": { "$count": {}, "window": { "range": [-1, 1] } } } } },
        doc! { "$setWindowFields": { "sortBy": { "seq": 1 }, "output": { "a": { "$sum": "$qty" }, "a.b": { "$sum": "$qty" } } } },
    ] {
        let error = run_pipeline(vec![doc! { "seq": 1, "qty": 1 }], &[stage])
            .expect_err("invalid setWindowFields");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn graph_lookup_stage_traverses_foreign_documents_with_depth_and_filters() {
    let resolver = StaticResolver::default().with_collection(
        "app",
        "foreign",
        vec![
            doc! { "name": "a", "neighbors": ["b", "c"], "kind": "keep" },
            doc! { "name": "b", "neighbors": ["d"], "kind": "skip" },
            doc! { "name": "c", "neighbors": ["d"], "kind": "keep" },
            doc! { "name": "d", "neighbors": [], "kind": "keep" },
        ],
    );

    let results = run_pipeline_with_static_resolver(
        vec![doc! { "start": "a" }],
        &[doc! {
            "$graphLookup": {
                "from": "foreign",
                "startWith": "$start",
                "connectFromField": "neighbors",
                "connectToField": "name",
                "depthField": "depth",
                "maxDepth": 2,
                "restrictSearchWithMatch": { "kind": "keep" },
                "as": "results"
            }
        }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![doc! {
            "start": "a",
            "results": [
                { "name": "a", "neighbors": ["b", "c"], "kind": "keep", "depth": 0_i64 },
                { "name": "c", "neighbors": ["d"], "kind": "keep", "depth": 1_i64 },
                { "name": "d", "neighbors": [], "kind": "keep", "depth": 2_i64 },
            ]
        }]
    );
}

#[test]
fn graph_lookup_stage_supports_outer_lookup_variables() {
    let resolver = StaticResolver::default()
        .with_collection("app", "local", vec![doc! {}])
        .with_collection(
            "app",
            "foreign",
            vec![doc! { "_id": 0, "from": "b", "to": "a" }],
        );

    let results = run_pipeline_with_static_resolver(
        vec![doc! { "seed": "a" }],
        &[doc! {
            "$lookup": {
                "from": "local",
                "let": { "start": "$seed" },
                "pipeline": [{
                    "$graphLookup": {
                        "from": "foreign",
                        "startWith": "$$start",
                        "connectFromField": "from",
                        "connectToField": "to",
                        "as": "matches"
                    }
                }],
                "as": "lookup"
            }
        }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![doc! {
            "seed": "a",
            "lookup": [{
                "matches": [{ "_id": 0, "from": "b", "to": "a" }]
            }]
        }]
    );
}

#[test]
fn graph_lookup_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$graphLookup": 1 },
        doc! { "$graphLookup": { "from": "foreign", "connectFromField": "neighbors", "connectToField": "name", "as": "results" } },
        doc! { "$graphLookup": { "from": 1, "startWith": "$seed", "connectFromField": "neighbors", "connectToField": "name", "as": "results" } },
        doc! { "$graphLookup": { "from": "foreign", "startWith": "$seed", "connectFromField": "$neighbors", "connectToField": "name", "as": "results" } },
        doc! { "$graphLookup": { "from": "foreign", "startWith": "$seed", "connectFromField": "neighbors", "connectToField": "name", "as": "$results" } },
        doc! { "$graphLookup": { "from": "foreign", "startWith": "$seed", "connectFromField": "neighbors", "connectToField": "name", "as": "results", "maxDepth": -1 } },
        doc! { "$graphLookup": { "from": "foreign", "startWith": "$seed", "connectFromField": "neighbors", "connectToField": "name", "as": "results", "restrictSearchWithMatch": 1 } },
    ] {
        let error =
            run_pipeline(vec![doc! { "seed": "a" }], &[stage]).expect_err("invalid graphLookup");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn geo_near_stage_filters_and_sorts_by_distance() {
    let results = run_pipeline_ok(
        vec![
            doc! { "name": "zero", "kind": "keep", "loc": [0.0, 0.0] },
            doc! { "name": "far", "kind": "skip", "loc": [5.0, 0.0] },
            doc! { "name": "near", "kind": "keep", "loc": [1.0, 0.0] },
        ],
        &[doc! {
            "$geoNear": {
                "near": [0.0, 0.0],
                "key": "loc",
                "distanceField": "dist",
                "includeLocs": "matchedLoc",
                "maxDistance": 2.0,
                "query": { "kind": "keep" }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "name": "zero", "kind": "keep", "loc": [0.0, 0.0], "dist": 0.0, "matchedLoc": [0.0, 0.0] },
            doc! { "name": "near", "kind": "keep", "loc": [1.0, 0.0], "dist": 1.0, "matchedLoc": [1.0, 0.0] },
        ]
    );
}

#[test]
fn geo_near_stage_supports_spherical_geojson_points() {
    let results = run_pipeline_ok(
        vec![doc! {
            "name": "north",
            "loc": { "type": "Point", "coordinates": [0.0, 1.0] }
        }],
        &[doc! {
            "$geoNear": {
                "near": { "type": "Point", "coordinates": [0.0, 0.0] },
                "key": "loc",
                "distanceField": "dist",
                "spherical": true
            }
        }],
    );

    let distance = results[0].get_f64("dist").expect("distance");
    assert!((distance - 111_319.49).abs() < 10.0);
}

#[test]
fn geo_near_stage_rejects_invalid_specs() {
    for pipeline in [
        vec![doc! { "$geoNear": { "key": "loc", "distanceField": "dist" } }],
        vec![
            doc! { "$project": { "loc": 1 } },
            doc! { "$geoNear": { "near": [0.0, 0.0], "key": "loc", "distanceField": "dist" } },
        ],
        vec![doc! { "$geoNear": { "near": [0.0, 0.0], "key": 1, "distanceField": "dist" } }],
        vec![doc! { "$geoNear": { "near": [0.0, 0.0], "key": "loc", "distanceMultiplier": -1.0 } }],
    ] {
        let error =
            run_pipeline(vec![doc! { "loc": [0.0, 0.0] }], &pipeline).expect_err("invalid geoNear");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn documents_stage_replaces_input_when_first() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 0, "ignored": true }],
        &[doc! { "$documents": [{ "a": 1 }, { "a": 2 }] }],
    );

    assert_eq!(results, vec![doc! { "a": 1 }, doc! { "a": 2 }]);
}

#[test]
fn bucket_stage_groups_documents_by_boundaries() {
    let results = run_pipeline_ok(
        vec![
            doc! { "price": 10, "qty": 1 },
            doc! { "price": 20, "qty": 2 },
            doc! { "price": 40, "qty": 3 },
        ],
        &[doc! {
            "$bucket": {
                "groupBy": "$price",
                "boundaries": [0, 20, 50],
                "output": {
                    "totalQty": { "$sum": "$qty" }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 0, "totalQty": 1_i64 },
            doc! { "_id": 20, "totalQty": 5_i64 },
        ]
    );
}

#[test]
fn bucket_auto_stage_groups_documents_and_preserves_incoming_order_with_push() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": 0, "n": 9 },
            doc! { "_id": 1, "n": 8 },
            doc! { "_id": 2, "n": 7 },
            doc! { "_id": 3, "n": 6 },
            doc! { "_id": 4, "n": 5 },
            doc! { "_id": 5, "n": 4 },
            doc! { "_id": 6, "n": 3 },
            doc! { "_id": 7, "n": 2 },
            doc! { "_id": 8, "n": 1 },
            doc! { "_id": 9, "n": 0 },
        ],
        &[doc! {
            "$bucketAuto": {
                "groupBy": "$n",
                "buckets": 2,
                "output": {
                    "docs": { "$push": "$$ROOT" }
                }
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! {
                "_id": { "min": 0, "max": 5 },
                "docs": [
                    { "_id": 5, "n": 4 },
                    { "_id": 6, "n": 3 },
                    { "_id": 7, "n": 2 },
                    { "_id": 8, "n": 1 },
                    { "_id": 9, "n": 0 },
                ]
            },
            doc! {
                "_id": { "min": 5, "max": 9 },
                "docs": [
                    { "_id": 0, "n": 9 },
                    { "_id": 1, "n": 8 },
                    { "_id": 2, "n": 7 },
                    { "_id": 3, "n": 6 },
                    { "_id": 4, "n": 5 },
                ]
            },
        ]
    );
}

#[test]
fn bucket_auto_stage_defaults_to_count_output() {
    let results = run_pipeline_ok(
        vec![
            doc! { "price": 10 },
            doc! { "price": 20 },
            doc! { "price": 30 },
            doc! { "price": 40 },
        ],
        &[doc! {
            "$bucketAuto": {
                "groupBy": "$price",
                "buckets": 2
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": { "min": 10, "max": 30 }, "count": 2_i64 },
            doc! { "_id": { "min": 30, "max": 40 }, "count": 2_i64 },
        ]
    );
}

#[test]
fn bucket_auto_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$bucketAuto": 1 },
        doc! { "$bucketAuto": {} },
        doc! { "$bucketAuto": { "groupBy": "price", "buckets": 2 } },
        doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 0 } },
        doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 2.5 } },
        doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 2, "output": 1 } },
        doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 2, "granularity": "R5" } },
        doc! { "$bucketAuto": { "groupBy": "$price", "buckets": 2, "unknown": true } },
    ] {
        let error =
            run_pipeline(vec![doc! { "price": 10 }], &[stage]).expect_err("invalid bucketAuto");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn out_stage_must_be_last_and_accepts_string_or_namespace_object_specs() {
    let string_results = run_pipeline_ok(
        vec![doc! { "_id": 1, "value": 1 }],
        &[doc! { "$out": "archive" }],
    );
    assert!(string_results.is_empty());

    let object_results = run_pipeline_ok(
        vec![doc! { "_id": 1, "value": 1 }],
        &[doc! { "$out": { "db": "analytics", "coll": "archive" } }],
    );
    assert!(object_results.is_empty());

    let error = run_pipeline(
        vec![doc! { "_id": 1, "value": 1 }],
        &[
            doc! { "$out": "archive" },
            doc! { "$match": { "value": 1 } },
        ],
    )
    .expect_err("$out must be last");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn out_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$out": 1 },
        doc! { "$out": {} },
        doc! { "$out": { "db": "analytics" } },
        doc! { "$out": { "coll": 1 } },
        doc! { "$out": { "db": 1, "coll": "archive" } },
        doc! { "$out": { "db": "analytics", "coll": "archive", "timeseries": {} } },
        doc! { "$out": { "db": "analytics", "coll": "archive", "unknown": true } },
    ] {
        let error = run_pipeline(vec![doc! { "_id": 1 }], &[stage]).expect_err("invalid $out");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn merge_stage_must_be_last_and_accepts_supported_mode_combinations() {
    let string_results = run_pipeline_ok(
        vec![doc! { "_id": 1, "value": 1 }],
        &[doc! { "$merge": "archive" }],
    );
    assert!(string_results.is_empty());

    let object_results = run_pipeline_ok(
        vec![doc! { "_id": 1, "value": 1 }],
        &[doc! {
            "$merge": {
                "into": { "db": "analytics", "coll": "archive" },
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }],
    );
    assert!(object_results.is_empty());

    let error = run_pipeline(
        vec![doc! { "_id": 1, "value": 1 }],
        &[
            doc! { "$merge": "archive" },
            doc! { "$match": { "value": 1 } },
        ],
    )
    .expect_err("$merge must be last");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn merge_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$merge": 1 },
        doc! { "$merge": {} },
        doc! { "$merge": { "into": { "db": "analytics" } } },
        doc! { "$merge": { "into": { "coll": 1 } } },
        doc! { "$merge": { "into": "archive", "on": [] } },
        doc! { "$merge": { "into": "archive", "on": [1] } },
        doc! { "$merge": { "into": "archive", "whenMatched": "keepExisting", "whenNotMatched": "discard" } },
        doc! { "$merge": { "into": "archive", "whenMatched": [] } },
        doc! { "$merge": { "into": "archive", "let": { "x": 1 } } },
        doc! { "$merge": { "into": "archive", "unknown": true } },
    ] {
        let error = run_pipeline(vec![doc! { "_id": 1 }], &[stage]).expect_err("invalid $merge");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn union_with_stage_appends_documents_from_another_collection() {
    let resolver = StaticResolver::default().with_collection(
        "app",
        "union",
        vec![doc! { "_id": "u1" }, doc! { "_id": "u2" }],
    );
    let results = run_pipeline_with_static_resolver(
        vec![doc! { "_id": "base" }],
        &[doc! { "$unionWith": "union" }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": "base" },
            doc! { "_id": "u1" },
            doc! { "_id": "u2" }
        ]
    );
}

#[test]
fn union_with_stage_supports_nested_subpipelines_and_collectionless_documents() {
    let resolver =
        StaticResolver::default().with_collection("app", "union", vec![doc! { "_id": "u1" }]);
    let results = run_pipeline_with_static_resolver(
        vec![doc! { "_id": "base" }],
        &[doc! {
            "$unionWith": {
                "coll": "union",
                "pipeline": [
                    { "$set": { "source": { "$literal": "union" } } },
                    { "$unionWith": { "pipeline": [{ "$documents": [{ "_id": "inline", "source": "inline" }] }] } }
                ]
            }
        }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": "base" },
            doc! { "_id": "u1", "source": "union" },
            doc! { "_id": "inline", "source": "inline" }
        ]
    );
}

#[test]
fn union_with_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$unionWith": 1 },
        doc! { "$unionWith": {} },
        doc! { "$unionWith": { "db": "app" } },
        doc! { "$unionWith": { "coll": 1 } },
        doc! { "$unionWith": { "pipeline": 1 } },
        doc! { "$unionWith": { "pipeline": [{ "$match": {} }] } },
        doc! { "$unionWith": { "coll": "union", "unknown": true } },
    ] {
        let error =
            run_pipeline(vec![doc! { "_id": "base" }], &[stage]).expect_err("invalid unionWith");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn lookup_stage_matches_local_and_foreign_fields_with_pipeline_and_let_variables() {
    let resolver = StaticResolver::default().with_collection(
        "app",
        "locations",
        vec![
            doc! {
                "_id": "doghouse",
                "coordinates": [25.0, 60.0],
                "extra": { "breeds": ["terrier", "dachshund", "bulldog"] }
            },
            doc! {
                "_id": "bullpen",
                "coordinates": [-25.0, -60.0],
                "extra": { "breeds": "Scottish Highland", "feeling": "bullish" }
            },
            doc! {
                "_id": "puppyhouse",
                "coordinates": [-25.0, 60.0],
                "extra": { "breeds": 1, "feeling": ["cute", "small"] }
            },
        ],
    );
    let results = run_pipeline_with_static_resolver(
        vec![
            doc! { "_id": "dog", "locationId": "doghouse" },
            doc! { "_id": "bull", "locationId": "bullpen" },
            doc! { "_id": "puppy", "locationId": "puppyhouse", "breed": 1 },
        ],
        &[doc! {
            "$lookup": {
                "from": "locations",
                "localField": "locationId",
                "foreignField": "_id",
                "as": "location",
                "let": { "animal_breed": "$breed" },
                "pipeline": [
                    { "$match": { "$expr": { "$eq": ["$$animal_breed", "$extra.breeds"] } } }
                ]
            }
        }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": "dog", "locationId": "doghouse", "location": [] },
            doc! { "_id": "bull", "locationId": "bullpen", "location": [] },
            doc! {
                "_id": "puppy",
                "locationId": "puppyhouse",
                "breed": 1,
                "location": [{
                    "_id": "puppyhouse",
                    "coordinates": [-25.0, 60.0],
                    "extra": { "breeds": 1, "feeling": ["cute", "small"] }
                }]
            },
        ]
    );
}

#[test]
fn lookup_stage_supports_collectionless_documents_pipeline_with_join_fields() {
    let results = run_pipeline_ok(
        vec![
            doc! { "_id": "a", "wanted": 2 },
            doc! { "_id": "b", "wanted": 3 },
        ],
        &[doc! {
            "$lookup": {
                "localField": "wanted",
                "foreignField": "x",
                "as": "matches",
                "pipeline": [
                    { "$documents": [{ "x": 1, "label": "one" }, { "x": 2, "label": "two" }, { "x": 3, "label": "three" }] },
                    { "$project": { "_id": 0, "x": 1, "label": 1 } }
                ]
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": "a", "wanted": 2, "matches": [{ "x": 2, "label": "two" }] },
            doc! { "_id": "b", "wanted": 3, "matches": [{ "x": 3, "label": "three" }] },
        ]
    );
}

#[test]
fn lookup_stage_exposes_outer_let_variables_to_nested_lookup_pipelines() {
    let resolver = StaticResolver::default()
        .with_collection(
            "app",
            "locations",
            vec![doc! { "locId": "north", "regionId": "r1" }],
        )
        .with_collection(
            "app",
            "regions",
            vec![
                doc! { "regionId": "r1", "climate": "cold" },
                doc! { "regionId": "r1", "climate": "warm" },
            ],
        );
    let results = run_pipeline_with_static_resolver(
        vec![doc! { "_id": "fox", "loc": "north", "wantedClimate": "cold" }],
        &[doc! {
            "$lookup": {
                "from": "locations",
                "localField": "loc",
                "foreignField": "locId",
                "as": "matches",
                "let": { "wantedClimate": "$wantedClimate" },
                "pipeline": [
                    {
                        "$lookup": {
                            "from": "regions",
                            "localField": "regionId",
                            "foreignField": "regionId",
                            "as": "regionMatches",
                            "pipeline": [
                                { "$match": { "$expr": { "$eq": ["$$wantedClimate", "$climate"] } } },
                                { "$project": { "_id": 0, "climate": 1 } }
                            ]
                        }
                    },
                    { "$project": { "_id": 0, "locId": 1, "regionMatches": 1 } }
                ]
            }
        }],
        &resolver,
    );

    assert_eq!(
        results,
        vec![doc! {
            "_id": "fox",
            "loc": "north",
            "wantedClimate": "cold",
            "matches": [{
                "locId": "north",
                "regionMatches": [{ "climate": "cold" }]
            }]
        }]
    );
}

#[test]
fn lookup_stage_returns_empty_arrays_for_missing_foreign_collections() {
    let results = run_pipeline_with_static_resolver(
        vec![doc! { "_id": "fox", "loc": "north" }],
        &[doc! {
            "$lookup": {
                "from": "missing",
                "localField": "loc",
                "foreignField": "locId",
                "as": "matches"
            }
        }],
        &StaticResolver::default(),
    );

    assert_eq!(
        results,
        vec![doc! { "_id": "fox", "loc": "north", "matches": [] }]
    );
}

#[test]
fn lookup_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$lookup": 1 },
        doc! { "$lookup": {} },
        doc! { "$lookup": { "from": "other" } },
        doc! { "$lookup": { "from": "other", "as": "joined" } },
        doc! { "$lookup": { "from": "other", "as": "joined", "localField": "a" } },
        doc! { "$lookup": { "from": "other", "as": "joined", "foreignField": "b" } },
        doc! { "$lookup": { "from": "other", "as": "joined", "localField": "a", "foreignField": "b", "let": { "v": "$a" } } },
        doc! { "$lookup": { "pipeline": [{ "$match": {} }], "as": "joined" } },
        doc! { "$lookup": { "from": { "db": "app" }, "pipeline": [{ "$documents": [{ "x": 1 }] }], "as": "joined" } },
        doc! { "$lookup": { "from": "other", "as": "joined", "pipeline": 1 } },
        doc! { "$lookup": { "from": "other", "as": "joined", "pipeline": [], "unknown": true } },
    ] {
        let error =
            run_pipeline(vec![doc! { "_id": "base" }], &[stage]).expect_err("invalid lookup");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn bucket_stage_supports_default_bucket_and_default_count_output() {
    let results = run_pipeline_ok(
        vec![doc! { "price": 10 }, doc! { "price": 120 }],
        &[doc! {
            "$bucket": {
                "groupBy": "$price",
                "boundaries": [0, 50, 100],
                "default": "other"
            }
        }],
    );

    assert_eq!(
        results,
        vec![
            doc! { "_id": 0, "count": 1_i64 },
            doc! { "_id": "other", "count": 1_i64 },
        ]
    );
}

#[test]
fn bucket_stage_rejects_invalid_specs_and_out_of_range_values_without_default() {
    for stage in [
        doc! { "$bucket": 1 },
        doc! { "$bucket": {} },
        doc! { "$bucket": { "groupBy": "price", "boundaries": [0, 10] } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [0] } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [10, 0] } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, "10"] } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 10], "default": 5 } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 10], "output": 1 } },
        doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 10], "unknown": true } },
    ] {
        let error = run_pipeline(vec![doc! { "price": 5 }], &[stage]).expect_err("invalid bucket");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "price": 20 }],
        &[doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 10] } }],
    )
    .expect_err("out-of-range bucket value");
    assert!(matches!(error, QueryError::InvalidArgument(_)));
}

#[test]
fn documents_stage_must_be_first() {
    let error = run_pipeline(
        vec![doc! { "_id": 0 }],
        &[
            doc! { "$project": { "_id": 1 } },
            doc! { "$documents": [{ "a": 1 }] },
        ],
    )
    .expect_err("$documents should only be allowed as the first stage");

    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn documents_stage_rejects_non_array_specs_and_non_document_elements() {
    let scalar_error =
        run_pipeline(Vec::new(), &[doc! { "$documents": "not-an-array" }]).expect_err("invalid");
    assert!(matches!(scalar_error, QueryError::InvalidStage));

    let element_error =
        run_pipeline(Vec::new(), &[doc! { "$documents": [{ "a": 1 }, 2] }]).expect_err("invalid");
    assert!(matches!(element_error, QueryError::ExpectedDocument));
}

#[test]
fn facet_stage_runs_multiple_subpipelines_and_emits_one_document() {
    let results = run_pipeline_ok(
        vec![
            doc! { "team": "red", "qty": 1 },
            doc! { "team": "blue", "qty": 3 },
            doc! { "team": "blue", "qty": 2 },
        ],
        &[doc! {
            "$facet": {
                "totals": [
                    { "$sortByCount": "$team" }
                ],
                "topQty": [
                    { "$sort": { "qty": -1 } },
                    { "$limit": 1 },
                    { "$project": { "_id": 0, "qty": 1 } }
                ]
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! {
            "totals": [
                { "_id": "blue", "count": 2_i64 },
                { "_id": "red", "count": 1_i64 }
            ],
            "topQty": [
                { "qty": 3 }
            ]
        }]
    );
}

#[test]
fn facet_stage_runs_against_empty_input() {
    let results = run_pipeline_ok(
        Vec::new(),
        &[doc! {
            "$facet": {
                "counted": [
                    { "$count": "total" }
                ]
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! {
            "counted": [
                { "total": 0_i64 }
            ]
        }]
    );
}

#[test]
fn facet_stage_rejects_invalid_specs_and_disallowed_substages() {
    for stage in [
        doc! { "$facet": 1 },
        doc! { "$facet": {} },
        doc! { "$facet": { "$bad": [] } },
        doc! { "$facet": { "bad.name": [] } },
        doc! { "$facet": { "values": 1 } },
        doc! { "$facet": { "values": [1] } },
        doc! { "$facet": { "values": [{ "$documents": [{ "a": 1 }] }] } },
        doc! { "$facet": { "values": [{ "$facet": { "nested": [] } }] } },
    ] {
        let error = run_pipeline(vec![doc! { "team": "red" }], &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }
}

#[test]
fn current_op_stage_emits_a_synthetic_inflight_operation() {
    let results = run_pipeline_ok(
        Vec::new(),
        &[
            doc! { "$currentOp": { "localOps": true } },
            doc! { "$project": { "_id": 0, "ns": 1, "type": 1 } },
        ],
    );

    assert_eq!(
        results,
        vec![doc! {
            "ns": "admin.$cmd.aggregate",
            "type": "op",
        }]
    );
}

#[test]
fn current_op_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$currentOp": 1 },
        doc! { "$currentOp": {} },
        doc! { "$currentOp": { "localOps": false } },
        doc! { "$currentOp": { "localOps": "yes" } },
        doc! { "$currentOp": { "localOps": true, "allUsers": false } },
        doc! { "$currentOp": { "idleConnections": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid $currentOp");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        Vec::new(),
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$currentOp": { "localOps": true } },
        ],
    )
    .expect_err("$currentOp should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn coll_stats_stage_reports_count_and_storage_stats() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 1, "qty": 12 }, doc! { "_id": 2, "qty": 34 }],
        &[doc! {
            "$collStats": {
                "count": {},
                "storageStats": { "scale": 1, "verbose": false }
            }
        }],
    );

    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.get_str("ns").expect("ns"), "app.synthetic");
    assert_eq!(result.get_i64("count").expect("count"), 2);
    let storage_stats = result.get_document("storageStats").expect("storageStats");
    assert_eq!(storage_stats.get_i64("count").expect("count"), 2);
    assert_eq!(storage_stats.get_i64("nindexes").expect("nindexes"), 1);
    assert!(storage_stats.get_i64("size").expect("size") > 0);
}

#[test]
fn coll_stats_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$collStats": 1 },
        doc! { "$collStats": { "count": 1 } },
        doc! { "$collStats": { "count": { "bad": true } } },
        doc! { "$collStats": { "storageStats": 1 } },
        doc! { "$collStats": { "storageStats": { "scale": 0 } } },
        doc! { "$collStats": { "latencyStats": {} } },
        doc! { "$collStats": { "queryExecStats": {} } },
    ] {
        let error = run_pipeline(vec![doc! { "_id": 1 }], &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$collStats": { "count": {} } },
        ],
    )
    .expect_err("$collStats should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn index_stats_stage_reports_synthetic_index_metadata() {
    let results = run_pipeline_ok(Vec::new(), &[doc! { "$indexStats": {} }]);

    assert_eq!(
        results,
        vec![doc! {
            "name": "_id_",
            "key": { "_id": 1 },
            "spec": { "name": "_id_", "key": { "_id": 1 }, "unique": true },
            "accesses": { "ops": 0_i64, "since": DateTime::from_millis(0) },
            "host": "mqlite",
        }]
    );
}

#[test]
fn index_stats_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$indexStats": 1 },
        doc! { "$indexStats": { "verbose": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[doc! { "$match": { "_id": 1 } }, doc! { "$indexStats": {} }],
    )
    .expect_err("$indexStats should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn plan_cache_stats_stage_reports_synthetic_cache_entries() {
    let results = run_pipeline_ok(Vec::new(), &[doc! { "$planCacheStats": {} }]);

    assert_eq!(
        results,
        vec![doc! {
            "namespace": "app.synthetic",
            "filterShape": "{\"qty\":{\"$gte\":\"?\"}}",
            "sortShape": "{}",
            "projectionShape": "{}",
            "sequence": 1_i64,
            "cachedPlan": { "type": "collectionScan" },
            "host": "mqlite",
        }]
    );
}

#[test]
fn plan_cache_stats_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$planCacheStats": 1 },
        doc! { "$planCacheStats": { "allHosts": true } },
        doc! { "$planCacheStats": { "unknown": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$planCacheStats": {} },
        ],
    )
    .expect_err("$planCacheStats should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_catalog_stage_reports_synthetic_catalog_entries() {
    let results = run_pipeline_ok(Vec::new(), &[doc! { "$listCatalog": {} }]);

    assert_eq!(
        results,
        vec![doc! {
            "db": "app",
            "ns": "app.synthetic",
            "name": "synthetic",
            "type": "collection",
            "options": {},
            "indexCount": 1_i64,
        }]
    );
}

#[test]
fn list_catalog_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listCatalog": 1 },
        doc! { "$listCatalog": { "all": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[doc! { "$match": { "_id": 1 } }, doc! { "$listCatalog": {} }],
    )
    .expect_err("$listCatalog should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_cluster_catalog_stage_reports_synthetic_catalog_entries() {
    let results = run_pipeline_ok(
        Vec::new(),
        &[doc! {
            "$listClusterCatalog": {
                "shards": true,
                "tracked": true,
                "balancingConfiguration": true,
            }
        }],
    );

    assert_eq!(
        results,
        vec![doc! {
            "db": "app",
            "ns": "app.synthetic",
            "type": "collection",
            "options": {},
            "info": { "readOnly": false },
            "idIndex": { "name": "_id_", "key": { "_id": 1 }, "unique": true },
            "sharded": false,
            "tracked": false,
            "shards": [],
        }]
    );
}

#[test]
fn list_cluster_catalog_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listClusterCatalog": 1 },
        doc! { "$listClusterCatalog": { "shards": 1 } },
        doc! { "$listClusterCatalog": { "tracked": "yes" } },
        doc! { "$listClusterCatalog": { "balancingConfiguration": 1 } },
        doc! { "$listClusterCatalog": { "unknown": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$listClusterCatalog": {} },
        ],
    )
    .expect_err("$listClusterCatalog should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_cached_and_active_users_stage_returns_no_results_without_auth() {
    let results = run_pipeline_ok(
        vec![doc! { "_id": 1 }],
        &[doc! { "$listCachedAndActiveUsers": {} }],
    );

    assert!(results.is_empty());
}

#[test]
fn list_cached_and_active_users_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listCachedAndActiveUsers": 1 },
        doc! { "$listCachedAndActiveUsers": { "all": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$listCachedAndActiveUsers": {} },
        ],
    )
    .expect_err("$listCachedAndActiveUsers should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_local_sessions_stage_accepts_public_specs() {
    for stage in [
        doc! { "$listLocalSessions": {} },
        doc! { "$listLocalSessions": { "allUsers": false } },
        doc! { "$listLocalSessions": { "allUsers": true } },
        doc! {
            "$listLocalSessions": {
                "users": [{ "user": "alice", "db": "admin" }]
            }
        },
    ] {
        let results = run_pipeline_ok(Vec::new(), &[stage]);
        assert!(results.is_empty());
    }
}

#[test]
fn list_local_sessions_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listLocalSessions": 1 },
        doc! { "$listLocalSessions": { "allUsers": "yes" } },
        doc! { "$listLocalSessions": { "users": "alice" } },
        doc! { "$listLocalSessions": { "users": ["alice"] } },
        doc! { "$listLocalSessions": { "users": [{ "user": "alice" }] } },
        doc! {
            "$listLocalSessions": {
                "allUsers": true,
                "users": [{ "user": "alice", "db": "admin" }]
            }
        },
        doc! { "$listLocalSessions": { "$_internalPredicate": {} } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$listLocalSessions": {} },
        ],
    )
    .expect_err("$listLocalSessions should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_sessions_stage_accepts_public_specs() {
    for stage in [
        doc! { "$listSessions": {} },
        doc! { "$listSessions": { "allUsers": false } },
        doc! { "$listSessions": { "allUsers": true } },
        doc! {
            "$listSessions": {
                "users": [{ "user": "alice", "db": "admin" }]
            }
        },
    ] {
        let results = run_pipeline_ok(Vec::new(), &[stage]);
        assert!(results.is_empty());
    }
}

#[test]
fn list_sessions_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listSessions": 1 },
        doc! { "$listSessions": { "allUsers": "yes" } },
        doc! { "$listSessions": { "users": "alice" } },
        doc! { "$listSessions": { "users": ["alice"] } },
        doc! { "$listSessions": { "users": [{ "user": "alice" }] } },
        doc! {
            "$listSessions": {
                "allUsers": true,
                "users": [{ "user": "alice", "db": "admin" }]
            }
        },
        doc! { "$listSessions": { "$_internalPredicate": {} } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$listSessions": {} },
        ],
    )
    .expect_err("$listSessions should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_sampled_queries_stage_accepts_public_specs() {
    for stage in [
        doc! { "$listSampledQueries": {} },
        doc! { "$listSampledQueries": { "namespace": "app.widgets" } },
    ] {
        let results = run_pipeline_ok(Vec::new(), &[stage]);
        assert!(results.is_empty());
    }
}

#[test]
fn list_sampled_queries_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listSampledQueries": 1 },
        doc! { "$listSampledQueries": { "namespace": 1 } },
        doc! { "$listSampledQueries": { "namespace": "invalid" } },
        doc! { "$listSampledQueries": { "namespace": "app." } },
        doc! { "$listSampledQueries": { "namespace": "app\0.widgets" } },
        doc! { "$listSampledQueries": { "all": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$listSampledQueries": {} },
        ],
    )
    .expect_err("$listSampledQueries should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_search_indexes_stage_accepts_public_specs() {
    for stage in [
        doc! { "$listSearchIndexes": {} },
        doc! { "$listSearchIndexes": { "name": "search-index" } },
        doc! { "$listSearchIndexes": { "id": "index-id" } },
    ] {
        let results = run_pipeline_ok(Vec::new(), &[stage]);
        assert!(results.is_empty());
    }
}

#[test]
fn list_search_indexes_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listSearchIndexes": 1 },
        doc! { "$listSearchIndexes": { "name": 1 } },
        doc! { "$listSearchIndexes": { "id": 1 } },
        doc! { "$listSearchIndexes": { "unknown": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        Vec::new(),
        &[doc! { "$listSearchIndexes": { "name": "search-index", "id": "index-id" } }],
    )
    .expect_err("name and id cannot both be set");
    assert!(matches!(error, QueryError::InvalidArgument(_)));

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$listSearchIndexes": {} },
        ],
    )
    .expect_err("$listSearchIndexes should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn query_settings_stage_accepts_public_specs() {
    for stage in [
        doc! { "$querySettings": {} },
        doc! { "$querySettings": { "showDebugQueryShape": false } },
        doc! { "$querySettings": { "showDebugQueryShape": true } },
    ] {
        let results = run_pipeline_ok(Vec::new(), &[stage]);
        assert!(results.is_empty());
    }
}

#[test]
fn query_settings_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$querySettings": 1 },
        doc! { "$querySettings": { "showDebugQueryShape": 1 } },
        doc! { "$querySettings": { "all": true } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$querySettings": {} },
        ],
    )
    .expect_err("$querySettings should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn list_mql_entities_stage_reports_supported_aggregation_stages() {
    let results = run_pipeline_ok(
        Vec::new(),
        &[doc! { "$listMqlEntities": { "entityType": "aggregationStages" } }],
    );

    assert!(results.iter().any(|document| {
        document
            .get_str("name")
            .map(|name| name == "$match")
            .unwrap_or(false)
    }));
    assert!(results.windows(2).all(|pair| {
        pair[0].get_str("name").expect("name") <= pair[1].get_str("name").expect("name")
    }));
}

#[test]
fn list_mql_entities_stage_rejects_invalid_specs() {
    for stage in [
        doc! { "$listMqlEntities": "" },
        doc! { "$listMqlEntities": {} },
        doc! { "$listMqlEntities": { "improperField": "aggregationStages" } },
        doc! { "$listMqlEntities": { "entityType": "improperValue" } },
    ] {
        let error = run_pipeline(Vec::new(), &[stage]).expect_err("invalid");
        assert!(matches!(error, QueryError::InvalidStage));
    }

    let error = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$documents": [{ "_id": 1 }] },
            doc! { "$listMqlEntities": { "entityType": "aggregationStages" } },
        ],
    )
    .expect_err("$listMqlEntities should only be valid as the first stage");
    assert!(matches!(error, QueryError::InvalidStage));
}

#[test]
fn replace_root_errors_when_new_root_is_not_a_document() {
    let error = run_pipeline(
        vec![doc! { "value": 5 }],
        &[doc! { "$replaceRoot": { "newRoot": "$value" } }],
    )
    .expect_err("replaceRoot should reject scalars");

    assert!(matches!(error, QueryError::ExpectedDocument));
}

#[test]
fn change_stream_stage_materializes_insert_update_delete_and_expanded_events() {
    let resolver = StaticResolver::default().with_change_events(vec![
        change_event(
            1,
            "app",
            Some("widgets"),
            "insert",
            Some(doc! { "_id": 1 }),
            Some(doc! { "_id": 1, "qty": 1 }),
            None,
            None,
            false,
            Document::new(),
        ),
        change_event(
            2,
            "app",
            Some("widgets"),
            "update",
            Some(doc! { "_id": 1 }),
            Some(doc! { "_id": 1, "qty": 2 }),
            Some(doc! { "_id": 1, "qty": 1 }),
            Some(doc! { "updatedFields": { "qty": 2 }, "removedFields": [] }),
            false,
            Document::new(),
        ),
        change_event(
            3,
            "app",
            Some("widgets"),
            "createIndexes",
            None,
            None,
            None,
            None,
            true,
            doc! { "operationDescription": { "indexes": [{ "name": "qty_1" }] } },
        ),
        change_event(
            4,
            "app",
            Some("widgets"),
            "delete",
            Some(doc! { "_id": 1 }),
            None,
            Some(doc! { "_id": 1, "qty": 2 }),
            None,
            false,
            Document::new(),
        ),
    ]);

    let results = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "fullDocument": "updateLookup", "fullDocumentBeforeChange": "whenAvailable", "showExpandedEvents": true } }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("change stream");

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get_str("operationType").expect("type"), "insert");
    assert_eq!(
        results[1]
            .get_document("fullDocument")
            .expect("fullDocument")
            .get_i32("qty")
            .expect("qty"),
        2
    );
    assert_eq!(
        results[1]
            .get_document("fullDocumentBeforeChange")
            .expect("fullDocumentBeforeChange")
            .get_i32("qty")
            .expect("qty"),
        1
    );
    assert_eq!(
        results[2].get_str("operationType").expect("type"),
        "createIndexes"
    );
    assert_eq!(
        results[3]
            .get_document("fullDocumentBeforeChange")
            .expect("fullDocumentBeforeChange")
            .get_i32("qty")
            .expect("qty"),
        2
    );
}

#[test]
fn change_stream_stage_supports_resume_after_and_start_at_operation_time() {
    let resolver = StaticResolver::default().with_change_events(vec![
        change_event(
            1,
            "app",
            Some("widgets"),
            "insert",
            Some(doc! { "_id": 1 }),
            Some(doc! { "_id": 1 }),
            None,
            None,
            false,
            Document::new(),
        ),
        change_event(
            2,
            "app",
            Some("widgets"),
            "insert",
            Some(doc! { "_id": 2 }),
            Some(doc! { "_id": 2 }),
            None,
            None,
            false,
            Document::new(),
        ),
    ]);

    let resumed = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "resumeAfter": { "sequence": 1 } } }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("resumed change stream");
    assert_eq!(resumed.len(), 1);
    assert_eq!(
        resumed[0]
            .get_document("documentKey")
            .expect("documentKey")
            .get_i32("_id")
            .expect("_id"),
        2
    );

    let started = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "startAtOperationTime": Timestamp { time: 2, increment: 0 } } }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("startAtOperationTime change stream");
    assert_eq!(started.len(), 1);
    assert_eq!(started[0].get_str("operationType").expect("type"), "insert");
}

#[test]
fn change_stream_stage_rejects_invalid_position_or_cluster_scope() {
    let invalid_position = run_pipeline(
        vec![doc! { "_id": 1 }],
        &[
            doc! { "$match": { "_id": 1 } },
            doc! { "$changeStream": {} },
        ],
    )
    .expect_err("change stream must be first");
    assert!(matches!(invalid_position, QueryError::InvalidStage));

    let invalid_cluster_scope = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "allChangesForCluster": true } }],
        "app",
        None,
        &StaticResolver::default(),
    )
    .expect_err("cluster change stream requires admin collectionless aggregate");
    assert!(matches!(invalid_cluster_scope, QueryError::InvalidStage));
}

#[test]
fn change_stream_stage_errors_when_required_images_or_resume_tokens_are_missing() {
    let resolver = StaticResolver::default().with_change_events(vec![change_event(
        1,
        "app",
        Some("widgets"),
        "update",
        Some(doc! { "_id": 1 }),
        None,
        None,
        Some(doc! { "updatedFields": { "qty": 2 }, "removedFields": [] }),
        false,
        Document::new(),
    )]);

    let missing_full_document = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "fullDocument": "required" } }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect_err("required fullDocument");
    assert!(matches!(
        missing_full_document,
        QueryError::InvalidArgument(_)
    ));

    let missing_resume_token = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! { "$changeStream": { "resumeAfter": { "sequence": 9 } } }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect_err("missing resume token");
    assert!(matches!(
        missing_resume_token,
        QueryError::InvalidArgument(_)
    ));
}

#[test]
fn change_stream_split_large_event_splits_oversized_events_and_enforces_size_limit() {
    let payload = "x".repeat(8 * 1024 * 1024);
    let resolver =
        StaticResolver::default().with_change_events(vec![large_update_change_event(1, &payload)]);

    let oversized = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! {
            "$changeStream": {
                "fullDocument": "updateLookup",
                "fullDocumentBeforeChange": "required"
            }
        }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect_err("oversized change stream event without split stage");
    assert!(matches!(oversized, QueryError::BsonObjectTooLarge(_)));

    let results = run_pipeline_with_resolver(
        Vec::new(),
        &[
            doc! {
                "$changeStream": {
                    "fullDocument": "updateLookup",
                    "fullDocumentBeforeChange": "required"
                }
            },
            doc! { "$changeStreamSplitLargeEvent": {} },
        ],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("split large change stream event");

    assert!(results.len() >= 2);
    for (index, fragment) in results.iter().enumerate() {
        let split_event = fragment.get_document("splitEvent").expect("split event");
        assert_eq!(
            split_event.get_i32("fragment").expect("fragment"),
            (index + 1) as i32
        );
        assert_eq!(split_event.get_i32("of").expect("of"), results.len() as i32);
        assert_eq!(
            fragment
                .get_document("_id")
                .expect("token")
                .get_i64("fragmentNum")
                .expect("fragmentNum"),
            index as i64
        );
    }

    let merged = merge_split_fragments(&results);
    assert_eq!(merged.get_str("operationType").expect("type"), "update");
    assert_eq!(
        merged
            .get_document("documentKey")
            .expect("documentKey")
            .get_i64("_id")
            .expect("_id"),
        1
    );
    assert_eq!(
        merged
            .get_document("fullDocument")
            .expect("fullDocument")
            .get_str("payload")
            .expect("payload")
            .len(),
        payload.len()
    );
    assert_eq!(
        merged
            .get_document("fullDocumentBeforeChange")
            .expect("fullDocumentBeforeChange")
            .get_str("payload")
            .expect("payload")
            .len(),
        payload.len()
    );
}

#[test]
fn change_stream_split_large_event_supports_fragment_resume_and_fails_when_pipeline_changes() {
    let payload = "x".repeat(8 * 1024 * 1024);
    let resolver = StaticResolver::default().with_change_events(vec![
        large_update_change_event(1, &payload),
        change_event(
            2,
            "app",
            Some("widgets"),
            "insert",
            Some(doc! { "_id": 2 }),
            Some(doc! { "_id": 2, "payload": "small" }),
            None,
            None,
            false,
            Document::new(),
        ),
    ]);
    let base_pipeline = vec![
        doc! {
            "$changeStream": {
                "fullDocument": "updateLookup",
                "fullDocumentBeforeChange": "whenAvailable"
            }
        },
        doc! { "$changeStreamSplitLargeEvent": {} },
    ];

    let initial = run_pipeline_with_resolver(
        Vec::new(),
        &base_pipeline,
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("initial split stream");
    let split_fragments = initial
        .iter()
        .take_while(|document| document.contains_key("splitEvent"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(split_fragments.len() >= 2);
    let resume_token = split_fragments[split_fragments.len() - 2]
        .get_document("_id")
        .expect("resume token")
        .clone();

    let resumed = run_pipeline_with_resolver(
        Vec::new(),
        &[
            doc! {
                "$changeStream": {
                    "fullDocument": "updateLookup",
                    "fullDocumentBeforeChange": "whenAvailable",
                    "resumeAfter": Bson::Document(resume_token.clone()),
                }
            },
            doc! { "$changeStreamSplitLargeEvent": {} },
        ],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect("resume after split fragment");
    assert_eq!(
        resumed[0]
            .get_document("splitEvent")
            .expect("splitEvent")
            .get_i32("fragment")
            .expect("fragment"),
        split_fragments.len() as i32
    );

    let missing_split_stage = run_pipeline_with_resolver(
        Vec::new(),
        &[doc! {
            "$changeStream": {
                "fullDocument": "updateLookup",
                "fullDocumentBeforeChange": "whenAvailable",
                "resumeAfter": Bson::Document(resume_token.clone()),
            }
        }],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect_err("split resume token requires split stage");
    assert!(matches!(
        missing_split_stage,
        QueryError::ChangeStreamFatalError(_)
    ));

    let incompatible_resume = run_pipeline_with_resolver(
        Vec::new(),
        &[
            doc! {
                "$changeStream": {
                    "fullDocument": "updateLookup",
                    "fullDocumentBeforeChange": "whenAvailable",
                    "resumeAfter": Bson::Document(resume_token),
                }
            },
            doc! { "$project": { "_id": 1, "operationType": 1, "documentKey": 1, "fullDocument": 1 } },
            doc! { "$changeStreamSplitLargeEvent": {} },
        ],
        "app",
        Some("widgets"),
        &resolver,
    )
    .expect_err("resume token should fail when the resumed pipeline no longer splits");
    assert!(matches!(
        incompatible_resume,
        QueryError::ChangeStreamFatalError(_)
    ));
}

#[test]
fn change_stream_split_large_event_validates_pipeline_position() {
    let missing_change_stream =
        run_pipeline(Vec::new(), &[doc! { "$changeStreamSplitLargeEvent": {} }])
            .expect_err("split stage requires change stream");
    assert!(matches!(
        missing_change_stream,
        QueryError::InvalidArgument(_)
    ));

    let not_last = run_pipeline_with_resolver(
        Vec::new(),
        &[
            doc! { "$changeStream": {} },
            doc! { "$changeStreamSplitLargeEvent": {} },
            doc! { "$project": { "_id": 1 } },
        ],
        "app",
        Some("widgets"),
        &StaticResolver::default(),
    )
    .expect_err("split stage must be last");
    assert!(matches!(not_last, QueryError::InvalidStage));

    let results = run_pipeline_with_resolver(
        Vec::new(),
        &[
            doc! { "$changeStream": {} },
            doc! { "$match": { "operationType": "insert" } },
            doc! { "$redact": "$$DESCEND" },
            doc! { "$changeStreamSplitLargeEvent": {} },
        ],
        "app",
        Some("widgets"),
        &StaticResolver::default().with_change_events(vec![change_event(
            1,
            "app",
            Some("widgets"),
            "insert",
            Some(doc! { "_id": 1 }),
            Some(doc! { "_id": 1, "qty": 1 }),
            None,
            None,
            false,
            Document::new(),
        )]),
    )
    .expect("split stage should be valid after match and redact");
    assert_eq!(results.len(), 1);
    assert!(!results[0].contains_key("splitEvent"));
}

#[test]
fn replace_with_errors_when_expression_is_not_a_document() {
    let error = run_pipeline(
        vec![doc! { "value": 5 }],
        &[doc! { "$replaceWith": "$value" }],
    )
    .expect_err("replaceWith should reject scalars");

    assert!(matches!(error, QueryError::ExpectedDocument));
}

#[test]
fn match_stage_propagates_invalid_operator_errors() {
    let error = run_pipeline(
        vec![doc! { "qty": 1 }],
        &[doc! { "$match": { "qty": { "$unknown": 1 } } }],
    )
    .expect_err("invalid operator");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedOperator(operator) if operator == "$unknown"
    ));
}

#[test]
fn filter_rejects_where_operator() {
    let error = document_matches(&doc! { "qty": 1 }, &doc! { "$where": "this.qty > 0" })
        .expect_err("unsupported query operator");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedOperator(operator) if operator == "$where"
    ));
}

#[test]
fn pipeline_surfaces_bucket_runtime_errors_as_bad_value_style_errors() {
    let error = run_pipeline(
        vec![doc! { "price": 20 }],
        &[doc! { "$bucket": { "groupBy": "$price", "boundaries": [0, 10] } }],
    )
    .expect_err("bucket should reject unmatched values without a default");

    assert!(matches!(error, QueryError::InvalidArgument(_)));
}

#[test]
fn projection_rejects_function_expression_operator() {
    let error = apply_projection(
        &doc! { "_id": 1, "value": 2 },
        Some(
            &doc! { "out": { "$function": { "body": "function() { return 1; }", "args": [], "lang": "js" } } },
        ),
    )
    .expect_err("unsupported expression");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedOperator(operator) if operator == "$function"
    ));
}

#[test]
fn projection_rejects_unsupported_expression_operator() {
    let error = apply_projection(
        &doc! { "_id": 1, "value": 2 },
        Some(&doc! { "out": { "$convert": { "input": 1, "to": "string" } } }),
    )
    .expect_err("unsupported expression");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedOperator(operator) if operator == "$convert"
    ));
}
