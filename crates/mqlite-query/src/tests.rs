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
    document_matches_expression, parse_filter, parse_update, run_pipeline,
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
}

impl CollectionResolver for StaticResolver {
    fn resolve_collection(&self, database: &str, collection: &str) -> Vec<Document> {
        self.collections
            .get(&(database.to_string(), collection.to_string()))
            .cloned()
            .unwrap_or_default()
    }
}

fn run_pipeline_with_static_resolver(
    documents: Vec<Document>,
    pipeline: &[Document],
    resolver: &StaticResolver,
) -> Vec<Document> {
    run_pipeline_with_resolver(documents, pipeline, "app", resolver).expect("pipeline")
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
    let document = doc! { "_id": 1, "left": 5, "right": 3, "sku": "abc" };
    let projected = apply_projection(
        &document,
        Some(&doc! {
            "eq": { "$eq": ["$left", 5] },
            "ne": { "$ne": ["$left", "$right"] },
            "gt": { "$gt": ["$left", "$right"] },
            "gte": { "$gte": ["$left", 5] },
            "lt": { "$lt": ["$right", "$left"] },
            "lte": { "$lte": ["$right", 3] },
            "and": { "$and": [true, { "$eq": ["$left", 5] }] },
            "or": { "$or": [false, { "$eq": ["$sku", "abc"] }] },
            "not": { "$not": [{ "$eq": ["$right", 5] }] },
            "in": { "$in": ["$sku", ["def", "abc"]] },
            "literal": { "$literal": { "nested": true } }
        }),
    )
    .expect("apply projection");

    assert_eq!(
        projected,
        doc! {
            "_id": 1,
            "eq": true,
            "ne": true,
            "gt": true,
            "gte": true,
            "lt": true,
            "lte": true,
            "and": true,
            "or": true,
            "not": true,
            "in": true,
            "literal": { "nested": true }
        }
    );
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
fn replace_root_errors_when_new_root_is_not_a_document() {
    let error = run_pipeline(
        vec![doc! { "value": 5 }],
        &[doc! { "$replaceRoot": { "newRoot": "$value" } }],
    )
    .expect_err("replaceRoot should reject scalars");

    assert!(matches!(error, QueryError::ExpectedDocument));
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
        Some(&doc! { "out": { "$add": [1, 2] } }),
    )
    .expect_err("unsupported expression");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedOperator(operator) if operator == "$add"
    ));
}
