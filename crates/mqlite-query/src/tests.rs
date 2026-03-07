use std::str::FromStr;

use bson::{Bson, DateTime, Decimal128, Document, Timestamp, doc, oid::ObjectId};
use pretty_assertions::assert_eq;

use crate::{
    QueryError, apply_projection, apply_update, document_matches, parse_update, run_pipeline,
};

// These cases are grounded in MongoDB matcher and pipeline tests such as
// expression_leaf_test.cpp, expression_tree_test.cpp, document_source_project_test.cpp,
// document_source_add_fields_test.cpp, document_source_unwind_test.cpp,
// document_source_group_test.cpp, document_source_replace_root_test.cpp, and
// document_source_sort_test.cpp.

fn run_pipeline_ok(documents: Vec<Document>, pipeline: &[Document]) -> Vec<Document> {
    run_pipeline(documents, pipeline).expect("pipeline")
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
fn group_stage_supports_sum_first_push_and_avg_accumulators() {
    let results = run_pipeline_ok(
        vec![
            doc! { "team": "blue", "sku": "b1", "qty": 2 },
            doc! { "team": "red", "sku": "r1", "qty": 1 },
            doc! { "team": "red", "sku": "r2", "qty": 3 },
        ],
        &[
            doc! {
                "$group": {
                    "_id": "$team",
                    "total": { "$sum": "$qty" },
                    "firstSku": { "$first": "$sku" },
                    "skus": { "$push": "$sku" },
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
                "avgQty": 2.0
            },
            doc! {
                "_id": "red",
                "total": 4_i64,
                "firstSku": "r1",
                "skus": ["r1", "r2"],
                "avgQty": 2.0
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
fn replace_root_errors_when_new_root_is_not_a_document() {
    let error = run_pipeline(
        vec![doc! { "value": 5 }],
        &[doc! { "$replaceRoot": { "newRoot": "$value" } }],
    )
    .expect_err("replaceRoot should reject scalars");

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
fn pipeline_rejects_unsupported_stage() {
    let error = run_pipeline(
        vec![doc! { "qty": 1 }],
        &[doc! { "$lookup": { "from": "other" } }],
    )
    .expect_err("unsupported stage");

    assert!(matches!(
        error,
        crate::QueryError::UnsupportedStage(stage) if stage == "$lookup"
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
