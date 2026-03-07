use std::{thread, time::Duration};

use assert_cmd::Command;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn inspect_and_verify_commands_work() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("cli.mongodb");

    let mut checkpoint = Command::cargo_bin("mqlite").expect("binary");
    checkpoint
        .args(["checkpoint", "--file"])
        .arg(&database_path)
        .assert()
        .success();

    let mut inspect = Command::cargo_bin("mqlite").expect("binary");
    inspect
        .args(["inspect", "--file"])
        .arg(&database_path)
        .assert()
        .success();

    let mut verify = Command::cargo_bin("mqlite").expect("binary");
    verify
        .args(["verify", "--file"])
        .arg(&database_path)
        .assert()
        .success();
}

#[test]
fn command_auto_spawns_and_recovers_after_broker_restart() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command.mongodb");

    let mut create = Command::cargo_bin("mqlite").expect("binary");
    create
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"create":"widgets"}"#,
        ])
        .assert()
        .success();

    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"sku":"alpha","qty":2}]}"#,
        ])
        .assert()
        .success();

    thread::sleep(Duration::from_secs(2));

    let mut find = Command::cargo_bin("mqlite").expect("binary");
    let output = find
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"widgets","filter":{"sku":"alpha"}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0]["sku"], "alpha");
    assert_eq!(first_batch[0]["qty"], 2);
}

#[test]
fn command_preserves_unique_indexes_across_restart() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-index.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"sku":1},"name":"sku_1","unique":true}]}"#,
        ])
        .assert()
        .success();

    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":1,"sku":"alpha"}]}"#,
        ])
        .assert()
        .success();

    thread::sleep(Duration::from_secs(2));

    let mut duplicate = Command::cargo_bin("mqlite").expect("binary");
    let output = duplicate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":2,"sku":"alpha"}]}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["code"], 11000);
}

#[test]
fn command_explain_reports_ixscan_for_indexed_find() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-explain.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"sku":1},"name":"sku_1","unique":true}]}"#,
        ])
        .assert()
        .success();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"explain":{"find":"widgets","filter":{"sku":"alpha"}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["queryPlanner"]["winningPlan"]["stage"], "IXSCAN");
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["indexName"],
        "sku_1"
    );
}

#[test]
fn command_explain_reports_compound_prefix_sort_plan() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-compound-explain.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"category":1,"qty":-1},"name":"category_1_qty_-1"}]}"#,
        ])
        .assert()
        .success();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"explain":{"find":"widgets","filter":{"category":"tools"},"sort":{"qty":1}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["queryPlanner"]["winningPlan"]["stage"], "IXSCAN");
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["indexName"],
        "category_1_qty_-1"
    );
    assert_eq!(response["queryPlanner"]["winningPlan"]["sortCovered"], true);
    assert_eq!(response["queryPlanner"]["winningPlan"]["scanDirection"], -1);
}

#[test]
fn command_explain_reports_descending_compound_range_bounds() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-compound-range.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"category":1,"qty":-1},"name":"category_1_qty_-1"}]}"#,
        ])
        .assert()
        .success();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"explain":{"find":"widgets","filter":{"category":"tools","qty":{"$gt":3,"$lte":9}},"sort":{"qty":-1}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["queryPlanner"]["winningPlan"]["stage"], "IXSCAN");
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["indexName"],
        "category_1_qty_-1"
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["lowerBound"],
        serde_json::json!({ "category": "tools", "qty": 9 })
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["lowerInclusive"],
        true
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["upperBound"],
        serde_json::json!({ "category": "tools", "qty": 3 })
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["upperInclusive"],
        false
    );
    assert_eq!(response["queryPlanner"]["winningPlan"]["matchedFields"], 2);
    assert_eq!(response["queryPlanner"]["winningPlan"]["sortCovered"], true);
    assert_eq!(response["queryPlanner"]["winningPlan"]["scanDirection"], 1);
}

#[test]
fn command_explain_prefers_lower_cost_index() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-cost-based.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"category":1,"status":1},"name":"category_1_status_1"},{"key":{"sku":1},"name":"sku_1","unique":true}]}"#,
        ])
        .assert()
        .success();

    let mut documents = (0..12)
        .map(|value| {
            json!({
                "_id": value,
                "category": "tools",
                "status": "active",
                "sku": format!("sku-{value:03}"),
            })
        })
        .collect::<Vec<_>>();
    documents.push(json!({
        "_id": 100,
        "category": "tools",
        "status": "active",
        "sku": "target",
    }));
    let insert_command = json!({
        "insert": "widgets",
        "documents": documents,
    })
    .to_string();

    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&insert_command)
        .assert()
        .success();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"explain":{"find":"widgets","filter":{"category":"tools","status":"active","sku":"target"}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["indexName"],
        "sku_1"
    );
    assert_eq!(response["queryPlanner"]["winningPlan"]["keysExamined"], 1);
    assert_eq!(response["queryPlanner"]["winningPlan"]["docsExamined"], 1);
}

#[test]
fn command_explain_reports_projection_covered() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-projection-covered.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"category":1,"qty":1,"_id":1},"name":"category_1_qty_1_id_1"}]}"#,
        ])
        .assert()
        .success();

    let insert_command = json!({
        "insert": "widgets",
        "documents": [
            { "_id": 1, "category": "tools", "qty": 3, "secret": "alpha" },
            { "_id": 2, "category": "tools", "qty": 5, "secret": "beta" },
            { "_id": 3, "category": "garden", "qty": 1, "secret": "gamma" }
        ]
    })
    .to_string();

    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&insert_command)
        .assert()
        .success();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"explain":{"find":"widgets","filter":{"category":"tools"},"projection":{"category":1,"qty":1,"_id":1},"sort":{"qty":1}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["indexName"],
        "category_1_qty_1_id_1"
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["filterCovered"],
        true
    );
    assert_eq!(
        response["queryPlanner"]["winningPlan"]["projectionCovered"],
        true
    );
    assert_eq!(response["queryPlanner"]["winningPlan"]["sortCovered"], true);
    assert_eq!(response["queryPlanner"]["winningPlan"]["docsExamined"], 0);
}

#[test]
fn command_explain_reports_plan_cache_usage_and_invalidation() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-plan-cache.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"sku":1},"name":"sku_1","unique":true}]}"#,
        ])
        .assert()
        .success();

    let explain_command = r#"{"explain":{"find":"widgets","filter":{"sku":"alpha"}}}"#;

    let mut first_explain = Command::cargo_bin("mqlite").expect("binary");
    let first_output = first_explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            explain_command,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let first_response: Value = serde_json::from_slice(&first_output).expect("json response");
    assert_eq!(first_response["queryPlanner"]["planCacheUsed"], false);

    let mut second_explain = Command::cargo_bin("mqlite").expect("binary");
    let second_output = second_explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            explain_command,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let second_response: Value = serde_json::from_slice(&second_output).expect("json response");
    assert_eq!(second_response["queryPlanner"]["planCacheUsed"], true);

    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":1,"sku":"alpha"}]}"#,
        ])
        .assert()
        .success();

    let mut third_explain = Command::cargo_bin("mqlite").expect("binary");
    let third_output = third_explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            explain_command,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let third_response: Value = serde_json::from_slice(&third_output).expect("json response");
    assert_eq!(third_response["queryPlanner"]["planCacheUsed"], false);
}

#[test]
fn command_explain_reports_multi_interval_or_scan() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-multi-interval.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"category":1,"sku":1},"name":"category_1_sku_1"}]}"#,
        ])
        .assert()
        .success();

    let insert_command = json!({
        "insert": "widgets",
        "documents": [
            { "_id": 1, "category": "tools", "sku": "a" },
            { "_id": 2, "category": "tools", "sku": "b" },
            { "_id": 3, "category": "tools", "sku": "c" },
            { "_id": 4, "category": "garden", "sku": "a" }
        ]
    })
    .to_string();
    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&insert_command)
        .assert()
        .success();

    let explain_command = json!({
        "explain": {
            "find": "widgets",
            "filter": {
                "$or": [
                    { "category": "tools", "sku": "a" },
                    { "category": "tools", "sku": "b" }
                ]
            },
            "projection": { "_id": 0, "category": 1, "sku": 1 },
            "sort": { "sku": 1 }
        }
    })
    .to_string();

    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let explain_output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&explain_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let explain_response: Value = serde_json::from_slice(&explain_output).expect("json response");
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["stage"],
        "IXSCAN"
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["intervalCount"],
        2
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["matchedFields"],
        2
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["projectionCovered"],
        true
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["docsExamined"],
        0
    );

    let find_command = json!({
        "find": "widgets",
        "filter": {
            "$or": [
                { "category": "tools", "sku": "a" },
                { "category": "tools", "sku": "b" }
            ]
        },
        "projection": { "_id": 0, "category": 1, "sku": 1 },
        "sort": { "sku": 1 }
    })
    .to_string();
    let mut find = Command::cargo_bin("mqlite").expect("binary");
    let find_output = find
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&find_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let find_response: Value = serde_json::from_slice(&find_output).expect("json response");
    let skus = find_response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .iter()
        .map(|document| document["sku"].as_str().expect("sku"))
        .collect::<Vec<_>>();
    assert_eq!(skus, vec!["a", "b"]);
}

#[test]
fn command_find_distinguishes_null_from_missing_in_covered_scan() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-null-vs-missing.mongodb");

    let mut create_indexes = Command::cargo_bin("mqlite").expect("binary");
    create_indexes
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"createIndexes":"widgets","indexes":[{"key":{"flag":1,"sku":1},"name":"flag_1_sku_1"}]}"#,
        ])
        .assert()
        .success();

    let insert_command = json!({
        "insert": "widgets",
        "documents": [
            { "_id": 1, "sku": "missing" },
            { "_id": 2, "sku": "null", "flag": Value::Null },
            { "_id": 3, "sku": "set", "flag": "yes" }
        ]
    })
    .to_string();
    let mut insert = Command::cargo_bin("mqlite").expect("binary");
    insert
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&insert_command)
        .assert()
        .success();

    let explain_command = json!({
        "explain": {
            "find": "widgets",
            "filter": { "flag": Value::Null },
            "projection": { "_id": 0, "flag": 1, "sku": 1 },
            "sort": { "sku": 1 }
        }
    })
    .to_string();
    let mut explain = Command::cargo_bin("mqlite").expect("binary");
    let explain_output = explain
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&explain_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let explain_response: Value = serde_json::from_slice(&explain_output).expect("json response");
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["projectionCovered"],
        true
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["docsExamined"],
        0
    );

    let find_command = json!({
        "find": "widgets",
        "filter": { "flag": Value::Null },
        "projection": { "_id": 0, "flag": 1, "sku": 1 },
        "sort": { "sku": 1 }
    })
    .to_string();
    let mut find = Command::cargo_bin("mqlite").expect("binary");
    let find_output = find
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
        ])
        .arg(&find_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let find_response: Value = serde_json::from_slice(&find_output).expect("json response");
    let first_batch = find_response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch");
    assert_eq!(first_batch.len(), 1);
    let document = first_batch[0].as_object().expect("document");
    assert_eq!(
        document.get("sku"),
        Some(&Value::String("null".to_string()))
    );
    assert!(document.contains_key("flag"));
    assert_eq!(document.get("flag"), Some(&Value::Null));
}
