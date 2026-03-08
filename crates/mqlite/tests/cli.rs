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
fn command_collectionless_aggregate_supports_documents_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-documents.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$documents":[{"a":1},{"a":2}]},{"$project":{"_id":0,"a":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "a": 1 }), json!({ "a": 2 })]);
}

#[test]
fn command_collectionless_aggregate_supports_current_op_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-current-op.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$currentOp":{"localOps":true}},{"$project":{"_id":0,"ns":1,"type":1,"command":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch[0]["ns"], "admin.$cmd.aggregate");
    assert_eq!(first_batch[0]["type"], "op");
    assert_eq!(first_batch[0]["command"]["aggregate"], 1);
    assert_eq!(
        first_batch[0]["command"]["pipeline"][0]["$currentOp"]["localOps"],
        true
    );
    assert_eq!(
        first_batch[0]["command"]["pipeline"][1]["$project"]["ns"],
        1
    );
}

#[test]
fn command_collection_aggregate_rejects_current_op_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-current-op-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$currentOp":{"localOps":true}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_coll_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-coll-stats.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha","qty":2},{"sku":"beta","qty":4}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"createIndexes":"widgets","indexes":[{"name":"sku_1","key":{"sku":1}}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$collStats":{"count":{},"storageStats":{}}},{"$project":{"_id":0,"ns":1,"count":1,"storageStats.count":1,"storageStats.nindexes":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch[0]["ns"], "app.widgets");
    assert_eq!(first_batch[0]["count"], 2);
    assert_eq!(first_batch[0]["storageStats"]["count"], 2);
    assert_eq!(first_batch[0]["storageStats"]["nindexes"], 2);
}

#[test]
fn command_collectionless_aggregate_rejects_coll_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-coll-stats-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$collStats":{"count":{}}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_index_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-index-stats.mongodb");

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
            r#"{"createIndexes":"widgets","indexes":[{"name":"sku_1","key":{"sku":1}}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$indexStats":{}},{"$project":{"_id":0,"name":1,"spec.key":1,"accesses.ops":1}},{"$sort":{"name":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "name": "_id_", "spec": { "key": { "_id": 1 } }, "accesses": { "ops": 0 } }),
            json!({ "name": "sku_1", "spec": { "key": { "sku": 1 } }, "accesses": { "ops": 0 } }),
        ]
    );
}

#[test]
fn command_collectionless_aggregate_rejects_index_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-index-stats-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$indexStats":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_plan_cache_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-plan-cache-stats.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha","qty":2},{"sku":"beta","qty":4}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"createIndexes":"widgets","indexes":[{"name":"sku_1","key":{"sku":1}}]}"#,
        ])
        .assert()
        .success();

    let mut find = Command::cargo_bin("mqlite").expect("binary");
    find.args([
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
    .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$planCacheStats":{}},{"$project":{"_id":0,"namespace":1,"filterShape":1,"cachedPlan":1,"host":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch[0]["namespace"], "app.widgets");
    assert!(first_batch[0]["filterShape"].as_str().is_some());
    assert_eq!(first_batch[0]["cachedPlan"]["type"], "index");
    assert_eq!(first_batch[0]["cachedPlan"]["name"], "sku_1");
    assert_eq!(first_batch[0]["host"], "mqlite");
}

#[test]
fn command_collectionless_aggregate_rejects_plan_cache_stats_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-plan-cache-stats-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$planCacheStats":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_collectionless_aggregate_supports_list_catalog_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-catalog.mongodb");

    let mut insert_widgets = Command::cargo_bin("mqlite").expect("binary");
    insert_widgets
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"sku":"alpha"}]}"#,
        ])
        .assert()
        .success();

    let mut insert_metrics = Command::cargo_bin("mqlite").expect("binary");
    insert_metrics
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"metrics","documents":[{"kind":"daily"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listCatalog":{}},{"$project":{"_id":0,"ns":1,"indexCount":1}},{"$sort":{"ns":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "ns": "analytics.metrics", "indexCount": 1 }),
            json!({ "ns": "app.widgets", "indexCount": 1 }),
        ]
    );
}

#[test]
fn command_collectionless_aggregate_rejects_list_catalog_stage_outside_admin() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-catalog-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listCatalog":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_collectionless_aggregate_supports_list_cluster_catalog_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-cluster-catalog.mongodb");

    let mut create_widgets = Command::cargo_bin("mqlite").expect("binary");
    create_widgets
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

    let mut create_metrics = Command::cargo_bin("mqlite").expect("binary");
    create_metrics
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"create":"metrics"}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listClusterCatalog":{"shards":true,"tracked":true,"balancingConfiguration":true}},{"$project":{"_id":0,"db":1,"ns":1,"sharded":1,"tracked":1,"shards":1}},{"$sort":{"ns":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "db": "analytics", "ns": "analytics.metrics", "sharded": false, "tracked": false, "shards": [] }),
            json!({ "db": "app", "ns": "app.widgets", "sharded": false, "tracked": false, "shards": [] }),
        ]
    );
}

#[test]
fn command_collectionless_aggregate_limits_list_cluster_catalog_stage_to_the_target_database() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-cluster-catalog-scoped.mongodb");

    let mut create_widgets = Command::cargo_bin("mqlite").expect("binary");
    create_widgets
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

    let mut create_metrics = Command::cargo_bin("mqlite").expect("binary");
    create_metrics
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"create":"metrics"}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listClusterCatalog":{}},{"$project":{"_id":0,"ns":1}},{"$sort":{"ns":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "ns": "analytics.metrics" })]);
}

#[test]
fn command_collectionless_aggregate_returns_empty_list_cluster_catalog_for_missing_database() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-cluster-catalog-missing.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "missing",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listClusterCatalog":{}}],"cursor":{}}"#,
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
    assert!(first_batch.is_empty());
}

#[test]
fn command_collection_aggregate_rejects_list_cluster_catalog_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-cluster-catalog-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listClusterCatalog":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_list_cached_and_active_users_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-cached-users.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listCachedAndActiveUsers":{}}],"cursor":{}}"#,
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
    assert!(first_batch.is_empty());
}

#[test]
fn command_collectionless_aggregate_supports_list_local_sessions_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-local-sessions.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listLocalSessions":{"allUsers":true}},{"$count":"count"}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "count": 0 })]);
}

#[test]
fn command_collection_aggregate_rejects_list_local_sessions_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-local-sessions-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"system.sessions","pipeline":[{"$listLocalSessions":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_list_sessions_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-sessions.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "config",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"system.sessions","pipeline":[{"$listSessions":{"allUsers":true}},{"$count":"count"}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "count": 0 })]);
}

#[test]
fn command_aggregate_rejects_list_sessions_stage_on_wrong_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-sessions-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listSessions":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_collectionless_aggregate_supports_list_sampled_queries_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-sampled-queries.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listSampledQueries":{"namespace":"app.widgets"}},{"$count":"count"}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "count": 0 })]);
}

#[test]
fn command_collectionless_aggregate_rejects_list_sampled_queries_stage_outside_admin() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-sampled-queries-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listSampledQueries":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_list_search_indexes_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-search-indexes.mongodb");

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

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listSearchIndexes":{"name":"search-index"}},{"$count":"count"}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "count": 0 })]);
}

#[test]
fn command_aggregate_supports_list_search_indexes_stage_for_missing_collection() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-search-indexes-missing.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listSearchIndexes":{}}],"cursor":{}}"#,
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
    assert!(first_batch.is_empty());
}

#[test]
fn command_collectionless_aggregate_rejects_list_search_indexes_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-search-indexes-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listSearchIndexes":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_rejects_list_search_indexes_with_id_and_name() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-search-indexes-bad-spec.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$listSearchIndexes":{"name":"search-index","id":"index-id"}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "BadValue");
}

#[test]
fn command_aggregate_supports_redact_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-redact.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"level":1,"nested":{"level":2,"keep":true,"deeper":{"level":3,"secret":true}}},{"_id":2,"level":3,"hidden":true}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$redact":{"$cond":[{"$lte":["$level",2]},"$$DESCEND","$$PRUNE"]}},{"$project":{"_id":0,"level":1,"nested":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "level": 1,
            "nested": {
                "level": 2,
                "keep": true
            }
        })]
    );
}

#[test]
fn command_aggregate_rejects_invalid_redact_decisions() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-redact-invalid.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"level":1}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$redact":"KEEP"}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_aggregate_supports_densify_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-densify.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"val":0},{"_id":2,"val":4},{"_id":3,"val":9}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"val":1}},{"$densify":{"field":"val","range":{"step":2,"bounds":[0,10]}}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "val": 0 }),
            json!({ "val": 2 }),
            json!({ "val": 4 }),
            json!({ "val": 6 }),
            json!({ "val": 8 }),
            json!({ "val": 9 }),
        ]
    );
}

#[test]
fn command_aggregate_rejects_invalid_densify_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-densify-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$densify":{"field":"val","range":{"step":1,"bounds":"partition"}}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_aggregate_supports_fill_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-fill.mongodb");

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
            r#"{"insert":"widgets","documents":[{"seq":1,"part":1,"linear":1,"other":1},{"seq":2,"part":2,"linear":1,"other":1},{"seq":3,"part":1,"linear":null,"other":null},{"seq":4,"part":2,"linear":null,"other":null},{"seq":5,"part":1,"linear":5,"other":10},{"seq":6,"part":2,"linear":6,"other":2}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"seq":1,"part":1,"linear":1,"other":1}},{"$fill":{"sortBy":{"seq":1},"partitionBy":"$part","output":{"linear":{"method":"linear"},"other":{"method":"locf"}}}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "seq": 1, "part": 1, "linear": 1, "other": 1 }),
            json!({ "seq": 3, "part": 1, "linear": 3, "other": 1 }),
            json!({ "seq": 5, "part": 1, "linear": 5, "other": 10 }),
            json!({ "seq": 2, "part": 2, "linear": 1, "other": 1 }),
            json!({ "seq": 4, "part": 2, "linear": 3.5, "other": 1 }),
            json!({ "seq": 6, "part": 2, "linear": 6, "other": 2 }),
        ]
    );
}

#[test]
fn command_aggregate_rejects_invalid_fill_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-fill-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$fill":{"output":{"qty":{"method":"linear"}}}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_aggregate_supports_set_window_fields_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-set-window-fields.mongodb");

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
            r#"{"insert":"widgets","documents":[{"team":"a","seq":2,"qty":3},{"team":"a","seq":0,"qty":1},{"team":"b","seq":1,"qty":5},{"team":"a","seq":1,"qty":2},{"team":"b","seq":0,"qty":4}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$setWindowFields":{"partitionBy":"$team","sortBy":{"seq":1},"output":{"runningQty":{"$sum":"$qty","window":{"documents":["unbounded","current"]}},"rank":{"$documentNumber":{}}}}},{"$project":{"_id":0,"team":1,"seq":1,"qty":1,"runningQty":1,"rank":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "team": "a", "seq": 0, "qty": 1, "runningQty": 1, "rank": 1 }),
            json!({ "team": "a", "seq": 1, "qty": 2, "runningQty": 3, "rank": 2 }),
            json!({ "team": "a", "seq": 2, "qty": 3, "runningQty": 6, "rank": 3 }),
            json!({ "team": "b", "seq": 0, "qty": 4, "runningQty": 4, "rank": 1 }),
            json!({ "team": "b", "seq": 1, "qty": 5, "runningQty": 9, "rank": 2 }),
        ]
    );
}

#[test]
fn command_aggregate_rejects_invalid_set_window_fields_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-set-window-fields-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$setWindowFields":{"output":{"rank":{"$rank":{}}}}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_aggregate_supports_graph_lookup_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-graph-lookup.mongodb");

    let mut insert_local = Command::cargo_bin("mqlite").expect("binary");
    insert_local
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"start":"a"}]}"#,
        ])
        .assert()
        .success();

    let mut insert_foreign = Command::cargo_bin("mqlite").expect("binary");
    insert_foreign
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"foreign","documents":[{"name":"a","neighbors":["b","c"],"kind":"keep"},{"name":"b","neighbors":["d"],"kind":"skip"},{"name":"c","neighbors":["d"],"kind":"keep"},{"name":"d","neighbors":[],"kind":"keep"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"start":1}},{"$graphLookup":{"from":"foreign","startWith":"$start","connectFromField":"neighbors","connectToField":"name","depthField":"depth","maxDepth":2,"restrictSearchWithMatch":{"kind":"keep"},"as":"results"}}],"cursor":{}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let mut first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch")
        .clone();
    for document in &mut first_batch {
        if let Some(results) = document.get_mut("results").and_then(Value::as_array_mut) {
            for foreign in results {
                foreign
                    .as_object_mut()
                    .expect("foreign document")
                    .remove("_id");
            }
        }
    }
    assert_eq!(
        &first_batch,
        &vec![json!({
            "start": "a",
            "results": [
                { "name": "a", "neighbors": ["b", "c"], "kind": "keep", "depth": 0 },
                { "name": "c", "neighbors": ["d"], "kind": "keep", "depth": 1 },
                { "name": "d", "neighbors": [], "kind": "keep", "depth": 2 },
            ]
        })]
    );
}

#[test]
fn command_aggregate_rejects_invalid_graph_lookup_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-graph-lookup-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$graphLookup":{"from":"foreign","connectFromField":"neighbors","connectToField":"name","as":"results"}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_aggregate_supports_geo_near_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-geo-near.mongodb");

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
            r#"{"insert":"widgets","documents":[{"name":"zero","kind":"keep","loc":[0.0,0.0]},{"name":"far","kind":"skip","loc":[5.0,0.0]},{"name":"near","kind":"keep","loc":[1.0,0.0]}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$geoNear":{"near":[0.0,0.0],"key":"loc","distanceField":"dist","includeLocs":"matchedLoc","maxDistance":2.0,"query":{"kind":"keep"}}},{"$project":{"_id":0,"name":1,"dist":1,"matchedLoc":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "name": "zero", "dist": 0.0, "matchedLoc": [0.0, 0.0] }),
            json!({ "name": "near", "dist": 1.0, "matchedLoc": [1.0, 0.0] }),
        ]
    );
}

#[test]
fn command_aggregate_rejects_non_initial_geo_near_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-geo-near-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$match":{}},{"$geoNear":{"near":[0.0,0.0],"key":"loc","distanceField":"dist"}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "FailedToParse");
}

#[test]
fn command_collectionless_aggregate_supports_query_settings_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-query-settings.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$querySettings":{"showDebugQueryShape":true}},{"$count":"count"}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "count": 0 })]);
}

#[test]
fn command_collection_aggregate_rejects_query_settings_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-query-settings-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$querySettings":{}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_collectionless_aggregate_supports_list_mql_entities_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-list-mql-entities.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "admin",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listMqlEntities":{"entityType":"aggregationStages"}},{"$match":{"name":"$match"}},{"$project":{"_id":0,"name":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch, &vec![json!({ "name": "$match" })]);
}

#[test]
fn command_collectionless_aggregate_rejects_list_mql_entities_stage_outside_admin() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-list-mql-entities-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":1,"pipeline":[{"$listMqlEntities":{"entityType":"aggregationStages"}}],"cursor":{}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "InvalidNamespace");
}

#[test]
fn command_aggregate_supports_bucket_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-bucket.mongodb");

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
            r#"{"insert":"widgets","documents":[{"price":10,"qty":1},{"price":20,"qty":2},{"price":40,"qty":3}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$bucket":{"groupBy":"$price","boundaries":[0,20,50],"output":{"totalQty":{"$sum":"$qty"}}}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "_id": 0, "totalQty": 1 }),
            json!({ "_id": 20, "totalQty": 5 }),
        ]
    );
}

#[test]
fn command_aggregate_supports_bucket_auto_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-bucket-auto.mongodb");

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
            r#"{"insert":"widgets","documents":[{"price":10},{"price":20},{"price":30},{"price":40}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$bucketAuto":{"groupBy":"$price","buckets":2}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "_id": { "min": 10, "max": 30 }, "count": 2 }),
            json!({ "_id": { "min": 30, "max": 40 }, "count": 2 }),
        ]
    );
}

#[test]
fn command_collection_aggregate_rejects_documents_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-documents-invalid.mongodb");

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$documents":[{"a":1}]}],"cursor":{}}"#,
        ])
        .assert()
        .failure();
}

#[test]
fn command_aggregate_supports_union_with_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-union-with.mongodb");

    let mut insert_base = Command::cargo_bin("mqlite").expect("binary");
    insert_base
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"widgets","documents":[{"label":"base"}]}"#,
        ])
        .assert()
        .success();

    let mut insert_union = Command::cargo_bin("mqlite").expect("binary");
    insert_union
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"archive","documents":[{"label":"u1"},{"label":"u2"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$unionWith":{"coll":"archive","pipeline":[{"$set":{"source":{"$literal":"archive"}}}]}},{"$sort":{"label":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "label": "base", "_id": first_batch[0]["_id"].clone() }),
            json!({ "label": "u1", "source": "archive", "_id": first_batch[1]["_id"].clone() }),
            json!({ "label": "u2", "source": "archive", "_id": first_batch[2]["_id"].clone() }),
        ]
    );
}

#[test]
fn command_aggregate_supports_lookup_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-lookup.mongodb");

    let mut insert_animals = Command::cargo_bin("mqlite").expect("binary");
    insert_animals
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"animals","documents":[{"name":"cat","loc":"barn","breed":"tabby"},{"name":"dog","loc":"kennel","breed":"terrier"},{"name":"puppy","loc":"kennel","breed":"corgi"}]}"#,
        ])
        .assert()
        .success();

    let mut insert_locations = Command::cargo_bin("mqlite").expect("binary");
    insert_locations
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"locations","documents":[{"locId":"barn","breed":"tabby","label":"hayloft"},{"locId":"kennel","breed":"terrier","label":"dog run"},{"locId":"kennel","breed":"husky","label":"sled shed"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"animals","pipeline":[{"$lookup":{"from":"locations","localField":"loc","foreignField":"locId","as":"matches","let":{"wantedBreed":"$breed"},"pipeline":[{"$match":{"$expr":{"$eq":["$$wantedBreed","$breed"]}}},{"$project":{"_id":0,"label":1,"locId":1}}]}},{"$project":{"_id":0,"name":1,"matches":1}},{"$sort":{"name":1}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "name": "cat", "matches": [{ "label": "hayloft", "locId": "barn" }] }),
            json!({ "name": "dog", "matches": [{ "label": "dog run", "locId": "kennel" }] }),
            json!({ "name": "puppy", "matches": [] }),
        ]
    );
}

#[test]
fn command_aggregate_supports_out_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-out.mongodb");

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
            r#"{"insert":"widgets","documents":[{"label":"base"},{"label":"next"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"label":1}},{"$out":{"db":"analytics","coll":"archive"}}],"cursor":{}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert!(
        response["cursor"]["firstBatch"]
            .as_array()
            .expect("firstBatch")
            .is_empty()
    );

    let mut find = Command::cargo_bin("mqlite").expect("binary");
    let find_output = find
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"archive","filter":{},"sort":{"label":1}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let find_response: Value = serde_json::from_slice(&find_output).expect("json response");
    let first_batch = find_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert_eq!(
        first_batch,
        &vec![
            json!({ "label": "base", "_id": first_batch[0]["_id"].clone() }),
            json!({ "label": "next", "_id": first_batch[1]["_id"].clone() }),
        ]
    );
}

#[test]
fn command_aggregate_supports_merge_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-merge.mongodb");

    let mut insert_source = Command::cargo_bin("mqlite").expect("binary");
    insert_source
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"source","documents":[{"key":1,"a":1,"b":"a"},{"key":2,"a":2,"b":"b"},{"key":3,"a":3,"b":"c"}]}"#,
        ])
        .assert()
        .success();

    let mut insert_target = Command::cargo_bin("mqlite").expect("binary");
    insert_target
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"insert":"archive","documents":[{"key":1,"a":10,"c":"z"},{"key":3,"a":30},{"key":4,"a":40}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"source","pipeline":[{"$merge":{"into":{"db":"analytics","coll":"archive"},"on":"key","whenMatched":"merge","whenNotMatched":"insert"}}],"cursor":{}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert!(
        response["cursor"]["firstBatch"]
            .as_array()
            .expect("firstBatch")
            .is_empty()
    );

    let mut find = Command::cargo_bin("mqlite").expect("binary");
    let find_output = find
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "analytics",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"archive","filter":{},"sort":{"key":1},"projection":{"_id":0,"key":1,"a":1,"b":1,"c":1}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let find_response: Value = serde_json::from_slice(&find_output).expect("json response");
    let first_batch = find_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert_eq!(
        first_batch,
        &vec![
            json!({ "key": 1, "a": 1, "b": "a", "c": "z" }),
            json!({ "key": 2, "a": 2, "b": "b" }),
            json!({ "key": 3, "a": 3, "b": "c" }),
            json!({ "key": 4, "a": 40 }),
        ]
    );
}

#[test]
fn command_aggregate_supports_sample_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-sample.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1},{"_id":2},{"_id":3}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$sample":{"size":2}}],"cursor":{}}"#,
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
    assert_eq!(first_batch.len(), 2);
    let ids = first_batch
        .iter()
        .map(|document| {
            let id = document.get("_id").expect("_id");
            assert_ne!(id, &Value::Null);
            id.to_string()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 2);
}

#[test]
fn command_aggregate_supports_sort_by_count_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-sort-by-count.mongodb");

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
            r#"{"insert":"widgets","documents":[{"team":"red"},{"team":"blue"},{"team":"blue"},{"team":"green"},{"team":"green"},{"team":"green"}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$sortByCount":"$team"}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![
            json!({ "_id": "green", "count": 3 }),
            json!({ "_id": "blue", "count": 2 }),
            json!({ "_id": "red", "count": 1 }),
        ]
    );
}

#[test]
fn command_aggregate_project_supports_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("expression-operators.mongodb");

    let insert_command = json!({
        "insert": "widgets",
        "documents": [{
            "_id": 1,
            "left": 5,
            "right": 3,
            "text": "abc",
            "array": [1, 2, 3],
            "object": { "a": 1, "b": 2 },
            "pairs": [["price", 24], ["item", "apple"]]
        }]
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

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "sum": { "$add": ["$left", "$right"] },
                    "difference": { "$subtract": ["$left", "$right"] },
                    "allTrue": { "$allElementsTrue": [true, 1, "ok"] },
                    "quotient": { "$divide": [7, 2] },
                    "anyTrue": { "$anyElementTrue": [0, false, "ok"] },
                    "remainder": { "$mod": [17, 5] },
                    "rounded": { "$round": [2.65, 1] },
                    "fallback": { "$ifNull": ["$missing", "$left"] },
                    "cmp": { "$cmp": ["$left", "$right"] },
                    "concat": { "$concat": ["prefix-", "$text"] },
                    "expr": { "$expr": { "$eq": ["$text", "abc"] } },
                    "const": { "$const": "fixed" },
                    "last": { "$arrayElemAt": ["$array", -1] },
                    "size": { "$size": "$array" },
                    "isArray": { "$isArray": "$array" },
                    "isNumber": { "$isNumber": "$left" },
                    "merged": { "$mergeObjects": ["$object", { "b": 9, "c": 3 }] },
                    "expanded": { "$objectToArray": "$object" },
                    "collapsed": { "$arrayToObject": "$pairs" },
                    "joined": { "$concatArrays": ["$array", [4, 5]] },
                    "first": { "$first": "$array" },
                    "tail": { "$last": "$array" },
                    "missing": { "$arrayElemAt": ["$array", 99] }
                }
            }
        ],
        "cursor": {}
    })
    .to_string();
    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
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
        .arg(&aggregate_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch");
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0]["sum"], 8);
    assert_eq!(first_batch[0]["difference"], 2);
    assert_eq!(first_batch[0]["allTrue"], true);
    assert_eq!(first_batch[0]["quotient"], 3.5);
    assert_eq!(first_batch[0]["anyTrue"], true);
    assert_eq!(first_batch[0]["remainder"], 2);
    assert_eq!(first_batch[0]["rounded"], 2.7);
    assert_eq!(first_batch[0]["fallback"], 5);
    assert_eq!(first_batch[0]["cmp"], 1);
    assert_eq!(first_batch[0]["concat"], "prefix-abc");
    assert_eq!(first_batch[0]["expr"], true);
    assert_eq!(first_batch[0]["const"], "fixed");
    assert_eq!(first_batch[0]["last"], 3);
    assert_eq!(first_batch[0]["size"], 3);
    assert_eq!(first_batch[0]["isArray"], true);
    assert_eq!(first_batch[0]["isNumber"], true);
    assert_eq!(first_batch[0]["merged"], json!({ "a": 1, "b": 9, "c": 3 }));
    assert_eq!(
        first_batch[0]["expanded"],
        json!([{ "k": "a", "v": 1 }, { "k": "b", "v": 2 }])
    );
    assert_eq!(
        first_batch[0]["collapsed"],
        json!({ "price": 24, "item": "apple" })
    );
    assert_eq!(first_batch[0]["joined"], json!([1, 2, 3, 4, 5]));
    assert_eq!(first_batch[0]["first"], 1);
    assert_eq!(first_batch[0]["tail"], 3);
    assert!(first_batch[0].get("missing").is_none());
}

#[test]
fn command_aggregate_supports_facet_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-facet.mongodb");

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
            r#"{"insert":"widgets","documents":[{"team":"red","qty":1},{"team":"blue","qty":3},{"team":"blue","qty":2}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$facet":{"totals":[{"$sortByCount":"$team"}],"topQty":[{"$sort":{"qty":-1}},{"$limit":1},{"$project":{"_id":0,"qty":1}}]}}],"cursor":{}}"#,
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "totals": [
                { "_id": "blue", "count": 2 },
                { "_id": "red", "count": 1 }
            ],
            "topQty": [
                { "qty": 3 }
            ]
        })]
    );
}

#[test]
fn command_find_supports_size_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-size.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"tags":["red","blue"]},{"_id":2,"tags":["red"]}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"tags":{"$size":2}}}"#,
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
    assert_eq!(first_batch[0]["tags"], json!(["red", "blue"]));
}

#[test]
fn command_find_supports_mod_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-mod.mongodb");

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
            r#"{"insert":"widgets","documents":[{"qty":5},{"qty":12},{"qty":4.7}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"qty":{"$mod":[5,2]}}}"#,
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
    assert_eq!(first_batch[0]["qty"], 12);
}

#[test]
fn command_find_supports_all_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-all.mongodb");

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
            r#"{"insert":"widgets","documents":[{"tags":["red","blue"]},{"tags":["red"]}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"tags":{"$all":["red","blue"]}}}"#,
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
    assert_eq!(first_batch[0]["tags"], json!(["red", "blue"]));
}

#[test]
fn command_find_supports_top_level_comment_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-comment.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha"},{"sku":"beta"}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"sku":"alpha","$comment":"metadata only"}}"#,
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
}

#[test]
fn command_find_supports_always_boolean_filters() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-always.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha"},{"sku":"beta"}]}"#,
        ])
        .assert()
        .success();

    let mut find_true = Command::cargo_bin("mqlite").expect("binary");
    let true_output = find_true
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"widgets","filter":{"$alwaysTrue":1}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let true_response: Value = serde_json::from_slice(&true_output).expect("json response");
    let true_batch = true_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert_eq!(true_batch.len(), 2);

    let mut find_false = Command::cargo_bin("mqlite").expect("binary");
    let false_output = find_false
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"widgets","filter":{"$alwaysFalse":1}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let false_response: Value = serde_json::from_slice(&false_output).expect("json response");
    let false_batch = false_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert!(false_batch.is_empty());
}

#[test]
fn command_find_supports_bit_test_filters() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-bitwise.mongodb");

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
            r#"{"insert":"widgets","documents":[{"qty":54},{"qty":55},{"qty":129}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"qty":{"$bitsAllSet":[1,2,4,5]}}}"#,
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
    assert_eq!(first_batch.len(), 2);
    assert_eq!(first_batch[0]["qty"], 54);
    assert_eq!(first_batch[1]["qty"], 55);
}

#[test]
fn command_find_supports_sample_rate_extremes() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-sample-rate.mongodb");

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
            r#"{"insert":"widgets","documents":[{"sku":"alpha"},{"sku":"beta"}]}"#,
        ])
        .assert()
        .success();

    let mut find_all = Command::cargo_bin("mqlite").expect("binary");
    let all_output = find_all
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"widgets","filter":{"$sampleRate":1.0}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let all_response: Value = serde_json::from_slice(&all_output).expect("json response");
    let all_batch = all_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert_eq!(all_batch.len(), 2);

    let mut find_none = Command::cargo_bin("mqlite").expect("binary");
    let none_output = find_none
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"find":"widgets","filter":{"$sampleRate":0.0}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let none_response: Value = serde_json::from_slice(&none_output).expect("json response");
    let none_batch = none_response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    assert!(none_batch.is_empty());
}

#[test]
fn command_find_rejects_where_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-where.mongodb");

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
            r#"{"find":"widgets","filter":{"$where":"this.qty > 0"}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["code"], 2);
    assert_eq!(response["codeName"], "BadValue");
    assert!(
        response["errmsg"]
            .as_str()
            .expect("errmsg")
            .contains("unsupported query operator `$where`")
    );
}

#[test]
fn command_find_rejects_function_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-function.mongodb");

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
            r#"{"insert":"widgets","documents":[{"qty":1}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{},"projection":{"out":{"$function":{"body":"function() { return 1; }","args":[],"lang":"js"}}}}"#,
        ])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["code"], 2);
    assert_eq!(response["codeName"], "BadValue");
    assert!(
        response["errmsg"]
            .as_str()
            .expect("errmsg")
            .contains("unsupported query operator `$function`")
    );
}

#[test]
fn command_aggregate_supports_replace_with_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-replace-with.mongodb");

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
            r#"{"insert":"widgets","documents":[{"comments":[{"user_id":"x","comment":"foo"},{"user_id":"y","comment":"bar"}]},{"comments":[{"user_id":"y","comment":"baz"}]}]}"#,
        ])
        .assert()
        .success();

    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"aggregate":"widgets","pipeline":[{"$unwind":"$comments"},{"$replaceWith":"$comments"},{"$group":{"_id":"$user_id","count":{"$sum":1}}},{"$sort":{"count":-1,"_id":1}}],"cursor":{}}"#,
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
    assert_eq!(first_batch.len(), 2);
    assert_eq!(first_batch[0]["_id"], "y");
    assert_eq!(first_batch[0]["count"], 2);
    assert_eq!(first_batch[1]["_id"], "x");
    assert_eq!(first_batch[1]["count"], 1);
}

#[test]
fn command_find_supports_not_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-not.mongodb");

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
            r#"{"insert":"widgets","documents":[{"qty":5},{"qty":12}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"qty":{"$not":{"$gt":5}}}}"#,
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
    assert_eq!(first_batch[0]["qty"], 5);
}

#[test]
fn command_find_supports_type_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-type.mongodb");

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
            r#"{"insert":"widgets","documents":[{"name":"Ada"},{"name":7}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"name":{"$type":"string"}}}"#,
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
    assert_eq!(first_batch[0]["name"], "Ada");
}

#[test]
fn command_find_supports_regex_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-regex.mongodb");

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
            r#"{"insert":"widgets","documents":[{"name":"Ada"},{"name":"bea"}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"name":{"$regex":"^a","$options":"i"}}}"#,
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
    assert_eq!(first_batch[0]["name"], "Ada");
}

#[test]
fn command_find_supports_elem_match_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-elemmatch.mongodb");

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
            r#"{"insert":"widgets","documents":[{"items":[{"qty":1},{"qty":4}]},{"items":[{"qty":2},{"qty":3}]}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"items":{"$elemMatch":{"qty":4}}}}"#,
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
    assert_eq!(first_batch[0]["items"][1]["qty"], 4);
}

#[test]
fn command_find_supports_expr_filter() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-expr.mongodb");

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
            r#"{"insert":"widgets","documents":[{"qty":5,"limit":4},{"qty":2,"limit":4}]}"#,
        ])
        .assert()
        .success();

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
            r#"{"find":"widgets","filter":{"$expr":{"$gt":["$qty","$limit"]}}}"#,
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
    assert_eq!(first_batch[0]["qty"], 5);
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
fn command_explain_uses_persisted_plan_cache_after_restart() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-persisted-plan-cache.mongodb");

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
            r#"{"explain":{"find":"widgets","filter":{"sku":"alpha"}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let first_response: Value = serde_json::from_slice(&first_output).expect("json response");
    assert_eq!(first_response["queryPlanner"]["planCacheUsed"], false);

    thread::sleep(Duration::from_secs(2));

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
            r#"{"explain":{"find":"widgets","filter":{"sku":"alpha"}}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let second_response: Value = serde_json::from_slice(&second_output).expect("json response");
    assert_eq!(second_response["queryPlanner"]["planCacheUsed"], true);
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
fn command_explain_reports_branch_union_or_plan() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-branch-union-or.mongodb");

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
            r#"{"createIndexes":"widgets","indexes":[{"key":{"sku":1},"name":"sku_1","unique":true},{"key":{"qty":1},"name":"qty_1"}]}"#,
        ])
        .assert()
        .success();

    let insert_command = json!({
        "insert": "widgets",
        "documents": [
            { "_id": 1, "sku": "alpha", "qty": 1 },
            { "_id": 2, "sku": "beta", "qty": 10 },
            { "_id": 3, "sku": "gamma", "qty": 7 },
            { "_id": 4, "sku": "delta", "qty": 2 }
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
                    { "sku": "alpha" },
                    { "qty": { "$gt": 5 } }
                ]
            },
            "sort": { "qty": 1 }
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
        "OR"
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["inputStages"]
            .as_array()
            .expect("input stages")
            .len(),
        2
    );
    assert_eq!(
        explain_response["queryPlanner"]["winningPlan"]["requiresSort"],
        true
    );

    let find_command = json!({
        "find": "widgets",
        "filter": {
            "$or": [
                { "sku": "alpha" },
                { "qty": { "$gt": 5 } }
            ]
        },
        "sort": { "qty": 1 }
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
    assert_eq!(skus, vec!["alpha", "gamma", "beta"]);
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

#[test]
fn command_aggregate_change_stream_reports_collection_events() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("change-stream.mongodb");

    for command in [
        json!({ "create": "widgets" }).to_string(),
        json!({ "insert": "widgets", "documents": [{ "_id": 1, "qty": 1 }] }).to_string(),
        json!({
            "update": "widgets",
            "updates": [{ "q": { "_id": 1 }, "u": { "$set": { "qty": 2 } } }]
        })
        .to_string(),
        json!({
            "createIndexes": "widgets",
            "indexes": [{ "name": "qty_1", "key": { "qty": 1 } }]
        })
        .to_string(),
        json!({
            "delete": "widgets",
            "deletes": [{ "q": { "_id": 1 }, "limit": 1 }]
        })
        .to_string(),
    ] {
        let mut apply = Command::cargo_bin("mqlite").expect("binary");
        apply
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
            .arg(&command)
            .assert()
            .success();
    }

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$changeStream": {
                    "fullDocument": "updateLookup",
                    "fullDocumentBeforeChange": "whenAvailable",
                    "showExpandedEvents": true
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "operationType": 1,
                    "documentKey": 1,
                    "fullDocument.qty": 1,
                    "fullDocumentBeforeChange.qty": 1,
                    "operationDescription": 1
                }
            }
        ],
        "cursor": {}
    })
    .to_string();
    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
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
        .arg(&aggregate_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch");
    let operation_types = first_batch
        .iter()
        .map(|document| document["operationType"].as_str().expect("type"))
        .collect::<Vec<_>>();
    assert_eq!(
        operation_types,
        vec!["create", "insert", "update", "createIndexes", "delete"]
    );
    assert_eq!(first_batch[2]["fullDocument"]["qty"], 2);
    assert_eq!(first_batch[2]["fullDocumentBeforeChange"]["qty"], 1);
    assert_eq!(
        first_batch[3]["operationDescription"]["indexes"][0]["name"],
        "qty_1"
    );
}

#[test]
fn command_collectionless_aggregate_supports_change_stream_stage() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("collectionless-change-stream.mongodb");

    for command in [
        json!({ "create": "widgets" }).to_string(),
        json!({ "insert": "widgets", "documents": [{ "_id": 1, "sku": "alpha" }] }).to_string(),
        json!({ "create": "gadgets" }).to_string(),
        json!({ "insert": "gadgets", "documents": [{ "_id": 2, "sku": "beta" }] }).to_string(),
    ] {
        let mut apply = Command::cargo_bin("mqlite").expect("binary");
        apply
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
            .arg(&command)
            .assert()
            .success();
    }

    let aggregate_command = json!({
        "aggregate": 1,
        "pipeline": [
            { "$changeStream": {} },
            { "$project": { "_id": 0, "operationType": 1, "ns.coll": 1 } }
        ],
        "cursor": {}
    })
    .to_string();
    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
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
        .arg(&aggregate_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch");
    let namespaces = first_batch
        .iter()
        .map(|document| document["ns"]["coll"].as_str().expect("collection"))
        .collect::<Vec<_>>();
    assert_eq!(namespaces, vec!["widgets", "gadgets"]);
    assert!(
        first_batch
            .iter()
            .all(|document| document["operationType"] == "insert")
    );
}

#[test]
fn command_change_stream_split_large_event_accepts_change_stream_pipeline() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("change-stream-split.mongodb");

    for command in [
        json!({ "create": "widgets" }).to_string(),
        json!({ "insert": "widgets", "documents": [{ "_id": 1, "sku": "alpha" }] }).to_string(),
    ] {
        let mut apply = Command::cargo_bin("mqlite").expect("binary");
        apply
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
            .arg(&command)
            .assert()
            .success();
    }

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            { "$changeStream": {} },
            { "$project": { "_id": 0, "operationType": 1, "documentKey": 1 } },
            { "$changeStreamSplitLargeEvent": {} }
        ],
        "cursor": {}
    })
    .to_string();
    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
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
        .arg(&aggregate_command)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first_batch = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch");
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0]["operationType"], "insert");
    assert!(first_batch[0].get("splitEvent").is_none());
}

#[test]
fn command_change_stream_split_large_event_rejects_non_change_stream_pipeline() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("change-stream-split-invalid.mongodb");

    let command = json!({
        "aggregate": "widgets",
        "pipeline": [{ "$changeStreamSplitLargeEvent": {} }],
        "cursor": {}
    })
    .to_string();
    let mut aggregate = Command::cargo_bin("mqlite").expect("binary");
    let output = aggregate
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
        .arg(&command)
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json error");
    assert_eq!(response["codeName"], "BadValue");
    assert!(
        response["errmsg"]
            .as_str()
            .expect("errmsg")
            .contains("$changeStreamSplitLargeEvent can only be used in a $changeStream pipeline")
    );
}
