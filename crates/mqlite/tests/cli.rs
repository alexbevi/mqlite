use std::{
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};

use assert_cmd::Command;
use bson::{Binary, Bson, doc, spec::BinarySubtype};
use mqlite_ipc::broker_paths;
use mqlite_storage::DatabaseFile;
use predicates::prelude::PredicateBooleanExt;
use serde_json::{Value, json};
use tempfile::tempdir;

fn assert_json_number_close(value: &Value, expected: f64) {
    let actual = value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .expect("numeric json value");
    assert!(
        (actual - expected).abs() < 1e-12,
        "expected {expected}, got {actual}"
    );
}

fn wait_for_broker_exit(database_path: &Path) {
    let manifest_path = broker_paths(database_path)
        .expect("broker paths")
        .manifest_path;
    let deadline = Instant::now() + Duration::from_secs(5);
    while manifest_path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
}

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

    let mut info = Command::cargo_bin("mqlite").expect("binary");
    info.args(["info", "--file"])
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
fn info_command_reports_current_and_checkpoint_state() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("info-cli.mongodb");

    let mut create = Command::cargo_bin("mqlite").expect("binary");
    create
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--eval",
            r#"{"create":"widgets"}"#,
        ])
        .assert()
        .success();

    let mut create_index = Command::cargo_bin("mqlite").expect("binary");
    create_index
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
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
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":1,"sku":"alpha","qty":2},{"_id":2,"sku":"beta","qty":4}]}"#,
        ])
        .assert()
        .success();

    wait_for_broker_exit(&database_path);

    let mut checkpoint = Command::cargo_bin("mqlite").expect("binary");
    checkpoint
        .args(["checkpoint", "--file"])
        .arg(&database_path)
        .assert()
        .success();

    let mut insert_after_checkpoint = Command::cargo_bin("mqlite").expect("binary");
    insert_after_checkpoint
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":3,"sku":"gamma","qty":6}]}"#,
        ])
        .assert()
        .success();

    let output = {
        let mut info = Command::cargo_bin("mqlite").expect("binary");
        info.args(["info", "--file"])
            .arg(&database_path)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone()
    };

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["summary"]["database_count"], 1);
    assert_eq!(response["summary"]["collection_count"], 1);
    assert_eq!(response["summary"]["index_count"], 2);
    assert_eq!(response["summary"]["record_count"], 3);
    assert_eq!(response["summary"]["index_entry_count"], 6);
    assert_eq!(response["last_applied_sequence"], 4);
    assert_eq!(response["last_checkpoint"]["last_applied_sequence"], 3);
    assert_eq!(response["last_checkpoint"]["record_count"], 2);
    assert_eq!(response["last_checkpoint"]["index_entry_count"], 4);
    assert_eq!(response["wal_since_checkpoint"]["record_count"], 1);

    let databases = response["databases"].as_array().expect("databases");
    let app = databases
        .iter()
        .find(|database| database["name"] == "app")
        .expect("app database");
    let collections = app["collections"].as_array().expect("collections");
    let widgets = collections
        .iter()
        .find(|collection| collection["name"] == "widgets")
        .expect("widgets collection");
    assert_eq!(widgets["document_count"], 3);
    assert_eq!(widgets["checkpoint"]["record_count"], 2);

    let indexes = widgets["indexes"].as_array().expect("indexes");
    let sku_index = indexes
        .iter()
        .find(|index| index["name"] == "sku_1")
        .expect("sku_1 index");
    assert_eq!(sku_index["unique"], true);
    assert_eq!(sku_index["entry_count"], 3);
    assert_eq!(sku_index["checkpoint"]["entry_count"], 2);
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
fn command_auto_spawned_broker_checkpoints_and_exits_when_launcher_process_ends() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("launcher-owned.mongodb");
    let manifest_path = broker_paths(&database_path)
        .expect("broker paths")
        .manifest_path;

    let mut create = Command::cargo_bin("mqlite").expect("binary");
    create
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "60",
            "--eval",
            r#"{"insert":"widgets","documents":[{"_id":1,"sku":"alpha"}]}"#,
        ])
        .assert()
        .success();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while manifest_path.exists() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }

    assert!(
        !manifest_path.exists(),
        "auto-spawned broker should exit after the launcher process ends"
    );

    let inspect = DatabaseFile::inspect(&database_path).expect("inspect after launcher exit");
    assert_eq!(inspect.current_record_count, 1);
    assert_eq!(inspect.wal_records_since_checkpoint, 0);
}

#[test]
fn command_reports_broker_startup_failure_for_invalid_existing_file() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-existing.mongodb");
    fs::write(&database_path, b"not-a-valid-mqlite-file").expect("write invalid file");

    let mut command = Command::cargo_bin("mqlite").expect("binary");
    command
        .args([
            "command",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--idle-shutdown-secs",
            "1",
            "--eval",
            r#"{"ping":1}"#,
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "mqlite broker exited before writing its manifest",
        ))
        .stderr(predicates::str::contains("supported v2 mqlite database"))
        .stderr(predicates::str::contains("timed out waiting for mqlite broker manifest").not());
}

#[test]
fn bench_command_reports_write_and_read_metrics() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("bench.mongodb");

    let mut bench = Command::cargo_bin("mqlite").expect("binary");
    let output = bench
        .args([
            "bench",
            "--file",
            database_path.to_str().expect("path"),
            "--db",
            "app",
            "--collection-prefix",
            "quick",
            "--writes",
            "3",
            "--reads",
            "3",
            "--write-batch-size",
            "2",
            "--index-field",
            "sku",
            "--unique-index",
            "--idle-shutdown-secs",
            "1",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["db"], "app");
    let collection_name = response["collection"].as_str().expect("collection");
    assert!(collection_name.starts_with("quick_"));
    assert_eq!(response["index"]["field"], "sku");
    assert_eq!(response["index"]["unique"], true);
    assert_eq!(response["writes"]["documents"], 3);
    assert_eq!(response["writes"]["commands"], 2);
    assert_eq!(response["writes"]["batchSize"], 2);
    assert_eq!(response["reads"]["documents"], 3);
    assert_eq!(response["reads"]["commands"], 3);
    assert_eq!(response["reads"]["batchSize"], 1);
    assert_eq!(response["totals"]["documents"], 6);
    assert!(
        response["writes"]["elapsedMs"]
            .as_f64()
            .expect("write elapsed")
            >= 0.0
    );
    assert!(
        response["reads"]["elapsedMs"]
            .as_f64()
            .expect("read elapsed")
            >= 0.0
    );

    let mut list_indexes = Command::cargo_bin("mqlite").expect("binary");
    let output = list_indexes
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
        .arg(format!(r#"{{"listIndexes":"{collection_name}"}}"#))
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let indexes = response["cursor"]["firstBatch"]
        .as_array()
        .expect("firstBatch");
    let sku_index = indexes
        .iter()
        .find(|index| index["name"] == "sku_1_unique")
        .expect("sku unique index");
    assert_eq!(sku_index["key"]["sku"], 1);
    assert_eq!(sku_index["unique"], true);
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
fn command_aggregate_project_supports_meta_expression() {
    const EXPECTED_DISTANCE: f64 = 111_319.490_793_273_57;
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("command-meta-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"name":"north","loc":{"type":"Point","coordinates":[0.0,1.0]}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$geoNear":{"near":{"type":"Point","coordinates":[0.0,0.0]},"key":"loc","distanceField":"dist","includeLocs":"matchedLoc","spherical":true}},{"$project":{"_id":0,"dist":1,"matchedLoc":1,"distMeta":{"$meta":"geoNearDistance"},"pointMeta":{"$meta":"geoNearPoint"}}}],"cursor":{}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("document");
    assert_json_number_close(&first["dist"], EXPECTED_DISTANCE);
    assert_json_number_close(&first["distMeta"], EXPECTED_DISTANCE);
    assert_eq!(
        first["matchedLoc"],
        json!({ "type": "Point", "coordinates": [0.0, 1.0] })
    );
    assert_eq!(
        first["pointMeta"],
        json!({ "type": "Point", "coordinates": [0.0, 1.0] })
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_meta_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-meta-expression-invalid.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$meta":"searchScore"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_to_hashed_index_key_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-hashed-index-key-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"hashThisStringLiteral","number":123,"ts":{"$timestamp":{"t":0,"i":0}},"oid":{"$oid":"47cc67093475061e3d95369d"},"date":{"$date":"1970-01-01T00:00:00.000Z"}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"stringHash":{"$toHashedIndexKey":"$value"},"numberHash":{"$toHashedIndexKey":"$number"},"timestampHash":{"$toHashedIndexKey":"$ts"},"objectIdHash":{"$toHashedIndexKey":"$oid"},"dateHash":{"$toHashedIndexKey":"$date"},"missingHash":{"$toHashedIndexKey":"$missingField"},"nullHash":{"$toHashedIndexKey":null},"expressionHash":{"$toHashedIndexKey":{"$pow":[2,4]}},"undefinedHash":{"$toHashedIndexKey":{"$literal":{"$undefined":true}}}}}],"cursor":{}}"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("document");
    assert_eq!(
        first["stringHash"].as_i64(),
        Some(-5_776_344_739_422_278_694)
    );
    assert_eq!(
        first["numberHash"].as_i64(),
        Some(-6_548_868_637_522_515_075)
    );
    assert_eq!(
        first["timestampHash"].as_i64(),
        Some(-7_867_208_682_377_458_672)
    );
    assert_eq!(
        first["objectIdHash"].as_i64(),
        Some(1_576_265_281_381_834_298)
    );
    assert_eq!(first["dateHash"].as_i64(), Some(-1_178_696_894_582_842_035));
    assert_eq!(
        first["missingHash"].as_i64(),
        Some(2_338_878_944_348_059_895)
    );
    assert_eq!(first["nullHash"].as_i64(), Some(2_338_878_944_348_059_895));
    assert_eq!(
        first["expressionHash"].as_i64(),
        Some(2_598_032_665_634_823_220)
    );
    assert_eq!(
        first["undefinedHash"].as_i64(),
        Some(40_158_834_000_849_533)
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_to_hashed_index_key_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("command-hashed-index-key-expression-invalid.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"hashThisStringLiteral"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$toHashedIndexKey":["$value",1]}}}],"cursor":{}}"#,
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
                    "type": { "$type": "$left" },
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
    assert_eq!(first_batch[0]["type"], "long");
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
fn command_aggregate_project_supports_scoped_and_field_access_expressions() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("scoped-expression-operators.mongodb");

    let insert_command = json!({
        "insert": "widgets",
        "documents": [{
            "_id": 1,
            "simple": [1, 2, 3, 4],
            "nested": [{ "a": 1 }, { "a": 2 }],
            "mixed": [{ "a": 1 }, {}, { "a": 2 }, { "a": null }],
            "nestedDoc": { "four": 4 },
            "lookupField": "a.b",
            "a.b": "literal",
            "special": { "$price": 5 }
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
                    "getFieldDynamic": { "$getField": "$lookupField" },
                    "getFieldObject": { "$getField": { "field": { "$const": "$price" }, "input": "$special" } }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "mapped": [11, 12, 13, 14],
            "mappedCurrent": [1, 2],
            "mappedMixed": [1, null, 2, null],
            "filtered": [3, 4],
            "filteredDefault": [2],
            "letValue": 14,
            "getFieldDynamic": "literal",
            "getFieldObject": 5
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_scoped_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-scoped-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"items":[1,2]}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$map":{"input":"$items","as":"value","in":"$$this"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_reduce_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("reduce-expression.mongodb");

    let insert_command = json!({
        "insert": "widgets",
        "documents": [
            {
                "_id": 1,
                "array": [1, 2, 3],
                "nested": [[1, 2, 3], [4, 5]]
            }
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

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "sum": {
                        "$reduce": {
                            "input": "$array",
                            "initialValue": 0,
                            "in": { "$add": ["$$value", "$$this"] }
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
                    }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "sum": 6,
            "nestedReduce": 54
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_reduce_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-reduce-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"items":[1,2]}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$reduce":{"input":"$$value","initialValue":[],"in":[]}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_switch_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("switch-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"flag":true}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
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
                    }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "firstMatch": "one is equal to one!",
            "defaulted": "no case matched."
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_switch_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-switch-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"x":1}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$switch":{"branches":[{"case":{"$eq":["$x",0]},"then":1}]}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_set_expressions() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("set-expressions.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"arr1":[1,2,3,2,1],"arr2":[2,3,4,3]}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "union": { "$setUnion": ["$arr1", "$arr2"] },
                    "intersection": { "$setIntersection": ["$arr1", "$arr2"] },
                    "difference": { "$setDifference": ["$arr1", "$arr2"] },
                    "equals": { "$setEquals": ["$arr1", [1, 2, 3, 2]] },
                    "isSubset": { "$setIsSubset": [[2, 3], "$arr2"] }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "union": [1, 2, 3, 4],
            "intersection": [2, 3],
            "difference": [1],
            "equals": true,
            "isSubset": true
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_set_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-set-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"arr":"nope"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$setUnion":["$arr",[1,2,3]]}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_case_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("case-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"text":"aBz","nested":{"str":"hello world"},"unicode":"D€"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "upper": { "$toUpper": "$text" },
                    "lower": { "$toLower": ["$text"] },
                    "fieldUpper": { "$toUpper": "$nested.str" },
                    "strcasecmpEqual": { "$strcasecmp": ["Ab", "aB"] },
                    "unicodeLower": { "$toLower": "$unicode" }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "upper": "ABZ",
            "lower": "abz",
            "fieldUpper": "HELLO WORLD",
            "strcasecmpEqual": 0,
            "unicodeLower": "d€"
        })]
    );
}

#[test]
fn command_aggregate_project_supports_bitwise_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("bitwise-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"a":3,"b":5}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "bitAnd": { "$bitAnd": ["$a", "$b"] },
                    "bitOr": { "$bitOr": ["$a", "$b"] },
                    "bitXor": { "$bitXor": ["$a", "$b"] },
                    "bitNot": { "$bitNot": "$a" }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "bitAnd": 1,
            "bitOr": 7,
            "bitXor": 6,
            "bitNot": -4
        })]
    );
}

#[test]
fn command_aggregate_project_supports_string_length_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("string-length-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"ascii":"MyString","multi":"é","emoji":"🧐🤓😎🥸🤩"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "asciiBytes": { "$strLenBytes": "$ascii" },
                    "asciiCp": { "$strLenCP": "$ascii" },
                    "multiBytes": { "$strLenBytes": "$multi" },
                    "multiCp": { "$strLenCP": "$multi" },
                    "emojiBytes": { "$strLenBytes": "$emoji" },
                    "emojiCp": { "$strLenCP": "$emoji" }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "asciiBytes": 8,
            "asciiCp": 8,
            "multiBytes": 2,
            "multiCp": 1,
            "emojiBytes": 20,
            "emojiCp": 5
        })]
    );
}

#[test]
fn command_aggregate_project_supports_string_position_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("string-position-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"ascii":"foobar foobar","utf8Bytes":"a∫∫b","utf8CodePoints":"cafétéria","empty":""}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "asciiBytes": { "$indexOfBytes": ["$ascii", "bar"] },
                    "asciiBytesFrom": { "$indexOfBytes": ["$ascii", "bar", 5] },
                    "utf8Bytes": { "$indexOfBytes": ["$utf8Bytes", "b", 6] },
                    "utf8Cp": { "$indexOfCP": ["$utf8CodePoints", "é"] },
                    "utf8CpFrom": { "$indexOfCP": ["$utf8CodePoints", "é", 4] },
                    "emptyCp": { "$indexOfCP": ["$empty", ""] }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "asciiBytes": 3,
            "asciiBytesFrom": 10,
            "utf8Bytes": 7,
            "utf8Cp": 3,
            "utf8CpFrom": 5,
            "emptyCp": 0
        })]
    );
}

#[test]
fn command_aggregate_project_supports_substring_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("substring-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"ascii":"abcd","utf8":"éclair","wide":"寿司sushi"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "asciiBytes": { "$substrBytes": ["$ascii", 1, 2] },
                    "utf8Bytes": { "$substrBytes": ["$utf8", 0, 4] },
                    "utf8Cp": { "$substrCP": ["$utf8", 0, 4] },
                    "wideCp": { "$substrCP": ["$wide", 0, 6] },
                    "aliasRest": { "$substr": ["$ascii", 2, -1] }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "asciiBytes": "bc",
            "utf8Bytes": "écl",
            "utf8Cp": "écla",
            "wideCp": "寿司sush",
            "aliasRest": "cd"
        })]
    );
}

#[test]
fn command_aggregate_project_supports_size_introspection_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("size-introspection-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"text":"éclair","bin":{"$binary":{"base64":"AQIDBA==","subType":"00"}}}]}"#,
        ])
        .assert()
        .success();

    let current = doc! {
        "_id": 1_i64,
        "text": "éclair",
        "bin": Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![1, 2, 3, 4]
        })
    };
    let expected_bson_size = bson::to_vec(&current)
        .expect("encode current document")
        .len() as i64;

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "textBytes": { "$binarySize": "$text" },
                    "binBytes": { "$binarySize": "$bin" },
                    "docBytes": { "$bsonSize": "$$CURRENT" }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "textBytes": 7,
            "binBytes": 4,
            "docBytes": expected_bson_size
        })]
    );
}

#[test]
fn command_aggregate_project_supports_math_and_trigonometric_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("math-trigonometric-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"zero":0,"one":1,"two":2,"four":4,"eight":8,"thousand":1000,"degrees":180}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "expZero": { "$exp": "$zero" },
                    "lnOne": { "$ln": "$one" },
                    "logBaseTwo": { "$log": ["$eight", "$two"] },
                    "logTen": { "$log10": "$thousand" },
                    "pow": { "$pow": ["$two", 3] },
                    "sqrt": { "$sqrt": "$four" },
                    "cosZero": { "$cos": "$zero" },
                    "sinZero": { "$sin": "$zero" },
                    "tanZero": { "$tan": "$zero" },
                    "acosOne": { "$acos": "$one" },
                    "asinZero": { "$asin": "$zero" },
                    "atanZero": { "$atan": "$zero" },
                    "atan2ZeroOne": { "$atan2": ["$zero", "$one"] },
                    "acoshOne": { "$acosh": "$one" },
                    "asinhZero": { "$asinh": "$zero" },
                    "atanhZero": { "$atanh": "$zero" },
                    "coshZero": { "$cosh": "$zero" },
                    "sinhZero": { "$sinh": "$zero" },
                    "tanhZero": { "$tanh": "$zero" },
                    "degToRad": { "$degreesToRadians": "$degrees" },
                    "radToDeg": { "$radiansToDegrees": std::f64::consts::PI }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["expZero"], 1);
    assert_eq!(first["lnOne"], 0);
    assert_eq!(first["logBaseTwo"], 3);
    assert_eq!(first["logTen"], 3);
    assert_eq!(first["pow"], 8);
    assert_eq!(first["sqrt"], 2);
    assert_eq!(first["cosZero"], 1);
    assert_eq!(first["sinZero"], 0);
    assert_eq!(first["tanZero"], 0);
    assert_eq!(first["acosOne"], 0);
    assert_eq!(first["asinZero"], 0);
    assert_eq!(first["atanZero"], 0);
    assert_eq!(first["atan2ZeroOne"], 0);
    assert_eq!(first["acoshOne"], 0);
    assert_eq!(first["asinhZero"], 0);
    assert_eq!(first["atanhZero"], 0);
    assert_eq!(first["coshZero"], 1);
    assert_eq!(first["sinhZero"], 0);
    assert_eq!(first["tanhZero"], 0);
    assert_json_number_close(&first["degToRad"], std::f64::consts::PI);
    assert_json_number_close(&first["radToDeg"], 180.0);
}

#[test]
fn command_aggregate_project_supports_split_and_replace_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("split-replace-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"abc->defg->hij","dotted":"a.b.c","unicode":"e\u0301","precomposed":"é"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "splitArrow": { "$split": ["$value", "->"] },
                    "splitUnicode": { "$split": ["$unicode", "e"] },
                    "splitNoMatch": { "$split": ["$precomposed", "e"] },
                    "replaceOne": { "$replaceOne": { "input": "$value", "find": "->", "replacement": "." } },
                    "replaceAll": { "$replaceAll": { "input": "$value", "find": "->", "replacement": "." } }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "splitArrow": ["abc", "defg", "hij"],
            "splitUnicode": ["", "\u{0301}"],
            "splitNoMatch": ["é"],
            "replaceOne": "abc.defg->hij",
            "replaceAll": "abc.defg.hij"
        })]
    );
}

#[test]
fn command_aggregate_project_supports_regex_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("regex-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"text":"Simple Example","unicode":"cafétéria","mixed":"Camel Case","dynamicRegex":{"$regularExpression":{"pattern":"(té)","options":""}}}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "firstMatch": { "$regexFind": { "input": "$text", "regex": "(m(p))" } },
                    "allMatches": { "$regexFindAll": { "input": "$unicode", "regex": "$dynamicRegex" } },
                    "caseInsensitive": { "$regexMatch": { "input": "$mixed", "regex": "camel", "options": "i" } }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "firstMatch": { "match": "mp", "idx": 2, "captures": ["mp", "p"] },
            "allMatches": [{ "match": "té", "idx": 4, "captures": ["té"] }],
            "caseInsensitive": true
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_regex_expression_inputs() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("regex-expression-invalid.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"text":"abc"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "out": {
                        "$regexFind": {
                            "input": "$text",
                            "regex": {
                                "$regularExpression": {
                                    "pattern": "abc",
                                    "options": "i"
                                }
                            },
                            "options": "m"
                        }
                    }
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
        .failure()
        .get_output()
        .stdout
        .clone();

    let response: Value = serde_json::from_slice(&output).expect("json response");
    assert_eq!(response["ok"], 0.0);
    assert_eq!(response["codeName"], "BadValue");
}

#[test]
fn command_aggregate_project_supports_utility_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("utility-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"ts":{"$timestamp":{"t":42,"i":7}},"nums":[2,1,3],"letters":["a","b"],"docs":[{"a":2},{"a":1}]}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "rand": { "$rand": {} },
                    "tsSecond": { "$tsSecond": "$ts" },
                    "tsIncrement": { "$tsIncrement": "$ts" },
                    "sortForward": { "$sortArray": { "input": "$nums", "sortBy": 1 } },
                    "zipDefaults": { "$zip": { "inputs": ["$nums", "$letters"], "defaults": [0, "?"], "useLongestLength": true } }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    let rand = first["rand"].as_f64().expect("rand result");
    assert!(rand >= 0.0);
    assert!(rand < 1.0);
    assert_eq!(first["tsSecond"], 42);
    assert_eq!(first["tsIncrement"], 7);
    assert_eq!(first["sortForward"], json!([1, 2, 3]));
    assert_eq!(first["zipDefaults"], json!([[2, "a"], [1, "b"], [3, "?"]]));
}

#[test]
fn command_aggregate_project_supports_conversion_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("convert-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"intText":"1","decimalText":"1.5","dateText":"1970-01-01T00:00:00.001Z","oidText":"0123456789abcdef01234567"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "convertInt": { "$convert": { "input": "$intText", "to": "int", "onError": 0, "onNull": -1 } },
                    "convertFallback": { "$convert": { "input": "bad", "to": "int", "onError": "fallback" } },
                    "convertNull": { "$convert": { "input": "$missing", "to": "int", "onNull": "nullish" } },
                    "toBool": { "$toBool": 0 },
                    "toDouble": { "$toDouble": "$decimalText" },
                    "toDecimalString": { "$toString": { "$toDecimal": "$decimalText" } },
                    "toDateString": { "$toString": { "$toDate": "$dateText" } },
                    "toObjectIdString": { "$toString": { "$toObjectId": "$oidText" } }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["convertInt"], 1);
    assert_eq!(first["convertFallback"], "fallback");
    assert_eq!(first["convertNull"], "nullish");
    assert_eq!(first["toBool"], false);
    assert_json_number_close(&first["toDouble"], 1.5);
    assert_eq!(first["toDecimalString"], "1.5");
    assert_eq!(first["toDateString"], "1970-01-01T00:00:00.001Z");
    assert_eq!(first["toObjectIdString"], "0123456789abcdef01234567");
}

#[test]
fn command_aggregate_project_supports_accumulator_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("accumulator-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"mixed":[1,2,3,"string",null],"nested":[[1,2,3],1,null],"value":5,"text":"hello"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "avgArray": { "$avg": "$mixed" },
                    "sumArray": { "$sum": "$mixed" },
                    "sumArgs": { "$sum": ["$value", 2, 3, { "$sum": [4, 5] }] },
                    "minArray": { "$min": "$mixed" },
                    "maxArray": { "$max": "$mixed" },
                    "minNested": { "$min": "$nested" },
                    "maxNested": { "$max": "$nested" }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_json_number_close(&first["avgArray"], 2.0);
    assert_eq!(first["sumArray"], 6);
    assert_eq!(first["sumArgs"], 19);
    assert_eq!(first["minArray"], 1);
    assert_eq!(first["maxArray"], "string");
    assert_eq!(first["minNested"], 1);
    assert_eq!(first["maxNested"], json!([1, 2, 3]));
}

#[test]
fn command_aggregate_project_supports_statistical_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("statistical-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"array":[0,"non-numeric",1,2],"continuous":[0,1,2],"twoValues":[0,2],"scalar":7,"std":[1,2,3],"argA":1,"argB":"skip","argC":3,"noNumeric":["non-numeric",[1,2,3]]}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "percentileDiscrete": {
                        "$percentile": {
                            "input": "$array",
                            "p": [0.5, 0.9, 0.1],
                            "method": "discrete"
                        }
                    },
                    "percentileContinuous": {
                        "$let": {
                            "vars": { "ps": [0.1, 0.5, 0.9] },
                            "in": {
                                "$percentile": {
                                    "input": "$continuous",
                                    "p": "$$ps",
                                    "method": "continuous"
                                }
                            }
                        }
                    },
                    "medianDiscrete": { "$median": { "input": "$array", "method": "discrete" } },
                    "medianContinuous": { "$median": { "input": "$twoValues", "method": "continuous" } },
                    "stdDevPopArray": { "$stdDevPop": "$std" },
                    "stdDevSampArray": { "$stdDevSamp": "$std" }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    let percentile_discrete = first["percentileDiscrete"]
        .as_array()
        .expect("percentile array");
    assert_json_number_close(&percentile_discrete[0], 1.0);
    assert_json_number_close(&percentile_discrete[1], 2.0);
    assert_json_number_close(&percentile_discrete[2], 0.0);
    let percentile_continuous = first["percentileContinuous"]
        .as_array()
        .expect("percentile array");
    assert_json_number_close(&percentile_continuous[0], 0.2);
    assert_json_number_close(&percentile_continuous[1], 1.0);
    assert_json_number_close(&percentile_continuous[2], 1.8);
    assert_json_number_close(&first["medianDiscrete"], 1.0);
    assert_json_number_close(&first["medianContinuous"], 1.0);
    assert_json_number_close(&first["stdDevPopArray"], 0.816496580927726);
    assert_json_number_close(&first["stdDevSampArray"], 1.0);
}

#[test]
fn command_aggregate_project_rejects_invalid_statistical_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-statistical-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"array":[1,2,3]}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$percentile":{"input":"$array","p":[0.5,2],"method":"continuous"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_n_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("n-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"a":[1,2,3,5,7,9],"n":4,"diff":2,"nullable":[null,2,null,1]}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "minStatic": { "$minN": { "n": 3, "input": [5, 4, 3, 2, 1] } },
                    "maxStatic": { "$maxN": { "n": 3, "input": [5, 4, 3, 2, 1] } },
                    "firstStatic": { "$firstN": { "n": 3, "input": [5, 4, 3, 2, 1] } },
                    "lastStatic": { "$lastN": { "n": 3, "input": [5, 4, 3, 2, 1] } },
                    "minField": { "$minN": { "n": 3, "input": "$a" } },
                    "maxField": { "$maxN": { "n": "$n", "input": "$a" } },
                    "firstExprN": { "$firstN": { "n": { "$subtract": ["$n", "$diff"] }, "input": [3, 4, 5] } },
                    "lastField": { "$lastN": { "n": "$n", "input": "$a" } },
                    "minSkipsNullish": { "$minN": { "n": 3, "input": "$nullable" } },
                    "maxSkipsNullish": { "$maxN": { "n": 3, "input": "$nullable" } }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["minStatic"], json!([1, 2, 3]));
    assert_eq!(first["maxStatic"], json!([5, 4, 3]));
    assert_eq!(first["firstStatic"], json!([5, 4, 3]));
    assert_eq!(first["lastStatic"], json!([3, 2, 1]));
    assert_eq!(first["minField"], json!([1, 2, 3]));
    assert_eq!(first["maxField"], json!([9, 7, 5, 3]));
    assert_eq!(first["firstExprN"], json!([3, 4]));
    assert_eq!(first["lastField"], json!([3, 5, 7, 9]));
    assert_eq!(first["minSkipsNullish"], json!([1, 2]));
    assert_eq!(first["maxSkipsNullish"], json!([2, 1]));
}

#[test]
fn command_aggregate_project_supports_date_part_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("date-part-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2017-06-19T15:13:25.713Z"},"timestamp":{"$timestamp":{"t":1497885205,"i":1}},"timezone":"America/New_York","offset":"+02:00"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "year": { "$year": "$date" },
                    "month": { "$month": "$date" },
                    "dayOfMonth": { "$dayOfMonth": ["$date"] },
                    "dayOfWeek": { "$dayOfWeek": "$date" },
                    "dayOfYear": { "$dayOfYear": "$date" },
                    "hourTz": { "$hour": { "date": "$date", "timezone": "$timezone" } },
                    "hourOffset": { "$hour": { "date": "$date", "timezone": "$offset" } },
                    "isoDayOfWeek": { "$isoDayOfWeek": "$date" },
                    "isoWeek": { "$isoWeek": "$date" },
                    "isoWeekYear": { "$isoWeekYear": "$date" },
                    "millisecond": { "$millisecond": "$date" },
                    "minute": { "$minute": "$date" },
                    "second": { "$second": "$timestamp" },
                    "week": { "$week": "$date" },
                    "nullTimezone": { "$year": { "date": "$date", "timezone": "$missing" } },
                    "nullDate": { "$month": "$missingDate" }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["year"], 2017);
    assert_eq!(first["month"], 6);
    assert_eq!(first["dayOfMonth"], 19);
    assert_eq!(first["dayOfWeek"], 2);
    assert_eq!(first["dayOfYear"], 170);
    assert_eq!(first["hourTz"], 11);
    assert_eq!(first["hourOffset"], 17);
    assert_eq!(first["isoDayOfWeek"], 1);
    assert_eq!(first["isoWeek"], 25);
    assert_eq!(first["isoWeekYear"], 2017);
    assert_eq!(first["millisecond"], 713);
    assert_eq!(first["minute"], 13);
    assert_eq!(first["second"], 25);
    assert_eq!(first["week"], 25);
    assert_eq!(first["nullTimezone"], Value::Null);
    assert_eq!(first["nullDate"], Value::Null);
}

#[test]
fn command_aggregate_project_supports_date_arithmetic_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("date-arithmetic-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"},"monthEnd":{"$date":"2020-07-14T12:34:56.789Z"},"weekEnd":{"$date":"2020-05-28T08:00:00.000Z"}}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "addedDaysMs": { "$toLong": { "$dateAdd": { "startDate": "$date", "unit": "day", "amount": 2 } } },
                    "subtractedMonthsMs": { "$toLong": { "$dateSubtract": { "startDate": "$date", "unit": "month", "amount": 2 } } },
                    "diffWeeks": { "$dateDiff": { "startDate": "$date", "endDate": "$weekEnd", "unit": "week", "startOfWeek": "thursday" } },
                    "diffMonths": { "$dateDiff": { "startDate": "$date", "endDate": "$monthEnd", "unit": "month" } },
                    "truncatedHourMs": { "$toLong": { "$dateTrunc": { "date": "$date", "unit": "hour" } } },
                    "truncatedWeekMs": { "$toLong": { "$dateTrunc": { "date": "$date", "unit": "week", "startOfWeek": "monday" } } },
                    "nullTimezone": { "$dateAdd": { "startDate": "$date", "unit": "day", "amount": 1, "timezone": "$missingTz" } }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["addedDaysMs"], 1_589_632_496_789_i64);
    assert_eq!(first["subtractedMonthsMs"], 1_584_189_296_789_i64);
    assert_eq!(first["diffWeeks"], 2);
    assert_eq!(first["diffMonths"], 2);
    assert_eq!(first["truncatedHourMs"], 1_589_457_600_000_i64);
    assert_eq!(first["truncatedWeekMs"], 1_589_155_200_000_i64);
    assert_eq!(first["nullTimezone"], Value::Null);
}

#[test]
fn command_aggregate_project_supports_date_parts_conversion_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("date-parts-conversion-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"},"timezone":"America/New_York"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "fromPartsMs": {
                        "$toLong": {
                            "$dateFromParts": {
                                "year": 2020,
                                "month": 5,
                                "day": 14,
                                "hour": 12,
                                "minute": 34,
                                "second": 56,
                                "millisecond": 789
                            }
                        }
                    },
                    "toParts": { "$dateToParts": { "date": "$date" } },
                    "toIsoParts": { "$dateToParts": { "date": "$date", "iso8601": true } },
                    "toPartsTz": { "$dateToParts": { "date": "$date", "timezone": "$timezone" } },
                    "nullTimezone": { "$dateToParts": { "date": "$date", "timezone": "$missingTz" } }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["fromPartsMs"], 1_589_459_696_789_i64);
    assert_eq!(
        first["toParts"],
        json!({
            "year": 2020,
            "month": 5,
            "day": 14,
            "hour": 12,
            "minute": 34,
            "second": 56,
            "millisecond": 789
        })
    );
    assert_eq!(
        first["toIsoParts"],
        json!({
            "isoWeekYear": 2020,
            "isoWeek": 20,
            "isoDayOfWeek": 4,
            "hour": 12,
            "minute": 34,
            "second": 56,
            "millisecond": 789
        })
    );
    assert_eq!(
        first["toPartsTz"],
        json!({
            "year": 2020,
            "month": 5,
            "day": 14,
            "hour": 8,
            "minute": 34,
            "second": 56,
            "millisecond": 789
        })
    );
    assert_eq!(first["nullTimezone"], Value::Null);
}

#[test]
fn command_aggregate_project_supports_date_string_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("date-string-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"},"timezone":"America/New_York"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "fromStringMs": { "$toLong": { "$dateFromString": { "dateString": "2020-05-14T12:34:56.789Z" } } },
                    "fromStringTzMs": {
                        "$toLong": {
                            "$dateFromString": {
                                "dateString": "2020/05/14 08:34:56",
                                "format": "%Y/%m/%d %H:%M:%S",
                                "timezone": "$timezone"
                            }
                        }
                    },
                    "fromStringOnNull": {
                        "$dateFromString": {
                            "dateString": "$missingDateString",
                            "onNull": "fallback"
                        }
                    },
                    "fromStringOnError": {
                        "$dateFromString": {
                            "dateString": "not a date",
                            "onError": "invalid"
                        }
                    },
                    "toStringDefault": { "$dateToString": { "date": "$date" } },
                    "toStringTz": {
                        "$dateToString": {
                            "date": "$date",
                            "timezone": "$timezone",
                            "format": "%Y-%m-%d %H:%M:%S %z (%Z minutes)"
                        }
                    },
                    "toStringIso": {
                        "$dateToString": {
                            "date": "$date",
                            "format": "%G-W%V-%u"
                        }
                    },
                    "toStringMonth": {
                        "$dateToString": {
                            "date": "$date",
                            "format": "%b (%B) %d, %Y"
                        }
                    },
                    "nullTimezone": {
                        "$dateToString": {
                            "date": "2020-05-14T12:34:56.789Z",
                            "timezone": "$missingTz"
                        }
                    },
                    "nullFormat": {
                        "$dateFromString": {
                            "dateString": "2020-05-14T12:34:56.789Z",
                            "format": "$missingFormat"
                        }
                    }
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
    let first = response["cursor"]["firstBatch"]
        .as_array()
        .expect("first batch")
        .first()
        .expect("projected document");
    assert_eq!(first["fromStringMs"], 1_589_459_696_789_i64);
    assert_eq!(first["fromStringTzMs"], 1_589_459_696_000_i64);
    assert_eq!(first["fromStringOnNull"], "fallback");
    assert_eq!(first["fromStringOnError"], "invalid");
    assert_eq!(first["toStringDefault"], "2020-05-14T12:34:56.789Z");
    assert_eq!(
        first["toStringTz"],
        "2020-05-14 08:34:56 -0400 (-240 minutes)"
    );
    assert_eq!(first["toStringIso"], "2020-W20-4");
    assert_eq!(first["toStringMonth"], "May (May) 14, 2020");
    assert_eq!(first["nullTimezone"], Value::Null);
    assert_eq!(first["nullFormat"], Value::Null);
}

#[test]
fn command_aggregate_project_supports_trim_expression_operators() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("trim-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"defaultWhitespace":" \u2001\u2002Odd unicode indentation\u200A ","customChars":"xXtrimXx","leftChars":"xyztrimzy","rightChars":"xyztrimzy"}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "trimmed": { "$trim": { "input": "$defaultWhitespace" } },
                    "leftTrimmed": { "$ltrim": { "input": "$defaultWhitespace" } },
                    "rightTrimmed": { "$rtrim": { "input": "$defaultWhitespace" } },
                    "customSet": { "$trim": { "input": "$customChars", "chars": "x" } },
                    "leftCustomSet": { "$ltrim": { "input": "$leftChars", "chars": "xyz" } },
                    "rightCustomSet": { "$rtrim": { "input": "$rightChars", "chars": "xyz" } }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "trimmed": "Odd unicode indentation",
            "leftTrimmed": "Odd unicode indentation\u{200A} ",
            "rightTrimmed": " \u{2001}\u{2002}Odd unicode indentation",
            "customSet": "XtrimX",
            "leftCustomSet": "trimzy",
            "rightCustomSet": "xyztrim"
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_utility_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-utility-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":5}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$tsSecond":"$value"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_n_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-n-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":[1,2,3]}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$firstN":{"n":0,"input":"$value"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_date_part_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-date-part-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2017-06-19T15:13:25.713Z"}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$hour":{"date":"$date","timezone":"DoesNot/Exist"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_date_arithmetic_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-date-arithmetic-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$dateAdd":{"startDate":"$date","unit":"century","amount":1}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_date_parts_conversion_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-date-parts-conversion-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$dateToParts":{"date":"$date","iso8601":"yes"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_date_string_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-date-string-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"date":{"$date":"2020-05-14T12:34:56.789Z"}}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$dateFromString":{"dateString":"2020-05-14","format":"%n"}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_conversion_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-convert-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"abc"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$convert":{"input":"$value","to":"int","base":16}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_split_and_replace_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-split-replace-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"abc"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$split":["$value",""]}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_math_and_trigonometric_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-math-trigonometric-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":-1}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$sqrt":"$value"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_case_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-case-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"flag":true}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$toUpper":"$flag"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_trim_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-trim-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"abc"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$trim":{"input":"$value","chars":5}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_string_length_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-string-length-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":null}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$strLenBytes":"$value"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_string_position_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-string-position-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"abc","token":null}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$indexOfCP":["$value","$token"]}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_substring_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-substring-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":"é"}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$substrBytes":["$value",1,1]}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_size_introspection_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-size-introspection-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":5}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$binarySize":"$value"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_rejects_invalid_bitwise_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("invalid-bitwise-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"value":12.5}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$bitNot":"$value"}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_field_mutation_expressions() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("field-mutation-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "setSimple": { "$setField": { "field": "status", "input": { "a": 1 }, "value": 24 } },
                    "unsetSimple": { "$unsetField": { "field": "a", "input": { "a": 1, "b": 2 } } },
                    "removeWithSetField": { "$setField": { "field": "a", "input": { "a": 1, "b": 2 }, "value": "$$REMOVE" } },
                    "literalDot": { "$setField": { "field": { "$const": "a.b" }, "input": { "$const": { "a.b": 5 } }, "value": 12345 } },
                    "literalDollar": { "$setField": { "field": { "$const": "$price" }, "input": { "$const": { "$price": 5 } }, "value": 9 } },
                    "nestedGet": {
                        "$getField": {
                            "field": "foo",
                            "input": { "$setField": { "field": "foo", "input": "$$ROOT", "value": 1234 } }
                        }
                    }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "setSimple": { "a": 1, "status": 24 },
            "unsetSimple": { "b": 2 },
            "removeWithSetField": { "b": 2 },
            "literalDot": { "a.b": 12345 },
            "literalDollar": { "$price": 9 },
            "nestedGet": 1234
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_field_mutation_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-field-mutation-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$setField":{"field":"$field_path","input":{},"value":0}}}}],"cursor":{}}"#,
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
fn command_aggregate_project_supports_array_sequence_expressions() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir.path().join("array-sequence-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1,"array":[1,2,3,2,1],"seq":[1,2,3]}]}"#,
        ])
        .assert()
        .success();

    let aggregate_command = json!({
        "aggregate": "widgets",
        "pipeline": [
            {
                "$project": {
                    "_id": 0,
                    "indexOfArray": { "$indexOfArray": ["$array", 2] },
                    "indexOfArrayFrom": { "$indexOfArray": ["$array", 2, 2] },
                    "range": { "$range": [0, 5, 2] },
                    "reverseArray": { "$reverseArray": "$seq" },
                    "sliceCount": { "$slice": ["$array", 2] },
                    "sliceWindow": { "$slice": ["$array", 1, 2] },
                    "nullIndex": { "$indexOfArray": [null, 2] },
                    "nullReverse": { "$reverseArray": "$missing" }
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
    assert_eq!(
        first_batch,
        &vec![json!({
            "indexOfArray": 1,
            "indexOfArrayFrom": 3,
            "range": [0, 2, 4],
            "reverseArray": [3, 2, 1],
            "sliceCount": [1, 2],
            "sliceWindow": [2, 3],
            "nullIndex": null,
            "nullReverse": null
        })]
    );
}

#[test]
fn command_aggregate_project_rejects_invalid_array_sequence_expression() {
    let temp_dir = tempdir().expect("tempdir");
    let database_path = temp_dir
        .path()
        .join("invalid-array-sequence-expression.mongodb");

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
            r#"{"insert":"widgets","documents":[{"_id":1}]}"#,
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
            r#"{"aggregate":"widgets","pipeline":[{"$project":{"_id":0,"out":{"$range":[1,3,0]}}}],"cursor":{}}"#,
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
