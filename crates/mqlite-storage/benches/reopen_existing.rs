use std::path::PathBuf;

use bson::doc;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use mqlite_catalog::{CollectionCatalog, CollectionRecord, apply_index_specs};
use mqlite_storage::{DatabaseFile, WalMutation};
use tempfile::TempDir;

const RECORD_COUNTS: &[u64] = &[10_000, 50_000];

struct SeededDatabase {
    _temp_dir: TempDir,
    path: PathBuf,
}

impl SeededDatabase {
    fn checkpointed_records(record_count: u64) -> Self {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join(format!("checkpointed-{record_count}.mongodb"));
        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: seeded_collection(record_count),
                    change_events: Vec::new(),
                })
                .expect("seed collection");
            database.checkpoint().expect("checkpoint");
        }
        Self {
            _temp_dir: temp_dir,
            path,
        }
    }
}

fn seeded_collection(record_count: u64) -> CollectionCatalog {
    let mut collection = CollectionCatalog::new(doc! {});
    for record_id in 1..=record_count {
        collection
            .insert_record(CollectionRecord::new(
                record_id,
                doc! {
                    "_id": record_id as i64,
                    "sku": format!("sku-{record_id:08}"),
                    "qty": (record_id % 100) as i64,
                    "category": format!("cat-{}", record_id % 16),
                },
            ))
            .expect("insert record");
    }
    apply_index_specs(
        &mut collection,
        &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
    )
    .expect("create index");
    collection
}

fn benchmark_reopen_existing(c: &mut Criterion) {
    let mut group = c.benchmark_group("reopen_existing_checkpointed");
    for &record_count in RECORD_COUNTS {
        let fixture = SeededDatabase::checkpointed_records(record_count);
        group.throughput(Throughput::Elements(record_count));
        group.bench_function(format!("records_{record_count}"), |bench| {
            bench.iter(|| {
                let database = DatabaseFile::open_or_create(&fixture.path).expect("reopen");
                black_box(database.last_applied_sequence());
            });
        });
    }
    group.finish();
}

fn benchmark_info_existing(c: &mut Criterion) {
    let mut group = c.benchmark_group("info_existing_checkpointed");
    for &record_count in RECORD_COUNTS {
        let fixture = SeededDatabase::checkpointed_records(record_count);
        group.throughput(Throughput::Elements(record_count));
        group.bench_function(format!("records_{record_count}"), |bench| {
            bench.iter(|| {
                let report = DatabaseFile::info(&fixture.path).expect("info");
                black_box(report.summary.record_count);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, benchmark_reopen_existing, benchmark_info_existing);
criterion_main!(benches);
