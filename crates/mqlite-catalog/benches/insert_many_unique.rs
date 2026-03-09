use bson::doc;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use mqlite_catalog::{CollectionCatalog, CollectionMutation, CollectionRecord, apply_index_specs};

const PRELOADED_RECORDS: u64 = 1_000;
const INSERTED_RECORDS: u64 = 1_000;
const SCALING_BATCH_RECORDS: u64 = 250;
const SCALING_PRELOADED_RECORDS: &[u64] = &[0, 10_000, 25_000];

fn benchmark_insert_many_unique(c: &mut Criterion) {
    let pending_records = build_records(PRELOADED_RECORDS + 1, INSERTED_RECORDS, "pending");
    let mut group = c.benchmark_group("insert_many_unique");
    group.throughput(Throughput::Elements(INSERTED_RECORDS));

    group.bench_function("sequential", |bench| {
        bench.iter_batched(
            seeded_collection,
            |mut collection| {
                for record in &pending_records {
                    collection.insert_record(record.clone()).expect("insert");
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("batched", |bench| {
        bench.iter_batched(
            seeded_collection,
            |mut collection| {
                let mutations = pending_records
                    .iter()
                    .map(CollectionMutation::Insert)
                    .collect::<Vec<_>>();
                collection.apply_mutations(&mutations).expect("apply batch");
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn benchmark_insert_batch_scaling_unique(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_batch_unique_scaling");
    group.throughput(Throughput::Elements(SCALING_BATCH_RECORDS));

    for &preloaded_records in SCALING_PRELOADED_RECORDS {
        group.bench_function(format!("preloaded_{preloaded_records}"), |bench| {
            bench.iter_batched(
                || seeded_collection_with_records(preloaded_records),
                |mut collection| {
                    let pending_records =
                        build_records(preloaded_records + 1, SCALING_BATCH_RECORDS, "pending");
                    let mutations = pending_records
                        .iter()
                        .map(CollectionMutation::Insert)
                        .collect::<Vec<_>>();
                    collection.apply_mutations(&mutations).expect("apply batch");
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn seeded_collection() -> CollectionCatalog {
    seeded_collection_with_records(PRELOADED_RECORDS)
}

fn seeded_collection_with_records(preloaded_records: u64) -> CollectionCatalog {
    let mut collection = CollectionCatalog::new(doc! {});
    for record in build_records(1, preloaded_records, "seed") {
        collection.insert_record(record).expect("seed insert");
    }
    apply_index_specs(
        &mut collection,
        &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
    )
    .expect("create unique index");
    collection
}

fn build_records(start_record_id: u64, count: u64, prefix: &str) -> Vec<CollectionRecord> {
    (0..count)
        .map(|offset| {
            let record_id = start_record_id + offset;
            CollectionRecord {
                record_id,
                document: doc! {
                    "_id": record_id as i64,
                    "sku": format!("{prefix}-{record_id:05}"),
                    "qty": record_id as i64,
                },
            }
        })
        .collect()
}

criterion_group!(
    benches,
    benchmark_insert_many_unique,
    benchmark_insert_batch_scaling_unique
);
criterion_main!(benches);
