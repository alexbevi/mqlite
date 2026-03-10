use std::{
    collections::{BTreeMap, HashSet},
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use anyhow::Result;
use fs4::FileExt;
use mqlite_catalog::Catalog;

use crate::{
    InfoCheckpoint, InfoCollection, InfoCollectionCheckpoint, InfoDatabase, InfoDatabaseCheckpoint,
    InfoIndex, InfoIndexCheckpoint, InfoReport, InfoSummary, InfoWal, InspectReport,
    PersistedChangeEvent, PersistedPlanCacheEntry, PersistedState,
    v2::{
        catalog::{CollectionHandle, PagerCollectionReadView, PagerNamespaceCatalog},
        layout::{
            FILE_FORMAT_VERSION, FILE_MAGIC, FileHeader, SUPERBLOCK_COUNT, SUPERBLOCK_LEN,
            Superblock,
        },
        page::{ChangeEventsPage, PlanCachePage},
        pager::Pager,
    },
};

pub(crate) fn is_v2_file(path: impl AsRef<Path>) -> Result<bool> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    Ok(&magic == FILE_MAGIC)
}

pub(crate) fn create_empty(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.lock_exclusive()?;
    file.set_len(0)?;

    let header = FileHeader::default();
    let superblock = Superblock::default();

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header.encode())?;
    file.write_all(&superblock.encode())?;
    file.write_all(&vec![0_u8; SUPERBLOCK_LEN * (SUPERBLOCK_COUNT - 1)])?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn read_info(path: impl AsRef<Path>) -> Result<InfoReport> {
    let path = path.as_ref().to_path_buf();
    let pager = Pager::open(&path)?;
    let header = pager.header();
    let superblock = pager.active_superblock();
    let summary = &superblock.summary;
    let page_size = u64::from(header.page_size);
    let page_bytes = summary.page_count.saturating_mul(page_size);
    let databases = open_namespace_catalog(&path)?
        .collection_handles()?
        .into_iter()
        .fold(
            BTreeMap::<String, Vec<CollectionHandle>>::new(),
            |mut grouped, collection| {
                grouped
                    .entry(collection.meta().database.clone())
                    .or_default()
                    .push(collection);
                grouped
            },
        )
        .into_iter()
        .map(|(name, collections)| build_database_info(name, collections))
        .collect::<Result<Vec<_>>>()?;

    Ok(InfoReport {
        path,
        file_format_version: FILE_FORMAT_VERSION,
        file_size: pager.file_size(),
        last_applied_sequence: superblock.durable_lsn,
        summary: InfoSummary {
            database_count: summary.database_count as usize,
            collection_count: summary.collection_count as usize,
            index_count: summary.index_count as usize,
            record_count: summary.record_count as usize,
            index_entry_count: summary.index_entry_count as usize,
            change_event_count: summary.change_event_count as usize,
            plan_cache_entry_count: summary.plan_cache_entry_count as usize,
            document_bytes: summary.document_bytes,
            index_bytes: summary.index_bytes,
            total_bytes: summary.document_bytes + summary.index_bytes,
        },
        last_checkpoint: InfoCheckpoint {
            generation: superblock.generation,
            last_applied_sequence: superblock.durable_lsn,
            last_checkpoint_unix_ms: superblock.last_checkpoint_unix_ms,
            active_superblock_slot: pager.active_superblock_slot(),
            valid_superblocks: pager.valid_superblocks(),
            database_count: summary.database_count as usize,
            collection_count: summary.collection_count as usize,
            index_count: summary.index_count as usize,
            snapshot_offset: 0,
            snapshot_len: 0,
            wal_offset: superblock.wal_start_offset,
            page_size: header.page_size as usize,
            page_count: summary.page_count as usize,
            page_bytes,
            record_page_count: 0,
            record_page_bytes: 0,
            index_page_count: 0,
            index_page_bytes: 0,
            change_event_page_count: 0,
            change_event_page_bytes: 0,
            record_count: summary.record_count as usize,
            index_entry_count: summary.index_entry_count as usize,
            change_event_count: summary.change_event_count as usize,
            plan_cache_entry_count: summary.plan_cache_entry_count as usize,
            total_bytes: page_bytes,
        },
        wal_since_checkpoint: InfoWal {
            record_count: 0,
            bytes: pager.wal_bytes(),
            truncated_tail: false,
        },
        databases,
    })
}

pub(crate) fn read_inspect(path: impl AsRef<Path>) -> Result<InspectReport> {
    let path = path.as_ref().to_path_buf();
    let pager = Pager::open(&path)?;
    let header = pager.header();
    let superblock = pager.active_superblock();
    let summary = &superblock.summary;
    let databases = open_namespace_catalog(&path)?
        .collection_handles()?
        .into_iter()
        .map(|collection| collection.meta().database.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

    Ok(InspectReport {
        path,
        file_format_version: FILE_FORMAT_VERSION,
        checkpoint_generation: superblock.generation,
        last_applied_sequence: superblock.durable_lsn,
        last_checkpoint_unix_ms: superblock.last_checkpoint_unix_ms,
        active_superblock_slot: pager.active_superblock_slot(),
        valid_superblocks: pager.valid_superblocks(),
        snapshot_offset: 0,
        snapshot_len: 0,
        wal_offset: superblock.wal_start_offset,
        page_size: header.page_size as usize,
        checkpoint_page_count: summary.page_count as usize,
        checkpoint_record_page_count: 0,
        checkpoint_index_page_count: 0,
        checkpoint_change_event_page_count: 0,
        checkpoint_record_count: summary.record_count as usize,
        checkpoint_index_entry_count: summary.index_entry_count as usize,
        checkpoint_change_event_count: summary.change_event_count as usize,
        current_record_count: summary.record_count as usize,
        current_index_entry_count: summary.index_entry_count as usize,
        current_change_event_count: summary.change_event_count as usize,
        wal_records_since_checkpoint: 0,
        wal_bytes_since_checkpoint: pager.wal_bytes(),
        truncated_wal_tail: false,
        file_size: pager.file_size(),
        databases,
    })
}

pub(crate) fn open_namespace_catalog(path: impl AsRef<Path>) -> Result<PagerNamespaceCatalog> {
    let pager = Arc::new(Pager::open(path)?);
    let namespace_root_page_id = pager.active_superblock().roots.namespace_root_page_id;
    Ok(PagerNamespaceCatalog::new(namespace_root_page_id, pager))
}

pub(crate) fn open_collection_read_view(
    path: impl AsRef<Path>,
    database: &str,
    collection: &str,
) -> Result<Option<PagerCollectionReadView>> {
    open_namespace_catalog(path)?.collection_read_view(database, collection)
}

pub(crate) fn load_catalog(path: impl AsRef<Path>) -> Result<Catalog> {
    open_namespace_catalog(path)?.load_catalog()
}

pub(crate) fn load_persisted_state(path: impl AsRef<Path>) -> Result<PersistedState> {
    let pager = Arc::new(Pager::open(path)?);
    let durable_lsn = pager.active_superblock().durable_lsn;
    let last_checkpoint_unix_ms = pager.active_superblock().last_checkpoint_unix_ms;
    let roots = pager.active_superblock().roots.clone();

    let catalog = PagerNamespaceCatalog::new(roots.namespace_root_page_id, Arc::clone(&pager))
        .load_catalog()?;

    Ok(PersistedState {
        file_format_version: FILE_FORMAT_VERSION,
        last_applied_sequence: durable_lsn,
        last_checkpoint_unix_ms,
        catalog,
        change_events: load_change_events(&pager, roots.change_stream_root_page_id)?,
        plan_cache_entries: load_plan_cache_entries(&pager, roots.plan_cache_root_page_id)?,
    })
}

fn build_database_info(name: String, collections: Vec<CollectionHandle>) -> Result<InfoDatabase> {
    let collections = collections
        .into_iter()
        .map(build_collection_info)
        .collect::<Result<Vec<_>>>()?;
    let collection_count = collections.len();
    let index_count = collections
        .iter()
        .map(|collection| collection.index_count)
        .sum();
    let record_count = collections
        .iter()
        .map(|collection| collection.document_count)
        .sum();
    let index_entry_count = collections
        .iter()
        .map(|collection| collection.index_entry_count)
        .sum();
    let document_bytes = collections
        .iter()
        .map(|collection| collection.document_bytes)
        .sum();
    let index_bytes = collections
        .iter()
        .map(|collection| collection.index_bytes)
        .sum();
    let total_bytes = document_bytes + index_bytes;

    Ok(InfoDatabase {
        name,
        collection_count,
        index_count,
        record_count,
        index_entry_count,
        document_bytes,
        index_bytes,
        total_bytes,
        checkpoint: InfoDatabaseCheckpoint {
            collection_count,
            index_count,
            record_count,
            index_entry_count,
            record_page_count: 0,
            record_page_bytes: 0,
            index_page_count: 0,
            index_page_bytes: 0,
            total_bytes,
        },
        collections,
    })
}

fn build_collection_info(collection: CollectionHandle) -> Result<InfoCollection> {
    let indexes = collection
        .indexes()
        .values()
        .map(|index| {
            Ok(InfoIndex {
                name: index.name().to_string(),
                key: index.key_pattern().clone(),
                unique: index.meta().unique,
                expire_after_seconds: index.meta().expire_after_seconds,
                entry_count: index.meta().entry_count as usize,
                bytes: index.meta().index_bytes,
                checkpoint: InfoIndexCheckpoint {
                    entry_count: index.meta().entry_count as usize,
                    page_count: 0,
                    page_bytes: 0,
                    root_page_id: index.meta().root_page_id,
                    total_bytes: index.meta().index_bytes,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let meta = collection.meta();
    let total_bytes = meta.summary.document_bytes + meta.summary.index_bytes;

    Ok(InfoCollection {
        name: meta.collection.clone(),
        document_count: meta.summary.record_count as usize,
        index_count: meta.summary.index_count as usize,
        index_entry_count: meta.summary.index_entry_count as usize,
        document_bytes: meta.summary.document_bytes,
        index_bytes: meta.summary.index_bytes,
        total_bytes,
        checkpoint: InfoCollectionCheckpoint {
            index_count: meta.summary.index_count as usize,
            record_count: meta.summary.record_count as usize,
            index_entry_count: meta.summary.index_entry_count as usize,
            record_page_count: 0,
            record_page_bytes: 0,
            index_page_count: 0,
            index_page_bytes: 0,
            total_bytes,
        },
        indexes,
    })
}

fn load_change_events(
    pager: &Pager,
    root_page_id: Option<u64>,
) -> Result<Vec<PersistedChangeEvent>> {
    let Some(mut page_id) = root_page_id else {
        return Ok(Vec::new());
    };
    let mut seen = HashSet::new();
    let mut events = Vec::new();
    loop {
        if !seen.insert(page_id) {
            return Err(anyhow::anyhow!(
                "v2 change-event page chain contains a cycle"
            ));
        }
        let page = ChangeEventsPage::decode(pager.read_page_bytes(page_id)?.as_ref())?;
        events.extend(page.events);
        let Some(next_page_id) = page.next_page_id else {
            break;
        };
        page_id = next_page_id;
    }
    Ok(events)
}

fn load_plan_cache_entries(
    pager: &Pager,
    root_page_id: Option<u64>,
) -> Result<Vec<PersistedPlanCacheEntry>> {
    let Some(mut page_id) = root_page_id else {
        return Ok(Vec::new());
    };
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    loop {
        if !seen.insert(page_id) {
            return Err(anyhow::anyhow!("v2 plan-cache page chain contains a cycle"));
        }
        let page = PlanCachePage::decode(pager.read_page_bytes(page_id)?.as_ref())?;
        entries.extend(page.entries);
        let Some(next_page_id) = page.next_page_id else {
            break;
        };
        page_id = next_page_id;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use bson::doc;
    use mqlite_catalog::{Catalog, CollectionCatalog, CollectionRecord, apply_index_specs};
    use tempfile::tempdir;

    use super::{
        create_empty, is_v2_file, load_catalog, load_persisted_state, read_info, read_inspect,
    };
    use crate::v2::{
        checkpoint::{write_catalog_checkpoint, write_state_checkpoint},
        layout::{DATA_START_OFFSET, DEFAULT_PAGE_SIZE},
    };

    #[test]
    fn creates_empty_v2_file_and_reads_info() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-empty.mongodb"));

        create_empty(&path).expect("create v2 file");
        assert!(is_v2_file(&path).expect("detect v2 magic"));

        let report = read_info(&path).expect("read v2 info");
        assert_eq!(report.file_format_version, 9);
        assert_eq!(report.summary.record_count, 0);
        assert_eq!(report.summary.index_count, 0);
        assert_eq!(report.last_checkpoint.active_superblock_slot, 0);
        assert_eq!(report.last_checkpoint.valid_superblocks, 1);
        assert_eq!(report.last_checkpoint.page_size, DEFAULT_PAGE_SIZE as usize);
        assert_eq!(report.last_checkpoint.wal_offset, DATA_START_OFFSET);
        assert_eq!(report.wal_since_checkpoint.bytes, 0);
    }

    #[test]
    fn creates_empty_v2_file_and_reads_inspect() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-inspect.mongodb"));

        create_empty(&path).expect("create v2 file");

        let report = read_inspect(&path).expect("read v2 inspect");
        assert_eq!(report.file_format_version, 9);
        assert_eq!(report.current_record_count, 0);
        assert_eq!(report.checkpoint_page_count, 0);
        assert_eq!(report.active_superblock_slot, 0);
        assert_eq!(report.valid_superblocks, 1);
        assert_eq!(report.page_size, DEFAULT_PAGE_SIZE as usize);
        assert_eq!(report.wal_offset, DATA_START_OFFSET);
        assert_eq!(report.wal_bytes_since_checkpoint, 0);
    }

    #[test]
    fn reads_namespace_details_from_v2_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-info.mongodb"));

        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord::new(1, doc! { "_id": 1, "sku": "alpha" }))
            .expect("insert");
        apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("index");

        let mut catalog = Catalog {
            databases: BTreeMap::new(),
        };
        catalog.replace_collection("app", "widgets", collection);
        write_catalog_checkpoint(&path, &catalog).expect("write checkpoint");

        let report = read_info(&path).expect("read info");
        assert_eq!(report.summary.database_count, 1);
        assert_eq!(report.summary.collection_count, 1);
        assert_eq!(report.summary.index_count, 2);
        assert_eq!(report.databases.len(), 1);
        assert_eq!(report.databases[0].name, "app");
        assert_eq!(report.databases[0].collections.len(), 1);
        assert_eq!(report.databases[0].collections[0].name, "widgets");
        assert_eq!(report.databases[0].collections[0].indexes.len(), 2);
    }

    #[test]
    fn loads_full_catalog_from_v2_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-catalog.mongodb"));

        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord::new(1, doc! { "_id": 1, "sku": "alpha" }))
            .expect("insert");
        collection
            .insert_record(CollectionRecord::new(2, doc! { "_id": 2, "sku": "beta" }))
            .expect("insert");
        apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("index");

        let mut catalog = Catalog {
            databases: BTreeMap::new(),
        };
        catalog.replace_collection("app", "widgets", collection);
        write_catalog_checkpoint(&path, &catalog).expect("write checkpoint");

        let loaded = load_catalog(&path).expect("load catalog");
        let widgets = loaded
            .get_collection("app", "widgets")
            .expect("widgets collection");
        assert_eq!(widgets.records.len(), 2);
        assert_eq!(widgets.indexes.len(), 2);
        assert_eq!(
            widgets.records[1].document,
            doc! { "_id": 2, "sku": "beta" }
        );
    }

    #[test]
    fn loads_duplicate_secondary_keys_from_v2_posting_lists() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-duplicate-keys.mongodb"));

        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord::new(
                1,
                doc! { "_id": 1, "category": "tools" },
            ))
            .expect("insert");
        collection
            .insert_record(CollectionRecord::new(
                2,
                doc! { "_id": 2, "category": "tools" },
            ))
            .expect("insert");
        collection
            .insert_record(CollectionRecord::new(
                3,
                doc! { "_id": 3, "category": "books" },
            ))
            .expect("insert");
        apply_index_specs(
            &mut collection,
            &[doc! { "key": { "category": 1 }, "name": "category_1", "unique": false }],
        )
        .expect("index");

        let mut catalog = Catalog {
            databases: BTreeMap::new(),
        };
        catalog.replace_collection("app", "widgets", collection);
        write_catalog_checkpoint(&path, &catalog).expect("write checkpoint");

        let loaded = load_catalog(&path).expect("load catalog");
        let category_index = loaded
            .get_collection("app", "widgets")
            .expect("widgets collection")
            .indexes
            .get("category_1")
            .expect("category index");
        let entries = category_index.scan_entries(&mqlite_catalog::IndexBounds {
            lower: Some(mqlite_catalog::IndexBound {
                key: doc! { "category": "tools" },
                inclusive: true,
            }),
            upper: Some(mqlite_catalog::IndexBound {
                key: doc! { "category": "tools" },
                inclusive: true,
            }),
        });

        assert_eq!(
            entries
                .into_iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn loads_persisted_state_from_v2_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-state.mongodb"));

        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord::new(1, doc! { "_id": 1, "sku": "alpha" }))
            .expect("insert");

        let mut catalog = Catalog {
            databases: BTreeMap::new(),
        };
        catalog.replace_collection("app", "widgets", collection);

        let state = crate::PersistedState {
            file_format_version: crate::FILE_FORMAT_VERSION,
            last_applied_sequence: 7,
            last_checkpoint_unix_ms: 123,
            catalog,
            change_events: vec![
                crate::PersistedChangeEvent::new(
                    &doc! { "_data": "token-1" },
                    bson::Timestamp {
                        time: 7,
                        increment: 1,
                    },
                    bson::DateTime::from_millis(1_700_000_000_000),
                    "app".to_string(),
                    Some("widgets".to_string()),
                    "insert".to_string(),
                    None,
                    None,
                    None,
                    None,
                    false,
                    &doc! {},
                )
                .expect("change event"),
            ],
            plan_cache_entries: vec![crate::PersistedPlanCacheEntry {
                namespace: "app.widgets".to_string(),
                filter_shape: "{\"sku\":?}".to_string(),
                sort_shape: "{}".to_string(),
                projection_shape: "{}".to_string(),
                sequence: 7,
                choice: crate::PersistedPlanCacheChoice::Index("sku_1".to_string()),
            }],
        };
        write_state_checkpoint(&path, &state).expect("write checkpoint");

        let loaded = load_persisted_state(&path).expect("load persisted state");
        assert_eq!(loaded.last_applied_sequence, 7);
        assert_eq!(loaded.change_events.len(), 1);
        assert_eq!(loaded.plan_cache_entries.len(), 1);
        assert_eq!(
            loaded
                .catalog
                .get_collection("app", "widgets")
                .expect("widgets")
                .records
                .len(),
            1
        );
    }
}
