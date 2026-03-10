use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::{Result, anyhow};
use bson::{Bson, Document, doc};
use mqlite_bson::compare_bson;
use mqlite_catalog::{CollectionCatalog, CollectionRecord, IndexBounds, IndexCatalog, IndexEntry};
use serde::{Deserialize, Serialize};

use crate::{
    engine::{CollectionReadView, IndexReadView},
    v2::{
        btree::{PageReader, RecordTree, ScanDirection, SecondaryTree},
        page::{
            CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceInternalPage,
            NamespaceLeafPage, PageId, RecordSlot, StatsPage, page_kind,
        },
        pager::Pager,
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootSet {
    pub namespace_root_page_id: Option<u64>,
    pub change_stream_root_page_id: Option<u64>,
    pub plan_cache_root_page_id: Option<u64>,
    pub stats_root_page_id: Option<u64>,
    pub freelist_root_page_id: Option<u64>,
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryCounters {
    pub database_count: u64,
    pub collection_count: u64,
    pub index_count: u64,
    pub record_count: u64,
    pub index_entry_count: u64,
    pub change_event_count: u64,
    pub plan_cache_entry_count: u64,
    pub document_bytes: u64,
    pub index_bytes: u64,
    pub page_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub database: String,
    pub collection: String,
    pub record_root_page_id: Option<u64>,
    pub index_directory_root_page_id: Option<u64>,
    pub options_bytes: Vec<u8>,
    pub next_record_id: u64,
    pub summary: SummaryCounters,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMeta {
    pub name: String,
    pub root_page_id: Option<u64>,
    pub key_pattern_bytes: Vec<u8>,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
    pub entry_count: u64,
    pub index_bytes: u64,
    pub stats_page_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedIndexStats {
    pub entry_count: u64,
    pub present_fields: BTreeMap<String, u64>,
    pub value_frequencies: BTreeMap<String, Vec<PersistedValueFrequency>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedValueFrequency {
    pub encoded_value: Vec<u8>,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionHandle {
    meta: CollectionMeta,
    record_tree: RecordTree,
    indexes: BTreeMap<String, IndexHandle>,
}

impl CollectionHandle {
    pub fn new(
        meta: CollectionMeta,
        indexes: impl IntoIterator<Item = IndexHandle>,
    ) -> Result<Self> {
        let indexes = indexes
            .into_iter()
            .map(|index| Ok((index.name().to_string(), index)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            record_tree: RecordTree::new(meta.record_root_page_id),
            meta,
            indexes,
        })
    }

    pub fn meta(&self) -> &CollectionMeta {
        &self.meta
    }

    pub fn indexes(&self) -> &BTreeMap<String, IndexHandle> {
        &self.indexes
    }

    pub fn record_by_id<R: PageReader>(
        &self,
        reader: &mut R,
        record_id: u64,
    ) -> Result<Option<RecordSlot>> {
        self.record_tree.lookup(reader, record_id)
    }

    pub fn scan_records<R: PageReader>(&self, reader: &mut R) -> Result<Vec<RecordSlot>> {
        self.record_tree.scan(reader)
    }

    pub fn lookup_by_id<R: PageReader>(
        &self,
        reader: &mut R,
        id: &Bson,
    ) -> Result<Option<RecordSlot>> {
        let Some(index) = self.indexes.get("_id_") else {
            return Ok(None);
        };
        let bounds = IndexBounds {
            lower: Some(mqlite_catalog::IndexBound {
                key: doc! { "_id": id.clone() },
                inclusive: true,
            }),
            upper: Some(mqlite_catalog::IndexBound {
                key: doc! { "_id": id.clone() },
                inclusive: true,
            }),
        };
        let Some(record_id) = index
            .scan_bounds(reader, &bounds, ScanDirection::Forward)?
            .into_iter()
            .next()
            .map(|entry| entry.record_id)
        else {
            return Ok(None);
        };
        self.record_tree.lookup(reader, record_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexHandle {
    meta: IndexMeta,
    key_pattern: Document,
    stats: Option<PersistedIndexStats>,
}

impl IndexHandle {
    pub fn new(meta: IndexMeta, stats: Option<PersistedIndexStats>) -> Result<Self> {
        Ok(Self {
            key_pattern: bson::from_slice(&meta.key_pattern_bytes)?,
            meta,
            stats,
        })
    }

    pub fn name(&self) -> &str {
        &self.meta.name
    }

    pub fn meta(&self) -> &IndexMeta {
        &self.meta
    }

    pub fn key_pattern(&self) -> &Document {
        &self.key_pattern
    }

    pub fn stats(&self) -> Option<&PersistedIndexStats> {
        self.stats.as_ref()
    }

    pub fn scan_bounds<R: PageReader>(
        &self,
        reader: &mut R,
        bounds: &IndexBounds,
        direction: ScanDirection,
    ) -> Result<Vec<IndexEntry>> {
        SecondaryTree::new(self.meta.root_page_id, self.key_pattern.clone())
            .scan_bounds(reader, bounds, direction)
    }
}

#[derive(Debug)]
pub(crate) struct PagerCollectionReadView {
    collection: CollectionHandle,
    pager: Arc<Mutex<Pager>>,
    indexes: BTreeMap<String, PagerIndexReadView>,
}

impl PagerCollectionReadView {
    pub fn new(collection: CollectionHandle, pager: Arc<Mutex<Pager>>) -> Self {
        let indexes = collection
            .indexes()
            .iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    PagerIndexReadView {
                        index: index.clone(),
                        pager: Arc::clone(&pager),
                    },
                )
            })
            .collect();
        Self {
            collection,
            pager,
            indexes,
        }
    }
}

impl CollectionReadView for PagerCollectionReadView {
    fn scan_records(&self) -> Result<Vec<CollectionRecord>> {
        let mut pager = lock_pager(&self.pager)?;
        self.collection
            .scan_records(&mut *pager)?
            .into_iter()
            .map(|record| {
                Ok(CollectionRecord::from_encoded(
                    record.record_id,
                    record.decode_document()?,
                    record.encoded_document,
                ))
            })
            .collect()
    }

    fn record_document(&self, record_id: u64) -> Result<Option<Document>> {
        let mut pager = lock_pager(&self.pager)?;
        self.collection
            .record_by_id(&mut *pager, record_id)?
            .map(|record| record.decode_document())
            .transpose()
    }

    fn index_names(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    fn index(&self, name: &str) -> Option<&dyn IndexReadView> {
        self.indexes
            .get(name)
            .map(|index| index as &dyn IndexReadView)
    }
}

#[derive(Debug)]
pub(crate) struct PagerIndexReadView {
    index: IndexHandle,
    pager: Arc<Mutex<Pager>>,
}

impl IndexReadView for PagerIndexReadView {
    fn name(&self) -> &str {
        self.index.name()
    }

    fn key_pattern(&self) -> &Document {
        self.index.key_pattern()
    }

    fn entry_count(&self) -> usize {
        self.index.meta().entry_count as usize
    }

    fn scan_entries(&self, bounds: &IndexBounds) -> Result<Vec<IndexEntry>> {
        let mut pager = lock_pager(&self.pager)?;
        self.index
            .scan_bounds(&mut *pager, bounds, ScanDirection::Forward)
    }

    fn estimate_bounds_count(&self, bounds: &IndexBounds) -> usize {
        self.scan_entries(bounds)
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    fn covers_paths(&self, paths: &std::collections::BTreeSet<String>) -> bool {
        paths
            .iter()
            .all(|path| self.key_pattern().contains_key(path))
    }

    fn estimate_value_count(&self, _field: &str, _value: &Bson) -> Option<usize> {
        let stats = self.index.stats()?;
        stats
            .value_frequencies
            .get(_field)?
            .iter()
            .find_map(|frequency| {
                decode_persisted_stat_value(&frequency.encoded_value)
                    .ok()
                    .filter(|candidate| compare_bson(candidate, _value).is_eq())
                    .map(|_| frequency.count as usize)
            })
    }

    fn estimate_values_count(&self, _field: &str, _values: &[Bson]) -> Option<usize> {
        _values.iter().try_fold(0_usize, |total, value| {
            self.estimate_value_count(_field, value)
                .map(|count| total + count)
        })
    }

    fn estimate_range_count(
        &self,
        field: &str,
        lower: Option<(&Bson, bool)>,
        upper: Option<(&Bson, bool)>,
    ) -> Option<usize> {
        let stats = self.index.stats()?;
        let frequencies = stats.value_frequencies.get(field)?;
        Some(
            frequencies
                .iter()
                .filter_map(|frequency| {
                    let value = decode_persisted_stat_value(&frequency.encoded_value).ok()?;
                    let lower_ok = lower.is_none_or(|(bound, inclusive)| {
                        let ordering = compare_bson(&value, bound);
                        ordering.is_gt() || (inclusive && ordering.is_eq())
                    });
                    let upper_ok = upper.is_none_or(|(bound, inclusive)| {
                        let ordering = compare_bson(&value, bound);
                        ordering.is_lt() || (inclusive && ordering.is_eq())
                    });
                    (lower_ok && upper_ok).then_some(frequency.count as usize)
                })
                .sum(),
        )
    }

    fn present_count(&self, field: &str) -> Option<usize> {
        self.index
            .stats()?
            .present_fields
            .get(field)
            .copied()
            .map(|count| count as usize)
    }
}

#[derive(Debug)]
pub(crate) struct PagerNamespaceCatalog {
    namespace_root_page_id: Option<PageId>,
    pager: Arc<Mutex<Pager>>,
}

impl PagerNamespaceCatalog {
    pub fn new(namespace_root_page_id: Option<PageId>, pager: Arc<Mutex<Pager>>) -> Self {
        Self {
            namespace_root_page_id,
            pager,
        }
    }

    pub fn collection_read_view(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<PagerCollectionReadView>> {
        let mut pager = lock_pager(&self.pager)?;
        let Some(collection_meta_page_id) = lookup_namespace_target(
            &mut *pager,
            self.namespace_root_page_id,
            &format!("{database}.{collection}"),
        )?
        else {
            return Ok(None);
        };
        let collection = load_collection_handle(&mut *pager, collection_meta_page_id)?;
        drop(pager);
        Ok(Some(PagerCollectionReadView::new(
            collection,
            Arc::clone(&self.pager),
        )))
    }

    pub fn collection_handles(&self) -> Result<Vec<CollectionHandle>> {
        let mut pager = lock_pager(&self.pager)?;
        scan_namespace_entries(&mut *pager, self.namespace_root_page_id)?
            .into_iter()
            .map(|entry| load_collection_handle(&mut *pager, entry.target_page_id))
            .collect()
    }

    pub fn load_catalog(&self) -> Result<mqlite_catalog::Catalog> {
        let mut pager = lock_pager(&self.pager)?;
        let mut catalog = mqlite_catalog::Catalog::new();
        for entry in scan_namespace_entries(&mut *pager, self.namespace_root_page_id)? {
            let collection = load_collection_catalog(&mut *pager, entry.target_page_id)?;
            catalog.replace_collection(
                &collection.meta.database,
                &collection.meta.collection,
                collection.catalog,
            );
        }
        Ok(catalog)
    }
}

fn lookup_namespace_target<R: PageReader>(
    reader: &mut R,
    mut page_id: Option<PageId>,
    target: &str,
) -> Result<Option<PageId>> {
    while let Some(current_page_id) = page_id {
        let page = reader.read_page(current_page_id)?;
        match page_kind(&page)? {
            crate::v2::layout::PageKind::NamespaceLeaf => {
                let leaf = NamespaceLeafPage::decode(&page)?;
                match leaf
                    .entries
                    .binary_search_by(|entry| entry.name.as_str().cmp(target))
                {
                    Ok(position) => return Ok(Some(leaf.entries[position].target_page_id)),
                    Err(_) => {
                        let should_advance = leaf
                            .entries
                            .last()
                            .is_some_and(|entry| entry.name.as_str() < target);
                        page_id = if should_advance {
                            leaf.next_page_id
                        } else {
                            None
                        };
                    }
                }
            }
            crate::v2::layout::PageKind::NamespaceInternal => {
                let internal = NamespaceInternalPage::decode(&page)?;
                let mut child_page_id = internal.first_child_page_id;
                for separator in &internal.separators {
                    if target < separator.name.as_str() {
                        break;
                    }
                    child_page_id = separator.child_page_id;
                }
                page_id = Some(child_page_id);
            }
            other => return Err(anyhow!("expected namespace page, found {:?}", other)),
        }
    }
    Ok(None)
}

fn load_collection_handle<R: PageReader>(
    reader: &mut R,
    meta_page_id: PageId,
) -> Result<CollectionHandle> {
    let page = reader.read_page(meta_page_id)?;
    let meta = CollectionMetaPage::decode(&page)?.meta;
    let indexes = load_index_handles(reader, meta.index_directory_root_page_id)?;
    CollectionHandle::new(meta, indexes)
}

fn load_collection_catalog<R: PageReader>(
    reader: &mut R,
    meta_page_id: PageId,
) -> Result<LoadedCollectionCatalog> {
    let handle = load_collection_handle(reader, meta_page_id)?;
    let options = bson::from_slice(&handle.meta().options_bytes)?;
    let records = handle
        .scan_records(reader)?
        .into_iter()
        .map(|record| {
            Ok(CollectionRecord::from_encoded(
                record.record_id,
                record.decode_document()?,
                record.encoded_document,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut indexes = BTreeMap::new();
    for (name, index) in handle.indexes() {
        let mut catalog = IndexCatalog::new(
            name.clone(),
            index.key_pattern().clone(),
            index.meta().unique,
        );
        catalog.expire_after_seconds = index.meta().expire_after_seconds;
        catalog.load_entries(index.scan_bounds(
            reader,
            &IndexBounds {
                lower: None,
                upper: None,
            },
            ScanDirection::Forward,
        )?)?;
        indexes.insert(name.clone(), catalog);
    }

    Ok(LoadedCollectionCatalog {
        meta: handle.meta().clone(),
        catalog: CollectionCatalog::from_parts(
            options,
            indexes,
            records,
            handle.meta().next_record_id,
        ),
    })
}

fn load_index_handles<R: PageReader>(
    reader: &mut R,
    root_page_id: Option<PageId>,
) -> Result<Vec<IndexHandle>> {
    let Some(mut leaf_page_id) = leftmost_namespace_leaf(reader, root_page_id)? else {
        return Ok(Vec::new());
    };

    let mut indexes = Vec::new();
    loop {
        let leaf = NamespaceLeafPage::decode(&reader.read_page(leaf_page_id)?)?;
        for entry in &leaf.entries {
            let page = reader.read_page(entry.target_page_id)?;
            let meta = IndexMetaPage::decode(&page)?.meta;
            let stats = load_index_stats(reader, meta.stats_page_id)?;
            indexes.push(IndexHandle::new(meta, stats)?);
        }
        match leaf.next_page_id {
            Some(next_page_id) => leaf_page_id = next_page_id,
            None => break,
        }
    }

    Ok(indexes)
}

fn load_index_stats<R: PageReader>(
    reader: &mut R,
    stats_page_id: Option<PageId>,
) -> Result<Option<PersistedIndexStats>> {
    let Some(stats_page_id) = stats_page_id else {
        return Ok(None);
    };
    let page = reader.read_page(stats_page_id)?;
    Ok(Some(StatsPage::decode(&page)?.stats))
}

fn decode_persisted_stat_value(bytes: &[u8]) -> Result<Bson> {
    let document = bson::from_slice::<Document>(bytes)?;
    document
        .get("v")
        .cloned()
        .ok_or_else(|| anyhow!("persisted stats value is missing field `v`"))
}

struct LoadedCollectionCatalog {
    meta: CollectionMeta,
    catalog: CollectionCatalog,
}

fn scan_namespace_entries<R: PageReader>(
    reader: &mut R,
    root_page_id: Option<PageId>,
) -> Result<Vec<NamespaceEntry>> {
    let Some(mut leaf_page_id) = leftmost_namespace_leaf(reader, root_page_id)? else {
        return Ok(Vec::new());
    };

    let mut entries = Vec::new();
    loop {
        let leaf = NamespaceLeafPage::decode(&reader.read_page(leaf_page_id)?)?;
        entries.extend(leaf.entries);
        match leaf.next_page_id {
            Some(next_page_id) => leaf_page_id = next_page_id,
            None => break,
        }
    }
    Ok(entries)
}

fn leftmost_namespace_leaf<R: PageReader>(
    reader: &mut R,
    mut page_id: Option<PageId>,
) -> Result<Option<PageId>> {
    while let Some(current_page_id) = page_id {
        let page = reader.read_page(current_page_id)?;
        match page_kind(&page)? {
            crate::v2::layout::PageKind::NamespaceLeaf => return Ok(Some(current_page_id)),
            crate::v2::layout::PageKind::NamespaceInternal => {
                page_id = Some(NamespaceInternalPage::decode(&page)?.first_child_page_id);
            }
            other => return Err(anyhow!("expected namespace page, found {:?}", other)),
        }
    }
    Ok(None)
}

fn lock_pager(pager: &Arc<Mutex<Pager>>) -> Result<MutexGuard<'_, Pager>> {
    pager
        .lock()
        .map_err(|_| anyhow!("v2 pager mutex was poisoned"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
        sync::{Arc, Mutex},
    };

    use anyhow::{Result, anyhow};
    use bson::{Bson, doc};
    use mqlite_catalog::IndexEntry;
    use tempfile::tempdir;

    use super::{
        CollectionHandle, CollectionMeta, IndexHandle, IndexMeta, PagerCollectionReadView,
        PagerNamespaceCatalog, PersistedIndexStats, PersistedValueFrequency, SummaryCounters,
    };
    use crate::{
        engine::CollectionReadView,
        v2::{
            btree::PageReader,
            engine::create_empty,
            layout::{DEFAULT_PAGE_SIZE, page_offset},
            page::{
                CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceLeafPage, PageId,
                RecordLeafPage, RecordSlot, SecondaryEntry, SecondaryLeafPage, StatsPage,
            },
            pager::Pager,
        },
    };

    #[derive(Default)]
    struct MemoryPageReader {
        pages: BTreeMap<PageId, Vec<u8>>,
    }

    impl MemoryPageReader {
        fn insert_page(&mut self, page_id: PageId, bytes: Vec<u8>) {
            self.pages.insert(page_id, bytes);
        }
    }

    impl PageReader for MemoryPageReader {
        fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
            self.pages
                .get(&page_id)
                .cloned()
                .ok_or_else(|| anyhow!("missing page {page_id}"))
        }
    }

    #[test]
    fn collection_handle_uses_persisted_id_index() {
        let mut reader = MemoryPageReader::default();
        reader.insert_page(
            1,
            RecordLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: vec![
                    RecordSlot::from_document(10, &doc! { "_id": 7, "sku": "alpha", "qty": 4 })
                        .expect("record"),
                ],
            }
            .encode()
            .expect("encode records")
            .to_vec(),
        );
        reader.insert_page(
            2,
            SecondaryLeafPage {
                page_id: 2,
                next_page_id: None,
                entries: vec![
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 10,
                            key: doc! { "_id": 7 },
                            present_fields: vec!["_id".to_string()],
                        },
                        &doc! { "_id": 1 },
                    )
                    .expect("entry"),
                ],
            }
            .encode()
            .expect("encode index")
            .to_vec(),
        );

        let collection = CollectionHandle::new(
            CollectionMeta {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                record_root_page_id: Some(1),
                index_directory_root_page_id: None,
                options_bytes: bson::to_vec(&doc! {}).expect("options"),
                next_record_id: 11,
                summary: SummaryCounters {
                    record_count: 1,
                    index_count: 1,
                    ..SummaryCounters::default()
                },
            },
            [IndexHandle::new(
                IndexMeta {
                    name: "_id_".to_string(),
                    root_page_id: Some(2),
                    key_pattern_bytes: bson::to_vec(&doc! { "_id": 1 }).expect("pattern"),
                    unique: true,
                    expire_after_seconds: None,
                    entry_count: 1,
                    index_bytes: 32,
                    stats_page_id: None,
                },
                None,
            )
            .expect("index handle")],
        )
        .expect("collection handle");

        let record = collection
            .lookup_by_id(&mut reader, &Bson::Int32(7))
            .expect("lookup")
            .expect("record");
        assert_eq!(
            record.decode_document().expect("decode"),
            doc! { "_id": 7, "sku": "alpha", "qty": 4 }
        );
    }

    #[test]
    fn pager_collection_read_view_reads_records_from_v2_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("view.mongodb");
        create_empty(&path).expect("create v2 file");

        write_page(
            &path,
            1,
            &RecordLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: vec![
                    RecordSlot::from_document(10, &doc! { "_id": 7, "sku": "alpha", "qty": 4 })
                        .expect("record"),
                ],
            }
            .encode()
            .expect("encode records"),
        );
        write_page(
            &path,
            2,
            &SecondaryLeafPage {
                page_id: 2,
                next_page_id: None,
                entries: vec![
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 10,
                            key: doc! { "_id": 7 },
                            present_fields: vec!["_id".to_string()],
                        },
                        &doc! { "_id": 1 },
                    )
                    .expect("entry"),
                ],
            }
            .encode()
            .expect("encode index"),
        );

        let collection = CollectionHandle::new(
            CollectionMeta {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                record_root_page_id: Some(1),
                index_directory_root_page_id: None,
                options_bytes: bson::to_vec(&doc! {}).expect("options"),
                next_record_id: 11,
                summary: SummaryCounters {
                    record_count: 1,
                    index_count: 1,
                    ..SummaryCounters::default()
                },
            },
            [IndexHandle::new(
                IndexMeta {
                    name: "_id_".to_string(),
                    root_page_id: Some(2),
                    key_pattern_bytes: bson::to_vec(&doc! { "_id": 1 }).expect("pattern"),
                    unique: true,
                    expire_after_seconds: None,
                    entry_count: 1,
                    index_bytes: 32,
                    stats_page_id: None,
                },
                None,
            )
            .expect("index handle")],
        )
        .expect("collection handle");
        let pager = Arc::new(Mutex::new(Pager::open(&path).expect("open pager")));
        let view = PagerCollectionReadView::new(collection, pager);

        let records = view.scan_records().expect("scan records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_id, 10);
        assert_eq!(
            view.record_document(10).expect("record document"),
            Some(doc! { "_id": 7, "sku": "alpha", "qty": 4 })
        );
        assert_eq!(view.index_names(), vec!["_id_".to_string()]);
    }

    #[test]
    fn pager_namespace_catalog_loads_collection_views_from_namespace_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("namespace-view.mongodb");
        create_empty(&path).expect("create v2 file");

        write_page(
            &path,
            1,
            &NamespaceLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: vec![NamespaceEntry {
                    name: "app.widgets".to_string(),
                    target_page_id: 2,
                }],
            }
            .encode()
            .expect("encode namespace leaf"),
        );
        write_page(
            &path,
            2,
            &CollectionMetaPage {
                page_id: 2,
                meta: CollectionMeta {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    record_root_page_id: Some(4),
                    index_directory_root_page_id: Some(3),
                    options_bytes: bson::to_vec(&doc! {}).expect("options"),
                    next_record_id: 11,
                    summary: SummaryCounters {
                        record_count: 1,
                        index_count: 1,
                        ..SummaryCounters::default()
                    },
                },
            }
            .encode()
            .expect("encode collection meta"),
        );
        write_page(
            &path,
            3,
            &NamespaceLeafPage {
                page_id: 3,
                next_page_id: None,
                entries: vec![NamespaceEntry {
                    name: "_id_".to_string(),
                    target_page_id: 5,
                }],
            }
            .encode()
            .expect("encode index directory"),
        );
        write_page(
            &path,
            4,
            &RecordLeafPage {
                page_id: 4,
                next_page_id: None,
                entries: vec![
                    RecordSlot::from_document(10, &doc! { "_id": 7, "sku": "alpha", "qty": 4 })
                        .expect("record"),
                ],
            }
            .encode()
            .expect("encode records"),
        );
        write_page(
            &path,
            5,
            &IndexMetaPage {
                page_id: 5,
                meta: IndexMeta {
                    name: "_id_".to_string(),
                    root_page_id: Some(6),
                    key_pattern_bytes: bson::to_vec(&doc! { "_id": 1 }).expect("pattern"),
                    unique: true,
                    expire_after_seconds: None,
                    entry_count: 1,
                    index_bytes: 32,
                    stats_page_id: Some(7),
                },
            }
            .encode()
            .expect("encode index meta"),
        );
        write_page(
            &path,
            6,
            &SecondaryLeafPage {
                page_id: 6,
                next_page_id: None,
                entries: vec![
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 10,
                            key: doc! { "_id": 7 },
                            present_fields: vec!["_id".to_string()],
                        },
                        &doc! { "_id": 1 },
                    )
                    .expect("entry"),
                ],
            }
            .encode()
            .expect("encode index"),
        );
        write_page(
            &path,
            7,
            &StatsPage {
                page_id: 7,
                stats: PersistedIndexStats {
                    entry_count: 1,
                    present_fields: [("_id".to_string(), 1)].into_iter().collect(),
                    value_frequencies: [(
                        "_id".to_string(),
                        vec![PersistedValueFrequency {
                            encoded_value: bson::to_vec(&doc! { "v": 7 }).expect("value"),
                            count: 1,
                        }],
                    )]
                    .into_iter()
                    .collect(),
                },
            }
            .encode()
            .expect("encode stats"),
        );

        let pager = Arc::new(Mutex::new(Pager::open(&path).expect("open pager")));
        let catalog = PagerNamespaceCatalog::new(Some(1), pager);
        let view = catalog
            .collection_read_view("app", "widgets")
            .expect("load view")
            .expect("collection view");

        assert_eq!(
            view.record_document(10).expect("record document"),
            Some(doc! { "_id": 7, "sku": "alpha", "qty": 4 })
        );
        assert_eq!(view.index_names(), vec!["_id_".to_string()]);
        let index = view.index("_id_").expect("index view");
        assert_eq!(index.estimate_value_count("_id", &Bson::Int32(7)), Some(1));
        assert_eq!(index.present_count("_id"), Some(1));
    }

    fn write_page(path: &std::path::Path, page_id: PageId, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open v2 file");
        let offset = page_offset(page_id, DEFAULT_PAGE_SIZE).expect("page offset");
        file.seek(SeekFrom::Start(offset)).expect("seek page");
        file.write_all(bytes).expect("write page");
        file.flush().expect("flush page");
    }
}
