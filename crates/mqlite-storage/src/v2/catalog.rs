use std::collections::BTreeMap;

use anyhow::Result;
use bson::{Bson, Document, doc};
use mqlite_catalog::{IndexBounds, IndexEntry};

use crate::v2::{
    btree::{PageReader, RecordTree, ScanDirection, SecondaryTree},
    page::RecordSlot,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootSet {
    pub namespace_root_page_id: Option<u64>,
    pub change_stream_root_page_id: Option<u64>,
    pub plan_cache_root_page_id: Option<u64>,
    pub stats_root_page_id: Option<u64>,
    pub freelist_root_page_id: Option<u64>,
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMeta {
    pub database: String,
    pub collection: String,
    pub record_root_page_id: Option<u64>,
    pub index_directory_root_page_id: Option<u64>,
    pub options_bytes: Vec<u8>,
    pub next_record_id: u64,
    pub summary: SummaryCounters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMeta {
    pub name: String,
    pub root_page_id: Option<u64>,
    pub key_pattern_bytes: Vec<u8>,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
    pub entry_count: u64,
    pub index_bytes: u64,
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
}

impl IndexHandle {
    pub fn new(meta: IndexMeta) -> Result<Self> {
        Ok(Self {
            key_pattern: bson::from_slice(&meta.key_pattern_bytes)?,
            meta,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::{Result, anyhow};
    use bson::{Bson, doc};
    use mqlite_catalog::IndexEntry;

    use super::{CollectionHandle, CollectionMeta, IndexHandle, IndexMeta, SummaryCounters};
    use crate::v2::{
        btree::PageReader,
        page::{PageId, RecordLeafPage, RecordSlot, SecondaryEntry, SecondaryLeafPage},
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
            [IndexHandle::new(IndexMeta {
                name: "_id_".to_string(),
                root_page_id: Some(2),
                key_pattern_bytes: bson::to_vec(&doc! { "_id": 1 }).expect("pattern"),
                unique: true,
                expire_after_seconds: None,
                entry_count: 1,
                index_bytes: 32,
            })
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
}
