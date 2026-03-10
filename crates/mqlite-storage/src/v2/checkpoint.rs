use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow};
use bson::Bson;
use fs4::FileExt;
use mqlite_bson::compare_bson;
use mqlite_catalog::{Catalog, CollectionCatalog, IndexCatalog};

use crate::v2::{
    catalog::{
        CollectionMeta, IndexMeta, PersistedIndexStats, PersistedValueFrequency, RootSet,
        SummaryCounters,
    },
    engine::create_empty,
    layout::{DEFAULT_PAGE_SIZE, HEADER_LEN, page_offset},
    page::{
        CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceInternalPage,
        NamespaceLeafPage, NamespaceSeparator, PageId, RecordInternalPage, RecordLeafPage,
        RecordSeparator, RecordSlot, SecondaryEntry, SecondaryInternalPage, SecondaryLeafPage,
        SecondarySeparator, StatsPage,
    },
};

use super::layout::Superblock;

pub(crate) fn write_catalog_checkpoint(path: impl AsRef<Path>, catalog: &Catalog) -> Result<()> {
    let path = path.as_ref();
    create_empty(path)?;

    let mut writer = CheckpointWriter::default();
    let mut summary = SummaryCounters {
        database_count: catalog.databases.len() as u64,
        ..SummaryCounters::default()
    };
    let mut namespace_entries = Vec::new();

    for (database_name, database) in &catalog.databases {
        for (collection_name, collection) in &database.collections {
            summary.collection_count += 1;
            summary.record_count += collection.records.len() as u64;
            summary.index_count += collection.indexes.len() as u64;

            let collection_page_start = writer.page_count();
            let (record_root_page_id, document_bytes) = writer.write_record_tree(collection)?;
            summary.document_bytes += document_bytes;
            let mut index_entry_count = 0_u64;
            let mut index_bytes = 0_u64;
            let mut index_directory_entries = Vec::new();
            for (index_name, index) in &collection.indexes {
                let (root_page_id, entry_count, bytes) = writer.write_secondary_tree(index)?;
                let stats_page_id =
                    writer.write_index_stats(build_persisted_index_stats(index)?)?;
                index_entry_count += entry_count;
                index_bytes += bytes;
                summary.index_entry_count += entry_count;

                let index_meta_page_id = writer.write_index_meta(IndexMeta {
                    name: index_name.clone(),
                    root_page_id,
                    key_pattern_bytes: bson::to_vec(&index.key)?,
                    unique: index.unique,
                    expire_after_seconds: index.expire_after_seconds,
                    entry_count,
                    index_bytes: bytes,
                    stats_page_id: Some(stats_page_id),
                })?;
                index_directory_entries.push(NamespaceEntry {
                    name: index_name.clone(),
                    target_page_id: index_meta_page_id,
                });
            }
            summary.index_bytes += index_bytes;
            let index_directory_root_page_id =
                writer.write_namespace_tree(index_directory_entries)?;
            let collection_meta_page_id = writer.write_collection_meta(CollectionMeta {
                database: database_name.clone(),
                collection: collection_name.clone(),
                record_root_page_id,
                index_directory_root_page_id,
                options_bytes: bson::to_vec(&collection.options)?,
                next_record_id: collection.next_record_id(),
                summary: SummaryCounters {
                    collection_count: 1,
                    index_count: collection.indexes.len() as u64,
                    record_count: collection.records.len() as u64,
                    index_entry_count,
                    document_bytes,
                    index_bytes,
                    page_count: writer.page_count().saturating_sub(collection_page_start),
                    ..SummaryCounters::default()
                },
            })?;
            namespace_entries.push(NamespaceEntry {
                name: format!("{database_name}.{collection_name}"),
                target_page_id: collection_meta_page_id,
            });
        }
    }

    let namespace_root_page_id = writer.write_namespace_tree(namespace_entries)?;
    summary.page_count = writer.page_count();

    let next_page_id = writer.next_page_id.max(1);
    let wal_offset = page_offset(next_page_id, DEFAULT_PAGE_SIZE)?;
    let superblock = Superblock {
        generation: 1,
        durable_lsn: 0,
        last_checkpoint_unix_ms: checkpoint_unix_ms(),
        wal_start_offset: wal_offset,
        wal_end_offset: wal_offset,
        roots: RootSet {
            namespace_root_page_id,
            change_stream_root_page_id: None,
            plan_cache_root_page_id: None,
            stats_root_page_id: None,
            freelist_root_page_id: None,
            next_page_id,
        },
        summary,
    };

    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.lock_exclusive()?;
    for (page_id, page) in writer.pages {
        file.seek(SeekFrom::Start(page_offset(page_id, DEFAULT_PAGE_SIZE)?))?;
        file.write_all(&page)?;
    }
    file.seek(SeekFrom::Start(HEADER_LEN as u64))?;
    file.write_all(&superblock.encode())?;
    file.set_len(wal_offset)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

#[derive(Default)]
struct CheckpointWriter {
    next_page_id: PageId,
    pages: Vec<(PageId, Vec<u8>)>,
}

impl CheckpointWriter {
    fn page_count(&self) -> u64 {
        self.pages.len() as u64
    }

    fn allocate_page_id(&mut self) -> PageId {
        let page_id = self.next_page_id.max(1);
        self.next_page_id = page_id + 1;
        page_id
    }

    fn write_collection_meta(&mut self, meta: CollectionMeta) -> Result<PageId> {
        let page_id = self.allocate_page_id();
        self.pages.push((
            page_id,
            CollectionMetaPage { page_id, meta }.encode()?.to_vec(),
        ));
        Ok(page_id)
    }

    fn write_index_meta(&mut self, meta: IndexMeta) -> Result<PageId> {
        let page_id = self.allocate_page_id();
        self.pages
            .push((page_id, IndexMetaPage { page_id, meta }.encode()?.to_vec()));
        Ok(page_id)
    }

    fn write_index_stats(&mut self, stats: PersistedIndexStats) -> Result<PageId> {
        let page_id = self.allocate_page_id();
        self.pages
            .push((page_id, StatsPage { page_id, stats }.encode()?.to_vec()));
        Ok(page_id)
    }

    fn write_namespace_tree(&mut self, mut entries: Vec<NamespaceEntry>) -> Result<Option<PageId>> {
        if entries.is_empty() {
            return Ok(None);
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let leaf_chunks = chunk_by_encode(entries, |chunk| {
            NamespaceLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: chunk.to_vec(),
            }
            .encode()
            .map(|_| ())
        })?;

        let mut children = Vec::new();
        let leaf_ids = (0..leaf_chunks.len())
            .map(|_| self.allocate_page_id())
            .collect::<Vec<_>>();
        for (index, chunk) in leaf_chunks.into_iter().enumerate() {
            let page_id = leaf_ids[index];
            let next_page_id = leaf_ids.get(index + 1).copied();
            let first_name = chunk.first().expect("namespace chunk").name.clone();
            self.pages.push((
                page_id,
                NamespaceLeafPage {
                    page_id,
                    next_page_id,
                    entries: chunk,
                }
                .encode()?
                .to_vec(),
            ));
            children.push(NamespaceChild {
                page_id,
                first_name,
            });
        }

        while children.len() > 1 {
            children = self.write_namespace_level(children)?;
        }
        Ok(Some(children[0].page_id))
    }

    fn write_namespace_level(
        &mut self,
        children: Vec<NamespaceChild>,
    ) -> Result<Vec<NamespaceChild>> {
        let chunks = chunk_by_encode(children, |chunk| {
            NamespaceInternalPage {
                page_id: 1,
                first_child_page_id: chunk[0].page_id,
                separators: chunk
                    .iter()
                    .skip(1)
                    .map(|child| NamespaceSeparator {
                        name: child.first_name.clone(),
                        child_page_id: child.page_id,
                    })
                    .collect(),
            }
            .encode()
            .map(|_| ())
        })?;

        let mut parents = Vec::new();
        for chunk in chunks {
            let page_id = self.allocate_page_id();
            let first_name = chunk[0].first_name.clone();
            self.pages.push((
                page_id,
                NamespaceInternalPage {
                    page_id,
                    first_child_page_id: chunk[0].page_id,
                    separators: chunk
                        .iter()
                        .skip(1)
                        .map(|child| NamespaceSeparator {
                            name: child.first_name.clone(),
                            child_page_id: child.page_id,
                        })
                        .collect(),
                }
                .encode()?
                .to_vec(),
            ));
            parents.push(NamespaceChild {
                page_id,
                first_name,
            });
        }
        Ok(parents)
    }

    fn write_record_tree(
        &mut self,
        collection: &CollectionCatalog,
    ) -> Result<(Option<PageId>, u64)> {
        if collection.records.is_empty() {
            return Ok((None, 0));
        }
        let mut document_bytes = 0_u64;
        let slots = collection
            .records
            .iter()
            .map(|record| {
                let slot = RecordSlot::from_document(record.record_id, &record.document)?;
                document_bytes += slot.encoded_document.len() as u64;
                Ok(slot)
            })
            .collect::<Result<Vec<_>>>()?;

        let leaf_chunks = chunk_by_encode(slots, |chunk| {
            RecordLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: chunk.to_vec(),
            }
            .encode()
            .map(|_| ())
        })?;
        let leaf_ids = (0..leaf_chunks.len())
            .map(|_| self.allocate_page_id())
            .collect::<Vec<_>>();
        let mut children = Vec::new();
        for (index, chunk) in leaf_chunks.into_iter().enumerate() {
            let page_id = leaf_ids[index];
            let next_page_id = leaf_ids.get(index + 1).copied();
            let first_record_id = chunk[0].record_id;
            self.pages.push((
                page_id,
                RecordLeafPage {
                    page_id,
                    next_page_id,
                    entries: chunk,
                }
                .encode()?
                .to_vec(),
            ));
            children.push(RecordChild {
                page_id,
                first_record_id,
            });
        }

        while children.len() > 1 {
            children = self.write_record_level(children)?;
        }
        Ok((Some(children[0].page_id), document_bytes))
    }

    fn write_record_level(&mut self, children: Vec<RecordChild>) -> Result<Vec<RecordChild>> {
        let chunks = chunk_by_encode(children, |chunk| {
            RecordInternalPage {
                page_id: 1,
                first_child_page_id: chunk[0].page_id,
                separators: chunk
                    .iter()
                    .skip(1)
                    .map(|child| RecordSeparator {
                        record_id: child.first_record_id,
                        child_page_id: child.page_id,
                    })
                    .collect(),
            }
            .encode()
            .map(|_| ())
        })?;
        let mut parents = Vec::new();
        for chunk in chunks {
            let page_id = self.allocate_page_id();
            let first_record_id = chunk[0].first_record_id;
            self.pages.push((
                page_id,
                RecordInternalPage {
                    page_id,
                    first_child_page_id: chunk[0].page_id,
                    separators: chunk
                        .iter()
                        .skip(1)
                        .map(|child| RecordSeparator {
                            record_id: child.first_record_id,
                            child_page_id: child.page_id,
                        })
                        .collect(),
                }
                .encode()?
                .to_vec(),
            ));
            parents.push(RecordChild {
                page_id,
                first_record_id,
            });
        }
        Ok(parents)
    }

    fn write_secondary_tree(&mut self, index: &IndexCatalog) -> Result<(Option<PageId>, u64, u64)> {
        let entries = index.entries_snapshot();
        if entries.is_empty() {
            return Ok((None, 0, 0));
        }
        let mut index_bytes = 0_u64;
        let secondary_entries = entries
            .iter()
            .map(|entry| {
                index_bytes += bson::to_vec(&entry.key)?.len() as u64;
                SecondaryEntry::from_index_entry(entry, &index.key)
            })
            .collect::<Result<Vec<_>>>()?;
        let entry_count = secondary_entries.len() as u64;

        let leaf_chunks = chunk_by_encode(secondary_entries, |chunk| {
            SecondaryLeafPage {
                page_id: 1,
                next_page_id: None,
                entries: chunk.to_vec(),
            }
            .encode()
            .map(|_| ())
        })?;
        let leaf_ids = (0..leaf_chunks.len())
            .map(|_| self.allocate_page_id())
            .collect::<Vec<_>>();
        let mut children = Vec::new();
        for (index, chunk) in leaf_chunks.into_iter().enumerate() {
            let page_id = leaf_ids[index];
            let next_page_id = leaf_ids.get(index + 1).copied();
            let first_entry = chunk[0].clone();
            self.pages.push((
                page_id,
                SecondaryLeafPage {
                    page_id,
                    next_page_id,
                    entries: chunk,
                }
                .encode()?
                .to_vec(),
            ));
            children.push(SecondaryChild {
                page_id,
                first_entry,
            });
        }

        while children.len() > 1 {
            children = self.write_secondary_level(children)?;
        }
        Ok((Some(children[0].page_id), entry_count, index_bytes))
    }

    fn write_secondary_level(
        &mut self,
        children: Vec<SecondaryChild>,
    ) -> Result<Vec<SecondaryChild>> {
        let chunks = chunk_by_encode(children, |chunk| {
            SecondaryInternalPage {
                page_id: 1,
                first_child_page_id: chunk[0].page_id,
                separators: chunk
                    .iter()
                    .skip(1)
                    .map(|child| SecondarySeparator {
                        key: child.first_entry.key.clone(),
                        record_id: child.first_entry.record_id,
                        child_page_id: child.page_id,
                    })
                    .collect(),
            }
            .encode()
            .map(|_| ())
        })?;
        let mut parents = Vec::new();
        for chunk in chunks {
            let page_id = self.allocate_page_id();
            let first_entry = chunk[0].first_entry.clone();
            self.pages.push((
                page_id,
                SecondaryInternalPage {
                    page_id,
                    first_child_page_id: chunk[0].page_id,
                    separators: chunk
                        .iter()
                        .skip(1)
                        .map(|child| SecondarySeparator {
                            key: child.first_entry.key.clone(),
                            record_id: child.first_entry.record_id,
                            child_page_id: child.page_id,
                        })
                        .collect(),
                }
                .encode()?
                .to_vec(),
            ));
            parents.push(SecondaryChild {
                page_id,
                first_entry,
            });
        }
        Ok(parents)
    }
}

#[derive(Clone)]
struct NamespaceChild {
    page_id: PageId,
    first_name: String,
}

#[derive(Clone)]
struct RecordChild {
    page_id: PageId,
    first_record_id: u64,
}

#[derive(Clone)]
struct SecondaryChild {
    page_id: PageId,
    first_entry: SecondaryEntry,
}

fn build_persisted_index_stats(index: &IndexCatalog) -> Result<PersistedIndexStats> {
    let entries = index.entries_snapshot();
    let mut present_fields = index
        .key
        .keys()
        .cloned()
        .map(|field| (field, 0_u64))
        .collect::<BTreeMap<_, _>>();
    let mut value_frequencies = index
        .key
        .keys()
        .cloned()
        .map(|field| (field, BTreeMap::<Vec<u8>, u64>::new()))
        .collect::<BTreeMap<_, _>>();

    for entry in &entries {
        for field in &entry.present_fields {
            if let Some(count) = present_fields.get_mut(field) {
                *count += 1;
            }
        }
        for (field, value) in &entry.key {
            if let Some(frequencies) = value_frequencies.get_mut(field) {
                let encoded = encode_persisted_stat_value(value)?;
                *frequencies.entry(encoded).or_insert(0) += 1;
            }
        }
    }

    let value_frequencies = value_frequencies
        .into_iter()
        .map(|(field, frequencies)| {
            let mut frequencies = frequencies
                .into_iter()
                .map(|(encoded_value, count)| PersistedValueFrequency {
                    encoded_value,
                    count,
                })
                .collect::<Vec<_>>();
            frequencies.sort_by(|left, right| {
                let left = decode_persisted_stat_value(&left.encoded_value)
                    .expect("persisted stats values are valid bson scalars");
                let right = decode_persisted_stat_value(&right.encoded_value)
                    .expect("persisted stats values are valid bson scalars");
                compare_bson(&left, &right)
            });
            (field, frequencies)
        })
        .collect();

    Ok(PersistedIndexStats {
        entry_count: entries.len() as u64,
        present_fields,
        value_frequencies,
    })
}

fn encode_persisted_stat_value(value: &Bson) -> Result<Vec<u8>> {
    Ok(bson::to_vec(&bson::doc! { "v": value.clone() })?)
}

fn decode_persisted_stat_value(bytes: &[u8]) -> Result<Bson> {
    let document = bson::from_slice::<bson::Document>(bytes)?;
    document
        .get("v")
        .cloned()
        .ok_or_else(|| anyhow!("persisted stats value is missing field `v`"))
}

fn chunk_by_encode<T: Clone>(
    items: Vec<T>,
    fits: impl Fn(&[T]) -> Result<()>,
) -> Result<Vec<Vec<T>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let mut chunks = Vec::new();
    let mut current = Vec::new();
    for item in items {
        current.push(item);
        if current.len() > 1 && fits(&current).is_err() {
            let last = current.pop().expect("overflowing item");
            chunks.push(current);
            current = vec![last];
            fits(&current).map_err(|_| anyhow!("item exceeds v2 page capacity"))?;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    Ok(chunks)
}

fn checkpoint_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bson::{Bson, doc};
    use mqlite_catalog::{Catalog, CollectionCatalog, CollectionRecord, apply_index_specs};
    use tempfile::tempdir;

    use super::write_catalog_checkpoint;
    use crate::{CollectionReadView, DatabaseFile, v2::engine::open_collection_read_view};

    #[test]
    fn writes_v2_checkpoint_that_round_trips_via_namespace_loader() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("checkpoint-v2.mongodb");

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

        let view = open_collection_read_view(&path, "app", "widgets")
            .expect("open view")
            .expect("collection view");
        assert_eq!(view.index_names().len(), 2);
        assert_eq!(
            view.record_document(2).expect("record"),
            Some(doc! { "_id": 2, "sku": "beta" })
        );
        let index = view.index("sku_1").expect("sku index");
        assert_eq!(
            index.estimate_value_count("sku", &Bson::String("alpha".to_string())),
            Some(1)
        );
        assert_eq!(index.present_count("sku"), Some(2));

        let info = DatabaseFile::info(&path).expect("info");
        assert_eq!(info.file_format_version, 8);
        assert_eq!(info.summary.collection_count, 1);
        assert_eq!(info.summary.index_count, 2);
        assert_eq!(info.summary.record_count, 2);
    }
}
