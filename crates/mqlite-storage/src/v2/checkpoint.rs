use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
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
    keycodec::encode_index_key,
    layout::{
        DATA_START_OFFSET, DEFAULT_PAGE_SIZE, FILE_MAGIC, FileHeader, HEADER_LEN, SUPERBLOCK_COUNT,
        SUPERBLOCK_LEN, page_offset,
    },
    page::{
        ChangeEventsPage, CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceInternalPage,
        NamespaceLeafPage, NamespaceSeparator, PageId, PlanCachePage, RecordInternalPage,
        RecordLeafPage, RecordSeparator, RecordSlot, SecondaryEntry, SecondaryInternalPage,
        SecondaryLeafPage, SecondarySeparator, StatsPage, page_kind_unchecked,
    },
    pager::Pager,
};
use crate::{PersistedChangeEvent, PersistedPlanCacheEntry, PersistedState};

use super::layout::Superblock;

pub(crate) struct CheckpointWriteResult {
    pub active_superblock_slot: usize,
    pub active_superblock: Superblock,
    pub file_size: u64,
}

pub(crate) fn write_catalog_checkpoint(path: impl AsRef<Path>, catalog: &Catalog) -> Result<()> {
    write_checkpoint_state(path, 0, checkpoint_unix_ms(), catalog, &[], &[])
}

pub(crate) fn write_state_checkpoint(path: impl AsRef<Path>, state: &PersistedState) -> Result<()> {
    write_checkpoint_state(
        path,
        state.last_applied_sequence,
        state.last_checkpoint_unix_ms,
        &state.catalog,
        &state.change_events,
        &state.plan_cache_entries,
    )
}

fn write_checkpoint_state(
    path: impl AsRef<Path>,
    durable_lsn: u64,
    last_checkpoint_unix_ms: u64,
    catalog: &Catalog,
    change_events: &[PersistedChangeEvent],
    plan_cache_entries: &[PersistedPlanCacheEntry],
) -> Result<()> {
    let path = path.as_ref();
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.lock_exclusive()?;
    let (active_superblock_slot, current_generation) = read_active_superblock_position(&mut file)?;
    write_checkpoint_state_to_file(
        &mut file,
        durable_lsn,
        last_checkpoint_unix_ms,
        catalog,
        change_events,
        plan_cache_entries,
        active_superblock_slot,
        current_generation,
    )?;
    Ok(())
}

pub(crate) fn initialize_empty_file(file: &mut File) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    file.write_all(&FileHeader::default().encode())?;
    file.write_all(&vec![0_u8; SUPERBLOCK_LEN * SUPERBLOCK_COUNT])?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn write_state_checkpoint_to_file(
    file: &mut File,
    state: &PersistedState,
    active_superblock_slot: usize,
    current_generation: u64,
) -> Result<CheckpointWriteResult> {
    write_checkpoint_state_to_file(
        file,
        state.last_applied_sequence,
        state.last_checkpoint_unix_ms,
        &state.catalog,
        &state.change_events,
        &state.plan_cache_entries,
        active_superblock_slot,
        current_generation,
    )
}

pub(crate) fn publish_state_snapshot_to_file(
    path: impl AsRef<Path>,
    file: &mut File,
    state: &PersistedState,
    active_superblock_slot: usize,
    current_generation: u64,
    base_file_len: u64,
    dirty_collections: &BTreeSet<(String, String)>,
    change_events_dirty: bool,
    plan_cache_dirty: bool,
) -> Result<CheckpointWriteResult> {
    let base_snapshot = load_base_snapshot(path)?;
    let mut writer =
        CheckpointWriter::with_next_page_id(next_page_id_after_file_len(base_file_len));
    let mut summary = SummaryCounters {
        database_count: state.catalog.databases.len() as u64,
        change_event_count: state.change_events.len() as u64,
        plan_cache_entry_count: state.plan_cache_entries.len() as u64,
        ..SummaryCounters::default()
    };
    let mut namespace_entries = Vec::new();
    let mut reused_page_count = 0_u64;

    for (database_name, database) in &state.catalog.databases {
        for (collection_name, collection) in &database.collections {
            summary.collection_count += 1;
            let namespace = format!("{database_name}.{collection_name}");
            if !dirty_collections.contains(&(database_name.clone(), collection_name.clone())) {
                if let Some(existing) = base_snapshot.collections.get(&namespace) {
                    let meta = &existing.meta;
                    summary.record_count += meta.summary.record_count;
                    summary.index_count += meta.summary.index_count;
                    summary.index_entry_count += meta.summary.index_entry_count;
                    summary.document_bytes += meta.summary.document_bytes;
                    summary.index_bytes += meta.summary.index_bytes;
                    reused_page_count += meta.summary.page_count;
                    namespace_entries.push(NamespaceEntry {
                        name: namespace,
                        target_page_id: existing.meta_page_id,
                    });
                    continue;
                }
            }

            let written =
                write_collection_snapshot(&mut writer, database_name, collection_name, collection)?;
            summary.record_count += written.summary.record_count;
            summary.index_count += written.summary.index_count;
            summary.index_entry_count += written.summary.index_entry_count;
            summary.document_bytes += written.summary.document_bytes;
            summary.index_bytes += written.summary.index_bytes;
            namespace_entries.push(NamespaceEntry {
                name: namespace,
                target_page_id: written.meta_page_id,
            });
        }
    }

    let pages_before_namespace = writer.page_count();
    let namespace_root_page_id = writer.write_namespace_tree(namespace_entries)?;
    let namespace_pages = writer.page_count().saturating_sub(pages_before_namespace);

    let (change_stream_root_page_id, change_event_pages) = if change_events_dirty {
        let before = writer.page_count();
        let root = writer.write_change_events(&state.change_events)?;
        (root, writer.page_count().saturating_sub(before))
    } else {
        (
            base_snapshot.change_stream_root_page_id,
            base_snapshot.change_event_page_count,
        )
    };

    let (plan_cache_root_page_id, plan_cache_pages) = if plan_cache_dirty {
        let before = writer.page_count();
        let root = writer.write_plan_cache(&state.plan_cache_entries)?;
        (root, writer.page_count().saturating_sub(before))
    } else {
        (
            base_snapshot.plan_cache_root_page_id,
            base_snapshot.plan_cache_page_count,
        )
    };

    summary.page_count =
        reused_page_count + namespace_pages + change_event_pages + plan_cache_pages;
    summary.page_count += writer
        .page_count()
        .saturating_sub(namespace_pages + change_event_pages + plan_cache_pages);

    let next_page_id = writer.next_page_id.max(1);
    let wal_offset = page_offset(next_page_id, DEFAULT_PAGE_SIZE)?;
    let next_slot = if current_generation == 0 {
        0
    } else {
        (active_superblock_slot + 1) % SUPERBLOCK_COUNT
    };
    let superblock = Superblock {
        generation: current_generation + 1,
        durable_lsn: state.last_applied_sequence,
        last_checkpoint_unix_ms: state.last_checkpoint_unix_ms,
        wal_start_offset: wal_offset,
        wal_end_offset: wal_offset,
        roots: RootSet {
            namespace_root_page_id,
            change_stream_root_page_id,
            plan_cache_root_page_id,
            stats_root_page_id: None,
            freelist_root_page_id: None,
            next_page_id,
        },
        summary,
    };

    file.set_len(base_file_len)?;
    for (page_id, page) in writer.pages {
        file.seek(SeekFrom::Start(page_offset(page_id, DEFAULT_PAGE_SIZE)?))?;
        file.write_all(&page)?;
    }
    file.seek(SeekFrom::Start(
        HEADER_LEN as u64 + next_slot as u64 * SUPERBLOCK_LEN as u64,
    ))?;
    file.write_all(&superblock.encode())?;
    file.set_len(wal_offset)?;
    file.flush()?;
    file.sync_all()?;
    Ok(CheckpointWriteResult {
        active_superblock_slot: next_slot,
        active_superblock: superblock,
        file_size: wal_offset,
    })
}

fn write_checkpoint_state_to_file(
    file: &mut File,
    durable_lsn: u64,
    last_checkpoint_unix_ms: u64,
    catalog: &Catalog,
    change_events: &[PersistedChangeEvent],
    plan_cache_entries: &[PersistedPlanCacheEntry],
    active_superblock_slot: usize,
    current_generation: u64,
) -> Result<CheckpointWriteResult> {
    if file.metadata()?.len() == 0 {
        initialize_empty_file(file)?;
    }

    let mut header_bytes = [0_u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_bytes)?;
    if &header_bytes[..8] != FILE_MAGIC {
        return Err(anyhow!("existing file is not a v2 database"));
    }
    FileHeader::decode(&header_bytes)?;

    let mut writer = CheckpointWriter::default();
    let mut summary = SummaryCounters {
        database_count: catalog.databases.len() as u64,
        change_event_count: change_events.len() as u64,
        plan_cache_entry_count: plan_cache_entries.len() as u64,
        ..SummaryCounters::default()
    };
    let mut namespace_entries = Vec::new();

    for (database_name, database) in &catalog.databases {
        for (collection_name, collection) in &database.collections {
            summary.collection_count += 1;
            let written =
                write_collection_snapshot(&mut writer, database_name, collection_name, collection)?;
            summary.record_count += written.summary.record_count;
            summary.index_count += written.summary.index_count;
            summary.index_entry_count += written.summary.index_entry_count;
            summary.document_bytes += written.summary.document_bytes;
            summary.index_bytes += written.summary.index_bytes;
            namespace_entries.push(NamespaceEntry {
                name: format!("{database_name}.{collection_name}"),
                target_page_id: written.meta_page_id,
            });
        }
    }

    let namespace_root_page_id = writer.write_namespace_tree(namespace_entries)?;
    let change_stream_root_page_id = writer.write_change_events(change_events)?;
    let plan_cache_root_page_id = writer.write_plan_cache(plan_cache_entries)?;
    summary.page_count = writer.page_count();

    let next_page_id = writer.next_page_id.max(1);
    let wal_offset = page_offset(next_page_id, DEFAULT_PAGE_SIZE)?;
    let next_slot = if current_generation == 0 {
        0
    } else {
        (active_superblock_slot + 1) % SUPERBLOCK_COUNT
    };
    let superblock = Superblock {
        generation: current_generation + 1,
        durable_lsn,
        last_checkpoint_unix_ms,
        wal_start_offset: wal_offset,
        wal_end_offset: wal_offset,
        roots: RootSet {
            namespace_root_page_id,
            change_stream_root_page_id,
            plan_cache_root_page_id,
            stats_root_page_id: None,
            freelist_root_page_id: None,
            next_page_id,
        },
        summary,
    };

    file.set_len(DATA_START_OFFSET)?;
    for (page_id, page) in writer.pages {
        file.seek(SeekFrom::Start(page_offset(page_id, DEFAULT_PAGE_SIZE)?))?;
        file.write_all(&page)?;
    }
    file.seek(SeekFrom::Start(
        HEADER_LEN as u64 + next_slot as u64 * SUPERBLOCK_LEN as u64,
    ))?;
    file.write_all(&superblock.encode())?;
    file.set_len(wal_offset)?;
    file.flush()?;
    file.sync_all()?;
    Ok(CheckpointWriteResult {
        active_superblock_slot: next_slot,
        active_superblock: superblock,
        file_size: wal_offset,
    })
}

#[derive(Debug)]
struct WrittenCollection {
    meta_page_id: PageId,
    summary: SummaryCounters,
}

fn write_collection_snapshot(
    writer: &mut CheckpointWriter,
    database_name: &str,
    collection_name: &str,
    collection: &CollectionCatalog,
) -> Result<WrittenCollection> {
    let collection_page_start = writer.page_count();
    let (record_root_page_id, document_bytes) = writer.write_record_tree(collection)?;
    let mut index_entry_count = 0_u64;
    let mut index_bytes = 0_u64;
    let mut index_directory_entries = Vec::new();
    for (index_name, index) in &collection.indexes {
        let (root_page_id, entry_count, bytes) = writer.write_secondary_tree(index)?;
        let stats_page_id = writer.write_index_stats(build_persisted_index_stats(index)?)?;
        index_entry_count += entry_count;
        index_bytes += bytes;

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
    let index_directory_root_page_id = writer.write_namespace_tree(index_directory_entries)?;
    let summary = SummaryCounters {
        collection_count: 1,
        index_count: collection.indexes.len() as u64,
        record_count: collection.records.len() as u64,
        index_entry_count,
        document_bytes,
        index_bytes,
        page_count: writer.page_count().saturating_sub(collection_page_start),
        ..SummaryCounters::default()
    };
    let meta_page_id = writer.write_collection_meta(CollectionMeta {
        database: database_name.to_string(),
        collection: collection_name.to_string(),
        record_root_page_id,
        index_directory_root_page_id,
        options_bytes: bson::to_vec(&collection.options)?,
        next_record_id: collection.next_record_id(),
        summary: summary.clone(),
    })?;
    Ok(WrittenCollection {
        meta_page_id,
        summary,
    })
}

fn read_active_superblock_position(file: &mut File) -> Result<(usize, u64)> {
    let file_size = file.metadata()?.len();
    if file_size < DATA_START_OFFSET {
        return Ok((0, 0));
    }

    let mut header_bytes = [0_u8; HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_bytes)?;
    if FileHeader::decode(&header_bytes).is_err() {
        return Ok((0, 0));
    }

    let mut active: Option<(usize, u64)> = None;
    for slot in 0..SUPERBLOCK_COUNT {
        file.seek(SeekFrom::Start(
            HEADER_LEN as u64 + slot as u64 * SUPERBLOCK_LEN as u64,
        ))?;
        let mut bytes = [0_u8; SUPERBLOCK_LEN];
        file.read_exact(&mut bytes)?;
        let superblock = match Superblock::decode(&bytes) {
            Ok(superblock) => superblock,
            Err(_) => continue,
        };
        match active {
            Some((_, generation)) if generation >= superblock.generation => {}
            _ => active = Some((slot, superblock.generation)),
        }
    }

    Ok(active.unwrap_or((0, 0)))
}

#[derive(Debug)]
struct BaseCollectionSnapshot {
    meta_page_id: PageId,
    meta: CollectionMeta,
}

#[derive(Debug, Default)]
struct BaseSnapshot {
    collections: BTreeMap<String, BaseCollectionSnapshot>,
    change_stream_root_page_id: Option<PageId>,
    change_event_page_count: u64,
    plan_cache_root_page_id: Option<PageId>,
    plan_cache_page_count: u64,
}

fn load_base_snapshot(path: impl AsRef<Path>) -> Result<BaseSnapshot> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(BaseSnapshot::default());
    }

    let pager = Pager::open(path)?;
    let superblock = pager.active_superblock().clone();
    let mut collections = BTreeMap::new();
    for entry in load_namespace_entries(&pager, superblock.roots.namespace_root_page_id)? {
        let page = pager.read_page_bytes(entry.target_page_id)?;
        let meta = CollectionMetaPage::decode(page.as_ref())?.meta;
        collections.insert(
            entry.name,
            BaseCollectionSnapshot {
                meta_page_id: entry.target_page_id,
                meta,
            },
        );
    }

    Ok(BaseSnapshot {
        collections,
        change_stream_root_page_id: superblock.roots.change_stream_root_page_id,
        change_event_page_count: count_change_event_pages(
            &pager,
            superblock.roots.change_stream_root_page_id,
        )?,
        plan_cache_root_page_id: superblock.roots.plan_cache_root_page_id,
        plan_cache_page_count: count_plan_cache_pages(
            &pager,
            superblock.roots.plan_cache_root_page_id,
        )?,
    })
}

fn load_namespace_entries(
    pager: &Pager,
    root_page_id: Option<PageId>,
) -> Result<Vec<NamespaceEntry>> {
    let Some(mut page_id) = leftmost_namespace_leaf(pager, root_page_id)? else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    loop {
        let leaf = NamespaceLeafPage::decode(pager.read_page_bytes(page_id)?.as_ref())?;
        entries.extend(leaf.entries);
        match leaf.next_page_id {
            Some(next_page_id) => page_id = next_page_id,
            None => break,
        }
    }
    Ok(entries)
}

fn leftmost_namespace_leaf(pager: &Pager, mut page_id: Option<PageId>) -> Result<Option<PageId>> {
    while let Some(current_page_id) = page_id {
        let page = pager.read_page_bytes(current_page_id)?;
        match page_kind_unchecked(page.as_ref())? {
            crate::v2::layout::PageKind::NamespaceLeaf => return Ok(Some(current_page_id)),
            crate::v2::layout::PageKind::NamespaceInternal => {
                page_id = Some(NamespaceInternalPage::decode(page.as_ref())?.first_child_page_id);
            }
            other => return Err(anyhow!("expected namespace page, found {:?}", other)),
        }
    }
    Ok(None)
}

fn count_change_event_pages(pager: &Pager, root_page_id: Option<PageId>) -> Result<u64> {
    let Some(mut page_id) = root_page_id else {
        return Ok(0);
    };
    let mut seen = HashSet::new();
    let mut count = 0_u64;
    loop {
        if !seen.insert(page_id) {
            return Err(anyhow!("change-event page chain contains a cycle"));
        }
        let page = ChangeEventsPage::decode(pager.read_page_bytes(page_id)?.as_ref())?;
        count += 1;
        match page.next_page_id {
            Some(next_page_id) => page_id = next_page_id,
            None => break,
        }
    }
    Ok(count)
}

fn count_plan_cache_pages(pager: &Pager, root_page_id: Option<PageId>) -> Result<u64> {
    let Some(mut page_id) = root_page_id else {
        return Ok(0);
    };
    let mut seen = HashSet::new();
    let mut count = 0_u64;
    loop {
        if !seen.insert(page_id) {
            return Err(anyhow!("plan-cache page chain contains a cycle"));
        }
        let page = PlanCachePage::decode(pager.read_page_bytes(page_id)?.as_ref())?;
        count += 1;
        match page.next_page_id {
            Some(next_page_id) => page_id = next_page_id,
            None => break,
        }
    }
    Ok(count)
}

fn next_page_id_after_file_len(base_file_len: u64) -> PageId {
    let used_bytes = base_file_len.saturating_sub(DATA_START_OFFSET);
    used_bytes.div_ceil(u64::from(DEFAULT_PAGE_SIZE)) + 1
}

#[derive(Default)]
struct CheckpointWriter {
    next_page_id: PageId,
    pages: Vec<(PageId, Vec<u8>)>,
}

impl CheckpointWriter {
    fn with_next_page_id(next_page_id: PageId) -> Self {
        Self {
            next_page_id: next_page_id.max(1),
            pages: Vec::new(),
        }
    }

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

    fn write_change_events(
        &mut self,
        change_events: &[PersistedChangeEvent],
    ) -> Result<Option<PageId>> {
        if change_events.is_empty() {
            return Ok(None);
        }
        let chunks = chunk_by_encode(change_events.to_vec(), |chunk| {
            ChangeEventsPage {
                page_id: 1,
                next_page_id: None,
                events: chunk.to_vec(),
            }
            .encode()
            .map(|_| ())
        })?;
        let page_ids = (0..chunks.len())
            .map(|_| self.allocate_page_id())
            .collect::<Vec<_>>();
        for (index, chunk) in chunks.into_iter().enumerate() {
            let page_id = page_ids[index];
            let next_page_id = page_ids.get(index + 1).copied();
            self.pages.push((
                page_id,
                ChangeEventsPage {
                    page_id,
                    next_page_id,
                    events: chunk,
                }
                .encode()?
                .to_vec(),
            ));
        }
        Ok(page_ids.first().copied())
    }

    fn write_plan_cache(
        &mut self,
        plan_cache_entries: &[PersistedPlanCacheEntry],
    ) -> Result<Option<PageId>> {
        if plan_cache_entries.is_empty() {
            return Ok(None);
        }
        let chunks = chunk_by_encode(plan_cache_entries.to_vec(), |chunk| {
            PlanCachePage {
                page_id: 1,
                next_page_id: None,
                entries: chunk.to_vec(),
            }
            .encode()
            .map(|_| ())
        })?;
        let page_ids = (0..chunks.len())
            .map(|_| self.allocate_page_id())
            .collect::<Vec<_>>();
        for (index, chunk) in chunks.into_iter().enumerate() {
            let page_id = page_ids[index];
            let next_page_id = page_ids.get(index + 1).copied();
            self.pages.push((
                page_id,
                PlanCachePage {
                    page_id,
                    next_page_id,
                    entries: chunk,
                }
                .encode()?
                .to_vec(),
            ));
        }
        Ok(page_ids.first().copied())
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
        let mut secondary_entries = Vec::new();
        let mut current_group = Vec::new();
        let mut current_key: Option<Vec<u8>> = None;
        for entry in entries.iter().cloned() {
            let encoded_key = encode_index_key(&entry.key, &index.key)?;
            match current_key.as_ref() {
                Some(existing) if *existing == encoded_key => current_group.push(entry),
                _ => {
                    if !current_group.is_empty() {
                        secondary_entries.push(SecondaryEntry::from_index_entries(
                            &current_group,
                            &index.key,
                        )?);
                        current_group.clear();
                    }
                    index_bytes += bson::to_vec(&entry.key)?.len() as u64;
                    current_key = Some(encoded_key);
                    current_group.push(entry);
                }
            }
        }
        if !current_group.is_empty() {
            secondary_entries.push(SecondaryEntry::from_index_entries(
                &current_group,
                &index.key,
            )?);
        }
        let entry_count = entries.len() as u64;

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
                    .map(|child| SecondarySeparator::from_entry(&child.first_entry, child.page_id))
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
                        .map(|child| {
                            SecondarySeparator::from_entry(&child.first_entry, child.page_id)
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

    use super::{write_catalog_checkpoint, write_state_checkpoint};
    use crate::{
        CollectionReadView, DatabaseFile, PersistedChangeEvent, PersistedPlanCacheChoice,
        PersistedPlanCacheEntry, PersistedState, v2::engine::open_collection_read_view,
    };

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
        assert_eq!(info.file_format_version, 9);
        assert_eq!(info.summary.collection_count, 1);
        assert_eq!(info.summary.index_count, 2);
        assert_eq!(info.summary.record_count, 2);
    }

    #[test]
    fn writes_large_change_event_and_plan_cache_chains() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("checkpoint-v2-large-metadata.mongodb");

        let change_events = (0..24_u32)
            .map(|index| {
                PersistedChangeEvent::new(
                    &doc! { "_data": format!("token-{index}") },
                    bson::Timestamp {
                        time: 1_700_000_000 + index,
                        increment: 1,
                    },
                    bson::DateTime::from_millis(1_700_000_000_000 + i64::from(index)),
                    "app".to_string(),
                    Some("widgets".to_string()),
                    "insert".to_string(),
                    Some(&doc! { "_id": index as i64 }),
                    Some(&doc! {
                        "_id": index as i64,
                        "payload": "x".repeat(512),
                    }),
                    None,
                    None,
                    false,
                    &doc! { "payload": "x".repeat(512) },
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .expect("build change events");
        let plan_cache_entries = (0..200_u64)
            .map(|index| PersistedPlanCacheEntry {
                namespace: "app.widgets".to_string(),
                filter_shape: format!("{{\"sku\":{index},\"payload\":?}}"),
                sort_shape: "{\"sku\":1}".to_string(),
                projection_shape: "{\"_id\":1}".to_string(),
                sequence: index,
                choice: PersistedPlanCacheChoice::Index("sku_1".to_string()),
            })
            .collect::<Vec<_>>();
        let state = PersistedState {
            file_format_version: 8,
            last_applied_sequence: 99,
            last_checkpoint_unix_ms: 1_700_000_000_000,
            catalog: Catalog::new(),
            change_events: change_events.clone(),
            plan_cache_entries: plan_cache_entries.clone(),
        };

        write_state_checkpoint(&path, &state).expect("write checkpoint");

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        assert_eq!(reopened.change_events(), change_events.as_slice());
        assert_eq!(
            reopened.persisted_plan_cache_entries(),
            plan_cache_entries.as_slice()
        );
    }
}
