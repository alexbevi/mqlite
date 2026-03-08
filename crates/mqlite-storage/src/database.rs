use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use blake3::Hasher;
use fs4::FileExt;
use mqlite_catalog::{
    Catalog, CatalogError, CollectionCatalog, CollectionRecord, DatabaseCatalog, IndexCatalog,
    IndexEntry, validate_collection_indexes,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FILE_MAGIC: &[u8; 8] = b"MQLTHDR7";
pub const FILE_FORMAT_VERSION: u32 = 7;
pub const PAGE_SIZE: usize = 4096;
const HEADER_LEN: usize = 4096;
const SUPERBLOCK_LEN: usize = 512;
const SUPERBLOCK_COUNT: usize = 2;
const DATA_START_OFFSET: u64 = (HEADER_LEN + (SUPERBLOCK_LEN * SUPERBLOCK_COUNT)) as u64;
const SUPERBLOCK_MAGIC: &[u8; 8] = b"MQLTSB07";
const WAL_FRAME_MAGIC: &[u8; 4] = b"WAL1";
const WAL_HEADER_LEN: usize = 40;
const PAGE_MAGIC: &[u8; 8] = b"MQLTPG07";
const PAGE_HEADER_LEN: usize = 32;
const SLOT_ENTRY_LEN: usize = 16;
const PAGE_KIND_RECORD: u16 = 1;
const PAGE_KIND_INDEX_LEAF: u16 = 2;
const PAGE_KIND_INDEX_INTERNAL: u16 = 3;
const PAGE_KIND_CHANGE_EVENT: u16 = 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedState {
    pub file_format_version: u32,
    pub last_applied_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
    pub catalog: Catalog,
    #[serde(default)]
    pub change_events: Vec<PersistedChangeEvent>,
    #[serde(default)]
    pub plan_cache_entries: Vec<PersistedPlanCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedChangeEvent {
    pub token: bson::Document,
    pub cluster_time: bson::Timestamp,
    pub wall_time: bson::DateTime,
    pub database: String,
    pub collection: Option<String>,
    pub operation_type: String,
    pub document_key: Option<bson::Document>,
    pub full_document: Option<bson::Document>,
    pub full_document_before_change: Option<bson::Document>,
    pub update_description: Option<bson::Document>,
    pub expanded: bool,
    #[serde(default)]
    pub extra_fields: bson::Document,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PersistedPlanCacheEntry {
    pub namespace: String,
    pub filter_shape: String,
    pub sort_shape: String,
    pub projection_shape: String,
    pub sequence: u64,
    pub choice: PersistedPlanCacheChoice,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PersistedPlanCacheChoice {
    CollectionScan,
    Index(String),
    Union(Vec<PersistedPlanCacheChoice>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalMutation {
    ReplaceCollection {
        database: String,
        collection: String,
        collection_state: CollectionCatalog,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
    ApplyCollectionChanges {
        database: String,
        collection: String,
        #[serde(default)]
        create_options: Option<bson::Document>,
        #[serde(default)]
        inserts: Vec<CollectionRecord>,
        #[serde(default)]
        updates: Vec<CollectionRecord>,
        #[serde(default)]
        deletes: Vec<u64>,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
    DropCollection {
        database: String,
        collection: String,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
}

#[derive(Debug)]
pub struct DatabaseFile {
    path: PathBuf,
    file: File,
    state: PersistedState,
    active_slot: usize,
    active_superblock: Superblock,
    valid_superblocks: usize,
    wal_end_offset: u64,
    wal_records_since_checkpoint: usize,
    wal_bytes_since_checkpoint: u64,
    truncated_wal_tail: bool,
    checkpoint_counts: CheckpointCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerifyReport {
    pub valid: bool,
    pub file_format_version: u32,
    pub checkpoint_generation: u64,
    pub last_applied_sequence: u64,
    pub databases: usize,
    pub collections: usize,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub change_event_count: usize,
    pub page_count: usize,
    pub record_page_count: usize,
    pub index_page_count: usize,
    pub change_event_page_count: usize,
    pub wal_records_since_checkpoint: usize,
    pub truncated_wal_tail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InspectReport {
    pub path: PathBuf,
    pub file_format_version: u32,
    pub checkpoint_generation: u64,
    pub last_applied_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
    pub active_superblock_slot: usize,
    pub valid_superblocks: usize,
    pub snapshot_offset: u64,
    pub snapshot_len: u64,
    pub wal_offset: u64,
    pub page_size: usize,
    pub checkpoint_page_count: usize,
    pub checkpoint_record_page_count: usize,
    pub checkpoint_index_page_count: usize,
    pub checkpoint_change_event_page_count: usize,
    pub checkpoint_record_count: usize,
    pub checkpoint_index_entry_count: usize,
    pub checkpoint_change_event_count: usize,
    pub current_record_count: usize,
    pub current_index_entry_count: usize,
    pub current_change_event_count: usize,
    pub wal_records_since_checkpoint: usize,
    pub wal_bytes_since_checkpoint: u64,
    pub truncated_wal_tail: bool,
    pub file_size: u64,
    pub databases: Vec<String>,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("file does not contain a valid mqlite header")]
    InvalidHeader,
    #[error("unsupported file format version {0}")]
    UnsupportedVersion(u32),
    #[error("file is truncated")]
    Truncated,
    #[error("file does not contain a valid superblock")]
    NoValidSuperblock,
    #[error("superblock checksum mismatch")]
    InvalidSuperblockChecksum,
    #[error("snapshot checksum mismatch")]
    InvalidSnapshotChecksum,
    #[error("invalid wal frame")]
    InvalidWalFrame,
    #[error("wal checksum mismatch")]
    InvalidWalChecksum,
    #[error("record does not fit in a storage page")]
    RecordTooLarge,
    #[error("page checksum mismatch")]
    InvalidPageChecksum,
    #[error("invalid page")]
    InvalidPage,
    #[error("invalid page reference")]
    InvalidPageReference,
    #[error("invalid persisted index state")]
    InvalidIndexState,
}

#[derive(Debug, Clone, Default)]
struct Superblock {
    generation: u64,
    last_applied_sequence: u64,
    last_checkpoint_unix_ms: u64,
    snapshot_offset: u64,
    snapshot_len: u64,
    wal_offset: u64,
    snapshot_checksum: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    sequence: u64,
    mutation: WalMutation,
}

#[derive(Debug, Default)]
struct WalRecovery {
    records: usize,
    bytes: u64,
    truncated_tail: bool,
    last_sequence: Option<u64>,
}

#[derive(Debug)]
struct LoadedState {
    state: PersistedState,
    active_slot: usize,
    active_superblock: Superblock,
    valid_superblocks: usize,
    wal_recovery: WalRecovery,
    file_size: u64,
    checkpoint_counts: CheckpointCounts,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotState {
    file_format_version: u32,
    last_applied_sequence: u64,
    last_checkpoint_unix_ms: u64,
    catalog: SnapshotCatalog,
    #[serde(default)]
    change_event_pages: Vec<PageRef>,
    #[serde(default)]
    change_event_count: usize,
    #[serde(default)]
    plan_cache_entries: Vec<PersistedPlanCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SnapshotCatalog {
    databases: BTreeMap<String, SnapshotDatabaseCatalog>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDatabaseCatalog {
    collections: BTreeMap<String, SnapshotCollectionCatalog>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotCollectionCatalog {
    options: bson::Document,
    indexes: BTreeMap<String, SnapshotIndexCatalog>,
    record_pages: Vec<PageRef>,
    record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotIndexCatalog {
    key: bson::Document,
    unique: bool,
    expire_after_seconds: Option<i64>,
    root_page_id: Option<u64>,
    pages: Vec<PageRef>,
    entry_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageRef {
    page_id: u64,
    offset: u64,
    checksum: [u8; 32],
    page_len: u32,
    entry_count: u16,
}

#[derive(Debug, Clone)]
struct EncodedPage {
    page_id: u64,
    bytes: Vec<u8>,
    checksum: [u8; 32],
    entry_count: u16,
}

#[derive(Debug, Default)]
struct EncodedSnapshotCatalog {
    catalog: SnapshotCatalog,
    change_event_page_ids: Vec<u64>,
    pages: Vec<EncodedPage>,
    record_page_count: usize,
    index_page_count: usize,
    change_event_page_count: usize,
    record_count: usize,
    index_entry_count: usize,
    change_event_count: usize,
}

#[derive(Debug, Default)]
struct SlottedPageBuilder {
    page_id: u64,
    page_kind: u16,
    page_extra: u64,
    bytes: Vec<u8>,
    slot_count: u16,
    free_start: usize,
    free_end: usize,
}

#[derive(Debug)]
struct EncodedIndexTree {
    root_page_id: Option<u64>,
    pages: Vec<EncodedPage>,
}

#[derive(Debug)]
struct EncodedTreePageSummary {
    page: EncodedPage,
    min_key: bson::Document,
    max_key: bson::Document,
}

#[derive(Debug)]
struct DecodedPage {
    page_kind: u16,
    page_extra: u64,
    entries: Vec<(u64, bson::Document)>,
}

#[derive(Debug, Clone, Copy, Default)]
struct CheckpointCounts {
    page_count: usize,
    record_page_count: usize,
    index_page_count: usize,
    change_event_page_count: usize,
    record_count: usize,
    index_entry_count: usize,
    change_event_count: usize,
}

impl DatabaseFile {
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        file.lock_exclusive()?;

        let mut database = Self {
            path,
            file,
            state: PersistedState {
                file_format_version: FILE_FORMAT_VERSION,
                last_applied_sequence: 0,
                last_checkpoint_unix_ms: current_unix_ms(),
                catalog: Catalog::new(),
                change_events: Vec::new(),
                plan_cache_entries: Vec::new(),
            },
            active_slot: 0,
            active_superblock: Superblock::default(),
            valid_superblocks: 0,
            wal_end_offset: DATA_START_OFFSET,
            wal_records_since_checkpoint: 0,
            wal_bytes_since_checkpoint: 0,
            truncated_wal_tail: false,
            checkpoint_counts: CheckpointCounts::default(),
        };

        if database.file.metadata()?.len() == 0 {
            database.initialize_file()?;
        } else {
            database.reload_from_disk()?;
        }

        Ok(database)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.state.catalog
    }

    pub fn last_applied_sequence(&self) -> u64 {
        self.state.last_applied_sequence
    }

    pub fn change_events(&self) -> &[PersistedChangeEvent] {
        &self.state.change_events
    }

    pub fn has_pending_wal(&self) -> bool {
        self.wal_records_since_checkpoint > 0
    }

    pub fn persisted_plan_cache_entries(&self) -> &[PersistedPlanCacheEntry] {
        &self.state.plan_cache_entries
    }

    pub fn set_persisted_plan_cache_entries(&mut self, mut entries: Vec<PersistedPlanCacheEntry>) {
        entries.sort();
        entries.dedup();
        self.state.plan_cache_entries = entries;
    }

    pub fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64> {
        let sequence = self.state.last_applied_sequence + 1;
        validate_mutation(&self.state, &mutation)?;

        let appended_bytes = append_wal_entry(
            &mut self.file,
            self.wal_end_offset,
            &WalEntry {
                sequence,
                mutation: mutation.clone(),
            },
        )?;

        apply_mutation(&mut self.state, sequence, &mutation)?;
        self.wal_end_offset += appended_bytes;
        self.wal_records_since_checkpoint += 1;
        self.wal_bytes_since_checkpoint += appended_bytes;
        self.truncated_wal_tail = false;
        Ok(sequence)
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.write_checkpoint()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn inspect(path: impl AsRef<Path>) -> Result<InspectReport> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let loaded = load_state(&mut file)?;
        Ok(InspectReport {
            path,
            file_format_version: loaded.state.file_format_version,
            checkpoint_generation: loaded.active_superblock.generation,
            last_applied_sequence: loaded.state.last_applied_sequence,
            last_checkpoint_unix_ms: loaded.state.last_checkpoint_unix_ms,
            active_superblock_slot: loaded.active_slot,
            valid_superblocks: loaded.valid_superblocks,
            snapshot_offset: loaded.active_superblock.snapshot_offset,
            snapshot_len: loaded.active_superblock.snapshot_len,
            wal_offset: loaded.active_superblock.wal_offset,
            page_size: PAGE_SIZE,
            checkpoint_page_count: loaded.checkpoint_counts.page_count,
            checkpoint_record_page_count: loaded.checkpoint_counts.record_page_count,
            checkpoint_index_page_count: loaded.checkpoint_counts.index_page_count,
            checkpoint_change_event_page_count: loaded.checkpoint_counts.change_event_page_count,
            checkpoint_record_count: loaded.checkpoint_counts.record_count,
            checkpoint_index_entry_count: loaded.checkpoint_counts.index_entry_count,
            checkpoint_change_event_count: loaded.checkpoint_counts.change_event_count,
            current_record_count: record_count(&loaded.state.catalog),
            current_index_entry_count: index_entry_count(&loaded.state.catalog),
            current_change_event_count: loaded.state.change_events.len(),
            wal_records_since_checkpoint: loaded.wal_recovery.records,
            wal_bytes_since_checkpoint: loaded.wal_recovery.bytes,
            truncated_wal_tail: loaded.wal_recovery.truncated_tail,
            file_size: loaded.file_size,
            databases: loaded.state.catalog.database_names(),
        })
    }

    pub fn verify(path: impl AsRef<Path>) -> Result<VerifyReport> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let loaded = load_state(&mut file)?;
        let collections = loaded
            .state
            .catalog
            .databases
            .values()
            .map(|database| database.collections.len())
            .sum();

        Ok(VerifyReport {
            valid: true,
            file_format_version: loaded.state.file_format_version,
            checkpoint_generation: loaded.active_superblock.generation,
            last_applied_sequence: loaded.state.last_applied_sequence,
            databases: loaded.state.catalog.databases.len(),
            collections,
            record_count: record_count(&loaded.state.catalog),
            index_entry_count: index_entry_count(&loaded.state.catalog),
            change_event_count: loaded.state.change_events.len(),
            page_count: loaded.checkpoint_counts.page_count,
            record_page_count: loaded.checkpoint_counts.record_page_count,
            index_page_count: loaded.checkpoint_counts.index_page_count,
            change_event_page_count: loaded.checkpoint_counts.change_event_page_count,
            wal_records_since_checkpoint: loaded.wal_recovery.records,
            truncated_wal_tail: loaded.wal_recovery.truncated_tail,
        })
    }

    fn initialize_file(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        self.file.write_all(&encode_header())?;
        self.file
            .write_all(&vec![0_u8; SUPERBLOCK_LEN * SUPERBLOCK_COUNT])?;
        self.file.flush()?;
        self.file.sync_all()?;
        self.write_checkpoint()
    }

    fn reload_from_disk(&mut self) -> Result<()> {
        let loaded = load_state(&mut self.file)?;
        self.state = loaded.state;
        self.active_slot = loaded.active_slot;
        self.active_superblock = loaded.active_superblock;
        self.valid_superblocks = loaded.valid_superblocks;
        self.wal_end_offset = loaded.file_size.max(DATA_START_OFFSET);
        self.wal_records_since_checkpoint = loaded.wal_recovery.records;
        self.wal_bytes_since_checkpoint = loaded.wal_recovery.bytes;
        self.truncated_wal_tail = loaded.wal_recovery.truncated_tail;
        self.checkpoint_counts = loaded.checkpoint_counts;
        Ok(())
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        self.state.file_format_version = FILE_FORMAT_VERSION;
        self.state.last_checkpoint_unix_ms = current_unix_ms();

        let pages_start = self.wal_end_offset.max(DATA_START_OFFSET);
        let encoded_catalog =
            encode_snapshot_catalog(&self.state.catalog, &self.state.change_events)?;

        let mut page_refs = Vec::with_capacity(encoded_catalog.pages.len());
        let mut page_offset = pages_start;
        for page in &encoded_catalog.pages {
            page_refs.push(PageRef {
                page_id: page.page_id,
                offset: page_offset,
                checksum: page.checksum,
                page_len: page.bytes.len() as u32,
                entry_count: page.entry_count,
            });
            page_offset += page.bytes.len() as u64;
        }

        let EncodedSnapshotCatalog {
            catalog,
            change_event_page_ids,
            pages,
            record_page_count,
            index_page_count,
            change_event_page_count,
            record_count,
            index_entry_count,
            change_event_count,
        } = encoded_catalog;

        let snapshot_catalog = resolve_snapshot_catalog(catalog, &page_refs)?;
        let snapshot_state = SnapshotState {
            file_format_version: FILE_FORMAT_VERSION,
            last_applied_sequence: self.state.last_applied_sequence,
            last_checkpoint_unix_ms: self.state.last_checkpoint_unix_ms,
            catalog: snapshot_catalog,
            change_event_pages: resolve_page_id_refs(&change_event_page_ids, &page_refs)?,
            change_event_count: self.state.change_events.len(),
            plan_cache_entries: self.state.plan_cache_entries.clone(),
        };
        let snapshot = bson::to_vec(&snapshot_state)?;
        let snapshot_offset = page_offset;

        self.file.seek(SeekFrom::Start(pages_start))?;
        for page in &pages {
            self.file.write_all(&page.bytes)?;
        }
        self.file.write_all(&snapshot)?;
        self.file.flush()?;
        self.file.sync_data()?;

        let next_slot = (self.active_slot + 1) % SUPERBLOCK_COUNT;
        let superblock = Superblock {
            generation: self.active_superblock.generation + 1,
            last_applied_sequence: self.state.last_applied_sequence,
            last_checkpoint_unix_ms: self.state.last_checkpoint_unix_ms,
            snapshot_offset,
            snapshot_len: snapshot.len() as u64,
            wal_offset: snapshot_offset + snapshot.len() as u64,
            snapshot_checksum: hash_bytes(&snapshot),
        };

        write_superblock(&mut self.file, next_slot, &superblock)?;
        self.file.flush()?;
        self.file.sync_all()?;

        self.active_slot = next_slot;
        self.active_superblock = superblock;
        self.valid_superblocks = self.valid_superblocks.max(1);
        self.wal_end_offset = self.active_superblock.wal_offset;
        self.wal_records_since_checkpoint = 0;
        self.wal_bytes_since_checkpoint = 0;
        self.truncated_wal_tail = false;
        self.checkpoint_counts = CheckpointCounts {
            page_count: page_refs.len(),
            record_page_count,
            index_page_count,
            change_event_page_count,
            record_count,
            index_entry_count,
            change_event_count,
        };
        Ok(())
    }
}

impl SlottedPageBuilder {
    fn new(page_id: u64, page_kind: u16, page_extra: u64) -> Self {
        Self {
            page_id,
            page_kind,
            page_extra,
            bytes: vec![0_u8; PAGE_SIZE],
            slot_count: 0,
            free_start: PAGE_HEADER_LEN,
            free_end: PAGE_SIZE,
        }
    }

    fn is_empty(&self) -> bool {
        self.slot_count == 0
    }

    fn can_fit(&self, payload_len: usize) -> bool {
        self.free_start + SLOT_ENTRY_LEN <= self.free_end.saturating_sub(payload_len)
    }

    fn insert(&mut self, entry_id: u64, payload_document: &bson::Document) -> Result<()> {
        let payload = bson::to_vec(payload_document)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !self.can_fit(payload.len()) {
            return Err(StorageError::InvalidPage.into());
        }

        self.free_end -= payload.len();
        self.bytes[self.free_end..self.free_end + payload.len()].copy_from_slice(&payload);

        let slot_offset = self.free_start;
        self.bytes[slot_offset..slot_offset + 8].copy_from_slice(&entry_id.to_le_bytes());
        self.bytes[slot_offset + 8..slot_offset + 10]
            .copy_from_slice(&(self.free_end as u16).to_le_bytes());
        self.bytes[slot_offset + 10..slot_offset + 12]
            .copy_from_slice(&(payload.len() as u16).to_le_bytes());
        self.bytes[slot_offset + 12..slot_offset + 16].copy_from_slice(&0_u32.to_le_bytes());

        self.slot_count += 1;
        self.free_start += SLOT_ENTRY_LEN;
        Ok(())
    }

    fn finish(mut self) -> EncodedPage {
        self.bytes[..8].copy_from_slice(PAGE_MAGIC);
        self.bytes[8..16].copy_from_slice(&self.page_id.to_le_bytes());
        self.bytes[16..18].copy_from_slice(&self.slot_count.to_le_bytes());
        self.bytes[18..20].copy_from_slice(&self.page_kind.to_le_bytes());
        self.bytes[20..22].copy_from_slice(&(self.free_start as u16).to_le_bytes());
        self.bytes[22..24].copy_from_slice(&(self.free_end as u16).to_le_bytes());
        self.bytes[24..32].copy_from_slice(&self.page_extra.to_le_bytes());

        EncodedPage {
            page_id: self.page_id,
            checksum: hash_bytes(&self.bytes),
            bytes: self.bytes,
            entry_count: self.slot_count,
        }
    }
}

fn encode_header() -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    header[16..20].copy_from_slice(&(SUPERBLOCK_LEN as u32).to_le_bytes());
    header[20..24].copy_from_slice(&(SUPERBLOCK_COUNT as u32).to_le_bytes());
    header[24..28].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    header
}

fn read_header(file: &mut File) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; HEADER_LEN];
    file.read_exact(&mut header)
        .map_err(|_| StorageError::Truncated)?;

    if &header[..8] != FILE_MAGIC {
        return Err(StorageError::InvalidHeader.into());
    }

    let file_format_version = u32::from_le_bytes(header[8..12].try_into().expect("version"));
    if file_format_version != FILE_FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(file_format_version).into());
    }

    let header_len = u32::from_le_bytes(header[12..16].try_into().expect("header len"));
    let superblock_len = u32::from_le_bytes(header[16..20].try_into().expect("superblock len"));
    let superblock_count = u32::from_le_bytes(header[20..24].try_into().expect("superblock count"));
    let page_size = u32::from_le_bytes(header[24..28].try_into().expect("page size"));
    if header_len as usize != HEADER_LEN
        || superblock_len as usize != SUPERBLOCK_LEN
        || superblock_count as usize != SUPERBLOCK_COUNT
        || page_size as usize != PAGE_SIZE
    {
        return Err(StorageError::InvalidHeader.into());
    }

    Ok(())
}

fn load_state(file: &mut File) -> Result<LoadedState> {
    read_header(file)?;

    let file_size = file.metadata()?.len();
    if file_size < DATA_START_OFFSET {
        return Err(StorageError::Truncated.into());
    }

    let mut candidates = Vec::new();
    let mut valid_superblocks = 0;
    for slot in 0..SUPERBLOCK_COUNT {
        let superblock = match read_superblock(file, slot) {
            Ok(Some(superblock)) => superblock,
            Ok(None) => continue,
            Err(error) if is_skippable_checkpoint_error(&error) => continue,
            Err(error) => return Err(error),
        };
        let (state, checkpoint_counts) = match read_snapshot(file, &superblock) {
            Ok(loaded) => loaded,
            Err(error) if is_skippable_checkpoint_error(&error) => continue,
            Err(error) => return Err(error),
        };
        valid_superblocks += 1;
        candidates.push((slot, superblock, state, checkpoint_counts));
    }

    let Some((active_slot, active_superblock, mut state, checkpoint_counts)) = candidates
        .into_iter()
        .max_by_key(|(_, superblock, _, _)| superblock.generation)
    else {
        return Err(StorageError::NoValidSuperblock.into());
    };

    let wal_recovery = replay_wal(file, active_superblock.wal_offset, &mut state)?;
    if let Some(last_sequence) = wal_recovery.last_sequence {
        state.last_applied_sequence = last_sequence;
    }

    Ok(LoadedState {
        state,
        active_slot,
        active_superblock,
        valid_superblocks,
        wal_recovery,
        file_size,
        checkpoint_counts,
    })
}

fn write_superblock(file: &mut File, slot: usize, superblock: &Superblock) -> Result<()> {
    let offset = superblock_offset(slot);
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&encode_superblock(superblock))?;
    Ok(())
}

fn read_superblock(file: &mut File, slot: usize) -> Result<Option<Superblock>> {
    let offset = superblock_offset(slot);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0_u8; SUPERBLOCK_LEN];
    file.read_exact(&mut bytes)
        .map_err(|_| StorageError::Truncated)?;

    if bytes.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    if &bytes[..8] != SUPERBLOCK_MAGIC {
        return Err(StorageError::InvalidHeader.into());
    }

    let expected_checksum = hash_bytes(&bytes[..88]);
    if bytes[88..120] != expected_checksum {
        return Err(StorageError::InvalidSuperblockChecksum.into());
    }

    Ok(Some(Superblock {
        generation: u64::from_le_bytes(bytes[8..16].try_into().expect("generation")),
        last_applied_sequence: u64::from_le_bytes(
            bytes[16..24].try_into().expect("last applied sequence"),
        ),
        last_checkpoint_unix_ms: u64::from_le_bytes(
            bytes[24..32].try_into().expect("checkpoint time"),
        ),
        snapshot_offset: u64::from_le_bytes(bytes[32..40].try_into().expect("snapshot offset")),
        snapshot_len: u64::from_le_bytes(bytes[40..48].try_into().expect("snapshot len")),
        wal_offset: u64::from_le_bytes(bytes[48..56].try_into().expect("wal offset")),
        snapshot_checksum: bytes[56..88].try_into().expect("snapshot checksum"),
    }))
}

fn encode_superblock(superblock: &Superblock) -> [u8; SUPERBLOCK_LEN] {
    let mut bytes = [0_u8; SUPERBLOCK_LEN];
    bytes[..8].copy_from_slice(SUPERBLOCK_MAGIC);
    bytes[8..16].copy_from_slice(&superblock.generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&superblock.last_applied_sequence.to_le_bytes());
    bytes[24..32].copy_from_slice(&superblock.last_checkpoint_unix_ms.to_le_bytes());
    bytes[32..40].copy_from_slice(&superblock.snapshot_offset.to_le_bytes());
    bytes[40..48].copy_from_slice(&superblock.snapshot_len.to_le_bytes());
    bytes[48..56].copy_from_slice(&superblock.wal_offset.to_le_bytes());
    bytes[56..88].copy_from_slice(&superblock.snapshot_checksum);
    let checksum = hash_bytes(&bytes[..88]);
    bytes[88..120].copy_from_slice(&checksum);
    bytes
}

fn read_snapshot(
    file: &mut File,
    superblock: &Superblock,
) -> Result<(PersistedState, CheckpointCounts)> {
    if superblock.snapshot_offset < DATA_START_OFFSET
        || superblock.wal_offset < superblock.snapshot_offset
    {
        return Err(StorageError::InvalidHeader.into());
    }

    file.seek(SeekFrom::Start(superblock.snapshot_offset))?;
    let mut snapshot = vec![0_u8; superblock.snapshot_len as usize];
    file.read_exact(&mut snapshot)
        .map_err(|_| StorageError::Truncated)?;
    if hash_bytes(&snapshot) != superblock.snapshot_checksum {
        return Err(StorageError::InvalidSnapshotChecksum.into());
    }

    let snapshot_state = bson::from_slice::<SnapshotState>(&snapshot)?;
    if snapshot_state.file_format_version != FILE_FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(snapshot_state.file_format_version).into());
    }

    let (catalog, checkpoint_counts) = restore_catalog(file, &snapshot_state.catalog)?;
    let change_events = restore_change_events(file, &snapshot_state.change_event_pages)?;
    if change_events.len() != snapshot_state.change_event_count {
        return Err(StorageError::InvalidPage.into());
    }
    Ok((
        PersistedState {
            file_format_version: FILE_FORMAT_VERSION,
            last_applied_sequence: snapshot_state.last_applied_sequence,
            last_checkpoint_unix_ms: snapshot_state.last_checkpoint_unix_ms,
            catalog,
            change_events,
            plan_cache_entries: snapshot_state.plan_cache_entries,
        },
        CheckpointCounts {
            page_count: checkpoint_counts.page_count + snapshot_state.change_event_pages.len(),
            change_event_page_count: snapshot_state.change_event_pages.len(),
            change_event_count: snapshot_state.change_event_count,
            ..checkpoint_counts
        },
    ))
}

fn append_wal_entry(file: &mut File, frame_offset: u64, entry: &WalEntry) -> Result<u64> {
    let payload = bson::to_vec(entry)?;
    let payload_checksum = hash_bytes(&payload);
    let frame_len = (WAL_HEADER_LEN + payload.len()) as u64;

    file.seek(SeekFrom::Start(frame_offset))?;
    file.write_all(WAL_FRAME_MAGIC)?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&payload_checksum)?;
    file.write_all(&payload)?;
    file.flush()?;
    file.sync_data()?;
    Ok(frame_len)
}

fn replay_wal(
    file: &mut File,
    start_offset: u64,
    state: &mut PersistedState,
) -> Result<WalRecovery> {
    let file_size = file.metadata()?.len();
    if start_offset > file_size {
        return Err(StorageError::Truncated.into());
    }

    let mut recovery = WalRecovery::default();
    let mut last_applied_sequence = state.last_applied_sequence;
    let mut offset = start_offset;
    while offset < file_size {
        if file_size - offset < WAL_HEADER_LEN as u64 {
            recovery.truncated_tail = true;
            break;
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0_u8; WAL_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|_| StorageError::Truncated)?;

        if &header[..4] != WAL_FRAME_MAGIC {
            break;
        }

        let payload_len = u32::from_le_bytes(header[4..8].try_into().expect("payload len"));
        let payload_end = offset + WAL_HEADER_LEN as u64 + payload_len as u64;
        if payload_end > file_size {
            recovery.truncated_tail = true;
            break;
        }

        let mut payload = vec![0_u8; payload_len as usize];
        file.read_exact(&mut payload)
            .map_err(|_| StorageError::Truncated)?;

        if hash_bytes(&payload) != header[8..40] {
            return Err(StorageError::InvalidWalChecksum.into());
        }

        let entry = bson::from_slice::<WalEntry>(&payload)?;
        if entry.sequence > last_applied_sequence {
            apply_mutation(state, entry.sequence, &entry.mutation)?;
            last_applied_sequence = entry.sequence;
            recovery.last_sequence = Some(entry.sequence);
            recovery.records += 1;
        }

        offset = payload_end;
    }

    recovery.bytes = offset.saturating_sub(start_offset);
    Ok(recovery)
}

fn apply_mutation(state: &mut PersistedState, sequence: u64, mutation: &WalMutation) -> Result<()> {
    match mutation {
        WalMutation::ReplaceCollection {
            database,
            collection,
            collection_state,
            change_events,
        } => {
            let mut hydrated = collection_state.clone();
            hydrated.hydrate_indexes();
            validate_collection_indexes(&hydrated).map_err(map_catalog_error)?;
            state
                .catalog
                .replace_collection(database, collection, hydrated);
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            inserts,
            updates,
            deletes,
            change_events,
        } => {
            apply_collection_changes(
                state,
                database,
                collection,
                create_options.as_ref(),
                inserts,
                updates,
                deletes,
            )?;
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::DropCollection {
            database,
            collection,
            change_events,
        } => {
            state.catalog.drop_collection(database, collection)?;
            state.change_events.extend(change_events.iter().cloned());
        }
    }
    state.last_applied_sequence = sequence;
    Ok(())
}

fn validate_mutation(state: &PersistedState, mutation: &WalMutation) -> Result<()> {
    match mutation {
        WalMutation::ReplaceCollection {
            collection_state, ..
        } => {
            let mut hydrated = collection_state.clone();
            hydrated.hydrate_indexes();
            validate_collection_indexes(&hydrated).map_err(map_catalog_error)?;
        }
        WalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            inserts,
            updates,
            deletes,
            ..
        } => {
            validate_collection_changes(
                state,
                database,
                collection,
                create_options.as_ref(),
                inserts,
                updates,
                deletes,
            )?;
        }
        WalMutation::DropCollection {
            database,
            collection,
            ..
        } => {
            state.catalog.get_collection(database, collection)?;
        }
    }
    Ok(())
}

fn apply_collection_changes(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    inserts: &[CollectionRecord],
    updates: &[CollectionRecord],
    deletes: &[u64],
) -> Result<()> {
    if state.catalog.get_collection(database, collection).is_err() {
        let Some(options) = create_options else {
            return Err(CatalogError::NamespaceNotFound(
                database.to_string(),
                collection.to_string(),
            )
            .into());
        };
        state
            .catalog
            .create_collection(database, collection, options.clone())?;
    }

    let collection_state = state.catalog.get_collection_mut(database, collection)?;
    apply_collection_change_set(collection_state, inserts, updates, deletes)
}

fn validate_collection_changes(
    state: &PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    inserts: &[CollectionRecord],
    updates: &[CollectionRecord],
    deletes: &[u64],
) -> Result<()> {
    let mut collection_state = match state.catalog.get_collection(database, collection) {
        Ok(collection_state) => collection_state.clone(),
        Err(CatalogError::NamespaceNotFound(_, _)) => {
            let Some(options) = create_options else {
                return Err(CatalogError::NamespaceNotFound(
                    database.to_string(),
                    collection.to_string(),
                )
                .into());
            };
            CollectionCatalog::new(options.clone())
        }
        Err(error) => return Err(error.into()),
    };
    apply_collection_change_set(&mut collection_state, inserts, updates, deletes)
}

fn apply_collection_change_set(
    collection_state: &mut CollectionCatalog,
    inserts: &[CollectionRecord],
    updates: &[CollectionRecord],
    deletes: &[u64],
) -> Result<()> {
    for record in inserts {
        collection_state
            .insert_record(record.clone())
            .map_err(map_catalog_error)?;
    }

    if !updates.is_empty() {
        let positions = collection_state
            .records
            .iter()
            .enumerate()
            .map(|(position, record)| (record.record_id, position))
            .collect::<BTreeMap<_, _>>();
        for record in updates {
            let Some(&position) = positions.get(&record.record_id) else {
                return Err(CatalogError::InvalidIndexState(format!(
                    "record id {} is missing for update",
                    record.record_id
                ))
                .into());
            };
            collection_state
                .update_record_at(position, record.document.clone())
                .map_err(map_catalog_error)?;
        }
    }

    if !deletes.is_empty() {
        let removed = deletes.iter().copied().collect::<BTreeSet<_>>();
        collection_state.delete_records(&removed);
    }

    Ok(())
}

fn encode_snapshot_catalog(
    catalog: &Catalog,
    change_events: &[PersistedChangeEvent],
) -> Result<EncodedSnapshotCatalog> {
    let mut encoded_catalog = EncodedSnapshotCatalog::default();
    let mut next_page_id = 1_u64;

    for (database_name, database) in &catalog.databases {
        let mut snapshot_database = SnapshotDatabaseCatalog {
            collections: BTreeMap::new(),
        };

        for (collection_name, collection) in &database.collections {
            validate_collection_indexes(collection).map_err(map_catalog_error)?;

            let record_pages = encode_collection_pages(&collection.records, &mut next_page_id)?;
            let record_page_count_before = encoded_catalog.pages.len();
            encoded_catalog.record_page_count += record_pages.len();
            encoded_catalog.pages.extend(record_pages);
            let record_page_ids = (record_page_count_before..encoded_catalog.pages.len())
                .map(|index| encoded_catalog.pages[index].page_id)
                .collect::<Vec<_>>();

            let mut snapshot_indexes = BTreeMap::new();
            for (index_name, index) in &collection.indexes {
                let encoded_index_tree =
                    encode_index_tree_pages(&index.entries, &mut next_page_id)?;
                let index_page_count_before = encoded_catalog.pages.len();
                encoded_catalog.index_page_count += encoded_index_tree.pages.len();
                encoded_catalog.index_entry_count += index.entries.len();
                encoded_catalog.pages.extend(encoded_index_tree.pages);
                let index_page_ids = (index_page_count_before..encoded_catalog.pages.len())
                    .map(|position| encoded_catalog.pages[position].page_id)
                    .collect::<Vec<_>>();

                snapshot_indexes.insert(
                    index_name.clone(),
                    SnapshotIndexCatalog {
                        key: index.key.clone(),
                        unique: index.unique,
                        expire_after_seconds: index.expire_after_seconds,
                        root_page_id: encoded_index_tree.root_page_id,
                        pages: placeholder_page_refs(index_page_ids),
                        entry_count: index.entries.len(),
                    },
                );
            }

            snapshot_database.collections.insert(
                collection_name.clone(),
                SnapshotCollectionCatalog {
                    options: collection.options.clone(),
                    indexes: snapshot_indexes,
                    record_pages: placeholder_page_refs(record_page_ids),
                    record_count: collection.records.len(),
                },
            );
            encoded_catalog.record_count += collection.records.len();
        }

        encoded_catalog
            .catalog
            .databases
            .insert(database_name.clone(), snapshot_database);
    }

    let change_event_page_count_before = encoded_catalog.pages.len();
    let change_event_pages = encode_change_event_pages(change_events, &mut next_page_id)?;
    encoded_catalog.change_event_page_count = change_event_pages.len();
    encoded_catalog.change_event_count = change_events.len();
    encoded_catalog.pages.extend(change_event_pages);
    encoded_catalog.change_event_page_ids = (change_event_page_count_before
        ..encoded_catalog.pages.len())
        .map(|index| encoded_catalog.pages[index].page_id)
        .collect();

    Ok(encoded_catalog)
}

fn resolve_snapshot_catalog(
    mut pending_catalog: SnapshotCatalog,
    page_refs: &[PageRef],
) -> Result<SnapshotCatalog> {
    let page_ref_by_id = page_refs
        .iter()
        .map(|page_ref| (page_ref.page_id, page_ref.clone()))
        .collect::<BTreeMap<_, _>>();

    for database in pending_catalog.databases.values_mut() {
        for collection in database.collections.values_mut() {
            collection.record_pages = collection
                .record_pages
                .iter()
                .map(|page_ref| {
                    page_ref_by_id
                        .get(&page_ref.page_id)
                        .cloned()
                        .ok_or(StorageError::InvalidPageReference)
                })
                .collect::<Result<Vec<_>, _>>()?;
            for index in collection.indexes.values_mut() {
                index.pages = index
                    .pages
                    .iter()
                    .map(|page_ref| {
                        page_ref_by_id
                            .get(&page_ref.page_id)
                            .cloned()
                            .ok_or(StorageError::InvalidPageReference)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
        }
    }

    Ok(pending_catalog)
}

fn encode_collection_pages(
    records: &[CollectionRecord],
    next_page_id: &mut u64,
) -> Result<Vec<EncodedPage>> {
    encode_document_pages(
        records
            .iter()
            .map(|record| (record.record_id, &record.document)),
        PAGE_KIND_RECORD,
        0,
        next_page_id,
    )
}

fn encode_change_event_pages(
    events: &[PersistedChangeEvent],
    next_page_id: &mut u64,
) -> Result<Vec<EncodedPage>> {
    let mut pages = Vec::new();
    let mut builder = SlottedPageBuilder::new(*next_page_id, PAGE_KIND_CHANGE_EVENT, 0);

    for (index, event) in events.iter().enumerate() {
        let document = change_event_as_document(event)?;
        let payload = bson::to_vec(&document)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !builder.can_fit(payload.len()) && !builder.is_empty() {
            pages.push(builder.finish());
            *next_page_id += 1;
            builder = SlottedPageBuilder::new(*next_page_id, PAGE_KIND_CHANGE_EVENT, 0);
        }
        builder.insert(index as u64, &document)?;
    }

    if !builder.is_empty() {
        pages.push(builder.finish());
        *next_page_id += 1;
    }

    Ok(pages)
}

fn encode_document_pages<'a, I>(
    entries: I,
    page_kind: u16,
    page_extra: u64,
    next_page_id: &mut u64,
) -> Result<Vec<EncodedPage>>
where
    I: IntoIterator<Item = (u64, &'a bson::Document)>,
{
    let mut pages = Vec::new();
    let mut builder = SlottedPageBuilder::new(*next_page_id, page_kind, page_extra);

    for (entry_id, document) in entries {
        let payload = bson::to_vec(document)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !builder.can_fit(payload.len()) {
            pages.push(builder.finish());
            *next_page_id += 1;
            builder = SlottedPageBuilder::new(*next_page_id, page_kind, page_extra);
        }
        builder.insert(entry_id, document)?;
    }

    if !builder.is_empty() {
        pages.push(builder.finish());
        *next_page_id += 1;
    }

    Ok(pages)
}

fn encode_index_tree_pages(
    entries: &[IndexEntry],
    next_page_id: &mut u64,
) -> Result<EncodedIndexTree> {
    if entries.is_empty() {
        return Ok(EncodedIndexTree {
            root_page_id: None,
            pages: Vec::new(),
        });
    }

    let mut pages = Vec::new();
    let mut level = build_leaf_index_pages(entries, next_page_id)?;
    pages.extend(level.iter().map(|summary| summary.page.clone()));

    while level.len() > 1 {
        level = build_internal_index_pages(&level, next_page_id)?;
        pages.extend(level.iter().map(|summary| summary.page.clone()));
    }

    Ok(EncodedIndexTree {
        root_page_id: Some(level[0].page.page_id),
        pages,
    })
}

fn build_leaf_index_pages(
    entries: &[IndexEntry],
    next_page_id: &mut u64,
) -> Result<Vec<EncodedTreePageSummary>> {
    let mut pages = Vec::new();
    let mut builder = SlottedPageBuilder::new(*next_page_id, PAGE_KIND_INDEX_LEAF, 0);
    let mut page_entries = Vec::<IndexEntry>::new();

    for entry in entries {
        let payload = bson::to_vec(&encode_index_entry_payload(entry)?)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !builder.can_fit(payload.len()) && !page_entries.is_empty() {
            pages.push(finish_leaf_page(builder, &page_entries));
            *next_page_id += 1;
            builder = SlottedPageBuilder::new(*next_page_id, PAGE_KIND_INDEX_LEAF, 0);
            page_entries.clear();
        }
        builder.insert(entry.record_id, &encode_index_entry_payload(entry)?)?;
        page_entries.push(entry.clone());
    }

    if !page_entries.is_empty() {
        pages.push(finish_leaf_page(builder, &page_entries));
        *next_page_id += 1;
    }

    Ok(pages)
}

fn build_internal_index_pages(
    children: &[EncodedTreePageSummary],
    next_page_id: &mut u64,
) -> Result<Vec<EncodedTreePageSummary>> {
    let mut pages = Vec::new();
    let mut position = 0;

    while position < children.len() {
        let left_child = &children[position];
        let mut builder = SlottedPageBuilder::new(
            *next_page_id,
            PAGE_KIND_INDEX_INTERNAL,
            left_child.page.page_id,
        );
        let min_key = left_child.min_key.clone();
        let mut max_key = left_child.max_key.clone();
        let mut child_count = 1;
        position += 1;

        while position < children.len() {
            let child = &children[position];
            let payload = bson::to_vec(&child.min_key)?;
            if !builder.can_fit(payload.len()) {
                break;
            }
            builder.insert(child.page.page_id, &child.min_key)?;
            max_key = child.max_key.clone();
            child_count += 1;
            position += 1;
        }

        if child_count < 2 {
            return Err(StorageError::InvalidPage.into());
        }

        pages.push(EncodedTreePageSummary {
            page: builder.finish(),
            min_key,
            max_key,
        });
        *next_page_id += 1;
    }

    Ok(pages)
}

fn finish_leaf_page(builder: SlottedPageBuilder, entries: &[IndexEntry]) -> EncodedTreePageSummary {
    EncodedTreePageSummary {
        page: builder.finish(),
        min_key: entries.first().expect("leaf entry").key.clone(),
        max_key: entries.last().expect("leaf entry").key.clone(),
    }
}

fn restore_catalog(
    file: &mut File,
    snapshot_catalog: &SnapshotCatalog,
) -> Result<(Catalog, CheckpointCounts)> {
    let mut catalog = Catalog::new();
    let mut counts = CheckpointCounts::default();

    for (database_name, database) in &snapshot_catalog.databases {
        let mut restored_database = DatabaseCatalog {
            collections: BTreeMap::new(),
        };

        for (collection_name, collection) in &database.collections {
            let mut records = Vec::with_capacity(collection.record_count);
            for page_ref in &collection.record_pages {
                let page_records = decode_record_page(file, page_ref)?;
                counts.page_count += 1;
                counts.record_page_count += 1;
                records.extend(page_records);
            }

            if records.len() != collection.record_count {
                return Err(StorageError::InvalidPage.into());
            }

            let mut indexes = BTreeMap::new();
            for (index_name, index) in &collection.indexes {
                let restored_index = restore_index(file, index_name, index)?;
                counts.page_count += index.pages.len();
                counts.index_page_count += index.pages.len();
                indexes.insert(index_name.clone(), restored_index);
            }

            let mut collection_state = CollectionCatalog {
                options: collection.options.clone(),
                indexes,
                records,
            };
            collection_state.hydrate_indexes();
            validate_collection_indexes(&collection_state).map_err(map_catalog_error)?;
            counts.record_count += collection_state.records.len();
            counts.index_entry_count += collection_state
                .indexes
                .values()
                .map(|index| index.entries.len())
                .sum::<usize>();
            restored_database
                .collections
                .insert(collection_name.clone(), collection_state);
        }

        catalog
            .databases
            .insert(database_name.clone(), restored_database);
    }

    Ok((catalog, counts))
}

fn restore_change_events(
    file: &mut File,
    page_refs: &[PageRef],
) -> Result<Vec<PersistedChangeEvent>> {
    let mut events = Vec::new();
    for page_ref in page_refs {
        let page = decode_page(file, page_ref)?;
        if page.page_kind != PAGE_KIND_CHANGE_EVENT {
            return Err(StorageError::InvalidPage.into());
        }
        for (_, document) in page.entries {
            events.push(bson::from_document(document)?);
        }
    }
    Ok(events)
}

fn restore_index(
    file: &mut File,
    index_name: &str,
    snapshot_index: &SnapshotIndexCatalog,
) -> Result<IndexCatalog> {
    let mut index = IndexCatalog::new(
        index_name.to_string(),
        snapshot_index.key.clone(),
        snapshot_index.unique,
    );
    index.expire_after_seconds = snapshot_index.expire_after_seconds;

    if let Some(root_page_id) = snapshot_index.root_page_id {
        let page_ref_by_id = snapshot_index
            .pages
            .iter()
            .map(|page_ref| (page_ref.page_id, page_ref))
            .collect::<BTreeMap<_, _>>();
        let mut visited = BTreeSet::new();
        index.entries = read_index_tree(file, root_page_id, &page_ref_by_id, &mut visited)?;
        if visited.len() != snapshot_index.pages.len()
            || index.entries.len() != snapshot_index.entry_count
        {
            return Err(StorageError::InvalidPage.into());
        }
    } else if !snapshot_index.pages.is_empty() || snapshot_index.entry_count != 0 {
        return Err(StorageError::InvalidPage.into());
    }

    index.rebuild_tree();
    Ok(index)
}

fn decode_record_page(file: &mut File, page_ref: &PageRef) -> Result<Vec<CollectionRecord>> {
    let page = decode_page(file, page_ref)?;
    if page.page_kind != PAGE_KIND_RECORD {
        return Err(StorageError::InvalidPage.into());
    }

    page.entries
        .into_iter()
        .map(|(record_id, document)| {
            Ok(CollectionRecord {
                record_id,
                document,
            })
        })
        .collect()
}

fn change_event_as_document(event: &PersistedChangeEvent) -> Result<bson::Document> {
    Ok(bson::to_document(event)?)
}

fn resolve_page_id_refs(page_ids: &[u64], page_refs: &[PageRef]) -> Result<Vec<PageRef>> {
    let page_ref_by_id = page_refs
        .iter()
        .map(|page_ref| (page_ref.page_id, page_ref.clone()))
        .collect::<BTreeMap<_, _>>();
    page_ids
        .iter()
        .map(|page_id| {
            page_ref_by_id
                .get(page_id)
                .cloned()
                .ok_or(StorageError::InvalidPageReference.into())
        })
        .collect()
}

fn read_index_tree(
    file: &mut File,
    page_id: u64,
    page_ref_by_id: &BTreeMap<u64, &PageRef>,
    visited: &mut BTreeSet<u64>,
) -> Result<Vec<IndexEntry>> {
    if !visited.insert(page_id) {
        return Err(StorageError::InvalidPage.into());
    }

    let page_ref = page_ref_by_id
        .get(&page_id)
        .copied()
        .ok_or(StorageError::InvalidPageReference)?;
    let page = decode_page(file, page_ref)?;
    match page.page_kind {
        PAGE_KIND_INDEX_LEAF => page
            .entries
            .into_iter()
            .map(|(record_id, payload)| decode_index_entry_payload(record_id, payload))
            .collect(),
        PAGE_KIND_INDEX_INTERNAL => {
            if page.page_extra == 0 {
                return Err(StorageError::InvalidPage.into());
            }

            let mut entries = read_index_tree(file, page.page_extra, page_ref_by_id, visited)?;
            for (child_page_id, separator_key) in page.entries {
                let child_entries = read_index_tree(file, child_page_id, page_ref_by_id, visited)?;
                let Some(first_entry) = child_entries.first() else {
                    return Err(StorageError::InvalidPage.into());
                };
                if first_entry.key != separator_key {
                    return Err(StorageError::InvalidPage.into());
                }
                entries.extend(child_entries);
            }
            Ok(entries)
        }
        _ => Err(StorageError::InvalidPage.into()),
    }
}

fn decode_page(file: &mut File, page_ref: &PageRef) -> Result<DecodedPage> {
    if page_ref.page_len as usize != PAGE_SIZE || page_ref.offset < DATA_START_OFFSET {
        return Err(StorageError::InvalidPageReference.into());
    }

    file.seek(SeekFrom::Start(page_ref.offset))?;
    let mut page = vec![0_u8; page_ref.page_len as usize];
    file.read_exact(&mut page)
        .map_err(|_| StorageError::Truncated)?;

    if hash_bytes(&page) != page_ref.checksum {
        return Err(StorageError::InvalidPageChecksum.into());
    }
    if &page[..8] != PAGE_MAGIC {
        return Err(StorageError::InvalidPage.into());
    }

    let page_id = u64::from_le_bytes(page[8..16].try_into().expect("page id"));
    if page_id != page_ref.page_id {
        return Err(StorageError::InvalidPage.into());
    }

    let slot_count = u16::from_le_bytes(page[16..18].try_into().expect("slot count"));
    let page_kind = u16::from_le_bytes(page[18..20].try_into().expect("page kind"));
    let free_start = u16::from_le_bytes(page[20..22].try_into().expect("free start")) as usize;
    let free_end = u16::from_le_bytes(page[22..24].try_into().expect("free end")) as usize;
    let page_extra = u64::from_le_bytes(page[24..32].try_into().expect("page extra"));

    if free_start > PAGE_SIZE
        || free_end > PAGE_SIZE
        || free_start < PAGE_HEADER_LEN
        || free_end < free_start
        || PAGE_HEADER_LEN + slot_count as usize * SLOT_ENTRY_LEN > free_start
    {
        return Err(StorageError::InvalidPage.into());
    }

    let mut entries = Vec::with_capacity(slot_count as usize);
    for slot_index in 0..slot_count as usize {
        let slot_offset = PAGE_HEADER_LEN + slot_index * SLOT_ENTRY_LEN;
        let entry_id = u64::from_le_bytes(
            page[slot_offset..slot_offset + 8]
                .try_into()
                .expect("record id"),
        );
        let record_offset = u16::from_le_bytes(
            page[slot_offset + 8..slot_offset + 10]
                .try_into()
                .expect("record offset"),
        ) as usize;
        let record_len = u16::from_le_bytes(
            page[slot_offset + 10..slot_offset + 12]
                .try_into()
                .expect("record len"),
        ) as usize;

        if record_offset < free_end || record_offset + record_len > PAGE_SIZE {
            return Err(StorageError::InvalidPage.into());
        }

        let document =
            bson::from_slice::<bson::Document>(&page[record_offset..record_offset + record_len])?;
        entries.push((entry_id, document));
    }

    if entries.len() != page_ref.entry_count as usize {
        return Err(StorageError::InvalidPage.into());
    }

    Ok(DecodedPage {
        page_kind,
        page_extra,
        entries,
    })
}

fn encode_index_entry_payload(entry: &IndexEntry) -> Result<bson::Document> {
    bson::to_document(entry).map_err(Into::into)
}

fn decode_index_entry_payload(record_id: u64, payload: bson::Document) -> Result<IndexEntry> {
    let mut entry = bson::from_document::<IndexEntry>(payload)?;
    entry.record_id = record_id;
    entry.present_fields.sort();
    entry.present_fields.dedup();
    Ok(entry)
}

fn is_skippable_checkpoint_error(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<StorageError>(),
        Some(
            StorageError::Truncated
                | StorageError::InvalidSuperblockChecksum
                | StorageError::InvalidSnapshotChecksum
                | StorageError::InvalidPageChecksum
                | StorageError::InvalidPage
                | StorageError::InvalidPageReference
                | StorageError::InvalidIndexState
        )
    )
}

fn superblock_offset(slot: usize) -> u64 {
    HEADER_LEN as u64 + (slot * SUPERBLOCK_LEN) as u64
}

fn record_count(catalog: &Catalog) -> usize {
    catalog
        .databases
        .values()
        .flat_map(|database| database.collections.values())
        .map(|collection| collection.records.len())
        .sum()
}

fn index_entry_count(catalog: &Catalog) -> usize {
    catalog
        .databases
        .values()
        .flat_map(|database| database.collections.values())
        .flat_map(|collection| collection.indexes.values())
        .map(|index| index.entries.len())
        .sum()
}

fn placeholder_page_refs(page_ids: Vec<u64>) -> Vec<PageRef> {
    page_ids
        .into_iter()
        .map(|page_id| PageRef {
            page_id,
            offset: 0,
            checksum: [0_u8; 32],
            page_len: 0,
            entry_count: 0,
        })
        .collect()
}

fn map_catalog_error(error: CatalogError) -> anyhow::Error {
    match error {
        CatalogError::DuplicateKey(_) | CatalogError::InvalidIndexState(_) => {
            StorageError::InvalidIndexState.into()
        }
        other => anyhow::Error::new(other),
    }
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
    };

    use bson::{DateTime, Document, Timestamp, doc};
    use mqlite_catalog::{
        CollectionCatalog, CollectionRecord, IndexBound, IndexBounds, apply_index_specs,
    };
    use tempfile::tempdir;

    use super::{
        DATA_START_OFFSET, DatabaseFile, FILE_FORMAT_VERSION, PAGE_KIND_INDEX_INTERNAL, PAGE_SIZE,
        PersistedChangeEvent, PersistedPlanCacheChoice, PersistedPlanCacheEntry, SnapshotState,
        VerifyReport, WalMutation, decode_page, read_superblock,
    };

    fn insert_record(collection: &mut CollectionCatalog, record_id: u64, document: bson::Document) {
        collection
            .insert_record(CollectionRecord {
                record_id,
                document,
            })
            .expect("insert record");
    }

    fn latest_snapshot(path: &std::path::Path, slot: usize) -> SnapshotState {
        let mut file = OpenOptions::new().read(true).open(path).expect("open file");
        let superblock = read_superblock(&mut file, slot)
            .expect("read superblock")
            .expect("superblock");
        file.seek(SeekFrom::Start(superblock.snapshot_offset))
            .expect("seek snapshot");
        let mut snapshot = vec![0_u8; superblock.snapshot_len as usize];
        file.read_exact(&mut snapshot).expect("read snapshot");
        bson::from_slice::<SnapshotState>(&snapshot).expect("decode snapshot")
    }

    fn sample_change_event(sequence: i64, operation_type: &str) -> PersistedChangeEvent {
        PersistedChangeEvent {
            token: doc! { "sequence": sequence, "event": 1_i32 },
            cluster_time: Timestamp {
                time: sequence as u32,
                increment: 1,
            },
            wall_time: DateTime::from_millis(sequence),
            database: "app".to_string(),
            collection: Some("widgets".to_string()),
            operation_type: operation_type.to_string(),
            document_key: Some(doc! { "_id": sequence }),
            full_document: Some(doc! { "_id": sequence, "qty": sequence }),
            full_document_before_change: None,
            update_description: None,
            expanded: false,
            extra_fields: Document::new(),
        }
    }

    #[test]
    fn recovers_replace_collection_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection =
                CollectionCatalog::new(doc! { "validator": { "qty": { "$gte": 0 } } });
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "a", "qty": 4 });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("commit mutation");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.file_format_version, FILE_FORMAT_VERSION);
        assert_eq!(inspect.last_applied_sequence, 1);
        assert_eq!(inspect.wal_records_since_checkpoint, 1);
        assert_eq!(inspect.databases, vec!["app".to_string()]);
    }

    #[test]
    fn recovers_incremental_collection_creation_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-create-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: Some(doc! { "validator": { "qty": { "$gte": 0 } } }),
                    inserts: vec![CollectionRecord {
                        record_id: 1,
                        document: doc! { "_id": 1, "sku": "alpha", "qty": 4 },
                    }],
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: vec![sample_change_event(1, "insert")],
                })
                .expect("commit mutation");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(
            collection
                .options
                .get_document("validator")
                .expect("validator")
                .get_document("qty")
                .expect("qty")
                .get_i32("$gte")
                .expect("$gte"),
            0
        );
        assert_eq!(collection.records.len(), 1);
        assert_eq!(collection.records[0].record_id, 1);
        assert_eq!(
            collection.records[0].document.get_str("sku").expect("sku"),
            "alpha"
        );
        assert_eq!(
            reopened.change_events(),
            &[sample_change_event(1, "insert")]
        );
    }

    #[test]
    fn recovers_incremental_collection_changes_and_indexes_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-delta-recovery.mongodb");
        let change_events = vec![
            sample_change_event(2, "insert"),
            sample_change_event(3, "update"),
            sample_change_event(4, "delete"),
        ];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(
                &mut collection,
                1,
                doc! { "_id": 1, "sku": "alpha", "qty": 1 },
            );
            insert_record(
                &mut collection,
                2,
                doc! { "_id": 2, "sku": "beta", "qty": 2 },
            );
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("base mutation");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    inserts: vec![CollectionRecord {
                        record_id: 3,
                        document: doc! { "_id": 3, "sku": "charlie", "qty": 3 },
                    }],
                    updates: vec![CollectionRecord {
                        record_id: 1,
                        document: doc! { "_id": 1, "sku": "delta", "qty": 9 },
                    }],
                    deletes: vec![2],
                    change_events: change_events.clone(),
                })
                .expect("delta mutation");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(
            collection
                .records
                .iter()
                .map(|record| record.record_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            collection.records[0].document.get_str("sku").expect("sku"),
            "delta"
        );
        assert_eq!(
            collection.records[1].document.get_str("sku").expect("sku"),
            "charlie"
        );
        let index = collection.indexes.get("sku_1").expect("sku index");
        assert_eq!(
            index
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.record_id,
                        entry.key.get_str("sku").expect("sku").to_string(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(3, "charlie".to_string()), (1, "delta".to_string())]
        );
        assert_eq!(reopened.change_events(), change_events.as_slice());
    }

    #[test]
    fn checkpoints_persist_record_pages_and_record_ids() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("page-persist.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(
                &mut collection,
                7,
                doc! { "_id": 1, "sku": "alpha", "qty": 1 },
            );
            insert_record(
                &mut collection,
                12,
                doc! { "_id": 2, "sku": "beta", "qty": 2 },
            );
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(collection.records.len(), 2);
        assert_eq!(collection.records[0].record_id, 7);
        assert_eq!(collection.records[1].record_id, 12);
        let sku_index = collection.indexes.get("sku_1").expect("sku index");
        assert_eq!(sku_index.entries.len(), 2);
        assert_eq!(sku_index.entries[0].record_id, 7);
        assert_eq!(sku_index.entries[1].record_id, 12);
        assert_eq!(
            collection.records[1].document.get_str("sku").expect("sku"),
            "beta"
        );
    }

    #[test]
    fn reopens_persisted_index_expire_after_seconds_metadata() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("ttl-index-persist.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            apply_index_specs(
                &mut collection,
                &[doc! {
                    "key": { "createdAt": 1 },
                    "name": "createdAt_1",
                    "expireAfterSeconds": 1
                }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(
            collection
                .indexes
                .get("createdAt_1")
                .expect("index")
                .expire_after_seconds,
            Some(1)
        );
    }

    #[test]
    fn reopens_index_entry_presence_for_null_and_missing_fields() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("presence-persist.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "missing" });
            insert_record(
                &mut collection,
                2,
                doc! { "_id": 2, "sku": "null", "flag": bson::Bson::Null },
            );
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "flag": 1, "sku": 1 }, "name": "flag_1_sku_1" }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        let index = collection
            .indexes
            .get("flag_1_sku_1")
            .expect("compound index");
        let entries_by_record = index
            .entries
            .iter()
            .map(|entry| (entry.record_id, entry))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            entries_by_record
                .get(&1)
                .expect("missing-field entry")
                .present_fields,
            vec!["sku".to_string()]
        );
        assert_eq!(
            entries_by_record
                .get(&2)
                .expect("null-field entry")
                .present_fields,
            vec!["flag".to_string(), "sku".to_string()]
        );
        assert_eq!(
            entries_by_record
                .get(&1)
                .expect("missing-field entry")
                .key
                .get("flag"),
            Some(&bson::Bson::Null)
        );
        assert_eq!(
            entries_by_record
                .get(&2)
                .expect("null-field entry")
                .key
                .get("flag"),
            Some(&bson::Bson::Null)
        );
    }

    #[test]
    fn reopens_persisted_plan_cache_entries() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("plan-cache-persist.mongodb");
        let entries = vec![PersistedPlanCacheEntry {
            namespace: "app.widgets".to_string(),
            filter_shape: "sku:eq".to_string(),
            sort_shape: "-".to_string(),
            projection_shape: "-".to_string(),
            sequence: 3,
            choice: PersistedPlanCacheChoice::Index("sku_1".to_string()),
        }];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database.set_persisted_plan_cache_entries(entries.clone());
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        assert_eq!(reopened.persisted_plan_cache_entries(), entries.as_slice());
    }

    #[test]
    fn recovers_change_events_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("change-events-wal.mongodb");
        let change_events = vec![sample_change_event(1, "insert")];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "qty": 1 });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: change_events.clone(),
                })
                .expect("mutation");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        assert_eq!(reopened.change_events(), change_events.as_slice());
    }

    #[test]
    fn checkpoints_persist_change_event_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("change-events-checkpoint.mongodb");
        let change_events = vec![
            sample_change_event(1, "insert"),
            sample_change_event(2, "update"),
        ];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "qty": 2 });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: change_events.clone(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        assert_eq!(reopened.change_events(), change_events.as_slice());

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.current_change_event_count, 2);
        assert_eq!(inspect.checkpoint_change_event_count, 2);
        assert!(inspect.checkpoint_change_event_page_count >= 1);

        let verify = DatabaseFile::verify(&path).expect("verify");
        assert_eq!(verify.change_event_count, 2);
        assert!(verify.change_event_page_count >= 1);
    }

    #[test]
    fn reopens_compound_descending_indexes_in_persisted_order() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("compound-descending.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(
                &mut collection,
                1,
                doc! { "_id": 1, "category": "tools", "qty": 9 },
            );
            insert_record(
                &mut collection,
                2,
                doc! { "_id": 2, "category": "tools", "qty": 3 },
            );
            insert_record(
                &mut collection,
                3,
                doc! { "_id": 3, "category": "tools", "qty": 5 },
            );
            insert_record(
                &mut collection,
                4,
                doc! { "_id": 4, "category": "garden", "qty": 1 },
            );
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1" }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        let index = collection
            .indexes
            .get("category_1_qty_-1")
            .expect("compound index");
        assert_eq!(
            index
                .entries
                .iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![4, 1, 3, 2]
        );
        let record_ids = index.scan_bounds(&IndexBounds {
            lower: Some(IndexBound {
                key: doc! { "category": "tools", "qty": bson::Bson::MaxKey },
                inclusive: true,
            }),
            upper: Some(IndexBound {
                key: doc! { "category": "tools", "qty": bson::Bson::MinKey },
                inclusive: true,
            }),
        });
        assert_eq!(record_ids, vec![1, 3, 2]);
    }

    #[test]
    fn spills_large_collections_across_multiple_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("multi-page.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            for record_id in 1..=12_u64 {
                insert_record(
                    &mut collection,
                    record_id,
                    doc! {
                        "_id": record_id as i64,
                        "payload": "x".repeat(PAGE_SIZE / 3),
                    },
                );
            }
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert!(inspect.checkpoint_record_page_count >= 2);
        assert_eq!(inspect.checkpoint_record_count, 12);
        assert_eq!(inspect.checkpoint_index_entry_count, 12);
    }

    #[test]
    fn spills_large_index_entries_across_multiple_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("index-pages.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            for record_id in 1..=300_u64 {
                insert_record(
                    &mut collection,
                    record_id,
                    doc! { "_id": record_id as i64, "sku": format!("sku-{record_id:04}") },
                );
            }
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert!(inspect.checkpoint_index_page_count >= 2);
        assert_eq!(inspect.checkpoint_index_entry_count, 600);

        let snapshot = latest_snapshot(&path, inspect.active_superblock_slot);
        let index = snapshot
            .catalog
            .databases
            .get("app")
            .expect("database")
            .collections
            .get("widgets")
            .expect("collection")
            .indexes
            .get("sku_1")
            .expect("index");
        let root_page_ref = index
            .pages
            .iter()
            .find(|page_ref| Some(page_ref.page_id) == index.root_page_id)
            .expect("root page ref");
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open file");
        let root_page = decode_page(&mut file, root_page_ref).expect("decode page");
        assert_eq!(root_page.page_kind, PAGE_KIND_INDEX_INTERNAL);
    }

    #[test]
    fn ignores_truncated_wal_tail_during_recovery() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("truncated-tail.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1 });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open raw file");
        file.seek(SeekFrom::End(0)).expect("seek");
        file.write_all(b"WAL1").expect("write partial frame");
        file.flush().expect("flush");

        let report = DatabaseFile::verify(&path).expect("verify");
        assert!(report.truncated_wal_tail);
        assert_eq!(report.wal_records_since_checkpoint, 1);
    }

    #[test]
    fn falls_back_to_previous_superblock_when_latest_pages_are_corrupted() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("corrupt-page.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "payload": "hello" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        let snapshot = latest_snapshot(&path, inspect.active_superblock_slot);
        let page_offset = snapshot
            .catalog
            .databases
            .get("app")
            .expect("database")
            .collections
            .get("widgets")
            .expect("collection")
            .record_pages[0]
            .offset;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open raw file");
        file.seek(SeekFrom::Start(page_offset))
            .expect("seek to page");
        file.write_all(&[0xFF]).expect("corrupt first byte");
        file.flush().expect("flush");

        let report = DatabaseFile::verify(&path).expect("verify should fall back");
        assert_eq!(inspect.checkpoint_page_count, 2);
        assert_eq!(report.page_count, 0);
        assert_eq!(report.record_count, 1);
        assert_eq!(report.index_entry_count, 1);
        assert_eq!(report.last_applied_sequence, 1);
    }

    #[test]
    fn falls_back_to_previous_superblock_when_latest_index_pages_are_corrupted() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("corrupt-index.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "alpha" });
            insert_record(&mut collection, 2, doc! { "_id": 2, "sku": "beta" });
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        let snapshot = latest_snapshot(&path, inspect.active_superblock_slot);
        let index_page_offset = snapshot
            .catalog
            .databases
            .get("app")
            .expect("database")
            .collections
            .get("widgets")
            .expect("collection")
            .indexes
            .get("sku_1")
            .expect("index")
            .pages[0]
            .offset;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open raw file");
        file.seek(SeekFrom::Start(index_page_offset))
            .expect("seek to page");
        file.write_all(&[0xAA]).expect("corrupt first byte");
        file.flush().expect("flush");

        let report = DatabaseFile::verify(&path).expect("verify should fall back");
        assert_eq!(report.record_count, 2);
        assert_eq!(report.index_entry_count, 4);
        assert_eq!(report.page_count, 0);
        assert_eq!(report.last_applied_sequence, 1);
    }

    #[test]
    fn verifies_database_file() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("verify.mongodb");
        let _database = DatabaseFile::open_or_create(&path).expect("create database");

        let report = DatabaseFile::verify(&path).expect("verify");
        assert_eq!(
            report,
            VerifyReport {
                valid: true,
                file_format_version: FILE_FORMAT_VERSION,
                checkpoint_generation: 1,
                last_applied_sequence: 0,
                databases: 0,
                collections: 0,
                record_count: 0,
                index_entry_count: 0,
                change_event_count: 0,
                page_count: 0,
                record_page_count: 0,
                index_page_count: 0,
                change_event_page_count: 0,
                wal_records_since_checkpoint: 0,
                truncated_wal_tail: false,
            }
        );
    }

    #[test]
    fn initializes_file_with_reserved_metadata_region() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("reserved-region.mongodb");
        let _database = DatabaseFile::open_or_create(&path).expect("create database");

        let metadata = std::fs::metadata(&path).expect("metadata");
        assert!(metadata.len() >= DATA_START_OFFSET);
    }
}
