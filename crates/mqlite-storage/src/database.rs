use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use blake3::Hasher;
use ciborium::{de as cbor_de, ser as cbor_ser};
use fs4::FileExt;
use mqlite_catalog::{
    Catalog, CatalogError, CollectionCatalog, CollectionMutation, CollectionRecord, IndexCatalog,
    IndexEntry, build_index_specs, validate_collection_indexes, validate_drop_indexes,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    engine::{CollectionMetadata, CollectionReadView, IndexMetadata, StorageEngine},
    v2::{
        checkpoint as v2_checkpoint, engine as v2_engine, layout as v2_layout,
        pager::Pager as V2Pager,
    },
};

pub const FILE_MAGIC: &[u8; 8] = v2_layout::FILE_MAGIC;
pub const FILE_FORMAT_VERSION: u32 = v2_layout::FILE_FORMAT_VERSION;
pub const PAGE_SIZE: usize = v2_layout::DEFAULT_PAGE_SIZE as usize;
const DATA_START_OFFSET: u64 = v2_layout::DATA_START_OFFSET;
const WAL_FRAME_MAGIC: &[u8; 4] = b"WAL1";
const WAL_HEADER_LEN: usize = 40;
const ZSTD_BLOB_MAGIC: &[u8; 8] = b"MQLTZST1";
const ZSTD_BLOB_HEADER_LEN: usize = 16;
const ZSTD_COMPRESSION_LEVEL: i32 = 1;
const COMPRESSION_MIN_SAVINGS_DIVISOR: usize = 8;
const WAL_COMPRESSION_MIN_LEN: usize = PAGE_SIZE;
const WAL_COMPRESSION_MIN_SAVINGS: usize = 512;
pub const EMPTY_BSON_DOCUMENT_BYTES: &[u8; 5] = &[5, 0, 0, 0, 0];

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
    token: Vec<u8>,
    pub cluster_time: bson::Timestamp,
    pub wall_time: bson::DateTime,
    pub database: String,
    pub collection: Option<String>,
    pub operation_type: String,
    document_key: Option<Vec<u8>>,
    full_document: Option<Vec<u8>>,
    full_document_before_change: Option<Vec<u8>>,
    update_description: Option<Vec<u8>>,
    pub expanded: bool,
    extra_fields: Vec<u8>,
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
pub enum CollectionChange {
    Insert(CollectionRecord),
    Update(CollectionRecord),
    Delete(u64),
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
    RewriteCollection {
        database: String,
        collection: String,
        options: bson::Document,
        #[serde(default)]
        changes: Vec<CollectionChange>,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
    ApplyCollectionChanges {
        database: String,
        collection: String,
        #[serde(default)]
        create_options: Option<bson::Document>,
        #[serde(default)]
        changes: Vec<CollectionChange>,
        // Retained for WAL backward compatibility with pre-ordered delta frames.
        #[serde(default)]
        inserts: Vec<CollectionRecord>,
        #[serde(default)]
        updates: Vec<CollectionRecord>,
        #[serde(default)]
        deletes: Vec<u64>,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
    CreateIndexes {
        database: String,
        collection: String,
        #[serde(default)]
        create_options: Option<bson::Document>,
        specs: Vec<bson::Document>,
        #[serde(default)]
        change_events: Vec<PersistedChangeEvent>,
    },
    DropIndexes {
        database: String,
        collection: String,
        target: String,
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
    validation_state: ValidationState,
    durable_sequence: u64,
    checkpoint_plan_cache_entries: Vec<PersistedPlanCacheEntry>,
    active_slot: usize,
    active_superblock: v2_layout::Superblock,
    valid_superblocks: usize,
    wal_end_offset: u64,
    dirty_collections: BTreeSet<(String, String)>,
    change_events_dirty: bool,
    wal_records_since_checkpoint: usize,
    wal_bytes_since_checkpoint: u64,
    truncated_wal_tail: bool,
    checkpoint_counts: CheckpointCounts,
    wal_sync_count: usize,
    concurrent_checkpoint: Option<PendingConcurrentCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartupMetadata {
    pub durable_sequence: u64,
    pub has_pending_wal: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoReport {
    pub path: PathBuf,
    pub file_format_version: u32,
    pub file_size: u64,
    pub last_applied_sequence: u64,
    pub summary: InfoSummary,
    pub last_checkpoint: InfoCheckpoint,
    pub wal_since_checkpoint: InfoWal,
    pub databases: Vec<InfoDatabase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoSummary {
    pub database_count: usize,
    pub collection_count: usize,
    pub index_count: usize,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub change_event_count: usize,
    pub plan_cache_entry_count: usize,
    pub document_bytes: u64,
    pub index_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoCheckpoint {
    pub generation: u64,
    pub last_applied_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
    pub active_superblock_slot: usize,
    pub valid_superblocks: usize,
    pub database_count: usize,
    pub collection_count: usize,
    pub index_count: usize,
    pub snapshot_offset: u64,
    pub snapshot_len: u64,
    pub wal_offset: u64,
    pub page_size: usize,
    pub page_count: usize,
    pub page_bytes: u64,
    pub record_page_count: usize,
    pub record_page_bytes: u64,
    pub index_page_count: usize,
    pub index_page_bytes: u64,
    pub change_event_page_count: usize,
    pub change_event_page_bytes: u64,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub change_event_count: usize,
    pub plan_cache_entry_count: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoWal {
    pub record_count: usize,
    pub bytes: u64,
    pub truncated_tail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoDatabase {
    pub name: String,
    pub collection_count: usize,
    pub index_count: usize,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub document_bytes: u64,
    pub index_bytes: u64,
    pub total_bytes: u64,
    pub checkpoint: InfoDatabaseCheckpoint,
    pub collections: Vec<InfoCollection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct InfoDatabaseCheckpoint {
    pub collection_count: usize,
    pub index_count: usize,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub record_page_count: usize,
    pub record_page_bytes: u64,
    pub index_page_count: usize,
    pub index_page_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoCollection {
    pub name: String,
    pub document_count: usize,
    pub index_count: usize,
    pub index_entry_count: usize,
    pub document_bytes: u64,
    pub index_bytes: u64,
    pub total_bytes: u64,
    pub checkpoint: InfoCollectionCheckpoint,
    pub indexes: Vec<InfoIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct InfoCollectionCheckpoint {
    pub index_count: usize,
    pub record_count: usize,
    pub index_entry_count: usize,
    pub record_page_count: usize,
    pub record_page_bytes: u64,
    pub index_page_count: usize,
    pub index_page_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoIndex {
    pub name: String,
    pub key: bson::Document,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
    pub entry_count: usize,
    pub bytes: u64,
    pub checkpoint: InfoIndexCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct InfoIndexCheckpoint {
    pub entry_count: usize,
    pub page_count: usize,
    pub page_bytes: u64,
    pub root_page_id: Option<u64>,
    pub total_bytes: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("file is truncated")]
    Truncated,
    #[error("invalid wal frame")]
    InvalidWalFrame,
    #[error("wal checksum mismatch")]
    InvalidWalChecksum,
    #[error("duplicate key error on index `{0}`")]
    DuplicateKey(String),
    #[error("invalid persisted index state")]
    InvalidIndexState,
    #[error("a concurrent checkpoint is already in progress")]
    ConcurrentCheckpointInProgress,
    #[error("no reusable checkpoint space is available for a concurrent checkpoint")]
    ConcurrentCheckpointNoReusableSpace,
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
    dirty_collections: BTreeSet<(String, String)>,
    change_events_dirty: bool,
}

#[derive(Debug, Default)]
struct WalMetadata {
    records: usize,
    bytes: u64,
    truncated_tail: bool,
}

#[derive(Debug)]
struct LoadedV2State {
    state: PersistedState,
    active_slot: usize,
    active_superblock: v2_layout::Superblock,
    valid_superblocks: usize,
    wal_recovery: WalRecovery,
    file_size: u64,
    checkpoint_counts: CheckpointCounts,
}

#[derive(Debug, Default)]
struct WalCatalogMetadata {
    databases: BTreeMap<String, WalDatabaseMetadata>,
    change_event_count: usize,
}

#[derive(Debug, Default)]
struct WalDatabaseMetadata {
    collections: BTreeMap<String, WalCollectionMetadata>,
}

#[derive(Debug, Default)]
struct WalCollectionMetadata {
    indexes: BTreeMap<String, WalIndexMetadata>,
    record_sizes: HashMap<u64, usize>,
    document_count: usize,
    document_bytes: u64,
}

#[derive(Debug, Clone)]
struct WalIndexMetadata {
    key: bson::Document,
    unique: bool,
    expire_after_seconds: Option<i64>,
    entry_count: usize,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactWalEntry {
    sequence: u64,
    mutation: CompactWalMutation,
}

#[derive(Debug, Serialize)]
struct EncodedWalEntry<'a> {
    sequence: u64,
    mutation: EncodedWalMutation<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CompactWalMutation {
    ReplaceCollection {
        database: String,
        collection: String,
        collection_state: CompactCollectionCatalog,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
    RewriteCollection {
        database: String,
        collection: String,
        options: Vec<u8>,
        changes: Vec<CompactCollectionChange>,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
    ApplyCollectionChanges {
        database: String,
        collection: String,
        create_options: Option<Vec<u8>>,
        changes: Vec<CompactCollectionChange>,
        inserts: Vec<CompactCollectionRecord>,
        updates: Vec<CompactCollectionRecord>,
        deletes: Vec<u64>,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
    CreateIndexes {
        database: String,
        collection: String,
        create_options: Option<Vec<u8>>,
        specs: Vec<Vec<u8>>,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
    DropIndexes {
        database: String,
        collection: String,
        target: String,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
    DropCollection {
        database: String,
        collection: String,
        change_events: Vec<CompactPersistedChangeEvent>,
    },
}

#[derive(Debug, Serialize)]
enum EncodedWalMutation<'a> {
    ReplaceCollection {
        database: &'a str,
        collection: &'a str,
        collection_state: CompactCollectionCatalog,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
    RewriteCollection {
        database: &'a str,
        collection: &'a str,
        options: Vec<u8>,
        changes: Vec<CompactCollectionChange>,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
    ApplyCollectionChanges {
        database: &'a str,
        collection: &'a str,
        create_options: Option<Vec<u8>>,
        changes: Vec<CompactCollectionChange>,
        inserts: Vec<CompactCollectionRecord>,
        updates: Vec<CompactCollectionRecord>,
        deletes: Vec<u64>,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
    CreateIndexes {
        database: &'a str,
        collection: &'a str,
        create_options: Option<Vec<u8>>,
        specs: Vec<Vec<u8>>,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
    DropIndexes {
        database: &'a str,
        collection: &'a str,
        target: &'a str,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
    DropCollection {
        database: &'a str,
        collection: &'a str,
        change_events: Vec<EncodedPersistedChangeEvent<'a>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactCollectionCatalog {
    options: Vec<u8>,
    indexes: BTreeMap<String, CompactIndexCatalog>,
    records: Vec<CompactCollectionRecord>,
    next_record_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactCollectionRecord {
    record_id: u64,
    document: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactIndexCatalog {
    key: Vec<u8>,
    unique: bool,
    expire_after_seconds: Option<i64>,
    entries: Vec<CompactIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactIndexEntry {
    record_id: u64,
    key: Vec<u8>,
    present_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactPersistedChangeEvent {
    token: Vec<u8>,
    cluster_time_time: u32,
    cluster_time_increment: u32,
    wall_time_millis: i64,
    database: String,
    collection: Option<String>,
    operation_type: String,
    document_key: Option<Vec<u8>>,
    full_document: Option<Vec<u8>>,
    full_document_before_change: Option<Vec<u8>>,
    update_description: Option<Vec<u8>>,
    expanded: bool,
    extra_fields: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct EncodedPersistedChangeEvent<'a> {
    token: &'a [u8],
    cluster_time_time: u32,
    cluster_time_increment: u32,
    wall_time_millis: i64,
    database: &'a str,
    collection: Option<&'a str>,
    operation_type: &'a str,
    document_key: Option<&'a [u8]>,
    full_document: Option<&'a [u8]>,
    full_document_before_change: Option<&'a [u8]>,
    update_description: Option<&'a [u8]>,
    expanded: bool,
    extra_fields: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CompactCollectionChange {
    Insert(CompactCollectionRecord),
    Update(CompactCollectionRecord),
    Delete(u64),
}

#[derive(Debug, Clone, Copy, Default)]
struct CheckpointCounts {
    page_count: usize,
    record_page_count: usize,
    index_page_count: usize,
    change_event_page_count: usize,
}

#[derive(Debug)]
struct PendingConcurrentCheckpoint {
    sequence: u64,
    dirty_collections: Arc<BTreeSet<(String, String)>>,
    change_events_dirty: bool,
    wal_records_since_checkpoint: usize,
    wal_bytes_since_checkpoint: u64,
}

#[derive(Debug)]
pub struct ConcurrentCheckpointJob {
    path: PathBuf,
    state: PersistedState,
    active_slot: usize,
    active_generation: u64,
    previous_wal_start_offset: u64,
    captured_wal_bytes: u64,
}

#[derive(Debug)]
pub struct CompletedConcurrentCheckpoint {
    sequence: u64,
    active_slot: usize,
    active_superblock: v2_layout::Superblock,
    valid_superblocks: usize,
    checkpoint_counts: CheckpointCounts,
    checkpoint_plan_cache_entries: Vec<PersistedPlanCacheEntry>,
}

impl DatabaseFile {
    pub fn startup_metadata(path: impl AsRef<Path>) -> Result<StartupMetadata> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(StartupMetadata {
                durable_sequence: 0,
                has_pending_wal: false,
            });
        }

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut magic = [0_u8; 8];
        if file.read_exact(&mut magic).is_err() || &magic != FILE_MAGIC {
            return Err(anyhow::anyhow!(
                "existing database file is not a supported v2 mqlite database; create a new file or rewrite `{}` as v2",
                path.display()
            ));
        }

        let pager = V2Pager::open(path)?;
        let file_size = std::fs::metadata(path)?.len();
        Ok(StartupMetadata {
            durable_sequence: pager.active_superblock().durable_lsn,
            has_pending_wal: file_size > pager.active_superblock().wal_start_offset,
        })
    }

    pub fn open_page_backed_collection_read_view(
        path: impl AsRef<Path>,
        database: &str,
        collection: &str,
    ) -> Result<Option<Box<dyn CollectionReadView>>> {
        Ok(
            v2_engine::open_collection_read_view(path, database, collection)?
                .map(|view| Box::new(view) as Box<dyn CollectionReadView>),
        )
    }

    pub fn read_plan_cache_entries(path: impl AsRef<Path>) -> Result<Vec<PersistedPlanCacheEntry>> {
        v2_engine::load_plan_cache_entries_only(path)
    }

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
            path: path.clone(),
            file,
            state: PersistedState {
                file_format_version: v2_layout::FILE_FORMAT_VERSION,
                last_applied_sequence: 0,
                last_checkpoint_unix_ms: current_unix_ms(),
                catalog: Catalog::new(),
                change_events: Vec::new(),
                plan_cache_entries: Vec::new(),
            },
            validation_state: ValidationState::default(),
            durable_sequence: 0,
            checkpoint_plan_cache_entries: Vec::new(),
            active_slot: 0,
            active_superblock: v2_layout::Superblock::default(),
            valid_superblocks: 0,
            wal_end_offset: DATA_START_OFFSET,
            dirty_collections: BTreeSet::new(),
            change_events_dirty: false,
            wal_records_since_checkpoint: 0,
            wal_bytes_since_checkpoint: 0,
            truncated_wal_tail: false,
            checkpoint_counts: CheckpointCounts::default(),
            wal_sync_count: 0,
            concurrent_checkpoint: None,
        };

        if database.file.metadata()?.len() == 0 {
            database.initialize_file()?;
        } else {
            if !v2_engine::is_v2_file(&path)? {
                return Err(anyhow::anyhow!(
                    "existing database file is not a supported v2 mqlite database; create a new file or rewrite `{}` as v2",
                    path.display()
                ));
            }
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

    pub fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }

    pub fn change_events(&self) -> &[PersistedChangeEvent] {
        &self.state.change_events
    }

    pub fn has_pending_wal(&self) -> bool {
        self.wal_records_since_checkpoint > 0
    }

    pub fn has_concurrent_checkpoint(&self) -> bool {
        self.concurrent_checkpoint.is_some()
    }

    pub fn persisted_plan_cache_entries(&self) -> &[PersistedPlanCacheEntry] {
        &self.state.plan_cache_entries
    }

    pub fn wal_sync_count(&self) -> usize {
        self.wal_sync_count
    }

    pub fn set_persisted_plan_cache_entries(&mut self, mut entries: Vec<PersistedPlanCacheEntry>) {
        entries.sort();
        entries.dedup();
        self.state.plan_cache_entries = entries;
    }

    pub fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64> {
        let sequence = self.commit_mutation_unflushed(mutation)?;
        self.sync_pending_wal()?;
        Ok(sequence)
    }

    pub fn commit_mutation_unflushed(&mut self, mutation: WalMutation) -> Result<u64> {
        let sequence = self.state.last_applied_sequence + 1;
        let validation_plan = validate_mutation(&self.state, &self.validation_state, &mutation)?;

        let appended_bytes = append_wal_entry(
            &mut self.file,
            self.wal_end_offset,
            sequence,
            &mutation,
            false,
        )?;

        mark_mutation_dirty(
            &mut self.dirty_collections,
            &mut self.change_events_dirty,
            &mutation,
        );
        apply_owned_mutation(&mut self.state, sequence, mutation)?;
        self.validation_state
            .apply_plan(&self.state.catalog, validation_plan)?;
        self.wal_end_offset += appended_bytes;
        self.wal_records_since_checkpoint += 1;
        self.wal_bytes_since_checkpoint += appended_bytes;
        self.truncated_wal_tail = false;
        Ok(sequence)
    }

    pub fn sync_pending_wal(&mut self) -> Result<u64> {
        if self.durable_sequence >= self.state.last_applied_sequence {
            return Ok(self.durable_sequence);
        }

        self.file.flush()?;
        self.file.sync_data()?;
        self.durable_sequence = self.state.last_applied_sequence;
        self.wal_sync_count += 1;
        Ok(self.durable_sequence)
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        if self.concurrent_checkpoint.is_some() {
            return Err(StorageError::ConcurrentCheckpointInProgress.into());
        }
        self.write_checkpoint()
    }

    pub fn prepare_concurrent_checkpoint(&mut self) -> Result<Option<ConcurrentCheckpointJob>> {
        if self.concurrent_checkpoint.is_some() {
            return Ok(None);
        }
        if self.durable_sequence < self.state.last_applied_sequence {
            return Ok(None);
        }
        if !self.has_pending_wal()
            && self.checkpoint_plan_cache_entries == self.state.plan_cache_entries
        {
            return Ok(None);
        }

        let last_checkpoint_unix_ms = current_unix_ms();
        let mut state = self.state.clone();
        state.file_format_version = v2_layout::FILE_FORMAT_VERSION;
        state.last_checkpoint_unix_ms = last_checkpoint_unix_ms;

        let dirty_collections = Arc::new(std::mem::take(&mut self.dirty_collections));
        let pending = PendingConcurrentCheckpoint {
            sequence: state.last_applied_sequence,
            dirty_collections: Arc::clone(&dirty_collections),
            change_events_dirty: self.change_events_dirty,
            wal_records_since_checkpoint: self.wal_records_since_checkpoint,
            wal_bytes_since_checkpoint: self.wal_bytes_since_checkpoint,
        };
        let captured_wal_bytes = pending.wal_bytes_since_checkpoint;
        self.change_events_dirty = false;
        self.wal_records_since_checkpoint = 0;
        self.wal_bytes_since_checkpoint = 0;
        self.concurrent_checkpoint = Some(pending);

        Ok(Some(ConcurrentCheckpointJob {
            path: self.path.clone(),
            state,
            active_slot: self.active_slot,
            active_generation: self.active_superblock.generation,
            previous_wal_start_offset: self.active_superblock.wal_start_offset,
            captured_wal_bytes,
        }))
    }

    pub fn finish_concurrent_checkpoint(
        &mut self,
        completed: CompletedConcurrentCheckpoint,
    ) -> Result<bool> {
        let Some(pending) = self.concurrent_checkpoint.take() else {
            return Ok(false);
        };
        if pending.sequence != completed.sequence {
            self.concurrent_checkpoint = Some(pending);
            return Ok(false);
        }

        self.active_slot = completed.active_slot;
        self.active_superblock = completed.active_superblock;
        self.valid_superblocks = completed.valid_superblocks.max(1);
        self.checkpoint_counts = completed.checkpoint_counts;
        self.state.last_checkpoint_unix_ms = self.active_superblock.last_checkpoint_unix_ms;
        self.checkpoint_plan_cache_entries = completed.checkpoint_plan_cache_entries;
        Ok(true)
    }

    pub fn abort_concurrent_checkpoint(&mut self) -> bool {
        let Some(pending) = self.concurrent_checkpoint.take() else {
            return false;
        };
        self.dirty_collections
            .extend(pending.dirty_collections.iter().cloned());
        self.change_events_dirty |= pending.change_events_dirty;
        self.wal_records_since_checkpoint += pending.wal_records_since_checkpoint;
        self.wal_bytes_since_checkpoint += pending.wal_bytes_since_checkpoint;
        true
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn inspect(path: impl AsRef<Path>) -> Result<InspectReport> {
        let path = path.as_ref().to_path_buf();
        if !v2_engine::is_v2_file(&path)? {
            return Err(anyhow::anyhow!(
                "existing database file is not a supported v2 mqlite database; create a new file or rewrite `{}` as v2",
                path.display()
            ));
        }
        let checkpoint = v2_engine::read_info(&path)?;
        let mut current = wal_metadata_from_info_report(&checkpoint);
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let wal_metadata = apply_wal_catalog_metadata(
            &mut file,
            checkpoint.last_checkpoint.wal_offset,
            &mut current,
        )?;
        if wal_metadata.records == 0 && !wal_metadata.truncated_tail {
            return v2_engine::read_inspect(&path);
        }
        Ok(build_v2_wal_inspect_report(
            path,
            &checkpoint,
            &current,
            &wal_metadata,
            file.metadata()?.len(),
        ))
    }

    pub fn info(path: impl AsRef<Path>) -> Result<InfoReport> {
        let path = path.as_ref().to_path_buf();
        if !v2_engine::is_v2_file(&path)? {
            return Err(anyhow::anyhow!(
                "existing database file is not a supported v2 mqlite database; create a new file or rewrite `{}` as v2",
                path.display()
            ));
        }
        let checkpoint = v2_engine::read_info(&path)?;
        let mut current = wal_metadata_from_info_report(&checkpoint);
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let wal_metadata = apply_wal_catalog_metadata(
            &mut file,
            checkpoint.last_checkpoint.wal_offset,
            &mut current,
        )?;
        if wal_metadata.records == 0 && !wal_metadata.truncated_tail {
            return Ok(checkpoint);
        }
        Ok(build_v2_wal_info_report(
            path,
            checkpoint,
            &current,
            &wal_metadata,
            file.metadata()?.len(),
        ))
    }

    pub fn verify(path: impl AsRef<Path>) -> Result<VerifyReport> {
        let path = path.as_ref();
        if !v2_engine::is_v2_file(path)? {
            return Err(anyhow::anyhow!(
                "existing database file is not a supported v2 mqlite database; create a new file or rewrite `{}` as v2",
                path.display()
            ));
        }
        let mut file = OpenOptions::new().read(true).open(path)?;
        let loaded = load_v2_state(path, &mut file)?;
        let collections = loaded
            .state
            .catalog
            .databases
            .values()
            .map(|database| database.collections.len())
            .sum();

        Ok(VerifyReport {
            valid: true,
            file_format_version: v2_layout::FILE_FORMAT_VERSION,
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
        v2_checkpoint::initialize_empty_file(&mut self.file)?;
        self.write_checkpoint()?;
        self.reload_from_disk()
    }

    fn reload_from_disk(&mut self) -> Result<()> {
        let loaded = load_v2_state(&self.path, &mut self.file)?;
        self.state = loaded.state;
        self.validation_state = ValidationState::build(&self.state.catalog)?;
        self.durable_sequence = loaded.active_superblock.durable_lsn;
        self.checkpoint_plan_cache_entries = self.state.plan_cache_entries.clone();
        self.active_slot = loaded.active_slot;
        self.active_superblock = loaded.active_superblock;
        self.valid_superblocks = loaded.valid_superblocks;
        self.wal_end_offset = loaded
            .file_size
            .max(self.active_superblock.wal_start_offset);
        self.dirty_collections = loaded.wal_recovery.dirty_collections;
        self.change_events_dirty = loaded.wal_recovery.change_events_dirty;
        self.wal_records_since_checkpoint = loaded.wal_recovery.records;
        self.wal_bytes_since_checkpoint = loaded.wal_recovery.bytes;
        self.truncated_wal_tail = loaded.wal_recovery.truncated_tail;
        self.checkpoint_counts = loaded.checkpoint_counts;
        self.wal_sync_count = 0;
        self.concurrent_checkpoint = None;
        Ok(())
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        self.state.file_format_version = v2_layout::FILE_FORMAT_VERSION;
        self.state.last_checkpoint_unix_ms = current_unix_ms();
        let completed = v2_checkpoint::write_state_checkpoint_to_file(
            &mut self.file,
            &self.state,
            self.active_slot,
            self.active_superblock.generation,
        )?;
        self.active_slot = completed.active_superblock_slot;
        self.active_superblock = completed.active_superblock.clone();
        self.valid_superblocks = if completed.active_superblock.generation > 1 {
            2
        } else {
            1
        };
        self.checkpoint_plan_cache_entries = self.state.plan_cache_entries.clone();
        self.dirty_collections.clear();
        self.change_events_dirty = false;
        self.wal_end_offset = completed.file_size;
        self.wal_records_since_checkpoint = 0;
        self.wal_bytes_since_checkpoint = 0;
        self.truncated_wal_tail = false;
        self.durable_sequence = self.state.last_applied_sequence;
        self.checkpoint_counts = v2_checkpoint_counts(&completed.active_superblock);
        Ok(())
    }
}

impl StorageEngine for DatabaseFile {
    fn catalog(&self) -> &Catalog {
        DatabaseFile::catalog(self)
    }

    fn database_names(&self) -> Result<Vec<String>> {
        Ok(self.catalog().database_names())
    }

    fn collection_names(&self, database: &str) -> Result<Vec<String>> {
        match self.catalog().collection_names(database) {
            Ok(names) => Ok(names),
            Err(CatalogError::DatabaseNotFound(_)) => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn collection_metadata(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<CollectionMetadata>> {
        match self.catalog().get_collection(database, collection) {
            Ok(collection) => Ok(Some(CollectionMetadata {
                options: collection.options.clone(),
            })),
            Err(CatalogError::NamespaceNotFound(_, _)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn list_indexes(&self, database: &str, collection: &str) -> Result<Option<Vec<IndexMetadata>>> {
        match self.catalog().get_collection(database, collection) {
            Ok(collection) => Ok(Some(
                collection
                    .indexes
                    .values()
                    .map(|index| IndexMetadata {
                        name: index.name.clone(),
                        key_pattern: index.key.clone(),
                        unique: index.unique,
                        expire_after_seconds: index.expire_after_seconds,
                    })
                    .collect(),
            )),
            Err(CatalogError::NamespaceNotFound(_, _)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn collection_read_view(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<&dyn CollectionReadView>> {
        match self.catalog().get_collection(database, collection) {
            Ok(collection) => Ok(Some(collection)),
            Err(CatalogError::NamespaceNotFound(_, _)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn last_applied_sequence(&self) -> u64 {
        DatabaseFile::last_applied_sequence(self)
    }

    fn durable_sequence(&self) -> u64 {
        DatabaseFile::durable_sequence(self)
    }

    fn wal_sync_count(&self) -> usize {
        DatabaseFile::wal_sync_count(self)
    }

    fn change_events(&self) -> &[PersistedChangeEvent] {
        DatabaseFile::change_events(self)
    }

    fn has_pending_wal(&self) -> bool {
        DatabaseFile::has_pending_wal(self)
    }

    fn has_concurrent_checkpoint(&self) -> bool {
        DatabaseFile::has_concurrent_checkpoint(self)
    }

    fn persisted_plan_cache_entries(&self) -> &[PersistedPlanCacheEntry] {
        DatabaseFile::persisted_plan_cache_entries(self)
    }

    fn set_persisted_plan_cache_entries(&mut self, entries: Vec<PersistedPlanCacheEntry>) {
        DatabaseFile::set_persisted_plan_cache_entries(self, entries);
    }

    fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64> {
        DatabaseFile::commit_mutation(self, mutation)
    }

    fn commit_mutation_unflushed(&mut self, mutation: WalMutation) -> Result<u64> {
        DatabaseFile::commit_mutation_unflushed(self, mutation)
    }

    fn sync_pending_wal(&mut self) -> Result<u64> {
        DatabaseFile::sync_pending_wal(self)
    }

    fn checkpoint(&mut self) -> Result<()> {
        DatabaseFile::checkpoint(self)
    }

    fn prepare_concurrent_checkpoint(&mut self) -> Result<Option<ConcurrentCheckpointJob>> {
        DatabaseFile::prepare_concurrent_checkpoint(self)
    }

    fn finish_concurrent_checkpoint(
        &mut self,
        completed: CompletedConcurrentCheckpoint,
    ) -> Result<bool> {
        DatabaseFile::finish_concurrent_checkpoint(self, completed)
    }

    fn abort_concurrent_checkpoint(&mut self) -> bool {
        DatabaseFile::abort_concurrent_checkpoint(self)
    }
}

impl ConcurrentCheckpointJob {
    pub fn run(self) -> Result<Option<CompletedConcurrentCheckpoint>> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let file_size = file.metadata()?.len();
        let preserved_wal_offset = self
            .previous_wal_start_offset
            .saturating_add(self.captured_wal_bytes)
            .min(file_size);
        let mut preserved_wal = Vec::new();
        if preserved_wal_offset < file_size {
            file.seek(SeekFrom::Start(preserved_wal_offset))?;
            preserved_wal.resize((file_size - preserved_wal_offset) as usize, 0);
            file.read_exact(&mut preserved_wal)?;
        }

        let completed = v2_checkpoint::write_state_checkpoint_to_file(
            &mut file,
            &self.state,
            self.active_slot,
            self.active_generation,
        )?;
        let mut active_superblock = completed.active_superblock.clone();
        let mut checkpoint_file_size = completed.file_size;
        if !preserved_wal.is_empty() {
            file.seek(SeekFrom::Start(checkpoint_file_size))?;
            file.write_all(&preserved_wal)?;
            checkpoint_file_size += preserved_wal.len() as u64;
            active_superblock.wal_start_offset = completed.file_size;
            active_superblock.wal_end_offset = checkpoint_file_size;

            let superblock_offset = v2_layout::HEADER_LEN as u64
                + completed.active_superblock_slot as u64 * v2_layout::SUPERBLOCK_LEN as u64;
            file.seek(SeekFrom::Start(superblock_offset))?;
            file.write_all(&active_superblock.encode())?;
            file.flush()?;
            file.sync_all()?;
        }
        Ok(Some(CompletedConcurrentCheckpoint {
            sequence: self.state.last_applied_sequence,
            active_slot: completed.active_superblock_slot,
            active_superblock: active_superblock.clone(),
            valid_superblocks: if active_superblock.generation > 1 {
                2
            } else {
                1
            },
            checkpoint_counts: v2_checkpoint_counts(&active_superblock),
            checkpoint_plan_cache_entries: self.state.plan_cache_entries.clone(),
        }))
    }
}

fn v2_checkpoint_counts(superblock: &v2_layout::Superblock) -> CheckpointCounts {
    CheckpointCounts {
        page_count: superblock.summary.page_count as usize,
        record_page_count: 0,
        index_page_count: 0,
        change_event_page_count: 0,
    }
}

fn load_v2_state(path: &Path, file: &mut File) -> Result<LoadedV2State> {
    let pager = V2Pager::open(path)?;
    let mut state = v2_engine::load_persisted_state(path)?;
    let wal_recovery = replay_wal(file, pager.active_superblock().wal_start_offset, &mut state)?;
    if let Some(last_sequence) = wal_recovery.last_sequence {
        state.last_applied_sequence = last_sequence;
    }
    state.file_format_version = v2_layout::FILE_FORMAT_VERSION;
    Ok(LoadedV2State {
        checkpoint_counts: v2_checkpoint_counts(pager.active_superblock()),
        state,
        active_slot: pager.active_superblock_slot(),
        active_superblock: pager.active_superblock().clone(),
        valid_superblocks: pager.valid_superblocks(),
        wal_recovery,
        file_size: file.metadata()?.len(),
    })
}

fn encode_document_bytes(document: &bson::Document) -> Result<Vec<u8>> {
    Ok(bson::to_vec(document)?)
}

fn decode_document_bytes(bytes: &[u8]) -> Result<bson::Document> {
    Ok(bson::from_slice(bytes)?)
}

fn encode_optional_document_bytes(document: Option<&bson::Document>) -> Result<Option<Vec<u8>>> {
    document.map(encode_document_bytes).transpose()
}

fn decode_optional_document_bytes(bytes: Option<Vec<u8>>) -> Result<Option<bson::Document>> {
    bytes.as_deref().map(decode_document_bytes).transpose()
}

impl PersistedChangeEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn from_encoded_fields(
        token: Vec<u8>,
        cluster_time: bson::Timestamp,
        wall_time: bson::DateTime,
        database: String,
        collection: Option<String>,
        operation_type: String,
        document_key: Option<Vec<u8>>,
        full_document: Option<Vec<u8>>,
        full_document_before_change: Option<Vec<u8>>,
        update_description: Option<Vec<u8>>,
        expanded: bool,
        extra_fields: Vec<u8>,
    ) -> Self {
        Self {
            token,
            cluster_time,
            wall_time,
            database,
            collection,
            operation_type,
            document_key,
            full_document,
            full_document_before_change,
            update_description,
            expanded,
            extra_fields,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        token: &bson::Document,
        cluster_time: bson::Timestamp,
        wall_time: bson::DateTime,
        database: String,
        collection: Option<String>,
        operation_type: String,
        document_key: Option<&bson::Document>,
        full_document: Option<&bson::Document>,
        full_document_before_change: Option<&bson::Document>,
        update_description: Option<&bson::Document>,
        expanded: bool,
        extra_fields: &bson::Document,
    ) -> Result<Self> {
        Ok(Self::from_encoded_fields(
            encode_document_bytes(token)?,
            cluster_time,
            wall_time,
            database,
            collection,
            operation_type,
            encode_optional_document_bytes(document_key)?,
            encode_optional_document_bytes(full_document)?,
            encode_optional_document_bytes(full_document_before_change)?,
            encode_optional_document_bytes(update_description)?,
            expanded,
            encode_document_bytes(extra_fields)?,
        ))
    }

    pub fn token_document(&self) -> Result<bson::Document> {
        decode_document_bytes(&self.token)
    }

    pub fn document_key_document(&self) -> Result<Option<bson::Document>> {
        decode_optional_document_bytes(self.document_key.clone())
    }

    pub fn full_document_document(&self) -> Result<Option<bson::Document>> {
        decode_optional_document_bytes(self.full_document.clone())
    }

    pub fn full_document_before_change_document(&self) -> Result<Option<bson::Document>> {
        decode_optional_document_bytes(self.full_document_before_change.clone())
    }

    pub fn update_description_document(&self) -> Result<Option<bson::Document>> {
        decode_optional_document_bytes(self.update_description.clone())
    }

    pub fn extra_fields_document(&self) -> Result<bson::Document> {
        decode_document_bytes(&self.extra_fields)
    }

    pub fn to_change_stream_document(&self) -> Result<bson::Document> {
        let mut document = bson::doc! {
            "token": bson::Bson::Document(self.token_document()?),
            "clusterTime": bson::Bson::Timestamp(self.cluster_time),
            "wallTime": bson::Bson::DateTime(self.wall_time),
            "database": self.database.clone(),
            "operationType": self.operation_type.clone(),
            "expanded": self.expanded,
            "extraFields": bson::Bson::Document(self.extra_fields_document()?),
        };
        if let Some(collection) = &self.collection {
            document.insert("collection", collection.clone());
        }
        if let Some(document_key) = self.document_key_document()? {
            document.insert("documentKey", bson::Bson::Document(document_key));
        }
        if let Some(full_document) = self.full_document_document()? {
            document.insert("fullDocument", bson::Bson::Document(full_document));
        }
        if let Some(full_document_before_change) = self.full_document_before_change_document()? {
            document.insert(
                "fullDocumentBeforeChange",
                bson::Bson::Document(full_document_before_change),
            );
        }
        if let Some(update_description) = self.update_description_document()? {
            document.insert(
                "updateDescription",
                bson::Bson::Document(update_description),
            );
        }
        Ok(document)
    }
}

impl CompactWalEntry {
    fn into_wal_entry(self) -> Result<WalEntry> {
        Ok(WalEntry {
            sequence: self.sequence,
            mutation: self.mutation.into_wal_mutation()?,
        })
    }
}

impl<'a> EncodedWalEntry<'a> {
    fn from_wal_entry(sequence: u64, mutation: &'a WalMutation) -> Result<Self> {
        Ok(Self {
            sequence,
            mutation: EncodedWalMutation::from_wal_mutation(mutation)?,
        })
    }
}

impl CompactWalMutation {
    fn into_wal_mutation(self) -> Result<WalMutation> {
        Ok(match self {
            Self::ReplaceCollection {
                database,
                collection,
                collection_state,
                change_events,
            } => WalMutation::ReplaceCollection {
                database,
                collection,
                collection_state: collection_state.into_collection_catalog()?,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::RewriteCollection {
                database,
                collection,
                options,
                changes,
                change_events,
            } => WalMutation::RewriteCollection {
                database,
                collection,
                options: decode_document_bytes(&options)?,
                changes: changes
                    .into_iter()
                    .map(CompactCollectionChange::into_collection_change)
                    .collect::<Result<Vec<_>>>()?,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::ApplyCollectionChanges {
                database,
                collection,
                create_options,
                changes,
                inserts,
                updates,
                deletes,
                change_events,
            } => WalMutation::ApplyCollectionChanges {
                database,
                collection,
                create_options: decode_optional_document_bytes(create_options)?,
                changes: changes
                    .into_iter()
                    .map(CompactCollectionChange::into_collection_change)
                    .collect::<Result<Vec<_>>>()?,
                inserts: inserts
                    .into_iter()
                    .map(CompactCollectionRecord::into_collection_record)
                    .collect::<Result<Vec<_>>>()?,
                updates: updates
                    .into_iter()
                    .map(CompactCollectionRecord::into_collection_record)
                    .collect::<Result<Vec<_>>>()?,
                deletes,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::CreateIndexes {
                database,
                collection,
                create_options,
                specs,
                change_events,
            } => WalMutation::CreateIndexes {
                database,
                collection,
                create_options: decode_optional_document_bytes(create_options)?,
                specs: specs
                    .iter()
                    .map(|bytes| decode_document_bytes(bytes))
                    .collect::<Result<Vec<_>>>()?,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::DropIndexes {
                database,
                collection,
                target,
                change_events,
            } => WalMutation::DropIndexes {
                database,
                collection,
                target,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
            Self::DropCollection {
                database,
                collection,
                change_events,
            } => WalMutation::DropCollection {
                database,
                collection,
                change_events: change_events
                    .into_iter()
                    .map(CompactPersistedChangeEvent::into_persisted_change_event)
                    .collect::<Result<Vec<_>>>()?,
            },
        })
    }
}

impl<'a> EncodedWalMutation<'a> {
    fn from_wal_mutation(mutation: &'a WalMutation) -> Result<Self> {
        Ok(match mutation {
            WalMutation::ReplaceCollection {
                database,
                collection,
                collection_state,
                change_events,
            } => Self::ReplaceCollection {
                database,
                collection,
                collection_state: CompactCollectionCatalog::from_collection_catalog(
                    collection_state,
                )?,
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
            WalMutation::RewriteCollection {
                database,
                collection,
                options,
                changes,
                change_events,
            } => Self::RewriteCollection {
                database,
                collection,
                options: encode_document_bytes(options)?,
                changes: changes
                    .iter()
                    .map(CompactCollectionChange::from_collection_change)
                    .collect::<Result<Vec<_>>>()?,
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
            WalMutation::ApplyCollectionChanges {
                database,
                collection,
                create_options,
                changes,
                inserts,
                updates,
                deletes,
                change_events,
            } => Self::ApplyCollectionChanges {
                database,
                collection,
                create_options: encode_optional_document_bytes(create_options.as_ref())?,
                changes: changes
                    .iter()
                    .map(CompactCollectionChange::from_collection_change)
                    .collect::<Result<Vec<_>>>()?,
                inserts: inserts
                    .iter()
                    .map(CompactCollectionRecord::from_collection_record)
                    .collect::<Result<Vec<_>>>()?,
                updates: updates
                    .iter()
                    .map(CompactCollectionRecord::from_collection_record)
                    .collect::<Result<Vec<_>>>()?,
                deletes: deletes.clone(),
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
            WalMutation::CreateIndexes {
                database,
                collection,
                create_options,
                specs,
                change_events,
            } => Self::CreateIndexes {
                database,
                collection,
                create_options: encode_optional_document_bytes(create_options.as_ref())?,
                specs: specs
                    .iter()
                    .map(encode_document_bytes)
                    .collect::<Result<Vec<_>>>()?,
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
            WalMutation::DropIndexes {
                database,
                collection,
                target,
                change_events,
            } => Self::DropIndexes {
                database,
                collection,
                target,
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
            WalMutation::DropCollection {
                database,
                collection,
                change_events,
            } => Self::DropCollection {
                database,
                collection,
                change_events: change_events
                    .iter()
                    .map(EncodedPersistedChangeEvent::from_persisted_change_event)
                    .collect(),
            },
        })
    }
}

impl CompactCollectionCatalog {
    fn from_collection_catalog(collection: &CollectionCatalog) -> Result<Self> {
        Ok(Self {
            options: encode_document_bytes(&collection.options)?,
            indexes: collection
                .indexes
                .iter()
                .map(|(index_name, index)| {
                    Ok((
                        index_name.clone(),
                        CompactIndexCatalog::from_index_catalog(index)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
            records: collection
                .records
                .iter()
                .map(CompactCollectionRecord::from_collection_record)
                .collect::<Result<Vec<_>>>()?,
            next_record_id: collection.next_record_id(),
        })
    }

    fn into_collection_catalog(self) -> Result<CollectionCatalog> {
        let indexes = self
            .indexes
            .into_iter()
            .map(|(index_name, index)| {
                Ok((index_name.clone(), index.into_index_catalog(&index_name)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let records = self
            .records
            .into_iter()
            .map(CompactCollectionRecord::into_collection_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(CollectionCatalog::from_parts(
            decode_document_bytes(&self.options)?,
            indexes,
            records,
            self.next_record_id,
        ))
    }
}

impl CompactCollectionRecord {
    fn from_collection_record(record: &CollectionRecord) -> Result<Self> {
        Ok(Self {
            record_id: record.record_id,
            document: record.encoded_document_bytes()?.into_owned(),
        })
    }

    fn into_collection_record(self) -> Result<CollectionRecord> {
        Ok(CollectionRecord::from_encoded(
            self.record_id,
            decode_document_bytes(&self.document)?,
            self.document,
        ))
    }
}

impl CompactIndexCatalog {
    fn from_index_catalog(index: &IndexCatalog) -> Result<Self> {
        Ok(Self {
            key: encode_document_bytes(&index.key)?,
            unique: index.unique,
            expire_after_seconds: index.expire_after_seconds,
            entries: index
                .entries_snapshot()
                .iter()
                .map(CompactIndexEntry::from_index_entry)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn into_index_catalog(self, index_name: &str) -> Result<IndexCatalog> {
        let mut index = IndexCatalog::new(
            index_name.to_string(),
            decode_document_bytes(&self.key)?,
            self.unique,
        );
        index.expire_after_seconds = self.expire_after_seconds;
        index.load_entries(
            self.entries
                .into_iter()
                .map(CompactIndexEntry::into_index_entry)
                .collect::<Result<Vec<_>>>()?,
        )?;
        Ok(index)
    }
}

impl CompactIndexEntry {
    fn from_index_entry(entry: &IndexEntry) -> Result<Self> {
        Ok(Self {
            record_id: entry.record_id,
            key: encode_document_bytes(&entry.key)?,
            present_fields: entry.present_fields.clone(),
        })
    }

    fn into_index_entry(self) -> Result<IndexEntry> {
        Ok(IndexEntry {
            record_id: self.record_id,
            key: decode_document_bytes(&self.key)?,
            present_fields: self.present_fields,
        })
    }
}

impl CompactPersistedChangeEvent {
    fn into_persisted_change_event(self) -> Result<PersistedChangeEvent> {
        Ok(PersistedChangeEvent {
            token: self.token,
            cluster_time: bson::Timestamp {
                time: self.cluster_time_time,
                increment: self.cluster_time_increment,
            },
            wall_time: bson::DateTime::from_millis(self.wall_time_millis),
            database: self.database,
            collection: self.collection,
            operation_type: self.operation_type,
            document_key: self.document_key,
            full_document: self.full_document,
            full_document_before_change: self.full_document_before_change,
            update_description: self.update_description,
            expanded: self.expanded,
            extra_fields: self.extra_fields,
        })
    }
}

impl<'a> EncodedPersistedChangeEvent<'a> {
    fn from_persisted_change_event(event: &'a PersistedChangeEvent) -> Self {
        Self {
            token: &event.token,
            cluster_time_time: event.cluster_time.time,
            cluster_time_increment: event.cluster_time.increment,
            wall_time_millis: event.wall_time.timestamp_millis(),
            database: &event.database,
            collection: event.collection.as_deref(),
            operation_type: &event.operation_type,
            document_key: event.document_key.as_deref(),
            full_document: event.full_document.as_deref(),
            full_document_before_change: event.full_document_before_change.as_deref(),
            update_description: event.update_description.as_deref(),
            expanded: event.expanded,
            extra_fields: &event.extra_fields,
        }
    }
}

impl CompactCollectionChange {
    fn from_collection_change(change: &CollectionChange) -> Result<Self> {
        Ok(match change {
            CollectionChange::Insert(record) => {
                Self::Insert(CompactCollectionRecord::from_collection_record(record)?)
            }
            CollectionChange::Update(record) => {
                Self::Update(CompactCollectionRecord::from_collection_record(record)?)
            }
            CollectionChange::Delete(record_id) => Self::Delete(*record_id),
        })
    }

    fn into_collection_change(self) -> Result<CollectionChange> {
        Ok(match self {
            Self::Insert(record) => CollectionChange::Insert(record.into_collection_record()?),
            Self::Update(record) => CollectionChange::Update(record.into_collection_record()?),
            Self::Delete(record_id) => CollectionChange::Delete(record_id),
        })
    }
}

fn required_compression_savings(raw_len: usize, min_savings: usize) -> usize {
    raw_len
        .div_ceil(COMPRESSION_MIN_SAVINGS_DIVISOR)
        .max(min_savings)
}

fn maybe_encode_zstd_blob(
    bytes: &[u8],
    min_input_len: usize,
    min_savings: usize,
) -> Result<Vec<u8>> {
    if bytes.len() < min_input_len {
        return Ok(bytes.to_vec());
    }

    let compressed = zstd::bulk::compress(bytes, ZSTD_COMPRESSION_LEVEL)?;
    let stored_len = ZSTD_BLOB_HEADER_LEN + compressed.len();
    if stored_len + required_compression_savings(bytes.len(), min_savings) > bytes.len() {
        return Ok(bytes.to_vec());
    }

    let mut stored = Vec::with_capacity(stored_len);
    stored.extend_from_slice(ZSTD_BLOB_MAGIC);
    stored.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    stored.extend_from_slice(&compressed);
    Ok(stored)
}

fn maybe_decode_zstd_blob(bytes: &[u8]) -> std::result::Result<Option<Vec<u8>>, ()> {
    if !bytes.starts_with(ZSTD_BLOB_MAGIC) {
        return Ok(None);
    }
    if bytes.len() < ZSTD_BLOB_HEADER_LEN {
        return Err(());
    }

    let expected_len = u64::from_le_bytes(
        bytes[8..ZSTD_BLOB_HEADER_LEN]
            .try_into()
            .expect("zstd blob len"),
    );
    let expected_len = usize::try_from(expected_len).map_err(|_| ())?;
    let decoded =
        zstd::bulk::decompress(&bytes[ZSTD_BLOB_HEADER_LEN..], expected_len).map_err(|_| ())?;
    if decoded.len() != expected_len {
        return Err(());
    }
    Ok(Some(decoded))
}

fn maybe_decode_stored_blob<'a>(bytes: &'a [u8]) -> std::result::Result<Cow<'a, [u8]>, ()> {
    match maybe_decode_zstd_blob(bytes)? {
        Some(decoded) => Ok(Cow::Owned(decoded)),
        None => Ok(Cow::Borrowed(bytes)),
    }
}

fn encode_wal_entry(sequence: u64, mutation: &WalMutation) -> Result<Vec<u8>> {
    let compact_wal_entry = EncodedWalEntry::from_wal_entry(sequence, mutation)?;
    let mut bytes = Vec::new();
    cbor_ser::into_writer(&compact_wal_entry, &mut bytes)?;
    maybe_encode_zstd_blob(&bytes, WAL_COMPRESSION_MIN_LEN, WAL_COMPRESSION_MIN_SAVINGS)
}

fn decode_wal_entry(bytes: &[u8]) -> Result<WalEntry> {
    let bytes = maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let mut cursor = Cursor::new(bytes.as_ref());
    let compact_wal_entry: CompactWalEntry = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != bytes.as_ref().len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    compact_wal_entry.into_wal_entry()
}

fn decode_compact_wal_entry(bytes: &[u8]) -> Result<CompactWalEntry> {
    let bytes = maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let mut cursor = Cursor::new(bytes.as_ref());
    let compact_wal_entry: CompactWalEntry = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != bytes.as_ref().len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok(compact_wal_entry)
}

fn append_wal_entry(
    file: &mut File,
    frame_offset: u64,
    sequence: u64,
    mutation: &WalMutation,
    sync: bool,
) -> Result<u64> {
    let payload = encode_wal_entry(sequence, mutation)?;
    let payload_checksum = hash_bytes(&payload);
    let frame_len = (WAL_HEADER_LEN + payload.len()) as u64;

    file.seek(SeekFrom::Start(frame_offset))?;
    file.write_all(WAL_FRAME_MAGIC)?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&payload_checksum)?;
    file.write_all(&payload)?;
    if sync {
        file.flush()?;
        file.sync_data()?;
    }
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

        let entry = decode_wal_entry(&payload)?;
        if entry.sequence > last_applied_sequence {
            mark_mutation_dirty(
                &mut recovery.dirty_collections,
                &mut recovery.change_events_dirty,
                &entry.mutation,
            );
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
            ensure_collection_indexes_hydrated(&mut hydrated);
            validate_collection_indexes(&hydrated).map_err(map_catalog_error)?;
            state
                .catalog
                .replace_collection(database, collection, hydrated);
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::RewriteCollection {
            database,
            collection,
            options,
            changes,
            change_events,
        } => {
            rewrite_collection(state, database, collection, options, changes)?;
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            changes,
            inserts,
            updates,
            deletes,
            change_events,
        } => {
            let changes = resolved_collection_changes(changes, inserts, updates, deletes);
            apply_collection_changes(
                state,
                database,
                collection,
                create_options.as_ref(),
                &changes,
            )?;
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::CreateIndexes {
            database,
            collection,
            create_options,
            specs,
            change_events,
        } => {
            apply_create_indexes(state, database, collection, create_options.as_ref(), specs)?;
            state.change_events.extend(change_events.iter().cloned());
        }
        WalMutation::DropIndexes {
            database,
            collection,
            target,
            change_events,
        } => {
            state.catalog.drop_indexes(database, collection, target)?;
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

fn apply_owned_mutation(
    state: &mut PersistedState,
    sequence: u64,
    owned_mutation: WalMutation,
) -> Result<()> {
    match owned_mutation {
        WalMutation::ReplaceCollection {
            database,
            collection,
            mut collection_state,
            change_events,
        } => {
            ensure_collection_indexes_hydrated(&mut collection_state);
            validate_collection_indexes(&collection_state).map_err(map_catalog_error)?;
            state
                .catalog
                .replace_collection(&database, &collection, collection_state);
            state.change_events.extend(change_events);
        }
        WalMutation::RewriteCollection {
            database,
            collection,
            options,
            changes,
            change_events,
        } => {
            rewrite_collection(state, &database, &collection, &options, &changes)?;
            state.change_events.extend(change_events);
        }
        WalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            changes,
            inserts,
            updates,
            deletes,
            change_events,
        } => {
            let changes = if changes.is_empty() {
                inserts
                    .into_iter()
                    .map(CollectionChange::Insert)
                    .chain(updates.into_iter().map(CollectionChange::Update))
                    .chain(deletes.into_iter().map(CollectionChange::Delete))
                    .collect::<Vec<_>>()
            } else {
                changes
            };
            apply_collection_changes_validated(
                state,
                &database,
                &collection,
                create_options.as_ref(),
                &changes,
            )?;
            state.change_events.extend(change_events);
        }
        WalMutation::CreateIndexes {
            database,
            collection,
            create_options,
            specs,
            change_events,
        } => {
            apply_create_indexes(
                state,
                &database,
                &collection,
                create_options.as_ref(),
                &specs,
            )?;
            state.change_events.extend(change_events);
        }
        WalMutation::DropIndexes {
            database,
            collection,
            target,
            change_events,
        } => {
            state
                .catalog
                .drop_indexes(&database, &collection, &target)?;
            state.change_events.extend(change_events);
        }
        WalMutation::DropCollection {
            database,
            collection,
            change_events,
        } => {
            state.catalog.drop_collection(&database, &collection)?;
            state.change_events.extend(change_events);
        }
    }
    state.last_applied_sequence = sequence;
    Ok(())
}

fn ensure_collection_indexes_hydrated(collection_state: &mut CollectionCatalog) {
    collection_state.refresh_runtime_state();
    for index in collection_state.indexes.values_mut() {
        if !index.stats_hydrated() {
            index.rebuild_tree();
        }
    }
}

fn mark_mutation_dirty(
    dirty_collections: &mut BTreeSet<(String, String)>,
    change_events_dirty: &mut bool,
    mutation: &WalMutation,
) {
    match mutation {
        WalMutation::ReplaceCollection {
            database,
            collection,
            change_events,
            ..
        }
        | WalMutation::RewriteCollection {
            database,
            collection,
            change_events,
            ..
        }
        | WalMutation::ApplyCollectionChanges {
            database,
            collection,
            change_events,
            ..
        }
        | WalMutation::CreateIndexes {
            database,
            collection,
            change_events,
            ..
        }
        | WalMutation::DropIndexes {
            database,
            collection,
            change_events,
            ..
        }
        | WalMutation::DropCollection {
            database,
            collection,
            change_events,
        } => {
            dirty_collections.insert((database.clone(), collection.clone()));
            if !change_events.is_empty() {
                *change_events_dirty = true;
            }
        }
    }
}

fn validate_mutation(
    state: &PersistedState,
    validation_state: &ValidationState,
    mutation: &WalMutation,
) -> Result<ValidationPlan> {
    let plan = match mutation {
        WalMutation::ReplaceCollection {
            database,
            collection,
            collection_state,
            ..
        } => {
            validate_collection_indexes(collection_state).map_err(map_catalog_error)?;
            ValidationPlan::RebuildCollection {
                database: database.clone(),
                collection: collection.clone(),
            }
        }
        WalMutation::RewriteCollection {
            database,
            collection,
            options,
            changes,
            ..
        } => {
            validate_rewrite_collection(options, changes)?;
            ValidationPlan::RebuildCollection {
                database: database.clone(),
                collection: collection.clone(),
            }
        }
        WalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            changes,
            inserts,
            updates,
            deletes,
            ..
        } => {
            let changes = resolved_collection_changes(changes, inserts, updates, deletes);
            let delta = validate_collection_changes(
                state,
                validation_state,
                database,
                collection,
                create_options.as_ref(),
                &changes,
            )?;
            if state.catalog.get_collection(database, collection).is_ok() {
                ValidationPlan::ApplyCollectionDelta {
                    database: database.clone(),
                    collection: collection.clone(),
                    delta,
                }
            } else {
                ValidationPlan::RebuildCollection {
                    database: database.clone(),
                    collection: collection.clone(),
                }
            }
        }
        WalMutation::CreateIndexes {
            database,
            collection,
            create_options,
            specs,
            ..
        } => {
            validate_create_indexes(state, database, collection, create_options.as_ref(), specs)?;
            ValidationPlan::RebuildCollection {
                database: database.clone(),
                collection: collection.clone(),
            }
        }
        WalMutation::DropIndexes {
            database,
            collection,
            target,
            ..
        } => {
            let collection_state = state.catalog.get_collection(database, collection)?;
            validate_drop_indexes(collection_state, target)?;
            ValidationPlan::RebuildCollection {
                database: database.clone(),
                collection: collection.clone(),
            }
        }
        WalMutation::DropCollection {
            database,
            collection,
            ..
        } => {
            state.catalog.get_collection(database, collection)?;
            ValidationPlan::RemoveCollection {
                database: database.clone(),
                collection: collection.clone(),
            }
        }
    };
    Ok(plan)
}

fn apply_create_indexes(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    specs: &[bson::Document],
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

    state.catalog.create_indexes(database, collection, specs)?;
    Ok(())
}

fn validate_create_indexes(
    state: &PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    specs: &[bson::Document],
) -> Result<Vec<IndexCatalog>> {
    let preview_collection;
    let collection_state = match state.catalog.get_collection(database, collection) {
        Ok(collection_state) => collection_state,
        Err(CatalogError::NamespaceNotFound(_, _)) => {
            let Some(options) = create_options else {
                return Err(CatalogError::NamespaceNotFound(
                    database.to_string(),
                    collection.to_string(),
                )
                .into());
            };
            preview_collection = CollectionCatalog::new(options.clone());
            &preview_collection
        }
        Err(error) => return Err(error.into()),
    };

    build_index_specs(collection_state, specs).map_err(Into::into)
}

fn apply_collection_changes(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    changes: &[CollectionChange],
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
    apply_collection_change_set(collection_state, changes)
}

fn apply_collection_changes_validated(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    changes: &[CollectionChange],
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
    apply_collection_change_set_validated(collection_state, changes)
}

fn rewrite_collection(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    options: &bson::Document,
    changes: &[CollectionChange],
) -> Result<()> {
    let mut collection_state = CollectionCatalog::new(options.clone());
    apply_collection_change_set(&mut collection_state, changes)?;
    state
        .catalog
        .replace_collection(database, collection, collection_state);
    Ok(())
}

fn validate_collection_changes<'a>(
    state: &'a PersistedState,
    validation_state: &'a ValidationState,
    database: &str,
    collection: &str,
    create_options: Option<&'a bson::Document>,
    changes: &'a [CollectionChange],
) -> Result<CollectionValidationDelta> {
    let collection_state = match state.catalog.get_collection(database, collection) {
        Ok(collection_state) => Some(collection_state),
        Err(CatalogError::NamespaceNotFound(_, _)) => None,
        Err(error) => return Err(error.into()),
    };
    let mut overlay = CollectionValidationOverlay::new(
        collection_state,
        validation_state.collection(database, collection),
        create_options,
        database,
        collection,
    )?;
    for change in changes {
        overlay.apply(change)?;
    }
    Ok(overlay.into_delta())
}

fn validate_rewrite_collection(
    options: &bson::Document,
    changes: &[CollectionChange],
) -> Result<()> {
    let mut preview_collection = CollectionCatalog::new(options.clone());
    apply_collection_change_set(&mut preview_collection, changes)
}

fn apply_collection_change_set(
    collection_state: &mut CollectionCatalog,
    changes: &[CollectionChange],
) -> Result<()> {
    let mutations = changes
        .iter()
        .map(|change| match change {
            CollectionChange::Insert(record) => CollectionMutation::Insert(record),
            CollectionChange::Update(record) => CollectionMutation::Update(record),
            CollectionChange::Delete(record_id) => CollectionMutation::Delete(*record_id),
        })
        .collect::<Vec<_>>();
    collection_state
        .apply_mutations(&mutations)
        .map_err(map_catalog_error)
}

fn apply_collection_change_set_validated(
    collection_state: &mut CollectionCatalog,
    changes: &[CollectionChange],
) -> Result<()> {
    let mutations = changes
        .iter()
        .map(|change| match change {
            CollectionChange::Insert(record) => CollectionMutation::Insert(record),
            CollectionChange::Update(record) => CollectionMutation::Update(record),
            CollectionChange::Delete(record_id) => CollectionMutation::Delete(*record_id),
        })
        .collect::<Vec<_>>();
    collection_state
        .apply_validated_mutations(&mutations)
        .map_err(map_catalog_error)
}

fn resolved_collection_changes(
    changes: &[CollectionChange],
    inserts: &[CollectionRecord],
    updates: &[CollectionRecord],
    deletes: &[u64],
) -> Vec<CollectionChange> {
    if !changes.is_empty() {
        return changes.to_vec();
    }

    inserts
        .iter()
        .cloned()
        .map(CollectionChange::Insert)
        .chain(updates.iter().cloned().map(CollectionChange::Update))
        .chain(deletes.iter().copied().map(CollectionChange::Delete))
        .collect()
}

type UniqueIndexKey = bson::Document;

#[derive(Debug, Clone)]
struct UniqueIndexValidator {
    name: String,
    key: bson::Document,
    entries: HashMap<UniqueIndexKey, u64>,
}

#[derive(Debug, Default)]
struct ValidationState {
    databases: HashMap<String, HashMap<String, CollectionValidationState>>,
}

#[derive(Debug, Clone, Default)]
struct CollectionValidationState {
    unique_indexes: BTreeMap<String, UniqueIndexValidator>,
}

#[derive(Debug)]
enum ValidationPlan {
    RebuildCollection {
        database: String,
        collection: String,
    },
    RemoveCollection {
        database: String,
        collection: String,
    },
    ApplyCollectionDelta {
        database: String,
        collection: String,
        delta: CollectionValidationDelta,
    },
}

#[derive(Debug, Default)]
struct CollectionValidationDelta {
    unique_indexes: Vec<UniqueIndexDelta>,
}

#[derive(Debug, Default)]
struct UniqueIndexDelta {
    name: String,
    additions: HashMap<UniqueIndexKey, u64>,
    removals: HashSet<UniqueIndexKey>,
}

struct CollectionValidationOverlay<'a> {
    base_collection: Option<&'a CollectionCatalog>,
    overlay_records: HashMap<u64, &'a bson::Document>,
    deleted_record_ids: HashSet<u64>,
    unique_indexes: Vec<UniqueIndexOverlay<'a>>,
}

struct UniqueIndexOverlay<'a> {
    name: String,
    key: bson::Document,
    base_entries: Option<&'a HashMap<UniqueIndexKey, u64>>,
    pending_entries: HashMap<UniqueIndexKey, u64>,
    removed_keys: HashSet<UniqueIndexKey>,
}

impl<'a> CollectionValidationOverlay<'a> {
    fn new(
        collection: Option<&'a CollectionCatalog>,
        validation_state: Option<&'a CollectionValidationState>,
        create_options: Option<&bson::Document>,
        database: &str,
        collection_name: &str,
    ) -> Result<Self> {
        let unique_indexes = match collection {
            Some(_) => validation_state
                .ok_or(StorageError::InvalidIndexState)?
                .unique_indexes
                .values()
                .map(UniqueIndexOverlay::from_validator)
                .collect(),
            None => {
                if create_options.is_none() {
                    return Err(CatalogError::NamespaceNotFound(
                        database.to_string(),
                        collection_name.to_string(),
                    )
                    .into());
                }
                vec![UniqueIndexOverlay::default_id_index()]
            }
        };

        Ok(Self {
            base_collection: collection,
            overlay_records: HashMap::new(),
            deleted_record_ids: HashSet::new(),
            unique_indexes,
        })
    }

    fn apply(&mut self, change: &'a CollectionChange) -> Result<()> {
        match change {
            CollectionChange::Insert(record) => self.insert(record),
            CollectionChange::Update(record) => self.update(record),
            CollectionChange::Delete(record_id) => self.delete(*record_id),
        }
    }

    fn insert(&mut self, record: &'a CollectionRecord) -> Result<()> {
        if self.current_document(record.record_id).is_some() {
            return Err(CatalogError::InvalidIndexState(format!(
                "duplicate record id {}",
                record.record_id
            ))
            .into());
        }

        let keys = self.unique_keys(&record.document)?;
        self.validate_unique_keys(record.record_id, &keys)?;
        self.deleted_record_ids.remove(&record.record_id);
        self.overlay_records
            .insert(record.record_id, &record.document);
        self.install_unique_keys(record.record_id, &keys);
        Ok(())
    }

    fn update(&mut self, record: &'a CollectionRecord) -> Result<()> {
        let current_keys = {
            let Some(current_document) = self.current_document(record.record_id) else {
                return Err(CatalogError::InvalidIndexState(format!(
                    "record id {} is missing for update",
                    record.record_id
                ))
                .into());
            };
            if current_document == &record.document {
                return Ok(());
            }
            self.unique_keys(current_document)?
        };
        let new_keys = self.unique_keys(&record.document)?;
        self.validate_unique_keys(record.record_id, &new_keys)?;
        self.remove_unique_keys(record.record_id, &current_keys);
        self.install_unique_keys(record.record_id, &new_keys);
        self.overlay_records
            .insert(record.record_id, &record.document);
        self.deleted_record_ids.remove(&record.record_id);
        Ok(())
    }

    fn delete(&mut self, record_id: u64) -> Result<()> {
        let Some(keys) = self
            .current_document(record_id)
            .map(|document| self.unique_keys(document))
            .transpose()?
        else {
            return Ok(());
        };
        self.remove_unique_keys(record_id, &keys);
        self.overlay_records.remove(&record_id);
        if self
            .base_collection
            .and_then(|collection| collection.record_position(record_id))
            .is_some()
        {
            self.deleted_record_ids.insert(record_id);
        } else {
            self.deleted_record_ids.remove(&record_id);
        }
        Ok(())
    }

    fn current_document(&self, record_id: u64) -> Option<&'a bson::Document> {
        if self.deleted_record_ids.contains(&record_id) {
            return None;
        }
        self.overlay_records.get(&record_id).copied().or_else(|| {
            self.base_collection.and_then(|collection| {
                collection
                    .record_position(record_id)
                    .and_then(|position| collection.records.get(position))
                    .map(|record| &record.document)
            })
        })
    }

    fn unique_keys(&self, document: &bson::Document) -> Result<Vec<(usize, UniqueIndexKey)>> {
        self.unique_indexes
            .iter()
            .enumerate()
            .map(|(index_position, index)| {
                Ok((
                    index_position,
                    mqlite_catalog::index_key_for_document(document, &index.key),
                ))
            })
            .collect()
    }

    fn validate_unique_keys(&self, record_id: u64, keys: &[(usize, UniqueIndexKey)]) -> Result<()> {
        for (index_position, key) in keys {
            if let Some(existing_record_id) =
                self.unique_indexes[*index_position].record_for_key(key)
            {
                if existing_record_id != record_id {
                    return Err(CatalogError::DuplicateKey(
                        self.unique_indexes[*index_position].name.clone(),
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    fn install_unique_keys(&mut self, record_id: u64, keys: &[(usize, UniqueIndexKey)]) {
        for (index_position, key) in keys {
            self.unique_indexes[*index_position].insert_key(key.clone(), record_id);
        }
    }

    fn remove_unique_keys(&mut self, record_id: u64, keys: &[(usize, UniqueIndexKey)]) {
        for (index_position, key) in keys {
            self.unique_indexes[*index_position].remove_key(key, record_id);
        }
    }

    fn into_delta(self) -> CollectionValidationDelta {
        CollectionValidationDelta {
            unique_indexes: self
                .unique_indexes
                .into_iter()
                .map(UniqueIndexOverlay::into_delta)
                .collect(),
        }
    }
}

impl UniqueIndexValidator {
    fn from_catalog(index: &IndexCatalog) -> Result<Self> {
        let mut entries = HashMap::with_capacity(index.entry_count());
        index.try_for_each_entry(|entry| {
            entries.insert(entry.key.clone(), entry.record_id);
            Ok::<(), CatalogError>(())
        })?;
        Ok(Self {
            name: index.name.clone(),
            key: index.key.clone(),
            entries,
        })
    }
}

impl ValidationState {
    fn build(catalog: &Catalog) -> Result<Self> {
        let mut state = Self::default();
        for (database_name, database) in &catalog.databases {
            for (collection_name, collection) in &database.collections {
                state.insert_collection(
                    database_name.clone(),
                    collection_name.clone(),
                    CollectionValidationState::from_collection(collection)?,
                );
            }
        }
        Ok(state)
    }

    fn collection(&self, database: &str, collection: &str) -> Option<&CollectionValidationState> {
        self.databases
            .get(database)
            .and_then(|db| db.get(collection))
    }

    fn apply_plan(&mut self, catalog: &Catalog, plan: ValidationPlan) -> Result<()> {
        match plan {
            ValidationPlan::RebuildCollection {
                database,
                collection,
            } => self.rebuild_collection(catalog, &database, &collection),
            ValidationPlan::RemoveCollection {
                database,
                collection,
            } => {
                self.remove_collection(&database, &collection);
                Ok(())
            }
            ValidationPlan::ApplyCollectionDelta {
                database,
                collection,
                delta,
            } => self.apply_collection_delta(&database, &collection, delta),
        }
    }

    fn rebuild_collection(
        &mut self,
        catalog: &Catalog,
        database: &str,
        collection: &str,
    ) -> Result<()> {
        let collection_state = catalog.get_collection(database, collection)?;
        self.insert_collection(
            database.to_string(),
            collection.to_string(),
            CollectionValidationState::from_collection(collection_state)?,
        );
        Ok(())
    }

    fn apply_collection_delta(
        &mut self,
        database: &str,
        collection: &str,
        delta: CollectionValidationDelta,
    ) -> Result<()> {
        let collection_state = self
            .databases
            .get_mut(database)
            .and_then(|db| db.get_mut(collection))
            .ok_or(StorageError::InvalidIndexState)?;
        for index_delta in delta.unique_indexes {
            let index_state = collection_state
                .unique_indexes
                .get_mut(&index_delta.name)
                .ok_or(StorageError::InvalidIndexState)?;
            for key in index_delta.removals {
                index_state.entries.remove(&key);
            }
            index_state.entries.extend(index_delta.additions);
        }
        Ok(())
    }

    fn insert_collection(
        &mut self,
        database: String,
        collection: String,
        validation_state: CollectionValidationState,
    ) {
        self.databases
            .entry(database)
            .or_default()
            .insert(collection, validation_state);
    }

    fn remove_collection(&mut self, database: &str, collection: &str) {
        let remove_database = if let Some(database_entry) = self.databases.get_mut(database) {
            database_entry.remove(collection);
            database_entry.is_empty()
        } else {
            false
        };
        if remove_database {
            self.databases.remove(database);
        }
    }
}

impl CollectionValidationState {
    fn from_collection(collection: &CollectionCatalog) -> Result<Self> {
        let unique_indexes = collection
            .indexes
            .values()
            .filter(|index| index.unique)
            .map(|index| {
                Ok((
                    index.name.clone(),
                    UniqueIndexValidator::from_catalog(index)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self { unique_indexes })
    }
}

impl<'a> UniqueIndexOverlay<'a> {
    fn from_validator(index: &'a UniqueIndexValidator) -> Self {
        Self {
            name: index.name.clone(),
            key: index.key.clone(),
            base_entries: Some(&index.entries),
            pending_entries: HashMap::new(),
            removed_keys: HashSet::new(),
        }
    }

    fn default_id_index() -> Self {
        Self {
            name: "_id_".to_string(),
            key: bson::doc! { "_id": 1 },
            base_entries: None,
            pending_entries: HashMap::new(),
            removed_keys: HashSet::new(),
        }
    }

    fn record_for_key(&self, key: &UniqueIndexKey) -> Option<u64> {
        if let Some(record_id) = self.pending_entries.get(key) {
            return Some(*record_id);
        }
        if self.removed_keys.contains(key) {
            return None;
        }
        self.base_entries
            .and_then(|entries| entries.get(key).copied())
    }

    fn insert_key(&mut self, key: UniqueIndexKey, record_id: u64) {
        self.removed_keys.remove(&key);
        self.pending_entries.insert(key, record_id);
    }

    fn remove_key(&mut self, key: &UniqueIndexKey, record_id: u64) {
        if self
            .pending_entries
            .get(key)
            .is_some_and(|existing_record_id| *existing_record_id == record_id)
        {
            self.pending_entries.remove(key);
        }
        if self
            .base_entries
            .and_then(|entries| entries.get(key))
            .is_some_and(|existing_record_id| *existing_record_id == record_id)
        {
            self.removed_keys.insert(key.clone());
        }
    }

    fn into_delta(self) -> UniqueIndexDelta {
        UniqueIndexDelta {
            name: self.name,
            additions: self.pending_entries,
            removals: self.removed_keys,
        }
    }
}

fn apply_wal_metadata_mutation(
    metadata: &mut WalCatalogMetadata,
    mutation: CompactWalMutation,
) -> Result<()> {
    match mutation {
        CompactWalMutation::ReplaceCollection {
            database,
            collection,
            collection_state,
            change_events,
        } => {
            let collection_metadata = wal_collection_metadata_from_compact(&collection_state)?;
            metadata
                .databases
                .entry(database)
                .or_default()
                .collections
                .insert(collection, collection_metadata);
            metadata.change_event_count += change_events.len();
        }
        CompactWalMutation::RewriteCollection {
            database,
            collection,
            changes,
            change_events,
            ..
        } => {
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            apply_wal_collection_changes(collection_metadata, &changes);
            metadata.change_event_count += change_events.len();
        }
        CompactWalMutation::ApplyCollectionChanges {
            database,
            collection,
            create_options,
            changes,
            inserts,
            updates,
            deletes,
            change_events,
        } => {
            if create_options.is_some() {
                let _ = ensure_wal_collection_metadata(metadata, &database, &collection);
            }
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            let changes = if changes.is_empty() {
                inserts
                    .into_iter()
                    .map(CompactCollectionChange::Insert)
                    .chain(updates.into_iter().map(CompactCollectionChange::Update))
                    .chain(deletes.into_iter().map(CompactCollectionChange::Delete))
                    .collect::<Vec<_>>()
            } else {
                changes
            };
            apply_wal_collection_changes(collection_metadata, &changes);
            metadata.change_event_count += change_events.len();
        }
        CompactWalMutation::CreateIndexes {
            database,
            collection,
            create_options,
            specs,
            change_events,
        } => {
            if create_options.is_some() {
                let _ = ensure_wal_collection_metadata(metadata, &database, &collection);
            }
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            for spec in specs {
                let document = decode_document_bytes(&spec)?;
                let name = document.get_str("name")?.to_string();
                let key = document.get_document("key")?.clone();
                let bytes =
                    estimate_index_bytes_for_count(collection_metadata.document_count, &key);
                collection_metadata.indexes.insert(
                    name,
                    WalIndexMetadata {
                        key,
                        unique: document.get_bool("unique").unwrap_or(false),
                        expire_after_seconds: document.get_i64("expireAfterSeconds").ok(),
                        entry_count: collection_metadata.document_count,
                        bytes,
                    },
                );
            }
            metadata.change_event_count += change_events.len();
        }
        CompactWalMutation::DropIndexes {
            database,
            collection,
            target,
            change_events,
        } => {
            if let Some(collection_metadata) = metadata
                .databases
                .get_mut(&database)
                .and_then(|database| database.collections.get_mut(&collection))
            {
                if target == "*" {
                    collection_metadata.indexes.retain(|name, _| name == "_id_");
                } else {
                    collection_metadata.indexes.remove(&target);
                }
            }
            metadata.change_event_count += change_events.len();
        }
        CompactWalMutation::DropCollection {
            database,
            collection,
            change_events,
        } => {
            if let Some(database_metadata) = metadata.databases.get_mut(&database) {
                database_metadata.collections.remove(&collection);
                if database_metadata.collections.is_empty() {
                    metadata.databases.remove(&database);
                }
            }
            metadata.change_event_count += change_events.len();
        }
    }
    Ok(())
}

fn wal_collection_metadata_from_compact(
    collection: &CompactCollectionCatalog,
) -> Result<WalCollectionMetadata> {
    let document_count = collection.records.len();
    let document_bytes = collection
        .records
        .iter()
        .map(|record| record.document.len() as u64)
        .sum();
    let record_sizes = collection
        .records
        .iter()
        .map(|record| (record.record_id, record.document.len()))
        .collect::<HashMap<_, _>>();
    let indexes = collection
        .indexes
        .iter()
        .map(|(name, index)| {
            Ok((
                name.clone(),
                WalIndexMetadata {
                    key: decode_document_bytes(&index.key)?,
                    unique: index.unique,
                    expire_after_seconds: index.expire_after_seconds,
                    entry_count: index.entries.len(),
                    bytes: estimate_compact_index_bytes(index),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(WalCollectionMetadata {
        indexes,
        record_sizes,
        document_count,
        document_bytes,
    })
}

fn ensure_wal_collection_metadata<'a>(
    metadata: &'a mut WalCatalogMetadata,
    database: &str,
    collection: &str,
) -> &'a mut WalCollectionMetadata {
    metadata
        .databases
        .entry(database.to_string())
        .or_default()
        .collections
        .entry(collection.to_string())
        .or_insert_with(default_wal_collection_metadata)
}

fn default_wal_collection_metadata() -> WalCollectionMetadata {
    let mut indexes = BTreeMap::new();
    indexes.insert(
        "_id_".to_string(),
        WalIndexMetadata {
            key: bson::doc! { "_id": 1 },
            unique: true,
            expire_after_seconds: None,
            entry_count: 0,
            bytes: 0,
        },
    );
    WalCollectionMetadata {
        indexes,
        record_sizes: HashMap::new(),
        document_count: 0,
        document_bytes: 0,
    }
}

fn apply_wal_collection_changes(
    collection: &mut WalCollectionMetadata,
    changes: &[CompactCollectionChange],
) {
    let previous_document_count = collection.document_count;
    for change in changes {
        match change {
            CompactCollectionChange::Insert(record) => {
                let new_len = record.document.len();
                match collection.record_sizes.insert(record.record_id, new_len) {
                    Some(old_len) => {
                        collection.document_bytes = collection
                            .document_bytes
                            .saturating_sub(old_len as u64)
                            .saturating_add(new_len as u64);
                    }
                    None => {
                        collection.document_count += 1;
                        collection.document_bytes += new_len as u64;
                    }
                }
            }
            CompactCollectionChange::Update(record) => {
                let new_len = record.document.len();
                match collection.record_sizes.insert(record.record_id, new_len) {
                    Some(old_len) => {
                        collection.document_bytes = collection
                            .document_bytes
                            .saturating_sub(old_len as u64)
                            .saturating_add(new_len as u64);
                    }
                    None => {
                        let previous_average = average_document_bytes(collection);
                        collection.document_bytes = collection
                            .document_bytes
                            .saturating_sub(previous_average)
                            .saturating_add(new_len as u64);
                    }
                }
            }
            CompactCollectionChange::Delete(record_id) => {
                if collection.document_count > 0 {
                    collection.document_count -= 1;
                }
                let removed_len = collection
                    .record_sizes
                    .remove(record_id)
                    .map(|len| len as u64)
                    .unwrap_or_else(|| average_document_bytes(collection));
                collection.document_bytes = collection.document_bytes.saturating_sub(removed_len);
            }
        }
    }

    for index in collection.indexes.values_mut() {
        index.bytes = scale_index_bytes(
            index.bytes,
            previous_document_count,
            collection.document_count,
            &index.key,
        );
        index.entry_count = collection.document_count;
    }
}

fn apply_wal_catalog_metadata(
    file: &mut File,
    start_offset: u64,
    metadata: &mut WalCatalogMetadata,
) -> Result<WalMetadata> {
    let file_size = file.metadata()?.len();
    if start_offset > file_size {
        return Err(StorageError::Truncated.into());
    }

    let mut wal = WalMetadata::default();
    let mut offset = start_offset;
    while offset < file_size {
        if file_size - offset < WAL_HEADER_LEN as u64 {
            wal.truncated_tail = true;
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
            wal.truncated_tail = true;
            break;
        }

        let mut payload = vec![0_u8; payload_len as usize];
        file.read_exact(&mut payload)
            .map_err(|_| StorageError::Truncated)?;

        if hash_bytes(&payload) != header[8..40] {
            return Err(StorageError::InvalidWalChecksum.into());
        }

        let entry = decode_compact_wal_entry(&payload)?;
        apply_wal_metadata_mutation(metadata, entry.mutation)?;
        wal.records += 1;
        offset = payload_end;
    }

    wal.bytes = offset.saturating_sub(start_offset);
    Ok(wal)
}

fn wal_metadata_from_info_report(report: &InfoReport) -> WalCatalogMetadata {
    let databases = report
        .databases
        .iter()
        .map(|database| {
            let collections = database
                .collections
                .iter()
                .map(|collection| {
                    let indexes = collection
                        .indexes
                        .iter()
                        .map(|index| {
                            (
                                index.name.clone(),
                                WalIndexMetadata {
                                    key: index.key.clone(),
                                    unique: index.unique,
                                    expire_after_seconds: index.expire_after_seconds,
                                    entry_count: index.entry_count,
                                    bytes: index.bytes,
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                    (
                        collection.name.clone(),
                        WalCollectionMetadata {
                            indexes,
                            record_sizes: HashMap::new(),
                            document_count: collection.document_count,
                            document_bytes: collection.document_bytes,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            (database.name.clone(), WalDatabaseMetadata { collections })
        })
        .collect::<BTreeMap<_, _>>();

    WalCatalogMetadata {
        databases,
        change_event_count: report.last_checkpoint.change_event_count,
    }
}

fn build_v2_wal_inspect_report(
    path: PathBuf,
    checkpoint: &InfoReport,
    metadata: &WalCatalogMetadata,
    wal: &WalMetadata,
    file_size: u64,
) -> InspectReport {
    InspectReport {
        path,
        file_format_version: checkpoint.file_format_version,
        checkpoint_generation: checkpoint.last_checkpoint.generation,
        last_applied_sequence: checkpoint.last_applied_sequence + wal.records as u64,
        last_checkpoint_unix_ms: checkpoint.last_checkpoint.last_checkpoint_unix_ms,
        active_superblock_slot: checkpoint.last_checkpoint.active_superblock_slot,
        valid_superblocks: checkpoint.last_checkpoint.valid_superblocks,
        snapshot_offset: 0,
        snapshot_len: 0,
        wal_offset: checkpoint.last_checkpoint.wal_offset,
        page_size: checkpoint.last_checkpoint.page_size,
        checkpoint_page_count: checkpoint.last_checkpoint.page_count,
        checkpoint_record_page_count: checkpoint.last_checkpoint.record_page_count,
        checkpoint_index_page_count: checkpoint.last_checkpoint.index_page_count,
        checkpoint_change_event_page_count: checkpoint.last_checkpoint.change_event_page_count,
        checkpoint_record_count: checkpoint.last_checkpoint.record_count,
        checkpoint_index_entry_count: checkpoint.last_checkpoint.index_entry_count,
        checkpoint_change_event_count: checkpoint.last_checkpoint.change_event_count,
        current_record_count: metadata
            .databases
            .values()
            .flat_map(|database| database.collections.values())
            .map(|collection| collection.document_count)
            .sum(),
        current_index_entry_count: metadata
            .databases
            .values()
            .flat_map(|database| database.collections.values())
            .flat_map(|collection| collection.indexes.values())
            .map(|index| index.entry_count)
            .sum(),
        current_change_event_count: metadata.change_event_count,
        wal_records_since_checkpoint: wal.records,
        wal_bytes_since_checkpoint: wal.bytes,
        truncated_wal_tail: wal.truncated_tail,
        file_size,
        databases: metadata.databases.keys().cloned().collect(),
    }
}

fn build_v2_wal_info_report(
    path: PathBuf,
    checkpoint: InfoReport,
    metadata: &WalCatalogMetadata,
    wal: &WalMetadata,
    file_size: u64,
) -> InfoReport {
    let databases = metadata
        .databases
        .iter()
        .map(|(name, database)| {
            let checkpoint_database = checkpoint
                .databases
                .iter()
                .find(|current| current.name == *name);
            build_v2_wal_database_info(name, database, checkpoint_database)
        })
        .collect::<Vec<_>>();
    let summary = InfoSummary {
        database_count: databases.len(),
        collection_count: databases
            .iter()
            .map(|database| database.collection_count)
            .sum(),
        index_count: databases.iter().map(|database| database.index_count).sum(),
        record_count: databases.iter().map(|database| database.record_count).sum(),
        index_entry_count: databases
            .iter()
            .map(|database| database.index_entry_count)
            .sum(),
        change_event_count: metadata.change_event_count,
        plan_cache_entry_count: checkpoint.last_checkpoint.plan_cache_entry_count,
        document_bytes: databases
            .iter()
            .map(|database| database.document_bytes)
            .sum(),
        index_bytes: databases.iter().map(|database| database.index_bytes).sum(),
        total_bytes: databases.iter().map(|database| database.total_bytes).sum(),
    };
    InfoReport {
        path,
        file_format_version: checkpoint.file_format_version,
        file_size,
        last_applied_sequence: checkpoint.last_applied_sequence + wal.records as u64,
        summary,
        last_checkpoint: checkpoint.last_checkpoint,
        wal_since_checkpoint: InfoWal {
            record_count: wal.records,
            bytes: wal.bytes,
            truncated_tail: wal.truncated_tail,
        },
        databases,
    }
}

fn build_v2_wal_database_info(
    name: &str,
    database: &WalDatabaseMetadata,
    checkpoint: Option<&InfoDatabase>,
) -> InfoDatabase {
    let collections = database
        .collections
        .iter()
        .map(|(collection_name, collection)| {
            let checkpoint_collection = checkpoint.and_then(|database| {
                database
                    .collections
                    .iter()
                    .find(|current| current.name == *collection_name)
            });
            build_v2_wal_collection_info(collection_name, collection, checkpoint_collection)
        })
        .collect::<Vec<_>>();
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
    InfoDatabase {
        name: name.to_string(),
        collection_count,
        index_count,
        record_count,
        index_entry_count,
        document_bytes,
        index_bytes,
        total_bytes: document_bytes + index_bytes,
        checkpoint: checkpoint
            .map(|database| database.checkpoint.clone())
            .unwrap_or_default(),
        collections,
    }
}

fn build_v2_wal_collection_info(
    name: &str,
    collection: &WalCollectionMetadata,
    checkpoint: Option<&InfoCollection>,
) -> InfoCollection {
    let indexes = collection
        .indexes
        .iter()
        .map(|(index_name, index)| {
            let checkpoint_index = checkpoint.and_then(|collection| {
                collection
                    .indexes
                    .iter()
                    .find(|current| current.name == *index_name)
            });
            InfoIndex {
                name: index_name.clone(),
                key: index.key.clone(),
                unique: index.unique,
                expire_after_seconds: index.expire_after_seconds,
                entry_count: index.entry_count,
                bytes: index.bytes,
                checkpoint: checkpoint_index
                    .map(|index| index.checkpoint.clone())
                    .unwrap_or_default(),
            }
        })
        .collect::<Vec<_>>();
    let index_entry_count = indexes.iter().map(|index| index.entry_count).sum();
    let index_bytes = indexes.iter().map(|index| index.bytes).sum();
    InfoCollection {
        name: name.to_string(),
        document_count: collection.document_count,
        index_count: indexes.len(),
        index_entry_count,
        document_bytes: collection.document_bytes,
        index_bytes,
        total_bytes: collection.document_bytes + index_bytes,
        checkpoint: checkpoint
            .map(|collection| collection.checkpoint.clone())
            .unwrap_or_default(),
        indexes,
    }
}

fn average_document_bytes(collection: &WalCollectionMetadata) -> u64 {
    if collection.document_count == 0 {
        0
    } else {
        collection.document_bytes / collection.document_count as u64
    }
}

fn scale_index_bytes(
    current_bytes: u64,
    previous_count: usize,
    next_count: usize,
    key: &bson::Document,
) -> u64 {
    if next_count == 0 {
        return 0;
    }
    if previous_count == 0 || current_bytes == 0 {
        return estimate_index_bytes_for_count(next_count, key);
    }
    current_bytes.saturating_mul(next_count as u64) / previous_count as u64
}

fn estimate_index_bytes_for_count(count: usize, key: &bson::Document) -> u64 {
    let key_bytes = bson::to_vec(key)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0);
    count as u64 * (key_bytes + 32).max(48)
}

fn estimate_compact_index_bytes(index: &CompactIndexCatalog) -> u64 {
    index
        .entries
        .iter()
        .map(|entry| {
            entry.key.len() as u64
                + entry
                    .present_fields
                    .iter()
                    .map(|field| field.len() as u64 + 4)
                    .sum::<u64>()
                + 24
        })
        .sum()
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
        .map(IndexCatalog::entry_count)
        .sum()
}

fn map_catalog_error(error: CatalogError) -> anyhow::Error {
    match error {
        CatalogError::DuplicateKey(name) => StorageError::DuplicateKey(name).into(),
        CatalogError::InvalidIndexState(_) => StorageError::InvalidIndexState.into(),
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
        collections::{BTreeMap, BTreeSet},
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
    };

    use bson::{DateTime, Document, Timestamp, doc};
    use mqlite_catalog::{
        CollectionCatalog, CollectionRecord, IndexBound, IndexBounds, apply_index_specs,
    };
    use tempfile::tempdir;

    use super::{
        CollectionChange, DATA_START_OFFSET, DatabaseFile, FILE_FORMAT_VERSION, PAGE_SIZE,
        PersistedChangeEvent, PersistedPlanCacheChoice, PersistedPlanCacheEntry, StartupMetadata,
        VerifyReport, WAL_FRAME_MAGIC, WAL_HEADER_LEN, WalMutation, ZSTD_BLOB_MAGIC,
    };
    use crate::StorageEngine;
    use crate::v2::engine as v2_engine;

    fn insert_record(collection: &mut CollectionCatalog, record_id: u64, document: bson::Document) {
        collection
            .insert_record(CollectionRecord::new(record_id, document))
            .expect("insert record");
    }

    fn first_wal_payload(path: &std::path::Path) -> Vec<u8> {
        let inspect = DatabaseFile::inspect(path).expect("inspect");
        let mut file = OpenOptions::new().read(true).open(path).expect("open file");
        file.seek(SeekFrom::Start(inspect.wal_offset))
            .expect("seek wal frame");

        let mut header = [0_u8; WAL_HEADER_LEN];
        file.read_exact(&mut header).expect("read wal header");
        assert_eq!(&header[..4], WAL_FRAME_MAGIC);

        let payload_len = u32::from_le_bytes(header[4..8].try_into().expect("payload len"));
        let mut payload = vec![0_u8; payload_len as usize];
        file.read_exact(&mut payload).expect("read wal payload");
        payload
    }

    fn sample_change_event(sequence: i64, operation_type: &str) -> PersistedChangeEvent {
        PersistedChangeEvent::new(
            &doc! { "sequence": sequence, "event": 1_i32 },
            Timestamp {
                time: sequence as u32,
                increment: 1,
            },
            DateTime::from_millis(sequence),
            "app".to_string(),
            Some("widgets".to_string()),
            operation_type.to_string(),
            Some(&doc! { "_id": sequence }),
            Some(&doc! { "_id": sequence, "qty": sequence }),
            None,
            None,
            false,
            &Document::new(),
        )
        .expect("sample change event")
    }

    fn validation_index_keys(
        database: &DatabaseFile,
        db: &str,
        collection: &str,
        index: &str,
    ) -> Vec<String> {
        let validation_index = database
            .validation_state
            .databases
            .get(db)
            .and_then(|db| db.get(collection))
            .and_then(|collection| collection.unique_indexes.get(index))
            .expect("validation index");
        let mut keys = validation_index
            .entries
            .keys()
            .map(|key| key.get_str("sku").expect("sku").to_string())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    #[test]
    fn unflushed_mutations_require_an_explicit_wal_sync() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-sync.mongodb");
        let mut database = DatabaseFile::open_or_create(&path).expect("create database");

        let sequence = database
            .commit_mutation_unflushed(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: Some(Document::new()),
                changes: vec![CollectionChange::Insert(CollectionRecord::new(
                    1,
                    doc! { "_id": 1, "sku": "alpha" },
                ))],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: vec![sample_change_event(1, "insert")],
            })
            .expect("commit mutation");

        assert_eq!(sequence, 1);
        assert_eq!(database.last_applied_sequence(), 1);
        assert_eq!(database.durable_sequence(), 0);
        assert_eq!(database.wal_sync_count(), 0);

        let durable = database.sync_pending_wal().expect("sync wal");
        assert_eq!(durable, 1);
        assert_eq!(database.durable_sequence(), 1);
        assert_eq!(database.wal_sync_count(), 1);
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
    fn info_uses_metadata_only_v2_reader_for_v2_files() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("info-v2.mongodb");

        v2_engine::create_empty(&path).expect("create v2 file");

        let report = DatabaseFile::info(&path).expect("info");
        assert_eq!(report.file_format_version, 9);
        assert_eq!(report.summary.database_count, 0);
        assert_eq!(report.summary.record_count, 0);
        assert_eq!(report.last_checkpoint.active_superblock_slot, 0);
        assert_eq!(report.last_checkpoint.valid_superblocks, 1);
    }

    #[test]
    fn inspect_reports_metadata_for_clean_v2_files() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("inspect-metadata-only.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut widgets = CollectionCatalog::new(doc! {});
            insert_record(&mut widgets, 1, doc! { "_id": 1, "sku": "alpha", "qty": 2 });
            apply_index_specs(
                &mut widgets,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: widgets,
                    change_events: vec![sample_change_event(1, "insert")],
                })
                .expect("seed widgets");
            database.checkpoint().expect("checkpoint");
        }

        let report = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(report.last_applied_sequence, 1);
        assert_eq!(report.current_record_count, 1);
        assert_eq!(report.current_index_entry_count, 2);
        assert_eq!(report.current_change_event_count, 1);
        assert_eq!(report.wal_records_since_checkpoint, 0);
        assert!(!report.truncated_wal_tail);
        assert_eq!(report.databases, vec!["app".to_string()]);
    }

    #[test]
    fn recovers_rewrite_collection_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-rewrite-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection =
                CollectionCatalog::new(doc! { "validator": { "qty": { "$gte": 0 } } });
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "a", "qty": 4 });
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1" }],
            )
            .expect("create index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("commit mutation");
            database
                .commit_mutation(WalMutation::RewriteCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    options: doc! { "validator": { "qty": { "$gte": 0 } } },
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        1,
                        doc! { "_id": 2, "sku": "b", "qty": 7 },
                    ))],
                    change_events: Vec::new(),
                })
                .expect("rewrite collection");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(collection.records.len(), 1);
        assert_eq!(
            collection.records[0].document.get_i32("qty").expect("qty"),
            7
        );
        assert_eq!(collection.indexes.len(), 1);
        assert!(collection.indexes.contains_key("_id_"));
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
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        1,
                        doc! { "_id": 1, "sku": "alpha", "qty": 4 },
                    ))],
                    inserts: vec![CollectionRecord::new(
                        1,
                        doc! { "_id": 1, "sku": "alpha", "qty": 4 },
                    )],
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
                    changes: vec![
                        CollectionChange::Insert(CollectionRecord::new(
                            3,
                            doc! { "_id": 3, "sku": "charlie", "qty": 3 },
                        )),
                        CollectionChange::Update(CollectionRecord::new(
                            1,
                            doc! { "_id": 1, "sku": "delta", "qty": 9 },
                        )),
                        CollectionChange::Delete(2),
                    ],
                    inserts: vec![CollectionRecord::new(
                        3,
                        doc! { "_id": 3, "sku": "charlie", "qty": 3 },
                    )],
                    updates: vec![CollectionRecord::new(
                        1,
                        doc! { "_id": 1, "sku": "delta", "qty": 9 },
                    )],
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
        let entries = index.entries_snapshot();
        assert_eq!(
            entries
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
    fn recovers_create_indexes_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-create-index-recovery.mongodb");
        let change_events = vec![sample_change_event(2, "createIndexes")];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "alpha" });
            insert_record(&mut collection, 2, doc! { "_id": 2, "sku": "beta" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("base mutation");
            database
                .commit_mutation(WalMutation::CreateIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    specs: vec![doc! {
                        "key": { "sku": 1 },
                        "name": "sku_1",
                        "unique": true,
                    }],
                    change_events: change_events.clone(),
                })
                .expect("create index mutation");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        let index = collection.indexes.get("sku_1").expect("sku index");
        let entries = index.entries_snapshot();
        assert_eq!(
            entries
                .iter()
                .map(|entry| {
                    (
                        entry.record_id,
                        entry.key.get_str("sku").expect("sku").to_string(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(1, "alpha".to_string()), (2, "beta".to_string())]
        );
        assert_eq!(reopened.change_events(), change_events.as_slice());
    }

    #[test]
    fn validation_cache_tracks_unique_keys_incrementally_across_writes() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("validation-cache.mongodb");

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
            .expect("replace collection");
        assert_eq!(
            validation_index_keys(&database, "app", "widgets", "sku_1"),
            vec!["alpha".to_string(), "beta".to_string()]
        );

        database
            .commit_mutation(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: None,
                changes: vec![CollectionChange::Insert(CollectionRecord::new(
                    3,
                    doc! { "_id": 3, "sku": "gamma" },
                ))],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: Vec::new(),
            })
            .expect("insert");
        assert_eq!(
            validation_index_keys(&database, "app", "widgets", "sku_1"),
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );

        let error = database
            .commit_mutation(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: None,
                changes: vec![CollectionChange::Insert(CollectionRecord::new(
                    4,
                    doc! { "_id": 4, "sku": "gamma" },
                ))],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: Vec::new(),
            })
            .expect_err("duplicate insert should fail");
        assert!(
            error
                .to_string()
                .contains("duplicate key error on index `sku_1`"),
            "unexpected duplicate error: {error:#}"
        );
        assert_eq!(
            validation_index_keys(&database, "app", "widgets", "sku_1"),
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );

        database
            .commit_mutation(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: None,
                changes: vec![CollectionChange::Update(CollectionRecord::new(
                    2,
                    doc! { "_id": 2, "sku": "delta" },
                ))],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: Vec::new(),
            })
            .expect("update");
        assert_eq!(
            validation_index_keys(&database, "app", "widgets", "sku_1"),
            vec![
                "alpha".to_string(),
                "delta".to_string(),
                "gamma".to_string()
            ]
        );

        database
            .commit_mutation(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: None,
                changes: vec![CollectionChange::Delete(1)],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: Vec::new(),
            })
            .expect("delete");
        assert_eq!(
            validation_index_keys(&database, "app", "widgets", "sku_1"),
            vec!["delta".to_string(), "gamma".to_string()]
        );

        database
            .commit_mutation(WalMutation::DropIndexes {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                target: "sku_1".to_string(),
                change_events: Vec::new(),
            })
            .expect("drop index");
        let validation_collection = database
            .validation_state
            .databases
            .get("app")
            .and_then(|db| db.get("widgets"))
            .expect("validation collection");
        assert!(validation_collection.unique_indexes.contains_key("_id_"));
        assert!(!validation_collection.unique_indexes.contains_key("sku_1"));
    }

    #[test]
    fn recovers_drop_indexes_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-drop-index-recovery.mongodb");
        let change_events = vec![sample_change_event(2, "dropIndexes")];

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "alpha" });
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
            database.checkpoint().expect("checkpoint");
            database
                .commit_mutation(WalMutation::DropIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    target: "sku_1".to_string(),
                    change_events: change_events.clone(),
                })
                .expect("drop index mutation");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert!(!collection.indexes.contains_key("sku_1"));
        assert!(collection.indexes.contains_key("_id_"));
        assert_eq!(reopened.change_events(), change_events.as_slice());
    }

    #[test]
    fn replays_ordered_update_then_insert_changes_without_false_duplicate_key_failures() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-ordered-delta-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "alpha" });
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
                    changes: vec![
                        CollectionChange::Update(CollectionRecord::new(
                            1,
                            doc! { "_id": 1, "sku": "beta" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            2,
                            doc! { "_id": 2, "sku": "alpha" },
                        )),
                    ],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("ordered mutation");
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
                .map(|record| record.document.get_str("sku").expect("sku"))
                .collect::<Vec<_>>(),
            vec!["beta", "alpha"]
        );
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
        let entries = sku_index.entries_snapshot();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].record_id, 7);
        assert_eq!(entries[1].record_id, 12);
        assert_eq!(
            collection.records[1].document.get_str("sku").expect("sku"),
            "beta"
        );
        assert_eq!(collection.next_record_id(), 13);
    }

    #[test]
    fn checkpoints_persist_next_record_id_after_deleting_the_highest_record_id() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("next-record-id.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 7, doc! { "_id": 1, "sku": "alpha" });
            insert_record(&mut collection, 12, doc! { "_id": 2, "sku": "beta" });
            assert_eq!(collection.delete_records(&BTreeSet::from([12_u64])), 1);
            assert_eq!(collection.next_record_id(), 13);
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
        assert_eq!(collection.records.len(), 1);
        assert_eq!(collection.records[0].record_id, 7);
        assert_eq!(collection.next_record_id(), 13);
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
            .entries_snapshot()
            .into_iter()
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
    fn checkpoints_replayed_replace_collection_after_reopen() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("replayed-replace-checkpoint.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1, "sku": "alpha" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection.clone(),
                    change_events: Vec::new(),
                })
                .expect("base mutation");
            database.checkpoint().expect("base checkpoint");

            collection
                .update_record_at(0, doc! { "_id": 1, "sku": "beta" })
                .expect("update record");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("wal mutation");
        }

        {
            let mut reopened = DatabaseFile::open_or_create(&path).expect("reopen with wal");
            let collection = reopened
                .catalog()
                .get_collection("app", "widgets")
                .expect("collection");
            assert_eq!(collection.records.len(), 1);
            assert_eq!(
                collection.records[0].document.get_str("sku").expect("sku"),
                "beta"
            );
            reopened.checkpoint().expect("checkpoint replayed state");
        }

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen after checkpoint");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(collection.records.len(), 1);
        assert_eq!(
            collection.records[0].document.get_str("sku").expect("sku"),
            "beta"
        );
        assert!(!reopened.has_pending_wal());
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
                .entries_snapshot()
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
    fn replays_compressed_wal_frames() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("compressed-wal.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            for record_id in 1..=20_u64 {
                insert_record(
                    &mut collection,
                    record_id,
                    doc! {
                        "_id": record_id as i64,
                        "payload": "z".repeat(PAGE_SIZE / 3),
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
        }

        let payload = first_wal_payload(&path);
        assert!(payload.starts_with(ZSTD_BLOB_MAGIC));

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(collection.records.len(), 20);
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
                checkpoint_generation: 2,
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

    #[test]
    fn rejects_existing_non_v2_files() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("legacy.mongodb");
        std::fs::write(&path, b"MQLTHD07legacy").expect("write legacy file");

        let error = DatabaseFile::open_or_create(&path).expect_err("reject non-v2 file");
        assert!(
            error.to_string().contains("supported v2 mqlite database"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn storage_metadata_surface_reports_namespaces_and_indexes() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("metadata-surface.mongodb");

        let mut database = DatabaseFile::open_or_create(&path).expect("create database");
        let mut collection =
            CollectionCatalog::new(doc! { "validator": { "sku": { "$exists": true } } });
        insert_record(
            &mut collection,
            1,
            doc! { "_id": 1_i64, "sku": "alpha", "qty": 2_i64 },
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
            .expect("seed collection");

        assert_eq!(
            StorageEngine::database_names(&database).expect("database names"),
            vec!["app".to_string()]
        );
        assert_eq!(
            StorageEngine::collection_names(&database, "app").expect("collection names"),
            vec!["widgets".to_string()]
        );
        assert_eq!(
            StorageEngine::collection_metadata(&database, "app", "widgets")
                .expect("collection metadata")
                .expect("collection exists")
                .options,
            doc! { "validator": { "sku": { "$exists": true } } }
        );

        let indexes = StorageEngine::list_indexes(&database, "app", "widgets")
            .expect("list indexes")
            .expect("indexes");
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].name, "_id_");
        assert_eq!(indexes[0].key_pattern, doc! { "_id": 1 });
        assert!(indexes[0].unique);
        assert_eq!(indexes[1].name, "sku_1");
        assert_eq!(indexes[1].key_pattern, doc! { "sku": 1 });
        assert!(indexes[1].unique);
    }

    #[test]
    fn startup_metadata_tracks_checkpointed_state_and_wal_tail() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("startup-metadata.mongodb");

        let mut database = DatabaseFile::open_or_create(&path).expect("create database");
        let clean = DatabaseFile::startup_metadata(&path).expect("startup metadata");
        assert_eq!(
            clean,
            StartupMetadata {
                durable_sequence: 0,
                has_pending_wal: false,
            }
        );

        database
            .commit_mutation(WalMutation::ReplaceCollection {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                collection_state: CollectionCatalog::new(doc! {}),
                change_events: Vec::new(),
            })
            .expect("append wal");
        let dirty = DatabaseFile::startup_metadata(&path).expect("startup metadata with wal");
        assert_eq!(dirty.durable_sequence, 0);
        assert!(dirty.has_pending_wal);

        database.checkpoint().expect("checkpoint");
        let checkpointed = DatabaseFile::startup_metadata(&path).expect("startup metadata clean");
        assert_eq!(
            checkpointed,
            StartupMetadata {
                durable_sequence: 1,
                has_pending_wal: false,
            }
        );
    }

    #[test]
    fn concurrent_checkpoints_preserve_writes_committed_after_capture() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("concurrent-checkpoint.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            insert_record(&mut collection, 1, doc! { "_id": 1_i64, "sku": "seed" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("seed mutation");
            database.checkpoint().expect("seed checkpoint");

            let mut large_collection = CollectionCatalog::new(doc! {});
            for record_id in 1..=96_u64 {
                insert_record(
                    &mut large_collection,
                    record_id,
                    doc! {
                        "_id": record_id as i64,
                        "sku": format!("sku-{record_id}"),
                        "payload": "x".repeat(128),
                    },
                );
            }
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: large_collection,
                    change_events: Vec::new(),
                })
                .expect("large mutation");

            let mut compact_collection = CollectionCatalog::new(doc! {});
            insert_record(
                &mut compact_collection,
                1,
                doc! { "_id": 1_i64, "sku": "alpha", "payload": "x".repeat(32) },
            );
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: compact_collection,
                    change_events: Vec::new(),
                })
                .expect("compact mutation");
            database.checkpoint().expect("compact checkpoint");

            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: CollectionCatalog::new(doc! {}),
                    change_events: Vec::new(),
                })
                .expect("empty mutation");
            database.checkpoint().expect("empty checkpoint");

            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        2,
                        doc! { "_id": 2_i64, "sku": "beta", "payload": "y".repeat(32) },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("captured insert");
            let job = database
                .prepare_concurrent_checkpoint()
                .expect("prepare concurrent checkpoint")
                .expect("checkpoint job");

            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        3,
                        doc! { "_id": 3_i64, "sku": "gamma", "payload": "z".repeat(32) },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("post-capture insert");

            let completed = job
                .run()
                .expect("run concurrent checkpoint")
                .expect("completed checkpoint");
            assert!(
                database
                    .finish_concurrent_checkpoint(completed)
                    .expect("finish checkpoint"),
                "expected checkpoint completion to apply"
            );
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.current_record_count, 2);
        assert_eq!(inspect.wal_records_since_checkpoint, 1);

        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        let ids = collection
            .records
            .iter()
            .map(|record| record.document.get_i64("_id").expect("_id"))
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn concurrent_checkpoints_append_when_no_reusable_space_exists() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join("concurrent-checkpoint-no-space.mongodb");

        let mut database = DatabaseFile::open_or_create(&path).expect("create database");
        database
            .commit_mutation(WalMutation::ApplyCollectionChanges {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                create_options: Some(doc! {}),
                changes: vec![CollectionChange::Insert(CollectionRecord::new(
                    1,
                    doc! { "_id": 1_i64, "sku": "alpha" },
                ))],
                inserts: Vec::new(),
                updates: Vec::new(),
                deletes: Vec::new(),
                change_events: Vec::new(),
            })
            .expect("mutation");

        let job = database
            .prepare_concurrent_checkpoint()
            .expect("prepare concurrent checkpoint")
            .expect("checkpoint job");
        assert!(
            database
                .finish_concurrent_checkpoint(
                    job.run()
                        .expect("run concurrent checkpoint")
                        .expect("completed checkpoint"),
                )
                .expect("finish checkpoint"),
            "expected the concurrent checkpoint to apply"
        );

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.current_record_count, 1);
        assert_eq!(inspect.wal_records_since_checkpoint, 0);
    }
}
