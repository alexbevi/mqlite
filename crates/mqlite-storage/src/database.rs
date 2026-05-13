use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use blake3::Hasher;
use bson::{Bson, Document, doc};
use ciborium::{de as cbor_de, ser as cbor_ser};
use fs4::FileExt;
use mqlite_catalog::{
    Catalog, CatalogError, CollectionCatalog, CollectionMutation, CollectionRecord, IndexBound,
    IndexBounds, IndexCatalog, IndexEntry, apply_index_specs, build_index_specs,
    validate_collection_indexes, validate_drop_indexes,
};
use mqlite_debug::{Component, add_counter, record_duration, set_metadata, span};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    engine::{CollectionMetadata, CollectionReadView, IndexMetadata, IndexReadView, StorageEngine},
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
const WAL_METADATA_PAYLOAD_MAGIC: &[u8; 8] = b"MQLWMD01";
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
    token: Arc<[u8]>,
    pub cluster_time: bson::Timestamp,
    pub wall_time: bson::DateTime,
    pub database: String,
    pub collection: Option<String>,
    pub operation_type: String,
    document_key: Option<Arc<[u8]>>,
    full_document: Option<Arc<[u8]>>,
    full_document_before_change: Option<Arc<[u8]>>,
    update_description: Option<Arc<[u8]>>,
    pub expanded: bool,
    extra_fields: Arc<[u8]>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalMutationKind {
    ReplaceCollection,
    RewriteCollection,
    ApplyCollectionChanges,
    CreateIndexes,
    DropIndexes,
    DropCollection,
}

impl WalMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ReplaceCollection => "replaceCollection",
            Self::RewriteCollection => "rewriteCollection",
            Self::ApplyCollectionChanges => "applyCollectionChanges",
            Self::CreateIndexes => "createIndexes",
            Self::DropIndexes => "dropIndexes",
            Self::DropCollection => "dropCollection",
        }
    }

    fn replay_apply_operation(self) -> &'static str {
        match self {
            Self::ReplaceCollection => "replay_apply_replace_collection",
            Self::RewriteCollection => "replay_apply_rewrite_collection",
            Self::ApplyCollectionChanges => "replay_apply_collection_changes",
            Self::CreateIndexes => "replay_apply_create_indexes",
            Self::DropIndexes => "replay_apply_drop_indexes",
            Self::DropCollection => "replay_apply_drop_collection",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WalMutationReplayStats {
    kind: WalMutationKind,
    touched_documents: u64,
    touched_document_bytes: u64,
    change_events: u64,
    index_specs: u64,
}

#[derive(Debug, Clone)]
struct DecodedWalEntry {
    entry: WalEntry,
    decoded_len: usize,
    compressed: bool,
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
struct WalFrameMetadata {
    #[serde(default)]
    sequence: u64,
    mutation: WalFrameMetadataMutation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalFrameMetadataMutation {
    ReplaceCollection {
        database: String,
        collection: String,
        collection_metadata: WalFrameCollectionMetadata,
        change_event_count: usize,
    },
    RewriteCollection {
        database: String,
        collection: String,
        changes: WalFrameCollectionChangesMetadata,
        change_event_count: usize,
    },
    ApplyCollectionChanges {
        database: String,
        collection: String,
        creates_collection: bool,
        changes: WalFrameCollectionChangesMetadata,
        change_event_count: usize,
    },
    CreateIndexes {
        database: String,
        collection: String,
        creates_collection: bool,
        indexes: Vec<WalFrameIndexMetadata>,
        change_event_count: usize,
    },
    DropIndexes {
        database: String,
        collection: String,
        target: String,
        change_event_count: usize,
    },
    DropCollection {
        database: String,
        collection: String,
        change_event_count: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalFrameCollectionMetadata {
    document_count: usize,
    document_bytes: u64,
    indexes: Vec<WalFrameIndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalFrameIndexMetadata {
    name: String,
    key: Vec<u8>,
    unique: bool,
    expire_after_seconds: Option<i64>,
    entry_count: usize,
    bytes: u64,
    #[serde(default)]
    value_frequencies: Vec<WalFrameValueFrequency>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalFrameValueFrequency {
    field: String,
    value: Vec<u8>,
    count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WalFrameCollectionChangesMetadata {
    inserts: usize,
    insert_bytes: u64,
    updates: usize,
    update_bytes: u64,
    deletes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactWalEntry {
    sequence: u64,
    mutation: CompactWalMutation,
}

#[derive(Debug, Deserialize)]
struct CompactWalEntrySequence {
    sequence: u64,
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
    dirty_collections: Arc<BTreeSet<(String, String)>>,
    change_events_dirty: bool,
    plan_cache_dirty: bool,
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

pub struct PendingWalCollectionReadView {
    pub last_sequence: u64,
    pub wal_records: usize,
    pub relevant_wal_records: usize,
    pub wal_bytes: u64,
    pub used_overlay: bool,
    pub view: Option<Box<dyn CollectionReadView>>,
}

pub struct PendingWalIdLookup {
    pub last_sequence: u64,
    pub wal_records: usize,
    pub relevant_wal_records: usize,
    pub wal_bytes: u64,
    pub document: Option<Document>,
}

pub struct PendingWalEqualityCount {
    pub last_sequence: u64,
    pub wal_records: usize,
    pub relevant_wal_records: usize,
    pub wal_bytes: u64,
    pub count: usize,
}

struct DeltaOverlayCollectionReadView {
    base: Arc<dyn CollectionReadView>,
    delta: CollectionCatalog,
    indexes: BTreeMap<String, DeltaOverlayIndexReadView>,
}

struct ArcCollectionReadView(Arc<dyn CollectionReadView>);

struct DeltaOverlayIndexReadView {
    name: String,
    key_pattern: Document,
    base: Arc<dyn CollectionReadView>,
    delta: IndexCatalog,
}

impl DeltaOverlayCollectionReadView {
    fn new(base: Arc<dyn CollectionReadView>, delta: CollectionCatalog) -> Self {
        let indexes = delta
            .indexes
            .iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    DeltaOverlayIndexReadView {
                        name: name.clone(),
                        key_pattern: index.key.clone(),
                        base: Arc::clone(&base),
                        delta: index.clone(),
                    },
                )
            })
            .collect();
        Self {
            base,
            delta,
            indexes,
        }
    }
}

impl CollectionReadView for DeltaOverlayCollectionReadView {
    fn scan_records(&self) -> Result<Vec<CollectionRecord>> {
        let mut records = self.base.scan_records()?;
        records.extend(self.delta.scan_records()?);
        Ok(records)
    }

    fn record_document(&self, record_id: u64) -> Result<Option<Document>> {
        if let Some(document) = self.delta.record_document(record_id)? {
            return Ok(Some(document));
        }
        self.base.record_document(record_id)
    }

    fn index_names(&self) -> Vec<String> {
        let mut names = self.base.index_names();
        for name in self.indexes.keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
        names
    }

    fn index(&self, name: &str) -> Option<&dyn IndexReadView> {
        self.indexes
            .get(name)
            .map(|index| index as &dyn IndexReadView)
            .or_else(|| self.base.index(name))
    }
}

impl CollectionReadView for ArcCollectionReadView {
    fn scan_records(&self) -> Result<Vec<CollectionRecord>> {
        self.0.scan_records()
    }

    fn record_document(&self, record_id: u64) -> Result<Option<Document>> {
        self.0.record_document(record_id)
    }

    fn index_names(&self) -> Vec<String> {
        self.0.index_names()
    }

    fn index(&self, name: &str) -> Option<&dyn IndexReadView> {
        self.0.index(name)
    }
}

impl IndexReadView for DeltaOverlayIndexReadView {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_pattern(&self) -> &Document {
        &self.key_pattern
    }

    fn entry_count(&self) -> usize {
        self.base
            .index(&self.name)
            .map(IndexReadView::entry_count)
            .unwrap_or(0)
            + self.delta.entry_count()
    }

    fn scan_entries(&self, bounds: &IndexBounds) -> Result<Vec<IndexEntry>> {
        let mut entries = self
            .base
            .index(&self.name)
            .map(|index| index.scan_entries(bounds))
            .transpose()?
            .unwrap_or_default();
        entries.extend(self.delta.scan_entries(bounds));
        Ok(entries)
    }

    fn estimate_bounds_count(&self, bounds: &IndexBounds) -> usize {
        self.base
            .index(&self.name)
            .map(|index| index.estimate_bounds_count(bounds))
            .unwrap_or(0)
            + self.delta.estimate_bounds_count(bounds)
    }

    fn covers_paths(&self, paths: &BTreeSet<String>) -> bool {
        self.delta.covers_paths(paths)
            || self
                .base
                .index(&self.name)
                .is_some_and(|index| index.covers_paths(paths))
    }

    fn estimate_value_count(&self, field: &str, value: &Bson) -> Option<usize> {
        let base = self
            .base
            .index(&self.name)
            .and_then(|index| index.estimate_value_count(field, value));
        let delta = self.delta.estimate_value_count(field, value);
        sum_optional_counts(base, delta)
    }

    fn estimate_values_count(&self, field: &str, values: &[Bson]) -> Option<usize> {
        let base = self
            .base
            .index(&self.name)
            .and_then(|index| index.estimate_values_count(field, values));
        let delta = self.delta.estimate_values_count(field, values);
        sum_optional_counts(base, delta)
    }

    fn estimate_range_count(
        &self,
        field: &str,
        lower: Option<(&Bson, bool)>,
        upper: Option<(&Bson, bool)>,
    ) -> Option<usize> {
        let base = self
            .base
            .index(&self.name)
            .and_then(|index| index.estimate_range_count(field, lower, upper));
        let delta = self.delta.estimate_range_count(field, lower, upper);
        sum_optional_counts(base, delta)
    }

    fn present_count(&self, field: &str) -> Option<usize> {
        let base = self
            .base
            .index(&self.name)
            .and_then(|index| index.present_count(field));
        let delta = self.delta.present_count(field);
        sum_optional_counts(base, delta)
    }
}

fn sum_optional_counts(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

impl DatabaseFile {
    pub fn startup_metadata(path: impl AsRef<Path>) -> Result<StartupMetadata> {
        let _span = span(Component::Storage, "startup_metadata");
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
        let _span = span(Component::Storage, "open_page_backed_collection_read_view");
        Ok(
            v2_engine::open_collection_read_view(path, database, collection)?
                .map(|view| Box::new(view) as Box<dyn CollectionReadView>),
        )
    }

    pub fn open_pending_wal_collection_read_view(
        path: impl AsRef<Path>,
        database: &str,
        collection: &str,
        max_wal_bytes: u64,
    ) -> Result<Option<PendingWalCollectionReadView>> {
        let _span = span(Component::Storage, "open_pending_wal_collection_read_view");
        let path = path.as_ref();
        let pager = V2Pager::open(path)?;
        let superblock = pager.active_superblock().clone();
        let file_size = std::fs::metadata(path)?.len();
        let wal_bytes = file_size.saturating_sub(superblock.wal_start_offset);
        if wal_bytes == 0 {
            return Ok(Some(PendingWalCollectionReadView {
                last_sequence: superblock.durable_lsn,
                wal_records: 0,
                relevant_wal_records: 0,
                wal_bytes: 0,
                used_overlay: false,
                view: Self::open_page_backed_collection_read_view(path, database, collection)?,
            }));
        }
        if wal_bytes > max_wal_bytes {
            return Ok(None);
        }

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut offset = superblock.wal_start_offset;
        let mut last_sequence = superblock.durable_lsn;
        let mut wal_records = 0_usize;
        let mut relevant_wal_records = 0_usize;
        let mut overlay_state: Option<PersistedState> = None;
        let mut overlay_delta: Option<CollectionCatalog> = None;
        let mut base_view: Option<Arc<dyn CollectionReadView>> = None;

        while offset < file_size {
            if file_size - offset < WAL_HEADER_LEN as u64 {
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
                break;
            }

            let mut payload = vec![0_u8; payload_len as usize];
            file.read_exact(&mut payload)
                .map_err(|_| StorageError::Truncated)?;

            if hash_bytes(&payload) != header[8..40] {
                return Err(StorageError::InvalidWalChecksum.into());
            }

            let compact = decode_compact_wal_entry(&payload)?;
            if compact.sequence <= last_sequence {
                offset = payload_end;
                continue;
            }

            wal_records += 1;
            last_sequence = compact.sequence;
            if compact_mutation_targets_namespace(&compact.mutation, database, collection) {
                relevant_wal_records += 1;
                let sequence = compact.sequence;
                let mutation = compact.into_wal_entry()?.mutation;
                if matches!(mutation, WalMutation::CreateIndexes { .. }) {
                    if base_view.is_none() {
                        base_view = Self::open_page_backed_collection_read_view(
                            path, database, collection,
                        )?
                        .map(Arc::from);
                    }
                    offset = payload_end;
                    continue;
                }
                if overlay_state.is_none() {
                    if base_view.is_none() {
                        base_view = Self::open_page_backed_collection_read_view(
                            path, database, collection,
                        )?
                        .map(Arc::from);
                    }
                    if let (Some(base), Some(delta)) =
                        (base_view.as_deref(), overlay_delta.as_mut())
                    {
                        if try_apply_insert_only_delta(delta, base, &mutation)? {
                            offset = payload_end;
                            continue;
                        }
                    } else if let Some(base) = base_view.as_deref() {
                        if let Some(delta) = build_insert_only_delta(base, &mutation)? {
                            overlay_delta = Some(delta);
                            offset = payload_end;
                            continue;
                        }
                    }
                    overlay_state = Some(build_pending_collection_overlay_state(
                        path,
                        database,
                        collection,
                        superblock.durable_lsn,
                        superblock.last_checkpoint_unix_ms,
                    )?);
                    if let Some(delta) = overlay_delta.take() {
                        merge_delta_into_overlay_state(
                            overlay_state.as_mut().expect("overlay state"),
                            database,
                            collection,
                            delta,
                        )?;
                    }
                }
                apply_mutation(
                    overlay_state.as_mut().expect("overlay state"),
                    sequence,
                    &mutation,
                )?;
            }

            offset = payload_end;
        }

        add_counter(
            Component::Storage,
            "pendingWalOverlayScannedRecords",
            wal_records as u64,
        );
        add_counter(
            Component::Storage,
            "pendingWalOverlayRelevantRecords",
            relevant_wal_records as u64,
        );
        add_counter(
            Component::Storage,
            "pendingWalOverlayScannedBytes",
            wal_bytes,
        );

        if relevant_wal_records == 0 {
            return Ok(Some(PendingWalCollectionReadView {
                last_sequence,
                wal_records,
                relevant_wal_records,
                wal_bytes,
                used_overlay: false,
                view: Self::open_page_backed_collection_read_view(path, database, collection)?,
            }));
        }

        let view = if let Some(state) = overlay_state {
            state
                .catalog
                .get_collection(database, collection)
                .ok()
                .cloned()
                .map(|collection| Box::new(collection) as Box<dyn CollectionReadView>)
        } else if overlay_delta.is_none() {
            base_view
                .or_else(|| {
                    Self::open_page_backed_collection_read_view(path, database, collection)
                        .ok()
                        .flatten()
                        .map(Arc::from)
                })
                .map(|base| Box::new(ArcCollectionReadView(base)) as Box<dyn CollectionReadView>)
        } else {
            let base = base_view.expect("base view initialized for insert-only overlay");
            let delta = overlay_delta.expect("delta initialized for insert-only overlay");
            Some(Box::new(DeltaOverlayCollectionReadView::new(base, delta))
                as Box<dyn CollectionReadView>)
        };

        Ok(Some(PendingWalCollectionReadView {
            last_sequence,
            wal_records,
            relevant_wal_records,
            wal_bytes,
            used_overlay: true,
            view,
        }))
    }

    pub fn find_pending_wal_document_by_id(
        path: impl AsRef<Path>,
        database: &str,
        collection: &str,
        id: &Bson,
        max_wal_bytes: u64,
    ) -> Result<Option<PendingWalIdLookup>> {
        let _span = span(Component::Storage, "find_pending_wal_document_by_id");
        let path = path.as_ref();
        let pager = V2Pager::open(path)?;
        let superblock = pager.active_superblock().clone();
        let file_size = std::fs::metadata(path)?.len();
        let wal_bytes = file_size.saturating_sub(superblock.wal_start_offset);
        if wal_bytes == 0 || wal_bytes > max_wal_bytes {
            return Ok(None);
        }

        let mut found = Self::open_page_backed_collection_read_view(path, database, collection)?
            .and_then(|view| find_page_backed_document_by_id(view.as_ref(), id).transpose())
            .transpose()?
            .map(|(record_id, document)| (record_id, document));
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut offset = superblock.wal_start_offset;
        let mut last_sequence = superblock.durable_lsn;
        let mut scanned_records = 0_u64;
        let mut relevant_records = 0_u64;

        while offset < file_size {
            if file_size - offset < WAL_HEADER_LEN as u64 {
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
                break;
            }

            let mut payload = vec![0_u8; payload_len as usize];
            file.read_exact(&mut payload)
                .map_err(|_| StorageError::Truncated)?;

            if hash_bytes(&payload) != header[8..40] {
                return Err(StorageError::InvalidWalChecksum.into());
            }

            if found.is_some() {
                if let Some(metadata) = decode_wal_frame_metadata(&payload)? {
                    scanned_records += 1;
                    if wal_metadata_preserves_id_lookup_after_found(
                        &metadata.mutation,
                        database,
                        collection,
                    ) {
                        if wal_metadata_targets_namespace(&metadata.mutation, database, collection)
                        {
                            relevant_records += 1;
                        }
                        offset = payload_end;
                        continue;
                    }
                    return Ok(None);
                }
            }

            let compact = decode_compact_wal_entry(&payload)?;
            if compact.sequence <= last_sequence {
                offset = payload_end;
                continue;
            }

            scanned_records += 1;
            last_sequence = compact.sequence;
            if !compact_mutation_targets_namespace(&compact.mutation, database, collection) {
                offset = payload_end;
                continue;
            }
            relevant_records += 1;

            match compact.mutation {
                CompactWalMutation::ApplyCollectionChanges {
                    changes,
                    inserts,
                    updates,
                    deletes,
                    ..
                } => {
                    apply_compact_id_lookup_changes(
                        &mut found, id, changes, inserts, updates, deletes,
                    )?;
                }
                CompactWalMutation::CreateIndexes { .. } => {}
                CompactWalMutation::ReplaceCollection { .. }
                | CompactWalMutation::RewriteCollection { .. }
                | CompactWalMutation::DropIndexes { .. }
                | CompactWalMutation::DropCollection { .. } => return Ok(None),
            }

            offset = payload_end;
        }

        add_counter(
            Component::Storage,
            "pendingWalIdLookupScannedRecords",
            scanned_records,
        );
        add_counter(
            Component::Storage,
            "pendingWalIdLookupRelevantRecords",
            relevant_records,
        );
        add_counter(
            Component::Storage,
            "pendingWalIdLookupScannedBytes",
            wal_bytes,
        );

        Ok(Some(PendingWalIdLookup {
            last_sequence,
            wal_records: scanned_records as usize,
            relevant_wal_records: relevant_records as usize,
            wal_bytes,
            document: found.map(|(_, document)| document),
        }))
    }

    pub fn count_pending_wal_field_eq(
        path: impl AsRef<Path>,
        database: &str,
        collection: &str,
        field: &str,
        value: &Bson,
        max_wal_bytes: u64,
    ) -> Result<Option<PendingWalEqualityCount>> {
        let _span = span(Component::Storage, "count_pending_wal_field_eq");
        let path = path.as_ref();
        let pager = V2Pager::open(path)?;
        let superblock = pager.active_superblock().clone();
        if superblock.summary.record_count != 0 {
            return Ok(None);
        }
        let file_size = std::fs::metadata(path)?.len();
        let wal_bytes = file_size.saturating_sub(superblock.wal_start_offset);
        if wal_bytes == 0 || wal_bytes > max_wal_bytes {
            return Ok(None);
        }

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut offset = superblock.wal_start_offset;
        let mut last_sequence = superblock.durable_lsn;
        let mut scanned_records = 0_u64;
        let mut relevant_records = 0_u64;
        let mut count = None::<usize>;

        while offset < file_size {
            if file_size - offset < WAL_HEADER_LEN as u64 {
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
                break;
            }

            let mut payload = vec![0_u8; payload_len as usize];
            file.read_exact(&mut payload)
                .map_err(|_| StorageError::Truncated)?;

            if hash_bytes(&payload) != header[8..40] {
                return Err(StorageError::InvalidWalChecksum.into());
            }

            if let Some(metadata) = decode_wal_frame_metadata(&payload)? {
                let sequence = if metadata.sequence == 0 {
                    decode_compact_wal_entry_sequence(&payload)?
                } else {
                    metadata.sequence
                };
                if sequence <= last_sequence {
                    offset = payload_end;
                    continue;
                }

                scanned_records += 1;
                last_sequence = sequence;
                if !wal_metadata_targets_namespace(&metadata.mutation, database, collection) {
                    offset = payload_end;
                    continue;
                }
                relevant_records += 1;
                match metadata.mutation {
                    WalFrameMetadataMutation::CreateIndexes { indexes, .. } => {
                        if let Some(metadata_count) =
                            index_metadata_value_count(&indexes, field, value)?
                        {
                            count = Some(metadata_count);
                        }
                        offset = payload_end;
                        continue;
                    }
                    WalFrameMetadataMutation::ApplyCollectionChanges { changes, .. } => {
                        if count.is_some() && changes.updates == 0 && changes.deletes == 0 {
                            let compact = decode_compact_wal_entry(&payload)?;
                            let CompactWalMutation::ApplyCollectionChanges {
                                changes,
                                inserts,
                                updates,
                                deletes,
                                ..
                            } = compact.mutation
                            else {
                                return Ok(None);
                            };
                            if !updates.is_empty() || !deletes.is_empty() {
                                return Ok(None);
                            }
                            for change in changes {
                                let CompactCollectionChange::Insert(record) = change else {
                                    return Ok(None);
                                };
                                if compact_record_field_eq(&record, field, value)? {
                                    *count.as_mut().expect("count initialized") += 1;
                                }
                            }
                            for record in inserts {
                                if compact_record_field_eq(&record, field, value)? {
                                    *count.as_mut().expect("count initialized") += 1;
                                }
                            }
                            offset = payload_end;
                            continue;
                        }
                        offset = payload_end;
                        continue;
                    }
                    WalFrameMetadataMutation::DropIndexes { .. } => {
                        offset = payload_end;
                        continue;
                    }
                    WalFrameMetadataMutation::ReplaceCollection { .. }
                    | WalFrameMetadataMutation::RewriteCollection { .. }
                    | WalFrameMetadataMutation::DropCollection { .. } => return Ok(None),
                }
            }

            let compact = decode_compact_wal_entry(&payload)?;
            if compact.sequence <= last_sequence {
                offset = payload_end;
                continue;
            }

            scanned_records += 1;
            last_sequence = compact.sequence;
            if !compact_mutation_targets_namespace(&compact.mutation, database, collection) {
                offset = payload_end;
                continue;
            }
            relevant_records += 1;

            match compact.mutation {
                CompactWalMutation::ApplyCollectionChanges {
                    changes,
                    inserts,
                    updates,
                    deletes,
                    ..
                } => {
                    if !updates.is_empty() || !deletes.is_empty() {
                        return Ok(None);
                    }
                    if !changes.is_empty() {
                        for change in changes {
                            let CompactCollectionChange::Insert(record) = change else {
                                return Ok(None);
                            };
                            if compact_record_field_eq(&record, field, value)? {
                                *count.get_or_insert(0) += 1;
                            }
                        }
                        offset = payload_end;
                        continue;
                    }
                    for record in inserts {
                        if compact_record_field_eq(&record, field, value)? {
                            *count.get_or_insert(0) += 1;
                        }
                    }
                }
                CompactWalMutation::CreateIndexes { .. } => {}
                CompactWalMutation::ReplaceCollection { .. }
                | CompactWalMutation::RewriteCollection { .. }
                | CompactWalMutation::DropIndexes { .. }
                | CompactWalMutation::DropCollection { .. } => return Ok(None),
            }

            offset = payload_end;
        }

        add_counter(
            Component::Storage,
            "pendingWalEqualityCountScannedRecords",
            scanned_records,
        );
        add_counter(
            Component::Storage,
            "pendingWalEqualityCountRelevantRecords",
            relevant_records,
        );
        add_counter(
            Component::Storage,
            "pendingWalEqualityCountScannedBytes",
            wal_bytes,
        );

        Ok(Some(PendingWalEqualityCount {
            last_sequence,
            wal_records: scanned_records as usize,
            relevant_wal_records: relevant_records as usize,
            wal_bytes,
            count: match count {
                Some(count) => count,
                None => return Ok(None),
            },
        }))
    }
}

fn apply_compact_id_lookup_changes(
    found: &mut Option<(u64, Document)>,
    id: &Bson,
    changes: Vec<CompactCollectionChange>,
    inserts: Vec<CompactCollectionRecord>,
    updates: Vec<CompactCollectionRecord>,
    deletes: Vec<u64>,
) -> Result<()> {
    if !changes.is_empty() {
        for change in changes {
            match change {
                CompactCollectionChange::Insert(record)
                | CompactCollectionChange::Update(record) => {
                    apply_compact_id_lookup_record(found, id, record)?;
                }
                CompactCollectionChange::Delete(record_id) => {
                    apply_compact_id_lookup_delete(found, record_id);
                }
            }
        }
        return Ok(());
    }

    if found.is_none() {
        for record in inserts {
            apply_compact_id_lookup_record(found, id, record)?;
            if found.is_some() {
                break;
            }
        }
    }

    for record in updates {
        apply_compact_id_lookup_record(found, id, record)?;
    }
    for record_id in deletes {
        apply_compact_id_lookup_delete(found, record_id);
    }
    Ok(())
}

fn apply_compact_id_lookup_record(
    found: &mut Option<(u64, Document)>,
    id: &Bson,
    record: CompactCollectionRecord,
) -> Result<()> {
    let document = decode_document_bytes(&record.document)?;
    if document.get("_id") == Some(id) {
        *found = Some((record.record_id, document));
    }
    Ok(())
}

fn compact_record_field_eq(
    record: &CompactCollectionRecord,
    field: &str,
    value: &Bson,
) -> Result<bool> {
    if let Some(matches) = top_level_string_field_eq(&record.document, field, value) {
        return Ok(matches);
    }
    let document = decode_document_bytes(&record.document)?;
    Ok(mqlite_bson::lookup_path_owned(&document, field).as_ref() == Some(value))
}

fn index_metadata_value_count(
    indexes: &[WalFrameIndexMetadata],
    field: &str,
    value: &Bson,
) -> Result<Option<usize>> {
    for index in indexes {
        let key = decode_document_bytes(&index.key)?;
        if key.len() != 1 || !key.contains_key(field) {
            continue;
        }
        for frequency in &index.value_frequencies {
            if frequency.field != field {
                continue;
            }
            let encoded = decode_document_bytes(&frequency.value)?;
            if encoded.get("v") == Some(value) {
                return Ok(Some(frequency.count));
            }
        }
    }
    Ok(None)
}

fn top_level_string_field_eq(document: &[u8], field: &str, value: &Bson) -> Option<bool> {
    let Bson::String(expected) = value else {
        return None;
    };
    if field.contains('.') || document.len() < 5 {
        return None;
    }
    let document_len = i32::from_le_bytes(document.get(0..4)?.try_into().ok()?) as usize;
    if document_len > document.len() || document_len < 5 {
        return None;
    }

    let mut offset = 4_usize;
    while offset < document_len.saturating_sub(1) {
        let element_type = *document.get(offset)?;
        offset += 1;
        let key_start = offset;
        while offset < document_len {
            if *document.get(offset)? == 0 {
                break;
            }
            offset += 1;
        }
        if offset >= document_len {
            return None;
        }
        let key = std::str::from_utf8(document.get(key_start..offset)?).ok()?;
        offset += 1;

        if element_type == 0x02 {
            let string_len =
                i32::from_le_bytes(document.get(offset..offset + 4)?.try_into().ok()?) as usize;
            let value_start = offset + 4;
            let value_end = value_start.checked_add(string_len.checked_sub(1)?)?;
            if value_end >= document_len {
                return None;
            }
            if key == field {
                let actual = std::str::from_utf8(document.get(value_start..value_end)?).ok()?;
                return Some(actual == expected);
            }
            offset = value_end + 1;
            continue;
        }

        if key == field {
            return Some(false);
        }
        offset = skip_bson_element_value(document, offset, document_len, element_type)?;
    }
    Some(false)
}

fn skip_bson_element_value(
    document: &[u8],
    offset: usize,
    document_len: usize,
    element_type: u8,
) -> Option<usize> {
    let next = match element_type {
        0x01 | 0x09 | 0x11 | 0x12 => offset.checked_add(8)?,
        0x02 | 0x0d | 0x0e => {
            let len =
                i32::from_le_bytes(document.get(offset..offset + 4)?.try_into().ok()?) as usize;
            offset.checked_add(4)?.checked_add(len)?
        }
        0x03 | 0x04 => {
            let len =
                i32::from_le_bytes(document.get(offset..offset + 4)?.try_into().ok()?) as usize;
            offset.checked_add(len)?
        }
        0x05 => {
            let len =
                i32::from_le_bytes(document.get(offset..offset + 4)?.try_into().ok()?) as usize;
            offset.checked_add(5)?.checked_add(len)?
        }
        0x07 => offset.checked_add(12)?,
        0x08 => offset.checked_add(1)?,
        0x0a => offset,
        0x10 => offset.checked_add(4)?,
        _ => return None,
    };
    (next <= document_len).then_some(next)
}

fn apply_compact_id_lookup_delete(found: &mut Option<(u64, Document)>, record_id: u64) {
    if found
        .as_ref()
        .is_some_and(|(found_record_id, _)| *found_record_id == record_id)
    {
        *found = None;
    }
}

fn find_page_backed_document_by_id(
    view: &dyn CollectionReadView,
    id: &Bson,
) -> Result<Option<(u64, Document)>> {
    let Some(index) = view.index("_id_") else {
        return Ok(None);
    };
    let key = doc! { "_id": id.clone() };
    let bounds = IndexBounds {
        lower: Some(IndexBound {
            key: key.clone(),
            inclusive: true,
        }),
        upper: Some(IndexBound {
            key,
            inclusive: true,
        }),
    };
    let Some(entry) = index.scan_entries(&bounds)?.into_iter().next() else {
        return Ok(None);
    };
    view.record_document(entry.record_id)
        .map(|document| document.map(|document| (entry.record_id, document)))
}

fn wal_metadata_targets_namespace(
    mutation: &WalFrameMetadataMutation,
    database: &str,
    collection: &str,
) -> bool {
    match mutation {
        WalFrameMetadataMutation::ReplaceCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | WalFrameMetadataMutation::RewriteCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | WalFrameMetadataMutation::ApplyCollectionChanges {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | WalFrameMetadataMutation::CreateIndexes {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | WalFrameMetadataMutation::DropIndexes {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | WalFrameMetadataMutation::DropCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        } => mutation_database == database && mutation_collection == collection,
    }
}

fn wal_metadata_preserves_id_lookup_after_found(
    mutation: &WalFrameMetadataMutation,
    database: &str,
    collection: &str,
) -> bool {
    if !wal_metadata_targets_namespace(mutation, database, collection) {
        return true;
    }

    match mutation {
        WalFrameMetadataMutation::ApplyCollectionChanges { changes, .. } => {
            changes.updates == 0 && changes.deletes == 0
        }
        WalFrameMetadataMutation::CreateIndexes { .. }
        | WalFrameMetadataMutation::DropIndexes { .. } => true,
        WalFrameMetadataMutation::ReplaceCollection { .. }
        | WalFrameMetadataMutation::RewriteCollection { .. }
        | WalFrameMetadataMutation::DropCollection { .. } => false,
    }
}

impl DatabaseFile {
    pub fn read_plan_cache_entries(path: impl AsRef<Path>) -> Result<Vec<PersistedPlanCacheEntry>> {
        let _span = span(Component::Storage, "read_plan_cache_entries");
        v2_engine::load_plan_cache_entries_only(path)
    }

    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let _span = span(Component::Storage, "open_or_create");
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

    pub fn wal_backlog_bytes(&self) -> u64 {
        self.wal_bytes_since_checkpoint
    }

    pub fn set_persisted_plan_cache_entries(&mut self, mut entries: Vec<PersistedPlanCacheEntry>) {
        entries.sort();
        entries.dedup();
        self.state.plan_cache_entries = entries;
    }

    pub fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64> {
        let _span = span(Component::Storage, "commit_mutation");
        let sequence = self.commit_mutation_unflushed(mutation)?;
        self.sync_pending_wal()?;
        Ok(sequence)
    }

    pub fn commit_mutation_unflushed(&mut self, mutation: WalMutation) -> Result<u64> {
        let _span = span(Component::Storage, "commit_mutation_unflushed");
        let sequence = self.state.last_applied_sequence + 1;
        let validation_plan = validate_mutation(&self.state, &self.validation_state, &mutation)?;
        let wal_metadata =
            wal_metadata_from_validation_plan(sequence, &mutation, &validation_plan)?;

        let appended_bytes = append_wal_entry(
            &mut self.file,
            self.wal_end_offset,
            sequence,
            &mutation,
            wal_metadata.as_ref(),
            false,
        )?;

        mark_mutation_dirty(
            &mut self.dirty_collections,
            &mut self.change_events_dirty,
            &mutation,
        );
        let validation_plan = apply_owned_mutation_with_validation_plan(
            &mut self.state,
            sequence,
            mutation,
            validation_plan,
        )?;
        self.validation_state
            .apply_plan(&self.state.catalog, validation_plan)?;
        self.wal_end_offset += appended_bytes;
        self.wal_records_since_checkpoint += 1;
        self.wal_bytes_since_checkpoint += appended_bytes;
        self.truncated_wal_tail = false;
        add_counter(Component::Storage, "walAppendedRecords", 1);
        add_counter(Component::Storage, "walAppendedBytes", appended_bytes);
        Ok(sequence)
    }

    pub fn sync_pending_wal(&mut self) -> Result<u64> {
        let _span = span(Component::Storage, "sync_pending_wal");
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
        let _span = span(Component::Storage, "foreground_checkpoint");
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
        let change_events_dirty = self.change_events_dirty;
        let plan_cache_dirty = self.checkpoint_plan_cache_entries != self.state.plan_cache_entries;
        let pending = PendingConcurrentCheckpoint {
            sequence: state.last_applied_sequence,
            dirty_collections: Arc::clone(&dirty_collections),
            change_events_dirty,
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
            dirty_collections,
            change_events_dirty,
            plan_cache_dirty,
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
        self.wal_end_offset = self.active_superblock.wal_end_offset;
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
        let _span = span(Component::Storage, "inspect");
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
        let _span = span(Component::Storage, "info");
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
        let _span = span(Component::Storage, "verify");
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
        let _span = span(Component::Storage, "initialize_file");
        v2_checkpoint::initialize_empty_file(&mut self.file)?;
        self.write_checkpoint()?;
        self.reload_from_disk()
    }

    fn reload_from_disk(&mut self) -> Result<()> {
        let _span = span(Component::Storage, "reload_from_disk");
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
        let _span = span(Component::Storage, "write_checkpoint");
        self.state.file_format_version = v2_layout::FILE_FORMAT_VERSION;
        self.state.last_checkpoint_unix_ms = current_unix_ms();
        let plan_cache_dirty = self.checkpoint_plan_cache_entries != self.state.plan_cache_entries;
        add_counter(
            Component::Storage,
            "foregroundCheckpointDirtyCollections",
            self.dirty_collections.len() as u64,
        );
        let completed = if self.valid_superblocks == 0 {
            v2_checkpoint::write_state_checkpoint_to_file(
                &mut self.file,
                &self.state,
                self.active_slot,
                self.active_superblock.generation,
            )?
        } else {
            add_counter(
                Component::Storage,
                "foregroundCheckpointPublishedSnapshots",
                1,
            );
            v2_checkpoint::publish_state_snapshot_to_file(
                &self.path,
                &mut self.file,
                &self.state,
                self.active_slot,
                self.active_superblock.generation,
                self.active_superblock.wal_start_offset,
                &self.dirty_collections,
                self.change_events_dirty,
                plan_cache_dirty,
            )?
        };
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

    fn wal_backlog_bytes(&self) -> u64 {
        DatabaseFile::wal_backlog_bytes(self)
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

        let completed = v2_checkpoint::publish_state_snapshot_to_file(
            &self.path,
            &mut file,
            &self.state,
            self.active_slot,
            self.active_generation,
            self.previous_wal_start_offset,
            &self.dirty_collections,
            self.change_events_dirty,
            self.plan_cache_dirty,
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
    let _span = span(Component::Storage, "load_v2_state");
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
        Self::from_shared_encoded_fields(
            token,
            cluster_time,
            wall_time,
            database,
            collection,
            operation_type,
            document_key.map(Arc::from),
            full_document.map(Arc::from),
            full_document_before_change.map(Arc::from),
            update_description.map(Arc::from),
            expanded,
            extra_fields,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_shared_encoded_fields(
        token: Vec<u8>,
        cluster_time: bson::Timestamp,
        wall_time: bson::DateTime,
        database: String,
        collection: Option<String>,
        operation_type: String,
        document_key: Option<Arc<[u8]>>,
        full_document: Option<Arc<[u8]>>,
        full_document_before_change: Option<Arc<[u8]>>,
        update_description: Option<Arc<[u8]>>,
        expanded: bool,
        extra_fields: Vec<u8>,
    ) -> Self {
        Self {
            token: Arc::from(token),
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
            extra_fields: Arc::from(extra_fields),
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
        self.document_key
            .as_deref()
            .map(decode_document_bytes)
            .transpose()
    }

    pub fn full_document_document(&self) -> Result<Option<bson::Document>> {
        self.full_document
            .as_deref()
            .map(decode_document_bytes)
            .transpose()
    }

    pub fn full_document_before_change_document(&self) -> Result<Option<bson::Document>> {
        self.full_document_before_change
            .as_deref()
            .map(decode_document_bytes)
            .transpose()
    }

    pub fn update_description_document(&self) -> Result<Option<bson::Document>> {
        self.update_description
            .as_deref()
            .map(decode_document_bytes)
            .transpose()
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

impl WalFrameMetadata {
    fn from_wal_mutation(sequence: u64, mutation: &WalMutation) -> Result<Self> {
        Ok(Self {
            sequence,
            mutation: WalFrameMetadataMutation::from_wal_mutation(mutation)?,
        })
    }
}

fn wal_metadata_from_validation_plan(
    sequence: u64,
    mutation: &WalMutation,
    validation_plan: &ValidationPlan,
) -> Result<Option<WalFrameMetadata>> {
    let (
        WalMutation::CreateIndexes {
            database,
            collection,
            create_options,
            change_events,
            ..
        },
        ValidationPlan::InstallCreatedIndexes { created, .. },
    ) = (mutation, validation_plan)
    else {
        return Ok(None);
    };

    Ok(Some(WalFrameMetadata {
        sequence,
        mutation: WalFrameMetadataMutation::CreateIndexes {
            database: database.clone(),
            collection: collection.clone(),
            creates_collection: create_options.is_some(),
            indexes: created
                .iter()
                .map(|index| WalFrameIndexMetadata::from_index_catalog(&index.name, index))
                .collect::<Result<Vec<_>>>()?,
            change_event_count: change_events.len(),
        },
    }))
}

impl WalFrameMetadataMutation {
    fn from_wal_mutation(mutation: &WalMutation) -> Result<Self> {
        Ok(match mutation {
            WalMutation::ReplaceCollection {
                database,
                collection,
                collection_state,
                change_events,
            } => Self::ReplaceCollection {
                database: database.clone(),
                collection: collection.clone(),
                collection_metadata: WalFrameCollectionMetadata::from_collection_catalog(
                    collection_state,
                )?,
                change_event_count: change_events.len(),
            },
            WalMutation::RewriteCollection {
                database,
                collection,
                changes,
                change_events,
                ..
            } => Self::RewriteCollection {
                database: database.clone(),
                collection: collection.clone(),
                changes: WalFrameCollectionChangesMetadata::from_collection_changes(changes),
                change_event_count: change_events.len(),
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
            } => {
                let resolved_changes =
                    resolved_collection_changes(changes, inserts, updates, deletes);
                Self::ApplyCollectionChanges {
                    database: database.clone(),
                    collection: collection.clone(),
                    creates_collection: create_options.is_some(),
                    changes: WalFrameCollectionChangesMetadata::from_collection_changes(
                        &resolved_changes,
                    ),
                    change_event_count: change_events.len(),
                }
            }
            WalMutation::CreateIndexes {
                database,
                collection,
                create_options,
                specs,
                change_events,
            } => Self::CreateIndexes {
                database: database.clone(),
                collection: collection.clone(),
                creates_collection: create_options.is_some(),
                indexes: specs
                    .iter()
                    .map(WalFrameIndexMetadata::from_index_spec)
                    .collect::<Result<Vec<_>>>()?,
                change_event_count: change_events.len(),
            },
            WalMutation::DropIndexes {
                database,
                collection,
                target,
                change_events,
            } => Self::DropIndexes {
                database: database.clone(),
                collection: collection.clone(),
                target: target.clone(),
                change_event_count: change_events.len(),
            },
            WalMutation::DropCollection {
                database,
                collection,
                change_events,
            } => Self::DropCollection {
                database: database.clone(),
                collection: collection.clone(),
                change_event_count: change_events.len(),
            },
        })
    }
}

impl WalFrameCollectionMetadata {
    fn from_collection_catalog(collection: &CollectionCatalog) -> Result<Self> {
        Ok(Self {
            document_count: collection.records.len(),
            document_bytes: collection
                .records
                .iter()
                .map(|record| {
                    record
                        .encoded_document_bytes()
                        .map(|bytes| bytes.len() as u64)
                        .map_err(anyhow::Error::from)
                })
                .sum::<Result<u64>>()?,
            indexes: collection
                .indexes
                .iter()
                .map(|(name, index)| WalFrameIndexMetadata::from_index_catalog(name, index))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl WalFrameIndexMetadata {
    fn from_index_catalog(name: &str, index: &IndexCatalog) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            key: encode_document_bytes(&index.key)?,
            unique: index.unique,
            expire_after_seconds: index.expire_after_seconds,
            entry_count: index.entry_count(),
            bytes: estimate_index_bytes_for_count(index.entry_count(), &index.key),
            value_frequencies: index_value_frequencies(index)?,
        })
    }

    fn from_index_spec(spec: &Document) -> Result<Self> {
        let key = spec.get_document("key")?.clone();
        let name = spec.get_str("name").unwrap_or("").to_string();
        Ok(Self {
            name,
            key: encode_document_bytes(&key)?,
            unique: spec.get_bool("unique").unwrap_or(false),
            expire_after_seconds: spec.get_i64("expireAfterSeconds").ok(),
            entry_count: 0,
            bytes: 0,
            value_frequencies: Vec::new(),
        })
    }
}

fn index_value_frequencies(index: &IndexCatalog) -> Result<Vec<WalFrameValueFrequency>> {
    let mut frequencies = Vec::new();
    for (field, values) in &index.stats.value_frequencies {
        for frequency in values {
            frequencies.push(WalFrameValueFrequency {
                field: field.clone(),
                value: encode_document_bytes(&doc! { "v": frequency.value.clone() })?,
                count: frequency.count,
            });
        }
    }
    Ok(frequencies)
}

impl WalFrameCollectionChangesMetadata {
    fn from_collection_changes(changes: &[CollectionChange]) -> Self {
        let mut metadata = Self::default();
        for change in changes {
            match change {
                CollectionChange::Insert(record) => {
                    metadata.inserts += 1;
                    metadata.insert_bytes += record
                        .encoded_document_bytes()
                        .ok()
                        .map(|bytes| bytes.len() as u64)
                        .unwrap_or(0);
                }
                CollectionChange::Update(record) => {
                    metadata.updates += 1;
                    metadata.update_bytes += record
                        .encoded_document_bytes()
                        .ok()
                        .map(|bytes| bytes.len() as u64)
                        .unwrap_or(0);
                }
                CollectionChange::Delete(_) => metadata.deletes += 1,
            }
        }
        metadata
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
            token: Arc::from(self.token),
            cluster_time: bson::Timestamp {
                time: self.cluster_time_time,
                increment: self.cluster_time_increment,
            },
            wall_time: bson::DateTime::from_millis(self.wall_time_millis),
            database: self.database,
            collection: self.collection,
            operation_type: self.operation_type,
            document_key: self.document_key.map(Arc::from),
            full_document: self.full_document.map(Arc::from),
            full_document_before_change: self.full_document_before_change.map(Arc::from),
            update_description: self.update_description.map(Arc::from),
            expanded: self.expanded,
            extra_fields: Arc::from(self.extra_fields),
        })
    }
}

impl<'a> EncodedPersistedChangeEvent<'a> {
    fn from_persisted_change_event(event: &'a PersistedChangeEvent) -> Self {
        Self {
            token: event.token.as_ref(),
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
            extra_fields: event.extra_fields.as_ref(),
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
    add_counter(Component::Storage, "zstdBlobsDecompressed", 1);
    add_counter(
        Component::Storage,
        "zstdBytesDecompressed",
        decoded.len() as u64,
    );
    Ok(Some(decoded))
}

fn maybe_decode_stored_blob<'a>(bytes: &'a [u8]) -> std::result::Result<Cow<'a, [u8]>, ()> {
    match maybe_decode_zstd_blob(bytes)? {
        Some(decoded) => Ok(Cow::Owned(decoded)),
        None => Ok(Cow::Borrowed(bytes)),
    }
}

fn encode_wal_entry(
    sequence: u64,
    mutation: &WalMutation,
    metadata: Option<&WalFrameMetadata>,
) -> Result<Vec<u8>> {
    let compact_wal_entry = EncodedWalEntry::from_wal_entry(sequence, mutation)?;
    let default_metadata;
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => {
            default_metadata = WalFrameMetadata::from_wal_mutation(sequence, mutation)?;
            &default_metadata
        }
    };
    let mut metadata_bytes = Vec::new();
    cbor_ser::into_writer(&metadata, &mut metadata_bytes)?;
    let mut entry_bytes = Vec::new();
    cbor_ser::into_writer(&compact_wal_entry, &mut entry_bytes)?;
    let metadata_len = u32::try_from(metadata_bytes.len())
        .map_err(|_| anyhow::anyhow!("WAL metadata is too large"))?;
    let entry_bytes = maybe_encode_zstd_blob(
        &entry_bytes,
        WAL_COMPRESSION_MIN_LEN,
        WAL_COMPRESSION_MIN_SAVINGS,
    )?;
    let mut bytes = Vec::with_capacity(12 + metadata_bytes.len() + entry_bytes.len());
    bytes.extend_from_slice(WAL_METADATA_PAYLOAD_MAGIC);
    bytes.extend_from_slice(&metadata_len.to_le_bytes());
    bytes.extend_from_slice(&metadata_bytes);
    bytes.extend_from_slice(&entry_bytes);
    Ok(bytes)
}

fn decode_wal_entry(bytes: &[u8]) -> Result<DecodedWalEntry> {
    let bytes = maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let compressed_payload = matches!(&bytes, Cow::Owned(_));
    let entry_bytes = split_wal_payload(bytes.as_ref())?.1;
    let entry_bytes =
        maybe_decode_stored_blob(entry_bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let decoded_len = entry_bytes.as_ref().len();
    let compressed = compressed_payload || matches!(&entry_bytes, Cow::Owned(_));
    let mut cursor = Cursor::new(entry_bytes);
    let compact_wal_entry: CompactWalEntry = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != cursor.get_ref().len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok(DecodedWalEntry {
        entry: compact_wal_entry.into_wal_entry()?,
        decoded_len,
        compressed,
    })
}

fn decode_compact_wal_entry(bytes: &[u8]) -> Result<CompactWalEntry> {
    let bytes = maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let entry_bytes = split_wal_payload(bytes.as_ref())?.1;
    let entry_bytes =
        maybe_decode_stored_blob(entry_bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let mut cursor = Cursor::new(entry_bytes);
    let compact_wal_entry: CompactWalEntry = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != cursor.get_ref().len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok(compact_wal_entry)
}

fn decode_compact_wal_entry_sequence(bytes: &[u8]) -> Result<u64> {
    let bytes = maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let entry_bytes = split_wal_payload(bytes.as_ref())?.1;
    let entry_bytes =
        maybe_decode_stored_blob(entry_bytes).map_err(|_| StorageError::InvalidWalFrame)?;
    let mut cursor = Cursor::new(entry_bytes);
    let compact_wal_entry: CompactWalEntrySequence = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != cursor.get_ref().len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok(compact_wal_entry.sequence)
}

fn decode_wal_frame_metadata(bytes: &[u8]) -> Result<Option<WalFrameMetadata>> {
    let bytes = if bytes.starts_with(WAL_METADATA_PAYLOAD_MAGIC) {
        Cow::Borrowed(bytes)
    } else {
        maybe_decode_stored_blob(bytes).map_err(|_| StorageError::InvalidWalFrame)?
    };
    let (Some(metadata_bytes), _) = split_wal_payload(bytes.as_ref())? else {
        return Ok(None);
    };
    let mut cursor = Cursor::new(metadata_bytes);
    let metadata = cbor_de::from_reader(&mut cursor)?;
    if cursor.position() != metadata_bytes.len() as u64 {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok(Some(metadata))
}

fn split_wal_payload(bytes: &[u8]) -> Result<(Option<&[u8]>, &[u8])> {
    if !bytes.starts_with(WAL_METADATA_PAYLOAD_MAGIC) {
        return Ok((None, bytes));
    }
    let Some(metadata_len_bytes) = bytes.get(8..12) else {
        return Err(StorageError::InvalidWalFrame.into());
    };
    let metadata_len = u32::from_le_bytes(metadata_len_bytes.try_into().expect("metadata len"));
    let metadata_start = 12;
    let metadata_end = metadata_start + metadata_len as usize;
    if metadata_end > bytes.len() {
        return Err(StorageError::InvalidWalFrame.into());
    }
    Ok((
        Some(&bytes[metadata_start..metadata_end]),
        &bytes[metadata_end..],
    ))
}

fn append_wal_entry(
    file: &mut File,
    frame_offset: u64,
    sequence: u64,
    mutation: &WalMutation,
    metadata: Option<&WalFrameMetadata>,
    sync: bool,
) -> Result<u64> {
    let payload = encode_wal_entry(sequence, mutation, metadata)?;
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
    let _span = span(Component::Storage, "replay_wal");
    let file_size = file.metadata()?.len();
    if start_offset > file_size {
        return Err(StorageError::Truncated.into());
    }

    let mut recovery = WalRecovery::default();
    let mut last_applied_sequence = state.last_applied_sequence;
    let mut offset = start_offset;
    let mut largest_payload_bytes = 0_u64;
    let mut largest_payload_kind = None;
    let mut decoded_bytes = 0_u64;
    let mut compressed_frames = 0_u64;
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

        let decoded = decode_wal_entry(&payload)?;
        if decoded.entry.sequence > last_applied_sequence {
            let mutation_stats = wal_mutation_replay_stats(&decoded.entry.mutation);
            let payload_len_u64 = payload_len as u64;
            let frame_bytes = WAL_HEADER_LEN as u64 + payload_len_u64;
            let kind = mutation_stats.kind.as_str();
            if payload_len_u64 > largest_payload_bytes {
                largest_payload_bytes = payload_len_u64;
                largest_payload_kind = Some(kind);
            }
            if decoded.compressed {
                compressed_frames += 1;
                add_counter(Component::Storage, "walReplayCompressedFrames", 1);
            }
            decoded_bytes += decoded.decoded_len as u64;
            mark_mutation_dirty(
                &mut recovery.dirty_collections,
                &mut recovery.change_events_dirty,
                &decoded.entry.mutation,
            );
            recovery.records += 1;
            add_counter(Component::Storage, "walReplayRecords", 1);
            add_counter(Component::Storage, "walReplayFrameBytes", frame_bytes);
            add_counter(Component::Storage, "walReplayPayloadBytes", payload_len_u64);
            add_counter(
                Component::Storage,
                "walReplayDecodedBytes",
                decoded.decoded_len as u64,
            );
            add_counter(Component::Storage, &format!("walReplayMutations.{kind}"), 1);
            add_counter(
                Component::Storage,
                &format!("walReplayPayloadBytes.{kind}"),
                payload_len_u64,
            );
            add_counter(
                Component::Storage,
                &format!("walReplayDecodedBytes.{kind}"),
                decoded.decoded_len as u64,
            );
            if mutation_stats.touched_documents > 0 {
                add_counter(
                    Component::Storage,
                    "walReplayTouchedDocuments",
                    mutation_stats.touched_documents,
                );
                add_counter(
                    Component::Storage,
                    &format!("walReplayTouchedDocuments.{kind}"),
                    mutation_stats.touched_documents,
                );
            }
            if mutation_stats.touched_document_bytes > 0 {
                add_counter(
                    Component::Storage,
                    "walReplayTouchedDocumentBytes",
                    mutation_stats.touched_document_bytes,
                );
                add_counter(
                    Component::Storage,
                    &format!("walReplayTouchedDocumentBytes.{kind}"),
                    mutation_stats.touched_document_bytes,
                );
            }
            if mutation_stats.change_events > 0 {
                add_counter(
                    Component::Storage,
                    "walReplayChangeEvents",
                    mutation_stats.change_events,
                );
                add_counter(
                    Component::Storage,
                    &format!("walReplayChangeEvents.{kind}"),
                    mutation_stats.change_events,
                );
            }
            if mutation_stats.index_specs > 0 {
                add_counter(
                    Component::Storage,
                    "walReplayIndexSpecs",
                    mutation_stats.index_specs,
                );
                add_counter(
                    Component::Storage,
                    &format!("walReplayIndexSpecs.{kind}"),
                    mutation_stats.index_specs,
                );
            }
            let applied_at = Instant::now();
            apply_mutation(state, decoded.entry.sequence, &decoded.entry.mutation)?;
            record_duration(
                Component::Storage,
                mutation_stats.kind.replay_apply_operation(),
                applied_at.elapsed(),
            );
            last_applied_sequence = decoded.entry.sequence;
            recovery.last_sequence = Some(decoded.entry.sequence);
        }

        offset = payload_end;
    }

    recovery.bytes = offset.saturating_sub(start_offset);
    set_metadata("walReplayDecodedBytes", decoded_bytes.to_string());
    set_metadata("walReplayCompressedFrames", compressed_frames.to_string());
    set_metadata(
        "walReplayLargestPayloadBytes",
        largest_payload_bytes.to_string(),
    );
    if let Some(kind) = largest_payload_kind {
        set_metadata("walReplayLargestPayloadMutation", kind);
    }
    Ok(recovery)
}

fn wal_mutation_replay_stats(mutation: &WalMutation) -> WalMutationReplayStats {
    match mutation {
        WalMutation::ReplaceCollection {
            collection_state,
            change_events,
            ..
        } => WalMutationReplayStats {
            kind: WalMutationKind::ReplaceCollection,
            touched_documents: collection_state.records.len() as u64,
            touched_document_bytes: collection_state
                .records
                .iter()
                .map(|record| {
                    record
                        .encoded_document_bytes()
                        .ok()
                        .map(|bytes| bytes.len() as u64)
                        .unwrap_or(0)
                })
                .sum(),
            change_events: change_events.len() as u64,
            index_specs: collection_state.indexes.len() as u64,
        },
        WalMutation::RewriteCollection {
            changes,
            change_events,
            ..
        } => collection_change_replay_stats(
            WalMutationKind::RewriteCollection,
            changes,
            change_events.len() as u64,
            0,
        ),
        WalMutation::ApplyCollectionChanges {
            changes,
            inserts,
            updates,
            deletes,
            change_events,
            ..
        } => {
            let resolved = resolved_collection_changes(changes, inserts, updates, deletes);
            collection_change_replay_stats(
                WalMutationKind::ApplyCollectionChanges,
                &resolved,
                change_events.len() as u64,
                0,
            )
        }
        WalMutation::CreateIndexes {
            specs,
            change_events,
            ..
        } => WalMutationReplayStats {
            kind: WalMutationKind::CreateIndexes,
            touched_documents: 0,
            touched_document_bytes: 0,
            change_events: change_events.len() as u64,
            index_specs: specs.len() as u64,
        },
        WalMutation::DropIndexes { change_events, .. } => WalMutationReplayStats {
            kind: WalMutationKind::DropIndexes,
            touched_documents: 0,
            touched_document_bytes: 0,
            change_events: change_events.len() as u64,
            index_specs: 1,
        },
        WalMutation::DropCollection { change_events, .. } => WalMutationReplayStats {
            kind: WalMutationKind::DropCollection,
            touched_documents: 0,
            touched_document_bytes: 0,
            change_events: change_events.len() as u64,
            index_specs: 0,
        },
    }
}

fn collection_change_replay_stats(
    kind: WalMutationKind,
    changes: &[CollectionChange],
    change_events: u64,
    index_specs: u64,
) -> WalMutationReplayStats {
    let mut touched_documents = 0_u64;
    let mut touched_document_bytes = 0_u64;
    for change in changes {
        touched_documents += 1;
        match change {
            CollectionChange::Insert(record) | CollectionChange::Update(record) => {
                touched_document_bytes += record
                    .encoded_document_bytes()
                    .ok()
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(0);
            }
            CollectionChange::Delete(_) => {}
        }
    }
    WalMutationReplayStats {
        kind,
        touched_documents,
        touched_document_bytes,
        change_events,
        index_specs,
    }
}

fn build_pending_collection_overlay_state(
    path: &Path,
    database: &str,
    collection: &str,
    durable_sequence: u64,
    last_checkpoint_unix_ms: u64,
) -> Result<PersistedState> {
    let _span = span(Component::Storage, "build_pending_collection_overlay_state");
    let mut catalog = Catalog::new();
    if let Some(collection_state) = v2_engine::open_collection_catalog(path, database, collection)?
    {
        catalog.replace_collection(database, collection, collection_state);
    }
    Ok(PersistedState {
        file_format_version: v2_layout::FILE_FORMAT_VERSION,
        last_applied_sequence: durable_sequence,
        last_checkpoint_unix_ms,
        catalog,
        change_events: Vec::new(),
        plan_cache_entries: Vec::new(),
    })
}

fn build_insert_only_delta(
    base: &dyn CollectionReadView,
    mutation: &WalMutation,
) -> Result<Option<CollectionCatalog>> {
    let mut delta = delta_collection_for_base(base)?;
    if try_apply_insert_only_delta(&mut delta, base, mutation)? {
        Ok(Some(delta))
    } else {
        Ok(None)
    }
}

fn delta_collection_for_base(base: &dyn CollectionReadView) -> Result<CollectionCatalog> {
    let mut delta = CollectionCatalog::new(Document::new());
    for index_name in base.index_names() {
        if index_name == "_id_" {
            continue;
        }
        let Some(index) = base.index(&index_name) else {
            continue;
        };
        apply_index_specs(
            &mut delta,
            &[doc! {
                "name": index.name(),
                "key": index.key_pattern().clone(),
            }],
        )
        .map_err(map_catalog_error)?;
    }
    Ok(delta)
}

fn try_apply_insert_only_delta(
    delta: &mut CollectionCatalog,
    base: &dyn CollectionReadView,
    mutation: &WalMutation,
) -> Result<bool> {
    let WalMutation::ApplyCollectionChanges {
        create_options,
        changes,
        inserts,
        updates,
        deletes,
        ..
    } = mutation
    else {
        return Ok(false);
    };
    if create_options.is_some() || !updates.is_empty() || !deletes.is_empty() {
        return Ok(false);
    }
    let changes = resolved_collection_changes(changes, inserts, updates, deletes);
    if changes
        .iter()
        .any(|change| !matches!(change, CollectionChange::Insert(_)))
    {
        return Ok(false);
    }
    let mut inserted_records = 0_u64;
    for change in changes {
        let CollectionChange::Insert(record) = change else {
            unreachable!("checked insert-only changes")
        };
        if base.record_document(record.record_id)?.is_some() {
            return Ok(false);
        }
        delta
            .insert_record(record.clone())
            .map_err(map_catalog_error)?;
        inserted_records += 1;
    }
    add_counter(
        Component::Storage,
        "pendingWalOverlayDeltaInserts",
        inserted_records,
    );
    Ok(true)
}

fn merge_delta_into_overlay_state(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    delta: CollectionCatalog,
) -> Result<()> {
    for record in delta.records {
        apply_collection_changes(
            state,
            database,
            collection,
            None,
            &[CollectionChange::Insert(record)],
        )?;
    }
    Ok(())
}

fn compact_mutation_targets_namespace(
    mutation: &CompactWalMutation,
    database: &str,
    collection: &str,
) -> bool {
    match mutation {
        CompactWalMutation::ReplaceCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | CompactWalMutation::RewriteCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | CompactWalMutation::ApplyCollectionChanges {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | CompactWalMutation::CreateIndexes {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | CompactWalMutation::DropIndexes {
            database: mutation_database,
            collection: mutation_collection,
            ..
        }
        | CompactWalMutation::DropCollection {
            database: mutation_database,
            collection: mutation_collection,
            ..
        } => mutation_database == database && mutation_collection == collection,
    }
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

fn apply_owned_mutation_with_validation_plan(
    state: &mut PersistedState,
    sequence: u64,
    owned_mutation: WalMutation,
    validation_plan: ValidationPlan,
) -> Result<ValidationPlan> {
    match (owned_mutation, validation_plan) {
        (
            WalMutation::CreateIndexes {
                database,
                collection,
                change_events,
                ..
            },
            ValidationPlan::InstallCreatedIndexes {
                database: plan_database,
                collection: plan_collection,
                create_options,
                created,
            },
        ) => {
            apply_created_indexes(
                state,
                &plan_database,
                &plan_collection,
                create_options.as_ref(),
                created,
            )?;
            state.change_events.extend(change_events);
            state.last_applied_sequence = sequence;
            Ok(ValidationPlan::RebuildCollection {
                database,
                collection,
            })
        }
        (owned_mutation, validation_plan) => {
            apply_owned_mutation(state, sequence, owned_mutation)?;
            Ok(validation_plan)
        }
    }
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
            let created = validate_create_indexes(
                state,
                database,
                collection,
                create_options.as_ref(),
                specs,
            )?;
            ValidationPlan::InstallCreatedIndexes {
                database: database.clone(),
                collection: collection.clone(),
                create_options: create_options.clone(),
                created,
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

fn apply_created_indexes(
    state: &mut PersistedState,
    database: &str,
    collection: &str,
    create_options: Option<&bson::Document>,
    created: Vec<IndexCatalog>,
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
    for index in created {
        collection_state.indexes.insert(index.name.clone(), index);
    }
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
    InstallCreatedIndexes {
        database: String,
        collection: String,
        create_options: Option<Document>,
        created: Vec<IndexCatalog>,
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
            }
            | ValidationPlan::InstallCreatedIndexes {
                database,
                collection,
                ..
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

fn apply_wal_frame_metadata(
    metadata: &mut WalCatalogMetadata,
    frame: WalFrameMetadata,
) -> Result<()> {
    match frame.mutation {
        WalFrameMetadataMutation::ReplaceCollection {
            database,
            collection,
            collection_metadata,
            change_event_count,
        } => {
            metadata
                .databases
                .entry(database)
                .or_default()
                .collections
                .insert(
                    collection,
                    wal_collection_metadata_from_frame(collection_metadata)?,
                );
            metadata.change_event_count += change_event_count;
        }
        WalFrameMetadataMutation::RewriteCollection {
            database,
            collection,
            changes,
            change_event_count,
        } => {
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            apply_wal_collection_change_metadata(collection_metadata, &changes);
            metadata.change_event_count += change_event_count;
        }
        WalFrameMetadataMutation::ApplyCollectionChanges {
            database,
            collection,
            creates_collection,
            changes,
            change_event_count,
        } => {
            if creates_collection {
                let _ = ensure_wal_collection_metadata(metadata, &database, &collection);
            }
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            apply_wal_collection_change_metadata(collection_metadata, &changes);
            metadata.change_event_count += change_event_count;
        }
        WalFrameMetadataMutation::CreateIndexes {
            database,
            collection,
            creates_collection,
            indexes,
            change_event_count,
        } => {
            if creates_collection {
                let _ = ensure_wal_collection_metadata(metadata, &database, &collection);
            }
            let collection_metadata =
                ensure_wal_collection_metadata(metadata, &database, &collection);
            for index in indexes {
                let key = decode_document_bytes(&index.key)?;
                let bytes =
                    estimate_index_bytes_for_count(collection_metadata.document_count, &key);
                collection_metadata.indexes.insert(
                    index.name,
                    WalIndexMetadata {
                        key,
                        unique: index.unique,
                        expire_after_seconds: index.expire_after_seconds,
                        entry_count: collection_metadata.document_count,
                        bytes,
                    },
                );
            }
            metadata.change_event_count += change_event_count;
        }
        WalFrameMetadataMutation::DropIndexes {
            database,
            collection,
            target,
            change_event_count,
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
            metadata.change_event_count += change_event_count;
        }
        WalFrameMetadataMutation::DropCollection {
            database,
            collection,
            change_event_count,
        } => {
            if let Some(database_metadata) = metadata.databases.get_mut(&database) {
                database_metadata.collections.remove(&collection);
                if database_metadata.collections.is_empty() {
                    metadata.databases.remove(&database);
                }
            }
            metadata.change_event_count += change_event_count;
        }
    }
    Ok(())
}

fn wal_collection_metadata_from_frame(
    collection: WalFrameCollectionMetadata,
) -> Result<WalCollectionMetadata> {
    let indexes = collection
        .indexes
        .into_iter()
        .map(|index| {
            Ok((
                index.name,
                WalIndexMetadata {
                    key: decode_document_bytes(&index.key)?,
                    unique: index.unique,
                    expire_after_seconds: index.expire_after_seconds,
                    entry_count: index.entry_count,
                    bytes: index.bytes,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(WalCollectionMetadata {
        indexes,
        record_sizes: HashMap::new(),
        document_count: collection.document_count,
        document_bytes: collection.document_bytes,
    })
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

fn apply_wal_collection_change_metadata(
    collection: &mut WalCollectionMetadata,
    changes: &WalFrameCollectionChangesMetadata,
) {
    let previous_document_count = collection.document_count;
    let previous_average = average_document_bytes(collection);
    collection.document_count = collection
        .document_count
        .saturating_add(changes.inserts)
        .saturating_sub(changes.deletes);
    collection.document_bytes = collection
        .document_bytes
        .saturating_add(changes.insert_bytes)
        .saturating_add(changes.update_bytes)
        .saturating_sub(previous_average.saturating_mul(changes.updates as u64))
        .saturating_sub(previous_average.saturating_mul(changes.deletes as u64));

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
    let _span = span(Component::Storage, "apply_wal_catalog_metadata");
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

        if let Some(frame_metadata) = decode_wal_frame_metadata(&payload)? {
            apply_wal_frame_metadata(metadata, frame_metadata)?;
            add_counter(Component::Storage, "walMetadataFastRecords", 1);
        } else {
            let entry = decode_compact_wal_entry(&payload)?;
            apply_wal_metadata_mutation(metadata, entry.mutation)?;
        }
        wal.records += 1;
        add_counter(Component::Storage, "walMetadataRecords", 1);
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
        sync::Arc,
    };

    use bson::{Bson, DateTime, Document, Timestamp, doc};
    use mqlite_catalog::{
        CollectionCatalog, CollectionRecord, IndexBound, IndexBounds, apply_index_specs,
    };
    use mqlite_debug::{Component, install, session};
    use tempfile::tempdir;

    use super::{
        CollectionChange, DATA_START_OFFSET, DatabaseFile, FILE_FORMAT_VERSION, PAGE_SIZE,
        PersistedChangeEvent, PersistedPlanCacheChoice, PersistedPlanCacheEntry, StartupMetadata,
        VerifyReport, WAL_FRAME_MAGIC, WAL_HEADER_LEN, WAL_METADATA_PAYLOAD_MAGIC, WalMutation,
        ZSTD_BLOB_MAGIC, split_wal_payload,
    };
    use crate::StorageEngine;
    use crate::v2::{
        engine as v2_engine,
        page::{NamespaceInternalPage, NamespaceLeafPage, page_kind_unchecked},
        pager::Pager as V2Pager,
    };

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

    fn namespace_meta_page_ids(path: &std::path::Path) -> BTreeMap<String, u64> {
        let pager = V2Pager::open(path).expect("open pager");
        let mut page_id = pager.active_superblock().roots.namespace_root_page_id;
        while let Some(current_page_id) = page_id {
            let page = pager.read_page_bytes(current_page_id).expect("read page");
            match page_kind_unchecked(page.as_ref()).expect("page kind") {
                crate::v2::layout::PageKind::NamespaceLeaf => break,
                crate::v2::layout::PageKind::NamespaceInternal => {
                    page_id = Some(
                        NamespaceInternalPage::decode(page.as_ref())
                            .expect("decode internal")
                            .first_child_page_id,
                    );
                }
                other => panic!("expected namespace page, found {other:?}"),
            }
        }

        let mut entries = BTreeMap::new();
        let mut next_page_id = page_id;
        while let Some(current_page_id) = next_page_id {
            let leaf = NamespaceLeafPage::decode(
                pager
                    .read_page_bytes(current_page_id)
                    .expect("read leaf")
                    .as_ref(),
            )
            .expect("decode leaf");
            next_page_id = leaf.next_page_id;
            for entry in leaf.entries {
                entries.insert(entry.name, entry.target_page_id);
            }
        }
        entries
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

    #[test]
    fn persisted_change_event_clone_shares_encoded_bytes() {
        let event = sample_change_event(7, "insert");
        let cloned = event.clone();

        assert!(Arc::ptr_eq(&event.token, &cloned.token));
        assert!(Arc::ptr_eq(
            event.document_key.as_ref().expect("document key"),
            cloned.document_key.as_ref().expect("cloned document key"),
        ));
        assert!(Arc::ptr_eq(
            event.full_document.as_ref().expect("full document"),
            cloned.full_document.as_ref().expect("cloned full document"),
        ));
        assert!(Arc::ptr_eq(&event.extra_fields, &cloned.extra_fields));
        assert_eq!(
            cloned
                .full_document_document()
                .expect("decode full document"),
            Some(doc! { "_id": 7_i64, "qty": 7_i64 })
        );
    }

    fn counter_value(
        report: &mqlite_debug::DebugReport,
        component: Component,
        name: &str,
    ) -> Option<u64> {
        report
            .counters
            .iter()
            .find(|counter| counter.component == component && counter.name == name)
            .map(|counter| counter.value)
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
    fn replay_wal_reports_mutation_breakdown_to_debug_session() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-replay-debug.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: Some(Document::new()),
                    changes: vec![
                        CollectionChange::Insert(CollectionRecord::new(
                            1,
                            doc! { "_id": 1, "sku": "alpha" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            2,
                            doc! { "_id": 2, "sku": "beta" },
                        )),
                    ],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: vec![sample_change_event(1, "insert")],
                })
                .expect("commit changes");
            database
                .commit_mutation(WalMutation::CreateIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    specs: vec![doc! { "key": { "sku": 1_i32 }, "name": "sku_1" }],
                    change_events: vec![sample_change_event(2, "createIndexes")],
                })
                .expect("commit indexes");
        }

        let replay_session = session("wal-replay-debug");
        let _install = install(&replay_session);
        let database = DatabaseFile::open_or_create(&path).expect("reopen database");
        drop(database);

        let report = replay_session.report();
        assert_eq!(
            counter_value(&report, Component::Storage, "walReplayRecords"),
            Some(2)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayMutations.applyCollectionChanges",
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayMutations.createIndexes"
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayTouchedDocuments.applyCollectionChanges",
            ),
            Some(2)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayIndexSpecs.createIndexes",
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayChangeEvents.applyCollectionChanges",
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "walReplayChangeEvents.createIndexes",
            ),
            Some(1)
        );
        assert_eq!(
            report
                .spans
                .iter()
                .any(|span| span.operation == "replay_apply_collection_changes"),
            true
        );
        assert_eq!(
            report
                .spans
                .iter()
                .any(|span| span.operation == "replay_apply_create_indexes"),
            true
        );
        assert_eq!(
            report
                .metadata
                .get("walReplayLargestPayloadMutation")
                .map(String::as_str),
            Some("createIndexes")
        );
    }

    #[test]
    fn pending_wal_collection_read_view_replays_only_target_namespace() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("pending-wal-overlay.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
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
                    change_events: Vec::new(),
                })
                .expect("commit base widgets");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "others".to_string(),
                    create_options: Some(Document::new()),
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        9,
                        doc! { "_id": 9, "sku": "other-base" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit base others");
            database.checkpoint().expect("checkpoint");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        2,
                        doc! { "_id": 2, "sku": "beta" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit overlay widgets");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "others".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        10,
                        doc! { "_id": 10, "sku": "other-overlay" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit overlay others");
        }

        let overlay =
            DatabaseFile::open_pending_wal_collection_read_view(&path, "app", "widgets", u64::MAX)
                .expect("open overlay")
                .expect("overlay result");

        assert!(overlay.used_overlay);
        assert_eq!(overlay.wal_records, 2);
        assert_eq!(overlay.relevant_wal_records, 1);
        assert_eq!(overlay.last_sequence, 4);

        let records = overlay
            .view
            .expect("overlay view")
            .scan_records()
            .expect("scan records");
        assert_eq!(records.len(), 2);
        let ids = records
            .iter()
            .map(|record| record.document.get_i32("_id").expect("_id"))
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn insert_only_pending_wal_overlay_avoids_full_collection_hydration() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("pending-wal-delta-overlay.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(Document::new());
            for record_id in 1..=100 {
                insert_record(
                    &mut collection,
                    record_id,
                    doc! {
                        "_id": record_id as i32,
                        "sku": format!("sku-{record_id:03}"),
                    },
                );
            }
            apply_index_specs(
                &mut collection,
                &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
            )
            .expect("create sku index");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                    change_events: Vec::new(),
                })
                .expect("commit checkpointed collection");
            database.checkpoint().expect("checkpoint");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        101,
                        doc! { "_id": 101, "sku": "sku-101" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit pending insert");
        }

        let debug_session = session("pending-wal-delta-overlay");
        let _install = install(&debug_session);
        let overlay =
            DatabaseFile::open_pending_wal_collection_read_view(&path, "app", "widgets", u64::MAX)
                .expect("open overlay")
                .expect("overlay result");

        assert!(overlay.used_overlay);
        assert_eq!(overlay.wal_records, 1);
        assert_eq!(overlay.relevant_wal_records, 1);
        let view = overlay.view.expect("overlay view");
        assert_eq!(
            view.record_document(1)
                .expect("read base record")
                .expect("base record")
                .get_str("sku")
                .expect("base sku"),
            "sku-001"
        );
        assert_eq!(
            view.record_document(101)
                .expect("read delta record")
                .expect("delta record")
                .get_str("sku")
                .expect("delta sku"),
            "sku-101"
        );

        let sku_index = view.index("sku_1").expect("sku index");
        let base_entries = sku_index
            .scan_entries(&exact_sku_bounds("sku-001"))
            .expect("scan base sku");
        assert_eq!(
            base_entries
                .iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let delta_entries = sku_index
            .scan_entries(&exact_sku_bounds("sku-101"))
            .expect("scan delta sku");
        assert_eq!(
            delta_entries
                .iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![101]
        );

        let report = debug_session.report();
        assert_eq!(
            counter_value(&report, Component::Storage, "pendingWalOverlayDeltaInserts",),
            Some(1)
        );
        assert_eq!(
            counter_value(&report, Component::Storage, "recordTreeScanRecords"),
            None
        );
    }

    #[test]
    fn create_indexes_pending_wal_overlay_keeps_base_records_page_backed() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join("pending-wal-create-indexes-overlay.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(Document::new());
            for record_id in 1..=3 {
                insert_record(
                    &mut collection,
                    record_id,
                    doc! {
                        "_id": record_id as i32,
                        "sku": format!("sku-{record_id}"),
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
                .expect("commit checkpointed collection");
            database.checkpoint().expect("checkpoint");
            database
                .commit_mutation(WalMutation::CreateIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    specs: vec![doc! { "key": { "sku": 1 }, "name": "sku_1" }],
                    change_events: Vec::new(),
                })
                .expect("commit pending index");
        }

        let overlay =
            DatabaseFile::open_pending_wal_collection_read_view(&path, "app", "widgets", u64::MAX)
                .expect("open overlay")
                .expect("overlay result");

        assert!(overlay.used_overlay);
        assert_eq!(overlay.wal_records, 1);
        assert_eq!(overlay.relevant_wal_records, 1);
        let view = overlay.view.expect("overlay view");
        assert_eq!(view.index("_id_").expect("_id index").entry_count(), 3);
        assert_eq!(
            view.record_document(2)
                .expect("record lookup")
                .expect("record")
                .get_str("sku")
                .expect("sku"),
            "sku-2"
        );
        assert!(
            view.index("sku_1").is_none(),
            "metadata-only createIndexes overlay must not expose an index whose entries are not page-backed"
        );
    }

    #[test]
    fn pending_wal_id_lookup_finds_insert_without_collection_overlay() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("pending-wal-id-lookup.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database.checkpoint().expect("checkpoint empty file");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: Some(Document::new()),
                    changes: vec![
                        CollectionChange::Insert(CollectionRecord::new(
                            1,
                            doc! { "_id": 1_i64, "sku": "alpha" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            2,
                            doc! { "_id": 2_i64, "sku": "beta" },
                        )),
                    ],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit pending inserts");
            database
                .commit_mutation(WalMutation::CreateIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    specs: vec![doc! { "key": { "sku": 1 }, "name": "sku_1" }],
                    change_events: Vec::new(),
                })
                .expect("commit pending index");
        }

        let lookup = DatabaseFile::find_pending_wal_document_by_id(
            &path,
            "app",
            "widgets",
            &Bson::Int64(2),
            u64::MAX,
        )
        .expect("lookup")
        .expect("supported lookup");
        let document = lookup.document.expect("document");
        assert_eq!(document.get_str("sku").expect("sku"), "beta");
    }

    #[test]
    fn pending_wal_equality_count_streams_insert_batches() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("pending-wal-equality-count.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database.checkpoint().expect("checkpoint empty file");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: Some(Document::new()),
                    changes: vec![
                        CollectionChange::Insert(CollectionRecord::new(
                            1,
                            doc! { "_id": 1_i64, "ticket": "z300" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            2,
                            doc! { "_id": 2_i64, "ticket": "x100" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            3,
                            doc! { "_id": 3_i64, "ticket": "z300" },
                        )),
                    ],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit pending inserts");
            database
                .commit_mutation(WalMutation::CreateIndexes {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    specs: vec![doc! { "key": { "ticket": 1 }, "name": "ticket_1" }],
                    change_events: Vec::new(),
                })
                .expect("commit pending index");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        4,
                        doc! { "_id": 4_i64, "ticket": "z300" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("commit later pending insert");
        }

        let count = DatabaseFile::count_pending_wal_field_eq(
            &path,
            "app",
            "widgets",
            "ticket",
            &Bson::String("z300".to_string()),
            u64::MAX,
        )
        .expect("count")
        .expect("supported count");
        assert_eq!(count.count, 3);
        assert_eq!(count.wal_records, 3);
        assert_eq!(count.relevant_wal_records, 3);
    }

    fn exact_sku_bounds(sku: &str) -> IndexBounds {
        let key = doc! { "sku": sku };
        IndexBounds {
            lower: Some(IndexBound {
                key: key.clone(),
                inclusive: true,
            }),
            upper: Some(IndexBound {
                key,
                inclusive: true,
            }),
        }
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
    fn info_folds_new_wal_frame_metadata_without_full_mutation_decode() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("info-v2-wal-metadata.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: Some(Document::new()),
                    changes: vec![
                        CollectionChange::Insert(CollectionRecord::new(
                            1,
                            doc! { "_id": 1_i64, "sku": "alpha" },
                        )),
                        CollectionChange::Insert(CollectionRecord::new(
                            2,
                            doc! { "_id": 2_i64, "sku": "beta" },
                        )),
                    ],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: vec![sample_change_event(1, "insert")],
                })
                .expect("append wal");
        }

        let debug_session = session("wal-frame-metadata-info");
        let report = {
            let _install = install(&debug_session);
            DatabaseFile::info(&path).expect("info")
        };
        assert_eq!(report.summary.record_count, 2);
        assert_eq!(report.summary.change_event_count, 1);
        assert_eq!(report.wal_since_checkpoint.record_count, 1);

        let debug_report = debug_session.report();
        assert_eq!(
            counter_value(&debug_report, Component::Storage, "walMetadataRecords"),
            Some(1)
        );
        assert_eq!(
            counter_value(&debug_report, Component::Storage, "walMetadataFastRecords"),
            Some(1)
        );
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
    fn replays_wal_frames_with_compressed_mutation_payload() {
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
        assert!(payload.starts_with(WAL_METADATA_PAYLOAD_MAGIC));
        let (_, entry_payload) = split_wal_payload(&payload).expect("split WAL payload");
        assert!(entry_payload.starts_with(ZSTD_BLOB_MAGIC));

        let debug_session = session("compressed-wal-entry-replay");
        let _install = install(&debug_session);
        let reopened = DatabaseFile::open_or_create(&path).expect("reopen");
        let collection = reopened
            .catalog()
            .get_collection("app", "widgets")
            .expect("collection");
        assert_eq!(collection.records.len(), 20);
        let report = debug_session.report();
        assert_eq!(
            counter_value(&report, Component::Storage, "zstdBlobsDecompressed"),
            Some(1)
        );
        assert!(
            counter_value(&report, Component::Storage, "zstdBytesDecompressed").unwrap_or(0) > 0
        );
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
    fn concurrent_checkpoint_advances_wal_append_offset_after_finish() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join("concurrent-checkpoint-append-after-finish.mongodb");

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

            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        2,
                        doc! { "_id": 2_i64, "sku": "captured" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("captured insert");

            let completed = database
                .prepare_concurrent_checkpoint()
                .expect("prepare concurrent checkpoint")
                .expect("checkpoint job")
                .run()
                .expect("run concurrent checkpoint")
                .expect("completed checkpoint");
            assert!(
                database
                    .finish_concurrent_checkpoint(completed)
                    .expect("finish checkpoint"),
                "expected checkpoint completion to apply"
            );

            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        3,
                        doc! { "_id": 3_i64, "sku": "after-finish" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("post-finish insert");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.current_record_count, 3);
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
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn concurrent_snapshot_publication_reuses_clean_collection_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join("concurrent-published-snapshot.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");

            let mut widgets = CollectionCatalog::new(doc! {});
            insert_record(&mut widgets, 1, doc! { "_id": 1_i64, "sku": "alpha" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: widgets,
                    change_events: Vec::new(),
                })
                .expect("seed widgets");

            let mut gadgets = CollectionCatalog::new(doc! {});
            insert_record(&mut gadgets, 1, doc! { "_id": 10_i64, "sku": "stable" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "gadgets".to_string(),
                    collection_state: gadgets,
                    change_events: Vec::new(),
                })
                .expect("seed gadgets");

            database.checkpoint().expect("base checkpoint");
        }

        let before = namespace_meta_page_ids(&path);

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("reopen");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        2,
                        doc! { "_id": 2_i64, "sku": "beta" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("mutate widgets");

            let completed = database
                .prepare_concurrent_checkpoint()
                .expect("prepare concurrent checkpoint")
                .expect("checkpoint job")
                .run()
                .expect("run concurrent checkpoint")
                .expect("completed checkpoint");
            assert!(
                database
                    .finish_concurrent_checkpoint(completed)
                    .expect("finish checkpoint"),
                "expected published snapshot to apply"
            );
        }

        let after = namespace_meta_page_ids(&path);
        assert_eq!(
            after.get("app.gadgets"),
            before.get("app.gadgets"),
            "unchanged collection should keep its published meta page"
        );
        assert_ne!(
            after.get("app.widgets"),
            before.get("app.widgets"),
            "dirty collection should publish a new meta page"
        );

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.wal_records_since_checkpoint, 0);
    }

    #[test]
    fn foreground_checkpoint_reuses_clean_collection_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir
            .path()
            .join("foreground-published-snapshot.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");

            let mut widgets = CollectionCatalog::new(doc! {});
            insert_record(&mut widgets, 1, doc! { "_id": 1_i64, "sku": "alpha" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: widgets,
                    change_events: Vec::new(),
                })
                .expect("seed widgets");

            let mut gadgets = CollectionCatalog::new(doc! {});
            insert_record(&mut gadgets, 1, doc! { "_id": 10_i64, "sku": "stable" });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "gadgets".to_string(),
                    collection_state: gadgets,
                    change_events: Vec::new(),
                })
                .expect("seed gadgets");

            database.checkpoint().expect("base checkpoint");
        }

        let before = namespace_meta_page_ids(&path);

        let debug_session = session("foreground-published-snapshot");
        {
            let _install = install(&debug_session);
            let mut database = DatabaseFile::open_or_create(&path).expect("reopen");
            database
                .commit_mutation(WalMutation::ApplyCollectionChanges {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    create_options: None,
                    changes: vec![CollectionChange::Insert(CollectionRecord::new(
                        2,
                        doc! { "_id": 2_i64, "sku": "beta" },
                    ))],
                    inserts: Vec::new(),
                    updates: Vec::new(),
                    deletes: Vec::new(),
                    change_events: Vec::new(),
                })
                .expect("mutate widgets");
            database.checkpoint().expect("foreground checkpoint");
        }

        let after = namespace_meta_page_ids(&path);
        assert_eq!(
            after.get("app.gadgets"),
            before.get("app.gadgets"),
            "unchanged collection should keep its published meta page"
        );
        assert_ne!(
            after.get("app.widgets"),
            before.get("app.widgets"),
            "dirty collection should publish a new meta page"
        );

        let report = debug_session.report();
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "foregroundCheckpointDirtyCollections",
            ),
            Some(1)
        );
        assert_eq!(
            counter_value(
                &report,
                Component::Storage,
                "foregroundCheckpointPublishedSnapshots",
            ),
            Some(1)
        );

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.wal_records_since_checkpoint, 0);
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
