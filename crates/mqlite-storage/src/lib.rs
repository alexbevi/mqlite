use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use blake3::Hasher;
use fs4::FileExt;
use mqlite_catalog::{Catalog, CollectionCatalog, CollectionRecord, DatabaseCatalog, IndexCatalog};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FILE_MAGIC: &[u8; 8] = b"MQLTHDR3";
pub const FILE_FORMAT_VERSION: u32 = 3;
pub const PAGE_SIZE: usize = 4096;
const HEADER_LEN: usize = 4096;
const SUPERBLOCK_LEN: usize = 512;
const SUPERBLOCK_COUNT: usize = 2;
const DATA_START_OFFSET: u64 = (HEADER_LEN + (SUPERBLOCK_LEN * SUPERBLOCK_COUNT)) as u64;
const SUPERBLOCK_MAGIC: &[u8; 8] = b"MQLTSB03";
const WAL_FRAME_MAGIC: &[u8; 4] = b"WAL1";
const WAL_HEADER_LEN: usize = 40;
const PAGE_MAGIC: &[u8; 8] = b"MQLTPG03";
const PAGE_HEADER_LEN: usize = 32;
const SLOT_ENTRY_LEN: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedState {
    pub file_format_version: u32,
    pub last_applied_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
    pub catalog: Catalog,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalMutation {
    ReplaceCollection {
        database: String,
        collection: String,
        collection_state: CollectionCatalog,
    },
    DropCollection {
        database: String,
        collection: String,
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
    wal_records_since_checkpoint: usize,
    wal_bytes_since_checkpoint: u64,
    truncated_wal_tail: bool,
    checkpoint_page_count: usize,
    checkpoint_record_count: usize,
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
    pub page_count: usize,
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
    pub checkpoint_record_count: usize,
    pub current_record_count: usize,
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
    checkpoint_page_count: usize,
    checkpoint_record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotState {
    file_format_version: u32,
    last_applied_sequence: u64,
    last_checkpoint_unix_ms: u64,
    catalog: SnapshotCatalog,
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
    indexes: BTreeMap<String, IndexCatalog>,
    pages: Vec<PageRef>,
    record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageRef {
    page_id: u64,
    offset: u64,
    checksum: [u8; 32],
    page_len: u32,
    record_count: u16,
}

#[derive(Debug)]
struct EncodedPage {
    page_id: u64,
    bytes: Vec<u8>,
    checksum: [u8; 32],
    record_count: u16,
}

#[derive(Debug, Default)]
struct EncodedSnapshotCatalog {
    catalog: SnapshotCatalog,
    pages: Vec<EncodedPage>,
    record_count: usize,
}

#[derive(Debug, Default)]
struct SlottedPageBuilder {
    page_id: u64,
    bytes: Vec<u8>,
    slot_count: u16,
    free_start: usize,
    free_end: usize,
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
            },
            active_slot: 0,
            active_superblock: Superblock::default(),
            valid_superblocks: 0,
            wal_records_since_checkpoint: 0,
            wal_bytes_since_checkpoint: 0,
            truncated_wal_tail: false,
            checkpoint_page_count: 0,
            checkpoint_record_count: 0,
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

    pub fn has_pending_wal(&self) -> bool {
        self.wal_records_since_checkpoint > 0
    }

    pub fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64> {
        let mut catalog = self.state.catalog.clone();
        apply_mutation(&mut catalog, &mutation)?;

        let sequence = self.state.last_applied_sequence + 1;
        let appended_bytes = append_wal_entry(
            &mut self.file,
            &WalEntry {
                sequence,
                mutation: mutation.clone(),
            },
        )?;

        self.state.catalog = catalog;
        self.state.last_applied_sequence = sequence;
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
            checkpoint_page_count: loaded.checkpoint_page_count,
            checkpoint_record_count: loaded.checkpoint_record_count,
            current_record_count: record_count(&loaded.state.catalog),
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
            page_count: loaded.checkpoint_page_count,
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
        self.wal_records_since_checkpoint = loaded.wal_recovery.records;
        self.wal_bytes_since_checkpoint = loaded.wal_recovery.bytes;
        self.truncated_wal_tail = loaded.wal_recovery.truncated_tail;
        self.checkpoint_page_count = loaded.checkpoint_page_count;
        self.checkpoint_record_count = loaded.checkpoint_record_count;
        Ok(())
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        self.state.file_format_version = FILE_FORMAT_VERSION;
        self.state.last_checkpoint_unix_ms = current_unix_ms();

        let pages_start = self.file.metadata()?.len().max(DATA_START_OFFSET);
        let encoded_catalog = encode_snapshot_catalog(&self.state.catalog)?;

        let mut page_refs = Vec::with_capacity(encoded_catalog.pages.len());
        let mut page_offset = pages_start;
        for page in &encoded_catalog.pages {
            page_refs.push(PageRef {
                page_id: page.page_id,
                offset: page_offset,
                checksum: page.checksum,
                page_len: page.bytes.len() as u32,
                record_count: page.record_count,
            });
            page_offset += page.bytes.len() as u64;
        }

        let snapshot_catalog = resolve_snapshot_catalog(encoded_catalog.catalog, &page_refs)?;
        let snapshot_state = SnapshotState {
            file_format_version: FILE_FORMAT_VERSION,
            last_applied_sequence: self.state.last_applied_sequence,
            last_checkpoint_unix_ms: self.state.last_checkpoint_unix_ms,
            catalog: snapshot_catalog,
        };
        let snapshot = bson::to_vec(&snapshot_state)?;
        let snapshot_offset = page_offset;

        self.file.seek(SeekFrom::Start(pages_start))?;
        for page in &encoded_catalog.pages {
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
        self.wal_records_since_checkpoint = 0;
        self.wal_bytes_since_checkpoint = 0;
        self.truncated_wal_tail = false;
        self.checkpoint_page_count = page_refs.len();
        self.checkpoint_record_count = encoded_catalog.record_count;
        Ok(())
    }
}

impl SlottedPageBuilder {
    fn new(page_id: u64) -> Self {
        Self {
            page_id,
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

    fn insert(&mut self, record: &CollectionRecord) -> Result<()> {
        let payload = bson::to_vec(&record.document)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !self.can_fit(payload.len()) {
            return Err(StorageError::InvalidPage.into());
        }

        self.free_end -= payload.len();
        self.bytes[self.free_end..self.free_end + payload.len()].copy_from_slice(&payload);

        let slot_offset = self.free_start;
        self.bytes[slot_offset..slot_offset + 8].copy_from_slice(&record.record_id.to_le_bytes());
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
        self.bytes[18..20].copy_from_slice(&0_u16.to_le_bytes());
        self.bytes[20..22].copy_from_slice(&(self.free_start as u16).to_le_bytes());
        self.bytes[22..24].copy_from_slice(&(self.free_end as u16).to_le_bytes());
        self.bytes[24..32].copy_from_slice(&0_u64.to_le_bytes());

        EncodedPage {
            page_id: self.page_id,
            checksum: hash_bytes(&self.bytes),
            bytes: self.bytes,
            record_count: self.slot_count,
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
        let (state, checkpoint_pages, checkpoint_records) = match read_snapshot(file, &superblock) {
            Ok(loaded) => loaded,
            Err(error) if is_skippable_checkpoint_error(&error) => continue,
            Err(error) => return Err(error),
        };
        valid_superblocks += 1;
        candidates.push((
            slot,
            superblock,
            state,
            checkpoint_pages,
            checkpoint_records,
        ));
    }

    let Some((
        active_slot,
        active_superblock,
        mut state,
        checkpoint_page_count,
        checkpoint_record_count,
    )) = candidates
        .into_iter()
        .max_by_key(|(_, superblock, _, _, _)| superblock.generation)
    else {
        return Err(StorageError::NoValidSuperblock.into());
    };

    let wal_recovery = replay_wal(
        file,
        active_superblock.wal_offset,
        &mut state.catalog,
        state.last_applied_sequence,
    )?;
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
        checkpoint_page_count,
        checkpoint_record_count,
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
) -> Result<(PersistedState, usize, usize)> {
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

    let (catalog, page_count, record_count) = restore_catalog(file, &snapshot_state.catalog)?;
    Ok((
        PersistedState {
            file_format_version: FILE_FORMAT_VERSION,
            last_applied_sequence: snapshot_state.last_applied_sequence,
            last_checkpoint_unix_ms: snapshot_state.last_checkpoint_unix_ms,
            catalog,
        },
        page_count,
        record_count,
    ))
}

fn append_wal_entry(file: &mut File, entry: &WalEntry) -> Result<u64> {
    let payload = bson::to_vec(entry)?;
    let payload_checksum = hash_bytes(&payload);
    let frame_len = (WAL_HEADER_LEN + payload.len()) as u64;
    let frame_offset = file.metadata()?.len().max(DATA_START_OFFSET);

    file.seek(SeekFrom::Start(frame_offset))?;
    file.write_all(WAL_FRAME_MAGIC)?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&payload_checksum)?;
    file.write_all(&payload)?;
    file.flush()?;
    file.sync_all()?;
    Ok(frame_len)
}

fn replay_wal(
    file: &mut File,
    start_offset: u64,
    catalog: &mut Catalog,
    mut last_applied_sequence: u64,
) -> Result<WalRecovery> {
    let file_size = file.metadata()?.len();
    if start_offset > file_size {
        return Err(StorageError::Truncated.into());
    }

    let mut recovery = WalRecovery::default();
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
            apply_mutation(catalog, &entry.mutation)?;
            last_applied_sequence = entry.sequence;
            recovery.last_sequence = Some(entry.sequence);
            recovery.records += 1;
        }

        offset = payload_end;
    }

    recovery.bytes = offset.saturating_sub(start_offset);
    Ok(recovery)
}

fn apply_mutation(catalog: &mut Catalog, mutation: &WalMutation) -> Result<()> {
    match mutation {
        WalMutation::ReplaceCollection {
            database,
            collection,
            collection_state,
        } => catalog.replace_collection(database, collection, collection_state.clone()),
        WalMutation::DropCollection {
            database,
            collection,
        } => {
            catalog.drop_collection(database, collection)?;
        }
    }
    Ok(())
}

fn encode_snapshot_catalog(catalog: &Catalog) -> Result<EncodedSnapshotCatalog> {
    let mut encoded_catalog = EncodedSnapshotCatalog::default();
    let mut next_page_id = 1_u64;

    for (database_name, database) in &catalog.databases {
        let mut snapshot_database = SnapshotDatabaseCatalog {
            collections: BTreeMap::new(),
        };

        for (collection_name, collection) in &database.collections {
            let pages = encode_collection_pages(&collection.records, &mut next_page_id)?;
            let page_count_before = encoded_catalog.pages.len();
            encoded_catalog.pages.extend(pages);
            let page_indexes = (page_count_before..encoded_catalog.pages.len())
                .map(|index| encoded_catalog.pages[index].page_id)
                .collect::<Vec<_>>();

            snapshot_database.collections.insert(
                collection_name.clone(),
                SnapshotCollectionCatalog {
                    options: collection.options.clone(),
                    indexes: collection.indexes.clone(),
                    pages: page_indexes
                        .into_iter()
                        .map(|page_id| PageRef {
                            page_id,
                            offset: 0,
                            checksum: [0_u8; 32],
                            page_len: 0,
                            record_count: 0,
                        })
                        .collect(),
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
            collection.pages = collection
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

    Ok(pending_catalog)
}

fn encode_collection_pages(
    records: &[CollectionRecord],
    next_page_id: &mut u64,
) -> Result<Vec<EncodedPage>> {
    let mut pages = Vec::new();
    let mut builder = SlottedPageBuilder::new(*next_page_id);

    for record in records {
        let payload = bson::to_vec(&record.document)?;
        if PAGE_HEADER_LEN + SLOT_ENTRY_LEN + payload.len() > PAGE_SIZE {
            return Err(StorageError::RecordTooLarge.into());
        }
        if !builder.can_fit(payload.len()) {
            pages.push(builder.finish());
            *next_page_id += 1;
            builder = SlottedPageBuilder::new(*next_page_id);
        }
        builder.insert(record)?;
    }

    if !builder.is_empty() {
        pages.push(builder.finish());
        *next_page_id += 1;
    }

    Ok(pages)
}

fn restore_catalog(
    file: &mut File,
    snapshot_catalog: &SnapshotCatalog,
) -> Result<(Catalog, usize, usize)> {
    let mut catalog = Catalog::new();
    let mut page_count = 0;
    let mut record_count_total = 0;

    for (database_name, database) in &snapshot_catalog.databases {
        let mut restored_database = DatabaseCatalog {
            collections: BTreeMap::new(),
        };

        for (collection_name, collection) in &database.collections {
            let mut records = Vec::with_capacity(collection.record_count);
            for page_ref in &collection.pages {
                let page_records = decode_page(file, page_ref)?;
                page_count += 1;
                records.extend(page_records);
            }

            if records.len() != collection.record_count {
                return Err(StorageError::InvalidPage.into());
            }

            record_count_total += records.len();
            restored_database.collections.insert(
                collection_name.clone(),
                CollectionCatalog {
                    options: collection.options.clone(),
                    indexes: collection.indexes.clone(),
                    records,
                },
            );
        }

        catalog
            .databases
            .insert(database_name.clone(), restored_database);
    }

    Ok((catalog, page_count, record_count_total))
}

fn decode_page(file: &mut File, page_ref: &PageRef) -> Result<Vec<CollectionRecord>> {
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
    let free_start = u16::from_le_bytes(page[20..22].try_into().expect("free start")) as usize;
    let free_end = u16::from_le_bytes(page[22..24].try_into().expect("free end")) as usize;

    if free_start > PAGE_SIZE
        || free_end > PAGE_SIZE
        || free_start < PAGE_HEADER_LEN
        || free_end < free_start
        || PAGE_HEADER_LEN + slot_count as usize * SLOT_ENTRY_LEN > free_start
    {
        return Err(StorageError::InvalidPage.into());
    }

    let mut records = Vec::with_capacity(slot_count as usize);
    for slot_index in 0..slot_count as usize {
        let slot_offset = PAGE_HEADER_LEN + slot_index * SLOT_ENTRY_LEN;
        let record_id = u64::from_le_bytes(
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
        records.push(CollectionRecord {
            record_id,
            document,
        });
    }

    if records.len() != page_ref.record_count as usize {
        return Err(StorageError::InvalidPage.into());
    }

    Ok(records)
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
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
    };

    use bson::doc;
    use mqlite_catalog::{CollectionCatalog, CollectionRecord};
    use tempfile::tempdir;

    use super::{
        DATA_START_OFFSET, DatabaseFile, FILE_FORMAT_VERSION, PAGE_SIZE, VerifyReport, WalMutation,
    };

    #[test]
    fn recovers_replace_collection_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection =
                CollectionCatalog::new(doc! { "validator": { "qty": { "$gte": 0 } } });
            collection.records.push(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "a", "qty": 4 },
            });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
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
    fn checkpoints_persist_record_pages_and_record_ids() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("page-persist.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            collection.records.push(CollectionRecord {
                record_id: 7,
                document: doc! { "_id": 1, "sku": "alpha", "qty": 1 },
            });
            collection.records.push(CollectionRecord {
                record_id: 12,
                document: doc! { "_id": 2, "sku": "beta", "qty": 2 },
            });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
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
        assert_eq!(
            collection.records[1].document.get_str("sku").expect("sku"),
            "beta"
        );
    }

    #[test]
    fn spills_large_collections_across_multiple_pages() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("multi-page.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            for record_id in 1..=12_u64 {
                collection.records.push(CollectionRecord {
                    record_id,
                    document: doc! {
                        "_id": record_id as i64,
                        "payload": "x".repeat(PAGE_SIZE / 3),
                    },
                });
            }
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert!(inspect.checkpoint_page_count >= 2);
        assert_eq!(inspect.checkpoint_record_count, 12);
    }

    #[test]
    fn ignores_truncated_wal_tail_during_recovery() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("truncated-tail.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection = CollectionCatalog::new(doc! {});
            collection.records.push(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1 },
            });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
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
            collection.records.push(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "payload": "hello" },
            });
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: collection,
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open raw file");
        let page_offset = inspect.snapshot_offset - PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(page_offset))
            .expect("seek to page");
        file.write_all(&[0xFF]).expect("corrupt first byte");
        file.flush().expect("flush");

        let report = DatabaseFile::verify(&path).expect("verify should fall back");
        assert_eq!(inspect.checkpoint_page_count, 1);
        assert_eq!(report.page_count, 0);
        assert_eq!(report.record_count, 1);
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
                page_count: 0,
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
