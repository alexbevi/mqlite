use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use blake3::Hasher;
use fs4::FileExt;
use mqlite_catalog::{Catalog, CollectionCatalog};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FILE_MAGIC: &[u8; 8] = b"MQLTHDR2";
pub const FILE_FORMAT_VERSION: u32 = 2;
const HEADER_LEN: usize = 4096;
const SUPERBLOCK_LEN: usize = 512;
const SUPERBLOCK_COUNT: usize = 2;
const DATA_START_OFFSET: u64 = (HEADER_LEN + (SUPERBLOCK_LEN * SUPERBLOCK_COUNT)) as u64;
const SUPERBLOCK_MAGIC: &[u8; 8] = b"MQLTSB02";
const WAL_FRAME_MAGIC: &[u8; 4] = b"WAL1";
const WAL_HEADER_LEN: usize = 40;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerifyReport {
    pub valid: bool,
    pub file_format_version: u32,
    pub checkpoint_generation: u64,
    pub last_applied_sequence: u64,
    pub databases: usize,
    pub collections: usize,
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
        Ok(())
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        self.state.file_format_version = FILE_FORMAT_VERSION;
        self.state.last_checkpoint_unix_ms = current_unix_ms();

        let snapshot = bson::to_vec(&self.state)?;
        let snapshot_offset = self.file.metadata()?.len().max(DATA_START_OFFSET);
        self.file.seek(SeekFrom::Start(snapshot_offset))?;
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
        Ok(())
    }
}

fn encode_header() -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
    header[16..20].copy_from_slice(&(SUPERBLOCK_LEN as u32).to_le_bytes());
    header[20..24].copy_from_slice(&(SUPERBLOCK_COUNT as u32).to_le_bytes());
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
    if header_len as usize != HEADER_LEN
        || superblock_len as usize != SUPERBLOCK_LEN
        || superblock_count as usize != SUPERBLOCK_COUNT
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
        let Some(superblock) = read_superblock(file, slot)? else {
            continue;
        };
        let state = match read_snapshot(file, &superblock) {
            Ok(state) => state,
            Err(_) => continue,
        };
        valid_superblocks += 1;
        candidates.push((slot, superblock, state));
    }

    let Some((active_slot, active_superblock, mut state)) = candidates
        .into_iter()
        .max_by_key(|(_, superblock, _)| superblock.generation)
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

fn read_snapshot(file: &mut File, superblock: &Superblock) -> Result<PersistedState> {
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

    let state = bson::from_slice::<PersistedState>(&snapshot)?;
    if state.file_format_version != FILE_FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(state.file_format_version).into());
    }
    Ok(state)
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
            return Err(StorageError::InvalidWalFrame.into());
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

fn superblock_offset(slot: usize) -> u64 {
    HEADER_LEN as u64 + (slot * SUPERBLOCK_LEN) as u64
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
    use mqlite_catalog::CollectionCatalog;
    use tempfile::tempdir;

    use super::{DATA_START_OFFSET, DatabaseFile, FILE_FORMAT_VERSION, VerifyReport, WalMutation};

    #[test]
    fn recovers_replace_collection_from_wal_without_checkpoint() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("wal-recovery.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            let mut collection =
                CollectionCatalog::new(doc! { "validator": { "qty": { "$gte": 0 } } });
            collection
                .documents
                .push(doc! { "_id": 1, "sku": "a", "qty": 4 });
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
    fn checkpoints_rotate_superblocks_and_clear_wal_view() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("checkpoint.mongodb");

        let first_slot;
        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            first_slot = DatabaseFile::inspect(&path)
                .expect("inspect")
                .active_superblock_slot;

            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: CollectionCatalog::new(doc! {}),
                })
                .expect("mutation");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_ne!(inspect.active_superblock_slot, first_slot);
        assert_eq!(inspect.wal_records_since_checkpoint, 0);
        assert!(inspect.checkpoint_generation >= 2);
    }

    #[test]
    fn ignores_truncated_wal_tail_during_recovery() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("truncated-tail.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .commit_mutation(WalMutation::ReplaceCollection {
                    database: "app".to_string(),
                    collection: "widgets".to_string(),
                    collection_state: CollectionCatalog::new(doc! {}),
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
                checkpoint_generation: 1,
                last_applied_sequence: 0,
                databases: 0,
                collections: 0,
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
