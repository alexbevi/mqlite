use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use fs4::FileExt;
use mqlite_catalog::Catalog;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FILE_MAGIC: &[u8; 8] = b"MQLITE01";
pub const FILE_FORMAT_VERSION: u32 = 1;
const HEADER_LEN: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedState {
    pub file_format_version: u32,
    pub checkpoint_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
    pub catalog: Catalog,
}

#[derive(Debug)]
pub struct DatabaseFile {
    path: PathBuf,
    file: File,
    state: PersistedState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerifyReport {
    pub valid: bool,
    pub file_format_version: u32,
    pub checkpoint_sequence: u64,
    pub databases: usize,
    pub collections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InspectReport {
    pub path: PathBuf,
    pub file_format_version: u32,
    pub checkpoint_sequence: u64,
    pub last_checkpoint_unix_ms: u64,
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
                checkpoint_sequence: 0,
                last_checkpoint_unix_ms: current_unix_ms(),
                catalog: Catalog::new(),
            },
        };

        if database.file.metadata()?.len() == 0 {
            database.write_checkpoint()?;
        } else {
            database.state = read_state(&mut database.file)?;
        }

        Ok(database)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.state.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.state.catalog
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
        let state = read_state(&mut file)?;
        Ok(InspectReport {
            path,
            file_format_version: state.file_format_version,
            checkpoint_sequence: state.checkpoint_sequence,
            last_checkpoint_unix_ms: state.last_checkpoint_unix_ms,
            databases: state.catalog.database_names(),
        })
    }

    pub fn verify(path: impl AsRef<Path>) -> Result<VerifyReport> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let state = read_state(&mut file)?;
        let collections = state
            .catalog
            .databases
            .values()
            .map(|database| database.collections.len())
            .sum();

        Ok(VerifyReport {
            valid: true,
            file_format_version: state.file_format_version,
            checkpoint_sequence: state.checkpoint_sequence,
            databases: state.catalog.databases.len(),
            collections,
        })
    }

    fn write_checkpoint(&mut self) -> Result<()> {
        self.state.checkpoint_sequence += 1;
        self.state.last_checkpoint_unix_ms = current_unix_ms();
        let snapshot = bson::to_vec(&self.state)?;
        let header = encode_header(
            self.state.file_format_version,
            self.state.checkpoint_sequence,
            snapshot.len() as u64,
        );

        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        self.file.write_all(&header)?;
        self.file.write_all(&snapshot)?;
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

fn encode_header(
    file_format_version: u32,
    checkpoint_sequence: u64,
    snapshot_len: u64,
) -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(FILE_MAGIC);
    header[8..12].copy_from_slice(&file_format_version.to_le_bytes());
    header[12..16].copy_from_slice(&0_u32.to_le_bytes());
    header[16..24].copy_from_slice(&checkpoint_sequence.to_le_bytes());
    header[24..32].copy_from_slice(&(HEADER_LEN as u64).to_le_bytes());
    header[32..40].copy_from_slice(&snapshot_len.to_le_bytes());
    header[40..48].copy_from_slice(&0_u64.to_le_bytes());
    header[48..56].copy_from_slice(&0_u64.to_le_bytes());
    header[56..64].copy_from_slice(&0_u64.to_le_bytes());
    header
}

fn read_state(file: &mut File) -> Result<PersistedState> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; HEADER_LEN];
    file.read_exact(&mut header)?;

    if &header[..8] != FILE_MAGIC {
        return Err(StorageError::InvalidHeader.into());
    }

    let file_format_version = u32::from_le_bytes(header[8..12].try_into().expect("version"));
    if file_format_version != FILE_FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(file_format_version).into());
    }

    let snapshot_offset = u64::from_le_bytes(header[24..32].try_into().expect("offset"));
    let snapshot_len = u64::from_le_bytes(header[32..40].try_into().expect("length"));
    if snapshot_offset < HEADER_LEN as u64 {
        return Err(StorageError::InvalidHeader.into());
    }

    file.seek(SeekFrom::Start(snapshot_offset))?;
    let mut snapshot = vec![0_u8; snapshot_len as usize];
    file.read_exact(&mut snapshot)
        .map_err(|_| StorageError::Truncated)?;

    Ok(bson::from_slice::<PersistedState>(&snapshot)?)
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use tempfile::tempdir;

    use super::{DatabaseFile, FILE_FORMAT_VERSION, VerifyReport};

    #[test]
    fn creates_and_reopens_database_file() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("test.mongodb");

        {
            let mut database = DatabaseFile::open_or_create(&path).expect("create database");
            database
                .catalog_mut()
                .create_collection("app", "widgets", doc! {})
                .expect("create collection");
            database.checkpoint().expect("checkpoint");
        }

        let inspect = DatabaseFile::inspect(&path).expect("inspect");
        assert_eq!(inspect.file_format_version, FILE_FORMAT_VERSION);
        assert_eq!(inspect.databases, vec!["app".to_string()]);
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
                checkpoint_sequence: 1,
                databases: 0,
                collections: 0,
            }
        );
    }
}
