use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use fs4::FileExt;

use crate::{
    InfoCheckpoint, InfoReport, InfoSummary, InfoWal,
    v2::{
        catalog::{PagerCollectionReadView, PagerNamespaceCatalog},
        layout::{
            FILE_FORMAT_VERSION, FILE_MAGIC, FileHeader, SUPERBLOCK_COUNT, SUPERBLOCK_LEN,
            Superblock,
        },
        pager::Pager,
    },
};

pub(crate) fn is_v2_file(path: impl AsRef<Path>) -> Result<bool> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    Ok(&magic == FILE_MAGIC)
}

pub(crate) fn create_empty(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    file.lock_exclusive()?;
    file.set_len(0)?;

    let header = FileHeader::default();
    let superblock = Superblock::default();

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header.encode())?;
    file.write_all(&superblock.encode())?;
    file.write_all(&vec![0_u8; SUPERBLOCK_LEN * (SUPERBLOCK_COUNT - 1)])?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn read_info(path: impl AsRef<Path>) -> Result<InfoReport> {
    let path = path.as_ref().to_path_buf();
    let pager = Pager::open(&path)?;
    let header = pager.header();
    let superblock = pager.active_superblock();
    let summary = &superblock.summary;
    let page_size = u64::from(header.page_size);
    let page_bytes = summary.page_count.saturating_mul(page_size);

    Ok(InfoReport {
        path,
        file_format_version: FILE_FORMAT_VERSION,
        file_size: pager.file_size(),
        last_applied_sequence: superblock.durable_lsn,
        summary: InfoSummary {
            database_count: summary.database_count as usize,
            collection_count: summary.collection_count as usize,
            index_count: summary.index_count as usize,
            record_count: summary.record_count as usize,
            index_entry_count: summary.index_entry_count as usize,
            change_event_count: summary.change_event_count as usize,
            plan_cache_entry_count: summary.plan_cache_entry_count as usize,
            document_bytes: summary.document_bytes,
            index_bytes: summary.index_bytes,
            total_bytes: summary.document_bytes + summary.index_bytes,
        },
        last_checkpoint: InfoCheckpoint {
            generation: superblock.generation,
            last_applied_sequence: superblock.durable_lsn,
            last_checkpoint_unix_ms: superblock.last_checkpoint_unix_ms,
            active_superblock_slot: pager.active_superblock_slot(),
            valid_superblocks: pager.valid_superblocks(),
            database_count: summary.database_count as usize,
            collection_count: summary.collection_count as usize,
            index_count: summary.index_count as usize,
            snapshot_offset: 0,
            snapshot_len: 0,
            wal_offset: superblock.wal_start_offset,
            page_size: header.page_size as usize,
            page_count: summary.page_count as usize,
            page_bytes,
            record_page_count: 0,
            record_page_bytes: 0,
            index_page_count: 0,
            index_page_bytes: 0,
            change_event_page_count: 0,
            change_event_page_bytes: 0,
            record_count: summary.record_count as usize,
            index_entry_count: summary.index_entry_count as usize,
            change_event_count: summary.change_event_count as usize,
            plan_cache_entry_count: summary.plan_cache_entry_count as usize,
            total_bytes: page_bytes,
        },
        wal_since_checkpoint: InfoWal {
            record_count: 0,
            bytes: pager.wal_bytes(),
            truncated_tail: false,
        },
        databases: Vec::new(),
    })
}

pub(crate) fn open_namespace_catalog(path: impl AsRef<Path>) -> Result<PagerNamespaceCatalog> {
    let pager = Arc::new(Mutex::new(Pager::open(path)?));
    let namespace_root_page_id = pager
        .lock()
        .map_err(|_| anyhow::anyhow!("v2 pager mutex was poisoned"))?
        .active_superblock()
        .roots
        .namespace_root_page_id;
    Ok(PagerNamespaceCatalog::new(namespace_root_page_id, pager))
}

pub(crate) fn open_collection_read_view(
    path: impl AsRef<Path>,
    database: &str,
    collection: &str,
) -> Result<Option<PagerCollectionReadView>> {
    open_namespace_catalog(path)?.collection_read_view(database, collection)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{create_empty, is_v2_file, read_info};
    use crate::v2::layout::{DATA_START_OFFSET, DEFAULT_PAGE_SIZE};

    #[test]
    fn creates_empty_v2_file_and_reads_info() {
        let temp_dir = tempdir().expect("tempdir");
        let path = PathBuf::from(temp_dir.path().join("v2-empty.mongodb"));

        create_empty(&path).expect("create v2 file");
        assert!(is_v2_file(&path).expect("detect v2 magic"));

        let report = read_info(&path).expect("read v2 info");
        assert_eq!(report.file_format_version, 8);
        assert_eq!(report.summary.record_count, 0);
        assert_eq!(report.summary.index_count, 0);
        assert_eq!(report.last_checkpoint.active_superblock_slot, 0);
        assert_eq!(report.last_checkpoint.valid_superblocks, 1);
        assert_eq!(report.last_checkpoint.page_size, DEFAULT_PAGE_SIZE as usize);
        assert_eq!(report.last_checkpoint.wal_offset, DATA_START_OFFSET);
        assert_eq!(report.wal_since_checkpoint.bytes, 0);
    }
}
