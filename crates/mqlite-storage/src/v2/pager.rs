use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};

use anyhow::{Result, anyhow};
use parking_lot::Mutex;

use crate::v2::{
    layout::{
        DATA_START_OFFSET, FileHeader, SUPERBLOCK_COUNT, SUPERBLOCK_LEN, Superblock, page_offset,
    },
    page::PageId,
};

const DEFAULT_CACHE_BYTES: usize = 32 * 1024 * 1024;
const MIN_CACHE_PAGES: usize = 128;

pub(crate) type SharedPage = Arc<[u8]>;

#[derive(Debug)]
pub struct Pager {
    file: File,
    header: FileHeader,
    active_superblock_slot: usize,
    active_superblock: Superblock,
    valid_superblocks: usize,
    file_size: u64,
    cache: Mutex<PageCache>,
}

#[derive(Debug)]
struct PageCache {
    pages: HashMap<PageId, CachedPage>,
    access_order: VecDeque<(u64, PageId)>,
    next_epoch: u64,
    capacity: usize,
}

#[derive(Debug, Clone)]
struct CachedPage {
    bytes: SharedPage,
    access_epoch: u64,
}

impl Pager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cache_capacity(path, None)
    }

    fn open_with_cache_capacity(
        path: impl AsRef<Path>,
        cache_page_capacity: Option<usize>,
    ) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_size = file.metadata()?.len();
        if file_size < DATA_START_OFFSET {
            return Err(anyhow!("v2 file is truncated"));
        }

        let mut header_bytes = [0_u8; crate::v2::layout::HEADER_LEN];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_bytes)?;
        let header = FileHeader::decode(&header_bytes)?;

        let mut active_superblock: Option<(usize, Superblock)> = None;
        let mut valid_superblocks = 0;
        for slot in 0..SUPERBLOCK_COUNT {
            let offset = crate::v2::layout::HEADER_LEN as u64 + slot as u64 * SUPERBLOCK_LEN as u64;
            file.seek(SeekFrom::Start(offset))?;
            let mut bytes = [0_u8; SUPERBLOCK_LEN];
            file.read_exact(&mut bytes)?;
            let superblock = match Superblock::decode(&bytes) {
                Ok(superblock) => superblock,
                Err(_) => continue,
            };
            valid_superblocks += 1;
            match active_superblock.as_ref() {
                Some((_, current)) if current.generation >= superblock.generation => {}
                _ => active_superblock = Some((slot, superblock)),
            }
        }

        let Some((active_superblock_slot, active_superblock)) = active_superblock else {
            return Err(anyhow!("v2 file does not contain a valid superblock"));
        };

        Ok(Self {
            file,
            header: header.clone(),
            active_superblock_slot,
            active_superblock,
            valid_superblocks,
            file_size,
            cache: Mutex::new(PageCache::new(
                cache_page_capacity
                    .unwrap_or_else(|| default_cache_page_capacity(header.page_size)),
            )),
        })
    }

    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    pub fn active_superblock_slot(&self) -> usize {
        self.active_superblock_slot
    }

    pub fn active_superblock(&self) -> &Superblock {
        &self.active_superblock
    }

    pub fn valid_superblocks(&self) -> usize {
        self.valid_superblocks
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    pub fn wal_bytes(&self) -> u64 {
        self.file_size
            .saturating_sub(self.active_superblock.wal_start_offset)
    }

    pub fn read_page_bytes(&self, page_id: PageId) -> Result<SharedPage> {
        if let Some(bytes) = self.cache.lock().get(page_id) {
            return Ok(bytes);
        }

        let offset = page_offset(page_id, self.header.page_size)?;
        let mut bytes = vec![0_u8; self.header.page_size as usize];
        read_exact_at(&self.file, &mut bytes, offset)?;
        let bytes: SharedPage = bytes.into();

        Ok(self.cache.lock().insert(page_id, bytes))
    }

    #[cfg(test)]
    fn cache_len(&self) -> usize {
        self.cache.lock().pages.len()
    }
}

impl PageCache {
    fn new(capacity: usize) -> Self {
        Self {
            pages: HashMap::new(),
            access_order: VecDeque::new(),
            next_epoch: 0,
            capacity: capacity.max(1),
        }
    }

    fn get(&mut self, page_id: PageId) -> Option<SharedPage> {
        let epoch = self.next_epoch();
        let page = self.pages.get_mut(&page_id)?;
        page.access_epoch = epoch;
        self.access_order.push_back((epoch, page_id));
        Some(Arc::clone(&page.bytes))
    }

    fn insert(&mut self, page_id: PageId, bytes: SharedPage) -> SharedPage {
        let epoch = self.next_epoch();
        let entry = self.pages.entry(page_id).or_insert_with(|| CachedPage {
            bytes: Arc::clone(&bytes),
            access_epoch: epoch,
        });
        entry.bytes = Arc::clone(&bytes);
        entry.access_epoch = epoch;
        self.access_order.push_back((epoch, page_id));
        self.evict_if_needed();
        bytes
    }

    fn next_epoch(&mut self) -> u64 {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.next_epoch
    }

    fn evict_if_needed(&mut self) {
        while self.pages.len() > self.capacity {
            let Some((epoch, page_id)) = self.access_order.pop_front() else {
                break;
            };
            let should_remove = self
                .pages
                .get(&page_id)
                .is_some_and(|page| page.access_epoch == epoch);
            if should_remove {
                self.pages.remove(&page_id);
            }
        }
    }
}

fn default_cache_page_capacity(page_size: u32) -> usize {
    (DEFAULT_CACHE_BYTES / page_size.max(1) as usize).max(MIN_CACHE_PAGES)
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], offset: u64) -> Result<()> {
    let mut read_offset = offset;
    while !bytes.is_empty() {
        let bytes_read = read_at(file, bytes, read_offset)?;
        if bytes_read == 0 {
            return Err(anyhow!(
                "unexpected EOF while reading page at offset {read_offset}"
            ));
        }
        read_offset += bytes_read as u64;
        bytes = &mut bytes[bytes_read..];
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(bytes, offset).map_err(Into::into)
}

#[cfg(windows)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(bytes, offset).map_err(Into::into)
}

#[cfg(not(any(unix, windows)))]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<usize> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read(bytes).map_err(|error| match error.kind() {
        ErrorKind::UnexpectedEof => anyhow!("unexpected EOF while reading page"),
        _ => error.into(),
    })
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write, sync::Arc};

    use tempfile::NamedTempFile;

    use super::Pager;
    use crate::v2::{
        layout::{DEFAULT_PAGE_SIZE, FileHeader, SUPERBLOCK_COUNT, SUPERBLOCK_LEN, Superblock},
        page::{RecordLeafPage, RecordSlot},
    };

    #[test]
    fn caches_repeated_page_reads() {
        let temp = NamedTempFile::new().expect("temp file");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(temp.path())
            .expect("open temp file");

        file.write_all(&FileHeader::default().encode())
            .expect("write header");
        file.write_all(&Superblock::default().encode())
            .expect("write superblock");
        file.write_all(&vec![0_u8; SUPERBLOCK_LEN * (SUPERBLOCK_COUNT - 1)])
            .expect("write remaining superblocks");
        let page = RecordLeafPage {
            page_id: 1,
            next_page_id: None,
            entries: vec![RecordSlot {
                record_id: 7,
                encoded_document: bson::to_vec(&bson::doc! { "_id": 7 }).expect("encode doc"),
            }],
        };
        file.write_all(&page.encode().expect("encode page"))
            .expect("write page");
        file.flush().expect("flush file");

        let pager = Pager::open_with_cache_capacity(
            temp.path(),
            Some(1.max(32 * 1024 / DEFAULT_PAGE_SIZE as usize)),
        )
        .expect("open pager");
        let first = pager.read_page_bytes(1).expect("first read");
        let second = pager.read_page_bytes(1).expect("second read");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(pager.cache_len(), 1);
    }
}
