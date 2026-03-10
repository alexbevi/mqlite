use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Result, anyhow};

use crate::v2::layout::{
    DATA_START_OFFSET, FileHeader, SUPERBLOCK_COUNT, SUPERBLOCK_LEN, Superblock,
};

#[derive(Debug)]
pub struct Pager {
    file: File,
    header: FileHeader,
    active_superblock_slot: usize,
    active_superblock: Superblock,
    valid_superblocks: usize,
    file_size: u64,
}

impl Pager {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
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
            header,
            active_superblock_slot,
            active_superblock,
            valid_superblocks,
            file_size,
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

    pub fn wal_bytes(&self) -> u64 {
        self.active_superblock
            .wal_end_offset
            .saturating_sub(self.active_superblock.wal_start_offset)
    }

    pub fn into_file(self) -> File {
        self.file
    }
}
