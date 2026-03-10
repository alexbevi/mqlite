use anyhow::{Result, anyhow};
use blake3::Hasher;

use crate::v2::catalog::{RootSet, SummaryCounters};

pub const FILE_MAGIC: &[u8; 8] = b"MQLTHD08";
pub const FILE_FORMAT_VERSION: u32 = 8;
pub const DEFAULT_PAGE_SIZE: u32 = 8192;
pub const HEADER_LEN: usize = 4096;
pub const SUPERBLOCK_COUNT: usize = 2;
pub const SUPERBLOCK_LEN: usize = DEFAULT_PAGE_SIZE as usize;
pub const DATA_START_OFFSET: u64 = (HEADER_LEN + SUPERBLOCK_COUNT * SUPERBLOCK_LEN) as u64;
pub const HEADER_CHECKSUM_OFFSET: usize = 48;
pub const SUPERBLOCK_MAGIC: &[u8; 8] = b"MQLTSB08";
const ROOT_SET_OFFSET: usize = 56;
const SUMMARY_OFFSET: usize = 104;
const SUPERBLOCK_CHECKSUM_OFFSET: usize = 184;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PageKind {
    NamespaceInternal = 1,
    NamespaceLeaf = 2,
    CollectionMeta = 3,
    IndexMeta = 4,
    RecordInternal = 5,
    RecordLeaf = 6,
    SecondaryInternal = 7,
    SecondaryLeaf = 8,
    ChangeEventInternal = 9,
    ChangeEventLeaf = 10,
    Freelist = 11,
    Overflow = 12,
    Stats = 13,
}

impl TryFrom<u16> for PageKind {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self> {
        Ok(match value {
            1 => Self::NamespaceInternal,
            2 => Self::NamespaceLeaf,
            3 => Self::CollectionMeta,
            4 => Self::IndexMeta,
            5 => Self::RecordInternal,
            6 => Self::RecordLeaf,
            7 => Self::SecondaryInternal,
            8 => Self::SecondaryLeaf,
            9 => Self::ChangeEventInternal,
            10 => Self::ChangeEventLeaf,
            11 => Self::Freelist,
            12 => Self::Overflow,
            13 => Self::Stats,
            _ => return Err(anyhow!("unknown v2 page kind {value}")),
        })
    }
}

pub fn page_offset(page_id: u64, page_size: u32) -> Result<u64> {
    if page_id == 0 {
        return Err(anyhow!("page id 0 is reserved"));
    }
    Ok(DATA_START_OFFSET + (page_id - 1) * u64::from(page_size))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub page_size: u32,
    pub superblock_count: u32,
    pub file_id: [u8; 16],
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            superblock_count: SUPERBLOCK_COUNT as u32,
            file_id: [0_u8; 16],
        }
    }
}

impl FileHeader {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[..8].copy_from_slice(FILE_MAGIC);
        bytes[8..12].copy_from_slice(&FILE_FORMAT_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        bytes[16..20].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
        bytes[20..24].copy_from_slice(&(SUPERBLOCK_LEN as u32).to_le_bytes());
        bytes[24..28].copy_from_slice(&self.superblock_count.to_le_bytes());
        bytes[28..32].copy_from_slice(&DATA_START_OFFSET.to_le_bytes()[..4]);
        bytes[32..48].copy_from_slice(&self.file_id);
        let checksum = hash_bytes(&bytes[..HEADER_CHECKSUM_OFFSET]);
        bytes[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + 32].copy_from_slice(&checksum);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(anyhow!("v2 header is truncated"));
        }
        if &bytes[..8] != FILE_MAGIC {
            return Err(anyhow!("v2 file magic mismatch"));
        }
        if hash_bytes(&bytes[..HEADER_CHECKSUM_OFFSET])
            != bytes[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + 32]
        {
            return Err(anyhow!("v2 header checksum mismatch"));
        }
        let file_format_version =
            u32::from_le_bytes(bytes[8..12].try_into().expect("format version"));
        if file_format_version != FILE_FORMAT_VERSION {
            return Err(anyhow!(
                "unsupported v2 file format version {file_format_version}"
            ));
        }
        let header_len = u32::from_le_bytes(bytes[16..20].try_into().expect("header len"));
        let superblock_len = u32::from_le_bytes(bytes[20..24].try_into().expect("superblock len"));
        let superblock_count =
            u32::from_le_bytes(bytes[24..28].try_into().expect("superblock count"));
        if header_len as usize != HEADER_LEN {
            return Err(anyhow!("unsupported v2 header length {header_len}"));
        }
        if superblock_len as usize != SUPERBLOCK_LEN {
            return Err(anyhow!("unsupported v2 superblock length {superblock_len}"));
        }
        if superblock_count as usize != SUPERBLOCK_COUNT {
            return Err(anyhow!(
                "unsupported v2 superblock count {superblock_count}"
            ));
        }
        Ok(Self {
            page_size: u32::from_le_bytes(bytes[12..16].try_into().expect("page size")),
            superblock_count,
            file_id: bytes[32..48].try_into().expect("file id"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
    pub generation: u64,
    pub durable_lsn: u64,
    pub last_checkpoint_unix_ms: u64,
    pub wal_start_offset: u64,
    pub wal_end_offset: u64,
    pub roots: RootSet,
    pub summary: SummaryCounters,
}

impl Default for Superblock {
    fn default() -> Self {
        Self {
            generation: 1,
            durable_lsn: 0,
            last_checkpoint_unix_ms: 0,
            wal_start_offset: DATA_START_OFFSET,
            wal_end_offset: DATA_START_OFFSET,
            roots: RootSet::default(),
            summary: SummaryCounters::default(),
        }
    }
}

impl Superblock {
    pub fn encode(&self) -> [u8; SUPERBLOCK_LEN] {
        let mut bytes = [0_u8; SUPERBLOCK_LEN];
        bytes[..8].copy_from_slice(SUPERBLOCK_MAGIC);
        bytes[8..16].copy_from_slice(&self.generation.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.durable_lsn.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.last_checkpoint_unix_ms.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.wal_start_offset.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.wal_end_offset.to_le_bytes());
        encode_root_set(
            &self.roots,
            &mut bytes[ROOT_SET_OFFSET..ROOT_SET_OFFSET + 48],
        );
        encode_summary(
            &self.summary,
            &mut bytes[SUMMARY_OFFSET..SUMMARY_OFFSET + 80],
        );
        let checksum = hash_bytes(&bytes[..SUPERBLOCK_CHECKSUM_OFFSET]);
        bytes[SUPERBLOCK_CHECKSUM_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET + 32]
            .copy_from_slice(&checksum);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SUPERBLOCK_LEN {
            return Err(anyhow!("v2 superblock is truncated"));
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(anyhow!("v2 superblock is empty"));
        }
        if &bytes[..8] != SUPERBLOCK_MAGIC {
            return Err(anyhow!("v2 superblock magic mismatch"));
        }
        if hash_bytes(&bytes[..SUPERBLOCK_CHECKSUM_OFFSET])
            != bytes[SUPERBLOCK_CHECKSUM_OFFSET..SUPERBLOCK_CHECKSUM_OFFSET + 32]
        {
            return Err(anyhow!("v2 superblock checksum mismatch"));
        }
        Ok(Self {
            generation: u64::from_le_bytes(bytes[8..16].try_into().expect("generation")),
            durable_lsn: u64::from_le_bytes(bytes[16..24].try_into().expect("durable lsn")),
            last_checkpoint_unix_ms: u64::from_le_bytes(
                bytes[24..32].try_into().expect("checkpoint time"),
            ),
            wal_start_offset: u64::from_le_bytes(bytes[32..40].try_into().expect("wal start")),
            wal_end_offset: u64::from_le_bytes(bytes[40..48].try_into().expect("wal end")),
            roots: decode_root_set(&bytes[ROOT_SET_OFFSET..ROOT_SET_OFFSET + 48]),
            summary: decode_summary(&bytes[SUMMARY_OFFSET..SUMMARY_OFFSET + 80]),
        })
    }
}

fn encode_root_set(roots: &RootSet, bytes: &mut [u8]) {
    bytes[..8].copy_from_slice(&encode_optional_u64(roots.namespace_root_page_id));
    bytes[8..16].copy_from_slice(&encode_optional_u64(roots.change_stream_root_page_id));
    bytes[16..24].copy_from_slice(&encode_optional_u64(roots.plan_cache_root_page_id));
    bytes[24..32].copy_from_slice(&encode_optional_u64(roots.stats_root_page_id));
    bytes[32..40].copy_from_slice(&encode_optional_u64(roots.freelist_root_page_id));
    bytes[40..48].copy_from_slice(&roots.next_page_id.to_le_bytes());
}

fn decode_root_set(bytes: &[u8]) -> RootSet {
    RootSet {
        namespace_root_page_id: decode_optional_u64(bytes[..8].try_into().expect("namespace")),
        change_stream_root_page_id: decode_optional_u64(bytes[8..16].try_into().expect("change")),
        plan_cache_root_page_id: decode_optional_u64(bytes[16..24].try_into().expect("plan")),
        stats_root_page_id: decode_optional_u64(bytes[24..32].try_into().expect("stats")),
        freelist_root_page_id: decode_optional_u64(bytes[32..40].try_into().expect("free")),
        next_page_id: u64::from_le_bytes(bytes[40..48].try_into().expect("next page")),
    }
}

fn encode_summary(summary: &SummaryCounters, bytes: &mut [u8]) {
    let values = [
        summary.database_count,
        summary.collection_count,
        summary.index_count,
        summary.record_count,
        summary.index_entry_count,
        summary.change_event_count,
        summary.plan_cache_entry_count,
        summary.document_bytes,
        summary.index_bytes,
        summary.page_count,
    ];
    for (index, value) in values.into_iter().enumerate() {
        let offset = index * 8;
        bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
}

fn decode_summary(bytes: &[u8]) -> SummaryCounters {
    let decode = |index: usize| -> u64 {
        let offset = index * 8;
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("summary value"))
    };
    SummaryCounters {
        database_count: decode(0),
        collection_count: decode(1),
        index_count: decode(2),
        record_count: decode(3),
        index_entry_count: decode(4),
        change_event_count: decode(5),
        plan_cache_entry_count: decode(6),
        document_bytes: decode(7),
        index_bytes: decode(8),
        page_count: decode(9),
    }
}

fn encode_optional_u64(value: Option<u64>) -> [u8; 8] {
    value.unwrap_or(u64::MAX).to_le_bytes()
}

fn decode_optional_u64(bytes: [u8; 8]) -> Option<u64> {
    match u64::from_le_bytes(bytes) {
        u64::MAX => None,
        value => Some(value),
    }
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::{FileHeader, Superblock};
    use crate::v2::catalog::{RootSet, SummaryCounters};

    #[test]
    fn round_trips_v2_header() {
        let header = FileHeader {
            page_size: 8192,
            superblock_count: 2,
            file_id: [7_u8; 16],
        };

        let encoded = header.encode();
        let decoded = FileHeader::decode(&encoded).expect("decode header");
        assert_eq!(decoded, header);
    }

    #[test]
    fn round_trips_v2_superblock() {
        let superblock = Superblock {
            generation: 9,
            durable_lsn: 42,
            last_checkpoint_unix_ms: 55,
            wal_start_offset: 8192,
            wal_end_offset: 12_288,
            roots: RootSet {
                namespace_root_page_id: Some(11),
                change_stream_root_page_id: Some(12),
                plan_cache_root_page_id: None,
                stats_root_page_id: Some(14),
                freelist_root_page_id: Some(15),
                next_page_id: 16,
            },
            summary: SummaryCounters {
                database_count: 1,
                collection_count: 2,
                index_count: 3,
                record_count: 4,
                index_entry_count: 5,
                change_event_count: 6,
                plan_cache_entry_count: 7,
                document_bytes: 8,
                index_bytes: 9,
                page_count: 10,
            },
        };

        let encoded = superblock.encode();
        let decoded = Superblock::decode(&encoded).expect("decode superblock");
        assert_eq!(decoded, superblock);
    }
}
