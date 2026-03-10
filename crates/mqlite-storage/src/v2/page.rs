use anyhow::{Result, anyhow};
use blake3::Hasher;
use bson::Document;
use ciborium::{de as cbor_de, ser as cbor_ser};
use mqlite_catalog::IndexEntry;

use crate::v2::{
    catalog::{CollectionMeta, IndexMeta, PersistedIndexStats},
    layout::{DEFAULT_PAGE_SIZE, PageKind},
};

pub(crate) type PageId = u64;

const PAGE_MAGIC: &[u8; 8] = b"MQLTPG08";
const PAGE_LEN: usize = DEFAULT_PAGE_SIZE as usize;
const PAGE_HEADER_LEN: usize = 64;
const PAGE_CHECKSUM_OFFSET: usize = 32;
const RECORD_LEAF_SLOT_LEN: usize = 16;
const RECORD_INTERNAL_SLOT_LEN: usize = 16;
const SECONDARY_LEAF_SLOT_LEN: usize = 24;
const SECONDARY_INTERNAL_SLOT_LEN: usize = 20;
const NAMESPACE_LEAF_SLOT_LEN: usize = 12;
const NAMESPACE_INTERNAL_SLOT_LEN: usize = 12;
const META_SLOT_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordSlot {
    pub record_id: u64,
    pub encoded_document: Vec<u8>,
}

impl RecordSlot {
    pub fn from_document(record_id: u64, document: &Document) -> Result<Self> {
        Ok(Self {
            record_id,
            encoded_document: bson::to_vec(document)?,
        })
    }

    pub fn decode_document(&self) -> Result<Document> {
        bson::from_slice(&self.encoded_document).map_err(Into::into)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordLeafPage {
    pub page_id: PageId,
    pub next_page_id: Option<PageId>,
    pub entries: Vec<RecordSlot>,
}

impl RecordLeafPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::RecordLeaf,
            self.page_id,
            self.next_page_id,
            self.entries.len(),
        )?;

        let mut upper = PAGE_LEN;
        let slot_area_end = page_slot_area_end(self.entries.len(), RECORD_LEAF_SLOT_LEN)?;
        for (index, entry) in self.entries.iter().enumerate() {
            upper = upper
                .checked_sub(entry.encoded_document.len())
                .ok_or_else(|| anyhow!("record leaf page {0} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "record leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + entry.encoded_document.len()]
                .copy_from_slice(&entry.encoded_document);

            let slot_offset = PAGE_HEADER_LEN + index * RECORD_LEAF_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8].copy_from_slice(&entry.record_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 10].copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 10..slot_offset + 12]
                .copy_from_slice(&(entry.encoded_document.len() as u16).to_le_bytes());
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::RecordLeaf)?;
        let mut entries = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * RECORD_LEAF_SLOT_LEN;
            let record_id = u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 8..slot_offset + 10].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 10..slot_offset + 12].try_into()?);
            let payload = slice_payload(bytes, payload_offset, payload_len)?;
            entries.push(RecordSlot {
                record_id,
                encoded_document: payload.to_vec(),
            });
        }

        Ok(Self {
            page_id: header.page_id,
            next_page_id: header.aux_page_id,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordSeparator {
    pub record_id: u64,
    pub child_page_id: PageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordInternalPage {
    pub page_id: PageId,
    pub first_child_page_id: PageId,
    pub separators: Vec<RecordSeparator>,
}

impl RecordInternalPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::RecordInternal,
            self.page_id,
            Some(self.first_child_page_id),
            self.separators.len(),
        )?;
        let slot_area_end = page_slot_area_end(self.separators.len(), RECORD_INTERNAL_SLOT_LEN)?;
        if slot_area_end > PAGE_LEN {
            return Err(anyhow!(
                "record internal page {} exceeds page capacity",
                self.page_id
            ));
        }
        for (index, separator) in self.separators.iter().enumerate() {
            let slot_offset = PAGE_HEADER_LEN + index * RECORD_INTERNAL_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8]
                .copy_from_slice(&separator.child_page_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 16]
                .copy_from_slice(&separator.record_id.to_le_bytes());
        }
        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::RecordInternal)?;
        let first_child_page_id = header.aux_page_id.ok_or_else(|| {
            anyhow!(
                "record internal page {} is missing first child",
                header.page_id
            )
        })?;
        let mut separators = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * RECORD_INTERNAL_SLOT_LEN;
            separators.push(RecordSeparator {
                child_page_id: u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?),
                record_id: u64::from_le_bytes(bytes[slot_offset + 8..slot_offset + 16].try_into()?),
            });
        }
        Ok(Self {
            page_id: header.page_id,
            first_child_page_id,
            separators,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryEntry {
    pub record_id: u64,
    pub key: Document,
    pub present_mask: u64,
}

impl SecondaryEntry {
    pub fn from_index_entry(entry: &IndexEntry, key_pattern: &Document) -> Result<Self> {
        Ok(Self {
            record_id: entry.record_id,
            key: entry.key.clone(),
            present_mask: present_mask_for_fields(key_pattern, &entry.present_fields)?,
        })
    }

    pub fn into_index_entry(self, key_pattern: &Document) -> IndexEntry {
        IndexEntry {
            record_id: self.record_id,
            key: self.key,
            present_fields: present_fields_from_mask(key_pattern, self.present_mask),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryLeafPage {
    pub page_id: PageId,
    pub next_page_id: Option<PageId>,
    pub entries: Vec<SecondaryEntry>,
}

impl SecondaryLeafPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::SecondaryLeaf,
            self.page_id,
            self.next_page_id,
            self.entries.len(),
        )?;

        let mut upper = PAGE_LEN;
        let slot_area_end = page_slot_area_end(self.entries.len(), SECONDARY_LEAF_SLOT_LEN)?;
        for (index, entry) in self.entries.iter().enumerate() {
            let encoded_key = bson::to_vec(&entry.key)?;
            upper = upper
                .checked_sub(encoded_key.len())
                .ok_or_else(|| anyhow!("secondary leaf page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + encoded_key.len()].copy_from_slice(&encoded_key);

            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_LEAF_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8].copy_from_slice(&entry.record_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 16]
                .copy_from_slice(&entry.present_mask.to_le_bytes());
            bytes[slot_offset + 16..slot_offset + 18]
                .copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 18..slot_offset + 20]
                .copy_from_slice(&(encoded_key.len() as u16).to_le_bytes());
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::SecondaryLeaf)?;
        let mut entries = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_LEAF_SLOT_LEN;
            let record_id = u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let present_mask =
                u64::from_le_bytes(bytes[slot_offset + 8..slot_offset + 16].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 16..slot_offset + 18].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 18..slot_offset + 20].try_into()?);
            entries.push(SecondaryEntry {
                record_id,
                present_mask,
                key: bson::from_slice(slice_payload(bytes, payload_offset, payload_len)?)?,
            });
        }

        Ok(Self {
            page_id: header.page_id,
            next_page_id: header.aux_page_id,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondarySeparator {
    pub key: Document,
    pub record_id: u64,
    pub child_page_id: PageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryInternalPage {
    pub page_id: PageId,
    pub first_child_page_id: PageId,
    pub separators: Vec<SecondarySeparator>,
}

impl SecondaryInternalPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::SecondaryInternal,
            self.page_id,
            Some(self.first_child_page_id),
            self.separators.len(),
        )?;

        let mut upper = PAGE_LEN;
        let slot_area_end = page_slot_area_end(self.separators.len(), SECONDARY_INTERNAL_SLOT_LEN)?;
        for (index, separator) in self.separators.iter().enumerate() {
            let encoded_key = bson::to_vec(&separator.key)?;
            upper = upper
                .checked_sub(encoded_key.len())
                .ok_or_else(|| anyhow!("secondary internal page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary internal page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + encoded_key.len()].copy_from_slice(&encoded_key);

            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_INTERNAL_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8]
                .copy_from_slice(&separator.child_page_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 16]
                .copy_from_slice(&separator.record_id.to_le_bytes());
            bytes[slot_offset + 16..slot_offset + 18]
                .copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 18..slot_offset + 20]
                .copy_from_slice(&(encoded_key.len() as u16).to_le_bytes());
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::SecondaryInternal)?;
        let first_child_page_id = header.aux_page_id.ok_or_else(|| {
            anyhow!(
                "secondary internal page {} is missing first child",
                header.page_id
            )
        })?;
        let mut separators = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_INTERNAL_SLOT_LEN;
            let child_page_id = u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let record_id =
                u64::from_le_bytes(bytes[slot_offset + 8..slot_offset + 16].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 16..slot_offset + 18].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 18..slot_offset + 20].try_into()?);
            separators.push(SecondarySeparator {
                child_page_id,
                record_id,
                key: bson::from_slice(slice_payload(bytes, payload_offset, payload_len)?)?,
            });
        }

        Ok(Self {
            page_id: header.page_id,
            first_child_page_id,
            separators,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceEntry {
    pub name: String,
    pub target_page_id: PageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceLeafPage {
    pub page_id: PageId,
    pub next_page_id: Option<PageId>,
    pub entries: Vec<NamespaceEntry>,
}

impl NamespaceLeafPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::NamespaceLeaf,
            self.page_id,
            self.next_page_id,
            self.entries.len(),
        )?;

        let mut upper = PAGE_LEN;
        let slot_area_end = page_slot_area_end(self.entries.len(), NAMESPACE_LEAF_SLOT_LEN)?;
        for (index, entry) in self.entries.iter().enumerate() {
            let name_bytes = entry.name.as_bytes();
            upper = upper
                .checked_sub(name_bytes.len())
                .ok_or_else(|| anyhow!("namespace leaf page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "namespace leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + name_bytes.len()].copy_from_slice(name_bytes);

            let slot_offset = PAGE_HEADER_LEN + index * NAMESPACE_LEAF_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8]
                .copy_from_slice(&entry.target_page_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 10].copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 10..slot_offset + 12]
                .copy_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::NamespaceLeaf)?;
        let mut entries = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * NAMESPACE_LEAF_SLOT_LEN;
            let target_page_id =
                u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 8..slot_offset + 10].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 10..slot_offset + 12].try_into()?);
            entries.push(NamespaceEntry {
                target_page_id,
                name: String::from_utf8(
                    slice_payload(bytes, payload_offset, payload_len)?.to_vec(),
                )?,
            });
        }

        Ok(Self {
            page_id: header.page_id,
            next_page_id: header.aux_page_id,
            entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceSeparator {
    pub name: String,
    pub child_page_id: PageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceInternalPage {
    pub page_id: PageId,
    pub first_child_page_id: PageId,
    pub separators: Vec<NamespaceSeparator>,
}

impl NamespaceInternalPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut bytes = [0_u8; PAGE_LEN];
        encode_header(
            &mut bytes,
            PageKind::NamespaceInternal,
            self.page_id,
            Some(self.first_child_page_id),
            self.separators.len(),
        )?;

        let mut upper = PAGE_LEN;
        let slot_area_end = page_slot_area_end(self.separators.len(), NAMESPACE_INTERNAL_SLOT_LEN)?;
        for (index, separator) in self.separators.iter().enumerate() {
            let name_bytes = separator.name.as_bytes();
            upper = upper
                .checked_sub(name_bytes.len())
                .ok_or_else(|| anyhow!("namespace internal page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "namespace internal page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + name_bytes.len()].copy_from_slice(name_bytes);

            let slot_offset = PAGE_HEADER_LEN + index * NAMESPACE_INTERNAL_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8]
                .copy_from_slice(&separator.child_page_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 10].copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 10..slot_offset + 12]
                .copy_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::NamespaceInternal)?;
        let first_child_page_id = header.aux_page_id.ok_or_else(|| {
            anyhow!(
                "namespace internal page {} is missing first child",
                header.page_id
            )
        })?;
        let mut separators = Vec::with_capacity(header.cell_count);
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * NAMESPACE_INTERNAL_SLOT_LEN;
            let child_page_id = u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 8..slot_offset + 10].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 10..slot_offset + 12].try_into()?);
            separators.push(NamespaceSeparator {
                child_page_id,
                name: String::from_utf8(
                    slice_payload(bytes, payload_offset, payload_len)?.to_vec(),
                )?,
            });
        }

        Ok(Self {
            page_id: header.page_id,
            first_child_page_id,
            separators,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionMetaPage {
    pub page_id: PageId,
    pub meta: CollectionMeta,
}

impl CollectionMetaPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut payload = Vec::new();
        cbor_ser::into_writer(&self.meta, &mut payload)?;
        encode_single_payload_page(PageKind::CollectionMeta, self.page_id, &payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::CollectionMeta)?;
        let payload = decode_single_payload(bytes)?;
        Ok(Self {
            page_id: header.page_id,
            meta: cbor_de::from_reader(payload)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexMetaPage {
    pub page_id: PageId,
    pub meta: IndexMeta,
}

impl IndexMetaPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut payload = Vec::new();
        cbor_ser::into_writer(&self.meta, &mut payload)?;
        encode_single_payload_page(PageKind::IndexMeta, self.page_id, &payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::IndexMeta)?;
        let payload = decode_single_payload(bytes)?;
        Ok(Self {
            page_id: header.page_id,
            meta: cbor_de::from_reader(payload)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatsPage {
    pub page_id: PageId,
    pub stats: PersistedIndexStats,
}

impl StatsPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut payload = Vec::new();
        cbor_ser::into_writer(&self.stats, &mut payload)?;
        encode_single_payload_page(PageKind::Stats, self.page_id, &payload)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::Stats)?;
        let payload = decode_single_payload(bytes)?;
        Ok(Self {
            page_id: header.page_id,
            stats: cbor_de::from_reader(payload)?,
        })
    }
}

pub(crate) fn page_kind(bytes: &[u8]) -> Result<PageKind> {
    if bytes.len() < PAGE_LEN {
        return Err(anyhow!("v2 page is truncated"));
    }
    if &bytes[..8] != PAGE_MAGIC {
        return Err(anyhow!("v2 page magic mismatch"));
    }
    if page_checksum(bytes) != bytes[PAGE_CHECKSUM_OFFSET..PAGE_HEADER_LEN] {
        return Err(anyhow!("v2 page checksum mismatch"));
    }
    PageKind::try_from(u16::from_le_bytes(bytes[8..10].try_into()?))
}

fn page_slot_area_end(entry_count: usize, slot_len: usize) -> Result<usize> {
    PAGE_HEADER_LEN
        .checked_add(entry_count.saturating_mul(slot_len))
        .ok_or_else(|| anyhow!("page slot area overflows"))
}

fn slice_payload(bytes: &[u8], payload_offset: u16, payload_len: u16) -> Result<&[u8]> {
    let start = payload_offset as usize;
    let end = start
        .checked_add(payload_len as usize)
        .ok_or_else(|| anyhow!("page payload length overflows"))?;
    if start < PAGE_HEADER_LEN || end > bytes.len() {
        return Err(anyhow!("page payload points outside the page"));
    }
    Ok(&bytes[start..end])
}

fn encode_single_payload_page(
    page_kind: PageKind,
    page_id: PageId,
    payload: &[u8],
) -> Result<[u8; PAGE_LEN]> {
    let mut bytes = [0_u8; PAGE_LEN];
    encode_header(&mut bytes, page_kind, page_id, None, 1)?;
    let slot_area_end = page_slot_area_end(1, META_SLOT_LEN)?;
    let payload_offset = PAGE_LEN
        .checked_sub(payload.len())
        .ok_or_else(|| anyhow!("page {page_id} overflows"))?;
    if payload_offset < slot_area_end {
        return Err(anyhow!("page {page_id} exceeds page capacity"));
    }
    bytes[payload_offset..payload_offset + payload.len()].copy_from_slice(payload);
    bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 2]
        .copy_from_slice(&(payload_offset as u16).to_le_bytes());
    bytes[PAGE_HEADER_LEN + 2..PAGE_HEADER_LEN + 4]
        .copy_from_slice(&(payload.len() as u16).to_le_bytes());
    finalize_checksum(&mut bytes);
    Ok(bytes)
}

fn decode_single_payload(bytes: &[u8]) -> Result<&[u8]> {
    let payload_offset =
        u16::from_le_bytes(bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 2].try_into()?);
    let payload_len =
        u16::from_le_bytes(bytes[PAGE_HEADER_LEN + 2..PAGE_HEADER_LEN + 4].try_into()?);
    slice_payload(bytes, payload_offset, payload_len)
}

fn present_mask_for_fields(key_pattern: &Document, present_fields: &[String]) -> Result<u64> {
    if key_pattern.len() > u64::BITS as usize {
        return Err(anyhow!(
            "v2 presence masks support at most {} key fields",
            u64::BITS
        ));
    }

    let mut mask = 0_u64;
    for (index, field) in key_pattern.keys().enumerate() {
        if present_fields.iter().any(|candidate| candidate == field) {
            mask |= 1_u64 << index;
        }
    }
    Ok(mask)
}

fn present_fields_from_mask(key_pattern: &Document, present_mask: u64) -> Vec<String> {
    key_pattern
        .keys()
        .enumerate()
        .filter(|(index, _)| present_mask & (1_u64 << index) != 0)
        .map(|(_, field)| field.clone())
        .collect()
}

fn encode_header(
    bytes: &mut [u8; PAGE_LEN],
    page_kind: PageKind,
    page_id: PageId,
    aux_page_id: Option<PageId>,
    cell_count: usize,
) -> Result<()> {
    if cell_count > u16::MAX as usize {
        return Err(anyhow!("page {page_id} contains too many cells"));
    }

    bytes[..8].copy_from_slice(PAGE_MAGIC);
    bytes[8..10].copy_from_slice(&(page_kind as u16).to_le_bytes());
    bytes[12..20].copy_from_slice(&page_id.to_le_bytes());
    bytes[20..28].copy_from_slice(&encode_optional_page_id(aux_page_id));
    bytes[28..30].copy_from_slice(&(cell_count as u16).to_le_bytes());
    Ok(())
}

fn finalize_checksum(bytes: &mut [u8; PAGE_LEN]) {
    let checksum = page_checksum(bytes);
    bytes[PAGE_CHECKSUM_OFFSET..PAGE_HEADER_LEN].copy_from_slice(&checksum);
}

fn page_checksum(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..PAGE_CHECKSUM_OFFSET]);
    hasher.update(&bytes[PAGE_HEADER_LEN..]);
    *hasher.finalize().as_bytes()
}

fn encode_optional_page_id(value: Option<PageId>) -> [u8; 8] {
    value.unwrap_or(u64::MAX).to_le_bytes()
}

fn decode_optional_page_id(bytes: [u8; 8]) -> Option<PageId> {
    match u64::from_le_bytes(bytes) {
        u64::MAX => None,
        value => Some(value),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodedHeader {
    page_id: PageId,
    aux_page_id: Option<PageId>,
    cell_count: usize,
}

fn decode_header(bytes: &[u8], expected_kind: PageKind) -> Result<DecodedHeader> {
    let page_kind = page_kind(bytes)?;
    if page_kind != expected_kind {
        return Err(anyhow!(
            "expected {:?} page, found {:?}",
            expected_kind,
            page_kind
        ));
    }

    Ok(DecodedHeader {
        page_id: u64::from_le_bytes(bytes[12..20].try_into()?),
        aux_page_id: decode_optional_page_id(bytes[20..28].try_into()?),
        cell_count: u16::from_le_bytes(bytes[28..30].try_into()?) as usize,
    })
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use mqlite_catalog::IndexEntry;

    use super::{
        CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceInternalPage,
        NamespaceLeafPage, NamespaceSeparator, RecordInternalPage, RecordLeafPage, RecordSeparator,
        RecordSlot, SecondaryEntry, SecondaryInternalPage, SecondaryLeafPage, SecondarySeparator,
        StatsPage,
    };
    use crate::v2::catalog::{
        CollectionMeta, IndexMeta, PersistedIndexStats, PersistedValueFrequency, SummaryCounters,
    };

    #[test]
    fn round_trips_record_pages() {
        let leaf = RecordLeafPage {
            page_id: 1,
            next_page_id: Some(2),
            entries: vec![
                RecordSlot::from_document(11, &doc! { "_id": 1, "sku": "a" }).expect("record"),
                RecordSlot::from_document(12, &doc! { "_id": 2, "sku": "b" }).expect("record"),
            ],
        };
        let decoded_leaf =
            RecordLeafPage::decode(&leaf.encode().expect("encode leaf")).expect("decode leaf");
        assert_eq!(decoded_leaf, leaf);

        let internal = RecordInternalPage {
            page_id: 5,
            first_child_page_id: 1,
            separators: vec![RecordSeparator {
                record_id: 12,
                child_page_id: 2,
            }],
        };
        let decoded_internal =
            RecordInternalPage::decode(&internal.encode().expect("encode internal"))
                .expect("decode internal");
        assert_eq!(decoded_internal, internal);
    }

    #[test]
    fn round_trips_secondary_pages() {
        let key_pattern = doc! { "sku": 1, "qty": -1 };
        let leaf = SecondaryLeafPage {
            page_id: 3,
            next_page_id: Some(4),
            entries: vec![
                SecondaryEntry::from_index_entry(
                    &IndexEntry {
                        record_id: 11,
                        key: doc! { "sku": "a", "qty": 3 },
                        present_fields: vec!["sku".to_string()],
                    },
                    &key_pattern,
                )
                .expect("entry"),
                SecondaryEntry::from_index_entry(
                    &IndexEntry {
                        record_id: 12,
                        key: doc! { "sku": "b", "qty": 4 },
                        present_fields: vec!["sku".to_string(), "qty".to_string()],
                    },
                    &key_pattern,
                )
                .expect("entry"),
            ],
        };
        let decoded_leaf =
            SecondaryLeafPage::decode(&leaf.encode().expect("encode leaf")).expect("decode leaf");
        assert_eq!(decoded_leaf, leaf);

        let internal = SecondaryInternalPage {
            page_id: 6,
            first_child_page_id: 3,
            separators: vec![SecondarySeparator {
                key: doc! { "sku": "b", "qty": 4 },
                record_id: 12,
                child_page_id: 4,
            }],
        };
        let decoded_internal =
            SecondaryInternalPage::decode(&internal.encode().expect("encode internal"))
                .expect("decode internal");
        assert_eq!(decoded_internal, internal);
    }

    #[test]
    fn round_trips_namespace_pages() {
        let leaf = NamespaceLeafPage {
            page_id: 7,
            next_page_id: Some(8),
            entries: vec![
                NamespaceEntry {
                    name: "app.widgets".to_string(),
                    target_page_id: 21,
                },
                NamespaceEntry {
                    name: "app.widgets:sku_1".to_string(),
                    target_page_id: 22,
                },
            ],
        };
        let decoded_leaf =
            NamespaceLeafPage::decode(&leaf.encode().expect("encode leaf")).expect("decode leaf");
        assert_eq!(decoded_leaf, leaf);

        let internal = NamespaceInternalPage {
            page_id: 9,
            first_child_page_id: 7,
            separators: vec![NamespaceSeparator {
                name: "app.widgets".to_string(),
                child_page_id: 8,
            }],
        };
        let decoded_internal =
            NamespaceInternalPage::decode(&internal.encode().expect("encode internal"))
                .expect("decode internal");
        assert_eq!(decoded_internal, internal);
    }

    #[test]
    fn round_trips_meta_pages() {
        let collection = CollectionMetaPage {
            page_id: 11,
            meta: CollectionMeta {
                database: "app".to_string(),
                collection: "widgets".to_string(),
                record_root_page_id: Some(31),
                index_directory_root_page_id: Some(32),
                options_bytes: bson::to_vec(&doc! { "capped": false }).expect("options"),
                next_record_id: 17,
                summary: SummaryCounters {
                    record_count: 4,
                    index_count: 2,
                    ..SummaryCounters::default()
                },
            },
        };
        let decoded_collection =
            CollectionMetaPage::decode(&collection.encode().expect("encode collection meta"))
                .expect("decode collection meta");
        assert_eq!(decoded_collection, collection);

        let index = IndexMetaPage {
            page_id: 12,
            meta: IndexMeta {
                name: "sku_1".to_string(),
                root_page_id: Some(41),
                key_pattern_bytes: bson::to_vec(&doc! { "sku": 1 }).expect("pattern"),
                unique: true,
                expire_after_seconds: None,
                entry_count: 4,
                index_bytes: 256,
                stats_page_id: Some(13),
            },
        };
        let decoded_index = IndexMetaPage::decode(&index.encode().expect("encode index meta"))
            .expect("decode index meta");
        assert_eq!(decoded_index, index);

        let stats = StatsPage {
            page_id: 13,
            stats: PersistedIndexStats {
                entry_count: 4,
                present_fields: [("sku".to_string(), 4)].into_iter().collect(),
                value_frequencies: [(
                    "sku".to_string(),
                    vec![PersistedValueFrequency {
                        encoded_value: bson::to_vec(&doc! { "v": "alpha" }).expect("value"),
                        count: 4,
                    }],
                )]
                .into_iter()
                .collect(),
            },
        };
        let decoded_stats =
            StatsPage::decode(&stats.encode().expect("encode stats")).expect("decode stats");
        assert_eq!(decoded_stats, stats);
    }
}
