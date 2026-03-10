use anyhow::{Result, anyhow};
use blake3::Hasher;
use bson::Document;
use ciborium::{de as cbor_de, ser as cbor_ser};
use mqlite_catalog::IndexEntry;

use crate::v2::{
    catalog::{CollectionMeta, IndexMeta, PersistedIndexStats},
    keycodec::encode_index_key,
    layout::{DEFAULT_PAGE_SIZE, PageKind},
};
use crate::{PersistedChangeEvent, PersistedPlanCacheEntry};

pub(crate) type PageId = u64;

const PAGE_MAGIC: &[u8; 8] = b"MQLTPG09";
const PAGE_LEN: usize = DEFAULT_PAGE_SIZE as usize;
const PAGE_HEADER_LEN: usize = 64;
const PAGE_CHECKSUM_OFFSET: usize = 32;
const RECORD_LEAF_SLOT_LEN: usize = 16;
const RECORD_INTERNAL_SLOT_LEN: usize = 16;
const SECONDARY_LEAF_SLOT_LEN: usize = 16;
const SECONDARY_INTERNAL_SLOT_LEN: usize = 24;
const SECONDARY_POSTING_LEN: usize = 16;
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
pub(crate) struct SecondaryPosting {
    pub record_id: u64,
    pub present_mask: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryEntry {
    pub key_bytes: Vec<u8>,
    pub normalized_key: Vec<u8>,
    pub postings: Vec<SecondaryPosting>,
}

impl SecondaryEntry {
    pub fn from_index_entry(entry: &IndexEntry, key_pattern: &Document) -> Result<Self> {
        Ok(Self {
            key_bytes: bson::to_vec(&entry.key)?,
            normalized_key: encode_index_key(&entry.key, key_pattern)?,
            postings: vec![SecondaryPosting {
                record_id: entry.record_id,
                present_mask: present_mask_for_fields(key_pattern, &entry.present_fields)?,
            }],
        })
    }

    pub fn from_index_entries(entries: &[IndexEntry], key_pattern: &Document) -> Result<Self> {
        let first = entries
            .first()
            .ok_or_else(|| anyhow!("secondary entry group cannot be empty"))?;
        Ok(Self {
            key_bytes: bson::to_vec(&first.key)?,
            normalized_key: encode_index_key(&first.key, key_pattern)?,
            postings: entries
                .iter()
                .map(|entry| {
                    Ok(SecondaryPosting {
                        record_id: entry.record_id,
                        present_mask: present_mask_for_fields(key_pattern, &entry.present_fields)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn from_key_document(
        key: &Document,
        postings: Vec<SecondaryPosting>,
        key_pattern: &Document,
    ) -> Result<Self> {
        Ok(Self {
            key_bytes: bson::to_vec(key)?,
            normalized_key: encode_index_key(key, key_pattern)?,
            postings,
        })
    }

    pub fn decode_key_document(&self) -> Result<Document> {
        bson::from_slice(&self.key_bytes).map_err(Into::into)
    }

    pub fn first_record_id(&self) -> u64 {
        self.postings
            .first()
            .map(|posting| posting.record_id)
            .unwrap_or_default()
    }

    pub fn posting_count(&self) -> usize {
        self.postings.len()
    }

    pub fn into_index_entries(self, key_pattern: &Document) -> Result<Vec<IndexEntry>> {
        let key = bson::from_slice::<Document>(&self.key_bytes)?;
        Ok(self
            .postings
            .into_iter()
            .map(|posting| IndexEntry {
                record_id: posting.record_id,
                key: key.clone(),
                present_fields: present_fields_from_mask(key_pattern, posting.present_mask),
            })
            .collect())
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
        let mut previous_key = Vec::new();
        for (index, entry) in self.entries.iter().enumerate() {
            let shared_prefix_len = shared_prefix_len(&previous_key, &entry.normalized_key)?;
            let normalized_suffix = &entry.normalized_key[shared_prefix_len as usize..];
            let postings_len = entry
                .postings
                .len()
                .checked_mul(SECONDARY_POSTING_LEN)
                .ok_or_else(|| anyhow!("secondary leaf page {} overflows", self.page_id))?;

            upper = upper
                .checked_sub(postings_len)
                .ok_or_else(|| anyhow!("secondary leaf page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            let postings_offset = upper;
            encode_secondary_postings(
                &entry.postings,
                &mut bytes[postings_offset..postings_offset + postings_len],
            );

            upper = upper
                .checked_sub(entry.key_bytes.len())
                .ok_or_else(|| anyhow!("secondary leaf page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + entry.key_bytes.len()].copy_from_slice(&entry.key_bytes);
            let key_offset = upper;

            upper = upper
                .checked_sub(normalized_suffix.len())
                .ok_or_else(|| anyhow!("secondary leaf page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary leaf page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + normalized_suffix.len()].copy_from_slice(normalized_suffix);
            let normalized_offset = upper;

            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_LEAF_SLOT_LEN;
            bytes[slot_offset..slot_offset + 2].copy_from_slice(&shared_prefix_len.to_le_bytes());
            bytes[slot_offset + 2..slot_offset + 4]
                .copy_from_slice(&(normalized_offset as u16).to_le_bytes());
            bytes[slot_offset + 4..slot_offset + 6]
                .copy_from_slice(&(normalized_suffix.len() as u16).to_le_bytes());
            bytes[slot_offset + 6..slot_offset + 8]
                .copy_from_slice(&(key_offset as u16).to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 10]
                .copy_from_slice(&(entry.key_bytes.len() as u16).to_le_bytes());
            bytes[slot_offset + 10..slot_offset + 12]
                .copy_from_slice(&(postings_offset as u16).to_le_bytes());
            bytes[slot_offset + 12..slot_offset + 14]
                .copy_from_slice(&(entry.postings.len() as u16).to_le_bytes());
            previous_key = entry.normalized_key.clone();
        }

        finalize_checksum(&mut bytes);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::SecondaryLeaf)?;
        let mut entries = Vec::with_capacity(header.cell_count);
        let mut previous_key = Vec::new();
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_LEAF_SLOT_LEN;
            let shared_prefix_len =
                u16::from_le_bytes(bytes[slot_offset..slot_offset + 2].try_into()?);
            let normalized_offset =
                u16::from_le_bytes(bytes[slot_offset + 2..slot_offset + 4].try_into()?);
            let normalized_len =
                u16::from_le_bytes(bytes[slot_offset + 4..slot_offset + 6].try_into()?);
            let key_offset =
                u16::from_le_bytes(bytes[slot_offset + 6..slot_offset + 8].try_into()?);
            let key_len = u16::from_le_bytes(bytes[slot_offset + 8..slot_offset + 10].try_into()?);
            let postings_offset =
                u16::from_le_bytes(bytes[slot_offset + 10..slot_offset + 12].try_into()?);
            let posting_count =
                u16::from_le_bytes(bytes[slot_offset + 12..slot_offset + 14].try_into()?);
            let mut normalized_key = previous_key
                .get(..shared_prefix_len as usize)
                .ok_or_else(|| anyhow!("secondary leaf page shared prefix exceeds previous key"))?
                .to_vec();
            normalized_key.extend_from_slice(slice_payload(
                bytes,
                normalized_offset,
                normalized_len,
            )?);
            let postings_len = posting_count as usize * SECONDARY_POSTING_LEN;
            entries.push(SecondaryEntry {
                key_bytes: slice_payload(bytes, key_offset, key_len)?.to_vec(),
                normalized_key: normalized_key.clone(),
                postings: decode_secondary_postings(slice_payload(
                    bytes,
                    postings_offset,
                    postings_len as u16,
                )?)?,
            });
            previous_key = normalized_key;
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
    pub normalized_key: Vec<u8>,
    pub record_id: u64,
    pub child_page_id: PageId,
}

impl SecondarySeparator {
    pub fn from_entry(entry: &SecondaryEntry, child_page_id: PageId) -> Self {
        Self {
            normalized_key: entry.normalized_key.clone(),
            record_id: entry.first_record_id(),
            child_page_id,
        }
    }

    pub fn from_index_entry(
        entry: &IndexEntry,
        child_page_id: PageId,
        key_pattern: &Document,
    ) -> Result<Self> {
        Ok(Self {
            normalized_key: encode_index_key(&entry.key, key_pattern)?,
            record_id: entry.record_id,
            child_page_id,
        })
    }
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
        let mut previous_key = Vec::new();
        for (index, separator) in self.separators.iter().enumerate() {
            let shared_prefix_len = shared_prefix_len(&previous_key, &separator.normalized_key)?;
            let normalized_suffix = &separator.normalized_key[shared_prefix_len as usize..];
            upper = upper
                .checked_sub(normalized_suffix.len())
                .ok_or_else(|| anyhow!("secondary internal page {} overflows", self.page_id))?;
            if upper < slot_area_end {
                return Err(anyhow!(
                    "secondary internal page {} exceeds page capacity",
                    self.page_id
                ));
            }
            bytes[upper..upper + normalized_suffix.len()].copy_from_slice(normalized_suffix);

            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_INTERNAL_SLOT_LEN;
            bytes[slot_offset..slot_offset + 8]
                .copy_from_slice(&separator.child_page_id.to_le_bytes());
            bytes[slot_offset + 8..slot_offset + 16]
                .copy_from_slice(&separator.record_id.to_le_bytes());
            bytes[slot_offset + 16..slot_offset + 18]
                .copy_from_slice(&shared_prefix_len.to_le_bytes());
            bytes[slot_offset + 18..slot_offset + 20]
                .copy_from_slice(&(upper as u16).to_le_bytes());
            bytes[slot_offset + 20..slot_offset + 22]
                .copy_from_slice(&(normalized_suffix.len() as u16).to_le_bytes());
            previous_key = separator.normalized_key.clone();
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
        let mut previous_key = Vec::new();
        for index in 0..header.cell_count {
            let slot_offset = PAGE_HEADER_LEN + index * SECONDARY_INTERNAL_SLOT_LEN;
            let child_page_id = u64::from_le_bytes(bytes[slot_offset..slot_offset + 8].try_into()?);
            let record_id =
                u64::from_le_bytes(bytes[slot_offset + 8..slot_offset + 16].try_into()?);
            let shared_prefix_len =
                u16::from_le_bytes(bytes[slot_offset + 16..slot_offset + 18].try_into()?);
            let payload_offset =
                u16::from_le_bytes(bytes[slot_offset + 18..slot_offset + 20].try_into()?);
            let payload_len =
                u16::from_le_bytes(bytes[slot_offset + 20..slot_offset + 22].try_into()?);
            let mut normalized_key = previous_key
                .get(..shared_prefix_len as usize)
                .ok_or_else(|| {
                    anyhow!("secondary internal page shared prefix exceeds previous key")
                })?
                .to_vec();
            normalized_key.extend_from_slice(slice_payload(bytes, payload_offset, payload_len)?);
            separators.push(SecondarySeparator {
                child_page_id,
                record_id,
                normalized_key: normalized_key.clone(),
            });
            previous_key = normalized_key;
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
        encode_single_payload_page(PageKind::CollectionMeta, self.page_id, None, &payload)
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
        encode_single_payload_page(PageKind::IndexMeta, self.page_id, None, &payload)
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
        encode_single_payload_page(PageKind::Stats, self.page_id, None, &payload)
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChangeEventsPage {
    pub page_id: PageId,
    pub next_page_id: Option<PageId>,
    pub events: Vec<PersistedChangeEvent>,
}

impl ChangeEventsPage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut payload = Vec::new();
        cbor_ser::into_writer(&self.events, &mut payload)?;
        encode_single_payload_page(
            PageKind::ChangeEventLeaf,
            self.page_id,
            self.next_page_id,
            &payload,
        )
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::ChangeEventLeaf)?;
        let payload = decode_single_payload(bytes)?;
        Ok(Self {
            page_id: header.page_id,
            next_page_id: header.aux_page_id,
            events: cbor_de::from_reader(payload)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlanCachePage {
    pub page_id: PageId,
    pub next_page_id: Option<PageId>,
    pub entries: Vec<PersistedPlanCacheEntry>,
}

impl PlanCachePage {
    pub fn encode(&self) -> Result<[u8; PAGE_LEN]> {
        let mut payload = Vec::new();
        cbor_ser::into_writer(&self.entries, &mut payload)?;
        encode_single_payload_page(
            PageKind::PlanCache,
            self.page_id,
            self.next_page_id,
            &payload,
        )
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = decode_header(bytes, PageKind::PlanCache)?;
        let payload = decode_single_payload(bytes)?;
        Ok(Self {
            page_id: header.page_id,
            next_page_id: header.aux_page_id,
            entries: cbor_de::from_reader(payload)?,
        })
    }
}

pub(crate) fn validate_page(bytes: &[u8]) -> Result<PageKind> {
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

pub(crate) fn page_kind(bytes: &[u8]) -> Result<PageKind> {
    validate_page(bytes)
}

pub(crate) fn page_kind_unchecked(bytes: &[u8]) -> Result<PageKind> {
    if bytes.len() < PAGE_LEN {
        return Err(anyhow!("v2 page is truncated"));
    }
    if &bytes[..8] != PAGE_MAGIC {
        return Err(anyhow!("v2 page magic mismatch"));
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
    aux_page_id: Option<PageId>,
    payload: &[u8],
) -> Result<[u8; PAGE_LEN]> {
    let mut bytes = [0_u8; PAGE_LEN];
    encode_header(&mut bytes, page_kind, page_id, aux_page_id, 1)?;
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

fn shared_prefix_len(left: &[u8], right: &[u8]) -> Result<u16> {
    let len = left
        .iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count();
    u16::try_from(len).map_err(|_| anyhow!("secondary key prefix exceeds u16::MAX"))
}

fn encode_secondary_postings(postings: &[SecondaryPosting], bytes: &mut [u8]) {
    for (index, posting) in postings.iter().enumerate() {
        let slot_offset = index * SECONDARY_POSTING_LEN;
        bytes[slot_offset..slot_offset + 8].copy_from_slice(&posting.record_id.to_le_bytes());
        bytes[slot_offset + 8..slot_offset + 16]
            .copy_from_slice(&posting.present_mask.to_le_bytes());
    }
}

fn decode_secondary_postings(bytes: &[u8]) -> Result<Vec<SecondaryPosting>> {
    if bytes.len() % SECONDARY_POSTING_LEN != 0 {
        return Err(anyhow!("secondary posting payload is misaligned"));
    }
    let mut postings = Vec::with_capacity(bytes.len() / SECONDARY_POSTING_LEN);
    for chunk in bytes.chunks_exact(SECONDARY_POSTING_LEN) {
        postings.push(SecondaryPosting {
            record_id: u64::from_le_bytes(chunk[..8].try_into()?),
            present_mask: u64::from_le_bytes(chunk[8..16].try_into()?),
        });
    }
    Ok(postings)
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
    let page_kind = page_kind_unchecked(bytes)?;
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
        ChangeEventsPage, CollectionMetaPage, IndexMetaPage, NamespaceEntry, NamespaceInternalPage,
        NamespaceLeafPage, NamespaceSeparator, PlanCachePage, RecordInternalPage, RecordLeafPage,
        RecordSeparator, RecordSlot, SecondaryEntry, SecondaryInternalPage, SecondaryLeafPage,
        SecondarySeparator, StatsPage,
    };
    use crate::v2::catalog::{
        CollectionMeta, IndexMeta, PersistedIndexStats, PersistedValueFrequency, SummaryCounters,
    };
    use crate::{PersistedChangeEvent, PersistedPlanCacheChoice, PersistedPlanCacheEntry};

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
                SecondaryEntry::from_index_entries(
                    &[
                        IndexEntry {
                            record_id: 11,
                            key: doc! { "sku": "a", "qty": 3 },
                            present_fields: vec!["sku".to_string()],
                        },
                        IndexEntry {
                            record_id: 12,
                            key: doc! { "sku": "a", "qty": 3 },
                            present_fields: vec!["sku".to_string(), "qty".to_string()],
                        },
                    ],
                    &key_pattern,
                )
                .expect("grouped entry"),
                SecondaryEntry::from_index_entry(
                    &IndexEntry {
                        record_id: 13,
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
            separators: vec![
                SecondarySeparator::from_index_entry(
                    &IndexEntry {
                        record_id: 13,
                        key: doc! { "sku": "b", "qty": 4 },
                        present_fields: vec!["sku".to_string(), "qty".to_string()],
                    },
                    4,
                    &key_pattern,
                )
                .expect("separator"),
            ],
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

        let change_events = ChangeEventsPage {
            page_id: 14,
            next_page_id: Some(15),
            events: vec![
                PersistedChangeEvent::new(
                    &doc! { "_data": "token-1" },
                    bson::Timestamp {
                        time: 7,
                        increment: 1,
                    },
                    bson::DateTime::from_millis(1_700_000_000_000),
                    "app".to_string(),
                    Some("widgets".to_string()),
                    "insert".to_string(),
                    None,
                    None,
                    None,
                    None,
                    false,
                    &doc! { "marker": 5 },
                )
                .expect("change event"),
            ],
        };
        let decoded_change_events =
            ChangeEventsPage::decode(&change_events.encode().expect("encode change events"))
                .expect("decode change events");
        assert_eq!(decoded_change_events, change_events);

        let plan_cache = PlanCachePage {
            page_id: 16,
            next_page_id: None,
            entries: vec![PersistedPlanCacheEntry {
                namespace: "app.widgets".to_string(),
                filter_shape: "{\"sku\":?}".to_string(),
                sort_shape: "{}".to_string(),
                projection_shape: "{}".to_string(),
                sequence: 9,
                choice: PersistedPlanCacheChoice::Index("sku_1".to_string()),
            }],
        };
        let decoded_plan_cache =
            PlanCachePage::decode(&plan_cache.encode().expect("encode plan cache"))
                .expect("decode plan cache");
        assert_eq!(decoded_plan_cache, plan_cache);
    }
}
