use std::cmp::Ordering;

use anyhow::{Result, anyhow};
use bson::{Bson, Document};
use mqlite_bson::compare_bson;
use mqlite_catalog::{IndexBound, IndexBounds, IndexEntry};

use crate::v2::{
    layout::PageKind,
    page::{
        PageId, RecordInternalPage, RecordLeafPage, RecordSlot, SecondaryEntry,
        SecondaryInternalPage, SecondaryLeafPage, page_kind,
    },
    pager::Pager,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanDirection {
    Forward,
    Backward,
}

pub(crate) trait PageReader {
    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>>;
}

impl PageReader for Pager {
    fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        self.read_page_bytes(page_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordTree {
    root_page_id: Option<PageId>,
}

impl RecordTree {
    pub fn new(root_page_id: Option<PageId>) -> Self {
        Self { root_page_id }
    }

    pub fn lookup<R: PageReader>(
        &self,
        reader: &mut R,
        record_id: u64,
    ) -> Result<Option<RecordSlot>> {
        let Some(mut page_id) = self.root_page_id else {
            return Ok(None);
        };

        loop {
            let page = reader.read_page(page_id)?;
            match page_kind(&page)? {
                PageKind::RecordLeaf => {
                    let leaf = RecordLeafPage::decode(&page)?;
                    return Ok(leaf
                        .entries
                        .into_iter()
                        .find(|entry| entry.record_id == record_id));
                }
                PageKind::RecordInternal => {
                    let internal = RecordInternalPage::decode(&page)?;
                    page_id = child_for_record_id(&internal, record_id);
                }
                other => {
                    return Err(anyhow!(
                        "record tree expected a record page, found {:?}",
                        other
                    ));
                }
            }
        }
    }

    pub fn scan<R: PageReader>(&self, reader: &mut R) -> Result<Vec<RecordSlot>> {
        let Some(mut page_id) = self.root_page_id else {
            return Ok(Vec::new());
        };
        loop {
            let page = reader.read_page(page_id)?;
            match page_kind(&page)? {
                PageKind::RecordLeaf => break,
                PageKind::RecordInternal => {
                    page_id = RecordInternalPage::decode(&page)?.first_child_page_id;
                }
                other => {
                    return Err(anyhow!(
                        "record tree expected a record page, found {:?}",
                        other
                    ));
                }
            }
        }

        let mut records = Vec::new();
        let mut next_page_id = Some(page_id);
        while let Some(current_page_id) = next_page_id {
            let page = RecordLeafPage::decode(&reader.read_page(current_page_id)?)?;
            next_page_id = page.next_page_id;
            records.extend(page.entries);
        }
        Ok(records)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecondaryTree {
    root_page_id: Option<PageId>,
    key_pattern: Document,
}

impl SecondaryTree {
    pub fn new(root_page_id: Option<PageId>, key_pattern: Document) -> Self {
        Self {
            root_page_id,
            key_pattern,
        }
    }

    pub fn scan_bounds<R: PageReader>(
        &self,
        reader: &mut R,
        bounds: &IndexBounds,
        direction: ScanDirection,
    ) -> Result<Vec<IndexEntry>> {
        let Some(root_page_id) = self.root_page_id else {
            return Ok(Vec::new());
        };
        let mut leaf_page_id = Some(if let Some(lower) = bounds.lower.as_ref() {
            find_leaf_for_lower_bound(reader, root_page_id, &self.key_pattern, lower)?
        } else {
            leftmost_secondary_leaf(reader, root_page_id)?
        });

        let mut entries = Vec::new();
        while let Some(current_page_id) = leaf_page_id {
            let leaf = SecondaryLeafPage::decode(&reader.read_page(current_page_id)?)?;
            leaf_page_id = leaf.next_page_id;
            let mut past_upper = false;
            for entry in leaf.entries {
                if !entry_within_bounds(&entry, bounds, &self.key_pattern) {
                    past_upper = entry_past_upper_bound(&entry, bounds, &self.key_pattern);
                    continue;
                }
                entries.push(entry.into_index_entry(&self.key_pattern));
            }
            if past_upper {
                break;
            }
        }

        if direction == ScanDirection::Backward {
            entries.reverse();
        }
        Ok(entries)
    }
}

fn leftmost_secondary_leaf<R: PageReader>(reader: &mut R, mut page_id: PageId) -> Result<PageId> {
    loop {
        let page = reader.read_page(page_id)?;
        match page_kind(&page)? {
            PageKind::SecondaryLeaf => return Ok(page_id),
            PageKind::SecondaryInternal => {
                page_id = SecondaryInternalPage::decode(&page)?.first_child_page_id;
            }
            other => {
                return Err(anyhow!(
                    "secondary tree expected a secondary page, found {:?}",
                    other
                ));
            }
        }
    }
}

fn find_leaf_for_lower_bound<R: PageReader>(
    reader: &mut R,
    mut page_id: PageId,
    key_pattern: &Document,
    lower: &IndexBound,
) -> Result<PageId> {
    let target_record_id = if lower.inclusive { u64::MIN } else { u64::MAX };
    loop {
        let page = reader.read_page(page_id)?;
        match page_kind(&page)? {
            PageKind::SecondaryLeaf => return Ok(page_id),
            PageKind::SecondaryInternal => {
                let internal = SecondaryInternalPage::decode(&page)?;
                page_id =
                    child_for_secondary_entry(&internal, &lower.key, target_record_id, key_pattern);
            }
            other => {
                return Err(anyhow!(
                    "secondary tree expected a secondary page, found {:?}",
                    other
                ));
            }
        }
    }
}

fn child_for_record_id(page: &RecordInternalPage, record_id: u64) -> PageId {
    let mut child_page_id = page.first_child_page_id;
    for separator in &page.separators {
        if record_id < separator.record_id {
            break;
        }
        child_page_id = separator.child_page_id;
    }
    child_page_id
}

fn child_for_secondary_entry(
    page: &SecondaryInternalPage,
    key: &Document,
    record_id: u64,
    key_pattern: &Document,
) -> PageId {
    let mut child_page_id = page.first_child_page_id;
    for separator in &page.separators {
        if compare_secondary_tuple(
            key,
            record_id,
            &separator.key,
            separator.record_id,
            key_pattern,
        )
        .is_lt()
        {
            break;
        }
        child_page_id = separator.child_page_id;
    }
    child_page_id
}

fn entry_within_bounds(
    entry: &SecondaryEntry,
    bounds: &IndexBounds,
    key_pattern: &Document,
) -> bool {
    entry_satisfies_lower_bound(entry, bounds.lower.as_ref(), key_pattern)
        && entry_satisfies_upper_bound(entry, bounds.upper.as_ref(), key_pattern)
}

fn entry_satisfies_lower_bound(
    entry: &SecondaryEntry,
    lower: Option<&IndexBound>,
    key_pattern: &Document,
) -> bool {
    lower.is_none_or(|bound| {
        let ordering = compare_secondary_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_gt() || (bound.inclusive && ordering.is_eq())
    })
}

fn entry_satisfies_upper_bound(
    entry: &SecondaryEntry,
    upper: Option<&IndexBound>,
    key_pattern: &Document,
) -> bool {
    upper.is_none_or(|bound| {
        let ordering = compare_secondary_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_lt() || (bound.inclusive && ordering.is_eq())
    })
}

fn entry_past_upper_bound(
    entry: &SecondaryEntry,
    bounds: &IndexBounds,
    key_pattern: &Document,
) -> bool {
    bounds.upper.as_ref().is_some_and(|bound| {
        let ordering = compare_secondary_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_gt() || (!bound.inclusive && ordering.is_eq())
    })
}

fn compare_secondary_tuple(
    left_key: &Document,
    left_record_id: u64,
    right_key: &Document,
    right_record_id: u64,
    key_pattern: &Document,
) -> Ordering {
    compare_secondary_keys(left_key, right_key, key_pattern)
        .then_with(|| left_record_id.cmp(&right_record_id))
}

fn compare_secondary_keys(left: &Document, right: &Document, key_pattern: &Document) -> Ordering {
    for (field, direction) in key_pattern {
        let left_value = left.get(field).unwrap_or(&Bson::Null);
        let right_value = right.get(field).unwrap_or(&Bson::Null);
        let mut ordering = compare_bson(left_value, right_value);
        if key_direction(direction) < 0 {
            ordering = ordering.reverse();
        }
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn key_direction(value: &Bson) -> i32 {
    match value {
        Bson::Int32(direction) if *direction < 0 => -1,
        Bson::Int64(direction) if *direction < 0 => -1,
        Bson::Double(direction) if *direction < 0.0 => -1,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::{Result, anyhow};
    use bson::doc;
    use mqlite_catalog::{IndexBound, IndexBounds, IndexEntry};

    use super::{PageId, PageReader, RecordTree, ScanDirection, SecondaryTree};
    use crate::v2::page::{
        RecordInternalPage, RecordLeafPage, RecordSeparator, RecordSlot, SecondaryEntry,
        SecondaryInternalPage, SecondaryLeafPage, SecondarySeparator,
    };

    #[derive(Default)]
    struct MemoryPageReader {
        pages: BTreeMap<PageId, Vec<u8>>,
    }

    impl MemoryPageReader {
        fn insert_page(&mut self, page_id: PageId, bytes: Vec<u8>) {
            self.pages.insert(page_id, bytes);
        }
    }

    impl PageReader for MemoryPageReader {
        fn read_page(&mut self, page_id: PageId) -> Result<Vec<u8>> {
            self.pages
                .get(&page_id)
                .cloned()
                .ok_or_else(|| anyhow!("missing page {page_id}"))
        }
    }

    #[test]
    fn record_tree_looks_up_and_scans_records() {
        let mut reader = MemoryPageReader::default();
        reader.insert_page(
            1,
            RecordInternalPage {
                page_id: 1,
                first_child_page_id: 2,
                separators: vec![RecordSeparator {
                    record_id: 3,
                    child_page_id: 3,
                }],
            }
            .encode()
            .expect("encode record internal")
            .to_vec(),
        );
        reader.insert_page(
            2,
            RecordLeafPage {
                page_id: 2,
                next_page_id: Some(3),
                entries: vec![
                    RecordSlot::from_document(1, &doc! { "_id": 1, "sku": "a" }).expect("record"),
                    RecordSlot::from_document(2, &doc! { "_id": 2, "sku": "b" }).expect("record"),
                ],
            }
            .encode()
            .expect("encode record leaf")
            .to_vec(),
        );
        reader.insert_page(
            3,
            RecordLeafPage {
                page_id: 3,
                next_page_id: None,
                entries: vec![
                    RecordSlot::from_document(3, &doc! { "_id": 3, "sku": "c" }).expect("record"),
                    RecordSlot::from_document(4, &doc! { "_id": 4, "sku": "d" }).expect("record"),
                ],
            }
            .encode()
            .expect("encode record leaf")
            .to_vec(),
        );

        let tree = RecordTree::new(Some(1));
        let record = tree
            .lookup(&mut reader, 4)
            .expect("lookup")
            .expect("record");
        assert_eq!(
            record.decode_document().expect("decode"),
            doc! { "_id": 4, "sku": "d" }
        );

        let scanned = tree.scan(&mut reader).expect("scan");
        let ids = scanned
            .into_iter()
            .map(|record| record.record_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn secondary_tree_scans_ranges_in_key_order() {
        let key_pattern = doc! { "sku": 1, "qty": -1 };
        let mut reader = MemoryPageReader::default();
        reader.insert_page(
            10,
            SecondaryInternalPage {
                page_id: 10,
                first_child_page_id: 11,
                separators: vec![SecondarySeparator {
                    key: doc! { "sku": "b", "qty": 5 },
                    record_id: 3,
                    child_page_id: 12,
                }],
            }
            .encode()
            .expect("encode secondary internal")
            .to_vec(),
        );
        reader.insert_page(
            11,
            SecondaryLeafPage {
                page_id: 11,
                next_page_id: Some(12),
                entries: vec![
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 1,
                            key: doc! { "sku": "a", "qty": 9 },
                            present_fields: vec!["sku".to_string(), "qty".to_string()],
                        },
                        &key_pattern,
                    )
                    .expect("entry"),
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 2,
                            key: doc! { "sku": "a", "qty": 3 },
                            present_fields: vec!["sku".to_string(), "qty".to_string()],
                        },
                        &key_pattern,
                    )
                    .expect("entry"),
                ],
            }
            .encode()
            .expect("encode secondary leaf")
            .to_vec(),
        );
        reader.insert_page(
            12,
            SecondaryLeafPage {
                page_id: 12,
                next_page_id: None,
                entries: vec![
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 3,
                            key: doc! { "sku": "b", "qty": 5 },
                            present_fields: vec!["sku".to_string(), "qty".to_string()],
                        },
                        &key_pattern,
                    )
                    .expect("entry"),
                    SecondaryEntry::from_index_entry(
                        &IndexEntry {
                            record_id: 4,
                            key: doc! { "sku": "c", "qty": 1 },
                            present_fields: vec!["sku".to_string()],
                        },
                        &key_pattern,
                    )
                    .expect("entry"),
                ],
            }
            .encode()
            .expect("encode secondary leaf")
            .to_vec(),
        );

        let tree = SecondaryTree::new(Some(10), key_pattern.clone());
        let bounds = IndexBounds {
            lower: Some(IndexBound {
                key: doc! { "sku": "a", "qty": 9 },
                inclusive: true,
            }),
            upper: Some(IndexBound {
                key: doc! { "sku": "b", "qty": 5 },
                inclusive: true,
            }),
        };

        let forward = tree
            .scan_bounds(&mut reader, &bounds, ScanDirection::Forward)
            .expect("forward scan");
        assert_eq!(
            forward
                .into_iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let backward = tree
            .scan_bounds(&mut reader, &bounds, ScanDirection::Backward)
            .expect("backward scan");
        assert_eq!(
            backward
                .into_iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
    }
}
