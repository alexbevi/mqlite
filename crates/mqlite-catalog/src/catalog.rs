use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
};

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, lookup_path_owned};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const INDEX_TREE_LEAF_CAPACITY: usize = 64;
const INDEX_TREE_BRANCH_CAPACITY: usize = 64;
const INDEX_PAGE_BYTES: usize = 4096;
const INDEX_PAGE_HEADER_BYTES: usize = 32;
const INDEX_PAGE_SLOT_BYTES: usize = 16;

fn default_next_record_id() -> u64 {
    1
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Catalog {
    pub databases: BTreeMap<String, DatabaseCatalog>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseCatalog {
    pub collections: BTreeMap<String, CollectionCatalog>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionCatalog {
    pub options: Document,
    pub indexes: BTreeMap<String, IndexCatalog>,
    pub records: Vec<CollectionRecord>,
    #[serde(default = "default_next_record_id")]
    pub next_record_id: u64,
    #[serde(skip, default)]
    record_positions: HashMap<u64, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionRecord {
    pub record_id: u64,
    pub document: Document,
}

#[derive(Debug, Clone, Copy)]
pub enum CollectionMutation<'a> {
    Insert(&'a CollectionRecord),
    Update(&'a CollectionRecord),
    Delete(u64),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexTree {
    pub root: Option<Box<IndexNode>>,
    pub height: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexNode {
    Leaf {
        entries: Vec<IndexEntry>,
    },
    Internal {
        separators: Vec<Document>,
        children: Vec<IndexNode>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBounds {
    pub lower: Option<IndexBound>,
    pub upper: Option<IndexBound>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBound {
    pub key: Document,
    pub inclusive: bool,
}

#[derive(Debug, Clone)]
struct NodeSummary {
    node: IndexNode,
    min_key: Document,
    max_key: Document,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct RuntimeIndexPage {
    entries: Vec<IndexEntry>,
    used_bytes: usize,
}

impl RuntimeIndexPage {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            used_bytes: INDEX_PAGE_HEADER_BYTES,
        }
    }

    fn first_entry(&self) -> Option<&IndexEntry> {
        self.entries.first()
    }

    fn last_entry(&self) -> Option<&IndexEntry> {
        self.entries.last()
    }

    fn can_fit_entry(&self, entry: &IndexEntry) -> Result<bool, CatalogError> {
        Ok(self.used_bytes + encoded_index_entry_storage_len(entry)? <= INDEX_PAGE_BYTES)
    }

    fn insert_entry(
        &mut self,
        entry: IndexEntry,
        key_pattern: &Document,
    ) -> Result<(), CatalogError> {
        let position = self
            .entries
            .binary_search_by(|existing| compare_index_entries(existing, &entry, key_pattern))
            .unwrap_or_else(|position| position);
        self.used_bytes += encoded_index_entry_storage_len(&entry)?;
        self.entries.insert(position, entry);
        Ok(())
    }
}

impl CollectionCatalog {
    pub fn new(options: Document) -> Self {
        let mut collection = Self {
            options,
            indexes: BTreeMap::from([(
                "_id_".to_string(),
                IndexCatalog::new("_id_".to_string(), doc! { "_id": 1 }, true),
            )]),
            records: Vec::new(),
            next_record_id: default_next_record_id(),
            record_positions: HashMap::new(),
        };
        collection.rebuild_runtime_state();
        collection
    }

    pub fn from_parts(
        options: Document,
        indexes: BTreeMap<String, IndexCatalog>,
        records: Vec<CollectionRecord>,
        next_record_id: u64,
    ) -> Self {
        let mut collection = Self {
            options,
            indexes,
            records,
            next_record_id,
            record_positions: HashMap::new(),
        };
        collection.rebuild_runtime_state();
        collection
    }

    pub fn next_record_id(&self) -> u64 {
        self.next_record_id
    }

    pub fn record_position(&self, record_id: u64) -> Option<usize> {
        self.record_positions.get(&record_id).copied()
    }

    pub fn documents(&self) -> Vec<Document> {
        self.records
            .iter()
            .map(|record| record.document.clone())
            .collect()
    }

    pub fn hydrate_indexes(&mut self) {
        self.rebuild_runtime_state();
        for index in self.indexes.values_mut() {
            index.rebuild_tree();
        }
    }

    pub fn insert_record(&mut self, record: CollectionRecord) -> Result<(), CatalogError> {
        if self.record_positions.contains_key(&record.record_id) {
            return Err(CatalogError::InvalidIndexState(format!(
                "duplicate record id {}",
                record.record_id
            )));
        }

        validate_record_against_indexes(&self.indexes, &record.document, None)?;
        for index in self.indexes.values_mut() {
            let entry = index_entry_for_document(record.record_id, &record.document, &index.key);
            index.append_entries(vec![entry])?;
        }
        self.record_positions
            .insert(record.record_id, self.records.len());
        self.next_record_id = self.next_record_id.max(record.record_id.saturating_add(1));
        self.records.push(record);
        Ok(())
    }

    pub fn update_record_at(
        &mut self,
        position: usize,
        document: Document,
    ) -> Result<bool, CatalogError> {
        let Some(existing) = self.records.get(position) else {
            return Err(CatalogError::InvalidIndexState(format!(
                "record position {position} is out of bounds"
            )));
        };
        if existing.document == document {
            return Ok(false);
        }

        let record_id = existing.record_id;
        validate_record_against_indexes(&self.indexes, &document, Some(record_id))?;

        for index in self.indexes.values_mut() {
            let mut entries = index.entries_snapshot();
            entries.retain(|entry| entry.record_id != record_id);
            let entry = index_entry_for_document(record_id, &document, &index.key);
            insert_index_entry(&mut entries, entry, &index.key);
            index.replace_entries(entries)?;
        }
        self.records[position].document = document;
        Ok(true)
    }

    pub fn delete_records(&mut self, record_ids: &BTreeSet<u64>) -> usize {
        if record_ids.is_empty() {
            return 0;
        }

        let before = self.records.len();
        self.records
            .retain(|record| !record_ids.contains(&record.record_id));
        self.rebuild_runtime_state();
        for index in self.indexes.values_mut() {
            let mut entries = index.entries_snapshot();
            entries.retain(|entry| !record_ids.contains(&entry.record_id));
            index
                .replace_entries(entries)
                .expect("existing index entries must remain encodable");
        }
        before - self.records.len()
    }

    pub fn apply_mutations(
        &mut self,
        mutations: &[CollectionMutation<'_>],
    ) -> Result<(), CatalogError> {
        if mutations.is_empty() {
            return Ok(());
        }

        if let Some(inserts) = insert_batch_mutations(mutations) {
            return self.apply_insert_batch(&inserts);
        }

        let base_positions = self.record_positions.clone();
        let plan = CollectionMutationPlan::build(mutations, &self.records, &base_positions)?;
        if plan.states.is_empty() {
            return Ok(());
        }

        let staged_indexes = self
            .indexes
            .values()
            .filter_map(|index| {
                stage_index_entries(index, &self.records, &base_positions, &plan)
                    .transpose()
                    .map(|entries| entries.map(|entries| (index.name.clone(), entries)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let original_records = std::mem::take(&mut self.records);
        self.records = plan.materialize_records(original_records);
        self.rebuild_runtime_state();
        for (name, entries) in staged_indexes {
            let index = self
                .indexes
                .get_mut(&name)
                .expect("staged index must exist");
            index.replace_entries(entries)?;
        }
        Ok(())
    }

    pub fn validate_indexes(&self) -> Result<(), CatalogError> {
        validate_collection_indexes(self)
    }

    pub fn refresh_runtime_state(&mut self) {
        self.rebuild_runtime_state();
    }

    fn rebuild_runtime_state(&mut self) {
        self.record_positions = self
            .records
            .iter()
            .enumerate()
            .map(|(position, record)| (record.record_id, position))
            .collect();
        let computed_next_record_id = self
            .records
            .iter()
            .map(|record| record.record_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        self.next_record_id = self.next_record_id.max(computed_next_record_id);
    }

    fn apply_insert_batch(&mut self, inserts: &[&CollectionRecord]) -> Result<(), CatalogError> {
        if inserts.is_empty() {
            return Ok(());
        }

        let mut seen_record_ids = HashSet::with_capacity(inserts.len());
        for record in inserts {
            if self.record_positions.contains_key(&record.record_id)
                || !seen_record_ids.insert(record.record_id)
            {
                return Err(CatalogError::InvalidIndexState(format!(
                    "duplicate record id {}",
                    record.record_id
                )));
            }
        }

        let staged_indexes = self
            .indexes
            .values()
            .map(|index| {
                stage_insert_entries(index, inserts).map(|entries| (index.name.clone(), entries))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let start_position = self.records.len();
        self.records
            .extend(inserts.iter().map(|record| (*record).clone()));
        for (offset, record) in inserts.iter().enumerate() {
            self.record_positions
                .insert(record.record_id, start_position + offset);
            self.next_record_id = self.next_record_id.max(record.record_id.saturating_add(1));
        }

        for (name, entries) in staged_indexes {
            let index = self
                .indexes
                .get_mut(&name)
                .expect("staged index must exist");
            index.append_entries(entries)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum StagedRecordState {
    Present { document: Document, appended: bool },
    Deleted,
}

#[derive(Debug, Default)]
struct CollectionMutationPlan {
    states: HashMap<u64, StagedRecordState>,
    insert_order: HashMap<u64, usize>,
    next_insert_order: usize,
}

impl CollectionMutationPlan {
    fn build(
        mutations: &[CollectionMutation<'_>],
        base_records: &[CollectionRecord],
        base_positions: &HashMap<u64, usize>,
    ) -> Result<Self, CatalogError> {
        let mut plan = Self::default();
        for mutation in mutations {
            match mutation {
                CollectionMutation::Insert(record) => {
                    if plan
                        .current_document(base_records, base_positions, record.record_id)
                        .is_some()
                    {
                        return Err(CatalogError::InvalidIndexState(format!(
                            "duplicate record id {}",
                            record.record_id
                        )));
                    }
                    plan.next_insert_order += 1;
                    plan.insert_order
                        .insert(record.record_id, plan.next_insert_order);
                    plan.states.insert(
                        record.record_id,
                        StagedRecordState::Present {
                            document: record.document.clone(),
                            appended: true,
                        },
                    );
                }
                CollectionMutation::Update(record) => {
                    let Some(current_document) =
                        plan.current_document(base_records, base_positions, record.record_id)
                    else {
                        return Err(CatalogError::InvalidIndexState(format!(
                            "record id {} is missing for update",
                            record.record_id
                        )));
                    };
                    if current_document == &record.document {
                        continue;
                    }
                    let appended = matches!(
                        plan.states.get(&record.record_id),
                        Some(StagedRecordState::Present { appended: true, .. })
                    );
                    plan.states.insert(
                        record.record_id,
                        StagedRecordState::Present {
                            document: record.document.clone(),
                            appended,
                        },
                    );
                }
                CollectionMutation::Delete(record_id) => {
                    if plan
                        .current_document(base_records, base_positions, *record_id)
                        .is_none()
                    {
                        continue;
                    }
                    plan.states.insert(*record_id, StagedRecordState::Deleted);
                }
            }
        }
        Ok(plan)
    }

    fn current_document<'a>(
        &'a self,
        base_records: &'a [CollectionRecord],
        base_positions: &HashMap<u64, usize>,
        record_id: u64,
    ) -> Option<&'a Document> {
        match self.states.get(&record_id) {
            Some(StagedRecordState::Present { document, .. }) => Some(document),
            Some(StagedRecordState::Deleted) => None,
            None => base_positions
                .get(&record_id)
                .map(|position| &base_records[*position].document),
        }
    }

    fn materialize_records(
        &self,
        original_records: Vec<CollectionRecord>,
    ) -> Vec<CollectionRecord> {
        let mut next_records = Vec::with_capacity(original_records.len());
        for mut record in original_records {
            match self.states.get(&record.record_id) {
                Some(StagedRecordState::Present {
                    document,
                    appended: false,
                }) => {
                    record.document = document.clone();
                    next_records.push(record);
                }
                Some(StagedRecordState::Present { appended: true, .. })
                | Some(StagedRecordState::Deleted) => {}
                None => next_records.push(record),
            }
        }

        let mut appended_records = self
            .states
            .iter()
            .filter_map(|(record_id, state)| match state {
                StagedRecordState::Present {
                    document,
                    appended: true,
                } => Some((
                    *self
                        .insert_order
                        .get(record_id)
                        .expect("appended records must have an insert order"),
                    CollectionRecord {
                        record_id: *record_id,
                        document: document.clone(),
                    },
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        appended_records.sort_by_key(|(order, _)| *order);
        next_records.extend(appended_records.into_iter().map(|(_, record)| record));
        next_records
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexCatalog {
    pub name: String,
    pub key: Document,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
    entries: Vec<IndexEntry>,
    #[serde(skip, default)]
    runtime_pages: Vec<RuntimeIndexPage>,
    #[serde(skip, default = "default_true")]
    entries_materialized: bool,
    #[serde(skip, default)]
    pub tree: IndexTree,
    #[serde(skip, default)]
    pub stats: IndexStats,
}

impl IndexCatalog {
    pub fn new(name: String, key: Document, unique: bool) -> Self {
        Self {
            name,
            key,
            unique,
            expire_after_seconds: None,
            entries: Vec::new(),
            runtime_pages: Vec::new(),
            entries_materialized: true,
            tree: IndexTree::default(),
            stats: IndexStats::default(),
        }
    }

    pub fn rebuild_tree(&mut self) {
        let entries = self.materialized_entries().to_vec();
        self.tree = IndexTree::build(&entries);
        self.stats = IndexStats::build(&entries, &self.key);
    }

    pub fn scan_entries(&self, bounds: &IndexBounds) -> Vec<IndexEntry> {
        let mut matched = Vec::new();
        if self.runtime_pages.is_empty() {
            let start = bounds.lower.as_ref().map_or(0, |bound| {
                lower_bound_index(&self.entries, bound, &self.key)
            });
            let end = bounds.upper.as_ref().map_or(self.entries.len(), |bound| {
                upper_bound_index(&self.entries, bound, &self.key)
            });
            if start < end {
                matched.extend(self.entries[start..end].iter().cloned());
            }
            return matched;
        }

        for page in &self.runtime_pages {
            if page.entries.is_empty() || !page_overlaps_bounds(page, bounds, &self.key) {
                continue;
            }
            let start = bounds.lower.as_ref().map_or(0, |bound| {
                lower_bound_index(&page.entries, bound, &self.key)
            });
            let end = bounds.upper.as_ref().map_or(page.entries.len(), |bound| {
                upper_bound_index(&page.entries, bound, &self.key)
            });
            if start < end {
                matched.extend(page.entries[start..end].iter().cloned());
            }
        }
        matched
    }

    pub fn scan_bounds(&self, bounds: &IndexBounds) -> Vec<u64> {
        self.scan_entries(bounds)
            .into_iter()
            .map(|entry| entry.record_id)
            .collect()
    }

    pub fn sort_entries(&self, entries: &mut [IndexEntry]) {
        entries.sort_by(|left, right| compare_index_entries(left, right, &self.key));
    }

    pub fn estimate_bounds_count(&self, bounds: &IndexBounds) -> usize {
        if self.runtime_pages.is_empty() {
            let start = bounds.lower.as_ref().map_or(0, |bound| {
                lower_bound_index(&self.entries, bound, &self.key)
            });
            let end = bounds.upper.as_ref().map_or(self.entries.len(), |bound| {
                upper_bound_index(&self.entries, bound, &self.key)
            });
            return end.saturating_sub(start);
        }

        self.runtime_pages
            .iter()
            .filter(|page| {
                !page.entries.is_empty() && page_overlaps_bounds(page, bounds, &self.key)
            })
            .map(|page| {
                let start = bounds.lower.as_ref().map_or(0, |bound| {
                    lower_bound_index(&page.entries, bound, &self.key)
                });
                let end = bounds.upper.as_ref().map_or(page.entries.len(), |bound| {
                    upper_bound_index(&page.entries, bound, &self.key)
                });
                end.saturating_sub(start)
            })
            .sum()
    }

    pub fn covers_paths(&self, paths: &BTreeSet<String>) -> bool {
        paths.iter().all(|path| self.key.contains_key(path))
    }

    pub fn estimate_value_count(&self, field: &str, value: &Bson) -> Option<usize> {
        self.stats
            .value_frequencies
            .get(field)
            .and_then(|frequencies| {
                frequencies
                    .iter()
                    .find(|frequency| compare_bson(&frequency.value, value).is_eq())
                    .map(|frequency| frequency.count)
            })
    }

    pub fn estimate_values_count(&self, field: &str, values: &[Bson]) -> Option<usize> {
        values.iter().try_fold(0_usize, |total, value| {
            self.estimate_value_count(field, value)
                .map(|count| total + count)
        })
    }

    pub fn estimate_range_count(
        &self,
        field: &str,
        lower: Option<(&Bson, bool)>,
        upper: Option<(&Bson, bool)>,
    ) -> Option<usize> {
        self.stats.value_frequencies.get(field).map(|frequencies| {
            frequencies
                .iter()
                .filter(|frequency| {
                    lower.is_none_or(|(value, inclusive)| {
                        let ordering = compare_bson(&frequency.value, value);
                        ordering.is_gt() || (inclusive && ordering.is_eq())
                    }) && upper.is_none_or(|(value, inclusive)| {
                        let ordering = compare_bson(&frequency.value, value);
                        ordering.is_lt() || (inclusive && ordering.is_eq())
                    })
                })
                .map(|frequency| frequency.count)
                .sum()
        })
    }

    pub fn present_count(&self, field: &str) -> Option<usize> {
        self.stats.present_fields.get(field).copied()
    }

    pub fn entry_count(&self) -> usize {
        if self.runtime_pages.is_empty() {
            self.entries.len()
        } else {
            self.runtime_pages
                .iter()
                .map(|page| page.entries.len())
                .sum::<usize>()
        }
    }

    pub fn entries_snapshot(&self) -> Vec<IndexEntry> {
        if self.entries_materialized {
            return self.entries.clone();
        }

        self.runtime_pages
            .iter()
            .flat_map(|page| page.entries.iter().cloned())
            .collect()
    }

    pub fn for_each_entry(&self, mut visit: impl FnMut(&IndexEntry)) {
        if self.runtime_pages.is_empty() {
            for entry in &self.entries {
                visit(entry);
            }
            return;
        }

        for page in &self.runtime_pages {
            for entry in &page.entries {
                visit(entry);
            }
        }
    }

    pub fn load_entries(&mut self, entries: Vec<IndexEntry>) -> Result<(), CatalogError> {
        self.replace_entries(entries)
    }

    pub fn stats_hydrated(&self) -> bool {
        self.stats.entry_count == self.entry_count()
            && self.stats.present_fields.len() == self.key.len()
            && self.stats.value_frequencies.len() == self.key.len()
    }

    fn refresh_stats(&mut self) {
        self.stats = if self.runtime_pages.is_empty() {
            IndexStats::build(&self.entries, &self.key)
        } else {
            IndexStats::build_pages(self.runtime_pages.iter(), &self.key)
        };
    }

    fn invalidate_tree(&mut self) {
        self.tree = IndexTree::default();
    }

    fn replace_entries(&mut self, entries: Vec<IndexEntry>) -> Result<(), CatalogError> {
        self.entries = entries;
        self.runtime_pages = build_runtime_index_pages(&self.entries, &self.key)?;
        self.entries_materialized = true;
        self.refresh_stats();
        self.invalidate_tree();
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<IndexEntry>) -> Result<(), CatalogError> {
        if entries.is_empty() {
            return Ok(());
        }

        self.ensure_runtime_pages()?;
        if self.stats_hydrated() {
            for entry in &entries {
                self.stats.insert_entry(entry, &self.key);
            }
        }

        for entry in entries {
            self.insert_entry_page_local(entry)?;
        }
        if !self.stats_hydrated() {
            self.refresh_stats();
        }
        self.invalidate_entries();
        self.invalidate_tree();
        Ok(())
    }

    fn contains_unique_key(&self, candidate_key: &Document, skip_record_id: Option<u64>) -> bool {
        if self.runtime_pages.is_empty() {
            let start = self.entries.partition_point(|entry| {
                compare_index_keys(&entry.key, candidate_key, &self.key).is_lt()
            });
            return self.entries[start..]
                .iter()
                .take_while(|entry| {
                    compare_index_keys(&entry.key, candidate_key, &self.key).is_eq()
                })
                .any(|entry| Some(entry.record_id) != skip_record_id);
        }

        let page_index = self.runtime_pages.partition_point(|page| {
            page.last_entry()
                .is_some_and(|last| compare_index_keys(&last.key, candidate_key, &self.key).is_lt())
        });
        let Some(page) = self.runtime_pages.get(page_index) else {
            return false;
        };
        let Some(first) = page.first_entry() else {
            return false;
        };
        if compare_index_keys(&first.key, candidate_key, &self.key).is_gt() {
            return false;
        }

        let start = page.entries.partition_point(|entry| {
            compare_index_keys(&entry.key, candidate_key, &self.key).is_lt()
        });
        page.entries[start..]
            .iter()
            .take_while(|entry| compare_index_keys(&entry.key, candidate_key, &self.key).is_eq())
            .any(|entry| Some(entry.record_id) != skip_record_id)
    }

    fn ensure_runtime_pages(&mut self) -> Result<(), CatalogError> {
        if self.runtime_pages.is_empty() && !self.entries.is_empty() {
            self.runtime_pages = build_runtime_index_pages(&self.entries, &self.key)?;
        }
        Ok(())
    }

    fn insert_entry_page_local(&mut self, entry: IndexEntry) -> Result<(), CatalogError> {
        if self.runtime_pages.is_empty() {
            let mut page = RuntimeIndexPage::new();
            page.insert_entry(entry, &self.key)?;
            self.runtime_pages.push(page);
            return Ok(());
        }

        let page_index = self.runtime_pages.partition_point(|page| {
            page.last_entry()
                .is_some_and(|last| compare_index_entries(last, &entry, &self.key).is_lt())
        });
        let page_index = page_index.min(self.runtime_pages.len().saturating_sub(1));
        self.runtime_pages[page_index].insert_entry(entry, &self.key)?;
        if self.runtime_pages[page_index].used_bytes > INDEX_PAGE_BYTES {
            let replacement =
                build_runtime_index_pages(&self.runtime_pages[page_index].entries, &self.key)?;
            self.runtime_pages
                .splice(page_index..=page_index, replacement.into_iter());
        }
        Ok(())
    }

    fn materialized_entries(&mut self) -> &[IndexEntry] {
        if !self.entries_materialized {
            self.entries = self
                .runtime_pages
                .iter()
                .flat_map(|page| page.entries.iter().cloned())
                .collect();
            self.entries_materialized = true;
        }
        &self.entries
    }

    fn invalidate_entries(&mut self) {
        self.entries.clear();
        self.entries_materialized = false;
    }
}

impl IndexTree {
    pub fn build(entries: &[IndexEntry]) -> Self {
        if entries.is_empty() {
            return Self::default();
        }

        let mut level = entries
            .chunks(INDEX_TREE_LEAF_CAPACITY)
            .map(|chunk| NodeSummary {
                node: IndexNode::Leaf {
                    entries: chunk.to_vec(),
                },
                min_key: chunk.first().expect("leaf entry").key.clone(),
                max_key: chunk.last().expect("leaf entry").key.clone(),
            })
            .collect::<Vec<_>>();
        let mut height = 1;

        while level.len() > 1 {
            height += 1;
            level = level
                .chunks(INDEX_TREE_BRANCH_CAPACITY)
                .map(build_internal_summary)
                .collect();
        }

        Self {
            root: Some(Box::new(level.remove(0).node)),
            height,
        }
    }

    pub fn scan_entries(&self, bounds: &IndexBounds, key_pattern: &Document) -> Vec<IndexEntry> {
        let mut entries = Vec::new();
        if let Some(root) = self.root.as_deref() {
            scan_node(root, bounds, key_pattern, &mut entries);
        }
        entries
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexEntry {
    pub record_id: u64,
    pub key: Document,
    #[serde(default)]
    pub present_fields: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexStats {
    pub entry_count: usize,
    pub present_fields: BTreeMap<String, usize>,
    pub value_frequencies: BTreeMap<String, Vec<ValueFrequency>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueFrequency {
    pub value: Bson,
    pub count: usize,
}

impl IndexStats {
    fn build(entries: &[IndexEntry], key_pattern: &Document) -> Self {
        let mut stats = Self {
            entry_count: entries.len(),
            present_fields: key_pattern
                .keys()
                .cloned()
                .map(|field| (field, 0_usize))
                .collect(),
            value_frequencies: key_pattern
                .keys()
                .cloned()
                .map(|field| (field, Vec::new()))
                .collect(),
        };
        for entry in entries {
            for field in &entry.present_fields {
                if let Some(count) = stats.present_fields.get_mut(field) {
                    *count += 1;
                }
            }
            for (field, value) in &entry.key {
                let frequencies = stats
                    .value_frequencies
                    .get_mut(field)
                    .expect("frequency field");
                match frequencies
                    .iter_mut()
                    .find(|frequency| compare_bson(&frequency.value, value).is_eq())
                {
                    Some(frequency) => frequency.count += 1,
                    None => frequencies.push(ValueFrequency {
                        value: value.clone(),
                        count: 1,
                    }),
                }
            }
        }
        for frequencies in stats.value_frequencies.values_mut() {
            frequencies.sort_by(|left, right| compare_bson(&left.value, &right.value));
        }
        stats
    }

    fn build_pages<'a, I>(pages: I, key_pattern: &Document) -> Self
    where
        I: IntoIterator<Item = &'a RuntimeIndexPage>,
    {
        let entries = pages
            .into_iter()
            .flat_map(|page| page.entries.iter().cloned())
            .collect::<Vec<_>>();
        Self::build(&entries, key_pattern)
    }

    fn insert_entry(&mut self, entry: &IndexEntry, key_pattern: &Document) {
        if self.present_fields.is_empty() && self.value_frequencies.is_empty() {
            *self = Self::build(&[], key_pattern);
        }

        self.entry_count += 1;
        for field in &entry.present_fields {
            if let Some(count) = self.present_fields.get_mut(field) {
                *count += 1;
            }
        }
        for (field, value) in &entry.key {
            let frequencies = self
                .value_frequencies
                .get_mut(field)
                .expect("frequency field");
            adjust_value_frequency(frequencies, value, 1);
        }
    }
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("namespace `{0}.{1}` already exists")]
    NamespaceExists(String, String),
    #[error("namespace `{0}.{1}` was not found")]
    NamespaceNotFound(String, String),
    #[error("database `{0}` was not found")]
    DatabaseNotFound(String),
    #[error("index specification must contain a document `key` field")]
    InvalidIndexSpec,
    #[error("index `{0}` already exists")]
    IndexExists(String),
    #[error("index `{0}` was not found")]
    IndexNotFound(String),
    #[error("duplicate key error on index `{0}`")]
    DuplicateKey(String),
    #[error("collection index state is invalid: {0}")]
    InvalidIndexState(String),
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            databases: BTreeMap::new(),
        }
    }

    pub fn database_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    pub fn collection_names(&self, database: &str) -> Result<Vec<String>, CatalogError> {
        let database = self
            .databases
            .get(database)
            .ok_or_else(|| CatalogError::DatabaseNotFound(database.to_string()))?;
        Ok(database.collections.keys().cloned().collect())
    }

    pub fn create_collection(
        &mut self,
        database: &str,
        collection: &str,
        options: Document,
    ) -> Result<(), CatalogError> {
        let database_entry = self
            .databases
            .entry(database.to_string())
            .or_insert_with(|| DatabaseCatalog {
                collections: BTreeMap::new(),
            });

        if database_entry.collections.contains_key(collection) {
            return Err(CatalogError::NamespaceExists(
                database.to_string(),
                collection.to_string(),
            ));
        }

        database_entry
            .collections
            .insert(collection.to_string(), CollectionCatalog::new(options));
        Ok(())
    }

    pub fn ensure_collection(
        &mut self,
        database: &str,
        collection: &str,
    ) -> &mut CollectionCatalog {
        if self
            .databases
            .get(database)
            .and_then(|db| db.collections.get(collection))
            .is_none()
        {
            let _ = self.create_collection(database, collection, Document::new());
        }

        self.databases
            .get_mut(database)
            .expect("database exists")
            .collections
            .get_mut(collection)
            .expect("collection exists")
    }

    pub fn drop_collection(
        &mut self,
        database: &str,
        collection: &str,
    ) -> Result<(), CatalogError> {
        let database_entry = self
            .databases
            .get_mut(database)
            .ok_or_else(|| CatalogError::DatabaseNotFound(database.to_string()))?;
        if database_entry.collections.remove(collection).is_none() {
            return Err(CatalogError::NamespaceNotFound(
                database.to_string(),
                collection.to_string(),
            ));
        }
        let remove_database = database_entry.collections.is_empty();
        if remove_database {
            self.databases.remove(database);
        }
        Ok(())
    }

    pub fn replace_collection(
        &mut self,
        database: &str,
        collection: &str,
        collection_state: CollectionCatalog,
    ) {
        let database_entry = self
            .databases
            .entry(database.to_string())
            .or_insert_with(|| DatabaseCatalog {
                collections: BTreeMap::new(),
            });
        database_entry
            .collections
            .insert(collection.to_string(), collection_state);
    }

    pub fn get_collection(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<&CollectionCatalog, CatalogError> {
        self.databases
            .get(database)
            .and_then(|db| db.collections.get(collection))
            .ok_or_else(|| {
                CatalogError::NamespaceNotFound(database.to_string(), collection.to_string())
            })
    }

    pub fn get_collection_mut(
        &mut self,
        database: &str,
        collection: &str,
    ) -> Result<&mut CollectionCatalog, CatalogError> {
        self.databases
            .get_mut(database)
            .and_then(|db| db.collections.get_mut(collection))
            .ok_or_else(|| {
                CatalogError::NamespaceNotFound(database.to_string(), collection.to_string())
            })
    }

    pub fn list_indexes(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Vec<IndexCatalog>, CatalogError> {
        let collection = self.get_collection(database, collection)?;
        Ok(collection.indexes.values().cloned().collect())
    }

    pub fn create_indexes(
        &mut self,
        database: &str,
        collection: &str,
        specs: &[Document],
    ) -> Result<Vec<IndexCatalog>, CatalogError> {
        let collection = self.ensure_collection(database, collection);
        apply_index_specs(collection, specs)
    }

    pub fn drop_indexes(
        &mut self,
        database: &str,
        collection: &str,
        target: &str,
    ) -> Result<usize, CatalogError> {
        let collection = self.get_collection_mut(database, collection)?;
        drop_indexes_from_collection(collection, target)
    }
}

pub fn apply_index_specs(
    collection: &mut CollectionCatalog,
    specs: &[Document],
) -> Result<Vec<IndexCatalog>, CatalogError> {
    let created = build_index_specs(collection, specs)?;
    for index in &created {
        collection.indexes.insert(index.name.clone(), index.clone());
    }
    Ok(created)
}

pub fn build_index_specs(
    collection: &CollectionCatalog,
    specs: &[Document],
) -> Result<Vec<IndexCatalog>, CatalogError> {
    let mut created = Vec::new();
    let mut pending_names = collection.indexes.keys().cloned().collect::<BTreeSet<_>>();

    for spec in specs {
        let key = spec
            .get_document("key")
            .map_err(|_| CatalogError::InvalidIndexSpec)?;
        let name = spec
            .get_str("name")
            .map(|value| value.to_string())
            .unwrap_or_else(|_| default_index_name(key));
        if !pending_names.insert(name.clone()) {
            return Err(CatalogError::IndexExists(name));
        }
        let unique = spec.get_bool("unique").unwrap_or(false);
        let expire_after_seconds = spec
            .get("expireAfterSeconds")
            .map(parse_expire_after_seconds)
            .transpose()?;
        let mut index = IndexCatalog::new(name.clone(), key.clone(), unique);
        index.expire_after_seconds = expire_after_seconds;
        let mut entries = Vec::with_capacity(collection.records.len());
        for record in &collection.records {
            validate_record_against_index(&index, &record.document, None)?;
            insert_index_entry(
                &mut entries,
                index_entry_for_document(record.record_id, &record.document, &index.key),
                &index.key,
            );
        }
        if unique {
            for pair in entries.windows(2) {
                if compare_index_keys(&pair[0].key, &pair[1].key, &index.key) == Ordering::Equal {
                    return Err(CatalogError::DuplicateKey(name.clone()));
                }
            }
        }
        index.replace_entries(entries)?;
        created.push(index);
    }

    Ok(created)
}

fn parse_expire_after_seconds(value: &Bson) -> Result<i64, CatalogError> {
    let value = match value {
        Bson::Int32(value) => i64::from(*value),
        Bson::Int64(value) => *value,
        Bson::Double(value) if value.fract() == 0.0 => *value as i64,
        _ => return Err(CatalogError::InvalidIndexSpec),
    };
    if value < 0 {
        return Err(CatalogError::InvalidIndexSpec);
    }
    Ok(value)
}

pub fn drop_indexes_from_collection(
    collection: &mut CollectionCatalog,
    target: &str,
) -> Result<usize, CatalogError> {
    let removed = validate_drop_indexes(collection, target)?;
    if target == "*" {
        let retained = collection.indexes.remove("_id_");
        collection.indexes.clear();
        if let Some(id_index) = retained {
            collection.indexes.insert(id_index.name.clone(), id_index);
        }
        return Ok(removed);
    }

    if target == "_id_" {
        return Ok(0);
    }

    collection.indexes.remove(target);
    Ok(removed)
}

pub fn validate_drop_indexes(
    collection: &CollectionCatalog,
    target: &str,
) -> Result<usize, CatalogError> {
    if target == "*" {
        return Ok(collection.indexes.len().saturating_sub(1));
    }

    if target == "_id_" {
        return Ok(0);
    }

    if collection.indexes.contains_key(target) {
        Ok(1)
    } else {
        Err(CatalogError::IndexNotFound(target.to_string()))
    }
}

pub fn default_index_name(key: &Document) -> String {
    key.iter()
        .map(|(field, direction)| {
            let direction = match direction {
                Bson::Int32(value) => value.to_string(),
                Bson::Int64(value) => value.to_string(),
                Bson::String(value) => value.clone(),
                _ => "1".to_string(),
            };
            format!("{field}_{direction}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

pub fn index_key_for_document(document: &Document, key_pattern: &Document) -> Document {
    let mut key = Document::new();
    for (field, _) in key_pattern {
        key.insert(
            field,
            lookup_path_owned(document, field).unwrap_or(Bson::Null),
        );
    }
    key
}

pub fn validate_collection_indexes(collection: &CollectionCatalog) -> Result<(), CatalogError> {
    let mut seen_record_ids = BTreeSet::new();
    let record_by_id = collection
        .records
        .iter()
        .map(|record| {
            if !seen_record_ids.insert(record.record_id) {
                return Err(CatalogError::InvalidIndexState(format!(
                    "duplicate record id {}",
                    record.record_id
                )));
            }
            Ok((record.record_id, &record.document))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    for index in collection.indexes.values() {
        let entries = index.entries_snapshot();
        if entries.len() != collection.records.len() {
            return Err(CatalogError::InvalidIndexState(format!(
                "index `{}` entry count {} does not match record count {}",
                index.name,
                entries.len(),
                collection.records.len()
            )));
        }

        let mut previous: Option<&IndexEntry> = None;
        let mut indexed_record_ids = BTreeSet::new();
        for entry in &entries {
            let Some(document) = record_by_id.get(&entry.record_id) else {
                return Err(CatalogError::InvalidIndexState(format!(
                    "index `{}` references missing record id {}",
                    index.name, entry.record_id
                )));
            };
            if !indexed_record_ids.insert(entry.record_id) {
                return Err(CatalogError::InvalidIndexState(format!(
                    "index `{}` contains duplicate entry for record id {}",
                    index.name, entry.record_id
                )));
            }

            let expected_key = index_key_for_document(document, &index.key);
            if expected_key != entry.key {
                return Err(CatalogError::InvalidIndexState(format!(
                    "index `{}` key mismatch for record id {}",
                    index.name, entry.record_id
                )));
            }
            let expected_present_fields = index
                .key
                .keys()
                .filter(|field| lookup_path_owned(document, field).is_some())
                .cloned()
                .collect::<Vec<_>>();
            if expected_present_fields != entry.present_fields {
                return Err(CatalogError::InvalidIndexState(format!(
                    "index `{}` presence mismatch for record id {}",
                    index.name, entry.record_id
                )));
            }

            if let Some(previous_entry) = previous {
                let ordering = compare_index_entries(previous_entry, entry, &index.key);
                if ordering != Ordering::Less {
                    return Err(CatalogError::InvalidIndexState(format!(
                        "index `{}` entries are not strictly ordered",
                        index.name
                    )));
                }
                if index.unique
                    && compare_index_keys(&previous_entry.key, &entry.key, &index.key)
                        == Ordering::Equal
                {
                    return Err(CatalogError::DuplicateKey(index.name.clone()));
                }
            }
            previous = Some(entry);
        }
    }

    Ok(())
}

fn validate_record_against_indexes(
    indexes: &BTreeMap<String, IndexCatalog>,
    document: &Document,
    skip_record_id: Option<u64>,
) -> Result<(), CatalogError> {
    for index in indexes.values() {
        validate_record_against_index(index, document, skip_record_id)?;
    }
    Ok(())
}

fn validate_record_against_index(
    index: &IndexCatalog,
    document: &Document,
    skip_record_id: Option<u64>,
) -> Result<(), CatalogError> {
    if !index.unique {
        return Ok(());
    }

    let candidate_key = index_key_for_document(document, &index.key);
    if index.contains_unique_key(&candidate_key, skip_record_id) {
        return Err(CatalogError::DuplicateKey(index.name.clone()));
    }
    Ok(())
}

fn index_entry_for_document(
    record_id: u64,
    document: &Document,
    key_pattern: &Document,
) -> IndexEntry {
    let mut present_fields = Vec::new();
    for (field, _) in key_pattern {
        if lookup_path_owned(document, field).is_some() {
            present_fields.push(field.clone());
        }
    }
    IndexEntry {
        record_id,
        key: index_key_for_document(document, key_pattern),
        present_fields,
    }
}

fn insert_index_entry(entries: &mut Vec<IndexEntry>, entry: IndexEntry, key_pattern: &Document) {
    let position = entries
        .binary_search_by(|existing| compare_index_entries(existing, &entry, key_pattern))
        .unwrap_or_else(|position| position);
    entries.insert(position, entry);
}

fn build_runtime_index_pages(
    entries: &[IndexEntry],
    key_pattern: &Document,
) -> Result<Vec<RuntimeIndexPage>, CatalogError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut pages = Vec::new();
    let mut page = RuntimeIndexPage::new();
    for entry in entries {
        if !page.entries.is_empty() && !page.can_fit_entry(entry)? {
            pages.push(page);
            page = RuntimeIndexPage::new();
        }
        page.insert_entry(entry.clone(), key_pattern)?;
    }
    if !page.entries.is_empty() {
        pages.push(page);
    }
    Ok(pages)
}

fn page_overlaps_bounds(
    page: &RuntimeIndexPage,
    bounds: &IndexBounds,
    key_pattern: &Document,
) -> bool {
    let Some(first) = page.first_entry() else {
        return false;
    };
    let Some(last) = page.last_entry() else {
        return false;
    };

    if let Some(lower) = bounds.lower.as_ref() {
        let ordering = compare_index_keys(&last.key, &lower.key, key_pattern);
        if ordering.is_lt() {
            return false;
        }
    }

    if let Some(upper) = bounds.upper.as_ref() {
        let ordering = compare_index_keys(&first.key, &upper.key, key_pattern);
        if ordering.is_gt() || (!upper.inclusive && ordering.is_eq()) {
            return false;
        }
    }

    true
}

fn encoded_index_entry_storage_len(entry: &IndexEntry) -> Result<usize, CatalogError> {
    let payload_len = bson::to_vec(entry)
        .map_err(|error| CatalogError::InvalidIndexState(error.to_string()))?
        .len();
    let storage_len = INDEX_PAGE_HEADER_BYTES + INDEX_PAGE_SLOT_BYTES + payload_len;
    if storage_len > INDEX_PAGE_BYTES {
        return Err(CatalogError::InvalidIndexState(
            "index entry exceeds runtime page capacity".to_string(),
        ));
    }
    Ok(INDEX_PAGE_SLOT_BYTES + payload_len)
}

fn insert_batch_mutations<'a>(
    mutations: &'a [CollectionMutation<'a>],
) -> Option<Vec<&'a CollectionRecord>> {
    let mut inserts = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        match mutation {
            CollectionMutation::Insert(record) => inserts.push(*record),
            CollectionMutation::Update(_) | CollectionMutation::Delete(_) => return None,
        }
    }
    Some(inserts)
}

fn stage_insert_entries(
    index: &IndexCatalog,
    inserts: &[&CollectionRecord],
) -> Result<Vec<IndexEntry>, CatalogError> {
    let mut entries = inserts
        .iter()
        .map(|record| index_entry_for_document(record.record_id, &record.document, &index.key))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| compare_index_entries(left, right, &index.key));

    if !index.unique {
        return Ok(entries);
    }

    for pair in entries.windows(2) {
        if compare_index_keys(&pair[0].key, &pair[1].key, &index.key) == Ordering::Equal {
            return Err(CatalogError::DuplicateKey(index.name.clone()));
        }
    }

    for entry in &entries {
        if index.contains_unique_key(&entry.key, None) {
            return Err(CatalogError::DuplicateKey(index.name.clone()));
        }
    }

    Ok(entries)
}

fn stage_index_entries(
    index: &IndexCatalog,
    base_records: &[CollectionRecord],
    base_positions: &HashMap<u64, usize>,
    plan: &CollectionMutationPlan,
) -> Result<Option<Vec<IndexEntry>>, CatalogError> {
    let mut affected_record_ids = HashSet::new();
    let mut replacements = Vec::new();

    for (record_id, state) in &plan.states {
        let base_document = base_positions
            .get(record_id)
            .map(|position| &base_records[*position].document);
        match state {
            StagedRecordState::Present { document, .. } => {
                let new_entry = index_entry_for_document(*record_id, document, &index.key);
                match base_document {
                    Some(base_document) => {
                        let previous_entry =
                            index_entry_for_document(*record_id, base_document, &index.key);
                        if previous_entry != new_entry {
                            affected_record_ids.insert(*record_id);
                            replacements.push(new_entry);
                        }
                    }
                    None => {
                        affected_record_ids.insert(*record_id);
                        replacements.push(new_entry);
                    }
                }
            }
            StagedRecordState::Deleted => {
                if base_document.is_some() {
                    affected_record_ids.insert(*record_id);
                }
            }
        }
    }

    if affected_record_ids.is_empty() {
        return Ok(None);
    }

    replacements.sort_by(|left, right| compare_index_entries(left, right, &index.key));
    let retained = index
        .entries_snapshot()
        .into_iter()
        .filter(|entry| !affected_record_ids.contains(&entry.record_id))
        .collect::<Vec<_>>();
    Ok(Some(merge_index_entries(
        retained,
        replacements,
        &index.key,
        index.unique,
        &index.name,
    )?))
}

fn merge_index_entries(
    retained: Vec<IndexEntry>,
    replacements: Vec<IndexEntry>,
    key_pattern: &Document,
    unique: bool,
    index_name: &str,
) -> Result<Vec<IndexEntry>, CatalogError> {
    let mut merged = Vec::with_capacity(retained.len() + replacements.len());
    let mut retained = retained.into_iter().peekable();
    let mut replacements = replacements.into_iter().peekable();

    while retained.peek().is_some() || replacements.peek().is_some() {
        let next = match (retained.peek(), replacements.peek()) {
            (Some(left), Some(right)) => {
                if compare_index_entries(left, right, key_pattern).is_le() {
                    retained.next().expect("peeked retained entry")
                } else {
                    replacements.next().expect("peeked replacement entry")
                }
            }
            (Some(_), None) => retained.next().expect("peeked retained entry"),
            (None, Some(_)) => replacements.next().expect("peeked replacement entry"),
            (None, None) => break,
        };
        push_index_entry(&mut merged, next, key_pattern, unique, index_name)?;
    }

    Ok(merged)
}

fn adjust_value_frequency(frequencies: &mut Vec<ValueFrequency>, value: &Bson, delta: isize) {
    match frequencies.binary_search_by(|frequency| compare_bson(&frequency.value, value)) {
        Ok(position) => {
            if delta.is_negative() {
                let remove = frequencies[position].count <= delta.unsigned_abs();
                if remove {
                    frequencies.remove(position);
                } else {
                    frequencies[position].count -= delta.unsigned_abs();
                }
            } else {
                frequencies[position].count += delta as usize;
            }
        }
        Err(position) => {
            if delta.is_positive() {
                frequencies.insert(
                    position,
                    ValueFrequency {
                        value: value.clone(),
                        count: delta as usize,
                    },
                );
            }
        }
    }
}

fn push_index_entry(
    merged: &mut Vec<IndexEntry>,
    entry: IndexEntry,
    key_pattern: &Document,
    unique: bool,
    index_name: &str,
) -> Result<(), CatalogError> {
    if unique
        && merged.last().is_some_and(|previous| {
            compare_index_keys(&previous.key, &entry.key, key_pattern).is_eq()
        })
    {
        return Err(CatalogError::DuplicateKey(index_name.to_string()));
    }
    merged.push(entry);
    Ok(())
}

fn compare_index_entries(
    left: &IndexEntry,
    right: &IndexEntry,
    key_pattern: &Document,
) -> Ordering {
    compare_index_keys(&left.key, &right.key, key_pattern)
        .then_with(|| left.record_id.cmp(&right.record_id))
}

fn compare_index_keys(left: &Document, right: &Document, key_pattern: &Document) -> Ordering {
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
        Bson::Int32(direction) => {
            if *direction < 0 {
                -1
            } else {
                1
            }
        }
        Bson::Int64(direction) => {
            if *direction < 0 {
                -1
            } else {
                1
            }
        }
        Bson::Double(direction) => {
            if *direction < 0.0 {
                -1
            } else {
                1
            }
        }
        _ => 1,
    }
}

fn build_internal_summary(children: &[NodeSummary]) -> NodeSummary {
    let min_key = children.first().expect("children").min_key.clone();
    let max_key = children.last().expect("children").max_key.clone();
    let separators = children
        .iter()
        .skip(1)
        .map(|child| child.min_key.clone())
        .collect();
    let node = IndexNode::Internal {
        separators,
        children: children.iter().map(|child| child.node.clone()).collect(),
    };
    NodeSummary {
        node,
        min_key,
        max_key,
    }
}

fn scan_node(
    node: &IndexNode,
    bounds: &IndexBounds,
    key_pattern: &Document,
    entries_out: &mut Vec<IndexEntry>,
) {
    match node {
        IndexNode::Leaf { entries } => {
            for entry in entries {
                if key_within_bounds(&entry.key, bounds, key_pattern) {
                    entries_out.push(entry.clone());
                }
            }
        }
        IndexNode::Internal {
            separators,
            children,
        } => {
            let start = bounds
                .lower
                .as_ref()
                .map_or(0, |bound| child_start_index(separators, bound, key_pattern));
            let end = bounds
                .upper
                .as_ref()
                .map_or(children.len().saturating_sub(1), |bound| {
                    child_end_index(separators, bound, key_pattern)
                });
            if start > end {
                return;
            }

            for child in &children[start..=end] {
                scan_node(child, bounds, key_pattern, entries_out);
            }
        }
    }
}

fn lower_bound_index(entries: &[IndexEntry], bound: &IndexBound, key_pattern: &Document) -> usize {
    entries.partition_point(|entry| {
        let ordering = compare_index_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_lt() || (!bound.inclusive && ordering.is_eq())
    })
}

fn upper_bound_index(entries: &[IndexEntry], bound: &IndexBound, key_pattern: &Document) -> usize {
    entries.partition_point(|entry| {
        let ordering = compare_index_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_lt() || (bound.inclusive && ordering.is_eq())
    })
}

fn child_start_index(separators: &[Document], bound: &IndexBound, key_pattern: &Document) -> usize {
    separators.partition_point(|separator| {
        let ordering = compare_index_keys(separator, &bound.key, key_pattern);
        ordering.is_lt() || (bound.inclusive && ordering.is_eq())
    })
}

fn child_end_index(separators: &[Document], bound: &IndexBound, key_pattern: &Document) -> usize {
    separators.partition_point(|separator| {
        let ordering = compare_index_keys(separator, &bound.key, key_pattern);
        ordering.is_lt() || (bound.inclusive && ordering.is_eq())
    })
}

fn key_within_bounds(key: &Document, bounds: &IndexBounds, key_pattern: &Document) -> bool {
    if let Some(lower) = bounds.lower.as_ref() {
        let ordering = compare_index_keys(key, &lower.key, key_pattern);
        if ordering.is_lt() || (!lower.inclusive && ordering.is_eq()) {
            return false;
        }
    }
    if let Some(upper) = bounds.upper.as_ref() {
        let ordering = compare_index_keys(key, &upper.key, key_pattern);
        if ordering.is_gt() || (!upper.inclusive && ordering.is_eq()) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bson::{Bson, doc};
    use pretty_assertions::assert_eq;

    use super::{
        Catalog, CatalogError, CollectionCatalog, CollectionMutation, CollectionRecord, IndexBound,
        IndexBounds, default_index_name,
    };

    #[test]
    fn creates_collection_with_default_id_index() {
        let mut catalog = Catalog::new();
        catalog
            .create_collection("app", "widgets", doc! { "capped": false })
            .expect("create collection");

        let indexes = catalog.list_indexes("app", "widgets").expect("indexes");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "_id_");
        assert!(indexes[0].entries.is_empty());
    }

    #[test]
    fn creates_and_drops_indexes() {
        let mut catalog = Catalog::new();
        catalog
            .create_collection("app", "widgets", doc! {})
            .expect("create");
        let created = catalog
            .create_indexes(
                "app",
                "widgets",
                &[doc! {
                    "key": { "sku": 1 },
                    "name": "sku_1",
                    "unique": true
                }],
            )
            .expect("create indexes");
        assert_eq!(created[0].name, "sku_1");
        assert_eq!(
            catalog
                .drop_indexes("app", "widgets", "sku_1")
                .expect("drop"),
            1
        );
    }

    #[test]
    fn builds_index_entries_for_existing_records() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "sku": "b" },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "a" },
            })
            .expect("insert");

        let created = super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("create index");

        let index = &created[0];
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.entries[0].record_id, 1);
        assert_eq!(index.entries[1].record_id, 2);
    }

    #[test]
    fn stores_expire_after_seconds_in_index_metadata() {
        let mut collection = CollectionCatalog::new(doc! {});

        let created = super::apply_index_specs(
            &mut collection,
            &[doc! {
                "key": { "createdAt": 1 },
                "name": "createdAt_1",
                "expireAfterSeconds": 1
            }],
        )
        .expect("create index");

        assert_eq!(created[0].expire_after_seconds, Some(1));
        assert_eq!(
            collection
                .indexes
                .get("createdAt_1")
                .expect("index")
                .expire_after_seconds,
            Some(1)
        );
    }

    #[test]
    fn rejects_duplicate_keys_for_unique_index() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "dup" },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "sku": "dup" },
            })
            .expect("insert");

        let error = super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect_err("duplicate unique index should fail");
        assert!(matches!(error, CatalogError::DuplicateKey(name) if name == "sku_1"));
    }

    #[test]
    fn updates_and_deletes_index_entries_incrementally() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "a" },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "sku": "b" },
            })
            .expect("insert");
        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("create index");

        let modified = collection
            .update_record_at(1, doc! { "_id": 2, "sku": "c" })
            .expect("update");
        assert!(modified);
        let entries = &collection.indexes.get("sku_1").expect("index").entries;
        assert_eq!(entries[0].key, doc! { "sku": "a" });
        assert_eq!(entries[1].key, doc! { "sku": "c" });

        let removed = collection.delete_records(&BTreeSet::from([1_u64]));
        assert_eq!(removed, 1);
        collection.validate_indexes().expect("validate");
        let entries = &collection.indexes.get("sku_1").expect("index").entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].record_id, 2);
    }

    #[test]
    fn applies_batched_mutations_and_updates_indexes_once() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "alpha", "qty": 1 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "sku": "beta", "qty": 2 },
            })
            .expect("insert");
        super::apply_index_specs(
            &mut collection,
            &[
                doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true },
                doc! { "key": { "qty": 1 }, "name": "qty_1" },
            ],
        )
        .expect("create indexes");

        let inserted = CollectionRecord {
            record_id: 3,
            document: doc! { "_id": 3, "sku": "gamma", "qty": 7 },
        };
        let updated = CollectionRecord {
            record_id: 1,
            document: doc! { "_id": 1, "sku": "alpha-2", "qty": 4 },
        };
        collection
            .apply_mutations(&[
                CollectionMutation::Update(&updated),
                CollectionMutation::Delete(2),
                CollectionMutation::Insert(&inserted),
            ])
            .expect("apply mutations");

        collection.validate_indexes().expect("validate");
        assert_eq!(collection.records.len(), 2);
        assert_eq!(collection.records[0].record_id, 1);
        assert_eq!(
            collection.records[0]
                .document
                .get_str("sku")
                .expect("updated sku"),
            "alpha-2"
        );
        assert_eq!(collection.records[1].record_id, 3);
        assert_eq!(
            collection.records[1]
                .document
                .get_str("sku")
                .expect("inserted sku"),
            "gamma"
        );

        let sku_entries = &collection.indexes.get("sku_1").expect("sku index").entries;
        assert_eq!(sku_entries.len(), 2);
        assert_eq!(sku_entries[0].key, doc! { "sku": "alpha-2" });
        assert_eq!(sku_entries[0].record_id, 1);
        assert_eq!(sku_entries[1].key, doc! { "sku": "gamma" });
        assert_eq!(sku_entries[1].record_id, 3);

        let qty_entries = &collection.indexes.get("qty_1").expect("qty index").entries;
        assert_eq!(qty_entries.len(), 2);
        assert_eq!(qty_entries[0].record_id, 1);
        assert_eq!(qty_entries[1].record_id, 3);
    }

    #[test]
    fn batched_mutations_reject_duplicate_unique_keys_without_partial_updates() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "sku": "alpha" },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "sku": "beta" },
            })
            .expect("insert");
        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("create index");

        let duplicate = CollectionRecord {
            record_id: 1,
            document: doc! { "_id": 1, "sku": "beta" },
        };
        let original = collection.clone();
        let error = collection
            .apply_mutations(&[CollectionMutation::Update(&duplicate)])
            .expect_err("duplicate key should fail");
        assert!(matches!(error, CatalogError::DuplicateKey(name) if name == "sku_1"));
        assert_eq!(collection, original);
    }

    #[test]
    fn tracks_next_record_id_and_record_positions_without_scanning() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 7,
                document: doc! { "_id": 1, "sku": "alpha" },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 12,
                document: doc! { "_id": 2, "sku": "beta" },
            })
            .expect("insert");
        assert_eq!(collection.next_record_id(), 13);
        assert_eq!(collection.record_position(7), Some(0));
        assert_eq!(collection.record_position(12), Some(1));

        let removed = collection.delete_records(&BTreeSet::from([12_u64]));
        assert_eq!(removed, 1);
        assert_eq!(collection.next_record_id(), 13);
        assert_eq!(collection.record_position(7), Some(0));
        assert_eq!(collection.record_position(12), None);

        collection
            .insert_record(CollectionRecord {
                record_id: collection.next_record_id(),
                document: doc! { "_id": 3, "sku": "gamma" },
            })
            .expect("insert");
        assert_eq!(collection.record_position(13), Some(1));
        assert_eq!(collection.next_record_id(), 14);
    }

    #[test]
    fn scans_index_bounds_across_multiple_leaf_groups() {
        let mut collection = CollectionCatalog::new(doc! {});
        for record_id in 1..=200_u64 {
            collection
                .insert_record(CollectionRecord {
                    record_id,
                    document: doc! {
                        "_id": record_id as i64,
                        "sku": format!("sku-{record_id:03}"),
                    },
                })
                .expect("insert");
        }
        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("create index");

        let index = collection.indexes.get("sku_1").expect("index");
        let record_ids = index.scan_bounds(&IndexBounds {
            lower: Some(IndexBound {
                key: doc! { "sku": "sku-050" },
                inclusive: true,
            }),
            upper: Some(IndexBound {
                key: doc! { "sku": "sku-099" },
                inclusive: true,
            }),
        });
        assert_eq!(record_ids.first().copied(), Some(50));
        assert_eq!(record_ids.last().copied(), Some(99));
        assert_eq!(record_ids.len(), 50);
    }

    #[test]
    fn batched_insert_mutations_preserve_stats_positions_and_bounds_without_tree_rebuild() {
        let mut collection = CollectionCatalog::new(doc! {});
        for record_id in 1..=64_u64 {
            collection
                .insert_record(CollectionRecord {
                    record_id,
                    document: doc! {
                        "_id": record_id as i64,
                        "sku": format!("sku-{record_id:03}"),
                    },
                })
                .expect("insert");
        }
        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
        )
        .expect("create index");

        let inserted = (65..=128_u64)
            .map(|record_id| CollectionRecord {
                record_id,
                document: doc! {
                    "_id": record_id as i64,
                    "sku": format!("sku-{record_id:03}"),
                },
            })
            .collect::<Vec<_>>();
        let mutations = inserted
            .iter()
            .map(CollectionMutation::Insert)
            .collect::<Vec<_>>();
        collection
            .apply_mutations(&mutations)
            .expect("apply mutations");

        let index = collection.indexes.get("sku_1").expect("index");
        assert!(index.tree.root.is_none());
        assert_eq!(index.stats.entry_count, 128);
        assert_eq!(collection.record_position(64), Some(63));
        assert_eq!(collection.record_position(65), Some(64));
        assert_eq!(collection.record_position(128), Some(127));
        assert_eq!(collection.next_record_id(), 129);

        let record_ids = index.scan_bounds(&IndexBounds {
            lower: Some(IndexBound {
                key: doc! { "sku": "sku-080" },
                inclusive: true,
            }),
            upper: Some(IndexBound {
                key: doc! { "sku": "sku-090" },
                inclusive: true,
            }),
        });
        assert_eq!(record_ids.first().copied(), Some(80));
        assert_eq!(record_ids.last().copied(), Some(90));
        assert_eq!(record_ids.len(), 11);
    }

    #[test]
    fn orders_compound_descending_index_entries_by_key_pattern() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "category": "tools", "qty": 9 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "category": "tools", "qty": 3 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 3,
                document: doc! { "_id": 3, "category": "tools", "qty": 5 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 4,
                document: doc! { "_id": 4, "category": "garden", "qty": 1 },
            })
            .expect("insert");

        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1" }],
        )
        .expect("create index");

        let entries = &collection
            .indexes
            .get("category_1_qty_-1")
            .expect("index")
            .entries;
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.record_id)
                .collect::<Vec<_>>(),
            vec![4, 1, 3, 2]
        );
    }

    #[test]
    fn scans_compound_descending_bounds_in_index_order() {
        let mut collection = CollectionCatalog::new(doc! {});
        collection
            .insert_record(CollectionRecord {
                record_id: 1,
                document: doc! { "_id": 1, "category": "tools", "qty": 9 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 2,
                document: doc! { "_id": 2, "category": "tools", "qty": 3 },
            })
            .expect("insert");
        collection
            .insert_record(CollectionRecord {
                record_id: 3,
                document: doc! { "_id": 3, "category": "tools", "qty": 5 },
            })
            .expect("insert");

        super::apply_index_specs(
            &mut collection,
            &[doc! { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1" }],
        )
        .expect("create index");

        let record_ids = collection
            .indexes
            .get("category_1_qty_-1")
            .expect("index")
            .scan_bounds(&IndexBounds {
                lower: Some(IndexBound {
                    key: doc! { "category": "tools", "qty": Bson::MaxKey },
                    inclusive: true,
                }),
                upper: Some(IndexBound {
                    key: doc! { "category": "tools", "qty": Bson::MinKey },
                    inclusive: true,
                }),
            });
        assert_eq!(record_ids, vec![1, 3, 2]);
    }

    #[test]
    fn rejects_duplicate_collection() {
        let mut catalog = Catalog::new();
        catalog
            .create_collection("app", "widgets", doc! {})
            .expect("create");
        let error = catalog
            .create_collection("app", "widgets", doc! {})
            .expect_err("duplicate should fail");
        assert!(matches!(error, CatalogError::NamespaceExists(_, _)));
    }

    #[test]
    fn dropping_the_last_collection_removes_the_database_entry() {
        let mut catalog = Catalog::new();
        catalog
            .create_collection("app", "widgets", doc! {})
            .expect("create");

        catalog
            .drop_collection("app", "widgets")
            .expect("drop collection");

        assert!(!catalog.databases.contains_key("app"));
        assert!(catalog.database_names().is_empty());
    }

    #[test]
    fn derives_default_index_name() {
        assert_eq!(
            default_index_name(&doc! { "sku": 1, "location": -1 }),
            "sku_1_location_-1"
        );
    }
}
