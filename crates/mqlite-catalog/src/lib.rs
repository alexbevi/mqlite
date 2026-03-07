use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, compare_documents, lookup_path_owned};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const INDEX_TREE_LEAF_CAPACITY: usize = 64;
const INDEX_TREE_BRANCH_CAPACITY: usize = 64;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionRecord {
    pub record_id: u64,
    pub document: Document,
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

impl CollectionCatalog {
    pub fn new(options: Document) -> Self {
        Self {
            options,
            indexes: BTreeMap::from([(
                "_id_".to_string(),
                IndexCatalog::new("_id_".to_string(), doc! { "_id": 1 }, true),
            )]),
            records: Vec::new(),
        }
    }

    pub fn next_record_id(&self) -> u64 {
        self.records
            .iter()
            .map(|record| record.record_id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn documents(&self) -> Vec<Document> {
        self.records
            .iter()
            .map(|record| record.document.clone())
            .collect()
    }

    pub fn hydrate_indexes(&mut self) {
        for index in self.indexes.values_mut() {
            index.rebuild_tree();
        }
    }

    pub fn insert_record(&mut self, record: CollectionRecord) -> Result<(), CatalogError> {
        if self
            .records
            .iter()
            .any(|existing| existing.record_id == record.record_id)
        {
            return Err(CatalogError::InvalidIndexState(format!(
                "duplicate record id {}",
                record.record_id
            )));
        }

        validate_record_against_indexes(&self.indexes, &record.document, None)?;
        for index in self.indexes.values_mut() {
            let entry = index_entry_for_document(record.record_id, &record.document, &index.key);
            insert_index_entry(&mut index.entries, entry, &index.key);
            index.rebuild_tree();
        }
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
            index.entries.retain(|entry| entry.record_id != record_id);
            let entry = index_entry_for_document(record_id, &document, &index.key);
            insert_index_entry(&mut index.entries, entry, &index.key);
            index.rebuild_tree();
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
        for index in self.indexes.values_mut() {
            index
                .entries
                .retain(|entry| !record_ids.contains(&entry.record_id));
            index.rebuild_tree();
        }
        before - self.records.len()
    }

    pub fn validate_indexes(&self) -> Result<(), CatalogError> {
        validate_collection_indexes(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexCatalog {
    pub name: String,
    pub key: Document,
    pub unique: bool,
    pub entries: Vec<IndexEntry>,
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
            entries: Vec::new(),
            tree: IndexTree::default(),
            stats: IndexStats::default(),
        }
    }

    pub fn rebuild_tree(&mut self) {
        self.tree = IndexTree::build(&self.entries);
        self.stats = IndexStats::build(&self.entries, &self.key);
    }

    pub fn scan_entries(&self, bounds: &IndexBounds) -> Vec<IndexEntry> {
        self.tree.scan_entries(bounds, &self.key)
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
        let start = bounds.lower.as_ref().map_or(0, |bound| {
            lower_bound_index(&self.entries, bound, &self.key)
        });
        let end = bounds.upper.as_ref().map_or(self.entries.len(), |bound| {
            upper_bound_index(&self.entries, bound, &self.key)
        });
        end.saturating_sub(start)
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
    let mut created = Vec::new();

    for spec in specs {
        let key = spec
            .get_document("key")
            .map_err(|_| CatalogError::InvalidIndexSpec)?;
        let name = spec
            .get_str("name")
            .map(|value| value.to_string())
            .unwrap_or_else(|_| default_index_name(key));
        if collection.indexes.contains_key(&name) {
            return Err(CatalogError::IndexExists(name));
        }
        let unique = spec.get_bool("unique").unwrap_or(false);
        let mut index = IndexCatalog::new(name.clone(), key.clone(), unique);
        for record in &collection.records {
            validate_record_against_index(&index, &record.document, None)?;
            insert_index_entry(
                &mut index.entries,
                index_entry_for_document(record.record_id, &record.document, &index.key),
                &index.key,
            );
        }
        index.rebuild_tree();
        collection.indexes.insert(name, index.clone());
        created.push(index);
    }

    Ok(created)
}

pub fn drop_indexes_from_collection(
    collection: &mut CollectionCatalog,
    target: &str,
) -> Result<usize, CatalogError> {
    if target == "*" {
        let retained = collection.indexes.remove("_id_");
        let removed = collection.indexes.len();
        collection.indexes.clear();
        if let Some(id_index) = retained {
            collection.indexes.insert(id_index.name.clone(), id_index);
        }
        return Ok(removed);
    }

    if target == "_id_" {
        return Ok(0);
    }

    match collection.indexes.remove(target) {
        Some(_) => Ok(1),
        None => Err(CatalogError::IndexNotFound(target.to_string())),
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
        if index.entries.len() != collection.records.len() {
            return Err(CatalogError::InvalidIndexState(format!(
                "index `{}` entry count {} does not match record count {}",
                index.name,
                index.entries.len(),
                collection.records.len()
            )));
        }

        let mut previous: Option<&IndexEntry> = None;
        let mut indexed_record_ids = BTreeSet::new();
        for entry in &index.entries {
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
    let conflict = index.entries.iter().any(|entry| {
        Some(entry.record_id) != skip_record_id
            && compare_documents(&entry.key, &candidate_key) == Ordering::Equal
    });
    if conflict {
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
        ordering.is_lt() || (bound.inclusive && ordering.is_eq())
    })
}

fn upper_bound_index(entries: &[IndexEntry], bound: &IndexBound, key_pattern: &Document) -> usize {
    entries.partition_point(|entry| {
        let ordering = compare_index_keys(&entry.key, &bound.key, key_pattern);
        ordering.is_lt() || (!bound.inclusive && ordering.is_eq())
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
        Catalog, CatalogError, CollectionCatalog, CollectionRecord, IndexBound, IndexBounds,
        default_index_name,
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
    fn scans_runtime_btree_bounds_across_multiple_leaf_groups() {
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
        assert!(index.tree.height > 1);
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
    fn derives_default_index_name() {
        assert_eq!(
            default_index_name(&doc! { "sku": 1, "location": -1 }),
            "sku_1_location_-1"
        );
    }
}
