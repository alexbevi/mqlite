use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use bson::{Bson, Document, doc};
use mqlite_bson::{compare_documents, lookup_path_owned};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
            insert_index_entry(&mut index.entries, entry);
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
            insert_index_entry(&mut index.entries, entry);
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
}

impl IndexCatalog {
    pub fn new(name: String, key: Document, unique: bool) -> Self {
        Self {
            name,
            key,
            unique,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexEntry {
    pub record_id: u64,
    pub key: Document,
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
            );
        }
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

            if let Some(previous_entry) = previous {
                let ordering = compare_index_entries(previous_entry, entry);
                if ordering != Ordering::Less {
                    return Err(CatalogError::InvalidIndexState(format!(
                        "index `{}` entries are not strictly ordered",
                        index.name
                    )));
                }
                if index.unique
                    && compare_documents(&previous_entry.key, &entry.key) == Ordering::Equal
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
    IndexEntry {
        record_id,
        key: index_key_for_document(document, key_pattern),
    }
}

fn insert_index_entry(entries: &mut Vec<IndexEntry>, entry: IndexEntry) {
    let position = entries
        .binary_search_by(|existing| compare_index_entries(existing, &entry))
        .unwrap_or_else(|position| position);
    entries.insert(position, entry);
}

fn compare_index_entries(left: &IndexEntry, right: &IndexEntry) -> Ordering {
    compare_documents(&left.key, &right.key).then_with(|| left.record_id.cmp(&right.record_id))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bson::doc;
    use pretty_assertions::assert_eq;

    use super::{Catalog, CatalogError, CollectionCatalog, CollectionRecord, default_index_name};

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
