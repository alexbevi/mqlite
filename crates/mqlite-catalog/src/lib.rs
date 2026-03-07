use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
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
                IndexCatalog {
                    name: "_id_".to_string(),
                    key: doc! { "_id": 1 },
                    unique: true,
                },
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexCatalog {
    pub name: String,
    pub key: Document,
    pub unique: bool,
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
        let index = IndexCatalog {
            name: name.clone(),
            key: key.clone(),
            unique,
        };
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

#[cfg(test)]
mod tests {
    use bson::doc;
    use pretty_assertions::assert_eq;

    use super::{Catalog, CatalogError, default_index_name};

    #[test]
    fn creates_collection_with_default_id_index() {
        let mut catalog = Catalog::new();
        catalog
            .create_collection("app", "widgets", doc! { "capped": false })
            .expect("create collection");

        let indexes = catalog.list_indexes("app", "widgets").expect("indexes");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "_id_");
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
