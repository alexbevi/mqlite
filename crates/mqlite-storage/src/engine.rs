use anyhow::Result;
use bson::{Bson, Document};
use mqlite_catalog::{
    Catalog, CollectionCatalog, CollectionRecord, IndexBounds, IndexCatalog, IndexEntry,
};

use crate::{
    CompletedConcurrentCheckpoint, ConcurrentCheckpointJob, PersistedChangeEvent,
    PersistedPlanCacheEntry, WalMutation,
};

pub trait CollectionReadView: Send + Sync {
    fn scan_records(&self) -> Result<Vec<CollectionRecord>>;
    fn record_document(&self, record_id: u64) -> Result<Option<Document>>;
    fn index_names(&self) -> Vec<String>;
    fn index(&self, name: &str) -> Option<&dyn IndexReadView>;
}

pub trait IndexReadView: Send + Sync {
    fn name(&self) -> &str;
    fn key_pattern(&self) -> &Document;
    fn entry_count(&self) -> usize;
    fn scan_entries(&self, bounds: &IndexBounds) -> Result<Vec<IndexEntry>>;
    fn estimate_bounds_count(&self, bounds: &IndexBounds) -> usize;
    fn covers_paths(&self, paths: &std::collections::BTreeSet<String>) -> bool;
    fn estimate_value_count(&self, field: &str, value: &Bson) -> Option<usize>;
    fn estimate_values_count(&self, field: &str, values: &[Bson]) -> Option<usize>;
    fn estimate_range_count(
        &self,
        field: &str,
        lower: Option<(&Bson, bool)>,
        upper: Option<(&Bson, bool)>,
    ) -> Option<usize>;
    fn present_count(&self, field: &str) -> Option<usize>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMetadata {
    pub options: Document,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    pub name: String,
    pub key_pattern: Document,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
}

impl CollectionReadView for CollectionCatalog {
    fn scan_records(&self) -> Result<Vec<CollectionRecord>> {
        Ok(self.records.clone())
    }

    fn record_document(&self, record_id: u64) -> Result<Option<Document>> {
        Ok(self
            .record_position(record_id)
            .and_then(|position| self.records.get(position))
            .map(|record| record.document.clone()))
    }

    fn index_names(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    fn index(&self, name: &str) -> Option<&dyn IndexReadView> {
        self.indexes
            .get(name)
            .map(|index| index as &dyn IndexReadView)
    }
}

impl IndexReadView for IndexCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn key_pattern(&self) -> &Document {
        &self.key
    }

    fn entry_count(&self) -> usize {
        IndexCatalog::entry_count(self)
    }

    fn scan_entries(&self, bounds: &IndexBounds) -> Result<Vec<IndexEntry>> {
        Ok(IndexCatalog::scan_entries(self, bounds))
    }

    fn estimate_bounds_count(&self, bounds: &IndexBounds) -> usize {
        IndexCatalog::estimate_bounds_count(self, bounds)
    }

    fn covers_paths(&self, paths: &std::collections::BTreeSet<String>) -> bool {
        IndexCatalog::covers_paths(self, paths)
    }

    fn estimate_value_count(&self, field: &str, value: &Bson) -> Option<usize> {
        IndexCatalog::estimate_value_count(self, field, value)
    }

    fn estimate_values_count(&self, field: &str, values: &[Bson]) -> Option<usize> {
        IndexCatalog::estimate_values_count(self, field, values)
    }

    fn estimate_range_count(
        &self,
        field: &str,
        lower: Option<(&Bson, bool)>,
        upper: Option<(&Bson, bool)>,
    ) -> Option<usize> {
        IndexCatalog::estimate_range_count(self, field, lower, upper)
    }

    fn present_count(&self, field: &str) -> Option<usize> {
        IndexCatalog::present_count(self, field)
    }
}

pub trait StorageEngine: Send + Sync {
    fn catalog(&self) -> &Catalog;
    fn database_names(&self) -> Result<Vec<String>>;
    fn collection_names(&self, database: &str) -> Result<Vec<String>>;
    fn collection_metadata(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<CollectionMetadata>>;
    fn list_indexes(&self, database: &str, collection: &str) -> Result<Option<Vec<IndexMetadata>>>;
    fn collection_read_view(
        &self,
        database: &str,
        collection: &str,
    ) -> Result<Option<&dyn CollectionReadView>>;
    fn last_applied_sequence(&self) -> u64;
    fn durable_sequence(&self) -> u64;
    fn wal_sync_count(&self) -> usize;
    fn wal_backlog_bytes(&self) -> u64;
    fn change_events(&self) -> &[PersistedChangeEvent];
    fn has_pending_wal(&self) -> bool;
    fn has_concurrent_checkpoint(&self) -> bool;
    fn persisted_plan_cache_entries(&self) -> &[PersistedPlanCacheEntry];
    fn set_persisted_plan_cache_entries(&mut self, entries: Vec<PersistedPlanCacheEntry>);
    fn commit_mutation(&mut self, mutation: WalMutation) -> Result<u64>;
    fn commit_mutation_unflushed(&mut self, mutation: WalMutation) -> Result<u64>;
    fn sync_pending_wal(&mut self) -> Result<u64>;
    fn checkpoint(&mut self) -> Result<()>;
    fn prepare_concurrent_checkpoint(&mut self) -> Result<Option<ConcurrentCheckpointJob>>;
    fn finish_concurrent_checkpoint(
        &mut self,
        completed: CompletedConcurrentCheckpoint,
    ) -> Result<bool>;
    fn abort_concurrent_checkpoint(&mut self) -> bool;
}

pub type BoxedStorageEngine = Box<dyn StorageEngine>;
