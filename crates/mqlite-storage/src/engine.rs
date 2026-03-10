use anyhow::Result;
use mqlite_catalog::Catalog;

use crate::{
    CompletedConcurrentCheckpoint, ConcurrentCheckpointJob, PersistedChangeEvent,
    PersistedPlanCacheEntry, WalMutation,
};

pub trait StorageEngine: Send + Sync {
    fn catalog(&self) -> &Catalog;
    fn last_applied_sequence(&self) -> u64;
    fn durable_sequence(&self) -> u64;
    fn wal_sync_count(&self) -> usize;
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
