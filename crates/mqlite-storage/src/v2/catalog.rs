#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootSet {
    pub namespace_root_page_id: Option<u64>,
    pub change_stream_root_page_id: Option<u64>,
    pub plan_cache_root_page_id: Option<u64>,
    pub stats_root_page_id: Option<u64>,
    pub freelist_root_page_id: Option<u64>,
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SummaryCounters {
    pub database_count: u64,
    pub collection_count: u64,
    pub index_count: u64,
    pub record_count: u64,
    pub index_entry_count: u64,
    pub change_event_count: u64,
    pub plan_cache_entry_count: u64,
    pub document_bytes: u64,
    pub index_bytes: u64,
    pub page_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMeta {
    pub database: String,
    pub collection: String,
    pub record_root_page_id: Option<u64>,
    pub index_directory_root_page_id: Option<u64>,
    pub options_bytes: Vec<u8>,
    pub next_record_id: u64,
    pub summary: SummaryCounters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMeta {
    pub name: String,
    pub root_page_id: Option<u64>,
    pub key_pattern_bytes: Vec<u8>,
    pub unique: bool,
    pub expire_after_seconds: Option<i64>,
    pub entry_count: u64,
    pub index_bytes: u64,
}
