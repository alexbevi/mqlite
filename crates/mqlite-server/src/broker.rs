use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use bson::{Bson, Document, doc};
use mqlite_bson::{compare_bson, ensure_object_id, lookup_path_owned, set_path};
use mqlite_catalog::{
    CatalogError, CollectionCatalog, CollectionRecord, IndexBound, IndexBounds, IndexCatalog,
    IndexEntry, apply_index_specs, drop_indexes_from_collection,
};
use mqlite_exec::{CursorError, CursorManager};
use mqlite_ipc::{
    BoxedStream, BrokerManifest, BrokerPaths, IpcListener, broker_paths, cleanup_endpoint,
    remove_manifest, write_manifest,
};
use mqlite_query::{
    CollectionResolver, MatchExpr, QueryError, apply_projection, apply_update, document_matches,
    document_matches_expression, parse_filter, parse_update, run_pipeline_with_resolver,
    upsert_seed_from_query,
};
use mqlite_storage::{
    DatabaseFile, PersistedPlanCacheChoice, PersistedPlanCacheEntry, WalMutation,
};
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

const MAX_BSON_OBJECT_SIZE: i32 = 16 * 1024 * 1024;
const MAX_MESSAGE_SIZE_BYTES: i32 = 48 * 1024 * 1024;
const MAX_WRITE_BATCH_SIZE: i32 = 100_000;
const MAX_OR_BRANCHES: usize = 32;
const MAX_MULTI_INTERVALS: usize = 128;
const MAX_ESTIMATED_INDEX_CANDIDATES: usize = 4;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub database_path: PathBuf,
    pub idle_shutdown_secs: u64,
}

impl BrokerConfig {
    pub fn new(database_path: impl AsRef<Path>, idle_shutdown_secs: u64) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            idle_shutdown_secs,
        }
    }
}

#[derive(Clone)]
pub struct Broker {
    config: BrokerConfig,
    paths: BrokerPaths,
    storage: Arc<RwLock<DatabaseFile>>,
    cursors: Arc<Mutex<CursorManager>>,
    plan_cache: Arc<Mutex<BTreeMap<PlanCacheKey, CachedPlan>>>,
    active_connections: Arc<AtomicUsize>,
    last_activity: Arc<Mutex<Instant>>,
}

#[derive(Debug, Error)]
pub struct CommandError {
    pub code: i32,
    pub code_name: &'static str,
    pub message: String,
}

impl CommandError {
    fn new(code: i32, code_name: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            code_name,
            message: message.into(),
        }
    }

    fn to_document(&self) -> Document {
        doc! {
            "ok": 0.0,
            "errmsg": &self.message,
            "code": self.code,
            "codeName": self.code_name,
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ({}): {}",
            self.code_name, self.code, self.message
        )
    }
}

impl From<CatalogError> for CommandError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::NamespaceExists(_, _) => {
                Self::new(48, "NamespaceExists", error.to_string())
            }
            CatalogError::NamespaceNotFound(_, _) | CatalogError::DatabaseNotFound(_) => {
                Self::new(26, "NamespaceNotFound", error.to_string())
            }
            CatalogError::InvalidIndexSpec => Self::new(2, "BadValue", error.to_string()),
            CatalogError::IndexExists(_) => {
                Self::new(85, "IndexOptionsConflict", error.to_string())
            }
            CatalogError::IndexNotFound(_) => Self::new(27, "IndexNotFound", error.to_string()),
            CatalogError::DuplicateKey(_) => Self::new(11000, "DuplicateKey", error.to_string()),
            CatalogError::InvalidIndexState(_) => Self::new(8, "UnknownError", error.to_string()),
        }
    }
}

impl From<QueryError> for CommandError {
    fn from(error: QueryError) -> Self {
        match error {
            QueryError::UnsupportedStage(_) => Self::new(40324, "Location40324", error.to_string()),
            QueryError::UnsupportedOperator(_) => Self::new(2, "BadValue", error.to_string()),
            QueryError::ExpectedDocument
            | QueryError::InvalidStage
            | QueryError::InvalidStructure => Self::new(9, "FailedToParse", error.to_string()),
            QueryError::InvalidArgument(_) => Self::new(2, "BadValue", error.to_string()),
            QueryError::MixedProjection
            | QueryError::InvalidUpdate
            | QueryError::ExpectedNumeric => Self::new(2, "BadValue", error.to_string()),
        }
    }
}

impl From<CursorError> for CommandError {
    fn from(error: CursorError) -> Self {
        Self::new(43, "CursorNotFound", error.to_string())
    }
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Result<Self> {
        let paths = broker_paths(&config.database_path)?;
        let storage = DatabaseFile::open_or_create(&paths.database_path)?;
        let plan_cache = storage
            .persisted_plan_cache_entries()
            .iter()
            .map(|entry| {
                (
                    plan_cache_key_from_entry(entry),
                    cached_plan_from_entry(entry),
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(Self {
            config,
            paths,
            storage: Arc::new(RwLock::new(storage)),
            cursors: Arc::new(Mutex::new(CursorManager::new())),
            plan_cache: Arc::new(Mutex::new(plan_cache)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            last_activity: Arc::new(Mutex::new(Instant::now())),
        })
    }

    pub fn paths(&self) -> &BrokerPaths {
        &self.paths
    }

    pub async fn serve(self) -> Result<()> {
        let listener = IpcListener::bind(&self.paths.endpoint).await?;
        let manifest = BrokerManifest {
            pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            database_path: self.paths.database_path.clone(),
            endpoint: self.paths.endpoint.clone(),
            fingerprint: self.paths.fingerprint.clone(),
            idle_shutdown_secs: self.config.idle_shutdown_secs,
        };
        write_manifest(&manifest, &self.paths.manifest_path)?;

        let idle_timeout = Duration::from_secs(self.config.idle_shutdown_secs.max(1));
        loop {
            let sleep = tokio::time::sleep(Duration::from_millis(250));
            tokio::pin!(sleep);

            tokio::select! {
                accepted = listener.accept() => {
                    let stream = accepted?;
                    *self.last_activity.lock() = Instant::now();
                    self.active_connections.fetch_add(1, Ordering::SeqCst);
                    let broker = self.clone();
                    tokio::spawn(async move {
                        let _ = broker.handle_connection(stream).await;
                        broker.active_connections.fetch_sub(1, Ordering::SeqCst);
                        *broker.last_activity.lock() = Instant::now();
                    });
                }
                _ = &mut sleep => {
                    if self.active_connections.load(Ordering::SeqCst) == 0
                        && self.last_activity.lock().elapsed() >= idle_timeout
                    {
                        break;
                    }
                }
            }
        }

        let persisted_cache = self.persisted_plan_cache_entries();
        let should_checkpoint = {
            let storage = self.storage.read();
            storage.has_pending_wal()
                || storage.persisted_plan_cache_entries() != persisted_cache.as_slice()
        };
        if should_checkpoint {
            let mut storage = self.storage.write();
            storage.set_persisted_plan_cache_entries(persisted_cache);
            storage.checkpoint()?;
        }
        remove_manifest(&self.paths.manifest_path)?;
        cleanup_endpoint(&self.paths.endpoint)?;
        Ok(())
    }

    fn cached_find_plan(
        &self,
        namespace: String,
        sequence: u64,
        collection: &CollectionCatalog,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
    ) -> Result<CachedFindPlan, CommandError> {
        let cache_key = build_plan_cache_key(&namespace, filter, sort, projection)?;
        let preferred_choice = self
            .plan_cache
            .lock()
            .get(&cache_key)
            .filter(|cached| cached.sequence == sequence)
            .map(|cached| cached.choice.clone());
        let plan = plan_find(
            collection,
            filter,
            sort,
            projection,
            preferred_choice.as_ref(),
        )?;
        self.plan_cache.lock().insert(
            cache_key,
            CachedPlan {
                sequence,
                choice: planned_choice(&plan),
            },
        );
        Ok(CachedFindPlan {
            plan,
            cache_used: preferred_choice.is_some(),
        })
    }

    fn persisted_plan_cache_entries(&self) -> Vec<PersistedPlanCacheEntry> {
        self.plan_cache
            .lock()
            .iter()
            .map(|(key, cached)| PersistedPlanCacheEntry {
                namespace: key.namespace.clone(),
                filter_shape: key.filter_shape.clone(),
                sort_shape: key.sort_shape.clone(),
                projection_shape: key.projection_shape.clone(),
                sequence: cached.sequence,
                choice: cached.choice.clone(),
            })
            .collect()
    }

    async fn handle_connection(&self, mut stream: BoxedStream) -> Result<()> {
        loop {
            let request = match read_op_msg(&mut stream).await {
                Ok(message) => message,
                Err(mqlite_wire::WireError::Io(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(());
                }
                Err(mqlite_wire::WireError::Io(error))
                    if error.kind() == std::io::ErrorKind::BrokenPipe =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };

            let body = request.materialize_command()?;
            let response_body = match self.dispatch(&body) {
                Ok(document) => ok_response(document),
                Err(error) => error.to_document(),
            };
            let response = OpMsg::new(
                request.request_id + 1,
                request.request_id,
                vec![PayloadSection::Body(response_body)],
            );
            write_op_msg(&mut stream, &response).await?;
        }
    }

    fn dispatch(&self, body: &Document) -> Result<Document, CommandError> {
        let command_name = command_name(body)
            .ok_or_else(|| CommandError::new(9, "FailedToParse", "command body is empty"))?;
        if command_name != "hello" && command_name != "isMaster" && command_name != "ismaster" {
            reject_unsupported_envelope(body)?;
        }

        match command_name.as_str() {
            "hello" | "isMaster" | "ismaster" => Ok(self.handle_hello()),
            "ping" => Ok(Document::new()),
            "buildInfo" | "buildinfo" => Ok(doc! {
                "version": env!("CARGO_PKG_VERSION"),
                "gitVersion": "mqlite",
                "maxBsonObjectSize": MAX_BSON_OBJECT_SIZE,
                "maxMessageSizeBytes": MAX_MESSAGE_SIZE_BYTES,
                "maxWriteBatchSize": MAX_WRITE_BATCH_SIZE,
            }),
            "getParameter" => Ok(self.handle_get_parameter(body)),
            "killAllSessions" => Ok(Document::new()),
            "listDatabases" => self.handle_list_databases(body),
            "listCollections" => self.handle_list_collections(body),
            "listIndexes" => self.handle_list_indexes(body),
            "explain" => self.handle_explain(body),
            "create" => self.handle_create(body),
            "dropDatabase" => self.handle_drop_database(body),
            "drop" => self.handle_drop(body),
            "renameCollection" => self.handle_rename_collection(body),
            "createIndexes" => self.handle_create_indexes(body),
            "dropIndexes" => self.handle_drop_indexes(body),
            "insert" => self.handle_insert(body),
            "find" => self.handle_find(body),
            "getMore" => self.handle_get_more(body),
            "killCursors" => self.handle_kill_cursors(body),
            "update" => self.handle_update(body),
            "delete" => self.handle_delete(body),
            "count" => self.handle_count(body),
            "distinct" => self.handle_distinct(body),
            "aggregate" => self.handle_aggregate(body),
            other => Err(CommandError::new(
                115,
                "CommandNotSupported",
                format!("command `{other}` is not supported"),
            )),
        }
    }

    fn handle_hello(&self) -> Document {
        doc! {
            "helloOk": true,
            "isWritablePrimary": true,
            "minWireVersion": 0,
            "maxWireVersion": 21,
            "maxBsonObjectSize": MAX_BSON_OBJECT_SIZE,
            "maxMessageSizeBytes": MAX_MESSAGE_SIZE_BYTES,
            "maxWriteBatchSize": MAX_WRITE_BATCH_SIZE,
            "localTime": bson::DateTime::now(),
        }
    }

    fn handle_get_parameter(&self, body: &Document) -> Document {
        let supported = doc! {
            "authenticationMechanisms": bson::Array::new(),
            "requireApiVersion": false,
        };

        match body.get("getParameter") {
            Some(Bson::String(value)) if value == "*" => supported,
            Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => {
                let mut response = Document::new();
                for (key, value) in body {
                    if key == "getParameter" || key.starts_with('$') {
                        continue;
                    }

                    if truthy_parameter_selector(value) {
                        if let Some(parameter) = supported.get(key) {
                            response.insert(key, parameter.clone());
                        }
                    }
                }
                response
            }
            _ => Document::new(),
        }
    }

    fn handle_list_databases(&self, body: &Document) -> Result<Document, CommandError> {
        let filter = body.get_document("filter").ok().cloned();
        let name_only = body.get_bool("nameOnly").unwrap_or(false);
        let storage = self.storage.read();
        let databases = storage
            .catalog()
            .database_names()
            .into_iter()
            .map(|name| {
                if name_only {
                    doc! { "name": name }
                } else {
                    doc! { "name": name, "sizeOnDisk": 0_i64, "empty": false }
                }
            })
            .filter(|row| {
                filter
                    .as_ref()
                    .is_none_or(|filter| document_matches(row, filter).unwrap_or(false))
            })
            .collect::<Vec<_>>();

        Ok(doc! {
            "databases": databases,
            "totalSize": 0_i64,
            "totalSizeMb": 0_i64,
        })
    }

    fn handle_list_collections(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let filter = body.get_document("filter").ok().cloned();
        let storage = self.storage.read();
        let collections = storage
            .catalog()
            .collection_names(&database)?
            .into_iter()
            .map(|name| {
                let collection = storage
                    .catalog()
                    .get_collection(&database, &name)
                    .expect("collection exists");
                doc! {
                    "name": name,
                    "type": "collection",
                    "options": collection.options.clone(),
                    "info": { "readOnly": false },
                    "idIndex": { "name": "_id_", "key": { "_id": 1 }, "unique": true },
                }
            })
            .filter(|row| {
                filter
                    .as_ref()
                    .is_none_or(|filter| document_matches(row, filter).unwrap_or(false))
            })
            .collect::<Vec<_>>();

        let cursor = self.cursors.lock().open(
            format!("{database}.$cmd.listCollections"),
            collections,
            body_batch_size(body, "cursor"),
            true,
        );
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_list_indexes(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("listIndexes").map_err(|_| {
            CommandError::new(9, "FailedToParse", "listIndexes requires a collection name")
        })?;
        let storage = self.storage.read();
        let indexes = storage
            .catalog()
            .list_indexes(&database, collection)?
            .into_iter()
            .map(index_to_document)
            .collect::<Vec<_>>();
        let cursor = self.cursors.lock().open(
            format!("{database}.{collection}"),
            indexes,
            body_batch_size(body, "cursor"),
            true,
        );
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_explain(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        if let Ok(verbosity) = body.get_str("verbosity") {
            if !matches!(
                verbosity,
                "queryPlanner" | "executionStats" | "allPlansExecution"
            ) {
                return Err(CommandError::new(
                    9,
                    "FailedToParse",
                    "explain verbosity must be queryPlanner, executionStats, or allPlansExecution",
                ));
            }
        }
        let command = body
            .get_document("explain")
            .map_err(|_| CommandError::new(9, "FailedToParse", "explain requires a document"))?;
        let command_name = command_name(command)
            .ok_or_else(|| CommandError::new(9, "FailedToParse", "explain command is empty"))?;
        match command_name.as_str() {
            "find" => self.handle_find_explain(&database, command),
            "delete" => self.handle_delete_explain(&database, command),
            "update" => self.handle_update_explain(&database, command),
            "distinct" => self.handle_distinct_explain(&database, command),
            "findAndModify" => self.handle_find_and_modify_explain(&database, command),
            "aggregate" => self.handle_aggregate_explain(&database, command),
            _ => Err(CommandError::new(
                115,
                "CommandNotSupported",
                format!("explain for `{command_name}` is not supported"),
            )),
        }
    }

    fn handle_find_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("find").map_err(|_| {
            CommandError::new(9, "FailedToParse", "find requires a collection name")
        })?;
        let filter = command
            .get_document("filter")
            .ok()
            .cloned()
            .unwrap_or_default();
        let sort = command.get_document("sort").ok().cloned();
        let projection = command.get_document("projection").ok().cloned();
        self.query_planner_response(
            database,
            collection_name,
            &filter,
            sort.as_ref(),
            projection.as_ref(),
        )
    }

    fn handle_aggregate_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("aggregate").map_err(|_| {
            CommandError::new(
                9,
                "FailedToParse",
                "aggregate explain requires a collection name",
            )
        })?;
        let pipeline = command
            .get_array("pipeline")
            .map_err(|_| {
                CommandError::new(
                    9,
                    "FailedToParse",
                    "aggregate explain requires a pipeline array",
                )
            })?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "pipeline stages must be documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if pipeline.iter().any(|stage| stage.contains_key("$out")) {
            return Err(CommandError::new(
                115,
                "CommandNotSupported",
                "explain for aggregation pipelines with `$out` is not supported",
            ));
        }
        let filter = pipeline
            .first()
            .and_then(|stage| stage.get_document("$match").ok())
            .cloned()
            .unwrap_or_default();
        let sort = pipeline
            .iter()
            .find_map(|stage| stage.get_document("$sort").ok())
            .cloned();
        let cached_plan =
            self.explain_find_plan(database, collection_name, &filter, sort.as_ref(), None)?;
        let namespace = format!("{database}.{collection_name}");
        let mut stages = bson::Array::new();
        stages.push(Bson::Document(doc! {
            "$cursor": {
                "queryPlanner": {
                    "namespace": namespace,
                    "planCacheUsed": cached_plan.cache_used,
                    "winningPlan": cached_plan.plan.to_document(),
                }
            }
        }));
        stages.extend(pipeline.into_iter().map(Bson::Document));
        Ok(doc! { "stages": stages })
    }

    fn handle_delete_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("delete").map_err(|_| {
            CommandError::new(9, "FailedToParse", "delete requires a collection name")
        })?;
        let operation = command
            .get_array("deletes")
            .map_err(|_| CommandError::new(9, "FailedToParse", "delete requires a deletes array"))?
            .first()
            .and_then(Bson::as_document)
            .ok_or_else(|| CommandError::new(9, "FailedToParse", "delete requires an operation"))?;
        let filter = operation
            .get_document("q")
            .map_err(|_| CommandError::new(9, "FailedToParse", "delete operations require `q`"))?;
        self.query_planner_response(database, collection_name, filter, None, None)
    }

    fn handle_update_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("update").map_err(|_| {
            CommandError::new(9, "FailedToParse", "update requires a collection name")
        })?;
        let operation = command
            .get_array("updates")
            .map_err(|_| CommandError::new(9, "FailedToParse", "update requires an updates array"))?
            .first()
            .and_then(Bson::as_document)
            .ok_or_else(|| CommandError::new(9, "FailedToParse", "update requires an operation"))?;
        let filter = operation
            .get_document("q")
            .map_err(|_| CommandError::new(9, "FailedToParse", "update operations require `q`"))?;
        self.query_planner_response(database, collection_name, filter, None, None)
    }

    fn handle_distinct_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("distinct").map_err(|_| {
            CommandError::new(9, "FailedToParse", "distinct requires a collection name")
        })?;
        let filter = command
            .get_document("query")
            .ok()
            .cloned()
            .unwrap_or_default();
        self.query_planner_response(database, collection_name, &filter, None, None)
    }

    fn handle_find_and_modify_explain(
        &self,
        database: &str,
        command: &Document,
    ) -> Result<Document, CommandError> {
        let collection_name = command.get_str("findAndModify").map_err(|_| {
            CommandError::new(
                9,
                "FailedToParse",
                "findAndModify requires a collection name",
            )
        })?;
        let filter = command
            .get_document("query")
            .ok()
            .cloned()
            .unwrap_or_default();
        let sort = command.get_document("sort").ok().cloned();
        self.query_planner_response(database, collection_name, &filter, sort.as_ref(), None)
    }

    fn query_planner_response(
        &self,
        database: &str,
        collection_name: &str,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
    ) -> Result<Document, CommandError> {
        let namespace = format!("{database}.{collection_name}");
        let cached_plan =
            self.explain_find_plan(database, collection_name, filter, sort, projection)?;
        Ok(doc! {
            "queryPlanner": {
                "namespace": namespace,
                "planCacheUsed": cached_plan.cache_used,
                "winningPlan": cached_plan.plan.to_document(),
            }
        })
    }

    fn explain_find_plan(
        &self,
        database: &str,
        collection_name: &str,
        filter: &Document,
        sort: Option<&Document>,
        projection: Option<&Document>,
    ) -> Result<CachedFindPlan, CommandError> {
        let storage = self.storage.read();
        let sequence = storage.last_applied_sequence();
        let namespace = format!("{database}.{collection_name}");
        match storage.catalog().get_collection(database, collection_name) {
            Ok(collection) => {
                self.cached_find_plan(namespace, sequence, collection, filter, sort, projection)
            }
            Err(CatalogError::NamespaceNotFound(_, _)) => Ok(CachedFindPlan {
                plan: PlannedFind::Collection {
                    documents: Vec::new(),
                    record_ids: Vec::new(),
                    docs_examined: 0,
                    sort_required: sort.is_some(),
                },
                cache_used: false,
            }),
            Err(error) => Err(error.into()),
        }
    }

    fn handle_create(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("create").map_err(|_| {
            CommandError::new(9, "FailedToParse", "create requires a collection name")
        })?;

        let mut options = body.clone();
        options.remove("create");
        options.remove("$db");

        let mut storage = self.storage.write();
        if storage
            .catalog()
            .get_collection(&database, collection)
            .is_ok()
        {
            return Err(CatalogError::NamespaceExists(database, collection.to_string()).into());
        }
        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection.to_string(),
                collection_state: CollectionCatalog::new(options),
            })
            .map_err(internal_error)?;
        Ok(Document::new())
    }

    fn handle_drop_database(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collections = {
            let storage = self.storage.read();
            storage
                .catalog()
                .collection_names(&database)
                .unwrap_or_default()
        };

        let mut storage = self.storage.write();
        for collection in collections {
            storage
                .commit_mutation(WalMutation::DropCollection {
                    database: database.clone(),
                    collection,
                })
                .map_err(internal_error)?;
        }

        Ok(doc! { "dropped": database })
    }

    fn handle_drop(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("drop").map_err(|_| {
            CommandError::new(9, "FailedToParse", "drop requires a collection name")
        })?;
        let mut storage = self.storage.write();
        let index_count = storage
            .catalog()
            .get_collection(&database, collection)?
            .indexes
            .len() as i32;
        storage
            .commit_mutation(WalMutation::DropCollection {
                database: database.clone(),
                collection: collection.to_string(),
            })
            .map_err(internal_error)?;
        Ok(doc! {
            "ns": format!("{database}.{collection}"),
            "nIndexesWas": index_count,
        })
    }

    fn handle_rename_collection(&self, body: &Document) -> Result<Document, CommandError> {
        let source_namespace = body.get_str("renameCollection").map_err(|_| {
            CommandError::new(
                9,
                "FailedToParse",
                "renameCollection requires a namespace string",
            )
        })?;
        let target_namespace = body.get_str("to").map_err(|_| {
            CommandError::new(
                9,
                "FailedToParse",
                "renameCollection requires a target namespace",
            )
        })?;
        let drop_target = body.get_bool("dropTarget").unwrap_or(false);
        let (source_database, source_collection) = parse_namespace(source_namespace)?;
        let (target_database, target_collection) = parse_namespace(target_namespace)?;

        let mut storage = self.storage.write();
        let source_state = storage
            .catalog()
            .get_collection(source_database, source_collection)?
            .clone();

        if source_database == target_database && source_collection == target_collection {
            return Ok(Document::new());
        }

        let target_exists = storage
            .catalog()
            .get_collection(target_database, target_collection)
            .is_ok();
        if target_exists {
            if !drop_target {
                return Err(CatalogError::NamespaceExists(
                    target_database.to_string(),
                    target_collection.to_string(),
                )
                .into());
            }
            storage
                .commit_mutation(WalMutation::DropCollection {
                    database: target_database.to_string(),
                    collection: target_collection.to_string(),
                })
                .map_err(internal_error)?;
        }

        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database: target_database.to_string(),
                collection: target_collection.to_string(),
                collection_state: source_state,
            })
            .map_err(internal_error)?;
        storage
            .commit_mutation(WalMutation::DropCollection {
                database: source_database.to_string(),
                collection: source_collection.to_string(),
            })
            .map_err(internal_error)?;
        Ok(Document::new())
    }

    fn handle_create_indexes(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("createIndexes").map_err(|_| {
            CommandError::new(
                9,
                "FailedToParse",
                "createIndexes requires a collection name",
            )
        })?;
        let specs = body
            .get_array("indexes")
            .map_err(|_| {
                CommandError::new(
                    9,
                    "FailedToParse",
                    "createIndexes requires an `indexes` array",
                )
            })?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "index specs must be documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut storage = self.storage.write();
        let collection_exists = storage
            .catalog()
            .get_collection(&database, collection)
            .is_ok();
        let mut collection_state = storage
            .catalog()
            .get_collection(&database, collection)
            .cloned()
            .unwrap_or_else(|_| CollectionCatalog::new(Document::new()));
        let before = collection_state.indexes.len() as i32;
        let created = apply_index_specs(&mut collection_state, &specs)?;
        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection.to_string(),
                collection_state,
            })
            .map_err(internal_error)?;
        Ok(doc! {
            "numIndexesBefore": before,
            "numIndexesAfter": before + created.len() as i32,
            "createdCollectionAutomatically": !collection_exists,
        })
    }

    fn handle_drop_indexes(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("dropIndexes").map_err(|_| {
            CommandError::new(9, "FailedToParse", "dropIndexes requires a collection name")
        })?;
        let target = body.get("index").and_then(Bson::as_str).ok_or_else(|| {
            CommandError::new(9, "FailedToParse", "dropIndexes requires an index target")
        })?;

        let mut storage = self.storage.write();
        let mut collection_state = storage
            .catalog()
            .get_collection(&database, collection)?
            .clone();
        let removed = drop_indexes_from_collection(&mut collection_state, target)?;
        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection.to_string(),
                collection_state,
            })
            .map_err(internal_error)?;
        Ok(doc! { "nIndexesWas": removed as i32 })
    }

    fn handle_insert(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body.get_str("insert").map_err(|_| {
            CommandError::new(9, "FailedToParse", "insert requires a collection name")
        })?;
        let documents = body
            .get_array("documents")
            .map_err(|_| CommandError::new(9, "FailedToParse", "insert requires documents"))?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "documents must be BSON documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut storage = self.storage.write();
        let mut collection_state = storage
            .catalog()
            .get_collection(&database, collection_name)
            .cloned()
            .unwrap_or_else(|_| CollectionCatalog::new(Document::new()));
        let inserted_total = documents.len() as i32;

        for mut document in documents {
            ensure_object_id(&mut document);
            let record_id = collection_state.next_record_id();
            collection_state.insert_record(CollectionRecord {
                record_id,
                document,
            })?;
        }
        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection_name.to_string(),
                collection_state,
            })
            .map_err(internal_error)?;
        Ok(doc! { "n": inserted_total })
    }

    fn handle_find(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("find").map_err(|_| {
            CommandError::new(9, "FailedToParse", "find requires a collection name")
        })?;
        let filter = body
            .get_document("filter")
            .ok()
            .cloned()
            .unwrap_or_default();
        let projection = body.get_document("projection").ok().cloned();
        let sort = body.get_document("sort").ok().cloned();
        let skip = body.get_i64("skip").unwrap_or(0).max(0) as usize;
        let limit = body.get_i64("limit").unwrap_or(0);
        let batch_size = body.get_i64("batchSize").ok();
        let single_batch = body.get_bool("singleBatch").unwrap_or(false);

        // Validate the query shape even when the collection does not exist so unsupported
        // operators never degrade into an empty successful result.
        parse_filter(&filter)?;
        if let Some(projection) = projection.as_ref() {
            let _ = apply_projection(&Document::new(), Some(projection))?;
        }

        let storage = self.storage.read();
        let sequence = storage.last_applied_sequence();
        let namespace = format!("{database}.{collection}");
        let execution = match storage.catalog().get_collection(&database, collection) {
            Ok(collection) => self
                .cached_find_plan(
                    namespace,
                    sequence,
                    collection,
                    &filter,
                    sort.as_ref(),
                    projection.as_ref(),
                )?
                .plan
                .into_execution(),
            Err(CatalogError::NamespaceNotFound(_, _)) => FindExecution {
                documents: Vec::new(),
                sort_covered: false,
                projection_applied: false,
            },
            Err(error) => return Err(error.into()),
        };
        let FindExecution {
            mut documents,
            sort_covered,
            projection_applied,
        } = execution;

        if let Some(sort) = sort.as_ref().filter(|_| !sort_covered) {
            sort_documents(&mut documents, sort);
        }
        documents = documents.into_iter().skip(skip).collect();
        if limit > 0 {
            documents.truncate(limit as usize);
        }
        let documents = if projection_applied {
            documents
        } else {
            documents
                .into_iter()
                .map(|document| apply_projection(&document, projection.as_ref()))
                .collect::<Result<Vec<_>, _>>()?
        };

        let cursor = self.cursors.lock().open(
            format!("{database}.{collection}"),
            documents,
            batch_size,
            single_batch,
        );
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_get_more(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let cursor_id = body
            .get_i64("getMore")
            .or_else(|_| body.get_i32("getMore").map(i64::from))
            .map_err(|_| CommandError::new(9, "FailedToParse", "getMore requires a cursor id"))?;
        let collection = body.get_str("collection").map_err(|_| {
            CommandError::new(9, "FailedToParse", "getMore requires a collection name")
        })?;
        let batch = self
            .cursors
            .lock()
            .get_more(cursor_id, body.get_i64("batchSize").ok())?;
        Ok(doc! {
            "cursor": {
                "id": batch.cursor_id,
                "ns": format!("{database}.{collection}"),
                "nextBatch": batch.documents,
            }
        })
    }

    fn handle_kill_cursors(&self, body: &Document) -> Result<Document, CommandError> {
        let cursor_ids = body
            .get_array("cursors")
            .map_err(|_| {
                CommandError::new(9, "FailedToParse", "killCursors requires a cursors array")
            })?
            .iter()
            .map(|value| {
                value.as_i64().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "cursor ids must be integers")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut manager = self.cursors.lock();
        let mut killed = Vec::new();
        let mut not_found = Vec::new();
        for cursor_id in cursor_ids {
            if manager.kill(cursor_id) {
                killed.push(Bson::Int64(cursor_id));
            } else {
                not_found.push(Bson::Int64(cursor_id));
            }
        }

        Ok(doc! {
            "cursorsKilled": killed,
            "cursorsNotFound": not_found,
            "cursorsAlive": Vec::<Bson>::new(),
            "cursorsUnknown": Vec::<Bson>::new(),
        })
    }

    fn handle_update(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body.get_str("update").map_err(|_| {
            CommandError::new(9, "FailedToParse", "update requires a collection name")
        })?;
        let operations = body
            .get_array("updates")
            .map_err(|_| CommandError::new(9, "FailedToParse", "update requires an updates array"))?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "updates must be documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut storage = self.storage.write();
        let mut collection_state = storage
            .catalog()
            .get_collection(&database, collection_name)
            .cloned()
            .unwrap_or_else(|_| CollectionCatalog::new(Document::new()));

        let mut matched = 0_i32;
        let mut modified = 0_i32;
        let mut upserted = Vec::new();

        for (operation_index, operation) in operations.iter().enumerate() {
            let query = operation.get_document("q").map_err(|_| {
                CommandError::new(9, "FailedToParse", "update operations require `q`")
            })?;
            let update = operation.get_document("u").map_err(|_| {
                CommandError::new(9, "FailedToParse", "update operations require `u`")
            })?;
            let update_spec = parse_update(update)?;
            let multi = operation.get_bool("multi").unwrap_or(false);
            let upsert = operation.get_bool("upsert").unwrap_or(false);

            let matching_indexes = collection_state
                .records
                .iter()
                .enumerate()
                .filter_map(|(index, record)| {
                    document_matches(&record.document, query)
                        .ok()
                        .and_then(|matches| matches.then_some(index))
                })
                .collect::<Vec<_>>();

            if matching_indexes.is_empty() {
                if upsert {
                    let mut document = upsert_seed_from_query(query);
                    apply_update(&mut document, &update_spec)?;
                    let upserted_id = ensure_object_id(&mut document);
                    let record_id = collection_state.next_record_id();
                    collection_state.insert_record(CollectionRecord {
                        record_id,
                        document,
                    })?;
                    upserted.push(doc! { "index": operation_index as i32, "_id": upserted_id });
                }
                continue;
            }

            let mut touched = 0;
            for document_index in matching_indexes {
                let original = collection_state.records[document_index].document.clone();
                let mut updated = original.clone();
                apply_update(&mut updated, &update_spec)?;
                matched += 1;
                if updated != original
                    && collection_state.update_record_at(document_index, updated)?
                {
                    modified += 1;
                }
                touched += 1;
                if !multi && touched >= 1 {
                    break;
                }
            }
        }

        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection_name.to_string(),
                collection_state,
            })
            .map_err(internal_error)?;
        Ok(doc! {
            "n": matched + upserted.len() as i32,
            "nModified": modified,
            "nUpserted": upserted.len() as i32,
            "upserted": upserted,
        })
    }

    fn handle_delete(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body.get_str("delete").map_err(|_| {
            CommandError::new(9, "FailedToParse", "delete requires a collection name")
        })?;
        let operations = body
            .get_array("deletes")
            .map_err(|_| CommandError::new(9, "FailedToParse", "delete requires a deletes array"))?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "deletes must be documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let validated_operations = operations
            .into_iter()
            .map(|operation| {
                let query = operation.get_document("q").map_err(|_| {
                    CommandError::new(9, "FailedToParse", "delete operations require `q`")
                })?;
                parse_filter(query)?;
                Ok((query.clone(), operation.get_i32("limit").unwrap_or(0)))
            })
            .collect::<Result<Vec<_>, CommandError>>()?;

        let mut storage = self.storage.write();
        let mut collection_state =
            match storage.catalog().get_collection(&database, collection_name) {
                Ok(collection) => collection.clone(),
                Err(CatalogError::NamespaceNotFound(_, _)) => return Ok(doc! { "n": 0 }),
                Err(error) => return Err(error.into()),
            };
        let mut deleted = 0_i32;

        for (query, limit) in validated_operations {
            let mut removed_record_ids = BTreeSet::new();
            for record in &collection_state.records {
                if !document_matches(&record.document, &query).unwrap_or(false) {
                    continue;
                }
                removed_record_ids.insert(record.record_id);
                if limit == 1 {
                    break;
                }
            }
            deleted += collection_state.delete_records(&removed_record_ids) as i32;
        }

        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database,
                collection: collection_name.to_string(),
                collection_state,
            })
            .map_err(internal_error)?;
        Ok(doc! { "n": deleted })
    }

    fn handle_count(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body.get_str("count").map_err(|_| {
            CommandError::new(9, "FailedToParse", "count requires a collection name")
        })?;
        let query = body.get_document("query").ok().cloned().unwrap_or_default();
        let skip = body.get_i64("skip").unwrap_or(0).max(0) as usize;
        let limit = body.get_i64("limit").unwrap_or(0);

        let storage = self.storage.read();
        let mut matches = storage
            .catalog()
            .get_collection(&database, collection_name)
            .map(|collection| {
                collection
                    .records
                    .iter()
                    .filter(|record| document_matches(&record.document, &query).unwrap_or(false))
                    .count()
            })
            .unwrap_or(0);

        matches = matches.saturating_sub(skip);
        if limit > 0 {
            matches = matches.min(limit as usize);
        }
        Ok(doc! { "n": matches as i64 })
    }

    fn handle_distinct(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body.get_str("distinct").map_err(|_| {
            CommandError::new(9, "FailedToParse", "distinct requires a collection name")
        })?;
        let key = body
            .get_str("key")
            .map_err(|_| CommandError::new(9, "FailedToParse", "distinct requires a key"))?;
        let query = body.get_document("query").ok().cloned().unwrap_or_default();

        let storage = self.storage.read();
        let mut seen = Vec::<Bson>::new();
        if let Ok(collection) = storage.catalog().get_collection(&database, collection_name) {
            for record in &collection.records {
                if !document_matches(&record.document, &query).unwrap_or(false) {
                    continue;
                }
                let value = lookup_path_owned(&record.document, key).unwrap_or(Bson::Null);
                if !seen.iter().any(|existing| existing == &value) {
                    seen.push(value);
                }
            }
        }

        Ok(doc! { "values": seen })
    }

    fn handle_aggregate(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let pipeline = body
            .get_array("pipeline")
            .map_err(|_| {
                CommandError::new(9, "FailedToParse", "aggregate requires a pipeline array")
            })?
            .iter()
            .map(|value| {
                value.as_document().cloned().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "pipeline stages must be documents")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch_size = body_batch_size(body, "cursor");
        let is_collectionless = matches!(
            body.get("aggregate"),
            Some(Bson::Int32(1)) | Some(Bson::Int64(1))
        );
        let out_target = pipeline
            .last()
            .and_then(|stage| stage.get("$out"))
            .map(parse_out_target)
            .transpose()?;
        let merge_target = if out_target.is_none() {
            pipeline
                .last()
                .and_then(|stage| stage.get("$merge"))
                .map(parse_merge_target)
                .transpose()?
        } else {
            None
        };
        let execution_pipeline = if out_target.is_some() || merge_target.is_some() {
            &pipeline[..pipeline.len().saturating_sub(1)]
        } else {
            &pipeline[..]
        };

        let starts_with_documents = pipeline
            .first()
            .and_then(|stage| stage.keys().next())
            .is_some_and(|stage_name| stage_name == "$documents");
        let starts_with_coll_stats = pipeline
            .first()
            .and_then(|stage| stage.keys().next())
            .is_some_and(|stage_name| stage_name == "$collStats");
        let starts_with_current_op = pipeline
            .first()
            .and_then(|stage| stage.keys().next())
            .is_some_and(|stage_name| stage_name == "$currentOp");
        let starts_with_index_stats = pipeline
            .first()
            .and_then(|stage| stage.keys().next())
            .is_some_and(|stage_name| stage_name == "$indexStats");

        if starts_with_coll_stats && is_collectionless {
            return Err(CommandError::new(
                73,
                "InvalidNamespace",
                "$collStats must be run against a collection namespace",
            ));
        }
        if starts_with_index_stats && is_collectionless {
            return Err(CommandError::new(
                73,
                "InvalidNamespace",
                "$indexStats must be run against a collection namespace",
            ));
        }
        if starts_with_current_op && !is_collectionless {
            return Err(CommandError::new(
                73,
                "InvalidNamespace",
                "$currentOp must be run against the 'admin' database with {aggregate: 1}",
            ));
        }

        if is_collectionless && starts_with_current_op {
            return self.handle_current_op_aggregate(
                body,
                &database,
                batch_size,
                execution_pipeline,
                out_target,
                merge_target,
            );
        }
        if starts_with_coll_stats {
            let collection_name = body.get_str("aggregate").map_err(|_| {
                CommandError::new(9, "FailedToParse", "aggregate requires a collection name")
            })?;
            return self.handle_coll_stats_aggregate(
                &database,
                collection_name,
                batch_size,
                execution_pipeline,
                out_target,
                merge_target,
            );
        }
        if starts_with_index_stats {
            let collection_name = body.get_str("aggregate").map_err(|_| {
                CommandError::new(9, "FailedToParse", "aggregate requires a collection name")
            })?;
            return self.handle_index_stats_aggregate(
                &database,
                collection_name,
                batch_size,
                execution_pipeline,
                out_target,
                merge_target,
            );
        }

        let (namespace, results) = {
            let storage = self.storage.read();
            let resolver = BrokerCollectionResolver {
                catalog: storage.catalog(),
            };
            let (namespace, input) = match body.get("aggregate") {
                Some(Bson::String(collection_name)) => {
                    if starts_with_documents {
                        return Err(CommandError::new(
                            73,
                            "InvalidNamespace",
                            "$documents is only valid for collectionless aggregates and subpipelines",
                        ));
                    }
                    let input = storage
                        .catalog()
                        .get_collection(&database, collection_name)
                        .map(|collection| collection.documents())
                        .unwrap_or_default();
                    (format!("{database}.{collection_name}"), input)
                }
                Some(Bson::Int32(1)) | Some(Bson::Int64(1)) => {
                    if !starts_with_documents {
                        return Err(CommandError::new(
                            73,
                            "InvalidNamespace",
                            "collectionless aggregate requires $documents as the first stage",
                        ));
                    }
                    (format!("{database}.$cmd.aggregate"), Vec::new())
                }
                _ => {
                    return Err(CommandError::new(
                        9,
                        "FailedToParse",
                        "aggregate requires a collection name or 1 for collectionless aggregate",
                    ));
                }
            };
            let results =
                run_pipeline_with_resolver(input, execution_pipeline, &database, &resolver)?;
            (namespace, results)
        };
        if let Some(target) = out_target {
            let target_database = target.database.as_deref().unwrap_or(&database);
            self.write_out_collection(target_database, &target.collection, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }
        if let Some(target) = merge_target {
            self.merge_into_collection(&database, &target, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }
        let cursor = self
            .cursors
            .lock()
            .open(namespace, results, batch_size, false);
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_coll_stats_aggregate(
        &self,
        database: &str,
        collection_name: &str,
        batch_size: Option<i64>,
        execution_pipeline: &[Document],
        out_target: Option<OutTarget>,
        merge_target: Option<MergeTarget>,
    ) -> Result<Document, CommandError> {
        let namespace = format!("{database}.{collection_name}");
        let results = {
            let storage = self.storage.read();
            let resolver = BrokerCollectionResolver {
                catalog: storage.catalog(),
            };
            let collection = storage
                .catalog()
                .get_collection(database, collection_name)?;
            let coll_stats = execution_pipeline
                .first()
                .and_then(|stage| stage.get("$collStats"))
                .ok_or_else(|| {
                    CommandError::new(
                        9,
                        "FailedToParse",
                        "$collStats aggregation requires a $collStats stage document",
                    )
                })?;
            let coll_stats = parse_coll_stats_stage(coll_stats)?;
            let stats_document = build_coll_stats_result(
                &namespace,
                collection,
                coll_stats.storage_stats_scale,
                coll_stats.include_count,
            );
            if execution_pipeline.len() > 1 {
                run_pipeline_with_resolver(
                    vec![stats_document],
                    &execution_pipeline[1..],
                    database,
                    &resolver,
                )?
            } else {
                vec![stats_document]
            }
        };

        if let Some(target) = out_target {
            let target_database = target.database.as_deref().unwrap_or(database);
            self.write_out_collection(target_database, &target.collection, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }
        if let Some(target) = merge_target {
            self.merge_into_collection(database, &target, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }

        let cursor = self
            .cursors
            .lock()
            .open(namespace, results, batch_size, false);
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_index_stats_aggregate(
        &self,
        database: &str,
        collection_name: &str,
        batch_size: Option<i64>,
        execution_pipeline: &[Document],
        out_target: Option<OutTarget>,
        merge_target: Option<MergeTarget>,
    ) -> Result<Document, CommandError> {
        let namespace = format!("{database}.{collection_name}");
        let results = {
            let storage = self.storage.read();
            let resolver = BrokerCollectionResolver {
                catalog: storage.catalog(),
            };
            let collection = storage
                .catalog()
                .get_collection(database, collection_name)?;
            let index_stats = execution_pipeline
                .first()
                .and_then(|stage| stage.get("$indexStats"))
                .ok_or_else(|| {
                    CommandError::new(
                        9,
                        "FailedToParse",
                        "$indexStats aggregation requires an $indexStats stage document",
                    )
                })?;
            parse_index_stats_stage(index_stats)?;
            let index_stats_documents = build_index_stats_results(collection);
            if execution_pipeline.len() > 1 {
                run_pipeline_with_resolver(
                    index_stats_documents,
                    &execution_pipeline[1..],
                    database,
                    &resolver,
                )?
            } else {
                index_stats_documents
            }
        };

        if let Some(target) = out_target {
            let target_database = target.database.as_deref().unwrap_or(database);
            self.write_out_collection(target_database, &target.collection, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }
        if let Some(target) = merge_target {
            self.merge_into_collection(database, &target, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }

        let cursor = self
            .cursors
            .lock()
            .open(namespace, results, batch_size, false);
        Ok(cursor_document(cursor, "firstBatch"))
    }

    fn handle_current_op_aggregate(
        &self,
        body: &Document,
        database: &str,
        batch_size: Option<i64>,
        execution_pipeline: &[Document],
        out_target: Option<OutTarget>,
        merge_target: Option<MergeTarget>,
    ) -> Result<Document, CommandError> {
        let current_op = body
            .get_array("pipeline")
            .ok()
            .and_then(|pipeline| pipeline.first())
            .and_then(Bson::as_document)
            .and_then(|stage| stage.get_document("$currentOp").ok());
        let current_op = current_op.ok_or_else(|| {
            CommandError::new(
                9,
                "FailedToParse",
                "collectionless $currentOp aggregation requires a $currentOp stage document",
            )
        })?;

        if database != "admin" {
            return Err(CommandError::new(
                73,
                "InvalidNamespace",
                "$currentOp must be run against the 'admin' database with {aggregate: 1}",
            ));
        }

        if !current_op.get_bool("localOps").unwrap_or(false) {
            return Err(CommandError::new(
                115,
                "CommandNotSupported",
                "collectionless $currentOp requires localOps: true",
            ));
        }

        for key in current_op.keys() {
            if key != "localOps" {
                return Err(CommandError::new(
                    115,
                    "CommandNotSupported",
                    "collectionless $currentOp only supports localOps",
                ));
            }
        }

        let mut operation = Document::new();
        operation.insert("type", "op");
        operation.insert("ns", format!("{database}.$cmd.aggregate"));
        operation.insert("command", Bson::Document(body.clone()));

        let namespace = format!("{database}.$cmd.aggregate");
        let results = if execution_pipeline.len() > 1 {
            let storage = self.storage.read();
            let resolver = BrokerCollectionResolver {
                catalog: storage.catalog(),
            };
            run_pipeline_with_resolver(
                vec![operation],
                &execution_pipeline[1..],
                database,
                &resolver,
            )?
        } else {
            vec![operation]
        };

        if let Some(target) = out_target {
            let target_database = target.database.as_deref().unwrap_or(database);
            self.write_out_collection(target_database, &target.collection, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }
        if let Some(target) = merge_target {
            self.merge_into_collection(database, &target, results)?;
            let cursor = self
                .cursors
                .lock()
                .open(namespace, Vec::new(), batch_size, false);
            return Ok(cursor_document(cursor, "firstBatch"));
        }

        let cursor = self
            .cursors
            .lock()
            .open(namespace, results, batch_size, false);
        Ok(cursor_document(cursor, "firstBatch"))
    }
}

#[derive(Debug, Clone, Copy)]
struct CollStatsStage {
    include_count: bool,
    storage_stats_scale: Option<i64>,
}

fn parse_coll_stats_stage(spec: &Bson) -> Result<CollStatsStage, CommandError> {
    let spec = spec.as_document().ok_or_else(|| {
        CommandError::new(9, "FailedToParse", "$collStats must take a nested object")
    })?;
    let mut include_count = false;
    let mut storage_stats_scale = None;

    for (key, value) in spec {
        match key.as_str() {
            "count" => {
                let document = value.as_document().ok_or_else(|| {
                    CommandError::new(9, "FailedToParse", "$collStats count must be an object")
                })?;
                if !document.is_empty() {
                    return Err(CommandError::new(
                        9,
                        "FailedToParse",
                        "$collStats count must be an empty object",
                    ));
                }
                include_count = true;
            }
            "storageStats" => {
                storage_stats_scale = Some(parse_coll_stats_storage_spec(value)?);
            }
            "latencyStats"
            | "queryExecStats"
            | "operationStats"
            | "targetAllNodes"
            | "$_requestOnTimeseriesView" => {
                return Err(CommandError::new(
                    115,
                    "CommandNotSupported",
                    format!("$collStats option `{key}` is not supported"),
                ));
            }
            _ => {
                return Err(CommandError::new(
                    9,
                    "FailedToParse",
                    format!("unsupported $collStats option `{key}`"),
                ));
            }
        }
    }

    Ok(CollStatsStage {
        include_count,
        storage_stats_scale,
    })
}

fn parse_coll_stats_storage_spec(spec: &Bson) -> Result<i64, CommandError> {
    let spec = spec.as_document().ok_or_else(|| {
        CommandError::new(
            9,
            "FailedToParse",
            "$collStats storageStats must be an object",
        )
    })?;
    let mut scale = 1_i64;
    for (key, value) in spec {
        match key.as_str() {
            "scale" => {
                scale = match value {
                    Bson::Int32(value) => i64::from(*value),
                    Bson::Int64(value) => *value,
                    Bson::Double(value) if value.fract() == 0.0 => *value as i64,
                    _ => {
                        return Err(CommandError::new(
                            9,
                            "FailedToParse",
                            "$collStats storageStats scale must be a positive integer",
                        ));
                    }
                };
                if scale <= 0 {
                    return Err(CommandError::new(
                        9,
                        "FailedToParse",
                        "$collStats storageStats scale must be a positive integer",
                    ));
                }
            }
            "verbose" | "waitForLock" | "numericOnly" => {
                value.as_bool().ok_or_else(|| {
                    CommandError::new(
                        9,
                        "FailedToParse",
                        format!("$collStats storageStats option `{key}` must be a boolean value"),
                    )
                })?;
            }
            _ => {
                return Err(CommandError::new(
                    9,
                    "FailedToParse",
                    format!("unsupported $collStats storageStats option `{key}`"),
                ));
            }
        }
    }
    Ok(scale)
}

fn build_coll_stats_result(
    namespace: &str,
    collection: &CollectionCatalog,
    storage_stats_scale: Option<i64>,
    include_count: bool,
) -> Document {
    let count = collection.records.len() as i64;
    let total_size = collection
        .records
        .iter()
        .map(|record| bson::to_vec(&record.document).unwrap_or_default().len() as i64)
        .sum::<i64>();
    let average_size = if count == 0 { 0 } else { total_size / count };

    let mut result = doc! {
        "ns": namespace,
    };
    if include_count {
        result.insert("count", count);
    }
    if let Some(scale) = storage_stats_scale {
        let mut total_index_size = 0_i64;
        let mut index_sizes = Document::new();
        for (name, index) in &collection.indexes {
            let size = approximate_index_size(index);
            total_index_size += size;
            index_sizes.insert(name.clone(), size / scale.max(1));
        }
        result.insert(
            "storageStats",
            doc! {
                "count": count,
                "size": total_size / scale.max(1),
                "avgObjSize": average_size / scale.max(1),
                "storageSize": total_size / scale.max(1),
                "nindexes": collection.indexes.len() as i64,
                "totalIndexSize": total_index_size / scale.max(1),
                "indexSizes": index_sizes,
            },
        );
    }
    result
}

fn approximate_index_size(index: &IndexCatalog) -> i64 {
    index
        .entries
        .iter()
        .map(|entry| bson::to_vec(&entry.key).unwrap_or_default().len() as i64 + 8)
        .sum()
}

fn parse_index_stats_stage(spec: &Bson) -> Result<(), CommandError> {
    let spec = spec.as_document().ok_or_else(|| {
        CommandError::new(9, "FailedToParse", "$indexStats must take a nested object")
    })?;
    if !spec.is_empty() {
        return Err(CommandError::new(
            9,
            "FailedToParse",
            "$indexStats stage specification must be an empty object",
        ));
    }
    Ok(())
}

fn build_index_stats_results(collection: &CollectionCatalog) -> Vec<Document> {
    let since = bson::DateTime::now();
    collection
        .indexes
        .values()
        .map(|index| {
            doc! {
                "name": index.name.clone(),
                "key": index.key.clone(),
                "spec": {
                    "name": index.name.clone(),
                    "key": index.key.clone(),
                    "unique": index.unique,
                },
                "accesses": {
                    "ops": 0_i64,
                    "since": since,
                },
                "host": "mqlite",
            }
        })
        .collect()
}

impl Broker {
    fn write_out_collection(
        &self,
        database: &str,
        collection: &str,
        results: Vec<Document>,
    ) -> Result<(), CommandError> {
        let mut storage = self.storage.write();
        let existing_options = storage
            .catalog()
            .get_collection(database, collection)
            .ok()
            .map(|catalog| catalog.options.clone())
            .unwrap_or_default();
        let mut collection_state = CollectionCatalog::new(existing_options);
        for mut document in results {
            ensure_object_id(&mut document);
            let record_id = collection_state.next_record_id();
            collection_state.insert_record(CollectionRecord {
                record_id,
                document,
            })?;
        }
        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database: database.to_string(),
                collection: collection.to_string(),
                collection_state,
            })
            .map(|_| ())
            .map_err(internal_error)
    }

    fn merge_into_collection(
        &self,
        default_database: &str,
        target: &MergeTarget,
        results: Vec<Document>,
    ) -> Result<(), CommandError> {
        let database = target.database.as_deref().unwrap_or(default_database);
        let mut storage = self.storage.write();
        let mut collection_state = storage
            .catalog()
            .get_collection(database, &target.collection)
            .cloned()
            .unwrap_or_else(|_| CollectionCatalog::new(Document::new()));
        let mut deferred_error = None;

        for mut document in results {
            if target.on_fields.iter().any(|field| field == "_id") {
                ensure_object_id(&mut document);
            }

            let matches = collection_state
                .records
                .iter()
                .enumerate()
                .filter(|(_, record)| {
                    merge_fields_match(&record.document, &document, &target.on_fields)
                })
                .map(|(position, _)| position)
                .collect::<Vec<_>>();

            if matches.len() > 1 {
                deferred_error.get_or_insert_with(|| {
                    CommandError::new(
                        11000,
                        "DuplicateKey",
                        "merge target contains duplicate `on` field values",
                    )
                });
                continue;
            }

            if let Some(position) = matches.first().copied() {
                let existing = collection_state.records[position].document.clone();
                let outcome = match target.when_matched {
                    MergeWhenMatched::Replace => collection_state
                        .update_record_at(position, document)
                        .map(|_| ()),
                    MergeWhenMatched::Merge => {
                        let mut merged = existing;
                        for (field, value) in document {
                            merged.insert(field, value);
                        }
                        collection_state
                            .update_record_at(position, merged)
                            .map(|_| ())
                    }
                    MergeWhenMatched::KeepExisting => Ok(()),
                    MergeWhenMatched::Fail => Err(CatalogError::DuplicateKey(
                        "merge encountered a matching target document".to_string(),
                    )),
                };

                if let Err(error) = outcome {
                    deferred_error.get_or_insert_with(|| match error {
                        CatalogError::DuplicateKey(message) => {
                            CommandError::new(11000, "DuplicateKey", message)
                        }
                        other => other.into(),
                    });
                }
                continue;
            }

            let outcome = match target.when_not_matched {
                MergeWhenNotMatched::Insert => collection_state.insert_record(CollectionRecord {
                    record_id: collection_state.next_record_id(),
                    document,
                }),
                MergeWhenNotMatched::Discard => Ok(()),
                MergeWhenNotMatched::Fail => Err(CatalogError::InvalidIndexState(
                    "merge did not find a matching target document".to_string(),
                )),
            };

            if let Err(error) = outcome {
                deferred_error.get_or_insert_with(|| match target.when_not_matched {
                    MergeWhenNotMatched::Fail => CommandError::new(
                        13113,
                        "MergeStageNoMatchingDocument",
                        "merge did not find a matching target document",
                    ),
                    _ => error.into(),
                });
            }
        }

        storage
            .commit_mutation(WalMutation::ReplaceCollection {
                database: database.to_string(),
                collection: target.collection.clone(),
                collection_state,
            })
            .map_err(internal_error)?;

        if let Some(error) = deferred_error {
            return Err(error);
        }

        Ok(())
    }
}

struct OutTarget {
    database: Option<String>,
    collection: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeWhenMatched {
    Replace,
    Merge,
    KeepExisting,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeWhenNotMatched {
    Insert,
    Discard,
    Fail,
}

struct MergeTarget {
    database: Option<String>,
    collection: String,
    on_fields: Vec<String>,
    when_matched: MergeWhenMatched,
    when_not_matched: MergeWhenNotMatched,
}

fn parse_out_target(value: &Bson) -> Result<OutTarget, CommandError> {
    match value {
        Bson::String(collection) => Ok(OutTarget {
            database: None,
            collection: collection.clone(),
        }),
        Bson::Document(document) => {
            let mut database = None;
            let mut collection = None;
            for (field, value) in document {
                match field.as_str() {
                    "db" => {
                        database = Some(
                            value
                                .as_str()
                                .ok_or_else(|| {
                                    CommandError::new(
                                        9,
                                        "FailedToParse",
                                        "`$out.db` must be a string",
                                    )
                                })?
                                .to_string(),
                        );
                    }
                    "coll" => {
                        collection = Some(
                            value
                                .as_str()
                                .ok_or_else(|| {
                                    CommandError::new(
                                        9,
                                        "FailedToParse",
                                        "`$out.coll` must be a string",
                                    )
                                })?
                                .to_string(),
                        );
                    }
                    "timeseries" => {
                        return Err(CommandError::new(
                            115,
                            "CommandNotSupported",
                            "aggregation stage `$out` does not support time-series targets",
                        ));
                    }
                    _ => {
                        return Err(CommandError::new(
                            9,
                            "FailedToParse",
                            "aggregation stage `$out` only supports string targets or `{ db, coll }` objects",
                        ));
                    }
                }
            }
            Ok(OutTarget {
                database,
                collection: collection.ok_or_else(|| {
                    CommandError::new(
                        9,
                        "FailedToParse",
                        "aggregation stage `$out` object targets require `coll`",
                    )
                })?,
            })
        }
        _ => Err(CommandError::new(
            9,
            "FailedToParse",
            "aggregation stage `$out` only supports string targets or `{ db, coll }` objects",
        )),
    }
}

fn parse_merge_target(value: &Bson) -> Result<MergeTarget, CommandError> {
    let invalid = || {
        CommandError::new(
            9,
            "FailedToParse",
            "aggregation stage `$merge` only supports a string target or an object with `into`, optional `on`, `whenMatched`, and `whenNotMatched`",
        )
    };

    match value {
        Bson::String(collection) => Ok(MergeTarget {
            database: None,
            collection: collection.clone(),
            on_fields: vec!["_id".to_string()],
            when_matched: MergeWhenMatched::Merge,
            when_not_matched: MergeWhenNotMatched::Insert,
        }),
        Bson::Document(document) => {
            let mut database = None;
            let mut collection = None;
            let mut on_fields = vec!["_id".to_string()];
            let mut when_matched = MergeWhenMatched::Merge;
            let mut when_not_matched = MergeWhenNotMatched::Insert;

            for (field, value) in document {
                match field.as_str() {
                    "into" => match value {
                        Bson::String(collection_name) => collection = Some(collection_name.clone()),
                        Bson::Document(namespace) => {
                            for (field, value) in namespace {
                                match field.as_str() {
                                    "db" => {
                                        database =
                                            Some(value.as_str().ok_or_else(invalid)?.to_string());
                                    }
                                    "coll" => {
                                        collection =
                                            Some(value.as_str().ok_or_else(invalid)?.to_string());
                                    }
                                    _ => return Err(invalid()),
                                }
                            }
                        }
                        _ => return Err(invalid()),
                    },
                    "on" => {
                        on_fields = match value {
                            Bson::String(field) => vec![field.clone()],
                            Bson::Array(fields) => fields
                                .iter()
                                .map(|field| field.as_str().map(str::to_string).ok_or_else(invalid))
                                .collect::<Result<Vec<_>, _>>()?,
                            _ => return Err(invalid()),
                        };
                        if on_fields.is_empty() {
                            return Err(invalid());
                        }
                    }
                    "whenMatched" => {
                        when_matched = match value {
                            Bson::String(mode) => match mode.as_str() {
                                "replace" => MergeWhenMatched::Replace,
                                "merge" => MergeWhenMatched::Merge,
                                "keepExisting" => MergeWhenMatched::KeepExisting,
                                "fail" => MergeWhenMatched::Fail,
                                _ => return Err(invalid()),
                            },
                            Bson::Array(_) => {
                                return Err(CommandError::new(
                                    115,
                                    "CommandNotSupported",
                                    "aggregation stage `$merge` does not yet support pipeline-style `whenMatched`",
                                ));
                            }
                            _ => return Err(invalid()),
                        };
                    }
                    "whenNotMatched" => {
                        when_not_matched = match value.as_str().ok_or_else(invalid)? {
                            "insert" => MergeWhenNotMatched::Insert,
                            "discard" => MergeWhenNotMatched::Discard,
                            "fail" => MergeWhenNotMatched::Fail,
                            _ => return Err(invalid()),
                        };
                    }
                    "let" => {
                        return Err(CommandError::new(
                            115,
                            "CommandNotSupported",
                            "aggregation stage `$merge` does not yet support `let` variables",
                        ));
                    }
                    "targetCollectionVersion" | "allowMergeOnNullishValues" => {
                        return Err(CommandError::new(
                            115,
                            "CommandNotSupported",
                            "aggregation stage `$merge` does not support router-only merge options",
                        ));
                    }
                    _ => return Err(invalid()),
                }
            }

            if !matches!(
                (when_matched, when_not_matched),
                (MergeWhenMatched::Replace, MergeWhenNotMatched::Insert)
                    | (MergeWhenMatched::Replace, MergeWhenNotMatched::Discard)
                    | (MergeWhenMatched::Replace, MergeWhenNotMatched::Fail)
                    | (MergeWhenMatched::Merge, MergeWhenNotMatched::Insert)
                    | (MergeWhenMatched::Merge, MergeWhenNotMatched::Discard)
                    | (MergeWhenMatched::Merge, MergeWhenNotMatched::Fail)
                    | (MergeWhenMatched::KeepExisting, MergeWhenNotMatched::Insert)
                    | (MergeWhenMatched::Fail, MergeWhenNotMatched::Insert)
            ) {
                return Err(CommandError::new(
                    51181,
                    "Location51181",
                    "the selected `whenMatched` and `whenNotMatched` mode combination is not supported",
                ));
            }

            Ok(MergeTarget {
                database,
                collection: collection.ok_or_else(invalid)?,
                on_fields,
                when_matched,
                when_not_matched,
            })
        }
        _ => Err(invalid()),
    }
}

fn merge_fields_match(left: &Document, right: &Document, on_fields: &[String]) -> bool {
    on_fields.iter().all(|field| {
        let left_value = lookup_path_owned(left, field).unwrap_or(Bson::Null);
        let right_value = lookup_path_owned(right, field).unwrap_or(Bson::Null);
        compare_bson(&left_value, &right_value).is_eq()
    })
}

struct BrokerCollectionResolver<'a> {
    catalog: &'a mqlite_catalog::Catalog,
}

impl CollectionResolver for BrokerCollectionResolver<'_> {
    fn resolve_collection(&self, database: &str, collection: &str) -> Vec<Document> {
        self.catalog
            .get_collection(database, collection)
            .map(|collection| collection.documents())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default)]
struct FieldBounds {
    eq: Option<Bson>,
    in_values: Option<Vec<Bson>>,
    lower: Option<(Bson, bool)>,
    upper: Option<(Bson, bool)>,
}

#[derive(Debug, Clone)]
struct IndexBoundsPlan {
    bounds: Vec<IndexBounds>,
    matched_fields: usize,
}

#[derive(Debug, Clone, Copy)]
struct SortPlan {
    direction: ScanDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone)]
struct FindExecution {
    documents: Vec<Document>,
    sort_covered: bool,
    projection_applied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PlanCacheKey {
    namespace: String,
    filter_shape: String,
    sort_shape: String,
    projection_shape: String,
}

#[derive(Debug, Clone)]
struct CachedPlan {
    sequence: u64,
    choice: PersistedPlanCacheChoice,
}

#[derive(Debug, Clone)]
struct CachedFindPlan {
    plan: PlannedFind,
    cache_used: bool,
}

#[derive(Debug, Clone)]
struct ProjectionRequirements {
    dependencies: BTreeSet<String>,
}

struct FindPlanContext<'a> {
    record_by_id: &'a BTreeMap<u64, &'a Document>,
    expression: &'a MatchExpr,
    filter_paths: &'a BTreeSet<String>,
    field_bounds: &'a BTreeMap<String, FieldBounds>,
    sort: Option<&'a Document>,
    projection: Option<&'a Document>,
    projection_requirements: Option<&'a ProjectionRequirements>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PlanCost {
    docs_examined: usize,
    requires_sort: bool,
    keys_examined: usize,
    projection_not_covered: bool,
    collection_scan: bool,
}

#[derive(Debug, Clone)]
enum PlannedFind {
    Collection {
        documents: Vec<Document>,
        record_ids: Vec<u64>,
        docs_examined: usize,
        sort_required: bool,
    },
    Index {
        index_name: String,
        bounds: Vec<IndexBounds>,
        documents: Vec<Document>,
        record_ids: Vec<u64>,
        matched_fields: usize,
        filter_covered: bool,
        sort_required: bool,
        sort_covered: bool,
        projection_covered: bool,
        projection_applied: bool,
        scan_direction: ScanDirection,
        keys_examined: usize,
        docs_examined: usize,
    },
    Or {
        branches: Vec<PlannedFind>,
        documents: Vec<Document>,
        record_ids: Vec<u64>,
        filter_covered: bool,
        sort_required: bool,
        sort_covered: bool,
        projection_covered: bool,
        projection_applied: bool,
        keys_examined: usize,
        docs_examined: usize,
    },
}

impl PlannedFind {
    fn to_document(&self) -> Document {
        match self {
            PlannedFind::Collection {
                docs_examined,
                sort_required,
                ..
            } => doc! {
                "stage": "COLLSCAN",
                "keysExamined": 0_i32,
                "docsExamined": *docs_examined as i32,
                "requiresSort": *sort_required,
                "filterCovered": false,
                "projectionCovered": false,
            },
            PlannedFind::Index {
                index_name, bounds, ..
            } => {
                let mut document = doc! {
                    "stage": "IXSCAN",
                    "indexName": index_name.clone(),
                };
                if bounds.len() == 1 {
                    if let Some(lower) = bounds[0].lower.as_ref() {
                        document.insert("lowerBound", Bson::Document(lower.key.clone()));
                        document.insert("lowerInclusive", lower.inclusive);
                    }
                    if let Some(upper) = bounds[0].upper.as_ref() {
                        document.insert("upperBound", Bson::Document(upper.key.clone()));
                        document.insert("upperInclusive", upper.inclusive);
                    }
                } else {
                    let intervals = bounds
                        .iter()
                        .map(|interval| {
                            let mut interval_document = Document::new();
                            if let Some(lower) = interval.lower.as_ref() {
                                interval_document
                                    .insert("lowerBound", Bson::Document(lower.key.clone()));
                                interval_document.insert("lowerInclusive", lower.inclusive);
                            }
                            if let Some(upper) = interval.upper.as_ref() {
                                interval_document
                                    .insert("upperBound", Bson::Document(upper.key.clone()));
                                interval_document.insert("upperInclusive", upper.inclusive);
                            }
                            Bson::Document(interval_document)
                        })
                        .collect::<Vec<_>>();
                    document.insert("intervalCount", bounds.len() as i32);
                    document.insert("intervals", Bson::Array(intervals));
                }
                if let PlannedFind::Index {
                    matched_fields,
                    filter_covered,
                    sort_required,
                    sort_covered,
                    projection_covered,
                    scan_direction,
                    keys_examined,
                    docs_examined,
                    ..
                } = self
                {
                    document.insert("matchedFields", *matched_fields as i32);
                    document.insert("filterCovered", *filter_covered);
                    document.insert("sortCovered", *sort_covered);
                    document.insert("projectionCovered", *projection_covered);
                    document.insert(
                        "scanDirection",
                        match scan_direction {
                            ScanDirection::Forward => 1,
                            ScanDirection::Backward => -1,
                        },
                    );
                    document.insert("keysExamined", *keys_examined as i32);
                    document.insert("docsExamined", *docs_examined as i32);
                    document.insert("requiresSort", *sort_required);
                }
                document
            }
            PlannedFind::Or {
                branches,
                filter_covered,
                sort_required,
                sort_covered,
                projection_covered,
                keys_examined,
                docs_examined,
                ..
            } => doc! {
                "stage": "OR",
                "inputStages": branches.iter().map(|branch| Bson::Document(branch.to_document())).collect::<Vec<_>>(),
                "keysExamined": *keys_examined as i32,
                "docsExamined": *docs_examined as i32,
                "requiresSort": *sort_required,
                "filterCovered": *filter_covered,
                "sortCovered": *sort_covered,
                "projectionCovered": *projection_covered,
            },
        }
    }

    fn cost(&self) -> PlanCost {
        match self {
            PlannedFind::Collection {
                docs_examined,
                sort_required,
                ..
            } => PlanCost {
                docs_examined: *docs_examined,
                requires_sort: *sort_required,
                keys_examined: 0,
                projection_not_covered: true,
                collection_scan: true,
            },
            PlannedFind::Index {
                sort_required,
                projection_covered,
                keys_examined,
                docs_examined,
                ..
            } => PlanCost {
                docs_examined: *docs_examined,
                requires_sort: *sort_required,
                keys_examined: *keys_examined,
                projection_not_covered: !*projection_covered,
                collection_scan: false,
            },
            PlannedFind::Or {
                sort_required,
                projection_covered,
                keys_examined,
                docs_examined,
                ..
            } => PlanCost {
                docs_examined: *docs_examined,
                requires_sort: *sort_required,
                keys_examined: *keys_examined,
                projection_not_covered: !*projection_covered,
                collection_scan: false,
            },
        }
    }

    fn into_execution(self) -> FindExecution {
        match self {
            PlannedFind::Collection { documents, .. } => FindExecution {
                documents,
                sort_covered: false,
                projection_applied: false,
            },
            PlannedFind::Index {
                documents,
                sort_covered,
                projection_applied,
                ..
            } => FindExecution {
                documents,
                sort_covered,
                projection_applied,
            },
            PlannedFind::Or {
                documents,
                sort_covered,
                projection_applied,
                ..
            } => FindExecution {
                documents,
                sort_covered,
                projection_applied,
            },
        }
    }

    fn record_ids(&self) -> &[u64] {
        match self {
            PlannedFind::Collection { record_ids, .. }
            | PlannedFind::Index { record_ids, .. }
            | PlannedFind::Or { record_ids, .. } => record_ids,
        }
    }

    fn filter_covered(&self) -> bool {
        match self {
            PlannedFind::Collection { .. } => false,
            PlannedFind::Index { filter_covered, .. } | PlannedFind::Or { filter_covered, .. } => {
                *filter_covered
            }
        }
    }

    fn keys_examined(&self) -> usize {
        match self {
            PlannedFind::Collection { .. } => 0,
            PlannedFind::Index { keys_examined, .. } | PlannedFind::Or { keys_examined, .. } => {
                *keys_examined
            }
        }
    }

    fn docs_examined(&self) -> usize {
        match self {
            PlannedFind::Collection { docs_examined, .. }
            | PlannedFind::Index { docs_examined, .. }
            | PlannedFind::Or { docs_examined, .. } => *docs_examined,
        }
    }

    fn branch_choices(&self) -> PersistedPlanCacheChoice {
        match self {
            PlannedFind::Collection { .. } => PersistedPlanCacheChoice::CollectionScan,
            PlannedFind::Index { index_name, .. } => {
                PersistedPlanCacheChoice::Index(index_name.clone())
            }
            PlannedFind::Or { branches, .. } => PersistedPlanCacheChoice::Union(
                branches
                    .iter()
                    .map(PlannedFind::branch_choices)
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

fn plan_find(
    collection: &CollectionCatalog,
    filter: &Document,
    sort: Option<&Document>,
    projection: Option<&Document>,
    preferred_choice: Option<&PersistedPlanCacheChoice>,
) -> Result<PlannedFind, CommandError> {
    let expression = parse_filter(filter)?;
    let simple_preferred_index = match preferred_choice {
        Some(PersistedPlanCacheChoice::CollectionScan) => None,
        Some(PersistedPlanCacheChoice::Index(name)) => Some(name.as_str()),
        Some(PersistedPlanCacheChoice::Union(_)) | None => None,
    };
    let simple_plan = plan_find_simple(
        collection,
        &expression,
        sort,
        projection,
        simple_preferred_index,
    )?;

    let Some(branches) = disjunctive_branches(&expression) else {
        return Ok(simple_plan);
    };
    if matches!(
        preferred_choice,
        Some(PersistedPlanCacheChoice::CollectionScan)
    ) {
        return Ok(simple_plan);
    }

    let preferred_branch_choices = match preferred_choice {
        Some(PersistedPlanCacheChoice::Union(choices)) if choices.len() == branches.len() => {
            Some(choices.as_slice())
        }
        _ => None,
    };
    let Some(or_plan) = plan_or_find(collection, &branches, sort, preferred_branch_choices)? else {
        return Ok(simple_plan);
    };
    Ok(if or_plan.cost() < simple_plan.cost() {
        or_plan
    } else {
        simple_plan
    })
}

fn plan_find_simple(
    collection: &CollectionCatalog,
    expression: &MatchExpr,
    sort: Option<&Document>,
    projection: Option<&Document>,
    preferred_index: Option<&str>,
) -> Result<PlannedFind, CommandError> {
    let field_bounds = extract_field_bounds(expression).unwrap_or_default();
    let filter_paths = collect_match_paths(expression);
    let projection_requirements = analyze_projection_requirements(projection)?;
    let record_by_id = collection
        .records
        .iter()
        .map(|record| (record.record_id, &record.document))
        .collect::<BTreeMap<_, _>>();
    let context = FindPlanContext {
        record_by_id: &record_by_id,
        expression,
        filter_paths: &filter_paths,
        field_bounds: &field_bounds,
        sort,
        projection,
        projection_requirements: projection_requirements.as_ref(),
    };

    let mut best_plan = plan_collection_scan(collection, expression, sort);
    if let Some(index_name) = preferred_index {
        if let Some(index) = collection.indexes.get(index_name) {
            if let Some(candidate) = evaluate_index_plan(&context, index)? {
                if candidate.cost() < best_plan.cost() {
                    best_plan = candidate;
                }
            }
        }
        return Ok(best_plan);
    }

    let mut estimated_candidates = collection
        .indexes
        .values()
        .filter_map(|index| {
            estimate_index_candidate(&context, index).map(|cost| (cost, index.name.clone()))
        })
        .collect::<Vec<_>>();
    estimated_candidates.sort_by(|(left_cost, left_name), (right_cost, right_name)| {
        left_cost
            .cmp(right_cost)
            .then_with(|| left_name.cmp(right_name))
    });

    for (_, index_name) in estimated_candidates
        .into_iter()
        .take(MAX_ESTIMATED_INDEX_CANDIDATES)
    {
        let Some(index) = collection.indexes.get(&index_name) else {
            continue;
        };
        if let Some(candidate) = evaluate_index_plan(&context, index)? {
            if candidate.cost() < best_plan.cost() {
                best_plan = candidate;
            }
        }
    }

    Ok(best_plan)
}

fn plan_collection_scan(
    collection: &CollectionCatalog,
    expression: &MatchExpr,
    sort: Option<&Document>,
) -> PlannedFind {
    let (record_ids, documents) = collection
        .records
        .iter()
        .filter(|record| document_matches_expression(&record.document, expression))
        .map(|record| (record.record_id, record.document.clone()))
        .unzip::<_, _, Vec<_>, Vec<_>>();
    PlannedFind::Collection {
        documents,
        record_ids,
        docs_examined: collection.records.len(),
        sort_required: sort.is_some(),
    }
}

fn plan_or_find(
    collection: &CollectionCatalog,
    branches: &[MatchExpr],
    sort: Option<&Document>,
    preferred_choices: Option<&[PersistedPlanCacheChoice]>,
) -> Result<Option<PlannedFind>, CommandError> {
    if branches.is_empty() || branches.len() > MAX_OR_BRANCHES {
        return Ok(None);
    }

    let record_by_id = collection
        .records
        .iter()
        .map(|record| (record.record_id, &record.document))
        .collect::<BTreeMap<_, _>>();
    let mut planned_branches = Vec::with_capacity(branches.len());
    for (index, branch) in branches.iter().enumerate() {
        let preferred_index = preferred_choices
            .and_then(|choices| choices.get(index))
            .and_then(|choice| match choice {
                PersistedPlanCacheChoice::Index(name) => Some(name.as_str()),
                _ => None,
            });
        planned_branches.push(plan_find_simple(
            collection,
            branch,
            None,
            None,
            preferred_index,
        )?);
    }

    let mut seen_record_ids = BTreeSet::new();
    let mut record_ids = Vec::new();
    for branch in &planned_branches {
        for record_id in branch.record_ids() {
            if seen_record_ids.insert(*record_id) {
                record_ids.push(*record_id);
            }
        }
    }

    let documents = record_ids
        .iter()
        .map(|record_id| fetch_record_document(&record_by_id, *record_id))
        .collect::<Result<Vec<_>, _>>()?;
    let filter_covered = planned_branches.iter().all(PlannedFind::filter_covered);
    let keys_examined = planned_branches
        .iter()
        .map(PlannedFind::keys_examined)
        .sum::<usize>();
    let docs_examined = planned_branches
        .iter()
        .map(PlannedFind::docs_examined)
        .sum::<usize>();

    Ok(Some(PlannedFind::Or {
        branches: planned_branches,
        documents,
        record_ids,
        filter_covered,
        sort_required: sort.is_some(),
        sort_covered: false,
        projection_covered: false,
        projection_applied: false,
        keys_examined,
        docs_examined,
    }))
}

fn evaluate_index_plan(
    context: &FindPlanContext<'_>,
    index: &IndexCatalog,
) -> Result<Option<PlannedFind>, CommandError> {
    let filter_plan = build_index_bounds(index, context.field_bounds);
    let sort_plan = analyze_sort(index, context.field_bounds, context.sort);
    let filter_supported =
        !context.filter_paths.is_empty() && index.covers_paths(context.filter_paths);
    let projection_supported = projection_supported(index, context, sort_plan);

    if filter_plan.is_none()
        && sort_plan.is_none()
        && !filter_supported
        && projection_supported.is_none()
    {
        return Ok(None);
    }

    let bounds = filter_plan
        .as_ref()
        .map(|plan| plan.bounds.clone())
        .unwrap_or_else(|| vec![full_range_bounds()]);
    let scan_direction = sort_plan
        .map(|plan| plan.direction)
        .unwrap_or(ScanDirection::Forward);
    let (entries, keys_examined) = scan_index_intervals(index, &bounds, scan_direction);

    let matched_fields = filter_plan
        .as_ref()
        .map(|plan| plan.matched_fields)
        .unwrap_or(0);
    let mut docs_examined = 0_usize;
    let mut documents = Vec::new();
    let mut record_ids = Vec::new();
    let mut filter_covered = filter_supported;
    let mut projection_covered = projection_supported.is_some();

    for entry in entries {
        let index_document = materialize_index_document(&entry)?;
        let mut fetched = None;
        let matches = if context.filter_paths.is_empty() {
            true
        } else if filter_supported {
            document_matches_expression(&index_document, context.expression)
        } else {
            filter_covered = false;
            let document = fetch_record_document(context.record_by_id, entry.record_id)?;
            docs_examined += 1;
            let is_match = document_matches_expression(&document, context.expression);
            fetched = Some(document);
            is_match
        };
        if !matches {
            continue;
        }

        let can_project_from_index = projection_supported.is_some();
        if projection_supported.is_some() && !can_project_from_index {
            projection_covered = false;
        }

        let document = if can_project_from_index {
            apply_projection(&index_document, context.projection)?
        } else {
            match fetched {
                Some(document) => document,
                None => {
                    docs_examined += 1;
                    fetch_record_document(context.record_by_id, entry.record_id)?
                }
            }
        };
        record_ids.push(entry.record_id);
        documents.push(document);
    }

    Ok(Some(PlannedFind::Index {
        index_name: index.name.clone(),
        bounds,
        documents,
        record_ids,
        matched_fields,
        filter_covered,
        sort_required: context.sort.is_some() && sort_plan.is_none(),
        sort_covered: sort_plan.is_some(),
        projection_covered,
        projection_applied: projection_covered,
        scan_direction,
        keys_examined,
        docs_examined,
    }))
}

fn estimate_index_candidate(
    context: &FindPlanContext<'_>,
    index: &IndexCatalog,
) -> Option<PlanCost> {
    let filter_plan = build_index_bounds(index, context.field_bounds);
    let sort_plan = analyze_sort(index, context.field_bounds, context.sort);
    let filter_supported =
        !context.filter_paths.is_empty() && index.covers_paths(context.filter_paths);
    let projection_supported = projection_supported(index, context, sort_plan);

    if filter_plan.is_none()
        && sort_plan.is_none()
        && !filter_supported
        && projection_supported.is_none()
    {
        return None;
    }

    let bounds = filter_plan
        .as_ref()
        .map(|plan| plan.bounds.as_slice())
        .unwrap_or(&[]);
    let estimated_keys_examined = if bounds.is_empty() {
        index.stats.entry_count
    } else {
        bounds
            .iter()
            .map(|interval| index.estimate_bounds_count(interval))
            .sum::<usize>()
    };
    let estimated_matches = estimate_filter_matches(
        index,
        context.field_bounds,
        context.filter_paths,
        filter_supported,
        estimated_keys_examined,
    );
    let estimated_docs_examined = if !context.filter_paths.is_empty() && !filter_supported {
        estimated_keys_examined
    } else if projection_supported.is_some() {
        0
    } else if context.filter_paths.is_empty() || filter_supported {
        estimated_matches
    } else {
        estimated_keys_examined
    };

    Some(PlanCost {
        docs_examined: estimated_docs_examined,
        requires_sort: context.sort.is_some() && sort_plan.is_none(),
        keys_examined: estimated_keys_examined,
        projection_not_covered: context.projection.is_some() && projection_supported.is_none(),
        collection_scan: false,
    })
}

fn estimate_filter_matches(
    index: &IndexCatalog,
    field_bounds: &BTreeMap<String, FieldBounds>,
    filter_paths: &BTreeSet<String>,
    filter_supported: bool,
    base_count: usize,
) -> usize {
    if !filter_supported || filter_paths.is_empty() {
        return base_count;
    }

    let mut estimates = vec![base_count.min(index.stats.entry_count)];
    for path in filter_paths {
        let Some(bounds) = field_bounds.get(path) else {
            continue;
        };
        let estimate = if let Some(value) = bounds.eq.as_ref() {
            index.estimate_value_count(path, value)
        } else if let Some(values) = bounds.in_values.as_ref() {
            index.estimate_values_count(path, values)
        } else if bounds.lower.is_some() || bounds.upper.is_some() {
            index.estimate_range_count(
                path,
                bounds
                    .lower
                    .as_ref()
                    .map(|(value, inclusive)| (value, *inclusive)),
                bounds
                    .upper
                    .as_ref()
                    .map(|(value, inclusive)| (value, *inclusive)),
            )
        } else {
            index.present_count(path)
        };
        if let Some(estimate) = estimate {
            estimates.push(estimate);
        }
    }

    estimates.into_iter().min().unwrap_or(base_count)
}

fn projection_supported<'a>(
    index: &IndexCatalog,
    context: &FindPlanContext<'a>,
    sort_plan: Option<SortPlan>,
) -> Option<&'a ProjectionRequirements> {
    context
        .projection_requirements
        .filter(|requirements| index.covers_paths(&requirements.dependencies))
        .filter(|_| context.sort.is_none() || sort_plan.is_some())
}

fn full_range_bounds() -> IndexBounds {
    IndexBounds {
        lower: None,
        upper: None,
    }
}

fn scan_index_intervals(
    index: &IndexCatalog,
    bounds: &[IndexBounds],
    scan_direction: ScanDirection,
) -> (Vec<IndexEntry>, usize) {
    let intervals = if bounds.is_empty() {
        vec![full_range_bounds()]
    } else {
        bounds.to_vec()
    };
    let mut keys_examined = 0_usize;
    let mut entry_by_record_id = BTreeMap::<u64, IndexEntry>::new();
    for interval in intervals {
        let entries = index.scan_entries(&interval);
        keys_examined += entries.len();
        for entry in entries {
            entry_by_record_id.entry(entry.record_id).or_insert(entry);
        }
    }

    let mut entries = entry_by_record_id.into_values().collect::<Vec<_>>();
    index.sort_entries(&mut entries);
    if scan_direction == ScanDirection::Backward {
        entries.reverse();
    }
    (entries, keys_examined)
}

fn extract_field_bounds(expression: &MatchExpr) -> Option<BTreeMap<String, FieldBounds>> {
    let mut field_bounds = BTreeMap::new();
    collect_field_bounds(expression, &mut field_bounds)?;
    (!field_bounds.is_empty()).then_some(field_bounds)
}

fn collect_field_bounds(
    expression: &MatchExpr,
    field_bounds: &mut BTreeMap<String, FieldBounds>,
) -> Option<()> {
    match expression {
        MatchExpr::AlwaysFalse | MatchExpr::AlwaysTrue | MatchExpr::SampleRate { .. } => Some(()),
        MatchExpr::And(items) => {
            for item in items {
                collect_field_bounds(item, field_bounds)?;
            }
            Some(())
        }
        MatchExpr::Not(_) => None,
        MatchExpr::Nor(_) => None,
        MatchExpr::Eq { path, value } => {
            field_bounds.entry(path.clone()).or_default().eq = Some(value.clone());
            Some(())
        }
        MatchExpr::In { path, values } => {
            merge_in_values(
                field_bounds.entry(path.clone()).or_default(),
                values.clone(),
            );
            Some(())
        }
        MatchExpr::Expr(_) => None,
        MatchExpr::Nin { .. } => None,
        MatchExpr::All { .. } => None,
        MatchExpr::Type { .. } => None,
        MatchExpr::ElemMatch { .. } => None,
        MatchExpr::Regex { .. } => None,
        MatchExpr::Size { .. } => None,
        MatchExpr::BitTest { .. } => Some(()),
        MatchExpr::Mod { .. } => None,
        MatchExpr::Gt { path, value } => {
            tighten_lower(
                field_bounds.entry(path.clone()).or_default(),
                value.clone(),
                false,
            );
            Some(())
        }
        MatchExpr::Gte { path, value } => {
            tighten_lower(
                field_bounds.entry(path.clone()).or_default(),
                value.clone(),
                true,
            );
            Some(())
        }
        MatchExpr::Lt { path, value } => {
            tighten_upper(
                field_bounds.entry(path.clone()).or_default(),
                value.clone(),
                false,
            );
            Some(())
        }
        MatchExpr::Lte { path, value } => {
            tighten_upper(
                field_bounds.entry(path.clone()).or_default(),
                value.clone(),
                true,
            );
            Some(())
        }
        MatchExpr::Or(items) => collect_or_field_bounds(items, field_bounds),
        MatchExpr::Ne { .. } | MatchExpr::Exists { .. } => None,
    }
}

fn collect_or_field_bounds(
    items: &[MatchExpr],
    field_bounds: &mut BTreeMap<String, FieldBounds>,
) -> Option<()> {
    let branches = items.iter().map(point_map).collect::<Option<Vec<_>>>()?;
    let first_branch = branches.first()?;
    let branch_fields = first_branch.keys().cloned().collect::<Vec<_>>();
    if branch_fields.is_empty()
        || branches
            .iter()
            .any(|branch| branch.keys().cloned().collect::<Vec<_>>() != branch_fields)
    {
        return None;
    }

    let varying_fields = branch_fields
        .iter()
        .filter(|field| {
            let first_value = first_branch.get(*field).expect("first branch field");
            branches.iter().skip(1).any(|branch| {
                let value = branch.get(*field).expect("branch field");
                !compare_bson(value, first_value).is_eq()
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if varying_fields.len() > 1 {
        return None;
    }

    for field in branch_fields {
        if varying_fields
            .first()
            .is_some_and(|varying| varying == &field)
        {
            let values = branches
                .iter()
                .map(|branch| branch.get(&field).expect("branch field").clone())
                .collect::<Vec<_>>();
            merge_in_values(field_bounds.entry(field).or_default(), values);
        } else {
            field_bounds.entry(field.clone()).or_default().eq = Some(
                first_branch
                    .get(&field)
                    .expect("first branch field")
                    .clone(),
            );
        }
    }
    Some(())
}

fn point_map(expression: &MatchExpr) -> Option<BTreeMap<String, Bson>> {
    match expression {
        MatchExpr::Eq { path, value } => Some(BTreeMap::from([(path.clone(), value.clone())])),
        MatchExpr::And(items) => {
            let mut points = BTreeMap::new();
            for item in items {
                for (path, value) in point_map(item)? {
                    if let Some(existing) = points.insert(path.clone(), value.clone()) {
                        if !compare_bson(&existing, &value).is_eq() {
                            return None;
                        }
                    }
                }
            }
            Some(points)
        }
        MatchExpr::Not(_) => None,
        MatchExpr::Nor(_) => None,
        MatchExpr::Gt { path, value: _ }
        | MatchExpr::Gte { path, value: _ }
        | MatchExpr::Lt { path, value: _ }
        | MatchExpr::Lte { path, value: _ } => {
            let mut bounds = BTreeMap::new();
            collect_field_bounds(expression, &mut bounds)?;
            if bounds.len() != 1 {
                return None;
            }
            point_value(bounds.get(path)?).map(|point| BTreeMap::from([(path.clone(), point)]))
        }
        _ => None,
    }
}

fn merge_in_values(bounds: &mut FieldBounds, mut values: Vec<Bson>) {
    values.sort_by(compare_bson);
    values.dedup_by(|left, right| compare_bson(left, right).is_eq());
    match (&bounds.eq, &bounds.in_values) {
        (Some(eq), _) => {
            if values.iter().any(|value| compare_bson(value, eq).is_eq()) {
                bounds.eq = Some(eq.clone());
            }
        }
        (None, Some(existing)) => {
            bounds.in_values = Some(
                existing
                    .iter()
                    .filter(|candidate| {
                        values
                            .iter()
                            .any(|value| compare_bson(candidate, value).is_eq())
                    })
                    .cloned()
                    .collect(),
            );
        }
        (None, None) => bounds.in_values = Some(values),
    }
}

fn collect_match_paths(expression: &MatchExpr) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    collect_match_paths_into(expression, &mut paths);
    paths
}

fn collect_match_paths_into(expression: &MatchExpr, paths: &mut BTreeSet<String>) {
    match expression {
        MatchExpr::AlwaysFalse | MatchExpr::AlwaysTrue | MatchExpr::SampleRate { .. } => {}
        MatchExpr::And(items) | MatchExpr::Or(items) | MatchExpr::Nor(items) => {
            for item in items {
                collect_match_paths_into(item, paths);
            }
        }
        MatchExpr::Not(expression) => collect_match_paths_into(expression, paths),
        MatchExpr::Expr(_) => {}
        MatchExpr::Eq { path, .. }
        | MatchExpr::Ne { path, .. }
        | MatchExpr::Gt { path, .. }
        | MatchExpr::Gte { path, .. }
        | MatchExpr::Lt { path, .. }
        | MatchExpr::Lte { path, .. }
        | MatchExpr::In { path, .. }
        | MatchExpr::Nin { path, .. }
        | MatchExpr::All { path, .. }
        | MatchExpr::Exists { path, .. }
        | MatchExpr::Type { path, .. }
        | MatchExpr::ElemMatch { path, .. }
        | MatchExpr::Regex { path, .. }
        | MatchExpr::Size { path, .. }
        | MatchExpr::BitTest { path, .. }
        | MatchExpr::Mod { path, .. } => {
            paths.insert(path.clone());
        }
    }
}

fn analyze_projection_requirements(
    projection: Option<&Document>,
) -> Result<Option<ProjectionRequirements>, QueryError> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    if !projection_include_mode(projection)? {
        return Ok(None);
    }

    let mut dependencies = BTreeSet::new();
    let include_id = projection
        .get("_id")
        .and_then(projection_flag)
        .unwrap_or(true);
    if include_id {
        dependencies.insert("_id".to_string());
    }

    for (field, value) in projection {
        if field == "_id" {
            continue;
        }
        match projection_flag(value) {
            Some(true) => {
                dependencies.insert(field.clone());
            }
            Some(false) => {}
            None => collect_expression_dependencies(value, &mut dependencies),
        }
    }

    Ok(Some(ProjectionRequirements { dependencies }))
}

fn projection_include_mode(projection: &Document) -> Result<bool, QueryError> {
    let mut include_mode = None;
    for (field, value) in projection {
        if field == "_id" {
            continue;
        }
        if let Some(flag) = projection_flag(value) {
            include_mode = match include_mode {
                None => Some(flag),
                Some(existing) if existing == flag => Some(existing),
                Some(_) => return Err(QueryError::MixedProjection),
            };
        } else {
            include_mode = Some(true);
        }
    }

    Ok(include_mode.unwrap_or_else(|| {
        projection
            .get("_id")
            .and_then(projection_flag)
            .unwrap_or(true)
    }))
}

fn projection_flag(value: &Bson) -> Option<bool> {
    match value {
        Bson::Boolean(value) => Some(*value),
        Bson::Int32(value) => Some(*value != 0),
        Bson::Int64(value) => Some(*value != 0),
        _ => None,
    }
}

fn collect_expression_dependencies(expression: &Bson, dependencies: &mut BTreeSet<String>) {
    match expression {
        Bson::String(path) if path.starts_with('$') => {
            dependencies.insert(path[1..].to_string());
        }
        Bson::Document(spec) if spec.len() == 1 && spec.contains_key("$literal") => {}
        Bson::Document(spec) => {
            for value in spec.values() {
                collect_expression_dependencies(value, dependencies);
            }
        }
        Bson::Array(items) => {
            for item in items {
                collect_expression_dependencies(item, dependencies);
            }
        }
        _ => {}
    }
}

fn materialize_index_document(entry: &IndexEntry) -> Result<Document, CommandError> {
    let mut document = Document::new();
    let present_fields = entry.present_fields.iter().collect::<BTreeSet<_>>();
    for (field, value) in &entry.key {
        if !present_fields.contains(&field) {
            continue;
        }
        set_path(&mut document, field, value.clone()).map_err(|_| {
            CommandError::new(2, "BadValue", format!("invalid index key path `{field}`"))
        })?;
    }
    Ok(document)
}

fn fetch_record_document(
    record_by_id: &BTreeMap<u64, &Document>,
    record_id: u64,
) -> Result<Document, CommandError> {
    record_by_id
        .get(&record_id)
        .map(|document| (*document).clone())
        .ok_or_else(|| CommandError::new(8, "UnknownError", "missing record for index entry"))
}

fn build_plan_cache_key(
    namespace: &str,
    filter: &Document,
    sort: Option<&Document>,
    projection: Option<&Document>,
) -> Result<PlanCacheKey, CommandError> {
    Ok(PlanCacheKey {
        namespace: namespace.to_string(),
        filter_shape: filter_shape(&parse_filter(filter)?),
        sort_shape: sort_shape(sort),
        projection_shape: projection_shape(projection),
    })
}

fn plan_cache_key_from_entry(entry: &PersistedPlanCacheEntry) -> PlanCacheKey {
    PlanCacheKey {
        namespace: entry.namespace.clone(),
        filter_shape: entry.filter_shape.clone(),
        sort_shape: entry.sort_shape.clone(),
        projection_shape: entry.projection_shape.clone(),
    }
}

fn cached_plan_from_entry(entry: &PersistedPlanCacheEntry) -> CachedPlan {
    CachedPlan {
        sequence: entry.sequence,
        choice: entry.choice.clone(),
    }
}

fn planned_choice(plan: &PlannedFind) -> PersistedPlanCacheChoice {
    plan.branch_choices()
}

fn filter_shape(expression: &MatchExpr) -> String {
    match expression {
        MatchExpr::AlwaysFalse => "alwaysFalse".to_string(),
        MatchExpr::AlwaysTrue => "alwaysTrue".to_string(),
        MatchExpr::SampleRate { .. } => "sampleRate".to_string(),
        MatchExpr::And(items) => format!(
            "and({})",
            items.iter().map(filter_shape).collect::<Vec<_>>().join(",")
        ),
        MatchExpr::Or(items) => format!(
            "or({})",
            items.iter().map(filter_shape).collect::<Vec<_>>().join(",")
        ),
        MatchExpr::Nor(items) => format!(
            "nor({})",
            items.iter().map(filter_shape).collect::<Vec<_>>().join(",")
        ),
        MatchExpr::Not(expression) => format!("not({})", filter_shape(expression)),
        MatchExpr::Expr(_) => "expr".to_string(),
        MatchExpr::Eq { path, .. } => format!("{path}:eq"),
        MatchExpr::Ne { path, .. } => format!("{path}:ne"),
        MatchExpr::Gt { path, .. } => format!("{path}:gt"),
        MatchExpr::Gte { path, .. } => format!("{path}:gte"),
        MatchExpr::Lt { path, .. } => format!("{path}:lt"),
        MatchExpr::Lte { path, .. } => format!("{path}:lte"),
        MatchExpr::In { path, values } => format!("{path}:in{}", values.len()),
        MatchExpr::Nin { path, values } => format!("{path}:nin{}", values.len()),
        MatchExpr::All { path, values } => format!("{path}:all{}", values.len()),
        MatchExpr::Exists { path, exists } => format!("{path}:exists{exists}"),
        MatchExpr::Type { path, type_set } => {
            format!(
                "{path}:type{}:{}",
                i32::from(type_set.all_numbers),
                type_set.codes.len()
            )
        }
        MatchExpr::ElemMatch {
            path,
            value_case,
            spec,
        } => format!("{path}:elem{}:{}", i32::from(*value_case), spec.len()),
        MatchExpr::Regex { path, options, .. } => format!("{path}:regex{options}"),
        MatchExpr::Size { path, size } => format!("{path}:size{size}"),
        MatchExpr::BitTest {
            path,
            mode,
            positions,
        } => format!("{path}:bit{:?}{}", mode, positions.len()),
        MatchExpr::Mod {
            path,
            divisor,
            remainder,
        } => format!("{path}:mod{divisor}:{remainder}"),
    }
}

fn sort_shape(sort: Option<&Document>) -> String {
    sort.map_or_else(
        || "-".to_string(),
        |sort| {
            sort.iter()
                .map(|(field, direction)| format!("{field}:{}", direction.as_i64().unwrap_or(1)))
                .collect::<Vec<_>>()
                .join(",")
        },
    )
}

fn projection_shape(projection: Option<&Document>) -> String {
    projection.map_or_else(
        || "-".to_string(),
        |projection| {
            projection
                .iter()
                .map(|(field, value)| match projection_flag(value) {
                    Some(flag) => format!("{field}:{}", i32::from(flag)),
                    None => format!("{field}:expr"),
                })
                .collect::<Vec<_>>()
                .join(",")
        },
    )
}

fn disjunctive_branches(expression: &MatchExpr) -> Option<Vec<MatchExpr>> {
    let branches = dnf_branches(expression)?;
    (branches.len() > 1).then(|| {
        branches
            .into_iter()
            .map(|terms| match terms.len() {
                0 => MatchExpr::And(Vec::new()),
                1 => terms.into_iter().next().expect("single term"),
                _ => MatchExpr::And(terms),
            })
            .collect()
    })
}

fn dnf_branches(expression: &MatchExpr) -> Option<Vec<Vec<MatchExpr>>> {
    match expression {
        MatchExpr::And(items) => {
            let mut branches = vec![Vec::new()];
            for item in items {
                let item_branches = dnf_branches(item)?;
                let mut next = Vec::new();
                for branch in &branches {
                    for item_branch in &item_branches {
                        let mut combined = branch.clone();
                        combined.extend(item_branch.clone());
                        next.push(combined);
                        if next.len() > MAX_OR_BRANCHES {
                            return None;
                        }
                    }
                }
                branches = next;
            }
            Some(branches)
        }
        MatchExpr::Or(items) => {
            let mut branches = Vec::new();
            for item in items {
                let item_branches = dnf_branches(item)?;
                branches.extend(item_branches);
                if branches.len() > MAX_OR_BRANCHES {
                    return None;
                }
            }
            Some(branches)
        }
        MatchExpr::Not(_) => None,
        MatchExpr::Nor(_) => None,
        other => Some(vec![vec![other.clone()]]),
    }
}

fn build_index_bounds(
    index: &IndexCatalog,
    field_bounds: &BTreeMap<String, FieldBounds>,
) -> Option<IndexBoundsPlan> {
    let key_fields = index
        .key
        .iter()
        .map(|(field, direction)| (field.clone(), direction_sign(direction).unwrap_or(1)))
        .collect::<Vec<_>>();
    let mut bounds = Vec::new();
    let mut equality_prefix = Vec::<(String, Bson)>::new();
    let matched_fields = collect_index_bounds(
        &key_fields,
        field_bounds,
        0,
        &mut equality_prefix,
        &mut bounds,
    )?;
    (!bounds.is_empty()).then_some(IndexBoundsPlan {
        bounds,
        matched_fields,
    })
}

fn collect_index_bounds(
    key_fields: &[(String, i32)],
    field_bounds: &BTreeMap<String, FieldBounds>,
    position: usize,
    equality_prefix: &mut Vec<(String, Bson)>,
    intervals: &mut Vec<IndexBounds>,
) -> Option<usize> {
    if intervals.len() > MAX_MULTI_INTERVALS {
        return None;
    }

    let Some((field, direction)) = key_fields.get(position) else {
        if equality_prefix.is_empty() {
            return None;
        }
        push_prefix_interval(key_fields, equality_prefix, intervals);
        return Some(equality_prefix.len());
    };

    let Some(bounds) = field_bounds.get(field) else {
        if equality_prefix.is_empty() {
            return None;
        }
        push_prefix_interval(key_fields, equality_prefix, intervals);
        return Some(equality_prefix.len());
    };

    if let Some(value) = point_value(bounds) {
        equality_prefix.push((field.clone(), value));
        let matched_fields = collect_index_bounds(
            key_fields,
            field_bounds,
            position + 1,
            equality_prefix,
            intervals,
        );
        equality_prefix.pop();
        return matched_fields;
    }

    if let Some(values) = point_values(bounds).filter(|values| values.len() > 1) {
        let mut matched_fields = equality_prefix.len() + 1;
        for value in values {
            equality_prefix.push((field.clone(), value));
            let branch_matched = collect_index_bounds(
                key_fields,
                field_bounds,
                position + 1,
                equality_prefix,
                intervals,
            )?;
            matched_fields = matched_fields.max(branch_matched);
            equality_prefix.pop();
            if intervals.len() > MAX_MULTI_INTERVALS {
                return None;
            }
        }
        return Some(matched_fields);
    }

    if bounds.lower.is_some() || bounds.upper.is_some() {
        let (lower_value, lower_inclusive) = index_order_lower_bound(*direction, bounds);
        let (upper_value, upper_inclusive) = index_order_upper_bound(*direction, bounds);
        intervals.push(IndexBounds {
            lower: Some(IndexBound {
                key: build_compound_bound_key(
                    key_fields,
                    equality_prefix,
                    field,
                    lower_value,
                    true,
                ),
                inclusive: lower_inclusive,
            }),
            upper: Some(IndexBound {
                key: build_compound_bound_key(
                    key_fields,
                    equality_prefix,
                    field,
                    upper_value,
                    false,
                ),
                inclusive: upper_inclusive,
            }),
        });
        return Some(equality_prefix.len() + 1);
    }

    if equality_prefix.is_empty() {
        None
    } else {
        push_prefix_interval(key_fields, equality_prefix, intervals);
        Some(equality_prefix.len())
    }
}

fn push_prefix_interval(
    key_fields: &[(String, i32)],
    equality_prefix: &[(String, Bson)],
    intervals: &mut Vec<IndexBounds>,
) {
    intervals.push(IndexBounds {
        lower: Some(IndexBound {
            key: build_prefix_bound_key(key_fields, equality_prefix, true),
            inclusive: true,
        }),
        upper: Some(IndexBound {
            key: build_prefix_bound_key(key_fields, equality_prefix, false),
            inclusive: true,
        }),
    });
}

fn analyze_sort(
    index: &IndexCatalog,
    field_bounds: &BTreeMap<String, FieldBounds>,
    sort: Option<&Document>,
) -> Option<SortPlan> {
    let sort = sort.filter(|sort| !sort.is_empty())?;
    let effective_sort = sort
        .iter()
        .filter(|(field, _)| field_bounds.get(*field).and_then(point_value).is_none())
        .collect::<Vec<_>>();
    if effective_sort.is_empty() {
        return Some(SortPlan {
            direction: ScanDirection::Forward,
        });
    }

    let index_fields = index
        .key
        .iter()
        .map(|(field, direction)| direction_sign(direction).map(|sign| (field.clone(), sign)))
        .collect::<Option<Vec<_>>>()?;
    let start = index_fields
        .iter()
        .take_while(|(field, _)| field_bounds.get(field).and_then(point_value).is_some())
        .count();

    if index_fields.len() < start + effective_sort.len() {
        return None;
    }

    let mut direct = true;
    let mut reverse = true;
    for ((sort_field, sort_direction), (index_field, index_direction)) in
        effective_sort.iter().zip(index_fields[start..].iter())
    {
        if *sort_field != index_field {
            return None;
        }
        let sort_direction = direction_sign(sort_direction)?;
        direct &= sort_direction == *index_direction;
        reverse &= sort_direction == -*index_direction;
    }

    if direct {
        return Some(SortPlan {
            direction: ScanDirection::Forward,
        });
    }
    if reverse {
        return Some(SortPlan {
            direction: ScanDirection::Backward,
        });
    }
    None
}

fn build_prefix_bound_key(
    key_fields: &[(String, i32)],
    equality_prefix: &[(String, Bson)],
    is_lower_bound: bool,
) -> Document {
    let mut key = Document::new();
    for (position, (field, direction)) in key_fields.iter().enumerate() {
        if let Some((_, value)) = equality_prefix.get(position) {
            key.insert(field, value.clone());
        } else {
            key.insert(field, trailing_fill_value(*direction, is_lower_bound));
        }
    }
    key
}

fn build_compound_bound_key(
    key_fields: &[(String, i32)],
    equality_prefix: &[(String, Bson)],
    range_field: &str,
    range_value: Bson,
    is_lower_bound: bool,
) -> Document {
    let mut key = Document::new();
    for (position, (field, direction)) in key_fields.iter().enumerate() {
        if let Some((_, value)) = equality_prefix.get(position) {
            key.insert(field, value.clone());
        } else if field == range_field {
            key.insert(field, range_value.clone());
        } else {
            key.insert(field, trailing_fill_value(*direction, is_lower_bound));
        }
    }
    key
}

fn index_order_lower_bound(direction: i32, bounds: &FieldBounds) -> (Bson, bool) {
    if direction < 0 {
        bounds
            .upper
            .as_ref()
            .map(|(value, inclusive)| (value.clone(), *inclusive))
            .unwrap_or((Bson::MaxKey, true))
    } else {
        bounds
            .lower
            .as_ref()
            .map(|(value, inclusive)| (value.clone(), *inclusive))
            .unwrap_or((Bson::MinKey, true))
    }
}

fn index_order_upper_bound(direction: i32, bounds: &FieldBounds) -> (Bson, bool) {
    if direction < 0 {
        bounds
            .lower
            .as_ref()
            .map(|(value, inclusive)| (value.clone(), *inclusive))
            .unwrap_or((Bson::MinKey, true))
    } else {
        bounds
            .upper
            .as_ref()
            .map(|(value, inclusive)| (value.clone(), *inclusive))
            .unwrap_or((Bson::MaxKey, true))
    }
}

fn trailing_fill_value(direction: i32, is_lower_bound: bool) -> Bson {
    match (direction < 0, is_lower_bound) {
        (false, true) => Bson::MinKey,
        (false, false) => Bson::MaxKey,
        (true, true) => Bson::MaxKey,
        (true, false) => Bson::MinKey,
    }
}

fn point_value(bounds: &FieldBounds) -> Option<Bson> {
    if let Some(value) = bounds.eq.as_ref() {
        return Some(value.clone());
    }
    if let Some(values) = bounds.in_values.as_ref().filter(|values| values.len() == 1) {
        return values.first().cloned();
    }

    match (&bounds.lower, &bounds.upper) {
        (Some((lower, true)), Some((upper, true))) if compare_bson(lower, upper).is_eq() => {
            Some(lower.clone())
        }
        _ => None,
    }
}

fn point_values(bounds: &FieldBounds) -> Option<Vec<Bson>> {
    bounds
        .in_values
        .clone()
        .or_else(|| point_value(bounds).map(|value| vec![value]))
}

fn direction_sign(value: &Bson) -> Option<i32> {
    let direction = match value {
        Bson::Int32(value) => i64::from(*value),
        Bson::Int64(value) => *value,
        Bson::Double(value) if value.fract() == 0.0 => *value as i64,
        _ => return None,
    };
    Some(if direction < 0 { -1 } else { 1 })
}

fn tighten_lower(bounds: &mut FieldBounds, candidate: Bson, inclusive: bool) {
    match bounds.lower.as_ref() {
        Some((current, current_inclusive)) => {
            let ordering = compare_bson(&candidate, current);
            if ordering.is_gt() || (ordering.is_eq() && !inclusive && *current_inclusive) {
                bounds.lower = Some((candidate, inclusive));
            }
        }
        None => bounds.lower = Some((candidate, inclusive)),
    }
}

fn tighten_upper(bounds: &mut FieldBounds, candidate: Bson, inclusive: bool) {
    match bounds.upper.as_ref() {
        Some((current, current_inclusive)) => {
            let ordering = compare_bson(&candidate, current);
            if ordering.is_lt() || (ordering.is_eq() && !inclusive && *current_inclusive) {
                bounds.upper = Some((candidate, inclusive));
            }
        }
        None => bounds.upper = Some((candidate, inclusive)),
    }
}

fn command_name(body: &Document) -> Option<String> {
    body.keys().find(|key| !key.starts_with('$')).cloned()
}

fn database_name(body: &Document) -> Result<String, CommandError> {
    body.get_str("$db")
        .map(str::to_string)
        .map_err(|_| CommandError::new(9, "FailedToParse", "command is missing `$db`"))
}

fn parse_namespace(namespace: &str) -> Result<(&str, &str), CommandError> {
    let (database, collection) = namespace.split_once('.').ok_or_else(|| {
        CommandError::new(
            9,
            "FailedToParse",
            "namespace must be a `database.collection` string",
        )
    })?;
    if database.is_empty() || collection.is_empty() {
        return Err(CommandError::new(
            9,
            "FailedToParse",
            "namespace must be a `database.collection` string",
        ));
    }
    Ok((database, collection))
}

fn body_batch_size(body: &Document, field: &str) -> Option<i64> {
    body.get_document(field)
        .ok()
        .and_then(|cursor| cursor.get_i64("batchSize").ok())
        .or_else(|| body.get_i64("batchSize").ok())
}

fn truthy_parameter_selector(value: &Bson) -> bool {
    match value {
        Bson::Boolean(value) => *value,
        Bson::Int32(value) => *value != 0,
        Bson::Int64(value) => *value != 0,
        Bson::Double(value) => *value != 0.0,
        _ => false,
    }
}

fn cursor_document(batch: mqlite_exec::CursorBatch, batch_field: &str) -> Document {
    let mut cursor = Document::new();
    cursor.insert("id", batch.cursor_id);
    cursor.insert("ns", batch.namespace);
    cursor.insert(
        batch_field,
        Bson::Array(batch.documents.into_iter().map(Bson::Document).collect()),
    );

    doc! { "cursor": cursor }
}

fn index_to_document(index: IndexCatalog) -> Document {
    let mut document = Document::new();
    document.insert("v", 2);
    document.insert("key", index.key);
    document.insert("name", index.name);
    if index.unique {
        document.insert("unique", true);
    }
    document
}

fn reject_unsupported_envelope(body: &Document) -> Result<(), CommandError> {
    const UNSUPPORTED_KEYS: [(&str, &str); 6] = [
        ("lsid", "logical sessions are not supported"),
        ("txnNumber", "transactions are not supported"),
        ("startTransaction", "transactions are not supported"),
        ("autocommit", "transactions are not supported"),
        ("readConcern", "read concern is not supported"),
        ("$readPreference", "read preference is not supported"),
    ];

    for (key, message) in UNSUPPORTED_KEYS {
        if body.contains_key(key) {
            return Err(CommandError::new(115, "CommandNotSupported", message));
        }
    }

    if let Ok(write_concern) = body.get_document("writeConcern") {
        reject_unsupported_write_concern(write_concern)?;
    }

    Ok(())
}

fn reject_unsupported_write_concern(write_concern: &Document) -> Result<(), CommandError> {
    for (key, value) in write_concern {
        let supported = match key.as_str() {
            "w" => default_write_concern_w(value),
            "j" | "journal" | "fsync" => falseish_write_concern_flag(value),
            "wtimeout" | "wtimeoutMS" => zero_write_concern_timeout(value),
            _ => false,
        };

        if !supported {
            return Err(CommandError::new(
                115,
                "CommandNotSupported",
                "non-default write concern is not supported",
            ));
        }
    }

    Ok(())
}

fn default_write_concern_w(value: &Bson) -> bool {
    match value {
        Bson::Int32(value) => *value == 1,
        Bson::Int64(value) => *value == 1,
        Bson::Double(value) => *value == 1.0,
        _ => false,
    }
}

fn falseish_write_concern_flag(value: &Bson) -> bool {
    match value {
        Bson::Boolean(value) => !value,
        Bson::Int32(value) => *value == 0,
        Bson::Int64(value) => *value == 0,
        Bson::Double(value) => *value == 0.0,
        _ => false,
    }
}

fn zero_write_concern_timeout(value: &Bson) -> bool {
    match value {
        Bson::Int32(value) => *value == 0,
        Bson::Int64(value) => *value == 0,
        Bson::Double(value) => *value == 0.0,
        _ => false,
    }
}

fn sort_documents(documents: &mut [Document], sort: &Document) {
    documents.sort_by(|left, right| {
        for (field, direction) in sort {
            let left_value = lookup_path_owned(left, field).unwrap_or(Bson::Null);
            let right_value = lookup_path_owned(right, field).unwrap_or(Bson::Null);
            let mut ordering = mqlite_bson::compare_bson(&left_value, &right_value);
            if direction.as_i64().unwrap_or(1) < 0 {
                ordering = ordering.reverse();
            }
            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn ok_response(mut body: Document) -> Document {
    body.insert("ok", 1.0);
    body
}

fn internal_error(error: anyhow::Error) -> CommandError {
    CommandError::new(8, "UnknownError", error.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{
        path::Path,
        time::{Duration, Instant},
    };

    use bson::{Bson, doc, oid::ObjectId};
    use mqlite_ipc::{connect, read_manifest};
    use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
    use tempfile::tempdir;
    use tokio::task::JoinHandle;

    use super::{Broker, BrokerConfig};

    async fn send_command(
        stream: &mut mqlite_ipc::BoxedStream,
        body: bson::Document,
    ) -> bson::Document {
        let message = OpMsg::new(1, 0, vec![PayloadSection::Body(body)]);
        write_op_msg(stream, &message).await.expect("write op msg");
        let reply = read_op_msg(stream).await.expect("read op msg");
        reply.body().cloned().expect("reply body")
    }

    async fn wait_for_manifest(manifest_path: &Path, serve_task: &JoinHandle<anyhow::Result<()>>) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manifest_path.exists() {
                return;
            }
            assert!(
                !serve_task.is_finished(),
                "broker exited before writing its manifest"
            );
            assert!(
                Instant::now() < deadline,
                "timed out waiting for broker manifest"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn start_broker_at(
        database_path: &Path,
    ) -> (JoinHandle<anyhow::Result<()>>, mqlite_ipc::BrokerManifest) {
        let broker = Broker::new(BrokerConfig::new(database_path, 1)).expect("broker");
        let manifest_path = broker.paths().manifest_path.clone();
        let serve_task = tokio::spawn(broker.clone().serve());
        wait_for_manifest(&manifest_path, &serve_task).await;
        let manifest = read_manifest(&manifest_path).expect("manifest");
        (serve_task, manifest)
    }

    async fn start_broker(
        database_name: &str,
    ) -> (
        JoinHandle<anyhow::Result<()>>,
        tempfile::TempDir,
        mqlite_ipc::BrokerManifest,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let database_path = temp_dir.path().join(database_name);
        let (serve_task, manifest) = start_broker_at(&database_path).await;
        (serve_task, temp_dir, manifest)
    }

    async fn assert_rejected(body: bson::Document, expected_code: i32) {
        let (serve_task, _temp_dir, manifest) = start_broker("unsupported.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");
        let response = send_command(&mut stream, body).await;
        assert_eq!(response.get_f64("ok").expect("ok"), 0.0);
        assert_eq!(response.get_i32("code").expect("code"), expected_code);
        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serves_hello_and_crud_over_local_ipc() {
        let (serve_task, _temp_dir, manifest) = start_broker("app.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let hello = send_command(&mut stream, doc! { "hello": 1, "$db": "admin" }).await;
        assert!(hello.get_bool("helloOk").expect("helloOk"));
        assert!(hello.get("logicalSessionTimeoutMinutes").is_none());

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "sku": "a", "qty": 1 },
                    { "sku": "b", "qty": 3 }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": { "qty": { "$gte": 2 } },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        assert_eq!(first_batch.len(), 1);
        assert_eq!(
            first_batch[0]
                .as_document()
                .expect("document")
                .get_str("sku")
                .expect("sku"),
            "b"
        );

        let aggregate = send_command(
            &mut stream,
            doc! {
                "aggregate": "widgets",
                "pipeline": [
                    { "$group": { "_id": Bson::Null, "total": { "$sum": "$qty" } } }
                ],
                "cursor": { "batchSize": 10 },
                "$db": "app"
            },
        )
        .await;
        let aggregate_batch = aggregate
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        assert_eq!(
            aggregate_batch[0]
                .as_document()
                .expect("document")
                .get("total"),
            Some(&Bson::Int64(4))
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn collectionless_current_op_aggregate_reports_the_inflight_command() {
        let (serve_task, _temp_dir, manifest) = start_broker("current-op.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let aggregate = send_command(
            &mut stream,
            doc! {
                "aggregate": 1,
                "pipeline": [
                    { "$currentOp": { "localOps": true } },
                    { "$project": { "_id": 0, "ns": 1, "type": 1, "command": 1 } }
                ],
                "cursor": {},
                "$db": "admin"
            },
        )
        .await;
        let aggregate_batch = aggregate
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        assert_eq!(aggregate_batch.len(), 1);
        let operation = aggregate_batch[0].as_document().expect("operation");
        assert_eq!(operation.get_str("ns").expect("ns"), "admin.$cmd.aggregate");
        assert_eq!(operation.get_str("type").expect("type"), "op");
        let command = operation.get_document("command").expect("command");
        assert_eq!(command.get_i32("aggregate").expect("aggregate"), 1);
        assert_eq!(
            command
                .get_array("pipeline")
                .expect("pipeline")
                .first()
                .and_then(Bson::as_document)
                .and_then(|stage| stage.get_document("$currentOp").ok())
                .and_then(|stage| stage.get_bool("localOps").ok()),
            Some(true)
        );
        assert_eq!(
            command
                .get_array("pipeline")
                .expect("pipeline")
                .get(1)
                .and_then(Bson::as_document)
                .and_then(|stage| stage.get_document("$project").ok())
                .and_then(|stage| stage.get_i32("ns").ok()),
            Some(1)
        );
        assert!(command.get_document("cursor").expect("cursor").is_empty());
        assert_eq!(command.get_str("$db").expect("$db"), "admin");

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn aggregate_out_replaces_the_target_collection_and_returns_an_empty_cursor() {
        let (serve_task, _temp_dir, manifest) = start_broker("aggregate-out.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "sku": "a", "qty": 2 },
                    { "_id": 2, "sku": "b", "qty": 1 }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let aggregate = send_command(
            &mut stream,
            doc! {
                "aggregate": "widgets",
                "pipeline": [
                    { "$match": { "qty": { "$gte": 2 } } },
                    { "$out": "report" }
                ],
                "cursor": {},
                "$db": "app"
            },
        )
        .await;
        let first_batch = aggregate
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        assert!(first_batch.is_empty());

        let report = send_command(
            &mut stream,
            doc! {
                "find": "report",
                "$db": "app"
            },
        )
        .await;
        let report_batch = report
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        assert_eq!(report_batch.len(), 1);
        assert_eq!(
            report_batch[0]
                .as_document()
                .expect("document")
                .get_str("sku")
                .expect("sku"),
            "a"
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_on_a_missing_collection_is_a_noop() {
        let (serve_task, _temp_dir, manifest) = start_broker("delete-noop.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let delete = send_command(
            &mut stream,
            doc! {
                "delete": "widgets",
                "deletes": [{ "q": {}, "limit": 0 }],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(delete.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(delete.get_i32("n").expect("n"), 0);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn drop_database_removes_all_collections_and_hides_the_database() {
        let (serve_task, _temp_dir, manifest) = start_broker("drop-database.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        for collection in ["widgets", "gadgets"] {
            let insert = send_command(
                &mut stream,
                doc! {
                    "insert": collection,
                    "documents": [{ "_id": ObjectId::new(), "name": collection }],
                    "$db": "app"
                },
            )
            .await;
            assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);
        }

        let drop_database =
            send_command(&mut stream, doc! { "dropDatabase": 1, "$db": "app" }).await;
        assert_eq!(drop_database.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(drop_database.get_str("dropped").expect("dropped"), "app");

        let list_databases = send_command(
            &mut stream,
            doc! { "listDatabases": 1, "nameOnly": true, "$db": "admin" },
        )
        .await;
        let databases = list_databases.get_array("databases").expect("databases");
        assert!(!databases.iter().any(|entry| {
            entry
                .as_document()
                .and_then(|document| document.get_str("name").ok())
                == Some("app")
        }));

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_ixscan_for_indexed_find() {
        let (serve_task, _temp_dir, manifest) = start_broker("explain.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(winning_plan.get_str("indexName").expect("index"), "sku_1");

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_cursor_planner_for_aggregate() {
        let (serve_task, _temp_dir, manifest) = start_broker("aggregate-explain.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [{ "_id": 1, "sku": "a", "qty": 2 }],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "aggregate": "widgets",
                    "pipeline": [
                        { "$match": { "sku": "a" } },
                        { "$group": { "_id": "$sku", "total": { "$sum": "$qty" } } }
                    ],
                    "cursor": {}
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
        )
        .await;
        let stages = explain.get_array("stages").expect("stages");
        let cursor_stage = stages[0].as_document().expect("cursor stage");
        let cursor = cursor_stage.get_document("$cursor").expect("$cursor");
        let planner = cursor.get_document("queryPlanner").expect("queryPlanner");
        assert_eq!(
            planner.get_str("namespace").expect("namespace"),
            "app.widgets"
        );
        assert!(planner.get_document("winningPlan").is_ok());

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_query_planner_for_delete_update_distinct_and_find_and_modify() {
        let (serve_task, _temp_dir, manifest) = start_broker("crud-explain.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [{ "_id": 1, "sku": "a", "qty": 2 }],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        for command in [
            doc! {
                "explain": {
                    "delete": "widgets",
                    "deletes": [{ "q": { "sku": "a" }, "limit": 1 }]
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
            doc! {
                "explain": {
                    "update": "widgets",
                    "updates": [{ "q": { "sku": "a" }, "u": { "$set": { "qty": 3 } } }]
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
            doc! {
                "explain": {
                    "distinct": "widgets",
                    "key": "sku",
                    "query": { "sku": "a" }
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
            doc! {
                "explain": {
                    "findAndModify": "widgets",
                    "query": { "sku": "a" },
                    "remove": true
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
        ] {
            let explain = send_command(&mut stream, command).await;
            let planner = explain.get_document("queryPlanner").expect("queryPlanner");
            assert_eq!(
                planner.get_str("namespace").expect("namespace"),
                "app.widgets"
            );
            assert!(planner.get_document("winningPlan").is_ok());
        }

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_rejects_aggregate_pipelines_with_out() {
        assert_rejected(
            doc! {
                "explain": {
                    "aggregate": "widgets",
                    "pipeline": [
                        { "$project": { "_id": 0 } },
                        { "$out": "report" }
                    ],
                    "cursor": {}
                },
                "verbosity": "queryPlanner",
                "$db": "app"
            },
            115,
        )
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_compound_prefix_and_sort_coverage() {
        let (serve_task, _temp_dir, manifest) = start_broker("compound.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1", "unique": false }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "category": "tools" },
                    "sort": { "qty": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(
            winning_plan.get_str("indexName").expect("index"),
            "category_1_qty_-1"
        );
        assert!(winning_plan.get_bool("sortCovered").expect("sort covered"));
        assert_eq!(
            winning_plan.get_i32("scanDirection").expect("direction"),
            -1
        );
        assert_eq!(winning_plan.get_i32("matchedFields").expect("matched"), 1);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_descending_compound_range_bounds() {
        let (serve_task, _temp_dir, manifest) = start_broker("compound-range.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1", "unique": false }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": {
                        "category": "tools",
                        "qty": { "$gt": 3, "$lte": 9 }
                    },
                    "sort": { "qty": -1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(
            winning_plan.get_str("indexName").expect("index"),
            "category_1_qty_-1"
        );
        assert_eq!(
            winning_plan.get_document("lowerBound").expect("lower"),
            &doc! { "category": "tools", "qty": 9 }
        );
        assert!(
            winning_plan
                .get_bool("lowerInclusive")
                .expect("lower inclusive")
        );
        assert_eq!(
            winning_plan.get_document("upperBound").expect("upper"),
            &doc! { "category": "tools", "qty": 3 }
        );
        assert!(
            !winning_plan
                .get_bool("upperInclusive")
                .expect("upper inclusive")
        );
        assert_eq!(winning_plan.get_i32("matchedFields").expect("matched"), 2);
        assert!(winning_plan.get_bool("sortCovered").expect("sort covered"));
        assert_eq!(winning_plan.get_i32("scanDirection").expect("direction"), 1);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn find_uses_compound_index_order_for_sort() {
        let (serve_task, _temp_dir, manifest) = start_broker("compound-find.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1", "unique": false }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "category": "tools", "qty": 9 },
                    { "_id": 2, "category": "tools", "qty": 3 },
                    { "_id": 3, "category": "tools", "qty": 5 },
                    { "_id": 4, "category": "garden", "qty": 1 }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": { "category": "tools" },
                "sort": { "qty": 1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        let quantities = first_batch
            .iter()
            .map(|value| {
                value
                    .as_document()
                    .expect("document")
                    .get_i32("qty")
                    .expect("qty")
            })
            .collect::<Vec<_>>();
        assert_eq!(quantities, vec![3, 5, 9]);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn find_uses_compound_descending_range_scan() {
        let (serve_task, _temp_dir, manifest) = start_broker("compound-range-find.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": -1 }, "name": "category_1_qty_-1", "unique": false }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "category": "tools", "qty": 9 },
                    { "_id": 2, "category": "tools", "qty": 3 },
                    { "_id": 3, "category": "tools", "qty": 5 },
                    { "_id": 4, "category": "garden", "qty": 1 }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": {
                    "category": "tools",
                    "qty": { "$gt": 3, "$lte": 9 }
                },
                "sort": { "qty": -1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        let quantities = first_batch
            .iter()
            .map(|value| {
                value
                    .as_document()
                    .expect("document")
                    .get_i32("qty")
                    .expect("qty")
            })
            .collect::<Vec<_>>();
        assert_eq!(quantities, vec![9, 5]);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_prefers_lower_cost_index_over_wider_compound_index() {
        let (serve_task, _temp_dir, manifest) = start_broker("cost-based.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "status": 1 }, "name": "category_1_status_1" },
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let mut documents = Vec::new();
        for value in 0..40_i32 {
            documents.push(doc! {
                "_id": value,
                "category": "tools",
                "status": "active",
                "sku": format!("sku-{value:03}"),
            });
        }
        documents.push(doc! {
            "_id": 100,
            "category": "tools",
            "status": "active",
            "sku": "target",
        });
        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": documents,
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": {
                        "category": "tools",
                        "status": "active",
                        "sku": "target"
                    }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(winning_plan.get_str("indexName").expect("index"), "sku_1");
        assert_eq!(winning_plan.get_i32("keysExamined").expect("keys"), 1);
        assert_eq!(winning_plan.get_i32("docsExamined").expect("docs"), 1);
        assert!(
            !winning_plan
                .get_bool("filterCovered")
                .expect("filter covered")
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn find_uses_projection_covered_index_scan() {
        let (serve_task, _temp_dir, manifest) = start_broker("projection-covered.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": 1, "_id": 1 }, "name": "category_1_qty_1_id_1" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "category": "tools", "qty": 3, "secret": "alpha" },
                    { "_id": 2, "category": "tools", "qty": 5, "secret": "beta" },
                    { "_id": 3, "category": "garden", "qty": 1, "secret": "gamma" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "category": "tools" },
                    "projection": { "category": 1, "qty": 1, "_id": 1 },
                    "sort": { "qty": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(
            winning_plan.get_str("indexName").expect("index"),
            "category_1_qty_1_id_1"
        );
        assert!(
            winning_plan
                .get_bool("filterCovered")
                .expect("filter covered")
        );
        assert!(
            winning_plan
                .get_bool("projectionCovered")
                .expect("projection covered")
        );
        assert!(winning_plan.get_bool("sortCovered").expect("sort covered"));
        assert_eq!(winning_plan.get_i32("docsExamined").expect("docs"), 0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": { "category": "tools" },
                "projection": { "category": 1, "qty": 1, "_id": 1 },
                "sort": { "qty": 1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|value| {
                    value
                        .as_document()
                        .expect("document")
                        .get_i32("qty")
                        .expect("qty")
                })
                .collect::<Vec<_>>(),
            vec![3, 5]
        );
        assert!(first_batch.iter().all(|value| {
            value
                .as_document()
                .expect("document")
                .get("secret")
                .is_none()
        }));

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_uses_point_interval_as_prefix_for_suffix_range_and_sort() {
        let (serve_task, _temp_dir, manifest) = start_broker("point-prefix.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "qty": 1, "sku": 1 }, "name": "category_1_qty_1_sku_1" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": {
                        "category": "tools",
                        "qty": { "$gte": 5, "$lte": 5 },
                        "sku": { "$gt": "b" }
                    },
                    "sort": { "sku": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(
            winning_plan.get_str("indexName").expect("index"),
            "category_1_qty_1_sku_1"
        );
        assert_eq!(winning_plan.get_i32("matchedFields").expect("matched"), 3);
        assert!(winning_plan.get_bool("sortCovered").expect("sort covered"));
        assert_eq!(
            winning_plan.get_document("lowerBound").expect("lower"),
            &doc! { "category": "tools", "qty": 5, "sku": "b" }
        );
        assert!(
            !winning_plan
                .get_bool("lowerInclusive")
                .expect("lower inclusive")
        );
        assert_eq!(
            winning_plan.get_document("upperBound").expect("upper"),
            &doc! { "category": "tools", "qty": 5, "sku": Bson::MaxKey }
        );
        assert!(
            winning_plan
                .get_bool("upperInclusive")
                .expect("upper inclusive")
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_reports_plan_cache_usage_and_invalidates_after_write() {
        let (serve_task, _temp_dir, manifest) = start_broker("plan-cache.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let first_explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        let first_planner = first_explain
            .get_document("queryPlanner")
            .expect("query planner");
        assert!(
            !first_planner
                .get_bool("planCacheUsed")
                .expect("plan cache used")
        );

        let second_explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        let second_planner = second_explain
            .get_document("queryPlanner")
            .expect("query planner");
        assert!(
            second_planner
                .get_bool("planCacheUsed")
                .expect("plan cache used")
        );
        assert_eq!(
            second_planner
                .get_document("winningPlan")
                .expect("winning plan")
                .get_str("indexName")
                .expect("index name"),
            "sku_1"
        );

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [{ "_id": 1, "sku": "alpha" }],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let third_explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        let third_planner = third_explain
            .get_document("queryPlanner")
            .expect("query planner");
        assert!(
            !third_planner
                .get_bool("planCacheUsed")
                .expect("plan cache used")
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_uses_persisted_plan_cache_after_restart() {
        let temp_dir = tempdir().expect("tempdir");
        let database_path = temp_dir.path().join("persisted-plan-cache.mongodb");

        let (serve_task, manifest) = start_broker_at(&database_path).await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        assert!(
            !explain
                .get_document("queryPlanner")
                .expect("query planner")
                .get_bool("planCacheUsed")
                .expect("plan cache used")
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");

        let (serve_task, manifest) = start_broker_at(&database_path).await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");
        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "sku": "alpha" }
                },
                "$db": "app"
            },
        )
        .await;
        let planner = explain.get_document("queryPlanner").expect("query planner");
        assert!(planner.get_bool("planCacheUsed").expect("plan cache used"));
        assert_eq!(
            planner
                .get_document("winningPlan")
                .expect("winning plan")
                .get_str("indexName")
                .expect("index"),
            "sku_1"
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_uses_branch_union_or_plan_for_distinct_indexes() {
        let (serve_task, _temp_dir, manifest) = start_broker("branch-union-or.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true },
                    { "key": { "qty": 1 }, "name": "qty_1" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "sku": "alpha", "qty": 1 },
                    { "_id": 2, "sku": "beta", "qty": 10 },
                    { "_id": 3, "sku": "gamma", "qty": 7 },
                    { "_id": 4, "sku": "delta", "qty": 2 }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": {
                        "$or": [
                            { "sku": "alpha" },
                            { "qty": { "$gt": 5 } }
                        ]
                    },
                    "sort": { "qty": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "OR");
        let input_stages = winning_plan.get_array("inputStages").expect("input stages");
        assert_eq!(input_stages.len(), 2);
        assert_eq!(
            input_stages[0]
                .as_document()
                .expect("stage")
                .get_str("stage")
                .expect("stage"),
            "IXSCAN"
        );
        assert_eq!(
            input_stages[1]
                .as_document()
                .expect("stage")
                .get_str("stage")
                .expect("stage"),
            "IXSCAN"
        );
        assert!(
            winning_plan
                .get_bool("requiresSort")
                .expect("requires sort")
        );

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": {
                    "$or": [
                        { "sku": "alpha" },
                        { "qty": { "$gt": 5 } }
                    ]
                },
                "sort": { "qty": 1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|value| {
                    value
                        .as_document()
                        .expect("document")
                        .get_str("sku")
                        .expect("sku")
                })
                .collect::<Vec<_>>(),
            vec!["alpha", "gamma", "beta"]
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explain_uses_multi_interval_or_scan_with_compound_suffix_bounds() {
        let (serve_task, _temp_dir, manifest) = start_broker("multi-interval.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "category": 1, "sku": 1 }, "name": "category_1_sku_1" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "category": "tools", "sku": "a" },
                    { "_id": 2, "category": "tools", "sku": "b" },
                    { "_id": 3, "category": "tools", "sku": "c" },
                    { "_id": 4, "category": "garden", "sku": "a" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": {
                        "$or": [
                            { "category": "tools", "sku": "a" },
                            { "category": "tools", "sku": "b" }
                        ]
                    },
                    "projection": { "_id": 0, "category": 1, "sku": 1 },
                    "sort": { "sku": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert_eq!(
            winning_plan.get_str("indexName").expect("index"),
            "category_1_sku_1"
        );
        assert_eq!(
            winning_plan
                .get_i32("intervalCount")
                .expect("interval count"),
            2
        );
        assert_eq!(winning_plan.get_i32("matchedFields").expect("matched"), 2);
        assert!(winning_plan.get_bool("sortCovered").expect("sort covered"));
        assert!(
            winning_plan
                .get_bool("projectionCovered")
                .expect("projection covered")
        );
        assert_eq!(winning_plan.get_i32("docsExamined").expect("docs"), 0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": {
                    "$or": [
                        { "category": "tools", "sku": "a" },
                        { "category": "tools", "sku": "b" }
                    ]
                },
                "projection": { "_id": 0, "category": 1, "sku": 1 },
                "sort": { "sku": 1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        assert_eq!(
            first_batch
                .iter()
                .map(|value| {
                    value
                        .as_document()
                        .expect("document")
                        .get_str("sku")
                        .expect("sku")
                })
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn find_uses_covered_index_scan_for_null_and_missing_distinction() {
        let (serve_task, _temp_dir, manifest) = start_broker("null-vs-missing.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "flag": 1, "sku": 1 }, "name": "flag_1_sku_1" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "sku": "missing" },
                    { "_id": 2, "sku": "null", "flag": Bson::Null },
                    { "_id": 3, "sku": "set", "flag": "yes" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        let explain = send_command(
            &mut stream,
            doc! {
                "explain": {
                    "find": "widgets",
                    "filter": { "flag": Bson::Null },
                    "projection": { "_id": 0, "flag": 1, "sku": 1 },
                    "sort": { "sku": 1 }
                },
                "$db": "app"
            },
        )
        .await;
        let winning_plan = explain
            .get_document("queryPlanner")
            .expect("query planner")
            .get_document("winningPlan")
            .expect("winning plan");
        assert_eq!(winning_plan.get_str("stage").expect("stage"), "IXSCAN");
        assert!(
            winning_plan
                .get_bool("filterCovered")
                .expect("filter covered")
        );
        assert!(
            winning_plan
                .get_bool("projectionCovered")
                .expect("projection covered")
        );
        assert_eq!(winning_plan.get_i32("docsExamined").expect("docs"), 0);

        let find = send_command(
            &mut stream,
            doc! {
                "find": "widgets",
                "filter": { "flag": Bson::Null },
                "projection": { "_id": 0, "flag": 1, "sku": 1 },
                "sort": { "sku": 1 },
                "$db": "app"
            },
        )
        .await;
        let first_batch = find
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("first batch");
        assert_eq!(first_batch.len(), 1);
        let document = first_batch[0].as_document().expect("document");
        assert_eq!(document.get_str("sku").expect("sku"), "null");
        assert_eq!(document.get("flag"), Some(&Bson::Null));

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preserves_unique_indexes_across_broker_restart() {
        let temp_dir = tempdir().expect("tempdir");
        let database_path = temp_dir.path().join("persisted-index.mongodb");

        let (serve_task, manifest) = start_broker_at(&database_path).await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let create_indexes = send_command(
            &mut stream,
            doc! {
                "createIndexes": "widgets",
                "indexes": [
                    { "key": { "sku": 1 }, "name": "sku_1", "unique": true }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(create_indexes.get_f64("ok").expect("ok"), 1.0);

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 1, "sku": "alpha" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");

        let (serve_task, manifest) = start_broker_at(&database_path).await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");
        let duplicate = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [
                    { "_id": 2, "sku": "alpha" }
                ],
                "$db": "app"
            },
        )
        .await;
        assert_eq!(duplicate.get_f64("ok").expect("ok"), 0.0);
        assert_eq!(duplicate.get_i32("code").expect("code"), 11000);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_sessions_and_transactions() {
        assert_rejected(
            doc! {
                "ping": 1,
                "lsid": { "id": ObjectId::new() },
                "$db": "admin"
            },
            115,
        )
        .await;
        assert_rejected(doc! { "ping": 1, "txnNumber": 1_i64, "$db": "admin" }, 115).await;
        assert_rejected(
            doc! { "ping": 1, "startTransaction": true, "autocommit": false, "$db": "admin" },
            115,
        )
        .await;
        assert_rejected(
            doc! {
                "killAllSessions": [],
                "lsid": { "id": ObjectId::new() },
                "$db": "admin"
            },
            115,
        )
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn accepts_default_write_concern_as_a_noop() {
        let (serve_task, _temp_dir, manifest) = start_broker("default-write-concern.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let insert = send_command(
            &mut stream,
            doc! {
                "insert": "widgets",
                "documents": [{ "_id": 1, "sku": "alpha" }],
                "writeConcern": { "w": 1, "j": false, "wtimeout": 0 },
                "$db": "app"
            },
        )
        .await;
        assert_eq!(insert.get_f64("ok").expect("ok"), 1.0);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_read_concern_and_nondefault_write_concern_envelopes() {
        assert_rejected(
            doc! { "ping": 1, "readConcern": { "level": "majority" }, "$db": "admin" },
            115,
        )
        .await;
        assert_rejected(
            doc! { "ping": 1, "writeConcern": { "w": "majority" }, "$db": "admin" },
            115,
        )
        .await;
        assert_rejected(
            doc! { "ping": 1, "writeConcern": { "w": 1, "journal": true }, "$db": "admin" },
            115,
        )
        .await;
        assert_rejected(
            doc! { "ping": 1, "$readPreference": { "mode": "secondary" }, "$db": "admin" },
            115,
        )
        .await;
        assert_rejected(
            doc! {
                "explain": { "find": "widgets", "$db": "app" },
                "verbosity": "unsupported",
                "$db": "app"
            },
            9,
        )
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serves_admin_compatibility_commands_for_driver_harnesses() {
        let (serve_task, _temp_dir, manifest) = start_broker("admin-compat.mongodb").await;
        let mut stream = connect(&manifest.endpoint).await.expect("connect");

        let kill_all_sessions =
            send_command(&mut stream, doc! { "killAllSessions": [], "$db": "admin" }).await;
        assert_eq!(kill_all_sessions.get_f64("ok").expect("ok"), 1.0);

        let all_parameters =
            send_command(&mut stream, doc! { "getParameter": "*", "$db": "admin" }).await;
        assert_eq!(all_parameters.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(
            all_parameters
                .get_array("authenticationMechanisms")
                .expect("authenticationMechanisms"),
            &bson::Array::new()
        );
        assert!(
            !all_parameters
                .get_bool("requireApiVersion")
                .expect("requireApiVersion")
        );

        let selected_parameters = send_command(
            &mut stream,
            doc! {
                "getParameter": 1,
                "authenticationMechanisms": 1,
                "$db": "admin"
            },
        )
        .await;
        assert_eq!(selected_parameters.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(
            selected_parameters
                .get_array("authenticationMechanisms")
                .expect("authenticationMechanisms"),
            &bson::Array::new()
        );
        assert!(selected_parameters.get("requireApiVersion").is_none());

        drop(stream);
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("shutdown timeout")
            .expect("join")
            .expect("serve");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_unsupported_commands() {
        assert_rejected(
            doc! {
                "findAndModify": "widgets",
                "query": { "_id": 1 },
                "$db": "app"
            },
            115,
        )
        .await;
    }
}
