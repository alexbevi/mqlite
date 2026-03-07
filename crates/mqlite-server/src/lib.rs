use std::{
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
use mqlite_bson::{ensure_object_id, lookup_path_owned};
use mqlite_catalog::{CatalogError, IndexCatalog};
use mqlite_exec::{CursorError, CursorManager};
use mqlite_ipc::{
    BoxedStream, BrokerManifest, BrokerPaths, IpcListener, broker_paths, cleanup_endpoint,
    remove_manifest, write_manifest,
};
use mqlite_query::{
    QueryError, apply_projection, apply_update, document_matches, parse_update, run_pipeline,
    upsert_seed_from_query,
};
use mqlite_storage::DatabaseFile;
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

const MAX_BSON_OBJECT_SIZE: i32 = 16 * 1024 * 1024;
const MAX_MESSAGE_SIZE_BYTES: i32 = 48 * 1024 * 1024;
const MAX_WRITE_BATCH_SIZE: i32 = 100_000;

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
        Ok(Self {
            config,
            paths,
            storage: Arc::new(RwLock::new(storage)),
            cursors: Arc::new(Mutex::new(CursorManager::new())),
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

        remove_manifest(&self.paths.manifest_path)?;
        cleanup_endpoint(&self.paths.endpoint)?;
        Ok(())
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
            "listDatabases" => self.handle_list_databases(body),
            "listCollections" => self.handle_list_collections(body),
            "listIndexes" => self.handle_list_indexes(body),
            "create" => self.handle_create(body),
            "drop" => self.handle_drop(body),
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

    fn handle_create(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection = body.get_str("create").map_err(|_| {
            CommandError::new(9, "FailedToParse", "create requires a collection name")
        })?;

        let mut options = body.clone();
        options.remove("create");
        options.remove("$db");

        let mut storage = self.storage.write();
        storage
            .catalog_mut()
            .create_collection(&database, collection, options)?;
        storage.checkpoint().map_err(internal_error)?;
        Ok(Document::new())
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
            .catalog_mut()
            .drop_collection(&database, collection)?;
        storage.checkpoint().map_err(internal_error)?;
        Ok(doc! {
            "ns": format!("{database}.{collection}"),
            "nIndexesWas": index_count,
        })
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
        let before = storage
            .catalog()
            .get_collection(&database, collection)
            .map(|collection| collection.indexes.len())
            .unwrap_or(0) as i32;
        let created = storage
            .catalog_mut()
            .create_indexes(&database, collection, &specs)?;
        storage.checkpoint().map_err(internal_error)?;
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
        let removed = storage
            .catalog_mut()
            .drop_indexes(&database, collection, target)?;
        storage.checkpoint().map_err(internal_error)?;
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
        storage
            .catalog_mut()
            .ensure_collection(&database, collection_name);
        let indexes = storage.catalog().list_indexes(&database, collection_name)?;

        let inserted_total = {
            let collection = storage
                .catalog_mut()
                .get_collection_mut(&database, collection_name)?;
            for mut document in documents {
                ensure_object_id(&mut document);
                enforce_unique_indexes(&indexes, &collection.documents, &document, None)?;
                collection.documents.push(document);
            }
            collection.documents.len() as i32
        };
        storage.checkpoint().map_err(internal_error)?;
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

        let storage = self.storage.read();
        let mut documents = storage
            .catalog()
            .get_collection(&database, collection)
            .map(|collection| collection.documents.clone())
            .unwrap_or_default()
            .into_iter()
            .filter(|document| document_matches(document, &filter).unwrap_or(false))
            .collect::<Vec<_>>();

        if let Some(sort) = sort.as_ref() {
            sort_documents(&mut documents, sort);
        }
        documents = documents.into_iter().skip(skip).collect();
        if limit > 0 {
            documents.truncate(limit as usize);
        }
        let documents = documents
            .into_iter()
            .map(|document| apply_projection(&document, projection.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

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
        storage
            .catalog_mut()
            .ensure_collection(&database, collection_name);
        let indexes = storage.catalog().list_indexes(&database, collection_name)?;
        let (matched, modified, upserted) = {
            let collection = storage
                .catalog_mut()
                .get_collection_mut(&database, collection_name)?;

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

                let matching_indexes = collection
                    .documents
                    .iter()
                    .enumerate()
                    .filter_map(|(index, document)| {
                        document_matches(document, query)
                            .ok()
                            .and_then(|matches| matches.then_some(index))
                    })
                    .collect::<Vec<_>>();

                if matching_indexes.is_empty() {
                    if upsert {
                        let mut document = upsert_seed_from_query(query);
                        apply_update(&mut document, &update_spec)?;
                        let upserted_id = ensure_object_id(&mut document);
                        enforce_unique_indexes(&indexes, &collection.documents, &document, None)?;
                        collection.documents.push(document);
                        upserted.push(doc! { "index": operation_index as i32, "_id": upserted_id });
                    }
                    continue;
                }

                let mut touched = 0;
                for document_index in matching_indexes {
                    let original = collection.documents[document_index].clone();
                    let mut updated = original.clone();
                    apply_update(&mut updated, &update_spec)?;
                    enforce_unique_indexes(
                        &indexes,
                        &collection.documents,
                        &updated,
                        Some(document_index),
                    )?;
                    matched += 1;
                    if updated != original {
                        collection.documents[document_index] = updated;
                        modified += 1;
                    }
                    touched += 1;
                    if !multi && touched >= 1 {
                        break;
                    }
                }
            }

            (matched, modified, upserted)
        };

        storage.checkpoint().map_err(internal_error)?;
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

        let mut storage = self.storage.write();
        let deleted = {
            let collection = storage
                .catalog_mut()
                .get_collection_mut(&database, collection_name)?;
            let mut deleted = 0_i32;

            for operation in operations {
                let query = operation.get_document("q").map_err(|_| {
                    CommandError::new(9, "FailedToParse", "delete operations require `q`")
                })?;
                let limit = operation.get_i32("limit").unwrap_or(0);
                let mut removed = 0_i32;
                collection.documents.retain(|document| {
                    let matches = document_matches(document, query).unwrap_or(false);
                    if matches && (limit == 0 || removed == 0) {
                        removed += 1;
                        deleted += 1;
                        false
                    } else {
                        true
                    }
                });
            }

            deleted
        };

        storage.checkpoint().map_err(internal_error)?;
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
                    .documents
                    .iter()
                    .filter(|document| document_matches(document, &query).unwrap_or(false))
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
            for document in &collection.documents {
                if !document_matches(document, &query).unwrap_or(false) {
                    continue;
                }
                let value = lookup_path_owned(document, key).unwrap_or(Bson::Null);
                if !seen.iter().any(|existing| existing == &value) {
                    seen.push(value);
                }
            }
        }

        Ok(doc! { "values": seen })
    }

    fn handle_aggregate(&self, body: &Document) -> Result<Document, CommandError> {
        let database = database_name(body)?;
        let collection_name = body
            .get("aggregate")
            .and_then(Bson::as_str)
            .ok_or_else(|| {
                CommandError::new(9, "FailedToParse", "aggregate requires a collection name")
            })?;
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

        let storage = self.storage.read();
        let input = storage
            .catalog()
            .get_collection(&database, collection_name)
            .map(|collection| collection.documents.clone())
            .unwrap_or_default();
        let results = run_pipeline(input, &pipeline)?;
        let cursor = self.cursors.lock().open(
            format!("{database}.{collection_name}"),
            results,
            batch_size,
            false,
        );
        Ok(cursor_document(cursor, "firstBatch"))
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

fn body_batch_size(body: &Document, field: &str) -> Option<i64> {
    body.get_document(field)
        .ok()
        .and_then(|cursor| cursor.get_i64("batchSize").ok())
        .or_else(|| body.get_i64("batchSize").ok())
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
    const UNSUPPORTED_KEYS: [(&str, &str); 7] = [
        ("lsid", "logical sessions are not supported"),
        ("txnNumber", "transactions are not supported"),
        ("startTransaction", "transactions are not supported"),
        ("autocommit", "transactions are not supported"),
        ("readConcern", "read concern is not supported"),
        ("writeConcern", "write concern is not supported"),
        ("$readPreference", "read preference is not supported"),
    ];

    for (key, message) in UNSUPPORTED_KEYS {
        if body.contains_key(key) {
            return Err(CommandError::new(115, "CommandNotSupported", message));
        }
    }

    Ok(())
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

fn enforce_unique_indexes(
    indexes: &[IndexCatalog],
    existing_documents: &[Document],
    candidate: &Document,
    skip_index: Option<usize>,
) -> Result<(), CommandError> {
    for index in indexes.iter().filter(|index| index.unique) {
        let candidate_key = unique_index_key(candidate, index);
        let conflict = existing_documents
            .iter()
            .enumerate()
            .any(|(position, document)| {
                if skip_index == Some(position) {
                    return false;
                }
                unique_index_key(document, index) == candidate_key
            });
        if conflict {
            return Err(CommandError::new(
                11000,
                "DuplicateKey",
                format!("duplicate key error on index `{}`", index.name),
            ));
        }
    }
    Ok(())
}

fn unique_index_key(document: &Document, index: &IndexCatalog) -> Document {
    let mut key = Document::new();
    for (field, _) in &index.key {
        key.insert(
            field,
            lookup_path_owned(document, field).unwrap_or(Bson::Null),
        );
    }
    key
}

#[cfg(test)]
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

    async fn start_broker(
        database_name: &str,
    ) -> (
        JoinHandle<anyhow::Result<()>>,
        tempfile::TempDir,
        mqlite_ipc::BrokerManifest,
    ) {
        let temp_dir = tempdir().expect("tempdir");
        let database_path = temp_dir.path().join(database_name);
        let broker = Broker::new(BrokerConfig::new(&database_path, 1)).expect("broker");
        let manifest_path = broker.paths().manifest_path.clone();
        let serve_task = tokio::spawn(broker.clone().serve());
        wait_for_manifest(&manifest_path, &serve_task).await;
        let manifest = read_manifest(&manifest_path).expect("manifest");
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
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_read_and_write_concern_envelopes() {
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
            doc! { "ping": 1, "$readPreference": { "mode": "secondary" }, "$db": "admin" },
            115,
        )
        .await;
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
