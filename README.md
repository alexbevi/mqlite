# mqlite

`mqlite` is an in-process-style MongoDB storage and query engine exposed through a local broker binary. Drivers connect through a `file://` URI, discover or spawn the local broker for a specific `.mongodb` file, and then communicate with the broker using MongoDB `OP_MSG` messages over local IPC instead of TCP.

## Current Scope

The repository now contains a working Rust workspace baseline with:
- A cross-crate architecture aligned to the long-term plan.
- A single-file durable store with a fixed header, dual superblocks, append-only WAL, and checkpoint snapshots backed by slotted record pages plus persisted B-tree index pages.
- Local IPC manifest and endpoint generation.
- `OP_MSG` encoding and decoding.
- A broker with core command handling and cursor support.
- A source-driven capability sync tool and checked-in MongoDB gap analysis for query and aggregation support.
- A direct CLI command path for broker validation without any patched driver.
- Progressive tests, CI scaffolding, and living specs.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the storage, index, broker, and command-execution design details.

## Build

Build from the workspace root with Rust 1.87 or newer. The workspace uses Rust edition 2024 and CI runs on stable Rust across macOS, Linux, and Windows.

Install the toolchain with `rustup` if needed:

```text
rustup toolchain install stable
rustup default stable
```

Build the CLI binary:

```text
cargo build -p mqlite
```

Build an optimized release binary:

```text
cargo build -p mqlite --release
```

Run the repo checks used by CI:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

The resulting binary is written to:
- `target/debug/mqlite` for `cargo build -p mqlite`
- `target/release/mqlite` for `cargo build -p mqlite --release`

## Upstream Reference Anchors

This baseline was shaped against the checked-out MongoDB references in the parent workspace:
- MongoDB server IDL and command metadata:
  - `../mongo/src/mongo/idl/generic_argument.idl`
  - `../mongo/src/mongo/db/repl/hello/hello.idl`
  - `../mongo/src/mongo/db/query/find_command.idl`
  - `../mongo/src/mongo/db/pipeline/aggregate_command.idl`
  - `../mongo/src/mongo/db/query/write_ops/write_ops.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/list_collections.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/list_indexes.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/create_indexes.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/drop_indexes.idl`
  - `../mongo/src/mongo/util/buildinfo.idl`
- Node driver integration seams:
  - `../node-mongodb-native/src/connection_string.ts`
  - `../node-mongodb-native/src/cmap/connect.ts`
  - `../node-mongodb-native/src/client-side-encryption/mongocryptd_manager.ts`
- Driver specifications:
  - `../specifications/source/mongodb-handshake/handshake.md`
  - `../specifications/source/connection-string/connection-string-spec.md`
  - `../specifications/source/compression/OP_COMPRESSED.md`

## Capability Gap Analysis

The repo now keeps a checked-in capability snapshot and gap report generated from the sibling MongoDB source checkout:
- `capabilities/mongodb/upstream-capabilities.generated.json`
- `capabilities/mqlite/support.generated.json`
- `capabilities/mqlite/gap-analysis.generated.json`
- `capabilities/mqlite/gap-analysis.generated.md`

These files are generated from the MongoDB query parser and aggregation registration sources, with command IDL files kept as reference anchors for command shape context. They are intended to drive gap analysis and direct validation planning for `mqlite`.

Resync the checked-in reports from the local sibling `mongo` checkout with:

```text
cargo run -p mqlite-capabilities -- sync
```

Check that the generated files are current without rewriting them with:

```text
cargo run -p mqlite-capabilities -- sync --check
```

## URI Model

Use an absolute file URI:

```text
file:///absolute/path/to/database.mongodb?db=app
```

- The URI path is always the filesystem path to the database file.
- The optional `db` query parameter selects the default database.
- Drivers must reject incompatible network-style options for `file://`, including auth, TLS, proxying, network compression, non-primary read preference, read concern, non-default write concern, replica-set/load-balanced options, and retryable reads or writes. The default acknowledged write concern remains a compatibility no-op, so `w=1`, `journal=false`, and zero write-concern timeouts may be accepted.

## Supported Features

### Broker and protocol
- `OP_MSG` request-response transport over local IPC.
- Unix domain sockets on POSIX.
- Named pipe endpoint naming on Windows.
- Ephemeral manifest discovery per database file.
- `hello`, `ping`, and `buildInfo` bootstrap commands.
- Compatibility-only admin helpers for driver test harnesses:
  - `killAllSessions` as a no-op cleanup command
  - `getParameter` with a minimal static reply for `authenticationMechanisms` and `requireApiVersion`

### Storage and catalog
- One durable `.mongodb` file per broker.
- Fixed file header and rotating superblocks.
- Append-only WAL for typed collection mutations.
- Checkpoint snapshots store collection records in fixed-size slotted pages with stable `RecordId`s.
- Secondary and unique index entries are stored in dedicated B-tree pages with internal and leaf nodes, keyed by BSON plus `RecordId`, and are validated against collection pages on reopen.
- Recovery replays WAL on top of the newest valid checkpoint and can fall back to an older superblock when the latest checkpoint pages are damaged.
- Multiple databases and collections in one file.
- Collection catalog metadata and persistent index state.

### Query and command surface
- `listDatabases`
- `listCollections`
- `listIndexes`
- `explain` for `find`
- `explain` for wrapped CRUD commands that plan a single target query, including `delete`, `update`, `distinct`, and `findAndModify`
- `explain` for collection-backed `aggregate` pipelines that do not end in `$out`, returning a `$cursor.queryPlanner` stage
- `create`
- `dropDatabase`
- `drop`
- `renameCollection`
- `createIndexes`
- `dropIndexes`
- `insert`
- `find`
- `getMore`
- `killCursors`
- `update`
- `delete`
- `count`
- `distinct`
- `aggregate`
- `listCollections` on a missing database returns an empty cursor for driver setup and cleanup flows.
- Collectionless `db.aggregate([{ $currentOp: { localOps: true } }])` compatibility on `admin`, returning the in-flight aggregate command for driver CRUD coverage.
- Collection-backed aggregation with a trailing `$out` stage, replacing the target collection and returning an empty cursor.
- Null-byte database or collection names are rejected with `InvalidNamespace`.
- TTL index metadata such as `expireAfterSeconds` is preserved in catalog state and returned by `listIndexes`, even though document expiration itself is not implemented.
- `configureFailPoint: "failCommand"` supports local driver-test compatibility for `mode: "off"`, `mode: "alwaysOn"`, and `mode: { times: N }`, with `failCommands`, `errorCode`, `errorLabels`, `blockConnection`, `blockTimeMS`, and `closeConnection`.
- Persistent `_id_` and secondary index durability across broker restarts.
- Histogram-backed and interval-count-aware `find` planning that ranks collection and index scans using index value frequencies, bounded interval counts, coverage, sort work, and a sequence-keyed per-query plan cache that persists across broker restart through checkpoint snapshots.
- Planner-backed `find` index scans for single-field and compound predicates, including compound-prefix equality/range plans, multi-interval `$in` and collapsed `$or` plans, sort-aware plans, and reverse scans over compatible indexes.
- Branch-union `OR` planning for non-collapsible disjunctions, allowing separate branch plans over different indexes before result union and outer sort/projection handling.
- Covered projection plans when the filter, sort, and projected fields can be satisfied from index keys alone, including explicit `null` versus missing-field distinctions recovered from persisted index-entry presence metadata.
- `explain` reports `IXSCAN` vs `COLLSCAN`, `planCacheUsed`, matched prefix depth, filter coverage, sort coverage, projection coverage, scan direction, single-interval bounds or multi-interval arrays, and keys/docs examined.

### Query semantics currently implemented
- Top-level trivial boolean predicates, top-level sample-rate predicates, bit-test predicates, equality, comparison, membership, negated-membership, `$all`, top-level `$comment`, `$not`, `$type`, regex matching, `$elemMatch`, top-level `$expr`, array-size, and modulus matching on dotted field paths via `$alwaysTrue`, `$alwaysFalse`, `$sampleRate`, `$bitsAllSet`, `$bitsAllClear`, `$bitsAnySet`, `$bitsAnyClear`, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$all`, `$comment`, `$not`, `$exists`, `$type`, `$regex`/`$options`, `$elemMatch`, `$expr`, `$size`, and `$mod`.
- Boolean query composition with `$and`, `$or`, and `$nor`.
- Basic projection.
- Compound-prefix index selection for equality prefixes, point-interval prefixes, multi-interval `$in` and collapsed `$or` branches, and range bounds.
- Sort-aware index planning for compatible `find` sorts, including reverse scans over descending key parts.
- Stats-backed index choice with a sequence-keyed plan cache when multiple candidate indexes are available, including persisted cache reuse after broker restart.
- Covered projection execution from index keys for compatible `find` projections, including covered `null` versus missing-field behavior from persisted index presence metadata.
- Replacement updates and modifier updates via `$set`, `$unset`, `$inc`.
- Pipeline-style updates using aggregation document transformation stages such as `$set`, `$addFields`, `$unset`, `$project`, `$replaceRoot`, and `$replaceWith` on the matched document stream.
- Aggregation expression operators `$literal`, `$const`, `$expr`, control flow via `$cond`/`$switch`, scoped variable operators `$let`/`$map`/`$filter`/`$reduce`, literal field access and mutation via `$getField`/`$setField`/`$unsetField`, metadata access via `$meta` for local `geoNearDistance`, `geoNearPoint`, `indexKey`, `recordId`, `sortKey`, `randVal`, and `textScore` slots when present, comparison operators `$cmp`/`$eq`/`$ne`/`$gt`/`$gte`/`$lt`/`$lte`/`$strcasecmp`, boolean composition via `$and`/`$or`/`$not`, boolean-array checks via `$allElementsTrue`/`$anyElementTrue`, membership via `$in`, arithmetic via `$add`/`$subtract`/`$multiply`/`$divide`/`$mod`, date construction, parsing, formatting, and arithmetic via `$dateFromString`/`$dateToString`/`$dateFromParts`/`$dateToParts`/`$dateAdd`/`$dateSubtract`/`$dateDiff`/`$dateTrunc`, accumulator-style expressions via `$sum`/`$avg`/`$min`/`$max`, statistical expressions via `$stdDevPop`/`$stdDevSamp`/`$percentile`/`$median` with `approximate`, `discrete`, and `continuous` percentile modes, array-selection `N` expressions via `$firstN`/`$lastN`/`$minN`/`$maxN`, date-part operators via `$year`/`$month`/`$dayOfMonth`/`$dayOfWeek`/`$dayOfYear`/`$hour`/`$minute`/`$second`/`$millisecond`/`$week`/`$isoDayOfWeek`/`$isoWeek`/`$isoWeekYear`, logarithmic and power functions via `$exp`/`$ln`/`$log`/`$log10`/`$pow`/`$sqrt`, trigonometric and angle-conversion operators via `$acos`/`$acosh`/`$asin`/`$asinh`/`$atan`/`$atan2`/`$atanh`/`$cos`/`$cosh`/`$sin`/`$sinh`/`$tan`/`$tanh`/`$degreesToRadians`/`$radiansToDegrees`, integer bitwise operators `$bitAnd`/`$bitOr`/`$bitXor`/`$bitNot`, numeric transforms via `$abs`/`$ceil`/`$floor`/`$round`/`$trunc`, binary and document size introspection via `$binarySize`/`$bsonSize`, type introspection via `$type`/`$isNumber`, conversion operators via `$convert`/`$toBool`/`$toDate`/`$toDecimal`/`$toDouble`/`$toInt`/`$toLong`/`$toObjectId`/`$toString`, utility operators via `$rand`/`$sortArray`/`$tsSecond`/`$tsIncrement`/`$zip`, ASCII case conversion via `$toLower`/`$toUpper`, string trimming via `$trim`/`$ltrim`/`$rtrim`, string length, position, substring, regex, split, and replacement via `$strLenBytes`/`$strLenCP`/`$indexOfBytes`/`$indexOfCP`/`$substr`/`$substrBytes`/`$substrCP`/`$regexFind`/`$regexFindAll`/`$regexMatch`/`$split`/`$replaceOne`/`$replaceAll`, string concatenation via `$concat`, null coalescing via `$ifNull`, set operators via `$setDifference`/`$setEquals`/`$setIntersection`/`$setIsSubset`/`$setUnion`, and array/document transforms via `$size`/`$isArray`/`$arrayElemAt`/`$first`/`$last`/`$concatArrays`/`$objectToArray`/`$arrayToObject`/`$mergeObjects` plus array-sequence helpers `$indexOfArray`/`$range`/`$reverseArray`/`$slice`.
- `$convert` currently covers the core scalar conversions plus `onError` and `onNull`. Feature-flagged or BinData-oriented forms such as `base`, `format`, `byteOrder`, and subtype-directed conversions remain explicitly unsupported.
- Explicit rejection of unsupported aggregation expression operators instead of silently treating single-key `$operator` documents as literal values.
- Mongo-like `$unwind` preserve semantics for missing, `null`, and empty-array inputs when `preserveNullAndEmptyArrays` is set.
- Aggregation stages:
  - `$documents`
  - `$facet`
  - `$bucket`
  - `$bucketAuto`
  - `$collStats`
  - `$currentOp`
  - `$densify`
  - `$fill`
  - `$geoNear`
  - `$graphLookup`
  - `$lookup`
  - `$indexStats`
  - `$listCatalog`
  - `$listClusterCatalog`
  - `$listCachedAndActiveUsers`
  - `$listLocalSessions`
  - `$listSampledQueries`
  - `$listSearchIndexes`
  - `$listSessions`
  - `$listMqlEntities`
  - `$planCacheStats`
  - `$sample`
  - `$sortByCount`
  - `$unionWith`
  - `$match`
  - `$merge`
  - `$querySettings`
  - `$out`
  - `$project`
  - `$set`
  - `$addFields`
  - `$unset`
  - `$limit`
  - `$skip`
  - `$sort`
  - `$count`
  - `$unwind`
  - `$group`
  - `$redact`
  - `$setWindowFields`
  - `$replaceRoot`
  - `$replaceWith`
- Same-file cross-namespace aggregation via `$unionWith` and `$lookup`, including collection-backed namespace resolution and collectionless `$documents` subpipelines for both stages.
- Same-file aggregation write stages via `$out`, including string targets and `{ db, coll }` targets within the same `.mongodb` file.
- Same-file aggregation write stages via `$merge`, including string targets or `{ db, coll }` targets, `on` fields, and the currently supported string mode combinations for `whenMatched` and `whenNotMatched`.
- Collectionless aggregation via `aggregate: 1` when the pipeline begins with `$documents` or `$changeStream`.
- Collectionless `$currentOp` aggregation with `localOps: true` on `admin`, including follow-on pipeline stages such as `$project` and `$match`.
- Collection-backed `$collStats` metadata aggregation as a first stage, currently supporting `count` and `storageStats` output against the local file-backed namespace.
- Collection-backed `$indexStats` metadata aggregation as a first stage, returning local index specs plus zeroed access counters for file-backed namespaces.
- Collectionless `$listCatalog` aggregation on `admin` and collection-scoped `$listCatalog` aggregation on existing namespaces, returning the local file-backed namespace catalog.
- Collectionless `$listClusterCatalog` aggregation on any database, returning local file-backed namespace metadata for that database or for the full file when run on `admin`, with `sharded: false`, optional `tracked: false`, and optional empty `shards` arrays because sharding is out of scope.
- `$listCachedAndActiveUsers` as an empty diagnostic source because authentication and user caching are out of scope for `mqlite`.
- Collectionless `$listLocalSessions` aggregation with the public `allUsers` and `users` filters, returning an empty result because `mqlite` does not implement logical sessions or a local session cache.
- Collectionless `$listSampledQueries` aggregation on `admin`, accepting the public optional `namespace` filter and returning an empty result because `mqlite` does not implement query sampling.
- Collection-backed `$listSearchIndexes` aggregation, accepting the public optional `name` or `id` filter and returning an empty result because `mqlite` does not implement a search-index subsystem.
- `$listSessions` aggregation on `config.system.sessions`, accepting the public `allUsers` and `users` filters and returning an empty result because `mqlite` does not implement persisted logical sessions.
- Collectionless `$querySettings` aggregation on `admin`, accepting the public `showDebugQueryShape` option and returning an empty result until `mqlite` grows a query-settings store.
- Collectionless `$listMqlEntities` aggregation on `admin` for `entityType: "aggregationStages"`, returning the sorted list of currently supported aggregation stage names.
- Collection-backed `$planCacheStats` metadata aggregation as a first stage, returning the local persisted per-namespace plan cache entries for `mqlite` query planning.
- `$densify` for numeric and date ranges, including `full`, `partition`, and explicit `[lower, upper]` bounds with partitioned local generation over the current document stream.
- `$fill` for constant-value, `locf`, and `linear` fills over the local document stream, including `sortBy`, `partitionByFields`, and `partitionBy` expression partitioning.
- `$geoNear` as a first-stage local geospatial scan over legacy coordinate pairs and GeoJSON points, including `query`, `minDistance`, `maxDistance`, `distanceMultiplier`, `distanceField`, `includeLocs`, `key`, and optional spherical distance calculation.
- `$graphLookup` for same-file recursive foreign-collection traversal, including `startWith`, `connectFromField`, `connectToField`, `maxDepth`, `depthField`, `restrictSearchWithMatch`, and outer-scope variable resolution inside nested pipelines.
- `$redact` aggregation with recursive `$$KEEP`, `$$PRUNE`, and `$$DESCEND` semantics over nested documents and arrays.
- `$setWindowFields` for local partitioned window execution with `partitionBy`, `sortBy`, document windows, single-sort-key range windows, supported accumulator window functions (`$sum`, `$avg`, `$first`, `$last`, `$push`, `$addToSet`, `$min`, `$max`, `$count`), ranking functions (`$documentNumber`, `$rank`, `$denseRank`), `$shift`, `$locf`, and `$linearFill`.
- `$changeStream` over a persisted local change-event log, including collection-scoped, database-scoped (`aggregate: 1`), and cluster-scoped (`admin` plus `allChangesForCluster: true`) streams, `resumeAfter`, `startAfter`, `startAtOperationTime`, `fullDocument`, `fullDocumentBeforeChange`, and `showExpandedEvents`. The current implementation returns the durable local history visible when the aggregate starts; it does not keep a live awaitData cursor open.
- `$changeStreamSplitLargeEvent` as the final stage in a `$changeStream` pipeline, splitting oversized local change events into deterministic fragments with fragment resume tokens and explicit fatal errors when a resumed pipeline no longer reproduces the referenced split event.
- Same-file `renameCollection` for local collection management, including cross-database renames within a single `.mongodb` file and optional `dropTarget` replacement.

## Unsupported Features

### Explicitly rejected by the current broker

These are already part of the tested failure surface:
- Logical sessions
- Retryable writes and retryable transaction envelopes
- Multi-document transactions
- `readConcern`
- `writeConcern`
- `$readPreference`
- Session and transaction envelopes remain rejected even though `killAllSessions` exists as a no-op admin compatibility command.
- Non-default `writeConcern` values remain rejected except for standalone-compatible `w: "majority"`, which is accepted as a no-op alongside the default acknowledged write concern.
- Unsupported commands
- Unsupported query operators
- Unsupported aggregation stages

### Out of scope for the mqlite architecture
- TCP listeners
- TLS
- Authentication and SASL
- Network compression
- Replica sets
- Sharding
- Load balancing

### Distributed consistency features
- Retryable reads

### Storage features
- Capped collections
- Tailable cursors
- Time-series collections
- TTL indexes
- Text indexes
- Geo indexes
- Hashed indexes
- Wildcard indexes
- Search and vector features
- Client-side field level encryption

### Language and server features
- MapReduce
- Server-side JavaScript, including `$where` and `$function`
- Replication internals
- Oplog mechanics
- Cluster administration commands

### Permanently out of scope
- `mqlite` will not embed a JavaScript engine. Server-side JavaScript surfaces such as `$where` and `$function` are permanently unsupported and are expected to fail explicitly with an unsupported-operator `BadValue` error.

## Test Strategy

Test coverage is a release gate:
- Unit tests cover BSON helpers, wire framing, query semantics, catalog rules, and storage primitives.
- Integration tests exercise broker behavior through real local IPC using `OP_MSG`.
- Storage tests cover WAL recovery, superblock rotation, slotted-page persistence, stable `RecordId` reopening, multi-page spill for records, B-tree indexes, and persisted change-event pages, truncated-tail handling, fallback to older checkpoints when newer record or index pages are corrupted, reopened compound descending index order checks, and persisted index-entry presence metadata for explicit `null` versus missing fields.
- Regression tests accompany each bug fix.
- CI runs on macOS, Linux, and Windows.
- Coverage reporting is wired into CI for the Linux job.
- Capability snapshot tests keep the checked-in MongoDB operator and stage catalog in sync with the current `mqlite` support surface, and direct parser/executor contract tests validate that supported query operators, aggregation stages, expression operators, accumulators, and window functions are accepted while unsupported ones are rejected predictably.
- Focused query and aggregation unit tests, inspired by MongoDB server matcher and pipeline tests, cover every currently supported query operator, aggregation stage, `$literal` expression operator, supported group accumulator, and supported `$setWindowFields` window operator.
- The current baseline includes explicit rejection tests for session and transaction envelopes, regression tests for unsupported query operators and aggregation stages, CLI tests that validate broker auto-spawn and restart recovery without any patched driver, direct CLI coverage for collection-scoped and collectionless `$changeStream` plus `$changeStreamSplitLargeEvent`, broker restart tests that prove unique indexes and persisted plan-cache entries survive checkpoint and reopen, and `explain` tests that verify plan-cache usage, persisted plan-cache reuse after restart, branch-union `OR`, compound-prefix, point-prefix, multi-interval `$or`/`$in`, range, cost-based, covered-projection, and null-vs-missing covered `find` plans return the expected `IXSCAN` or `OR` metadata and work counters.
- The Node driver workspace now also carries a dedicated `file://` harness (`test/mocha_mqlite.js` and `test/tools/runner/run_mqlite.cjs`) so existing `node-mongodb-native` integration suites can be replayed against mqlite without editing the suites themselves. The default suite list lives in `test/tools/runner/mqlite_suite_registry.ts`, so unwanted suites can be removed by commenting out lines there. `npm run check:mqlite` runs that broad registry, `npm run check:mqlite:crud` keeps a small green bring-up subset, and `npm run check:mqlite:operations` runs the larger operation-focused set covering CRUD, aggregation, indexing, commands, BSON options, and example-style operation suites. Extra Mocha flags can be forwarded after `--`, so `npm run check:mqlite -- --reporter min` still expands the mqlite registry instead of falling back to recursive test discovery.

## CLI

```text
mqlite <subcommand> [options]
mqlite --help
mqlite <subcommand> --help
```

`mqlite` is currently a subcommand-oriented CLI. Every command takes an explicit `--file` path instead of the SQLite-style positional database argument, and clap provides `-h, --help` on the root command and on each subcommand.

Top-level option:

| Option | Summary | Example |
| --- | --- | --- |
| `-h, --help` | Show the top-level command summary and list of subcommands. | `mqlite --help` |

### `serve`

Starts a broker for one `.mongodb` file, publishes a local IPC manifest, serves `OP_MSG` traffic, and exits after the broker has been idle for the configured timeout. If the target file does not exist yet, it is created.

Usage:

```text
mqlite serve --file <path> [--idle-shutdown-secs <seconds>]
```

Options:

| Option | Summary | Example |
| --- | --- | --- |
| `--file <path>` | Target `.mongodb` file for the broker; required. | `mqlite serve --file /tmp/app.mongodb` |
| `--idle-shutdown-secs <seconds>` | Idle timeout before the broker shuts down and checkpoints any pending WAL; defaults to `60`. | `mqlite serve --file /tmp/app.mongodb --idle-shutdown-secs 300` |
| `-h, --help` | Show command help. | `mqlite serve --help` |

### `command`

Auto-spawns or reuses the broker for the target file, sends one MongoDB command over a real `OP_MSG` request, prints the BSON reply as pretty JSON, and returns a non-zero exit status when the reply has `ok: 0`. If the target file does not exist yet, it is created through the spawned broker.

Usage:

```text
mqlite command --file <path> [--db <name>] [--eval <json>] [--idle-shutdown-secs <seconds>]
```

Options:

| Option | Summary | Example |
| --- | --- | --- |
| `--file <path>` | Target `.mongodb` file for broker discovery or auto-spawn; required. | `mqlite command --file /tmp/app.mongodb --eval '{"ping":1}'` |
| `--db <name>` | Default database to inject as `$db` when the payload does not already contain `$db`; defaults to `admin` when omitted. | `mqlite command --file /tmp/app.mongodb --db app --eval '{"ping":1}'` |
| `--eval <json>` | Inline JSON object to convert to BSON and send as the command body. When omitted, `mqlite command` reads the full JSON object from stdin instead. | `mqlite command --file /tmp/app.mongodb --db app --eval '{"create":"widgets"}'` |
| `--idle-shutdown-secs <seconds>` | Idle timeout to pass to an auto-spawned broker; defaults to `60`. This does not affect an already-running broker for the same file. | `mqlite command --file /tmp/app.mongodb --db app --idle-shutdown-secs 5 --eval '{"ping":1}'` |
| `-h, --help` | Show command help. | `mqlite command --help` |

Examples:

```text
mqlite command --file /tmp/example.mongodb --db app --eval '{"create":"widgets"}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"insert":"widgets","documents":[{"sku":"alpha","qty":2}]}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"find":"widgets","filter":{"sku":"alpha"}}'
printf '%s\n' '{"listCollections":1}' | mqlite command --file /tmp/example.mongodb --db app
```

### `checkpoint`

Opens or creates the target file, writes a new checkpoint, and then prints the resulting storage inspection report as pretty JSON.

Usage:

```text
mqlite checkpoint --file <path>
```

Options:

| Option | Summary | Example |
| --- | --- | --- |
| `--file <path>` | Target `.mongodb` file to checkpoint; required. | `mqlite checkpoint --file /tmp/app.mongodb` |
| `-h, --help` | Show command help. | `mqlite checkpoint --help` |

### `verify`

Loads an existing `.mongodb` file, validates the durable structure that can be checked on open, and prints a JSON verification report.

Usage:

```text
mqlite verify --file <path>
```

Options:

| Option | Summary | Example |
| --- | --- | --- |
| `--file <path>` | Existing `.mongodb` file to verify; required. | `mqlite verify --file /tmp/app.mongodb` |
| `-h, --help` | Show command help. | `mqlite verify --help` |

### `inspect`

Loads an existing `.mongodb` file and prints a JSON report with checkpoint metadata, WAL counters, file size, page counts, and discovered database names.

Usage:

```text
mqlite inspect --file <path>
```

Options:

| Option | Summary | Example |
| --- | --- | --- |
| `--file <path>` | Existing `.mongodb` file to inspect; required. | `mqlite inspect --file /tmp/app.mongodb` |
| `-h, --help` | Show command help. | `mqlite inspect --help` |

## Direct Validation

The broker can be exercised directly from the CLI without adapting a driver first. `mqlite command` is the main direct validation path: it auto-spawns or reuses the broker for the target file, sends a real `OP_MSG` request over local IPC, and prints the BSON reply as JSON.

## Compared with `sqlite3`

The current CLI is intentionally explicit, but it does not yet feel as lightweight as the standard `sqlite3` shell.

| Area | `mqlite` today | `sqlite3`-style expectation | Possible improvement |
| --- | --- | --- | --- |
| Entry point | Requires a subcommand such as `command` or `serve`. | Common usage starts with the database path itself. | Accept `mqlite <path>` as a shorthand for opening the file in a default shell or one-shot mode. |
| File selection | Uses `--file <path>` everywhere. | Uses a positional database argument. | Add a positional file argument while keeping `--file` as an explicit alias. |
| Query input | Uses JSON command documents and MongoDB command names. | Uses SQL statements and shell dot-commands. | Add an interactive shell with history, multi-line input, and helper meta-commands for common admin tasks. |
| Database selection | One file may contain multiple MongoDB databases, so commands may need `--db` or `$db`. | One file normally feels like one database namespace. | Make one user database the default shell context and let `use <db>` or a shell flag switch context explicitly. |
| Process model | `command` may auto-spawn a background broker and wait for an idle shutdown. | The CLI usually opens the file directly and exits when the command finishes. | Hide broker lifecycle behind a shell/one-shot UX so local use feels direct even though the broker remains the implementation detail. |
| Maintenance commands | `inspect`, `verify`, and `checkpoint` are separate top-level verbs. | Many maintenance tasks are discoverable inside the shell. | Add shell commands analogous to `.tables`, `.schema`, or integrity-check helpers, with the current top-level verbs kept for scripting. |

If the goal is to make `mqlite` feel closer to `sqlite3` without losing the MongoDB-compatible transport model, the most impactful changes would be:
- Add a positional file argument and a top-level one-shot execution mode such as `mqlite /tmp/app.mongodb --db app --eval '{"ping":1}'`.
- Add an interactive shell that keeps a current file and current database context so repeated `--file` and `--db` flags are unnecessary.
- Introduce short, discoverable shell helpers for inspection and verification so local workflows do not have to remember multiple top-level maintenance verbs.

## Notes

This baseline intentionally favors a stable executable slice over speculative completeness. The current file format now implements fixed metadata, rotating superblocks, WAL-backed mutation durability, slotted record pages, persisted B-tree index pages, stable `RecordId`s, persisted plan-cache snapshots, stats-backed and plan-cached compound `find` planning, branch-union `OR` planning, covered null-vs-missing index execution, and replay on open. The next storage steps are page reuse/compaction and more incremental page-splitting and reuse policies so runtime tree maintenance does not need to reconstruct from the persisted entry set.
