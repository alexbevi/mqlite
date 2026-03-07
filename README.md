# mqlite

`mqlite` is an in-process-style MongoDB storage and query engine exposed through a local broker binary. Drivers connect through a `file://` URI, discover or spawn the local broker for a specific `.mongodb` file, and then communicate with the broker using MongoDB `OP_MSG` messages over local IPC instead of TCP.

## Current Scope

The repository now contains a working Rust workspace baseline with:
- A cross-crate architecture aligned to the long-term plan.
- A single-file durable store with a fixed header, dual superblocks, append-only WAL, and checkpoint snapshots.
- Local IPC manifest and endpoint generation.
- `OP_MSG` encoding and decoding.
- A broker with core command handling and cursor support.
- A direct CLI command path for broker validation without any patched driver.
- Progressive tests, CI scaffolding, and living specs.

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

## URI Model

Use an absolute file URI:

```text
file:///absolute/path/to/database.mongodb?db=app
```

- The URI path is always the filesystem path to the database file.
- The optional `db` query parameter selects the default database.
- Drivers must reject incompatible network-style options for `file://`.

## Supported Features

### Broker and protocol
- `OP_MSG` request-response transport over local IPC.
- Unix domain sockets on POSIX.
- Named pipe endpoint naming on Windows.
- Ephemeral manifest discovery per database file.
- `hello`, `ping`, and `buildInfo` bootstrap commands.

### Storage and catalog
- One durable `.mongodb` file per broker.
- Fixed file header and rotating superblocks.
- Append-only WAL for typed collection mutations.
- Recovery by replaying WAL on top of the latest valid checkpoint snapshot.
- Multiple databases and collections in one file.
- Collection catalog metadata and index metadata.

### Query and command surface
- `listDatabases`
- `listCollections`
- `listIndexes`
- `create`
- `drop`
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

### Query semantics currently implemented
- Equality and comparison matching on dotted field paths.
- Boolean query composition with `$and` and `$or`.
- Basic projection.
- Replacement updates and modifier updates via `$set`, `$unset`, `$inc`.
- Aggregation stages:
  - `$match`
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
  - `$replaceRoot`

## Unsupported Features

### Explicitly rejected by the current broker

These are already part of the tested failure surface:
- Logical sessions
- Retryable writes and retryable transaction envelopes
- Multi-document transactions
- `readConcern`
- `writeConcern`
- `$readPreference`
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
- Change streams

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
- Server-side JavaScript
- Replication internals
- Oplog mechanics
- Cluster administration commands

## Test Strategy

Test coverage is a release gate:
- Unit tests cover BSON helpers, wire framing, query semantics, catalog rules, and storage primitives.
- Integration tests exercise broker behavior through real local IPC using `OP_MSG`.
- Storage tests cover WAL recovery, superblock rotation, and truncated-tail handling.
- Regression tests accompany each bug fix.
- CI runs on macOS, Linux, and Windows.
- Coverage reporting is wired into CI for the Linux job.
- The current baseline includes explicit rejection tests for session and transaction envelopes, regression tests for unsupported query operators and aggregation stages, and CLI tests that validate broker auto-spawn and restart recovery without any patched driver.

## CLI

```text
mqlite serve --file /path/to/data.mongodb
mqlite command --file /path/to/data.mongodb --db app --eval '{"ping":1}'
mqlite checkpoint --file /path/to/data.mongodb
mqlite verify --file /path/to/data.mongodb
mqlite inspect --file /path/to/data.mongodb
```

## Direct Validation

The broker can now be exercised directly from the CLI without adapting a driver first. `mqlite command` auto-spawns or reuses the broker for the target file, sends a real `OP_MSG` request over local IPC, and prints the BSON reply as JSON.

Example:

```text
mqlite command --file /tmp/example.mongodb --db app --eval '{"create":"widgets"}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"insert":"widgets","documents":[{"sku":"alpha","qty":2}]}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"find":"widgets","filter":{"sku":"alpha"}}'
```

## Notes

This baseline intentionally favors a stable executable slice over speculative completeness. The current file format now implements fixed metadata, rotating superblocks, WAL-backed mutation durability, checkpoint snapshots, and replay on open. The next storage steps are compaction, finer-grained record/index structures, and planner execution beyond the current in-memory catalog model.
