# DRIVER.md

## Goal

Adapt MongoDB drivers to support:

```text
file:///absolute/path/to/database.mongodb?db=app
```

The driver still speaks `OP_MSG` exclusively. The only difference is that the remote socket becomes a local IPC stream backed by a per-file `mqlite` broker.

## Required Driver Behavior

### URI parsing
- Intercept `file://` before normal MongoDB URI handling.
- Treat the URI path as the database file path.
- Treat `db=<name>` as the default database selector.
- Canonicalize the path before broker discovery or spawn.

### Option policy
- Reject the following on `file://` URIs:
  - auth options
  - TLS options
  - network compression
  - proxy options
  - `replicaSet`
  - `loadBalanced`
  - `readConcern`
  - read preference other than primary/direct single
  - `retryReads`
  - `retryWrites`
  - session and transaction enabling knobs when exposed directly
- Treat the default acknowledged write concern as a no-op compatibility case:
  - allow `w=1`
  - allow `journal=false` / `j=false`
  - allow `wtimeout=0` / `wtimeoutMS=0`
  - reject any non-default `writeConcern`
- Preserve local client-side options such as:
  - `appName`
  - pool sizing
  - timeout values
  - monitoring toggles
  - Node-only broker spawn controls such as `mqliteSpawnPath`, `mqliteSpawnTimeoutMS`, and `mqliteIdleShutdownSecs`

### Broker lifecycle
- Compute the manifest path adjacent to the target `.mongodb` file.
- If the manifest exists and the broker is alive, attach to the advertised endpoint.
- If no live broker exists, spawn `mqlite serve --file <path>`.
- Re-read the manifest and connect once the endpoint is ready.
- Brokers are shared per file and may shut down after an idle timeout.

### Transport
- On POSIX, connect with a Unix domain socket.
- On Windows, connect with a named pipe.
- The transport must present a duplex byte stream to the existing command path.

### Handshake expectations
- Use `hello` over `OP_MSG`.
- Expect a direct standalone-style reply:
  - `ok: 1`
  - `helloOk: true`
  - `isWritablePrimary: true`
  - modern `maxWireVersion`
  - no `logicalSessionTimeoutMinutes`
  - no replica set metadata
  - no auth or compression negotiation
- Expect minimal admin compatibility helpers to exist for test harness setup:
  - `killAllSessions` succeeds as a no-op cleanup command
  - `getParameter` returns at least `authenticationMechanisms` and `requireApiVersion`
- `db.aggregate([{ $currentOp: { localOps: true } }])` on `admin` returns the in-flight aggregate command and can be followed by normal pipeline stages such as `$project`
- `db.collection.aggregate([{ $collStats: { count: {}, storageStats: {} } }])` returns local namespace metadata for the file-backed collection
- `db.collection.aggregate([{ $indexStats: {} }])` returns local index metadata for the file-backed collection
- `db.admin.aggregate([{ $listCatalog: {} }])` returns the local file-backed namespace catalog
- `db.collection.aggregate([{ $listCachedAndActiveUsers: {} }])` returns an empty result because `mqlite` has no auth user cache
- `db.admin.aggregate([{ $listMqlEntities: { entityType: "aggregationStages" } }])` returns the sorted list of currently supported aggregation stages
- `db.collection.aggregate([{ $planCacheStats: {} }])` returns local persisted plan-cache metadata for the file-backed collection
- `listCollections` on a missing database returns an empty cursor so driver cleanup/setup paths do not fail on fresh files
- Update operations accept aggregation pipeline updates over the matched document stream
  - `dropDatabase` is supported for local test setup and teardown
  - `renameCollection` is supported for local collection management, including `dropTarget`
  - `writeConcern: { w: "majority" }` is accepted as a standalone-style no-op compatibility case
  - wrapped CRUD `explain(...)` calls for `delete`, `update`, `distinct`, and `findAndModify` return a `queryPlanner` response
  - collection-backed `aggregate(...).explain()` returns a `$cursor.queryPlanner` stage for non-`$out` pipelines
  - collection-backed aggregation accepts a trailing `$out` stage and returns an empty cursor
  - session and transaction envelopes are still rejected explicitly

## Node Driver Adaptation Notes

`node-mongodb-native` already has most of the required seams:
- URI parsing lives in `src/connection_string.ts`.
- Initial handshake flows through `src/cmap/connect.ts`.
- Socket creation is centralized in `makeSocket`.
- Spawn-and-connect behavior already exists in the `mongocryptd` helper path and can be mirrored for `mqlite`.

Recommended Node changes for the first adapter:
- Add a `file://` parser that returns a local-broker topology configuration.
- Add a local stream factory for Unix sockets or named pipes.
- Skip incompatible SDAM features by forcing single-topology semantics.
- Reuse existing command monitoring and pooling behavior after the local stream is established.
- Add a dedicated mqlite runner instead of editing existing integration suites in place. The current Node workspace uses `test/mocha_mqlite.js` plus `test/tools/runner/run_mqlite.cjs` so the same tests can be pointed at `file:///...` with profile-based selection, and `test/tools/runner/mqlite_suite_registry.ts` is the checked-in allowlist you comment out to remove suites from the default run.

## Reference Anchors

The current adapter design was checked against:
- `../node-mongodb-native/src/connection_string.ts`
- `../node-mongodb-native/src/cmap/connect.ts`
- `../node-mongodb-native/src/client-side-encryption/mongocryptd_manager.ts`
- `../specifications/source/mongodb-handshake/handshake.md`
- `../specifications/source/connection-string/connection-string-spec.md`
- `../specifications/source/compression/OP_COMPRESSED.md`

The current supported and unsupported query and aggregation surface is tracked in:
- `capabilities/mongodb/upstream-capabilities.generated.json`
- `capabilities/mqlite/gap-analysis.generated.json`
- `capabilities/mqlite/gap-analysis.generated.md`

Driver bring-up should use those reports as the source of truth for which command, query, and aggregation tests are expected to pass today versus fail explicitly.
`mqlite` also supports collectionless `aggregate: 1` commands when the pipeline begins with `$documents` or `$currentOp`, which is the direct validation path for collectionless aggregation before any driver-specific adapter work.
Cross-namespace aggregation stages operate only within the same local `.mongodb` file; there is no network federation, so stages such as `$unionWith` and `$lookup` resolve foreign namespaces from the same broker-owned file, and write stages such as `$out` and `$merge` only target namespaces in that same file.
Server-side JavaScript is permanently out of scope for `mqlite`, so `$where` and `$function` should remain explicit unsupported-operator failures rather than compatibility gaps to close later.

## Driver Test Checklist

Any driver integration should include:
- URI parsing success and rejection tests.
- Broker auto-spawn tests.
- Broker reuse across multiple clients for the same file.
- Idle shutdown and reconnect behavior.
- `hello` handshake compatibility.
- CRUD smoke tests over the local stream.
- Query and aggregation smoke coverage for every currently supported operator and stage listed in `capabilities/mqlite/gap-analysis.generated.md`, rather than only broad happy-path CRUD.
- Broker restart tests after index creation so unique-index durability is exercised through reopen.
- `explain` smoke tests so plan-cache usage, persisted plan-cache reuse after restart, branch-union `OR`, compound-prefix, point-prefix, multi-interval `$or`/`$in`, range, cost-based, covered-projection, and null-vs-missing covered `IXSCAN` selection can be validated over the file-backed broker, including `planCacheUsed`, `keysExamined`, and `docsExamined`.
- Command monitoring verification.
- Explicit failure tests for unsupported options.
- Explicit failure tests for permanently unsupported server-side JavaScript surfaces such as `$where` and `$function`.

For `node-mongodb-native`, the current direct driver-validation commands are:

```text
MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite
MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite:crud
MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite:operations
```

- `check:mqlite` points the integration harness at a `file://` database and runs the explicit suite list from `test/tools/runner/mqlite_suite_registry.ts`.
- `check:mqlite:crud` uses the same harness but restricts execution to the current curated CRUD bring-up profile in that same registry, which starts with `test/integration/crud/abstract_operation.test.ts` and can grow without modifying the underlying tests.
- `check:mqlite:operations` runs the broader operation-focused registry in that same file, including CRUD, aggregation, indexing, run-command, read/write concern, command monitoring, BSON option, and operation-example suites so coverage against the upstream integration tree can be measured quickly.
- Extra Mocha flags can be forwarded after `--` without disabling profile expansion, so high-level summaries like `MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite -- --reporter min` still use the registry instead of falling back to recursive test discovery.
