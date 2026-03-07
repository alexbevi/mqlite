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
  - `writeConcern`
  - read preference other than primary/direct single
  - `retryReads`
  - `retryWrites`
  - session and transaction enabling knobs when exposed directly
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
- Add a dedicated mqlite runner instead of editing existing integration suites in place. The current Node workspace uses `test/mocha_mqlite.js` plus `test/tools/runner/run_mqlite.cjs` so the same tests can be pointed at `file:///...` with profile-based selection.

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

## Driver Test Checklist

Any driver integration should include:
- URI parsing success and rejection tests.
- Broker auto-spawn tests.
- Broker reuse across multiple clients for the same file.
- Idle shutdown and reconnect behavior.
- `hello` handshake compatibility.
- CRUD smoke tests over the local stream.
- Broker restart tests after index creation so unique-index durability is exercised through reopen.
- `explain` smoke tests so plan-cache usage, persisted plan-cache reuse after restart, branch-union `OR`, compound-prefix, point-prefix, multi-interval `$or`/`$in`, range, cost-based, covered-projection, and null-vs-missing covered `IXSCAN` selection can be validated over the file-backed broker, including `planCacheUsed`, `keysExamined`, and `docsExamined`.
- Command monitoring verification.
- Explicit failure tests for unsupported options.

For `node-mongodb-native`, the current direct driver-validation commands are:

```text
MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite
MQLITE_BINARY=../mqlite/target/debug/mqlite npm run check:mqlite:crud
```

- `check:mqlite` points the integration harness at a `file://` database and runs the normal integration tree under the dedicated mqlite hook.
- `check:mqlite:crud` uses the same harness but restricts execution to the current curated CRUD bring-up profile, which starts with `test/integration/crud/abstract_operation.test.ts` and can grow without modifying the underlying tests.
