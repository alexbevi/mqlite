# AGENTS.md

## Repo Rules
- Keep `README.md` and `DRIVER.md` in sync with code. Any supported or unsupported behavior change must update the docs in the same patch.
- Keep tests in the same patch as code. A feature is not complete until parser, execution, recovery, and rejection behavior are covered where applicable.
- Treat `mongo`, `node-mongodb-native`, and `specifications` in the workspace as reference material only. `mqlite` remains an independent Rust implementation.

## Architecture Decisions
- `mqlite` is a broker-per-file MongoDB-compatible local engine that communicates via `OP_MSG` over local IPC only.
- One `.mongodb` file is the durable store of record. Sidecars such as manifests are ephemeral and may be recreated.
- The durable file uses a fixed header, two rotating superblocks, checkpoint snapshots, fixed-size slotted record pages with stable `RecordId`s, persisted B-tree index pages with internal and leaf nodes keyed by BSON plus `RecordId`, and an append-only WAL for typed collection mutations.
- The compatibility target is a MongoDB Stable API v1 subset plus the minimum bootstrap/admin commands needed by drivers.
- Unsupported distributed/server features must fail explicitly and must have regression coverage.
- `mqlite command` is the default direct validation path before any driver patching work.
- Reopen must validate persisted index pages against collection pages; do not silently rebuild index state from collection snapshots during load.
- Indexed `find` planning should be directly observable through `explain`, not just inferred from behavior.

## Upstream Reference Anchors
- Server generic command fields and unsupported envelope behavior are keyed off `../mongo/src/mongo/idl/generic_argument.idl`.
- Handshake reply shape is anchored to `../mongo/src/mongo/db/repl/hello/hello.idl`.
- Command field and reply shapes are anchored to:
  - `../mongo/src/mongo/db/query/find_command.idl`
  - `../mongo/src/mongo/db/pipeline/aggregate_command.idl`
  - `../mongo/src/mongo/db/query/write_ops/write_ops.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/list_collections.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/list_indexes.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/create_indexes.idl`
  - `../mongo/src/mongo/db/shard_role/ddl/drop_indexes.idl`
  - `../mongo/src/mongo/util/buildinfo.idl`
- Node driver integration design is anchored to:
  - `../node-mongodb-native/src/connection_string.ts`
  - `../node-mongodb-native/src/cmap/connect.ts`
  - `../node-mongodb-native/src/client-side-encryption/mongocryptd_manager.ts`
- Driver-side protocol expectations are anchored to:
  - `../specifications/source/mongodb-handshake/handshake.md`
  - `../specifications/source/connection-string/connection-string-spec.md`
  - `../specifications/source/compression/OP_COMPRESSED.md`

## Test Discipline
- Add unit tests for pure logic and encoding.
- Add integration tests for broker behavior over real IPC and `OP_MSG`.
- Add storage recovery and page-format tests whenever the file format or mutation log changes, including persisted index-page checks.
- Add planner tests and `explain` coverage whenever index scan selection changes.
- Add regression tests for every bug fix.
- Preserve cross-platform behavior by keeping CI green on macOS, Linux, and Windows.
