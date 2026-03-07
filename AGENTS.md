# AGENTS.md

## Repo Rules
- Keep `README.md` and `DRIVER.md` in sync with code. Any supported or unsupported behavior change must update the docs in the same patch.
- Keep `ARCHITECTURE.md` in sync with storage, indexing, planner, and command-execution changes in the same patch.
- Keep the generated capability snapshots under `capabilities/` in sync with query and aggregation support changes. Run `cargo run -p mqlite-capabilities -- sync` whenever the supported or unsupported operator and stage surface changes.
- Keep tests in the same patch as code. A feature is not complete until parser, execution, recovery, and rejection behavior are covered where applicable.
- Treat `mongo`, `node-mongodb-native`, and `specifications` in the workspace as reference material only. `mqlite` remains an independent Rust implementation.

## Architecture Decisions
- `mqlite` is a broker-per-file MongoDB-compatible local engine that communicates via `OP_MSG` over local IPC only.
- One `.mongodb` file is the durable store of record. Sidecars such as manifests are ephemeral and may be recreated.
- The durable file uses a fixed header, two rotating superblocks, checkpoint snapshots, fixed-size slotted record pages with stable `RecordId`s, persisted B-tree index pages with internal and leaf nodes keyed by BSON plus `RecordId`, and an append-only WAL for typed collection mutations.
- The durable file also persists a local change-event log in slotted pages, and WAL mutations must append collection changes and change-stream events atomically.
- The compatibility target is a MongoDB Stable API v1 subset plus the minimum bootstrap/admin commands needed by drivers.
- Unsupported distributed/server features must fail explicitly and must have regression coverage.
- Server-side JavaScript is permanently out of scope. `$where` and `$function` must remain explicit unsupported-operator failures.
- `mqlite command` is the default direct validation path before any driver patching work.
- Reopen must validate persisted index pages against collection pages; do not silently rebuild index state from collection snapshots during load.
- Indexed `find` planning should be directly observable through `explain`, not just inferred from behavior.
- Compound-prefix and sort-aware index plans must match persisted index key order, including descending key parts, after checkpoint and reopen.
- Planner selection should use measurable work such as keys examined, docs examined, sort work, and coverage, not fixed heuristic scores alone.
- `find` planning should prefer histogram-style index statistics, bounded interval estimates, and a sequence-keyed plan cache before falling back to exact execution work.
- Covered index execution must preserve explicit `null` versus missing-field semantics using persisted index-entry presence metadata, not by silently fetching documents.
- General disjunctions should prefer branch-union `OR` planning over collection scan when branch-local plans on distinct indexes are cheaper, even when the disjunction cannot collapse to one shared interval scan.
- Plan-cache state should persist in checkpoint snapshots so compatible `find` shapes can reuse cached choices after broker restart.
- Cross-namespace aggregation stages such as `$unionWith` and `$lookup` must resolve foreign namespaces from the same broker-owned `.mongodb` file, including collectionless `$documents` subpipelines where supported.
- `$changeStream` is backed by the persisted local change-event log and currently exposes the durable history visible when the aggregate starts; it is not a live awaitData stream yet.
- `$setWindowFields` support is a local in-memory execution path and must keep its supported window-function list explicit; unsupported window functions should fail during stage parsing instead of silently degrading.

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
- Add capability snapshot and gap-analysis tests whenever query or aggregation support changes, and ensure the checked-in capability report still matches the live support surface.
- Add storage recovery and page-format tests whenever the file format or mutation log changes, including persisted index-page checks.
- Add planner tests and `explain` coverage whenever index scan selection changes.
- Add storage-level regression tests whenever index ordering semantics change so descending or compound key behavior is validated after reopen, not only in memory.
- Add planner regressions for cost-based ranking, plan-cache usage, covered projections, null-vs-missing covered execution, and point- or multi-interval prefix handling whenever `find` planning changes.
- Add restart-time regressions whenever persisted plan-cache encoding or planner choice shapes change.
- Add regression tests for every bug fix.
- Preserve cross-platform behavior by keeping CI green on macOS, Linux, and Windows.
