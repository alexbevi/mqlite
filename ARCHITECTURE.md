# ARCHITECTURE.md

## Overview

`mqlite` is a broker-per-file MongoDB-compatible local engine.

- One broker process owns one `.mongodb` file at a time.
- Clients talk to the broker with MongoDB `OP_MSG` over local IPC only.
- The `.mongodb` file is the durable system of record.
- Sidecars such as manifests and socket or pipe endpoints are ephemeral.

The current workspace is split into focused crates:

- `mqlite-bson`: BSON ordering, dotted-path helpers, and `_id` generation support.
- `mqlite-wire`: `OP_MSG` framing and parsing.
- `mqlite-ipc`: manifest discovery plus Unix socket or Windows named-pipe transport.
- `mqlite-catalog`: databases, collections, records, and in-memory index metadata.
- `mqlite-storage`: file format, recovery, checkpoints, pages, and persisted planner cache state.
- `mqlite-query`: filter parsing, projection, updates, and aggregation semantics.
- `mqlite-exec`: cursor batching and cursor lifecycle.
- `mqlite-server`: command dispatch, planning, execution, and broker lifecycle.
- `mqlite`: CLI entrypoints such as `serve`, `command`, `inspect`, `verify`, and `checkpoint`.

## Source Layout

The Rust workspace now follows a smaller-crate-root layout consistent with the Rust Book's
guidance on packages, crates, and modules:

- `src/lib.rs` is kept intentionally small and primarily declares modules plus the public re-exports
  that define the crate surface.
- Large implementation files were moved behind semantically named modules so a reader can discover
  the crate API before reading implementation details.
- Domain-heavy crates expose the concepts Rust developers would expect first:
  - `mqlite-query` separates capabilities, error types, expression evaluation, filter parsing,
    projection handling, updates, and pipeline execution.
  - `mqlite-server` exposes the broker from a small crate root and keeps broker command handling in
    `broker.rs`.
  - `mqlite-storage`, `mqlite-catalog`, and `mqlite-capabilities` re-export from named module files
    instead of placing their entire implementation directly in `lib.rs`.
- Tests remain in-crate for internal behavior-heavy crates, but the public crate root is no longer
  mixed with implementation and test code.

This keeps the module tree readable for first-time contributors while preserving the existing
behavior and test surface.

## Broker Model

The broker is the only writer for a database file.

- Reads are served from in-process state loaded from the file plus any applied WAL mutations.
- Writes append WAL records, update in-memory state, and become durable before command success.
- Idle shutdown triggers a checkpoint so the current catalog, pages, and plan-cache state are written back into the main file.
- Drivers and the direct CLI both discover or spawn the broker through the same manifest flow.

## Durable File Layout

The file is versioned and self-describing.

- Fixed header:
  - magic and file format version
  - reserved metadata region
- Dual rotating superblocks:
  - generation number
  - last applied sequence
  - checkpoint time
  - snapshot offset and length
  - WAL start offset
  - snapshot checksum
- Data region:
  - slotted record pages
  - slotted index leaf and internal pages
  - BSON snapshot metadata
  - append-only WAL frames after the active snapshot

The current format version is encoded in the header and checkpoint snapshot. Recovery rejects unsupported versions.

## Snapshot Contents

Each checkpoint snapshot stores the minimum metadata needed to reopen the durable state without rebuilding it from user documents.

- Database and collection catalog structure
- Collection options
- Record page references per collection
- Index page references, root page ids, key patterns, and uniqueness flags
- Persisted plan-cache entries keyed by namespace and query shape

The snapshot does not inline all records and index entries directly. Those live in fixed-size pages referenced by the snapshot metadata.

## Record Storage

Collections are stored as slotted pages of BSON documents.

- Each record has a stable `RecordId`.
- Slotted pages keep compact page-local metadata plus BSON payload offsets.
- Checkpoints write record pages first, then reference them from the snapshot.
- Reopen reconstructs `CollectionCatalog.records` from those pages and preserves `RecordId` stability.

Current behavior:

- Inserts allocate the next `RecordId`.
- Updates preserve `RecordId`.
- Deletes remove records and corresponding index entries.
- Recovery replays WAL mutations on top of the newest valid checkpoint.

## Index Storage

Indexes are persisted separately from records and are not rebuilt from collection scans during load.

- Each index stores ordered `IndexEntry` values:
  - indexed BSON key document
  - `RecordId`
  - `present_fields` metadata to distinguish explicit `null` from missing paths
- Checkpoints encode indexes into persisted B-tree pages:
  - leaf pages hold serialized `IndexEntry` payloads
  - internal pages hold separator keys and child page references
- Each persisted index records its root page id plus the full page reference set.
- Reopen traverses the B-tree from the persisted root and reconstructs ordered entries.
- Reopen validates index entries against collection pages and fails instead of silently rebuilding mismatched state.

Current index capabilities:

- `_id_` is always present
- single-field and compound B-tree indexes
- unique index enforcement
- ascending and descending key parts
- stable BSON ordering plus `RecordId` tie-breaking
- persisted missing-vs-null metadata

## Aggregation Execution

Aggregation execution lives in `mqlite-query` and is intentionally split between pure document
transforms and broker-backed collection resolution.

- `run_pipeline()` executes stages against an in-memory document stream only.
- `run_pipeline_with_resolver()` adds a `CollectionResolver` so stages can read sibling namespaces
  from the same broker-owned file.
- `PipelineContext` carries:
  - the active database
  - whether execution is inside `$facet`
  - the collection resolver
  - expression variables for correlated subpipelines

Current cross-namespace aggregation behavior:

- `$unionWith` resolves a foreign collection from the same `.mongodb` file or runs a collectionless
  subpipeline that starts with `$documents`.
- `$lookup` resolves a foreign collection from the same `.mongodb` file and supports:
  - `localField` and `foreignField` equality joins
  - optional `pipeline` filters or reshaping on the joined documents
  - `let` variables for correlated subpipelines
  - collectionless `$documents` subpipelines when `from` is omitted
- Nested lookup-style subpipelines inherit outer variables by value so correlated `$expr` filters
  continue to work in nested stages.
- `$out` is a broker-backed terminal write stage that replaces a same-file target namespace and
  returns an empty cursor result to the client.
- `$merge` is a broker-backed terminal write stage that merges pipeline results into a same-file
  target namespace using supported `whenMatched` and `whenNotMatched` string modes plus optional
  `on` fields.
- The current implementation does not federate across files or processes.

## WAL And Recovery

Mutations are durable through an append-only WAL.

- Every logical collection replacement or drop is written as a typed WAL frame.
- WAL frames include a sequence number and checksum.
- The broker applies the mutation to in-memory state only after the WAL append succeeds.
- Recovery loads the newest valid checkpoint, then replays WAL frames with sequence numbers greater than the checkpoint sequence.
- Truncated WAL tails are detected and ignored when the preceding frames are valid.
- If the newest checkpoint is corrupt, recovery can fall back to the older superblock and continue from there.

## Planner And Persisted Plan Cache

`find` planning is intentionally local but no longer purely heuristic.

- Candidate ranking uses:
  - histogram-style value frequencies derived from persisted index entries
  - interval cardinality estimates from index bounds
  - sort work
  - projection coverage
  - expected document fetches
- A sequence-keyed plan cache stores the last winning choice for each query shape:
  - namespace
  - filter shape
  - sort shape
  - projection shape
- The broker keeps this cache in memory during execution.
- Idle checkpoint persists the cache into the snapshot so it survives broker restart.
- Cache reuse is valid only when the collection state sequence matches.

Current cached choices can represent:

- collection scan
- single index scan
- branch-union `OR` plans with one cached choice per branch

## Query Planning

There are currently three major `find` planning modes.

### 1. Collection Scan

Fallback when no useful index plan exists.

- Scans records in collection order
- Applies filter against full documents
- Outer sort and projection are applied later if needed

### 2. Single Index Plan

Used when one index can serve the whole query shape.

- Supports compound-prefix equality and range planning
- Supports reverse scans for compatible descending sort patterns
- Supports multi-interval scans for:
  - `$in`
  - collapsed `$or` cases that share one indexed shape
- Supports covered filtering and projection when the index contains the required paths
- Uses `present_fields` to keep explicit `null` distinct from missing values during covered reads

### 3. Branch-Union `OR` Plan

Used for non-collapsible disjunctions.

- The filter is expanded into DNF-style branches with a branch-count safety cap.
- Each branch is planned independently.
- Branch plans may choose different indexes or fall back to collection scan.
- Result `RecordId`s are unioned and deduplicated across branches.
- The outer query applies final sort and projection semantics.

This is the mechanism that handles broader `OR` shapes that cannot be represented as one shared interval scan.

## Command Execution Flow

The broker command path is:

1. Client or CLI opens a local IPC stream.
2. The broker reads an `OP_MSG`.
3. `mqlite-wire` materializes the body and document sequences.
4. `mqlite-server` rejects unsupported envelope fields such as sessions or read concern.
5. The command dispatcher selects a handler by command name.
6. Handlers interact with the current `DatabaseFile` state:
   - reads use the loaded catalog and planner
   - writes update collection state and commit a WAL mutation
7. `find` and `explain` use the planner:
   - parse filter
   - analyze bounds and projection dependencies
   - consult the sequence-keyed plan cache
   - choose a collection, index, or branch-union `OR` plan
   - execute the plan and return documents or explain metadata
8. `aggregate` uses the pipeline runner:
   - parse each stage into the supported Rust-native semantics
   - synthesize collectionless metadata-stage input for supported first stages such as `$currentOp`
   - execute pure document stages in memory
   - resolve same-file foreign namespaces for `$unionWith` and `$lookup`
   - thread `$lookup` `let` variables through correlated subpipelines
9. Cursor-producing commands hand results to `mqlite-exec`.
10. The broker writes the reply back as `OP_MSG`.

## Explain Surface

`explain` is the primary way to observe planner behavior.

The current `winningPlan` output can report:

- `COLLSCAN`
- `IXSCAN`
- `OR`

And may include:

- `planCacheUsed`
- `keysExamined`
- `docsExamined`
- `requiresSort`
- `filterCovered`
- `projectionCovered`
- `sortCovered`
- `scanDirection`
- single-interval bounds
- multi-interval arrays
- `inputStages` for branch-union `OR` plans

## Unsupported Architectural Areas

These remain intentionally out of scope for the current design:

- distributed topology features such as replication and sharding
- sessions and multi-document transactions
- TCP networking, TLS, auth, and wire compression
- cost-based optimization from persisted collection statistics beyond current per-index value frequencies
- page-level incremental B-tree maintenance that avoids rebuilding the persisted entry set during checkpoint

## Validation Path

The default direct validation path is `mqlite command`.

It exercises:

- broker discovery or spawn
- real local IPC
- real `OP_MSG`
- real command dispatch
- real storage reopen and recovery

This remains the primary design validation path before any driver patching work.
