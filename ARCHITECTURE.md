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
- `mqlite`: CLI entrypoints such as `serve`, `command`, `info`, `inspect`, `verify`, and `checkpoint`.

`mqlite-storage/src/v2/` is the on-disk storage engine. It contains the v2 header and superblock
format, typed record and secondary page codecs, checkpoint writers, recovery readers, a shared
pager that serves page reads through offset-based I/O plus an in-process page cache, and
page-backed metadata paths for `info` and `inspect`.

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
- Writes append WAL records, update in-memory state, and become durable before command success. Concurrent writers share a short group-commit sync barrier so multiple acknowledged commands can ride the same `fsync`.
- Running brokers checkpoint automatically about every 60 seconds when dirty, but only after a brief quiet window with no command in flight. If the last client disconnects and the broker stays quiet, it now hands off a checkpoint promptly instead of waiting for idle shutdown. The broker captures checkpoint state briefly under the storage lock, then writes the checkpoint on a background worker so later commands can keep running. Writes that arrive after that capture remain in the WAL tail and are preserved across the checkpoint.
- Idle shutdown triggers a checkpoint so the current catalog, pages, and plan-cache state are written back into the main file.
- CRUD and DDL commands also append local change-event records in the same WAL mutation as the collection change so `$changeStream` recovery stays atomic.
- Drivers and the direct CLI both discover or spawn the broker through the same manifest flow.
- The attach or spawn path treats the manifest as the readiness signal, but if `serve` exits before publishing it the caller reports that startup error directly instead of waiting out the manifest timeout.
- Auto-spawned brokers can also receive a watched parent pid; once that launcher process has died and the broker has no active IPC connections left, the broker exits immediately instead of waiting for the normal idle timeout.

The CLI surfaces split along intent:
- `mqlite command` is the direct wire-protocol validation path.
- `mqlite info` summarizes the recovered current catalog state, per-namespace sizes and counts, WAL backlog, and the most recent checkpoint.
- `mqlite inspect` stays focused on lower-level file, superblock, WAL, and catalog metadata.
- `mqlite` now creates and writes only the page-backed v2 file format. `mqlite info` and `mqlite inspect` answer from checkpoint metadata and, when needed, fold in the WAL tail with a metadata-only scan instead of hydrating every collection and index page first. Pre-v2 files are rejected explicitly. `mqlite verify` remains the slower full validation path.

## Durable File Layout

The v2 file is versioned and self-describing.

- Fixed header:
  - magic and file format version
  - reserved metadata region
- Dual rotating superblocks:
  - generation number
  - durable sequence
  - checkpoint time
  - WAL start and end offsets
  - durable root page ids
  - persisted summary counters
- Data region:
  - namespace internal and leaf pages
  - collection-meta, index-meta, and stats pages
  - record internal and leaf pages
  - secondary-index internal and leaf pages
  - change-event and plan-cache pages
  - append-only WAL frames after the checkpointed page graph

The current format version is encoded in the header and active superblock. Recovery rejects unsupported versions.

The v2 format uses 8 KiB fixed pages behind the same single-file model. Its page set is typed up
front rather than being reconstructed from checkpoint snapshot references:

- namespace internal and leaf pages keyed by namespace or index-name strings
- collection-meta and index-meta pages for page roots, key patterns, and counters
- stats pages for persisted per-index value frequencies and field-presence counts
- record internal and leaf pages keyed by stable `RecordId`
- secondary-index internal and leaf pages keyed by persisted BSON key plus `RecordId`
- two rotating superblocks with summary counters for metadata-only open paths

The storage crate resolves collections through persisted namespace metadata, serves page-backed
collection and index read views without full catalog hydration on reopen, and emits full v2
checkpoints by writing namespace, meta, record, secondary-index, stats, change-event, and
plan-cache pages plus a new superblock summary. The v2 `info` path builds its per-database,
per-collection, and per-index report from that persisted namespace and meta page graph and folds
in any WAL tail with a metadata-only pass. Page-backed v2 index read views preserve histogram-style
value frequencies and field-presence counts through checkpoint and reopen, so planning can keep
using persisted estimates instead of rebuilding in-memory stats. The same v2 page graph can also
be materialized back into a full `Catalog` when a broker path still needs collection-owned mutable
state instead of a borrowed page-backed read view.

The `find` planner is now being split away from direct `CollectionCatalog` assumptions. Its
planning and costing paths target a narrower collection and index read view so the broker can plug
in page-backed record and index handles later without changing the planner’s cost model or explain
surface. The broker’s `find` and `explain` read paths now ask the storage engine for that borrowed
collection read view instead of reaching straight through to `Catalog` state. The v2 collection and
index handles now also implement that shared read-view surface over a pager, so the remaining gap
is write-side durability and full broker cutover rather than planner compatibility.

## Snapshot Contents

Each checkpoint snapshot stores the minimum metadata needed to reopen the durable state without rebuilding it from user documents.

- Database and collection catalog structure
- Collection options
- Record page references per collection
- Index page references, root page ids, key patterns, and uniqueness flags
- Persisted plan-cache entries keyed by namespace and query shape
- Persisted change-event page references plus change-event counts

The snapshot does not inline all records and index entries directly. Those live in fixed-size pages referenced by the snapshot metadata.

## Change-Event Storage

`$changeStream` is backed by a persisted local change-event log inside the main `.mongodb` file.

- Change events are stored separately from collection records and index pages.
- Checkpoints encode them as slotted BSON pages so reopen does not depend on the transient WAL.
- WAL mutations carry both the collection change and the associated change-event entries together.
- Recovery replays change events and collection state from the same WAL records, so a crash cannot leave the catalog ahead of the change-stream history or vice versa.

Each persisted change event records:

- a local resume token
- `clusterTime` and `wallTime`
- namespace scope
- operation type
- optional `documentKey`
- optional post-image and pre-image documents
- optional update description
- whether the event is part of the expanded-event surface
- extra stage-visible metadata such as rename targets or index specs

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
- Tree navigation now routes through internal pages with binary search and serves exact `_id`
  lookups directly from the persisted index path instead of materializing a one-entry bounds scan.
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
  - the active source collection, if any
  - whether execution is inside `$facet`
  - the collection resolver
  - expression variables for correlated subpipelines
- Expression evaluation treats `$$CURRENT` as the source for ordinary `"$field.path"` lookups,
  so scoped operators and correlated subpipelines can rebind field-path resolution without
  rewriting inner expressions.

Current cross-namespace aggregation behavior:

- `$unionWith` resolves a foreign collection from the same `.mongodb` file or runs a collectionless
  subpipeline that starts with `$documents`.
- `$lookup` resolves a foreign collection from the same `.mongodb` file and supports:
  - `localField` and `foreignField` equality joins
  - optional `pipeline` filters or reshaping on the joined documents
  - `let` variables for correlated subpipelines
  - collectionless `$documents` subpipelines when `from` is omitted
- `$changeStream` resolves the persisted local change-event log from the same `.mongodb` file and supports:
  - collection, database, and cluster scopes
  - `resumeAfter`, `startAfter`, and `startAtOperationTime`
  - `fullDocument` and `fullDocumentBeforeChange`
  - `showExpandedEvents`
  - finite historical reads over the durable local log at aggregate start time
- `$changeStreamSplitLargeEvent` runs as the final stage in a `$changeStream` pipeline and:
  - splits oversized change events into deterministic top-level-field fragments
  - emits fragment `_id` tokens that extend the base change-event token with `fragmentNum`
  - adds `splitEvent: { fragment, of }` metadata to each fragment
  - enforces fatal resume errors when a resumed pipeline no longer reproduces the referenced split event
- `$setWindowFields` executes locally over the in-memory document stream after the stage-level
  sort and partition step, with:
  - `partitionBy` expression partitioning
  - `sortBy`-driven document order
  - document windows
  - single-sort-key numeric or date range windows
  - supported accumulator-style window functions, ranking functions, `$shift`, `$locf`, and `$linearFill`
  - explicit parse-time rejection for unsupported window functions
- Nested lookup-style subpipelines inherit outer variables by value so correlated `$expr` filters
  continue to work in nested stages.
- Aggregation control flow currently includes `$cond` and `$switch`, while scoped expressions
  include `$let`, `$map`, `$filter`, `$reduce` with optional `limit`. Set-style array expressions
  currently include `$setDifference`, `$setEquals`, `$setIntersection`, `$setIsSubset`, and
  `$setUnion`. Accumulator-style expressions currently include `$sum`, `$avg`, `$min`, and
  `$max`, while statistical expressions currently include `$stdDevPop`, `$stdDevSamp`,
  `$percentile`, and `$median`, with local `approximate`, `discrete`, and `continuous`
  percentile evaluators over per-document scalar or array inputs. Metadata expressions currently
  include `$meta` over broker-local `geoNearDistance`, `geoNearPoint`, `indexKey`, `recordId`,
  `sortKey`, `randVal`, and `textScore` slots when upstream stages or planners attach them to the
  in-flight document stream. Hashed-key expressions currently include `$toHashedIndexKey`, using
  Mongo-compatible MD5 hashing over canonical BSON type tags, field-name recursion, and
  `safeNumberLongForHash`-style numeric coercion so local hashed-key derivation matches server
  results. Array-selection `N`
  expressions include `$firstN`, `$lastN`, `$minN`, and `$maxN`. Date-part expressions currently include `$year`, `$month`, `$dayOfMonth`,
  `$dayOfWeek`, `$dayOfYear`, `$hour`, `$minute`, `$second`, `$millisecond`, `$week`,
  `$isoDayOfWeek`, `$isoWeek`, and `$isoWeekYear`, while date construction, parsing, formatting,
  and arithmetic currently include `$dateFromString`, `$dateToString`, `$dateFromParts`,
  `$dateToParts`, `$dateAdd`, `$dateSubtract`, `$dateDiff`, and `$dateTrunc`, including
  Mongo-style named timezone arguments over UTC, fixed-offset, and Olson timezone names plus
  ISO-week construction and week-boundary handling through `startOfWeek`. Basic ASCII string case
  expressions currently include `$toLower`, `$toUpper`,
  and `$strcasecmp`. Size introspection currently includes `$binarySize` and `$bsonSize`. String
  trimming currently includes `$trim`, `$ltrim`, and `$rtrim`. Logarithmic and power expressions
  currently include `$exp`, `$ln`, `$log`, `$log10`, `$pow`, and `$sqrt`. Trigonometric and
  angular-conversion expressions currently include `$acos`, `$acosh`, `$asin`, `$asinh`, `$atan`,
  `$atan2`, `$atanh`, `$cos`, `$cosh`, `$sin`, `$sinh`, `$tan`, `$tanh`, `$degreesToRadians`,
  and `$radiansToDegrees`. String-length, string-position, substring, regex, split, and replacement
  expressions currently include `$strLenBytes`, `$strLenCP`, `$indexOfBytes`, `$indexOfCP`,
  `$substr`, `$substrBytes`, `$substrCP`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$split`,
  `$replaceOne`, and `$replaceAll`,
  conversion expressions currently include `$convert`, `$toBool`, `$toDate`, `$toDecimal`,
  `$toDouble`, `$toInt`, `$toLong`, `$toObjectId`, and `$toString`,
  utility expressions currently include `$rand`, `$sortArray`, `$tsSecond`, `$tsIncrement`, and
  `$zip`,
  while integer bitwise expressions include `$bitAnd`, `$bitOr`, `$bitXor`, and `$bitNot`,
  alongside literal field access and mutation via `$getField`, `$setField`, and `$unsetField`,
  all executed in-process without a separate expression VM. Nullable numeric operators share a
  common evaluator path so nullish inputs return `null` consistently while domain violations fail
  explicitly. String replacement and split currently support string operands only; regex-backed
  variants remain out of scope until the feature-flagged upstream behavior is intentionally adopted.
  `$convert` currently supports the core scalar target families plus `onError` and `onNull`;
  feature-flagged or BinData-oriented variants such as `base`, `format`, `byteOrder`, and subtype
  conversions are still rejected explicitly.
- `$out` is a broker-backed terminal write stage that replaces a same-file target namespace
  through a fresh-collection rewrite WAL frame, preserving collection options but resetting the
  namespace to a new `_id_` index plus the pipeline output documents, and returns an empty cursor
  result to the client.
- `$merge` is a broker-backed terminal write stage that merges pipeline results into a same-file
  target namespace using supported `whenMatched` and `whenNotMatched` string modes plus optional
  `on` fields. It reuses the same ordered delta persistence path as CRUD writes instead of
  replacing the whole target collection image.
- The current implementation does not federate across files or processes.

## WAL And Recovery

Mutations are durable through an append-only WAL.

- CRUD writes append ordered typed per-record insert, update, and delete deltas, creating collections through the same WAL path when needed.
- Ordered CRUD deltas and index create/drop operations use typed WAL frames. `$out`-style namespace resets use a fresh-collection rewrite frame that rebuilds the target from collection options plus ordered inserts, while collection replacement and drop remain collection-level WAL frames where a full collection image still has to move as one unit.
- WAL frames include a sequence number and checksum.
- WAL frames and checkpoint snapshots encode their control metadata in a compact CBOR envelope while preserving embedded MongoDB documents as raw BSON bytes, so exact BSON typing survives recovery without paying BSON-document overhead for the whole envelope.
- Checkpoint pages, snapshot metadata, and large WAL payloads can be wrapped in a small zstd envelope when low-level compression saves meaningful space. Logical record and index pages still decode back to fixed 4 KiB pages, and checksums always cover the stored bytes on disk.
- Broker-built `CollectionRecord`s can carry cached raw BSON bytes into storage so insert and update writes can reuse the same document encoding for change-event construction, WAL record encoding, and later checkpoint page encoding instead of serializing the same document repeatedly.
- Checkpoint page builders size record, change-event, and index payloads from already-encoded BSON bytes and then write those bytes directly into slotted pages, avoiding the older “encode to measure, then encode again to insert” path.
- When the broker already hands storage pre-encoded change-event byte fields, WAL append serialization borrows those byte slices directly into the compact CBOR frame instead of cloning a second owned copy before the write.
- Persisted change events also keep `documentKey`, `fullDocument`, `fullDocumentBeforeChange`, `updateDescription`, `extraFields`, and the resume-token document as raw BSON blobs, materializing nested `Document`s only when a `$changeStream` reader asks for them.
- The broker applies the mutation to in-memory state immediately after the WAL append succeeds, then waits for a shared WAL sync barrier before acknowledging command success.
- Read paths borrow storage only after the visible sequence is durable, so queries and metadata commands do not observe broker-local writes that are still waiting on the shared WAL sync.
- Applying CRUD deltas batches touched record state and index-entry maintenance in memory, keeps runtime secondary indexes as leaf-sized in-memory pages, updates only the touched leaves during inserts, keeps per-index planner histogram stats incrementally, and avoids rebuilding a query-only in-memory tree on every mutation.
- Storage also keeps an in-memory per-collection unique-key validation cache derived from the catalog at open time, keyed by structured BSON index keys, then applies small insert/update/delete deltas to that cache after each durable write instead of rebuilding duplicate-key state from every unique index on every command. Per-command validation overlays borrow the batch's write documents directly so insert-heavy workloads do not clone a second copy of every document just to validate uniqueness.
- After that storage validation succeeds on the live write path, collection delta application uses a trusted catalog mutation path that skips re-running the same unique-key duplicate probes during commit. WAL replay still uses the fully validating path so reopen continues to reject inconsistent persisted mutations.
- Each collection persists its next `RecordId` high-water mark in the checkpoint metadata and reconstructs a transient `record_id -> record vector position` map on load so later writes can allocate ids and find target rows without rescanning the collection.
- Checkpoints carry forward unchanged record, index, and change-event pages from the active snapshot. Dirty collections are diffed against the active snapshot so unchanged prefix/suffix record pages, unchanged index trees, and unchanged change-event page ranges can still be reused instead of forcing a full namespace rewrite.
- Checkpoints can reuse the inactive superblock slot's preserved snapshot or stale WAL region when the next snapshot fits there, while keeping new WAL appends after the preserved fallback checkpoint region.
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
- `update` and `delete` reuse the same indexed candidate planning path as `find`, then reconcile those base candidates against the command-local insert, update, and delete overlay so ordered multi-op writes preserve in-request visibility.
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
   - resolve persisted local change-event input for `$changeStream`
   - synthesize collectionless metadata-stage input for supported first stages such as `$currentOp`
   - synthesize collection metadata-stage input for supported first stages such as `$collStats`
   - synthesize collection index metadata-stage input for supported first stages such as `$indexStats`
   - synthesize collection catalog metadata-stage input for supported first stages such as `$listCatalog`
   - synthesize cluster-catalog metadata-stage input for supported first stages such as `$listClusterCatalog`
   - synthesize auth-diagnostic metadata-stage input for supported first stages such as `$listCachedAndActiveUsers`
   - synthesize session-metadata input for supported first stages such as `$listLocalSessions` and `$listSessions`
   - synthesize query-sampling metadata input for supported first stages such as `$listSampledQueries`
   - synthesize search-index metadata input for supported first stages such as `$listSearchIndexes`
   - synthesize query-settings metadata input for supported first stages such as `$querySettings`
   - synthesize collectionless capability-metadata input for supported first stages such as `$listMqlEntities`
   - synthesize collection plan-cache metadata-stage input for supported first stages such as `$planCacheStats`
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
