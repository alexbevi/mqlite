# mqlite

`mqlite` is a local MongoDB-compatible engine built around a single `.mongodb` file.
It runs one broker per file, speaks MongoDB `OP_MSG` over local IPC, and lets clients use a `file://` URI instead of connecting to a TCP server.

The project is aimed at local-first workflows:
- application development that wants document storage without a separate database service
- integration and driver testing against a real MongoDB-style wire protocol
- tooling that benefits from a durable, inspectable, single-file data store

`mqlite` is not trying to be a full MongoDB server. It targets a practical Stable API v1 subset plus the bootstrap and admin behavior needed to make local tooling and drivers useful.

## Why Use It

- Single-file durability: one `.mongodb` file is the source of truth.
- Local transport: no TCP listener, no port management, no network stack in the happy path.
- MongoDB command model: commands still flow as BSON over `OP_MSG`.
- Direct validation path: the CLI can exercise the broker without patching a driver first.
- Explicit compatibility story: exact supported and unsupported operators and stages are tracked in generated capability reports instead of being buried in the README.

## Quick Start

`mqlite` requires Rust 1.87 or newer.

Build the binary:

```text
cargo build -p mqlite
```

Create a collection, insert a document, and query it back:

```text
mqlite command --file /tmp/example.mongodb --db app --eval '{"create":"widgets"}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"insert":"widgets","documents":[{"sku":"alpha","qty":2}]}'
mqlite command --file /tmp/example.mongodb --db app --eval '{"find":"widgets","filter":{"sku":"alpha"}}'
```

`mqlite command` will reuse an existing broker for that file or auto-spawn one if needed.
If the auto-spawned broker exits before it publishes its manifest, the command reports that startup failure instead of collapsing it into a generic manifest timeout.
Launcher-owned brokers are also started with `--watch-parent-pid`, so `mqlite command` and driver-managed brokers shut themselves down shortly after the spawning process exits instead of waiting out the full idle timeout.
Broker startup now stays lazy for clean checkpointed files: the broker can publish its manifest before it opens the mutable storage engine, and simple indexed reads can route through page-backed v2 handles directly from the durable file instead of hydrating the full runtime catalog first.
Brokers also checkpoint automatically after about 60 seconds once the broker reaches a brief quiet window with no command in flight. If the last client disconnects and the broker stays quiet, it now checkpoints promptly instead of waiting for idle shutdown, and a loaded broker will also checkpoint early when the WAL tail grows past its backlog threshold even if a client keeps the connection open. Those background checkpoints now publish a new page-root snapshot that rewrites only dirty collection/index page subtrees plus fresh namespace metadata, so clean collection pages can be reused and later broker starts have much less WAL to replay. Any writes that arrive after the published snapshot stay in the WAL tail until the next publish or full checkpoint.

If you want a broker to stay up explicitly, run:

```text
mqlite serve --file /tmp/example.mongodb
```

## URI Model

Driver-facing URIs use an absolute `file://` path:

```text
file:///absolute/path/to/database.mongodb?db=app
```

- The path selects the durable `.mongodb` file.
- The optional `db` query parameter selects the default database.
- The broker is local to that file and communicates over IPC, not TCP.
- Network and distributed-server options do not apply to this transport model.

For adapter details and option policy, see [DRIVER.md](DRIVER.md).

## CLI

The CLI is intentionally small and focused:

| Command | Purpose |
| --- | --- |
| `mqlite serve --file <path>` | Run a broker for one `.mongodb` file. |
| `mqlite command --file <path>` | Send one MongoDB command over a real `OP_MSG` request and print the reply as JSON. |
| `mqlite bench --file <path>` | Run a quick local broker benchmark and report startup, write, and point-query latency against the selected index path. |
| `mqlite checkpoint --file <path>` | Force a checkpoint and print storage metadata. |
| `mqlite info --file <path>` | Print current database, collection, and index counts and sizes plus last-checkpoint details. |
| `mqlite verify --file <path>` | Validate the durable file structure that can be checked on open. |
| `mqlite inspect --file <path>` | Print file, checkpoint, WAL, and catalog metadata. |

`mqlite` now creates and writes only the page-backed v2 file format. `mqlite info` summarizes the current state recovered from the file, including per-database, per-collection, and per-index sizes and counts, and separates that from the most recent checkpoint metadata. `mqlite inspect` remains the lower-level file-layout report. Both commands answer from persisted checkpoint metadata and, when needed, fold in the WAL tail with a metadata-only scan instead of rehydrating every record and index page first, so they return quickly even on large files. Older pre-v2 files are rejected explicitly. Use `mqlite verify` when you want the slower full page-validation pass.

`mqlite command --debug` keeps the normal command reply on stdout and emits a structured debug report on stderr. That report combines client-side IPC and wire timings with broker-side server, query, storage, catalog, exec, and BSON spans, plus counters such as page-cache hits and misses, WAL replay counts, per-mutation replay bytes and document counts, and lightweight process snapshots.

You can also pipe JSON into `mqlite command`:

```text
printf '%s\n' '{"listCollections":1}' | mqlite command --file /tmp/example.mongodb --db app
```

`mqlite bench` now reports broker startup latency, per-phase throughput, the query field used for point reads, the first and slowest point-query latency it observed, and explicit startup / first-point-query budget verdicts so storage changes can be checked against the local startup SLO.

## What Works Today

At a high level, the current engine already covers:
- single-file durable storage with WAL, checkpoints, recovery, and persisted secondary indexes
- selective zstd compression for checkpoint pages, snapshot metadata, and large WAL frames when the stored bytes shrink materially
- page-local in-memory secondary-index maintenance so bulk inserts only rewrite touched index leaves
- persisted v2 index stats so page-backed reads keep value-frequency and field-presence estimates after checkpoint and reopen
- v2 page graphs can now be materialized back into a full in-memory catalog when a broker path still needs collection-owned state
- v2 checkpoints can now carry change-stream history and persisted plan-cache entries alongside the page graph
- incrementally maintained in-memory unique-index validation caches that keep structured BSON keys and borrow per-batch write documents so writes do not rebuild duplicate-key state or duplicate full documents during validation
- storage commits apply already-validated CRUD deltas through the catalog without repeating the same unique-index duplicate probes on the hot path
- write-path BSON reuse so inserted and updated records can carry cached raw BSON bytes into WAL and checkpoint encoding instead of reserializing the same document for every layer
- multiple databases and collections inside one file
- core MongoDB command flows such as `hello`, `ping`, CRUD, index management, and cursors
- `find` planning with observable `explain` output for collection and index scans
- a substantial local aggregation subset, including same-file cross-namespace stages and write stages
- persisted local change-stream history for `$changeStream`
- direct CLI-based validation without a patched driver

For the exact command, query, expression, and aggregation surface, use the generated capability docs:
- [capabilities/mqlite/gap-analysis.generated.md](capabilities/mqlite/gap-analysis.generated.md)
- [capabilities/mqlite/support.generated.json](capabilities/mqlite/support.generated.json)

## What It Does Not Try To Be

`mqlite` is intentionally a local engine, not a distributed database server. That means:
- no TCP listeners, TLS, auth, or network compression
- no replica sets, sharding, or load balancing
- no logical sessions, multi-document transactions, or retryable read/write semantics
- no server-side JavaScript; `$where` and `$function` remain explicit unsupported failures

Some MongoDB features are also still unimplemented in the local engine. The capability reports above are the source of truth for those gaps.

## Build And Test

Common workspace commands:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

If query or aggregation support changes, resync the checked-in capability snapshots with:

```text
cargo run -p mqlite-capabilities -- sync
```

## Further Reading

- [ARCHITECTURE.md](ARCHITECTURE.md): storage, index, planner, broker, and durability design
- [DRIVER.md](DRIVER.md): `file://` adapter model and driver expectations
- [capabilities/mqlite/gap-analysis.generated.md](capabilities/mqlite/gap-analysis.generated.md): precise supported and unsupported surface
