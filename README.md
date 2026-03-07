# mqlite

`mqlite` is an in-process-style MongoDB storage and query engine exposed through a local broker binary. Drivers connect through a `file://` URI, discover or spawn the local broker for a specific `.mongodb` file, and then communicate with the broker using MongoDB `OP_MSG` messages over local IPC instead of TCP.

## Current Scope

The repository now contains a working Rust workspace baseline with:
- A cross-crate architecture aligned to the long-term plan.
- A single-file durable store with a fixed header, dual superblocks, append-only WAL, and checkpoint snapshots backed by slotted record pages plus persisted B-tree index pages.
- Local IPC manifest and endpoint generation.
- `OP_MSG` encoding and decoding.
- A broker with core command handling and cursor support.
- A direct CLI command path for broker validation without any patched driver.
- Progressive tests, CI scaffolding, and living specs.

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
- Persistent `_id_` and secondary index durability across broker restarts.
- Planner-backed single-field index scans for `find`, with `explain` reporting `IXSCAN` vs `COLLSCAN`.

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
- Storage tests cover WAL recovery, superblock rotation, slotted-page persistence, stable `RecordId` reopening, multi-page spill for records and B-tree indexes, truncated-tail handling, and fallback to older checkpoints when newer record or index pages are corrupted.
- Regression tests accompany each bug fix.
- CI runs on macOS, Linux, and Windows.
- Coverage reporting is wired into CI for the Linux job.
- The current baseline includes explicit rejection tests for session and transaction envelopes, regression tests for unsupported query operators and aggregation stages, CLI tests that validate broker auto-spawn and restart recovery without any patched driver, broker restart tests that prove unique indexes survive checkpoint and reopen, and `explain` tests that verify indexed `find` plans return `IXSCAN`.

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

This baseline intentionally favors a stable executable slice over speculative completeness. The current file format now implements fixed metadata, rotating superblocks, WAL-backed mutation durability, slotted record pages, persisted B-tree index pages, stable `RecordId`s, and replay on open. The next storage steps are page reuse/compaction, compound-prefix and sort-aware planning, and eventually page-splitting and reuse policies that operate incrementally instead of rebuilding runtime tree state from the persisted entry set.
