use std::{
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, ExitStatus, Stdio},
    time::SystemTime,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use bson::{Bson, Document, doc};
use clap::{Parser, Subcommand};
use mqlite_debug::{Component, SessionHandle, install, session};
use mqlite_ipc::{BoxedStream, BrokerPaths, broker_paths, connect, read_manifest, remove_manifest};
use mqlite_server::{Broker, BrokerConfig};
use mqlite_storage::DatabaseFile;
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "mqlite")]
#[command(about = "A local MongoDB-compatible broker for file-backed databases")]
struct Cli {
    #[arg(long, global = true)]
    debug: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
        #[arg(long)]
        watch_parent_pid: Option<u32>,
    },
    Checkpoint {
        #[arg(long)]
        file: PathBuf,
    },
    Verify {
        #[arg(long)]
        file: PathBuf,
    },
    Inspect {
        #[arg(long)]
        file: PathBuf,
    },
    Info {
        #[arg(long)]
        file: PathBuf,
    },
    #[command(name = "command")]
    Request {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        db: Option<String>,
        #[arg(long)]
        eval: Option<String>,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
    },
    Bench {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value = "bench")]
        db: String,
        #[arg(long, default_value = "bench")]
        collection_prefix: String,
        #[arg(long, default_value_t = 1000)]
        writes: u32,
        #[arg(long, default_value_t = 1000)]
        reads: u32,
        #[arg(long, default_value_t = 1)]
        write_batch_size: u32,
        #[arg(long)]
        index_field: Option<String>,
        #[arg(long, requires = "index_field")]
        unique_index: bool,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let debug = cli.debug;

    match cli.command {
        Command::Serve {
            file,
            idle_shutdown_secs,
            watch_parent_pid,
        } => {
            let mut config = BrokerConfig::new(file, idle_shutdown_secs);
            config.watch_parent_pid = watch_parent_pid;
            let broker = Broker::new(config)?;
            broker.serve().await?;
        }
        Command::Checkpoint { file } => {
            let session = debug.then(|| session("cli.checkpoint"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("command", "checkpoint");
                session.insert_metadata("file", file.display().to_string());
            }
            let inspect = {
                let _install = session.as_ref().map(install);
                let mut database = DatabaseFile::open_or_create(file)?;
                database.checkpoint()?;
                DatabaseFile::inspect(database.path())?
            };
            println!("{}", serde_json::to_string_pretty(&inspect)?);
            emit_local_debug_report(session.as_ref())?;
        }
        Command::Verify { file } => {
            let session = debug.then(|| session("cli.verify"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("command", "verify");
                session.insert_metadata("file", file.display().to_string());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&{
                    let _install = session.as_ref().map(install);
                    DatabaseFile::verify(file)?
                })?
            );
            emit_local_debug_report(session.as_ref())?;
        }
        Command::Inspect { file } => {
            let session = debug.then(|| session("cli.inspect"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("command", "inspect");
                session.insert_metadata("file", file.display().to_string());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&{
                    let _install = session.as_ref().map(install);
                    DatabaseFile::inspect(file)?
                })?
            );
            emit_local_debug_report(session.as_ref())?;
        }
        Command::Info { file } => {
            let session = debug.then(|| session("cli.info"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("command", "info");
                session.insert_metadata("file", file.display().to_string());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&{
                    let _install = session.as_ref().map(install);
                    DatabaseFile::info(file)?
                })?
            );
            emit_local_debug_report(session.as_ref())?;
        }
        Command::Request {
            file,
            db,
            eval,
            idle_shutdown_secs,
        } => {
            let session = debug.then(|| session("cli.command"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("file", file.display().to_string());
            }
            let parse_started = Instant::now();
            let mut command = {
                let _install = session.as_ref().map(install);
                parse_command_document(eval)?
            };
            if let Some(session) = session.as_ref() {
                session.record_duration(
                    Component::Cli,
                    "parse_command_document",
                    parse_started.elapsed(),
                );
            }
            if !command.contains_key("$db") {
                command.insert("$db", db.unwrap_or_else(|| "admin".to_string()));
            }
            if let Some(session) = session.as_ref() {
                if let Ok(database) = command.get_str("$db") {
                    session.insert_metadata("database", database);
                }
                if let Some(command_name) =
                    command.keys().find(|key| !key.starts_with('$')).cloned()
                {
                    session.insert_metadata("command", command_name);
                }
            }
            if debug {
                command.insert("$mqliteDebug", true);
            }

            let mut stream =
                connect_or_spawn_broker(&file, idle_shutdown_secs, session.as_ref()).await?;
            let mut response = send_command(&mut stream, command, session.as_ref()).await?;
            let broker_debug = response
                .remove("$mqliteDebug")
                .and_then(|value| value.as_document().cloned());
            println!("{}", serde_json::to_string_pretty(&response)?);
            emit_command_debug_report(session.as_ref(), broker_debug.as_ref())?;
            if response.get_f64("ok").unwrap_or(0.0) == 0.0 {
                bail!(
                    "{}",
                    response
                        .get_str("errmsg")
                        .unwrap_or("mqlite command returned an error")
                );
            }
        }
        Command::Bench {
            file,
            db,
            collection_prefix,
            writes,
            reads,
            write_batch_size,
            index_field,
            unique_index,
            idle_shutdown_secs,
        } => {
            let session = debug.then(|| session("cli.bench"));
            if let Some(session) = session.as_ref() {
                session.insert_metadata("command", "bench");
                session.insert_metadata("file", file.display().to_string());
            }
            let bench_started = Instant::now();
            let report = run_benchmark(
                &file,
                BenchmarkOptions {
                    db: &db,
                    collection_prefix: &collection_prefix,
                    writes,
                    reads,
                    write_batch_size,
                    index_field: index_field.as_deref(),
                    unique_index,
                    idle_shutdown_secs,
                },
            )
            .await?;
            if let Some(session) = session.as_ref() {
                session.record_duration(Component::Cli, "run_benchmark", bench_started.elapsed());
            }
            println!("{}", serde_json::to_string_pretty(&report)?);
            emit_local_debug_report(session.as_ref())?;
        }
    }

    Ok(())
}

fn parse_command_document(eval: Option<String>) -> Result<Document> {
    let payload = match eval {
        Some(payload) => payload,
        None => {
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .context("failed to read command JSON from stdin")?;
            buffer
        }
    };

    let value: serde_json::Value =
        serde_json::from_str(&payload).context("command payload must be valid JSON")?;
    let document = bson::to_bson(&value)
        .context("failed to convert JSON payload to BSON")?
        .as_document()
        .cloned()
        .ok_or_else(|| anyhow!("command payload must be a JSON object"))?;
    Ok(document)
}

async fn connect_or_spawn_broker(
    file: &Path,
    idle_shutdown_secs: u64,
    debug: Option<&SessionHandle>,
) -> Result<BoxedStream> {
    let paths = broker_paths(file)?;

    if let Some(stream) = try_connect_existing(&paths, debug).await? {
        return Ok(stream);
    }

    if let Some(debug) = debug {
        debug.insert_metadata("brokerLaunch", "spawned");
    }
    let child = spawn_broker(&paths.database_path, idle_shutdown_secs, debug)?;
    wait_for_broker(&paths, child, Duration::from_secs(5), debug).await
}

async fn try_connect_existing(
    paths: &BrokerPaths,
    debug: Option<&SessionHandle>,
) -> Result<Option<BoxedStream>> {
    let started = Instant::now();
    if !paths.manifest_path.exists() {
        if let Some(debug) = debug {
            debug.record_duration(Component::Ipc, "try_connect_existing", started.elapsed());
        }
        return Ok(None);
    }

    let manifest = match read_manifest(&paths.manifest_path) {
        Ok(manifest) => manifest,
        Err(_) => {
            let _ = remove_manifest(&paths.manifest_path);
            if let Some(debug) = debug {
                debug.record_duration(Component::Ipc, "try_connect_existing", started.elapsed());
            }
            return Ok(None);
        }
    };

    match connect(&manifest.endpoint).await {
        Ok(stream) => {
            if let Some(debug) = debug {
                debug.record_duration(Component::Ipc, "try_connect_existing", started.elapsed());
                debug.insert_metadata("brokerLaunch", "existing");
            }
            Ok(Some(stream))
        }
        Err(_) => {
            let _ = remove_manifest(&paths.manifest_path);
            if let Some(debug) = debug {
                debug.record_duration(Component::Ipc, "try_connect_existing", started.elapsed());
            }
            Ok(None)
        }
    }
}

fn spawn_broker(
    file: &Path,
    idle_shutdown_secs: u64,
    debug: Option<&SessionHandle>,
) -> Result<Child> {
    let started = Instant::now();
    let current_executable =
        std::env::current_exe().context("failed to locate mqlite executable")?;
    let child = ProcessCommand::new(current_executable)
        .args(["serve", "--file"])
        .arg(file)
        .args(["--watch-parent-pid", &std::process::id().to_string()])
        .args(["--idle-shutdown-secs", &idle_shutdown_secs.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn mqlite broker")?;
    if let Some(debug) = debug {
        debug.record_duration(Component::Ipc, "spawn_broker", started.elapsed());
    }
    Ok(child)
}

async fn wait_for_broker(
    paths: &BrokerPaths,
    mut child: Child,
    timeout: Duration,
    debug: Option<&SessionHandle>,
) -> Result<BoxedStream> {
    let started = Instant::now();
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(manifest) = read_manifest(&paths.manifest_path) {
            if let Ok(stream) = connect(&manifest.endpoint).await {
                if let Some(debug) = debug {
                    debug.record_duration(Component::Ipc, "wait_for_broker", started.elapsed());
                }
                return Ok(stream);
            }
        }

        if let Some(status) = child
            .try_wait()
            .context("failed to observe mqlite broker startup")?
        {
            return Err(broker_startup_error(paths, &mut child, status));
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for mqlite broker manifest at {}",
                paths.manifest_path.display()
            );
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn broker_startup_error(
    paths: &BrokerPaths,
    child: &mut Child,
    status: ExitStatus,
) -> anyhow::Error {
    let mut stderr = String::new();
    if let Some(mut broker_stderr) = child.stderr.take() {
        let _ = broker_stderr.read_to_string(&mut stderr);
    }

    let startup_message = stderr
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .unwrap_or_default();
    if startup_message.is_empty() {
        anyhow!(
            "mqlite broker exited before writing its manifest at {} with status {status}",
            paths.manifest_path.display()
        )
    } else {
        anyhow!(
            "mqlite broker exited before writing its manifest at {}: {startup_message}",
            paths.manifest_path.display()
        )
    }
}

async fn send_command(
    stream: &mut BoxedStream,
    body: Document,
    debug: Option<&SessionHandle>,
) -> Result<Document> {
    let request_fields = body.len() as u64;
    let request = OpMsg::new(1, 0, vec![PayloadSection::Body(body)]);
    let write_started = Instant::now();
    write_op_msg(stream, &request).await?;
    if let Some(debug) = debug {
        debug.record_duration(
            Component::Wire,
            "client_write_op_msg",
            write_started.elapsed(),
        );
        debug.record_counter(Component::Wire, "clientRequestFields", request_fields);
    }
    let read_started = Instant::now();
    let response = read_op_msg(stream).await?;
    if let Some(debug) = debug {
        debug.record_duration(
            Component::Wire,
            "client_read_op_msg",
            read_started.elapsed(),
        );
    }
    response
        .body()
        .cloned()
        .ok_or_else(|| anyhow!("broker reply did not contain a body section"))
}

async fn send_checked_command(stream: &mut BoxedStream, body: Document) -> Result<Document> {
    let response = send_command(stream, body, None).await?;
    if response.get_f64("ok").unwrap_or(0.0) == 0.0 {
        bail!(
            "{}",
            response
                .get_str("errmsg")
                .unwrap_or("mqlite benchmark command returned an error")
        );
    }
    Ok(response)
}

fn emit_local_debug_report(session: Option<&SessionHandle>) -> Result<()> {
    let Some(session) = session else {
        return Ok(());
    };
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "debug": {
                "client": session.report(),
            }
        }))?
    );
    Ok(())
}

fn emit_command_debug_report(
    session: Option<&SessionHandle>,
    broker_debug: Option<&Document>,
) -> Result<()> {
    let Some(session) = session else {
        return Ok(());
    };
    let mut debug = serde_json::Map::new();
    debug.insert(
        "client".to_string(),
        serde_json::to_value(session.report())?,
    );
    if let Some(broker_debug) = broker_debug {
        debug.insert("broker".to_string(), serde_json::to_value(broker_debug)?);
    }
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({ "debug": debug }))?
    );
    Ok(())
}

struct BenchmarkOptions<'a> {
    db: &'a str,
    collection_prefix: &'a str,
    writes: u32,
    reads: u32,
    write_batch_size: u32,
    index_field: Option<&'a str>,
    unique_index: bool,
    idle_shutdown_secs: u64,
}

const BENCH_STARTUP_TARGET_MS: f64 = 300.0;
const BENCH_FIRST_POINT_QUERY_TARGET_MS: f64 = 500.0;

async fn run_benchmark(file: &Path, options: BenchmarkOptions<'_>) -> Result<serde_json::Value> {
    let BenchmarkOptions {
        db,
        collection_prefix,
        writes,
        reads,
        write_batch_size,
        index_field,
        unique_index,
        idle_shutdown_secs,
    } = options;

    if write_batch_size == 0 {
        bail!("--write-batch-size must be greater than 0");
    }

    let run_id = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_micros()
        .to_string();
    let collection = format!("{collection_prefix}_{run_id}");
    let startup_started_at = Instant::now();
    let mut stream = connect_or_spawn_broker(file, idle_shutdown_secs, None).await?;
    let startup_elapsed = startup_started_at.elapsed();

    send_checked_command(
        &mut stream,
        doc! {
            "create": collection.as_str(),
            "$db": db,
        },
    )
    .await?;

    if let Some(field) = index_field {
        let index_name = if unique_index {
            format!("{field}_1_unique")
        } else {
            format!("{field}_1")
        };
        let mut key = Document::new();
        key.insert(field, 1);
        let mut index_spec = Document::new();
        index_spec.insert("key", Bson::Document(key));
        index_spec.insert("name", index_name);
        index_spec.insert("unique", unique_index);
        send_checked_command(
            &mut stream,
            doc! {
                "createIndexes": collection.as_str(),
                "indexes": Bson::Array(vec![Bson::Document(index_spec)]),
                "$db": db,
            },
        )
        .await?;
    }

    let write_started_at = Instant::now();
    let mut write_commands = 0u32;
    let mut batch_start = 0u32;
    while batch_start < writes {
        let batch_end = (batch_start + write_batch_size).min(writes);
        let documents = (batch_start..batch_end)
            .map(|sequence| benchmark_document(&run_id, sequence, index_field))
            .collect::<Vec<_>>();
        send_checked_command(
            &mut stream,
            doc! {
                "insert": collection.as_str(),
                "documents": Bson::Array(documents),
                "$db": db,
            },
        )
        .await?;
        write_commands += 1;
        batch_start = batch_end;
    }
    let write_elapsed = write_started_at.elapsed();

    let readable = reads.min(writes);
    let read_started_at = Instant::now();
    let mut first_read_elapsed = None;
    let mut max_read_elapsed = Duration::ZERO;
    let query_field = index_field.unwrap_or("_id");
    for sequence in 0..readable {
        let query_started_at = Instant::now();
        let response = send_checked_command(
            &mut stream,
            doc! {
                "find": collection.as_str(),
                "filter": benchmark_filter(&run_id, sequence, index_field),
                "limit": 1,
                "$db": db,
            },
        )
        .await?;
        let query_elapsed = query_started_at.elapsed();
        first_read_elapsed.get_or_insert(query_elapsed);
        max_read_elapsed = max_read_elapsed.max(query_elapsed);
        let first_batch = response
            .get_document("cursor")
            .context("find reply missing cursor")?
            .get_array("firstBatch")
            .context("find reply missing firstBatch")?;
        if first_batch.len() != 1 {
            bail!(
                "benchmark read expected exactly one document for sequence {sequence}, got {}",
                first_batch.len()
            );
        }
    }
    let read_elapsed = read_started_at.elapsed();
    let total_elapsed = write_elapsed + read_elapsed;
    let first_read_elapsed = first_read_elapsed.unwrap_or(Duration::ZERO);

    Ok(json!({
        "file": file.display().to_string(),
        "db": db,
        "collection": collection,
        "runId": run_id,
        "startup": benchmark_budget_report(startup_elapsed, BENCH_STARTUP_TARGET_MS),
        "index": index_field.map(|field| {
            json!({
                "field": field,
                "unique": unique_index,
            })
        }),
        "writes": benchmark_phase_report(
            write_elapsed,
            writes,
            write_commands,
            write_batch_size,
            "n/a",
            Duration::ZERO,
            Duration::ZERO,
        ),
        "reads": benchmark_phase_report(read_elapsed, readable, readable, 1, query_field, first_read_elapsed, max_read_elapsed),
        "targets": {
            "startupMs": BENCH_STARTUP_TARGET_MS,
            "firstPointQueryMs": BENCH_FIRST_POINT_QUERY_TARGET_MS,
        },
        "budgets": {
            "startup": benchmark_budget_report(startup_elapsed, BENCH_STARTUP_TARGET_MS),
            "firstPointQuery": benchmark_budget_report(first_read_elapsed, BENCH_FIRST_POINT_QUERY_TARGET_MS),
        },
        "totals": {
            "documents": writes + readable,
            "elapsedMs": duration_ms(total_elapsed),
            "docsPerSec": rate_per_second(writes + readable, total_elapsed),
        }
    }))
}

fn benchmark_filter(run_id: &str, sequence: u32, index_field: Option<&str>) -> Document {
    match index_field {
        Some(field) => doc! { field: format!("{run_id}-{sequence}") },
        None => doc! { "_id": format!("{run_id}-{sequence}") },
    }
}

fn benchmark_document(run_id: &str, sequence: u32, index_field: Option<&str>) -> Bson {
    let mut document = doc! {
        "_id": format!("{run_id}-{sequence}"),
        "runId": run_id,
        "seq": i64::from(sequence),
        "payload": format!("payload-{sequence}"),
    };
    if let Some(field) = index_field {
        document.insert(field, format!("{run_id}-{sequence}"));
    }
    Bson::Document(document)
}

fn benchmark_phase_report(
    elapsed: Duration,
    documents: u32,
    commands: u32,
    batch_size: u32,
    query_field: &str,
    first_query_elapsed: Duration,
    max_query_elapsed: Duration,
) -> serde_json::Value {
    json!({
        "documents": documents,
        "commands": commands,
        "batchSize": batch_size,
        "queryField": query_field,
        "elapsedMs": duration_ms(elapsed),
        "firstQueryElapsedMs": duration_ms(first_query_elapsed),
        "maxQueryElapsedMs": duration_ms(max_query_elapsed),
        "docsPerSec": rate_per_second(documents, elapsed),
        "commandsPerSec": rate_per_second(commands, elapsed),
    })
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn benchmark_budget_report(elapsed: Duration, target_ms: f64) -> serde_json::Value {
    let elapsed_ms = duration_ms(elapsed);
    json!({
        "elapsedMs": elapsed_ms,
        "targetMs": target_ms,
        "withinTarget": elapsed_ms <= target_ms,
    })
}

fn rate_per_second(count: u32, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return f64::INFINITY;
    }
    f64::from(count) / seconds
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bson::doc;
    use serde_json::json;

    use super::{benchmark_budget_report, benchmark_filter};

    #[test]
    fn benchmark_filter_targets_secondary_index_when_present() {
        assert_eq!(
            benchmark_filter("run", 7, Some("sku")),
            doc! { "sku": "run-7" }
        );
        assert_eq!(benchmark_filter("run", 7, None), doc! { "_id": "run-7" });
    }

    #[test]
    fn benchmark_budget_report_flags_threshold_crossing() {
        let within = benchmark_budget_report(Duration::from_millis(120), 300.0);
        assert_eq!(within["withinTarget"], json!(true));
        assert_eq!(within["targetMs"], json!(300.0));

        let over = benchmark_budget_report(Duration::from_millis(620), 500.0);
        assert_eq!(over["withinTarget"], json!(false));
        assert_eq!(over["elapsedMs"], json!(620.0));
    }
}
