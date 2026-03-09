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
use mqlite_ipc::{BoxedStream, BrokerPaths, broker_paths, connect, read_manifest, remove_manifest};
use mqlite_server::{Broker, BrokerConfig};
use mqlite_storage::DatabaseFile;
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "mqlite")]
#[command(about = "A local MongoDB-compatible broker for file-backed databases")]
struct Cli {
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
            let mut database = DatabaseFile::open_or_create(file)?;
            database.checkpoint()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&DatabaseFile::inspect(database.path())?)?
            );
        }
        Command::Verify { file } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&DatabaseFile::verify(file)?)?
            );
        }
        Command::Inspect { file } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&DatabaseFile::inspect(file)?)?
            );
        }
        Command::Request {
            file,
            db,
            eval,
            idle_shutdown_secs,
        } => {
            let mut command = parse_command_document(eval)?;
            if !command.contains_key("$db") {
                command.insert("$db", db.unwrap_or_else(|| "admin".to_string()));
            }

            let mut stream = connect_or_spawn_broker(&file, idle_shutdown_secs).await?;
            let response = send_command(&mut stream, command).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
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
            println!("{}", serde_json::to_string_pretty(&report)?);
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

async fn connect_or_spawn_broker(file: &Path, idle_shutdown_secs: u64) -> Result<BoxedStream> {
    let paths = broker_paths(file)?;

    if let Some(stream) = try_connect_existing(&paths).await? {
        return Ok(stream);
    }

    let child = spawn_broker(&paths.database_path, idle_shutdown_secs)?;
    wait_for_broker(&paths, child, Duration::from_secs(5)).await
}

async fn try_connect_existing(paths: &BrokerPaths) -> Result<Option<BoxedStream>> {
    if !paths.manifest_path.exists() {
        return Ok(None);
    }

    let manifest = match read_manifest(&paths.manifest_path) {
        Ok(manifest) => manifest,
        Err(_) => {
            let _ = remove_manifest(&paths.manifest_path);
            return Ok(None);
        }
    };

    match connect(&manifest.endpoint).await {
        Ok(stream) => Ok(Some(stream)),
        Err(_) => {
            let _ = remove_manifest(&paths.manifest_path);
            Ok(None)
        }
    }
}

fn spawn_broker(file: &Path, idle_shutdown_secs: u64) -> Result<Child> {
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
    Ok(child)
}

async fn wait_for_broker(
    paths: &BrokerPaths,
    mut child: Child,
    timeout: Duration,
) -> Result<BoxedStream> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(manifest) = read_manifest(&paths.manifest_path) {
            if let Ok(stream) = connect(&manifest.endpoint).await {
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

async fn send_command(stream: &mut BoxedStream, body: Document) -> Result<Document> {
    let request = OpMsg::new(1, 0, vec![PayloadSection::Body(body)]);
    write_op_msg(stream, &request).await?;
    let response = read_op_msg(stream).await?;
    response
        .body()
        .cloned()
        .ok_or_else(|| anyhow!("broker reply did not contain a body section"))
}

async fn send_checked_command(stream: &mut BoxedStream, body: Document) -> Result<Document> {
    let response = send_command(stream, body).await?;
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
    let mut stream = connect_or_spawn_broker(file, idle_shutdown_secs).await?;

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
    for sequence in 0..readable {
        let response = send_checked_command(
            &mut stream,
            doc! {
                "find": collection.as_str(),
                "filter": {
                    "_id": format!("{run_id}-{sequence}"),
                },
                "limit": 1,
                "$db": db,
            },
        )
        .await?;
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

    Ok(json!({
        "file": file.display().to_string(),
        "db": db,
        "collection": collection,
        "runId": run_id,
        "index": index_field.map(|field| {
            json!({
                "field": field,
                "unique": unique_index,
            })
        }),
        "writes": benchmark_phase_report(write_elapsed, writes, write_commands, write_batch_size),
        "reads": benchmark_phase_report(read_elapsed, readable, readable, 1),
        "totals": {
            "documents": writes + readable,
            "elapsedMs": duration_ms(total_elapsed),
            "docsPerSec": rate_per_second(writes + readable, total_elapsed),
        }
    }))
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
) -> serde_json::Value {
    json!({
        "documents": documents,
        "commands": commands,
        "batchSize": batch_size,
        "elapsedMs": duration_ms(elapsed),
        "docsPerSec": rate_per_second(documents, elapsed),
        "commandsPerSec": rate_per_second(commands, elapsed),
    })
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn rate_per_second(count: u32, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return f64::INFINITY;
    }
    f64::from(count) / seconds
}
