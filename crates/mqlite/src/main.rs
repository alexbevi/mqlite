use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, ExitStatus, Stdio},
    time::SystemTime,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use bson::{Bson, Document, doc, oid::ObjectId};
use clap::{Parser, Subcommand, ValueEnum};
use mqlite_catalog::{CollectionCatalog, CollectionRecord, apply_index_specs};
use mqlite_debug::{Component, SessionHandle, install, session};
use mqlite_ipc::{BoxedStream, BrokerPaths, broker_paths, connect, read_manifest, remove_manifest};
use mqlite_server::{Broker, BrokerConfig};
use mqlite_storage::{DatabaseFile, WalMutation};
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};
use serde_json::json;

const BACKGROUND_CHECKPOINT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20 * 60);

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
        file: Option<PathBuf>,
        #[command(subcommand)]
        command: Option<BenchCommand>,
        #[arg(long, value_enum, default_value_t = BenchmarkProfile::Legacy)]
        profile: BenchmarkProfile,
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

#[derive(Debug, Subcommand)]
enum BenchCommand {
    Seed {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, value_enum, default_value_t = BenchmarkProfile::Smoke)]
        profile: BenchmarkProfile,
        #[arg(long, default_value = "bench")]
        db: String,
        #[arg(long, default_value = "widgets")]
        collection: String,
        #[arg(long)]
        reset: bool,
        #[arg(long, default_value_t = 0)]
        dirty_wal_records: u32,
        #[arg(long)]
        allow_large: bool,
        #[arg(long)]
        allow_stress: bool,
    },
    Run {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, value_enum, default_value_t = BenchmarkProfile::Smoke)]
        profile: BenchmarkProfile,
        #[arg(long, default_value = "bench")]
        db: String,
        #[arg(long, default_value = "widgets")]
        collection: String,
        #[arg(long, default_value = "all")]
        scenario: String,
        #[arg(long, default_value_t = 100)]
        write_batch_size: u32,
        #[arg(long, default_value_t = 25)]
        checkpoint_probe_commands: u32,
        #[arg(long, default_value_t = 500)]
        checkpoint_test_delay_ms: u64,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
        #[arg(long)]
        allow_large: bool,
        #[arg(long)]
        allow_stress: bool,
    },
    TradesImport {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long, default_value = "market")]
        db: String,
        #[arg(long, default_value = "trades")]
        collection: String,
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,
        #[arg(long)]
        reset: bool,
        #[arg(long)]
        create_indexes: bool,
        #[arg(long)]
        checkpoint: bool,
        #[arg(long)]
        background_checkpoint: bool,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
    },
    TradesRead {
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value = "market")]
        db: String,
        #[arg(long, default_value = "trades")]
        collection: String,
        #[arg(long, default_value = "5597a1627df886b33f839f9b")]
        id: String,
        #[arg(long, default_value = "z300")]
        ticket: String,
        #[arg(long, default_value = "abcd")]
        ticker: String,
        #[arg(long, default_value_t = 100)]
        reads: u32,
        #[arg(long, default_value_t = 10)]
        count_reads: u32,
        #[arg(long, default_value_t = 2500)]
        expected_ticket_count: i64,
        #[arg(long, default_value_t = 60)]
        idle_shutdown_secs: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchmarkProfile {
    Legacy,
    Smoke,
    Default,
    Extended,
    Stress,
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
            command,
            profile,
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
            if let Some(command) = command {
                if let Some(session) = session.as_ref() {
                    session.insert_metadata("command", "bench");
                    match &command {
                        BenchCommand::Seed { file, .. }
                        | BenchCommand::Run { file, .. }
                        | BenchCommand::TradesImport { file, .. }
                        | BenchCommand::TradesRead { file, .. } => {
                            session.insert_metadata("file", file.display().to_string());
                        }
                    }
                }
                let bench_started = Instant::now();
                let report = match command {
                    BenchCommand::Seed {
                        file,
                        profile,
                        db,
                        collection,
                        reset,
                        dirty_wal_records,
                        allow_large,
                        allow_stress,
                    } => seed_benchmark_fixture(
                        &file,
                        BenchmarkFixtureOptions {
                            profile,
                            db: &db,
                            collection: &collection,
                            reset,
                            dirty_wal_records,
                            allow_large,
                            allow_stress,
                        },
                    )?,
                    BenchCommand::Run {
                        file,
                        profile,
                        db,
                        collection,
                        scenario,
                        write_batch_size,
                        checkpoint_probe_commands,
                        checkpoint_test_delay_ms,
                        idle_shutdown_secs,
                        allow_large,
                        allow_stress,
                    } => {
                        run_benchmark_fixture(
                            &file,
                            BenchmarkRunOptions {
                                profile,
                                db: &db,
                                collection: &collection,
                                scenario: &scenario,
                                write_batch_size,
                                checkpoint_probe_commands,
                                checkpoint_test_delay_ms,
                                idle_shutdown_secs,
                                allow_large,
                                allow_stress,
                            },
                        )
                        .await?
                    }
                    BenchCommand::TradesImport {
                        file,
                        fixture,
                        db,
                        collection,
                        batch_size,
                        reset,
                        create_indexes,
                        checkpoint,
                        background_checkpoint,
                        idle_shutdown_secs,
                    } => {
                        run_trades_import_benchmark(
                            &file,
                            TradesImportOptions {
                                fixture: &fixture,
                                db: &db,
                                collection: &collection,
                                batch_size,
                                reset,
                                create_indexes,
                                checkpoint,
                                background_checkpoint,
                                idle_shutdown_secs,
                            },
                        )
                        .await?
                    }
                    BenchCommand::TradesRead {
                        file,
                        db,
                        collection,
                        id,
                        ticket,
                        ticker,
                        reads,
                        count_reads,
                        expected_ticket_count,
                        idle_shutdown_secs,
                    } => {
                        run_trades_read_benchmark(
                            &file,
                            TradesReadOptions {
                                db: &db,
                                collection: &collection,
                                id: &id,
                                ticket: &ticket,
                                ticker: &ticker,
                                reads,
                                count_reads,
                                expected_ticket_count,
                                idle_shutdown_secs,
                            },
                        )
                        .await?
                    }
                };
                if let Some(session) = session.as_ref() {
                    session.record_duration(
                        Component::Cli,
                        "run_benchmark",
                        bench_started.elapsed(),
                    );
                }
                println!("{}", serde_json::to_string_pretty(&report)?);
                emit_local_debug_report(session.as_ref())?;
                return Ok(());
            }

            let file = file.ok_or_else(|| anyhow!("bench requires --file"))?;
            let profile = if profile == BenchmarkProfile::Legacy {
                profile
            } else {
                bail!(
                    "bench --profile requires a bench subcommand such as `bench seed` or `bench run`"
                );
            };
            let _ = profile;
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

async fn send_checked_command_with_broker_debug(
    stream: &mut BoxedStream,
    mut body: Document,
) -> Result<(Document, Option<Document>)> {
    body.insert("$mqliteDebug", true);
    let mut response = send_command(stream, body, None).await?;
    let broker_debug = response
        .remove("$mqliteDebug")
        .and_then(|value| value.as_document().cloned());
    if response.get_f64("ok").unwrap_or(0.0) == 0.0 {
        bail!(
            "{}",
            response
                .get_str("errmsg")
                .unwrap_or("mqlite benchmark command returned an error")
        );
    }
    Ok((response, broker_debug))
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

#[derive(Debug, Clone, Copy)]
struct BenchmarkProfileSpec {
    name: &'static str,
    documents: u32,
    point_reads: u32,
    payload_bytes: usize,
}

struct BenchmarkFixtureOptions<'a> {
    profile: BenchmarkProfile,
    db: &'a str,
    collection: &'a str,
    reset: bool,
    dirty_wal_records: u32,
    allow_large: bool,
    allow_stress: bool,
}

struct BenchmarkRunOptions<'a> {
    profile: BenchmarkProfile,
    db: &'a str,
    collection: &'a str,
    scenario: &'a str,
    write_batch_size: u32,
    checkpoint_probe_commands: u32,
    checkpoint_test_delay_ms: u64,
    idle_shutdown_secs: u64,
    allow_large: bool,
    allow_stress: bool,
}

struct TradesImportOptions<'a> {
    fixture: &'a Path,
    db: &'a str,
    collection: &'a str,
    batch_size: usize,
    reset: bool,
    create_indexes: bool,
    checkpoint: bool,
    background_checkpoint: bool,
    idle_shutdown_secs: u64,
}

struct TradesReadOptions<'a> {
    db: &'a str,
    collection: &'a str,
    id: &'a str,
    ticket: &'a str,
    ticker: &'a str,
    reads: u32,
    count_reads: u32,
    expected_ticket_count: i64,
    idle_shutdown_secs: u64,
}

fn benchmark_profile_spec(profile: BenchmarkProfile) -> Result<BenchmarkProfileSpec> {
    Ok(match profile {
        BenchmarkProfile::Legacy => bail!("legacy is only valid for the original bench command"),
        BenchmarkProfile::Smoke => BenchmarkProfileSpec {
            name: "smoke",
            documents: 10_000,
            point_reads: 1_000,
            payload_bytes: 96,
        },
        BenchmarkProfile::Default => BenchmarkProfileSpec {
            name: "default",
            documents: 50_000,
            point_reads: 5_000,
            payload_bytes: 384,
        },
        BenchmarkProfile::Extended => BenchmarkProfileSpec {
            name: "extended",
            documents: 250_000,
            point_reads: 10_000,
            payload_bytes: 384,
        },
        BenchmarkProfile::Stress => BenchmarkProfileSpec {
            name: "stress",
            documents: 1_000_000,
            point_reads: 25_000,
            payload_bytes: 384,
        },
    })
}

fn validate_benchmark_profile(
    profile: BenchmarkProfile,
    allow_large: bool,
    allow_stress: bool,
) -> Result<BenchmarkProfileSpec> {
    let spec = benchmark_profile_spec(profile)?;
    match profile {
        BenchmarkProfile::Extended if !allow_large => {
            bail!("extended benchmark profile requires --allow-large")
        }
        BenchmarkProfile::Stress if !allow_stress => {
            bail!("stress benchmark profile requires --allow-stress")
        }
        _ => Ok(spec),
    }
}

fn seed_benchmark_fixture(
    file: &Path,
    options: BenchmarkFixtureOptions<'_>,
) -> Result<serde_json::Value> {
    let spec =
        validate_benchmark_profile(options.profile, options.allow_large, options.allow_stress)?;
    let started = Instant::now();
    if options.reset && file.exists() {
        std::fs::remove_file(file)
            .with_context(|| format!("failed to reset benchmark fixture `{}`", file.display()))?;
    }
    if file.exists() && !options.reset {
        let info = DatabaseFile::info(file)?;
        return Ok(json!({
            "schemaVersion": 1,
            "command": "seed",
            "profile": spec.name,
            "file": file.display().to_string(),
            "db": options.db,
            "collection": options.collection,
            "reused": true,
            "elapsedMs": duration_ms(started.elapsed()),
            "storage": benchmark_storage_summary(&info),
        }));
    }

    let build_started = Instant::now();
    let mut collection = CollectionCatalog::new(doc! {});
    for record_id in 1..=u64::from(spec.documents) {
        collection
            .insert_record(CollectionRecord::new(
                record_id,
                benchmark_fixture_document(record_id as u32, spec.payload_bytes),
            ))
            .with_context(|| format!("failed to seed benchmark document {record_id}"))?;
    }
    apply_index_specs(
        &mut collection,
        &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
    )?;
    let build_elapsed = build_started.elapsed();

    let write_started = Instant::now();
    let mut database = DatabaseFile::open_or_create(file)?;
    database.commit_mutation(WalMutation::ReplaceCollection {
        database: options.db.to_string(),
        collection: options.collection.to_string(),
        collection_state: collection,
        change_events: Vec::new(),
    })?;
    database.checkpoint()?;
    if options.dirty_wal_records > 0 {
        let changes = ((spec.documents + 1)..=(spec.documents + options.dirty_wal_records))
            .map(|sequence| {
                mqlite_storage::CollectionChange::Insert(CollectionRecord::new(
                    u64::from(sequence),
                    benchmark_fixture_document(sequence, spec.payload_bytes),
                ))
            })
            .collect::<Vec<_>>();
        database.commit_mutation(WalMutation::ApplyCollectionChanges {
            database: options.db.to_string(),
            collection: options.collection.to_string(),
            create_options: None,
            changes,
            inserts: Vec::new(),
            updates: Vec::new(),
            deletes: Vec::new(),
            change_events: Vec::new(),
        })?;
    }
    drop(database);
    let write_elapsed = write_started.elapsed();
    let info = DatabaseFile::info(file)?;

    Ok(json!({
        "schemaVersion": 1,
        "command": "seed",
        "profile": spec.name,
        "file": file.display().to_string(),
        "db": options.db,
        "collection": options.collection,
        "reused": false,
        "documents": spec.documents,
        "pointReads": spec.point_reads,
        "payloadBytes": spec.payload_bytes,
        "dirtyWalRecords": options.dirty_wal_records,
        "elapsedMs": duration_ms(started.elapsed()),
        "phases": {
            "buildDocumentsMs": duration_ms(build_elapsed),
            "writeAndCheckpointMs": duration_ms(write_elapsed),
        },
        "storage": benchmark_storage_summary(&info),
    }))
}

async fn run_benchmark_fixture(
    file: &Path,
    options: BenchmarkRunOptions<'_>,
) -> Result<serde_json::Value> {
    let spec =
        validate_benchmark_profile(options.profile, options.allow_large, options.allow_stress)?;
    let scenarios = BenchmarkScenarios::parse(options.scenario)?;
    let started = Instant::now();
    let before = DatabaseFile::info(file)?;
    let metadata = if scenarios.metadata {
        Some(run_benchmark_metadata_scenario(file)?)
    } else {
        None
    };
    let mut startup = None;
    let mut point_reads = None;
    let mut writes = None;
    if scenarios.point_reads() {
        let startup_started = Instant::now();
        let mut stream = connect_or_spawn_broker(file, options.idle_shutdown_secs, None).await?;
        let startup_elapsed = startup_started.elapsed();
        startup = Some(benchmark_budget_report(
            startup_elapsed,
            BENCH_STARTUP_TARGET_MS,
        ));
        let reads = if scenarios.warm_point {
            spec.point_reads
        } else {
            1
        };
        point_reads = Some(
            run_benchmark_point_reads(&mut stream, options.db, options.collection, reads).await?,
        );
    }
    if scenarios.writes {
        let mut stream = connect_or_spawn_broker(file, options.idle_shutdown_secs, None).await?;
        writes = Some(
            run_benchmark_write_scenario(
                &mut stream,
                options.db,
                options.collection,
                &spec,
                options.write_batch_size,
            )
            .await?,
        );
    }
    let checkpoint = if scenarios.checkpoint {
        Some(run_benchmark_checkpoint_scenario(
            file,
            options.db,
            options.collection,
            &spec,
        )?)
    } else {
        None
    };
    let checkpoint_load = if scenarios.checkpoint_load {
        Some(
            run_benchmark_checkpoint_load_scenario(
                file,
                options.db,
                options.collection,
                &spec,
                options.checkpoint_probe_commands,
                options.checkpoint_test_delay_ms,
                options.idle_shutdown_secs,
            )
            .await?,
        )
    } else {
        None
    };
    let verify = if scenarios.verify {
        Some(run_benchmark_verify_scenario(file)?)
    } else {
        None
    };
    let after = DatabaseFile::info(file)?;

    Ok(json!({
        "schemaVersion": 1,
        "command": "run",
        "profile": spec.name,
        "scenario": options.scenario,
        "file": file.display().to_string(),
        "db": options.db,
        "collection": options.collection,
        "elapsedMs": duration_ms(started.elapsed()),
        "startup": startup,
        "metadata": metadata,
        "pointReads": point_reads,
        "writes": writes,
        "checkpoint": checkpoint,
        "checkpointLoad": checkpoint_load,
        "verify": verify,
        "storageBefore": benchmark_storage_summary(&before),
        "storageAfter": benchmark_storage_summary(&after),
        "targets": {
            "startupMs": BENCH_STARTUP_TARGET_MS,
            "firstPointQueryMs": BENCH_FIRST_POINT_QUERY_TARGET_MS,
        },
    }))
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkScenarios {
    metadata: bool,
    first_point: bool,
    warm_point: bool,
    writes: bool,
    checkpoint: bool,
    checkpoint_load: bool,
    verify: bool,
}

impl BenchmarkScenarios {
    fn parse(raw: &str) -> Result<Self> {
        let mut scenarios = Self {
            metadata: false,
            first_point: false,
            warm_point: false,
            writes: false,
            checkpoint: false,
            checkpoint_load: false,
            verify: false,
        };
        for part in raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            match part {
                "all" => {
                    scenarios.metadata = true;
                    scenarios.first_point = true;
                    scenarios.warm_point = true;
                }
                "metadata" => scenarios.metadata = true,
                "startup" | "first-point" | "dirty-read" => scenarios.first_point = true,
                "warm-point" => scenarios.warm_point = true,
                "writes" => scenarios.writes = true,
                "checkpoint" => scenarios.checkpoint = true,
                "checkpoint-load" | "checkpoint-concurrent" => scenarios.checkpoint_load = true,
                "verify" | "recovery-verify" => scenarios.verify = true,
                other => bail!("unsupported benchmark scenario `{other}`"),
            }
        }
        if !scenarios.metadata
            && !scenarios.first_point
            && !scenarios.warm_point
            && !scenarios.writes
            && !scenarios.checkpoint
            && !scenarios.checkpoint_load
            && !scenarios.verify
        {
            bail!("at least one benchmark scenario is required");
        }
        Ok(scenarios)
    }

    fn point_reads(self) -> bool {
        self.first_point || self.warm_point
    }
}

fn run_benchmark_metadata_scenario(file: &Path) -> Result<serde_json::Value> {
    let info_started = Instant::now();
    let info = DatabaseFile::info(file)?;
    let info_elapsed = info_started.elapsed();
    let inspect_started = Instant::now();
    let inspect = DatabaseFile::inspect(file)?;
    let inspect_elapsed = inspect_started.elapsed();
    Ok(json!({
        "infoElapsedMs": duration_ms(info_elapsed),
        "inspectElapsedMs": duration_ms(inspect_elapsed),
        "info": benchmark_storage_summary(&info),
        "inspect": {
            "checkpointGeneration": inspect.checkpoint_generation,
            "walRecordsSinceCheckpoint": inspect.wal_records_since_checkpoint,
            "walBytesSinceCheckpoint": inspect.wal_bytes_since_checkpoint,
        }
    }))
}

fn run_benchmark_verify_scenario(file: &Path) -> Result<serde_json::Value> {
    let started = Instant::now();
    let report = DatabaseFile::verify(file)?;
    let elapsed = started.elapsed();
    Ok(json!({
        "elapsedMs": duration_ms(elapsed),
        "valid": report.valid,
        "checkpointGeneration": report.checkpoint_generation,
        "pageCount": report.page_count,
        "recordCount": report.record_count,
        "indexEntryCount": report.index_entry_count,
        "walRecordsSinceCheckpoint": report.wal_records_since_checkpoint,
        "truncatedWalTail": report.truncated_wal_tail,
    }))
}

async fn run_trades_import_benchmark(
    file: &Path,
    options: TradesImportOptions<'_>,
) -> Result<serde_json::Value> {
    if options.batch_size == 0 {
        bail!("--batch-size must be greater than 0");
    }
    if options.checkpoint && options.background_checkpoint {
        bail!("--checkpoint and --background-checkpoint are mutually exclusive");
    }
    if options.reset && file.exists() {
        std::fs::remove_file(file)
            .with_context(|| format!("failed to reset benchmark database `{}`", file.display()))?;
    }

    let started = Instant::now();
    let startup_started = Instant::now();
    let mut stream = connect_or_spawn_broker(file, options.idle_shutdown_secs, None).await?;
    let startup_elapsed = startup_started.elapsed();

    let mut reader = open_trades_fixture(options.fixture)?;
    let mut line = String::new();
    let mut batch = Vec::with_capacity(options.batch_size);
    let mut documents = 0_u64;
    let mut batches = 0_u64;
    let mut parse_elapsed = Duration::ZERO;
    let mut insert_elapsed = Duration::ZERO;
    let mut insert_latencies = Vec::new();

    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .with_context(|| format!("failed to read `{}`", options.fixture.display()))?;
        if bytes == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let parse_started = Instant::now();
        let document = parse_ndjson_bson_document(line.trim_end())?;
        parse_elapsed += parse_started.elapsed();
        batch.push(Bson::Document(document));
        if batch.len() == options.batch_size {
            flush_trades_import_batch(
                &mut stream,
                options.db,
                options.collection,
                &mut batch,
                &mut documents,
                &mut batches,
                &mut insert_elapsed,
                &mut insert_latencies,
            )
            .await?;
        }
    }
    if !batch.is_empty() {
        flush_trades_import_batch(
            &mut stream,
            options.db,
            options.collection,
            &mut batch,
            &mut documents,
            &mut batches,
            &mut insert_elapsed,
            &mut insert_latencies,
        )
        .await?;
    }
    let count_started = Instant::now();
    let count_response = send_checked_command(
        &mut stream,
        doc! {
            "count": options.collection,
            "query": {},
            "$db": options.db,
        },
    )
    .await?;
    let count_elapsed = count_started.elapsed();
    let visible_count = bson_numeric_i64(
        count_response
            .get("n")
            .ok_or_else(|| anyhow!("count response missing `n`"))?,
    )
    .context("count response `n` must be numeric")? as u64;
    if visible_count != documents {
        bail!(
            "trades import sent {documents} documents but count returned {visible_count}; refusing to report a successful benchmark"
        );
    }
    let (indexes_created, index_elapsed, index_response) = if options.create_indexes {
        let index_started = Instant::now();
        let response = send_checked_command(
            &mut stream,
            doc! {
                "createIndexes": options.collection,
                "indexes": [
                    { "key": { "ticker": 1 }, "name": "ticker_1" },
                    { "key": { "ticket": 1 }, "name": "ticket_1" },
                ],
                "$db": options.db,
            },
        )
        .await?;
        let created = response
            .get_i32("numIndexesAfter")
            .ok()
            .map(|after| after >= 3)
            .unwrap_or(false);
        (created, Some(index_started.elapsed()), Some(response))
    } else {
        (false, None, None)
    };
    let (checkpointed, checkpoint_elapsed, checkpoint_response) = if options.checkpoint {
        let checkpoint_started = Instant::now();
        let checkpoint_response = send_checked_command(
            &mut stream,
            doc! {
                "mqliteCheckpoint": 1,
                "$db": "admin",
            },
        )
        .await?;
        (
            checkpoint_response
                .get_bool("checkpointed")
                .unwrap_or(false),
            Some(checkpoint_started.elapsed()),
            Some(checkpoint_response),
        )
    } else {
        (false, None, None)
    };
    let (
        background_checkpoint_queued,
        background_checkpoint_elapsed,
        background_checkpoint_response,
    ) = if options.background_checkpoint {
        let checkpoint_started = Instant::now();
        let checkpoint_response = send_checked_command(
            &mut stream,
            doc! {
                "mqliteCheckpoint": 1,
                "background": true,
                "$db": "admin",
            },
        )
        .await?;
        (
            checkpoint_response.get_bool("queued").unwrap_or(false),
            Some(checkpoint_started.elapsed()),
            Some(checkpoint_response),
        )
    } else {
        (false, None, None)
    };

    let background_checkpoint_wait_elapsed = if options.background_checkpoint {
        drop(stream);
        let wait_started = Instant::now();
        wait_for_broker_shutdown(file, BACKGROUND_CHECKPOINT_SHUTDOWN_TIMEOUT)?;
        Some(wait_started.elapsed())
    } else {
        None
    };

    let info = DatabaseFile::info(file)?;
    let completion = validate_trades_import_completion(
        &info,
        documents,
        options.create_indexes,
        options.checkpoint,
        options.background_checkpoint,
        checkpointed,
        background_checkpoint_queued,
    )?;
    let elapsed = started.elapsed();
    Ok(json!({
        "schemaVersion": 1,
        "command": "trades-import",
        "file": file.display().to_string(),
        "fixture": options.fixture.display().to_string(),
        "db": options.db,
        "collection": options.collection,
        "batchSize": options.batch_size,
        "documents": documents,
        "verifiedCount": visible_count,
        "batches": batches,
        "elapsedMs": duration_ms(elapsed),
        "docsPerSec": rate_per_second_u64(documents, elapsed),
        "startupMs": duration_ms(startup_elapsed),
        "parseMs": duration_ms(parse_elapsed),
        "insertMs": duration_ms(insert_elapsed),
        "countVerificationMs": duration_ms(count_elapsed),
        "indexesRequested": options.create_indexes,
        "indexesCreated": indexes_created,
        "indexBuildMs": index_elapsed.map(duration_ms),
        "indexBuildResponse": index_response,
        "checkpointRequested": options.checkpoint,
        "checkpointMs": checkpoint_elapsed.map(duration_ms),
        "checkpointed": checkpointed,
        "checkpointResponse": checkpoint_response,
        "backgroundCheckpointRequested": options.background_checkpoint,
        "backgroundCheckpointQueued": background_checkpoint_queued,
        "backgroundCheckpointRequestMs": background_checkpoint_elapsed.map(duration_ms),
        "backgroundCheckpointWaitMs": background_checkpoint_wait_elapsed.map(duration_ms),
        "backgroundCheckpointResponse": background_checkpoint_response,
        "completionVerified": completion.completion_verified,
        "cleanCheckpointVerified": completion.clean_checkpoint_verified,
        "insertP50Ms": duration_ms(percentile_duration(&insert_latencies, 50.0)),
        "insertP95Ms": duration_ms(percentile_duration(&insert_latencies, 95.0)),
        "insertMaxMs": duration_ms(insert_latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "storage": benchmark_storage_summary(&info),
    }))
}

#[derive(Debug, Clone, Copy)]
struct TradesImportCompletion {
    completion_verified: bool,
    clean_checkpoint_verified: bool,
}

fn validate_trades_import_completion(
    info: &mqlite_storage::InfoReport,
    documents: u64,
    create_indexes: bool,
    checkpoint_requested: bool,
    background_checkpoint_requested: bool,
    checkpointed: bool,
    background_checkpoint_queued: bool,
) -> Result<TradesImportCompletion> {
    if info.summary.record_count as u64 != documents {
        bail!(
            "trades import sent {documents} documents but final storage metadata reports {}; refusing to report a successful benchmark",
            info.summary.record_count
        );
    }
    if create_indexes && info.summary.index_count < 3 {
        bail!(
            "trades import requested secondary indexes but final storage metadata reports only {} indexes; refusing to report a successful benchmark",
            info.summary.index_count
        );
    }
    if checkpoint_requested && !checkpointed {
        bail!(
            "trades import requested a foreground checkpoint but the command did not report checkpointed=true"
        );
    }
    if background_checkpoint_requested && !background_checkpoint_queued {
        bail!(
            "trades import requested a background checkpoint but the command did not report queued=true"
        );
    }

    let clean_checkpoint_requested = checkpoint_requested || background_checkpoint_requested;
    if clean_checkpoint_requested {
        if info.wal_since_checkpoint.record_count != 0 || info.wal_since_checkpoint.bytes != 0 {
            bail!(
                "trades import requested a clean checkpoint but final storage still has {} WAL records and {} WAL bytes",
                info.wal_since_checkpoint.record_count,
                info.wal_since_checkpoint.bytes
            );
        }
        if info.last_checkpoint.record_count as u64 != documents {
            bail!(
                "trades import requested a clean checkpoint but last checkpoint reports {} records for {documents} imported documents",
                info.last_checkpoint.record_count
            );
        }
        if info.last_checkpoint.last_applied_sequence != info.last_applied_sequence {
            bail!(
                "trades import requested a clean checkpoint but last checkpoint sequence {} does not match current sequence {}",
                info.last_checkpoint.last_applied_sequence,
                info.last_applied_sequence
            );
        }
        if create_indexes && info.last_checkpoint.index_count < 3 {
            bail!(
                "trades import requested checkpointed secondary indexes but last checkpoint reports only {} indexes",
                info.last_checkpoint.index_count
            );
        }
    }

    Ok(TradesImportCompletion {
        completion_verified: true,
        clean_checkpoint_verified: clean_checkpoint_requested,
    })
}

fn wait_for_broker_shutdown(file: &Path, timeout: Duration) -> Result<()> {
    let paths = broker_paths(file)?;
    let deadline = Instant::now() + timeout;
    while paths.manifest_path.exists() {
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for mqlite broker manifest to be removed at {}",
                paths.manifest_path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

async fn run_trades_read_benchmark(
    file: &Path,
    options: TradesReadOptions<'_>,
) -> Result<serde_json::Value> {
    if options.reads == 0 {
        bail!("--reads must be greater than 0");
    }
    if options.count_reads == 0 {
        bail!("--count-reads must be greater than 0");
    }

    let started = Instant::now();
    let startup_started = Instant::now();
    let mut stream = connect_or_spawn_broker(file, options.idle_shutdown_secs, None).await?;
    let startup_elapsed = startup_started.elapsed();
    let id_find = run_trades_id_find_reads(&mut stream, &options).await?;
    let ticket_find =
        run_trades_field_find_reads(&mut stream, &options, "ticket", options.ticket).await?;
    let ticker_find =
        run_trades_field_find_reads(&mut stream, &options, "ticker", options.ticker).await?;
    let count = run_trades_ticket_count_reads(&mut stream, &options).await?;
    let info = DatabaseFile::info(file)?;

    Ok(json!({
        "schemaVersion": 1,
        "command": "trades-read",
        "file": file.display().to_string(),
        "db": options.db,
        "collection": options.collection,
        "id": options.id,
        "ticket": options.ticket,
        "ticker": options.ticker,
        "elapsedMs": duration_ms(started.elapsed()),
        "startupMs": duration_ms(startup_elapsed),
        "idFind": id_find,
        "ticketFind": ticket_find,
        "tickerFind": ticker_find,
        "ticketCount": count,
        "storage": benchmark_storage_summary(&info),
    }))
}

async fn run_trades_id_find_reads(
    stream: &mut BoxedStream,
    options: &TradesReadOptions<'_>,
) -> Result<serde_json::Value> {
    let object_id = ObjectId::parse_str(options.id).with_context(|| {
        format!(
            "--id must be a 24-character ObjectId hex string, got `{}`",
            options.id
        )
    })?;
    let mut latencies = Vec::with_capacity(options.reads as usize);
    let mut first_query_debug = None;
    let started = Instant::now();
    for index in 0..options.reads {
        let command = doc! {
            "find": options.collection,
            "filter": { "_id": Bson::ObjectId(object_id) },
            "limit": 1,
            "singleBatch": true,
            "$db": options.db,
        };
        let query_started = Instant::now();
        let response = if index == 0 {
            let (response, debug) = send_checked_command_with_broker_debug(stream, command).await?;
            first_query_debug = debug;
            response
        } else {
            send_checked_command(stream, command).await?
        };
        latencies.push(query_started.elapsed());
        let first_batch = response
            .get_document("cursor")
            .context("_id find reply missing cursor")?
            .get_array("firstBatch")
            .context("_id find reply missing firstBatch")?;
        if first_batch.len() != 1 {
            bail!(
                "trades _id find expected one document for value `{}`, got {}",
                options.id,
                first_batch.len()
            );
        }
    }

    Ok(json!({
        "reads": options.reads,
        "field": "_id",
        "value": Bson::ObjectId(object_id),
        "elapsedMs": duration_ms(started.elapsed()),
        "queriesPerSec": rate_per_second(options.reads, started.elapsed()),
        "firstQueryElapsedMs": duration_ms(*latencies.first().unwrap_or(&Duration::ZERO)),
        "p50Ms": duration_ms(percentile_duration(&latencies, 50.0)),
        "p95Ms": duration_ms(percentile_duration(&latencies, 95.0)),
        "maxMs": duration_ms(latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "firstQueryDebug": first_query_debug,
    }))
}

async fn run_trades_field_find_reads(
    stream: &mut BoxedStream,
    options: &TradesReadOptions<'_>,
    field: &str,
    value: &str,
) -> Result<serde_json::Value> {
    let mut latencies = Vec::with_capacity(options.reads as usize);
    let mut first_query_debug = None;
    let mut first_document_id = None;
    let started = Instant::now();
    for index in 0..options.reads {
        let command = doc! {
            "find": options.collection,
            "filter": { field: value },
            "limit": 1,
            "singleBatch": true,
            "$db": options.db,
        };
        let query_started = Instant::now();
        let response = if index == 0 {
            let (response, debug) = send_checked_command_with_broker_debug(stream, command).await?;
            first_query_debug = debug;
            response
        } else {
            send_checked_command(stream, command).await?
        };
        latencies.push(query_started.elapsed());
        let first_batch = response
            .get_document("cursor")
            .context("find reply missing cursor")?
            .get_array("firstBatch")
            .context("find reply missing firstBatch")?;
        if first_batch.len() != 1 {
            bail!(
                "trades {field} find expected one document for value `{value}`, got {}",
                first_batch.len()
            );
        }
        if first_document_id.is_none() {
            first_document_id = first_batch[0]
                .as_document()
                .and_then(|document| document.get("_id").cloned());
        }
    }

    Ok(json!({
        "reads": options.reads,
        "field": field,
        "value": value,
        "elapsedMs": duration_ms(started.elapsed()),
        "queriesPerSec": rate_per_second(options.reads, started.elapsed()),
        "firstQueryElapsedMs": duration_ms(*latencies.first().unwrap_or(&Duration::ZERO)),
        "p50Ms": duration_ms(percentile_duration(&latencies, 50.0)),
        "p95Ms": duration_ms(percentile_duration(&latencies, 95.0)),
        "maxMs": duration_ms(latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "firstDocumentId": first_document_id,
        "firstQueryDebug": first_query_debug,
    }))
}

async fn run_trades_ticket_count_reads(
    stream: &mut BoxedStream,
    options: &TradesReadOptions<'_>,
) -> Result<serde_json::Value> {
    let mut latencies = Vec::with_capacity(options.count_reads as usize);
    let mut first_query_debug = None;
    let started = Instant::now();
    for index in 0..options.count_reads {
        let command = doc! {
            "count": options.collection,
            "query": { "ticket": options.ticket },
            "$db": options.db,
        };
        let query_started = Instant::now();
        let response = if index == 0 {
            let (response, debug) = send_checked_command_with_broker_debug(stream, command).await?;
            first_query_debug = debug;
            response
        } else {
            send_checked_command(stream, command).await?
        };
        latencies.push(query_started.elapsed());
        let count = bson_numeric_i64(
            response
                .get("n")
                .ok_or_else(|| anyhow!("count response missing `n`"))?,
        )
        .context("count response `n` must be numeric")?;
        if count != options.expected_ticket_count {
            bail!(
                "trades ticket count expected {}, got {count}",
                options.expected_ticket_count
            );
        }
    }

    Ok(json!({
        "reads": options.count_reads,
        "expectedCount": options.expected_ticket_count,
        "elapsedMs": duration_ms(started.elapsed()),
        "queriesPerSec": rate_per_second(options.count_reads, started.elapsed()),
        "firstQueryElapsedMs": duration_ms(*latencies.first().unwrap_or(&Duration::ZERO)),
        "p50Ms": duration_ms(percentile_duration(&latencies, 50.0)),
        "p95Ms": duration_ms(percentile_duration(&latencies, 95.0)),
        "maxMs": duration_ms(latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "firstQueryDebug": first_query_debug,
    }))
}

async fn flush_trades_import_batch(
    stream: &mut BoxedStream,
    db: &str,
    collection: &str,
    batch: &mut Vec<Bson>,
    documents: &mut u64,
    batches: &mut u64,
    insert_elapsed: &mut Duration,
    insert_latencies: &mut Vec<Duration>,
) -> Result<()> {
    let batch_len = batch.len() as u64;
    let command = doc! {
        "insert": collection,
        "documents": std::mem::take(batch),
        "$db": db,
    };
    let started = Instant::now();
    send_checked_command(stream, command).await?;
    let elapsed = started.elapsed();
    *insert_elapsed += elapsed;
    insert_latencies.push(elapsed);
    *documents += batch_len;
    *batches += 1;
    Ok(())
}

fn open_trades_fixture(path: &Path) -> Result<Box<dyn BufRead>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open trades fixture `{}`", path.display()))?;
    if path.extension().and_then(|extension| extension.to_str()) == Some("zst") {
        let decoder = zstd::stream::read::Decoder::new(file)
            .with_context(|| format!("failed to open zstd fixture `{}`", path.display()))?;
        Ok(Box::new(BufReader::new(decoder)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn parse_ndjson_bson_document(line: &str) -> Result<Document> {
    let value: serde_json::Value =
        serde_json::from_str(line).context("trades fixture line must be valid JSON")?;
    bson::to_bson(&value)
        .context("failed to convert trades JSON line to BSON")?
        .as_document()
        .cloned()
        .ok_or_else(|| anyhow!("trades fixture line must be a JSON object"))
}

fn bson_numeric_i64(value: &Bson) -> Result<i64> {
    match value {
        Bson::Int32(value) => Ok(i64::from(*value)),
        Bson::Int64(value) => Ok(*value),
        Bson::Double(value) if value.fract() == 0.0 => Ok(*value as i64),
        _ => bail!("expected numeric BSON value"),
    }
}

async fn run_benchmark_point_reads(
    stream: &mut BoxedStream,
    db: &str,
    collection: &str,
    reads: u32,
) -> Result<serde_json::Value> {
    let mut latencies = Vec::with_capacity(reads as usize);
    let mut first_query_debug = None;
    let started = Instant::now();
    for index in 0..reads {
        let sequence = index + 1;
        let query_started = Instant::now();
        let command = doc! {
            "find": collection,
            "filter": { "sku": format!("sku-{sequence:08}") },
            "limit": 1,
            "$db": db,
        };
        let response = if index == 0 {
            let (response, debug) = send_checked_command_with_broker_debug(stream, command).await?;
            first_query_debug = debug;
            response
        } else {
            send_checked_command(stream, command).await?
        };
        let elapsed = query_started.elapsed();
        let first_batch = response
            .get_document("cursor")
            .context("find reply missing cursor")?
            .get_array("firstBatch")
            .context("find reply missing firstBatch")?;
        if first_batch.len() != 1 {
            bail!(
                "benchmark point read expected one document for sequence {sequence}, got {}",
                first_batch.len()
            );
        }
        latencies.push(elapsed);
    }
    let elapsed = started.elapsed();
    Ok(json!({
        "documents": reads,
        "elapsedMs": duration_ms(elapsed),
        "docsPerSec": rate_per_second(reads, elapsed),
        "firstQueryElapsedMs": duration_ms(*latencies.first().unwrap_or(&Duration::ZERO)),
        "p50Ms": duration_ms(percentile_duration(&latencies, 50.0)),
        "p95Ms": duration_ms(percentile_duration(&latencies, 95.0)),
        "maxMs": duration_ms(latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "firstQueryDebug": first_query_debug,
    }))
}

async fn run_benchmark_write_scenario(
    stream: &mut BoxedStream,
    db: &str,
    collection: &str,
    spec: &BenchmarkProfileSpec,
    write_batch_size: u32,
) -> Result<serde_json::Value> {
    if write_batch_size == 0 {
        bail!("--write-batch-size must be greater than 0");
    }
    let write_count = benchmark_write_documents(spec);
    let run_id = benchmark_run_id()?;
    let target_collection = format!("{collection}_writes_{run_id}");
    send_checked_command(
        stream,
        doc! {
            "create": target_collection.as_str(),
            "$db": db,
        },
    )
    .await?;
    send_checked_command(
        stream,
        doc! {
            "createIndexes": target_collection.as_str(),
            "indexes": Bson::Array(vec![
                Bson::Document(doc! {
                    "key": { "sku": 1 },
                    "name": "sku_1",
                    "unique": true,
                })
            ]),
            "$db": db,
        },
    )
    .await?;

    let mut latencies = Vec::new();
    let started = Instant::now();
    let mut batch_start = 0_u32;
    while batch_start < write_count {
        let batch_end = (batch_start + write_batch_size).min(write_count);
        let documents = (batch_start..batch_end)
            .map(|offset| {
                Bson::Document(benchmark_fixture_document(
                    10_000_000_u32.saturating_add(offset),
                    spec.payload_bytes,
                ))
            })
            .collect::<Vec<_>>();
        let command_started = Instant::now();
        send_checked_command(
            stream,
            doc! {
                "insert": target_collection.as_str(),
                "documents": Bson::Array(documents),
                "$db": db,
            },
        )
        .await?;
        latencies.push(command_started.elapsed());
        batch_start = batch_end;
    }
    let elapsed = started.elapsed();
    Ok(json!({
        "collection": target_collection,
        "documents": write_count,
        "commands": latencies.len(),
        "batchSize": write_batch_size,
        "elapsedMs": duration_ms(elapsed),
        "docsPerSec": rate_per_second(write_count, elapsed),
        "p50CommandMs": duration_ms(percentile_duration(&latencies, 50.0)),
        "p95CommandMs": duration_ms(percentile_duration(&latencies, 95.0)),
        "maxCommandMs": duration_ms(latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
    }))
}

async fn run_benchmark_checkpoint_load_scenario(
    file: &Path,
    db: &str,
    collection: &str,
    spec: &BenchmarkProfileSpec,
    probe_commands: u32,
    checkpoint_test_delay_ms: u64,
    idle_shutdown_secs: u64,
) -> Result<serde_json::Value> {
    if probe_commands == 0 {
        bail!("--checkpoint-probe-commands must be greater than 0");
    }

    let mut config = BrokerConfig::new(file, idle_shutdown_secs.max(1));
    config.checkpoint_interval_secs = 60;
    config.checkpoint_wal_bytes_threshold = 64;
    config.checkpoint_test_delay_ms = checkpoint_test_delay_ms;
    let broker = Broker::new(config)?;
    let paths = broker.paths().clone();
    let _ = remove_manifest(&paths.manifest_path);
    let serve_task = tokio::spawn(broker.clone().serve());
    let mut stream = connect_embedded_broker(&paths, &serve_task).await?;

    let run_id = benchmark_run_id()?;
    let trigger_started = Instant::now();
    send_checked_command(
        &mut stream,
        doc! {
            "insert": collection,
            "documents": Bson::Array(vec![Bson::Document(doc! {
                "_id": format!("checkpoint-load-{run_id}-trigger"),
                "sku": format!("checkpoint-load-{run_id}-trigger"),
                "category": "checkpoint-load",
                "qty": 1_i64,
                "active": true,
                "payload": deterministic_payload(30_000_000, spec.payload_bytes),
            })]),
            "$db": db,
        },
    )
    .await?;
    let trigger_elapsed = trigger_started.elapsed();

    let checkpoint_request_started = Instant::now();
    let checkpoint_request = send_checked_command(
        &mut stream,
        doc! { "mqliteCheckpoint": 1, "background": true, "$db": "admin" },
    )
    .await?;
    let checkpoint_request_elapsed = checkpoint_request_started.elapsed();

    let capture_started = Instant::now();
    let capture_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if broker.has_concurrent_checkpoint_for_benchmark()? {
            break;
        }
        if Instant::now() >= capture_deadline {
            drop(stream);
            let _ = tokio::time::timeout(Duration::from_secs(5), serve_task).await;
            bail!("timed out waiting for background checkpoint handoff");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let checkpoint_observed_after = capture_started.elapsed();

    let mut probe_latencies = Vec::with_capacity(probe_commands as usize);
    for _ in 0..probe_commands {
        let command_started = Instant::now();
        send_checked_command(&mut stream, doc! { "ping": 1, "$db": "admin" }).await?;
        probe_latencies.push(command_started.elapsed());
    }

    let insert_started = Instant::now();
    send_checked_command(
        &mut stream,
        doc! {
            "insert": collection,
            "documents": Bson::Array(vec![Bson::Document(doc! {
                "_id": format!("checkpoint-load-{run_id}-during"),
                "sku": format!("checkpoint-load-{run_id}-during"),
                "category": "checkpoint-load",
                "qty": 2_i64,
                "active": true,
                "payload": deterministic_payload(30_000_001, spec.payload_bytes),
            })]),
            "$db": db,
        },
    )
    .await?;
    let insert_during_checkpoint_elapsed = insert_started.elapsed();
    let still_running_after_probes = broker.has_concurrent_checkpoint_for_benchmark()?;

    drop(stream);
    tokio::time::timeout(
        Duration::from_secs(idle_shutdown_secs.max(1) + 30),
        serve_task,
    )
    .await
    .context("timed out waiting for embedded benchmark broker shutdown")?
    .context("embedded benchmark broker join failed")?
    .context("embedded benchmark broker failed")?;

    Ok(json!({
        "triggerInsertMs": duration_ms(trigger_elapsed),
        "checkpointRequestMs": duration_ms(checkpoint_request_elapsed),
        "checkpointRequest": checkpoint_request,
        "checkpointObservedAfterMs": duration_ms(checkpoint_observed_after),
        "checkpointTestDelayMs": checkpoint_test_delay_ms,
        "probeCommands": probe_commands,
        "p50CommandMs": duration_ms(percentile_duration(&probe_latencies, 50.0)),
        "p95CommandMs": duration_ms(percentile_duration(&probe_latencies, 95.0)),
        "maxCommandMs": duration_ms(probe_latencies.iter().copied().max().unwrap_or(Duration::ZERO)),
        "insertDuringCheckpointMs": duration_ms(insert_during_checkpoint_elapsed),
        "stillRunningAfterProbes": still_running_after_probes,
    }))
}

async fn connect_embedded_broker(
    paths: &BrokerPaths,
    serve_task: &tokio::task::JoinHandle<Result<()>>,
) -> Result<BoxedStream> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if serve_task.is_finished() {
            bail!(
                "embedded benchmark broker exited before writing its manifest at {}",
                paths.manifest_path.display()
            );
        }
        if let Ok(manifest) = read_manifest(&paths.manifest_path) {
            if let Ok(stream) = connect(&manifest.endpoint).await {
                return Ok(stream);
            }
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for embedded benchmark broker manifest at {}",
                paths.manifest_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn run_benchmark_checkpoint_scenario(
    file: &Path,
    db: &str,
    collection: &str,
    spec: &BenchmarkProfileSpec,
) -> Result<serde_json::Value> {
    let run_id = benchmark_run_id()?;
    let target_collection = format!("{collection}_checkpoint_{run_id}");
    let dirty_documents = benchmark_checkpoint_documents(spec);
    let build_started = Instant::now();
    let mut dirty_collection = CollectionCatalog::new(Document::new());
    for offset in 0..dirty_documents {
        let sequence = 20_000_000_u32.saturating_add(offset);
        dirty_collection.insert_record(CollectionRecord::new(
            u64::from(offset + 1),
            benchmark_fixture_document(sequence, spec.payload_bytes),
        ))?;
    }
    apply_index_specs(
        &mut dirty_collection,
        &[doc! { "key": { "sku": 1 }, "name": "sku_1", "unique": true }],
    )?;
    let build_elapsed = build_started.elapsed();

    let mut database = DatabaseFile::open_or_create(file)?;
    let mutation_started = Instant::now();
    database.commit_mutation(WalMutation::ReplaceCollection {
        database: db.to_string(),
        collection: target_collection.clone(),
        collection_state: dirty_collection,
        change_events: Vec::new(),
    })?;
    let mutation_elapsed = mutation_started.elapsed();

    let checkpoint_started = Instant::now();
    database.checkpoint()?;
    let checkpoint_elapsed = checkpoint_started.elapsed();
    let info = DatabaseFile::info(file)?;

    Ok(json!({
        "collection": target_collection,
        "dirtyDocuments": dirty_documents,
        "buildDocumentsMs": duration_ms(build_elapsed),
        "mutationElapsedMs": duration_ms(mutation_elapsed),
        "checkpointElapsedMs": duration_ms(checkpoint_elapsed),
        "storage": benchmark_storage_summary(&info),
    }))
}

fn benchmark_write_documents(spec: &BenchmarkProfileSpec) -> u32 {
    (spec.documents / 10).clamp(1_000, 10_000)
}

fn benchmark_checkpoint_documents(spec: &BenchmarkProfileSpec) -> u32 {
    (spec.documents / 10).clamp(1_000, 10_000)
}

fn benchmark_storage_summary(info: &mqlite_storage::InfoReport) -> serde_json::Value {
    json!({
        "fileSize": info.file_size,
        "lastAppliedSequence": info.last_applied_sequence,
        "databaseCount": info.summary.database_count,
        "collectionCount": info.summary.collection_count,
        "indexCount": info.summary.index_count,
        "recordCount": info.summary.record_count,
        "indexEntryCount": info.summary.index_entry_count,
        "documentBytes": info.summary.document_bytes,
        "indexBytes": info.summary.index_bytes,
        "checkpointGeneration": info.last_checkpoint.generation,
        "checkpointPageCount": info.last_checkpoint.page_count,
        "walRecords": info.wal_since_checkpoint.record_count,
        "walBytes": info.wal_since_checkpoint.bytes,
    })
}

fn benchmark_fixture_document(sequence: u32, payload_bytes: usize) -> Document {
    doc! {
        "_id": i64::from(sequence),
        "sku": format!("sku-{sequence:08}"),
        "category": format!("cat-{}", sequence % 32),
        "qty": i64::from(sequence % 10_000),
        "active": sequence % 2 == 0,
        "payload": deterministic_payload(sequence, payload_bytes),
    }
}

fn benchmark_run_id() -> Result<String> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_micros()
        .to_string())
}

fn deterministic_payload(sequence: u32, len: usize) -> String {
    let seed = format!("payload-{sequence:08}-");
    let mut payload = String::with_capacity(len);
    while payload.len() < len {
        payload.push_str(&seed);
    }
    payload.truncate(len);
    payload
}

fn percentile_duration(values: &[Duration], percentile: f64) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((percentile / 100.0) * (sorted.len().saturating_sub(1) as f64)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
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

fn rate_per_second_u64(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return f64::INFINITY;
    }
    count as f64 / seconds
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use bson::doc;
    use mqlite_storage::{InfoCheckpoint, InfoReport, InfoSummary, InfoWal};
    use serde_json::json;

    use super::{
        BenchmarkProfile, BenchmarkScenarios, benchmark_budget_report, benchmark_filter,
        validate_benchmark_profile, validate_trades_import_completion,
    };

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

    #[test]
    fn benchmark_profile_requires_explicit_large_opt_in() {
        assert!(validate_benchmark_profile(BenchmarkProfile::Smoke, false, false).is_ok());
        assert!(validate_benchmark_profile(BenchmarkProfile::Default, false, false).is_ok());
        assert!(validate_benchmark_profile(BenchmarkProfile::Extended, false, false).is_err());
        assert!(validate_benchmark_profile(BenchmarkProfile::Extended, true, false).is_ok());
        assert!(validate_benchmark_profile(BenchmarkProfile::Stress, true, false).is_err());
        assert!(validate_benchmark_profile(BenchmarkProfile::Stress, true, true).is_ok());
    }

    #[test]
    fn benchmark_scenario_parser_supports_grouped_scenarios() {
        let all = BenchmarkScenarios::parse("all").expect("all scenarios");
        assert!(all.metadata);
        assert!(all.first_point);
        assert!(all.warm_point);
        assert!(!all.writes);
        assert!(!all.checkpoint);
        assert!(!all.verify);

        let selected = BenchmarkScenarios::parse(
            "metadata,first-point,writes,checkpoint,checkpoint-load,verify",
        )
        .expect("selected scenarios");
        assert!(selected.metadata);
        assert!(selected.first_point);
        assert!(!selected.warm_point);
        assert!(selected.writes);
        assert!(selected.checkpoint);
        assert!(selected.checkpoint_load);
        assert!(selected.verify);
        let dirty = BenchmarkScenarios::parse("dirty-read").expect("dirty-read scenario");
        assert!(!dirty.metadata);
        assert!(dirty.first_point);
        assert!(!dirty.warm_point);
        assert!(BenchmarkScenarios::parse("unknown").is_err());
    }

    #[test]
    fn trades_import_completion_rejects_stale_checkpoint_metadata() {
        let mut info = trades_import_info(236_000, 0, 3);
        info.summary.record_count = 1_000_001;
        info.last_applied_sequence = 1002;
        info.last_checkpoint.last_applied_sequence = 236;

        let err =
            validate_trades_import_completion(&info, 1_000_001, true, true, false, true, false)
                .expect_err("stale checkpoint should fail");

        assert!(
            err.to_string()
                .contains("last checkpoint reports 236000 records")
        );
    }

    #[test]
    fn trades_import_completion_rejects_dirty_checkpoint_result() {
        let mut info = trades_import_info(3, 2, 3);
        info.summary.record_count = 3;

        let err = validate_trades_import_completion(&info, 3, true, false, true, false, true)
            .expect_err("dirty WAL should fail");

        assert!(
            err.to_string()
                .contains("final storage still has 2 WAL records")
        );
    }

    fn trades_import_info(
        checkpoint_record_count: usize,
        wal_records: usize,
        index_count: usize,
    ) -> InfoReport {
        InfoReport {
            path: PathBuf::from("/tmp/test.mongodb"),
            file_format_version: 2,
            file_size: 4096,
            last_applied_sequence: 1002,
            summary: InfoSummary {
                database_count: 1,
                collection_count: 1,
                index_count,
                record_count: checkpoint_record_count + wal_records,
                index_entry_count: 0,
                change_event_count: 0,
                plan_cache_entry_count: 0,
                document_bytes: 0,
                index_bytes: 0,
                total_bytes: 0,
            },
            last_checkpoint: InfoCheckpoint {
                generation: 2,
                last_applied_sequence: 1002,
                last_checkpoint_unix_ms: 0,
                active_superblock_slot: 0,
                valid_superblocks: 2,
                database_count: 1,
                collection_count: 1,
                index_count,
                snapshot_offset: 0,
                snapshot_len: 0,
                wal_offset: 0,
                page_size: 8192,
                page_count: 1,
                page_bytes: 8192,
                record_page_count: 1,
                record_page_bytes: 8192,
                index_page_count: 1,
                index_page_bytes: 8192,
                change_event_page_count: 0,
                change_event_page_bytes: 0,
                record_count: checkpoint_record_count,
                index_entry_count: 0,
                change_event_count: 0,
                plan_cache_entry_count: 0,
                total_bytes: 8192,
            },
            wal_since_checkpoint: InfoWal {
                record_count: wal_records,
                bytes: if wal_records == 0 { 0 } else { 1024 },
                truncated_tail: false,
            },
            databases: Vec::new(),
        }
    }
}
