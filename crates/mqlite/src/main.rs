use std::{
    io::Read,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use bson::Document;
use clap::{Parser, Subcommand};
use mqlite_ipc::{BoxedStream, BrokerPaths, broker_paths, connect, read_manifest, remove_manifest};
use mqlite_server::{Broker, BrokerConfig};
use mqlite_storage::DatabaseFile;
use mqlite_wire::{OpMsg, PayloadSection, read_op_msg, write_op_msg};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve {
            file,
            idle_shutdown_secs,
        } => {
            let broker = Broker::new(BrokerConfig::new(file, idle_shutdown_secs))?;
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

    spawn_broker(&paths.database_path, idle_shutdown_secs)?;
    wait_for_broker(&paths, Duration::from_secs(5)).await
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

fn spawn_broker(file: &Path, idle_shutdown_secs: u64) -> Result<()> {
    let current_executable =
        std::env::current_exe().context("failed to locate mqlite executable")?;
    ProcessCommand::new(current_executable)
        .args(["serve", "--file"])
        .arg(file)
        .args(["--idle-shutdown-secs", &idle_shutdown_secs.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn mqlite broker")?;
    Ok(())
}

async fn wait_for_broker(paths: &BrokerPaths, timeout: Duration) -> Result<BoxedStream> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(manifest) = read_manifest(&paths.manifest_path) {
            if let Ok(stream) = connect(&manifest.endpoint).await {
                return Ok(stream);
            }
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

async fn send_command(stream: &mut BoxedStream, body: Document) -> Result<Document> {
    let request = OpMsg::new(1, 0, vec![PayloadSection::Body(body)]);
    write_op_msg(stream, &request).await?;
    let response = read_op_msg(stream).await?;
    response
        .body()
        .cloned()
        .ok_or_else(|| anyhow!("broker reply did not contain a body section"))
}
