use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use mqlite_server::{Broker, BrokerConfig};
use mqlite_storage::DatabaseFile;

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
    }

    Ok(())
}
