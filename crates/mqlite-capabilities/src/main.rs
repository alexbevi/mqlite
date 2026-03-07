use std::{env, path::PathBuf, process};

use anyhow::{Context, Result, bail};
use mqlite_capabilities::{
    check_artifacts, find_repo_root, render_artifacts_from_upstream, write_artifacts,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let check = args.iter().any(|arg| arg == "--check");

    if args.is_empty() || args[0] != "sync" || args.iter().any(|arg| arg == "--help") {
        bail!("usage: cargo run -p mqlite-capabilities -- sync [--check]");
    }

    if args.len() > 2 || (args.len() == 2 && !check) {
        bail!("usage: cargo run -p mqlite-capabilities -- sync [--check]");
    }

    let current_dir = env::current_dir().context("failed to determine current directory")?;
    let repo_root = find_repo_root(&current_dir)
        .or_else(|| find_repo_root(PathBuf::from(env!("CARGO_MANIFEST_DIR")).as_path()))
        .context("failed to locate the mqlite repo root")?;
    let artifacts = render_artifacts_from_upstream(&repo_root)?;

    if check {
        check_artifacts(&repo_root, &artifacts)?;
    } else {
        write_artifacts(&repo_root, &artifacts)?;
    }

    Ok(())
}
