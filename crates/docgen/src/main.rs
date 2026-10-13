//! The `krabka-docgen` command line.
//!
//! `all` writes the generated reference tree. `snippets` rewrites the fenced
//! code blocks of the checked-in markdown from the source regions they name.
//! CI runs `snippets` in place and fails when the working tree changes, so a
//! source edit that a documentation page quotes cannot land on its own.

use {
    clap::{Parser, Subcommand},
    std::path::PathBuf,
};

#[derive(Parser)]
#[command(name = "krabka-docgen", about = "Generate Krabka reference docs")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the full broker reference tree under --out.
    All {
        #[arg(long)]
        out: PathBuf,
    },
    /// Sync fenced code blocks in markdown from anchored source regions.
    Snippets {
        /// Markdown tree to scan (default: docs).
        #[arg(long, default_value = "docs")]
        content: PathBuf,
        /// Crates dir that snippet paths are relative to (default: crates).
        #[arg(long, default_value = "crates")]
        crates: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::All { out } => {
            krabka_docgen::emit::write_reference_tree(&out)?;
            eprintln!("wrote reference tree to {}", out.display());
            Ok(())
        }
        Command::Snippets { content, crates } => {
            let changed = krabka_docgen::sync_snippets(&content, &crates)?;
            eprintln!("synced snippets in {changed} file(s)");
            Ok(())
        }
    }
}
