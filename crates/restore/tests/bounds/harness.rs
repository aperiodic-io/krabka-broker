//! Driving a restore the way the binary does, and reading the partition it
//! wrote back.
//!
//! Every scenario differs only in the bound flags it passes and the archive it
//! points at, so the command line, the fresh target directory, and the reopen
//! are built once here.

#[path = "../support/args.rs"]
mod cli_args;

use std::path::{Path, PathBuf};

use clap::Parser as _;
use krabka_log::{Log, LogConfig, name};
use krabka_restore::{Cli, RestoreArgs, restore};
use tempfile::TempDir;

/// Build the `RestoreArgs` every scenario shares -- a local archive, a
/// fresh target directory, and a standalone node 1, matching the shape
/// `materialize.rs`'s own tests use to satisfy `format_target`'s
/// `--node-id` requirement -- via `Cli::try_parse_from`, the same path the
/// binary and the crate's own tests use. `extra` carries the bound flags
/// under test.
pub(crate) fn restore_args(archive_dir: &Path, target_dir: &Path, extra: &[&str]) -> RestoreArgs {
    let argv = cli_args::restore_argv(
        archive_dir,
        target_dir,
        cli_args::RestoreOptions {
            extra,
            ..Default::default()
        },
    );
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// Run a restore of `archive_dir` with `extra` bound flags, into a fresh
/// empty target directory. Returns the target's `TempDir` (keep it alive
/// for as long as the returned path is read) and the target log directory
/// itself.
pub(crate) async fn run_restore(archive_dir: &Path, extra: &[&str]) -> (TempDir, PathBuf) {
    let target = tempfile::tempdir().expect("target tempdir");
    let target_dir = target.path().join("restored");
    let args = restore_args(archive_dir, &target_dir, extra);
    restore(&args).await.expect("restore");
    (target, target_dir)
}

/// Reopen the partition `restore()` wrote, the way an operator would after
/// the tool exits.
pub(crate) fn reopen(target_dir: &Path, topic: &str, partition: i32) -> Log {
    let dir = name::partition_dir(target_dir, topic, partition);
    Log::open(&dir, LogConfig::default()).expect("reopen restored partition")
}

pub(crate) fn check_batches(
    target: &Path,
    end: i64,
    expected: &[krabka_protocol::records::RecordBatch],
) {
    let log = reopen(target, "orders", 0);
    assert2::check!(log.log_end_offset() == krabka_ids::Offset(end));
    let read = log
        .read(krabka_ids::Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    assert2::check!(read.batches == expected);
}

/// A filtered first batch still claims its offsets; the following batch is unchanged.
pub(crate) fn empty_first_batch(
    fixture: &[krabka_protocol::records::RecordBatch],
) -> Vec<krabka_protocol::records::RecordBatch> {
    vec![
        krabka_protocol::records::RecordBatch {
            records: Vec::new(),
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ]
}

/// Keep both archive and restore directories alive while checking key-filtered bytes.
pub(crate) async fn restore_excluding_keys(
    fixture: &mut [krabka_protocol::records::RecordBatch],
) -> (TempDir, TempDir, PathBuf) {
    let archive = crate::archive::build_archive("orders", 0, fixture);
    let (target, dir) = run_restore(archive.path(), &["--exclude-key", "^drop"]).await;
    (archive, target, dir)
}
