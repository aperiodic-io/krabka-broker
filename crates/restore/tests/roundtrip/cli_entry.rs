//! What `run_from_args`, the entry point the binary's `main` calls, does with
//! a whole archive: the exit code and the target it leaves behind, with and
//! without `--dry-run`.
//!
//! Both tests here drive the argv-parsing entry point rather than `restore`,
//! so they also cover the flag handling that sits in front of the pipeline.

use assert2::check;
use krabka_log::name;
use krabka_restore::{EXIT_OK, run_from_args};

use crate::fixture::build_fixture;

struct CliRestore {
    fixture: crate::fixture::Fixture,
    _target: tempfile::TempDir,
    log_dir: std::path::PathBuf,
}

async fn cli_restore(extra: &[&str]) -> CliRestore {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let code = run_from_args(crate::args::restore_argv(
        fixture.archive_root.path(),
        &log_dir,
        crate::args::RestoreOptions {
            extra,
            ..Default::default()
        },
    ))
    .await;
    check!(code == EXIT_OK);
    CliRestore {
        fixture,
        _target: target,
        log_dir,
    }
}

/// 1. Full restore round-trip through the CLI-parsing entry point.
#[tokio::test]
async fn run_from_args_restores_the_archive_and_returns_exit_ok() {
    let restored = cli_restore(&[]).await;
    check!(
        restored
            .log_dir
            .join(krabka_format::META_PROPERTIES)
            .exists()
    );
}

/// 4. `--dry-run` reports success but writes no partition data.
///
/// `restore()` still formats the target under `--dry-run` (`format_target`
/// runs unconditionally; only `write_segment`'s log-writing is skipped), so
/// `log_dir` itself, and the bootstrap files inside it, DO exist afterward --
/// exactly the shape `crates/restore/src/materialize.rs`'s own
/// `dry_run_matches_a_real_run_but_writes_nothing` unit test checks. What
/// must be absent is each partition's own data directory.
#[tokio::test]
async fn dry_run_reports_success_but_writes_no_partition_data() {
    let restored = cli_restore(&["--dry-run"]).await;
    for partition in restored.fixture.partitions() {
        let dir = name::partition_dir(&restored.log_dir, partition.topic, partition.partition);
        check!(!dir.exists(), "{}-{}", partition.topic, partition.partition);
    }
}
