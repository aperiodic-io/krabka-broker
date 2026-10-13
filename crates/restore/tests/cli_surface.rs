//! End-to-end checks of the `krabka-restore` command-line surface.
//!
//! Each test runs the binary as a subprocess, so it covers the clap surface,
//! the argument validation, and the exit codes an operator's runbook branches
//! on. The pipeline stages are not reached: every case here stops on a flag or
//! on the target directory.

use {
    assert2::check,
    krabka_restore::{
        ArchiveArgs, EXIT_ARCHIVE_UNREADABLE, EXIT_BAD_ARGUMENTS, EXIT_DIRTY_LOG_DIR, EXIT_OK,
        ReportFormat, RestoreArgs, TargetArgs,
    },
    std::{path::Path, process::Command},
};

fn run(args: &[&str]) -> std::process::Output {
    // `CARGO_BIN_EXE_<bin>` is set because the binary is in this package.
    let bin = env!("CARGO_BIN_EXE_krabka-restore");
    Command::new(bin)
        .args(args)
        .output()
        .expect("run krabka-restore")
}

fn code(output: &std::process::Output) -> i32 { output.status.code().expect("exit code") }

#[test]
fn help_renders_every_flag_group() {
    let output = run(&["--help"]);
    check!(code(&output) == EXIT_OK);
    let help = String::from_utf8(output.stdout).expect("utf-8 help");
    for heading in [
        "Archive source",
        "Target cluster",
        "Selection and bounds",
        "Behaviour",
    ] {
        check!(help.contains(heading), "{heading}");
    }
    for flag in [
        "--archive-local",
        "--archive-s3-bucket",
        "--archive-s3-region",
        "--archive-s3-endpoint",
        "--archive-s3-access-key-id",
        "--archive-s3-secret-access-key",
        "--archive-s3-allow-http",
        "--archive-gcs-bucket",
        "--archive-gcs-service-account-path",
        "--archive-gcs-endpoint",
        "--archive-gcs-allow-http",
        "--archive-prefix",
        "--rlmm-snapshot",
        "--metadata-snapshot",
        "--worm-key-id",
        "--worm-public-key",
        "--log-dir",
        "--cluster-id",
        "--node-id",
        "--standalone",
        "--initial-controllers",
        "--no-initial-controllers",
        "--controller-listener",
        "--topic",
        "--to-offset",
        "--to-timestamp",
        "--exclude-key",
        "--exclude-header",
        "--exclude-producer-id",
        "--exclude-offset",
        "--dry-run",
        "--report",
        "--continue-on-corrupt",
    ] {
        check!(help.contains(flag), "{flag}");
    }
}

#[test]
fn an_unknown_flag_is_a_bad_argument() {
    let output = run(&["--not-a-flag"]);
    check!(code(&output) == EXIT_BAD_ARGUMENTS);
    check!(!output.stderr.is_empty());
}

#[test]
fn two_bounds_on_one_partition_are_a_bad_argument() {
    let archive = tempfile::tempdir().expect("temp dir");
    let target = tempfile::tempdir().expect("temp dir");
    let output = run(&[
        "--archive-local",
        &archive.path().display().to_string(),
        "--log-dir",
        &target.path().join("restored").display().to_string(),
        "--to-offset",
        "orders:0=10",
        "--to-offset",
        "orders:0=20",
    ]);
    check!(code(&output) == EXIT_BAD_ARGUMENTS);
}

#[test]
fn a_non_empty_target_is_refused_before_the_archive_is_read() {
    let archive = tempfile::tempdir().expect("temp dir");
    let target = tempfile::tempdir().expect("temp dir");
    std::fs::write(target.path().join("meta.properties"), b"version=1\n").expect("write");
    let output = run(&[
        "--archive-local",
        &archive.path().display().to_string(),
        "--log-dir",
        &target.path().display().to_string(),
    ]);
    check!(code(&output) == EXIT_DIRTY_LOG_DIR);
}

#[test]
fn an_archive_that_cannot_be_opened_is_reported_as_unreadable() {
    let target = tempfile::tempdir().expect("temp dir");
    let missing = target.path().join("no-such-archive");
    let output = run(&[
        "--archive-local",
        &missing.display().to_string(),
        "--log-dir",
        &target.path().join("restored").display().to_string(),
    ]);
    check!(code(&output) == EXIT_ARCHIVE_UNREADABLE);
}

#[test]
fn restore_args_are_constructible_without_an_argv() {
    let args = RestoreArgs {
        archive: ArchiveArgs {
            location: krabka_object_store::ArchiveArgs {
                local: Some(Path::new("/archive").to_path_buf()),
                s3_bucket: None,
                s3_region: None,
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_allow_http: false,
                gcs_bucket: None,
                gcs_service_account_path: None,
                gcs_endpoint: None,
                gcs_allow_http: false,
                prefix: Some("tier".to_owned()),
            },
            rlmm_snapshot: None,
            metadata_snapshot: None,
            diskless_wal_capture: None,
            worm_key_id: Vec::new(),
            worm_public_key: Vec::new(),
        },
        worm_expect_head: Vec::new(),
        target: TargetArgs {
            log_dir: Path::new("/target").to_path_buf(),
            cluster_id: None,
            node_id: None,
            standalone: true,
            initial_controllers: Vec::new(),
            no_initial_controllers: false,
            controller_listener: Some("127.0.0.1:9093".to_owned()),
        },
        topic: vec!["orders".to_owned()],
        to_offset: Vec::new(),
        to_timestamp: None,
        exclude_key: Vec::new(),
        exclude_header: Vec::new(),
        exclude_producer_id: Vec::new(),
        exclude_offset: Vec::new(),
        dry_run: true,
        report: ReportFormat::Json,
        continue_on_corrupt: false,
    };
    check!(args.validate().is_ok());
    check!(args.selects_topic("orders"));
    check!(!args.selects_topic("payments"));
}
