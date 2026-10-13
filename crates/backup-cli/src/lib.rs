//! Captures the inputs a point-in-time restore needs, and puts committed
//! consumer-group offsets back after one.
//!
//! `krabka restore` rebuilds a log directory out of a KIP-405 archive, and it
//! needs three things the archive does not hold. Two are files on a broker's
//! own disk: `<log.dir>/remote-log-metadata/snapshot`, without which a segment
//! the old cluster had already released is indistinguishable from a live one,
//! and the controller's newest `<end-offset>-<epoch>.checkpoint`, which is
//! where topic configuration, ACLs, client quotas, SCRAM credentials and
//! finalized feature levels live. The third is not a file at all: the committed
//! offsets of every consumer group, which sit in the compacted, never-tiered
//! `__consumer_offsets` topic.
//!
//! All three are destroyed by the disaster that makes an operator reach for a
//! restore, so a copy has to exist before it. This tool makes that copy and
//! checks it.
//!
//! # Why a tool and not a `kubectl exec`
//!
//! The krabka container image is built from an apko base and has no shell, no
//! `cp` and no `tar`, so `kubectl exec` and `kubectl cp` cannot take a file off
//! a running broker. The supported copy is this binary, run beside the broker
//! with the same volume mounted read-only — a `CronJob` in the same namespace —
//! or on the node itself. Both snapshot files are written temp-then-rename, so
//! a reader that opens one by name always sees a whole file.
//!
//! Group offsets need a cluster rather than a volume, so the same run takes
//! them over the ordinary Kafka wire protocol, with `ListGroups` and
//! `OffsetFetch`. Nothing here speaks a krabka-private API key, which is what
//! lets the same capture run against a cluster that is not krabka.
//!
//! # The four subcommands
//!
//! `capture` copies the inputs into the archive under
//! `restore-inputs/<capture-id>/`, beside the tiered segments the restore reads,
//! with a `manifest.json` recording each artifact's size and SHA-256. `list`
//! names the captures. `verify` re-reads one and checks it against those
//! digests, which is the check a backup is worth nothing without. `restore-
//! offsets` commits a capture's offsets into a restored cluster, so a group
//! resumes where it stopped instead of at `auto.offset.reset`.
//!
//! # Exit codes
//!
//! `0` success, [`EXIT_BAD_ARGUMENT`], [`EXIT_UNREADABLE`], [`EXIT_INTEGRITY`]
//! and [`EXIT_CLUSTER`]. They line up with `krabka restore`'s own where the two
//! tools mean the same thing, because one runbook branches on both.
//!
//! The crate is a library as well as a binary, for the same reason
//! `krabka-guard`, `krabka-barrier` and `krabka-format` are: a test that spawns
//! the binary needs a Cargo working tree to build it from, and a Bazel test
//! sandbox has none. Tests call [`run_from_args`] in process instead.

use clap::Parser as _;

pub mod archive;
pub mod capture;
pub mod cli;
pub mod connection;
pub mod diskless;
pub mod error;
pub mod manifest;
pub mod offsets;
pub mod run;

pub use self::{
    cli::{Cli, Command},
    error::{BackupError, EXIT_BAD_ARGUMENT, EXIT_CLUSTER, EXIT_INTEGRITY, EXIT_UNREADABLE},
};

krabka_macros::parsed_cli_entrypoint!({
    /// Run the tool from an argv-style iterator, returning its exit code.
    ///
    /// # Panics
    ///
    /// Panics if `argv` does not parse, which for a caller passing a literal
    /// argument list is a bug in that list rather than a runtime condition.
});

/// Run one parsed command line, mapping its error onto an exit code.
pub async fn run(cli: Cli) -> i32 {
    let outcome = match &cli.command {
        Command::Capture {
            log_dir,
            bootstrap_server,
            security,
            signing,
            archive,
        } => run::capture_configured(
            log_dir.as_deref(),
            bootstrap_server.as_deref(),
            security,
            signing,
            archive,
        )
        .await
        .map(|_| ()),
        Command::List { archive } => run::list(archive).await.map(|_| ()),
        Command::Verify { capture, archive } => run::verify(capture, archive).await,
        Command::RestoreOffsets {
            capture,
            bootstrap_server,
            security,
            dry_run,
            trust,
            archive,
        } => run::restore_offsets_configured(
            capture,
            bootstrap_server,
            security,
            trust,
            *dry_run,
            archive,
        )
        .await
        .map(|_| ()),
    };
    match outcome {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error}");
            error.exit_code()
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            Cli, Command, EXIT_BAD_ARGUMENT, EXIT_INTEGRITY, EXIT_UNREADABLE, run, run_from_args,
        },
        crate::{
            capture::capture_key,
            manifest::{MANIFEST, RLMM_SNAPSHOT},
        },
        assert2::check,
        clap::Parser as _,
    };

    // A log directory holding the RLMM snapshot a capture takes off a node.
    krabka_macros::snapshot_node_fixture!(node);

    /// The one capture id the archive holds, which `latest` also resolves to.
    fn only_capture(archive_root: &std::path::Path) -> String {
        let mut ids: Vec<String> = std::fs::read_dir(archive_root.join("restore-inputs"))
            .expect("read the capture root")
            .flatten()
            .filter_map(|entry| entry.file_name().to_str().map(ToOwned::to_owned))
            .collect();
        ids.sort();
        ids.pop().expect("the archive holds a capture")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_and_the_checks_that_read_it_back_all_exit_zero() {
        let node = node();
        let archive_root = tempfile::tempdir().expect("archive root");
        let log_dir = node.path().display().to_string();
        let archive = archive_root.path().display().to_string();

        check!(
            run_from_args([
                "krabka-backup",
                "capture",
                "--log-dir",
                &log_dir,
                "--archive-local",
                &archive,
            ])
            .await
                == 0
        );
        check!(run_from_args(["krabka-backup", "list", "--archive-local", &archive]).await == 0);
        check!(run_from_args(["krabka-backup", "verify", "--archive-local", &archive]).await == 0);
        check!(
            run_from_args([
                "krabka-backup",
                "restore-offsets",
                "--dry-run",
                "-b",
                "127.0.0.1:9092",
                "--capture",
                &only_capture(archive_root.path()),
                "--archive-local",
                &archive,
            ])
            .await
                == EXIT_UNREADABLE,
            "a capture of the on-disk inputs alone holds no offsets to restore",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_damaged_capture_exits_with_the_integrity_code_a_runbook_branches_on() {
        let node = node();
        let archive_root = tempfile::tempdir().expect("archive root");
        let archive = archive_root.path().display().to_string();
        check!(
            run_from_args([
                "krabka-backup",
                "capture",
                "--log-dir",
                &node.path().display().to_string(),
                "--archive-local",
                &archive,
            ])
            .await
                == 0
        );

        let id = only_capture(archive_root.path());
        std::fs::write(
            archive_root.path().join(capture_key(&id, RLMM_SNAPSHOT)),
            b"truncated",
        )
        .expect("truncate the captured snapshot");

        check!(
            run_from_args(["krabka-backup", "verify", "--archive-local", &archive]).await
                == EXIT_INTEGRITY
        );

        // And an archive whose manifest is gone is unreadable rather than
        // corrupt: the two codes send a runbook down different branches.
        std::fs::remove_file(archive_root.path().join(capture_key(&id, MANIFEST)))
            .expect("remove the manifest");
        check!(
            run_from_args(["krabka-backup", "verify", "--archive-local", &archive]).await
                == EXIT_UNREADABLE
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_finds_nothing_exits_unreadable() {
        let empty = tempfile::tempdir().expect("an empty log dir");
        let archive_root = tempfile::tempdir().expect("archive root");
        check!(
            run_from_args([
                "krabka-backup",
                "capture",
                "--log-dir",
                &empty.path().display().to_string(),
                "--archive-local",
                &archive_root.path().display().to_string(),
            ])
            .await
                == EXIT_UNREADABLE
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_flag_that_names_a_backend_that_was_not_selected_exits_bad_argument() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let cli = Cli::parse_from([
            "krabka-backup",
            "list",
            "--archive-local",
            &archive_root.path().display().to_string(),
            "--archive-s3-region",
            "eu-west-1",
        ]);
        check!(let Command::List { .. } = &cli.command);
        check!(run(cli).await == EXIT_BAD_ARGUMENT);
    }
}
