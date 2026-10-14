//! The `krabka-backup` command line.
//!
//! Four subcommands, and every one of them takes the same `--archive-*` flag
//! group: a capture is written into the archive a restore reads, so the tool
//! never introduces a second place to point at.

use {
    crate::{archive::ArchiveArgs, connection::SecurityArgs},
    clap::{Args, Parser, Subcommand},
};

/// The capture selector that means "the newest capture in the archive".
pub const LATEST: &str = "latest";

/// Optional signing key for an authenticated diskless capture boundary.
#[derive(Args, Debug, Default)]
pub struct CaptureSigningArgs {
    /// Stable id of the Ed25519 key that signs the diskless capture.
    #[arg(long, value_name = "ID", requires = "worm_signing_key")]
    pub worm_signing_key_id: Option<String>,

    /// PKCS#8 Ed25519 private key used to sign the diskless capture.
    #[arg(long, value_name = "PATH", requires = "worm_signing_key_id")]
    pub worm_signing_key: Option<std::path::PathBuf>,
}

/// Independently pinned trust material for a signed diskless capture.
#[derive(Args, Debug, Default)]
pub struct CaptureTrustArgs {
    /// Trusted id of the Ed25519 key that signed the capture.
    #[arg(long, value_name = "ID", requires_all = ["worm_public_key", "worm_expect_head"])]
    pub worm_key_id: Option<String>,

    /// Raw 32-byte Ed25519 public key for `--worm-key-id`.
    #[arg(long, value_name = "PATH", requires_all = ["worm_key_id", "worm_expect_head"])]
    pub worm_public_key: Option<std::path::PathBuf>,

    /// Independently recorded head of the signed diskless capture.
    #[arg(long, value_name = "HEX", requires_all = ["worm_key_id", "worm_public_key"])]
    pub worm_expect_head: Option<String>,
}

/// Captures the inputs a point-in-time restore needs, and puts committed group
/// offsets back after one.
#[derive(Parser, Debug)]
#[command(name = "krabka-backup", version, about, long_about = None)]
pub struct Cli {
    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// One thing the tool does.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Copy this node's restore inputs into the archive.
    Capture {
        /// The broker's `log.dir`, mounted read-only. The capture reads the
        /// metadata checkpoint from its `__cluster_metadata-0`, so it finds one
        /// only when this is also the node's metadata log directory. Without
        /// it the capture takes group offsets only.
        #[arg(long, value_name = "DIR")]
        log_dir: Option<std::path::PathBuf>,

        /// A broker to read committed group offsets from. Without it the
        /// capture takes the on-disk snapshots only.
        #[arg(
            long,
            short = 'b',
            value_name = "HOST:PORT",
            env = "KRABKA_BOOTSTRAP_SERVER"
        )]
        bootstrap_server: Option<String>,

        /// TLS and SASL properties for the broker connection.
        #[command(flatten)]
        security: SecurityArgs,

        /// Optional authenticated boundary for diskless WAL recovery.
        #[command(flatten)]
        signing: CaptureSigningArgs,

        /// Where the capture goes.
        #[command(flatten, next_help_heading = "Archive")]
        archive: ArchiveArgs,
    },

    /// List the captures the archive holds.
    List {
        /// Where the captures are.
        #[command(flatten, next_help_heading = "Archive")]
        archive: ArchiveArgs,
    },

    /// Re-read a capture and check every artifact against its recorded digest.
    Verify {
        /// Which capture, or `latest`.
        #[arg(long, value_name = "ID", default_value = LATEST)]
        capture: String,

        /// Where the captures are.
        #[command(flatten, next_help_heading = "Archive")]
        archive: ArchiveArgs,
    },

    /// Commit a capture's group offsets into a restored cluster.
    RestoreOffsets {
        /// Which capture, or `latest`.
        #[arg(long, value_name = "ID", default_value = LATEST)]
        capture: String,

        /// The restored cluster.
        #[arg(
            long,
            short = 'b',
            value_name = "HOST:PORT",
            env = "KRABKA_BOOTSTRAP_SERVER"
        )]
        bootstrap_server: String,

        /// TLS and SASL properties for the restored cluster connection.
        #[command(flatten)]
        security: SecurityArgs,

        /// Report what would be committed and commit nothing.
        #[arg(long)]
        dry_run: bool,

        /// Trust boundary for offsets bound into a signed diskless capture.
        #[command(flatten)]
        trust: CaptureTrustArgs,

        /// Where the captures are.
        #[command(flatten, next_help_heading = "Archive")]
        archive: ArchiveArgs,
    },
}

#[cfg(test)]
mod tests {
    use {
        super::{Cli, Command},
        assert2::{assert, check},
        clap::Parser as _,
    };

    #[test]
    fn a_capture_takes_both_sources_and_one_archive() {
        let cli = Cli::try_parse_from([
            "krabka-backup",
            "capture",
            "--log-dir",
            "/var/lib/krabka",
            "--bootstrap-server",
            "broker-1:9092",
            "--archive-s3-bucket",
            "krabka-tier",
            "--archive-prefix",
            "prod/",
        ])
        .expect("valid command line");

        assert!(let Command::Capture { .. } = &cli.command);
        let Command::Capture {
            log_dir,
            bootstrap_server,
            archive,
            ..
        } = cli.command
        else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(log_dir == Some(std::path::PathBuf::from("/var/lib/krabka")));
        check!(bootstrap_server == Some("broker-1:9092".to_owned()));
        check!(archive.s3_bucket == Some("krabka-tier".to_owned()));
        check!(archive.prefix == Some("prod/".to_owned()));
    }

    #[yare::parameterized(
        capture = { "capture" },
        list = { "list" },
        verify = { "verify" },
        restore_offsets = { "restore-offsets" },
    )]
    fn every_subcommand_shows_the_archive_flags_under_one_heading(name: &str) {
        use clap::CommandFactory as _;

        let help = Cli::command()
            .find_subcommand(name)
            .expect("subcommand")
            .clone()
            .render_long_help()
            .to_string();
        let heading = help.find("\nArchive:\n").expect("an Archive heading");
        check!(help[heading..].contains("--archive-local"), "{name}");
        check!(help[heading..].contains("--archive-prefix"), "{name}");
    }

    #[test]
    fn a_capture_without_an_archive_is_rejected() {
        check!(Cli::try_parse_from(["krabka-backup", "capture", "--log-dir", "/data"]).is_err());
    }

    #[test]
    fn verify_and_restore_offsets_default_to_the_newest_capture() {
        let verify =
            Cli::try_parse_from(["krabka-backup", "verify", "--archive-local", "/mnt/backups"])
                .expect("valid command line");
        assert!(let Command::Verify { .. } = &verify.command);
        let Command::Verify { capture, .. } = verify.command else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(capture == "latest");

        let restore = Cli::try_parse_from([
            "krabka-backup",
            "restore-offsets",
            "-b",
            "broker-1:9092",
            "--archive-local",
            "/mnt/backups",
        ])
        .expect("valid command line");
        assert!(let Command::RestoreOffsets { .. } = &restore.command);
        let Command::RestoreOffsets {
            capture, dry_run, ..
        } = restore.command
        else {
            unreachable!("the assertion above rejected every other command")
        };
        check!(capture == "latest");
        check!(!dry_run);
    }

    #[test]
    fn restoring_offsets_needs_a_cluster_to_commit_into() {
        check!(
            Cli::try_parse_from([
                "krabka-backup",
                "restore-offsets",
                "--archive-local",
                "/mnt/backups",
            ])
            .is_err()
        );
    }

    #[test]
    fn cluster_commands_accept_kafka_command_config() {
        let capture = Cli::try_parse_from([
            "krabka-backup",
            "capture",
            "-b",
            "broker-1:9093",
            "--command-config",
            "/etc/krabka/backup-client.properties",
            "--archive-local",
            "/mnt/backups",
        ])
        .expect("capture command config");
        assert!(let Command::Capture { .. } = &capture.command);

        let restore = Cli::try_parse_from([
            "krabka-backup",
            "restore-offsets",
            "-b",
            "broker-1:9093",
            "--command-config",
            "/etc/krabka/backup-client.properties",
            "--archive-local",
            "/mnt/backups",
        ])
        .expect("restore command config");
        assert!(let Command::RestoreOffsets { .. } = &restore.command);
    }

    #[test]
    fn diskless_capture_signing_key_is_an_explicit_pair() {
        check!(
            Cli::try_parse_from([
                "krabka-backup",
                "capture",
                "-b",
                "broker:9092",
                "--archive-local",
                "/archive",
                "--worm-signing-key-id",
                "capture-key",
                "--worm-signing-key",
                "/keys/capture.pk8",
            ])
            .is_ok()
        );
        check!(
            Cli::try_parse_from([
                "krabka-backup",
                "capture",
                "-b",
                "broker:9092",
                "--archive-local",
                "/archive",
                "--worm-signing-key-id",
                "capture-key",
            ])
            .is_err()
        );
    }

    #[test]
    fn signed_offset_restore_trust_is_an_explicit_triple() {
        let base = [
            "krabka-backup",
            "restore-offsets",
            "-b",
            "broker:9092",
            "--archive-local",
            "/archive",
        ];
        check!(
            Cli::try_parse_from(base.into_iter().chain([
                "--worm-key-id",
                "capture-key",
                "--worm-public-key",
                "/keys/capture.pub",
                "--worm-expect-head",
                "00",
            ]))
            .is_ok()
        );
        check!(
            Cli::try_parse_from(base.into_iter().chain(["--worm-key-id", "capture-key"])).is_err()
        );
    }
}
