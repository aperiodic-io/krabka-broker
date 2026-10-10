//! Offline point-in-time restore of a krabka cluster.
//!
//! `krabka restore` reads a KIP-405 tiered-storage archive from object storage
//! and materializes a complete, bootable krabka data directory, replayed up to
//! a bound and verified segment by segment as it rehydrates. The bound is an
//! offset, a timestamp, or a set of exclude-record predicates, so an operator
//! can recover an event-sourced system to the state it held just before a bad
//! write. Recovery of that kind is a hand-built runbook everywhere else.
//!
//! The tool runs when the cluster does not. It reads the archive through the
//! object-store layer and formats the target through [`krabka_format`], and it
//! does not depend on the broker.
//!
//! # Stages
//!
//! A restore runs five stages, and [`restore`] drives them in order:
//!
//! 1. **Discover** lists the archive and groups the keys into segments per
//!    topic partition.
//! 2. **Verify** checks the framing and the CRC of every archived segment, and
//!    derives its true end offset and maximum timestamp.
//! 3. **Bound** compiles the operator's predicates and decides which batches
//!    and records survive.
//! 4. **Materialize** formats the target log directory and writes the
//!    partition data.
//! 5. **Report** renders what happened, as text or as JSON.
//!
//! # Limits
//!
//! A batch that the bound filters is re-encoded from the records that survive,
//! so its bytes are not identical to the archived bytes. `--exclude-key` and
//! `--exclude-header` match raw bytes and decode no payload. Without
//! `--rlmm-snapshot` a segment the old cluster had marked for deletion is
//! indistinguishable from a live one. Without `--metadata-snapshot`, topic
//! configuration, ACLs, client quotas, SCRAM credentials, and finalized
//! feature levels cannot be recovered.
//!
//! # Errors and exit codes
//!
//! The library returns [`RestoreError`]. Only [`run`] and the binary map one
//! onto an exit code, so an embedding tool keeps the structured error. See
//! [`EXIT_OK`] for the codes.

use std::{ffi::OsString, path::Path};

use clap::Parser;
use krabka_remote_storage::{
    AuthenticatedArchive, Sha256Digest, TrustedManifestKeys, VerifyRequest, authenticate_archive,
    diskless::CAPTURE_HEAD_NAME,
};

mod args;
mod backend;
mod bound;
mod discover;
mod diskless;
mod error;
mod materialize;
mod report;
mod verify;

pub use self::{
    args::{
        ArchiveArgs, HeaderPattern, OffsetBound, OffsetRange, PartitionRef, RestoreArgs, TargetArgs,
    },
    backend::{ArchiveStore, object_store_config, open_archive},
    bound::{BatchDecision, Predicates, RecordDecision},
    discover::{
        ArchiveInventory, ArchiveObject, PartitionInventory, SegmentInventory,
        UNRECOGNIZED_SAMPLE_LIMIT, UnrecognizedKeys, inventory,
    },
    error::{
        EXIT_ARCHIVE_UNREADABLE, EXIT_BAD_ARGUMENTS, EXIT_DIRTY_LOG_DIR, EXIT_INTEGRITY,
        EXIT_MATERIALIZE, EXIT_OK, RestoreError,
    },
    materialize::{
        FormatTargetOutcome, SegmentOutcome, format_target, seed_rlmm_snapshot, write_segment,
    },
    report::{
        AuthenticationReport, DisklessPartitionReport, DisklessRestoreReport,
        MetadataRestoreReport, PartitionReport, ReportFormat, RestoreReport, SkippedSegment,
    },
    verify::{SegmentFacts, VerifiedSegment, verify_segment, verify_segment_authenticated},
};

/// The restore command line.
///
/// The binary and [`run_from_args`] share it, so both accept exactly the same
/// flags.
#[derive(Parser)]
// `long_about = None` keeps the struct's rustdoc out of `--help`. Without it
// clap renders the doc comment, intra-doc links and all, as the long help.
#[command(
    name = "krabka-restore",
    version,
    about = "Rebuild a bootable krabka log directory from a KIP-405 tiered-storage archive, \
             replayed to a point in time",
    long_about = None
)]
pub struct Cli {
    /// The restore's arguments, flattened so they are top-level flags.
    #[command(flatten)]
    pub args: RestoreArgs,
}

/// Run a restore and return the process exit code.
///
/// Every failure is reported on stderr and mapped onto the exit code
/// [`RestoreError::exit_code`] gives, rather than raised.
pub async fn run(args: RestoreArgs) -> i32 {
    let format = args.report;
    match restore(&args).await {
        Ok(report) => {
            if let Some(warning) = report.metadata.warning() {
                eprintln!("krabka restore: {warning}");
            }
            println!("{}", report.render(format));
            EXIT_OK
        }
        Err(error) => {
            eprintln!("krabka restore: {error}");
            error.exit_code()
        }
    }
}

/// Run a restore from an argv-style iterator and return the process exit code.
///
/// `--help` and `--version` render to stdout and return [`EXIT_OK`]. A
/// malformed command line renders to stderr and returns [`EXIT_BAD_ARGUMENTS`].
pub async fn run_from_args<I, T>(argv: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    match Cli::try_parse_from(argv) {
        Ok(cli) => run(cli.args).await,
        Err(error) => {
            let _ = error.print();
            if error.use_stderr() {
                EXIT_BAD_ARGUMENTS
            } else {
                EXIT_OK
            }
        }
    }
}

/// Run a restore and return its report.
///
/// This is the entry point for a caller that wants the structured outcome and
/// the structured error rather than an exit code.
///
/// # Errors
///
/// Returns [`RestoreError`] for a bad bound, a bound that names a topic
/// partition the archive does not hold, a target that is not empty, an
/// archive that cannot be read, a segment that fails verification, or a target
/// that rejects a write. `--continue-on-corrupt` turns a verification failure
/// into a skipped segment in the report instead of an error.
pub async fn restore(args: &RestoreArgs) -> Result<RestoreReport, RestoreError> {
    args.validate()?;
    // Check the target before the archive scan. An operator who pointed at a
    // live data directory learns it in a second, not after a full download.
    ensure_empty_log_dir(&args.target.log_dir)?;

    let store = open_archive(args)?;
    let diskless_capture = diskless::load(args.archive.diskless_wal_capture.as_deref()).await?;
    let mut archive = inventory(&store, args).await?;
    let trusted = trusted_keys(args)?;
    let has_classic = archive
        .partitions
        .iter()
        .any(|partition| !partition.segments.is_empty())
        || args.worm_expect_head.iter().any(|value| {
            value
                .split_once('=')
                .is_some_and(|(name, _)| name != CAPTURE_HEAD_NAME)
        });
    let diskless_authentication = match (&diskless_capture, &trusted) {
        (Some(capture), Some((trusted, count))) => {
            let (claims, head) = diskless::authenticate(capture, trusted, args)?;
            Some((claims, head, *count))
        }
        (None, _) => {
            if args.worm_expect_head.iter().any(|value| {
                value
                    .split_once('=')
                    .is_some_and(|(name, _)| name == CAPTURE_HEAD_NAME)
            }) {
                return Err(RestoreError::Authenticity {
                    reason: "a diskless-capture head was pinned but no --diskless-wal-capture was supplied".to_owned(),
                });
            }
            None
        }
        (Some(_), None) => None,
    };
    let diskless_claims = diskless_authentication
        .as_ref()
        .map(|(claims, _, _)| claims);
    let mut authenticated = match (&trusted, has_classic) {
        (Some((trusted, count)), true) => {
            authenticate_source(&store, args, trusted, *count, diskless_claims).await?
        }
        _ => None,
    };
    if let Some((_, head, count)) = &diskless_authentication {
        let report = authenticated.get_or_insert_with(|| {
            (
                None,
                AuthenticationReport {
                    partitions: 0,
                    manifests: 0,
                    objects: 0,
                    trusted_keys: *count,
                    chain_heads: std::collections::BTreeMap::new(),
                    tail_truncation_protected: true,
                },
            )
        });
        report.1.partitions += 1;
        report.1.manifests += 1;
        report
            .1
            .chain_heads
            .insert(CAPTURE_HEAD_NAME.to_owned(), head.clone());
    }
    let (metadata_authenticated, rlmm_authenticated) =
        authenticate_snapshots(args, diskless_capture.as_ref(), trusted.is_some()).await?;
    if rlmm_authenticated && has_classic {
        let rlmm_snapshot = args.archive.rlmm_snapshot.as_deref().ok_or_else(|| {
            RestoreError::Integrity("authenticated RLMM snapshot has no path".to_owned())
        })?;
        discover::reconcile_authenticated_with_snapshot(
            &mut archive.partitions,
            args,
            rlmm_snapshot,
        )?;
        if archive.partitions.is_empty() {
            return Err(RestoreError::EmptyArchive {
                prefix: store.prefix().unwrap_or("").to_owned(),
            });
        }
    }
    if let Some(capture) = &diskless_capture {
        diskless::add_partitions(&mut archive, capture, args)?;
    }
    if archive.partitions.is_empty() {
        return Err(RestoreError::EmptyArchive {
            prefix: store.prefix().unwrap_or("").to_owned(),
        });
    }
    let predicates = Predicates::from_args(args)?;
    let format = format_target(args, &archive).await?;
    seed_rlmm_snapshot(
        args,
        authenticated
            .as_ref()
            .and_then(|(archive, _)| archive.as_ref())
            .map(AuthenticatedArchive::report),
    )?;

    let mut consumed_authenticated_objects = std::collections::BTreeSet::new();
    if metadata_authenticated {
        consumed_authenticated_objects.insert("cluster-metadata.checkpoint".to_owned());
    }
    if rlmm_authenticated {
        consumed_authenticated_objects.insert("rlmm-snapshot".to_owned());
    }
    let mut partitions = Vec::with_capacity(archive.partitions.len());
    let mut skipped = Vec::new();
    for entry in &archive.partitions {
        if entry.segments.is_empty() {
            continue;
        }
        let mut segments = Vec::with_capacity(entry.segments.len());
        for segment in &entry.segments {
            let verification = match &authenticated {
                Some((Some(authenticated), _)) => {
                    verify_segment_authenticated(
                        &store,
                        &entry.partition,
                        segment,
                        authenticated.objects(),
                    )
                    .await
                }
                Some((None, _)) | None => verify_segment(&store, &entry.partition, segment).await,
            };
            let verified = match verification {
                Ok(verified) => verified,
                Err(error)
                    if args.continue_on_corrupt
                        && !matches!(error, RestoreError::Authenticity { .. }) =>
                {
                    skipped.push(SkippedSegment {
                        topic: entry.partition.topic.clone(),
                        partition: entry.partition.partition,
                        segment_id: segment.segment_id,
                        reason: error.to_string(),
                    });
                    continue;
                }
                Err(error) => return Err(error),
            };
            if matches!(&authenticated, Some((Some(_), _))) {
                consumed_authenticated_objects
                    .extend(segment.artifacts().map(|object| object.key.to_string()));
            }
            segments.push(write_segment(args, &entry.partition, &verified, &predicates).await?);
        }
        partitions.push(PartitionReport {
            topic: entry.partition.topic.clone(),
            partition: entry.partition.partition,
            topic_id: entry.partition.topic_id,
            segments,
        });
    }

    let diskless = match diskless_capture {
        Some(capture) => {
            let report =
                diskless::materialize(&store, args, &predicates, &capture, diskless_claims).await?;
            if diskless_claims.is_some() {
                consumed_authenticated_objects.extend(
                    capture
                        .partitions
                        .iter()
                        .filter(|partition| args.selects_topic(&partition.topic))
                        .flat_map(|partition| &partition.ranges)
                        .map(|range| range.object_key.clone()),
                );
            }
            Some(report)
        }
        None => None,
    };
    if let Some((_, report)) = &mut authenticated {
        report.objects = u64::try_from(consumed_authenticated_objects.len()).unwrap_or(u64::MAX);
    }
    Ok(RestoreReport {
        dry_run: args.dry_run,
        log_dir: args.target.log_dir.clone(),
        cluster_id: format.cluster_id,
        authentication: authenticated.map(|(_, report)| report),
        diskless,
        metadata: format.metadata,
        partitions,
        skipped,
    })
}

async fn authenticate_snapshots(
    args: &RestoreArgs,
    capture: Option<&krabka_remote_storage::diskless::DisklessWalCapture>,
    authenticated_restore: bool,
) -> Result<(bool, bool), RestoreError> {
    let metadata = authenticate_captured_snapshot(
        args.archive.metadata_snapshot.as_deref(),
        capture.and_then(|capture| capture.metadata_snapshot_sha256),
        authenticated_restore,
        "--metadata-snapshot",
    )
    .await?;
    let rlmm = authenticate_captured_snapshot(
        args.archive.rlmm_snapshot.as_deref(),
        capture.and_then(|capture| capture.rlmm_snapshot_sha256),
        authenticated_restore,
        "--rlmm-snapshot",
    )
    .await?;
    Ok((metadata, rlmm))
}

async fn authenticate_captured_snapshot(
    path: Option<&std::path::Path>,
    expected: Option<Sha256Digest>,
    authenticated_restore: bool,
    flag: &str,
) -> Result<bool, RestoreError> {
    let Some(path) = path else {
        return Ok(false);
    };
    if !authenticated_restore {
        return Ok(false);
    }
    let expected = expected.ok_or_else(|| RestoreError::Authenticity {
        reason: format!("{flag} is not bound to the signed diskless capture"),
    })?;
    let bytes = tokio::fs::read(path).await?;
    let actual = Sha256Digest::of(&bytes);
    if actual != expected {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "{flag} differs from the signed capture: expected SHA-256 {expected}, found {actual}"
            ),
        });
    }
    Ok(true)
}

async fn authenticate_source(
    store: &ArchiveStore,
    args: &RestoreArgs,
    trusted: &TrustedManifestKeys,
    trusted_keys: usize,
    diskless_claims: Option<
        &std::collections::BTreeMap<String, krabka_remote_storage::ObjectEntry>,
    >,
) -> Result<Option<(Option<AuthenticatedArchive>, AuthenticationReport)>, RestoreError> {
    let request = VerifyRequest {
        prefix: store.prefix().map(str::to_owned),
        externally_authenticated_objects: diskless_claims
            .into_iter()
            .flat_map(|claims| claims.keys().cloned())
            .collect(),
        ..VerifyRequest::default()
    };
    let authenticated = authenticate_archive(store.store(), &request, trusted)
        .await
        .map_err(|error| RestoreError::Authenticity {
            reason: error.to_string(),
        })?;
    let report = authenticated.report();
    let reason = if report.partitions.is_empty() {
        Some("no WORM manifest chain found".to_owned())
    } else if let Some(found) = report.first_break() {
        Some(format!(
            "manifest `{}`: {}",
            found.manifest_key, found.reason
        ))
    } else if !report.fully_attested() {
        Some("one or more manifests are unsigned or use an untrusted key".to_owned())
    } else if !report.ok() {
        Some("the archive contains objects outside its authenticated manifests".to_owned())
    } else if report.has_epoch_restarts() {
        Some("the archive contains an untrusted manifest-chain restart".to_owned())
    } else {
        None
    };
    if let Some(reason) = reason {
        return Err(RestoreError::Authenticity { reason });
    }
    let expected_heads = args
        .worm_expect_head
        .iter()
        .filter_map(|value| value.split_once('='))
        .filter(|(partition, _)| *partition != CAPTURE_HEAD_NAME)
        .map(|(partition, head)| (partition.to_owned(), head.to_ascii_lowercase()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let actual_heads = report
        .partitions
        .iter()
        .filter_map(|partition| {
            partition
                .head
                .map(|head| (partition.partition_dir.clone(), head.to_string()))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    if expected_heads != actual_heads {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "pinned WORM chain heads do not exactly cover authenticated partitions: expected {expected_heads:?}, found {actual_heads:?}"
            ),
        });
    }
    let coverage = AuthenticationReport {
        partitions: report.partitions.len(),
        manifests: report.manifests(),
        objects: u64::try_from(authenticated.objects().len()).unwrap_or(u64::MAX),
        trusted_keys,
        chain_heads: actual_heads,
        tail_truncation_protected: true,
    };
    Ok(Some((Some(authenticated), coverage)))
}

fn trusted_keys(args: &RestoreArgs) -> Result<Option<(TrustedManifestKeys, usize)>, RestoreError> {
    if args.archive.worm_key_id.is_empty() {
        return Ok(None);
    }
    let pairs = args
        .archive
        .worm_key_id
        .iter()
        .zip(&args.archive.worm_public_key)
        .map(|(id, path)| {
            std::fs::read(path)
                .map(|key| (id.clone(), key))
                .map_err(|error| {
                    RestoreError::InvalidArgument(format!(
                        "cannot read --worm-public-key {}: {error}",
                        path.display()
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let count = pairs.len();
    Ok(Some((TrustedManifestKeys::from_pairs(pairs), count)))
}

/// Refuse a target that already holds entries.
///
/// A restore writes a whole cluster, so it never merges into existing state.
/// An absent path is acceptable, and the formatter creates it.
fn ensure_empty_log_dir(log_dir: &Path) -> Result<(), RestoreError> {
    if !log_dir.exists() {
        return Ok(());
    }
    if std::fs::read_dir(log_dir)?.next().is_some() {
        return Err(RestoreError::LogDirNotEmpty(log_dir.to_path_buf()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::CommandFactory as _;

    use super::*;

    #[test]
    fn the_command_line_is_internally_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn an_absent_target_is_acceptable() {
        let parent = tempfile::tempdir().expect("temp dir");
        check!(ensure_empty_log_dir(&parent.path().join("missing")).is_ok());
    }

    #[test]
    fn an_empty_target_is_acceptable() {
        let target = tempfile::tempdir().expect("temp dir");
        check!(ensure_empty_log_dir(target.path()).is_ok());
    }

    #[test]
    fn a_target_that_holds_anything_is_refused() {
        let target = tempfile::tempdir().expect("temp dir");
        std::fs::write(target.path().join("meta.properties"), b"version=1\n").expect("write");
        let refused = ensure_empty_log_dir(target.path());
        check!(matches!(refused, Err(RestoreError::LogDirNotEmpty(_))));
        check!(refused.expect_err("refused").exit_code() == EXIT_DIRTY_LOG_DIR);
    }

    #[tokio::test]
    async fn help_renders_and_succeeds() {
        check!(run_from_args(["krabka-restore", "--help"]).await == EXIT_OK);
    }

    #[tokio::test]
    async fn a_malformed_command_line_is_a_bad_argument() {
        check!(run_from_args(["krabka-restore", "--not-a-flag"]).await == EXIT_BAD_ARGUMENTS);
        check!(run_from_args(["krabka-restore"]).await == EXIT_BAD_ARGUMENTS);
    }
}
