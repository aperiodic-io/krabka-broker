//! End-to-end proof that the full restore pipeline reacts correctly to a
//! damaged tiered-storage archive.
//!
//! `crates/restore/src/verify.rs`'s own unit tests already check each
//! corruption case in isolation, against hand-built fixture bytes. This file
//! does not repeat that: it builds a real archive the way the broker's
//! remote-log-manager actually would -- append real batches to a real
//! `krabka_log::Log`, roll a segment, and archive it through
//! `LocalTieredStorage::copy_log_segment_data`, the same path
//! `crates/remote-storage/tests/jvm_tiered_storage.rs` exercises -- then
//! damages one archived artifact on disk and drives the damage through
//! `krabka_restore::restore`, the crate's structured top-level entry point.
//! What's under test is the pipeline's *reaction*: the right
//! [`RestoreError`] variant, the right exit code, the right object named, and
//! -- under `--continue-on-corrupt` -- the right segment skipped while
//! everything else restores intact.

#[path = "support/args.rs"]
mod cli_args;

use std::path::Path as StdPath;

use assert2::{assert, check};
use bytes::Bytes;
use clap::Parser as _;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::{Log, LogConfig, name};
use krabka_protocol::records::{CRC_COVERAGE_START, Record, RecordBatch};
use krabka_remote_storage::{
    LocalTieredStorage, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentState, RemoteStorageManager as _, TopicIdPartition,
};
use krabka_restore::{Cli, EXIT_BAD_ARGUMENTS, EXIT_INTEGRITY, RestoreArgs, RestoreError, restore};
use uuid::Uuid;

krabka_macros::export_segment_data_fixture!(export_segment_data);

/// A `LogConfig` whose `segment_size` is shrunk to a 1-byte equivalent, so a
/// second `append` always rolls the first batch into its own sealed segment
/// that [`Log::tierable_segments`] will hand back.
///
/// `krabka-restore` depends on `krabka-log` but not on `krabka-units`
/// directly, so this crate has no path to name `krabka_units::ByteSize` or
/// call `krabka_units::prelude::bytes`. Scaling the default 1 GiB
/// `segment_size` down through `Div<f64>` -- implemented on every `uom`
/// quantity `krabka-units` wraps, and reachable here purely through operator
/// syntax without naming the type -- gets the same tiny segment size the
/// sibling `jvm_tiered_storage.rs` fixture builds with `bytes(1)`.
fn tiny_segment_config() -> LogConfig {
    let default = LogConfig::default();
    LogConfig {
        segment_size: default.segment_size / 1_073_741_824.0,
        ..default
    }
}

/// One record batch with a 256-byte value, long enough that a corruption
/// offset deep in the body is nowhere near the batch's boundaries.
fn long_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from(vec![b'x'; 256])),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

/// One archived segment, and the exact batches the local log held for it
/// before archiving -- what a correct restore must reproduce.
struct ArchivedSegment {
    segment_id: Uuid,
    original_batches: Vec<RecordBatch>,
}

/// Build a real, two-batch krabka log, roll it so the first batch seals into
/// its own segment, and archive that sealed segment into `archive_root`
/// through the same `LocalTieredStorage::copy_log_segment_data` call the
/// broker's remote-log-manager uses. The second (still-active) batch is never
/// tiered, matching a real broker: only sealed segments are copied.
fn archive_segment(
    archive_root: &StdPath,
    topic: &str,
    partition: i32,
    topic_id: Uuid,
    segment_id: Uuid,
) -> ArchivedSegment {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");
    log.append(&mut long_batch()).expect("append first batch");
    log.append(&mut long_batch())
        .expect("append second batch, rolling the first into a sealed segment");

    log.sync().unwrap();
    let export = log
        .tierable_segments()
        .into_iter()
        .next()
        .expect("one sealed segment after the roll");

    // The full local log holds both batches; only the sealed one (the first)
    // was archived, so the comparison fixture keeps just that range.
    let full_read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back the local log");
    let original_batches: Vec<RecordBatch> = full_read
        .batches
        .into_iter()
        .filter(|batch| {
            let last = batch.base_offset + i64::from(batch.last_offset_delta);
            batch.base_offset >= export.base_offset.0 && last <= export.last_offset.0
        })
        .collect();
    assert!(
        !original_batches.is_empty(),
        "the sealed segment's batch must survive the range filter"
    );

    let metadata = RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, topic, partition),
            segment_id,
        ),
        export.base_offset.0,
        export.last_offset.0,
        export.max_timestamp,
        1,
        0,
        RemoteLogSegmentDetails::new(
            i32::try_from(std::fs::metadata(&export.log_path).unwrap().len()).expect("fits i32"),
            RemoteLogSegmentState::CopySegmentFinished,
            maplit::btreemap! {LeaderEpoch(0) => export.base_offset.0},
        ),
    )
    .expect("valid remote metadata");

    let storage = LocalTieredStorage::new(archive_root);
    storage
        .copy_log_segment_data(
            &metadata,
            &export_segment_data(&export, ProducerSnapshotExport::Include, || {
                Bytes::from_static(b"0\n1\n0 0\n")
            }),
        )
        .expect("archive the segment");

    ArchivedSegment {
        segment_id,
        original_batches,
    }
}

/// Build `RestoreArgs` with a valid target-side flag set (`--node-id`,
/// `--standalone`, `--controller-listener`) so `format_target` succeeds and
/// the pipeline reaches segment verification, plus whatever `extra` flags a
/// scenario needs.
fn restore_args(archive_root: &StdPath, log_dir: &StdPath, extra: &[&str]) -> RestoreArgs {
    let argv = cli_args::restore_argv(
        archive_root,
        log_dir,
        cli_args::RestoreOptions {
            extra,
            ..Default::default()
        },
    );
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// Recursively collect every file under `dir`.
fn collect_files(dir: &StdPath, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Find the single file under `root` whose path satisfies `predicate`, or
/// panic with a message naming how many matches were actually found. A
/// non-unique match would make the corruption below ambiguous about which
/// archived object it hits, so this refuses to guess.
fn find_one_file(root: &StdPath, predicate: impl Fn(&StdPath) -> bool) -> std::path::PathBuf {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    let mut matches: Vec<_> = files.into_iter().filter(|p| predicate(p)).collect();
    match matches.len() {
        1 => matches.remove(0),
        n => panic!(
            "expected exactly one matching file under {}, found {n}",
            root.display()
        ),
    }
}

fn has_extension(path: &StdPath, extension: &str) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some(extension)
}

/// The relative object key `verify_segment` reports for `path`, matching how
/// `ArchiveObject::key` renders: the path under the archive root, with
/// forward slashes.
fn relative_key(archive_root: &StdPath, path: &StdPath) -> String {
    path.strip_prefix(archive_root)
        .expect("path is under the archive root")
        .to_str()
        .expect("archive paths are UTF-8")
        .replace(std::path::MAIN_SEPARATOR, "/")
}

/// Flip one byte inside a batch's CRC-covered region: at a fixed offset well
/// past both `CRC_COVERAGE_START` (21, `base_offset` and
/// `partition_leader_epoch` sit below it and could be patched freely without
/// upsetting the CRC) and the fixed 61-byte v2 batch header, landing squarely
/// in the record body. That breaks only the CRC check, not the framing or
/// any header field `verify_segment` reads before it gets to the CRC.
fn corrupt_crc_covered_byte(path: &StdPath) {
    const HEADER_LEN: usize = 61;
    const CORRUPT_OFFSET: usize = 100;
    assert!(CORRUPT_OFFSET >= CRC_COVERAGE_START);
    assert!(CORRUPT_OFFSET >= HEADER_LEN);

    let mut bytes = std::fs::read(path).expect("read log file");
    assert!(
        bytes.len() > CORRUPT_OFFSET,
        "the test batch must be long enough to corrupt a body byte"
    );
    bytes[CORRUPT_OFFSET] ^= 0xFF;
    std::fs::write(path, bytes).expect("write corrupted log file");
}

/// Truncate a `.log` file to roughly half its length, landing inside the
/// single batch it holds rather than on a batch boundary.
fn truncate_log_mid_batch(path: &StdPath) {
    let full_len = std::fs::metadata(path).expect("stat log file").len();
    assert!(
        full_len > 20,
        "the test batch must be long enough to truncate meaningfully"
    );
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open log file for truncation");
    file.set_len(full_len / 2).expect("truncate log file");
}

#[tokio::test]
async fn a_flipped_crc_byte_fails_the_whole_restore_and_names_the_object() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    archive_segment(
        archive.path(),
        "orders",
        0,
        Uuid::from_u128(1),
        Uuid::from_u128(0xC12C),
    );

    let log_path = find_one_file(archive.path(), |p| has_extension(p, "log"));
    let expected_key = relative_key(archive.path(), &log_path);
    corrupt_crc_covered_byte(&log_path);

    let args = restore_args(archive.path(), &target_parent.path().join("restored"), &[]);
    let error = restore(&args)
        .await
        .expect_err("a flipped CRC byte must fail the restore");

    let exit_code = error.exit_code();
    let RestoreError::ChecksumMismatch { key, .. } = &error else {
        panic!("expected ChecksumMismatch, got {error:?}");
    };
    check!(*key == expected_key);
    check!(exit_code == EXIT_INTEGRITY);
}

#[tokio::test]
async fn a_log_truncated_mid_batch_fails_the_whole_restore_and_names_the_object() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    archive_segment(
        archive.path(),
        "orders",
        0,
        Uuid::from_u128(2),
        Uuid::from_u128(0x7ecc),
    );

    let log_path = find_one_file(archive.path(), |p| has_extension(p, "log"));
    let expected_key = relative_key(archive.path(), &log_path);
    truncate_log_mid_batch(&log_path);

    let args = restore_args(archive.path(), &target_parent.path().join("restored"), &[]);
    let error = restore(&args)
        .await
        .expect_err("a log truncated mid-batch must fail the restore");

    let exit_code = error.exit_code();
    let RestoreError::TruncatedSegment { key, .. } = &error else {
        panic!("expected TruncatedSegment, got {error:?}");
    };
    check!(*key == expected_key);
    check!(exit_code == EXIT_INTEGRITY);
}

#[tokio::test]
async fn a_missing_timeindex_is_a_torn_copy_naming_the_right_artifact() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    let segment_id = Uuid::from_u128(0x71E5);
    let segment = archive_segment(archive.path(), "orders", 0, Uuid::from_u128(3), segment_id);

    let time_index_path = find_one_file(archive.path(), |p| has_extension(p, "timeindex"));
    std::fs::remove_file(&time_index_path).expect("remove the .timeindex artifact");

    let args = restore_args(archive.path(), &target_parent.path().join("restored"), &[]);
    let error = restore(&args)
        .await
        .expect_err("a torn copy missing .timeindex must fail the restore");

    let exit_code = error.exit_code();
    let RestoreError::TornCopy {
        topic,
        partition,
        segment_id: reported_id,
        artifact,
    } = &error
    else {
        panic!("expected TornCopy, got {error:?}");
    };
    check!(topic == "orders");
    check!(*partition == 0);
    check!(*reported_id == segment.segment_id);
    check!(artifact == ".timeindex");
    check!(exit_code == EXIT_INTEGRITY);
}

#[tokio::test]
async fn continue_on_corrupt_skips_only_the_damaged_segment_and_restores_the_rest() {
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    let log_dir = target_parent.path().join("restored");

    let clean = archive_segment(
        archive.path(),
        "orders-clean",
        0,
        Uuid::from_u128(10),
        Uuid::from_u128(0xC1EA),
    );
    let damaged = archive_segment(
        archive.path(),
        "orders-damaged",
        0,
        Uuid::from_u128(20),
        Uuid::from_u128(0xBAD5),
    );

    let damaged_log = find_one_file(archive.path(), |p| {
        has_extension(p, "log") && p.to_string_lossy().contains("orders-damaged")
    });
    corrupt_crc_covered_byte(&damaged_log);

    let args = restore_args(archive.path(), &log_dir, &["--continue-on-corrupt"]);
    let report = restore(&args)
        .await
        .expect("--continue-on-corrupt must still succeed overall");

    check!(report.skipped.len() == 1);
    let skipped = &report.skipped[0];
    check!(skipped.topic == "orders-damaged");
    check!(skipped.partition == 0);
    check!(skipped.segment_id == damaged.segment_id);
    check!(
        skipped.reason.contains("checksum"),
        "reason was {:?}",
        skipped.reason
    );

    // The clean partition reads back exactly what was archived for it.
    let clean_dir = name::partition_dir(&log_dir, "orders-clean", 0);
    let restored_clean =
        Log::open(&clean_dir, LogConfig::default()).expect("open restored clean partition");
    let read_back = restored_clean
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read restored clean partition");
    check!(read_back.batches == clean.original_batches);

    // The damaged partition's only segment was skipped before any write, so
    // `write_segment` never created a directory for it.
    let damaged_dir = name::partition_dir(&log_dir, "orders-damaged", 0);
    check!(!damaged_dir.exists());
}

#[tokio::test]
async fn continue_on_corrupt_does_not_rescue_a_format_target_failure() {
    // `--continue-on-corrupt` only turns a `verify_segment` failure into a
    // skipped segment; `format_target` runs before any segment is even
    // looked at and its error propagates unconditionally. Omitting
    // `--node-id` is the cheapest way to make `format_target` fail on a
    // command line that is otherwise completely valid, proving
    // `--continue-on-corrupt` does not paper over a target-side failure.
    let archive = tempfile::tempdir().expect("archive root");
    let target_parent = tempfile::tempdir().expect("target parent");
    archive_segment(
        archive.path(),
        "orders",
        0,
        Uuid::from_u128(30),
        Uuid::from_u128(0xF00D),
    );

    let command_line = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive.path().display().to_string(),
        "--log-dir".to_owned(),
        target_parent.path().join("restored").display().to_string(),
        "--continue-on-corrupt".to_owned(),
    ];
    let args = Cli::try_parse_from(command_line)
        .expect("valid command line")
        .args;

    let error = restore(&args)
        .await
        .expect_err("missing --node-id must fail even with --continue-on-corrupt");

    check!(matches!(error, RestoreError::InvalidArgument(_)));
    check!(error.exit_code() == EXIT_BAD_ARGUMENTS);
}
