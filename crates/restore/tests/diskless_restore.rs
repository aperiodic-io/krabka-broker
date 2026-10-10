use assert2::check;
use bytes::{Bytes, BytesMut};
use clap::Parser as _;
use krabka_audit::FileEd25519Signer;
use krabka_ids::Offset;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch, validate_one_v2_batch};
use krabka_remote_storage::{
    ObjectEntry, RlmmCacheDump, Sha256Digest,
    diskless::{
        CAPTURE_HEAD_NAME, CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture,
        WalIndexEntry, WalObjectBuilder,
    },
};
use krabka_remote_storage_topic::snapshot::Snapshot;
use krabka_restore::{Cli, RestoreError, restore};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use uuid::Uuid;

fn fixture(root: &std::path::Path, object: bool, corrupt: bool) -> (DisklessWalCapture, Uuid) {
    fixture_with_second_base(root, object, corrupt, 1)
}

fn fixture_with_second_base(
    root: &std::path::Path,
    object: bool,
    corrupt: bool,
    second_base: i64,
) -> (DisklessWalCapture, Uuid) {
    let topic_id = Uuid::from_u128(44);
    let mut first = BytesMut::new();
    RecordBatch {
        base_offset: 0,
        records: vec![Record {
            value: Some(Bytes::from_static(b"deleted")),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode(&mut first)
    .unwrap();
    let mut second = BytesMut::new();
    RecordBatch {
        base_offset: second_base,
        records: vec![Record {
            value: Some(Bytes::from_static(b"kept")),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode(&mut second)
    .unwrap();
    let mut encoded = first.clone();
    encoded.extend_from_slice(&second);
    let mut builder = WalObjectBuilder::new();
    builder.append_run(topic_id, 0, 0, second_base, &encoded);
    let bytes = builder.finish();
    let manifest = krabka_remote_storage::diskless::parse_wal_object(&bytes)
        .unwrap()
        .remove(0);
    if object {
        let path = root.join("diskless-wal/1");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(
            path.join("a.ckwl"),
            if corrupt {
                b"broken".as_slice()
            } else {
                &bytes
            },
        )
        .unwrap();
    }
    let capture = DisklessWalCapture {
        format_version: 1,
        captured_at_ms: 42,
        source_cutoffs: vec![9],
        partitions: vec![DisklessPartitionCapture {
            topic: "orders".into(),
            topic_id,
            partition: 0,
            delete_floor: 1,
            recovery_cutoff: second_base + 1,
            ranges: vec![CapturedWalRange {
                object_key: "diskless-wal/1/a.ckwl".into(),
                entry: WalIndexEntry {
                    topic_id,
                    partition: 0,
                    first_offset: 0,
                    last_offset: second_base,
                    byte_start: manifest.byte_start,
                    byte_len: u32::try_from(encoded.len()).unwrap(),
                    max_timestamp_ms: 0,
                },
            }],
        }],
        metadata_snapshot_sha256: None,
        rlmm_snapshot_sha256: None,
        group_offsets_sha256: None,
        authentication: None,
    };
    (capture, topic_id)
}

fn capture_args(
    archive: &std::path::Path,
    target: &std::path::Path,
    capture: &std::path::Path,
    extra: &[String],
) -> krabka_restore::RestoreArgs {
    let mut argv = vec![
        "restore".to_owned(),
        "--archive-local".to_owned(),
        archive.display().to_string(),
        "--log-dir".to_owned(),
        target.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:19093".to_owned(),
        "--diskless-wal-capture".to_owned(),
        capture.display().to_string(),
    ];
    argv.extend_from_slice(extra);
    Cli::parse_from(argv).args
}

fn args(
    archive: &std::path::Path,
    target: &std::path::Path,
    capture: &std::path::Path,
) -> krabka_restore::RestoreArgs {
    capture_args(archive, target, capture, &[])
}

fn bounded_args(
    archive: &std::path::Path,
    target: &std::path::Path,
    capture: &std::path::Path,
) -> krabka_restore::RestoreArgs {
    capture_args(
        archive,
        target,
        capture,
        &["--to-offset".into(), "orders:0=0".into()],
    )
}

fn trusted_args(
    archive: &std::path::Path,
    target: &std::path::Path,
    capture: &std::path::Path,
    public_key: &std::path::Path,
    head: &str,
) -> krabka_restore::RestoreArgs {
    capture_args(
        archive,
        target,
        capture,
        &[
            "--worm-key-id".to_owned(),
            "capture-key".to_owned(),
            "--worm-public-key".to_owned(),
            public_key.display().to_string(),
            "--worm-expect-head".to_owned(),
            format!("{CAPTURE_HEAD_NAME}={head}"),
        ],
    )
}

fn write_capture(path: &std::path::Path, capture: &DisklessWalCapture) {
    std::fs::write(path, serde_json::to_vec(capture).unwrap()).unwrap();
}

/// Open the two test directories, build the selected WAL and publish its capture.
fn restore_fixture(
    object: bool,
    corrupt: bool,
) -> (tempfile::TempDir, tempfile::TempDir, std::path::PathBuf) {
    let (archive, target, capture_path, capture) = unwritten_fixture(object, corrupt);
    write_capture(&capture_path, &capture);
    (archive, target, capture_path)
}

fn check_orders_offsets(target: &std::path::Path, start: Offset, end: Offset) {
    let log = Log::open(target.join("orders-0"), LogConfig::default()).unwrap();
    check!(log.log_start_offset() == start);
    check!(log.log_end_offset() == end);
}

fn unwritten_fixture(
    object: bool,
    corrupt: bool,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
    DisklessWalCapture,
) {
    let archive = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let capture_path = archive.path().join("capture.json");
    let (capture, _) = fixture(archive.path(), object, corrupt);
    (archive, target, capture_path, capture)
}

async fn dry_run_error(
    archive: &std::path::Path,
    target: &std::path::Path,
    capture: &std::path::Path,
) -> RestoreError {
    let mut options = args(archive, target, capture);
    options.dry_run = true;
    restore(&options).await.unwrap_err()
}

#[tokio::test]
async fn referenced_wal_restores_at_original_offsets_and_orphans_are_ignored() {
    let (archive, target, capture_path) = restore_fixture(true, false);
    std::fs::write(archive.path().join("orphan.ckwl"), b"uncommitted garbage").unwrap();
    let report = restore(&args(archive.path(), target.path(), &capture_path))
        .await
        .unwrap();
    check!(report.diskless.as_ref().unwrap().partitions[0].recovery_cutoff == 2);
    check_orders_offsets(target.path(), Offset(1), Offset(2));
}

#[tokio::test]
async fn diskless_restore_rejects_an_offset_bound_below_the_delete_floor() {
    let (archive, target, capture_path) = restore_fixture(true, false);

    let error = restore(&bounded_args(archive.path(), target.path(), &capture_path))
        .await
        .unwrap_err();
    check!(matches!(error, RestoreError::InvalidArgument(_)));
}

#[tokio::test]
async fn diskless_capture_that_selects_nothing_is_an_empty_archive() {
    let (archive, target, capture_path) = restore_fixture(false, false);
    let mut args = args(archive.path(), target.path(), &capture_path);
    args.topic = vec!["payments".to_owned()];

    let error = restore(&args).await.unwrap_err();
    check!(matches!(error, RestoreError::EmptyArchive { .. }));
}

#[tokio::test]
async fn diskless_restore_accepts_complete_batch_subranges_of_a_footer_run() {
    let (archive, target, capture_path, mut capture) = unwritten_fixture(true, false);
    let range = &mut capture.partitions[0].ranges[0].entry;
    let object = std::fs::read(archive.path().join("diskless-wal/1/a.ckwl")).unwrap();
    let start = usize::try_from(range.byte_start).unwrap();
    let first_len = validate_one_v2_batch(&object[start..]).unwrap().total_len;
    range.first_offset = 1;
    range.byte_start += u64::try_from(first_len).unwrap();
    range.byte_len -= u32::try_from(first_len).unwrap();
    write_capture(&capture_path, &capture);

    let report = restore(&args(archive.path(), target.path(), &capture_path))
        .await
        .unwrap();
    check!(report.diskless.as_ref().unwrap().partitions[0].records == 1);
    check_orders_offsets(target.path(), Offset(1), Offset(2));
}

#[tokio::test]
async fn diskless_restore_accepts_ranges_wholly_below_the_delete_floor() {
    let (archive, target, capture_path, mut capture) = unwritten_fixture(true, false);
    capture.partitions[0].delete_floor = 2;
    capture.partitions[0].recovery_cutoff = 2;
    write_capture(&capture_path, &capture);

    let report = restore(&args(archive.path(), target.path(), &capture_path))
        .await
        .unwrap();

    let diskless = &report.diskless.as_ref().unwrap().partitions[0];
    check!(diskless.recovery_cutoff == 2);
    check!(diskless.records == 0);
    let log = Log::open(target.path().join("orders-0"), LogConfig::default()).unwrap();
    check!(log.log_start_offset() == Offset(2));
    check!(log.log_end_offset() == Offset(2));
}

#[tokio::test]
async fn diskless_restore_rejects_one_topic_name_with_multiple_ids() {
    let archive = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let capture_path = archive.path().join("capture.json");
    let (mut capture, _) = fixture(archive.path(), false, false);
    capture.partitions[0].ranges.clear();
    capture.partitions[0].delete_floor = 0;
    capture.partitions[0].recovery_cutoff = 0;
    let mut conflicting = capture.partitions[0].clone();
    conflicting.topic_id = Uuid::from_u128(45);
    conflicting.partition = 1;
    capture.partitions.push(conflicting);
    write_capture(&capture_path, &capture);

    let error = restore(&args(archive.path(), target.path(), &capture_path))
        .await
        .unwrap_err();
    check!(matches!(error, RestoreError::Integrity(_)));
}

#[tokio::test]
async fn missing_or_corrupt_required_wal_fails_closed() {
    for (object, corrupt) in [(false, false), (true, true)] {
        let (archive, target, capture_path) = restore_fixture(object, corrupt);
        let error = restore(&args(archive.path(), target.path(), &capture_path))
            .await
            .unwrap_err();
        check!(matches!(
            error,
            RestoreError::Integrity { .. } | RestoreError::ObjectStore(_)
        ));
    }
}

#[tokio::test]
async fn trusted_capture_accepts_only_its_key_head_state_and_wal_bytes() {
    let archive = tempfile::tempdir().unwrap();
    let capture_path = archive.path().join("capture.json");
    let public_path = archive.path().join("capture.pub");
    let rlmm_path = archive.path().join("rlmm-snapshot");
    Snapshot {
        committed_offsets: vec![9],
        dump: RlmmCacheDump::default(),
    }
    .write_atomic(&rlmm_path)
    .unwrap();
    let rlmm_bytes = std::fs::read(&rlmm_path).unwrap();
    let (mut capture, _) = fixture(archive.path(), true, false);
    capture.metadata_snapshot_sha256 = Some(Sha256Digest::of(b"trusted metadata"));
    capture.rlmm_snapshot_sha256 = Some(Sha256Digest::of(&rlmm_bytes));
    let wal_key = capture.partitions[0].ranges[0].object_key.clone();
    let wal = std::fs::read(archive.path().join(&wal_key)).unwrap();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "capture-key".into()).unwrap();
    std::fs::write(&public_path, signer.public_key()).unwrap();
    let head = capture
        .seal(
            vec![ObjectEntry {
                suffix: ".ckwl".into(),
                key: wal_key.clone(),
                size_bytes: wal.len() as u64,
                sha256: Sha256Digest::of(&wal),
                e_tag: None,
                version_id: None,
                create_precondition: false,
            }],
            &signer,
        )
        .unwrap()
        .to_string();
    write_capture(&capture_path, &capture);

    let target = tempfile::tempdir().unwrap();
    let mut trusted = trusted_args(
        archive.path(),
        target.path(),
        &capture_path,
        &public_path,
        &head,
    );
    trusted.archive.rlmm_snapshot = Some(rlmm_path.clone());
    let report = restore(&trusted).await.unwrap();
    check!(report.authentication.unwrap().chain_heads[CAPTURE_HEAD_NAME] == head);
    let restored_rlmm = Snapshot::load(
        &target
            .path()
            .join("remote-log-metadata")
            .join(krabka_remote_storage_topic::snapshot::SNAPSHOT_FILE_NAME),
    )
    .unwrap()
    .unwrap();
    check!(restored_rlmm.committed_offsets == vec![-1]);

    let target = tempfile::tempdir().unwrap();
    let tampered_rlmm_path = archive.path().join("tampered-rlmm-snapshot");
    std::fs::write(&tampered_rlmm_path, b"tampered rlmm").unwrap();
    let mut tampered_rlmm = trusted_args(
        archive.path(),
        target.path(),
        &capture_path,
        &public_path,
        &head,
    );
    tampered_rlmm.archive.rlmm_snapshot = Some(tampered_rlmm_path);
    let error = restore(&tampered_rlmm).await.unwrap_err();
    check!(matches!(error, RestoreError::Authenticity { .. }));

    let target = tempfile::tempdir().unwrap();
    let metadata_path = archive.path().join("tampered-metadata.checkpoint");
    std::fs::write(&metadata_path, b"tampered metadata").unwrap();
    let mut tampered_metadata = trusted_args(
        archive.path(),
        target.path(),
        &capture_path,
        &public_path,
        &head,
    );
    tampered_metadata.archive.metadata_snapshot = Some(metadata_path);
    let error = restore(&tampered_metadata).await.unwrap_err();
    check!(matches!(error, RestoreError::Authenticity { .. }));

    let target = tempfile::tempdir().unwrap();
    let mut missing_classic = trusted_args(
        archive.path(),
        target.path(),
        &capture_path,
        &public_path,
        &head,
    );
    missing_classic.worm_expect_head.push(format!(
        "orders-0-AAAAAAAAAAAAAAAAAAAAAA={}",
        "00".repeat(32)
    ));
    let error = restore(&missing_classic).await.unwrap_err();
    check!(matches!(error, RestoreError::Authenticity { .. }));

    for (name, changed, expected_head, key_bytes) in [
        (
            "changed state",
            {
                let mut changed = capture.clone();
                changed.partitions[0].delete_floor = 0;
                changed
            },
            head.clone(),
            signer.public_key(),
        ),
        (
            "wrong head",
            capture.clone(),
            "00".repeat(32),
            signer.public_key(),
        ),
        ("untrusted key", capture.clone(), head.clone(), vec![0; 32]),
    ] {
        let target = tempfile::tempdir().unwrap();
        let changed_path = archive.path().join(format!("{name}.json"));
        let key_path = archive.path().join(format!("{name}.pub"));
        write_capture(&changed_path, &changed);
        std::fs::write(&key_path, key_bytes).unwrap();
        let error = restore(&trusted_args(
            archive.path(),
            target.path(),
            &changed_path,
            &key_path,
            &expected_head,
        ))
        .await
        .unwrap_err();
        check!(matches!(error, RestoreError::Authenticity { .. }), "{name}");
    }

    let target = tempfile::tempdir().unwrap();
    let mut wal_tampered = wal;
    wal_tampered[10] ^= 1;
    std::fs::write(archive.path().join(wal_key), wal_tampered).unwrap();
    let error = restore(&trusted_args(
        archive.path(),
        target.path(),
        &capture_path,
        &public_path,
        &head,
    ))
    .await
    .unwrap_err();
    check!(matches!(error, RestoreError::Authenticity { .. }));
}

#[tokio::test]
async fn dry_run_rejects_a_capture_that_selects_only_part_of_a_footer_run() {
    let (archive, target, capture_path, mut capture) = unwritten_fixture(true, false);
    capture.partitions[0].ranges[0].entry.last_offset = 0;
    capture.partitions[0].ranges[0].entry.byte_len /= 2;
    capture.partitions[0].recovery_cutoff = 1;
    write_capture(&capture_path, &capture);
    let error = dry_run_error(archive.path(), target.path(), &capture_path).await;
    check!(matches!(
        error,
        RestoreError::Integrity(_) | RestoreError::Records(_)
    ));
}

#[tokio::test]
async fn dry_run_rejects_an_internal_batch_offset_gap() {
    let archive = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let capture_path = archive.path().join("capture.json");
    let (capture, _) = fixture_with_second_base(archive.path(), true, false, 2);
    write_capture(&capture_path, &capture);
    let error = dry_run_error(archive.path(), target.path(), &capture_path).await;
    check!(matches!(error, RestoreError::Integrity(_)));
}
