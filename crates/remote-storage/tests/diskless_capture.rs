use {
    assert2::check,
    krabka_audit::FileEd25519Signer,
    krabka_remote_storage::{
        ObjectEntry, Sha256Digest, TrustedManifestKeys,
        diskless::{
            CAPTURE_HEAD_NAME, CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture,
            WalCaptureProjection, WalDeleteFloorKey, WalDeleteFloorRecord, WalFlushRecord,
            WalIndexEntry, WalIndexKey,
        },
    },
    ring::{rand::SystemRandom, signature::Ed25519KeyPair},
    std::collections::HashMap,
    uuid::Uuid,
};

fn entry(first: i64, last: i64) -> WalIndexEntry {
    WalIndexEntry {
        topic_id: Uuid::from_u128(7),
        partition: 0,
        first_offset: first,
        last_offset: last,
        byte_start: 6,
        byte_len: 64,
        max_timestamp_ms: 100,
    }
}

fn value(object: &str, entry: WalIndexEntry) -> Vec<u8> {
    WalFlushRecord {
        object_key: object.to_owned(),
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries: vec![entry],
    }
    .to_bytes()
    .unwrap()
    .to_vec()
}

#[test]
fn retries_tombstones_and_delete_floors_capture_only_committed_live_state() {
    let mut projection = WalCaptureProjection::default();
    let first = entry(0, 3);
    let second = entry(4, 7);
    let first_key = WalIndexKey::from(&first).to_bytes();
    let second_key = WalIndexKey::from(&second).to_bytes();

    projection
        .apply(
            Some(&first_key),
            Some(&value("orphan-after-retry", first.clone())),
        )
        .unwrap();
    projection
        .apply(Some(&first_key), Some(&value("live-a", first)))
        .unwrap();
    projection
        .apply(Some(&second_key), Some(&value("deleted", second)))
        .unwrap();
    projection.apply(Some(&second_key), None).unwrap();

    let floor_key = WalDeleteFloorKey {
        topic_id: Uuid::from_u128(7),
        partition: 0,
    }
    .to_bytes();
    for floor in [3, 1] {
        let bytes = WalDeleteFloorRecord {
            topic_id: Uuid::from_u128(7),
            partition: 0,
            floor,
        }
        .to_bytes()
        .unwrap();
        projection.apply(Some(&floor_key), Some(&bytes)).unwrap();
    }

    let capture = projection
        .capture(
            &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 1))]),
            vec![12],
            1_700_000_000_000,
        )
        .unwrap();
    check!(capture.partitions.len() == 1);
    check!(capture.partitions[0].delete_floor == 3);
    check!(capture.partitions[0].recovery_cutoff == 4);
    check!(capture.partitions[0].ranges.len() == 1);
    check!(capture.partitions[0].ranges[0].object_key == "live-a");

    let encoded = serde_json::to_vec(&capture).unwrap();
    check!(DisklessWalCapture::from_slice(&encoded).unwrap() == capture);
}

#[test]
fn keyed_values_replace_and_tombstones_remove_their_range() {
    let mut projection = WalCaptureProjection::default();
    let live = entry(0, 3);
    let deleted = entry(4, 7);
    let live_key = WalIndexKey::from(&live).to_bytes();
    let deleted_key = WalIndexKey::from(&deleted).to_bytes();

    projection
        .apply(Some(&live_key), Some(&value("first", live.clone())))
        .unwrap();
    projection
        .apply(Some(&live_key), Some(&value("keyed", live)))
        .unwrap();
    projection
        .apply(Some(&deleted_key), Some(&value("deleted", deleted)))
        .unwrap();
    projection.apply(Some(&deleted_key), None).unwrap();

    let capture = projection
        .capture(
            &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 1))]),
            vec![12],
            0,
        )
        .unwrap();
    check!(capture.partitions[0].ranges.len() == 1);
    check!(capture.partitions[0].ranges[0].object_key == "keyed");
}

#[test]
fn an_unkeyed_index_record_is_rejected() {
    let mut projection = WalCaptureProjection::default();
    let range = entry(0, 3);

    check!(
        projection
            .apply(None, Some(&value("unkeyed", range)))
            .unwrap_err()
            == "diskless WAL index record has no key"
    );
    check!(projection.apply(None, None).unwrap_err() == "diskless WAL index record has no key");
}

#[test]
fn distinct_overlapping_ranges_fail_closed() {
    let mut projection = WalCaptureProjection::default();
    for (first, last) in [(0, 5), (3, 7)] {
        let e = entry(first, last);
        projection
            .apply(
                Some(&WalIndexKey::from(&e).to_bytes()),
                Some(&value("wal", e)),
            )
            .unwrap();
    }
    let result = projection.capture(
        &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 1))]),
        vec![2],
        0,
    );
    check!(result.unwrap_err().contains("overlap"));
}

#[test]
fn adjacent_batches_from_one_footer_run_are_captured_as_one_range() {
    let mut projection = WalCaptureProjection::default();
    let mut first = entry(0, 0);
    first.byte_start = 6;
    first.byte_len = 20;
    let mut second = entry(1, 1);
    second.byte_start = 26;
    second.byte_len = 30;
    for batch in [first, second] {
        projection
            .apply(
                Some(&WalIndexKey::from(&batch).to_bytes()),
                Some(&value("wal", batch)),
            )
            .unwrap();
    }
    let capture = projection
        .capture(
            &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 1))]),
            vec![2],
            0,
        )
        .unwrap();
    check!(capture.partitions[0].ranges.len() == 1);
    check!(capture.partitions[0].ranges[0].entry.first_offset == 0);
    check!(capture.partitions[0].ranges[0].entry.last_offset == 1);
    check!(capture.partitions[0].ranges[0].entry.byte_len == 50);
}

#[test]
fn capture_preserves_empty_diskless_partitions() {
    let capture = WalCaptureProjection::default()
        .capture(
            &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 3))]),
            vec![0],
            0,
        )
        .unwrap();

    check!(capture.partitions.len() == 3);
    check!(
        capture
            .partitions
            .iter()
            .all(|partition| partition.ranges.is_empty())
    );
    check!(
        capture
            .partitions
            .iter()
            .map(|partition| partition.partition)
            .collect::<Vec<_>>()
            == vec![0, 1, 2]
    );
}

#[test]
fn capture_ignores_index_state_for_topics_outside_the_authorized_topology() {
    let mut projection = WalCaptureProjection::default();
    let mut unauthorized = entry(0, 0);
    unauthorized.topic_id = Uuid::from_u128(8);
    projection
        .apply(
            Some(&WalIndexKey::from(&unauthorized).to_bytes()),
            Some(&value("other-tenant", unauthorized)),
        )
        .unwrap();

    let capture = projection
        .capture(
            &HashMap::from([(Uuid::from_u128(7), ("orders".to_owned(), 1))]),
            vec![1],
            0,
        )
        .unwrap();

    check!(capture.partitions.len() == 1);
    check!(capture.partitions[0].topic == "orders");
    check!(capture.partitions[0].ranges.is_empty());
}

fn signable_capture() -> DisklessWalCapture {
    DisklessWalCapture {
        format_version: DisklessWalCapture::FORMAT_VERSION,
        captured_at_ms: 42,
        source_cutoffs: vec![9],
        partitions: vec![DisklessPartitionCapture {
            topic: "orders".into(),
            topic_id: Uuid::from_u128(7),
            partition: 0,
            delete_floor: 0,
            recovery_cutoff: 1,
            ranges: vec![CapturedWalRange {
                object_key: "diskless-wal/a.ckwl".into(),
                entry: entry(0, 0),
            }],
        }],
        metadata_snapshot_sha256: None,
        rlmm_snapshot_sha256: None,
        group_offsets_sha256: None,
        authentication: None,
    }
}

fn signer() -> (FileEd25519Signer, Vec<u8>) {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "capture-key".into()).unwrap();
    let public = signer.public_key();
    (signer, public)
}

#[test]
fn signed_capture_binds_state_objects_trust_and_external_head() {
    let (signer, public) = signer();
    let mut capture = signable_capture();
    capture.rlmm_snapshot_sha256 = Some(Sha256Digest::of(b"rlmm snapshot"));
    let wal = b"exact wal bytes";
    let head = capture
        .seal(
            vec![ObjectEntry {
                suffix: ".ckwl".into(),
                key: "diskless-wal/a.ckwl".into(),
                size_bytes: wal.len() as u64,
                sha256: Sha256Digest::of(wal),
                e_tag: None,
                version_id: None,
                create_precondition: false,
            }],
            &signer,
        )
        .unwrap();
    let trusted = TrustedManifestKeys::single("capture-key".into(), public);
    let claims = capture
        .authenticate(&trusted, Some(&head.to_string()))
        .unwrap();
    check!(claims["diskless-wal/a.ckwl"].sha256 == Sha256Digest::of(wal));

    let mut changed_floor = capture.clone();
    changed_floor.partitions[0].delete_floor = 1;
    check!(
        changed_floor
            .authenticate(&trusted, Some(&head.to_string()))
            .is_err()
    );

    let mut changed_rlmm = capture.clone();
    changed_rlmm.rlmm_snapshot_sha256 = Some(Sha256Digest::of(b"different snapshot"));
    check!(
        changed_rlmm
            .authenticate(&trusted, Some(&head.to_string()))
            .is_err()
    );

    let mut changed_offsets = capture.clone();
    changed_offsets.group_offsets_sha256 = Some(Sha256Digest::of(b"forged offsets"));
    check!(
        changed_offsets
            .authenticate(&trusted, Some(&head.to_string()))
            .is_err()
    );

    let untrusted = TrustedManifestKeys::single("another-key".into(), vec![0; 32]);
    check!(
        capture
            .authenticate(&untrusted, Some(&head.to_string()))
            .is_err()
    );
    check!(
        capture
            .authenticate(&trusted, Some(&"00".repeat(32)))
            .is_err()
    );
    check!(
        capture
            .authenticate(&trusted, None)
            .unwrap_err()
            .contains(CAPTURE_HEAD_NAME)
    );
}

#[test]
fn decoded_capture_rejects_unordered_duplicate_and_false_cutoff_ranges() {
    for mutate in [
        |capture: &mut DisklessWalCapture| capture.partitions[0].recovery_cutoff = 2,
        |capture: &mut DisklessWalCapture| {
            let duplicate = capture.partitions[0].ranges[0].clone();
            capture.partitions[0].ranges.push(duplicate);
        },
    ] {
        let mut capture = signable_capture();
        mutate(&mut capture);
        let encoded = serde_json::to_vec(&capture).unwrap();
        check!(DisklessWalCapture::from_slice(&encoded).is_err());
    }
}

#[test]
fn decoded_capture_rejects_a_gap_in_the_live_range() {
    let mut capture = signable_capture();
    capture.partitions[0].delete_floor = 1;
    capture.partitions[0].ranges[0].entry.first_offset = 2;
    capture.partitions[0].ranges[0].entry.last_offset = 2;
    capture.partitions[0].recovery_cutoff = 3;

    let error = DisklessWalCapture::from_slice(&serde_json::to_vec(&capture).unwrap()).unwrap_err();
    check!(error.contains("gap at 1"), "{error}");
}

#[test]
fn decoded_capture_rejects_topic_names_that_can_escape_the_target() {
    for topic in ["/tmp/escape", "../escape", "orders/escape"] {
        let mut capture = signable_capture();
        topic.clone_into(&mut capture.partitions[0].topic);
        let encoded = serde_json::to_vec(&capture).unwrap();
        check!(DisklessWalCapture::from_slice(&encoded).is_err(), "{topic}");
    }
}
