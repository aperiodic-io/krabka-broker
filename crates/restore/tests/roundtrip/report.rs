//! The structured report `restore` returns, compared as a whole `RestoreReport`
//! against the counts the fixture must produce, and then rendered through the
//! `--report json` path.

use assert2::check;
use krabka_audit::FileEd25519Signer;
use krabka_ids::Offset;
use krabka_remote_storage::MANIFEST_SUFFIX;
use krabka_restore::{
    MetadataRestoreReport, PartitionReport, ReportFormat, RestoreReport, SegmentOutcome, restore,
};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};

use crate::{args::restore_args, fixture::build_fixture};

#[tokio::test]
async fn authenticated_object_count_covers_only_the_selected_topic() {
    let fixture = build_fixture();
    let keys = tempfile::tempdir().unwrap();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = std::sync::Arc::new(
        FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "key".to_owned()).unwrap(),
    );
    let public_key = keys.path().join("key.pub");
    std::fs::write(&public_key, signer.public_key()).unwrap();
    let heads = fixture.sign_worm(&[signer]);
    let mut extra = vec![
        "--topic".to_owned(),
        "orders".to_owned(),
        "--worm-key-id".to_owned(),
        "key".to_owned(),
        "--worm-public-key".to_owned(),
        public_key.display().to_string(),
    ];
    for (partition, head) in heads {
        extra.push("--worm-expect-head".to_owned());
        extra.push(format!("{partition}={head}"));
    }
    let expected = std::fs::read_dir(fixture.archive_root.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("orders-"))
        .flat_map(|entry| {
            std::fs::read_dir(entry.path())
                .unwrap()
                .filter_map(Result::ok)
        })
        .filter(|entry| {
            !entry
                .file_name()
                .to_string_lossy()
                .ends_with(MANIFEST_SUFFIX)
        })
        .count() as u64;
    let target = tempfile::tempdir().unwrap();
    let refs = extra.iter().map(String::as_str).collect::<Vec<_>>();
    let args = restore_args(
        fixture.archive_root.path(),
        target.path(),
        crate::args::RestoreOptions {
            extra: &refs,
            ..Default::default()
        },
    );

    let report = restore(&args).await.unwrap();
    check!(report.authentication.unwrap().objects == expected);
}

/// 5. `restore()`'s report -- the structured value `--report json` renders --
/// carries exactly the record and segment counts this fixture should
/// produce, compared as whole structs rather than field by field.
#[tokio::test]
async fn json_report_matches_the_fixtures_exact_record_and_segment_counts() {
    let fixture = build_fixture();
    let restored = crate::restored::fresh_restore(fixture.archive_root.path()).await;

    let expected = RestoreReport {
        dry_run: false,
        log_dir: restored.log_dir.clone(),
        cluster_id: restored.cluster_id,
        authentication: None,
        diskless: None,
        metadata: MetadataRestoreReport {
            snapshot: None,
            topic_configs: 0,
            access_control_entries: 0,
            client_quotas: 0,
            scram_credentials: 0,
            feature_levels: 0,
            topics_without_configuration: vec!["orders".to_owned(), "payments".to_owned()],
        },
        partitions: fixture
            .partitions()
            .iter()
            .map(|partition| PartitionReport {
                topic: partition.topic.to_owned(),
                partition: partition.partition,
                topic_id: partition.topic_id,
                segments: partition
                    .segments
                    .iter()
                    .map(|segment| {
                        let base_offset = Offset(segment.batch.base_offset);
                        let end_offset = Offset(
                            segment.batch.base_offset + i64::from(segment.batch.last_offset_delta),
                        );
                        SegmentOutcome {
                            segment_id: segment.segment_id,
                            base_offset,
                            end_offset,
                            batches_kept: 1,
                            batches_rewritten: 0,
                            batches_emptied: 0,
                            records_kept: u64::try_from(segment.batch.records.len())
                                .expect("fixture batches stay tiny"),
                            records_dropped: 0,
                            bytes_written: u64::try_from(segment.batch.encoded_len())
                                .expect("fixture batches stay tiny"),
                        }
                    })
                    .collect(),
            })
            .collect(),
        skipped: Vec::new(),
    };
    check!(restored.report == expected);

    // Also render and reparse as JSON, exercising the `--report json` path
    // itself rather than only the underlying struct it renders.
    let json = restored.report.render(ReportFormat::Json);
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    check!(value["cluster_id"] == serde_json::json!(restored.cluster_id.to_string()));
    check!(value["partitions"][0]["topic"] == serde_json::json!("orders"));
    check!(value["partitions"][0]["partition"] == serde_json::json!(0));
    check!(
        value["partitions"][0]["segments"]
            .as_array()
            .expect("segments array")
            .len()
            == 2
    );
    check!(
        value["skipped"]
            .as_array()
            .expect("skipped array")
            .is_empty()
    );
}
