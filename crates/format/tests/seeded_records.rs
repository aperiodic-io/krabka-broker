//! Formatting a log directory with caller-supplied metadata records.
//!
//! A point-in-time restore rebuilds a cluster from tiered-storage archives, and
//! the broker it hands the directory to has to boot with the restored topics
//! already present. `krabka_format::run_with_records` takes those topic and
//! partition records and seeds them next to the records the flags produce, so
//! the restore tool does not repeat the bootstrap write itself.
//!
//! Each test formats a directory in process and reads back what the two
//! consumers of the seed stream wrote: the bootstrap files a controller loads
//! (`bootstrap.records.bin` and its `bootstrap.json` mirror), and the
//! offset-zero KIP-630/KIP-853 bootstrap checkpoint a dynamic format writes.

use std::path::{Path, PathBuf};

use assert2::check;
use clap::Parser as _;
use krabka_format::MetadataRecord;
use krabka_metadata::{
    KRaftVersionRecord, NodeId, PartitionRecord, TopicRecord, VoterSet, VotersRecord,
};
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::Deserialize as _;

krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// Pinned so two formats of the same arguments are byte-comparable.
const CLUSTER_ID: &str = "8a2f4e1c-0000-4000-8000-00000000c1d0";
/// Pinned for the same reason: an omitted `--directory-id` is generated fresh.
const DIRECTORY_ID: &str = "8a2f4e1c-0000-4000-8000-00000000d1d0";

/// A directory the formatter accepts.
///
/// It refuses a non-empty target, and `tempfile::tempdir` hands back a path
/// that already exists, so every test formats a child of one.
fn empty_log_dir(parent: &tempfile::TempDir, name: &str) -> PathBuf {
    parent.path().join(name)
}

/// The records a restore recovered for one topic: the topic, then its
/// partitions, which is the order the partitions are keyed on.
fn restored_topic(name: &str, topic_id: u128, partitions: i32) -> Vec<MetadataRecord> {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.to_string(),
        topic_id: Uuid::from_u128(topic_id),
        partitions,
        replication_factor: 1,
    })];
    records.extend((0..partitions).map(|partition| {
        MetadataRecord::V1Partition(PartitionRecord {
            directories: vec![Uuid::nil()],
            ..single_replica_partition(name, krabka_ids::PartitionIndex(partition), NodeId(1))
        })
    }));
    records
}

/// Formats `log_dir` as node 1 with a pinned cluster id, `flags`, and `extra`
/// seeded.
async fn format(log_dir: &Path, flags: &[&str], extra: Vec<MetadataRecord>) {
    let mut argv = vec![
        "krabka-format".to_string(),
        "--log-dir".to_string(),
        log_dir.display().to_string(),
        "--cluster-id".to_string(),
        CLUSTER_ID.to_string(),
        "--node-id".to_string(),
        "1".to_string(),
    ];
    argv.extend(flags.iter().map(|flag| (*flag).to_string()));
    let code = krabka_format::run_from_args_with_records(argv, extra).await;
    check!(code == 0);
}

fn bootstrap_records(log_dir: &Path) -> Vec<MetadataRecord> {
    krabka_broker::bootstrap::load_bootstrap_records(log_dir).expect("bootstrap records")
}

fn offset_zero_checkpoint(log_dir: &Path) -> PathBuf {
    log_dir
        .join("__cluster_metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

/// Decodes the standard padded alphabet `bootstrap.json` mirrors each record
/// with.
fn base64_decode(input: &str) -> Vec<u8> {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::new();
    for symbol in input.bytes().filter(|byte| *byte != b'=') {
        let index = ALPHA
            .iter()
            .position(|candidate| *candidate == symbol)
            .expect("manifest records are base64");
        acc = (acc << 6) | u32::try_from(index).expect("an alphabet index is below 64");
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((acc >> bits) & 0xff).expect("masked to one byte"));
        }
    }
    out
}

/// The seed stream is one vec, and the position of the seeded records in it is
/// the contract: the feature levels decide how every later record is read, and
/// a seeded ACL should name a topic the image already holds.
#[tokio::test]
async fn seeded_records_follow_the_features_and_lead_the_acl_entries() {
    const ACL: &str = "principal=User:restore,host=*,operation=Read,permission=Allow,resource=Topic:restored-orders";

    let parent = tempfile::tempdir().unwrap();
    let plain_dir = empty_log_dir(&parent, "plain");
    let seeded_dir = empty_log_dir(&parent, "seeded");
    let mut extra = restored_topic("restored-orders", 1, 2);
    extra.extend(restored_topic("restored-payments", 2, 1));

    format(&plain_dir, &["--add-acl", ACL], Vec::new()).await;
    format(&seeded_dir, &["--add-acl", ACL], extra.clone()).await;

    let plain = bootstrap_records(&plain_dir);
    let seeded = bootstrap_records(&seeded_dir);

    // Splice the seeded records into the unseeded stream at the end of its
    // feature block. A seeded format must produce exactly that.
    let after_features = plain
        .iter()
        .position(|record| !matches!(record, MetadataRecord::V1FeatureLevel(_)))
        .expect("--add-acl leaves a record after the feature block");
    let mut expected = plain[..after_features].to_vec();
    expected.extend(extra);
    expected.extend_from_slice(&plain[after_features..]);

    check!(seeded == expected);
}

/// `bootstrap.json` is the operator-readable mirror of the binary stream, so it
/// carries the seeded records as well.
#[tokio::test]
async fn the_manifest_mirrors_the_seeded_binary_stream() {
    let (_parent, log_dir, extra) = formatted_restored_topic(&[]).await;

    let records = bootstrap_records(&log_dir);
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(log_dir.join("bootstrap.json")).expect("bootstrap.json"),
    )
    .expect("bootstrap.json is json");
    let mirrored: Vec<MetadataRecord> = manifest["records_b64"]
        .as_array()
        .expect("records_b64 is an array")
        .iter()
        .map(|entry| {
            let blob = base64_decode(entry.as_str().expect("a base64 string"));
            <SerdeCompat<MetadataRecord>>::deserialize(&blob).expect("a mirrored record")
        })
        .collect();

    check!(manifest["record_count"].as_u64() == u64::try_from(records.len()).ok());
    check!(mirrored == records);
    check!(records.ends_with(&extra));
}

/// The offset-zero checkpoint is Kafka's bootstrap snapshot: the KIP-853
/// control state, then the seed stream in its own order, the seeded topics
/// too. The active controller writes that stream to the metadata log, so a
/// checkpoint that missed the seeded records, or that reordered the stream,
/// would boot a different cluster.
#[tokio::test]
async fn a_dynamic_format_seeds_the_offset_zero_checkpoint() {
    let (_parent, log_dir, extra) = formatted_restored_topic(&["--no-initial-controllers"]).await;

    let stream = bootstrap_records(&log_dir);
    check!(stream.ends_with(&extra));
    let mut expected = vec![
        MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
        MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::default(),
        }),
    ];
    // Kafka's `TopicRecord` carries no partition count: the partition records
    // after it give the count. So a topic reads back with none.
    expected.extend(stream.into_iter().map(|record| match record {
        MetadataRecord::V1Topic(topic) => MetadataRecord::V1Topic(TopicRecord {
            partitions: 0,
            ..topic
        }),
        other => other,
    }));
    let written = std::fs::read(offset_zero_checkpoint(&log_dir)).expect("checkpoint");
    check!(krabka_raft::deserialize_metadata_snapshot_image(&written).ok() == Some(expected));
}

/// `run` seeds nothing, so it has to write what `run_with_records` writes for an
/// empty seed list. Every generated identity is pinned here, which leaves the
/// two outputs comparable byte for byte, except for the time that
/// `meta.properties` records: that file is compared by its ids.
#[tokio::test]
async fn run_writes_what_run_with_records_writes_for_no_extras() {
    let parent = tempfile::tempdir().unwrap();
    let via_run = empty_log_dir(&parent, "run");
    let via_seam = empty_log_dir(&parent, "seam");
    let argv = |log_dir: &Path| {
        vec![
            "krabka-format".to_string(),
            "--log-dir".to_string(),
            log_dir.display().to_string(),
            "--cluster-id".to_string(),
            CLUSTER_ID.to_string(),
            "--directory-id".to_string(),
            DIRECTORY_ID.to_string(),
            "--node-id".to_string(),
            "1".to_string(),
            "--no-initial-controllers".to_string(),
        ]
    };

    let run_code = krabka_format::run(krabka_format::Cli::parse_from(argv(&via_run)).args).await;
    let seam_code = krabka_format::run_with_records(
        krabka_format::Cli::parse_from(argv(&via_seam)).args,
        Vec::new(),
    )
    .await;

    check!(run_code == 0);
    check!(seam_code == 0);
    let meta_from_run = krabka_format::MetaProperties::read(&via_run).expect("run output");
    let meta_from_seam =
        krabka_format::MetaProperties::read(&via_seam).expect("run_with_records output");
    check!(meta_from_run.is_some());
    check!(meta_from_run == meta_from_seam);
    for name in ["bootstrap.json", "bootstrap.records.bin"] {
        let from_run = std::fs::read(via_run.join(name)).expect("run output");
        let from_seam = std::fs::read(via_seam.join(name)).expect("run_with_records output");
        check!(from_run == from_seam, "{name} differs");
    }
    let run_checkpoint = std::fs::read(offset_zero_checkpoint(&via_run)).expect("run checkpoint");
    let seam_checkpoint =
        std::fs::read(offset_zero_checkpoint(&via_seam)).expect("run_with_records checkpoint");
    check!(run_checkpoint == seam_checkpoint);
}

async fn formatted_restored_topic(
    extra_args: &[&str],
) -> (tempfile::TempDir, std::path::PathBuf, Vec<MetadataRecord>) {
    let parent = tempfile::tempdir().unwrap();
    let log_dir = empty_log_dir(&parent, "seeded");
    let extra = restored_topic("restored-orders", 1, 2);
    format(&log_dir, extra_args, extra.clone()).await;
    (parent, log_dir, extra)
}
