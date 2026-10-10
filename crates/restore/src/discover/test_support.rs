//! Fixtures shared by the discovery unit tests: an archive written key by key
//! at the exact KIP-405 layout, the [`SegmentInventory`] a complete copy of one
//! segment should produce, and the `--rlmm-snapshot` file the reconciliation
//! tests hand back to the scan.

use clap::Parser as _;
use krabka_ids::{LeaderEpoch, Offset, PartitionIndex};
use krabka_remote_storage::{
    RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RlmmCacheDump, TopicIdPartition, kafka_uuid,
};
use krabka_remote_storage_topic::Snapshot;
use uuid::Uuid;

use crate::{
    args::RestoreArgs,
    backend::ArchiveStore,
    discover::{ArchiveObject, SegmentInventory},
};

/// transaction index is excluded on purpose: it is optional even for a
/// copy discovery has no reason to call torn.
pub(super) const FULL_SEGMENT_SUFFIXES: [&str; 5] = [
    ".log",
    ".index",
    ".timeindex",
    ".snapshot",
    ".leader_epoch_checkpoint",
];

/// Bytes every fixture artifact is written with, so every
/// [`ArchiveObject::size`] in an expected structure is this length.
pub(super) const STUB_BYTES: &[u8] = b"stub";

pub(super) fn args_from(archive_dir: &std::path::Path, extra: &[&str]) -> RestoreArgs {
    let mut argv: Vec<String> = vec![
        "krabka-restore".to_owned(),
        "--log-dir".to_owned(),
        "/target".to_owned(),
        "--archive-local".to_owned(),
        archive_dir.display().to_string(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    crate::Cli::parse_from(argv).args
}

/// The directory-naming identity of one partition, factored out of
/// [`write_artifact`]'s arguments so the helper stays under Clippy's
/// argument-count limit.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct PartitionKey<'a> {
    #[default("orders")]
    pub(super) topic: &'a str,
    #[default(PartitionIndex(0))]
    pub(super) partition: PartitionIndex,
    #[default(Uuid::from_u128(1))]
    pub(super) topic_id: Uuid,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SegmentSetup<'a> {
    pub(super) partition: PartitionKey<'a>,
    #[default(Offset(0))]
    pub(super) base_offset: Offset,
    #[default(Uuid::from_u128(10))]
    pub(super) segment_id: Uuid,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ArtifactSetup<'a> {
    pub(super) prefix: Option<&'a str>,
    pub(super) segment: SegmentSetup<'a>,
    #[default(".log")]
    pub(super) suffix: &'a str,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SnapshotSegmentSetup<'a> {
    pub(super) segment: SegmentSetup<'a>,
    #[default(RemoteLogSegmentState::CopySegmentFinished)]
    pub(super) state: RemoteLogSegmentState,
}

/// Write one artifact at the exact KIP-405 archive key layout, by hand.
pub(super) fn write_artifact(root: &std::path::Path, setup: ArtifactSetup<'_>) {
    let ArtifactSetup {
        prefix,
        segment,
        suffix,
    } = setup;
    let SegmentSetup {
        partition,
        base_offset,
        segment_id,
    } = segment;
    let base_offset = base_offset.0;
    let dir_name = format!(
        "{}-{}-{}",
        partition.topic,
        partition.partition.0,
        kafka_uuid(partition.topic_id)
    );
    let file_name = format!("{base_offset:020}-{}{suffix}", kafka_uuid(segment_id));
    let mut dir = root.to_path_buf();
    if let Some(prefix) = prefix {
        dir.push(prefix);
    }
    dir.push(dir_name);
    std::fs::create_dir_all(&dir).expect("create partition dir");
    std::fs::write(dir.join(file_name), STUB_BYTES).expect("write artifact");
}

/// Write every artifact of one complete segment copy.
pub(super) fn write_full_segment(root: &std::path::Path, setup: SegmentSetup<'_>) {
    for suffix in FULL_SEGMENT_SUFFIXES {
        write_artifact(
            root,
            ArtifactSetup {
                segment: setup,
                suffix,
                ..Default::default()
            },
        );
    }
}

/// The [`SegmentInventory`] a call to [`write_full_segment`] with the same
/// arguments produces.
pub(super) fn expected_full_segment(
    store: &ArchiveStore,
    setup: SegmentSetup<'_>,
) -> SegmentInventory {
    let SegmentSetup {
        partition,
        base_offset,
        segment_id,
    } = setup;
    let PartitionKey {
        topic,
        partition,
        topic_id,
    } = partition;
    let partition = partition.0;
    let base_offset = base_offset.0;
    let object = |suffix: &str| {
        Some(ArchiveObject {
            key: store.key(&format!(
                "{topic}-{partition}-{}/{base_offset:020}-{}{suffix}",
                kafka_uuid(topic_id),
                kafka_uuid(segment_id),
            )),
            size: STUB_BYTES.len() as u64,
        })
    };
    SegmentInventory {
        segment_id,
        base_offset: Offset(base_offset),
        log: object(".log"),
        offset_index: object(".index"),
        time_index: object(".timeindex"),
        producer_snapshot: object(".snapshot"),
        leader_epoch: object(".leader_epoch_checkpoint"),
        transaction_index: None,
    }
}

/// One RLMM-tracked segment, for a `--rlmm-snapshot` fixture.
pub(super) fn snapshot_segment(setup: SnapshotSegmentSetup<'_>) -> RemoteLogSegmentMetadata {
    let SnapshotSegmentSetup {
        segment: setup,
        state,
    } = setup;
    let SegmentSetup {
        partition,
        base_offset,
        segment_id,
    } = setup;
    let PartitionKey {
        topic,
        partition,
        topic_id,
    } = partition;
    let partition = partition.0;
    let base_offset = base_offset.0;
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, topic, partition),
            segment_id,
        ),
        base_offset,
        base_offset,
        0,
        1,
        0,
        RemoteLogSegmentDetails::new(
            i32::try_from(STUB_BYTES.len()).expect("stub length fits in i32"),
            state,
            maplit::btreemap! {LeaderEpoch(0) => base_offset},
        ),
    )
    .expect("valid segment metadata")
}

pub(super) fn write_snapshot(path: &std::path::Path, dump: RlmmCacheDump) {
    Snapshot {
        committed_offsets: Vec::new(),
        dump,
    }
    .write_atomic(path)
    .expect("write snapshot");
}

/// One complete orders-0 segment with caller-chosen base offset.
pub(super) fn single_segment_archive(base_offset: Offset) -> (tempfile::TempDir, Uuid, Uuid) {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic_id = Uuid::from_u128(1);
    let segment_id = Uuid::from_u128(10);
    write_full_segment(
        archive.path(),
        SegmentSetup {
            partition: PartitionKey {
                topic_id,
                ..Default::default()
            },
            base_offset,
            segment_id,
        },
    );
    (archive, topic_id, segment_id)
}
