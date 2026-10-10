//! Protocol fixtures shared by coordinator retention and deletion tests.

use std::sync::Arc;

use bytes::Bytes;
use krabka_log::Offset;
use krabka_protocol::{
    owned::{
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
    },
    records::{Attributes, Record, RecordBatch},
};

use crate::{
    broker::Broker,
    coordinator::unified::{
        GroupCoordinator,
        actor::MetadataProvider,
        classic_state::OffsetEntry,
        config::NextGenConfig,
        offsets_log::OffsetsLog,
        persistence::{Key, parse_key},
        share::config::ShareGroupConfig,
        streams::config::StreamsGroupConfig,
    },
    test_support::{peer, principal, request_context},
};

krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// Canonical partition/epoch pairs from an optional actual assignment map.
pub(crate) fn sorted_epoch_pairs(
    epochs: Option<&std::collections::HashMap<i32, i32>>,
) -> Vec<(i32, i32)> {
    let mut pairs: Vec<(i32, i32)> = epochs
        .map(|epochs| epochs.iter().map(|(&p, &e)| (p, e)).collect())
        .unwrap_or_default();
    pairs.sort_unstable();
    pairs
}

/// A topic-delta fixture using cluster id one and applying records in order.
pub(crate) fn metadata_delta_image(
    records: &[krabka_metadata::MetadataRecord],
) -> krabka_metadata::MetadataImage {
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::from_u128(1), records)
}

/// The offsets topic with id seven, one listed partition, and the supplied replicas.
/// The image retains a nil cluster id and default partition fields except its leader term.
pub(crate) fn offsets_partition_image(
    (partition, leader, epoch): (i32, krabka_metadata::NodeId, i32),
    replicas: &[krabka_metadata::NodeId],
) -> krabka_metadata::MetadataImage {
    krabka_metadata::MetadataImage::from_records(
        uuid::Uuid::nil(),
        &[
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: crate::coordinator::bootstrap::OFFSETS_TOPIC.into(),
                topic_id: uuid::Uuid::from_u128(7),
                partitions: partition + 1,
                replication_factor: i16::try_from(replicas.len()).unwrap(),
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: crate::coordinator::bootstrap::OFFSETS_TOPIC.into(),
                partition,
                leader,
                leader_epoch: krabka_metadata::LeaderEpoch(epoch),
                replicas: replicas.to_vec(),
                isr: replicas.to_vec(),
                ..krabka_metadata::PartitionRecord::default()
            }),
        ],
    )
}

/// An independent expected committed-offset entry with the ordinary fixture fields.
pub(crate) fn offset_entry(offset: i64) -> OffsetEntry {
    OffsetEntry {
        offset: Offset(offset),
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
        topic_id: None,
    }
}

/// A replay fixture with enumerated record offsets and the original protocol
/// defaults for the remaining batch headers, including leader epoch zero.
pub(crate) fn replay_offset_batch(producer_id: Option<i64>, records: Vec<Record>) -> RecordBatch {
    let records: Vec<Record> = records
        .into_iter()
        .zip(0..)
        .map(|(record, offset_delta)| Record {
            offset_delta,
            ..record
        })
        .collect();
    RecordBatch {
        producer_id: producer_id.unwrap_or(-1),
        producer_epoch: if producer_id.is_some() { 0 } else { -1 },
        attributes: Attributes::default().with_transactional(producer_id.is_some()),
        last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
        records,
        ..RecordBatch::default()
    }
}

/// Read the keys and values actually persisted in the test log, in record order.
pub(crate) fn parsed_offset_records(batches: &[RecordBatch]) -> Vec<(Key, Option<Bytes>)> {
    batches
        .iter()
        .flat_map(|batch| &batch.records)
        .map(|record| {
            (
                parse_key(record.key.as_ref().unwrap()).unwrap(),
                record.value.clone(),
            )
        })
        .collect()
}

/// The leader epoch of every encoded batch below `end`, in physical log order.
pub(crate) fn read_batch_epochs(log: &krabka_log::Log, end: Offset) -> Vec<i32> {
    let read = log
        .read_raw(Offset(0), end, krabka_units::mebibytes(1))
        .expect("read the log");
    let mut cursor: &[u8] = &read.bytes;
    let mut epochs = Vec::new();
    while !cursor.is_empty() {
        let batch = RecordBatch::decode(&mut cursor).expect("decode a batch");
        epochs.push(batch.partition_leader_epoch);
    }
    epochs
}

/// A default log and the directory that must outlive it.
pub(crate) fn temp_log() -> (tempfile::TempDir, krabka_log::Log) {
    let dir = tempfile::tempdir().unwrap();
    let log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    (dir, log)
}

/// Coordinator fixtures with the broker's unchanged default membership settings.
pub(crate) fn default_coordinator(
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
) -> Arc<GroupCoordinator> {
    coordinator_with_config(NextGenConfig::default(), metadata, offsets_log)
}

pub(crate) fn coordinator_with_config(
    config: NextGenConfig,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        config,
        ShareGroupConfig::default(),
        metadata,
        offsets_log,
        StreamsGroupConfig::default(),
    ))
}

/// A nontransactional offset record with the ordinary unversioned fixture fields.
#[derive(Clone, Copy)]
pub(crate) struct OffsetRecordSetup<'a> {
    pub group: &'a str,
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    pub offset: Offset,
}

impl Default for OffsetRecordSetup<'_> {
    fn default() -> Self {
        Self {
            group: "g",
            topic: "t",
            partition: krabka_ids::PartitionIndex::default(),
            offset: Offset::ZERO,
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) enum DeletionTarget {
    #[default]
    OffsetsOnly,
    Group,
}

/// An ordered deletion batch, optionally ending with the group's tombstone.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct DeletionTombstonesSetup<'a> {
    #[default("g")]
    pub group: &'a str,
    pub offsets: &'a [(&'a str, krabka_ids::PartitionIndex)],
    pub target: DeletionTarget,
}

pub(crate) fn deletion_tombstones(
    setup: DeletionTombstonesSetup<'_>,
) -> Vec<(crate::coordinator::persistence::Key, Option<bytes::Bytes>)> {
    use crate::coordinator::persistence::Key;
    let mut records: Vec<_> = setup
        .offsets
        .iter()
        .map(|(topic, partition)| {
            (
                Key::OffsetCommit {
                    group_id: setup.group.into(),
                    topic: (*topic).into(),
                    partition: partition.0,
                },
                None,
            )
        })
        .collect();
    if matches!(setup.target, DeletionTarget::Group) {
        records.push((
            Key::GroupMetadata {
                group_id: setup.group.into(),
            },
            None,
        ));
    }
    records
}

/// Deletion expectations for ordered partitions of a single topic.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct TopicDeletionTombstonesSetup<'a> {
    #[default("g")]
    pub group: &'a str,
    #[default("orders")]
    pub topic: &'a str,
    pub partitions: &'a [krabka_ids::PartitionIndex],
    pub target: DeletionTarget,
}

pub(crate) fn topic_deletion_tombstones(
    setup: TopicDeletionTombstonesSetup<'_>,
) -> Vec<(crate::coordinator::persistence::Key, Option<bytes::Bytes>)> {
    let offsets: Vec<_> = setup
        .partitions
        .iter()
        .map(|partition| (setup.topic, *partition))
        .collect();
    deletion_tombstones(DeletionTombstonesSetup {
        group: setup.group,
        offsets: &offsets,
        target: setup.target,
    })
}

pub(crate) fn offset_record(setup: OffsetRecordSetup<'_>) -> krabka_protocol::records::Record {
    let OffsetRecordSetup {
        group,
        topic,
        partition,
        offset,
    } = setup;
    let value = crate::coordinator::persistence::OffsetCommitValue {
        offset,
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
        topic_id: None,
    };
    krabka_protocol::records::Record {
        key: Some(
            crate::coordinator::persistence::OffsetCommitValue::encode_key(
                group,
                topic,
                partition.0,
            )
            .unwrap(),
        ),
        value: Some(value.encode_value()),
        ..Default::default()
    }
}

pub(crate) fn commit_request(group: &str, topic: &str, offset: i64) -> OffsetCommitRequest {
    OffsetCommitRequest {
        group_id: group.into(),
        generation_id_or_member_epoch: -1,
        topics: vec![OffsetCommitRequestTopic {
            name: topic.into(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: offset,
                committed_leader_epoch: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) async fn fetch_offset(broker: &Broker, group: &str, topic: &str, version: i16) -> i64 {
    let request = OffsetFetchRequest {
        group_id: group.into(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: topic.into(),
            partition_indexes: vec![0],
            ..Default::default()
        }]),
        ..Default::default()
    };
    let principal = principal("admin");
    let peer = peer();
    let context = request_context(&principal, &peer, "consumer");
    let response = crate::handlers::offset_fetch::handle(broker, request, version, &context)
        .await
        .expect("OffsetFetch");
    response.topics[0].partitions[0].committed_offset
}

/// Land the commit-wait fixture's changes after its wait has started. Capture
/// statements and leader/notify actions stay explicit at each caller.
macro_rules! schedule_commit_changes {
    ($case:ident, $partition:ident;
        captures { $($captures:tt)* }
        leader($leader:ident, $epoch:ident) { $($leader_action:tt)* }
        notify { $($notify:tt)* }
    ) => {{
        $($captures)*
        let $partition = ::std::sync::Arc::clone(&$partition);
        let (moves_to, hw_later) = ($case.moves_to, $case.hw_later);
        ::tokio::spawn(async move {
            // The wait must start before the changes land.
            ::tokio::time::sleep(::std::time::Duration::from_millis(20)).await;
            if let Some(($leader, $epoch)) = moves_to {
                $($leader_action)*
            }
            if let Some(hw) = hw_later {
                $partition.replica_state.lock().await.hw = ::krabka_log::Offset(hw);
                $($notify)*
            }
        })
    }};
}
pub(crate) use schedule_commit_changes;

pub(crate) struct CommitWaitCase {
    pub what: &'static str,
    pub hw_now: i64,
    pub moves_to: Option<(u64, i32)>,
    pub hw_later: Option<i64>,
    pub timeout: std::time::Duration,
    pub expected: CommitWaitOutcome,
}

#[derive(Clone, Copy)]
pub(crate) enum CommitWaitOutcome {
    Committed,
    NotLeader,
    TimedOut,
}

impl CommitWaitOutcome {
    /// Independent Kafka error expectation for the commit-wait matrix.
    pub(crate) fn kafka_error(self) -> Option<crate::test_support::KafkaErrorCode> {
        match self {
            Self::Committed => None,
            Self::NotLeader => Some(crate::test_support::KafkaErrorCode(
                crate::codes::NOT_COORDINATOR,
            )),
            Self::TimedOut => Some(crate::test_support::KafkaErrorCode(
                crate::codes::COORDINATOR_NOT_AVAILABLE,
            )),
        }
    }
}

pub(crate) fn commit_wait_cases() -> [CommitWaitCase; 6] {
    use std::time::Duration;

    use CommitWaitOutcome::{Committed, NotLeader, TimedOut};
    let long = Duration::from_secs(30);
    [
        CommitWaitCase {
            what: "already committed",
            hw_now: 2,
            moves_to: None,
            hw_later: None,
            timeout: long,
            expected: Committed,
        },
        CommitWaitCase {
            what: "committed when the followers catch up",
            hw_now: 0,
            moves_to: None,
            hw_later: Some(2),
            timeout: long,
            expected: Committed,
        },
        CommitWaitCase {
            what: "another broker takes the partition first",
            hw_now: 0,
            moves_to: Some((2, 1)),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "this broker leads again, at a newer epoch",
            hw_now: 0,
            moves_to: Some((1, 1)),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "the high watermark passes the records after the partition moved",
            hw_now: 0,
            moves_to: Some((2, 1)),
            hw_later: Some(2),
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "the followers never catch up",
            hw_now: 0,
            moves_to: None,
            hw_later: None,
            timeout: Duration::from_millis(100),
            expected: TimedOut,
        },
    ]
}
