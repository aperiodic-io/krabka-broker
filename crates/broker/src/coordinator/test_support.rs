//! Protocol fixtures shared by coordinator retention and deletion tests.

use {
    crate::{
        broker::Broker,
        codes::{COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR},
        coordinator::{
            bootstrap::OFFSETS_TOPIC,
            persistence::{Key, OffsetCommitValue},
            unified::{
                GroupCoordinator,
                actor::MetadataProvider,
                classic_state::OffsetEntry,
                config::NextGenConfig,
                offsets_log::OffsetsLog,
                persistence::{Key as NextGenKey, parse_key},
                share::config::ShareGroupConfig,
                streams::config::StreamsGroupConfig,
            },
        },
        handlers::offset_fetch::handle,
        test_support::{KafkaErrorCode, peer, principal, request_context},
    },
    bytes::Bytes,
    krabka_log::Offset,
    krabka_protocol::{
        owned::{
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
        },
        records::{Attributes, Record, RecordBatch},
    },
    std::sync::Arc,
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

/// A configuration key and its value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConfigEntry<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

/// The configuration entry `key` set to `value`.
pub(crate) fn entry<'a>(key: &'a str, value: &'a str) -> ConfigEntry<'a> {
    ConfigEntry { key, value }
}

/// Owned configuration entries by key.
pub(crate) fn config_map(
    entries: &[ConfigEntry<'_>],
) -> std::collections::BTreeMap<String, String> {
    entries
        .iter()
        .map(|entry| (entry.key.to_owned(), entry.value.to_owned()))
        .collect()
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
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &[
        krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: OFFSETS_TOPIC.into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: partition + 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }),
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: OFFSETS_TOPIC.into(),
            partition,
            leader,
            leader_epoch: krabka_metadata::LeaderEpoch(epoch),
            replicas: replicas.to_vec(),
            isr: replicas.to_vec(),
            ..krabka_metadata::PartitionRecord::default()
        }),
    ])
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

/// A record of the offsets topic: its parsed key, and its value or `None` for
/// a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OffsetsRecord {
    pub key: NextGenKey,
    pub value: Option<Bytes>,
}

/// Read the keys and values actually persisted in the test log, in record order.
pub(crate) fn parsed_offset_records(batches: &[RecordBatch]) -> Vec<OffsetsRecord> {
    batches
        .iter()
        .flat_map(|batch| &batch.records)
        .map(|record| OffsetsRecord {
            key: parse_key(record.key.as_ref().unwrap()).unwrap(),
            value: record.value.clone(),
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
pub(crate) struct TempLog {
    pub dir: tempfile::TempDir,
    pub log: krabka_log::Log,
}

pub(crate) fn temp_log() -> TempLog {
    let dir = tempfile::tempdir().unwrap();
    let log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    TempLog { dir, log }
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

/// A partition of a topic that is named by the topic's name.
#[derive(Clone, Copy)]
pub(crate) struct NamedPartition<'a> {
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
}

/// An ordered deletion batch, optionally ending with the group's tombstone.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct DeletionTombstonesSetup<'a> {
    #[default("g")]
    pub group: &'a str,
    pub offsets: &'a [NamedPartition<'a>],
    pub target: DeletionTarget,
}

#[bon::builder]
pub(crate) fn deletion_tombstones(
    #[builder(default = "g")] group: &'_ str,
    #[builder(default)] offsets: &'_ [NamedPartition<'_>],
    #[builder(default)] target: DeletionTarget,
) -> Vec<OffsetsRecord> {
    let setup = DeletionTombstonesSetup {
        group,
        offsets,
        target,
    };
    let mut records: Vec<_> = setup
        .offsets
        .iter()
        .map(|&NamedPartition { topic, partition }| OffsetsRecord {
            key: Key::OffsetCommit {
                group_id: setup.group.into(),
                topic: topic.into(),
                partition: partition.0,
            },
            value: None,
        })
        .collect();
    if matches!(setup.target, DeletionTarget::Group) {
        records.push(OffsetsRecord {
            key: Key::GroupMetadata {
                group_id: setup.group.into(),
            },
            value: None,
        });
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

#[bon::builder]
pub(crate) fn topic_deletion_tombstones(
    #[builder(default = "g")] group: &'_ str,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default)] partitions: &'_ [krabka_ids::PartitionIndex],
    #[builder(default)] target: DeletionTarget,
) -> Vec<OffsetsRecord> {
    let setup = TopicDeletionTombstonesSetup {
        group,
        topic,
        partitions,
        target,
    };
    let offsets: Vec<_> = setup
        .partitions
        .iter()
        .map(|&partition| NamedPartition {
            topic: setup.topic,
            partition,
        })
        .collect();
    deletion_tombstones()
        .group(setup.group)
        .offsets(&offsets)
        .target(setup.target)
        .call()
}

#[bon::builder]
pub(crate) fn offset_record(
    #[builder(default = OffsetRecordSetup::default().group)] group: &'_ str,
    #[builder(default = OffsetRecordSetup::default().topic)] topic: &'_ str,
    #[builder(into, default = OffsetRecordSetup::default().partition)]
    partition: krabka_ids::PartitionIndex,
    #[builder(into, default = OffsetRecordSetup::default().offset)] offset: Offset,
) -> krabka_protocol::records::Record {
    let setup = OffsetRecordSetup {
        group,
        topic,
        partition,
        offset,
    };
    let OffsetRecordSetup {
        group,
        topic,
        partition,
        offset,
    } = setup;
    let value = OffsetCommitValue {
        offset,
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
        topic_id: None,
    };
    krabka_protocol::records::Record {
        key: Some(OffsetCommitValue::encode_key(group, topic, partition.0).unwrap()),
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
    let response = handle(broker, request, version, &context)
        .await
        .expect("OffsetFetch");
    response.topics[0].partitions[0].committed_offset
}

/// The partition's new leader and leader epoch after it moved.
#[derive(Clone, Copy)]
pub(crate) struct LeaderMove {
    pub leader: u64,
    pub epoch: i32,
}

pub(crate) struct CommitWaitCase {
    pub what: &'static str,
    pub hw_now: i64,
    pub moves_to: Option<LeaderMove>,
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
    pub(crate) fn kafka_error(self) -> Option<KafkaErrorCode> {
        match self {
            Self::Committed => None,
            Self::NotLeader => Some(KafkaErrorCode(NOT_COORDINATOR)),
            Self::TimedOut => Some(KafkaErrorCode(COORDINATOR_NOT_AVAILABLE)),
        }
    }
}

pub(crate) fn commit_wait_cases() -> [CommitWaitCase; 6] {
    use {
        CommitWaitOutcome::{Committed, NotLeader, TimedOut},
        std::time::Duration,
    };
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
            moves_to: Some(LeaderMove {
                leader: 2,
                epoch: 1,
            }),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "this broker leads again, at a newer epoch",
            hw_now: 0,
            moves_to: Some(LeaderMove {
                leader: 1,
                epoch: 1,
            }),
            hw_later: None,
            timeout: long,
            expected: NotLeader,
        },
        CommitWaitCase {
            what: "the high watermark passes the records after the partition moved",
            hw_now: 0,
            moves_to: Some(LeaderMove {
                leader: 2,
                epoch: 1,
            }),
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
