//! The one request fixture shared by the `TxnOffsetCommit` unit tests: a
//! two-partition commit for a single topic, which both the response builders
//! and the `__consumer_offsets` append are checked against.

use {
    krabka_ids::ProducerId,
    krabka_protocol::{
        owned::txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::collections::HashSet,
};

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
pub(super) struct ProducerEpoch(pub i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TxnCommitProducer {
    #[default(ProducerId(42))]
    pub id: ProducerId,
    pub epoch: ProducerEpoch,
}

impl TxnCommitProducer {
    /// A producer before its first epoch bump.
    pub(super) fn initial(id: ProducerId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct TxnCommitSetup<'a> {
    #[default("tid".into())]
    pub transactional_id: String,
    #[default("group-a")]
    pub group_id: &'a str,
    pub producer: TxnCommitProducer,
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// A transactional commit request whose remaining wire fields keep their defaults.
#[bon::builder]
pub(super) fn request_for(
    #[builder(default = "tid" . into ())] transactional_id: String,
    #[builder(default = "group-a")] group_id: &'_ str,
    #[builder(default)] producer: TxnCommitProducer,
    #[builder(default)] topics: Vec<TxnOffsetCommitRequestTopic>,
) -> TxnOffsetCommitRequest {
    let setup = TxnCommitSetup {
        transactional_id,
        group_id,
        producer,
        topics,
    };
    TxnOffsetCommitRequest {
        transactional_id: setup.transactional_id,
        group_id: setup.group_id.to_owned(),
        producer_id: setup.producer.id.0,
        producer_epoch: setup.producer.epoch.0,
        topics: setup.topics,
        ..Default::default()
    }
}

/// A request topic whose partition rows keep their wire defaults.
#[bon::builder]
pub(super) fn request_topic(
    #[builder(into)] name: String,
    partitions: Vec<i32>,
    #[builder(default)] topic_id: WireUuid,
) -> TxnOffsetCommitRequestTopic {
    TxnOffsetCommitRequestTopic {
        name,
        topic_id,
        partitions: partitions
            .into_iter()
            .map(|partition_index| TxnOffsetCommitRequestPartition {
                partition_index,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn request() -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest {
        transactional_id: "tid".into(),
        group_id: "group-a".into(),
        producer_id: 47,
        producer_epoch: 5,
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "orders".into(),
            partitions: vec![
                TxnOffsetCommitRequestPartition {
                    partition_index: 2,
                    committed_offset: 103,
                    committed_leader_epoch: 7,
                    committed_metadata: Some("first".into()),
                    ..Default::default()
                },
                TxnOffsetCommitRequestPartition {
                    partition_index: 3,
                    committed_offset: 107,
                    committed_leader_epoch: 8,
                    committed_metadata: Some("second".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A topic name and a partition index: the key of one offset row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RowKey<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
}

/// Visits every requested key, including refused rows, against the supplied append oracle.
pub(super) fn check_appended_keys(
    topics: &[TxnOffsetCommitRequestTopic],
    appended: &[RowKey<'_>],
    mut check: impl FnMut(RowKey<'_>, bool),
) {
    let appended: HashSet<RowKey<'_>> = appended.iter().copied().collect();
    for topic in topics {
        for partition in &topic.partitions {
            let key = RowKey {
                topic: topic.name.as_str(),
                partition: partition.partition_index,
            };
            check(key, appended.contains(&key));
        }
    }
}
