//! The one request fixture shared by the `TxnOffsetCommit` unit tests: a
//! two-partition commit for a single topic, which both the response builders
//! and the `__consumer_offsets` append are checked against.

use std::collections::HashSet;

use krabka_ids::ProducerId;
use krabka_protocol::owned::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};

#[derive(Clone, Copy, Default)]
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

    pub(super) fn from_wire((id, epoch): (i64, i16)) -> Self {
        Self {
            id: ProducerId(id),
            epoch: ProducerEpoch(epoch),
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
pub(super) fn request_for(setup: TxnCommitSetup<'_>) -> TxnOffsetCommitRequest {
    TxnOffsetCommitRequest {
        transactional_id: setup.transactional_id,
        group_id: setup.group_id.to_owned(),
        producer_id: setup.producer.id.0,
        producer_epoch: setup.producer.epoch.0,
        topics: setup.topics,
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

/// Visits every requested key, including refused rows, against the supplied append oracle.
pub(super) fn check_appended_keys(
    topics: &[TxnOffsetCommitRequestTopic],
    appended: &[(&str, i32)],
    mut check: impl FnMut((&str, i32), bool),
) {
    let appended: HashSet<(&str, i32)> = appended.iter().copied().collect();
    for topic in topics {
        for partition in &topic.partitions {
            let key = (topic.name.as_str(), partition.partition_index);
            check(key, appended.contains(&key));
        }
    }
}
