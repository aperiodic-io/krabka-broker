//! `DeleteGroups` across a coordinator reload.
//!
//! A deleted group must stay deleted when its `__consumer_offsets` partition
//! replays again. Kafka's `GroupCoordinatorShard.deleteGroups` writes an
//! `OffsetCommit` tombstone for every committed offset and every open
//! transactional offset of the group, then the group tombstone, all in one
//! batch. These tests delete a replayed group, append the delete batch to the
//! same log, and replay the log into a new coordinator.

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::records::Record;

use super::replay::{finalize, replay_records};
use crate::{
    coordinator::{
        persistence::Key,
        test_support::{parsed_offset_records, replay_offset_batch as batch},
        unified::{GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog},
    },
    txn::marker::{MarkerType, build_marker_batch},
};

fn coordinator(log: Arc<InMemoryOffsetsLog>) -> Arc<GroupCoordinator> {
    crate::coordinator::test_support::default_coordinator(
        crate::coordinator::unified::actor::test_support::empty_metadata(),
        log,
    )
}

fn commit(topic: &str, partition: i32, offset: i64) -> Record {
    crate::coordinator::test_support::offset_record(
        crate::coordinator::test_support::OffsetRecordSetup {
            topic,
            partition: krabka_ids::PartitionIndex(partition),
            offset: krabka_log::Offset(offset),
            ..Default::default()
        },
    )
}

#[derive(Clone, Copy)]
enum MarkerAfterDelete {
    None,
    Commit,
}

struct Row {
    name: &'static str,
    /// Plain offset commits of group `g` in the log before the delete.
    committed: Vec<Record>,
    /// Offset commits of an open transaction of producer 7.
    transactional: Vec<Record>,
    /// Append a commit marker for producer 7 after the delete batch.
    marker_after_delete: MarkerAfterDelete,
    expected_batch: Vec<(Key, Option<Bytes>)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_group_and_its_offsets_stay_deleted_after_replay() {
    let orders_group_tombstones = crate::coordinator::test_support::topic_deletion_tombstones(
        crate::coordinator::test_support::TopicDeletionTombstonesSetup {
            partitions: &[krabka_ids::PartitionIndex(0), krabka_ids::PartitionIndex(1)],
            target: crate::coordinator::test_support::DeletionTarget::Group,
            ..Default::default()
        },
    );
    let rows = [
        Row {
            name: "no offsets",
            committed: vec![],
            transactional: vec![],
            marker_after_delete: MarkerAfterDelete::None,
            expected_batch: crate::coordinator::test_support::deletion_tombstones(
                crate::coordinator::test_support::DeletionTombstonesSetup {
                    target: crate::coordinator::test_support::DeletionTarget::Group,
                    ..Default::default()
                },
            ),
        },
        Row {
            name: "two offsets on two topics",
            committed: vec![commit("orders", 0, 10), commit("payments", 3, 30)],
            transactional: vec![],
            marker_after_delete: MarkerAfterDelete::None,
            expected_batch: crate::coordinator::test_support::deletion_tombstones(
                crate::coordinator::test_support::DeletionTombstonesSetup {
                    offsets: &[
                        ("orders", krabka_ids::PartitionIndex(0)),
                        ("payments", krabka_ids::PartitionIndex(3)),
                    ],
                    target: crate::coordinator::test_support::DeletionTarget::Group,
                    ..Default::default()
                },
            ),
        },
        Row {
            name: "one pending transactional offset",
            committed: vec![commit("orders", 0, 10)],
            transactional: vec![commit("orders", 1, 11)],
            marker_after_delete: MarkerAfterDelete::None,
            expected_batch: orders_group_tombstones.clone(),
        },
        Row {
            name: "transaction commits after the delete",
            committed: vec![commit("orders", 0, 10)],
            transactional: vec![commit("orders", 0, 12), commit("orders", 1, 11)],
            marker_after_delete: MarkerAfterDelete::Commit,
            expected_batch: orders_group_tombstones.clone(),
        },
    ];

    for row in rows {
        let (_dir, mut log) = crate::coordinator::test_support::temp_log();
        // A classic GroupMetadata record makes the group exist even when it has
        // no offsets.
        let (group_key, group_value) = super::test_support::classic_group_record("g", "m1");
        let empty_group = crate::coordinator::persistence::GroupMetadataValue {
            members: vec![],
            leader: None,
            ..crate::coordinator::persistence::GroupMetadataValue::decode_value(&group_value)
                .unwrap()
        };
        log.append(&mut batch(
            None,
            vec![Record {
                key: Some(group_key),
                value: Some(empty_group.encode_value().unwrap()),
                ..Default::default()
            }],
        ))
        .unwrap();
        if !row.committed.is_empty() {
            log.append(&mut batch(None, row.committed)).unwrap();
        }
        if !row.transactional.is_empty() {
            log.append(&mut batch(Some(7), row.transactional)).unwrap();
        }

        let offsets_log = Arc::new(InMemoryOffsetsLog::default());
        let before = coordinator(offsets_log.clone());
        finalize(&before, replay_records(&log, &before).unwrap()).await;
        check!(before.delete_group("g").await == Ok(()), "{}", row.name);

        let appended = offsets_log.batches().await;
        let written = parsed_offset_records(&appended);
        check!(appended.len() == 1, "{}", row.name);
        check!(written == row.expected_batch, "{}", row.name);

        for mut delete in appended {
            log.append(&mut delete).unwrap();
        }
        if matches!(row.marker_after_delete, MarkerAfterDelete::Commit) {
            log.append(&mut build_marker_batch(
                ProducerId(7),
                0,
                Offset(0),
                MarkerType::Commit,
                0,
            ))
            .unwrap();
        }

        let after = coordinator(Arc::new(InMemoryOffsetsLog::default()));
        let replayed = replay_records(&log, &after).unwrap();
        check!(!replayed.committed.contains_key("g"), "{}", row.name);
        check!(!replayed.pending_txn.contains_key("g"), "{}", row.name);
        finalize(&after, replayed).await;
        check!(after.find("g").is_none(), "{}", row.name);
    }
}
