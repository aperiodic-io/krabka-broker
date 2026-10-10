//! Unit tests for tombstoning the offsets of deleted topics.

use std::collections::{HashMap, HashSet};

use assert2::check;
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::coordinator::{
    test_support::{offset_entry, parsed_offset_records},
    unified::{
        actor::{
            GroupActorMessage,
            test_support::{make_coordinator, rpc},
        },
        classic_state::{ClassicGroup, OffsetEntry},
        group::{CoordinatorGroup, GroupKind},
        persistence::Key,
    },
};

/// The id of the deleted `orders` topic.
const OLD_ORDERS: uuid::Uuid = uuid::Uuid::from_u128(1);
/// The id of the `orders` topic created again with the same name.
const NEW_ORDERS: uuid::Uuid = uuid::Uuid::from_u128(2);

fn entry(offset: i64, topic_id: Option<uuid::Uuid>) -> OffsetEntry {
    OffsetEntry {
        topic_id,
        ..offset_entry(offset)
    }
}

fn key(topic: &str, partition: i32) -> (String, i32) {
    (topic.to_string(), partition)
}

#[derive(Clone, Copy)]
enum AppendResult {
    Success,
    Failure,
}

struct Row {
    name: &'static str,
    deleted: Vec<&'static str>,
    append_result: AppendResult,
    expected_reply: Vec<(String, i32)>,
    expected_records: Vec<(Key, Option<Bytes>)>,
    expected_committed: HashSet<(String, i32)>,
    expected_pending: HashSet<(String, i32)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_topic_tombstones_its_offsets_in_the_group() {
    let all_committed = HashSet::from([
        key("orders", 0),
        key("orders", 1),
        key("orders", 3),
        key("payments", 0),
    ]);
    let rows = [
        Row {
            name: "one of two topics, and the offset of the new topic stays",
            deleted: vec!["orders"],
            append_result: AppendResult::Success,
            expected_reply: vec![key("orders", 0), key("orders", 1), key("orders", 2)],
            expected_records: crate::coordinator::test_support::topic_deletion_tombstones(
                crate::coordinator::test_support::TopicDeletionTombstonesSetup {
                    partitions: &[
                        krabka_ids::PartitionIndex(0),
                        krabka_ids::PartitionIndex(1),
                        krabka_ids::PartitionIndex(2),
                    ],
                    ..Default::default()
                },
            ),
            expected_committed: HashSet::from([key("orders", 3), key("payments", 0)]),
            expected_pending: HashSet::from([key("payments", 1)]),
        },
        Row {
            name: "a topic the group never committed",
            deleted: vec!["unknown"],
            append_result: AppendResult::Success,
            expected_reply: vec![],
            expected_records: vec![],
            expected_committed: all_committed.clone(),
            expected_pending: HashSet::from([key("orders", 2), key("payments", 1)]),
        },
        Row {
            name: "the append fails",
            deleted: vec!["orders", "payments"],
            append_result: AppendResult::Failure,
            expected_reply: vec![],
            expected_records: vec![],
            expected_committed: all_committed.clone(),
            expected_pending: HashSet::from([key("orders", 2), key("payments", 1)]),
        },
    ];

    for row in rows {
        let (coordinator, log) = make_coordinator();
        let mut group = CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(ClassicGroup::new("g")),
            HashMap::from([
                // Replayed from the log, so no topic id.
                (key("orders", 0), entry(10, None)),
                (key("orders", 1), entry(11, Some(OLD_ORDERS))),
                // Committed to the topic created again with the same name.
                (key("orders", 3), entry(13, Some(NEW_ORDERS))),
                (key("payments", 0), entry(20, None)),
            ]),
        );
        group.add_pending_txn_offsets(7, 100, [key("orders", 2)]);
        group.add_pending_txn_offsets(8, 101, [key("payments", 1)]);
        coordinator.seed_classic("g", Box::new(group));
        let handle = coordinator.find("g").unwrap();
        log.fail_next.store(
            matches!(row.append_result, AppendResult::Failure),
            std::sync::atomic::Ordering::SeqCst,
        );

        let (reply, deleted) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::DeleteTopicOffsets {
                topics: row
                    .deleted
                    .iter()
                    .map(|name| ((*name).to_string(), OLD_ORDERS))
                    .collect(),
                reply,
            })
            .await
            .unwrap();
        check!(deleted.await.unwrap() == row.expected_reply, "{}", row.name);

        let records = parsed_offset_records(&log.batches().await);
        check!(records == row.expected_records, "{}", row.name);

        let offsets = rpc::fetch_offsets(&handle).await;
        check!(
            offsets.committed.into_keys().collect::<HashSet<_>>() == row.expected_committed,
            "{}",
            row.name
        );
        check!(offsets.pending_txn == row.expected_pending, "{}", row.name);
    }
}
