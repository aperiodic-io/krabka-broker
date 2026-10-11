//! Tests for the log walk itself: the inter-batch cursor that
//! [`super::replay::replay_records`] advances, and the transactional records
//! it holds back until a commit marker arrives.

use assert2::check;
use krabka_protocol::records::RecordBatch;

use super::replay::replay_records;
use crate::coordinator::{
    persistence::OffsetCommitValue, test_support::temp_log, unified::actor::test_support::rpc,
};

fn append_record(log: &mut krabka_log::Log, record: krabka_protocol::records::Record) {
    let mut batch = RecordBatch {
        records: vec![record],
        ..RecordBatch::default()
    };
    log.append(&mut batch).unwrap();
}

/// `replay_records` must walk EVERY batch in the log, not the first batch
/// only.
///
/// The cursor that advances between batches is
/// `base_offset + last_offset_delta + 1`. A two-record first batch, where
/// `last_offset_delta == 1`, followed by a second batch replays fully only
/// when that arithmetic is exact. Both offset-commit records from the
/// first batch AND the commit from the second batch must land in
/// `acc.committed`.
#[tokio::test]
async fn replay_records_walks_all_batches() {
    use krabka_protocol::records::Record;

    let coord = super::test_support::bare_coordinator();

    // Build an offset-commit record with a distinct committed offset per
    // (topic, partition), so we can tell which batches were replayed.
    let commit_record = super::test_support::commit_record;

    let (_dir, mut log) = temp_log();

    // First batch spans TWO offsets (last_offset_delta == 1): partitions 0
    // and 1 commit at offsets 100 and 101.
    let mut batch1 = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch1.records.push(commit_record(0, 100));
    batch1.records.push(Record {
        offset_delta: 1,
        ..commit_record(1, 101)
    });
    log.append(&mut batch1).unwrap();

    // Second batch: partition 2 commits at offset 202. Reaching it requires
    // the inter-batch cursor to have advanced past the first batch.
    let mut batch2 = RecordBatch::default();
    batch2.records.push(commit_record(2, 202));
    log.append(&mut batch2).unwrap();

    let replayed = replay_records(&log, &coord).unwrap();
    let committed = replayed
        .committed
        .get("g")
        .expect("group g has committed offsets");

    // All three commits present — the second batch is only reached when the
    // cursor arithmetic `base_offset + last_offset_delta + 1` is exact.
    check!(committed.len() == 3);
    check!(committed[&("t".to_string(), 0)].offset == 100);
    check!(committed[&("t".to_string(), 1)].offset == 101);
    check!(committed[&("t".to_string(), 2)].offset == 202);
}

#[test]
fn replay_applies_only_committed_transactional_offsets() {
    use krabka_log::{Offset, ProducerId};
    use krabka_protocol::records::Attributes;

    use crate::txn::marker::{MarkerType, build_marker_batch};

    let coordinator = super::test_support::bare_coordinator();
    let record = super::test_support::commit_record;
    let transactional = |partition: i32, offset: i64| RecordBatch {
        producer_id: 7,
        producer_epoch: 0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![record(partition, offset)],
        ..RecordBatch::default()
    };

    let (_dir, mut log) = temp_log();
    log.append(&mut transactional(0, 111)).unwrap();
    log.append(&mut build_marker_batch(
        ProducerId(7),
        0,
        Offset(0),
        MarkerType::Commit,
        0,
    ))
    .unwrap();
    log.append(&mut transactional(1, 222)).unwrap();
    log.append(&mut build_marker_batch(
        ProducerId(7),
        0,
        Offset(0),
        MarkerType::Abort,
        0,
    ))
    .unwrap();
    log.append(&mut transactional(2, 333)).unwrap();

    let replayed = replay_records(&log, &coordinator).unwrap();
    let committed = replayed.committed.get("g").expect("committed transaction");
    check!(committed.len() == 1);
    check!(committed[&("t".to_string(), 0)].offset == 111);
    check!(!committed.contains_key(&("t".to_string(), 1)));
    check!(!committed.contains_key(&("t".to_string(), 2)));
}

/// KIP-447 across a coordinator reload. A transaction whose offset commits the
/// log ends without a marker for is still open: its marker can arrive after
/// the reload. Replay must not apply those offsets, and must not forget them
/// either -- the group has to keep answering `UNSTABLE_OFFSET_COMMIT` to a
/// `require_stable` `OffsetFetch` for exactly those keys.
#[tokio::test]
async fn replay_carries_an_open_transactions_offsets_forward_as_pending() {
    use krabka_log::Offset;
    use krabka_protocol::records::{Attributes, Record};

    use super::{replay::finalize, test_support::bare_coordinator};

    let coordinator = bare_coordinator();
    let (_dir, mut log) = temp_log();
    log.append(&mut RecordBatch {
        producer_id: 7,
        producer_epoch: 0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![Record {
            key: Some(OffsetCommitValue::encode_key("g", "t", 4).unwrap()),
            value: Some(
                OffsetCommitValue {
                    offset: Offset(444),
                    leader_epoch: -1,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                    expire_timestamp_ms: None,
                    topic_id: None,
                }
                .encode_value(),
            ),
            ..Default::default()
        }],
        ..RecordBatch::default()
    })
    .unwrap();

    let replayed = replay_records(&log, &coordinator).unwrap();
    check!(!replayed.committed.contains_key("g"));
    check!(
        replayed.pending_txn["g"][&7]
            == super::replay::PendingTxnKeys {
                written_at: 0,
                keys: std::collections::HashSet::from([("t".to_string(), 4)]),
            }
    );

    // The group is offset-only and open-transaction-only, so `finalize` still
    // has to spawn an actor for it and hand it the marks.
    finalize(&coordinator, replayed).await;
    let handle = coordinator.find("g").expect("seeded classic actor");
    let offsets = rpc::fetch_offsets(&handle).await;
    check!(offsets.committed.is_empty());
    check!(offsets.pending_txn == std::collections::HashSet::from([("t".to_string(), 4)]));
}

/// A group can have more producers with open transactions than its actor's
/// mailbox holds, and every one of their marks has to reach the actor. A mark
/// that goes missing is a partition that answers a `require_stable`
/// `OffsetFetch` with the stale committed offset the transaction is replacing,
/// until some later replay recovers it -- the duplicate processing KIP-447
/// exists to prevent.
///
/// The test runs on the current-thread runtime, where the freshly spawned
/// actor cannot drain a single message while `finalize` holds the thread. A
/// seed that does not await therefore loses everything past the mailbox
/// capacity, deterministically.
#[tokio::test]
async fn replay_seeds_every_open_transaction_past_the_actor_mailbox() {
    use std::collections::{HashMap, HashSet};

    use super::{
        replay::{PendingTxnKeys, Replayed, finalize},
        test_support::bare_coordinator_with_mailbox,
    };
    use crate::coordinator::unified::GroupSeed;

    const MAILBOX: usize = 2;
    const PRODUCERS: i64 = 8;

    let coordinator = bare_coordinator_with_mailbox(MAILBOX);
    // A next-gen group: `finalize` spawns its actor and hands it the recovered
    // state by message, rather than building the group in process the way it
    // does for a classic one.
    coordinator
        .seeds
        .insert("g".to_string(), GroupSeed::default());

    let mut by_producer: HashMap<i64, PendingTxnKeys> = HashMap::new();
    for producer_id in 0..PRODUCERS {
        by_producer.insert(
            producer_id,
            PendingTxnKeys {
                written_at: producer_id,
                keys: HashSet::from([("t".to_string(), i32::try_from(producer_id).unwrap())]),
            },
        );
    }
    let replayed = Replayed {
        pending_txn: HashMap::from([("g".to_string(), by_producer)]),
        ..Replayed::default()
    };

    finalize(&coordinator, replayed).await;

    let handle = coordinator.find("g").expect("seeded consumer actor");
    let offsets = rpc::fetch_offsets(&handle).await;
    let expected: HashSet<(String, i32)> = (0..PRODUCERS)
        .map(|producer_id| ("t".to_string(), i32::try_from(producer_id).unwrap()))
        .collect();
    check!(offsets.pending_txn == expected);
}

/// Replay honours the tombstones the offset-retention sweep and `OffsetDelete`
/// write: a null-valued offset key drops that offset, and a null-valued group
/// key drops the group without touching the offsets that outlive it. This is
/// what `GroupMetadataManager.loadGroupsAndOffsets` does.
#[test]
fn replay_honours_offset_and_group_tombstones() {
    use krabka_protocol::records::Record;

    use crate::coordinator::persistence::GroupMetadataValue;

    let coordinator = super::test_support::bare_coordinator();
    let commit = super::test_support::commit_record;
    let tombstone = |key: bytes::Bytes| Record {
        key: Some(key),
        value: None,
        ..Default::default()
    };
    let group_metadata = Record {
        key: Some(GroupMetadataValue::encode_key("g").unwrap()),
        value: Some(
            GroupMetadataValue {
                protocol_type: "consumer".into(),
                generation: 3,
                protocol_name: None,
                leader: None,
                current_state_timestamp_ms: 777,
                members: Vec::new(),
            }
            .encode_value()
            .unwrap(),
        ),
        ..Default::default()
    };

    let (_dir, mut log) = temp_log();
    for record in [
        commit(0, 100),
        commit(1, 101),
        group_metadata,
        tombstone(OffsetCommitValue::encode_key("g", "t", 0).unwrap()),
    ] {
        append_record(&mut log, record);
    }

    let replayed = replay_records(&log, &coordinator).unwrap();
    let committed = replayed.committed.get("g").expect("group g");
    check!(!committed.contains_key(&("t".to_string(), 0)));
    check!(committed[&("t".to_string(), 1)].offset == 101);
    // The memberless snapshot records when the group emptied, which is where
    // the offset-retention sweep measures from after a restart.
    check!(replayed.empty_since.get("g") == Some(&777));
    check!(replayed.classic.contains_key("g"));

    // The group's own tombstone drops the group and its empty-since stamp, and
    // leaves the surviving offset alone.
    let mut batch = RecordBatch {
        records: vec![tombstone(GroupMetadataValue::encode_key("g").unwrap())],
        ..RecordBatch::default()
    };
    log.append(&mut batch).unwrap();
    let replayed = replay_records(&log, &coordinator).unwrap();
    check!(!replayed.classic.contains_key("g"));
    check!(!replayed.empty_since.contains_key("g"));
    check!(replayed.committed["g"][&("t".to_string(), 1)].offset == 101);
}

/// A group the retention sweep reaped in full — every offset tombstoned and
/// then the group's own record — must not come back as a live group after a
/// restart.
///
/// The sweep writes both tombstones in one batch, so replay reads the offset
/// tombstones first. Leaving the group id behind with an empty offsets map is
/// enough for `finalize` to spawn a classic actor for it, which puts a group
/// the operator already reaped back on `ListGroups` after every restart.
#[tokio::test]
async fn a_fully_reaped_group_does_not_come_back_after_replay() {
    use krabka_protocol::records::Record;

    use super::replay::finalize;
    use crate::coordinator::persistence::GroupMetadataValue;

    let coordinator = super::test_support::bare_coordinator();
    let tombstone = |key: bytes::Bytes| Record {
        key: Some(key),
        value: None,
        ..Default::default()
    };

    let (_dir, mut log) = temp_log();
    for record in [
        super::test_support::commit_record_for_group("reaped", 0, 100),
        // The sweep's batch: the last offset, then the group itself.
        tombstone(OffsetCommitValue::encode_key("reaped", "t", 0).unwrap()),
        tombstone(GroupMetadataValue::encode_key("reaped").unwrap()),
    ] {
        append_record(&mut log, record);
    }

    let replayed = replay_records(&log, &coordinator).unwrap();
    check!(!replayed.committed.contains_key("reaped"));
    check!(!replayed.classic.contains_key("reaped"));

    finalize(&coordinator, replayed).await;
    check!(coordinator.find("reaped").is_none());
}

/// #987: a replayed offset keeps the topic id its version 4 record carries,
/// as Kafka's `OffsetAndMetadata.fromRecord` does, so a restarted coordinator
/// still tells the offsets of a re-created topic apart. A version 1 record,
/// written for a per-commit expiry, carries none.
#[tokio::test]
async fn replay_keeps_the_committed_topic_id() {
    use krabka_log::Offset;
    use krabka_protocol::records::Record;

    use crate::coordinator::unified::classic_state::OffsetEntry;

    let coord = super::test_support::bare_coordinator();
    let topic_id = uuid::Uuid::from_u128(0xABCD);
    let entry = |offset: i64, expire_timestamp_ms, topic_id| OffsetEntry {
        offset: Offset(offset),
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms,
        topic_id,
    };
    let record = |partition: i32, delta: i32, entry: &OffsetEntry| Record {
        offset_delta: delta,
        key: Some(OffsetCommitValue::encode_key("g", "t", partition).unwrap()),
        value: Some(OffsetCommitValue::from(entry).encode_value()),
        ..Default::default()
    };

    let (_dir, mut log) = temp_log();
    let mut batch = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch
        .records
        .push(record(0, 0, &entry(10, None, Some(topic_id))));
    batch
        .records
        .push(record(1, 1, &entry(11, Some(9_000), Some(topic_id))));
    log.append(&mut batch).unwrap();

    let replayed = replay_records(&log, &coord).unwrap();

    check!(
        replayed.committed.get("g")
            == Some(&std::collections::HashMap::from([
                (("t".to_string(), 0), entry(10, None, Some(topic_id))),
                (("t".to_string(), 1), entry(11, Some(9_000), None)),
            ]))
    );
}
