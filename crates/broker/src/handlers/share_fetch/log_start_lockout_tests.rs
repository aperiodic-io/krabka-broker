//! End-to-end tests for issue #948: `ShareFetch` used to close the connection
//! once the log start offset moved past the share-partition start offset
//! (SPSO), because the log read that materialization and acquisition both
//! start from answered `OffsetTooLow` and the handler propagated it.
//!
//! Kafka's `ShareFetchUtils.processFetchResponse` catches the equivalent
//! `OFFSET_OUT_OF_RANGE` from the log read, calls
//! `SharePartition.updateCacheAndOffsets` to archive the `Available` records
//! below the new log start and move the SPSO and SPEO, and answers the
//! partition with `NONE`, retrying the fetch so a record already readable at
//! or above the new log start is not held back for another round trip. These
//! tests drive that same scenario against a live broker: produce records,
//! move the log start with a real `DeleteRecords`, and fetch again.
//!
//! The same fixtures serve two more end-to-end tests of the acquire pass: the
//! record lock limit that bounds the window, and a partition whose log read
//! fails beside a healthy one.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::{
    owned::{
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        share_fetch_request::ShareFetchRequest,
        share_fetch_response::PartitionData,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{initialize_share_state, peer, principal, start_broker_no_audit_with},
};

/// One partition row of a multi-partition `ShareFetch`: its
/// `(partition_index, error_code, acquired ranges)`.
type PartitionOutcome = (i32, i16, Vec<(i64, i64)>);

/// `DeleteRecords` v2, matching the other handler tests.
const DELETE_RECORDS_VERSION: i16 = 2;

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(AllowAllAuthorizer)).await
}

async fn create_topic(broker: &BrokerHandle, name: &str, num_partitions: i32) -> WireUuid {
    crate::handlers::test_support::create_topic(
        broker,
        crate::handlers::test_support::ClientTopicSetup {
            client_id: "share-log-start-lockout-test",
            name,
            partitions: crate::handlers::test_support::TopicPartitionCount(num_partitions),
        },
    )
    .await
}

/// Appends `count` one-record batches to `partition_index` of `topic`.
async fn produce_records(broker: &BrokerHandle, topic: &str, partition_index: i32, count: i32) {
    crate::handlers::test_support::produce_records(
        broker,
        crate::handlers::test_support::ProduceRecordsSetup {
            topic,
            partition_index: krabka_ids::PartitionIndex(partition_index),
            count: crate::handlers::test_support::RecordCount(count),
        },
    )
    .await;
}

/// Trims `partition_index` of `topic` to `new_log_start` with a real
/// `DeleteRecords`, and returns the resulting error code.
async fn delete_records(
    broker: &BrokerHandle,
    topic: &str,
    partition_index: i32,
    new_log_start: i64,
) -> i16 {
    let request = DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: topic.to_string(),
            partitions: vec![DeleteRecordsPartition {
                partition_index,
                offset: new_log_start,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("admin"),
        client_id = "admin-client"
    );
    let response =
        crate::handlers::delete_records::handle(&shared, request, DELETE_RECORDS_VERSION, &ctx)
            .await
            .expect("handle delete records");
    response.topics[0].partitions[0].error_code
}

/// Sends a `ShareFetch` naming every partition index of `topic_id` with
/// `max_records`, and returns each row's `PartitionData` in request order.
use krabka_ids::PartitionIndex;

use crate::handlers::test_support::{RecordCount, ShareSessionEpoch};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ShareFetchRowsSetup<'a> {
    #[default("g")]
    group: &'a str,
    #[default(ShareSessionEpoch(0))]
    epoch: ShareSessionEpoch,
    #[default(WireUuid::ZERO)]
    topic_id: WireUuid,
    #[default(&[PartitionIndex(0)])]
    partitions: &'a [PartitionIndex],
    #[default(RecordCount(500))]
    max_records: RecordCount,
}

impl<'a> ShareFetchRowsSetup<'a> {
    fn incremental_pair(group: &'a str, topic_id: WireUuid) -> Self {
        Self {
            group,
            topic_id,
            epoch: ShareSessionEpoch(1),
            partitions: &[PartitionIndex(0), PartitionIndex(1)],
            ..Default::default()
        }
    }
}

async fn share_fetch_rows(
    broker: &BrokerHandle,
    setup: ShareFetchRowsSetup<'_>,
) -> Vec<PartitionData> {
    let request = ShareFetchRequest {
        group_id: Some(setup.group.into()),
        member_id: Some("member".into()),
        share_session_epoch: setup.epoch.0,
        max_wait_ms: 0,
        max_bytes: 1 << 20,
        max_records: setup.max_records.0,
        batch_size: setup.max_records.0.max(1),
        // Record-limit acquisition keeps the priming fetch at exactly the selected count.
        share_acquire_mode: 1,
        topics: crate::handlers::test_support::share_fetch_topics(
            setup.topic_id,
            &setup
                .partitions
                .iter()
                .map(|index| index.0)
                .collect::<Vec<_>>(),
        ),
        ..Default::default()
    };
    crate::handlers::test_support::share_fetch_partition_rows(
        broker,
        &request,
        crate::handlers::test_support::ShareWireSetup::default(),
    )
    .await
}

async fn share_fetch_one(broker: &BrokerHandle, setup: ShareFetchRowsSetup<'_>) -> PartitionData {
    assert!(setup.partitions.len() == 1, "one partition requested");
    let partition_index = setup.partitions[0].0;
    share_fetch_rows(broker, setup)
        .await
        .into_iter()
        .next()
        .unwrap_or(PartitionData {
            partition_index,
            ..Default::default()
        })
}

use crate::handlers::test_support::acquired_share_records as acquired;

fn topic_uuid(topic_id: WireUuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(topic_id.0)
}

async fn spso(broker: &BrokerHandle, group: &str, topic_id: WireUuid, partition_index: i32) -> i64 {
    let shared = broker.broker_arc_for_test();
    shared
        .share_partition_leaders
        .peek_for_test(group, topic_uuid(topic_id), partition_index)
        .expect("a cached share-partition leader")
        .lock()
        .await
        .start_offset
        .0
}

/// One row of the table: what is below the new log start when it moves past
/// the SPSO, and what the handler must do about it.
struct Row {
    name: &'static str,
    /// Records to acquire on the priming fetch, out of the 5 produced, so
    /// that fewer than 5 are handed out and the rest stay `Available`.
    prime_max_records: i32,
    /// The `DeleteRecords` offset: the new log start.
    new_log_start: i64,
    /// The lockout fetch's partition error.
    expected_error: i16,
    /// The lockout fetch's acquired ranges. The handler retries once in
    /// place after repairing the SPSO, so any record already readable at or
    /// above the new log start is handed out on this same pass -- not on a
    /// later fetch.
    expected_acquired: Vec<(i64, i64)>,
    /// The SPSO right after the lockout fetch.
    expected_spso: i64,
    /// What a follow-up fetch acquires: empty whenever the lockout fetch
    /// already handed out everything currently acquirable (the in-place
    /// retry means there is usually nothing left), and still empty for the
    /// "acquired stays locked" row, whose lock the lockout fetch cannot and
    /// must not touch.
    expected_next_acquired: Vec<(i64, i64)>,
}

/// The table from issue #948: nothing in flight, `Available` records below
/// the new start (archived), `Acquired` records below the new start (kept
/// locked until the lock ends), and a log start at the SPSO (no change).
#[tokio::test]
async fn share_fetch_survives_the_log_start_moving_past_the_spso() {
    let (broker, _dir) = start().await;

    let rows = [
        Row {
            name: "nothing in flight",
            prime_max_records: 0,
            new_log_start: 3,
            expected_error: codes::NONE,
            // The lockout fetch repairs the SPSO and retries in place, so it
            // already hands out [3,4] itself.
            expected_acquired: vec![(3, 4)],
            expected_spso: 3,
            expected_next_acquired: vec![],
        },
        Row {
            name: "available records below the new start are archived, acquired stay locked",
            prime_max_records: 2,
            new_log_start: 3,
            expected_error: codes::NONE,
            // [0,1] is Acquired by the priming fetch's member and blocks the
            // SPSO from following the log start; offset 2, which was
            // Available, is archived. Offsets [3,4] are unaffected by either
            // and are handed out on this same lockout fetch, in place.
            expected_acquired: vec![(3, 4)],
            expected_spso: 0,
            // The [0,1] lock is still held, and [3,4] were already handed
            // out above, so nothing new is left for a follow-up fetch.
            expected_next_acquired: vec![],
        },
        Row {
            name: "log start equal to the SPSO changes nothing",
            prime_max_records: 0,
            new_log_start: 0,
            expected_error: codes::NONE,
            expected_acquired: vec![(0, 4)],
            expected_spso: 0,
            expected_next_acquired: vec![],
        },
    ];

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        let topic = format!("lockout-row-{index}");
        let group = format!("g-lockout-row-{index}");
        let topic_id = create_topic(&broker, &topic, 1).await;
        initialize_share_state(&broker, &group, topic_uuid(topic_id), 0).await;

        // Prime the leader cell at SPSO 0 before any record exists, so a
        // later `DeleteRecords` moves the log start out from under a cell
        // that is already cached, exactly as issue #948 describes.
        let mut epoch = 0;
        let opened = share_fetch_one(
            &broker,
            ShareFetchRowsSetup {
                group: &group,
                epoch: ShareSessionEpoch(epoch),
                topic_id,
                ..Default::default()
            },
        )
        .await;
        assert!(opened.error_code == codes::NONE, "{}: {opened:?}", row.name);

        produce_records(&broker, &topic, 0, 5).await;

        // With `prime_max_records > 0`, this acquires that many of the 5
        // produced records, leaving the rest `Available`. It is skipped
        // entirely for a row that wants the window still unmaterialized
        // (`prime_max_records == 0`), so the leader cell's window stays
        // exactly as `initialize_share_state` left it.
        if row.prime_max_records > 0 {
            epoch += 1;
            let priming = share_fetch_rows(
                &broker,
                ShareFetchRowsSetup {
                    group: &group,
                    epoch: ShareSessionEpoch(epoch),
                    topic_id,
                    max_records: RecordCount(row.prime_max_records),
                    ..Default::default()
                },
            )
            .await;
            assert!(
                priming[0].error_code == codes::NONE,
                "{}: priming fetch {:?}",
                row.name,
                priming[0]
            );
        }

        let delete_error = delete_records(&broker, &topic, 0, row.new_log_start).await;
        assert!(delete_error == codes::NONE, "{}: {delete_error}", row.name);

        epoch += 1;
        let lockout = share_fetch_one(
            &broker,
            ShareFetchRowsSetup {
                group: &group,
                epoch: ShareSessionEpoch(epoch),
                topic_id,
                ..Default::default()
            },
        )
        .await;
        let after_lockout_spso = spso(&broker, &group, topic_id, 0).await;
        epoch += 1;
        let next = share_fetch_one(
            &broker,
            ShareFetchRowsSetup {
                group: &group,
                epoch: ShareSessionEpoch(epoch),
                topic_id,
                ..Default::default()
            },
        )
        .await;

        actual.push((
            row.name,
            lockout.error_code,
            acquired(&lockout),
            after_lockout_spso,
            acquired(&next),
        ));
        expected.push((
            row.name,
            row.expected_error,
            row.expected_acquired,
            row.expected_spso,
            row.expected_next_acquired,
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka's `ShareFetchUtils.processFetchResponse` turns the log read error of
/// one partition into that partition's error code with no records, and the
/// other partitions of the request still return theirs. A batch that does not
/// check out is `CORRUPT_MESSAGE`. The request must not fail as a whole, which
/// closes the connection.
#[tokio::test]
async fn an_unreadable_partition_fails_alone() {
    let (broker, dir) = start().await;
    let topic = "unreadable-and-healthy";
    let group = "g-unreadable-and-healthy";
    let topic_id = primed_pair(&broker, topic, group).await;

    // Give the only batch of partition 0 a length of zero, which is shorter
    // than a batch header: bytes 8 to 11 of a record batch are its
    // `batch_length`.
    let segment = std::fs::read_dir(dir.path().join(format!("{topic}-0")))
        .expect("the partition directory")
        .map(|entry| entry.expect("a directory entry").path())
        .find(|path| path.extension().is_some_and(|ext| ext == "log"))
        .expect("a segment file");
    let mut bytes = std::fs::read(&segment).expect("read the segment");
    bytes[8..12].copy_from_slice(&0_i32.to_be_bytes());
    std::fs::write(&segment, bytes).expect("corrupt the segment");

    let rows = share_fetch_rows(
        &broker,
        ShareFetchRowsSetup::incremental_pair(group, topic_id),
    )
    .await;
    let by_partition = sorted_partition_outcomes(&rows);

    assert!(
        by_partition
            == vec![
                (0, codes::CORRUPT_MESSAGE, vec![]),
                (1, codes::NONE, vec![(0, 4)]),
            ]
    );
    broker.shutdown().await;
}

/// A second, healthy partition in the same `ShareFetch` request must still
/// get its records when the first partition's log start has moved past its
/// SPSO: the recovery is per-partition, not per-request.
#[tokio::test]
async fn a_healthy_partition_in_the_same_request_is_unaffected() {
    let (broker, _dir) = start().await;
    let topic = "lockout-and-healthy";
    let group = "g-lockout-and-healthy";
    let topic_id = primed_pair(&broker, topic, group).await;

    // Only partition 0's log start moves past its SPSO.
    let delete_error = delete_records(&broker, topic, 0, 3).await;
    assert!(delete_error == codes::NONE, "{delete_error}");

    let rows = share_fetch_rows(
        &broker,
        ShareFetchRowsSetup::incremental_pair(group, topic_id),
    )
    .await;
    // The partitions rotate by session epoch, so the rows come in either
    // order.
    let by_partition = sorted_partition_outcomes(&rows);

    assert!(
        by_partition
            == vec![
                (0, codes::NONE, vec![(3, 4)]),
                (1, codes::NONE, vec![(0, 4)]),
            ]
    );
    broker.shutdown().await;
}

/// Kafka's `SharePartition` stops taking records once the window from the SPSO
/// to the SPEO reaches `group.share.partition.max.record.locks`, until an
/// acknowledgement, a release or a lock timeout moves the SPSO. A member that
/// fetches again without acknowledging must not get another limit's worth of
/// records each time.
#[tokio::test]
async fn a_member_that_does_not_acknowledge_gets_no_more_than_the_record_lock_limit() {
    let (broker, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.max_inflight_records = 100;
    })
    .await;
    let topic = "record-lock-limit";
    let group = "g-record-lock-limit";
    let topic_id = create_topic(&broker, topic, 1).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 0).await;
    let opened = share_fetch_one(
        &broker,
        ShareFetchRowsSetup {
            group,
            topic_id,
            ..Default::default()
        },
    )
    .await;
    assert!(opened.error_code == codes::NONE, "{opened:?}");
    produce_records(&broker, topic, 0, 250).await;

    let mut per_fetch = Vec::new();
    for epoch in 1..=3 {
        let row = share_fetch_one(
            &broker,
            ShareFetchRowsSetup {
                group,
                epoch: ShareSessionEpoch(epoch),
                topic_id,
                ..Default::default()
            },
        )
        .await;
        per_fetch.push((row.error_code, acquired(&row)));
    }
    let end_offset = broker
        .broker_arc_for_test()
        .share_partition_leaders
        .peek_for_test(group, topic_uuid(topic_id), 0)
        .expect("a cached share-partition leader")
        .lock()
        .await
        .end_offset
        .0;

    assert!(
        (per_fetch, end_offset)
            == (
                vec![
                    (codes::NONE, vec![(0, 99)]),
                    (codes::NONE, vec![]),
                    (codes::NONE, vec![]),
                ],
                100
            )
    );
    broker.shutdown().await;
}

/// Prime both partitions in one session while empty, then append five records each.
async fn primed_pair(broker: &BrokerHandle, topic: &str, group: &str) -> WireUuid {
    let topic_id = create_topic(broker, topic, 2).await;
    initialize_share_state(broker, group, topic_uuid(topic_id), 0).await;
    initialize_share_state(broker, group, topic_uuid(topic_id), 1).await;
    let opened = share_fetch_rows(
        broker,
        ShareFetchRowsSetup {
            group,
            topic_id,
            partitions: &[PartitionIndex(0), PartitionIndex(1)],
            ..Default::default()
        },
    )
    .await;
    assert!(
        opened.iter().all(|row| row.error_code == codes::NONE),
        "{opened:?}"
    );
    produce_records(broker, topic, 0, 5).await;
    produce_records(broker, topic, 1, 5).await;
    topic_id
}

fn sorted_partition_outcomes(rows: &[PartitionData]) -> Vec<PartitionOutcome> {
    let mut outcomes: Vec<_> = rows
        .iter()
        .map(|row| (row.partition_index, row.error_code, acquired(row)))
        .collect();
    outcomes.sort_unstable();
    outcomes
}
