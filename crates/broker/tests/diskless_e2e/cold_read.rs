//! Flush, trim, then read a trimmed offset back out of the object store.
//!
//! The WAL quorum keeps a committed prefix on local disks only until the
//! flusher has moved it into the object store and the index record naming it
//! has been committed and projected. After that the local logs are trimmed and
//! the only copy of those offsets is the flushed object.
//!
//! What makes this a real cold read, and not an accidental local hit, is the
//! assertion that the partition's log start has actually passed the offset
//! being fetched. Below the log start there is nothing left to serve locally,
//! so `OFFSET_OUT_OF_RANGE` is the only answer the log path can give and the
//! object store is the only thing that can rescue it.

use std::time::Duration;

use assert2::assert;

use crate::{
    RECORDS, TOPIC,
    wire::{
        OFFSET_OUT_OF_RANGE, assert_matches_produced, delete_records_below, earliest_offset,
        fetch_error_code, fetch_log,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_diskless_offsets_are_served_from_the_object_store() {
    // Flush often, and keep no safety lag behind the committed index frontier,
    // so the trim reaches every offset the flush covered.
    let (cluster, admin, topic_id, leader, _values, producer) =
        crate::topic::flushing_workload(krabka_units::millis(100)).await;

    // Wait for the flusher to publish an index record covering the whole
    // committed prefix and for the trim behind it to move the log start off
    // zero.
    let (leader_broker, committed) = crate::topic::seeded_leader(&cluster, leader);
    let (log_start, high_watermark, frontier) = await_trimmed_flush(leader_broker, committed).await;
    assert!(frontier == Some(committed));
    assert!(high_watermark == committed);
    assert!(
        log_start > 0,
        "offset 0 is still on local disk, so a fetch of it would not be a cold read"
    );

    // Offset 0 is now below the partition's log start. Only the flushed object
    // can answer for it.
    let cold = fetch_log(
        &cluster.bootstrap_for_node(leader),
        topic_id,
        0,
        RECORDS,
        Duration::from_secs(90),
    )
    .await;
    assert_matches_produced(&cold, 0, RECORDS);

    drop(producer);
    drop(admin);
    cluster.shutdown().await;
}

/// Poll the leader's own flush inputs until the committed prefix is both
/// flushed and trimmed, and return `(log start, high watermark, projected
/// flush frontier)`.
///
/// `diskless_flush_state_for_test` reports the same values the flusher itself
/// reads, so this waits on the real flush pipeline rather than on a proxy for
/// it. The `diskless` flag is asserted here because every other assertion in
/// this case would also hold for an ordinary topic that simply never trimmed.
async fn await_trimmed_flush(
    leader: &krabka_broker::BrokerHandle,
    committed: i64,
) -> (i64, i64, Option<i64>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let state = leader.diskless_flush_state_for_test(TOPIC, 0).await;
        if let Some((diskless, _, log_start, _, high_watermark, frontier)) = state {
            assert!(
                diskless,
                "the topic must be diskless for this case to mean anything"
            );
            if log_start > 0 && frontier == Some(committed) {
                return (log_start, high_watermark, frontier);
            }
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "the diskless flusher did not flush and trim the committed prefix; last state \
             was {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `DeleteRecords` on a diskless topic has to reach the object tier, not just
/// the local WAL.
///
/// The trim frontier is no help here. By the time this runs the flusher has
/// already trimmed the local log past every offset the case deletes, so a
/// broker that measured the delete against `log_start_offset` would call the
/// request a no-op and go on serving the deleted records out of the bucket for
/// as long as the topic lived. The two assertions below are what an operator
/// checks after running `kafka-delete-records`: the deleted offsets are gone,
/// and the ones above the floor are not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_diskless_records_leave_the_object_store_unreadable() {
    let (cluster, admin, topic_id, leader, _values, producer) =
        crate::topic::flushing_workload(krabka_units::millis(100)).await;
    let bootstrap = cluster.bootstrap_for_node(leader);

    let (leader_broker, committed) = crate::topic::seeded_leader(&cluster, leader);
    let (log_start, _, _) = await_trimmed_flush(leader_broker, committed).await;
    // Everything the case deletes is already below the local log start, so the
    // delete can only be answered by the index.
    let floor = 4;
    assert!(
        log_start > floor,
        "the flusher has to have trimmed past the delete point for this case to \
         mean anything; log start was {log_start}"
    );
    // The whole range still reads back from the object store first, so a later
    // OFFSET_OUT_OF_RANGE cannot be a cold read that never worked.
    let before = fetch_log(&bootstrap, topic_id, 0, RECORDS, Duration::from_secs(90)).await;
    assert_matches_produced(&before, 0, RECORDS);
    assert!(earliest_offset(&bootstrap).await == 0);

    let low_watermark = delete_records_below(&bootstrap, floor).await;

    assert!(low_watermark == floor);
    assert!(earliest_offset(&bootstrap).await == floor);
    assert!(fetch_error_code(&bootstrap, topic_id, 0).await == OFFSET_OUT_OF_RANGE);
    assert!(fetch_error_code(&bootstrap, topic_id, floor - 1).await == OFFSET_OUT_OF_RANGE);
    // Above the floor the object store still answers.
    let kept = fetch_log(
        &bootstrap,
        topic_id,
        floor,
        RECORDS - usize::try_from(floor).expect("small floor"),
        Duration::from_secs(90),
    )
    .await;
    assert!(kept.records[0].0 == floor);

    drop(producer);
    drop(admin);
    cluster.shutdown().await;
}
