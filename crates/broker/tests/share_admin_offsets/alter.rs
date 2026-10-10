//! Tests for `AlterShareGroupOffsets`, `api_key` 91.
//!
//! Alter resets the share-partition start offset of an empty group, and the
//! broker fences it with `NON_EMPTY_GROUP` while the group still has a live
//! member.

use std::time::Duration;

use assert2::assert;
use krabka_protocol::owned::alter_share_group_offsets_request::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
    AlterShareGroupOffsetsRequestTopic,
};

use crate::{
    describe::describe_until,
    harness::{NON_EMPTY_GROUP, NONE, fetch_until_acquired, join, wait_for_share_init},
};

/// Alter resets the SPSO of an empty group.
///
/// The persister re-initializes its state at the requested offset, the broker
/// invalidates the leader cache, and a later `ShareFetch` acquires from the new
/// offset.
///
/// The group has *no members* when Alter runs, so the membership lifecycle has
/// never seeded the share-state. Alter thus initializes from absent at the
/// group's current epoch, 1, with `start_offset = 5`, and records the
/// partition as initialized, as Kafka's `alterShareGroupOffsets` does. A later
/// first join finds the partition initialized and does not initialize it
/// again, so the SPSO from Alter survives and the first `ShareFetch` acquires
/// from offset 5. The test thus exercises the real acquire path against the
/// reset and invalidated leader cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_resets_empty_group() {
    let (_permit, _broker, client, _dir, tid) =
        crate::harness::seeded_empty_group(crate::support::share::ShareRecordSeedSetup {
            count: crate::support::share::ShareRecordCount(6),
        })
        .await;

    // Alter: reset SPSO to 5 on the empty (never-joined) group. This
    // initializes-from-absent at the group epoch 1, and invalidates the
    // (empty) leader cache. Retry while the persister leadership is still
    // settling.
    let mut altered = false;
    for _ in 0..40 {
        let resp = client
            .send(AlterShareGroupOffsetsRequest {
                group_id: "g1".into(),
                topics: vec![AlterShareGroupOffsetsRequestTopic {
                    topic_name: "t".into(),
                    partitions: vec![AlterShareGroupOffsetsRequestPartition {
                        partition_index: 0,
                        start_offset: 5,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("AlterShareGroupOffsets");
        if resp.error_code == NONE && resp.responses[0].partitions[0].error_code == NONE {
            altered = true;
            break;
        }
        // intentional: bounded retry of the Alter mutation RPC while the share
        // persister leadership settles; coordinator-local state with no
        // metadata-image signal or awaiter.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(altered, "AlterShareGroupOffsets never succeeded");

    // Describe now reports the new SPSO.
    let group = describe_until(&client, "g1", "t", vec![0], 5).await;
    assert!(
        group.topics[0].partitions[0].start_offset == 5,
        "SPSO must be 5 after Alter, got {}",
        group.topics[0].partitions[0].start_offset
    );

    // Join and ShareFetch: must acquire starting at offset 5 (the reset SPSO).
    // The first join does not initialize the already-initialized partition
    // again, so the acquire reads the reset SPSO 5 via the invalidated leader
    // cache.
    let (member, _epoch) = join(&client, "g1", "t").await;
    let row = fetch_until_acquired(
        &client,
        crate::support::share::ShareSessionSetup::opening(&member, tid),
    )
    .await;
    assert!(
        row.acquired_records[0].first_offset == 5,
        "fetch after Alter must acquire from offset 5, got {:?}",
        row.acquired_records
    );
}

/// Alter on a non-empty group is rejected at the top level.
///
/// The group has a live member, so the response carries `NON_EMPTY_GROUP`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_non_empty_group_fenced() {
    let (_permit, broker, client, _dir, tid) =
        crate::harness::seeded_empty_group(crate::support::share::ShareRecordSeedSetup::default())
            .await;

    // Live member present (steady-state heartbeat), never leaves.
    let (_member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;

    let resp = client
        .send(AlterShareGroupOffsetsRequest {
            group_id: "g1".into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: "t".into(),
                partitions: vec![AlterShareGroupOffsetsRequestPartition {
                    partition_index: 0,
                    start_offset: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterShareGroupOffsets");
    assert!(
        resp.error_code == NON_EMPTY_GROUP,
        "alter on non-empty group must be NON_EMPTY_GROUP (68), got {}",
        resp.error_code
    );
}
