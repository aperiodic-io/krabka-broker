//! Test for the restore of `delivery_complete_count` across a restart.
//!
//! The delivery complete count is Kafka's `SharePartition.deliveryCompleteCount`:
//! the number of Acknowledged and Archived records in the in-flight window at
//! or above the SPSO. Share-group lag is `end - start - count`, so a recovered
//! group that reset it to 0 would over-report its lag. The test lives apart
//! from the admin-RPC surfaces because it asserts on the recovered share-state
//! summary rather than on a Describe, Alter, or Delete response.

use assert2::assert;

use crate::{
    harness::{
        InitializedRestartFixture, acquired_count, fetch_until_acquired,
        initialized_restart_fixture,
    },
    support::share::{
        ShareAck, ShareRecordCount, ShareRecordSeedSetup, ShareSessionSetup, acknowledge_success,
        rejoin_group,
    },
};

async fn assert_share_summary(
    broker: &krabka_broker::BrokerHandle,
    tid: uuid::Uuid,
    expected_dcc: i32,
) {
    let summary = broker
        .share_state_summary_for_test("g1", tid, 0)
        .await
        .map(|(_, _, start, dcc)| (start, dcc));
    assert!(summary == Some((0, expected_dcc)));
}

/// Lag restore: `delivery_complete_count` survives a broker restart.
///
/// Produce N. Acquire all of them and Accept `1..N-1`, so offset 0 still holds
/// the SPSO at 0 and the window holds N-1 terminal records. Wait for the
/// persist. Restart on the same dir with Rejoin. Then read the share-state
/// summary: the SPSO is 0 and the count is the restored N-1, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_complete_count_restored_across_restart() {
    const N: i64 = 4;
    const COMPLETE: i32 = 3; // N - 1
    let InitializedRestartFixture {
        _permit,
        _dir,
        log_dir,
        topic: initial,
    } = initialized_restart_fixture(ShareRecordSeedSetup {
        count: ShareRecordCount(N),
    })
    .await;

    let tid;
    {
        let (broker, client, topic, member) = initial;
        tid = topic;

        // Acquire 0..N-1 and Accept 1..N-1: the SPSO stays at 0 behind the
        // still-acquired offset 0, and the window holds N-1 terminal records.
        let row = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
        assert!(acquired_count(&row) == N, "must acquire all {N} offsets");
        acknowledge_success(
            &client,
            ShareAck::prefix_for(&member, tid, krabka_ids::Offset(N - 1))
                .starting_at(krabka_ids::Offset(1)),
        )
        .await;

        // Wait until the persisted summary reflects the count before restarting.
        broker
            .wait_until_share_delivery_complete("g1", tid, 0, COMPLETE)
            .await;
        assert_share_summary(&broker, tid, COMPLETE).await;

        // The awaiter above confirms the count is durable; shut down immediately.
        broker.shutdown().await;
    }

    {
        let (broker, _client) = rejoin_group(log_dir, "g1").await;

        // The summary load is driven by the share coordinator reading the
        // persisted record; await until the recovered state is present.
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        assert_share_summary(&broker, tid, COMPLETE).await;
    }
}
