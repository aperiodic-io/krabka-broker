//! What each KIP-932 acknowledgement type does to an acquired batch. Accept
//! advances the share-partition start offset, and the advance survives a
//! broker restart because the broker persisted it to the share coordinator.
//! Release re-delivers the same offsets at a higher `delivery_count`. Reject
//! archives them, so the start offset moves past the poison record.

use {
    crate::{
        harness::{
            ConsumptionFixture, broker_config, broker_test_permit, consumption_fixture,
            initialize_consumption, join, produce_n,
        },
        share_rpc::{
            AcquiredRecordCount, acquired_count, fetch_count, fetch_empty, fetch_until_acquired,
            share_fetch,
        },
        support::share::{
            ConsumptionSetup, RefetchSetup, RetryEpochs, ShareAck, ShareRecordCount,
            ShareRecordSeedSetup, ShareRecordsSetup, ShareSessionEpoch, ShareSessionSetup,
            ShareTopicSetup, acknowledge_success, refetch_while_empty, rejoin_group, start_topic,
        },
    },
    assert2::{assert, check},
};

/// Acquire 3 records, Accept them all, and observe the SPSO advance. The test
/// then restarts the broker on the same data dir to prove the broker persisted
/// the advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_accept_restart() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let (broker, client, topic) =
            start_topic(broker_config(log_dir.clone()), ShareTopicSetup::default()).await;
        tid = topic;
        let (member, _) = initialize_consumption(&broker, &client, ConsumptionSetup {
            topic_id: tid,
            ..Default::default()
        })
        .await;
        let session = ShareSessionSetup::joined(&member, tid);

        // First fetch (epoch 0 opens the session): acquire offsets 0..2.
        let row = fetch_until_acquired(&client, session.with_epoch(ShareSessionEpoch(0))).await;
        check!(
            acquired_count(&row) == 3,
            "must acquire all 3 offsets, got {:?}",
            row.acquired_records
        );
        check!(
            row.acquired_records.iter().all(|r| r.delivery_count == 1),
            "first delivery_count must be 1, got {:?}",
            row.acquired_records
        );
        check!(
            row.records.is_some(),
            "acquired records must carry record bytes"
        );

        // Accept offsets 0..2 (session epoch is now 1 after the open).
        acknowledge_success(
            &client,
            ShareAck::prefix_for(&member, tid, krabka_ids::Offset(2)),
        )
        .await;

        // Next fetch (epoch 2): the SPSO advanced past 2 — nothing left.
        fetch_empty(&client, session.fetch_at(ShareSessionEpoch(2))).await;

        // Wait until the persister has landed the advanced SPSO (>= 3, past
        // offset 2) in __share_group_state before shutting down, so the
        // restart below sees the durable SPSO.
        broker.wait_until_share_spso("g1", tid, 0, 3).await;
        broker.shutdown().await;
    }

    {
        let (broker, client) = rejoin_group(log_dir, "g1").await;

        // A fresh member rejoins the recovered group; a fresh-session fetch
        // must observe the recovered SPSO (past offset 2) — zero acquired.
        // Wait until the share state is recovered on the new broker, then
        // assert in a single fetch (no timing guess needed).
        let (member, _) = join(&client, "g1", "t").await;
        let session = ShareSessionSetup::joined(&member, tid);
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        let row = share_fetch(&client, session.fetch_at(ShareSessionEpoch(0))).await;
        let acquired = acquired_count(&row);
        assert!(
            acquired == 0,
            "recovered SPSO must skip the accepted records; re-acquired {acquired}"
        );
    }
}

/// Release re-delivers the same offsets with an incremented `delivery_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers() {
    let AcquiredPair { fixture, row } = acquired_pair().await;
    let session = fixture.session();

    assert!(row.acquired_records.iter().all(|r| r.delivery_count == 1));

    // Release offsets 0..1 (epoch 1).
    acknowledge_success(
        &fixture.client,
        ShareAck::prefix_for(&fixture.member, fixture.tid, krabka_ids::Offset(1)).release(),
    )
    .await;

    // Next fetch (epoch 2): the same offsets are re-acquired at delivery_count 2.
    let row2 = fetch_count(
        &fixture.client,
        session.fetch_at(ShareSessionEpoch(2)),
        AcquiredRecordCount(2),
    )
    .await;
    assert!(
        row2.acquired_records.iter().all(|r| r.delivery_count == 2),
        "redelivery must bump delivery_count to 2, got {:?}",
        row2.acquired_records
    );
}

/// Reject archives the records: the broker never re-delivers them AND the SPSO
/// advances past them. A freshly produced offset is the only thing the test
/// acquires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_archives() {
    let AcquiredPair { fixture, row: _row } = acquired_pair().await;
    let session = fixture.session();

    // Reject offsets 0..1 (epoch 1) → archived.
    acknowledge_success(
        &fixture.client,
        ShareAck::prefix_for(&fixture.member, fixture.tid, krabka_ids::Offset(1)).reject(),
    )
    .await;

    // Next fetch (epoch 2): nothing re-acquired.
    fetch_empty(&fixture.client, session.fetch_at(ShareSessionEpoch(2))).await;

    // Produce one more (offset 2). The SPSO advanced past the rejected pair, so
    // only the new offset is acquired — proving the rejected ones were skipped.
    produce_n(&fixture.client, ShareRecordsSetup {
        topic_id: fixture.tid,
        ..Default::default()
    })
    .await;
    let row3 = share_fetch(&fixture.client, session.fetch_at(ShareSessionEpoch(3))).await;
    let row3 = refetch_while_empty(&fixture.client, row3, RefetchSetup {
        session: ShareSessionSetup {
            member: &fixture.member,
            topic_id: fixture.tid,
            ..Default::default()
        },
        epochs: RetryEpochs {
            start: ShareSessionEpoch(4),
            ..Default::default()
        },
    })
    .await;
    assert!(
        acquired_count(&row3) == 1,
        "only the new offset must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 2 && row3.acquired_records[0].last_offset == 2,
        "acquired offset must be 2 (past the rejected 0..1), got {:?}",
        row3.acquired_records
    );
}

struct AcquiredPair {
    fixture: ConsumptionFixture,
    row: krabka_protocol::owned::share_fetch_response::PartitionData,
}

async fn acquired_pair() -> AcquiredPair {
    let fixture = consumption_fixture(ShareRecordSeedSetup {
        count: ShareRecordCount(2),
    })
    .await;
    let row = acquire_both(&fixture.client, &fixture.member, fixture.tid).await;
    AcquiredPair { fixture, row }
}

async fn acquire_both(
    client: &krabka_client_core::Client,
    member: &str,
    tid: uuid::Uuid,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let row = fetch_until_acquired(client, ShareSessionSetup::opening(member, tid)).await;
    assert!(acquired_count(&row) == 2, "acquire both offsets");
    row
}
