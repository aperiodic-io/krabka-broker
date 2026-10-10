//! Tests for the distributed flavour of `QuorumWalStore`, where durability
//! comes from remote fsync acknowledgements: which acknowledgement counts
//! towards the watermark, how a voter-set change takes effect, and what a
//! reopen must not truncate.

use std::sync::Arc;

use assert2::assert;
use krabka_ids::{Offset, PartitionIndex};
use krabka_kraft_core::NodeId;
use uuid::Uuid;

use super::{
    QuorumWalStore,
    test_support::{append_source, batch, distributed_store, open_log, source_log},
};
use crate::wal::WalStore;

/// Wait until the spawned `sync_durable` has fsynced `offset` and recorded the
/// leader's own vote for it. The fsync runs off the test thread, so a fixed
/// pause proves nothing about whether the vote is in yet, and a follower
/// acknowledgement that arrives before it is one vote of three, not a majority.
async fn wait_for_leader_vote(store: &QuorumWalStore, offset: Offset) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while store
            .engine
            .voter_durable_offset(NodeId(1))
            .is_none_or(|voted| voted < offset)
        {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the leader records its own fsync as a vote");
}

#[tokio::test]
async fn distributed_wal_waits_for_a_remote_fsync_ack() {
    let dir = tempfile::tempdir().unwrap();
    let source = source_log(dir.path());
    let store = Arc::new(distributed_store(source, Uuid::from_u128(99), 3));
    let metrics = crate::metrics::BrokerMetrics::new();
    store.engine.attach_observability(
        crate::wal::quorum::registry::ShardId {
            topic_id: Uuid::from_u128(99),
            partition: PartitionIndex(0),
        },
        metrics.clone(),
    );
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let (_results, leo) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let mut sync = tokio::spawn(async move { syncing.sync_durable(leo).await });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
            .await
            .is_err()
    );
    wait_for_leader_vote(&store, leo).await;
    assert!(!store.engine.record_follower_ack(NodeId(9), leo));
    assert!(!store.engine.record_follower_ack(NodeId(1), leo));
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(leo.0 + 1))
    );
    assert!(store.engine.record_follower_ack(NodeId(2), leo));

    assert!(sync.await.unwrap().unwrap() == leo);
    assert!(store.engine.durable_watermark() == leo);
    assert!(
        metrics
            .diskless_wal_durable_watermark
            .get_or_create(&crate::metrics::WalShardLabel {
                topic_id: Uuid::from_u128(99).to_string(),
                partition: 0,
            })
            .get()
            == leo.0
    );
    assert!(store.engine.replica_end_offsets() == vec![leo]);
    assert!(store.trim_to_offset(leo).await.unwrap() == leo);
    assert!(store.engine.replica_start_offsets() == vec![leo]);
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(leo.0 - 1))
    );

    let (_results, next) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let mut sync = tokio::spawn(async move { syncing.sync_durable(next).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
            .await
            .is_err()
    );
    wait_for_leader_vote(&store, next).await;
    store.engine.configure_distributed(NodeId(1), &[]);
    let error = sync.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("placement disappeared"));
    assert!(metrics.diskless_wal_quorum_loss_events_total.get() == 1);
}

#[tokio::test]
async fn durable_advance_waits_for_an_offset_strictly_after_the_observation() {
    let dir = tempfile::tempdir().unwrap();
    let source = source_log(dir.path());
    let store = Arc::new(distributed_store(source, Uuid::new_v4(), 3));
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let (_results, first) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let sync = tokio::spawn(async move { syncing.sync_durable(first).await });
    // The spawned sync has not run, so the leader has not fsynced `first`:
    // one follower's fsync alone is not a majority of durable copies.
    assert!(!store.engine.record_follower_ack(NodeId(2), first));
    assert!(sync.await.unwrap().unwrap() == first);

    let engine = Arc::clone(&store.engine);
    let mut waiting = tokio::spawn(async move { engine.wait_for_durable_advance(first).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );

    let (_results, second) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let sync = tokio::spawn(async move { syncing.sync_durable(second).await });
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(first.0 - 1))
    );
    assert!(!store.engine.record_follower_ack(NodeId(2), second));

    assert!(sync.await.unwrap().unwrap() == second);
    assert!(waiting.await.unwrap() == second);
}

async fn appended_distributed_store() -> (tempfile::TempDir, QuorumWalStore, Offset) {
    let dir = tempfile::tempdir().unwrap();
    let store = super::test_support::fresh_distributed_store(dir.path());
    let (_, end) = append_source(&store, 1).await;
    (dir, store, end)
}

#[tokio::test]
async fn distributed_wal_rejects_misordered_incomplete_or_duplicate_voter_sets() {
    for voters in [
        vec![NodeId(2), NodeId(1), NodeId(3)],
        vec![NodeId(1), NodeId(2)],
        vec![NodeId(1), NodeId(2), NodeId(2)],
        vec![NodeId(1), NodeId(1), NodeId(2)],
    ] {
        let (_dir, store, leo) = appended_distributed_store().await;

        store.engine.configure_distributed(NodeId(1), &voters);

        assert!(!store.engine.record_follower_ack(NodeId(2), leo));
        assert!(store.engine.durable_watermark() == Offset(0));
    }
}

#[tokio::test]
async fn distributed_wal_reconfiguration_replaces_the_remote_voter_set() {
    let (_dir, store, leo) = appended_distributed_store().await;
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);

    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(3), NodeId(4)]);

    assert!(!store.engine.record_follower_ack(NodeId(2), leo));
    // Voter 3 is a remote voter of the new set, but the leader has not
    // fsynced `leo` yet, so its acknowledgement alone commits nothing.
    assert!(!store.engine.record_follower_ack(NodeId(3), leo));
    assert!(store.engine.durable_watermark() == Offset(0));
    assert!(store.sync_durable(leo).await.unwrap() == leo);
    assert!(store.engine.durable_watermark() == leo);
}

#[tokio::test]
async fn distributed_wal_reopens_without_truncating_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let source_dir = dir.path().join("source");
    let source = open_log(&source_dir);
    let mut batch = batch(2);
    source.lock().unwrap().append(&mut batch).unwrap();
    source.lock().unwrap().sync().unwrap();
    let store = distributed_store(source.clone(), Uuid::from_u128(100), 3);
    assert!(store.engine.durable_watermark() == Offset(0));
    assert!(store.engine.replica_end_offsets() == vec![Offset(2)]);
    drop(store);
    drop(source);

    let source = open_log(&source_dir);
    let reopened = distributed_store(source, Uuid::from_u128(100), 3);

    assert!(reopened.engine.durable_watermark() == Offset(0));
    assert!(reopened.engine.replica_end_offsets() == vec![Offset(2)]);
    reopened
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    assert!(reopened.engine.record_follower_ack(NodeId(2), Offset(2)));
    assert!(reopened.engine.durable_watermark() == Offset(2));
}
