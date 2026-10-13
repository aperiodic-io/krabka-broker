//! The metadata-sharing proof itself: kill the partition leader and read every
//! record back from the survivor.
//!
//! The suite has exactly one test, and it is long because the scenario is: boot
//! three brokers, produce until segments tier, watch the follower evict its own
//! copy of what the leader tiered, wait for the follower's RLMM consumer to
//! catch up, shut the leader down, wait for the surviving quorum to elect the
//! follower, then consume from offset 0. The steps that stand on their own live
//! in the sibling modules; what remains here is the ordering between them and
//! the discriminating assertion, which must keep requiring every record back.

use {
    crate::{
        RECORDS, TOPIC,
        multi_client::fetch_all_records,
        multi_cluster::{
            await_all_brokers_registered, await_all_rlmm_active, start_three_tiered_brokers,
        },
        multi_workload::{
            await_follower_local_eviction, create_tiered_topic, produce_and_await_remote_segments,
        },
        support::client::connect_owned,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::time::{Duration, Instant},
};

/// In-process multi-broker tiered metadata-sharing proof.
///
/// Three brokers share a `Local` remote tier and a topic-backed RLMM with rf=3
/// metadata replication. Broker 1 leads the rf=2 user partition and runs the
/// RLM copy task. Broker 2 only consumes `__remote_log_metadata` to learn the
/// segment locations, and uses that same metadata to evict its own local copy
/// of every tiered segment while broker 1 still leads. After broker 1 shuts
/// down, the surviving 2-out-of-3 quorum commits a new partition-leader
/// record, and broker 2 serves all records from the remote tier. That proves
/// the RLMM metadata-sharing claim.
///
/// The test runs under plain `cargo test`, with no Docker, no `MinIO`, and no
/// host.docker.internal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiered_storage_metadata_sharing_via_survivor() {
    let (b1, b2, b3, log_dirs, remote_dir) = start_three_tiered_brokers().await;

    // Wait for all brokers to see each other registered.
    await_all_brokers_registered(&b1, &b2, &b3).await;
    eprintln!("ITEST: all 3 brokers registered; waiting for topic-backed RLMM to activate");

    // Wait for all RLMMs to activate (metadata topic created + bootstrap done).
    await_all_rlmm_active(&b1, &b2, &b3).await;
    eprintln!("ITEST: RLMM active on all 3 brokers; creating tiered topic");

    // Build an admin client against broker 1 for CreateTopics + Produce.
    let b1_bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());
    let admin = connect_owned(&b1_bootstrap, "tiered-multi-admin", "admin client").await;

    create_tiered_topic(&admin, &b1, &b2).await;
    eprintln!("ITEST: tiered config propagated; discovering partition leader");

    // Discover which of broker 1 / broker 2 is the partition leader.
    // With rf=2 and 3 registered brokers, round-robin assigns [1, 2];
    // broker 1 is the preferred leader. Wait until b1's metadata image names
    // the partition leader as one of the two replicas, then read which.
    let b1_id = b1.node_id();
    let b2_id = b2.node_id();
    b1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|p| p.leader == b1_id || p.leader == b2_id)
    })
    .await;
    let (leader_node_id, follower_node_id, follower_addr) =
        if b1.partition_leader_for_test(TOPIC, 0) == Some(b1_id) {
            let f_addr = format!("127.0.0.1:{}", b2.listen_addr().port());
            (b1_id, b2_id, f_addr)
        } else {
            let f_addr = format!("127.0.0.1:{}", b1.listen_addr().port());
            (b2_id, b1_id, f_addr)
        };
    eprintln!(
        "ITEST: partition leader=broker{leader_node_id} follower=broker{follower_node_id}; \
         producing {RECORDS} records"
    );

    produce_and_await_remote_segments(&admin, remote_dir.path()).await;

    // The follower enforces local retention on its own disk, off the shared
    // RLMM, without ever having run the copy task. Watch it happen while the
    // leader is still up: after the failover below there is no longer a
    // follower to observe, and an eviction seen only after the election would
    // not tell the two behaviors apart.
    let follower_log_dir = log_dirs[usize::try_from(follower_node_id).unwrap() - 1].path();
    eprintln!(
        "ITEST: waiting for follower (broker{follower_node_id}) to evict its sealed segments"
    );
    await_follower_local_eviction(follower_log_dir).await;
    eprintln!("ITEST: follower (broker{follower_node_id}) is down to its active segment");

    // Give the RLMM time to propagate CopySegment metadata to the follower via
    // __remote_log_metadata (rf=3).  Interval=1s → 8 ticks plus consume latency.
    // intentional: the follower's RLMM consumer catching up on
    // __remote_log_metadata has no metadata-image/metric signal to await;
    // wait a fixed propagation window before killing the leader.
    eprintln!("ITEST: waiting 8s for RLMM metadata propagation to follower");
    // real-time wait (not a progress poll): RLMM propagates CopySegment metadata to the follower over the broker's own 1s interval ticks; no in-process observable to poll.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Shut down the partition leader.  The surviving 2/3 quorum (broker 2 + 3)
    // can still commit the partition-leader-election record.
    eprintln!("ITEST: shutting down leader (broker{leader_node_id}); waiting for failover");

    // Drop the admin client before shutdown so its connection doesn't block.
    drop(admin);

    // Move all three handles into Options so we can selectively shut the leader
    // down and retain the survivor.
    let mut opt_b1: Option<BrokerHandle> = Some(b1);
    let mut opt_b2: Option<BrokerHandle> = Some(b2);
    let mut opt_b3: Option<BrokerHandle> = Some(b3);

    if leader_node_id == opt_b1.as_ref().unwrap().node_id() {
        opt_b1.take().unwrap().shutdown().await;
        eprintln!("ITEST: broker1 (leader) shut down");
    } else {
        opt_b2.take().unwrap().shutdown().await;
        eprintln!("ITEST: broker2 (leader) shut down");
    }

    // The surviving replica is whichever of b1/b2 is still alive (the follower).
    let survivor = if follower_node_id
        == opt_b1
            .as_ref()
            .map_or(0, krabka_broker::BrokerHandle::node_id)
    {
        opt_b1.as_ref().unwrap()
    } else {
        opt_b2.as_ref().unwrap()
    };

    // Wait for the survivor to become the user-partition leader.
    // The surviving quorum (broker2 + broker3) commits the new leader record.
    eprintln!("ITEST: waiting for survivor (broker{follower_node_id}) to become partition leader");
    // Failover moves the partition leader off the (killed) old leader; with
    // rf=2 the only surviving replica is the follower, so the new leader can
    // only be `follower_node_id`.
    survivor
        .wait_until_partition_leader_changed(TOPIC, 0, krabka_broker::NodeId(leader_node_id))
        .await;
    eprintln!("ITEST: survivor (broker{follower_node_id}) is now partition leader");

    // Give the survivor's RLMM 3 more reconcile ticks to settle on the
    // now-led partition's metadata (RLMM interval=1s → 3 extra ticks).
    // intentional: RLMM reconcile settling has no image/metric signal to await.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Consume ALL produced records from the survivor at offset 0.
    // These can only come from the shared remote tier (local log evicted by
    // local.retention.bytes=1) using metadata the survivor consumed from
    // __remote_log_metadata (it never ran the copy task itself).
    eprintln!(
        "ITEST: consuming {RECORDS} records from survivor (broker{follower_node_id}) \
         at {follower_addr}"
    );
    let consume_deadline = Instant::now() + Duration::from_mins(1);
    let served = fetch_all_records(&follower_addr, TOPIC, 0, 0, RECORDS, consume_deadline).await;

    eprintln!("ITEST: survivor served {served} records (expected >= {RECORDS})");
    assert!(
        served >= RECORDS,
        "expected >= {RECORDS} records served by the surviving broker via the remote tier; \
         got {served}. The survivor (broker{follower_node_id}) should have learned segment \
         locations from __remote_log_metadata (rf=3) without having run the copy task itself."
    );

    // Shut down surviving brokers.
    if let Some(h) = opt_b1.take() {
        h.shutdown().await;
    }
    if let Some(h) = opt_b2.take() {
        h.shutdown().await;
    }
    if let Some(h) = opt_b3.take() {
        h.shutdown().await;
    }
}
