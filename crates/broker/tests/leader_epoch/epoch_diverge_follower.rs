//! The follower side of KIP-320, end to end: a follower holding a divergent
//! suffix truncates it in band on the leader's `diverging_epoch` response and
//! converges back, without ever issuing an `OffsetForLeaderEpoch` RPC.
//!
//! This is the only test in the suite that needs a real three-broker cluster,
//! because the truncation is done by the follower's own running replicator, so
//! it carries the cluster setup that the single-broker fixture cannot give it.

use {
    crate::{
        cluster_lock,
        epoch_harness::record,
        support::{
            self,
            client::connect_client,
            produce::{batch_request, replicated_topic_produce_setup},
            registered_cluster, shutdown_cluster,
            topics::AssignedTopicSetup,
        },
    },
    assert2::assert,
};

/// KIP-320 follower side, end to end. A follower whose local log has a
/// divergent suffix beyond the leader's epoch boundary must truncate that
/// suffix *in band*, on the leader's `diverging_epoch` Fetch response. It must
/// never issue an `OffsetForLeaderEpoch` RPC. That path serves the `FENCED`
/// and `UNKNOWN_LEADER_EPOCH` error codes, which this scenario never produces.
///
/// The test reaches determinism without racing a real unclean election:
///   1. A 3-broker cluster, a topic with rf=3, and broker 1 as the partition
///      leader.
///   2. Produce `k = 8` records through the leader with acks=-1. Every replica
///      converges to LEO 8 with checkpoint `0 -> 0`, because the produce
///      handler stamps leader epoch 0 onto each batch.
///   3. On a *follower*, append a divergent suffix of 5 extra records straight
///      to its local log with `produce_records_for_test`. Those batches carry
///      epoch -1, so they add no checkpoint entry. The follower's latest
///      recorded epoch stays 0 while its LEO jumps to 13, so it holds records
///      the leader does not have.
///   4. The follower's already-running replicator fetches at offset 13 and
///      advertises `last_fetched_epoch = 0`. The leader, at LEO 8 and latest
///      epoch 0, computes `epoch_and_offset_for(0, 8) = (0, 8)`. The epoch-0
///      end, 8, is below the fetch offset, 13, so the leader answers
///      `diverging_epoch { end_offset: 8 }`. The replicator truncates to 8 and
///      converges back to the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_truncates_in_band_on_diverging_epoch() {
    let _g = cluster_lock().lock().await;
    let cluster = registered_cluster(3).await;

    // cluster[0] is node 1, and the topic pins it as the leader of partition 0
    // (the same placement the replication tests use).
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(leader_addr.clone(), None).await;
    let topic_id = support::topics::create_assigned_partition(
        &admin,
        AssignedTopicSetup::three_replicas("divtrunc"),
        cluster.iter().map(|(broker, _, _)| broker),
    )
    .await;

    // Produce k = 8 records to the leader at epoch 0 (acks=-1 so it lands
    // on the followers too). One record per batch keeps offsets dense.
    let k: i64 = 8;
    let producer = connect_client(leader_addr, None).await;
    for i in 0..k {
        let prod = producer
            .send(batch_request(
                record(&format!("v{i}")),
                replicated_topic_produce_setup()
                    .topic("divtrunc".into())
                    .topic_id(topic_id.0)
                    .call(),
            ))
            .await
            .unwrap();
        assert!(prod.responses[0].partition_responses[0].error_code == 0);
    }

    // Wait for all three brokers to converge to LEO k.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("divtrunc", 0, k).await;
    }

    // Pick a follower: a broker that is not the partition leader.
    let leader_id = cluster[0]
        .0
        .partition_leader_for_test("divtrunc", 0)
        .expect("partition leader known");
    let follower_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != leader_id)
        .expect("a non-leader replica exists");
    let follower = &cluster[follower_idx].0;

    // Append a divergent suffix to the follower's local log. These batches
    // carry epoch -1 (no checkpoint row), so the follower's latest recorded
    // epoch stays 0 while its LEO jumps past the leader's epoch-0 boundary.
    let suffix: i64 = 5;
    follower
        .produce_records_for_test("divtrunc", 0, usize::try_from(suffix).unwrap())
        .await
        .expect("inject divergent suffix");
    let diverged_leo = follower
        .local_log_end_offset("divtrunc", 0)
        .expect("follower leo after suffix");
    assert!(
        diverged_leo == k + suffix,
        "follower should hold a divergent suffix (expected {}, got {diverged_leo})",
        k + suffix
    );

    // The leader stays at LEO k, so its epoch-0 boundary (8) is below the
    // follower's fetch offset (13): the next follower Fetch gets a
    // `diverging_epoch` and the replicator truncates in band back to k.
    // Follower truncates its divergent suffix and re-replicates to match the
    // leader exactly. Wait for the follower to settle at exactly k (it may
    // transiently sit above k with divergent data before truncating).
    follower
        .wait_until_local_log_end_offset_eq("divtrunc", 0, k)
        .await;
    let f_leo = follower.local_log_end_offset("divtrunc", 0).unwrap_or(-1);
    let l_leo = cluster[0]
        .0
        .local_log_end_offset("divtrunc", 0)
        .unwrap_or(-1);
    assert!(
        f_leo == l_leo && f_leo == k,
        "follower did not converge to leader (follower={f_leo}, leader={l_leo}, k={k})"
    );

    // Final cross-check: leader LEO and follower LEO agree.
    assert!(
        follower.local_log_end_offset("divtrunc", 0)
            == cluster[0].0.local_log_end_offset("divtrunc", 0)
    );

    shutdown_cluster(cluster).await;
}
