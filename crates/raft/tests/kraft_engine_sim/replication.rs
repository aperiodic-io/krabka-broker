//! Record-carrying replication: a change submitted at a follower is refused
//! with the leader hint, and once it reaches the leader the record itself
//! travels to every follower and lands in all three metadata images.

use std::time::Duration;

use krabka_raft::RaftError;

use crate::harness::{await_single_leader, await_until, start_engines, topic_record};

/// 2. `submit_change` on a follower forwards to the leader, commits through
///    record-carrying replication, and the topic appears in ALL three images.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_submit_change_propagates() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(200);

    let _dirs = start_engines(
        &net,
        crate::harness::SimClusterSetup {
            ids: &ids,
            cluster_id: cid,
            ..Default::default()
        },
    );

    let (leader, _epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;

    // Submit on a FOLLOWER. The follower rejects with NotLeader{leader}; we then
    // submit to the leader (the handle layer's forward is Task 8 — here we drive
    // the forward explicitly via the leader handle the hint points at).
    let follower = *ids.iter().find(|&&id| id != leader).unwrap();
    let fol = net.get(follower).unwrap();
    let res = fol.submit_change(vec![topic_record("orders", 1)]).await;
    let leader_hint = match res {
        Err(RaftError::NotLeader { current_leader }) => current_leader,
        other => panic!("follower submit should reject with NotLeader, got {other:?}"),
    };
    assert2::assert!(leader_hint == Some(leader));

    // Forward to the leader (record-carrying replication commits it on a majority).
    tokio::time::timeout(
        Duration::from_secs(10),
        net.get(leader)
            .unwrap()
            .submit_change(vec![topic_record("orders", 1)]),
    )
    .await
    .expect("leader submit did not hang")
    .expect("leader submit ok");

    // The topic must appear in ALL three engines' current_image (real replication
    // carried the record bytes to the followers, which applied on HWM advance).
    for &id in &ids {
        let ctrl = net.get(id).unwrap();
        await_until(Duration::from_secs(10), || {
            ctrl.current_image().topic("orders").map(|_| ())
        })
        .await;
        assert2::assert!(ctrl.current_image().topic("orders").is_some());
    }

    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}
