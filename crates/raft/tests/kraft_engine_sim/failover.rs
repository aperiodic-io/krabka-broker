//! Leader failover: that the survivors of a leader kill elect a new leader at a
//! higher epoch and can still commit, that a leader killed and reopened over its
//! own data dir five times running never leaves the quorum leaderless, and that
//! a leader the network isolates gives its epoch up instead of holding it.

use crate::{
    harness as fixture,
    harness::{
        STAGGERED_TIMEOUTS, await_single_leader, await_until, metadata_log, start_engines,
        topic_record, voter_set,
    },
};

/// Start the failover fixture and retain its directories through the scenario.
async fn elected_cluster(
    net: &crate::sim_net::SimNet,
    setup: crate::harness::SimClusterSetup<'_>,
) -> (Vec<tempfile::TempDir>, fixture::NodeId, u32) {
    let ids = setup.ids;
    let dirs = start_engines(net, setup);
    let (leader, epoch) = await_single_leader(net, ids, fixture::Duration::from_secs(10)).await;
    (dirs, leader, epoch)
}

/// Polls `ctrl`'s quorum-state snapshot until `f` accepts it, or panics.
///
/// This is the view `DescribeQuorum`, Metadata and `BrokerHeartbeat` all serve
/// from, so it is the right place to observe whether a node still answers as
/// the controller leader.
async fn await_surviving_leader(
    net: &crate::sim_net::SimNet,
    ids: &[fixture::NodeId],
    former: fixture::NodeId,
    former_epoch: u32,
) -> (Vec<fixture::NodeId>, fixture::NodeId, u32) {
    let survivors = ids
        .iter()
        .copied()
        .filter(|id| *id != former)
        .collect::<Vec<_>>();
    let (leader, epoch) =
        await_single_leader(net, &survivors, fixture::Duration::from_secs(15)).await;
    assert2::assert!((leader != former, epoch > former_epoch) == (true, true));
    (survivors, leader, epoch)
}

async fn await_quorum_state<F>(
    ctrl: &fixture::KraftController,
    timeout: fixture::Duration,
    mut f: F,
) where
    F: FnMut(&krabka_raft::kraft::QuorumStateSnapshot) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(qs) = ctrl.quorum_state().await
            && f(&qs)
        {
            return;
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "quorum state never satisfied the condition"
        );
        tokio::task::yield_now().await;
    }
}

/// 3. After a kill of the leader, the remaining two re-elect a single new
///    leader, and a `submit_change` to the new leader commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failure_reelects() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(300);

    let (_dirs, leader, epoch1) = elected_cluster(
        &net,
        crate::harness::SimClusterSetup {
            ids: &ids,
            cluster_id: cid,
            ..Default::default()
        },
    )
    .await;

    // Kill the leader: shut it down and remove it from the registry so peers see
    // it as unreachable.
    net.get(leader).unwrap().shutdown().await;
    net.remove(leader);

    // The two survivors must elect a NEW single leader at a higher epoch.
    let (survivors, new_leader, _epoch2) = await_surviving_leader(&net, &ids, leader, epoch1).await;

    // A submit to the new leader commits across the two survivors.
    tokio::time::timeout(
        fixture::Duration::from_secs(10),
        net.get(new_leader)
            .unwrap()
            .submit_change(vec![topic_record("post-failover", 7)]),
    )
    .await
    .expect("post-failover submit did not hang")
    .expect("post-failover submit ok");

    for &id in &survivors {
        let ctrl = net.get(id).unwrap();
        await_until(fixture::Duration::from_secs(10), || {
            ctrl.current_image().topic("post-failover").map(|_| ())
        })
        .await;
    }

    for &id in &survivors {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 3b. The same leader is killed and reopened over its own data dir five times
///     in a row, and the quorum must converge on one leader after every round.
///
///     This is the acceptance for an ex-leader that comes back. Leadership is
///     volatile, so the reopened node loads `leader_id == None` while its two
///     followers still believe it leads. It must not answer their Fetches as if
///     it did. When it did, each answer scored as a live fetch, their watchdogs
///     never expired, they held their stale leader belief, and a KIP-996
///     pre-vote only grants when the voter follows no leader. Nothing could
///     ever win a pre-vote and the cluster stayed leaderless.
///
///     Wall-clock, not `start_paused`: the engines are real spawned tasks and
///     `await_single_leader` polls them with `yield_now`, so the runtime is
///     never idle and virtual time would never auto-advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_leader_restart_reelects() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(301);
    let dirs: fixture::HashMap<_, _> = ids
        .iter()
        .copied()
        .zip(start_engines(
            &net,
            crate::harness::SimClusterSetup {
                ids: &ids,
                cluster_id: cid,
                ..Default::default()
            },
        ))
        .collect();

    for _ in 0..5 {
        let (leader, _) = await_single_leader(&net, &ids, fixture::Duration::from_secs(10)).await;
        net.get(leader).unwrap().shutdown().await;
        net.remove(leader);
        tokio::time::sleep(fixture::Duration::from_millis(50)).await;
        let reopened = fixture::KraftController::open(
            dirs[&leader].path().to_path_buf(),
            leader,
            cid,
            uuid::Uuid::nil(),
            voter_set(&ids),
            STAGGERED_TIMEOUTS[usize::try_from(leader.0 - 1).unwrap()],
            None,
            fixture::ControllerFetchMissLimit::default(),
            fixture::MetadataRaftCommandQueueCapacity::default(),
            fixture::MetadataRaftFetchMax::default(),
            fixture::Arc::new(net.as_peer(leader)),
            0,
            krabka_units::prelude::bytes(0),
            krabka_units::prelude::millis(0),
            fixture::MetadataSnapshotFetchMax::default(),
            metadata_log(),
            krabka_raft::kraft::Activation::default(),
        )
        .expect("reopen leader");
        net.register(leader, reopened);
    }

    let _ = await_single_leader(&net, &ids, fixture::Duration::from_secs(10)).await;
    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 3c. A leader that the network isolates must resign instead of holding an
///     epoch it can no longer serve, and must rejoin as a follower once the
///     partition heals.
///
///     Nothing else can tell it. The majority side elects at a higher epoch, but
///     the isolated node receives none of that traffic, and under KIP-996 the
///     new round's pre-votes never reach it either -- so with no check-quorum it
///     keeps answering `DescribeQuorum`, Metadata and `BrokerHeartbeat` as the
///     leader of the old epoch for as long as the partition lasts. The
///     `election_safety` model property cannot see this: the two leaders hold
///     different epochs.
///
///     The isolated node keeps running, which is what separates this from the
///     kill above: its whole state machine ticks on, and the only thing that
///     changes is that no voter's Fetch arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_leader_resigns_and_rejoins_after_heal() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(302);
    let (_dirs, leader, epoch1) = elected_cluster(
        &net,
        crate::harness::SimClusterSetup {
            ids: &ids,
            cluster_id: cid,
            ..Default::default()
        },
    )
    .await;
    let isolated = net.get(leader).expect("leader is registered");
    net.partition(leader);

    // Check-quorum is 1.5x the fetch timeout, and the longest configured
    // timeout here is 450ms, so a resignation is due well inside this budget.
    // Until it resigns the node still names itself leader of `epoch1`.
    await_quorum_state(&isolated, fixture::Duration::from_secs(10), |qs| {
        qs.leader_id.is_none()
    })
    .await;

    // The majority side elects its own leader at a higher epoch and commits.
    let (_survivors, new_leader, epoch2) = await_surviving_leader(&net, &ids, leader, epoch1).await;
    tokio::time::timeout(
        fixture::Duration::from_secs(10),
        net.get(new_leader)
            .expect("new leader is registered")
            .submit_change(vec![topic_record("post-partition", 8)]),
    )
    .await
    .expect("post-partition submit did not hang")
    .expect("post-partition submit ok");

    // Heal: the rejoining node attaches to the new leader's epoch as a
    // follower, and replicates what it missed.
    net.heal(leader);
    await_quorum_state(&isolated, fixture::Duration::from_secs(15), |qs| {
        qs.leader_id == Some(new_leader) && qs.leader_epoch >= epoch2
    })
    .await;
    await_until(fixture::Duration::from_secs(15), || {
        isolated.current_image().topic("post-partition").map(|_| ())
    })
    .await;

    for &id in &ids {
        net.get(id).expect("still registered").shutdown().await;
    }
}
