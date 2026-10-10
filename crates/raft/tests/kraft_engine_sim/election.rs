//! Leader election: that three engines settle on exactly one leader, and that a
//! bare majority still elects one when every voter shares an election timeout.
//!
//! The uniform-timeout case is the regression guard for the split-vote
//! livelock, which the staggered topologies cannot reach.

use std::time::Duration;

use krabka_raft::kraft::NodeId;
use krabka_units::prelude::millis;

use crate::harness::{await_single_leader, build_engine, start_engines};

/// 1. Three engines elect exactly one leader and agree on the epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_engines_elect_one_leader() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(100);

    // Staggered election timeouts so one node reliably wins the first round.
    let _dirs = start_engines(
        &net,
        crate::harness::SimClusterSetup {
            ids: &ids,
            cluster_id: cid,
            ..Default::default()
        },
    );

    let (leader, epoch) = await_single_leader(&net, &ids, Duration::from_secs(10)).await;
    assert2::assert!(epoch >= 1);
    // Exactly one node reports itself as the leader.
    let mut self_leaders = 0;
    for &id in &ids {
        let qs = net.get(id).unwrap().quorum_state().await.unwrap();
        if qs.leader_id == Some(id) {
            self_leaders += 1;
        }
    }
    assert2::assert!(self_leaders == 1);
    assert2::assert!(ids.contains(&leader));

    for &id in &ids {
        net.get(id).unwrap().shutdown().await;
    }
}

/// 1b. A bare majority, exactly 2 of a 3-voter set, elects a stable leader even
///     with UNIFORM election timeouts and in-process lockstep.
///
///     This guards the split-vote livelock fix. Without per-node, per-epoch
///     election-timeout jitter, the two closely-synchronized voters both become
///     candidates every round, self-vote, and never reach a majority, because
///     the third voter is down. They then churn for tens of seconds. The
///     `start_n_node`-style topologies with all voters up never exercised this.
///     The mixed JVM and Krabka quorum did, because the JVM boots slowly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bare_majority_two_of_three_elects_with_uniform_timeouts() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(150);

    // UNIFORM timeout for both live voters (no manual stagger) — the production
    // controller config uses a single election timeout for every node. Only
    // voters 1 and 2 are started; voter 3 stays down, so {1,2} is the bare
    // majority of the 3-voter set.
    let mut dirs = Vec::new();
    for &id in &[NodeId(1), NodeId(2)] {
        let (ctrl, dir) = build_engine(
            &net,
            crate::harness::SimEngineSetup {
                me: id,
                ids: &ids,
                cluster_id: cid,
                election_timeout: millis(200),
                ..Default::default()
            },
        );
        net.register(id, ctrl);
        dirs.push(dir);
    }

    // Must converge quickly via self-staggering; without the jitter fix this
    // livelocks well past the deadline.
    let (leader, epoch) =
        await_single_leader(&net, &[NodeId(1), NodeId(2)], Duration::from_secs(8)).await;
    assert2::assert!(epoch >= 1);
    assert2::assert!(leader == 1 || leader == 2);

    for &id in &[NodeId(1), NodeId(2)] {
        net.get(id).unwrap().shutdown().await;
    }
}
