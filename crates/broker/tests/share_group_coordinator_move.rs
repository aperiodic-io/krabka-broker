//! A share group's coordinator answers a heartbeat only once the records it
//! wrote are committed, so a member's epoch survives the coordinator moving.
//!
//! Kafka's `CoordinatorRuntime` completes a write when the high watermark of
//! the group's `__consumer_offsets` partition passes it. In
//! `ShareConsumerTest.test_broker_failure`, the coordinator of the group
//! answered three joins with epochs whose records were in its log only, and
//! then stopped cleanly. The broker that took the partition over had none of
//! them, and it answered every member's next heartbeat with
//! `GROUP_ID_NOT_FOUND`, which a share consumer cannot recover from.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
};
use tempfile::TempDir;

use crate::support::{
    client::connect_owned, discovery::coordinator_lookup_request, share::crash_follower,
    shutdown_cluster,
};

mod support;

const GROUP: &str = "share-move";
const KEY_TYPE_GROUP: i8 = 0;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// The time a lookup may take to see a creation or a coordinator move.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// Three brokers with one `__consumer_offsets` partition, replicated on all
/// of them. A stopped broker stays in its ISRs: the leader does not shrink
/// them on lag, and the controller does not fence the broker, for longer
/// than any test here runs.
async fn start_three() -> Cluster {
    support::fixed_internal_isr_cluster(|config| {
        config.offsets_topic_num_partitions = 1;
    })
    .await
}

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "share-coordinator-move",
        "client",
    )
    .await
}

/// Look `GROUP` up until a broker other than `excluded` answers as its
/// coordinator, and return that broker's node id.
async fn coordinator_of(client: &Client, excluded: Option<u64>) -> u64 {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = client
            .send(coordinator_lookup_request(
                GROUP,
                KEY_TYPE_GROUP,
                vec![GROUP.into()],
            ))
            .await
            .expect("FindCoordinator");
        if let [row] = found.coordinators.as_slice()
            && row.error_code == 0
            && let Ok(node_id) = u64::try_from(row.node_id)
            && Some(node_id) != excluded
        {
            return node_id;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: a client has no awaiter for the creation or the
        // move; it retries, as Kafka's clients do.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn heartbeat(member_id: &str, member_epoch: i32) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: GROUP.into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec!["orders".into()]),
        ..Default::default()
    }
}

/// Send `request` until the coordinator has loaded the group's partition.
///
/// `FindCoordinator` names a broker once it leads the partition, which can be
/// before it has replayed it, and the coordinator answers
/// `COORDINATOR_LOAD_IN_PROGRESS` until then.
async fn heartbeat_once_loaded(
    client: &Client,
    request: ShareGroupHeartbeatRequest,
) -> ShareGroupHeartbeatResponse {
    let deadline = Instant::now() + SETTLE;
    loop {
        let response = client
            .send(request.clone())
            .await
            .expect("ShareGroupHeartbeat");
        if response.error_code != COORDINATOR_LOAD_IN_PROGRESS || Instant::now() >= deadline {
            return response;
        }
        // intentional: the client retries a coordinator that is loading.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn position_of(cluster: &Cluster, node_id: u64) -> usize {
    cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == node_id)
        .expect("a cluster member")
}

/// A join whose records cannot commit, because a follower of the group's
/// offsets partition is down but still in its ISR, is not answered with an
/// epoch. It times out as Kafka's coordinator write does, and the client
/// gets `COORDINATOR_NOT_AVAILABLE`, which it retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_join_that_does_not_commit_is_not_answered() {
    let mut cluster = start_three().await;
    let lookup = client(&cluster[0].0).await;
    let coordinator = coordinator_of(&lookup, None).await;
    lookup.close();
    let stopped_dir = crash_follower(&mut cluster, coordinator).await;

    let member = client(&cluster[position_of(&cluster, coordinator)].0).await;
    let joined = heartbeat_once_loaded(&member, heartbeat("member-1", 0)).await;
    member.close();

    assert!(
        joined
            == ShareGroupHeartbeatResponse {
                error_code: COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            }
    );

    shutdown_cluster(cluster).await;
    drop(stopped_dir);
}

/// The start of `ShareConsumerTest.test_broker_failure` at a small scale:
/// a member joins, its coordinator stops cleanly, and the broker that takes
/// the group over accepts the member's next heartbeat at the epoch it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_keeps_its_epoch_when_its_coordinator_stops_cleanly() {
    let mut cluster = start_three().await;
    let lookup = client(&cluster[0].0).await;
    let coordinator = coordinator_of(&lookup, None).await;
    lookup.close();

    let position = position_of(&cluster, coordinator);
    let member = client(&cluster[position].0).await;
    let joined = heartbeat_once_loaded(&member, heartbeat("member-1", 0)).await;
    member.close();
    assert!(joined.error_code == 0, "{joined:?}");
    assert!(joined.member_epoch > 0, "{joined:?}");

    let (stopped, _, stopped_dir) = cluster.remove(position);
    stopped
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("the controlled shutdown drains");
    let survivor = client(&cluster[0].0).await;
    let next = coordinator_of(&survivor, Some(coordinator)).await;
    survivor.close();
    let member = client(&cluster[position_of(&cluster, next)].0).await;
    let resumed = heartbeat_once_loaded(&member, heartbeat("member-1", joined.member_epoch)).await;
    member.close();

    check!(resumed.error_code == 0, "{resumed:?}");
    check!(resumed.member_id.as_deref() == Some("member-1"));
    check!(resumed.member_epoch >= joined.member_epoch);

    shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
