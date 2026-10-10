//! A broker restart reloads a streams group from `__consumer_offsets`, and the
//! reloaded group goes on assigning its tasks.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
};

use crate::streams_harness::{
    active_partitions_for, boot, connect, create_topic, finalize_streams_version, first_join,
    follow_up, join_and_converge, topology,
};

const GROUP: &str = "streams-restart";
const TOPIC: &str = "restart-input";

/// A member as its client sees it: the epoch and the active partitions of
/// subtopology `0` that the coordinator last sent.
#[derive(Debug)]
struct Member {
    id: String,
    epoch: i32,
    active: Vec<i32>,
}

impl Member {
    /// Sends one heartbeat that reports the owned tasks, and takes the epoch
    /// and any new task list from the response.
    async fn heartbeat(&mut self, client: &Client) {
        let owned = if self.active.is_empty() {
            vec![]
        } else {
            vec![ReqTaskIds {
                subtopology_id: "0".into(),
                partitions: self.active.clone(),
                ..Default::default()
            }]
        };
        let resp = client
            .send(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: GROUP,
                member_id: &self.id,
                epoch: crate::support::streams::StreamsMemberEpoch(self.epoch),
                active: Some(owned),
            }))
            .await
            .expect("heartbeat");
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        self.epoch = resp.member_epoch;
        if resp.active_tasks.is_some() {
            self.active = active_partitions_for(&resp, "0");
        }
    }
}

/// A member that joins after a restart gets the task that the reloaded member
/// gives up, as it would without the restart.
///
/// The broker spawns the actor of a reloaded group during the
/// `__consumer_offsets` replay, before it connects the metadata source that the
/// assignment reads. An actor that never saw the source assigned no task, so
/// the reloaded member revoked both of its tasks and the new member got none:
/// the Kafka system test `test_streams_should_failover_while_brokers_down`
/// then saw no instance process a record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reloaded_group_assigns_tasks_to_a_member_that_joins_after_a_restart() {
    let (broker, bootstrap, dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, TOPIC, 2).await;
    let (id, joined) = join_and_converge(&client, GROUP, topology(TOPIC, vec![]), 2, 10).await;
    let mut reloaded = Member {
        id,
        epoch: joined.member_epoch,
        active: active_partitions_for(&joined, "0"),
    };
    assert!(reloaded.active == vec![0, 1]);
    broker.shutdown().await;

    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let client = connect(&broker.listen_addr().to_string()).await;

    let resp = client
        .send(StreamsGroupHeartbeatRequest {
            process_id: Some("p2".into()),
            ..first_join(GROUP, topology(TOPIC, vec![]))
        })
        .await
        .expect("join after the restart");
    assert!(resp.error_code == 0, "join error: {resp:?}");
    let mut joining = Member {
        id: resp.member_id.clone(),
        epoch: resp.member_epoch,
        active: active_partitions_for(&resp, "0"),
    };

    // intentional: the hand-over takes a revocation by one member and then a
    // heartbeat of the other, and the assignment is coordinator-local state
    // that only the heartbeat responses expose.
    for _ in 0..20 {
        if reloaded.active == [0] && joining.active == [1] {
            break;
        }
        reloaded.heartbeat(&client).await;
        joining.heartbeat(&client).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        (reloaded.active, joining.active) == (vec![0], vec![1]),
        "the members did not share the tasks after the restart"
    );
}
