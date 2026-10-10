//! ISR changes in a role-separated cluster: one controller-only node and two
//! broker-only nodes.
//!
//! A partition leader proposes each ISR change to the active controller in an
//! `AlterPartition`. Kafka sends it on the CONTROLLER listener of that
//! controller. A controller-only node has no broker registration. So a leader
//! that looks for the controller among the registered brokers never reaches
//! it, and the controller never commits the change.
//!
//! Only the leader can put a replica back into the ISR. A follower that
//! restarts and catches up is therefore the test: it rejoins the ISR only when
//! the `AlterPartition` of the leader reaches the controller.

use assert2::assert;
use krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, config::NodeRole};
use tempfile::TempDir;

mod support;

/// A booted role-separated cluster: node 1 is the controller-only voter, and
/// nodes 2 and 3 are broker-only nodes.
struct RoleSeparated {
    controller: BrokerHandle,
    brokers: Vec<BrokerHandle>,
    /// The configs of the broker-only nodes, in the order of `brokers` at
    /// boot. A test restarts a node on the same ports and log dir with them.
    broker_configs: Vec<BrokerConfig>,
    // Dropping these removes the log dirs that the nodes still hold open.
    _dirs: Vec<TempDir>,
}

impl RoleSeparated {
    async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

/// Boots the controller-only node first and waits until it leads, then boots
/// the two broker-only nodes and waits until both are registered.
async fn start_role_separated() -> RoleSeparated {
    const NODES: usize = 3;
    let (endpoints, mut data_listeners, mut ctrl_listeners) =
        support::single_controller_endpoints(NODES).await;
    let topology = endpoints.topology();
    let mut dirs = Vec::with_capacity(NODES);

    let ctrl_dir = TempDir::new().unwrap();
    let ctrl_cfg = topology.config(
        ctrl_dir.path(),
        crate::support::RoleNodeSetup {
            role: NodeRole::Controller,
            ..Default::default()
        },
    );
    let controller = support::start_held_node(
        ctrl_cfg,
        &mut ctrl_listeners,
        &mut data_listeners,
        "controller-only start",
    )
    .await;
    dirs.push(ctrl_dir);
    controller.wait_until_controller_leader().await;

    let mut brokers = Vec::with_capacity(NODES - 1);
    let mut broker_configs = Vec::with_capacity(NODES - 1);
    for index in 1..NODES {
        let dir = TempDir::new().unwrap();
        let cfg = topology.config(
            dir.path(),
            crate::support::RoleNodeSetup {
                index: crate::support::NodeIndex(index),
                mode: BootstrapMode::Join,
                ..Default::default()
            },
        );
        broker_configs.push(cfg.clone());
        brokers.push(
            support::start_held_node(
                cfg,
                &mut ctrl_listeners,
                &mut data_listeners,
                "broker-only start",
            )
            .await,
        );
        dirs.push(dir);
    }
    controller.wait_until_brokers_registered(NODES - 1).await;
    RoleSeparated {
        controller,
        brokers,
        broker_configs,
        _dirs: dirs,
    }
}

/// The ISR of partition 0 of `topic` in the image of the controller, sorted.
fn sorted_isr(cluster: &RoleSeparated, topic: &str) -> Vec<u64> {
    let mut isr = cluster
        .controller
        .partition_isr_for_test(topic, 0)
        .expect("the partition is in the image");
    isr.sort_unstable();
    isr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_rejoins_the_isr_when_the_controller_is_controller_only() {
    support::init_tracing();
    let topic = "rolesep-isr";
    let mut cluster = start_role_separated().await;
    crate::support::client::create_topic_for_broker(
        &cluster.brokers[0],
        crate::support::topics::CreateTopicSetup {
            topic,
            replication_factor: crate::support::topics::TopicReplicationFactor(2),
            ..Default::default()
        },
    )
    .await;
    cluster.controller.wait_until_isr_len(topic, 0, 2).await;
    assert!(sorted_isr(&cluster, topic) == vec![2, 3]);

    let leader = cluster
        .controller
        .partition_record_for_test(topic, 0)
        .expect("the partition is in the image")
        .leader
        .0;
    let follower = cluster
        .brokers
        .iter()
        .position(|broker| broker.node_id() != leader)
        .expect("a follower");
    let follower_config = cluster.broker_configs[follower].clone();

    // The follower stops without a controlled shutdown, so it leaves the ISR.
    cluster.brokers.remove(follower).crash_for_test().await;
    cluster.controller.wait_until_isr_len(topic, 0, 1).await;
    assert!(sorted_isr(&cluster, topic) == vec![leader]);

    // The restarted follower catches up. Only an `AlterPartition` from the
    // leader that the controller commits puts it back into the ISR.
    cluster
        .brokers
        .push(support::start_reusing_addrs(&follower_config, "restarted follower").await);
    cluster.controller.wait_until_isr_len(topic, 0, 2).await;
    assert!(sorted_isr(&cluster, topic) == vec![2, 3]);

    cluster.shutdown().await;
}
