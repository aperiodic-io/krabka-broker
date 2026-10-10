//! The `__krabka_audit` topic in a cluster with an isolated controller: one
//! controller-only node that is the whole voter set, and broker-only nodes.
//!
//! The controller starts first and sees no registered broker, so it creates
//! nothing. Kafka never places a replica on a controller-only node. The first
//! broker-only node to start creates the topic on itself, and its audit writer
//! then records the broker start in that partition.

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, config::NodeRole, coordinator::AUDIT_TOPIC};
use krabka_raft::NodeId;
use tempfile::TempDir;

mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_a_broker_holds_the_audit_partition_under_an_isolated_controller() {
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(2).await;
    let voters = vec![(1_u64, controller_addrs[0])];
    let mut client_listeners = client_listeners.into_iter();
    let mut controller_listeners = controller_listeners.into_iter();

    let controller_dir = TempDir::new().unwrap();
    let mut controller_config = support::broker_config(
        controller_dir.path(),
        crate::support::ClusterNodeSetup {
            client_addrs: &client_addrs,
            controller_addrs: &controller_addrs,
            voters: crate::support::controller_voters(&voters),
            ..Default::default()
        },
    );
    controller_config.roles = vec![NodeRole::Controller];
    let controller = Broker::start_with_listeners(
        controller_config,
        controller_listeners.next(),
        client_listeners.next(),
    )
    .await
    .expect("controller-only start");
    controller.wait_until_controller_leader().await;
    let controller_created = controller
        .controller_image_for_test()
        .topic(AUDIT_TOPIC)
        .is_some();

    let broker_dir = TempDir::new().unwrap();
    let mut broker_config = support::broker_config(
        broker_dir.path(),
        crate::support::ClusterNodeSetup {
            index: crate::support::NodeIndex(1),
            client_addrs: &client_addrs,
            controller_addrs: &controller_addrs,
            voters: crate::support::controller_voters(&voters),
            mode: BootstrapMode::Join,
        },
    );
    broker_config.roles = vec![NodeRole::Broker];
    let broker = Broker::start_with_listeners(
        broker_config,
        controller_listeners.next(),
        client_listeners.next(),
    )
    .await
    .expect("broker-only start");
    controller
        .wait_until_partition_present(AUDIT_TOPIC, 0)
        .await;
    let placement: Vec<_> = controller
        .controller_image_for_test()
        .partitions_of(AUDIT_TOPIC)
        .map(|record| {
            (
                record.partition,
                record.leader,
                record.replicas.clone(),
                record.isr.clone(),
            )
        })
        .collect();
    // Only a broker that leads an audit partition writes its `BrokerStarted`
    // event, and it writes the event into that partition.
    broker
        .wait_until_local_log_end_offset(AUDIT_TOPIC, 0, 1)
        .await;

    assert!(!controller_created);
    assert!(placement == vec![(0, NodeId(2), vec![NodeId(2)], vec![NodeId(2)])]);
    assert!(controller.local_log_end_offset(AUDIT_TOPIC, 0).is_none());

    broker.shutdown().await;
    controller.shutdown().await;
}
