use std::sync::Arc;

use assert2::{assert, check};
use tempfile::tempdir;

use super::*;
use crate::{
    broker::{
        Broker,
        test_support::{local_partition_with_records, submit_metadata_topic_partition},
    },
    config::BrokerConfig,
    partition::WriterMessage,
    test_support::{
        UNSUPPORTED_LEVEL_FAULT, finalize_unstable_metadata_version, write_bootstrap_records,
    },
};

async fn assert_listener_stops_accepting(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => {
                drop(stream);
                assert!(
                    std::time::Instant::now() < deadline,
                    "listener at {addr} still accepts connections after shutdown"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(_) => return,
        }
    }
}

async fn wait_for_connection_count(broker: &Broker, expected: usize, message: &'static str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if broker.connections.total() == expected {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{message}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn single_broker_handle_helpers_observe_real_state_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    let handle = Broker::start(config).await.expect("broker start");
    let broker = handle.broker_arc_for_test();

    check!(broker.handlers().get(18).is_some());
    check!(handle.metrics_addr().is_some_and(|addr| addr.port() != 0));
    check!(handle.offset_for_leader_epoch_count_for_test() == 0);
    broker
        .offset_for_leader_epoch_requests
        .store(2, std::sync::atomic::Ordering::Release);
    assert!(handle.offset_for_leader_epoch_count_for_test() == 2);
    assert!(!handle.rlmm_topic_backed_active_for_test());
    broker.metrics.tiered_storage_rlmm_topic_backed.set(1);
    assert!(handle.rlmm_topic_backed_active_for_test());
    broker.metrics.tiered_storage_rlmm_topic_backed.set(2);
    check!(!handle.rlmm_topic_backed_active_for_test());
    check!(handle.reload_tls().is_err());
    check!(!handle.has_partition("missing-mutant-topic", 0));
    check!(handle.local_log_end_offset("missing-mutant-topic", 0) == None);
    check!(
        handle
            .test_advance_log_start("missing-mutant-topic", 0, 10)
            .await
            .is_err()
    );
    check!(
        handle
            .change_membership([krabka_raft::NodeId(1)].into_iter().collect())
            .await
            .is_ok()
    );

    let leader = handle.wait_until_controller_leader().await;
    assert!(leader == krabka_raft::NodeId(handle.node_id()));
    assert!(handle.controller_leader_id() == Some(krabka_raft::NodeId(handle.node_id())));

    let mut endpoints = handle.self_registration_endpoints();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while endpoints.is_empty() && std::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
        endpoints = handle.self_registration_endpoints();
    }
    assert!(!endpoints.is_empty());

    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: -1,
                incarnation_id: uuid::Uuid::from_u128(0xBEEF),
                port: 19_092,
                endpoints: vec![krabka_metadata::BrokerEndpoint {
                    name: "PLAINTEXT".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19_092,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                }],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(
                    handle.node_id() + 1,
                ))
            },
        ))
        .await
        .expect("submit peer broker registration");
    assert!(
        handle
            .controller_image_for_test()
            .broker(krabka_raft::NodeId(handle.node_id() + 1))
            .is_some()
    );
    handle.wait_until_brokers_registered(2).await;
    assert!(handle.broker_count() == 2);

    // The peer registered just above never heartbeats this node, and this
    // node is the only controller, yet the peer is electable: `ElectLeaders`
    // decides liveness from the replicated fencing state rather than from the
    // controller's local heartbeat registry, so any node serving the request
    // answers the same. `broker_alive_for_test` reads that local registry
    // instead, which is why an id nothing ever registered is not alive there.
    handle.wait_until_broker_electable(handle.node_id()).await;
    handle
        .wait_until_broker_electable(handle.node_id() + 1)
        .await;
    check!(!handle.broker_alive_for_test(u64::MAX).await);

    let topic = "handle-mutant-topic";
    let partition_leader = handle.node_id() + 1;
    let partition_isr = [partition_leader, handle.node_id()];
    submit_metadata_topic_partition(
        &handle,
        crate::broker::test_support::MetadataPartitionSetup {
            topic,
            topic_id: uuid::Uuid::from_u128(0xCAFE),
            leader: krabka_ids::NodeId(partition_leader),
            replicas: partition_isr
                .iter()
                .copied()
                .map(krabka_ids::NodeId)
                .collect(),
            isr: partition_isr
                .iter()
                .copied()
                .map(krabka_ids::NodeId)
                .collect(),
            ..Default::default()
        },
    )
    .await;
    handle.wait_until_partition_present(topic, 0).await;
    check!(handle.has_partition(topic, 0));
    check!(handle.partition_leader_for_test(topic, 0) == Some(partition_leader));
    check!(handle.partition_isr_for_test(topic, 0) == Some(partition_isr.to_vec()));
    let observed_partition = handle
        .partition_record_for_test(topic, 0)
        .expect("partition record");
    let expected_partition = krabka_metadata::PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader: krabka_audit::NodeId(partition_leader),
        replicas: partition_isr
            .iter()
            .copied()
            .map(krabka_audit::NodeId)
            .collect(),
        isr: partition_isr
            .iter()
            .copied()
            .map(krabka_audit::NodeId)
            .collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(3),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: vec![uuid::Uuid::nil(); partition_isr.len()],
        partition_epoch: 0,
    };
    assert!(observed_partition == expected_partition);
    check!(handle.partition_leader_for_test("missing-mutant-topic", 0) == None);
    check!(handle.partition_isr_for_test("missing-mutant-topic", 0) == None);
    check!(handle.partition_record_for_test("missing-mutant-topic", 0) == None);
    check!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_partition_leader_changed(
                topic,
                0,
                krabka_raft::NodeId(handle.node_id())
            ),
        )
        .await
        .is_ok()
    );

    assert!(
        matches!(
            broker.controller.read_snapshot_range(0, 1),
            krabka_raft::SnapshotRange::NoSnapshot
        ),
        "test should start without a metadata snapshot"
    );
    handle
        .trigger_snapshot_for_test()
        .await
        .expect("trigger metadata snapshot");
    let krabka_raft::SnapshotRange::Slice(snapshot) = broker.controller.read_snapshot_range(0, 1)
    else {
        panic!("trigger_snapshot_for_test should write a readable snapshot");
    };
    assert!(snapshot.total_size > 0);
    assert!(!snapshot.bytes.is_empty());

    let local_topic = "handle-local-log-mutant-topic";
    let local_part = local_partition_with_records(
        dir.path(),
        crate::broker::test_support::LocalPartitionSetup {
            topic: local_topic,
            values: &[b"a", b"b"],
            ..Default::default()
        },
    );
    assert!(!handle.partition_exists_for_test(local_topic, 0));
    broker.partitions.insert(
        local_topic.into(),
        PartitionIndex(0),
        Arc::clone(&local_part),
    );
    assert!(handle.partition_exists_for_test(local_topic, 0));
    assert!(handle.local_log_end_offset(local_topic, 0) == Some(2));
    handle.test_set_leader_epoch(local_topic, 0, 7);
    assert!(
        local_part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            == 7
    );
    handle
        .test_truncate_local_log(local_topic, 0, 1)
        .await
        .expect("truncate local partition");
    assert!(handle.local_log_end_offset(local_topic, 0) == Some(0));

    handle.shutdown().await;
}

async fn start_default_broker(dir: &std::path::Path) -> (BrokerHandle, Arc<Broker>, SocketAddr) {
    let config = BrokerConfig::for_tests(dir.to_path_buf());
    let handle = Broker::start(config).await.unwrap();
    let broker = handle.broker_arc_for_test();
    let addr = handle.listen_addr();
    (handle, broker, addr)
}

struct ShutdownFixture {
    handle: BrokerHandle,
    broker: Arc<Broker>,
    addr: SocketAddr,
    partition: Arc<crate::partition::Partition>,
}

async fn shutdown_fixture(
    root: &std::path::Path,
    setup: crate::broker::test_support::LocalPartitionSetup<'_>,
) -> ShutdownFixture {
    let (handle, broker, addr) = start_default_broker(root).await;
    let (topic, index) = (setup.topic, setup.partition);
    let partition = local_partition_with_records(root, setup);
    broker
        .partitions
        .insert(topic.into(), index, partition.clone());
    ShutdownFixture {
        handle,
        broker,
        addr,
        partition,
    }
}

#[tokio::test]
async fn start_and_shutdown_clean() {
    let dir = tempdir().unwrap();
    let ShutdownFixture {
        handle,
        broker,
        addr,
        partition,
    } = shutdown_fixture(
        dir.path(),
        crate::broker::test_support::LocalPartitionSetup {
            topic: "shutdown",
            ..Default::default()
        },
    )
    .await;
    assert!(addr.port() != 0);
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("listener accepts before shutdown");
    wait_for_connection_count(&broker, 1, "accept_loop did not register live connection").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.shutdown())
        .await
        .expect("broker shutdown completes");
    assert!(broker.connections.total() == 0);
    assert!(partition.take_writer_handle().is_none());
    let (ack, _ack_rx) = tokio::sync::oneshot::channel();
    assert!(
        partition
            .writer_tx
            .send(WriterMessage::Compact { ack })
            .await
            .is_err()
    );
    drop(stream);
    assert_listener_stops_accepting(addr).await;
}

#[tokio::test]
async fn dropping_handle_stops_idle_connections_and_partition_writers() {
    let dir = tempdir().unwrap();
    let ShutdownFixture {
        handle,
        broker,
        addr,
        partition,
    } = shutdown_fixture(
        dir.path(),
        crate::broker::test_support::LocalPartitionSetup {
            topic: "drop-shutdown",
            ..Default::default()
        },
    )
    .await;
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("listener accepts before handle drop");
    wait_for_connection_count(&broker, 1, "accept_loop did not register live connection").await;

    drop(handle);

    wait_for_connection_count(
        &broker,
        0,
        "dropping BrokerHandle did not stop the idle connection",
    )
    .await;
    assert!(partition.take_writer_handle().is_none());
    assert_listener_stops_accepting(addr).await;
    drop(stream);
    broker.controller.cancel().await;
}

#[tokio::test]
async fn controlled_shutdown_timeout_stops_listener_and_reports_error() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.unwrap();
    let addr = handle.listen_addr();
    let err = handle
        .controlled_shutdown(std::time::Duration::ZERO)
        .await
        .expect_err("zero-timeout controlled shutdown should report drain timeout");
    assert!(matches!(err, BrokerError::ShutdownTimeout(timeout) if timeout.is_zero()));
    assert_listener_stops_accepting(addr).await;
}

/// Kafka halts a controller's process when the controller replays a feature
/// level that it does not support. The broker cannot halt its host, so it
/// latches the self-shutdown flag and keeps the reason for the host to exit on.
#[tokio::test]
async fn a_fatal_controller_fault_latches_self_shutdown_and_names_its_reason() {
    let dir = tempdir().unwrap();
    // The default config supports no unstable feature level.
    let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let mut should_shutdown = handle.should_shutdown_rx();
    check!(!*should_shutdown.borrow());
    check!(handle.fatal_fault().is_none());

    latch_unsupported_fault(&handle, &mut should_shutdown).await;
    check!(handle.fatal_fault().as_deref() == Some(UNSUPPORTED_LEVEL_FAULT));

    // The flag is latched, so there is no leadership drain to wait for.
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        handle.controlled_shutdown(std::time::Duration::from_secs(300)),
    )
    .await
    .expect("controlled shutdown waited for a drain that the dead controller cannot serve")
    .expect("controlled shutdown");
}

/// A node whose log finalized a level above its range never starts. The
/// refusal is the error of `Broker::start`, so the binary exits non-zero with
/// Kafka's message.
#[tokio::test]
async fn a_broker_refuses_to_start_over_a_log_finalized_at_an_unsupported_level() {
    let dir = tempdir().unwrap();
    let mut first = BrokerConfig::for_tests(dir.path().to_path_buf());
    first.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    let handle = Broker::start(first).await.expect("first start");
    handle
        .submit_metadata_record_for_test(finalize_unstable_metadata_version())
        .await
        .expect("finalize the unstable level");
    handle.shutdown().await;

    let mut second = BrokerConfig::for_tests(dir.path().to_path_buf());
    second.bootstrap_mode = krabka_raft::BootstrapMode::Rejoin;
    let Err(refused) = Broker::start(second).await else {
        panic!("a broker without the flag started over an unstable log");
    };
    check!(matches!(&refused, BrokerError::FatalFault(fault) if fault == UNSUPPORTED_LEVEL_FAULT));
    check!(refused.to_string() == format!("Encountered fatal fault: {UNSUPPORTED_LEVEL_FAULT}"));
}

/// A fault that lands while the node is still joining the quorum ends the start
/// at once. The bootstrap records finalize an unsupported level, so the
/// controller stops right after that batch commits, and the registrations that
/// follow it would each retry against the dead controller under backoff. The
/// backoff here is an hour, so a start that waited on it would time the test out
/// instead of returning.
#[tokio::test]
async fn a_fault_while_joining_the_quorum_fails_the_start_without_waiting_on_backoff() {
    let dir = tempdir().unwrap();
    write_bootstrap_records(dir.path(), &[finalize_unstable_metadata_version()]);
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.self_registration_backoff_min = krabka_units::hours(1);
    config.self_registration_backoff_max = krabka_units::hours(1);

    let started = tokio::time::timeout(std::time::Duration::from_secs(60), Broker::start(config))
        .await
        .expect("the start waited on the retry backoff of a dead controller");

    let Err(refused) = started else {
        panic!("a broker without the flag started over a log that finalizes an unstable level");
    };
    check!(
        matches!(&refused, BrokerError::FatalFault(fault) if fault == UNSUPPORTED_LEVEL_FAULT),
        "{refused}"
    );
}

/// Kafka halts the process on a controller fault, so no `LogManager.shutdown`
/// runs and no `.kafka_cleanshutdown` is written: the restart registers with
/// `previousBrokerEpoch` -1, an unclean shutdown for ELR. A stop that a fault
/// caused leaves no proof either, and an ordinary stop still does.
#[tokio::test]
async fn only_a_stop_without_a_fatal_fault_leaves_a_clean_shutdown_proof() {
    for (name, faulted, proof_expected) in [
        ("an ordinary stop", false, true),
        ("a stop after a fatal fault", true, false),
    ] {
        let dir = tempdir().unwrap();
        let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("broker start");
        if faulted {
            let mut should_shutdown = handle.should_shutdown_rx();
            latch_unsupported_fault(&handle, &mut should_shutdown).await;
        }

        handle.shutdown().await;

        check!(
            dir.path().join(".kafka_cleanshutdown").exists() == proof_expected,
            "{name}"
        );
    }
}

async fn latch_unsupported_fault(
    handle: &BrokerHandle,
    should_shutdown: &mut tokio::sync::watch::Receiver<bool>,
) {
    handle
        .submit_metadata_record_for_test(finalize_unstable_metadata_version())
        .await
        .expect("the unsupported level commits before the controller stops");

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        should_shutdown.wait_for(|down| *down),
    )
    .await
    .expect("the fault did not latch the self-shutdown flag within 30s")
    .expect("the self-shutdown flag closed");
}
