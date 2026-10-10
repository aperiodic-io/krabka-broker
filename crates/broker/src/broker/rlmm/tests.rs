use assert2::{assert, check};
use krabka_units::{mebibytes, millis, minutes, secs};
use tempfile::tempdir;

use super::*;

#[test]
fn loopback_bootstrap_maps_wildcard_to_loopback() {
    use std::net::SocketAddr;
    let cases = [
        ("0.0.0.0:9092", "127.0.0.1:9092"),
        ("192.168.1.5:9094", "192.168.1.5:9094"),
        ("[::]:9092", "[::1]:9092"),
        ("[2001:db8::5]:9092", "[2001:db8::5]:9092"),
    ];
    for (listen, expected) in cases {
        assert!(
            loopback_bootstrap(listen.parse::<SocketAddr>().unwrap()) == expected,
            "listen {listen}"
        );
    }
}

#[tokio::test]
async fn rlmm_bootstrap_backoff_returns_false_when_cancelled() {
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let mut backoff = std::time::Duration::from_mins(1);

    assert!(
        !rlmm_bootstrap_backoff(&mut backoff, std::time::Duration::from_mins(2), &shutdown,).await
    );
    assert!(backoff == std::time::Duration::from_mins(1));
}

#[tokio::test]
async fn rlmm_bootstrap_backoff_returns_true_after_sleep_and_advances() {
    tokio::time::pause();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(async move {
        let mut backoff = std::time::Duration::from_millis(250);
        let ok = rlmm_bootstrap_backoff(
            &mut backoff,
            std::time::Duration::from_millis(500),
            &shutdown,
        )
        .await;
        (ok, backoff)
    });

    tokio::time::advance(std::time::Duration::from_millis(250)).await;
    let (ok, backoff) = task.await.expect("backoff task");
    assert!(ok);
    assert!(backoff == std::time::Duration::from_millis(500));
}

#[tokio::test]
async fn rlmm_reconciler_applies_initial_and_changed_assignments() {
    let log: Arc<dyn krabka_remote_storage_topic::MetadataEventLog> =
        krabka_remote_storage_topic::InProcessMetadataEventLog::new(3);
    let snapshot_dir = tempfile::tempdir().expect("snapshot tempdir");
    let manager = krabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
        log,
        tokio::runtime::Handle::current(),
        snapshot_dir.path().join("rlmm-manager"),
        std::time::Duration::from_hours(1),
    )
    .expect("topic-backed manager start");
    let (set_tx, set_rx) = tokio::sync::watch::channel(vec![0, 2]);
    let shutdown = CancellationToken::new();

    let reconciler = tokio::spawn(run_rlmm_reconciler(
        manager.clone(),
        set_rx,
        std::time::Duration::from_secs(1),
        shutdown.clone(),
    ));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while manager.assigned_metadata_partitions() != vec![0, 2] {
        assert!(
            std::time::Instant::now() < deadline,
            "initial assignment was not reconciled"
        );
        tokio::task::yield_now().await;
    }

    set_tx.send(vec![1]).expect("send changed assignment");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while manager.assigned_metadata_partitions() != vec![1] {
        assert!(
            std::time::Instant::now() < deadline,
            "changed assignment was not reconciled"
        );
        tokio::task::yield_now().await;
    }

    shutdown.cancel();
    reconciler.await.expect("reconciler exits");
}

#[test]
fn needed_metadata_partitions_covers_led_and_followed() {
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use krabka_remote_storage::TopicIdPartition;
    use krabka_remote_storage_topic::metadata_partition_for;
    use uuid::Uuid;

    let topic_id = Uuid::from_u128(0xABCD);
    let mut image = MetadataImage::new(Uuid::from_u128(1));
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id,
        partitions: 3,
        replication_factor: 2,
    }));
    // node 7 leads p0, follows p1 (replica), is absent from p2.
    for (partition, leader, replicas) in [
        (0_i32, 7_u64, vec![7_u64, 8]),
        (1, 8, vec![8, 7]),
        (2, 8, vec![8, 9]),
    ] {
        image.apply(&MetadataRecord::V1Partition(
            crate::handlers::test_support::replicated_partition(
                crate::handlers::test_support::ReplicatedPartitionSetup {
                    partition: krabka_ids::PartitionIndex(partition),
                    leader: krabka_audit::NodeId(leader),
                    replicas: &crate::test_support::replica_nodes(&replicas),
                    ..Default::default()
                },
            ),
        ));
    }

    let got = needed_metadata_partitions(&image, krabka_audit::NodeId(7), 50);

    let mut expected = vec![
        metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 0), 50),
        metadata_partition_for(&TopicIdPartition::new(topic_id, "orders", 1), 50),
    ];
    expected.sort_unstable();
    expected.dedup();
    assert!(
        got == expected,
        "p2 (node 7 not a replica) must be excluded"
    );
}

#[test]
fn rlmm_backoff_doubles_then_caps() {
    use std::time::Duration;
    let max = Duration::from_secs(10);
    let cases = [
        (Duration::from_millis(250), Duration::from_millis(500)),
        (Duration::from_secs(8), max), // 16s capped to 10s
        (max, max),
    ];
    for (current, expected) in cases {
        assert!(
            next_rlmm_backoff(current, max) == expected,
            "current {current:?}"
        );
    }
}

#[test]
fn diskless_index_gets_topic_kickoff_with_in_memory_rlmm() {
    let dir = tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    check!(matches!(
        config.remote_log_metadata,
        crate::config::RlmmKind::InMemory
    ));
    check!(kafka_swap_kickoff(&config).is_none());

    config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
        dir: dir.path().join("objects"),
    });
    let kickoff = kafka_swap_kickoff(&config).expect("diskless index kickoff");
    check!(kickoff.cfg.num_partitions == crate::config::DEFAULT_RLMM_TOPIC_NUM_PARTITIONS);
    check!(kickoff.cfg.replication == crate::config::DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR);
    check!(kickoff.cfg.min_isr == crate::config::DEFAULT_RLMM_TOPIC_MIN_ISR);
}

#[test]
fn metadata_log_config_copies_shared_transport_policy() {
    let policy = crate::config::KafkaRlmmConfig {
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(7)
            .unwrap(),
        frame_max: krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
            .unwrap(),
        bootstrap: "broker-0:9094".into(),
        num_partitions: 8,
        replication: 2,
        min_isr: 1,
        topic_create_timeout: secs(45),
        fetch_max_wait: millis(750),
        fetch_max_bytes: mebibytes(2),
        fetch_retry_backoff: millis(300),
        event_queue_capacity: krabka_remote_storage_topic::MetadataEventQueueCapacity::new(2048)
            .unwrap(),
        ..crate::config::KafkaRlmmConfig::default()
    };

    let rlmm = metadata_log_config(
        &policy,
        krabka_remote_storage_topic::METADATA_TOPIC.to_owned(),
        "rlmm-client".to_owned(),
    );
    let diskless = metadata_log_config(
        &policy,
        crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC.to_owned(),
        "diskless-client".to_owned(),
    );

    for config in [&rlmm, &diskless] {
        check!(config.bootstrap == "broker-0:9094");
        check!(config.num_partitions == 8);
        check!(config.replication == 2);
        check!(config.min_isr == Some(1));
        check!(config.topic_create_timeout == secs(45));
        check!(config.fetch_max_wait == millis(750));
        check!(config.fetch_max_bytes == mebibytes(2));
        check!(config.fetch_retry_backoff == millis(300));
        check!(config.event_queue_capacity.capacity() == 2048);
        check!(config.dispatch_queue_capacity.get() == 7);
        check!(config.frame_max.size() == krabka_units::kibibytes(32));
    }
    check!(rlmm.topic == krabka_remote_storage_topic::METADATA_TOPIC);
    check!(rlmm.client_id == "rlmm-client");
    check!(diskless.topic == crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC);
    check!(diskless.client_id == "diskless-client");
}

#[tokio::test]
async fn cancelled_topic_rlmm_bootstrap_attempts_once_without_activating() {
    // A loopback address with nothing listening: bind to learn a free
    // port, then drop the listener so the bootstrap's dial cannot
    // succeed. On Windows such a connect does not fail fast, which is
    // exactly why the bootstrap must honour the token mid-attempt.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bootstrap = listener.local_addr().unwrap().to_string();
    drop(listener);

    let swap = Arc::new(krabka_remote_storage_topic::SwappableRlmm::new(Arc::new(
        krabka_remote_storage_topic::NotReadyRlmm::new(),
    )));
    let snapshot_dir = tempdir().unwrap();
    let cfg = super::test_kickoff(crate::config::KafkaRlmmConfig {
        bootstrap,
        num_partitions: 1,
        replication: 1,
        min_isr: 1,
        snapshot_interval: minutes(1),
        snapshot_dir: snapshot_dir.path().to_path_buf(),
        security: None,
        ..crate::config::KafkaRlmmConfig::default()
    });
    let metrics = crate::metrics::BrokerMetrics::new();
    let (_image_tx, image_rx) = tokio::sync::watch::channel(Arc::new(
        krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
    ));
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        bootstrap_topic_rlmm(
            swap,
            cfg,
            tokio::runtime::Handle::current(),
            metrics.clone(),
            krabka_raft::NodeId(7),
            image_rx,
            shutdown,
        ),
    )
    .await
    .expect("cancelled bootstrap should return promptly");

    // One attempt was recorded, but the cancelled token stopped the
    // dial before anything could activate the topic-backed manager.
    assert!(metrics.tiered_storage_rlmm_bootstrap_attempts.get() == 1);
    assert!(metrics.tiered_storage_rlmm_topic_backed.get() == 0);
}
