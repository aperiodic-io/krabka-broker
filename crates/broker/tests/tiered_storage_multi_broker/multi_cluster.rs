//! Boot of the three in-process brokers this suite needs, and the two waits
//! that gate on the cluster reaching a usable state.
//!
//! The scenario needs a 3-voter static quorum with a *shared* `Local` remote
//! tier and a topic-backed `RlmmKind`, all bootstrapping into broker 1's own
//! loopback listener. That boot is long enough, and specific enough, to sit
//! apart from the test that drives it. The waits belong beside it because both
//! observe the same three handles: broker registration on every node, and the
//! `tiered_storage_rlmm_topic_backed` gauge flipping on every node.

use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, RemoteStorageBackend,
    RlmmKind,
};
use tempfile::TempDir;

use crate::support;

/// Boots three in-process brokers with a shared Local remote tier and a
/// topic-backed RLMM.
///
/// The test needs a 3-voter quorum, so that the surviving 2 out of 3 can commit
/// the partition-leader-election record after broker 1 shuts down.
///
/// Returns `(broker1, broker2, broker3, dirs[], shared_remote_dir)`. All
/// brokers share the remote dir, so they write to and read from the same object
/// store.
pub(crate) async fn start_three_tiered_brokers() -> (
    BrokerHandle,
    BrokerHandle,
    BrokerHandle,
    Vec<TempDir>,
    TempDir,
) {
    let default_segment_size = krabka_broker::BrokerConfig::for_tests(std::path::PathBuf::from(
        "/nonexistent-log-dir-for-defaults",
    ))
    .log_config
    .segment_size;
    start_three_tiered_brokers_with_segment_sizes([default_segment_size; 3]).await
}

/// The same three brokers, each with its own `log.segment.bytes`.
///
/// A replica rolls its own segments, so two replicas of one partition need not
/// agree on where a segment ends. Real clusters reach that state through a
/// config change, a restart or a reassignment; a test reaches it by giving the
/// brokers different broker-level defaults and creating a topic that overrides
/// neither. The offsets are the same on every replica; only the boundaries
/// differ, which is exactly the condition a base-offset skip set mishandles.
pub(crate) async fn start_three_tiered_brokers_with_segment_sizes(
    segment_sizes: [krabka_units::ByteSize; 3],
) -> (
    BrokerHandle,
    BrokerHandle,
    BrokerHandle,
    Vec<TempDir>,
    TempDir,
) {
    support::init_tracing();

    // Pre-bind concrete client + controller ports for all 3 brokers.
    // Concrete ports are required: the advertised_listener is registered into
    // the controller image before the listener binds (a `:0` would register
    // port 0 and break inter-broker replication); controller ports go into the
    // static voter set so peers can dial each other.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(3).await;

    let log_dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("log dir")).collect();
    // **Shared** remote dir: all brokers point at the same Local object store.
    let remote_dir = TempDir::new().expect("shared remote dir");

    // 3-voter static voter set.
    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Build a config for broker `i` (1-indexed broker_id/node_id).
    let broker_configs: Vec<BrokerConfig> = (0..3)
        .map(|i| {
            let mut cfg =
                crate::support::node_config(crate::support::NodeIndex(i), log_dirs[i].path());
            cfg.log_config.segment_size = segment_sizes[i];
            cfg.directory_id = uuid::Uuid::from_u128(u128::try_from(i + 1).unwrap());
            cfg.listen_addr = client_addrs[i];
            cfg.advertised_listener = format!("127.0.0.1:{}", client_addrs[i].port());
            cfg.controller_listen_addr = controller_addrs[i];
            cfg.controller_quorum_voters = crate::support::controller_voters(&voters);
            cfg.bootstrap_mode = BootstrapMode::Bootstrap;
            cfg.auto_join = false;
            cfg.bootstrap_servers = vec![];
            cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
                dir: remote_dir.path().to_path_buf(),
            });
            cfg.remote_log_manager_interval = krabka_units::secs(1);
            // RLMM: all 3 brokers bootstrap into broker 1's loopback.
            // num_partitions=1 keeps all metadata on a single partition.
            // replication=3 prevents the topic from being created before all
            // brokers are registered; sorted placement makes broker 1 leader.
            // Broker 2's RLMM consumer reads CopySegment events from broker 1
            // before broker 1 dies; the cached metadata is then used for remote
            // reads from the survivor.
            cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
                bootstrap: format!("127.0.0.1:{}", client_addrs[0].port()),
                num_partitions: 1,
                replication: 3,
                snapshot_interval: krabka_units::hours(1),
                snapshot_dir: std::path::PathBuf::new(), // derived from log_dir
                security: None,
                ..KafkaRlmmConfig::default()
            });
            cfg
        })
        .collect();

    // Static cold-boot: all 3 start concurrently (sequential would deadlock —
    // a leader needs a majority of the static voter set up).
    let starts: Vec<_> = broker_configs
        .into_iter()
        .zip(controller_listeners)
        .zip(client_listeners)
        .map(|((config, controller), client)| {
            tokio::spawn(async move {
                Broker::start_with_listeners(config, Some(controller), Some(client)).await
            })
        })
        .collect();
    let mut starts = starts.into_iter();
    let b1 = starts
        .next()
        .unwrap()
        .await
        .expect("b1 spawn join")
        .expect("b1 start");
    let b2 = starts
        .next()
        .unwrap()
        .await
        .expect("b2 spawn join")
        .expect("b2 start");
    let b3 = starts
        .next()
        .unwrap()
        .await
        .expect("b3 spawn join")
        .expect("b3 start");

    (b1, b2, b3, log_dirs, remote_dir)
}

/// Waits until all three brokers see each other registered, that is, until
/// `broker_count` >= 3.
pub(crate) async fn await_all_brokers_registered(
    b1: &BrokerHandle,
    b2: &BrokerHandle,
    b3: &BrokerHandle,
) {
    // Each broker's own metadata image must show all 3 brokers registered.
    b1.wait_until_brokers_registered(3).await;
    b2.wait_until_brokers_registered(3).await;
    b3.wait_until_brokers_registered(3).await;
}

/// Waits until the topic-backed RLMM is active on all three brokers.
pub(crate) async fn await_all_rlmm_active(b1: &BrokerHandle, b2: &BrokerHandle, b3: &BrokerHandle) {
    // Topic-backed RLMM going live flips the tiered_storage_rlmm_topic_backed
    // gauge to 1 on each broker (the same signal rlmm_topic_backed_active_for_test
    // reads directly).
    b1.wait_for_metrics("b1 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b2.wait_for_metrics("b2 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b3.wait_for_metrics("b3 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
}

/// Connect the admin only once registration and the topic-backed RLMM are ready.
pub(crate) async fn ready_admin(
    brokers: [&BrokerHandle; 3],
    client_id: &str,
) -> krabka_client_core::Client {
    let [b1, b2, b3] = brokers;
    await_all_brokers_registered(b1, b2, b3).await;
    await_all_rlmm_active(b1, b2, b3).await;
    let bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());
    support::client::connect_owned(&bootstrap, client_id, "admin client").await
}
