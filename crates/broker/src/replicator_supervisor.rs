//! Subscribes to the controller's metadata-image watch channel and
//! on each apply:
//!
//! 1. **Materializes the local on-disk partition** for any
//!    `(topic, partition)` where this broker is in `replicas`,
//!    regardless of leader/follower role. With round-robin replica
//!    placement, the broker that handles a `CreateTopics` request is
//!    usually not the partition leader. The lazy supervisor-driven path
//!    is therefore the only one that materializes the partition on the
//!    leader broker reliably.
//!
//! 2. **Spawns a `replicator::run_fetcher` task** per `(leader, fetcher id)`
//!    this broker follows anything from, hands it the partitions where this
//!    broker is in `replicas` but is NOT the leader, and cancels a fetcher
//!    once nothing is left for it to follow.
//!
//!    A partition that joins or leaves a running fetcher does so through the
//!    fetcher's shared partition map, so a reassignment costs a map write
//!    rather than a task teardown and a fresh dial. `num.replica.fetchers`
//!    (`[runtime] replica_fetchers`) says how many fetchers a leader gets.

#[cfg(test)]
use crate::share_coordinator::coordinator::test_support::assert_recovered_epoch_is_fenced;
use {
    dashmap::DashMap,
    krabka_log::LogConfig,
    krabka_raft::NodeId,
    std::{
        collections::HashMap,
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio::task::JoinHandle,
    tokio_util::sync::CancellationToken,
};
mod desired_sets;
mod dir_assignments;
mod local_partitions;
mod materialize;
mod pruning;
mod reconcile;
mod replica_tasks;
#[cfg(test)]
mod test_support;
mod topic_config;
mod wal_followers;

pub(crate) use self::{
    desired_sets::{desired_follower_set, desired_local_set, desired_wal_placements},
    materialize::{MaterializePartitionConfig, materialize_partition},
    topic_config::push_topic_configs,
};
use {
    self::dir_assignments::{AssignDirsReporter, NetworkAssignDirsReporter},
    crate::{
        api_catalog::UnstableApiVersions,
        broker::registered_listener_endpoint,
        config::ReplicationRuntimeConfig,
        controller_endpoint::ControllerDialer,
        diskless::hot_tail::HotTailCache,
        log_dir_id::LogDirIds,
        log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource,
        metrics::BrokerMetrics,
        network::client::InterBrokerClient,
        partition_registry::PartitionRegistry,
        producer_state::ProducerState,
        replicator::{FollowedPartitions, ReplicationConnectionConfig},
        share_coordinator::coordinator::ShareCoordinator,
        throttle::ThrottleState,
        txn::coordinator::TxnCoordinator,
        wal::quorum::registry::{ShardId, WalShardRegistry},
    },
};

/// A `(topic, partition)` pair. The supervisor keys follower tasks, local
/// materialization, and dir-assignment reports on this pair.
pub(crate) type TopicPartition = (String, i32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalFollowerSpec {
    topic: String,
    leader: NodeId,
    leader_epoch: krabka_metadata::LeaderEpoch,
}

/// Which fetcher a partition belongs to: its leader, and the fetcher id the
/// partition hashes onto within that leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct FetcherKey {
    leader: NodeId,
    fetcher_id: usize,
}

/// The host and port of a leader's listener, as a fetcher dials it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LeaderEndpoint {
    host: String,
    port: u16,
}

/// One running fetcher: the task, the token that stops it, the partitions it
/// follows, and the endpoint it dialled.
struct FetcherTask {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    /// Shared with the running task. A reconcile writes the partitions the
    /// fetcher should follow; the fetcher reads them at the top of each round.
    followed: FollowedPartitions,
    /// The leader endpoint this fetcher dialled. A leader that re-registers on
    /// a different address needs a new connection, which means a new fetcher.
    endpoint: LeaderEndpoint,
}

#[derive(Debug)]
struct WalFollowerTask {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

fn resolve_leader_endpoint(
    broker: &krabka_metadata::BrokerRegistrationRecord,
    listener_name: &str,
) -> LeaderEndpoint {
    let (host, port) = registered_listener_endpoint(broker, listener_name);
    LeaderEndpoint {
        host: host.to_owned(),
        port,
    }
}

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    broker_id: i32,
    controller: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    log_dirs: Vec<PathBuf>,
    log_config: LogConfig,
    unstable_api_versions: UnstableApiVersions,
    client_id: String,
    tasks: DashMap<FetcherKey, FetcherTask>,
    wal_tasks: DashMap<ShardId, WalFollowerTask>,
    wal_task_targets: DashMap<ShardId, WalFollowerSpec>,
    shutdown: CancellationToken,
    txn_coordinator: Option<Arc<TxnCoordinator>>,
    /// KIP-932 share coordinator. Each reconcile refreshes its view of
    /// locally-led `__share_group_state` partitions, the same as for the
    /// txn coordinator.
    share_coordinator: Option<Arc<ShareCoordinator>>,
    /// Shared outbound dialer. It uses TLS and SASL when configured, and raw
    /// TCP otherwise. Each spawned replicator clones this Arc.
    inter_broker_client: Arc<InterBrokerClient>,
    /// Listener protocol used for inter-broker dials. It decides whether
    /// the dialer runs TLS and SASL.
    inter_broker_listener_protocol: krabka_security::ListenerProtocol,
    inter_broker_server_name: String,
    replication: ReplicationRuntimeConfig,
    /// Name of the listener whose endpoint the supervisor resolves from the
    /// metadata image when it dials peers.
    inter_broker_listener_name: String,
    /// KIP-73: broker-wide throttle state forwarded to each spawned
    /// replicator so they can consult the follower-in token bucket.
    throttle_state: Arc<ThrottleState>,
    /// KIP-113 runtime offline-dir registry. Forwarded into each
    /// `materialize_partition` and each spawned `Replicator::Config`, so the
    /// partition writer's storage-failure path can flip the dir
    /// offline broker-wide.
    log_dir_status: LogDirRegistry,
    /// Broker-wide idempotent/transactional producer-sequence tracker.
    /// Forwarded into each `materialize_partition`, so that the partition
    /// writer copies the producer state of each replicated transaction marker
    /// into it, and into each spawned replicator.
    producer_state: Arc<ProducerState>,
    max_produce_group: usize,
    partition_writer_queue_depth: usize,
    diskless_wal_local_replica_count: usize,
    /// Broker-wide metrics handle. Each spawned replicator
    /// clones this so it can increment `replication_bytes_in` after a
    /// successful follower-side append.
    metrics: BrokerMetrics,
    /// KIP-858: stable UUID per configured log.dir. The reconcile loop uses
    /// these to build `AssignReplicasToDirs` reports.
    log_dir_ids: LogDirIds,
    /// Shared advisory cache for quorum-committed diskless WAL tails.
    hot_tail: Arc<HotTailCache>,
    /// Registry exposed through the KIP-595 shard router for diskless WAL
    /// fetches to newly materialized partitions.
    wal_shards: Arc<WalShardRegistry>,
    /// KIP-858: tracks the last-reported dir UUID per (topic, partition), so
    /// the supervisor sends `AssignReplicasToDirs` only on first
    /// materialization or after a KIP-113 log-dir swap.
    reported_dirs: dashmap::DashMap<TopicPartition, uuid::Uuid>,
    /// Topic identities observed by the preceding reconcile. A comparison of
    /// UUIDs, rather than names alone, also detects a delete followed by a
    /// same-name recreation, and it does not treat startup-only on-disk logs
    /// as tombstoned.
    known_topic_ids: Mutex<HashMap<String, uuid::Uuid>>,
    assign_dirs_reporter: Arc<dyn AssignDirsReporter>,
}

pub(crate) struct ReplicatorSupervisorConfig {
    pub node_id: NodeId,
    pub broker_id: i32,
    pub controller: Arc<dyn MetadataSource>,
    pub partitions: Arc<PartitionRegistry>,
    pub log_dirs: Vec<PathBuf>,
    pub log_config: LogConfig,
    /// Whether the broker serves Kafka trunk's topic keys, which is what lets
    /// `max.decompressed.message.bytes` reach the partition logs.
    pub unstable_api_versions: UnstableApiVersions,
    pub client_id: String,
    pub shutdown: CancellationToken,
    pub txn_coordinator: Option<Arc<TxnCoordinator>>,
    pub share_coordinator: Option<Arc<ShareCoordinator>>,
    pub inter_broker_client: Arc<InterBrokerClient>,
    pub inter_broker_listener_protocol: krabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    pub inter_broker_listener_name: String,
    /// The controller listener's protocol, its SNI / SASL server name, and the
    /// statically configured quorum. KIP-858 dir-assignment reports are
    /// addressed to the controller leader's CONTROLLER listener, so they
    /// travel that channel rather than the inter-broker one.
    pub controller_listener_protocol: krabka_security::ListenerProtocol,
    pub controller_server_name: String,
    pub controller_quorum_voters: Vec<(NodeId, String)>,
    pub replication: ReplicationRuntimeConfig,
    pub throttle_state: Arc<ThrottleState>,
    pub log_dir_status: LogDirRegistry,
    pub producer_state: Arc<ProducerState>,
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub metrics: BrokerMetrics,
    pub log_dir_ids: LogDirIds,
    pub hot_tail: Arc<HotTailCache>,
    pub wal_shards: Arc<WalShardRegistry>,
}

impl ReplicatorSupervisor {
    /// Clone the common dialer and policy after each initializer's identity and shutdown.
    fn connection_config(&self) -> ReplicationConnectionConfig {
        ReplicationConnectionConfig {
            inter_broker_client: self.inter_broker_client.clone(),
            inter_broker_listener_protocol: self.inter_broker_listener_protocol,
            inter_broker_server_name: self.inter_broker_server_name.clone(),
            replication: self.replication.clone(),
        }
    }

    pub(crate) fn new(config: ReplicatorSupervisorConfig) -> Self {
        let ReplicatorSupervisorConfig {
            node_id,
            broker_id,
            controller,
            partitions,
            log_dirs,
            log_config,
            unstable_api_versions,
            client_id,
            shutdown,
            txn_coordinator,
            share_coordinator,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_server_name,
            inter_broker_listener_name,
            replication,
            throttle_state,
            log_dir_status,
            producer_state,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            metrics,
            log_dir_ids,
            hot_tail,
            wal_shards,
            controller_listener_protocol,
            controller_server_name,
            controller_quorum_voters,
        } = config;
        let assign_dirs_reporter: Arc<dyn AssignDirsReporter> =
            Arc::new(NetworkAssignDirsReporter {
                dialer: ControllerDialer {
                    outbound_client: Arc::clone(&inter_broker_client),
                    listener_protocol: controller_listener_protocol,
                    server_name: controller_server_name,
                    quorum_voters: controller_quorum_voters,
                },
            });
        let known_topic_ids = controller
            .current_image()
            .topics()
            .map(|topic| (topic.name.clone(), topic.topic_id))
            .collect();
        Self {
            node_id,
            broker_id,
            controller,
            partitions,
            log_dirs,
            log_config,
            unstable_api_versions,
            client_id,
            tasks: DashMap::new(),
            wal_tasks: DashMap::new(),
            wal_task_targets: DashMap::new(),
            shutdown,
            txn_coordinator,
            share_coordinator,
            inter_broker_client,
            inter_broker_listener_protocol,
            inter_broker_server_name,
            inter_broker_listener_name,
            replication,
            throttle_state,
            log_dir_status,
            producer_state,
            max_produce_group,
            partition_writer_queue_depth,
            diskless_wal_local_replica_count,
            metrics,
            log_dir_ids,
            hot_tail,
            wal_shards,
            reported_dirs: dashmap::DashMap::new(),
            known_topic_ids: Mutex::new(known_topic_ids),
            assign_dirs_reporter,
        }
    }

    pub(crate) async fn run(self) {
        let mut rx = self.controller.watch_image();
        let mut first_reconcile = true;
        'reconciles: loop {
            let image = rx.borrow().clone();
            if first_reconcile {
                self.reconcile(&image).await;
                first_reconcile = false;
            } else {
                tokio::select! {
                    biased;
                    () = self.shutdown.cancelled() => break,
                    () = self.reconcile(&image) => {}
                }
            }
            loop {
                tokio::select! {
                    () = self.shutdown.cancelled() => break 'reconciles,
                    res = rx.changed() => {
                        if res.is_err() {
                            break 'reconciles;
                        }
                        break;
                    }
                    () = async {
                        match &self.share_coordinator {
                            Some(coordinator) => coordinator.recovery_requested().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        // A lost append reply must recover on the state
                        // coordinator even if its metadata image is unchanged.
                        // Refresh only the coordinator: a full reconcile can
                        // enqueue configuration work behind the blocked writer.
                        if let Some(coordinator) = &self.share_coordinator {
                            let image = rx.borrow().clone();
                            drop(coordinator.refresh_leader_partitions(&image).await);
                        }
                    }
                }
            }
        }
        let task_keys = self
            .tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for key in task_keys {
            if let Some((_, task)) = self.tasks.remove(&key) {
                task.shutdown.cancel();
                task.handle.abort();
                let _ = task.handle.await;
            }
        }
        let wal_task_keys = self
            .wal_tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for shard in wal_task_keys {
            if let Some((_, task)) = self.wal_tasks.remove(&shard) {
                task.shutdown.cancel();
                task.handle.abort();
                let _ = task.handle.await;
            }
        }
    }

    pub(crate) fn spawn(self) -> JoinHandle<()> { tokio::spawn(self.run()) }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            replicator_supervisor::test_support::{
                broker_record, image_with, partition_record, supervisor_fixture, topic_record,
            },
            share_coordinator::{
                bootstrap,
                config::ShareCoordinatorConfig,
                coordinator::{
                    LoadStatus, ShareStateError, message, test_support::image_with_topic,
                },
            },
        },
        assert2::assert,
        krabka_ids::PartitionIndex,
        krabka_log::Offset,
        std::time::Duration,
    };

    async fn await_share_status(coordinator: &ShareCoordinator, expected: LoadStatus) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if coordinator.load_status(PartitionIndex(0)).await == Some(expected) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("share recovery reaches the expected status");
    }

    /// The remote state coordinator receives no metadata update when its
    /// accepted append outlives an RPC. Its supervisor must recover on the
    /// uncertainty notice, then retry a failed barrier on the next RPC.
    #[tokio::test]
    async fn unchanged_metadata_recovers_an_indeterminate_share_append_and_client_retry() {
        let image = image_with(&[
            topic_record().topic(bootstrap::TOPIC).call(),
            partition_record()
                .topic(bootstrap::TOPIC)
                .leader(2)
                .replicas(vec![NodeId(2)])
                .call(),
        ]);
        let (mut supervisor, partitions, _reporter, _dir) = supervisor_fixture(image);
        let coordinator = Arc::new(ShareCoordinator::new(
            krabka_metadata::NodeId(2),
            Arc::clone(&partitions),
            ShareCoordinatorConfig {
                state_topic_num_partitions: 1,
                write_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        ));
        supervisor.share_coordinator = Some(Arc::clone(&coordinator));
        let shutdown = supervisor.shutdown.clone();
        let metadata = supervisor.controller.watch_image();
        let task = supervisor.spawn();
        await_share_status(&coordinator, LoadStatus::Active).await;
        let partition = partitions.get(bootstrap::TOPIC, PartitionIndex(0)).unwrap();

        // The real writer retains this accepted append while its reply waits
        // for the HW update. A FIFO barrier cannot pass the suspended writer.
        let hw = partition.replica_state.lock().await;
        let topic_id = uuid::Uuid::from_bytes([73; 16]);
        let topic_image = image_with_topic(topic_id, 1);
        assert!(
            coordinator
                .initialize(&topic_image, "g", topic_id, 0, 2, Offset(10))
                .await
                == Err(ShareStateError::Operation {
                    code: codes::COORDINATOR_NOT_AVAILABLE,
                    message: message::REQUEST_TIMED_OUT,
                })
        );
        // No RPC or metadata refresh triggers this first recovery attempt.
        await_share_status(&coordinator, LoadStatus::Loading).await;
        await_share_status(&coordinator, LoadStatus::Pending).await;
        assert!(!metadata.has_changed().unwrap());

        drop(hw);
        assert!(
            coordinator
                .initialize(&topic_image, "g", topic_id, 0, 1, Offset(20))
                .await
                == Err(ShareStateError::Operation {
                    code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    message: message::COORDINATOR_LOAD_IN_PROGRESS,
                })
        );
        await_share_status(&coordinator, LoadStatus::Active).await;
        assert_recovered_epoch_is_fenced()
            .coordinator(&coordinator)
            .topic_image(&topic_image)
            .topic_id(topic_id)
            .call()
            .await;
        assert!(!metadata.has_changed().unwrap());
        shutdown.cancel();
        task.await.unwrap();
        let writer = partition.take_writer_handle().unwrap();
        writer.abort();
        let _ = writer.await;
    }

    #[test]
    fn resolve_leader_endpoint_prefers_matching_listener() {
        let broker = broker_record(NodeId(1));
        assert!(
            resolve_leader_endpoint(&broker, "INTERNAL")
                == LeaderEndpoint {
                    host: "internal-host".into(),
                    port: 19092,
                }
        );
        assert!(
            resolve_leader_endpoint(&broker, "EXTERNAL")
                == LeaderEndpoint {
                    host: "legacy-host".into(),
                    port: 9092,
                }
        );
    }

    #[tokio::test]
    async fn run_reconciles_initial_image_before_shutdown() {
        let img = image_with(&[
            topic_record().call(),
            partition_record()
                .topic("t")
                .leader(2)
                .replicas(vec![NodeId(2)])
                .call(),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img);
        supervisor.shutdown.cancel();

        supervisor.run().await;

        assert!(partitions.contains("t", PartitionIndex(0)));
    }
}
