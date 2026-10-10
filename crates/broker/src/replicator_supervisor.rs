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

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use krabka_log::LogConfig;
use krabka_raft::NodeId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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

use self::dir_assignments::{AssignDirsReporter, NetworkAssignDirsReporter};
pub(crate) use self::{
    desired_sets::{desired_follower_set, desired_local_set, desired_wal_placements},
    materialize::{MaterializePartitionConfig, materialize_partition},
    topic_config::push_topic_configs,
};
use crate::{
    config::ReplicationRuntimeConfig, partition_registry::PartitionRegistry,
    replicator::ReplicationConnectionConfig, throttle::ThrottleState,
    txn::coordinator::TxnCoordinator,
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
type FetcherKey = (NodeId, usize);

/// One running fetcher: the task, the token that stops it, the partitions it
/// follows, and the endpoint it dialled.
struct FetcherTask {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
    /// Shared with the running task. A reconcile writes the partitions the
    /// fetcher should follow; the fetcher reads them at the top of each round.
    followed: crate::replicator::FollowedPartitions,
    /// The leader endpoint this fetcher dialled. A leader that re-registers on
    /// a different address needs a new connection, which means a new fetcher.
    endpoint: (String, u16),
}

#[derive(Debug)]
struct WalFollowerTask {
    shutdown: CancellationToken,
    handle: JoinHandle<()>,
}

fn resolve_leader_endpoint(
    broker: &krabka_metadata::BrokerRegistrationRecord,
    listener_name: &str,
) -> (String, u16) {
    let (host, port) = crate::broker::registered_listener_endpoint(broker, listener_name);
    (host.to_owned(), port)
}

pub(crate) struct ReplicatorSupervisor {
    node_id: NodeId,
    broker_id: i32,
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    log_dirs: Vec<PathBuf>,
    log_config: LogConfig,
    unstable_api_versions: crate::api_catalog::UnstableApiVersions,
    client_id: String,
    tasks: DashMap<FetcherKey, FetcherTask>,
    wal_tasks: DashMap<crate::wal::quorum::registry::ShardId, WalFollowerTask>,
    wal_task_targets: DashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    shutdown: CancellationToken,
    txn_coordinator: Option<Arc<TxnCoordinator>>,
    /// KIP-932 share coordinator. Each reconcile refreshes its view of
    /// locally-led `__share_group_state` partitions, the same as for the
    /// txn coordinator.
    share_coordinator: Option<Arc<crate::share_coordinator::coordinator::ShareCoordinator>>,
    /// Shared outbound dialer. It uses TLS and SASL when configured, and raw
    /// TCP otherwise. Each spawned replicator clones this Arc.
    inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
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
    log_dir_status: crate::log_dir_status::LogDirRegistry,
    /// Broker-wide idempotent/transactional producer-sequence tracker.
    /// Forwarded into each `materialize_partition`, so that the partition
    /// writer copies the producer state of each replicated transaction marker
    /// into it, and into each spawned replicator.
    producer_state: Arc<crate::producer_state::ProducerState>,
    max_produce_group: usize,
    partition_writer_queue_depth: usize,
    diskless_wal_local_replica_count: usize,
    /// Broker-wide metrics handle. Each spawned replicator
    /// clones this so it can increment `replication_bytes_in` after a
    /// successful follower-side append.
    metrics: crate::metrics::BrokerMetrics,
    /// KIP-858: stable UUID per configured log.dir. The reconcile loop uses
    /// these to build `AssignReplicasToDirs` reports.
    log_dir_ids: crate::log_dir_id::LogDirIds,
    /// Shared advisory cache for quorum-committed diskless WAL tails.
    hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    /// Registry exposed through the KIP-595 shard router for diskless WAL
    /// fetches to newly materialized partitions.
    wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
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
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
    pub partitions: Arc<PartitionRegistry>,
    pub log_dirs: Vec<PathBuf>,
    pub log_config: LogConfig,
    /// Whether the broker serves Kafka trunk's topic keys, which is what lets
    /// `max.decompressed.message.bytes` reach the partition logs.
    pub unstable_api_versions: crate::api_catalog::UnstableApiVersions,
    pub client_id: String,
    pub shutdown: CancellationToken,
    pub txn_coordinator: Option<Arc<TxnCoordinator>>,
    pub share_coordinator: Option<Arc<crate::share_coordinator::coordinator::ShareCoordinator>>,
    pub inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
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
    pub log_dir_status: crate::log_dir_status::LogDirRegistry,
    pub producer_state: Arc<crate::producer_state::ProducerState>,
    pub max_produce_group: usize,
    pub partition_writer_queue_depth: usize,
    pub diskless_wal_local_replica_count: usize,
    pub metrics: crate::metrics::BrokerMetrics,
    pub log_dir_ids: crate::log_dir_id::LogDirIds,
    pub hot_tail: Arc<crate::diskless::hot_tail::HotTailCache>,
    pub wal_shards: Arc<crate::wal::quorum::registry::WalShardRegistry>,
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
                dialer: crate::controller_endpoint::ControllerDialer {
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
        loop {
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
            tokio::select! {
                () = self.shutdown.cancelled() => break,
                res = rx.changed() => {
                    if res.is_err() {
                        break;
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

    pub(crate) fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        broker_record, image_with, partition_record, supervisor_fixture, topic_record,
    };

    #[test]
    fn resolve_leader_endpoint_prefers_matching_listener() {
        let broker = broker_record(NodeId(1));
        assert!(resolve_leader_endpoint(&broker, "INTERNAL") == ("internal-host".into(), 19092));
        assert!(resolve_leader_endpoint(&broker, "EXTERNAL") == ("legacy-host".into(), 9092));
    }

    #[tokio::test]
    async fn run_reconciles_initial_image_before_shutdown() {
        let img = image_with(&[
            topic_record(crate::replicator_supervisor::test_support::SupervisorTopicSetup::default()),
            partition_record(crate::replicator_supervisor::test_support::SupervisorPartitionSetup::single_replica("t", NodeId(2))),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img);
        supervisor.shutdown.cancel();

        supervisor.run().await;

        assert!(partitions.contains("t", PartitionIndex(0)));
    }
}
