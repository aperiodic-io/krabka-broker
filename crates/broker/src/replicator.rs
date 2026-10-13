//! Per-leader replication task.
//!
//! One task -- a *fetcher* -- owns one connection to one leader and every
//! partition this broker follows from that leader. Each round it folds those
//! partitions into a single Kafka `Fetch` request, with `replica_id` set to
//! this broker's `node_id`, and applies the response row by row: appending the
//! batches each row carries, truncating on the KIP-320 in-band divergence
//! signal, and resetting on `OFFSET_OUT_OF_RANGE`. A partition the leader
//! answers `NOT_LEADER_OR_FOLLOWER` is dropped from the fetcher, and the next
//! supervisor reconcile decides where it belongs.
//!
//! Kafka's shape, and the reason for it: a `ReplicaFetcherThread` per
//! `(leader, fetcher id)` pair, with `num.replica.fetchers` fetchers per
//! leader and partitions hashed onto them. A follower of ten thousand
//! partitions across three peers holds a handful of connections and sends a
//! handful of Fetch requests per round, rather than ten thousand of each. The
//! per-round request is a KIP-227 incremental one after the first, so a
//! caught-up follower's request names almost nothing at all.
//!
//! The supervisor adds and removes partitions on a running fetcher through
//! [`FollowedPartitions`] rather than by spawning and cancelling a task, so a
//! reassignment does not redial the leader.

use {
    krabka_ids::PartitionIndex,
    krabka_log::{Log, LogConfig},
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    krabka_raft::NodeId,
    krabka_security::ListenerProtocol,
    std::{
        collections::BTreeMap,
        hash::{Hash as _, Hasher as _},
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
};

pub(crate) mod connection;
mod fetch_loop;
mod follower_throttle;
/// Benchmark seam over the replicator's per-batch work, driven by
/// `benches/perf_deferrals.rs`.
#[cfg(any(test, feature = "test-helpers"))]
pub mod hot_path;
mod raw_fetch;
mod response;
mod session;
#[cfg(test)]
mod test_support;
mod truncation;

pub(crate) use self::follower_throttle::ReplicaLag;
use {
    self::fetch_loop::run_fetcher_loop,
    crate::{
        broker::spawn_partition_with_replication_target, config::ReplicationRuntimeConfig,
        log_dir::place_partition_dir_avoiding, log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource, metrics::BrokerMetrics,
        network::client::InterBrokerClient, partition::ReplicationTarget,
        partition_registry::PartitionRegistry, producer_state::ProducerState,
        throttle::ThrottleState,
    },
};

/// Outbound dialer and pacing policy shared by the two replication loops.
#[derive(Clone)]
pub(crate) struct ReplicationConnectionConfig {
    /// Shared outbound dialer.
    ///
    /// The dialer connects through TLS and SASL when the inter-broker listener
    /// needs them. It falls back to raw TCP for PLAINTEXT. The fetcher's own
    /// connection carries the Fetch rounds; this one is for the
    /// `OffsetForLeaderEpoch` lookup a fenced partition makes on its own.
    pub inter_broker_client: Arc<InterBrokerClient>,
    pub inter_broker_listener_protocol: ListenerProtocol,
    pub inter_broker_server_name: String,
    pub replication: ReplicationRuntimeConfig,
}

#[cfg(test)]
#[bon::builder]
pub(crate) fn replication_connection_for_test(
    #[builder(default = Arc::new(InterBrokerClient::new(None, None)))] inter_broker_client: Arc<
        InterBrokerClient,
    >,
    #[builder(default = ListenerProtocol::Plaintext)]
    inter_broker_listener_protocol: ListenerProtocol,
    #[builder(default = String::from("localhost"))] inter_broker_server_name: String,
    #[builder(default = ReplicationRuntimeConfig::default())] replication: ReplicationRuntimeConfig,
) -> ReplicationConnectionConfig {
    ReplicationConnectionConfig {
        inter_broker_client,
        inter_broker_listener_protocol,
        inter_broker_server_name,
        replication,
    }
}

/// Configuration handed to a single replicator task.
pub(crate) struct Config {
    pub node_id: NodeId,
    /// The topic this task follows, as the `Arc<str>` the partition registry
    /// keys it by.
    ///
    /// The task records `replication_bytes_in` once per replicated record
    /// batch, and the metric's label set holds an `Arc<str>`, so sharing the
    /// registry's copy makes that record a hash and a refcount bump instead of
    /// a `String` allocation per batch.
    pub topic: Arc<str>,
    /// Wire-format `topic_id` for the partition.
    ///
    /// The `Fetch` request needs this value to fill the v13+ wire field. At
    /// v ≥ 13 Kafka drops `FetchTopic.topic` in favour of `topic_id`
    /// (KIP-516), and the handler of the leader resolves the topic name from
    /// `topic_id` only. If the replicator sends `WireUuid::ZERO` here, the
    /// leader returns `UNKNOWN_TOPIC_OR_PARTITION` for every fetch.
    pub topic_id: WireUuid,
    pub partition: PartitionIndex,
    pub leader_node_id: NodeId,
    pub leader_epoch: krabka_metadata::LeaderEpoch,
    /// The `host` portion of the leader from the metadata image.
    ///
    /// This is the inter-broker endpoint when one is available, and the legacy
    /// broker host if not.
    pub leader_host: String,
    pub leader_port: u16,
    pub partitions: Arc<PartitionRegistry>,
    pub log_dirs: Vec<PathBuf>,
    pub log_settings: LogConfig,
    pub client_id: String,
    pub connection: ReplicationConnectionConfig,
    /// KIP-73 broker-wide throttle state.
    ///
    /// The follower-in bucket gates the outbound Fetch bytes while this
    /// partition is throttled.
    pub throttle_state: Arc<ThrottleState>,
    /// Controller handle that reads the current metadata image each Fetch
    /// round.
    ///
    /// The replicator uses the image to look up
    /// `follower.replication.throttled.replicas`.
    pub controller: Arc<dyn MetadataSource>,
    /// KIP-113 runtime offline-dir registry.
    ///
    /// The replicator forwards this registry into `spawn_partition`. The
    /// per-partition writer can then set the owning dir offline after a
    /// segment-write failure or an fsync failure.
    pub log_dir_status: LogDirRegistry,
    /// Broker-wide producer-sequence tracker for idempotent and transactional
    /// producers.
    ///
    /// The replicator forwards this tracker into `spawn_partition` through
    /// `ensure_local_partition`, so that the per-partition writer copies the
    /// producer state of each replicated transaction marker into it. The
    /// replicator also removes the entries of a tail that it truncates.
    pub producer_state: Arc<ProducerState>,
    /// Broker-wide metrics handle.
    ///
    /// The replicator increments `replication_bytes_in` after a successful
    /// follower-side append.
    pub metrics: BrokerMetrics,
    /// How far this partition trails the leader's high watermark, which decides
    /// whether the KIP-73 follower throttle applies to it: a follower that has
    /// caught up is never throttled.
    pub lag: ReplicaLag,
}

/// A topic and partition, as the fetcher's partition map keys it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FollowedKey {
    pub(crate) topic: Arc<str>,
    pub(crate) partition: PartitionIndex,
}

/// A generation of the followed partitions and the entries it holds.
pub(crate) struct FollowedSnapshot {
    pub(crate) generation: u64,
    pub(crate) entries: BTreeMap<FollowedKey, Arc<Config>>,
}

/// The partitions one fetcher follows, shared with the supervisor.
///
/// The supervisor replaces entries as metadata moves. The fetcher clones a
/// snapshot only when the generation changes, so ordinary Fetch rounds never
/// walk this map or hold this lock.
#[derive(Default)]
pub(crate) struct FollowedPartitionsState {
    entries: Mutex<BTreeMap<FollowedKey, Arc<Config>>>,
    generation: std::sync::atomic::AtomicU64,
}

pub(crate) type FollowedPartitions = Arc<FollowedPartitionsState>;

impl FollowedPartitionsState {
    pub(crate) fn new(entries: BTreeMap<FollowedKey, Arc<Config>>) -> Self {
        Self {
            entries: Mutex::new(entries),
            generation: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub(crate) fn lock(
        &self,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'_, BTreeMap<FollowedKey, Arc<Config>>>> {
        self.entries.lock()
    }

    pub(crate) fn replace(&self, entries: BTreeMap<FollowedKey, Arc<Config>>) {
        let mut current = self
            .entries
            .lock()
            .expect("followed-partitions mutex poisoned");
        let unchanged = current.len() == entries.len()
            && current
                .iter()
                .zip(&entries)
                .all(|((old_key, old), (new_key, new))| {
                    old_key == new_key && Arc::ptr_eq(old, new)
                });
        if unchanged {
            return;
        }
        *current = entries;
        drop(current);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn snapshot_if_changed(&self, generation: u64) -> Option<FollowedSnapshot> {
        let current = self.generation.load(std::sync::atomic::Ordering::Acquire);
        (current != generation).then(|| FollowedSnapshot {
            generation: current,
            entries: self
                .entries
                .lock()
                .expect("followed-partitions mutex poisoned")
                .clone(),
        })
    }

    pub(crate) fn remove(&self, key: &FollowedKey) {
        if self
            .entries
            .lock()
            .expect("followed-partitions mutex poisoned")
            .remove(key)
            .is_some()
        {
            self.generation
                .fetch_add(1, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Everything one fetcher needs that is the same for every partition it
/// follows: which leader it dials, how it dials, and how it paces itself.
pub(crate) struct FetcherConfig {
    pub node_id: NodeId,
    pub leader_node_id: NodeId,
    /// The `host` portion of the leader from the metadata image: the
    /// inter-broker endpoint when one is available, and the legacy broker host
    /// if not.
    pub leader_host: String,
    pub leader_port: u16,
    pub client_id: String,
    pub shutdown: CancellationToken,
    pub connection: ReplicationConnectionConfig,
    /// The partitions this fetcher follows right now.
    pub followed: FollowedPartitions,
}

/// Which of a leader's fetchers owns `key`.
///
/// Kafka's `AbstractFetcherManager.getFetcherId`: the topic's hash and the
/// partition index, folded onto `num.replica.fetchers`. The mapping is stable,
/// so a partition stays on one fetcher for as long as its leader does, and a
/// topic's partitions spread across the fetchers rather than piling onto one.
pub(crate) fn fetcher_id_for(topic: &str, partition: i32, fetchers: usize) -> usize {
    let fetchers = fetchers.max(1);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut hasher);
    partition.hash(&mut hasher);
    usize::try_from(hasher.finish() % fetchers as u64).unwrap_or(0)
}

/// Entry point. Drives one leader's replication loop until cancelled.
pub(crate) async fn run_fetcher(fetcher: FetcherConfig) {
    info!(
        leader_node_id = fetcher.leader_node_id.0,
        host = %fetcher.leader_host,
        port = fetcher.leader_port,
        "replicator.fetcher.started"
    );

    if let Err(e) = run_fetcher_loop(&fetcher).await {
        warn!(error = %e, leader_node_id = fetcher.leader_node_id.0,
            "replicator fetcher stopped on unrecoverable error");
    }

    info!(
        leader_node_id = fetcher.leader_node_id.0,
        "replicator.fetcher.stopped"
    );
}

/// Builds or recovers the on-disk `Partition` for this follower.
///
/// The function inserts the partition into the shared `partitions` map of the
/// broker. The function is idempotent.
pub(super) fn ensure_local_partition(cfg: &Config) -> Result<(), String> {
    // `materialize_if_vacant` runs the build under the per-key lock, so two
    // concurrent replicators for the same partition can never both build it.
    cfg.partitions
        .materialize_if_vacant(&cfg.topic, cfg.partition, || {
            // A name that fails the check would put the directory outside
            // the log directory.
            krabka_log::topic_name::validate_topic_name(&cfg.topic).map_err(|e| e.to_string())?;
            let dir = place_partition_dir_avoiding(
                &cfg.log_dirs,
                cfg.partitions.cordoned_log_dirs(),
                &cfg.topic,
                cfg.partition.get(),
            );
            std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
            let log =
                Log::open(&dir, cfg.log_settings.clone()).map_err(|e| format!("Log::open: {e}"))?;
            let owning_dir = dir
                .parent()
                .expect("placed partition dir always has a parent log.dir")
                .to_path_buf();
            Ok(spawn_partition_with_replication_target(
                cfg.topic.to_string(),
                task_replication_target(cfg),
                cfg.partition,
                owning_dir,
                log,
                cfg.log_dir_status.clone(),
                cfg.producer_state.clone(),
                false,
            ))
        })
}

/// `true` if this task no longer targets the leader and epoch in the committed
/// metadata image.
///
/// The cancellation of a follower-replicator on a leadership change is
/// cooperative. The run loop checks the shutdown token only between fetches.
/// The replicator can thus process an in-flight Fetch response after metadata
/// has selected another target. Applying that stale response can append,
/// truncate, or reset the local log against the wrong leader. Each response and
/// each destructive recovery path rechecks the task's immutable target.
fn replication_target_changed(cfg: &Config) -> bool {
    let image = cfg.controller.current_image();
    image
        .topic(&cfg.topic)
        .is_none_or(|topic| topic.topic_id.as_bytes() != &cfg.topic_id.0)
        || image
            .partition(&cfg.topic, cfg.partition.get())
            .is_none_or(|partition| {
                partition.leader != cfg.leader_node_id || partition.leader_epoch != cfg.leader_epoch
            })
}

fn task_replication_target(cfg: &Config) -> ReplicationTarget {
    ReplicationTarget {
        topic_id: Some(uuid::Uuid::from_bytes(cfg.topic_id.0)),
        leader_node_id: cfg.leader_node_id,
        leader_epoch: cfg.leader_epoch,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::replicator::test_support::{
            LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, image_with_leader,
            image_with_topic_id_and_leader, test_config,
        },
        assert2::assert,
    };

    #[test]
    fn replication_target_changed_compares_topic_leader_and_epoch() {
        let cases = [
            (LEADER_ID, krabka_metadata::LeaderEpoch(4), false),
            (NODE_ID, krabka_metadata::LeaderEpoch(4), true),
            (LEADER_ID, krabka_metadata::LeaderEpoch(3), true),
        ];
        for (leader, target_epoch, want) in cases {
            let (mut cfg, _log_dir) = test_config(image_with_leader(leader));
            cfg.leader_epoch = target_epoch;
            assert!(
                replication_target_changed(&cfg) == want,
                "metadata leader {leader}, task epoch {target_epoch:?}"
            );
        }

        let (cfg, _log_dir) = test_config(image_with_topic_id_and_leader(
            uuid::Uuid::new_v4(),
            LEADER_ID,
        ));
        assert!(
            replication_target_changed(&cfg),
            "a recreated topic with the same name and generation is a new target"
        );
    }

    /// Adding a partition to a fetcher is what opens its on-disk log, and the
    /// log opens against the target the metadata image named -- otherwise the
    /// first response would be applied to a partition whose replication target
    /// is still the default.
    #[tokio::test]
    async fn following_a_partition_materializes_it_against_its_replication_target() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let partitions = cfg.partitions.clone();

        ensure_local_partition(&cfg).expect("materialize");

        let partition = partitions
            .get(TOPIC, PartitionIndex(PARTITION))
            .expect("materialized");
        assert!(
            *partition.replication_target.read().await
                == ReplicationTarget {
                    topic_id: Some(uuid::Uuid::from_bytes(WIRE_TOPIC_ID.0)),
                    leader_node_id: LEADER_ID,
                    leader_epoch: krabka_metadata::LeaderEpoch(4),
                }
        );
    }

    /// Kafka hashes a partition onto one of a leader's fetchers and keeps it
    /// there, so the connection a partition rides on does not move under it
    /// between rounds. With one fetcher every partition lands on it.
    #[test]
    fn the_fetcher_a_partition_hashes_onto_is_stable_and_in_range() {
        for fetchers in [0_usize, 1, 4, 7] {
            let bound = fetchers.max(1);
            for partition in 0..32 {
                let first = fetcher_id_for("orders", partition, fetchers);
                assert!(first < bound, "{fetchers} fetchers, partition {partition}");
                assert!(first == fetcher_id_for("orders", partition, fetchers));
            }
        }
        for partition in 0..32 {
            assert!(fetcher_id_for("orders", partition, 1) == 0);
        }
    }

    /// A leader's partitions spread across its fetchers rather than piling
    /// onto one, which is the whole point of `num.replica.fetchers`.
    #[test]
    fn several_fetchers_carry_more_than_one_of_a_topic_s_partitions_each() {
        let used: std::collections::BTreeSet<usize> = (0..64)
            .map(|partition| fetcher_id_for("orders", partition, 4))
            .collect();
        assert!(used.len() == 4, "64 partitions reached only {used:?}");
    }
}
