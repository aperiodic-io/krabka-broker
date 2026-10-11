//! Construction-time config for `Controller::start`.
//!
//! This root holds `ControllerConfig` itself, the `BootstrapMode` that decides
//! how a freshly-formatted node joins a quorum, and the default values every
//! other part of the module falls back to. The validated scalar policies live
//! in `limits`, and the router seams the broker installs on a controller live
//! in `routing`.

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use krabka_kraft_core::snapshot_fetch::METADATA_SNAPSHOT_FETCH_HARD_MAX;
use krabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, Time, hours, mebibytes, millis, secs},
};
use uuid::Uuid;

use crate::{RaftListenerHandshake, network::OutboundDialer, types::NodeId};

mod kafka_release;
mod limits;
mod metadata_log;
mod routing;

pub use self::{
    kafka_release::{KAFKA_4_3_1_APIS, ReleasedApi, kafka_4_3_1_api, kafka_4_3_1_max},
    limits::{ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax},
    metadata_log::{
        DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL, DEFAULT_METADATA_LOG_SEGMENT_SIZE,
        DEFAULT_METADATA_MAX_IDLE_INTERVAL, DEFAULT_METADATA_MAX_RETENTION,
        DEFAULT_METADATA_MAX_RETENTION_SIZE, METADATA_PARTITION_DIR, MIN_METADATA_LOG_SEGMENT_SIZE,
        MetadataLogConfig, metadata_partition_dir,
    },
    routing::{
        ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
        ControllerAdminRouter, ControllerApiVersion, LATEST_PRODUCTION_METADATA_VERSION,
        RaftShardRouter, ShardRouteFuture, UnstableApiVersions, UnstableFeatureVersions,
        supported_feature_range, supported_feature_ranges,
    },
};

/// `metadata.log.max.record.bytes.between.snapshots` default: 20 MiB.
const DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS: ByteSize = mebibytes(20);

/// `metadata.log.max.snapshot.interval.ms` default: one hour.
const DEFAULT_MAX_SNAPSHOT_INTERVAL: Time = hours(1);

/// Election timeout used by [`ControllerConfig::for_tests`].
const TEST_ELECTION_TIMEOUT: Time = secs(1);

/// Leader heartbeat cadence used by [`ControllerConfig::for_tests`].
const TEST_HEARTBEAT_INTERVAL: Time = millis(200);

pub const DEFAULT_CONTROLLER_FETCH_MISS_LIMIT: u32 = 3;
pub const DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY: usize = 256;
pub const DEFAULT_METADATA_RAFT_FETCH_MAX: ByteSize = mebibytes(8);

/// Bootstrap orchestration for a freshly-formatted controller node.
///
/// The engine runs KIP-996 pre-vote, so cold-booting voters that race each
/// other do not disrupt an established epoch. This enum decides something the
/// election rules cannot: where a freshly-formatted node gets its first voter
/// set from. `Controller::start` validates the mode against the on-disk
/// metadata log and rejects a mismatch.
///
/// 1. One broker boots with `Bootstrap` — its configured
///    [`ControllerConfig::initial_voters`] seed the quorum.
/// 2. Remaining brokers boot with `Join` — they start empty, discover the
///    leader through [`ControllerConfig::bootstrap_servers`], and add
///    themselves once caught up.
/// 3. After the initial format, restarted brokers use `Rejoin` — their
///    on-disk metadata log, checkpoint, and quorum-state file already carry
///    the membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Cold-boot the first voter of a fresh cluster. `Controller::start`
    /// requires an empty metadata log and a non-empty
    /// [`ControllerConfig::initial_voters`], which becomes the seed voter set
    /// that elects this broker on its first election timeout. A node with no
    /// initial voters, neither configured nor in its bootstrap checkpoint,
    /// that names [`ControllerConfig::bootstrap_servers`] or sets
    /// [`ControllerConfig::auto_join`] is started as `Join` instead.
    Bootstrap,

    /// Cold-boot a subsequent voter with an empty start. `Controller::start`
    /// requires an empty metadata log. The node runs as an observer, fetches
    /// from a peer in [`ControllerConfig::bootstrap_servers`] to find the
    /// leader, then auto-joins (issuing `AddVoter` for itself once caught up)
    /// when [`ControllerConfig::auto_join`] is set.
    Join,

    /// Restart a previously-formatted broker. `Controller::start` requires a
    /// non-empty metadata log and recovers the voter set, the epoch, and the
    /// committed metadata from that log, the latest checkpoint, and the
    /// quorum-state file.
    Rejoin,
}

/// The limits Kafka's `SocketServer` puts on every listener, including the
/// controller's: `ControllerServer` builds its own `SocketServer`, and that
/// hands `socket.request.max.bytes` and `connections.max.idle.ms` to each
/// `Processor` and creates a `ConnectionQuotas` for `max.connections` and
/// `max.connections.per.ip`.
///
/// The defaults are Kafka's: 100 MiB requests, ten idle minutes, and no
/// connection ceiling.
#[derive(Clone, Copy, Debug, PartialEq, krabka_macros::FieldDefaults)]
pub struct ListenerLimits {
    /// `socket.request.max.bytes`: the largest request frame the listener
    /// reads. A larger size prefix closes the connection before the frame is
    /// read, as `NetworkReceive.readFrom` does.
    #[default(mebibytes(100))]
    pub max_request_size: ByteSize,
    /// `connections.max.idle.ms`: how long the listener waits for the next
    /// request frame before it closes the connection. `None` expires none.
    #[default(Some(Duration::from_mins(10)))]
    pub max_idle: Option<Duration>,
    /// `max.connections`: live connections the listener accepts, `usize::MAX`
    /// for no ceiling.
    #[default(usize::MAX)]
    pub max_connections: usize,
    /// `max.connections.per.ip`: live connections per peer address,
    /// `usize::MAX` for no ceiling.
    #[default(usize::MAX)]
    pub max_connections_per_ip: usize,
}

// Quantities render in the operator form (`1s`, `20MiB`) rather than `uom`'s
// dimension-annotated `Debug`, which is unreadable in a log.
#[derive(Clone, derive_more::Debug)]
pub struct ControllerConfig {
    /// Capacity used by outbound controller client connections.
    pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    /// Maximum frame size used by outbound controller client connections.
    pub client_frame_max: krabka_client_core::ClientFrameMax,
    #[debug("{:?}", node_id.0)]
    pub node_id: NodeId,
    /// Endpoints used only to discover the leader at cold start (KIP-853 dynamic).
    pub bootstrap_servers: Vec<String>,
    /// This replica's stable directory id (generated at format time).
    pub directory_id: Uuid,
    /// Issue `AddVoter` for self once caught up as an observer.
    pub auto_join: bool,
    /// Max allowed lag (in log entries) for an observer to be promotable.
    pub observer_lag_bound: u64,
    /// Initial voter set for the bootstrapping node only; empty for joiners.
    pub initial_voters: krabka_metadata::VoterSet,
    pub controller_listen_addr: SocketAddr,
    /// The metadata log directory, Kafka's `metadata.log.dir`. The controller
    /// keeps the metadata partition in its
    /// [`METADATA_PARTITION_DIR`] subdirectory.
    pub log_dir: PathBuf,
    #[debug("{:?}", election_timeout.human().to_string())]
    pub election_timeout: Time,
    /// Explicit heartbeat cadence. `None` preserves the derived
    /// `election_timeout / 3` behavior.
    #[debug("{:?}", heartbeat_interval.map(|value| value.human().to_string()))]
    pub heartbeat_interval: Option<Time>,
    pub controller_fetch_miss_limit: ControllerFetchMissLimit,
    pub metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    pub metadata_raft_fetch_max: MetadataRaftFetchMax,
    pub client_id: String,
    pub bootstrap_mode: BootstrapMode,
    /// The bootstrap metadata of a static quorum: Kafka's
    /// `bootstrap.checkpoint`, which `krabka-format` writes as
    /// `bootstrap.records.bin`.
    ///
    /// The active controller writes these records once, when it activates on
    /// a metadata log that holds no `metadata.version`. A bootstrap checkpoint
    /// at offset 0 and epoch 0 that holds metadata records replaces them, as
    /// Kafka's `QuorumController.handleLoadBootstrap` does. An empty list
    /// writes nothing.
    #[debug("{}", bootstrap_records.len())]
    pub bootstrap_records: Vec<krabka_metadata::MetadataRecord>,
    /// This node's static `min.insync.replicas`: Kafka's
    /// `ConfigurationControlManager.getStaticallyConfiguredMinInsyncReplicas`.
    /// When the bootstrap records finalize `eligible.leader.replicas.version`
    /// above 0, the active controller writes it as the cluster-level
    /// `min.insync.replicas` together with them, as Kafka's
    /// `ActivationRecordsGenerator.recordsForEmptyLog` does. Default: `1`.
    pub default_min_insync_replicas: i32,
    /// Cluster UUID applied to the `MetadataImage` on first construction.
    /// `None` falls back to `Uuid::nil()` (legacy single-node default).
    /// The operator sets this to the `KafkaCluster` UID so every broker
    /// in the same cluster shares one identifier across restarts.
    pub cluster_id: Option<Uuid>,
    /// Optional outbound dialer. `None` means: open a plain TCP socket
    /// to peers (legacy PLAINTEXT-only path). The broker injects an
    /// `InterBrokerClient`-backed dialer here when inter-broker TLS or
    /// SASL is configured.
    #[debug("{}", dialer.is_some())]
    pub dialer: Option<Arc<dyn OutboundDialer>>,
    /// Optional inbound handshake hook. `None` keeps the legacy
    /// PLAINTEXT path. The broker injects a `BrokerRaftHandshake`
    /// implementation here when the controller listener should
    /// terminate TLS and/or SASL before raft frames start flowing.
    #[debug("{}", handshake.is_some())]
    pub handshake: Option<Arc<dyn RaftListenerHandshake>>,
    /// Optional KIP-595 shard router. Metadata traffic returns `None`; diskless
    /// WAL shards return an encoded response body and bypass metadata dispatch.
    #[debug("{}", shard_router.is_some())]
    pub shard_router: Option<Arc<dyn RaftShardRouter>>,
    /// Optional KIP-919 Admin router. The broker injects its existing handler
    /// registry here after construction, keeping controller and broker
    /// semantics on one implementation.
    #[debug("{}", admin_router.is_some())]
    pub admin_router: Option<Arc<dyn ControllerAdminRouter>>,
    /// Kafka's internal `unstable.api.versions.enable`: whether the controller
    /// listener advertises and accepts a `latestVersionUnstable` version, and
    /// whether it advertises feature levels past the latest production ones.
    /// Kafka builds the listener's `SimpleApiVersionManager` with this flag
    /// alone.
    pub unstable_api_versions: UnstableApiVersions,
    /// Kafka's internal `unstable.feature.versions.enable`: the flag the
    /// controller's own `QuorumFeatures` use, so it caps the feature levels this
    /// controller accepts on replay and refuses to start above.
    pub unstable_feature_versions: UnstableFeatureVersions,
    /// The request-size, idle and connection limits of the controller
    /// listener.
    pub listener_limits: ListenerLimits,
    /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
    #[debug("{:?}", max_bytes_between_snapshots.human().to_string())]
    pub max_bytes_between_snapshots: ByteSize,
    /// `metadata.log.max.snapshot.interval.ms` (default 1 h; 0 = disabled).
    #[debug("{:?}", max_snapshot_interval.human().to_string())]
    pub max_snapshot_interval: Time,
    /// Snapshot once committed offset advances this many records past the last
    /// snapshot. The cleaning by [`Self::metadata_log`]'s retention limits
    /// then decides when the log below a snapshot goes. `0` disables this
    /// trigger.
    pub snapshot_interval_records: u64,
    /// Maximum metadata snapshot size this follower will fetch. Deployments may
    /// lower the default 1 GiB security ceiling but cannot raise it.
    #[debug("{:?}", metadata_snapshot_fetch_max.human().to_string())]
    pub metadata_snapshot_fetch_max: ByteSize,
    /// How the metadata log rolls, how long it keeps the prefix a snapshot
    /// covers, and how often an idle leader appends to it.
    pub metadata_log: MetadataLogConfig,
}

impl ControllerConfig {
    /// # Panics
    /// Panics only if the static loopback test address is invalid.
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        let listen: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let directory_id = Uuid::from_u128(u128::from(node_id.0));
        Self {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            node_id,
            bootstrap_servers: vec![],
            directory_id,
            auto_join: false,
            observer_lag_bound: 1000,
            initial_voters: krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
                id: node_id,
                directory_id,
                endpoints: vec![krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: listen.ip().to_string(),
                    port: listen.port(),
                }],
                kraft_version: krabka_metadata::KRaftVersionRange::default(),
            }]),
            controller_listen_addr: listen,
            log_dir,
            election_timeout: TEST_ELECTION_TIMEOUT,
            heartbeat_interval: Some(TEST_HEARTBEAT_INTERVAL),
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            client_id: "krabka-controller-test".into(),
            bootstrap_mode: BootstrapMode::Bootstrap,
            // A test counts the offsets its own writes land at, so the leader
            // writes no bootstrap records ahead of them.
            bootstrap_records: Vec::new(),
            default_min_insync_replicas: 1,
            cluster_id: None,
            dialer: None,
            handshake: None,
            shard_router: None,
            admin_router: None,
            unstable_api_versions: UnstableApiVersions::Disabled,
            unstable_feature_versions: UnstableFeatureVersions::Disabled,
            listener_limits: ListenerLimits::default(),
            max_bytes_between_snapshots: DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS,
            max_snapshot_interval: DEFAULT_MAX_SNAPSHOT_INTERVAL,
            snapshot_interval_records: 0,
            metadata_snapshot_fetch_max: METADATA_SNAPSHOT_FETCH_HARD_MAX,
            // A test counts the offsets its own writes land at, so the leader
            // appends no KIP-835 `NoOpRecord` between them.
            metadata_log: MetadataLogConfig {
                max_idle_interval: secs(0),
                ..MetadataLogConfig::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{ByteSizeExt as _, TimeExt as _};

    use super::*;

    #[test]
    fn for_tests_uses_expected_snapshot_defaults() {
        let cfg = ControllerConfig::for_tests(NodeId(7), PathBuf::from("/tmp/raft-test"));

        check!(
            (
                cfg.max_bytes_between_snapshots,
                cfg.max_snapshot_interval,
                cfg.snapshot_interval_records,
                cfg.metadata_snapshot_fetch_max,
            ) == (mebibytes(20), hours(1), 0, METADATA_SNAPSHOT_FETCH_HARD_MAX,)
        );
        // The quantities must carry the magnitudes the Kafka configs name, not
        // just compare equal to the constants they were built from.
        check!(cfg.max_bytes_between_snapshots.bytes_u64() == 20 * 1024 * 1024);
        check!(cfg.max_snapshot_interval.millis_i64() == 3_600_000);
        check!(cfg.election_timeout.millis_i64() == 1_000);
        check!(
            cfg.heartbeat_interval
                .expect("test heartbeat is explicit")
                .millis_i64()
                == 200
        );
    }

    #[test]
    fn debug_reports_configuration_fields_and_optional_hooks() {
        let cfg = ControllerConfig::for_tests(NodeId(7), PathBuf::from("/tmp/raft-test"));
        let rendered = format!("{cfg:?}");

        for needle in [
            "ControllerConfig",
            "node_id: 7",
            "client_id: \"krabka-controller-test\"",
            "dialer: false",
            "handshake: false",
            // Quantities render in the operator form, so 20 MiB reads as `20MiB`
            // rather than as a bare byte count.
            "max_bytes_between_snapshots: \"20MiB\"",
            "election_timeout: \"1s\"",
            "max_snapshot_interval: \"1h\"",
            "metadata_snapshot_fetch_max: \"1GiB\"",
        ] {
            assert2::assert!(rendered.contains(needle));
        }
    }
}
