//! Metadata Raft quorum for Krabka.
//!
//! `krabka-raft` runs a hand-rolled KIP-595 `KRaft` consensus engine, the
//! [`kraft::KraftController`], over Krabka's storage ([`krabka_log`]) and
//! transport ([`krabka_client_core`]). The public entry point is
//! [`Controller::start`]. It spawns the engine and opens a TCP listener. That
//! listener serves the real KIP-595 RPCs (Fetch=1, Vote=52,
//! BeginQuorumEpoch=53, EndQuorumEpoch=54) and the Krabka-private observer and
//! forward RPCs. [`Controller::start`] returns a [`ControllerHandle`], which
//! submits metadata changes and reads the current
//! [`krabka_metadata::MetadataImage`].
//!
//! ## Quick start
//!
//! ```no_run
//! use {
//!     krabka_metadata::{MetadataRecord, TopicRecord},
//!     krabka_raft::{Controller, ControllerConfig, NodeId},
//!     std::time::Duration,
//!     uuid::Uuid,
//! };
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
//! let controller = Controller::start(cfg).await?;
//!
//! controller
//!     .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
//!         name: "my-topic".into(),
//!         topic_id: Uuid::new_v4(),
//!         partitions: 3,
//!         replication_factor: 1,
//!     })])
//!     .await?;
//!
//! assert2::assert!(controller.current_image().topic("my-topic").is_some());
//! controller.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! The controller persists and recovers `KRaft` metadata records, and it serves
//! and installs KIP-630 snapshots through `FetchSnapshot`. It also publishes
//! the current metadata image to broker tasks and exposes Krabka-private submit
//! and fetch RPCs for broker and observer integration.
//!
//! KIP-853-style observer bootstrap and auto-join are wired through the broker
//! and controller configuration. [`ControllerHandle::add_learner`] stages a
//! node identity; [`ControllerHandle::change_membership`] reconciles a
//! one-voter delta into `add_voter` or `remove_voter`.
//! Mixed JVM and Krabka controller quorums are outside this crate's
//! compatibility target.

#![doc(html_root_url = "https://docs.rs/krabka-raft/1.0.3")]

mod config;
mod connection_limiter;
mod controller;
mod error;
pub mod handshake;
pub mod kraft;
mod network;
pub mod reconfig;
pub mod voter_requests;
mod voter_wire;
/// The deterministic `KRaft` failure-scenario simulator with trace recording,
/// re-exported from the leaf [`krabka_kraft_core::sim`] module. `krabka-docgen`
/// runs [`scenarios::scenarios`] in-process to render the failure-scenario
/// slideshow.
#[cfg(feature = "scenarios")]
pub use krabka_kraft_core::sim as scenarios;
mod server;
mod snapshot;
#[cfg(test)]
mod test_support;
mod types;
mod wire;

pub use {
    config::{
        BootstrapMode, ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
        ControllerAdminRouter, ControllerApiVersion, ControllerConfig, ControllerFetchMissLimit,
        DEFAULT_METADATA_LOG_SEGMENT_ROLL_INTERVAL, DEFAULT_METADATA_LOG_SEGMENT_SIZE,
        DEFAULT_METADATA_MAX_IDLE_INTERVAL, DEFAULT_METADATA_MAX_RETENTION,
        DEFAULT_METADATA_MAX_RETENTION_SIZE, KAFKA_4_3_1_APIS, LATEST_PRODUCTION_METADATA_VERSION,
        ListenerLimits, METADATA_PARTITION_DIR, MIN_METADATA_LOG_SEGMENT_SIZE, MetadataLogConfig,
        MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax, RaftShardRouter, ReleasedApi,
        ShardRouteFuture, UnstableApiVersions, UnstableFeatureVersions, kafka_4_3_1_api,
        kafka_4_3_1_max, metadata_partition_dir, supported_feature_range, supported_feature_ranges,
    },
    connection_limiter::{ConnectionGuard, ConnectionLimit, ConnectionLimiter},
    controller::{
        Controller, ControllerHandle, QuorumState, QuorumStateSnapshot, SnapshotRange,
        SnapshotSlice, metadata_log_nonempty,
    },
    error::{MetadataReplayError, PersistedFormatError, RaftError},
    handshake::{
        AllowAllGrants, ClusterGrants, ClusterOperation, ControllerApiVersions, RaftConnection,
        RaftHandshakeError, RaftListenerHandshake,
    },
    kraft::{
        MetadataFetchSlice,
        controller::{control_batch_image_records, decode_committed_value, is_kip835_noop},
    },
    network::{OutboundDialer, PlaintextDialer},
    reconfig::{AddVoter, ReconfigOutcome, RemoveVoter, UpdateVoter},
    server::{
        api_versions_max_version, describe_quorum::describe_quorum, finalized_feature_keys,
        is_valid_api_versions_request, is_valid_client_info, supported_feature_key,
        supported_feature_keys, unsupported_version_response,
    },
    types::{
        AppData, AppDataResponse, DelegationTokenMutation, Node, NodeId, OffsetReservation,
        SubmitChangeResult,
    },
};

/// Serialize a Kafka metadata snapshot, including KIP-853 control state.
///
/// # Errors
/// Returns an error if a metadata or control record cannot be encoded.
pub fn serialize_metadata_snapshot(
    image: &krabka_metadata::MetadataImage,
    last_contained_log_timestamp: i64,
) -> Result<bytes::Bytes, RaftError> {
    snapshot::SnapshotWriter::serialize(image, last_contained_log_timestamp)
}

/// Serialize the bootstrap checkpoint of a dynamic format, as Kafka's
/// `Formatter.writeBoostrapSnapshot` writes it: the KIP-853 `kraft.version`
/// and voter set, then the bootstrap records in their own order.
///
/// A controller does not apply these records to its image. The active
/// controller writes them to an empty metadata log, and every replica applies
/// them from there.
///
/// # Errors
/// Returns an error if `kraft_version` exceeds `int16`, or if a voter or a
/// bootstrap record cannot be encoded.
pub fn serialize_bootstrap_snapshot(
    kraft_version: u16,
    voters: &krabka_metadata::VoterSet,
    records: &[krabka_metadata::MetadataRecord],
    last_contained_log_timestamp: i64,
) -> Result<bytes::Bytes, RaftError> {
    snapshot::SnapshotWriter::serialize_bootstrap(
        &snapshot::SnapshotControlState {
            kraft_version,
            voters: voters.clone(),
        },
        records,
        last_contained_log_timestamp,
    )
}

/// Decode the KIP-630 metadata records from a Kafka metadata snapshot.
///
/// KIP-853 quorum controls are intentionally omitted: a restore formats a new
/// quorum, while the returned records describe the cluster state it recovers.
///
/// # Errors
/// Returns an error when the snapshot framing, ordering, or a metadata record
/// is invalid.
pub fn deserialize_metadata_snapshot(
    bytes: &[u8],
) -> Result<Vec<krabka_metadata::MetadataRecord>, RaftError> {
    Ok(snapshot::SnapshotReader::read(bytes)?.metadata_records)
}

/// Decode the whole image a Kafka metadata snapshot holds: its KIP-853 quorum
/// controls, as a `V1KRaftVersion` and a `V1Voters` record, ahead of its
/// KIP-630 metadata records.
///
/// A broker-only observer installs a snapshot through this, so its image names
/// the voters, and the endpoints that reach them, as a controller's image does.
///
/// # Errors
/// Returns an error when the snapshot framing, ordering, a control record, or a
/// metadata record is invalid.
pub fn deserialize_metadata_snapshot_image(
    bytes: &[u8],
) -> Result<Vec<krabka_metadata::MetadataRecord>, RaftError> {
    let snapshot = snapshot::SnapshotReader::read(bytes)?;
    let controls = snapshot.control_state.into_iter().flat_map(|controls| {
        [
            krabka_metadata::MetadataRecord::V1KRaftVersion(krabka_metadata::KRaftVersionRecord {
                kraft_version: controls.kraft_version,
            }),
            krabka_metadata::MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
                voters: controls.voters,
            }),
        ]
    });
    Ok(controls.chain(snapshot.metadata_records).collect())
}

/// Read the append timestamp of the last record batch contained in a KIP-630
/// metadata snapshot.
///
/// # Errors
/// Returns an error when the snapshot header is missing or malformed.
pub fn metadata_snapshot_last_contained_log_timestamp(bytes: &[u8]) -> Result<i64, RaftError> {
    snapshot::SnapshotReader::last_contained_log_timestamp(bytes)
}
pub use wire::{
    API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
    KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest,
    KrabkaSubmitChangeResponse, PRIVATE_BASELINE_VERSION, PRIVATE_CLUSTER_AUTHORIZATION_FAILED,
    PRIVATE_UNSUPPORTED_VERSION, SUBMIT_CHANGE_UNCOMMITTED_TAIL, private_api_highest_version,
    private_request_version,
};
