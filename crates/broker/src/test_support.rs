//! Shared in-crate scaffolding for the per-handler `#[cfg(test)] mod tests`
//! modules.
//!
//! The mutant-hardening pass (#713) copied the same helper set into ~40
//! handler test modules: a deny-everything authorizer, principal, peer, and
//! request-context builders, wire codec helpers, and a temp-dir broker
//! launcher. This module holds one copy of each. Handlers keep only thin,
//! behaviour-specific facades over them. Their own principal name, client id,
//! `BrokerConfig` tweaks, and negotiated wire version live at the call site, so
//! this module centralises no behaviour that a handler needs to vary.
//!
//! It also holds [`FakeMetadataSource`], the one metadata authority every
//! suite in this crate fakes with.
//!
//! [`BrokenTimer`] is the one fixture here that is not handler scaffolding.
//! Every broker cadence loop takes its ticker through an injectable field, and
//! every one of them stops when that ticker gives out, so the fake that makes a
//! ticker give out is shared rather than copied into each of their test
//! modules.
//!
//! `LogCapture` records what one piece of work logs, for the tests that check
//! the level of a log line.

use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use krabka_metadata::{
    BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
    PartitionRecord, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_partition_request::{
            AlterPartitionRequest, PartitionData as AlterPartitionPartition,
            TopicData as AlterPartitionTopic,
        },
        alter_partition_response::AlterPartitionResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, Record, RecordBatch},
};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    SubmitChangeResult, UpdateVoter,
};
use krabka_security::{AuthMethod, Principal};
use qubit_clock::{
    MonotonicClock, MonotonicInstant, StdMonotonicClock, TimeError, Timer, TimerFuture,
    TimerUnavailableError,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
};

use crate::{
    api_catalog::UnstableApiVersions,
    authorizer::{
        AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    },
    bootstrap::encode_bootstrap_records,
    broker::{Broker, BrokerHandle, spawn_partition},
    codes::{
        COORDINATOR_NOT_AVAILABLE, KAFKA_STORAGE_ERROR, MESSAGE_TOO_LARGE, NONE, NOT_COORDINATOR,
        NOT_ENOUGH_REPLICAS, NOT_LEADER_OR_FOLLOWER, RECORD_LIST_TOO_LARGE, REQUEST_TIMED_OUT,
        UNKNOWN_SERVER_ERROR,
    },
    config::BrokerConfig,
    config_keys::{BROKER_WITNESS, MIN_INSYNC_REPLICAS, WITNESS_TRUE},
    controller_endpoint::ControllerDialer,
    elr::state::PartitionElr,
    error::BrokerError,
    features::ELR_VERSION,
    handlers::{
        DispatchKind, RequestContext, acl_wire::CLUSTER_RESOURCE_NAME, alter_partition::handle,
        describe_configs::effective_topic_configs, encode_response,
    },
    heartbeat::{
        controller_state::{ControllerLivenessState, LivenessTransition, TestClock},
        fencing::{RegistrationChange, registration_change},
    },
    log_dir::partition_dir,
    log_dir_status::LogDirRegistry,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    partition::Partition,
    partition_registry::PartitionRegistry,
    producer_state::ProducerState,
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
    txn::{bootstrap::TOPIC, coordinator::leadership::LoadStatus},
};

/// The localhost ECDSA pair used by reload and socket-drain tests.
pub(crate) fn localhost_ecdsa_pair() -> (rcgen::Certificate, rcgen::KeyPair) {
    ecdsa_pair_for_host("localhost")
}

/// A fresh self-signed ECDSA fixture with the caller's exact host SAN.
pub(crate) fn ecdsa_pair_for_host(host: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
    let params = rcgen::CertificateParams::new(vec![host.to_string()]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = params.self_signed(&key).unwrap();
    (cert, key)
}

/// Advertise every finalized feature from zero through its finalized level.
pub(crate) fn finalized_broker_features(
    image: &krabka_metadata::MetadataImage,
) -> Vec<krabka_protocol::owned::broker_registration_request::Feature> {
    image
        .finalized_features()
        .iter()
        .map(
            |(name, level)| krabka_protocol::owned::broker_registration_request::Feature {
                name: name.clone(),
                min_supported_version: 0,
                max_supported_version: *level,
                ..Default::default()
            },
        )
        .collect()
}

/// Generate a real Ed25519 pair for signing and trust-set fixtures.
pub(crate) fn fresh_ed25519_key() -> ring::signature::Ed25519KeyPair {
    use ring::signature::Ed25519KeyPair;
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8")
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct OperatorKeyFileSetup<'a> {
    #[default("alice.pub")]
    pub name: &'a str,
    #[default("write public key")]
    pub context: &'a str,
}

/// Stage an operator public key while returning the private signing pair to its owner.
pub(crate) fn ed25519_public_key_file(
    dir: &std::path::Path,
    setup: OperatorKeyFileSetup<'_>,
) -> (ring::signature::Ed25519KeyPair, std::path::PathBuf) {
    use ring::signature::KeyPair as _;
    let pair = fresh_ed25519_key();
    let path = dir.join(setup.name);
    std::fs::write(&path, pair.public_key().as_ref()).expect(setup.context);
    (pair, path)
}

/// Binds an HTTPS fixture with a fresh loopback certificate and returns the
/// trust bundle path. The directory stays alive for background client requests.
pub(crate) async fn loopback_tls_listener()
-> (tokio::net::TcpListener, TlsAcceptor, std::path::PathBuf) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (cert, key) = ecdsa_pair_for_host("127.0.0.1");
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let cert_path = dir.path().join("cert.pem");
    std::fs::write(&cert_path, cert.pem()).unwrap();
    let key_path = dir.path().join("key.pem");
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let priv_key = PrivateKeyDer::from_pem_file(&key_path).unwrap();
    let server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, priv_key)
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    (listener, TlsAcceptor::from(Arc::new(server_cfg)), cert_path)
}

/// Serve concurrent TLS connections with the caller's protocol handler until shutdown.
pub(crate) async fn serve_loopback_tls<F, Fut>(
    handler: F,
) -> (
    std::net::SocketAddr,
    tokio_util::sync::CancellationToken,
    std::path::PathBuf,
)
where
    F: Fn(tokio_rustls::server::TlsStream<tokio::net::TcpStream>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (listener, acceptor, cert_path) = loopback_tls_listener().await;
    let addr = listener.local_addr().unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let srv_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = srv_shutdown.cancelled() => break,
                Ok((sock, _peer)) = listener.accept() => {
                    let acceptor = acceptor.clone();
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let Ok(tls) = acceptor.accept(sock).await else { return };
                        handler(tls).await;
                    });
                }
            }
        }
    });
    (addr, shutdown, cert_path)
}

/// Own the temporary directory alongside its replica until the caller drops both.
pub(crate) fn temporary_partition(
    setup: StandalonePartitionSetup<'_>,
) -> (tempfile::TempDir, Arc<Partition>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let partition = open_partition(directory.path(), setup);
    (directory, partition)
}

/// Opens a local replica with a running writer in the supplied log directory.
pub(crate) fn open_partition(
    log_dir: &std::path::Path,
    setup: StandalonePartitionSetup<'_>,
) -> Arc<Partition> {
    let path = partition_dir(log_dir, setup.topic, setup.partition.0);
    std::fs::create_dir_all(&path).expect("create partition directory");
    let log = krabka_log::Log::open(&path, krabka_log::LogConfig::default()).expect("open log");
    spawn_standalone_partition(log_dir, log, setup)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum StorageMode {
    #[default]
    Local,
    Diskless,
}

impl StorageMode {
    pub(crate) fn from_wire(diskless: bool) -> Self {
        if diskless {
            Self::Diskless
        } else {
            Self::Local
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum FreezeState {
    #[default]
    Frozen,
    Thawed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub(crate) struct PartitionCount(pub i32);

impl Default for PartitionCount {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub(crate) struct ReplicationFactor(pub i16);

impl Default for ReplicationFactor {
    fn default() -> Self {
        Self(3)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub(crate) struct PartitionEpoch(pub i32);

/// An expected Kafka response code, including deliberately invalid fixture values.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(crate) struct KafkaErrorCode(pub i16);

/// A wall-clock coordinate, distinct from a record offset or a duration.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(crate) struct UnixMillis(pub i64);

/// Start a standalone writer over the caller's already-opened log.
#[derive(Clone, Copy)]
pub(crate) struct StandalonePartitionSetup<'a> {
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    pub storage: StorageMode,
}

impl Default for StandalonePartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            partition: krabka_ids::PartitionIndex::default(),
            storage: StorageMode::Local,
        }
    }
}

pub(crate) fn spawn_standalone_partition(
    log_dir: &std::path::Path,
    log: krabka_log::Log,
    setup: StandalonePartitionSetup<'_>,
) -> Arc<Partition> {
    let StandalonePartitionSetup {
        topic,
        partition,
        storage,
    } = setup;
    spawn_partition(
        topic.to_owned(),
        partition,
        log_dir.to_path_buf(),
        log,
        LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        storage == StorageMode::Diskless,
    )
}

/// Block an artifact rewrite or deletion using a real filesystem directory collision.
pub(crate) fn block_log_artifact_paths(
    root: &std::path::Path,
    topic: &str,
    extension: &str,
    expectation: &str,
) -> Vec<std::path::PathBuf> {
    let directory = partition_dir(root, topic, 0);
    let mut blocked = Vec::new();
    for entry in std::fs::read_dir(&directory).expect("read partition dir") {
        let path = entry.expect("partition dir entry").path();
        if path.extension().is_some_and(|extension| extension == "log") {
            let artifact = path.with_extension(extension);
            std::fs::create_dir(&artifact).expect(expectation);
            blocked.push(artifact);
        }
    }
    assert2::assert!(
        !blocked.is_empty(),
        "the fixture sealed no segment to block"
    );
    blocked
}

krabka_macros::bound_start_fixture!(config, bound_controller_config, crate);
krabka_macros::bound_start_fixture!(
    start,
    start_bound_controller,
    crate,
    expect,
    bound_controller_config
);

/// Boot a broker leading a single-voter quorum with separate loopback listeners.
pub(crate) async fn start_controller() -> (BrokerHandle, SocketAddr, tempfile::TempDir) {
    let (controller, controller_addr, dir) = start_bound_controller(|_| {}).await;
    // Bind the handle after the directory so a failed readiness wait drops the broker first.
    let broker = controller;
    broker.wait_until_controller_leader().await;
    (broker, controller_addr, dir)
}

/// Builds non-transactional records with explicit values and a shared timestamp.
pub(crate) fn static_records_batch(
    values: &[&'static [u8]],
    timestamp_ms: UnixMillis,
) -> RecordBatch {
    let records = values
        .iter()
        .enumerate()
        .map(|(index, value)| Record {
            attributes: 0,
            offset_delta: i32::try_from(index).expect("record offset fits i32"),
            timestamp_delta: 0,
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: vec![],
        })
        .collect();
    fixture_records_batch(
        records,
        RecordOffsetDelta(i32::try_from(values.len()).expect("record count fits i32") - 1),
        timestamp_ms,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordCount(pub i32);

impl Default for RecordCount {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RepeatedRecordsSetup {
    pub count: RecordCount,
    pub timestamp: UnixMillis,
}

#[derive(Clone, Copy)]
struct RecordOffsetDelta(i32);

/// Partition fixtures append records with a deterministic wall-clock timestamp.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct PartitionRecordsSetup {
    pub count: RecordCount,
    #[default(UnixMillis(1_700_000_000))]
    pub timestamp: UnixMillis,
}

pub(crate) fn partition_records_batch(setup: PartitionRecordsSetup) -> RecordBatch {
    repeated_records_batch(RepeatedRecordsSetup {
        count: setup.count,
        timestamp: setup.timestamp,
    })
}

/// Builds a batch of identical values, preserving the explicit count header.
pub(crate) fn repeated_records_batch(setup: RepeatedRecordsSetup) -> RecordBatch {
    let RepeatedRecordsSetup {
        count,
        timestamp: timestamp_ms,
    } = setup;
    let records = (0..count.0)
        .map(|offset_delta| Record {
            attributes: 0,
            offset_delta,
            timestamp_delta: 0,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: vec![],
        })
        .collect();
    fixture_records_batch(records, RecordOffsetDelta(count.0 - 1), timestamp_ms)
}

/// Supplies the common non-transactional header for fixture records.
fn fixture_records_batch(
    records: Vec<Record>,
    last_offset_delta: RecordOffsetDelta,
    timestamp_ms: UnixMillis,
) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: Attributes::default(),
        last_offset_delta: last_offset_delta.0,
        base_timestamp: timestamp_ms.0,
        max_timestamp: timestamp_ms.0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

/// A mock broker serving the caller's version handshake and one request kind.
pub(crate) async fn mock_request_broker(
    request_key: i16,
    mut response: impl FnMut() -> Vec<u8> + Send + 'static,
    versions: fn() -> Vec<u8>,
) -> krabka_client_core::MockBroker {
    krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
        if api_key == krabka_protocol::owned::api_versions_request::API_KEY {
            return Some(versions());
        }
        if api_key == request_key {
            return Some(response());
        }
        None
    })
    .await
}

/// Owned configuration pairs, preserving the input's duplicate-key policy.
pub(crate) fn string_pairs(values: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// The signed-action placeholder metadata used by retention freeze fixtures.
#[derive(Clone, Copy)]
pub(crate) struct FreezeSetup<'a> {
    pub scope: &'a str,
    pub pattern_type: krabka_metadata::PatternType,
    pub state: FreezeState,
    pub reason: &'a str,
}

impl Default for FreezeSetup<'_> {
    fn default() -> Self {
        Self {
            scope: "orders",
            pattern_type: krabka_metadata::PatternType::Literal,
            state: FreezeState::Frozen,
            reason: "DR cutover",
        }
    }
}

pub(crate) fn topic_freeze_record(setup: FreezeSetup<'_>) -> krabka_metadata::TopicFreezeRecord {
    let FreezeSetup {
        scope,
        pattern_type,
        state,
        reason,
    } = setup;
    krabka_metadata::TopicFreezeRecord {
        scope: scope.to_owned(),
        pattern_type,
        frozen: state == FreezeState::Frozen,
        reason: reason.to_owned(),
        set_by: "User:alice".to_owned(),
        set_at_ms: 1_770_000_000_000,
        proposal_id: uuid::Uuid::nil(),
        key_id: String::new(),
        signature: Vec::new(),
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct FrozenTopicsImageSetup {
    #[default(uuid::Uuid::from_u128(0x5150))]
    pub cluster: uuid::Uuid,
}

/// One live DR-cutover freeze per scope, in the requested cluster image.
pub(crate) fn frozen_topics_image(
    scopes: &[(&str, krabka_metadata::PatternType)],
    setup: FrozenTopicsImageSetup,
) -> MetadataImage {
    let mut image = MetadataImage::new(setup.cluster);
    for &(scope, pattern_type) in scopes {
        image.apply(&krabka_metadata::MetadataRecord::V1TopicFreeze(
            topic_freeze_record(FreezeSetup {
                scope,
                pattern_type,
                ..Default::default()
            }),
        ));
    }
    image
}

pub(crate) fn topic_thaw_record(
    scope: &str,
    pattern_type: krabka_metadata::PatternType,
) -> krabka_metadata::TopicFreezeRecord {
    krabka_metadata::TopicFreezeRecord {
        set_by: "User:bob".to_owned(),
        set_at_ms: 1_770_000_100_000,
        proposal_id: uuid::Uuid::from_u128(7),
        ..topic_freeze_record(FreezeSetup {
            scope,
            pattern_type,
            state: FreezeState::Thawed,
            reason: "",
        })
    }
}

/// A topic-only image for policy tests, before any partitions are installed.
#[derive(Clone, Copy)]
pub(crate) struct TopicSetup<'a> {
    pub topic: &'a str,
    pub topic_id: uuid::Uuid,
    pub partitions: PartitionCount,
    pub replication_factor: ReplicationFactor,
}

impl Default for TopicSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            topic_id: uuid::Uuid::from_u128(1),
            partitions: PartitionCount::default(),
            replication_factor: ReplicationFactor::default(),
        }
    }
}

impl TopicSetup<'_> {
    /// Topic metadata for fixtures whose partition records have one replica.
    pub(crate) fn single_replica() -> Self {
        Self {
            replication_factor: ReplicationFactor(1),
            ..Self::default()
        }
    }
}

pub(crate) fn topic_image(setup: TopicSetup<'_>) -> MetadataImage {
    let TopicSetup {
        topic,
        topic_id,
        partitions,
        replication_factor,
    } = setup;
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id,
        partitions: partitions.0,
        replication_factor: replication_factor.0,
    }));
    image
}

/// Apply one topic before constructing its partition fixture.
pub(crate) fn topic_partition_image(
    topic: &str,
    replication_factor: i16,
    partition: impl FnOnce() -> PartitionRecord,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor,
    }));
    image.apply(&MetadataRecord::V1Partition(partition()));
    image
}

krabka_macros::topic_record_fixture!(single_partition_topic);

/// Seeds the one-partition reassignment fixtures with the fixed leader epoch.
#[derive(Clone)]
pub(crate) struct ReassignmentSetup<'a> {
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    pub adding: Vec<NodeId>,
    pub removing: Vec<NodeId>,
    pub leader: NodeId,
    pub directories: &'a [uuid::Uuid],
}

impl Default for ReassignmentSetup<'_> {
    fn default() -> Self {
        Self {
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            adding: vec![],
            removing: vec![],
            leader: NodeId(1),
            directories: &[],
        }
    }
}

impl ReassignmentSetup<'_> {
    /// An in-flight replacement with the new replica already in the ISR.
    pub(crate) fn replacing_replica(removed: NodeId, added: NodeId) -> Self {
        let mut replicas = Self::default().replicas;
        replicas.push(added);
        Self {
            isr: replicas.clone(),
            replicas,
            adding: vec![added],
            removing: vec![removed],
            ..Default::default()
        }
    }
}

pub(crate) fn replica_nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

pub(crate) fn reassignment_partition(setup: ReassignmentSetup<'_>) -> PartitionRecord {
    let ReassignmentSetup {
        replicas,
        isr,
        adding,
        removing,
        leader,
        directories,
    } = setup;
    PartitionRecord {
        topic: "foo".into(),
        partition: 0,
        leader,
        replicas,
        isr,
        leader_epoch: krabka_metadata::LeaderEpoch(5),
        adding_replicas: adding,
        removing_replicas: removing,
        directories: directories.to_vec(),
        partition_epoch: 0,
    }
}

/// Metadata defaults used by both ELR models, without finalizing a feature version.
pub(crate) fn elr_model_image(
    topic: &str,
    replication_factor: i16,
    min_isr: usize,
) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.to_string(),
        topic_id: uuid::Uuid::from_u128(1),
        partitions: 1,
        replication_factor,
    }));
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.to_string(),
        overrides: [(MIN_INSYNC_REPLICAS.to_string(), min_isr.to_string())]
            .into_iter()
            .collect(),
    }));
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: MIN_INSYNC_REPLICAS.to_string(),
        config_value: Some(min_isr.to_string()),
    }));
    image
}

/// A single-partition ISR proposal with epochs read explicitly by its caller.
#[derive(Clone, Copy, Default)]
pub(crate) struct WireBrokerId(pub i32);

#[derive(Clone, Copy, Default)]
pub(crate) struct BrokerEpoch(pub i64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct IsrBrokerIdentity {
    #[default(WireBrokerId(1))]
    pub id: WireBrokerId,
    pub epoch: BrokerEpoch,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct IsrPartitionEpochs {
    pub leader: krabka_metadata::LeaderEpoch,
    pub partition: PartitionEpoch,
}

#[derive(Clone, Copy)]
pub(crate) struct IsrProposalSetup<'a> {
    pub topic_id: WireUuid,
    pub broker: IsrBrokerIdentity,
    pub partition: IsrPartitionEpochs,
    pub new_isr: &'a [WireBrokerId],
}

impl Default for IsrProposalSetup<'_> {
    fn default() -> Self {
        Self {
            topic_id: WireUuid::default(),
            broker: IsrBrokerIdentity::default(),
            partition: IsrPartitionEpochs::default(),
            new_isr: &[WireBrokerId(1)],
        }
    }
}

pub(crate) fn alter_partition_request(setup: IsrProposalSetup<'_>) -> AlterPartitionRequest {
    let IsrProposalSetup {
        topic_id,
        broker,
        partition,
        new_isr,
    } = setup;
    AlterPartitionRequest {
        broker_id: broker.id.0,
        broker_epoch: broker.epoch.0,
        topics: vec![AlterPartitionTopic {
            topic_id,
            partitions: vec![AlterPartitionPartition {
                partition_index: 0,
                leader_epoch: partition.leader.0,
                partition_epoch: partition.partition.0,
                new_isr: new_isr.iter().map(|id| id.0).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Identity of the seeded ELR partition; epochs still come from separate live snapshots.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct LiveIsrSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    #[default(WireUuid([9; 16]))]
    pub topic_id: WireUuid,
    #[default(krabka_metadata::LeaderEpoch(7))]
    pub leader_epoch: krabka_metadata::LeaderEpoch,
    /// v2 keeps the plain ISR list, avoiding the v3 broker-epoch eligibility extension.
    #[default(krabka_ids::ApiVersion(2))]
    pub version: krabka_ids::ApiVersion,
    #[default(&[WireBrokerId(1)])]
    pub new_isr: &'a [WireBrokerId],
}

/// Send a real ISR proposal using the caller's wire identity and separate epoch snapshots.
pub(crate) async fn propose_isr(
    broker: &Arc<Broker>,
    setup: LiveIsrSetup<'_>,
) -> AlterPartitionResponse {
    let LiveIsrSetup {
        topic,
        topic_id,
        leader_epoch,
        version,
        new_isr,
    } = setup;
    let principal = principal("replica");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "broker-client");
    // The controller checks the sender's broker epoch and the row's partition
    // epoch. Keep the two image snapshots in the original request-read order.
    let image = broker.controller.current_image();
    let request = alter_partition_request(IsrProposalSetup {
        topic_id,
        broker: IsrBrokerIdentity {
            epoch: BrokerEpoch(image.broker_epoch(NodeId(1)).unwrap_or(-1)),
            ..Default::default()
        },
        partition: IsrPartitionEpochs {
            leader: leader_epoch,
            partition: PartitionEpoch(
                broker
                    .controller
                    .current_image()
                    .partition(topic, 0)
                    .expect("partition")
                    .partition_epoch,
            ),
        },
        new_isr,
    });
    handle(broker, request, version.0, &ctx)
        .await
        .expect("AlterPartition")
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IsrResponseCheck {
    EnvelopeAndPartition,
    PartitionOnly,
}

/// Propose an ISR change and verify its partition result, optionally checking the envelope first.
pub(crate) async fn accepted_isr_proposal(
    broker: &Arc<Broker>,
    setup: LiveIsrSetup<'_>,
    response_check: IsrResponseCheck,
) {
    let response = propose_isr(broker, setup).await;
    if response_check == IsrResponseCheck::EnvelopeAndPartition {
        assert2::assert!(response.error_code == NONE);
    }
    assert2::assert!(
        response.topics[0].partitions[0].error_code == NONE,
        "AlterPartition refused the proposal: {response:?}"
    );
}

/// Authorizer that denies every request. It drives the authorization-failure
/// path in every handler that consults the cluster authorizer.
#[derive(Debug)]
pub(crate) struct DenyAll;

impl Authorizer for DenyAll {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        _req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        AuthorizationResult::Deny
    }
}

/// An authorizer that lets the broker's own controller requests through and
/// asks the wrapped one about everything else.
///
/// A test broker talks to its own controller over a plaintext controller
/// listener, as the `ANONYMOUS` principal. A new registration stays fenced
/// until a heartbeat unfences it, and the heartbeat needs `ClusterAction` on
/// the `Cluster`. The auto topic creation of a coordinator topic sends
/// `CreateTopics` with no client principal, and the controller checks
/// `Create` on the `Cluster` for it. A Kafka operator grants the inter-broker
/// principal `ClusterAction` and `Create`, or makes it a super user, for the
/// same reasons. Tests that exercise a restrictive authorizer on client
/// requests wrap it in this, so the broker still unfences and still creates
/// its coordinator topics.
#[derive(Debug)]
pub(crate) struct ControllerPeerAllowed<A>(pub(crate) A);

// Wrappers customize individual authorization while forwarding metadata and scans.
macro_rules! delegate_authorizer_metadata {
    () => {
        fn is_configured(&self) -> bool {
            self.0.is_configured()
        }

        fn decision_ttl(&self) -> Option<std::time::Duration> {
            self.0.decision_ttl()
        }

        fn authorize_by_resource_type(
            &self,
            source: &dyn crate::authorizer::AclSource,
            principal: &krabka_security::Principal,
            host: &std::net::SocketAddr,
            resource_type: krabka_metadata::ResourceType,
            operation: krabka_metadata::AclOperation,
        ) -> crate::authorizer::AuthorizationResult {
            self.0
                .authorize_by_resource_type(source, principal, host, resource_type, operation)
        }
    };
}

impl<A: Authorizer> Authorizer for ControllerPeerAllowed<A> {
    fn authorize(
        &self,
        source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if request.principal.name == "ANONYMOUS"
            && request.resource_type == krabka_metadata::ResourceType::Cluster
            && matches!(
                request.operation,
                krabka_metadata::AclOperation::ClusterAction
                    | krabka_metadata::AclOperation::Create
            )
        {
            AuthorizationResult::Allow
        } else {
            self.0.authorize(source, request)
        }
    }

    delegate_authorizer_metadata!();
}

/// An authorizer that a fixture holds as a trait object, so that it can go
/// inside [`ControllerPeerAllowed`].
#[derive(Debug)]
struct SharedAuthorizer(std::sync::Arc<dyn Authorizer>);

impl Authorizer for SharedAuthorizer {
    fn authorize(
        &self,
        source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        self.0.authorize(source, request)
    }

    delegate_authorizer_metadata!();
}

/// Wraps `authorizer` in [`ControllerPeerAllowed`].
///
/// A fixture that waits for a coordinator needs this: the broker places a
/// coordinator topic only on an unfenced broker, as Kafka's
/// `ReplicaPlacer` does.
pub(crate) fn controller_peer_allowed(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> std::sync::Arc<dyn Authorizer> {
    std::sync::Arc::new(ControllerPeerAllowed(SharedAuthorizer(authorizer)))
}

/// Finalize `eligible.leader.replicas.version` at 1 in `image`.
///
/// KIP-966 ELR maintenance is gated on the feature, and its release default is
/// 0 at every `metadata.version` krabka supports, so a fixture that wants the
/// controller to publish an ELR has to finalize it the way an operator's
/// `kafka-features upgrade` would. The tests that assert the feature is *off*
/// build their image without calling this.
pub(crate) fn finalize_elr_version(image: &mut MetadataImage) {
    image.apply(&MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: ELR_VERSION.into(),
            level: 1,
        },
    ));
}

/// Finalize `eligible.leader.replicas.version` at 1 on a running broker, and
/// wait until its own image reports the level.
///
/// The image-level [`finalize_elr_version`] cannot serve a test that boots a
/// broker: the level has to arrive as a committed record, the way an
/// operator's `kafka-features upgrade --feature
/// eligible.leader.replicas.version=1` delivers it.
pub(crate) async fn finalize_elr_version_on(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: ELR_VERSION.into(),
                level: 1,
            },
        )])
        .await
        .expect("submit eligible.leader.replicas.version");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if broker
                .controller
                .current_image()
                .finalized_feature(ELR_VERSION)
                == Some(1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("eligible.leader.replicas.version visible");
}

/// The record that finalizes `metadata.version` 33 (`4.4-IV2`), one of trunk's
/// unstable levels. Kafka 4.3 supports levels up to 30, so a controller with
/// the unstable flag off that replays this record stops over a fatal fault.
pub(crate) fn finalize_unstable_metadata_version() -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
        name: "metadata.version".into(),
        level: 33,
    })
}

/// The fault that replaying [`finalize_unstable_metadata_version`] raises.
pub(crate) const UNSUPPORTED_LEVEL_FAULT: &str = "Tried to apply FeatureLevelRecord \
    FeatureLevelRecord(name='metadata.version', featureLevel=33), \
    but this controller only supports versions 7-30";

/// Writes `records` as the length-prefixed frames of `bootstrap.records.bin`,
/// the file `krabka format` leaves for the first start to submit.
pub(crate) fn write_bootstrap_records(log_dir: &std::path::Path, records: &[MetadataRecord]) {
    let bytes = encode_bootstrap_records(records).expect("encode bootstrap records");
    std::fs::write(log_dir.join("bootstrap.records.bin"), bytes).expect("write bootstrap records");
}

/// Initialize the share state of `(group, topic_id, partition)` at state
/// epoch 1 with no start offset, as the group coordinator does when it
/// assigns the partition to a member (Kafka's Initialize-first flow). The
/// share coordinator refuses a read of a key with no state, so a handler test
/// that fetches or acknowledges without a share group heartbeat calls this
/// first.
pub(crate) async fn initialize_share_state(
    broker: &BrokerHandle,
    group: &str,
    topic_id: uuid::Uuid,
    partition: i32,
) {
    broker
        .broker_arc_for_test()
        .group_coordinator
        .share_persister()
        .expect("share persister")
        .initialize(
            group,
            topic_id,
            partition,
            1,
            krabka_log::Offset(UNINITIALIZED_START_OFFSET),
        )
        .await
        .expect("initialize the share state");
}

/// End the heartbeat session of `broker_id` on the controller `broker`, as if
/// that broker stopped and its session expired.
///
/// It first waits for a liveness tick of the current controller term to
/// finish. The first tick of a term seeds every registered broker with a
/// session, so a session ended before that tick would be opened again.
pub(crate) async fn end_heartbeat_session(broker: &Broker, broker_id: u64) {
    let ticks = broker.metrics.controller_fencing_publications_total.get();
    tokio::time::timeout(Duration::from_secs(10), async {
        while broker.metrics.controller_fencing_publications_total.get() <= ticks {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a liveness tick of this controller term finished");
    broker.liveness.end_session(broker_id).await;
}

/// Build an anonymous-auth [`Principal`] with the given name and no groups.
///
/// The name matters. Authorization decisions and audit records key on this
/// subject, so each handler passes the identity its scenario expects, such as
/// `"alice"`, `"admin"`, or `"ANONYMOUS"`.
pub(crate) fn principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

/// Build a SASL/PLAIN-authenticated [`Principal`] with the given name and no
/// groups, as a client that logged in over a `SASL_PLAINTEXT` listener is.
pub(crate) fn sasl_principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::SaslPlain,
        groups: Vec::new(),
    }
}

/// The loopback peer address (`127.0.0.1:9092`) that handler tests attribute
/// requests to.
pub(crate) fn peer() -> SocketAddr {
    "127.0.0.1:9092".parse().unwrap()
}

/// Make this broker the leader of each `__transaction_state` partition in
/// `partitions`, as the controller does when it creates the topic, and wait
/// until the transaction coordinator loads each one.
///
/// The image holds only these partitions of the topic, so this broker leads no
/// other partition of it. The replica reconcile then materializes each
/// partition and installs its leadership, and the coordinator loads it, as in
/// production. A `__transaction_state` write then passes the leadership and
/// ISR checks of its commit rule.
pub(crate) async fn lead_transaction_state_partitions(
    handle: &BrokerHandle,
    partitions: &[krabka_ids::PartitionIndex],
) {
    let broker = handle.broker_arc_for_test();
    let node = broker.config.node_id;
    let topic = TOPIC;
    // The controller translates a partition record against the topic that the
    // image already holds, so the topic goes first.
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: topic.into(),
                topic_id: uuid::Uuid::from_u128(0x7472_616e_7361_6374_696f_6e73),
                partitions: 0,
                replication_factor: 1,
            },
        )])
        .await
        .expect("create __transaction_state");
    let led: BTreeSet<i32> = partitions.iter().map(|partition| partition.get()).collect();
    broker
        .controller
        .submit_change(
            led.iter()
                .map(|&partition| {
                    MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                        topic: topic.into(),
                        partition,
                        leader: node,
                        replicas: vec![node],
                        isr: vec![node],
                        ..Default::default()
                    })
                })
                .collect(),
        )
        .await
        .expect("lead the __transaction_state partitions");
    let loaded = tokio::time::timeout(Duration::from_secs(30), async {
        for &partition in &led {
            while broker
                .txn_coordinator
                .load_status(krabka_ids::PartitionIndex(partition))
                .await
                != Some(LoadStatus::Loaded)
            {
                // intentional: a load has no awaiter that a test can reach.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await;
    assert2::assert!(
        loaded.is_ok(),
        "the transaction coordinator did not load {led:?}"
    );
}

/// The registration of an unfenced broker `node_id` at `127.0.0.1:9092`, at
/// broker epoch 0, with a nil incarnation id and no rack, log directories,
/// endpoints, or supported features.
///
/// A test spells out each field its scenario depends on and takes the rest
/// from here: `BrokerRegistrationRecord { fenced: true,
/// ..broker_registration(NodeId(2)) }`.
pub(crate) fn broker_registration(node_id: NodeId) -> krabka_metadata::BrokerRegistrationRecord {
    krabka_metadata::BrokerRegistrationRecord {
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id,
        broker_epoch: 0,
        incarnation_id: uuid::Uuid::nil(),
        host: "127.0.0.1".into(),
        port: 9092,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![],
        features: std::collections::BTreeMap::new(),
    }
}

/// A named plaintext endpoint using the caller's host and port.
pub(crate) fn plaintext_broker_endpoint(host: &str, port: u16) -> krabka_metadata::BrokerEndpoint {
    krabka_metadata::BrokerEndpoint {
        name: "PLAINTEXT".into(),
        host: host.into(),
        port,
        protocol: krabka_security::ListenerProtocol::Plaintext,
    }
}

/// Wait up to five seconds for `broker` to become the controller leader.
pub(crate) async fn wait_for_controller_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !broker
        .controller
        .watch_leader()
        .borrow()
        .is_some_and(|node| node == broker.config.node_id)
    {
        assert2::assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Register `node_id` as a remote broker in the controller's image.
pub(crate) async fn seed_remote_broker(handle: &BrokerHandle, node_id: NodeId) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: -1,
                ..broker_registration(node_id)
            },
        )])
        .await
        .expect("seed broker registration");
}

/// Fence `node_id` the way the controller does: its heartbeat session is
/// fenced, and its registration says so, which is what every node reads.
pub(crate) async fn fence_remote_broker(handle: &BrokerHandle, node_id: u64) {
    let broker = handle.broker_arc_for_test();
    broker.liveness.record_fenced_heartbeat(node_id).await;
    let fence = registration_change(
        &broker.controller.current_image(),
        krabka_raft::NodeId(node_id),
        RegistrationChange::FENCE,
    );
    broker
        .controller
        .submit_change(fence.into_iter().collect())
        .await
        .expect("publish broker fencing");
}

/// Give `node_id` the witness role, as the controller-managed
/// `broker.witness` broker config does.
pub(crate) async fn make_witness(handle: &BrokerHandle, node_id: u64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_raft::NodeId(node_id),
                config_name: BROKER_WITNESS.to_string(),
                config_value: Some(WITNESS_TRUE.to_string()),
            },
        )])
        .await
        .expect("publish the witness role");
}

/// Put `node_id` in controlled shutdown, the way the controller's heartbeat
/// state machine does: its registration says so, and it stays unfenced.
pub(crate) async fn begin_controlled_shutdown(handle: &BrokerHandle, node_id: u64) {
    let broker = handle.broker_arc_for_test();
    let change = registration_change(
        &broker.controller.current_image(),
        krabka_raft::NodeId(node_id),
        RegistrationChange::CONTROLLED_SHUTDOWN,
    );
    broker
        .controller
        .submit_change(change.into_iter().collect())
        .await
        .expect("publish controlled shutdown");
}

/// Build a [`RequestContext`] over the given principal, peer, and client id.
///
/// The remaining fields are the plaintext, non-sendfile defaults that every
/// handler test shares. `client_id` is a parameter because it feeds
/// client-quota lookups and therefore varies per handler.
pub(crate) fn request_context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
    client_id: &'a str,
) -> RequestContext<'a> {
    RequestContext::new(
        principal,
        peer,
        client_id,
        "test-connection",
        false,
        "PLAINTEXT",
    )
}

/// Encode a request to wire bytes at `version`.
pub(crate) fn encode_request<T: Encode>(req: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut buf, version).expect("encode request");
    buf.freeze()
}

/// Decode a response from `bytes` at `version`, and assert that the decoder
/// consumed every byte.
pub(crate) fn decode_response<T: Decode<'static>>(bytes: &Bytes, version: i16) -> T {
    let mut cur: &[u8] = bytes.as_ref();
    let resp = T::decode(&mut cur, version).expect("decode response");
    assert2::assert!(cur.is_empty(), "response decoder consumed all bytes");
    resp
}

/// Encode and decode a response while returning the decoder's consumption result.
pub(crate) fn round_trip_response<T: Encode + Decode<'static>>(
    response: &T,
    version: krabka_ids::ApiVersion,
) -> (T, bool) {
    let bytes = encode_response(response, version.0).expect("encode");
    let mut cursor: &[u8] = &bytes;
    let decoded = T::decode(&mut cursor, version.0).expect("decode");
    (decoded, cursor.is_empty())
}

/// An authorizer that reads the grants from the principal name.
///
/// The name is a `+`-separated list of `<ResourceType>:<AclOperation>` grants,
/// in the `Debug` spelling, for example `Cluster:ClusterAction+Group:Read`. A
/// grant allows the operation on every resource of that type. Every other
/// request is denied, so the name `none` holds no grant.
#[derive(Debug)]
pub(crate) struct GrantsInPrincipalName;

impl Authorizer for GrantsInPrincipalName {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let wanted = format!("{:?}:{:?}", request.resource_type, request.operation);
        if request
            .principal
            .name
            .split('+')
            .any(|grant| grant == wanted)
        {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    }
}

/// A literal `Allow` ACL for `principal` (such as `User:alice`) from any host,
/// to `operation` on the `resource_type` resource named `resource_name`.
#[derive(Clone, Copy)]
pub(crate) struct AllowAclSetup<'a> {
    pub resource_type: krabka_metadata::ResourceType,
    pub resource_name: &'a str,
    pub principal: &'a str,
    pub operation: krabka_metadata::AclOperation,
}

impl Default for AllowAclSetup<'_> {
    fn default() -> Self {
        Self {
            resource_type: krabka_metadata::ResourceType::Topic,
            resource_name: "orders",
            principal: "User:alice",
            operation: krabka_metadata::AclOperation::Read,
        }
    }
}

pub(crate) fn allow_acl(setup: AllowAclSetup<'_>) -> krabka_metadata::AclEntry {
    let AllowAclSetup {
        resource_type,
        resource_name,
        principal,
        operation,
    } = setup;
    krabka_metadata::AclEntry {
        resource_type,
        resource_name: resource_name.to_string(),
        pattern_type: krabka_metadata::PatternType::Literal,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: krabka_metadata::PermissionType::Allow,
    }
}

/// Independent cluster `DescribeConfigs` implication cases shared by configuration APIs.
pub(crate) fn describe_configs_acl_cases<'a, R>(
    allowed: &'a R,
    denied: &'a R,
) -> Vec<(&'static str, Option<krabka_metadata::AclOperation>, &'a R)> {
    use krabka_metadata::AclOperation;
    vec![
        ("no-grant", None, denied),
        ("describe", Some(AclOperation::Describe), denied),
        ("alter", Some(AclOperation::Alter), denied),
        (
            "describe-configs",
            Some(AclOperation::DescribeConfigs),
            allowed,
        ),
        ("alter-configs", Some(AclOperation::AlterConfigs), allowed),
        ("all", Some(AclOperation::All), allowed),
    ]
}

/// Commit one literal `Allow` ACL for `User:<user>` on the cluster resource.
///
/// A test that starts its broker with a [`crate::authorizer::SimpleAclAuthorizer`]
/// uses it to check a handler against Kafka's operation-implication table
/// (for example, `AlterConfigs` implies `DescribeConfigs`, `Alter` does not).
pub(crate) async fn grant_cluster_operation(
    handle: &BrokerHandle,
    user: &str,
    operation: krabka_metadata::AclOperation,
) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(allow_acl(
            AllowAclSetup {
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
                principal: &format!("User:{user}"),
                operation,
            },
        ))])
        .await
        .expect("commit cluster acl");
}

/// Commit one literal `Allow` ACL for `User:<user>` on a `Topic` resource.
///
/// `topic` is the literal resource name; pass `"*"` for Kafka's
/// grant-on-every-topic wildcard. A test that starts its broker with a
/// [`crate::authorizer::SimpleAclAuthorizer`] uses it to check a handler
/// against Kafka's per-topic ACL filtering, including the operation-implication
/// table (for example, `Read` implies `Describe`).
pub(crate) async fn grant_topic_operation(
    handle: &BrokerHandle,
    user: &str,
    topic: &str,
    operation: krabka_metadata::AclOperation,
) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(allow_acl(
            AllowAclSetup {
                resource_name: topic,
                principal: &format!("User:{user}"),
                operation,
                ..Default::default()
            },
        ))])
        .await
        .expect("commit topic acl");
}

/// Serve one request through the broker's dispatch registry, as the connection
/// loop does for a [`crate::handlers::DispatchKind::Context`] entry.
///
/// It panics when `api_key` is not registered as a context dispatch. An
/// authorization test that calls it therefore also fails when its api goes
/// back to a dispatch kind that gets no principal.
pub(crate) async fn dispatch_context(
    broker: &Broker,
    api_key: i16,
    version: i16,
    body: &[u8],
    ctx: &RequestContext<'_>,
) -> Bytes {
    try_dispatch_context(broker, api_key, version, body, ctx)
        .await
        .unwrap_or_else(|error| panic!("api_key {api_key} handler: {error}"))
}

/// [`dispatch_context`], returning the handler's error instead of panicking
/// on it: a test of a request the adapter or the handler refuses.
pub(crate) async fn try_dispatch_context(
    broker: &Broker,
    api_key: i16,
    version: i16,
    body: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let entry = broker
        .handlers()
        .get(api_key)
        .unwrap_or_else(|| panic!("api_key {api_key} is registered"));
    let DispatchKind::Context(handler) = entry.kind() else {
        panic!("api_key {api_key} is a context dispatch, so its handler gets the principal");
    };
    handler(broker, version, body, ctx).await
}

/// Serve `req` through the broker's dispatch registry as wire bytes at
/// `version`, and decode the response at the same version.
///
/// A `typed` handler takes and returns structs, so its unit tests call it
/// directly. This is the path for a test about the wire itself: a field that
/// an older version drops, or the encoding of the response.
pub(crate) async fn dispatch_wire<Resp: Decode<'static>>(
    broker: &Broker,
    api_key: i16,
    version: i16,
    req: &impl Encode,
    ctx: &RequestContext<'_>,
) -> Resp {
    let bytes =
        dispatch_context(broker, api_key, version, &encode_request(req, version), ctx).await;
    decode_response(&bytes, version)
}

/// Configure one transaction-state partition with the supplied authorizer.
/// Readiness waits remain the caller's responsibility.
pub(crate) fn configure_single_partition_transactions(
    config: &mut BrokerConfig,
    authorizer: Arc<dyn Authorizer>,
) {
    config.authorizer = authorizer;
    config.transaction_state_num_partitions = 1;
    config.transaction_state_replication_factor = 1;
}

/// Start an in-process broker over a fresh temp dir. It applies `configure` to
/// the [`BrokerConfig::for_tests`] baseline before start.
///
/// Each handler passes a closure with exactly the config tweaks it needs, such
/// as an authorizer to install, an `audit_enabled` toggle, or share and streams
/// groups to enable. The returned [`tempfile::TempDir`] must outlive the
/// broker.
pub(crate) fn start_broker_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut cfg);
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    })
}

/// Finalizes `share.version` at `level` on a started broker: the feature that
/// turns the share-group APIs on (1) or off (0), as Kafka's
/// `isShareGroupProtocolEnabled` reads it.
pub(crate) async fn finalize_share_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: krabka_metadata::metadata_version::SHARE_VERSION_FEATURE.into(),
                level,
            },
        )])
        .await
        .expect("finalize share.version");
}

/// Start an in-process broker with only its authorizer swapped in.
///
/// This is the most common `start_broker` shape across handler test modules;
/// see [`wire_helpers`]. A handler test drives an authorization-failure
/// path with [`DenyAll`] or a custom authorizer, and it otherwise takes
/// the `for_tests` defaults.
pub(crate) async fn start_broker_with_authorizer(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| cfg.authorizer = authorizer).await
}

/// Like [`start_broker_with`], but it turns `audit_enabled` off before it
/// applies `configure`.
///
/// Most handler tests do not exercise the audit path and turn it off, so that
/// audit-log assertions elsewhere in the suite stay stable.
pub(crate) fn start_broker_no_audit_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        configure(cfg);
    })
}

/// Start an in-process broker on the [`BrokerConfig::for_tests`] baseline with
/// audit logging off.
pub(crate) fn start_broker_no_audit()
-> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    start_broker_no_audit_with(|_| {})
}

/// Like [`start_broker_with_authorizer`], but it also disables audit logging.
///
/// This is the second most common `start_broker` shape. Admin-handler tests
/// that do not exercise the audit path swap the authorizer and turn
/// `audit_enabled` off, so that audit-log assertions elsewhere in the suite
/// stay stable.
pub(crate) async fn start_broker_with_authorizer_no_audit(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await
}

/// Like [`start_broker_with`], but it then waits until the group coordinator
/// serves `__consumer_offsets`, which no broker creates at startup.
///
/// A group-handler test that needs more than an authorizer, such as streams
/// groups turned on, passes its config tweaks here.
pub(crate) async fn start_group_broker_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_broker_with(configure).await;
    handle.wait_until_group_coordinator_ready().await;
    (handle, dir)
}

/// Start an in-process broker with `authorizer` installed, and wait until its
/// group coordinator serves `__consumer_offsets`.
///
/// The broker installs `authorizer` wrapped in [`controller_peer_allowed`],
/// so a restrictive authorizer still lets the broker unfence and create the
/// coordinator topic that this waits for.
pub(crate) async fn start_group_broker(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_group_broker_with(|cfg| cfg.authorizer = controller_peer_allowed(authorizer)).await
}

/// Like [`start_group_broker`], but it also disables audit logging.
pub(crate) async fn start_group_broker_no_audit(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_group_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = controller_peer_allowed(authorizer);
    })
    .await
}

#[derive(Clone, Copy, Default)]
pub(crate) enum ShareApiSupport {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ShareBrokerSetup {
    pub support: ShareApiSupport,
}

/// Like [`start_group_broker`], but it also waits until the share coordinator
/// serves `__share_group_state`. Disabled support then finalizes
/// `share.version` at 0, which turns the share-group APIs off.
pub(crate) async fn start_share_broker(
    authorizer: std::sync::Arc<dyn Authorizer>,
    setup: ShareBrokerSetup,
) -> (BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = start_group_broker(authorizer).await;
    handle.wait_until_share_coordinator_ready().await;
    if matches!(setup.support, ShareApiSupport::Disabled) {
        finalize_share_version(&handle.broker_arc_for_test(), 0).await;
    }
    (handle, dir)
}

/// Generate the `encode_request` / `decode_response` / `test_context`
/// wrapper trio that every handler's `#[cfg(test)] mod handler_tests` binds
/// over [`encode_request`], [`decode_response`], and [`request_context`].
///
/// `wire_helpers!(ReqTy, RespTy, version = V, client_id = "id")` pins one
/// wire version; leaving out `version = V` makes `encode_request` and
/// `decode_response` take the version per call instead, for
/// version-negotiation tests. A leading visibility, as in
/// `wire_helpers!(pub(super) ReqTy, ...)`, lets a shared `test_support`
/// module hand the helpers to its sibling test modules.
macro_rules! wire_helpers {
    ($vis:vis $req:ty, $resp:ty, $(version = $version:expr,)? client_id = $client_id:expr) => {
        crate::test_support::encode_helper!($vis $req $(, version = $version)?);
        crate::test_support::response_helpers!($vis $resp, $(version = $version,)? client_id = $client_id);
    };
}
pub(crate) use wire_helpers;

/// Like [`wire_helpers`], but for handlers whose `handle()` takes an
/// already-typed request, so there is nothing to encode, and returns wire
/// `Bytes`. Only `decode_response` and `test_context` are needed.
macro_rules! response_helpers {
    ($vis:vis $resp:ty, $(version = $version:expr,)? client_id = $client_id:expr) => {
        crate::test_support::decode_helper!($vis $resp $(, version = $version)?);
        crate::test_support::context_helper!($vis client_id = $client_id);
    };
}
pub(crate) use response_helpers;

/// Like [`wire_helpers`], but for handlers that take no [`RequestContext`],
/// so there is no `test_context` to generate. It generates only
/// `encode_request` and `decode_response`.
macro_rules! codec_helpers {
    ($vis:vis $req:ty, $resp:ty $(, version = $version:expr)?) => {
        crate::test_support::encode_helper!($vis $req $(, version = $version)?);
        crate::test_support::decode_helper!($vis $resp $(, version = $version)?);
    };
}
pub(crate) use codec_helpers;

/// The `encode_request` that [`wire_helpers`] and [`codec_helpers`] generate.
macro_rules! encode_helper {
    ($vis:vis $req:ty, version = $version:expr) => {
        $vis fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }
    };
    ($vis:vis $req:ty) => {
        $vis fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }
    };
}
pub(crate) use encode_helper;

/// The `decode_response` that the other wire macros generate, usable on its
/// own by a test module that decodes but never encodes or builds a context.
macro_rules! decode_helper {
    ($vis:vis $resp:ty, version = $version:expr) => {
        $vis fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }
    };
    ($vis:vis $resp:ty) => {
        $vis fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }
    };
}
pub(crate) use decode_helper;

/// The `test_context` that [`wire_helpers`] and [`response_helpers`]
/// generate, usable on its own by a test module that needs only the context.
macro_rules! context_helper {
    ($vis:vis client_id = $client_id:expr) => {
        $vis fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use context_helper;

/// Bind `$ctx` to the calling module's `test_context` for a request from the
/// anonymous principal named `$name` at [`peer`].
///
/// `test_ctx!(ctx, "admin");` stands for the three statements that build the
/// principal, the peer, and the context. The principal and the peer live in
/// hidden bindings for the rest of the enclosing block, so a test that reads
/// either one again builds them itself.
macro_rules! test_ctx {
    ($ctx:ident, $name:expr) => {
        let principal = crate::test_support::principal($name);
        let peer = crate::test_support::peer();
        let $ctx = test_context(&principal, &peer);
    };
}
pub(crate) use test_ctx;

/// The outcome a [`FakeMetadataSource`] returns from `submit_change`, as a
/// function of the batch it was handed.
type SubmitOutcome =
    Box<dyn Fn(&[MetadataRecord]) -> Result<SubmitChangeResult, RaftError> + Send + Sync>;

/// The metadata authority that this crate's unit tests read from and write
/// through.
///
/// One image, one leader, and one capture buffer stand in for the whole
/// controller. Every [`MetadataSource`] method has a behaving default, so a
/// method added to the trait reaches every suite that fakes metadata at once,
/// rather than arriving as another `unimplemented!()` in another hand-rolled
/// double.
///
/// The image and the leader live in `watch` channels whose senders the fake
/// keeps, so [`FakeMetadataSource::set_image`] and
/// [`FakeMetadataSource::set_leader`] push a change through to whatever the
/// code under test watches, and a watcher of an unchanged fake waits rather
/// than seeing the channel close. Every batch that reaches `submit_change` is
/// captured in order and readable with [`FakeMetadataSource::submitted`].
///
/// Behaviour that genuinely varies per test stays at the call site:
/// [`FakeMetadataSourceBuilder::on_submit`] installs a different write
/// outcome, [`FakeMetadataSourceBuilder::stall_submits`] models a raft commit
/// that never returns, and
/// [`FakeMetadataSourceBuilder::controller_bound_addr`] sets the listener
/// address a test dials or asserts on, and
/// [`FakeMetadataSourceBuilder::term`] with
/// [`FakeMetadataSourceBuilder::without_controller_epoch`] set the controller
/// epoch a caller fences its writes against.
pub(crate) struct FakeMetadataSource {
    image_tx: watch::Sender<Arc<MetadataImage>>,
    leader_tx: watch::Sender<Option<NodeId>>,
    fatal_tx: watch::Sender<Option<String>>,
    controller_bound_addr: SocketAddr,
    term: u64,
    owns_controller_epoch: bool,
    submitted: Mutex<Vec<Vec<MetadataRecord>>>,
    on_submit: SubmitOutcome,
    stall_submits: bool,
    current_image_calls: AtomicUsize,
    controller_bound_addr_calls: AtomicUsize,
}

/// Yield-poll a condition with a bounded guard against a stalled test.
pub(crate) async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}

/// Plaintext controller transport for fixtures with no configured voters.
pub(crate) fn plaintext_controller_dialer() -> ControllerDialer {
    plaintext_controller_dialer_with_voters(Vec::new())
}

/// The same transport with a statically configured controller quorum.
pub(crate) fn plaintext_controller_dialer_with_voters(
    voters: Vec<(krabka_raft::NodeId, String)>,
) -> ControllerDialer {
    ControllerDialer {
        outbound_client: Arc::new(InterBrokerClient::new(None, None)),
        listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        server_name: "localhost".to_owned(),
        quorum_voters: voters,
    }
}

impl FakeMetadataSource {
    /// A static image with no elected controller and an ephemeral loopback listener.
    pub(crate) fn static_image(image: MetadataImage) -> Self {
        Self::builder()
            .image(image)
            .controller_bound_addr(SocketAddr::from(([127, 0, 0, 1], 0)))
            .build()
    }

    /// Use a test-owned publication channel, independently of successful submits.
    pub(crate) fn published_image_channel(image_tx: watch::Sender<Arc<MetadataImage>>) -> Self {
        Self {
            image_tx,
            ..Self::static_image(MetadataImage::new(uuid::Uuid::nil()))
        }
    }

    /// A builder over an empty image with no elected leader, no committed
    /// metadata, and an unspecified controller address. Every seam is a
    /// method on the returned builder.
    pub(crate) fn builder() -> FakeMetadataSourceBuilder {
        FakeMetadataSourceBuilder {
            image: Arc::new(MetadataImage::new(uuid::Uuid::nil())),
            leader: None,
            controller_bound_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            term: 0,
            owns_controller_epoch: true,
            on_submit: None,
            stall_submits: false,
        }
    }

    /// Publish `image` as the current metadata, as an applied change does.
    /// Every `watch_image` receiver observes it.
    pub(crate) fn set_image(&self, image: impl Into<Arc<MetadataImage>>) {
        self.image_tx.send_replace(image.into());
    }

    /// Publish the image that `records` build, under the nil cluster id.
    pub(crate) fn set_records(&self, records: &[MetadataRecord]) {
        self.set_image(MetadataImage::from_records(uuid::Uuid::nil(), records));
    }

    /// Publish a controller-leader change. Every `watch_leader` receiver
    /// observes it, and `quorum_state` reports it as `current_leader`.
    pub(crate) fn set_leader(&self, leader: Option<NodeId>) {
        self.leader_tx.send_replace(leader);
    }

    /// Publish a fatal controller fault, as a controller does before it stops
    /// itself. Every `watch_fatal` receiver observes it. A fake that never
    /// calls this reports no fault, and its watchers wait rather than seeing
    /// the channel close.
    pub(crate) fn set_fatal(&self, fault: &str) {
        self.fatal_tx.send_replace(Some(fault.to_owned()));
    }

    /// The leader channel's sender, for a test that drives a spawned watcher
    /// and wants a push with no receiver to fail rather than pass silently:
    /// `send` errors when nothing is watching, where
    /// [`FakeMetadataSource::set_leader`] would not.
    pub(crate) fn leader_tx(&self) -> &watch::Sender<Option<NodeId>> {
        &self.leader_tx
    }

    /// Every batch handed to `submit_change`, in call order, one entry per
    /// call. A test that asks what the code under test appended reads this
    /// rather than a success flag.
    pub(crate) fn submitted(&self) -> Vec<Vec<MetadataRecord>> {
        self.submitted
            .lock()
            .expect("the submitted batches are not poisoned")
            .clone()
    }

    /// [`FakeMetadataSource::submitted`] flattened, for a test that cares
    /// which records arrived but not how they were batched.
    pub(crate) fn submitted_records(&self) -> Vec<MetadataRecord> {
        self.submitted().concat()
    }

    /// How many times `current_image` was called.
    pub(crate) fn current_image_calls(&self) -> usize {
        self.current_image_calls.load(atomic::Ordering::Relaxed)
    }

    /// How many times `controller_bound_addr` was called.
    pub(crate) fn controller_bound_addr_calls(&self) -> usize {
        self.controller_bound_addr_calls
            .load(atomic::Ordering::Relaxed)
    }
}

/// Builder for [`FakeMetadataSource`]; see that type for what each seam is
/// for.
pub(crate) struct FakeMetadataSourceBuilder {
    image: Arc<MetadataImage>,
    leader: Option<NodeId>,
    controller_bound_addr: SocketAddr,
    term: u64,
    owns_controller_epoch: bool,
    on_submit: Option<SubmitOutcome>,
    stall_submits: bool,
}

impl FakeMetadataSourceBuilder {
    /// Serve `image` as the current metadata.
    pub(crate) fn image(mut self, image: impl Into<Arc<MetadataImage>>) -> Self {
        self.image = image.into();
        self
    }

    /// Serve the image that `records` build, under the nil cluster id.
    pub(crate) fn records(self, records: &[MetadataRecord]) -> Self {
        self.image(MetadataImage::from_records(uuid::Uuid::nil(), records))
    }

    /// Report `leader` as the controller leader, from `watch_leader` and as
    /// `quorum_state().current_leader`.
    pub(crate) fn leader(mut self, leader: Option<NodeId>) -> Self {
        self.leader = leader;
        self
    }

    /// Report `addr` as the controller listener's bound address.
    pub(crate) fn controller_bound_addr(mut self, addr: SocketAddr) -> Self {
        self.controller_bound_addr = addr;
        self
    }

    /// Report `term` as the quorum's current term, and -- unless
    /// [`FakeMetadataSourceBuilder::without_controller_epoch`] is set -- as
    /// the current controller epoch, which is what the trait's own default
    /// does with the term.
    pub(crate) fn term(mut self, term: u64) -> Self {
        self.term = term;
        self
    }

    /// Report no controller epoch at all, as a broker-only observer does: it
    /// tracks the leader id but does not own the controller's term state, so
    /// `current_controller_epoch` is `None` however the quorum's term reads.
    pub(crate) fn without_controller_epoch(mut self) -> Self {
        self.owns_controller_epoch = false;
        self
    }

    /// Decide the result of each `submit_change` from the batch it was
    /// handed. The batch is captured either way; only the outcome the caller
    /// sees changes.
    pub(crate) fn on_submit(
        mut self,
        outcome: impl Fn(&[MetadataRecord]) -> Result<SubmitChangeResult, RaftError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.on_submit = Some(Box::new(outcome));
        self
    }

    /// Never complete a `submit_change`. This models a raft commit that
    /// stalls, so that the caller's own timeout or cancellation path runs.
    pub(crate) fn stall_submits(mut self) -> Self {
        self.stall_submits = true;
        self
    }

    pub(crate) fn build(self) -> FakeMetadataSource {
        let (image_tx, _) = watch::channel(self.image);
        let (leader_tx, _) = watch::channel(self.leader);
        let (fatal_tx, _) = watch::channel(None);
        FakeMetadataSource {
            image_tx,
            leader_tx,
            fatal_tx,
            controller_bound_addr: self.controller_bound_addr,
            term: self.term,
            owns_controller_epoch: self.owns_controller_epoch,
            submitted: Mutex::new(Vec::new()),
            on_submit: self
                .on_submit
                .unwrap_or_else(|| Box::new(|_| Ok(SubmitChangeResult::default()))),
            stall_submits: self.stall_submits,
            current_image_calls: AtomicUsize::new(0),
            controller_bound_addr_calls: AtomicUsize::new(0),
        }
    }
}

/// Every reconfiguration path rejects with this. The fake has no raft log to
/// reconfigure, and reconfiguration is covered against the real controller.
fn unsupported() -> RaftError {
    RaftError::Unsupported("fake metadata source")
}

#[async_trait::async_trait]
impl MetadataSource for FakeMetadataSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.current_image_calls
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.image_tx.borrow().clone()
    }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_tx.subscribe()
    }

    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_tx.subscribe()
    }

    fn watch_fatal(&self) -> watch::Receiver<Option<String>> {
        self.fatal_tx.subscribe()
    }

    /// A quorum that has committed nothing and knows no voters. Its leader
    /// comes from the fake's leader channel, so `watch_leader` and
    /// `quorum_state` cannot disagree, and its term is
    /// [`FakeMetadataSourceBuilder::term`].
    fn quorum_state(&self) -> QuorumState {
        QuorumState {
            current_term: self.term,
            current_leader: *self.leader_tx.borrow(),
            ..Default::default()
        }
    }

    /// The quorum's term, unless the fake stands in for a source that owns no
    /// quorum view -- see
    /// [`FakeMetadataSourceBuilder::without_controller_epoch`].
    fn current_controller_epoch(&self) -> Option<u64> {
        self.owns_controller_epoch.then_some(self.term)
    }

    /// No fake has voted in a controller election.
    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        None
    }

    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        if self.stall_submits {
            std::future::pending::<()>().await;
        }
        let outcome = (self.on_submit)(&records);
        self.submitted
            .lock()
            .expect("the submitted batches are not poisoned")
            .push(records);
        outcome
    }

    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        Err(unsupported())
    }

    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        Err(unsupported())
    }

    fn controller_bound_addr(&self) -> SocketAddr {
        self.controller_bound_addr_calls
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.controller_bound_addr
    }

    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        SnapshotRange::NoSnapshot
    }

    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        Err(unsupported())
    }

    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    /// Finalizing `kraft.version` is a reconfiguration too, so it rejects with
    /// the others rather than falling through to the trait's `NotLeader`,
    /// which would send a caller down a leadership branch the fake never
    /// models.
    async fn finalize_kraft_version(&self, _version: u16) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn cancel(&self) {}
}

/// The point in a deadline's life at which a [`BrokenTimer`] fails.
///
/// [`Timer`] reports the two separately — the outer `Result` of `at` covers
/// registration, and the [`TimerFuture`] it hands back covers everything after
/// — and so do [`crate::time_util::arm`] and [`crate::time_util::fired`], which
/// is why a cadence loop has two ways to lose its ticker rather than one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerFailure {
    /// The registration is refused outright, so `arm` reports `None`.
    Registration,
    /// The registration is accepted and the deadline it yields then resolves
    /// to an error, so `fired` reports `false`.
    Completion,
}

/// A timer whose backend gives out, for the tests that assert a cadence loop
/// stops instead of spinning once it has no ticker left.
///
/// The first `healthy` deadlines are honoured, and each of them completes the
/// moment it is armed whatever duration it was asked for, so a loop takes that
/// many ticks and no more real time passes than the test needs. Every deadline
/// after them fails, at [`TimerFailure`].
///
/// This is hand-rolled rather than taken from
/// `qubit_clock::test_util::FaultInjectingTimer`, because that fixture honours
/// a deadline that is already due, and the start-up deadline of every cadence
/// loop here but the group actor's is `Duration::ZERO` — exactly the
/// registration these tests need to see refused.
pub(crate) struct BrokenTimer {
    /// The domain every deadline handed to [`Self::at`] is validated against.
    /// Nothing here reads the time; the clock exists to give the timer a
    /// domain, as the [`Timer`] contract requires.
    clock: StdMonotonicClock,
    /// Where in a deadline's life the failure surfaces.
    failure: TimerFailure,
    /// How many leading deadlines are honoured before the failures start.
    healthy: usize,
    /// How many deadlines have been asked for so far.
    registrations: AtomicUsize,
}

impl BrokenTimer {
    /// A timer that fails every deadline, including the start-up one.
    pub(crate) fn dead(failure: TimerFailure) -> Arc<Self> {
        Self::dead_after(0, failure)
    }

    /// A timer that honours `healthy` deadlines — each completing at once, so
    /// the loop takes that many ticks — and fails every deadline after them.
    pub(crate) fn dead_after(healthy: usize, failure: TimerFailure) -> Arc<Self> {
        Arc::new(Self {
            clock: StdMonotonicClock::new(),
            failure,
            healthy,
            registrations: AtomicUsize::new(0),
        })
    }

    /// This timer as the trait object a cadence loop's config holds, leaving
    /// the caller its own handle to read [`Self::registrations`] from.
    pub(crate) fn injectable(self: &Arc<Self>) -> Arc<dyn Timer> {
        Arc::clone(self) as Arc<dyn Timer>
    }

    /// How many deadlines the loop under test has asked this timer for.
    ///
    /// A loop that stopped asked for exactly one more than it was given; a
    /// loop that re-armed through the failure keeps climbing.
    pub(crate) fn registrations(&self) -> usize {
        self.registrations.load(Ordering::Relaxed)
    }
}

impl Timer for BrokenTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, _deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let nth = self.registrations.fetch_add(1, Ordering::Relaxed);
        if nth < self.healthy {
            return Ok(Box::pin(std::future::ready(Ok(()))));
        }
        let error = TimeError::TimerUnavailable {
            source: TimerUnavailableError::BackendUnavailable {
                backend: "krabka-broker test",
                source: Box::new(io::Error::other("the timer backend is gone")),
            },
        };
        match self.failure {
            TimerFailure::Registration => Err(error),
            TimerFailure::Completion => Ok(Box::pin(std::future::ready(Err(error)))),
        }
    }
}

// One `tracing` event that a `LogCapture` recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggedEvent {
    pub(crate) level: tracing::Level,
    pub(crate) target: String,
    pub(crate) message: String,
}

// Records the `tracing` events logged inside a `LogCapture::span`, so a test
// sees what one piece of work logged and not what the rest of a running broker
// logged at the same time.
#[derive(Clone, Default)]
pub(crate) struct LogCapture {
    events: Arc<Mutex<Vec<LoggedEvent>>>,
}

impl LogCapture {
    // The name `LogCapture::span` gives its span.
    const SPAN_NAME: &str = "log_capture";

    // A dispatcher that records into this capture. Install it with
    // `tracing::dispatcher::set_default` or `with_default` on the thread that
    // does the work.
    pub(crate) fn dispatch(&self) -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    // The span whose events the capture records. Create it while the
    // capture's dispatcher is the default one.
    pub(crate) fn span() -> tracing::Span {
        tracing::info_span!("log_capture")
    }

    // The events recorded so far, oldest first.
    pub(crate) fn events(&self) -> Vec<LoggedEvent> {
        self.events
            .lock()
            .expect("the captured events are not poisoned")
            .clone()
    }
}

impl<S> Layer<S> for LogCapture
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let inside = ctx
            .event_scope(event)
            .is_some_and(|mut scope| scope.any(|span| span.name() == Self::SPAN_NAME));
        if !inside {
            return;
        }
        let mut message = MessageField::default();
        event.record(&mut message);
        self.events
            .lock()
            .expect("the captured events are not poisoned")
            .push(LoggedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                message: message.0,
            });
    }
}

// The `message` of an event, as `format_args!` renders it.
#[derive(Default)]
struct MessageField(String);

impl tracing::field::Visit for MessageField {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;
    use krabka_metadata::{
        AclOperation, KRaftVersionRange, MetadataImage, MetadataRecord, ResourceType, Voter,
    };
    use krabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, RemoveVoter, SnapshotRange,
        SubmitChangeResult, UpdateVoter,
    };

    use super::FakeMetadataSource;
    use crate::{
        authorizer::{
            AclSource, AllowAllAuthorizer, AuthorizationRequest,
            AuthorizationResult::{self, Allow, Deny},
            Authorizer,
        },
        metadata_source::MetadataSource,
    };

    /// An authorizer with a decision cache of its own and a real
    /// `authorize_by_resource_type` scan, so that forwarding is visible.
    #[derive(Debug)]
    struct Cached;

    impl Authorizer for Cached {
        fn authorize(
            &self,
            _source: &dyn AclSource,
            _request: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Deny
        }

        fn decision_ttl(&self) -> Option<std::time::Duration> {
            Some(std::time::Duration::from_secs(7))
        }

        fn authorize_by_resource_type(
            &self,
            _source: &dyn AclSource,
            _principal: &krabka_security::Principal,
            _host: &std::net::SocketAddr,
            _resource_type: krabka_metadata::ResourceType,
            _operation: krabka_metadata::AclOperation,
        ) -> AuthorizationResult {
            AuthorizationResult::Allow
        }
    }

    /// `controller_peer_allowed` allows only `ClusterAction` and `Create` on
    /// the `Cluster` for the `ANONYMOUS` controller peer, and answers
    /// everything else as the wrapped authorizer does.
    #[test]
    fn controller_peer_allowed_adds_only_the_controller_peer_grant() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let peer = super::peer();
        let alice = super::principal("alice");
        let authorizer = super::controller_peer_allowed(std::sync::Arc::new(Cached));
        let cases = [
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::ClusterAction,
                Allow,
            ),
            (
                "alice",
                ResourceType::Cluster,
                AclOperation::ClusterAction,
                Deny,
            ),
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::Create,
                Allow,
            ),
            ("alice", ResourceType::Cluster, AclOperation::Create, Deny),
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::Alter,
                Deny,
            ),
            ("ANONYMOUS", ResourceType::Topic, AclOperation::Create, Deny),
            (
                "ANONYMOUS",
                ResourceType::Topic,
                AclOperation::ClusterAction,
                Deny,
            ),
        ];
        for (name, resource_type, operation, expected) in cases {
            let principal = super::principal(name);
            let request = AuthorizationRequest {
                principal: &principal,
                host: &peer,
                resource_type,
                resource_name: "kafka-cluster",
                operation,
            };
            assert!(
                authorizer.authorize(&image, &request) == expected,
                "{name} {resource_type:?} {operation:?}"
            );
        }
        assert!(
            (
                authorizer.is_configured(),
                authorizer.decision_ttl(),
                authorizer.authorize_by_resource_type(
                    &image,
                    &alice,
                    &peer,
                    ResourceType::Topic,
                    AclOperation::Write,
                ),
            ) == (true, Some(std::time::Duration::from_secs(7)), Allow)
        );
        let unconfigured = super::controller_peer_allowed(std::sync::Arc::new(AllowAllAuthorizer));
        assert!(!unconfigured.is_configured());
    }

    fn voter() -> Voter {
        Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(1),
            endpoints: Vec::new(),
            kraft_version: KRaftVersionRange::default(),
        }
    }

    /// Every reconfiguration path rejects the same way, `finalize_kraft_version`
    /// included: the fake has no raft log to reconfigure, and a caller that
    /// reached one should see that rather than a leadership error the fake
    /// never models.
    #[tokio::test]
    async fn every_reconfiguration_path_rejects_as_unsupported() {
        let source = FakeMetadataSource::builder().build();

        assert!(let Err(RaftError::Unsupported(_)) =
            source.change_membership(BTreeSet::from([NodeId(1)])).await);
        assert!(let Err(RaftError::Unsupported(_)) =
            source.add_learner(NodeId(1), Node::default()).await);
        assert!(let Err(RaftError::Unsupported(_)) = source.trigger_snapshot().await);
        assert!(let Err(RaftError::Unsupported(_)) = source
            .add_voter(AddVoter {
                voter: voter(),
                ack_when_committed: true,
            })
            .await);
        assert!(let Err(RaftError::Unsupported(_)) = source
            .remove_voter(RemoveVoter {
                id: NodeId(1),
                directory_id: uuid::Uuid::from_u128(1),
            })
            .await);
        assert!(let Err(RaftError::Unsupported(_)) =
            source.update_voter(UpdateVoter { voter: voter() }).await);
        assert!(let Err(RaftError::Unsupported(_)) = source.finalize_kraft_version(1).await);
    }

    /// The controller epoch is the quorum's term by default, as the trait's
    /// own default makes it, and is `None` for a source that owns no quorum
    /// view -- which is what a broker-only observer reports.
    #[test]
    fn the_controller_epoch_follows_the_term_unless_the_fake_owns_no_quorum() {
        let controller = FakeMetadataSource::builder().term(7).build();
        assert!(controller.quorum_state().current_term == 7);
        assert!(controller.current_controller_epoch() == Some(7));

        let observer = FakeMetadataSource::builder()
            .term(7)
            .without_controller_epoch()
            .build();
        assert!(observer.quorum_state().current_term == 7);
        assert!(observer.current_controller_epoch().is_none());
    }

    /// The quorum has committed nothing and knows no voters, and its leader is
    /// whatever the leader channel last published, so `watch_leader` and
    /// `quorum_state` cannot disagree.
    #[test]
    fn quorum_state_reports_the_leader_channel_over_an_empty_quorum() {
        let source = FakeMetadataSource::builder()
            .leader(Some(NodeId(2)))
            .build();

        let QuorumState {
            current_term,
            last_applied_index,
            current_leader,
            voters,
            voter_nodes,
            per_voter_matched_index,
            ..
        } = source.quorum_state();
        assert!(current_term == 0);
        assert!(last_applied_index == 0);
        assert!(current_leader == Some(NodeId(2)));
        assert!(voters.is_empty());
        assert!(voter_nodes.is_empty());
        assert!(per_voter_matched_index.is_empty());
        assert!(source.current_metadata_offset() == -1);

        source.set_leader(None);
        assert!(source.quorum_state().current_leader.is_none());
    }

    /// The fake keeps no checkpoint to serve and has cast no vote.
    #[test]
    fn the_fake_serves_no_snapshot_and_records_no_vote() {
        let source = FakeMetadataSource::builder().build();

        assert!(matches!(
            source.read_snapshot_range(0, 1024),
            SnapshotRange::NoSnapshot
        ));
        assert!(source.voted_directory_id().is_none());
    }

    /// `cancel` has no background work to stop, and leaves the source serving.
    #[tokio::test]
    async fn cancel_leaves_the_source_serving() {
        let source = FakeMetadataSource::builder().build();

        source.cancel().await;

        assert!(let Ok(result) = source.submit_change(Vec::new()).await);
        assert!(result == SubmitChangeResult::default());
        assert!(source.submitted() == vec![Vec::<MetadataRecord>::new()]);
    }
}

/// Independent published ELR fixture; both lists retain the caller's order.
pub(crate) fn partition_elr(eligible: &[i32], last_known: &[i32]) -> PartitionElr {
    PartitionElr {
        eligible_leader_replicas: eligible.to_vec(),
        last_known_elr: last_known.to_vec(),
    }
}

/// A plaintext dialer that records each connection's client ID before dialing.
#[derive(Clone)]
pub(crate) struct RecordingDialer {
    pub(crate) client_ids: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl krabka_raft::OutboundDialer for RecordingDialer {
    async fn dial(
        &self,
        target: NodeId,
        addr: &str,
        options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
        self.client_ids
            .lock()
            .unwrap()
            .push(options.client_id.clone());
        krabka_raft::PlaintextDialer
            .dial(target, addr, options)
            .await
    }
}

krabka_macros::keyed_record_batch_fixture!(keyed_records_batch);

#[derive(Clone, Copy, Default)]
pub(crate) struct EmptyPayloadBatchSetup {
    pub count: RecordCount,
}

/// An empty-payload fixture batch with the protocol's original default headers.
pub(crate) fn default_records_batch(setup: EmptyPayloadBatchSetup) -> RecordBatch {
    let n = setup.count.0;
    let mut batch = RecordBatch {
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for offset_delta in 0..n {
        batch.records.push(Record {
            offset_delta,
            ..Default::default()
        });
    }
    batch
}

/// Explicit values with the protocol's default batch header and leader epoch zero.
pub(crate) fn default_value_records_batch(values: &[&'static [u8]]) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: 0,
        ..static_records_batch(values, UnixMillis(0))
    }
}

pub(crate) fn single_value_batch(value: &'static [u8]) -> RecordBatch {
    default_value_records_batch(&[value])
}

/// Spawn a caught-up replica whose entire local log is committed.
/// Cleaner and retention fixtures must advance the high watermark through
/// the follower path, which clamps it to the local log end.
pub(crate) struct CommittedPartitionSetup<'a> {
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    pub leader: krabka_metadata::NodeId,
    pub registry: LogDirRegistry,
}

impl Default for CommittedPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            partition: krabka_ids::PartitionIndex(0),
            leader: krabka_metadata::NodeId(1),
            registry: LogDirRegistry::default(),
        }
    }
}

pub(crate) async fn committed_partition(
    root: &std::path::Path,
    log: krabka_log::Log,
    setup: CommittedPartitionSetup<'_>,
) -> Arc<Partition> {
    let CommittedPartitionSetup {
        topic,
        partition,
        leader,
        registry,
    } = setup;
    let part = spawn_partition(
        topic.to_owned(),
        partition,
        root.to_path_buf(),
        log,
        registry,
        Arc::new(ProducerState::new()),
        false,
    );
    part.current_leader.store(leader.0, Ordering::Relaxed);
    part.set_follower_hw(krabka_log::Offset(i64::MAX)).await;
    part
}

/// Finalize ELR and seed a topic, its partition and minimum ISR together.
pub(crate) fn elr_topic_records(
    partition: PartitionRecord,
    topic_id: uuid::Uuid,
    min_isr: &str,
) -> Vec<MetadataRecord> {
    let topic = partition.topic.clone();
    vec![
        MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
            name: ELR_VERSION.into(),
            level: 1,
        }),
        MetadataRecord::V1Topic(TopicRecord {
            name: topic.clone(),
            topic_id,
            partitions: 1,
            replication_factor: i16::try_from(partition.replicas.len()).unwrap(),
        }),
        MetadataRecord::V1Partition(partition),
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic,
            overrides: [(MIN_INSYNC_REPLICAS.to_owned(), min_isr.to_owned())]
                .into_iter()
                .collect(),
        }),
    ]
}

/// Keep both temporary directories alive while callers use their paths.
pub(crate) fn two_log_dirs() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Vec<std::path::PathBuf>,
) {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let paths = vec![a.path().to_path_buf(), b.path().to_path_buf()];
    (a, b, paths)
}

/// Wire topology edge shared by authorization and actor cases.
pub(crate) fn source_to_repartition()
-> krabka_protocol::owned::streams_group_heartbeat_request::Subtopology {
    krabka_protocol::owned::streams_group_heartbeat_request::Subtopology {
        subtopology_id: "0".into(),
        source_topics: vec!["orders".into()],
        repartition_sink_topics: vec!["rp".into()],
        ..Default::default()
    }
}

/// Describe a topic independently of the create response's config conversion.
type TopicConfigOverrides = std::collections::BTreeMap<String, String>;
static NO_TOPIC_OVERRIDES: TopicConfigOverrides = TopicConfigOverrides::new();

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct DescribedCreationSetup<'a> {
    #[default(krabka_metadata::NodeId(1))]
    pub node: krabka_metadata::NodeId,
    #[default("t")]
    pub topic: &'a str,
    #[default(&NO_TOPIC_OVERRIDES)]
    pub overrides: &'a TopicConfigOverrides,
}

pub(crate) fn described_creation_configs(
    image: &krabka_metadata::MetadataImage,
    setup: DescribedCreationSetup<'_>,
) -> Vec<krabka_protocol::owned::create_topics_response::CreatableTopicConfigs> {
    effective_topic_configs(
        image,
        setup.node,
        setup.topic,
        setup.overrides,
        UnstableApiVersions::Disabled,
        &std::collections::BTreeMap::new(),
    )
    .into_iter()
    .map(
        |entry| krabka_protocol::owned::create_topics_response::CreatableTopicConfigs {
            name: entry.name,
            value: entry.value,
            read_only: entry.read_only,
            config_source: entry.config_source,
            is_sensitive: entry.is_sensitive,
            ..Default::default()
        },
    )
    .collect()
}

/// A fresh log and deterministic timestamps bracketing its segment age limit.
pub(crate) struct SegmentAgeFixture {
    pub(crate) log: krabka_log::Log,
    pub(crate) config: krabka_log::LogConfig,
    pub(crate) dir: tempfile::TempDir,
    pub(crate) roll_ms: i64,
    pub(crate) first_ms: i64,
}

pub(crate) fn segment_age_fixture() -> SegmentAgeFixture {
    use krabka_units::convert::TimeExt as _;
    let dir = tempfile::tempdir().unwrap();
    let config = krabka_log::LogConfig::default();
    let roll_ms = config.segment_roll_interval.millis_i64_trunc();
    let log = krabka_log::Log::open(dir.path(), config.clone()).unwrap();
    SegmentAgeFixture {
        dir,
        config,
        log,
        roll_ms,
        first_ms: 1_700_000_000_000,
    }
}

/// Owned configuration values from the same key/value table used by each test.
pub(crate) fn config_map(pairs: &[(&str, &str)]) -> std::collections::BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// Wait for spawned tasks to enter their bodies before testing their cleanup.
pub(crate) async fn wait_tasks_started(ready: impl Fn() -> bool, context: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect(context);
}

/// Describe the partitions of a single topic with the standard fixture page limit.
pub(crate) fn topic_partitions_request(
    topic: &str,
) -> krabka_protocol::owned::describe_topic_partitions_request::DescribeTopicPartitionsRequest {
    use krabka_protocol::owned::describe_topic_partitions_request::{
        DescribeTopicPartitionsRequest, TopicRequest,
    };
    DescribeTopicPartitionsRequest {
        topics: vec![TopicRequest {
            name: topic.into(),
            ..Default::default()
        }],
        response_partition_limit: 2000,
        ..Default::default()
    }
}

/// Start the one-partition transaction fixture with grants encoded in principal names.
pub(crate) async fn start_transaction_grant_broker() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| {
        configure_single_partition_transactions(
            cfg,
            Arc::new(ControllerPeerAllowed(GrantsInPrincipalName)),
        );
    })
    .await
}

/// A scripted wire response after its correlation id, including flexible header tags.
pub(crate) fn scripted_response_body(body: &impl Encode, version: i16, flexible: bool) -> Vec<u8> {
    let mut out = bytes::BytesMut::new();
    if flexible {
        bytes::BufMut::put_u8(&mut out, 0);
    }
    body.encode(&mut out, version)
        .expect("encode the scripted answer");
    out.to_vec()
}

/// Kafka's common offset/transaction append-error translations, independent of either mapper.
pub(crate) const COORDINATOR_WRITE_ERROR_CASES: [(i16, i16); 7] = [
    (NOT_ENOUGH_REPLICAS, COORDINATOR_NOT_AVAILABLE),
    (REQUEST_TIMED_OUT, COORDINATOR_NOT_AVAILABLE),
    (NOT_LEADER_OR_FOLLOWER, NOT_COORDINATOR),
    (KAFKA_STORAGE_ERROR, NOT_COORDINATOR),
    (MESSAGE_TOO_LARGE, UNKNOWN_SERVER_ERROR),
    (RECORD_LIST_TOO_LARGE, UNKNOWN_SERVER_ERROR),
    (UNKNOWN_SERVER_ERROR, UNKNOWN_SERVER_ERROR),
];

/// One heartbeat expiring after the fixed ten-millisecond test window.
pub(crate) async fn expired_broker_fixture(
    broker: u64,
) -> (TestClock, ControllerLivenessState, Vec<LivenessTransition>) {
    use crate::heartbeat::controller_state::{ControllerLivenessState, TestClock};
    let clock = TestClock::new();
    let liveness =
        ControllerLivenessState::with_test_clock(std::time::Duration::from_millis(10), &clock);
    liveness.record_heartbeat(broker).await;
    clock.advance(std::time::Duration::from_millis(11));
    let transitions = liveness.tick().await;
    (clock, liveness, transitions)
}

/// A controller voter, independent of broker registrations.
pub(crate) struct VoterImageSetup {
    pub node: krabka_ids::NodeId,
    pub endpoints: Vec<krabka_metadata::VoterEndpoint>,
}

impl Default for VoterImageSetup {
    fn default() -> Self {
        Self {
            node: krabka_ids::NodeId(1),
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_owned(),
                host: "127.0.0.1".to_owned(),
                port: 9093,
            }],
        }
    }
}

pub(crate) fn voter_image(setup: VoterImageSetup) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1Voters(
        krabka_metadata::VotersRecord {
            voters: krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
                id: setup.node,
                directory_id: uuid::Uuid::nil(),
                endpoints: setup.endpoints,
                kraft_version: krabka_metadata::KRaftVersionRange::default(),
            }]),
        },
    ));
    image
}

/// Start the allow-all fixture and wait for its local controller to lead.
pub(crate) async fn started_controller_broker() -> (BrokerHandle, tempfile::TempDir, Arc<Broker>) {
    let (handle, dir) = start_broker_with_authorizer(Arc::new(AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    wait_for_controller_leader(&broker).await;
    (handle, dir, broker)
}

/// Append a fixture batch through the partition's real log.
pub(crate) fn append_partition_batch(partition: &Partition, batch: &mut RecordBatch) {
    partition
        .log
        .lock()
        .expect("partition log lock")
        .append(batch)
        .expect("append");
}

#[derive(Clone, Copy)]
pub(crate) enum OAuthTokenContents {
    Nonempty,
    Empty,
}

/// Keep the token file's directory alive while callers exercise config parsing or validation.
pub(crate) fn oauth_token_file(
    contents: OAuthTokenContents,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let token_path = dir.path().join("token");
    let bytes = match contents {
        OAuthTokenContents::Nonempty => "header.payload.\n",
        OAuthTokenContents::Empty => "\n",
    };
    std::fs::write(&token_path, bytes).unwrap();
    (dir, token_path)
}

pub(crate) fn listener_time_overrides(
    overrides: &[(&str, krabka_units::Time)],
) -> std::collections::BTreeMap<String, krabka_units::Time> {
    overrides
        .iter()
        .map(|(name, value)| ((*name).to_string(), *value))
        .collect()
}

/// Keep a sweep's log root alive alongside its initially empty registry.
pub(crate) fn sweep_registry_fixture() -> (tempfile::TempDir, Arc<PartitionRegistry>) {
    (
        tempfile::tempdir().expect("log root"),
        Arc::new(PartitionRegistry::new()),
    )
}

/// Block on the blocking pool until a sweep has armed its one interval timer.
pub(crate) async fn park_manual_timer(clock: &Arc<qubit_clock::ManualMonotonicClock>) -> bool {
    let waiters = Arc::clone(clock);
    tokio::task::spawn_blocking(move || waiters.wait_for_waiters(1, Duration::from_secs(5)))
        .await
        .unwrap()
}

use krabka_metadata::NodeId as InSyncReplicaId;
use uuid::Uuid as ReplicaDirectory;

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct InSyncPartitionSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    #[default(krabka_metadata::LeaderEpoch(0))]
    pub leader_epoch: krabka_metadata::LeaderEpoch,
    #[default(vec![InSyncReplicaId(1), InSyncReplicaId(2)])]
    pub replicas: Vec<InSyncReplicaId>,
    pub directories: Vec<ReplicaDirectory>,
}

/// A fully in-sync assignment whose first replica leads, retaining exact directory slots.
pub(crate) fn in_sync_partition(setup: InSyncPartitionSetup<'_>) -> PartitionRecord {
    PartitionRecord {
        topic: setup.topic.to_string(),
        partition: setup.partition.0,
        leader: setup.replicas[0],
        isr: setup.replicas.clone(),
        replicas: setup.replicas,
        leader_epoch: setup.leader_epoch,
        directories: setup.directories,
        adding_replicas: vec![],
        removing_replicas: vec![],
        partition_epoch: 0,
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct StreamsJoinSetup<'a> {
    #[default("g")]
    pub group_id: &'a str,
    #[default("m1")]
    pub member_id: &'a str,
}

/// A streams join reporting empty owned task sets and its complete topology.
pub(crate) fn streams_join_request(
    topology: krabka_protocol::owned::streams_group_heartbeat_request::Topology,
    setup: StreamsJoinSetup<'_>,
) -> krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
    krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest {
        group_id: setup.group_id.into(),
        member_id: setup.member_id.into(),
        member_epoch: 0,
        rebalance_timeout_ms: 1_000,
        active_tasks: Some(vec![]),
        standby_tasks: Some(vec![]),
        warmup_tasks: Some(vec![]),
        topology: Some(topology),
        ..Default::default()
    }
}
