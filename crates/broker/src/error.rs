//! Internal errors produced by the broker's handlers and lifecycle.
//!
//! These are NOT Kafka wire-level error codes. Those live in
//! [`crate::codes`]. The handler boundary converts a `BrokerError` to a wire
//! code.

use thiserror::Error;

/// Errors produced by the broker's lifecycle and handlers.
///
/// [`crate::Broker::start`] returns these errors, and the per-connection serve
/// loops pass them up. The `#[non_exhaustive]` attribute lets a later release
/// add variants without a breaking change.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BrokerError {
    /// Filesystem I/O failure, for example when the broker binds the listener
    /// or opens log dirs.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Storage-layer error that comes up from [`krabka_log`].
    #[error("log: {0}")]
    Log(#[from] krabka_log::LogError),

    /// Wire-protocol decoding or encoding error.
    #[error("protocol: {0}")]
    Protocol(#[from] krabka_protocol::ProtocolError),

    /// Consensus or raft transport error.
    #[error("raft: {0}")]
    Raft(#[from] krabka_raft::RaftError),

    /// The peer sent an `(api_key, version)` pair that the handler table
    /// cannot serve.
    #[error("unsupported api_key={api_key} version={version}")]
    UnsupportedApi {
        /// The unsupported Kafka API key.
        api_key: i16,
        /// The unsupported version negotiated by the peer.
        version: i16,
    },

    /// The log refused a client batch because of the producer's transaction
    /// state on the partition (KIP-890).
    #[error("transactional append refused: {0:?}")]
    TransactionAppend(krabka_log::TransactionAppendRefusal),

    /// A produce request arrived at a partition whose writer actor has
    /// exited. This normally happens only at shutdown.
    #[error("partition writer for {topic}-{partition} died")]
    PartitionWriterDied {
        /// Topic name of the dead writer.
        topic: String,
        /// Partition index of the dead writer.
        partition: i32,
    },

    /// The broker is shutting down and refuses new work.
    #[error("shutting down")]
    Shutdown,

    /// A failure during [`crate::Broker::start`], such as controller bring-up
    /// or a leader-election timeout.
    #[error("startup failed: {0}")]
    Startup(String),

    /// The metadata controller that this node hosts met a fault that Kafka's
    /// `ProcessTerminatingFaultHandler` halts the process over, such as a
    /// replay of a `FeatureLevelRecord` above the range it supports. The text
    /// is the `Encountered fatal fault: ` prefix of that handler's log line,
    /// then the message of the exception that caused the fault. Kafka's
    /// `failureMessage` between the two, such as `Unable to apply
    /// FeatureLevelRecord record at offset N on active controller, ...`, is
    /// not part of it. [`crate::Broker::start`] returns it when the fault
    /// stops the start, and a host returns it from `main` after a stop that a
    /// fault caused, so that the process exits non-zero as Kafka's does.
    #[error("Encountered fatal fault: {0}")]
    FatalFault(String),

    /// A group-coordinator request arrived while the group is in a state that
    /// does not allow it, for example a heartbeat during
    /// `PreparingRebalance`.
    #[error("group {group_id} is in state {state:?}, request not allowed")]
    GroupInvalidState {
        /// The affected group id.
        group_id: String,
        /// The current `GroupState` rendered via `Debug`.
        state: String,
    },

    /// The client named a `member_id` that the coordinator does not track for
    /// this group.
    #[error("unknown member {member_id} in group {group_id}")]
    UnknownMember {
        /// The affected group id.
        group_id: String,
        /// The unrecognized member id.
        member_id: String,
    },

    /// The client sent a request bound to a stale generation.
    #[error("group {group_id} generation mismatch: have {current}, got {requested}")]
    GenerationMismatch {
        /// The affected group id.
        group_id: String,
        /// The coordinator's current generation.
        current: i32,
        /// The generation the client supplied.
        requested: i32,
    },

    /// The client's producer epoch is older than the current one registered
    /// for this producer id.
    #[error("producer epoch fenced: pid={producer_id} got {requested}, current {current}")]
    ProducerEpochFenced {
        /// The producer id that was fenced.
        producer_id: i64,
        /// The epoch currently registered for this producer id.
        current: i16,
        /// The epoch the client supplied.
        requested: i16,
    },

    /// A transaction marker came from an older coordinator generation than
    /// the latest marker already recorded for the producer.
    #[error("transaction coordinator epoch fenced: got {requested}, current {current}")]
    CoordinatorEpochFenced {
        /// The latest coordinator epoch recorded by the partition log.
        current: i32,
        /// The coordinator epoch carried by the marker request.
        requested: i32,
    },

    #[error("fenced leader epoch (have={have}, current={current})")]
    FencedLeaderEpoch { have: i32, current: i32 },

    #[error("unknown leader epoch ({0})")]
    UnknownLeaderEpoch(i32),

    /// A replication-layer failure, such as a failed fetch from the leader or
    /// a truncation error. It maps to `UNKNOWN_SERVER_ERROR` on the wire.
    #[error("replication: {0}")]
    Replication(String),

    /// A transactional operation failed. It maps to `UNKNOWN_SERVER_ERROR` on
    /// the wire. Handlers choose the specific wire codes.
    #[error("transaction: {0}")]
    Txn(String),

    /// No producer id could be handed out: the local block is used up and
    /// the controller did not grant the next one. Kafka's
    /// `RPCProducerIdManager.generateProducerId` throws
    /// `COORDINATOR_LOAD_IN_PROGRESS` in that case, which a client retries,
    /// so this maps to that code on the wire.
    #[error("producer ID block unavailable: {0}")]
    ProducerIdBlockUnavailable(String),

    /// The leader of one (topic, partition) refused a transaction marker, or
    /// the marker did not commit there. A remote leader answers `code` in its
    /// `WriteTxnMarkers` response, and this broker gives the same code for a
    /// partition that it leads itself. The transaction marker fan-out
    /// classifies `code` as retriable or fatal (KIP-98's
    /// `TransactionMarkerRequestCompletionHandler`) rather than treating
    /// every non-`NONE` answer the same way.
    #[error("WriteTxnMarkers refused (code {code}): {message}")]
    MarkerWriteRefused {
        /// The per-partition error code the leader answered.
        code: i16,
        /// A human-readable description naming the partition and code.
        message: String,
    },

    /// A KIP-932 share-coordinator (persister) operation failed. It maps to
    /// `UNKNOWN_SERVER_ERROR` on the wire. Handlers choose the specific wire
    /// codes.
    #[error("share: {0}")]
    Share(String),

    /// The share-state coordinator answered a KIP-932 persister call for one
    /// partition with a non-zero error code, for example `NOT_COORDINATOR` or
    /// `FENCED_STATE_EPOCH`. The share-partition leader maps `code` to the
    /// error that it gives the client.
    #[error("share state partition error (code {code}): {message}")]
    SharePartitionState {
        /// The partition error code that the coordinator answered.
        code: i16,
        /// What failed, for the log.
        message: String,
    },

    /// A group coordinator write to `__consumer_offsets` that this broker
    /// cannot confirm as committed. `code` is the answer of Kafka's
    /// `CoordinatorOperationExceptionHelper.handleOperationException` for it:
    /// `NOT_COORDINATOR` when this broker does not lead the partition, or
    /// stops leading it before the high watermark covers the write, and
    /// `COORDINATOR_NOT_AVAILABLE` when the write times out.
    #[error(
        "group coordinator write to __consumer_offsets-{partition} is not committed (code {code})"
    )]
    CoordinatorWriteUncommitted {
        /// The `__consumer_offsets` partition of the write.
        partition: i32,
        /// The coordinator error code that the client gets.
        code: i16,
    },

    /// A transaction coordinator write to `__transaction_state` that did not
    /// commit. `code` is the answer of Kafka's
    /// `TransactionStateManager.appendTransactionToLog` for it, after its
    /// mapping of the append error: `NOT_COORDINATOR` when this broker does
    /// not lead the partition or stops leading it before the write commits,
    /// `COORDINATOR_NOT_AVAILABLE` when the ISR is too small or the write
    /// times out, and `COORDINATOR_LOAD_IN_PROGRESS` while the partition
    /// loads.
    #[error(
        "transaction coordinator write to __transaction_state-{partition} is not committed (code {code})"
    )]
    TransactionStateWriteUncommitted {
        /// The `__transaction_state` partition of the write.
        partition: i32,
        /// The coordinator error code that the client gets.
        code: i16,
    },

    /// Two listeners share the same `bind_addr`.
    #[error("listener bind conflict: {a} and {b} share bind_addr")]
    ListenerConflict { a: String, b: String },

    /// `inter_broker_listener_name` does not match any listener name.
    #[error("inter_broker_listener_name {name} not in listeners list")]
    InvalidInterBrokerListener { name: String },

    /// `process.roles` was empty. A node must be a `controller`, a `broker`,
    /// or both.
    #[error("process.roles must list at least one role")]
    EmptyRoles,

    /// A non-controller node lists itself in `controller_quorum_voters`.
    #[error("node {node_id} is not a controller but appears in its own controller_quorum_voters")]
    NonControllerIsVoter { node_id: krabka_raft::NodeId },

    /// A controller-only node names `[[listeners]]`. Kafka refuses a
    /// `listeners` config that names any listener outside
    /// `controller.listener.names` when `process.roles=controller`. Krabka's
    /// controller listener is `controller_listen_addr`, so `[[listeners]]`
    /// must be empty.
    #[error(
        "The [[listeners]] config must be empty when process.roles=controller, as Kafka's \
         listeners config must only contain KRaft controller listeners from \
         controller.listener.names: a controller-only node opens only its controller listener, \
         controller_listen_addr. It names: {names}"
    )]
    ControllerOnlyNodeListeners { names: String },

    /// `process.roles` names `witness` without `broker`.
    #[error(
        "process.roles names `witness` without `broker`: a witness holds a full copy of every \
         partition it replicates, so it must also be a broker"
    )]
    WitnessRequiresBrokerRole,

    /// `process.roles` names `witness` without `controller`.
    #[error(
        "process.roles names `witness` without `controller`: a witness votes in the metadata \
         quorum, and a node that cannot vote gives the witness site no say in a site loss"
    )]
    WitnessRequiresControllerRole,

    /// `[stretch] sites` does not name exactly three sites.
    #[error(
        "stretch.sites names {count} sites: a stretch profile needs exactly three, two that \
         serve clients and one witness site"
    )]
    StretchProfileNeedsThreeSites {
        /// Number of entries the operator supplied.
        count: usize,
    },

    /// `[stretch] sites` names the same site more than once.
    #[error(
        "stretch.sites names {site} more than once: three distinct sites are what makes a \
         single-site loss survivable"
    )]
    StretchProfileDuplicateSite {
        /// The repeated site name.
        site: String,
    },

    /// `[stretch] witness_site` is not one of `[stretch] sites`.
    #[error("stretch.witness_site {site} is not one of stretch.sites")]
    StretchWitnessSiteUnknown {
        /// The unrecognized site name.
        site: String,
    },

    /// `[stretch] preferred_leader_site` is not one of `[stretch] sites`.
    #[error("stretch.preferred_leader_site {site} is not one of stretch.sites")]
    StretchPreferredSiteUnknown {
        /// The unrecognized site name.
        site: String,
    },

    /// `[stretch] preferred_leader_site` names the witness site.
    #[error(
        "stretch.preferred_leader_site is the witness site {site}: a witness never leads a \
         partition, so leadership cannot prefer that site"
    )]
    StretchPreferredSiteIsWitness {
        /// The witness site that leadership was pinned to.
        site: String,
    },

    /// A stretch profile is configured but this node has no rack.
    #[error(
        "stretch profile is configured but rack is unset: a node in a stretch cluster must name \
         the site it runs in"
    )]
    StretchRequiresRack,

    /// This node's rack is not one of `[stretch] sites`.
    #[error("rack {rack} is not one of stretch.sites")]
    StretchRackNotInProfile {
        /// This node's rack identifier.
        rack: String,
    },

    /// This node runs in the witness site but does not carry the witness role.
    #[error(
        "this node runs in the witness site but process.roles does not name `witness`: a node \
         in the witness site must not serve clients or lead a partition"
    )]
    StretchWitnessSiteNeedsWitnessRole,

    /// This node carries the witness role outside the witness site.
    #[error(
        "process.roles names `witness` but rack {rack} is not the witness site: a witness in a \
         site that serves clients would leave that site without a leader-eligible replica"
    )]
    StretchWitnessRoleOutsideWitnessSite {
        /// This node's rack identifier.
        rack: String,
    },

    /// The stretch profile cannot survive a site loss with `acks=all` intact.
    ///
    /// Either bound of
    /// [`krabka_verified::stretch::min_insync_is_site_loss_safe`] can fail
    /// here, so the message names both.
    #[error(
        "min.insync.replicas {min_insync} is unsafe at replication factor {replication_factor} \
         across three sites: it must exceed the replicas any one site holds, so that no single \
         site can hold every copy of an acknowledged write, and it must not exceed the replicas \
         a site loss leaves, so that acks=all writes keep committing after one"
    )]
    StretchMinInsyncUnsafe {
        /// The configured default `min.insync.replicas`.
        min_insync: i32,
        /// The replication factor the check ran against.
        replication_factor: i16,
    },

    /// A SASL listener is declared but `enabled_sasl_mechanisms` is empty.
    #[error("SASL listener {name} declared but enabled_sasl_mechanisms is empty")]
    SaslListenerNoMechanisms { name: String },

    /// A listener offers SASL/PLAIN while the broker holds no PLAIN
    /// credential, so every PLAIN login would fail with
    /// `SASL_AUTHENTICATION_FAILED`.
    #[error(
        "listener {name} enables SASL/PLAIN but no PLAIN credentials are configured: set \
         [sasl_plain] credentials_path in broker.toml"
    )]
    PlainListenerNoCredentials { name: String },

    /// `Gssapi` is an enabled SASL mechanism, but the config supplied no
    /// `gssapi` block with a keytab, a service name, and a principal
    /// mapping.
    #[error("GSSAPI is an enabled SASL mechanism but gssapi config is missing")]
    GssapiConfigMissing,

    /// TLS configuration error.
    #[error("tls: {0}")]
    Tls(String),

    /// The broker failed to read or decode the bootstrap records file that
    /// `krabka format --add-scram` wrote.
    #[error("bootstrap file {path:?}: {source}")]
    BootstrapFile {
        /// Path to the file that could not be read or decoded.
        path: std::path::PathBuf,
        /// Underlying I/O or decode error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("invalid leader_imbalance_check_interval = {value}: must be >= 1")]
    InvalidLeaderRebalanceInterval { value: u64 },

    /// Runtime tuning contains an invalid scalar or field relation.
    #[error("invalid runtime configuration: {0}")]
    InvalidRuntimeConfig(String),

    #[error("controlled shutdown did not complete within {0:?}")]
    ShutdownTimeout(std::time::Duration),
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn display_unsupported_api() {
        let e = BrokerError::UnsupportedApi {
            api_key: 7,
            version: 9,
        };
        assert!(e.to_string().contains("api_key=7"));
        assert!(e.to_string().contains("version=9"));
    }
}
