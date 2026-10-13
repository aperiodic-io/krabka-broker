//! The label sets the broker attaches to its metric families, the label
//! values that are closed enums rather than free strings, and the sentinel
//! value that keeps an unbounded input from becoming an unbounded label. They
//! live together because cardinality is a property of the whole set, not of
//! any one family that uses it.

use {
    crate::break_glass::action_name,
    krabka_metadata::BreakGlassAction as GatedAction,
    prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder},
    std::{
        fmt,
        hash::{Hash, Hasher},
        sync::Arc,
    },
};

/// Sentinel label value that folds unbounded inputs (unrecognised
/// `api_key`s, `SaslAuthenticate` without a prior handshake) into one
/// series, keeping label cardinality bounded.
pub(crate) const UNKNOWN_LABEL: &str = "Unknown";

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
///
/// `topic` is an `Arc<str>` and not a `String` because the data path builds
/// one of these per topic per request and then throws it away: `get_or_create`
/// only needs to hash the label to find the counter, and the copy it clones on
/// a first sighting is kept forever. An `Arc<str>` makes the common case a
/// hash plus a refcount bump. The registry hands out the shared name —
/// see `PartitionRegistry::shared_topic_name`. Rendering is unchanged:
/// prometheus-client encodes an `Arc<str>` through the same `&str` impl a
/// `String` reaches.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: Arc<str>,
}

/// Per-partition label set, paired with the `partition_*` and `delivery_*`
/// metric families. Consumed by the rebalancer's metric scraper.
///
/// `metrics::eviction` indexes labels as the data path materialises them and
/// reconciles that index with the current image. A partition's series leave
/// when this broker no longer hosts it, including invented client labels and a
/// straggler write after reassignment. The index retains no tombstones, so
/// cardinality is bounded by currently hosted partitions plus labels created
/// since the most recent bounded reconciliation pass.
///
/// `topic` is an `Arc<str>` for the reason [`TopicLabel::topic`] is: this is
/// the label set the produce, fetch and replication paths build once per
/// partition per request, so the allocation it used to cost was O(partitions)
/// on exactly the path the partition registry was made allocation-free for.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: Arc<str>,
    pub partition: i32,
}

/// Per-follower replica-lag label set, paired with the `replica_lag_records`
/// gauge family. `replica` is the follower's node id, so a partition this
/// broker leads carries one series per follower in its replica set and none
/// for the leader itself.
///
/// Cardinality is bounded by the partitions this broker leads times their
/// replication factor. Two rules hold that bound. The lag sampler republishes
/// the whole set each pass and releases what the pass no longer justifies, so
/// losing leadership or dropping a replica takes the series with it, and
/// `metrics::eviction` releases the rest when the image stops naming this
/// broker in the partition's replica set. No client input reaches this label
/// set: both the partition and the follower come from the leader's own
/// replica state.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReplicaLagLabel {
    pub topic: String,
    pub partition: i32,
    pub replica: u64,
}

/// Per-(group, topic, partition) consumer-group-lag label set, paired with the
/// `consumer_group_lag_records` gauge family. It covers classic and KIP-848
/// groups alike, because a group's committed offsets live on the
/// protocol-agnostic `CoordinatorGroup` whichever protocol its members speak.
///
/// This is the widest label set the broker emits, so its bound is worth
/// stating exactly. A series exists only for a `(topic, partition)` a group
/// this broker coordinates has actually committed an offset for, and only
/// while that partition is still in the metadata image. The commit itself is
/// bounded: `OffsetCommit` writes to `__consumer_offsets`, so a client cannot
/// invent a series more cheaply than it can write a record. Every way a series
/// stops being justified releases it — the group is deleted, this broker stops
/// coordinating the group, the topic is deleted, or the sampler's next pass no
/// longer names the tuple.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConsumerGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// One diskless WAL shard. Topic UUIDs keep delete/recreate cycles distinct.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WalShardLabel {
    pub topic_id: String,
    pub partition: i32,
}

/// One voter in a diskless WAL shard's metadata-selected quorum.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WalVoterLabel {
    pub topic_id: String,
    pub partition: i32,
    pub voter: u64,
}

/// Fleet-complete KIP-932 backlog for one share-group partition.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// One share group, paired with the `share_group_dlq_*` counter families:
/// Kafka's `group` tag on the `DeadLetterQueue*` meters of `ShareGroupMetrics`.
///
/// A series exists only for a group that this broker has made an attempt to
/// dead-letter a record for, and the group has to opt in with
/// `errors.deadletterqueue.topic.name`, so cardinality is bounded by the share
/// groups that use a queue. `BrokerMetrics::evict_share_group_dlq_series`
/// releases a group's series when the group is deleted on this broker as its
/// coordinator, and only then: a broker that stops coordinating the group's
/// offsets partition still leads share partitions of it, and keeps counting. A
/// broker that only led the group's partitions keeps its series until it
/// restarts, as Kafka's per-group meter maps are kept until the broker closes,
/// and so does one whose write was still in flight when the group was deleted.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupIdLabel {
    pub group_id: String,
}

/// KIP-511 client software fingerprint, attached to the
/// `client_software_versions_total` counter on every accepted v3+
/// `ApiVersions` handshake.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientSoftwareLabel {
    pub software_name: String,
    pub software_version: String,
}

/// Per-API request fingerprint, paired with the
/// `api_requests` counter family. `api_key` is the
/// `ApiKey::IntoStaticStr`-derived variant name (e.g. `"Produce"`,
/// `"DescribeQuorum"`) so operators see human-readable api-name
/// labels. A krabka-private api key carries its own RPC name instead
/// (e.g. `"TriggerBarrier"`), because the generated enum does not
/// know that range. Cardinality is bounded by `ApiKey::ALL.len()`
/// (~80 entries) plus one label per krabka-private RPC; requests for
/// unknown api keys land under the `"Unknown"` sentinel label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ApiKeyLabel {
    pub api_key: String,
}

/// Per-barrier-group label set, paired with the `barrier_*` families.
/// `group` is the barrier group name. Cardinality is bounded by
/// `BrokerConfig::barrier_max_groups`, because the coordinator rejects a new
/// group past that cap.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BarrierGroupLabel {
    pub group: String,
}

/// Controller directory identity attached to the current vote.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectoryLabel {
    pub directory_id: String,
}

/// SASL mechanism fingerprint, paired with the
/// `{successful,failed}_authentication_total` counter families.
/// `mechanism` is the canonical Kafka wire name from
/// [`krabka_security::SaslMechanism::wire_name`] (`"PLAIN"`,
/// `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"`, `"GSSAPI"`) when the
/// `SaslAuthenticate` frame arrived in a valid sequence; the
/// `"Unknown"` sentinel covers `ILLEGAL_SASL_STATE` rejects where
/// no prior `SaslHandshake` ran and the mechanism is unset.
/// Cardinality is bounded by `SaslMechanism::*` + 1 — a tight set
/// regardless of client population.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SaslMechanismLabel {
    pub mechanism: String,
}

/// Authorization-denial fingerprint, paired with the
/// `authorization_denied_total` counter family. Both values are the `Debug`
/// spelling of a closed enum -- `krabka_metadata::AclOperation` and
/// `krabka_metadata::ResourceType` -- so the series count is bounded by the
/// product of the two variant sets and no principal, host or resource name
/// reaches this label set.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct AuthorizationDeniedLabel {
    pub operation: String,
    pub resource_type: String,
}

/// KFC-7 rejection fingerprint, paired with the
/// `schema_validation_rejections` counter family. `reason` is one of the five
/// fixed values `unframed`, `unknown_id`, `wrong_subject`, `body_mismatch`,
/// and `registry_unavailable`, so a topic with schema validation on adds at
/// most five series. No schema id, subject, or client string reaches this
/// label set.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SchemaRejectionLabel {
    pub topic: String,
    pub reason: String,
}

/// KFC-9 state of a break-glass proposal.
///
/// The four states are the whole lifecycle of a proposal, so a closed enum
/// bounds the `break_glass_proposals` label set at four series.
///
/// `ALL` lists every state, in lifecycle order.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", all, label_value)]
pub enum BreakGlassState {
    /// The proposal has fewer approvals than the broker needs.
    Pending,
    /// The proposal has every approval it needs and no transition used it yet.
    Approved,
    /// The proposal passed its expiry time and no transition used it.
    Expired,
    /// A privileged transition used the proposal.
    Consumed,
}

/// KFC-9 privileged transition that a break-glass proposal authorizes, as a
/// metric label.
///
/// [`krabka_metadata::BreakGlassAction`] is the one definition of the gated
/// set. This newtype is only what lets that definition *be* a label value:
/// both the enum and `EncodeLabelValue` are foreign to this crate, so the
/// orphan rule forbids implementing the trait on the enum directly.
///
/// The label set stays as closed as a broker-local enum made it. The wrapped
/// enum is itself closed, so the `break_glass_refusals` and
/// `break_glass_bypassed` families still carry one series per gated operation
/// and no caller can name an eighth action. What the newtype adds is that
/// there is now one enum rather than two: an action added to the metadata
/// definition cannot be forgotten here, and `break_glass::action_name` is the
/// one spelling that the configuration, the audit event and the metric label
/// all share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakGlassAction(pub GatedAction);

impl BreakGlassAction {
    /// The `action` label value this transition renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str { action_name(self.0) }
}

/// Hashing the label text rather than the wrapped enum, which carries no
/// `Hash` of its own. Two actions are equal exactly when their names are, so
/// the hash agrees with `Eq`.
impl Hash for BreakGlassAction {
    fn hash<H: Hasher>(&self, state: &mut H) { self.as_str().hash(state); }
}

impl EncodeLabelValue for BreakGlassAction {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// KFC-9 proposal-state label set, paired with the `break_glass_proposals`
/// gauge family. Cardinality is bounded at four, because the field is the
/// closed [`BreakGlassState`] enum and no caller can name a fifth state.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassStateLabel {
    pub state: BreakGlassState,
}

/// KFC-9 privileged-transition label set, paired with the
/// `break_glass_refusals` and `break_glass_bypassed` counter families.
/// Cardinality is bounded at seven, because the field wraps the closed
/// [`krabka_metadata::BreakGlassAction`] enum and no caller can name an eighth
/// action.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassActionLabel {
    pub action: BreakGlassAction,
}

/// Why the broker stopped serving a client connection.
///
/// The four reasons are every way a connection ends on its own — as a peer
/// that stopped talking, or as bytes that are not a request — plus the TLS
/// handshake the peer never drove, so a closed enum bounds the
/// `connection_closes` label set at four series however many connections the
/// broker serves.
///
/// A connection the broker drops because of a request it did read is *not* a
/// fifth reason: each such exit is already one count in another family, and
/// counting it twice would make the two disagree. A request at an
/// unregistered `api_key` is an `api_requests{api_key="Unknown"}`; one whose
/// version is out of range is an `unsupported_api_requests`; one whose
/// handler failed, or whose response would not encode or send, was counted by
/// `api_requests` when it was dispatched; and a rejected `SaslAuthenticate` —
/// including the pre-auth gate's `ILLEGAL_SASL_STATE`, under the mechanism a
/// `SaslHandshake` named or the `Unknown` sentinel when none did — is a
/// `failed_authentication`. What is left over is the broker
/// failing to encode or to send its own answer to a SASL frame, which is a
/// broker fault rather than anything the connection did, and is logged.
///
/// `ALL` lists every reason, in the order this documentation gives them.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", all, label_value)]
pub enum ConnectionCloseReason {
    /// The connection went `connections.max.idle.ms` without a complete frame
    /// — counting a TLS handshake it opened the socket for and never drove,
    /// which Kafka's idle expiry covers for the same reason.
    Idle,
    /// KIP-368: the SASL session passed the token's expiry without an in-band
    /// re-authentication.
    SaslSessionExpired,
    /// The client sent bytes the broker could not read as a request: either
    /// the length-delimited codec refused the frame, or the frame was too
    /// short or too malformed to parse a request header out of.
    DecodeError,
    /// The client closed its end of the connection.
    PeerClosed,
    /// Inbound connection rejected because `max.connections` was reached.
    MaxConnections,
    /// Inbound connection rejected because `max.connections.per.ip` was reached.
    MaxConnectionsPerIp,
}

/// Connection-close label set, paired with the `connection_closes` counter
/// family. Cardinality is bounded at six, because the field is the closed
/// [`ConnectionCloseReason`] enum and no caller can name a seventh reason.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConnectionCloseReasonLabel {
    pub reason: ConnectionCloseReason,
}

/// Why one partition's compaction pass failed, as a metric label.
///
/// The cleaner classifies every `compact_log` error into one of these three,
/// so the `log_cleaner_failures` family carries at most three series per
/// partition however the failure is spelled. The distinction is the one an
/// operator acts on: a storage failure is a disk to replace, and a writer
/// failure is a partition whose actor is gone.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", label_value)]
pub enum CleanerFailureReason {
    /// The log layer returned an `io::Error`. The compaction rewrite, the
    /// swap or an fsync failed against the disk, and the writer arm has
    /// already asked the log-dir registry to take the dir offline.
    Io,
    /// The partition's writer actor was gone, or dropped the acknowledgement
    /// before it answered. Nothing reached the disk.
    Writer,
    /// Any other error the log layer returned, such as a corrupt segment the
    /// rewrite could not read.
    Other,
}

/// Compaction-failure label set, paired with the `log_cleaner_failures`
/// counter family.
///
/// The partition half is bounded exactly as [`PartitionLabel`] is -- the
/// cleaner only ever sweeps partitions this broker leads -- and `reason` is
/// the closed [`CleanerFailureReason`] enum, so a partition that fails every
/// way it can adds three series and no client input reaches the set.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CleanerFailureLabel {
    pub topic: Arc<str>,
    pub partition: i32,
    pub reason: CleanerFailureReason,
}

/// The client quota that caused a throttle the broker applied.
///
/// Kafka splits its quotas the same way, and names them the same way in
/// `kafka.server:type=*QuotaManager`. The five variants are the quotas whose
/// delay this broker *sleeps* on: `producer_byte_rate` (KIP-13),
/// `consumer_byte_rate` (KIP-13), `request_percentage` (KIP-124),
/// `controller_mutation_rate` (KIP-599), which `CreateTopics`,
/// `CreatePartitions` and `DeleteTopics` apply inline once they have assembled
/// their response, and `connection_creation_rate` (KIP-612) on the accept path.
/// Kafka's `LeaderReplication` and `FollowerReplication` quotas are absent
/// because KIP-73 throttles a follower fetch by dropping partitions out of the
/// response rather than by delaying it, so there is no sleep to attribute. The
/// replication throttle is published as its own byte-rate instead:
/// `replication_throttled_bytes_out` and `replication_throttled_bytes_in`,
/// which stand for Kafka's `LeaderReplication` and `FollowerReplication`
/// `byte-rate` sensors, with `replication_throttle_sleeps` for the rounds it
/// held back entirely.
///
/// The `quota_type` label value, `as_str`, is Kafka's own `QuotaType` name, so
/// one dashboard query reads the same against either broker. `ALL` lists
/// every quota the broker applies a throttle for.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(all, label_value)]
pub enum QuotaType {
    /// KIP-13 `producer_byte_rate`, charged on the Produce path.
    Produce,
    /// KIP-13 `consumer_byte_rate`, charged on the Fetch path.
    Fetch,
    /// KIP-124 `request_percentage`, charged on every api by handler time.
    Request,
    /// KIP-599 `controller_mutation_rate`, charged on the topic-mutating
    /// admin apis by the number of partitions the request moves.
    ControllerMutation,
    /// KIP-612 `connection_creation_rate`, charged on the accept path.
    #[enum_str(name = "connection_creation_rate")]
    ConnectionCreation,
}

impl QuotaType {
    /// The dynamic-config key this quota is configured under, as
    /// `AlterClientQuotas` spells it.
    ///
    /// It is the inverse of the key each bucket is created under, and it is
    /// what lets the quota-expiry sweep find the series an expired bucket
    /// published. `None` for a key no quota is charged against.
    #[must_use]
    pub fn from_config_key(key: &str) -> Option<Self> {
        match key {
            "producer_byte_rate" => Some(Self::Produce),
            "consumer_byte_rate" => Some(Self::Fetch),
            "request_percentage" => Some(Self::Request),
            "controller_mutation_rate" => Some(Self::ControllerMutation),
            "connection_creation_rate" => Some(Self::ConnectionCreation),
            _ => None,
        }
    }
}

/// Applied-quota label set, paired with the `quota_throttle_duration_seconds`
/// histogram family. Cardinality is bounded at five, because the field is the
/// closed [`QuotaType`] enum: no principal, client id or topic reaches this
/// label set, so no client can invent a series.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QuotaTypeLabel {
    pub quota_type: QuotaType,
}

/// `KRaft` voter consensus state label set, paired with `raft_current_state`.
/// One-hot encoded across the four mutually exclusive states.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RaftStateLabel {
    pub state: &'static str,
}

impl RaftStateLabel {
    pub const ALL: [Self; 4] = [
        Self::LEADER,
        Self::FOLLOWER,
        Self::CANDIDATE,
        Self::OBSERVER,
    ];
    pub const CANDIDATE: Self = Self { state: "candidate" };
    pub const FOLLOWER: Self = Self { state: "follower" };
    pub const LEADER: Self = Self { state: "leader" };
    pub const OBSERVER: Self = Self { state: "observer" };
}

/// Per-entity client quota label set, paired with
/// `quota_entity_throttle_seconds_total`.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QuotaEntityLabel {
    pub quota_type: QuotaType,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

/// The drain path a fetch response's records regions took to the socket.
///
/// Which one a fetch takes is the whole return on the zero-copy fetch writer,
/// and a regression that quietly routes every fetch onto a copy path moves no
/// other series. These three are the complete set, so a closed enum bounds the
/// `fetch_response_drain` family at three series and no connection can invent
/// a fourth.
///
/// `ALL` lists every path. Registration creates all three series at zero from
/// it, so a dashboard finds them on a broker that has served no fetch yet, and
/// on a target where one of them is unreachable.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", all, label_value)]
pub enum FetchDrainPath {
    /// The kernel `sendfile(2)` moved at least one records region from the
    /// page cache to the socket with no userspace copy. A plaintext or kTLS
    /// connection takes this path for a records run at or above
    /// `sendfile_min`.
    Sendfile,
    /// Every records region left as userspace bytes beside the response
    /// metadata, the portable Increment-C path. Userspace TLS, Windows, a
    /// records run below `sendfile_min`, and a `read_committed` read all land
    /// here.
    Vectored,
    /// The response carried a file-backed records region that the drain had to
    /// `pread` into a buffer, because the stream declared itself
    /// sendfile-capable but would not hand out a socket. It is the fallback
    /// arm *inside* the drain, distinct from [`Self::Vectored`], where the
    /// bytes were already in userspace before the plan was built.
    Pread,
}

/// Drain-path label set, paired with the `fetch_response_drain` counter
/// family. Cardinality is bounded at three, because the field is the closed
/// [`FetchDrainPath`] enum.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FetchDrainPathLabel {
    pub path: FetchDrainPath,
}

#[cfg(test)]
mod tests {
    use {
        super::{PartitionLabel, TopicLabel},
        assert2::assert,
        prometheus_client::{
            encoding::{EncodeLabelSet, text::encode},
            metrics::{counter::Counter, family::Family},
            registry::Registry,
        },
    };

    /// [`TopicLabel`] exactly as it read before its `topic` became an
    /// `Arc<str>`.
    ///
    /// What an operator scrapes must not move, so the previous definition is
    /// kept here as the thing the current one is measured against. It is not
    /// redundant with the frozen bytes in [`CAPTURED`]: prometheus-client
    /// reaches a `String` and an `Arc<str>` through different
    /// `EncodeLabelValue` impls, and this is what says the two agree, on this
    /// version of the crate and on the next one.
    #[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
    struct OwnedTopicLabel {
        topic: String,
    }

    /// [`PartitionLabel`] exactly as it read before its `topic` became an
    /// `Arc<str>`. See [`OwnedTopicLabel`].
    #[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
    struct OwnedPartitionLabel {
        topic: String,
        partition: i32,
    }

    /// One counter sample: the topic and partition it is recorded against and
    /// the count.
    #[derive(Clone, Copy)]
    struct Sample {
        topic: &'static str,
        partition: i32,
        count: u64,
    }

    /// One sample per case.
    ///
    /// The names are deliberately not all well-behaved. The text exposition
    /// format gives `"`, `\` and a newline inside a label value a meaning of
    /// their own, so a rendering difference between the two encodings would
    /// show up there first if it showed up anywhere.
    const SAMPLES: [Sample; 4] = [
        Sample {
            topic: "orders",
            partition: 0,
            count: 3,
        },
        Sample {
            topic: "orders",
            partition: 7,
            count: 11,
        },
        Sample {
            topic: "payments.eu-west-1",
            partition: 2,
            count: 5,
        },
        Sample {
            topic: "quote\"back\\slash\nnewline",
            partition: 1,
            count: 1,
        },
    ];

    /// What the `String`-labelled families rendered, sample by sample, before
    /// `topic` became an `Arc<str>`. Captured from a run of
    /// [`owned_labels_still_render_the_captured_bytes`] against the previous
    /// definitions, which [`OwnedTopicLabel`] and [`OwnedPartitionLabel`]
    /// preserve.
    const CAPTURED: [&str; 4] = [
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"orders\"} 3\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"orders\",partition=\"0\"} 3\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"orders\"} 11\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"orders\",partition=\"7\"} 11\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"payments.eu-west-1\"} 5\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"payments.eu-west-1\",partition=\"2\"} 5\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"quote\"back\\slash\nnewline\"} 1\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"quote\"back\\slash\nnewline\",partition=\"1\"} 1\n\
         # EOF\n",
    ];

    /// Register a topic-labelled and a partition-labelled counter family
    /// under a fresh registry, record one sample against each, and encode.
    ///
    /// One sample per rendering, because a `Family` iterates in hash order
    /// and a two-series rendering is only equal to another up to that order —
    /// which is exactly the kind of "equal enough" this test must not accept.
    fn render<T, P>(
        topic_label: impl Fn(&str) -> T,
        partition_label: impl Fn(&str, i32) -> P,
        sample: Sample,
    ) -> String
    where
        T: EncodeLabelSet + Clone + Eq + std::fmt::Debug + std::hash::Hash + Send + Sync + 'static,
        P: EncodeLabelSet + Clone + Eq + std::fmt::Debug + std::hash::Hash + Send + Sync + 'static,
    {
        let Sample {
            topic,
            partition,
            count,
        } = sample;
        let mut registry = Registry::default();
        let topics = Family::<T, Counter>::default();
        let partitions = Family::<P, Counter>::default();
        registry.register(
            "topic_produce_requests",
            "Produce requests per topic",
            topics.clone(),
        );
        registry.register(
            "partition_bytes_in",
            "Bytes appended per partition",
            partitions.clone(),
        );
        topics.get_or_create(&topic_label(topic)).inc_by(count);
        partitions
            .get_or_create(&partition_label(topic, partition))
            .inc_by(count);
        let mut buf = String::new();
        encode(&mut buf, &registry).expect("encoding into a String cannot fail");
        buf
    }

    /// Render one sample through the current, `Arc<str>`-labelled sets.
    fn render_shared(sample: Sample) -> String {
        render(
            |topic| TopicLabel {
                topic: topic.into(),
            },
            |topic, partition| PartitionLabel {
                topic: topic.into(),
                partition,
            },
            sample,
        )
    }

    /// Render one sample through the previous, `String`-labelled sets.
    fn render_owned(sample: Sample) -> String {
        render(
            |topic| OwnedTopicLabel {
                topic: topic.to_owned(),
            },
            |topic, partition| OwnedPartitionLabel {
                topic: topic.to_owned(),
                partition,
            },
            sample,
        )
    }

    /// The bytes an operator scrapes are the contract, and this change was
    /// meant to move none of them.
    #[test]
    fn shared_topic_labels_render_exactly_as_owned_ones_did() {
        for sample in SAMPLES {
            assert!(render_shared(sample) == render_owned(sample));
        }
    }

    /// [`CAPTURED`] is the previous implementation's output, so it is only
    /// worth comparing against for as long as it stays that. This is what
    /// says it does.
    #[test]
    fn owned_labels_still_render_the_captured_bytes() {
        for (sample, captured) in SAMPLES.into_iter().zip(CAPTURED) {
            assert!(render_owned(sample) == captured);
        }
    }

    /// The acceptance the change was asked for: today's scrape, byte for
    /// byte, out of the `Arc<str>` label sets.
    #[test]
    fn shared_labels_render_the_captured_bytes() {
        for (sample, captured) in SAMPLES.into_iter().zip(CAPTURED) {
            assert!(render_shared(sample) == captured);
        }
    }
}
