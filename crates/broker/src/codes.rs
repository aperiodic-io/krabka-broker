//! Kafka wire-level error codes used by the broker.
//!
//! Per-(topic, partition) response fields use these `i16` values. JVM clients
//! react to specific codes, so a substitution changes client behavior. Every
//! constant in the Kafka range comes from `Errors.values()` in the
//! `kafka-clients` jar of the pinned `apache/kafka:4.3.1` image, and a guard
//! test in the private `tests` module re-checks each one against that extracted
//! table.
//!
//! One fenced block at the end of this file holds the krabka-private codes at
//! 1000 and above. A broker returns those only on a krabka-private api key.

#[cfg(test)]
mod tests;

/// Declares the constants that carry a wire code, and collects them into the
/// named table for the guard test: `KAFKA_RANGE_CODES` for the Apache Kafka
/// codes, `KAFKA_TRUNK_CODES` for the codes Kafka trunk assigns and no Kafka
/// release has yet, and `KRABKA_PRIVATE_CODES` for the krabka-private ones.
///
/// Declaring through the macro is what makes the guard total: a constant that
/// is not written here is not a wire code, and a constant that is written here
/// cannot escape the check against the extracted Kafka table.
macro_rules! wire_codes {
    ($table:ident; $( $(#[$meta:meta])* $name:ident = $value:expr; )*) => {
        $( $(#[$meta])* pub const $name: i16 = $value; )*

        /// Every constant in this block, with the name a guard-test failure
        /// reports.
        #[cfg(test)]
        const $table: &[(&str, i16)] = &[$( (stringify!($name), $name), )*];
    };
}

wire_codes! {
    KAFKA_RANGE_CODES;
    NONE = 0;
    UNKNOWN_SERVER_ERROR = -1;
    OFFSET_OUT_OF_RANGE = 1;
    /// `CORRUPT_MESSAGE` (2): the broker received a record batch whose bytes
    /// are malformed, or whose CRC or magic does not match any supported
    /// format.
    CORRUPT_MESSAGE = 2;
    UNKNOWN_TOPIC_OR_PARTITION = 3;
    /// `LEADER_NOT_AVAILABLE` (5): the partition has no leader this broker can
    /// name. Kafka's `KRaftMetadataCache` sets it on a `Metadata` partition row
    /// whose `leader_id` it is answering as `-1`; a JVM client reads it as
    /// retriable and refreshes its metadata.
    LEADER_NOT_AVAILABLE = 5;
    NOT_LEADER_OR_FOLLOWER = 6;
    REQUEST_TIMED_OUT = 7;
    /// `REPLICA_NOT_AVAILABLE` (9, KIP-113): this broker does not host the
    /// targeted replica. `AlterReplicaLogDirs` and `DescribeLogDirs` return it
    /// when the client names a `(topic, partition)` that the broker does not
    /// own.
    ///
    /// 11 is `STALE_CONTROLLER_EPOCH`, which a JVM `AdminClient` turns into
    /// `ControllerMovedException`, so a replica miss must not use it.
    REPLICA_NOT_AVAILABLE = 9;
    /// `MESSAGE_TOO_LARGE` (10): a produced record or batch is larger than
    /// `max.message.bytes` for the topic, or larger than the broker's
    /// `message.max.bytes`. The JVM maps it to `RecordTooLargeException`,
    /// which no producer retries.
    MESSAGE_TOO_LARGE = 10;
    /// `RECORD_LIST_TOO_LARGE` (18): a produced record set is larger than the
    /// topic's `segment.bytes`. Kafka's `RecordBatchTooLargeException`, which a
    /// JVM producer does not retry.
    RECORD_LIST_TOO_LARGE = 18;
    /// `OFFSET_METADATA_TOO_LARGE` (12): an `OffsetCommit` partition row
    /// carries committed metadata longer than `offset.metadata.max.bytes`.
    OFFSET_METADATA_TOO_LARGE = 12;
    /// `KAFKA_STORAGE_ERROR` (56, KIP-113): a log-dir-level I/O failure on
    /// open, rename, remove, append, or read, or a concurrent move with a
    /// conflicting target.
    KAFKA_STORAGE_ERROR = 56;
    /// `LOG_DIR_NOT_FOUND` (57, KIP-113): the destination directory in an
    /// `AlterReplicaLogDirs` request is not one of this broker's configured
    /// `log.dirs`.
    LOG_DIR_NOT_FOUND = 57;
    /// `NETWORK_EXCEPTION` (13): the server disconnected before a response
    /// arrived. A broker reports it for an inter-broker call it could not
    /// complete, as Kafka's `AddPartitionsToTxnManager` does.
    NETWORK_EXCEPTION = 13;
    COORDINATOR_NOT_AVAILABLE = 15;
    /// `COORDINATOR_LOAD_IN_PROGRESS` (14, KIP-848): the group coordinator is
    /// still loading state from `__consumer_offsets`. Clients should retry
    /// after a brief back-off.
    COORDINATOR_LOAD_IN_PROGRESS = 14;
    NOT_COORDINATOR = 16;
    INVALID_TOPIC_EXCEPTION = 17;
    /// `ILLEGAL_SASL_STATE` (34): the broker received a request on a SASL
    /// listener before the connection completed `SaslHandshake` and
    /// `SaslAuthenticate`, or in the wrong order. The broker closes the
    /// connection after it emits this code.
    ILLEGAL_SASL_STATE = 34;
    /// `UNSUPPORTED_SASL_MECHANISM` (33): the client requested a SASL
    /// mechanism the broker does not offer on this listener.
    UNSUPPORTED_SASL_MECHANISM = 33;
    UNSUPPORTED_VERSION = 35;
    /// `SASL_AUTHENTICATION_FAILED` (58) — the supplied credentials were
    /// rejected.
    SASL_AUTHENTICATION_FAILED = 58;
    STALE_BROKER_EPOCH = 77;
    BROKER_ID_NOT_REGISTERED = 102;
    DUPLICATE_BROKER_REGISTRATION = 101;
    TOPIC_ALREADY_EXISTS = 36;
    INVALID_PARTITIONS = 37;
    INVALID_REPLICATION_FACTOR = 38;
    NOT_CONTROLLER = 41;
    /// `LISTENER_NOT_FOUND` (72): the leader of a partition is registered but
    /// has no endpoint on the listener the request arrived on. `Metadata` v6
    /// and later answer it beside a `-1` leader; earlier versions answer
    /// `LEADER_NOT_AVAILABLE`.
    LISTENER_NOT_FOUND = 72;
    /// `TOPIC_DELETION_DISABLED` (73): `delete.topic.enable` is `false`, so
    /// `DeleteTopics` v3 and later refuse every topic.
    TOPIC_DELETION_DISABLED = 73;
    /// `INVALID_REQUEST` (42): the request is structurally or semantically
    /// unacceptable. It is also the code Kafka returns for a resource type
    /// that an `AlterConfigs` or `IncrementalAlterConfigs` broker does not
    /// support, because the protocol assigns no distinct code for that.
    INVALID_REQUEST = 42;
    /// `INVALID_REGULAR_EXPRESSION` (128): a request contains a malformed
    /// regex.
    INVALID_REGULAR_EXPRESSION = 128;
    /// `REBOOTSTRAP_REQUIRED` (129, KIP-1242): the client connected using
    /// stale cluster or node metadata and must bootstrap again.
    REBOOTSTRAP_REQUIRED = 129;
    /// `INVALID_RECORD` (87): a record-bearing payload is structurally
    /// malformed. Examples include an invalid Produce `MessageSet` and a
    /// `PushTelemetry` payload that cannot be decompressed or decoded as OTLP
    /// metrics.
    INVALID_RECORD = 87;

    // Group coordinator codes.
    ILLEGAL_GENERATION = 22;
    INCONSISTENT_GROUP_PROTOCOL = 23;
    /// `INVALID_GROUP_ID` (24): the request named the empty string as its
    /// `group_id`. Kafka's `GroupCoordinatorService` checks this before any
    /// group lookup.
    INVALID_GROUP_ID = 24;
    UNKNOWN_MEMBER_ID = 25;
    /// `INVALID_SESSION_TIMEOUT` (26): a `JoinGroup` session timeout outside
    /// `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
    INVALID_SESSION_TIMEOUT = 26;
    REBALANCE_IN_PROGRESS = 27;
    /// `INVALID_TIMESTAMP` (32): a producer supplied a timestamp type or value
    /// that the broker cannot accept for the target topic.
    INVALID_TIMESTAMP = 32;
    MEMBER_ID_REQUIRED = 79;
    /// `GROUP_MAX_SIZE_REACHED` (81): a new member cannot join because the
    /// group already has its configured maximum number of members.
    GROUP_MAX_SIZE_REACHED = 81;
    /// `UNSTABLE_OFFSET_COMMIT` (88, KIP-447): `OffsetFetch` with
    /// `require_stable` found an offset that an in-flight transaction has not
    /// yet resolved. The JVM consumer retries.
    UNSTABLE_OFFSET_COMMIT = 88;

    // Idempotent-producer codes.
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
    /// `INVALID_PRODUCER_EPOCH` (47): the producer's epoch does not match the
    /// coordinator's current epoch, or no transaction state exists for the
    /// given (`transactional_id`, `producer_id`) pair. The Rust producer
    /// client maps this code to `ProducerError::FencedProducer`.
    INVALID_PRODUCER_EPOCH = 47;
    /// `INVALID_PRODUCER_ID_MAPPING` (49) — the transactional id is not mapped
    /// to the supplied producer id.
    INVALID_PRODUCER_ID_MAPPING = 49;
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53;

    // Transactional protocol codes.
    INVALID_TXN_STATE = 48;
    /// `INVALID_TRANSACTION_TIMEOUT` (50): the `InitProducerId` request asked
    /// for a transaction timeout that is not positive, or that is above
    /// `transaction.max.timeout.ms`.
    INVALID_TRANSACTION_TIMEOUT = 50;
    CONCURRENT_TRANSACTIONS = 51;
    /// `TRANSACTION_COORDINATOR_FENCED` (52): the marker came from an older
    /// transaction-coordinator generation than the partition has observed.
    TRANSACTION_COORDINATOR_FENCED = 52;
    /// `PRODUCER_FENCED` (90, KIP-360): the request carried a
    /// `(producer_id, producer_epoch)` pair that the transaction coordinator
    /// no longer recognises as the transactional id's live producer. A JVM
    /// client raises `ProducerFencedException`, which is fatal to that
    /// producer instance.
    PRODUCER_FENCED = 90;
    /// `TRANSACTION_ABORTABLE` (120, KIP-890) — the operation failed but the
    /// transaction can still be aborted by the client; e.g.
    /// `AddPartitionsToTxn` verify-only found a partition that is not part of
    /// the ongoing transaction.
    TRANSACTION_ABORTABLE = 120;

    /// `FENCED_INSTANCE_ID` (82, KIP-345): another client is currently pinned
    /// to the same `group.instance.id`. The losing client must exit. The
    /// broker fences it across `JoinGroup`, `SyncGroup`, `Heartbeat`,
    /// `OffsetCommit`, `TxnOffsetCommit`, and `LeaveGroup`.
    FENCED_INSTANCE_ID = 82;

    /// `STALE_MEMBER_EPOCH` (113, KIP-848): the supplied member epoch is older
    /// than the coordinator's current epoch for the consumer group member.
    STALE_MEMBER_EPOCH = 113;
    /// `FENCED_MEMBER_EPOCH` (110, KIP-848): the supplied member epoch is
    /// newer than the coordinator's. The consumer must rejoin from epoch 0.
    FENCED_MEMBER_EPOCH = 110;
    /// `UNSUPPORTED_ASSIGNOR` (112, KIP-848): the requested `server_assignor`
    /// is not enabled on this broker.
    UNSUPPORTED_ASSIGNOR = 112;
    /// `UNRELEASED_INSTANCE_ID` (111, KIP-848 + KIP-345): the static
    /// `instance_id` is still bound to a live member of the group.
    UNRELEASED_INSTANCE_ID = 111;
    /// `MISMATCHED_ENDPOINT_TYPE` (114, KIP-919): the request reached a broker
    /// endpoint while asking for controllers, or vice versa.
    MISMATCHED_ENDPOINT_TYPE = 114;
    /// `UNSUPPORTED_ENDPOINT_TYPE` (115, KIP-919): the request named an
    /// `endpoint_type` this broker does not recognize (neither `BROKER` nor
    /// `CONTROLLER`).
    UNSUPPORTED_ENDPOINT_TYPE = 115;
    /// `UNKNOWN_SUBSCRIPTION_ID` (117, KIP-848): the coordinator did not find
    /// the consumer's persisted subscription identifier.
    UNKNOWN_SUBSCRIPTION_ID = 117;

    /// `INVALID_RECORD_STATE` (121, KIP-932): an acknowledgement targeted a
    /// record no longer Acquired.
    INVALID_RECORD_STATE = 121;
    /// `SHARE_SESSION_NOT_FOUND` (122, KIP-932): the named share session does
    /// not exist.
    SHARE_SESSION_NOT_FOUND = 122;
    /// `INVALID_SHARE_SESSION_EPOCH` (123, KIP-932): the share session epoch
    /// did not match the broker's expectation.
    INVALID_SHARE_SESSION_EPOCH = 123;
    /// `FENCED_STATE_EPOCH` (124, KIP-932): the share coordinator fenced a
    /// write on a stale state epoch.
    FENCED_STATE_EPOCH = 124;
    /// `STREAMS_INVALID_TOPOLOGY` (130, KIP-1071): the topology of a
    /// `StreamsGroupHeartbeat` cannot be configured, for example because a
    /// repartition source topic is never a sink topic.
    STREAMS_INVALID_TOPOLOGY = 130;
    /// `STREAMS_INVALID_TOPOLOGY_EPOCH` (131, KIP-1071): the topology epoch of
    /// a `StreamsGroupHeartbeat` is not valid.
    STREAMS_INVALID_TOPOLOGY_EPOCH = 131;
    /// `STREAMS_TOPOLOGY_FENCED` (132, KIP-1071): the topology of the member
    /// is older than the topology of the group.
    STREAMS_TOPOLOGY_FENCED = 132;
    /// `SHARE_SESSION_LIMIT_REACHED` (133, KIP-932): the per-broker share
    /// session cache is full. 133 is the last code the pinned image assigns.
    SHARE_SESSION_LIMIT_REACHED = 133;

    // Admin handler codes.
    /// `INVALID_CONFIG` (40): a config key/value pair is invalid or unknown.
    INVALID_CONFIG = 40;
    /// `NON_EMPTY_GROUP` (68): the group still has live members, so the broker
    /// cannot delete it.
    NON_EMPTY_GROUP = 68;
    /// `GROUP_ID_NOT_FOUND` (69): no group with the given id exists.
    GROUP_ID_NOT_FOUND = 69;
    /// `GROUP_SUBSCRIBED_TO_TOPIC` (86, KIP-496): `OffsetDelete` refused the
    /// request because the live consumer-protocol group still subscribes to
    /// the topic. The operator must stop the consumers first.
    GROUP_SUBSCRIBED_TO_TOPIC = 86;

    // `AlterUserScramCredentials` (KIP-554) result codes.
    /// `CLUSTER_AUTHORIZATION_FAILED` (31): the principal has no cluster-level
    /// authorization. The `AlterUserScramCredentials` handler returns it in
    /// place of an ACL check when the request principal is not the configured
    /// super-user.
    CLUSTER_AUTHORIZATION_FAILED = 31;
    /// `RESOURCE_NOT_FOUND` (91): the resource that the request names does not
    /// exist. KIP-554 uses it for missing SCRAM credentials, in both Alter
    /// deletion rows and Describe per-user result rows.
    RESOURCE_NOT_FOUND = 91;
    /// `UNACCEPTABLE_CREDENTIAL` (93): per-user error for an upsertion that
    /// carries invalid SCRAM parameters, such as iterations below 4096, too
    /// many iterations, or an empty username.
    UNACCEPTABLE_CREDENTIAL = 93;
    /// `DUPLICATE_RESOURCE` (92): per-user error when the same user appears
    /// twice in one `AlterUserScramCredentials` or
    /// `DescribeUserScramCredentials` request.
    DUPLICATE_RESOURCE = 92;

    /// `INVALID_UPDATE_VERSION` (95, KIP-584): a feature-level update in
    /// `UpdateFeatures` is outside the broker's supported range, or it tries
    /// an unguarded downgrade or deletion of a finalized feature.
    INVALID_UPDATE_VERSION = 95;

    /// `FEATURE_UPDATE_FAILED` (96, KIP-584): the cluster failed to persist a
    /// validated feature update. For example, Raft rejected the metadata
    /// write, or the write timed out.
    FEATURE_UPDATE_FAILED = 96;

    /// `PRINCIPAL_DESERIALIZATION_FAILURE` (97, KIP-590): the controller could
    /// not read the `request_principal` a forwarding node put in an
    /// `Envelope`. `kafka.server.EnvelopeUtils` raises it whenever the
    /// configured `KafkaPrincipalSerde` throws, which covers an absent
    /// principal, a schema version outside the supported range, and a
    /// truncated `DefaultPrincipalData` body.
    PRINCIPAL_DESERIALIZATION_FAILURE = 97;

    // KIP-853 voter-reconfiguration codes. Kafka assigns exactly three, at
    // 125 to 127; there is no separate code for a malformed voter update.
    /// `INVALID_VOTER_KEY` (125, KIP-853): a raft RPC carried a voter key
    /// whose replica id or directory id does not match the local replica.
    /// `KafkaRaftClient` returns it from `Vote`, `BeginQuorumEpoch`, and
    /// `EndQuorumEpoch`, which krabka's raft layer serves without it today.
    INVALID_VOTER_KEY = 125;
    /// `DUPLICATE_VOTER` (126, KIP-853): the requested node id is already
    /// present in the voter set.
    DUPLICATE_VOTER = 126;
    /// `VOTER_NOT_FOUND` (127, KIP-853): the exact node/directory voter
    /// identity was not found.
    VOTER_NOT_FOUND = 127;

    // ACL authorization codes.
    /// `TOPIC_AUTHORIZATION_FAILED` (29): principal lacks permission on the
    /// topic.
    TOPIC_AUTHORIZATION_FAILED = 29;
    /// `GROUP_AUTHORIZATION_FAILED` (30): principal lacks permission on the
    /// group.
    GROUP_AUTHORIZATION_FAILED = 30;
    /// `SECURITY_DISABLED` (54): the cluster runs without an authorizer, so
    /// the ACL administration RPCs -- `DescribeAcls`, `CreateAcls` and
    /// `DeleteAcls` -- have nothing to read or write. Kafka's `KafkaApis`
    /// answers all three with this code whenever `authorizer.class.name` is
    /// unset, rather than with an empty listing or a durable binding no
    /// decision point will ever consult.
    SECURITY_DISABLED = 54;
    /// `OPERATION_NOT_ATTEMPTED` (55): a batch failed on another item, so
    /// this one was not tried. `AddPartitionsToTxn` puts it on every valid
    /// partition of a transaction that names an unauthorized or unknown
    /// partition.
    OPERATION_NOT_ATTEMPTED = 55;

    // Bulletproof EOS / acks=all codes.
    /// `NOT_ENOUGH_REPLICAS` (19): per-partition error that `acks=all` Produce
    /// returns when the request completes without enough in-sync replicas that
    /// confirm the write. The record is durably on the leader's log. The
    /// producer should retry.
    NOT_ENOUGH_REPLICAS = 19;

    /// `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20): per-partition error that
    /// `acks=all` Produce returns when the append succeeded on the leader but
    /// the HW timeout elapsed before enough in-sync replicas confirmed the
    /// write. The record is durably on the leader's log, but not yet on every
    /// ISR follower.
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;

    /// `INVALID_REQUIRED_ACKS` (21): a Produce request's `acks` field held a
    /// value other than 0, 1 or -1. `ReplicaManager.isValidRequiredAcks`
    /// admits only those three; every other value refuses every partition of
    /// the request with this code, and appends nothing.
    INVALID_REQUIRED_ACKS = 21;

    /// `FENCED_LEADER_EPOCH` (74, KIP-101): caller's `current_leader_epoch` is
    /// older than the partition's current `leader_epoch`. The caller should
    /// re-fetch metadata, or call `OffsetForLeaderEpoch` to learn the
    /// truncation point.
    FENCED_LEADER_EPOCH = 74;

    /// `UNKNOWN_LEADER_EPOCH` (75, KIP-101): caller's `current_leader_epoch`
    /// is newer than the broker's view. This is metadata propagation lag. The
    /// caller retries after a brief wait.
    UNKNOWN_LEADER_EPOCH = 75;

    /// `OFFSET_NOT_AVAILABLE` (78, KIP-207): a `ListOffsets` v5-and-up request
    /// asked for `LATEST`, or resolved a sentinel to an offset at or above the
    /// bound, while the new leader's high watermark has not yet caught up to
    /// the start offset of its own epoch. Answering with a real offset here
    /// could later move backwards once the watermark advances, which would
    /// let a client observe a non-monotonic end of partition.
    /// `Partition.maybeOffsetsError` raises it; a v1-v4 request gets
    /// `LEADER_NOT_AVAILABLE` (5) for the same condition instead, because the
    /// dedicated code did not exist yet.
    OFFSET_NOT_AVAILABLE = 78;

    /// `TRANSACTIONAL_ID_NOT_FOUND` (105, KIP-664): `DescribeTransactions`
    /// named a transactional id this broker's coordinator holds no entry
    /// for, or one whose entry is `Dead`.
    TRANSACTIONAL_ID_NOT_FOUND = 105;

    /// `INELIGIBLE_REPLICA` (107, KIP-903): an `AlterPartition` proposed a new
    /// ISR that holds at least one ineligible replica. Such a replica is a
    /// broker that is not currently registered, or one whose stamped broker
    /// epoch is stale against the controller's registration epoch. The
    /// partition's ISR does not change.
    INELIGIBLE_REPLICA = 107;

    /// `OFFSET_MOVED_TO_TIERED_STORAGE` (109, KIP-405): a *follower* asked its
    /// leader for an offset that the leader's local log no longer holds but
    /// the remote tier does. Kafka's
    /// `ReplicaManager.handleOffsetOutOfRangeError` answers a follower fetch
    /// in `[logStartOffset, localLogStartOffset)` with this code instead of
    /// streaming the whole archive back down the replication path. The
    /// follower reads the leader's local log start with
    /// `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)` and restarts its log there.
    ///
    /// A consumer never sees it: the same band is served to a consumer out of
    /// the tier, so only a fetch that carries a replica id is answered this
    /// way.
    OFFSET_MOVED_TO_TIERED_STORAGE = 109;

    // Leader election codes.
    PREFERRED_LEADER_NOT_AVAILABLE = 80;
    ELIGIBLE_LEADERS_NOT_AVAILABLE = 83;
    ELECTION_NOT_NEEDED = 84;

    // Partition reassignment codes (KIP-455).
    INVALID_REPLICA_ASSIGNMENT = 39;
    NO_REASSIGNMENT_IN_PROGRESS = 85;

    // KIP-227 incremental-fetch-session codes.
    /// `FETCH_SESSION_ID_NOT_FOUND` (70): the broker returns this at the top
    /// level of a `FetchResponse` when the request carried a non-zero
    /// `session_id` that the broker's session cache does not hold. The session
    /// was evicted, never existed, or is already closed.
    FETCH_SESSION_ID_NOT_FOUND = 70;
    /// `INVALID_FETCH_SESSION_EPOCH` (71): the broker returns this at the top
    /// level of a `FetchResponse` when the request's `session_epoch` does not
    /// match the cached session's current epoch. It also returns it when
    /// `session_id == 0` and `session_epoch` is neither `0` (new session) nor
    /// `-1` (sessionless full fetch).
    INVALID_FETCH_SESSION_EPOCH = 71;
    /// `FETCH_SESSION_TOPIC_ID_ERROR` (106): the broker returns this at the
    /// top level of a `FetchResponse` when an incremental request's version
    /// does not match the topic-id use of its session: v12 or earlier on a
    /// session a v13+ request created, or the other way round.
    FETCH_SESSION_TOPIC_ID_ERROR = 106;

    // KIP-48 delegation-token codes.
    DELEGATION_TOKEN_AUTH_DISABLED = 61;
    DELEGATION_TOKEN_NOT_FOUND = 62;
    DELEGATION_TOKEN_OWNER_MISMATCH = 63;
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64;
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65;
    DELEGATION_TOKEN_EXPIRED = 66;
    /// `INVALID_PRINCIPAL_TYPE` (67): a `CreateDelegationToken` renewer whose
    /// principal type is not `User`.
    INVALID_PRINCIPAL_TYPE = 67;

    // KIP-630 FetchSnapshot (api_key 59) codes.
    /// `SNAPSHOT_NOT_FOUND` (98): the requested `__cluster_metadata` snapshot
    /// does not exist, because the controller has not generated one yet.
    SNAPSHOT_NOT_FOUND = 98;
    /// `POSITION_OUT_OF_RANGE` (99): the requested `position` is past the end
    /// of the `__cluster_metadata` snapshot.
    POSITION_OUT_OF_RANGE = 99;
    /// `INCONSISTENT_CLUSTER_ID` (104): the request's `cluster_id` does not
    /// match this cluster's id.
    INCONSISTENT_CLUSTER_ID = 104;
    /// `UNKNOWN_CONTROLLER_ID` (116): Kafka's "This controller ID is not
    /// known."
    UNKNOWN_CONTROLLER_ID = 116;
    /// `INVALID_REGISTRATION` (119): a broker/controller registration is
    /// malformed.
    INVALID_REGISTRATION = 119;
    /// `UNKNOWN_TOPIC_ID` (100): a request referenced a topic by UUID that
    /// this cluster does not know about (KIP-516).
    UNKNOWN_TOPIC_ID = 100;
    /// `INCONSISTENT_TOPIC_ID` (103): a request supplied a topic UUID that
    /// does not match the UUID stored for the named topic (KIP-516).
    INCONSISTENT_TOPIC_ID = 103;

    /// `UNSUPPORTED_COMPRESSION_TYPE` (76): a KIP-714 `PushTelemetry` carried
    /// a `compression_type` that the broker cannot decompress, or a `Produce`
    /// batch below v7 used zstd, which `ProduceRequest.validateRecords`
    /// refuses because a client that old is not assumed to be able to decode
    /// zstd back out of a fetch.
    UNSUPPORTED_COMPRESSION_TYPE = 76;

    /// `THROTTLING_QUOTA_EXCEEDED` (89): a KIP-714 client pushed or fetched
    /// telemetry faster than the configured interval allows.
    THROTTLING_QUOTA_EXCEEDED = 89;

    /// `TELEMETRY_TOO_LARGE` (118): KIP-714 `PushTelemetry` payload exceeded
    /// `telemetry.max.bytes`.
    TELEMETRY_TOO_LARGE = 118;

    /// `POLICY_VIOLATION` (44): the request parameters do not satisfy the
    /// configured policy. Apache Kafka returns it from `CreateTopicPolicy` and
    /// `AlterConfigPolicy`.
    ///
    /// The JVM maps 44 to `PolicyViolationException`, which extends
    /// `ApiException` and not `RetriableException`, so no client retries it.
    ///
    /// KFC-9 returns this code for a produce to a frozen topic, and for a
    /// privileged transition that carries no break-glass approval.
    POLICY_VIOLATION = 44;
}

// ---------------------------------------------------------------------------
// Kafka trunk codes.
//
// Kafka trunk assigns these above the 133 the pinned `apache/kafka:4.3.1`
// image ends at. They serve the trunk-only api keys krabka implements because
// krabka-protocol vendors their schemas, and a guard test checks them against
// the trunk `Errors` table recorded in the private `tests` module.
// ---------------------------------------------------------------------------

wire_codes! {
    KAFKA_TRUNK_CODES;
    /// `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED` (135, KIP-1331): the
    /// broker could not store a `StreamsGroupTopologyDescriptionUpdate`.
    STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED = 135;
    /// `CONTROLLER_ID_NOT_REGISTERED` (136, KIP-1312): `UnregisterController`
    /// named a controller id that has no registration.
    CONTROLLER_ID_NOT_REGISTERED = 136;
}

// ---------------------------------------------------------------------------
// krabka-private error codes.
//
// The Apache Kafka table in the pinned image ends at 133
// (`SHARE_SESSION_LIMIT_REACHED`), Kafka trunk's at 136, and Kafka assigns
// codes upward from 0, so krabka reserves 1000 and above for errors that no
// Kafka error table names. The gap below 1000 is what keeps the two ranges
// apart as Kafka adds codes. A broker returns a code in this range only on a krabka-private api
// key, which sits at or above `crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR`.
// A JVM client cannot negotiate such an api key, so it never receives one of
// these codes.
//
// The private error-code range and the private api-key range are separate
// namespaces that share the floor 1000 and nothing else. One number can be a
// legal error code and a legal api key at the same time. 1010 is
// `OPERATOR_SIGNATURE_REQUIRED` here and `AlterBarrierGroups` in
// `crate::handlers`. That is not a collision, because an error code and an api
// key are different fields on the wire.
// ---------------------------------------------------------------------------

wire_codes! {
    KRABKA_PRIVATE_CODES;
    /// `BARRIER_INJECTION_IN_PROGRESS` (1000): an injection for this barrier
    /// group is already in flight. The caller should retry after a brief
    /// back-off.
    ///
    /// The barrier coordinator runs one injection per group at a time, because
    /// an injection freezes the target set and consumes an epoch before it
    /// appends the first marker. A second `TriggerBarrier` for the same group
    /// gets this code instead of a second epoch.
    ///
    /// Kafka has no generic operation-in-progress code.
    /// `CONCURRENT_TRANSACTIONS` (51) and `REBALANCE_IN_PROGRESS` (27) both
    /// drive JVM client state machines, so neither one carries this meaning.
    /// The broker returns this code only on a krabka-private api key, and it
    /// never reaches a JVM client.
    BARRIER_INJECTION_IN_PROGRESS = 1000;

    /// `BREAK_GLASS_APPROVAL_REQUIRED` (1006): the request names a privileged
    /// transition that needs an approved break-glass proposal, and no such
    /// proposal exists.
    ///
    /// KFC-9 gates these transitions on an approved proposal: a thaw of a
    /// topic write-freeze, a forced leader epoch bump, a broker
    /// unregistration, a reassignment cancel, and a delete of a topic or of
    /// records. The operator should collect the approvals first, then send the
    /// request again.
    BREAK_GLASS_APPROVAL_REQUIRED = 1006;

    /// `BREAK_GLASS_DUPLICATE_APPROVER` (1007): the principal on the
    /// connection already approved this break-glass proposal.
    ///
    /// A two-person rule counts distinct principals. A second approval from
    /// one principal does not move the proposal closer to its threshold, so
    /// the broker refuses it instead of storing it.
    BREAK_GLASS_DUPLICATE_APPROVER = 1007;

    /// `BREAK_GLASS_NOT_AN_APPROVER` (1008): the principal on the connection
    /// is not in the configured break-glass approver set.
    ///
    /// The approver set comes from `broker.toml` and not from the metadata
    /// log, so a principal who can write the metadata log cannot add an
    /// approver. The ACL store keeps `super_users` out for the same reason.
    BREAK_GLASS_NOT_AN_APPROVER = 1008;

    /// `OPERATOR_SIGNATURE_INVALID` (1009): the broker refused the detached
    /// operator signature on the request.
    ///
    /// One code covers five distinct failures:
    ///
    /// - The signature does not verify against the named key.
    /// - The `key_id` names no configured operator key.
    /// - The `set_by` principal is not the principal bound to that key.
    /// - The timestamp sits outside the configured skew window.
    /// - The timestamp repeats one the broker already accepted.
    ///
    /// The response message names which check failed. The code does not,
    /// because an error code that separates the five tells an attacker which
    /// check they failed.
    ///
    /// The freeze path and the break-glass path share this code, because both
    /// verify against one operator key set with one set of rules.
    OPERATOR_SIGNATURE_INVALID = 1009;

    /// `OPERATOR_SIGNATURE_REQUIRED` (1010): the request needs a detached
    /// operator signature and carries none.
    ///
    /// A thaw of a topic write-freeze always needs one. A freeze needs one
    /// when `freeze.require_signature` is `true`. The freeze path and the
    /// break-glass path share this code.
    OPERATOR_SIGNATURE_REQUIRED = 1010;

    /// `FREEZE_SCOPE_INVALID` (1011): the freeze scope in the request is not
    /// one the broker accepts.
    ///
    /// The scope is empty, it is not a legal topic name, or it names an
    /// internal topic. The broker never freezes a `__` name, because a prefix
    /// scope of `""` would otherwise freeze `__consumer_offsets` and stop the
    /// cluster.
    FREEZE_SCOPE_INVALID = 1011;

    /// `FREEZE_LIMIT_EXCEEDED` (1012): the freeze registry already holds
    /// `freeze.max_entries` entries.
    ///
    /// The produce path resolves a prefix scope with a reverse walk over the
    /// prefixed entries, so a bounded registry bounds that walk. The operator
    /// should remove an entry before they add another one.
    FREEZE_LIMIT_EXCEEDED = 1012;
}

/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`:
/// a verification that could not reach the coordinator is a retriable load
/// (`NETWORK_EXCEPTION` becomes `COORDINATOR_LOAD_IN_PROGRESS`, which asks
/// the client to retry without a coordinator lookup), an unavailable topic
/// or replica set is `COORDINATOR_NOT_AVAILABLE`, and a lost leadership is
/// `NOT_COORDINATOR`.
#[must_use]
pub(crate) fn coordinator_operation_code(code: i16) -> i16 {
    match code {
        NETWORK_EXCEPTION => COORDINATOR_LOAD_IN_PROGRESS,
        UNKNOWN_TOPIC_OR_PARTITION | NOT_ENOUGH_REPLICAS | REQUEST_TIMED_OUT => {
            COORDINATOR_NOT_AVAILABLE
        }
        NOT_LEADER_OR_FOLLOWER | KAFKA_STORAGE_ERROR => NOT_COORDINATOR,
        other => other,
    }
}

/// Maps an internal [`crate::error::BrokerError`] to a wire-level code. Most
/// internal errors map to `UNKNOWN_SERVER_ERROR`. Specific variants map to
/// more meaningful codes.
#[must_use]
pub fn from_broker_error(err: &crate::error::BrokerError) -> i16 {
    use crate::error::BrokerError;
    match err {
        // KFC-1 `delivery.schedule.monotonic`. The log refuses a batch whose
        // delivery time precedes one the partition already holds, and the
        // refusal is a statement about the batch's timestamp, not a server
        // fault: the produce row carries the same code the produce-side
        // `delivery.max.delay.ms` bound answers with.
        BrokerError::Log(krabka_log::LogError::ScheduleRunsBackwards { .. }) => INVALID_TIMESTAMP,
        // Kafka trunk's `max.decompressed.message.bytes`: the
        // `InvalidRecordException` a decompressing read throws for a record
        // above the limit is `INVALID_RECORD` under `Errors.forException`.
        BrokerError::Log(krabka_log::LogError::RecordTooLarge { .. }) => INVALID_RECORD,
        // Kafka's `LocalLog.maybeHandleIOException` turns every `IOException`
        // of an append or a read into `KafkaStorageException`, which
        // `Errors.forException` answers with `KAFKA_STORAGE_ERROR`: retriable,
        // and it makes the client refresh its metadata. Left to the catch-all
        // below, a disk fault is `UNKNOWN_SERVER_ERROR`, which the JVM
        // producer never retries.
        BrokerError::Log(krabka_log::LogError::Io(_)) => KAFKA_STORAGE_ERROR,
        BrokerError::UnsupportedApi { .. } => UNSUPPORTED_VERSION,
        // A raft write that reached a node which is no longer the leader is
        // the same answer to a client as a partition whose writer is gone:
        // retry, after refreshing metadata.
        BrokerError::PartitionWriterDied { .. }
        | BrokerError::Raft(krabka_raft::RaftError::NotLeader { .. }) => NOT_LEADER_OR_FOLLOWER,
        BrokerError::Raft(krabka_raft::RaftError::Network(_)) => REQUEST_TIMED_OUT,
        BrokerError::GroupInvalidState { .. } => REBALANCE_IN_PROGRESS,
        BrokerError::UnknownMember { .. } => UNKNOWN_MEMBER_ID,
        BrokerError::GenerationMismatch { .. } => ILLEGAL_GENERATION,
        BrokerError::ProducerEpochFenced { .. }
        | BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::StaleProducerEpoch,
        ) => INVALID_PRODUCER_EPOCH,
        BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::InvalidTransactionState,
        ) => INVALID_TXN_STATE,
        BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::OutOfOrderSequence,
        ) => OUT_OF_ORDER_SEQUENCE_NUMBER,
        BrokerError::CoordinatorEpochFenced { .. } => TRANSACTION_COORDINATOR_FENCED,
        BrokerError::ProducerIdBlockUnavailable(_) => COORDINATOR_LOAD_IN_PROGRESS,
        BrokerError::FencedLeaderEpoch { .. } => FENCED_LEADER_EPOCH,
        BrokerError::UnknownLeaderEpoch(_) => UNKNOWN_LEADER_EPOCH,
        BrokerError::CoordinatorWriteUncommitted { code, .. }
        | BrokerError::TransactionStateWriteUncommitted { code, .. } => *code,
        BrokerError::Replication(_)
        | BrokerError::Shutdown
        | BrokerError::Io(_)
        | BrokerError::Log(_)
        | BrokerError::Protocol(_)
        | BrokerError::Startup(_)
        | BrokerError::FatalFault(_)
        | BrokerError::Txn(_)
        | BrokerError::MarkerWriteRefused { .. }
        | BrokerError::Share(_)
        | BrokerError::SharePartitionState { .. }
        | BrokerError::ListenerConflict { .. }
        | BrokerError::InvalidInterBrokerListener { .. }
        | BrokerError::EmptyRoles
        | BrokerError::NonControllerIsVoter { .. }
        | BrokerError::ControllerOnlyNodeListeners { .. }
        | BrokerError::WitnessRequiresBrokerRole
        | BrokerError::WitnessRequiresControllerRole
        | BrokerError::StretchProfileNeedsThreeSites { .. }
        | BrokerError::StretchProfileDuplicateSite { .. }
        | BrokerError::StretchWitnessSiteUnknown { .. }
        | BrokerError::StretchPreferredSiteUnknown { .. }
        | BrokerError::StretchPreferredSiteIsWitness { .. }
        | BrokerError::StretchRequiresRack
        | BrokerError::StretchRackNotInProfile { .. }
        | BrokerError::StretchWitnessSiteNeedsWitnessRole
        | BrokerError::StretchWitnessRoleOutsideWitnessSite { .. }
        | BrokerError::StretchMinInsyncUnsafe { .. }
        | BrokerError::SaslListenerNoMechanisms { .. }
        | BrokerError::PlainListenerNoCredentials { .. }
        | BrokerError::GssapiConfigMissing
        | BrokerError::Tls(_)
        | BrokerError::BootstrapFile { .. }
        | BrokerError::InvalidLeaderRebalanceInterval { .. }
        | BrokerError::InvalidRuntimeConfig(_)
        | BrokerError::ShutdownTimeout(_)
        | BrokerError::Raft(_) => UNKNOWN_SERVER_ERROR,
    }
}

/// Kafka coordinator response mapping for a failed metadata append.
pub(crate) fn coordinator_append_error(code: i16) -> i16 {
    match code {
        UNKNOWN_TOPIC_OR_PARTITION | NOT_ENOUGH_REPLICAS | REQUEST_TIMED_OUT => {
            COORDINATOR_NOT_AVAILABLE
        }
        NOT_LEADER_OR_FOLLOWER | KAFKA_STORAGE_ERROR => NOT_COORDINATOR,
        MESSAGE_TOO_LARGE => UNKNOWN_SERVER_ERROR,
        other => other,
    }
}
