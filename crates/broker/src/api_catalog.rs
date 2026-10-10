//! Public catalog of the Kafka protocol APIs this broker advertises.
//!
//! This is the single source of truth for both the live `ApiVersions`
//! (`api_key` 18) response and the generated protocol-API reference page. The
//! handler in `handlers::api_versions` calls [`supported_apis`] with the kind
//! of listener the request arrived on, the broker's KIP-714 setting and its
//! [`VersionGates`], so a listener a client reaches advertises what a Kafka
//! broker advertises. Under the default gates that is exactly Kafka 4.3.1's
//! table: [`CatalogApi::released`], from [`krabka_raft::KAFKA_4_3_1_APIS`], is
//! the one per-API record of what that release serves, and both the
//! advertised range and the receive-side refusal ([`is_disabled_version`])
//! are derived from it. The dispatch registry calls [`dispatched_apis`] instead, because what the broker
//! serves is wider than what any one listener names. `krabka-docgen` reads the
//! same list and does not spawn the broker binary.
//!
//! It is also the source of truth for the per-KIP rows of the generated
//! `docs/KIP_MATRIX.md`. [`KIP_ANNOTATIONS`] holds one [`KipAnnotation`] per
//! KIP that any file under `crates/` names. `aspect generate-kip-matrix`
//! parses that table as text, between the `BEGIN KIP_ANNOTATIONS` and
//! `END KIP_ANNOTATIONS` marker comments, and fails when a KIP appears in
//! `crates/` without a row here, when a row names a KIP that no file mentions,
//! or when a row points at a module or test that does not exist. Keep each
//! entry in the literal shape the existing ones use: the generator does not
//! compile Rust.

use krabka_protocol::owned::api_versions_response::ApiVersion;
pub use krabka_raft::UnstableApiVersions;

/// How far the broker takes one KIP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KipStatus {
    /// The KIP's behavior is in the tree and the listed tests establish it.
    Implemented,
    /// Part of the KIP is in the tree. `note` says which part is not.
    Partial,
    /// The KIP is deliberately not implemented. `note` cites the decision.
    OutOfScope,
}

/// One row of the generated KIP matrix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KipAnnotation {
    /// `KIP-<n>`, or a non-KIP key for a scope decision that has no KIP.
    pub key: &'static str,
    /// The Kafka behavior the row is about, in a few words.
    pub claim: &'static str,
    /// How far the tree takes it.
    pub status: KipStatus,
    /// The module that owns the behavior, as a path from the repository root.
    pub module: &'static str,
    /// The tests that establish the status, as `path` or `path::function`
    /// from the repository root. A container-driven suite carries its Kafka
    /// image into the matrix through its crate's `BUILD.bazel` `docker` map.
    /// A `tests/librdkafka_conformance.rs::<function>` entry here is also what
    /// puts the clients that test drives in the matrix's client column.
    pub tests: &'static [&'static str],
    /// What the status leaves out, or the citation for a scope decision.
    pub note: &'static str,
}

/// The key of the row that records mixed JVM and Krabka controller quorums
/// as out of scope. It is not a KIP, so the generator does not look for it in
/// `crates/`.
pub const MIXED_QUORUM_KEY: &str = "mixed-quorum";

/// The key of the KIP whose forwarding half the same decision rules out. The
/// controller listener serves `Envelope`; nothing in the tree builds one,
/// because a Krabka broker reaches its controller over the krabka-private
/// `SubmitChange` RPC, and a JVM controller is not a peer krabka speaks to.
pub const FORWARDING_KEY: &str = "KIP-590";

/// Where the out-of-scope decision for mixed quorums and JVM-side forwarding
/// is written down. The generator checks that this line still says so.
pub const OUT_OF_SCOPE_CITATION: &str = "crates/raft/src/lib.rs:54";

/// The per-KIP rows of `docs/KIP_MATRIX.md`, in ascending KIP order, with the
/// non-KIP scope rows last.
// BEGIN KIP_ANNOTATIONS
pub const KIP_ANNOTATIONS: &[KipAnnotation] = &[
    KipAnnotation {
        key: "KIP-13",
        claim: "Producer and consumer byte-rate quotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/mod.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/client_quotas/throttling.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-32",
        claim: "Message timestamps in the v1 message set and the v2 batch",
        status: KipStatus::Implemented,
        module: "crates/records-legacy/src/lib.rs",
        tests: &[
            "crates/log/tests/integration.rs::read_jvm_produced_log_dir",
            "crates/log/tests/integration.rs::jvm_consumes_rust_written_log_dir",
            "crates/broker/tests/legacy_fetch.rs",
        ],
        note: "The v0 and v1 message sets reach the wire only through `Fetch` v0-v3 and `Produce` v0-v2, which Kafka 4.x refuses and krabka serves only under the `[runtime]` key `legacy_request_versions_enable` (default off). Produce v3 and up still up-converts a legacy message set it carries.",
    },
    KipAnnotation {
        key: "KIP-48",
        claim: "Delegation tokens: create, renew, expire, describe and SCRAM token auth",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_delegation_token.rs",
        tests: &[
            "crates/broker/tests/delegation_tokens.rs",
            "crates/broker/tests/jvm_acceptance_quotas/delegation_tokens.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-62",
        claim: "Classic group state machine with the AwaitingSync stage",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_state/group.rs",
        tests: &[
            "crates/broker/tests/group_protocol_negotiation.rs",
            "crates/broker/tests/unit/consumer_group.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-73",
        claim: "Replication throttling through the leader and follower throttle keys",
        status: KipStatus::Implemented,
        module: "crates/broker/src/throttle/mod.rs",
        tests: &["crates/broker/tests/throttle.rs"],
        note: "The measured throttled-replication rate is published as `krabka_broker_replication_throttled_bytes_out` and `krabka_broker_replication_throttled_bytes_in`, which stand for Kafka's `kafka.server:type=LeaderReplication,name=byte-rate` and its `FollowerReplication` twin, with `krabka_broker_replication_throttle_sleeps` for the rounds the throttle held back entirely. Kafka delays a throttled fetch; krabka drops the partition from the round and the follower re-asks, so there is no `throttle_time_ms` to attribute and the byte-rate is what says whether the throttle is biting.",
    },
    KipAnnotation {
        key: "KIP-74",
        claim: "Fetch response size limits: max_bytes and partition_max_bytes",
        status: KipStatus::Implemented,
        module: "crates/log/src/log/read.rs",
        tests: &[
            "crates/log/src/log/read/tests.rs::a_read_never_skips_past_a_segment_its_budget_clipped",
            "crates/log/src/log/read/tests.rs::a_descriptor_read_never_skips_past_a_segment_its_budget_clipped",
            "crates/broker/tests/replication.rs::a_follower_that_catches_up_across_segments_copies_every_batch",
        ],
        note: "Only the first batch of a read can be larger than the budget, so a fetch always makes progress, and a read that the budget stops inside a segment does not go on into the next one.",
    },
    KipAnnotation {
        key: "KIP-98",
        claim: "Transactions and idempotent producers, with transactional-id expiry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/coordinator.rs",
        tests: &[
            "crates/broker/tests/transactions.rs",
            "crates/broker/tests/transactions/txn_fencing.rs",
            "crates/broker/tests/jvm_streams_app.rs",
            "crates/broker/tests/jvm_connect_distributed.rs",
            "crates/broker/tests/librdkafka_conformance.rs::next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x",
            "crates/broker/src/handlers/produce/producer_checks.rs::a_producer_with_no_state_on_a_never_appended_partition_starts_at_zero_under_trunk",
        ],
        note: "The idempotent-producer sequence check is Kafka 4.3.1's: a producer with no state may start at any sequence. Kafka trunk's KAFKA-15591 rule, which answers OUT_OF_ORDER_SEQUENCE_NUMBER to a non-zero first sequence from a producer with no state on a partition that has never held a record, applies only under `unstable.api.versions.enable`.",
    },
    KipAnnotation {
        key: "KIP-101",
        claim: "Leader-epoch based truncation for followers",
        status: KipStatus::Implemented,
        module: "crates/broker/src/replicator/truncation.rs",
        tests: &[
            "crates/broker/tests/leader_epoch.rs",
            "crates/broker/tests/leader_epoch/epoch_diverge_leader.rs",
            "crates/broker/tests/leader_epoch/epoch_fencing.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-107",
        claim: "DeleteRecords admin request",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/delete_records.rs",
        tests: &[
            "crates/broker/tests/client_admin_delete_records.rs",
            "crates/broker/tests/admin_handlers/admin_delete_records.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-108",
        claim: "A create-topic policy refusing a topic with POLICY_VIOLATION",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_policy.rs",
        tests: &[
            "crates/broker/src/topic_policy.rs",
            "crates/broker/tests/admin_handlers/admin_topic_policy.rs",
            "crates/broker/tests/topic_freeze/wire.rs",
        ],
        note: "The policy is the declared `[topic_policy]` rule set rather than a Java class named by `create.topic.policy.class.name`: a replication-factor floor, a partition ceiling, a `min.insync.replicas` floor, and required / forbidden config values. It runs where Kafka calls `CreateTopicPolicy.validate` — after config validation, before the records are generated — on validate-only requests too. A frozen topic answers with the same error 44.",
    },
    KipAnnotation {
        key: "KIP-110",
        claim: "Zstandard compression in the v2 record batch",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/fetch_downconvert.rs",
        tests: &[
            "crates/broker/tests/recompression.rs",
            "crates/broker/tests/legacy_fetch.rs",
        ],
        note: "A zstd batch is re-compressed as snappy for a v0 or v1 fetch, because those formats never carried zstd. Those fetch versions are served only under `legacy_request_versions_enable`.",
    },
    KipAnnotation {
        key: "KIP-112",
        claim: "JBOD: a broker with an offline log directory stays registered",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_heartbeat/failover.rs",
        tests: &[
            "crates/broker/tests/jbod_disk_failure.rs",
            "crates/broker/tests/offline_replicas.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-113",
        claim: "AlterReplicaLogDirs and DescribeLogDirs: replica moves between log directories",
        status: KipStatus::Implemented,
        module: "crates/broker/src/future_log.rs",
        tests: &[
            "crates/broker/tests/alter_replica_log_dirs.rs",
            "crates/broker/tests/jbod.rs",
            "crates/broker/tests/jvm_acceptance_quotas/log_dirs.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-124",
        claim: "Request-rate quotas as a percentage of handler time",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/request.rs",
        tests: &[
            "crates/broker/tests/client_quotas/throttling.rs",
            "crates/broker/src/network/dispatch/tests/throttle_mute.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-133",
        claim: "An alter-config policy refusing a topic config change with POLICY_VIOLATION",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_policy.rs",
        tests: &[
            "crates/broker/src/handlers/alter_configs/topic_configs.rs",
            "crates/broker/src/handlers/incremental_alter_configs/topic_scope.rs",
            "crates/broker/tests/admin_handlers/admin_topic_policy.rs",
        ],
        note: "The same `[topic_policy]` rule set stands in for the class named by `alter.config.policy.class.name`. Both alter paths check the resolved post-change config map, as `AlterConfigPolicy.validate` does; its `RequestMetadata` carries no partition count and no replication factor, so those two rules apply to `CreateTopics` alone.",
    },
    KipAnnotation {
        key: "KIP-207",
        claim: "The high watermark a new leader reports may regress after an election",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/resolve.rs",
        tests: &[
            "crates/broker/src/data_path_model/model.rs",
            "crates/broker/src/handlers/list_offsets/resolve.rs::offset_not_available_follows_partition_fetch_offset_for_timestamp",
        ],
        note: "The exhaustive data-path model checks durability without a watermark monotonicity assertion, which is what the KIP allows. ListOffsets fences a client lookup while the leader's epoch start is above its high watermark, as Partition.fetchOffsetForTimestamp does: OFFSET_NOT_AVAILABLE from v5, LEADER_NOT_AVAILABLE below.",
    },
    KipAnnotation {
        key: "KIP-211",
        claim: "Committed-offset retention measured from the group's last activity",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/retention.rs",
        tests: &["crates/broker/tests/offsets_retention.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-219",
        claim: "Respond first, then mute the channel for the throttle time",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/dispatch/response.rs",
        tests: &[
            "crates/broker/tests/client_quotas/throttling.rs",
            "crates/broker/src/network/dispatch/throttle_audit.rs::throttle_echo_divergences_are_the_recorded_ones",
        ],
        note: "Every API a request quota can hold on the ordinary dispatch path reports the delay it was held for: the dispatch loop patches a leading `ThrottleTimeMs`, and `Produce`, `Fetch` and `ApiVersions` -- whose schemas bury the field behind an array -- charge the quota in the handler and set it on the typed response instead. The dispatch loop decodes and encodes again the other buried-field responses (the delegation-token APIs and `OffsetDelete`) to set the field. The throttle-echo section below lists the buried-field APIs and what each one's `RequestQuotaPolicy` costs.",
    },
    KipAnnotation {
        key: "KIP-226",
        claim: "DescribeConfigs reports the source of every value",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_configs.rs",
        tests: &["crates/broker/tests/jvm_acceptance_cli/configs.rs"],
        note: "A broker resource is checked against Kafka 4.3.1's whole `KafkaConfig` key roster: a key that is not dynamic is refused, and every dynamic key is parsed and range-checked. Dynamic keys with no krabka behaviour behind them (thread pool sizes, the log cleaner, connection limits, SSL and SASL listener settings, `listeners`) are stored and reported and do not change how the broker runs, and a `CLASS` value is accepted without loading the class.",
    },
    KipAnnotation {
        key: "KIP-227",
        claim: "Incremental fetch sessions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/fetch_session.rs",
        tests: &["crates/broker/tests/fetch_session.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-255",
        claim: "SASL/OAUTHBEARER",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/oauthbearer.rs",
        tests: &["crates/broker/tests/auth_handlers/oauthbearer.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-257",
        claim: "Quota entities keyed by user, client id, or both",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/lookup.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/tuple_quota_enforcement.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-279",
        claim: "Leader-epoch truncation that converges after several leader changes",
        status: KipStatus::Implemented,
        module: "crates/log/src/leader_epoch_checkpoint/lookup.rs",
        tests: &[
            "crates/log/src/leader_epoch_model.rs",
            "crates/broker/tests/leader_epoch.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-290",
        claim: "Prefixed ACL patterns and the MATCH filter",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/acl_wire.rs",
        tests: &["crates/broker/tests/acl_handlers.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-320",
        claim: "Leader epochs in Fetch and ListOffsets, and OffsetForLeaderEpoch for consumers",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_for_leader_epoch.rs",
        tests: &[
            "crates/broker/tests/jvm_kip320_divergence.rs",
            "crates/broker/tests/jvm_kip320_divergence/wire_conformance.rs",
            "crates/broker/tests/consumer_proactive_validation.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-345",
        claim: "Static consumer-group membership",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_state/membership.rs",
        tests: &[
            "crates/broker/tests/static_membership.rs",
            "crates/broker/tests/jvm_acceptance_cli/console_groups.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-360",
        claim: "Epoch bump when a transactional producer re-initialises",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/init_producer_id/transactional.rs",
        tests: &[
            "crates/broker/tests/transactions/txn_fencing.rs::init_producer_id_fences_a_stale_producer_identity",
            "crates/broker/tests/jvm_acceptance_durability/transactional_eos.rs::transactional_console_producer_eos",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-368",
        claim: "SASL re-authentication and session expiry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/dispatch/session.rs",
        tests: &[
            "crates/broker/tests/auth_handlers/oauthbearer_sessions.rs",
            "crates/broker/tests/auth_handlers/plain.rs::plain_session_capped_by_connections_max_reauth_then_closes",
            "crates/broker/tests/auth_handlers/scram.rs::scram_session_capped_by_connections_max_reauth_then_closes",
            "crates/broker/tests/jvm_acceptance_sasl/scram.rs::jvm_sasl_scram_sha512_in_band_reauth_under_max_reauth_window",
        ],
        note: "`connections.max.reauth.ms` bounds every mechanism, and PLAIN, SCRAM and GSSAPI all re-authenticate in band under it. GSSAPI carries no automated re-auth case, because the suite has no KDC.",
    },
    KipAnnotation {
        key: "KIP-371",
        claim: "`ssl.principal.mapping.rules` maps an mTLS Subject DN to a principal",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/ssl_principal_mapper.rs",
        tests: &[
            "crates/broker/src/network/auth/ssl_principal_mapper.rs::kafka_documented_rules_map_the_subject_dn",
            "crates/broker/src/file_config/listener.rs::apply_to_listener_parses_principal_mapping_rules",
            "crates/broker/tests/jvm_acceptance_tls/mtls_principal_mapping.rs",
        ],
        note: "The rules are per listener, under `[listeners.tls_config]`. Kafka's broker-wide `ssl.principal.mapping.rules` and its `listener.name.<name>.` prefixed form are not read from `server_properties`. As in Kafka, the rules map the Subject DN on SSL listeners only, and patterns and replacements follow `java.util.regex`; a SASL_SSL listener takes its principal from the SASL exchange. The patterns are read as `java.util.regex` reads them through a rewrite for `fancy_regex`: `(?i)` folds ASCII case only, `(?iu)` folds by `Character.toLowerCase` of `Character.toUpperCase`, in a character class as well, and the POSIX classes such as `Alpha` and `Punct` are ASCII unless `(?U)` is on. One residue remains, documented in the `java_fold` module: a backreference under a case flag compares as `fancy_regex` does. Under `(?i)` it matches a non-ASCII letter in the other case, where Java's does not, and under `(?iu)` it does not match `i` with `İ` or `ı`, or a letter with a case that takes another number of bytes, such as `s` with `ſ`, where Java's does.",
    },
    KipAnnotation {
        key: "KIP-373",
        claim: "Delegation tokens for other users: the `USER` resource type and the `CREATE_TOKENS` and `DESCRIBE_TOKENS` operations",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_delegation_token.rs",
        tests: &[
            "crates/broker/src/handlers/create_delegation_token/tests.rs::create_tokens_acl_admits_minting_for_that_owner_only",
            "crates/broker/src/handlers/describe_delegation_token/tests.rs::describe_tokens_acl_on_the_owner_grants_all_of_their_tokens",
            "crates/broker/src/handlers/acl_wire/binding_filter/tests.rs::exact_user_token_filter_matches_the_stored_binding",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-382",
        claim: "MirrorMaker 2 replicates a Kafka cluster onto krabka",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_commit.rs",
        tests: &[
            "crates/broker/tests/mirror_maker2.rs::mirror_maker2_migrates_a_kafka_cluster_onto_krabka",
        ],
        note: "The stock `connect-mirror-maker.sh` of `apache/kafka:4.3.1` mirrors a broker of that release onto krabka: records with their headers, MM2's compacted `heartbeats`, checkpoints and offset-syncs topics, a consumer group's translated position, and a `retention.ms` carried over by `sync.topic.configs`. `sync.topic.acls` is left at its default; because neither cluster in the suite has an authorizer, MM2 skips the sync at the source, and the target-side `CreateAcls` krabka would answer `SECURITY_DISABLED` is asserted directly. `docs/operations/migrate-from-kafka.md` is the cutover procedure. Kafka trunk's four newest topic keys (`remote.copy.lag.ms`, `remote.copy.lag.bytes`, `max.decompressed.message.bytes`, `errors.deadletterqueue.group.enable`) are unknown topic configs by default, as they are on 4.3.1, so a replay from a trunk cluster that sets one fails as it does against a 4.3.1 broker; `unstable.api.versions.enable` accepts and describes them. With the flag on, `remote.copy.lag.ms` and `remote.copy.lag.bytes` hold a sealed segment back from the remote tier (KIP-1241), and `max.decompressed.message.bytes` refuses a compressed record above it wherever the broker decompresses one, as trunk does: on produce with `INVALID_RECORD`; in log compaction, where it fails the pass and leaves the partition uncleanable until the limit changes, where trunk holds it until a broker restart; and in a by-timestamp or `MAX_TIMESTAMP` `ListOffsets` on the local log or the remote tier, which answers `INVALID_RECORD` for a record it has to read. Follower replication is not checked, as in trunk. `errors.deadletterqueue.topic.name`, `errors.deadletterqueue.copy.record.enable` and the three `*.assignor.offload.enable` group keys are accepted on a group with trunk's validation and stored, and nothing reads them yet.",
    },
    KipAnnotation {
        key: "KIP-392",
        claim: "Fetch from the closest replica",
        status: KipStatus::Implemented,
        module: "crates/broker/src/replica_selector.rs",
        tests: &["crates/broker/tests/kip_392_fetch_from_follower.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-394",
        claim: "JoinGroup member-id bootstrap with MEMBER_ID_REQUIRED",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_ops/join.rs",
        tests: &["crates/broker/tests/group_protocol_negotiation.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-405",
        claim: "Tiered storage: remote segment copy, read, retention and metadata",
        status: KipStatus::Implemented,
        module: "crates/broker/src/remote_log_manager.rs",
        tests: &[
            "crates/broker/tests/jvm_acceptance_tiered.rs",
            "crates/broker/tests/tiered_storage_multi_broker.rs",
            "crates/remote-storage/tests/gcs_emulator.rs",
            "crates/remote-storage/tests/jvm_tiered_storage.rs",
            "crates/restore/tests/roundtrip.rs",
            "crates/restore/tests/roundtrip/consume.rs",
        ],
        note: "The tier's traffic and lag are published per topic under the `krabka_broker_remote_*` names, which stand for Kafka's `BrokerTopicMetrics` `RemoteCopyBytesPerSec`, `RemoteFetchBytesPerSec`, the three `Remote*RequestsPerSec` and `Remote*ErrorsPerSec` meters, and the four `Remote*Lag*` gauges. The bounded reader pool and the on-disk index cache report `krabka_broker_remote_log_reader_task_queue_size`, `_avg_idle_percent` and `_fetch_duration_seconds` for Kafka's `RemoteLogManager` gauges, plus rejection and cache hit / miss counters Kafka has no counterpart for. The GCS lane of the evidence, `gcs_emulator.rs`, covers the native `[remote_storage.gcs]` backend's reads, deletes and WORM startup gate against an emulator; it does not cover a GCS copy, because `object_store` writes an object with the Cloud Storage XML API and no GCS emulator serves that PUT. The copy path is covered against MinIO and `InMemory` instead.",
    },
    KipAnnotation {
        key: "KIP-412",
        claim: "Dynamic broker log levels through the BROKER_LOGGER resource",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/incremental_alter_configs.rs",
        tests: &[
            "crates/broker/tests/jvm_broker_loggers.rs",
            "crates/broker/tests/broker_logger_config.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-429",
        claim: "Cooperative rebalance protocol negotiation in JoinGroup",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_ops/join.rs",
        tests: &[
            "crates/broker/tests/group_protocol_negotiation.rs",
            "crates/broker/tests/jvm_acceptance_cli/console_groups.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-430",
        claim: "Authorized-operations bitfields in Metadata, DescribeGroups and DescribeCluster",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/authorized_operations.rs",
        tests: &["crates/broker/tests/authorized_operations.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-447",
        claim: "OffsetFetch require_stable and the UNSTABLE_OFFSET_COMMIT answer",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_fetch/unstable.rs",
        tests: &[
            "crates/broker/tests/txn_offset_commit_materialize.rs",
            "crates/broker/src/handlers/offset_fetch/tests.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-455",
        claim: "AlterPartitionReassignments and ListPartitionReassignments",
        status: KipStatus::Implemented,
        module: "crates/broker/src/reassignment.rs",
        tests: &[
            "crates/broker/tests/partition_reassignment.rs",
            "crates/broker/tests/jvm_acceptance_reassign.rs",
            "crates/broker/tests/jvm_acceptance_reassign/cancel_gate.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-460",
        claim: "ElectLeaders with unclean election, and automatic preferred-leader rebalance",
        status: KipStatus::Implemented,
        module: "crates/broker/src/leader_rebalance.rs",
        tests: &[
            "crates/broker/tests/elect_leaders.rs",
            "crates/broker/tests/elect_leaders/auto_rebalance.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-464",
        claim: "CreateTopics num_partitions and replication_factor -1 take the broker defaults",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_topics.rs",
        tests: &["crates/broker/src/handlers/create_topics/tests.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-467",
        claim: "Per-record error indices and messages in the Produce response",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/produce/schema.rs",
        tests: &["crates/broker/tests/schema_validation/rejected.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-482",
        claim: "Flexible versions and tagged fields, with the v2 request header",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/dispatch.rs",
        tests: &[
            "crates/broker/src/network/dispatch/tests.rs",
            "crates/broker/tests/librdkafka_conformance.rs::round_trip_group_join_and_api_versions_with_kcat",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-496",
        claim: "OffsetDelete for consumer groups",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/offset_delete.rs",
        tests: &[
            "crates/broker/tests/offset_delete.rs",
            "crates/broker/tests/jvm_acceptance_cli/consumer_groups.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-500",
        claim: "KRaft mode: broker heartbeats, fencing and controlled shutdown",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_heartbeat.rs",
        tests: &[
            "crates/broker/tests/controlled_shutdown.rs",
            "crates/broker/tests/advertised_controller_liveness.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-511",
        claim: "Client software name and version in ApiVersions v3",
        status: KipStatus::Implemented,
        module: "crates/raft/src/server/api_versions/client_software.rs",
        tests: &[
            "crates/broker/tests/client_software_versions.rs",
            "crates/broker/tests/librdkafka_conformance.rs::round_trip_group_join_and_api_versions_with_kcat",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-516",
        claim: "Topic identifiers in Metadata, Produce, Fetch, offsets and DeleteTopics",
        status: KipStatus::Implemented,
        module: "crates/broker/src/topic_resolve.rs",
        tests: &[
            "crates/broker/tests/kip516_metadata.rs",
            "crates/broker/tests/kip516_produce.rs",
            "crates/broker/tests/kip516_fetch.rs",
            "crates/broker/tests/kip516_offsets.rs",
            "crates/broker/tests/kip516_delete_topics.rs",
            "crates/broker/tests/librdkafka_conformance.rs::next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-525",
        claim: "CreateTopics v5 returns the created topic's configuration",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/create_topics.rs",
        tests: &["crates/broker/tests/admin_handlers.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-534",
        claim: "Compaction retains the last tombstone and transaction marker for a delay",
        status: KipStatus::Implemented,
        module: "crates/log/src/compact.rs",
        tests: &[
            "crates/log/src/compact/retention_fuzz.rs",
            "crates/log/src/compact_model/pass.rs",
            "crates/broker/tests/compaction.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-546",
        claim: "DescribeClientQuotas and AlterClientQuotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_client_quotas.rs",
        tests: &[
            "crates/broker/tests/client_quotas.rs",
            "crates/broker/tests/jvm_acceptance_quotas/client_quotas.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-554",
        claim: "SCRAM credentials through AlterUserScramCredentials and DescribeUserScramCredentials",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_user_scram_credentials.rs",
        tests: &[
            "crates/broker/tests/describe_user_scram_credentials.rs",
            "crates/broker/tests/auth_handlers/alter_scram.rs",
            "crates/broker/tests/jvm_acceptance_sasl/scram.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-559",
        claim: "protocol_type and protocol_name in JoinGroup v7 and SyncGroup v5",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/sync_group.rs",
        tests: &["crates/broker/tests/kip559_l7_proxy_fields.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-584",
        claim: "Feature versioning: UpdateFeatures and the finalized features in ApiVersions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/update_features.rs",
        tests: &[
            "crates/broker/tests/feature_finalization.rs",
            "crates/broker/tests/api_versions_features.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        note: "The finalizable features are `metadata.version`, `group.version`, `transaction.version`, `share.version`, `streams.version`, `eligible.leader.replicas.version` and `kraft.version`, the last finalized by a KRaft control record rather than by `UpdateFeatures`. `metadata.version` is supported up to 4.3.1's latest production level, `4.3-IV0` (30), unless Kafka's `unstable.feature.versions.enable` is set, which raises it to trunk's `4.5-IV0` (34), Kafka's `latestTesting`, in `ApiVersions`, node registration, `UpdateFeatures` and `krabka format` alike.",
    },
    KipAnnotation {
        key: "KIP-590",
        claim: "Envelope: the controller listener serves a forwarded admin write",
        status: KipStatus::Implemented,
        module: "crates/broker/src/envelope.rs",
        tests: &[
            "crates/broker/tests/kip590_envelope.rs",
            "crates/broker/tests/jvm_role_separated_admin.rs",
        ],
        note: "The broker side of KIP-590 is not needed: a Krabka broker reaches its controller over the krabka-private `SubmitChange` RPC (`crates/broker/src/metadata_source/observer_source.rs`), and a JVM controller is outside the compatibility target (crates/raft/src/lib.rs:54).",
    },
    KipAnnotation {
        key: "KIP-595",
        claim: "The KRaft controller quorum: Vote, BeginQuorumEpoch, EndQuorumEpoch, Fetch and DescribeQuorum",
        status: KipStatus::Implemented,
        module: "crates/kraft-core/src/core.rs",
        tests: &[
            "crates/raft/tests/kraft_sim.rs",
            "crates/raft/tests/kraft_engine_sim.rs",
            "crates/broker/tests/jvm_static_quorum_spike.rs",
            "crates/broker/tests/admin_handlers/admin_describe_quorum.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-599",
        claim: "Controller mutation quotas on CreateTopics, CreatePartitions and DeleteTopics",
        status: KipStatus::Implemented,
        module: "crates/broker/src/quota/controller_mutation.rs",
        tests: &["crates/broker/tests/controller_mutation_quota.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-612",
        claim: "Per-IP connection creation rate quotas",
        status: KipStatus::Implemented,
        module: "crates/broker/src/broker/accept.rs",
        tests: &["crates/broker/tests/ip_quotas.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-630",
        claim: "Metadata snapshots: the checkpoint file and FetchSnapshot",
        status: KipStatus::Implemented,
        module: "crates/raft/src/snapshot.rs",
        tests: &[
            "crates/raft/tests/snapshot.rs",
            "crates/raft/tests/kraft_checkpoint_jvm.rs",
            "crates/broker/tests/fetch_snapshot.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-631",
        claim: "The KRaft metadata records and broker registration",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/broker_registration.rs",
        tests: &[
            "crates/raft/tests/kraft_checkpoint_jvm.rs",
            "crates/broker/tests/unregister_broker.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-642",
        claim: "Multi-node quorum reassignment in one operation",
        status: KipStatus::OutOfScope,
        module: "crates/raft/src/controller/membership.rs",
        tests: &[],
        note: "Voter changes go one node at a time through KIP-853. `change_membership` rejects a batch that adds or removes more than one voter.",
    },
    KipAnnotation {
        key: "KIP-664",
        claim: "DescribeProducers, DescribeTransactions and ListTransactions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_producers.rs",
        tests: &[
            "crates/broker/tests/describe_producers.rs",
            "crates/broker/tests/list_describe_transactions.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-704",
        claim: "AlterPartition's leader recovery state after an unclean election",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_partition/isr_update.rs",
        tests: &[
            "crates/broker/src/handlers/alter_partition/isr_update.rs::error_response_matches_kafkas_default_fields",
            "crates/broker/src/handlers/alter_partition/isr_update.rs::recovering_partition_cannot_expand_until_leader_reports_recovered",
            "crates/broker/src/handlers/alter_partition/tests.rs",
            "crates/broker/tests/leader_election.rs::unclean_failover_recovers_after_a_real_broker_restart",
        ],
        note: "Unclean election commits RECOVERING with the leader change. The controller fences illegal RECOVERED-to-RECOVERING transitions and multi-member ISR proposals that still report RECOVERING; the elected leader's next maintenance proposal reports RECOVERED. Recovery state is stored in standard KRaft partition metadata and survives replay and snapshot restore.",
    },
    KipAnnotation {
        key: "KIP-714",
        claim: "Client metrics push: GetTelemetrySubscriptions and PushTelemetry",
        status: KipStatus::Implemented,
        module: "crates/broker/src/client_metrics/mod.rs",
        tests: &[
            "crates/broker/tests/client_telemetry.rs",
            "crates/broker/tests/client_metrics_config.rs",
            "crates/broker/tests/librdkafka_conformance.rs::next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x",
        ],
        note: "GetTelemetrySubscriptions (71) and PushTelemetry (72) are advertised only when the broker has a client-metrics receiver: the `[runtime]` key `client_metrics_enable`, or a configured `client_metrics_otlp_endpoint`, which implies it. The default is off, which is what a stock Kafka broker advertises when `metric.reporters` holds no `ClientTelemetry` implementation, so a modern Java or librdkafka client opens no telemetry handshake it has nowhere to push to. `api_catalog::ClientMetricsReceiver` names the gate and `BrokerConfig::client_metrics_receiver` reads it. Both handlers stay registered either way and answer a client that sends one anyway. A closed connection drops the instance it created, and an idle instance expires after the larger of one minute and three push intervals, as in Kafka. `PushTelemetry` follows Kafka 4.3.1 in two places where trunk has since changed: every push sets the instance's terminating flag, a rejected one included, and a payload that decompresses past `telemetry.max.bytes` is answered `INVALID_RECORD`. `unstable.api.versions.enable` selects trunk's behavior for both, which records the flag only for an accepted push (KAFKA-20898) and answers `TELEMETRY_TOO_LARGE` (KAFKA-21076). The `match` patterns of a subscription are `java.util.regex` patterns, read through the same rewrite, and the same residue remains, documented in the `java_fold` module: a backreference under a case flag compares as `fancy_regex` does. Under `(?i)` it matches a non-ASCII letter in the other case, where Java's does not, and under `(?iu)` it does not match `i` with `İ` or `ı`, or a letter with a case that takes another number of bytes, such as `s` with `ſ`, where Java's does.",
    },
    KipAnnotation {
        key: "KIP-734",
        claim: "ListOffsets MAX_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/timestamp.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-768",
        claim: "SASL/OAUTHBEARER against an OIDC provider's JWKS endpoint",
        status: KipStatus::Implemented,
        module: "crates/broker/src/oauth_jwks.rs",
        tests: &[
            "crates/broker/tests/auth_handlers/oauthbearer_tokens.rs::sasl_oauthbearer_signed_token_happy_path",
            "crates/broker/src/oauth_jwks/refresher/tests.rs",
            "crates/broker/src/oauth_jwks/fetch.rs::fetch_jwks_parses_served_keyset",
            "crates/verified/src/jwks.rs::cache_requires_one_fresh_stable_generation",
        ],
        note: "The broker half of the KIP: `crates/broker/src/oauth_jwks/` GETs the provider's JWKS document over HTTP or HTTPS, parses it, and swaps the key set into the shared `JwksHandle` a `SignedJwsValidator` reads, so rotated keys are picked up with no restart. It refreshes on a cadence and on a validator's unknown-kid signal, rate-limits the on-demand path, keeps the previous key set when a fetch fails, and fences readers with the even/odd generation counter that crates/verified/src/jwks.rs:42 proves admission against; the same file's crates/verified/src/jwks.rs:84 keeps the on-demand limiter monotonic across a wall-clock rollback. Cache expiry, issuer and audience checks, the principal and groups claims, the `typ` check, clock skew, an operator-supplied `IdP` TLS trust bundle and the `use=enc` filter are configured from the `[oauthbearer]` TOML table rather than Kafka's `sasl.oauthbearer.*` JAAS options. The KIP's client half -- the login callback that retrieves a token with an OAuth `client_credentials` grant -- is a client concern and lives in `krabka-client-rs`, not in this repository.",
    },
    KipAnnotation {
        key: "KIP-778",
        claim: "metadata.version as a finalized feature that `krabka format` bootstraps",
        status: KipStatus::Implemented,
        module: "crates/format/src/format/features.rs",
        tests: &[
            "crates/format/tests/format_smoke.rs",
            "crates/broker/tests/format_features.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-814",
        claim: "A static leader that rejoins a stable group gets `skip_assignment` and keeps the current assignment",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/classic_ops/join.rs",
        tests: &[
            "crates/broker/tests/static_membership.rs::static_rejoin_preserves_assignment_and_generation",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-827",
        claim: "DescribeLogDirs v4 reports total and usable bytes per directory",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_log_dirs/dirs.rs",
        tests: &["crates/broker/tests/jvm_acceptance_quotas/log_dirs.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-835",
        claim: "The KRaft leader appends a NoOpRecord every metadata.max.idle.interval.ms",
        status: KipStatus::Implemented,
        module: "crates/raft/src/kraft/controller/idle.rs",
        tests: &[
            "crates/raft/src/kraft/controller/tests_cleaning.rs::a_leader_appends_kip835_no_ops_that_change_nothing",
            "crates/raft/src/kraft/controller/tests_cleaning.rs::the_no_op_timer_runs_only_on_a_leader_with_an_interval",
            "crates/raft/src/kraft/controller/tests_apply.rs::quorum_snapshot_timestamp_tracks_applied_not_newer_committed_records",
            "crates/broker/src/metadata_observer/fetch.rs::apply_fetch_records_advances_data_batch_offset_and_publishes",
            "crates/broker/src/metadata_observer.rs::pending_record_processing_time_tracks_average_and_max",
        ],
        note: "The NoOpRecord heartbeat is in the tree. Controller and broker metrics publish the last applied record offset and append timestamp, and time since that timestamp. Pending-record processing gauges report the process-lifetime average and maximum microseconds spent on completed apply passes that advanced across pending committed records. Gauges refresh on the broker polling interval.",
    },
    KipAnnotation {
        key: "KIP-841",
        claim: "Unclean leader election when the ISR is empty and the topic allows it",
        status: KipStatus::Implemented,
        module: "crates/broker/src/leader_election.rs",
        tests: &[
            "crates/broker/tests/leader_election.rs",
            "crates/broker/src/leader_election/scan/dead_broker_tests.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-848",
        claim: "The next-generation consumer group protocol",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/consumer_state.rs",
        tests: &[
            "crates/broker/tests/consumer_group_next_gen.rs",
            "crates/broker/tests/jvm_consumer_group_next_gen.rs",
            "crates/broker/tests/group_version.rs",
            "crates/broker/tests/jvm_acceptance_cli/consumer_groups.rs",
            "crates/broker/tests/librdkafka_conformance.rs::next_gen_group_topic_ids_and_telemetry_with_librdkafka_2x",
        ],
        note: "A `ConsumerGroupHeartbeat` whose `SubscribedTopicRegex` does not compile is answered `INVALID_REGULAR_EXPRESSION` (128) before any member record is written, and the member is not admitted, as Kafka does. The pattern is compiled with Rust `regex` in Unicode mode, which accepts RE2J's Unicode character classes; topic names are ASCII, so RE2J's ASCII-only perl classes cannot diverge on a match. An inline flag group naming a flag RE2J has no equivalent for (`x`, `u`, `R`) is rejected ahead of the compile with RE2J's own message, since `regex` would take it. The `re2j` wrapper translates RE2J's `\\Q...\\E` literal quoting and character-class rules before compiling, while keeping Rust `regex`'s bounded non-backtracking matching. A repetition large enough to exceed `regex`'s compiled-size limit remains a narrow acceptance difference. No JVM-lane case covers the refusal: `KafkaConsumer.subscribe(Pattern)` and `kafka-console-consumer --include` compile the pattern locally with `java.util.regex`, so a stock JVM client never sends an invalid one to the broker. The group resolves each subscribed regex once for all its members, as Kafka's `TopicRegexResolver` does: against the metadata image, keeping the topics that the principal of the heartbeat that resolves it may `Describe`. It records the result as a `ConsumerGroupRegularExpression` record, so a coordinator failover restores the topics of every regex subscription without a heartbeat that carries the pattern. A heartbeat of any member resolves the patterns again when a member's pattern is not resolved yet, when a topic was created since the last resolution, or when the last one is older than `group.consumer.regex.refresh.interval.ms` (10 minutes), and never within 10 seconds of the last one. A change of the ACLs counts as a new topic here, where Kafka picks it up at the refresh interval. A group's `consumer.session.timeout.ms`, `consumer.heartbeat.interval.ms` and `consumer.assignment.interval.ms` are accepted, validated against the `group.consumer.min.*` and `group.consumer.max.*` bounds and stored by `AlterConfigs` and `IncrementalAlterConfigs` on a `GROUP` resource, as Kafka does, and the consumer coordinator applies them to that group over the broker's `group.consumer.*` values. A next target assignment waits for `consumer.assignment.interval.ms` (1 s by default) since the last one, as in Kafka's `canComputeNextTargetAssignment`.",
    },
    KipAnnotation {
        key: "KIP-853",
        claim: "Dynamic controller quorum: AddRaftVoter, RemoveRaftVoter, UpdateRaftVoter and auto-join",
        status: KipStatus::Implemented,
        module: "crates/raft/src/kraft/controller/reconfiguration.rs",
        tests: &[
            "crates/broker/tests/dynamic_voters.rs",
            "crates/raft/tests/reconfig.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-858",
        claim: "Directory identifiers: AssignReplicasToDirs and directory ids in heartbeats",
        status: KipStatus::Implemented,
        module: "crates/broker/src/assign_dirs.rs",
        tests: &[
            "crates/broker/tests/jbod_disk_failure.rs",
            "crates/broker/tests/offline_replicas.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-860",
        claim: "AlterPartitionReassignments refuses a replication-factor change unless the request allows it",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_partition_reassignments/plan.rs",
        tests: &[
            "crates/broker/src/handlers/alter_partition_reassignments/plan.rs::the_replication_factor_check_counts_the_set_the_partition_is_headed_for",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-890",
        claim: "Transactions v2: verify-only AddPartitionsToTxn and epoch bumps on completion",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/version.rs",
        tests: &[
            "crates/broker/tests/transaction_version.rs",
            "crates/broker/tests/transaction_version/txnver_verify_only.rs",
        ],
        note: "What a request may do follows its API version, as in Kafka, whatever `transaction.version` the cluster finalized: EndTxn v5 and AddPartitionsToTxn v4 and later are TV_2 and bump the epoch, and older versions and AddOffsetsToTxn are TV_0. The cluster level picks the `__transaction_state` value format and the rules of a server-initiated abort. Two answers follow Kafka trunk only under `unstable.api.versions.enable`: an EndTxn v5 commit at the pre-abort epoch after CompleteAbort answers PRODUCER_FENCED (KAFKA-20785, where 4.3.1 answers INVALID_TXN_STATE), and `LastProducerEpoch` (tag 4) is written to and read from `__transaction_state` (KAFKA-20357, where 4.3.1 keeps it in memory). A transactional offset commit records the topic id only from TxnOffsetCommit v6 and under that flag.",
    },
    KipAnnotation {
        key: "KIP-903",
        claim: "Broker epochs fence stale replicas in AlterPartition",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/alter_partition/isr_update.rs",
        tests: &[
            "crates/raft/src/kraft/controller/tests_broker_registration.rs",
            "crates/broker/src/elr/tests.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-919",
        claim: "Admin clients bootstrap from controllers: ControllerRegistration, DescribeCluster and UnregisterBroker",
        status: KipStatus::Implemented,
        module: "crates/broker/src/controller_admin.rs",
        tests: &[
            "crates/broker/tests/jvm_bootstrap_controller.rs",
            "crates/broker/tests/client_admin_controller_bootstrap.rs",
            "crates/broker/tests/unregister_broker.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-932",
        claim: "Share groups: membership, ShareFetch, ShareAcknowledge, the share coordinator and admin offsets",
        status: KipStatus::Implemented,
        module: "crates/broker/src/share_partition/mod.rs",
        tests: &[
            "crates/broker/tests/share_groups.rs",
            "crates/broker/tests/share_consume.rs",
            "crates/broker/tests/share_admin_offsets.rs",
            "crates/broker/tests/jvm_share_groups.rs",
        ],
        note: "Group-level configs are accepted, validated and stored for every key of Kafka 4.3.1's `GroupConfig`, by `AlterConfigs` and `IncrementalAlterConfigs` on a `GROUP` resource, as Kafka does. The share partitions apply `share.auto.offset.reset`, `share.record.lock.duration.ms`, `share.delivery.count.limit`, `share.partition.max.record.locks`, `share.isolation.level` and `share.renew.acknowledge.enable`. The share coordinator applies `share.session.timeout.ms`, `share.heartbeat.interval.ms` and `share.assignment.interval.ms` to that group over the broker's `group.share.*` values, and a next target assignment waits for the assignment interval (1 s by default) since the last one, as in Kafka's `canComputeNextTargetAssignment`.",
    },
    KipAnnotation {
        key: "KIP-939",
        claim: "Two-phase commit transactions with the KIP-939 timeout rules",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/two_pc.rs",
        tests: &[
            "crates/broker/tests/transactions_2pc.rs",
            "crates/broker/src/txn/two_pc_model.rs",
        ],
        note: "`InitProducerId` v6 is `latestVersionUnstable` in Kafka 4.3.1, so it is served only under `unstable.api.versions.enable`, and 2PC itself only under `transaction.two.phase.commit.enable`. `keepPreparedTxn` is answered UNSUPPORTED_VERSION, as 4.3.1's `TransactionCoordinator.handleInitProducerId` answers it, unless both switches are on; then krabka's prepared-transaction recovery serves it, which no Kafka release implements.",
    },
    KipAnnotation {
        key: "KIP-950",
        claim: "Tiered storage disablement: remote.log.copy.disable and remote.log.delete.on.disable",
        status: KipStatus::Implemented,
        module: "crates/broker/src/remote_log_manager.rs",
        tests: &[
            "crates/broker/src/config_keys/validation/tests.rs",
            "crates/broker/tests/jvm_acceptance_tiered.rs",
        ],
        note: "`remote.storage.enable` going true -> false is refused unless `remote.log.delete.on.disable=true` comes with it, and the flip then erases the partition's remote segments and raises its log start offset to the local log start. `remote.log.copy.disable=true` is the read-only tier: no new copies, reads and remote retention unchanged. Under a WORM archive the cascade clears the partition's remote metadata and removes nothing from the archive, as a `DeleteTopics` cascade does.",
    },
    KipAnnotation {
        key: "KIP-951",
        claim: "Leader hints in the Produce and Fetch responses",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/produce/pipeline.rs",
        tests: &[
            "crates/broker/tests/produce_leader_gate.rs",
            "crates/broker/tests/producer_leader_routing.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-966",
        claim: "Eligible leader replicas, unclean recovery and DescribeTopicPartitions",
        status: KipStatus::Implemented,
        module: "crates/broker/src/elr.rs",
        tests: &[
            "crates/broker/tests/unclean_recovery.rs",
            "crates/broker/tests/describe_topic_partitions.rs",
            "crates/broker/tests/jvm_acceptance_cli/elr_columns.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        note: "ELR maintenance is gated on the `eligible.leader.replicas.version` feature, as Kafka gates it on `FeatureControlManager.isElrFeatureEnabled()`: at level 0 the controller publishes no eligible or last-known-eligible set, and a downgrade to 0 clears what an earlier level 1 published. The release default is level 1 from `metadata.version` 4.1-IV0, where `ELRV_1` bootstraps, and 0 below it; level 1 declares Kafka's KIP-1022 dependency on `metadata.version` at 4.0-IV1. ELR is carried by standard per-partition KRaft metadata and is consumed during JVM-compatible replay. The last-known ELR follows Kafka 4.3.1's `PartitionChangeBuilder` (`useLastKnownLeaderInBalancedRecovery` is always on): it holds the single last leader of a partition that has no leader and is empty otherwise, and when that leader returns it is elected as an unclean leader (`RECOVERING`, a singleton ISR) with no `unclean.leader.election.enable` or `unclean.recovery.strategy` consulted, unless an ELR member is there to lead cleanly first. krabka's partition record always names a leader, so a partition with no leader keeps its last leader in the record, marks it with that one-member last-known ELR, and `Metadata` and `DescribeTopicPartitions` report it as `leader = -1`; the election runs when a broker unfences (`handleBrokerUnfenced`). The operator election in `ElectLeaders` does not take the last-known rung.",
    },
    KipAnnotation {
        key: "KIP-996",
        claim: "Pre-vote before a KRaft election",
        status: KipStatus::Implemented,
        module: "crates/kraft-core/src/core/election.rs",
        tests: &[
            "crates/kraft-core/src/core/election/tests.rs",
            "crates/raft/tests/kraft_engine_sim/failover.rs",
            "crates/broker/tests/jvm_static_quorum_spike/contested_election.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1005",
        claim: "ListOffsets LATEST_TIERED_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/sentinels.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1022",
        claim: "Feature flags with dependency checks at format and upgrade time",
        status: KipStatus::Implemented,
        module: "crates/format/src/format/features.rs",
        tests: &[
            "crates/broker/tests/format_features.rs",
            "crates/broker/tests/jvm_features.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1023",
        claim: "ListOffsets EARLIEST_PENDING_UPLOAD_TIMESTAMP",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/resolve.rs",
        tests: &["crates/broker/tests/list_offsets_isolation/timestamp_sentinels.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1038",
        claim: "ListTransactions filters transactional ids by an RE2J-compiled pattern",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_transactions.rs",
        tests: &[
            "crates/broker/src/handlers/list_transactions.rs",
            "crates/broker/src/re2j.rs",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1066",
        claim: "cordoned.log.dirs: cordoned log directories take no new replica",
        status: KipStatus::Implemented,
        module: "crates/broker/src/cordoned_log_dirs.rs",
        tests: &[
            "crates/broker/src/cordoned_log_dirs.rs",
            "crates/broker/src/handlers/broker_heartbeat/tests.rs::a_heartbeat_stores_its_cordoned_dirs_from_4_3_iv0",
            "crates/broker/src/handlers/create_topics/placement/tests.rs::fully_cordoned_brokers_are_not_automatic_placement_candidates",
            "crates/broker/src/handlers/incremental_alter_configs/broker_scope.rs",
            "crates/broker/tests/alter_replica_log_dirs.rs",
        ],
        note: "Matches Kafka 4.3.1. A manual CreateTopics or CreatePartitions assignment, or an AlterPartitionReassignments target, that names a broker whose log directories are all cordoned is accepted, as a live `apache/kafka:4.3.1` accepts it: the `INVALID_REPLICA_ASSIGNMENT` refusal `The manual partition assignment includes broker N, but all its log directories are cordoned.` is KAFKA-20832, which is on Kafka trunk and the 4.3 branch after 4.3.1 only. The controller does not tell a forwarded write of `cordoned.log.dirs` from a direct one, so it does not apply Kafka's rule that a direct controller write may only remove entries.",
    },
    KipAnnotation {
        key: "KIP-1071",
        claim: "Streams groups: StreamsGroupHeartbeat and StreamsGroupDescribe",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/streams/mod.rs",
        tests: &[
            "crates/broker/tests/streams_groups.rs",
            "crates/broker/tests/streams_classic_upgrade.rs",
            "crates/broker/tests/jvm_streams_groups.rs",
            "crates/broker/tests/jvm_streams_app.rs",
        ],
        note: "A group's `streams.*` overrides, `streams.initial.rebalance.delay.ms` and `streams.assignment.interval.ms` included, apply per group, and a key of another coordinator stored beside them leaves them intact.",
    },
    KipAnnotation {
        key: "KIP-1073",
        claim: "DescribeCluster hides fenced brokers from clients",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/describe_cluster.rs",
        tests: &["crates/broker/tests/role_separation_observer.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1075",
        claim: "A server-side timeout for remote ListOffsets work",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_offsets/remote.rs",
        tests: &["crates/broker/src/config_keys/registry/tests.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1101",
        claim: "The MetadataHash tagged field on the group-metadata records, and the streams partition-metadata record it retires",
        status: KipStatus::OutOfScope,
        module: "crates/broker/src/coordinator/unified/persistence_next_gen/epochs.rs",
        tests: &[],
        note: "The hash is how Kafka decides a group must rebalance because its subscribed topics changed shape. The consumer and share groups of krabka decide that from the metadata image instead and write the hash as 0, which is what Kafka writes for a group whose hash is unset, and Kafka's own reader accepts it. The streams group keeps Kafka's hash, because KIP-1071 configures the topology again when it changes, and writes it in its group metadata record. krabka keeps the streams partition-metadata snapshot this KIP retired, on the key version Kafka no longer assigns, where Kafka's serde skips it as an unknown type rather than mis-reading it.",
    },
    KipAnnotation {
        key: "KIP-1142",
        claim: "ListConfigResources",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/list_config_resources.rs",
        tests: &["crates/broker/tests/admin_handlers/admin_listings.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1155",
        claim: "A checkpoint at every metadata.version downgrade",
        status: KipStatus::Implemented,
        module: "crates/raft/src/kraft/controller/snapshotting.rs",
        tests: &["crates/raft/src/kraft/controller/tests_downgrade.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1186",
        claim: "kraft.version upgrade and the last-voter check that the Kafka 4.3 quorum tools drive",
        status: KipStatus::Implemented,
        module: "crates/raft/src/server/voter_admin.rs",
        tests: &["crates/broker/tests/jvm_features.rs"],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1191",
        claim: "Share-group dead-letter queue: a record that a member rejects, or that uses up the delivery count limit, is written to the group's dead-letter topic before it is archived",
        status: KipStatus::Partial,
        module: "crates/broker/src/share_partition/dlq.rs",
        tests: &[
            "crates/broker/src/features.rs::share_version_two_is_advertised_only_under_unstable_feature_versions",
            "crates/broker/src/handlers/update_features/preconditions.rs::share_version_two_needs_unstable_feature_versions",
            "crates/format/src/format/features.rs::share_version_two_is_trunks_level",
            "crates/broker/src/share_partition/state/dlq.rs",
            "crates/broker/src/share_partition/dlq/record.rs",
            "crates/broker/src/share_partition/dlq/validate.rs",
            "crates/broker/src/share_partition/dlq/coalesce.rs",
            "crates/broker/src/share_partition/dlq/writer.rs",
            "crates/broker/src/metrics/share_dlq.rs",
            "crates/broker/src/share_partition/manager/dead_letter.rs",
            "crates/broker/tests/share_consume/dead_letter_queue.rs",
        ],
        note: "Kafka trunk's `share.version` 2, which a node supports only under `unstable.feature.versions.enable`; the topic key `errors.deadletterqueue.group.enable` and the group keys `errors.deadletterqueue.topic.name` and `errors.deadletterqueue.copy.record.enable` need `unstable.api.versions.enable` too, so trunk mode is both switches. At `share.version` 1, the level a default node runs, a group's dead-letter keys change nothing and a reject archives at once. A group with a topic name gets one record for each offset that is rejected or reaches `share.delivery.count.limit`, with Kafka's six `__dlq.errors.*` headers and, with `errors.deadletterqueue.copy.record.enable`, the source key and value. It is an acks=all produce to the leader of the topic partition that the source partition maps to, as Kafka's `ShareGroupDLQStateManager` sends it, over the inter-broker listener, and it retries five times from 1 s to 30 s. The writes of a broker share one connection to each destination leader, neighbouring runs with the same delivery count and cause are one write, and at most eight writes run at once. As in Kafka's `ShareGroupDLQStateManager`, the rounds of every write that waits for one destination leader go out in as few produce requests as `max.message.bytes` allows, across runs and partitions: one request is in flight to a leader, the rounds that arrive meanwhile go out together in the next, the rounds for one partition are one batch, and each write archives its run when the response holds the row for its own partition, and retries on its own. The eight writers are the most runs that share a request. The record is persisted as delivery state `ARCHIVING` (3) first and as `ARCHIVED` (4) when the write ends, whatever its result, and a leader that finds `ARCHIVING` after a restart writes the record again, as `SharePartition.initiateDLQAndArchive` and `maybeResumeDlqArchiving` do. The topic has to have `errors.deadletterqueue.group.enable=true`, a name that starts with `errors.deadletterqueue.topic.name.prefix` (default `dlq.`) and not with `__`, and a missing topic is created only when `errors.deadletterqueue.auto.create.topics.enable` is true. Both cluster keys are read from the dynamic broker config, per broker and then cluster-wide, and neither is in the `DescribeConfigs` key roster of a broker yet. The `DeadLetterQueue*` meters of Kafka's `ShareGroupMetrics` are exported for each group as the counters `krabka_broker_share_group_dlq_records_total` (`DeadLetterQueueRecordCount`), `krabka_broker_share_group_dlq_produce_requests_total` (`DeadLetterQueueTotalProduceRequestsPerSec`, one for each round of each write for each attempt, as Kafka counts it) and `krabka_broker_share_group_dlq_failed_produce_requests_total` (`DeadLetterQueueFailedProduceRequestsPerSec`), and `group.share.dlq.manager.class.name`, a JVM class name, has no counterpart. A copied record that the six headers and the batch header push over `max.message.bytes` is written with its headers alone, where trunk's writer sends it whole and loses it to `MESSAGE_TOO_LARGE`.",
    },
    KipAnnotation {
        key: "KIP-1222",
        claim: "Share acquisition lock renewal: the Renew acknowledge type and IsRenewAck on ShareFetch and ShareAcknowledge v2",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/share_fetch/acknowledge.rs",
        tests: &[
            "crates/broker/src/handlers/share_fetch/renew_tests.rs::renew_acknowledgements_renew_only_the_renew_offsets",
            "crates/broker/src/handlers/share_fetch/renew_tests.rs::a_renew_fetch_answers_a_denied_topic_as_an_acknowledge_error",
            "crates/broker/tests/share_consume/lock_lifetime.rs::renew_extends_lock_not_redelivered",
        ],
        note: "`share.renew.acknowledge.enable` (default true) is a `GroupConfig` key that `AlterConfigs` and `IncrementalAlterConfigs` accept on a `GROUP` resource and that the share partitions apply: a group that sets it to false answers a Renew acknowledgement with INVALID_RECORD_STATE, and a Renew type without IsRenewAck is INVALID_REQUEST.",
    },
    KipAnnotation {
        key: "KIP-1241",
        claim: "Delayed remote copy: remote.copy.lag.ms and remote.copy.lag.bytes hold a sealed segment back from the remote tier",
        status: KipStatus::Implemented,
        module: "crates/broker/src/remote_log_manager/copy.rs",
        tests: &[
            "crates/broker/src/remote_log_manager/copy.rs::a_sealed_segment_waits_out_its_copy_lag",
            "crates/broker/src/remote_log_manager/copy.rs::a_derived_copy_lag_is_the_effective_local_retention",
            "crates/broker/src/remote_log_manager.rs::tick_all_honours_the_remote_copy_lag_on_a_trunk_broker",
        ],
        note: "Kafka trunk's topic keys and their `log.remote.copy.lag.ms` and `log.remote.copy.lag.bytes` broker defaults, served only under `unstable.api.versions.enable`. By default they are unknown configs, as on 4.3.1, and a sealed segment is copied as soon as it is sealed. With the flag on, the copy pass stops at the first sealed segment that is neither old enough by its newest record nor far enough behind newer local data, as `RLMCopyTask.candidateLogSegments` does.",
    },
    KipAnnotation {
        key: "KIP-1242",
        claim: "ApiVersions v5 routing identity and REBOOTSTRAP_REQUIRED",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/api_versions.rs",
        tests: &["crates/broker/src/handlers/api_versions/tests.rs"],
        note: "Kafka trunk's ApiVersions v5, served only under `unstable.api.versions.enable`. By default both listeners serve 4.3.1's v0-v4 and answer v5 `UNSUPPORTED_VERSION` with that range, so REBOOTSTRAP_REQUIRED (129) is never sent. On a SASL listener the first ApiVersions, which arrives before authentication, is answered as Kafka's `SaslServerAuthenticator` answers it: `INVALID_REQUEST` for an invalid request and otherwise the table, with no routing check.",
    },
    KipAnnotation {
        key: "KIP-1251",
        claim: "OffsetCommit accepts an older member epoch for a partition assigned at or before that epoch",
        status: KipStatus::Implemented,
        module: "crates/broker/src/coordinator/unified/consumer_state/group.rs",
        tests: &[
            "crates/broker/src/handlers/offset_commit/group_validation_tests.rs::consumer_group_commit_follows_kip_1251",
            "crates/broker/src/coordinator/unified/consumer_state/group.rs::offset_commit_follows_kafka_consumer_group_rule",
        ],
        note: "",
    },
    KipAnnotation {
        key: "KIP-1263",
        claim: "The AssignmentTimestamp tagged field on the target-assignment metadata records",
        status: KipStatus::OutOfScope,
        module: "crates/broker/src/coordinator/unified/persistence_next_gen/epochs.rs",
        tests: &[],
        note: "Kafka stamps each target assignment with the time it was computed, for its own assignment metrics. krabka does not measure assignment latency from the log, so it writes the tagged field's default of 0, which is what Kafka writes when it has no timestamp to record.",
    },
    KipAnnotation {
        key: "KIP-1276",
        claim: "CIDR-range ACL hosts (10.0.0.0/8, 2001:db8::/32) in CreateAcls and host matching",
        status: KipStatus::Implemented,
        module: "crates/authz/src/cidr.rs",
        tests: &[
            "crates/authz/src/cidr.rs",
            "crates/authz/src/simple/matching.rs",
            "crates/broker/src/handlers/create_acls/validate.rs",
        ],
        note: "Trunk behavior, behind `unstable.feature.versions.enable`. By default `CreateAcls` and the authorizer behave as Kafka 4.3.1 does: any host, an empty one or one containing `/` included, is stored and compared as text. Under the flag `CreateAcls` applies trunk's `validateHostPattern`, which accepts a CIDR host from `metadata.version` 4.4-IV1 (level 32) and answers UNSUPPORTED_VERSION below it, and the authorizer reads a stored host containing `/` as a range from that version.",
    },
    KipAnnotation {
        key: "KIP-1312",
        claim: "UnregisterController drops a controller registration",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/unregister_controller.rs",
        tests: &[
            "crates/broker/src/handlers/unregister_controller/tests.rs::refusals_follow_trunks_order",
            "crates/broker/src/handlers/unregister_controller/tests.rs::handle_unregisters_a_registered_controller_as_trunk_does",
            "crates/broker/tests/controller_admin_surface.rs::controller_listener_serves_unregister_controller",
            "crates/broker/tests/unregister_controller_forward.rs",
        ],
        note: "Matches Kafka trunk: no Kafka release has api key 94. The controller listener answers it and a broker listener forwards it in an Envelope, as trunk's `ControllerApis` and `KafkaApis` do. It is advertised and accepted only under `unstable.api.versions.enable`; by default a request for key 94 closes the connection, as it does against a 4.3.1 broker or controller, and an Envelope carrying one is answered UNSUPPORTED_VERSION. It needs `metadata.version` 4.4-IV2 (level 33), which a node supports only under `unstable.feature.versions.enable`, and answers trunk's UNSUPPORTED_VERSION below it.",
    },
    KipAnnotation {
        key: "KIP-1319",
        claim: "TxnOffsetCommit v6: topics by id, GROUP_ID_NOT_FOUND and STALE_MEMBER_EPOCH",
        status: KipStatus::Implemented,
        module: "crates/broker/src/txn/handlers/txn_offset_commit.rs",
        tests: &[
            "crates/broker/src/txn/handlers/txn_offset_commit/integration_tests.rs::v6_resolves_topic_ids_before_the_read_gate_and_the_existence_check",
            "crates/broker/src/txn/handlers/txn_offset_commit/integration_tests.rs::v6_answers_group_id_not_found_where_older_versions_answer_illegal_generation",
            "crates/broker/tests/transactions/txn_offset_commit_topic_ids.rs::send_offsets_to_transaction_commits_by_topic_id",
        ],
        note: "Kafka trunk's TxnOffsetCommit v6, which Kafka 4.3.1 predates, served only under `unstable.api.versions.enable`. By default the broker advertises and accepts 4.3.1's v0-v5.",
    },
    KipAnnotation {
        key: "KIP-1331",
        claim: "Streams topology descriptions: StreamsGroupHeartbeat, StreamsGroupDescribe v1 and StreamsGroupTopologyDescriptionUpdate",
        status: KipStatus::Partial,
        module: "crates/broker/src/coordinator/unified/streams/description.rs",
        tests: &[
            "crates/broker/src/coordinator/unified/streams/actor/tests.rs::heartbeat_response_carries_the_recovery_lag_at_version_1_only",
            "crates/broker/src/handlers/streams_group_heartbeat.rs::handle_answers_v1_with_the_recovery_lag_and_no_topology_description_request",
            "crates/broker/src/handlers/streams_group_describe/tests.rs::version_1_names_the_assignor_and_the_topology_description_status",
            "crates/broker/src/coordinator/unified/streams/actor/tests.rs::a_member_missing_a_rack_aware_tag_gets_missing_client_tags_at_version_1",
            "crates/broker/src/handlers/streams_group_topology_description_update/tests.rs::handle_answers_as_a_trunk_broker_without_a_plugin",
            "crates/broker/src/handlers/streams_group_topology_description_update/tests.rs::handle_refuses_what_a_trunk_broker_with_a_plugin_refuses",
            "crates/broker/src/coordinator/unified/streams/actor/description/tests.rs::one_member_of_a_group_is_asked_for_the_description",
            "crates/broker/src/coordinator/unified/streams/actor/description/tests.rs::a_loaded_group_is_not_asked_for_a_stored_epoch",
            "crates/broker/tests/streams_groups/streams_topology_description.rs::one_member_is_asked_and_its_push_is_described",
            "crates/broker/tests/streams_groups/streams_topology_description.rs::a_deleted_and_recreated_group_is_asked_again",
        ],
        note: "Matches Kafka trunk, and served only under `unstable.api.versions.enable`: by default StreamsGroupHeartbeat and StreamsGroupDescribe are 4.3.1's v0, api key 93 is not advertised and closes the connection, and trunk's `streams.*` group keys are unknown group configs. Without `group.streams.topology.description.plugin.class`, as a Kafka broker runs by default, a heartbeat never sets TopologyDescriptionRequired, a describe that asks for the description answers NOT_STORED, and StreamsGroupTopologyDescriptionUpdate (93) answers UNSUPPORTED_VERSION with trunk's `The broker has no streams group topology description plugin configured.` once the streams protocol and group Read gates pass. krabka cannot load a JVM class, so the key accepts only Kafka's reference plugin, `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`, which krabka builds in, and any other class stops the broker at startup. With that plugin, a version 1 heartbeat asks one member for the description under Kafka's back-off of 30 seconds that doubles to one hour. The broker keeps the pushed description in memory and its epoch in the `StoredDescriptionTopologyEpoch` of the group metadata record, a describe answers it as AVAILABLE, and DeleteGroups drops it. As with Kafka's in-memory plugin, a broker restart or a coordinator move loses the description but keeps the stored epoch, so the group is not asked again until its topology epoch changes. Kafka writes the UNCERTAIN epoch before it calls the plugin, and krabka, whose plugin stores in the same step, writes only the pushed epoch. The built-in plugin never fails, so `FailedDescriptionTopologyEpoch` stays -1 and there is no cleanup cycle for a plugin to retry. Heartbeat v1 carries MISSING_CLIENT_TAGS when a tag key named by the group's `streams.rack.aware.assignment.tags`, whose default is the broker's `group.streams.rack.aware.assignment.tags`, is missing from the member's client tags. DeleteGroups v3, which adds a per-group ErrorMessage, is trunk's too and is advertised only under the same flag. krabka never answers GROUP_DELETION_FAILED, since its plugin cannot fail a delete, so the message is always null.",
    },
    KipAnnotation {
        key: "KIP-1357",
        claim: "StreamsGroupDescribe v1 names the group's task assignor",
        status: KipStatus::Implemented,
        module: "crates/broker/src/handlers/streams_group_describe/render.rs",
        tests: &[
            "crates/broker/src/handlers/streams_group_describe/tests.rs::version_1_names_the_assignor_and_the_topology_description_status",
        ],
        note: "Kafka trunk's StreamsGroupDescribe v1, served only under `unstable.api.versions.enable`.",
    },
    KipAnnotation {
        key: "SASL/GSSAPI",
        claim: "SASL/Kerberos authentication",
        status: KipStatus::Implemented,
        module: "crates/broker/src/network/auth/gssapi.rs",
        tests: &[
            "crates/broker/tests/gssapi_e2e.rs",
            "crates/broker/tests/auth_handlers/gssapi.rs",
        ],
        note: "Kerberos predates the KIP process (KAFKA-1686, Kafka 0.9), so no KIP number. Both suites run in the scheduled `container gssapi` CI job: the KDC fixture writes keytabs through a bind mount, so the lane is schedule and workflow_dispatch only.",
    },
    KipAnnotation {
        key: "mixed-quorum",
        claim: "A controller quorum with both JVM and Krabka voters",
        status: KipStatus::OutOfScope,
        module: "crates/raft/src/lib.rs",
        tests: &[],
        note: "Outside the raft crate's compatibility target: crates/raft/src/lib.rs:54.",
    },
];
// END KIP_ANNOTATIONS

macro_rules! v {
    ($mod:ident) => {
        CatalogApi::new(
            krabka_protocol::owned::$mod::API_KEY,
            krabka_protocol::owned::$mod::MIN_VERSION,
            krabka_protocol::owned::$mod::MAX_VERSION,
        )
    };
}

/// Whether the broker serves the request versions Kafka 4.x removed.
///
/// krabka-only; Kafka has no switch for it. The `[runtime]` key
/// `legacy_request_versions_enable` reads it. While it is
/// [`Disabled`][Self::Disabled], the default, every listener advertises and
/// accepts exactly Kafka 4.3.1's minimum versions: `Fetch` from v4 and
/// `ListOffsets` from v1, and `Produce` from v3, though `Produce` is still
/// advertised from v0 as Kafka advertises it (KAFKA-18659). A request below
/// the minimum closes the connection, as it does against a 4.3.1 broker.
/// [`Enabled`][Self::Enabled] serves `Fetch` v0-v3, `ListOffsets` v0 and
/// `Produce` v0-v2 as well.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LegacyRequestVersions {
    /// Serve Kafka 4.3.1's minimum versions only.
    #[default]
    Disabled,
    /// Also serve the pre-4.0 request versions krabka still decodes.
    Enabled,
}

impl From<bool> for LegacyRequestVersions {
    fn from(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// The two version switches a listener's table depends on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VersionGates {
    /// Kafka's internal `unstable.api.versions.enable`.
    pub unstable: UnstableApiVersions,
    /// krabka's `legacy_request_versions_enable`.
    pub legacy: LegacyRequestVersions,
}

/// One API the broker dispatches, with the version range it decodes and the
/// range Kafka 4.3.1 serves.
///
/// `min_version..=max_version` is what the handler decodes and answers.
/// `released` is the Kafka 4.3.1 row for the key, from
/// [`krabka_raft::KAFKA_4_3_1_APIS`], or `None` when that release has no such
/// api key; it is the single table the default gates read.
///
/// Under the defaults a listener advertises and accepts `released` exactly.
/// [`UnstableApiVersions::Enabled`] lifts the maximum to `max_version` and
/// admits the api keys 4.3.1 lacks, as Kafka's `unstable.api.versions.enable`
/// does for a `latestVersionUnstable` version; [`LegacyRequestVersions::Enabled`]
/// lowers the minimum to `min_version`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogApi {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub released: Option<krabka_raft::ReleasedApi>,
}

impl CatalogApi {
    /// The catalog row for `api_key` decoded over `min_version..=max_version`,
    /// with its Kafka 4.3.1 row looked up.
    ///
    /// A krabka-private api key -- `GetReplicaLogInfo` (1020), at or above
    /// `KRABKA_PRIVATE_API_KEY_FLOOR` -- has no Kafka counterpart for either
    /// switch to track, so its whole decodable range counts as released.
    #[must_use]
    pub const fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        let released = if api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR {
            Some(krabka_raft::ReleasedApi {
                api_key,
                min_version,
                max_version,
            })
        } else {
            krabka_raft::kafka_4_3_1_api(api_key)
        };
        Self {
            api_key,
            min_version,
            max_version,
            released,
        }
    }

    /// Kafka 4.3.1's highest version of this api, or `None` when that
    /// release does not have it.
    #[must_use]
    pub const fn released_max(self) -> Option<i16> {
        match self.released {
            Some(released) => Some(released.max_version),
            None => None,
        }
    }

    /// The versions this api is accepted at under `gates`, or `None` when the
    /// api key is not accepted at all.
    #[must_use]
    pub fn accepted(self, gates: VersionGates) -> Option<std::ops::RangeInclusive<i16>> {
        let max = match gates.unstable {
            UnstableApiVersions::Enabled => self.max_version,
            UnstableApiVersions::Disabled => self.released_max()?,
        };
        let min = match (gates.legacy, self.released) {
            (LegacyRequestVersions::Disabled, Some(released)) => {
                released.min_version.max(self.min_version)
            }
            _ => self.min_version,
        };
        Some(min..=max)
    }

    /// The `ApiVersions` row for this api under `gates`, Kafka's
    /// `ApiKeys.toApiVersionForApiResponse`, or `None` when the listener
    /// leaves the key out. `Produce` keeps advertising from v0 whatever it
    /// accepts, as Kafka's `PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION` does.
    #[must_use]
    pub fn advertised(self, gates: VersionGates) -> Option<ApiVersion> {
        let accepted = self.accepted(gates)?;
        let min_version = if self.api_key == krabka_protocol::owned::produce_request::API_KEY {
            PRODUCE_ADVERTISED_MIN_VERSION.min(*accepted.start())
        } else {
            *accepted.start()
        };
        Some(ApiVersion {
            api_key: self.api_key,
            min_version,
            max_version: *accepted.end(),
            ..Default::default()
        })
    }
}

/// Kafka's `ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION`: a broker
/// listener advertises `Produce` from v0 although it accepts only v3 and up,
/// because librdkafka reads a higher minimum as no `Produce` support at all
/// (KAFKA-18659).
const PRODUCE_ADVERTISED_MIN_VERSION: i16 = 0;

/// Which of the broker's listeners an `ApiVersions` response goes out on, and
/// where [`INTER_BROKER_ONLY_APIS`] may dispatch.
///
/// Apache Kafka tags every request schema with the listener types that accept
/// it, and `ApiVersionsResponse.filterApis` drops every row whose tag does not
/// hold for the listener the request arrived on. A Kafka broker answers with
/// `ListenerType.BROKER` on every listener it binds, client-facing or
/// inter-broker alike, so a key tagged `controller` only --
/// `AlterPartition`, `BrokerRegistration`, and the rest of
/// [`INTER_BROKER_ONLY_APIS`] -- reaches no broker listener at all, and the
/// socket layer closes a connection that sends one. krabka's controller
/// listener does the same through `krabka_raft`'s own
/// `CONTROLLER_LISTENER_APIS`; this enum is the broker side of it.
///
/// krabka still has to accept and answer [`INTER_BROKER_ONLY_APIS`]
/// somewhere, because it reaches a peer over the peer's *inter-broker*
/// endpoint for those RPCs rather than over a separate controller listener.
/// That is [`InterBroker`][Self::InterBroker] and
/// [`ClientAndInterBroker`][Self::ClientAndInterBroker] alike; only
/// [`Client`][Self::Client] closes the connection instead of dispatching.
/// Advertising is narrower: only the listener no client can reach --
/// [`InterBroker`][Self::InterBroker] -- keeps them in `ApiVersions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListenerKind {
    /// A listener a Kafka client reaches and that is not
    /// `inter.broker.listener.name`. It advertises what a Kafka broker
    /// advertises, every key except [`INTER_BROKER_ONLY_APIS`], and it closes
    /// the connection rather than dispatch one of them.
    Client,
    /// The listener `inter.broker.listener.name` names, when the broker also
    /// binds at least one other, purely client-facing listener -- a Kafka
    /// deployment's `BROKER` listener, reached only by peers. It dispatches
    /// and advertises [`INTER_BROKER_ONLY_APIS`], because a krabka broker
    /// negotiates those RPCs against the table this listener advertises
    /// before it can send one.
    InterBroker,
    /// The listener `inter.broker.listener.name` names, when it is the
    /// broker's only listener -- the default single-listener configuration --
    /// and so also serves ordinary clients.
    ///
    /// It still dispatches [`INTER_BROKER_ONLY_APIS`], the same as
    /// [`InterBroker`][Self::InterBroker]: krabka's peers reach it there
    /// because there is nowhere else. It withholds them from `ApiVersions`
    /// the same as [`Client`][Self::Client]: no Kafka broker listener ever
    /// advertises a controller-scoped key, and a client can reach this one.
    ClientAndInterBroker,
}

/// Whether the broker has somewhere to send KIP-714 client metrics.
///
/// Kafka advertises `GetTelemetrySubscriptions` and `PushTelemetry` only when
/// `metric.reporters` holds a `ClientTelemetry` implementation; a stock broker
/// has none and answers `ApiVersions` without those two rows, which is what
/// keeps a modern Java or librdkafka client from opening a telemetry handshake
/// it has nowhere to push to. krabka's equivalent is
/// `client_metrics_enable`, and
/// [`crate::config::BrokerConfig::client_metrics_receiver`] reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientMetricsReceiver {
    /// A receiver is configured, so the two KIP-714 keys are advertised.
    Configured,
    /// No receiver is configured. The handlers stay registered and answer a
    /// client that sends one anyway, exactly as they do today; nothing invites
    /// a client to start.
    Absent,
}

/// The api keys scoped to the listener `inter.broker.listener.name` names:
/// withheld from `ApiVersions` on every other listener (#843), and refused --
/// connection closed, no response -- when dispatched there (#683).
///
/// Eight of them are tagged `controller` only by their request schema in
/// `krabka-protocol`, so no Kafka broker listener has ever advertised or
/// dispatched them: `AlterPartition` (56), `FetchSnapshot` (59),
/// `BrokerRegistration` (62), `BrokerHeartbeat` (63), `AllocateProducerIds`
/// (67), `ControllerRegistration` (70), `AssignReplicasToDirs` (73) and
/// `UpdateRaftVoter` (82). krabka has no separate controller listener for
/// them -- its controller listener routes several of them back into these
/// same broker handlers, through `krabka_raft`'s own
/// `CONTROLLER_LISTENER_APIS` -- so it accepts them on
/// [`ListenerKind::InterBroker`] and
/// [`ListenerKind::ClientAndInterBroker`] instead, where every handler still
/// gates on `ClusterAction`. That is not a reason to advertise or dispatch
/// them on a listener a client reaches too: a tool or an audit that reads the
/// advertised set to infer a node's role would read a krabka broker as a
/// controller, and a client that skips version negotiation would otherwise
/// still reach the handlers.
///
/// `GetReplicaLogInfo` (1020) is the ninth. It is krabka-private: Kafka trunk
/// gives 93, the key it used to hold, to `StreamsGroupTopologyDescriptionUpdate`,
/// and no released Kafka advertises either -- `mirror.gcr.io/apache/kafka:4.3.1`
/// stops at api key 92. The only caller in this tree is the KIP-966 unclean
/// recovery manager, which dials a replica's inter-broker endpoint. It
/// therefore belongs on the same side of the split as the other eight.
///
/// [`dispatched_apis`] still carries every key here at its full version
/// range, because the version bounds a listener serves at do not change; only
/// whether the listener accepts the key at all does, and that is enforced in
/// `crate::network::dispatch` by reading
/// [`crate::config::BrokerConfig::listener_kind`], not by trimming this table.
pub const INTER_BROKER_ONLY_APIS: &[i16] = {
    use krabka_protocol::owned;
    &[
        owned::alter_partition_request::API_KEY,
        owned::fetch_snapshot_request::API_KEY,
        owned::broker_registration_request::API_KEY,
        owned::broker_heartbeat_request::API_KEY,
        owned::allocate_producer_ids_request::API_KEY,
        owned::controller_registration_request::API_KEY,
        owned::assign_replicas_to_dirs_request::API_KEY,
        owned::update_raft_voter_request::API_KEY,
        owned::get_replica_log_info_request::API_KEY,
    ]
};

/// The KIP-714 client-metrics keys, advertised only behind
/// [`ClientMetricsReceiver::Configured`].
pub const CLIENT_METRICS_APIS: &[i16] = {
    use krabka_protocol::owned;
    &[
        owned::get_telemetry_subscriptions_request::API_KEY,
        owned::push_telemetry_request::API_KEY,
    ]
};

/// Every API the broker dispatches, with the version range it decodes and
/// its latest stable version, in the order the handlers were added. Update it
/// when you add a handler.
///
/// This is the union across both listener kinds and both telemetry settings.
/// [`dispatched_apis`] and [`supported_apis`] are both views of it.
#[must_use]
pub fn catalog_apis() -> Vec<CatalogApi> {
    let mut apis = client_facing_apis();
    apis.extend(admin_apis());
    apis
}

/// Every API the broker dispatches, over the whole range its handler decodes,
/// mirrored from each API's generated `MIN_VERSION` and `MAX_VERSION`.
///
/// This is what the dispatch registry takes its per-key version bounds from.
/// It is not what any listener advertises; that is [`supported_apis`]. A
/// version outside Kafka 4.3.1's range, or any version of a key that release
/// lacks, is inside this range, and [`is_disabled_version`] is what refuses it
/// under the default [`VersionGates`].
#[must_use]
pub fn dispatched_apis() -> Vec<ApiVersion> {
    catalog_apis()
        .into_iter()
        .map(|api| ApiVersion {
            api_key: api.api_key,
            min_version: api.min_version,
            max_version: api.max_version,
            ..Default::default()
        })
        .collect()
}

/// The catalog, keyed for the per-request lookup [`is_disabled_version`]
/// makes. Computed once.
static CATALOG_BY_KEY: std::sync::LazyLock<std::collections::BTreeMap<i16, CatalogApi>> =
    std::sync::LazyLock::new(|| {
        catalog_apis()
            .into_iter()
            .map(|api| (api.api_key, api))
            .collect()
    });

/// Whether `version` of `api_key` is a decodable version that `gates`
/// disables: above Kafka 4.3.1's maximum, below its minimum, or any version of
/// an api key that release does not have.
///
/// Kafka's `Processor.parseRequestHeader` asks `ApiKeys.isVersionEnabled`,
/// which refuses a version the release knows but does not enable, and throws
/// `InvalidRequestException` for it; `RequestHeader.parse` throws the same for
/// an api key the release does not know (`ApiKeys.forId`). `SocketServer`
/// closes the connection without a response either way. `ApiVersions` is
/// exempt in Kafka -- every version of it reaches `KafkaApis`, which answers
/// `UNSUPPORTED_VERSION` -- so it is never disabled here. A version outside
/// the decodable range is not this function's concern: [`dispatched_apis`]
/// refuses it first. A krabka-private api key, at or above 1000, is not in
/// the catalog and never disabled.
#[must_use]
pub fn is_disabled_version(api_key: i16, version: i16, gates: VersionGates) -> bool {
    if api_key == krabka_protocol::owned::api_versions_request::API_KEY {
        return false;
    }
    CATALOG_BY_KEY.get(&api_key).is_some_and(|api| {
        api.accepted(gates)
            .is_none_or(|accepted| !accepted.contains(&version))
    })
}

/// The API set `listener` advertises, sorted by API key.
///
/// Kafka fills the `ApiKeys` collection from `ApiKeys.apisForListener`, an
/// `EnumSet` that iterates in id order, so every Kafka broker listener answers
/// with a strictly ascending table, and so does this.
///
/// `client_metrics` gates the two KIP-714 keys on every listener kind, the way
/// Kafka gates them on a configured `ClientTelemetry` reporter. The
/// control-plane keys in [`INTER_BROKER_ONLY_APIS`] are withheld on every
/// listener a client can reach -- [`ListenerKind::Client`] and
/// [`ListenerKind::ClientAndInterBroker`] alike -- and kept only on
/// [`ListenerKind::InterBroker`], the dedicated listener no client dials
/// (#843). Dispatch is a separate, wider gate: see [`INTER_BROKER_ONLY_APIS`].
/// `gates` picks each row's range, and leaves out a key Kafka 4.3.1 lacks
/// unless unstable api versions are enabled.
#[must_use]
pub fn supported_apis(
    listener: ListenerKind,
    client_metrics: ClientMetricsReceiver,
    gates: VersionGates,
) -> Vec<ApiVersion> {
    let mut apis: Vec<ApiVersion> = catalog_apis()
        .into_iter()
        .filter(|api| {
            let withheld_control_plane = listener != ListenerKind::InterBroker
                && INTER_BROKER_ONLY_APIS.contains(&api.api_key);
            let withheld_telemetry = client_metrics == ClientMetricsReceiver::Absent
                && CLIENT_METRICS_APIS.contains(&api.api_key);
            !withheld_control_plane && !withheld_telemetry
        })
        .filter_map(|api| api.advertised(gates))
        .collect();
    apis.sort_unstable_by_key(|api| api.api_key);
    apis
}

fn client_facing_apis() -> Vec<CatalogApi> {
    use krabka_protocol::owned;
    vec![
        v!(api_versions_request),
        // Kafka 4.0 removed `Produce` v0-2, `Fetch` v0-3 and `ListOffsets`
        // v0. krabka still decodes them -- `Produce` up-converts the legacy
        // `MessageSet`, `Fetch` answers from the `kafka_3_6_2` flavor and
        // `ListOffsets` v0 from its own module -- and serves them only under
        // `LegacyRequestVersions::Enabled`. The row's Kafka 4.3.1 range is what
        // the default gate reads.
        CatalogApi::new(
            owned::produce_request::API_KEY,
            krabka_protocol::kafka_3_6_2::owned::produce_request::MIN_VERSION,
            owned::produce_request::MAX_VERSION,
        ),
        CatalogApi::new(
            owned::fetch_request::API_KEY,
            krabka_protocol::kafka_3_6_2::owned::fetch_request::MIN_VERSION,
            owned::fetch_request::MAX_VERSION,
        ),
        CatalogApi::new(
            owned::list_offsets_request::API_KEY,
            0,
            owned::list_offsets_request::MAX_VERSION,
        ),
        v!(metadata_request),
        v!(find_coordinator_request),
        v!(join_group_request),
        v!(sync_group_request),
        v!(heartbeat_request),
        v!(leave_group_request),
        v!(sasl_handshake_request),
        v!(sasl_authenticate_request),
        v!(offset_commit_request),
        v!(offset_fetch_request),
    ]
}

fn admin_apis() -> Vec<CatalogApi> {
    vec![
        v!(create_topics_request),
        v!(delete_topics_request),
        v!(delete_records_request),
        v!(init_producer_id_request),
        // AllocateProducerIds is the controller-backed broker RPC used to
        // reserve durable, cluster-wide producer-ID blocks.
        v!(allocate_producer_ids_request),
        v!(offset_for_leader_epoch_request),
        v!(add_partitions_to_txn_request),
        v!(add_offsets_to_txn_request),
        v!(end_txn_request),
        v!(write_txn_markers_request),
        // Version 6 (KIP-1319, topic ids) is Kafka trunk's; 4.3.1 stops at 5,
        // so v6 is served only under `unstable.api.versions.enable`.
        v!(txn_offset_commit_request),
        v!(describe_configs_request),
        v!(alter_replica_log_dirs_request),
        v!(describe_log_dirs_request),
        v!(describe_groups_request),
        v!(list_groups_request),
        v!(alter_configs_request),
        v!(create_partitions_request),
        v!(delete_groups_request),
        v!(incremental_alter_configs_request),
        v!(alter_partition_request),
        v!(assign_replicas_to_dirs_request),
        v!(describe_cluster_request),
        v!(broker_heartbeat_request),
        v!(broker_registration_request),
        v!(controller_registration_request),
        // UnregisterBroker (KIP-919) — admin RPC to permanently drop a
        // broker registration from the cluster's metadata image.
        v!(unregister_broker_request),
        // UnregisterController (KIP-1312, Kafka trunk) — drops a controller
        // registration. Tagged `broker` and `controller`: a broker listener
        // forwards it to the active controller. Kafka 4.3.1 has no api key 94,
        // so it is advertised and accepted only under
        // `unstable.api.versions.enable`.
        v!(unregister_controller_request),
        v!(alter_user_scram_credentials_request),
        // UpdateFeatures (api_key 57, KIP-584) — `kafka-features` admin tool
        // finalizes broker-supported features through a Raft-persisted path.
        v!(update_features_request),
        v!(describe_acls_request),
        v!(create_acls_request),
        v!(delete_acls_request),
        v!(elect_leaders_request),
        v!(alter_partition_reassignments_request),
        v!(list_partition_reassignments_request),
        // OffsetDelete (api_key 47, KIP-496): completes
        // `kafka-consumer-groups --delete-offsets` parity.
        v!(offset_delete_request),
        v!(describe_client_quotas_request),
        v!(alter_client_quotas_request),
        v!(describe_user_scram_credentials_request),
        // KIP-48: delegation-token RPCs. Conditional on the
        // broker having a master key configured is tempting, but Kafka
        // always advertises these — clients discover support at this
        // level then get DELEGATION_TOKEN_AUTH_DISABLED (61) on the
        // actual call when the broker isn't configured for tokens.
        v!(create_delegation_token_request),
        v!(renew_delegation_token_request),
        v!(expire_delegation_token_request),
        v!(describe_delegation_token_request),
        // DescribeProducers (KIP-664) — admin introspection of
        // per-(topic, partition) idempotent / transactional producer state.
        v!(describe_producers_request),
        // DescribeTransactions + ListTransactions (KIP-664) — admin
        // introspection of the TxnCoordinator's local state map.
        v!(describe_transactions_request),
        v!(list_transactions_request),
        // DescribeTopicPartitions (KIP-966) — paginated topic listing
        // used by JVM admin clients 3.7+ in place of fanned-out Metadata
        // calls for `kafka-topics --describe`.
        v!(describe_topic_partitions_request),
        // KIP-714 client-metrics push handshake, advertised only behind
        // `ClientMetricsReceiver::Configured`. Kafka advertises the pair only
        // when `metric.reporters` holds a `ClientTelemetry` implementation, so
        // a client that sees the rows has somewhere to push to.
        v!(get_telemetry_subscriptions_request),
        v!(push_telemetry_request),
        // ListConfigResources (KIP-1142) — typed enumeration of every
        // configurable resource (topics + brokers + client_metrics). v0
        // is the legacy ListClientMetricsResources surface (KIP-714); v1
        // adds the `resource_types` filter.
        v!(list_config_resources_request),
        // DescribeQuorum (KIP-595) — `kafka-metadata-quorum --describe`
        // admin introspection of the controller-raft quorum.
        v!(describe_quorum_request),
        // FetchSnapshot (KIP-630) — controller-snapshot byte-range fetch
        // used by replicas catching up via __cluster_metadata snapshots.
        v!(fetch_snapshot_request),
        // KIP-848 next-gen consumer group protocol.
        v!(consumer_group_heartbeat_request),
        v!(consumer_group_describe_request),
        // KIP-932 share-group membership protocol.
        v!(share_group_heartbeat_request),
        v!(share_group_describe_request),
        // KIP-1071 streams-group rebalance protocol. Version 1 of both
        // (KIP-1331, KIP-1357) is Kafka trunk's and is served only under
        // `unstable.api.versions.enable`.
        v!(streams_group_heartbeat_request),
        v!(streams_group_describe_request),
        // KIP-1331 topology description push (Kafka trunk). krabka stores it
        // in Kafka's in-memory plugin when
        // `group.streams.topology.description.plugin.class` names it, and
        // answers as a trunk broker without a plugin otherwise. Kafka 4.3.1
        // has no api key 93, so it is advertised and accepted only under
        // `unstable.api.versions.enable`.
        v!(streams_group_topology_description_update_request),
        // KIP-932 ShareFetch / ShareAcknowledge data-plane RPCs.
        v!(share_fetch_request),
        v!(share_acknowledge_request),
        // KIP-932 share-group admin offset RPCs.
        v!(describe_share_group_offsets_request),
        v!(alter_share_group_offsets_request),
        v!(delete_share_group_offsets_request),
        // KIP-932 share-coordinator (persister) RPCs.
        v!(initialize_share_group_state_request),
        v!(read_share_group_state_request),
        v!(write_share_group_state_request),
        v!(delete_share_group_state_request),
        v!(read_share_group_state_summary_request),
        // GetReplicaLogInfo (KIP-966) — inter-broker RPC the controller's
        // unclean recovery manager uses to read each replica's LEO + leader
        // epoch. Advertised so InterBrokerClient version negotiation succeeds.
        v!(get_replica_log_info_request),
        // KIP-853 dynamic-quorum reconfiguration — `kafka-metadata-quorum
        // --add-controller / --remove-controller` and the controller
        // auto-join path.
        v!(add_raft_voter_request),
        v!(remove_raft_voter_request),
        v!(update_raft_voter_request),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;

    /// The default gates: Kafka 4.3.1's table exactly.
    const STRICT: VersionGates = VersionGates {
        unstable: UnstableApiVersions::Disabled,
        legacy: LegacyRequestVersions::Disabled,
    };
    /// Both opt-ins on: everything krabka decodes.
    const ALL: VersionGates = VersionGates {
        unstable: UnstableApiVersions::Enabled,
        legacy: LegacyRequestVersions::Enabled,
    };
    /// Every combination of the two switches.
    const EVERY_GATE: [VersionGates; 4] = [
        STRICT,
        VersionGates {
            unstable: UnstableApiVersions::Enabled,
            legacy: LegacyRequestVersions::Disabled,
        },
        VersionGates {
            unstable: UnstableApiVersions::Disabled,
            legacy: LegacyRequestVersions::Enabled,
        },
        ALL,
    ];

    /// The client listener's table, which is what a Kafka client reads.
    fn client_apis() -> Vec<ApiVersion> {
        supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, STRICT)
    }

    #[test]
    fn share_group_apis_are_advertised() {
        let apis = client_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        assert!(keys.contains(&76));
        assert!(keys.contains(&77));
        let hb = apis.iter().find(|a| a.api_key == 76).unwrap();
        assert!(hb.min_version == 1 && hb.max_version == 1);
    }

    #[test]
    fn streams_group_apis_are_advertised() {
        let apis = client_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        // StreamsGroupHeartbeat(88), StreamsGroupDescribe(89), at Kafka
        // 4.3.1's v0; trunk's v1 is behind `unstable.api.versions.enable`.
        assert!(keys.contains(&88));
        assert!(keys.contains(&89));
        let hb = apis.iter().find(|a| a.api_key == 88).unwrap();
        assert!((hb.min_version, hb.max_version) == (0, 0));
    }

    #[test]
    fn share_coordinator_persister_apis_are_advertised() {
        let apis = client_apis();
        let keys: Vec<i16> = apis.iter().map(|a| a.api_key).collect();
        // InitializeShareGroupState(83), ReadShareGroupState(84),
        // WriteShareGroupState(85), DeleteShareGroupState(86),
        // ReadShareGroupStateSummary(87).
        for k in [83, 84, 85, 86, 87] {
            assert!(
                keys.contains(&k),
                "persister api_key {k} must be advertised"
            );
        }
    }

    #[test]
    fn supported_apis_is_nonempty_and_sane() {
        for apis in [client_apis(), dispatched_apis()] {
            assert!(!apis.is_empty(), "advertised API table must not be empty");
            // ApiVersions itself (api_key 18) is always advertised.
            assert!(apis.iter().any(|a| a.api_key == 18));
            for a in &apis {
                assert!(a.min_version <= a.max_version, "api {} min>max", a.api_key);
            }
        }
    }

    /// The keys one advertised table names, in ascending order.
    fn keys_of(apis: &[ApiVersion]) -> BTreeSet<i16> {
        apis.iter().map(|api| api.api_key).collect()
    }

    /// A client listener advertises what a Kafka broker advertises: every
    /// dispatched key except the control-plane set and, with no receiver
    /// configured, the two KIP-714 keys.
    #[test]
    fn the_client_listener_withholds_the_control_plane_and_telemetry_keys() {
        let withheld: BTreeSet<i16> = INTER_BROKER_ONLY_APIS
            .iter()
            .chain(CLIENT_METRICS_APIS)
            .copied()
            .collect();
        let dispatched = keys_of(&dispatched_apis());
        assert!(withheld.is_subset(&dispatched));
        let all = supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, ALL);
        assert!(keys_of(&all) == dispatched.difference(&withheld).copied().collect());
    }

    /// The dedicated inter-broker listener keeps the control-plane keys,
    /// because a krabka broker negotiates them against a peer's inter-broker
    /// endpoint. This is the split-listener configuration: a client cannot
    /// reach this listener.
    #[test]
    fn the_inter_broker_listener_keeps_the_control_plane_keys() {
        let apis = supported_apis(
            ListenerKind::InterBroker,
            ClientMetricsReceiver::Absent,
            STRICT,
        );
        let keys = keys_of(&apis);
        for api_key in INTER_BROKER_ONLY_APIS {
            assert!(keys.contains(api_key), "api_key {api_key}");
        }
        for api_key in CLIENT_METRICS_APIS {
            assert!(!keys.contains(api_key), "api_key {api_key}");
        }
    }

    /// #843: no listener a client can reach advertises a control-plane key,
    /// including the default single-listener broker where the sole listener
    /// carries client and inter-broker traffic together. Table-driven over
    /// every listener configuration from the issue: the default single
    /// listener, a split client/inter-broker configuration, and a listener
    /// that is neither (`Client`, e.g. a would-be controller-named listener
    /// that `inter.broker.listener.name` does not point at).
    #[test]
    fn no_client_reachable_listener_advertises_a_control_plane_key() {
        for listener in [ListenerKind::Client, ListenerKind::ClientAndInterBroker] {
            let keys = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Absent,
                STRICT,
            ));
            for api_key in INTER_BROKER_ONLY_APIS {
                assert!(
                    !keys.contains(api_key),
                    "listener {listener:?} must not advertise api_key {api_key}"
                );
            }
        }
    }

    /// A configured receiver adds the KIP-714 pair, and nothing else, to
    /// every listener kind.
    #[test]
    fn a_configured_client_metrics_receiver_adds_only_the_two_telemetry_keys() {
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            let absent = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Absent,
                STRICT,
            ));
            let configured = keys_of(&supported_apis(
                listener,
                ClientMetricsReceiver::Configured,
                STRICT,
            ));
            assert!(
                configured
                    .difference(&absent)
                    .copied()
                    .collect::<BTreeSet<i16>>()
                    == CLIENT_METRICS_APIS.iter().copied().collect()
            );
        }
    }

    /// Every advertised row keeps the version range the dispatch table serves
    /// it at, so filtering never narrows a range.
    #[test]
    fn filtering_by_listener_does_not_move_a_version_range() {
        let dispatched = dispatched_apis();
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            for metrics in [
                ClientMetricsReceiver::Absent,
                ClientMetricsReceiver::Configured,
            ] {
                for api in supported_apis(listener, metrics, ALL) {
                    assert!(dispatched.contains(&api), "api_key {}", api.api_key);
                }
            }
        }
    }

    /// #842: every listener's table is strictly ascending by api key, as
    /// Kafka's `apisForListener` `EnumSet` orders it.
    #[test]
    fn every_listener_table_is_strictly_ascending() {
        for listener in [
            ListenerKind::Client,
            ListenerKind::InterBroker,
            ListenerKind::ClientAndInterBroker,
        ] {
            for metrics in [
                ClientMetricsReceiver::Absent,
                ClientMetricsReceiver::Configured,
            ] {
                for gates in EVERY_GATE {
                    let apis = supported_apis(listener, metrics, gates);
                    assert!(
                        apis.windows(2)
                            .all(|pair| pair[0].api_key < pair[1].api_key),
                        "{listener:?} {metrics:?} {gates:?}"
                    );
                }
            }
        }
    }

    /// #784: with both switches at their defaults every listener advertises
    /// exactly Kafka 4.3.1's table -- each key's range is the release's
    /// `validVersions`, `Produce` advertised from v0 (KAFKA-18659) -- and no
    /// key that release lacks except krabka-private `GetReplicaLogInfo` on
    /// the inter-broker listener.
    #[test]
    fn the_default_table_is_kafka_4_3_1s() {
        let released: std::collections::BTreeMap<i16, (i16, i16)> = krabka_raft::KAFKA_4_3_1_APIS
            .iter()
            .map(|api| (api.api_key, (api.min_version, api.max_version)))
            .collect();
        let apis = supported_apis(
            ListenerKind::InterBroker,
            ClientMetricsReceiver::Configured,
            STRICT,
        );
        for api in &apis {
            let range = (api.min_version, api.max_version);
            match (api.api_key, released.get(&api.api_key)) {
                (0, Some(&(3, max))) => assert!(range == (0, max), "Produce"),
                (key, Some(&want)) => assert!(range == want, "api_key {key}"),
                (key, None) => assert!(
                    key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                    "api_key {key} is not in Kafka 4.3.1"
                ),
            }
        }
    }

    /// #784: what each switch moves, in the whole table. Table-driven over
    /// every combination: each row names the keys whose advertised range
    /// differs from the default table, and how.
    #[test]
    fn each_switch_moves_only_its_own_rows() {
        type Moved = Vec<(i16, Option<(i16, i16)>, Option<(i16, i16)>)>;
        let table = |gates| {
            supported_apis(
                ListenerKind::InterBroker,
                ClientMetricsReceiver::Configured,
                gates,
            )
        };
        let strict = table(STRICT);
        let moved = |gates| -> Moved {
            let other = table(gates);
            let keys: BTreeSet<i16> = keys_of(&strict).union(&keys_of(&other)).copied().collect();
            let range = |apis: &[ApiVersion], key| {
                apis.iter()
                    .find(|api| api.api_key == key)
                    .map(|api| (api.min_version, api.max_version))
            };
            keys.into_iter()
                .map(|key| (key, range(&strict, key), range(&other, key)))
                .filter(|(_, before, after)| before != after)
                .collect()
        };
        let unstable: Moved = vec![
            (18, Some((0, 4)), Some((0, 5))),
            (22, Some((0, 5)), Some((0, 6))),
            (28, Some((0, 5)), Some((0, 6))),
            // KIP-1331's per-group `ErrorMessage`.
            (42, Some((0, 2)), Some((0, 3))),
            (88, Some((0, 0)), Some((0, 1))),
            (89, Some((0, 0)), Some((0, 1))),
            (93, None, Some((0, 0))),
            (94, None, Some((0, 0))),
        ];
        let legacy: Moved = vec![
            (1, Some((4, 18)), Some((0, 18))),
            (2, Some((1, 11)), Some((0, 11))),
        ];
        let both: Moved = {
            let mut rows = legacy.clone();
            rows.extend(unstable.iter().copied());
            rows.sort_unstable_by_key(|row| row.0);
            rows
        };
        for (gates, expected) in [
            (EVERY_GATE[1], unstable),
            (EVERY_GATE[2], legacy),
            (ALL, both),
        ] {
            assert!(moved(gates) == expected, "{gates:?}");
        }
    }

    /// #784: a version is refused on receive exactly when it is not
    /// accepted, table-driven over the gated rows of every switch
    /// combination. `Produce` v0-v2 is refused by default although it is
    /// advertised, as Kafka 4.3.1 refuses it; `ApiVersions` is never refused
    /// here, since its handler answers `UNSUPPORTED_VERSION`.
    #[test]
    fn a_gated_version_is_refused_exactly_when_it_is_not_accepted() {
        let [strict, unstable, legacy, all] = EVERY_GATE;
        let cases: &[(i16, i16, [bool; 4])] = &[
            // (api_key, version, disabled under [strict, unstable, legacy, all])
            (0, 0, [true, true, false, false]),
            (0, 2, [true, true, false, false]),
            (0, 3, [false, false, false, false]),
            (1, 3, [true, true, false, false]),
            (1, 4, [false, false, false, false]),
            (2, 0, [true, true, false, false]),
            (2, 1, [false, false, false, false]),
            (18, 5, [false, false, false, false]),
            (22, 6, [true, false, true, false]),
            (28, 6, [true, false, true, false]),
            (28, 5, [false, false, false, false]),
            (88, 1, [true, false, true, false]),
            (89, 1, [true, false, true, false]),
            (93, 0, [true, false, true, false]),
            (94, 0, [true, false, true, false]),
            (1020, 0, [false, false, false, false]),
        ];
        for &(api_key, version, want) in cases {
            for (gates, disabled) in [strict, unstable, legacy, all].into_iter().zip(want) {
                assert!(
                    is_disabled_version(api_key, version, gates) == disabled,
                    "api_key {api_key} v{version} {gates:?}"
                );
            }
        }
    }

    /// The single row Kafka puts in an `UNSUPPORTED_VERSION` answer
    /// (`ApiVersionsResponse.toApiVersion(API_VERSIONS)`) is the same
    /// `ApiVersions` row every listener advertises.
    #[test]
    fn the_unsupported_version_row_is_the_advertised_api_versions_row() {
        for (gates, max_version) in [(STRICT, 4), (ALL, 5)] {
            let advertised: Vec<ApiVersion> =
                supported_apis(ListenerKind::Client, ClientMetricsReceiver::Absent, gates)
                    .into_iter()
                    .filter(|api| api.api_key == 18)
                    .collect();
            assert!(
                advertised
                    == vec![ApiVersion {
                        api_key: 18,
                        min_version: 0,
                        max_version,
                        ..Default::default()
                    }],
                "{gates:?}"
            );
            assert!(
                krabka_raft::unsupported_version_response(gates.unstable).api_keys == advertised,
                "{gates:?}"
            );
        }
    }

    /// The KIP number of a `KIP-<n>` key, or `None` for a scope-only key.
    fn kip_number(key: &str) -> Option<u32> {
        key.strip_prefix("KIP-")?.parse().ok()
    }

    #[test]
    fn kip_annotation_keys_are_unique_and_ordered() {
        let keys: Vec<&str> = KIP_ANNOTATIONS.iter().map(|row| row.key).collect();
        let unique: BTreeSet<&str> = keys.iter().copied().collect();
        assert!(unique.len() == keys.len(), "duplicate keys in {keys:?}");

        // KIP rows first, ascending; the scope-only rows after them.
        let numbers: Vec<u32> = keys.iter().filter_map(|key| kip_number(key)).collect();
        let mut sorted = numbers.clone();
        sorted.sort_unstable();
        assert!(numbers == sorted, "KIP rows are not in ascending order");
        let first_scope_row = keys.iter().position(|key| kip_number(key).is_none());
        if let Some(index) = first_scope_row {
            assert!(
                keys[index..].iter().all(|key| kip_number(key).is_none()),
                "a KIP row follows a scope-only row in {keys:?}"
            );
        }
        assert!(keys.contains(&MIXED_QUORUM_KEY));
    }

    /// Whether `path` is a Rust source file named from the repository root.
    fn is_rust_source_path(path: &str) -> bool {
        path.starts_with("crates/")
            && std::path::Path::new(path).extension() == Some(std::ffi::OsStr::new("rs"))
    }

    #[test]
    fn kip_annotation_rows_are_complete() {
        for row in KIP_ANNOTATIONS {
            assert!(!row.claim.is_empty(), "{} has no claim", row.key);
            assert!(
                is_rust_source_path(row.module),
                "{} owner {} is not a Rust source path from the repository root",
                row.key,
                row.module
            );
            for test in row.tests {
                let path = test.split_once("::").map_or(*test, |(path, _)| path);
                assert!(
                    is_rust_source_path(path),
                    "{} test {test} is not a Rust source path from the repository root",
                    row.key
                );
            }
            match row.status {
                KipStatus::Implemented => {
                    assert!(
                        !row.tests.is_empty(),
                        "{} is Implemented without a test",
                        row.key
                    );
                }
                KipStatus::Partial => {
                    assert!(
                        !row.tests.is_empty(),
                        "{} is Partial without a test",
                        row.key
                    );
                    assert!(
                        !row.note.is_empty(),
                        "{} is Partial without a note",
                        row.key
                    );
                }
                KipStatus::OutOfScope => {
                    assert!(row.tests.is_empty(), "{} is OutOfScope with tests", row.key);
                    assert!(
                        !row.note.is_empty(),
                        "{} is OutOfScope without a note",
                        row.key
                    );
                }
            }
        }
    }

    /// Both rows that rest on the mixed-quorum decision cite it, and each
    /// carries the status that decision leaves it with: the mixed-quorum row
    /// is out of scope, and KIP-590 is served-only -- the controller listener
    /// answers `Envelope`, and a broker-only node needs no forwarding path of
    /// its own because it writes over the krabka-private `SubmitChange` RPC.
    #[test]
    fn mixed_quorum_and_forwarding_rows_cite_the_raft_decision() {
        for key in [FORWARDING_KEY, MIXED_QUORUM_KEY] {
            let row = KIP_ANNOTATIONS
                .iter()
                .find(|row| row.key == key)
                .unwrap_or_else(|| panic!("{key} has no annotation"));
            assert!(
                row.note.contains(OUT_OF_SCOPE_CITATION),
                "{key} does not cite {OUT_OF_SCOPE_CITATION}"
            );
        }
        let mixed = KIP_ANNOTATIONS
            .iter()
            .find(|row| row.key == MIXED_QUORUM_KEY)
            .expect("mixed-quorum row");
        assert!(mixed.status == KipStatus::OutOfScope);

        let forwarding = KIP_ANNOTATIONS
            .iter()
            .find(|row| row.key == FORWARDING_KEY)
            .expect("KIP-590 row");
        assert!(forwarding.status == KipStatus::Implemented);
    }

    /// Every module and test path an annotation names is a file in the tree.
    ///
    /// Cargo exports `CARGO_MANIFEST_DIR`, from which the repository root is
    /// two levels up. Bazel stages no source tree for a unit test, so there
    /// the check has nothing to look at and returns; `aspect generate-kip-matrix`
    /// makes the same check in CI's docs job, against the checked-out tree,
    /// and also checks that a `path::function` entry names a function the
    /// file defines.
    #[test]
    fn kip_annotation_paths_exist() {
        // A Bazel sandbox stages only this crate's sources, so the paths the
        // rows cite in other crates are absent there even though the
        // directory exists; `TEST_SRCDIR` is how Bazel announces itself. The
        // generator in the docs CI job is the hermetic gate for these paths.
        if std::env::var_os("TEST_SRCDIR").is_some() {
            return;
        }
        let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") else {
            return;
        };
        let root = std::path::Path::new(&manifest_dir).join("../..");
        if !root.join("crates").is_dir() {
            return;
        }
        for row in KIP_ANNOTATIONS {
            assert!(
                root.join(row.module).is_file(),
                "{} owner {} is missing",
                row.key,
                row.module
            );
            for test in row.tests {
                let path = test.split_once("::").map_or(*test, |(path, _)| path);
                assert!(
                    root.join(path).is_file(),
                    "{} test {path} is missing",
                    row.key
                );
            }
        }
        let (path, _) = OUT_OF_SCOPE_CITATION.split_once(':').expect("path:line");
        assert!(
            root.join(path).is_file(),
            "{OUT_OF_SCOPE_CITATION} is missing"
        );
    }
}
