//! TOML file-config surface for the `krabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and applied to
//! [`crate::BrokerConfig`] by [`FileConfig::apply_to`]. Every field here is
//! `Option` or defaulted: a present value replaces the current broker value,
//! an absent one retains it.
//!
//! [`FileConfig`] is the whole document; each `[section]` it names has its own
//! module below, holding that section's TOML shape and the code that applies
//! it. `runtime_macros` is declared first and with `#[macro_use]`, because
//! `macro_rules!` is textually scoped and every `runtime_*` module expands the
//! `set_runtime_*` macros it defines.

use {
    krabka_security::ListenerProtocol, krabka_units::Time, schemars::JsonSchema, serde::Deserialize,
};

#[macro_use]
mod runtime_macros;

mod apply;
mod audit;
mod authorization;
mod delegation_token;
mod gssapi;
mod listener;
mod listener_settings;
mod oauthbearer;
mod oauthbearer_apply;
mod object_store;
mod privileged_actions;
mod process;
mod quorum_voters;
mod remote_storage;
mod runtime_config;
mod runtime_coordinators;
mod runtime_core;
mod runtime_policy;
mod runtime_storage;
mod runtime_transactions;
mod sasl_plain;
mod schema_registry;
mod tail;
mod topic_policy;
mod validate;

#[cfg(test)]
mod test_support;

pub use self::{
    audit::{
        FileAuditCheckpointConfig, FileAuditConfig, FileAuditSigningConfig, FileAuditSpoolConfig,
    },
    authorization::{AuthzType, FileAuthorizationConfig, FileOpaConfig},
    delegation_token::FileDelegationTokenConfig,
    gssapi::{FileGssapiConfig, FileInterBrokerCredentials},
    listener::{FileClientAuthMode, FileListener, FileListenerSaslConfig, FileTlsConfig},
    oauthbearer::FileOAuthBearerConfig,
    object_store::{FileRemoteStorageGcsConfig, FileRemoteStorageS3Config, FileWormConfig},
    privileged_actions::{FileBreakGlassConfig, FileFreezeConfig, FileOperatorKey},
    process::{FileProcessConfig, FileStretchConfig},
    quorum_voters::{parse_bootstrap_server, parse_quorum_voter},
    remote_storage::{FileKafkaRlmmConfig, FileRemoteStorageConfig},
    runtime_config::RuntimeFileConfig,
    sasl_plain::FileSaslPlainConfig,
    schema_registry::FileSchemaRegistryConfig,
    topic_policy::FileTopicPolicyConfig,
};

/// Schema-only stand-ins for the `krabka_units` value types.
///
/// A `Time`, `ByteSize`, or `Ratio` field is a human-readable string in the
/// TOML file, so its JSON Schema is `type: string`. That alone loses the unit,
/// and the generated reference page has a units column. Each marker type here
/// keeps the string type and adds a `format` that names the unit.
/// `#[krabka_macros::human_units]` on a config struct points each
/// `Option<Time>`, `Option<ByteSize>` and `Option<Ratio>` field at its marker
/// here and at its `krabka_units::serde_units::human` codec.
pub mod schema_units {
    use {
        schemars::{JsonSchema, Schema, SchemaGenerator, json_schema},
        std::borrow::Cow,
    };

    macro_rules! unit_marker {
        ($name:ident, $format:literal, $doc:literal) => {
            #[doc = $doc]
            pub struct $name;

            impl JsonSchema for $name {
                fn schema_name() -> Cow<'static, str> {
                    Cow::Borrowed(stringify!($name))
                }

                fn inline_schema() -> bool {
                    true
                }

                fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
                    json_schema!({ "type": "string", "format": $format })
                }
            }
        };
    }

    unit_marker!(
        Duration,
        "duration",
        "A `krabka_units::Time` written with a unit suffix, such as `\"500ms\"` or `\"30s\"`."
    );
    unit_marker!(
        ByteSize,
        "byte-size",
        "A `krabka_units::ByteSize` written with a unit suffix, such as `\"1MiB\"`."
    );
    unit_marker!(
        Ratio,
        "ratio",
        "A `krabka_units::Ratio` written as a percentage or a fraction, such as `\"10%\"`."
    );
}

/// The JSON Schema for [`FileConfig`], as a JSON value.
///
/// `krabka-broker --print-config-schema` prints this document, the checked-in
/// copy is `docs/config-schema.json`, and `aspect generate-config-reference`
/// renders `docs/config-reference.md` from it. `krabka-docgen` builds the same
/// value in process. Every `///` comment on a config field becomes the
/// `description` of that field, so the doc comments are the reference text.
///
/// # Panics
///
/// Panics if the generated schema cannot be represented as JSON, which would
/// be a bug in the `schemars` derive rather than in any input.
#[must_use]
pub fn config_schema() -> serde_json::Value {
    let schema = schemars::schema_for!(FileConfig);
    serde_json::to_value(schema).expect("FileConfig schema serializes")
}

/// Failures surfaced by [`FileConfig::apply_to`]. Each variant
/// corresponds to a specific misconfiguration the broker can diagnose
/// at startup; the variants exist (rather than a single `String`
/// fallthrough) so the binary entry point can log structured context.
#[derive(Debug, thiserror::Error)]
pub enum FileConfigError {
    /// A `[section]` referenced by another field is missing — e.g.
    /// `[authorization] type = "opa"` without an `[authorization.opa]`
    /// table. The payload names the missing section.
    #[error("missing required TOML section: {0}")]
    MissingSection(String),
    /// `OpaAuthorizer::new` failed (see [`crate::authorizer::opa::OpaConfigError`]).
    /// The payload is the underlying error's `Debug` form — formatted
    /// here rather than at the call site so the binary entry point can
    /// log a single string.
    #[error("OPA authorizer configuration error: {0}")]
    OpaConfig(String),
    /// [`crate::schema_validation::SchemaValidator::new`] failed. The payload
    /// is the underlying
    /// [`SchemaValidatorError`][crate::schema_validation::SchemaValidatorError]'s
    /// `Debug` form — formatted here rather than at the call site so the
    /// binary entry point can log a single string.
    #[error("schema registry configuration error: {0}")]
    SchemaRegistryConfig(String),
    /// The `[[operator_keys]]` trust set could not be loaded, or a section
    /// demands a signature it has no key to verify. The payload is the
    /// underlying [`OperatorKeyError`][crate::operator_keys::OperatorKeyError]
    /// message, or a description of the cross-section rule that failed —
    /// formatted here rather than at the call site so the binary entry point
    /// can log a single string.
    ///
    /// Every case is a startup error, never a downgrade to unsigned
    /// operation: a broker that quietly stopped checking signatures is the
    /// failure this variant exists to prevent.
    #[error("operator key configuration error: {0}")]
    OperatorKeys(String),
    /// A TOML section's contents conflict in a way only the apply step
    /// can diagnose — e.g. `[remote_storage]` carrying both `storage_dir`
    /// (local backend) and `[remote_storage.s3]` (object-store backend).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// A `controller_quorum_voters` entry is malformed (no `@`, non-numeric
    /// node id) or its `<host>:<port>` could not be DNS-resolved within the
    /// startup retry budget. The payload is the offending entry plus the
    /// underlying reason.
    #[error("invalid controller_quorum_voters entry: {0}")]
    InvalidQuorumVoter(String),
}

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — new fields may be added and old binaries should warn rather
/// than refuse to start.
#[krabka_macros::human_units]
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileConfig {
    /// Operational runtime policy. Present values replace the current broker
    /// value; absent values retain it.
    pub runtime: Option<RuntimeFileConfig>,
    /// This node's id, Kafka's `node.id` / `broker.id`. It is the id the
    /// broker registers with the controller and reports in `Metadata`
    /// responses. Absent leaves the `BrokerConfig` default intact.
    pub broker_id: Option<i32>,
    /// Primary log directory, the first entry of Kafka's `log.dirs`. It is
    /// the partition data directory when
    /// [`extra_log_dirs`][Self::extra_log_dirs] is empty, and it holds the
    /// `__cluster_metadata-0` raft log unless
    /// [`metadata_log_dir`][Self::metadata_log_dir] names another directory.
    /// Absent leaves the `BrokerConfig` default intact.
    pub log_dir: Option<String>,
    /// Additional JBOD data directories (KIP-113). Maps to
    /// [`crate::BrokerConfig::extra_log_dirs`].
    #[serde(default)]
    pub extra_log_dirs: Vec<String>,
    /// The metadata log directory, Kafka's `metadata.log.dir`. It holds the
    /// `__cluster_metadata-0` raft log, its snapshots, the node's
    /// `meta.properties` and the bootstrap records. A directory that is
    /// none of the data directories holds the metadata log only: no partition
    /// is placed there, and `DescribeLogDirs` does not report it. Absent keeps
    /// the metadata log in [`log_dir`][Self::log_dir], as Kafka keeps it in
    /// the first entry of `log.dirs`. `--metadata-log-dir` wins over it.
    pub metadata_log_dir: Option<String>,
    /// KIP-392: this broker's rack id. Maps to `BrokerConfig::rack`.
    pub rack: Option<String>,

    /// KIP-392: replica selector name (`"leader"` | `"rack-aware"`).
    /// Maps to `BrokerConfig::replica_selector`.
    pub replica_selector: Option<String>,

    /// `[stretch]` section — the three-site stretch deployment this node
    /// belongs to. Maps to `BrokerConfig::stretch`. Absent leaves the
    /// `BrokerConfig` default `None`, an ordinary non-stretched cluster.
    pub stretch: Option<FileStretchConfig>,
    /// How often this broker sends `BrokerHeartbeat` to the controller leader.
    /// Absent leaves the `BrokerConfig` default intact.
    pub heartbeat_interval: Option<Time>,
    /// Controller-side session timeout for broker heartbeats. Absent leaves the
    /// `BrokerConfig` default intact.
    pub heartbeat_timeout: Option<Time>,
    /// Maximum follower lag before the leader proposes ISR shrink. Absent
    /// leaves the `BrokerConfig` default intact.
    pub replica_lag_time_max: Option<Time>,
    /// Controller election timeout, Kafka's `controller.quorum.fetch.timeout.ms`.
    /// It is the follower fetch watchdog, and 1.5x of it is the leader's
    /// check-quorum window: a leader that has not been fetched from by a
    /// majority of the voters within that window resigns its epoch. Absent
    /// leaves the `BrokerConfig` default intact.
    pub controller_election_timeout: Option<Time>,
    /// Controller heartbeat interval. Absent leaves the `BrokerConfig` default intact.
    pub controller_heartbeat_interval: Option<Time>,
    /// Name of the listener that carries inter-broker traffic — raft,
    /// replication, and heartbeats. Kafka's `inter.broker.listener.name`. It
    /// must match a `name` in [`listeners`][Self::listeners] when that list is
    /// non-empty. Absent leaves the `BrokerConfig` default `"PLAINTEXT"`.
    pub inter_broker_listener_name: Option<String>,

    /// Maximum number of live broker connections across all listeners
    /// (Kafka `max.connections`). Connections accepted past this ceiling
    /// are closed immediately. Absent leaves the `BrokerConfig` default
    /// `usize::MAX` (unlimited), matching Kafka's `Integer.MAX_VALUE`.
    pub max_connections: Option<usize>,

    /// Maximum number of live connections from any single client IP
    /// (Kafka `max.connections.per.ip`). Absent leaves the `BrokerConfig`
    /// default `usize::MAX` (unlimited).
    pub max_connections_per_ip: Option<usize>,

    /// How long a connection may go without a complete request frame before
    /// the broker closes it (Kafka `connections.max.idle.ms`). Absent leaves
    /// the `BrokerConfig` default of ten minutes, which is Kafka's 600000. A
    /// `[[listeners]]` entry may carry its own `connections_max_idle`, which
    /// wins for that listener.
    pub connections_max_idle: Option<Time>,

    /// KIP-368 `connections.max.reauth.ms`: how long an authenticated SASL
    /// session may live before the client must re-authenticate in band.
    /// Absent, or non-positive, disables re-authentication, which is Kafka's
    /// default. It applies to every mechanism: PLAIN, SCRAM and GSSAPI
    /// sessions are bounded by it alone, while an OAUTHBEARER or
    /// delegation-token session expires at the earlier of its credential's
    /// expiry and this window. A `[[listeners]]` entry may carry its own
    /// `connections_max_reauth`, which wins for that listener.
    pub connections_max_reauth: Option<Time>,

    /// KIP-595 static controller quorum voter set. Each entry is
    /// `<node_id>@<host>:<port>` pointing at a broker's controller listener
    /// (port 9093). At apply time each entry is parsed (NOT DNS-resolved) and
    /// its `<host>:<port>` is carried verbatim into
    /// `BrokerConfig::controller_quorum_voters`. The inter-broker dialer
    /// re-resolves the host on every (re)connect (`TcpStream::connect`), so a
    /// peer that restarts on a new pod IP (a `StatefulSet` pod keeps its stable
    /// DNS name but gets a fresh A record) is reached again without restarting
    /// this broker — pre-resolving here would freeze the peer's boot-time IP
    /// and strand a rejoining voter. Empty leaves the single self-voter the
    /// binary seeds (standalone).
    #[serde(default)]
    pub controller_quorum_voters: Vec<String>,

    /// KIP-853 controller discovery endpoints. Hosts remain unresolved so DNS
    /// names can be refreshed on each retry.
    #[serde(default)]
    pub bootstrap_servers: Vec<String>,

    /// Enable automatic dynamic controller enrollment.
    pub auto_join: Option<bool>,

    /// TLS server name (SNI) presented when dialing a PEER's controller
    /// listener for the KIP-595 quorum. The operator renders the shared
    /// headless-Service FQDN here — a SAN on every broker's serving cert —
    /// so mTLS validation succeeds no matter which peer (resolved to a pod
    /// IP) is dialed. Absent falls back to `"localhost"`. Maps to
    /// [`crate::BrokerConfig::controller_server_name`].
    pub controller_server_name: Option<String>,

    #[serde(default)]
    pub listeners: Vec<FileListener>,
    /// Raw Apache Kafka `server.properties` keys, for the settings krabka
    /// reads under their Kafka names rather than a dedicated TOML key. The
    /// broker consults `transaction.two.phase.commit.enable`,
    /// `quota.window.num`, `quota.window.size.seconds`, `num.partitions`,
    /// `default.replication.factor`, `log.roll.ms`, `delete.topic.enable`,
    /// `auto.create.topics.enable`, `transaction.partition.verification.enable`,
    /// the KIP-1066 `cordoned.log.dirs`, the KIP-1331
    /// `group.streams.topology.description.plugin.class`, and Kafka's two
    /// internal switches. krabka cannot load a JVM class, so
    /// `group.streams.topology.description.plugin.class` accepts only
    /// `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`.
    /// krabka builds in that plugin, which keeps the latest topology
    /// description of each streams group in memory until the broker stops.
    /// `unstable.api.versions.enable`
    /// (default `false`) advertises and serves what krabka implements from
    /// Kafka trunk beyond 4.3.1: `ApiVersions` v5 (KIP-1242),
    /// `TxnOffsetCommit` v6 (KIP-1319), `StreamsGroupHeartbeat` and
    /// `StreamsGroupDescribe` v1 (KIP-1331, KIP-1357),
    /// `StreamsGroupTopologyDescriptionUpdate` (93) and `UnregisterController`
    /// (94), `InitProducerId` v6 and with 2PC the KIP-939 `keepPreparedTxn`
    /// recovery, and trunk's group config keys; while it is `false` every
    /// listener serves exactly Kafka 4.3.1's API versions.
    /// `unstable.feature.versions.enable` (default `false`) lets this node
    /// support `metadata.version` past 4.3.1's `4.3-IV0` (30), up to trunk's
    /// `4.5-IV0` (34), which the CIDR ACL hosts of KIP-1276 and the controller
    /// unregistration of KIP-1312 need. Any other entry is accepted and
    /// ignored.
    /// A key set here loses to the equivalent dedicated key, which is applied
    /// first.
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[schemars(with = "Option<String>")]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    pub tls_config: Option<FileTlsConfig>,

    /// SASL/OAUTHBEARER validator tuning. Only relevant when a
    /// listener enables the `OAUTHBEARER` mechanism.
    pub oauthbearer: Option<FileOAuthBearerConfig>,

    /// KIP-48: delegation-token master key + lifetime knobs.
    /// Env var `KRABKA_DELEGATION_TOKEN_SECRET_KEY` wins over `secret_key`
    /// here. When neither source provides a key, the broker disables
    /// delegation-token auth.
    pub delegation_token: Option<FileDelegationTokenConfig>,

    /// Principals that are unconditionally authorized for
    /// all operations, including KIP-48 delegation-token `act-as`.
    ///
    /// Super-user status does not substitute for authentication. The
    /// delegation-token RPCs (`CreateDelegationToken`,
    /// `RenewDelegationToken`, `ExpireDelegationToken`) require a principal
    /// that authenticated with SASL or mTLS, and answer
    /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) on a PLAINTEXT or
    /// one-way-TLS connection whatever this list holds.
    ///
    /// An entry may use Kafka's `User:<name>` form. As in Kafka,
    /// `User:ANONYMOUS` is accepted: it makes every unauthenticated client a
    /// super user, and a `PLAINTEXT` controller listener with a deny-capable
    /// authorizer needs it.
    ///
    /// `None` and `Some(empty)` are equivalent — both leave
    /// `BrokerConfig.super_users` empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub super_users: Option<Vec<String>>,

    /// KIP-405: tiered-storage enablement. Setting
    /// `storage_dir` turns tiered storage on broker-wide and roots the
    /// local reference `RemoteStorageManager` there.
    pub remote_storage: Option<FileRemoteStorageConfig>,

    /// Pluggable cluster authorizer + super-user list.
    /// `None` ⇒ [`crate::authorizer::AllowAllAuthorizer`] with empty
    /// super-users (default-on-no-config behavior). When `Some`, the
    /// `type` field selects the authorizer implementation; for
    /// `type = "opa"`, the `[authorization.opa]` subtable is required.
    pub authorization: Option<FileAuthorizationConfig>,

    /// `[sasl_plain]` section — where the broker's static SASL/PLAIN
    /// credential table is read from. Absent leaves the table empty, which
    /// `BrokerConfig::validate` refuses once a listener offers PLAIN.
    pub sasl_plain: Option<FileSaslPlainConfig>,

    /// `[process]` section — `KRaft` `process.roles`. Absent / empty leaves
    /// the `BrokerConfig` default `[Controller, Broker]`.
    pub process: Option<FileProcessConfig>,

    /// SASL/GSSAPI (Kerberos) accept-path config. Broker-global —
    /// there is one `[gssapi]` block per broker. Relevant when a listener
    /// enables the `GSSAPI` mechanism.
    pub gssapi: Option<FileGssapiConfig>,

    /// Credentials this broker uses to authenticate *to* peer brokers and
    /// controller listeners (inter-broker initiate path).
    pub inter_broker_credentials: Option<FileInterBrokerCredentials>,

    /// Exact authenticated principal name to broker node ID bindings for
    /// diskless WAL follower fetches.
    pub inter_broker_principal_node_ids: Option<std::collections::HashMap<String, u64>>,

    /// `FedRAMP` 20x MLA audit subsystem configuration.
    /// Absent → secure default (enabled, standard internal topic name).
    pub audit: Option<FileAuditConfig>,

    /// `[schema_registry]` section — the Confluent-compatible registry that
    /// supplies the schemas for KFC-7 broker-side validation. `None` means no
    /// topic can turn schema validation on.
    pub schema_registry: Option<FileSchemaRegistryConfig>,

    /// `[[operator_keys]]` — the shared operator key trust set.
    ///
    /// Top-level rather than nested under `[break_glass]`, because two
    /// subsystems verify against it: a freeze record's detached signature and
    /// a break-glass approval's. One provisioning step covers both. Empty is
    /// the default and means no operator key is configured.
    #[serde(default)]
    pub operator_keys: Vec<FileOperatorKey>,

    /// `[freeze]` section — the topic write-freeze registry's bounds and its
    /// signature requirement. Absent leaves the `BrokerConfig` defaults.
    pub freeze: Option<FileFreezeConfig>,

    /// `[break_glass]` section — the two-person rule over the privileged
    /// transitions. Absent leaves the `BrokerConfig` defaults, which run no
    /// break-glass workflow.
    pub break_glass: Option<FileBreakGlassConfig>,

    /// `[topic_policy]` section — the KIP-108 / KIP-133 rule set a topic must
    /// satisfy before the controller commits its creation or its config
    /// change. Absent declares no rule, which is Kafka's default of no policy
    /// class.
    pub topic_policy: Option<FileTopicPolicyConfig>,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn config_schema_produces_non_empty_json_object() {
        let schema = config_schema();
        assert!(schema.is_object());
        assert!(schema.get("properties").is_some());
    }
}
