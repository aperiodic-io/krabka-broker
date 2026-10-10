//! The broker binary's command line, as the clap parser the process entry
//! point builds its configuration from.

use std::{net::SocketAddr, path::PathBuf};

use base64::Engine as _;
use clap::Parser;
use krabka_raft::NodeId;
use krabka_units::{ByteSize, Time};
use uuid::Uuid;

use crate::runtime_args::RuntimeArgs;

#[krabka_macros::runtime_policy_fields(node_cli)]
#[krabka_macros::krabka_env]
#[derive(Debug, Parser)]
#[command(
    name = "krabka-broker",
    version,
    about = "Cluster-capable Apache Kafka-compatible krabka broker"
)]
pub struct Args {
    #[command(flatten)]
    pub runtime: RuntimeArgs,

    #[command(flatten)]
    pub profiling: krabka_telemetry::profiling::ProfilingConfig,

    /// TCP address of the client listener. Mutually exclusive with
    /// `--config-file`. A node without the `broker` role opens no client
    /// listener, as a Kafka controller-only node does not.
    #[arg(long, default_value = "127.0.0.1:9092", conflicts_with = "config_file")]
    pub listen_addr: SocketAddr,

    /// TCP address of the controller listener, which serves the raft quorum
    /// and the controller API. It must equal the endpoint that `krabka format`
    /// recorded for this node in the voter set. Default: `--listen-addr` with
    /// port 9093, or `0.0.0.0:9093` under `--config-file`.
    #[arg(long, env = "KRABKA_CONTROLLER_LISTEN_ADDR")]
    pub controller_listen_addr: Option<SocketAddr>,

    /// `host:port` to advertise to clients. Default: `listen_addr`.
    /// The operator sets it with the env var `KRABKA_ADVERTISED_LISTENER`.
    /// Mutually exclusive with `--config-file`.
    #[arg(
        long,
        env = "KRABKA_ADVERTISED_LISTENER",
        conflicts_with = "config_file"
    )]
    pub advertised_listener: Option<String>,

    /// Path to an operator-managed TOML config file. When it is set,
    /// `--listen-addr` and `--advertised-listener` must NOT be set. The
    /// listener configuration then comes from the file's `[[listeners]]`
    /// table. See `krabka_broker::file_config::FileConfig`.
    #[arg(long)]
    pub config_file: Option<PathBuf>,

    /// Print the JSON Schema of the `--config-file` document to stdout and
    /// exit. Every other flag is ignored. `docs/config-schema.json` is a
    /// checked-in copy of this output, and `docs/config-reference.md` is
    /// generated from it.
    #[arg(long)]
    pub print_config_schema: bool,

    /// Primary log directory, the first entry of Kafka's `log.dirs`. It is
    /// the default partition data directory, and it holds the
    /// cluster-metadata raft log unless `--metadata-log-dir` names another
    /// directory.
    #[arg(long, default_value = "./krabka-data")]
    pub log_dir: PathBuf,

    /// The cluster-metadata log directory, Kafka's `metadata.log.dir`. It
    /// holds the `__cluster_metadata-0` raft log, its snapshots, this node's
    /// `meta.properties` and the bootstrap records. Unset, the metadata
    /// log is in `--log-dir`. A directory that is not one of the data
    /// directories holds the metadata log only, and no partition goes there.
    #[arg(long, env = "KRABKA_METADATA_LOG_DIR")]
    pub metadata_log_dir: Option<PathBuf>,

    /// More JBOD data directories (KIP-113), comma-separated. Least-loaded
    /// placement spreads new partitions across `--log-dir` and these
    /// directories. This maps to a Kafka `log.dirs` with more than one
    /// entry.
    #[arg(
        long,
        env = "KRABKA_EXTRA_LOG_DIRS",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub extra_log_dirs: Vec<PathBuf>,

    /// Numeric broker id: Kafka's `node.id`, which is also this node's raft
    /// id. A value other than the default wins over the `broker_id` of
    /// `--config-file`.
    #[arg(long, default_value_t = krabka_broker::config::DEFAULT_BROKER_ID)]
    pub broker_id: i32,

    /// `KRaft` `process.roles`, comma-separated (`controller`, `broker`,
    /// `witness`). Default: the combined set. `witness` is a modifier that
    /// comes with the other two roles. The operator normally sets this in
    /// the `[process]` section of `--config-file` instead.
    #[arg(
        long,
        env = "KRABKA_PROCESS_ROLES",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub process_roles: Vec<String>,

    /// Cluster UUID. Every broker in the same cluster must share this
    /// value. The operator sets it with the env var `KRABKA_CLUSTER_ID`,
    /// which holds the `KafkaCluster` UID. Accepts Kafka's base64 `Uuid`
    /// form -- what `Metadata` and `DescribeCluster` report (#1042) -- or
    /// `java.util.UUID`'s hyphenated form.
    #[arg(long, env = "KRABKA_CLUSTER_ID", value_parser = parse_cluster_id)]
    pub cluster_id: Option<Uuid>,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint.
    /// An empty string or `none` disables it. Default: `0.0.0.0:9404`.
    /// That is the same port `jmx_prometheus_javaagent` uses for vanilla
    /// Kafka, so existing scrape configs apply unchanged.
    #[arg(
        long,
        env = "KRABKA_METRICS_LISTEN_ADDR",
        default_value = "0.0.0.0:9404"
    )]
    pub metrics_listen_addr: String,

    /// Bind address for the `/healthz` and `/readyz` HTTP probes.
    /// An empty string or `none` disables them. Default: `0.0.0.0:9405`,
    /// one past the metrics port. The reference Kubernetes manifests under
    /// `packaging/k8s/` point both probes at it.
    #[arg(
        long,
        env = "KRABKA_HEALTH_LISTEN_ADDR",
        default_value = "0.0.0.0:9405"
    )]
    pub health_listen_addr: String,

    /// How many `__cluster_metadata` records this node may trail the quorum's
    /// committed offset by and still answer `/readyz` with 200.
    #[arg(long, env = "KRABKA_READINESS_MAX_METADATA_LAG")]
    pub readiness_max_metadata_lag: Option<u64>,

    /// Partition disk-usage scan cadence. `0s` disables the scanner entirely.
    /// The scanner populates the `partition_disk_bytes` gauge, and the
    /// rebalancer's usage scraper reads that gauge.
    #[arg(long, env = "KRABKA_PARTITION_DISK_SCAN_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub partition_disk_scan_interval: Option<Time>,

    /// KIP-853: controller endpoints to discover the quorum leader at cold
    /// start, comma-separated `host:port`. Joiner nodes use them, that is,
    /// nodes formatted without `--standalone` or `--initial-controllers`.
    /// This maps to Kafka's `controller.quorum.bootstrap.servers`.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_BOOTSTRAP_SERVERS",
        value_delimiter = ',',
        num_args = 0..
    )]
    #[arg(value_parser = krabka_broker::file_config::parse_bootstrap_server)]
    pub controller_bootstrap_servers: Vec<String>,

    /// KIP-595 static controller voters, comma-separated
    /// `<node_id>@<host>:<port>`. Hosts are resolved on each connection.
    #[arg(
        long,
        env = "KRABKA_CONTROLLER_QUORUM_VOTERS",
        value_delimiter = ',',
        num_args = 0..,
        value_parser = krabka_broker::file_config::parse_quorum_voter
    )]
    pub controller_quorum_voters: Vec<(NodeId, String)>,

    /// KIP-853: auto-join the quorum as a voter after the node catches up as
    /// an observer. This maps to Kafka's
    /// `controller.quorum.auto.join.enable`.
    #[arg(long, env = "KRABKA_CONTROLLER_AUTO_JOIN")]
    pub controller_auto_join: bool,

    /// Capacity of the metadata Raft command queue.
    #[arg(
        long,
        env = "KRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY",
        value_parser = parse_metadata_raft_command_queue_capacity
    )]
    pub metadata_raft_command_queue_capacity: Option<usize>,

    /// Per-read and per-snapshot-request metadata Raft byte budget.
    pub metadata_raft_fetch_max: Option<ByteSize>,

    /// Controlled-shutdown leadership drain timeout in milliseconds.
    pub controlled_shutdown_drain_timeout: Option<Time>,

    /// Maximum bytes between metadata-log snapshots.
    pub metadata_max_bytes_between_snapshots: Option<ByteSize>,

    /// Maximum time between metadata-log snapshots. `0s` disables the interval cap.
    #[arg(long, env = "KRABKA_METADATA_MAX_SNAPSHOT_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub metadata_max_snapshot_interval: Option<Time>,

    /// Committed-record gap between metadata-log snapshots.
    #[arg(
        long,
        env = "KRABKA_METADATA_SNAPSHOT_INTERVAL_RECORDS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub metadata_snapshot_interval_records: Option<u64>,

    /// Maximum metadata snapshot size a follower will fetch.
    pub metadata_snapshot_fetch_max: Option<ByteSize>,

    /// Largest metadata-log segment (`metadata.log.segment.bytes`), from
    /// 8 MiB to 2147483647 bytes.
    pub metadata_log_segment_bytes: Option<ByteSize>,

    /// Longest time a metadata-log segment stays active
    /// (`metadata.log.segment.ms`).
    pub metadata_log_segment_roll_interval: Option<Time>,

    /// Largest combined size of the metadata log and its snapshots
    /// (`metadata.max.retention.bytes`).
    #[arg(
        long,
        env = "KRABKA_METADATA_MAX_RETENTION_BYTES",
        value_parser = krabka_units::parse::non_negative_byte_size
    )]
    pub metadata_max_retention_bytes: Option<ByteSize>,

    /// Age at which a metadata snapshot is deleted
    /// (`metadata.max.retention.ms`).
    #[arg(long, env = "KRABKA_METADATA_MAX_RETENTION", value_parser = krabka_units::parse::non_negative_time)]
    pub metadata_max_retention: Option<Time>,

    /// KIP-835 no-op record cadence of the active controller
    /// (`metadata.max.idle.interval.ms`). `0s` disables the no-op records.
    #[arg(long, env = "KRABKA_METADATA_MAX_IDLE_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub metadata_max_idle_interval: Option<Time>,

    /// Idle-transaction abort cleanup interval. `0s` disables the reaper.
    #[arg(long, env = "KRABKA_TXN_ABORT_CLEANUP_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub txn_abort_cleanup_interval: Option<Time>,

    /// Transactional-id expiry (`transactional.id.expiration.ms`).
    pub txn_id_expiration: Option<Time>,

    /// Transactional-id expiry sweep cadence
    /// (`transaction.remove.expired.transaction.cleanup.interval.ms`). `0s`
    /// disables the sweep.
    #[arg(long, env = "KRABKA_TXN_ID_EXPIRATION_CLEANUP_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub txn_id_expiration_cleanup_interval: Option<Time>,

    /// Auto preferred-replica election scan cadence.
    pub leader_imbalance_check_interval: Option<Time>,

    /// TLS cert/key reload polling interval. `0s` disables the watcher.
    #[arg(long, env = "KRABKA_TLS_RELOAD_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub tls_reload_interval: Option<Time>,

    /// Maximum incremental fetch-session cache slots.
    #[arg(long, env = "KRABKA_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS")]
    pub max_incremental_fetch_session_cache_slots: Option<usize>,

    /// Maximum live broker connections across all listeners.
    #[arg(long, env = "KRABKA_MAX_CONNECTIONS")]
    pub max_connections: Option<usize>,

    /// Maximum live broker connections from any single client IP.
    #[arg(long, env = "KRABKA_MAX_CONNECTIONS_PER_IP")]
    pub max_connections_per_ip: Option<usize>,

    /// Delegation-token maximum lifetime.
    pub delegation_token_max_lifetime: Option<Time>,

    /// Delegation-token expiry sweep interval.
    pub delegation_token_expiry_check_interval: Option<Time>,

    /// Delegation-token default renew period.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_RENEW_PERIOD",
        value_parser = krabka_units::parse::positive_time
    )]
    pub delegation_token_default_renew_period: Option<Time>,

    /// `RemoteLogManager` copy/retention cadence in milliseconds.
    pub remote_log_manager_interval: Option<Time>,

    /// Delegation-token HMAC master key. Prefer secrets managers over shell history.
    #[arg(
        long,
        env = "KRABKA_DELEGATION_TOKEN_SECRET_KEY",
        hide_env_values = true
    )]
    pub delegation_token_secret_key: Option<String>,

    /// Disable OpenTelemetry SDK/exporters when truthy.
    #[arg(long, env = "OTEL_SDK_DISABLED")]
    pub otel_sdk_disabled: Option<String>,

    /// KRABKA-specific OTLP endpoint override.
    #[arg(long, env = "KRABKA_OTLP_ENDPOINT")]
    pub krabka_otlp_endpoint: Option<String>,

    /// OpenTelemetry traces endpoint override.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")]
    pub otel_exporter_otlp_traces_endpoint: Option<String>,

    /// OpenTelemetry endpoint override shared by signals.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otel_exporter_otlp_endpoint: Option<String>,

    /// Enable OTLP export without setting an endpoint.
    #[arg(long, env = "KRABKA_OTLP_ENABLED")]
    pub krabka_otlp_enabled: Option<String>,

    /// OTLP protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "KRABKA_OTLP_PROTOCOL")]
    pub krabka_otlp_protocol: Option<String>,

    /// OpenTelemetry exporter protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "OTEL_EXPORTER_OTLP_PROTOCOL")]
    pub otel_exporter_otlp_protocol: Option<String>,

    /// OTLP head sampling ratio in `[0.0, 1.0]`.
    #[arg(long, env = "KRABKA_OTLP_SAMPLE_RATIO")]
    pub krabka_otlp_sample_ratio: Option<String>,

    /// OpenTelemetry sampler argument used as the trace sample ratio.
    #[arg(long, env = "OTEL_TRACES_SAMPLER_ARG")]
    pub otel_traces_sampler_arg: Option<String>,

    /// OpenTelemetry service name.
    #[arg(long, env = "OTEL_SERVICE_NAME")]
    pub otel_service_name: Option<String>,

    /// KRABKA-specific OTLP timeout.
    #[arg(long, env = "KRABKA_OTLP_TIMEOUT", value_parser = krabka_units::parse::non_negative_time)]
    pub krabka_otlp_timeout: Option<Time>,

    /// OpenTelemetry exporter timeout in seconds.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TIMEOUT_SECS")]
    pub otel_exporter_otlp_timeout_secs: Option<String>,

    /// OTLP heartbeat interval. `0s` disables heartbeats.
    #[arg(long, env = "KRABKA_OTLP_HEARTBEAT_INTERVAL", value_parser = krabka_units::parse::non_negative_time)]
    pub krabka_otlp_heartbeat_interval: Option<Time>,
}

/// Parses a `--cluster-id` value in Kafka's base64 `Uuid` form (what
/// `Metadata` and `DescribeCluster` report, e.g. `AQIDBAUGBwgJCgsMDQ4PEA`) or
/// `java.util.UUID`'s hyphenated form.
fn parse_cluster_id(value: &str) -> Result<uuid::Uuid, String> {
    if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value)
        && let Ok(bytes) = <[u8; 16]>::try_from(bytes)
    {
        return Ok(uuid::Uuid::from_bytes(bytes));
    }
    value.parse().map_err(|_| {
        format!("{value:?} is not a cluster id: neither a base64 Uuid nor a hyphenated UUID")
    })
}

fn parse_metadata_raft_command_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    krabka_raft::MetadataRaftCommandQueueCapacity::new(value)
        .map(krabka_raft::MetadataRaftCommandQueueCapacity::get)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::secs;

    use super::*;
    use crate::test_support::env_guard;

    /// `--cluster-id` accepts Kafka's base64 `Uuid` form -- what `Metadata`
    /// and `DescribeCluster` report (#1082) -- as well as the hyphenated
    /// `java.util.UUID` form.
    #[test]
    fn cluster_id_accepts_kafka_base64_and_hyphenated_uuid_forms() {
        let _guard = env_guard();
        let id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

        let base64 =
            Args::try_parse_from(["krabka-broker", "--cluster-id", "AQIDBAUGBwgJCgsMDQ4PEA"])
                .expect("parse base64 cluster id");
        assert!(base64.cluster_id == Some(id));

        let hyphenated = Args::try_parse_from(["krabka-broker", "--cluster-id", &id.to_string()])
            .expect("parse hyphenated cluster id");
        assert!(hyphenated.cluster_id == Some(id));

        assert!(
            Args::try_parse_from(["krabka-broker", "--cluster-id", "not-a-cluster-id"]).is_err()
        );
    }

    #[test]
    fn profiling_policy_reads_environment_and_cli_wins() {
        let _guard = env_guard();

        let defaults = Args::try_parse_from(["krabka-broker"]).expect("parse defaults");
        assert!(defaults.profiling == krabka_telemetry::profiling::ProfilingConfig::default());

        temp_env::with_vars(
            [
                ("KRABKA_PROFILING_CPU_DEFAULT_DURATION", Some("2s")),
                ("KRABKA_PROFILING_CPU_SAMPLE_FREQUENCY", Some("101Hz")),
            ],
            || {
                let args = Args::try_parse_from([
                    "krabka-broker",
                    "--profiling-cpu-default-duration=3s",
                    "--profiling-cpu-sample-frequency=103Hz",
                ])
                .expect("parse profiling overrides");
                assert!(args.profiling.profiling_cpu_default_duration == secs(3));
                assert!(
                    args.profiling.profiling_cpu_sample_frequency.frequency()
                        == krabka_units::per_sec(103)
                );
            },
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_listen_addr() {
        use clap::Parser;

        let _guard = env_guard();

        let res = Args::try_parse_from([
            "krabka-broker",
            "--config-file=/tmp/a.toml",
            "--listen-addr=127.0.0.1:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("listen-addr"),
            "expected clap conflict mentioning both flags, got: {s}"
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_advertised_listener() {
        use clap::Parser;

        let _guard = env_guard();

        let res = Args::try_parse_from([
            "krabka-broker",
            "--config-file=/tmp/a.toml",
            "--advertised-listener=h:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("advertised-listener"),
            "expected clap conflict, got: {s}"
        );
    }

    #[test]
    fn print_config_schema_parses_and_defaults_off() {
        let _guard = env_guard();

        let args = Args::try_parse_from(["krabka-broker", "--print-config-schema"]).unwrap();
        assert!(args.print_config_schema);
        let defaults = Args::try_parse_from(["krabka-broker"]).unwrap();
        assert!(!defaults.print_config_schema);
    }

    #[test]
    fn config_file_alone_parses() {
        use clap::Parser;

        let _guard = env_guard();

        let args = Args::try_parse_from(["krabka-broker", "--config-file=/tmp/a.toml"]).unwrap();
        assert!(args.config_file.as_deref() == Some(std::path::Path::new("/tmp/a.toml")));
        assert!(args.advertised_listener.is_none());
    }

    #[test]
    fn controller_bootstrap_server_keeps_unresolved_dns_name() {
        let _guard = env_guard();
        let endpoint = "broker-0.headless.default.svc.cluster.local:9093";
        let args =
            Args::try_parse_from(["krabka-broker", "--controller-bootstrap-servers", endpoint])
                .unwrap();
        assert!(args.controller_bootstrap_servers == [endpoint]);
    }

    #[test]
    fn controller_quorum_voter_keeps_unresolved_dns_name() {
        let _guard = env_guard();
        let endpoint = "2@broker-2.headless.default.svc.cluster.local:9093";
        let args = Args::try_parse_from(["krabka-broker", "--controller-quorum-voters", endpoint])
            .unwrap();
        assert!(args.controller_quorum_voters[0].0 == krabka_raft::NodeId(2));
        assert!(args.controller_quorum_voters[0].1 == &endpoint[2..]);
    }

    /// The values each flag is offered in [`top_level_flags_keep_their_shape`].
    /// Between them they tell every value parser `Args` uses apart: zero
    /// against positive durations and sizes, signed against unsigned, `u32`
    /// against `u64`, the metadata command-queue bound, and the address,
    /// voter and cluster-id forms.
    const PROBES: [&str; 17] = [
        "0",
        "1",
        "-1",
        "1.5",
        "40000",
        "3000000000",
        "5000000000",
        "0ms",
        "1ms",
        "0B",
        "1B",
        "3GiB",
        "true",
        "127.0.0.1:9092",
        "h:9093",
        "1@h:9093",
        "AQIDBAUGBwgJCgsMDQ4PEA",
    ];

    krabka_macros::flag_metadata_fixture!(flag_metadata);

    /// One line per flag `Args` declares itself, leaving out the flattened
    /// runtime and profiling groups: its long name, its environment variable,
    /// its default values, and which of [`PROBES`] its value parser accepts
    /// (`+`) or refuses (`-`).
    fn top_level_flag_shapes() -> String {
        use clap::{Args as _, CommandFactory as _};

        let flattened = krabka_telemetry::profiling::ProfilingConfig::augment_args(
            RuntimeArgs::augment_args(clap::Command::new("flattened")),
        );
        let command = Args::command();
        command
            .get_arguments()
            .filter(|arg| {
                flattened
                    .get_arguments()
                    .all(|inner| inner.get_id() != arg.get_id())
            })
            .map(|arg| {
                let (long, env, defaults) = flag_metadata(arg);
                let accepted = PROBES
                    .iter()
                    .map(|probe| {
                        let flag = format!("--{long}={probe}");
                        if command
                            .clone()
                            .try_get_matches_from(["krabka-broker", flag.as_str()])
                            .is_ok()
                        {
                            '+'
                        } else {
                            '-'
                        }
                    })
                    .collect::<String>();
                format!("{long} {env} {defaults:?} {accepted}")
            })
            .flat_map(|line| [line, "\n".to_owned()])
            .collect()
    }

    /// Every flag `Args` declares itself keeps its long name, environment
    /// variable, defaults and value domain.
    #[test]
    fn top_level_flags_keep_their_shape() {
        let _guard = env_guard();

        assert!(top_level_flag_shapes() == TOP_LEVEL_FLAG_SHAPES);
    }

    /// The output of [`top_level_flag_shapes`], pinned before `#[krabka_env]`
    /// took over any of the flags' attributes.
    const TOP_LEVEL_FLAG_SHAPES: &str = "\
        listen-addr  [\"127.0.0.1:9092\"] -------------+---\n\
        controller-listen-addr KRABKA_CONTROLLER_LISTEN_ADDR [] -------------+---\n\
        advertised-listener KRABKA_ADVERTISED_LISTENER [] +++++++++++++++++\n\
        config-file  [] +++++++++++++++++\n\
        print-config-schema  [] -----------------\n\
        log-dir  [\"./krabka-data\"] +++++++++++++++++\n\
        metadata-log-dir KRABKA_METADATA_LOG_DIR [] +++++++++++++++++\n\
        extra-log-dirs KRABKA_EXTRA_LOG_DIRS [] +++++++++++++++++\n\
        broker-id  [\"1\"] +++-+------------\n\
        process-roles KRABKA_PROCESS_ROLES [] +++++++++++++++++\n\
        cluster-id KRABKA_CLUSTER_ID [] ----------------+\n\
        metrics-listen-addr KRABKA_METRICS_LISTEN_ADDR [\"0.0.0.0:9404\"] +++++++++++++++++\n\
        health-listen-addr KRABKA_HEALTH_LISTEN_ADDR [\"0.0.0.0:9405\"] +++++++++++++++++\n\
        readiness-max-metadata-lag KRABKA_READINESS_MAX_METADATA_LAG [] ++--+++----------\n\
        partition-disk-scan-interval KRABKA_PARTITION_DISK_SCAN_INTERVAL [] +------++--------\n\
        controller-bootstrap-servers KRABKA_CONTROLLER_BOOTSTRAP_SERVERS [] -------------+++-\n\
        controller-quorum-voters KRABKA_CONTROLLER_QUORUM_VOTERS [] ---------------+-\n\
        controller-auto-join KRABKA_CONTROLLER_AUTO_JOIN [] -----------------\n\
        observer-lag-bound KRABKA_OBSERVER_LAG_BOUND [] ++--+++----------\n\
        heartbeat-interval KRABKA_HEARTBEAT_INTERVAL [] --------+--------\n\
        heartbeat-timeout KRABKA_HEARTBEAT_TIMEOUT [] --------+--------\n\
        replica-lag-time-max KRABKA_REPLICA_LAG_TIME_MAX [] --------+--------\n\
        controller-election-timeout KRABKA_CONTROLLER_ELECTION_TIMEOUT [] --------+--------\n\
        controller-heartbeat-interval KRABKA_CONTROLLER_HEARTBEAT_INTERVAL [] --------+--------\n\
        controller-fetch-miss-limit KRABKA_CONTROLLER_FETCH_MISS_LIMIT [] -+--++-----------\n\
        metadata-raft-command-queue-capacity KRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY [] -+--+++----------\n\
        metadata-raft-fetch-max KRABKA_METADATA_RAFT_FETCH_MAX [] ----------++-----\n\
        controlled-shutdown-drain-timeout KRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT [] --------+--------\n\
        metadata-max-bytes-between-snapshots KRABKA_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS [] ----------++-----\n\
        metadata-max-snapshot-interval KRABKA_METADATA_MAX_SNAPSHOT_INTERVAL [] +------++--------\n\
        metadata-snapshot-interval-records KRABKA_METADATA_SNAPSHOT_INTERVAL_RECORDS [] -+--+++----------\n\
        metadata-snapshot-fetch-max KRABKA_METADATA_SNAPSHOT_FETCH_MAX [] ----------++-----\n\
        metadata-log-segment-bytes KRABKA_METADATA_LOG_SEGMENT_BYTES [] ----------++-----\n\
        metadata-log-segment-roll-interval KRABKA_METADATA_LOG_SEGMENT_ROLL_INTERVAL [] --------+--------\n\
        metadata-max-retention-bytes KRABKA_METADATA_MAX_RETENTION_BYTES [] +--------+++-----\n\
        metadata-max-retention KRABKA_METADATA_MAX_RETENTION [] +------++--------\n\
        metadata-max-idle-interval KRABKA_METADATA_MAX_IDLE_INTERVAL [] +------++--------\n\
        txn-abort-cleanup-interval KRABKA_TXN_ABORT_CLEANUP_INTERVAL [] +------++--------\n\
        txn-id-expiration KRABKA_TXN_ID_EXPIRATION [] --------+--------\n\
        txn-id-expiration-cleanup-interval KRABKA_TXN_ID_EXPIRATION_CLEANUP_INTERVAL [] +------++--------\n\
        leader-imbalance-check-interval KRABKA_LEADER_IMBALANCE_CHECK_INTERVAL [] --------+--------\n\
        tls-reload-interval KRABKA_TLS_RELOAD_INTERVAL [] +------++--------\n\
        max-incremental-fetch-session-cache-slots KRABKA_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS [] ++--+++----------\n\
        max-connections KRABKA_MAX_CONNECTIONS [] ++--+++----------\n\
        max-connections-per-ip KRABKA_MAX_CONNECTIONS_PER_IP [] ++--+++----------\n\
        delegation-token-max-lifetime KRABKA_DELEGATION_TOKEN_MAX_LIFETIME [] --------+--------\n\
        delegation-token-expiry-check-interval KRABKA_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL [] --------+--------\n\
        delegation-token-default-renew-period KRABKA_DELEGATION_TOKEN_RENEW_PERIOD [] --------+--------\n\
        remote-log-manager-interval KRABKA_REMOTE_LOG_MANAGER_INTERVAL [] --------+--------\n\
        delegation-token-secret-key KRABKA_DELEGATION_TOKEN_SECRET_KEY [] +++++++++++++++++\n\
        otel-sdk-disabled OTEL_SDK_DISABLED [] +++++++++++++++++\n\
        krabka-otlp-endpoint KRABKA_OTLP_ENDPOINT [] +++++++++++++++++\n\
        otel-exporter-otlp-traces-endpoint OTEL_EXPORTER_OTLP_TRACES_ENDPOINT [] +++++++++++++++++\n\
        otel-exporter-otlp-endpoint OTEL_EXPORTER_OTLP_ENDPOINT [] +++++++++++++++++\n\
        krabka-otlp-enabled KRABKA_OTLP_ENABLED [] +++++++++++++++++\n\
        krabka-otlp-protocol KRABKA_OTLP_PROTOCOL [] +++++++++++++++++\n\
        otel-exporter-otlp-protocol OTEL_EXPORTER_OTLP_PROTOCOL [] +++++++++++++++++\n\
        krabka-otlp-sample-ratio KRABKA_OTLP_SAMPLE_RATIO [] +++++++++++++++++\n\
        otel-traces-sampler-arg OTEL_TRACES_SAMPLER_ARG [] +++++++++++++++++\n\
        otel-service-name OTEL_SERVICE_NAME [] +++++++++++++++++\n\
        krabka-otlp-timeout KRABKA_OTLP_TIMEOUT [] +------++--------\n\
        otel-exporter-otlp-timeout-secs OTEL_EXPORTER_OTLP_TIMEOUT_SECS [] +++++++++++++++++\n\
        krabka-otlp-heartbeat-interval KRABKA_OTLP_HEARTBEAT_INTERVAL [] +------++--------\n\
";
}
