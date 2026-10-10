//! Tests for the overlay of the command line onto a `BrokerConfig`.

use assert2::assert;
use clap::Parser;
use krabka_units::{convert::TimeExt as _, secs};

use super::*;
use crate::test_support::env_guard;

#[test]
fn runtime_policy_cli_rejects_invalid_and_accepts_valid_values() {
    let _guard = env_guard();

    let cases = [
        (vec!["krabka-broker", "--cleaner-interval=0ms"], false),
        (vec!["krabka-broker", "--cleaner-interval=1ms"], true),
        (
            vec![
                "krabka-broker",
                "--classic-group-initial-rebalance-delay=0ms",
            ],
            true,
        ),
        (
            vec!["krabka-broker", "--sync-group-follower-wait=0ms"],
            false,
        ),
        (
            vec!["krabka-broker", "--offsets-topic-replication-factor=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--offsets-topic-replication-factor=1"],
            true,
        ),
        (vec!["krabka-broker", "--replication-fetch-min=0B"], false),
        (vec!["krabka-broker", "--replication-fetch-min=1B"], true),
        (
            vec!["krabka-broker", "--metadata-snapshot-fetch-max=0B"],
            false,
        ),
        (
            vec!["krabka-broker", "--metadata-snapshot-fetch-max=512MiB"],
            true,
        ),
        (
            vec!["krabka-broker", "--record-decompression-max-ratio=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--record-decompression-max-ratio=50"],
            true,
        ),
        (
            vec!["krabka-broker", "--record-decompression-output-floor=0B"],
            false,
        ),
        (
            vec!["krabka-broker", "--client-dispatch-queue-capacity=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-local-replica-count=0"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-local-replica-count=5"],
            true,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-flush-interval=0ms"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-flush-max-size=4MiB"],
            true,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-hot-tail-max-size=16MiB"],
            true,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-trim-safety-lag=-1"],
            false,
        ),
        (
            vec!["krabka-broker", "--diskless-wal-trim-safety-lag=0"],
            true,
        ),
        (vec!["krabka-broker", "--client-frame-max=101MiB"], false),
        (
            vec![
                "krabka-broker",
                "--client-dispatch-queue-capacity=7",
                "--client-frame-max=32KiB",
            ],
            true,
        ),
        (
            vec![
                "krabka-broker",
                "--record-decompression-output-ceiling=512MiB",
            ],
            true,
        ),
    ];

    for (args, accepted) in cases {
        assert!(Args::try_parse_from(args).is_ok() == accepted);
    }

    // KIP-460: the cluster-wide imbalance ratio is the ZooKeeper controller's
    // gate, and the KRaft controller krabka follows has none. The flag is gone
    // rather than accepted-and-ignored, so a start-up that still passes it
    // fails loudly instead of quietly rebalancing on every tick.
    assert!(Args::try_parse_from(["krabka-broker", "--leader-imbalance-per-broker=100%"]).is_err());

    let args = Args::try_parse_from(["krabka-broker", "--record-decompression-max-ratio=101"])
        .expect("parse positive ratio");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_err()
    );

    let args = Args::try_parse_from(["krabka-broker", "--metadata-snapshot-fetch-max=1073741825B"])
        .expect("parse dimensioned over-ceiling size");
    assert!(
        args.apply_runtime_to(&mut BrokerConfig::default(), None)
            .is_err()
    );
}

#[test]
fn runtime_policy_cli_reads_krabka_environment() {
    let _guard = env_guard();

    temp_env::with_vars(
        [
            ("KRABKA_CLEANER_INTERVAL", Some("17ms")),
            ("KRABKA_SOCKET_REQUEST_MAX", Some("100MiB")),
            ("KRABKA_SASL_SERVER_MAX_RECEIVE", Some("64KiB")),
            ("KRABKA_CONNECTION_FAILED_AUTHENTICATION_DELAY", Some("0ms")),
            ("KRABKA_METADATA_SNAPSHOT_FETCH_MAX", Some("512MiB")),
            ("KRABKA_CONTROLLER_HEARTBEAT_INTERVAL", Some("500ms")),
            ("KRABKA_CONTROLLER_FETCH_MISS_LIMIT", Some("7")),
            ("KRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY", Some("512")),
            ("KRABKA_METADATA_RAFT_FETCH_MAX", Some("4MiB")),
            ("KRABKA_RECORD_DECOMPRESSION_MAX_RATIO", Some("50")),
            ("KRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR", Some("8MiB")),
            ("KRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING", Some("512MiB")),
            ("KRABKA_LOG_READ_BUFFER_CAP", Some("2MiB")),
            ("KRABKA_LOG_READ_AHEAD_MAX", Some("8MiB")),
            ("KRABKA_LOG_TIMESTAMP_SCAN_WINDOW", Some("32KiB")),
            ("KRABKA_TRANSACTION_RECOVERY_READ_MAX", Some("3MiB")),
            ("KRABKA_DISKLESS_WAL_LOCAL_REPLICA_COUNT", Some("5")),
            ("KRABKA_DISKLESS_WAL_FLUSH_INTERVAL", Some("125ms")),
            ("KRABKA_DISKLESS_WAL_FLUSH_MAX_SIZE", Some("4MiB")),
            ("KRABKA_DISKLESS_WAL_HOT_TAIL_MAX_SIZE", Some("16MiB")),
            ("KRABKA_DISKLESS_WAL_TRIM_SAFETY_LAG", Some("0")),
            ("KRABKA_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT", Some("3s")),
            ("KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
            ("KRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
            ("KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY", Some("0ms")),
        ],
        || {
            let args = Args::try_parse_from(["krabka-broker"]).expect("parse environment");
            assert!(args.runtime.cleaner_interval == Some(Time::from_millis(17)));
            assert!(args.runtime.socket_request_max == Some(krabka_units::mebibytes(100)));
            assert!(args.metadata_snapshot_fetch_max == Some(krabka_units::mebibytes(512)));
            assert!(args.controller_fetch_miss_limit == Some(7));
            assert!(args.metadata_raft_command_queue_capacity == Some(512));
            assert!(args.metadata_raft_fetch_max == Some(krabka_units::mebibytes(4)));
            assert!(
                args.runtime.record_decompression_max_ratio == Some(krabka_units::fraction(50.0))
            );
            let mut config = BrokerConfig::default();
            args.apply_runtime_to(&mut config, None)
                .expect("apply environment runtime");
            assert!(config.sasl_server_max_receive == krabka_units::kibibytes(64));
            assert!(config.connection_failed_authentication_delay == krabka_units::millis(0));
            assert!(config.metadata_snapshot_fetch_max == krabka_units::mebibytes(512));
            assert!(config.controller_heartbeat_interval_explicit);
            assert!(config.controller_heartbeat_interval == krabka_units::millis(500));
            assert!(config.controller_fetch_miss_limit.get() == 7);
            assert!(config.metadata_raft_command_queue_capacity.get() == 512);
            assert!(config.metadata_raft_fetch_max.bytes() == 4 * 1024 * 1024);
            assert!(
                config.record_decompression_policy().unwrap().output_floor()
                    == krabka_units::mebibytes(8)
            );
            assert!(config.log_config.read_buffer_cap == krabka_units::mebibytes(2));
            assert!(config.log_config.read_ahead_max == krabka_units::mebibytes(8));
            assert!(config.log_config.timestamp_scan_window == krabka_units::kibibytes(32));
            assert!(config.transaction_recovery_read_max == krabka_units::mebibytes(3));
            assert!(config.diskless_wal_local_replica_count == 5);
            assert!(config.diskless_wal_flush_interval == krabka_units::millis(125));
            assert!(config.diskless_wal_flush_max_size == krabka_units::mebibytes(4));
            assert!(config.diskless_wal_hot_tail_max_size == krabka_units::mebibytes(16));
            assert!(config.diskless_wal_trim_safety_lag == 0);
            assert!(config.diskless_wal_index_projection_timeout == krabka_units::secs(3));
            assert!(config.client_dispatch_queue_capacity.get() == 7);
            assert!(config.client_frame_max.size() == krabka_units::kibibytes(32));
            assert!(config.classic_group_initial_rebalance_delay == krabka_units::millis(0));
            assert!(config.validate().is_ok());
        },
    );
}

#[test]
fn client_resource_policy_defaults_and_cli_precedence() {
    let _guard = env_guard();

    let defaults = Args::try_parse_from(["krabka-broker"]).expect("parse defaults");
    let mut config = BrokerConfig::default();
    defaults
        .apply_runtime_to(&mut config, None)
        .expect("apply defaults");
    assert!(config.client_dispatch_queue_capacity.get() == 64);
    assert!(config.client_frame_max.size() == krabka_units::mebibytes(100));

    temp_env::with_vars(
        [
            ("KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
            ("KRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
        ],
        || {
            let args = Args::try_parse_from([
                "krabka-broker",
                "--client-dispatch-queue-capacity=9",
                "--client-frame-max=64KiB",
            ])
            .expect("parse CLI overrides");
            let mut config = BrokerConfig::default();
            args.apply_runtime_to(&mut config, None)
                .expect("apply CLI overrides");
            assert!(config.client_dispatch_queue_capacity.get() == 9);
            assert!(config.client_frame_max.size() == krabka_units::kibibytes(64));
        },
    );
}

#[test]
fn group_member_limits_and_streams_switch_apply_from_cli() {
    let _guard = env_guard();

    let args = Args::try_parse_from([
        "krabka-broker",
        "--share-group-max-size=17",
        "--share-group-partition-max-record-locks=150",
        "--share-group-delivery-count-limit=3",
        "--streams-group-enable=false",
        "--streams-group-max-size=19",
        "--streams-group-rack-aware-assignment-tags=zone,rack",
    ])
    .expect("parse group limits");
    let mut config = BrokerConfig::default();

    args.apply_runtime_to(&mut config, None)
        .expect("apply group limits");

    assert!(config.share_group.max_size == 17);
    assert!(config.share_group.max_inflight_records == 150);
    assert!(config.share_group.max_delivery_attempts == 3);
    assert!(!config.streams_group.enable);
    assert!(config.streams_group.max_size == 19);
    assert!(config.streams_group.rack_aware_assignment_tags == ["zone", "rack"]);
}

/// `--transaction-partition-verification-enable` is the static layer of a
/// dynamic Kafka config: absent it leaves Kafka's default, and either value
/// records that the operator named the key.
#[test]
fn partition_verification_switch_applies_from_cli() {
    let _guard = env_guard();

    // (the flag, the value, whether the operator named the key)
    let cases = [
        (None, true, false),
        (
            Some("--transaction-partition-verification-enable=false"),
            false,
            true,
        ),
        (
            Some("--transaction-partition-verification-enable=true"),
            true,
            true,
        ),
    ];
    for (flag, want, named) in cases {
        let args = Args::try_parse_from(std::iter::once("krabka-broker").chain(flag))
            .expect("parse the switch");
        let mut config = BrokerConfig::default();

        args.apply_runtime_to(&mut config, None)
            .expect("apply the switch");

        assert!(
            (
                config.transaction_partition_verification_enable,
                config
                    .static_config_origins
                    .supplied_kafka_keys
                    .contains("transaction.partition.verification.enable")
            ) == (want, named),
            "{flag:?}"
        );
    }
}

fn file_runtime_with_nondefault_values() -> krabka_broker::file_config::FileConfig {
    toml::from_str(
        r#"
        [runtime]
        cleaner_interval = "7s"
        controlled_shutdown_drain_timeout = "9s"
        auto_join_voter_request_timeout = "9s"
        share_state_replication_factor = 2
        transaction_state_replication_factor = 2
        offsets_topic_replication_factor = 2
        "#,
    )
    .expect("parse runtime file config")
}

fn nondefault_runtime_config() -> (BrokerConfig, Option<krabka_units::Time>) {
    let mut config = BrokerConfig::default();
    let file = file_runtime_with_nondefault_values();
    let shutdown = file
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
    file.apply_to(&mut config).expect("apply file runtime");
    (config, shutdown)
}

#[test]
fn explicit_cli_default_runtime_values_override_file() {
    let _guard = env_guard();

    let args = Args::try_parse_from([
        "krabka-broker",
        "--cleaner-interval=30s",
        "--controlled-shutdown-drain-timeout=20s",
        "--auto-join-voter-request-timeout=30s",
        "--share-state-replication-factor=3",
        "--transaction-state-replication-factor=3",
        "--offsets-topic-replication-factor=3",
    ])
    .expect("parse explicit CLI defaults");
    let (mut config, file_shutdown) = nondefault_runtime_config();

    let shutdown = args
        .apply_runtime_to(&mut config, file_shutdown)
        .expect("overlay CLI runtime");

    check_default_runtime_overlay(&config, shutdown);
}

#[test]
fn explicit_env_default_runtime_values_override_file() {
    let _guard = env_guard();

    temp_env::with_vars(
        [
            ("KRABKA_CLEANER_INTERVAL", Some("30s")),
            ("KRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT", Some("20s")),
            ("KRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT", Some("30s")),
            ("KRABKA_SHARE_STATE_REPLICATION_FACTOR", Some("3")),
            ("KRABKA_TRANSACTION_STATE_REPLICATION_FACTOR", Some("3")),
            ("KRABKA_OFFSETS_TOPIC_REPLICATION_FACTOR", Some("3")),
        ],
        || {
            let args = Args::try_parse_from(["krabka-broker"]).expect("parse env defaults");
            let (mut config, file_shutdown) = nondefault_runtime_config();

            let shutdown = args
                .apply_runtime_to(&mut config, file_shutdown)
                .expect("overlay env runtime");

            check_default_runtime_overlay(&config, shutdown);
        },
    );
}

/// The five settings Kafka writes into the coordinator topics' configs reach
/// the broker config from the command line, and a segment size outside
/// Kafka's `int` range is refused at parse time.
#[test]
fn internal_topic_config_settings_apply_from_cli() {
    let _guard = env_guard();

    assert!(Args::try_parse_from(["krabka-broker", "--offsets-topic-segment-bytes=2GiB"]).is_err());
    assert!(Args::try_parse_from(["krabka-broker", "--transaction-state-min-isr=0"]).is_err());

    let args = Args::try_parse_from([
        "krabka-broker",
        "--offsets-topic-segment-bytes=2MiB",
        "--transaction-state-segment-bytes=3MiB",
        "--transaction-state-min-isr=1",
        "--share-state-segment-bytes=4MiB",
        "--share-state-min-isr=3",
    ])
    .expect("parse internal topic settings");
    let mut config = BrokerConfig::default();
    args.apply_runtime_to(&mut config, None)
        .expect("overlay CLI runtime");

    assert!(
        (
            config.offsets_topic_segment_bytes,
            config.transaction_state_segment_bytes,
            config.transaction_state_min_isr,
            config.share_coordinator.state_topic_segment_bytes,
            config.share_coordinator.state_topic_min_isr,
        ) == (
            krabka_units::mebibytes(2),
            krabka_units::mebibytes(3),
            1,
            krabka_units::mebibytes(4),
            3,
        )
    );
}

/// The five `MetadataLogConfig` flags, and their environment variables, set
/// the whole of `BrokerConfig::metadata_log` over what the file set. A flag
/// value outside Kafka's domain is refused when the overlay applies, or at
/// parse time when it is negative.
#[test]
fn metadata_log_settings_apply_from_cli_and_environment() {
    use krabka_units::{bytes, days, hours, mebibytes, millis};

    let _guard = env_guard();

    let file: krabka_broker::file_config::FileConfig = toml::from_str(
        r#"
        [runtime]
        metadata_log_segment_bytes = "32MiB"
        metadata_max_retention = "1d"
        metadata_max_idle_interval = "250ms"
        "#,
    )
    .expect("parse runtime file config");
    let flags = Args::try_parse_from([
        "krabka-broker",
        "--metadata-log-segment-bytes=16MiB",
        "--metadata-log-segment-roll-interval=1h",
        "--metadata-max-retention-bytes=0B",
    ])
    .expect("parse metadata log flags");
    let mut from_flags = BrokerConfig::default();
    file.apply_to(&mut from_flags).expect("apply file runtime");
    flags
        .apply_runtime_to(&mut from_flags, None)
        .expect("overlay CLI runtime");

    let from_environment = temp_env::with_vars(
        [
            ("KRABKA_METADATA_LOG_SEGMENT_BYTES", Some("8MiB")),
            ("KRABKA_METADATA_LOG_SEGMENT_ROLL_INTERVAL", Some("2h")),
            ("KRABKA_METADATA_MAX_RETENTION_BYTES", Some("200MiB")),
            ("KRABKA_METADATA_MAX_RETENTION", Some("2d")),
            ("KRABKA_METADATA_MAX_IDLE_INTERVAL", Some("0ms")),
        ],
        || {
            let args = Args::try_parse_from(["krabka-broker"]).expect("parse environment");
            let mut config = BrokerConfig::default();
            args.apply_runtime_to(&mut config, None)
                .expect("apply environment runtime");
            config.metadata_log
        },
    );

    assert!(
        (from_flags.metadata_log, from_environment)
            == (
                krabka_raft::MetadataLogConfig {
                    segment_size: mebibytes(16),
                    segment_roll_interval: hours(1),
                    max_retention_size: Some(bytes(0)),
                    max_retention: Some(days(1)),
                    max_idle_interval: millis(250),
                },
                krabka_raft::MetadataLogConfig {
                    segment_size: mebibytes(8),
                    segment_roll_interval: hours(2),
                    max_retention_size: Some(mebibytes(200)),
                    max_retention: Some(days(2)),
                    max_idle_interval: millis(0),
                },
            )
    );

    // (the flag, whether it parses, whether the overlay then applies)
    let cases = [
        ("--metadata-log-segment-bytes=8MiB", true, true),
        ("--metadata-log-segment-bytes=4MiB", true, false),
        ("--metadata-log-segment-bytes=2GiB", true, false),
        ("--metadata-log-segment-bytes=0B", false, false),
        ("--metadata-log-segment-roll-interval=0ms", false, false),
        ("--metadata-log-segment-roll-interval=250us", true, false),
        ("--metadata-max-retention-bytes=1.5B", true, false),
        ("--metadata-max-retention-bytes=-1B", false, false),
        ("--metadata-max-retention=0ms", true, true),
        ("--metadata-max-retention=-1ms", false, false),
        ("--metadata-max-idle-interval=0ms", true, true),
        ("--metadata-max-idle-interval=2147483648ms", true, false),
    ];
    let mut actual = Vec::with_capacity(cases.len());
    for (flag, _, _) in cases {
        let outcome =
            Args::try_parse_from(["krabka-broker", flag]).map_or((false, false), |args| {
                let applied = args
                    .apply_runtime_to(&mut BrokerConfig::default(), None)
                    .is_ok();
                (true, applied)
            });
        actual.push((flag, outcome.0, outcome.1));
    }
    assert!(actual == cases);
}

/// `--queued-max-requests` parses into a `PositiveCount` and reaches the
/// runtime config as its `usize`.
#[test]
fn queued_max_requests_flag_reaches_the_runtime_config() {
    let _guard = env_guard();

    let args = Args::try_parse_from(["krabka-broker", "--queued-max-requests=5"])
        .expect("parse --queued-max-requests");
    assert!(
        args.runtime
            .queued_max_requests
            .map(krabka_broker::config_value::PositiveCount::into_value)
            == Some(5)
    );
}

fn check_default_runtime_overlay(config: &BrokerConfig, shutdown: krabka_units::Time) {
    assert!(
        (
            config.cleaner_interval,
            shutdown,
            config.auto_join_voter_request_timeout,
            config.share_coordinator.state_topic_replication_factor,
            config.transaction_state_replication_factor,
            config.offsets_topic_replication_factor,
        ) == (secs(30), secs(20), secs(30), 3, 3, 3)
    );
}
