//! Cross-cutting tests for the `[runtime]` table.
//!
//! These exercise `RuntimeFileConfig` as a whole — the representative apply,
//! the quantity round trip across every millisecond and byte key, and the
//! relational checks that only run once the whole table is applied — so they
//! belong to the table's own module rather than to one domain applier.

use assert2::assert;
use krabka_units::{
    bytes,
    convert::{ByteSizeExt as _, TimeExt as _},
    days, hours, mebibytes, millis, minutes, secs,
};

use crate::file_config::FileConfig;

#[test]
fn runtime_file_config_applies_representative_values() {
    let cfg = crate::file_config::test_support::configured(
        r#"
[runtime]
cleaner_interval = "7s"
isr_scan_interval = "800ms"
opa_http_timeout = "2500ms"
fetch_max_bytes = "2MiB"
replication_fetch_max = "2MiB"
replication_fetch_max_wait = "750ms"
replication_fetch_min = "2B"
diskless_wal_flush_interval = "125ms"
diskless_wal_flush_max_size = "4MiB"
diskless_wal_hot_tail_max_size = "16MiB"
diskless_wal_trim_safety_lag = 0
diskless_wal_index_projection_timeout = "3s"
controller_heartbeat_interval = "500ms"
controller_fetch_miss_limit = 7
metadata_raft_command_queue_capacity = 512
metadata_raft_fetch_max = "4MiB"
log_segment_bytes = "1MiB"
share_group_max_size = 17
share_group_backlog_poll_interval = "250ms"
streams_group_enable = false
streams_group_max_size = 19
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(
        (
            cfg.cleaner_interval,
            cfg.isr_scan_interval,
            cfg.opa_http_timeout,
            cfg.replication.fetch_max,
            cfg.replication.fetch_max_wait,
            cfg.replication.fetch_min,
            cfg.fetch_max_bytes,
        ) == (
            secs(7),
            millis(800),
            millis(2_500),
            mebibytes(2),
            millis(750),
            bytes(2),
            mebibytes(2),
        )
    );
    assert!(cfg.controller_heartbeat_interval_explicit);
    assert!(cfg.controller_heartbeat_interval == millis(500));
    assert!(cfg.controller_fetch_miss_limit.get() == 7);
    assert!(cfg.metadata_raft_command_queue_capacity.get() == 512);
    assert!(cfg.metadata_raft_fetch_max.bytes() == 4 * 1024 * 1024);
    assert!(cfg.log_config.segment_size == mebibytes(1));
    assert!(cfg.diskless_wal_flush_interval == millis(125));
    assert!(cfg.diskless_wal_flush_max_size == mebibytes(4));
    assert!(cfg.diskless_wal_hot_tail_max_size == mebibytes(16));
    assert!(cfg.diskless_wal_trim_safety_lag == 0);
    assert!(cfg.diskless_wal_index_projection_timeout == secs(3));
    assert!(cfg.share_group.max_size == 17);
    assert!(cfg.share_group.backlog_poll_interval == std::time::Duration::from_millis(250));
    assert!(!cfg.streams_group.enable);
    assert!(cfg.streams_group.max_size == 19);
}
/// Every time and byte-size runtime key must survive the round trip
/// TOML quantity → wire integer unchanged. This is the
/// regression the `krabka-units` adoption exists to prevent: a mapping
/// that reads `30000` as 30 000 *seconds*, or writes a 30 s timeout back
/// as `30`, changes a Kafka wire field by three orders of magnitude.
#[test]
fn runtime_millisecond_and_byte_keys_round_trip_through_quantities() {
    let cfg = crate::file_config::test_support::configured(
        r#"
[runtime]
heartbeat_interval = "3s"
heartbeat_timeout = "9s"
replica_lag_time_max = "30s"
transaction_max_timeout = "15min"
producer_id_expiration = "24h"
client_metrics_default_interval = "5min"
delegation_token_max_lifetime = "7d"
socket_request_max = "100MiB"
client_metrics_telemetry_max = "1MiB"
observer_fetch_max = "1MiB"
fetch_max_bytes = "56MiB"
replication_fetch_max_wait = "500ms"
replication_fetch_max = "1MiB"
replication_fetch_min = "1B"
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    // Landed as dimensioned quantities, spelled in their natural units.
    assert!(cfg.heartbeat_interval == secs(3));
    assert!(cfg.heartbeat_timeout == secs(9));
    assert!(cfg.replica_lag_time_max == secs(30));
    assert!(cfg.transaction_max_timeout == minutes(15));
    assert!(cfg.producer_id_expiration == hours(24));
    assert!(cfg.client_metrics_default_interval == minutes(5));
    assert!(cfg.delegation_token_max_lifetime == days(7));
    assert!(cfg.socket_request_max == mebibytes(100));
    assert!(cfg.client_metrics_telemetry_max == mebibytes(1));
    assert!(cfg.observer_fetch_max == mebibytes(1));
    assert!(cfg.fetch_max_bytes == mebibytes(56));
    assert!(cfg.replication.fetch_max_wait == millis(500));
    assert!(cfg.replication.fetch_max == mebibytes(1));
    assert!(cfg.replication.fetch_min == bytes(1));

    // …and leave for the wire exactly the integers that came in.
    let millis: [(&str, i64); 8] = [
        ("heartbeat_interval", cfg.heartbeat_interval.millis_i64()),
        ("heartbeat_timeout", cfg.heartbeat_timeout.millis_i64()),
        (
            "replica_lag_time_max",
            cfg.replica_lag_time_max.millis_i64(),
        ),
        (
            "transaction_max_timeout",
            i64::from(cfg.transaction_max_timeout.millis_i32()),
        ),
        (
            "producer_id_expiration",
            cfg.producer_id_expiration.millis_i64(),
        ),
        (
            "client_metrics_default_interval",
            i64::from(cfg.client_metrics_default_interval.millis_i32()),
        ),
        (
            "delegation_token_max_lifetime",
            cfg.delegation_token_max_lifetime.millis_i64(),
        ),
        // Truncating, exactly as `build_fetch_request` narrows it for the
        // `FetchRequest.max_wait_ms` wire field.
        (
            "replication_fetch_max_wait",
            cfg.replication.fetch_max_wait.millis_i64_trunc(),
        ),
    ];
    assert!(
        millis
            == [
                ("heartbeat_interval", 3_000),
                ("heartbeat_timeout", 9_000),
                ("replica_lag_time_max", 30_000),
                ("transaction_max_timeout", 900_000),
                ("producer_id_expiration", 86_400_000),
                ("client_metrics_default_interval", 300_000),
                ("delegation_token_max_lifetime", 604_800_000),
                ("replication_fetch_max_wait", 500),
            ]
    );
    let sizes: [(&str, i64); 6] = [
        ("socket_request_max", cfg.socket_request_max.bytes_i64()),
        (
            "client_metrics_telemetry_max",
            i64::from(cfg.client_metrics_telemetry_max.bytes_i32()),
        ),
        ("observer_fetch_max", cfg.observer_fetch_max.bytes_i64()),
        (
            "fetch_max_bytes",
            i64::from(cfg.fetch_max_bytes.bytes_i32()),
        ),
        (
            "replication_fetch_max",
            i64::from(cfg.replication.fetch_max.bytes_i32()),
        ),
        (
            "replication_fetch_min",
            i64::from(cfg.replication.fetch_min.bytes_i32()),
        ),
    ];
    assert!(
        sizes
            == [
                ("socket_request_max", 104_857_600),
                ("client_metrics_telemetry_max", 1_048_576),
                ("observer_fetch_max", 1_048_576),
                ("fetch_max_bytes", 58_720_256),
                ("replication_fetch_max", 1_048_576),
                ("replication_fetch_min", 1),
            ]
    );
}
#[test]
fn runtime_file_config_rejects_relational_conflicts() {
    let cases = [(
        "[runtime]\nreplication_fetch_min = \"3B\"\nreplication_fetch_max = \"2B\"\n",
        "replication fetch minimum",
    )];

    for (source, message) in cases {
        let error = crate::file_config::test_support::configured(source, "parse runtime config")
            .expect_err("relational conflict must fail");
        assert!(error.to_string().contains(message));
    }
}

/// Kafka's `group.initial.rebalance.delay.ms` is `atLeast(0)`, so the file
/// accepts zero for it, where the other coordinator timings stay positive.
#[test]
fn runtime_file_config_accepts_a_zero_classic_group_initial_rebalance_delay() {
    let cfg = crate::file_config::test_support::configured(
        "[runtime]\nclassic_group_initial_rebalance_delay = \"0ms\"\n",
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(cfg.classic_group_initial_rebalance_delay == millis(0));
    assert!(cfg.validate().is_ok());
}

/// Kafka's defaults are `sasl.server.max.receive.size` = 524288 and
/// `connection.failed.authentication.delay.ms` = 100. The delay is
/// `atLeast(0)`, so zero is a value and turns the delay off, and a negative
/// one is an error.
#[test]
fn the_sasl_receive_limit_and_failed_authentication_delay_follow_kafka() {
    let defaults = crate::config::BrokerConfig::default();
    assert!(defaults.sasl_server_max_receive.bytes_i64() == 524_288);
    assert!(defaults.connection_failed_authentication_delay == millis(100));

    let cases = [
        (
            "[runtime]\nsasl_server_max_receive = \"64KiB\"\nconnection_failed_authentication_delay = \"0ms\"\n",
            bytes(65_536),
            std::time::Duration::ZERO,
        ),
        (
            "[runtime]\nconnection_failed_authentication_delay = \"250ms\"\n",
            bytes(524_288),
            std::time::Duration::from_millis(250),
        ),
    ];
    for (source, want_limit, want_delay) in cases {
        let file: FileConfig = toml::from_str(source).expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).expect("apply runtime config");
        assert!(cfg.sasl_server_max_receive == want_limit, "{source}");
        assert!(cfg.failed_authentication_delay() == want_delay, "{source}");
    }

    let file: FileConfig =
        toml::from_str("[runtime]\nconnection_failed_authentication_delay = \"-1ms\"\n")
            .expect("parse runtime config");
    let error = file
        .apply_to(&mut crate::config::BrokerConfig::default())
        .expect_err("a negative delay must fail");
    assert!(
        error
            .to_string()
            .contains("connection_failed_authentication_delay")
    );
}

/// The loader records which Kafka keys the `[runtime]` table named, whatever
/// their values, so `DescribeConfigs` reports a supplied setting at
/// `STATIC_BROKER_CONFIG` even at Kafka's default. A table that names none
/// records none, a field that is not a Kafka key records nothing, and a second
/// table applied afterwards, as the command-line overlay is, adds to the first.
#[test]
fn the_runtime_table_records_the_kafka_keys_it_named() {
    let recorded = |sources: &[&str]| -> Vec<&'static str> {
        let mut cfg = crate::config::BrokerConfig::default();
        for source in sources {
            let file: FileConfig = toml::from_str(source).expect("parse runtime config");
            file.apply_to(&mut cfg).expect("apply runtime config");
        }
        cfg.static_config_origins
            .supplied_kafka_keys
            .into_iter()
            .collect()
    };

    assert!(
        (
            recorded(&["[runtime]\n"]),
            recorded(&["[runtime]\ncleaner_interval = \"7s\"\n"]),
            recorded(&[
                "[runtime]\nconsumer_group_session_timeout = \"45s\"\nshare_group_max_size = 200\n\
                 streams_group_rack_aware_assignment_tags = [\"rack\"]\n"
            ]),
            recorded(&[
                "[runtime]\nconsumer_group_session_timeout = \"45s\"\n",
                "[runtime]\nsocket_send_buffer = \"1MiB\"\n",
            ]),
        ) == (
            Vec::new(),
            Vec::new(),
            vec![
                "group.consumer.session.timeout.ms",
                "group.share.max.size",
                "group.streams.rack.aware.assignment.tags",
            ],
            vec![
                "group.consumer.session.timeout.ms",
                "socket.send.buffer.bytes"
            ],
        )
    );
}

/// The five keys of Kafka's `MetadataLogConfig` set the whole of
/// `BrokerConfig::metadata_log` together, and the loader records each Kafka
/// key the table named, so `DescribeConfigs` reports it at
/// `STATIC_BROKER_CONFIG`.
#[test]
fn the_metadata_log_keys_set_the_whole_metadata_log_config() {
    let cfg = crate::file_config::test_support::configured(
        r#"
[runtime]
metadata_log_segment_bytes = "16MiB"
metadata_log_segment_roll_interval = "1h"
metadata_max_retention_bytes = "0B"
metadata_max_retention = "2d"
metadata_max_idle_interval = "0ms"
"#,
        "parse runtime config",
    )
    .expect("apply runtime config");

    assert!(
        (
            cfg.metadata_log,
            cfg.static_config_origins
                .supplied_kafka_keys
                .into_iter()
                .collect::<Vec<_>>(),
        ) == (
            krabka_raft::MetadataLogConfig {
                segment_size: mebibytes(16),
                segment_roll_interval: hours(1),
                max_retention_size: Some(bytes(0)),
                max_retention: Some(days(2)),
                max_idle_interval: millis(0),
            },
            vec![
                "metadata.log.segment.bytes",
                "metadata.log.segment.ms",
                "metadata.max.idle.interval.ms",
                "metadata.max.retention.bytes",
                "metadata.max.retention.ms",
            ],
        )
    );
}
