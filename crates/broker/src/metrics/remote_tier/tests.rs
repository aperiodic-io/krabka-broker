//! Each path's counters land on their own series, a zero-byte move records
//! nothing, and the lag gauges are set rather than accumulated.

use {super::*, assert2::check};

/// The `krabka_broker_*` sample lines a fresh registry renders.
fn body(metrics: &BrokerMetrics) -> String {
    let mut out = String::new();
    prometheus_client::encoding::text::encode(
        &mut out,
        &metrics.registry.try_lock().expect("registry"),
    )
    .expect("encode");
    out
}

#[test]
fn each_path_counts_requests_errors_and_bytes_on_its_own_series() {
    let metrics = BrokerMetrics::new();

    for path in [
        RemoteTierPath::Copy,
        RemoteTierPath::Fetch,
        RemoteTierPath::Delete,
    ] {
        metrics.record_remote_request(path, "orders");
        metrics.record_remote_error(path, "orders");
        metrics.record_remote_bytes(path, "orders", 512);
    }

    let rendered = body(&metrics);
    for series in [
        "krabka_broker_remote_copy_requests_total{topic=\"orders\"} 1",
        "krabka_broker_remote_fetch_requests_total{topic=\"orders\"} 1",
        "krabka_broker_remote_delete_requests_total{topic=\"orders\"} 1",
        "krabka_broker_remote_copy_errors_total{topic=\"orders\"} 1",
        "krabka_broker_remote_fetch_errors_total{topic=\"orders\"} 1",
        "krabka_broker_remote_delete_errors_total{topic=\"orders\"} 1",
        "krabka_broker_remote_copy_bytes_total{topic=\"orders\"} 512",
        "krabka_broker_remote_fetch_bytes_total{topic=\"orders\"} 512",
    ] {
        check!(rendered.contains(series), "missing {series}");
    }
    // The delete path moves no bytes, so it has no byte counter to land on.
    check!(!rendered.contains("krabka_broker_remote_delete_bytes"));
}

#[test]
fn a_zero_byte_move_materialises_no_series() {
    let metrics = BrokerMetrics::new();

    metrics.record_remote_bytes(RemoteTierPath::Copy, "orders", 0);

    check!(!body(&metrics).contains("krabka_broker_remote_copy_bytes_total{"));
}

/// Lag is a level, not a total: a second round that finds less to do must
/// report less, not more.
#[test]
fn lag_gauges_take_the_newest_reading() {
    let metrics = BrokerMetrics::new();

    metrics.set_remote_copy_lag("orders", 9, 900);
    metrics.set_remote_copy_lag("orders", 2, 200);
    metrics.set_remote_delete_lag("orders", 7, 700);
    metrics.set_remote_delete_lag("orders", 0, 0);

    let rendered = body(&metrics);
    for series in [
        "krabka_broker_remote_copy_lag_segments{topic=\"orders\"} 2",
        "krabka_broker_remote_copy_lag_bytes{topic=\"orders\"} 200",
        "krabka_broker_remote_delete_lag_segments{topic=\"orders\"} 0",
        "krabka_broker_remote_delete_lag_bytes{topic=\"orders\"} 0",
    ] {
        check!(rendered.contains(series), "missing {series}");
    }
}

#[test]
fn the_replication_throttle_counters_are_broker_wide() {
    let metrics = BrokerMetrics::new();

    metrics.record_replication_throttled_out(1_024);
    metrics.record_replication_throttled_in(512);
    metrics.record_replication_throttle_sleep();
    metrics.record_replication_throttle_sleep();

    check!(metrics.replication_throttled_bytes_out_total.get() == 1_024);
    check!(metrics.replication_throttled_bytes_in_total.get() == 512);
    check!(metrics.replication_throttle_sleeps_total.get() == 2);
}
