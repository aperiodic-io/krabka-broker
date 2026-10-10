//! A bucket that nothing charged for longer than the expiration is dropped
//! along with the metric series it published, and an active one is left alone.

use assert2::check;
use krabka_metadata::EntityKey;
use krabka_units::{millis, secs};

use super::*;
use crate::metrics::QuotaEntityLabel;

fn key(user: &str, client_id: &str) -> EntityKey {
    vec![
        ("user".into(), Some(user.into())),
        ("client-id".into(), Some(client_id.into())),
    ]
}

fn alice_fixture() -> (QuotaBuckets, BrokerMetrics) {
    let buckets = QuotaBuckets::new();
    let metrics = BrokerMetrics::new();
    drop(buckets.get_or_create("producer_byte_rate", &key("alice", "app"), 1024.0));
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: Some("app".into()),
            }),
    );
    (buckets, metrics)
}

/// The series names a `/metrics` body carries for the per-entity throttle.
fn throttle_series(metrics: &BrokerMetrics) -> Vec<String> {
    let mut body = String::new();
    prometheus_client::encoding::text::encode(
        &mut body,
        &metrics.registry.try_lock().expect("registry"),
    )
    .expect("encode");
    body.lines()
        .filter(|line| line.starts_with("krabka_broker_quota_entity_throttle_seconds_total{"))
        .map(ToString::to_string)
        .collect()
}

fn check_expired(buckets: &QuotaBuckets, metrics: &BrokerMetrics) {
    check!(buckets.len() == 0);
    check!(throttle_series(metrics).is_empty());
}

#[test]
fn an_inactive_bucket_and_its_metric_series_are_both_dropped() {
    let (buckets, metrics) = alice_fixture();
    check!(throttle_series(&metrics).len() == 1);

    // Nothing has touched the bucket since it was made, so any positive age
    // below "just now" expires it.
    sweep(&buckets, &metrics, millis(0));

    check_expired(&buckets, &metrics);
}

#[test]
fn a_bucket_inside_the_window_keeps_its_series() {
    let (buckets, metrics) = alice_fixture();

    sweep(&buckets, &metrics, secs(3600));

    check!(buckets.len() == 1);
    check!(throttle_series(&metrics).len() == 1);
}

/// A bucket keyed by a user alone published its throttle under that user with
/// no client id, whichever client created it, so that is the series its expiry
/// releases (#1213). Keying the release on the creating client would have
/// named `client_id="app"` and left this series behind.
#[test]
fn a_user_bucket_releases_the_series_of_its_own_key() {
    let buckets = QuotaBuckets::new();
    let metrics = BrokerMetrics::new();
    let user: EntityKey = vec![("user".into(), Some("alice".into()))];
    drop(buckets.get_or_create("producer_byte_rate", &user, 1024.0));
    drop(
        metrics
            .quota_entity_throttle_seconds_total
            .get_or_create(&QuotaEntityLabel {
                quota_type: QuotaType::Produce,
                user: Some("alice".into()),
                client_id: None,
            }),
    );
    check!(throttle_series(&metrics).len() == 1);

    sweep(&buckets, &metrics, millis(0));

    check_expired(&buckets, &metrics);
}

/// Kafka charges every quota under its own config key, and the series is
/// labelled by the `QuotaType` that key names; a sweep that could not make
/// that trip would leave the label set behind.
#[test]
fn every_quota_key_a_bucket_is_created_under_names_a_quota_type() {
    for (config_key, want) in [
        ("producer_byte_rate", QuotaType::Produce),
        ("consumer_byte_rate", QuotaType::Fetch),
        ("request_percentage", QuotaType::Request),
        ("controller_mutation_rate", QuotaType::ControllerMutation),
        ("connection_creation_rate", QuotaType::ConnectionCreation),
    ] {
        check!(QuotaType::from_config_key(config_key) == Some(want));
    }
    check!(QuotaType::from_config_key("not_a_quota") == None);
}
