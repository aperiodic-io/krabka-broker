//! Fixtures shared by the client-metrics manager tests and the
//! `subscription` submodule's tests: a `MetadataImage` carrying one
//! client-metrics subscription, a stock `ClientAttributes` value, and the
//! unwrapper for a successful `SubscriptionDecision`.

use {
    super::{ClientAttributes, SubscriptionAssignment, SubscriptionDecision},
    krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord},
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// One key and value of a client metrics config.
#[derive(Clone, Copy)]
pub(super) struct ConfigEntry<'a> {
    pub(super) key: &'a str,
    pub(super) value: &'a str,
}

pub(super) fn img_with(name: &str, kvs: &[ConfigEntry<'_>]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    apply_config(&mut img, name, kvs);
    img
}

/// An image whose `all` subscription matches every metric at `interval_ms`.
pub(super) fn all_metrics_img(interval_ms: &str) -> MetadataImage {
    img_with("all", &[
        ConfigEntry {
            key: "metrics",
            value: "*",
        },
        ConfigEntry {
            key: "interval.ms",
            value: interval_ms,
        },
    ])
}

pub(super) fn apply_config(img: &mut MetadataImage, name: &str, kvs: &[ConfigEntry<'_>]) {
    let mut cfgs = BTreeMap::new();
    for ConfigEntry { key, value } in kvs {
        cfgs.insert((*key).to_string(), (*value).to_string());
    }
    img.apply(&MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: name.into(),
            configs: cfgs,
        },
    ));
}

pub(super) fn attrs() -> ClientAttributes {
    ClientAttributes {
        connection_id: "connection-a".into(),
        client_instance_id: Uuid::from_u128(7),
        client_id: "svc-1".into(),
        software_name: "apache-kafka-java".into(),
        software_version: "3.9.0".into(),
        source_address: "10.0.0.5".into(),
        source_port: 5556,
    }
}

pub(super) fn expect_assignment(decision: SubscriptionDecision) -> SubscriptionAssignment {
    let SubscriptionDecision::Assign(assignment) = decision else {
        panic!("expected subscription assignment");
    };
    assignment
}
