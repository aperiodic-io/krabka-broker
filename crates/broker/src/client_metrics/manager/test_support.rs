//! Fixtures shared by the client-metrics manager tests and the
//! `subscription` submodule's tests: a `MetadataImage` carrying one
//! client-metrics subscription, a stock `ClientAttributes` value, and the
//! unwrapper for a successful `SubscriptionDecision`.

use std::collections::BTreeMap;

use krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord};
use uuid::Uuid;

use super::{ClientAttributes, SubscriptionAssignment, SubscriptionDecision};

pub(super) fn img_with(name: &str, kvs: &[(&str, &str)]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    apply_config(&mut img, name, kvs);
    img
}

pub(super) fn apply_config(img: &mut MetadataImage, name: &str, kvs: &[(&str, &str)]) {
    let mut cfgs = BTreeMap::new();
    for (k, v) in kvs {
        cfgs.insert((*k).to_string(), (*v).to_string());
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
