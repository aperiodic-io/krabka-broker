//! Request builders and the broker harness shared by the `UpdateFeatures`
//! tests.
//!
//! The validation tests and the end-to-end handler tests build the same
//! requests and assert on the same result rows, so the fixtures live in one
//! module rather than being duplicated per test file.

use {
    super::answer,
    crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        features::{ELR_VERSION, METADATA_VERSION},
        handlers::RequestContext,
        test_support::{peer, principal, request_context, start_broker_no_audit_with},
    },
    krabka_protocol::owned::{
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
        update_features_response::UpdateFeaturesResponse,
    },
    krabka_security::Principal,
    std::{net::SocketAddr, sync::Arc},
};

pub(super) const VERSION: i16 = 1;

fn feature_update(name: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    FeatureUpdateKey {
        feature: name.into(),
        max_version_level: level,
        upgrade_type,
        ..Default::default()
    }
}

pub(super) fn named_update(name: &str, level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    feature_update(name, level, upgrade_type)
}

pub(super) fn metadata_update(level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    feature_update(METADATA_VERSION, level, upgrade_type)
}

pub(super) fn elr_update(level: i16, upgrade_type: i8) -> FeatureUpdateKey {
    feature_update(ELR_VERSION, level, upgrade_type)
}

pub(super) fn validate_only(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: updates,
        validate_only: true,
        ..Default::default()
    }
}

pub(super) fn apply_request(updates: Vec<FeatureUpdateKey>) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: updates,
        validate_only: false,
        ..Default::default()
    }
}

pub(super) fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
    request_context(principal, peer, "update-features-client")
}

pub(super) async fn start_broker(
    authorizer: Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await
}

pub(super) async fn call_with(
    authorizer: Arc<dyn Authorizer>,
    req: UpdateFeaturesRequest,
) -> (UpdateFeaturesResponse, BrokerHandle, tempfile::TempDir) {
    let (broker_handle, dir) = start_broker(authorizer).await;
    let resp = call(&broker_handle, req, VERSION).await;
    (resp, broker_handle, dir)
}

pub(super) async fn call(
    broker_handle: &BrokerHandle,
    req: UpdateFeaturesRequest,
    version: i16,
) -> UpdateFeaturesResponse {
    let broker = broker_handle.broker_arc_for_test();
    request_identity!((principal, peer, ctx), principal("admin"), context);
    answer(&broker, req, version, &ctx).await
}

pub(super) async fn wait_for_finalized_feature(broker: &Broker, feature: &str, level: i16) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker.controller.current_image().finalized_feature(feature) == Some(level) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("feature level visible");
}
