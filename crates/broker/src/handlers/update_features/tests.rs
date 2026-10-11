//! End-to-end tests for the `UpdateFeatures` handler, which drive a live
//! broker and so are kept out of the module root.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::owned::update_features_response::UpdatableFeatureResult;

use super::*;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::Broker,
    features::{LATEST_PRODUCTION_METADATA_VERSION, METADATA_VERSION},
    handlers::update_features::test_support::{
        apply_request, call, call_with, metadata_update, named_update, start_broker, validate_only,
        wait_for_finalized_feature,
    },
    test_support::{DenyAll, start_broker_no_audit_with},
};

const PREFIX: &str =
    "The update failed for all features since the following feature had an error: ";

fn refused(message: &str) -> UpdateFeaturesResponse {
    unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::INVALID_UPDATE_VERSION,
        error_message: Some(format!("{PREFIX}{message}")),
        results: vec![],
    })
}

fn accepted(features: &[&str]) -> UpdateFeaturesResponse {
    unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::NONE,
        error_message: None,
        results: features
            .iter()
            .map(|&feature| tagged_wire!(UpdatableFeatureResult {
                feature: feature.into(),
                error_code: codes::NONE,
                error_message: Some("NONE".into()),
            }))
            .collect(),
    })
}

#[tokio::test]
async fn handle_denies_cluster_alter_with_top_level_error() {
    let req = validate_only(vec![metadata_update(
        crate::features::METADATA_VERSION_MAX,
        1,
    )]);

    let (resp, broker_handle, _dir) = Box::pin(call_with(Arc::new(DenyAll), req)).await;

    let expected = unthrottled_wire!(UpdateFeaturesResponse {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Cluster authorization failed.".to_string()),
        results: vec![],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// A self-bootstrapped broker finalizes `metadata.version` 30 (4.3-IV0),
/// `group.version` 1 and `transaction.version` 2, and leaves
/// `share.version`, `streams.version` and ELR off.
#[tokio::test]
async fn requests_apply_atomically_with_kafkas_response_shape() {
    let group = "group.version";
    let share = "share.version";
    let cases = [
        (
            "a failing row fails the request, v1",
            1,
            vec![named_update(group, 0, 2), named_update(share, 9, 1)],
            false,
            refused(
                "Invalid update version 9 for feature share.version. Local controller 1 only \
                 supports versions 0-1",
            ),
            Some(1),
        ),
        (
            "a failing row fails the request, v2",
            2,
            vec![named_update(group, 0, 2), named_update(share, 9, 1)],
            false,
            refused(
                "Invalid update version 9 for feature share.version. Local controller 1 only \
                 supports versions 0-1",
            ),
            Some(1),
        ),
        (
            "success, v0",
            0,
            vec![named_update(group, 1, 1)],
            false,
            accepted(&[group]),
            Some(1),
        ),
        (
            "success, v1",
            1,
            vec![named_update(group, 0, 2)],
            false,
            accepted(&[group]),
            None,
        ),
        (
            "success, v2",
            2,
            vec![named_update(group, 0, 2)],
            false,
            accepted(&[]),
            None,
        ),
        (
            "validate_only, v2",
            2,
            vec![named_update(group, 0, 2)],
            true,
            accepted(&[]),
            Some(1),
        ),
        (
            "an empty request is a no-op",
            1,
            vec![],
            false,
            accepted(&[]),
            Some(1),
        ),
        (
            "a repeated name keeps its last row",
            1,
            vec![named_update(group, 9, 1), named_update(group, 0, 2)],
            false,
            accepted(&[group, group]),
            None,
        ),
    ];
    for (case, version, updates, validate, expected, group_after) in cases {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        let req = UpdateFeaturesRequest {
            feature_updates: updates,
            validate_only: validate,
            ..Default::default()
        };
        let resp = Box::pin(call(&broker_handle, req, version)).await;
        assert!(resp == expected, "{case}");
        // A write that lands turns group.version off, so wait for it before
        // reading the level back; a refusal or a no-op has nothing to wait on.
        let broker = broker_handle.broker_arc_for_test();
        if group_after.is_none() {
            wait_for_group_version_off(&broker).await;
        }
        assert!(
            broker.controller.current_image().finalized_feature(group) == group_after,
            "{case}"
        );
        broker_handle.shutdown().await;
    }
}

async fn wait_for_group_version_off(broker: &Broker) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while broker
            .controller
            .current_image()
            .finalized_feature("group.version")
            .is_some()
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("group.version turned off");
}

#[tokio::test]
async fn handle_persists_a_lossless_metadata_downgrade() {
    // 4.4-IV0 is a Kafka trunk level: the controller supports it only under
    // `unstable.feature.versions.enable`.
    let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    })
    .await;
    let target = LATEST_PRODUCTION_METADATA_VERSION - 1;
    // 4.3-IV0 changed metadata, so step up to 4.4-IV0 first and back down
    // to 4.3-IV0, a downgrade across 4.4-IV0 alone.
    let up = Box::pin(call(
        &broker_handle,
        apply_request(vec![metadata_update(target + 2, 1)]),
        1,
    ))
    .await;
    assert!(up == accepted(&[crate::features::METADATA_VERSION]));
    let broker = broker_handle.broker_arc_for_test();
    wait_for_finalized_feature(&broker, METADATA_VERSION, target + 2).await;

    let down = Box::pin(call(
        &broker_handle,
        apply_request(vec![metadata_update(target + 1, 2)]),
        1,
    ))
    .await;

    assert!(down == accepted(&[crate::features::METADATA_VERSION]));
    wait_for_finalized_feature(&broker, METADATA_VERSION, target + 1).await;
    broker_handle.shutdown().await;
}
