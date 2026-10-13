//! The KIP-584 `UpdateFeatures` (`api_key` 57) write path.
//!
//! A finalize of `metadata.version` writes a Raft-persisted `V1FeatureLevel`
//! record, and reports the finalized feature and a real epoch through
//! `ApiVersions`. Validation rejects an unsupported feature and a level
//! outside the range with Kafka's top-level error. `validate_only` runs every
//! check and persists nothing.

use {
    crate::support::{configs::feature_update, discovery::api_versions_request_for},
    assert2::assert,
};
mod support;

use {
    krabka_format::LATEST_PRODUCTION_METADATA_VERSION,
    krabka_metadata::metadata_version::METADATA_VERSION_MAX,
    krabka_protocol::owned::update_features_request::UpdateFeaturesRequest,
};

fn metadata_version_update(level: i16) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: vec![feature_update("metadata.version", level, 1)],
        ..Default::default()
    }
}

/// Starts a broker with Kafka's `unstable.feature.versions.enable` on, the
/// setting a node needs to support trunk's `metadata.version` levels.
async fn start_with_unstable_features() -> support::InProcess {
    support::start_configured(|config| {
        config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    })
    .await
}

/// #784: by default a node supports `metadata.version` only up to 4.3.1's
/// latest production level, so trunk's 4.5-IV0 is refused with the text a
/// Kafka 4.3.1 controller answers (`QuorumFeatures.reasonNotSupported`) and the
/// supported range `ApiVersions` advertises stops at 30.
#[tokio::test]
async fn an_unstable_metadata_version_is_refused_by_default() {
    let p = support::start().await;
    let resp = p
        .client
        .send(metadata_version_update(METADATA_VERSION_MAX))
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_message.as_deref()
            == Some(
                format!(
                    "The update failed for all features since the following feature had an \
                     error: Invalid update version {METADATA_VERSION_MAX} for feature \
                     metadata.version. Local controller 1 only supports versions 7-30"
                )
                .as_str()
            ),
        "{resp:?}"
    );
    assert_feature_error(&resp, "metadata.version");
    let av = p
        .client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    let supported = av
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .map(|f| (f.min_version, f.max_version));
    assert!(
        supported == Some((7, LATEST_PRODUCTION_METADATA_VERSION)),
        "{av:?}"
    );
    p.broker.shutdown().await;
}

/// A cluster bootstraps at 4.3-IV0 and, on a node that supports unstable
/// feature levels, opts into Kafka trunk's 4.4 levels by finalizing them.
#[tokio::test]
async fn finalizes_metadata_version_and_surfaces_in_api_versions() {
    let p = start_with_unstable_features().await;

    let resp = p
        .client
        .send(metadata_version_update(METADATA_VERSION_MAX))
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");
    if let Some(row) = resp
        .results
        .iter()
        .find(|r| r.feature == "metadata.version")
    {
        assert!(row.error_code == 0, "{resp:?}");
    }

    // ApiVersions now surfaces the finalized feature with a real epoch.
    let av = p
        .client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    let fin = av
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized");
    assert!(fin.max_version_level == METADATA_VERSION_MAX, "{av:?}");
    assert!(av.finalized_features_epoch >= 0, "{av:?}");

    p.broker.shutdown().await;
}

fn share_version_update(level: i16) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        feature_updates: vec![feature_update("share.version", level, 1)],
        ..Default::default()
    }
}

/// The `(supported max, finalized level)` of `share.version` in `ApiVersions`.
async fn share_version_in_api_versions(p: &support::InProcess) -> (Option<i16>, Option<i16>) {
    let av = p
        .client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    (
        av.supported_features
            .iter()
            .find(|f| f.name == "share.version")
            .map(|f| f.max_version),
        av.finalized_features
            .iter()
            .find(|f| f.name == "share.version")
            .map(|f| f.max_version_level),
    )
}

/// KIP-1191: `share.version` 2 is trunk's `SV_2`. A default node supports
/// 0-1 as Kafka 4.3.1 does and refuses level 2.
#[tokio::test]
async fn share_version_two_is_refused_by_default() {
    let p = support::start().await;
    assert!(share_version_in_api_versions(&p).await == (Some(1), Some(1)));

    let resp = p
        .client
        .send(share_version_update(2))
        .await
        .expect("UpdateFeatures");
    assert_feature_error(&resp, "share.version");
    assert!(
        resp.error_message
            .as_deref()
            .is_some_and(|message| message.ends_with(
                "Invalid update version 2 for feature share.version. \
                 Local controller 1 only supports versions 0-1"
            )),
        "{resp:?}"
    );
    p.broker.shutdown().await;
}

/// KIP-1191: with `unstable.feature.versions.enable` on, `ApiVersions`
/// advertises `share.version` 0-2, `UpdateFeatures` finalizes level 2, and
/// `validate_only` persists nothing.
#[tokio::test]
async fn share_version_two_is_finalizable_under_unstable_feature_versions() {
    let p = start_with_unstable_features().await;
    assert!(share_version_in_api_versions(&p).await == (Some(2), Some(1)));

    let mut validate_only = share_version_update(2);
    validate_only.validate_only = true;
    let resp = p.client.send(validate_only).await.expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");
    assert!(
        share_version_in_api_versions(&p).await == (Some(2), Some(1)),
        "validate_only must not finalize level 2"
    );

    let resp = p
        .client
        .send(share_version_update(2))
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");
    assert!(share_version_in_api_versions(&p).await == (Some(2), Some(2)));

    let resp = p
        .client
        .send(share_version_update(3))
        .await
        .expect("UpdateFeatures");
    assert_feature_error(&resp, "share.version");
    p.broker.shutdown().await;
}

/// Kafka answers a failed feature with `INVALID_UPDATE_VERSION` (95) at the
/// top level, a message naming the feature, and no rows, at every version.
fn assert_feature_error(
    resp: &krabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
    feature: &str,
) {
    assert!(resp.error_code == 95, "{resp:?}");
    assert!(resp.results.is_empty(), "{resp:?}");
    assert!(
        resp.error_message
            .as_deref()
            .is_some_and(|message| message.starts_with(
                "The update failed for all features since the following feature had an error: "
            ) && message.contains(&format!("for feature {feature}."))),
        "{resp:?}"
    );
}

#[tokio::test]
async fn rejects_unsupported_feature() {
    let p = support::start().await;
    let mut req = metadata_version_update(1);
    req.feature_updates[0].feature = "not.a.feature".into();
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    assert_feature_error(&resp, "not.a.feature");
    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_level_above_supported_max() {
    let p = support::start().await;
    let resp = p
        .client
        .send(metadata_version_update(99))
        .await
        .expect("UpdateFeatures");
    assert_feature_error(&resp, "metadata.version");
    p.broker.shutdown().await;
}

fn metadata_version(
    api_versions: &krabka_protocol::owned::api_versions_response::ApiVersionsResponse,
) -> i16 {
    api_versions
        .finalized_features
        .iter()
        .find(|feature| feature.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap")
        .max_version_level
}

#[tokio::test]
async fn validate_only_does_not_persist() {
    let p = start_with_unstable_features().await;

    // A self-bootstrapped broker already finalizes metadata.version=4.3-IV0, so
    // emptiness no longer signals "nothing persisted". Capture the level + epoch
    // BEFORE, send a validate_only request that WOULD change metadata.version,
    // then assert neither moved — validate_only must run the checks without
    // persisting (no epoch bump, no level change).
    let api_versions = || {
        p.client
            .send(api_versions_request_for("krabka-test", "0.0.0"))
    };

    let before = api_versions().await.expect("ApiVersions");
    assert!(
        metadata_version(&before) == LATEST_PRODUCTION_METADATA_VERSION,
        "{before:?}"
    );
    let epoch_before = before.finalized_features_epoch;
    assert!(epoch_before >= 0, "{before:?}");

    // Request an upgrade with validate_only; it would change
    // metadata.version if persisted.
    let mut req = metadata_version_update(METADATA_VERSION_MAX);
    req.validate_only = true;
    let resp = p.client.send(req).await.expect("UpdateFeatures");
    assert!(resp.error_code == 0, "{resp:?}");

    // Nothing changed: same level, same epoch.
    let after = api_versions().await.expect("ApiVersions");
    assert!(
        metadata_version(&after) == LATEST_PRODUCTION_METADATA_VERSION,
        "validate_only must not change the level: {after:?}",
    );
    assert!(
        after.finalized_features_epoch == epoch_before,
        "validate_only must not bump the epoch: {after:?}",
    );
    p.broker.shutdown().await;
}

#[tokio::test]
async fn rejects_level_below_min_floor() {
    let p = support::start().await;
    // Level 6 is below the supported minimum (METADATA_VERSION_MIN = 7).
    let resp = p
        .client
        .send(metadata_version_update(6))
        .await
        .expect("UpdateFeatures");
    assert_feature_error(&resp, "metadata.version");
    p.broker.shutdown().await;
}
