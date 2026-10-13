//! Construction of the `UpdateFeatures` response.
//!
//! Kafka answers a request in one of three shapes, which this module builds:
//! a failure the controller raised outside the feature checks (a top-level
//! code and message, no rows), a feature error (`QuorumController`'s prefixed
//! top-level message, no rows), and success (a top-level `NONE`, with one
//! `NONE` row per request row only at versions 0 and 1).

use {
    super::validate::UpdateError,
    crate::codes,
    krabka_protocol::owned::{
        update_features_request::UpdateFeaturesRequest,
        update_features_response::{UpdatableFeatureResult, UpdateFeaturesResponse},
    },
};

/// `Errors.NONE.message()`: `NONE` has no exception, so its message is the
/// enum constant's name.
const NONE_MESSAGE: &str = "NONE";

/// The prefix `QuorumController.updateFeatures` puts ahead of the error of
/// the feature that failed.
const FEATURE_ERROR_PREFIX: &str =
    "The update failed for all features since the following feature had an error: ";

/// A failure raised outside the feature checks, as
/// `UpdateFeaturesResponse.createWithErrors` answers it: no rows.
pub(super) fn top_level_error(code: i16, msg: &str) -> UpdateFeaturesResponse {
    UpdateFeaturesResponse {
        error_code: code,
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

/// A feature that failed validation: its code, the prefixed message, and no
/// rows, at every version.
pub(super) fn feature_error(error: &UpdateError) -> UpdateFeaturesResponse {
    top_level_error(
        error.code,
        &format!("{FEATURE_ERROR_PREFIX}{}", error.message),
    )
}

/// A request that succeeded. Versions 0 and 1 carry one `NONE` row per
/// request row, in request order; version 2 has no `results` field.
pub(super) fn success(request: &UpdateFeaturesRequest, version: i16) -> UpdateFeaturesResponse {
    let results = if version <= 1 {
        request
            .feature_updates
            .iter()
            .map(|update| UpdatableFeatureResult {
                feature: update.feature.clone(),
                error_code: codes::NONE,
                error_message: Some(NONE_MESSAGE.to_string()),
                ..Default::default()
            })
            .collect()
    } else {
        Vec::new()
    };
    UpdateFeaturesResponse {
        error_code: codes::NONE,
        error_message: None,
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::assert,
        krabka_protocol::owned::update_features_request::FeatureUpdateKey,
    };

    fn request(features: &[&str]) -> UpdateFeaturesRequest {
        UpdateFeaturesRequest {
            feature_updates: features
                .iter()
                .map(|&feature| FeatureUpdateKey {
                    feature: feature.into(),
                    max_version_level: 1,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn none_row(feature: &str) -> UpdatableFeatureResult {
        krabka_macros::tagged_wire!(UpdatableFeatureResult {
            feature: feature.into(),
            error_code: codes::NONE,
            error_message: Some("NONE".into()),
        })
    }

    #[test]
    fn success_rows_only_below_version_two() {
        let features = ["share.version", "group.version", "share.version"];
        let cases = [
            (0, features.iter().map(|f| none_row(f)).collect::<Vec<_>>()),
            (1, features.iter().map(|f| none_row(f)).collect()),
            (2, vec![]),
        ];
        for (version, results) in cases {
            let expected = krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
                error_code: codes::NONE,
                error_message: None,
                results,
            });
            assert!(
                success(&request(&features), version) == expected,
                "v{version}"
            );
        }
    }

    #[test]
    fn feature_error_prefixes_the_message_and_drops_rows() {
        let error = UpdateError {
            code: codes::INVALID_UPDATE_VERSION,
            message: "Invalid update version 9 for feature share.version. Local controller 1 \
                      only supports versions 0-1"
                .into(),
        };
        let expected = krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
            error_code: codes::INVALID_UPDATE_VERSION,
            error_message: Some(
                "The update failed for all features since the following feature had an error: \
                 Invalid update version 9 for feature share.version. Local controller 1 only \
                 supports versions 0-1"
                    .into(),
            ),
            results: vec![],
        });
        assert!(feature_error(&error) == expected);
    }

    #[test]
    fn top_level_error_has_no_rows() {
        let expected = krabka_macros::unthrottled_wire!(UpdateFeaturesResponse {
            error_code: codes::NOT_CONTROLLER,
            error_message: Some("not the controller".into()),
            results: vec![],
        });
        assert!(top_level_error(codes::NOT_CONTROLLER, "not the controller") == expected);
    }
}
