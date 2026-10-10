//! The KIP-584 feature rows that an `ApiVersions` response carries: the
//! `supported_features` range this broker compiles in, and the
//! `finalized_features` levels it reads out of the live metadata image, with
//! Kafka's minimum `metadata.version` before the first feature delta.
//!
//! The rules are Kafka 4.3.1's, and the controller listener answers with the
//! same ones: both call `krabka_raft`'s shared helpers, which hold the Kafka
//! references. In short, every feature is advertised from Kafka's minimum
//! production level -- 0 for all but `metadata.version` -- a request below v4
//! does not see a zero minimum (`alterFeatureLevel0`), and no finalized row
//! is at level 0, `kraft.version` included.

use krabka_protocol::owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey};

/// The supported-feature rows of this broker's feature table at `api_version`,
/// under `unstable`.
pub(super) fn supported_feature_keys(
    api_version: i16,
    unstable: krabka_raft::UnstableFeatureVersions,
) -> Vec<SupportedFeatureKey> {
    crate::features::supported_features(unstable)
        .iter()
        .filter_map(|feature| {
            krabka_raft::supported_feature_key(
                feature.name,
                feature.min_version,
                feature.max_version,
                api_version,
            )
        })
        .collect()
}

/// The finalized-feature rows of `image`: every level above 0.
pub(super) fn finalized_feature_keys(
    image: &krabka_metadata::MetadataImage,
) -> Vec<FinalizedFeatureKey> {
    krabka_raft::finalized_feature_keys(image)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{FeatureLevelRecord, KRaftVersionRecord, MetadataRecord};

    use super::*;

    krabka_macros::supported_feature_fixture!(supported);
    krabka_macros::supported_features_fixture!(modern_supported, supported);

    krabka_macros::finalized_feature_fixture!(finalized);

    /// #783: below v4 only `metadata.version` stays, because every other
    /// feature's minimum is 0; from v4 each feature carries Kafka's
    /// `minimumProduction`.
    #[test]
    fn supported_features_follow_kafkas_minimums() {
        let metadata_version = supported(
            "metadata.version",
            crate::features::METADATA_VERSION_MIN,
            crate::features::LATEST_PRODUCTION_METADATA_VERSION,
        );
        let modern = modern_supported(metadata_version.clone(), 1);
        let legacy = vec![metadata_version];
        for (api_version, expected) in [(0, &legacy), (3, &legacy), (4, &modern), (5, &modern)] {
            check!(
                supported_feature_keys(api_version, krabka_raft::UnstableFeatureVersions::Disabled)
                    == *expected,
                "v{api_version}"
            );
        }
    }

    /// A fresh image follows Kafka's initial `FeaturesPublisher` state: the
    /// minimum metadata version is finalized, while `kraft.version` 0 is no row.
    #[test]
    fn a_fresh_image_advertises_kafkas_minimum_metadata_version() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        check!(
            finalized_feature_keys(&image)
                == vec![finalized(
                    "metadata.version",
                    crate::features::METADATA_VERSION_MIN
                )]
        );
    }

    #[test]
    fn finalized_feature_keys_keep_levels_above_zero() {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (name, level) in [
            ("metadata.version", 24),
            ("group.version", 1),
            ("share.version", 0),
        ] {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level,
            }));
        }
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
        image.apply(&MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
        // The krabka-owned `krabka.version` is a finalized row like any other.
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "krabka.version".into(),
            level: 1,
        }));
        check!(
            finalized_feature_keys(&image)
                == vec![
                    finalized("group.version", 1),
                    finalized("krabka.version", 1),
                    finalized("kraft.version", 1),
                    finalized("metadata.version", 24)
                ]
        );
    }
}
