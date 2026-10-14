//! The KIP-584 feature rows of an `ApiVersions` response, built by one set of
//! rules for the controller listener and the broker listener. The two pass it
//! different caps, as Kafka does: the controller listener's
//! `SimpleApiVersionManager` takes `unstable.api.versions.enable`, and the
//! broker's `BrokerFeatures.createDefault` takes
//! `unstable.feature.versions.enable`.
//!
//! The rules are Kafka 4.3.1's:
//!
//! - `BrokerFeatures.defaultSupportedFeatures` advertises each feature from its
//!   minimum production level, which is 0 for every feature except
//!   `metadata.version`, whose minimum is `MetadataVersion.MINIMUM_VERSION`. A
//!   feature whose maximum is 0 is left out.
//! - `ApiVersionsResponse.maybeFilterSupportedFeatureKeys` omits every
//!   supported feature whose minimum is 0 when the request version is below 4
//!   (`alterFeatureLevel0`, KAFKA-17492): older clients fail to deserialize a
//!   zero minimum.
//! - `ApiVersionsResponse.createFinalizedFeatureKeys` omits every finalized
//!   level 0, and `KRaftMetadataCache.features` adds `kraft.version` only when
//!   its level is above 0.
//! - The finalized-features epoch is the offset of the last record the
//!   metadata image contains, `image.highestOffsetAndEpoch().offset()`.

use {
    crate::{UnstableFeatureVersions, supported_feature_range},
    krabka_protocol::owned::api_versions_response::{FinalizedFeatureKey, SupportedFeatureKey},
};

/// First `ApiVersions` version whose JVM client reads a zero feature minimum.
/// Below it Kafka sets `alterFeatureLevel0`.
const ZERO_MINIMUM_API_VERSION: i16 = 4;

/// The supported-feature row for one feature at `api_version`, or `None` when
/// Kafka leaves the feature out of the response.
#[must_use]
pub fn supported_feature_key(
    name: &str,
    min_version: i16,
    max_version: i16,
    api_version: i16,
) -> Option<SupportedFeatureKey> {
    let unregistrable = max_version == 0;
    let alter_level_zero = api_version < ZERO_MINIMUM_API_VERSION && min_version == 0;
    (!unregistrable && !alter_level_zero).then(|| SupportedFeatureKey {
        name: name.to_owned(),
        min_version,
        max_version,
        ..Default::default()
    })
}

/// Every supported-feature row of the `krabka_metadata` feature registry at
/// `api_version`, each feature capped as `unstable` caps it
/// ([`crate::supported_feature_range`]).
#[must_use]
pub fn supported_feature_keys(
    api_version: i16,
    unstable: UnstableFeatureVersions,
) -> Vec<SupportedFeatureKey> {
    krabka_metadata::feature_registry()
        .iter()
        .filter_map(|feature| {
            let (min_version, max_version) = supported_feature_range(*feature, unstable);
            supported_feature_key(feature.name(), min_version, max_version, api_version)
        })
        .collect()
}

/// The finalized-feature rows of `image`, in name order: every finalized
/// level above 0, `metadata.version` at Kafka's minimum before the first
/// feature delta, and `kraft.version` when its level is above 0.
#[must_use]
pub fn finalized_feature_keys(image: &krabka_metadata::MetadataImage) -> Vec<FinalizedFeatureKey> {
    let mut levels = image.finalized_features().clone();
    // Kafka's FeaturesPublisher starts with FinalizedFeatures.fromKRaftVersion
    // (MetadataVersion.MINIMUM_VERSION) and keeps that until a feature delta
    // arrives. An empty Krabka image represents that same startup state.
    levels
        .entry(krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.to_owned())
        .or_insert(krabka_metadata::metadata_version::METADATA_VERSION_MIN);
    levels.insert(
        krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE.to_owned(),
        i16::try_from(image.kraft_version()).unwrap_or(i16::MAX),
    );
    levels
        .into_iter()
        .filter(|(_, level)| *level != 0)
        .map(|(name, level)| FinalizedFeatureKey {
            name,
            // Kafka reports the finalized level as both bounds.
            max_version_level: level,
            min_version_level: level,
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_metadata::metadata_version::{METADATA_VERSION_MAX, METADATA_VERSION_MIN},
    };

    krabka_macros::supported_feature_fixture!(supported);
    krabka_macros::supported_features_fixture!(modern_supported, supported);

    krabka_macros::finalized_feature_fixture!(finalized);

    use crate::test_support::feature_image as image;

    /// #783's table: below v4 only the features with a non-zero minimum stay;
    /// from v4 every feature is advertised from Kafka's minimum production
    /// level. #784: `metadata.version` tops out at 4.3.1's latest production
    /// level, 30, unless `unstable.feature.versions.enable` is set, as
    /// `BrokerFeatures.defaultSupportedFeatures` caps it.
    #[yare::parameterized(
        stable_v0 = {
            0,
            crate::UnstableFeatureVersions::Disabled,
            vec![supported("metadata.version", METADATA_VERSION_MIN, 30)],
        },
        stable_v3 = {
            3,
            crate::UnstableFeatureVersions::Disabled,
            vec![supported("metadata.version", METADATA_VERSION_MIN, 30)],
        },
        stable_v4 = {
            4,
            crate::UnstableFeatureVersions::Disabled,
            modern_supported(supported("metadata.version", METADATA_VERSION_MIN, 30), 1),
        },
        stable_v5 = {
            5,
            crate::UnstableFeatureVersions::Disabled,
            modern_supported(supported("metadata.version", METADATA_VERSION_MIN, 30), 1),
        },
        unstable_v0 = {
            0,
            crate::UnstableFeatureVersions::Enabled,
            vec![supported("metadata.version", METADATA_VERSION_MIN, METADATA_VERSION_MAX)],
        },
        unstable_v3 = {
            3,
            crate::UnstableFeatureVersions::Enabled,
            vec![supported("metadata.version", METADATA_VERSION_MIN, METADATA_VERSION_MAX)],
        },
        unstable_v4 = {
            4,
            crate::UnstableFeatureVersions::Enabled,
            modern_supported(supported("metadata.version", METADATA_VERSION_MIN, METADATA_VERSION_MAX), 2),
        },
        unstable_v5 = {
            5,
            crate::UnstableFeatureVersions::Enabled,
            modern_supported(supported("metadata.version", METADATA_VERSION_MIN, METADATA_VERSION_MAX), 2),
        },
    )]
    fn supported_features_follow_kafkas_minimums_and_alter_level_zero(
        api_version: i16,
        unstable: UnstableFeatureVersions,
        expected: Vec<SupportedFeatureKey>,
    ) {
        // `share.version` 2 (KIP-1191) is trunk's, so only the flag advertises it.
        check!(
            supported_feature_keys(api_version, unstable) == expected,
            "v{api_version} {unstable:?}"
        );
    }

    /// A feature whose maximum is 0 cannot be finalized above 0, and Kafka
    /// does not advertise it at any version.
    #[yare::parameterized(
        v0 = { 0 },
        v3 = { 3 },
        v4 = { 4 },
        v5 = { 5 },
    )]
    fn a_feature_with_maximum_zero_is_never_advertised(api_version: i16) {
        check!(supported_feature_key("x.version", 0, 0, api_version) == None);
        check!(supported_feature_key("x.version", 1, 2, 3) == Some(supported("x.version", 1, 2)));
    }

    /// Level-0 rows are never finalized rows, `kraft.version` included.
    #[yare::parameterized(
        empty_levels = { 0, &[][..], vec![finalized("metadata.version", METADATA_VERSION_MIN)] },
        group_level = {
            0,
            &[("metadata.version", 25), ("group.version", 1)][..],
            vec![finalized("group.version", 1), finalized("metadata.version", 25)],
        },
        kraft_level = {
            1,
            &[("metadata.version", 25)][..],
            vec![finalized("kraft.version", 1), finalized("metadata.version", 25)],
        },
        zero_group_ignored = {
            0,
            &[("metadata.version", 25), ("group.version", 1), ("group.version", 0)][..],
            vec![finalized("metadata.version", 25)],
        },
        krabka_version = {
            1,
            &[("metadata.version", 30), ("krabka.version", 1)][..],
            vec![
                finalized("krabka.version", 1),
                finalized("kraft.version", 1),
                finalized("metadata.version", 30),
            ],
        },
        zero_krabka_ignored = {
            0,
            &[("metadata.version", 30), ("krabka.version", 0)][..],
            vec![finalized("metadata.version", 30)],
        },
    )]
    fn finalized_features_omit_level_zero(
        kraft_version: u16,
        levels: &[(&str, i16)],
        expected: Vec<FinalizedFeatureKey>,
    ) {
        check!(
            finalized_feature_keys(&image(kraft_version, levels)) == expected,
            "kraft.version {kraft_version}, {levels:?}"
        );
    }
}
