//! KIP-584 supported-feature surface for the broker. This module re-exports
//! the `krabka_metadata` feature registry and derives the `ApiVersions`
//! advertisement rows from it, so the advertised and the validated feature
//! sets can never disagree. The behavioral gating helper `require_feature`
//! lives here because it returns broker error codes.

use krabka_metadata::MetadataImage;
/// The `eligible.leader.replicas.version` feature name (KIP-966). It gates the
/// controller's ELR bookkeeping, which `ElrPublisher::extend` reads with
/// `feature_enabled`, and `UpdateFeatures` clears the published state when it
/// is finalized back to 0.
pub(crate) use krabka_metadata::metadata_version::ELR_VERSION_FEATURE as ELR_VERSION;
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
// Re-exported for `ApiVersions` tests / range-bound assertions; consumed only
// from `#[cfg(test)]` modules, so the non-test lib target sees them as unused.
#[cfg(test)]
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_MAX;
#[cfg(test)]
pub(crate) use krabka_metadata::metadata_version::METADATA_VERSION_MIN;
/// The `share.version` feature name (KIP-932). The share-group APIs are on
/// only while it is finalized at 1 or more, see [`share_groups_enabled`].
pub(crate) use krabka_metadata::metadata_version::SHARE_VERSION_FEATURE as SHARE_VERSION;
/// The `streams.version` feature name (KIP-1071). It gates
/// `StreamsGroupHeartbeat` and `StreamsGroupDescribe`. Those handlers read it
/// with `feature_enabled`.
pub(crate) use krabka_metadata::metadata_version::STREAMS_VERSION_FEATURE as STREAMS_VERSION;

/// The `metadata.version` a self-bootstrapped cluster finalizes: Kafka 4.3's
/// `MetadataVersion.LATEST_PRODUCTION`, `4.3-IV0`. It is the level
/// `krabka format` picks when no release is named, and the two must agree.
///
/// The feature table also carries Kafka trunk's unstable `4.4-IV0` to
/// `4.5-IV0`. A stock 4.3 node or tool does not know them, so a node supports
/// them only under `unstable.feature.versions.enable`, and a cluster reaches
/// them only through `UpdateFeatures` or `krabka format` with that set.
pub(crate) const LATEST_PRODUCTION_METADATA_VERSION: i16 =
    krabka_raft::LATEST_PRODUCTION_METADATA_VERSION;

/// The `metadata.version` level at which CIDR-based ACL host patterns
/// (KIP-1276) are accepted: upstream Kafka's `4.4-IV1`,
/// `MetadataVersion.isCidrAclSupported`.
pub(crate) const CIDR_ACL_HOST_MIN_LEVEL: i16 =
    krabka_metadata::metadata_version::CIDR_ACL_MIN_LEVEL;

/// The `metadata.version` level from which the controller serves
/// `UnregisterController` (KIP-1312): Kafka trunk's `4.4-IV2`,
/// `MetadataVersion.isControllerUnregistrationSupported`.
pub(crate) const CONTROLLER_UNREGISTRATION_MIN_LEVEL: i16 =
    krabka_metadata::metadata_version::CONTROLLER_UNREGISTRATION_MIN_LEVEL;

/// One row of the `ApiVersions.supported_features` advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing under `unstable`. They come
/// from the `krabka_metadata` registry, the single source of truth, capped as
/// Kafka's `BrokerFeatures.defaultSupportedFeatures` caps them.
pub(crate) fn supported_features(
    unstable: krabka_raft::UnstableFeatureVersions,
) -> Vec<SupportedFeature> {
    krabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let (min_version, max_version) = krabka_raft::supported_feature_range(*f, unstable);
            SupportedFeature {
                name: f.name(),
                min_version,
                max_version,
            }
        })
        .collect()
}

/// Look up a supported feature by name. It pairs with `supported_features` as
/// the module's feature-surface API. The `UpdateFeatures` handler resolves the
/// registry feature directly, so the non-test lib target sees this as unused.
#[cfg(test)]
pub(crate) fn lookup(name: &str) -> Option<SupportedFeature> {
    krabka_metadata::feature(name).map(|f| {
        let (min_version, max_version) = f.supported_range();
        SupportedFeature {
            name: f.name(),
            min_version,
            max_version,
        }
    })
}

/// KIP-584 admission gate. Returns `Err(UNSUPPORTED_VERSION)` when `name` is
/// finalized below `required_level`. It is permissive when the feature is
/// unfinalized, because there is no level to gate against. This matches how
/// the range guard treats a missing level.
///
/// This permissiveness is correct only for a feature whose own presence in
/// the registry is what changed -- a legacy image predates the feature
/// entirely, so there is nothing to compare against and the RPC must still
/// work. It is the wrong helper for [`CIDR_ACL_HOST_MIN_LEVEL`]: an
/// unfinalized `metadata.version` there is a real (if legacy or
/// pre-bootstrap) image, never a blank check. See [`cidr_hosts_supported`].
pub(crate) fn require_feature(
    image: &MetadataImage,
    name: &str,
    required_level: i16,
) -> Result<(), i16> {
    let finalized = image.finalized_features().get(name).copied();
    if finalized.is_some_and(|level| level < required_level) {
        Err(crate::codes::UNSUPPORTED_VERSION)
    } else {
        Ok(())
    }
}

/// KIP-1276 admission gate: whether `image` supports CIDR-range ACL hosts.
///
/// Unlike [`require_feature`], an unfinalized `metadata.version` is treated
/// as [`LATEST_PRODUCTION_METADATA_VERSION`], the level a cluster bootstraps
/// at, not as an unconditional pass: CIDR hosts are Kafka trunk's, and a
/// cluster reaches them only by finalizing 4.4-IV1 (#784).
pub(crate) fn cidr_hosts_supported(image: &MetadataImage) -> bool {
    image
        .finalized_features()
        .get(METADATA_VERSION)
        .copied()
        .unwrap_or(LATEST_PRODUCTION_METADATA_VERSION)
        >= CIDR_ACL_HOST_MIN_LEVEL
}

/// True when `name` is finalized at >= `level`. It treats an UNFINALIZED
/// feature as level 0, which is disabled. Use it for features where absence
/// means "off", for example `group.version` → next-gen disabled. This differs
/// from `require_feature`, which is permissive on absence and serves
/// metadata.version-gated RPCs on legacy images.
pub(crate) fn feature_enabled(
    image: &krabka_metadata::MetadataImage,
    name: &str,
    level: i16,
) -> bool {
    image.finalized_features().get(name).copied().unwrap_or(0) >= level
}

/// KIP-932: whether `image` finalizes `share.version` at 1 or more, the level
/// that turns on share groups. It is Kafka's
/// `KafkaApis.isShareGroupProtocolEnabled` (`ShareVersion.supportsShareGroups`),
/// the gate of `ShareGroupHeartbeat`, `ShareGroupDescribe`, `ShareFetch`,
/// `ShareAcknowledge` and the three `*ShareGroupOffsets` APIs. Kafka 4.3.1 has
/// no broker config for it.
pub(crate) fn share_groups_enabled(image: &krabka_metadata::MetadataImage) -> bool {
    feature_enabled(image, SHARE_VERSION, 1)
}

/// KIP-1191: whether `image` finalizes `share.version` at 2 or more, the
/// level that turns on the share-group dead-letter queue. It is Kafka's
/// `ShareVersion.supportsShareGroupDLQ`, which `BrokerServer` hands to every
/// `SharePartition`. A node reaches level 2 only under
/// `unstable.feature.versions.enable`, so this is false for a default node.
pub(crate) fn share_dlq_supported(image: &krabka_metadata::MetadataImage) -> bool {
    feature_enabled(image, SHARE_VERSION, 2)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn feature_enabled_treats_absence_as_disabled() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        assert!(!feature_enabled(&image, "group.version", 1)); // absent → disabled
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));
        assert!(feature_enabled(&image, "group.version", 1)); // present at 1 → enabled
    }

    /// Kafka enables share groups at share.version 1 and the DLQ at 2.
    #[test]
    fn share_group_and_dlq_gates_follow_the_finalized_share_version() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        for (finalized, groups, dlq) in [
            (None, false, false),
            (Some(0), false, false),
            (Some(1), true, false),
            (Some(2), true, true),
        ] {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = finalized {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: SHARE_VERSION.into(),
                    level,
                }));
            }
            assert!(share_groups_enabled(&image) == groups, "{finalized:?}");
            assert!(share_dlq_supported(&image) == dlq, "{finalized:?}");
        }
    }

    /// KIP-1276's gate: an image with no `metadata.version` is judged against
    /// the bootstrap level, 4.3-IV0, which refuses CIDR hosts; a finalized
    /// level admits them only from 4.4-IV1.
    #[test]
    fn cidr_hosts_supported_follows_the_finalized_metadata_version() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        for (finalized, want) in [
            (None, false),
            (Some(LATEST_PRODUCTION_METADATA_VERSION), false),
            (Some(CIDR_ACL_HOST_MIN_LEVEL - 1), false),
            (Some(CIDR_ACL_HOST_MIN_LEVEL), true),
        ] {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = finalized {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: METADATA_VERSION.into(),
                    level,
                }));
            }
            assert!(cidr_hosts_supported(&image) == want, "{finalized:?}");
        }
    }

    /// Kafka's `checkIfMetadataChanged` walks from the higher level down
    /// and stops at the first level whose `didMetadataChange` is set, so the
    /// higher level's own flag counts and the lower level's does not. The
    /// broker's `UpdateFeatures` path reads it from `krabka_metadata`, and
    /// these are the cases the broker-local table it replaced was held to.
    #[test]
    fn metadata_changed_between_walks_like_kafka() {
        use krabka_metadata::metadata_version::metadata_changed_between;

        for (source, target, want) in [
            (25, 25, false),
            (25, 24, false),
            (24, 25, false),
            (29, 24, false),
            (25, 22, true),
            (25, 23, false),
            (23, 22, true),
            (17, 15, true),
            (16, 15, false),
            (13, 12, true),
            (12, 11, false),
            (30, 29, true),
            (32, 31, true),
            (31, 30, false),
            (8, 7, true),
            (7, 6, true),
            (33, 32, true),
        ] {
            assert!(
                metadata_changed_between(source, target) == want,
                "{source} -> {target}"
            );
        }
    }

    #[test]
    fn self_bootstrap_and_format_default_to_the_same_metadata_version() {
        assert!(
            LATEST_PRODUCTION_METADATA_VERSION == krabka_format::LATEST_PRODUCTION_METADATA_VERSION
        );
    }

    #[test]
    fn supported_features_include_metadata_version() {
        let expected = SupportedFeature {
            name: METADATA_VERSION,
            min_version: METADATA_VERSION_MIN,
            max_version: METADATA_VERSION_MAX,
        };
        assert!(lookup(METADATA_VERSION) == Some(expected));
        assert!(lookup("not.a.feature").is_none());
    }

    #[test]
    fn share_version_is_supported() {
        // The registry lists trunk's `SV_2`; a default node advertises 4.3.1's
        // range, 0-1, below.
        let expected = SupportedFeature {
            name: SHARE_VERSION,
            min_version: 0,
            max_version: 2,
        };
        assert!(lookup(SHARE_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features(krabka_raft::UnstableFeatureVersions::Disabled)
                .iter()
                .any(|f| f.name == SHARE_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    /// KIP-1191: level 2 is trunk's `SV_2`, so `ApiVersions` advertises it
    /// only under `unstable.feature.versions.enable`.
    #[test]
    fn share_version_two_is_advertised_only_under_unstable_feature_versions() {
        let advertised = |unstable| {
            supported_features(unstable)
                .into_iter()
                .find(|f| f.name == SHARE_VERSION)
                .map(|f| (f.min_version, f.max_version))
        };
        assert!(
            (
                advertised(krabka_raft::UnstableFeatureVersions::Disabled),
                advertised(krabka_raft::UnstableFeatureVersions::Enabled),
            ) == (Some((0, 1)), Some((0, 2)))
        );
    }

    /// The whole supported-feature table a default node advertises: the
    /// registry, with the krabka-owned `krabka.version` at [0, 1] after Kafka's
    /// own features, which keep their ranges.
    #[test]
    fn supported_features_include_krabka_version() {
        let feature = |name, min_version, max_version| SupportedFeature {
            name,
            min_version,
            max_version,
        };
        assert!(
            supported_features(krabka_raft::UnstableFeatureVersions::Disabled)
                == vec![
                    feature(
                        METADATA_VERSION,
                        METADATA_VERSION_MIN,
                        LATEST_PRODUCTION_METADATA_VERSION
                    ),
                    feature("group.version", 0, 1),
                    feature("transaction.version", 0, 2),
                    feature(SHARE_VERSION, 0, 1),
                    feature(STREAMS_VERSION, 0, 1),
                    feature(ELR_VERSION, 0, 1),
                    feature("kraft.version", 0, 1),
                    feature("krabka.version", 0, 1),
                ]
        );
    }

    #[test]
    fn streams_version_is_supported() {
        let expected = SupportedFeature {
            name: STREAMS_VERSION,
            min_version: 0,
            max_version: 1,
        };
        assert!(lookup(STREAMS_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features(krabka_raft::UnstableFeatureVersions::Disabled)
                .iter()
                .any(|f| f.name == STREAMS_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    #[test]
    fn require_feature_is_permissive_on_unfinalized() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        assert!(require_feature(&image, METADATA_VERSION, 11).is_ok());
    }

    #[test]
    fn require_feature_gates_below_level() {
        use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: METADATA_VERSION.to_string(),
            level: 10,
        }));
        for (required_level, want) in [
            (11, Err(crate::codes::UNSUPPORTED_VERSION)),
            (10, Ok(())),
            (7, Ok(())),
        ] {
            assert!(
                require_feature(&image, METADATA_VERSION, required_level) == want,
                "level {required_level}"
            );
        }
    }
}
