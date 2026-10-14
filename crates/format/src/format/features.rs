//! Resolution of the bootstrap `metadata.version` and of the KIP-1022 feature
//! levels a format finalizes.
//!
//! `--release-version` and `--feature` both decide what the seed
//! `FeatureLevelRecord`s say, and the two interact: one sets the base release
//! and the other overrides individual features, except for `metadata.version`
//! where they conflict. The rules and the validation `kafka-storage format`
//! performs live here, apart from the flag definitions they read.

use {
    krabka_metadata::{FeatureLevelRecord, metadata_version::KRAFT_VERSION_FEATURE},
    krabka_raft::UnstableFeatureVersions,
    std::collections::BTreeMap,
};

/// What `--release-version` and `--feature` resolve to.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ResolvedFeatures {
    /// The `metadata.version` level the cluster bootstraps at.
    pub(super) metadata_version: i16,
    /// The levels `--feature` set, by feature name.
    pub(super) overrides: BTreeMap<String, i16>,
}

/// The highest `metadata.version` this format accepts under `unstable`:
/// Kafka 4.3's latest production level, or with unstable feature versions the
/// highest level krabka knows (Kafka's `MetadataVersion.latestTesting()`).
fn metadata_version_ceiling(unstable: UnstableFeatureVersions) -> i16 {
    krabka_metadata::feature(krabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
        .map_or(LATEST_PRODUCTION_METADATA_VERSION, |feature| {
            krabka_raft::supported_feature_range(feature, unstable).1
        })
}

/// The highest `metadata.version` level the table of the emulated Kafka knows:
/// Kafka's `MetadataVersion.latestTesting()`. It is one past the latest
/// production level in 4.3.1's table (`4.4-IV0`, 31), and the highest level
/// krabka knows under `unstable`. A level above it does not exist. A level
/// between [`metadata_version_ceiling`] and it exists but is not yet stable.
fn metadata_version_latest_testing(unstable: UnstableFeatureVersions) -> i16 {
    match unstable {
        UnstableFeatureVersions::Disabled => LATEST_PRODUCTION_METADATA_VERSION + 1,
        UnstableFeatureVersions::Enabled => metadata_version_ceiling(unstable),
    }
}

/// The lookup key Kafka's `MetadataVersion.fromVersionString` derives from a
/// release string: the first two dot-separated segments when there are more
/// than two, so `4.3.1` and `4.3.x` both look up `4.3`, and the whole string
/// otherwise. Java's `String.split` drops trailing empty segments, so `4.3.1.`
/// looks up `4.3` but `4.3.` is looked up as it stands.
fn kafka_version_key(s: &str) -> &str {
    let significant = s.trim_end_matches('.');
    significant
        .match_indices('.')
        .nth(1)
        .map_or(s, |(end, _)| &significant[..end])
}

/// Map a release string to a supported `metadata.version` feature level, as
/// Kafka's `MetadataVersion.fromVersionString(versionString,
/// unstableFeatureVersionsEnabled)` does: an unknown release, or one past the
/// latest production level while unstable feature versions are off, is
/// refused with Kafka's message, which lists the releases that are accepted.
///
/// Kafka looks the string up by its first two segments and does not trim it.
/// The `krabka_metadata` lookup does trim, so a key with surrounding
/// whitespace is refused here, as an unknown release, before that lookup runs.
fn resolve_release_level(s: &str, unstable: UnstableFeatureVersions) -> Result<i16, String> {
    let ceiling = metadata_version_ceiling(unstable);
    Some(kafka_version_key(s))
        .filter(|key| key.trim() == *key)
        .and_then(krabka_metadata::metadata_version::from_version_string)
        .map(krabka_metadata::metadata_version::MetadataVersion::feature_level)
        .filter(|level| {
            krabka_metadata::metadata_version::is_supported_level(*level) && *level <= ceiling
        })
        .ok_or_else(|| {
            let supported: Vec<&str> = (krabka_metadata::metadata_version::METADATA_VERSION_MIN
                ..=ceiling)
                .filter_map(krabka_metadata::metadata_version::from_feature_level)
                .map(krabka_metadata::metadata_version::MetadataVersion::ivn)
                .collect();
            format!(
                "Unknown metadata.version '{s}'. Supported metadata.version are: {}",
                supported.join(", ")
            )
        })
}

/// Parse one `--feature NAME=LEVEL` spec into the feature and its level.
pub(super) fn parse_feature_spec(s: &str) -> Result<FeatureLevelRecord, String> {
    let (name, level) = s
        .split_once('=')
        .ok_or("--feature must be NAME=LEVEL, e.g. transaction.version=2")?;
    let name = name.trim();
    if name.is_empty() {
        return Err("feature name must not be empty".into());
    }
    let level: i16 = level
        .trim()
        .parse()
        .map_err(|e| format!("feature level: {e}"))?;
    Ok(FeatureLevelRecord {
        name: name.to_string(),
        level,
    })
}

/// The `metadata.version` a cluster formats at when neither `--release-version`
/// nor `--feature metadata.version` names one: Kafka 4.3's
/// `MetadataVersion.LATEST_PRODUCTION`, `4.3-IV0`.
///
/// The levels above it that the feature table carries, `4.4-IV0` to
/// `4.5-IV0`, are Kafka trunk's unstable versions. A stock 4.3 node or tool
/// does not know them, so a format names one only under
/// `--unstable-feature-versions-enable`, Kafka's
/// `unstable.feature.versions.enable`.
pub const LATEST_PRODUCTION_METADATA_VERSION: i16 = krabka_raft::LATEST_PRODUCTION_METADATA_VERSION;

/// Kafka's message for a `--feature` name its formatter does not support. The
/// list is every registered feature but `metadata.version`, by name, sorted:
/// Kafka's `Feature.PRODUCTION_FEATURES` and krabka's `krabka.version`.
fn unsupported_feature(name: &str) -> String {
    let mut supported: Vec<&str> = krabka_metadata::feature_registry()
        .iter()
        .map(|feat| feat.name())
        .filter(|feature| *feature != krabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
        .collect();
    supported.sort_unstable();
    format!(
        "Unsupported feature: {name}. Supported features are: {}",
        supported.join(", ")
    )
}

/// Kafka's `MetadataVersion.fromFeatureLevel` and then
/// `Formatter.verifyReleaseVersion`, for `--feature metadata.version=LEVEL`:
/// a level outside the emulated table does not exist, and a level that exists
/// but is past the latest production one is `not yet stable` unless `unstable`
/// allows it.
fn verify_metadata_version_level(
    level: i16,
    unstable: UnstableFeatureVersions,
) -> Result<(), String> {
    use krabka_metadata::metadata_version::{METADATA_VERSION_MIN, from_feature_level};

    let latest_testing = metadata_version_latest_testing(unstable);
    let version = from_feature_level(level)
        .filter(|_| (METADATA_VERSION_MIN..=latest_testing).contains(&level))
        .ok_or_else(|| {
            format!(
                "No MetadataVersion with feature level {level}. \
                 Valid feature levels are from {METADATA_VERSION_MIN} to {latest_testing}."
            )
        })?;
    if level > metadata_version_ceiling(unstable) {
        return Err(format!(
            "metadata.version {} is not yet stable.",
            version.ivn()
        ));
    }
    Ok(())
}

/// Resolve `krabka format`'s KIP-1022 feature flags into the bootstrap
/// `metadata.version` level and the per-feature override map, applying the
/// validation `kafka-storage format` performs:
///
/// - every `--feature` names a feature Kafka's formatter supports, which is
///   every registered one except `metadata.version`, and a level that feature
///   defines (else reject);
/// - `--feature metadata.version=X` conflicts with `--release-version`, must
///   name a level in the emulated Kafka's table, and a level past the latest
///   production one is refused as `not yet stable` unless `unstable` allows it
///   (`Formatter.verifyReleaseVersion`);
/// - `bootstrap_mv` = `--feature metadata.version` if set, else
///   `--release-version`, else [`LATEST_PRODUCTION_METADATA_VERSION`], or the
///   highest level krabka knows under `unstable` (`latestTesting`);
/// - the fully-resolved feature set satisfies every KIP-1022 dependency.
///
/// The checks run in Kafka's order, and the error texts are Kafka's:
/// `kafka-storage format` parses `--release-version` first, resolves the
/// release (`calculateEffectiveReleaseVersion`), and only then checks every
/// feature name and level (`calculateEffectiveFeatureLevels`).
pub(super) fn resolve_format_features(
    release_version: Option<&str>,
    features: &[FeatureLevelRecord],
    unstable: UnstableFeatureVersions,
) -> Result<ResolvedFeatures, String> {
    use krabka_metadata::metadata_version::METADATA_VERSION_FEATURE;

    let release_level = release_version
        .map(|rv| resolve_release_level(rv, unstable))
        .transpose()?;
    let mut feature_mv: Option<i16> = None;
    for FeatureLevelRecord { level, .. } in features
        .iter()
        .filter(|feature| feature.name == METADATA_VERSION_FEATURE)
    {
        if release_version.is_some() {
            return Err(
                "Use --release-version instead of --feature metadata.version=X to avoid ambiguity."
                    .into(),
            );
        }
        verify_metadata_version_level(*level, unstable)?;
        feature_mv = Some(*level);
    }
    let bootstrap_mv = feature_mv
        .or(release_level)
        .unwrap_or_else(|| metadata_version_ceiling(unstable));

    // Every name is checked before any level, as `calculateEffectiveFeatureLevels`
    // does. `metadata.version` is not one of Kafka's `supportedFeatures`; the
    // release checks above own it.
    if let Some(FeatureLevelRecord { name, .. }) = features.iter().find(|feature| {
        feature.name != METADATA_VERSION_FEATURE
            && krabka_metadata::feature(&feature.name).is_none()
    }) {
        return Err(unsupported_feature(name));
    }

    let mut overrides: BTreeMap<String, i16> = BTreeMap::new();
    for FeatureLevelRecord { name, level } in features {
        // KIP-853 persists kraft.version as a raft control record, never as a
        // FeatureLevelRecord. Its mode-specific validation happens separately.
        if name == KRAFT_VERSION_FEATURE {
            continue;
        }
        // A level that Kafka has not released is a level of trunk, so the
        // range is the one this node supports under `unstable`. The name was
        // checked above; `metadata.version` is checked by the release checks.
        if let Some((min, max)) = krabka_metadata::feature(name)
            .filter(|_| name != METADATA_VERSION_FEATURE)
            .map(|feat| krabka_raft::supported_feature_range(feat, unstable))
            && !(min..=max).contains(level)
        {
            return Err(format!("No feature:{name} with feature level {level}"));
        }
        if overrides.insert(name.clone(), *level).is_some() {
            return Err(format!("feature {name} specified more than once"));
        }
    }

    // KIP-1022 dependency validation over the fully-resolved feature set
    // (every registered feature at its override-or-default level).
    let resolved: BTreeMap<String, i16> = krabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let level = overrides
                .get(f.name())
                .copied()
                .unwrap_or_else(|| f.default_level(bootstrap_mv));
            (f.name().to_string(), level)
        })
        .collect();
    krabka_metadata::validate_feature_dependencies(&resolved)?;

    Ok(ResolvedFeatures {
        metadata_version: bootstrap_mv,
        overrides,
    })
}

#[cfg(test)]
mod tests {

    use {super::*, assert2::check, krabka_metadata::MetadataRecord};

    /// `unstable.feature.versions.enable` at Kafka's default.
    const STRICT: UnstableFeatureVersions = UnstableFeatureVersions::Disabled;

    /// A `--feature` spec.
    fn spec(name: &str, level: i16) -> FeatureLevelRecord {
        FeatureLevelRecord {
            name: name.to_owned(),
            level,
        }
    }

    /// A level equal to the supported minimum is in range. Every other case
    /// sits strictly inside the range or well outside it, so relaxing the
    /// guard from `<` to `<=` -- which rejects the minimum itself -- changed
    /// nothing any test looked at.
    #[test]
    fn resolve_features_accepts_a_level_at_the_supported_minimum() {
        // group.version supports 0..=1; metadata.version 7..=25.
        check!(resolve_format_features(None, &[spec("group.version", 0)], STRICT).is_ok());
        check!(resolve_format_features(None, &[spec("metadata.version", 7)], STRICT).is_ok());
    }

    #[yare::parameterized(
        kafka_4_0 = { "4.0", Some(25) },
        kafka_3_7_iv4 = { "3.7-IV4", Some(19) },
        below_supported_minimum = { "2.8", None },
        unknown_release = { "9.9-IV0", None },
    )]
    fn release_version_maps_to_feature_level(input: &str, want: Option<i16>) {
        assert2::assert!(resolve_release_level(input, STRICT).ok() == want);
    }

    #[test]
    fn bootstrap_seeds_every_nonzero_feature_at_release_default() {
        let bootstrap_mv = krabka_metadata::metadata_version::from_version_string("4.0")
            .unwrap()
            .feature_level();
        // Exercises the exact helper `run()` uses, so it tracks the registry
        // as features are added in later tasks. Features whose release default
        // is 0 are omitted (KIP-1022: level 0 = absent = disabled), matching
        // `kafka-storage format`.
        let records = krabka_metadata::bootstrap_feature_records(bootstrap_mv);
        for feat in krabka_metadata::feature_registry() {
            let found = records.iter().find_map(|r| match r {
                MetadataRecord::V1FeatureLevel(f) if f.name == feat.name() => Some(f.level),
                _ => None,
            });
            let expected = feat.default_level(bootstrap_mv);
            if expected > 0 {
                assert2::assert!(found == Some(expected));
            } else {
                assert2::assert!(found.is_none());
            }
        }
    }

    // The no-flag default path (`LATEST_PRODUCTION_METADATA_VERSION`) is
    // covered end-to-end by `format_smoke.rs`, which formats without
    // `--release-version` and asserts the FeatureLevel record is present.
    #[test]
    fn short_release_string_resolves_to_its_last_iv() {
        assert2::assert!(resolve_release_level("4.0", STRICT).unwrap() == 25);
    }

    #[test]
    fn parse_feature_spec_happy_path() {
        assert2::assert!(
            parse_feature_spec("group.version=1").unwrap() == spec("group.version", 1)
        );
        assert2::assert!(
            parse_feature_spec("metadata.version=20").unwrap() == spec("metadata.version", 20)
        );
    }

    #[yare::parameterized(
        missing_separator = { "noequals" },
        non_integer_level = { "group.version=abc" },
        empty_level = { "group.version=" },
        empty_name = { "=1" },
    )]
    fn parse_feature_spec_error_branches(bad: &str) {
        assert2::assert!(parse_feature_spec(bad).is_err());
    }

    #[test]
    fn resolve_features_defaults_bootstrap_mv_to_latest_production() {
        // No --release-version, no metadata.version override → bootstrap at
        // 4.3-IV0, not at trunk's unstable levels; an explicit non-metadata
        // feature becomes an override.
        let ResolvedFeatures {
            metadata_version: mv,
            overrides: ov,
        } = resolve_format_features(None, &[spec("group.version", 1)], STRICT).expect("resolve");
        assert2::assert!(mv == LATEST_PRODUCTION_METADATA_VERSION);
        assert2::assert!(ov.get("group.version") == Some(&1));
    }

    #[test]
    fn resolve_features_metadata_version_feature_sets_bootstrap_mv() {
        let ResolvedFeatures {
            metadata_version: mv,
            overrides: ov,
        } = resolve_format_features(None, &[spec("metadata.version", 20)], STRICT)
            .expect("resolve");
        assert2::assert!(mv == 20);
        assert2::assert!(ov.get("metadata.version") == Some(&20));
    }

    #[test]
    fn resolve_features_release_version_sets_bootstrap_mv() {
        let ResolvedFeatures {
            metadata_version: mv,
            overrides: ov,
        } = resolve_format_features(Some("4.0-IV0"), &[], STRICT).expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.is_empty());
    }

    #[test]
    fn resolve_features_release_and_feature_combine() {
        // --release-version sets the base; a non-metadata --feature overrides it.
        let ResolvedFeatures {
            metadata_version: mv,
            overrides: ov,
        } = resolve_format_features(Some("4.0-IV0"), &[spec("transaction.version", 2)], STRICT)
            .expect("resolve");
        assert2::assert!(mv == 22);
        assert2::assert!(ov.get("transaction.version") == Some(&2));
    }

    /// Kafka's `Feature.PRODUCTION_FEATURES` names and krabka's own
    /// `krabka.version`, sorted: what a formatter that meets an unknown
    /// `--feature` name lists. It has no `metadata.version`.
    const SUPPORTED_FEATURES: &str = "eligible.leader.replicas.version, group.version, \
                                      krabka.version, kraft.version, share.version, \
                                      streams.version, transaction.version";

    /// The releases `MetadataVersion.metadataVersionsToString` lists while
    /// unstable feature versions are off.
    const STRICT_RELEASES: &str = "3.3-IV3, 3.4-IV0, 3.5-IV0, 3.5-IV1, 3.5-IV2, 3.6-IV0, \
                                   3.6-IV1, 3.6-IV2, 3.7-IV0, 3.7-IV1, 3.7-IV2, 3.7-IV3, \
                                   3.7-IV4, 3.8-IV0, 3.9-IV0, 4.0-IV0, 4.0-IV1, 4.0-IV2, \
                                   4.0-IV3, 4.1-IV0, 4.1-IV1, 4.2-IV0, 4.2-IV1, 4.3-IV0";

    fn unknown_release(release: &str) -> String {
        format!(
            "Unknown metadata.version '{release}'. Supported metadata.version are: \
             {STRICT_RELEASES}"
        )
    }

    /// `kafka-storage format` maps `--release-version` through
    /// `MetadataVersion.fromVersionString`, which looks the string up by its
    /// first two dot-separated segments and does not trim it.
    #[yare::parameterized(
        production_release = { "4.3", STRICT, Some(30) },
        three_segment_release = { "4.3.1", STRICT, Some(30) },
        nonnumeric_suffix = { "4.3.x", STRICT, Some(30) },
        extra_segments = { "4.3.1.7", STRICT, Some(30) },
        old_release = { "4.0.0", STRICT, Some(25) },
        kafka_example = { "3.8.1", STRICT, Some(20) },
        iv_suffix_and_extra = { "4.3-IV0.9", STRICT, Some(30) },
        old_iv_suffix_and_extra = { "3.7-IV4.1", STRICT, Some(19) },
        trunk_release_enabled = { "4.4.1", UnstableFeatureVersions::Enabled, Some(33) },
        trunk_release_disabled = { "4.4.1", STRICT, None },
        trunk_iv_disabled = { "4.4-IV0.1", STRICT, None },
        pre_minimum_release = { "2.8.1", STRICT, None },
        trailing_extra_segment = { "4.3.1.", STRICT, Some(30) },
        trailing_empty_segment = { "4.3.", STRICT, None },
        empty_middle_segment = { "4..3", STRICT, None },
        missing_minor = { "4", STRICT, None },
        empty = { "", STRICT, None },
        whitespace_after_extra_segment = { "4.3.1 ", STRICT, Some(30) },
        whitespace_before_release = { " 4.3", STRICT, None },
        whitespace_before_extra_segment = { " 4.3.1", STRICT, None },
        whitespace_after_release = { "4.3 ", STRICT, None },
        tab_after_release = { "4.3\t", STRICT, None },
        whitespace_after_iv = { "4.3-IV0 ", STRICT, None },
    )]
    fn release_versions_are_looked_up_by_their_first_two_segments(
        release: &str,
        unstable: UnstableFeatureVersions,
        want: Option<i16>,
    ) {
        let want = want.ok_or_else(|| unknown_release(release));
        assert2::check!(
            resolve_release_level(release, unstable) == want,
            "{release:?} {unstable:?}"
        );
    }

    /// Kafka 4.3.1's `kafka-storage format` messages, word for word: the
    /// unknown-feature list is its production features and `krabka.version`
    /// in name order, and a level a feature does not define is
    /// `Feature.fromFeatureLevel`'s refusal.
    #[yare::parameterized(
        unknown_feature = {
            "bogus.version", 1,
            "Unsupported feature: bogus.version. Supported features are: \
             eligible.leader.replicas.version, group.version, krabka.version, \
             kraft.version, share.version, streams.version, transaction.version"
        },
        transaction_level = {
            "transaction.version",
            9,
            "No feature:transaction.version with feature level 9",
        },
        group_level = { "group.version", 5, "No feature:group.version with feature level 5" },
    )]
    fn feature_refusals_use_kafkas_messages(feature: &str, level: i16, expected: &str) {
        let err = resolve_format_features(None, &[spec(feature, level)], STRICT).unwrap_err();
        assert2::assert!(err == expected, "{feature:?}");
    }

    #[yare::parameterized(
        group_version_above_maximum = { "group.version", 5 },
        metadata_version_above_maximum = { "metadata.version", 99 },
        metadata_version_below_minimum = { "metadata.version", 1 },
    )]
    fn resolve_features_rejects_out_of_range_level(name: &str, level: i16) {
        assert2::assert!(resolve_format_features(None, &[spec(name, level)], STRICT).is_err());
    }

    /// `--release-version 4.3.1` reaches the bootstrap `metadata.version`
    /// through the whole feature resolution, not only the release lookup.
    #[test]
    fn a_three_segment_release_version_formats_at_its_release() {
        check!(
            resolve_format_features(Some("4.3.1"), &[], STRICT)
                == Ok(ResolvedFeatures {
                    metadata_version: 30,
                    overrides: BTreeMap::new(),
                })
        );
    }

    /// Every rejection `kafka-storage format` makes about `--feature` and
    /// `--release-version`, with Kafka 4.3.1's text (`Formatter`,
    /// `Feature.fromFeatureLevel`, `MetadataVersion.fromFeatureLevel`), and
    /// the order it makes them in: the release first, then every name, then
    /// every level.
    #[yare::parameterized(
        unknown_feature = {
            None,
            vec![spec("bogus.version", 1)],
            STRICT,
            format!("Unsupported feature: bogus.version. Supported features are: {SUPPORTED_FEATURES}"),
        },
        unknown_feature_unstable = {
            None,
            vec![spec("bogus.version", 1)],
            UnstableFeatureVersions::Enabled,
            format!("Unsupported feature: bogus.version. Supported features are: {SUPPORTED_FEATURES}"),
        },
        group_level = {
            None,
            vec![spec("group.version", 5)],
            STRICT,
            "No feature:group.version with feature level 5".to_owned(),
        },
        transaction_level = {
            None,
            vec![spec("transaction.version", 3)],
            STRICT,
            "No feature:transaction.version with feature level 3".to_owned(),
        },
        negative_level = {
            None,
            vec![spec("group.version", -1)],
            STRICT,
            "No feature:group.version with feature level -1".to_owned(),
        },
        metadata_below_table = {
            None,
            vec![spec("metadata.version", 6)],
            STRICT,
            "No MetadataVersion with feature level 6. Valid feature levels are from 7 to 31.".to_owned(),
        },
        metadata_one_below_table = {
            None,
            vec![spec("metadata.version", 1)],
            STRICT,
            "No MetadataVersion with feature level 1. Valid feature levels are from 7 to 31.".to_owned(),
        },
        unstable_level_not_stable = {
            None,
            vec![spec("metadata.version", 31)],
            STRICT,
            "metadata.version 4.4-IV0 is not yet stable.".to_owned(),
        },
        metadata_32_above_table = {
            None,
            vec![spec("metadata.version", 32)],
            STRICT,
            "No MetadataVersion with feature level 32. Valid feature levels are from 7 to 31.".to_owned(),
        },
        metadata_33_above_table = {
            None,
            vec![spec("metadata.version", 33)],
            STRICT,
            "No MetadataVersion with feature level 33. Valid feature levels are from 7 to 31.".to_owned(),
        },
        metadata_34_above_table = {
            None,
            vec![spec("metadata.version", 34)],
            STRICT,
            "No MetadataVersion with feature level 34. Valid feature levels are from 7 to 31.".to_owned(),
        },
        metadata_99_above_table = {
            None,
            vec![spec("metadata.version", 99)],
            STRICT,
            "No MetadataVersion with feature level 99. Valid feature levels are from 7 to 31.".to_owned(),
        },
        unstable_below_table = {
            None,
            vec![spec("metadata.version", 6)],
            UnstableFeatureVersions::Enabled,
            "No MetadataVersion with feature level 6. Valid feature levels are from 7 to 34.".to_owned(),
        },
        unstable_above_table = {
            None,
            vec![spec("metadata.version", 35)],
            UnstableFeatureVersions::Enabled,
            "No MetadataVersion with feature level 35. Valid feature levels are from 7 to 34.".to_owned(),
        },
        unknown_release_precedes_feature = {
            Some("9.9"),
            vec![spec("bogus.version", 1)],
            STRICT,
            unknown_release("9.9"),
        },
        release_and_metadata_are_ambiguous = {
            Some("4.0-IV0"),
            vec![spec("metadata.version", 24)],
            STRICT,
            "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.".to_owned(),
        },
        ambiguity_precedes_bad_level = {
            Some("4.0-IV0"),
            vec![spec("metadata.version", 99)],
            STRICT,
            "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.".to_owned(),
        },
        metadata_error_precedes_unknown_name = {
            None,
            vec![spec("bogus.version", 1), spec("metadata.version", 99)],
            STRICT,
            "No MetadataVersion with feature level 99. Valid feature levels are from 7 to 31.".to_owned(),
        },
        unknown_name_precedes_bad_level = {
            None,
            vec![spec("group.version", 5), spec("bogus.version", 1)],
            STRICT,
            format!("Unsupported feature: bogus.version. Supported features are: {SUPPORTED_FEATURES}"),
        },
    )]
    fn feature_errors_are_kafkas_formatter_errors(
        release: Option<&str>,
        features: Vec<FeatureLevelRecord>,
        unstable: UnstableFeatureVersions,
        want: String,
    ) {
        assert2::check!(
            resolve_format_features(release, &features, unstable)
                .map(|resolved| resolved.metadata_version)
                == Err(want),
            "{release:?} {features:?} {unstable:?}"
        );
    }

    /// #784: a Kafka trunk `metadata.version` (4.4-IV0 to 4.5-IV0) is
    /// refused with Kafka 4.3.1's `kafka-storage format` messages unless
    /// unstable feature versions are enabled, and the default release is
    /// `latestTesting` when they are.
    #[yare::parameterized(
        default_stable = { None, None, STRICT, Ok(30) },
        default_trunk = { None, None, UnstableFeatureVersions::Enabled, Ok(34) },
        unknown_iv_stable = { Some("4.4"), None, STRICT, Err(unknown_release("4.4")) },
        first_trunk_iv = { Some("4.4-IV0"), None, UnstableFeatureVersions::Enabled, Ok(31) },
        trunk_release = { Some("4.4"), None, UnstableFeatureVersions::Enabled, Ok(33) },
        next_trunk_iv = { Some("4.5-IV0"), None, UnstableFeatureVersions::Enabled, Ok(34) },
        next_trunk_release = { Some("4.5"), None, UnstableFeatureVersions::Enabled, Ok(34) },
        trunk_iv_stable_refused = {
            Some("4.5-IV0"),
            None,
            STRICT,
            Err(unknown_release("4.5-IV0")),
        },
        explicit_unstable_level_refused = {
            None,
            Some(31),
            STRICT,
            Err("metadata.version 4.4-IV0 is not yet stable.".to_owned()),
        },
        unknown_level_33_stable = {
            None,
            Some(33),
            STRICT,
            Err("No MetadataVersion with feature level 33. Valid feature levels are from 7 to 31.".to_owned()),
        },
        unknown_level_34_stable = {
            None,
            Some(34),
            STRICT,
            Err("No MetadataVersion with feature level 34. Valid feature levels are from 7 to 31.".to_owned()),
        },
        explicit_stable_level = { None, Some(30), STRICT, Ok(30) },
        explicit_trunk_level_31 = { None, Some(31), UnstableFeatureVersions::Enabled, Ok(31) },
        explicit_trunk_level_32 = { None, Some(32), UnstableFeatureVersions::Enabled, Ok(32) },
        explicit_trunk_level_33 = { None, Some(33), UnstableFeatureVersions::Enabled, Ok(33) },
        explicit_trunk_level_34 = { None, Some(34), UnstableFeatureVersions::Enabled, Ok(34) },
    )]
    fn unstable_metadata_versions_follow_unstable_feature_versions_enable(
        release: Option<&str>,
        level: Option<i16>,
        unstable: UnstableFeatureVersions,
        want: Result<i16, String>,
    ) {
        let features = level.map_or_else(Vec::new, |level| vec![spec("metadata.version", level)]);
        assert2::check!(
            resolve_format_features(release, &features, unstable)
                .map(|resolved| resolved.metadata_version)
                == want,
            "{release:?} {features:?} {unstable:?}"
        );
    }

    /// KIP-1191: `share.version` 2 is Kafka trunk's `SV_2`. Kafka 4.3.1 stops
    /// at 1, so a default format refuses 2 and seeds 1; with unstable feature
    /// versions the level is accepted, and a format at `4.4-IV0` or later
    /// seeds it as trunk's default, from the registry's `default_level`.
    #[yare::parameterized(
        default_stable = { None, None, STRICT, Ok(Some(1)) },
        explicit_one_stable = { None, Some(1), STRICT, Ok(Some(1)) },
        explicit_two_stable_refused = {
            None,
            Some(2),
            STRICT,
            Err("No feature:share.version with feature level 2".to_owned()),
        },
        explicit_two_trunk = { None, Some(2), UnstableFeatureVersions::Enabled, Ok(Some(2)) },
        explicit_one_trunk = { None, Some(1), UnstableFeatureVersions::Enabled, Ok(Some(1)) },
        default_trunk = { None, None, UnstableFeatureVersions::Enabled, Ok(Some(2)) },
        release_4_4_iv0_trunk = {
            Some("4.4-IV0"),
            None,
            UnstableFeatureVersions::Enabled,
            Ok(Some(2)),
        },
        release_4_3_trunk = { Some("4.3"), None, UnstableFeatureVersions::Enabled, Ok(Some(1)) },
        release_4_1_trunk = { Some("4.1"), None, UnstableFeatureVersions::Enabled, Ok(None) },
    )]
    fn share_version_two_is_trunks_level(
        release: Option<&str>,
        level: Option<i16>,
        unstable: UnstableFeatureVersions,
        want: Result<Option<i16>, String>,
    ) {
        let features = level.map_or_else(Vec::new, |level| vec![spec("share.version", level)]);
        let seeded = resolve_format_features(release, &features, unstable).map(
            |ResolvedFeatures {
                 metadata_version: bootstrap_mv,
                 overrides,
             }| {
                krabka_metadata::bootstrap_feature_records_with_overrides(bootstrap_mv, &overrides)
                    .into_iter()
                    .find_map(|record| match record {
                        MetadataRecord::V1FeatureLevel(f) if f.name == "share.version" => {
                            Some(f.level)
                        }
                        _ => None,
                    })
            },
        );
        assert2::check!(seeded == want, "{release:?} {features:?} {unstable:?}");
    }

    /// `krabka.version` bootstraps at level 0, which seeds no record, as Kafka
    /// omits every level-0 feature from `bootstrap.checkpoint`. A JVM node in
    /// a mixed cluster supports only level 0, so the level is left for an
    /// operator to finalize once every node is krabka. `--feature
    /// krabka.version=N` overrides it, as `kafka-storage format --feature`
    /// overrides a Kafka feature, and a level past the supported range is
    /// `Feature.fromFeatureLevel`'s refusal.
    #[yare::parameterized(
        no_override = { "no override" },
        override_one = { "override to 1" },
        override_zero = { "override to 0" },
        override_above_range = { "override past the range" },
    )]
    fn krabka_version_bootstraps_at_level_zero_unless_overridden(scenario: &str) {
        /// What it is, the `--feature` flags, and the seeded records or the error.
        struct Case<'a> {
            what: &'a str,
            features: Vec<FeatureLevelRecord>,
            want: Result<Vec<MetadataRecord>, String>,
        }
        let feature = |name: &str, level| {
            MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
                name: name.into(),
                level,
            })
        };
        // The latest production release's defaults, Kafka 4.3's.
        let release_defaults = vec![
            feature("metadata.version", LATEST_PRODUCTION_METADATA_VERSION),
            feature("group.version", 1),
            feature("transaction.version", 2),
            feature("share.version", 1),
            feature("streams.version", 1),
            feature("eligible.leader.replicas.version", 1),
        ];
        let with_krabka = |level| {
            let mut records = release_defaults.clone();
            records.push(feature("krabka.version", level));
            records
        };
        let krabka = |level| vec![spec("krabka.version", level)];
        let cases = [
            Case {
                what: "no override",
                features: vec![],
                want: Ok(release_defaults.clone()),
            },
            Case {
                what: "override to 1",
                features: krabka(1),
                want: Ok(with_krabka(1)),
            },
            Case {
                what: "override to 0",
                features: krabka(0),
                want: Ok(release_defaults.clone()),
            },
            Case {
                what: "override past the range",
                features: krabka(2),
                want: Err("No feature:krabka.version with feature level 2".to_owned()),
            },
        ];
        let Case {
            what,
            features,
            want,
        } = cases
            .into_iter()
            .find(|case| case.what == scenario)
            .expect("named yare case exists");
        let seeded = resolve_format_features(None, &features, STRICT).map(
            |ResolvedFeatures {
                 metadata_version: bootstrap_mv,
                 overrides,
             }| {
                krabka_metadata::bootstrap_feature_records_with_overrides(bootstrap_mv, &overrides)
            },
        );
        assert2::check!(seeded == want, "{what}");
        assert2::check!(parse_feature_spec("krabka.version=0") == Ok(spec("krabka.version", 0)));
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert2::assert!(resolve_format_features(Some("2.8"), &[], STRICT).is_err());
    }
}
