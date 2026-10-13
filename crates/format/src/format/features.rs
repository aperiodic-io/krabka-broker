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

    #[test]
    fn release_version_maps_to_feature_level() {
        for (input, want) in [
            ("4.0", Some(25)),
            ("3.7-IV4", Some(19)),
            ("2.8", None),     // below MIN / unknown
            ("9.9-IV0", None), // unknown
        ] {
            assert2::assert!(resolve_release_level(input, STRICT).ok() == want);
        }
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

    #[test]
    fn parse_feature_spec_error_branches() {
        for bad in [
            "noequals",          // missing '='
            "group.version=abc", // non-integer level
            "group.version=",    // empty level
            "=1",                // empty name
        ] {
            assert2::assert!(parse_feature_spec(bad).is_err());
        }
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
    #[test]
    fn release_versions_are_looked_up_by_their_first_two_segments() {
        let trunk = UnstableFeatureVersions::Enabled;
        // (release string, unstable, level, or None for the unknown-release error)
        let cases = [
            ("4.3", STRICT, Some(30)),
            // The natural spelling of a 4.3.1 target.
            ("4.3.1", STRICT, Some(30)),
            ("4.3.x", STRICT, Some(30)),
            ("4.3.1.7", STRICT, Some(30)),
            ("4.0.0", STRICT, Some(25)),
            // Kafka's own example: 3.8.1 parses to 3.8-IV0.
            ("3.8.1", STRICT, Some(20)),
            ("4.3-IV0.9", STRICT, Some(30)),
            ("3.7-IV4.1", STRICT, Some(19)),
            ("4.4.1", trunk, Some(33)),
            ("4.4.1", STRICT, None),
            ("4.4-IV0.1", STRICT, None),
            ("2.8.1", STRICT, None),
            // Java's `split` drops trailing empty segments: `4.3.1.` looks up
            // `4.3`, and `4.3.` is left as it stands.
            ("4.3.1.", STRICT, Some(30)),
            ("4.3.", STRICT, None),
            ("4..3", STRICT, None),
            ("4", STRICT, None),
            ("", STRICT, None),
            // Kafka does not trim the key. A third segment is not part of it.
            ("4.3.1 ", STRICT, Some(30)),
            (" 4.3", STRICT, None),
            (" 4.3.1", STRICT, None),
            ("4.3 ", STRICT, None),
            ("4.3\t", STRICT, None),
            ("4.3-IV0 ", STRICT, None),
        ];
        for (release, unstable, want) in cases {
            let want = want.ok_or_else(|| unknown_release(release));
            check!(
                resolve_release_level(release, unstable) == want,
                "{release:?} {unstable:?}"
            );
        }
    }

    /// Kafka 4.3.1's `kafka-storage format` messages, word for word: the
    /// unknown-feature list is its production features and `krabka.version`
    /// in name order, and a level a feature does not define is
    /// `Feature.fromFeatureLevel`'s refusal.
    #[test]
    fn feature_refusals_use_kafkas_messages() {
        for (feature, expected) in [
            (
                ("bogus.version", 1),
                "Unsupported feature: bogus.version. Supported features are: \
                 eligible.leader.replicas.version, group.version, krabka.version, \
                 kraft.version, share.version, streams.version, transaction.version",
            ),
            (
                ("transaction.version", 9),
                "No feature:transaction.version with feature level 9",
            ),
            (
                ("group.version", 5),
                "No feature:group.version with feature level 5",
            ),
        ] {
            let err =
                resolve_format_features(None, &[spec(feature.0, feature.1)], STRICT).unwrap_err();
            assert2::assert!(err == expected, "{feature:?}");
        }
    }

    #[test]
    fn resolve_features_rejects_out_of_range_level() {
        for (name, level) in [
            ("group.version", 5),     // group.version supports 0..=1
            ("metadata.version", 99), // metadata.version supports 7..=25
            ("metadata.version", 1),
        ] {
            assert2::assert!(resolve_format_features(None, &[spec(name, level)], STRICT).is_err());
        }
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
    #[test]
    fn feature_errors_are_kafkas_formatter_errors() {
        /// What it is, `--release-version`, the `--feature` flags, whether
        /// unstable versions are enabled, and the expected error.
        struct Case<'a> {
            what: &'a str,
            release: Option<&'a str>,
            features: Vec<FeatureLevelRecord>,
            unstable: UnstableFeatureVersions,
            want: String,
        }
        let trunk = UnstableFeatureVersions::Enabled;
        let mv = |level| spec("metadata.version", level);
        let unsupported = |name: &str| {
            format!("Unsupported feature: {name}. Supported features are: {SUPPORTED_FEATURES}")
        };
        let no_feature =
            |name: &str, level: i16| format!("No feature:{name} with feature level {level}");
        let no_metadata_version = |level: i16, top: i16| {
            format!(
                "No MetadataVersion with feature level {level}. \
                 Valid feature levels are from 7 to {top}."
            )
        };
        let not_stable = |ivn: &str| format!("metadata.version {ivn} is not yet stable.");
        let ambiguity =
            "Use --release-version instead of --feature metadata.version=X to avoid ambiguity.";

        let cases: Vec<Case<'_>> = vec![
            // An unknown name, in both modes. `metadata.version` is not in the list.
            Case {
                what: "unknown feature",
                release: None,
                features: vec![spec("bogus.version", 1)],
                unstable: STRICT,
                want: unsupported("bogus.version"),
            },
            Case {
                what: "unknown feature, unstable",
                release: None,
                features: vec![spec("bogus.version", 1)],
                unstable: trunk,
                want: unsupported("bogus.version"),
            },
            // A level the feature does not define.
            Case {
                what: "group.version above its range",
                release: None,
                features: vec![spec("group.version", 5)],
                unstable: STRICT,
                want: no_feature("group.version", 5),
            },
            Case {
                what: "transaction.version above its range",
                release: None,
                features: vec![spec("transaction.version", 3)],
                unstable: STRICT,
                want: no_feature("transaction.version", 3),
            },
            Case {
                what: "negative level",
                release: None,
                features: vec![spec("group.version", -1)],
                unstable: STRICT,
                want: no_feature("group.version", -1),
            },
            // metadata.version is a table of levels, of which 4.3.1 knows 7 to 31.
            Case {
                what: "below the table",
                release: None,
                features: vec![mv(6)],
                unstable: STRICT,
                want: no_metadata_version(6, 31),
            },
            Case {
                what: "1 is below the table",
                release: None,
                features: vec![mv(1)],
                unstable: STRICT,
                want: no_metadata_version(1, 31),
            },
            Case {
                what: "31 exists and is not stable",
                release: None,
                features: vec![mv(31)],
                unstable: STRICT,
                want: not_stable("4.4-IV0"),
            },
            Case {
                what: "32 is past 4.3.1's table",
                release: None,
                features: vec![mv(32)],
                unstable: STRICT,
                want: no_metadata_version(32, 31),
            },
            Case {
                what: "33 is past 4.3.1's table",
                release: None,
                features: vec![mv(33)],
                unstable: STRICT,
                want: no_metadata_version(33, 31),
            },
            Case {
                what: "34 is past 4.3.1's table",
                release: None,
                features: vec![mv(34)],
                unstable: STRICT,
                want: no_metadata_version(34, 31),
            },
            Case {
                what: "99 is past 4.3.1's table",
                release: None,
                features: vec![mv(99)],
                unstable: STRICT,
                want: no_metadata_version(99, 31),
            },
            // Under unstable feature versions the table is the one krabka knows.
            Case {
                what: "below the table, unstable",
                release: None,
                features: vec![mv(6)],
                unstable: trunk,
                want: no_metadata_version(6, 34),
            },
            Case {
                what: "past the table, unstable",
                release: None,
                features: vec![mv(35)],
                unstable: trunk,
                want: no_metadata_version(35, 34),
            },
            // Order: the release first, then names, then levels.
            Case {
                what: "the release string outranks a bad feature",
                release: Some("9.9"),
                features: vec![spec("bogus.version", 1)],
                unstable: STRICT,
                want: unknown_release("9.9"),
            },
            Case {
                what: "a release and a metadata.version feature",
                release: Some("4.0-IV0"),
                features: vec![mv(24)],
                unstable: STRICT,
                want: ambiguity.to_owned(),
            },
            Case {
                what: "ambiguity outranks a level outside the table",
                release: Some("4.0-IV0"),
                features: vec![mv(99)],
                unstable: STRICT,
                want: ambiguity.to_owned(),
            },
            Case {
                what: "a metadata.version problem outranks an unknown name",
                release: None,
                features: vec![spec("bogus.version", 1), mv(99)],
                unstable: STRICT,
                want: no_metadata_version(99, 31),
            },
            Case {
                what: "an unknown name outranks a bad level",
                release: None,
                features: vec![spec("group.version", 5), spec("bogus.version", 1)],
                unstable: STRICT,
                want: unsupported("bogus.version"),
            },
        ];
        for Case {
            what,
            release,
            features,
            unstable,
            want,
        } in cases
        {
            check!(
                resolve_format_features(release, &features, unstable)
                    .map(|resolved| resolved.metadata_version)
                    == Err(want),
                "{what}"
            );
        }
    }

    /// #784: a Kafka trunk `metadata.version` (4.4-IV0 to 4.5-IV0) is
    /// refused with Kafka 4.3.1's `kafka-storage format` messages unless
    /// unstable feature versions are enabled, and the default release is
    /// `latestTesting` when they are.
    #[test]
    fn unstable_metadata_versions_follow_unstable_feature_versions_enable() {
        /// `--release-version`, the `--feature` flags, whether unstable versions
        /// are enabled, and the expected `metadata.version` or error.
        struct Case<'a> {
            release: Option<&'a str>,
            features: Vec<FeatureLevelRecord>,
            unstable: UnstableFeatureVersions,
            want: Result<i16, String>,
        }
        let trunk = UnstableFeatureVersions::Enabled;
        let mv = |level| vec![spec("metadata.version", level)];
        let cases: Vec<Case<'_>> = vec![
            Case { release: None, features: vec![], unstable: STRICT, want: Ok(30) },
            Case { release: None, features: vec![], unstable: trunk, want: Ok(34) },
            Case { release: Some("4.4"), features: vec![], unstable: STRICT, want: Err(unknown_release("4.4")) },
            Case { release: Some("4.4-IV0"), features: vec![], unstable: trunk, want: Ok(31) },
            Case { release: Some("4.4"), features: vec![], unstable: trunk, want: Ok(33) },
            Case { release: Some("4.5-IV0"), features: vec![], unstable: trunk, want: Ok(34) },
            Case { release: Some("4.5"), features: vec![], unstable: trunk, want: Ok(34) },
            Case { release: Some("4.5-IV0"), features: vec![], unstable: STRICT, want: Err(unknown_release("4.5-IV0")) },
            Case { release: None, features: mv(31), unstable: STRICT, want: Err("metadata.version 4.4-IV0 is not yet stable.".to_owned()) },
            // 4.3.1's table ends at 4.4-IV0, so the trunk levels do not exist.
            Case { release: None, features: mv(33), unstable: STRICT, want: Err("No MetadataVersion with feature level 33. Valid feature levels are from 7 to 31.".to_owned()) },
            Case { release: None, features: mv(34), unstable: STRICT, want: Err("No MetadataVersion with feature level 34. Valid feature levels are from 7 to 31.".to_owned()) },
            Case { release: None, features: mv(30), unstable: STRICT, want: Ok(30) },
            Case { release: None, features: mv(31), unstable: trunk, want: Ok(31) },
            Case { release: None, features: mv(32), unstable: trunk, want: Ok(32) },
            Case { release: None, features: mv(33), unstable: trunk, want: Ok(33) },
            Case { release: None, features: mv(34), unstable: trunk, want: Ok(34) },
        ];
        for Case {
            release,
            features,
            unstable,
            want,
        } in cases
        {
            check!(
                resolve_format_features(release, &features, unstable)
                    .map(|resolved| resolved.metadata_version)
                    == want,
                "{release:?} {features:?} {unstable:?}"
            );
        }
    }

    /// KIP-1191: `share.version` 2 is Kafka trunk's `SV_2`. Kafka 4.3.1 stops
    /// at 1, so a default format refuses 2 and seeds 1; with unstable feature
    /// versions the level is accepted, and a format at `4.4-IV0` or later
    /// seeds it as trunk's default, from the registry's `default_level`.
    #[test]
    fn share_version_two_is_trunks_level() {
        /// `--release-version`, the `--feature` flags, whether unstable versions
        /// are enabled, and the seeded `share.version` or the error.
        struct Case<'a> {
            release: Option<&'a str>,
            features: Vec<FeatureLevelRecord>,
            unstable: UnstableFeatureVersions,
            want: Result<Option<i16>, String>,
        }
        let trunk = UnstableFeatureVersions::Enabled;
        let share = |level| vec![spec("share.version", level)];
        let cases: Vec<Case<'_>> = vec![
            Case {
                release: None,
                features: vec![],
                unstable: STRICT,
                want: Ok(Some(1)),
            },
            Case {
                release: None,
                features: share(1),
                unstable: STRICT,
                want: Ok(Some(1)),
            },
            Case {
                release: None,
                features: share(2),
                unstable: STRICT,
                want: Err("No feature:share.version with feature level 2".to_owned()),
            },
            Case {
                release: None,
                features: share(2),
                unstable: trunk,
                want: Ok(Some(2)),
            },
            Case {
                release: None,
                features: share(1),
                unstable: trunk,
                want: Ok(Some(1)),
            },
            // trunk's default at `4.4-IV0` and above
            Case {
                release: None,
                features: vec![],
                unstable: trunk,
                want: Ok(Some(2)),
            },
            Case {
                release: Some("4.4-IV0"),
                features: vec![],
                unstable: trunk,
                want: Ok(Some(2)),
            },
            // below `4.4-IV0` the release default is 1, and 0 below `4.2-IV0`
            Case {
                release: Some("4.3"),
                features: vec![],
                unstable: trunk,
                want: Ok(Some(1)),
            },
            Case {
                release: Some("4.1"),
                features: vec![],
                unstable: trunk,
                want: Ok(None),
            },
        ];
        for Case {
            release,
            features,
            unstable,
            want,
        } in cases
        {
            let seeded = resolve_format_features(release, &features, unstable).map(
                |ResolvedFeatures {
                     metadata_version: bootstrap_mv,
                     overrides,
                 }| {
                    krabka_metadata::bootstrap_feature_records_with_overrides(
                        bootstrap_mv,
                        &overrides,
                    )
                    .into_iter()
                    .find_map(|record| match record {
                        MetadataRecord::V1FeatureLevel(f) if f.name == "share.version" => {
                            Some(f.level)
                        }
                        _ => None,
                    })
                },
            );
            check!(seeded == want, "{release:?} {features:?} {unstable:?}");
        }
    }

    /// `krabka.version` bootstraps at level 0, which seeds no record, as Kafka
    /// omits every level-0 feature from `bootstrap.checkpoint`. A JVM node in
    /// a mixed cluster supports only level 0, so the level is left for an
    /// operator to finalize once every node is krabka. `--feature
    /// krabka.version=N` overrides it, as `kafka-storage format --feature`
    /// overrides a Kafka feature, and a level past the supported range is
    /// `Feature.fromFeatureLevel`'s refusal.
    #[test]
    fn krabka_version_bootstraps_at_level_zero_unless_overridden() {
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
        let actual: Vec<_> = cases
            .iter()
            .map(|Case { what, features, .. }| {
                let seeded = resolve_format_features(None, features, STRICT).map(
                    |ResolvedFeatures {
                         metadata_version: bootstrap_mv,
                         overrides,
                     }| {
                        krabka_metadata::bootstrap_feature_records_with_overrides(
                            bootstrap_mv,
                            &overrides,
                        )
                    },
                );
                (*what, seeded)
            })
            .collect();
        let expected: Vec<_> = cases
            .into_iter()
            .map(|Case { what, want, .. }| (what, want))
            .collect();
        check!(actual == expected);
        check!(parse_feature_spec("krabka.version=0") == Ok(spec("krabka.version", 0)));
    }

    #[test]
    fn resolve_features_rejects_bad_release_string() {
        assert2::assert!(resolve_format_features(Some("2.8"), &[], STRICT).is_err());
    }
}
