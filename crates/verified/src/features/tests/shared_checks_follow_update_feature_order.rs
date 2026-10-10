use assert2::assert;

use super::*;

#[test]
fn shared_checks_follow_update_feature_order() {
    let unsupported = |facts: FeatureUpdateFacts| FeatureUpdateFacts {
        all_nodes_support: false,
        ..facts
    };
    let cases = [
        ("upgrade", row(FeatureRowSetup::default()), D::EmitFeature),
        (
            "same level",
            row(FeatureRowSetup {
                current: FeatureLevel(1),
                ..Default::default()
            }),
            D::EmitFeature,
        ),
        (
            "safe downgrade",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                requested: FeatureLevel(0),
                current: FeatureLevel(1),
                ..Default::default()
            }),
            D::EmitFeature,
        ),
        (
            "unsafe downgrade",
            row(FeatureRowSetup {
                update_type: T::UnsafeDowngrade,
                requested: FeatureLevel(0),
                current: FeatureLevel(1),
                ..Default::default()
            }),
            D::EmitFeature,
        ),
        // A level-0 downgrade of a feature that is not finalized is a
        // no-op that Kafka still writes.
        (
            "delete unfinalized",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                requested: FeatureLevel(0),
                ..Default::default()
            }),
            D::EmitFeature,
        ),
        (
            "unknown type before everything",
            FeatureUpdateFacts {
                update_type: None,
                ..unsupported(row(FeatureRowSetup {
                    requested: FeatureLevel(-1),
                    kind: UNMET,
                    ..Default::default()
                }))
            },
            D::UnknownUpdateType,
        ),
        (
            "negative before support",
            unsupported(row(FeatureRowSetup {
                requested: FeatureLevel(-1),
                ..Default::default()
            })),
            D::NegativeLevel,
        ),
        (
            "support before direction",
            unsupported(row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                ..Default::default()
            })),
            D::UnsupportedByNode,
        ),
        (
            "downgrade as upgrade",
            row(FeatureRowSetup {
                requested: FeatureLevel(0),
                current: FeatureLevel(1),
                ..Default::default()
            }),
            D::DowngradeWithoutFlag,
        ),
        (
            "downgrade type raises",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                ..Default::default()
            }),
            D::DowngradeToNewerLevel,
        ),
        (
            "direction before dependencies",
            row(FeatureRowSetup {
                update_type: T::UnsafeDowngrade,
                kind: UNMET,
                ..Default::default()
            }),
            D::DowngradeToNewerLevel,
        ),
        (
            "dependency unmet",
            row(FeatureRowSetup {
                kind: UNMET,
                ..Default::default()
            }),
            D::DependencyUnmet,
        ),
    ];
    for (case, facts, expected) in cases {
        assert!(feature_update_decision(facts) == expected, "{case}");
    }
}

#[test]
fn metadata_version_refuses_every_lossy_downgrade() {
    let cases = [
        (
            "lossless safe",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                requested: FeatureLevel(24),
                current: FeatureLevel(25),
                kind: LOSSLESS,
            }),
            D::EmitFeature,
        ),
        (
            "lossless unsafe",
            row(FeatureRowSetup {
                update_type: T::UnsafeDowngrade,
                requested: FeatureLevel(24),
                current: FeatureLevel(25),
                kind: LOSSLESS,
            }),
            D::EmitFeature,
        ),
        (
            "lossy safe",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                requested: FeatureLevel(22),
                current: FeatureLevel(25),
                kind: LOSSY,
            }),
            D::LossyMetadataDowngrade,
        ),
        (
            "lossy unsafe",
            row(FeatureRowSetup {
                update_type: T::UnsafeDowngrade,
                requested: FeatureLevel(17),
                current: FeatureLevel(25),
                kind: LOSSY,
            }),
            D::UnsafeMetadataDowngrade,
        ),
        // The walk only matters for a downgrade.
        (
            "upgrade across a change",
            row(FeatureRowSetup {
                requested: FeatureLevel(30),
                current: FeatureLevel(25),
                kind: LOSSY,
                ..Default::default()
            }),
            D::EmitFeature,
        ),
        (
            "upgrade type downgrade",
            row(FeatureRowSetup {
                requested: FeatureLevel(24),
                current: FeatureLevel(25),
                kind: LOSSLESS,
                ..Default::default()
            }),
            D::DowngradeWithoutFlag,
        ),
    ];
    for (case, facts, expected) in cases {
        assert!(feature_update_decision(facts) == expected, "{case}");
    }
}

#[test]
fn kraft_version_upgrades_through_raft_and_never_downgrades() {
    let cases = [
        (
            "upgrade",
            row(FeatureRowSetup {
                kind: FeatureFixtureKind::KRaftVersion,
                ..Default::default()
            }),
            D::UpgradeKRaft,
        ),
        (
            "same level upgrade",
            row(FeatureRowSetup {
                current: FeatureLevel(1),
                kind: FeatureFixtureKind::KRaftVersion,
                ..Default::default()
            }),
            D::UpgradeKRaft,
        ),
        (
            "downgrade",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                requested: FeatureLevel(0),
                current: FeatureLevel(1),
                kind: FeatureFixtureKind::KRaftVersion,
            }),
            D::KRaftDowngrade,
        ),
        (
            "unsafe downgrade",
            row(FeatureRowSetup {
                update_type: T::UnsafeDowngrade,
                requested: FeatureLevel(0),
                current: FeatureLevel(1),
                kind: FeatureFixtureKind::KRaftVersion,
            }),
            D::KRaftDowngrade,
        ),
        (
            "downgrade type at the current level",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                current: FeatureLevel(1),
                kind: FeatureFixtureKind::KRaftVersion,
                ..Default::default()
            }),
            D::NoChange,
        ),
        (
            "downgrade type raises",
            row(FeatureRowSetup {
                update_type: T::SafeDowngrade,
                kind: FeatureFixtureKind::KRaftVersion,
                ..Default::default()
            }),
            D::DowngradeToNewerLevel,
        ),
    ];
    for (case, facts, expected) in cases {
        assert!(feature_update_decision(facts) == expected, "{case}");
    }
}
