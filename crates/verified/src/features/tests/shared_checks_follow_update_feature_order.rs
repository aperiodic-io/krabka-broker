use {super::*, assert2::assert};

#[test]
fn shared_checks_follow_update_feature_order() {
    let unsupported = |facts: FeatureUpdateFacts| FeatureUpdateFacts {
        all_nodes_support: false,
        ..facts
    };
    let cases = [
        ("upgrade", row().call(), D::EmitFeature),
        ("same level", row().current(1).call(), D::EmitFeature),
        (
            "safe downgrade",
            row()
                .update_type(T::SafeDowngrade)
                .requested(0)
                .current(1)
                .call(),
            D::EmitFeature,
        ),
        (
            "unsafe downgrade",
            row()
                .update_type(T::UnsafeDowngrade)
                .requested(0)
                .current(1)
                .call(),
            D::EmitFeature,
        ),
        // A level-0 downgrade of a feature that is not finalized is a
        // no-op that Kafka still writes.
        (
            "delete unfinalized",
            row().update_type(T::SafeDowngrade).requested(0).call(),
            D::EmitFeature,
        ),
        (
            "unknown type before everything",
            FeatureUpdateFacts {
                update_type: None,
                ..unsupported(row().requested(-1).kind(UNMET).call())
            },
            D::UnknownUpdateType,
        ),
        (
            "negative before support",
            unsupported(row().requested(-1).call()),
            D::NegativeLevel,
        ),
        (
            "support before direction",
            unsupported(row().update_type(T::SafeDowngrade).call()),
            D::UnsupportedByNode,
        ),
        (
            "downgrade as upgrade",
            row().requested(0).current(1).call(),
            D::DowngradeWithoutFlag,
        ),
        (
            "downgrade type raises",
            row().update_type(T::SafeDowngrade).call(),
            D::DowngradeToNewerLevel,
        ),
        (
            "direction before dependencies",
            row().update_type(T::UnsafeDowngrade).kind(UNMET).call(),
            D::DowngradeToNewerLevel,
        ),
        (
            "dependency unmet",
            row().kind(UNMET).call(),
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
            row()
                .update_type(T::SafeDowngrade)
                .requested(24)
                .current(25)
                .kind(LOSSLESS)
                .call(),
            D::EmitFeature,
        ),
        (
            "lossless unsafe",
            row()
                .update_type(T::UnsafeDowngrade)
                .requested(24)
                .current(25)
                .kind(LOSSLESS)
                .call(),
            D::EmitFeature,
        ),
        (
            "lossy safe",
            row()
                .update_type(T::SafeDowngrade)
                .requested(22)
                .current(25)
                .kind(LOSSY)
                .call(),
            D::LossyMetadataDowngrade,
        ),
        (
            "lossy unsafe",
            row()
                .update_type(T::UnsafeDowngrade)
                .requested(17)
                .current(25)
                .kind(LOSSY)
                .call(),
            D::UnsafeMetadataDowngrade,
        ),
        // The walk only matters for a downgrade.
        (
            "upgrade across a change",
            row().requested(30).current(25).kind(LOSSY).call(),
            D::EmitFeature,
        ),
        (
            "upgrade type downgrade",
            row().requested(24).current(25).kind(LOSSLESS).call(),
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
            row().kind(FeatureFixtureKind::KRaftVersion).call(),
            D::UpgradeKRaft,
        ),
        (
            "same level upgrade",
            row()
                .current(1)
                .kind(FeatureFixtureKind::KRaftVersion)
                .call(),
            D::UpgradeKRaft,
        ),
        (
            "downgrade",
            row()
                .update_type(T::SafeDowngrade)
                .requested(0)
                .current(1)
                .kind(FeatureFixtureKind::KRaftVersion)
                .call(),
            D::KRaftDowngrade,
        ),
        (
            "unsafe downgrade",
            row()
                .update_type(T::UnsafeDowngrade)
                .requested(0)
                .current(1)
                .kind(FeatureFixtureKind::KRaftVersion)
                .call(),
            D::KRaftDowngrade,
        ),
        (
            "downgrade type at the current level",
            row()
                .update_type(T::SafeDowngrade)
                .current(1)
                .kind(FeatureFixtureKind::KRaftVersion)
                .call(),
            D::NoChange,
        ),
        (
            "downgrade type raises",
            row()
                .update_type(T::SafeDowngrade)
                .kind(FeatureFixtureKind::KRaftVersion)
                .call(),
            D::DowngradeToNewerLevel,
        ),
    ];
    for (case, facts, expected) in cases {
        assert!(feature_update_decision(facts) == expected, "{case}");
    }
}
