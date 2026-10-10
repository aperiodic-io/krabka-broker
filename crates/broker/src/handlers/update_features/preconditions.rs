//! Predicates over the live metadata image that gate a feature update.
//!
//! Each function answers one question about the cluster as the image records
//! it, in the words Kafka's controller answers it: whether every node supports
//! a level (`FeatureControlManager.reasonNotSupported`) and whether the
//! KIP-1022 dependencies of a level hold (`Feature.validateVersion`).

use std::collections::BTreeMap;

/// `MetadataVersion.isControllerRegistrationSupported`: controllers
/// register from `3.7-IV0`.
const CONTROLLER_REGISTRATION_MIN_LEVEL: i16 =
    krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL;

/// Kafka's `QuorumFeatures.DISABLED`: what a node that did not register a
/// feature supports.
const DISABLED: (i16, i16) = (0, 0);

/// `QuorumFeatures.reasonNotSupported`, with `VersionRange.toString`.
fn reason_range_not_supported(level: i16, what: &str, (min, max): (i16, i16)) -> Option<String> {
    if (min..=max).contains(&level) {
        None
    } else if max == 0 {
        Some(format!("{what} does not support this feature."))
    } else if min == max {
        Some(format!("{what} only supports versions {min}"))
    } else {
        Some(format!("{what} only supports versions {min}-{max}"))
    }
}

/// The controller answering an `UpdateFeatures`: its node id, which Kafka
/// names as `Local controller <id>`, and the
/// `unstable.feature.versions.enable` its own supported ranges follow
/// (`QuorumFeatures.defaultSupportedFeatureMap`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalController {
    pub node_id: krabka_metadata::NodeId,
    pub unstable_features: krabka_raft::UnstableFeatureVersions,
}

/// `FeatureControlManager.reasonNotSupported`: why some node cannot take
/// `feature` to `level`, or `None` when every node can.
///
/// The local controller answers first from this binary's feature registry,
/// then every registered broker. From `3.7-IV0`, when controllers register,
/// every other registered controller answers too, and a quorum voter that has
/// not registered blocks the update.
pub(super) fn reason_not_supported(
    image: &krabka_metadata::MetadataImage,
    local: LocalController,
    feature: &str,
    level: i16,
) -> Option<String> {
    let local_controller = local.node_id;
    let local = krabka_metadata::feature(feature).map_or(DISABLED, |feature| {
        krabka_raft::supported_feature_range(feature, local.unstable_features)
    });
    let range = |features: &BTreeMap<String, (i16, i16)>| {
        features.get(feature).copied().unwrap_or(DISABLED)
    };
    reason_range_not_supported(
        level,
        &format!("Local controller {local_controller}"),
        local,
    )
    .or_else(|| {
        image.brokers().find_map(|broker| {
            reason_range_not_supported(
                level,
                &format!("Broker {}", broker.node_id),
                range(&broker.features),
            )
        })
    })
    .or_else(|| {
        if image
            .finalized_metadata_version()
            .is_none_or(|mv| mv < CONTROLLER_REGISTRATION_MIN_LEVEL)
        {
            return None;
        }
        image
            .controllers()
            .filter(|controller| controller.node_id != local_controller)
            .find_map(|controller| {
                reason_range_not_supported(
                    level,
                    &format!("Controller {}", controller.node_id),
                    range(&controller.features),
                )
            })
            .or_else(|| {
                image
                    .voters()
                    .iter()
                    .map(|voter| voter.id)
                    .find(|&id| id != local_controller && image.controller(id).is_none())
                    .map(|id| {
                        format!(
                            "controller {id} has not registered, and may not support this \
                                 feature"
                        )
                    })
            })
    })
}

/// The dependency half of `Feature.validateVersion`: why `feature` cannot be
/// set to `level` given `proposed`, the levels the request would leave, or
/// `None` when every dependency holds. `deps` is the feature's
/// `dependencies(level)`.
pub(super) fn dependency_error(
    feature: &str,
    level: i16,
    deps: &[(&str, i16)],
    proposed: &BTreeMap<String, i16>,
) -> Option<String> {
    deps.iter().find_map(|&(dep, min_level)| {
        proposed
            .get(dep)
            .is_none_or(|&have| have < min_level)
            .then(|| {
                format!(
                    "{feature} could not be set to {level} because it depends on {dep} level \
                     {min_level}"
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{
        BrokerRegistrationRecord, ControllerRegistrationRecord, FeatureLevelRecord, MetadataImage,
        MetadataRecord, NodeId,
    };

    use super::*;

    const LOCAL: LocalController = LocalController {
        node_id: NodeId(1),
        unstable_features: krabka_raft::UnstableFeatureVersions::Disabled,
    };

    #[test]
    fn range_reasons_use_kafka_version_range_text() {
        let cases = [
            (1, (0, 1), None),
            (9, (0, 1), Some("Broker 2 only supports versions 0-1")),
            (2, (1, 1), Some("Broker 2 only supports versions 1")),
            (1, DISABLED, Some("Broker 2 does not support this feature.")),
            (0, DISABLED, None),
        ];
        for (level, range, want) in cases {
            assert!(
                reason_range_not_supported(level, "Broker 2", range).as_deref() == want,
                "{level} {range:?}"
            );
        }
    }

    /// KIP-1191: `share.version` 2 is Kafka trunk's `SV_2`, so a controller
    /// supports it only with `unstable.feature.versions.enable`, and answers
    /// as a 4.3.1 controller does without it.
    #[test]
    fn share_version_two_needs_unstable_feature_versions() {
        let local = |unstable| LocalController {
            node_id: NodeId(1),
            unstable_features: unstable,
        };
        let image = MetadataImage::new(uuid::Uuid::nil());
        let rows = [
            (1, krabka_raft::UnstableFeatureVersions::Disabled, None),
            (
                2,
                krabka_raft::UnstableFeatureVersions::Disabled,
                Some("Local controller 1 only supports versions 0-1"),
            ),
            (2, krabka_raft::UnstableFeatureVersions::Enabled, None),
            (
                3,
                krabka_raft::UnstableFeatureVersions::Enabled,
                Some("Local controller 1 only supports versions 0-2"),
            ),
        ];
        for (level, unstable, want) in rows {
            assert!(
                reason_not_supported(&image, local(unstable), "share.version", level).as_deref()
                    == want,
                "{level} {unstable:?}"
            );
        }
    }

    fn image(metadata_version: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: metadata_version,
        }));
        image
    }

    fn broker(node_id: u64, features: BTreeMap<String, (i16, i16)>) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            host: String::new(),
            port: 0,
            features,
            ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
        })
    }

    fn controller(node_id: u64, features: BTreeMap<String, (i16, i16)>) -> MetadataRecord {
        MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
            node_id: NodeId(node_id),
            incarnation_id: uuid::Uuid::nil(),
            zk_migration_ready: false,
            endpoints: vec![],
            features,
        })
    }

    fn voters(ids: &[u64]) -> MetadataRecord {
        MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
            voters: krabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
                krabka_metadata::voters::Voter {
                    id: NodeId(id),
                    directory_id: uuid::Uuid::from_u128(u128::from(id)),
                    endpoints: vec![],
                    kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
                }
            })),
        })
    }

    #[test]
    fn nodes_answer_in_kafka_order() {
        let all = krabka_metadata::supported_feature_ranges();
        let mut no_group = all.clone();
        no_group.remove("group.version");
        let cases = [
            (
                "every node supports",
                30,
                vec![broker(2, all.clone())],
                "group.version",
                1,
                None,
            ),
            (
                "unknown feature, local first",
                30,
                vec![broker(2, no_group.clone())],
                "no.such.feature",
                1,
                Some("Local controller 1 does not support this feature."),
            ),
            (
                "local range",
                30,
                vec![],
                "group.version",
                9,
                Some("Local controller 1 only supports versions 0-1"),
            ),
            (
                "broker without the feature",
                30,
                vec![broker(2, no_group.clone())],
                "group.version",
                1,
                Some("Broker 2 does not support this feature."),
            ),
            (
                "other controller without the feature",
                30,
                vec![controller(3, no_group.clone())],
                "group.version",
                1,
                Some("Controller 3 does not support this feature."),
            ),
            (
                "the local controller's registration is not read",
                30,
                vec![controller(1, no_group.clone())],
                "group.version",
                1,
                None,
            ),
            (
                "controllers do not answer before 3.7-IV0",
                14,
                vec![controller(3, no_group.clone()), voters(&[1, 4])],
                "group.version",
                1,
                None,
            ),
            (
                "unregistered voter",
                30,
                vec![controller(3, all.clone()), voters(&[1, 3, 4])],
                "group.version",
                1,
                Some("controller 4 has not registered, and may not support this feature"),
            ),
        ];
        for (case, metadata_version, records, feature, level, want) in cases {
            let mut image = image(metadata_version);
            for record in &records {
                image.apply(record);
            }
            assert!(
                reason_not_supported(&image, LOCAL, feature, level).as_deref() == want,
                "{case}"
            );
        }
    }

    #[test]
    fn dependencies_read_the_proposed_levels() {
        let deps: &[(&str, i16)] = &[("metadata.version", 23)];
        let proposed = |mv: Option<i16>| -> BTreeMap<String, i16> {
            mv.map(|level| ("metadata.version".to_string(), level))
                .into_iter()
                .collect()
        };
        let want = Some(
            "eligible.leader.replicas.version could not be set to 1 because it depends on \
             metadata.version level 23"
                .to_string(),
        );
        for (mv, expected) in [
            (None, want.clone()),
            (Some(22), want),
            (Some(23), None),
            (Some(30), None),
        ] {
            assert!(
                dependency_error("eligible.leader.replicas.version", 1, deps, &proposed(mv))
                    == expected,
                "{mv:?}"
            );
        }
        assert!(dependency_error("group.version", 1, &[], &proposed(None)).is_none());
    }
}
