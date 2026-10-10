//! Tests for the request planner, kept in their own file because they build
//! whole metadata images and outweigh the module they cover.

use assert2::assert;
use krabka_metadata::NodeId;
use krabka_raft::UnstableFeatureVersions;

use super::*;
use crate::handlers::update_features::test_support::{
    elr_update, metadata_update, named_update, validate_only,
};

/// The local controller at the defaults: `unstable.feature.versions.enable`
/// off, as a stock Kafka 4.3.1 controller runs.
const LOCAL: LocalController = LocalController {
    node_id: NodeId(1),
    unstable_features: UnstableFeatureVersions::Disabled,
};
const UPGRADE: i8 = 1;
const SAFE: i8 = 2;
const UNSAFE: i8 = 3;

/// The advertised feature ranges are wire data, including unsupported levels.
struct RegisteredFeatureRanges(std::collections::BTreeMap<String, (i16, i16)>);

impl Default for RegisteredFeatureRanges {
    fn default() -> Self {
        Self(krabka_metadata::supported_feature_ranges())
    }
}

#[derive(krabka_macros::FieldDefaults)]
struct RemoteFeatureSetup {
    #[default(NodeId(2))]
    node_id: NodeId,
    ranges: RegisteredFeatureRanges,
}

fn remote_feature_image(
    mut image: krabka_metadata::MetadataImage,
    setup: RemoteFeatureSetup,
) -> krabka_metadata::MetadataImage {
    image.apply(&MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            host: String::new(),
            port: 0,
            features: setup.ranges.0,
            ..crate::test_support::broker_registration(setup.node_id)
        },
    ));
    image
}

fn feature_record(name: &str, level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: name.into(),
        level,
    })
}

/// An image finalized at `levels`.
fn image(levels: &[(&str, i16)]) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for &(name, level) in levels {
        image.apply(&feature_record(name, level));
    }
    image
}

fn invalid(feature: &str, level: i16, reason: &str) -> UpdateError {
    UpdateError {
        code: codes::INVALID_UPDATE_VERSION,
        message: format!("Invalid update version {level} for feature {feature}. {reason}"),
    }
}

fn plan(records: Vec<MetadataRecord>, features: &[&str]) -> UpdatePlan {
    UpdatePlan {
        records,
        features: features.iter().map(ToString::to_string).collect(),
        ..UpdatePlan::default()
    }
}

/// The row-validation cases of krabka-io/krabka-broker#780, each against
/// Kafka 4.3.1's `FeatureControlManager.updateFeature`.
#[test]
fn rows_validate_as_kafkas_update_feature() {
    let mv = "metadata.version";
    let group = "group.version";
    let share = "share.version";
    let elr = "eligible.leader.replicas.version";
    let cases = [
        (
            "empty request",
            image(&[(mv, 25)]),
            vec![],
            Ok(plan(vec![], &[])),
        ),
        (
            "a repeated name keeps its last row",
            image(&[(mv, 25)]),
            vec![
                named_update(group, 9, UPGRADE),
                named_update(group, 1, UPGRADE),
            ],
            Ok(plan(vec![feature_record(group, 1)], &[group])),
        ),
        (
            "unknown feature above level 0",
            image(&[(mv, 25)]),
            vec![named_update("no.such.feature", 1, UPGRADE)],
            Err(invalid(
                "no.such.feature",
                1,
                "Local controller 1 does not support this feature.",
            )),
        ),
        (
            "unknown feature at level 0",
            image(&[(mv, 25)]),
            vec![named_update("no.such.feature", 0, SAFE)],
            Err(invalid(
                "no.such.feature",
                0,
                "Feature no.such.feature not found.",
            )),
        ),
        (
            "level 0 downgrade of a feature that is not finalized",
            image(&[(mv, 25)]),
            vec![named_update(share, 0, SAFE)],
            Ok(plan(vec![feature_record(share, 0)], &[share])),
        ),
        (
            "an unknown upgrade type",
            image(&[(mv, 25)]),
            vec![named_update(group, 1, 0)],
            Err(invalid(
                group,
                1,
                "The controller does not support the given upgrade type.",
            )),
        ),
        (
            "a negative level",
            image(&[(mv, 25)]),
            vec![named_update(group, -1, UPGRADE)],
            Err(invalid(
                group,
                -1,
                "A feature version cannot be less than 0.",
            )),
        ),
        (
            "a level above the local range",
            image(&[(mv, 25)]),
            vec![named_update(share, 9, UPGRADE)],
            Err(invalid(
                share,
                9,
                "Local controller 1 only supports versions 0-1",
            )),
        ),
        (
            "a downgrade without a downgrade type",
            image(&[(mv, 25), (group, 1)]),
            vec![named_update(group, 0, UPGRADE)],
            Err(invalid(
                group,
                0,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            )),
        ),
        (
            "a downgrade type that raises the level",
            image(&[(mv, 25)]),
            vec![named_update(group, 1, SAFE)],
            Err(invalid(group, 1, "Can't downgrade to a newer version.")),
        ),
        (
            "dependencies read the levels the request proposes",
            image(&[(mv, 22)]),
            vec![metadata_update(23, UPGRADE), elr_update(1, UPGRADE)],
            Ok(UpdatePlan {
                enables_elr: true,
                ..plan(
                    vec![feature_record(mv, 23), feature_record(elr, 1)],
                    &[mv, elr],
                )
            }),
        ),
        (
            "an unmet dependency",
            image(&[(mv, 22)]),
            vec![elr_update(1, UPGRADE)],
            Err(invalid(
                elr,
                1,
                "eligible.leader.replicas.version could not be set to 1 because it depends on \
                 metadata.version level 23",
            )),
        ),
    ];
    for (case, image, updates, expected) in cases {
        let actual = plan_updates(&validate_only(updates), &image, LOCAL);
        assert!(actual == expected, "{case}");
    }
}

/// A version 0 row carries `AllowDowngrade` rather than `UpgradeType`, and
/// Kafka reads the `UpgradeType` default, UPGRADE, so the flag never
/// authorizes a downgrade.
/// #784: a Kafka trunk `metadata.version` (31-33) is past what the local
/// controller supports unless `unstable.feature.versions.enable` is set, and
/// is refused with `QuorumFeatures.reasonNotSupported`'s text, as a 4.3.1
/// controller refuses it. With the flag set, trunk's levels are accepted.
#[test]
fn an_unstable_metadata_version_needs_unstable_feature_versions() {
    let mv = "metadata.version";
    let trunk = LocalController {
        unstable_features: UnstableFeatureVersions::Enabled,
        ..LOCAL
    };
    let refused = |level| {
        Err(invalid(
            mv,
            level,
            "Local controller 1 only supports versions 7-30",
        ))
    };
    let accepted = |level| Ok(plan(vec![feature_record(mv, level)], &[mv]));
    for (local, level, expected) in [
        (LOCAL, 30, accepted(30)),
        (LOCAL, 31, refused(31)),
        (LOCAL, 33, refused(33)),
        (trunk, 31, accepted(31)),
        (trunk, 33, accepted(33)),
    ] {
        let image = image(&[(mv, 30)]);
        let request = validate_only(vec![metadata_update(level, UPGRADE)]);
        assert!(
            plan_updates(&request, &image, local) == expected,
            "{:?} level {level}",
            local.unstable_features
        );
    }
}

#[test]
fn version_zero_allow_downgrade_is_read_as_upgrade() {
    let request = UpdateFeaturesRequest {
        feature_updates: vec![FeatureUpdateKey {
            feature: "group.version".into(),
            max_version_level: 0,
            allow_downgrade: true,
            ..Default::default()
        }],
        ..Default::default()
    };
    let image = image(&[("metadata.version", 25), ("group.version", 1)]);
    assert!(
        plan_updates(&request, &image, LOCAL)
            == Err(invalid(
                "group.version",
                0,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            ))
    );
}

/// The first failing entry in Kafka's `HashMap` order names the error, and
/// the valid rows ahead of it write nothing.
#[test]
fn the_first_failure_in_map_order_fails_the_whole_request() {
    let image = image(&[("metadata.version", 25)]);
    let request = validate_only(vec![
        named_update("share.version", 9, UPGRADE),
        named_update("group.version", 9, UPGRADE),
    ]);
    assert!(
        plan_updates(&request, &image, LOCAL)
            == Err(invalid(
                "group.version",
                9,
                "Local controller 1 only supports versions 0-1",
            ))
    );
}

/// krabka-io/krabka-broker#781: Kafka's `didMetadataChange` walk decides a
/// `metadata.version` downgrade, whatever the downgrade type, and an accepted
/// downgrade writes only its feature record.
#[test]
fn metadata_version_downgrades_follow_did_metadata_change() {
    let refused = |current: i16, target: i16, reason: &str| {
        Err(UpdateError {
            code: codes::INVALID_UPDATE_VERSION,
            message: format!(
                "Unsupported metadata.version downgrade from {current} to {target}. {reason}"
            ),
        })
    };
    let lossy = "Refusing to perform the requested downgrade because it might delete metadata \
                 information.";
    let unsafe_reason = "Unsafe metadata downgrade is not supported in this version.";
    let accepted = |target: i16| {
        Ok(plan(
            vec![feature_record("metadata.version", target)],
            &["metadata.version"],
        ))
    };
    let cases = [
        (25, 24, SAFE, accepted(24)),
        (25, 24, UNSAFE, accepted(24)),
        (25, 22, SAFE, refused(25, 22, lossy)),
        (25, 17, UNSAFE, refused(25, 17, unsafe_reason)),
        (17, 15, UNSAFE, refused(17, 15, unsafe_reason)),
        // 3.6-IV1 (13) changed metadata, so leaving it is lossy; 3.6-IV0
        // (12) did not.
        (13, 12, SAFE, refused(13, 12, lossy)),
        (12, 11, SAFE, accepted(11)),
        (29, 24, SAFE, accepted(24)),
        (30, 29, SAFE, refused(30, 29, lossy)),
        (
            25,
            24,
            UPGRADE,
            Err(invalid(
                "metadata.version",
                24,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            )),
        ),
        (
            25,
            6,
            SAFE,
            Err(invalid(
                "metadata.version",
                6,
                "Local controller 1 only supports versions 7-30",
            )),
        ),
    ];
    for (current, target, upgrade_type, expected) in cases {
        let image = image(&[("metadata.version", current)]);
        let request = validate_only(vec![metadata_update(target, upgrade_type)]);
        assert!(
            plan_updates(&request, &image, LOCAL) == expected,
            "{current} -> {target} type {upgrade_type}"
        );
    }
}

#[test]
fn kraft_version_rows_follow_kafka() {
    let kraft = "kraft.version";
    let cases = [
        (
            "upgrade",
            vec![named_update(kraft, 1, UPGRADE)],
            Ok(UpdatePlan {
                kraft_upgrade: Some(1),
                ..plan(vec![], &[kraft])
            }),
        ),
        (
            "upgrade to the current level",
            vec![named_update(kraft, 0, UPGRADE)],
            Ok(plan(vec![], &[kraft])),
        ),
        (
            "a downgrade type that raises the level",
            vec![named_update(kraft, 1, SAFE)],
            Err(invalid(kraft, 1, "Can't downgrade to a newer version.")),
        ),
        (
            "above the supported range",
            vec![named_update(kraft, 2, UPGRADE)],
            Err(invalid(
                kraft,
                2,
                "Local controller 1 only supports versions 0-1",
            )),
        ),
    ];
    for (case, updates, expected) in cases {
        let image = image(&[("metadata.version", 25)]);
        assert!(
            plan_updates(&validate_only(updates), &image, LOCAL) == expected,
            "{case}"
        );
    }
}

/// An image at 4.3-IV0, with `eligible.leader.replicas.version` finalized at
/// `elr_level` when one is given and a published ELR on topic `orders` when
/// `published` is.
fn elr_image(elr_level: Option<i16>, published: bool) -> krabka_metadata::MetadataImage {
    let mut image = image(&[("metadata.version", 30)]);
    if let Some(level) = elr_level {
        image.apply(&feature_record(crate::features::ELR_VERSION, level));
    }
    if published {
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "orders".into(),
                partition: 0,
                ..Default::default()
            },
        ));
        image.apply(&MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "orders".into(),
                partition: 0,
                eligible_leader_replicas: vec![NodeId(2), NodeId(3)],
                last_known_elr: vec![],
            },
        ));
    }
    image
}

/// KIP-966: finalizing the feature back to 0 clears the memberships it
/// published, ahead of the feature record; a cluster that never published
/// any writes the feature record alone.
#[test]
fn an_elr_downgrade_clears_the_published_state_before_the_feature_record() {
    let elr = crate::features::ELR_VERSION;
    let clear = MetadataRecord::V1PartitionElr(krabka_metadata::PartitionElrRecord {
        topic: "orders".into(),
        partition: 0,
        eligible_leader_replicas: vec![],
        last_known_elr: vec![],
    });
    for (published, records) in [
        (true, vec![clear, feature_record(elr, 0)]),
        (false, vec![feature_record(elr, 0)]),
    ] {
        let image = elr_image(Some(1), published);
        let request = validate_only(vec![elr_update(0, SAFE)]);
        assert!(
            plan_updates(&request, &image, LOCAL) == Ok(plan(records, &[elr])),
            "{published}"
        );
    }
}

/// A broker that did not register a feature supports only level 0, so it
/// blocks turning the feature on but not off.
#[test]
fn a_feature_unaware_broker_blocks_only_enabling() {
    let elr = crate::features::ELR_VERSION;
    let unaware = |elr_level| {
        let image = elr_image(elr_level, false);
        let mut features = krabka_metadata::supported_feature_ranges();
        features.remove(elr);
        remote_feature_image(
            image,
            RemoteFeatureSetup {
                ranges: RegisteredFeatureRanges(features),
                ..Default::default()
            },
        )
    };
    let cases = [
        (
            "enabling",
            unaware(None),
            elr_update(1, UPGRADE),
            Err(invalid(elr, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "disabling",
            unaware(Some(1)),
            elr_update(0, SAFE),
            Ok(plan(vec![feature_record(elr, 0)], &[elr])),
        ),
    ];
    for (case, image, update, expected) in cases {
        assert!(
            plan_updates(&validate_only(vec![update]), &image, LOCAL) == expected,
            "{case}"
        );
    }
}

/// Kafka's `maybeGenerateElrSafetyRecords`, over the starting broker configs:
/// the cluster-level `min.insync.replicas` is kept or set to the static
/// value, and a broker-level one is removed, whether or not that broker is
/// still registered: Kafka walks every broker config resource
/// (`brokersWithConfigs`), not the registered brokers.
#[test]
fn enabling_elr_writes_kafkas_safety_config_records() {
    let key = crate::config_keys::MIN_INSYNC_REPLICAS;
    let cluster = krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID;
    let node = NodeId(1);
    let unregistered = NodeId(7);
    let config = |node_id, value: Option<&str>| {
        MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
            node_id,
            config_name: key.into(),
            config_value: value.map(str::to_owned),
        })
    };
    let cases = [
        (None, node, None, 2, vec![config(cluster, Some("2"))]),
        (Some("3"), node, None, 2, vec![]),
        (
            None,
            node,
            Some("2"),
            1,
            vec![config(cluster, Some("1")), config(node, None)],
        ),
        (Some("3"), node, Some("2"), 1, vec![config(node, None)]),
        (
            Some("3"),
            unregistered,
            Some("2"),
            1,
            vec![config(unregistered, None)],
        ),
    ];
    for (cluster_value, broker, broker_value, static_value, want) in cases {
        let mut image = elr_image(None, false);
        if let Some(value) = cluster_value {
            image.apply(&config(cluster, Some(value)));
        }
        if let Some(value) = broker_value {
            image.apply(&config(broker, Some(value)));
        }
        assert!(
            elr_safety_records(&image, static_value) == want,
            "{cluster_value:?} {broker:?} {broker_value:?} {static_value}"
        );
    }
}

/// `krabka.version` finalizes as Kafka 4.3.1's `FeatureControlManager`
/// finalizes a feature other than `metadata.version` and `kraft.version`,
/// `transaction.version` for one: a level every node supports writes a
/// `FeatureLevelRecord`; a level the local controller or a registered broker
/// does not support is refused; a downgrade needs `SAFE_DOWNGRADE` or
/// `UNSAFE_DOWNGRADE`, and with either it is written, since no
/// `metadata changed` check applies outside `metadata.version`.
#[test]
fn krabka_version_finalizes_as_kafka_finalizes_a_non_metadata_feature() {
    let kv = krabka_metadata::krabka_version::KRABKA_VERSION_FEATURE;
    let mv = "metadata.version";
    let with_broker = |levels: &[(&str, i16)], range: Option<(i16, i16)>| {
        let image = image(levels);
        let mut features = krabka_metadata::supported_feature_ranges();
        match range {
            Some(range) => features.insert(kv.to_owned(), range),
            None => features.remove(kv),
        };
        remote_feature_image(
            image,
            RemoteFeatureSetup {
                ranges: RegisteredFeatureRanges(features),
                ..Default::default()
            },
        )
    };
    let cases = [
        (
            "finalize level 1 on an unfinalized cluster",
            image(&[(mv, 30)]),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "finalize level 1 with a broker that supports it",
            with_broker(&[(mv, 30)], Some((0, 1))),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "a broker that advertises a narrower range",
            with_broker(&[(mv, 30)], Some((0, 0))),
            named_update(kv, 1, UPGRADE),
            Err(invalid(kv, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "a broker that does not register the feature",
            with_broker(&[(mv, 30)], None),
            named_update(kv, 1, UPGRADE),
            Err(invalid(kv, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "a level above the local controller's range",
            image(&[(mv, 30)]),
            named_update(kv, 2, UPGRADE),
            Err(invalid(
                kv,
                2,
                "Local controller 1 only supports versions 0-1",
            )),
        ),
        (
            "re-finalizing the current level",
            image(&[(mv, 30), (kv, 1)]),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "a downgrade without a downgrade type",
            image(&[(mv, 30), (kv, 1)]),
            named_update(kv, 0, UPGRADE),
            Err(invalid(
                kv,
                0,
                "Can't downgrade the version of this feature without setting the upgrade type \
                 to either safe or unsafe downgrade.",
            )),
        ),
        (
            "a safe downgrade",
            image(&[(mv, 30), (kv, 1)]),
            named_update(kv, 0, SAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "an unsafe downgrade",
            image(&[(mv, 30), (kv, 1)]),
            named_update(kv, 0, UNSAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "a downgrade a narrower broker still supports",
            with_broker(&[(mv, 30), (kv, 1)], Some((0, 0))),
            named_update(kv, 0, SAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "a downgrade type that raises the level",
            image(&[(mv, 30)]),
            named_update(kv, 1, SAFE),
            Err(invalid(kv, 1, "Can't downgrade to a newer version.")),
        ),
    ];
    let actual: Vec<_> = cases
        .iter()
        .map(|(case, image, update, _)| {
            (
                *case,
                plan_updates(&validate_only(vec![update.clone()]), image, LOCAL),
            )
        })
        .collect();
    let expected: Vec<_> = cases
        .into_iter()
        .map(|(case, _, _, want)| (case, want))
        .collect();
    assert!(actual == expected);
}
