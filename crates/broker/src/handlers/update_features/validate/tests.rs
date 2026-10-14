//! Tests for the request planner, kept in their own file because they build
//! whole metadata images and outweigh the module they cover.

use {
    super::*,
    crate::{
        config_keys::MIN_INSYNC_REPLICAS,
        features::ELR_VERSION,
        handlers::update_features::test_support::{
            elr_update, metadata_update, named_update, validate_only,
        },
        test_support::broker_registration,
    },
    assert2::assert,
    krabka_metadata::NodeId,
    krabka_raft::UnstableFeatureVersions,
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

#[derive(derive_more::From, derive_more::Into)]
/// The advertised feature ranges are wire data, including unsupported levels.
struct RegisteredFeatureRanges(std::collections::BTreeMap<String, (i16, i16)>);

impl Default for RegisteredFeatureRanges {
    fn default() -> Self { Self(krabka_metadata::supported_feature_ranges()) }
}

#[derive(krabka_macros::FieldDefaults)]
struct RemoteFeatureSetup {
    #[default(NodeId(2))]
    node_id: NodeId,
    ranges: RegisteredFeatureRanges,
}

#[bon::builder]
fn remote_feature_image(
    mut image: krabka_metadata::MetadataImage,
    #[builder(into, default = NodeId (2))] node_id: NodeId,
    #[builder(into, default)] ranges: RegisteredFeatureRanges,
) -> krabka_metadata::MetadataImage {
    let setup = RemoteFeatureSetup { node_id, ranges };
    image.apply(&MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            host: String::new(),
            port: 0,
            features: setup.ranges.0,
            ..broker_registration(setup.node_id)
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

/// A feature and the level it is finalized at.
#[derive(Clone, Copy)]
struct FeatureLevel<'a> {
    name: &'a str,
    level: i16,
}

/// An image finalized at `levels`.
fn image(levels: &[FeatureLevel<'_>]) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for &FeatureLevel { name, level } in levels {
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
#[yare::parameterized(
    empty_request = {
        "empty request",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![],
        Ok(plan(vec![], &[])),
    },
    repeated_name_keeps_last_row = {
        "a repeated name keeps its last row",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("group.version", 9, UPGRADE), named_update("group.version", 1, UPGRADE)],
        Ok(plan(vec![feature_record("group.version", 1)], &["group.version"])),
    },
    unknown_feature_above_zero = {
        "unknown feature above level 0",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("no.such.feature", 1, UPGRADE)],
        Err(invalid("no.such.feature", 1, "Local controller 1 does not support this feature.")),
    },
    unknown_feature_at_zero = {
        "unknown feature at level 0",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("no.such.feature", 0, SAFE)],
        Err(invalid("no.such.feature", 0, "Feature no.such.feature not found.")),
    },
    level_zero_downgrade_of_unfinalized_feature = {
        "level 0 downgrade of a feature that is not finalized",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("share.version", 0, SAFE)],
        Ok(plan(vec![feature_record("share.version", 0)], &["share.version"])),
    },
    unknown_upgrade_type = {
        "an unknown upgrade type",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("group.version", 1, 0)],
        Err(invalid("group.version", 1, "The controller does not support the given upgrade type.")),
    },
    negative_level = {
        "a negative level",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("group.version", -1, UPGRADE)],
        Err(invalid("group.version", -1, "A feature version cannot be less than 0.")),
    },
    above_local_range = {
        "a level above the local range",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("share.version", 9, UPGRADE)],
        Err(invalid("share.version", 9, "Local controller 1 only supports versions 0-1")),
    },
    downgrade_without_downgrade_type = {
        "a downgrade without a downgrade type",
        image(&[
            FeatureLevel {
                name: "metadata.version",
                level: 25,
            },
            FeatureLevel {
                name: "group.version",
                level: 1,
            },
        ]),
        vec![named_update("group.version", 0, UPGRADE)],
        Err(invalid(
            "group.version",
            0,
            "Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.",
        )),
    },
    downgrade_type_raises_level = {
        "a downgrade type that raises the level",
        image(&[FeatureLevel { name: "metadata.version", level: 25 }]),
        vec![named_update("group.version", 1, SAFE)],
        Err(invalid("group.version", 1, "Can't downgrade to a newer version.")),
    },
    dependency_uses_proposed_levels = {
        "dependencies read the levels the request proposes",
        image(&[FeatureLevel { name: "metadata.version", level: 22 }]),
        vec![metadata_update(23, UPGRADE), elr_update(1, UPGRADE)],
        Ok(UpdatePlan {
            enables_elr: true,
            ..plan(
                vec![
                    feature_record("metadata.version", 23),
                    feature_record("eligible.leader.replicas.version", 1),
                ],
                &["metadata.version", "eligible.leader.replicas.version"],
            )
        }),
    },
    unmet_dependency = {
        "an unmet dependency",
        image(&[FeatureLevel { name: "metadata.version", level: 22 }]),
        vec![elr_update(1, UPGRADE)],
        Err(invalid(
            "eligible.leader.replicas.version",
            1,
            "eligible.leader.replicas.version could not be set to 1 because it depends on metadata.version level 23",
        )),
    },
)]
fn rows_validate_as_kafkas_update_feature(
    case: &str,
    image: krabka_metadata::MetadataImage,
    updates: Vec<FeatureUpdateKey>,
    expected: Result<UpdatePlan, UpdateError>,
) {
    let actual = plan_updates(&validate_only(updates), &image, LOCAL);
    assert2::assert!(actual == expected, "{case}");
}

/// A version 0 row carries `AllowDowngrade` rather than `UpgradeType`, and
/// Kafka reads the `UpgradeType` default, UPGRADE, so the flag never
/// authorizes a downgrade.
/// #784: a Kafka trunk `metadata.version` (31-33) is past what the local
/// controller supports unless `unstable.feature.versions.enable` is set, and
/// is refused with `QuorumFeatures.reasonNotSupported`'s text, as a 4.3.1
/// controller refuses it. With the flag set, trunk's levels are accepted.
#[yare::parameterized(
    latest_stable_level = {
        LOCAL,
        30,
        Ok(plan(vec![feature_record("metadata.version", 30)], &["metadata.version"])),
    },
    first_unstable_level_is_refused = {
        LOCAL,
        31,
        Err(invalid("metadata.version", 31, "Local controller 1 only supports versions 7-30")),
    },
    higher_unstable_level_is_refused = {
        LOCAL,
        33,
        Err(invalid("metadata.version", 33, "Local controller 1 only supports versions 7-30")),
    },
    enabled_unstable_level = {
        LocalController { unstable_features: UnstableFeatureVersions::Enabled, ..LOCAL },
        31,
        Ok(plan(vec![feature_record("metadata.version", 31)], &["metadata.version"])),
    },
    enabled_higher_unstable_level = {
        LocalController { unstable_features: UnstableFeatureVersions::Enabled, ..LOCAL },
        33,
        Ok(plan(vec![feature_record("metadata.version", 33)], &["metadata.version"])),
    },
)]
fn an_unstable_metadata_version_needs_unstable_feature_versions(
    local: LocalController,
    level: i16,
    expected: Result<UpdatePlan, UpdateError>,
) {
    let image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 30,
    }]);
    let request = validate_only(vec![metadata_update(level, UPGRADE)]);
    assert2::assert!(
        plan_updates(&request, &image, local) == expected,
        "{:?} level {level}",
        local.unstable_features
    );
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
    let image = image(&[
        FeatureLevel {
            name: "metadata.version",
            level: 25,
        },
        FeatureLevel {
            name: "group.version",
            level: 1,
        },
    ]);
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
    let image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 25,
    }]);
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
        Ok(plan(vec![feature_record("metadata.version", target)], &[
            "metadata.version",
        ]))
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
        let image = image(&[FeatureLevel {
            name: "metadata.version",
            level: current,
        }]);
        let request = validate_only(vec![metadata_update(target, upgrade_type)]);
        assert!(
            plan_updates(&request, &image, LOCAL) == expected,
            "{current} -> {target} type {upgrade_type}"
        );
    }
}

#[yare::parameterized(
    upgrade = {
        "upgrade",
        vec![named_update("kraft.version", 1, UPGRADE)],
        Ok(UpdatePlan { kraft_upgrade: Some(1), ..plan(vec![], &["kraft.version"]) }),
    },
    upgrade_to_current = {
        "upgrade to the current level",
        vec![named_update("kraft.version", 0, UPGRADE)],
        Ok(plan(vec![], &["kraft.version"])),
    },
    downgrade_type_raises_level = {
        "a downgrade type that raises the level",
        vec![named_update("kraft.version", 1, SAFE)],
        Err(invalid("kraft.version", 1, "Can't downgrade to a newer version.")),
    },
    above_supported_range = {
        "above the supported range",
        vec![named_update("kraft.version", 2, UPGRADE)],
        Err(invalid("kraft.version", 2, "Local controller 1 only supports versions 0-1")),
    },
)]
fn kraft_version_rows_follow_kafka(
    case: &str,
    updates: Vec<FeatureUpdateKey>,
    expected: Result<UpdatePlan, UpdateError>,
) {
    let image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 25,
    }]);
    assert2::assert!(
        plan_updates(&validate_only(updates), &image, LOCAL) == expected,
        "{case}"
    );
}

/// An image at 4.3-IV0, with `eligible.leader.replicas.version` finalized at
/// `elr_level` when one is given and a published ELR on topic `orders` when
/// `published` is.
fn elr_image(elr_level: Option<i16>, published: bool) -> krabka_metadata::MetadataImage {
    let mut image = image(&[FeatureLevel {
        name: "metadata.version",
        level: 30,
    }]);
    if let Some(level) = elr_level {
        image.apply(&feature_record(ELR_VERSION, level));
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
#[yare::parameterized(published = { true }, never_published = { false })]
fn an_elr_downgrade_clears_the_published_state_before_the_feature_record(published: bool) {
    let elr = ELR_VERSION;
    let mut records = Vec::new();
    if published {
        records.push(MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "orders".into(),
                partition: 0,
                eligible_leader_replicas: vec![],
                last_known_elr: vec![],
            },
        ));
    }
    records.push(feature_record(elr, 0));
    let image = elr_image(Some(1), published);
    let request = validate_only(vec![elr_update(0, SAFE)]);
    assert2::assert!(
        plan_updates(&request, &image, LOCAL) == Ok(plan(records, &[elr])),
        "{published}"
    );
}

/// A broker that did not register a feature supports only level 0, so it
/// blocks turning the feature on but not off.
#[test]
fn a_feature_unaware_broker_blocks_only_enabling() {
    let elr = ELR_VERSION;
    let unaware = |elr_level| {
        let image = elr_image(elr_level, false);
        let mut features = krabka_metadata::supported_feature_ranges();
        features.remove(elr);
        remote_feature_image().image(image).ranges(features).call()
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
    let key = MIN_INSYNC_REPLICAS;
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
        (None, node, Some("2"), 1, vec![
            config(cluster, Some("1")),
            config(node, None),
        ]),
        (Some("3"), node, Some("2"), 1, vec![config(node, None)]),
        (Some("3"), unregistered, Some("2"), 1, vec![config(
            unregistered,
            None,
        )]),
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
    let with_broker = |levels: &[FeatureLevel<'_>], range| {
        let image = image(levels);
        let mut features = krabka_metadata::supported_feature_ranges();
        match range {
            Some(range) => features.insert(kv.to_owned(), range),
            None => features.remove(kv),
        };
        remote_feature_image().image(image).ranges(features).call()
    };
    let cases = [
        (
            "finalize level 1 on an unfinalized cluster",
            image(&[FeatureLevel {
                name: mv,
                level: 30,
            }]),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "finalize level 1 with a broker that supports it",
            with_broker(
                &[FeatureLevel {
                    name: mv,
                    level: 30,
                }],
                Some((0, 1)),
            ),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "a broker that advertises a narrower range",
            with_broker(
                &[FeatureLevel {
                    name: mv,
                    level: 30,
                }],
                Some((0, 0)),
            ),
            named_update(kv, 1, UPGRADE),
            Err(invalid(kv, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "a broker that does not register the feature",
            with_broker(
                &[FeatureLevel {
                    name: mv,
                    level: 30,
                }],
                None,
            ),
            named_update(kv, 1, UPGRADE),
            Err(invalid(kv, 1, "Broker 2 does not support this feature.")),
        ),
        (
            "a level above the local controller's range",
            image(&[FeatureLevel {
                name: mv,
                level: 30,
            }]),
            named_update(kv, 2, UPGRADE),
            Err(invalid(
                kv,
                2,
                "Local controller 1 only supports versions 0-1",
            )),
        ),
        (
            "re-finalizing the current level",
            image(&[
                FeatureLevel {
                    name: mv,
                    level: 30,
                },
                FeatureLevel { name: kv, level: 1 },
            ]),
            named_update(kv, 1, UPGRADE),
            Ok(plan(vec![feature_record(kv, 1)], &[kv])),
        ),
        (
            "a downgrade without a downgrade type",
            image(&[
                FeatureLevel {
                    name: mv,
                    level: 30,
                },
                FeatureLevel { name: kv, level: 1 },
            ]),
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
            image(&[
                FeatureLevel {
                    name: mv,
                    level: 30,
                },
                FeatureLevel { name: kv, level: 1 },
            ]),
            named_update(kv, 0, SAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "an unsafe downgrade",
            image(&[
                FeatureLevel {
                    name: mv,
                    level: 30,
                },
                FeatureLevel { name: kv, level: 1 },
            ]),
            named_update(kv, 0, UNSAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "a downgrade a narrower broker still supports",
            with_broker(
                &[
                    FeatureLevel {
                        name: mv,
                        level: 30,
                    },
                    FeatureLevel { name: kv, level: 1 },
                ],
                Some((0, 0)),
            ),
            named_update(kv, 0, SAFE),
            Ok(plan(vec![feature_record(kv, 0)], &[kv])),
        ),
        (
            "a downgrade type that raises the level",
            image(&[FeatureLevel {
                name: mv,
                level: 30,
            }]),
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
