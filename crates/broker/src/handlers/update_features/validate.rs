//! Validation of a whole `UpdateFeatures` request.
//!
//! This module is Kafka 4.3.1's `FeatureControlManager.updateFeatures`. The
//! rows go into a map keyed by feature name, so a repeated name keeps its last
//! row. Each entry is validated in the map's iteration order against the
//! current image. The first entry that fails stops the request and nothing is
//! written. Otherwise every entry's records form one atomic batch.

use {
    super::{
        java_order::hash_map_order,
        preconditions::{LocalController, dependency_error, reason_not_supported},
        upgrade_type::update_type,
    },
    crate::{
        codes, config_keys::MIN_INSYNC_REPLICAS, elr::clear_published_elr, features::ELR_VERSION,
    },
    krabka_metadata::{
        FeatureLevelRecord, MetadataRecord,
        metadata_version::{KRAFT_VERSION_FEATURE, METADATA_VERSION_FEATURE, METADATA_VERSION_MIN},
    },
    krabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    krabka_verified::features::{
        FeatureKind, FeatureLevels, FeatureUpdateDecision, FeatureUpdateFacts,
        feature_update_decision,
    },
    std::collections::BTreeMap,
};

/// Kafka's `MetadataVersion.MINIMUM_VERSION` name, which
/// `Feature.validateVersion` quotes.
const METADATA_VERSION_MIN_NAME: &str = "3.3-IV3";

/// Why a request failed: the error of the first entry that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UpdateError {
    pub code: i16,
    pub message: String,
}

impl UpdateError {
    /// `FeatureControlManager.invalidUpdateVersion`.
    fn invalid_update_version(feature: &str, level: i16, reason: &str) -> Self {
        Self {
            code: codes::INVALID_UPDATE_VERSION,
            message: format!("Invalid update version {level} for feature {feature}. {reason}"),
        }
    }

    /// `FeatureControlManager.unsupportedMetadataDowngrade`.
    fn unsupported_metadata_downgrade(current: i16, target: i16, reason: &str) -> Self {
        Self {
            code: codes::INVALID_UPDATE_VERSION,
            message: format!(
                "Unsupported metadata.version downgrade from {current} to {target}. {reason}"
            ),
        }
    }
}

/// What an accepted request does.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct UpdatePlan {
    /// The metadata records, in the order Kafka's controller writes them.
    pub records: Vec<MetadataRecord>,
    /// The `kraft.version` level the Raft layer must move to, when the
    /// request raises it.
    pub kraft_upgrade: Option<u16>,
    /// The request turns `eligible.leader.replicas.version` on, which
    /// `ConfigurationControlManager.updateFeatures` pairs with its safety
    /// config records.
    pub enables_elr: bool,
    /// Every feature the request names, each once.
    pub features: Vec<String>,
}

/// Validate `request` against `image` and plan its writes.
///
/// `local_controller` is the node that answers for the controller's own
/// feature support, which Kafka names as `Local controller <id>`, with the
/// `unstable.feature.versions.enable` that support follows.
///
/// # Errors
///
/// Returns the error of the first entry that fails, in Kafka's map order.
pub(super) fn plan_updates(
    request: &UpdateFeaturesRequest,
    image: &krabka_metadata::MetadataImage,
    local_controller: LocalController,
) -> Result<UpdatePlan, UpdateError> {
    let order = hash_map_order(
        request
            .feature_updates
            .iter()
            .map(|update| update.feature.as_str()),
    );
    // The last row for a name wins, as `HashMap.put` keeps it.
    let updates: Vec<&FeatureUpdateKey> = order
        .iter()
        .filter_map(|name| {
            request
                .feature_updates
                .iter()
                .rev()
                .find(|update| update.feature == *name)
        })
        .collect();

    // `proposedUpdatedVersions`: the finalized levels, with every update of
    // this request applied on top.
    let mut proposed: BTreeMap<String, i16> = image.finalized_features().clone();
    proposed.remove(KRAFT_VERSION_FEATURE);
    for update in &updates {
        proposed.insert(update.feature.clone(), update.max_version_level);
    }

    let mut plan = UpdatePlan {
        features: order.iter().map(ToString::to_string).collect(),
        ..UpdatePlan::default()
    };
    for update in updates {
        plan_update(update, image, local_controller, &proposed, &mut plan)?;
        if update.feature == ELR_VERSION && update.max_version_level > 0 {
            plan.enables_elr = true;
        }
    }
    Ok(plan)
}

/// `FeatureControlManager.updateFeature` for one entry.
fn plan_update(
    update: &FeatureUpdateKey,
    image: &krabka_metadata::MetadataImage,
    local_controller: LocalController,
    proposed: &BTreeMap<String, i16>,
    plan: &mut UpdatePlan,
) -> Result<(), UpdateError> {
    let name = update.feature.as_str();
    let level = update.max_version_level;
    let (current, kind, dependency) = if name == METADATA_VERSION_FEATURE {
        let current = image.finalized_metadata_version().unwrap_or(0);
        let kind = FeatureKind::MetadataVersion {
            metadata_changed: krabka_metadata::metadata_version::metadata_changed_between(
                current, level,
            ),
        };
        (current, kind, None)
    } else if name == KRAFT_VERSION_FEATURE {
        let current = i16::try_from(image.kraft_version()).unwrap_or(i16::MAX);
        (current, FeatureKind::KRaftVersion, None)
    } else {
        let current = image.finalized_feature(name).unwrap_or(0);
        let dependency = feature_dependency_error(name, level, proposed);
        let kind = FeatureKind::Other {
            dependencies_met: dependency.is_none(),
        };
        (current, kind, dependency)
    };
    let not_supported = reason_not_supported(image, local_controller, name, level);
    let decision = feature_update_decision(FeatureUpdateFacts {
        update_type: update_type(update.upgrade_type),
        levels: FeatureLevels {
            requested: level,
            current,
        },
        all_nodes_support: not_supported.is_none(),
        kind,
    });
    let invalid = |reason: &str| Err(UpdateError::invalid_update_version(name, level, reason));
    match decision {
        FeatureUpdateDecision::UnknownUpdateType => {
            invalid("The controller does not support the given upgrade type.")
        }
        FeatureUpdateDecision::NegativeLevel => invalid("A feature version cannot be less than 0."),
        FeatureUpdateDecision::UnsupportedByNode => invalid(&not_supported.unwrap_or_default()),
        FeatureUpdateDecision::DowngradeWithoutFlag => invalid(
            "Can't downgrade the version of this feature without setting the upgrade type to \
             either safe or unsafe downgrade.",
        ),
        FeatureUpdateDecision::DowngradeToNewerLevel => {
            invalid("Can't downgrade to a newer version.")
        }
        FeatureUpdateDecision::UnsafeMetadataDowngrade => {
            Err(UpdateError::unsupported_metadata_downgrade(
                current,
                level,
                "Unsafe metadata downgrade is not supported in this version.",
            ))
        }
        FeatureUpdateDecision::LossyMetadataDowngrade => {
            Err(UpdateError::unsupported_metadata_downgrade(
                current,
                level,
                "Refusing to perform the requested downgrade because it might delete metadata \
                 information.",
            ))
        }
        FeatureUpdateDecision::KRaftDowngrade => {
            invalid("Can't downgrade the version of this feature.")
        }
        FeatureUpdateDecision::DependencyUnmet => invalid(&dependency.unwrap_or_default()),
        FeatureUpdateDecision::UpgradeKRaft => {
            if level > current {
                plan.kraft_upgrade = u16::try_from(level).ok();
            }
            Ok(())
        }
        FeatureUpdateDecision::NoChange => Ok(()),
        FeatureUpdateDecision::EmitFeature => {
            // KIP-966: turning the feature off clears the memberships it
            // published. The clearing records go in first, so a replay that
            // stops between them and the feature record has already forgotten
            // the memberships rather than kept them under a feature that is
            // still on.
            if name == ELR_VERSION && level == 0 && current >= 1 {
                plan.records.extend(clear_published_elr(image));
            }
            plan.records
                .push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: name.to_string(),
                    level,
                }));
            Ok(())
        }
    }
}

/// `Feature.featureFromName` and `Feature.validateVersion` for a feature
/// other than `metadata.version` and `kraft.version`, against the proposed
/// levels, or `None` when the level may be set.
fn feature_dependency_error(
    name: &str,
    level: i16,
    proposed: &BTreeMap<String, i16>,
) -> Option<String> {
    let Some(feature) = krabka_metadata::feature(name) else {
        return Some(format!("Feature {name} not found."));
    };
    if level >= 1
        && proposed
            .get(METADATA_VERSION_FEATURE)
            .is_none_or(|&mv| mv < METADATA_VERSION_MIN)
    {
        return Some(format!(
            "{name} could not be set to {level} because it depends on \
             metadata.version={METADATA_VERSION_MIN} ({METADATA_VERSION_MIN_NAME})"
        ));
    }
    dependency_error(name, level, feature.dependencies(level), proposed)
}

/// Kafka's `ConfigurationControlManager.maybeGenerateElrSafetyRecords`: the
/// config records that make it safe to turn ELR on, written in the same batch
/// as, and ahead of, the feature record. The cluster-level
/// `min.insync.replicas` is set to the broker's static value when it has
/// none, and every broker-level `min.insync.replicas` is removed, including
/// one left behind by a broker that has since unregistered.
pub(super) fn elr_safety_records(
    image: &krabka_metadata::MetadataImage,
    static_min_insync_replicas: i32,
) -> Vec<MetadataRecord> {
    let key = MIN_INSYNC_REPLICAS;
    let mut records = Vec::new();
    if !image
        .default_broker_config()
        .is_some_and(|configs| configs.contains_key(key))
    {
        records.push(MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(static_min_insync_replicas.to_string()),
            },
        ));
    }
    // Every broker config resource, registered broker or not, as Kafka walks
    // `brokersWithConfigs`: unregistering a broker leaves its configs behind.
    // The image names those resources only through its record form.
    let mut nodes: Vec<krabka_metadata::NodeId> = image
        .to_records()
        .into_iter()
        .filter_map(|record| match record {
            MetadataRecord::V1BrokerConfig(config)
                if config.node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                    && config.config_name == key =>
            {
                Some(config.node_id)
            }
            _ => None,
        })
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    records.extend(nodes.into_iter().map(|node_id| {
        MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
            node_id,
            config_name: key.into(),
            config_value: None,
        })
    }));
    records
}

#[cfg(test)]
mod tests;
