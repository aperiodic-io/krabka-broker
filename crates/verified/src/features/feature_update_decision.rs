use super::{
    FeatureKind, FeatureLevels, FeatureUpdateDecision, FeatureUpdateFacts, FeatureUpdateType,
};
#[cfg(creusot)]
use creusot_std::prelude::*;

crate::creusot_open_spec! {
    /// The reference decision: the first failed rule in `updateFeature`'s
    /// order, or what the admitted row does.
    pub fn feature_update_model(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    pearlite! {
        match facts.update_type {
            None => FeatureUpdateDecision::UnknownUpdateType,
            Some(update_type) => {
                let requested = facts.levels.requested@;
                let current = facts.levels.current@;
                if requested < 0 {
                    FeatureUpdateDecision::NegativeLevel
                } else if !facts.all_nodes_support {
                    FeatureUpdateDecision::UnsupportedByNode
                } else if requested < current && update_type == FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DowngradeWithoutFlag
                } else if requested > current && update_type != FeatureUpdateType::Upgrade {
                    FeatureUpdateDecision::DowngradeToNewerLevel
                } else {
                    match facts.kind {
                        FeatureKind::MetadataVersion { metadata_changed } =>
                            if requested < current && metadata_changed {
                                if update_type == FeatureUpdateType::UnsafeDowngrade {
                                    FeatureUpdateDecision::UnsafeMetadataDowngrade
                                } else {
                                    FeatureUpdateDecision::LossyMetadataDowngrade
                                }
                            } else {
                                FeatureUpdateDecision::EmitFeature
                            },
                        FeatureKind::KRaftVersion =>
                            if update_type == FeatureUpdateType::Upgrade {
                                FeatureUpdateDecision::UpgradeKRaft
                            } else if requested != current {
                                FeatureUpdateDecision::KRaftDowngrade
                            } else {
                                FeatureUpdateDecision::NoChange
                            },
                        FeatureKind::Other { dependencies_met } =>
                            if dependencies_met {
                                FeatureUpdateDecision::EmitFeature
                            } else {
                                FeatureUpdateDecision::DependencyUnmet
                            },
                    }
                }
            }
        }
    }
    }
}

/// Decide one feature row from the host's facts.
///
/// A rejection names the first rule that fails in the order of Kafka's
/// `FeatureControlManager.updateFeature`. A `metadata.version` downgrade is
/// admitted only when no level between the two changed metadata, whatever the
/// downgrade type, and an admitted `metadata.version` row never writes
/// anything but its feature-level record.
#[cfg_attr(creusot, ensures(result == feature_update_model(facts)))]
#[cfg_attr(creusot, ensures(match facts.kind {
    FeatureKind::MetadataVersion { metadata_changed } =>
        (metadata_changed && result == FeatureUpdateDecision::EmitFeature)
            ==> facts.levels.requested@ >= facts.levels.current@,
    _ => true,
}))]
#[must_use]
pub fn feature_update_decision(facts: FeatureUpdateFacts) -> FeatureUpdateDecision {
    let Some(update_type) = facts.update_type else {
        return FeatureUpdateDecision::UnknownUpdateType;
    };
    let FeatureLevels { requested, current } = facts.levels;
    if requested < 0 {
        return FeatureUpdateDecision::NegativeLevel;
    }
    if !facts.all_nodes_support {
        return FeatureUpdateDecision::UnsupportedByNode;
    }
    let upgrade = matches!(update_type, FeatureUpdateType::Upgrade);
    if requested < current && upgrade {
        return FeatureUpdateDecision::DowngradeWithoutFlag;
    }
    if requested > current && !upgrade {
        return FeatureUpdateDecision::DowngradeToNewerLevel;
    }
    match facts.kind {
        FeatureKind::MetadataVersion { metadata_changed } => {
            if requested < current && metadata_changed {
                if matches!(update_type, FeatureUpdateType::UnsafeDowngrade) {
                    FeatureUpdateDecision::UnsafeMetadataDowngrade
                } else {
                    FeatureUpdateDecision::LossyMetadataDowngrade
                }
            } else {
                FeatureUpdateDecision::EmitFeature
            }
        }
        FeatureKind::KRaftVersion => {
            if upgrade {
                FeatureUpdateDecision::UpgradeKRaft
            } else if requested != current {
                FeatureUpdateDecision::KRaftDowngrade
            } else {
                FeatureUpdateDecision::NoChange
            }
        }
        FeatureKind::Other { dependencies_met } => {
            if dependencies_met {
                FeatureUpdateDecision::EmitFeature
            } else {
                FeatureUpdateDecision::DependencyUnmet
            }
        }
    }
}
