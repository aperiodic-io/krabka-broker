//! Feature-finalization admission for one `UpdateFeatures` row.
//!
//! The decision follows Kafka 4.3.1's `FeatureControlManager.updateFeature`
//! check for check: the upgrade type, a negative level, every node's support
//! (`reasonNotSupported`), the update direction against the current level
//! (an unfinalized feature reads as level 0), and then the per-feature tail:
//! `updateMetadataVersion`'s `didMetadataChange` refusal for
//! `metadata.version`, the no-downgrade rule for `kraft.version`, and
//! `Feature.validateVersion`'s KIP-1022 dependencies for every other feature.

#[cfg(creusot)]
use creusot_std::prelude::*;

/// KIP-584 `FeatureUpdate.UpgradeType`, decoded from the request.
#[krabka_macros::model_type]
pub enum FeatureUpdateType {
    Upgrade,
    SafeDowngrade,
    UnsafeDowngrade,
}

/// The levels one feature row compares.
#[krabka_macros::model_type]
pub struct FeatureLevels {
    /// The row's `MaxVersionLevel`.
    pub requested: i16,
    /// The current level, `0` for a feature that is not finalized.
    pub current: i16,
}

/// Which branch of `updateFeature` decides the row after the shared checks,
/// with the one fact that branch reads.
#[krabka_macros::model_type]
pub enum FeatureKind {
    /// `metadata.version`. `metadata_changed` is Kafka's
    /// `MetadataVersion.checkIfMetadataChanged` between the current and the
    /// requested level.
    MetadataVersion { metadata_changed: bool },
    /// `kraft.version`, which the Raft layer finalizes.
    KRaftVersion,
    /// Every other feature. `dependencies_met` is `Feature.validateVersion`
    /// against the proposed levels: the finalized levels with every update
    /// of the same request applied.
    Other { dependencies_met: bool },
}

/// Everything one `UpdateFeatures` row is decided on.
#[krabka_macros::model_type]
pub struct FeatureUpdateFacts {
    /// `None` when the row carries an unknown upgrade-type code.
    pub update_type: Option<FeatureUpdateType>,
    pub levels: FeatureLevels,
    /// The local controller, every registered broker, and every registered
    /// controller support the requested level, and every quorum controller
    /// has registered.
    pub all_nodes_support: bool,
    pub kind: FeatureKind,
}

/// The first failed rule, or what an admitted row does.
#[krabka_macros::model_type]
pub enum FeatureUpdateDecision {
    UnknownUpdateType,
    NegativeLevel,
    UnsupportedByNode,
    DowngradeWithoutFlag,
    DowngradeToNewerLevel,
    /// A `metadata.version` downgrade across a level that changed metadata,
    /// asked for as `UNSAFE_DOWNGRADE`.
    UnsafeMetadataDowngrade,
    /// A `metadata.version` downgrade across a level that changed metadata,
    /// asked for as `SAFE_DOWNGRADE`.
    LossyMetadataDowngrade,
    /// A `kraft.version` downgrade.
    KRaftDowngrade,
    DependencyUnmet,
    /// Emit the feature-level record.
    EmitFeature,
    /// Hand the `kraft.version` upgrade to the Raft layer.
    UpgradeKRaft,
    /// A `kraft.version` row with a downgrade type at the current level:
    /// nothing to do.
    NoChange,
}

mod feature_update_decision;
pub use feature_update_decision::feature_update_decision;
#[cfg(creusot)]
pub use feature_update_decision::feature_update_model;

#[cfg(test)]
mod tests;
