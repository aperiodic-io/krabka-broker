//! KIP-584 `FeatureUpdate.UpgradeType` decoding.
//!
//! Kafka's `QuorumController.updateFeatures` reads the wire `UpgradeType` at
//! every request version with `FeatureUpdate.UpgradeType.fromCode`. The field
//! starts at version 1 with a default of 1, so a version 0 row always decodes
//! as UPGRADE and its `AllowDowngrade` flag is not read.

use krabka_verified::features::FeatureUpdateType;

/// `FeatureUpdate.UpgradeType.fromCode`: 1 is UPGRADE, 2 is
/// `SAFE_DOWNGRADE`, 3 is `UNSAFE_DOWNGRADE`, and every other code is
/// `UNKNOWN`, here `None`.
pub(super) fn update_type(upgrade_type: i8) -> Option<FeatureUpdateType> {
    match upgrade_type {
        1 => Some(FeatureUpdateType::Upgrade),
        2 => Some(FeatureUpdateType::SafeDowngrade),
        3 => Some(FeatureUpdateType::UnsafeDowngrade),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        unknown = { 0, None },
        upgrade = { 1, Some(FeatureUpdateType::Upgrade) },
        safe_downgrade = { 2, Some(FeatureUpdateType::SafeDowngrade) },
        unsafe_downgrade = { 3, Some(FeatureUpdateType::UnsafeDowngrade) },
        too_large = { 4, None },
        negative = { -1, None },
    )]
    fn update_type_follows_from_code(code: i8, want: Option<FeatureUpdateType>) {
        assert2::assert!(update_type(code) == want, "{code}");
    }
}
