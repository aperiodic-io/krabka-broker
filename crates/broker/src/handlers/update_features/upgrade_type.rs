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
    use {super::*, assert2::assert};

    #[test]
    fn update_type_follows_from_code() {
        for (code, want) in [
            (0, None),
            (1, Some(FeatureUpdateType::Upgrade)),
            (2, Some(FeatureUpdateType::SafeDowngrade)),
            (3, Some(FeatureUpdateType::UnsafeDowngrade)),
            (4, None),
            (-1, None),
        ] {
            assert!(update_type(code) == want, "{code}");
        }
    }
}
