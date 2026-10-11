//! The independent compatibility and protocol feature gates, together with
//! the two sets of defaults that the production and the test constructor use.

use crate::api_catalog::{LegacyRequestVersions, UnstableApiVersions, VersionGates};

/// Construction-time configuration for [`crate::Broker::start`].
///
/// Build it directly when you embed the broker as a library. In production,
/// build it with the `krabka-broker` binary's clap CLI.
#[derive(Debug, Clone, Copy)]
pub struct BrokerFeatureFlags {
    pub oauthbearer_jwks_ignore_key_use: bool,
    pub auto_leader_rebalance_enable: bool,
    pub transaction_two_phase_commit_enable: bool,
    /// Kafka's internal `unstable.api.versions.enable`, read from
    /// `server_properties` under that name. While it is
    /// [`Disabled`][crate::api_catalog::UnstableApiVersions::Disabled], the
    /// default, every listener -- broker and controller alike -- advertises
    /// and accepts exactly Kafka 4.3.1's API versions, and closes a connection
    /// that sends a version or an api key only Kafka trunk has. It also keeps
    /// the KIP-939 `keepPreparedTxn` recovery, which 4.3.1 answers
    /// `UNSUPPORTED_VERSION`, off.
    pub unstable_api_versions: UnstableApiVersions,
    /// Kafka's internal `unstable.feature.versions.enable`, read from
    /// `server_properties` under that name. While it is
    /// [`Disabled`][krabka_raft::UnstableFeatureVersions::Disabled], the
    /// default, this node supports `metadata.version` only up to 4.3.1's
    /// latest production level, `4.3-IV0`: that is what `ApiVersions`
    /// advertises, what the node registers, and the ceiling `UpdateFeatures`
    /// checks.
    pub unstable_feature_versions: krabka_raft::UnstableFeatureVersions,
    /// krabka's `[runtime]` `legacy_request_versions_enable`: whether the
    /// pre-4.0 `Fetch`, `ListOffsets` and `Produce` versions Kafka 4.x refuses
    /// are served. See [`crate::api_catalog::LegacyRequestVersions`].
    pub legacy_request_versions: LegacyRequestVersions,
}

impl BrokerFeatureFlags {
    /// The two switches a listener's API table depends on.
    #[must_use]
    pub const fn version_gates(&self) -> VersionGates {
        VersionGates {
            unstable: self.unstable_api_versions,
            legacy: self.legacy_request_versions,
        }
    }
}

pub(super) const fn test_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: false,
        transaction_two_phase_commit_enable: false,
        unstable_api_versions: UnstableApiVersions::Disabled,
        unstable_feature_versions: krabka_raft::UnstableFeatureVersions::Disabled,
        legacy_request_versions: LegacyRequestVersions::Disabled,
    }
}

pub(super) const fn default_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: true,
        transaction_two_phase_commit_enable: false,
        unstable_api_versions: UnstableApiVersions::Disabled,
        unstable_feature_versions: krabka_raft::UnstableFeatureVersions::Disabled,
        legacy_request_versions: LegacyRequestVersions::Disabled,
    }
}
