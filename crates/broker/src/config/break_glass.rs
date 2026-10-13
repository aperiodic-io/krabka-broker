//! The `[break_glass]` runtime policy: who may approve a privileged
//! transition, and what the background unclean-recovery path does where there
//! is nobody to ask.

use {
    crate::config::{DEFAULT_BREAK_GLASS_PROPOSAL_TTL, DEFAULT_BREAK_GLASS_REQUIRED_APPROVALS},
    krabka_units::Time,
    schemars::JsonSchema,
    serde::Deserialize,
};

/// Runtime `[break_glass]` policy: who may approve a privileged transition,
/// how many of them it takes, and which actions also demand a signature.
///
/// The approver set comes from this broker's own file, not from the metadata
/// log, for the reason that keeps `super_users` out of the ACL store: an
/// attacker who can write the metadata log must not be able to add themselves
/// to the set that authorizes a data-losing operation.
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub struct BreakGlassConfig {
    /// Principals that may approve a proposal. Empty is the default and means
    /// no break-glass workflow is configured on this broker.
    pub approvers: Vec<String>,
    /// Distinct approving principals a proposal needs. Never below
    /// [`MIN_BREAK_GLASS_REQUIRED_APPROVALS`](super::MIN_BREAK_GLASS_REQUIRED_APPROVALS).
    #[default(DEFAULT_BREAK_GLASS_REQUIRED_APPROVALS)]
    pub required_approvals: usize,
    /// How long a proposal stays usable after it is created.
    #[default(DEFAULT_BREAK_GLASS_PROPOSAL_TTL)]
    pub proposal_ttl: Time,
    /// Actions whose approvals must also carry a detached operator signature.
    /// Empty is the default; see
    /// [`DEFAULT_BREAK_GLASS_SIGNED_ACTIONS`](super::DEFAULT_BREAK_GLASS_SIGNED_ACTIONS).
    pub signed_actions: Vec<String>,
    /// What the background unclean-recovery path does, where there is no
    /// caller to ask for an approval.
    pub background_unclean_recovery: BackgroundUncleanRecovery,
}

/// What the background unclean-recovery path does when it cannot ask anybody
/// for a break-glass approval.
///
/// The controller starts unclean recovery from leader election and from the
/// broker-heartbeat path. Neither carries a request context, so neither has a
/// caller to refuse. An operator who types an unclean election can be asked
/// for a second signature; a controller that reacts to a dead broker at 03:00
/// cannot.
///
/// The TOML spelling is `off | audit-only | require`, so the serde rename is
/// `kebab-case`.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BackgroundUncleanRecovery {
    /// Recovery runs and writes no break-glass audit event.
    Off,
    /// Recovery runs and writes a bypassed break-glass audit event naming the
    /// partition and the strategy. An operator can then prove after the fact
    /// that a data-losing election happened with no approval.
    #[default]
    AuditOnly,
    /// Recovery does not run. The partition stays leaderless and visibly
    /// offline, and the refusal is audited.
    Require,
}
