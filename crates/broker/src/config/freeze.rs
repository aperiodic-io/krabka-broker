//! The `[freeze]` runtime policy: how large the topic write-freeze registry
//! may grow, and when a freeze record has to carry an operator signature.

use {
    crate::config::{DEFAULT_FREEZE_MAX_ENTRIES, DEFAULT_FREEZE_SIGNATURE_MAX_SKEW},
    krabka_units::Time,
};

/// Runtime `[freeze]` policy: the topic write-freeze registry's bounds and its
/// signature requirement.
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub struct FreezeConfig {
    /// Ceiling on live registry entries. A request that would exceed it is
    /// refused.
    #[default(DEFAULT_FREEZE_MAX_ENTRIES)]
    pub max_entries: usize,
    /// Demand a detached operator signature on every freeze as well as on
    /// every thaw.
    ///
    /// A thaw is signed either way. The default `false` keeps a freeze
    /// available in one command on a cluster that has no key material
    /// provisioned yet, because freezing is the safe direction.
    pub require_signature: bool,
    /// How far a signed freeze record's timestamp may sit from the
    /// controller's clock. A record outside the window is refused, which is
    /// what stops an old signature being replayed.
    #[default(DEFAULT_FREEZE_SIGNATURE_MAX_SKEW)]
    pub signature_max_skew: Time,
}
