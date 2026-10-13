//! The TOML shapes of the privileged-action sections — `[[operator_keys]]`,
//! `[freeze]` and `[break_glass]` — and the apply step that merges them.
//!
//! The three sections share one module because two rules cross them: a
//! demanded signature needs an operator key to verify it, and that key set is
//! provisioned once for both the freeze path and the break-glass path.

use {
    super::{
        FileConfigError,
        validate::{invalid_runtime_value, positive_time},
    },
    crate::{
        break_glass::{ALL_ACTIONS, action_from_name, action_name},
        config::{
            BackgroundUncleanRecovery, BrokerConfig, DEFAULT_BREAK_GLASS_SIGNED_ACTIONS,
            MIN_BREAK_GLASS_REQUIRED_APPROVALS,
        },
        operator_keys::{OperatorKeyEntry, OperatorKeys},
    },
    krabka_units::Time,
};

/// TOML shape of one `[[operator_keys]]` entry. Maps to
/// [`crate::operator_keys::OperatorKeyEntry`].
///
/// `deny_unknown_fields` so a misspelled key is rejected at parse time. An
/// ignored `principal` typo would leave a key bound to nobody, and the
/// principal binding is what stops one operator's key signing in another
/// operator's name.
#[krabka_macros::config_table(strict)]
pub struct FileOperatorKey {
    /// Stable identifier that a signed freeze record or break-glass approval
    /// names. Must be unique across the array.
    pub key_id: String,
    /// The principal this key speaks for, e.g. `"User:alice"`. Must be unique
    /// across the array. The broker refuses a signed record whose claimed
    /// author is not this principal.
    pub principal: String,
    /// Path to the raw 32-byte Ed25519 public key, the bytes an
    /// [`krabka_audit::FileEd25519Signer`] reports as its public key. It is
    /// read at startup, so a bad path stops the broker at boot and not in the
    /// middle of an incident.
    pub public_key_path: String,
}

impl From<&FileOperatorKey> for OperatorKeyEntry {
    fn from(file: &FileOperatorKey) -> Self {
        Self {
            key_id: file.key_id.clone(),
            principal: file.principal.clone(),
            public_key_path: std::path::PathBuf::from(&file.public_key_path),
        }
    }
}

/// TOML shape of `[freeze]`. Maps to [`crate::config::FreezeConfig`].
///
/// Every field is `Option`: a present value replaces the current broker value,
/// an absent one retains it. `deny_unknown_fields` so a misspelled
/// `require_signature` is rejected at parse time rather than leaving the
/// broker on the opposite policy to the one the operator wrote.
#[krabka_macros::human_units]
#[krabka_macros::config_table(strict)]
pub struct FileFreezeConfig {
    /// Ceiling on live freeze registry entries. Default
    /// [`crate::config::DEFAULT_FREEZE_MAX_ENTRIES`]. Must be at least 1.
    pub max_entries: Option<usize>,
    /// **Security-sensitive.** Demand a detached operator signature on a
    /// freeze as well as on a thaw. Default `false`, which keeps a freeze
    /// available in one command during an incident on a cluster with no key
    /// material yet. A thaw is signed either way.
    ///
    /// Setting this to `true` with no `[[operator_keys]]` entry is a startup
    /// error: there would be no key to verify the demanded signature against.
    pub require_signature: Option<bool>,
    /// How far a signed freeze record's timestamp may sit from the
    /// controller's clock. Default
    /// [`crate::config::DEFAULT_FREEZE_SIGNATURE_MAX_SKEW`].
    pub signature_max_skew: Option<Time>,
}

/// TOML shape of `[break_glass]`. Maps to
/// [`crate::config::BreakGlassConfig`].
///
/// Every field is `Option`, so `approvers = []` and `signed_actions = []` are
/// each a written choice and are distinct from omitting the key.
/// `deny_unknown_fields` so a misspelled key is rejected at parse time.
#[krabka_macros::human_units]
#[krabka_macros::config_table(strict)]
pub struct FileBreakGlassConfig {
    /// Principals that may approve a proposal. Omitted leaves the
    /// `BrokerConfig` value, which is empty.
    pub approvers: Option<Vec<String>>,
    /// Distinct approving principals a proposal needs. Default
    /// [`crate::config::DEFAULT_BREAK_GLASS_REQUIRED_APPROVALS`]. Values below
    /// [`crate::config::MIN_BREAK_GLASS_REQUIRED_APPROVALS`] are a startup
    /// error: a two-person rule with one approval is one person.
    pub required_approvals: Option<usize>,
    /// How long a proposal stays usable. Default
    /// [`crate::config::DEFAULT_BREAK_GLASS_PROPOSAL_TTL`].
    pub proposal_ttl: Option<Time>,
    /// Actions whose approvals must also carry a detached operator signature.
    /// Omitted inside a present `[break_glass]` section selects
    /// [`crate::config::DEFAULT_BREAK_GLASS_SIGNED_ACTIONS`], the irreversible
    /// set. Naming any action with no `[[operator_keys]]` entry is a startup
    /// error; write `signed_actions = []` to demand no signature.
    pub signed_actions: Option<Vec<String>>,
    /// What the background unclean-recovery path does, where there is no
    /// caller to ask for an approval. Default
    /// [`BackgroundUncleanRecovery::AuditOnly`].
    pub background_unclean_recovery: Option<BackgroundUncleanRecovery>,
}

/// Every break-glass action name, comma separated, for an error message.
///
/// An operator who misspells one needs the list in front of them, because the
/// names are not the wire spellings and not the CLI subcommands.
fn known_break_glass_action_names() -> String {
    ALL_ACTIONS
        .into_iter()
        .map(action_name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Apply `[[operator_keys]]`, `[freeze]` and `[break_glass]`, then check the
/// two rules that cross those sections.
///
/// Both cross-section rules are startup errors. A broker that boots with a
/// demanded signature and no key to verify it against refuses every such
/// request at run time, with nothing said at boot to explain why.
pub(super) fn apply_privileged_action_policy(
    operator_keys: &[FileOperatorKey],
    freeze: Option<FileFreezeConfig>,
    break_glass: Option<FileBreakGlassConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    if !operator_keys.is_empty() {
        let entries: Vec<OperatorKeyEntry> =
            operator_keys.iter().map(OperatorKeyEntry::from).collect();
        cfg.operator_keys = OperatorKeys::load(&entries)
            .map_err(|error| FileConfigError::OperatorKeys(error.to_string()))?;
    }

    if let Some(freeze) = freeze {
        if let Some(max_entries) = freeze.max_entries {
            if max_entries == 0 {
                return Err(invalid_runtime_value(
                    "freeze.max_entries",
                    "must be at least 1; a registry that holds nothing can never freeze a topic",
                ));
            }
            cfg.freeze.max_entries = max_entries;
        }
        if let Some(require_signature) = freeze.require_signature {
            cfg.freeze.require_signature = require_signature;
        }
        if let Some(skew) = freeze.signature_max_skew {
            cfg.freeze.signature_max_skew = positive_time("freeze.signature_max_skew", skew)?;
        }
    }

    if let Some(break_glass) = break_glass {
        if let Some(approvers) = break_glass.approvers {
            cfg.break_glass.approvers = approvers;
        }
        if let Some(required) = break_glass.required_approvals {
            if required < MIN_BREAK_GLASS_REQUIRED_APPROVALS {
                return Err(invalid_runtime_value(
                    "break_glass.required_approvals",
                    "must be at least 2; a two-person rule with one approval is one person",
                ));
            }
            cfg.break_glass.required_approvals = required;
        }
        if let Some(ttl) = break_glass.proposal_ttl {
            cfg.break_glass.proposal_ttl = positive_time("break_glass.proposal_ttl", ttl)?;
        }
        cfg.break_glass.signed_actions = break_glass.signed_actions.unwrap_or_else(|| {
            DEFAULT_BREAK_GLASS_SIGNED_ACTIONS
                .iter()
                .map(|action| (*action).to_owned())
                .collect()
        });
        for action in &cfg.break_glass.signed_actions {
            if action_from_name(action).is_none() {
                return Err(invalid_runtime_value(
                    "break_glass.signed_actions",
                    format!(
                        "{action:?} names no break-glass action. A name that matches no action \
                         demands a signature for nothing, so the action the name was meant to \
                         protect would be approved unsigned. The names are: {}",
                        known_break_glass_action_names()
                    ),
                ));
            }
        }
        if let Some(mode) = break_glass.background_unclean_recovery {
            cfg.break_glass.background_unclean_recovery = mode;
        }
    }

    if cfg.operator_keys.is_empty() {
        if let Some(action) = cfg.break_glass.signed_actions.first() {
            return Err(FileConfigError::OperatorKeys(format!(
                "break_glass.signed_actions names {action:?} but no [[operator_keys]] entry is \
                 configured; every approval of that action would be refused. Provision an \
                 operator key, or write `signed_actions = []`"
            )));
        }
        if cfg.freeze.require_signature {
            return Err(FileConfigError::OperatorKeys(
                "freeze.require_signature is true but no [[operator_keys]] entry is configured; \
                 every freeze and every thaw would be refused"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
