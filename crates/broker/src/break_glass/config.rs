//! The runtime view of `[break_glass]`.
//!
//! [`crate::config::BreakGlassConfig`] holds the parsed and validated values.
//! The file-config layer already refused a `required_approvals` below two, a
//! `signed_actions` entry that names no action, a `signed_actions` entry with
//! no operator key, and a malformed duration. This
//! module answers only the three questions the workflow asks of that
//! configuration at run time: is the workflow on, is this principal an
//! approver, and does this action need a signature.
//!
//! # The approver set is a per-node value
//!
//! `approvers` comes from each broker's own `broker.toml`, and not from the
//! metadata log, because an attacker who can write the metadata log must not be
//! able to add themselves to the set that authorizes a data-losing operation.
//! Two brokers can then disagree during a rolling configuration change, and
//! nothing in the cluster reconciles them. [`BreakGlassPolicy::fingerprint`]
//! is what makes a disagreement visible after the fact: every break-glass audit
//! event carries it.

use {
    crate::{break_glass::action_name, config::BreakGlassConfig, operator_keys},
    krabka_metadata::BreakGlassAction,
    krabka_units::Time,
};

/// A borrowed view of `[break_glass]`, with the questions the workflow asks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BreakGlassPolicy<'a> {
    config: &'a BreakGlassConfig,
}

impl<'a> BreakGlassPolicy<'a> {
    /// Read `config` as a break-glass policy.
    pub(crate) fn new(config: &'a BreakGlassConfig) -> Self { Self { config } }

    /// Whether this broker runs the break-glass workflow.
    ///
    /// An empty approver set turns the workflow off. Every gated transition
    /// then behaves as it does on a cluster with no `[break_glass]` section,
    /// which is the rule that keeps a feature nobody uses free. Every action is
    /// gated together, so there is no per-action switch here. The one action
    /// with its own setting is the background unclean recovery, which has no
    /// caller to refuse, and
    /// [`BackgroundUncleanRecovery`](crate::config::BackgroundUncleanRecovery)
    /// governs it on the recovery path.
    pub(crate) fn is_enabled(self) -> bool { !self.config.approvers.is_empty() }

    /// Whether `principal` may approve a proposal on this broker.
    ///
    /// The broker asks this question when a person approves, and never when it
    /// spends the approval. See [`crate::break_glass::gate::authorize`].
    pub(crate) fn is_approver(self, principal: &str) -> bool {
        self.config
            .approvers
            .iter()
            .any(|approver| approver == principal)
    }

    /// Whether an approval of `action` must carry a detached operator
    /// signature.
    ///
    /// The `signed_actions` entries name an action with the string that
    /// [`action_name`] returns. The configuration layer already refused an
    /// entry that names no action, so a `false` here means the operator did
    /// not ask for a signature, and never that they misspelled the request.
    pub(crate) fn needs_signature(self, action: BreakGlassAction) -> bool {
        let name = action_name(action);
        self.config
            .signed_actions
            .iter()
            .any(|signed| signed == name)
    }

    /// How many distinct principals must approve a proposal.
    ///
    /// The file-config layer refuses a value below two, so a two-person rule
    /// cannot be configured down to one person.
    pub(crate) fn required_approvals(self) -> usize { self.config.required_approvals }

    /// How long a new proposal stays usable.
    ///
    /// The lifetime is also the safety bound on removing an approver. Wait it
    /// out, and every pending approval by that principal is dead.
    pub(crate) fn proposal_ttl(self) -> Time { self.config.proposal_ttl }

    /// The SHA-256 fingerprint of this broker's approver set.
    ///
    /// Every break-glass audit event carries it, so a broker that disagrees
    /// with its peers about the set is visible in the audit log afterwards.
    pub(crate) fn fingerprint(self) -> String {
        operator_keys::approver_set_fingerprint(&self.config.approvers)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{break_glass::ALL_ACTIONS, operator_keys::approver_set_fingerprint},
        assert2::check,
        krabka_units::minutes,
    };

    fn config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            required_approvals: 2,
            proposal_ttl: minutes(30),
            signed_actions: vec!["delete_topic".to_owned()],
            ..BreakGlassConfig::default()
        }
    }

    #[test]
    fn an_empty_approver_set_turns_the_workflow_off() {
        let off = BreakGlassConfig::default();
        check!(!BreakGlassPolicy::new(&off).is_enabled());

        let on = config();
        check!(BreakGlassPolicy::new(&on).is_enabled());
    }

    #[test]
    fn only_a_configured_principal_is_an_approver() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let cases = [
            ("a configured approver", "User:alice", true),
            ("a second configured approver", "User:bob", true),
            ("a principal outside the set", "User:mallory", false),
            ("the bare name of an approver", "alice", false),
            ("an empty principal", "", false),
        ];
        for (label, principal, expected) in cases {
            check!(policy.is_approver(principal) == expected, "case {label}");
        }
    }

    #[test]
    fn only_a_named_action_needs_a_signature() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        for action in ALL_ACTIONS {
            let expected = action == krabka_metadata::BreakGlassAction::DeleteTopic;
            check!(
                policy.needs_signature(action) == expected,
                "{}",
                action_name(action)
            );
        }
    }

    #[test]
    fn an_empty_signed_action_list_needs_no_signature_anywhere() {
        let config = BreakGlassConfig {
            signed_actions: Vec::new(),
            ..config()
        };
        let policy = BreakGlassPolicy::new(&config);
        for action in ALL_ACTIONS {
            check!(!policy.needs_signature(action), "{}", action_name(action));
        }
    }

    #[test]
    fn the_policy_reports_the_configured_bounds_and_fingerprint() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);

        check!(policy.required_approvals() == 2);
        check!(policy.proposal_ttl() == minutes(30));
        check!(policy.fingerprint() == approver_set_fingerprint(&config.approvers));
    }
}
