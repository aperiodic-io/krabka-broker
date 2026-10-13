//! KFC-9: the break-glass two-person rule over the privileged transitions.
//!
//! A privileged transition can lose committed data, or can lift the protection
//! that stops another transition from losing committed data. Break-glass makes
//! two different people agree before the broker does one. An operator opens a
//! proposal, a second operator approves it, and the approved proposal is then a
//! standing authorization for a bounded window.
//!
//! The approved proposal is not a field on the request it authorizes. No Kafka
//! request gains a field for this feature. An operator gets the approval out of
//! band through the three private APIs, then runs the ordinary JVM tool, and
//! the gated handler looks for the approval in its own metadata image.
//!
//! # Modules
//!
//! | Module | Purpose |
//! | --- | --- |
//! | [`config`] | the runtime view of `[break_glass]` |
//! | [`signing`] | the canonical bytes that an approval signature covers |
//! | [`gate`] | find an approved proposal and stamp it consumed |
//! | [`handlers`] | api keys 1017, 1018, and 1019 |
//! | [`metrics`] | the `break_glass_*` metric families |
//! | [`sweep`] | the background reaper for expired proposals |
//!
//! # The action vocabulary
//!
//! [`krabka_metadata::BreakGlassAction`] is the one definition of the gated
//! set. This module maps it to the two forms the rest of the code needs: the
//! wire value that the private APIs carry, and the name that `signed_actions`,
//! the audit event, and the metric label all use. There is no second action
//! enum. [`crate::metrics::BreakGlassAction`] is a newtype over this one,
//! because the orphan rule keeps the metric-label trait off a foreign enum,
//! and it renders through [`action_name`].
//!
//! # Consumption is atomic with the transition
//!
//! [`gate::authorize`] returns the consumed proposal record. A metadata-backed
//! action puts it in the same `submit_change` call as its own records. A local
//! action commits it first through [`persistence`]. Both paths prevent the
//! action from starting while the approval remains reusable.

pub(crate) mod config;
#[cfg(test)]
mod cross_spend_model;
pub(crate) mod gate;
pub(crate) mod handlers;
pub(crate) mod metrics;
pub(crate) mod persistence;
pub(crate) mod signing;
#[cfg(test)]
mod state_model;
pub(crate) mod sweep;

use krabka_metadata::BreakGlassAction;

/// The wire value of a break-glass action, as the private APIs carry it.
///
/// The values start at one, in the declaration order of
/// [`krabka_metadata::BreakGlassAction`]. Zero names no action, so the default
/// request that a client builds with no action set does not decode as a real
/// transition.
pub(crate) fn action_to_wire(action: BreakGlassAction) -> i8 {
    match action {
        BreakGlassAction::ThawTopicFreeze => 1,
        BreakGlassAction::UncleanElectLeaders => 2,
        BreakGlassAction::UncleanRecovery => 3,
        BreakGlassAction::UnregisterBroker => 4,
        BreakGlassAction::CancelReassignment => 5,
        BreakGlassAction::DeleteTopic => 6,
        BreakGlassAction::DeleteRecords => 7,
    }
}

/// The action that a wire value names, or `None` when no action takes it.
pub(crate) fn action_from_wire(value: i8) -> Option<BreakGlassAction> {
    match value {
        1 => Some(BreakGlassAction::ThawTopicFreeze),
        2 => Some(BreakGlassAction::UncleanElectLeaders),
        3 => Some(BreakGlassAction::UncleanRecovery),
        4 => Some(BreakGlassAction::UnregisterBroker),
        5 => Some(BreakGlassAction::CancelReassignment),
        6 => Some(BreakGlassAction::DeleteTopic),
        7 => Some(BreakGlassAction::DeleteRecords),
        _ => None,
    }
}

/// The name of a break-glass action, in one spelling for every surface.
///
/// `break_glass.signed_actions` names an action with this string, the audit
/// event carries it, and the `break_glass_refusals` and `break_glass_bypassed`
/// metric families label with it. One spelling keeps an operator from having to
/// learn a second one.
pub(crate) fn action_name(action: BreakGlassAction) -> &'static str {
    match action {
        BreakGlassAction::ThawTopicFreeze => "thaw_topic_freeze",
        BreakGlassAction::UncleanElectLeaders => "unclean_elect_leaders",
        BreakGlassAction::UncleanRecovery => "unclean_recovery",
        BreakGlassAction::UnregisterBroker => "unregister_broker",
        BreakGlassAction::CancelReassignment => "cancel_reassignment",
        BreakGlassAction::DeleteTopic => "delete_topic",
        BreakGlassAction::DeleteRecords => "delete_records",
    }
}

/// The action that `name` spells, if it spells one.
///
/// This is the inverse of [`action_name`], and it reads the same table, so a
/// new action cannot be spelled one way here and another way there. The
/// configuration layer uses it to refuse a `break_glass.signed_actions` entry
/// that names no action. A misspelled entry would otherwise match no action,
/// and the broker would demand no signature for the action the operator meant
/// to protect.
pub(crate) fn action_from_name(name: &str) -> Option<BreakGlassAction> {
    ALL_ACTIONS
        .into_iter()
        .find(|action| action_name(*action) == name)
}

/// Every gated action, in the order of the wire values.
pub(crate) const ALL_ACTIONS: [BreakGlassAction; 7] = [
    BreakGlassAction::ThawTopicFreeze,
    BreakGlassAction::UncleanElectLeaders,
    BreakGlassAction::UncleanRecovery,
    BreakGlassAction::UnregisterBroker,
    BreakGlassAction::CancelReassignment,
    BreakGlassAction::DeleteTopic,
    BreakGlassAction::DeleteRecords,
];

/// Whether the target of an action names one partition.
///
/// A partition target is `"<topic>-<partition>"`, and
/// [`gate::authorize`] lets a proposal on the bare topic name cover it. An
/// action that does not target a partition takes the exact target only. Without
/// this split a proposal to delete the topic `logs` would also cover the topic
/// `logs-2024`, because that name reads as partition 2024 of topic `logs`.
pub(crate) fn action_targets_partition(action: BreakGlassAction) -> bool {
    matches!(
        action,
        BreakGlassAction::UncleanElectLeaders
            | BreakGlassAction::UncleanRecovery
            | BreakGlassAction::CancelReassignment
            | BreakGlassAction::DeleteRecords
    )
}

#[cfg(test)]
mod tests {
    use {
        super::{
            ALL_ACTIONS, action_from_name, action_from_wire, action_name, action_targets_partition,
            action_to_wire,
        },
        crate::config::DEFAULT_BREAK_GLASS_SIGNED_ACTIONS,
        assert2::check,
        krabka_metadata::BreakGlassAction,
    };

    #[test]
    fn every_action_round_trips_through_its_wire_value() {
        for action in ALL_ACTIONS {
            let wire = action_to_wire(action);
            check!(
                action_from_wire(wire) == Some(action),
                "{}",
                action_name(action)
            );
            check!(wire >= 1, "{}", action_name(action));
        }
    }

    #[test]
    fn no_action_takes_the_zero_wire_value_that_a_default_request_carries() {
        check!(action_from_wire(0) == None);
        for value in [-1_i8, 8, 127, -128] {
            check!(action_from_wire(value) == None, "value {value}");
        }
    }

    #[test]
    fn every_action_has_its_own_wire_value_and_its_own_name() {
        for (index, action) in ALL_ACTIONS.iter().enumerate() {
            for other in &ALL_ACTIONS[index + 1..] {
                check!(action_to_wire(*action) != action_to_wire(*other));
                check!(action_name(*action) != action_name(*other));
            }
        }
    }

    #[test]
    fn the_default_signed_actions_name_actions_that_exist() {
        for name in DEFAULT_BREAK_GLASS_SIGNED_ACTIONS {
            check!(
                ALL_ACTIONS.iter().any(|a| action_name(*a) == *name),
                "signed action {name}"
            );
        }
    }

    #[test]
    fn only_the_partition_scoped_actions_take_a_partition_target() {
        let cases = [
            ("thaw a freeze", BreakGlassAction::ThawTopicFreeze, false),
            (
                "unclean election",
                BreakGlassAction::UncleanElectLeaders,
                true,
            ),
            ("unclean recovery", BreakGlassAction::UncleanRecovery, true),
            (
                "unregister a broker",
                BreakGlassAction::UnregisterBroker,
                false,
            ),
            (
                "cancel a reassignment",
                BreakGlassAction::CancelReassignment,
                true,
            ),
            ("delete a topic", BreakGlassAction::DeleteTopic, false),
            ("delete records", BreakGlassAction::DeleteRecords, true),
        ];
        for (label, action, expected) in cases {
            check!(action_targets_partition(action) == expected, "case {label}");
        }
    }

    #[test]
    fn every_action_name_reads_back_as_its_action() {
        for action in ALL_ACTIONS {
            let name = action_name(action);
            check!(action_from_name(name) == Some(action), "{name}");
        }
    }

    #[test]
    fn a_name_that_spells_no_action_reads_back_as_none() {
        for (label, name) in [
            ("a plural misspelling", "delete_topics"),
            ("a hyphenated spelling", "delete-topic"),
            ("a capitalised spelling", "Delete_Topic"),
            ("an empty name", ""),
            ("a name with trailing space", "delete_topic "),
            ("an invented action", "reformat_cluster"),
        ] {
            check!(action_from_name(name).is_none(), "case {label}");
        }
    }
}
