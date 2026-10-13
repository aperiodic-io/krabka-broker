//! The `break_glass_*` metric families, fed from the metadata image.
//!
//! Three families cover the workflow. `break_glass_proposals` is a gauge over
//! the four proposal states, and a watch on the metadata image refreshes it.
//! `break_glass_refusals` counts the gated transitions the broker refused for
//! want of an approval. `break_glass_bypassed` counts the privileged
//! transitions the broker did with no approval at all, and it is the family an
//! operator should alert on, because it counts data-losing transitions that no
//! second person agreed to.
//!
//! Every label set is bounded. The `action` label takes one of the seven action
//! names, and the `state` label takes one of the four state names.

use {
    crate::{
        break_glass::{config::BreakGlassPolicy, gate::distinct_approvers},
        config::BreakGlassConfig,
        metrics::{BreakGlassAction as ActionLabel, BreakGlassState, BrokerMetrics},
    },
    krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataImage},
};

/// Account one gated transition that the broker refused for want of an approved
/// proposal.
pub(crate) fn record_refusal(metrics: &BrokerMetrics, action: BreakGlassAction) {
    metrics.record_break_glass_refusal(ActionLabel(action));
}

/// Account one privileged transition that ran with no approved proposal.
///
/// The background unclean-recovery path is the only caller. It has nobody to
/// ask for an approval, so under the `audit-only` policy it runs and counts the
/// bypass here.
pub(crate) fn record_bypass(metrics: &BrokerMetrics, action: BreakGlassAction) {
    metrics.record_break_glass_bypass(ActionLabel(action));
}

/// Refresh the `break_glass_proposals` gauge from `image`.
///
/// An image watch calls this on every published image. Each of the four states
/// is set on every call, including the states with no proposal in them, so a
/// series that drops to zero reports zero instead of holding its last value.
///
/// A withdrawn proposal counts in no state. It is not pending, nothing approved
/// it into use, it did not expire, and no transition consumed it.
pub(crate) fn record_proposal_states(
    metrics: &BrokerMetrics,
    image: &MetadataImage,
    config: &BreakGlassConfig,
    now_ms: i64,
) {
    let policy = BreakGlassPolicy::new(config);
    let mut counts = [0_i64; 4];
    for proposal in image.break_glass_proposals() {
        if let Some(state) = proposal_state(policy, proposal, now_ms) {
            counts[state_index(state)] += 1;
        }
    }
    for state in BreakGlassState::ALL {
        metrics.record_break_glass_proposals(state, counts[state_index(state)]);
    }
}

/// The state a proposal counts in, or `None` when it counts in none.
fn proposal_state(
    policy: BreakGlassPolicy<'_>,
    proposal: &BreakGlassProposalRecord,
    now_ms: i64,
) -> Option<BreakGlassState> {
    if proposal.withdrawn {
        return None;
    }
    if proposal.consumed_at_ms != 0 {
        return Some(BreakGlassState::Consumed);
    }
    if now_ms >= proposal.expires_at_ms {
        return Some(BreakGlassState::Expired);
    }
    if distinct_approvers(proposal) >= policy.required_approvals() {
        Some(BreakGlassState::Approved)
    } else {
        Some(BreakGlassState::Pending)
    }
}

/// The slot a state takes in the counting array.
fn state_index(state: BreakGlassState) -> usize {
    match state {
        BreakGlassState::Pending => 0,
        BreakGlassState::Approved => 1,
        BreakGlassState::Expired => 2,
        BreakGlassState::Consumed => 3,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            break_glass::{
                ALL_ACTIONS, action_name,
                gate::tests::{EXPIRES_MS, NOW_MS, approval, config, image_of, proposal},
            },
            metrics::{BreakGlassActionLabel, BreakGlassStateLabel},
        },
        assert2::check,
        krabka_metadata::BreakGlassProposalRecord,
    };

    fn states_of(proposals: &[BreakGlassProposalRecord]) -> Vec<Option<BreakGlassState>> {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        proposals
            .iter()
            .map(|proposal| proposal_state(policy, proposal, NOW_MS))
            .collect()
    }

    fn gauge(metrics: &BrokerMetrics, state: BreakGlassState) -> i64 {
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel { state })
            .get()
    }

    /// The `break_glass_refusals` count that `action` labels.
    fn refusals(metrics: &BrokerMetrics, action: BreakGlassAction) -> u64 {
        metrics
            .break_glass_refusals
            .get_or_create(&BreakGlassActionLabel {
                action: ActionLabel(action),
            })
            .get()
    }

    /// The `break_glass_bypassed` count that `action` labels.
    fn bypasses(metrics: &BrokerMetrics, action: BreakGlassAction) -> u64 {
        metrics
            .break_glass_bypassed
            .get_or_create(&BreakGlassActionLabel {
                action: ActionLabel(action),
            })
            .get()
    }

    #[test]
    fn every_proposal_counts_in_the_state_of_its_lifecycle() {
        let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let cases = [
            (
                "one approval short",
                BreakGlassProposalRecord {
                    approvals: vec![approval("User:bob")],
                    ..base.clone()
                },
                Some(BreakGlassState::Pending),
            ),
            (
                "every approval in",
                base.clone(),
                Some(BreakGlassState::Approved),
            ),
            (
                "past its expiry",
                BreakGlassProposalRecord {
                    expires_at_ms: NOW_MS,
                    ..base.clone()
                },
                Some(BreakGlassState::Expired),
            ),
            (
                "spent on a transition",
                BreakGlassProposalRecord {
                    consumed_at_ms: NOW_MS - 1,
                    ..base.clone()
                },
                Some(BreakGlassState::Consumed),
            ),
            (
                "spent and then past its expiry",
                BreakGlassProposalRecord {
                    consumed_at_ms: NOW_MS - 1,
                    expires_at_ms: NOW_MS,
                    ..base.clone()
                },
                Some(BreakGlassState::Consumed),
            ),
            (
                "withdrawn",
                BreakGlassProposalRecord {
                    withdrawn: true,
                    ..base.clone()
                },
                None,
            ),
        ];
        for (label, proposal, expected) in cases {
            check!(states_of(&[proposal]) == vec![expected], "case {label}");
        }
    }

    #[test]
    fn the_gauge_counts_every_state_and_reports_the_empty_ones_as_zero() {
        let metrics = BrokerMetrics::new();
        let image = image_of(&[
            proposal(1, BreakGlassAction::DeleteTopic, "doomed"),
            proposal(2, BreakGlassAction::DeleteRecords, "orders-3"),
            BreakGlassProposalRecord {
                approvals: vec![approval("User:bob")],
                ..proposal(3, BreakGlassAction::DeleteTopic, "other")
            },
            BreakGlassProposalRecord {
                withdrawn: true,
                ..proposal(4, BreakGlassAction::DeleteTopic, "dropped")
            },
        ]);

        record_proposal_states(&metrics, &image, &config(), NOW_MS);

        let cases = [
            (BreakGlassState::Pending, 1),
            (BreakGlassState::Approved, 2),
            (BreakGlassState::Expired, 0),
            (BreakGlassState::Consumed, 0),
        ];
        for (state, expected) in cases {
            check!(gauge(&metrics, state) == expected, "{}", state.as_str());
        }
    }

    #[test]
    fn a_second_refresh_replaces_the_counts_it_found() {
        let metrics = BrokerMetrics::new();
        let full = image_of(&[proposal(1, BreakGlassAction::DeleteTopic, "doomed")]);
        record_proposal_states(&metrics, &full, &config(), NOW_MS);

        let empty = image_of(&[]);
        record_proposal_states(&metrics, &empty, &config(), NOW_MS);

        for state in BreakGlassState::ALL {
            check!(gauge(&metrics, state) == 0, "{}", state.as_str());
        }
    }

    #[test]
    fn a_refusal_and_a_bypass_count_under_the_action_they_name() {
        let metrics = BrokerMetrics::new();

        record_refusal(&metrics, BreakGlassAction::DeleteTopic);
        record_refusal(&metrics, BreakGlassAction::DeleteTopic);
        record_bypass(&metrics, BreakGlassAction::UncleanRecovery);

        check!(refusals(&metrics, BreakGlassAction::DeleteTopic) == 2);
        check!(refusals(&metrics, BreakGlassAction::UncleanRecovery) == 0);
        check!(bypasses(&metrics, BreakGlassAction::UncleanRecovery) == 1);
        check!(bypasses(&metrics, BreakGlassAction::DeleteTopic) == 0);
    }

    /// Every action counts in its own pair of series.
    ///
    /// Each action takes a different number of refusals, so an action whose
    /// label collided with another's reads back the wrong count rather than
    /// passing. The loop runs over [`ALL_ACTIONS`], so an action added to the
    /// metadata enum is counted here the day it exists.
    #[test]
    fn every_action_counts_in_its_own_pair_of_series() {
        let metrics = BrokerMetrics::new();
        for (index, action) in ALL_ACTIONS.into_iter().enumerate() {
            for _ in 0..=index {
                record_refusal(&metrics, action);
            }
            record_bypass(&metrics, action);
        }

        for (index, action) in ALL_ACTIONS.into_iter().enumerate() {
            let expected = u64::try_from(index).expect("a seven-element index") + 1;
            let name = action_name(action);
            check!(refusals(&metrics, action) == expected, "{name}");
            check!(bypasses(&metrics, action) == 1, "{name}");
        }
    }

    #[test]
    fn a_proposal_that_expires_exactly_now_counts_as_expired() {
        let expiring = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let config = config();
        let policy = BreakGlassPolicy::new(&config);

        check!(proposal_state(policy, &expiring, NOW_MS) == Some(BreakGlassState::Approved));
        check!(proposal_state(policy, &expiring, EXPIRES_MS) == Some(BreakGlassState::Expired));
    }
}
