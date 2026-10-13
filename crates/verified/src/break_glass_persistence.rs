//! Break-glass consumption and local-action persistence decisions.

use creusot_std::prelude::*;

/// How the incoming consume relates to the committed proposal image.
#[krabka_macros::model_type]
pub enum BreakGlassProposalState {
    Missing,
    Stale,
    ExactPending,
}

/// Facts checked before a consumed proposal enters the metadata log.
#[krabka_macros::model_type]
pub struct BreakGlassConsumptionFacts {
    pub proposal: BreakGlassProposalState,
    pub consumed_at_ms: i64,
    pub uncommitted_tail: bool,
}

/// Why a consumed proposal may or may not enter the metadata log.
#[krabka_macros::model_type]
pub enum BreakGlassConsumptionDecision {
    Missing,
    Malformed,
    Stale,
    InFlight,
    Append,
}

/// Admit only an exact, positive-timestamp mutation of the current pending
/// proposal, with no uncommitted metadata tail.
///
/// Every refusal reports the first failing check, in this order: a missing
/// proposal, a nonpositive consumption timestamp (`0` is the unconsumed
/// sentinel), a stale proposal, then an uncommitted tail.
#[ensures((result == BreakGlassConsumptionDecision::Missing) ==
    (facts.proposal == BreakGlassProposalState::Missing))]
#[ensures((result == BreakGlassConsumptionDecision::Malformed) ==
    (facts.proposal != BreakGlassProposalState::Missing && facts.consumed_at_ms@ <= 0))]
#[ensures((result == BreakGlassConsumptionDecision::Stale) ==
    (facts.proposal == BreakGlassProposalState::Stale && facts.consumed_at_ms@ > 0))]
#[ensures((result == BreakGlassConsumptionDecision::InFlight) == (
    facts.proposal == BreakGlassProposalState::ExactPending
        && facts.consumed_at_ms@ > 0
        && facts.uncommitted_tail
))]
#[ensures((result == BreakGlassConsumptionDecision::Append) == (
    facts.proposal == BreakGlassProposalState::ExactPending
        && facts.consumed_at_ms@ > 0
        && !facts.uncommitted_tail
))]
#[must_use]
pub fn break_glass_consumption_decision(
    facts: BreakGlassConsumptionFacts,
) -> BreakGlassConsumptionDecision {
    match facts.proposal {
        BreakGlassProposalState::Missing => BreakGlassConsumptionDecision::Missing,
        BreakGlassProposalState::Stale => {
            if facts.consumed_at_ms <= 0 {
                BreakGlassConsumptionDecision::Malformed
            } else {
                BreakGlassConsumptionDecision::Stale
            }
        }
        BreakGlassProposalState::ExactPending => {
            if facts.consumed_at_ms <= 0 {
                BreakGlassConsumptionDecision::Malformed
            } else if facts.uncommitted_tail {
                BreakGlassConsumptionDecision::InFlight
            } else {
                BreakGlassConsumptionDecision::Append
            }
        }
    }
}

/// Durable-spend state for an action outside the metadata log.
#[krabka_macros::model_type]
pub enum BreakGlassLocalSpendState {
    Ungated,
    MissingOrMismatched,
    Pending,
    Committed,
}

/// Facts checked before an action outside the metadata log may start.
#[krabka_macros::model_type]
pub struct BreakGlassLocalActionFacts {
    pub spend: BreakGlassLocalSpendState,
    pub commit_succeeded: bool,
}

/// Whether a privileged local action may start.
#[krabka_macros::model_type]
pub enum BreakGlassLocalActionDecision {
    Reject,
    Apply,
}

/// Permit an ungated action, a retry whose spend already committed, or an
/// exactly bound consume whose commit just succeeded. Every other order fails
/// closed.
#[ensures((result == BreakGlassLocalActionDecision::Apply) == (
    facts.spend == BreakGlassLocalSpendState::Ungated
        || facts.spend == BreakGlassLocalSpendState::Committed
        || (facts.spend == BreakGlassLocalSpendState::Pending && facts.commit_succeeded)
))]
#[must_use]
pub fn break_glass_local_action_decision(
    facts: BreakGlassLocalActionFacts,
) -> BreakGlassLocalActionDecision {
    match facts.spend {
        BreakGlassLocalSpendState::Ungated | BreakGlassLocalSpendState::Committed => {
            BreakGlassLocalActionDecision::Apply
        }
        BreakGlassLocalSpendState::Pending if facts.commit_succeeded => {
            BreakGlassLocalActionDecision::Apply
        }
        BreakGlassLocalSpendState::MissingOrMismatched | BreakGlassLocalSpendState::Pending => {
            BreakGlassLocalActionDecision::Reject
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BreakGlassConsumptionDecision, BreakGlassConsumptionFacts, BreakGlassLocalActionDecision,
        BreakGlassLocalActionFacts, BreakGlassLocalSpendState, BreakGlassProposalState,
        break_glass_consumption_decision, break_glass_local_action_decision,
    };

    #[test]
    fn consumption_requires_the_exact_current_proposal_and_no_uncommitted_tail() {
        let valid = BreakGlassConsumptionFacts {
            proposal: BreakGlassProposalState::ExactPending,
            consumed_at_ms: i64::MAX,
            uncommitted_tail: false,
        };
        assert2::check!(
            break_glass_consumption_decision(valid) == BreakGlassConsumptionDecision::Append
        );
        for (facts, expected) in [
            (
                BreakGlassConsumptionFacts {
                    proposal: BreakGlassProposalState::Missing,
                    ..valid
                },
                BreakGlassConsumptionDecision::Missing,
            ),
            (
                BreakGlassConsumptionFacts {
                    consumed_at_ms: 0,
                    ..valid
                },
                BreakGlassConsumptionDecision::Malformed,
            ),
            (
                BreakGlassConsumptionFacts {
                    proposal: BreakGlassProposalState::Missing,
                    consumed_at_ms: 0,
                    uncommitted_tail: true,
                },
                BreakGlassConsumptionDecision::Missing,
            ),
            (
                BreakGlassConsumptionFacts {
                    consumed_at_ms: -1,
                    uncommitted_tail: true,
                    ..valid
                },
                BreakGlassConsumptionDecision::Malformed,
            ),
            (
                BreakGlassConsumptionFacts {
                    proposal: BreakGlassProposalState::Stale,
                    consumed_at_ms: 0,
                    ..valid
                },
                BreakGlassConsumptionDecision::Malformed,
            ),
            (
                BreakGlassConsumptionFacts {
                    proposal: BreakGlassProposalState::Stale,
                    ..valid
                },
                BreakGlassConsumptionDecision::Stale,
            ),
            (
                BreakGlassConsumptionFacts {
                    proposal: BreakGlassProposalState::Stale,
                    uncommitted_tail: true,
                    ..valid
                },
                BreakGlassConsumptionDecision::Stale,
            ),
            (
                BreakGlassConsumptionFacts {
                    uncommitted_tail: true,
                    ..valid
                },
                BreakGlassConsumptionDecision::InFlight,
            ),
        ] {
            assert2::check!(break_glass_consumption_decision(facts) == expected);
        }
    }

    #[test]
    fn local_actions_run_only_after_a_matching_durable_spend() {
        for (facts, expected) in [
            (
                BreakGlassLocalActionFacts {
                    spend: BreakGlassLocalSpendState::Ungated,
                    commit_succeeded: false,
                },
                BreakGlassLocalActionDecision::Apply,
            ),
            (
                BreakGlassLocalActionFacts {
                    spend: BreakGlassLocalSpendState::Pending,
                    commit_succeeded: true,
                },
                BreakGlassLocalActionDecision::Apply,
            ),
            (
                BreakGlassLocalActionFacts {
                    spend: BreakGlassLocalSpendState::Committed,
                    commit_succeeded: false,
                },
                BreakGlassLocalActionDecision::Apply,
            ),
            (
                BreakGlassLocalActionFacts {
                    spend: BreakGlassLocalSpendState::Pending,
                    commit_succeeded: false,
                },
                BreakGlassLocalActionDecision::Reject,
            ),
            (
                BreakGlassLocalActionFacts {
                    spend: BreakGlassLocalSpendState::MissingOrMismatched,
                    commit_succeeded: true,
                },
                BreakGlassLocalActionDecision::Reject,
            ),
        ] {
            assert2::check!(break_glass_local_action_decision(facts) == expected);
        }
    }
}
