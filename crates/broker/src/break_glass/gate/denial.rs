//! The refusal a gated transition reports, and the rule that picks one refusal
//! out of several.
//!
//! A request can pass over more than one proposal that does not authorize it.
//! The gate answers with the one that came nearest, so an operator reads the
//! refusal they can act on rather than whichever record the image happened to
//! yield first.

use krabka_metadata::BreakGlassAction;
use uuid::Uuid;

use crate::break_glass::action_name;

/// A refused privileged transition, and the evidence for the refusal.
///
/// The caller picks the wire code from its own context, because the same
/// refusal reaches a Kafka client as `POLICY_VIOLATION` (44) and reaches the
/// private thaw path as `BREAK_GLASS_APPROVAL_REQUIRED` (1006). The `Display`
/// text is what a response carries as its `error_message`, and what the audit
/// event carries as its reason.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("break-glass refused {} on {}: {}", action_name(*.action), .target, .reason)]
pub(crate) struct BreakGlassDenial {
    /// The transition the caller asked for.
    pub action: BreakGlassAction,
    /// The target the caller asked for, in the form the caller uses.
    pub target: String,
    /// Why no approval authorized the transition.
    pub reason: DenialReason,
}

impl BreakGlassDenial {
    /// The proposal the refusal names, or `None` when no proposal covers the
    /// request.
    ///
    /// An audit event carries this id, so an operator can join the refusal to
    /// the proposal that nearly authorized it.
    pub(crate) fn proposal_id(&self) -> Option<Uuid> {
        self.reason.proposal_id()
    }
}

/// Why the gate refused a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DenialReason {
    /// No proposal names the action and covers the target.
    #[error("no approved proposal covers the request")]
    NoProposal,
    /// A proposal covers the request, and too few distinct principals approved
    /// it.
    #[error("proposal {proposal_id} holds {held} of {required} approvals")]
    NotEnoughApprovals {
        /// The proposal that is short of approvals.
        proposal_id: Uuid,
        /// Distinct principals that approved it.
        held: usize,
        /// Distinct principals it needs.
        required: usize,
    },
    /// The action needs a signed approval, and an approval carries none.
    #[error("proposal {proposal_id} carries an unsigned approval and the action needs a signature")]
    Unsigned {
        /// The proposal with the unsigned approval.
        proposal_id: Uuid,
    },
    /// The proposal passed its expiry time.
    #[error("proposal {proposal_id} expired at {expires_at_ms}")]
    Expired {
        /// The proposal that expired.
        proposal_id: Uuid,
        /// Epoch milliseconds at which it expired.
        expires_at_ms: i64,
    },
    /// An operator withdrew the proposal.
    #[error("proposal {proposal_id} is withdrawn")]
    Withdrawn {
        /// The withdrawn proposal.
        proposal_id: Uuid,
    },
    /// Another transition already spent the proposal.
    #[error("proposal {proposal_id} was consumed at {consumed_at_ms}")]
    Consumed {
        /// The spent proposal.
        proposal_id: Uuid,
        /// Epoch milliseconds at which it was spent.
        consumed_at_ms: i64,
    },
}

impl DenialReason {
    /// The covering proposal, absent when none covers the request.
    fn proposal_id(self) -> Option<Uuid> {
        match self {
            DenialReason::NoProposal => None,
            DenialReason::NotEnoughApprovals { proposal_id, .. }
            | DenialReason::Unsigned { proposal_id }
            | DenialReason::Expired { proposal_id, .. }
            | DenialReason::Withdrawn { proposal_id }
            | DenialReason::Consumed { proposal_id, .. } => Some(proposal_id),
        }
    }

    /// How near this proposal came to authorizing the transition. A lower rank
    /// is nearer.
    ///
    /// The gate reports the nearest refusal, because that is the one an
    /// operator can act on. A proposal one approval short is a better answer
    /// than a proposal somebody withdrew last week.
    fn rank(self) -> u8 {
        match self {
            DenialReason::NotEnoughApprovals { .. } => 0,
            DenialReason::Unsigned { .. } => 1,
            DenialReason::Expired { .. } => 2,
            DenialReason::Withdrawn { .. } => 3,
            DenialReason::Consumed { .. } => 4,
            DenialReason::NoProposal => 5,
        }
    }

    /// The proposal this reason names, or the nil id for
    /// [`DenialReason::NoProposal`]. It breaks a tie between two reasons of one
    /// rank, so the answer does not depend on the image iteration order.
    fn tie_break(self) -> Uuid {
        self.proposal_id().unwrap_or_else(Uuid::nil)
    }
}

/// The refusal to report when more than one covering proposal is unusable.
pub(super) fn nearer_reason(
    current: Option<DenialReason>,
    candidate: DenialReason,
) -> DenialReason {
    match current {
        None => candidate,
        Some(current) => {
            if (candidate.rank(), candidate.tie_break()) < (current.rank(), current.tie_break()) {
                candidate
            } else {
                current
            }
        }
    }
}
