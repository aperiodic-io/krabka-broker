//! Replica-directory assignment and controller-response decisions.

use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Change selected for one reported topic-partition replica.
    pub enum DirectoryAssignmentDecision {
        Ignore,
        NoOp,
        Assign(usize),
    }
}

/// Select only the reporting broker's exact replica slot, and suppress an
/// assignment that is already current.
#[ensures((result == DirectoryAssignmentDecision::Ignore) == (replica_slot == None))]
#[ensures((result == DirectoryAssignmentDecision::NoOp) ==
    (replica_slot != None && already_assigned))]
#[ensures(forall<slot: usize> result == DirectoryAssignmentDecision::Assign(slot) ==
    (replica_slot == Some(slot) && !already_assigned))]
#[must_use]
pub fn directory_assignment_decision(
    replica_slot: Option<usize>,
    already_assigned: bool,
) -> DirectoryAssignmentDecision {
    match replica_slot {
        None => DirectoryAssignmentDecision::Ignore,
        Some(_) if already_assigned => DirectoryAssignmentDecision::NoOp,
        Some(slot) => DirectoryAssignmentDecision::Assign(slot),
    }
}

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Classification of an assignment report's controller response.
    pub enum DirectoryResponseDecision {
        ControllerError,
        StaleController,
        Accept,
    }
}

/// Accept success only while the controller identity that answered remains
/// the current leader. A missing or changed leader makes the response stale.
#[ensures((result == DirectoryResponseDecision::ControllerError) == !response_ok)]
#[ensures((result == DirectoryResponseDecision::StaleController) ==
    (response_ok && current_controller != Some(sent_controller)))]
#[ensures((result == DirectoryResponseDecision::Accept) ==
    (response_ok && current_controller == Some(sent_controller)))]
#[must_use]
pub fn directory_response_decision(
    response_ok: bool,
    sent_controller: u64,
    current_controller: Option<u64>,
) -> DirectoryResponseDecision {
    if !response_ok {
        DirectoryResponseDecision::ControllerError
    } else if current_controller != Some(sent_controller) {
        DirectoryResponseDecision::StaleController
    } else {
        DirectoryResponseDecision::Accept
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            DirectoryAssignmentDecision, DirectoryResponseDecision, directory_assignment_decision,
            directory_response_decision,
        },
        assert2::check,
    };

    #[test]
    fn planning_is_exact_idempotent_and_response_fenced() {
        use {
            DirectoryAssignmentDecision::{Assign, Ignore, NoOp},
            DirectoryResponseDecision::{Accept, ControllerError, StaleController},
        };

        check!(directory_assignment_decision(None, false) == Ignore);
        check!(directory_assignment_decision(None, true) == Ignore);
        check!(directory_assignment_decision(Some(3), true) == NoOp);
        check!(directory_assignment_decision(Some(3), false) == Assign(3));

        check!(directory_response_decision(false, 7, Some(7)) == ControllerError);
        check!(directory_response_decision(true, 7, Some(8)) == StaleController);
        check!(directory_response_decision(true, 7, None) == StaleController);
        check!(directory_response_decision(true, 7, Some(7)) == Accept);
    }
}
