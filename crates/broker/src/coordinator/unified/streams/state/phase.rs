//! The KIP-1071 group lifecycle phase and the Kafka group-state string it
//! serializes to.
//!
//! The phase is what `DescribeGroups`, `ListGroups`, and the admin tools read,
//! so its string mapping is a wire-visible contract and is kept apart from the
//! state machine that sets it.

/// The KIP-1071 group lifecycle state.
///
/// Its `as_str` is the Kafka group-state string `DescribeGroups`,
/// `ListGroups`, and the admin tools read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, krabka_macros::EnumStr)]
pub enum StreamsGroupStatePhase {
    /// No members.
    #[default]
    Empty,
    /// The group has members but cannot be assigned yet. Usually no topology
    /// is initialized, or required internal topics are missing.
    NotReady,
    /// A reconcile is in flight computing a new target assignment.
    Assigning,
    /// A target exists, and members converge on it by revoking and
    /// installing tasks.
    Reconciling,
    /// All members are at the assignment epoch with no pending revocations.
    Stable,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[yare::parameterized(
        empty = { StreamsGroupStatePhase::Empty, "Empty" },
        not_ready = { StreamsGroupStatePhase::NotReady, "NotReady" },
        assigning = { StreamsGroupStatePhase::Assigning, "Assigning" },
        reconciling = { StreamsGroupStatePhase::Reconciling, "Reconciling" },
        stable = { StreamsGroupStatePhase::Stable, "Stable" },
    )]
    fn group_state_phase_as_str_strings(phase: StreamsGroupStatePhase, want: &str) {
        assert2::assert!(phase.as_str() == want);
    }

    #[test]
    fn default_group_state_phase_is_empty() {
        assert!(StreamsGroupStatePhase::default() == StreamsGroupStatePhase::Empty);
    }
}
