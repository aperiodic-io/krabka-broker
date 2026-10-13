//! The lifecycle state machine of one remote log segment.
//!
//! [`RemoteLogSegmentState`] holds the four states a segment moves through and
//! the single rule that decides which move is legal, so every metadata update
//! is checked against one place.

use krabka_verified::storage::{RemoteSegmentLifecycle, remote_segment_transition};

/// Lifecycle state of a remote log segment.
///
/// Valid transitions (see [`RemoteLogSegmentState::is_valid_transition`]),
/// plus every self transition:
///
/// ```text
/// CopySegmentStarted ──► CopySegmentFinished ──► DeleteSegmentStarted ──► DeleteSegmentFinished
///         └───────────────────────────────────►┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteLogSegmentState {
    /// A copy to the remote tier has begun but not finished. The starting
    /// state of every segment.
    CopySegmentStarted,
    /// The copy finished; the segment is durable in the remote tier and
    /// readable.
    CopySegmentFinished,
    /// Deletion from the remote tier has begun.
    DeleteSegmentStarted,
    /// The segment has been fully removed from the remote tier.
    DeleteSegmentFinished,
}

impl RemoteLogSegmentState {
    /// `true` if a segment currently in `self` may transition to `target`.
    ///
    /// This is Kafka's `RemoteLogSegmentState.isValidTransition`, so a
    /// same-state transition is valid: Kafka admits it to keep retries and
    /// failover idempotent.
    #[must_use]
    pub fn is_valid_transition(self, target: Self) -> bool {
        remote_segment_transition(self.lifecycle(), target.lifecycle())
    }

    /// The proof kernels' view of this state.
    pub(crate) const fn lifecycle(self) -> RemoteSegmentLifecycle {
        match self {
            Self::CopySegmentStarted => RemoteSegmentLifecycle::CopyStarted,
            Self::CopySegmentFinished => RemoteSegmentLifecycle::CopyFinished,
            Self::DeleteSegmentStarted => RemoteSegmentLifecycle::DeleteStarted,
            Self::DeleteSegmentFinished => RemoteSegmentLifecycle::DeleteFinished,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    krabka_macros::remote_segment_transition_matrix!(expected_transitions);

    #[test]
    fn segment_state_transition_matrix_is_exhaustive() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        let states = [
            CopySegmentStarted,
            CopySegmentFinished,
            DeleteSegmentStarted,
            DeleteSegmentFinished,
        ];
        // Kafka's `RemoteLogSegmentState.isValidTransition`, row = source.
        let expected = expected_transitions();
        for (from_index, from) in states.into_iter().enumerate() {
            for (to_index, to) in states.into_iter().enumerate() {
                check!(
                    from.is_valid_transition(to) == expected[from_index][to_index],
                    "{from:?} -> {to:?}"
                );
            }
        }
    }
}
