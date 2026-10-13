//! The partition-delete lifecycle.
//!
//! [`RemotePartitionDeleteMetadata`] records how far the deletion of one
//! partition's remote data has got, and [`RemotePartitionDeleteState`] holds
//! the three states plus the rule for moving between them. This lifecycle is
//! separate from the per-segment one in
//! [`RemoteLogSegmentState`](crate::metadata::RemoteLogSegmentState).

use {
    crate::metadata::TopicIdPartition,
    krabka_verified::storage::{
        RemotePartitionDeleteLifecycle, remote_partition_delete_transition,
    },
};

/// Lifecycle state of a remote *partition* deletion.
///
/// ```text
/// DeletePartitionMarked ──► DeletePartitionStarted ──► DeletePartitionFinished
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemotePartitionDeleteState {
    /// The partition has been marked for deletion of all its remote
    /// segments.
    DeletePartitionMarked,
    /// Deletion of the partition's remote segments has begun.
    DeletePartitionStarted,
    /// All remote segments for the partition have been deleted.
    DeletePartitionFinished,
}

impl RemotePartitionDeleteState {
    /// `true` if a partition currently in `from` may transition to `target`.
    ///
    /// A `from` of `None` means the partition was never marked. This is
    /// Kafka's `RemotePartitionDeleteState.isValidTransition`, so a same-state
    /// transition is valid: Kafka admits it to keep retries and failover
    /// idempotent.
    #[must_use]
    pub fn is_valid_transition(from: Option<Self>, target: Self) -> bool {
        remote_partition_delete_transition(from.map(Self::lifecycle), target.lifecycle())
    }

    const fn lifecycle(self) -> RemotePartitionDeleteLifecycle {
        match self {
            Self::DeletePartitionMarked => RemotePartitionDeleteLifecycle::Marked,
            Self::DeletePartitionStarted => RemotePartitionDeleteLifecycle::Started,
            Self::DeletePartitionFinished => RemotePartitionDeleteLifecycle::Finished,
        }
    }
}

/// Metadata describing the deletion lifecycle of a partition's remote data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePartitionDeleteMetadata {
    /// The partition being deleted from the remote tier.
    pub topic_id_partition: TopicIdPartition,
    /// Current deletion state.
    pub state: RemotePartitionDeleteState,
    /// Wall-clock time of this event.
    pub event_timestamp_ms: i64,
    /// Broker that produced this metadata.
    pub broker_id: i32,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn partition_delete_transition_matrix_is_exhaustive() {
        use RemotePartitionDeleteState::{
            DeletePartitionFinished, DeletePartitionMarked, DeletePartitionStarted,
        };
        let from_states = [
            None,
            Some(DeletePartitionMarked),
            Some(DeletePartitionStarted),
            Some(DeletePartitionFinished),
        ];
        let targets = [
            DeletePartitionMarked,
            DeletePartitionStarted,
            DeletePartitionFinished,
        ];
        // Kafka's `RemotePartitionDeleteState.isValidTransition`, row =
        // source, the first row being no prior state.
        let expected = [
            [true, false, false],
            [true, true, false],
            [false, true, true],
            [false, false, true],
        ];
        for (from_index, from) in from_states.into_iter().enumerate() {
            for (to_index, to) in targets.into_iter().enumerate() {
                check!(
                    RemotePartitionDeleteState::is_valid_transition(from, to)
                        == expected[from_index][to_index],
                    "{from:?} -> {to:?}"
                );
            }
        }
    }
}
