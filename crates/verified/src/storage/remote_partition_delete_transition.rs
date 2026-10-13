use {
    super::{RemoteCacheAction, RemotePartitionDeleteLifecycle, RemoteSegmentLifecycle},
    creusot_std::prelude::*,
};

/// The forward edges of Kafka's segment lifecycle:
/// `COPY_SEGMENT_STARTED -> COPY_SEGMENT_FINISHED | DELETE_SEGMENT_STARTED`,
/// `COPY_SEGMENT_FINISHED -> DELETE_SEGMENT_STARTED`, and
/// `DELETE_SEGMENT_STARTED -> DELETE_SEGMENT_FINISHED`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn segment_forward_edge(from: RemoteSegmentLifecycle, to: RemoteSegmentLifecycle) -> bool {
    match (from, to) {
        (RemoteSegmentLifecycle::CopyStarted, RemoteSegmentLifecycle::CopyFinished)
        | (RemoteSegmentLifecycle::CopyStarted, RemoteSegmentLifecycle::DeleteStarted)
        | (RemoteSegmentLifecycle::CopyFinished, RemoteSegmentLifecycle::DeleteStarted)
        | (RemoteSegmentLifecycle::DeleteStarted, RemoteSegmentLifecycle::DeleteFinished) => true,
        _ => false,
    }
}

/// Kafka's `RemoteLogSegmentState.isValidTransition` for a known source
/// state: a forward lifecycle edge, or a self transition, which Kafka admits
/// so retries and failover stay idempotent. A segment with no source state is
/// the host's add path, which admits only `CopyStarted`.
#[ensures(result == (from == to || segment_forward_edge(from, to)))]
#[must_use]
pub const fn remote_segment_transition(
    from: RemoteSegmentLifecycle,
    to: RemoteSegmentLifecycle,
) -> bool {
    matches!(
        (from, to),
        (
            RemoteSegmentLifecycle::CopyStarted,
            RemoteSegmentLifecycle::CopyStarted
                | RemoteSegmentLifecycle::CopyFinished
                | RemoteSegmentLifecycle::DeleteStarted
        ) | (
            RemoteSegmentLifecycle::CopyFinished,
            RemoteSegmentLifecycle::CopyFinished | RemoteSegmentLifecycle::DeleteStarted
        ) | (
            RemoteSegmentLifecycle::DeleteStarted,
            RemoteSegmentLifecycle::DeleteStarted | RemoteSegmentLifecycle::DeleteFinished
        ) | (
            RemoteSegmentLifecycle::DeleteFinished,
            RemoteSegmentLifecycle::DeleteFinished
        )
    )
}

/// Classify one remote-segment cache update against the cached state, `None`
/// when the cache holds no such segment.
///
/// A forward lifecycle edge stores its target: readable at `CopyFinished`,
/// hidden at `DeleteStarted`, removed at `DeleteFinished`. An update for a
/// missing segment is an idempotent tombstone when it is `DeleteFinished` and
/// rejected otherwise, so an update can never resurrect a segment.
///
/// Kafka's `RemoteLogMetadataCache` admits every self transition. This cache
/// is stricter: a self transition is a no-op only for an exact retry (the host
/// compares timestamp, broker and custom metadata) and is rejected as a
/// conflict otherwise.
#[ensures(result == match current {
    None => if target == RemoteSegmentLifecycle::DeleteFinished {
        RemoteCacheAction::Noop
    } else {
        RemoteCacheAction::Reject
    },
    Some(current) => if current == target {
        if exact_retry { RemoteCacheAction::Noop } else { RemoteCacheAction::Reject }
    } else if segment_forward_edge(current, target) {
        match target {
            RemoteSegmentLifecycle::CopyFinished => RemoteCacheAction::StoreFinished,
            RemoteSegmentLifecycle::DeleteFinished => RemoteCacheAction::Remove,
            _ => RemoteCacheAction::StoreHidden,
        }
    } else {
        RemoteCacheAction::Reject
    },
})]
#[must_use]
pub const fn remote_cache_action(
    current: Option<RemoteSegmentLifecycle>,
    target: RemoteSegmentLifecycle,
    exact_retry: bool,
) -> RemoteCacheAction {
    match (current, target) {
        (None, RemoteSegmentLifecycle::DeleteFinished) => RemoteCacheAction::Noop,
        (Some(RemoteSegmentLifecycle::CopyStarted), RemoteSegmentLifecycle::CopyStarted)
        | (Some(RemoteSegmentLifecycle::CopyFinished), RemoteSegmentLifecycle::CopyFinished)
        | (Some(RemoteSegmentLifecycle::DeleteStarted), RemoteSegmentLifecycle::DeleteStarted)
        | (Some(RemoteSegmentLifecycle::DeleteFinished), RemoteSegmentLifecycle::DeleteFinished) => {
            if exact_retry {
                RemoteCacheAction::Noop
            } else {
                RemoteCacheAction::Reject
            }
        }
        (Some(RemoteSegmentLifecycle::CopyStarted), RemoteSegmentLifecycle::CopyFinished) => {
            RemoteCacheAction::StoreFinished
        }
        (
            Some(RemoteSegmentLifecycle::CopyStarted | RemoteSegmentLifecycle::CopyFinished),
            RemoteSegmentLifecycle::DeleteStarted,
        ) => RemoteCacheAction::StoreHidden,
        (Some(RemoteSegmentLifecycle::DeleteStarted), RemoteSegmentLifecycle::DeleteFinished) => {
            RemoteCacheAction::Remove
        }
        _ => RemoteCacheAction::Reject,
    }
}

/// The forward edges of Kafka's partition-delete lifecycle:
/// `DELETE_PARTITION_MARKED -> DELETE_PARTITION_STARTED -> DELETE_PARTITION_FINISHED`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn partition_delete_forward_edge(
    from: RemotePartitionDeleteLifecycle,
    to: RemotePartitionDeleteLifecycle,
) -> bool {
    match (from, to) {
        (RemotePartitionDeleteLifecycle::Marked, RemotePartitionDeleteLifecycle::Started)
        | (RemotePartitionDeleteLifecycle::Started, RemotePartitionDeleteLifecycle::Finished) => {
            true
        }
        _ => false,
    }
}

/// Kafka's `RemotePartitionDeleteState.isValidTransition`: with no prior
/// state only `DELETE_PARTITION_MARKED` is admitted; otherwise a forward edge
/// or an idempotent self transition.
#[ensures(result == match from {
    None => to == RemotePartitionDeleteLifecycle::Marked,
    Some(from) => from == to || partition_delete_forward_edge(from, to),
})]
#[must_use]
pub const fn remote_partition_delete_transition(
    from: Option<RemotePartitionDeleteLifecycle>,
    to: RemotePartitionDeleteLifecycle,
) -> bool {
    matches!(
        (from, to),
        (None, RemotePartitionDeleteLifecycle::Marked)
            | (
                Some(RemotePartitionDeleteLifecycle::Marked),
                RemotePartitionDeleteLifecycle::Marked | RemotePartitionDeleteLifecycle::Started
            )
            | (
                Some(RemotePartitionDeleteLifecycle::Started),
                RemotePartitionDeleteLifecycle::Started | RemotePartitionDeleteLifecycle::Finished
            )
            | (
                Some(RemotePartitionDeleteLifecycle::Finished),
                RemotePartitionDeleteLifecycle::Finished
            )
    )
}
