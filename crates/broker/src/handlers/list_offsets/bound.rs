//! The one offset every `ListOffsets` answer is measured against.
//!
//! Kafka's `Partition.fetchOffsetForTimestamp` opens by choosing a
//! `lastFetchableOffset` and then uses it twice over: LATEST *is* that offset,
//! and every sentinel that resolves against record data is refused when it
//! lands at or above it. [`fetch_bound`] makes that choice from the request,
//! and [`last_fetchable_offset`] reads the offset it names.

use crate::partition::Partition;

/// Request `replica_id` (-1) that marks an ordinary client. Kafka's
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`. A follower sends its own node id
/// and the offset-debugging path sends -2.
#[cfg(test)]
const CONSUMER_REPLICA_ID: i32 = -1;
/// Request `isolation_level` (1) asking for committed data only. Kafka's
/// `IsolationLevel.READ_COMMITTED`; 0 is `READ_UNCOMMITTED`. `Fetch` reads the
/// same field the same way.
#[cfg(test)]
const READ_COMMITTED: i8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FetchBound {
    replica_id: i32,
    isolation_level: i8,
}

/// The bound a request's `replica_id` and `isolation_level` select, one arm per
/// line of `ReplicaManager.fetchOffset`'s `isolationLevelOpt`.
///
/// Kafka passes the isolation level down only when `replicaId ==
/// ListOffsetsRequest.CONSUMER_REPLICA_ID` (-1), and passes `None` for every
/// other `replicaId`. A follower probing its leader must not be bounded by
/// either watermark: it replicates committed and uncommitted records alike, so
/// bounding it would stall replication behind an open transaction until that
/// transaction resolved. `ListOffsetsRequest.DEBUGGING_REPLICA_ID` (-2) is not
/// -1 either, so it files with the replicas even though no follower sends it.
pub(super) const fn fetch_bound(replica_id: i32, isolation_level: i8) -> FetchBound {
    FetchBound {
        replica_id,
        isolation_level,
    }
}

impl FetchBound {
    /// The request's `replica_id`, unpacked for the leadership gate. Kafka's
    /// `ListOffsetsRequest.DEBUGGING_REPLICA_ID` (-2) is the one value the
    /// gate lets read a follower's local log instead of redirecting to the
    /// leader.
    pub(super) const fn replica_id(self) -> i32 {
        self.replica_id
    }
}

/// Reads the offset a [`FetchBound`] names.
///
/// The high watermark keeps a client's answer inside what the ISR has
/// acknowledged, and the last stable offset additionally pins a
/// `read_committed` client in front of the oldest transaction that has not
/// resolved, or whose marker the high watermark has not passed. Kafka's
/// `UnifiedLog.lastStableOffset` is itself
/// `min(first unstable offset, high watermark)`, so the
/// minimum below is what makes [`FetchBound::LastStable`] that same value
/// rather than a first-offset that could sit above the watermark.
///
/// Both accessors are the ones [`krabka_verified::fetch_visibility`] uses to
/// compute a `Fetch` response's `last_stable_offset`, so a consumer that seeks
/// to end and then fetches from there sees one consistent end of partition by
/// construction rather than by coincidence.
///
/// The high watermark lives behind an async mutex of its own, so this is called
/// before the log mutex is taken and never under it.
pub(super) async fn last_fetchable_offset(partition: &Partition, bound: FetchBound) -> Option<i64> {
    fetchable_offsets(partition, bound)
        .await
        .map(|offsets| offsets.last_fetchable)
}

/// The bound a request is measured against, and the high watermark read in
/// the same pass.
///
/// `Partition.fetchOffsetForTimestamp` needs both. The bound decides which
/// resolved offsets a client may be handed, while KIP-207's
/// `maybeOffsetsError` compares the leader epoch's start offset against the
/// high watermark whatever the isolation level, so a `read_committed` client
/// is not fenced merely because an open transaction holds its last stable
/// offset below the epoch start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FetchableOffsets {
    /// Kafka's `lastFetchableOffset` for the request's [`FetchBound`].
    pub(super) last_fetchable: i64,
    /// The partition's high watermark.
    pub(super) high_watermark: i64,
}

/// Reads the [`FetchableOffsets`] of `partition` for `bound`, or `None` when
/// an offset is malformed. See [`last_fetchable_offset`] for the bound.
pub(super) async fn fetchable_offsets(
    partition: &Partition,
    bound: FetchBound,
) -> Option<FetchableOffsets> {
    let high_watermark = partition.high_watermark().await;
    let last_stable = partition.last_stable_offset(high_watermark).0;
    let high_watermark = high_watermark.0;
    let log_end = partition.log_end_offset().0;
    checked_bound(bound, log_end, high_watermark, last_stable).map(|last_fetchable| {
        FetchableOffsets {
            last_fetchable,
            high_watermark,
        }
    })
}

fn checked_bound(
    bound: FetchBound,
    log_end: i64,
    high_watermark: i64,
    last_stable: i64,
) -> Option<i64> {
    match krabka_verified::list_offsets_bound_decision(krabka_verified::ListOffsetsBoundFacts {
        replica_id: bound.replica_id,
        isolation_level: bound.isolation_level,
        log_end,
        high_watermark,
        last_stable,
    }) {
        krabka_verified::ListOffsetsBoundDecision::RejectMalformed => None,
        krabka_verified::ListOffsetsBoundDecision::Bound { offset } => Some(offset),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Kafka reads a `ListOffsets` request's `isolation_level` only when the
    /// request came from a client, so choosing the bound that decides every
    /// answer has to read `replica_id` as well as `isolation_level`. Every row
    /// here is one line of `ReplicaManager.fetchOffset`'s `isolationLevelOpt`
    /// paired with the arm of `Partition.fetchOffsetForTimestamp`'s
    /// `isolationLevel match` it lands in.
    #[test]
    fn the_bound_follows_the_replica_id_before_the_isolation_level() {
        let cases = [
            // The transactional consumer this whole path exists for. It is the
            // one shape that must stop in front of an open transaction.
            (
                "read_committed consumer",
                CONSUMER_REPLICA_ID,
                READ_COMMITTED,
                FetchBound {
                    replica_id: CONSUMER_REPLICA_ID,
                    isolation_level: READ_COMMITTED,
                },
            ),
            // The same client asking for everything. It still cannot see past
            // what the ISR acknowledged, which is the divergence this bound
            // closed: the log end offset would have handed it records no
            // fetch would serve.
            (
                "read_uncommitted consumer",
                CONSUMER_REPLICA_ID,
                0,
                FetchBound {
                    replica_id: CONSUMER_REPLICA_ID,
                    isolation_level: 0,
                },
            ),
            // A follower probing its leader. Kafka discards the byte for any
            // replica id, and it has to: a replica copies uncommitted records
            // too, so bounding it would stall replication behind an open
            // transaction until that transaction resolved.
            (
                "follower replica",
                3,
                READ_COMMITTED,
                FetchBound {
                    replica_id: 3,
                    isolation_level: READ_COMMITTED,
                },
            ),
            // The same follower with the byte it actually sends. `None` is
            // chosen by the replica id alone, so the isolation level cannot
            // reach the answer either way.
            (
                "follower replica, read_uncommitted",
                3,
                0,
                FetchBound {
                    replica_id: 3,
                    isolation_level: 0,
                },
            ),
            // `ListOffsetsRequest.DEBUGGING_REPLICA_ID`. It is not -1, so Kafka
            // files it with the replicas rather than with the clients even
            // though no follower sends it.
            (
                "debugging replica",
                -2,
                READ_COMMITTED,
                FetchBound {
                    replica_id: -2,
                    isolation_level: READ_COMMITTED,
                },
            ),
        ];
        for (label, replica_id, isolation_level, expected) in cases {
            check!(
                fetch_bound(replica_id, isolation_level) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn selected_bound_fails_closed_without_rejecting_an_unused_tier_fact() {
        let consumer = fetch_bound(CONSUMER_REPLICA_ID, 0);
        check!(checked_bound(consumer, 0, 6, 0) == Some(6));
        check!(checked_bound(consumer, 0, -1, 0).is_none());

        let replica = fetch_bound(3, READ_COMMITTED);
        check!(checked_bound(replica, 7, -1, -1) == Some(7));
        check!(checked_bound(replica, -1, 0, 0).is_none());
    }
}
