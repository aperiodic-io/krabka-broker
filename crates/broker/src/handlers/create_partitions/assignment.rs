//! Replica placement for the partitions that `CreatePartitions` adds: the
//! site-aware automatic placement, and the validation of an explicit
//! `assignments` list against the live broker set and the topic's
//! replication factor.

use {
    crate::{
        codes,
        handlers::create_topics::validate_manual_partition_assignment,
        site_placement::{
            PlacementRng, SiteBrokerView, placement_failure_reason, stretch_replicas,
        },
    },
    krabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment,
    krabka_raft::NodeId,
};

#[cfg(test)]
mod tests;

/// Resolve the replica list for each newly-added partition.
///
/// `provided` is the caller's `assignments` field. `None` selects the
/// automatic site-aware placement. `Some(...)` is used verbatim, after each
/// list passes Kafka's `validateManualPartitionAssignment` against the
/// registered `brokers` and the topic's replication factor `rf`. The caller
/// has already checked that it holds one list per new partition.
///
/// `new_partition_count` is `new_count - existing`. It is always above 0 by
/// the time this helper runs, because the `INVALID_PARTITIONS` check runs
/// earlier.
///
/// On the automatic path the helper places just the new partitions, with a
/// fresh random start. Kafka's `createPartitions` does the same: it hands the
/// placer `numPartitions = additional` and the placer ignores the index of the
/// first new partition, so the new partitions do not continue the rotation of
/// the existing ones.
///
/// It returns one replica list per new partition, in `existing..new_count`
/// order. It returns an `(error_code, error_message)` pair with Kafka's
/// message instead when the request is invalid, and the caller stamps that
/// pair into the per-topic result.
pub(super) fn resolve_new_partition_assignments(
    provided: Option<&Vec<CreatePartitionsAssignment>>,
    brokers: &[SiteBrokerView],
    new_partition_count: usize,
    rf: i16,
    preferred_site: Option<&str>,
    rng: &mut PlacementRng,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    if let Some(provided) = provided {
        let registered: Vec<NodeId> = brokers.iter().map(|broker| broker.node_id).collect();
        let replication_factor = usize::try_from(rf).ok();
        return provided
            .iter()
            .map(|assignment| {
                validate_manual_partition_assignment(
                    &assignment.broker_ids,
                    &registered,
                    replication_factor,
                )
                .map_err(|message| (codes::INVALID_REPLICA_ASSIGNMENT, message))
            })
            .collect();
    }
    let placed = stretch_replicas(
        brokers,
        i32::try_from(new_partition_count).unwrap_or(i32::MAX),
        rf,
        preferred_site,
        rng,
    );
    if placed.is_empty() {
        // Kafka's `createPartitions` lets the placer's message through as it
        // is, with no "Unable to replicate" prefix.
        return Err((
            codes::INVALID_REPLICATION_FACTOR,
            placement_failure_reason(rf, brokers),
        ));
    }
    Ok(placed)
}
