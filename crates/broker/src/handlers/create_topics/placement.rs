//! Replica placement for `CreateTopics`: the site-aware automatic placement
//! and the validation of an explicit `assignments` field. The handler asks
//! this module for the replica list of every partition of a new topic, and
//! it reports the error code that comes back on an assignment it rejects.

use krabka_protocol::owned::create_topics_request::CreatableTopic;

use crate::{
    codes,
    config_keys::resolve_broker_witness,
    cordoned_log_dirs::fully_cordoned_brokers,
    site_placement::{PlacementRng, SiteBrokerView, placement_failure_reason, stretch_replicas},
};

#[cfg(test)]
mod tests;

/// Kafka's `ReplicationControlManager.validateManualPartitionAssignment` on
/// one partition's replica list: the replicas in the order the client sent
/// them, or Kafka's `INVALID_REPLICA_ASSIGNMENT` message.
///
/// The brokers are checked in ascending id order, as Kafka sorts them, so the
/// message names the least unregistered or repeated broker.
/// `replication_factor` is the replica count of the partitions before this
/// one, which this one must match.
pub(crate) fn validate_manual_partition_assignment(
    broker_ids: &[i32],
    registered: &[krabka_raft::NodeId],
    replication_factor: Option<usize>,
) -> Result<Vec<krabka_raft::NodeId>, String> {
    if broker_ids.is_empty() {
        return Err("The manual partition assignment includes an empty replica list.".to_owned());
    }
    let mut sorted = broker_ids.to_vec();
    sorted.sort_unstable();
    let mut previous = None;
    for broker_id in sorted {
        let known =
            u64::try_from(broker_id).is_ok_and(|id| registered.contains(&krabka_raft::NodeId(id)));
        if !known {
            return Err(format!(
                "The manual partition assignment includes broker {broker_id}, but no such \
                 broker is registered."
            ));
        }
        if previous == Some(broker_id) {
            return Err(format!(
                "The manual partition assignment includes the broker {broker_id} more than once."
            ));
        }
        previous = Some(broker_id);
    }
    if let Some(expected) = replication_factor
        && broker_ids.len() != expected
    {
        return Err(format!(
            "The manual partition assignment includes a partition with {} replica(s), but \
             this is not consistent with previous partitions, which have {expected} replica(s).",
            broker_ids.len()
        ));
    }
    Ok(broker_ids
        .iter()
        .filter_map(|id| u64::try_from(*id).ok().map(krabka_raft::NodeId))
        .collect())
}

/// Kafka's `ReplicationControlManager.createTopic` checks on a manual
/// assignment, with its codes and messages: the replication factor and the
/// partition count must be -1, no partition may be assigned twice, each
/// replica list must pass [`validate_manual_partition_assignment`], and the
/// partitions must be `0..n`.
fn manual_replicas(
    topic: &CreatableTopic,
    brokers: &[krabka_raft::NodeId],
) -> Result<Vec<Vec<krabka_raft::NodeId>>, (i16, String)> {
    if topic.replication_factor != -1 {
        return Err((
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but replication factor was not set \
             to -1."
                .to_owned(),
        ));
    }
    if topic.num_partitions != -1 {
        return Err((
            codes::INVALID_REQUEST,
            "A manual partition assignment was specified, but numPartitions was not set to -1."
                .to_owned(),
        ));
    }
    let mut by_partition = std::collections::BTreeMap::new();
    let mut replication_factor = None;
    for assignment in &topic.assignments {
        if by_partition.contains_key(&assignment.partition_index) {
            return Err((
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "Found multiple manual partition assignments for partition {}",
                    assignment.partition_index
                ),
            ));
        }
        let replicas = validate_manual_partition_assignment(
            &assignment.broker_ids,
            brokers,
            replication_factor,
        )
        .map_err(|message| (codes::INVALID_REPLICA_ASSIGNMENT, message))?;
        replication_factor = Some(replicas.len());
        by_partition.insert(assignment.partition_index, replicas);
    }
    if by_partition
        .keys()
        .copied()
        .ne(0..i32::try_from(by_partition.len()).unwrap_or(i32::MAX))
    {
        return Err((
            codes::INVALID_REPLICA_ASSIGNMENT,
            "partitions should be a consecutive 0-based integer sequence".to_owned(),
        ));
    }
    Ok(by_partition.into_values().collect())
}

/// The `INVALID_REPLICATION_FACTOR` message of `CreateTopics` for a placement
/// that cannot put `replication_factor` replicas on `brokers`, as Kafka's
/// `createTopic` wraps the `StripedReplicaPlacer` refusal. `brokers` is what
/// the placement got: fenced brokers included.
///
/// `CreatePartitions` does not wrap it: Kafka's `createPartitions` lets the
/// placer's exception through, so its row carries [`placement_failure_reason`]
/// alone.
pub(crate) fn placement_failure_message(
    replication_factor: i16,
    brokers: &[SiteBrokerView],
) -> String {
    format!(
        "Unable to replicate the partition {replication_factor} time(s): {}",
        placement_failure_reason(replication_factor, brokers)
    )
}

/// The leader and the ISR a new partition starts with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InitialLeadership {
    pub(crate) leader: krabka_raft::NodeId,
    pub(crate) isr: Vec<krabka_raft::NodeId>,
}

/// The starting leadership of each partition an automatic placement made.
///
/// Kafka's `ReplicationControlManager.createTopic` builds the ISR from the
/// replicas that pass `ClusterControlManager.isActive`, so a fenced replica
/// stays in the replica list and out of the ISR. The placement never puts a
/// fenced broker or a witness first, so the first replica leads. `inactive` is
/// [`inactive_brokers`].
pub(crate) fn automatic_leaderships(
    assignments: &[Vec<krabka_raft::NodeId>],
    inactive: &std::collections::HashSet<u64>,
) -> Vec<InitialLeadership> {
    assignments
        .iter()
        .map(|replicas| InitialLeadership {
            leader: replicas[0],
            isr: replicas
                .iter()
                .copied()
                .filter(|replica| !inactive.contains(&replica.0))
                .collect(),
        })
        .collect()
}

/// The brokers that Kafka's `ClusterControlManager.isActive` refuses: the
/// `unavailable` ones (fenced or past their heartbeat deadline) and the ones in
/// controlled shutdown. `unavailable` is
/// [`crate::handlers::offline_replicas::unavailable_brokers`].
pub(crate) fn inactive_brokers(
    image: &krabka_metadata::MetadataImage,
    unavailable: &std::collections::HashSet<u64>,
) -> std::collections::HashSet<u64> {
    let mut inactive = unavailable.clone();
    inactive.extend(controlled_shutdown_brokers(image));
    inactive
}

/// The brokers whose registration says they are in controlled shutdown.
fn controlled_shutdown_brokers(
    image: &krabka_metadata::MetadataImage,
) -> impl Iterator<Item = u64> {
    image
        .brokers()
        .filter(|registration| registration.in_controlled_shutdown)
        .map(|registration| registration.node_id.0)
}

/// The starting leadership of each partition of a manual assignment.
///
/// Kafka's `ReplicationControlManager` builds the ISR of a manually assigned
/// partition from the listed brokers that are active (registered, not fenced
/// and not in controlled shutdown), in the listed order, and
/// `buildPartitionRegistration` makes the first of them the leader. The
/// replica list stays as the client sent it. `inactive` is
/// [`inactive_brokers`].
///
/// A krabka witness replicates but never leads (see
/// [`crate::site_placement`]), so the leader is the first active replica that
/// is not in `witnesses`. An active witness stays in the ISR.
///
/// `first_partition` is the index of the first partition in `assignments`:
/// 0 for `CreateTopics`, the current partition count for `CreatePartitions`.
/// The error is the `INVALID_REPLICA_ASSIGNMENT` message for the first
/// partition that has no active broker (Kafka's message), or no active broker
/// that may lead.
pub(crate) fn manual_leaderships(
    assignments: &[Vec<krabka_raft::NodeId>],
    inactive: &std::collections::HashSet<u64>,
    witnesses: &std::collections::HashSet<krabka_raft::NodeId>,
    first_partition: i32,
) -> Result<Vec<InitialLeadership>, String> {
    let mut partition = first_partition;
    let mut leaderships = Vec::with_capacity(assignments.len());
    for replicas in assignments {
        let isr = replicas
            .iter()
            .copied()
            .filter(|replica| !inactive.contains(&replica.0))
            .collect::<Vec<_>>();
        if isr.is_empty() {
            return Err(format!(
                "All brokers specified in the manual partition assignment for partition \
                 {partition} are fenced or in controlled shutdown."
            ));
        }
        let Some(leader) = isr
            .iter()
            .copied()
            .find(|replica| !witnesses.contains(replica))
        else {
            return Err(format!(
                "All active brokers specified in the manual partition assignment for partition \
                 {partition} are witnesses, and a witness cannot lead."
            ));
        };
        leaderships.push(InitialLeadership { leader, isr });
        partition = partition.saturating_add(1);
    }
    Ok(leaderships)
}

/// The brokers automatic placement leaves out, as Kafka's
/// `BrokerHeartbeatManager.UsableBrokerIterator` skips them: the ones in
/// controlled shutdown (`BrokerHeartbeatState.shuttingDown`) and, as
/// `ClusterControlManager.usableBrokers` filters on
/// `BrokerRegistration.hasUncordonedDirs`, every broker whose log directories
/// are all cordoned (KIP-1066).
///
/// A fenced broker is not left out. Kafka hands it to the placer as a last
/// resort, and [`site_broker_views`] marks it as fenced. This includes a
/// broker that finished its controlled shutdown. Its registration keeps
/// `InControlledShutdown`, because only a new registration clears that flag.
/// But `BrokerHeartbeatManager.touch` takes a fenced broker out of controlled
/// shutdown, so Kafka places replicas on the stopped broker as on any fenced
/// broker. If the placement read the flag alone, a topic created after a
/// clean stop would need one broker more than Kafka needs. `unavailable` is
/// [`crate::handlers::offline_replicas::unavailable_brokers`]: the brokers
/// that this node knows to be fenced or past their heartbeat deadline.
pub(crate) fn automatic_placement_exclusions(
    image: &krabka_metadata::MetadataImage,
    unavailable: &std::collections::HashSet<u64>,
) -> std::collections::HashSet<u64> {
    let mut excluded = fully_cordoned_brokers(image);
    excluded.extend(
        controlled_shutdown_brokers(image).filter(|node_id| !unavailable.contains(node_id)),
    );
    excluded
}

/// The registered brokers as the site-aware placement sees them, in node-id
/// order.
///
/// `excluded` brokers are left out ([`automatic_placement_exclusions`] for an
/// automatic placement, nothing for a manual assignment, which needs only the
/// registrations). A broker in `fenced` stays in, tagged so the placement
/// takes it last and never puts it first. `fenced` is
/// [`crate::handlers::offline_replicas::unavailable_brokers`].
///
/// The list keeps the race tolerance of the plain broker list. On a cluster
/// that just started, the image may not hold the self-registration record
/// yet. `local_broker` covers that window: it is this node when this node is
/// itself a broker, and the list then holds it alone. That entry declares no
/// site, so the placement stays the plain Kafka round-robin.
///
/// `local_broker` is `None` on a node whose `process.roles` exclude `broker`.
/// KIP-919 puts `CreateTopics` and `CreatePartitions` on the controller
/// listener, so a controller-only node answers both, and it never
/// self-registers as a broker -- `register_broker` skips it deliberately.
/// Substituting it here would place partitions on a node that hosts no
/// replicas and materialize them locally, leaving topic metadata that nothing
/// can ever serve. With no fallback the list stays empty, the placement
/// cannot be satisfied, and the caller reports `INVALID_REPLICATION_FACTOR`,
/// which is what a Kafka controller with no registered brokers returns.
pub(crate) fn site_broker_views(
    image: &krabka_metadata::MetadataImage,
    local_broker: Option<krabka_raft::NodeId>,
    excluded: &std::collections::HashSet<u64>,
    fenced: &std::collections::HashSet<u64>,
) -> Vec<SiteBrokerView> {
    let has_registrations = image.brokers().next().is_some();
    let mut views = image
        .brokers()
        .filter(|broker| !excluded.contains(&broker.node_id.0))
        .map(|broker| SiteBrokerView {
            node_id: broker.node_id,
            site: broker.rack.clone(),
            is_witness: resolve_broker_witness(image, broker.node_id),
            fenced: fenced.contains(&broker.node_id.0),
        })
        .collect::<Vec<_>>();
    if let (false, true, Some(node_id)) = (has_registrations, views.is_empty(), local_broker) {
        views.push(SiteBrokerView {
            node_id,
            site: None,
            is_witness: false,
            fenced: false,
        });
    }
    views.sort_by_key(|view| view.node_id);
    views
}

/// The replica list of each partition of a new topic.
///
/// An explicit `assignments` field wins, as it does in Kafka. The handler
/// then takes the caller's lists verbatim, after [`manual_replicas`]
/// validates them. Without that field the placement is automatic and
/// site-aware: see [`stretch_replicas`].
///
/// The result is an empty outer vec when the automatic placement cannot
/// satisfy the request, and the caller reports `INVALID_REPLICATION_FACTOR`.
/// An invalid explicit assignment gives Kafka's error code and message
/// instead.
pub(super) fn resolve_assignments(
    topic: &CreatableTopic,
    brokers: &[SiteBrokerView],
    preferred_site: Option<&str>,
    rng: &mut PlacementRng,
) -> Result<Vec<Vec<krabka_raft::NodeId>>, (i16, String)> {
    if topic.assignments.is_empty() {
        return Ok(stretch_replicas(
            brokers,
            topic.num_partitions,
            topic.replication_factor,
            preferred_site,
            rng,
        ));
    }
    let node_ids = brokers
        .iter()
        .map(|broker| broker.node_id)
        .collect::<Vec<_>>();
    manual_replicas(topic, &node_ids)
}
