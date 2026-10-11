//! The offline-replica state that `Metadata` and `DescribeTopicPartitions`
//! share: the KIP-112 / KIP-858 `offlineReplicas` list, and the leader and ISR
//! columns that have to agree with it.
//!
//! `kafka-topics --describe --unavailable-partitions` and
//! `--under-replicated-partitions` read this list, and so does every dashboard
//! built on the `AdminClient`. Two tools that are often named here do not.
//! Cruise Control needs a broker-side JVM metrics reporter and JMX, and krabka
//! has neither, so it does not run against krabka at all. Burrow is a
//! consumer-lag monitor: it decodes `__consumer_offsets` and never asks for
//! replica state. `docs/operations/ecosystem-support.md` holds the whole list.
//! Kafka computes the offline set in
//! `KRaftMetadataCache.getOfflineReplicas`: a replica is offline when its
//! broker has no registration in the metadata image, when that broker is
//! fenced, when that broker has no endpoint on the listener the request
//! arrived on, or when the log directory that holds the replica is not among
//! the broker's online directories.
//!
//! Krabka keeps the directory half of that state in the same place Kafka does.
//! [`BrokerRegistrationRecord::log_dirs`] is the broker's *online* directory
//! set, and the controller trims a reported-offline directory out of it while
//! it runs the KIP-112 failover (see
//! [`crate::handlers::broker_heartbeat::failover`]), exactly as Kafka's
//! `ReplicationControlManager.handleDirectoriesOffline` emits a
//! `BrokerRegistrationChangeRecord` carrying the surviving directories. The
//! offline set of a broker is therefore the directories named by partition
//! assignments that its registration no longer lists.
//!
//! The fencing half is replicated too. Only the controller leader keeps a
//! heartbeat registry, so it writes what that registry decides into the
//! broker's registration (see [`crate::heartbeat::fencing`]), the way Kafka's
//! controller writes `BrokerRegistrationChangeRecord.fenced`.
//! [`unavailable_brokers`] reads it back out of the image, so a request served by a follower answers with the
//! same set as one served by the controller. `DescribeCluster` calls the same
//! helper for its `is_fenced` column, so the two answers cannot drift apart.

use std::collections::HashSet;

use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, NodeId, PartitionRecord};

use super::metadata::wire_id;
use crate::{broker::Broker, elr::state::is_leaderless, heartbeat::fencing::fenced_node_ids};

/// The brokers this node knows to be fenced or past their heartbeat deadline.
///
/// The fence replicated on each registration answers this on every node. The
/// controller leader unions its live registry on top of it: the publication
/// trails that registry by at most one liveness tick, and the node that made
/// the decision must not report less than it already knows.
///
/// `DescribeCluster` reads the same set for `is_fenced` and for the KIP-1073
/// `include_fenced_brokers` filter.
pub(crate) async fn unavailable_brokers(broker: &Broker, image: &MetadataImage) -> HashSet<u64> {
    let mut unavailable = fenced_node_ids(image);
    let is_controller = *broker.controller.watch_leader().borrow() == Some(broker.config.node_id);
    if is_controller {
        unavailable.extend(broker.liveness.unavailable_snapshot().await);
    }
    unavailable
}

/// The brokers an election may hand leadership to: registered in `image` and
/// not reported by [`unavailable_brokers`].
///
/// `ElectLeaders` is a controller-bound request, but `controllerId` names a
/// rotating unfenced broker rather than the quorum leader (see
/// [`crate::handlers::controller_id`]), so an `AdminClient` sends the election
/// to whichever broker that rotation last named. Deciding liveness from the
/// node-local heartbeat registry would answer the same election differently
/// depending on where it landed, because only the controller leader keeps that
/// registry. This set is the fence replicated on the registrations every node
/// carries, with the controller's own registry unioned in on the node that
/// owns it, so every broker refuses and permits the same elections.
pub(crate) async fn live_brokers(broker: &Broker, image: &MetadataImage) -> HashSet<u64> {
    electable(image, &unavailable_brokers(broker, image).await)
}

/// The projection behind [`live_brokers`]: the registered brokers of `image`
/// that `unavailable` does not name.
///
/// A broker with no registration is never electable, so an id absent from the
/// image is absent from the result whether or not `unavailable` mentions it.
fn electable(image: &MetadataImage, unavailable: &HashSet<u64>) -> HashSet<u64> {
    image
        .brokers()
        .map(|registration| registration.node_id.0)
        .filter(|node| !unavailable.contains(node))
        .collect()
}

/// The endpoint of `broker` named `listener`, as Kafka's
/// `BrokerRegistration.node(listenerName)` finds it, or `None` when the broker
/// has no endpoint on that listener. There is no fallback to another listener.
pub(crate) fn listener_endpoint<'a>(
    broker: &'a BrokerRegistrationRecord,
    listener: &str,
) -> Option<&'a krabka_metadata::BrokerEndpoint> {
    broker
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == listener)
}

/// The offline replicas of `partition`, in replica order.
///
/// `unavailable` comes from [`unavailable_brokers`], and `listener` is the
/// listener the request arrived on. The result is the wire value for
/// `MetadataResponsePartition.offline_replicas` and
/// `DescribeTopicPartitionsResponsePartition.offline_replicas`.
pub(crate) fn offline_replicas(
    image: &MetadataImage,
    partition: &PartitionRecord,
    unavailable: &HashSet<u64>,
    listener: &str,
) -> Vec<i32> {
    partition
        .replicas
        .iter()
        .enumerate()
        .filter(|&(slot, &replica)| {
            let directory = partition.directories.get(slot).copied();
            is_offline(image, unavailable, listener, replica, directory)
        })
        .map(|(_, &replica)| wire_id(replica))
        .collect()
}

/// Kafka's `MetadataResponse.NO_LEADER_ID`. `kafka-topics` renders it as
/// `Leader: none`, and both of its health filters key on it.
pub(crate) const NO_LEADER_ID: i32 = -1;

/// The leader, ISR and offline-replica columns of one partition row, decided
/// together so the two APIs cannot answer differently.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartitionAvailability {
    /// `MetadataResponsePartition.leader_id` /
    /// `DescribeTopicPartitionsResponsePartition.leader_id`.
    pub(crate) leader_id: i32,
    /// `isr_nodes`, in ISR order.
    pub(crate) isr_nodes: Vec<i32>,
    /// `offline_replicas`, in replica order.
    pub(crate) offline_replicas: Vec<i32>,
}

/// Project `partition` into the three columns that describe its health.
///
/// A replica on a log directory its own broker no longer lists as online is
/// not a leader and is not in-sync, whatever the partition record still says.
///
/// A leader with no registration, or none with an endpoint on `listener`, also
/// answers `-1`, as Kafka's `KRaftMetadataCache.getAliveEndpoint` finds no node
/// for it. `Metadata` tells the two cases apart for its error code: see
/// [`leader_endpoint_error`].
///
/// Apache Kafka answers that shape from the image alone. Its controller writes
/// the conclusion down -- measured against `mirror.gcr.io/apache/kafka:4.3.1`,
/// a one-replica partition whose log directory fills up answers
/// `Leader: none  Replicas: 1  Isr:` while its sibling on the surviving
/// directory still answers `Leader: 1  Isr: 1` -- and `KRaftMetadataCache`
/// copies the leader and the ISR through untouched. Read out of
/// `kafka-metadata-4.3.1.jar`: `maybeFilterAliveReplicas` returns its argument
/// unchanged unless the caller passes `errorUnavailableEndpoints`, the legacy
/// flag that drops replicas with no listener, and neither the modern
/// `Metadata` path nor `DescribeTopicPartitions` passes it.
///
/// Krabka's controller reaches the same conclusion --
/// `crate::leader_election::compute_offline_dir_failover_changes` decides
/// `FailoverDecision::Unavailable` for exactly this partition -- and then
/// cannot write it down. `PartitionRecord::leader` is a [`NodeId`], which has
/// no `-1`, so the record goes on naming a replica that can no longer lead,
/// and an ISR shrink on its own would leave a leader outside its own ISR. The
/// conclusion is applied here instead, once, for both APIs.
///
/// It is deliberately narrower than [`offline_replicas`], which also reports a
/// fenced broker's replicas and an unregistered broker's. Those two are
/// offline for reasons the controller *can* record, and does: it shrinks the
/// ISR and moves leadership on the same edge. Kafka passes both straight
/// through its cache, so dropping them from the reported ISR here would
/// diverge from Kafka in a state Kafka does answer, to fix one it never
/// reaches.
///
/// Why any of it matters: `kafka-topics --describe --unavailable-partitions`
/// and `--under-replicated-partitions` never read `offlineReplicas`. Read out
/// of `kafka-tools-4.3.1.jar`, `TopicCommand$PartitionDescription` has no
/// reference to it at all, and `TopicPartitionInfo` has no field to carry it.
/// The two filters are `!hasLeader() || !liveBrokers.contains(leader.id())`
/// and `replicationFactor - isr.size() > 0`, so a partition on a dead disk is
/// invisible to both for as long as it reports a live leader and a full ISR,
/// however faithfully the third column names the disk.
///
/// The same holds for a partition the controller left without a leader. Kafka
/// writes `leader = -1` and an ISR that has lost the last leader; krabka's
/// record names a leader whatever happens, so the controller publishes the
/// last-known ELR that names it instead (see
/// [`crate::elr::state::is_leaderless`]) and this projection reads it back:
/// `Leader: none`, and the last leader is out of the ISR.
pub(crate) fn partition_availability(
    image: &MetadataImage,
    partition: &PartitionRecord,
    unavailable: &HashSet<u64>,
    listener: &str,
) -> PartitionAvailability {
    let dead_dir: Vec<i32> = partition
        .replicas
        .iter()
        .enumerate()
        .filter(|&(slot, &replica)| {
            image.broker(replica).is_some_and(|registration| {
                !has_online_dir(registration, partition.directories.get(slot).copied())
            })
        })
        .map(|(_, &replica)| wire_id(replica))
        .collect();
    let leader = wire_id(partition.leader);
    let leaderless = is_leaderless(image, partition);
    PartitionAvailability {
        leader_id: if leaderless
            || dead_dir.contains(&leader)
            || leader_endpoint_error(image, partition.leader, listener).is_some()
        {
            NO_LEADER_ID
        } else {
            leader
        },
        isr_nodes: partition
            .isr
            .iter()
            .copied()
            .map(wire_id)
            .filter(|replica| !dead_dir.contains(replica))
            .filter(|replica| !leaderless || *replica != leader)
            .collect(),
        offline_replicas: offline_replicas(image, partition, unavailable, listener),
    }
}

/// Why `leader` has no endpoint to advertise on `listener`, or `None` when it
/// has one. This is Kafka's `KRaftMetadataCache.getAliveEndpoint`, and the two
/// errors that `partitionMetadata` derives from its absence.
pub(crate) fn leader_endpoint_error(
    image: &MetadataImage,
    leader: NodeId,
    listener: &str,
) -> Option<LeaderEndpointError> {
    match image.broker(leader) {
        None => Some(LeaderEndpointError::Unregistered),
        Some(registration) if listener_endpoint(registration, listener).is_none() => {
            Some(LeaderEndpointError::ListenerNotFound)
        }
        Some(_) => None,
    }
}

/// The two ways a leader can lack an endpoint on the listener of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaderEndpointError {
    /// The leader has no registration. Kafka answers `LEADER_NOT_AVAILABLE`.
    Unregistered,
    /// The leader is registered without the listener. Kafka answers
    /// `LISTENER_NOT_FOUND` from `Metadata` v6 on, and `LEADER_NOT_AVAILABLE`
    /// before that.
    ListenerNotFound,
}

/// Whether the replica `replica` holds on `directory` is offline.
fn is_offline(
    image: &MetadataImage,
    unavailable: &HashSet<u64>,
    listener: &str,
    replica: NodeId,
    directory: Option<uuid::Uuid>,
) -> bool {
    // Unregistered: Kafka reports the replica offline rather than dropping it,
    // so the id still appears in `replica_nodes` next to it.
    let Some(registration) = image.broker(replica) else {
        return true;
    };
    unavailable.contains(&replica.0)
        || listener_endpoint(registration, listener).is_none()
        || !has_online_dir(registration, directory)
}

/// Whether `directory` is one of the broker's online log directories.
///
/// The unassigned directory id is online, as in `DirectoryId.isOnline`,
/// because the owning broker has not reported its `AssignReplicasToDirs` yet
/// and no disk can be blamed for a replica nobody has placed.
///
/// `DirectoryId.isOnline` also reads an empty directory list as "everything
/// on this broker is online", for registrations written before metadata
/// version 3.7-IV2 carried log dirs at all. Krabka has no such registration:
/// a broker publishes an id for every entry of `log.dirs` when it registers,
/// and the only writer that shortens that list is the KIP-112 retire path in
/// [`crate::handlers::broker_heartbeat::failover`]. An empty list here means
/// the broker reported its last surviving directory offline, so a concrete
/// non-nil assignment on it names a dead disk, not a broker that predates
/// directory assignment.
fn has_online_dir(registration: &BrokerRegistrationRecord, directory: Option<uuid::Uuid>) -> bool {
    let Some(directory) = directory else {
        return true;
    };
    directory.is_nil() || registration.log_dirs.contains(&directory)
}

/// The directory half of Kafka's `LeaderAcceptor`:
/// `clusterControl.hasOnlineDir(replica, partition.directory(replica))`.
///
/// A replica whose broker has no registration in `image` has no directory
/// list to blame, so it passes: the election's liveness set already refuses an
/// unregistered broker.
pub(crate) fn replica_dir_online(
    image: &MetadataImage,
    partition: &PartitionRecord,
    replica: NodeId,
) -> bool {
    let directory = partition
        .replicas
        .iter()
        .position(|&candidate| candidate == replica)
        .and_then(|slot| partition.directories.get(slot).copied());
    image
        .broker(replica)
        .is_none_or(|registration| has_online_dir(registration, directory))
}

#[cfg(test)]
mod tests;
