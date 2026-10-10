//! The records a heartbeat writes when it fences a broker, lets it shut down,
//! or moves it into controlled shutdown.
//!
//! Kafka writes the same partition changes for all three
//! (`ReplicationControlManager.handleBrokerFenced` and
//! `handleBrokerInControlledShutdown` both run
//! `generateLeaderAndIsrUpdates` with the broker to remove): the broker leaves
//! the ISR of every partition it is in, and every partition it leads elects a
//! new leader. That is what the dead-broker failover scan computes, so this
//! module asks it about the broker.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::NodeId;

use crate::heartbeat::controller_state::ControllerLivenessState;

/// The partition changes that take `broker` out of every ISR and every
/// leadership, and whether it still leads a partition that another replica
/// can take (Kafka's `BrokerToIsrs.hasLeaderships`).
#[derive(Debug, Default, PartialEq)]
pub(super) struct LeaveIsrs {
    pub(super) changes: Vec<MetadataRecord>,
    pub(super) has_leaderships: bool,
}

/// Compute [`LeaveIsrs`] for `broker` against `image`.
///
/// A partition that no other replica can lead (a single-replica internal
/// topic, or an ISR whose other members are witnesses or down) is left alone
/// and does not count as a leadership. Kafka marks it leaderless, but it has
/// no other replica to serve it either way, and counting it would hold the
/// controlled shutdown for ever.
///
/// The scan does not hand a partition to the offset-aware recovery manager.
/// The broker is still running and still holds its log, so there is nothing
/// to recover yet.
pub(super) async fn leave_isrs(
    image: &MetadataImage,
    broker: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
) -> LeaveIsrs {
    let plan =
        crate::leader_election::compute_failover_changes(image, broker, liveness, metrics).await;
    let has_leaderships = plan.changes.iter().any(|change| {
        matches!(change, MetadataRecord::V1Partition(moved)
            if moved.leader != broker
                && image
                    .partition(&moved.topic, moved.partition)
                    .is_some_and(|current| current.leader == broker))
    });
    LeaveIsrs {
        changes: plan.changes,
        has_leaderships,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{LeaderEpoch, PartitionRecord};
    use uuid::Uuid;

    use super::*;
    use crate::handlers::broker_heartbeat::test_support::{
        image_with_dir_partition, liveness_with,
    };

    /// One partition, and what leaving its ISR does to it.
    struct Case {
        what: &'static str,
        leader: u64,
        isr: &'static [u64],
        alive: &'static [u64],
        witnesses: &'static [u64],
        expected_leader_and_isr: Option<(u64, &'static [u64])>,
        has_leaderships: bool,
    }

    /// krabka-io/krabka-broker#824: broker 1 leaves every ISR, not only the
    /// partitions it leads.
    #[tokio::test]
    async fn a_broker_leaves_every_isr_and_every_transferable_leadership() {
        let cases = [
            Case {
                what: "a partition it leads, with a live follower",
                leader: 1,
                isr: &[1, 2],
                alive: &[1, 2],
                witnesses: &[],
                expected_leader_and_isr: Some((2, &[2])),
                has_leaderships: true,
            },
            Case {
                what: "a partition it only follows",
                leader: 2,
                isr: &[2, 1],
                alive: &[1, 2],
                witnesses: &[],
                expected_leader_and_isr: Some((2, &[2])),
                has_leaderships: false,
            },
            Case {
                what: "a partition no other replica can lead",
                leader: 1,
                isr: &[1],
                alive: &[1, 2],
                witnesses: &[],
                expected_leader_and_isr: None,
                has_leaderships: false,
            },
            Case {
                what: "a partition whose other ISR member is a witness",
                leader: 1,
                isr: &[1, 2],
                alive: &[1, 2],
                witnesses: &[2],
                expected_leader_and_isr: None,
                has_leaderships: false,
            },
            Case {
                what: "a partition it is not in",
                leader: 2,
                isr: &[2],
                alive: &[1, 2],
                witnesses: &[],
                expected_leader_and_isr: None,
                has_leaderships: false,
            },
        ];
        for case in cases {
            let node = |id: &u64| NodeId(*id);
            let mut image = image_with_dir_partition(
                crate::handlers::broker_heartbeat::test_support::ElectionSetup {
                    leader: krabka_raft::NodeId(case.leader),
                    replicas: &[krabka_raft::NodeId(1), krabka_raft::NodeId(2)],
                    isr: &crate::test_support::replica_nodes(case.isr),
                    dirs: &[Uuid::nil(), Uuid::nil()],
                    ..Default::default()
                },
            );
            crate::leader_election::test_support::mark_witnesses_in_image(
                &mut image,
                case.witnesses,
            );
            let alive: Vec<NodeId> = case.alive.iter().map(node).collect();
            let liveness = liveness_with(&alive).await;
            let metrics = crate::metrics::BrokerMetrics::new();

            let left = leave_isrs(&image, NodeId(1), &liveness, &metrics).await;

            let expected_changes: Vec<MetadataRecord> = case
                .expected_leader_and_isr
                .iter()
                .map(|(leader, isr)| {
                    let before = image.partition("t", 0).expect("the partition");
                    let moved = *leader != case.leader;
                    MetadataRecord::V1Partition(PartitionRecord {
                        leader: NodeId(*leader),
                        isr: isr.iter().map(node).collect(),
                        leader_epoch: if moved {
                            LeaderEpoch(before.leader_epoch.0 + 1)
                        } else {
                            before.leader_epoch
                        },
                        partition_epoch: before.partition_epoch + 1,
                        ..before.clone()
                    })
                })
                .collect();
            check!(
                left == LeaveIsrs {
                    changes: expected_changes,
                    has_leaderships: case.has_leaderships,
                },
                "{}",
                case.what
            );
        }
    }
}
