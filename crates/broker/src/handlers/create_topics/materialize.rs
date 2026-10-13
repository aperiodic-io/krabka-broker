//! Installs the initial leader and ISR of a newly committed partition.
//!
//! Both creation handlers open their local replicas through
//! [`crate::handlers::partition_materialization`] before installing this role.

use crate::{error::BrokerError, partition::Partition, producer_state::ProducerState};

fn is_local_leader(leader: krabka_raft::NodeId, node_id: krabka_raft::NodeId) -> bool {
    leader == node_id
}

impl super::InitialLeadership {
    /// The authoritative registration shared by both creation APIs.
    pub(crate) fn partition_record(
        &self,
        topic: &str,
        partition: i32,
        replicas: &[krabka_raft::NodeId],
    ) -> krabka_metadata::MetadataRecord {
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: topic.to_owned(),
            partition,
            leader: self.leader,
            replicas: replicas.to_vec(),
            isr: self.isr.clone(),
            leader_epoch: krabka_metadata::LeaderEpoch(super::INITIAL_LEADER_EPOCH),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    }

    /// Installs this leadership on a partition this broker has just
    /// materialized: the first leader at `leader_epoch` and, when this broker
    /// leads, the ISR.
    ///
    /// When this broker leads a disk-backed partition, the leader epoch is
    /// recorded at the log end before the role is published, as Kafka's
    /// `Partition.makeLeader` does through `maybeAssignEpochStartOffset` on
    /// the first `PartitionRecord`. A follower that fetches before the
    /// leader's first write can then be placed straight away, instead of
    /// waiting for the supervisor's next reconcile to record the epoch. A
    /// diskless partition's promotion belongs to the supervisor, which
    /// prepares its log from the WAL, so it and a follower replica only
    /// install the leader. The disk-backed leader also takes the producer
    /// state of its new log into `producer_state`, as every promotion does.
    ///
    /// `CreatePartitions` materializes its new partitions through this too.
    ///
    /// # Errors
    /// Returns the leader-epoch checkpoint's error when the epoch cannot be
    /// recorded. The role and the ISR are then not installed, and the
    /// supervisor's next reconcile retries the promotion, as it does for a
    /// promotion of its own that fails.
    pub(crate) async fn install(
        &self,
        partition: &Partition,
        producer_state: &ProducerState,
        topic_id: uuid::Uuid,
        node_id: krabka_raft::NodeId,
        replicas: &[krabka_raft::NodeId],
        leader_epoch: i32,
    ) -> Result<(), BrokerError> {
        let leader = self.leader;
        if !is_local_leader(leader, node_id) {
            partition
                .install_leader_change(leader.0, leader_epoch)
                .await;
            return Ok(());
        }
        if partition.diskless {
            partition
                .install_leader_change(leader.0, leader_epoch)
                .await;
        } else {
            partition
                .install_local_leadership(producer_state, Some(topic_id), leader.0, leader_epoch)
                .await?;
        }
        partition.install_isr(&self.isr, replicas, leader).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::is_local_leader,
        crate::{
            handlers::{
                create_topics::InitialLeadership,
                partition_materialization::should_materialize_locally,
            },
            partition::{Partition, test_support::test_partition},
            producer_state::ProducerState,
        },
        assert2::assert,
        krabka_raft::NodeId,
        std::sync::{Arc, atomic::Ordering},
    };

    /// What an installed leadership left on a partition.
    #[derive(Debug, PartialEq, Eq)]
    struct InstalledRole {
        /// The leader-epoch cache, as `(epoch, start_offset)` pairs.
        epoch_history: Vec<(i32, i64)>,
        leader: u64,
        leader_epoch: i32,
        /// The ISR, sorted by node id.
        isr: Vec<u64>,
    }

    async fn installed_role(partition: &Partition) -> InstalledRole {
        let epoch_history = partition
            .log
            .lock()
            .expect("log mutex")
            .epoch_checkpoint()
            .entries()
            .iter()
            .map(|entry| (entry.epoch.0, entry.start_offset.0))
            .collect();
        let mut isr: Vec<u64> = partition
            .replica_state
            .lock()
            .await
            .isr
            .iter()
            .map(|node| node.0)
            .collect();
        isr.sort_unstable();
        InstalledRole {
            epoch_history,
            leader: partition.current_leader.load(Ordering::Acquire),
            leader_epoch: partition.current_leader_epoch.load(Ordering::Acquire),
            isr,
        }
    }

    use crate::partition::test_support::EpochCheckpointFull;

    /// Kafka's `Partition.makeLeader` records the first leader epoch at the
    /// log end the moment a disk-backed partition's leader is created, so a
    /// fresh leader's epoch cache names its epoch before any write and before
    /// any supervisor reconcile. A follower replica and a diskless leader,
    /// whose promotion the supervisor prepares, record nothing here. A
    /// promotion whose epoch cannot be recorded publishes neither the role
    /// nor the ISR.
    #[tokio::test]
    async fn a_new_partition_leader_records_its_epoch_before_publishing_the_role() {
        const LOCAL: NodeId = NodeId(1);
        let leadership = |leader| InitialLeadership {
            leader,
            isr: vec![NodeId(1), NodeId(2)],
        };
        // (name, leader, diskless, checkpoint fails, install succeeds, role)
        let cases = [
            (
                "local disk-backed leader",
                LOCAL,
                false,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![(4, 0)],
                    leader: 1,
                    leader_epoch: 4,
                    isr: vec![1, 2],
                },
            ),
            (
                "follower replica",
                NodeId(2),
                false,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 2,
                    leader_epoch: 4,
                    isr: vec![],
                },
            ),
            (
                "local diskless leader",
                LOCAL,
                true,
                false,
                true,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 1,
                    leader_epoch: 4,
                    isr: vec![1, 2],
                },
            ),
            (
                "epoch checkpoint on a full disk",
                LOCAL,
                false,
                true,
                false,
                InstalledRole {
                    epoch_history: vec![],
                    leader: 0,
                    leader_epoch: 0,
                    isr: vec![],
                },
            ),
        ];
        for (name, leader, diskless, checkpoint_fails, installs, expected) in cases {
            let (mut partition, _dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
            partition.diskless = diskless;
            if checkpoint_fails {
                partition
                    .log
                    .lock()
                    .expect("log mutex")
                    .test_set_io(Arc::new(EpochCheckpointFull));
            }

            let result = leadership(leader)
                .install(
                    &partition,
                    &ProducerState::new(),
                    uuid::Uuid::from_u128(7),
                    LOCAL,
                    &[NodeId(1), NodeId(2)],
                    4,
                )
                .await;

            assert!(result.is_ok() == installs, "{name}");
            assert!(installed_role(&partition).await == expected, "{name}");
        }
    }

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        let materialize_cases: [(&[krabka_raft::NodeId], krabka_raft::NodeId, bool); 3] = [
            (&[NodeId(1), NodeId(2)], NodeId(1), true),
            (&[NodeId(1), NodeId(2)], NodeId(2), true),
            (&[NodeId(1), NodeId(2)], NodeId(3), false),
        ];
        for (replicas, node_id, want) in materialize_cases {
            assert!(
                should_materialize_locally(replicas, node_id) == want,
                "replicas {replicas:?}, node {node_id}"
            );
        }

        let leader_cases: [(krabka_raft::NodeId, krabka_raft::NodeId, bool); 2] =
            [(NodeId(1), NodeId(1), true), (NodeId(2), NodeId(1), false)];
        for (leader, node_id, want) in leader_cases {
            assert!(
                is_local_leader(leader, node_id) == want,
                "leader {leader}, node {node_id}"
            );
        }
    }
}
