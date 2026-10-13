//! The `PartitionRecord` batch that `CreatePartitions` commits.
//!
//! Local replicas open through [`crate::handlers::partition_materialization`],
//! which installs the same initial leader and ISR that these records name.

use {
    crate::handlers::create_topics::InitialLeadership, krabka_metadata::MetadataRecord,
    krabka_raft::NodeId,
};

pub(super) fn partition_records(
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
    leaderships: &[InitialLeadership],
) -> Vec<MetadataRecord> {
    // Kafka's `buildPartitionRegistration`: the leader is the first ISR
    // member, and the ISR holds only the replicas that were active.
    indices
        .iter()
        .zip(assignments.iter().zip(leaderships))
        .map(|(index, (replicas, leadership))| leadership.partition_record(topic, *index, replicas))
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::handlers::partition_materialization::should_materialize_locally,
        assert2::check,
    };

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(1)
        ));
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(2)
        ));
        check!(!should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(3)
        ));
    }
}
