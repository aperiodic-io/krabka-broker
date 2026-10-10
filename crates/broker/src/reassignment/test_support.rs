//! Metadata-image and liveness fixture builders that the reassignment unit
//! tests share. The policy tests and the background-task tests both need an
//! image with one in-flight reassignment, so the builders live in one place
//! rather than in either test module.

use std::sync::Arc;

use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
};
use uuid::Uuid;

use crate::{
    heartbeat::controller_state::ControllerLivenessState, test_support::ReassignmentSetup,
};

pub(super) async fn liveness(alive: &[u64]) -> ControllerLivenessState {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for n in alive {
        l.record_heartbeat(*n).await;
    }
    l
}

pub(super) fn first_partition(rec: &MetadataRecord) -> &PartitionRecord {
    match rec {
        MetadataRecord::V1Partition(p) => p,
        _ => panic!("expected V1Partition"),
    }
}

/// Builds an image with explicit directories. It tests that
/// `compute_reassignment_progress` keeps the directories aligned after a
/// completion removes a replica from the set.
pub(super) fn img(setup: ReassignmentSetup<'_>) -> Arc<MetadataImage> {
    let mut image = MetadataImage::new(Uuid::nil());
    for n in 1..=6u64 {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: String::new(),
                port: 0,
                ..crate::test_support::broker_registration(krabka_raft::NodeId(n))
            },
        ));
    }
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: "foo".into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(setup.replicas.len())
            .expect("replication factor fits i16"),
    }));
    image.apply(&MetadataRecord::V1Partition(
        crate::test_support::reassignment_partition(setup),
    ));
    Arc::new(image)
}
