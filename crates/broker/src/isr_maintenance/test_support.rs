//! Fixtures shared by this module's unit tests: metadata records to seed an
//! image, a real on-disk partition, a way to force a replica state, and the
//! `MetadataSource` the ISR code reads its image and leader from.

use {
    crate::{
        partition::Partition,
        replica_state::FollowerStats,
        test_support::{FakeMetadataSource, PartitionEpoch, broker_registration},
    },
    krabka_ids::LeaderEpoch,
    krabka_log::Offset,
    krabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    },
    krabka_raft::NodeId,
    std::time::{Duration, Instant},
};

pub(super) fn reg(id: NodeId) -> MetadataRecord { reg_at(id, &format!("b{id}"), 9092) }

/// The registration of broker `id`, with epoch `id`, that advertises
/// `host:port`.
pub(super) fn reg_at(id: NodeId, host: &str, port: u16) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        broker_epoch: i64::try_from(id.0).unwrap(),
        host: host.to_string(),
        port,
        ..broker_registration(id)
    })
}

pub(super) fn topic(name: &str, topic_id: uuid::Uuid) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.to_string(),
        topic_id,
        partitions: 1,
        replication_factor: 3,
    })
}

pub(super) use crate::test_support::open_partition as fixture_partition;

/// Install `isr` and `replicas` with `leader` at `leader_epoch` on `part`.
/// Each `(follower, age)` in `stale_followers` has not fetched from this
/// leader and last caught up `age` ago.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct IsrSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    #[default(&[NodeId(1), NodeId(2)])]
    pub isr: &'a [NodeId],
    #[default(&[NodeId(1), NodeId(2)])]
    pub replicas: &'a [NodeId],
    #[default(NodeId(1))]
    pub leader: NodeId,
    pub leader_epoch: LeaderEpoch,
    pub partition_epoch: PartitionEpoch,
}

pub(super) async fn set_replica_state(
    part: &Partition,
    record: MetadataRecord,
    stale_followers: &[(NodeId, Duration)],
) {
    let MetadataRecord::V1Partition(record) = record else {
        unreachable!("the test fixture must be a partition record");
    };
    let now = Instant::now();
    let mut st = part.replica_state.lock().await;
    st.install_isr(&record.isr, &record.replicas, record.leader, now);
    st.current_leader_epoch = record.leader_epoch;
    for &(follower, last_caught_up_age) in stale_followers {
        st.per_follower.insert(follower, FollowerStats {
            leo: Offset(0),
            last_fetch: None,
            last_fetch_leader_leo: Offset(-1),
            last_caught_up: Some(
                now.checked_sub(last_caught_up_age)
                    .expect("test caught-up age is representable"),
            ),
            broker_epoch: None,
            fetched_since_isr_exit: false,
        });
    }
}

/// Partition 0 of `topic` as the metadata image holds it.
#[bon::builder]
pub(super) fn partition(
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default = & [NodeId (1) , NodeId (2)])] isr: &'_ [NodeId],
    #[builder(default = & [NodeId (1) , NodeId (2)])] replicas: &'_ [NodeId],
    #[builder(into, default = NodeId (1))] leader: NodeId,
    #[builder(into, default)] leader_epoch: LeaderEpoch,
    #[builder(into, default)] partition_epoch: PartitionEpoch,
) -> MetadataRecord {
    let setup = IsrSetup {
        topic,
        isr,
        replicas,
        leader,
        leader_epoch,
        partition_epoch,
    };
    let IsrSetup {
        topic,
        isr,
        replicas,
        leader,
        leader_epoch,
        partition_epoch,
        ..
    } = setup;
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        leader_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: partition_epoch.0,
    })
}

/// A metadata source over `image`, with `leader` as the controller leader.
pub(super) fn fake_source(image: MetadataImage, leader: Option<NodeId>) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(leader)
        .build()
}
