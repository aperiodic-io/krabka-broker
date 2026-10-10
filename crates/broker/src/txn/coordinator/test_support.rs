//! Shared fixtures for the transaction-coordinator unit tests.
//!
//! The builders make a `TxnCoordinator` with no live partitions and a plain
//! `TxnEntry`, so the tests in more than one submodule build the same fixture
//! without repeating it.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_metadata::{
    LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord,
};

use super::TxnCoordinator;
use crate::{
    partition::Partition,
    partition_registry::PartitionRegistry,
    test_support::PartitionCount,
    txn::{bootstrap, state::TxnEntry},
};

/// The topic of the data partition [`live_coordinator`] hosts.
pub(super) const DATA_TOPIC: &str = "orders";

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct StateImageSetup<'a> {
    #[default(uuid::Uuid::from_u128(1))]
    pub topic_id: uuid::Uuid,
    #[default(NodeId(1))]
    pub leader: NodeId,
    pub leader_epoch: LeaderEpoch,
    #[default(&[NodeId(1)])]
    pub replicas: &'a [NodeId],
}

/// Single-partition transaction-state metadata, locally led by default.
pub(super) fn state_image(setup: StateImageSetup<'_>) -> MetadataImage {
    let StateImageSetup {
        topic_id,
        leader,
        leader_epoch,
        replicas,
    } = setup;
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: bootstrap::TOPIC.to_owned(),
        topic_id,
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).expect("test replication factor"),
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: bootstrap::TOPIC.to_owned(),
        partition: 0,
        leader,
        replicas: replicas.to_vec(),
        isr: replicas.to_vec(),
        leader_epoch,
        ..Default::default()
    }));
    image
}

pub(super) fn state_registry(dir: &Path) -> Arc<PartitionRegistry> {
    let partitions = Arc::new(PartitionRegistry::new());
    partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        crate::test_support::open_partition(
            dir,
            crate::test_support::StandalonePartitionSetup {
                topic: bootstrap::TOPIC,
                ..Default::default()
            },
        ),
    );
    partitions
}

pub(super) fn coordinator_with_registry(
    node: NodeId,
    partitions: Arc<PartitionRegistry>,
    num_partitions: PartitionCount,
) -> TxnCoordinator {
    TxnCoordinator::new(
        node,
        partitions,
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        num_partitions.0,
        krabka_units::mebibytes(1),
    )
}

/// The live transaction fixtures host the same locally led `orders-0` log.
pub(super) async fn hosted_data_partition(
    dir: &Path,
    partitions: &PartitionRegistry,
) -> Arc<Partition> {
    let data = crate::test_support::open_partition(
        dir,
        crate::test_support::StandalonePartitionSetup {
            topic: DATA_TOPIC,
            ..Default::default()
        },
    );
    // The metadata reconcile installs this broker, node 1, as the leader.
    data.install_leader_change(1, 0).await;
    partitions.insert(
        DATA_TOPIC.into(),
        PartitionIndex::default(),
        Arc::clone(&data),
    );
    data
}

/// A coordinator that leads its one `__transaction_state` partition and hosts
/// the data partition `orders-0`, both as real logs under `dir`, so a marker
/// fan-out and an append succeed. It returns the data partition too, for a test
/// that reads the markers.
pub(super) async fn live_coordinator(dir: &Path) -> (Arc<TxnCoordinator>, Arc<Partition>) {
    let partitions = state_registry(dir);
    let data = hosted_data_partition(dir, &partitions).await;
    let coordinator = Arc::new(coordinator_with_registry(
        NodeId(1),
        partitions,
        crate::test_support::PartitionCount(1),
    ));
    let image = state_image(crate::txn::coordinator::test_support::StateImageSetup::default());
    coordinator
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    (coordinator, data)
}

pub(super) fn test_coordinator() -> TxnCoordinator {
    test_coordinator_with_partitions(PartitionCount(50))
}

pub(super) fn test_coordinator_with_partitions(num_partitions: PartitionCount) -> TxnCoordinator {
    coordinator_with_registry(
        NodeId(1),
        Arc::new(PartitionRegistry::new()),
        num_partitions,
    )
}

#[derive(Clone, Copy)]
pub(super) struct ProducerEpoch(pub i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TxnEntrySetup<'a> {
    #[default("tid-a")]
    pub transactional_id: &'a str,
    #[default(ProducerId(1000))]
    pub producer: ProducerId,
    #[default(ProducerId(-1))]
    pub previous: ProducerId,
    #[default(ProducerId(-1))]
    pub next: ProducerId,
    #[default(ProducerEpoch(-1))]
    pub next_epoch: ProducerEpoch,
}

pub(super) fn entry(setup: TxnEntrySetup<'_>) -> TxnEntry {
    let mut e = TxnEntry::new_empty(setup.transactional_id.into(), setup.producer, 0, 60_000, 0);
    e.prev_producer_id = setup.previous;
    e.next_producer_id = setup.next;
    e.next_producer_epoch = setup.next_epoch.0;
    e
}
