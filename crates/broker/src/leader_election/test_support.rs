//! Fixtures shared by the leader-election test modules. They build metadata
//! images with one partition, seed a controller liveness registry alive or
//! dead, and provide a `MetadataSource` double that records every batch the
//! code under test submits.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use krabka_metadata::{
    BrokerConfigRecord, LeaderEpoch, MetadataImage, MetadataRecord, PartitionRecord,
    TopicConfigRecord, TopicRecord,
};
use krabka_raft::NodeId;
use uuid::Uuid;

use crate::{
    heartbeat::controller_state::{ControllerLivenessState, TestClock},
    test_support::FakeMetadataSource,
};

#[derive(Clone, Copy, Default)]
pub enum ElrFinalization {
    #[default]
    Unchanged,
    Enabled,
}

#[derive(Clone, Copy)]
pub struct ElectionSetup<'a> {
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    pub leader: NodeId,
    pub replicas: &'a [NodeId],
    pub isr: &'a [NodeId],
    pub dirs: &'a [Uuid],
    pub configs: &'a [(&'a str, &'a str)],
    pub elr: ElrFinalization,
}

impl Default for ElectionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "t",
            partition: krabka_ids::PartitionIndex(0),
            leader: NodeId(1),
            replicas: &[NodeId(1), NodeId(2), NodeId(3)],
            isr: &[NodeId(1), NodeId(2), NodeId(3)],
            dirs: &[],
            configs: &[],
            elr: ElrFinalization::Unchanged,
        }
    }
}

pub fn img_with_partition(setup: ElectionSetup<'_>) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: setup.topic.into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(setup.replicas.len()).unwrap(),
    }));
    img.apply(&MetadataRecord::V1Partition(seed_partition(setup)));
    if matches!(setup.elr, ElrFinalization::Enabled) {
        crate::test_support::finalize_elr_version(&mut img);
    }
    if !setup.configs.is_empty() {
        set_topic_configs(
            &mut img,
            TopicConfigSetup {
                topic: setup.topic,
                entries: setup.configs,
            },
        );
    }
    img
}

/// Input partition for election tests, before any leader or ISR change.
pub fn seed_partition(setup: ElectionSetup<'_>) -> PartitionRecord {
    let ElectionSetup {
        topic,
        partition,
        leader,
        replicas,
        isr,
        dirs,
        ..
    } = setup;
    PartitionRecord {
        topic: topic.into(),
        partition: partition.0,
        leader,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: dirs.to_vec(),
        partition_epoch: 0,
    }
}

/// Run a failover scan whose metrics are not inspected by the test.
pub async fn failover(
    image: &MetadataImage,
    dead: NodeId,
    liveness: &ControllerLivenessState,
) -> super::policy::FailoverPlan {
    super::scan::compute_failover_changes(
        image,
        dead,
        liveness,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await
}

/// Run a scan with a fresh liveness registry containing the given live brokers.
pub async fn failover_with_alive(
    image: &MetadataImage,
    dead: NodeId,
    alive: &[u64],
) -> super::policy::FailoverPlan {
    let liveness = liveness_with_alive(alive).await;
    failover(image, dead, &liveness).await
}

/// Independent expected result of a clean election for the three-replica fixture.
pub fn expected_clean_election(
    leader: NodeId,
    isr: &[NodeId],
    directories: Vec<Uuid>,
) -> PartitionRecord {
    expected_partition(ExpectedPartitionSetup {
        leader,
        isr,
        directories,
        ..Default::default()
    })
}

/// Independent expected partition after one change to the three-replica fixture.
pub struct ExpectedPartitionSetup<'a> {
    pub topic: &'a str,
    pub leader: NodeId,
    pub isr: &'a [NodeId],
    pub leader_epoch: LeaderEpoch,
    pub directories: Vec<Uuid>,
}

impl Default for ExpectedPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "t",
            leader: NodeId(2),
            isr: &[NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(6),
            directories: vec![],
        }
    }
}

pub fn expected_partition(setup: ExpectedPartitionSetup<'_>) -> PartitionRecord {
    let ExpectedPartitionSetup {
        topic,
        leader,
        isr,
        leader_epoch,
        directories,
    } = setup;
    PartitionRecord {
        topic: topic.into(),
        partition: 0,
        leader,
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: isr.to_vec(),
        leader_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories,
        partition_epoch: 1,
    }
}

/// The witness set for a plain, non-stretch cluster. Every pre-witness
/// behaviour must be unchanged against it.
pub fn no_witnesses() -> std::collections::HashSet<NodeId> {
    std::collections::HashSet::new()
}

/// Mark `ids` as data-bearing witnesses.
pub fn witnesses(ids: &[u64]) -> std::collections::HashSet<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

/// Register `ids` as brokers and publish `broker.witness=true` for each
/// one, which is the path the real broker takes at registration. Use this
/// where the code under test reads the witness set out of the image.
pub fn mark_witnesses_in_image(img: &mut MetadataImage, ids: &[u64]) {
    register_brokers(img, ids);
    for &id in ids {
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(id),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
    }
}

/// The alive-broker set the operator elections take, spelled as the ids that
/// are up. Equivalent to [`liveness_with_alive`] followed by `alive_snapshot`,
/// without the registry the selectors no longer read.
pub fn alive_set(alive: &[u64]) -> std::collections::HashSet<u64> {
    alive.iter().copied().collect()
}

pub async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for &n in alive {
        l.record_heartbeat(n).await;
    }
    Arc::new(l)
}

/// A metadata source over `image`, with `leader` as the controller leader. It
/// records every batch the driver submits, which is what these tests assert
/// on.
pub fn fake_source(image: MetadataImage, leader: Option<NodeId>) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(leader)
            .build(),
    )
}

/// Like [`fake_source`], but no `submit_change` ever completes. This models a raft
/// commit that stalls, so the driver's own timeout path runs.
pub fn stalled_fake_source(
    image: MetadataImage,
    leader: Option<NodeId>,
) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(leader)
            .stall_submits()
            .build(),
    )
}

pub fn recovery_handle_for_tests() -> crate::unclean_recovery::UncleanRecoveryHandle {
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    crate::unclean_recovery::UncleanRecoveryHandle::for_tests(tx)
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TopicConfigSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub entries: &'a [(&'a str, &'a str)],
}

/// Apply the complete override map in one record, with ELR state in its own records.
pub fn set_topic_configs(img: &mut MetadataImage, setup: TopicConfigSetup<'_>) {
    let TopicConfigSetup { topic, entries } = setup;
    let overrides: BTreeMap<String, String> = entries
        .iter()
        .filter(|(key, _)| *key != crate::config_keys::ELIGIBLE_LEADER_REPLICAS)
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    if !overrides.is_empty() {
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides,
        }));
    }
    for (_, value) in entries
        .iter()
        .filter(|(key, _)| *key == crate::config_keys::ELIGIBLE_LEADER_REPLICAS)
    {
        for record in crate::elr::state::test_records(topic, value) {
            img.apply(&record);
        }
    }
}

pub fn set_cluster_default(img: &mut MetadataImage, key: &str, value: &str) {
    img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: key.into(),
        config_value: Some(value.into()),
    }));
}

/// The submitted batches that carry partition changes.
///
/// A liveness tick also publishes the controller's fencing decisions (see
/// [`crate::heartbeat::fencing`]), so a failover test reads the batches it
/// cares about through this filter rather than by position.
pub fn partition_batches(batches: &[Vec<MetadataRecord>]) -> Vec<Vec<MetadataRecord>> {
    batches
        .iter()
        .filter(|batch| {
            batch
                .iter()
                .all(|record| matches!(record, MetadataRecord::V1Partition(_)))
        })
        .cloned()
        .collect()
}

/// The `(broker, fencing change)` pairs of the registration changes a liveness
/// tick published, in submission order.
pub fn fencing_updates(
    batches: &[Vec<MetadataRecord>],
) -> Vec<(u64, krabka_metadata::FencingChange)> {
    batches
        .iter()
        .flatten()
        .filter_map(|record| match record {
            MetadataRecord::V1BrokerRegistrationChange(change) => {
                Some((change.node_id.0, change.fenced))
            }
            _ => None,
        })
        .collect()
}

/// The one `PartitionRecord` in a change list that also carries records of
/// other kinds -- a KIP-966 failover appends the republished ELR beside the
/// election, so the election cannot be read by position. Panics unless
/// exactly one partition change is there.
pub fn elected_partition(changes: &[MetadataRecord]) -> &PartitionRecord {
    let mut partitions = changes.iter().filter_map(|record| match record {
        MetadataRecord::V1Partition(pr) => Some(pr),
        MetadataRecord::V1PartitionUpdate(update) => Some(&update.partition),
        _ => None,
    });
    let first = partitions
        .next()
        .unwrap_or_else(|| panic!("expected a partition change, got {changes:?}"));
    assert!(
        partitions.next().is_none(),
        "expected exactly one partition change, got {changes:?}"
    );
    first
}

/// Extract the single-element `PartitionRecord` from a one-entry change
/// list. Panics if the list is empty or carries a non-partition record.
pub fn one_partition_change(changes: &[MetadataRecord]) -> &PartitionRecord {
    assert!(
        changes.len() == 1,
        "expected exactly one change, got {changes:?}"
    );
    match &changes[0] {
        MetadataRecord::V1Partition(pr) => pr,
        MetadataRecord::V1PartitionUpdate(update) => &update.partition,
        other => panic!("expected a partition change, got {other:?}"),
    }
}

/// Liveness where every broker in `dead` has an expired session and every
/// broker in `alive` heartbeated inside the current window. The `tick`
/// that flips `dead` to `Dead` runs here, so the caller sees no edge.
pub async fn liveness_with_dead(dead: &[u64], alive: &[u64]) -> Arc<ControllerLivenessState> {
    let clock = TestClock::new();
    let l = ControllerLivenessState::with_test_clock(std::time::Duration::from_millis(10), &clock);
    for &n in dead {
        l.record_heartbeat(n).await;
    }
    clock.advance(std::time::Duration::from_millis(11));
    for &n in alive {
        l.record_heartbeat(n).await;
    }
    let _ = l.tick().await;
    Arc::new(l)
}

pub fn register_brokers(img: &mut MetadataImage, ids: &[u64]) {
    for &id in ids {
        register_broker_with_dirs(img, id, vec![]);
    }
}

/// Register broker `id` with `log_dirs` as its online directories.
pub fn register_broker_with_dirs(img: &mut MetadataImage, id: u64, log_dirs: Vec<uuid::Uuid>) {
    img.apply(&MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            incarnation_id: Uuid::from_u128(u128::from(id)),
            log_dirs,
            ..crate::test_support::broker_registration(krabka_raft::NodeId(id))
        },
    ));
}
