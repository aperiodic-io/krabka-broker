//! The fixture that the coordinator unit tests share.
//!
//! The module opens the state partitions and the data partitions of one
//! cluster, and it builds a coordinator over them. Every test module under
//! this coordinator needs that setup, so it lives in one file.

use std::sync::Arc;

use krabka_metadata::{MetadataRecord, NodeId};
use krabka_units::{Time, millis};
use tempfile::{TempDir, tempdir};

use super::BarrierCoordinator;
use crate::{
    barrier::{
        STATE_TOPIC,
        config::BarrierConfig,
        metrics::BrokerBarrierMetrics,
        state::GroupSpec,
        test_support::{metadata_source, open_partition, topic_records},
    },
    metadata_source::MetadataSource,
    metrics::BrokerMetrics,
    partition_registry::PartitionRegistry,
    test_support::FakeMetadataSource,
};

pub(crate) const GROUP: &str = "orders-cut";

fn config() -> BarrierConfig {
    BarrierConfig {
        state_topic_num_partitions: 4,
        injection_timeout: millis(30),
        retry_backoff: millis(1),
        retry_backoff_max: millis(2),
        min_injection_interval: millis(1),
        ..BarrierConfig::default()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RetainedCutCount(pub i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct GroupSpecSetup<'a> {
    #[default(&["orders"])]
    pub(crate) topics: &'a [&'a str],
    pub(crate) interval: Option<Time>,
    #[default(RetainedCutCount(4))]
    pub(crate) retained_cuts: RetainedCutCount,
}

pub(crate) fn spec(setup: GroupSpecSetup<'_>) -> GroupSpec {
    GroupSpec {
        topics: setup.topics.iter().map(|t| (*t).to_owned()).collect(),
        interval: setup.interval,
        retained_cuts: setup.retained_cuts.0,
    }
}

fn cluster_records() -> Vec<MetadataRecord> {
    [
        topic_records(STATE_TOPIC, 4, NodeId(1)),
        topic_records("orders", 2, NodeId(1)),
        topic_records("payments", 1, NodeId(1)),
    ]
    .concat()
}

// A broker that leads every state partition and every data partition.
pub(crate) struct Fixture {
    _dir: TempDir,
    pub(super) registry: Arc<PartitionRegistry>,
    pub(crate) source: Arc<FakeMetadataSource>,
    config: BarrierConfig,
}

impl Fixture {
    // Every partition of the cluster is open here, and this broker leads
    // all of them.
    pub(crate) fn new() -> Self {
        Self::with_data_partitions(&[("orders", 2), ("payments", 1)])
    }

    // Open the state partitions, and only the named data partitions.
    pub(super) fn with_data_partitions(data: &[(&str, i32)]) -> Self {
        let dir = tempdir().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        for p in 0..4 {
            open_partition(
                &registry,
                dir.path(),
                crate::test_support::StandalonePartitionSetup {
                    topic: STATE_TOPIC,
                    partition: krabka_ids::PartitionIndex(p),
                    ..Default::default()
                },
            );
        }
        for (topic, count) in data {
            for p in 0..*count {
                open_partition(
                    &registry,
                    dir.path(),
                    crate::test_support::StandalonePartitionSetup {
                        topic,
                        partition: krabka_ids::PartitionIndex(p),
                        ..Default::default()
                    },
                );
            }
        }
        Self {
            _dir: dir,
            registry,
            source: Arc::new(metadata_source(&cluster_records())),
            config: config(),
        }
    }

    pub(crate) async fn coordinator(&self) -> BarrierCoordinator {
        for (topic, count) in [("orders", 2), ("payments", 1)] {
            for partition in 0..count {
                if let Some(handle) = self
                    .registry
                    .get(topic, krabka_ids::PartitionIndex(partition))
                {
                    handle.install_leader_change(NodeId(1).get(), 3).await;
                }
            }
        }
        let controller: Arc<dyn MetadataSource> = Arc::clone(&self.source) as _;
        let coordinator = BarrierCoordinator::new(
            NodeId(1),
            Arc::clone(&self.registry),
            controller,
            Arc::default(),
            self.config.clone(),
            Arc::new(BrokerBarrierMetrics::new(BrokerMetrics::new())),
        );
        coordinator
            .refresh_leader_partitions(&self.source.current_image())
            .await;
        coordinator
    }

    /// Start the coordinator and create a group before driving its protocol.
    pub(crate) async fn coordinator_with_group(
        &self,
        group: &str,
        spec: GroupSpec,
    ) -> BarrierCoordinator {
        let coordinator = self.coordinator().await;
        coordinator
            .create_group(group, spec)
            .await
            .expect("the group is created");
        coordinator
    }

    // A coordinator that replayed the state partitions from the log.
    pub(super) async fn recovered(&self) -> BarrierCoordinator {
        let coordinator = self.coordinator().await;
        coordinator
            .recover(&self.source.current_image())
            .await
            .expect("recovery succeeds");
        coordinator
    }
}
