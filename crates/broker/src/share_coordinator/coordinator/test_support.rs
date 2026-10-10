//! Shared fixtures for the `ShareCoordinator` unit tests: a `StateBatch`
//! builder, a real `__share_group_state` partition with a live writer, a
//! coordinator that owns all of them, a leadership seed, and a reader of the
//! records a state partition holds.
//!
//! Every submodule of `coordinator` needs the same live partition logs, so the
//! builders live in one place instead of once per test module.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
use qubit_clock::{ManualMonotonicClock, ManualWallClock};

use super::ShareCoordinator;
use crate::{
    partition_registry::PartitionRegistry,
    share_coordinator::{
        bootstrap,
        config::ShareCoordinatorConfig,
        persistence::{
            KEY_SHARE_SNAPSHOT, ShareSnapshotValue, ShareUpdateValue, StateBatch, parse_state_key,
        },
    },
};

/// The wall-clock time, in milliseconds, at which every test coordinator
/// starts.
pub(crate) const NOW_MS: i64 = 1_700_000_000_000;

use std::ops::RangeInclusive;

#[derive(Clone, Copy, Default)]
pub(crate) enum FixtureDeliveryState {
    #[default]
    Available,
    Acknowledged,
    Archiving,
    Archived,
}
pub(crate) use crate::share_partition::state::test_support::DeliveryCount as DeliveryAttemptCount;

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct StateBatchSetup {
    #[default(Offset(0)..=Offset(9))]
    pub bounds: RangeInclusive<Offset>,
    pub delivery: FixtureDeliveryState,
    #[default(DeliveryAttemptCount(1))]
    pub attempts: DeliveryAttemptCount,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct StateSeedEpoch(pub i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct StateSeedSetup {
    #[default(uuid::Uuid::from_bytes([5; 16]))]
    pub topic: uuid::Uuid,
    #[default(StateSeedEpoch(1))]
    pub epoch: StateSeedEpoch,
    pub offset: Offset,
}

pub(super) fn batch(bounds: RangeInclusive<Offset>) -> StateBatch {
    state_batch(StateBatchSetup {
        bounds,
        ..Default::default()
    })
}

/// A literal batch fixture shared by wire-layout and state-combination tests.
pub(crate) fn state_batch(setup: StateBatchSetup) -> StateBatch {
    let StateBatchSetup {
        bounds,
        delivery,
        attempts,
    } = setup;
    let (first_offset, last_offset) = bounds.into_inner();
    StateBatch {
        first_offset,
        last_offset,
        delivery_state: match delivery {
            FixtureDeliveryState::Available => 0,
            FixtureDeliveryState::Acknowledged => 2,
            FixtureDeliveryState::Archiving => 3,
            FixtureDeliveryState::Archived => 4,
        },
        delivery_count: attempts.0,
    }
}

/// Builds a real `__share_group_state`-`p` partition and registers it.
///
/// The partition has a live writer, and it leads on node 1 at leader epoch
/// 0. This function mirrors `fixture_partition` in `partition_registry`.
pub(crate) fn open_state_partition(reg: &PartitionRegistry, log_dir: &Path, p: i32) {
    let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, p);
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = Log::open(&part_dir, LogConfig::default()).unwrap();
    let part = crate::broker::spawn_partition_with_replication_target(
        bootstrap::TOPIC.to_string(),
        // The metadata reconcile installs the leader of the image before the
        // partition is visible: here this broker, node 1, at epoch 0.
        crate::partition::ReplicationTarget {
            topic_id: None,
            leader_node_id: krabka_raft::NodeId(1),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
        },
        PartitionIndex(p),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    reg.insert(bootstrap::TOPIC.into(), PartitionIndex(p), part);
}

/// Lead the open state logs, then initialize partition zero of group `g`.
pub(super) async fn initialize_led_group(
    coordinator: &ShareCoordinator,
    setup: StateSeedSetup,
) -> krabka_metadata::MetadataImage {
    lead_all(coordinator).await;
    let image = image_with_topic(setup.topic, 1);
    coordinator
        .initialize(&image, "g", setup.topic, 0, setup.epoch.0, setup.offset)
        .await
        .unwrap();
    image
}

/// Register the requested number of live, initially led state partitions.
pub(crate) fn open_state_partitions(dir: &Path, count: i32) -> Arc<PartitionRegistry> {
    let registry = Arc::new(PartitionRegistry::new());
    for partition in 0..count {
        open_state_partition(&registry, dir, partition);
    }
    registry
}

/// A manual wall clock that reads [`NOW_MS`] until a test moves it.
pub(crate) fn manual_clock() -> Arc<ManualWallClock> {
    ManualMonotonicClock::new_shared()
        .new_wall_clock(UNIX_EPOCH + Duration::from_millis(NOW_MS.unsigned_abs()))
}

/// Moves `clock` to `ms` milliseconds after [`NOW_MS`].
pub(crate) fn set_clock(clock: &ManualWallClock, ms: u64) {
    clock.reanchor(UNIX_EPOCH + Duration::from_millis(NOW_MS.unsigned_abs() + ms));
}

/// A coordinator with `config` over all of its state partitions, open
/// locally, and the manual clock that drives it.
pub(crate) fn configured_coordinator(
    dir: &Path,
    config: ShareCoordinatorConfig,
) -> (
    ShareCoordinator,
    Arc<PartitionRegistry>,
    Arc<ManualWallClock>,
) {
    let reg = open_state_partitions(dir, config.state_topic_num_partitions);
    let clock = manual_clock();
    let coord = ShareCoordinator::with_wall_clock(
        krabka_audit::NodeId(1),
        reg.clone(),
        config,
        Arc::clone(&clock) as Arc<dyn qubit_clock::WallClock>,
    );
    (coord, reg, clock)
}

/// A coordinator that leads every state partition it touches.
///
/// All 50 `__share_group_state` partitions are open locally.
pub(super) fn coordinator(dir: &Path) -> (ShareCoordinator, Arc<PartitionRegistry>) {
    let (coord, reg, _clock) = configured_coordinator(dir, ShareCoordinatorConfig::default());
    (coord, reg)
}

pub(super) async fn lead_all(coord: &ShareCoordinator) {
    coord.lead_all_partitions_for_test().await;
}

/// One record of a state partition log, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Logged {
    Snapshot(ShareSnapshotValue),
    Update(ShareUpdateValue),
    Tombstone,
}

/// Decoded records appended after the caller's previous log-length snapshot.
pub(crate) fn logged_since(
    coordinator: &ShareCoordinator,
    state_partition: PartitionIndex,
    before: usize,
) -> Vec<Logged> {
    logged_records(coordinator, state_partition)
        .into_iter()
        .skip(before)
        .map(|(_, logged)| logged)
        .collect()
}

/// Every record of `state_partition` from its log start, with its offset.
pub(crate) fn logged_records(
    coord: &ShareCoordinator,
    state_partition: PartitionIndex,
) -> Vec<(Offset, Logged)> {
    let part = coord
        .partitions
        .get(bootstrap::TOPIC, state_partition)
        .expect("state partition open");
    let mut records = Vec::new();
    let mut offset = part.log_start_offset();
    loop {
        let out = part
            .read_log(offset, krabka_units::mebibytes(1))
            .expect("read state partition");
        if out.batches.is_empty() {
            return records;
        }
        for batch in &out.batches {
            for rec in &batch.records {
                let key = parse_state_key(rec.key.as_ref().expect("key")).expect("state key");
                let logged = match (&rec.value, key.record_type) {
                    (None, _) => Logged::Tombstone,
                    (Some(value), KEY_SHARE_SNAPSHOT) => {
                        Logged::Snapshot(ShareSnapshotValue::decode(value).expect("snapshot"))
                    }
                    (Some(value), _) => {
                        Logged::Update(ShareUpdateValue::decode(value).expect("update"))
                    }
                };
                records.push((
                    Offset(batch.base_offset + i64::from(rec.offset_delta)),
                    logged,
                ));
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
}

/// A metadata image that holds one data topic, `t`, with id `topic_id` and
/// `partitions` partitions.
pub(crate) fn image_with_topic(
    topic_id: uuid::Uuid,
    partitions: i32,
) -> krabka_metadata::MetadataImage {
    image_with_topics(&[(topic_id, partitions)])
}

/// A metadata image that holds one data topic `t<i>` for each
/// `(topic_id, partitions)` of `topics`.
pub(crate) fn image_with_topics(topics: &[(uuid::Uuid, i32)]) -> krabka_metadata::MetadataImage {
    let mut records = Vec::new();
    for (index, (topic_id, partitions)) in topics.iter().enumerate() {
        let name = if index == 0 {
            "t".to_owned()
        } else {
            format!("t{index}")
        };
        records.push(krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: name.clone(),
                topic_id: *topic_id,
                partitions: *partitions,
                replication_factor: 1,
            },
        ));
        for partition in 0..*partitions {
            records.push(krabka_metadata::MetadataRecord::V1Partition(
                crate::coordinator::test_support::single_replica_partition(
                    &name,
                    krabka_ids::PartitionIndex(partition),
                    krabka_metadata::NodeId(1),
                ),
            ));
        }
    }
    krabka_metadata::MetadataImage::from_records(uuid::Uuid::nil(), &records)
}

/// A `WriteShareGroupState` partition.
pub(crate) fn share_write(
    epochs: (i32, i32),
    progress: (i64, i32),
    batches: Vec<StateBatch>,
) -> super::ShareWrite {
    super::ShareWrite {
        state_epoch: epochs.0,
        leader_epoch: epochs.1,
        start_offset: Offset(progress.0),
        delivery_complete_count: progress.1,
        batches,
    }
}
