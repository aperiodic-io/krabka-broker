//! Fixture builders for unit tests that need a real partition: one over a
//! temporary log directory, one wired to a live writer task, and a helper that
//! appends records straight to the log.
//!
//! [`test_partition`] is `pub(crate)` because the fetch read path's own tests
//! drive [`crate::handlers::fetch`] against a partition too, and a second copy
//! of this fixture would be a second thing to keep in step with `Partition`.
//! [`test_partition_with_writer`] is `pub(crate)` for the same reason: the
//! group coordinator's offsets log appends through a real writer in its tests.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI32, AtomicU64},
};

use arc_swap::ArcSwap;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};
use tempfile::tempdir;
use tokio::sync::{Notify, mpsc};

use crate::{
    delivery::DeliveryHandles,
    partition::{
        Partition, WriterMessage, empty_marker_materialization, initial_replication_target,
    },
};

// Fails leader-epoch checkpoint writes as a full disk would.
krabka_macros::epoch_checkpoint_failure!(pub(crate) EpochCheckpointFull, StorageFull);

pub(crate) fn test_partition(hw_advance_notify: Arc<Notify>) -> (Partition, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let p = Partition {
        topic: "t".into(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        marker_materialization: empty_marker_materialization(),
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        hw_advance_notify,
        current_leader: Arc::new(AtomicU64::new(0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        delivery: DeliveryHandles::new(),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    };
    (p, dir)
}

/// Whether a commit fixture keeps the partition's initial leader or installs the test term.
#[derive(Clone, Copy, Default)]
pub(crate) enum CommitFixtureLeader {
    #[default]
    KeepPartitionDefault,
    BrokerOneAtEpochZero,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct CommitPartitionSetup {
    pub high_watermark: krabka_log::Offset,
    pub leader: CommitFixtureLeader,
}

/// A real temporary partition with the requested commit frontier and its own notifier.
pub(crate) async fn commit_partition(
    setup: CommitPartitionSetup,
) -> (Arc<Partition>, tempfile::TempDir, Arc<Notify>) {
    let notify = Arc::new(Notify::new());
    let (partition, directory) = test_partition(Arc::clone(&notify));
    let partition = Arc::new(partition);
    if matches!(setup.leader, CommitFixtureLeader::BrokerOneAtEpochZero) {
        partition.install_leader_change(1, 0).await;
    }
    partition.replica_state.lock().await.hw = setup.high_watermark;
    (partition, directory, notify)
}

pub(crate) fn test_partition_with_writer() -> (Partition, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let log_dir = Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf()));
    let p = Partition::writer_fixture("t", log, log_dir, |identity, storage, rx, signals| {
        tokio::spawn(crate::partition_writer::run(
            identity,
            storage,
            rx,
            signals,
            (
                crate::log_dir_status::LogDirRegistry::default(),
                Arc::new(crate::producer_state::ProducerState::new()),
                None,
            ),
        ))
    });
    (p, dir)
}

/// Project checkpoint rows without choosing the caller's mutex-poison diagnostic.
pub(crate) fn epoch_history(log: &Log) -> Vec<(i32, i64)> {
    log.epoch_checkpoint()
        .entries()
        .iter()
        .map(|entry| (entry.epoch.0, entry.start_offset.0))
        .collect()
}

/// Install the three-replica setup; expected ISR rows remain independent in callers.
pub(crate) async fn install_three_replica_isr(partition: &Partition) {
    let replicas = [
        krabka_audit::NodeId(1),
        krabka_audit::NodeId(2),
        krabka_audit::NodeId(3),
    ];
    partition
        .install_isr(&replicas, &replicas, krabka_audit::NodeId(1))
        .await;
}

pub(super) fn append_records(p: &Partition, setup: crate::test_support::PartitionRecordsSetup) {
    let mut batch = crate::test_support::partition_records_batch(setup);
    p.log
        .lock()
        .expect("log mutex")
        .append(&mut batch)
        .expect("append");
}
