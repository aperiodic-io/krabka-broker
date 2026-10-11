//! Fixtures that more than one of this module's unit-test modules needs: a
//! three-record batch to seed a log with, and a spawned `Partition` backed by
//! that log on disk.

use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use krabka_log::{Log, LogConfig};
use krabka_metadata::NodeId;
use krabka_protocol::records::RecordBatch;

use crate::{
    broker::spawn_partition,
    diskless::index_log::DisklessIndexLog,
    log_dir_status::LogDirRegistry,
    partition::Partition,
    producer_state::ProducerState,
    test_support::{
        RecordCount, RepeatedRecordsSetup, StorageMode, UnixMillis, repeated_records_batch,
    },
    time_util::now_ms,
};

/// A batch of `count` records, stamped with the wall clock the way a real
/// producer stamps one. The timestamp matters: `retention.ms` reads the
/// batch's `max_timestamp` off the index the flusher builds from this, and a
/// batch left at the epoch would be older than any retention window.
fn batch(count: i32) -> RecordBatch {
    RecordBatch {
        partition_leader_epoch: 0,
        ..repeated_records_batch(RepeatedRecordsSetup {
            count: RecordCount(count),
            timestamp: UnixMillis(now_ms()),
        })
    }
}

pub(super) fn orders_partition(root: &Path) -> Arc<Partition> {
    test_partition(root, FlusherPartitionSetup::default())
}

#[derive(Clone, Copy)]
pub(super) struct FlusherPartitionSetup<'a> {
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    pub storage: StorageMode,
    pub leader: NodeId,
}

impl Default for FlusherPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            partition: krabka_ids::PartitionIndex(0),
            storage: StorageMode::Diskless,
            leader: NodeId(1),
        }
    }
}

pub(super) fn test_partition(root: &Path, setup: FlusherPartitionSetup<'_>) -> Arc<Partition> {
    let FlusherPartitionSetup {
        topic,
        partition,
        storage,
        leader,
    } = setup;
    let partition_dir = root.join(format!("{topic}-{partition}"));
    std::fs::create_dir_all(&partition_dir).unwrap();
    let mut log = Log::open(&partition_dir, LogConfig::default()).unwrap();
    log.append(&mut batch(3)).unwrap();
    let handle = spawn_partition(
        topic.to_owned(),
        partition,
        root.to_path_buf(),
        log,
        LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        storage == StorageMode::Diskless,
    );
    handle.current_leader.store(leader.0, Ordering::Relaxed);
    handle
}

/// A real in-memory metadata log and its live diskless index projection.
pub(super) async fn test_index_log() -> DisklessIndexLog {
    DisklessIndexLog::start(krabka_remote_storage_topic::InProcessMetadataEventLog::new(
        1,
    ))
    .await
    .unwrap()
}

pub(super) use crate::diskless::index_log::test_support::flush_record;
