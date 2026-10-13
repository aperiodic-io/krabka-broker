//! Fixtures the future-log unit tests share: a deterministic `StampSource`, a
//! default `MovePolicy`, and builders for a source `Partition` and the record
//! batches it holds.

use {
    super::MovePolicy,
    crate::{
        log_dir::future_partition_dir,
        partition::Partition,
        partition_registry::PartitionRegistry,
        test_support::{
            PartitionRecordsSetup, RecordCount, open_partition, partition_records_batch,
        },
        testing::single_value_batch,
        throttle::TokenBucket,
    },
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_units::{mebibytes, millis},
    std::{
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    },
};

#[derive(Debug)]
pub(super) struct TestStampSource(pub(super) AtomicU64);

impl krabka_log::StampSource for TestStampSource {
    fn next_stamp(&self) -> u64 { self.0.fetch_add(1, Ordering::Relaxed) }
}

pub(super) fn test_policy() -> MovePolicy {
    MovePolicy {
        retry_backoff: millis(5),
        read_chunk: mebibytes(1),
        throttle: Arc::new(TokenBucket::new()),
    }
}

/// Build a `Partition` rooted at `<log_dir>/<topic>-<partition>`
/// and do not use `Broker::start`. Returns the parent dir
/// and the `Arc<Partition>`.
pub(super) fn fixture_partition(
    log_dir: &Path,
    topic: &str,
    partition: PartitionIndex,
) -> Arc<Partition> {
    open_partition()
        .log_dir(log_dir)
        .topic(topic)
        .partition(partition.get())
        .call()
}

pub(super) fn append_records(part: &Arc<Partition>, setup: PartitionRecordsSetup) {
    let mut batch = partition_records_batch()
        .count(setup.count)
        .timestamp(setup.timestamp)
        .call();
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source records");
}

pub(super) struct FutureSourceSetup {
    pub count: RecordCount,
    pub stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
}

impl Default for FutureSourceSetup {
    fn default() -> Self {
        Self {
            count: RecordCount(3),
            stamp_source: None,
        }
    }
}

#[bon::builder]
pub(super) fn source_partition(
    primary: &Path,
    #[builder(into, default = FutureSourceSetup::default().count)] count: RecordCount,
    stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
) -> Arc<Partition> {
    let setup = FutureSourceSetup {
        count,
        stamp_source,
    };
    let part = fixture_partition(primary, "t", PartitionIndex(0));
    if let Some(stamp_source) = setup.stamp_source {
        part.log
            .lock()
            .expect("source log")
            .set_stamp_source(stamp_source)
            .expect("source stamp index");
    }
    append_records(&part, PartitionRecordsSetup {
        count: setup.count,
        ..Default::default()
    });
    part
}

/// Own both log roots for a populated source and its future replica.
pub(super) struct SourceFixture {
    pub partition: Arc<Partition>,
    pub target: tempfile::TempDir,
    pub primary: tempfile::TempDir,
}

#[bon::builder]
pub(super) fn source_fixture(
    #[builder(into, default = FutureSourceSetup::default().count)] count: RecordCount,
    stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
) -> SourceFixture {
    let setup = FutureSourceSetup {
        count,
        stamp_source,
    };
    let primary = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let partition = source_partition()
        .primary(primary.path())
        .count(setup.count)
        .maybe_stamp_source(setup.stamp_source)
        .call();
    SourceFixture {
        partition,
        target,
        primary,
    }
}

pub(super) struct RegisteredSourceFixture {
    pub partition: Arc<Partition>,
    pub future_logs: Arc<dashmap::DashMap<(String, PartitionIndex), Arc<super::FutureLogState>>>,
    pub partitions: Arc<PartitionRegistry>,
}

/// Publish a populated source in registries using its configured timestamp source.
#[bon::builder]
pub(super) fn registered_source_fixture(
    primary: &Path,
    #[builder(into, default = FutureSourceSetup::default().count)] count: RecordCount,
    stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
) -> RegisteredSourceFixture {
    let setup = FutureSourceSetup {
        count,
        stamp_source,
    };
    let partitions = Arc::new(PartitionRegistry::with_stamp_source(
        setup.stamp_source.clone(),
    ));
    let future_logs = Arc::new(dashmap::DashMap::new());
    let partition = source_partition()
        .primary(primary)
        .count(setup.count)
        .maybe_stamp_source(setup.stamp_source)
        .call();
    partitions.insert("t".into(), PartitionIndex(0), partition.clone());
    RegisteredSourceFixture {
        partition,
        future_logs,
        partitions,
    }
}

pub(super) struct PlantedFutureLog {
    pub path: PathBuf,
    pub cancel: tokio_util::sync::CancellationToken,
}

pub(super) fn plant_future_log(
    future_logs: &dashmap::DashMap<(String, PartitionIndex), Arc<super::FutureLogState>>,
    target: &Path,
) -> PlantedFutureLog {
    let (path, log) = open_future_log(target);
    let cancel = tokio_util::sync::CancellationToken::new();
    future_logs.insert(
        ("t".into(), PartitionIndex(0)),
        Arc::new(super::FutureLogState {
            target_log_dir: target.to_path_buf(),
            future_path: path.clone(),
            future_log: log,
            cancel: cancel.clone(),
            task: Mutex::new(None),
        }),
    );
    PlantedFutureLog { path, cancel }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub(super) struct RecordValueBytes(pub usize);

impl Default for RecordValueBytes {
    fn default() -> Self { Self(20) }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct FutureRecordSetup {
    pub value_size: RecordValueBytes,
    #[default(krabka_ids::LeaderEpoch(1))]
    pub leader_epoch: krabka_ids::LeaderEpoch,
}

pub(super) use append_epoch_batch as append_value_batch;

/// Appends one repeated-value record at the selected leader epoch.
#[bon::builder]
pub(super) fn append_epoch_batch(
    part: &Arc<Partition>,
    #[builder(into, default)] value_size: RecordValueBytes,
    #[builder(into, default = krabka_ids :: LeaderEpoch (1))] leader_epoch: krabka_ids::LeaderEpoch,
) {
    let setup = FutureRecordSetup {
        value_size,
        leader_epoch,
    };
    let mut batch = single_value_batch()
        .leader_epoch(setup.leader_epoch.0)
        .timestamp_ms(1_700_000_000)
        .value(Bytes::from(vec![b'x'; setup.value_size.0]))
        .call();
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch)
        .expect("append source batch");
}

/// Open a staged future replica at the same path a real move uses.
pub(super) fn open_future_log(target: &Path) -> (PathBuf, Arc<Mutex<krabka_log::Log>>) {
    let path = future_partition_dir(target, "t", 0);
    std::fs::create_dir_all(&path).unwrap();
    let log = krabka_log::Log::open(&path, krabka_log::LogConfig::default()).unwrap();
    (path, Arc::new(Mutex::new(log)))
}
