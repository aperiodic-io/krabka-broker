//! Fixtures the partition-writer unit tests share: batch builders, a log
//! opener, and the stub sequencer and WAL the diskless tests drive.

use {
    super::{run_with_sequencer, writer_macros::run_writer},
    crate::{
        config::BrokerConfig,
        delivery::DeliveryHandles,
        diskless::hot_tail::HotTailCache,
        error::BrokerError,
        log_dir_status::LogDirRegistry,
        partition::{AppendedBatch, ProduceData, ProduceJob, WriterMessage},
        producer_state::ProducerState,
        replica_state::ReplicaState,
        test_support::{EmptyPayloadBatchSetup, RecordCount},
        wal::{OffsetSequencer, SharedWal, WalStore},
    },
    krabka_ids::PartitionIndex,
    krabka_log::{Log, LogConfig, Offset},
    krabka_protocol::records::RecordBatch,
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    tokio::sync::{Notify, mpsc, oneshot},
};

#[derive(Debug)]
pub(super) struct FixedStamp(pub(super) u64);

impl krabka_log::StampSource for FixedStamp {
    fn next_stamp(&self) -> u64 { self.0 }
}

struct TestSequencer {
    next: AtomicI64,
}

#[async_trait::async_trait]
impl OffsetSequencer for TestSequencer {
    async fn assign(
        &self,
        _topic: &str,
        _partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError> {
        let base = self.next.fetch_add(i64::from(count), Ordering::SeqCst);
        Ok(Offset(base))
    }
}

pub(super) fn test_sequencer() -> Arc<dyn OffsetSequencer> {
    Arc::new(TestSequencer {
        next: AtomicI64::new(0),
    })
}

pub(super) use crate::test_support::default_records_batch as sample_batch;

pub(super) struct GatedWal {
    sync_started: Mutex<Option<oneshot::Sender<()>>>,
    release_sync: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    pub(super) trimmed_to: AtomicI64,
    trim_failures: AtomicUsize,
    hot_tail: Option<(Arc<HotTailCache>, uuid::Uuid, PartitionIndex)>,
}

impl GatedWal {
    pub(super) fn gated_channel() -> (Arc<Self>, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (sync_started_tx, sync_started_rx) = oneshot::channel();
        let (release_sync_tx, release_sync_rx) = oneshot::channel();
        (
            Arc::new(Self::new(sync_started_tx, release_sync_rx)),
            sync_started_rx,
            release_sync_tx,
        )
    }

    pub(super) fn new(
        sync_started: oneshot::Sender<()>,
        release_sync: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            sync_started: Mutex::new(Some(sync_started)),
            release_sync: tokio::sync::Mutex::new(Some(release_sync)),
            trimmed_to: AtomicI64::new(-1),
            trim_failures: AtomicUsize::new(0),
            hot_tail: None,
        }
    }

    #[must_use]
    pub(super) fn fail_trim_times(self, failures: usize) -> Self {
        self.trim_failures.store(failures, Ordering::SeqCst);
        self
    }

    #[must_use]
    pub(super) fn with_hot_tail(
        mut self,
        cache: Arc<HotTailCache>,
        topic_id: uuid::Uuid,
        partition: PartitionIndex,
    ) -> Self {
        self.hot_tail = Some((cache, topic_id, partition));
        self
    }
}

#[async_trait::async_trait]
impl WalStore for GatedWal {
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        if let Some(started) = self.sync_started.lock().unwrap().take() {
            let _ = started.send(());
        }
        let release = self
            .release_sync
            .lock()
            .await
            .take()
            .expect("sync release receiver present");
        release.await.expect("sync release sent");
        Ok(leo)
    }

    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        if self
            .trim_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(BrokerError::Replication("injected WAL trim failure".into()));
        }
        self.trimmed_to.store(new_start.0, Ordering::SeqCst);
        Ok(new_start)
    }

    fn invalidate_hot_tail(&self) {
        if let Some((cache, topic_id, partition)) = &self.hot_tail {
            cache.remove_partition(*topic_id, *partition);
        }
    }
}

pub(super) fn open_log_with_records(path: &std::path::Path, records: i32) -> Log {
    let mut log = Log::open(path, LogConfig::default()).expect("open log");
    if records > 0 {
        log.append(&mut sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(records),
        }))
        .expect("append");
    }
    log
}

/// Writer services and signals, with each test overriding only what it observes.
/// The fixture owns no log or sender: callers choose explicitly whether the
/// spawned writer receives their log by move or by clone.
pub(super) struct WriterOptions {
    pub(super) topic: String,
    pub(super) partition: PartitionIndex,
    pub(super) append_notify: Arc<Notify>,
    pub(super) replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub(super) hw_advance_notify: Arc<Notify>,
    pub(super) delivery: DeliveryHandles,
    pub(super) log_dir_status: LogDirRegistry,
    pub(super) producer_state: Arc<ProducerState>,
    pub(super) wal: Option<SharedWal>,
    pub(super) max_produce_group: usize,
    pub(super) sequencer: Option<Arc<dyn OffsetSequencer>>,
}

impl WriterOptions {
    pub(super) fn replica_handles(&self) -> (Arc<tokio::sync::Mutex<ReplicaState>>, Arc<Notify>) {
        (
            Arc::clone(&self.replica_state),
            Arc::clone(&self.hw_advance_notify),
        )
    }
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            topic: "t".into(),
            partition: PartitionIndex(0),
            append_notify: Arc::new(Notify::new()),
            replica_state: Arc::new(tokio::sync::Mutex::new(ReplicaState::new())),
            hw_advance_notify: Arc::new(Notify::new()),
            delivery: DeliveryHandles::new(),
            log_dir_status: LogDirRegistry::default(),
            producer_state: Arc::new(ProducerState::new()),
            wal: None,
            max_produce_group: BrokerConfig::default().max_produce_group,
            sequencer: None,
        }
    }
}

pub(super) fn spawn_writer(
    dir: &std::path::Path,
    log: Arc<Mutex<Log>>,
    receiver: mpsc::Receiver<WriterMessage>,
    options: WriterOptions,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_writer!(
        options.topic, options.partition, log,
        Arc::new(arc_swap::ArcSwap::from_pointee(dir.to_path_buf())), receiver,
        options.append_notify, options.replica_state, options.hw_advance_notify,
        options.delivery, options.log_dir_status, options.producer_state, options.wal;
        options.max_produce_group, options.sequencer,
    ))
}

pub(super) fn open_default_log(dir: &std::path::Path) -> Arc<Mutex<Log>> {
    Arc::new(Mutex::new(
        Log::open(dir, LogConfig::default()).expect("open log"),
    ))
}

/// The default fixture for tests that retain their log after spawning.
/// Destructuring it preserves the original caller-owned log and notify handles.
pub(super) struct DefaultWriter {
    pub(super) dir: tempfile::TempDir,
    pub(super) log: Arc<Mutex<Log>>,
    pub(super) sender: mpsc::Sender<WriterMessage>,
    pub(super) writer: tokio::task::JoinHandle<()>,
    pub(super) notify: Arc<Notify>,
}

pub(super) fn default_writer() -> DefaultWriter {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (sender, receiver) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let writer = spawn_writer(dir.path(), log.clone(), receiver, WriterOptions {
        append_notify: notify.clone(),
        ..Default::default()
    });
    DefaultWriter {
        dir,
        log,
        sender,
        writer,
        notify,
    }
}

pub(super) type ProduceAck = oneshot::Receiver<Result<AppendedBatch, BrokerError>>;

/// Queue an owned batch without waiting for its append. Callers decide when to
/// receive or drop the ack, so notification and group-draining tests retain
/// their original ordering.
pub(super) async fn queue_batch(
    sender: &mpsc::Sender<WriterMessage>,
    batch: RecordBatch,
) -> ProduceAck {
    let (ack, receiver) = oneshot::channel();
    sender
        .send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(batch),
            ack,
            producer_check: None,
        }))
        .await
        .expect("send produce");
    receiver
}

pub(super) async fn replica_with_isr(nodes: &[u64]) -> Arc<tokio::sync::Mutex<ReplicaState>> {
    let replica = Arc::new(tokio::sync::Mutex::new(ReplicaState::new()));
    let nodes: Vec<_> = nodes.iter().copied().map(krabka_audit::NodeId).collect();
    replica.lock().await.install_isr(
        &nodes,
        &nodes,
        krabka_audit::NodeId(1),
        std::time::Instant::now(),
    );
    replica
}

/// RF=1 writer notifications and replica state for high-watermark tests.
pub(super) struct ObservedWriter {
    pub(super) writer: tokio::task::JoinHandle<()>,
    pub(super) replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub(super) hw_advance_notify: Arc<Notify>,
}

pub(super) async fn observed_single_replica_writer(
    dir: &std::path::Path,
    log: Arc<Mutex<Log>>,
    rx: mpsc::Receiver<WriterMessage>,
    update: impl FnOnce(&mut WriterOptions),
) -> ObservedWriter {
    let mut options = single_replica_options().await;
    update(&mut options);
    observed_writer(dir, log, rx, options)
}

pub(super) fn observed_writer(
    dir: &std::path::Path,
    log: Arc<Mutex<Log>>,
    rx: mpsc::Receiver<WriterMessage>,
    options: WriterOptions,
) -> ObservedWriter {
    let (replica_state, hw_advance_notify) = options.replica_handles();
    let writer = spawn_writer(dir, log, rx, options);
    ObservedWriter {
        writer,
        replica_state,
        hw_advance_notify,
    }
}

pub(super) async fn single_replica_options() -> WriterOptions {
    WriterOptions {
        replica_state: replica_with_isr(&[1]).await,
        ..Default::default()
    }
}

/// A running writer with a one-slot queue, retaining caller ownership of its log.
pub(super) fn spawn_default_writer(
    path: &std::path::Path,
    log: Arc<Mutex<Log>>,
) -> (mpsc::Sender<WriterMessage>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(path, log, rx, WriterOptions::default());
    (tx, writer)
}
