//! Writer-loop tests for the diskless path, where an external sequencer
//! assigns offsets and the WAL, not the local log, decides durability.

use {
    super::*,
    crate::{
        codes,
        diskless::hot_tail::HotTailCache,
        log_dir_status::LogDirRegistry,
        partition_writer::test_support::{
            GatedWal, observed_single_replica_writer, sample_batch, test_sequencer,
        },
        test_support::{EmptyPayloadBatchSetup, FakeMetadataSource, RecordCount},
        wal::{ControllerSequencer, LocalFsyncWal, OffsetSequencer, SharedWal},
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_log::{LogConfig, Offset},
    krabka_raft::RaftError,
    std::sync::atomic::Ordering,
    tempfile::tempdir,
    tokio::sync::oneshot,
};

fn seeded_log(path: &std::path::Path) -> Arc<Mutex<Log>> {
    let log = open_default_log(path);
    log.lock()
        .expect("lock")
        .append(&mut sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(4),
        }))
        .expect("append");
    log
}

fn trim_wal(failures: usize) -> (Arc<GatedWal>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let wal = Arc::new(GatedWal::new(started_tx, release_rx).fail_trim_times(failures));
    (wal, started_rx, release_tx)
}

fn check_trim_frontiers(wal: &GatedWal, log: &Mutex<Log>) {
    check!(wal.trimmed_to.load(Ordering::SeqCst) == 3);
    check!(log.lock().expect("lock").log_start_offset() == Offset(3));
}

/// The controller quorum refuses an offset reservation before it reserves
/// anything when it has no leader, when its leader moved, and when its new
/// leader has not yet committed its epoch. That is a leader election, not a
/// disk failure. The group appends nothing, the log directory stays online,
/// and the group answers `NOT_LEADER_OR_FOLLOWER`: the code that Kafka's
/// produce path gives when a broker cannot append because leadership moves.
/// The client then refreshes its metadata and sends the batch again.
#[tokio::test]
async fn a_reservation_refused_during_a_controller_election_answers_not_leader() {
    /// Builds the refusal of the controller for one case.
    type Refusal = fn() -> RaftError;
    // (what, the refusal of the controller)
    let cases: [(&str, Refusal); 4] = [
        ("no controller leader is known", || RaftError::NotLeader {
            current_leader: None,
        }),
        ("the controller leader moved", || RaftError::NotLeader {
            current_leader: Some(krabka_raft::NodeId(2)),
        }),
        ("a controller election is in progress", || {
            RaftError::LeaderUnknown
        }),
        (
            "the new controller leader has not committed its epoch",
            || RaftError::UncommittedTail,
        ),
    ];
    for (what, refusal) in cases {
        let dir = tempdir().expect("tempdir");
        let log = open_default_log(dir.path());
        let controller = Arc::new(
            FakeMetadataSource::builder()
                .term(7)
                .on_submit(move |_| Err(refusal()))
                .build(),
        );
        let sequencer: Arc<dyn OffsetSequencer> = Arc::new(ControllerSequencer::new(controller));
        let wal: SharedWal = Arc::new(LocalFsyncWal::new(log.clone()));
        let log_dir_status = LogDirRegistry::default();
        let (tx, rx) = mpsc::channel(1);
        let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions {
            log_dir_status: log_dir_status.clone(),
            wal: Some(wal),
            sequencer: Some(sequencer),
            ..Default::default()
        });

        let ack_rx = queue_batch(&tx, sample_batch(EmptyPayloadBatchSetup::default())).await;
        let answer = ack_rx
            .await
            .expect("ack recv")
            .map_err(|error| codes::from_broker_error(&error));
        drop(tx);
        writer.await.expect("writer join");

        let log_end = log.lock().expect("lock").log_end_offset();
        check!(
            (answer, log_end, log_dir_status.offline())
                == (Err(codes::NOT_LEADER_OR_FOLLOWER), Offset(0), Vec::new()),
            "{what}"
        );
    }
}

#[tokio::test]
async fn diskless_writer_acks_all_gates_on_durable_hw() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (wal, sync_started_rx, release_sync_tx) = GatedWal::gated_channel();
    let wal: Option<SharedWal> = Some(wal);
    let (tx, rx) = mpsc::channel(1);
    let observed = observed_single_replica_writer(dir.path(), log.clone(), rx, |options| {
        options.wal = wal;
        options.sequencer = Some(test_sequencer());
    })
    .await;

    let hw_waiter = observed.hw_advance_notify.notified();
    tokio::pin!(hw_waiter);

    let ack_rx = queue_batch(
        &tx,
        sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(3),
        }),
    )
    .await;

    let assigned = ack_rx.await.expect("ack recv").expect("append ok");
    assert!(assigned.base_offset == 0);
    tokio::time::timeout(std::time::Duration::from_secs(1), sync_started_rx)
        .await
        .expect("wal sync_durable did not start")
        .expect("sync start signal sent");

    assert!(observed.replica_state.lock().await.hw == 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut hw_waiter)
            .await
            .is_err()
    );

    release_sync_tx.send(()).expect("release sync");
    tokio::time::timeout(std::time::Duration::from_secs(1), &mut hw_waiter)
        .await
        .expect("hw_advance_notify did not fire");
    assert!(observed.replica_state.lock().await.hw == 3);

    drop(tx);
    observed.writer.await.expect("writer join");
}

#[tokio::test]
async fn diskless_acked_record_survives_reopen() {
    let dir = tempdir().expect("tempdir");
    {
        let log = open_default_log(dir.path());
        let wal: Option<SharedWal> = Some(Arc::new(LocalFsyncWal::new(log.clone())));
        let (tx, rx) = mpsc::channel(1);
        let append_notify = Arc::new(Notify::new());
        let replica_state = replica_with_isr(&[1]).await;
        let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions {
            append_notify,
            replica_state: replica_state.clone(),
            wal,
            sequencer: Some(test_sequencer()),
            ..Default::default()
        });

        let ack_rx = queue_batch(&tx, sample_batch(EmptyPayloadBatchSetup::default())).await;

        let assigned = ack_rx.await.expect("ack recv").expect("append ok");
        assert2::assert!((assigned.base_offset) == (0));

        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(10), writer)
            .await
            .expect("writer did not drain after local fsync")
            .expect("writer join");
        assert2::assert!((replica_state.lock().await.hw) == (1));
    }

    let log = Log::open(dir.path(), LogConfig::default()).expect("reopen log");
    assert2::assert!((log.log_end_offset()) == (Offset(1)));
}

#[tokio::test]
async fn diskless_writer_keeps_wal_and_local_trim_frontiers_equal() {
    let dir = tempdir().expect("tempdir");
    let log = seeded_log(dir.path());

    let (gated_wal, _sync_started_rx, _release_sync_tx) = trim_wal(0);
    let wal: SharedWal = gated_wal.clone();
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions {
        wal: Some(wal),
        ..Default::default()
    });

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack,
    })
    .await
    .expect("send trim");

    check!(ack_rx.await.expect("trim ack").expect("trim succeeds") == Offset(3));
    check_trim_frontiers(&gated_wal, &log);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn diskless_trim_retry_finishes_after_wal_failure() {
    let dir = tempdir().expect("tempdir");
    let log = seeded_log(dir.path());

    let (gated_wal, _sync_started_rx, _release_sync_tx) = trim_wal(1);
    let wal: SharedWal = gated_wal.clone();
    let (tx, rx) = mpsc::channel(2);
    let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions {
        wal: Some(wal),
        ..Default::default()
    });

    let (first_ack, first_rx) = oneshot::channel();
    tx.send(WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack: first_ack,
    })
    .await
    .expect("send first trim");
    check!(first_rx.await.expect("first trim ack").is_err());
    check!(log.lock().expect("lock").log_start_offset() == Offset(0));

    let (retry_ack, retry_rx) = oneshot::channel();
    tx.send(WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack: retry_ack,
    })
    .await
    .expect("send retry trim");
    check!(
        retry_rx
            .await
            .expect("retry trim ack")
            .expect("retry succeeds")
            == Offset(3)
    );
    check_trim_frontiers(&gated_wal, &log);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn diskless_writer_invalidates_hot_tail_after_log_rewrite() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    log.lock()
        .expect("lock")
        .append(&mut sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(2),
        }))
        .expect("append");

    let (sync_started_tx, _sync_started_rx) = oneshot::channel();
    let (_release_sync_tx, release_sync_rx) = oneshot::channel();
    let topic_id = uuid::Uuid::from_u128(13);
    let partition = PartitionIndex(0);
    let cache = Arc::new(HotTailCache::default());
    let mut encoded = BytesMut::new();
    sample_batch(EmptyPayloadBatchSetup {
        count: RecordCount(2),
    })
    .encode(&mut encoded)
    .expect("encode batch");
    let encoded = encoded.freeze();
    cache.insert_run(topic_id, partition, &encoded);
    let gated_wal = Arc::new(
        GatedWal::new(sync_started_tx, release_sync_rx).with_hot_tail(
            cache.clone(),
            topic_id,
            partition,
        ),
    );
    let wal: SharedWal = gated_wal.clone();
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(dir.path(), log, rx, WriterOptions {
        partition,
        wal: Some(wal),
        ..Default::default()
    });

    let (trunc_tx, trunc_rx) = oneshot::channel();
    tx.send(WriterMessage::Truncate {
        offset: Offset(0),
        ack: trunc_tx,
    })
    .await
    .expect("send truncate");

    trunc_rx
        .await
        .expect("truncate ack")
        .expect("truncate succeeds");
    check!(
        cache
            .get(topic_id, partition, 0, i64::MAX, usize::MAX)
            .is_none()
    );

    cache.insert_run(topic_id, partition, &encoded);
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::ResetTo {
        new_base: Offset(5),
        ack,
    })
    .await
    .expect("send reset");

    ack_rx.await.expect("reset ack").expect("reset succeeds");
    check!(
        cache
            .get(topic_id, partition, 0, i64::MAX, usize::MAX)
            .is_none()
    );

    drop(tx);
    writer.await.expect("writer join");
}
