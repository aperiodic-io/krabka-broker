//! Writer-loop tests for the Produce arm: offset assignment, the ack
//! ordering, verbatim byte-exactness, group draining, and the append
//! notification.

use {
    super::*,
    crate::{
        partition::{ProduceData, ProduceJob},
        partition_writer::test_support::{GatedWal, sample_batch, test_sequencer},
        test_support::{EmptyPayloadBatchSetup, RecordCount},
        wal::SharedWal,
    },
    assert2::assert,
    krabka_log::Offset,
    tempfile::tempdir,
    tokio::sync::oneshot,
};

async fn writer_after_first_append() -> DefaultWriter {
    let fixture = default_writer();
    let ack = queue_batch(
        &fixture.sender,
        sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(3),
        }),
    )
    .await;
    let assigned = ack.await.expect("ack recv").expect("append ok");
    assert!(assigned.base_offset == 0);
    fixture
}

#[tokio::test]
async fn writer_appends_and_acks() {
    let DefaultWriter {
        dir: _dir,
        log: _log,
        sender: tx,
        writer,
        notify: _notify,
    } = writer_after_first_append().await;

    // Second append assigns offset 3.
    let ack_rx = queue_batch(
        &tx,
        sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(2),
        }),
    )
    .await;
    assert!(
        ack_rx
            .await
            .expect("ack recv 2")
            .expect("append 2 ok")
            .base_offset
            == 3
    );

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_groups_queued_produces_up_to_configured_cap() {
    const MAX_GROUP: usize = 2;

    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (sync_started_tx, sync_started_rx) = oneshot::channel();
    let (_release_sync_tx, release_sync_rx) = oneshot::channel();
    let wal: SharedWal = Arc::new(GatedWal::new(sync_started_tx, release_sync_rx));
    let (tx, rx) = mpsc::channel(3);

    for _ in 0..3 {
        let _ack_rx = queue_batch(&tx, sample_batch(EmptyPayloadBatchSetup::default())).await;
    }

    let writer = spawn_writer(dir.path(), log.clone(), rx, WriterOptions {
        wal: Some(wal),
        max_produce_group: MAX_GROUP,
        sequencer: Some(test_sequencer()),
        ..Default::default()
    });

    tokio::time::timeout(std::time::Duration::from_secs(10), sync_started_rx)
        .await
        .expect("first group did not reach WAL sync")
        .expect("first group reached WAL sync");
    assert!(log.lock().unwrap().log_end_offset() == Offset(2));

    writer.abort();
    let _ = writer.await;
    drop(tx);
}

#[tokio::test]
async fn durable_sync_ack_waits_for_diskless_wal() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (wal, sync_started_rx, release_sync_tx) = GatedWal::gated_channel();
    let wal: SharedWal = wal;
    let (tx, rx) = mpsc::channel(2);
    let writer = spawn_writer(dir.path(), log, rx, WriterOptions {
        wal: Some(wal),
        max_produce_group: 1,
        sequencer: Some(test_sequencer()),
        ..Default::default()
    });

    let append_ack_rx = queue_batch(&tx, sample_batch(EmptyPayloadBatchSetup::default())).await;
    assert!(
        append_ack_rx
            .await
            .expect("append ack")
            .expect("append")
            .base_offset
            == 0
    );

    let (durable_ack, mut durable_ack_rx) = oneshot::channel();
    tx.send(WriterMessage::SyncDurable {
        leo: Offset(1),
        ack: durable_ack,
    })
    .await
    .expect("send durable sync");
    sync_started_rx.await.expect("WAL sync started");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut durable_ack_rx)
            .await
            .is_err(),
        "durable ack must wait for WAL"
    );

    release_sync_tx.send(()).expect("release WAL sync");
    durable_ack_rx
        .await
        .expect("durable ack")
        .expect("durable sync");
    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_appends_and_acks_on_multi_thread_runtime() {
    let DefaultWriter {
        dir: _dir,
        log: _log,
        sender: tx,
        writer,
        notify: _notify,
    } = writer_after_first_append().await;

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_appends_verbatim_byte_exact() {
    use {krabka_log::VerbatimBatch, krabka_protocol::records::RecordBatch as ProtoBatch};

    let DefaultWriter {
        dir: _dir,
        log,
        sender: tx,
        writer,
        notify: _notify,
    } = default_writer();

    // "Producer" batch with a bogus base_offset + epoch the log overwrites.
    let mut producer = sample_batch(EmptyPayloadBatchSetup::default());
    producer.base_offset = 555;
    producer.partition_leader_epoch = -1;
    producer.max_timestamp = 1_234;
    let mut wire = bytes::BytesMut::new();
    producer.encode(&mut wire).unwrap();
    let wire = wire.freeze();

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Verbatim(VerbatimBatch {
            bytes: wire.clone(),
            last_offset_delta: 0,
            max_timestamp: 1_234,
            leader_epoch: krabka_log::LeaderEpoch(5),
            producer_id: krabka_log::ProducerId(-1),
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        }),
        ack,
        producer_check: None,
    }))
    .await
    .expect("send verbatim job");
    let assigned = ack_rx.await.expect("ack").expect("append ok");
    assert!(assigned.base_offset == 0);

    // Read back: bytes 21.. must equal the producer's, only offset+epoch changed.
    let r = log
        .lock()
        .unwrap()
        .read_raw(Offset(0), Offset(1), krabka_units::mebibytes(10))
        .unwrap();
    assert!(&r.bytes[21..] == &wire[21..], "CRC-covered region verbatim");
    assert!(&r.bytes[17..21] == &wire[17..21], "CRC unchanged");
    // Decodes with the assigned offset + stamped epoch.
    let mut cur: &[u8] = &r.bytes;
    let decoded = ProtoBatch::decode(&mut cur).unwrap();
    assert!(decoded.base_offset == 0);
    assert!(decoded.partition_leader_epoch == 5);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_fires_notify_after_append() {
    let DefaultWriter {
        dir: _dir,
        log: _log,
        sender: tx,
        writer,
        notify,
    } = default_writer();

    // Subscribe BEFORE sending so we don't miss the notification.
    let waiter = notify.notified();
    tokio::pin!(waiter);

    let _ack_rx = queue_batch(&tx, sample_batch(EmptyPayloadBatchSetup::default())).await;

    // Should wake within a short timeout.
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("notify did not fire");

    drop(tx);
    writer.await.expect("writer join");
}
