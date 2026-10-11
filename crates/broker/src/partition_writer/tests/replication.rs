//! Writer-loop tests for the follower-facing arms: an append at a caller
//! supplied offset and the truncation that undoes one.

use assert2::assert;
use krabka_log::Offset;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition_writer::test_support::sample_batch,
    test_support::{EmptyPayloadBatchSetup, RecordCount},
};

#[tokio::test]
async fn writer_handles_replicate_with_caller_offset() {
    let DefaultWriter {
        dir: _dir,
        log,
        sender: tx,
        writer,
        notify: _notify,
    } = default_writer();

    // First replicate batch must start at offset 0 to match the
    // empty local log's `log_end_offset()`.
    let mut batch = sample_batch(EmptyPayloadBatchSetup {
        count: RecordCount(3),
    });
    batch.base_offset = 0;
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Replicate { batch, ack })
        .await
        .expect("send replicate");
    ack_rx.await.expect("ack recv").expect("replicate ok");
    assert!(log.lock().unwrap().log_end_offset() == 3);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_replicate_offset_mismatch_surfaces_error() {
    let DefaultWriter {
        dir: _dir,
        log,
        sender: tx,
        writer,
        notify: _notify,
    } = default_writer();

    // Kafka's `appendAsFollower` takes a first offset at or past the log end
    // offset and refuses one below it. Offset 7 leaves a hole, the way a
    // compacted leader's log does, and is taken.
    for (base_offset, expect_ok, expected_end) in [(7, true, 8), (5, false, 8)] {
        let mut batch = sample_batch(EmptyPayloadBatchSetup::default());
        batch.base_offset = base_offset;
        let (ack, ack_rx) = oneshot::channel();
        tx.send(WriterMessage::Replicate { batch, ack })
            .await
            .expect("send replicate");
        let result = ack_rx.await.expect("ack recv");
        if expect_ok {
            result.expect("replicate ok");
        } else {
            let err = result.expect_err("expected offset mismatch");
            assert!(matches!(err, crate::error::BrokerError::Log(_)));
        }
        assert!(log.lock().unwrap().log_end_offset() == expected_end);
    }

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_truncate_drops_records() {
    let DefaultWriter {
        dir: _dir,
        log,
        sender: tx,
        writer,
        notify: _notify,
    } = default_writer();

    // Produce two batches so the log has some data.
    for _ in 0..2 {
        let ack_rx = queue_batch(
            &tx,
            sample_batch(EmptyPayloadBatchSetup {
                count: RecordCount(2),
            }),
        )
        .await;
        ack_rx.await.expect("ack").expect("ok");
    }
    assert!(log.lock().unwrap().log_end_offset() == 4);

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Truncate {
        offset: Offset(0),
        ack,
    })
    .await
    .expect("send truncate");
    ack_rx.await.expect("ack").expect("truncate ok");
    assert!(log.lock().unwrap().log_end_offset() == 0);

    drop(tx);
    writer.await.expect("writer join");
}
