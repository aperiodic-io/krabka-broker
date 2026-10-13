//! Writer-loop tests for the high-watermark recompute that follows an
//! append, including the replication factors that must leave it where it
//! was.

use {
    super::*,
    crate::partition_writer::test_support::{ObservedWriter, observed_writer, sample_batch},
    assert2::assert,
    tempfile::tempdir,
};

async fn fixture(
    isr: &[u64],
) -> (
    tempfile::TempDir,
    mpsc::Sender<WriterMessage>,
    ObservedWriter,
) {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, rx) = mpsc::channel(1);
    let observed = observed_writer(dir.path(), log, rx, WriterOptions {
        replica_state: replica_with_isr(isr).await,
        ..Default::default()
    });
    (dir, tx, observed)
}

#[tokio::test]
async fn writer_fires_hw_notify_after_produce_when_rf_one() {
    let (_dir, tx, observed) = fixture(&[1]).await;

    let waiter = observed.hw_advance_notify.notified();
    tokio::pin!(waiter);

    let _ack_rx = queue_batch(&tx, sample_batch().count(2).call()).await;

    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("hw_advance_notify did not fire");

    assert!(observed.replica_state.lock().await.hw == 2);

    drop(tx);
    observed.writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_does_not_notify_hw_when_append_leaves_hw_unchanged() {
    let (_dir, tx, observed) = fixture(&[1, 2]).await;

    let waiter = observed.hw_advance_notify.notified();
    tokio::pin!(waiter);

    let ack_rx = queue_batch(&tx, sample_batch().call()).await;
    ack_rx.await.expect("ack").expect("append ok");

    assert!(observed.replica_state.lock().await.hw == 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), waiter)
            .await
            .is_err()
    );

    drop(tx);
    observed.writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_does_not_advance_hw_when_followers_lagging() {
    let (_dir, tx, observed) = fixture(&[1, 2, 3]).await;

    let ack_rx = queue_batch(&tx, sample_batch().count(3).call()).await;
    ack_rx.await.expect("ack").expect("append ok");

    assert!(observed.replica_state.lock().await.hw == 0);

    drop(tx);
    observed.writer.await.expect("writer join");
}
