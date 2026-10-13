//! Writer-loop tests for the arms that maintain the log rather than write
//! to it: a config swap and a log-start trim.

use {
    super::*,
    crate::partition_writer::test_support::{sample_batch, spawn_default_writer},
    assert2::assert,
    krabka_log::{LogConfig, Offset},
    tempfile::tempdir,
};

#[tokio::test]
async fn writer_set_log_config_swaps_config() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let (tx, writer) = spawn_default_writer(dir.path(), log.clone());

    let new_cfg = LogConfig {
        retention: Some(krabka_units::minutes(2)),
        ..LogConfig::default()
    };
    let (ack, ack_rx) = tokio::sync::oneshot::channel();
    tx.send(WriterMessage::SetLogConfig {
        config: new_cfg.clone(),
        ack,
    })
    .await
    .expect("send");
    ack_rx.await.expect("ack");

    let observed = log.lock().expect("lock").config_snapshot();
    assert!(observed.retention == new_cfg.retention);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_trim_to_offset_advances_log_start() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    // Pre-populate with two batches → LEO = 4.
    for _ in 0..2 {
        log.lock()
            .expect("lock")
            .append(&mut sample_batch().count(2).call())
            .expect("append");
    }

    let (tx, writer) = spawn_default_writer(dir.path(), log.clone());

    let (trim_ack, ack_rx) = tokio::sync::oneshot::channel();
    let msg = WriterMessage::TrimToOffset {
        new_start: Offset(3),
        ack: trim_ack,
    };
    tx.send(msg).await.expect("send");
    let new_start = ack_rx.await.expect("ack").expect("trim ok");
    assert!(new_start >= 3);
    assert!(log.lock().expect("lock").log_start_offset() == new_start);

    drop(tx);
    writer.await.expect("writer join");
}

#[derive(Debug)]
struct ObserveFlush(std::sync::Arc<tokio::sync::Notify>);

impl krabka_log::LogIo for ObserveFlush {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        file.sync_data()?;
        self.0.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn writer_flush_timer_runs_after_a_live_config_change_without_another_append() {
    for config in [
        LogConfig {
            flush_interval: Some(krabka_units::millis(10)),
            ..LogConfig::default()
        },
        LogConfig {
            flush_messages: Some(2),
            ..LogConfig::default()
        },
    ] {
        let dir = tempdir().unwrap();
        let log = open_default_log(dir.path());
        let flushed = std::sync::Arc::new(tokio::sync::Notify::new());
        {
            let mut log = log.lock().unwrap();
            log.append(&mut sample_batch().count(2).call()).unwrap();
            log.test_set_io(std::sync::Arc::new(ObserveFlush(flushed.clone())));
        }
        let (tx, writer) = spawn_default_writer(dir.path(), log.clone());
        let (ack, ack_rx) = tokio::sync::oneshot::channel();
        tx.send(WriterMessage::SetLogConfig { config, ack })
            .await
            .unwrap();
        ack_rx.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), flushed.notified())
            .await
            .expect("idle partition flushes after the live threshold changes");
        drop(tx);
        writer.await.unwrap();
    }
}
