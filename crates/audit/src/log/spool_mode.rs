//! The AU-5 degraded path of the `AuditWriter`: spool on sink failure, and
//! replay in order once the sink recovers.
//!
//! Spool mode is sticky. Once a sink write fails, every record — chained event
//! and `Checkpoint` alike — goes to the durable `Spool` instead, so the order
//! of the hash chain is preserved. The replay ticker drains the spool back to
//! the sink and leaves spool mode only when the spool is empty.

use super::writer::AuditWriter;
use crate::sink::{AuditError, AuditRecord};

impl AuditWriter {
    /// Write to the sink, or to the spool.
    ///
    /// This method writes to the spool when the writer is in spool mode, which
    /// is sticky, or when the sink write fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, spooling = self.spooling)
    )]
    pub(super) async fn write_or_spool(
        &mut self,
        record: &AuditRecord,
        durable: bool,
    ) -> Result<(), AuditError> {
        if self.spooling {
            return self.spool_record(record, durable);
        }
        match self.sink.write(record.clone(), durable).await {
            Ok(()) => Ok(()),
            Err(error @ AuditError::Indeterminate(_)) => Err(error),
            Err(error) => {
                tracing::warn!(%error, "audit sink write failed; entering spool mode");
                self.spooling = true;
                self.spool_record(record, durable)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(class = ?record.class))]
    fn spool_record(&mut self, record: &AuditRecord, durable: bool) -> Result<(), AuditError> {
        let Some(spool) = &mut self.spool else {
            return Err(AuditError::Unavailable(
                "audit sink failed and spool is disabled".into(),
            ));
        };
        match spool.append(record) {
            Ok(true) => {
                if durable {
                    spool.sync()?;
                }
                self.stats.inc_spooled();
                self.stats.set_depth(spool.count(), spool.size());
                Ok(())
            }
            Ok(false) => {
                tracing::warn!("audit spool full; record dropped");
                Err(AuditError::Unavailable("audit spool is full".into()))
            }
            Err(e) => {
                tracing::error!(error = %e, "audit spool write failed; record dropped");
                Err(e)
            }
        }
    }

    /// Drain the spool to the sink in order.
    ///
    /// The writer exits spool mode when the spool is fully drained.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(replayed = tracing::field::Empty, total = tracing::field::Empty)
    )]
    pub(super) async fn try_replay(&mut self) -> Result<(), AuditError> {
        let Some(spool) = &mut self.spool else {
            self.spooling = false;
            return Ok(());
        };
        if spool.is_empty() {
            self.spooling = false;
            return Ok(());
        }
        let records = match spool.read_all() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "audit spool read failed during replay");
                return Ok(());
            }
        };
        let mut replayed = 0usize;
        while replayed < records.len() {
            let record = &records[replayed];
            spool.begin_replay(record)?;
            match self.sink.write(record.clone(), true).await {
                Ok(()) => {
                    if let Err(error) = spool.commit_replay(record) {
                        return Err(AuditError::Indeterminate(format!(
                            "replay append succeeded but cursor commit failed: {error}"
                        )));
                    }
                    replayed += 1;
                    self.stats.inc_replayed_by(1);
                    self.stats.set_depth(spool.count(), spool.size());
                }
                Err(error @ AuditError::Indeterminate(_)) => return Err(error),
                Err(_) => {
                    spool.abort_replay()?;
                    break;
                }
            }
        }
        let span = tracing::Span::current();
        span.record("replayed", replayed);
        span.record("total", records.len());
        if replayed == records.len() {
            spool.truncate()?;
            self.stats.set_depth(spool.count(), spool.size());
            self.spooling = false;
            tracing::info!(replayed, "audit spool drained; resumed direct topic writes");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::prelude::{ByteSizeExt as _, TimeExt as _, bytes};

    use super::*;
    use crate::{
        event::AuditEventClass,
        log::{
            AuditLog, AuditMode,
            test_support::{
                FailableSink, REPLAY_EVERY, ROOMY_CAP, await_until, failed_sink_stats,
                finish_writer, healthy_sink_stats, life, params, params_with_clock, product,
                record_sequences, spawn_writer, test_signer,
            },
        },
        spool::Spool,
        stats::AuditStats,
    };

    struct FailingSpoolFixture {
        clock: Arc<qubit_clock::ManualMonotonicClock>,
        stats: Arc<AuditStats>,
        sink: Arc<FailableSink>,
        log: Arc<AuditLog>,
        handle: tokio::task::JoinHandle<()>,
        _public_key: Option<Vec<u8>>,
        // Close every task and shared state before deleting its spool directory.
        _directory: tempfile::TempDir,
    }

    use crate::log::test_support::{AuditEventCount, CheckpointFrequency};

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct FailingSpoolSetup {
        #[default(ROOMY_CAP)]
        cap: krabka_units::ByteSize,
        frequency: CheckpointFrequency,
    }

    fn failing_spool(setup: FailingSpoolSetup) -> FailingSpoolFixture {
        let FailingSpoolSetup { cap, frequency } = setup;
        let checkpoint_every_n = match frequency {
            CheckpointFrequency::Disabled => None,
            CheckpointFrequency::Every(count) => Some(count.0),
        };
        let directory = tempfile::tempdir().unwrap();
        let (signer, public_key) = checkpoint_every_n.map_or((None, None), |_| {
            let (signer, public_key) = test_signer();
            (Some(signer), Some(public_key))
        });
        let (sink, stats) = failed_sink_stats();
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(directory.path(), cap).unwrap();
        let (mut params, clock) = params_with_clock(sink.clone(), spool, stats.clone());
        params.signer = signer;
        params.checkpoint_every_n = checkpoint_every_n.unwrap_or(0);
        FailingSpoolFixture {
            log,
            sink,
            stats,
            clock,
            handle: spawn_writer(rx, params),
            _public_key: public_key,
            _directory: directory,
        }
    }

    fn fail_closed_writer(
        sink: Arc<FailableSink>,
        spool: Spool,
    ) -> (Arc<AuditLog>, tokio::task::JoinHandle<()>) {
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let handle = spawn_writer(receiver, params(sink, spool, Arc::new(AuditStats::new())));
        (log, handle)
    }

    async fn check_required_failure(log: &AuditLog, reason: &str) {
        let error = log.emit_required(life(crate::NodeId(1))).await.unwrap_err();
        check!(error.to_string().contains(reason));
    }

    fn first_chained_record() -> AuditRecord {
        let mut record = AuditRecord::from_event(&life(crate::NodeId(0)), &product());
        record.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
        record
    }

    fn check_spool_within_capacity(directory: &std::path::Path, capacity: krabka_units::ByteSize) {
        check!(
            std::fs::metadata(directory.join("audit.spool"))
                .unwrap()
                .len()
                <= capacity.bytes_u64() + u64::try_from(crate::spool::FILE_HEADER_LEN).unwrap()
        );
    }

    fn record_size(directory: &std::path::Path, record: &AuditRecord) -> krabka_units::ByteSize {
        let mut spool = Spool::open(directory, ROOMY_CAP).unwrap();
        check!(spool.append(record).unwrap());
        spool.size()
    }

    async fn replay_spool(
        sink: &FailableSink,
        stats: &AuditStats,
        clock: &qubit_clock::ManualMonotonicClock,
    ) {
        sink.set_fail(false);
        clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        await_until("spool drained after replay", || stats.depth() == 0).await;
    }

    async fn drained_records(
        log: Arc<AuditLog>,
        handle: tokio::task::JoinHandle<()>,
        sink: &FailableSink,
        stats: &AuditStats,
        clock: &qubit_clock::ManualMonotonicClock,
    ) -> Vec<AuditRecord> {
        check!(sink.inner.records().is_empty());
        replay_spool(sink, stats, clock).await;
        finish_writer(log, handle).await;
        sink.inner.records()
    }

    #[tokio::test]
    async fn records_spool_on_sink_failure_then_replay_to_sink() {
        let fixture = failing_spool(FailingSpoolSetup::default());

        fixture.log.emit(life(crate::NodeId(1)));
        fixture.log.emit(life(crate::NodeId(2)));
        fixture.log.emit(life(crate::NodeId(3)));
        // wait until the writer has drained all three into the spool
        await_until("3 records spooled", || fixture.stats.spooled() >= 3).await;
        check!(fixture.stats.depth() >= 3);
        // Nothing reached the topic yet; drained_records checks this before replay.

        // topic recovers; fire the replay ticker by advancing the manual clock
        let recs = drained_records(
            fixture.log,
            fixture.handle,
            &fixture.sink,
            &fixture.stats,
            &fixture.clock,
        )
        .await;

        // all three chained records reached the sink, in order, with monotonic seq
        let seqs = record_sequences(&recs);
        check!(
            (seqs, fixture.stats.replayed() >= 3, fixture.stats.depth())
                == (
                    vec!["0".to_string(), "1".to_string(), "2".to_string()],
                    true,
                    0
                )
        );
    }

    #[tokio::test]
    async fn direct_writes_when_sink_healthy_do_not_spool() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, stats) = healthy_sink_stats(); // healthy
        let (log, rx) = AuditLog::new(16);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let h = spawn_writer(rx, params(sink.clone(), spool, stats.clone()));
        crate::log::test_support::emit_lifecycle(&log, &[crate::NodeId(1), crate::NodeId(2)]);
        finish_writer(log, h).await;
        check!((sink.inner.records().len(), stats.spooled(), stats.depth()) == (2, 0, 0));
    }

    #[tokio::test]
    async fn fail_closed_write_requests_durable_sink_acknowledgement() {
        let dir = tempfile::tempdir().unwrap();
        let (sink, stats) = healthy_sink_stats();
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let handle = spawn_writer(receiver, params(sink.clone(), spool, Arc::clone(&stats)));

        log.emit_required(life(crate::NodeId(1))).await.unwrap();
        finish_writer(log, handle).await;

        check!(
            (
                sink.durable_requests(),
                sink.inner.records().len(),
                stats.depth()
            ) == (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn indeterminate_durable_write_fails_stopped_without_spool_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailableSink::default());
        sink.set_indeterminate(true);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let (log, handle) = fail_closed_writer(sink, spool);

        check_required_failure(&log, "indeterminate").await;
        handle.await.unwrap();
        check!(
            log.emit_required(life(crate::NodeId(2)))
                .await
                .unwrap_err()
                .to_string()
                .contains("not running")
        );
        check!(Spool::open(dir.path(), ROOMY_CAP).unwrap().is_empty());
    }

    #[tokio::test]
    async fn indeterminate_replay_poison_survives_after_a_successful_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let mut chain = crate::chain::ChainState::new();
        for event in [life(crate::NodeId(1)), life(crate::NodeId(2))] {
            let mut record = AuditRecord::from_event(&event, &product());
            let (seq, previous) = chain.extend(&record.value);
            record.push_chain_headers(seq, &previous);
            check!(spool.append(&record).unwrap());
        }
        let sink = Arc::new(FailableSink::default());
        sink.set_indeterminate_after(1);
        let (log, receiver) = AuditLog::new(8);
        let (params, clock) = params_with_clock(sink.clone(), spool, Arc::new(AuditStats::new()));
        let handle = spawn_writer(receiver, params);

        tokio::task::yield_now().await;
        clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        handle.await.unwrap();
        check!(sink.inner.records().len() == 1);
        check!(matches!(
            Spool::open(dir.path(), ROOMY_CAP),
            Err(AuditError::Poisoned(_))
        ));
        drop(log);
    }

    #[tokio::test]
    async fn checkpoint_is_spooled_in_spool_mode_and_replayed_in_order() {
        // Topic down: records and count-triggered checkpoints spool together.
        // Emit a checkpoint after every 2 records.
        let fixture = failing_spool(FailingSpoolSetup {
            frequency: CheckpointFrequency::Every(AuditEventCount(2)),
            ..Default::default()
        });
        fixture.log.emit(life(crate::NodeId(0)));
        fixture.log.emit(life(crate::NodeId(1))); // 2 records → triggers a checkpoint, all spooled
        // 2 chained records + 1 count-triggered checkpoint all land in the spool
        await_until("2 records + checkpoint spooled", || {
            fixture.stats.spooled() >= 3
        })
        .await;
        // Nothing is on the topic yet; drained_records checks this before replay.
        let recs = drained_records(
            fixture.log,
            fixture.handle,
            &fixture.sink,
            &fixture.stats,
            &fixture.clock,
        )
        .await;
        // exactly 2 chained records, and at least one checkpoint, and the checkpoint
        // appears AFTER both chained records (it was spooled + replayed in order).
        check!(
            recs.iter()
                .filter(|r| r.class != AuditEventClass::Checkpoint)
                .count()
                == 2
        );
        let cp_idx = recs
            .iter()
            .position(|r| r.class == AuditEventClass::Checkpoint)
            .expect("checkpoint present");
        let chained_before = recs[..cp_idx]
            .iter()
            .filter(|r| r.class != AuditEventClass::Checkpoint)
            .count();
        check!(chained_before == 2); // checkpoint comes after the 2 records it covers
    }

    #[tokio::test]
    async fn spool_overflow_drops_and_updates_stats() {
        let dir = tempfile::tempdir().unwrap();
        // size of one chained record, to cap the spool at ~1 record
        let one = {
            let d2 = tempfile::tempdir().unwrap();
            let mut s = Spool::open(d2.path(), ROOMY_CAP).unwrap();
            let mut rec = AuditRecord::from_event(&life(crate::NodeId(0)), &product());
            rec.push_chain_headers(0, &crate::chain::GENESIS_HEAD);
            s.append(&rec).unwrap();
            s.size()
        };
        let (sink, stats) = failed_sink_stats(); // stay in spool mode (no replay), so drops accumulate
        let (log, rx) = AuditLog::new(64);
        let spool = Spool::open(dir.path(), one).unwrap();
        let h = spawn_writer(rx, params(sink.clone(), spool, stats.clone()));
        for i in 0..6 {
            log.emit(life(crate::NodeId(i)));
        }
        // wait until all six events are accounted for (each is spooled or dropped)
        await_until("6 events processed", || {
            stats.spooled() + stats.dropped() >= 6
        })
        .await;
        finish_writer(log, h).await;
        // Strict bounds chosen to also kill the "return constant 1" mutants.
        assert2::check!(stats.dropped() >= 2); // many overflowed (kills inc_dropped/() , dropped->0/1)
        assert2::check!(stats.spool_bytes() > bytes(1)); // ~one record is buffered (kills spool_bytes->0/1)
    }

    #[tokio::test]
    async fn full_spool_refuses_a_fail_closed_record() {
        let probe = tempfile::tempdir().unwrap();
        let existing = first_chained_record();
        let one = record_size(probe.path(), &existing);

        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), one).unwrap();
        check!(spool.append(&existing).unwrap());
        let sink = Arc::new(FailableSink::default());
        sink.set_fail(true);
        let (log, handle) = fail_closed_writer(sink, spool);

        check_required_failure(&log, "spool is full").await;

        finish_writer(log, handle).await;
    }

    #[tokio::test]
    async fn fail_open_spool_loss_is_followed_by_a_chain_marker() {
        let probe = tempfile::tempdir().unwrap();
        let first = first_chained_record();
        let one = record_size(probe.path(), &first);

        let dir = tempfile::tempdir().unwrap();
        let (sink, stats) = failed_sink_stats();
        let spool = Spool::open(dir.path(), one).unwrap();
        let (log, receiver) = AuditLog::new_with_mode_and_spool(8, AuditMode::FailOpen, &spool);
        let (params, clock) = params_with_clock(sink.clone(), spool, stats.clone());
        let handle = spawn_writer(receiver, params);

        crate::log::test_support::emit_lifecycle(
            &log,
            &[crate::NodeId(0), crate::NodeId(1), crate::NodeId(2)],
        );
        await_until("one spooled and two lost", || stats.dropped() == 2).await;
        check_spool_within_capacity(dir.path(), one);

        sink.set_fail(false);
        clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        await_until("record replayed", || sink.inner.records().len() == 1).await;
        clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        await_until("coalesced loss marker replayed", || {
            sink.inner.records().len() == 2
        })
        .await;
        let records = sink.inner.records();
        let lost: u64 = records
            .iter()
            .filter(|record| record.class == AuditEventClass::RecordsLost)
            .map(|record| {
                serde_json::from_slice::<serde_json::Value>(&record.value).unwrap()["records_lost"]
                    .as_u64()
                    .unwrap()
            })
            .sum();
        check!(lost == 2);
        check_spool_within_capacity(dir.path(), one);

        finish_writer(log, handle).await;
    }

    #[tokio::test]
    async fn partial_replay_keeps_remainder_then_drains() {
        let fixture = failing_spool(FailingSpoolSetup::default());
        fixture.log.emit(life(crate::NodeId(0)));
        fixture.log.emit(life(crate::NodeId(1)));
        fixture.log.emit(life(crate::NodeId(2)));
        await_until("3 records spooled", || fixture.stats.depth() == 3).await;

        // allow exactly 2 replay writes, then fail → partial replay
        fixture.sink.allow_n(2);
        fixture
            .clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        await_until("2 of 3 replayed", || {
            fixture.stats.replayed() == 2 && fixture.stats.depth() == 1
        })
        .await;
        check!(fixture.stats.depth() == 1); // remainder retained, still spooling

        // allow the rest; fire the replay ticker again to drain the remainder
        fixture.sink.allow_unlimited();
        fixture
            .clock
            .advance(REPLAY_EVERY.to_std())
            .expect("manual time moves forward");
        await_until("remainder drained", || fixture.stats.depth() == 0).await;

        finish_writer(fixture.log, fixture.handle).await;
        // all 3 chained records reached the sink exactly once, in seq order
        let seqs = record_sequences(&fixture.sink.inner.records());
        check!(seqs == vec!["0".to_string(), "1".to_string(), "2".to_string()]);
    }
}
