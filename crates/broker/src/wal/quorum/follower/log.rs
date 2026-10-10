//! The follower's own replica log: where it lives under a log directory, and
//! the trim, reset, and append operations that move it forward. Appended bytes
//! are fsynced before the durable-offset checkpoint advances. Trim publishes
//! its new checkpoint floor before deleting a prefix or advancing the log's
//! own floor, so recovery can finish an interrupted trim.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use krabka_ids::Offset;
use krabka_kraft_core::{LogOffsetMetadata, LogView as _};
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::RecordsPayload;
use krabka_raft::NodeId;

use super::{
    Config,
    checkpoint::{DURABLE_OFFSET_FILE, DurableRange, recover_durable_offset, write_durable_offset},
};
use crate::wal::quorum::{
    engine::{split_batches, sync_replica},
    log_view::ShardLog,
    registry::ShardId,
    shard_dir,
};

#[derive(Debug, Clone)]
pub(super) struct FollowerLog {
    pub(super) log: ShardLog,
    durable_offset_path: PathBuf,
}

impl FollowerLog {
    pub(super) fn open(config: &Config) -> Result<Self, crate::BrokerError> {
        let dir = config
            .log_dirs
            .iter()
            .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
            .find(|candidate| candidate.exists())
            .map_or_else(
                || {
                    let partition_dir = crate::log_dir::place_partition_dir(
                        &config.log_dirs,
                        &config.topic,
                        config.shard.partition.0,
                    );
                    partition_dir
                        .parent()
                        .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
                        .ok_or_else(|| {
                            crate::BrokerError::Replication("WAL log dir has no parent".into())
                        })
                },
                Ok,
            )?;
        Self::open_at(dir, &config.storage)
    }

    pub(super) fn open_at(dir: PathBuf, storage: &LogConfig) -> Result<Self, crate::BrokerError> {
        let mut log_config = storage.clone();
        log_config.validate_on_open = true;
        let durable_offset_path = dir.join(DURABLE_OFFSET_FILE);
        let mut log = Log::open(dir, log_config)?;
        recover_durable_offset(&mut log, &durable_offset_path)?;
        Ok(Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        })
    }

    #[cfg(test)]
    pub(super) fn for_log(log: Log) -> Self {
        let durable_offset_path = log.dir().join(DURABLE_OFFSET_FILE);
        write_durable_offset(
            &durable_offset_path,
            DurableRange {
                start: log.log_start_offset(),
                end: log.log_end_offset(),
            },
        )
        .unwrap();
        Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        }
    }

    pub(super) fn end_offset(&self) -> Offset {
        self.log.lock().log_end_offset()
    }

    pub(super) fn start_offset(&self) -> Offset {
        self.log.lock().log_start_offset()
    }

    pub(super) fn last_epoch(&self) -> i32 {
        self.log
            .lock()
            .epoch_checkpoint()
            .latest_epoch()
            .map_or(-1, |epoch| epoch.0)
    }

    pub(super) async fn trim_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        if offset <= self.start_offset() {
            return Ok(());
        }
        let follower = self.clone();
        run_blocking(move || follower.trim_to_blocking(offset)).await
    }

    pub(super) fn trim_to_blocking(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        let mut log = self.log.lock();
        if offset <= log.log_start_offset() {
            return Ok(());
        }
        let end = log.log_end_offset();
        let floor = Offset(krabka_verified::truncation_frontier(end.0, offset.0));
        // Sync before checkpointing even if a previous append's checkpoint
        // failed. Publish the trim intent before the log can remove bytes or
        // write its own floor; either on-disk floor then fits this WAL range.
        log.sync()?;
        write_durable_offset(
            &self.durable_offset_path,
            DurableRange { start: floor, end },
        )?;
        log.trim_to_offset(floor)?;
        log.sync()?;
        Ok(())
    }

    pub(super) async fn reset_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        self.mutate_and_checkpoint(move |log| {
            log.reset_to(offset)?;
            Ok(DurableRange {
                start: offset,
                end: offset,
            })
        })
        .await
    }

    pub(super) async fn truncate_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        self.mutate_and_checkpoint(move |log| {
            log.truncate_to(offset)?;
            Ok(DurableRange {
                start: log.log_start_offset(),
                end: log.log_end_offset(),
            })
        })
        .await
    }

    /// Fsync the mutation before publishing its new durable range.
    async fn mutate_and_checkpoint(
        &self,
        mutation: impl FnOnce(&mut Log) -> Result<DurableRange, crate::BrokerError> + Send + 'static,
    ) -> Result<(), crate::BrokerError> {
        let log = self.log.clone();
        let durable_offset_path = self.durable_offset_path.clone();
        run_blocking(move || {
            let mut log = log.lock();
            let range = mutation(&mut log)?;
            log.sync()?;
            write_durable_offset(&durable_offset_path, range)?;
            Ok(())
        })
        .await
    }

    /// Truncate this log after the leader answered a Fetch with the diverging
    /// epoch `diverging`, as KIP-595's `KafkaMetadataLog.truncateToEndOffset`
    /// does.
    ///
    /// The leader's end offset for the epoch is only an upper bound: this
    /// log's own copy of that epoch can end earlier, and whatever follows it
    /// here belongs to an epoch the leader does not hold. The follower
    /// truncates to the end of its own copy, capped at the leader's, or to
    /// where its own copy of an older epoch ends when it never held that epoch.
    /// Kafka's `KRaft` log treats epoch 0 as "no epoch" and skips the local
    /// lookup for it; a diskless partition's first leader epoch is 0, so the
    /// WAL looks it up like every other epoch.
    ///
    /// A leader that cannot place this follower's last epoch, because it is
    /// newer than every epoch in the leader's log, answers its log end and its
    /// latest, older epoch; this rule then truncates back to where this log's
    /// copy of that older epoch ends.
    /// A truncation point below the retained range, or one that would not
    /// shorten the log, resets it to the leader's log start instead, so a
    /// divergence always makes progress.
    pub(super) async fn resolve_divergence(
        &self,
        diverging: LogOffsetMetadata,
        leader_start: Offset,
        requested: Offset,
    ) -> Result<(), crate::BrokerError> {
        let local = self.log.end_offset_for_epoch(diverging.epoch);
        let truncation = Offset(if local.epoch == diverging.epoch {
            local.offset.min(diverging.offset)
        } else {
            local.offset
        });
        if truncation < self.start_offset() || truncation >= requested {
            self.reset_to(leader_start).await
        } else {
            self.truncate_to(truncation).await
        }
    }

    pub(super) async fn append(
        &self,
        requested: Offset,
        leader_end: Offset,
        records: Option<RecordsPayload>,
    ) -> Result<Offset, crate::BrokerError> {
        if self.end_offset() != requested {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower moved from requested offset {} to {}",
                requested.0,
                self.end_offset().0
            )));
        }
        let Some(records) = records else {
            return Ok(requested);
        };
        let mut encoded = BytesMut::with_capacity(records.payload_len());
        records.encode_to(&mut encoded).map_err(|error| {
            crate::BrokerError::Replication(format!("encode WAL fetch: {error}"))
        })?;
        let bytes: Bytes = encoded.freeze();
        let batches = split_batches(&bytes)?;
        let mut expected = requested;
        for batch in &batches {
            if batch.base_offset != expected {
                return Err(crate::BrokerError::Replication(format!(
                    "WAL fetch is not contiguous at {}, got {}",
                    expected.0, batch.base_offset.0
                )));
            }
            expected = Offset(batch.last_offset.0.checked_add(1).ok_or_else(|| {
                crate::BrokerError::Replication("WAL fetch offset overflow".into())
            })?);
        }
        if expected.cmp(&leader_end).is_gt() {
            return Err(crate::BrokerError::Replication(format!(
                "WAL fetch ends at {}, beyond leader LEO {}",
                expected.0, leader_end.0
            )));
        }
        sync_replica(self.log.clone(), batches).await?;
        let actual = self.end_offset();
        if actual != expected {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower ended at {}, expected {}",
                actual.0, expected.0
            )));
        }
        let durable_offset_path = self.durable_offset_path.clone();
        let start = self.start_offset();
        run_blocking(move || {
            write_durable_offset(&durable_offset_path, DurableRange { start, end: actual })?;
            Ok(())
        })
        .await?;
        Ok(actual)
    }
}

pub(super) fn voter_dir(root: &Path, topic: &str, shard: ShardId, node_id: NodeId) -> PathBuf {
    shard_dir(root, topic, Some(shard.topic_id), shard.partition)
        .join(format!("voter-{}", node_id.0))
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, crate::BrokerError> + Send + 'static,
) -> Result<T, crate::BrokerError> {
    crate::blocking::run_blocking(operation)
        .await
        .map_err(|error| {
            crate::partition_writer::storage_failure_error(
                "WAL follower storage task panicked",
                error,
            )
        })?
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;

    fn fresh_follower() -> (tempfile::TempDir, FollowerLog) {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        (dir, FollowerLog::for_log(log))
    }

    #[cfg(unix)]
    #[test]
    fn interrupted_trim_keeps_a_recoverable_durable_range() {
        use std::sync::atomic::{AtomicU8, Ordering};

        use krabka_log::IoTarget;

        #[derive(Debug)]
        struct InterruptedTrim(Arc<AtomicU8>);
        impl InterruptedTrim {
            fn fails(&self, phase: u8) -> bool {
                self.0
                    .compare_exchange(phase, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            }
        }
        impl krabka_log::LogIo for InterruptedTrim {
            fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
                if self.fails(1) {
                    return Err(std::io::Error::other("interrupted before byte sync"));
                }
                file.sync_data()
            }
            fn rename(&self, target: IoTarget, from: &Path, to: &Path) -> std::io::Result<()> {
                if target == IoTarget::LogStartOffsetCheckpoint && self.fails(2) {
                    return Err(std::io::Error::other(
                        "interrupted before floor publication",
                    ));
                }
                std::fs::rename(from, to)
            }
            fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
                std::fs::File::open(dir)?.sync_all()?;
                if self.fails(3) {
                    return Err(std::io::Error::other("interrupted after floor publication"));
                }
                Ok(())
            }
        }

        for prior_end in [1, 3] {
            for requested in [1, 3, 4] {
                for failure in 1..=4 {
                    let dir = tempfile::tempdir().unwrap();
                    let fault = Arc::new(AtomicU8::new(0));
                    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
                    log.append(&mut RecordBatch {
                        records: vec![Record::default()],
                        ..RecordBatch::default()
                    })
                    .unwrap();
                    let mut tail = RecordBatch {
                        last_offset_delta: 1,
                        records: (0..2)
                            .map(|offset_delta| Record {
                                offset_delta,
                                ..Record::default()
                            })
                            .collect(),
                        ..RecordBatch::default()
                    };
                    if prior_end == 3 {
                        log.append(&mut tail).unwrap();
                    }
                    log.sync().unwrap();
                    let follower = FollowerLog::for_log(log);
                    if prior_end == 1 {
                        follower.log.lock().append(&mut tail).unwrap();
                    }
                    follower
                        .log
                        .lock()
                        .test_set_io(Arc::new(InterruptedTrim(fault.clone())));
                    fault.store(failure, Ordering::SeqCst);
                    let temporary = follower
                        .durable_offset_path
                        .with_extension("checkpoint.tmp");
                    if failure == 4 {
                        std::fs::create_dir(&temporary).unwrap();
                    }
                    assert!(follower.trim_to_blocking(Offset(requested)).is_err());
                    let published = matches!(failure, 2 | 3);
                    let floor = if published { requested.min(3) } else { 0 };
                    let end = if published { 3 } else { prior_end };
                    assert!(
                        std::fs::read_to_string(&follower.durable_offset_path).unwrap()
                            == format!("0\n{floor} {end}\n")
                    );
                    if failure == 4 {
                        std::fs::remove_dir(temporary).unwrap();
                    }
                    drop(follower);

                    let reopened =
                        FollowerLog::open_at(dir.path().to_path_buf(), &LogConfig::default())
                            .unwrap();
                    assert!(reopened.start_offset() == Offset(floor));
                    assert!(reopened.end_offset() == Offset(end));
                    reopened.trim_to_blocking(Offset(requested)).unwrap();
                    drop(reopened);
                    let again =
                        FollowerLog::open_at(dir.path().to_path_buf(), &LogConfig::default())
                            .unwrap();
                    assert!(again.start_offset() == Offset(requested.min(end)));
                    assert!(again.end_offset() == Offset(end));
                }
            }
        }
    }

    #[tokio::test]
    async fn follower_appends_and_syncs_a_contiguous_fetch() {
        let (dir, follower) = fresh_follower();
        let batch = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(1), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap();

        assert2::assert!((end) == (Offset(1)));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!((reopened.log_end_offset()) == (Offset(1)));
    }

    #[tokio::test]
    async fn follower_rejects_a_gap_before_writing() {
        let (_dir, follower) = fresh_follower();
        let batch = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let error = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not contiguous"));
        assert2::assert!((follower.end_offset()) == (Offset(0)));
    }

    #[tokio::test]
    async fn follower_accepts_a_partial_fetch_and_rejects_a_leader_overrun() {
        let (_dir, follower) = fresh_follower();
        let first = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![first])))
            .await
            .unwrap();

        assert!(end == Offset(1));
        let beyond_leader = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        let error = follower
            .append(
                Offset(1),
                Offset(1),
                Some(RecordsPayload::V2(vec![beyond_leader])),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("beyond leader LEO"));
        assert!(follower.end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn follower_reset_persists_the_leader_log_start() {
        let (dir, follower) = fresh_follower();

        follower.reset_to(Offset(7)).await.unwrap();

        assert2::assert!((follower.end_offset()) == (Offset(7)));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!((reopened.log_start_offset()) == (Offset(7)));
        assert2::assert!((reopened.log_end_offset()) == (Offset(7)));
    }

    #[tokio::test]
    async fn follower_trim_persists_the_leader_log_start() {
        let (dir, follower) = fresh_follower();
        let batches = (0..2)
            .map(|base_offset| RecordBatch {
                base_offset,
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .collect();
        follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(batches)))
            .await
            .unwrap();

        follower.trim_to(Offset(1)).await.unwrap();

        assert2::assert!((follower.start_offset()) == (Offset(1)));
        assert2::assert!((follower.end_offset()) == (Offset(2)));
        drop(follower);
        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &dir.path().join(DURABLE_OFFSET_FILE)).unwrap();
        assert2::assert!((reopened.log_start_offset()) == (Offset(1)));
        assert2::assert!((reopened.log_end_offset()) == (Offset(2)));
    }

    /// The leader could not place epoch 7, newer than its whole log, and
    /// answered its log end with its latest epoch, 5. This log never held
    /// epoch 5, so its copy of that epoch ends where epoch 7 starts.
    #[tokio::test]
    async fn unplaceable_epoch_divergence_truncates_to_the_local_end_of_the_leader_epoch() {
        let (_dir, follower) = fresh_follower();
        follower
            .append(
                Offset(0),
                Offset(1),
                Some(RecordsPayload::V2(vec![RecordBatch {
                    base_offset: 0,
                    partition_leader_epoch: 7,
                    records: vec![Record::default()],
                    ..RecordBatch::default()
                }])),
            )
            .await
            .unwrap();

        follower
            .resolve_divergence(
                LogOffsetMetadata {
                    offset: 1,
                    epoch: 5,
                },
                Offset(0),
                Offset(1),
            )
            .await
            .unwrap();

        assert2::assert!((follower.end_offset()) == (Offset(0)));
        assert2::assert!((follower.last_epoch()) == (-1));
    }

    #[tokio::test]
    async fn divergence_before_the_retained_range_resets_to_the_leader_start() {
        let (_dir, follower) = fresh_follower();
        follower
            .append(
                Offset(0),
                Offset(2),
                Some(RecordsPayload::V2(
                    (0..2)
                        .map(|base_offset| RecordBatch {
                            base_offset,
                            records: vec![Record::default()],
                            ..RecordBatch::default()
                        })
                        .collect(),
                )),
            )
            .await
            .unwrap();
        follower.trim_to(Offset(1)).await.unwrap();

        follower
            .resolve_divergence(
                LogOffsetMetadata {
                    offset: 0,
                    epoch: 0,
                },
                Offset(0),
                Offset(2),
            )
            .await
            .unwrap();

        assert2::assert!((follower.start_offset()) == (Offset(0)));
        assert2::assert!((follower.end_offset()) == (Offset(0)));
        assert2::assert!((follower.last_epoch()) == (-1));
    }
}
