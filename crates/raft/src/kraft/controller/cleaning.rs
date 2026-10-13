//! Metadata log cleaning by retention: Kafka's `KafkaRaftLog.maybeClean`.
//!
//! A snapshot does not move the log start by itself. The engine keeps every
//! snapshot, and the log prefix they cover, until `metadata.max.retention.bytes`
//! or `metadata.max.retention.ms` lets the oldest snapshot go. Then the oldest
//! snapshot is deleted, the log start moves up to the next snapshot, and every
//! segment wholly below the new log start is deleted. The newest snapshot
//! always stays, so a node with fewer than two snapshots cleans nothing.
//!
//! The size rule runs first and the age rule second, as in Kafka. Each walks
//! the snapshots from the oldest and stops at the first one it keeps.

use {
    super::{Engine, KraftController, checkpoint::checkpoint_ids},
    crate::{error::RaftError, kraft::snapshot_fetch::SnapshotId, snapshot::SnapshotReader},
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, Time, TimeExt as _},
};

/// One snapshot the cleaning weighs: its `(end_offset, epoch)` id and the
/// size of its `.checkpoint` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedSnapshot {
    pub id: SnapshotId,
    pub size: u64,
}

/// Kafka's `cleanSnapshotsRetentionSize` rule: a snapshot goes while the log
/// and the snapshots still kept are together larger than `max`.
#[must_use]
pub fn retention_size_breached(log_size: u64, snapshots_size: u64, max: ByteSize) -> bool {
    log_size.saturating_add(snapshots_size) > max.bytes_u64()
}

/// Kafka's `cleanSnapshotsRetentionMs` rule: a snapshot goes once its last
/// record is more than `max` older than `now_ms`.
#[must_use]
pub fn retention_age_breached(now_ms: i64, last_contained_log_timestamp: i64, max: Time) -> bool {
    now_ms.saturating_sub(last_contained_log_timestamp) > max.millis_i64()
}

/// The snapshots at or above `log_start`, oldest first: the ones Kafka's
/// `KafkaRaftLog.snapshots` holds. A snapshot below the log start no longer
/// starts a log a reader could continue from.
fn retained_snapshots(dir: &std::path::Path, log_start: Offset) -> Vec<RetainedSnapshot> {
    let mut ids: Vec<SnapshotId> = checkpoint_ids(dir)
        .into_iter()
        .filter(|id| id.0 >= log_start.0)
        .collect();
    ids.sort_unstable();
    ids.into_iter()
        .filter_map(|id| {
            let size = std::fs::metadata(dir.join(super::checkpoint::checkpoint_name(id.0, id.1)))
                .ok()?
                .len();
            Some(RetainedSnapshot { id, size })
        })
        .collect()
}

impl Engine {
    /// Kafka's `KafkaRaftLog.maybeClean`: apply the size rule, then the age
    /// rule. Returns `true` when it deleted a snapshot or a segment.
    ///
    /// # Errors
    /// Returns the [`RaftError`] of a log trim that failed. The snapshots that
    /// the failed step was to delete stay on disk.
    pub fn maybe_clean(&mut self) -> Result<bool, RaftError> {
        if self.downgrade_snapshot_pending.is_some() {
            return Ok(false);
        }
        let by_size = match self.metadata_log.max_retention_size {
            Some(max) => self.clean_by_size(max)?,
            None => false,
        };
        let by_age = match self.metadata_log.max_retention {
            Some(max) => self.clean_by_age(max)?,
            None => false,
        };
        Ok(by_size || by_age)
    }

    fn clean_by_size(&mut self, max: ByteSize) -> Result<bool, RaftError> {
        let snapshots = retained_snapshots(&self.data_dir, self.log.log_start_offset());
        let mut kept: u64 = snapshots.iter().map(|snapshot| snapshot.size).sum();
        let mut cleaned = false;
        for pair in snapshots.windows(2) {
            if !retention_size_breached(self.log.size().bytes_u64(), kept, max) {
                break;
            }
            kept = kept.saturating_sub(pair[0].size);
            if !self.delete_before_snapshot(pair[1].id, "size")? {
                break;
            }
            cleaned = true;
        }
        Ok(cleaned)
    }

    fn clean_by_age(&mut self, max: Time) -> Result<bool, RaftError> {
        let snapshots = retained_snapshots(&self.data_dir, self.log.log_start_offset());
        let now_ms = KraftController::wall_clock_ms();
        let mut cleaned = false;
        for pair in snapshots.windows(2) {
            let Some(timestamp) = self.snapshot_timestamp(pair[0].id) else {
                break;
            };
            if !retention_age_breached(now_ms, timestamp, max) {
                break;
            }
            if !self.delete_before_snapshot(pair[1].id, "age")? {
                break;
            }
            cleaned = true;
        }
        Ok(cleaned)
    }

    /// The `last_contained_log_timestamp` in the header of snapshot `id`, or
    /// `None` when the file does not read.
    fn snapshot_timestamp(&self, id: SnapshotId) -> Option<i64> {
        let bytes = super::checkpoint::load_checkpoint_by_id(&self.data_dir, id.0, id.1)?;
        SnapshotReader::last_contained_log_timestamp(&bytes).ok()
    }

    /// Kafka's `KafkaRaftLog.deleteBeforeSnapshot`: move the log start up to
    /// snapshot `id`, delete the segments wholly below it, and delete the
    /// snapshots below it. It does nothing, and returns `false`, unless `id`
    /// is above the log start, at or below the newest snapshot, and
    /// committed.
    fn delete_before_snapshot(&mut self, id: SnapshotId, rule: &str) -> Result<bool, RaftError> {
        let boundary = Offset(id.0);
        let latest = super::checkpoint::latest_checkpoint_id(&self.data_dir);
        let admitted = self.log.log_start_offset() < boundary
            && latest.is_some_and(|latest| id.0 <= latest.0)
            && boundary <= self.log.hwm();
        if !admitted {
            return Ok(false);
        }
        self.log.prune_to(boundary)?;
        let deleted = delete_checkpoints_below(&self.data_dir, id);
        tracing::info!(
            rule,
            log_start = boundary.0,
            deleted_snapshots = ?deleted,
            "kraft: cleaned the metadata log by its retention limit"
        );
        Ok(true)
    }
}

/// Delete the `.checkpoint` files in `dir` whose ids order below `id`, except
/// the newest of them, and return the ids it deleted, oldest first.
///
/// The newest snapshot below the new log start stays for one more cleaning,
/// as [`retain_recent_checkpoints`](super::checkpoint::retain_recent_checkpoints)
/// keeps it: a follower that is part way through a `FetchSnapshot` transfer of
/// it would otherwise get `SNAPSHOT_NOT_FOUND` on its next chunk and start
/// again from position 0 against the newer one. Kafka keeps the file of a
/// forgotten snapshot for `file.delete.delay.ms` for its open readers. The
/// delete is best effort: a file that does not delete stays, and the next
/// cleaning tries it again.
fn delete_checkpoints_below(dir: &std::path::Path, id: SnapshotId) -> Vec<SnapshotId> {
    let mut below: Vec<SnapshotId> = checkpoint_ids(dir)
        .into_iter()
        .filter(|found| *found < id)
        .collect();
    below.sort_unstable();
    below.pop();
    below.retain(|found| {
        std::fs::remove_file(dir.join(super::checkpoint::checkpoint_name(found.0, found.1))).is_ok()
    });
    below
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{bytes, millis},
    };

    /// The size rule counts the log and the snapshots still kept, and breaks
    /// only strictly above the limit, as Kafka's `logSize + snapshotsSize >
    /// retentionMaxBytes` does.
    #[test]
    fn the_size_rule_breaks_strictly_above_the_limit() {
        for (log_size, snapshots_size, max, want) in [
            (1_000, 1_048, 2_048, false),
            (1_000, 1_049, 2_048, true),
            (0, 0, 0, false),
            (1, 0, 0, true),
            (u64::MAX, 1, 2_048, true),
        ] {
            check!(
                retention_size_breached(log_size, snapshots_size, bytes(max)) == want,
                "log {log_size} + snapshots {snapshots_size} against {max}"
            );
        }
    }

    /// The age rule breaks once the snapshot's last record is strictly older
    /// than the limit, as Kafka's `now - timestamp > retentionMillis` does.
    #[test]
    fn the_age_rule_breaks_strictly_past_the_limit() {
        for (now, timestamp, max, want) in [
            (10_000, 0, 10_000, false),
            (10_001, 0, 10_000, true),
            (5, 10, 0, false),
            (i64::MAX, i64::MIN, 1, true),
        ] {
            check!(
                retention_age_breached(now, timestamp, millis(max)) == want,
                "now {now}, last record {timestamp}, limit {max}"
            );
        }
    }

    /// Only the snapshots at or above the log start are weighed, oldest
    /// first, each with its file size; a file that is not a canonical
    /// checkpoint is not one.
    #[test]
    fn the_retained_snapshots_are_those_at_or_above_the_log_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        for (name, size) in [
            ("00000000000000000004-0000000001.checkpoint", 4),
            ("00000000000000000010-0000000001.checkpoint", 10),
            ("00000000000000000007-0000000001.checkpoint", 7),
            ("00000000000000000002-0000000001.checkpoint", 2),
            ("high-watermark", 1),
            ("00000000000000000004-0000000001.checkpoint.tmp", 1),
        ] {
            std::fs::write(dir.path().join(name), vec![0u8; size]).expect("write");
        }

        check!(
            retained_snapshots(dir.path(), Offset(4))
                == vec![
                    RetainedSnapshot {
                        id: (4, 1),
                        size: 4
                    },
                    RetainedSnapshot {
                        id: (7, 1),
                        size: 7
                    },
                    RetainedSnapshot {
                        id: (10, 1),
                        size: 10
                    },
                ]
        );
        // The newest snapshot below the boundary stays for a reader that is
        // part way through it, and an older one goes.
        check!(delete_checkpoints_below(dir.path(), (7, 1)) == vec![(2, 1)]);
        check!(delete_checkpoints_below(dir.path(), (10, 1)) == vec![(4, 1)]);
        check!(
            retained_snapshots(dir.path(), Offset(0))
                .iter()
                .map(|snapshot| snapshot.id)
                .collect::<Vec<_>>()
                == vec![(7, 1), (10, 1)]
        );
    }
}
