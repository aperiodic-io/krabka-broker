//! Snapshot writing on this node: the KIP-630 interval-driven checkpoint, the
//! explicit trigger, and the mandatory KIP-1155 downgrade checkpoint that is
//! retried until it succeeds and prunes the log below it.
//!
//! An interval-driven checkpoint does not prune by itself. The metadata log
//! keeps the prefix a snapshot covers until its retention limits let the
//! oldest snapshot go, which the `cleaning` module decides.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord};
use krabka_units::prelude::{ByteSizeExt as _, TimeExt as _};

use super::{
    Engine,
    checkpoint::{latest_checkpoint_id, retain_recent_checkpoints, write_checkpoint},
    offsets::{
        committed_records_since_snapshot, snapshot_bytes_reached, snapshot_interval_reached,
        snapshot_time_reached,
    },
    recovery::replay_committed,
};
use crate::{
    error::RaftError,
    snapshot::{SnapshotReader, SnapshotWriter},
};

impl Engine {
    /// (Every voter, KIP-630) once the committed offset has advanced past the
    /// last snapshot by `snapshot_interval_records` records, by
    /// `max_bytes_between_snapshots` bytes, or `max_snapshot_interval` has
    /// elapsed since the last checkpoint, serialize the current image to a
    /// checkpoint, then clean the log by its retention limits.
    ///
    /// This runs on every voter, not only the leader: a follower's HWM
    /// advances on every applied Fetch response just like a leader's, and
    /// Kafka's `SnapshotGenerator` is installed on every `KRaft` node,
    /// controller or broker, independently of role.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, hwm = tracing::field::Empty)
    )]
    pub fn maybe_snapshot(&mut self) {
        if self.downgrade_snapshot_pending.is_some() {
            return;
        }
        let hwm = self.log.hwm();
        let advanced = committed_records_since_snapshot(hwm, self.last_snapshot_end_offset);
        let records_due = snapshot_interval_reached(advanced, self.snapshot_interval_records);
        let bytes_due = snapshot_bytes_reached(
            self.bytes_since_snapshot,
            self.max_bytes_between_snapshots.bytes_u64(),
        );
        let elapsed_ms = self.now().0.saturating_sub(self.last_snapshot_at_ms);
        let max_snapshot_interval_ms =
            u64::try_from(self.max_snapshot_interval.millis_i64()).unwrap_or(0);
        let time_due = snapshot_time_reached(elapsed_ms, max_snapshot_interval_ms);
        if !(records_due || bytes_due || time_due) {
            return;
        }
        tracing::Span::current().record("hwm", hwm.0);
        if let Err(error) = self.write_snapshot_and_clean() {
            tracing::error!(?error, "kraft: snapshot/clean failed");
        }
    }

    /// Write a checkpoint of the committed image at the high watermark, then
    /// clean the log by its retention limits, as Kafka's snapshot generator
    /// and log cleaner together do.
    ///
    /// # Errors
    /// Returns the [`RaftError`] of the checkpoint write or of the cleaning.
    pub fn write_snapshot_and_clean(&mut self) -> Result<(), RaftError> {
        let end_offset = self.write_current_snapshot()?;
        self.last_snapshot_end_offset = end_offset;
        self.last_snapshot_at_ms = self.now().0;
        self.bytes_since_snapshot = 0;
        self.maybe_clean()?;
        Ok(())
    }

    /// (KIP-630) `SnapshotHeaderRecord.last_contained_log_timestamp` for a
    /// snapshot covering `[0, end_offset)`: the create-time of the batch
    /// holding the last record it contains, which is what Kafka's
    /// `MetadataLoader` passes to `KafkaRaftClient.createSnapshot` as the
    /// append time of the last batch folded in.
    ///
    /// A boundary at the log start has no readable record below it, because a
    /// previous checkpoint already pruned the prefix away or installed over
    /// it. The last record such a snapshot contains is the very one the
    /// previous checkpoint named, so the stamp carries forward rather than
    /// reverting a live create-time to the epoch. It stays `0` only on a node
    /// that has never checkpointed, where nothing has ever named one.
    fn last_contained_ts(&self, end_offset: Offset) -> i64 {
        self.log
            .timestamp_below(end_offset)
            .unwrap_or(self.last_snapshot_timestamp_ms)
    }

    /// Write the committed image and remember the timestamp it names before
    /// either retention cleaning or pruning changes the readable log prefix.
    fn write_current_snapshot(&mut self) -> Result<Offset, RaftError> {
        let last_contained_ts = self.last_contained_ts(self.log.hwm());
        let bytes = SnapshotWriter::serialize(&self.image, last_contained_ts)?;
        let end_offset = self.write_snapshot_checkpoint(&bytes)?;
        self.last_snapshot_timestamp_ms = last_contained_ts;
        Ok(end_offset)
    }

    pub fn write_snapshot_and_prune(&mut self) -> Result<(), RaftError> {
        let end_offset = self.write_current_snapshot()?;
        self.prune_to_snapshot(end_offset)?;
        Ok(())
    }

    pub fn write_downgrade_snapshot_and_prune(&mut self) -> Result<(), RaftError> {
        let Some(pending) = self.downgrade_snapshot_pending.clone() else {
            return Ok(());
        };
        #[cfg(test)]
        if self.downgrade_snapshot_failures_remaining > 0 {
            self.downgrade_snapshot_failures_remaining -= 1;
            return Err(RaftError::Startup(
                "injected metadata downgrade snapshot failure".into(),
            ));
        }
        let last_contained_ts = self.last_contained_ts(pending.end_offset);
        let bytes = SnapshotWriter::serialize(&pending.image, last_contained_ts)?;
        // KIP-1155 is a snapshot reload, not only a checkpoint write. Decode
        // the exact bytes first and rebuild every metadata index from their
        // lower-version record representation before discarding the log
        // prefix. This also proves locally that the checkpoint is readable
        // before any durable state is pruned.
        let contents = SnapshotReader::read(&bytes)?;
        let mut reloaded =
            MetadataImage::from_records(pending.image.cluster_id(), &contents.metadata_records);
        if let Some(control) = &contents.control_state {
            reloaded.apply(&MetadataRecord::V1KRaftVersion(
                krabka_metadata::KRaftVersionRecord {
                    kraft_version: control.kraft_version,
                },
            ));
            reloaded.apply(&MetadataRecord::V1Voters(VotersRecord {
                voters: control.voters.clone(),
            }));
        }
        write_checkpoint(&self.data_dir, pending.end_offset.0, pending.epoch, &bytes)?;
        self.last_snapshot_timestamp_ms = last_contained_ts;

        // Rebuild the committed suffix before pruning. This preserves records
        // committed after the downgrade failure while keeping the checkpoint
        // itself pinned to the exact lower-version image and boundary.
        let next_pending = replay_committed(
            &self.log,
            &mut reloaded,
            pending.end_offset,
            self.metadata_raft_fetch_max,
        )?;
        reloaded.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: self.controls.committed_version,
            },
        ));
        reloaded.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: self.controls.committed_voters.clone(),
        }));

        self.prune_to_snapshot(pending.end_offset)?;
        self.image = reloaded;
        self.downgrade_snapshot_pending = next_pending;
        if self.downgrade_snapshot_pending.is_none() {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
        Ok(())
    }

    pub fn retry_pending_downgrade_snapshot(&mut self) {
        while self.downgrade_snapshot_pending.is_some() {
            if let Err(error) = self.write_downgrade_snapshot_and_prune() {
                // A committed record that does not replay stops the
                // controller, wherever the replay meets it.
                if let RaftError::MetadataReplay(fault) = error {
                    tracing::error!(%fault, "kraft: stopping the controller");
                    self.replay_fault = Some(fault);
                    break;
                }
                tracing::error!(
                    ?error,
                    "mandatory metadata downgrade snapshot remains pending"
                );
                break;
            }
        }
    }

    pub fn write_snapshot_checkpoint(&self, bytes: &[u8]) -> Result<Offset, RaftError> {
        let end_offset = self.log.hwm();
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        write_checkpoint(&self.data_dir, end_offset.0, epoch, bytes)?;
        Ok(end_offset)
    }

    pub fn prune_to_snapshot(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        if !krabka_verified::snapshot_prune_admission(end_offset.0, self.log.hwm().0) {
            return Err(RaftError::ChangeRejected(
                "snapshot prune boundary is negative or beyond the committed frontier".into(),
            ));
        }
        self.log.prune_to(end_offset)?;
        self.last_snapshot_end_offset = end_offset;
        self.last_snapshot_at_ms = self.now().0;
        self.bytes_since_snapshot = 0;
        retain_recent_checkpoints(&self.data_dir);
        Ok(())
    }

    /// The latest local snapshot id `(end_offset, epoch)`, if any (leader's
    /// `FetchSnapshot` hint).
    pub fn latest_snapshot_id(&self) -> Option<(i64, i32)> {
        latest_checkpoint_id(&self.data_dir)
    }

    /// Serialize the current image into a KIP-630 checkpoint under the data dir.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, end_offset = self.log.hwm().0),
        err
    )]
    pub fn do_trigger_snapshot(&mut self) -> Result<(), RaftError> {
        if self.downgrade_snapshot_pending.is_some() {
            return Err(RaftError::ChangeRejected(
                "mandatory metadata downgrade snapshot is pending".into(),
            ));
        }
        let end_offset = self.log.hwm();
        let last_contained_ts = self.last_contained_ts(end_offset);
        let bytes = SnapshotWriter::serialize(&self.image, last_contained_ts)?;
        let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
        // Checkpoint filenames encode the raw offset (on-disk boundary).
        write_checkpoint(&self.data_dir, end_offset.0, epoch, &bytes)?;
        self.last_snapshot_timestamp_ms = last_contained_ts;
        Ok(())
    }
}
