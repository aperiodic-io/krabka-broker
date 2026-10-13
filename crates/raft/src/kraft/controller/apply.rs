//! The high-watermark advance and the apply of the records it newly commits
//! into the published [`MetadataImage`], including the capture of a
//! `metadata.version` downgrade boundary that KIP-1155 must checkpoint at.

use {
    super::{
        Engine, PendingDowngradeSnapshot,
        offsets::{
            batch_base_in_apply_window, expected_hwm_after_advance, hwm_advanced_as_expected,
        },
        records::{decode_committed_value, is_kip835_noop, next_batch_offset},
    },
    krabka_ids::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, VotersRecord},
    std::sync::Arc,
};

impl Engine {
    /// Advance the HWM and apply the records newly committed by it to the
    /// [`MetadataImage`], then publish and resolve any satisfied waiters.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, new_hwm = new_hwm.0, prev_hwm = tracing::field::Empty)
    )]
    pub fn advance_and_apply(&mut self, new_hwm: Offset) {
        // A controller that met a record it cannot replay takes no more part.
        if self.replay_fault.is_some() {
            return;
        }
        let prev_hwm = self.log.hwm();
        tracing::Span::current().record("prev_hwm", prev_hwm.0);
        let expected_hwm = expected_hwm_after_advance(prev_hwm, new_hwm, self.log.log_end_offset());
        self.log.advance_hwm(new_hwm);
        let applied_hwm = self.log.hwm();
        self.commit_control_state(applied_hwm);
        self.try_resolve_reconfiguration();
        if !hwm_advanced_as_expected(applied_hwm, expected_hwm) {
            tracing::error!(
                prev_hwm = prev_hwm.0,
                new_hwm = new_hwm.0,
                expected_hwm = expected_hwm.0,
                applied_hwm = applied_hwm.0,
                "kraft: high watermark failed to advance"
            );
            self.fail_waiters_reached_by(
                expected_hwm,
                "high watermark failed to advance to committed offset",
            );
            self.maybe_snapshot();
            return;
        }
        if applied_hwm <= prev_hwm {
            self.try_resolve_waiters();
            self.maybe_snapshot();
            return;
        }
        let processing_started = std::time::Instant::now();
        let mut cursor = prev_hwm;
        let mut changed = false;
        let mut metadata_version_downgraded = false;
        while cursor < applied_hwm {
            match self
                .log
                .read_decoded(cursor, self.metadata_raft_fetch_max.size())
            {
                Ok(batches) => {
                    let next = next_batch_offset(&batches);
                    if batches.is_empty() {
                        break;
                    }
                    for batch in &batches {
                        if !batch_base_in_apply_window(batch.base_offset, prev_hwm, applied_hwm) {
                            continue;
                        }
                        // KIP-630's byte-size snapshot cap counts the whole
                        // log, so this runs before the control-batch skip
                        // below (a LeaderChange batch still occupies log
                        // bytes even though it carries no metadata records).
                        self.bytes_since_snapshot = self
                            .bytes_since_snapshot
                            .saturating_add(u64::try_from(batch.encoded_len()).unwrap_or(u64::MAX));
                        // The LeaderChange control batch carries no metadata records;
                        // never feed it to the metadata decoder.
                        if batch.attributes.is_control_batch() {
                            self.last_applied_record_timestamp_ms = batch.max_timestamp;
                            continue;
                        }
                        for rec in &batch.records {
                            let Some(value) = rec.value.as_ref() else {
                                continue;
                            };
                            // A KIP-835 no-op changes nothing.
                            if is_kip835_noop(value) {
                                continue;
                            }
                            let offset = batch
                                .base_offset
                                .saturating_add(i64::from(rec.offset_delta));
                            let meta = match decode_committed_value(value, &self.image, offset) {
                                Ok(Some(meta)) => meta,
                                Ok(None) => continue,
                                Err(fault) => {
                                    // The controller stops: nothing after this
                                    // record applies, and nothing publishes.
                                    tracing::error!(%fault, "kraft: stopping the controller");
                                    self.replay_fault = Some(fault);
                                    return;
                                }
                            };
                            match self.image.validate(&meta) {
                                Ok(()) => {
                                    let is_metadata_version_downgrade = matches!(
                                        &meta,
                                        MetadataRecord::V1FeatureLevel(feature)
                                            if feature.name
                                                == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                                                && self
                                                    .image
                                                    .finalized_metadata_version()
                                                    .is_some_and(|current| feature.level < current)
                                    );
                                    self.image.apply(&meta);
                                    if is_metadata_version_downgrade
                                        && self.downgrade_snapshot_pending.is_none()
                                    {
                                        let end_offset = Offset(
                                            batch
                                                .base_offset
                                                .saturating_add(i64::from(rec.offset_delta))
                                                .saturating_add(1),
                                        );
                                        let mut image = self.image.clone();
                                        image.apply(&MetadataRecord::V1KRaftVersion(
                                            krabka_metadata::KRaftVersionRecord {
                                                kraft_version: self.controls.version_at(end_offset),
                                            },
                                        ));
                                        image.apply(&MetadataRecord::V1Voters(VotersRecord {
                                            voters: self.controls.voters_at(end_offset),
                                        }));
                                        self.downgrade_snapshot_pending =
                                            Some(PendingDowngradeSnapshot {
                                                image,
                                                end_offset,
                                                epoch: batch.partition_leader_epoch,
                                            });
                                    }
                                    metadata_version_downgraded |= is_metadata_version_downgrade;
                                    changed = true;
                                }
                                Err(e) => {
                                    // Record the first rejection against any
                                    // waiter that covers this offset so the
                                    // submitter learns the canonical error.
                                    self.note_rejection(Offset(batch.base_offset), &e);
                                    tracing::debug!(
                                        ?e,
                                        "kraft: rejected committed record on apply"
                                    );
                                }
                            }
                        }
                        // The batch has been consumed completely by this
                        // apply pass. Track its timestamp separately from the
                        // quorum high watermark, which may already be ahead.
                        self.last_applied_record_timestamp_ms = batch.max_timestamp;
                    }
                    let Some(next) = next.filter(|next| *next > cursor) else {
                        break;
                    };
                    cursor = next;
                }
                Err(e) => {
                    tracing::error!(?e, "kraft: read for apply failed");
                    break;
                }
            }
        }
        if changed && !metadata_version_downgraded && self.downgrade_snapshot_pending.is_none() {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
        // KIP-835 measures the time to consume a set of pending committed
        // records, not the age of the records (reported separately by the
        // applied-record lag metric). Only count a pass that reached the
        // committed boundary; a read failure leaves pending work behind.
        if cursor >= applied_hwm {
            let elapsed_us =
                u64::try_from(processing_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            self.pending_record_processing_time_us_total = self
                .pending_record_processing_time_us_total
                .saturating_add(u128::from(elapsed_us));
            self.pending_record_processing_time_samples = self
                .pending_record_processing_time_samples
                .saturating_add(1);
            self.pending_record_processing_time_us_max =
                self.pending_record_processing_time_us_max.max(elapsed_us);
        }
        // KIP-1155: every quorum member that replays a metadata.version
        // downgrade immediately checkpoints the lower-version image and
        // discards the incompatible log prefix. This is local work, so
        // followers do it too rather than waiting to become leader.
        if metadata_version_downgraded {
            self.retry_pending_downgrade_snapshot();
        }
        self.publish_leader();
        self.try_resolve_waiters();
        self.maybe_snapshot();
    }
}
