//! Restart recovery over the durable log: replay of the committed metadata
//! prefix into a [`MetadataImage`], replay of the KIP-853 control records into
//! the seed [`QuorumState`], and the control state as of a chosen boundary.

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::records::metadata::control::ControlRecord;
use krabka_verified::recovery::{
    ReplayCursorDecision, ReplayRecordDecision, replay_cursor_decision, replay_record_decision,
    should_capture_first_downgrade,
};

use super::{
    PendingDowngradeSnapshot,
    control_state::voter_set_from_wire,
    records::{decode_committed_value, decode_control_record, next_batch_offset},
};
use crate::{
    config::MetadataRaftFetchMax,
    error::{MetadataReplayError, RaftError},
    kraft::{log::KraftLog, types::QuorumState},
};

/// Replay committed log batches starting at `from` into `image` (idempotent:
/// records that fail `validate` are skipped). Used by restart recovery.
///
/// # Errors
/// [`RaftError::MetadataReplay`] for a committed record that does not decode,
/// which stops the controller as live apply does, and the error of a log read.
pub fn replay_committed(
    log: &KraftLog,
    image: &mut MetadataImage,
    from: Offset,
    max: MetadataRaftFetchMax,
) -> Result<Option<PendingDowngradeSnapshot>, RaftError> {
    replay_committed_with_timestamp(log, image, from, max).map(|(pending, _)| pending)
}

/// Replay committed log batches and return the timestamp of the last batch
/// covered by the replay, matching the controller's live apply timestamp.
pub fn replay_committed_with_timestamp(
    log: &KraftLog,
    image: &mut MetadataImage,
    from: Offset,
    max: MetadataRaftFetchMax,
) -> Result<(Option<PendingDowngradeSnapshot>, Option<i64>), RaftError> {
    let mut cursor = from;
    let target = log.hwm();
    let mut pending = None;
    let mut last_applied_timestamp_ms = None;
    while cursor < target {
        let batches = log.read_decoded(cursor, max.size())?;
        let next = next_batch_offset(&batches);
        if batches.is_empty() {
            break;
        }
        for batch in &batches {
            if batch.base_offset >= from.0 && batch.base_offset < target.0 {
                last_applied_timestamp_ms = Some(batch.max_timestamp);
            }
            for rec in &batch.records {
                let ReplayRecordDecision::Apply(record_offset) = replay_record_decision(
                    batch.base_offset,
                    rec.offset_delta,
                    from.0,
                    target.0,
                    batch.attributes.is_control_batch(),
                    false,
                ) else {
                    continue;
                };
                let record_offset = Offset(record_offset);
                let Some(value) = rec.value.as_ref() else {
                    continue;
                };
                // A record that does not decode stops recovery; one that
                // decodes and does not validate is skipped, as live apply
                // skips it (see `decode_committed_value`).
                if let Some(meta) = decode_committed_value(value, image, record_offset.0)?
                    && image.validate(&meta).is_ok()
                {
                    let is_metadata_version_downgrade = matches!(
                        &meta,
                        MetadataRecord::V1FeatureLevel(feature)
                            if feature.name
                                == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                                && image
                                    .finalized_metadata_version()
                                    .is_some_and(|current| feature.level < current)
                    );
                    image.apply(&meta);
                    if should_capture_first_downgrade(
                        pending.is_some(),
                        is_metadata_version_downgrade,
                    ) {
                        pending = Some(PendingDowngradeSnapshot {
                            image: image.clone(),
                            end_offset: record_offset + 1,
                            epoch: batch.partition_leader_epoch,
                        });
                    }
                }
            }
        }
        match replay_cursor_decision(cursor.0, next.map(|offset| offset.0)) {
            ReplayCursorDecision::Advance(next) => cursor = Offset(next),
            ReplayCursorDecision::Stop => break,
        }
    }
    Ok((pending, last_applied_timestamp_ms))
}

/// Replays the KIP-853 control records of the committed log into `state`.
///
/// # Errors
/// [`MetadataReplayError::InvalidControlRecord`] for a control record that does
/// not decode, a negative `kraft.version`, or a voter set that does not
/// convert. Kafka's `KRaftControlRecordStateMachine` throws while it reads
/// each (`RecordsIterator`, `VoterSet.fromVotersRecord`), and
/// `KafkaRaftClientDriver` hands the throw to the fatal
/// `SharedServer.raftManagerFaultHandler`, which halts the process. Also the
/// error of a log read.
pub fn replay_control_records(
    log: &KraftLog,
    state: &mut QuorumState,
    max: MetadataRaftFetchMax,
) -> Result<(), RaftError> {
    replay_control_records_until(log, state, log.hwm(), max)
}

fn replay_control_records_until(
    log: &KraftLog,
    state: &mut QuorumState,
    target: Offset,
    max: MetadataRaftFetchMax,
) -> Result<(), RaftError> {
    let from = log.log_start_offset();
    let mut cursor = from;
    while cursor < target {
        let batches = log.read_decoded(cursor, max.size())?;
        let next = next_batch_offset(&batches);
        if batches.is_empty() {
            break;
        }
        for batch in &batches {
            for record in &batch.records {
                if !matches!(
                    replay_record_decision(
                        batch.base_offset,
                        record.offset_delta,
                        from.0,
                        target.0,
                        batch.attributes.is_control_batch(),
                        true,
                    ),
                    ReplayRecordDecision::Apply(_)
                ) {
                    continue;
                }
                let offset = batch
                    .base_offset
                    .saturating_add(i64::from(record.offset_delta));
                apply_control_record(state, record, offset)?;
            }
        }
        match replay_cursor_decision(cursor.0, next.map(|offset| offset.0)) {
            ReplayCursorDecision::Advance(next) => cursor = Offset(next),
            ReplayCursorDecision::Stop => break,
        }
    }
    Ok(())
}

/// Folds the control record at `offset` into `state`.
///
/// # Errors
/// [`MetadataReplayError::InvalidControlRecord`] as [`replay_control_records`]
/// describes.
fn apply_control_record(
    state: &mut QuorumState,
    record: &krabka_protocol::records::Record,
    offset: i64,
) -> Result<(), MetadataReplayError> {
    let invalid = |reason: String| MetadataReplayError::InvalidControlRecord { offset, reason };
    match decode_control_record(record).map_err(|error| invalid(error.to_string()))? {
        ControlRecord::KRaftVersion(record) => {
            state.kraft_version = u16::try_from(record.k_raft_version).map_err(|_| {
                invalid(format!("negative kraft.version {}", record.k_raft_version))
            })?;
        }
        ControlRecord::Voters(record) => {
            state.voters =
                voter_set_from_wire(&record).map_err(|error| invalid(error.to_string()))?;
        }
        _ => {}
    }
    Ok(())
}

pub fn control_state_at(
    log: &KraftLog,
    bootstrap: &QuorumState,
    end_offset: Offset,
    max: MetadataRaftFetchMax,
) -> Result<QuorumState, RaftError> {
    let mut state = bootstrap.clone();
    replay_control_records_until(log, &mut state, end_offset, max)?;
    Ok(state)
}
