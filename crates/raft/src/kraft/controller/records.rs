//! Construction, encoding and decoding of the record batches the engine writes
//! to and reads from the metadata log: metadata value batches, KIP-853 typed
//! control batches, and the KIP-595 `LeaderChange` marker.

use {
    crate::{
        error::{MetadataReplayError, RaftError},
        kraft::types::{Epoch, NodeId},
    },
    krabka_ids::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, TranslateError, VoterSet, from_kraft_value},
    krabka_protocol::{
        Decode, Encode,
        owned::no_op_record::NoOpRecord,
        records::{
            Record, RecordBatch,
            metadata::{KraftMetadataRecord, control::ControlRecord, decode_value_header},
        },
    },
};

/// Decodes the committed metadata value at `offset` against `image`, as every
/// controller replay path reads it: live apply, restart recovery, the
/// downgrade-snapshot rebuild and the leader's walk of an earlier epoch's
/// tail.
///
/// Bytes that are not a metadata record this build reads are a
/// [`MetadataReplayError::UndecodableRecord`], and the caller stops the
/// controller. Kafka's controller cannot replay such a record either:
/// `MetadataRecordSerde` throws on it, and `QuorumController` and the
/// controller-role `MetadataLoader` hand the throw to a fatal fault handler
/// (`SharedServer.fatalQuorumControllerFaultHandler` and
/// `SharedServer.metadataLoaderFaultHandler`, whose `fatal` is
/// `processRoles.contains(ProcessRole.ControllerRole)`). The snapshot reader
/// refuses the same bytes, so a snapshot and the log it replaces agree.
///
/// An empty KIP-835 `NoOpRecord` is `Ok(None)`: it changes nothing.
///
/// A record that decodes but names a topic, partition or ACL the image does
/// not hold is `Ok(None)`: the caller skips it, as it skips a record that fails
/// `MetadataImage::validate`. Kafka never commits such a record, because its
/// active controller replays each record before it appends it. A krabka leader
/// checks a write against its committed image, so a write can lose a race to an
/// earlier one still in flight (a partition change behind the deletion of its
/// topic). Every replica then decodes the same bytes against the same image
/// and skips the same record, so the replicas agree. Stopping on it instead
/// would stop every controller on a race a client can cause.
///
/// `TranslateError::InvalidReference` is one of those image lookups (an
/// unknown partition, a directory list measured against the image's replicas)
/// and is skipped too. `TranslateError::InvalidValue` is decided by the bytes
/// alone (an unknown `fenced` or `leader_recovery_state` value, an integer out
/// of range) and is undecodable, as are the krabka-private record errors
/// (`UnknownPrivateTag`, `UnknownPrivateRecordVersion`, `PrivateTagMismatch`,
/// `TrailingPrivateRecordBytes`): only the image-resolution errors below are
/// skips.
///
/// # Errors
/// [`MetadataReplayError::UndecodableRecord`] as above.
pub fn decode_committed_value(
    value: &[u8],
    image: &MetadataImage,
    offset: i64,
) -> Result<Option<MetadataRecord>, MetadataReplayError> {
    // A KIP-835 no-op changes nothing, and has no image record to become.
    if is_kip835_noop(value) {
        return Ok(None);
    }
    match from_kraft_value(value, image) {
        Ok(record) => Ok(Some(record)),
        Err(
            error @ (TranslateError::UnknownTopicId(_)
            | TranslateError::UnknownTopicName(_)
            | TranslateError::UnknownAclId(_)
            | TranslateError::InvalidReference { .. }),
        ) => {
            tracing::warn!(
                offset,
                %error,
                "kraft: skipped a committed record that names state the image does not hold"
            );
            Ok(None)
        }
        Err(error) => Err(MetadataReplayError::UndecodableRecord { offset, error }),
    }
}

/// The api key of Kafka's `NoOpRecord`.
const NO_OP_RECORD_API_KEY: u32 = 20;

/// The value bytes of the empty KIP-835 `NoOpRecord` at apiVersion 0, which
/// Kafka's `QuorumController` appends every `metadata.max.idle.interval.ms`.
///
/// # Errors
/// Returns the [`RaftError`] of the record encoder.
pub fn noop_record_value() -> Result<bytes::Bytes, RaftError> {
    Ok(KraftMetadataRecord::NoOp(NoOpRecord::default()).encode_value(0)?)
}

/// Whether `value` is an empty KIP-835 `NoOpRecord`: a record that changes
/// nothing, which every replay skips. A `NoOpRecord` that carries a private
/// tagged field is a krabka record carrier, and it is not one.
#[must_use]
pub fn is_kip835_noop(value: &[u8]) -> bool {
    let mut header = value;
    if decode_value_header(&mut header).ok().map(|h| h.api_key) != Some(NO_OP_RECORD_API_KEY) {
        return false;
    }
    matches!(
        KraftMetadataRecord::decode_value(value),
        Ok((KraftMetadataRecord::NoOp(record), _)) if record.unknown_tagged_fields.0.is_empty()
    )
}

pub fn metadata_record_batch(
    leader_epoch: Epoch,
    blobs: &[bytes::Bytes],
) -> Result<RecordBatch, RaftError> {
    if blobs.is_empty() {
        return Ok(RecordBatch {
            partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
            ..Default::default()
        });
    }

    let deltas = krabka_verified::metadata_record_offset_deltas(blobs.len()).ok_or_else(|| {
        RaftError::ChangeRejected("metadata batch offset deltas exceed i32".to_string())
    })?;
    // The kernel returns one delta per record, so the batch is non-empty.
    let last_offset_delta = deltas.last().copied().unwrap_or_default();
    let records: Vec<Record> = blobs
        .iter()
        .zip(deltas)
        .map(|(blob, offset_delta)| Record {
            offset_delta,
            value: Some(blob.clone()),
            ..Default::default()
        })
        .collect();

    Ok(RecordBatch {
        partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
        last_offset_delta,
        records,
        ..Default::default()
    })
}

pub fn typed_control_batch(
    leader_epoch: Epoch,
    controls: &[ControlRecord],
) -> Result<RecordBatch, RaftError> {
    let records = controls
        .iter()
        .enumerate()
        .map(|(index, control)| {
            let (key, value) = control.encode_key_value()?;
            Ok(Record {
                offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
                key: Some(key),
                value: Some(value),
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, krabka_protocol::ProtocolError>>()?;
    Ok(RecordBatch {
        partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
        attributes: krabka_protocol::records::Attributes::default().with_control(true),
        last_offset_delta: i32::try_from(controls.len().saturating_sub(1)).unwrap_or(i32::MAX),
        records,
        ..Default::default()
    })
}

/// Decode one record of a control batch.
///
/// # Errors
/// [`RaftError::MalformedControlRecord`] for a missing or empty key or value,
/// as Kafka's `RecordsIterator.decodeControlRecord` throws, and the decoder's
/// error for a key or value that does not decode.
pub fn decode_control_record(record: &Record) -> Result<ControlRecord, RaftError> {
    let key = match &record.key {
        None => {
            return Err(RaftError::MalformedControlRecord(
                "Missing key in the record when a key was expected",
            ));
        }
        Some(key) if key.is_empty() => {
            return Err(RaftError::MalformedControlRecord(
                "Got an unexpected empty key in the record",
            ));
        }
        Some(key) => key,
    };
    let value = match &record.value {
        None => {
            return Err(RaftError::MalformedControlRecord(
                "Missing value in the record when a value was expected",
            ));
        }
        Some(value) if value.is_empty() => {
            return Err(RaftError::MalformedControlRecord(
                "Got an unexpected empty value in the record",
            ));
        }
        Some(value) => value,
    };
    Ok(ControlRecord::decode(key, value)?)
}

/// Build the leader's `LeaderChange` control batch for `epoch`: a single
/// KIP-595 `LeaderChange` control record (control-batch attribute set), naming
/// the new leader and the current voter set. A real `KRaft` batch MUST contain at
/// least one record — an empty batch crashes a JVM follower
/// (`Batch must contain at least one record`) — so this carries the proper
/// `LeaderChangeMessage` rather than zero records. Krabka readers skip it via
/// `is_control_batch()`; it occupies exactly one log offset
/// (`last_offset_delta = 0`), unchanged from the prior empty batch.
///
/// The message is always version 0, at every `kraft.version`. Kafka writes it
/// at `ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION`, which is 0, and
/// reads it at that version too, so a version 1 message, with its voter
/// directory ids, is unreadable to `kafka-dump-log` and to a JVM replica.
// cargo-mutants: the `version: 0` field equals `LeaderChangeMessage`'s `Default` (i16 -> 0), so
// deleting it yields byte-identical encoding; it is not the wire schema version
// (that is the `0` passed to `msg.encode`). Equivalent mutant.
#[cfg_attr(test, mutants::skip)]
pub fn leader_change_batch(epoch: Epoch, leader_id: NodeId, voter_set: &VoterSet) -> RecordBatch {
    use krabka_protocol::{
        Encode,
        owned::{
            common::leader_change_message::voter::Voter, leader_change_message::LeaderChangeMessage,
        },
        records::{
            header::Attributes,
            metadata::control::{ControlRecordType, control_record_key},
        },
    };

    let voters: Vec<Voter> = voter_set
        .iter()
        .map(|voter| Voter {
            voter_id: i32::try_from(voter.id.0).unwrap_or(i32::MAX),
            ..Default::default()
        })
        .collect();
    let msg = LeaderChangeMessage {
        version: 0,
        leader_id: i32::try_from(leader_id.0).unwrap_or(i32::MAX),
        voters: voters.clone(),
        granting_voters: voters,
        ..Default::default()
    };
    let mut value = bytes::BytesMut::new();
    // LeaderChangeMessage v0; encode is infallible for a well-formed message.
    let _ = msg.encode(&mut value, 0);
    let key = control_record_key(ControlRecordType::LeaderChange);
    RecordBatch {
        partition_leader_epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
        attributes: Attributes::default().with_control(true),
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            key: Some(key),
            value: Some(value.freeze()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Build the batch a leader starts `epoch` with: the [`leader_change_batch`]
/// marker, then each of `controls` at the offsets that follow it.
///
/// # Errors
/// Returns the [`RaftError`] of the control-record encoder.
pub fn start_of_epoch_batch(
    epoch: Epoch,
    leader_id: NodeId,
    voter_set: &VoterSet,
    controls: &[ControlRecord],
) -> Result<RecordBatch, RaftError> {
    let mut batch = leader_change_batch(epoch, leader_id, voter_set);
    for (offset_delta, control) in (1_i32..).zip(controls) {
        let (key, value) = control.encode_key_value()?;
        batch.records.push(Record {
            offset_delta,
            key: Some(key),
            value: Some(value),
            ..Default::default()
        });
        batch.last_offset_delta = offset_delta;
    }
    Ok(batch)
}

/// Encode a run of `RecordBatch`es into one contiguous `Bytes` blob (each batch
/// is self-describing via its `batch_length` header, so they concatenate and
/// decode back in order — see [`decode_batches`]). Used by the leader's Fetch
/// serve path to ship replicated record bytes to a follower.
pub fn encode_batches(batches: &[RecordBatch]) -> bytes::Bytes {
    let mut out = bytes::BytesMut::new();
    for batch in batches {
        if let Err(e) = batch.encode(&mut out) {
            tracing::error!(?e, "kraft: encode batch for fetch serve failed");
        }
    }
    out.freeze()
}

/// Decode the contiguous `Bytes` blob produced by [`encode_batches`] back into a
/// `Vec<RecordBatch>` (each batch's `base_offset` is preserved). Used by the
/// follower's Fetch-response apply path.
pub fn decode_batches(mut buf: &[u8]) -> Result<Vec<RecordBatch>, RaftError> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        match RecordBatch::decode(&mut buf) {
            Ok(batch) => out.push(batch),
            Err(e) => {
                return Err(RaftError::ChangeRejected(format!(
                    "decode replicated batch: {e}"
                )));
            }
        }
    }
    Ok(out)
}

pub fn next_batch_offset(batches: &[RecordBatch]) -> Option<Offset> {
    batches.last().map(|batch| {
        Offset(
            batch
                .base_offset
                .saturating_add(i64::from(batch.last_offset_delta))
                .saturating_add(1),
        )
    })
}

#[cfg(test)]
mod metadata_record_batch_tests {
    use {super::*, assert2::check};

    #[test]
    fn metadata_record_offset_deltas_cover_empty_single_and_multiple_batches() {
        let empty = metadata_record_batch(5, &[]).expect("empty batch");
        check!(empty.records.is_empty());
        check!(empty.last_offset_delta == 0);
        check!(empty.partition_leader_epoch == 5);

        for (blobs, expected_deltas) in [
            (vec![bytes::Bytes::from_static(b"a")], vec![0]),
            (
                vec![
                    bytes::Bytes::from_static(b"a"),
                    bytes::Bytes::from_static(b"b"),
                    bytes::Bytes::from_static(b"c"),
                ],
                vec![0, 1, 2],
            ),
        ] {
            let batch = metadata_record_batch(5, &blobs).expect("metadata batch");
            check!(batch.partition_leader_epoch == 5);
            check!(
                batch
                    .records
                    .iter()
                    .map(|record| record.offset_delta)
                    .collect::<Vec<_>>()
                    == expected_deltas
            );
            check!(batch.last_offset_delta == *expected_deltas.last().expect("non-empty"));
        }

        let control = typed_control_batch(7, &[ControlRecord::KRaftVersion(
            krabka_protocol::owned::k_raft_version_record::KRaftVersionRecord::default(),
        )])
        .expect("control batch");
        check!(control.partition_leader_epoch == 7);
    }
}
