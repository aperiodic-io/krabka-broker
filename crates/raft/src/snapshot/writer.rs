//! Serialization of a [`MetadataImage`] into the canonical KIP-630
//! `.checkpoint` byte layout.
//!
//! The batch order, the base offsets of each batch, and the rule that
//! `V1Voters` and `V1KRaftVersion` travel as KIP-853 control records rather
//! than as KIP-631 metadata values are all decided here.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_metadata::{MetadataImage, MetadataRecord, to_kraft_values};
use krabka_protocol::{
    owned::{
        k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
        snapshot_footer_record::SnapshotFooterRecord, snapshot_header_record::SnapshotHeaderRecord,
    },
    records::{
        Record, RecordBatch,
        metadata::control::{ControlRecord, encode_typed_control_batch},
    },
};

use super::{
    SNAPSHOT_DATA_BASE_OFFSET, SNAPSHOT_HEADER_BASE_OFFSET, SNAPSHOT_KRAFT_VERSION_BASE_OFFSET,
    SNAPSHOT_VOTERS_BASE_OFFSET, SnapshotControlState, voters::voter_set_to_wire,
};
use crate::error::RaftError;

/// Identifies a snapshot by the log position it covers: `end_offset` is
/// the offset of the last record contained in the snapshot, and `epoch`
/// is the leader epoch at that offset. The engine names the on-disk artifact
/// `<end_offset>-<epoch>.checkpoint` (both fields zero-padded so lexical sort
/// matches numeric sort) and parses it back directly.
///
/// Serializes a [`MetadataImage`] into the canonical KIP-630
/// `.checkpoint` byte layout: header, `KRaftVersion`, and `Voters` control
/// batches, one data batch of `MetadataRecord` values, then a footer control
/// batch — concatenated encoded Kafka `RecordBatch`es.
pub(crate) struct SnapshotWriter;

impl SnapshotWriter {
    /// Produce the full `.checkpoint` bytes for `image`.
    /// `last_contained_log_timestamp` is the create-time of the last log
    /// record folded into this snapshot (recorded in the header).
    pub(crate) fn serialize(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, RaftError> {
        Self::serialize_with_control_state(
            image,
            last_contained_log_timestamp,
            &SnapshotControlState::from_image(image),
        )
    }

    /// Produce a checkpoint with an explicitly supplied committed KIP-853
    /// control state.
    pub(crate) fn serialize_with_control_state(
        image: &MetadataImage,
        last_contained_log_timestamp: i64,
        control_state: &SnapshotControlState,
    ) -> Result<Bytes, RaftError> {
        // Each `MetadataRecord` is translated against the very image being
        // snapshotted (a whole-map V1TopicConfig diffs against its own image and
        // so emits all-sets-no-tombstones — correct for a from-scratch snapshot).
        let mut value_blobs: Vec<Bytes> = Vec::new();
        for rec in &image.to_records() {
            // `V1Voters` / `V1KRaftVersion` are encoded as KIP-853 control
            // records, never as KIP-631 metadata values.
            if matches!(
                rec,
                MetadataRecord::V1Voters(_)
                    | MetadataRecord::V1KRaftVersion(_)
                    | MetadataRecord::V1PartitionElr(_)
                    | MetadataRecord::V1PartitionRecovery(_)
            ) {
                continue;
            }
            let mut blobs = to_kraft_values(rec, image)
                .map_err(|e| RaftError::ChangeRejected(format!("snapshot encode: {e}")))?;
            value_blobs.append(&mut blobs);
        }
        Self::write(last_contained_log_timestamp, control_state, value_blobs)
    }

    /// Produce the bootstrap checkpoint of a dynamic format: the KIP-853
    /// control state, then `records` in their own order.
    ///
    /// Kafka's `Formatter.writeBoostrapSnapshot` appends the bootstrap records
    /// to its snapshot writer as they are, and the active controller later
    /// writes them to the log in that order. So each record is translated
    /// against the image that the records before it produce, as the
    /// controller translates them.
    pub(crate) fn serialize_bootstrap(
        control_state: &SnapshotControlState,
        records: &[MetadataRecord],
        last_contained_log_timestamp: i64,
    ) -> Result<Bytes, RaftError> {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        let mut value_blobs: Vec<Bytes> = Vec::new();
        for record in records {
            let mut blobs = to_kraft_values(record, &image)
                .map_err(|e| RaftError::ChangeRejected(format!("snapshot encode: {e}")))?;
            value_blobs.append(&mut blobs);
            image.apply(record);
        }
        Self::write(last_contained_log_timestamp, control_state, value_blobs)
    }

    /// The `.checkpoint` bytes for `control_state` and the KIP-631 value
    /// blobs of the metadata records.
    fn write(
        last_contained_log_timestamp: i64,
        control_state: &SnapshotControlState,
        value_blobs: Vec<Bytes>,
    ) -> Result<Bytes, RaftError> {
        let mut out = BytesMut::new();

        // (1) SnapshotHeader control batch at base_offset 0 — the real KIP-630
        // `SnapshotHeaderRecord` (flexible message), encoded via the protocol
        // control-batch builder so the JVM `kafka-dump-log` decoder parses it.
        let header = SnapshotHeaderRecord {
            last_contained_log_timestamp,
            ..Default::default()
        };
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_HEADER_BASE_OFFSET,
            &ControlRecord::SnapshotHeader(header),
        )?);

        // (2) KIP-853 control state. Kafka snapshots place the finalized
        // kraft.version before the voter set that it governs.
        let kraft_version = i16::try_from(control_state.kraft_version).map_err(|_| {
            RaftError::ChangeRejected("snapshot kraft.version exceeds int16".into())
        })?;
        let kv_record = WireKRaftVersionRecord {
            version: 0,
            k_raft_version: kraft_version,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_KRAFT_VERSION_BASE_OFFSET,
            &ControlRecord::KRaftVersion(kv_record),
        )?);
        out.put_slice(&encode_typed_control_batch(
            SNAPSHOT_VOTERS_BASE_OFFSET,
            &ControlRecord::Voters(voter_set_to_wire(&control_state.voters)?),
        )?);

        // (3) Data batch at base_offset 3: one record per KIP-631 value blob.
        let total_blobs = value_blobs.len();
        if !value_blobs.is_empty() {
            let last_offset_delta = total_blobs
                .checked_sub(1)
                .and_then(|delta| i32::try_from(delta).ok())
                .unwrap_or(i32::MAX);
            let data_records = value_blobs
                .into_iter()
                .enumerate()
                .map(|(i, blob)| {
                    let mut record = Record {
                        value: Some(blob),
                        ..Default::default()
                    };
                    record.offset_delta = i32::try_from(i).unwrap_or(i32::MAX);
                    record
                })
                .collect();
            let mut data_batch = RecordBatch {
                records: data_records,
                ..Default::default()
            };
            data_batch.base_offset = SNAPSHOT_DATA_BASE_OFFSET;
            data_batch.last_offset_delta = last_offset_delta;
            data_batch.encode(&mut out)?;
        }

        // (4) SnapshotFooter control batch (real KIP-630 `SnapshotFooterRecord`).
        let footer_base_offset = SNAPSHOT_DATA_BASE_OFFSET
            .saturating_add(i64::try_from(total_blobs).unwrap_or(i64::MAX));
        let footer = SnapshotFooterRecord::default();
        out.put_slice(&encode_typed_control_batch(
            footer_base_offset,
            &ControlRecord::SnapshotFooter(footer),
        )?);

        Ok(out.freeze())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::NodeId;
    use krabka_protocol::owned::voters_record::VotersRecord as WireVotersRecord;
    use uuid::Uuid;

    use super::*;

    krabka_macros::snapshot_topic_fixture!(append_snapshot_topic);

    fn decode_single_control(batch: &RecordBatch) -> ControlRecord {
        let record = batch.records.first().expect("one control record");
        ControlRecord::decode(
            record.key.as_ref().expect("control key"),
            record.value.as_ref().expect("control value"),
        )
        .expect("decode control")
    }

    fn check_header(cursor: &mut &[u8], expected: SnapshotHeaderRecord) {
        let header = RecordBatch::decode(cursor).expect("header batch");
        check!(
            (
                header.base_offset,
                header.attributes.is_control_batch(),
                header.records.len()
            ) == (0, true, 1)
        );
        assert2::assert!(decode_single_control(&header) == ControlRecord::SnapshotHeader(expected));
    }

    #[test]
    fn writer_emits_canonical_header_data_offsets_and_footer() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        append_snapshot_topic(&mut image);

        let timestamp = 1_700_000_000_123;
        let bytes = SnapshotWriter::serialize(&image, timestamp).unwrap();
        let mut cur: &[u8] = &bytes;

        check_header(
            &mut cur,
            SnapshotHeaderRecord {
                version: 0,
                last_contained_log_timestamp: timestamp,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            },
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch(),
                kraft_version.records.len(),
            ) == (1, true, 1)
        );
        assert2::assert!(
            decode_single_control(&kraft_version)
                == ControlRecord::KRaftVersion(WireKRaftVersionRecord {
                    version: 0,
                    k_raft_version: 0,
                    ..Default::default()
                })
        );

        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!(
            (
                voters.base_offset,
                voters.attributes.is_control_batch(),
                voters.records.len(),
            ) == (2, true, 1)
        );
        assert2::assert!(
            decode_single_control(&voters) == ControlRecord::Voters(WireVotersRecord::default())
        );

        let data = RecordBatch::decode(&mut cur).expect("data batch");
        check!(
            (
                data.base_offset,
                data.attributes.is_control_batch(),
                data.records.len() >= 2
            ) == (3, false, true)
        );
        check!(
            data.last_offset_delta
                == i32::try_from(data.records.len() - 1).expect("record count fits")
        );
        for (i, record) in data.records.iter().enumerate() {
            assert2::assert!(record.offset_delta == i32::try_from(i).expect("index fits"));
            assert2::assert!(record.value.is_some());
        }

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            footer.base_offset == 3 + i64::try_from(data.records.len()).expect("record count fits")
        );
        check!((footer.attributes.is_control_batch(), footer.records.len()) == (true, 1));
        let ctrl = decode_single_control(&footer);
        assert2::assert!(ctrl == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default()));
        check!(cur.is_empty());
    }

    #[test]
    fn writer_emits_empty_snapshot_header_and_footer_offsets() {
        let image = MetadataImage::new(Uuid::new_v4());

        let bytes = SnapshotWriter::serialize(&image, 99).unwrap();
        let mut cur: &[u8] = &bytes;

        check_header(
            &mut cur,
            SnapshotHeaderRecord {
                version: 0,
                last_contained_log_timestamp: 99,
                ..Default::default()
            },
        );

        let kraft_version = RecordBatch::decode(&mut cur).expect("kraft.version batch");
        check!(
            (
                kraft_version.base_offset,
                kraft_version.attributes.is_control_batch()
            ) == (1, true)
        );
        let voters = RecordBatch::decode(&mut cur).expect("voters batch");
        check!((voters.base_offset, voters.attributes.is_control_batch()) == (2, true));

        let footer = RecordBatch::decode(&mut cur).expect("footer batch");
        check!(
            (
                footer.base_offset,
                footer.attributes.is_control_batch(),
                footer.records.len()
            ) == (3, true, 1)
        );
        assert2::assert!(
            decode_single_control(&footer)
                == ControlRecord::SnapshotFooter(SnapshotFooterRecord::default())
        );
        check!(cur.is_empty());
    }

    /// The bootstrap checkpoint keeps the bootstrap records in their own
    /// order, `metadata.version` first, as Kafka's formatter appends them. An
    /// image of the same records would list the feature levels last, by name.
    #[test]
    fn the_bootstrap_checkpoint_keeps_the_records_in_their_order() {
        use krabka_metadata::{FeatureLevelRecord, Voter, VoterEndpoint};

        use crate::snapshot::{SnapshotContents, SnapshotReader};

        let feature = |name: &str, level: i16| {
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level,
            })
        };
        let records = vec![
            feature("metadata.version", 27),
            feature("group.version", 1),
            MetadataRecord::V1ScramCredential(krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: krabka_security::SaslMechanism::ScramSha512,
                salt: vec![1; 16],
                stored_key: vec![2; 64],
                server_key: vec![3; 64],
                iterations: 4096,
            }),
            feature("eligible.leader.replicas.version", 1),
        ];
        let control_state = SnapshotControlState {
            kraft_version: 1,
            voters: krabka_metadata::VoterSet::from_voters([Voter {
                id: NodeId(1),
                directory_id: Uuid::from_u128(1),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "localhost".into(),
                    port: 9093,
                }],
                kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
            }]),
        };

        let bytes = SnapshotWriter::serialize_bootstrap(&control_state, &records, 5).unwrap();

        check!(
            SnapshotReader::read(&bytes).unwrap()
                == SnapshotContents {
                    control_state: Some(control_state),
                    last_contained_log_timestamp: 5,
                    metadata_records: records,
                }
        );
    }

    /// The apiKeys of Kafka's `MetadataRecordType` table
    /// (`metadata/src/main/resources/common/metadata/*Record.json`).
    /// `MetadataRecordSerde.apiMessageFor` throws for any other id, so
    /// `kafka-dump-log` and `kafka-metadata-shell` fail on the record.
    const KAFKA_METADATA_RECORD_API_KEYS: [u32; 27] = [
        0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
        28, 29,
    ];

    /// Every data record of a checkpoint names an apiKey that Kafka knows,
    /// also for the krabka-only state. One row for each krabka-only record.
    #[test]
    fn writer_emits_only_kafka_metadata_record_types() {
        use krabka_metadata::{
            BreakGlassAction, BreakGlassProposalRecord, FeatureLevelRecord,
            PartitionOffsetAdvanceRecord, PatternType, TopicFreezeRecord,
        };
        use krabka_protocol::records::metadata::KraftMetadataRecord;

        let rows: [(&str, MetadataRecord); 4] = [
            (
                "features epoch",
                MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: "metadata.version".into(),
                    level: 25,
                }),
            ),
            (
                "diskless offset advance",
                MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                    topic: "orders".into(),
                    partition: 0,
                    count: 12,
                }),
            ),
            (
                "write freeze",
                MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
                    scope: "orders".into(),
                    pattern_type: PatternType::Literal,
                    frozen: true,
                    reason: "incident".into(),
                    set_by: "User:alice".into(),
                    set_at_ms: 1,
                    proposal_id: Uuid::nil(),
                    key_id: String::new(),
                    signature: vec![],
                }),
            ),
            (
                "break-glass proposal",
                MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
                    proposal_id: Uuid::from_u128(0xB1),
                    action: BreakGlassAction::ThawTopicFreeze,
                    target: "literal:orders".into(),
                    proposer: "User:alice".into(),
                    reason: "incident closed".into(),
                    created_at_ms: 1,
                    expires_at_ms: 2,
                    approvals: vec![],
                    consumed_at_ms: 0,
                    withdrawn: false,
                }),
            ),
        ];
        for (label, rec) in rows {
            let mut image = MetadataImage::new(Uuid::new_v4());
            image.apply(&rec);

            let bytes = SnapshotWriter::serialize(&image, 1).unwrap();
            let mut cur: &[u8] = &bytes;
            let mut api_keys = Vec::new();
            while !cur.is_empty() {
                let batch = RecordBatch::decode(&mut cur).expect("batch");
                if batch.attributes.is_control_batch() {
                    continue;
                }
                for record in &batch.records {
                    let value = record.value.as_ref().expect("metadata value");
                    let (decoded, _) = KraftMetadataRecord::decode_value(value).expect("decode");
                    api_keys.push(decoded.api_key());
                }
            }

            check!(!api_keys.is_empty(), "{label}");
            check!(
                api_keys
                    .iter()
                    .all(|key| KAFKA_METADATA_RECORD_API_KEYS.contains(key)),
                "{label}: {api_keys:?}"
            );
        }
    }
}
