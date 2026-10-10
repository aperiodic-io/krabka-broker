//! Decoding of a canonical `.checkpoint` byte stream back into its KIP-853
//! control state and its KIP-631 metadata records.
//!
//! The batch order a checkpoint must follow is enforced by a small state
//! machine, so a truncated or reordered artifact is rejected rather than
//! half-applied. The byte-range accessor that serves `FetchSnapshot` requests
//! is here too, because it reads the same artifact.

use krabka_metadata::{MetadataImage, from_kraft_value};
use krabka_protocol::records::{RecordBatch, metadata::control::ControlRecord};
use uuid::Uuid;

use super::{SnapshotContents, SnapshotControlState, voters::voter_set_from_wire};
use crate::error::RaftError;

/// Reads a canonical `.checkpoint` byte stream back into the sequence of
/// `MetadataRecord`s it contains (skipping the header/footer control
/// batches), plus a raw byte-range accessor for `FetchSnapshot` serving.
pub(crate) struct SnapshotReader;

impl SnapshotReader {
    /// The `last_contained_log_timestamp` of the `SnapshotHeaderRecord` that
    /// opens a checkpoint, read from its first batch alone: Kafka's
    /// `Snapshots.lastContainedLogTimestamp`.
    pub(crate) fn last_contained_log_timestamp(bytes: &[u8]) -> Result<i64, RaftError> {
        let mut cursor: &[u8] = bytes;
        let batch = RecordBatch::decode(&mut cursor)?;
        let header = batch
            .attributes
            .is_control_batch()
            .then(|| batch.records.first())
            .flatten()
            .and_then(|record| record.key.as_ref().zip(record.value.as_ref()));
        match header.map(|(key, value)| ControlRecord::decode(key, value)) {
            Some(Ok(ControlRecord::SnapshotHeader(header))) => {
                Ok(header.last_contained_log_timestamp)
            }
            Some(Err(error)) => Err(error.into()),
            _ => Err(invalid_snapshot_order()),
        }
    }

    /// Decode a canonical checkpoint, separating KIP-853 control state from
    /// KIP-631 metadata records.
    pub(crate) fn read(bytes: &[u8]) -> Result<SnapshotContents, RaftError> {
        let mut cursor: &[u8] = bytes;
        let mut records = Vec::new();
        let mut stage = SnapshotReadStage::Header;
        let mut kraft_version = None;
        let mut voters = None;
        let mut last_contained_log_timestamp = 0;
        // A context image accumulating decoded records in log order so each
        // subsequent `from_kraft_value` resolves topic ids / whole-map config
        // merges / ACL ids against prior records. The cluster id is irrelevant
        // to translation, so a nil placeholder suffices.
        let mut ctx = MetadataImage::new(Uuid::nil());
        while !cursor.is_empty() {
            let batch = RecordBatch::decode(&mut cursor)?;
            if batch.attributes.is_control_batch() {
                for record in &batch.records {
                    let (Some(key), Some(value)) = (&record.key, &record.value) else {
                        return Err(invalid_snapshot_order());
                    };
                    match (stage, ControlRecord::decode(key, value)?) {
                        (SnapshotReadStage::Header, ControlRecord::SnapshotHeader(header)) => {
                            last_contained_log_timestamp = header.last_contained_log_timestamp;
                            stage = SnapshotReadStage::KRaftVersion;
                        }
                        (SnapshotReadStage::KRaftVersion, ControlRecord::KRaftVersion(record)) => {
                            kraft_version =
                                Some(u16::try_from(record.k_raft_version).map_err(|_| {
                                    RaftError::ChangeRejected(
                                        "negative kraft.version in snapshot".into(),
                                    )
                                })?);
                            stage = SnapshotReadStage::Voters;
                        }
                        (SnapshotReadStage::Voters, ControlRecord::Voters(record)) => {
                            voters = Some(voter_set_from_wire(&record)?);
                            stage = SnapshotReadStage::MetadataOrFooter;
                        }
                        (
                            SnapshotReadStage::MetadataOrFooter
                            | SnapshotReadStage::KRaftVersion
                            | SnapshotReadStage::LegacyMetadataOrFooter,
                            ControlRecord::SnapshotFooter(_),
                        ) => {
                            stage = SnapshotReadStage::Done;
                        }
                        _ => return Err(invalid_snapshot_order()),
                    }
                }
                continue;
            }
            if stage == SnapshotReadStage::KRaftVersion {
                stage = SnapshotReadStage::LegacyMetadataOrFooter;
            }
            if !matches!(
                stage,
                SnapshotReadStage::MetadataOrFooter | SnapshotReadStage::LegacyMetadataOrFooter
            ) {
                return Err(invalid_snapshot_order());
            }
            for rec in &batch.records {
                let Some(value) = rec.value.as_ref() else {
                    continue;
                };
                let decoded = from_kraft_value(value, &ctx)
                    .map_err(|e| RaftError::ChangeRejected(format!("snapshot decode: {e}")))?;
                ctx.apply(&decoded);
                records.push(decoded);
            }
        }
        if stage != SnapshotReadStage::Done {
            return Err(invalid_snapshot_order());
        }
        Ok(SnapshotContents {
            control_state: match (kraft_version, voters) {
                (Some(kraft_version), Some(voters)) => Some(SnapshotControlState {
                    kraft_version,
                    voters,
                }),
                (None, None) => None,
                _ => return Err(invalid_snapshot_order()),
            },
            last_contained_log_timestamp,
            metadata_records: records,
        })
    }

    /// Return the `[position, position + max)` slice of `bytes`, clamped
    /// to the buffer length. A `position` at or past EOF yields an empty
    /// slice. Used to serve `FetchSnapshot` byte-range requests (KIP-595
    /// §`FetchSnapshot`).
    pub(crate) fn byte_range(bytes: &[u8], position: usize, max: usize) -> &[u8] {
        let start = position.min(bytes.len());
        let end = start.saturating_add(max).min(bytes.len());
        &bytes[start..end]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotReadStage {
    Header,
    KRaftVersion,
    Voters,
    MetadataOrFooter,
    LegacyMetadataOrFooter,
    Done,
}

fn invalid_snapshot_order() -> RaftError {
    RaftError::ChangeRejected(
        "snapshot must contain header, kraft.version, voters, metadata, footer in order".into(),
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::{BufMut, BytesMut};
    use krabka_metadata::{
        FeatureLevelRecord, LeaderRecoveryState, MetadataRecord, NodeId, PartitionElrRecord,
        PartitionRecord, PartitionRecoveryRecord, TopicRecord, Voter, VoterEndpoint, VoterSet,
        voters::KRaftVersionRange,
    };
    use krabka_protocol::{
        owned::{
            k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
            snapshot_footer_record::SnapshotFooterRecord,
            snapshot_header_record::SnapshotHeaderRecord,
            voters_record::VotersRecord as WireVotersRecord,
        },
        records::metadata::control::encode_typed_control_batch,
    };

    use super::*;
    use crate::snapshot::{
        SNAPSHOT_KRAFT_VERSION_BASE_OFFSET, SNAPSHOT_VOTERS_BASE_OFFSET, SnapshotWriter,
    };

    krabka_macros::single_replica_partition_fixture!(single_replica_partition);

    fn sample_voter(id: NodeId, port: u16) -> Voter {
        Voter {
            id,
            directory_id: Uuid::from_u128(u128::from(id.0)),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port,
            }],
            kraft_version: KRaftVersionRange { min: 0, max: 1 },
        }
    }

    /// The header timestamp reads from the first batch alone, and anything
    /// that does not open with a snapshot header is refused.
    #[test]
    fn the_header_timestamp_reads_from_the_first_batch() {
        let image = MetadataImage::new(Uuid::nil());
        let snapshot = SnapshotWriter::serialize(&image, 1_700_000_123_456).unwrap();
        assert2::check!(
            SnapshotReader::last_contained_log_timestamp(&snapshot).ok() == Some(1_700_000_123_456)
        );
        for (what, bytes) in [("empty", &b""[..]), ("garbage", &b"\x00\x01\x02"[..])] {
            assert2::check!(
                SnapshotReader::last_contained_log_timestamp(bytes).is_err(),
                "{what}"
            );
        }
    }

    #[test]
    fn writer_reader_round_trips_image() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        // A realistic topic: the `V1Topic` plus its partition records. KIP-631
        // framing carries no partition count on the `TopicRecord`, so the round
        // trip derives partitions/RF from the partition records — a bare
        // `V1Topic` (declaring partitions but with no `V1Partition`s) would not
        // round-trip its declared count.
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 2,
        }));
        for p in 0..3 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                replicas: vec![NodeId(1), NodeId(2)],
                isr: vec![NodeId(1), NodeId(2)],
                ..single_replica_partition("orders", krabka_ids::PartitionIndex(p), NodeId(1))
            }));
        }
        image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
            topic: "orders".into(),
            partition: 0,
            eligible_leader_replicas: vec![NodeId(2)],
            last_known_elr: vec![],
        }));
        image.apply(&MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: "orders".into(),
                partition: 0,
                state: LeaderRecoveryState::Recovering,
            },
        ));

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        assert2::assert!(MetadataImage::from_records(cid, &records) == image);
    }

    #[test]
    fn writer_reader_round_trips_kip853_control_state_separately() {
        let cid = Uuid::new_v4();
        let voters = VoterSet::from_voters([
            sample_voter(NodeId(1), 9_093),
            sample_voter(NodeId(2), 9_094),
        ]);
        let mut image = MetadataImage::new(cid);
        image.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        ));
        image.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
            voters: voters.clone(),
        }));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::new_v4(),
            partitions: 0,
            replication_factor: 1,
        }));

        let bytes = SnapshotWriter::serialize(&image, 123).expect("serialize");
        let snapshot = SnapshotReader::read(&bytes).expect("read snapshot");

        assert2::assert!(
            snapshot.control_state
                == Some(SnapshotControlState {
                    kraft_version: 1,
                    voters,
                })
        );
        assert2::assert!(snapshot.metadata_records.iter().all(|record| !matches!(
            record,
            MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
        )));
        assert2::assert!(snapshot.metadata_records.len() == 1);
    }

    #[test]
    fn reader_accepts_legacy_header_data_footer_snapshot() {
        let mut image = MetadataImage::new(Uuid::new_v4());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "legacy".into(),
            topic_id: Uuid::new_v4(),
            partitions: 0,
            replication_factor: 1,
        }));
        let encoded = SnapshotWriter::serialize(&image, 0).expect("serialize current snapshot");
        let mut input = encoded.as_ref();
        let mut legacy = BytesMut::new();
        while !input.is_empty() {
            let batch = RecordBatch::decode(&mut input).expect("decode current batch");
            if !matches!(
                batch.base_offset,
                SNAPSHOT_KRAFT_VERSION_BASE_OFFSET | SNAPSHOT_VOTERS_BASE_OFFSET
            ) {
                batch.encode(&mut legacy).expect("encode legacy batch");
            }
        }

        let snapshot = SnapshotReader::read(&legacy).expect("read legacy snapshot");
        assert2::assert!(snapshot.control_state.is_none());
        assert2::assert!(snapshot.metadata_records.len() == 1);
    }

    /// A snapshot of an image carrying finalized KIP-584 features must
    /// reproduce both the feature levels AND the finalized-features epoch
    /// exactly on read-back. Regression guard for the bug where `to_records`
    /// emitted no `V1FeatureLevel` records: `metadata.version` (range guard /
    /// SCRAM + delegation-token gates) and `group.version` (next-gen consumer
    /// groups) silently vanished after any compaction or learner snapshot
    /// install. The epoch (3 here, from a re-finalize) exceeds the live feature
    /// count (2), so a naive "replay one record per feature" fix would
    /// reconstruct epoch=1 and fail this assertion.
    #[test]
    fn writer_reader_round_trips_image_with_features() {
        let cid = Uuid::new_v4();
        let mut image = MetadataImage::new(cid);
        for (name, level) in [
            ("metadata.version", 24),
            ("metadata.version", 25),
            ("group.version", 1),
            ("metadata.version", 25),
        ] {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: name.into(),
                level,
            }));
        }
        assert2::assert!(image.finalized_features_epoch() == 3);

        let bytes = SnapshotWriter::serialize(&image, 1_700_000_000_000).unwrap();
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        let rebuilt = MetadataImage::from_records(cid, &records);
        assert2::assert!(rebuilt == image);
        check!(
            (
                rebuilt.finalized_features().get("metadata.version"),
                rebuilt.finalized_features().get("group.version"),
                rebuilt.finalized_features_epoch(),
            ) == (Some(&25), Some(&1), 3)
        );
    }

    #[test]
    fn writer_reader_round_trips_empty_image() {
        let cid = Uuid::new_v4();
        let image = MetadataImage::new(cid);

        let bytes = SnapshotWriter::serialize(&image, 0).unwrap();
        let records = SnapshotReader::read(&bytes).unwrap().metadata_records;
        assert2::assert!(records.is_empty());
        assert2::assert!(MetadataImage::from_records(cid, &records) == image);
    }

    #[test]
    fn reader_rejects_out_of_order_kip853_controls() {
        let mut bytes = BytesMut::new();
        bytes.put_slice(
            &encode_typed_control_batch(
                0,
                &ControlRecord::SnapshotHeader(SnapshotHeaderRecord::default()),
            )
            .expect("header"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(1, &ControlRecord::Voters(WireVotersRecord::default()))
                .expect("voters"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(
                2,
                &ControlRecord::KRaftVersion(WireKRaftVersionRecord::default()),
            )
            .expect("kraft.version"),
        );
        bytes.put_slice(
            &encode_typed_control_batch(
                3,
                &ControlRecord::SnapshotFooter(SnapshotFooterRecord::default()),
            )
            .expect("footer"),
        );

        assert2::assert!(SnapshotReader::read(&bytes).is_err());
    }

    #[test]
    fn byte_range_returns_expected_slice() {
        type TestCase1<'a> = (&'a str, usize, usize, &'a [u8]);
        let buf: Vec<u8> = (0u8..=255).collect();
        let cases: [TestCase1<'_>; 3] = [
            // In-range read.
            ("in-range read", 10, 5, &buf[10..15]),
            // Position past EOF → empty.
            ("position past EOF", 1000, 5, &[]),
            // Length clamps to buffer end.
            ("length clamped to end", 250, 100, &buf[250..]),
        ];
        for (_case, position, max, want) in cases {
            assert2::assert!(SnapshotReader::byte_range(&buf, position, max) == want);
        }
    }
}
