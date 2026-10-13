//! One observer fetch round trip: the `API_KEY_METADATA_FETCH` request to a
//! controller voter, and the decode-and-apply step that folds the returned
//! record batches into the observer's `MetadataImage`.
//!
//! A response that names a snapshot instead of carrying records is handed to
//! [`snapshot::install_snapshot`], on the same connection, before the round
//! trip returns.

use {
    super::{
        ObserverConfig,
        snapshot::{SnapshotTarget, install_snapshot},
        store::ObserverStore,
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::records::{
        HEADER_LEN, RecordBatch, RecordBatchHeader, RecordsError, validate_one_v2_batch,
    },
    krabka_raft::NodeId,
    krabka_units::convert::ByteSizeExt as _,
    std::sync::Arc,
    tokio::sync::watch,
    tracing::{debug, error, info, warn},
};

/// What one successful observer fetch round trip learned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FetchOutcome {
    /// Offset to fetch from next: one past the last batch applied.
    pub(super) next_fetch_offset: u64,
    /// The quorum's committed offset, as the controller that answered last
    /// heard it. The observer trails it by whatever the response did not
    /// carry, so it is the only value that says how far behind this node is.
    ///
    /// This is the response's `quorum_high_watermark` and not its
    /// `high_watermark`: every controller serves this fetch, so the responder
    /// may be a follower whose own watermark is clamped to a log end far below
    /// what the quorum has committed. Reading that one would let an observer
    /// that had drawn level with a lagging follower call itself caught up.
    pub(super) quorum_high_watermark: i64,
    /// Lowest offset the responder still retains. Everything below it has been
    /// pruned behind a snapshot, so a fetch there reads no records at all.
    pub(super) log_start_offset: i64,
    /// The leader the responder believes is current, when it names one.
    pub(super) leader_hint: Option<NodeId>,
    /// Committed records this round trip could not decode and skipped.
    pub(super) load_errors: u64,
    /// An invalid `KRaft` control record this round trip met, which stops the
    /// broker.
    pub(super) fatal: Option<String>,
    /// Time spent decoding and applying newly returned committed records, in
    /// microseconds. `None` for a snapshot install or a fetch that did not
    /// advance.
    pub(super) pending_record_processing_time_us: Option<u64>,
    /// Append timestamp of the latest batch applied by this fetch, if any.
    pub(super) last_applied_record_timestamp_ms: Option<i64>,
}

/// Runs one iteration: it fetches from `addr` at `fetch_offset`, decodes and
/// applies the records, and returns the new fetch offset together with the
/// quorum's committed offset. It returns `None` on a transport error, so
/// that the caller fails over.
///
/// A response that carries a `snapshot_id` means `fetch_offset` has been pruned
/// away on the controller: the round trip then installs that snapshot over the
/// same connection and resumes at its end offset, instead of asking for the
/// same missing records again on every poll.
pub(super) async fn fetch_once(
    config: &ObserverConfig,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
    store: &mut ObserverStore,
) -> Option<FetchOutcome> {
    let opts = krabka_client_core::ConnectionOptions {
        client_id: config.client_id.clone(),
        dispatch_queue_capacity: config.client_dispatch_queue_capacity,
        frame_max: config.client_frame_max,
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = match config.dialer.dial(target, addr, opts).await {
        Ok(c) => c,
        Err(e) => {
            debug!(%addr, error = %e, "observer dial failed");
            return None;
        }
    };
    // One exit point past the dial, so the connection is closed on every path
    // through the fetch and the snapshot transfer that may follow it.
    let outcome = fetch_over(config, &conn, addr, target, fetch_offset, image_tx, store).await;
    conn.close();
    outcome
}

/// The round trip itself, over an open connection.
async fn fetch_over(
    config: &ObserverConfig,
    conn: &krabka_client_core::Connection,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
    store: &mut ObserverStore,
) -> Option<FetchOutcome> {
    let req = krabka_raft::KrabkaMetadataFetchRequest {
        fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
        max_bytes: config.max_bytes.bytes_i32(),
        // The leader lists this node in `DescribeQuorum` under these, as it
        // would a Kafka broker that fetches `__cluster_metadata`.
        replica_id: i32::try_from(config.node_id.0).unwrap_or(-1),
        replica_directory_id: config.directory_id,
    };
    let mut body = Vec::with_capacity(32);
    req.encode_v0(&mut body);
    // Negotiated from the `krabka.version` this observer has applied. Before
    // its first fetch the image is empty, which reads as level 0: v0, the
    // 1.0.0 baseline every controller serves.
    let version = krabka_raft::private_request_version(
        krabka_metadata::PrivateRpc::MetadataFetch,
        Some(&image_tx.borrow()),
    );

    let resp_body = match conn
        .raw_request(
            krabka_raft::API_KEY_METADATA_FETCH,
            version,
            bytes::Bytes::from(body),
        )
        .await
    {
        Ok(b) => b,
        Err(e) => {
            debug!(%addr, error = %e, "observer fetch request failed");
            return None;
        }
    };

    let mut cur: &[u8] = &resp_body;
    let resp = match krabka_raft::KrabkaMetadataFetchResponse::decode_v0(&mut cur) {
        Ok(r) => r,
        Err(e) => {
            warn!(%addr, error = %e, "observer response decode failed");
            return None;
        }
    };
    if resp.error_code != 0 {
        return None;
    }

    let (applied, pending_record_processing_time_us) = if let Some(snapshot_id) = resp.snapshot_id {
        (
            Applied {
                next_offset: install_snapshot(
                    config,
                    conn,
                    SnapshotTarget {
                        controller: target,
                        leader_epoch: resp.leader_epoch,
                    },
                    snapshot_id,
                    image_tx,
                    store,
                )
                .await?,
                load_errors: 0,
                fatal: None,
                last_applied_record_timestamp_ms: Some(store.last_applied_record_timestamp_ms()),
            },
            None,
        )
    } else {
        let started = std::time::Instant::now();
        let applied = apply_fetch_records(fetch_offset, &resp.records, image_tx);
        let processing_us = if applied.next_offset > fetch_offset {
            Some(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
        } else {
            None
        };
        (applied, processing_us)
    };
    Some(FetchOutcome {
        next_fetch_offset: applied.next_offset,
        quorum_high_watermark: resp.quorum_high_watermark,
        log_start_offset: resp.log_start_offset,
        leader_hint: u64::try_from(resp.leader_hint).ok().map(NodeId),
        load_errors: applied.load_errors,
        fatal: applied.fatal,
        pending_record_processing_time_us,
        last_applied_record_timestamp_ms: applied.last_applied_record_timestamp_ms,
    })
}

/// What applying the records of one fetch response did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Applied {
    /// Offset to fetch from next: one past the last whole batch the response
    /// carried; after a fatal fault, the offset of the batch that raised it;
    /// and the offset fetched from when the response's framing is corrupt.
    pub(super) next_offset: u64,
    /// Committed records that did not decode. The first one ends the apply
    /// of the response, so this is 0 or 1.
    pub(super) load_errors: u64,
    /// An invalid `KRaft` control record, which stops the broker.
    pub(super) fatal: Option<String>,
    /// Append timestamp of the latest fully applied record batch.
    pub(super) last_applied_record_timestamp_ms: Option<i64>,
}

/// One whole v2 batch of a fetch response: its parsed header and its bytes.
struct FramedBatch<'a> {
    header: &'a RecordBatchHeader,
    bytes: &'a [u8],
}

/// The batches of one fetch response, each with its header, after the checks
/// Kafka's raft layer makes before it appends the records of a `FETCH`
/// response.
///
/// `KafkaRaftClient.appendAsFollower` hands the records to
/// `KafkaMetadataLog.appendAsFollower`, and on to `UnifiedLog.appendAsFollower`.
/// Its `analyzeAndValidateRecords` walks `MemoryRecords.batches()`, whose
/// `ByteBufferLogInputStream` ends the walk at a trailing batch the buffer
/// does not hold in full, so `trimInvalidBytes` drops those bytes and the
/// whole batches before them are appended. A batch whose CRC does not match,
/// or whose header is not a v2 batch header, throws `CorruptRecordException`
/// instead, and the append takes none of the response. `appendAsFollower`
/// catches that exception, logs it at INFO, and leaves the log end where it
/// was. The next `FETCH` asks for the same offset again; no fault handler
/// sees it, and the node keeps running.
///
/// # Errors
/// The first batch whose framing is corrupt, other than a truncated tail.
fn framed_batches(records: &[u8]) -> Result<Vec<FramedBatch<'_>>, RecordsError> {
    let mut framed = Vec::new();
    let mut rest = records;
    while !rest.is_empty() {
        match validate_one_v2_batch(rest) {
            Ok(batch) => {
                let (bytes, after) = rest.split_at(batch.total_len);
                framed.push(FramedBatch {
                    header: batch.header,
                    bytes,
                });
                rest = after;
            }
            // A batch the response holds only part of, as a fetch cut at its
            // byte budget can end: it is dropped, as `trimInvalidBytes` drops
            // it, and fetched again next time.
            Err(RecordsError::HeaderTooShort { .. } | RecordsError::BodyTooShort { .. }) => break,
            Err(error) => return Err(error),
        }
    }
    Ok(framed)
}

/// `bytes` in lowercase hex, as Kafka's `KafkaRaftClient.convertToHexadecimal`
/// logs a batch header.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Folds the record batches of one fetch response into the image, as Kafka's
/// `MetadataLoader.handleCommit` loads one commit.
///
/// A record that does not decode is classified as the controller classifies
/// it ([`krabka_raft::decode_committed_value`]), so both roles skip the same
/// records. A broker-only node does not stop on one: Kafka's `SharedServer`
/// builds the "metadata loading" fault handler with
/// `fatal = processRoles.contains(ControllerRole)`, so on a broker it is a
/// `LoggingFaultHandler` that logs at ERROR and bumps
/// `metadata-load-error-count`. The throw also ends `handleCommit`'s loop
/// over the commit, so the records after it in that commit are never loaded,
/// and the loader goes on with the next commit. The rest of this response is
/// skipped the same way, and the fetch resumes past it.
///
/// An invalid `KRaft` control record is fatal on every Kafka role:
/// `KafkaRaftClientDriver` hands it to `SharedServer.raftManagerFaultHandler`,
/// which halts the process. The apply stops at it, and the caller stops the
/// broker.
///
/// Before any of that, the response's framing is checked as Kafka's raft
/// layer checks a `FETCH` response before it appends it (see
/// [`framed_batches`]). A response whose framing is corrupt applies nothing
/// and leaves the fetch offset where it was, so the next poll asks for the
/// same records again.
fn apply_fetch_records(
    fetch_offset: u64,
    records: &[u8],
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> Applied {
    // No new records: the controller had nothing past `fetch_offset`. Skip the
    // expensive full-image clone entirely.
    if records.is_empty() {
        return Applied {
            next_offset: fetch_offset,
            load_errors: 0,
            fatal: None,
            last_applied_record_timestamp_ms: None,
        };
    }

    let framed = match framed_batches(records) {
        Ok(framed) => framed,
        Err(error) => {
            // Kafka's `KafkaRaftClient.appendAsFollower` logs the failed
            // append at INFO, with the first batch header in hex, and appends
            // nothing.
            let header = &records[..records.len().min(HEADER_LEN)];
            info!(
                fetch_offset,
                %error,
                batch_header = %hex(header),
                "observer failed to append the records of a metadata fetch; fetching them again"
            );
            return Applied {
                next_offset: fetch_offset,
                load_errors: 0,
                fatal: None,
                last_applied_record_timestamp_ms: None,
            };
        }
    };

    let mut next: MetadataImage = (**image_tx.borrow()).clone();
    let mut new_offset = fetch_offset;
    let mut load_errors = 0;
    let mut fatal = None;
    let mut last_applied_record_timestamp_ms = None;
    for FramedBatch { header, mut bytes } in framed {
        let index = u64::try_from(header.base_offset.get().max(0)).unwrap_or(0);
        let next_offset = index
            .saturating_add(u64::try_from(header.last_offset_delta.get().max(0)).unwrap_or(0))
            .saturating_add(1);
        // After a record that did not decode, the rest of the response is
        // skipped: the offset moves past it, and nothing in it applies.
        if load_errors > 0 {
            new_offset = next_offset;
            continue;
        }
        // The framing and CRC are sound, so a batch that still does not
        // decode holds records that do not parse. Kafka's raft layer appends
        // such a batch, and its `RecordsIterator` throws when the
        // `MetadataLoader` reads it: the load error above.
        let batch = match RecordBatch::decode(&mut bytes) {
            Ok(batch) => batch,
            Err(error) => {
                error!(
                    offset = header.base_offset.get(),
                    %error,
                    "observer could not load a committed metadata batch; skipping the rest of \
                     this fetch"
                );
                load_errors += 1;
                new_offset = next_offset;
                continue;
            }
        };
        // A control batch carries no metadata records. Its KIP-853
        // `KRaftVersionRecord` and `VotersRecord` set the quorum the image
        // names, as they do on a controller once committed, and every record
        // this fetch returns is committed. That voter set is how a node in a
        // dynamic quorum, which knows only its bootstrap servers, learns the
        // endpoint of each voter.
        if batch.attributes.is_control_batch() {
            match krabka_raft::control_batch_image_records(&batch) {
                Ok(controls) => {
                    for control in &controls {
                        next.apply(control);
                    }
                    last_applied_record_timestamp_ms = Some(batch.max_timestamp);
                }
                Err(fault) => {
                    error!(%fault, "observer met an invalid KRaft control record; stopping the broker");
                    fatal = Some(fault.to_string());
                    break;
                }
            }
            new_offset = next_offset;
            continue;
        }
        for r in &batch.records {
            let Some(value) = r.value.as_ref() else {
                continue;
            };
            let offset = batch.base_offset.saturating_add(i64::from(r.offset_delta));
            match krabka_raft::decode_committed_value(value, &next, offset) {
                Ok(Some(rec)) => {
                    if let Err(e) = next.validate(&rec) {
                        warn!(offset, error = %e, "observer skipped record failing validation");
                        continue;
                    }
                    next.apply(&rec);
                }
                // A KIP-835 no-op, or a record naming state the image does
                // not hold, which the controller skips too.
                Ok(None) => {}
                Err(fault) => {
                    error!(
                        offset,
                        %fault,
                        "observer could not load a committed metadata record; skipping the rest \
                         of this fetch"
                    );
                    load_errors += 1;
                    break;
                }
            }
        }
        if load_errors == 0 {
            last_applied_record_timestamp_ms = Some(batch.max_timestamp);
        }
        new_offset = next_offset;
    }
    if new_offset != fetch_offset {
        let _ = image_tx.send_replace(Arc::new(next));
    }
    Applied {
        next_offset: new_offset.max(fetch_offset),
        load_errors,
        fatal,
        last_applied_record_timestamp_ms,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::single_partition_topic,
        assert2::assert,
        bytes::Bytes,
        krabka_metadata::{MetadataRecord, to_kraft_values},
        krabka_protocol::records::{Record, header::Attributes},
        uuid::Uuid,
    };

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(single_partition_topic(name, Uuid::new_v4()))
    }

    fn metadata_batch(base_offset: i64, rec: &MetadataRecord) -> RecordBatch {
        let values = to_kraft_values(rec, &MetadataImage::new(Uuid::nil())).expect("to kraft");
        let records: Vec<Record> = values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| Record {
                offset_delta: i32::try_from(idx).unwrap(),
                value: Some(value),
                ..Default::default()
            })
            .collect();
        RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap(),
            records,
            ..Default::default()
        }
    }

    /// A control batch holding one KIP-595 `LeaderChange` control record, as a
    /// leader starts its epoch with. The record carries a real key and value:
    /// a control record without them is refused, as Kafka's
    /// `RecordsIterator.decodeControlRecord` refuses it.
    fn control_batch(base_offset: i64) -> RecordBatch {
        use krabka_protocol::{
            owned::leader_change_message::LeaderChangeMessage,
            records::metadata::control::ControlRecord,
        };
        let (key, value) = ControlRecord::LeaderChange(LeaderChangeMessage::default())
            .encode_key_value()
            .expect("encode the leader change record");
        RecordBatch {
            base_offset,
            attributes: Attributes::default().with_control(true),
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                key: Some(key),
                value: Some(value),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn encode_batches(batches: &[RecordBatch]) -> Bytes {
        let mut out = Vec::new();
        for batch in batches {
            batch.encode(&mut out).expect("encode batch");
        }
        Bytes::from(out)
    }

    fn image_channel(cluster_id: Uuid) -> watch::Sender<Arc<MetadataImage>> {
        let (tx, _) = watch::channel(Arc::new(MetadataImage::new(cluster_id)));
        tx
    }

    #[test]
    fn apply_fetch_records_advances_past_control_batch() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[control_batch(6)]);

        let new_offset = apply_fetch_records(6, &records, &image_tx).next_offset;

        assert!(new_offset == 7);
    }

    /// The batch a leader of a freshly formatted dynamic quorum starts with:
    /// its `LeaderChange` marker, then the `kraft.version` and voter set of the
    /// bootstrap checkpoint. The observer takes the voters, endpoints included,
    /// into its image, which is how it reaches a leader it knows only by id.
    #[test]
    fn apply_fetch_records_takes_the_voter_set_of_a_control_batch_into_the_image() {
        use krabka_protocol::{
            owned::{
                k_raft_version_record::KRaftVersionRecord as WireKRaftVersion,
                voters_record::{
                    Endpoint, KRaftVersionFeature, Voter as WireVoter, VotersRecord as WireVoters,
                },
            },
            records::metadata::control::ControlRecord,
        };

        let cluster_id = Uuid::new_v4();
        let directory_id = Uuid::from_u128(3001);
        let controls = [
            ControlRecord::KRaftVersion(WireKRaftVersion {
                k_raft_version: 1,
                ..Default::default()
            }),
            ControlRecord::Voters(WireVoters {
                voters: vec![WireVoter {
                    voter_id: 3001,
                    voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                        *directory_id.as_bytes(),
                    ),
                    endpoints: vec![Endpoint {
                        name: "CONTROLLER_PLAINTEXT".into(),
                        host: "ducker12".into(),
                        port: 9592,
                        ..Default::default()
                    }],
                    k_raft_version_feature: KRaftVersionFeature {
                        min_supported_version: 0,
                        max_supported_version: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ];
        let mut batch = control_batch(0);
        for (offset_delta, control) in (1_i32..).zip(&controls) {
            let (key, value) = control.encode_key_value().expect("encode control record");
            batch.records.push(Record {
                offset_delta,
                key: Some(key),
                value: Some(value),
                ..Default::default()
            });
            batch.last_offset_delta = offset_delta;
        }
        let image_tx = image_channel(cluster_id);

        let new_offset = apply_fetch_records(0, &encode_batches(&[batch]), &image_tx).next_offset;

        let mut expected = MetadataImage::new(cluster_id);
        expected.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        ));
        expected.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
            voters: krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
                id: NodeId(3001),
                directory_id,
                endpoints: vec![krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER_PLAINTEXT".into(),
                    host: "ducker12".into(),
                    port: 9592,
                }],
                kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
            }]),
        }));
        assert!(new_offset == 3);
        assert!(**image_tx.borrow() == expected);
    }

    #[test]
    fn apply_fetch_records_advances_data_batch_offset_and_publishes() {
        let image_tx = image_channel(Uuid::new_v4());
        let mut batch = metadata_batch(4, &topic_record("offset-topic"));
        batch.max_timestamp = 1_700_000_000_123;
        let records = encode_batches(&[batch]);

        let applied = apply_fetch_records(4, &records, &image_tx);

        assert!(applied.next_offset == 5);
        assert!(applied.last_applied_record_timestamp_ms == Some(1_700_000_000_123));
        assert!(image_tx.borrow().topic("offset-topic").is_some());
    }

    #[test]
    fn apply_fetch_records_advances_past_every_offset_in_a_batch() {
        let image_tx = image_channel(Uuid::new_v4());
        let mut batch = metadata_batch(4, &topic_record("multi-record-offset-topic"));
        batch.last_offset_delta = 999;

        let new_offset = apply_fetch_records(4, &encode_batches(&[batch]), &image_tx).next_offset;

        assert!(new_offset == 1_004);
    }

    /// One record that does not apply, and what applying it must do.
    struct Case<'a> {
        /// What the record is.
        name: &'a str,
        /// The record value that does not apply.
        failing: Vec<u8>,
        /// What applying it must do.
        want: Want,
    }

    /// What applying one response must do with a record that does not
    /// apply.
    enum Want {
        /// Count one load error and skip the rest of the response.
        LoadError,
        /// Count one load error and skip the rest of the response, where the
        /// decoder refuses the bytes with exactly this error.
        LoadErrorOn(krabka_metadata::TranslateError),
        /// Skip the record alone, as the controller skips it.
        Skip,
        /// Skip the record alone, where the decoder reports exactly this
        /// error against the image: one the controller skips too.
        SkipOn(krabka_metadata::TranslateError),
        /// Whatever the pinned `krabka-metadata` decoder says: a load error
        /// where it refuses the bytes, an apply where it reads them.
        AsTheDecoderSays,
    }

    /// One row per failure kind. A response carries a topic before the
    /// failing record, a topic after it in the same batch, and a topic in a
    /// later batch. A record that does not decode counts one load error, and
    /// nothing after it in the response applies, as Kafka's `MetadataLoader`
    /// abandons the rest of a commit; the broker does not stop, and the fetch
    /// resumes past the response. A record the controller skips is skipped
    /// alone: an `InvalidReference`, which the image decides, is skipped,
    /// and an `InvalidValue`, which the bytes alone decide, is a load error.
    #[test]
    fn a_record_that_does_not_decode_counts_and_skips_the_rest_of_the_response() {
        use {
            crate::metadata_observer::test_support::{
                encode_batches, patched_topic_value, topic_value, undecodable_private_value,
                values_batch,
            },
            krabka_metadata::TranslateError,
            krabka_protocol::{
                owned::{
                    broker_registration_change_record::BrokerRegistrationChangeRecord,
                    partition_change_record::PartitionChangeRecord,
                },
                primitives::uuid::Uuid as KUuid,
                records::metadata::KraftMetadataRecord,
            },
        };

        let unknown_topic_config = to_kraft_values(
            &MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "ghost".into(),
                overrides: [("retention.ms".to_string(), "1".to_string())].into(),
            }),
            &MetadataImage::new(Uuid::nil()),
        )
        .expect("encode a topic config")
        .remove(0)
        .to_vec();
        // Partition 0 of topic `before`, which has no partitions.
        let unknown_partition_change =
            KraftMetadataRecord::PartitionChange(PartitionChangeRecord {
                topic_id: KUuid(Uuid::from_u128(1).into_bytes()),
                partition_id: 0,
                ..Default::default()
            })
            .encode_value(0)
            .expect("encode a partition change")
            .to_vec();
        // A `fenced` value Kafka's `BrokerRegistrationFencingChange` does not
        // define.
        let unknown_fenced_change =
            KraftMetadataRecord::BrokerRegistrationChange(BrokerRegistrationChangeRecord {
                broker_id: 1,
                broker_epoch: 1,
                fenced: 2,
                ..Default::default()
            })
            .encode_value(0)
            .expect("encode a broker registration change")
            .to_vec();
        let cases: [Case<'_>; 8] = [
            Case {
                name: "unknown apiKey",
                failing: patched_topic_value(1, 99),
                want: Want::LoadError,
            },
            Case {
                name: "value version above the highest supported",
                failing: patched_topic_value(2, 99),
                want: Want::LoadError,
            },
            Case {
                name: "undecodable krabka-private record",
                failing: undecodable_private_value(),
                want: Want::LoadError,
            },
            // `krabka-metadata` checks the KIP-631 frame version from the
            // revision that versions the private records on.
            Case {
                name: "frame version other than 1",
                failing: patched_topic_value(0, 2),
                want: Want::AsTheDecoderSays,
            },
            Case {
                name: "validate failure",
                failing: unknown_topic_config,
                want: Want::Skip,
            },
            Case {
                name: "empty KIP-835 no-op",
                failing: krabka_protocol::records::metadata::KraftMetadataRecord::NoOp(
                    krabka_protocol::owned::no_op_record::NoOpRecord::default(),
                )
                .encode_value(0)
                .expect("encode a no-op")
                .to_vec(),
                want: Want::Skip,
            },
            Case {
                name: "invalid reference: unknown partition of a known topic",
                failing: unknown_partition_change,
                want: Want::SkipOn(TranslateError::InvalidReference {
                    field: "partition change",
                    detail: "unknown partition before-0".into(),
                }),
            },
            Case {
                name: "invalid value: unknown fenced value",
                failing: unknown_fenced_change,
                want: Want::LoadErrorOn(TranslateError::InvalidValue {
                    field: "broker registration change fenced",
                    detail: "unknown value 2".into(),
                }),
            },
        ];

        for Case {
            name: case,
            failing,
            want,
        } in cases
        {
            let cluster_id = Uuid::nil();
            let image_tx = image_channel(cluster_id);
            let records = encode_batches(&[
                values_batch(0, &[topic_value("before", 1)]),
                values_batch(1, &[failing.clone(), topic_value("same-batch", 2)]),
                values_batch(3, &[topic_value("after", 3)]),
            ]);

            let applied = apply_fetch_records(0, &records, &image_tx);

            let load_error = match want {
                Want::LoadError => {
                    let refused = krabka_metadata::from_kraft_value(
                        &failing,
                        &MetadataImage::new(cluster_id),
                    )
                    .is_err();
                    assert!(refused, "{case}: the decoder must refuse the bytes");
                    true
                }
                Want::LoadErrorOn(error) => {
                    let refused =
                        krabka_metadata::from_kraft_value(&failing, &image_tx.borrow()).err();
                    assert!(refused == Some(error), "{case}: the decoder's error");
                    true
                }
                Want::Skip => false,
                Want::SkipOn(error) => {
                    let refused =
                        krabka_metadata::from_kraft_value(&failing, &image_tx.borrow()).err();
                    assert!(refused == Some(error), "{case}: the decoder's error");
                    false
                }
                Want::AsTheDecoderSays => {
                    krabka_metadata::from_kraft_value(&failing, &MetadataImage::new(cluster_id))
                        .is_err()
                }
            };
            let image = image_tx.borrow().clone();
            assert!(
                (
                    applied,
                    image.topic("before").is_some(),
                    image.topic("same-batch").is_some(),
                    image.topic("after").is_some(),
                ) == (
                    Applied {
                        next_offset: 4,
                        load_errors: u64::from(load_error),
                        fatal: None,
                        last_applied_record_timestamp_ms: Some(0),
                    },
                    true,
                    !load_error,
                    !load_error,
                ),
                "{case}"
            );
        }
    }

    /// An invalid `KRaft` control record stops the apply at its batch and
    /// reports the fault that stops the broker, as Kafka halts on one on
    /// every role. Nothing after it applies.
    #[test]
    fn an_invalid_control_record_is_fatal() {
        use crate::metadata_observer::test_support::{
            encode_batches, negative_kraft_version_batch, topic_value, values_batch,
        };

        let undecodable = {
            let mut batch = negative_kraft_version_batch(1);
            batch.records[0].value = Some(Bytes::from_static(&[0xff]));
            batch
        };
        let cases = [
            ("negative kraft.version", negative_kraft_version_batch(1)),
            ("control record that does not decode", undecodable),
        ];
        for (case, control) in cases {
            let image_tx = image_channel(Uuid::nil());
            let fault = krabka_raft::control_batch_image_records(&control)
                .expect_err("an invalid control record")
                .to_string();
            let records = encode_batches(&[
                values_batch(0, &[topic_value("before", 1)]),
                control,
                values_batch(2, &[topic_value("after", 2)]),
            ]);

            let applied = apply_fetch_records(0, &records, &image_tx);

            let image = image_tx.borrow().clone();
            assert!(
                (
                    applied,
                    image.topic("before").is_some(),
                    image.topic("after").is_some(),
                ) == (
                    Applied {
                        next_offset: 1,
                        load_errors: 0,
                        fatal: Some(fault),
                        last_applied_record_timestamp_ms: Some(0),
                    },
                    true,
                    false,
                ),
                "{case}"
            );
        }
    }

    /// The wire bytes of one metadata batch at `base_offset` holding a topic
    /// named `name`.
    fn topic_batch_bytes(base_offset: i64, name: &str, id: u128) -> Vec<u8> {
        use crate::metadata_observer::test_support::{encode_batches, topic_value, values_batch};
        encode_batches(&[values_batch(base_offset, &[topic_value(name, id)])]).to_vec()
    }

    /// `batch` with its `records_count` set to `count` and its CRC made good
    /// again: sound framing around records that do not parse.
    fn with_records_count(mut batch: Vec<u8>, count: i32) -> Vec<u8> {
        batch[57..61].copy_from_slice(&count.to_be_bytes());
        let crc = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        batch
    }

    /// One row per way a fetched batch can be malformed, each in a response
    /// that carries batches `a` at 0, `b` at 1 and `c` at 2, with `b` the
    /// malformed one.
    ///
    /// Kafka's raft layer refuses a response with a corrupt batch whole: the
    /// append takes none of it, and the fetch offset stays. A batch the
    /// response holds only part of is dropped with what follows it, and the
    /// whole batches before it apply. A batch whose framing and CRC are sound
    /// but whose records do not parse is the `MetadataLoader`'s load error:
    /// counted, with the rest of the response skipped and the offset moved
    /// past it.
    #[test]
    fn a_malformed_batch_is_refused_trimmed_or_counted_as_kafka_does() {
        let a = topic_batch_bytes(0, "a", 1);
        let b = topic_batch_bytes(1, "b", 2);
        let c = topic_batch_bytes(2, "c", 3);
        let corrupt_crc = {
            let mut bytes = b.clone();
            *bytes.last_mut().expect("a batch has bytes") ^= 0xff;
            bytes
        };
        let unsupported_magic = {
            let mut bytes = b.clone();
            bytes[16] = 1;
            bytes
        };
        let negative_length = {
            let mut bytes = b.clone();
            bytes[8..12].copy_from_slice(&(-1_i32).to_be_bytes());
            bytes
        };
        let truncated_body = b[..b.len() - 3].to_vec();
        let truncated_header = b[..20].to_vec();
        let unparseable_records = with_records_count(b.clone(), 5);

        let refused = Applied {
            next_offset: 0,
            load_errors: 0,
            fatal: None,
            last_applied_record_timestamp_ms: None,
        };
        let cases = [
            (
                "corrupt CRC",
                corrupt_crc,
                c.clone(),
                refused.clone(),
                [false; 3],
            ),
            (
                "unsupported magic",
                unsupported_magic,
                c.clone(),
                refused.clone(),
                [false; 3],
            ),
            (
                "negative batch length",
                negative_length,
                c.clone(),
                refused.clone(),
                [false; 3],
            ),
            (
                "truncated body",
                truncated_body,
                Vec::new(),
                Applied {
                    next_offset: 1,
                    load_errors: 0,
                    fatal: None,
                    last_applied_record_timestamp_ms: Some(0),
                },
                [true, false, false],
            ),
            (
                "truncated header",
                truncated_header,
                Vec::new(),
                Applied {
                    next_offset: 1,
                    load_errors: 0,
                    fatal: None,
                    last_applied_record_timestamp_ms: Some(0),
                },
                [true, false, false],
            ),
            (
                "records that do not parse in a sound batch",
                unparseable_records,
                c.clone(),
                Applied {
                    next_offset: 3,
                    load_errors: 1,
                    fatal: None,
                    last_applied_record_timestamp_ms: Some(0),
                },
                [true, false, false],
            ),
        ];

        for (case, malformed, tail, want, topics) in cases {
            let image_tx = image_channel(Uuid::nil());
            let response = [a.clone(), malformed, tail].concat();

            let applied = apply_fetch_records(0, &response, &image_tx);

            let image = image_tx.borrow().clone();
            assert!(
                (applied, [
                    image.topic("a").is_some(),
                    image.topic("b").is_some(),
                    image.topic("c").is_some(),
                ],) == (want, topics),
                "{case}"
            );
        }
    }

    #[test]
    fn hex_matches_kafkas_lowercase_header_dump() {
        assert!(hex(&[0x00, 0x0a, 0xff]) == "000aff");
    }
}
