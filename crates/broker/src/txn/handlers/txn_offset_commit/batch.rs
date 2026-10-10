//! The transactional append to `__consumer_offsets` that carries a
//! `TxnOffsetCommit`'s offsets.
//!
//! The rows are the ordinary `OffsetCommitKey` / `OffsetCommitValue` pair, but
//! the batch around them is stamped `is_transactional=true` with the
//! producer's (pid, epoch), so the log's LSO machinery withholds the offsets
//! until a commit or abort marker resolves the transaction.
//!
//! The append is a group coordinator write like any other: it goes to the log
//! only while this broker leads the offsets partition, it carries the leader
//! epoch, and the commit is answered only once the high watermark covers it
//! (see [`crate::coordinator::unified::offsets_log::append_as_leader`]).

use krabka_metadata::NodeId;
use krabka_protocol::{
    owned::txn_offset_commit_request::TxnOffsetCommitRequest,
    records::{Attributes, Record, RecordBatch},
};

use crate::{
    codes,
    coordinator::{
        persistence::OffsetCommitValue,
        unified::offsets_log::{LeaderAppend, append_as_leader},
    },
    error::BrokerError,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
};

/// What one `TxnOffsetCommit` wrote to the log: the offsets-log position of
/// its records, and the `(topic, partition)` keys they cover.
#[derive(Debug)]
pub(super) struct AppendedTxnOffsets {
    /// Base offset the batch was assigned in `__consumer_offsets`.
    pub(super) written_at: i64,
    pub(super) keys: Vec<(String, i32)>,
}

/// Append the transactional offset records to `__consumer_offsets`, and
/// report where they landed, which `(topic, partition)` keys they cover, and
/// the write whose commit the caller still waits for. `None` means every row
/// was denied or unknown, and nothing was appended.
///
/// The offsets partition's `WriteTxnMarkers` handler materializes these records
/// into the owning group actor after the commit marker is durable. This keeps
/// visibility on the group-coordinator broker even when the transaction
/// coordinator is a different broker.
///
/// The returned keys are the ones the caller marks pending on the group actor
/// for KIP-447. They come from the same walk that builds the batch, so a key
/// can never be marked pending without a record in the log behind it for the
/// transaction's marker to find again. The base offset travels with them
/// because it is what orders the mark against that marker.
///
/// `local` is this broker's partitions, the metadata that names the offsets
/// partition's leader, and this broker's id. `producer_check` is the KIP-890
/// check the log runs under its append lock, with the guard the producer's
/// verification started, and a refusal is answered as Kafka's append throws
/// it. `record_topic_ids` says whether the offset records carry the topic ids
/// the handler resolved, which Kafka 4.3.1 does not record.
pub(super) async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    local: (&PartitionRegistry, &dyn MetadataSource, NodeId),
    offsets_partition: i32,
    now_ms: i64,
    (denied_topics, unknown_rows): (
        &std::collections::HashSet<String>,
        &std::collections::HashSet<(String, i32)>,
    ),
    (producer_check, record_topic_ids): (crate::partition::ProducerAppendCheck, bool),
) -> Result<Option<(AppendedTxnOffsets, LeaderAppend)>, i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        base_timestamp: now_ms,
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
        // TxnOffsetCommit records are broker-generated, so the request has no
        // client sequence. Use the stable first sequence so the log retains
        // the producer epoch needed to fence the completion marker.
        base_sequence: 0,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    let mut keys: Vec<(String, i32)> = Vec::new();
    for topic in &req.topics {
        if denied_topics.contains(&topic.name) {
            continue;
        }
        for part in &topic.partitions {
            if unknown_rows.contains(&(topic.name.clone(), part.partition_index)) {
                continue;
            }
            let value = OffsetCommitValue {
                offset: krabka_log::Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
                // `TxnOffsetCommit` has no `retention_time_ms` field at any
                // version, so a transactional commit always takes the
                // broker-wide retention.
                expire_timestamp_ms: None,
                // The id the handler resolved the topic to, as Kafka trunk's
                // `OffsetAndMetadata.fromRequest` keeps it (KIP-1319). 4.3.1
                // keeps the zero id, which is no id.
                topic_id: Some(uuid::Uuid::from_bytes(topic.topic_id.0))
                    .filter(|id| record_topic_ids && !id.is_nil()),
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(
                    OffsetCommitValue::encode_key(&req.group_id, &topic.name, part.partition_index)
                        .map_err(|error| {
                            tracing::warn!(
                                group_id = %req.group_id,
                                %error,
                                "transactional offset commit key is not encodable",
                            );
                            codes::UNKNOWN_SERVER_ERROR
                        })?,
                ),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            keys.push((topic.name.clone(), part.partition_index));
            delta += 1;
        }
    }

    // If every row was denied or unknown, there's nothing to append; succeed
    // silently.
    if batch.records.is_empty() {
        return Ok(None);
    }

    batch.last_offset_delta = (delta - 1).max(0);

    match append_as_leader(local, offsets_partition, batch, Some(producer_check)).await {
        Ok(write) => Ok(Some((
            AppendedTxnOffsets {
                written_at: write.base_offset.get(),
                keys,
            },
            write,
        ))),
        Err(error) => {
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                %error,
                "TxnOffsetCommit: the offsets append failed"
            );
            Err(append_error_code(&error))
        }
    }
}

/// The code a `TxnOffsetCommit` whose append failed answers, as Kafka's
/// `CoordinatorOperationExceptionHelper.handleOperationException` maps the
/// exception `CoordinatorPartitionWriter.append` throws.
///
/// A refusal of the producer check is the exception Kafka's append throws:
/// `INVALID_PRODUCER_EPOCH` or `INVALID_TXN_STATE`. A partition this broker
/// does not lead, or has no live replica of, is `NOT_LEADER_OR_FOLLOWER`,
/// which the helper answers `NOT_COORDINATOR`. Anything else is unexpected.
pub(super) fn append_error_code(error: &BrokerError) -> i16 {
    match error {
        BrokerError::TransactionAppend(_) | BrokerError::CoordinatorWriteUncommitted { .. } => {
            codes::from_broker_error(error)
        }
        BrokerError::PartitionWriterDied { .. } => codes::NOT_COORDINATOR,
        _ => codes::UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, Offset, ReadOutput};
    use krabka_metadata::MetadataImage;

    use super::*;
    use crate::{
        coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC},
        test_support::{FakeMetadataSource, open_partition},
        txn::handlers::txn_offset_commit::{test_support::request, verification::offset_batch},
    };

    /// This broker.
    const NODE: NodeId = NodeId(1);

    /// Metadata in which `leader` leads the offsets partition at `epoch`.
    fn offsets_led_by(leader: NodeId, epoch: i32) -> FakeMetadataSource {
        FakeMetadataSource::builder()
            .image(offsets_image(leader, epoch))
            .build()
    }

    /// An image in which `leader` leads the offsets partition at `epoch`.
    fn offsets_image(leader: NodeId, epoch: i32) -> MetadataImage {
        crate::coordinator::test_support::offsets_partition_image(
            (OFFSETS_PARTITION, leader, epoch),
            &[leader],
        )
    }

    /// Metadata in which this broker leads the offsets partition.
    fn led_here() -> FakeMetadataSource {
        offsets_led_by(NODE, 0)
    }

    fn offsets_registry() -> (tempfile::TempDir, Arc<PartitionRegistry>) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let registry = Arc::new(PartitionRegistry::new());
        registry.insert(
            OFFSETS_TOPIC.into(),
            PartitionIndex(OFFSETS_PARTITION),
            open_partition(
                dir.path(),
                crate::test_support::StandalonePartitionSetup {
                    topic: OFFSETS_TOPIC,
                    partition: krabka_ids::PartitionIndex(OFFSETS_PARTITION),
                    ..Default::default()
                },
            ),
        );
        (dir, registry)
    }

    fn offsets_partition(registry: &PartitionRegistry) -> Arc<crate::partition::Partition> {
        registry
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
            .expect("offsets partition")
    }

    fn read_offsets(log: &Log) -> ReadOutput {
        log.read(Offset(0), krabka_units::mebibytes(1))
            .expect("read offsets log")
    }

    /// Bind the registered partition and keep its read guard in the caller's scope.
    macro_rules! read_registered_offsets {
        ($registry:expr; $part:ident, $log:ident, $read:ident) => {
            let $part = offsets_partition($registry);
            let $log = $part.log.lock().expect("lock offsets log");
            let $read = read_offsets(&$log);
        };
    }

    /// The append config of these tests, evaluated in the original argument order.
    macro_rules! append_as_test_leader {
        ($req:expr, $registry:expr, $filters:expr, $check:expr, $topic_ids:expr) => {
            append_txn_batch(
                $req,
                ($registry, &led_here(), NODE),
                OFFSETS_PARTITION,
                12_345,
                $filters,
                ($check, $topic_ids),
            )
        };
    }

    /// The check a verification of `req`'s producer on the offsets partition
    /// hands the append, as `verification::verify_producer` starts it.
    async fn verified(
        registry: &PartitionRegistry,
        req: &TxnOffsetCommitRequest,
    ) -> crate::partition::ProducerAppendCheck {
        let part = offsets_partition(registry);
        let batch = offset_batch(req);
        let guard = part
            .start_transaction_verification(batch, false, (0, i64::MAX))
            .await
            .expect("verification starts");
        crate::partition::ProducerAppendCheck { batch, guard }
    }

    async fn append_here(
        req: &TxnOffsetCommitRequest,
        registry: &PartitionRegistry,
        filters: (&HashSet<String>, &HashSet<(String, i32)>),
        record_topic_ids: bool,
    ) -> Result<Option<(AppendedTxnOffsets, LeaderAppend)>, i16> {
        append_as_test_leader!(
            req,
            registry,
            filters,
            verified(registry, req).await,
            record_topic_ids
        )
        .await
    }

    #[tokio::test]
    async fn append_txn_batch_writes_transactional_offset_records() {
        let (_dir, registry) = offsets_registry();
        let req = request();

        let (appended, _write) =
            append_here(&req, &registry, (&HashSet::new(), &HashSet::new()), false)
                .await
                .expect("append batch")
                .expect("records appended");
        check!(appended.written_at == 0);
        check!(appended.keys == vec![("orders".to_string(), 2), ("orders".to_string(), 3)]);

        read_registered_offsets!(&registry; part, log, read);
        assert!(read.batches.len() == 1);
        let batch = &read.batches[0];
        check!(batch.attributes.is_transactional());
        check!(batch.max_timestamp == 12_345);
        check!(log.offset_for_timestamp(12_345) == Some((Offset(0), 12_345)));
        check!(batch.producer_id == 47);
        check!(batch.producer_epoch == 5);
        check!(batch.base_sequence == 0);
        check!(batch.last_offset_delta == 1);
        check!(log.transaction_marker_state(krabka_log::ProducerId(47)) == (5, -1, true));
        let record_rows: Vec<_> = batch
            .records
            .iter()
            .map(|r| {
                (
                    r.offset_delta,
                    r.timestamp_delta,
                    r.key.is_some(),
                    r.value.is_some(),
                )
            })
            .collect();
        assert!(record_rows == vec![(0, 0, true, true), (1, 0, true, true)]);
    }

    #[tokio::test]
    async fn append_txn_batch_skips_denied_topics_without_appending() {
        let (_dir, registry) = offsets_registry();
        let req = request();
        let denied = maplit::hashset! {"orders".to_string()};

        let appended = append_here(&req, &registry, (&denied, &HashSet::new()), false)
            .await
            .expect("all denied succeeds");
        check!(appended.is_none());
        read_registered_offsets!(&registry; part, log, read);
        assert!(read.batches.is_empty());
    }

    #[tokio::test]
    async fn append_txn_batch_returns_not_coordinator_when_offsets_partition_missing() {
        let registry = Arc::new(PartitionRegistry::new());
        let req = request();
        let unverified = crate::partition::ProducerAppendCheck {
            batch: offset_batch(&req),
            guard: krabka_log::VerificationGuard::SENTINEL,
        };
        let err = append_as_test_leader!(
            &req,
            &registry,
            (&HashSet::new(), &HashSet::new()),
            unverified,
            false
        )
        .await
        .expect_err("missing offsets partition");

        assert!(err == codes::NOT_COORDINATOR);
    }

    /// Kafka's `CoordinatorRuntime` writes a `TxnOffsetCommit` as the leader of
    /// the offsets partition, with its leader epoch on the batch, and answers
    /// it once the high watermark covers the batch under that leadership. A
    /// broker that answered at the local append could acknowledge offsets the
    /// next leader never gets, and the transaction then commits without them.
    #[tokio::test]
    async fn the_append_is_a_leader_write_that_commits_under_its_term() {
        struct Case {
            what: &'static str,
            /// The leader and leader epoch of the offsets partition.
            led_by: (NodeId, i32),
            /// The leader and epoch the partition moves to after the append.
            moves_to: Option<(NodeId, i32)>,
            /// The append's answer, then the commit's.
            expected: Result<Result<(), i16>, i16>,
            /// The leader epoch of every batch in the log afterwards.
            logged_epochs: Vec<i32>,
        }
        let cases = [
            Case {
                what: "this broker leads",
                led_by: (NODE, 7),
                moves_to: None,
                expected: Ok(Ok(())),
                logged_epochs: vec![7],
            },
            Case {
                what: "another broker leads",
                led_by: (NodeId(2), 7),
                moves_to: None,
                expected: Err(codes::NOT_COORDINATOR),
                logged_epochs: vec![],
            },
            Case {
                what: "the partition moves before the commit is confirmed",
                led_by: (NODE, 7),
                moves_to: Some((NodeId(2), 8)),
                expected: Ok(Err(codes::NOT_COORDINATOR)),
                logged_epochs: vec![7],
            },
        ];
        for case in cases {
            let (_dir, registry) = offsets_registry();
            let metadata = offsets_led_by(case.led_by.0, case.led_by.1);
            let req = request();

            let appended = append_txn_batch(
                &req,
                (&registry, &metadata, NODE),
                OFFSETS_PARTITION,
                12_345,
                (&HashSet::new(), &HashSet::new()),
                (verified(&registry, &req).await, false),
            )
            .await;
            if let Some((leader, epoch)) = case.moves_to {
                metadata.set_image(offsets_image(leader, epoch));
            }
            let outcome = match appended {
                Ok(Some((_, write))) => Ok(write
                    .committed()
                    .await
                    .map_err(|error| codes::from_broker_error(&error))),
                Ok(None) => panic!("{}: every row was appended", case.what),
                Err(code) => Err(code),
            };

            let part = offsets_partition(&registry);
            let logged_epochs: Vec<i32> = read_offsets(&part.log.lock().expect("lock offsets log"))
                .batches
                .iter()
                .map(|batch| batch.partition_leader_epoch)
                .collect();
            assert!(outcome == case.expected, "{}", case.what);
            assert!(logged_epochs == case.logged_epochs, "{}", case.what);
        }
    }

    #[tokio::test]
    async fn append_txn_batch_skips_unknown_rows_without_appending_them() {
        let (_dir, registry) = offsets_registry();
        let req = request();
        let unknown = maplit::hashset! {("orders".to_string(), 3)};

        let appended = append_here(&req, &registry, (&HashSet::new(), &unknown), false)
            .await
            .expect("append batch")
            .expect("one row still appended");
        let (appended, _write) = appended;
        check!(appended.keys == vec![("orders".to_string(), 2)]);

        read_registered_offsets!(&registry; part, log, read);
        assert!(read.batches.len() == 1);
        assert!(read.batches[0].records.len() == 1);
    }

    /// Kafka 4.3.1 records the zero topic id for a transactional commit
    /// (`OffsetAndMetadata.fromRequest(partition, now)`), and trunk records the
    /// id its `KafkaApis` resolved.
    #[tokio::test]
    async fn the_topic_id_is_recorded_only_when_the_caller_asks_for_it() {
        let topic_id = uuid::Uuid::from_u128(0xfeed);
        // (label, record the topic id, the id the offset record carries)
        for (mode, record_topic_ids, recorded) in
            [("4.3.1", false, None), ("trunk", true, Some(topic_id))]
        {
            let (_dir, registry) = offsets_registry();
            let mut req = request();
            req.topics[0].topic_id = krabka_protocol::primitives::uuid::Uuid(topic_id.into_bytes());

            append_here(
                &req,
                &registry,
                (&HashSet::new(), &HashSet::new()),
                record_topic_ids,
            )
            .await
            .expect("append batch")
            .expect("records appended");

            read_registered_offsets!(&registry; part, log, read);
            let recorded_ids: Vec<Option<uuid::Uuid>> = read.batches[0]
                .records
                .iter()
                .map(|record| {
                    OffsetCommitValue::decode_value(record.value.as_deref().expect("a value"))
                        .expect("decode the offset record")
                        .topic_id
                })
                .collect();
            assert!(recorded_ids == vec![recorded, recorded], "{mode}");
        }
    }

    /// The log runs its producer check under the append lock, so an append the
    /// verification did not admit never lands: a stale epoch is
    /// `INVALID_PRODUCER_EPOCH`, and a producer with no verified transaction is
    /// `INVALID_TXN_STATE`.
    #[tokio::test]
    async fn the_append_refuses_a_producer_the_log_check_does_not_admit() {
        let (_dir, registry) = offsets_registry();
        let req = request();
        append_here(&req, &registry, (&HashSet::new(), &HashSet::new()), false)
            .await
            .expect("the verified producer appends");

        let mut stale = request();
        stale.producer_epoch = req.producer_epoch - 1;
        let mut other = request();
        other.producer_id += 1;
        // (label, request, expected code)
        let cases = [
            ("a stale epoch", stale, codes::INVALID_PRODUCER_EPOCH),
            ("an unverified producer", other, codes::INVALID_TXN_STATE),
        ];
        for (label, req, expected) in cases {
            let unverified = crate::partition::ProducerAppendCheck {
                batch: offset_batch(&req),
                guard: krabka_log::VerificationGuard::SENTINEL,
            };
            let refused = append_as_test_leader!(
                &req,
                &registry,
                (&HashSet::new(), &HashSet::new()),
                unverified,
                false
            )
            .await
            .expect_err(label);
            assert!(refused == expected, "{label}");
        }

        read_registered_offsets!(&registry; part, log, read);
        assert!(read.batches.len() == 1, "only the verified append landed");
    }
}
