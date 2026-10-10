//! Appending one transaction marker to a partition, and the offset resolution
//! that a `__consumer_offsets` marker triggers.
//!
//! Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
//! path go through this module rather than calling the partition themselves,
//! so that the durable marker and the in-memory publication can never drift
//! apart. Both reach it through [`super::leader`], which checks the leadership
//! of the partition first and waits for the marker to commit after.

use std::{collections::HashMap, sync::Arc};

use krabka_log::Offset;
use krabka_verified::transaction::{
    TransactionMarkerMaterializationDecision as Decision, TransactionMarkerPartitionState,
    TransactionMarkerRequest,
};

use super::offsets::{pending_offset_entries, resolve_pending_offsets};
use crate::{
    coordinator::{bootstrap::OFFSETS_TOPIC, unified::GroupCoordinator},
    error::BrokerError,
    txn::marker::{MarkerType, build_marker_batch},
};

/// Append a transaction marker and, on a `__consumer_offsets` partition,
/// resolve the offset commits the transaction wrote against the local group
/// actors: a commit publishes them, an abort discards them, and both drop the
/// KIP-447 pending marks that made them answer `UNSTABLE_OFFSET_COMMIT`.
///
/// Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
/// path must use this function. Keeping marker append and actor resolution in
/// one path prevents local transactions from becoming durable in the log but
/// remaining invisible — or, after an abort, permanently unstable — until the
/// next coordinator replay.
///
/// The log scan runs before the append, because it starts from the producer's
/// first unresolved record and the marker itself ends that transaction.
///
/// A commit needs the group coordinator, because losing it would lose offsets
/// the transaction made durable. An abort publishes nothing, so a caller with
/// no coordinator — and therefore no group actors holding pending marks — has
/// nothing to resolve.
///
/// The function does not wait for the marker to commit. It returns the offset
/// that the high watermark has to reach for that: the offset after the marker
/// it appended. An exact retry of a marker that the log already holds appends
/// nothing, and returns the log end offset, which is past that marker. The
/// marker can still be uncommitted, so the retry waits for it as the first
/// write did.
///
/// The caller holds the transition read guard of the partition, so the leader
/// epoch that the marker carries is the epoch the caller admitted it under.
pub(super) async fn append_marker_and_materialize(
    partition: &crate::partition::Partition,
    group_coordinator: Option<&Arc<GroupCoordinator>>,
    topic: &str,
    marker: MarkerAppend,
) -> Result<Offset, BrokerError> {
    let MarkerAppend {
        producer_id,
        producer_epoch,
        marker_type,
        coordinator_epoch,
        commit_stamp,
        transaction_version,
    } = marker;
    if commit_stamp.is_some() && marker_type != MarkerType::Commit {
        return Err(BrokerError::Txn(
            "a transaction commit stamp cannot be attached to an abort marker".into(),
        ));
    }

    // The guard spans admission, durable append, and offset publication. Two
    // conflicting marker requests therefore cannot both observe the same
    // pending transaction and publish different outcomes.
    let mut materialization = partition.marker_materialization.lock().await;
    if let Some((owed_type, resolved_through, offsets)) = materialization.get(&producer_id).cloned()
    {
        let coordinator = group_coordinator.ok_or_else(|| {
            BrokerError::Txn("cannot retry committed offsets without a group coordinator".into())
        })?;
        resolve_pending_offsets(
            coordinator,
            producer_id,
            owed_type,
            resolved_through,
            offsets,
        )
        .await?;
        materialization.remove(&producer_id);
    }
    let (current_producer_epoch, current_coordinator_epoch, has_pending_transaction) = partition
        .log
        .lock()
        .map_err(|_| BrokerError::Txn("transaction marker log lock poisoned".into()))?
        .transaction_marker_state(producer_id);
    if krabka_verified::transaction::transaction_marker_equal_epoch_fenced(
        transaction_version,
        producer_epoch,
        current_producer_epoch,
        has_pending_transaction,
    ) {
        return Err(BrokerError::ProducerEpochFenced {
            producer_id: producer_id.get(),
            current: current_producer_epoch,
            requested: producer_epoch,
        });
    }
    let decision = krabka_verified::transaction_marker_materialization_decision(
        TransactionMarkerRequest {
            producer_id: producer_id.get(),
            producer_epoch,
            coordinator_epoch,
            is_commit: marker_type == MarkerType::Commit,
            is_offsets_partition: topic == OFFSETS_TOPIC,
        },
        TransactionMarkerPartitionState {
            producer_epoch: current_producer_epoch,
            coordinator_epoch: current_coordinator_epoch,
            has_pending_transaction,
        },
    );
    match decision {
        Decision::RejectMalformed => {
            return Err(BrokerError::Txn(
                "transaction marker contains a malformed producer or coordinator generation".into(),
            ));
        }
        Decision::RejectProducerEpoch => {
            return Err(BrokerError::ProducerEpochFenced {
                producer_id: producer_id.get(),
                current: current_producer_epoch,
                requested: producer_epoch,
            });
        }
        Decision::RejectCoordinatorEpoch => {
            return Err(BrokerError::CoordinatorEpochFenced {
                current: current_coordinator_epoch,
                requested: coordinator_epoch,
            });
        }
        Decision::Retry => return Ok(partition.log_end_offset()),
        Decision::AppendAndPublishOffsets | Decision::AppendWithoutOffsetPublication => {}
    }
    let pending_offsets = if topic == OFFSETS_TOPIC {
        match (marker_type, group_coordinator) {
            (_, Some(coordinator)) => (
                Some(coordinator),
                pending_offset_entries(partition, producer_id)?,
            ),
            (MarkerType::Commit, None) => {
                return Err(BrokerError::Txn(
                    "cannot commit transactional offsets without a group coordinator".into(),
                ));
            }
            (MarkerType::Abort, None) => (None, HashMap::new()),
        }
    } else {
        (None, HashMap::new())
    };

    let mut marker = build_marker_batch(
        producer_id,
        producer_epoch,
        partition.log_end_offset(),
        marker_type,
        coordinator_epoch,
    );
    // The owned produce path stamps this field from the metadata image. A
    // marker does not travel that path, so it stamps its own, and a marker
    // that kept the default of zero would carry a false leader epoch.
    marker.partition_leader_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let append_from = partition.log_end_offset();
    let appended = if let Some(stamp) = commit_stamp {
        partition.produce_commit_marker(marker, stamp).await
    } else {
        // A control batch takes the control append path, which applies no
        // compression rewrite. Kafka never compresses a control batch that
        // arrived uncompressed.
        partition.produce_control_batch(marker).await
    };
    let marker_offset = match appended {
        Ok(offset) => offset,
        Err(error) => {
            // The writer can append the marker and still lose the
            // acknowledgement, for example when it exits right after the
            // append. The coordinator retries the marker, and the retry finds
            // the transaction already ended, so the offsets it wrote can no
            // longer be scanned. Keep the resolution the landed marker owes;
            // the retry drains it before it answers. Kafka's
            // `GroupCoordinator.completeTransaction` writes the marker and
            // completes the offsets in one operation, so a retried marker
            // completes them too.
            if let (Some(_), offsets) = &pending_offsets
                && let Some(landed) =
                    landed_marker_offset(partition, producer_id, producer_epoch, append_from)?
            {
                materialization.insert(producer_id, (marker_type, landed, offsets.clone()));
            }
            return Err(error);
        }
    };

    if let (Some(coordinator), offsets) = pending_offsets {
        // Retain the decoded resolution until the actor acknowledges it. An
        // exact marker retry drains this entry without another append.
        materialization.insert(
            producer_id,
            (marker_type, marker_offset.get(), offsets.clone()),
        );
        // The marker's own log position resolves the KIP-447 marks: it is what
        // tells a group actor that a mark still on its way, for records below
        // it, belongs to the transaction this marker ends.
        resolve_pending_offsets(
            coordinator,
            producer_id,
            marker_type,
            marker_offset.get(),
            offsets,
        )
        .await?;
        materialization.remove(&producer_id);
    }
    Ok(marker_offset + 1)
}

/// The offset of the control batch of `producer_id` at `producer_epoch` that
/// the log holds at or after `from`, if an append whose acknowledgement was
/// lost did land.
fn landed_marker_offset(
    partition: &crate::partition::Partition,
    producer_id: krabka_log::ProducerId,
    producer_epoch: i16,
    from: krabka_log::Offset,
) -> Result<Option<i64>, BrokerError> {
    let log = partition
        .log
        .lock()
        .map_err(|_| BrokerError::Txn("transaction marker log lock poisoned".into()))?;
    let end = log.log_end_offset();
    let mut next = from;
    while next < end {
        let read = log.read(next, krabka_units::mebibytes(1))?;
        if read.batches.is_empty() {
            break;
        }
        for batch in &read.batches {
            if batch.producer_id == producer_id.get()
                && batch.producer_epoch == producer_epoch
                && batch.attributes.is_control_batch()
            {
                return Ok(Some(batch.base_offset));
            }
            next = next.max(krabka_log::Offset(
                batch.base_offset + i64::from(batch.last_offset_delta) + 1,
            ));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarkerAppend {
    pub(crate) producer_id: krabka_log::ProducerId,
    pub(crate) producer_epoch: i16,
    pub(crate) marker_type: MarkerType,
    pub(crate) coordinator_epoch: i32,
    pub(crate) commit_stamp: Option<u64>,
    /// The `transaction.version` the transaction completes under, from the
    /// `WriteTxnMarkers` v2 `TransactionVersion` field. At 2 and above an
    /// equal producer epoch fences the marker.
    pub(crate) transaction_version: i16,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::{
        coordinator::{
            persistence::OffsetCommitValue,
            unified::{actor::test_support::rpc, classic_state::OffsetEntry},
        },
        txn::handlers::write_txn_markers::{
            CommittedOffsets,
            test_support::{
                local_offsets_partition, open_partition, single_transactional_batch, start_broker,
                transactional_offset_batch,
            },
        },
    };

    #[tokio::test]
    async fn aborted_offsets_are_not_published_by_the_offsets_partition_marker() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-abort-group";
        let part = local_offsets_partition(&broker, group_id);
        let producer_id = krabka_log::ProducerId(92);
        part.produce_batch(transactional_offset_batch(
            (producer_id, 5),
            (group_id, "orders", 3),
            &OffsetCommitValue {
                offset: Offset(99),
                leader_epoch: 4,
                metadata: "aborted".into(),
                commit_timestamp_ms: 456,
                expire_timestamp_ms: None,
                topic_id: None,
            },
        ))
        .await
        .expect("append transactional offset");

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            MarkerAppend {
                producer_id,
                producer_epoch: 5,
                marker_type: MarkerType::Abort,
                coordinator_epoch: 0,
                commit_stamp: None,
                transaction_version: 0,
            },
        )
        .await
        .expect("abort marker");

        assert!(broker.group_coordinator.find(group_id).is_none());
        {
            let log = part.log.lock().expect("offsets log lock");
            assert!(log.pending_transaction_start(producer_id).is_none());
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn internal_marker_path_records_supplied_commit_stamp() {
        use krabka_protocol::records::Record;

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(
            &broker,
            dir.path(),
            crate::test_support::StandalonePartitionSetup {
                topic: "stamped-orders",
                ..Default::default()
            },
        );
        let part = broker
            .partitions
            .get("stamped-orders", PartitionIndex(0))
            .expect("local partition");
        part.log
            .lock()
            .expect("partition log")
            .set_stamp_source(Arc::new(krabka_log::MonotonicStampSource::new(1, 1)))
            .expect("install stamp source");

        let producer_id = krabka_log::ProducerId(700);
        part.produce_batch(single_transactional_batch(
            (producer_id, 2),
            Record {
                value: Some(Bytes::from_static(b"event")),
                ..Record::default()
            },
        ))
        .await
        .expect("append transactional data");
        assert!(part.stamp_for_offset(krabka_log::Offset(0)).is_none());

        append_marker_and_materialize(
            &part,
            None,
            "stamped-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 2,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: Some(900),
                transaction_version: 0,
            },
        )
        .await
        .expect("commit marker");

        assert!(part.stamp_for_offset(krabka_log::Offset(0)) == Some(900));
        assert!(part.stamp_for_offset(krabka_log::Offset(1)).is_none());
        broker_handle.shutdown().await;
    }

    /// #876: Kafka's `ProducerAppendInfo.checkProducerEpoch`. At transaction
    /// version 2 and above a marker at the partition's current epoch is
    /// fenced while that producer's transaction is open, and admitted as a
    /// retry once it is not. Below 2 only a lower epoch is fenced.
    #[tokio::test]
    async fn transaction_version_2_fences_an_equal_marker_epoch() {
        use krabka_protocol::records::Record;

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let producer_id = krabka_log::ProducerId(711);
        // (label, transaction version, marker epoch, transaction open,
        //  expected code, whether the marker is appended)
        let cases = [
            (
                "classic, equal epoch, open",
                0,
                4,
                true,
                crate::codes::NONE,
                true,
            ),
            (
                "tv2, equal epoch, open",
                2,
                4,
                true,
                crate::codes::INVALID_PRODUCER_EPOCH,
                false,
            ),
            // The retry of a marker already written: krabka's exact-retry
            // suppression answers it without a second append.
            (
                "tv2, equal epoch, closed",
                2,
                4,
                false,
                crate::codes::NONE,
                false,
            ),
            (
                "tv2, lower epoch, closed",
                2,
                3,
                false,
                crate::codes::INVALID_PRODUCER_EPOCH,
                false,
            ),
            (
                "tv2, higher epoch, open",
                2,
                5,
                true,
                crate::codes::NONE,
                true,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (index, (label, transaction_version, marker_epoch, open, code, appended)) in
            cases.into_iter().enumerate()
        {
            let topic = format!("tv-marker-{index}");
            let part = open_partition(
                &broker,
                dir.path(),
                crate::test_support::StandalonePartitionSetup {
                    topic: &topic,
                    ..Default::default()
                },
            );
            part.produce_batch(single_transactional_batch(
                (producer_id, 4),
                Record {
                    value: Some(Bytes::from_static(b"event")),
                    ..Record::default()
                },
            ))
            .await
            .expect("append transactional data");
            let marker = MarkerAppend {
                producer_id,
                producer_epoch: 4,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: None,
                transaction_version: 0,
            };
            if !open {
                append_marker_and_materialize(&part, None, &topic, marker)
                    .await
                    .expect("close the transaction");
            }
            let before = part.log_end_offset();
            let result = append_marker_and_materialize(
                &part,
                None,
                &topic,
                MarkerAppend {
                    producer_epoch: marker_epoch,
                    transaction_version,
                    ..marker
                },
            )
            .await;
            let answered = result.map_or_else(
                |error| crate::codes::from_broker_error(&error),
                |_| crate::codes::NONE,
            );
            actual.push((label, answered, part.log_end_offset() != before));
            expected.push((label, code, appended));
        }
        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn marker_adapter_fences_generations_and_suppresses_exact_retries() {
        use krabka_protocol::records::Record;

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(
            &broker,
            dir.path(),
            crate::test_support::StandalonePartitionSetup {
                topic: "fenced-orders",
                ..Default::default()
            },
        );
        let part = broker
            .partitions
            .get("fenced-orders", PartitionIndex(0))
            .expect("local partition");
        let producer_id = krabka_log::ProducerId(701);
        let append_data = |epoch| {
            single_transactional_batch(
                (producer_id, epoch),
                Record {
                    value: Some(Bytes::from_static(b"event")),
                    ..Record::default()
                },
            )
        };
        part.produce_batch(append_data(5))
            .await
            .expect("append first transaction");
        let before_marker = part.log_end_offset();

        let malformed = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id: krabka_log::ProducerId(-1),
                producer_epoch: 0,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: None,
                transaction_version: 0,
            },
        )
        .await;
        assert!(matches!(malformed, Err(BrokerError::Txn(_))));

        let stale_producer = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 4,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 10,
                commit_stamp: None,
                transaction_version: 0,
            },
        )
        .await;
        assert!(matches!(
            stale_producer,
            Err(BrokerError::ProducerEpochFenced { .. })
        ));
        assert!(part.log_end_offset() == before_marker);

        let marker = MarkerAppend {
            producer_id,
            producer_epoch: 5,
            marker_type: MarkerType::Commit,
            coordinator_epoch: 10,
            commit_stamp: None,
            transaction_version: 0,
        };
        let conflicting_marker = MarkerAppend {
            marker_type: MarkerType::Abort,
            ..marker
        };
        let (first, second) = tokio::join!(
            append_marker_and_materialize(&part, None, "fenced-orders", marker),
            append_marker_and_materialize(&part, None, "fenced-orders", conflicting_marker),
        );
        first.expect("append current marker");
        second.expect("suppress conflicting completed marker");
        let after_marker = part.log_end_offset();
        assert!(after_marker == before_marker + 1);

        append_marker_and_materialize(&part, None, "fenced-orders", marker)
            .await
            .expect("exact marker retry");
        assert!(part.log_end_offset() == after_marker);

        part.produce_batch(append_data(6))
            .await
            .expect("append next transaction");
        let before_stale_coordinator = part.log_end_offset();
        let stale_coordinator = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 6,
                marker_type: MarkerType::Abort,
                coordinator_epoch: 9,
                commit_stamp: None,
                transaction_version: 0,
            },
        )
        .await;
        assert!(matches!(
            stale_coordinator,
            Err(BrokerError::CoordinatorEpochFenced { .. })
        ));
        assert!(part.log_end_offset() == before_stale_coordinator);
        assert!(
            part.log
                .lock()
                .expect("partition log")
                .pending_transaction_start(producer_id)
                .is_some()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_marker_append_never_publishes_transactional_offsets() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "failed-marker-group";
        let part = local_offsets_partition(&broker, group_id);
        let producer_id = krabka_log::ProducerId(702);
        part.produce_batch(transactional_offset_batch(
            (producer_id, 1),
            (group_id, "orders", 0),
            &OffsetCommitValue {
                offset: Offset(55),
                leader_epoch: 1,
                metadata: "must-stay-hidden".into(),
                commit_timestamp_ms: 1,
                expire_timestamp_ms: None,
                topic_id: None,
            },
        ))
        .await
        .expect("append transactional offset");

        let writer = part.take_writer_handle().expect("partition writer");
        writer.abort();
        let _ = writer.await;
        let result = append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            MarkerAppend {
                producer_id,
                producer_epoch: 1,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: None,
                transaction_version: 0,
            },
        )
        .await;

        assert!(result.is_err());
        assert!(broker.group_coordinator.find(group_id).is_none());
        broker_handle.shutdown().await;
    }

    /// A partition that shares `part`'s log and writer, but whose writer
    /// acknowledgements never arrive: each append lands, and its caller sees
    /// the acknowledgement dropped.
    fn dropping_acks(part: &crate::partition::Partition) -> crate::partition::Partition {
        use crate::partition::{ProduceJob, WriterMessage};

        let (tx, mut rx) = tokio::sync::mpsc::channel::<WriterMessage>(8);
        let writer = part.writer_tx.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let WriterMessage::Produce(job) = message else {
                    continue;
                };
                let ProduceJob {
                    data,
                    ack,
                    producer_check,
                } = job;
                let (landed_tx, landed_rx) = tokio::sync::oneshot::channel();
                let forwarded = writer
                    .send(WriterMessage::Produce(ProduceJob {
                        data,
                        ack: landed_tx,
                        producer_check,
                    }))
                    .await;
                if forwarded.is_ok() {
                    let _ = landed_rx.await;
                }
                drop(ack);
            }
        });
        crate::partition::Partition {
            writer_tx: tx,
            ..part.clone()
        }
    }

    /// #976: a commit marker on `__consumer_offsets` whose append landed but
    /// whose acknowledgement was lost still publishes the transaction's
    /// offsets when the coordinator retries it. Kafka's
    /// `GroupCoordinator.completeTransaction` completes the offsets together
    /// with the marker, so the retry answers `NONE` with the offsets
    /// visible, and the log holds one marker.
    #[tokio::test]
    async fn a_retried_commit_marker_publishes_offsets_after_a_lost_ack() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "lost-ack-group";
        let part = local_offsets_partition(&broker, group_id);
        let producer_id = krabka_log::ProducerId(704);
        part.produce_batch(transactional_offset_batch(
            (producer_id, 1),
            (group_id, "orders", 0),
            &OffsetCommitValue {
                offset: Offset(55),
                leader_epoch: 1,
                metadata: "committed".into(),
                commit_timestamp_ms: 1,
                expire_timestamp_ms: None,
                topic_id: None,
            },
        ))
        .await
        .expect("append transactional offset");
        let marker = MarkerAppend {
            producer_id,
            producer_epoch: 1,
            marker_type: MarkerType::Commit,
            coordinator_epoch: 0,
            commit_stamp: None,
            transaction_version: 0,
        };

        let lost = append_marker_and_materialize(
            &dropping_acks(&part),
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await;
        assert!(lost.is_err());
        let after_marker = part.log_end_offset();

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await
        .expect("retried marker");
        assert!(part.log_end_offset() == after_marker);

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let committed = rpc::fetch_offsets(&handle).await;
        assert!(
            committed
                .committed
                .get(&("orders".into(), 0))
                .map(|entry| (entry.offset, entry.metadata.as_str()))
                == Some((Offset(55), "committed"))
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn exact_marker_retry_drains_retained_offset_publication() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "retained-marker-group";
        let part = local_offsets_partition(&broker, group_id);
        let producer_id = krabka_log::ProducerId(703);
        let marker = MarkerAppend {
            producer_id,
            producer_epoch: 1,
            marker_type: MarkerType::Commit,
            coordinator_epoch: 4,
            commit_stamp: None,
            transaction_version: 0,
        };
        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await
        .expect("append marker generation");
        let after_marker = part.log_end_offset();
        let marker_count = || {
            part.log
                .lock()
                .expect("offsets log")
                .read(Offset(0), krabka_units::mebibytes(1))
                .expect("read offsets log")
                .batches
                .into_iter()
                .filter(|batch| {
                    batch.producer_id == producer_id.get() && batch.attributes.is_control_batch()
                })
                .count()
        };
        let markers_before_retry = marker_count();

        let mut retained = CommittedOffsets::new();
        retained.insert(
            group_id.into(),
            vec![(
                ("orders".into(), 3),
                OffsetEntry {
                    offset: Offset(88),
                    leader_epoch: 2,
                    metadata: "retained".into(),
                    commit_timestamp_ms: i64::MAX,
                    expire_timestamp_ms: None,
                    topic_id: None,
                },
            )],
        );
        part.marker_materialization.lock().await.insert(
            producer_id,
            (MarkerType::Commit, after_marker.get() - 1, retained),
        );

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await
        .expect("retry retained publication");
        assert!(marker_count() == markers_before_retry);
        assert!(
            !part
                .marker_materialization
                .lock()
                .await
                .contains_key(&producer_id)
        );

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let committed = rpc::fetch_offsets(&handle).await;
        assert!(
            committed
                .committed
                .get(&("orders".into(), 3))
                .is_some_and(|entry| { entry.offset == 88 && entry.metadata == "retained" })
        );
        broker_handle.shutdown().await;
    }
}
