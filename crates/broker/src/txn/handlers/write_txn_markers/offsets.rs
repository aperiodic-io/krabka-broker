//! The `__consumer_offsets` half of a transaction marker: reading back the
//! offset commits the transaction wrote, and resolving them against the local
//! group actors.
//!
//! A transactional offset commit is invisible to a consumer until the
//! transaction commits, so the records sit in the offsets log with no offset
//! behind them in the actor — only KIP-447's pending mark, which makes a
//! `require_stable` `OffsetFetch` answer `UNSTABLE_OFFSET_COMMIT`. When a
//! marker lands, this module rescans the log from the transaction's first
//! record and decodes every `OffsetCommit` key the producer wrote. A commit
//! publishes those offsets and drops the marks; an abort drops the marks and
//! publishes nothing. Either way the group stops reporting the partitions as
//! unstable.

use std::{collections::HashMap, sync::Arc};

use crate::{
    coordinator::{
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            GroupCoordinator,
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::OffsetEntry,
        },
    },
    error::BrokerError,
    txn::marker::MarkerType,
};

pub(crate) type CommittedOffsets = HashMap<String, Vec<((String, i32), OffsetEntry)>>;

pub(super) fn pending_offset_entries(
    partition: &crate::partition::Partition,
    producer_id: krabka_log::ProducerId,
) -> Result<CommittedOffsets, BrokerError> {
    let log = partition.log.lock().map_err(|_| {
        BrokerError::Replication("offsets log lock poisoned while applying txn marker".into())
    })?;
    scan_pending_offset_entries(&log, producer_id)
}

/// Reads the offset commits of `producer_id`'s open transaction from `log`,
/// without the offsets that a later plain tombstone deleted.
fn scan_pending_offset_entries(
    log: &krabka_log::Log,
    producer_id: krabka_log::ProducerId,
) -> Result<CommittedOffsets, BrokerError> {
    let Some(mut next) = log.pending_transaction_start(producer_id) else {
        return Ok(HashMap::new());
    };
    let end = log.log_end_offset();
    let mut offsets: CommittedOffsets = HashMap::new();
    while next < end {
        let read = log.read(next, krabka_units::mebibytes(1))?;
        if read.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &read.batches {
            if !batch.attributes.is_transactional() && !batch.attributes.is_control_batch() {
                drop_tombstoned_entries(&mut offsets, batch)?;
            }
            if batch.producer_id == producer_id.get()
                && batch.attributes.is_transactional()
                && !batch.attributes.is_control_batch()
            {
                for record in &batch.records {
                    let (Some(key), Some(value)) = (&record.key, &record.value) else {
                        continue;
                    };
                    if let Key::OffsetCommit {
                        group_id,
                        topic,
                        partition,
                    } = parse_key(key)?
                    {
                        let value = OffsetCommitValue::decode_value(value)?;
                        offsets
                            .entry(group_id)
                            .or_default()
                            .push(((topic, partition), OffsetEntry::from(value)));
                    }
                }
            }
            advanced_to = checked_batch_advance(
                advanced_to,
                end,
                batch.base_offset,
                batch.last_offset_delta,
            )?;
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(offsets)
}

/// Removes the entries that a plain `OffsetCommit` tombstone in `batch`
/// deletes. Kafka's `OffsetMetadataManager.replay` drops a key from every open
/// transaction when it replays such a tombstone, so the marker does not publish
/// it. The group keeps its (maybe empty) entry, so the marker still clears the
/// producer's pending marks on a live group. `DeleteGroups`, `OffsetDelete` and offset retention write these
/// tombstones.
fn drop_tombstoned_entries(
    offsets: &mut CommittedOffsets,
    batch: &krabka_protocol::records::RecordBatch,
) -> Result<(), BrokerError> {
    for record in &batch.records {
        let (Some(key), None) = (&record.key, &record.value) else {
            continue;
        };
        if let Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } = parse_key(key)?
            && let Some(entries) = offsets.get_mut(&group_id)
        {
            entries.retain(|((entry_topic, entry_partition), _)| {
                *entry_topic != topic || *entry_partition != partition
            });
        }
    }
    Ok(())
}

fn checked_batch_advance(
    cursor: krabka_log::Offset,
    end: krabka_log::Offset,
    base_offset: i64,
    last_offset_delta: i32,
) -> Result<krabka_log::Offset, BrokerError> {
    match krabka_verified::replay_batch_cursor_decision(
        cursor.0,
        end.0,
        Some((base_offset, last_offset_delta)),
    ) {
        krabka_verified::ReplayCursorDecision::Advance(next) => Ok(krabka_log::Offset(next)),
        krabka_verified::ReplayCursorDecision::Stop => Err(BrokerError::Txn(format!(
            "transactional offset scan rejected batch {base_offset}+{last_offset_delta} at cursor {cursor} before {end}"
        ))),
    }
}

/// Resolve a transaction's offset commits against the local group actors.
///
/// A commit publishes the offsets and clears the producer's KIP-447 pending
/// marks in one actor turn, so no fetch can observe a partition that is
/// neither unstable nor updated. An abort clears the marks and publishes
/// nothing, leaving the group's stable offsets where they were.
///
/// A commit uses the get-or-create factory, which detects and replaces a
/// closed actor: the offsets have to become visible even if the actor failed
/// since the transaction began, without waiting for a marker retry the durable
/// commit marker will never trigger. An abort has nothing to publish, so it
/// only touches a live actor that already exists: it never resurrects a group
/// that went away with its marks, and it never fails the marker over one. The
/// marker is already durable by the time this runs, and an actor that is gone
/// took its pending marks with it, so a failed hand-off leaves nothing
/// unresolved -- only a commit, whose offsets would otherwise be lost from
/// memory, still reports the failure.
///
/// One transaction can hold offset commits for several groups on the same
/// offsets partition, and every one of them has to be resolved. The marker is
/// durable before this runs and it ended the log's pending transaction, so a
/// retry would find nothing to rescan: a group skipped here keeps its pending
/// marks for ever. The loop therefore visits every group and reports a
/// failure only once it has, rather than returning at the first one.
///
/// `resolved_through` is the marker's own offset in the offsets log. It goes
/// to the actor with the resolution because a `TxnOffsetCommit` marks its keys
/// after its append is durable, so a mark for this very transaction can still
/// be in flight; the actor compares the two log positions and drops it.
pub(super) async fn resolve_pending_offsets(
    coordinator: &Arc<GroupCoordinator>,
    producer_id: krabka_log::ProducerId,
    marker_type: MarkerType,
    resolved_through: i64,
    offsets: CommittedOffsets,
) -> Result<(), BrokerError> {
    let commit = marker_type == MarkerType::Commit;
    let mut unresolved: Vec<String> = Vec::new();
    for (group_id, entries) in offsets {
        // A commit with nothing left to publish must not create a group: a
        // tombstone removed every offset, and `DeleteGroups` may have
        // removed the group too.
        let handle = if commit && !entries.is_empty() {
            Some(coordinator.get_or_create_group(&group_id, GroupKindTag::Classic))
        } else {
            coordinator.find(&group_id).filter(|h| !h.tx.is_closed())
        };
        let Some(handle) = handle else {
            continue;
        };
        let (reply, response) = tokio::sync::oneshot::channel();
        let resolved = handle
            .tx
            .send(GroupActorMessage::ResolveTxnOffsets {
                producer_id: producer_id.get(),
                resolved_through,
                committed: if commit { entries } else { Vec::new() },
                reply,
            })
            .await
            .is_ok()
            && response.await.is_ok();
        if !resolved {
            tracing::warn!(
                group = %group_id,
                "WriteTxnMarkers: could not resolve the transaction's offset commits"
            );
            unresolved.push(group_id);
        }
    }
    if commit && !unresolved.is_empty() {
        return Err(BrokerError::Txn(format!(
            "could not resolve transactional offset commits for groups {}",
            unresolved.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::assert;
    use krabka_log::{Offset, ProducerId};
    use krabka_protocol::records::{Record, RecordBatch};

    use super::{CommittedOffsets, checked_batch_advance, scan_pending_offset_entries};
    use crate::coordinator::{
        persistence::OffsetCommitValue,
        test_support::{offset_entry as entry, replay_offset_batch},
    };

    fn commit(group: &str, topic: &str, partition: i32, offset: i64) -> Record {
        crate::coordinator::test_support::offset_record(
            crate::coordinator::test_support::OffsetRecordSetup {
                group,
                topic,
                partition: krabka_ids::PartitionIndex(partition),
                offset: krabka_log::Offset(offset),
            },
        )
    }

    fn tombstone(group: &str, topic: &str, partition: i32) -> Record {
        Record {
            key: Some(OffsetCommitValue::encode_key(group, topic, partition).unwrap()),
            value: None,
            ..Default::default()
        }
    }

    fn batch(transactional: bool, records: Vec<Record>) -> RecordBatch {
        replay_offset_batch(transactional.then_some(7), records)
    }

    /// Kafka's `OffsetMetadataManager.replay` removes a key from every open
    /// transaction when it replays a plain tombstone for it. The marker must
    /// then not publish that key: `DeleteGroups` tombstones a group's pending
    /// transactional offsets, and a commit marker must not bring them back.
    #[test]
    fn a_plain_tombstone_after_the_transaction_drops_its_offset() {
        let rows: [(&str, Vec<Record>, CommittedOffsets); 3] = [
            (
                "no tombstone",
                vec![],
                HashMap::from([
                    (
                        "g".to_string(),
                        vec![
                            (("orders".to_string(), 0), entry(10)),
                            (("orders".to_string(), 1), entry(11)),
                        ],
                    ),
                    ("h".to_string(), vec![(("t".to_string(), 0), entry(20))]),
                ]),
            ),
            (
                "one key of one group",
                vec![tombstone("g", "orders", 0)],
                HashMap::from([
                    (
                        "g".to_string(),
                        vec![(("orders".to_string(), 1), entry(11))],
                    ),
                    ("h".to_string(), vec![(("t".to_string(), 0), entry(20))]),
                ]),
            ),
            (
                "every key of a deleted group",
                vec![tombstone("g", "orders", 0), tombstone("g", "orders", 1)],
                HashMap::from([
                    ("g".to_string(), vec![]),
                    ("h".to_string(), vec![(("t".to_string(), 0), entry(20))]),
                ]),
            ),
        ];
        for (name, tombstones, expected) in rows {
            let dir = tempfile::tempdir().unwrap();
            let mut log =
                krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
            log.append(&mut batch(
                true,
                vec![
                    commit("g", "orders", 0, 10),
                    commit("g", "orders", 1, 11),
                    commit("h", "t", 0, 20),
                ],
            ))
            .unwrap();
            if !tombstones.is_empty() {
                log.append(&mut batch(false, tombstones)).unwrap();
            }
            let scanned = scan_pending_offset_entries(&log, ProducerId(7)).unwrap();
            assert!(scanned == expected, "{name}");
        }
    }

    #[test]
    fn transactional_offset_scan_rejects_overlap_bounds_and_overflow() {
        assert!(checked_batch_advance(Offset(5), Offset(10), 5, 1).ok() == Some(Offset(7)));
        assert!(checked_batch_advance(Offset(5), Offset(10), 4, 1).is_err());
        assert!(checked_batch_advance(Offset(5), Offset(10), 9, 1).is_err());
        assert!(
            checked_batch_advance(Offset(i64::MAX - 1), Offset(i64::MAX), i64::MAX - 1, 1).is_err()
        );
        assert!(checked_batch_advance(Offset(5), Offset(10), 5, -1).is_err());
    }
}
