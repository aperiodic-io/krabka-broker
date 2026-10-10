//! Last-stable-offset bookkeeping and the effect of one transaction end
//! marker on the in-memory transaction state.
//!
//! An open transaction holds the LSO at its first offset, so every append
//! that is not transactional data moves the LSO through the one helper
//! here and a commit or abort marker closes the transaction through the
//! other.

use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::RecordBatch;

use super::{
    Log,
    control::{batch_control_marker_coordinator_epoch, transaction_marker_flags},
};
use crate::{error::LogError, txn_index::AbortedTxn};

impl Log {
    /// Recompute the first unstable offset from every open transaction and
    /// every complete transaction whose marker is not below the high
    /// watermark yet.
    ///
    /// The earliest of those transactions holds the offset at its first
    /// offset. With none, the offset advances to the exact log end. Every
    /// append path and recovery use this same selection.
    pub(super) fn refresh_lso(&mut self) -> Result<(), LogError> {
        let starts: Vec<_> = self
            .pending
            .values()
            .chain(self.unreplicated.keys().next())
            .map(|offset| offset.0)
            .collect();
        let log_end = self.log_end_offset();
        self.lso = krabka_verified::first_unstable_offset(&starts, log_end.0)
            .map(Offset)
            .ok_or_else(|| {
                LogError::Corrupt(format!(
                    "pending transaction starts beyond log end {log_end}"
                ))
            })?;
        Ok(())
    }

    /// The last stable offset that a reader sees at `high_watermark`.
    ///
    /// The call first releases every complete transaction whose marker offset
    /// is below `high_watermark`, as Kafka's
    /// `ProducerStateManager.onHighWatermarkUpdated` does. It then answers
    /// `min(first unstable offset, high_watermark)`, which is Kafka's
    /// `UnifiedLog.lastStableOffset`. A transaction whose marker the high
    /// watermark has not passed still holds the answer at its first offset,
    /// because a leader change can truncate that marker away.
    ///
    /// Call it with the partition's current high watermark. The high watermark
    /// only moves forward, so a released transaction never has to come back.
    pub fn last_stable_offset(&mut self, high_watermark: Offset) -> Offset {
        self.release_replicated_transactions(high_watermark);
        self.lso.min(high_watermark)
    }

    /// Release every complete transaction whose marker offset is below
    /// `high_watermark`, and move the first unstable offset forward.
    ///
    /// The writer calls this before it appends a transaction marker, so the
    /// set of complete transactions stays bounded on a partition that no
    /// reader fetches from.
    pub fn release_replicated_transactions(&mut self, high_watermark: Offset) {
        let before = self.unreplicated.len();
        self.unreplicated
            .retain(|_, marker_offset| *marker_offset >= high_watermark);
        if self.unreplicated.len() != before
            && let Err(error) = self.refresh_lso()
        {
            // Releasing a transaction removes a start offset and cannot put a
            // start beyond the log end. Keep the lower offset if it happens.
            tracing::warn!(%error, "last stable offset refresh failed");
        }
    }

    /// Apply one transaction end marker to the in-memory transaction state.
    ///
    /// The marker closes `producer_id`'s open transaction on this partition.
    /// An ABORT marker appends the transaction's offset range to the active
    /// `.txnindex`. A COMMIT marker stamps the transaction's data ranges with
    /// `transaction_stamp`, or with the next stamp from the installed source.
    ///
    /// This method never runs for a barrier marker.
    pub(super) fn apply_transaction_marker(
        &mut self,
        batch: &RecordBatch,
        producer_id: ProducerId,
        last_offset: Offset,
        transaction_stamp: Option<u64>,
    ) -> Result<(), LogError> {
        // Read the inner control record: key = (version: i16, type: i16) BE.
        let (is_abort, is_commit) = transaction_marker_flags(batch)?;
        let closes = krabka_verified::transaction_marker_closes(
            is_abort,
            is_commit,
            self.pending.contains_key(&producer_id),
        );
        if (is_abort || is_commit)
            && producer_id.get() >= 0
            && let Some(epoch) = batch_control_marker_coordinator_epoch(batch)?
        {
            self.coordinator_epochs.insert(producer_id, epoch);
        }
        if closes && is_abort {
            let pending_start = self.pending.get(&producer_id).map(|offset| offset.0);
            let Some((start, last)) = krabka_verified::aborted_transaction_interval(
                pending_start,
                last_offset.0,
                producer_id.get(),
            ) else {
                return Err(LogError::Corrupt(format!(
                    "invalid aborted transaction interval for producer {producer_id}"
                )));
            };
            // Kafka's `ProducerStateManager.lastStableOffset(completedTxn)`:
            // the first offset of another still-open transaction, or of an
            // earlier completed transaction whose marker has not yet crossed
            // the high watermark (still in `self.unreplicated`, exactly as
            // `refresh_lso` also chains in), or one past this transaction's
            // own last offset when none of those remain.
            let other_starts: Vec<i64> = self
                .pending
                .iter()
                .filter(|(other_pid, _)| **other_pid != producer_id)
                .map(|(_, offset)| offset.0)
                .chain(self.unreplicated.keys().next().map(|offset| offset.0))
                .collect();
            let last_stable_offset = krabka_verified::first_unstable_offset(
                &other_starts,
                last_offset.0 + 1,
            )
            .map(Offset)
            .ok_or_else(|| {
                LogError::Corrupt(format!(
                    "pending transaction starts beyond aborted marker offset for producer {producer_id}"
                ))
            })?;
            self.active_txn_index.append(AbortedTxn {
                start_offset: Offset(start),
                last_offset: Offset(last),
                producer_id,
                last_stable_offset,
            })?;
        }
        let stamp_ranges = self
            .pending_stamp_ranges
            .get(&producer_id)
            .cloned()
            .unwrap_or_default();
        if closes && is_commit && !stamp_ranges.is_empty() {
            let stamp = transaction_stamp
                .or_else(|| self.stamp_source.as_ref().map(|source| source.next_stamp()));
            if let Some(stamp) = stamp {
                for (base, last) in stamp_ranges {
                    self.record_stamp_value(base, last, stamp)?;
                }
            }
        }
        // Keep the in-memory transaction state until all durable sidecar
        // writes succeed. A caller can then retry a marker whose log append
        // succeeded but whose index update failed.
        if closes {
            if let Some(first_offset) = self.pending.remove(&producer_id) {
                self.unreplicated.insert(first_offset, last_offset);
            }
            self.pending_stamp_ranges.remove(&producer_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_ids::LeaderEpoch;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        log::test_support::{
            abort_marker, append_transaction, commit_marker, sample_batch, test_batch_at, test_log,
            tiny_segments, transaction_entries, transaction_fields, transactional_batch,
            verbatim_from,
        },
        name,
    };

    // ---- transactional LSO / txnindex tests ----

    #[test]
    fn transactional_batch_holds_lso() {
        let (_dir, mut log) = test_log();
        // First, a non-txn batch — LSO advances past it.
        let mut b0 = sample_batch(1);
        log.append(&mut b0).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());

        // Now an in-flight txn batch — LSO stays.
        let mut b1 = transactional_batch(1000, 0, &["a", "b"]); // pid=1000 epoch=0
        let old_lso = log.lso();
        log.append(&mut b1).unwrap();
        assert2::assert!(log.lso() == old_lso);

        // Commit marker: the LSO catches up once the high watermark passes it.
        let mut commit = commit_marker(1000, 0);
        log.append(&mut commit).unwrap();
        let log_end = log.log_end_offset();
        assert2::assert!(log.last_stable_offset(log_end) == log_end);
    }

    #[test]
    fn negative_producer_ids_never_create_transaction_state() {
        let dir = tempdir().unwrap();
        {
            let mut log = crate::test_support::open_log(dir.path());
            crate::log::test_support::install_stamps(&mut log, 40, 1);

            let mut owned = sample_batch(1);
            owned.producer_id = -2;
            owned.producer_epoch = 0;
            owned.base_sequence = 0;
            owned.attributes = owned.attributes.with_transactional(true);
            log.append(&mut owned).unwrap();
            assert2::assert!(log.lso() == Offset(1));
            assert2::assert!(log.stamp_for_offset(Offset(0)) == Some(40));

            let mut marker = commit_marker(-2, 0);
            log.append(&mut marker).unwrap();
            assert2::assert!(transaction_fields(&log, ProducerId(-2)) == (-1, None));

            let mut verbatim = test_batch_at(0);
            verbatim.producer_id = -3;
            verbatim.producer_epoch = 0;
            verbatim.base_sequence = 0;
            verbatim.attributes = verbatim.attributes.with_transactional(true);
            let (_wire, batch) = verbatim_from(&verbatim, LeaderEpoch(0));
            log.append_verbatim(&batch).unwrap();
            assert2::assert!(log.lso() == Offset(3));
            assert2::assert!(log.stamp_for_offset(Offset(2)) == Some(41));
            assert2::assert!(log.producer_state_snapshot().is_empty());
        }

        let reopened = crate::test_support::open_log(dir.path());
        assert2::assert!(reopened.lso() == reopened.log_end_offset());
        assert2::assert!(transaction_fields(&reopened, ProducerId(-2)) == (-1, None));
        assert2::assert!(transaction_fields(&reopened, ProducerId(-3)) == (-1, None));
        assert2::assert!(reopened.producer_state_snapshot().is_empty());
    }

    #[test]
    fn abort_marker_writes_txnindex_entry() {
        let (dir, mut log) = test_log();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap();

        let mut a = abort_marker(1000, 0);
        log.append(&mut a).unwrap();

        let entries = transaction_entries(dir.path(), "00000000000000000000.txnindex");
        // Txn batch was the first append: start_offset = 0.
        // last_offset = abort marker's base_offset + last_offset_delta = 3 + 0 = 3.
        // (The 3-record txn batch occupies offsets 0-2; the marker lands at offset 3.)
        assert2::assert!(
            entries
                == [crate::test_support::aborted_txn(
                    crate::test_support::AbortedTxnSetup::default()
                )]
        );
    }

    /// An earlier completed transaction, still held in `unreplicated`
    /// because the high watermark has not passed its marker yet, holds the
    /// last-stable-offset written for a later transaction's abort marker --
    /// the same way it holds `refresh_lso`'s in-memory `lso`. Before this
    /// fix, `other_starts` only chained `self.pending` (other still-open
    /// transactions), so this scenario wrote `last_offset + 1` into the
    /// `.txnindex` entry instead of the earlier unreplicated start.
    #[test]
    fn abort_marker_lso_is_held_by_an_earlier_unreplicated_transaction() {
        let (dir, mut log) = test_log();

        // Producer 1000 commits first: its marker lands at offset 2, and it
        // stays in `unreplicated` (this test never advances the high
        // watermark past it).
        let mut t1 = transactional_batch(1000, 0, &["a", "b"]);
        log.append(&mut t1).unwrap();
        log.append(&mut commit_marker(1000, 0)).unwrap();

        // Producer 2000 opens after that and then aborts.
        let mut t2 = transactional_batch(2000, 0, &["c"]);
        log.append(&mut t2).unwrap();
        log.append(&mut abort_marker(2000, 0)).unwrap();

        let entries = transaction_entries(dir.path(), "00000000000000000000.txnindex");
        // Producer 1000's transaction started at offset 0: that is the
        // earlier unreplicated start, and it must be the recorded LSO, not
        // producer 2000's own last_offset + 1.
        assert2::assert!(
            entries
                == [crate::test_support::aborted_txn(
                    crate::test_support::AbortedTxnSetup {
                        producer: crate::ProducerId(2000),
                        bounds: crate::Offset(3)..=crate::Offset(4),
                        stable: crate::Offset(0)
                    }
                )]
        );
    }

    #[test]
    fn aborted_transaction_uses_cached_marker_segment_beyond_range_end() {
        let (dir, mut log) = test_log();
        let mut transaction = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut transaction).unwrap();

        log.set_config(tiny_segments());
        let (marker_base, _) = log.append(&mut abort_marker(1000, 0)).unwrap();
        log.append(&mut sample_batch(1)).unwrap();
        std::fs::remove_file(name::txnindex_path(dir.path(), marker_base.0)).unwrap();

        assert2::assert!(
            log.aborted_in_range(Offset(0), marker_base)
                == [crate::test_support::aborted_txn(
                    crate::test_support::AbortedTxnSetup::default()
                )]
        );
    }

    // The aborted-txn `last_offset` is `marker.base_offset +
    // marker.last_offset_delta`. Using a marker that spans TWO offsets
    // (`last_offset_delta = 1`) pins the `+`: the txn batch occupies offsets
    // 0..=2, the abort marker lands at base_offset 3 with delta 1, so the
    // recorded `last_offset` is `3 + 1 = 4`. Mutating `+`→`-` would record 2.
    #[test]
    fn abort_marker_last_offset_uses_base_plus_delta() {
        let (dir, mut log) = test_log();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap(); // offsets 0..=2

        // Abort marker spanning two offsets (delta 1): base 3, last 4.
        let mut a = abort_marker(1000, 0);
        a.last_offset_delta = 1;
        log.append(&mut a).unwrap();

        let entries = transaction_entries(dir.path(), "00000000000000000000.txnindex");
        assert2::assert!(
            entries
                == [AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(4), // 3 + 1, not 3 - 1
                    producer_id: ProducerId(1000),
                    last_stable_offset: Offset(5),
                }]
        );
    }

    // LSO tracking (owned path) keys on `is_transactional() && !pid.is_none()`.
    // A NON-transactional batch that carries a valid producer_id (idempotent
    // producer, pid >= 0) must NOT be treated as an open txn: LSO advances to
    // log_end. Mutating `&&`→`||` would hold LSO at the batch base (0).
    #[test]
    fn non_txn_batch_with_valid_pid_advances_lso() {
        let (_dir, mut log) = test_log();
        // Idempotent (not transactional) producer: pid >= 0, no transactional
        // attribute bit set.
        let mut b = sample_batch(2);
        b.producer_id = 55;
        b.producer_epoch = 0;
        assert2::assert!(!b.attributes.is_transactional());
        log.append(&mut b).unwrap();
        // Not an open txn → LSO advances to log_end (2), not held at 0.
        assert2::assert!(log.lso() == Offset(2));
        assert2::assert!(log.log_end_offset() == Offset(2));
    }

    #[test]
    fn lso_held_by_remaining_producer_after_partial_commit() {
        let (_dir, mut log) = test_log();

        // Open two producers' transactions in parallel.
        let mut t1 = transactional_batch(1000, 0, &["a", "b"]);
        log.append(&mut t1).unwrap();
        let mut t2 = transactional_batch(2000, 0, &["c"]);
        log.append(&mut t2).unwrap();
        let lso_after_open = log.lso();

        // Commit producer 1000. LSO must still be held back by 2000.
        let mut c1 = commit_marker(1000, 0);
        log.append(&mut c1).unwrap();
        let log_end = log.log_end_offset();
        assert2::assert!(log.last_stable_offset(log_end) == Offset(2));
        assert2::assert!(log.lso() > lso_after_open);

        // Commit producer 2000. LSO advances to log_end_offset.
        let mut c2 = commit_marker(2000, 0);
        log.append(&mut c2).unwrap();
        let log_end = log.log_end_offset();
        assert2::assert!(log.last_stable_offset(log_end) == log_end);
    }

    #[test]
    fn only_a_valid_marker_for_the_pending_producer_closes_it() {
        for case in ["malformed", "different-producer"] {
            let dir = tempdir().unwrap();
            {
                let mut log = crate::test_support::open_log(dir.path());
                append_transaction(&mut log, (1000, 0), &["a", "b"]);
                let held = log.lso();
                let mut marker = if case == "malformed" {
                    let mut marker = commit_marker(1000, 0);
                    marker.records[0].key = Some(Bytes::from_static(&[0, 0, 0]));
                    marker
                } else {
                    commit_marker(2000, 0)
                };
                // Kafka's `ControlRecordType.parseTypeId` refuses a key too
                // short to hold its type, so the malformed marker is never
                // written.
                let refused = log.append(&mut marker).err().map(|e| e.to_string());
                let want = (case == "malformed").then(|| {
                    "Invalid value size found for end control record key. Must have at least 4 \
                     bytes, but found only 3"
                        .to_owned()
                });
                assert2::assert!(refused == want, "case {case}");
                assert2::assert!(log.lso() == held, "case {case}");
                assert2::assert!(
                    log.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)),
                    "case {case}"
                );
            }
            let reopened = crate::test_support::open_log(dir.path());
            assert2::assert!(reopened.lso() == Offset(0), "case {case}");
            assert2::assert!(
                reopened.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)),
                "case {case}"
            );
        }
    }

    /// Kafka keeps a complete transaction in the first unstable offset until
    /// the high watermark passes its marker (`ProducerStateManager
    /// .removeUnreplicatedTransactions` drops it only when
    /// `lastOffset < highWatermark`), and reports
    /// `min(first unstable offset, high watermark)`.
    #[test]
    fn a_complete_transaction_holds_the_lso_until_the_high_watermark_passes_its_marker() {
        struct Case {
            name: &'static str,
            transaction: bool,
            high_watermarks: &'static [i64],
            want: Offset,
        }
        let cases = [
            Case {
                name: "high watermark inside the transaction",
                transaction: true,
                high_watermarks: &[12],
                want: Offset(10),
            },
            Case {
                name: "high watermark at the marker",
                transaction: true,
                high_watermarks: &[15],
                want: Offset(10),
            },
            Case {
                name: "high watermark past the marker",
                transaction: true,
                high_watermarks: &[16],
                want: Offset(16),
            },
            Case {
                name: "high watermark moves past the marker in steps",
                transaction: true,
                high_watermarks: &[12, 15, 16],
                want: Offset(16),
            },
            Case {
                name: "no transaction",
                transaction: false,
                high_watermarks: &[16],
                want: Offset(16),
            },
        ];
        for case in cases {
            let (_dir, mut log) = test_log();
            log.append(&mut sample_batch(10)).unwrap(); // offsets 0 to 9
            if case.transaction {
                log.append(&mut transactional_batch(
                    1000,
                    0,
                    &["a", "b", "c", "d", "e"],
                ))
                .unwrap(); // offsets 10 to 14
                log.append(&mut commit_marker(1000, 0)).unwrap(); // offset 15
            } else {
                log.append(&mut sample_batch(6)).unwrap(); // offsets 10 to 15
            }
            let mut got = Offset(-1);
            for &high_watermark in case.high_watermarks {
                got = log.last_stable_offset(Offset(high_watermark));
            }
            assert2::assert!(got == case.want, "case {}", case.name);
        }
    }

    /// A reopened log replays the markers after its last producer snapshot,
    /// so a complete transaction still holds the LSO until the high watermark
    /// passes its marker, as Kafka's log recovery puts it in
    /// `unreplicatedTxns`. A released transaction never holds it again.
    #[test]
    fn a_reopened_log_holds_a_complete_transaction_until_the_high_watermark_passes() {
        let dir = tempdir().unwrap();
        {
            let mut log = crate::test_support::open_log(dir.path());
            log.append(&mut sample_batch(10)).unwrap(); // offsets 0 to 9
            append_transaction(&mut log, (1000, 0), &["a", "b"]); // offsets 10 and 11
            log.append(&mut abort_marker(1000, 0)).unwrap(); // offset 12
        }
        let mut reopened = crate::test_support::open_log(dir.path());
        assert2::assert!(
            [12, 13, 11].map(|hw| reopened.last_stable_offset(Offset(hw)))
                == [Offset(10), Offset(13), Offset(11)]
        );
    }

    #[test]
    fn stale_pending_start_beyond_log_end_is_rejected() {
        let (_dir, mut log) = test_log();
        log.pending.insert(ProducerId(1000), Offset(1));
        let error = log.refresh_lso().unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = error);
        assert2::assert!(log.lso() == Offset(0));
    }
}
