//! The transaction state a compaction pass carries while it walks the log.
//!
//! This is Kafka's `CleanedTransactionMetadata`. It tracks the aborted and
//! committed transactions the cleaner has met so far, so the pass can tell
//! which data batches belong to an aborted transaction, which markers may age
//! out, and which aborted-transaction entries the rewritten `.txnindex` has to
//! carry. Both passes drive it in offset order, the offset-map pass and the
//! rewrite pass, each with an instance of its own.

use {
    crate::{
        error::LogError,
        log::control::{ABORT_CONTROL_TYPE, COMMIT_CONTROL_TYPE, parse_control_marker_type},
        txn_index::AbortedTxn,
    },
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::RecordBatch,
    std::collections::{HashMap, HashSet},
};

/// An aborted transaction the pass has reached, and the last batch of it the
/// pass has read.
#[derive(Debug, Clone, Copy)]
struct AbortedTransactionState {
    txn: AbortedTxn,
    /// The last offset of the newest data batch of this transaction that the
    /// pass has read. `None` while the pass has seen no batch of it.
    last_observed_batch_offset: Option<Offset>,
}

/// Which transactions the pass has met, per producer.
///
/// The rules are Kafka's:
///
/// - A data batch of an aborted transaction is discardable, and it keeps the
///   transaction's abort marker alive.
/// - An abort marker survives only while the pass has read a batch of its
///   transaction. The surviving marker's aborted-transaction entry goes to
///   the cleaned index.
/// - A commit marker survives only while the pass has read a non-aborted
///   transactional batch of its producer since the previous commit marker.
///
/// The pass reads a batch before it filters the batch's records, so a
/// transaction whose records have all been superseded still holds its marker
/// for one more pass. The marker ages out on the pass after that, through the
/// delete horizon.
#[derive(Debug, Default)]
pub struct CleanedTransactionMetadata {
    /// Producers with a committed-or-open transactional data batch read since
    /// their last commit marker.
    ongoing_committed: HashSet<ProducerId>,
    /// Aborted transactions the pass has reached and not yet closed with their
    /// abort marker, one per producer.
    ongoing_aborted: HashMap<ProducerId, AbortedTransactionState>,
    /// Aborted transactions the pass has not reached yet, ordered so that the
    /// one with the smallest first offset is last.
    pending_aborted: Vec<AbortedTxn>,
    /// Aborted transactions whose abort marker the pass kept, in marker order.
    /// They form the rewritten output's `.txnindex`.
    cleaned_index: Vec<AbortedTxn>,
}

fn batch_last_offset(batch: &RecordBatch) -> Offset {
    Offset(batch.base_offset + i64::from(batch.last_offset_delta))
}

impl CleanedTransactionMetadata {
    /// Add aborted transactions that the pass is about to walk over. The
    /// caller passes every aborted transaction that starts before the end of
    /// the range it is about to read and ends at or after its start, however
    /// far later in the log its marker sits. Adding the same transaction twice
    /// is harmless.
    pub fn add_aborted_transactions(&mut self, txns: impl IntoIterator<Item = AbortedTxn>) {
        self.pending_aborted.extend(txns);
        self.pending_aborted
            .sort_by_key(|txn| std::cmp::Reverse(txn.start_offset));
    }

    /// Take the aborted transactions whose abort marker the pass has kept since
    /// the last call. The rewrite writes them as the output's `.txnindex`.
    pub fn take_cleaned_index(&mut self) -> Vec<AbortedTxn> {
        std::mem::take(&mut self.cleaned_index)
    }

    /// Move every aborted transaction that starts at or before `offset` from
    /// the pending list to the ongoing ones. A producer has one open
    /// transaction at a time, so a second entry for a producer that is already
    /// ongoing is a duplicate and is dropped.
    fn consume_aborted_up_to(&mut self, offset: Offset) {
        while let Some(txn) = self
            .pending_aborted
            .pop_if(|txn| txn.start_offset <= offset)
        {
            self.ongoing_aborted
                .entry(txn.producer_id)
                .or_insert(AbortedTransactionState {
                    txn,
                    last_observed_batch_offset: None,
                });
        }
    }

    /// Update the state with a control batch the pass has just read, and say
    /// whether the batch is discardable.
    ///
    /// A discardable marker is one whose transaction the pass saw no batch of.
    /// Discardable does not mean deleted: the rewrite stamps such a marker with
    /// a delete horizon, and drops it once the horizon has passed. A control
    /// batch of any other type, such as a barrier marker, is never
    /// discardable.
    ///
    /// # Errors
    /// Returns the error of [`parse_control_marker_type`] for a key that is
    /// short or carries a negative version. Kafka's `ControlRecordType.parse`
    /// throws there, and the pass stops.
    pub fn on_control_batch_read(&mut self, batch: &RecordBatch) -> Result<bool, LogError> {
        self.consume_aborted_up_to(batch_last_offset(batch));
        // A control batch with no record was emptied by an earlier pass.
        let Some(record) = batch.records.first() else {
            return Ok(true);
        };
        let producer_id = ProducerId(batch.producer_id);
        Ok(match parse_control_marker_type(record.key.as_deref())? {
            ABORT_CONTROL_TYPE => match self.ongoing_aborted.remove(&producer_id) {
                // Keep the marker until every batch of the transaction is gone.
                Some(state) if state.last_observed_batch_offset.is_some() => {
                    self.cleaned_index.push(state.txn);
                    false
                }
                _ => true,
            },
            // The marker is discardable when the pass read no batch of the
            // transaction.
            COMMIT_CONTROL_TYPE => !self.ongoing_committed.remove(&producer_id),
            _ => false,
        })
    }

    /// Update the state with a data batch the pass has just read, and say
    /// whether the batch belongs to an aborted transaction, which makes all of
    /// its records discardable.
    pub fn on_batch_read(&mut self, batch: &RecordBatch) -> bool {
        let last_offset = batch_last_offset(batch);
        self.consume_aborted_up_to(last_offset);
        if !batch.attributes.is_transactional() {
            return false;
        }
        let producer_id = ProducerId(batch.producer_id);
        if let Some(state) = self.ongoing_aborted.get_mut(&producer_id) {
            state.last_observed_batch_offset = Some(last_offset);
            true
        } else {
            self.ongoing_committed.insert(producer_id);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::compact::test_support::{control_batch, make_record},
        krabka_protocol::records::{Attributes, RecordBatch},
    };

    const ABORT: i16 = ABORT_CONTROL_TYPE;
    const COMMIT: i16 = COMMIT_CONTROL_TYPE;

    fn data(base_offset: i64, producer_id: i64, transactional: bool) -> RecordBatch {
        RecordBatch {
            base_offset,
            last_offset_delta: 0,
            producer_id,
            attributes: Attributes::default().with_transactional(transactional),
            records: vec![make_record(0, Some(b"k"), Some(b"v"))],
            ..RecordBatch::default()
        }
    }

    fn aborted(producer_id: i64, start: i64, last: i64) -> AbortedTxn {
        AbortedTxn {
            start_offset: Offset(start),
            last_offset: Offset(last),
            producer_id: ProducerId(producer_id),
            last_stable_offset: Offset(last + 1),
        }
    }

    /// One step a pass takes over a batch, and what the state answers.
    enum Step {
        Data(RecordBatch, bool),
        Control(RecordBatch, bool),
    }

    fn run(txns: Vec<AbortedTxn>, steps: Vec<Step>) -> Vec<AbortedTxn> {
        let mut meta = CleanedTransactionMetadata::default();
        meta.add_aborted_transactions(txns);
        for (index, step) in steps.into_iter().enumerate() {
            let (answer, want) = match step {
                Step::Data(batch, want) => (meta.on_batch_read(&batch), want),
                Step::Control(batch, want) => (meta.on_control_batch_read(&batch).unwrap(), want),
            };
            assert2::assert!(answer == want, "step {index}");
        }
        meta.take_cleaned_index()
    }

    fn aborted_steps(mut prefix: Vec<Step>) -> Vec<Step> {
        prefix.extend([
            Step::Data(data(1, 7, true), true),
            Step::Control(control_batch(2, 7, ABORT), false),
        ]);
        prefix
    }

    #[test]
    fn a_batch_of_an_aborted_transaction_is_discardable_and_holds_its_marker() {
        let kept = run(
            vec![aborted(7, 1, 2)],
            // Before the transaction's first offset: committed data.
            aborted_steps(vec![Step::Data(data(0, 7, true), false)]),
        );
        assert2::assert!(kept == vec![aborted(7, 1, 2)]);
    }

    #[test]
    fn an_abort_marker_with_no_batch_left_is_discardable_and_leaves_the_index() {
        let kept = run(vec![aborted(7, 1, 2)], vec![Step::Control(
            control_batch(2, 7, ABORT),
            true,
        )]);
        assert2::assert!(kept.is_empty());
    }

    #[test]
    fn a_commit_marker_is_discardable_once_the_batches_before_it_are_gone() {
        run(vec![], vec![
            Step::Control(control_batch(0, 7, COMMIT), true),
            Step::Data(data(1, 7, true), false),
            Step::Control(control_batch(2, 7, COMMIT), false),
            // The second marker consumed the observation, so the next
            // marker of the same producer has no batch behind it.
            Step::Control(control_batch(3, 7, COMMIT), true),
        ]);
    }

    #[test]
    fn one_producers_data_does_not_hold_another_producers_marker() {
        run(vec![], vec![
            Step::Data(data(0, 7, true), false),
            Step::Control(control_batch(1, 8, COMMIT), true),
            Step::Control(control_batch(2, 7, COMMIT), false),
        ]);
    }

    #[test]
    fn non_transactional_batches_and_unknown_control_types_hold_nothing() {
        run(vec![], vec![
            Step::Data(data(0, 7, false), false),
            Step::Control(control_batch(1, 7, COMMIT), true),
            // A control type that is neither commit nor abort, such as a
            // barrier marker, is never discardable.
            Step::Control(control_batch(2, -1, 1000), false),
        ]);
    }

    #[test]
    fn an_emptied_control_batch_is_discardable() {
        let mut emptied = control_batch(0, 7, COMMIT);
        emptied.records.clear();
        run(vec![], vec![Step::Control(emptied, true)]);
    }

    #[test]
    fn an_aborted_transaction_added_twice_is_tracked_once() {
        let kept = run(
            vec![aborted(7, 1, 2), aborted(7, 1, 2)],
            aborted_steps(vec![]),
        );
        assert2::assert!(kept == vec![aborted(7, 1, 2)]);
    }

    #[test]
    fn transactions_of_one_producer_are_told_apart_by_their_first_offset() {
        // The first transaction of producer 7 aborts at 2, the second commits.
        // The second transaction's data starts after the first one's marker,
        // so it is not aborted.
        let kept = run(vec![aborted(7, 0, 2)], vec![
            Step::Data(data(0, 7, true), true),
            Step::Control(control_batch(2, 7, ABORT), false),
            Step::Data(data(3, 7, true), false),
            Step::Control(control_batch(4, 7, COMMIT), false),
        ]);
        assert2::assert!(kept == vec![aborted(7, 0, 2)]);
    }
}
