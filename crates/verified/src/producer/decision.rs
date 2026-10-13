#[cfg(creusot)]
use super::{
    first_retained_duplicate, retained_duplicate_exists, retained_matches, sequence_decision,
};
use {
    super::{ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, increment_sequence},
    creusot_std::prelude::*,
};

/// Classify an incoming batch against a producer's tracked entry and the
/// sequence ranges of the batches it retains.
///
/// This is Kafka's `UnifiedLog.analyzeAndValidateProducerState` for a client
/// append. It first looks the batch up among the retained batches
/// (`ProducerStateEntry.findDuplicateBatch`): at the entry's epoch, a batch
/// whose base sequence and last sequence (`base_sequence + last_offset_delta`
/// modulo `2^31`) equal a retained batch's is a duplicate of the first such
/// batch. `None` slots never match. Only a batch that repeats no retained
/// batch goes on to `ProducerAppendInfo.checkProducerEpoch` and
/// `checkSequence`:
///
/// - a producer with no entry can start at any sequence, except that under
///   `trunk_rules` it must start at sequence 0 on a partition whose log is
///   empty (`log_empty`) and is otherwise out of order;
/// - a lower epoch is fenced;
/// - a higher epoch must start at sequence 0 and is otherwise out of order;
/// - the same epoch must continue at the last sequence plus one modulo `2^31`
///   and is otherwise out of order.
///
/// The empty-log rule is Kafka trunk's (KAFKA-15591, in
/// `ProducerAppendInfo.checkSequence`); Kafka 4.3.1 does not have it.
/// `log_empty` is Kafka's `ProducerStateManager.mapEndOffset() == 0`: no
/// record has ever been appended to the partition, so no producer state can
/// have been lost, and `trunk_rules` says whether the host serves Kafka
/// trunk's behaviour.
///
/// The host supplies the retained batches at the entry's epoch; a batch at
/// any other epoch never matches them.
#[ensures(match result {
    ProducerDecision::Duplicate { retained: index } => first_retained_duplicate(
        entry, retained@, producer_epoch, base_sequence, last_offset_delta, index@),
    _ => !retained_duplicate_exists(
            entry, retained@, producer_epoch, base_sequence, last_offset_delta)
        && result == sequence_decision(
            entry, producer_epoch, base_sequence, log_empty, trunk_rules),
})]
#[must_use]
pub fn producer_decision(
    entry: Option<ProducerEntryFacts>,
    retained: &[Option<RetainedSequenceRange>],
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
    log_empty: bool,
    trunk_rules: bool,
) -> ProducerDecision {
    let Some(tracked) = entry else {
        if trunk_rules && log_empty && base_sequence != 0 {
            return ProducerDecision::OutOfOrder;
        }
        return ProducerDecision::Append;
    };
    if producer_epoch == tracked.epoch {
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let mut i = 0usize;
        #[invariant(i@ <= retained@.len())]
        #[invariant(forall<j: Int> 0 <= j && j < i@
            ==> !retained_matches(retained@[j], base_sequence, last_offset_delta))]
        #[variant(retained@.len() - i@)]
        while i < retained.len() {
            if let Some(range) = retained[i]
                && range.base_sequence == base_sequence
                && range.last_sequence == last_sequence
            {
                return ProducerDecision::Duplicate { retained: i };
            }
            i += 1;
        }
    }
    if producer_epoch < tracked.epoch {
        return ProducerDecision::Fenced;
    }
    if producer_epoch > tracked.epoch {
        if base_sequence == 0 {
            return ProducerDecision::Append;
        }
        return ProducerDecision::OutOfOrder;
    }
    if base_sequence == increment_sequence(tracked.last_sequence, 1) {
        return ProducerDecision::Append;
    }
    ProducerDecision::OutOfOrder
}
