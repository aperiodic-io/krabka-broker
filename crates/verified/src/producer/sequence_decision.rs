#[cfg(creusot)]
use super::{ProducerDecision, ProducerEntryFacts, RetainedSequenceRange};
use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn sequence_modulo_2_31(sequence: Int) -> Int {
    pearlite! { sequence.rem_euclid(2147483648) }
}

/// Advance a Kafka producer sequence modulo `2^31`.
///
/// The result is `sequence + increment` reduced modulo `2^31`. For the
/// nonnegative operands Kafka uses, that is exactly
/// `DefaultRecordBatch.incrementSequence`: the sum when it fits in an `i32`,
/// otherwise the sum minus `2^31`.
#[ensures(result@ == sequence_modulo_2_31(sequence@ + increment@))]
#[must_use]
pub fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    // Reduce the sum into `[0, 2^31)` without leaving `i32`: adding `2^31` is
    // subtracting `i32::MIN`, and subtracting `2^31` is adding it.
    match sequence.checked_add(increment) {
        Some(sum) if sum >= 0 => sum,
        Some(sum) => sum - i32::MIN,
        // The sum passed `i32::MAX`, so both operands are positive.
        None if increment > 0 => sequence + (increment + i32::MIN),
        // The sum fell below `i32::MIN`, so both operands are negative.
        None => (sequence - i32::MIN) + (increment - i32::MIN),
    }
}

/// Move a Kafka producer sequence backwards modulo `2^31`.
///
/// The result is `sequence - decrement` reduced modulo `2^31`. For the
/// nonnegative operands Kafka uses, that is exactly
/// `DefaultRecordBatch.decrementSequence`: the difference when it is not
/// negative, otherwise the difference plus `2^31`.
#[ensures(result@ == sequence_modulo_2_31(sequence@ - decrement@))]
#[must_use]
pub fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    // The same reduction as `increment_sequence`, for a difference.
    match sequence.checked_sub(decrement) {
        Some(difference) if difference >= 0 => difference,
        Some(difference) => difference - i32::MIN,
        // The difference passed `i32::MAX`: `sequence >= 0 > decrement`.
        None if decrement < 0 => sequence + (i32::MIN - decrement),
        // The difference fell below `i32::MIN`: `sequence < 0 < decrement`.
        None => (sequence - i32::MIN) - (decrement + i32::MIN),
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn retained_matches(
    slot: Option<RetainedSequenceRange>,
    base_sequence: i32,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        match slot {
            Some(range) => range.base_sequence@ == base_sequence@
                && range.last_sequence@
                    == sequence_modulo_2_31(base_sequence@ + last_offset_delta@),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn retained_duplicate_exists(
    entry: Option<ProducerEntryFacts>,
    retained: Seq<Option<RetainedSequenceRange>>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        match entry {
            Some(tracked) => tracked.epoch@ == producer_epoch@
                && exists<i: Int> 0 <= i && i < retained.len()
                    && retained_matches(retained[i], base_sequence, last_offset_delta),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn first_retained_duplicate(
    entry: Option<ProducerEntryFacts>,
    retained: Seq<Option<RetainedSequenceRange>>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
    index: Int,
) -> bool {
    pearlite! {
        match entry {
            Some(tracked) => tracked.epoch@ == producer_epoch@
                && 0 <= index && index < retained.len()
                && retained_matches(retained[index], base_sequence, last_offset_delta)
                && forall<j: Int> 0 <= j && j < index
                    ==> !retained_matches(retained[j], base_sequence, last_offset_delta),
            None => false,
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn sequence_decision(
    entry: Option<ProducerEntryFacts>,
    producer_epoch: i16,
    base_sequence: i32,
    log_empty: bool,
    trunk_rules: bool,
) -> ProducerDecision {
    pearlite! {
        match entry {
            None => if trunk_rules && log_empty && base_sequence@ != 0 {
                ProducerDecision::OutOfOrder
            } else {
                ProducerDecision::Append
            },
            Some(tracked) => if producer_epoch@ < tracked.epoch@ {
                ProducerDecision::Fenced
            } else if producer_epoch@ > tracked.epoch@ {
                if base_sequence@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder }
            } else if base_sequence@ == sequence_modulo_2_31(tracked.last_sequence@ + 1) {
                ProducerDecision::Append
            } else {
                ProducerDecision::OutOfOrder
            },
        }
    }
}
