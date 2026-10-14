//! Exact, overflow-safe advancement for signed metadata epochs.

#[cfg(creusot)]
use creusot_std::prelude::ensures;

/// Return the exact successor of `current` when it fits below `maximum`.
///
/// The explicit maximum lets narrower host fields, such as Kafka `int32`
/// epochs, use the same proved `i64` primitive as barrier epochs.
#[must_use]
#[cfg_attr(
    creusot,
    ensures(match result {
        Some(next) => current@ < maximum@ && next@ == current@ + 1 && next@ <= maximum@,
        None => current@ >= maximum@,
    })
)]
pub const fn exact_epoch_successor(current: i64, maximum: i64) -> Option<i64> {
    if current < maximum {
        Some(current + 1)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::exact_epoch_successor;

    #[yare::parameterized(
        negative = { -1, i64::from(i32::MAX), Some(0) },
        zero = { 0, i64::from(i32::MAX), Some(1) },
        ordinary = { 41, i64::from(i32::MAX), Some(42) },
        maximum = { i64::from(i32::MAX) - 1, i64::from(i32::MAX), Some(i64::from(i32::MAX)) },
        i64_max = { i64::MAX - 1, i64::MAX, Some(i64::MAX) },
    )]
    fn returns_the_exact_successor(current: i64, maximum: i64, expected: Option<i64>) {
        assert2::assert!(exact_epoch_successor(current, maximum) == expected);
    }

    #[test]
    fn rejects_the_maximum_and_values_above_it() {
        assert2::assert!(exact_epoch_successor(i64::from(i32::MAX), i64::from(i32::MAX)).is_none());
        assert2::assert!(exact_epoch_successor(i64::MAX, i64::MAX).is_none());
        assert2::assert!(exact_epoch_successor(7, 6).is_none());
    }
}
