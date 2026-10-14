use super::*;

#[yare::parameterized(
    absent = { None, None },
    ordinary = { Some(9), Some(10) },
    maximum_successor = { Some(i64::MAX - 1), Some(i64::MAX) },
    maximum_overflow = { Some(i64::MAX), None },
)]
fn delete_target_rejects_offset_exhaustion(last_offset: Option<i64>, expected: Option<i64>) {
    check!(retention_delete_target(last_offset) == expected);
}
