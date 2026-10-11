//! Application of one piggybacked acknowledgement batch to a share
//! partition's acquisition state.
//!
//! `ShareAcknowledge` applies the same batches without a fetch, so this step
//! is shared and not folded into the acquire pass.

use std::time::{Duration, Instant};

use krabka_log::Offset;

use crate::{
    codes,
    share_partition::state::{AckType, AcquisitionState},
};

/// The KIP-1222 acknowledge type `Renew`.
const ACK_RENEW: i8 = 4;

/// How an acknowledgement request treats the acknowledge type `Renew`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Renewal {
    /// The request set `IsRenewAck`.
    pub(crate) requested: bool,
    /// The group allows renewals: `share.renew.acknowledge.enable`, as
    /// [`GroupShareSettings`](crate::share_partition::group_settings::GroupShareSettings)
    /// resolves it.
    pub(crate) enabled: bool,
    /// The new lock length of a renewed record.
    pub(crate) lock_duration: Duration,
}

/// The highest acknowledge type of a request version without `Renew`:
/// `Reject`.
const MAX_ACK_TYPE: i8 = 3;

/// Kafka's `KafkaApis.validateAcknowledgementBatches` for the batches of one
/// partition, as `(first_offset, last_offset, acknowledge_types)`.
///
/// A partition is refused with `INVALID_REQUEST` when a batch has its first
/// offset past its last, starts before the last offset of the batch in front
/// of it, has no acknowledge type, has more than one type but not one per
/// offset, has a type outside `[0, 3]` (`[0, 4]` when the version supports
/// `Renew`), or has the type `Renew` in a request without `IsRenewAck`.
pub(crate) fn acknowledgement_batches_are_valid<'a>(
    batches: impl IntoIterator<Item = (i64, i64, &'a [i8])>,
    supports_renew: bool,
    is_renew_ack: bool,
) -> bool {
    let max_type = if supports_renew {
        ACK_RENEW
    } else {
        MAX_ACK_TYPE
    };
    let mut previous_last = -1_i64;
    for (first, last, types) in batches {
        let per_offset = types.len() > 1;
        let covers_range = i64::try_from(types.len())
            .is_ok_and(|count| last.checked_sub(first) == Some(count - 1));
        let valid = first <= last
            && first >= previous_last
            && !types.is_empty()
            && (!per_offset || covers_range)
            && types.iter().all(|ack| (0..=max_type).contains(ack))
            && (is_renew_ack || !types.contains(&ACK_RENEW));
        if !valid {
            return false;
        }
        previous_last = last;
    }
    true
}

/// What one member's acknowledgements apply with.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AckApplication<'a> {
    pub(crate) member: &'a str,
    pub(crate) now: Instant,
    pub(crate) renewal: Renewal,
    /// The group's delivery count limit: a released record at the limit is
    /// archived.
    pub(crate) max_attempts: i16,
}

/// Kafka's `SharePartition.acknowledge`: applies the acknowledgement batches
/// of one partition, as `(first_offset, last_offset, acknowledge_types)`, as
/// one unit.
///
/// It first expires every lock whose deadline has passed. Kafka arms a timer
/// on each acquired batch (`releaseAcquisitionLockOnTimeout`), so at the
/// deadline the record is `AVAILABLE` (or `ARCHIVED` at the delivery limit)
/// and a later acknowledgement or renewal of it is `INVALID_RECORD_STATE`.
/// The expiry stays even when an acknowledgement fails, because the timer
/// fires whatever a member sends.
///
/// It stops at the first batch that fails and restores the state that `st`
/// held before the first batch, so nothing of a failed acknowledgement stays
/// or is persisted, as `rollbackOrProcessStateUpdates` does. It returns that
/// batch's error code, or `NONE`.
pub(crate) fn apply_acknowledgements<'b>(
    st: &mut AcquisitionState,
    application: &AckApplication<'_>,
    batches: impl IntoIterator<Item = (i64, i64, &'b [i8])>,
) -> i16 {
    st.expire_locks(application.now, application.max_attempts);
    let before = st.clone();
    for (first, last, types) in batches {
        if let Err(code) = apply_one_ack(st, application, first, last, types) {
            *st = before;
            return code;
        }
    }
    codes::NONE
}

/// Applies one acknowledgement batch to the state machine.
///
/// The batch as a whole must lie inside the records that were handed out, as
/// Kafka's `fetchSubMapForAcknowledgementBatch` checks before it looks at any
/// offset. A singleton `acknowledge_types` applies that type across the whole
/// range; otherwise each entry maps to one offset, starting at `first`. This
/// function merges a run of the same type into one call, and stops at the
/// first run that fails.
///
/// The type `Renew` (4) renews the lock of its offsets and leaves their
/// state as it is. The other types take their normal transition in the same
/// batch, as in Kafka's `SharePartition.acknowledgePerOffsetBatchRecords` and
/// `acknowledgeCompleteBatch`. A `Renew` in a request without `IsRenewAck`
/// is `INVALID_REQUEST`, and a `Renew` for a group that does not allow it is
/// `INVALID_RECORD_STATE`.
fn apply_one_ack(
    st: &mut AcquisitionState,
    application: &AckApplication<'_>,
    first: i64,
    last: i64,
    types: &[i8],
) -> Result<(), i16> {
    let &AckApplication {
        member,
        now,
        renewal,
        max_attempts,
    } = application;
    if first > last || types.is_empty() {
        return Err(codes::INVALID_REQUEST);
    }
    if st.ack_bounds(Offset(first), Offset(last))?.is_none() {
        return Ok(());
    }
    let apply = |st: &mut AcquisitionState, run_first: i64, run_last: i64, ack_type: i8| {
        if ack_type == ACK_RENEW {
            if !renewal.requested {
                return Err(codes::INVALID_REQUEST);
            }
            if !renewal.enabled {
                return Err(codes::INVALID_RECORD_STATE);
            }
            return st.renew(
                member,
                Offset(run_first),
                Offset(run_last),
                now,
                renewal.lock_duration,
            );
        }
        let ack = AckType::from_i8(ack_type).ok_or(codes::INVALID_REQUEST)?;
        st.acknowledge(
            member,
            Offset(run_first),
            Offset(run_last),
            ack,
            max_attempts,
        )
    };
    if let [ack_type] = types {
        return apply(st, first, last, *ack_type);
    }
    let range_len = last
        .checked_sub(first)
        .and_then(|len| len.checked_add(1))
        .and_then(|len| usize::try_from(len).ok());
    if range_len != Some(types.len()) {
        return Err(codes::INVALID_REQUEST);
    }
    // Walk the per-offset type list, coalescing equal-typed runs.
    let mut run_start = first;
    for run in types.chunk_by(|a, b| a == b) {
        let run_end = run_start + i64::try_from(run.len()).unwrap_or(i64::MAX) - 1;
        apply(st, run_start, run_end, run[0])?;
        run_start = run_end + 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::share_partition::state::{AcquisitionState, RecordState};

    #[test]
    fn singleton_ack_type_applies_to_the_whole_range() {
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(200), 200);
        assert!(
            state
                .acquire(
                    "member",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .len()
                == 1
        );

        let application = AckApplication {
            member: "member",
            now: Instant::now(),
            renewal: Renewal {
                requested: false,
                enabled: true,
                lock_duration: Duration::from_secs(30),
            },
            max_attempts: 5,
        };
        apply_one_ack(&mut state, &application, 0, 199, &[1]).expect("acknowledge");

        assert!(state.start_offset == Offset(200));
        assert!(
            state
                .acquire(
                    "other",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .is_empty()
        );
    }

    /// The setup a [`Row`] starts from.
    #[derive(Debug, Clone, Copy)]
    enum Setup {
        /// `member` holds offsets 0 to 9 at delivery count 1.
        Acquired,
        /// Offsets 0 to 4 are accepted, so the SPSO is 5, and `member` holds
        /// 5 to 9.
        HalfAccepted,
        /// `member` holds 0 to 4 and `other` holds 5 to 9.
        Split,
        /// `member` holds 0 to 9 at delivery count 2, the limit of these rows.
        AtTheLimit,
    }

    /// One row: the setup, the batches, and what Kafka's
    /// `SharePartition.acknowledge` leaves behind.
    struct Row {
        name: &'static str,
        setup: Setup,
        batches: &'static [(i64, i64, &'static [i8])],
        error: i16,
        spso: i64,
        states: Vec<(i64, RecordState)>,
    }

    const LIMIT: i16 = 2;
    const ACCEPT: i8 = 1;
    const RELEASE: i8 = 2;

    fn states(first: i64, last: i64, state: RecordState) -> Vec<(i64, RecordState)> {
        (first..=last).map(|offset| (offset, state)).collect()
    }

    fn start(setup: Setup) -> AcquisitionState {
        let now = Instant::now();
        let lock = Duration::from_secs(30);
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(10), 100);
        match setup {
            Setup::Acquired | Setup::HalfAccepted => {
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
            }
            Setup::Split => {
                state.acquire("member", 5, Offset(i64::MAX), now, lock, LIMIT);
                state.acquire("other", 5, Offset(i64::MAX), now, lock, LIMIT);
            }
            Setup::AtTheLimit => {
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
                state.release_member("member", LIMIT);
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
            }
        }
        if let Setup::HalfAccepted = setup {
            state
                .acknowledge("member", Offset(0), Offset(4), AckType::Accept, LIMIT)
                .expect("accept the first half");
        }
        state
    }

    fn rows() -> Vec<Row> {
        use RecordState::Acquired;
        vec![
            Row {
                name: "a failed second batch rolls the first back",
                setup: Setup::Split,
                batches: &[(0, 4, &[ACCEPT]), (5, 9, &[ACCEPT])],
                error: codes::INVALID_RECORD_STATE,
                spso: 0,
                states: states(0, 9, Acquired),
            },
            Row {
                name: "a batch below the SPSO is skipped",
                setup: Setup::HalfAccepted,
                batches: &[(0, 2, &[ACCEPT])],
                error: codes::NONE,
                spso: 5,
                states: states(5, 9, Acquired),
            },
            Row {
                name: "a batch across the SPSO acknowledges the part above it",
                setup: Setup::HalfAccepted,
                batches: &[(3, 7, &[ACCEPT])],
                error: codes::NONE,
                spso: 8,
                states: states(8, 9, Acquired),
            },
            Row {
                name: "per-offset types across the SPSO",
                setup: Setup::HalfAccepted,
                batches: &[(4, 6, &[RELEASE, ACCEPT, ACCEPT])],
                error: codes::NONE,
                spso: 7,
                states: states(7, 9, Acquired),
            },
            Row {
                name: "a batch past the records handed out",
                setup: Setup::Acquired,
                batches: &[(0, 4, &[ACCEPT]), (8, 12, &[ACCEPT])],
                error: codes::INVALID_REQUEST,
                spso: 0,
                states: states(0, 9, Acquired),
            },
            Row {
                name: "a release at the delivery limit archives",
                setup: Setup::AtTheLimit,
                batches: &[(0, 9, &[RELEASE])],
                error: codes::NONE,
                spso: 10,
                states: Vec::new(),
            },
            Row {
                name: "a release under the limit offers the records again",
                setup: Setup::Acquired,
                batches: &[(0, 9, &[RELEASE])],
                error: codes::NONE,
                spso: 0,
                states: states(0, 9, RecordState::Available),
            },
            Row {
                name: "a release of part of the records at the limit",
                setup: Setup::AtTheLimit,
                batches: &[(0, 4, &[RELEASE])],
                error: codes::NONE,
                spso: 5,
                states: states(5, 9, Acquired),
            },
            Row {
                // The first batch archives 0 to 9 and moves the SPSO past
                // them, so the second batch is below the SPSO.
                name: "an archived run is below the SPSO for a later batch",
                setup: Setup::AtTheLimit,
                batches: &[(0, 9, &[RELEASE]), (0, 9, &[ACCEPT])],
                error: codes::NONE,
                spso: 10,
                states: Vec::new(),
            },
        ]
    }

    #[test]
    fn acknowledgements_apply_as_kafka_applies_them() {
        let application = AckApplication {
            member: "member",
            now: Instant::now(),
            renewal: Renewal {
                requested: false,
                enabled: true,
                lock_duration: Duration::from_secs(30),
            },
            max_attempts: LIMIT,
        };
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for row in rows() {
            let mut state = start(row.setup);
            let error = apply_acknowledgements(
                &mut state,
                &application,
                row.batches
                    .iter()
                    .map(|&(first, last, types)| (first, last, types)),
            );
            actual.push((row.name, error, state.start_offset.0, state.record_states()));
            expected.push((row.name, row.error, row.spso, row.states));
        }
        assert!(actual == expected);
    }

    /// Kafka's lock timer fires at the deadline, so an acknowledgement or a
    /// renewal that arrives after it finds the records `AVAILABLE`, or
    /// `ARCHIVED` at the delivery limit, and no sweep has to run first.
    #[test]
    fn an_acknowledgement_after_the_lock_deadline_is_refused() {
        use RecordState::Available;
        const RENEW: i8 = 4;
        // (name, setup, seconds after the acquisition, renew request,
        // acknowledge type, error, SPSO, states)
        type Case = (
            &'static str,
            Setup,
            u64,
            bool,
            i8,
            i16,
            i64,
            Vec<(i64, RecordState)>,
        );
        let rows: Vec<Case> = vec![
            (
                "an acknowledgement inside the lock",
                Setup::Acquired,
                29,
                false,
                ACCEPT,
                codes::NONE,
                10,
                Vec::new(),
            ),
            (
                "an acknowledgement past the lock",
                Setup::Acquired,
                31,
                false,
                ACCEPT,
                codes::INVALID_RECORD_STATE,
                0,
                states(0, 9, Available),
            ),
            (
                "a renewal past the lock",
                Setup::Acquired,
                31,
                true,
                RENEW,
                codes::INVALID_RECORD_STATE,
                0,
                states(0, 9, Available),
            ),
            (
                "an acknowledgement past a lock at the delivery limit",
                Setup::AtTheLimit,
                31,
                false,
                ACCEPT,
                codes::NONE,
                10,
                Vec::new(),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, setup, after, requested, ack_type, error, spso, want) in rows {
            let mut state = start(setup);
            let application = AckApplication {
                member: "member",
                now: Instant::now() + Duration::from_secs(after),
                renewal: Renewal {
                    requested,
                    enabled: true,
                    lock_duration: Duration::from_secs(30),
                },
                max_attempts: LIMIT,
            };
            let got = apply_acknowledgements(&mut state, &application, [(0, 9, &[ack_type][..])]);
            actual.push((name, got, state.start_offset.0, state.record_states()));
            expected.push((name, error, spso, want));
        }
        assert!(actual == expected);
    }

    /// A lock that runs out at the delivery limit archives its records at
    /// once, and a lock under the limit offers them again.
    #[test]
    fn a_lock_expiry_at_the_limit_archives() {
        let expired = Instant::now() + Duration::from_secs(60);
        let mut actual = Vec::new();
        for setup in [Setup::Acquired, Setup::AtTheLimit] {
            let mut state = start(setup);
            state.expire_locks(expired, LIMIT);
            actual.push((state.start_offset.0, state.record_states()));
        }
        assert!(actual == vec![(0, states(0, 9, RecordState::Available)), (10, Vec::new()),]);
    }
}
