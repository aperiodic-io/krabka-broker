use {
    super::{ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, producer_decision},
    crate::transaction::{
        InitProducerIdIdentityDecision, init_producer_id_identity_decision, next_producer_identity,
    },
    creusot_std::prelude::*,
};

type RotationRetry = (
    (i64, i16),
    InitProducerIdIdentityDecision,
    InitProducerIdIdentityDecision,
    InitProducerIdIdentityDecision,
    ProducerDecision,
    ProducerDecision,
);

/// Normal rotation reserves the final epoch for the old PID's marker. Once
/// that marker and the coordinator completion are installed, old data is
/// fenced but the exhausted identity can retry initialization. The first
/// subsequent epoch bump transfers that retry to the fresh identity.
/// Fresh PID allocation, PID-keyed lookup, marker installation and faithful
/// coordinator last/previous identity projection are host facts. Recovery
/// rotation, full API admission, expiry and persistence are outside this law.
#[requires(old_pid@ >= 0 && fresh_pid@ >= 0 && old_pid != fresh_pid)]
#[requires(request.0@ <= i16::MAX@ - 1)]
#[ensures(result.0.0 == fresh_pid && result.0.1@ == 0 && result.0.0 != old_pid)]
#[ensures(result.1 == InitProducerIdIdentityDecision::Retry)]
#[ensures(result.2 == InitProducerIdIdentityDecision::Fenced)]
#[ensures(result.3 == InitProducerIdIdentityDecision::Retry)]
#[ensures(result.4 == ProducerDecision::Fenced && result.5 == ProducerDecision::Append)]
pub(super) fn rotation_marker_bounds_identity_retry(
    old_pid: i64,
    fresh_pid: i64,
    retained: &[Option<RetainedSequenceRange>],
    request: (i16, i32, i32),
    log_empty: bool,
    trunk_rules: bool,
) -> RotationRetry {
    let old_epoch = i16::MAX - 1;
    let rotated = next_producer_identity(true, false, old_pid, old_epoch, Some(fresh_pid)).unwrap();
    let identity_retry = init_producer_id_identity_decision(
        rotated.0, rotated.1, old_epoch, old_pid, old_pid, old_epoch,
    );
    let advanced = next_producer_identity(true, false, rotated.0, rotated.1, None).unwrap();
    let old_after_bump = init_producer_id_identity_decision(
        advanced.0, advanced.1, rotated.1, old_pid, old_pid, old_epoch,
    );
    let fresh_retry = init_producer_id_identity_decision(
        advanced.0, advanced.1, rotated.1, old_pid, rotated.0, rotated.1,
    );
    // Match the normal completion's old-PID marker projection, not the fresh
    // identity's epoch. Retained aliases cannot bypass this higher epoch.
    let marker = ProducerEntryFacts {
        epoch: old_epoch.saturating_add(1),
        last_sequence: -1,
    };
    let old_data = producer_decision(
        Some(marker),
        retained,
        request.0,
        request.1,
        request.2,
        log_empty,
        trunk_rules,
    );
    let fresh_data = producer_decision(None, &[], rotated.1, 0, request.2, log_empty, trunk_rules);
    (
        rotated,
        identity_retry,
        old_after_bump,
        fresh_retry,
        old_data,
        fresh_data,
    )
}
