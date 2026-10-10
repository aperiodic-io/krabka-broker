//! Fixtures shared by the unit tests of the share-partition state machine.
//!
//! The concern modules under `state` each carry their own `#[cfg(test)] mod
//! tests`, and they take one clock origin and one lock duration from here, so a
//! test reads the same timings wherever it sits.

use std::time::{Duration, Instant};

use krabka_log::Offset;

use crate::test_support::RecordCount;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum DeadLetterQueue {
    #[default]
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct AcquiredWindowSetup {
    #[default(Offset(3))]
    pub end: Offset,
    #[default(RecordCount(10))]
    pub record_limit: RecordCount,
    pub dead_letter_queue: DeadLetterQueue,
}

#[derive(Clone, Copy)]
pub(crate) struct DeliveryCount(pub i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct DlqRangeSetup {
    #[default(Offset(0))]
    pub first: Offset,
    #[default(Offset(0))]
    pub last: Offset,
    #[default(DeliveryCount(1))]
    pub delivery_count: DeliveryCount,
    pub cause: Option<super::DlqCause>,
}

pub(super) fn t0() -> Instant {
    Instant::now()
}

pub(super) const LOCK: Duration = Duration::from_secs(30);

/// A materialized zero-based window already held by m1 with the ordinary test lock.
pub(crate) fn acquired_state(end: Offset) -> super::AcquisitionState {
    let mut state = super::AcquisitionState::new(krabka_log::Offset(0));
    acquire_window(
        &mut state,
        AcquiredWindowSetup {
            end,
            ..Default::default()
        },
    );
    state
}

/// Populate an existing fixture under the caller's lock, using its original record limit.
pub(crate) fn acquire_window(state: &mut super::AcquisitionState, setup: AcquiredWindowSetup) {
    let AcquiredWindowSetup {
        end,
        record_limit,
        dead_letter_queue,
    } = setup;
    if dead_letter_queue == DeadLetterQueue::Enabled {
        state.set_dlq_enabled(true);
    }
    state.materialize(end, 100);
    let _ = state.acquire(
        "m1",
        record_limit.0,
        krabka_log::Offset(i64::MAX),
        t0(),
        LOCK,
        5,
    );
}

pub(crate) fn dlq_range(setup: DlqRangeSetup) -> super::DlqRange {
    super::DlqRange {
        first: setup.first,
        last: setup.last,
        delivery_count: setup.delivery_count.0,
        cause: setup.cause,
    }
}
