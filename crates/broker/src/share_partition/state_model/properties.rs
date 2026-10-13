//! The stateright `Model` implementation: the initial state, the enabled
//! actions, the transition function, the search boundary, and the properties
//! the checker proves.
//!
//! The transition arms call the production `AcquisitionState` methods
//! directly, so this file is where the model meets the real code. It is one
//! `impl` block and cannot be split further.

use {
    super::{
        config::{LOCK, ShareModel},
        invariants::{
            assert_transition, delivery_complete_count_is_terminal_in_window, lock_consistency,
            mutual_exclusion, window_integrity,
        },
        observe::{acquired_runs, deferred_offsets, offset_state},
        state::{ShareAction, ShareState},
    },
    crate::share_partition::state::{AckType, AcquisitionState, RecordState},
    assert2::assert,
    krabka_log::Offset,
    stateright::{Model, Property},
};

impl Model for ShareModel {
    type Action = ShareAction;
    type State = ShareState;

    krabka_macros::model_transition! { last, action, state; {
        match action {
            ShareAction::Produce => {
                if state.hwm >= self.max_offset {
                    return None;
                }
                state.hwm += 1;
            }
            ShareAction::Materialize => {
                let before = state.sm.end_offset;
                state.sm.materialize(state.hwm, self.max_inflight);
                if state.sm.end_offset == before {
                    return None; // no-op: nothing materialized
                }
            }
            ShareAction::Acquire {
                member,
                max_records,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                let deferred = deferred_offsets(&state.sm);
                let handed_out = state.sm.acquire(
                    &name,
                    max_records,
                    krabka_log::Offset(i64::MAX),
                    now,
                    LOCK,
                    self.max_attempts,
                );
                for range in &handed_out {
                    for raw in range.first.0..=range.last.0 {
                        assert!(
                            !deferred.contains(&Offset(raw)),
                            "acquire handed out deferred offset {raw}"
                        );
                    }
                }
            }
            ShareAction::Defer { first, last: hi } => {
                state.sm.defer_internal(first, hi);
                if state.sm == last.sm {
                    return None; // nothing in the range was Available
                }
            }
            ShareAction::PromoteDeferred => {
                state.sm.promote_deferred();
                if state.sm == last.sm {
                    return None; // nothing was deferred
                }
            }
            ShareAction::Acknowledge {
                member,
                first,
                last: hi,
                ack,
            } => {
                let name = Self::member_name(member);
                if state
                    .sm
                    .acknowledge(&name, first, hi, ack, self.max_attempts)
                    .is_err()
                {
                    return None; // inapplicable ack: no transition
                }
            }
            ShareAction::Renew {
                member,
                first,
                last: hi,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.renew(&name, first, hi, now, LOCK).is_err() {
                    return None; // inapplicable renew: no transition
                }
            }
            ShareAction::ExpireLocks => {
                let now = self.now(state.clock);
                state.sm.expire_locks(now, self.max_attempts);
            }
            ShareAction::Tick => {
                if state.clock >= self.max_tick {
                    return None;
                }
                state.clock += 1;
            }
            ShareAction::Reload => {
                let deferred = deferred_offsets(&state.sm);
                let window = (state.sm.start_offset, state.sm.end_offset);
                let (start, _, batches) = state.sm.to_persist_batches();
                let mut fresh = AcquisitionState::new(start);
                fresh.load_from(start, state.sm.state_epoch, state.sm.leader_epoch, &batches);
                // KFC-1: `Deferred` persists as `Available`, so the new leader
                // re-derives it from the log and its own clock. The model's
                // clock has not moved, so the same offsets come back deferred,
                // and the round trip must lose none of them.
                for off in &deferred {
                    fresh.defer_internal(*off, *off);
                }
                assert!(
                    (fresh.start_offset, fresh.end_offset) == window,
                    "reload lost part of the window: {window:?} -> {:?}",
                    (fresh.start_offset, fresh.end_offset)
                );
                assert!(
                    deferred_offsets(&fresh) == deferred,
                    "reload lost the deferral: {deferred:?} -> {:?}",
                    deferred_offsets(&fresh)
                );
                state.sm = fresh;
            }
            ShareAction::AdvanceLogStart { new_start } => {
                if new_start <= state.log_start {
                    return None; // log start never moves backward
                }
                state.sm.advance_past_log_start(new_start);
                state.log_start = new_start;
            }
        }
        assert_transition(&last.sm, &state.sm, action);
        Some(state)
    }}

    fn init_states(&self) -> Vec<Self::State> {
        vec![ShareState {
            sm: AcquisitionState::new(Offset(0)),
            clock: 0,
            hwm: Offset(0),
            log_start: Offset(0),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let has_available = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        let has_acquired = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Acquired);

        if state.hwm < self.max_offset {
            actions.push(ShareAction::Produce);
        }
        // Materialize only when there are produced-but-unmaterialized records and
        // no Available batch remains (the real `materialize` no-ops otherwise).
        if state.sm.end_offset < state.hwm && !has_available {
            actions.push(ShareAction::Materialize);
        }
        if has_available {
            for member in 0..self.members {
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: 1,
                });
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: i32::MAX,
                });
            }
        }
        // Data-dependent: ack/renew only over ranges a member actually holds.
        for member in 0..self.members {
            let name = Self::member_name(member);
            for (first, last) in acquired_runs(&state.sm, &name) {
                for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                    actions.push(ShareAction::Acknowledge {
                        member,
                        first,
                        last,
                        ack,
                    });
                }
                actions.push(ShareAction::Renew {
                    member,
                    first,
                    last,
                });
                // A split (first half) exercises partial-ack / partial-renew.
                if last > first {
                    let mid = first + (last.0 - first.0) / 2;
                    for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                        actions.push(ShareAction::Acknowledge {
                            member,
                            first,
                            last: mid,
                            ack,
                        });
                    }
                    actions.push(ShareAction::Renew {
                        member,
                        first,
                        last: mid,
                    });
                }
            }
        }
        if self.allow_defer {
            // Every sub-range of the window that covers something the schedule
            // could still hold back. Ranges rather than single offsets, so the
            // model exercises the splits `defer_internal` makes at its edges.
            for first in state.sm.start_offset.0..state.sm.end_offset.0 {
                for last in first..state.sm.end_offset.0 {
                    let defers_something = (first..=last).any(|raw| {
                        offset_state(&state.sm, Offset(raw)) == Some(RecordState::Available)
                    });
                    if defers_something {
                        actions.push(ShareAction::Defer {
                            first: Offset(first),
                            last: Offset(last),
                        });
                    }
                }
            }
            if !deferred_offsets(&state.sm).is_empty() {
                actions.push(ShareAction::PromoteDeferred);
            }
        }
        if has_acquired {
            actions.push(ShareAction::ExpireLocks);
        }
        if state.clock < self.max_tick {
            actions.push(ShareAction::Tick);
        }
        if self.allow_reload && state.sm.end_offset > state.sm.start_offset {
            actions.push(ShareAction::Reload);
        }
        if self.allow_log_start_advance {
            for raw in (state.log_start.0 + 1)..=state.sm.end_offset.0 {
                actions.push(ShareAction::AdvanceLogStart {
                    new_start: Offset(raw),
                });
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("window_integrity", |_, s: &ShareState| {
                window_integrity(&s.sm)
            }),
            Property::always("mutual_exclusion", |_, s: &ShareState| {
                mutual_exclusion(&s.sm)
            }),
            Property::always("lock_consistency", |_, s: &ShareState| {
                lock_consistency(&s.sm)
            }),
            Property::always(
                "delivery_complete_count_is_terminal_in_window",
                |_, s: &ShareState| delivery_complete_count_is_terminal_in_window(&s.sm),
            ),
            Property::always(
                "delivery_count_bounded",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.batches
                        .iter()
                        .all(|b| b.delivery_count <= m.max_attempts)
                },
            ),
            Property::always("spso_in_range", |m: &ShareModel, s: &ShareState| {
                0 <= s.sm.start_offset
                    && s.sm.start_offset <= s.sm.end_offset
                    && s.sm.end_offset <= m.max_offset
            }),
            Property::sometimes("can_advance_spso", |_, s: &ShareState| {
                s.sm.start_offset > 0
            }),
            Property::sometimes("can_acknowledge", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Acknowledged)
            }),
            Property::sometimes("can_archive", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Archived)
            }),
            Property::sometimes("can_redeliver", |_, s: &ShareState| {
                s.sm.batches.iter().any(|b| b.delivery_count >= 2)
            }),
            // The count invariant is not vacuous: a terminal record can sit in
            // the window above a record that still holds the SPSO.
            Property::sometimes("can_count_delivery_complete", |_, s: &ShareState| {
                s.sm.delivery_complete_count > 0
            }),
        ];
        if !self.allow_defer {
            // Kafka's `numInFlightRecords` never passes the record lock limit
            // (`group.share.partition.max.record.locks`): the window is at most
            // `max_inflight` long. A deferred run is not in flight and is
            // promoted into the window later, so the claim is for the models
            // without deferral.
            properties.push(Property::always(
                "window_within_record_lock_limit",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.end_offset.0 - s.sm.start_offset.0 <= i64::from(m.max_inflight)
                },
            ));
        }
        if self.allow_log_start_advance {
            // The log start offset never runs ahead of what was produced: it
            // can only move over records that exist.
            properties.push(Property::always(
                "log_start_never_exceeds_hwm",
                |_, s: &ShareState| s.log_start <= s.hwm,
            ));
            // The scenario `advance_past_log_start_leaves_other_members_acquired_records_alone`
            // covers by hand: the log start moves past the SPSO while an
            // Acquired run still blocks the SPSO itself from following it.
            properties.push(Property::sometimes(
                "log_start_advance_blocked_by_acquired",
                |_, s: &ShareState| {
                    s.log_start > s.sm.start_offset
                        && s.sm
                            .batches
                            .iter()
                            .any(|b| b.state == RecordState::Acquired)
                },
            ));
        }
        if self.allow_defer {
            properties.push(Property::sometimes("can_defer", |_, s: &ShareState| {
                !deferred_offsets(&s.sm).is_empty()
            }));
            // The claim KFC-1 makes for share groups, and the one a classic
            // group cannot have: a record is handed out while a record below it
            // waits for its delivery time.
            properties.push(Property::sometimes(
                "can_deliver_behind_a_deferred_record",
                |_, s: &ShareState| {
                    deferred_offsets(&s.sm).first().is_some_and(|waiting| {
                        s.sm.batches
                            .iter()
                            .any(|b| b.state == RecordState::Acquired && b.first_offset > *waiting)
                    })
                },
            ));
        }
        properties
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound ONLY the design-unbounded dimensions (so the space is finite);
        // do NOT bound delivery_count — its <= max_attempts boundedness is a
        // property we verify, so pruning it would mask a violation. The 12-batch
        // cap is a loose structural safety net (real max over a <=3 window is 3).
        state.clock <= self.max_tick
            && state.hwm <= self.max_offset
            && state.sm.end_offset <= self.max_offset
            && state.log_start <= self.max_offset
            && state.sm.batches.len() <= 12
    }
}
