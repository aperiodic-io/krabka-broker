//! The decoded `__barrier_state` records and the fold that replays them.
//!
//! Recovery reads the state partition it leads and folds every record into the
//! group map. The fold is the one place that decides what a tombstone removes,
//! so it stays apart from the entry it writes into.

use {
    super::{GroupEntry, PendingInjection},
    crate::barrier::persistence::{CutValue, GroupValue, InjectionStartValue},
    krabka_verified::{
        BarrierRecoveryFoldAction, BarrierRecoveryRecordKind, barrier_recovery_fold_action,
    },
    std::collections::BTreeMap,
};

fn pending_epoch(state: &BTreeMap<String, GroupEntry>, group: &str) -> Option<i64> {
    state
        .get(group)
        .and_then(|entry| entry.pending.as_ref().map(|pending| pending.epoch))
}

/// One decoded `__barrier_state` record. A `None` value is a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateRecord {
    /// A group definition. A tombstone deletes the group.
    Group {
        group: String,
        value: Option<GroupValue>,
    },
    /// The frozen target set of one epoch. A tombstone drops the record of an
    /// epoch that left the retention window.
    InjectionStart {
        group: String,
        epoch: i64,
        value: Option<InjectionStartValue>,
    },
    /// The published offsets of one epoch. A tombstone drops the cut of an
    /// epoch that left the retention window.
    Cut {
        group: String,
        epoch: i64,
        value: Option<CutValue>,
    },
}

/// Fold one replayed record into the state map.
///
/// A group tombstone removes the whole entry. An injection-start tombstone or
/// a cut tombstone removes only what it names.
pub(crate) fn apply_record(state: &mut BTreeMap<String, GroupEntry>, record: StateRecord) {
    match record {
        StateRecord::Group { group, value } => {
            let action = barrier_recovery_fold_action(
                BarrierRecoveryRecordKind::Group,
                value.is_some(),
                0,
                None,
                false,
            );
            match action {
                BarrierRecoveryFoldAction::DefineGroup => {
                    state.entry(group).or_default().definition =
                        value.expect("the verified action requires a group value");
                }
                BarrierRecoveryFoldAction::RemoveGroup => {
                    state.remove(&group);
                }
                _ => unreachable!("the group record kind selects a group action"),
            }
        }
        StateRecord::InjectionStart {
            group,
            epoch,
            value,
        } => {
            let pending_epoch = pending_epoch(state, &group);
            let epoch_already_consumed = state.get(&group).is_some_and(|entry| {
                entry.cuts.contains_key(&epoch) || epoch <= entry.definition.last_epoch
            });
            let action = barrier_recovery_fold_action(
                BarrierRecoveryRecordKind::InjectionStart,
                value.is_some(),
                epoch,
                pending_epoch,
                epoch_already_consumed,
            );
            match action {
                BarrierRecoveryFoldAction::SetPending => {
                    state.entry(group).or_default().pending = Some(PendingInjection {
                        epoch,
                        start: value.expect("the verified action requires an injection value"),
                    });
                }
                BarrierRecoveryFoldAction::ClearPending => {
                    if let Some(entry) = state.get_mut(&group) {
                        entry.pending = None;
                    }
                }
                BarrierRecoveryFoldAction::KeepPending => {}
                _ => unreachable!("the injection record kind selects an injection action"),
            }
        }
        StateRecord::Cut {
            group,
            epoch,
            value,
        } => {
            let pending_epoch = pending_epoch(state, &group);
            let action = barrier_recovery_fold_action(
                BarrierRecoveryRecordKind::Cut,
                value.is_some(),
                epoch,
                pending_epoch,
                false,
            );
            match action {
                BarrierRecoveryFoldAction::UpsertCut { retire_pending } => {
                    let entry = state.entry(group).or_default();
                    entry.cuts.insert(
                        epoch,
                        value.expect("the verified action requires a cut value"),
                    );
                    if retire_pending {
                        entry.pending = None;
                    }
                }
                BarrierRecoveryFoldAction::RemoveCut => {
                    if let Some(entry) = state.get_mut(&group) {
                        entry.cuts.remove(&epoch);
                    }
                }
                _ => unreachable!("the cut record kind selects a cut action"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{StateRecord, apply_record},
        crate::barrier::{
            persistence::CutStatus,
            state::{
                PendingInjection,
                test_support::{cut_value, group_value, start_value},
            },
        },
        assert2::assert,
        std::collections::BTreeMap,
    };

    fn state_with_group(epoch: i64) -> BTreeMap<String, super::GroupEntry> {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::Group {
            group: "orders-cut".to_owned(),
            value: Some(group_value(epoch)),
        });
        state
    }

    #[test]
    fn a_group_record_defines_the_entry() {
        let state = state_with_group(7);
        let entry = state.get("orders-cut").expect("the group is there");
        assert!(entry.definition == group_value(7));
        assert!(entry.is_defined());
        assert!(entry.last_epoch() == 7);
    }

    #[test]
    fn a_group_tombstone_removes_the_whole_entry() {
        let mut state = state_with_group(7);
        apply_record(&mut state, StateRecord::Cut {
            group: "orders-cut".to_owned(),
            epoch: 7,
            value: Some(cut_value(CutStatus::Complete)),
        });
        apply_record(&mut state, StateRecord::Group {
            group: "orders-cut".to_owned(),
            value: None,
        });
        assert!(state.is_empty());
    }

    #[test]
    fn a_cut_record_retires_the_injection_start_of_its_epoch() {
        let mut state = state_with_group(0);
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 1,
            value: Some(start_value(3)),
        });
        assert!(state["orders-cut"].pending.is_some());

        apply_record(&mut state, StateRecord::Cut {
            group: "orders-cut".to_owned(),
            epoch: 1,
            value: Some(cut_value(CutStatus::Complete)),
        });
        let entry = &state["orders-cut"];
        assert!(entry.pending.is_none());
        assert!(entry.cuts.keys().copied().collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn a_retried_start_cannot_reopen_an_epoch_that_has_a_cut() {
        let mut state = BTreeMap::new();
        for record in [
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 7,
                value: Some(start_value(3)),
            },
            StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch: 7,
                value: Some(cut_value(CutStatus::Complete)),
            },
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 7,
                value: Some(start_value(3)),
            },
        ] {
            apply_record(&mut state, record);
        }
        assert!(state["orders-cut"].pending.is_none());
        assert!(state["orders-cut"].cuts.contains_key(&7));
    }

    #[test]
    fn a_retired_cut_cannot_be_reopened_below_the_group_epoch() {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::Group {
            group: "orders-cut".to_owned(),
            value: Some(group_value(9)),
        });
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 7,
            value: Some(start_value(3)),
        });

        assert!(state["orders-cut"].pending.is_none());
        assert!(state["orders-cut"].last_epoch() == 9);
    }

    #[test]
    fn keyed_records_do_not_cross_groups_and_tombstones_do_not_create_entries() {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 2,
            value: Some(start_value(3)),
        });
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "payments-cut".to_owned(),
            epoch: 4,
            value: Some(start_value(3)),
        });
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 2,
            value: None,
        });
        apply_record(&mut state, StateRecord::Cut {
            group: "absent".to_owned(),
            epoch: 1,
            value: None,
        });

        assert!(state["orders-cut"].pending.is_none());
        assert!(
            state["payments-cut"]
                .pending
                .as_ref()
                .map(|pending| pending.epoch)
                == Some(4)
        );
        assert!(!state.contains_key("absent"));
    }

    #[test]
    fn an_injection_start_with_no_cut_stays_pending() {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::Group {
            group: "orders-cut".to_owned(),
            value: Some(group_value(1)),
        });
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 2,
            value: Some(start_value(4)),
        });
        let entry = &state["orders-cut"];
        assert!(
            entry.pending
                == Some(PendingInjection {
                    epoch: 2,
                    start: start_value(4),
                })
        );
        assert!(entry.last_epoch() == 2);
    }

    #[test]
    fn a_tombstone_drops_only_the_epoch_it_names() {
        let mut state = state_with_group(0);
        for epoch in [1, 2, 3] {
            apply_record(&mut state, StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch,
                value: Some(cut_value(CutStatus::Complete)),
            });
        }
        apply_record(&mut state, StateRecord::Cut {
            group: "orders-cut".to_owned(),
            epoch: 1,
            value: None,
        });
        assert!(state["orders-cut"].cuts.keys().copied().collect::<Vec<_>>() == vec![2, 3]);
    }

    #[test]
    fn an_injection_start_tombstone_of_another_epoch_keeps_the_pending_one() {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 9,
            value: Some(start_value(2)),
        });
        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 4,
            value: None,
        });
        assert!(state["orders-cut"].pending.as_ref().map(|p| p.epoch) == Some(9));

        apply_record(&mut state, StateRecord::InjectionStart {
            group: "orders-cut".to_owned(),
            epoch: 9,
            value: None,
        });
        assert!(state["orders-cut"].pending.is_none());
    }

    // The newest group record sits after the cut records of older epochs in
    // the log, so the fold creates the entry for a cut it sees first.
    #[test]
    fn a_cut_that_precedes_the_group_record_survives_the_fold() {
        let mut state = BTreeMap::new();
        apply_record(&mut state, StateRecord::Cut {
            group: "orders-cut".to_owned(),
            epoch: 6,
            value: Some(cut_value(CutStatus::Partial)),
        });
        assert!(!state["orders-cut"].is_defined());

        apply_record(&mut state, StateRecord::Group {
            group: "orders-cut".to_owned(),
            value: Some(group_value(6)),
        });
        let entry = &state["orders-cut"];
        assert!(entry.is_defined());
        assert!(entry.cuts.contains_key(&6));
    }
}
