//! The stateright [`Model`] implementation for [`CompactModel`]: the initial
//! state, the action alphabet, the transition relation that runs one
//! [`compact_pass`], the pass rules as `always` properties, and the
//! non-vacuity witnesses. The block lives alone in this file because a trait
//! implementation cannot be split across modules.

use {
    super::{
        invariants::{Invariant, live_keys},
        pass::compact_pass,
        state::{Cleaner, CompactAction, CompactModel, CompactState, Entry, EntryKind},
    },
    crate::compact::TxnDataState,
    stateright::{Model, Property},
};

impl CompactState {
    /// Whether `rule` holds for the pass that produced this state. A state
    /// that no pass produced holds every rule.
    fn pass_obeys(&self, rule: Invariant) -> bool {
        self.last_pass
            .as_ref()
            .is_none_or(|input| rule.holds(input, &self.log, self.clock))
    }

    /// Whether the pass that produced this state deleted an entry that
    /// `select` picks out.
    fn pass_removed(&self, select: fn(&Entry) -> bool) -> bool {
        self.last_pass.as_ref().is_some_and(|input| {
            input.iter().filter(|e| select(e)).count()
                > self.log.iter().filter(|e| select(e)).count()
        })
    }
}

impl Model for CompactModel {
    type Action = CompactAction;
    type State = CompactState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CompactState {
            log: vec![],
            clock: 0,
            last_pass: None,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Cap log growth so the reachable space stays bounded. The per-position
        // alphabet is deliberately minimal: `retain_decision` branches only on
        // value-*presence* (live vs tombstone) and txn-state, never on the value
        // byte or commit/abort, so we fix the data value to 0 and emit a single
        // marker kind per producer. Collapsing those two provably-irrelevant
        // dimensions cuts the alphabet 10 → 6 symbols (the dominant state-space
        // driver) with zero loss of decision coverage. Commit and abort markers
        // carry different control keys, so two commit markers are exactly the
        // pair a control-key dedup would collapse.
        if s.log.len() < self.max_len {
            for key in 0u8..=1 {
                actions.push(CompactAction::AppendData(key, 0));
                actions.push(CompactAction::AppendTombstone(key));
            }
            for pid in 0u8..=1 {
                actions.push(CompactAction::AppendCommit(pid));
            }
        }
        for dt in [1i64, 2] {
            if s.clock + dt <= self.max_clock {
                actions.push(CompactAction::Tick(dt));
            }
        }
        actions.push(CompactAction::Compact);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = CompactState {
            last_pass: None,
            ..last.clone()
        };
        match action {
            CompactAction::AppendData(key, value) => s.log.push(Entry {
                key: Some(key),
                kind: EntryKind::Data { value: Some(value) },
                horizon: None,
            }),
            CompactAction::AppendTombstone(key) => s.log.push(Entry {
                key: Some(key),
                kind: EntryKind::Data { value: None },
                horizon: None,
            }),
            CompactAction::AppendCommit(pid) => s.log.push(Entry {
                key: None,
                kind: EntryKind::Marker {
                    producer_id: pid,
                    commit: true,
                },
                horizon: None,
            }),
            CompactAction::Tick(dt) => s.clock += dt,
            CompactAction::Compact => {
                s.log = compact_pass(&last.log, last.clock, self.cleaner);
                s.last_pass = Some(last.log.clone());
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // ---- Safety: the five pass rules (see `invariants.rs`). ----
            Property::always("control_not_deduped", |_, s: &CompactState| {
                s.pass_obeys(Invariant::ControlNotDeduped)
            }),
            Property::always("marker_data_precedence", |_, s: &CompactState| {
                s.pass_obeys(Invariant::MarkerDataPrecedence)
            }),
            Property::always("tombstone_aging", |_, s: &CompactState| {
                s.pass_obeys(Invariant::TombstoneAging)
            }),
            Property::always("idempotent_stamp", |_, s: &CompactState| {
                s.pass_obeys(Invariant::IdempotentStamp)
            }),
            Property::always("no_data_loss", |_, s: &CompactState| {
                s.pass_obeys(Invariant::NoDataLoss)
            }),
            // ---- Non-vacuity witnesses. ----
            // A delete-horizon was stamped and the entry retained.
            Property::sometimes("horizon_stamped", |_, s: &CompactState| {
                s.log.iter().any(|e| e.horizon.is_some())
            }),
            // Two markers coexist in one log, the pair a control-key dedup
            // would have collapsed to one.
            Property::sometimes("two_markers_coexist", |_, s: &CompactState| {
                s.log
                    .iter()
                    .filter(|e| matches!(e.kind, EntryKind::Marker { .. }))
                    .count()
                    >= 2
            }),
            // A marker is retained because the pass reads data of its
            // transaction in front of it.
            Property::sometimes(
                "marker_retained_for_observed_data",
                |_, s: &CompactState| {
                    Cleaner::marker_states(&s.log)
                        .into_values()
                        .any(|state| state == TxnDataState::DataSurvives)
                },
            ),
            // A pass ages a marker out while its producer still has live data
            // in the log: the case a producer-wide survivor rule would have
            // kept forever, and `marker_data_precedence` leaves to the
            // marker's own transaction.
            Property::sometimes("marker_ages_out_beside_live_data", |_, s: &CompactState| {
                let markers_of = |log: &[Entry], pid: u8| {
                    log.iter()
                            .filter(|e| {
                                matches!(e.kind, EntryKind::Marker { producer_id, .. } if producer_id == pid)
                            })
                            .count()
                };
                s.last_pass.as_ref().is_some_and(|input| {
                    live_keys(&s.log)
                        .into_iter()
                        .any(|pid| markers_of(input, pid) > markers_of(&s.log, pid))
                })
            }),
            // A retained tombstone reaches an elapsed horizon.
            Property::sometimes("tombstone_horizon_elapsed", |_, s: &CompactState| {
                s.log.iter().any(|e| {
                    matches!(e.kind, EntryKind::Data { value: None }) && e.horizon_elapsed(s.clock)
                })
            }),
            // A retained marker reaches an elapsed horizon.
            Property::sometimes("marker_horizon_elapsed", |_, s: &CompactState| {
                s.log.iter().any(|e| {
                    matches!(e.kind, EntryKind::Marker { .. }) && e.horizon_elapsed(s.clock)
                })
            }),
            // A pass ages a tombstone out, so `tombstone_aging` is exercised on
            // a deletion and not only on retentions.
            Property::sometimes("pass_ages_out_tombstone", |_, s: &CompactState| {
                s.pass_removed(|e| matches!(e.kind, EntryKind::Data { value: None }))
            }),
            // A pass ages a marker out, so `control_not_deduped` is exercised
            // on a deletion and not only on retentions.
            Property::sometimes("pass_ages_out_marker", |_, s: &CompactState| {
                s.pass_removed(|e| matches!(e.kind, EntryKind::Marker { .. }))
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.len() <= self.max_len && s.clock <= self.max_clock
    }
}
