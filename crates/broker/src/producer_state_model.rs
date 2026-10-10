//! Exhaustive stateright enumeration of the idempotent-producer tracker: the
//! host's retained-batch bookkeeping ([`ProducerEntry::retained_batches`] and
//! [`super::entry::earlier_after_completion`]) around the proved
//! `producer_decision` kernel, as `check_retained` runs them. There is one
//! producer id per partition, and the broker serializes its requests, so the
//! model enumerates every bounded submit sequence.
//!
//! The oracle is independent of the host's representation. The state carries a
//! ghost [`Window`]: the producer's epoch, its last accepted sequence, and the
//! sequence ranges of the batches it accepted at that epoch, newest last, cut
//! to Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN` (five). Every submit
//! is classified by the host and by [`kafka_decision`] over the window, and a
//! disagreement is recorded in the state, where the `always` property
//! `decision_matches_kafka` turns it into a counterexample path.
//!
//! Batches hold one or two records (`last_offset_delta` 0 or 1), so retained
//! ranges differ in shape and a retry must match both ends of one. A batch is
//! placed at offset `base_sequence`, which keeps offsets out of the
//! fingerprint's growth while still giving every batch in the window a distinct
//! offset that a duplicate answer must echo.
//!
//! See the design spec `crates/broker/docs/transaction-coordinator-design.md`.

use std::collections::VecDeque;

use krabka_verified::increment_sequence;
use stateright::{Model, Property};

use super::{
    Decision, ProducerEntry, RetainedBatch,
    decision::{Checked, SequenceContext, check_retained},
    entry::{EarlierBatches, NUM_BATCHES_TO_RETAIN},
};
use crate::{model_check::check_model, partition::LogOffset};

const MAX_STATES: usize = 2_000_000;
const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// The counts grew (from 13 and 92) when the model started to carry the host's
// retained batches and two-record batches, and to fill the five-batch window:
// the old model kept `earlier` empty, so only a retry of the last batch was
// ever a duplicate.
const PINNED_UNIQUE_STATES_BASIC: usize = 731;
const PINNED_UNIQUE_STATES_WIDE: usize = 4_233;

struct ProducerModel {
    max_epoch: i16,
    max_seq: i32,
}

/// A retained batch as the fingerprint holds it: its sequence range. Its
/// offsets follow from the range (see the module docs).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Range {
    base: i32,
    last: i32,
}

impl Range {
    fn batch(self) -> RetainedBatch {
        RetainedBatch {
            base_sequence: self.base,
            last_sequence: self.last,
            base_offset: LogOffset::from(self.base),
            last_offset: LogOffset::from(self.last),
            timestamp: 0,
        }
    }

    fn of(batch: RetainedBatch) -> Self {
        Self {
            base: batch.base_sequence,
            last: batch.last_sequence,
        }
    }
}

/// The host's tracked entry, projected onto what the fingerprint can hash.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct HostEntry {
    epoch: i16,
    last: Range,
    earlier: [Option<Range>; NUM_BATCHES_TO_RETAIN - 1],
}

impl HostEntry {
    fn entry(&self) -> ProducerEntry {
        let last = self.last.batch();
        ProducerEntry {
            epoch: self.epoch,
            last_sequence: last.last_sequence,
            last_offset: last.last_offset,
            base_offset: last.base_offset,
            last_timestamp: 0,
            entry_timestamp: 0,
            current_txn_first_offset: None,
            earlier: self.earlier.map(|slot| slot.map(Range::batch)),
        }
    }

    /// The entry after the host accepts `batch` at `epoch`: exactly what
    /// `ProducerState::commit` stores.
    fn after_append(existing: Option<&Self>, epoch: i16, batch: Range) -> Self {
        let (_, earlier): (bool, EarlierBatches) = super::entry::earlier_after_completion(
            existing.map(HostEntry::entry),
            epoch,
            batch.batch(),
        );
        Self {
            epoch,
            last: Range {
                base: batch.base,
                last: increment_sequence(batch.base, batch.last - batch.base),
            },
            earlier: earlier.map(|slot| slot.map(Range::of)),
        }
    }
}

/// The ghost: what Kafka's producer state holds, written the plain way.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Window {
    epoch: i16,
    last_sequence: i32,
    /// The accepted batches at `epoch`, oldest first, at most five.
    retained: VecDeque<Range>,
}

/// Kafka's answer to one batch, with a duplicate's retained range.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Answer {
    Append,
    Duplicate(Range),
    OutOfOrder,
    Fenced,
}

impl Answer {
    fn of(checked: Checked) -> Self {
        match checked.decision {
            Decision::Append => Self::Append,
            Decision::Duplicate { base_offset } => {
                let batch = checked
                    .duplicate
                    .expect("a duplicate names its retained batch");
                assert2::assert!(batch.base_offset == base_offset);
                Self::Duplicate(Range::of(batch))
            }
            Decision::OutOfOrder => Self::OutOfOrder,
            Decision::Fenced => Self::Fenced,
        }
    }
}

/// Kafka's classification of `batch` at `epoch` against `window`:
/// `UnifiedLog.analyzeAndValidateProducerState` looks the batch up among the
/// retained batches first (`ProducerStateEntry.findDuplicateBatch`, same epoch
/// only), then `ProducerAppendInfo.checkProducerEpoch` and `checkSequence`
/// decide the rest.
fn kafka_decision(window: Option<&Window>, epoch: i16, batch: Range) -> Answer {
    let Some(window) = window else {
        return Answer::Append;
    };
    if epoch == window.epoch && window.retained.contains(&batch) {
        return Answer::Duplicate(batch);
    }
    if epoch < window.epoch {
        Answer::Fenced
    } else if epoch > window.epoch {
        if batch.base == 0 {
            Answer::Append
        } else {
            Answer::OutOfOrder
        }
    } else if batch.base == window.last_sequence + 1 {
        Answer::Append
    } else {
        Answer::OutOfOrder
    }
}

/// A submit whose outcome a `sometimes` witness looks for. It is recorded only
/// on the transition that produced it and cleared by the next, so it adds at
/// most a few states per reachable entry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Witness {
    /// A retry of the last accepted batch answered as a duplicate.
    DuplicateOfLast,
    /// A retry of a retained batch other than the last answered as a
    /// duplicate: only the five-batch window can answer it.
    DuplicateOfOlder,
    /// A retry of the oldest of five retained batches answered as a
    /// duplicate.
    DuplicateOfOldest,
    /// With the five-batch window full, a retry of sequences below it, which
    /// only an evicted batch can have held, was refused as out of order
    /// rather than deduplicated.
    RetryBelowWindowRefused,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ProdState {
    host: Option<HostEntry>,
    window: Option<Window>,
    witness: Option<Witness>,
    /// The first submit on which the host and Kafka disagreed. It is only ever
    /// set by a defect, so it costs no states on a correct host.
    violation: Option<(i16, Range, Answer, Answer)>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ProdAction {
    /// Submit the batch `range` at `epoch`.
    Submit(i16, Range),
}

impl Model for ProducerModel {
    type State = ProdState;
    type Action = ProdAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProdState {
            host: None,
            window: None,
            witness: None,
            violation: None,
        }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        for epoch in 0..=self.max_epoch {
            for base in 0..=self.max_seq {
                for delta in 0..=1 {
                    actions.push(ProdAction::Submit(
                        epoch,
                        Range {
                            base,
                            last: base + delta,
                        },
                    ));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let ProdAction::Submit(epoch, batch) = action;
        let entry = last.host.as_ref().map(HostEntry::entry);
        let host = Answer::of(check_retained(
            entry.as_ref(),
            SequenceContext::RELEASED,
            epoch,
            batch.base,
            batch.last - batch.base,
        ));
        let kafka = kafka_decision(last.window.as_ref(), epoch, batch);
        let mut s = ProdState {
            witness: None,
            ..last.clone()
        };
        if host != kafka {
            s.violation.get_or_insert((epoch, batch, host, kafka));
            return Some(s);
        }
        match host {
            Answer::Append => {
                s.host = Some(HostEntry::after_append(last.host.as_ref(), epoch, batch));
                let mut window = last
                    .window
                    .clone()
                    .filter(|window| window.epoch == epoch)
                    .unwrap_or(Window {
                        epoch,
                        last_sequence: -1,
                        retained: VecDeque::new(),
                    });
                window.last_sequence = batch.last;
                window.retained.push_back(batch);
                if window.retained.len() > NUM_BATCHES_TO_RETAIN {
                    window.retained.pop_front();
                }
                s.window = Some(window);
                Some(s)
            }
            Answer::Duplicate(range) => {
                let retained = &last.window.as_ref()?.retained;
                s.witness = Some(if retained.back() == Some(&range) {
                    Witness::DuplicateOfLast
                } else if retained.len() == NUM_BATCHES_TO_RETAIN
                    && retained.front() == Some(&range)
                {
                    Witness::DuplicateOfOldest
                } else {
                    Witness::DuplicateOfOlder
                });
                Some(s)
            }
            Answer::OutOfOrder
                if last.window.as_ref().is_some_and(|w| {
                    w.epoch == epoch
                        && w.retained.len() == NUM_BATCHES_TO_RETAIN
                        && w.retained
                            .front()
                            .is_some_and(|oldest| batch.last < oldest.base)
                }) =>
            {
                s.witness = Some(Witness::RetryBelowWindowRefused);
                Some(s)
            }
            Answer::OutOfOrder | Answer::Fenced => None,
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: the host answers every submit as Kafka does over the
            // ghost window. That covers dedup of any of the five retained
            // batches, contiguity within an epoch, a fresh start at a new
            // epoch, and fencing of a stale epoch.
            Property::always("decision_matches_kafka", |_, s: &ProdState| {
                s.violation.is_none()
            }),
            Property::sometimes("duplicate_of_last_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfLast)
            }),
            Property::sometimes("duplicate_of_older_retained_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfOlder)
            }),
            Property::sometimes("duplicate_of_oldest_retained_batch", |_, s: &ProdState| {
                s.witness == Some(Witness::DuplicateOfOldest)
            }),
            Property::sometimes("retry_below_window_refused", |_, s: &ProdState| {
                s.witness == Some(Witness::RetryBelowWindowRefused)
            }),
            Property::sometimes("can_bump_epoch", |_, s: &ProdState| {
                s.window.as_ref().is_some_and(|w| w.epoch >= 1)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.window
            .as_ref()
            .is_none_or(|w| w.epoch <= self.max_epoch && w.last_sequence <= self.max_seq)
    }
}

fn run(model: ProducerModel, label: &str, pinned_unique_states: usize) {
    check_model(
        model,
        label,
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        pinned_unique_states,
    );
}

#[test]
fn producer_basic() {
    // Six single-record batches fill the five-batch window and evict one.
    run(
        ProducerModel {
            max_epoch: 1,
            max_seq: 6,
        },
        "producer_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn producer_wide() {
    run(
        ProducerModel {
            max_epoch: 3,
            max_seq: 9,
        },
        "producer_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
