//! The pure idempotent-producer dedup and ordering decision, plus the async
//! `check` that wraps it in the per-partition lock.
//!
//! `check_pure` classifies an incoming batch as an append, a duplicate, an
//! out-of-order sequence, or a fenced epoch from the tracked entry alone, so
//! the exhaustive state model and the property test can drive the KIP-98
//! classification without a broker.

use {
    super::{ProducerEntry, ProducerState, RetainedBatch, entry::NUM_BATCHES_TO_RETAIN},
    crate::{api_catalog::UnstableApiVersions, partition::LogOffset},
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_verified::{
        ProducerDecision, ProducerEntryFacts, RetainedSequenceRange, producer_decision,
    },
};

/// How the tracker answers one idempotent-producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Append the batch.
    Append,
    /// The batch repeats a retained batch that starts at `base_offset`.
    Duplicate { base_offset: LogOffset },
    /// Kafka's `OUT_OF_ORDER_SEQUENCE_NUMBER`.
    OutOfOrder,
    /// Kafka's `INVALID_PRODUCER_EPOCH`: the batch's epoch is stale.
    Fenced,
}

/// The decision for one batch, and for a duplicate the batch it repeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checked {
    pub decision: Decision,
    /// The retained batch a [`Decision::Duplicate`] repeats. `None` for every
    /// other decision.
    pub duplicate: Option<RetainedBatch>,
}

/// What the sequence check reads about the partition beside the producer's
/// own entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceContext {
    /// Kafka's `ProducerStateManager.mapEndOffset() == 0`: no record has ever
    /// been appended to the partition's log.
    pub log_empty: bool,
    /// Kafka's `unstable.api.versions.enable`. Kafka trunk's empty-log rule
    /// (KAFKA-15591), which Kafka 4.3.1 does not have, applies only when it is
    /// enabled.
    pub unstable: UnstableApiVersions,
}

impl SequenceContext {
    /// A partition that holds records, on a broker serving Kafka 4.3.1.
    #[cfg(test)]
    pub(crate) const RELEASED: Self = Self {
        log_empty: false,
        unstable: UnstableApiVersions::Disabled,
    };
}

/// Pure idempotent-producer dedup/ordering decision.
///
/// The async `check` is a thin lock-acquiring wrapper over this function. The
/// decision is a separate function so that the tests can exhaustively test and
/// property-test it in isolation. See `producer_state_model.rs`.
#[cfg(test)]
pub(crate) fn check_pure(
    entry: Option<&ProducerEntry>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> Decision {
    check_retained(
        entry,
        SequenceContext::RELEASED,
        producer_epoch,
        base_sequence,
        last_offset_delta,
    )
    .decision
}

/// [`check_pure`], with the batch a duplicate repeats.
///
/// The whole classification, including Kafka's search of the producer's five
/// retained batches (`ProducerStateEntry.findDuplicateBatch`) ahead of the
/// sequence check, is the proved [`producer_decision`]. This function only
/// hands it the entry's epoch, last sequence and retained sequence ranges and
/// the partition's [`SequenceContext`], and maps a duplicate's index back to
/// the retained batch it names.
pub(crate) fn check_retained(
    entry: Option<&ProducerEntry>,
    context: SequenceContext,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
) -> Checked {
    let batches: [Option<RetainedBatch>; NUM_BATCHES_TO_RETAIN] = entry.map_or(
        [None; NUM_BATCHES_TO_RETAIN],
        ProducerEntry::retained_batches,
    );
    let ranges = batches.map(|slot| {
        slot.map(|batch| RetainedSequenceRange {
            base_sequence: batch.base_sequence,
            last_sequence: batch.last_sequence,
        })
    });
    let facts = entry.map(|entry| ProducerEntryFacts {
        epoch: entry.epoch,
        last_sequence: entry.last_sequence,
    });
    let decision = match producer_decision(
        facts,
        &ranges,
        producer_epoch,
        base_sequence,
        last_offset_delta,
        context.log_empty,
        context.unstable == UnstableApiVersions::Enabled,
    ) {
        ProducerDecision::Append => Decision::Append,
        ProducerDecision::Duplicate { retained } => {
            let batch = batches.get(retained).copied().flatten().expect(
                "producer_decision proves a duplicate index names an occupied retained slot",
            );
            return Checked {
                decision: Decision::Duplicate {
                    base_offset: batch.base_offset,
                },
                duplicate: Some(batch),
            };
        }
        ProducerDecision::OutOfOrder => Decision::OutOfOrder,
        ProducerDecision::Fenced => Decision::Fenced,
    };
    Checked {
        decision,
        duplicate: None,
    }
}

impl ProducerState {
    /// Decide whether to append the incoming batch.
    ///
    /// `base_sequence` is the wire `base_sequence`. `last_offset_delta` is
    /// the batch's `last_offset_delta` field. Together they imply the
    /// batch's `last_sequence = base_sequence + last_offset_delta`.
    #[cfg(test)]
    pub async fn check(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Decision {
        self.check_batch(
            topic,
            partition,
            SequenceContext::RELEASED,
            (producer_id, producer_epoch),
            (base_sequence, last_offset_delta),
        )
        .await
        .decision
    }

    /// [`Self::check`] against the partition's [`SequenceContext`], with the
    /// retained batch a duplicate repeats, read under the same lock.
    pub async fn check_batch(
        &self,
        topic: &str,
        partition: PartitionIndex,
        context: SequenceContext,
        (producer_id, producer_epoch): (i64, i16),
        (base_sequence, last_offset_delta): (i32, i32),
    ) -> Checked {
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        check_retained(
            s.entries.get(&ProducerId(producer_id)),
            context,
            producer_epoch,
            base_sequence,
            last_offset_delta,
        )
    }
}

#[cfg(test)]
mod fuzz {
    use {
        super::{Decision, ProducerEntry, check_pure},
        proptest::prelude::*,
        std::collections::HashMap,
    };

    proptest! {
        /// Large-N randomized submit sequences over `check_pure`.
        ///
        /// The accepted-append log per epoch is a contiguous, duplicate-free,
        /// monotonic prefix. A lower epoch is fenced. A higher epoch starts a
        /// new prefix at sequence 0 and rejects any other sequence. This test complements the exhaustive
        /// `producer_state_model` at a scale the BFS cannot reach: epoch 0..6,
        /// `base_seq` 0..200, and up to 400 ops.
        #[test]
        fn idempotent_log_invariants(
            ops in proptest::collection::vec(
                (0i16..6, 0i32..200), // (producer_epoch, base_sequence)
                0..400usize,
            )
        ) {
            let mut entry: Option<ProducerEntry> = None;
            let mut next_offset: i64 = 0;
            // Reference: per-epoch highest accepted sequence (must stay contiguous).
            let mut hi: HashMap<i16, i32> = HashMap::new();
            for (epoch, base_seq) in ops {
                let d = check_pure(entry.as_ref(), epoch, base_seq, 0);
                match d {
                    Decision::Append => {
                        if let Some(e) = &entry {
                            if epoch == e.epoch {
                                prop_assert_eq!(
                                    base_seq,
                                    e.last_sequence + 1,
                                    "same-epoch Append must be contiguous"
                                );
                            } else {
                                prop_assert!(epoch > e.epoch, "Append epoch must be fresh");
                                prop_assert_eq!(base_seq, 0, "a new epoch must start at 0");
                            }
                        }
                        // Per-epoch contiguity: an accepted seq for a fresh epoch
                        // starts the prefix; a same-epoch accept extends it by 1.
                        if let Some(p) = hi.get(&epoch).copied() {
                            prop_assert_eq!(
                                base_seq,
                                p + 1,
                                "accepted sequence must extend the per-epoch prefix"
                            );
                        }
                        hi.insert(epoch, base_seq);
                        entry = Some(ProducerEntry {
                            epoch,
                            last_sequence: base_seq,
                            last_offset: next_offset,
                            base_offset: next_offset,
                            last_timestamp: 0,
                            entry_timestamp: 0,
                            current_txn_first_offset: None,
                            earlier: super::super::NO_EARLIER_BATCHES,
                        });
                        next_offset += 1;
                    }
                    Decision::Duplicate { .. } => {
                        let e = entry.as_ref().expect("Duplicate implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq == e.last_sequence,
                            "single-record duplicate must match the committed sequence"
                        );
                    }
                    Decision::OutOfOrder => {
                        let e = entry.as_ref().expect("OutOfOrder implies an entry");
                        if epoch == e.epoch {
                            prop_assert!(
                                base_seq != e.last_sequence && base_seq != e.last_sequence + 1,
                                "OutOfOrder must be neither a retry nor the next sequence"
                            );
                        } else {
                            prop_assert!(
                                epoch > e.epoch && base_seq != 0,
                                "a new epoch is out of order only when it does not start at 0"
                            );
                        }
                    }
                    Decision::Fenced => {
                        let e = entry.as_ref().expect("Fenced implies an entry");
                        prop_assert!(epoch < e.epoch, "Fenced must be a stale epoch");
                    }
                }
            }
        }
    }
}
