use {super::*, assert2::assert};

#[test]
fn sequence_arithmetic_matches_kafka_wraparound() {
    // (sequence, step, incremented, decremented), from Kafka's
    // `DefaultRecordBatch.incrementSequence` / `decrementSequence`.
    let cases = [
        (0, 0, 0, 0),
        (5, 3, 8, 2),
        (i32::MAX, 1, 0, i32::MAX - 1),
        (i32::MAX - 1, 3, 1, i32::MAX - 4),
        (0, 1, 1, i32::MAX),
        (2, 5, 7, i32::MAX - 2),
        (i32::MAX, i32::MAX, i32::MAX - 1, 0),
        // Outside Kafka's nonnegative domain the result is still the
        // exact value modulo `2^31`, so a `-1` (no sequence) advances to 0.
        (-1, 1, 0, i32::MAX - 1),
        (i32::MIN, -1, i32::MAX, 1),
        (i32::MIN, i32::MIN, 0, 0),
        (5, i32::MIN, 5, 5),
        (-3, i32::MAX, i32::MAX - 3, i32::MAX - 1),
    ];
    for (sequence, step, incremented, decremented) in cases {
        assert!(increment_sequence(sequence, step) == incremented);
        assert!(decrement_sequence(sequence, step) == decremented);
    }
}

#[test]
fn producer_decision_covers_all_outcomes() {
    // (label, entry, epoch, base sequence, last offset delta, decision)
    let cases = [
        (
            "no entry, any sequence",
            None,
            0,
            17,
            0,
            ProducerDecision::Append,
        ),
        (
            "lower epoch",
            Some(ENTRY),
            1,
            4,
            2,
            ProducerDecision::Fenced,
        ),
        (
            "higher epoch at 0",
            Some(ENTRY),
            3,
            0,
            0,
            ProducerDecision::Append,
        ),
        (
            "higher epoch continues the sequence",
            Some(ENTRY),
            3,
            7,
            0,
            ProducerDecision::OutOfOrder,
        ),
        (
            "higher epoch repeats the last batch",
            Some(ENTRY),
            3,
            4,
            2,
            ProducerDecision::OutOfOrder,
        ),
        (
            "same epoch, next sequence",
            Some(ENTRY),
            2,
            7,
            0,
            ProducerDecision::Append,
        ),
        (
            "same epoch, last batch again",
            Some(ENTRY),
            2,
            4,
            2,
            ProducerDecision::Duplicate { retained: 4 },
        ),
        (
            "same epoch, an earlier retained batch again",
            Some(ENTRY),
            2,
            1,
            1,
            ProducerDecision::Duplicate { retained: 2 },
        ),
        (
            "same epoch, the oldest retained batch again",
            Some(ENTRY),
            2,
            0,
            0,
            ProducerDecision::Duplicate { retained: 1 },
        ),
        (
            "same epoch, a sub-range of a retained batch",
            Some(ENTRY),
            2,
            1,
            0,
            ProducerDecision::OutOfOrder,
        ),
        (
            "same epoch, gap",
            Some(ENTRY),
            2,
            9,
            0,
            ProducerDecision::OutOfOrder,
        ),
        (
            "same epoch, same base sequence but different delta",
            Some(ENTRY),
            2,
            4,
            3,
            ProducerDecision::OutOfOrder,
        ),
    ];
    for (label, entry, epoch, base_sequence, delta, expected) in cases {
        assert!(
            producer_decision(entry, &RETAINED, epoch, base_sequence, delta, false, false)
                == expected,
            "case: {label}"
        );
    }
}

/// Kafka trunk's KAFKA-15591 rule, which Kafka 4.3.1 does not have: a
/// producer with no entry must start at sequence 0 on a partition that has
/// never held a record. Every other input keeps the 4.3.1 decision.
#[test]
fn a_producer_with_no_state_on_an_empty_log_starts_at_zero_under_trunk_rules() {
    // (label, entry, base sequence, log empty, trunk rules, decision)
    let cases = [
        (
            "never appended, no entry, sequence 0",
            None,
            0,
            true,
            true,
            ProducerDecision::Append,
        ),
        (
            "never appended, no entry, sequence 7",
            None,
            7,
            true,
            true,
            ProducerDecision::OutOfOrder,
        ),
        (
            "has records, no entry, sequence 7",
            None,
            7,
            false,
            true,
            ProducerDecision::Append,
        ),
        (
            "has records, entry expired, sequence 0",
            None,
            0,
            false,
            true,
            ProducerDecision::Append,
        ),
        (
            "never appended, no entry, sequence 7, Kafka 4.3.1",
            None,
            7,
            true,
            false,
            ProducerDecision::Append,
        ),
        (
            "never appended, an entry continues its sequence",
            Some(ENTRY),
            7,
            true,
            true,
            ProducerDecision::Append,
        ),
        (
            "never appended, an entry at a gap",
            Some(ENTRY),
            9,
            true,
            true,
            ProducerDecision::OutOfOrder,
        ),
    ];
    for (label, entry, base_sequence, log_empty, trunk_rules, expected) in cases {
        assert!(
            producer_decision(
                entry,
                &RETAINED,
                ENTRY.epoch,
                base_sequence,
                0,
                log_empty,
                trunk_rules,
            ) == expected,
            "case: {label}"
        );
    }
}

#[test]
fn sequence_continues_across_wraparound() {
    let entry = ProducerEntryFacts {
        epoch: 0,
        last_sequence: i32::MAX,
    };
    let retained = [Some(RetainedSequenceRange {
        base_sequence: i32::MAX - 1,
        last_sequence: i32::MAX,
    })];
    assert!(
        producer_decision(Some(entry), &retained, 0, 0, 3, false, false)
            == ProducerDecision::Append
    );
    assert!(
        producer_decision(Some(entry), &retained, 0, i32::MAX - 1, 1, false, false)
            == ProducerDecision::Duplicate { retained: 0 }
    );
    assert!(
        producer_decision(Some(entry), &[], 0, i32::MAX - 1, 1, false, false)
            == ProducerDecision::OutOfOrder
    );
}
