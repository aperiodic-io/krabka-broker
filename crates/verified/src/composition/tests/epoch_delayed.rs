use {
    super::{ProducerDecision, epoch_handoff_survives_delayed_completions, recovered_window_row},
    crate::transaction::InitProducerIdIdentityDecision,
    assert2::assert,
    proptest::prelude::*,
};

#[test]
fn delayed_callbacks_cannot_replace_coordinates_with_a_same_sequence_alias() {
    let first = recovered_window_row(8, 2, 0);
    let mut alias = recovered_window_row(60, 2, 0);
    alias.producer_epoch = 6;
    let delayed = [alias; 32];
    for hwm in [10, 11, 62, 63] {
        let result = epoch_handoff_survives_delayed_completions(
            70,
            hwm,
            7,
            &[],
            first,
            &delayed,
            (i32::MAX - 1, 2),
        );
        assert!(result.0 == InitProducerIdIdentityDecision::Retry);
        assert!(result.1 == vec![(false, ProducerDecision::Fenced); 32]);
        assert!(result.2 == ProducerDecision::Duplicate { retained: 4 });
        assert!(result.3 == (8, 11, hwm >= 11));
    }
}

#[test]
fn zero_callbacks_and_maximum_frontier_preserve_the_new_batch_retry() {
    for epoch in [0, i16::MAX - 2] {
        let first = super::maximum_epoch_row(epoch);
        let delayed = [first; 6];
        for callbacks in [&[][..], &delayed[..]] {
            for hwm in [i64::MAX - 1, i64::MAX] {
                let result = epoch_handoff_survives_delayed_completions(
                    i64::MAX,
                    hwm,
                    epoch,
                    &[],
                    first,
                    callbacks,
                    (-1, -1),
                );
                assert!(result.0 == InitProducerIdIdentityDecision::Retry);
                assert!(result.1 == vec![(false, ProducerDecision::Fenced); callbacks.len()]);
                assert!(result.2 == ProducerDecision::Duplicate { retained: 4 });
                assert!(result.3 == (i64::MAX - 1, i64::MAX, hwm == i64::MAX));
            }
        }
    }
}

proptest! {
    #[test]
    fn callback_order_and_physical_offsets_cannot_undo_epoch_handoff(
        epoch in 0_i16..i16::MAX - 1,
        first_base in 0_i64..100,
        delta in 0_i32..8,
        sequence in 0_i32..=i32::MAX,
        old_count in 0_usize..=5,
        raw in prop::collection::vec((0_i64..100, 0_i32..8, 0_i32..=i32::MAX, any::<u16>()), 0..64),
        request in (any::<i32>(), any::<i32>()), hwm in 0_i64..=110,
    ) {
        let mut first = recovered_window_row(first_base, delta, sequence);
        first.producer_epoch = epoch;
        let old = super::epoch_rows(old_count, epoch, sequence);
        let delayed: Vec<_> = raw.into_iter().map(|(base, delta, sequence, value)| {
            let mut row = recovered_window_row(base, delta, sequence);
            row.producer_epoch = i16::try_from(u32::from(value) % (u32::try_from(epoch).unwrap() + 1)).unwrap();
            row
        }).collect();
        let result = epoch_handoff_survives_delayed_completions(110, hwm, epoch, &old, first, &delayed, request);
        assert!(result.0 == InitProducerIdIdentityDecision::Retry);
        assert!(result.1 == vec![(false, ProducerDecision::Fenced); delayed.len()]);
        assert!(result.2 == ProducerDecision::Duplicate { retained: 4 });
        assert!(result.3 == (first_base, first_base + i64::from(delta) + 1, hwm > first_base + i64::from(delta)));
    }
}
