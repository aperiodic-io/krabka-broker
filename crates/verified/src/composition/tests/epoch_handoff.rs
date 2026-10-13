use {
    super::{
        ProducerDecision, epoch_handoff_distinguishes_identity_and_data_retry, recovered_window_row,
    },
    crate::transaction::InitProducerIdIdentityDecision,
    assert2::assert,
    proptest::prelude::*,
};

#[test]
fn allocating_a_rotated_identity_does_not_update_the_old_partition_entry() {
    use crate::{
        producer::{ProducerEntryFacts, RetainedSequenceRange, producer_decision},
        transaction::next_producer_identity,
    };

    let epoch = i16::MAX - 1;
    assert!(next_producer_identity(true, false, 42, epoch, Some(43)) == Some((43, 0)));
    let retained = [Some(RetainedSequenceRange {
        base_sequence: 0,
        last_sequence: 0,
    })];
    let decision = producer_decision(
        Some(ProducerEntryFacts {
            epoch,
            last_sequence: 0,
        }),
        &retained,
        epoch,
        0,
        0,
        false,
        false,
    );
    assert!(decision == ProducerDecision::Duplicate { retained: 0 });
    // The transition trusts the supplied allocation; freshness is a host fact.
    assert!(next_producer_identity(true, false, 42, epoch, Some(42)) == Some((42, 0)));
}

#[test]
fn identity_retry_is_accepted_while_old_data_is_fenced() {
    for epoch in [0, 7, i16::MAX - 2] {
        let mut first = recovered_window_row(8, 2, 0);
        first.producer_epoch = epoch;
        let old = super::epoch_rows(5, epoch, 0);
        for request in [(i32::MAX - 1, 2), (0, 0), (-1, -1)] {
            let result = epoch_handoff_distinguishes_identity_and_data_retry(
                19, 19, epoch, &old, first, request,
            );
            assert!(
                result
                    == (
                        epoch + 1,
                        InitProducerIdIdentityDecision::Retry,
                        vec![5],
                        ProducerDecision::Fenced
                    )
            );
        }
    }
}

#[test]
fn marker_only_and_empty_windows_do_not_remove_the_partition_fence() {
    for epoch in [0, i16::MAX - 2] {
        let first = super::maximum_epoch_row(epoch);
        for hwm in [0, i64::MAX - 1, i64::MAX] {
            let result = epoch_handoff_distinguishes_identity_and_data_retry(
                i64::MAX,
                hwm,
                epoch,
                &[],
                first,
                (i32::MAX, 0),
            );
            assert!(
                result
                    == (
                        epoch + 1,
                        InitProducerIdIdentityDecision::Retry,
                        vec![0],
                        ProducerDecision::Fenced
                    )
            );
        }
    }
}

proptest! {
    #[test]
    fn epoch_handoff_outranks_physical_offsets_and_sequence_aliases(
        epoch in 0_i16..i16::MAX - 1, count in 0_usize..=5,
        incoming_base in 0_i64..64, sequence in 0_i32..=i32::MAX,
        request in (any::<i32>(), any::<i32>()), hwm in 0_i64..=70,
    ) {
        let old = super::epoch_rows(count, epoch, sequence);
        let mut first = recovered_window_row(incoming_base, 2, sequence);
        first.producer_epoch = epoch;
        let result = epoch_handoff_distinguishes_identity_and_data_retry(
            70, hwm, epoch, &old, first, request,
        );
        assert!(result == (epoch + 1, InitProducerIdIdentityDecision::Retry, vec![count], ProducerDecision::Fenced));
    }
}
