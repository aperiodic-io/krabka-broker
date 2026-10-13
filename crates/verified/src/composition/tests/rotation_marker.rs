use {
    super::{ProducerDecision, RetainedSequenceRange, rotation_marker_bounds_identity_retry},
    crate::transaction::InitProducerIdIdentityDecision,
    assert2::assert,
    proptest::prelude::*,
};

fn check_rotation(
    identities: (i64, i64),
    aliases: &[Option<RetainedSequenceRange>],
    request: (i16, i32, i32),
    log_empty: bool,
    trunk: bool,
) {
    assert!(
        rotation_marker_bounds_identity_retry(
            identities.0,
            identities.1,
            aliases,
            request,
            log_empty,
            trunk,
        ) == (
            (identities.1, 0),
            InitProducerIdIdentityDecision::Retry,
            InitProducerIdIdentityDecision::Fenced,
            InitProducerIdIdentityDecision::Retry,
            ProducerDecision::Fenced,
            ProducerDecision::Append,
        )
    );
}

#[test]
fn marker_fences_aliases_and_the_next_bump_retires_the_old_identity_retry() {
    for (old, fresh) in [(0, 1), (42, 43), (i64::MAX, 0)] {
        for (base, delta) in [(0, 0), (i32::MAX, 1), (-1, i32::MIN)] {
            let aliases = vec![
                Some(RetainedSequenceRange {
                    base_sequence: base,
                    last_sequence: crate::producer::increment_sequence(base, delta),
                });
                5
            ];
            for epoch in [i16::MIN, 0, i16::MAX - 1] {
                for log_empty in [false, true] {
                    for trunk in [false, true] {
                        check_rotation(
                            (old, fresh),
                            &aliases,
                            (epoch, base, delta),
                            log_empty,
                            trunk,
                        );
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn rotation_handoff_ignores_old_sequence_aliases(
        old in 0_i64..=i64::MAX, fresh in 0_i64..=i64::MAX,
        epoch in i16::MIN..i16::MAX, base in any::<i32>(), delta in any::<i32>(),
        aliases in prop::collection::vec(
            prop::option::of((any::<i32>(), any::<i32>())), 0..=8),
        log_empty in any::<bool>(), trunk in any::<bool>(),
    ) {
        prop_assume!(old != fresh);
        let retained: Vec<_> = aliases.into_iter().map(|slot| slot.map(|(base_sequence, last_sequence)|
            RetainedSequenceRange { base_sequence, last_sequence })).collect();
        check_rotation((old, fresh), &retained, (epoch, base, delta), log_empty, trunk);
    }
}
