use {
    super::{admitted_loss_marker_preserves_pending, loss_settlement_is_idempotent},
    crate::audit::{AuditLossMarkerAdmission, AuditLosses},
    assert2::assert,
    proptest::prelude::*,
};

fn loss_oracle(state: AuditLosses, marker: AuditLosses) -> AuditLosses {
    if state.generation != marker.generation {
        return state;
    }
    let remainder = i128::from(state.count) - i128::from(marker.count);
    AuditLosses {
        generation: state.generation + u64::from(remainder > 0),
        count: u64::try_from(remainder.max(0)).unwrap(),
    }
}

proptest! {
    #[test]
    fn settlement_witness_matches_an_independent_loss_ledger(
        generation in 0..u64::MAX, count in any::<u64>(),
        marker_generation in any::<u64>(), marker_count in any::<u64>(),
        same_generation in any::<bool>(),
    ) {
        let state = AuditLosses { generation, count };
        let marker = AuditLosses {
            generation: if same_generation { generation } else { marker_generation },
            count: marker_count,
        };
        let expected = loss_oracle(state, marker);
        let (settled, replayed) = loss_settlement_is_idempotent(state, marker);
        assert!(settled == expected && replayed == expected);
    }

    #[test]
    fn admitted_snapshot_preserves_later_losses_and_fences_duplicates(
        generation in 0..u64::MAX, count in any::<u64>(),
        sampled_count in any::<u64>(), previous in any::<u64>(),
        header in any::<bool>(), fields in 0_u64..5,
    ) {
        let state = AuditLosses { generation, count };
        let marker = AuditLosses { generation, count: sampled_count.min(count) };
        let result = admitted_loss_marker_preserves_pending(state, marker, previous, header, fields);
        if !header || fields != 2 || marker.count == 0 || generation <= previous {
            assert!(result.is_none());
        } else {
            let expected = loss_oracle(state, marker);
            let next = if expected.count == 0 {
                AuditLossMarkerAdmission::Reject
            } else {
                AuditLossMarkerAdmission::Admit { generation: generation + 1 }
            };
            assert!(result == Some((expected, expected, AuditLossMarkerAdmission::Reject, next)));
        }
    }
}

#[test]
fn replay_witness_covers_snapshot_edges_and_generation_ceiling() {
    for generation in [0, 1, u64::MAX - 1] {
        for (count, reported, marker_generation) in [
            (0, 0, generation),
            (5, 0, generation),
            (2, 3, generation),
            (5, 2, u64::MAX),
        ] {
            let state = AuditLosses { generation, count };
            let marker = AuditLosses {
                generation: marker_generation,
                count: reported,
            };
            let expected = loss_oracle(state, marker);
            assert!(loss_settlement_is_idempotent(state, marker) == (expected, expected));
        }
        let state = AuditLosses {
            generation,
            count: 5,
        };
        let empty = AuditLosses {
            generation,
            count: 0,
        };
        assert!(admitted_loss_marker_preserves_pending(state, empty, 0, true, 2).is_none());
    }
    for generation in [1, 7, u64::MAX - 1] {
        for (count, reported) in [(2, 2), (3, 2), (u64::MAX, 1), (u64::MAX, u64::MAX)] {
            let state = AuditLosses { generation, count };
            let marker = AuditLosses {
                generation,
                count: reported,
            };
            let expected = loss_oracle(state, marker);
            let next = if expected.count == 0 {
                AuditLossMarkerAdmission::Reject
            } else {
                AuditLossMarkerAdmission::Admit {
                    generation: generation + 1,
                }
            };
            assert!(
                admitted_loss_marker_preserves_pending(state, marker, generation - 1, true, 2)
                    == Some((expected, expected, AuditLossMarkerAdmission::Reject, next))
            );
            assert!(
                admitted_loss_marker_preserves_pending(state, marker, generation, true, 2)
                    .is_none()
            );
            assert!(
                admitted_loss_marker_preserves_pending(state, marker, generation - 1, false, 2)
                    .is_none()
            );
            assert!(
                admitted_loss_marker_preserves_pending(state, marker, generation - 1, true, 3)
                    .is_none()
            );
        }
    }
}
