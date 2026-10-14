use super::*;

#[test]
fn target_counts_are_positive_exact_and_overflow_safe() {
    assert2::check!(
        barrier_target_count_decision(4, 3) == BarrierTargetCountDecision::Expand { next: 7 }
    );
    assert2::check!(barrier_target_count_decision(4, 0) == BarrierTargetCountDecision::Malformed);
    assert2::check!(
        barrier_target_count_decision(i64::MAX, 1) == BarrierTargetCountDecision::Overflow
    );
}

const ADMITTED_MARKER: BarrierMarkerFenceFacts = BarrierMarkerFenceFacts {
    image_present: true,
    expected_leader: 2,
    expected_epoch: 7,
    image_leader: 2,
    image_epoch: 7,
    current_leader: 2,
    current_epoch: 7,
};

#[yare::parameterized(
    matching_generation = { ADMITTED_MARKER, BarrierMarkerFenceDecision::Append },
    epoch_zero_is_valid = { BarrierMarkerFenceFacts {
        expected_epoch: 0, image_epoch: 0, current_epoch: 0, ..ADMITTED_MARKER
    }, BarrierMarkerFenceDecision::Append },
    missing_image = {
        BarrierMarkerFenceFacts { image_present: false, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::Malformed,
    },
    negative_expected_epoch = {
        BarrierMarkerFenceFacts { expected_epoch: -1, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::Malformed,
    },
    negative_image_epoch = {
        BarrierMarkerFenceFacts { image_epoch: -1, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::Malformed,
    },
    negative_current_epoch = {
        BarrierMarkerFenceFacts { current_epoch: -1, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::Malformed,
    },
    malformed_precedes_other_leader = { BarrierMarkerFenceFacts {
        current_epoch: -1, image_leader: 3, ..ADMITTED_MARKER
    }, BarrierMarkerFenceDecision::Malformed },
    image_names_other_leader = {
        BarrierMarkerFenceFacts { image_leader: 3, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::NotLeader,
    },
    installed_other_leader = {
        BarrierMarkerFenceFacts { current_leader: 3, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::NotLeader,
    },
    other_leader_precedes_other_epoch = { BarrierMarkerFenceFacts {
        current_leader: 3, current_epoch: 8, ..ADMITTED_MARKER
    }, BarrierMarkerFenceDecision::NotLeader },
    image_other_epoch = {
        BarrierMarkerFenceFacts { image_epoch: 8, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::FencedEpoch,
    },
    installed_other_epoch = {
        BarrierMarkerFenceFacts { current_epoch: 6, ..ADMITTED_MARKER },
        BarrierMarkerFenceDecision::FencedEpoch,
    },
)]
fn marker_fencing_ranks_malformed_then_leader_then_epoch(
    facts: BarrierMarkerFenceFacts,
    expected: BarrierMarkerFenceDecision,
) {
    assert2::check!(barrier_marker_fence_decision(facts) == expected);
}

#[test]
fn only_requested_nonnegative_placements_enter_a_cut() {
    assert2::check!(barrier_placement_decision(true, 0) == BarrierPlacementDecision::Accept);
    assert2::check!(barrier_placement_decision(false, 4) == BarrierPlacementDecision::Reject);
    assert2::check!(barrier_placement_decision(true, -1) == BarrierPlacementDecision::Reject);
}

#[test]
fn a_cut_is_complete_exactly_when_nothing_is_missing() {
    assert2::check!(barrier_cut_classification(false) == BarrierCutClassification::Complete);
    assert2::check!(barrier_cut_classification(true) == BarrierCutClassification::Partial);
}
