use {super::*, assert2::assert};

#[test]
fn selection_covers_isolation_tiers_and_overflow_edges() {
    use ListOffsetsSelectionDecision::{Resolved, Unknown};

    let selection = |kind, candidate_offset, last_fetchable| {
        list_offsets_selection_decision(ListOffsetsSelectionFacts {
            kind,
            candidate_offset,
            candidate_timestamp: -1,
            candidate_epoch: -1,
            last_fetchable,
        })
    };
    assert!(
        selection(ListOffsetsKind::Unsupported, 10, 10)
            == ListOffsetsSelectionDecision::RejectMalformed
    );
    assert!(selection(ListOffsetsKind::Timestamp, -1, 10) == Unknown);
    assert!(
        selection(ListOffsetsKind::Timestamp, -2, 10)
            == ListOffsetsSelectionDecision::RejectMalformed
    );
    assert!(
        list_offsets_selection_decision(ListOffsetsSelectionFacts {
            kind: ListOffsetsKind::Timestamp,
            candidate_offset: 5,
            candidate_timestamp: 100,
            candidate_epoch: -1,
            last_fetchable: 10,
        }) == Resolved {
            offset: 5,
            timestamp: 100,
            leader_epoch: -1,
        }
    );
    assert!(
        list_offsets_selection_decision(ListOffsetsSelectionFacts {
            kind: ListOffsetsKind::Timestamp,
            candidate_offset: 5,
            candidate_timestamp: 100,
            candidate_epoch: -2,
            last_fetchable: 10,
        }) == ListOffsetsSelectionDecision::RejectMalformed
    );
    assert!(
        list_offsets_selection_decision(ListOffsetsSelectionFacts {
            kind: ListOffsetsKind::Earliest,
            candidate_offset: 0,
            candidate_timestamp: 0,
            candidate_epoch: 0,
            last_fetchable: 0,
        }) == Resolved {
            offset: 0,
            timestamp: 0,
            leader_epoch: 0,
        }
    );
    assert!(
        list_offsets_selection_decision(ListOffsetsSelectionFacts {
            kind: ListOffsetsKind::Timestamp,
            candidate_offset: 0,
            candidate_timestamp: 0,
            candidate_epoch: 0,
            last_fetchable: -1,
        }) == ListOffsetsSelectionDecision::RejectMalformed
    );
    assert!(
        selection(ListOffsetsKind::EarliestLocal, 5, 2)
            == Resolved {
                offset: 5,
                timestamp: -1,
                leader_epoch: -1,
            }
    );
    assert!(
        selection(ListOffsetsKind::Latest, 10, 6)
            == Resolved {
                offset: 6,
                timestamp: -1,
                leader_epoch: -1,
            }
    );
    assert!(
        selection(ListOffsetsKind::Latest, 3, 5)
            == Resolved {
                offset: 3,
                timestamp: -1,
                leader_epoch: -1,
            }
    );
    assert!(selection(ListOffsetsKind::Timestamp, 6, 6) == Unknown);
    assert!(
        selection(ListOffsetsKind::Timestamp, 3, 5)
            == Resolved {
                offset: 3,
                timestamp: -1,
                leader_epoch: -1,
            }
    );
    assert!(selection(ListOffsetsKind::Timestamp, 5, 3) == Unknown);
    assert!(
        selection(ListOffsetsKind::Earliest, 8, 0)
            == Resolved {
                offset: 8,
                timestamp: -1,
                leader_epoch: -1,
            }
    );
    assert!(selection(ListOffsetsKind::Timestamp, i64::MAX, i64::MAX) == Unknown);
}
