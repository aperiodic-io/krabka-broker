use assert2::assert;

use super::*;
use crate::producer_snapshot::ProducerSnapshotEntryFacts;

#[test]
fn trim_composition_boundaries_expose_inherited_frontier_requirement() {
    let facts = DeleteRecordsTrimFacts {
        requested: 5,
        current_start: 0,
        high_watermark: 5,
        log_end: 10,
        has_delivery_watermark: true,
        delivery_watermark: 5,
    };
    // An admitted request cannot bound an already-unbounded WAL start.
    assert!(
        delete_records_trim_decision(facts) == DeleteRecordsTrimDecision::Apply { frontier: 5 }
    );
    assert!(
        delete_records_trim_application(5, 8, 0)
            == DeleteRecordsTrimApplication::TrimLocal { frontier: 8 }
    );
    for (requested, wal, local) in [(5, 0, 0), (5, 8, 3), (5, 3, 8), (i64::MAX, 0, 0)] {
        for applied in [
            &[][..],
            &[false, false],
            &[true],
            &[false, true, false, true, true],
        ] {
            let frontier = requested.max(wal).max(local);
            let result = trim_steps_converge(requested, wal, local, applied);
            assert!(result.0 >= wal && result.1 >= local);
            assert!(result.0 <= frontier && result.1 <= frontier);
        }
    }
    for requested in [-2, -1, 0, 3, 5, 6] {
        trim_witnesses::check_logical(
            DeleteRecordsTrimFacts { requested, ..facts },
            3,
            2,
            &[0, 3, 5, 10, 11],
        );
    }
    // Snapshot validity preserves historical producer state, even when
    // its last batch is below the new logical start. It is not a data read.
    let historical = ProducerSnapshotEntryFacts {
        producer_id: 1,
        producer_epoch: 0,
        last_sequence: 0,
        last_offset: 0,
        offset_delta: 0,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    };
    assert!(crate::producer_snapshot::producer_snapshot_entry_valid(
        10, historical
    ));
    for lag in [-1, 0, 2, 5, 6, i64::MAX] {
        trim_witnesses::check_physical(5, i64::MAX, lag, 0, 1, &[false, true, true]);
    }
}

#[test]
fn remote_timestamp_composition_handles_padding_and_conservative_rows() {
    let offsets = [0, 3, 6];
    let timestamps = [100, 300, 200];
    for entries in [
        &[][..],
        &[(100, 0), (300, 3), (300, 6)],
        &[(100, 0), (300, 3), (300, 6), (0, 0), (0, 0)],
        // Conservative bounds may decrease without skipping a match;
        // this is accepted by remote scanning but fails row validation.
        &[(500, 0), (100, 3), (300, 6)],
        &[(i64::MIN, 0), (0, 0)],
    ] {
        for target in [i64::MIN, 100, 200, 300, 301, 500, i64::MAX] {
            let (count, floor) = validated_time_scan::prefix_oracle(entries, target);
            let expected = timestamps.iter().position(|time| *time >= target);
            assert!(
                remote_timestamp_scan_preserves_first(entries, &offsets, &timestamps, target)
                    == (count, floor, expected)
            );
            let validated = validated_time_scan::archive_oracle(entries, 6)
                .then_some((count, floor, floor))
                .ok_or(());
            assert!(validated_remote_and_local_time_starts_agree(entries, 6, target) == validated);
        }
    }
    // Structural validity alone cannot establish a truthful running maximum.
    // This sorted, in-range row skips a real earlier match.
    assert!(restore_time_index_entry_valid(None, 0, 3, 6));
    let misleading = [(0, 3)];
    let count = remote_time_index_candidate_count(&misleading, 100);
    assert!(count == 1 && misleading[count - 1].1 == 3);
    assert!(first_timestamp_index(&timestamps, 100) == Some(0));
    assert!(first_timestamp_index(&timestamps[1..], 100).map(|index| index + 1) == Some(1));
}
