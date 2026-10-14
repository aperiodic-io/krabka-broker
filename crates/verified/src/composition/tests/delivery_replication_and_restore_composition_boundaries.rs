use {super::*, crate::restore::RestoreTimestampType, assert2::assert};

#[test]
fn delivery_replication_and_restore_composition_boundaries() {
    for activations in [&[][..], &[10, 20, 5], &[i64::MIN], &[
        i64::MAX - 1,
        i64::MAX,
    ]] {
        for uncertainty in [-1, 0, 2, i64::MAX] {
            for now in [i64::MIN, 19, 20, i64::MAX] {
                let all_due = activations.iter().all(|activation| {
                    uncertainty >= 0
                        && i128::from(*activation) + i128::from(uncertainty) <= i128::from(now)
                });
                assert!(segment_maximum_proves_delivery(activations, uncertainty, now) == all_due);
            }
        }
    }
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 6,
        hw: 6,
        lso: 6,
        deliverable: 6,
    };
    for (batches, activations) in super::VALID_SCHEDULES.into_iter().chain([
        (&[(0, -1)][..], &[10][..]),
        (&[(0, 1), (1, 1)][..], &[10, 20][..]),
    ]) {
        for (uncertainty, now) in [(-1, 100), (0, 0), (0, 100), (2, 100), (i64::MAX, i64::MAX)] {
            let result = scheduled_prefix_bounds_fetch(batches, activations, uncertainty, now, w);
            assert!(
                result.is_some()
                    == (batches == [(0, 1), (2, 1), (4, 1)] || batches == [(0, 1), (4, 1)])
            );
        }
    }
    assert!(
        scheduled_prefix_bounds_fetch(&[], &[], 0, 0, FetchWatermarks { log_end: 0, ..w })
            .is_some()
    );
    let facts = ReplicaFetchFacts {
        request_leader_epoch: 2,
        current_leader_epoch: 2,
        target_matches: true,
        reported_target_matches: true,
        error_code: 0,
        diverging_epoch: -1,
    };
    // Success clamps an over-reported HWM to the exact appended log end.
    assert!(fenced_replication_bounds_fetch(facts, -1, 6, 1, i64::MAX, w) == (8, 8, 6));
    // Divergence never appends even when valid append coordinates are present.
    assert!(
        fenced_replication_bounds_fetch(
            ReplicaFetchFacts {
                diverging_epoch: 0,
                ..facts
            },
            3,
            6,
            1,
            8,
            w
        ) == (3, 3, 3)
    );
    for fenced in [
        ReplicaFetchFacts {
            request_leader_epoch: 1,
            ..facts
        },
        ReplicaFetchFacts {
            target_matches: false,
            ..facts
        },
        ReplicaFetchFacts {
            reported_target_matches: false,
            ..facts
        },
        ReplicaFetchFacts {
            error_code: 6,
            ..facts
        },
    ] {
        assert!(fenced_replication_bounds_fetch(fenced, -1, 6, 1, i64::MAX, w) == (6, 6, 6));
    }
    // Compacted replication may skip offset 6; the physical end advances,
    // while read-committed Fetch stays capped by the remaining watermarks.
    assert!(fenced_replication_bounds_fetch(facts, -1, 7, 1, 8, w) == (9, 8, 6));
    assert!(fenced_replication_bounds_fetch(facts, -1, 6, -1, 8, w) == (6, 6, 6));
    let exclusions = restore_exclusions(false);
    let records = [
        (
            RestoreRecordDeltas {
                offset_delta: 0,
                timestamp_delta: 0,
            },
            exclusions,
        ),
        (
            RestoreRecordDeltas {
                offset_delta: 1,
                timestamp_delta: 1,
            },
            exclusions,
        ),
    ];
    let frame = RestoreBatchFrame {
        base_offset: 4,
        last_offset_delta: 1,
        timestamp_type: RestoreTimestampType::CreateTime,
        base_timestamp: 10,
        max_timestamp: 11,
    };
    for records in [&[][..], &records, &[(records[0].0, RestoreExclusions {
        producer: true,
        ..exclusions
    })]] {
        for offset_bound in [None, Some(3), Some(4), Some(5)] {
            for timestamp_bound in [None, Some(10), Some(11), Some(12)] {
                assert!(restore_selection_matches_oracle(
                    frame,
                    records,
                    offset_bound,
                    timestamp_bound
                ));
            }
        }
    }
    assert!(restore_selection_matches_oracle(
        RestoreBatchFrame {
            timestamp_type: crate::restore::RestoreTimestampType::LogAppendTime,
            base_timestamp: i64::MAX,
            ..frame
        },
        &records,
        Some(5),
        Some(12),
    ));
    assert!(restore_selection_matches_oracle(
        frame,
        &[(
            RestoreRecordDeltas {
                offset_delta: 2,
                timestamp_delta: 0
            },
            exclusions
        )],
        None,
        None,
    ));
    for invalid in [
        RestoreBatchFrame {
            base_offset: -1,
            ..frame
        },
        RestoreBatchFrame {
            last_offset_delta: -1,
            ..frame
        },
        RestoreBatchFrame {
            base_offset: i64::MAX,
            last_offset_delta: 0,
            ..frame
        },
    ] {
        assert!(restore_selection_respects_batch_extent(invalid, &[], None, None) == None);
    }
}

fn restore_selection_matches_oracle(
    frame: RestoreBatchFrame,
    records: &[(RestoreRecordDeltas, RestoreExclusions)],
    offset_bound: Option<i64>,
    timestamp_bound: Option<i64>,
) -> bool {
    let expected = (|| {
        if frame.base_offset < 0 || frame.last_offset_delta < 0 {
            return None;
        }
        let end = frame
            .base_offset
            .checked_add(i64::from(frame.last_offset_delta))?
            .checked_add(1)?;
        let mut selected = Vec::new();
        for (i, (record, exclusions)) in records.iter().enumerate() {
            let (offset, timestamp) = restore_record_coordinates(frame, *record)?;
            if offset_bound.is_none_or(|bound| offset <= bound)
                && timestamp_bound.is_none_or(|bound| timestamp < bound)
                && !(exclusions.producer
                    || exclusions.offset
                    || exclusions.content.key
                    || exclusions.content.header)
            {
                selected.push(i);
            }
        }
        let decision = if selected.len() == records.len() {
            RestoreFilterDecision::Keep
        } else if selected.is_empty() {
            RestoreFilterDecision::Empty
        } else {
            RestoreFilterDecision::Filter
        };
        Some((end, decision, selected))
    })();
    restore_selection_respects_batch_extent(frame, records, offset_bound, timestamp_bound)
        == expected
}
