use {
    super::*,
    crate::restore::{RestoreContentExclusions, RestoreTimestampType},
};

proptest! {
    #[test]
    fn filtered_rewrites_preserve_an_independent_record_and_retry_oracle(
        rows in proptest::collection::btree_map(0_i32..=64, (any::<i64>(), any::<bool>()), 0..16),
        base in 0_i64..=i64::MAX - 65,
        sequence in 0_i32..=i32::MAX, epoch in 0_i16..=i16::MAX,
        append_time in any::<bool>(), offset_bound in proptest::option::of(any::<i64>()),
        timestamp_bound in proptest::option::of(any::<i64>()),
    ) {
        let frame = RestoreBatchFrame {
            base_offset: base, last_offset_delta: 64,
            timestamp_type: if append_time { RestoreTimestampType::LogAppendTime } else { RestoreTimestampType::CreateTime },
            base_timestamp: 0, max_timestamp: i64::MAX,
        };
        let records: Vec<_> = rows.into_iter().map(|(offset_delta, (timestamp_delta, exclude))| (
            RestoreRecordDeltas { offset_delta, timestamp_delta },
            RestoreExclusions { producer: false, offset: false,
                content: RestoreContentExclusions { key: exclude, header: false } },
        )).collect();
        let expected: Vec<_> = records.iter().enumerate().filter_map(|(i, (record, exclusions))| {
            let offset = base + i64::from(record.offset_delta);
            let timestamp = if append_time { i64::MAX } else { record.timestamp_delta };
            (offset_bound.is_none_or(|bound| offset <= bound)
                && timestamp_bound.is_none_or(|bound| timestamp < bound)
                && !exclusions.content.key).then_some((i, offset, timestamp))
        }).collect();
        let result = filtered_restore_preserves_producer_retry(frame, &records, (offset_bound, timestamp_bound), (7, epoch, sequence));
        prop_assert_eq!(result.0, expected);
        prop_assert_eq!(result.1, base + 65);
        prop_assert_eq!(result.2, sequence);
        prop_assert_eq!(result.3, ProducerDecision::Duplicate { retained: 4 });
    }
}

#[test]
fn empty_and_trailing_filtered_batches_keep_the_original_sequence_span() {
    let frame = RestoreBatchFrame {
        base_offset: 9,
        last_offset_delta: 2,
        timestamp_type: RestoreTimestampType::LogAppendTime,
        base_timestamp: i64::MAX,
        max_timestamp: 100,
    };
    let exclusions = RestoreExclusions {
        producer: false,
        offset: false,
        content: RestoreContentExclusions {
            key: false,
            header: false,
        },
    };
    let records = [
        (
            RestoreRecordDeltas {
                offset_delta: 0,
                timestamp_delta: i64::MAX,
            },
            exclusions,
        ),
        (
            RestoreRecordDeltas {
                offset_delta: 2,
                timestamp_delta: i64::MIN,
            },
            exclusions,
        ),
    ];
    for bound in [Some(8), Some(9), None] {
        let result = filtered_restore_preserves_producer_retry(
            frame,
            &records,
            (bound, None),
            (7, 2, i32::MAX - 1),
        );
        let expected: Vec<_> = records
            .iter()
            .enumerate()
            .filter_map(|(i, (record, _))| {
                let offset = 9 + i64::from(record.offset_delta);
                bound
                    .is_none_or(|bound| offset <= bound)
                    .then_some((i, offset, 100))
            })
            .collect();
        assert2::assert!(result.0 == expected);
        assert2::assert!(result.1 == 12 && result.2 == i32::MAX - 1);
        assert2::assert!(result.3 == ProducerDecision::Duplicate { retained: 4 });
    }
}
