use {super::*, assert2::check};

const NO_CONTENT: RestoreContentExclusions = RestoreContentExclusions {
    key: false,
    header: false,
};

const NONE: RestoreExclusions = RestoreExclusions {
    producer: false,
    offset: false,
    content: NO_CONTENT,
};

fn frame(
    timestamp_type: RestoreTimestampType,
    base_timestamp: i64,
    max_timestamp: i64,
) -> RestoreBatchFrame {
    RestoreBatchFrame {
        base_offset: 10,
        last_offset_delta: 4,
        timestamp_type,
        base_timestamp,
        max_timestamp,
    }
}

const fn deltas(offset_delta: i32, timestamp_delta: i64) -> RestoreRecordDeltas {
    RestoreRecordDeltas {
        offset_delta,
        timestamp_delta,
    }
}

const NON_IDEMPOTENT: RestoreProducer = RestoreProducer {
    control: false,
    transactional: false,
    producer_id: -1,
    producer_epoch: -1,
    base_sequence: -1,
};

const fn layout(base_offset: i64, last_offset_delta: i32, records_count: i32) -> RestoreLayout {
    RestoreLayout {
        base_offset,
        last_offset_delta,
        records_count,
    }
}

mod archive_reconciliation_covers_every_scan_and_snapshot_pair;

mod rewritten_records_are_strict_bounded_and_timestamp_checked;
