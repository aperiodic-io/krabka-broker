use super::*;

const fn record(has_key: bool, has_value: bool) -> RecordMeta { RecordMeta { has_key, has_value } }

const fn batch(is_control: bool, existing_horizon: Option<i64>) -> BatchMeta {
    BatchMeta {
        is_control,
        producer_id: -1,
        existing_horizon,
    }
}

mod compute_horizon_saturates_at_i64_bounds;
