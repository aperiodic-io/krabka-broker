//! The partition row the `ListOffsets` handler emits for a partition that
//! resolved to an error rather than to an offset.
//!
//! Kafka pairs an error row with the `UNKNOWN_TIMESTAMP` and `UNKNOWN_OFFSET`
//! placeholders, and a client reads the pair as "nothing resolved here", so
//! every error path in the handler builds its row through this one function.

use {
    super::sentinels::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
    krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse,
};

pub(super) fn error_response(
    partition_index: i32,
    error_code: i16,
) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index,
        error_code,
        timestamp: UNKNOWN_TIMESTAMP,
        offset: UNKNOWN_OFFSET,
        ..Default::default()
    }
}
