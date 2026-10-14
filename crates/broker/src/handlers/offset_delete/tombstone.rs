//! Appending the offset tombstones to the group's `__consumer_offsets`
//! partition.
//!
//! Deleting a committed offset is a write, not a metadata edit. Kafka runs it
//! as a `CoordinatorRuntime` write operation, which appends as the partition
//! leader and completes once the high watermark covers the records, so the
//! tombstones go through the group coordinator's offsets log, which does the
//! same. The error mapping for a write that fails lives here with it.

use {
    crate::{broker::Broker, codes},
    krabka_protocol::records::RecordBatch,
};

/// Append `batch` to the `__consumer_offsets` partition of `group_id`, and
/// return once it is committed.
///
/// # Errors
///
/// Returns the top-level code that Kafka's `handleOperationException` answers
/// for the failed write: `NOT_COORDINATOR` when this broker does not lead the
/// partition or stops leading it first, and `COORDINATOR_NOT_AVAILABLE` when
/// the write does not commit in time.
pub(super) async fn append_tombstones(
    broker: &Broker,
    group_id: &str,
    batch: RecordBatch,
) -> Result<(), i16> {
    broker
        .group_coordinator
        .offsets_log
        .append(group_id, batch)
        .await
        .map_err(|error| {
            tracing::warn!(group_id, %error, "OffsetDelete: the tombstone write failed");
            operation_error_code(codes::from_broker_error(&error))
        })
}

/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`: the
/// top-level code a failed coordinator write answers with.
fn operation_error_code(code: i16) -> i16 {
    match code {
        codes::MESSAGE_TOO_LARGE | codes::RECORD_LIST_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => codes::coordinator_operation_code(other),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[yare::parameterized(
        network_exception = { codes::NETWORK_EXCEPTION, codes::COORDINATOR_LOAD_IN_PROGRESS },
        unknown_topic = { codes::UNKNOWN_TOPIC_OR_PARTITION, codes::COORDINATOR_NOT_AVAILABLE },
        corrupt_message = { codes::CORRUPT_MESSAGE, codes::CORRUPT_MESSAGE },
        not_enough_replicas = { codes::NOT_ENOUGH_REPLICAS, codes::COORDINATOR_NOT_AVAILABLE },
        request_timed_out = { codes::REQUEST_TIMED_OUT, codes::COORDINATOR_NOT_AVAILABLE },
        not_leader = { codes::NOT_LEADER_OR_FOLLOWER, codes::NOT_COORDINATOR },
        kafka_storage_error = { codes::KAFKA_STORAGE_ERROR, codes::NOT_COORDINATOR },
        message_too_large = { codes::MESSAGE_TOO_LARGE, codes::UNKNOWN_SERVER_ERROR },
        record_list_too_large = { codes::RECORD_LIST_TOO_LARGE, codes::UNKNOWN_SERVER_ERROR },
        unknown_server_error = { codes::UNKNOWN_SERVER_ERROR, codes::UNKNOWN_SERVER_ERROR },
    )]
    fn write_errors_map_as_kafka_handle_operation_exception(write: i16, want: i16) {
        check!(operation_error_code(write) == want, "{write}");
    }
}
