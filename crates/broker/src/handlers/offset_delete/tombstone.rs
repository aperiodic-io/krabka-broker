//! Appending the offset tombstones to the group's `__consumer_offsets`
//! partition.
//!
//! Deleting a committed offset is a write, not a metadata edit. Kafka runs it
//! as a `CoordinatorRuntime` write operation, which appends as the partition
//! leader and completes once the high watermark covers the records, so the
//! tombstones go through the group coordinator's offsets log, which does the
//! same. The error mapping for a write that fails lives here with it.

use krabka_protocol::records::RecordBatch;

use crate::{broker::Broker, codes};

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
    use assert2::check;

    use super::*;

    #[test]
    fn write_errors_map_as_kafka_handle_operation_exception() {
        for (write, want) in [
            (
                codes::NETWORK_EXCEPTION,
                codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (codes::CORRUPT_MESSAGE, codes::CORRUPT_MESSAGE),
        ]
        .into_iter()
        .chain(crate::test_support::COORDINATOR_WRITE_ERROR_CASES)
        {
            check!(operation_error_code(write) == want, "{write}");
        }
    }
}
