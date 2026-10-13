//! `ReadShareGroupStateSummary` (`api_key=87`).
//!
//! This handler returns the small summary for each
//! `(group, topic, partition)` without the full state-batch list, as Kafka's
//! `ShareCoordinatorService.readStateSummary` does. The summary holds the
//! state epoch, the leader epoch, the start offset, and the delivery-complete
//! count. An empty group id, an empty topic list or a topic with no
//! partitions gets an empty response. A partition this broker does not lead
//! returns `NOT_COORDINATOR`, one that still loads
//! `COORDINATOR_LOAD_IN_PROGRESS`, a negative partition `INVALID_REQUEST`, and
//! a topic partition the metadata image lacks `UNKNOWN_TOPIC_OR_PARTITION`.
//! A key this broker leads but does not know returns Kafka's uninitialized
//! summary: start offset and delivery-complete count `-1`, epochs `0`.

use {
    crate::share_coordinator::coordinator::{
        ShareCoordinator, ShareStateError, UNINITIALIZED_START_OFFSET,
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
        read_share_group_state_summary_response::{
            PartitionResult, ReadShareGroupStateSummaryResponse, ReadStateSummaryResult,
        },
    },
};

krabka_macros::share_state_handler!(
    ReadShareGroupStateSummaryRequest,
    ReadShareGroupStateSummaryResponse,
    ReadStateSummaryResult,
    PartitionResult,
    read_summaries
);

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readStateSummary` does.
async fn read_summaries(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateSummaryRequest,
) -> ReadShareGroupStateSummaryResponse {
    if req.group_id.is_empty()
        || super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
    {
        return ReadShareGroupStateSummaryResponse::default();
    }
    let results = krabka_macros::share_state_results!(
        req,
        ReadStateSummaryResult,
        |group_id, topic_id, pd| {
            match coordinator
                .read_summary_checked(image, group_id, topic_id, pd.partition)
                .await
            {
                Ok(Some((state_epoch, leader_epoch, start_offset, delivery_complete_count))) => {
                    PartitionResult {
                        partition: pd.partition,
                        state_epoch,
                        leader_epoch,
                        start_offset: start_offset.0,
                        delivery_complete_count,
                        ..Default::default()
                    }
                }
                // Kafka's `PartitionFactory` sentinels for a key with no state.
                Ok(None) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: UNINITIALIZED_START_OFFSET,
                    delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message, and
                // the schema default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error_message(error)),
                    ..Default::default()
                },
            }
        }
    );

    ReadShareGroupStateSummaryResponse {
        results,
        ..Default::default()
    }
}

/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;

/// The row message: a refusal's own message, or the operation error that
/// `readStateSummary` prefixes with `Unable to read share group state summary: `.
fn error_message(error: ShareStateError) -> String {
    match error {
        ShareStateError::Refused { message, .. } => message.to_owned(),
        ShareStateError::Operation { message, .. } => {
            format!("Unable to read share group state summary: {message}")
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionResult, ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse,
            read_summaries,
        },
        crate::{
            codes,
            share_coordinator::handlers::test_support::{
                EmptyStateRequests, StateReply, StoredStateRow, check_stored_rows, state_response,
            },
        },
        krabka_protocol::{
            UnknownTaggedFields,
            owned::read_share_group_state_summary_request::{PartitionData, ReadStateSummaryData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([41; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([41; 16]);

    fn request(group_id: &str, partitions: &[i32]) -> ReadShareGroupStateSummaryRequest {
        krabka_macros::share_state_request! {
            ReadShareGroupStateSummaryRequest, ReadStateSummaryData;
            group [group_id];
            topic [WIRE_TOPIC];
            partitions [partitions.iter().map(|&partition| {
            PartitionData { partition, ..Default::default() }
            }).collect()];
        }
    }

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateSummaryResponse {
        state_response(WIRE_TOPIC, partitions)
    }

    fn error_row(partition: i32, code: i16, message: &str) -> PartitionResult {
        ReadShareGroupStateSummaryResponse::error_row(partition, code, Some(message))
    }

    /// The whole `ReadShareGroupStateSummaryResponse` for each request shape,
    /// over topic T with two partitions and state for partition 0 only, as
    /// Kafka's `ShareCoordinatorService.readStateSummary` and
    /// `ShareCoordinatorShard.readStateSummary` answer.
    #[tokio::test]
    async fn read_summary_answers_as_kafka() {
        let stored = PartitionResult {
            partition: 0,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 17,
            leader_epoch: 3,
            start_offset: 101,
            delivery_complete_count: 9,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let missing = PartitionResult {
            partition: 1,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 0,
            leader_epoch: 0,
            start_offset: -1,
            delivery_complete_count: -1,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let empty = ReadShareGroupStateSummaryResponse::default();
        // Each row owns its request and independently expected response.
        let rows = [
            StoredStateRow {
                led: true,
                request: request("share-group", &[0]),
                expected: response(vec![stored]),
            },
            StoredStateRow {
                led: true,
                request: request("share-group", &[1]),
                expected: response(vec![missing]),
            },
            StoredStateRow {
                led: false,
                request: request("share-group", &[0]),
                expected: response(vec![error_row(
                    0,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state summary: This is not the correct coordinator.",
                )]),
            },
            StoredStateRow {
                led: true,
                request: request("share-group", &[-1]),
                expected: response(vec![error_row(
                    -1,
                    codes::INVALID_REQUEST,
                    "The partition id cannot be a negative number.",
                )]),
            },
            StoredStateRow {
                led: true,
                request: request("share-group", &[2]),
                expected: response(vec![error_row(
                    2,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "This server does not host this topic-partition.",
                )]),
            },
        ];
        let empty_requests = EmptyStateRequests {
            group: request("", &[0]),
            partitions: request("share-group", &[]),
            topics: ReadShareGroupStateSummaryRequest {
                group_id: "share-group".into(),
                ..Default::default()
            },
        };

        check_stored_rows(
            rows.into_iter().chain(empty_requests.into_rows(empty)),
            TOPIC,
            2,
            0,
            read_summaries,
        )
        .await;
    }
}
