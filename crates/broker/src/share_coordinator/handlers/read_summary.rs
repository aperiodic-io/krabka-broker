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

super::share_state_handler!(
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
    if req.group_id.is_empty() || super::empty_partition_data!(req) {
        return ReadShareGroupStateSummaryResponse::default();
    }
    let results = super::state_results!(req, ReadStateSummaryResult, |group_id, topic_id, pd| {
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
    });

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
        super::*,
        crate::codes,
        krabka_protocol::{
            UnknownTaggedFields,
            owned::read_share_group_state_summary_request::{PartitionData, ReadStateSummaryData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([41; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([41; 16]);

    super::super::test_support::request_fixture! {
        ReadShareGroupStateSummaryRequest, ReadStateSummaryData, PartitionData;
        fn request(group_id: &str, [], partitions: &[i32]);
        topic WIRE_TOPIC; |partition| {
            PartitionData { partition, ..Default::default() }
        }
    }

    super::super::test_support::response_fixture!(
        ReadShareGroupStateSummaryResponse,
        ReadStateSummaryResult,
        PartitionResult,
        WIRE_TOPIC
    );

    super::super::test_support::error_row_fixture!(PartitionResult);

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
        // (led, request, expected response)
        let rows = [
            (true, request("share-group", &[0]), response(vec![stored])),
            (true, request("share-group", &[1]), response(vec![missing])),
            (
                false,
                request("share-group", &[0]),
                response(vec![error_row(
                    0,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state summary: This is not the correct coordinator.",
                )]),
            ),
            (
                true,
                request("share-group", &[-1]),
                response(vec![error_row(
                    -1,
                    codes::INVALID_REQUEST,
                    "The partition id cannot be a negative number.",
                )]),
            ),
            (
                true,
                request("share-group", &[2]),
                response(vec![error_row(
                    2,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "This server does not host this topic-partition.",
                )]),
            ),
            (true, request("", &[0]), empty.clone()),
            (true, request("share-group", &[]), empty.clone()),
            (
                true,
                super::super::test_support::no_topics!(
                    ReadShareGroupStateSummaryRequest,
                    "share-group"
                ),
                empty,
            ),
        ];

        super::super::test_support::stored_response_rows!(rows, TOPIC, (2, 0), read_summaries);
    }
}
