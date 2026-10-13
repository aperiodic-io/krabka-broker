//! `ReadShareGroupState` (`api_key=84`). The handler returns the durable
//! delivery state for each `(group, topic, partition)`: the start offset and
//! the state batches. A partition this broker does not lead returns
//! per-partition `NOT_COORDINATOR`, and a partition that still loads returns
//! `COORDINATOR_LOAD_IN_PROGRESS`. A key with no state returns
//! `INVALID_REQUEST`. A request leader epoch above the stored one is persisted
//! before the answer, so the older share-partition leader is fenced.

use {
    crate::share_coordinator::coordinator::ShareCoordinator,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        read_share_group_state_request::ReadShareGroupStateRequest,
        read_share_group_state_response::{
            PartitionResult, ReadShareGroupStateResponse, ReadStateResult, StateBatch,
        },
    },
};

krabka_macros::share_state_handler!(
    ReadShareGroupStateRequest,
    ReadShareGroupStateResponse,
    ReadStateResult,
    PartitionResult,
    read_state
);

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readState` does.
///
/// An empty topic list, a topic with no partitions, or an empty group id gets
/// a response with no results.
async fn read_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateRequest,
) -> ReadShareGroupStateResponse {
    if super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
        || req.group_id.is_empty()
    {
        return ReadShareGroupStateResponse::default();
    }
    let results =
        krabka_macros::share_state_results!(req, ReadStateResult, |group_id, topic_id, pd| {
            match coordinator
                .read(image, group_id, topic_id, pd.partition, pd.leader_epoch)
                .await
            {
                Ok(st) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: st.state_epoch,
                    start_offset: st.start_offset.0,
                    state_batches: st.state_batches.iter().map(StateBatch::from).collect(),
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message,
                // and the default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error.row_message("read")),
                    ..Default::default()
                },
            }
        });

    ReadShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionResult, ReadShareGroupStateRequest, ReadShareGroupStateResponse, StateBatch,
            read_state,
        },
        crate::{
            codes,
            share_coordinator::handlers::test_support::{
                EmptyStateRequests, StateReply, StoredStateRow, check_stored_rows, state_response,
            },
        },
        krabka_protocol::{
            UnknownTaggedFields,
            owned::read_share_group_state_request::{PartitionData, ReadStateData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([33; 16]);

    struct ReadPartition {
        partition: i32,
        leader_epoch: i32,
    }

    fn request(group_id: &str, partitions: &[ReadPartition]) -> ReadShareGroupStateRequest {
        krabka_macros::share_state_request! {
            ReadShareGroupStateRequest, ReadStateData;
            group [group_id];
            topic [WIRE_TOPIC];
            partitions [partitions.iter().map(|row| {
            PartitionData { partition: row.partition, leader_epoch: row.leader_epoch, ..Default::default() }
            }).collect()];
        }
    }

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateResponse {
        state_response(WIRE_TOPIC, partitions)
    }

    fn error_row(partition: i32, code: i16, message: &str) -> PartitionResult {
        ReadShareGroupStateResponse::error_row(partition, code, Some(message))
    }

    /// The whole `ReadShareGroupStateResponse` for each request shape, over
    /// one stored key: partition 4 at state epoch 17, start offset 101, one
    /// batch, and leader epoch 3.
    #[tokio::test]
    async fn read_state_answers_as_kafka() {
        let stored = PartitionResult {
            partition: 4,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 17,
            start_offset: 101,
            state_batches: vec![StateBatch {
                first_offset: 101,
                last_offset: 105,
                delivery_state: 2,
                delivery_count: 3,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let empty = ReadShareGroupStateResponse::default();
        // Each row owns its request and independently expected response.
        let rows = [
            StoredStateRow {
                led: true,
                request: request("share-group", &[ReadPartition {
                    partition: 4,
                    leader_epoch: 3,
                }]),
                expected: response(vec![stored]),
            },
            StoredStateRow {
                led: true,
                request: request("share-group", &[ReadPartition {
                    partition: 6,
                    leader_epoch: 3,
                }]),
                expected: response(vec![error_row(
                    6,
                    codes::INVALID_REQUEST,
                    "Read operation on uninitialized share partition not allowed.",
                )]),
            },
            StoredStateRow {
                led: true,
                request: request("share-group", &[ReadPartition {
                    partition: 4,
                    leader_epoch: 2,
                }]),
                expected: response(vec![error_row(
                    4,
                    codes::FENCED_LEADER_EPOCH,
                    "The leader epoch in the request is older than the epoch on the broker.",
                )]),
            },
            StoredStateRow {
                led: false,
                request: request("share-group", &[ReadPartition {
                    partition: 4,
                    leader_epoch: 3,
                }]),
                expected: response(vec![error_row(
                    4,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state: This is not the correct coordinator.",
                )]),
            },
        ];
        let empty_requests = EmptyStateRequests {
            group: request("", &[ReadPartition {
                partition: 4,
                leader_epoch: 3,
            }]),
            partitions: request("share-group", &[]),
            topics: ReadShareGroupStateRequest {
                group_id: "share-group".into(),
                ..Default::default()
            },
        };

        check_stored_rows(
            rows.into_iter().chain(empty_requests.into_rows(empty)),
            TOPIC,
            8,
            4,
            read_state,
        )
        .await;
    }
}
