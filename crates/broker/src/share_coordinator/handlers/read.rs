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
    if super::empty_partition_data!(req) || req.group_id.is_empty() {
        return ReadShareGroupStateResponse::default();
    }
    let results = super::state_results!(req, ReadStateResult, |group_id, topic_id, pd| {
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
        super::*,
        crate::codes,
        krabka_protocol::{
            UnknownTaggedFields,
            owned::read_share_group_state_request::{PartitionData, ReadStateData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([33; 16]);

    super::super::test_support::request_fixture! {
        ReadShareGroupStateRequest, ReadStateData, PartitionData;
        fn request(group_id: &str, [], partitions: &[(i32, i32)]);
        topic WIRE_TOPIC; |(partition, leader_epoch)| {
            PartitionData { partition, leader_epoch, ..Default::default() }
        }
    }

    super::super::test_support::response_fixture!(
        ReadShareGroupStateResponse,
        ReadStateResult,
        PartitionResult,
        WIRE_TOPIC
    );

    super::super::test_support::error_row_fixture!(PartitionResult);

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
        // (led, request, expected response)
        let rows = [
            (
                true,
                request("share-group", &[(4, 3)]),
                response(vec![stored]),
            ),
            (
                true,
                request("share-group", &[(6, 3)]),
                response(vec![error_row(
                    6,
                    codes::INVALID_REQUEST,
                    "Read operation on uninitialized share partition not allowed.",
                )]),
            ),
            (
                true,
                request("share-group", &[(4, 2)]),
                response(vec![error_row(
                    4,
                    codes::FENCED_LEADER_EPOCH,
                    "The leader epoch in the request is older than the epoch on the broker.",
                )]),
            ),
            (
                false,
                request("share-group", &[(4, 3)]),
                response(vec![error_row(
                    4,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state: This is not the correct coordinator.",
                )]),
            ),
            (true, request("", &[(4, 3)]), empty.clone()),
            (true, request("share-group", &[]), empty.clone()),
            (
                true,
                super::super::test_support::no_topics!(ReadShareGroupStateRequest, "share-group"),
                empty,
            ),
        ];

        super::super::test_support::stored_response_rows!(rows, TOPIC, (8, 4), read_state);
    }
}
