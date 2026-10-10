//! `WriteShareGroupState` (`api_key=85`). The handler applies a delivery-state
//! delta for each `(group, topic, partition)`. The delta advances the start
//! offset, upserts state batches, and sets the delivery-complete count. It
//! keeps the stored state epoch and leader epoch. A key with no state returns
//! `INVALID_REQUEST`, and epoch fencing returns the per-partition error code.

use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use krabka_protocol::owned::{
    write_share_group_state_request::WriteShareGroupStateRequest,
    write_share_group_state_response::{
        PartitionResult, WriteShareGroupStateResponse, WriteStateResult,
    },
};

use crate::{
    codes,
    share_coordinator::{
        coordinator::{ShareCoordinator, ShareWrite},
        persistence::StateBatch,
    },
};

super::share_state_handler!(
    WriteShareGroupStateRequest,
    WriteShareGroupStateResponse,
    WriteStateResult,
    PartitionResult,
    write_state
);

/// Applies every partition of `req`, as Kafka's
/// `ShareCoordinatorService.writeState` does.
///
/// An empty topic list, a topic with no partitions, or an empty group id gets
/// a response with no results.
async fn write_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: WriteShareGroupStateRequest,
) -> WriteShareGroupStateResponse {
    if super::empty_partition_data!(req) || req.group_id.is_empty() {
        return WriteShareGroupStateResponse::default();
    }
    let results = super::state_results!(req, WriteStateResult, |group_id, topic_id, pd| {
        let request = ShareWrite {
            state_epoch: pd.state_epoch,
            leader_epoch: pd.leader_epoch,
            start_offset: Offset(pd.start_offset),
            delivery_complete_count: pd.delivery_complete_count,
            batches: pd.state_batches.iter().map(StateBatch::from).collect(),
        };
        let result = coordinator
            .write(image, group_id, topic_id, pd.partition, request)
            .await;
        match result {
            Ok(()) => PartitionResult {
                partition: pd.partition,
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            },
            Err(error) => PartitionResult {
                partition: pd.partition,
                error_code: error.code(),
                error_message: Some(error.row_message("write")),
                ..Default::default()
            },
        }
    });

    WriteShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::write_share_group_state_request::{
            PartitionData, StateBatch as WireStateBatch, WriteStateData,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([51; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([51; 16]);

    fn request(group_id: &str, partitions: &[(i32, i32)]) -> WriteShareGroupStateRequest {
        WriteShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![WriteStateData {
                topic_id: WIRE_TOPIC,
                partitions: partitions
                    .iter()
                    .map(|&(partition, state_epoch)| PartitionData {
                        partition,
                        state_epoch,
                        leader_epoch: 3,
                        start_offset: 101,
                        delivery_complete_count: 9,
                        state_batches: vec![WireStateBatch {
                            first_offset: 101,
                            last_offset: 105,
                            delivery_state: 2,
                            delivery_count: 3,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn row(partition: i32, error_code: i16, message: Option<&str>) -> PartitionResult {
        PartitionResult {
            partition,
            error_code,
            error_message: message.map(str::to_owned),
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }
    }

    super::super::test_support::response_fixture!(
        WriteShareGroupStateResponse,
        WriteStateResult,
        PartitionResult,
        WIRE_TOPIC
    );

    /// The whole `WriteShareGroupStateResponse` for each request shape, over
    /// one initialized key (partition 4, state epoch 17), and the stored
    /// summary after the request.
    #[tokio::test]
    async fn write_state_answers_as_kafka() {
        let initial = Some((17, 0, Offset(90), 0));
        // (led, request, expected response, summary of partition 4 after)
        let rows = [
            (
                true,
                request("share-group", &[(4, 17)]),
                response(vec![row(4, codes::NONE, None)]),
                Some((17, 0, Offset(101), 9)),
            ),
            (
                true,
                request("share-group", &[(6, 17)]),
                response(vec![row(
                    6,
                    codes::INVALID_REQUEST,
                    Some("Write operation on uninitialized share partition not allowed."),
                )]),
                initial,
            ),
            (
                true,
                request("share-group", &[(4, 16)]),
                response(vec![row(
                    4,
                    codes::FENCED_STATE_EPOCH,
                    Some(
                        "The coordinator rejected the request because the state epoch did not match.",
                    ),
                )]),
                initial,
            ),
            (
                false,
                request("share-group", &[(4, 17)]),
                response(vec![row(
                    4,
                    codes::NOT_COORDINATOR,
                    Some("Unable to write share group state: This is not the correct coordinator."),
                )]),
                None,
            ),
            (
                true,
                request("", &[(4, 17)]),
                WriteShareGroupStateResponse::default(),
                initial,
            ),
            (
                true,
                request("share-group", &[]),
                WriteShareGroupStateResponse::default(),
                initial,
            ),
        ];

        for (index, (led, req, expected, summary)) in rows.into_iter().enumerate() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let (coordinator, image) = super::super::test_support::initialized_state(
                dir.path(),
                super::super::test_support::StateFixtureSetup {
                    topic: TOPIC,
                    partitions: crate::test_support::PartitionCount(8),
                    partition: krabka_ids::PartitionIndex(4),
                },
            )
            .await;
            super::super::test_support::retain_leadership(&coordinator, led).await;

            let resp = write_state(&coordinator, &image, req).await;
            check!(resp == expected, "row {index}");
            let stored = coordinator
                .read_summary("share-group", TOPIC, 4)
                .await
                .ok()
                .flatten();
            check!(stored == summary, "row {index}");
        }
    }
}
