//! `DeleteShareGroupState` (`api_key=86`). The handler tombstones the durable
//! share state for each `(group, topic, partition)` that has state and drops
//! the in-memory entry. The handler gates on local leadership of the target
//! `__share_group_state` partition.

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::{
    delete_share_group_state_request::DeleteShareGroupStateRequest,
    delete_share_group_state_response::{
        DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult,
    },
};

use crate::share_coordinator::coordinator::ShareCoordinator;

super::share_state_handler!(
    DeleteShareGroupStateRequest,
    DeleteShareGroupStateResponse,
    DeleteStateResult,
    PartitionResult,
    delete_state
);

/// Deletes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.deleteState` does.
///
/// An empty group id, an empty topic list, or a topic with no partitions gets
/// a response with no results.
async fn delete_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: DeleteShareGroupStateRequest,
) -> DeleteShareGroupStateResponse {
    if req.group_id.is_empty() || super::empty_partition_data!(req) {
        return DeleteShareGroupStateResponse::default();
    }
    let results = super::state_results!(req, DeleteStateResult, |group_id, topic_id, pd| {
        let result = coordinator
            .delete(image, group_id, topic_id, pd.partition)
            .await;
        let (error_code, error_message) = super::operation_result(result, "delete");
        PartitionResult {
            partition: pd.partition,
            error_code,
            error_message,
            ..Default::default()
        }
    });

    DeleteShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::PartitionIndex;
    use krabka_log::Offset;
    use krabka_protocol::{
        owned::delete_share_group_state_request::{DeleteStateData, PartitionData},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::{
        codes,
        share_coordinator::coordinator::test_support::{
            Logged, image_with_topic, logged_records, logged_since,
        },
        test_support::KafkaErrorCode,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([31; 16]);
    const UNKNOWN_TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([34; 16]);

    super::super::test_support::request_fixture! {
        DeleteShareGroupStateRequest, DeleteStateData, PartitionData;
        fn request(group_id: &str, [topic_id: uuid::Uuid], partitions: &[i32]);
        topic ProtoUuid(*topic_id.as_bytes()); |partition| {
            PartitionData { partition, ..Default::default() }
        }
    }

    super::super::test_support::response_fixture!(DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult; keyed);

    /// The whole response and the appended records for each request, over a
    /// state for partition 0 only of a two-partition topic, as Kafka's
    /// `ShareCoordinatorShard.deleteState` answers it.
    #[tokio::test]
    async fn delete_state_answers_as_kafka() {
        let unknown = Some("This server does not host this topic-partition.");
        // (name, led, request, expected response, appended records, partition
        //  0 still has state)
        let rows = [
            (
                "a key with state is tombstoned",
                true,
                request("g", TOPIC, &[0]),
                response(StateResponseSetup::default()),
                vec![Logged::Tombstone],
                false,
            ),
            (
                "a key with no state writes nothing",
                true,
                request("g", TOPIC, &[1]),
                response(StateResponseSetup {
                    partition: PartitionIndex(1),
                    ..Default::default()
                }),
                vec![],
                true,
            ),
            (
                "a negative partition",
                true,
                request("g", TOPIC, &[-1]),
                response(StateResponseSetup {
                    partition: PartitionIndex(-1),
                    code: KafkaErrorCode(codes::INVALID_REQUEST),
                    message: Some("The partition id cannot be a negative number."),
                    ..Default::default()
                }),
                vec![],
                true,
            ),
            (
                "a partition past the partition count",
                true,
                request("g", TOPIC, &[2]),
                response(StateResponseSetup {
                    partition: PartitionIndex(2),
                    code: KafkaErrorCode(codes::UNKNOWN_TOPIC_OR_PARTITION),
                    message: unknown,
                    ..Default::default()
                }),
                vec![],
                true,
            ),
            (
                "a topic id the image does not hold",
                true,
                request("g", UNKNOWN_TOPIC, &[0]),
                response(StateResponseSetup {
                    topic: UNKNOWN_TOPIC,
                    code: KafkaErrorCode(codes::UNKNOWN_TOPIC_OR_PARTITION),
                    message: unknown,
                    ..Default::default()
                }),
                vec![],
                true,
            ),
            (
                "an empty group id",
                true,
                request("", TOPIC, &[0]),
                DeleteShareGroupStateResponse::default(),
                vec![],
                true,
            ),
            (
                "a topic with no partitions",
                true,
                request("g", TOPIC, &[]),
                DeleteShareGroupStateResponse::default(),
                vec![],
                true,
            ),
            (
                "a state partition this broker does not lead",
                false,
                request("g", TOPIC, &[0]),
                response(StateResponseSetup {
                    code: KafkaErrorCode(codes::NOT_COORDINATOR),
                    message: Some(
                        "Unable to delete share group state: This is not the correct coordinator.",
                    ),
                    ..Default::default()
                }),
                vec![],
                // The resignation itself drops the in-memory keys.
                false,
            ),
        ];

        for (name, led, req, expected, appended, kept) in rows {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator = super::super::test_support::coordinator(dir.path());
            let image = image_with_topic(TOPIC, 2);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize(&image, "g", TOPIC, 0, 1, Offset(0))
                .await
                .expect("seed state");
            let partition = req.topics[0].partitions.first().map_or(0, |p| p.partition);
            let topic_id = uuid::Uuid::from_bytes(req.topics[0].topic_id.0);
            let state_partition = coordinator.state_partition_for("g", &topic_id, partition);
            let before = logged_records(&coordinator, state_partition).len();
            if !led {
                coordinator
                    .refresh_leader_partitions(&MetadataImage::default())
                    .await
                    .finished()
                    .await;
            }

            let resp = delete_state(&coordinator, &image, req).await;
            check!(resp == expected, "{name}");
            let logged = logged_since(&coordinator, state_partition, before);
            check!(logged == appended, "{name}");
            check!(
                coordinator.state_for_test("g", TOPIC, 0).await.is_some() == kept,
                "{name}"
            );
        }
    }
}
