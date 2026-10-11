//! Topic-level response assembly for the KIP-516 id-resolution failures,
//! which answer every partition row of one topic with the same error code.

use super::{INVALID_OFFSET, framing::FramedTopic};

/// Build a topic-level error response for the KIP-516 id-resolution failures
/// `UNKNOWN_TOPIC_ID` and `INCONSISTENT_TOPIC_ID`.
///
/// Every partition row in the request gets the same error code. The function
/// sets `base_offset` to -1 to signal "no offset assigned". This matches
/// Kafka's behavior on pre-append errors.
pub(super) fn build_topic_error_response(
    topic: &FramedTopic,
    code: i16,
) -> krabka_protocol::owned::produce_response::TopicProduceResponse {
    use krabka_protocol::owned::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    TopicProduceResponse {
        name: topic.name.clone(),
        topic_id: topic.topic_id,
        partition_responses: topic
            .partition_data
            .iter()
            .map(|p| PartitionProduceResponse {
                index: p.index,
                error_code: code,
                base_offset: INVALID_OFFSET,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::{
        codes::UNKNOWN_TOPIC_ID,
        handlers::produce::framing::{FramedPartition, PartitionPayload},
    };

    #[test]
    fn build_topic_error_response_preserves_topic_and_partition_fields() {
        use krabka_protocol::owned::produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
        };
        let topic_id = krabka_protocol::primitives::uuid::Uuid([7; 16]);
        let topic = FramedTopic {
            name: "orders".into(),
            topic_id,
            partition_data: vec![
                FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Null,
                },
                FramedPartition {
                    index: 4,
                    payload: PartitionPayload::Slice(Bytes::from_static(b"not-a-batch")),
                },
            ],
        };

        let resp = build_topic_error_response(&topic, UNKNOWN_TOPIC_ID);

        let error_partition = |index: i32| {
            tagged_wire!(PartitionProduceResponse {
                index,
                error_code: crate::codes::UNKNOWN_TOPIC_ID,
                base_offset: -1,
                log_append_time_ms: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
                current_leader: tagged_wire!(LeaderIdAndEpoch {
                    leader_id: -1,
                    leader_epoch: -1,
                }),
            })
        };
        let expected = tagged_wire!(TopicProduceResponse {
            name: "orders".to_string(),
            topic_id,
            partition_responses: vec![error_partition(0), error_partition(4)],
        });
        assert!(resp == expected);
    }
}
