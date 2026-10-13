//! Constructors for the `DeleteRecords` response tree: one partition row, one
//! topic group, and the response that carries them.
//!
//! Every row the handler emits goes through these, so the wire shape of a
//! success, an error, and a denial is decided in one place. The `-1`
//! `low_watermark` on an error row is the value Kafka reports for a partition
//! it did not trim.

use krabka_protocol::owned::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};

pub(super) fn partition_result(
    partition_index: i32,
    low_watermark: i64,
    error_code: i16,
) -> DeleteRecordsPartitionResult {
    DeleteRecordsPartitionResult {
        partition_index,
        low_watermark,
        error_code,
        ..Default::default()
    }
}

pub(super) fn error_partition_result(
    partition_index: i32,
    error_code: i16,
) -> DeleteRecordsPartitionResult {
    partition_result(partition_index, -1, error_code)
}

pub(super) fn topic_result(
    name: String,
    partitions: Vec<DeleteRecordsPartitionResult>,
) -> DeleteRecordsTopicResult {
    DeleteRecordsTopicResult {
        name,
        partitions,
        ..Default::default()
    }
}

pub(super) fn delete_records_response(
    topics: Vec<DeleteRecordsTopicResult>,
) -> DeleteRecordsResponse {
    DeleteRecordsResponse {
        topics,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::codes, assert2::assert};

    #[test]
    fn response_helpers_preserve_topic_and_partition_fields() {
        let denied = error_partition_result(7, codes::TOPIC_AUTHORIZATION_FAILED);
        let expected_denied = tagged_wire!(DeleteRecordsPartitionResult {
            partition_index: 7,
            low_watermark: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        });
        assert!(denied == expected_denied);

        let ok = partition_result(3, 44, codes::NONE);
        let expected_ok = tagged_wire!(DeleteRecordsPartitionResult {
            partition_index: 3,
            low_watermark: 44,
            error_code: codes::NONE,
        });
        assert!(ok == expected_ok);

        let topic = topic_result("orders".into(), vec![denied]);
        let expected_topic = tagged_wire!(DeleteRecordsTopicResult {
            name: "orders".into(),
            partitions: vec![expected_denied],
        });
        assert!(topic == expected_topic);

        let resp = delete_records_response(vec![topic]);
        let expected_resp = unthrottled_wire!(DeleteRecordsResponse {
            topics: vec![expected_topic],
        });
        assert!(resp == expected_resp);
    }
}
