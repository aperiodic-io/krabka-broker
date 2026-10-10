//! Response assembly for `AlterPartitionReassignments`: the per-partition
//! result rows and the whole-request error envelope.

use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
    },
};

/// A partition the request altered, or would have.
///
/// Kafka's `ReplicationControlManager.alterPartitionReassignments` sets
/// `ErrorMessage` from `ApiError.NONE.message()`, which is null.
pub(super) fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: 0,
        error_message: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn err_row(
    partition_index: i32,
    code: i16,
    msg: String,
) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
) -> AlterPartitionReassignmentsResponse {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        // Kafka's `AlterPartitionReassignmentsRequest.getErrorResponse` leaves
        // this at its schema default, `true`, whatever the request asked.
        allow_replication_factor_change: true,
        error_code: code,
        error_message: Some(msg.to_string()),
        responses,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        codes::{CLUSTER_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION},
        handlers::alter_partition_reassignments::test_support::{
            ReassignmentRequestSetup, request,
        },
    };

    #[test]
    fn row_builders_preserve_non_default_fields() {
        let ok = ok_row(7);
        let expected_ok = tagged_wire!(ReassignablePartitionResponse {
            partition_index: 7,
            error_code: 0,
            error_message: None,
        });
        assert!(ok == expected_ok);

        let err = err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "missing partition".into());
        let expected_err = tagged_wire!(ReassignablePartitionResponse {
            partition_index: 8,
            error_code: UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("missing partition".into()),
        });
        assert!(err == expected_err);
    }

    #[test]
    fn whole_request_error_preserves_request_shape() {
        let req = request(ReassignmentRequestSetup {
            topic: "payments",
            partition_index: krabka_ids::PartitionIndex(8),
            ..Default::default()
        });

        let resp = whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied");

        let expected = unthrottled_wire!(AlterPartitionReassignmentsResponse {
            allow_replication_factor_change: true,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("denied".into()),
            responses: vec![tagged_wire!(ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![tagged_wire!(ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                })],
            })],
        });
        assert!(resp == expected);
    }
}
