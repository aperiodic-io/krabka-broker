//! The `CreateAcls` response rows: one result per creation, the envelope that
//! carries them, the bulk stamp a failed controller submit leaves on the
//! creations it covered.
//!
//! Kafka answers `CreateAcls` positionally, so every path through the handler
//! has to produce exactly one row per request creation. Keeping the row
//! constructors together makes that invariant easy to see.

use {
    super::ValidCreation,
    crate::{codes, handlers::submit_failure_code},
    krabka_protocol::owned::create_acls_response::{AclCreationResult, CreateAclsResponse},
    krabka_raft::RaftError,
};

/// The row for a creation the controller committed.
///
/// Kafka's `AclApis.handleCreateAcls` answers a successful creation with a
/// bare `new AclCreationResult()` and sets the error fields only when the
/// creation failed, so the row carries the generated default for
/// `ErrorMessage`: the empty string, not null.
pub(super) fn acl_success_result() -> AclCreationResult {
    AclCreationResult {
        error_code: codes::NONE,
        error_message: Some(String::new()),
        ..Default::default()
    }
}

pub(super) fn acl_error_result(code: i16, msg: impl Into<String>) -> AclCreationResult {
    AclCreationResult {
        error_code: code,
        error_message: Some(msg.into()),
        ..Default::default()
    }
}

pub(super) fn create_acls_response(results: Vec<AclCreationResult>) -> CreateAclsResponse {
    CreateAclsResponse {
        results,
        ..Default::default()
    }
}

/// Stamps every submitted creation with the error a failed controller write
/// maps to.
///
/// Kafka's controller fails the `createAcls` write with
/// `NotControllerException` when it is not (or is no longer) the active
/// controller, and wraps anything else as an `UnknownServerException`, whose
/// text `ApiError.fromThrowable` drops so no internal detail reaches the
/// client. Neither carries a coordinator error, and neither carries a message:
/// Kafka writes `ApiError.message()`, which is null for both.
pub(super) fn apply_submit_error(
    results: &mut [AclCreationResult],
    to_submit: &[ValidCreation],
    err: &RaftError,
) {
    let code = match err {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        other => submit_failure_code(other, codes::UNKNOWN_SERVER_ERROR),
    };
    for creation in to_submit {
        results[creation.result_index] = AclCreationResult {
            error_code: code,
            error_message: None,
            ..Default::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::create_acls::test_support::{OPERATION_READ, creation, validate},
        assert2::assert,
        krabka_metadata::MetadataRecord,
    };

    #[test]
    fn submit_error_maps_controller_failures_and_leaves_rejected_rows() {
        let submitted = vec![ValidCreation {
            result_index: 0,
            record: MetadataRecord::V1AccessControlEntry(
                validate(&creation("topic-a", "User:alice", OPERATION_READ))
                    .expect("valid creation"),
            ),
        }];
        let cases = [
            (
                RaftError::NotLeader {
                    current_leader: None,
                },
                codes::NOT_CONTROLLER,
            ),
            (RaftError::LeaderUnknown, codes::NOT_CONTROLLER),
            (RaftError::UncommittedTail, codes::NOT_CONTROLLER),
            (RaftError::Shutdown, codes::UNKNOWN_SERVER_ERROR),
            (
                RaftError::ChangeRejected("internal detail".into()),
                codes::UNKNOWN_SERVER_ERROR,
            ),
        ];
        for (error, code) in cases {
            let mut results = vec![
                acl_success_result(),
                acl_error_result(codes::INVALID_REQUEST, "already invalid"),
            ];
            apply_submit_error(&mut results, &submitted, &error);
            let expected = vec![
                tagged_wire!(AclCreationResult {
                    error_code: code,
                    error_message: None,
                }),
                tagged_wire!(AclCreationResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("already invalid".into()),
                }),
            ];
            assert!(results == expected, "{error}");
        }
    }
}
