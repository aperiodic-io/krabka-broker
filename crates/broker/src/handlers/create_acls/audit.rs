//! The audit trail for `CreateAcls`: which creations actually reached the
//! metadata log, and the single `AdminOperation` event they produce.
//!
//! A creation counts as audited only when it passed validation and its result
//! row still holds `NONE` after the controller submit, so the selection is a
//! join over the request, the results, and the submitted records. That join is
//! worth its own file.

use {
    super::ValidCreation,
    crate::{
        codes,
        handlers::{RequestContext, audit_admin_success, audit_resource},
    },
    krabka_protocol::owned::{
        create_acls_request::CreateAclsRequest, create_acls_response::AclCreationResult,
    },
};

pub(super) fn created_acl_resources(
    req: &CreateAclsRequest,
    results: &[AclCreationResult],
    to_submit: &[ValidCreation],
) -> Vec<krabka_audit::AuditResource> {
    to_submit
        .iter()
        .filter(|creation| results[creation.result_index].error_code == codes::NONE)
        .map(|creation| {
            audit_resource(
                "Acl",
                req.creations[creation.result_index].resource_name.clone(),
            )
        })
        .collect()
}

/// Emits one `CreateAcls` `AdminOperation` record for the created ACLs, or
/// nothing when the request created none.
pub(super) fn audit_created_acls(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    created_acls: Vec<krabka_audit::AuditResource>,
) {
    audit_admin_success(audit_log, ctx, "CreateAcls", created_acls);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::create_acls::{
            response::{acl_error_result, acl_success_result},
            test_support::{OPERATION_READ, OPERATION_WRITE, creation, request, validate},
        },
        assert2::assert,
        krabka_metadata::MetadataRecord,
    };

    #[test]
    fn created_acl_resources_include_only_successful_submitted_creations() {
        let req = request(vec![
            creation("topic-ok", "User:alice", OPERATION_READ),
            creation("topic-bad", "User:bob", OPERATION_WRITE),
        ]);
        let submitted = vec![
            ValidCreation {
                result_index: 0,
                record: MetadataRecord::V1AccessControlEntry(validate(&req.creations[0]).unwrap()),
            },
            ValidCreation {
                result_index: 1,
                record: MetadataRecord::V1AccessControlEntry(validate(&req.creations[1]).unwrap()),
            },
        ];
        let results = vec![
            acl_success_result(),
            acl_error_result(codes::COORDINATOR_NOT_AVAILABLE, "submit failed"),
        ];

        let resources = created_acl_resources(&req, &results, &submitted);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Acl".to_string(),
            name: "topic-ok".to_string(),
        }];
        assert!(resources == expected);
    }
}
