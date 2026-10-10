//! The audit trail for `DeleteAcls`: which of the matched ACL entries actually
//! left the metadata log, and the single `AdminOperation` event they produce.
//!
//! A deletion counts as audited only when its filter result still holds `NONE`
//! after the controller submit, so the selection is a scan over the response
//! rows rather than over the request. That join is worth its own file.

use krabka_protocol::owned::delete_acls_response::DeleteAclsFilterResult;

use crate::{
    codes,
    handlers::{RequestContext, audit_admin_success, audit_resource},
};

pub(super) fn deleted_acl_resources(
    filter_results: &[DeleteAclsFilterResult],
) -> Vec<krabka_audit::AuditResource> {
    filter_results
        .iter()
        .filter(|r| r.error_code == codes::NONE)
        .flat_map(|r| r.matching_acls.iter())
        .map(|m| audit_resource("Acl", m.resource_name.clone()))
        .collect()
}

/// Emits one `DeleteAcls` `AdminOperation` record for the deleted ACLs, or
/// nothing when the request deleted none.
pub(super) fn audit_deleted_acls(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    deleted_acls: Vec<krabka_audit::AuditResource>,
) {
    audit_admin_success(audit_log, ctx, "DeleteAcls", deleted_acls);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::AclOperation;

    use super::*;
    use crate::handlers::delete_acls::{
        response::{filter_result, matching_acl_result},
        test_support::acl,
    };

    #[test]
    fn deleted_acl_resources_include_only_successful_matches() {
        let ok = filter_result(
            codes::NONE,
            None,
            vec![matching_acl_result(&acl(
                crate::test_support::AllowAclSetup::default(),
            ))],
        );
        let failed = filter_result(
            codes::COORDINATOR_NOT_AVAILABLE,
            Some("submit failed".into()),
            vec![matching_acl_result(&acl(
                crate::test_support::AllowAclSetup {
                    resource_name: "payments",
                    principal: "User:bob",
                    operation: AclOperation::Write,
                    ..Default::default()
                },
            ))],
        );

        let resources = deleted_acl_resources(&[ok, failed]);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Acl".into(),
            name: "orders".into(),
        }];
        assert!(resources == expected);
    }
}
