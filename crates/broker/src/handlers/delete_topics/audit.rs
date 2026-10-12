//! The audit trail for `DeleteTopics`.
//!
//! Only topics that actually went away are auditable, so the response rows are
//! filtered down to the successful, named ones. Each owned deletion task
//! emits its `AdminOperation` record after commit and cleanup, even when the
//! client stops waiting before the commit completes.

use krabka_protocol::owned::delete_topics_response::DeletableTopicResult;

use crate::{
    codes,
    handlers::{RequestContext, audit_admin_success, audit_resource},
};

/// Picks the audit resources for the topics this request actually deleted.
///
/// A row with a non-zero error code did not delete anything, and a row without
/// a name was requested by an id that resolved to nothing.
pub(super) fn deleted_topic_resources(
    results: &[DeletableTopicResult],
) -> Vec<krabka_audit::AuditResource> {
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .filter_map(|t| t.name.as_deref().map(|n| audit_resource("Topic", n)))
        .collect()
}

/// Emits a `DeleteTopics` `AdminOperation` record for committed deletions,
/// or nothing when the request deleted none.
pub(super) fn audit_deleted_topics(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    deleted: Vec<krabka_audit::AuditResource>,
) {
    audit_admin_success(audit_log, ctx, "DeleteTopics", deleted);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;

    use super::*;
    use crate::handlers::delete_topics::wire::delete_topic_result;

    #[test]
    fn deleted_topic_resources_include_only_successful_named_topics() {
        let results = vec![
            delete_topic_result(Some("ok".into()), WireUuid::ZERO, codes::NONE),
            delete_topic_result(
                Some("denied".into()),
                WireUuid::ZERO,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ),
            delete_topic_result(None, WireUuid([1; 16]), codes::NONE),
        ];

        let resources = deleted_topic_resources(&results);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }
}
