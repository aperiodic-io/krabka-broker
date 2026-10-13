//! The OCSF Authorize Session record (class 3003), which reports an ACL check
//! that denied a principal access to a named resource. Only denials reach this
//! module, so the record is always a failure.

use {
    super::{ProductInfo, envelope::metadata},
    crate::event::{AuditEndpoint, AuditPrincipal},
    serde_json::json,
};

pub(super) fn ocsf_authorization_denied(
    principal: &AuditPrincipal,
    source: &AuditEndpoint,
    resource_type: &str,
    resource_name: &str,
    operation: &str,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3003 Authorize Session, activity 2 = Deny.
    let class_uid = 3003_i64;
    let activity_id = 2_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Deny",
        "time": time_ms,
        "status_id": 2,
        "operation": operation,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": [ { "type": resource_type, "name": resource_name } ],
        "metadata": metadata(product),
    })
}
