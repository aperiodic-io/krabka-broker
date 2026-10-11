//! The OCSF Authentication record (class 3002), which reports one logon
//! attempt: the SASL or TLS mechanism that carried it, the principal it
//! claimed, and why it failed when it did.

use serde_json::json;

use super::{
    ProductInfo,
    envelope::{metadata, status_id},
};
use crate::event::{AuditEndpoint, AuditOutcome, AuditPrincipal};

pub(super) fn ocsf_authentication(
    outcome: AuditOutcome,
    mechanism: &str,
    principal: &AuditPrincipal,
    source: &AuditEndpoint,
    reason: Option<&String>,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3002 Authentication, activity 1 = Logon.
    let class_uid = 3002_i64;
    let activity_id = 1_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Logon",
        "time": time_ms,
        "status_id": status_id(outcome),
        "status_detail": reason,
        "auth_protocol": mechanism,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "metadata": metadata(product),
    })
}
