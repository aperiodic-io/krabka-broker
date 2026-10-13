//! The parts of an OCSF record that do not vary with its class: the `metadata`
//! block that stamps the schema version and the emitting product, and the
//! `status_id` code that reports whether the audited operation succeeded.

use {super::ProductInfo, crate::event::AuditOutcome, serde_json::json};

const SCHEMA_VERSION: &str = "1.3.0";

pub(super) fn status_id(outcome: AuditOutcome) -> i64 {
    match outcome {
        AuditOutcome::Success => 1,
        AuditOutcome::Failure => 2,
    }
}

pub(super) fn metadata(product: &ProductInfo) -> serde_json::Value {
    json!({
        "version": SCHEMA_VERSION,
        "product": {
            "vendor_name": product.vendor_name,
            "name": product.name,
            "version": product.version,
        }
    })
}
