//! The OCSF Application Lifecycle record (class 6002), which reports that a
//! broker started, began stopping, applied a configuration change, or reloaded
//! its TLS material. The record names the broker as the OCSF device rather
//! than a principal, because no client requested it.

use {
    super::{ProductInfo, envelope::metadata},
    crate::{event::LifecycleKind, ids::EpochMs},
    krabka_ids::NodeId,
    serde_json::json,
};

pub(super) fn ocsf_lifecycle(
    kind: LifecycleKind,
    node_id: NodeId,
    time_ms: EpochMs,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6002 Application Lifecycle.
    let class_uid = 6002_i64;
    let (activity_id, activity_name) = match kind {
        LifecycleKind::BrokerStarted => (1_i64, "BrokerStarted"),
        LifecycleKind::BrokerStopping => (4, "BrokerStopping"),
        LifecycleKind::ConfigApplied => (3, "ConfigApplied"),
        LifecycleKind::TlsReloaded => (3, "TlsReloaded"),
    };
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": activity_name,
        "time": time_ms.0,
        "status_id": 1,
        "device": { "uid": node_id.0.to_string(), "type_id": 1 },
        "metadata": metadata(product),
    })
}
