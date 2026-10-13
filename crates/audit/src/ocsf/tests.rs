//! Unit tests for the OCSF mapping.
//!
//! Each test pins one audit event to the class, category, and activity ids the
//! OCSF schema assigns it, because those numbers are what a downstream SIEM
//! matches on and a silent change to one is not otherwise visible.

use {
    super::*,
    crate::{
        event::*,
        test_support::{admin, authentication, denied, privileged_freeze},
    },
    assert2::check,
};

fn product() -> ProductInfo {
    ProductInfo {
        vendor_name: "Krabka".into(),
        name: "krabka-broker".into(),
        version: "0.3.7".into(),
    }
}

/// A JSON pointer into an OCSF document and the value expected there.
struct ExpectedField {
    path: &'static str,
    value: serde_json::Value,
}

fn field(path: &'static str, value: serde_json::Value) -> ExpectedField {
    ExpectedField { path, value }
}

fn check_fields(value: &serde_json::Value, expected: &[ExpectedField]) {
    for ExpectedField {
        path,
        value: expected,
    } in expected
    {
        check!(value.pointer(path) == Some(expected), "field {path}");
    }
}

#[test]
fn authentication_failure_maps_to_3002() {
    let ev = authentication(51120, Some("authentication failed"), 1_700_000_000_000);
    let j = to_ocsf(&ev, &product());
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(3002)),
        field("/category_uid", serde_json::json!(3)),
        field("/status_id", serde_json::json!(2)),
        field("/time", serde_json::json!(1_700_000_000_000_i64)),
        field("/actor/user/name", serde_json::json!("alice")),
        field("/src_endpoint/ip", serde_json::json!("10.0.0.1")),
        field("/src_endpoint/port", serde_json::json!(51120)),
        field("/auth_protocol", serde_json::json!("SASL/PLAIN")),
        field("/metadata/product/vendor_name", serde_json::json!("Krabka")),
    ]);
}

#[test]
fn authorization_denied_maps_to_3003_failure() {
    let ev = denied(5);
    let j = to_ocsf(&ev, &product());
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(3003)),
        field("/status_id", serde_json::json!(2)),
        field("/type_uid", serde_json::json!(300_302_i64)),
        field("/actor/user/name", serde_json::json!("bob")),
        field("/resources/0/type", serde_json::json!("Topic")),
        field("/resources/0/name", serde_json::json!("secrets")),
        field("/operation", serde_json::json!("Write")),
    ]);
}

#[test]
fn admin_operation_maps_to_6003_with_resources() {
    let ev = admin(6);
    let j = to_ocsf(&ev, &product());
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(6003)),
        field("/category_uid", serde_json::json!(6)),
        field("/status_id", serde_json::json!(1)),
        field("/api/operation", serde_json::json!("CreateTopics")),
        field("/resources/0/name", serde_json::json!("orders")),
    ]);
}

fn expected_metadata() -> serde_json::Value {
    serde_json::json!({
        "version": "1.3.0",
        "product": {
            "vendor_name": "Krabka",
            "name": "krabka-broker",
            "version": "0.3.7",
        }
    })
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct EventTimeMillis(i64);

#[derive(krabka_macros::FieldDefaults)]
struct UnsignedPrivilegeSetup<'a> {
    #[default(AuditOutcome::Success)]
    outcome: AuditOutcome,
    #[default(PrivilegedPhase::Consumed)]
    phase: PrivilegedPhase,
    #[default("unclean_elect_leaders")]
    action: &'a str,
    #[default("orders-3")]
    target: &'a str,
    #[default("bg-7")]
    proposal_id: &'a str,
    #[default(AuditPrincipal { name: "User:alice".into(), auth_method: "MTls".into() })]
    principal: AuditPrincipal,
    counterparties: Vec<AuditPrincipal>,
    #[default(AuditEndpoint { ip: "10.0.0.4".into(), port: 9092 })]
    source: AuditEndpoint,
    reason: &'a str,
    time_ms: EventTimeMillis,
}

#[bon::builder]
fn unsigned_privilege(
    #[builder(default = AuditOutcome :: Success)] outcome: AuditOutcome,
    #[builder(default = PrivilegedPhase :: Consumed)] phase: PrivilegedPhase,
    #[builder(default = "unclean_elect_leaders")] action: &'_ str,
    #[builder(default = "orders-3")] target: &'_ str,
    #[builder(default = "bg-7")] proposal_id: &'_ str,
    #[builder(default = AuditPrincipal { name : "User:alice" . into () , auth_method : "MTls" . into () })]
    principal: AuditPrincipal,
    #[builder(default)] counterparties: Vec<AuditPrincipal>,
    #[builder(default = AuditEndpoint { ip : "10.0.0.4" . into () , port : 9092 })]
    source: AuditEndpoint,
    #[builder(default)] reason: &'_ str,
    #[builder(into, default)] time_ms: EventTimeMillis,
) -> AuditEvent {
    let setup = UnsignedPrivilegeSetup {
        outcome,
        phase,
        action,
        target,
        proposal_id,
        principal,
        counterparties,
        source,
        reason,
        time_ms,
    };
    let UnsignedPrivilegeSetup {
        outcome,
        phase,
        action,
        target,
        proposal_id,
        principal,
        counterparties,
        source,
        reason,
        time_ms,
    } = setup;
    AuditEvent::PrivilegedAction {
        outcome,
        phase,
        action: action.into(),
        target: target.into(),
        proposal_id: proposal_id.into(),
        principal,
        counterparties,
        approver_set_fingerprint: "f00dcafe".into(),
        key_id: String::new(),
        signature: vec![],
        signature_verified: false,
        signed_at_ms: 0,
        source,
        reason: reason.into(),
        time_ms: time_ms.0,
    }
}

#[test]
fn privileged_action_maps_to_6003_with_the_whole_body() {
    let alice = AuditPrincipal {
        name: "User:alice".into(),
        auth_method: "MTls".into(),
    };
    let bob = AuditPrincipal {
        name: "User:bob".into(),
        auth_method: "SaslScram".into(),
    };
    let carol = AuditPrincipal {
        name: "User:carol".into(),
        auth_method: "MTls".into(),
    };
    let cases = [
        (
            "signed freeze, verified, no proposal",
            privileged_freeze(vec![0xde, 0xad, 0xbe, 0xef], 0, 10),
            serde_json::json!({
                "class_uid": 6003,
                "category_uid": 6,
                "type_uid": 600_300,
                "activity_id": 0,
                "time": 10,
                "status_id": 1,
                "status_detail": "incident 42",
                "api": {
                    "operation": "topic_freeze.applied",
                    "service": { "name": "kafka" },
                },
                "actor": { "user": { "name": "User:alice", "type": "MTls" } },
                "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                "privileged_action": {
                    "phase": "applied",
                    "action": "topic_freeze",
                    "target": "orders",
                    "proposal_id": "",
                    "counterparties": [],
                    "approver_set_fingerprint": "",
                    "key_id": "op-1",
                    "signature": "deadbeef",
                    "signature_verified": true,
                    "signed_at_ms": 0,
                },
                "metadata": expected_metadata(),
            }),
        ),
        (
            "unsigned two-person consumption",
            unsigned_privilege()
                .principal(carol.clone())
                .counterparties(vec![alice.clone(), bob.clone()])
                .time_ms(11)
                .call(),
            serde_json::json!({
                "class_uid": 6003,
                "category_uid": 6,
                "type_uid": 600_300,
                "activity_id": 0,
                "time": 11,
                "status_id": 1,
                "status_detail": "",
                "api": {
                    "operation": "unclean_elect_leaders.consumed",
                    "service": { "name": "kafka" },
                },
                "actor": { "user": { "name": "User:carol", "type": "MTls" } },
                "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                "privileged_action": {
                    "phase": "consumed",
                    "action": "unclean_elect_leaders",
                    "target": "orders-3",
                    "proposal_id": "bg-7",
                    "counterparties": [
                        { "name": "User:alice", "type": "MTls" },
                        { "name": "User:bob", "type": "SaslScram" },
                    ],
                    "approver_set_fingerprint": "f00dcafe",
                    "key_id": "",
                    "signature": "",
                    "signature_verified": false,
                    "signed_at_ms": 0,
                },
                "metadata": expected_metadata(),
            }),
        ),
        (
            "bypassed gate reports failure",
            unsigned_privilege()
                .outcome(AuditOutcome::Failure)
                .phase(PrivilegedPhase::Bypassed)
                .action("unclean_recovery")
                .target("orders-9")
                .proposal_id("")
                .principal(AuditPrincipal {
                    name: "broker".into(),
                    auth_method: "Internal".into(),
                })
                .reason("background recovery ran without an approval")
                .time_ms(12)
                .call(),
            serde_json::json!({
                "class_uid": 6003,
                "category_uid": 6,
                "type_uid": 600_300,
                "activity_id": 0,
                "time": 12,
                "status_id": 2,
                "status_detail": "background recovery ran without an approval",
                "api": {
                    "operation": "unclean_recovery.bypassed",
                    "service": { "name": "kafka" },
                },
                "actor": { "user": { "name": "broker", "type": "Internal" } },
                "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                "privileged_action": {
                    "phase": "bypassed",
                    "action": "unclean_recovery",
                    "target": "orders-9",
                    "proposal_id": "",
                    "counterparties": [],
                    "approver_set_fingerprint": "f00dcafe",
                    "key_id": "",
                    "signature": "",
                    "signature_verified": false,
                    "signed_at_ms": 0,
                },
                "metadata": expected_metadata(),
            }),
        ),
    ];
    for (label, event, expected) in cases {
        check!(to_ocsf(&event, &product()) == expected, "case {label}");
    }
}

#[test]
fn lifecycle_maps_to_6002() {
    let ev = AuditEvent::Lifecycle {
        kind: LifecycleKind::BrokerStarted,
        node_id: 1,
        time_ms: 7,
    };
    let j = to_ocsf(&ev, &product());
    check!(
        (
            j["class_uid"].clone(),
            j["status_id"].clone(),
            j["activity_name"].clone(),
            j["device"]["uid"].clone(),
        ) == (
            serde_json::json!(6002),
            serde_json::json!(1),
            serde_json::json!("BrokerStarted"),
            serde_json::json!("1"),
        )
    );
}
