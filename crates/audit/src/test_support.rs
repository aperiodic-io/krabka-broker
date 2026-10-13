//! Shared audit event and signing fixtures.

use {
    crate::{
        AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, AuditResource, FileEd25519Signer,
        PrivilegedPhase,
    },
    ring::signature::{Ed25519KeyPair, KeyPair},
    std::sync::Arc,
};

pub(crate) fn signer() -> (FileEd25519Signer, Vec<u8>) {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
    let public_key = key_pair.public_key().as_ref().to_vec();
    (
        FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "k1".into()).unwrap(),
        public_key,
    )
}

pub(crate) fn shared_signer() -> (Arc<FileEd25519Signer>, Vec<u8>) {
    let (signer, public_key) = signer();
    (Arc::new(signer), public_key)
}

pub(crate) fn authentication(port: u16, reason: Option<&str>, time_ms: i64) -> AuditEvent {
    AuditEvent::Authentication {
        outcome: AuditOutcome::Failure,
        mechanism: "SASL/PLAIN".into(),
        principal: AuditPrincipal {
            name: "alice".into(),
            auth_method: "SaslPlain".into(),
        },
        source: AuditEndpoint {
            ip: "10.0.0.1".into(),
            port,
        },
        reason: reason.map(str::to_owned),
        time_ms,
    }
}

pub(crate) fn denied(time_ms: i64) -> AuditEvent {
    AuditEvent::AuthorizationDenied {
        principal: AuditPrincipal {
            name: "bob".into(),
            auth_method: "MTls".into(),
        },
        source: AuditEndpoint {
            ip: "10.0.0.2".into(),
            port: 4444,
        },
        resource_type: "Topic".into(),
        resource_name: "secrets".into(),
        operation: "Write".into(),
        time_ms,
    }
}

pub(crate) fn admin(time_ms: i64) -> AuditEvent {
    AuditEvent::AdminOperation {
        outcome: AuditOutcome::Success,
        principal: AuditPrincipal {
            name: "admin".into(),
            auth_method: "MTls".into(),
        },
        source: AuditEndpoint {
            ip: "10.0.0.3".into(),
            port: 9092,
        },
        operation: "CreateTopics".into(),
        resources: vec![AuditResource {
            resource_type: "Topic".into(),
            name: "orders".into(),
        }],
        time_ms,
    }
}

pub(crate) fn privileged_freeze(signature: Vec<u8>, signed_at_ms: i64, time_ms: i64) -> AuditEvent {
    AuditEvent::PrivilegedAction {
        outcome: AuditOutcome::Success,
        phase: PrivilegedPhase::Applied,
        action: "topic_freeze".into(),
        target: "orders".into(),
        proposal_id: String::new(),
        principal: AuditPrincipal {
            name: "User:alice".into(),
            auth_method: "MTls".into(),
        },
        counterparties: vec![],
        approver_set_fingerprint: String::new(),
        key_id: "op-1".into(),
        signature,
        signature_verified: true,
        signed_at_ms,
        source: AuditEndpoint {
            ip: "10.0.0.4".into(),
            port: 9092,
        },
        reason: "incident 42".into(),
        time_ms,
    }
}
