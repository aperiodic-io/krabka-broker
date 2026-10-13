//! The audit hook that an admin handler calls once it has changed the cluster.
//!
//! The hook builds the `AdminOperation` event from the request context, so
//! every admin RPC reports the principal, the source endpoint, and the changed
//! resources in the same shape.

use {super::RequestContext, crate::time_util::now_ms};

/// Emits an `AdminOperation` audit event for a completed admin request.
///
/// Call this on the SUCCESS path of each admin handler, after the broker
/// applies the operation and knows the set of resources that it changed
/// successfully. [`audit_admin_success`] is the usual entry point: it fixes
/// the outcome and drops a request that changed nothing.
pub(crate) fn audit_admin(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    operation: &str,
    outcome: krabka_audit::AuditOutcome,
    resources: Vec<krabka_audit::AuditResource>,
) {
    audit_admin_for(
        audit_log,
        ctx.principal,
        ctx.peer,
        operation,
        outcome,
        resources,
    );
}

/// Emits an `AdminOperation` audit event for a caller the handler knows only
/// by principal and peer.
///
/// The delegation-token apis authenticate the connection rather than the
/// request, so their dispatch adapters hold a [`krabka_security::Principal`]
/// and the peer address but no [`RequestContext`]. Everything else about the
/// record is identical to [`audit_admin`].
pub(crate) fn audit_admin_for(
    audit_log: &krabka_audit::AuditLog,
    principal: &krabka_security::Principal,
    peer: &std::net::SocketAddr,
    operation: &str,
    outcome: krabka_audit::AuditOutcome,
    resources: Vec<krabka_audit::AuditResource>,
) {
    audit_log.emit(krabka_audit::AuditEvent::AdminOperation {
        outcome,
        principal: krabka_audit::AuditPrincipal {
            name: principal.name.clone(),
            auth_method: format!("{:?}", principal.auth_method),
        },
        source: krabka_audit::AuditEndpoint {
            ip: peer.ip().to_string(),
            port: peer.port(),
        },
        operation: operation.to_string(),
        resources,
        time_ms: now_ms(),
    });
}

/// Emits a successful `AdminOperation` event, or nothing when the request
/// changed nothing.
///
/// This is the hook an admin handler calls on its success path. A request
/// every row of which failed authorization or validation changed no resource,
/// and an audit record naming an empty resource set would claim otherwise.
pub(crate) fn audit_admin_success(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    operation: &str,
    resources: Vec<krabka_audit::AuditResource>,
) {
    if !resources.is_empty() {
        audit_admin(
            audit_log,
            ctx,
            operation,
            krabka_audit::AuditOutcome::Success,
            resources,
        );
    }
}

/// Names one audited resource.
pub(crate) fn audit_resource(
    resource_type: &str,
    name: impl Into<String>,
) -> krabka_audit::AuditResource {
    krabka_audit::AuditResource {
        resource_type: resource_type.to_string(),
        name: name.into(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::net::SocketAddr};

    /// The channel and authenticated caller used by both admin audit paths.
    macro_rules! audit_fixture {
        (($log:ident, $rx:ident),($principal:ident, $peer:ident, $ctx:ident)) => {
            let ($log, mut $rx) = krabka_audit::AuditLog::new(8);
            let $principal = crate::test_support::sasl_principal("admin");
            let $peer: SocketAddr = "192.0.2.10:9092".parse().unwrap();
            let $ctx = RequestContext::new(
                &$principal,
                &$peer,
                "admin-client",
                "connection-a",
                false,
                "PLAINTEXT",
            );
        };
    }

    #[test]
    fn audit_admin_emits_admin_operation_event() {
        audit_fixture!((log, rx), (principal, peer, ctx));

        audit_admin(
            log.as_ref(),
            &ctx,
            "CreateTopics",
            krabka_audit::AuditOutcome::Success,
            vec![krabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        match rx.try_recv().expect("admin audit event") {
            krabka_audit::AuditEvent::AdminOperation {
                outcome,
                principal,
                source,
                operation,
                resources,
                ..
            } => {
                assert!(
                    (
                        outcome,
                        principal.name.as_str(),
                        principal.auth_method.as_str(),
                        source.ip.as_str(),
                        source.port,
                        operation.as_str(),
                        resources.len(),
                        resources[0].resource_type.as_str(),
                        resources[0].name.as_str()
                    ) == (
                        krabka_audit::AuditOutcome::Success,
                        "admin",
                        "SaslPlain",
                        "192.0.2.10",
                        9092,
                        "CreateTopics",
                        1,
                        "Topic",
                        "orders"
                    )
                );
            }
            other => panic!("expected admin operation event, got {other:?}"),
        }
    }

    #[test]
    fn audit_admin_success_skips_an_empty_resource_list() {
        audit_fixture!((log, rx), (principal, peer, ctx));

        audit_admin_success(log.as_ref(), &ctx, "AlterConfigs", Vec::new());
        assert!(rx.try_recv().is_err(), "no resource changed, no record");

        audit_admin_success(log.as_ref(), &ctx, "AlterConfigs", vec![audit_resource(
            "Topic", "orders",
        )]);

        let krabka_audit::AuditEvent::AdminOperation {
            outcome,
            operation,
            resources,
            ..
        } = rx.try_recv().expect("admin audit event")
        else {
            panic!("expected AdminOperation");
        };
        assert!(
            (outcome, operation.as_str(), resources)
                == (krabka_audit::AuditOutcome::Success, "AlterConfigs", vec![
                    audit_resource("Topic", "orders")
                ],)
        );
    }
}
