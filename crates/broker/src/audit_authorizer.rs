//! Authorizer decorator that counts every Deny decision and records it as an
//! audit event.

use std::sync::Arc;

use krabka_audit::{AuditEndpoint, AuditEvent, AuditLog, AuditPrincipal};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    metrics::BrokerMetrics,
};

/// Wraps an [`Authorizer`]. It forwards decisions, bumps
/// `authorization_denied_total` and emits an audit record on every Deny of
/// [`Authorizer::authorize`]. [`Authorizer::authorize_quiet`] forwards the
/// decision and leaves neither behind, for the checks that Kafka makes with
/// `logIfDenied = false`. The handlers audit Allow decisions for admin
/// operations separately (Task 8), so this decorator does not duplicate them.
///
/// The broker installs it whether or not audit is enabled: with
/// `audit.enabled=false` the [`AuditLog`] is the disabled one and drops the
/// emit, and the counter is the only record a Deny leaves behind.
#[derive(Debug)]
pub struct AuditingAuthorizer {
    inner: Arc<dyn Authorizer>,
    audit: Arc<AuditLog>,
    metrics: BrokerMetrics,
}

impl AuditingAuthorizer {
    #[must_use]
    pub fn new(inner: Arc<dyn Authorizer>, audit: Arc<AuditLog>, metrics: BrokerMetrics) -> Self {
        Self {
            inner,
            audit,
            metrics,
        }
    }
}

impl Authorizer for AuditingAuthorizer {
    fn authorize(
        &self,
        source: &dyn krabka_authz::AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let result = self.inner.authorize(source, req);
        if result == AuthorizationResult::Deny {
            self.metrics.record_authorization_denied(
                &format!("{:?}", req.operation),
                &format!("{:?}", req.resource_type),
            );
            self.audit.emit(AuditEvent::AuthorizationDenied {
                principal: AuditPrincipal {
                    name: req.principal.name.clone(),
                    auth_method: format!("{:?}", req.principal.auth_method),
                },
                source: AuditEndpoint {
                    ip: krabka_authz::jdk_host_address(req.host.ip()),
                    port: req.host.port(),
                },
                resource_type: format!("{:?}", req.resource_type),
                resource_name: req.resource_name.to_string(),
                operation: format!("{:?}", req.operation),
                time_ms: crate::time_util::now_ms(),
            });
        }
        result
    }

    /// The decision without the audit record and the counter. Kafka makes some
    /// checks with `logIfDenied = false`: the cluster-wide `Create` and
    /// `Delete` shortcuts that fall back to a per-topic check, the
    /// `DescribeConfigs` disclosure check of `CreateTopics`, the `Describe`
    /// filter of an all-topics `Metadata`, and every operations bit field. A
    /// Deny there is not a refusal, so it must not show up in the audit trail
    /// or in `authorization_denied_total`.
    fn authorize_quiet(
        &self,
        source: &dyn krabka_authz::AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        self.inner.authorize_quiet(source, req)
    }

    /// Forward the wrapped authorizer's answer: the decorator adds auditing,
    /// not a decision point, so a broker with no authorizer configured must
    /// still report itself as having none.
    fn is_configured(&self) -> bool {
        self.inner.is_configured()
    }

    /// Forward the wrapped authorizer's answer, for the same reason as
    /// `is_configured`: this decorator adds auditing around a decision, it
    /// does not change how long that decision stays fresh.
    fn decision_ttl(&self) -> Option<std::time::Duration> {
        self.inner.decision_ttl()
    }

    /// Forward the wrapped authorizer's answer. This resource-type-wide check
    /// is not per-resource, so `authorize` above (which audits Deny) never
    /// sees it; a caller that wants a Deny from this path audited would need
    /// its own event, which no handler asks for yet.
    fn authorize_by_resource_type(
        &self,
        source: &dyn krabka_authz::AclSource,
        principal: &krabka_security::Principal,
        host: &std::net::SocketAddr,
        resource_type: krabka_metadata::ResourceType,
        operation: krabka_metadata::AclOperation,
    ) -> AuthorizationResult {
        self.inner
            .authorize_by_resource_type(source, principal, host, resource_type, operation)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::check;
    use krabka_metadata::{AclOperation, ResourceType};

    use super::*;
    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        test_support::{DenyAll, principal},
    };

    // Expand the setup in the caller so receiver and metrics keep their scopes.
    macro_rules! denial_fixture {
        ($rx:ident, $metrics:ident, $authz:ident, $principal:ident, $host:ident, $image:ident) => {
            let (log, mut $rx) = krabka_audit::AuditLog::new(8);
            let $metrics = crate::metrics::BrokerMetrics::new();
            let $authz = AuditingAuthorizer::new(Arc::new(DenyAll), log, $metrics.clone());
            let $principal = principal("anonymous");
            let $host: SocketAddr = "10.0.0.9:5555".parse().unwrap();
            let $image = krabka_metadata::MetadataImage::default();
        };
    }

    fn secret_write(
        authz: &AuditingAuthorizer,
        image: &krabka_metadata::MetadataImage,
        principal: &krabka_security::Principal,
        host: &SocketAddr,
    ) -> AuthorizationResult {
        authz.authorize(
            image,
            &AuthorizationRequest {
                principal,
                host,
                resource_type: ResourceType::Topic,
                resource_name: "secrets",
                operation: AclOperation::Write,
            },
        )
    }

    fn denied_label() -> crate::metrics::AuthorizationDeniedLabel {
        crate::metrics::AuthorizationDeniedLabel {
            operation: "Write".to_string(),
            resource_type: "Topic".to_string(),
        }
    }

    #[tokio::test]
    async fn deny_decision_emits_audit_record() {
        denial_fixture!(rx, metrics, authz, principal, host, image);
        let result = secret_write(&authz, &image, &principal, &host);
        check!(result == AuthorizationResult::Deny);

        let ev = rx.try_recv().expect("an audit event was emitted");
        match ev {
            krabka_audit::AuditEvent::AuthorizationDenied {
                resource_name,
                operation,
                ..
            } => {
                check!(resource_name == "secrets");
                check!(operation == "Write");
            }
            other => panic!("unexpected event: {other:?}"),
        }
        let denied = metrics
            .authorization_denied
            .get_or_create(&denied_label())
            .get();
        check!(denied == 1);
    }

    /// With `audit.enabled=false` the broker still installs the decorator over
    /// a disabled [`AuditLog`]. The emit goes nowhere; the counter is what a
    /// Deny leaves behind.
    #[tokio::test]
    async fn deny_decision_counts_with_audit_disabled() {
        let metrics = crate::metrics::BrokerMetrics::new();
        let authz = AuditingAuthorizer::new(
            Arc::new(DenyAll),
            krabka_audit::AuditLog::disabled(),
            metrics.clone(),
        );

        let principal = principal("anonymous");
        let host: SocketAddr = "10.0.0.9:5555".parse().unwrap();
        let image = krabka_metadata::MetadataImage::default();
        let result = secret_write(&authz, &image, &principal, &host);

        check!(result == AuthorizationResult::Deny);
        let denied = metrics
            .authorization_denied
            .get_or_create(&denied_label())
            .get();
        check!(denied == 1);
    }

    /// The decorator is installed over whatever the operator configured, so
    /// the ACL RPCs must still see a broker with no authorizer as one.
    #[test]
    fn is_configured_follows_the_wrapped_authorizer() {
        let metrics = crate::metrics::BrokerMetrics::new();
        let over_allow_all = AuditingAuthorizer::new(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            krabka_audit::AuditLog::disabled(),
            metrics.clone(),
        );
        let over_deny_all = AuditingAuthorizer::new(
            Arc::new(DenyAll),
            krabka_audit::AuditLog::disabled(),
            metrics,
        );

        check!(!over_allow_all.is_configured());
        check!(over_deny_all.is_configured());
    }

    /// A fake authorizer that reports a fixed [`Authorizer::decision_ttl`],
    /// standing in for `OpaAuthorizer` without pulling in its HTTP setup.
    #[derive(Debug)]
    struct FixedTtl(std::time::Duration);

    impl Authorizer for FixedTtl {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            _req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            AuthorizationResult::Allow
        }

        fn decision_ttl(&self) -> Option<std::time::Duration> {
            Some(self.0)
        }
    }

    /// This decorator adds auditing around a decision, not a cache; it must
    /// forward the wrapped authorizer's `decision_ttl` unchanged, the same
    /// way it forwards `is_configured`.
    #[test]
    fn decision_ttl_follows_the_wrapped_authorizer() {
        let metrics = crate::metrics::BrokerMetrics::new();
        let ttl = std::time::Duration::from_secs(42);
        let over_fixed_ttl = AuditingAuthorizer::new(
            Arc::new(FixedTtl(ttl)),
            krabka_audit::AuditLog::disabled(),
            metrics.clone(),
        );
        let over_no_ttl = AuditingAuthorizer::new(
            Arc::new(DenyAll),
            krabka_audit::AuditLog::disabled(),
            metrics,
        );

        check!(over_fixed_ttl.decision_ttl() == Some(ttl));
        check!(over_no_ttl.decision_ttl() == None);
    }

    /// A check that Kafka makes with `logIfDenied = false` takes the decision
    /// of the wrapped authorizer and leaves neither an audit record nor a
    /// count, where the same Deny through `authorize` leaves both.
    #[tokio::test]
    async fn a_quiet_deny_leaves_no_audit_record_and_no_count() {
        denial_fixture!(rx, metrics, authz, principal, host, image);
        let request = AuthorizationRequest {
            principal: &principal,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "secrets",
            operation: AclOperation::Write,
        };
        let denied = || {
            metrics
                .authorization_denied
                .get_or_create(&denied_label())
                .get()
        };

        let quiet = authz.authorize_quiet(&image, &request);

        check!(quiet == AuthorizationResult::Deny);
        check!(rx.try_recv().is_err());
        check!(denied() == 0);

        let loud = authz.authorize(&image, &request);

        check!(loud == AuthorizationResult::Deny);
        check!(rx.try_recv().is_ok());
        check!(denied() == 1);
    }
}
