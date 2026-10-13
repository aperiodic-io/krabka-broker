//! ACL-based authorizer behind the [`Authorizer`] trait.
//!
//! The authorizer applies the super-user bypass, deny-wins-over-allow, LITERAL
//! and PREFIXED matching, and principal, host, and operation wildcards.
//!
//! [`SimpleAclAuthorizer`] with an empty source and empty super-users denies
//! everything by default. This default-deny behavior matches Kafka's
//! `StandardAuthorizer` once an operator explicitly configures an authorizer.
//! Kafka's `allow.everyone.if.no.acl.found` (default `false`) flips that
//! default: when enabled and NO ACL at all applies to the resource --
//! regardless of principal, host, operation, or permission type -- the
//! request is allowed. If at least one ACL applies to the resource and none
//! of them matches, the request is still denied. Set it with
//! [`SimpleAclAuthorizer::with_allow_everyone_if_no_acl_found`].

use {
    krabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType},
    krabka_security::Principal,
    krabka_verified::{
        AclDecision, acl_decision,
        authz::{AclDefault, AclFacts},
    },
    std::{collections::HashSet, net::SocketAddr},
};

mod coverage;
mod matching;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        coverage::DeniedNames,
        matching::{matches_host, matches_operation, matches_principal, matches_resource},
    },
    crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer, jdk_host_address},
};

/// The resource name Kafka's default `authorizeByResourceType` probes first,
/// a name that no real resource is expected to carry.
const HARDCODED_RESOURCE_NAME: &str = "hardcode";

/// Authorizer that consults the cluster's persisted ACLs.
///
/// The caller supplies the [`AclSource`] per call: a `MetadataImage` for the
/// broker, an `AclCache` for the gateway.
///
/// This type holds the configured super-user set. Principals in this set bypass
/// ACL evaluation and always get `Allow`.
#[derive(Debug)]
pub struct SimpleAclAuthorizer {
    super_users: HashSet<String>,
    allow_everyone_if_no_acl_found: bool,
}

impl SimpleAclAuthorizer {
    #[must_use]
    pub fn new(super_users: HashSet<String>) -> Self {
        Self {
            super_users,
            allow_everyone_if_no_acl_found: false,
        }
    }

    /// Sets Kafka's `allow.everyone.if.no.acl.found` (default `false`). See
    /// the module doc for the exact semantics.
    #[must_use]
    pub fn with_allow_everyone_if_no_acl_found(mut self, allow: bool) -> Self {
        self.allow_everyone_if_no_acl_found = allow;
        self
    }
}

impl Authorizer for SimpleAclAuthorizer {
    // Per-request ACL decision: skip_all keeps the borrowed principal/host
    // structs (which may carry the raw name) out of span fields; only
    // non-sensitive routing context is recorded. No `err` — this returns a
    // plain Allow/Deny, not a Result. `decision` is filled in before each
    // return path.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            principal = %req.principal.name,
            resource_type = ?req.resource_type,
            resource = %req.resource_name,
            operation = ?req.operation,
            host = %jdk_host_address(req.host.ip()),
            decision = tracing::field::Empty,
        )
    )]
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let span = tracing::Span::current();
        let super_user = self.super_users.contains(&req.principal.name);
        let mut saw_allow = false;
        let mut saw_deny = false;
        let mut has_resource_acls = false;
        if !super_user {
            let user_pattern = format!("User:{}", req.principal.name);
            let host_str = jdk_host_address(req.host.ip());
            let cidr_hosts_supported = source.cidr_hosts_supported();
            for entry in source.matching_acls(req.resource_type, req.resource_name) {
                if !matches_resource(entry, req.resource_type, req.resource_name) {
                    continue;
                }
                has_resource_acls = true;
                if !matches_principal(entry, &user_pattern)
                    || !matches_host(entry, &host_str, req.host.ip(), cidr_hosts_supported)
                    || !matches_operation(entry.operation, req.operation, entry.permission_type)
                {
                    continue;
                }
                match entry.permission_type {
                    PermissionType::Allow => saw_allow = true,
                    PermissionType::Deny => {
                        saw_deny = true;
                        break;
                    }
                }
            }
        }
        let default_decision = if self.allow_everyone_if_no_acl_found && !has_resource_acls {
            AclDefault::Allow
        } else {
            AclDefault::Deny
        };
        let decision = acl_decision(AclFacts {
            super_user,
            saw_allow,
            saw_deny,
            default_decision,
        });

        let (label, result) = match decision {
            AclDecision::AllowSuperuser => ("allow-superuser", AuthorizationResult::Allow),
            AclDecision::AllowAcl => ("allow-acl", AuthorizationResult::Allow),
            AclDecision::AllowNoAcl => ("allow-no-acl", AuthorizationResult::Allow),
            AclDecision::DenyExplicit => ("deny-explicit", AuthorizationResult::Deny),
            AclDecision::DenyDefault => ("deny-default", AuthorizationResult::Deny),
        };
        span.record("decision", label);
        result
    }

    /// Allows first when the ordinary [`Self::authorize`] decision allows the
    /// hard-coded name `hardcode`, as Kafka's default implementation does.
    /// Otherwise it scans every stored entry of `resource_type` for an ALLOW ACL, matching
    /// `principal`, `host`, and `operation`, that no DENY covers.
    ///
    /// A literal ALLOW needs its exact name to survive the matching DENYs.
    /// Kafka checks a prefix ALLOW only against dominating DENY prefixes and
    /// the DENY wildcard; literal exclusions cannot dominate a prefix grant.
    /// A wildcard ALLOW survives unless a wildcard DENY exists. These pattern
    /// checks need no synthetic name that another ACL could happen to deny.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            principal = %principal.name,
            resource_type = ?resource_type,
            operation = ?operation,
            host = %jdk_host_address(host.ip()),
            decision = tracing::field::Empty,
        )
    )]
    fn authorize_by_resource_type(
        &self,
        source: &dyn AclSource,
        principal: &Principal,
        host: &SocketAddr,
        resource_type: ResourceType,
        operation: AclOperation,
    ) -> AuthorizationResult {
        let span = tracing::Span::current();
        if self.super_users.contains(&principal.name) {
            span.record("decision", "allow-superuser");
            return AuthorizationResult::Allow;
        }
        // Kafka's `Authorizer.authorizeByResourceType` first authorizes a
        // hard-coded resource name, and StandardAuthorizer inherits it. The
        // probe is allowed when a wildcard ALLOW covers the type or, under
        // `allow.everyone.if.no.acl.found`, when no ACL of the type covers
        // the name, so a cluster that runs the flag admits a principal that
        // has no topic ACL at all.
        let hardcode = AuthorizationRequest {
            principal,
            host,
            resource_type,
            resource_name: HARDCODED_RESOURCE_NAME,
            operation,
        };
        if self.authorize(source, &hardcode) == AuthorizationResult::Allow {
            span.record("decision", "allow-hardcode");
            return AuthorizationResult::Allow;
        }
        let user_pattern = format!("User:{}", principal.name);
        let host_str = jdk_host_address(host.ip());
        let cidr_hosts_supported = source.cidr_hosts_supported();
        let applicable: Vec<_> = source
            .acls_of_type(resource_type)
            .filter(|entry| {
                matches_principal(entry, &user_pattern)
                    && matches_host(entry, &host_str, host.ip(), cidr_hosts_supported)
                    && matches_operation(entry.operation, operation, entry.permission_type)
            })
            .collect();
        let denied = DeniedNames::new(
            applicable
                .iter()
                .copied()
                .filter(|entry| entry.permission_type == PermissionType::Deny),
        );
        let uncovered = applicable.iter().any(|entry| {
            entry.permission_type == PermissionType::Allow
                && match entry.pattern_type {
                    PatternType::Literal if entry.resource_name == "*" => denied.allows_wildcard(),
                    PatternType::Literal => denied.allows_literal(&entry.resource_name),
                    PatternType::Prefixed => denied.allows_prefix(&entry.resource_name),
                }
        });
        if uncovered {
            span.record("decision", "allow-acl");
            return AuthorizationResult::Allow;
        }
        span.record("decision", "deny-default");
        AuthorizationResult::Deny
    }
}
