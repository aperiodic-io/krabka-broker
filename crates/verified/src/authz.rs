//! Kafka ACL precedence and SASL session/request admission.

use creusot_std::prelude::*;

/// Why ACL evaluation allowed or denied a request.
#[krabka_macros::model_type]
pub enum AclDecision {
    AllowSuperuser,
    AllowAcl,
    /// `allow.everyone.if.no.acl.found` allowed the request because no ACL
    /// at all applies to the resource.
    AllowNoAcl,
    DenyExplicit,
    DenyDefault,
}

/// Resource-pattern class used by the verified ACL applicability adapter.
#[krabka_macros::model_type]
pub enum AclPatternKind {
    Literal,
    Prefixed,
}

/// ACL operation class used by the verified implication table.
#[krabka_macros::model_type]
pub enum AclOperationKind {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
    TwoPhaseCommit,
    /// KIP-373: create a delegation token owned by the `User` resource.
    CreateTokens,
    /// KIP-373: describe the delegation tokens the `User` resource owns.
    DescribeTokens,
}

/// Whether a stored ACL names the requested resource type.
#[krabka_macros::model_type]
pub enum AclResourceTypeMatch {
    Same,
    Different,
}

/// How a stored ACL's resource name relates to the requested resource name.
#[krabka_macros::model_type]
pub struct AclResourceFacts {
    pub resource_type: AclResourceTypeMatch,
    /// The stored name equals the requested name.
    pub exact_name: bool,
    /// The stored name is the literal wildcard `*`.
    pub wildcard_name: bool,
    /// The requested name starts with the stored name.
    pub name_has_prefix: bool,
}

/// Authentication phase used to admit a Kafka request, after Kafka's
/// `SaslServerAuthenticator` states.
#[krabka_macros::model_type]
pub enum RequestAuthState {
    /// No `SaslHandshake` has run yet: only `SaslHandshake` (17) and
    /// `ApiVersions` (18) are Kafka requests the authenticator handles.
    PreHandshake,
    /// A handshake chose a mechanism, for the initial authentication or a
    /// KIP-368 re-authentication: only `SaslAuthenticate` (36) may follow.
    Exchanging,
    /// Authentication failed, including a mechanism switch or a controller
    /// session that expired: the next frame, whatever it is, fails
    /// the connection.
    Failed,
    Authenticated,
}

/// The outcome when no ACL matched the request.
#[krabka_macros::model_type]
pub enum AclDefault {
    Deny,
    /// `allow.everyone.if.no.acl.found` is set and no ACL at all applies to
    /// the resource.
    Allow,
}

/// What the precedence loop observed for one request.
#[krabka_macros::model_type]
pub struct AclFacts {
    pub super_user: bool,
    /// Some applicable ALLOW ACL matched the principal, host, and operation.
    pub saw_allow: bool,
    /// Some applicable DENY ACL matched the principal, host, and operation.
    pub saw_deny: bool,
    pub default_decision: AclDefault,
}

mod acl_decision;
pub use acl_decision::{
    acl_decision, acl_identity_match, acl_operation_match, acl_resource_match,
    request_auth_admission,
};

mod session;
pub use session::{sasl_session_expiry, session_expired_for_request};

mod controller_session;
pub use controller_session::controller_request_admission;

#[cfg(test)]
mod tests;
