//! Shared Kafka-ACL authorization evaluator (broker + gateway).
//!
//! This crate holds the [`Authorizer`] trait, the ACL evaluator
//! ([`SimpleAclAuthorizer`] and [`AllowAllAuthorizer`]), and an [`AclSource`]
//! abstraction. One evaluator therefore serves both the broker and the gateway.
//! The broker passes a `MetadataImage` snapshot. The gateway passes an
//! [`AclCache`] over a `Vec<AclEntry>` that it fetched with `DescribeAcls`.
//!
//! The decision logic lives here once, so the two callers can never drift. That
//! logic covers the super-user bypass, deny-wins, and operation implication.
//!
//! ## Authorizing a request
//!
//! ```rust
//! use std::net::SocketAddr;
//!
//! use krabka_authz::{AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer};
//! use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
//! use krabka_security::{AuthMethod, Principal};
//! use uuid::Uuid;
//!
//! let image = MetadataImage::new(Uuid::nil());
//! let principal = Principal {
//!     name: "alice".into(),
//!     auth_method: AuthMethod::SaslPlain,
//!     groups: vec![],
//! };
//! let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
//! let req = AuthorizationRequest {
//!     principal: &principal,
//!     host: &host,
//!     resource_type: ResourceType::Topic,
//!     resource_name: "orders",
//!     operation: AclOperation::Read,
//! };
//!
//! assert2::assert!(AllowAllAuthorizer.authorize(&image, &req) == AuthorizationResult::Allow);
//! ```
#![forbid(unsafe_code)]

mod allow_all;
pub mod cache;
pub mod cidr;
mod host_format;
#[cfg(test)]
mod precedence;
mod simple;
mod source;

use std::net::SocketAddr;

pub use allow_all::AllowAllAuthorizer;
pub use cache::AclCache;
pub use host_format::jdk_host_address;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_security::Principal;
pub use simple::SimpleAclAuthorizer;
pub use source::{AclEntries, AclSource};

/// What the caller asks `authorize`: which principal wants to do which
/// operation on which resource, and from which host.
///
/// The struct borrows its references, so handler-side construction is
/// allocation-free.
#[derive(Debug, Clone)]
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

/// Binary outcome: Kafka's ACL surface is allow or deny.
///
/// The trait boundary does not expose intermediate states, for example "not yet
/// decided".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    Allow,
    Deny,
}

/// Pluggable per-broker or per-gateway authorization decision point.
///
/// Implementations own whatever state they need to make a decision, for example
/// a super-user set, an HTTP client, or a decision cache. The caller holds a
/// single `Arc<dyn Authorizer>`.
///
/// Implementations MUST be `Send + Sync + Debug`: handler code paths
/// are async and the broker logs configs at startup.
///
/// The decision consults a [`AclSource`]. The broker passes its
/// `MetadataImage` and the gateway passes an [`AclCache`]. ACL-free
/// implementations (`AllowAll`, OPA) ignore it.
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Decide whether `req.principal` may do `req.operation` on
    /// `(req.resource_type, req.resource_name)` from `req.host`.
    ///
    /// The authorizer may consult `source`, as the ACL-backed implementations
    /// do, or ignore it entirely, as `AllowAll` and `Opa` do.
    fn authorize(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult;

    /// The decision of [`Authorizer::authorize`] for a check that Kafka makes
    /// with `logIfDenied = false`: a probe whose Deny is not a refusal, such
    /// as the cluster-wide `Create` shortcut of `CreateTopics` that falls back
    /// to a per-topic check, or the operations bit field a `Describe` reports.
    ///
    /// The decision is the same. What differs is what a Deny leaves behind:
    /// none of the audit records and counters that `authorize` may write. The
    /// default forwards to `authorize`, which is right for an implementation
    /// that writes none. A decorator that audits a Deny MUST override this to
    /// forward to the authorizer it wraps without auditing, and one that only
    /// forwards MUST forward this method too.
    fn authorize_quiet(
        &self,
        source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        self.authorize(source, req)
    }

    /// Whether this implementation is a real authorization decision point,
    /// the way Kafka's `authorizer.class.name` names one.
    ///
    /// Only [`AllowAllAuthorizer`] answers `false`: it is what a deployment
    /// gets when it configures no authorizer at all, so a stored ACL would
    /// never be consulted. The ACL administration RPCs read this to answer
    /// `SECURITY_DISABLED`, as Kafka's `KafkaApis` does under
    /// `authorizer.isEmpty`. A decorator must forward it to the authorizer it
    /// wraps.
    fn is_configured(&self) -> bool {
        true
    }

    /// Upper bound on how long a caller-side cache may reuse a decision from
    /// this authorizer without asking again, or `None` if a decision is
    /// exactly as fresh as whatever `source` snapshot it was computed
    /// against -- true of the ACL-backed implementations, whose grants live
    /// in the metadata log a caller can already key a cache on.
    ///
    /// An authorizer whose decisions can go stale independently of `source`
    /// -- an HTTP-backed policy engine such as OPA is the motivating case --
    /// overrides this to its own decision-cache TTL, so a caller-side cache
    /// keyed on metadata alone cannot outlive it and silently miss a policy
    /// change the metadata log never saw. A decorator MUST forward the
    /// wrapped authorizer's answer, as `is_configured` does.
    fn decision_ttl(&self) -> Option<std::time::Duration> {
        None
    }

    /// Whether `principal` holds `operation` on at least one resource pattern
    /// of `resource_type` -- Kafka's `AuthorizerUtils.authorizeByResourceType`,
    /// used where a request is not itself scoped to one resource name. KIP-599
    /// motivates the check: `InitProducerId` with no transactional id accepts
    /// either cluster-wide `IdempotentWrite` or `Write` on any topic, so a
    /// principal that can produce need not also hold a cluster-wide grant.
    ///
    /// The default answers [`AuthorizationResult::Deny`]: an implementation
    /// that cannot enumerate its ACL space (an HTTP-backed policy engine, for
    /// example) has no cheap way to answer this honestly, and understating a
    /// grant is the safe direction. [`AllowAllAuthorizer`] overrides this to
    /// always answer `Allow`, since it never denies anything.
    /// [`SimpleAclAuthorizer`] overrides it with a real scan. A decorator MUST
    /// forward the wrapped authorizer's answer, as it does for `is_configured`
    /// and `decision_ttl`.
    fn authorize_by_resource_type(
        &self,
        _source: &dyn AclSource,
        _principal: &Principal,
        _host: &SocketAddr,
        _resource_type: ResourceType,
        _operation: AclOperation,
    ) -> AuthorizationResult {
        AuthorizationResult::Deny
    }
}

// Batch spans record only shared request context; inner decisions retain their child spans.
macro_rules! topic_authorization_batch {
    (
        $(#[$doc:meta])*
        $name:ident($authorizer:ident, $source:ident, $principal:ident, $host:ident,
            $operation:ident, $topic_names:ident $(, $log_denied:ident)?) $body:block
    ) => {
        $(#[$doc])*
        #[must_use]
        #[tracing::instrument(
            level = "debug", skip_all,
            fields(principal = %$principal.name, operation = ?$operation, host = %$host.ip())
        )]
        pub fn $name<'a>(
            $authorizer: &dyn Authorizer,
            $source: &dyn AclSource,
            $principal: &Principal,
            $host: &SocketAddr,
            $operation: AclOperation,
            $topic_names: impl IntoIterator<Item = &'a str>
            $(, $log_denied: bool)?
        ) -> std::collections::HashMap<&'a str, AuthorizationResult> $body
    };
}

topic_authorization_batch! {
    /// Batch-authorize a set of topic names against the same principal, host, and
    /// operation.
    ///
    /// The `Produce`, `Fetch`, and `Metadata` per-topic enforcement paths call this
    /// function. The returned map borrows its keys from the input iterator, so
    /// callers can avoid a copy of the topic strings.
    // Batch entry point for per-topic enforcement. skip_all keeps the borrowed
    // principal/host out of span fields; only the shared operation + principal
    // name are recorded. Each inner `authorize` opens its own child span, so this
    // is a batch-level span, not a per-entry loop span.
    authorize_topics(authorizer, source, principal, host, operation, topic_names) {
        authorize_topics_logged(
            authorizer,
            source,
            principal,
            host,
            operation,
            topic_names,
            true,
        )
    }
}

topic_authorization_batch! {
    /// [`authorize_topics`] with Kafka's `logIfDenied` flag. With `log_denied` off
    /// each name goes through [`Authorizer::authorize_quiet`], and a Deny leaves
    /// no audit record and no counter behind.
    ///
    /// `Metadata` is the caller that turns it off: for all topics it filters every
    /// topic by `Describe` and hides the denied ones, so a Deny there is not a
    /// refusal (`logIfDenied = !metadataRequest.isAllTopics`).
    authorize_topics_logged(authorizer, source, principal, host, operation, topic_names, log_denied) {
        topic_names
            .into_iter()
            .map(|name| {
                let req = AuthorizationRequest {
                    principal,
                    host,
                    resource_type: ResourceType::Topic,
                    resource_name: name,
                    operation,
                };
                let decision = if log_denied {
                    authorizer.authorize(source, &req)
                } else {
                    authorizer.authorize_quiet(source, &req)
                };
                (name, decision)
            })
            .collect()
    }
}

#[cfg(test)]
mod test_support;
