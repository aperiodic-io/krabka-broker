//! Default authorizer when authorization is unset.
//!
//! This authorizer returns `Allow` for any request. It is an explicit type, so
//! the "allow everything" behavior is clear at config time. The behavior does
//! not come from the empty-input path of the ACL implementation.

use crate::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};

/// Authorizer that always returns [`AuthorizationResult::Allow`].
///
/// This is the default authorizer value. An operator selects it with
/// `type = "allow_all"` in the broker or gateway config, or by omission of the
/// field.
#[derive(Debug, Default)]
pub struct AllowAllAuthorizer;

impl Authorizer for AllowAllAuthorizer {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        _req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        AuthorizationResult::Allow
    }

    /// `false`: this is the value a deployment holds when it configured no
    /// authorizer, which is what the ACL administration RPCs report as
    /// `SECURITY_DISABLED`.
    fn is_configured(&self) -> bool { false }

    /// Always `Allow`, for the same reason `authorize` is: this authorizer
    /// never denies anything, on any resource of any type.
    fn authorize_by_resource_type(
        &self,
        _source: &dyn AclSource,
        _principal: &krabka_security::Principal,
        _host: &std::net::SocketAddr,
        _resource_type: krabka_metadata::ResourceType,
        _operation: krabka_metadata::AclOperation,
    ) -> AuthorizationResult {
        AuthorizationResult::Allow
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_metadata::{AclOperation, MetadataImage, ResourceType},
        krabka_security::{AuthMethod, Principal},
        std::net::SocketAddr,
        uuid::Uuid,
    };

    struct RequestIdentity {
        image: MetadataImage,
        principal: Principal,
        host: SocketAddr,
    }

    fn request_identity() -> RequestIdentity {
        let image = MetadataImage::new(Uuid::nil());
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        };
        let host = "1.2.3.4:9092".parse().unwrap();
        RequestIdentity {
            image,
            principal,
            host,
        }
    }

    #[test]
    fn allow_all_returns_allow_for_any_request() {
        let RequestIdentity {
            image: img,
            principal: p,
            host,
        } = request_identity();
        let req = AuthorizationRequest {
            principal: &p,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "anything",
            operation: AclOperation::Write,
        };
        assert2::assert!(AllowAllAuthorizer.authorize(&img, &req) == AuthorizationResult::Allow);
    }

    #[test]
    fn allow_all_is_not_a_configured_authorizer() {
        assert2::assert!(!AllowAllAuthorizer.is_configured());
    }

    #[test]
    fn allow_all_authorizes_by_resource_type_for_any_request() {
        let RequestIdentity {
            image: img,
            principal: p,
            host,
        } = request_identity();
        assert2::assert!(
            AllowAllAuthorizer.authorize_by_resource_type(
                &img,
                &p,
                &host,
                ResourceType::Topic,
                AclOperation::Write,
            ) == AuthorizationResult::Allow
        );
    }
}
