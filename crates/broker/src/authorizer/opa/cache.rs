//! Key and entry types for the OPA decision cache. They live apart from the
//! authorizer itself because they are pure data: the cache key is the tuple
//! that identifies one authorization question, and the entry is the answer
//! plus the monotonic-clock stamp, in milliseconds, at which it goes stale.

use {
    krabka_authz::AuthorizationResult,
    krabka_metadata::{AclOperation, ResourceType},
    std::net::IpAddr,
};

#[derive(Debug, Clone, PartialEq, Eq, std::hash::Hash)]
pub(super) struct CacheKey {
    pub(super) principal: String,
    pub(super) operation: AclOperation,
    pub(super) resource_type: ResourceType,
    pub(super) resource_name: String,
    pub(super) host: IpAddr,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CachedDecision {
    pub(super) decision: AuthorizationResult,
    pub(super) expires_at_ms: i128,
}

#[cfg(test)]
mod tests {
    use {
        super::CacheKey,
        krabka_metadata::{AclOperation, ResourceType},
        std::net::{IpAddr, Ipv4Addr},
    };

    fn key() -> CacheKey {
        CacheKey {
            principal: "User:alice".to_string(),
            operation: AclOperation::Read,
            resource_type: ResourceType::Topic,
            resource_name: "orders".to_string(),
            host: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        }
    }

    #[test]
    fn every_opa_input_axis_is_part_of_the_cache_key() {
        let expected = key();
        for changed in [
            CacheKey {
                principal: "User:bob".to_string(),
                ..key()
            },
            CacheKey {
                operation: AclOperation::Write,
                ..key()
            },
            CacheKey {
                resource_type: ResourceType::Group,
                ..key()
            },
            CacheKey {
                resource_name: "payments".to_string(),
                ..key()
            },
            CacheKey {
                host: IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)),
                ..key()
            },
        ] {
            assert2::check!(changed != expected);
        }
    }
}
