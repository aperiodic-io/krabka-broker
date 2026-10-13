//! Authorization and key validation for `FindCoordinator`, and the refused-key
//! row Kafka puts in the response.
//!
//! GROUP and TRANSACTION keys are authorized one key at a time, so a denied
//! group id in a multi-key v4+ request leaves the authorized keys resolving
//! normally in the same response. SHARE keys are different: Kafka checks
//! `ClusterAction` with `authorizeClusterOperation`, which throws, so a denial
//! fails the whole request before any key is validated. The handler asks
//! [`cluster_action_allowed`] once for that, and this module then partitions
//! the requested keys into refused rows and still-to-resolve keys.

use {
    super::{
        KEY_TYPE_GROUP, KEY_TYPE_SHARE, KEY_TYPE_TRANSACTION, resolve::parse_share_key,
        response::no_node_row,
    },
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        broker::Broker,
        codes,
        handlers::{RequestContext, acl_wire::CLUSTER_RESOURCE_NAME},
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::find_coordinator_response::Coordinator,
    krabka_verified::broker::{FindCoordinatorAdmission, find_coordinator_admission},
};

pub(super) enum KeySlot {
    Resolve(String),
    Rejected(Coordinator),
}

/// Whether the request's principal holds `ClusterAction` on the cluster, the
/// check Kafka makes for a SHARE key before it validates the key.
pub(super) fn cluster_action_allowed(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
) -> bool {
    broker
        .config
        .authorizer
        .authorize(image, &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: AclOperation::ClusterAction,
        })
        == AuthorizationResult::Allow
}

/// Authorize and validate a single `FindCoordinator` key.
///
/// GROUP needs `Describe` on `Group(key)`. TRANSACTION needs `Describe` on
/// `TransactionalId(key)`. A SHARE key reaches this point only once the
/// request holds `ClusterAction`, so it is admitted at v6+ when its composite
/// key parses. Unsupported SHARE versions, unknown key types, and malformed
/// SHARE keys fail closed without consulting the authorizer.
fn key_admission(
    authorizer: &dyn Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
    api_version: i16,
    key_type: i8,
    key: &str,
) -> FindCoordinatorAdmission {
    let (resource_type, operation) = match key_type {
        KEY_TYPE_GROUP => (ResourceType::Group, AclOperation::Describe),
        KEY_TYPE_TRANSACTION => (ResourceType::TransactionalId, AclOperation::Describe),
        KEY_TYPE_SHARE => {
            let share_key_valid = parse_share_key(key).is_some();
            return find_coordinator_admission(api_version, key_type, true, share_key_valid);
        }
        _ => return find_coordinator_admission(api_version, key_type, false, false),
    };
    let acl_allowed = authorizer.authorize(image, &AuthorizationRequest {
        principal,
        host,
        resource_type,
        resource_name: key,
        operation,
    }) == AuthorizationResult::Allow;
    find_coordinator_admission(api_version, key_type, acl_allowed, true)
}

pub(super) fn authorize_keys(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    api_version: i16,
    key_type: i8,
    keys: Vec<String>,
) -> Vec<KeySlot> {
    keys.into_iter()
        .map(|key| {
            let admission = key_admission(
                broker.config.authorizer.as_ref(),
                image,
                context.principal,
                context.peer,
                api_version,
                key_type,
                &key,
            );
            // Kafka's `getCoordinator` answers each refusal with
            // `(error, Node.noNode)` and no message.
            let error_code = match admission {
                FindCoordinatorAdmission::AllowGroup
                | FindCoordinatorAdmission::AllowTransaction
                | FindCoordinatorAdmission::AllowShare => return KeySlot::Resolve(key),
                FindCoordinatorAdmission::DenyGroup => codes::GROUP_AUTHORIZATION_FAILED,
                FindCoordinatorAdmission::DenyTransaction => {
                    codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                }
                FindCoordinatorAdmission::DenyCluster => codes::CLUSTER_AUTHORIZATION_FAILED,
                FindCoordinatorAdmission::InvalidRequest => codes::INVALID_REQUEST,
            };
            KeySlot::Rejected(no_node_row(key, error_code))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::SimpleAclAuthorizer,
            test_support::{peer, principal},
        },
        assert2::assert,
    };

    const SHARE_KEY: &str = "share-group:AAAAAAAAAAAAAAAAAAAAAA:0";

    fn deny_authorizer() -> SimpleAclAuthorizer {
        SimpleAclAuthorizer::new(std::collections::HashSet::new())
    }

    fn anon() -> krabka_security::Principal { principal("ANONYMOUS") }

    /// (version, key type, key, expected admission) under an authorizer that
    /// denies everything. The SHARE rows never consult the authorizer: the
    /// handler checks `ClusterAction` for the request before this runs.
    #[test]
    fn key_admission_follows_get_coordinator() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = peer();
        let rows = [
            (6, KEY_TYPE_GROUP, "g", FindCoordinatorAdmission::DenyGroup),
            (
                6,
                KEY_TYPE_TRANSACTION,
                "t",
                FindCoordinatorAdmission::DenyTransaction,
            ),
            (
                6,
                KEY_TYPE_SHARE,
                SHARE_KEY,
                FindCoordinatorAdmission::AllowShare,
            ),
            (
                6,
                KEY_TYPE_SHARE,
                "malformed",
                FindCoordinatorAdmission::InvalidRequest,
            ),
            (
                5,
                KEY_TYPE_SHARE,
                SHARE_KEY,
                FindCoordinatorAdmission::InvalidRequest,
            ),
            (6, i8::MAX, "g", FindCoordinatorAdmission::InvalidRequest),
        ];
        for (version, key_type, key, expected) in rows {
            let got = key_admission(&authz, &image, &anon(), &peer, version, key_type, key);
            assert!(got == expected, "v{version} type {key_type} key {key:?}");
        }
    }
}
