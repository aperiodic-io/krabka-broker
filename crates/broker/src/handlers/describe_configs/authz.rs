//! The per-resource `DescribeConfigs` ACL check, and the result entry a denied
//! resource gets.
//!
//! Authorization is decided per resource entry rather than per request, so one
//! denied topic in a multi-resource `DescribeConfigs` leaves the authorized
//! entries in the same response. This module holds that decision and nothing
//! about which configs a resource reports.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::describe_configs_response::DescribeConfigsResult;

use super::wire::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
    RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
};

/// Per-resource `DescribeConfigs` ACL check.
///
/// A Topic resource needs `DescribeConfigs` on `Topic(name)`, and a Group
/// resource on `Group(name)`. A Broker, `BROKER_LOGGER` or KIP-714
/// `CLIENT_METRICS` resource needs `DescribeConfigs` on
/// `Cluster("kafka-cluster")`, as Kafka's `ConfigHelper` requires: a
/// subscription's `match` names client ids and source addresses.
///
/// This function returns the authorization-failed code to stamp on a Deny. It
/// returns `None` when the check allows the request. A resource type Kafka
/// does not describe never reaches it: [`unexpected_resource_type_results`]
/// refuses the whole request first.
pub(super) fn resource_authz_failure(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
    resource_type: i8,
    resource_name: &str,
) -> Option<i16> {
    let (rt, name, failure_code): (ResourceType, &str, i16) = match resource_type {
        RESOURCE_TYPE_TOPIC => (
            ResourceType::Topic,
            resource_name,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_BROKER | RESOURCE_TYPE_BROKER_LOGGER | RESOURCE_TYPE_CLIENT_METRICS => (
            ResourceType::Cluster,
            crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_GROUP => (
            ResourceType::Group,
            resource_name,
            codes::GROUP_AUTHORIZATION_FAILED,
        ),
        _ => return None,
    };
    let allow = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: rt,
            resource_name: name,
            operation: AclOperation::DescribeConfigs,
        },
    );
    (allow == AuthorizationResult::Deny).then_some(failure_code)
}

/// Kafka's `Errors.INVALID_REQUEST.message()`.
const INVALID_REQUEST_MESSAGE: &str = "This most likely occurs because of a request being \
     malformed by the client library or the message was sent to an incompatible broker. See the \
     broker logs for more details.";

/// Kafka's answer to a request that names a resource type it does not
/// describe, or `None` when every type is one it does.
///
/// `ConfigHelper.handleDescribeConfigsRequest` partitions the request by
/// authorization first and throws `InvalidRequestException("Unexpected
/// resource type ...")` for any type but `BROKER`, `BROKER_LOGGER`,
/// `CLIENT_METRICS`, `TOPIC` and `GROUP`. Nothing catches it inside, so
/// `DescribeConfigsRequest.getErrorResponse` stamps *every* resource of the
/// request with `INVALID_REQUEST`, the generic message and no configs.
pub(super) fn unexpected_resource_type_results(
    resources: &[krabka_protocol::owned::describe_configs_request::DescribeConfigsResource],
) -> Option<Vec<DescribeConfigsResult>> {
    let known = |resource_type| {
        matches!(
            resource_type,
            RESOURCE_TYPE_TOPIC
                | RESOURCE_TYPE_BROKER
                | RESOURCE_TYPE_BROKER_LOGGER
                | RESOURCE_TYPE_CLIENT_METRICS
                | RESOURCE_TYPE_GROUP
        )
    };
    resources
        .iter()
        .any(|resource| !known(resource.resource_type))
        .then(|| {
            resources
                .iter()
                .map(|resource| DescribeConfigsResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(INVALID_REQUEST_MESSAGE.to_owned()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: Vec::new(),
                    ..Default::default()
                })
                .collect()
        })
}

/// Builds a `DescribeConfigsResult` that carries only the
/// authorization-failed error code, for a denied resource.
pub(super) fn denied_result(
    resource_type: i8,
    resource_name: String,
    error_code: i16,
) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code,
        error_message: Some(authorization_failed_message(error_code).into()),
        resource_type,
        resource_name,
        configs: Vec::new(),
        ..Default::default()
    }
}

/// Kafka's `Errors.message()` for the three authorization failures.
const fn authorization_failed_message(error_code: i16) -> &'static str {
    match error_code {
        codes::TOPIC_AUTHORIZATION_FAILED => "Topic authorization failed.",
        codes::GROUP_AUTHORIZATION_FAILED => "Group authorization failed.",
        _ => "Cluster authorization failed.",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataImage;
    use uuid::Uuid;

    use super::*;
    use crate::test_support::peer;

    fn anon() -> krabka_security::Principal {
        crate::test_support::principal("ANONYMOUS")
    }

    macro_rules! empty_acl_resource_check {
        (($authorizer:ident, $image:ident, $peer:ident, $code:ident), $kind:expr, $name:expr) => {
            let $authorizer =
                crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
            let $image = MetadataImage::new(Uuid::nil());
            let $peer = peer();
            let $code =
                super::resource_authz_failure(&$authorizer, &$image, &anon(), &$peer, $kind, $name);
        };
    }

    #[test]
    fn topic_resource_denied_yields_topic_authorization_failed() {
        empty_acl_resource_check!((authz, image, peer, code), super::RESOURCE_TYPE_TOPIC, "t");
        assert!(code == Some(crate::codes::TOPIC_AUTHORIZATION_FAILED));
        let res = super::denied_result(
            super::RESOURCE_TYPE_TOPIC,
            "t".into(),
            crate::codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected = tagged_wire!(DescribeConfigsResult {
            error_code: crate::codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".to_string()),
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "t".to_string(),
            configs: Vec::new(),
        });
        assert!(res == expected);
    }

    /// Kafka's `ConfigHelper` authorizes a KIP-714 subscription with
    /// `DescribeConfigs` on the cluster. `AlterConfigs` implies it, and
    /// `Describe` does not.
    #[test]
    fn a_client_metrics_resource_needs_describe_configs_on_the_cluster() {
        let cases = [
            (None, Some(crate::codes::CLUSTER_AUTHORIZATION_FAILED)),
            (Some(AclOperation::DescribeConfigs), None),
            (Some(AclOperation::AlterConfigs), None),
            (
                Some(AclOperation::Describe),
                Some(crate::codes::CLUSTER_AUTHORIZATION_FAILED),
            ),
        ];
        let authz = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let peer = peer();
        for (grant, want) in cases {
            let mut image = MetadataImage::new(Uuid::nil());
            if let Some(operation) = grant {
                image.apply(&krabka_metadata::MetadataRecord::V1AccessControlEntry(
                    crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
                        resource_type: ResourceType::Cluster,
                        resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                        principal: "User:ANONYMOUS",
                        operation,
                    }),
                ));
            }
            let code = super::resource_authz_failure(
                &authz,
                &image,
                &anon(),
                &peer,
                super::RESOURCE_TYPE_CLIENT_METRICS,
                "sub-a",
            );
            assert!(code == want, "{grant:?}");
        }
        let denied = super::denied_result(
            super::RESOURCE_TYPE_CLIENT_METRICS,
            "sub-a".into(),
            crate::codes::CLUSTER_AUTHORIZATION_FAILED,
        );
        assert!(
            denied
                == tagged_wire!(DescribeConfigsResult {
                    error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("Cluster authorization failed.".to_owned()),
                    resource_type: super::RESOURCE_TYPE_CLIENT_METRICS,
                    resource_name: "sub-a".to_owned(),
                    configs: Vec::new(),
                })
        );
    }

    /// One resource of a type Kafka does not describe fails the whole
    /// request: every resource, the known ones included, is `INVALID_REQUEST`
    /// with the generic message and no configs.
    #[test]
    fn an_unknown_resource_type_refuses_every_resource_of_the_request() {
        use krabka_protocol::owned::describe_configs_request::DescribeConfigsResource;

        let resource = |resource_type, name: &str| DescribeConfigsResource {
            resource_type,
            resource_name: name.to_owned(),
            ..Default::default()
        };
        let known = [
            resource(RESOURCE_TYPE_TOPIC, "t"),
            resource(RESOURCE_TYPE_GROUP, "g"),
        ];
        assert!(unexpected_resource_type_results(&known).is_none());

        let mixed = [resource(RESOURCE_TYPE_TOPIC, "t"), resource(0, "x")];
        let refused = |resource_type, name: &str| {
            tagged_wire!(DescribeConfigsResult {
                error_code: crate::codes::INVALID_REQUEST,
                error_message: Some(
                    "This most likely occurs because of a request being malformed by the client \
                 library or the message was sent to an incompatible broker. See the broker logs \
                 for more details."
                        .to_owned(),
                ),
                resource_type,
                resource_name: name.to_owned(),
                configs: Vec::new(),
            })
        };
        assert!(
            unexpected_resource_type_results(&mixed)
                == Some(vec![refused(RESOURCE_TYPE_TOPIC, "t"), refused(0, "x")])
        );
    }

    #[test]
    fn broker_resource_denied_yields_cluster_authorization_failed() {
        empty_acl_resource_check!((authz, image, peer, code), super::RESOURCE_TYPE_BROKER, "1");
        assert!(code == Some(crate::codes::CLUSTER_AUTHORIZATION_FAILED));
    }
}
