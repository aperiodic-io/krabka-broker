//! The ACL gates that a handler applies before it does any work.
//!
//! Every gate returns `true` when the authorizer denies the principal, so a
//! handler reads as `if <gate>(..) { return <error>; }` and each caller stays
//! free to choose the error code that its RPC answers with.

use {
    super::{RequestContext, acl_wire},
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer, authorize_topics},
        broker::Broker,
        codes::{self, GROUP_AUTHORIZATION_FAILED, UNSUPPORTED_VERSION},
        handlers::describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    std::collections::{HashMap, HashSet},
};

/// Kafka's resource-specific refusal for the two config mutation APIs.
/// Legacy `AlterConfigs` does not support broker logger resources.
pub(crate) fn config_resource_refusal(
    authorizer: &dyn Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    (kind, name): (i8, &str),
    broker_logger_supported: bool,
) -> Option<(i16, String)> {
    let (resource_type, resource_name, code, message) = match kind {
        RESOURCE_TYPE_TOPIC => (
            ResourceType::Topic,
            name,
            codes::TOPIC_AUTHORIZATION_FAILED,
            "Topic authorization failed.",
        ),
        RESOURCE_TYPE_GROUP => (
            ResourceType::Group,
            name,
            codes::GROUP_AUTHORIZATION_FAILED,
            "Group authorization failed.",
        ),
        kind if matches!(kind, RESOURCE_TYPE_BROKER | RESOURCE_TYPE_CLIENT_METRICS)
            || (broker_logger_supported && kind == RESOURCE_TYPE_BROKER_LOGGER) =>
        {
            (
                ResourceType::Cluster,
                acl_wire::CLUSTER_RESOURCE_NAME,
                codes::CLUSTER_AUTHORIZATION_FAILED,
                "Cluster authorization failed.",
            )
        }
        other => {
            return Some((
                codes::INVALID_REQUEST,
                format!("Unknown resource type {other}"),
            ));
        }
    };
    acl_denied(
        authorizer,
        image,
        ctx,
        resource_type,
        resource_name,
        AclOperation::AlterConfigs,
    )
    .then(|| (code, message.to_owned()))
}

pub(crate) fn acl_denied(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> bool {
    authorizer.authorize(image, &AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type,
        resource_name,
        operation,
    }) == AuthorizationResult::Deny
}

pub(crate) fn acl_denied_quiet(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> bool {
    authorizer.authorize_quiet(image, &AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type,
        resource_name,
        operation,
    }) == AuthorizationResult::Deny
}

krabka_macros::named_acl_gates!(
    group_read_denied: Group(Read, group_id),
    group_delete_denied: Group(Delete, group_id),
    /// The `Describe` gate on `Group(group_id)`, the twin of [`group_read_denied`].
    group_describe_denied: Group(Describe, group_id),
    cluster_alter_denied: Cluster(Alter),
    cluster_alter_configs_denied: Cluster(AlterConfigs),
    cluster_action_denied: Cluster(ClusterAction),
    /// The `Describe` gate on `Cluster("kafka-cluster")`, the twin of [`cluster_alter_denied`].
    /// The barrier, write-freeze and break-glass control planes all use this gate.
    cluster_describe_denied: Cluster(Describe),
);

/// Kafka's cluster shortcut is quiet: a denial falls back to per-topic checks.
pub(crate) fn cluster_shortcut_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    operation: AclOperation,
) -> bool {
    acl_denied_quiet(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        ResourceType::Cluster,
        acl_wire::CLUSTER_RESOURCE_NAME,
        operation,
    )
}

/// The authorizer's decision on `operation` on `Topic(name)` for each of
/// `names`, for `ctx`'s principal.
///
/// Every name is authorized through [`authorize_topics`], so each denial is
/// audited the way a per-topic refusal is.
pub(crate) fn topic_decisions<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    operation: AclOperation,
    names: impl IntoIterator<Item = &'a str>,
) -> HashMap<&'a str, AuthorizationResult> {
    authorize_topics(authorizer, image, ctx.principal, ctx.peer, operation, names)
}

/// The names among `names` that [`topic_decisions`] decides `decision`.
fn topics_decided<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    operation: AclOperation,
    names: impl IntoIterator<Item = &'a str>,
    decision: AuthorizationResult,
) -> HashSet<String> {
    topic_decisions(authorizer, image, ctx, operation, names)
        .into_iter()
        .filter(|(_, result)| *result == decision)
        .map(|(name, _)| name.to_owned())
        .collect()
}

/// The names among `names` whose `operation` on `Topic` the authorizer
/// denies to `ctx`'s principal.
///
/// A name the caller never passed is absent, so a lookup reads it as allowed.
#[krabka_macros::topic_filter(Deny)]
pub(crate) fn denied_topics<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    operation: AclOperation,
    names: impl IntoIterator<Item = &'a str>,
) -> HashSet<String> {
}

/// The names among `names` whose `operation` on `Topic` the authorizer
/// allows to `ctx`'s principal, the twin of [`denied_topics`].
///
/// A name the caller never passed is absent, so a lookup reads it as denied.
#[krabka_macros::topic_filter(Allow)]
pub(crate) fn allowed_topics<'a>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    operation: AclOperation,
    names: impl IntoIterator<Item = &'a str>,
) -> HashSet<String> {
}

/// `true` when `names` is non-empty and at least one of its distinct names is
/// `Describe`-denied for `ctx`'s principal.
///
/// It is the subscribed-topic-names gate of the `ConsumerGroupHeartbeat` and
/// `ShareGroupHeartbeat` handlers. A `None` or empty list means Kafka's
/// `subscribedTopicSet` is empty, which is vacuously fully authorized.
/// KIP-932's `ShareGroupHeartbeatRequest` carries no `SubscribedTopicRegex`,
/// so this names gate is the whole of its topic check.
pub(crate) fn subscribed_names_describe_denied(
    authorizer: &dyn Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    names: Option<&[String]>,
) -> bool {
    let Some(names) = names.filter(|names| !names.is_empty()) else {
        return false;
    };
    let unique: HashSet<&str> = names.iter().map(String::as_str).collect();
    !denied_topics(
        authorizer,
        image,
        ctx,
        krabka_metadata::AclOperation::Describe,
        unique,
    )
    .is_empty()
}

/// Subscription denial code shared by consumer and share heartbeats.
pub(crate) fn subscribed_names_refusal(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    names: Option<&[String]>,
) -> Option<i16> {
    subscribed_names_describe_denied(broker.config.authorizer.as_ref(), image, ctx, names)
        .then_some(codes::TOPIC_AUTHORIZATION_FAILED)
}

/// Kafka's `filterByAuthorized(DESCRIBE, TOPIC, requiredTopics)` as the
/// streams-group handlers read it: `true` when any of `names` is
/// `Describe`-denied for `ctx`'s principal.
///
/// It stops at the first denial, so a name after it is never authorized and
/// never audited.
pub(crate) fn any_topic_describe_denied(
    authorizer: &dyn Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    names: &[String],
) -> bool {
    names.iter().any(|topic| {
        acl_denied(
            authorizer,
            image,
            ctx,
            krabka_metadata::ResourceType::Topic,
            topic,
            krabka_metadata::AclOperation::Describe,
        )
    })
}

/// The name a `Produce` or `Fetch` topic row stands for: its `name` when the
/// wire carries one, else the name that `topic_id` resolves to in `image`.
///
/// A row with neither, or an id that does not resolve, gives the empty name.
pub(crate) fn requested_topic_name(
    image: &krabka_metadata::MetadataImage,
    name: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) -> String {
    if !name.is_empty() {
        name.to_owned()
    } else if topic_id != krabka_protocol::primitives::uuid::Uuid::ZERO {
        image
            .topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))
            .unwrap_or_default()
            .to_owned()
    } else {
        String::new()
    }
}

/// Protocol availability precedes the audited group-read authorization check.
pub(crate) fn group_protocol_refusal(
    enabled: bool,
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &super::RequestContext<'_>,
    group: &str,
) -> Option<i16> {
    if !enabled {
        Some(UNSUPPORTED_VERSION)
    } else if group_read_denied(broker.config.authorizer.as_ref(), image, ctx, group) {
        Some(GROUP_AUTHORIZATION_FAILED)
    } else {
        None
    }
}

/// Classic group calls authorize first, release the image, then validate and route.
pub(crate) fn classic_group_refusal(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    group: &str,
) -> Option<i16> {
    {
        let image = broker.controller.current_image();
        if group_read_denied(broker.config.authorizer.as_ref(), &image, ctx, group) {
            return Some(codes::GROUP_AUTHORIZATION_FAILED);
        }
    }
    group
        .is_empty()
        .then_some(codes::INVALID_GROUP_ID)
        .or_else(|| super::group_coordinator_error(broker, group))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::SimpleAclAuthorizer,
            handlers::test_support::operators_principal as principal,
            test_support::{allow_acl, peer},
        },
        assert2::{assert, check},
        krabka_metadata::{AclOperation, MetadataImage, ResourceType},
        std::{collections::HashSet, net::SocketAddr},
    };

    fn test_context<'a>(
        principal: &'a krabka_security::Principal,
        peer: &'a SocketAddr,
        client_id: &'a str,
    ) -> RequestContext<'a> {
        RequestContext::new(
            principal,
            peer,
            client_id,
            "connection-a",
            false,
            "PLAINTEXT",
        )
    }

    #[test]
    fn acl_denied_reports_simple_acl_denial() {
        let authorizer = SimpleAclAuthorizer::new(HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer, "client-a");

        assert!(acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "orders",
            AclOperation::Describe,
        ));
    }

    /// The barrier, write-freeze, and break-glass read APIs all gate on
    /// [`cluster_describe_denied`], and every one of them answers
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) when it returns `true`.
    #[test]
    fn cluster_describe_denied_refuses_a_principal_with_no_cluster_describe() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer, "krabka-guard");

        for (label, super_users, denied) in [
            ("an empty acl store denies by default", Vec::new(), true),
            (
                "a super user reads the cluster",
                vec![principal.name.clone()],
                false,
            ),
            (
                "another super user does not lend its grant",
                vec!["bob".to_string()],
                true,
            ),
        ] {
            let authorizer =
                SimpleAclAuthorizer::new(super_users.into_iter().collect::<HashSet<String>>());

            check!(
                cluster_describe_denied(&authorizer, &image, &ctx) == denied,
                "case {label}"
            );
        }

        // The code every caller answers on a denial, and the number Kafka
        // assigns it.
        check!(crate::codes::CLUSTER_AUTHORIZATION_FAILED == 31);
    }

    /// [`denied_topics`] and [`allowed_topics`] keep exactly the names the
    /// authorizer denies and allows, with each one owned and every duplicate
    /// folded into one entry.
    #[test]
    fn denied_and_allowed_topics_split_the_names_by_decision() {
        let principal = principal();
        let peer = peer();
        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            false,
            "PLAINTEXT",
        );
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1AccessControlEntry(
            allow_acl().operation(AclOperation::Write).call(),
        ));
        let authorizer = SimpleAclAuthorizer::new(HashSet::new());

        for (label, operation, names, denied, allowed) in [
            ("no names", AclOperation::Write, vec![], vec![], vec![]),
            (
                "the granted name passes",
                AclOperation::Write,
                vec!["orders"],
                vec![],
                vec!["orders"],
            ),
            (
                "an ungranted name is denied once",
                AclOperation::Write,
                vec!["orders", "shipments", "shipments"],
                vec!["shipments"],
                vec!["orders"],
            ),
            (
                "a grant for another operation does not lend itself",
                AclOperation::Read,
                vec!["orders"],
                vec!["orders"],
                vec![],
            ),
        ] {
            let owned = |names: Vec<&str>| -> HashSet<String> {
                names.into_iter().map(String::from).collect()
            };
            check!(
                denied_topics(&authorizer, &image, &ctx, operation, names.clone()) == owned(denied),
                "case {label}"
            );
            check!(
                allowed_topics(&authorizer, &image, &ctx, operation, names) == owned(allowed),
                "case {label}"
            );
        }
    }

    /// [`requested_topic_name`] prefers the wire name and gives the empty
    /// name when neither the name nor the id names a topic.
    #[test]
    fn requested_topic_name_prefers_the_wire_name() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let unknown = krabka_protocol::primitives::uuid::Uuid([7; 16]);
        let zero = krabka_protocol::primitives::uuid::Uuid::ZERO;

        for (label, name, id, expected) in [
            ("a wire name wins over an id", "orders", unknown, "orders"),
            ("no name and no id", "", zero, ""),
            ("an id that does not resolve", "", unknown, ""),
        ] {
            check!(
                requested_topic_name(&image, name, id) == expected,
                "case {label}"
            );
        }
    }
}
