//! The `Describe` and `Delete` ACL checks that gate `DeleteTopics`.
//!
//! Kafka checks `Delete` on the `Cluster` resource once, as a shortcut: an
//! Allow there makes every candidate topic describable and deletable without
//! a further lookup. A Deny is not a whole-request failure -- it falls back to
//! `Describe` and `Delete` on each `Topic(name)` individually
//! (`ControllerApis.handleDeleteTopics`/`deleteTopics`), so a principal
//! scoped to a literal or prefixed topic ACL can still delete the topics its
//! ACL covers.
//!
//! The two decisions are separate because they answer different rows. A name
//! row the principal may not describe answers `TOPIC_AUTHORIZATION_FAILED`
//! before the existence check, so a caller cannot tell a hidden topic from an
//! absent one. An id row the principal may not delete carries the topic name
//! only when the principal may describe the topic.

use std::collections::HashSet;

use krabka_metadata::AclOperation;
use krabka_protocol::{
    owned::delete_topics_response::DeletableTopicResult, primitives::uuid::Uuid as WireUuid,
};

use super::{request::TopicNameRequest, wire::delete_topic_result};
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    handlers::{RequestContext, cluster_shortcut_denied},
};

/// The topics of one request that the principal may describe and may delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TopicAccess {
    /// Cluster `Delete` is allowed: every topic is describable and deletable.
    Cluster,
    /// Cluster `Delete` is denied: the per-topic decisions.
    Topics {
        /// The names `Describe` on `Topic(name)` allows.
        describable: HashSet<String>,
        /// The names `Delete` on `Topic(name)` allows.
        deletable: HashSet<String>,
    },
}

impl TopicAccess {
    /// Whether the principal may describe `name`.
    pub(super) fn may_describe(&self, name: &str) -> bool {
        match self {
            Self::Cluster => true,
            Self::Topics { describable, .. } => describable.contains(name),
        }
    }

    /// Whether the principal may delete `name`.
    pub(super) fn may_delete(&self, name: &str) -> bool {
        match self {
            Self::Cluster => true,
            Self::Topics { deletable, .. } => deletable.contains(name),
        }
    }
}

/// Authorizes `Describe` and `Delete` for every name in `names`.
///
/// Cluster `Delete` is checked once first: when it is allowed, this answers
/// [`TopicAccess::Cluster`] without a per-topic lookup.
pub(super) fn topic_access<'a>(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    names: impl IntoIterator<Item = &'a str> + Clone,
) -> TopicAccess {
    if !cluster_shortcut_denied(broker, image, context, AclOperation::Delete) {
        return TopicAccess::Cluster;
    }
    TopicAccess::Topics {
        describable: allowed(
            broker,
            image,
            context,
            AclOperation::Describe,
            names.clone(),
        ),
        deletable: allowed(broker, image, context, AclOperation::Delete, names),
    }
}

/// What the checks ahead of the deletion decide for one request row.
#[derive(Debug, PartialEq)]
pub(super) enum Admission {
    /// The row is answered without a deletion.
    Answer(DeletableTopicResult),
    /// The principal may delete this existing topic.
    Delete {
        /// The topic name.
        name: String,
        /// The topic id, which the response row carries.
        topic_id: WireUuid,
    },
}

/// Runs Kafka's existence and authorization checks on one validated row.
///
/// An id row that names no topic answers `UNKNOWN_TOPIC_ID` with its id. An
/// id row the principal may not delete answers `TOPIC_AUTHORIZATION_FAILED`
/// with its id, and with the name only when the principal may describe the
/// topic. A name row answers `TOPIC_AUTHORIZATION_FAILED` when the principal
/// may not describe it, `UNKNOWN_TOPIC_OR_PARTITION` when no topic has the
/// name, and `TOPIC_AUTHORIZATION_FAILED` when the principal may not delete
/// it, in that order, each with the zero id (`ControllerApis.deleteTopics`).
pub(super) fn admit_row(
    (name, by_id, requested_id): TopicNameRequest,
    image: &krabka_metadata::MetadataImage,
    access: &TopicAccess,
) -> Admission {
    let answer =
        |name, topic_id, code| Admission::Answer(delete_topic_result(name, topic_id, code));
    if by_id {
        return match name {
            None => answer(None, requested_id, codes::UNKNOWN_TOPIC_ID),
            Some(name) if access.may_delete(&name) => Admission::Delete {
                name,
                topic_id: requested_id,
            },
            Some(name) if access.may_describe(&name) => {
                answer(Some(name), requested_id, codes::TOPIC_AUTHORIZATION_FAILED)
            }
            Some(_) => answer(None, requested_id, codes::TOPIC_AUTHORIZATION_FAILED),
        };
    }
    let Some(name) = name else {
        return answer(None, WireUuid::ZERO, codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    if !access.may_describe(&name) {
        return answer(
            Some(name),
            WireUuid::ZERO,
            codes::TOPIC_AUTHORIZATION_FAILED,
        );
    }
    let Some(topic) = image.topic(&name) else {
        return answer(
            Some(name),
            WireUuid::ZERO,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        );
    };
    let topic_id = WireUuid(topic.topic_id.into_bytes());
    if access.may_delete(&name) {
        Admission::Delete { name, topic_id }
    } else {
        answer(
            Some(name),
            WireUuid::ZERO,
            codes::TOPIC_AUTHORIZATION_FAILED,
        )
    }
}

/// The names in `names` that `operation` on `Topic(name)` allows.
fn allowed<'a>(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    operation: AclOperation,
    names: impl IntoIterator<Item = &'a str>,
) -> HashSet<String> {
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        operation,
        names,
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Allow)
    .map(|(name, _)| name.to_string())
    .collect()
}
