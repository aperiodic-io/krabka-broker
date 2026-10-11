//! The group-level `Describe` gate that every `OffsetFetch` request passes
//! through, on both the legacy and the KIP-516 request shapes.
//!
//! Committed offsets belong to a group, so the first authorization decision is
//! always `Describe` on `Group(group_id)`; the per-topic `Describe` checks that
//! follow are made where the topic rows are built. Keeping the group decision
//! in one place is what lets the v0 to v7 and v8 and above paths apply it
//! identically.

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes::GROUP_AUTHORIZATION_FAILED,
    handlers::{RequestContext, group_coordinator_error, group_describe_denied},
};

/// Gate the group's `Describe` permission before checking its coordinator.
/// A refusal belongs to the whole legacy response or the v8+ group entry.
pub(super) fn group_error(
    broker: &Broker,
    context: &RequestContext<'_>,
    group_id: &str,
) -> Option<i16> {
    if group_describe_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        context,
        group_id,
    ) {
        Some(GROUP_AUTHORIZATION_FAILED)
    } else {
        group_coordinator_error(broker, group_id)
    }
}

/// Share the per-topic Describe batch across both `OffsetFetch` wire shapes.
pub(super) fn topic_decisions<'a>(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    names: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashMap<&'a str, AuthorizationResult> {
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        krabka_metadata::AclOperation::Describe,
        names,
    )
}

/// Fetch-all replies silently omit denied topics and retain sorted topic order.
pub(super) fn visible_topics<'a, P>(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    topics: std::collections::BTreeMap<&'a str, Vec<P>>,
) -> impl Iterator<Item = (&'a str, Vec<P>)> + use<'a, P> {
    let decisions = topic_decisions(broker, image, context, topics.keys().copied());
    topics
        .into_iter()
        .filter(move |(name, _)| decisions.get(name).copied() == Some(AuthorizationResult::Allow))
}
