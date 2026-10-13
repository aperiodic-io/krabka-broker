//! `StreamsGroupTopologyDescriptionUpdate` (`api_key` 93, KIP-1331, Kafka
//! trunk).
//!
//! A Streams client pushes its full topology description with this RPC after
//! a heartbeat answers `TopologyDescriptionRequired`. Kafka stores it through
//! the plugin `group.streams.topology.description.plugin.class` names; krabka
//! builds in Kafka's in-memory plugin and runs it when that key names it (see
//! [`crate::coordinator::unified::streams::description`]). The answers, in
//! Kafka's order:
//!
//! 1. `UNSUPPORTED_VERSION` (35) with no message while the streams protocol is
//!    off, which is `KafkaApis.handleStreamsGroupTopologyDescriptionUpdate`.
//! 2. `GROUP_AUTHORIZATION_FAILED` (30) with no message when the caller lacks
//!    `Read` on the group. KIP-1331 treats a push like an offset commit, so
//!    `Read` is enough.
//! 3. `UNSUPPORTED_VERSION` (35) with the message of the
//!    `UnsupportedVersionException` that
//!    `GroupCoordinatorService.throwIfStreamsGroupTopologyDescriptionUpdateInvalid`
//!    throws when no plugin is configured, and `INVALID_REQUEST` (42) when
//!    the member id or the group id is empty.
//! 4. The coordinator errors of a group this broker does not coordinate.
//! 5. `GROUP_ID_NOT_FOUND` (69) for a group that is not a streams group, and
//!    then whatever the group's actor answers: `UNKNOWN_MEMBER_ID`,
//!    `INVALID_REQUEST` for another topology epoch or a malformed
//!    description, or success once the description is stored.

use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{
            GroupType,
            streams::actor::{DescriptionPush, PushAnswer, StreamsGroupActorMessage},
        },
        handlers::{
            ErrorResponse as _, RequestContext, group_coordinator_error, group_read_denied,
            streams_protocol_enabled,
        },
        task_util::{AskError, ask},
    },
    krabka_protocol::owned::{
        streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest,
        streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
    },
};

/// The message of trunk's `UnsupportedVersionException` for a broker with no
/// topology description plugin.
const NO_PLUGIN_MESSAGE: &str =
    "The broker has no streams group topology description plugin configured.";

context_handler! {
    StreamsGroupTopologyDescriptionUpdateRequest => StreamsGroupTopologyDescriptionUpdateResponse,
    (broker, req, _version, ctx),
    {
        let (error_code, error_message) = answer(broker, req, ctx).await;
        Ok(StreamsGroupTopologyDescriptionUpdateResponse::error(
            error_code,
            error_message,
        ))
    }
}

/// The error code and message that trunk answers `req` with.
async fn answer(
    broker: &Broker,
    req: StreamsGroupTopologyDescriptionUpdateRequest,
    ctx: &RequestContext<'_>,
) -> PushAnswer {
    let invalid = |message: &str| (codes::INVALID_REQUEST, Some(message.to_owned()));
    {
        let image = broker.controller.current_image();
        if !streams_protocol_enabled(broker, &image) {
            return (codes::UNSUPPORTED_VERSION, None);
        }
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return (codes::GROUP_AUTHORIZATION_FAILED, None);
        }
    }
    if !broker
        .config
        .streams_group
        .topology_description_plugin
        .is_configured()
    {
        return (
            codes::UNSUPPORTED_VERSION,
            Some(NO_PLUGIN_MESSAGE.to_owned()),
        );
    }
    if req.member_id.is_empty() {
        return invalid("MemberId can't be empty.");
    }
    if req.group_id.is_empty() {
        return invalid("GroupId can't be empty.");
    }
    if let Some(error_code) = group_coordinator_error(broker, &req.group_id) {
        return (error_code, None);
    }
    let coordinator = &broker.group_coordinator;
    let Some(handle) = coordinator.find_streams(&req.group_id) else {
        // `GroupMetadataManager.streamsGroup`'s two messages.
        let other_type = coordinator
            .group_type(&req.group_id)
            .is_some_and(|group_type| group_type != GroupType::Streams)
            || coordinator.find(&req.group_id).is_some();
        let message = if other_type {
            format!("Group {} is not a streams group.", req.group_id)
        } else {
            format!("Group {} not found.", req.group_id)
        };
        return (codes::GROUP_ID_NOT_FOUND, Some(message));
    };
    let push = DescriptionPush {
        member_id: req.member_id,
        topology_epoch: req.topology_epoch,
        description: req.topology_description,
    };
    let asked = ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::PushDescription {
            push: Box::new(push),
            reply,
        }
    })
    .await;
    match asked {
        Ok(answer) => answer,
        Err(AskError::Closed) => (codes::COORDINATOR_LOAD_IN_PROGRESS, None),
        Err(AskError::Dropped) => (codes::UNKNOWN_SERVER_ERROR, None),
    }
}

#[cfg(test)]
mod tests;
