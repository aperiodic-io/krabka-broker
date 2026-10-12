//! `JoinGroup` (`api_key=11`). This handler routes the request into the
//! group's unified actor as a `ClassicJoin` message, and waits for the reply.
//!
//! The actor parks the reply until the rebalance boundary, which is its
//! rebalance-deadline timer, an all-members-joined early completion, or a
//! membership change. The connection therefore blocks here for exactly as long
//! as the earlier `Notify`-based wait.

use krabka_protocol::owned::{
    join_group_request::JoinGroupRequest,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
};

use crate::{
    codes,
    coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag, join_append_error_code},
        config::NextGenConfig,
        streams::migration::DowngradeOutcome,
    },
    task_util::ask,
    time_util::now_ms,
};

context_handler! {
    // cargo-mutants: coordinator-backed response projection; integration-tested.
    #[cfg_attr(test, mutants::skip)]
    JoinGroupRequest => JoinGroupResponse,
    (broker, req, version, ctx),
    {
        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        {
            let img = broker.controller.current_image();
            let denied = crate::handlers::group_read_denied(
                broker.config.authorizer.as_ref(),
                &img,
                ctx,
                req.group_id.as_str(),
            );
            if denied {
                return Ok(respond(
                    version,
                    JoinGroupResponse {
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        ..Default::default()
                    },
                ));
            }
        }

        if let Some(error_code) = request_error(&req, &broker.group_coordinator.config)
            .or_else(|| crate::handlers::group_coordinator_error(broker, &req.group_id))
        {
            return Ok(respond(
                version,
                JoinGroupResponse {
                    error_code,
                    member_id: req.member_id,
                    ..Default::default()
                },
            ));
        }

        // Route to the one actor for this id, spawning a classic-kind actor if the
        // id is brand-new. Both RPC families reach the same actor; if a next-gen
        // consumer actor already owns the id, the actor's `ClassicJoin` arm replies
        // `INCONSISTENT_GROUP_PROTOCOL` — that is where the per-group kind lock now
        // lives.
        //
        // Mark the group as Classic so that a later StreamsGroupHeartbeat for the
        // same id can detect it as a classic group and either convert or reject it
        // (KIP-1071 cold upgrade). First-mark-wins: a prior `mark_next_gen` (or any
        // other type lock) from a consumer-protocol group is not overridden.
        // KIP-1071 cold downgrade: Kafka's `classicGroupJoin` refuses a join to a
        // streams group with members, and sends one to an empty streams group to
        // `classicGroupJoinToClassicGroup`, which deletes it. Non-streams group
        // ids pass through unchanged.
        match broker
            .group_coordinator
            .try_convert_streams_to_classic(&req.group_id, &req.member_id, now_ms())
            .await
        {
            Ok(DowngradeOutcome::RejectLiveMembers) => {
                return Ok(respond(
                    version,
                    JoinGroupResponse {
                        error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                        ..Default::default()
                    },
                ));
            }
            Ok(DowngradeOutcome::Removed) => {
                return Ok(respond(
                    version,
                    JoinGroupResponse {
                        error_code: codes::UNKNOWN_MEMBER_ID,
                        member_id: req.member_id,
                        ..Default::default()
                    },
                ));
            }
            Ok(DowngradeOutcome::NotStreams | DowngradeOutcome::Converted) => {}
            // The append future's failure: Kafka reverts the deletion and answers
            // `appendGroupMetadataErrorToResponseError` of the write error.
            Err(error) => {
                return Ok(respond(
                    version,
                    JoinGroupResponse {
                        error_code: join_append_error_code(&error),
                        ..Default::default()
                    },
                ));
            }
        }

        // Kafka's `classicGroupJoinToClassicGroup`: a member id names a member
        // of an existing group, so a group that does not exist is not created
        // for it.
        if !req.member_id.is_empty() && broker.group_coordinator.find(&req.group_id).is_none() {
            return Ok(respond(
                version,
                JoinGroupResponse {
                    error_code: codes::UNKNOWN_MEMBER_ID,
                    member_id: req.member_id,
                    ..Default::default()
                },
            ));
        }

        broker.group_coordinator.mark_classic(&req.group_id);
        let handle = broker
            .group_coordinator
            .get_or_create_group(&req.group_id, GroupKindTag::Classic);

        // A classic join to a consumer group runs Kafka's
        // `maybeUpdateRegularExpressions` with the request context of the join:
        // the actor resolves the group's patterns against this image with this
        // principal's `Describe` decisions. The offset is read before the image,
        // as the `ConsumerGroupHeartbeat` handler reads it.
        let metadata_offset = broker.controller.current_metadata_offset();
        let regex_resolver = std::sync::Arc::new(
            crate::coordinator::unified::regex_resolver::ImageTopicRegexResolver::new(
                broker.controller.current_image(),
                metadata_offset,
                broker.config.authorizer.clone(),
                ctx.principal.clone(),
                *ctx.peer,
            ),
        );

        // A closed mailbox and a dropped reply both answer REBALANCE_IN_PROGRESS.
        let Ok(result) = ask(&handle.tx, |reply| GroupActorMessage::ClassicJoin {
            req,
            version,
            client_id: ctx.client_id.unwrap_or_default().to_owned(),
            client_host: ctx.client_host(),
            regex_resolver,
            reply,
        })
        .await
        else {
            return Ok(respond(
                version,
                JoinGroupResponse {
                    error_code: codes::REBALANCE_IN_PROGRESS,
                    ..Default::default()
                },
            ));
        };

        let resp = JoinGroupResponse {
            error_code: result.error_code,
            generation_id: result.generation_id,
            protocol_type: result.protocol_type,
            protocol_name: result.protocol_name,
            leader: result.leader,
            skip_assignment: result.skip_assignment,
            member_id: result.member_id,
            members: result
                .members
                .into_iter()
                .map(|m| JoinGroupResponseMember {
                    member_id: m.member_id,
                    group_instance_id: m.group_instance_id,
                    metadata: m.metadata,
                    ..Default::default()
                })
                .collect(),
            throttle_time_ms: 0,
            ..Default::default()
        };
        Ok(respond(version, resp))
    }
}

/// The request checks of Kafka's `GroupCoordinatorService.joinGroup`, which
/// answer before any group is looked up: an empty group id, then a session
/// timeout outside `group.min.session.timeout.ms` and
/// `group.max.session.timeout.ms`.
fn request_error(req: &JoinGroupRequest, config: &NextGenConfig) -> Option<i16> {
    let session_timeout = u64::try_from(req.session_timeout_ms).ok();
    let min = u64::try_from(config.classic_min_session_timeout.as_millis()).unwrap_or(u64::MAX);
    let max = u64::try_from(config.classic_max_session_timeout.as_millis()).unwrap_or(u64::MAX);
    if req.group_id.is_empty() {
        Some(codes::INVALID_GROUP_ID)
    } else if session_timeout.is_none_or(|timeout| timeout < min || timeout > max) {
        Some(codes::INVALID_SESSION_TIMEOUT)
    } else {
        None
    }
}

/// Answers `resp` after the `ProtocolName` normalisation of Kafka's
/// `JoinGroupResponse` constructor, which every `JoinGroup` reply passes
/// through: from v7, where the field is nullable, an empty name goes on the
/// wire as null. Below v7 a null name already encodes as the empty string.
fn respond(version: i16, mut resp: JoinGroupResponse) -> JoinGroupResponse {
    if version >= 7 && resp.protocol_name.as_deref() == Some("") {
        resp.protocol_name = None;
    }
    resp
}

#[cfg(test)]
mod tests {
    use krabka_protocol::{Decode, owned::join_group_response};

    use super::*;
    use crate::handlers::encode_response;

    /// #791: Kafka's `GroupCoordinatorService.joinGroup` request checks with
    /// the default `group.min.session.timeout.ms` and
    /// `group.max.session.timeout.ms`.
    #[test]
    fn request_checks_match_kafka_defaults() {
        let config = NextGenConfig::default();
        for (group_id, session_timeout_ms, want) in [
            ("", 10_000, Some(codes::INVALID_GROUP_ID)),
            ("g", -1, Some(codes::INVALID_SESSION_TIMEOUT)),
            ("g", 5_999, Some(codes::INVALID_SESSION_TIMEOUT)),
            ("g", 6_000, None),
            ("g", 1_800_000, None),
            ("g", 1_800_001, Some(codes::INVALID_SESSION_TIMEOUT)),
        ] {
            let req = JoinGroupRequest {
                group_id: group_id.into(),
                session_timeout_ms,
                ..Default::default()
            };
            assert2::check!(
                request_error(&req, &config) == want,
                "{group_id:?} {session_timeout_ms}"
            );
        }
    }

    /// Kafka's `JoinGroupResponse` constructor sends an empty `ProtocolName`
    /// as null from v7 and as `""` below it; a chosen protocol passes through.
    #[test]
    fn protocol_name_normalised_like_kafka() {
        for version in join_group_response::MIN_VERSION..=join_group_response::MAX_VERSION {
            let empty = (version < 7).then(String::new);
            for (sent, want) in [
                (Some(String::new()), empty.clone()),
                (None, empty.clone()),
                (Some("range".to_owned()), Some("range".to_owned())),
            ] {
                let bytes = encode_response(
                    &respond(
                        version,
                        JoinGroupResponse {
                            error_code: codes::GROUP_AUTHORIZATION_FAILED,
                            protocol_name: sent.clone(),
                            ..Default::default()
                        },
                    ),
                    version,
                )
                .expect("encode");
                let mut cur: &[u8] = &bytes;
                let got = JoinGroupResponse::decode(&mut cur, version).expect("decode");
                assert2::check!(
                    got == unthrottled_wire!(JoinGroupResponse {
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        generation_id: -1,
                        protocol_type: None,
                        protocol_name: want,
                        leader: String::new(),
                        skip_assignment: false,
                        member_id: String::new(),
                        members: vec![],
                    }),
                    "v{version} {sent:?}"
                );
            }
        }
    }

    /// A response that names its protocol leaves `respond` unchanged.
    #[test]
    fn respond_passes_a_chosen_protocol_through() {
        let resp = JoinGroupResponse {
            error_code: codes::NONE,
            generation_id: 1,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            leader: "member-1".into(),
            member_id: "member-1".into(),
            members: vec![],
            throttle_time_ms: 0,
            ..Default::default()
        };
        assert2::check!(respond(5, resp.clone()) == resp);
    }
}
