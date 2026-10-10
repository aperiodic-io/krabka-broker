//! `ShareGroupDescribe` (`api_key` 77), from KIP-932. It returns one
//! `DescribedGroup` per requested `group_id`, built from the share actor's
//! `Describe` view, as Kafka's `KafkaApis.handleShareGroupDescribe` does.
//!
//! `network::dispatch` intercepts this request inline, not through
//! `build_table`, so the handler receives the per-connection principal and the
//! peer `SocketAddr` for the per-group `Describe` ACL gate.

use krabka_protocol::owned::{
    share_group_describe_request::ShareGroupDescribeRequest,
    share_group_describe_response::{DescribedGroup, ShareGroupDescribeResponse},
};

use crate::{
    codes,
    coordinator::unified::{GroupType, share::actor::ShareGroupActorMessage},
    handlers::{
        authorized_operations::{DescribedGroupRow as _, fill_group_authorized_operations},
        consumer_group_describe::{DescribedTopics, hide_undescribable_topics},
    },
    task_util::{AskError, ask},
};

impl DescribedTopics for DescribedGroup {
    fn topics(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .flat_map(|member| &member.assignment.topic_partitions)
            .map(|topic| topic.topic_name.as_str())
    }
}

context_handler! {
    ShareGroupDescribeRequest => ShareGroupDescribeResponse,
    (broker, req, _version, ctx),
    {
        // Kafka's `isShareGroupProtocolEnabled` gate comes before any ACL check,
        // and `getErrorResponse` answers every requested group. Share groups are
        // on from a finalized `share.version` of 1.
        let image = broker.controller.current_image();
        if !crate::features::share_groups_enabled(&image) {
            let groups = req
                .group_ids
                .iter()
                .map(|gid| DescribedGroup::error_row(gid, codes::UNSUPPORTED_VERSION, None))
                .collect();
            return Ok(response(groups));
        }

        let authorizer = broker.config.authorizer.as_ref();
        let coordinator = &broker.group_coordinator;
        // Kafka adds the GROUP_AUTHORIZATION_FAILED rows first, then the
        // coordinator results.
        let mut groups: Vec<DescribedGroup> = Vec::new();
        let mut invalid: Vec<DescribedGroup> = Vec::new();
        let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        for gid in &req.group_ids {
            if crate::handlers::group_describe_denied(authorizer, &image, ctx, gid) {
                groups.push(DescribedGroup::error_row(
                    gid,
                    codes::GROUP_AUTHORIZATION_FAILED,
                    None,
                ));
                continue;
            }
            // GroupCoordinatorService.shareGroupDescribe rejects an empty id
            // before it routes the group to a shard, and its row comes ahead of
            // the shard results.
            if gid.is_empty() {
                invalid.push(DescribedGroup::error_row("", codes::INVALID_GROUP_ID, None));
                continue;
            }
            if let Some(error_code) = crate::handlers::group_coordinator_error(broker, gid) {
                described.push(DescribedGroup::error_row(gid, error_code, None));
                continue;
            }
            let handle = match coordinator.group_type(gid) {
                Some(GroupType::Share) | None => coordinator.find_share(gid),
                Some(_) => None,
            };
            let Some(handle) = handle else {
                described.push(DescribedGroup::error_row(
                    gid,
                    codes::GROUP_ID_NOT_FOUND,
                    Some(crate::handlers::share_group_not_found_message(
                        coordinator,
                        gid,
                    )),
                ));
                continue;
            };

            let asked = ask(&handle.tx, |reply| ShareGroupActorMessage::Describe {
                reply,
            })
            .await;
            described.push(match asked {
                Ok(view) => view.into_described_group(&image),
                Err(AskError::Closed) => {
                    DescribedGroup::error_row(gid, codes::COORDINATOR_LOAD_IN_PROGRESS, None)
                }
                Err(AskError::Dropped) => {
                    DescribedGroup::error_row(gid, codes::UNKNOWN_SERVER_ERROR, None)
                }
            });
        }

        // KIP-430: the group operations bitfield, only on opt-in and only for rows
        // that came back clean.
        fill_group_authorized_operations(
            authorizer,
            &image,
            ctx,
            req.include_authorized_operations,
            &mut described,
        );
        groups.extend(invalid);
        groups.extend(described);

        // Clients may not see topics they cannot `Describe`: a group whose
        // assignment names one is replaced by Kafka's TOPIC_AUTHORIZATION_FAILED
        // row with no members.
        hide_undescribable_topics(authorizer, &image, ctx, &mut groups);

        Ok(response(groups))
    }
}

fn response(groups: Vec<DescribedGroup>) -> ShareGroupDescribeResponse {
    ShareGroupDescribeResponse {
        groups,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{AclOperation, ResourceType};
    use krabka_protocol::owned::share_group_describe_response;

    use super::*;
    use crate::{
        broker::Broker,
        test_support::{DenyAll, peer, principal, test_ctx},
    };

    fn request(group_ids: &[&str]) -> ShareGroupDescribeRequest {
        ShareGroupDescribeRequest {
            group_ids: group_ids.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        }
    }

    crate::test_support::context_helper!(client_id = "admin-client");

    #[tokio::test]
    async fn handle_denied_groups_preserve_group_ids_and_error_codes() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = crate::test_support::start_share_broker(
            Arc::new(DenyAll),
            crate::test_support::ShareBrokerSetup::default(),
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "alice");
        let resp = handle(&broker, request(&["g1", "g2"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: ["g1", "g2"]
                .map(|group_id| DescribedGroup {
                    group_id: group_id.into(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka's protocol gate answers every group with `UNSUPPORTED_VERSION`,
    /// before any ACL check.
    #[tokio::test]
    async fn handle_disabled_feature_wins_even_when_share_actor_exists() {
        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = crate::test_support::start_share_broker(
            Arc::new(DenyAll),
            crate::test_support::ShareBrokerSetup {
                support: crate::test_support::ShareApiSupport::Disabled,
            },
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        broker.group_coordinator.mark_share("g1");
        let _actor = broker.group_coordinator.get_or_create_share("g1");
        test_ctx!(ctx, "alice");
        let resp = handle(&broker, request(&["g1"]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![DescribedGroup {
                error_code: codes::UNSUPPORTED_VERSION,
                group_id: "g1".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    fn acl(resource_type: ResourceType, name: &str) -> krabka_metadata::MetadataRecord {
        crate::handlers::group_heartbeat_test_support::acl(
            resource_type,
            name,
            AclOperation::Describe,
        )
    }

    fn topic(
        name: &str,
        topic_id: uuid::Uuid,
        node: krabka_raft::NodeId,
    ) -> Vec<krabka_metadata::MetadataRecord> {
        crate::handlers::group_heartbeat_test_support::topic_with_partitions(
            crate::handlers::group_heartbeat_test_support::GroupTopicSetup {
                name,
                topic_id,
                partitions: crate::test_support::PartitionCount(1),
                node,
            },
        )
    }

    /// Seeds share group `group_id` with member `m1` assigned partition 0 of
    /// each of `topics`, plus partition 0 of a topic the image does not hold.
    async fn seed_group(broker: &Broker, group_id: &str, topics: &[uuid::Uuid]) {
        use crate::coordinator::unified::{
            ShareGroupSeed,
            share::persistence::{
                ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            },
        };
        let mut assigned: Vec<_> = topics
            .iter()
            .map(|t| {
                (
                    krabka_protocol::primitives::uuid::Uuid(*t.as_bytes()),
                    vec![0],
                )
            })
            .collect();
        assigned.push((krabka_protocol::primitives::uuid::Uuid([0xEE; 16]), vec![0]));
        let coordinator = &broker.group_coordinator;
        coordinator.mark_share(group_id);
        let handle = coordinator.get_or_create_share(group_id);
        handle
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                group_epoch: 2,
                target_epoch: 2,
                assignment_timestamp_ms: 0,
                members: [(
                    "m1".to_owned(),
                    ShareGroupMemberMetadataValue {
                        rack_id: None,
                        client_id: "client".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                    },
                )]
                .into(),
                current_per_member: [(
                    "m1".to_owned(),
                    ShareGroupCurrentMemberAssignmentValue {
                        member_epoch: 2,
                        previous_member_epoch: 1,
                        assigned_partitions: assigned,
                    },
                )]
                .into(),
                ..ShareGroupSeed::default()
            }))
            .await
            .expect("seed share group");
    }

    /// Kafka's `handleShareGroupDescribe` with an ACL authorizer: denied rows
    /// come first, a missing group names itself in the message, topic names
    /// come from the image (a topic it lacks is left out), the operations
    /// bitfield is filled on request, and a group whose assignment names a
    /// topic the caller cannot `Describe` is replaced by Kafka's
    /// `TOPIC_AUTHORIZATION_FAILED` row.
    #[tokio::test]
    async fn handle_matches_kafka_with_acls() {
        use krabka_protocol::owned::{
            common::share_group_describe_response::{
                assignment::Assignment, topic_partitions::TopicPartitions,
            },
            share_group_describe_response::Member,
        };

        let version = share_group_describe_response::MAX_VERSION;
        let (broker_handle, _dir) = crate::test_support::start_share_broker(
            Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            )),
            crate::test_support::ShareBrokerSetup::default(),
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let node = broker_handle.node_id();
        let visible = uuid::Uuid::from_u128(1);
        let hidden = uuid::Uuid::from_u128(2);
        let mut records = vec![
            acl(ResourceType::Group, "g"),
            acl(ResourceType::Group, "h"),
            acl(ResourceType::Group, "missing"),
            acl(ResourceType::Topic, "t"),
        ];
        records.extend(topic("t", visible, krabka_raft::NodeId(node)));
        records.extend(topic("secret", hidden, krabka_raft::NodeId(node)));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("ACLs and topics");
        seed_group(&broker, "g", &[visible]).await;
        seed_group(&broker, "h", &[visible, hidden]).await;
        request_identity!(
            (principal, peer, ctx),
            principal("alice"),
            client_id = "admin-client"
        );
        let req = ShareGroupDescribeRequest {
            group_ids: vec!["g".into(), "denied".into(), "h".into(), "missing".into()],
            include_authorized_operations: true,
            ..Default::default()
        };

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![
                DescribedGroup::error_row("denied", codes::GROUP_AUTHORIZATION_FAILED, None),
                DescribedGroup {
                    group_id: "g".into(),
                    group_state: "Stable".into(),
                    group_epoch: 2,
                    assignment_epoch: 2,
                    assignor_name: "simple".into(),
                    members: vec![Member {
                        member_id: "m1".into(),
                        member_epoch: 2,
                        client_id: "client".into(),
                        client_host: "/127.0.0.1".into(),
                        subscribed_topic_names: vec!["t".into()],
                        assignment: Assignment {
                            topic_partitions: vec![TopicPartitions {
                                topic_id: krabka_protocol::primitives::uuid::Uuid(
                                    *visible.as_bytes(),
                                ),
                                topic_name: "t".into(),
                                partitions: vec![0],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    }],
                    // DESCRIBE (8) is the only group operation granted.
                    authorized_operations: 1 << 8,
                    ..Default::default()
                },
                DescribedGroup::error_row(
                    "h",
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    Some("The group has described topic(s) that the client is not authorized to describe.".into()),
                ),
                DescribedGroup::error_row(
                    "missing",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group missing not found.".into()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// `INVALID_GROUP_ID` for an empty id, ahead of the shard rows, and
    /// Kafka's message for a group of another type.
    #[tokio::test]
    async fn handle_refuses_empty_and_foreign_group_ids() {
        let version = share_group_describe_response::MAX_VERSION;
        broker_fixture!((broker_handle, _dir, broker), share_allow_all);
        let _classic = broker.group_coordinator.get_or_create_classic("classic");
        test_ctx!(ctx, "alice");

        let resp = handle(&broker, request(&["classic", ""]), version, &ctx)
            .await
            .expect("handle");

        let expected = ShareGroupDescribeResponse {
            groups: vec![
                DescribedGroup::error_row("", codes::INVALID_GROUP_ID, None),
                DescribedGroup::error_row(
                    "classic",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group classic is not a share group.".into()),
                ),
            ],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
