//! `ConsumerGroupDescribe` (`api_key` 69).
//!
//! This handler returns one `DescribedGroup` per requested `group_id`. It
//! renders each KIP-848 consumer group from the actor's `Describe` view the
//! way Kafka's `ConsumerGroup.asDescribedGroup` does: the group state, the
//! group and assignment epochs, the preferred server assignor (or the default
//! one), and every member with its current and target assignment.
//!
//! Ordering and gating follow Kafka's `KafkaApis.handleConsumerGroupDescribe`:
//!
//! - The consumer-protocol gate is checked once, before any authorization.
//!   When the `consumer` rebalance protocol is not enabled, or `group.version`
//!   does not finalize the next-gen consumer-group RPCs, every requested group
//!   gets `UNSUPPORTED_VERSION` and no ACL is consulted.
//! - Past the gate, a group `Describe` denial is a per-row
//!   `GROUP_AUTHORIZATION_FAILED`, and those denied rows are placed first in
//!   the response, ahead of the coordinator results (which keep request
//!   order among themselves).
//! - KIP-430: when the request sets `include_authorized_operations`, every
//!   row with `error_code == NONE` carries the bitfield of group operations
//!   the principal holds. Every other row keeps the wire-default `i32::MIN`
//!   "not present" sentinel.
//! - Clients may not see topics they cannot `Describe`: a group whose
//!   assignment or target assignment names one is replaced by a
//!   `TOPIC_AUTHORIZATION_FAILED` row with no members.

use std::collections::{HashMap, HashSet};

use krabka_metadata::{AclOperation, MetadataImage};
use krabka_protocol::{
    owned::{
        common::consumer_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        },
        consumer_group_describe_request::ConsumerGroupDescribeRequest,
        consumer_group_describe_response::{ConsumerGroupDescribeResponse, DescribedGroup, Member},
    },
    primitives::uuid::Uuid,
};

use crate::{
    authorizer::Authorizer,
    codes,
    coordinator::unified::actor::{DescribeMember, DescribeView, GroupActorMessage},
    handlers::{
        authorized_operations::{DescribedGroupRow, fill_group_authorized_operations},
        group_version_disabled,
    },
    task_util::{AskError, ask},
};

/// `member_type` of a member that speaks the classic protocol inside a
/// consumer group.
const MEMBER_TYPE_CLASSIC: i8 = 0;
/// `member_type` of a member that speaks the consumer protocol.
const MEMBER_TYPE_CONSUMER: i8 = 1;

context_handler! {
    ConsumerGroupDescribeRequest => ConsumerGroupDescribeResponse,
    (broker, req, _version, ctx),
    {
        let coordinator = broker.group_coordinator.clone();
        let image = broker.controller.current_image();

        // Kafka's `isConsumerGroupProtocolEnabled` gate, checked before any
        // authorization: the `consumer` rebalance protocol must be enabled and
        // `group.version` must be finalized at 1 or above.
        if !coordinator.config.next_gen_enabled() || group_version_disabled(&image) {
            let described = req
                .group_ids
                .iter()
                .map(|group_id| DescribedGroup::error_row(group_id, codes::UNSUPPORTED_VERSION, None))
                .collect();
            return Ok(response(described));
        }

        let default_assignor = coordinator
            .config
            .assignors
            .first()
            .map(|a| a.name())
            .unwrap_or_default();
        // Kafka places every GROUP_AUTHORIZATION_FAILED row first, ahead of the
        // coordinator results, which keep request order among themselves.
        let mut denied: Vec<DescribedGroup> = Vec::new();
        let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
        for group_id in &req.group_ids {
            if crate::handlers::group_describe_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                group_id,
            ) {
                denied.push(DescribedGroup::error_row(
                    group_id,
                    codes::GROUP_AUTHORIZATION_FAILED,
                    None,
                ));
                continue;
            }
            // GroupCoordinatorService.consumerGroupDescribe rejects an empty id
            // before it routes the group to a shard.
            if group_id.is_empty() {
                described.push(DescribedGroup::error_row("", codes::INVALID_GROUP_ID, None));
                continue;
            }
            if let Some(error_code) = crate::handlers::group_coordinator_error(broker, group_id) {
                described.push(DescribedGroup::error_row(group_id, error_code, None));
                continue;
            }
            // The `Describe` arm dispatches on the actor's LIVE `group.kind`: it
            // replies ONLY for a consumer-kind group and drops the sender
            // otherwise, so an upgraded group is reachable and a classic group
            // is not.
            // A share or streams group is another type's group, which Kafka's
            // `consumerGroup` lookup refuses whether or not an actor holds its
            // offsets.
            if coordinator.is_share_or_streams_group(group_id) {
                described.push(not_found_row(group_id, "is not a consumer group"));
                continue;
            }
            let Some(handle) = coordinator.find(group_id) else {
                described.push(not_found_row(group_id, "not found"));
                continue;
            };
            described.push(
                match ask(&handle.tx, |reply| GroupActorMessage::Describe { reply }).await {
                    Ok(view) => described_group(view, default_assignor, &image),
                    Err(AskError::Closed) => {
                        DescribedGroup::error_row(group_id, codes::COORDINATOR_LOAD_IN_PROGRESS, None)
                    }
                    Err(AskError::Dropped) => not_found_row(group_id, "is not a consumer group"),
                },
            );
        }

        // KIP-430: bitfield of group operations the principal is authorized for,
        // filled only on opt-in and only for rows that came back clean. Denied
        // and errored rows keep the wire-default `i32::MIN` sentinel.
        fill_group_authorized_operations(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            req.include_authorized_operations,
            &mut described,
        );

        denied.extend(described);
        hide_undescribable_topics(broker.config.authorizer.as_ref(), &image, ctx, &mut denied);
        Ok(response(denied))
    }
}

/// The message of the row Kafka substitutes for a group whose assignment names
/// a topic the caller cannot `Describe`.
const UNAUTHORIZED_TOPICS_MESSAGE: &str =
    "The group has described topic(s) that the client is not authorized to describe.";

/// Topic names exposed by a group describe response, including target assignments.
pub(super) trait DescribedTopics: DescribedGroupRow {
    fn topics(&self) -> impl Iterator<Item = &str>;
}

impl DescribedTopics for DescribedGroup {
    fn topics(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .flat_map(|member| {
                member
                    .assignment
                    .topic_partitions
                    .iter()
                    .chain(&member.target_assignment.topic_partitions)
            })
            .map(|topic| topic.topic_name.as_str())
    }
}

/// Kafka's `handleConsumerGroupDescribe`: "Clients are not allowed to see
/// topics that are not authorized for Describe". A group with a topic in the
/// assignment or target assignment of any member that the caller cannot
/// `Describe` is replaced by a `TOPIC_AUTHORIZATION_FAILED` row with no members,
/// so the topic names, ids and partition ownership stay hidden.
pub(super) fn hide_undescribable_topics<G: DescribedTopics>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    groups: &mut [G],
) {
    let named: HashSet<&str> = groups.iter().flat_map(DescribedTopics::topics).collect();
    let undescribable =
        crate::handlers::denied_topics(authorizer, image, ctx, AclOperation::Describe, named);
    if undescribable.is_empty() {
        return;
    }
    for group in groups {
        if group.topics().any(|topic| undescribable.contains(topic)) {
            *group = G::error_row(
                group.group_id(),
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some(UNAUTHORIZED_TOPICS_MESSAGE.to_owned()),
            );
        }
    }
}

/// `GROUP_ID_NOT_FOUND` with the message of Kafka's `consumerGroup` lookup,
/// `Group <id> not found.` or `Group <id> is not a consumer group.`.
fn not_found_row(group_id: &str, reason: &str) -> DescribedGroup {
    DescribedGroup::error_row(
        group_id,
        codes::GROUP_ID_NOT_FOUND,
        Some(format!("Group {group_id} {reason}.")),
    )
}

/// Renders a consumer group as `ConsumerGroup.asDescribedGroup` does. Members
/// are sorted by id, topics by name and partitions ascending, so the answer
/// does not depend on map order.
fn described_group(
    view: DescribeView,
    default_assignor: &str,
    image: &MetadataImage,
) -> DescribedGroup {
    let mut members: Vec<Member> = view
        .members
        .into_iter()
        .map(|m| described_member(m, image))
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.group_state.into(),
        group_epoch: view.group_epoch,
        assignment_epoch: view.assignment_epoch,
        assignor_name: view
            .preferred_server_assignor
            .unwrap_or_else(|| default_assignor.to_string()),
        members,
        ..Default::default()
    }
}

/// `ConsumerGroupMember.asConsumerGroupDescribeMember`: the current assignment
/// is the assigned partitions plus those pending revocation, which the member
/// still owns until it confirms the revocation.
fn described_member(m: DescribeMember, image: &MetadataImage) -> Member {
    let mut owned = m.assigned_partitions;
    for (topic_id, partitions) in m.partitions_pending_revocation {
        owned.entry(topic_id).or_default().extend(partitions);
    }
    Member {
        member_id: m.member_id,
        instance_id: m.instance_id,
        rack_id: m.rack_id,
        member_epoch: m.member_epoch,
        client_id: m.client_id,
        client_host: m.client_host,
        subscribed_topic_names: m.subscribed_topic_names,
        subscribed_topic_regex: m.subscribed_topic_regex,
        assignment: assignment(owned, image),
        target_assignment: assignment(m.target_partitions, image),
        member_type: if m.is_classic {
            MEMBER_TYPE_CLASSIC
        } else {
            MEMBER_TYPE_CONSUMER
        },
        ..Default::default()
    }
}

/// Names each topic from the metadata image. Kafka drops a topic the image no
/// longer has.
fn assignment(partitions: HashMap<Uuid, Vec<i32>>, image: &MetadataImage) -> Assignment {
    let mut topic_partitions: Vec<TopicPartitions> = partitions
        .into_iter()
        .filter_map(|(topic_id, mut partitions)| {
            let topic_name = image.topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))?;
            partitions.sort_unstable();
            Some(TopicPartitions {
                topic_id,
                topic_name: topic_name.to_string(),
                partitions,
                ..Default::default()
            })
        })
        .collect();
    topic_partitions.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
    Assignment {
        topic_partitions,
        ..Default::default()
    }
}

fn response(groups: Vec<DescribedGroup>) -> ConsumerGroupDescribeResponse {
    ConsumerGroupDescribeResponse {
        groups,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataRecord;

    use super::*;
    use crate::{
        broker::Broker,
        handlers::{
            authorized_operations::authorized_operations_bits,
            group_heartbeat_test_support::{
                acl_authorizer, image_with_group_version, set_group_version,
            },
        },
    };

    const VERSION: i16 = krabka_protocol::owned::consumer_group_describe_request::MAX_VERSION;

    fn request(group_ids: Vec<&str>) -> ConsumerGroupDescribeRequest {
        ConsumerGroupDescribeRequest {
            group_ids: group_ids.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    async fn describe_groups(
        broker: &Broker,
        request: ConsumerGroupDescribeRequest,
        context: &crate::handlers::RequestContext<'_>,
    ) -> ConsumerGroupDescribeResponse {
        handle(broker, request, VERSION, context)
            .await
            .expect("ConsumerGroupDescribe handler")
    }

    fn group_errors(response: &ConsumerGroupDescribeResponse) -> Vec<(&str, i16)> {
        response
            .groups
            .iter()
            .map(|group| (group.group_id.as_str(), group.error_code))
            .collect()
    }

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        // (finalized group.version level; None = fresh image) → disabled?
        let cases = [(None, true), (Some(1), false), (Some(0), true)];
        for (level, want_disabled) in cases {
            let image = match level {
                None => MetadataImage::new(uuid::Uuid::nil()),
                Some(level) => image_with_group_version(
                    crate::handlers::group_heartbeat_test_support::GroupFeatureLevel(level),
                ),
            };
            assert!(
                group_version_disabled(&image) == want_disabled,
                "level {level:?}"
            );
        }
    }

    const ORDERS: Uuid = Uuid([1; 16]);
    const PAYMENTS: Uuid = Uuid([2; 16]);
    /// A topic id the image does not hold, as after a topic deletion.
    const DELETED: Uuid = Uuid([3; 16]);

    fn image_with_topics() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (name, id) in [("orders", ORDERS), ("payments", PAYMENTS)] {
            image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: name.into(),
                topic_id: uuid::Uuid::from_bytes(id.0),
                partitions: 4,
                replication_factor: 1,
            }));
        }
        image
    }

    fn partitions(entries: &[(Uuid, &[i32])]) -> HashMap<Uuid, Vec<i32>> {
        entries.iter().map(|(id, p)| (*id, p.to_vec())).collect()
    }

    /// The same literal member identity appears in the input view and expected wire member.
    /// Their assignments remain separate, including the independently expected wire mapping.
    macro_rules! member_identity_fixture {
        ($name:ident -> $type:ident, $encoding:ident, {$($extra:tt)*}) => {
            fn $name(member_id: &str) -> $type {
                member_identity_fixture!(@$encoding $type {
                    member_id: member_id.into(),
                    instance_id: None,
                    rack_id: None,
                    member_epoch: 5,
                    client_id: "client".into(),
                    client_host: "/10.0.0.1".into(),
                    subscribed_topic_names: vec!["orders".into(), "payments".into()],
                    subscribed_topic_regex: None,
                    $($extra)*
                })
            }
        };
        (@plain $type:ident $fields:tt) => { $type $fields };
        (@tagged $type:ident $fields:tt) => { tagged_wire!($type $fields) };
    }

    member_identity_fixture!(view_member -> DescribeMember, plain, {
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        target_partitions: HashMap::new(),
        is_classic: false,
    });

    fn view(group_state: &'static str, members: Vec<DescribeMember>) -> DescribeView {
        DescribeView {
            group_id: "cg".into(),
            group_epoch: 5,
            assignment_epoch: 5,
            group_state,
            preferred_server_assignor: None,
            members,
        }
    }

    fn topic(topic_id: Uuid, topic_name: &str, partitions: &[i32]) -> TopicPartitions {
        tagged_wire!(TopicPartitions {
            topic_id,
            topic_name: topic_name.into(),
            partitions: partitions.to_vec(),
        })
    }

    fn assigned(topics: Vec<TopicPartitions>) -> Assignment {
        tagged_wire!(Assignment {
            topic_partitions: topics,
        })
    }

    member_identity_fixture!(wire_member -> Member, tagged, {
        assignment: assigned(vec![]),
        target_assignment: assigned(vec![]),
        member_type: MEMBER_TYPE_CONSUMER,
    });

    fn wire_group(group_state: &str, assignor_name: &str, members: Vec<Member>) -> DescribedGroup {
        tagged_wire!(DescribedGroup {
            error_code: codes::NONE,
            error_message: None,
            group_id: "cg".into(),
            group_state: group_state.into(),
            group_epoch: 5,
            assignment_epoch: 5,
            assignor_name: assignor_name.into(),
            members,
            authorized_operations: i32::MIN,
        })
    }

    /// `ConsumerGroup.asDescribedGroup`: the view's state and epochs, the
    /// preferred assignor or the default, and each member with its owned
    /// partitions (assigned plus pending revocation) and its target, named
    /// from the image. A topic the image lacks is dropped.
    #[test]
    fn a_consumer_group_is_described_whole() {
        let stable = {
            let mut native = view_member("m-a");
            native.instance_id = Some("instance-a".into());
            native.rack_id = Some("rack-1".into());
            native.assigned_partitions = partitions(&[(PAYMENTS, &[1, 0])]);
            native.target_partitions = partitions(&[(PAYMENTS, &[0, 1])]);
            let mut classic = view_member("m-b");
            classic.is_classic = true;
            classic.subscribed_topic_regex = Some("ord.*".into());
            classic.assigned_partitions = partitions(&[(ORDERS, &[0]), (DELETED, &[0])]);
            classic.target_partitions = partitions(&[(ORDERS, &[0])]);
            let mut v = view("Stable", vec![classic, native]);
            v.preferred_server_assignor = Some("range".into());
            v
        };
        let stable_expected = {
            let mut native = wire_member("m-a");
            native.instance_id = Some("instance-a".into());
            native.rack_id = Some("rack-1".into());
            native.assignment = assigned(vec![topic(PAYMENTS, "payments", &[0, 1])]);
            native.target_assignment = assigned(vec![topic(PAYMENTS, "payments", &[0, 1])]);
            let mut classic = wire_member("m-b");
            classic.member_type = MEMBER_TYPE_CLASSIC;
            classic.subscribed_topic_regex = Some("ord.*".into());
            classic.assignment = assigned(vec![topic(ORDERS, "orders", &[0])]);
            classic.target_assignment = assigned(vec![topic(ORDERS, "orders", &[0])]);
            wire_group("Stable", "range", vec![native, classic])
        };
        let reconciling = {
            let mut m = view_member("m-a");
            m.member_epoch = 4;
            m.assigned_partitions = partitions(&[(ORDERS, &[2])]);
            m.partitions_pending_revocation = partitions(&[(ORDERS, &[1]), (PAYMENTS, &[3])]);
            m.target_partitions = partitions(&[(ORDERS, &[2])]);
            view("Reconciling", vec![m])
        };
        let reconciling_expected = {
            let mut m = wire_member("m-a");
            m.member_epoch = 4;
            m.assignment = assigned(vec![
                topic(ORDERS, "orders", &[1, 2]),
                topic(PAYMENTS, "payments", &[3]),
            ]);
            m.target_assignment = assigned(vec![topic(ORDERS, "orders", &[2])]);
            wire_group("Reconciling", "uniform", vec![m])
        };
        // (view, expected row)
        let rows = [
            (
                view("Empty", vec![]),
                wire_group("Empty", "uniform", vec![]),
            ),
            (stable, stable_expected),
            (reconciling, reconciling_expected),
        ];
        let image = image_with_topics();
        for (v, expected) in rows {
            let state = v.group_state;
            assert!(described_group(v, "uniform", &image) == expected, "{state}");
        }
    }

    #[test]
    fn response_preserves_group_rows() {
        let first = DescribedGroup::error_row("a", codes::GROUP_ID_NOT_FOUND, None);
        let second = DescribedGroup::error_row("b", codes::UNSUPPORTED_VERSION, None);

        let resp = response(vec![first, second]);

        let expected = unthrottled_wire!(ConsumerGroupDescribeResponse {
            groups: vec![
                tagged_wire!(DescribedGroup {
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    error_message: None,
                    group_id: "a".to_string(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: vec![],
                    authorized_operations: -2_147_483_648,
                }),
                tagged_wire!(DescribedGroup {
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    group_id: "b".to_string(),
                    group_state: String::new(),
                    group_epoch: 0,
                    assignment_epoch: 0,
                    assignor_name: String::new(),
                    members: vec![],
                    authorized_operations: -2_147_483_648,
                }),
            ],
        });
        assert!(resp == expected, "{resp:?}");
    }

    /// Kafka's `handleConsumerGroupDescribe` checks `Describe` on every topic
    /// in the assignment and the target assignment of every member: a group
    /// that names one the caller cannot describe becomes a
    /// `TOPIC_AUTHORIZATION_FAILED` row with no members, and the other groups
    /// stay whole.
    #[test]
    fn a_group_naming_an_undescribable_topic_is_replaced_by_an_error_row() {
        let mut image = image_with_topics();
        image.apply(&MetadataRecord::V1AccessControlEntry(
            crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
                operation: AclOperation::Describe,
                ..Default::default()
            }),
        ));
        let authorizer = acl_authorizer();
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("alice"),
            client_id = "alice-client",
            address = crate::test_support::peer()
        );

        let orders = || topic(ORDERS, "orders", &[0]);
        let payments = || topic(PAYMENTS, "payments", &[0]);
        let member = |assignment: Vec<TopicPartitions>, target: Vec<TopicPartitions>| {
            let mut member = wire_member("m");
            member.assignment = assigned(assignment);
            member.target_assignment = assigned(target);
            member
        };
        let group = |group_id: &str, members: Vec<Member>| DescribedGroup {
            group_id: group_id.into(),
            ..wire_group("Stable", "uniform", members)
        };
        let hidden = |group_id: &str| DescribedGroup {
            group_id: group_id.into(),
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some(UNAUTHORIZED_TOPICS_MESSAGE.into()),
            ..Default::default()
        };
        let mut groups = vec![
            group("describable", vec![member(vec![orders()], vec![orders()])]),
            group("assigned", vec![member(vec![payments()], vec![])]),
            group("targeted", vec![member(vec![orders()], vec![payments()])]),
            group(
                "second-member",
                vec![
                    member(vec![orders()], vec![]),
                    member(vec![payments()], vec![]),
                ],
            ),
            group("empty", vec![]),
        ];
        let expected = vec![
            groups[0].clone(),
            hidden("assigned"),
            hidden("targeted"),
            hidden("second-member"),
            groups[4].clone(),
        ];

        hide_undescribable_topics(&authorizer, &image, &ctx, &mut groups);

        assert!(groups == expected, "{groups:?}");
    }

    /// A group Kafka's `consumerGroup` lookup rejects is `GROUP_ID_NOT_FOUND`
    /// with the lookup's message, and an empty id is `INVALID_GROUP_ID`.
    #[tokio::test]
    async fn handle_answers_a_group_that_is_not_a_consumer_group() {
        let (broker_handle, _dir) = crate::test_support::start_group_broker_with(|_| {}).await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker
            .group_coordinator
            .get_or_create_classic("classic-group");
        broker.group_coordinator.mark_share("share-group");
        broker.group_coordinator.mark_streams("streams-group");
        let _share_actor = broker.group_coordinator.get_or_create_share("share-actor");
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("admin"),
            client_id = "admin-client",
            address = crate::test_support::peer()
        );
        let row = |group_id: &str, error_code, message: Option<&str>| {
            tagged_wire!(DescribedGroup {
                error_code,
                error_message: message.map(str::to_string),
                group_id: group_id.to_string(),
                group_state: String::new(),
                group_epoch: 0,
                assignment_epoch: 0,
                assignor_name: String::new(),
                members: vec![],
                authorized_operations: i32::MIN,
            })
        };
        // (requested id, expected row)
        let rows = [
            (
                "missing-group",
                row(
                    "missing-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group missing-group not found."),
                ),
            ),
            (
                "classic-group",
                row(
                    "classic-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group classic-group is not a consumer group."),
                ),
            ),
            (
                "share-group",
                row(
                    "share-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group share-group is not a consumer group."),
                ),
            ),
            (
                "share-actor",
                row(
                    "share-actor",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group share-actor is not a consumer group."),
                ),
            ),
            (
                "streams-group",
                row(
                    "streams-group",
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group streams-group is not a consumer group."),
                ),
            ),
            ("", row("", codes::INVALID_GROUP_ID, None)),
        ];
        for (group_id, expected) in rows {
            let resp = handle(&broker, request(vec![group_id]), VERSION, &ctx)
                .await
                .expect("ConsumerGroupDescribe handler");

            assert!(
                resp == unthrottled_wire!(ConsumerGroupDescribeResponse {
                    groups: vec![expected],
                }),
                "{group_id:?}"
            );
        }

        broker_handle.shutdown().await;
    }

    /// Request `include_authorized_operations` on a request built with
    /// [`request`], which does not set it — [`request_with_ops`] below sets
    /// it explicitly.
    fn request_with_ops(
        group_ids: Vec<&str>,
        include_authorized_operations: bool,
    ) -> ConsumerGroupDescribeRequest {
        ConsumerGroupDescribeRequest {
            include_authorized_operations,
            ..request(group_ids)
        }
    }

    /// The `group.version` protocol gate — `UNSUPPORTED_VERSION` when the
    /// next-gen consumer-group RPCs are not finalized — runs BEFORE the
    /// group ACL check, matching Kafka's `handleConsumerGroupDescribe`. A
    /// principal denied `Describe` on the group still gets
    /// `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`, when the
    /// protocol itself is unavailable, and every requested group gets it —
    /// none are individually authorization-checked.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl_for_every_row() {
        let authorizer = acl_authorizer();
        broker_fixture!(
            (broker_handle, _dir, broker),
            group_controller_peer(authorizer)
        );
        set_group_version(
            &broker,
            crate::handlers::group_heartbeat_test_support::GroupFeatureLevel(0),
        )
        .await;
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("nobody"),
            client_id = "consumer-client",
            address = crate::test_support::peer()
        );
        let req = request(vec!["denied-group", "also-denied"]);

        let resp = describe_groups(&broker, req, &ctx).await;

        assert!(
            group_errors(&resp)
                == vec![
                    ("denied-group", codes::UNSUPPORTED_VERSION),
                    ("also-denied", codes::UNSUPPORTED_VERSION),
                ],
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// Kafka puts every `GROUP_AUTHORIZATION_FAILED` row first, ahead of the
    /// coordinator results, regardless of the order the client requested the
    /// groups in.
    #[tokio::test]
    async fn handle_orders_denied_rows_before_allowed_rows() {
        let authorizer = acl_authorizer();
        broker_fixture!(
            (broker_handle, _dir, broker),
            group_controller_peer(authorizer)
        );
        // Grant "alice" Describe on "allowed" only; "denied" has no matching
        // ACL and stays denied under SimpleAclAuthorizer's default-deny.
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(
                crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
                    resource_type: krabka_metadata::ResourceType::Group,
                    resource_name: "allowed",
                    operation: krabka_metadata::AclOperation::Describe,
                    ..Default::default()
                }),
            )])
            .await
            .expect("grant alice Describe on allowed");
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("alice"),
            client_id = "alice-client",
            address = crate::test_support::peer()
        );
        let req = request(vec!["allowed", "denied"]);

        let resp = describe_groups(&broker, req, &ctx).await;

        // Requested in order [allowed, denied]; the denied row comes first
        // in the response, ahead of the (unknown, hence GROUP_ID_NOT_FOUND)
        // allowed row.
        assert!(
            group_errors(&resp)
                == vec![
                    ("denied", codes::GROUP_AUTHORIZATION_FAILED),
                    ("allowed", codes::GROUP_ID_NOT_FOUND),
                ],
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// KIP-430: with the flag set, a row that comes back clean carries the
    /// bitfield of group operations the principal holds; the flag unset (or
    /// an errored row) keeps the wire-default `i32::MIN` sentinel.
    #[tokio::test]
    async fn handle_fills_authorized_operations_only_on_opt_in_for_clean_rows() {
        let authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
        let (broker_handle, _dir) = crate::test_support::start_group_broker_no_audit(
            std::sync::Arc::clone(&authorizer) as _,
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker
            .group_coordinator
            .get_or_create_consumer("live-group");
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("admin"),
            client_id = "admin-client",
            address = crate::test_support::peer()
        );

        let expected_bits = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &ctx,
            krabka_metadata::ResourceType::Group,
            "live-group",
        );
        assert!(expected_bits != i32::MIN);
        for (include, bits) in [(false, i32::MIN), (true, expected_bits)] {
            let resp = handle(
                &broker,
                request_with_ops(vec!["live-group"], include),
                VERSION,
                &ctx,
            )
            .await
            .expect("ConsumerGroupDescribe handler");
            assert!(
                resp.groups
                    == vec![DescribedGroup {
                        group_id: "live-group".into(),
                        group_state: "Empty".into(),
                        group_epoch: 1,
                        assignment_epoch: 1,
                        assignor_name: "uniform".into(),
                        authorized_operations: bits,
                        ..Default::default()
                    }],
                "include = {include}: {resp:?}"
            );
        }

        broker_handle.shutdown().await;
    }
}
