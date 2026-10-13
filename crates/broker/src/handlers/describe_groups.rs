//! `DescribeGroups` (`api_key=15`). The response holds one entry per requested
//! `group_id`.
//!
//! The rows follow Kafka's `GroupMetadataManager.describeGroups`, which
//! describes only classic groups:
//!
//! - A `Stable` classic group reports its selected protocol name
//!   (`protocol_data`), and each member carries its `JoinGroup` metadata for
//!   that protocol (`member_metadata`). Any other state leaves both empty, as
//!   `ClassicGroupMember.describeNoMetadata` does.
//! - Every member carries its `group_instance_id` and its current assignment
//!   bytes, in every state.
//! - `protocol_type` is the stored type, or `""` for a typeless group.
//! - An unknown group, and a KIP-848 consumer, streams or share group, is
//!   answered in state `Dead`. From v6 the row carries `GROUP_ID_NOT_FOUND`
//!   and Kafka's message; below v6 its error is `NONE`, which older admin
//!   clients read as "the group does not exist".
//!
//! As in Kafka's `KafkaApis.handleDescribeGroupsRequest`, every group the
//! principal may not `Describe` is answered first with
//! `GROUP_AUTHORIZATION_FAILED`, ahead of the coordinator results for the
//! allowed groups.
//!
//! KIP-430: when the request sets `include_authorized_operations`, each
//! coordinator row whose error is `NONE` carries a bitfield of the group
//! operations that the principal may perform. Every other row keeps the
//! `i32::MIN` "not present" sentinel.

#[cfg(test)]
use crate::authorizer::SimpleAclAuthorizer;
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{
            GroupType,
            actor::{ClassicView, GroupActorMessage},
            classic_state::GroupState,
        },
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, authorized_operations::fill_group_authorized_operations,
            group_coordinator_error, group_describe_denied,
        },
        task_util::{AskError, ask},
    },
    bytes::Bytes,
    krabka_protocol::owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup, DescribedGroupMember},
    },
};

/// The first `DescribeGroups` version whose unknown-group row carries
/// `GROUP_ID_NOT_FOUND`. Older versions answer `NONE` in state `Dead`.
const GROUP_ID_NOT_FOUND_MIN_VERSION: i16 = 6;

/// The first `DescribeGroups` version with `authorized_operations`.
const AUTHORIZED_OPERATIONS_MIN_VERSION: i16 = 3;

pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeGroupsRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DescribeGroupsResponse, BrokerError> {
    let image = broker.controller.current_image();

    // Kafka answers every GROUP_AUTHORIZATION_FAILED row first, then the
    // coordinator results for the allowed groups in request order.
    let mut denied: Vec<DescribedGroup> = Vec::new();
    let mut groups: Vec<DescribedGroup> = Vec::with_capacity(req.groups.len());
    for gid in req.groups {
        if group_describe_denied(broker.config.authorizer.as_ref(), &image, ctx, gid.as_str()) {
            denied.push(DescribedGroup {
                group_id: gid,
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            });
            continue;
        }
        if let Some(error_code) = group_coordinator_error(broker, &gid) {
            groups.push(DescribedGroup {
                group_id: gid,
                error_code,
                ..Default::default()
            });
            continue;
        }
        groups.push(describe_one(broker, gid, version).await);
    }

    // KIP-430: Kafka fills the bitfield for every coordinator row whose error
    // is NONE, a below-v6 `Dead` row included.
    fill_group_authorized_operations(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        version >= AUTHORIZED_OPERATIONS_MIN_VERSION && req.include_authorized_operations,
        &mut groups,
    );

    denied.extend(groups);
    Ok(DescribeGroupsResponse {
        groups: denied,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

/// Describes one allowed group the way `GroupMetadataManager.describeGroups`
/// does: a live classic group is projected, and anything else is a `Dead` row.
async fn describe_one(broker: &Broker, group_id: String, version: i16) -> DescribedGroup {
    let coordinator = &broker.group_coordinator;
    // A streams or share group may keep a drained classic actor as its offset
    // home; Kafka's `classicGroup` lookup still rejects it by type.
    if matches!(
        coordinator.group_type(&group_id),
        Some(GroupType::Streams | GroupType::Share)
    ) {
        let message = format!("Group {group_id} is not a classic group.");
        return dead_row(group_id, version, message);
    }
    let Some(handle) = coordinator.find(&group_id) else {
        let message = format!("Group {group_id} not found.");
        return dead_row(group_id, version, message);
    };
    // `ClassicInspect` replies only while the live group is classic; a
    // KIP-848 consumer group drops the sender.
    match ask(&handle.tx, |reply| GroupActorMessage::ClassicInspect {
        reply,
    })
    .await
    {
        Ok(view) => described_classic(view),
        Err(AskError::Closed) => {
            let message = format!("Group {group_id} not found.");
            dead_row(group_id, version, message)
        }
        Err(AskError::Dropped) => {
            let message = format!("Group {group_id} is not a classic group.");
            dead_row(group_id, version, message)
        }
    }
}

/// The row for a group Kafka's `classicGroup` lookup does not find: state
/// `Dead`, and from v6 `GROUP_ID_NOT_FOUND` with the lookup's message.
fn dead_row(group_id: String, version: i16, message: String) -> DescribedGroup {
    let not_found = version >= GROUP_ID_NOT_FOUND_MIN_VERSION;
    DescribedGroup {
        group_id,
        group_state: "Dead".into(),
        error_code: if not_found {
            codes::GROUP_ID_NOT_FOUND
        } else {
            codes::NONE
        },
        error_message: not_found.then_some(message),
        ..Default::default()
    }
}

/// Projects a live classic group. Only a `Stable` group reports its protocol
/// name and the members' metadata for it.
fn described_classic(view: ClassicView) -> DescribedGroup {
    let stable = view.state == GroupState::Stable;
    let members = view
        .members
        .into_iter()
        .map(|m| DescribedGroupMember {
            member_id: m.member_id,
            group_instance_id: m.group_instance_id,
            client_id: m.client_id,
            client_host: m.host,
            member_metadata: if stable {
                m.protocol_metadata
            } else {
                Bytes::new()
            },
            member_assignment: m.assignment.unwrap_or_default(),
            ..Default::default()
        })
        .collect();
    DescribedGroup {
        group_id: view.group_id,
        group_state: view.state.as_str().into(),
        protocol_type: view.protocol_type.unwrap_or_default(),
        protocol_data: if stable {
            view.protocol_name.unwrap_or_default()
        } else {
            String::new()
        },
        error_code: codes::NONE,
        members,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            coordinator::unified::actor::ClassicMemberView,
            handlers::{
                acl_wire::operation_to_wire, authorized_operations::authorized_operations_bits,
            },
            test_support::{
                DenyAll, KafkaErrorCode, allow_acl, peer, principal, request_context,
                start_group_broker_no_audit, test_ctx,
            },
        },
        assert2::assert,
        krabka_metadata::{AclOperation, ResourceType},
        std::sync::Arc,
    };

    const VERSION: krabka_ids::ApiVersion =
        krabka_ids::ApiVersion(krabka_protocol::owned::describe_groups_response::MAX_VERSION);

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    #[derive(Clone, Copy, Default)]
    enum OperationsDisclosure {
        #[default]
        Omit,
        Include,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct GroupDescribeSetup<'a> {
        #[default(&["classic-a"])]
        groups: &'a [&'a str],
        operations: OperationsDisclosure,
    }

    #[bon::builder]
    fn request(
        #[builder(default = & ["classic-a"])] groups: &'_ [&'_ str],
        #[builder(default)] operations: OperationsDisclosure,
    ) -> DescribeGroupsRequest {
        let setup = GroupDescribeSetup { groups, operations };
        DescribeGroupsRequest {
            groups: setup.groups.iter().map(|g| (*g).to_string()).collect(),
            include_authorized_operations: matches!(
                setup.operations,
                OperationsDisclosure::Include
            ),
            ..Default::default()
        }
    }

    /// A `DescribedGroup` carrying only an error: every projection field keeps
    /// its wire default, `authorized_operations` included.
    fn error_row(group_id: &str, error_code: KafkaErrorCode) -> DescribedGroup {
        krabka_macros::tagged_wire!(DescribedGroup {
            error_code: error_code.0,
            error_message: None,
            group_id: group_id.to_string(),
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: vec![],
            authorized_operations: i32::MIN,
        })
    }

    async fn drive(
        broker: &Broker,
        version: krabka_ids::ApiVersion,
        req: &DescribeGroupsRequest,
    ) -> DescribeGroupsResponse {
        test_ctx!(ctx, "admin");
        handle(broker, req.clone(), version.0, &ctx)
            .await
            .expect("handle")
    }

    /// A Deny on `Describe Group` answers the row, not the request: each named
    /// group gets its own `GROUP_AUTHORIZATION_FAILED` and the coordinator is
    /// never consulted.
    #[tokio::test]
    async fn a_denied_group_is_refused_per_row() {
        let (broker_handle, _dir) = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(
            &broker,
            VERSION,
            &request().groups(&["group-a", "group-b"]).call(),
        )
        .await;

        assert!(
            resp == krabka_macros::unthrottled_wire!(DescribeGroupsResponse {
                groups: vec![
                    error_row(
                        "group-a",
                        crate::test_support::KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED)
                    ),
                    error_row(
                        "group-b",
                        crate::test_support::KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED)
                    ),
                ],
            })
        );
        broker_handle.shutdown().await;
    }

    #[derive(derive_more::From, derive_more::Into)]
    /// A `Dead` row as `GroupMetadataManager.describeGroups` answers a group
    /// its `classicGroup` lookup rejects.
    #[derive(Clone, Copy)]
    struct AuthorizedGroupOperations(i32);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct DeadGroupSetup<'a> {
        #[default("never-seen")]
        group: &'a str,
        error: KafkaErrorCode,
        message: Option<&'a str>,
        #[default(AuthorizedGroupOperations(i32::MIN))]
        operations: AuthorizedGroupOperations,
    }

    #[bon::builder]
    fn dead(
        #[builder(default = "never-seen")] group: &'_ str,
        #[builder(into, default)] error: KafkaErrorCode,
        message: Option<&'_ str>,
        #[builder(into, default = AuthorizedGroupOperations (i32 :: MIN))]
        operations: AuthorizedGroupOperations,
    ) -> DescribedGroup {
        let setup = DeadGroupSetup {
            group,
            error,
            message,
            operations,
        };
        DescribedGroup {
            group_state: "Dead".into(),
            error_message: setup.message.map(str::to_string),
            authorized_operations: setup.operations.0,
            ..error_row(setup.group, setup.error)
        }
    }

    /// An unknown group and a KIP-848 consumer group are both `Dead`: from v6
    /// with `GROUP_ID_NOT_FOUND` and Kafka's message, below v6 with `NONE`.
    /// A below-v6 `NONE` row still gets the KIP-430 bitfield, because Kafka
    /// fills it for every coordinator row whose error is `NONE`.
    #[tokio::test]
    async fn a_group_that_is_not_classic_is_dead() {
        let all_bits = {
            let p = principal("admin");
            authorized_operations_bits(
                &AllowAllAuthorizer,
                &krabka_metadata::MetadataImage::new(uuid::Uuid::nil()),
                &request_context(&p, &peer(), "test-client"),
                ResourceType::Group,
                "x",
            )
        };
        // (version, group, include flag, expected row)
        let rows = [
            (
                krabka_ids::ApiVersion(5),
                "never-seen",
                OperationsDisclosure::Omit,
                dead().call(),
            ),
            (
                krabka_ids::ApiVersion(5),
                "never-seen",
                OperationsDisclosure::Include,
                dead().operations(all_bits).call(),
            ),
            (
                krabka_ids::ApiVersion(6),
                "never-seen",
                OperationsDisclosure::Include,
                dead()
                    .error(codes::GROUP_ID_NOT_FOUND)
                    .maybe_message(Some("Group never-seen not found."))
                    .call(),
            ),
            (
                krabka_ids::ApiVersion(5),
                "next-gen",
                OperationsDisclosure::Omit,
                dead().group("next-gen").call(),
            ),
            (
                krabka_ids::ApiVersion(6),
                "next-gen",
                OperationsDisclosure::Omit,
                dead()
                    .group("next-gen")
                    .error(codes::GROUP_ID_NOT_FOUND)
                    .maybe_message(Some("Group next-gen is not a classic group."))
                    .call(),
            ),
        ];
        broker_fixture!((broker_handle, _dir, broker), group_allow_all);
        let _ = broker.group_coordinator.get_or_create_consumer("next-gen");

        for (version, group, include_ops, expected) in rows {
            let resp = drive(
                &broker,
                version,
                &request().groups(&[group]).operations(include_ops).call(),
            )
            .await;

            assert!(
                resp == krabka_macros::unthrottled_wire!(DescribeGroupsResponse {
                    groups: vec![expected],
                }),
                "v{version} {group}"
            );
        }
        broker_handle.shutdown().await;
    }

    fn member_view(assignment: Option<&'static [u8]>) -> ClassicMemberView {
        ClassicMemberView {
            member_id: "m-1".into(),
            client_id: "client-1".into(),
            host: "/10.0.0.1".into(),
            group_instance_id: Some("instance-1".into()),
            protocol_metadata: Bytes::from_static(b"range-metadata"),
            assignment: assignment.map(Bytes::from_static),
        }
    }

    fn classic_view(state: GroupState, assignment: Option<&'static [u8]>) -> ClassicView {
        ClassicView {
            group_id: "classic".into(),
            state,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            generation_id: 3,
            members: vec![member_view(assignment)],
        }
    }

    /// Only a `Stable` group reports its protocol name and member metadata;
    /// every state keeps the member's instance id and assignment, as
    /// `ClassicGroupMember.describeNoMetadata` does.
    #[test]
    fn only_a_stable_group_carries_protocol_metadata() {
        let member = |metadata: &'static [u8], assignment: &'static [u8]| {
            krabka_macros::tagged_wire!(DescribedGroupMember {
                member_id: "m-1".into(),
                group_instance_id: Some("instance-1".into()),
                client_id: "client-1".into(),
                client_host: "/10.0.0.1".into(),
                member_metadata: Bytes::from_static(metadata),
                member_assignment: Bytes::from_static(assignment),
            })
        };
        let group = |state: &str, protocol_data: &str, m: DescribedGroupMember| DescribedGroup {
            group_state: state.into(),
            protocol_type: "consumer".into(),
            protocol_data: protocol_data.into(),
            members: vec![m],
            ..error_row("classic", KafkaErrorCode(codes::NONE))
        };
        // (view, expected row)
        let rows = [
            (
                classic_view(GroupState::Stable, Some(b"assigned")),
                group("Stable", "range", member(b"range-metadata", b"assigned")),
            ),
            (
                classic_view(GroupState::CompletingRebalance, None),
                group("CompletingRebalance", "", member(b"", b"")),
            ),
            (
                classic_view(GroupState::PreparingRebalance, Some(b"assigned")),
                group("PreparingRebalance", "", member(b"", b"assigned")),
            ),
        ];
        for (view, expected) in rows {
            let state = view.state;
            assert!(described_classic(view) == expected, "{state:?}");
        }
    }

    /// A live classic group is projected with its state and, per Kafka, the
    /// empty string for the `protocol_type` and `protocol_data` a group that
    /// has not yet joined a protocol carries. Without the KIP-430 flag the
    /// bitfield keeps the `i32::MIN` "not present" sentinel, which is what
    /// separates this row from `the_authorized_operations_bitfield_is_filled_only_on_opt_in`.
    #[tokio::test]
    async fn a_classic_group_is_projected_without_the_kip430_bitfield() {
        broker_fixture!((broker_handle, _dir, broker), group_allow_all);
        let _ = broker.group_coordinator.get_or_create_classic("classic-a");

        let resp = drive(&broker, VERSION, &request().call()).await;

        assert!(
            resp == krabka_macros::unthrottled_wire!(DescribeGroupsResponse {
                groups: vec![krabka_macros::tagged_wire!(DescribedGroup {
                    error_code: codes::NONE,
                    error_message: None,
                    group_id: "classic-a".into(),
                    group_state: "Empty".into(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: vec![],
                    authorized_operations: i32::MIN,
                })],
            }),
            "{resp:?}"
        );
        broker_handle.shutdown().await;
    }

    /// KIP-430: with the flag set the row carries the group operations the
    /// principal holds, so the field moves off its sentinel.
    #[tokio::test]
    async fn the_authorized_operations_bitfield_is_filled_only_on_opt_in() {
        let authorizer = Arc::new(AllowAllAuthorizer);
        let (broker_handle, _dir) = start_group_broker_no_audit(Arc::clone(&authorizer) as _).await;
        let broker = broker_handle.broker_arc_for_test();
        let _ = broker.group_coordinator.get_or_create_classic("classic-a");

        let resp = drive(
            &broker,
            VERSION,
            &request().operations(OperationsDisclosure::Include).call(),
        )
        .await;

        request_identity!((p, peer), principal("admin"));
        let expected = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &request_context(&p, &peer, "test-client"),
            ResourceType::Group,
            "classic-a",
        );
        assert!(expected != i32::MIN);
        assert!(
            resp.groups
                .iter()
                .map(|g| (g.group_id.as_str(), g.error_code, g.authorized_operations))
                .collect::<Vec<_>>()
                == vec![("classic-a", codes::NONE, expected)]
        );
        broker_handle.shutdown().await;
    }

    fn allow(group: &str, operation: AclOperation) -> krabka_metadata::MetadataRecord {
        krabka_metadata::MetadataRecord::V1AccessControlEntry(
            allow_acl()
                .resource_type(ResourceType::Group)
                .resource_name(group)
                .principal("User:admin")
                .operation(operation)
                .call(),
        )
    }

    fn bit(op: AclOperation) -> i32 { 1_i32 << operation_to_wire(op) }

    /// `Empty` classic `allowed`, as `DescribeGroups` answers it with
    /// `authorized_operations` bits.
    fn empty_row(group_id: &str, authorized_operations: i32) -> DescribedGroup {
        DescribedGroup {
            group_state: "Empty".into(),
            authorized_operations,
            ..error_row(group_id, KafkaErrorCode(codes::NONE))
        }
    }

    /// Kafka's `handleDescribeGroupsRequest` answers the denied groups first
    /// and then the coordinator results, and fills KIP-430 bits from the
    /// group's whole supported set (`Read`, `Describe`, `Delete`,
    /// `DescribeConfigs`, `AlterConfigs`).
    #[tokio::test]
    async fn denied_rows_come_first_and_the_bits_cover_every_group_operation() {
        // (ACLs on `allowed`, include flag, expected response rows)
        let rows = [
            (
                vec![AclOperation::Describe],
                OperationsDisclosure::Omit,
                vec![
                    error_row(
                        "denied",
                        crate::test_support::KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED),
                    ),
                    empty_row("allowed", i32::MIN),
                ],
            ),
            (
                vec![AclOperation::Describe, AclOperation::AlterConfigs],
                OperationsDisclosure::Include,
                vec![
                    error_row(
                        "denied",
                        crate::test_support::KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED),
                    ),
                    empty_row(
                        "allowed",
                        bit(AclOperation::Describe)
                            | bit(AclOperation::DescribeConfigs)
                            | bit(AclOperation::AlterConfigs),
                    ),
                ],
            ),
            (
                vec![AclOperation::All],
                OperationsDisclosure::Include,
                vec![
                    error_row(
                        "denied",
                        crate::test_support::KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED),
                    ),
                    empty_row(
                        "allowed",
                        bit(AclOperation::Read)
                            | bit(AclOperation::Describe)
                            | bit(AclOperation::Delete)
                            | bit(AclOperation::DescribeConfigs)
                            | bit(AclOperation::AlterConfigs),
                    ),
                ],
            ),
        ];
        for (acls, include_ops, expected) in rows {
            let authorizer = SimpleAclAuthorizer::new(std::collections::HashSet::new());
            let (broker_handle, _dir) = start_group_broker_no_audit(Arc::new(authorizer)).await;
            let broker = broker_handle.broker_arc_for_test();
            broker
                .controller
                .submit_change(acls.iter().map(|op| allow("allowed", *op)).collect())
                .await
                .expect("grant ACLs");
            let _ = broker.group_coordinator.get_or_create_classic("allowed");

            let resp = drive(
                &broker,
                VERSION,
                &request()
                    .groups(&["allowed", "denied"])
                    .operations(include_ops)
                    .call(),
            )
            .await;

            assert!(
                resp == krabka_macros::unthrottled_wire!(DescribeGroupsResponse {
                    groups: expected
                }),
                "{acls:?}"
            );
            broker_handle.shutdown().await;
        }
    }

    /// The `GroupState` -> Kafka string projection is exhaustive; every state a
    /// classic group can report has its own name on the wire.
    #[test]
    fn every_group_state_has_its_kafka_name() {
        assert!(
            [
                GroupState::Empty,
                GroupState::PreparingRebalance,
                GroupState::CompletingRebalance,
                GroupState::Stable,
            ]
            .map(GroupState::as_str)
                == [
                    "Empty",
                    "PreparingRebalance",
                    "CompletingRebalance",
                    "Stable"
                ]
        );
    }
}
