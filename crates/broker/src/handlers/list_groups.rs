//! `ListGroups` (`api_key=16`) returns every known group from all four
//! registries.
//!
//! The four registries are:
//!
//! - Classic groups from `GroupCoordinator::list_groups`, type `"classic"`.
//! - Next-gen KIP-848 consumer groups from the consumer registry of the
//!   unified coordinator, type `"consumer"`.
//! - KIP-932 share groups from its share registry, type `"share"`.
//! - KIP-1071 streams groups from its streams registry, type `"streams"`.
//!
//! Each group reports its live state, read from its actor, and the protocol
//! type of Kafka's `asListedGroup`. The handler honors both optional filters,
//! as Kafka's `GroupMetadataManager.listGroups` does: `states_filter` (v4+)
//! compares without case after a trim, and `types_filter` (v5+) compares
//! without case. For example, `kafka-share-groups.sh --list` sends
//! `["share"]`, `kafka-streams-groups.sh --list` sends `["streams"]`, and
//! `kafka-consumer-groups.sh --list` sends `["consumer"]`.
//!
//! Authorization follows Kafka's `KafkaApis.handleListGroupsRequest`: a
//! principal with `Describe` on the cluster sees every group, and any other
//! principal sees only the groups it may `Describe`.
//!
//! While any `__consumer_offsets` partition this broker leads is still
//! loading, the answer is `COORDINATOR_LOAD_IN_PROGRESS` and no groups, as in
//! Kafka, so a client asks again instead of reading a partial list.
//!
//! The handler emits a `group_id` at most once. The registries are disjoint by
//! `GroupType`, but the handler dedups defensively.

use std::collections::HashSet;

use krabka_protocol::owned::{
    list_groups_request::ListGroupsRequest,
    list_groups_response::{ListGroupsResponse, ListedGroup},
};

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::{
        GroupType, actor::GroupActorMessage, share::actor::ShareGroupActorMessage,
        streams::actor::StreamsGroupActorMessage,
    },
    handlers::{
        cluster_describe_denied, coordinator_routing::any_group_partition_loading,
        group_describe_denied,
    },
    task_util::ask,
};

/// Wire `group_type` string for classic (pre-KIP-848) groups.
const GROUP_TYPE_CLASSIC: &str = "classic";
/// Wire `group_type` string for KIP-848 next-gen consumer groups.
const GROUP_TYPE_CONSUMER: &str = "consumer";
/// Wire `group_type` string for KIP-932 share groups.
const GROUP_TYPE_SHARE: &str = "share";
/// Wire `group_type` string for KIP-1071 streams groups.
const GROUP_TYPE_STREAMS: &str = "streams";
/// Kafka's consumer embedded-protocol type (`ConsumerProtocol.PROTOCOL_TYPE`),
/// which a next-gen consumer group reports as its `protocol_type`.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";
/// Kafka's `ShareGroup.PROTOCOL_TYPE`.
const SHARE_PROTOCOL_TYPE: &str = "share";
/// Kafka's `StreamsGroup.PROTOCOL_TYPE`.
const STREAMS_PROTOCOL_TYPE: &str = "streams";

context_handler! {
    ListGroupsRequest => ListGroupsResponse,
    (broker, req, _version, ctx),
    {
        // Kafka's `GroupCoordinatorService.listGroups` reads every local shard and
        // answers the load error of one that is still loading, with no groups.
        if any_group_partition_loading(broker) {
            let resp = ListGroupsResponse {
                error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                ..Default::default()
            };
            return Ok(resp);
        }
        let candidates = collect_groups(broker).await;

        let image = broker.controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        // Kafka checks `Describe` on the cluster once. A principal that has it
        // sees every group; any other principal sees only the groups it may
        // `Describe`, and a denied group is silently omitted.
        let cluster_describe = !cluster_describe_denied(authorizer, &image, ctx);
        let may_describe = |group_id: &str| {
            cluster_describe || !group_describe_denied(authorizer, &image, ctx, group_id)
        };

        Ok(ListGroupsResponse {
            error_code: codes::NONE,
            groups: filter_groups(candidates, &req, &may_describe),
            throttle_time_ms: 0,
            ..Default::default()
        })
    }
}

/// Every group the coordinator hosts, as Kafka's `asListedGroup` renders it,
/// before the filters and the ACLs.
///
/// Each next-gen group's state comes from its actor. An actor that has
/// stopped, or that no longer holds a group of the registry's type, answers
/// nothing and is left out.
async fn collect_groups(broker: &Broker) -> Vec<ListedGroup> {
    let coordinator = &broker.group_coordinator;
    let mut groups: Vec<ListedGroup> = Vec::new();
    // Ids already emitted, so the same group_id never appears twice across the
    // (disjoint-by-design) registries.
    let mut emitted: HashSet<String> = HashSet::new();

    // ── Classic groups (group_type "classic") ───────────────────────────
    for s in coordinator.list_groups().await {
        // KIP-1071: a Streams-locked group keeps its drained classic-kind
        // offset-home actor in this classic snapshot. Report it via the streams
        // pass (group_type="streams"), never here as "classic".
        if coordinator.group_type(&s.group_id) == Some(GroupType::Streams) {
            continue;
        }
        let group = listed(
            s.group_id,
            // Kafka's `ClassicGroup.asListedGroup`: `protocolType.orElse("")`,
            // so a group that only commits offsets reports "".
            s.protocol_type.unwrap_or_default(),
            s.state.as_str().into(),
            GROUP_TYPE_CLASSIC,
        );
        push_once(&mut groups, &mut emitted, group);
    }

    // ── KIP-848 next-gen consumer groups (group_type "consumer") ────────
    // `consumer_group_ids` returns every id of the shared `groups` map. The
    // `Describe` arm replies only for a live consumer-kind group, so a classic
    // group, and a Streams-locked offset home, answer nothing here.
    for gid in coordinator.consumer_group_ids() {
        if emitted.contains(&gid) || coordinator.group_type(&gid) == Some(GroupType::Streams) {
            continue;
        }
        let Some(handle) = coordinator.find(&gid) else {
            continue;
        };
        if let Ok(view) = ask(&handle.tx, |reply| GroupActorMessage::Describe { reply }).await {
            let group = listed(
                gid,
                CONSUMER_PROTOCOL_TYPE.into(),
                view.group_state.into(),
                GROUP_TYPE_CONSUMER,
            );
            push_once(&mut groups, &mut emitted, group);
        }
    }

    // ── KIP-932 share groups (group_type "share") ───────────────────────
    for gid in coordinator.share_group_ids() {
        let Some(handle) = coordinator.find_share(&gid) else {
            continue;
        };
        if let Ok(view) = ask(&handle.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        {
            let group = listed(
                gid,
                SHARE_PROTOCOL_TYPE.into(),
                view.group_state,
                GROUP_TYPE_SHARE,
            );
            push_once(&mut groups, &mut emitted, group);
        }
    }

    // ── KIP-1071 streams groups (group_type "streams") ──────────────────
    // Surfaced so the JVM `kafka-streams-groups.sh --list` / `--describe`
    // (AdminClient `listGroups(typesFilter=[Streams])`) can find them; the
    // describe hop is gated behind a non-empty list on the JVM side.
    if broker.config.streams_group.enable {
        for gid in coordinator.streams_group_ids() {
            let Some(handle) = coordinator.find_streams(&gid) else {
                continue;
            };
            if let Ok(view) = ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
                reply,
            })
            .await
            {
                let group = listed(
                    gid,
                    STREAMS_PROTOCOL_TYPE.into(),
                    view.group_state,
                    GROUP_TYPE_STREAMS,
                );
                push_once(&mut groups, &mut emitted, group);
            }
        }
    }

    groups
}

/// Appends `group` unless a group with its id was already emitted.
fn push_once(groups: &mut Vec<ListedGroup>, emitted: &mut HashSet<String>, group: ListedGroup) {
    if emitted.insert(group.group_id.clone()) {
        groups.push(group);
    }
}

fn listed(
    group_id: String,
    protocol_type: String,
    group_state: String,
    group_type: &str,
) -> ListedGroup {
    ListedGroup {
        group_id,
        protocol_type,
        group_state,
        group_type: group_type.into(),
        ..Default::default()
    }
}

/// Keeps the groups that match the request's filters and that `authorized`
/// allows.
///
/// It follows Kafka's `GroupMetadataManager.listGroups`: each `states_filter`
/// value is lower-cased and trimmed and compared with the lower-cased state,
/// and each `types_filter` value is compared with the type without case. An
/// empty filter matches every group.
fn filter_groups(
    groups: Vec<ListedGroup>,
    req: &ListGroupsRequest,
    authorized: &impl Fn(&str) -> bool,
) -> Vec<ListedGroup> {
    let states: HashSet<String> = req
        .states_filter
        .iter()
        .map(|state| state.to_lowercase().trim().to_owned())
        .collect();
    groups
        .into_iter()
        .filter(|group| {
            (states.is_empty() || states.contains(&group.group_state.to_lowercase()))
                && (req.types_filter.is_empty()
                    || req
                        .types_filter
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case(&group.group_type)))
                && authorized(&group.group_id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{AclOperation, MetadataRecord, ResourceType};

    use super::*;
    use crate::{coordinator::unified::classic_state::GroupState, test_support::test_ctx};

    const VERSION: i16 = krabka_protocol::owned::list_groups_response::MAX_VERSION;

    crate::test_support::context_helper!(client_id = "admin-client");

    fn group(group_id: &str, protocol_type: &str, state: &str, group_type: &str) -> ListedGroup {
        listed(
            group_id.into(),
            protocol_type.into(),
            state.into(),
            group_type,
        )
    }

    fn sorted(mut groups: Vec<ListedGroup>) -> Vec<ListedGroup> {
        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        groups
    }

    /// Every group kind the handler lists reports its live state and Kafka's
    /// protocol type: an offset-only classic group reports "", and an empty
    /// consumer, share or streams group reports `Empty`.
    #[tokio::test]
    async fn handler_lists_each_kind_with_its_state_and_protocol_type() {
        let (broker_handle, _dir) = crate::test_support::start_broker_no_audit_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = &broker.group_coordinator;
        let _classic = coordinator.get_or_create_classic("classic-a");
        let _consumer = coordinator.get_or_create_consumer("consumer-a");
        coordinator.mark_share("share-a");
        let _share = coordinator.get_or_create_share("share-a");
        test_ctx!(ctx, "admin");

        let resp = handle(&broker, ListGroupsRequest::default(), VERSION, &ctx)
            .await
            .expect("handle");

        let expected = unthrottled_wire!(ListGroupsResponse {
            error_code: codes::NONE,
            groups: vec![
                group("classic-a", "", "Empty", "classic"),
                group("consumer-a", "consumer", "Empty", "consumer"),
                group("share-a", "share", "Empty", "share"),
            ],
        });
        let resp = ListGroupsResponse {
            groups: sorted(resp.groups),
            ..resp
        };
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    fn acl(resource_type: ResourceType, name: &str, user: &str) -> MetadataRecord {
        MetadataRecord::V1AccessControlEntry(crate::test_support::allow_acl(
            crate::test_support::AllowAclSetup {
                resource_type,
                resource_name: name,
                principal: &format!("User:{user}"),
                operation: AclOperation::Describe,
            },
        ))
    }

    /// Kafka's `handleListGroupsRequest`: cluster `Describe` lists every group
    /// with no per-group check, and without it only the groups the principal
    /// may `Describe` are listed.
    #[tokio::test]
    async fn handler_authorizes_by_cluster_describe_then_group_describe() {
        let cluster = crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME;
        let both = vec![
            group("g-a", "", "Empty", "classic"),
            group("g-b", "", "Empty", "classic"),
        ];
        // (user, ACLs, expected groups)
        let rows = [
            (
                "monitor",
                vec![acl(ResourceType::Cluster, cluster, "monitor")],
                both.clone(),
            ),
            (
                "reader",
                vec![acl(ResourceType::Group, "g-a", "reader")],
                vec![group("g-a", "", "Empty", "classic")],
            ),
            ("nobody", Vec::new(), Vec::new()),
            ("admin", Vec::new(), both),
        ];

        for (user, acls, expected_groups) in rows {
            let (broker_handle, _dir) = crate::test_support::start_broker_no_audit_with(|cfg| {
                cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                    HashSet::from(["admin".to_string()]),
                ));
            })
            .await;
            let broker = broker_handle.broker_arc_for_test();
            if !acls.is_empty() {
                broker
                    .controller
                    .submit_change(acls)
                    .await
                    .expect("commit ACLs");
            }
            let _a = broker.group_coordinator.get_or_create_classic("g-a");
            let _b = broker.group_coordinator.get_or_create_classic("g-b");
            test_ctx!(ctx, user);

            let resp = handle(&broker, ListGroupsRequest::default(), VERSION, &ctx)
                .await
                .expect("handle");

            let expected = ListGroupsResponse {
                groups: expected_groups,
                ..Default::default()
            };
            let resp = ListGroupsResponse {
                groups: sorted(resp.groups),
                ..resp
            };
            assert!(resp == expected, "{user}: {resp:?}");
            broker_handle.shutdown().await;
        }
    }

    #[test]
    fn filter_groups_matches_kafka_filters() {
        let groups = vec![
            group("classic-a", "", "Empty", "classic"),
            group("classic-b", "consumer", "PreparingRebalance", "classic"),
            group("consumer-a", "consumer", "Reconciling", "consumer"),
            group("consumer-b", "consumer", "Stable", "consumer"),
            group("share-a", "share", "Empty", "share"),
            group("streams-a", "streams", "NotReady", "streams"),
            group("denied", "consumer", "Stable", "consumer"),
        ];
        let request = |states: &[&str], types: &[&str]| ListGroupsRequest {
            states_filter: states.iter().map(|s| (*s).to_owned()).collect(),
            types_filter: types.iter().map(|t| (*t).to_owned()).collect(),
            ..Default::default()
        };
        let ids = |ids: &[&str]| ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        // (request, expected group ids)
        let rows = [
            (
                request(&[], &[]),
                ids(&[
                    "classic-a",
                    "classic-b",
                    "consumer-a",
                    "consumer-b",
                    "share-a",
                    "streams-a",
                ]),
            ),
            (request(&["empty"], &[]), ids(&["classic-a", "share-a"])),
            (request(&[" STABLE "], &[]), ids(&["consumer-b"])),
            (
                request(&["reconciling", "notready"], &[]),
                ids(&["consumer-a", "streams-a"]),
            ),
            (request(&["Dead"], &[]), ids(&[])),
            (request(&[], &["Share"]), ids(&["share-a"])),
            (
                request(&[], &["CLASSIC", "streams"]),
                ids(&["classic-a", "classic-b", "streams-a"]),
            ),
            (request(&[], &["unknown"]), ids(&[])),
            (request(&["empty"], &["consumer"]), ids(&[])),
        ];

        for (req, expected) in rows {
            let listed = filter_groups(groups.clone(), &req, &|gid| gid != "denied");
            let expected: Vec<ListedGroup> = groups
                .iter()
                .filter(|g| expected.contains(&g.group_id))
                .cloned()
                .collect();
            assert!(listed == expected, "{req:?}");
        }
    }

    #[test]
    fn group_state_as_str_covers_all_classic_states() {
        let cases = [
            (GroupState::Empty, "Empty"),
            (GroupState::PreparingRebalance, "PreparingRebalance"),
            (GroupState::CompletingRebalance, "CompletingRebalance"),
            (GroupState::Stable, "Stable"),
        ];
        for (state, want) in cases {
            assert!(state.as_str() == want, "{state:?}");
        }
    }
}
