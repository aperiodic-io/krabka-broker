//! Read-only projections of a live group for the admin and describe paths.
//!
//! The classic `DescribeGroups` / `ListGroups` surface and the KIP-848
//! `ConsumerGroupDescribe` surface both read a group that may be classic or
//! consumer at any moment, so the projections that hide that difference live
//! together here.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_protocol::primitives::uuid::Uuid;

use super::MetadataProvider;
use crate::coordinator::{
    GroupSnapshot, MemberSnapshot,
    unified::{
        classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
        consumer_state::GroupState,
        group::CoordinatorGroup,
        migration,
        reconciler::ReconcileInput,
    },
};

/// Read-only projection of a classic `Group` for the admin and offset-delete
/// handlers. Those handlers need member subscriptions that the next-gen
/// `DescribeView` does not carry.
#[derive(Debug, Clone)]
pub struct ClassicView {
    pub group_id: String,
    pub state: ClassicGroupState,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub generation_id: i32,
    pub members: Vec<ClassicMemberView>,
}

#[derive(Debug, Clone)]
pub struct ClassicMemberView {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub group_instance_id: Option<String>,
    pub protocol_metadata: Bytes,
    pub assignment: Option<Bytes>,
}

impl ClassicView {
    /// Build the admin `GroupSnapshot` (`ListGroups`/`DescribeGroups`).
    #[must_use]
    pub fn snapshot(&self) -> GroupSnapshot {
        GroupSnapshot {
            group_id: self.group_id.clone(),
            state: self.state,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            generation_id: self.generation_id,
            members: self
                .members
                .iter()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.host.clone(),
                    assignment: m
                        .assignment
                        .as_ref()
                        .map(|b| b.to_vec())
                        .unwrap_or_default(),
                    protocol_metadata: m.protocol_metadata.to_vec(),
                })
                .collect(),
        }
    }
}

/// Projects a next-gen consumer `GroupState` into the classic admin
/// `GroupSnapshot` (`ListGroups` and `DescribeGroups`).
///
/// This function reports KIP-848 consumer groups to the classic admin path
/// with `protocol_type = "consumer"` and the classic `Stable` state. `Stable`
/// is the value the classic path uses for a settled group, and Kafka shows a
/// healthy consumer group as `Stable`. It sets `generation_id` to the group
/// epoch, the next-gen equivalent of the classic generation.
///
/// Each member's assignment is its reconciler TARGET, translated to a
/// `ConsumerProtocolAssignment` blob. `serve_classic_sync` and the heartbeat
/// response use that same source, so an assigned member reports a non-empty
/// assignment. This includes a hosted classic member.
fn build_consumer_snapshot(state: &GroupState, image: &ReconcileInput) -> GroupSnapshot {
    GroupSnapshot {
        group_id: state.group_id.clone(),
        state: ClassicGroupState::Stable,
        protocol_type: Some("consumer".into()),
        // Next-gen (KIP-848) members carry no classic JoinGroup protocol
        // name; `DescribeGroups` is the classic API, so leave it empty.
        protocol_name: None,
        generation_id: state.group_epoch,
        members: state
            .members
            .values()
            .map(|m| {
                let target = state
                    .target
                    .per_member
                    .get(&m.member_id)
                    .cloned()
                    .unwrap_or_default();
                let assignment = migration::target_to_consumer_assignment(&target, image).to_vec();
                MemberSnapshot {
                    member_id: m.member_id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    assignment,
                    // Next-gen members carry no classic JoinGroup metadata.
                    protocol_metadata: Vec::new(),
                }
            })
            .collect(),
    }
}

#[derive(Debug, Clone)]
pub struct DescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    /// The group state, from `GroupState::state_name`.
    pub group_state: &'static str,
    /// The server assignor most members name, as Kafka's
    /// `ConsumerGroup.preferredServerAssignor` picks it. `None` when no
    /// member names one; the describe path then reports the default
    /// assignor.
    pub preferred_server_assignor: Option<String>,
    pub members: Vec<DescribeMember>,
}

#[derive(Debug, Clone)]
pub struct DescribeMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    /// The subscribed topic names, sorted.
    pub subscribed_topic_names: Vec<String>,
    pub subscribed_topic_regex: Option<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    /// Partitions the member still owns until it confirms their revocation.
    pub partitions_pending_revocation: HashMap<Uuid, Vec<i32>>,
    /// The member's share of the group's target assignment.
    pub target_partitions: HashMap<Uuid, Vec<i32>>,
    /// `true` if and only if this is a classic member hosted in an upgraded
    /// group, which means its `ClassicMemberFacade` is set. This flag separates
    /// a classic-protocol member served through the next-gen machinery from a
    /// native consumer member.
    pub is_classic: bool,
}

pub(super) fn inspect_any(
    group: &CoordinatorGroup,
    metadata: &dyn MetadataProvider,
) -> Option<GroupSnapshot> {
    if let Some(state) = group.as_classic() {
        Some(build_classic_view(state).snapshot())
    } else {
        group
            .as_consumer()
            .map(|state| build_consumer_snapshot(state, &metadata.snapshot()))
    }
}

/// Build the read-only classic view for the admin / offset-delete handlers.
pub(super) fn build_classic_view(state: &ClassicState) -> ClassicView {
    ClassicView {
        group_id: state.group_id.clone(),
        state: state.state,
        protocol_type: state.protocol_type.clone(),
        protocol_name: state.protocol_name.clone(),
        generation_id: state.generation_id,
        members: state
            .members
            .values()
            .map(|m| ClassicMemberView {
                member_id: m.id.clone(),
                client_id: m.client_id.clone(),
                host: m.host.clone(),
                group_instance_id: m.group_instance_id.clone(),
                protocol_metadata: m.protocol_metadata.clone(),
                assignment: m.assignment.clone(),
            })
            .collect(),
    }
}

pub(super) fn build_describe(state: &GroupState) -> DescribeView {
    DescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        group_state: state.state_name(),
        preferred_server_assignor: preferred_server_assignor(state),
        members: state
            .members
            .values()
            .map(|m| {
                let mut subscribed_topic_names: Vec<String> =
                    m.subscribed_topic_names.iter().cloned().collect();
                subscribed_topic_names.sort_unstable();
                DescribeMember {
                    member_id: m.member_id.clone(),
                    instance_id: m.instance_id.clone(),
                    rack_id: m.rack_id.clone(),
                    member_epoch: m.member_epoch,
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    subscribed_topic_names,
                    subscribed_topic_regex: m.subscribed_topic_regex.clone(),
                    assigned_partitions: m.assigned_partitions.clone(),
                    partitions_pending_revocation: m.partitions_pending_revocation.clone(),
                    target_partitions: state
                        .target
                        .per_member
                        .get(&m.member_id)
                        .cloned()
                        .unwrap_or_default(),
                    is_classic: m.is_classic(),
                }
            })
            .collect(),
    }
}

/// The server assignor the most members name, as Kafka's
/// `ConsumerGroup.preferredServerAssignor` counts them. A tie goes to the
/// smallest name, so the answer does not depend on map order. It is the
/// assignor that computes the group's target, and the one `Describe` reports.
pub(super) fn preferred_server_assignor(state: &GroupState) -> Option<String> {
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for name in state
        .members
        .values()
        .filter_map(|m| m.server_assignor.as_deref())
    {
        *votes.entry(name).or_default() += 1;
    }
    votes
        .into_iter()
        .max_by(|(a_name, a_votes), (b_name, b_votes)| {
            a_votes.cmp(b_votes).then_with(|| b_name.cmp(a_name))
        })
        .map(|(name, _)| name.to_string())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use crate::{
        codes,
        coordinator::unified::actor::{
            GroupActorMessage,
            test_support::{
                make_coordinator, make_coordinator_with_topic_policy, rpc, seed_and_upgrade,
            },
        },
    };

    /// Kafka's `preferredServerAssignor` takes the name the most members
    /// chose; krabka breaks a tie on the smallest name.
    #[test]
    fn the_preferred_assignor_is_the_most_named_one() {
        // (each member's `server_assignor`, expected preference)
        let rows: [(Vec<Option<&str>>, Option<&str>); 4] = [
            (vec![], None),
            (vec![None, None], None),
            (
                vec![Some("range"), Some("uniform"), Some("uniform"), None],
                Some("uniform"),
            ),
            (vec![Some("uniform"), Some("range")], Some("range")),
        ];
        for (assignors, expected) in rows {
            let mut state = crate::coordinator::unified::consumer_state::GroupState::new("g");
            for (i, assignor) in assignors.iter().enumerate() {
                let mut m = crate::coordinator::unified::consumer_state::test_support::member(
                    &format!("m{i}"),
                );
                m.server_assignor = assignor.map(str::to_string);
                state.members.insert(m.member_id.clone(), m);
            }
            assert!(
                super::preferred_server_assignor(&state).as_deref() == expected,
                "{assignors:?}"
            );
        }
    }

    // ── classic actor arms + coordinator admin surface ──────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_admin_surface_and_immediate_join() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequest;
        let (coord, log) = make_coordinator();
        let handle =
            crate::coordinator::unified::actor::test_support::marked_classic_handle(&coord, "g");

        // Empty member_id → immediate MEMBER_ID_REQUIRED (no member added).
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicJoin {
            req: JoinGroupRequest {
                group_id: "g".into(),
                member_id: String::new(),
                protocol_type: "consumer".into(),
                protocols: vec![
                    krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                        name: "range".into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            version: 4,
            client_id: "client-a".into(),
            client_host: String::new(),
            regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
            reply: tx,
        })
        .await;
        let r = rx.await.unwrap();
        assert!(r.error_code == codes::MEMBER_ID_REQUIRED);

        // ClassicInspect → empty view.
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicInspect {
            reply: tx,
        })
        .await;
        let view = rx.await.unwrap();
        assert!(view.group_id == "g" && view.members.is_empty());

        // Admin surface lists/describes the classic group, then deletes it (empty).
        let listed = coord.list_groups().await;
        check!(listed.iter().any(|s| s.group_id == "g"));
        check!(coord.describe_group("g").await.is_some());
        check!(coord.delete_group("g").await == Ok(()));
        check!(coord.describe_group("g").await.is_none());
        check!(log.has_classic_group_metadata_tombstone("g").await);
        check!(coord.group_type("g").is_none());
    }

    /// KIP-848 admin coherence: after an in-place UPGRADE the group is
    /// consumer-kind, yet the classic `kafka-consumer-groups --list` and
    /// `--describe` path must still report it. `describe_group` inspects the
    /// LIVE group and projects the consumer state into a `GroupSnapshot`, and
    /// `list_groups` includes it too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_reports_an_upgraded_consumer_group() {
        // Pin `Upgrade` policy so the transient native member's leave in
        // `seed_and_upgrade` does NOT downgrade the group back to classic —
        // we want it to STAY consumer-kind for this test.
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        // Seed classic "m-classic" subscribed to "t", then upgrade in place via a
        // native consumer heartbeat. The group is now consumer-kind, hosting the
        // classic member (the native member left in the helper).
        let _handle = seed_and_upgrade(&coord, "t").await;

        let snap = coord
            .describe_group("g")
            .await
            .expect("describe must surface an upgraded consumer group");
        // The hosted classic member survives the upgrade and is reported.
        // KIP-848 next-gen consumer groups report protocol_type "consumer".
        // The assignment is projected from the member's reconciler target, so
        // an assigned hosted-classic member has non-empty assignment bytes.
        // generation_id mirrors the group epoch (the next-gen analogue of a
        // classic group's generation) and must have advanced off 0.
        check!(snap.group_id.as_str() == "g");
        check!(!snap.members.is_empty());
        check!(snap.members.iter().any(|m| m.member_id == "m-classic"));
        check!(snap.protocol_type.as_deref() == Some("consumer"));
        check!(
            snap.members
                .iter()
                .any(|m| m.member_id == "m-classic" && !m.assignment.is_empty())
        );
        check!(snap.generation_id >= 1);

        // `list_groups` produces the wire `group_type="classic"` rows; an
        // upgraded (consumer-kind) group is NOT a classic row, so it does not
        // appear here. The `ListGroups` handler surfaces it separately via
        // `consumer_group_ids()` tagged `group_type="consumer"` (so it is neither
        // double-counted nor mislabeled). Assert both halves of that contract.
        let listed = coord.list_groups().await;
        assert!(
            !listed.iter().any(|s| s.group_id == "g"),
            "an upgraded consumer group must not be reported as a classic row"
        );
        assert!(
            coord.consumer_group_ids().contains(&"g".to_string()),
            "the upgraded consumer group must be listed for the wire `consumer` pass"
        );
    }
}
