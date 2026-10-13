//! Wire requests, canonical projections and ownership oracles shared by the
//! reconciliation and consumer-group composition models.

use {
    super::{HeartbeatStep, RegexResolution, step_heartbeat},
    crate::coordinator::unified::{
        ClientIdentity,
        actor::MetadataProvider,
        config::NextGenConfig,
        consumer_state::{GroupState, MemberState},
        persistence_next_gen::MemberAssignmentState,
        reconciler::ReconcileInput,
    },
    krabka_protocol::{
        owned::consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        primitives::uuid::Uuid,
    },
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, HashSet},
        time::{Duration, Instant},
    },
};

pub const TOPIC: Uuid = Uuid([7; 16]);
pub const TOPIC_NAME: &str = "t";

/// Whether a target coordinate is still held by another member's client ledger.
macro_rules! handoff_witness {
    ($state:ident) => {
        overlaps_others(
            $state.members.iter().map(|m| (&m.id, &m.target)),
            &$state.client_owned,
        )
    };
}
pub(crate) use handoff_witness;

/// Compare original projected member epochs with the group rebuilt by the real step.
macro_rules! model_epoch_assertion {
    ($(#[$meta:meta])* $visibility:vis fn $name:ident($state:ty, $group:ty)) => {
        $(#[$meta])*
        $visibility fn $name(pre: &$state, post: &$group) {
            assert_member_epochs(
                pre.members.iter().map(|m| (m.id.as_str(), m.member_epoch)),
                post,
            );
        }
    };
}
pub(crate) use model_epoch_assertion;

/// Declare model properties in their original order, retaining each oracle expression.
macro_rules! model_properties {
    (@method $state_type:ty; $($properties:tt)*) => {
        fn properties(&self) -> Vec<::stateright::Property<Self>> {
            $crate::coordinator::unified::actor::reconciliation_model_support::model_properties!(
                $state_type; $($properties)*
            )
        }
    };
    ($state_type:ty; $($kind:ident $name:literal => |$state:ident| $oracle:expr),+ $(,)?) => {
        vec![$(::stateright::Property::$kind($name, |_, $state: &$state_type| $oracle)),+]
    };
}
pub(crate) use model_properties;

/// Keep each model's concrete runner and bounds while sharing its pinned-count check.
/// The declaration chooses whether properties run before or after the original pin.
macro_rules! pinned_model_runner {
    (@checked $(#[$meta:meta])* $visibility:vis fn $name:ident($model:ty);
        $bfs:path, $pin:path; $depth:expr, $states:expr; $order:ident;
        |$checker:ident, $label:ident| { $($checks:tt)* }) => {
        $(#[$meta])*
        $visibility fn $name(model: $model, $label: &str, pinned_unique_states: usize) {
            let $checker = $bfs(model, $label, $depth, $states);
            $($checks)*
            $crate::coordinator::unified::actor::reconciliation_model_support::pinned_model_runner! { @checked_before $order, $checker }
            $pin($checker.unique_state_count(), pinned_unique_states, $label,);
            $crate::coordinator::unified::actor::reconciliation_model_support::pinned_model_runner! { @checked_after $order, $checker }
        }
    };
    (@checked_before properties_first, $checker:ident) => { $checker.assert_properties(); };
    (@checked_before properties_last, $checker:ident) => {};
    (@checked_after properties_first, $checker:ident) => {};
    (@checked_after properties_last, $checker:ident) => { $checker.assert_properties(); };

    ($visibility:vis fn $name:ident($model:ty); $depth:expr, $states:expr; $order:ident) => {
        $visibility fn $name(model: $model, label: &str, pinned_unique_states: usize) {
            let checker = $crate::model_check::run_bfs(model, label, $depth, $states);
            $crate::coordinator::unified::actor::reconciliation_model_support::pinned_model_runner!(@before $order, checker);
            $crate::model_check::assert_pinned_count(
                ::stateright::Checker::unique_state_count(&checker), pinned_unique_states, label,
            );
            $crate::coordinator::unified::actor::reconciliation_model_support::pinned_model_runner!(@after $order, checker);
        }
    };
    (@before properties_first, $checker:ident) => { ::stateright::Checker::assert_properties(&$checker); };
    (@before properties_last, $checker:ident) => {};
    (@after properties_first, $checker:ident) => {};
    (@after properties_last, $checker:ident) => { ::stateright::Checker::assert_properties(&$checker); };
}
pub(crate) use pinned_model_runner;

/// Rebuild the common real member/group state while retaining each model's extra projection fields.
macro_rules! rebuild_model_group {
    ($(#[$doc:meta])* $visibility:vis fn $name:ident($state:ident: &$state_type:ty); |$member:ident, $built:ident| $(; mutate($updated:ident) $extra:block)?) => {
        $(#[$doc])*
        $visibility fn $name($state: &$state_type) -> $crate::coordinator::unified::consumer_state::GroupState {
            use $crate::coordinator::unified::actor::reconciliation_model_support::{modeled_member, insert_modeled_member};
            let mut group = $crate::coordinator::unified::consumer_state::GroupState::new("g");
            group.group_epoch = $state.group_epoch;
            group.record_metadata_hash($state.metadata_hash);
            group.target.epoch = $state.target_epoch;
            let now = std::time::Instant::now();
            for $member in &$state.members {
                let $built = modeled_member(&$member.id, $member.member_epoch, $member.assignment_state,
                    &$member.assigned, &$member.pending_revocation, now);
                $(let mut $updated = $built;
                $extra
                let $built = $updated;)?
                insert_modeled_member(&mut group, $built, &$member.target);
            }
            group
        }
    };
}
pub(crate) use rebuild_model_group;

/// Canonical member fields of both consumer reconciliation models.
/// Extra fields precede the target, retaining each model's derived hash field order.
macro_rules! member_projection {
    ($(#[$doc:meta])* $visibility:vis struct $name:ident { $($(#[$meta:meta])* $extra:ident: $ty:ty,)* }) => {
        $(#[$doc])*
        #[derive(Clone, PartialEq, Eq, Hash, Debug)]
        $visibility struct $name {
            $visibility id: String,
            $visibility member_epoch: i32,
            $visibility assignment_state: $crate::coordinator::unified::persistence_next_gen::MemberAssignmentState,
            /// The coordinator's current assignment, sorted.
            $visibility assigned: Vec<i32>,
            $visibility pending_revocation: Vec<i32>,
            $($(#[$meta])* $visibility $extra: $ty,)*
            /// The member's sorted target assignment.
            $visibility target: Vec<i32>,
        }
        impl $name {
            $visibility fn from_member(
                member: &$crate::coordinator::unified::consumer_state::MemberState,
                target: Option<&::std::collections::HashMap<::krabka_protocol::primitives::uuid::Uuid, Vec<i32>>>,
                $($extra: $ty,)*
            ) -> Self {
                use $crate::coordinator::unified::actor::reconciliation_model_support::parts_of;
                Self {
                    id: member.member_id.clone(),
                    member_epoch: member.member_epoch,
                    assignment_state: member.assignment_state,
                    assigned: parts_of(Some(&member.assigned_partitions)),
                    pending_revocation: parts_of(Some(&member.partitions_pending_revocation)),
                    $($extra,)*
                    target: parts_of(target),
                }
            }
        }
    };
}

pub(crate) use member_projection;

/// Group and faithful-client ledgers shared by the consumer reconciliation models.
macro_rules! group_projection {
    ($visibility:vis struct $name:ident {
        members: Vec<$member:ty>,
        $($(#[$meta:meta])* $extra:ident: $ty:ty,)*
    }) => {
        #[derive(Clone, PartialEq, Eq, Hash, Debug)]
        $visibility struct $name {
            $visibility group_epoch: i32,
            /// The metadata hash that the group recorded last.
            $visibility metadata_hash: i64,
            $visibility target_epoch: i32,
            /// Coordinator-side members, sorted by id.
            $visibility members: Vec<$member>,
            /// Ground-truth ownership, sorted by member id and partition.
            $visibility client_owned: Vec<(String, Vec<i32>)>,
            /// Last advertised assignment, which the faithful client follows, sorted by id and partition.
            $visibility advertised: Vec<(String, Vec<i32>)>,
            $($(#[$meta])* $visibility $extra: $ty,)*
        }
        $visibility fn member<'a>(state: &'a $name, id: &str) -> Option<&'a $member> {
            state.members.iter().find(|member| member.id == id)
        }
        impl $name {
            $visibility fn empty($($extra: $ty,)*) -> Self {
                Self {
                    group_epoch: $crate::coordinator::unified::INITIAL_GROUP_EPOCH,
                    metadata_hash: 0,
                    target_epoch: $crate::coordinator::unified::INITIAL_GROUP_EPOCH,
                    members: vec![],
                    client_owned: vec![],
                    advertised: vec![],
                    $($extra,)*
                }
            }
            $visibility fn from_group(
                group: &$crate::coordinator::unified::consumer_state::GroupState,
                members: Vec<$member>,
                owned: &::std::collections::BTreeMap<String, ::std::collections::BTreeSet<i32>>,
                advertised: &::std::collections::BTreeMap<String, Vec<i32>>,
                $($extra: $ty,)*
            ) -> Self {
                use $crate::coordinator::unified::actor::reconciliation_model_support::owned_to_vec;
                Self {
                    group_epoch: group.group_epoch,
                    metadata_hash: group.metadata_hash(),
                    target_epoch: group.target.epoch,
                    members,
                    client_owned: owned_to_vec(owned),
                    advertised: advertised.iter().map(|(id, parts)| (id.clone(), parts.clone())).collect(),
                    $($extra,)*
                }
            }
        }
    };
}

pub(crate) use group_projection;

/// Membership and faithful-client actions of both models, in their original
/// enum order. Composition-specific actions follow the client moves.
macro_rules! model_actions {
    ($visibility:vis enum $name:ident { $($extra:tt)* }) => {
        #[derive(Clone, PartialEq, Eq, Hash, Debug)]
        $visibility enum $name {
            Join(String),
            Leave(String),
            Heartbeat(String),
            /// A heartbeat without owned partitions, as the unchanged Java client sends.
            Keepalive(String),
            ClientAdd(String, i32),
            ClientRevoke(String, i32),
            $($extra)*
        }
        impl $name {
            $visibility fn member_heartbeat(
                self,
                epoch: impl Fn(&str) -> Option<i32>,
            ) -> Option<$crate::coordinator::unified::actor::reconciliation_model_support::MemberHeartbeat> {
                use $crate::coordinator::unified::actor::reconciliation_model_support::MemberHeartbeat;
                Some(match self {
                    Self::Join(id) => {
                        if epoch(&id).is_some() {
                            return None;
                        }
                        MemberHeartbeat::Join(id)
                    }
                    Self::Leave(id) => {
                        epoch(&id)?;
                        MemberHeartbeat::Leave(id)
                    }
                    Self::Heartbeat(id) => {
                        let value = epoch(&id)?;
                        MemberHeartbeat::Heartbeat(id, value)
                    }
                    Self::Keepalive(id) => {
                        let value = epoch(&id)?;
                        MemberHeartbeat::Keepalive(id, value)
                    }
                    _ => return None,
                })
            }
        }
    };
}
pub(crate) use model_actions;

/// Enumerate joins, then each member's epoch-gated events and faithful-client
/// moves. Model-specific actions follow those moves for each member.
macro_rules! enqueue_member_actions {
    (
        $self:ident,
        $state:ident,
        $actions:ident,
        $kind:ident,
        $lookup:ident; |
        $member:ident |
        $extra:block
    ) => {{
        let under_cap = $state.group_epoch < $self.max_epoch;
        if under_cap {
            for &id in &$self.pool {
                if $lookup($state, id).is_none() {
                    $actions.push($kind::Join(id.to_string()));
                }
            }
        }
        for $member in &$state.members {
            if under_cap {
                $actions.push($kind::Leave($member.id.clone()));
                $actions.push($kind::Heartbeat($member.id.clone()));
                $actions.push($kind::Keepalive($member.id.clone()));
            }
            for (add, partition) in
                $crate::coordinator::unified::actor::reconciliation_model_support::client_moves(
                    &$state.advertised,
                    &$state.client_owned,
                    &$member.id,
                )
            {
                $actions.push(if add {
                    $kind::ClientAdd($member.id.clone(), partition)
                } else {
                    $kind::ClientRevoke($member.id.clone(), partition)
                });
            }
            $extra
        }
    }};
}
pub(crate) use enqueue_member_actions;

/// A membership/client transition using each model's own projection and epoch
/// oracle. Extra dispatch arms retain composition-specific transitions.
macro_rules! member_next_state {
    (
        fn next_state($self:ident, $last:ident, $action:ident; $kind:ident);
        helpers($lookup:ident, $rebuild:ident, $check:ident, $project:ident);
        setup { $($setup:tt)* }
        actions { $($pattern:pat => $result:expr,)* }
        project($($extra:expr),*)
    ) => {
        fn next_state(&$self, $last: &Self::State, $action: Self::Action) -> Option<Self::State> {
            use $crate::coordinator::unified::actor::reconciliation_model_support::{
                apply_client_move, apply_member_heartbeat, owned_map,
            };
            let mut owned = owned_map(&$last.client_owned);
            let mut advertised: ::std::collections::BTreeMap<_, _> = $last.advertised.iter().cloned().collect();
            $($setup)*
            let add = matches!($action, $kind::ClientAdd(..));
            let event = match $action {
                $kind::ClientAdd(id, partition) | $kind::ClientRevoke(id, partition) => {
                    let mut next = $last.clone();
                    next.client_owned = apply_client_move(&$last.advertised, &mut owned, id, partition, add)?;
                    return Some(next);
                }
                $($pattern => $result,)*
                event => event.member_heartbeat(|id| $lookup($last, id).map(|member| member.member_epoch))?,
            };
            let mut group = $rebuild($last);
            apply_member_heartbeat(&mut group, &$self.metadata(), event, &mut owned, &mut advertised);
            $check($last, &group);
            Some($project(&group, &owned, &advertised $(, $extra)*))
        }
    };
}
pub(crate) use member_next_state;

#[derive(Debug)]
pub struct ModelMetadata(ReconcileInput);

impl MetadataProvider for ModelMetadata {
    fn snapshot(&self) -> ReconcileInput { self.0.clone() }
}

pub fn metadata(partitions: i32) -> ModelMetadata {
    ModelMetadata(ReconcileInput {
        topic_id_by_name: [(TOPIC_NAME.to_string(), TOPIC)].into(),
        partitions_per_topic: [(TOPIC, partitions)].into(),
        ..Default::default()
    })
}

pub fn config() -> NextGenConfig { NextGenConfig::default() }

pub fn drive_heartbeat(
    group: &mut GroupState,
    metadata: &dyn MetadataProvider,
    request: &ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    step_heartbeat(
        group,
        &config(),
        metadata,
        request,
        ClientIdentity { id: "", host: "" },
        Instant::now(),
        &RegexResolution::none(),
    )
}

pub fn modeled_member(
    id: &str,
    epoch: i32,
    state: MemberAssignmentState,
    assigned: &[i32],
    pending_revocation: &[i32],
    now: Instant,
) -> MemberState {
    MemberState {
        member_id: id.into(),
        instance_id: None,
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        subscribed_topic_names: [TOPIC_NAME.to_owned()].into(),
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout: Duration::from_mins(1),
        member_epoch: epoch,
        previous_member_epoch: 0,
        assignment_state: state,
        assigned_partitions: to_map(assigned),
        partitions_pending_revocation: to_map(pending_revocation),
        assignment_epochs: HashMap::new(),
        last_seen: now,
        classic: None,
    }
}

pub fn insert_modeled_member(group: &mut GroupState, member: MemberState, target: &[i32]) {
    if !target.is_empty() {
        group
            .target
            .per_member
            .insert(member.member_id.clone(), to_map(target));
    }
    group.members.insert(member.member_id.clone(), member);
}

pub fn client_moves(
    advertised: &[(String, Vec<i32>)],
    owned: &[(String, Vec<i32>)],
    id: &str,
) -> Vec<(bool, i32)> {
    let advertised: BTreeSet<_> = advertised_for(advertised, id).into_iter().collect();
    let owned: BTreeSet<_> = owned
        .iter()
        .find(|(member, _)| member == id)
        .map(|(_, parts)| parts.iter().copied().collect())
        .unwrap_or_default();
    advertised
        .iter()
        .filter(|p| !owned.contains(*p))
        .map(|&p| (true, p))
        .chain(
            owned
                .iter()
                .filter(|p| !advertised.contains(*p))
                .map(|&p| (false, p)),
        )
        .collect()
}

pub enum MemberHeartbeat {
    Join(String),
    Leave(String),
    Heartbeat(String, i32),
    Keepalive(String, i32),
}

/// Apply the same wire event and client-advertisement bookkeeping in both
/// models. Callers retain their membership gates and epoch-monotonicity oracle.
pub fn apply_member_heartbeat(
    group: &mut GroupState,
    metadata: &dyn MetadataProvider,
    event: MemberHeartbeat,
    owned: &mut BTreeMap<String, BTreeSet<i32>>,
    advertised: &mut BTreeMap<String, Vec<i32>>,
) {
    let (id, request, keepalive) = match event {
        MemberHeartbeat::Join(id) => {
            let request = hb_request(&id, 0, &BTreeSet::new());
            owned.entry(id.clone()).or_default();
            (id, request, false)
        }
        MemberHeartbeat::Leave(id) => {
            let request = hb_request(&id, -1, &BTreeSet::new());
            let _ = drive_heartbeat(group, metadata, &request);
            owned.remove(&id);
            advertised.remove(&id);
            return;
        }
        MemberHeartbeat::Heartbeat(id, epoch) => {
            let request = hb_request(&id, epoch, &owned.get(&id).cloned().unwrap_or_default());
            (id, request, false)
        }
        MemberHeartbeat::Keepalive(id, epoch) => {
            let request = keepalive_request(&id, epoch);
            (id, request, true)
        }
    };
    let step = drive_heartbeat(group, metadata, &request);
    match advertised_of(&step) {
        Some(assignment) => {
            advertised.insert(id, assignment);
        }
        None if !keepalive => {
            advertised.insert(id, Vec::new());
        }
        None => {}
    }
}

pub fn apply_client_move(
    advertised: &[(String, Vec<i32>)],
    owned: &mut BTreeMap<String, BTreeSet<i32>>,
    id: String,
    partition: i32,
    add: bool,
) -> Option<Vec<(String, Vec<i32>)>> {
    let advertised_has = advertised_for(advertised, &id).contains(&partition);
    let entry = owned.entry(id).or_default();
    if advertised_has != add || entry.contains(&partition) == add {
        return None;
    }
    if add {
        entry.insert(partition);
    } else {
        entry.remove(&partition);
    }
    Some(owned_to_vec(owned))
}

pub fn hb_request(
    member_id: &str,
    member_epoch: i32,
    owned: &BTreeSet<i32>,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec![TOPIC_NAME.into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: Some(vec![TopicPartitions {
            topic_id: TOPIC,
            partitions: owned.iter().copied().collect(),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

/// The steady-state heartbeat of the Java client sends no unchanged fields.
/// An absent owned set means "unchanged" (`ownsRevokedPartitions(null)`).
pub fn keepalive_request(member_id: &str, member_epoch: i32) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        rebalance_timeout_ms: -1,
        ..Default::default()
    }
}

/// No assignment in the response means that the member keeps the one it has.
pub fn advertised_of(step: &HeartbeatStep) -> Option<Vec<i32>> {
    let assignment = step.response.assignment.as_ref()?;
    let mut partitions: Vec<i32> = assignment
        .topic_partitions
        .iter()
        .filter(|tp| tp.topic_id == TOPIC)
        .flat_map(|tp| tp.partitions.iter().copied())
        .collect();
    partitions.sort_unstable();
    Some(partitions)
}

pub fn parts_of(map: Option<&HashMap<Uuid, Vec<i32>>>) -> Vec<i32> {
    let mut partitions = map.and_then(|m| m.get(&TOPIC)).cloned().unwrap_or_default();
    partitions.sort_unstable();
    partitions
}

pub fn to_map(parts: &[i32]) -> HashMap<Uuid, Vec<i32>> {
    if parts.is_empty() {
        HashMap::new()
    } else {
        [(TOPIC, parts.to_vec())].into()
    }
}

/// Clone identities while collecting each coordinate sequence into the requested container.
macro_rules! collect_owned_coordinates {
    ($owned:ident) => {
        $owned
            .iter()
            .map(|(id, parts)| (id.clone(), parts.iter().copied().collect()))
            .collect()
    };
}

pub fn owned_map(owned: &[(String, Vec<i32>)]) -> BTreeMap<String, BTreeSet<i32>> {
    collect_owned_coordinates!(owned)
}

pub fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    collect_owned_coordinates!(owned)
}

pub fn advertised_for(advertised: &[(String, Vec<i32>)], id: &str) -> Vec<i32> {
    advertised
        .iter()
        .find(|(member, _)| member == id)
        .map(|(_, parts)| parts.clone())
        .unwrap_or_default()
}

pub fn exclusive_ownership(owned: &[(String, Vec<i32>)]) -> bool {
    let mut seen = HashSet::new();
    owned
        .iter()
        .flat_map(|(_, parts)| parts)
        .all(|p| seen.insert(p))
}

/// The independent ownership oracle for the coordinator's advertised ledger.
pub fn advertised_disjoint(
    advertised: &[(String, Vec<i32>)],
    owned: &[(String, Vec<i32>)],
) -> bool {
    !overlaps_others(advertised.iter().map(|(id, parts)| (id, parts)), owned)
}

pub fn overlaps_others<'a>(
    mut assignments: impl Iterator<Item = (&'a String, &'a Vec<i32>)>,
    owned: &[(String, Vec<i32>)],
) -> bool {
    assignments.any(|(id, parts)| {
        parts.iter().any(|p| {
            owned
                .iter()
                .any(|(other, held)| other != id && held.contains(p))
        })
    })
}

/// A transition may remove a member, but must never lower a surviving epoch.
pub fn assert_member_epochs<'a>(members: impl Iterator<Item = (&'a str, i32)>, post: &GroupState) {
    for (id, epoch) in members {
        if let Some(member) = post.members.get(id) {
            assert2::assert!(
                member.member_epoch >= epoch,
                "member_epoch regressed for {}: {} -> {}",
                id,
                epoch,
                member.member_epoch
            );
        }
    }
}
