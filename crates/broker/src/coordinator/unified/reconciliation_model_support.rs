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
    assert2::assert,
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

pub fn owned_map(owned: &[(String, Vec<i32>)]) -> BTreeMap<String, BTreeSet<i32>> {
    owned
        .iter()
        .map(|(id, parts)| (id.clone(), parts.iter().copied().collect()))
        .collect()
}

pub fn owned_to_vec(owned: &BTreeMap<String, BTreeSet<i32>>) -> Vec<(String, Vec<i32>)> {
    owned
        .iter()
        .map(|(id, parts)| (id.clone(), parts.iter().copied().collect()))
        .collect()
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
            assert!(
                member.member_epoch >= epoch,
                "member_epoch regressed for {}: {} -> {}",
                id,
                epoch,
                member.member_epoch
            );
        }
    }
}

/// Record an epoch regression in a model's ghost violation ledger.
pub fn record_epoch_regression<V: Ord>(
    previous: i16,
    current: i16,
    violations: &mut BTreeSet<V>,
    violation: V,
) {
    if current < previous {
        violations.insert(violation);
    }
}

#[test]
fn epoch_ledger_keeps_existing_violations_and_records_only_regressions() {
    for (previous, current, regressed) in [
        (0, 0, false),
        (0, 1, false),
        (1, 0, true),
        (i16::MIN, i16::MAX, false),
        (i16::MAX, i16::MIN, true),
    ] {
        let mut ledger = BTreeSet::from(["existing violation"]);
        record_epoch_regression(previous, current, &mut ledger, "epoch regression");
        record_epoch_regression(previous, current, &mut ledger, "epoch regression");
        assert!(ledger.contains("existing violation"));
        assert!(ledger.contains("epoch regression") == regressed);
        assert!(ledger.len() == if regressed { 2 } else { 1 });
    }
}
