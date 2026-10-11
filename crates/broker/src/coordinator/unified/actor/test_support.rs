//! Shared unit-test fixtures for the group-actor modules: a static metadata
//! provider, coordinator builders, classic-group seeders, and the offsets-log
//! readers that several actor submodules assert against.

use std::{collections::HashMap, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, primitives::uuid::Uuid,
};

use crate::coordinator::unified::{
    ClientIdentity, classic_state::Member, config::ConsumerGroupMigrationPolicy,
    consumer_state::MemberState, offsets_log::fake::ClassicMetadataRecord,
    persistence::GroupMetadataValue, share::config::ShareGroupConfig,
    streams::config::StreamsGroupConfig, test_support::MemberEpoch,
};

pub(crate) mod rpc;

use super::{GroupActorHandle, MetadataProvider};
use crate::{
    codes,
    coordinator::unified::{
        GroupCoordinator,
        config::NextGenConfig,
        consumer_state::GroupState,
        group::{CoordinatorGroup, GroupKind},
        offsets_log::fake::InMemoryOffsetsLog,
        reconciler::ReconcileInput,
    },
};

#[derive(Debug)]
pub(super) struct StaticMetadata {
    pub(super) input: ReconcileInput,
}
impl MetadataProvider for StaticMetadata {
    fn snapshot(&self) -> ReconcileInput {
        self.input.clone()
    }
}

/// A reconciler image containing one named topic with the supplied wire id.
pub(crate) fn topic_reconcile_input(
    topic: &str,
    topic_id: Uuid,
    partitions: i32,
) -> ReconcileInput {
    ReconcileInput {
        topic_id_by_name: [(topic.to_string(), topic_id)].into(),
        partitions_per_topic: [(topic_id, partitions)].into(),
        ..ReconcileInput::default()
    }
}

pub(crate) fn empty_metadata() -> Arc<dyn MetadataProvider> {
    Arc::new(StaticMetadata {
        input: ReconcileInput::default(),
    })
}

pub(super) fn coordinator_with_log(
    config: NextGenConfig,
    metadata: Arc<dyn MetadataProvider>,
    log: Arc<InMemoryOffsetsLog>,
) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        config,
        ShareGroupConfig::assigning_at_once(),
        metadata,
        log,
        StreamsGroupConfig::default(),
    ))
}

pub(super) fn make_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_config(NextGenConfig::assigning_at_once())
}

/// As [`make_coordinator`], but with an explicit consumer-group config.
pub(super) fn make_coordinator_with_config(
    config: NextGenConfig,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = coordinator_with_log(config, empty_metadata(), log.clone());
    (coord, log)
}

/// Consumer members with the actor tests' ordinary subscription and client metadata.
pub(super) fn subscribed_consumer_group(
    group_id: &str,
    member_ids: &[&str],
    topics: &[&str],
) -> GroupState {
    let mut state = GroupState::new(group_id);
    for member_id in member_ids {
        state.add_or_update_member(subscribed_member(ConsumerMemberSetup {
            member_id,
            topics,
            client: ClientIdentity {
                id: "client",
                host: "host",
            },
            ..Default::default()
        }));
    }
    state
}

/// A subscribing consumer with the actor fixtures' ordinary rebalance timeout.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ConsumerMemberSetup<'a> {
    #[default("m1")]
    pub member_id: &'a str,
    #[default(&["t"])]
    pub topics: &'a [&'a str],
    #[default(crate::coordinator::unified::ClientIdentity { id: "client", host: "host" })]
    pub client: ClientIdentity<'a>,
    #[default(std::time::Instant::now())]
    pub now: std::time::Instant,
}

pub(super) fn subscribed_member(setup: ConsumerMemberSetup<'_>) -> MemberState {
    let ConsumerMemberSetup {
        member_id,
        topics,
        client,
        now,
    } = setup;
    super::member_state::build_member(
        member_id,
        &ConsumerGroupHeartbeatRequest {
            subscribed_topic_names: Some(topics.iter().map(|topic| (*topic).into()).collect()),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
        client,
        now,
    )
}

pub(super) fn completing_classic_group(member_ids: &[&str]) -> CoordinatorGroup {
    use super::super::classic_state::{ClassicGroup as ClassicState, Member};

    let mut state = ClassicState::new("g");
    state.protocol_type = Some("consumer".into());
    for member_id in member_ids {
        state.add_member(Member::new(
            *member_id,
            "client",
            "host",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::from_static(b"subscription"))],
        ));
    }
    state.resolve_selected_protocol_metadata("range");
    state.complete_rebalance("range");
    CoordinatorGroup::seeded("g", GroupKind::Classic(state), HashMap::new())
}

/// Borrow the services exactly as a live actor turn does.
pub(super) fn actor_services<'a>(
    coordinator: &'a Arc<GroupCoordinator>,
    offsets_log: &'a InMemoryOffsetsLog,
) -> super::ActorServices<'a> {
    super::ActorServices {
        config: &coordinator.config,
        metadata: coordinator.metadata.as_ref(),
        offsets_log,
        coordinator,
    }
}

pub(super) fn seed_classic_group(
    coordinator: &Arc<GroupCoordinator>,
    group: CoordinatorGroup,
) -> (Arc<GroupActorHandle>, i32) {
    let generation = group.as_classic().unwrap().generation_id;
    coordinator.seed_classic("g", Box::new(group));
    (coordinator.find("g").unwrap(), generation)
}

pub(super) fn seed_completing_classic(
    coordinator: &Arc<GroupCoordinator>,
    members: &[&str],
) -> (Arc<GroupActorHandle>, i32) {
    seed_classic_group(coordinator, completing_classic_group(members))
}

pub(super) async fn last_classic_metadata(log: &InMemoryOffsetsLog) -> GroupMetadataValue {
    use crate::coordinator::unified::persistence::{GroupMetadataValue, Key, parse_key};

    for batch in log.batches().await.iter().rev() {
        for record in batch.records.iter().rev() {
            if record.key.as_ref().is_some_and(|key| {
                matches!(
                    parse_key(key),
                    Ok(Key::GroupMetadata { group_id: ref id }) if id == "g"
                )
            }) {
                return GroupMetadataValue::decode_value(
                    record.value.as_deref().expect("classic metadata value"),
                )
                .expect("valid classic metadata");
            }
        }
    }
    panic!("classic metadata record not found")
}

/// A coordinator whose metadata image holds one topic `t` with `partitions`
/// partitions, so the reconciler can resolve a `t` subscription to real
/// topic-id/partitions and compute a target assignment.
pub(super) fn make_coordinator_with_topic(
    topic: &str,
    partitions: i32,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_topic_policy(topic, partitions, ConsumerGroupMigrationPolicy::default())
}

/// A two-partition topic `t` whose classic and consumer groups can migrate both ways.
pub(super) fn bidirectional_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional)
}

/// One classic member using the common retention/dispatch test identity.
pub(crate) fn classic_member(member_id: &str) -> Member {
    Member::new(
        member_id,
        "client",
        "127.0.0.1",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), bytes::Bytes::new())],
    )
}

/// One follower already parked for a classic sync reply.
pub(super) fn parked_follower(
    member_id: &str,
) -> (
    super::ParkedWaiters,
    tokio::sync::oneshot::Receiver<super::SyncResult>,
) {
    let (reply, response) = tokio::sync::oneshot::channel();
    let mut parked = super::ParkedWaiters::default();
    parked.followers.insert(member_id.into(), reply);
    (parked, response)
}

/// As [`make_coordinator_with_topic`], but with an explicit migration
/// policy. Hosted-classic tests pin `Upgrade` so that the native member's
/// leave in `seed_and_upgrade` does NOT trigger a downgrade back to
/// classic, which would strand them on the wrong RPC path. The tests
/// exercise the downgrade trigger itself with `Bidirectional` and
/// `Downgrade`.
pub(super) fn make_coordinator_with_topic_policy(
    topic: &str,
    partitions: i32,
    policy: ConsumerGroupMigrationPolicy,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_topic_config(
        topic,
        partitions,
        NextGenConfig {
            migration_policy: policy,
            ..NextGenConfig::assigning_at_once()
        },
    )
}

/// As [`make_coordinator_with_topic`], but with an explicit consumer-group
/// config.
pub(super) fn make_coordinator_with_topic_config(
    topic: &str,
    partitions: i32,
    config: NextGenConfig,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let topic_id = Uuid([7; 16]);
    let input = ReconcileInput {
        topic_id_by_name: [(topic.to_string(), topic_id)].into(),
        partitions_per_topic: [(topic_id, partitions)].into(),
        ..Default::default()
    };
    let metadata: Arc<dyn MetadataProvider> = Arc::new(StaticMetadata { input });
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = coordinator_with_log(config, metadata, log.clone());
    (coord, log)
}

// ── KIP-848: serving hosted classic members off the reconciler ─────

/// A real classic consumer client's `JoinGroup` protocol metadata: a
/// `ConsumerProtocolSubscription` with the leading version-negotiation
/// prefix.
pub(crate) fn subscription_blob(topics: &[&str]) -> Bytes {
    subscription_blob_at(0, topics)
}

/// Classic subscription metadata with its negotiated version prefix and body.
pub(crate) fn subscription_blob_at(version: i16, topics: &[&str]) -> Bytes {
    use bytes::{BufMut, BytesMut};
    use krabka_protocol::{
        Encode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
    };
    let sub = ConsumerProtocolSubscription {
        topics: topics.iter().map(|s| (*s).to_string()).collect(),
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(version);
    sub.encode(&mut out, version).unwrap();
    out.freeze()
}

/// Decode a `SyncGroup` assignment blob (version prefix + body) back into a
/// `ConsumerProtocolAssignment`.
pub(super) fn decode_assignment(
    blob: &Bytes,
) -> krabka_protocol::owned::consumer_protocol_assignment::ConsumerProtocolAssignment {
    use bytes::Buf;
    use krabka_protocol::{
        Decode, owned::consumer_protocol_assignment::ConsumerProtocolAssignment,
    };
    let mut cur = &blob[..];
    let version = cur.get_i16();
    ConsumerProtocolAssignment::decode(&mut cur, version).expect("assignment decodes")
}

/// Seeds a classic consumer group with member `m-classic` subscribed to
/// `topic`, then upgrades it in place with a native consumer heartbeat.
/// After this returns, the group is consumer-kind and `m-classic` has a
/// target.
pub(super) async fn seed_and_upgrade(
    coord: &Arc<GroupCoordinator>,
    topic: &str,
) -> Arc<GroupActorHandle> {
    let handle = seed_classic_member(
        coord,
        ClassicMemberSetup {
            topic,
            ..Default::default()
        },
    );

    upgrade_with_transient_native(&handle, topic).await;
    handle
}

/// The two-partition topic fixture with classic-to-consumer upgrades enabled.
pub(super) fn upgrade_coordinator() -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Upgrade)
}

/// Upgrade the fixture's hosted member, then rejoin it on the classic facade.
pub(super) async fn upgrade_and_rejoin_classic(
    coordinator: &Arc<GroupCoordinator>,
) -> (Arc<GroupActorHandle>, super::JoinResult) {
    let handle = seed_and_upgrade(coordinator, "t").await;
    let joined = rpc::classic_join(&handle, "m-classic", "t").await;
    (handle, joined)
}

/// Drive an in-place upgrade with a native join, then have that member leave.
pub(super) async fn upgrade_with_transient_native(handle: &Arc<GroupActorHandle>, topic: &str) {
    // Native consumer heartbeat triggers the in-place upgrade and the
    // reconcile that gives m-classic a target.
    let resp = rpc::consumer_heartbeat(handle, "", 0, Some(topic)).await;
    assert!(resp.error_code == codes::NONE);

    // The native heartbeat minted a transient consumer member to drive the
    // upgrade. Have it leave so the group hosts only the classic member(s)
    // under test — otherwise it would claim a share of the partitions.
    let native_id = resp.member_id.expect("native member id");
    assert!(
        rpc::consumer_request(
            handle,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: native_id,
                member_epoch: -1,
                ..Default::default()
            }
        )
        .await
        .error_code
            == codes::NONE
    );
}

/// `true` if and only if some appended record WRITES a classic k2
/// `GroupMetadata` for `group_id` with a non-null value.
pub(super) async fn log_has_classic_group_metadata_write(
    log: &InMemoryOffsetsLog,
    group_id: &str,
) -> bool {
    log.has_classic_group_metadata_record(group_id, ClassicMetadataRecord::Write)
        .await
}

/// Seeds a classic consumer group "g" with a single classic member
/// `member_id` subscribed to `topic`, and with an optional KIP-345 static
/// `group_instance_id`. It mirrors the inline seeding that the upgrade and
/// downgrade tests use, but it takes parameters, so a static-identity test
/// can attach an instance id. The fixed `m-classic` in `seed_and_upgrade`
/// cannot do that.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ClassicMemberSetup<'a> {
    #[default("m-classic")]
    pub member_id: &'a str,
    #[default("t")]
    pub topic: &'a str,
    pub instance_id: Option<&'a str>,
}

impl<'a> ClassicMemberSetup<'a> {
    /// A dynamic member with the default topic subscription and generation.
    pub(super) fn dynamic(member_id: &'a str) -> Self {
        Self {
            member_id,
            ..Default::default()
        }
    }
}

pub(super) fn seed_classic_member(
    coord: &Arc<GroupCoordinator>,
    setup: ClassicMemberSetup<'_>,
) -> Arc<GroupActorHandle> {
    use super::super::{
        classic_state::{ClassicGroup as ClassicState, Member},
        group::{CoordinatorGroup, GroupKind},
    };
    let ClassicMemberSetup {
        member_id,
        topic,
        instance_id,
    } = setup;

    let mut cs = ClassicState::new("g");
    cs.protocol_type = Some("consumer".into());
    cs.generation_id = 1;
    cs.add_member(
        Member::new(
            member_id,
            "client",
            "127.0.0.1",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_mins(1),
            vec![("range".into(), subscription_blob(&[topic]))],
        )
        .with_instance_id(instance_id.map(str::to_string)),
    );
    let group = Box::new(CoordinatorGroup::seeded(
        "g",
        GroupKind::Classic(cs),
        HashMap::new(),
    ));
    coord.seed_classic("g", group);
    coord.find("g").expect("seeded classic actor")
}

/// Seed a stable classic group and report the generation before a leave.
pub(super) fn seed_stable_classic(
    coordinator: &Arc<GroupCoordinator>,
    members: &[&str],
) -> (Arc<GroupActorHandle>, i32) {
    let mut group = completing_classic_group(members);
    group.as_classic_mut().unwrap().state = super::super::classic_state::GroupState::Stable;
    seed_classic_group(coordinator, group)
}

/// Spawn consumer-kind, host a classic member, then downgrade. The inspect
/// reply is the barrier after the leave, so callers see the live classic kind.
pub(super) async fn spawn_and_downgrade(
    coordinator: &Arc<GroupCoordinator>,
) -> (Arc<GroupActorHandle>, super::ClassicView) {
    let handle = coordinator.get_or_create_consumer("g");
    assert!(
        handle.kind == super::GroupKindTag::Consumer,
        "the group must be spawned consumer-kind"
    );
    let joined = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(joined.error_code == codes::NONE);
    let native = joined.member_id.expect("native member id");
    let hosted = rpc::classic_join(&handle, "m-classic", "t").await;
    assert!(hosted.error_code == codes::NONE);
    let left = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
    assert!(left.error_code == codes::NONE);
    let view = rpc::classic_inspect(&handle).await;
    (handle, view)
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct BidirectionalClassicSetup<'a> {
    #[default("m1")]
    pub member_id: &'a str,
    pub instance_id: Option<&'a str>,
}

/// A bidirectional coordinator whose first actor was spawned from a classic seed.
pub(super) fn seeded_bidirectional_coordinator(
    setup: BidirectionalClassicSetup<'_>,
) -> (
    Arc<GroupCoordinator>,
    Arc<InMemoryOffsetsLog>,
    Arc<GroupActorHandle>,
) {
    let (coordinator, log) = bidirectional_coordinator();
    let handle = seed_classic_member(
        &coordinator,
        ClassicMemberSetup {
            member_id: setup.member_id,
            instance_id: setup.instance_id,
            ..Default::default()
        },
    );
    (coordinator, log, handle)
}

pub(super) struct JoinedNativeConsumer {
    pub member_id: String,
    pub epoch: MemberEpoch,
}

/// Join a group's first native consumer and retain its assigned identity and epoch.
pub(super) async fn join_native_consumer(handle: &Arc<GroupActorHandle>) -> JoinedNativeConsumer {
    let response = rpc::consumer_heartbeat(handle, "", 0, Some("t")).await;
    assert!(response.error_code == codes::NONE);
    JoinedNativeConsumer {
        member_id: response.member_id.expect("native member id"),
        epoch: MemberEpoch(response.member_epoch),
    }
}

/// Seed a convertible classic member and join its first native consumer.
pub(super) async fn seed_classic_with_native(
    coordinator: &Arc<GroupCoordinator>,
) -> (Arc<GroupActorHandle>, String) {
    let handle = seed_classic_member(coordinator, ClassicMemberSetup::default());
    let native = join_native_consumer(&handle).await;
    (handle, native.member_id)
}

/// A marked classic handle, in the same get-before-mark order as coordinator dispatch.
pub(super) fn marked_classic_handle(
    coord: &Arc<GroupCoordinator>,
    group_id: &str,
) -> Arc<GroupActorHandle> {
    let handle = coord.get_or_create_classic(group_id);
    coord.mark_classic(group_id);
    handle
}

/// The bidirectional migration fixture with both a hosted classic and a native consumer.
pub(super) async fn bidirectional_with_members() -> (
    Arc<GroupCoordinator>,
    Arc<InMemoryOffsetsLog>,
    Arc<GroupActorHandle>,
    String,
) {
    let (coord, log) = bidirectional_coordinator();
    let (handle, native) = seed_classic_with_native(&coord).await;
    (coord, log, handle, native)
}
