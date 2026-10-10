//! Behavioural tests for the KIP-211 sweep, driven against a running broker.
//!
//! Each test commits through the real `OffsetCommit` handler, so the offsets
//! land in a real `__consumer_offsets` partition, and reads back through the
//! real `OffsetFetch` handler. The sweep's clock is a parameter, so a test
//! moves time forward by passing a later `now_ms` rather than sleeping.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_protocol::owned::{
    create_topics_request::{self},
    leave_group_request::LeaveGroupRequest,
    offset_commit_request::OffsetCommitRequest,
};
use krabka_units::mebibytes;
use tokio::sync::oneshot;

use super::sweep;
use crate::{
    broker::Broker,
    codes,
    coordinator::{
        bootstrap::OFFSETS_TOPIC,
        partitioner::partition_for_group,
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            GroupType,
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::{ClassicGroup, GroupState, OffsetEntry},
            group::{CoordinatorGroup, GroupKind},
        },
    },
    test_support::{
        UnixMillis, dispatch_context, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

const GROUP: &str = "retention-group";
const TOPIC: &str = "orders";
const MEMBER: &str = "m1";
const GENERATION: i32 = 1;
/// `retention_time_ms` reaches the broker at v2-v4 only, so the commits here
/// run at v4.
const COMMIT_VERSION: i16 = 4;
/// The legacy single-group `OffsetFetch` path.
const FETCH_VERSION: i16 = 7;
/// One minute of retention keeps the arithmetic readable.
const RETENTION_MS: i64 = 60_000;
/// The sweep interval a test runs with, and so the grace a memberless group
/// gets before the pass may delete it for holding no offsets.
const CHECK_INTERVAL_MS: i64 = 600_000;

/// Sweep locally owned groups with the retention settings shared by these cases.
async fn sweep_at(broker: &Broker, now_ms: i64) -> Vec<(String, super::ReapOutcome)> {
    sweep(
        &broker.group_coordinator,
        |_| true,
        now_ms,
        RETENTION_MS,
        CHECK_INTERVAL_MS,
    )
    .await
}

/// Independent expected result for a pass that deletes the group.
fn deleted_group(reaped: Vec<(String, i32)>) -> Vec<(String, super::ReapOutcome)> {
    vec![(
        GROUP.to_string(),
        super::ReapOutcome {
            reaped,
            group_deleted: true,
        },
    )]
}

/// Start a broker that holds `TOPIC`, since `OffsetCommit` answers
/// `UNKNOWN_TOPIC_OR_PARTITION` for a topic the image does not hold.
async fn start() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (broker, dir) =
        start_broker_with_authorizer_no_audit(Arc::new(crate::authorizer::AllowAllAuthorizer))
            .await;
    broker.wait_until_group_coordinator_ready().await;
    let admin = principal("admin");
    let address = peer();
    let ctx = request_context(&admin, &address, "retention-admin");
    let request = crate::handlers::test_support::configured_topic_request(
        crate::handlers::test_support::CreateTopicSetup {
            topic: TOPIC,
            ..Default::default()
        },
    );
    dispatch_context(
        &broker.broker_arc_for_test(),
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    (broker, dir)
}

/// Leave a real group memberless with its committed offset still retained.
async fn memberless_group_with_committed_offset() -> (
    crate::broker::BrokerHandle,
    tempfile::TempDir,
    std::sync::Arc<Broker>,
) {
    let (handle, dir) = start().await;
    let broker = handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;
    remove_last_member(&broker).await;
    (handle, dir, broker)
}

/// Install a `Stable` classic group holding one member, so a commit fences
/// against a real membership and a leave really empties the group.
fn seed_group_with_member(broker: &Broker) {
    let mut state = ClassicGroup::new(GROUP);
    state.protocol_type = Some("consumer".into());
    state.add_member(crate::coordinator::unified::actor::test_support::classic_member(MEMBER));
    state.state = GroupState::Stable;
    state.generation_id = GENERATION;
    let group = CoordinatorGroup::seeded(
        GROUP,
        GroupKind::Classic(state),
        std::collections::HashMap::new(),
    );
    broker
        .group_coordinator
        .seed_classic(GROUP, Box::new(group));
}

/// Seed a group the way bootstrap replay leaves one after a restart: its
/// committed offsets are back, its members are not, and `empty_since_ms` holds
/// whatever moment the group's k2 snapshot carried — `None` for a group that
/// never wrote one.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ReplayedGroupSetup<'a> {
    protocol_type: Option<&'a str>,
    empty_since_ms: Option<UnixMillis>,
    #[default(UnixMillis(0))]
    commit_timestamp_ms: UnixMillis,
}

fn seed_replayed_group(broker: &Broker, setup: ReplayedGroupSetup<'_>) {
    let mut state = ClassicGroup::new(GROUP);
    state.protocol_type = setup.protocol_type.map(str::to_string);
    let mut group = CoordinatorGroup::seeded(
        GROUP,
        GroupKind::Classic(state),
        [(
            (TOPIC.to_string(), 0),
            OffsetEntry {
                offset: krabka_log::Offset(42),
                leader_epoch: -1,
                metadata: String::new(),
                commit_timestamp_ms: setup.commit_timestamp_ms.0,
                expire_timestamp_ms: None,
                topic_id: None,
            },
        )]
        .into(),
    );
    group.empty_since_ms = setup.empty_since_ms.map(|at| at.0);
    broker
        .group_coordinator
        .seed_classic(GROUP, Box::new(group));
}

/// The moment `GROUP`'s actor stamped the group memberless.
///
/// The actor refreshes `empty_since_ms` at the end of every turn, so this
/// read — a turn of its own — is ordered after the stamp of the turn before
/// it. A sweep driven from the returned instant therefore exercises exactly
/// the grace it passes, however late the runner scheduled the actor.
async fn empty_since_ms(broker: &Broker) -> i64 {
    let handle = broker.group_coordinator.find(GROUP).expect("group actor");
    let (reply, stamped) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::TestEmptySinceMs { reply })
        .await
        .expect("send TestEmptySinceMs");
    stamped
        .await
        .expect("TestEmptySinceMs reply")
        .expect("actor stamped its empty-since moment")
}

/// Commit one offset through the `OffsetCommit` handler.
async fn commit_offset(broker: &Broker, offset: i64, retention_time_ms: i64) {
    let request = OffsetCommitRequest {
        generation_id_or_member_epoch: GENERATION,
        member_id: MEMBER.into(),
        retention_time_ms,
        ..crate::coordinator::test_support::commit_request(GROUP, TOPIC, offset)
    };
    let response = commit_request(broker, request).await;
    let code = response.topics[0].partitions[0].error_code;
    assert!(code == codes::NONE, "commit failed with error_code {code}");
}

/// The committed offset the `OffsetFetch` handler reports, `-1` for none.
async fn fetched_offset(broker: &Broker) -> i64 {
    crate::coordinator::test_support::fetch_offset(broker, GROUP, TOPIC, FETCH_VERSION).await
}

/// Remove the group's last member through the classic `LeaveGroup` path.
async fn remove_last_member(broker: &Broker) {
    let handle = broker
        .group_coordinator
        .find(GROUP)
        .expect("seeded group actor");
    let (reply, result) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicLeave {
            req: LeaveGroupRequest {
                group_id: GROUP.into(),
                member_id: MEMBER.into(),
                ..Default::default()
            },
            version: 0,
            reply,
        })
        .await
        .expect("send ClassicLeave");
    result.await.expect("LeaveGroup reply");
}

/// Every record the group's `__consumer_offsets` partition holds, as
/// `(key, value)` with a `None` value for a tombstone.
fn offsets_log_records(broker: &Broker) -> Vec<(Key, Option<bytes::Bytes>)> {
    let partition_id = partition_for_group(&broker.controller.current_image(), GROUP);
    let partition = broker
        .partitions
        .get(OFFSETS_TOPIC, PartitionIndex(partition_id))
        .expect("offsets partition is materialized");
    let log = partition.log.lock().expect("log lock");
    let mut next = log.log_start_offset();
    let end = log.log_end_offset();
    let mut out = Vec::new();
    while next < end {
        let read = log.read(next, mebibytes(1)).expect("read offsets log");
        if read.batches.is_empty() {
            break;
        }
        for batch in &read.batches {
            for record in &batch.records {
                if let Some(key) = &record.key
                    && let Ok(key) = parse_key(key)
                {
                    out.push((key, record.value.clone()));
                }
            }
            next = krabka_log::Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
    out
}

fn committed_offset_key() -> Key {
    Key::OffsetCommit {
        group_id: GROUP.into(),
        topic: TOPIC.into(),
        partition: 0,
    }
}

/// `true` when the log holds a null-valued record for `key`.
fn has_tombstone(records: &[(Key, Option<bytes::Bytes>)], key: &Key) -> bool {
    records
        .iter()
        .any(|(logged, value)| logged == key && value.is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_group_loses_its_offsets_after_the_retention() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;
    assert!(fetched_offset(&broker).await == 42);

    remove_last_member(&broker).await;

    let now_ms = empty_since_ms(&broker).await + RETENTION_MS + 1;
    let swept = sweep_at(&broker, now_ms).await;

    assert!(swept == deleted_group(vec![(TOPIC.to_string(), 0)]));
    check_group_deleted(&broker).await;
    let records = offsets_log_records(&broker);
    check!(has_tombstone(&records, &committed_offset_key()));
    // The group held nothing else, so its own record went in the same pass.
    check!(has_tombstone(
        &records,
        &Key::GroupMetadata {
            group_id: GROUP.into()
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_group_keeps_its_offsets_across_the_same_interval() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, -1).await;

    // The same clock reading that reaped the empty group above, and then some.
    let now_ms = crate::time_util::now_ms() + RETENTION_MS * 10;
    let swept = sweep_at(&broker, now_ms).await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
    check!(!has_tombstone(
        &offsets_log_records(&broker),
        &committed_offset_key()
    ));
    check!(broker.group_coordinator.find(GROUP).is_some());
}

/// KIP-211: `retention_time_ms` on a v2-v4 commit overrides the broker's
/// `offsets.retention.minutes` for that one commit. The sweep runs with a
/// broker retention far longer than the per-commit value, so only a broker
/// that honours the request expires the offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_commit_retention_time_expires_before_the_broker_default() {
    const PER_COMMIT_MS: i64 = 1_000;
    const BROKER_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    seed_group_with_member(&broker);
    commit_offset(&broker, 42, PER_COMMIT_MS).await;
    remove_last_member(&broker).await;

    // The record carries the absolute expiry the request asked for.
    let records = offsets_log_records(&broker);
    let value = records
        .iter()
        .rev()
        .find_map(|(key, value)| (*key == committed_offset_key()).then_some(value.as_ref()?))
        .expect("committed offset record");
    let decoded = OffsetCommitValue::decode_value(value).expect("decode value");
    let expire_timestamp_ms = decoded
        .expire_timestamp_ms
        .expect("per-commit retention is persisted");
    check!(expire_timestamp_ms == decoded.commit_timestamp_ms + PER_COMMIT_MS);

    // Just before the per-commit expiry nothing goes, even though it is far
    // past nothing at all.
    let before = sweep(
        &broker.group_coordinator,
        |_| true,
        expire_timestamp_ms - 1,
        BROKER_RETENTION_MS,
        CHECK_INTERVAL_MS,
    )
    .await;
    check!(before.is_empty());

    let after = sweep(
        &broker.group_coordinator,
        |_| true,
        expire_timestamp_ms,
        BROKER_RETENTION_MS,
        CHECK_INTERVAL_MS,
    )
    .await;
    check!(after.len() == 1);
    check!(fetched_offset(&broker).await == -1);
}

/// A group whose `__consumer_offsets` partition this broker does not lead is
/// left alone: the leader sweeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_this_broker_does_not_own_is_not_swept() {
    let (_broker_handle, _dir, broker) = memberless_group_with_committed_offset().await;

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep(
        &broker.group_coordinator,
        |_| false,
        now_ms,
        RETENTION_MS,
        CHECK_INTERVAL_MS,
    )
    .await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
}

/// A streams group parks its committed offsets on a classic `groups` entry
/// while its members live in a streams actor, so the sweep must not read that
/// entry's emptiness as "nobody is using these offsets".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_streams_group_offset_home_is_not_swept() {
    let (_broker_handle, _dir, broker) = memberless_group_with_committed_offset().await;
    // Lock the id to the streams namespace, as a KIP-1071 group would.
    let _ = broker.group_coordinator.get_or_create_streams(GROUP);

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep_at(&broker, now_ms).await;

    assert!(swept.is_empty());
    check!(fetched_offset(&broker).await == 42);
}

/// The sweep only touches groups that hold an actor, so an id no request has
/// ever named is not swept at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_with_no_groups_sweeps_nothing() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();

    let now_ms = crate::time_util::now_ms() + RETENTION_MS + 1;
    let swept = sweep_at(&broker, now_ms).await;

    assert!(swept.is_empty());
}

/// A memberless group that keeps no committed offset is dead, and the sweep
/// tombstones it whether or not this pass expired anything.
///
/// Kafka's `GroupMetadataManager.cleanupGroupMetadata` transitions every
/// `Empty` group with no offsets to `Dead` and appends its tombstone, on the
/// same pass that expires offsets. Verified against `apache/kafka:4.3.1`: a
/// group left with no members and no offsets by `kafka-consumer-groups
/// --delete-offsets` is gone from `--list` after one
/// `offsets.retention.check.interval.ms`. Without this the group's record and
/// its actor sit in `__consumer_offsets` and `ListGroups` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_group_that_holds_no_offsets_is_reaped() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    let _ = broker
        .group_coordinator
        .get_or_create_group(GROUP, GroupKindTag::Classic);

    // No offset's age can save it. It waits out one sweep interval all the
    // same, so an actor a request has only just spawned is not reaped before
    // its first message lands.
    let now_ms = crate::time_util::now_ms();
    check!(
        sweep_at(&broker, now_ms).await.is_empty(),
        "a group that has only just been created is left alone"
    );

    // Sweep one whole interval after the actor's own stamp, not after a clock
    // this test reads. The stamp lands on the actor's turn, which a loaded
    // machine can schedule after any instant read here; measuring from the
    // stamp is what makes the grace exactly one interval either way.
    let swept = sweep_at(&broker, empty_since_ms(&broker).await + CHECK_INTERVAL_MS).await;

    assert!(swept == deleted_group(Vec::new()));
    check!(broker.group_coordinator.find(GROUP).is_none());
    check!(has_tombstone(
        &offsets_log_records(&broker),
        &Key::GroupMetadata {
            group_id: GROUP.into()
        }
    ));
}

/// The same rule after an `OffsetDelete` takes a group's last offset: the
/// group itself goes on the next pass, even though the pass expired nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_whose_last_offset_was_deleted_is_reaped_next_pass() {
    let (_broker_handle, _dir, broker) = memberless_group_with_committed_offset().await;

    // Take the offset out from under the group the way `OffsetDelete` does,
    // leaving a memberless group that holds nothing.
    let handle = broker.group_coordinator.find(GROUP).expect("group actor");
    let (reply, done) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::RemoveCommitted {
            keys: vec![(TOPIC.to_string(), 0)],
            reply,
        })
        .await
        .expect("send RemoveCommitted");
    done.await.expect("RemoveCommitted reply");

    let swept = sweep_at(&broker, crate::time_util::now_ms() + CHECK_INTERVAL_MS).await;

    assert!(swept == deleted_group(Vec::new()));
    check!(broker.group_coordinator.find(GROUP).is_none());
}

/// A request that replaces the reaped actor between the reap and the registry
/// cleanup keeps its own auxiliary state.
///
/// `forget_group` clears the protocol-type lock that routing and KIP-848
/// migration read, and the durable seed a later respawn replays from. Both
/// belong to whatever handle the registry holds, so clearing them when the
/// conditional removal kept a fresh handle strips state the replacement is
/// already serving on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacement_actor_keeps_its_type_and_seed_when_the_reaped_one_is_forgotten() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    let coordinator = &broker.group_coordinator;

    // The handle the sweep reaped, now stale.
    let reaped = coordinator.get_or_create_group(GROUP, GroupKindTag::Classic);
    // A `ConsumerGroupHeartbeat` got in first: it installed a fresh actor
    // under the same id, locked the id to the next-gen namespace, and left a
    // seed for a later respawn.
    coordinator.groups.remove(GROUP);
    let replacement = coordinator.get_or_create_group(GROUP, GroupKindTag::Consumer);
    coordinator.mark_next_gen(GROUP);
    coordinator.seeds.insert(
        GROUP.to_string(),
        crate::coordinator::unified::GroupSeed::default(),
    );

    super::forget_group(coordinator, GROUP, &reaped);

    check!(coordinator.group_type(GROUP) == Some(GroupType::NextGen));
    check!(coordinator.seeds.contains_key(GROUP));
    let live = coordinator.find(GROUP).expect("the replacement survives");
    check!(Arc::ptr_eq(&live, &replacement));
}

/// A restart cannot restore a group-empty moment that was never written down.
/// A simple group — one that only ever committed offsets, so it took no
/// protocol type and wrote no k2 snapshot — measures its retention from the
/// commit, which is what Kafka's `ClassicGroup.offsetExpirationCondition` does
/// for it. Measuring from the moment this process happened to start the actor
/// instead would hand the group a fresh `offsets.retention.minutes` on every
/// restart, and a broker restarted more often than the retention would never
/// reap it at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_simple_group_expires_from_its_commit_not_from_the_restart() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    let restarted_at = crate::time_util::now_ms();
    seed_replayed_group(
        &broker,
        ReplayedGroupSetup {
            commit_timestamp_ms: UnixMillis(restarted_at - RETENTION_MS * 10),
            ..Default::default()
        },
    );

    let swept = sweep_at(&broker, restarted_at).await;

    assert!(swept == deleted_group(vec![(TOPIC.to_string(), 0)]));
    check_group_deleted(&broker).await;
}

/// The other half of that rule. A classic group some consumer joined carries a
/// protocol type, and Kafka measures its retention from the moment it went
/// empty — a moment its memberless k2 snapshot persists, so a restart does not
/// move it. A commit far older than the retention therefore survives until the
/// group itself has been empty that long.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_joined_group_expires_from_the_moment_it_emptied() {
    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();
    let emptied_at = crate::time_util::now_ms();
    seed_replayed_group(
        &broker,
        ReplayedGroupSetup {
            protocol_type: Some("consumer"),
            empty_since_ms: Some(UnixMillis(emptied_at)),
            commit_timestamp_ms: UnixMillis(emptied_at - RETENTION_MS * 10),
        },
    );

    let early = sweep_at(&broker, emptied_at + RETENTION_MS - 1).await;
    assert!(early.is_empty());
    check!(fetched_offset(&broker).await == 42);

    let late = sweep_at(&broker, emptied_at + RETENTION_MS).await;
    assert!(late == deleted_group(vec![(TOPIC.to_string(), 0)]));
    check!(fetched_offset(&broker).await == -1);
}

/// A commit the broker acknowledged is never deleted by a sweep running
/// beside it.
///
/// The two writes have to be ordered by the group's actor. The sweep decides
/// what to tombstone from the actor's in-memory offsets, so a commit that
/// appended its record outside the mailbox and queued the in-memory update
/// afterwards leaves a window: the sweep reads the stale, expired entry,
/// tombstones the key behind the newer record, deletes the group, and stops
/// the actor with the queued update still in flight. The client is told the
/// commit succeeded and the offset is gone.
///
/// Each round seeds a memberless group holding one long-expired offset — the
/// state in which the very next sweep reaps — and starts a commit against it,
/// then sweeps while that commit is in flight. Whatever order the two land
/// in, an acknowledged commit must be readable afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_commit_is_never_reaped_by_a_concurrent_sweep() {
    const ROUNDS: usize = 16;
    const NEW_OFFSET: i64 = 99;

    let (broker_handle, _dir) = start().await;
    let broker = broker_handle.broker_arc_for_test();

    for round in 0..ROUNDS {
        let group = format!("racer-{round}");
        let now = crate::time_util::now_ms();
        // Memberless, and its one offset committed long enough ago that the
        // next sweep takes it and the group with it.
        broker.group_coordinator.seed_classic(
            &group,
            Box::new(CoordinatorGroup::seeded(
                group.clone(),
                GroupKind::Classic(ClassicGroup::new(&group)),
                [(
                    (TOPIC.to_string(), 0),
                    OffsetEntry {
                        offset: krabka_log::Offset(1),
                        leader_epoch: -1,
                        metadata: String::new(),
                        commit_timestamp_ms: now - RETENTION_MS * 10,
                        expire_timestamp_ms: None,
                        topic_id: None,
                    },
                )]
                .into(),
            )),
        );

        let committing = {
            let broker = Arc::clone(&broker);
            let group = group.clone();
            tokio::spawn(async move { simple_commit(&broker, &group, NEW_OFFSET).await })
        };
        // Sweep for as long as the commit is in flight, so the pass lands in
        // every window the commit passes through.
        let coordinator = &broker.group_coordinator;
        while !committing.is_finished() {
            let _ = sweep(
                coordinator,
                |id| id == group,
                now,
                RETENTION_MS,
                CHECK_INTERVAL_MS,
            )
            .await;
            tokio::task::yield_now().await;
        }

        let code = committing.await.expect("commit task");
        if code == codes::NONE {
            check!(
                simple_fetch(&broker, &group).await == NEW_OFFSET,
                "round {round}: the commit was acknowledged but the offset is gone"
            );
        }
    }
}

/// Commit one offset for `group` through the `OffsetCommit` handler as a
/// simple consumer, and return the per-partition error code.
async fn simple_commit(broker: &Broker, group: &str, offset: i64) -> i16 {
    let request = crate::coordinator::test_support::commit_request(group, TOPIC, offset);
    let response = commit_request(broker, request).await;
    response.topics[0].partitions[0].error_code
}

/// The committed offset `OffsetFetch` reports for `group`, `-1` for none.
async fn simple_fetch(broker: &Broker, group: &str) -> i64 {
    crate::coordinator::test_support::fetch_offset(broker, group, TOPIC, FETCH_VERSION).await
}

async fn commit_request(
    broker: &Broker,
    request: krabka_protocol::owned::offset_commit_request::OffsetCommitRequest,
) -> krabka_protocol::owned::offset_commit_response::OffsetCommitResponse {
    let principal = principal("admin");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "consumer");
    crate::handlers::offset_commit::handle(broker, request, COMMIT_VERSION, &ctx)
        .await
        .expect("OffsetCommit")
}

/// Check the directory before fetching: an unknown id creates another actor.
async fn check_group_deleted(broker: &Broker) {
    check!(broker.group_coordinator.find(GROUP).is_none());
    check!(fetched_offset(broker).await == -1);
}
