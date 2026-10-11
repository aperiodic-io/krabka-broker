//! Tests for the rebuild of a classic group's members and state from a
//! persisted `GroupMetadata` value.

use std::{collections::HashMap, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    records::RecordBatch,
};
use tokio::sync::oneshot;

use super::{
    apply::apply_group_metadata,
    replay::{Replayed, apply_record, finalize},
    test_support::{bare_coordinator, classic_group_record},
};
use crate::{
    codes,
    coordinator::{
        persistence::{self, GroupMetadataValue, MemberMetadata},
        unified::{
            actor::{GroupActorHandle, GroupActorMessage, JoinResult, JoinResultMember},
            classic_state::{
                ClassicGroup as ClassicState, GroupState as ClassicGroupState, Member,
            },
            regex_resolver::no_topic_regex_resolver,
        },
    },
};

fn stored_member(
    group_instance_id: Option<&str>,
    rebalance_timeout_ms: i32,
    session_timeout_ms: i32,
) -> MemberMetadata {
    MemberMetadata {
        member_id: "m1".into(),
        group_instance_id: group_instance_id.map(str::to_string),
        client_id: "c".into(),
        client_host: "h".into(),
        rebalance_timeout_ms,
        session_timeout_ms,
        subscription: Bytes::from_static(b"sub"),
        assignment: Bytes::from_static(b"asn"),
    }
}

fn stored_group(
    protocol_type: &str,
    protocol_name: Option<&str>,
    members: Vec<MemberMetadata>,
) -> GroupMetadataValue {
    GroupMetadataValue {
        protocol_type: protocol_type.into(),
        generation: 5,
        protocol_name: protocol_name.map(str::to_string),
        leader: (!members.is_empty()).then(|| "m1".to_string()),
        current_state_timestamp_ms: 0,
        members,
    }
}

// The member `m1` of `stored_member`, as replay should rebuild it.
fn loaded_member(
    group_instance_id: Option<&str>,
    rebalance_timeout: Duration,
    session_timeout: Duration,
) -> Member {
    Member {
        id: "m1".into(),
        group_instance_id: group_instance_id.map(str::to_string),
        client_id: "c".into(),
        host: "h".into(),
        session_timeout,
        rebalance_timeout,
        last_heartbeat: std::time::Instant::now(),
        protocol_metadata: Bytes::from_static(b"sub"),
        protocols: vec![("range".into(), Bytes::from_static(b"sub"))],
        assignment: Some(Bytes::from_static(b"asn")),
        is_new: false,
    }
}

/// Independently expected stable group rebuilt from the row's stored member fields.
fn loaded_group(
    instance_id: Option<&str>,
    rebalance_timeout: Duration,
    session_timeout: Duration,
) -> ClassicState {
    ClassicState {
        state: ClassicGroupState::Stable,
        protocol_type: Some("consumer".into()),
        generation_id: 5,
        leader_id: Some("m1".into()),
        protocol_name: Some("range".into()),
        members: HashMap::from([(
            "m1".to_string(),
            loaded_member(instance_id, rebalance_timeout, session_timeout),
        )]),
        static_members: instance_id
            .into_iter()
            .map(|id| (id.to_string(), "m1".to_string()))
            .collect(),
        ..ClassicState::new("g")
    }
}

// Kafka's `GroupMetadataManager.replay(GroupMetadataKey, GroupMetadataValue)`:
// every loaded member supports the selected protocol with its stored
// subscription, a stored rebalance timeout of -1 becomes the session timeout,
// and an empty protocol type becomes no protocol type.
#[test]
fn replay_rebuilds_the_group_as_kafka_loads_it() {
    let secs = Duration::from_secs;
    let cases = [
        (
            "a static member with a stored rebalance timeout",
            stored_group(
                "consumer",
                Some("range"),
                vec![stored_member(Some("inst"), 45_000, 30_000)],
            ),
            loaded_group(Some("inst"), secs(45), secs(30)),
        ),
        (
            "a member whose stored rebalance timeout is -1",
            stored_group(
                "consumer",
                Some("range"),
                vec![stored_member(None, -1, 30_000)],
            ),
            loaded_group(None, secs(30), secs(30)),
        ),
        (
            "an empty group with an empty protocol type",
            stored_group("", None, vec![]),
            ClassicState {
                generation_id: 5,
                ..ClassicState::new("g")
            },
        ),
    ];
    // Every row is replayed before the one comparison, so a failure shows
    // each row that differs.
    let mut replayed_rows = Vec::new();
    let mut expected_rows = Vec::new();
    for (name, value, mut expected) in cases {
        let mut replayed = ClassicState::new("g");
        apply_group_metadata(&mut replayed, value, 0);
        // Replay stamps each member's last heartbeat with the load time.
        for (member_id, member) in &mut expected.members {
            if let Some(loaded) = replayed.members.get(member_id) {
                member.last_heartbeat = loaded.last_heartbeat;
            }
        }
        replayed_rows.push((name, replayed));
        expected_rows.push((name, expected));
    }
    assert!(replayed_rows == expected_rows);
}

// Queues a `JoinGroup` v5 from `member_id` that proposes `range` with
// `metadata`, and returns the channel its reply arrives on.
async fn queue_join(
    handle: &GroupActorHandle,
    member_id: &str,
    metadata: &'static [u8],
) -> oneshot::Receiver<JoinResult> {
    let (reply, joined) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicJoin {
            req: JoinGroupRequest {
                group_id: "g".into(),
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                member_id: member_id.into(),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: Bytes::from_static(metadata),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 5,
            client_id: "client-a".into(),
            client_host: "/127.0.0.1".into(),
            regex_resolver: no_topic_regex_resolver(),
            reply,
        })
        .await
        .expect("the group actor is running");
    joined
}

// After a coordinator restart, a new member that proposes the replayed
// group's protocol joins it, and the rebalance completes once the replayed
// member joins again. Kafka's replay gives the loaded member the group's
// protocol, so its `supportsProtocols` check lets the new member in.
#[tokio::test]
async fn a_replayed_group_admits_a_new_member_that_proposes_its_protocol() {
    let coordinator = bare_coordinator();
    let (key, value) = classic_group_record("g", "m1");
    let mut replayed = Replayed::default();
    apply_record(
        &coordinator,
        &mut replayed,
        persistence::parse_key(&key).unwrap(),
        &value,
        &RecordBatch::default(),
    )
    .unwrap();
    finalize(&coordinator, replayed).await;
    let handle = coordinator
        .find("g")
        .expect("the replayed group has an actor");

    // KIP-394: a new member's first `JoinGroup` gets the member id to use.
    let asked = queue_join(&handle, "", b"new").await.await.unwrap();
    assert!(
        asked
            == JoinResult {
                error_code: codes::MEMBER_ID_REQUIRED,
                member_id: asked.member_id.clone(),
                ..JoinResult::default()
            }
    );
    let new_member = asked.member_id;
    let new_member_joined = queue_join(&handle, &new_member, b"new").await;
    let leader = queue_join(&handle, "m1", b"old").await.await.unwrap();
    let follower = new_member_joined.await.unwrap();

    let next_generation = JoinResult {
        error_code: codes::NONE,
        generation_id: 4,
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        leader: "m1".into(),
        ..JoinResult::default()
    };
    let mut members = vec![
        JoinResultMember {
            member_id: "m1".into(),
            group_instance_id: None,
            metadata: Bytes::from_static(b"old"),
        },
        JoinResultMember {
            member_id: new_member.clone(),
            group_instance_id: None,
            metadata: Bytes::from_static(b"new"),
        },
    ];
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    assert!(
        leader
            == JoinResult {
                member_id: "m1".into(),
                members,
                ..next_generation.clone()
            }
    );
    assert!(
        follower
            == JoinResult {
                member_id: new_member,
                ..next_generation
            }
    );
}
