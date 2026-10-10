//! KIP-345 static-membership integration tests.
//!
//! These tests exercise the full `JoinGroup` → `SyncGroup` → rejoin cycle,
//! where the rejoin uses the same `group.instance.id`, through the in-process
//! broker harness. They check the three KIP-345 invariants:
//!
//! 1. A static rejoin into a `Stable` group gets a new member id, fences
//!    the old one, keeps the prior assignment and does NOT advance
//!    `generation_id`.
//! 2. The broker rejects a second client that uses the same
//!    `group.instance.id` while the first is still live, with
//!    `FENCED_INSTANCE_ID`.
//! 3. `LeaveGroup` v3+ with a `MemberIdentity { member_id: "",
//!    group_instance_id: Some(...) }` resolves the static slot and
//!    removes it.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest,
    join_group_request::JoinGroupRequest,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::{LeaveGroupRequest, MemberIdentity},
    sync_group_request::SyncGroupRequest,
};

use crate::support::classic::{classic_join_request, classic_sync_request, sync_assignment};

mod support;

/// `FENCED_INSTANCE_ID` (82, KIP-345).
const FENCED_INSTANCE_ID: i16 = 82;

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

fn join_request(group_id: &str, member_id: &str, instance_id: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: instance_id.map(str::to_string),
        ..classic_join_request(crate::support::classic::ClassicJoinSetup {
            group_id: (group_id).into(),
            member_id: (member_id).into(),
            timeouts: crate::support::classic::ClassicTimeouts {
                rebalance: krabka_units::millis(1_500),
                ..Default::default()
            },
            ..Default::default()
        })
    }
}

/// Boot a group with one static member and sync an assignment for it.
/// Returns the assigned `(member_id, generation_id, assignment)`.
async fn bootstrap_static_member(
    client: &krabka_client_core::Client,
    group_id: &str,
    instance_id: &str,
    assignment: Bytes,
) -> (String, i32, Bytes) {
    // 1. Empty member_id → a static member joins at once with a generated
    //    `<instance id>-<uuid>` member id and becomes the leader. Kafka's
    //    `MEMBER_ID_REQUIRED` round trip is for dynamic members only.
    let r1 = client
        .send(join_request(group_id, "", Some(instance_id)))
        .await
        .expect("JoinGroup");
    assert!(r1.error_code == 0);
    let mid = r1.member_id.clone();
    assert!(mid.starts_with(&format!("{instance_id}-")));
    assert!(r1.leader == mid);
    let generation = r1.generation_id;

    // 3. Leader SyncGroup installs an assignment for itself.
    let r3 = client
        .send(SyncGroupRequest {
            group_instance_id: Some(instance_id.into()),
            ..classic_sync_request(crate::support::classic::ClassicSyncSetup {
                group_id: (group_id).into(),
                generation_id: crate::support::classic::GenerationId(generation),
                member_id: mid.clone(),
                assignments: vec![sync_assignment(mid.clone(), assignment.clone())],
                ..Default::default()
            })
        })
        .await
        .expect("SyncGroup");
    assert!(r3.error_code == 0);
    assert!(r3.assignment == assignment);

    (mid, generation, assignment)
}

#[tokio::test]
async fn static_rejoin_preserves_assignment_and_generation() {
    let p = start().await;

    let (mid1, gen1, assignment) = bootstrap_static_member(
        &p.client,
        "g-static-1",
        "instance-A",
        Bytes::from_static(b"assignment-bytes"),
    )
    .await;

    // Confirm Heartbeat works with both member_id and instance_id.
    let hb = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-static-1".into(),
            generation_id: gen1,
            member_id: mid1.clone(),
            group_instance_id: Some("instance-A".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat");
    assert!(hb.error_code == 0);

    // A restart: the same instance id with an empty member id. Kafka's
    // `updateStaticMemberThenRebalanceOrCompleteJoin` gives it a new member
    // id in place of the old one. The group is Stable and the selected
    // protocol does not change, so the generation stays; at v9 the leader
    // gets the member list with `skip_assignment` (KIP-814).
    let rejoin = p
        .client
        .send(join_request("g-static-1", "", Some("instance-A")))
        .await
        .expect("static rejoin");
    let mid2 = rejoin.member_id.clone();
    check!(mid2 != mid1);
    check!(mid2.starts_with("instance-A-"));
    check!(
        rejoin
            == JoinGroupResponse {
                generation_id: gen1,
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                leader: mid2.clone(),
                skip_assignment: true,
                member_id: mid2.clone(),
                members: vec![JoinGroupResponseMember {
                    member_id: mid2.clone(),
                    group_instance_id: Some("instance-A".into()),
                    metadata: Bytes::new(),
                    ..Default::default()
                }],
                ..JoinGroupResponse::default()
            },
        "static rejoin must keep generation_id and replace the member id"
    );

    // The old instance is fenced, and the new member id keeps the
    // assignment.
    let old = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-static-1".into(),
            generation_id: gen1,
            member_id: mid1,
            group_instance_id: Some("instance-A".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat (old member id)");
    check!(old.error_code == FENCED_INSTANCE_ID);
    let sync = p
        .client
        .send(SyncGroupRequest {
            group_id: "g-static-1".into(),
            generation_id: gen1,
            member_id: mid2,
            group_instance_id: Some("instance-A".into()),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            ..Default::default()
        })
        .await
        .expect("SyncGroup (new member id)");
    check!(sync.error_code == 0);
    check!(sync.assignment == assignment);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn second_client_with_same_instance_id_is_fenced() {
    let p = start().await;

    let (mid1, gen1, _) = bootstrap_static_member(
        &p.client,
        "g-fence",
        "instance-B",
        Bytes::from_static(b"asgn"),
    )
    .await;

    // A *different* live `member_id` claims the same instance id. The
    // KIP-345 rule: reject with FENCED_INSTANCE_ID.
    let intruder = p
        .client
        .send(join_request(
            "g-fence",
            "imposter-member-id",
            Some("instance-B"),
        ))
        .await
        .expect("intruder JoinGroup");
    assert!(intruder.error_code == FENCED_INSTANCE_ID);

    // Heartbeat with a wrong member_id but the right instance id is also
    // fenced. (Defense-in-depth: a client whose `member_id` was reset
    // shouldn't be able to talk under the static slot until it
    // re-bootstraps.)
    let hb_fenced = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-fence".into(),
            generation_id: gen1,
            member_id: "wrong-member-id".into(),
            group_instance_id: Some("instance-B".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat (fenced)");
    assert!(hb_fenced.error_code == FENCED_INSTANCE_ID);

    // The original member is unaffected.
    let hb_ok = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-fence".into(),
            generation_id: gen1,
            member_id: mid1.clone(),
            group_instance_id: Some("instance-B".into()),
            ..Default::default()
        })
        .await
        .expect("Heartbeat (incumbent)");
    assert!(hb_ok.error_code == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn leave_group_resolves_static_member_by_instance_id() {
    let p = start().await;

    let (mid, _gen, _) = bootstrap_static_member(
        &p.client,
        "g-leave",
        "instance-C",
        Bytes::from_static(b"asgn"),
    )
    .await;

    // LeaveGroup v3+ with empty member_id, identity carries only the
    // instance id. Broker must resolve via the static index, remove the
    // slot, and echo the instance id in the response with NONE.
    let resp = p
        .client
        .send(LeaveGroupRequest {
            group_id: "g-leave".into(),
            member_id: String::new(),
            members: vec![MemberIdentity {
                member_id: String::new(),
                group_instance_id: Some("instance-C".into()),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    check!(resp.error_code == 0);
    assert!(resp.members.len() == 1);
    check!(resp.members[0].error_code == 0);
    check!(resp.members[0].group_instance_id.as_deref() == Some("instance-C"));

    // A subsequent Heartbeat from the old member id should be rejected —
    // the slot is gone.
    let hb = p
        .client
        .send(HeartbeatRequest {
            group_id: "g-leave".into(),
            generation_id: 1,
            member_id: mid,
            ..Default::default()
        })
        .await
        .expect("Heartbeat after leave");
    assert!(hb.error_code != 0);

    p.broker.shutdown().await;
}
