//! Share-group membership scenarios: a first join, a second member joining the
//! same group, a leave at `member_epoch == -1`, and replay of the membership
//! from `__consumer_offsets` after a broker restart.
//!
//! These tests assert on what `ShareGroupHeartbeat` and `ShareGroupDescribe`
//! report about members, so they are kept apart from the share-state
//! initialization scenarios that assert on the persister.

use assert2::{assert, check};

use crate::share_group_harness::{
    boot, connect, create_topic, describe, heartbeat, start, total_assigned,
};

/// A single member joins and advances to epoch 1 with an empty assignment,
/// because Kafka assigns only partitions whose share state is initialized.
/// The join initializes them, and a later heartbeat receives every partition
/// of the subscribed topic at a bumped epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_join_assignment() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t1", 4).await;

    let member_id = uuid::Uuid::new_v4().to_string();
    let mut req = heartbeat("g1", &member_id, 0);
    req.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp = client.send(req).await.unwrap();

    check!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    check!(resp.member_id.as_deref() == Some(member_id.as_str()));
    check!(
        resp.member_epoch == 2,
        "first join advances the new group from epoch 1 to 2, got {}",
        resp.member_epoch
    );
    check!(resp.assignment.is_some() && total_assigned(&resp) == 0);

    let assigned = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut epoch = resp.member_epoch;
        loop {
            let resp = client
                .send(heartbeat("g1", &member_id, epoch))
                .await
                .unwrap();
            assert!(resp.error_code == 0, "heartbeat failed: {resp:?}");
            epoch = resp.member_epoch;
            if resp.assignment.is_some() {
                return resp;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the initialized partitions are assigned");
    check!(assigned.member_epoch == 3);
    check!(
        total_assigned(&assigned) == 4,
        "single member must own all 4 partitions"
    );
}

/// Two members join the same group. After both converge, `ShareGroupDescribe`
/// reports one group with both members and a non-trivial group epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_then_describe() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t2", 4).await;

    let mut m1 = heartbeat("g1", &uuid::Uuid::new_v4().to_string(), 0);
    m1.subscribed_topic_names = Some(vec!["t2".into()]);
    let r1 = client.send(m1).await.unwrap();
    assert!(r1.error_code == 0, "m1 join failed: {:?}", r1.error_code);
    let mid1 = r1.member_id.clone().unwrap();

    let mut m2 = heartbeat("g1", &uuid::Uuid::new_v4().to_string(), 0);
    m2.subscribed_topic_names = Some(vec!["t2".into()]);
    let r2 = client.send(m2).await.unwrap();
    assert!(r2.error_code == 0, "m2 join failed: {:?}", r2.error_code);
    let mid2 = r2.member_id.clone().unwrap();
    assert!(mid1 != mid2, "members must have distinct ids");

    // m1 re-heartbeats at its returned epoch so it learns the rebalanced
    // assignment after m2 bumped the group epoch.
    let mut m1b = heartbeat("g1", &mid1, r1.member_epoch);
    m1b.subscribed_topic_names = Some(vec!["t2".into()]);
    let r1b = client.send(m1b).await.unwrap();
    assert!(r1b.error_code == 0, "m1 re-hb failed: {:?}", r1b.error_code);

    let desc = describe(&client, "g1").await;
    assert!(desc.groups.len() == 1, "expected exactly one group row");
    let g = &desc.groups[0];
    check!(g.error_code == 0, "describe error: {:?}", g.error_code);
    check!(
        g.members.len() == 2,
        "describe must show both members, got {}",
        g.members.len()
    );
    check!(
        g.group_epoch >= 1,
        "group epoch must have advanced, got {}",
        g.group_epoch
    );
}

/// A member leaves by sending `member_epoch == -1`. The leave succeeds, and
/// the broker keeps the group but reports it with zero members, in state
/// "Empty".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_leave_epoch_minus_one() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t3", 2).await;

    let mut join = heartbeat("g1", &uuid::Uuid::new_v4().to_string(), 0);
    join.subscribed_topic_names = Some(vec!["t3".into()]);
    let r = client.send(join).await.unwrap();
    assert!(r.error_code == 0, "join failed: {:?}", r.error_code);
    let mid = r.member_id.clone().unwrap();

    let leave = heartbeat("g1", &mid, -1);
    let lr = client.send(leave).await.unwrap();
    assert!(lr.error_code == 0, "leave failed: {:?}", lr.error_code);

    let desc = describe(&client, "g1").await;
    assert!(
        desc.groups.len() == 1,
        "group row still present after leave"
    );
    let g = &desc.groups[0];
    // The empty group is retained (the actor stays alive with 0 members).
    assert!(
        g.error_code == 0,
        "retained empty group describe error: {:?}",
        g.error_code
    );
    assert!(
        g.members.is_empty(),
        "no members must remain after leave, got {}",
        g.members.len()
    );
}

/// Share-group state persists to `__consumer_offsets`. After a broker restart,
/// a Rejoin on the same data dir, replay reconstructs the group and the member,
/// and `ShareGroupDescribe` reports them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let member_id;
    {
        let broker = start(log_dir.clone()).await;
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;
        create_topic(&client, "t4", 2).await;

        let mut join = heartbeat("g1", &uuid::Uuid::new_v4().to_string(), 0);
        join.subscribed_topic_names = Some(vec!["t4".into()]);
        let r = client.send(join).await.unwrap();
        assert!(r.error_code == 0, "join failed: {:?}", r.error_code);
        member_id = r.member_id.clone().unwrap();

        // flush_pending inside the share actor awaits offsets_log.append before
        // returning the heartbeat response, so the join record is durable on
        // disk by the time the client receives the response above.
        broker.shutdown().await;
    }

    {
        let (_broker, client) = crate::share_group_harness::rejoin(log_dir).await;

        let desc = describe(&client, "g1").await;
        assert!(desc.groups.len() == 1, "group row after restart");
        let g = &desc.groups[0];
        check!(
            g.error_code == 0,
            "recovered group describe error: {:?}",
            g.error_code
        );
        check!(
            g.group_epoch >= 1,
            "recovered group epoch must be >= 1, got {}",
            g.group_epoch
        );
        check!(
            g.members.iter().any(|m| m.member_id == member_id),
            "recovered group must contain the original member {member_id}, members: {:?}",
            g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
        );
    }
}
