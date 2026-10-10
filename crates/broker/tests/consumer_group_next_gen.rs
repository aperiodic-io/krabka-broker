//! Raw-RPC integration tests for KIP-848 next-gen consumer groups,
//! driven against an in-process Krabka broker through `krabka-client-core`.

mod support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
    consumer_group_describe_request::ConsumerGroupDescribeRequest,
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
    list_groups_request::ListGroupsRequest,
};

use crate::support::{
    classic::classic_join_request,
    client::connect_client,
    topics::{creatable_topic, create_topic_request},
};

async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.heartbeat_timeout = krabka_units::secs(30);
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (topic).into(),
                partitions: crate::support::topics::TopicPartitionCount(partitions),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
}

/// A heartbeat Kafka accepts at v1: an empty `member_id` becomes a
/// client-generated id, as a KIP-848 consumer sends it, and a join (epoch 0)
/// reports its empty owned partitions.
fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ConsumerGroupHeartbeatRequest {
    let member_id = if member_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        member_id.into()
    };
    ConsumerGroupHeartbeatRequest {
        rebalance_timeout_ms: 60_000,
        topic_partitions: (epoch == 0).then(Vec::new),
        ..crate::support::consumer_groups::consumer_heartbeat(group, member_id, epoch)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_full_lifecycle() {
    let (_b, _d, client) = consumer_case("t1", 4, "c1").await;

    let mut req = heartbeat("g1", "", 0);
    req.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == 0);
    let member_id = resp.member_id.clone().unwrap();
    // A new group starts at Kafka's epoch 1, and the join bumps it to 2.
    assert!(resp.member_epoch == 2);
    let assigned = resp.assignment.as_ref().unwrap();
    let total_partitions: usize = assigned
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(total_partitions == 4);

    let mut hb2 = heartbeat("g1", &member_id, 2);
    hb2.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp2 = client.send(hb2).await.unwrap();
    assert!(resp2.error_code == 0);
    assert!(resp2.member_epoch == 2);

    let leave = heartbeat("g1", &member_id, -1);
    let resp3 = client.send(leave).await.unwrap();
    assert!(resp3.error_code == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_split_partitions() {
    let (_b, _d, client) = consumer_case("t2", 4, "c").await;

    let mut a = heartbeat("g2", "", 0);
    a.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra = client.send(a).await.unwrap();
    assert!(ra.error_code == 0, "A join failed: {:?}", ra.error_code);
    let mid_a = ra.member_id.clone().unwrap();

    let mut b = heartbeat("g2", "", 0);
    b.subscribed_topic_names = Some(vec!["t2".into()]);
    let rb = client.send(b).await.unwrap();
    assert!(rb.error_code == 0, "B join failed: {:?}", rb.error_code);
    let mid_b = rb.member_id.clone().unwrap();
    let b_epoch = rb.member_epoch;

    // A re-heartbeats at its own epoch (1) to learn the rebalanced assignment
    // and revoke the partitions B's target needs. B's join bumped the group
    // epoch to 2 and updated A's target, but A's stored member_epoch is still
    // 1 — we must heartbeat at that epoch.
    // Each re-heartbeat reports what the member owns, so it is Kafka's full
    // request and the response carries the assignment.
    let owned = |response: &ConsumerGroupHeartbeatResponse| {
        response
            .assignment
            .as_ref()
            .map(crate::support::consumer_groups::reported_assignment)
    };
    let mut a3 = heartbeat("g2", &mid_a, ra.member_epoch);
    a3.subscribed_topic_names = Some(vec!["t2".into()]);
    a3.topic_partitions = owned(&ra);
    let ra3 = client.send(a3).await.unwrap();
    assert!(ra3.error_code == 0, "A re-hb failed: {:?}", ra3.error_code);
    // A acknowledges the revocation by reporting only what it now owns.
    let mut a4 = heartbeat("g2", &mid_a, ra3.member_epoch);
    a4.subscribed_topic_names = Some(vec!["t2".into()]);
    a4.topic_partitions = owned(&ra3);
    let ra4 = client.send(a4).await.unwrap();
    assert!(ra4.error_code == 0, "A ack failed: {:?}", ra4.error_code);

    // B re-heartbeats to acquire the partitions A just released. Per KIP-848 the
    // coordinator withholds a partition from its new owner until the previous
    // owner has revoked it, so B's *join* response (rb) intentionally carries
    // fewer partitions than B's target; B converges on this next heartbeat, now
    // that A's re-heartbeat above revoked them.
    let mut b3 = heartbeat("g2", &mid_b, b_epoch);
    b3.subscribed_topic_names = Some(vec!["t2".into()]);
    b3.topic_partitions = owned(&rb);
    let rb3 = client.send(b3).await.unwrap();
    assert!(rb3.error_code == 0, "B re-hb failed: {:?}", rb3.error_code);

    let parts_a: usize = ra3
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    let parts_b: usize = rb3
        .assignment
        .unwrap()
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(parts_a + parts_b == 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_locked_against_next_gen() {
    let (_b, _d, client) = consumer_case("t3", 2, "c").await;

    // A live classic group that does not use the consumer embedded protocol.
    // Kafka's `validateOnlineUpgrade` refuses to upgrade it; an empty classic
    // group would be replaced instead.
    let join = |member_id: String| {
        classic_join_request(crate::support::classic::ClassicJoinSetup {
            group_id: ("g3").into(),
            member_id,
            timeouts: crate::support::classic::ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            },
            protocol_type: ("connect").into(),
            protocols: vec![
                krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                    name: "default".into(),
                    ..Default::default()
                },
            ],
        })
    };
    let required = client.send(join(String::new())).await.unwrap();
    let joined = client.send(join(required.member_id)).await.unwrap();
    assert!(joined.error_code == 0, "{joined:?}");

    let mut req = heartbeat("g3", "", 0);
    req.subscribed_topic_names = Some(vec!["t3".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(
        (resp.error_code, resp.error_message.as_deref())
            == (
                krabka_broker::codes::GROUP_ID_NOT_FOUND,
                Some(
                    "Cannot upgrade classic group g3 to consumer group because the group does \
                     not use the consumer embedded protocol."
                )
            )
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_returns_unsupported_version() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.next_gen_consumer_group.rebalance_protocols =
        vec![krabka_broker::coordinator::unified::config::RebalanceProtocol::Classic];
    let broker = Broker::start(config).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = Arc::new(connect_client(bootstrap.as_str(), Some("c")).await);

    let mut req = heartbeat("g4", "", 0);
    req.subscribed_topic_names = Some(vec!["t".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == krabka_broker::codes::UNSUPPORTED_VERSION);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_after_join() {
    let (_b, _d, client) = consumer_case("t5", 2, "c").await;

    let mut req = heartbeat("g5", "", 0);
    req.subscribed_topic_names = Some(vec!["t5".into()]);
    let _ = client.send(req).await.unwrap();

    let desc = client
        .send(ConsumerGroupDescribeRequest {
            group_ids: vec!["g5".into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(desc.groups.len() == 1);
    check!(desc.groups[0].error_code == 0);
    check!(desc.groups[0].group_state == "Stable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_epoch_is_fenced() {
    let (_b, _d, client) = consumer_case("t6", 2, "c").await;

    // A joins; group_epoch goes 1→2, A's member_epoch = 2.
    let mut req = heartbeat("g6", "", 0);
    req.subscribed_topic_names = Some(vec!["t6".into()]);
    let r = client.send(req).await.unwrap();
    assert!(r.error_code == 0);
    let mid = r.member_id.unwrap();

    // B joins; group_epoch goes 2→3, B's member_epoch = 3, A's is still 2.
    let mut req2 = heartbeat("g6", "", 0);
    req2.subscribed_topic_names = Some(vec!["t6".into()]);
    let rb = client.send(req2).await.unwrap();
    assert!(rb.error_code == 0);

    // A's heartbeat at epoch 2 succeeds and tells A to give up half of its
    // partitions. Kafka's `CurrentAssignmentBuilder` keeps A at epoch 2 until
    // A reports an owned set without them.
    let mut catch_up = heartbeat("g6", &mid, 2);
    catch_up.subscribed_topic_names = Some(vec!["t6".into()]);
    let rc = client.send(catch_up).await.unwrap();
    assert!(rc.error_code == 0);
    assert!(
        rc.member_epoch == 2,
        "A stays at epoch 2 while it owns partitions it must revoke"
    );
    let kept = rc.assignment.expect("A is told its assignment shrank");

    // A reports what it keeps, and moves to epoch 3.
    let mut acknowledge = heartbeat("g6", &mid, 2);
    acknowledge.topic_partitions =
        Some(crate::support::consumer_groups::reported_assignment(&kept));
    let ra = client.send(acknowledge).await.unwrap();
    assert!(ra.error_code == 0);
    assert!(ra.member_epoch == 3, "A moves to epoch 3 once it revoked");

    // Now A re-heartbeats at the OLD epoch 2 and reports no owned partitions;
    // A's stored epoch is 3. Kafka's `throwIfConsumerGroupMemberEpochIsInvalid`
    // accepts the previous epoch only with owned partitions inside the
    // assignment, so this heartbeat is fenced.
    let stale = heartbeat("g6", &mid, 2);
    let resp = client.send(stale).await.unwrap();
    assert!(resp.error_code == krabka_broker::codes::FENCED_MEMBER_EPOCH);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_with_client_member_id_echoes_and_assigns() {
    let (_b, _d, client) = consumer_case("tc", 2, "c").await;

    // Client supplies its own member id (GA KIP-848 semantics).
    let mut req = heartbeat("gc", "client-generated-id", 0);
    req.subscribed_topic_names = Some(vec!["tc".into()]);
    let resp = client.send(req).await.unwrap();

    assert!(resp.error_code == 0, "client-id first-join failed");
    assert!(
        resp.member_id.as_deref() == Some("client-generated-id"),
        "broker must echo the client-supplied member id"
    );
    let parts: usize = resp
        .assignment
        .expect("assignment present")
        .topic_partitions
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    assert!(
        parts == 2,
        "single member should be assigned both partitions"
    );
}

/// `kafka-consumer-groups.sh --list` sends `ListGroups` (`api_key` 16) with
/// `types_filter = ["consumer"]`. A live next-gen consumer group must appear in
/// that response with `group_type == "consumer"`. It must NOT appear when the
/// request filters on `["share"]`, and it must appear exactly once with no
/// filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_next_gen_consumer_group() {
    let (_b, _d, client) = consumer_case("tlist", 2, "c").await;

    // Join a next-gen consumer group so it is registered in the coordinator.
    let mut req = heartbeat("glist", "", 0);
    req.subscribed_topic_names = Some(vec!["tlist".into()]);
    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);

    // types_filter = ["consumer"] → contains glist tagged "consumer".
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[consumer]");
    assert!(resp.error_code == 0, "list error: {:?}", resp.error_code);
    let row = resp
        .groups
        .iter()
        .find(|g| g.group_id == "glist")
        .unwrap_or_else(|| {
            panic!(
                "consumer group glist missing from ListGroups[consumer], got {:?}",
                resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
            )
        });
    assert!(
        row.group_type == "consumer",
        "expected group_type=consumer, got {:?}",
        row.group_type
    );

    // types_filter = ["share"] → glist must NOT appear.
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["share".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[share]");
    assert!(
        !resp.groups.iter().any(|g| g.group_id == "glist"),
        "consumer group glist must be excluded under types_filter=[share], got {:?}",
        resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
    );

    // No filter → glist appears exactly once, tagged "consumer".
    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups[all]");
    let matches: Vec<_> = resp
        .groups
        .iter()
        .filter(|g| g.group_id == "glist")
        .collect();
    assert!(
        matches.len() == 1,
        "glist must be listed exactly once, got {} rows",
        matches.len()
    );
    assert!(
        matches[0].group_type == "consumer",
        "unfiltered list must tag glist as consumer, got {:?}",
        matches[0].group_type
    );
}

/// KIP-848: a `SubscribedTopicRegex` that does not compile fails the
/// heartbeat with `INVALID_REGULAR_EXPRESSION` (128) and admits no member.
///
/// Kafka's `GroupMetadataManager.throwIfRegularExpressionIsInvalid` compiles
/// the pattern before it writes any member record. The failure mode this pins
/// is the opposite one: admitting the member with `NONE` and never assigning
/// it a partition, which leaves a consumer sitting on an empty assignment
/// with no diagnostic.
///
/// This is the wire test rather than a JVM-lane one because no stock JVM
/// client can reach the broker check. `KafkaConsumer.subscribe(Pattern)` and
/// `kafka-console-consumer --include` both compile the pattern locally with
/// `java.util.regex`, so an invalid pattern dies in the client with
/// `PatternSyntaxException` (observed against `apache/kafka:4.3.1`:
/// `ConsoleConsumer$ConsumerWrapper` compiles at construction, before any
/// request is sent). Only a compiled 4.x application using the
/// `SubscriptionPattern` overload sends the string through, and this
/// repository has no image carrying both a JDK and the 4.x client jars.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_subscribed_topic_regex_fails_the_heartbeat() {
    let (_b, _d, client) = consumer_case("t-badregex", 1, "c-badregex").await;

    for pattern in ["(", "[a-", "a{2,1}"] {
        let mut req = heartbeat("g-badregex", "", 0);
        req.subscribed_topic_regex = Some(pattern.into());
        let resp = client.send(req).await.unwrap();
        check!(
            resp.error_code == 128,
            "pattern {pattern:?} must answer INVALID_REGULAR_EXPRESSION, got {} ({:?})",
            resp.error_code,
            resp.error_message
        );
        check!(
            resp.error_message
                .as_deref()
                .unwrap_or_default()
                .contains("is not a valid regular expression"),
            "pattern {pattern:?} must carry Kafka's message, got {:?}",
            resp.error_message
        );
        check!(
            resp.member_id.is_none() || resp.member_epoch == 0,
            "pattern {pattern:?} must not admit a member: {resp:?}"
        );
    }

    // The group is still joinable with a pattern that does compile, so the
    // refusals above left no member state behind.
    let mut good = heartbeat("g-badregex", "", 0);
    good.subscribed_topic_regex = Some("t-bad.*".into());
    let resp = client.send(good).await.unwrap();
    assert!(resp.error_code == 0, "valid pattern: {resp:?}");
    assert!(resp.member_epoch == 1);
}

/// Kafka's `GroupMetadataManager.onMetadataUpdate`: a member that subscribed
/// to a topic before the topic existed gets its partitions, with a new member
/// epoch, at its first heartbeat after the broker applies the topic. It waits
/// neither for a periodic refresh nor for a session to time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_created_after_the_member_joined_reaches_its_next_heartbeat() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect_client(bootstrap.as_str(), Some("c-late")).await;
    let mut join = heartbeat("g-late", "", 0);
    join.subscribed_topic_names = Some(vec!["late".into()]);
    let joined = client.send(join).await.unwrap();
    let member_id = joined.member_id.clone().expect("member id");
    check!(joined.error_code == 0);
    check!(joined.member_epoch == 2);
    check!(joined.assignment == Some(Assignment::default()));

    create_topic(&client, "late", 3).await;
    let metadata = client
        .send(crate::support::discovery::named_topic_metadata("late"))
        .await
        .expect("Metadata");
    let topic_id = metadata.topics[0].topic_id;

    // The broker refreshes the group when its metadata image holds the topic,
    // which can be a moment after `CreateTopics` answers. The member keeps
    // heartbeating at its epoch, as a consumer does, for far less than the
    // 45 s session timeout.
    let created = Instant::now();
    let refreshed = loop {
        let answer = client
            .send(heartbeat("g-late", &member_id, 2))
            .await
            .unwrap();
        if answer.assignment.is_some() || created.elapsed() > Duration::from_secs(10) {
            break answer;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    check!(
        refreshed
            == ConsumerGroupHeartbeatResponse {
                member_id: Some(member_id),
                member_epoch: 3,
                heartbeat_interval_ms: 5_000,
                assignment: Some(Assignment {
                    topic_partitions: vec![TopicPartitions {
                        topic_id,
                        partitions: vec![0, 1, 2],
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }
    );
}

/// Kafka's `consumerGroupHeartbeatIntervalMs`: a group's
/// `consumer.heartbeat.interval.ms` replaces the broker's
/// `group.consumer.heartbeat.interval.ms` in the heartbeat response of that
/// group only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_config_overrides_the_heartbeat_interval_of_its_group() {
    let (broker, bootstrap, _dir) = boot().await;
    let client = Arc::new(connect_client(bootstrap.as_str(), Some("c")).await);
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: "g-tuned".into(),
                configs: [
                    (
                        "consumer.heartbeat.interval.ms".to_owned(),
                        "7000".to_owned(),
                    ),
                    ("consumer.session.timeout.ms".to_owned(), "50000".to_owned()),
                ]
                .into(),
            },
        ))
        .await
        .expect("set the group config");

    let mut intervals = Vec::new();
    for group in ["g-tuned", "g-plain"] {
        let mut join = heartbeat(group, "", 0);
        join.subscribed_topic_names = Some(vec![]);
        let answer = client.send(join).await.unwrap();
        check!(answer.error_code == 0, "{group}: {answer:?}");
        intervals.push((group, answer.heartbeat_interval_ms));
    }

    check!(intervals == [("g-tuned", 7_000), ("g-plain", 5_000)]);
}

async fn consumer_case(
    topic: &str,
    partitions: i32,
    client_id: &str,
) -> (krabka_broker::BrokerHandle, tempfile::TempDir, Arc<Client>) {
    let (broker, bootstrap, dir) = boot().await;
    let client = Arc::new(connect_client(bootstrap.as_str(), Some(client_id)).await);
    create_topic(&client, topic, partitions).await;
    (broker, dir, client)
}
