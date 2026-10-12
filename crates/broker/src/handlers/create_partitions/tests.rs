//! End-to-end tests of the `CreatePartitions` handler, driven against a
//! running broker: the authorization gate, the per-topic error rows, the
//! `validate_only` dry run, a successful grow, and the KIP-599 mutation quota.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse;
use krabka_units::convert::TimeExt as _;

use super::*;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::{Broker, BrokerHandle},
    handlers::{
        RequestContext,
        create_partitions::test_support::{
            PartitionMutation, PartitionTopicSetup, SeedTopicSetup, VERSION, assn,
            expected_response, expected_result, request, seed_controller_quota, seed_topic,
            topic_req,
        },
        test_support::{TopicPartitionCount, replica_availability},
    },
    test_support::{
        DenyAll, begin_controlled_shutdown, fence_remote_broker, make_witness, peer, principal,
        seed_remote_broker,
    },
};

crate::test_support::context_helper!(client_id = "admin-client");

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &CreatePartitionsRequest,
    ctx: &RequestContext<'_>,
) -> CreatePartitionsResponse {
    handle(broker, req.clone(), VERSION, ctx)
        .await
        .expect("handle")
}

#[derive(Default)]
struct PartitionRowsCaseSetup {
    topics: Vec<krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic>,
    mutation: PartitionMutation,
    expected: Vec<CreatePartitionsTopicResult>,
}

async fn check_topic_rows(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    setup: PartitionRowsCaseSetup,
) {
    let response = drive(broker, &request(setup.topics, setup.mutation), ctx).await;
    let expected = expected_response(setup.expected);
    assert!(response == expected);
}

macro_rules! seeded_partition_topic {
    (($handle:ident, $directory:ident, $broker:ident), $setup:expr) => {
        let ($handle, $directory) =
            start_broker(std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&$handle, $setup).await;
        let $broker = $handle.broker_arc_for_test();
    };
}

#[tokio::test]
async fn handle_denies_topic_alter_for_each_topic() {
    broker_fixture!((broker_handle, _dir, broker), deny_all);
    request_identity!((p, peer), principal("alice"));
    check_topic_rows(
        &broker,
        &test_context(&p, &peer),
        PartitionRowsCaseSetup {
            topics: vec![
                topic_req(PartitionTopicSetup {
                    name: "orders",
                    count: crate::handlers::test_support::TopicPartitionCount(2),
                    ..Default::default()
                }),
                topic_req(PartitionTopicSetup {
                    name: "payments",
                    count: crate::handlers::test_support::TopicPartitionCount(2),
                    ..Default::default()
                }),
            ],
            expected: vec![
                expected_result("orders", codes::TOPIC_AUTHORIZATION_FAILED, None),
                expected_result("payments", codes::TOPIC_AUTHORIZATION_FAILED, None),
            ],
            ..Default::default()
        },
    )
    .await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_unknown_topic_and_rejects_same_partition_count() {
    seeded_partition_topic!(
        (broker_handle, _dir, broker),
        SeedTopicSetup {
            name: "stable",
            ..Default::default()
        }
    );
    request_identity!((p, peer), principal("admin"));
    check_topic_rows(
        &broker,
        &test_context(&p, &peer),
        PartitionRowsCaseSetup {
            topics: vec![
                topic_req(PartitionTopicSetup {
                    name: "missing",
                    ..Default::default()
                }),
                topic_req(PartitionTopicSetup {
                    name: "stable",
                    count: crate::handlers::test_support::TopicPartitionCount(2),
                    ..Default::default()
                }),
            ],
            expected: vec![
                expected_result("missing", codes::UNKNOWN_TOPIC_OR_PARTITION, None),
                expected_result(
                    "stable",
                    codes::INVALID_PARTITIONS,
                    Some("Topic already has 2 partition(s).".into()),
                ),
            ],
            ..Default::default()
        },
    )
    .await;
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("stable")
            .count()
            == 2
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn validate_only_reports_success_without_adding_partitions() {
    seeded_partition_topic!(
        (broker_handle, _dir, broker),
        SeedTopicSetup::single_partition("dry-run")
    );
    request_identity!((p, peer), principal("admin"));
    check_topic_rows(
        &broker,
        &test_context(&p, &peer),
        PartitionRowsCaseSetup {
            topics: vec![topic_req(PartitionTopicSetup {
                name: "dry-run",
                ..Default::default()
            })],
            expected: vec![expected_result("dry-run", codes::NONE, None)],
            mutation: PartitionMutation::Validate,
        },
    )
    .await;
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("dry-run")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_adds_new_partitions_and_preserves_response_identity() {
    seeded_partition_topic!(
        (broker_handle, _dir, broker),
        SeedTopicSetup::single_partition("grow")
    );
    request_identity!((p, peer), principal("admin"));
    check_topic_rows(
        &broker,
        &test_context(&p, &peer),
        PartitionRowsCaseSetup {
            topics: vec![topic_req(PartitionTopicSetup {
                name: "grow",
                assignments: Some(vec![assn(&[1]), assn(&[1])]),
                ..Default::default()
            })],
            expected: vec![expected_result("grow", codes::NONE, None)],
            ..Default::default()
        },
    )
    .await;
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("grow")
            .count()
            == 3
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_an_unplaceable_new_diskless_partition() {
    seeded_partition_topic!(
        (broker_handle, _dir, broker),
        SeedTopicSetup::single_partition("diskless-grow")
    );
    broker_handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "diskless-grow".into(),
                overrides: maplit::btreemap! {
                    crate::config_keys::DISKLESS.to_string() => "true".to_string()
                },
            },
        )])
        .await
        .expect("mark topic diskless");
    let req = request(
        vec![topic_req(PartitionTopicSetup {
            name: "diskless-grow",
            count: crate::handlers::test_support::TopicPartitionCount(2),
            assignments: Some(vec![assn(&[1])]),
        })],
        PartitionMutation::Apply,
    );

    let resp = drive(&broker, &req, &test_context(&principal("admin"), &peer())).await;

    assert!(resp.results[0].error_code == codes::INVALID_CONFIG);
    let message = resp.results[0].error_message.as_deref().unwrap_or_default();
    for needle in ["partition 1", "leader 1", "broker.rack"] {
        check!(message.contains(needle), "{message}");
    }
    assert!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("diskless-grow")
            .count()
            == 1
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn strict_create_partitions_rejects_after_quota_exhaustion() {
    seeded_partition_topic!(
        (broker_handle, _dir, broker),
        SeedTopicSetup {
            name: "metered",
            ..Default::default()
        }
    );
    seed_controller_quota(&broker_handle, 2.0).await;
    request_identity!((p, peer), principal("admin"));
    check_topic_rows(
        &broker,
        &test_context(&p, &peer),
        PartitionRowsCaseSetup {
            topics: vec![topic_req(PartitionTopicSetup {
                name: "metered",
                count: crate::handlers::test_support::TopicPartitionCount(5),
                ..Default::default()
            })],
            expected: vec![expected_result("metered", codes::NONE, None)],
            ..Default::default()
        },
    )
    .await;

    let rejected = drive(
        &broker,
        &request(
            vec![topic_req(PartitionTopicSetup {
                name: "metered",
                count: crate::handlers::test_support::TopicPartitionCount(6),
                ..Default::default()
            })],
            PartitionMutation::Apply,
        ),
        &test_context(&p, &peer),
    )
    .await;
    let expected = CreatePartitionsResponse {
        throttle_time_ms: rejected.throttle_time_ms,
        results: vec![CreatePartitionsTopicResult {
            name: "metered".into(),
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            error_message: Some("The throttling quota has been exceeded.".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(rejected == expected);
    check!(rejected.throttle_time_ms > 0 && rejected.throttle_time_ms <= 500);
    check!(
        broker_handle
            .controller_image_for_test()
            .partitions_of("metered")
            .count()
            == 5
    );
    broker_handle.shutdown().await;
}

/// Reuse one growth reservation across a known-stale guarded attempt and a
/// fresh retry. The independent request uses another principal and grows the
/// topic before the stale attempt reaches controller admission.
#[tokio::test]
async fn a_conflicting_growth_refunds_only_its_obsolete_partition_delta() {
    for concurrent_count in [9, 10] {
        seeded_partition_topic!(
            (broker_handle, _dir, broker),
            SeedTopicSetup::single_partition("metered")
        );
        seed_controller_quota(&broker_handle, 2.0).await;
        request_identity!((p, peer), principal("admin"));
        let ctx = test_context(&p, &peer);
        let stale = broker.controller.current_image();
        let quota = Mutex::new(ctx.controller_mutation_quota(&broker, &stale, true));
        let growth = GrowthContext {
            broker: &broker,
            ctx: &ctx,
            quota: &quota,
            charged: AtomicU64::new(0),
            prior_delay: Time::ZERO,
            validate_only: false,
        };
        let topic = topic_req(PartitionTopicSetup {
            name: "metered",
            count: TopicPartitionCount(10),
            ..Default::default()
        });
        let concurrent = principal("unmetered");
        let response = drive(
            &broker,
            &request(
                vec![topic_req(PartitionTopicSetup {
                    name: "metered",
                    count: TopicPartitionCount(concurrent_count),
                    ..Default::default()
                })],
                PartitionMutation::Apply,
            ),
            &test_context(&concurrent, &peer),
        )
        .await;
        assert!(response.results[0].error_code == codes::NONE);
        assert!(matches!(
            grow_topic(&growth, &topic, stale).await,
            Err(RaftError::StaleMetadataImage)
        ));
        assert!(growth.charged.load(Ordering::Relaxed) == 9);

        let retried = grow_topic(&growth, &topic, broker.controller.current_image())
            .await
            .unwrap();
        assert!(
            retried.error_code
                == if concurrent_count == 9 {
                    codes::NONE
                } else {
                    codes::INVALID_PARTITIONS
                }
        );
        assert!(growth.charged.load(Ordering::Relaxed) == u64::from(concurrent_count == 9));

        // A request may consume the remaining burst, including one overdraft,
        // but the next request must still be refused and report real debt.
        let next_count = if concurrent_count == 9 { 12 } else { 13 };
        for (count, expected) in [
            (next_count, codes::NONE),
            (next_count + 1, codes::THROTTLING_QUOTA_EXCEEDED),
        ] {
            let answer = drive(
                &broker,
                &request(
                    vec![topic_req(PartitionTopicSetup {
                        name: "metered",
                        count: TopicPartitionCount(count),
                        ..Default::default()
                    })],
                    PartitionMutation::Apply,
                ),
                &ctx,
            )
            .await;
            assert!(answer.results[0].error_code == expected);
            if expected == codes::THROTTLING_QUOTA_EXCEEDED {
                assert!(answer.throttle_time_ms > 0 && answer.throttle_time_ms <= 500);
            }
        }
        broker_handle.shutdown().await;
    }
}

/// Commit topic `grow` with one partition on the remote brokers 3 and 4, so
/// its replication factor is 2.
async fn seed_remote_topic(handle: &BrokerHandle) {
    let replicas = vec![krabka_raft::NodeId(3), krabka_raft::NodeId(4)];
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: "grow".into(),
                topic_id: uuid::Uuid::new_v4(),
                partitions: 1,
                replication_factor: 2,
            }),
            krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: "grow".into(),
                partition: 0,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ])
        .await
        .expect("seed topic");
}

/// A manual assignment for a new partition keeps its replica list, but its
/// ISR holds only the listed brokers that are active, and its leader is the
/// first of them (#745). Kafka's `ReplicationControlManager.createPartitions`
/// filters the ISR with `ClusterControlManager.isActive`, and answers
/// `INVALID_REPLICA_ASSIGNMENT` when no listed broker is active.
///
/// Brokers 2, 3 and 4 are remote registrations. A fenced heartbeat on the
/// controller makes a broker unavailable, as a real fenced broker is.
#[tokio::test]
async fn manual_assignment_leaves_unavailable_brokers_out_of_the_isr() {
    /// One row: the fenced brokers, the witness brokers, the replica list of
    /// each new partition, and the expected error code, error message and
    /// `(leader, isr)` per new partition.
    use crate::handlers::test_support::ManualAssignmentRow as Row;
    let n = krabka_raft::NodeId;
    let rows: [Row; 6] = [
        (
            &[],
            &[],
            &[&[2, 3]],
            codes::NONE,
            None,
            vec![(n(2), vec![n(2), n(3)])],
        ),
        (
            &[2],
            &[],
            &[&[2, 3]],
            codes::NONE,
            None,
            vec![(n(3), vec![n(3)])],
        ),
        (
            &[2],
            &[],
            &[&[4, 2], &[2, 3]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(4)]), (n(3), vec![n(3)])],
        ),
        (
            &[2, 3],
            &[],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 1 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All active brokers specified in the manual partition assignment for partition \
                 1 are witnesses, and a witness cannot lead.",
            ),
            vec![],
        ),
        (
            &[],
            &[3],
            &[&[3, 4]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(3), n(4)])],
        ),
    ];

    for (fenced, witnesses, lists, error_code, error_message, partitions) in rows {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        for node_id in [2, 3, 4] {
            seed_remote_broker(&broker_handle, krabka_raft::NodeId(node_id)).await;
        }
        seed_remote_topic(&broker_handle).await;
        for &node_id in witnesses {
            make_witness(&broker_handle, node_id).await;
        }
        for &node_id in fenced {
            fence_remote_broker(&broker_handle, node_id).await;
        }
        let broker = broker_handle.broker_arc_for_test();
        let count = 1 + i32::try_from(lists.len()).expect("count");
        let req = request(
            vec![topic_req(PartitionTopicSetup {
                name: "grow",
                count: crate::handlers::test_support::TopicPartitionCount(count),
                assignments: Some(lists.iter().map(|ids| assn(ids)).collect()),
            })],
            PartitionMutation::Apply,
        );

        let resp = drive(&broker, &req, &test_context(&principal("admin"), &peer())).await;

        let expected = expected_response(vec![expected_result(
            "grow",
            error_code,
            error_message.map(str::to_owned),
        )]);
        check!(resp == expected, "fenced {fenced:?}, assignment {lists:?}");

        let image = broker_handle.controller_image_for_test();
        let added = (1..)
            .map_while(|index| image.partition("grow", index).cloned())
            .collect::<Vec<_>>();
        let expected_records = partitions
            .into_iter()
            .zip(lists)
            .zip(1..)
            .map(
                |(((leader, isr), replicas), partition)| krabka_metadata::PartitionRecord {
                    topic: "grow".into(),
                    partition,
                    leader,
                    replicas: replicas
                        .iter()
                        .map(|&id| n(u64::try_from(id).expect("broker id")))
                        .collect(),
                    isr,
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                },
            )
            .collect::<Vec<_>>();
        check!(
            added == expected_records,
            "fenced {fenced:?}, assignment {lists:?}"
        );
        broker_handle.shutdown().await;
    }
}

/// #1201: Kafka's `createPartitions` gives the placer every registered broker
/// that is not in controlled shutdown, fenced ones included. The placer takes a
/// fenced broker last and never first, the ISR keeps the active replicas, and
/// the placer's refusal reaches the row as it is, with no "Unable to replicate"
/// prefix, because `createPartitions` does not wrap it as `createTopic` does.
///
/// Topic `grow` has replication factor 2. The cluster is the local broker 1
/// and the remote brokers 3 and 4.
#[tokio::test]
async fn automatic_growth_takes_fenced_brokers_last_and_words_refusals_like_the_placer() {
    /// One row: the fenced brokers, the brokers in controlled shutdown, and
    /// either how many replicas of each new partition are fenced, or the
    /// refusal message.
    type Row = (&'static [u64], &'static [u64], Result<usize, &'static str>);
    let rows: [Row; 3] = [
        (&[4], &[], Ok(0)),
        (&[3, 4], &[], Ok(1)),
        (
            &[],
            &[3, 4],
            Err(
                "The target replication factor of 2 cannot be reached because only 1 broker(s) \
                 are registered or some brokers have all their log directories cordoned.",
            ),
        ),
    ];

    for (fenced, shutting_down, outcome) in rows {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        for node_id in [3, 4] {
            seed_remote_broker(&broker_handle, krabka_raft::NodeId(node_id)).await;
        }
        seed_remote_topic(&broker_handle).await;
        for &node_id in fenced {
            fence_remote_broker(&broker_handle, node_id).await;
        }
        for &node_id in shutting_down {
            begin_controlled_shutdown(&broker_handle, node_id).await;
        }
        let broker = broker_handle.broker_arc_for_test();

        let resp = drive(
            &broker,
            &request(
                vec![topic_req(PartitionTopicSetup {
                    name: "grow",
                    count: crate::handlers::test_support::TopicPartitionCount(5),
                    ..Default::default()
                })],
                PartitionMutation::Apply,
            ),
            &test_context(&principal("admin"), &peer()),
        )
        .await;

        let image = broker_handle.controller_image_for_test();
        let added = (1..)
            .map_while(|index| image.partition("grow", index).cloned())
            .collect::<Vec<_>>();
        let label = format!("fenced {fenced:?}, shutting down {shutting_down:?}");
        let (error_code, error_message) = match outcome {
            Ok(_) => (codes::NONE, None),
            Err(message) => (codes::INVALID_REPLICATION_FACTOR, Some(message.to_owned())),
        };
        let expected = expected_response(vec![expected_result("grow", error_code, error_message)]);
        check!(resp == expected, "{label}");
        let Ok(fenced_replicas) = outcome else {
            check!(added.is_empty(), "{label}");
            broker_handle.shutdown().await;
            continue;
        };
        check!(added.len() == 4, "{label}");
        for record in &added {
            let replicas = &record.replicas;
            let (fenced_flags, isr) = replica_availability(replicas, fenced);
            check!(
                replicas.len() == 2
                    && fenced_flags.iter().filter(|flag| **flag).count() == fenced_replicas
                    && fenced_flags.is_sorted(),
                "{label}, {replicas:?}"
            );
            check!(
                *record
                    == krabka_metadata::PartitionRecord {
                        topic: "grow".into(),
                        partition: record.partition,
                        leader: isr[0],
                        replicas: replicas.clone(),
                        isr,
                        leader_epoch: krabka_metadata::LeaderEpoch(0),
                        adding_replicas: vec![],
                        removing_replicas: vec![],
                        directories: vec![],
                        partition_epoch: 0,
                    },
                "{label}"
            );
        }
        broker_handle.shutdown().await;
    }
}

/// #744: Kafka's `ControllerApis.createPartitions` answers a duplicated name
/// once with `INVALID_REQUEST` and grows none of its rows, and the
/// controller's count checks answer with Kafka's messages. Topic `t` has 2
/// partitions of replication factor 1.
#[tokio::test]
async fn rows_follow_kafkas_duplicate_and_count_checks() {
    fn row(name: &str, error_code: i16, message: Option<&str>) -> CreatePartitionsTopicResult {
        expected_result(name, error_code, message.map(str::to_owned))
    }
    let duplicate = || row("t", codes::INVALID_REQUEST, Some("Duplicate topic name."));
    let four_partition_growth = topic_req(PartitionTopicSetup {
        count: TopicPartitionCount(4),
        ..Default::default()
    });
    let duplicate_growth = vec![four_partition_growth.clone(), four_partition_growth.clone()];
    let cases = [
        (
            "the same growth twice",
            duplicate_growth.clone(),
            PartitionMutation::Apply,
            vec![duplicate()],
        ),
        (
            "two different growths",
            vec![
                four_partition_growth.clone(),
                topic_req(PartitionTopicSetup {
                    count: crate::handlers::test_support::TopicPartitionCount(6),
                    ..Default::default()
                }),
            ],
            PartitionMutation::Apply,
            vec![duplicate()],
        ),
        (
            "the same growth twice, validate only",
            duplicate_growth.clone(),
            PartitionMutation::Validate,
            vec![duplicate()],
        ),
        (
            "a duplicate answers ahead of the other rows",
            vec![
                topic_req(PartitionTopicSetup {
                    name: "u",
                    ..Default::default()
                }),
                four_partition_growth.clone(),
                four_partition_growth.clone(),
            ],
            PartitionMutation::Apply,
            vec![
                duplicate(),
                row("u", codes::UNKNOWN_TOPIC_OR_PARTITION, None),
            ],
        ),
        (
            "a count below the current one",
            vec![topic_req(PartitionTopicSetup {
                count: crate::handlers::test_support::TopicPartitionCount(1),
                ..Default::default()
            })],
            PartitionMutation::Apply,
            vec![row(
                "t",
                codes::INVALID_PARTITIONS,
                Some("The topic t currently has 2 partition(s); 1 would not be an increase."),
            )],
        ),
        (
            "fewer assignments than new partitions",
            vec![topic_req(PartitionTopicSetup {
                count: crate::handlers::test_support::TopicPartitionCount(4),
                assignments: Some(vec![assn(&[1])]),
                ..Default::default()
            })],
            PartitionMutation::Apply,
            vec![row(
                "t",
                codes::INVALID_REPLICA_ASSIGNMENT,
                Some(
                    "Attempted to add 2 additional partition(s), but only 1 assignment(s) were \
                     specified.",
                ),
            )],
        ),
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, topics, mutation, results) in cases {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, SeedTopicSetup::default()).await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((p, peer), principal("admin"));

        let resp = drive(
            &broker,
            &request(topics, mutation),
            &test_context(&p, &peer),
        )
        .await;
        let partitions = broker_handle
            .controller_image_for_test()
            .partitions_of("t")
            .count();
        actual.push((label, resp, partitions));
        expected.push((label, expected_response(results), 2));
        broker_handle.shutdown().await;
    }
    assert!(actual == expected);
}

/// Both operations initially observe the same count. Only one may create the
/// new indices; the other must see the committed count on its next attempt.
#[tokio::test]
async fn concurrent_growth_revalidates_partition_count_before_append() {
    seeded_partition_topic!(
        (handle, _dir, broker),
        SeedTopicSetup::single_partition("t")
    );
    let p = principal("admin");
    let h = peer();
    let ctx = test_context(&p, &h);
    let req = request(
        vec![topic_req(PartitionTopicSetup::default())],
        PartitionMutation::Apply,
    );
    let (first, second) = tokio::join!(drive(&broker, &req, &ctx), drive(&broker, &req, &ctx));
    let mut got = [first, second];
    got.sort_unstable_by_key(|response| response.results[0].error_code);
    let expected = [
        expected_response(vec![expected_result("t", codes::NONE, None)]),
        expected_response(vec![expected_result(
            "t",
            codes::INVALID_PARTITIONS,
            Some("Topic already has 3 partition(s).".into()),
        )]),
    ];
    assert!(got == expected);
    assert!(
        broker
            .controller
            .current_image()
            .topic("t")
            .expect("topic")
            .partitions
            == 3
    );
    handle.shutdown().await;
}
