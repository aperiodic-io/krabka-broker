//! End-to-end tests of the `CreatePartitions` handler, driven against a
//! running broker: the authorization gate, the per-topic error rows, the
//! `validate_only` dry run, a successful grow, and the KIP-599 mutation quota.

use {
    super::*,
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::{Broker, BrokerHandle},
        handlers::{
            RequestContext,
            create_partitions::test_support::{
                PartitionMutation, VERSION, assn, expected_response, expected_result, request,
                seed_controller_quota, seed_topic, topic_req,
            },
            test_support::{
                ALL_BROKERS_FENCED_PARTITION_ERROR, LiveBrokerFixture,
                StartedBroker as HandlerStartedBroker, TopicPartitionCount,
                assigned_partition_record, invalid_manual_assignment, replica_availability,
            },
        },
        test_support::{
            begin_controlled_shutdown, fence_remote_broker, make_witness, peer, principal,
            seed_remote_broker,
        },
    },
    assert2::{assert, check},
    krabka_protocol::owned::create_partitions_response::CreatePartitionsResponse,
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
};

/// Commit the requested initial partition image before exposing this broker.
#[bon::builder]
async fn start_partition_topic(
    name: &str,
    #[builder(into, default = TopicPartitionCount(2))] partitions: TopicPartitionCount,
) -> LiveBrokerFixture {
    let HandlerStartedBroker {
        broker: handle,
        dir: directory,
    } = start_broker(Arc::new(AllowAllAuthorizer)).await;
    seed_topic()
        .handle(&handle)
        .name(name)
        .partitions(partitions)
        .call()
        .await;
    let broker = handle.broker_arc_for_test();
    LiveBrokerFixture {
        handle,
        directory,
        broker,
    }
}

krabka_macros::wire_codec_helpers! {
    test_context ["admin-client"];
}

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

/// Compare complete topic rows, using the supplied context or a fresh admin context.
#[bon::builder]
async fn check_topic_rows(
    broker: &Broker,
    ctx: Option<&RequestContext<'_>>,
    #[builder(default)] topics: Vec<
        krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic,
    >,
    #[builder(default)] mutation: PartitionMutation,
    #[builder(default)] expected: Vec<CreatePartitionsTopicResult>,
) {
    let setup = PartitionRowsCaseSetup {
        topics,
        mutation,
        expected,
    };
    let request = request(setup.topics, setup.mutation);
    let response = if let Some(ctx) = ctx {
        drive(broker, &request, ctx).await
    } else {
        let admin = principal("admin");
        let address = peer();
        drive(broker, &request, &test_context(&admin, &address)).await
    };
    let expected = expected_response(setup.expected);
    assert!(response == expected);
}

#[tokio::test]
async fn handle_denies_topic_alter_for_each_topic() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, p, peer);
        boot [start_broker] deny_all;
        user ["alice"];
    };
    check_topic_rows()
        .broker(&broker)
        .ctx(&test_context(&p, &peer))
        .topics(vec![
            topic_req().name("orders").count(2).call(),
            topic_req().name("payments").count(2).call(),
        ])
        .expected(vec![
            expected_result("orders", codes::TOPIC_AUTHORIZATION_FAILED, None),
            expected_result("payments", codes::TOPIC_AUTHORIZATION_FAILED, None),
        ])
        .call()
        .await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_unknown_topic_and_rejects_same_partition_count() {
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_partition_topic().name("stable").call().await;
    check_topic_rows()
        .broker(&broker)
        .topics(vec![
            topic_req().name("missing").call(),
            topic_req().name("stable").count(2).call(),
        ])
        .expected(vec![
            expected_result("missing", codes::UNKNOWN_TOPIC_OR_PARTITION, None),
            expected_result(
                "stable",
                codes::INVALID_PARTITIONS,
                Some("Topic already has 2 partition(s).".into()),
            ),
        ])
        .call()
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
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_partition_topic()
        .name("dry-run")
        .partitions(1)
        .call()
        .await;
    check_topic_rows()
        .broker(&broker)
        .topics(vec![topic_req().name("dry-run").call()])
        .mutation(PartitionMutation::Validate)
        .expected(vec![expected_result("dry-run", codes::NONE, None)])
        .call()
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
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_partition_topic()
        .name("grow")
        .partitions(1)
        .call()
        .await;
    check_topic_rows()
        .broker(&broker)
        .topics(vec![
            topic_req()
                .name("grow")
                .assignments(vec![assn(&[1]), assn(&[1])])
                .call(),
        ])
        .expected(vec![expected_result("grow", codes::NONE, None)])
        .call()
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
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_partition_topic()
        .name("diskless-grow")
        .partitions(1)
        .call()
        .await;
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
        vec![
            topic_req()
                .name("diskless-grow")
                .count(2)
                .maybe_assignments(Some(vec![assn(&[1])]))
                .call(),
        ],
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
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_partition_topic().name("metered").call().await;
    seed_controller_quota(&broker_handle, 2.0).await;
    krabka_macros::request_test_identity! {
        bind(p, peer);
        user ["admin"];
    };
    check_topic_rows()
        .broker(&broker)
        .ctx(&test_context(&p, &peer))
        .topics(vec![topic_req().name("metered").count(5).call()])
        .expected(vec![expected_result("metered", codes::NONE, None)])
        .call()
        .await;

    let rejected = drive(
        &broker,
        &request(
            vec![topic_req().name("metered").count(6).call()],
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
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: _dir,
            broker,
        } = start_partition_topic()
            .name("metered")
            .partitions(1)
            .call()
            .await;
        krabka_macros::request_test_identity! { bind(p, peer); user ["admin"]; };
        seed_controller_quota(&broker_handle, 2.0).await;
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
        let topic = topic_req().name("metered").count(10).call();
        let concurrent = principal("unmetered");
        let response = drive(
            &broker,
            &request(
                vec![topic_req().name("metered").count(concurrent_count).call()],
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
                    vec![topic_req().name("metered").count(count).call()],
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
        (&[], &[], &[&[2, 3]], codes::NONE, None, vec![(n(2), vec![
            n(2),
            n(3),
        ])]),
        (&[2], &[], &[&[2, 3]], codes::NONE, None, vec![(
            n(3),
            vec![n(3)],
        )]),
        (&[2], &[], &[&[4, 2], &[2, 3]], codes::NONE, None, vec![
            (n(4), vec![n(4)]),
            (n(3), vec![n(3)]),
        ]),
        invalid_manual_assignment(&[2, 3], &[&[2, 3]], ALL_BROKERS_FENCED_PARTITION_ERROR),
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
        (&[], &[3], &[&[3, 4]], codes::NONE, None, vec![(
            n(4),
            vec![n(3), n(4)],
        )]),
    ];

    for (fenced, witnesses, lists, error_code, error_message, partitions) in rows {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
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
            vec![
                topic_req()
                    .name("grow")
                    .count(count)
                    .maybe_assignments(Some(lists.iter().map(|ids| assn(ids)).collect()))
                    .call(),
            ],
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
            .map(|(((leader, isr), replicas), partition)| {
                assigned_partition_record(
                    "grow",
                    partition,
                    leader,
                    replicas,
                    isr,
                    krabka_metadata::LeaderEpoch(0),
                )
            })
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
    struct GrowthRow {
        fenced: &'static [u64],
        shutting_down: &'static [u64],
        outcome: Result<usize, &'static str>,
    }

    let rows: [GrowthRow; 3] = [
        GrowthRow {
            fenced: &[4],
            shutting_down: &[],
            outcome: Ok(0),
        },
        GrowthRow {
            fenced: &[3, 4],
            shutting_down: &[],
            outcome: Ok(1),
        },
        GrowthRow {
            fenced: &[],
            shutting_down: &[3, 4],
            outcome: Err(
                "The target replication factor of 2 cannot be reached because only 1 broker(s) \
                 are registered or some brokers have all their log directories cordoned.",
            ),
        },
    ];

    for GrowthRow {
        fenced,
        shutting_down,
        outcome,
    } in rows
    {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
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
                vec![topic_req().name("grow").count(5).call()],
                PartitionMutation::Apply,
            ),
            &test_context(&principal("admin"), &peer()),
        )
        .await;

        let ctrl_image = broker_handle.controller_image_for_test();
        let added: Vec<_> = (1..)
            .map_while(|idx| ctrl_image.partition("grow", idx).cloned())
            .collect();
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
    let four_partition_growth = topic_req().count(4).call();
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
            vec![four_partition_growth.clone(), topic_req().count(6).call()],
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
                topic_req().name("u").call(),
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
            vec![topic_req().count(1).call()],
            PartitionMutation::Apply,
            vec![row(
                "t",
                codes::INVALID_PARTITIONS,
                Some("The topic t currently has 2 partition(s); 1 would not be an increase."),
            )],
        ),
        (
            "fewer assignments than new partitions",
            vec![
                topic_req()
                    .count(4)
                    .maybe_assignments(Some(vec![assn(&[1])]))
                    .call(),
            ],
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
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
        seed_topic().handle(&broker_handle).call().await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(p, peer);
            user ["admin"];
        };

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
    let LiveBrokerFixture {
        handle,
        directory: _dir,
        broker,
    } = start_partition_topic().name("t").partitions(1).call().await;
    let p = principal("admin");
    let h = peer();
    let ctx = test_context(&p, &h);
    let req = request(vec![topic_req().call()], PartitionMutation::Apply);
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
