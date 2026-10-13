//! The plain `kafka-reassign-partitions --execute` and `--verify` round-trip
//! against a three-broker SASL cluster.
//!
//! The move completes only after the added replica fetches the real log and
//! joins the ISR; no metadata record is injected by the test.

use {
    crate::{
        cluster::{
            admin_text_cluster, assert_reassigned, assignment, execute_plan, offline_for_partition,
            reassignment_cluster, removed_replica,
        },
        jvm_acceptance::{KAFKA_IMAGE_TXN, broker0_advertised, verify_console_reassignment},
        oracle::{AssignedTopicCommandSetup, CliRun, OracleComparison},
        support::{ContainerInput, JvmDockerSetup, jvm_docker_command},
    },
    assert2::assert,
    std::{io::Write as _, process::Stdio, time::Duration},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
#[allow(clippy::too_many_lines)] // Keeps the external CLI lifecycle in one end-to-end test.
async fn jvm_kafka_reassign_partitions_end_to_end() {
    const TOPIC: &str = "krabka-reassign-itest";

    let (cluster, _admin_props, admin_mount) = Box::pin(reassignment_cluster(TOPIC)).await;

    let mut producer = jvm_docker_command(JvmDockerSetup {
        image: KAFKA_IMAGE_TXN,
        mounts: &[&admin_mount],
        args: &[
            "kafka-console-producer",
            "--topic",
            TOPIC,
            "--bootstrap-server",
            broker0_advertised(),
            "--producer.config",
            "/client.properties",
        ],
        input: ContainerInput::Attached,
    })
    .stdin(Stdio::piped())
    .spawn()
    .expect("spawn kafka-console-producer");
    producer
        .stdin
        .as_mut()
        .expect("producer stdin")
        .write_all(b"before-reassignment\n")
        .expect("write record");
    drop(producer.stdin.take());
    assert!(producer.wait().expect("producer exit").success());

    let (initial, new_node, staying) = assignment(&cluster.h1, TOPIC, true);
    let (_json_file, json_mount) = execute_plan(&admin_mount, TOPIC, staying, new_node, false);

    let pr_after = cluster
        .h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after alter");
    let removing_replica = removed_replica(&pr_after, &initial);
    let handles = [&cluster.h1, &cluster.h2, &cluster.h3];
    let leader_leo = handles[usize::try_from(staying - 1).unwrap()]
        .local_log_end_offset(TOPIC, 0)
        .expect("leader log");
    assert!(leader_leo > 0, "the reassignment must move real records");
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if handles[usize::try_from(new_node - 1).unwrap()].local_log_end_offset(TOPIC, 0)
                == Some(leader_leo)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("added replica reaches the leader LEO");

    // Wait until adding_replicas and removing_replicas are both drained from
    // the committed metadata image.
    let completed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if cluster
                .h1
                .partition_record_for_test(TOPIC, 0)
                .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "reassignment did not complete: {:?}",
        cluster.h1.partition_record_for_test(TOPIC, 0)
    );
    // After completion the replica set must match [staying, new_node].
    assert_reassigned(&cluster.h1, TOPIC, staying, new_node);
    let dirs = [cluster.d1.path(), cluster.d2.path(), cluster.d3.path()];
    let removed_dir =
        dirs[usize::try_from(removing_replica.0 - 1).unwrap()].join(format!("{TOPIC}-0"));
    tokio::time::timeout(Duration::from_secs(30), async {
        while removed_dir.exists() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("removed replica directory is pruned");
    eprintln!("KRABKA[test] reassignment completed; running --verify");

    // --verify should report completion.
    let verify_out = verify_console_reassignment(&admin_mount, &json_mount);

    assert!(
        String::from_utf8_lossy(&verify_out.stdout)
            .to_ascii_lowercase()
            .contains("complete"),
        "verify did not report completion: {}",
        String::from_utf8_lossy(&verify_out.stdout)
    );

    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
}

// ── `--generate` and `--additional` ─────────────────────────────────────────
//
// The case above is `--execute` and `--verify`, which is the middle of the
// tool's workflow. The two ends were untested: `--generate`, which is how an
// operator obtains the document the other verbs read, and `--additional`,
// which is what stops a second `--execute` from cancelling the first one's
// work.

use {
    crate::{
        oracle::{Side, ToolFile},
        tool_output::{
            Assignment, TopicPartition, parse_generate, reassignment_json, topics_to_move_json,
        },
    },
    std::collections::BTreeSet,
};

/// Where the documents these cases write are placed inside the container.
const TOPICS_JSON: &str = "/tmp/krabka-topics-to-move.json";
const PLAN_JSON: &str = "/tmp/krabka-reassignment.json";
/// Where the SASL client configuration is mounted for the cluster cases.
const CLIENT_PROPS: &str = "/client.properties";

/// One `kafka-reassign-partitions` invocation, with the files it names.
fn reassign(side: &Side<'_>, props: Option<&str>, args: &[&str], files: Vec<ToolFile>) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    let mut files = files;
    if let Some(props) = props {
        full.extend_from_slice(&["--command-config", CLIENT_PROPS]);
        files.push(ToolFile::new(CLIENT_PROPS, props));
    }
    side.run_with_files("kafka-reassign-partitions", &full, &files, None)
}

/// What a `--generate` answer must be true of, whichever broker produced it.
///
/// Stated once and applied to both sides, so krabka's answer cannot be held to
/// a weaker rule than Kafka's. The proposal itself is not compared between the
/// sides: the two clusters have different broker sets on purpose -- one node
/// against three -- and `--generate` is a round-robin over whatever
/// `--broker-list` names, so equal proposals would mean the case had stopped
/// testing anything.
fn assert_generated_plan_is_usable(
    side: &str,
    topic: &str,
    partitions: i32,
    replication_factor: usize,
    brokers: &BTreeSet<i32>,
    current: &[Assignment],
    proposed: &[Assignment],
) {
    let expected: BTreeSet<TopicPartition> = (0..partitions)
        .map(|index| TopicPartition::new(topic, index))
        .collect();
    let covered = |plan: &[Assignment]| -> BTreeSet<TopicPartition> {
        plan.iter().map(|a| a.partition.clone()).collect()
    };
    assert!(
        covered(current) == expected,
        "{side}: the current assignment must cover every partition of {topic}: {current:?}",
    );
    assert!(
        covered(proposed) == expected,
        "{side}: the proposal must cover every partition of {topic}: {proposed:?}",
    );
    for assignment in current.iter().chain(proposed) {
        let replicas: BTreeSet<i32> = assignment.replicas.iter().copied().collect();
        assert!(
            replicas.len() == assignment.replicas.len()
                && replicas.len() == replication_factor
                && replicas.is_subset(brokers),
            "{side}: {assignment:?} must be {replication_factor} distinct brokers out of \
             {brokers:?}",
        );
    }
}

/// `--generate` produces a usable plan on krabka and on Apache Kafka.
///
/// The tool builds the proposal itself, out of `DescribeTopics` and
/// `DescribeCluster`; what a broker contributes is the current assignment and
/// the broker set. So the rule the two sides share is that the plan is
/// *usable* -- it covers the topic's partitions, and every replica in it is a
/// broker the operator named -- and a broker that mis-reported either would
/// hand the operator a document that `--execute` then refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reassign_partitions_generate_produces_a_usable_plan_on_both() {
    const TOPIC: &str = "krabka-generate-itest";
    const PARTITIONS: i32 = 3;

    let comparison = OracleComparison::start("reassign-generate").await;
    let [oracle_side, krabka_side] = comparison.sides();

    let document = topics_to_move_json(&[TOPIC]);
    for side in [&oracle_side, &krabka_side] {
        side.run("kafka-topics", &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            &PARTITIONS.to_string(),
            "--replication-factor",
            "1",
        ])
        .expect_success();

        let generated = reassign(
            side,
            None,
            &[
                "--generate",
                "--topics-to-move-json-file",
                TOPICS_JSON,
                "--broker-list",
                "1",
            ],
            vec![ToolFile::new(TOPICS_JSON, &document)],
        );
        assert!(
            generated.succeeded(),
            "{}: --generate failed:\n{}",
            side.label(),
            generated.text(),
        );
        let plans = parse_generate(&generated.stdout);
        let Some((current, proposed)) = plans else {
            panic!(
                "{}: --generate printed neither plan:\n{}",
                side.label(),
                generated.stdout,
            );
        };
        assert_generated_plan_is_usable(
            side.label(),
            TOPIC,
            PARTITIONS,
            1,
            &BTreeSet::from([1]),
            &current,
            &proposed,
        );
    }

    comparison.broker.shutdown().await;
}

/// A second `--execute --additional` leaves the first reassignment running.
///
/// Without `--additional` the tool cancels every reassignment its document
/// does not mention, which is the behaviour that loses an operator's
/// half-finished move when they start a second one. The flag is client-side,
/// but what it protects is server state, so the assertion is made against the
/// metadata image rather than against what the tool said about itself.
///
/// # Why this half has no oracle
///
/// A reassignment that is still running is one whose new replica has not
/// caught up, and on a single stock node there is no second broker to move a
/// replica to. The oracle in this file cannot host the premise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reassign_partitions_additional_keeps_the_reassignment_already_running() {
    const TOPIC: &str = "krabka-additional-itest";

    let (brokers, props, advertised) = Box::pin(admin_text_cluster()).await;
    let side = Side::Krabka {
        bootstrap: &advertised,
    };

    side.create_assigned_topic(AssignedTopicCommandSetup {
        topic: TOPIC,
        assignment: "1,1",
        props_path: CLIENT_PROPS,
        files: &[ToolFile::new(CLIENT_PROPS, &props)],
    });
    for partition in 0..2 {
        brokers
            .h1
            .wait_until_partition_present(TOPIC, partition)
            .await;
    }

    // Stop a registered, non-bootstrap target so the first move cannot race
    // replica catch-up and complete before the second command runs.
    let (offline_node, handles, first) = offline_for_partition(
        [brokers.h1, brokers.h2, brokers.h3],
        TOPIC,
        "partition record",
    )
    .await;
    let h1 = handles[0].as_ref().expect("bootstrap broker stays live");

    let staying = i32::try_from(first.replicas[0].0).expect("a node id fits");
    let first_plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(TOPIC, 0),
        replicas: vec![
            staying,
            i32::try_from(offline_node).expect("a node id fits"),
        ],
    }]);
    reassign(
        &side,
        Some(&props),
        &["--execute", "--reassignment-json-file", PLAN_JSON],
        vec![ToolFile::new(PLAN_JSON, &first_plan)],
    )
    .expect_success();
    h1.wait_for_image(|image| {
        image
            .partition(TOPIC, 0)
            .is_some_and(|record| !record.adding_replicas.is_empty())
    })
    .await;

    // A no-op assignment is enough to exercise the client's `--additional`
    // path. Its contract here is that it must not cancel partition 0.
    let second = h1
        .partition_record_for_test(TOPIC, 1)
        .expect("second partition record");
    let second_plan = reassignment_json(&[Assignment {
        partition: TopicPartition::new(TOPIC, 1),
        replicas: second
            .replicas
            .iter()
            .map(|node| i32::try_from(node.0).expect("a node id fits"))
            .collect(),
    }]);
    reassign(
        &side,
        Some(&props),
        &[
            "--execute",
            "--reassignment-json-file",
            PLAN_JSON,
            "--additional",
        ],
        vec![ToolFile::new(PLAN_JSON, &second_plan)],
    )
    .expect_success();

    let record = h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after the second execute");
    assert!(
        !record.adding_replicas.is_empty(),
        "partition 0 must still be reassigning after --additional: {record:?}",
    );

    for handle in handles.into_iter().flatten() {
        handle.shutdown().await;
    }
}
