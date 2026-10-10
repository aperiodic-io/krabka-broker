//! Preferred-leader election driven by `kafka-leader-election
//! --election-type preferred` against a three-broker SASL cluster.
//!
//! The scenario needs a partition whose preferred replica is alive, in the ISR,
//! and not currently the leader, which this file reaches by metadata injection
//! rather than by killing a broker; the test's own doc comment explains why.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image_and_mount,
    wait_jvm_isr_contains, wait_jvm_partition_any_leader, wait_jvm_partition_leader,
};

/// JVM acceptance test for `kafka-leader-election --election-type preferred`.
///
/// The cluster has three brokers because `controllerId` rotates over the
/// unfenced brokers rather than naming the quorum leader, so the
/// `AdminClient` inside the tool may send `ElectLeaders` to any of them. A
/// two-broker cluster would hide an election that only one node answers
/// correctly, which is the regression this case exists to catch.
///
/// Scenario:
/// 1. Boot a 3-broker `SASL_PLAINTEXT` cluster and create an rf=2 topic.
/// 2. Wait for the cluster to assign a leader. Expect broker 1, the
///    preferred replica.
/// 3. Move leadership to broker 2 by injecting a `PartitionRecord`, keeping
///    broker 1 in the ISR. The comment at the injection says why the test
///    does not kill a broker to get there.
/// 4. Wait until every broker would let an operator elect broker 1.
/// 5. Run `kafka-leader-election --election-type preferred` from the JVM CLI
///    image cp-kafka:7.5.0. Older images do not ship this tool.
/// 6. Assert Docker exits 0.
/// 7. Poll until broker 1 is leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_leader_election_preferred() {
    const TOPIC: &str = "krabka-elect-preferred-itest";

    let (cluster, admin_props) = Box::pin(crate::cluster::admin_cluster()).await;
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic as super-user via the 7.5 JVM image.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--replica-assignment",
            "1:2",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );

    // Wait for broker 1 to see the partition in the committed metadata image.
    cluster.h1.wait_until_partition_present(TOPIC, 0).await;

    // Record the initial leader (should be broker 1 as preferred replica).
    let initial_leader = wait_jvm_partition_any_leader(&cluster.h1, TOPIC, 0).await;
    eprintln!("KRABKA[test] initial partition leader: {initial_leader}");

    // For the preferred election to do anything interesting we need broker 1
    // to be the preferred (replicas[0]). The assignment above is [1, 2], since
    // an automatic placement would start at a random broker. Assert it held.
    assert!(
        initial_leader == 1,
        "expected broker 1 to be the initial/preferred leader; got {initial_leader}"
    );

    // Inject a PartitionRecord that makes broker 2 the current leader while
    // keeping broker 1 in the ISR as a non-leader replica.
    //
    // This simulates the "preferred replica is not current leader" scenario
    // that `kafka-leader-election --election-type preferred` is designed to
    // fix. We use metadata injection rather than an organic leader change
    // because:
    //
    // 1. An organic leader change requires killing broker 1, and the raft
    //    re-election that follows leaves the surviving controller without a
    //    heartbeat record for broker 2 until broker 2 heartbeats to it. The
    //    controller publishes that gap as a fencing, so `ElectLeaders` refuses
    //    with `PreferredNotAlive` for the width of the window.
    //
    // 2. Under WSL2, inter-broker replication flows through the Windows-host IP
    //    (`host.docker.internal` = 192.168.65.254), not back into the WSL VM
    //    where the peers live, so organic ISR expansion would time out anyway.
    //
    // Metadata injection bypasses both limitations and matches the technique
    // used by `tests/elect_leaders.rs::unclean_election_via_wire_picks_alive_replica`.
    cluster
        .h1
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
            crate::cluster::election_partition(
                TOPIC,
                krabka_broker::NodeId(2),
                vec![krabka_broker::NodeId(1), krabka_broker::NodeId(2)],
                vec![krabka_broker::NodeId(2), krabka_broker::NodeId(1)],
            ),
        ))
        .await
        .expect("inject PartitionRecord making broker 2 the leader");

    // Wait for the injected state to propagate to broker 2's metadata image:
    // leader=2, ISR contains both 1 and 2.
    wait_jvm_partition_leader(&cluster.h2, TOPIC, 0, 2).await;
    wait_jvm_isr_contains(&cluster.h2, TOPIC, 0, 1).await;

    // A PREFERRED election refuses with PREFERRED_LEADER_NOT_AVAILABLE until
    // broker 1 is electable, and neither wait above implies that: the leader
    // and the ISR are replicated partition state, while a broker becomes
    // electable only once the controller has seen its heartbeat and published
    // it unfenced. `controllerId` rotates over the unfenced brokers, so the
    // `AdminClient` sends this election to any of the three and every one of
    // them must agree. Settle on all three rather than on whichever the
    // rotation happens to name.
    for handle in [&cluster.h1, &cluster.h2, &cluster.h3] {
        handle.wait_until_broker_electable(1).await;
    }
    eprintln!(
        "KRABKA[test] broker 2 is current leader; broker 1 is in ISR and electable \
         everywhere — running preferred election"
    );

    // Run kafka-leader-election via the 7.5 JVM image.
    // kafka-leader-election is NOT present in cp-kafka:6.1.1 (Kafka 2.7).
    // cp-kafka:7.5.0 (Kafka 3.5) ships it. The tool sends `ElectLeaders`
    // (api_key 43) which the Rust broker now handles via T4/T5.
    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE_TXN,
        mounts: &[&admin_mount],
        args: &[
            "kafka-leader-election",
            "--election-type",
            "preferred",
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--bootstrap-server",
            broker0_advertised(),
            "--admin.config",
            "/client.properties",
        ],
        ..Default::default()
    })
    .output()
    .expect("spawn kafka-leader-election");

    let election_stdout = String::from_utf8_lossy(&out.stdout);
    let election_stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!(
        "KRABKA[test] kafka-leader-election status={} stdout={election_stdout} stderr={election_stderr}",
        out.status
    );
    assert!(
        out.status.success(),
        "kafka-leader-election failed: stdout={election_stdout} stderr={election_stderr}",
    );

    // Poll until broker 1 is the leader again on broker 2's view.
    wait_jvm_partition_leader(&cluster.h2, TOPIC, 0, 1).await;
    eprintln!("KRABKA[test] preferred election confirmed: broker 1 is leader again");

    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
}

// ── the two partition selectors, and the two per-partition codes ────────────
//
// The case above drives `--election-type preferred --topic --partition`, which
// is one partition named three ways over. `kafka-leader-election` has two more
// selectors and neither reached a suite: `--all-topic-partitions`, which sends
// a null `topic_partitions` and means every partition the broker knows, and
// `--path-to-json-file`, which names a set in a document.
//
// The codes matter as much as the selectors. `ElectLeaders` reports per
// partition, so a request can succeed while every partition in it failed, and
// the tool sorts the rows into three different sentences: an election it
// performed, an `ELECTION_NOT_NEEDED` (84) it folds into `Valid replica
// already elected`, and anything else as an error naming the exception
// `Errors.forCode` built. A broker that answered 80 where Kafka answers 84
// prints an incident where Kafka prints a no-op, and no in-process test sees
// the difference.

use crate::{
    oracle::{Side, ToolFile},
    tool_output::{ElectionOutcome, TopicPartition, election_json, parse_election},
};

/// The topic the two selectors are pointed at.
const SELECTOR_TOPIC: &str = "krabka-elect-selectors-itest";

/// Its partition count. More than one, so a selector that answered for a
/// single partition would be visibly short.
const SELECTOR_PARTITIONS: i32 = 3;

/// Where the `--path-to-json-file` document is placed inside whichever
/// container the tool runs in.
const ELECTION_JSON: &str = "/tmp/krabka-election.json";

/// Every partition of [`SELECTOR_TOPIC`].
fn selector_partitions() -> Vec<TopicPartition> {
    (0..SELECTOR_PARTITIONS)
        .map(|index| TopicPartition::new(SELECTOR_TOPIC, index))
        .collect()
}

/// `--all-topic-partitions` and `--path-to-json-file` over a cluster where
/// every partition already sits on its preferred leader, compared with Apache
/// Kafka.
///
/// A topic at replication factor one is preferred-elected by construction: its
/// only replica is `replicas[0]` and it leads. So both selectors are asked a
/// question whose answer is `ELECTION_NOT_NEEDED` for every partition, which
/// is the one per-partition code an operator meets on a healthy cluster and
/// the one this suite could not otherwise reach.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn election_selectors_report_election_not_needed_as_apache_kafka_does() {
    // Kafka first: the claim that a healthy partition answers 84 rather than
    // succeeding is a claim about Kafka, and this is where a wrong one fails.
    let comparison = crate::oracle::OracleComparison::start("elect-selectors").await;
    let [oracle_side, krabka_side] = comparison.sides();

    let document = election_json(&selector_partitions());
    let mut answers = Vec::new();
    for side in [&oracle_side, &krabka_side] {
        side.run(
            "kafka-topics",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--create",
                "--if-not-exists",
                "--topic",
                SELECTOR_TOPIC,
                "--partitions",
                &SELECTOR_PARTITIONS.to_string(),
                "--replication-factor",
                "1",
            ],
        )
        .expect_success();

        // The document selector first. It names exactly this topic's
        // partitions, so its answer is this topic's answer and nothing else's.
        let by_file = side.run_with_files(
            "kafka-leader-election",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--election-type",
                "preferred",
                "--path-to-json-file",
                ELECTION_JSON,
            ],
            &[ToolFile::new(ELECTION_JSON, &document)],
            None,
        );
        let outcomes = parse_election(&by_file.text());
        let expected: std::collections::BTreeMap<_, _> = selector_partitions()
            .into_iter()
            .map(|partition| (partition, ElectionOutcome::AlreadyElected))
            .collect();
        assert!(
            outcomes == expected,
            "{}: --path-to-json-file must report 84 for every partition, got {outcomes:?}\n{}",
            side.label(),
            by_file.text(),
        );

        // And the whole-cluster selector. Its answer covers the internal
        // topics too, so it is narrowed to this topic's partitions before it
        // is compared -- the coordinator topics differ between the two sides
        // and say nothing about the selector.
        let by_all = side.run(
            "kafka-leader-election",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--election-type",
                "preferred",
                "--all-topic-partitions",
            ],
        );
        let mine: std::collections::BTreeMap<_, _> = parse_election(&by_all.text())
            .into_iter()
            .filter(|(partition, _)| partition.topic == SELECTOR_TOPIC)
            .collect();
        answers.push(mine);
    }
    assert!(
        answers[0] == answers[1],
        "--all-topic-partitions: krabka and Apache Kafka disagreed: {answers:?}",
    );

    comparison.broker.shutdown().await;
}

/// A partition whose preferred replica is out of the ISR reports
/// `PREFERRED_LEADER_NOT_AVAILABLE` (80), and the tool builds Kafka's own
/// exception from it.
///
/// # Why this half has no oracle
///
/// Code 80 needs a partition whose `replicas[0]` is alive but not electable,
/// which needs a replica set larger than one, which needs more than one
/// broker. The oracle in this file is a single stock node, so it cannot be put
/// in that shape at all -- and a multi-node stock cluster is a different
/// harness, not a variation on this one.
///
/// What is still cross-validated is the half that matters most: `Errors` is
/// Kafka's, and `LeaderElectionCommand` is Kafka's, so the class name asserted
/// below is what Kafka's own client built out of the number krabka sent. A
/// broker that answered 84, or 41, or an unassigned code would print a
/// different class here, or none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn a_preferred_replica_outside_the_isr_reports_code_80() {
    const TOPIC: &str = "krabka-elect-unavailable-itest";

    let (brokers, props, advertised) = Box::pin(crate::cluster::admin_text_cluster()).await;
    let side = Side::Krabka {
        bootstrap: &advertised,
    };
    let props_file = ToolFile::new("/client.properties", &props);

    side.run_with_files(
        "kafka-topics",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--command-config",
            "/client.properties",
        ],
        std::slice::from_ref(&props_file),
        None,
    )
    .expect_success();
    brokers.h1.wait_until_partition_present(TOPIC, 0).await;

    // Keep a registered non-controller broker offline so the injected
    // preferred replica cannot race back into the ISR before the JVM command.
    let controller_leader = brokers.h1.wait_until_controller_leader().await.0;
    let preferred = (2_u64..=3)
        .find(|node| *node != controller_leader)
        .expect("a non-bootstrap, non-controller broker");
    let mut handles = [Some(brokers.h1), Some(brokers.h2), Some(brokers.h3)];
    handles[usize::try_from(preferred - 1).unwrap()]
        .take()
        .expect("preferred replica handle")
        .shutdown()
        .await;
    let h1 = handles[0].as_ref().expect("bootstrap broker stays live");

    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
        crate::cluster::election_partition(
            TOPIC,
            krabka_broker::NodeId(1),
            vec![krabka_broker::NodeId(preferred), krabka_broker::NodeId(1)],
            vec![krabka_broker::NodeId(1)],
        ),
    ))
    .await
    .expect("inject a partition whose preferred replica is outside the ISR");
    h1.wait_for_image(|image| {
        image.partition(TOPIC, 0).is_some_and(|record| {
            record.leader == krabka_broker::NodeId(1)
                && record.replicas.first() == Some(&krabka_broker::NodeId(preferred))
                && record.isr == [krabka_broker::NodeId(1)]
        })
    })
    .await;
    wait_jvm_partition_leader(h1, TOPIC, 0, 1).await;

    let document = election_json(&[TopicPartition::new(TOPIC, 0)]);
    let run = side.run_with_files(
        "kafka-leader-election",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--election-type",
            "preferred",
            "--path-to-json-file",
            ELECTION_JSON,
            "--admin.config",
            "/client.properties",
        ],
        &[
            ToolFile::new("/client.properties", &props),
            ToolFile::new(ELECTION_JSON, &document),
        ],
        None,
    );
    let outcomes = parse_election(&run.text());
    assert!(
        outcomes
            == std::collections::BTreeMap::from([(
                TopicPartition::new(TOPIC, 0),
                ElectionOutcome::Failed(
                    "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException"
                        .to_owned(),
                ),
            )]),
        "the election must report code 80 as Kafka's own exception, got {outcomes:?}\n{}",
        run.text(),
    );

    for handle in handles.into_iter().flatten() {
        handle.shutdown().await;
    }
}
