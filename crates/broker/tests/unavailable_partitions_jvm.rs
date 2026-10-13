//! `kafka-topics --describe --unavailable-partitions` and
//! `--under-replicated-partitions` over a failed log directory, against krabka
//! and against Apache Kafka in the same shape.
//!
//! ## Why this suite exists
//!
//! Neither filter reads `offlineReplicas`. Disassembled out of the pinned
//! `kafka-tools-4.3.1.jar`, `TopicCommand$PartitionDescription` has no
//! reference to the field, and `TopicPartitionInfo` in
//! `kafka-clients-4.3.1.jar` has no member to carry it. The two predicates are
//!
//! ```text
//! hasUnavailablePartitions(live) = !hasLeader() || !live.contains(leader.id())
//! isUnderReplicated()            = replicationFactor - isr.size() > 0
//! ```
//!
//! so a partition whose only replica sits on a dead disk stays invisible to an
//! operator for as long as the broker reports it as led and in-sync, however
//! faithfully a third column names the disk. That is the bug epic #27 opened
//! with.
//!
//! ## What it asserts
//!
//! Both sides are put in the same shape -- one broker, two log directories, a
//! topic spread across both, then one directory killed -- and the same two
//! commands are run against each from the same
//! `mirror.gcr.io/apache/kafka:4.3.1` image. The rule is stated once, in
//! [`assert_health_filters_agree`], and applied to both: each filter prints
//! exactly the partitions on the dead disk, prints them with `Leader: none`
//! and an empty ISR, and leaves the partitions on the surviving disk alone.
//!
//! Running it against real Kafka is what makes the krabka half worth anything.
//! A hand-written expectation can be wrong about Kafka; this one fails on the
//! oracle first if it is.
//!
//! Gated `#[ignore]` (requires Docker); the Bazel target that owns this suite
//! runs it with `--ignored`.

mod support;

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    std::{
        collections::BTreeSet,
        process::Command,
        time::{Duration, Instant},
    },
};

/// The release both halves of the differential use: krabka's client is this
/// image's `kafka-topics`, and the oracle broker is the same image.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The tool, at its path inside the image.
const TOOL: &str = "/opt/kafka/bin/kafka-topics.sh";

const TOPIC: &str = "unavailable-partitions";

/// Four partitions at RF=1. Two log directories and a least-loaded placement
/// put two on each, so every run has both a doomed partition and a survivor,
/// which is what stops a filter that prints everything from passing.
const PARTITIONS: i32 = 4;

/// How long a side gets to notice its own dead disk.
const CONVERGE_BUDGET: Duration = Duration::from_secs(120);

/// The pause between one poll and the next.
const POLL_GAP: Duration = Duration::from_secs(1);

/// One `kafka-topics --describe` partition row, in the columns the health
/// filters are about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Row {
    partition: i32,
    /// `None` is the `Leader: none` the tool prints for a `-1` leader.
    leader: Option<i32>,
    isr: Vec<i32>,
}

/// Parse the partition rows out of a `kafka-topics --describe` stdout.
///
/// The tool prints one tab-separated `Key: value` field per column, after a
/// topic-summary line that carries no `Partition:` field. `Isr:` with nothing
/// after it is an empty ISR, not a missing column, which is exactly the case
/// this suite is about.
fn parse_rows(stdout: &str) -> Vec<Row> {
    let mut rows: Vec<Row> = stdout
        .lines()
        .filter(|line| line.contains("Partition:"))
        .map(|line| {
            let field = |key: &str| -> String {
                let prefix = format!("{key}:");
                line.split('\t')
                    .filter_map(|part| part.trim().strip_prefix(&prefix).map(str::trim))
                    .map(str::to_string)
                    .next()
                    .unwrap_or_default()
            };
            let ids = |value: &str| -> Vec<i32> {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(|id| id.parse().expect("replica id is numeric"))
                    .collect()
            };
            let leader = field("Leader");
            Row {
                partition: field("Partition").parse().expect("partition is numeric"),
                leader: if leader == "none" {
                    None
                } else {
                    Some(leader.parse().expect("leader id is numeric"))
                },
                isr: ids(&field("Isr")),
            }
        })
        .collect();
    rows.sort();
    rows
}

/// The partition indices a set of rows covers.
fn indices(rows: &[Row]) -> BTreeSet<i32> { rows.iter().map(|row| row.partition).collect() }

/// What one side of the differential answered for one cluster in the
/// failed-disk shape.
#[derive(Debug)]
struct HealthReport {
    /// The partitions this side placed on the directory that was then killed,
    /// established from the cluster's own state rather than from the tool.
    doomed: BTreeSet<i32>,
    /// Every partition of the topic, from a plain `--describe`.
    all: Vec<Row>,
    /// `--describe --unavailable-partitions`.
    unavailable: Vec<Row>,
    /// `--describe --under-replicated-partitions`.
    under_replicated: Vec<Row>,
}

/// The rule both sides are held to.
///
/// Stated once so the krabka half cannot be checked against a weaker rule than
/// the Kafka half.
fn assert_health_filters_agree(side: &str, report: &HealthReport) {
    assert!(
        !report.doomed.is_empty() && report.doomed.len() < usize::try_from(PARTITIONS).unwrap(),
        "{side}: the premise needs partitions on both directories, got {report:?}",
    );
    assert!(
        indices(&report.unavailable) == report.doomed,
        "{side}: --unavailable-partitions must print exactly the partitions on the dead disk: \
         {report:?}",
    );
    assert!(
        indices(&report.under_replicated) == report.doomed,
        "{side}: --under-replicated-partitions must print exactly the partitions on the dead \
         disk: {report:?}",
    );
    for row in &report.all {
        let expected_leaderless = report.doomed.contains(&row.partition);
        assert!(
            row.leader.is_none() == expected_leaderless
                && row.isr.is_empty() == expected_leaderless,
            "{side}: partition {} is {}on the dead disk, so it must {}: {report:?}",
            row.partition,
            if expected_leaderless { "" } else { "not " },
            if expected_leaderless {
                "report no leader and an empty ISR"
            } else {
                "keep its leader and ISR"
            },
        );
    }
}

/// `kafka-topics --describe` plus the two health filters, all through one
/// `describe_one`, so both sides go through the identical code path.
fn health_report(describe_one: &dyn Fn(&[&str]) -> String, doomed: BTreeSet<i32>) -> HealthReport {
    HealthReport {
        doomed,
        all: parse_rows(&describe_one(&[])),
        unavailable: parse_rows(&describe_one(&["--unavailable-partitions"])),
        under_replicated: parse_rows(&describe_one(&["--under-replicated-partitions"])),
    }
}

/// Poll `describe_one` until a partition of `TOPIC` reports no leader, so the
/// cluster has finished reacting to its dead disk.
fn wait_until_a_partition_is_leaderless(side: &str, describe_one: &dyn Fn(&[&str]) -> String) {
    let deadline = Instant::now() + CONVERGE_BUDGET;
    loop {
        let rows = parse_rows(&describe_one(&[]));
        if rows.iter().any(|row| row.leader.is_none()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{side}: no partition of {TOPIC} lost its leader within {CONVERGE_BUDGET:?}: {rows:?}",
        );
        std::thread::sleep(POLL_GAP);
    }
}

/// Run `kafka-topics` from a throwaway container against a broker outside it.
fn kafka_topics_against(bootstrap: &str, args: &[&str]) -> String {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        KAFKA_IMAGE,
        TOOL,
        "--bootstrap-server",
        bootstrap,
    ];
    full.extend_from_slice(args);
    run_docker("kafka-topics", &full)
}

/// Run a `docker` subcommand, print it for the log, and return its stdout.
/// Panics unless it succeeded.
fn run_docker(what: &str, args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn docker {what}: {e}"));
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    eprintln!(
        "KRABKA[test] docker {what} {:?} status={}\nstdout:\n{stdout}\nstderr:\n{}",
        &args[1..],
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(out.status.success(), "docker {what} failed");
    stdout
}

// ---------------------------------------------------------------- krabka side

/// One krabka broker over two log directories, with one of them killed.
struct KrabkaCluster {
    handle: BrokerHandle,
    bootstrap: String,
    doomed: BTreeSet<i32>,
    _primary: tempfile::TempDir,
    _extra: tempfile::TempDir,
}

impl KrabkaCluster {
    /// Boot the broker, create the topic with the real tool, wait for every
    /// replica to be attributed to a directory, then kill the second one.
    async fn start() -> Self {
        support::init_tracing();
        let primary = tempfile::tempdir().expect("primary log dir");
        let extra = tempfile::tempdir().expect("extra log dir");
        let (handle, bootstrap) =
            support::start_jvm_bound(primary.path().to_path_buf(), |config| {
                config.extra_log_dirs = vec![extra.path().to_path_buf()];
            })
            .await;

        let created = kafka_topics_against(&bootstrap, &[
            "--create",
            "--topic",
            TOPIC,
            "--partitions",
            &PARTITIONS.to_string(),
            "--replication-factor",
            "1",
        ]);
        assert!(created.contains("Created topic"), "kafka-topics --create");
        for partition in 0..PARTITIONS {
            handle.wait_until_partition_present(TOPIC, partition).await;
        }

        // Until the replicator supervisor has reported every local replica's
        // owning directory, `directories` holds the unassigned sentinel and no
        // replica can be attributed to a disk.
        let deadline = tokio::time::Instant::now() + CONVERGE_BUDGET;
        while handle
            .controller_image_for_test()
            .partitions_of(TOPIC)
            .filter(|p| p.directories.first().is_some_and(|d| !d.is_nil()))
            .count()
            != usize::try_from(PARTITIONS).unwrap()
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "krabka: replica directories were never assigned",
            );
            tokio::time::sleep(POLL_GAP).await;
        }

        let extra_id = krabka_broker::log_dir_id::LogDirIds::resolve(&[
            primary.path().into(),
            extra.path().into(),
        ])
        .id_for(extra.path())
        .expect("extra dir id");
        let doomed: BTreeSet<i32> = handle
            .controller_image_for_test()
            .partitions_of(TOPIC)
            .filter(|p| p.directories.first() == Some(&extra_id))
            .map(|p| p.partition)
            .collect();

        // Only the second directory: with the first still online the broker
        // does not take the all-dirs-offline self-shutdown path.
        assert!(
            handle.test_mark_log_dir_offline(extra.path()),
            "the extra dir must have been registered and online",
        );

        Self {
            handle,
            bootstrap,
            doomed,
            _primary: primary,
            _extra: extra,
        }
    }

    /// `kafka-topics --describe --topic <TOPIC> <args>` against this broker.
    ///
    /// The `--topic` filter is not decoration: without it a health filter
    /// answers for every topic on the broker, and a partition of some *other*
    /// topic on the same dead disk would be read as one of this topic's.
    fn describe_one(&self) -> impl Fn(&[&str]) -> String + '_ {
        move |args: &[&str]| {
            let mut full = vec!["--describe", "--topic", TOPIC];
            full.extend_from_slice(args);
            kafka_topics_against(&self.bootstrap, &full)
        }
    }

    fn health_report(&self) -> HealthReport {
        let describe_one = self.describe_one();
        wait_until_a_partition_is_leaderless("krabka", &describe_one);
        health_report(&describe_one, self.doomed.clone())
    }

    async fn shutdown(self) { self.handle.shutdown().await; }
}

// ---------------------------------------------------------------- oracle side

/// The oracle's second log directory, mounted on a tmpfs this small so that
/// filling it is quick.
const ORACLE_TMPFS: &str = "/var/lib/kafka/data2";

/// Enough 1 KiB records to overrun that tmpfs several times over even if the
/// placement puts only one partition on it.
const ORACLE_RECORDS: &str = "20000";

/// A single-node `apache/kafka` broker over two log directories, one of them
/// on a tiny tmpfs. Dropping it removes the container.
struct OracleCluster {
    name: String,
    doomed: BTreeSet<i32>,
}

impl OracleCluster {
    /// Boot the broker, create the topic, then fill the tmpfs until the write
    /// path fails with `No space left on device`, which is what Kafka's
    /// `LogDirFailureChannel` reacts to.
    fn start() -> Self {
        let name = support::unique_container_name("krabka-unavailable-oracle");
        let tmpfs = format!("{ORACLE_TMPFS}:size=4m");
        let log_dirs = format!("KAFKA_LOG_DIRS=/var/lib/kafka/data1,{ORACLE_TMPFS}");
        let mut args = vec!["run", "-d", "--name", &name, "--tmpfs", &tmpfs];
        args.extend_from_slice(support::kafka_single_node_env_args());
        args.extend_from_slice(&[
            "-e",
            &log_dirs,
            "-e",
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
            KAFKA_IMAGE,
        ]);
        run_docker("run kafka oracle", &args);
        let mut oracle = Self {
            name,
            doomed: BTreeSet::new(),
        };
        oracle.wait_ready();
        oracle.create_topic();
        oracle.fill_the_tmpfs();
        oracle.doomed = oracle.partitions_on_tmpfs();
        oracle
    }

    /// The tool, aimed at the oracle's own loopback listener from inside its
    /// container, so the oracle needs no published port and cannot collide
    /// with a concurrent suite.
    fn exec(&self, args: &[&str]) -> std::process::Output { support::docker_exec(&self.name, args) }

    fn wait_ready(&self) {
        let deadline = Instant::now() + CONVERGE_BUDGET;
        while !self
            .exec(&[TOOL, "--bootstrap-server", "localhost:9092", "--list"])
            .status
            .success()
        {
            assert!(
                Instant::now() < deadline,
                "{KAFKA_IMAGE} did not answer within {CONVERGE_BUDGET:?}:\n{}",
                self.logs(),
            );
            std::thread::sleep(POLL_GAP);
        }
    }

    fn create_topic(&self) {
        let out = self.exec(&[
            TOOL,
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--topic",
            TOPIC,
            "--partitions",
            &PARTITIONS.to_string(),
            "--replication-factor",
            "1",
        ]);
        assert!(
            out.status.success(),
            "oracle kafka-topics --create failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
    }

    /// Produce until the tmpfs is out of space, which is the `IOException: No
    /// space left on device` that Kafka's `LogDirFailureChannel` reacts to.
    ///
    /// Every send aimed at the dead directory then expires rather than being
    /// rejected, so the timeouts are bounded: the defaults leave the tool
    /// waiting two minutes for batches that will never land. `delivery.timeout`
    /// must stay at or above `linger + request.timeout`, or the producer
    /// refuses to start at all.
    fn fill_the_tmpfs(&self) {
        let out = self.exec(&[
            "/opt/kafka/bin/kafka-producer-perf-test.sh",
            "--topic",
            TOPIC,
            "--num-records",
            ORACLE_RECORDS,
            "--record-size",
            "1024",
            "--throughput",
            "-1",
            "--producer-props",
            "bootstrap.servers=localhost:9092",
            "acks=1",
            "linger.ms=0",
            "request.timeout.ms=5000",
            "delivery.timeout.ms=10000",
            "max.block.ms=10000",
        ]);
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        eprintln!(
            "KRABKA[test] oracle producer status={}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            out.status,
        );
        // The expired sends make the run itself look like a failure, so the
        // premise is that records reached the broker at all -- a producer that
        // never started (a rejected client config, say) prints no summary and
        // would otherwise be diagnosed as krabka missing a leaderless
        // partition ninety seconds later.
        assert!(
            stdout.contains("records sent"),
            "the oracle producer never sent anything: {stdout}{stderr}",
        );
    }

    /// The partitions whose directory sits on the tmpfs, read off the
    /// container's own filesystem rather than out of the tool being tested.
    fn partitions_on_tmpfs(&self) -> BTreeSet<i32> {
        let out = self.exec(&["ls", ORACLE_TMPFS]);
        assert!(out.status.success(), "oracle ls {ORACLE_TMPFS} failed");
        let prefix = format!("{TOPIC}-");
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|entry| entry.trim().strip_prefix(&prefix))
            .map(|index| index.parse().expect("partition dir index is numeric"))
            .collect()
    }

    fn describe_one(&self) -> impl Fn(&[&str]) -> String + '_ {
        move |args: &[&str]| {
            let mut full: Vec<&str> = vec![
                TOOL,
                "--bootstrap-server",
                "localhost:9092",
                "--describe",
                "--topic",
                TOPIC,
            ];
            full.extend_from_slice(args);
            let out = self.exec(&full);
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            eprintln!(
                "KRABKA[test] oracle kafka-topics {args:?} status={}\nstdout:\n{stdout}\nstderr:\n{}",
                out.status,
                String::from_utf8_lossy(&out.stderr),
            );
            assert!(
                out.status.success(),
                "oracle kafka-topics --describe failed"
            );
            stdout
        }
    }

    fn health_report(&self) -> HealthReport {
        let describe_one = self.describe_one();
        wait_until_a_partition_is_leaderless("apache/kafka:4.3.1", &describe_one);
        health_report(&describe_one, self.doomed.clone())
    }

    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .expect("spawn docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }
}

impl Drop for OracleCluster {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

// ---------------------------------------------------------------------- suite

/// One cluster of each kind, the same two commands against both, the same rule
/// applied to both answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn both_health_filters_print_a_partition_on_a_failed_log_dir() {
    // Kafka first: if the rule below is wrong about Kafka, this is where the
    // suite says so, before krabka is blamed for missing it.
    let oracle = tokio::task::spawn_blocking(OracleCluster::start)
        .await
        .expect("oracle boot");
    assert_health_filters_agree("apache/kafka:4.3.1", &oracle.health_report());

    let krabka = KrabkaCluster::start().await;
    assert_health_filters_agree("krabka", &krabka.health_report());

    krabka.shutdown().await;
}
