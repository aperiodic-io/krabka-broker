//! The `kafka-consumer-groups` administration tool: `--list`, `--describe`, and
//! the KIP-496 `--delete-offsets` path.
//!
//! These runs exercise the JVM `AdminClient` group APIs rather than a consumer,
//! so they stay apart from the `kafka-console-consumer` suites.

use std::io::Write as _;

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE, broker0_advertised, docker_run_kafka_tool};

/// `kafka-consumer-groups --list` and `--describe` round-trip after a
/// real consumer has joined a group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_list_describe() {
    const TOPIC: &str = "krabka-cg-list-itest";
    const GROUP: &str = "krabka-cg-list-grp";

    let (_broker, _dir) = crate::jvm_acceptance::start_console_group(TOPIC, GROUP, 1).await;

    let list_out = docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--list",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&list_out.stdout);
    assert!(s.contains(GROUP), "list output missing {GROUP}: {s}");

    let desc_out = crate::jvm_acceptance::describe_console_group(GROUP);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        s.contains(TOPIC),
        "describe output missing topic {TOPIC}: {s}"
    );
}

/// `kafka-consumer-groups --delete-offsets` exercises `OffsetDelete`
/// (`api_key` 47, KIP-496) end-to-end against `cp-kafka:6.1.1`. The JVM
/// `AdminClient` flow under this CLI runs `FindCoordinator` →
/// `DescribeGroups` → `OffsetDelete`. After the consumer exits, the group
/// is `Empty`, so the KIP-496 subscription guard skips and the tombstone
/// path runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_consumer_groups_delete_offsets() {
    const TOPIC: &str = "krabka-cg-delete-offsets-itest";
    const GROUP: &str = "krabka-cg-delete-offsets-grp";

    let (_broker, _dir) = crate::jvm_acceptance::start_console_group(TOPIC, GROUP, 2).await;

    // Sanity: --describe before delete should list TOPIC for GROUP. If this
    // fails, the failure is on the commit/coordinator path — not on
    // OffsetDelete — and the test would otherwise pass-by-accident below.
    let pre_desc = crate::jvm_acceptance::describe_console_group(GROUP);
    let pre_s = String::from_utf8_lossy(&pre_desc.stdout);
    assert!(
        pre_s.contains(TOPIC),
        "pre-delete --describe missing {TOPIC}: {pre_s}"
    );

    // Run --delete-offsets via a piped-stdin spawn so any Y/N prompt the
    // 2.7 build may emit is satisfied. `kafka-consumer-groups` in 2.7
    // generally does not prompt for --delete-offsets when all flags are
    // supplied; the piped "y\n" is defensive and ignored otherwise.
    let mut child = crate::support::jvm_spawn_piped(
        &mut crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
            image: KAFKA_IMAGE,
            args: &[
                "kafka-consumer-groups",
                "--bootstrap-server",
                broker0_advertised(),
                "--delete-offsets",
                "--group",
                GROUP,
                "--topic",
                TOPIC,
            ],
            input: crate::support::ContainerInput::Attached,
            ..Default::default()
        }),
        "spawn delete-offsets",
    );
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(stdin, "y").expect("write y");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait delete-offsets");
    assert!(
        out.status.success(),
        "delete-offsets failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let s = String::from_utf8_lossy(&out.stdout);
    // Kafka 2.7 prints a "TOPIC | PARTITION | STATUS" table with
    // "Successful" per row on success. Be lenient: any of the indicators
    // is enough since header formatting drifts across CLI versions.
    assert!(
        s.contains("Successful") || s.contains(TOPIC),
        "delete-offsets stdout missing success indicator: {s}"
    );

    // Post-delete --describe: no data row should reference TOPIC for
    // GROUP. Header text may still mention column names, so guard with a
    // line-level check that the line both belongs to GROUP and refers to
    // TOPIC.
    let post_desc = crate::jvm_acceptance::describe_console_group(GROUP);
    let post_s = String::from_utf8_lossy(&post_desc.stdout);
    let leaked = post_s
        .lines()
        .any(|l| l.starts_with(GROUP) && l.contains(TOPIC));
    assert!(
        !leaked,
        "post-delete --describe still shows {TOPIC} for {GROUP}: {post_s}"
    );
}

// ── `--reset-offsets` and `--delete`, against the stock broker ──────────────
//
// Everything below runs the *same* `kafka-consumer-groups` binary, out of the
// same `apache/kafka:4.3.1` image, against krabka and against a stock broker of
// that release, and compares what the tool printed once it has been parsed.
// The two clusters are put in the same shape first, by the same commands, so a
// difference in the answer is a difference in the broker.
//
// The oracle is asked first in every case. If the rule a case states is wrong
// about Kafka, that is where the suite says so, before krabka is blamed for
// missing it.

use crate::{
    group_output::{
        DeleteOutcome, ResetRow, kafka_exceptions, parse_delete_groups, parse_export_csv,
        parse_reset_table,
    },
    oracle::{CliRun, Side, ToolFile},
};

/// The topic every reset case reads. One partition, so the record-to-partition
/// mapping is not the producer's to choose: Kafka's default partitioner is
/// sticky, and with two partitions the same six records land differently on
/// the two sides and every expected offset below becomes a coin toss.
const RESET_TOPIC: &str = "krabka-reset-offsets-itest";

/// How many records the topic holds, and therefore its log end offset.
const RESET_RECORDS: i64 = 6;

/// Where the seeded group's committed offset is parked.
///
/// Strictly between `0` and [`RESET_RECORDS`], which is what makes
/// `--to-current` a distinguishable answer: parked at either end it would be
/// indistinguishable from `--to-earliest` or `--to-latest`, and a broker that
/// confused the three would pass.
const RESET_CURRENT: i64 = 3;

/// Where the `--from-file` and `--execute` cases put their offsets document
/// inside whichever container the tool runs in.
const OFFSETS_CSV: &str = "/tmp/krabka-offsets.csv";

/// One `kafka-consumer-groups` invocation against `side`.
fn consumer_groups(side: &Side<'_>, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    side.run("kafka-consumer-groups", &full)
}

/// One `kafka-consumer-groups` invocation that reads a file the harness wrote.
fn consumer_groups_with_file(side: &Side<'_>, csv: &str, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    side.run_with_files(
        "kafka-consumer-groups",
        &full,
        &[ToolFile::new(OFFSETS_CSV, csv)],
        None,
    )
}

/// Create [`RESET_TOPIC`] on `side` and fill it with [`RESET_RECORDS`]
/// records.
fn seed_topic(side: &Side<'_>) {
    side.run(
        "kafka-topics",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--create",
            "--if-not-exists",
            "--topic",
            RESET_TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
    )
    .expect_success();

    let mut records = String::new();
    for index in 0..RESET_RECORDS {
        use std::fmt::Write as _;
        writeln!(records, "record-{index}").expect("a String never fails to grow");
    }
    side.run_with_files(
        "kafka-console-producer",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--topic",
            RESET_TOPIC,
        ],
        &[],
        Some(&records),
    )
    .expect_success();
}

/// Register `group` on `side` with its committed offset at
/// [`RESET_CURRENT`].
///
/// The consumer reads the whole topic and exits, which both commits an offset
/// and leaves the group `Empty` -- the state `--reset-offsets` requires. The
/// reset that follows moves the commit off the end of the log, so the three
/// interesting positions are three different numbers.
fn seed_group(side: &Side<'_>, group: &str) {
    side.run(
        "kafka-console-consumer",
        &[
            "--bootstrap-server",
            side.bootstrap(),
            "--topic",
            RESET_TOPIC,
            "--group",
            group,
            "--from-beginning",
            "--max-messages",
            &RESET_RECORDS.to_string(),
            "--timeout-ms",
            "30000",
        ],
    )
    .expect_success();

    consumer_groups(
        side,
        &[
            "--group",
            group,
            "--topic",
            RESET_TOPIC,
            "--reset-offsets",
            "--to-offset",
            &RESET_CURRENT.to_string(),
            "--execute",
        ],
    )
    .expect_success();
}

/// The committed offset `--describe` reports for `group` on partition 0.
fn committed_offset(side: &Side<'_>, group: &str) -> Option<i64> {
    let described = consumer_groups(side, &["--describe", "--group", group]).expect_success();
    described
        .stdout
        .lines()
        .filter(|line| line.contains(group) && line.contains(RESET_TOPIC))
        .find_map(|line| {
            // `GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG …`
            let fields: Vec<&str> = line.split_whitespace().collect();
            fields.get(3)?.parse().ok()
        })
}

/// Every `--reset-offsets` mode, the arguments that select it, and the offset
/// it must land on given the seeded shape.
///
/// The expected offset is stated even though the oracle is also consulted,
/// because the two answer different questions. The oracle says krabka agrees
/// with Kafka; the number says the pair agree on the *right* thing, and stops
/// a case from passing on two brokers that are wrong in the same way -- which
/// is exactly what would happen if the seeding above quietly produced nothing.
const RESET_MODES: &[(&str, &[&str], i64)] = &[
    ("--to-earliest", &["--to-earliest"], 0),
    ("--to-latest", &["--to-latest"], RESET_RECORDS),
    ("--to-current", &["--to-current"], RESET_CURRENT),
    ("--to-offset", &["--to-offset", "2"], 2),
    (
        "--shift-by backwards",
        &["--shift-by", "-1"],
        RESET_CURRENT - 1,
    ),
    (
        "--shift-by forwards",
        &["--shift-by", "2"],
        RESET_CURRENT + 2,
    ),
    // Past the end of the log. Kafka clamps to the log end offset rather than
    // committing an offset no fetch could ever use, and says so on stderr.
    (
        "--shift-by past the end",
        &["--shift-by", "100"],
        RESET_RECORDS,
    ),
    // No record predates the epoch, so the first offset at or after it is the
    // first record.
    (
        "--to-datetime before the log",
        &["--to-datetime", "1970-01-01T00:00:00.000"],
        0,
    ),
    // No record follows it either, so there is no offset for the timestamp and
    // the tool falls back to the log end offset.
    (
        "--to-datetime after the log",
        &["--to-datetime", "2999-01-01T00:00:00.000"],
        RESET_RECORDS,
    ),
    // `now`, which is after every record: the same fallback as above, reached
    // through the duration arithmetic instead of a literal.
    (
        "--by-duration zero",
        &["--by-duration", "P0DT0H0M0S"],
        RESET_RECORDS,
    ),
    // Ten years ago, which is before every record.
    (
        "--by-duration a decade",
        &["--by-duration", "P3650DT0H0M0S"],
        0,
    ),
];

/// The rows a mode must produce, given the group it was run for.
fn expected_rows(group: &str, new_offset: i64) -> Vec<ResetRow> {
    vec![ResetRow {
        group: group.to_owned(),
        topic: RESET_TOPIC.to_owned(),
        partition: 0,
        new_offset,
    }]
}

/// Every `--reset-offsets` mode, `--dry-run` against `--execute`, and the
/// `--export` / `--from-file` round trip, compared with Apache Kafka.
///
/// `--dry-run` is what lets the whole table share one group: it prints the
/// offset it would commit and commits nothing, so eleven modes cost eleven
/// invocations rather than eleven groups. The two cases that must change
/// something -- the `--execute` control and the file round trip -- take their
/// own groups afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn reset_offsets_modes_match_apache_kafka() {
    const DRY_GROUP: &str = "krabka-reset-dry-grp";
    const EXECUTE_GROUP: &str = "krabka-reset-execute-grp";
    const FILE_GROUP: &str = "krabka-reset-file-grp";

    // Kafka first. A wrong rule fails here, on somebody else's broker, before
    // krabka is asked the same question.
    let comparison = crate::oracle::OracleComparison::start("reset-offsets").await;
    let [oracle_side, krabka_side] = comparison.sides();

    for side in [&oracle_side, &krabka_side] {
        seed_topic(side);
        for group in [DRY_GROUP, EXECUTE_GROUP, FILE_GROUP] {
            seed_group(side, group);
        }
    }

    for (label, mode, expected) in RESET_MODES {
        let mut answers = Vec::new();
        for side in [&oracle_side, &krabka_side] {
            let mut args = vec![
                "--group",
                DRY_GROUP,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
            ];
            args.extend_from_slice(mode);
            args.push("--dry-run");
            let run = consumer_groups(side, &args).expect_success();
            let rows = parse_reset_table(&run.stdout);
            assert!(
                rows == expected_rows(DRY_GROUP, *expected),
                "{}: {label} must resolve to offset {expected}, got {rows:?}\n{}",
                side.label(),
                run.text(),
            );
            answers.push(rows);
        }
        assert!(
            answers[0] == answers[1],
            "{label}: krabka and Apache Kafka disagreed: {answers:?}",
        );
        // A dry run commits nothing, which is the premise the shared group
        // rests on. Without this the modes after the first would be reading a
        // group some earlier mode had moved.
        for side in [&oracle_side, &krabka_side] {
            let parked = committed_offset(side, DRY_GROUP);
            assert!(
                parked == Some(RESET_CURRENT),
                "{}: {label} --dry-run must not commit anything, got {parked:?}",
                side.label(),
            );
        }
    }

    // `--execute` is the same command with the other verb, and it must move
    // the commit the dry run described.
    for side in [&oracle_side, &krabka_side] {
        let run = consumer_groups(
            side,
            &[
                "--group",
                EXECUTE_GROUP,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
                "--to-earliest",
                "--execute",
            ],
        )
        .expect_success();
        let rows = parse_reset_table(&run.stdout);
        assert!(
            rows == expected_rows(EXECUTE_GROUP, 0),
            "{}: --execute must print the row it committed, got {rows:?}",
            side.label(),
        );
        let parked = committed_offset(side, EXECUTE_GROUP);
        assert!(
            parked == Some(0),
            "{}: --execute must commit what it printed, got {parked:?}",
            side.label(),
        );
    }

    // `--export` writes the CSV that `--from-file` reads back, so the pair is
    // one round trip: park the group somewhere else, then restore it from the
    // file and land back where the export was taken.
    let mut exports = Vec::new();
    for side in [&oracle_side, &krabka_side] {
        let exported = consumer_groups(
            side,
            &[
                "--group",
                FILE_GROUP,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
                "--to-current",
                "--export",
            ],
        )
        .expect_success();
        let rows = parse_export_csv(&exported.stdout);
        assert!(
            rows.len() == 1 && rows[0].topic == RESET_TOPIC && rows[0].offset == RESET_CURRENT,
            "{}: --export must describe the committed offset, got {rows:?}",
            side.label(),
        );
        exports.push((rows, exported.stdout));
    }
    assert!(
        exports[0].0 == exports[1].0,
        "--export: krabka and Apache Kafka disagreed: {:?}",
        exports.iter().map(|(rows, _)| rows).collect::<Vec<_>>(),
    );

    for (side, (_, csv)) in [&oracle_side, &krabka_side].into_iter().zip(&exports) {
        consumer_groups(
            side,
            &[
                "--group",
                FILE_GROUP,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
                "--to-earliest",
                "--execute",
            ],
        )
        .expect_success();
        let restored = consumer_groups_with_file(
            side,
            csv,
            &[
                "--group",
                FILE_GROUP,
                "--reset-offsets",
                "--from-file",
                OFFSETS_CSV,
                "--execute",
            ],
        )
        .expect_success();
        let rows = parse_reset_table(&restored.stdout);
        assert!(
            rows == expected_rows(FILE_GROUP, RESET_CURRENT),
            "{}: --from-file must restore the exported offset, got {rows:?}\n{}",
            side.label(),
            restored.text(),
        );
        let parked = committed_offset(side, FILE_GROUP);
        assert!(
            parked == Some(RESET_CURRENT),
            "{}: --from-file --execute must commit the file's offset, got {parked:?}",
            side.label(),
        );
    }

    comparison.broker.shutdown().await;
}

/// A live group refuses a reset and refuses a delete, an absent group refuses
/// a delete, and an empty one accepts it -- all four as Apache Kafka does.
///
/// The three outcomes are one case because they share the expensive half: a
/// consumer that is actually joined. A group is only ever not `Empty` while
/// somebody is in it, and the two refusals are about exactly that state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn group_delete_and_reset_refusals_match_apache_kafka() {
    const LIVE_GROUP: &str = "krabka-group-live-grp";
    const EMPTY_GROUP: &str = "krabka-group-empty-grp";
    const ABSENT_GROUP: &str = "krabka-group-absent-grp";

    let comparison = crate::oracle::OracleComparison::start("group-lifecycle").await;
    let [oracle_side, krabka_side] = comparison.sides();

    for side in [&oracle_side, &krabka_side] {
        seed_topic(side);
        seed_group(side, EMPTY_GROUP);
    }

    for side in [&oracle_side, &krabka_side] {
        // A consumer that stays. `--timeout-ms` is deliberately absent: the
        // consumer must still be in the group when the two refusals below run,
        // and the container is removed when the handle is dropped.
        let _consumer = side.run_detached(
            "kafka-console-consumer",
            &[
                "--bootstrap-server",
                side.bootstrap(),
                "--topic",
                RESET_TOPIC,
                "--group",
                LIVE_GROUP,
                "--from-beginning",
            ],
        );
        wait_until_group_is_live(side, LIVE_GROUP);

        let reset = consumer_groups(
            side,
            &[
                "--group",
                LIVE_GROUP,
                "--topic",
                RESET_TOPIC,
                "--reset-offsets",
                "--to-earliest",
                "--dry-run",
            ],
        );
        // The exit status is not the signal here: apache/kafka:4.3.1 prints
        // this refusal, then prints the empty NEW-OFFSET header and exits 0.
        // What the tool does promise is that it names the state it refused on
        // and proposes nothing, so that is what both sides are held to.
        assert!(
            reset.text().contains("can only be reset if the group"),
            "{}: the refusal must name the group's state, got:\n{}",
            side.label(),
            reset.text(),
        );
        assert!(
            parse_reset_table(&reset.text()) == Vec::<ResetRow>::new(),
            "{}: a refused reset must propose no offsets, got:\n{}",
            side.label(),
            reset.text(),
        );

        let delete_live = consumer_groups(side, &["--delete", "--group", LIVE_GROUP]);
        assert!(
            parse_delete_groups(&delete_live.text())
                == vec![DeleteOutcome {
                    group: LIVE_GROUP.to_owned(),
                    failure: Some(
                        "org.apache.kafka.common.errors.GroupNotEmptyException".to_owned()
                    ),
                }],
            "{}: deleting a live group must report NON_EMPTY_GROUP, got:\n{}",
            side.label(),
            delete_live.text(),
        );
    }

    // The remaining two states need no consumer, so they run after both
    // detached containers are gone.
    for side in [&oracle_side, &krabka_side] {
        let absent = consumer_groups(side, &["--delete", "--group", ABSENT_GROUP]);
        assert!(
            parse_delete_groups(&absent.text())
                == vec![DeleteOutcome {
                    group: ABSENT_GROUP.to_owned(),
                    failure: Some(
                        "org.apache.kafka.common.errors.GroupIdNotFoundException".to_owned()
                    ),
                }],
            "{}: deleting a group that never existed must report GROUP_ID_NOT_FOUND, got:\n{}",
            side.label(),
            absent.text(),
        );

        // The positive control. A delete that refused everything would pass
        // both cases above.
        let empty = consumer_groups(side, &["--delete", "--group", EMPTY_GROUP]);
        assert!(
            empty.succeeded() && kafka_exceptions(&empty.text()).is_empty(),
            "{}: an empty group must be deletable, got:\n{}",
            side.label(),
            empty.text(),
        );
        let listed = consumer_groups(side, &["--list"]).expect_success();
        assert!(
            !listed.stdout.contains(EMPTY_GROUP),
            "{}: the deleted group must be gone from --list, got:\n{}",
            side.label(),
            listed.stdout,
        );
    }

    comparison.broker.shutdown().await;
}

/// Poll `--describe --state` until `group` reports `Stable`.
///
/// A `kafka-console-consumer` container takes a few seconds to start a JVM and
/// finish a join, and every assertion after it is about a group that is not
/// `Empty`. Without this wait the refusals would race the consumer and the
/// case would sometimes assert that Kafka refuses a reset it in fact allows.
///
/// `Stable` and not merely "not `Empty`": a group nobody has ever joined is
/// absent rather than empty, and the tool answers for it with a sentence that
/// names the group and no state at all -- which a negative test would read as
/// success.
fn wait_until_group_is_live(side: &Side<'_>, group: &str) {
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(120);
    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        let state = consumer_groups(side, &["--describe", "--state", "--group", group]);
        if state.succeeded() && state.stdout.contains(group) && state.stdout.contains("Stable") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{}: {group} never reached Stable within {BUDGET:?}:\n{}",
            side.label(),
            state.text(),
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}
