//! Kafka's own `__consumer_offsets` formatters, run over the topic krabka
//! writes.
//!
//! # Why this suite exists
//!
//! Burrow, `kafka-lag-exporter` and several vendor Kafka integrations do not
//! ask the broker for a group's position. They consume `__consumer_offsets`
//! and decode the records with Kafka's internal schemas. A one-field or
//! one-version divergence in those bytes makes such a stack report zero
//! consumers on a healthy cluster, and it reports it without an error
//! anywhere. `crate::coordinator::unified::persistence` says its layouts
//! mirror Kafka's `OffsetCommitValue` and `GroupMetadataValue`; this suite is
//! what holds that claim to a real decoder.
//!
//! The decoder is Kafka's, out of the pinned `apache/kafka:4.3.1` image:
//! `kafka-console-consumer.sh --formatter` with the two formatters that ship
//! for this topic. It is the same code Kafka's own operators read the topic
//! with, so an agreement here is evidence about every reader built on those
//! schemas.
//!
//! # How the formatter class names were determined
//!
//! They were read out of the Apache Kafka source tree at tag `4.3.1`, at
//! `tools/src/main/java/org/apache/kafka/tools/consumer/`, which holds
//! `OffsetsMessageFormatter.java` and `GroupMetadataMessageFormatter.java`.
//! Both are in package `org.apache.kafka.tools.consumer`. Older releases held
//! them in the core module as nested classes of `GroupMetadataManager`, spelled
//! `kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter`;
//! that name is gone in 4.x. Each name lives in one constant below
//! ([`OFFSETS_FORMATTER`] and [`GROUP_METADATA_FORMATTER`]), so a wrong name is
//! a one-line fix.
//!
//! # The rule the later key versions have to satisfy
//!
//! Both formatters extend `CoordinatorRecordMessageFormatter`, which
//! deserializes the key AND the value of every record before it filters on the
//! record type. A key type the `GroupCoordinatorRecordSerde` does not know
//! raises `UnknownRecordTypeException`, which the formatter swallows: the
//! record is skipped. Every other failure, an unknown value version included,
//! is re-thrown, and `ConsoleConsumer` then exits non-zero. So a krabka record
//! under a key version Kafka knows must decode exactly, or the tool dies in the
//! middle of the topic and the operator loses the rows after it.
//!
//! krabka writes six families into this topic (see
//! `coordinator::unified::persistence`): the `OffsetCommit` keys 0 and 1, the
//! classic `GroupMetadata` key 2, the KIP-848 next-gen keys 3 and 5 to 8, the
//! KIP-932 share-group keys, and the KIP-1071 streams-group keys. This suite
//! drives the classic families and the KIP-848 family, so the formatter meets
//! the later key versions on the same partitions it decodes the offsets from.
//!
//! # What it asserts
//!
//! - Each decoded offsets row for a group krabka served matches, field by
//!   field, what krabka itself reports as that group's committed offset.
//! - The decoded classic group-metadata rows carry the member krabka
//!   registered, with the client id, the timeouts, the protocol and the leader
//!   the group really had, and the rebalance that empties the group raises the
//!   generation.
//! - Both runs exit zero, and the offsets run skips more records than the
//!   group-metadata run printed, so the KIP-848 records really were on the
//!   partitions it read and really were skipped rather than fatal.
//!
//! Gated `#[ignore]` (requires Docker); the Bazel target that owns this suite
//! runs it with `--ignored`.

mod support;

use std::process::Command;

use assert2::assert;
use krabka_broker::BrokerHandle;

/// The pinned release whose tools decode the topic.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The client that drives the KIP-848 half.
///
/// `jvm_consumer_group_next_gen` already holds krabka's next-gen path to this
/// release, so a failure here is about the records rather than about the
/// handshake that wrote them. The formatters stay on [`KAFKA_IMAGE`].
const KAFKA_IMAGE_NEXT_GEN: &str = "mirror.gcr.io/apache/kafka:4.0.0";

/// Kafka's decoder for the `OffsetCommit` families, key versions 0 and 1.
const OFFSETS_FORMATTER: &str = "org.apache.kafka.tools.consumer.OffsetsMessageFormatter";

/// Kafka's decoder for the classic `GroupMetadata` family, key version 2.
const GROUP_METADATA_FORMATTER: &str =
    "org.apache.kafka.tools.consumer.GroupMetadataMessageFormatter";

/// The tools, at their paths inside the Apache image. They are not on `PATH`.
const CONSOLE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-consumer.sh";
const CONSOLE_PRODUCER: &str = "/opt/kafka/bin/kafka-console-producer.sh";
const TOPICS_TOOL: &str = "/opt/kafka/bin/kafka-topics.sh";
const GROUPS_TOOL: &str = "/opt/kafka/bin/kafka-consumer-groups.sh";

/// The internal topic the formatters read.
const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// The topic the two groups consume.
const TOPIC: &str = "krabka-offsets-formatter";

/// The group that speaks the classic protocol, so krabka writes it a key
/// version 2 `GroupMetadata` record for each completed rebalance.
const CLASSIC_GROUP: &str = "krabka-offsets-formatter-classic";

/// The group that speaks the KIP-848 protocol, so krabka writes it the later
/// key versions the formatters have to skip.
const NEXT_GEN_GROUP: &str = "krabka-offsets-formatter-next-gen";

/// `client.id` of both consumers. Kafka builds a dynamic member id as
/// `<client.id>-<uuid>`, so this is also the prefix of the member id krabka
/// stores.
const CLIENT_ID: &str = "krabka-offsets-formatter-client";

/// How many records the topic holds. The consumer reads all of them and then
/// commits its position, so this is also the offset every group commits.
const RECORDS: i64 = 5;

/// `session.timeout.ms` of the classic consumer, stored on the member.
/// Above the broker's `group.min.session.timeout.ms` floor of six seconds.
const SESSION_TIMEOUT_MS: i32 = 10_000;

/// `max.poll.interval.ms` of both consumers. The classic protocol carries it to
/// the coordinator as the member's rebalance timeout.
const REBALANCE_TIMEOUT_MS: i32 = 60_000;

/// The assignor the classic consumer asks for, and the protocol name krabka
/// must therefore record on the group.
const ASSIGNOR: &str = "org.apache.kafka.clients.consumer.RangeAssignor";
const PROTOCOL_NAME: &str = "range";

/// How long a console consumer waits for a record before it stops. The runs
/// over `__consumer_offsets` have no message count to stop at, so this is what
/// ends them.
const DRAIN_TIMEOUT_MS: &str = "15000";

/// How long a run that does have a message count may take to reach it.
const CONSUME_TIMEOUT_MS: &str = "60000";

/// The lower bound on how many records a KIP-848 group leaves behind: the group
/// epoch (key 3), the member metadata (key 5) and the current assignment
/// (key 8). krabka writes more, and the assertion stays true if it writes
/// fewer of some other kind.
const NEXT_GEN_RECORDS: usize = 3;

// --------------------------------------------------------------- decoded rows

/// One row of `OffsetsMessageFormatter` output, in the fields that say what a
/// lag monitor would read.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OffsetRow {
    /// The key's record type. `1` is `OffsetCommitKey`; `0` is the legacy key.
    key_type: i64,
    group: String,
    topic: String,
    partition: i64,
    /// The value schema version krabka wrote.
    value_version: i64,
    offset: i64,
    metadata: String,
}

/// One row of `GroupMetadataMessageFormatter` output.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupMetadataRow {
    key_type: i64,
    group: String,
    value_version: i64,
    protocol_type: String,
    protocol: Option<String>,
    generation: i64,
    leader: Option<String>,
    members: Vec<MemberRow>,
}

/// One member of a decoded group-metadata row.
///
/// The member id and the client host are not in it: Kafka mints the id from a
/// fresh UUID and the host is the container's address on the Docker bridge, so
/// neither is a value the test can state. [`assert_member_identity`] checks
/// both against the rules that do hold.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberRow {
    group_instance_id: Option<String>,
    client_id: String,
    session_timeout: i32,
    rebalance_timeout: i32,
    /// Kafka renders the two `bytes` fields as base64, and both carry the
    /// consumer protocol's own encoding. What matters here is that krabka
    /// stored them at all: an empty subscription is a group no assignor can
    /// reassign after a coordinator failover.
    subscription_is_empty: bool,
    assignment_is_empty: bool,
}

/// The member id and client host of a decoded member, kept beside
/// [`MemberRow`] because they are asserted by rule rather than by value.
#[derive(Debug, Clone)]
struct MemberIdentity {
    member_id: String,
    client_host: String,
}

// ------------------------------------------------------------------- decoding

/// The JSON objects one formatter run printed.
///
/// `CoordinatorRecordMessageFormatter` writes one object per record and no
/// separator between them, so the stream is concatenated JSON rather than one
/// document or one object per line.
fn decode_stream(stdout: &str) -> Vec<serde_json::Value> {
    // The tool writes its own notices to stdout ahead of the records, and they
    // are not the formatter's output: 4.3.1 deprecates `--consumer-property`
    // and says so on stdout before the first record. The flag stays, because
    // the KIP-848 run drives `apache/kafka:4.0.0`, which has no replacement
    // for it; the stream simply starts at the first object.
    let stdout = stdout.find('{').map_or("", |start| &stdout[start..]);
    serde_json::Deserializer::from_str(stdout)
        .into_iter::<serde_json::Value>()
        .map(|value| value.expect("the formatter writes one JSON object per record"))
        .collect()
}

/// A `"key"`/`"value"` object as [`OffsetRow`].
fn offset_row(json: &serde_json::Value) -> OffsetRow {
    let key = &json["key"];
    let value = &json["value"];
    OffsetRow {
        key_type: int_field(key, "type"),
        group: string_field(&key["data"], "group"),
        topic: string_field(&key["data"], "topic"),
        partition: int_field(&key["data"], "partition"),
        value_version: int_field(value, "version"),
        offset: int_field(&value["data"], "offset"),
        metadata: string_field(&value["data"], "metadata"),
    }
}

/// A `"key"`/`"value"` object as [`GroupMetadataRow`] and the identities of its
/// members, in the members' own order.
fn group_metadata_row(json: &serde_json::Value) -> (GroupMetadataRow, Vec<MemberIdentity>) {
    let key = &json["key"];
    let value = &json["value"];
    let data = &value["data"];
    let raw_members = data["members"]
        .as_array()
        .expect("a group-metadata value carries a members array");
    let members = raw_members
        .iter()
        .map(|member| MemberRow {
            group_instance_id: optional_string_field(member, "groupInstanceId"),
            client_id: string_field(member, "clientId"),
            session_timeout: small_int_field(member, "sessionTimeout"),
            rebalance_timeout: small_int_field(member, "rebalanceTimeout"),
            subscription_is_empty: string_field(member, "subscription").is_empty(),
            assignment_is_empty: string_field(member, "assignment").is_empty(),
        })
        .collect();
    let identities = raw_members
        .iter()
        .map(|member| MemberIdentity {
            member_id: string_field(member, "memberId"),
            client_host: string_field(member, "clientHost"),
        })
        .collect();
    let row = GroupMetadataRow {
        key_type: int_field(key, "type"),
        group: string_field(&key["data"], "group"),
        value_version: int_field(value, "version"),
        protocol_type: string_field(data, "protocolType"),
        protocol: optional_string_field(data, "protocol"),
        generation: int_field(data, "generation"),
        leader: optional_string_field(data, "leader"),
        members,
    };
    (row, identities)
}

fn int_field(json: &serde_json::Value, name: &str) -> i64 {
    json[name]
        .as_i64()
        .unwrap_or_else(|| panic!("field {name} is an integer in {json}"))
}

fn small_int_field(json: &serde_json::Value, name: &str) -> i32 {
    i32::try_from(int_field(json, name)).expect("a timeout field fits in i32")
}

fn string_field(json: &serde_json::Value, name: &str) -> String {
    json[name]
        .as_str()
        .unwrap_or_else(|| panic!("field {name} is a string in {json}"))
        .to_owned()
}

/// A field Kafka's converter omits when it is null, and prints as a string
/// otherwise.
fn optional_string_field(json: &serde_json::Value, name: &str) -> Option<String> {
    match &json[name] {
        serde_json::Value::Null => None,
        value => Some(
            value
                .as_str()
                .unwrap_or_else(|| panic!("field {name} is a string or null in {json}"))
                .to_owned(),
        ),
    }
}

/// How many records a console-consumer run read, out of the count it reports on
/// stderr when it stops.
fn processed_records(stderr: &str) -> usize {
    const PREFIX: &str = "Processed a total of ";
    let line = stderr
        .lines()
        .find_map(|line| line.trim().strip_prefix(PREFIX))
        .unwrap_or_else(|| panic!("no record count in the consumer's stderr:\n{stderr}"));
    line.split_whitespace()
        .next()
        .expect("the count precedes the word 'messages'")
        .parse()
        .expect("the record count is numeric")
}

// --------------------------------------------------------------------- docker

/// Run one tool from `image` against a broker outside the container, and hand
/// back what it did without asserting that it succeeded.
fn docker_run(image: &str, args: &[&str]) -> std::process::Output {
    let mut full: Vec<&str> = vec![
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        image,
    ];
    full.extend_from_slice(args);
    let out = Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker run");
    eprintln!(
        "KRABKA[test] docker run {image} {args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// [`docker_run`], and the tool must have succeeded.
fn docker_run_ok(image: &str, args: &[&str]) -> std::process::Output {
    let out = docker_run(image, args);
    assert!(out.status.success(), "docker run {image} {args:?} failed");
    out
}

// ------------------------------------------------------------- the krabka side

/// One krabka broker the containers reach at `host.docker.internal`.
struct Cluster {
    handle: BrokerHandle,
    bootstrap: String,
    _dir: tempfile::TempDir,
}

impl Cluster {
    /// Boot the broker and create the topic with the real tool.
    async fn start() -> Self {
        support::init_tracing();
        let dir = tempfile::tempdir().expect("log dir");
        // Adopt bound listeners so concurrent test binaries cannot take the ports.
        let (handle, bootstrap) = support::start_jvm_bound(dir.path().to_path_buf(), |_| {}).await;

        let cluster = Self {
            handle,
            bootstrap,
            _dir: dir,
        };
        cluster.create_topic().await;
        cluster
    }

    async fn create_topic(&self) {
        let created = docker_run_ok(
            KAFKA_IMAGE,
            &[
                TOPICS_TOOL,
                "--bootstrap-server",
                &self.bootstrap,
                "--create",
                "--topic",
                TOPIC,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ],
        );
        assert!(
            String::from_utf8_lossy(&created.stdout).contains("Created topic"),
            "kafka-topics --create",
        );
        self.handle.wait_until_partition_present(TOPIC, 0).await;
    }

    /// Fill the topic with [`RECORDS`] records over the console producer's
    /// stdin.
    fn produce(&self) {
        let mut child_command =
            crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
                image: KAFKA_IMAGE,
                args: &[
                    CONSOLE_PRODUCER,
                    "--bootstrap-server",
                    &self.bootstrap,
                    "--topic",
                    TOPIC,
                ],
                input: crate::support::ContainerInput::Attached,
                ..Default::default()
            });
        // One record per line, and a final newline so the producer sends the
        // last one rather than holding a partial line.
        let lines: Vec<String> = (0..RECORDS).map(|i| format!("record-{i}")).collect();
        let body = format!("{}\n", lines.join("\n"));
        let out = crate::support::jvm_stdin_output(&mut child_command, body.as_bytes());
        assert!(
            out.status.success(),
            "console producer failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
    }

    /// The `CURRENT-OFFSET` krabka reports for `group` on partition 0 of
    /// [`TOPIC`], which is krabka's own answer for what it committed.
    ///
    /// `kafka-consumer-groups --describe` prints one row for each assigned
    /// partition, with `GROUP TOPIC PARTITION CURRENT-OFFSET` in the first four
    /// columns and a header line above them.
    fn committed_offset(&self, group: &str) -> i64 {
        let out = docker_run_ok(
            KAFKA_IMAGE,
            &[
                GROUPS_TOOL,
                "--bootstrap-server",
                &self.bootstrap,
                "--describe",
                "--group",
                group,
            ],
        );
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let columns = stdout
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .find(|columns| {
                columns.len() > 3 && columns[0] == group && columns[1] == TOPIC && columns[2] == "0"
            })
            .unwrap_or_else(|| panic!("no offset row for {group} {TOPIC}-0 in:\n{stdout}"));
        columns[3]
            .parse()
            .expect("CURRENT-OFFSET is numeric once a group has committed")
    }

    /// Read [`RECORDS`] records as `group` over the classic protocol, then
    /// close. The join and the sync are one rebalance, the close commits the
    /// position, and the `LeaveGroup` that follows empties the group and is a
    /// second rebalance.
    fn consume_as_classic_group(&self) {
        let out = docker_run_ok(
            KAFKA_IMAGE,
            &[
                CONSOLE_CONSUMER,
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                TOPIC,
                "--group",
                CLASSIC_GROUP,
                "--from-beginning",
                "--max-messages",
                &RECORDS.to_string(),
                "--timeout-ms",
                CONSUME_TIMEOUT_MS,
                "--consumer-property",
                "group.protocol=classic",
                "--consumer-property",
                &format!("client.id={CLIENT_ID}"),
                "--consumer-property",
                "enable.auto.commit=true",
                "--consumer-property",
                &format!("session.timeout.ms={SESSION_TIMEOUT_MS}"),
                "--consumer-property",
                &format!("max.poll.interval.ms={REBALANCE_TIMEOUT_MS}"),
                "--consumer-property",
                &format!("partition.assignment.strategy={ASSIGNOR}"),
            ],
        );
        assert_records_read(&out);
    }

    /// The same read as a KIP-848 group, so krabka writes the later key
    /// versions onto the same partition it wrote this group's offsets to.
    ///
    /// The next-gen protocol rejects `session.timeout.ms`,
    /// `heartbeat.interval.ms` and `partition.assignment.strategy` in the
    /// client itself: `ConsumerConfig` fails the run rather than ignoring them.
    fn consume_as_next_gen_group(&self) {
        let out = docker_run_ok(
            KAFKA_IMAGE_NEXT_GEN,
            &[
                CONSOLE_CONSUMER,
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                TOPIC,
                "--group",
                NEXT_GEN_GROUP,
                "--from-beginning",
                "--max-messages",
                &RECORDS.to_string(),
                "--timeout-ms",
                CONSUME_TIMEOUT_MS,
                "--consumer-property",
                "group.protocol=consumer",
                "--consumer-property",
                &format!("client.id={CLIENT_ID}"),
                "--consumer-property",
                "enable.auto.commit=true",
                "--consumer-property",
                &format!("max.poll.interval.ms={REBALANCE_TIMEOUT_MS}"),
            ],
        );
        assert_records_read(&out);
    }

    /// Read `__consumer_offsets` from the beginning through `formatter`, and
    /// hand back the decoded rows and how many records the run read.
    ///
    /// The run takes no `--group`, so the console consumer invents one and
    /// commits nothing for it. Its own group-metadata records land in this
    /// topic like any other group's; every assertion below is filtered by group
    /// id, so they are inert.
    fn read_offsets_topic(&self, formatter: &str) -> (Vec<serde_json::Value>, usize) {
        let out = docker_run_ok(
            KAFKA_IMAGE,
            &[
                CONSOLE_CONSUMER,
                "--bootstrap-server",
                &self.bootstrap,
                "--topic",
                OFFSETS_TOPIC,
                "--from-beginning",
                "--timeout-ms",
                DRAIN_TIMEOUT_MS,
                "--formatter",
                formatter,
                "--consumer-property",
                "exclude.internal.topics=false",
            ],
        );
        let decoded = decode_stream(&String::from_utf8_lossy(&out.stdout));
        let processed = processed_records(&String::from_utf8_lossy(&out.stderr));
        (decoded, processed)
    }

    async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

/// Every produced record reached the consumer, so the group really did read to
/// the end of the topic and the position it commits is [`RECORDS`].
fn assert_records_read(out: &std::process::Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    for index in 0..RECORDS {
        let record = format!("record-{index}");
        assert!(
            stdout.contains(&record),
            "the consumer did not read {record}: {stdout}",
        );
    }
}

// ------------------------------------------------------------------- the rules

/// What the decoded offsets row for `group` has to be.
fn expected_offset_row(group: &str, committed: i64) -> OffsetRow {
    OffsetRow {
        // `OffsetCommitKey`, the key version krabka writes for every commit.
        key_type: 1,
        group: group.to_owned(),
        topic: TOPIC.to_owned(),
        partition: 0,
        // The version that carries the topic id. Kafka 4.3's
        // `GroupCoordinatorRecordHelpers.offsetCommitValueVersion` writes it
        // for every commit except a KIP-211 per-commit expiry, which gets
        // version 1 and which no modern consumer asks for.
        value_version: 4,
        offset: committed,
        // The JVM consumer commits no metadata.
        metadata: String::new(),
    }
}

/// The member krabka registered for the classic consumer.
fn expected_member() -> MemberRow {
    MemberRow {
        // A dynamic member. The consumer sets no `group.instance.id`.
        group_instance_id: None,
        client_id: CLIENT_ID.to_owned(),
        session_timeout: SESSION_TIMEOUT_MS,
        rebalance_timeout: REBALANCE_TIMEOUT_MS,
        subscription_is_empty: false,
        assignment_is_empty: false,
    }
}

/// The member id and host, which are minted rather than configured.
///
/// Kafka builds a dynamic member id as `<client.id>-<uuid>`, and the broker
/// records the address the member connected from.
fn assert_member_identity(identity: &MemberIdentity) {
    assert!(
        identity.member_id.starts_with(&format!("{CLIENT_ID}-")),
        "krabka stored a member id that is not the client's: {identity:?}",
    );
    assert!(
        !identity.client_host.is_empty(),
        "krabka stored no client host: {identity:?}",
    );
}

/// The offsets rows for one group, oldest first. A group commits more than
/// once, so the last row is the one a lag monitor would report.
fn rows_for(rows: &[OffsetRow], group: &str) -> Vec<OffsetRow> {
    rows.iter()
        .filter(|row| row.group == group)
        .cloned()
        .collect()
}

// --------------------------------------------------------------------- the suite

/// One broker, two groups, and Kafka's two formatters over what they left in
/// `__consumer_offsets`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn kafka_formatters_decode_krabka_consumer_offsets() {
    let cluster = Cluster::start().await;
    cluster.produce();

    // The classic group first: it is the only one that writes a key version 2
    // record, and the KIP-848 group that follows puts the later key versions in
    // front of the formatters.
    cluster.consume_as_classic_group();
    cluster.consume_as_next_gen_group();

    let classic_committed = cluster.committed_offset(CLASSIC_GROUP);
    let next_gen_committed = cluster.committed_offset(NEXT_GEN_GROUP);
    assert!(
        classic_committed == RECORDS && next_gen_committed == RECORDS,
        "the premise is that both groups read the whole topic and committed \
         {RECORDS}: classic={classic_committed} next-gen={next_gen_committed}",
    );

    // The group-metadata run first, so its row count is a lower bound on the
    // key version 2 records the offsets run then skips.
    let (raw_metadata, _) = cluster.read_offsets_topic(GROUP_METADATA_FORMATTER);
    let decoded_metadata: Vec<(GroupMetadataRow, Vec<MemberIdentity>)> =
        raw_metadata.iter().map(group_metadata_row).collect();
    let (offsets_json, offsets_processed) = cluster.read_offsets_topic(OFFSETS_FORMATTER);
    let offsets: Vec<OffsetRow> = offsets_json.iter().map(offset_row).collect();

    // 1. Both groups' committed offsets decode to what krabka says it holds.
    for (group, committed) in [
        (CLASSIC_GROUP, classic_committed),
        (NEXT_GEN_GROUP, next_gen_committed),
    ] {
        let group_rows = rows_for(&offsets, group);
        let last = group_rows
            .last()
            .unwrap_or_else(|| panic!("no offsets row for {group} in {offsets:?}"));
        assert!(
            *last == expected_offset_row(group, committed),
            "Kafka's formatter read a different commit for {group} than krabka holds",
        );
    }

    // 2. The classic group's rebalances decode to the group krabka served.
    let classic_metadata: Vec<&(GroupMetadataRow, Vec<MemberIdentity>)> = decoded_metadata
        .iter()
        .filter(|(row, _)| row.group == CLASSIC_GROUP)
        .collect();
    let (with_members, identities) = classic_metadata
        .iter()
        .find(|(row, _)| !row.members.is_empty())
        .unwrap_or_else(|| panic!("no group-metadata row carries a member: {classic_metadata:?}"));
    assert!(
        *with_members
            == GroupMetadataRow {
                // `GroupMetadataKey`, the only key version 2 family.
                key_type: 2,
                group: CLASSIC_GROUP.to_owned(),
                // The newest non-flexible schema, the one with
                // `groupInstanceId` and `currentStateTimestamp`.
                value_version: 3,
                protocol_type: "consumer".to_owned(),
                protocol: Some(PROTOCOL_NAME.to_owned()),
                generation: with_members.generation,
                leader: Some(identities[0].member_id.clone()),
                members: vec![expected_member()],
            },
        "Kafka's formatter read a different group than krabka served",
    );
    assert_member_identity(&identities[0]);

    // The `LeaveGroup` on close is a rebalance of its own: krabka empties the
    // group under a higher generation.
    let (last_classic, _) = classic_metadata
        .last()
        .expect("the classic group has at least one row");
    assert!(
        with_members.generation >= 1
            && last_classic.members.is_empty()
            && last_classic.generation > with_members.generation,
        "the rebalance that empties the group did not raise the generation: {classic_metadata:?}",
    );

    // 3. The KIP-848 group writes no key version 2 record at all: its state
    //    lives under the later key versions.
    assert!(
        !decoded_metadata
            .iter()
            .any(|(row, _)| row.group == NEXT_GEN_GROUP),
        "a KIP-848 group must not appear as classic group metadata: {decoded_metadata:?}",
    );

    // 4. The offsets run read the whole topic, and what it did not print is at
    //    least the key version 2 records the previous run printed plus the
    //    KIP-848 records. Both runs exited zero, so the formatter skipped every
    //    one of them instead of dying on it.
    let skipped = offsets_processed - offsets.len();
    assert!(
        skipped >= decoded_metadata.len() + NEXT_GEN_RECORDS,
        "the offsets run read {offsets_processed} records and printed {}, so it skipped \
         {skipped}; the topic holds at least {} group-metadata and {NEXT_GEN_RECORDS} KIP-848 \
         records",
        offsets.len(),
        decoded_metadata.len(),
    );

    cluster.shutdown().await;
}
