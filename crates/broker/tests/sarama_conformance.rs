//! `kaf` -- a Go CLI on IBM/sarama -- against krabka.
//!
//! Every other non-JVM client this tree drives is librdkafka, so one decoder
//! stands behind every non-JVM row of the evidence set. sarama is an
//! independent reimplementation of the protocol: its own request-version
//! floors, its own header and tagged-field handling, its own tolerance for
//! absent optional fields. A response only librdkafka and the JVM client
//! accept would pass every other suite here and fail this one.
//!
//! ## What the client is pinned to
//!
//! The image is built in this repository from an apko lock
//! (`//bazel/images:kaf_sarama_tar`), and that lock is what pins the binary.
//! `kaf --version` is no help: the release build stamps the version through
//! goreleaser, and the Wolfi package does not, so the binary reports
//! `latest (HEAD)` whatever it is. There is therefore no version banner to
//! assert on, and nothing below asserts on one -- what the suite pins is
//! behaviour.
//!
//! ## Which RPC each command issues
//!
//! sarama at the version this client builds against implements neither
//! `DescribeCluster` (api key 60) nor `DescribeTopicPartitions` (api key 75):
//! `request.go` lists both as unimplemented, and `ClusterAdmin::DescribeCluster`
//! and `ClusterAdmin::DescribeTopics` are both `Metadata` round-trips. So
//! `kaf nodes` and `kaf topic describe` do *not* exercise those two APIs, and
//! this suite does not claim they do. What it establishes instead is that the
//! answers agree: krabka's own `DescribeCluster` and `DescribeTopicPartitions`
//! responses are read on the host and used to *build* the rows `kaf` must
//! print from `Metadata`. A broker whose newer cluster and topic APIs drifted
//! from what it says in `Metadata` fails here.
//!
//! ## The versions this suite actually pins, and why they are fixed
//!
//! `kaf`'s `getConfig` sets `saramaConfig.Version = sarama.V1_1_0_0` and only a
//! config file's `cluster.Version` raises it (`cmd/kaf/kaf.go:24-35`). Nothing
//! here writes a config file, so 1.1 is what runs -- and at 1.1 **sarama sends
//! no `ApiVersions` request at all**:
//!
//! ```text
//! // broker.go:177
//! usingApiVersionsRequests := conf.Version.IsAtLeast(V2_4_0_0) && conf.ApiVersionsRequest
//! ```
//!
//! `ApiVersionsRequest` defaults to true (`config.go:560`), but the `V2_4_0_0`
//! conjunct is false, so the KIP-511 exchange never happens. Every request
//! version below is chosen from `conf.Version` alone and sent at krabka
//! sight-unseen, which makes this the one suite here where a client's floors
//! are not negotiated:
//!
//! | RPC | version | where sarama picks it |
//! | --- | --- | --- |
//! | `FindCoordinator` | 1 | `client.go:1213-1216` (2 needs `V2_0_0_0`) |
//! | `JoinGroup` | 2 | `consumer_group.go:451-461` (3 needs `V2_0_0_0`) |
//! | `SyncGroup` | 1 | `consumer_group.go:521-524` (2 needs `V2_0_0_0`) |
//! | `OffsetCommit` | 3 | `offset_manager.go:286-304` (4 needs `V2_1_0_0`) |
//!
//! A previous revision of this doc claimed the versions were "negotiated by an
//! `ApiVersions` exchange". That was wrong, and the line above is what replaces
//! it.
//!
//! ## What has been ruled out for the group path, and how
//!
//! CI has failed this suite on the group phase. These were checked by reading
//! `birdayz/kaf` at `v0.2.14` and `IBM/sarama` at `v1.43.2`, the revisions the
//! image's `go.mod` pins:
//!
//! * **`kaf` does reach `sarama.NewConsumerGroupFromClient`.** `consumeCmd.Run`
//!   has exactly one branch on the way there -- `if groupFlag != ""` at
//!   `consume.go:142` -- and `--group` is passed. Nothing between flag parsing
//!   and that branch can `errorExit`: `PreRun` is a no-op while `--proto-type`
//!   is unset, `--offset oldest` takes the `case "oldest"` arm rather than the
//!   `strconv.ParseInt` arm that can exit (`consume.go:119-132`), and the
//!   header-filter loop is skipped with no `--header`.
//! * **No config file and no `KAFKA_*` env is needed.** `onInit` synthesises
//!   `currentCluster` when `cfg.ActiveCluster()` is nil and then overwrites its
//!   brokers from the flag (`kaf.go:194-214`), so `currentCluster` is never nil
//!   with `--brokers` and no config. The same code already served `kaf topics`
//!   and `kaf produce` earlier in this test.
//! * **`--offset oldest` is not lost to ordering.** It is set on `cfg` *after*
//!   `getClientFromConfig(cfg)` has already built the client, but `getConfig`
//!   returns a `*sarama.Config` and `sarama.NewClient` keeps that pointer
//!   (`kaf.go:238-244`), so `cfg.Consumer.Offsets.Initial = OffsetOldest` at
//!   `consume.go:122` still reaches the session.
//! * **A group path that cannot proceed exits; it does not hang.** Every wait
//!   sarama takes there is bounded: `findCoordinator` retries
//!   `Metadata.Retry.Max` = 3 at 250 ms (`config.go:520-521`) with a 2 s sleep
//!   per attempt when the answer is `COORDINATOR_NOT_AVAILABLE`
//!   (`client.go:1240-1247`); `newSession` retries
//!   `Consumer.Group.Rebalance.Retry.Max` = 4 at 2 s (`config.go:554-555`); and
//!   a `JoinGroup` or `SyncGroup` that is never answered hits
//!   `Net.ReadTimeout` = 30 s (`broker.go:937`, `config.go:515`) and is
//!   returned rather than retried (`consumer_group.go:294-300`). The ceiling is
//!   therefore about `5 * (2 + 9) + 30` seconds, and past it `Consume` returns
//!   an error that `withConsumerGroup` turns into `errorExit`
//!   (`consume.go:181-184`), which [`GroupConsumer::exited`] observes.
//!
//! What is *not* ruled out is which side of `JoinGroup`/`SyncGroup` fails, and
//! that is the point of the two waits below rather than one: the phase banners
//! and sarama's own `--verbose` log are streamed as they happen, so the answer
//! is in the log from the first second rather than only in an assertion message
//! at the very end. The last CI failure was diagnosed by arithmetic on Bazel's
//! wall clock because a sibling suite in the same shard had pushed this one's
//! output past GitHub's log-API truncation point; streaming is what stops that
//! from happening again, since truncation drops the tail.

mod jvm_acceptance;
mod support;

use {
    crate::{
        jvm_acceptance::{broker0_advertised, start_host_broker},
        support::{admin::topic_spec, client::connect_owned},
    },
    assert2::assert,
    krabka_client_admin::AdminClient,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse,
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::DescribeGroupsResponse,
        describe_topic_partitions_request::{DescribeTopicPartitionsRequest, TopicRequest},
        describe_topic_partitions_response::{
            DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
        },
    },
    std::{
        collections::BTreeMap,
        fmt::{self, Debug, Display},
        io::{Read, Write},
        process::{Child, Command, ExitStatus, Stdio},
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
        time::Duration,
    },
    tokio::time::Instant,
};

/// The image, built here and named in the `sarama_conformance` entry of the
/// `docker` map in `BUILD.bazel`.
const IMAGE: &str = "docker.io/krabka-io/kaf:0.2.14";
/// The binary inside it.
const PROGRAM: &str = "/usr/bin/kaf";

const TOPIC: &str = "sarama-conformance";
const GROUP: &str = "sarama-conformance-group";
/// More than one, so the partition count `kaf` prints is a number it read
/// rather than the only number a single-partition topic could have.
const PARTITIONS: i32 = 3;
/// The one record, produced to partition 0 with sarama's manual partitioner.
const PAYLOAD: &str = "hello-from-sarama";

/// sarama's own default `ClientID`, which `kaf` never overrides -- `getConfig`
/// in `kaf.go` sets `Version` and `Producer.Return.Successes` and leaves
/// `ClientID` alone, so `config.go:558`'s `c.ClientID = defaultClientID` stands.
/// It is what identifies the group member below as this client rather than the
/// suite's own.
const SARAMA_CLIENT_ID: &str = "sarama";

/// How long the group consumer may take to appear as a member of [`GROUP`].
///
/// This has to sit *above* sarama's own give-up ceiling on the group path,
/// which the module doc works out at roughly 85 s, or a timeout here means
/// nothing: a shorter deadline can expire while the client is still inside its
/// own retry chain, and cannot tell that apart from a client that joined and
/// was never recorded. At two minutes, a `kaf` that is still running when this
/// expires is a `kaf` that believes it holds a session -- which puts the fault
/// on what the broker records, not on the join.
const JOIN_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a joined member may then take to read the record and commit.
/// sarama auto-commits marked offsets every second (`config.go:545-546`) and
/// retries a failed commit `Consumer.Offsets.Retry.Max` = 3 times, so this is
/// slack, not a budget.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(45);

/// How often a wait that has seen no change still says it is alive. Without it
/// a stalled phase prints one line and then looks indistinguishable in the log
/// from a phase that was never entered.
const HEARTBEAT: Duration = Duration::from_secs(5);

/// How many lines of the consumer's own output are echoed live. sarama's
/// `--verbose` log is a few dozen lines for a healthy session and unbounded for
/// a client stuck in a retry loop; the whole stream is still captured and
/// asserted on, this only caps what is mirrored into the CI log.
const ECHO_LINE_BUDGET: usize = 400;

/// `docker run` for one `kaf` invocation, with the persistent `--brokers` flag
/// already pointed at the broker's advertised listener.
fn kaf_command(container_name: Option<&str>, args: &[&str]) -> Command {
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "--rm",
        "-i",
        "--add-host=host.docker.internal:host-gateway",
    ]);
    if let Some(name) = container_name {
        cmd.args(["--name", name]);
    }
    cmd.args(["--entrypoint", PROGRAM, IMAGE, "--brokers"]);
    cmd.arg(broker0_advertised());
    cmd.args(args);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Print one container invocation and everything it produced, in the format
/// the other container suites in this tree use. Used for the subcommands that
/// finish in a moment; the long-lived consumer streams instead.
fn log_run<A: Debug>(args: &A, status: &str, stdout: &str, stderr: &str) {
    eprintln!(
        "KRABKA[test] docker run {IMAGE} {args:?} status={status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Drain a child pipe on its own thread into a buffer the test can read at any
/// time, echoing each complete line to the test's own stderr as it arrives.
///
/// A pipe left unread fills at 64 KiB and blocks the writer, which for the
/// long-lived consumer below would stall the client mid-session; draining also
/// means a container that is killed still yields what it printed first. The
/// echo is what puts sarama's account of the join in the log at the moment it
/// happens, rather than in an assertion message that a truncated log never
/// reaches.
fn drain<R: Read + Send + 'static>(
    label: &'static str,
    mut source: R,
) -> (Arc<Mutex<Vec<u8>>>, JoinHandle<()>) {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let pump = {
        let sink = Arc::clone(&sink);
        thread::spawn(move || {
            let mut buf = [0_u8; 4096];
            let mut pending: Vec<u8> = Vec::new();
            let mut echoed = 0_usize;
            loop {
                match source.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => {
                        let chunk = &buf[..read];
                        sink.lock()
                            .expect("kaf output sink")
                            .extend_from_slice(chunk);
                        pending.extend_from_slice(chunk);
                        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                            let line: Vec<u8> = pending.drain(..=end).collect();
                            echo(label, &line, &mut echoed);
                        }
                    }
                }
            }
            if !pending.is_empty() {
                echo(label, &pending, &mut echoed);
            }
        })
    };
    (sink, pump)
}

/// Mirror one line of a container's output, until [`ECHO_LINE_BUDGET`] is
/// spent. The budget is announced once, so a reader knows the stream was cut
/// here and not by the log collector.
fn echo(label: &str, line: &[u8], echoed: &mut usize) {
    if *echoed < ECHO_LINE_BUDGET {
        eprintln!(
            "KRABKA[test] kaf[{label}] {}",
            String::from_utf8_lossy(line).trim_end()
        );
    } else if *echoed == ECHO_LINE_BUDGET {
        eprintln!(
            "KRABKA[test] kaf[{label}] -- {ECHO_LINE_BUDGET} lines echoed, suppressing the rest \
             (all of it is still asserted on) --"
        );
    }
    *echoed += 1;
}

/// A wait's running commentary: it prints the first observation, every change
/// after it, and a heartbeat while nothing changes, each stamped with how long
/// the phase has been running.
///
/// The point is that the log carries the evidence even when the test is killed,
/// the container is killed, or the collector truncates the tail.
struct Progress {
    phase: &'static str,
    started: Instant,
    last: Option<String>,
    printed: Instant,
}

impl Progress {
    fn start(phase: &'static str) -> Self {
        let started = Instant::now();
        eprintln!("KRABKA[test] phase={phase} begin");
        Self {
            phase,
            started,
            last: None,
            printed: started,
        }
    }

    /// Record one observation, printing it if it is new or if the heartbeat is
    /// due.
    fn observe(&mut self, observation: &str) {
        let changed = self.last.as_deref() != Some(observation);
        if changed || self.printed.elapsed() >= HEARTBEAT {
            let phase = self.phase;
            let elapsed = self.started.elapsed();
            eprintln!("KRABKA[test] phase={phase} t={elapsed:.1?} {observation}");
            self.printed = Instant::now();
        }
        if changed {
            self.last = Some(observation.to_owned());
        }
    }

    /// Close the phase out with how it ended and how long it took.
    fn finish(&self, outcome: &str) {
        let phase = self.phase;
        let elapsed = self.started.elapsed();
        eprintln!("KRABKA[test] phase={phase} end t={elapsed:.1?} {outcome}");
    }

    fn elapsed(&self) -> Duration { self.started.elapsed() }
}

/// The failure text of a wait, or the empty string when it succeeded.
fn reason<T>(outcome: &Result<T, String>) -> &str {
    outcome.as_ref().err().map_or("", String::as_str)
}

/// Run one `kaf` subcommand that terminates on its own, and return its stdout.
fn run_kaf(args: &[&str], input: Option<&str>) -> String {
    let mut child = kaf_command(None, args).spawn().expect("spawn kaf");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("kaf stdin")
            .write_all(input.as_bytes())
            .expect("write kaf stdin");
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait for kaf");
    log_run(
        &args,
        &out.status.to_string(),
        &String::from_utf8_lossy(&out.stdout),
        &String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "kaf {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).expect("kaf stdout is utf-8")
}

/// The cells of one `kaf` table row. Every table `kaf` prints is padded with
/// a `text/tabwriter`, so a column is only recoverable by splitting on runs of
/// whitespace -- which is also why every value asserted below is one token.
fn cells(line: &str) -> Vec<String> { line.split_whitespace().map(str::to_owned).collect() }

/// Go's `%v` for a slice of ints: `[1]`, `[1 2]`. `kaf` prints the replica and
/// ISR lists that way, so the expected value has to be built that way too.
fn go_slice(nodes: &[i32]) -> String {
    let joined = nodes
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{joined}]")
}

/// krabka's own `DescribeCluster` answer, read on the host.
async fn krabka_cluster(client: &Client) -> DescribeClusterResponse {
    client
        .send(DescribeClusterRequest::default())
        .await
        .expect("DescribeCluster")
}

/// krabka's own `DescribeTopicPartitions` answer for [`TOPIC`], partitions in
/// index order.
async fn krabka_partitions(client: &Client) -> Vec<DescribeTopicPartitionsResponsePartition> {
    let resp: DescribeTopicPartitionsResponse = client
        .send(DescribeTopicPartitionsRequest {
            topics: vec![TopicRequest {
                name: TOPIC.into(),
                ..Default::default()
            }],
            response_partition_limit: 2000,
            cursor: None,
            ..Default::default()
        })
        .await
        .expect("DescribeTopicPartitions");
    assert!(resp.topics.len() == 1, "{resp:?}");
    let mut partitions = resp.topics[0].partitions.clone();
    partitions.sort_by_key(|partition| partition.partition_index);
    partitions
}

/// `kaf topics`: the `Metadata` listing, asserted as a whole row.
fn metadata_lists_topic() {
    let listing = run_kaf(&["topics"], None);
    let row = listing
        .lines()
        .map(cells)
        .find(|row| row.first().map(String::as_str) == Some(TOPIC))
        .unwrap_or_default();
    // NAME, PARTITIONS, REPLICAS -- the last from the length of partition 0's
    // replica list, which is the replication factor the topic was created at.
    assert!(
        row == vec![TOPIC.to_string(), PARTITIONS.to_string(), "1".to_string()],
        "kaf topics did not list {TOPIC} as krabka created it: {listing}"
    );
}

/// `kaf produce`: one line of stdin becomes one record, and `kaf` reports the
/// partition and offset the broker acknowledged.
fn produce_lands() {
    let receipt = run_kaf(
        &["produce", TOPIC, "--partition", "0"],
        Some(&format!("{PAYLOAD}\n")),
    );
    assert!(
        receipt == "Sent record to partition 0 at offset 0.\n",
        "unexpected produce receipt: {receipt}"
    );
}

/// `kaf consume` running as a consumer group in its own container, with both
/// of its pipes drained and echoed while it runs.
///
/// It never returns on its own: `withConsumerGroup` in `consume.go` hands the
/// session `cmd.Context()`, which `rootCmd.Execute()` leaves as
/// `context.Background()`, and `ConsumeClaim` blocks on the claim's channel.
/// The test stops the container once it has seen what it is waiting for.
struct GroupConsumer {
    /// The `docker run --name` of the container, for `docker kill`.
    container: String,
    child: Child,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    pumps: Vec<JoinHandle<()>>,
}

impl GroupConsumer {
    /// Start `kaf consume` on [`TOPIC`] as a member of [`GROUP`].
    ///
    /// `--group`/`-g` and `--commit` are `consumeCmd`'s own flags:
    ///
    /// ```text
    /// consumeCmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
    /// consumeCmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
    /// ```
    ///
    /// `groupFlag` is what picks the code path -- `withConsumerGroup` when it
    /// is set, `withoutConsumerGroup`, which fetches the partitions directly
    /// and never joins or commits, when it is not -- and `groupCommitFlag`
    /// only ever reaches `s.MarkMessage` inside `ConsumeClaim`, which is on
    /// the group path alone. `--commit` without `--group` is therefore silent
    /// and inert, and a `--group` that fails to bind degrades to a consumer
    /// that looks alive and commits nothing.
    ///
    /// So every flag here sits *before* the positional topic. cobra parses
    /// flags interspersed with arguments and `kaf` sets neither
    /// `TraverseChildren` nor `DisableFlagParsing`, so a trailing flag does
    /// bind -- but a flag that lands where it does not bind is ignored in
    /// silence, and the ordering that cannot be misread costs nothing.
    ///
    /// `--offset oldest` is `offsetFlag`'s own default, and restates it
    /// because it is load-bearing for a *new* group: it is the only thing that
    /// sets `Consumer.Offsets.Initial`, which sarama leaves at `OffsetNewest`,
    /// and a member that starts at the newest offset never sees the record
    /// this suite produced before it joined.
    ///
    /// `--verbose` turns on sarama's own logger (`kaf.go:216-218` points it at
    /// stderr). Nothing else narrates a join, an assignment or a commit, and
    /// stdout stays clean because the log goes to stderr -- which is also why
    /// the two pipes are drained separately below.
    fn spawn(container: &str) -> Self {
        let args = [
            "consume",
            "--group",
            GROUP,
            "--commit",
            "--offset",
            "oldest",
            "--output",
            "raw",
            "--verbose",
            TOPIC,
        ];
        let mut child = kaf_command(Some(container), &args)
            .spawn()
            .expect("spawn kaf consume");
        drop(child.stdin.take());
        eprintln!("KRABKA[test] docker run {IMAGE} {args:?} container={container} started");
        let (stdout, out_pump) = drain("stdout", child.stdout.take().expect("kaf consume stdout"));
        let (stderr, err_pump) = drain("stderr", child.stderr.take().expect("kaf consume stderr"));
        Self {
            container: container.to_owned(),
            child,
            stdout,
            stderr,
            pumps: vec![out_pump, err_pump],
        }
    }

    /// The exit status, once the client has stopped on its own. A `kaf
    /// consume` that ends before the test kills it has failed: `errorExit` is
    /// how every unhandled error in `kaf` leaves, and the group path has no
    /// other exit.
    fn exited(&mut self) -> Option<ExitStatus> { self.child.try_wait().expect("poll kaf consume") }

    /// Stop the container, join the drains, and hand back its stdout. Both
    /// streams were echoed line by line as they arrived, so this prints only
    /// the trailer that says how the container ended.
    fn stop(mut self) -> String {
        // The container may already be gone if `kaf` exited on an error, so a
        // failed `docker kill` is not itself a failure -- the assertions on
        // the client's own output are what decide the case.
        let _ = Command::new("docker")
            .args(["kill", &self.container])
            .output()
            .expect("run docker kill");
        let status = self.child.wait().expect("wait for kaf consume");
        for pump in self.pumps {
            pump.join().expect("join kaf output drain");
        }
        let stdout =
            String::from_utf8_lossy(&self.stdout.lock().expect("kaf consume stdout")).into_owned();
        let stderr_bytes = self.stderr.lock().expect("kaf consume stderr").len();
        let container = &self.container;
        eprintln!(
            "KRABKA[test] docker kill {container} status={status} stdout_bytes={} \
             stderr_bytes={stderr_bytes}",
            stdout.len()
        );
        stdout
    }
}

/// What one `DescribeGroups` answer says about [`GROUP`], reduced to the line
/// the wait logs.
///
/// It keeps `error_code` because krabka answers a group it has never heard of
/// with `GROUP_ID_NOT_FOUND` and an otherwise defaulted entry
/// (`handlers/describe_groups.rs`), whose `group_state` is the empty string.
/// A summary that carried only the state could not tell that apart from a
/// group that exists and is `Empty`, and those two say opposite things about
/// whether the client ever reached the coordinator.
#[derive(Clone, PartialEq, Eq)]
struct GroupShape {
    /// `None` when the response carried no entry for [`GROUP`] at all.
    error_code: Option<i16>,
    state: String,
    protocol_type: String,
    protocol_name: String,
    /// One `client_id` per member, in the order the broker listed them.
    members: Vec<String>,
}

impl GroupShape {
    fn of(response: &DescribeGroupsResponse) -> Self {
        let Some(group) = response.groups.iter().find(|group| group.group_id == GROUP) else {
            return Self {
                error_code: None,
                state: String::new(),
                protocol_type: String::new(),
                protocol_name: String::new(),
                members: Vec::new(),
            };
        };
        Self {
            error_code: Some(group.error_code),
            state: group.group_state.clone(),
            protocol_type: group.protocol_type.clone(),
            protocol_name: group.protocol_data.clone(),
            members: group
                .members
                .iter()
                .map(|member| member.client_id.clone())
                .collect(),
        }
    }

    /// The `client_id` of the first member, when there is one.
    fn first_member(&self) -> Option<&str> { self.members.first().map(String::as_str) }
}

impl Display for GroupShape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error_code {
            None => write!(f, "DescribeGroups({GROUP}): no entry in the response"),
            Some(code) => write!(
                f,
                "DescribeGroups({GROUP}): error_code={code} state={:?} protocol_type={:?} \
                 protocol_name={:?} members={:?}",
                self.state, self.protocol_type, self.protocol_name, self.members
            ),
        }
    }
}

/// krabka's `DescribeGroups` answer for [`GROUP`], read on the host.
async fn describe_group(client: &Client) -> DescribeGroupsResponse {
    client
        .send(DescribeGroupsRequest {
            groups: vec![GROUP.to_string()],
            ..Default::default()
        })
        .await
        .expect("DescribeGroups")
}

/// Wait until `kaf` is a member of [`GROUP`], and return the member's
/// `client_id`.
///
/// This is the assertion that a group was used at all. Without it the only
/// symptom of a `kaf` that fell back to `withoutConsumerGroup` is an offset
/// that never arrives, which looks like a slow broker for as long as the
/// commit wait lasts and then blames the wrong thing.
///
/// krabka lists a group's members in every state, `PreparingRebalance`
/// included, so this does not wait for `Stable` and does not filter on state:
/// the first member of any shape ends the wait.
async fn await_group_member(
    client: &Client,
    consumer: &mut GroupConsumer,
) -> Result<String, String> {
    let mut progress = Progress::start("join");
    let deadline = Instant::now() + JOIN_TIMEOUT;
    loop {
        if let Some(status) = consumer.exited() {
            progress.finish("kaf exited");
            return Err(format!(
                "kaf consume exited on its own with {status} after {:.1?}, before joining \
                 {GROUP}. Every failure on sarama's group path ends in errorExit, so its own \
                 log above says which request was refused.",
                progress.elapsed()
            ));
        }
        let observed = GroupShape::of(&describe_group(client).await);
        progress.observe(&observed.to_string());
        if let Some(member) = observed.first_member() {
            let member = member.to_owned();
            progress.finish(&format!("joined by client_id={member}"));
            return Ok(member);
        }
        let elapsed = progress.elapsed();
        if Instant::now() >= deadline {
            progress.finish("timed out");
            return Err(format!(
                "kaf consume never joined {GROUP} within {JOIN_TIMEOUT:?} ({elapsed:.1?} \
                 elapsed); it is still running, and the last answer was: {observed}. That \
                 deadline is above sarama's own ceiling on the group path -- see this module's \
                 doc -- so a client still alive here is one that did not give up, and the \
                 question is whether the broker recorded the member it thinks it has."
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Poll krabka for the group's committed offsets until one appears.
async fn await_committed_offsets(
    admin: &mut AdminClient,
    consumer: &mut GroupConsumer,
) -> Result<BTreeMap<(String, i32), i64>, String> {
    let mut progress = Progress::start("commit");
    let deadline = Instant::now() + COMMIT_TIMEOUT;
    loop {
        if let Some(status) = consumer.exited() {
            progress.finish("kaf exited");
            return Err(format!(
                "kaf consume exited on its own with {status} after {:.1?}, before committing an \
                 offset for {GROUP}",
                progress.elapsed()
            ));
        }
        let offsets = admin
            .list_consumer_group_offsets(GROUP)
            .await
            .expect("list consumer group offsets");
        progress.observe(&format!("OffsetFetch({GROUP}): {offsets:?}"));
        if !offsets.is_empty() {
            progress.finish("committed");
            return Ok(offsets);
        }
        if Instant::now() >= deadline {
            progress.finish("timed out");
            return Err(format!(
                "no committed offset for {GROUP} within {COMMIT_TIMEOUT:?}, though the client \
                 is a member of it, so the join is not what failed. sarama auto-commits every \
                 second, but only offsets for messages it has already handed ConsumeClaim, so \
                 nothing to commit means nothing was consumed. Read the kaf[stderr] lines \
                 above for the fetch that did not decode; the commit is downstream of it."
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Watch the group consumer through both things that have to happen: it joins
/// [`GROUP`], and the group's committed offset appears. Returns the member's
/// `client_id` with those offsets, or the reason the wait ended, so that the
/// caller can stop the container and print what it said before asserting.
async fn observe_group_consumer(
    client: &Client,
    admin: &mut AdminClient,
    consumer: &mut GroupConsumer,
) -> Result<(String, BTreeMap<(String, i32), i64>), String> {
    let member = await_group_member(client, consumer).await?;
    let offsets = await_committed_offsets(admin, consumer).await?;
    Ok((member, offsets))
}

/// `kaf nodes`: `DescribeCluster` in sarama's sense, which is a `Metadata`
/// round-trip, checked against krabka's own `DescribeCluster` answer.
fn nodes_match_cluster(cluster: &DescribeClusterResponse) {
    let expected: Vec<Vec<String>> = cluster
        .brokers
        .iter()
        .map(|broker| {
            vec![
                broker.broker_id.to_string(),
                format!("{}:{}", broker.host, broker.port),
                (broker.broker_id == cluster.controller_id).to_string(),
            ]
        })
        .collect();
    let listing = run_kaf(&["nodes"], None);
    // The header row is `ID ADDRESS CONTROLLER`; `kaf nodes` has no flag to
    // suppress it (`--no-headers` is registered on `kaf node ls` alone).
    let rows: Vec<Vec<String>> = listing
        .lines()
        .map(cells)
        .filter(|row| row.first().map(String::as_str) != Some("ID"))
        .filter(|row| !row.is_empty())
        .collect();
    assert!(
        rows == expected,
        "kaf nodes disagrees with krabka's DescribeCluster: {listing}"
    );
}

/// `kaf topic describe`: sarama's `DescribeTopics`, again a `Metadata` round
/// trip, checked against krabka's own `DescribeTopicPartitions` answer.
fn topic_describe_matches_partitions(partitions: &[DescribeTopicPartitionsResponsePartition]) {
    let expected: Vec<Vec<String>> = partitions
        .iter()
        .map(|partition| {
            // One record was produced, to partition 0, so that partition's
            // high watermark is 1 and every other partition's is 0.
            let high_watermark = i32::from(partition.partition_index == 0);
            vec![
                partition.partition_index.to_string(),
                high_watermark.to_string(),
                partition.leader_id.to_string(),
                go_slice(&partition.replica_nodes),
                go_slice(&partition.isr_nodes),
            ]
        })
        .collect();

    let description = run_kaf(&["topic", "describe", TOPIC], None);
    // The partition table sits between its dashed divider and the summed
    // watermark line; the config table below it has its own dividers.
    let rows: Vec<Vec<String>> = description
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("---------"))
        .skip(1)
        .take_while(|line| !line.starts_with("Summed"))
        .map(cells)
        .filter(|row| !row.is_empty())
        .collect();
    assert!(
        rows == expected,
        "kaf topic describe disagrees with krabka's DescribeTopicPartitions: {description}"
    );
}

/// `kaf group describe`: `DescribeGroups` plus `OffsetFetch` plus
/// `ListOffsets`, checked against the offsets krabka reports for the group.
fn group_describe_shows_commit(offsets: &BTreeMap<(String, i32), i64>) {
    let description = run_kaf(&["group", "describe", GROUP], None);
    let rows: Vec<Vec<String>> = description.lines().map(cells).collect();
    for ((topic, partition), offset) in offsets {
        // Partition, Group Offset, High Watermark, Lag. The one record is the
        // whole log, so the watermark is the committed offset and the lag 0.
        let expected = vec![
            partition.to_string(),
            offset.to_string(),
            offset.to_string(),
            "0".to_string(),
        ];
        assert!(
            rows.contains(&expected),
            "kaf group describe has no row for {topic}-{partition} at offset \
             {offset}: {description}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn sarama_round_trip_and_cluster_views_agree_with_krabka() {
    let (broker, _dir) = start_host_broker().await;
    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin client");
    admin
        .create_topics(
            &[topic_spec(TOPIC.to_string(), PARTITIONS, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .expect("create conformance topic");
    let client = connect_owned(
        broker.listen_addr().to_string(),
        "krabka-sarama-conformance",
        "client build",
    )
    .await;

    metadata_lists_topic();
    produce_lands();

    let container = format!("krabka-sarama-consumer-{}", std::process::id());
    let mut consumer = GroupConsumer::spawn(&container);
    let outcome = observe_group_consumer(&client, &mut admin, &mut consumer).await;
    // Stop and log the container before asserting, so the client's own account
    // of the run is complete in the output of a failure.
    let consumed_stdout = consumer.stop();
    assert!(outcome.is_ok(), "{}", reason(&outcome));
    let (member, offsets) = outcome.unwrap_or_default();
    // The group was joined by *this* client and not by some other member the
    // broker had lying around.
    assert!(
        member == SARAMA_CLIENT_ID,
        "{GROUP} was joined by client_id={member}, not sarama's default \
         {SARAMA_CLIENT_ID}"
    );
    assert!(
        consumed_stdout == format!("{PAYLOAD}\n"),
        "kaf consume did not return the produced payload: stdout={consumed_stdout}"
    );
    // The record is offset 0, so the group's committed position is 1, on the
    // one partition it was produced to and on no other.
    assert!(
        offsets == BTreeMap::from([((TOPIC.to_string(), 0), 1)]),
        "unexpected committed offsets for {GROUP}: {offsets:?}"
    );
    group_describe_shows_commit(&offsets);

    let cluster = krabka_cluster(&client).await;
    nodes_match_cluster(&cluster);
    let partitions = krabka_partitions(&client).await;
    assert!(
        partitions.len() == usize::try_from(PARTITIONS).expect("partition count fits usize"),
        "krabka describes {} partitions, not {PARTITIONS}",
        partitions.len()
    );
    topic_describe_matches_partitions(&partitions);

    broker.shutdown().await;
}
