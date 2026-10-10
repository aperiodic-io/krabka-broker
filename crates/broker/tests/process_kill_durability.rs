//! What an `acks=all` ack is worth when the process holding the log is killed.
//!
//! Every other durability suite in this repository stops a broker by calling
//! [`krabka_broker::BrokerHandle::shutdown`], which cancels the replicator
//! supervisor, drains the disk scanner and awaits every listener task before
//! anything drops. That is a graceful drain, and a graceful drain is not the
//! question an operator asks. The question is whether a record the client was
//! *told* was durable is still there after the process that told it so went
//! away without warning.
//!
//! So this suite runs the broker as a real process out of `//packaging`'s
//! image -- the same binary, base and entrypoint that ship -- and kills it with
//! `SIGKILL`, mid-produce, over and over.
//!
//! # The fsync policy the guarantee is stated under
//!
//! [`krabka_log::LogConfig::flush_on_append`] defaults to `false` and no broker
//! path turns it on: the classic partition log is written with `write(2)` and
//! is *not* fsynced per append.
//! [`flush_on_append_stays_off_by_default`] pins that, because it is the
//! premise of everything below. Under it the guarantee this suite proves is:
//!
//! > A single-broker cluster with `acks=all` and `min.insync.replicas=1`
//! > keeps every record the client saw acked across a `SIGKILL` of the broker
//! > process, and comes back with its log ending on a valid batch boundary.
//!
//! That is a statement about *process* loss, not machine loss. `SIGKILL` ends
//! the process; it does not evict the page cache, so bytes the broker had
//! already handed the kernel are still on their way to the disk and the file
//! the next boot opens contains them. Machine loss is the strictly harder case
//! and, without a per-append fsync, is not covered here -- see the tail-tearing
//! note below for how far this suite goes towards it.
//!
//! One broker, deliberately: a follower that already holds the record would
//! make the ack survivable for a reason that has nothing to do with fsync.
//! With `replication.factor=1` the leader is the only copy, so the answer is
//! about the write path alone.
//!
//! # Reaching a torn tail deterministically
//!
//! A partial trailing batch is what `Log::open`'s tail recovery exists for, and
//! until now it was only ever fed hand-written files in unit tests. Killing a
//! process cannot be relied on to produce one: `write(2)` to a regular file is
//! not interrupted halfway by a signal, and the page cache outlives the process
//! either way. Waiting for a lucky kill would make "a torn tail is reached" a
//! claim rather than a fact.
//!
//! So the tear is made by hand, on the dead process's file, out of bytes the
//! broker itself wrote: [`tear_tail`] copies the first half of the last
//! complete batch onto the end of the segment. That is byte-for-byte the state
//! a machine that lost power mid-write leaves behind -- a header promising
//! `batch_length` bytes with fewer than that following it -- and it lands
//! strictly *after* every record the client was told was durable, so the
//! invariant under test is untouched. [`torn_tail_is_a_partial_trailing_batch`]
//! checks the tear is really a tear before the broker sees it, and the cycle
//! loop fails if no cycle ever produced one.
//!
//! # What every cycle asserts
//!
//! 1. every key the client saw acked at `acks=all` is readable after restart;
//! 2. the recovered segment walks batch-by-batch to exactly EOF, with every
//!    CRC intact -- no partial batch and no garbage left in the log.
//!
//! Half the cycles are a bare `SIGKILL` and half tear the tail as well, so both
//! recovery paths run several times in one execution.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::Client;
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_protocol::{
    owned::create_topics_request::{CreatableTopic, CreateTopicsRequest},
    records::{CRC_COVERAGE_START, HEADER_LEN, RecordBatchHeader},
};
use support::{docker, free_port};
use zerocopy::FromBytes;

mod support;

/// The tag `//packaging:image_load` loads the broker image under.
///
/// `//bazel/defs.bzl` sets `KRABKA_BROKER_IMAGE` to the same string from
/// `//bazel/krabka_image.bzl`, so the Bazel lane and a hand-run
/// `bazel run -c opt //packaging:image_load && cargo test -- --ignored` agree.
const DEFAULT_IMAGE: &str = "docker.io/krabka-io/krabka-broker:dev";

/// Where the image's `working_dir` is, and what the data volume mounts on.
const CONTAINER_ROOT: &str = "/var/lib/krabka";

/// The formatted log directory inside that mount.
const CONTAINER_LOG_DIR: &str = "/var/lib/krabka/data";

/// Kill/restart cycles. Odd-numbered ones tear the tail as well, so this many
/// gives three bare kills and three torn tails in one run.
const CYCLES: usize = 6;

/// Records produced and individually acked before the kill is fired. These are
/// the floor on "the client saw an ack": whatever the in-flight burst manages
/// on top is a bonus.
const SETTLED_RECORDS: usize = 20;

/// Upper bound on the produce attempts made while the kill is in flight. The
/// burst stops at the first failure; this only keeps a broker that somehow
/// survives from looping forever.
const BURST_ATTEMPTS: usize = 500;

/// How long a restarted broker gets to answer a produce.
const READY_TIMEOUT: Duration = Duration::from_secs(90);

/// The image tag to run.
fn image() -> String {
    std::env::var("KRABKA_BROKER_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_owned())
}

/// One broker, running as a container process out of the packaging image.
///
/// The data directory is a host directory bind-mounted at the image's
/// `working_dir`, which is what lets the test read and tear the very bytes the
/// broker is writing. The container is created once and then stopped and
/// started again per cycle, so every boot after the first opens the log the
/// previous incarnation left behind.
struct BrokerProcess {
    name: String,
    /// Published host port for the container's 9092.
    port: u16,
    /// Host side of the `/var/lib/krabka` mount.
    root: tempfile::TempDir,
}

impl BrokerProcess {
    /// Format a fresh log directory and boot a broker on it.
    fn start() -> Self {
        let root = tempfile::TempDir::new().expect("host data directory");
        let name = format!("krabka-kill-{}", std::process::id());
        let port = free_port();
        let node = Self { name, port, root };

        // Run as whoever owns the host directory. The image's own user is
        // 65532, which cannot write a directory this test created.
        node.run_tool(
            "/usr/bin/krabka-format",
            &[
                &format!("--log-dir={CONTAINER_LOG_DIR}"),
                "--standalone",
                "--node-id=1",
                // `--listen-addr` is set below, so the broker derives its
                // controller listener from it: same address, port 9093.
                "--controller-listener=0.0.0.0:9093",
                "--ignore-formatted",
            ],
        );

        let mount = format!("{}:{CONTAINER_ROOT}", node.host_root().display());
        let publish = format!("127.0.0.1:{}:9092", node.port);
        let advertised = format!("--advertised-listener=127.0.0.1:{}", node.port);
        docker(&[
            "run",
            "--detach",
            "--name",
            &node.name,
            "--user",
            &node.user(),
            "--volume",
            &mount,
            "--publish",
            &publish,
            &image(),
            &format!("--log-dir={CONTAINER_LOG_DIR}"),
            "--broker-id=1",
            "--listen-addr=0.0.0.0:9092",
            &advertised,
            "--metrics-listen-addr=none",
            "--health-listen-addr=none",
            // One broker holds every replica, as a single-node Kafka sets
            // `offsets.topic.replication.factor=1`. The default of 3 leaves
            // `__consumer_offsets` uncreatable and the consumer below with
            // no coordinator.
            "--offsets-topic-replication-factor=1",
        ]);
        node
    }

    /// `uid:gid` of the host data directory, for `docker run --user`.
    fn user(&self) -> String {
        crate::support::storage::host_directory_user(self.host_root())
    }

    fn host_root(&self) -> &Path {
        self.root.path()
    }

    /// The host-side log directory the container writes into.
    fn host_log_dir(&self) -> PathBuf {
        self.host_root().join("data")
    }

    fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Run one of the image's other binaries against the same mount.
    fn run_tool(&self, entrypoint: &str, args: &[&str]) {
        let mount = format!("{}:{CONTAINER_ROOT}", self.host_root().display());
        let user = self.user();
        let img = image();
        let mut command = vec![
            "run",
            "--rm",
            "--user",
            &user,
            "--volume",
            &mount,
            "--entrypoint",
            entrypoint,
            &img,
        ];
        command.extend_from_slice(args);
        docker(&command);
    }

    /// Block until the container has exited, and return its status.
    fn wait_exit(&self) -> String {
        docker(&["wait", &self.name])
    }

    /// Boot the same container again, on the log directory it left behind.
    fn restart(&self) {
        docker(&["start", &self.name]);
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        // Best effort: a container that never started leaves nothing to remove,
        // and a panic in `drop` would replace the real failure with this one.
        crate::support::remove_container_with_volumes(&self.name);
    }
}

/// One batch found by [`walk_batches`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Batch {
    start: usize,
    end: usize,
    base_offset: i64,
    last_offset: i64,
}

/// How a segment file ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tail {
    /// The last batch ends exactly at EOF: a valid batch boundary.
    Boundary,
    /// `n` bytes past the last complete batch: a partial trailing batch.
    Partial(usize),
}

/// Walk `bytes` as a chain of v2 record batches.
///
/// Returns the batches read and how the file ends. A batch counts as read only
/// when its declared length fits and its CRC32C over `CRC_COVERAGE_START..`
/// matches, so a batch whose bytes are present but damaged ends the walk the
/// same way a short one does.
fn walk_batches(bytes: &[u8]) -> (Vec<Batch>, Tail) {
    let mut batches = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let Some(header_bytes) = bytes.get(at..at + HEADER_LEN) else {
            break;
        };
        let Ok(header) = RecordBatchHeader::ref_from_bytes(header_bytes) else {
            break;
        };
        let Ok(declared) = usize::try_from(header.batch_length.get()) else {
            break;
        };
        // `batch_length` counts everything after itself, and it and
        // `base_offset` are the first twelve bytes. A batch shorter than the
        // fixed header cannot be one, whatever its header claims.
        if 12 + declared < HEADER_LEN {
            break;
        }
        let Some(end) = at.checked_add(12 + declared) else {
            break;
        };
        let Some(batch) = bytes.get(at..end) else {
            break;
        };
        if header.magic != 2 || crc32c::crc32c(&batch[CRC_COVERAGE_START..]) != header.crc.get() {
            break;
        }
        batches.push(Batch {
            start: at,
            end,
            base_offset: header.base_offset.get(),
            last_offset: header.base_offset.get() + i64::from(header.last_offset_delta.get()),
        });
        at = end;
    }
    let tail = if at == bytes.len() {
        Tail::Boundary
    } else {
        Tail::Partial(bytes.len() - at)
    };
    (batches, tail)
}

/// The active segment of `topic`'s partition 0, under a formatted log dir.
///
/// The highest base offset is the active one; the directory is found by name so
/// the test does not restate the broker's `<topic>-<partition>` convention as a
/// path it builds itself.
fn active_segment(log_dir: &Path, topic: &str) -> PathBuf {
    let prefix = format!("{topic}-");
    let partition_dir = std::fs::read_dir(log_dir)
        .expect("read the log directory")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
        })
        .unwrap_or_else(|| {
            panic!(
                "no partition directory for {topic} under {}",
                log_dir.display()
            )
        });
    let mut segments: Vec<PathBuf> = std::fs::read_dir(&partition_dir)
        .expect("read the partition directory")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    segments.sort();
    segments
        .pop()
        .unwrap_or_else(|| panic!("no segment in {}", partition_dir.display()))
}

/// Append the first half of the segment's last complete batch to it.
///
/// The result is a header that promises more bytes than follow it: exactly what
/// an interrupted write leaves, built out of bytes the broker wrote itself
/// rather than out of invented ones. Returns the byte the file now ends at.
///
/// The copy lands after every complete batch, so no record -- acked or not --
/// loses a byte.
fn tear_tail(segment: &Path) -> usize {
    use std::io::Write as _;

    let bytes = std::fs::read(segment).expect("read the segment");
    let (batches, tail) = walk_batches(&bytes);
    assert!(
        tail == Tail::Boundary,
        "a kill left {} already torn; the tear below assumes a whole file",
        segment.display()
    );
    let last = *batches
        .last()
        .expect("the segment holds at least one batch");
    let half = (last.end - last.start) / 2;
    // Twelve bytes is `base_offset` plus `batch_length`, so the tail that is
    // left states a length nothing follows: the reader can see it is short
    // rather than having to infer it from a run-off-the-end.
    assert!(
        half >= 12,
        "a batch of {} bytes is too short to tear",
        last.end - last.start
    );

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(segment)
        .expect("open the segment for the tear");
    file.write_all(&bytes[last.start..last.start + half])
        .expect("write the partial batch");
    file.flush().expect("flush the partial batch");
    bytes.len() + half
}

/// Leave `segment` ending in a partial trailing batch, whatever it ends in now.
///
/// A `SIGKILL` is not expected to tear the tail on its own -- see the module
/// documentation -- but if one ever does, that is the state under test and
/// there is nothing left to arrange.
fn ensure_torn(segment: &Path) {
    let before = std::fs::read(segment).expect("read the segment");
    if let (_, Tail::Partial(n)) = walk_batches(&before) {
        eprintln!(
            "{} was already torn by the kill: {n} trailing bytes",
            segment.display()
        );
        return;
    }
    let torn_len = tear_tail(segment);
    let after = std::fs::read(segment).expect("read the torn segment");
    let (_, tail) = walk_batches(&after);
    assert!(
        matches!(tail, Tail::Partial(_)),
        "the tear left no partial batch in a {torn_len}-byte segment"
    );
}

/// Produce one record per key, awaiting each ack, and return the keys acked.
async fn produce_settled(producer: &Producer, topic: &str, keys: &[String]) -> Vec<String> {
    let mut acked = Vec::new();
    for key in keys {
        match producer.send(record(topic, key)).await {
            Ok(_) => acked.push(key.clone()),
            Err(_) => break,
        }
    }
    acked
}

fn record(topic: &str, key: &str) -> ProducerRecord {
    crate::support::producer::producer_record(crate::support::producer::ProducerRecordSetup {
        topic: topic.to_owned(),
        partition: Some(krabka_ids::PartitionIndex(0)),
        key: Some(key.to_owned().into()),
        value: Some(key.to_owned().into()),
    })
}

/// A producer that reports an ack only when the broker gave it one.
///
/// Idempotence off and `retries=0`: a retry would let the client re-send after
/// the restart and report an ack for a record the killed broker never
/// acknowledged, which is precisely the confusion this suite has to avoid.
/// `acks=All` with one in-sync replica is the leader's own commit.
async fn producer_for(bootstrap: &str) -> Producer {
    crate::support::producer::no_retry_acks_all_producer(bootstrap, None, "build a producer").await
}

/// Wait until a fresh producer can get one record acked, and return its key.
///
/// This is the readiness probe: a broker that acks a produce has opened its log
/// directory, recovered whatever the last incarnation left, elected itself
/// leader and admitted the write. The probe record is a real acked record, so
/// the caller adds it to the expected set like any other.
async fn wait_ready(bootstrap: &str, topic: &str, key: &str) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last: Option<String> = None;
    while Instant::now() < deadline {
        if let Ok(producer) = Producer::builder()
            .bootstrap(bootstrap)
            .acks(Acks::All)
            .enable_idempotence(false)
            .retries(0)
            .linger(Duration::ZERO)
            .build()
            .await
        {
            match producer.send(record(topic, key)).await {
                Ok(_) => return,
                Err(e) => last = Some(format!("{e}")),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("broker at {bootstrap} did not ack a produce within {READY_TIMEOUT:?}: {last:?}");
}

/// Every key readable from the topic, from the earliest offset.
async fn read_all(bootstrap: &str, topic: &str, group: &str, want: usize) -> BTreeSet<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .subscribe(vec![topic.to_owned()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("build a consumer");

    let mut keys = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while keys.len() < want && Instant::now() < deadline {
        let records = consumer
            .poll(krabka_units::millis(500))
            .await
            .expect("poll the topic");
        for record in records {
            if let Some(key) = record.key {
                keys.insert(String::from_utf8_lossy(&key).into_owned());
            }
        }
    }
    consumer.close().await.expect("close the consumer");
    keys
}

/// The premise every claim in this file rests on.
///
/// `flush_on_append` is off, so nothing below is a statement about a machine
/// that lost power: it is a statement about a process that was killed while the
/// kernel kept its page cache. A change that turned this on would strengthen
/// the guarantee, and a change that made the broker set it per partition would
/// mean this suite is describing a configuration nobody runs -- either way, the
/// text above needs rewriting, so it fails here first.
#[test]
fn flush_on_append_stays_off_by_default() {
    check!(!krabka_log::LogConfig::default().flush_on_append);
}

/// A torn tail is a partial trailing batch, and the walk says so.
///
/// [`tear_tail`] is what makes the container cycles deterministic, so what it
/// produces is worth checking without a daemon in the way: a whole file walks
/// to a boundary, and the same file with half a batch appended walks to the
/// same batches and reports exactly those extra bytes as partial.
#[test]
fn torn_tail_is_a_partial_trailing_batch() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let segment = dir.path().join("00000000000000000000.log");
    let whole = batch_bytes(0, 3);
    std::fs::write(&segment, &whole).expect("write a whole segment");

    let (before, tail) = walk_batches(&whole);
    check!(tail == Tail::Boundary);
    check!(before.len() == 1);
    check!(
        before[0]
            == Batch {
                start: 0,
                end: whole.len(),
                base_offset: 0,
                last_offset: 3,
            }
    );

    let torn_len = tear_tail(&segment);
    let torn = std::fs::read(&segment).expect("read the torn segment");
    check!(torn.len() == torn_len);
    check!(walk_batches(&torn) == (before, Tail::Partial(whole.len() / 2)));
}

/// One well-formed empty v2 batch, for [`torn_tail_is_a_partial_trailing_batch`].
///
/// Empty of records but complete as a batch: the walk reads headers and CRCs,
/// which is all this needs to exercise.
fn batch_bytes(base_offset: i64, last_offset_delta: i32) -> Vec<u8> {
    let mut bytes = vec![0u8; HEADER_LEN];
    bytes[0..8].copy_from_slice(&base_offset.to_be_bytes());
    let batch_length = i32::try_from(HEADER_LEN - 12).expect("header fits in an i32");
    bytes[8..12].copy_from_slice(&batch_length.to_be_bytes());
    bytes[16] = 2;
    bytes[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    let crc = crc32c::crc32c(&bytes[CRC_COVERAGE_START..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    bytes
}

/// Create the suite's topic, retrying until the fresh broker answers.
///
/// A container that has just started is not yet listening, so the first attempt
/// is expected to fail on the connection rather than on the request.
async fn create_topic(bootstrap: &str, topic: &str) {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(admin) = Client::builder().bootstrap(bootstrap).build().await {
            let created = admin
                .send(CreateTopicsRequest {
                    topics: vec![CreatableTopic {
                        name: topic.into(),
                        num_partitions: 1,
                        // One replica: the leader is the only copy, so a
                        // surviving ack is the write path's doing rather than a
                        // follower's.
                        replication_factor: 1,
                        ..Default::default()
                    }],
                    timeout_ms: 10_000,
                    ..Default::default()
                })
                .await;
            match created {
                Ok(response) if response.topics[0].error_code == 0 => return,
                Ok(response) => last = format!("error code {}", response.topics[0].error_code),
                Err(e) => last = format!("{e}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("could not create {topic} on {bootstrap} within {READY_TIMEOUT:?}: {last}");
}

/// Produce until the broker stops answering, and return every key it acked.
///
/// The kill runs on its own task so the `SIGKILL` lands somewhere inside this
/// sequence of produce requests rather than between two quiet moments.
async fn produce_through_a_kill(
    producer: &Producer,
    topic: &str,
    cycle: usize,
    container: String,
) -> Vec<String> {
    let killer = tokio::task::spawn_blocking(move || {
        let out = Command::new("docker")
            .args(["kill", "--signal", "KILL", &container])
            .output()
            .expect("spawn docker kill");
        assert!(out.status.success(), "docker kill failed: {out:?}");
    });

    let mut acked = Vec::new();
    for i in 0..BURST_ATTEMPTS {
        let key = format!("c{cycle}-b{i}");
        match producer.send(record(topic, &key)).await {
            Ok(_) => acked.push(key),
            Err(_) => break,
        }
    }
    killer.await.expect("the kill task");
    acked
}

/// Assert the recovered segment is a whole chain of batches ending at EOF.
///
/// The readiness probe runs before this, so a tail recovery that had left the
/// partial batch in place shows up as a chain that stops short of the end.
fn check_recovered_segment(segment: &Path, cycle: usize) {
    let bytes = std::fs::read(segment).expect("read the recovered segment");
    let (batches, tail) = walk_batches(&bytes);
    let shown = segment.display();
    check!(
        tail == Tail::Boundary,
        "cycle {cycle}: {shown} does not end on a batch boundary"
    );
    check!(!batches.is_empty(), "cycle {cycle}: {shown} is empty");
    for pair in batches.windows(2) {
        check!(
            pair[1].base_offset == pair[0].last_offset + 1,
            "cycle {cycle}: an offset gap between batches at byte {}",
            pair[1].start
        );
    }
}

/// One kill/restart cycle. Returns the keys the client saw acked in it.
async fn kill_cycle(broker: &BrokerProcess, topic: &str, cycle: usize, tear: bool) -> Vec<String> {
    let bootstrap = broker.bootstrap();
    let mut acked = Vec::new();

    let ready_key = format!("c{cycle}-ready");
    wait_ready(&bootstrap, topic, &ready_key).await;
    acked.push(ready_key);

    let producer = producer_for(&bootstrap).await;

    // A floor of acks that are unambiguously the client's: each was awaited to
    // completion before the next was sent.
    let settled: Vec<String> = (0..SETTLED_RECORDS)
        .map(|i| format!("c{cycle}-s{i}"))
        .collect();
    let got = produce_settled(&producer, topic, &settled).await;
    assert!(
        got.len() == SETTLED_RECORDS,
        "cycle {cycle}: the broker acked {} of {SETTLED_RECORDS} settled records",
        got.len()
    );
    acked.extend(got);
    acked.extend(produce_through_a_kill(&producer, topic, cycle, broker.name.clone()).await);

    // 137 is 128 + SIGKILL: the daemon's record that the process did not get to
    // run a line of shutdown code.
    let status = broker.wait_exit();
    check!(status == "137", "cycle {cycle}: broker exited {status}");

    if tear {
        ensure_torn(&active_segment(&broker.host_log_dir(), topic));
    }

    broker.restart();
    let recovered_key = format!("c{cycle}-recovered");
    wait_ready(&bootstrap, topic, &recovered_key).await;
    acked.push(recovered_key);

    check_recovered_segment(&active_segment(&broker.host_log_dir(), topic), cycle);
    acked
}

/// `SIGKILL` the broker mid-produce, over and over, and prove every ack held.
///
/// See the module documentation for the guarantee, the fsync policy it is
/// stated under, and why the torn tail is made rather than waited for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and //packaging:image"]
async fn acks_all_survives_a_sigkill_of_the_broker_process() {
    const TOPIC: &str = "kill-durability";

    let broker = BrokerProcess::start();
    let bootstrap = broker.bootstrap();
    create_topic(&bootstrap, TOPIC).await;

    // Every key the client was ever told was durable, across every cycle.
    let mut acked: BTreeSet<String> = BTreeSet::new();
    let mut torn_cycles = 0usize;

    for cycle in 0..CYCLES {
        // Half the cycles are a bare kill and half tear the tail as well, so
        // both recovery paths run several times in one execution.
        let tear = cycle % 2 == 1;
        acked.extend(kill_cycle(&broker, TOPIC, cycle, tear).await);
        if tear {
            torn_cycles += 1;
        }

        let readable = read_all(&bootstrap, TOPIC, &format!("verify-{cycle}"), acked.len()).await;
        let lost: Vec<&String> = acked.difference(&readable).collect();
        assert!(
            lost.is_empty(),
            "cycle {cycle}: {} acked records are gone after the kill: {lost:?}",
            lost.len()
        );
    }

    // "Deterministically, not by luck": were the tear ever to stop producing a
    // partial batch, the cycles above would prove nothing about tail recovery
    // and would still pass.
    assert!(
        torn_cycles == CYCLES / 2,
        "expected {} torn-tail cycles, reached {torn_cycles}",
        CYCLES / 2
    );
}
