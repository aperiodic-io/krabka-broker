//! Topic setup and the JVM produce/consume round trip the tiered-storage
//! suites share.
//!
//! Every helper here is shaped by KIP-405: the topic carries the remote-storage
//! overrides, the producer forces a segment roll, and the consumer reads back
//! offsets whose local segments are already evicted.

use assert2::assert;

use super::{
    docker::{KAFKA_IMAGE, KAFKA_IMAGE_TIERED, docker_run_kafka_tool_with_image},
    minio::minio_list_objects,
    ports::broker0_advertised,
};

/// Segment size every tiered-storage suite boots its broker with, as
/// `BrokerConfig::log_config.segment_size`.
///
/// The topic cannot ask for it. Kafka's topic-level `segment.bytes` has a
/// floor of 1 MiB (`LogConfig`, `atLeast(1024 * 1024)`), which krabka matches,
/// and the only topic config that goes below it is Kafka 4.x's internal
/// `internal.segment.bytes` — a key `TopicCommand` in every image these suites
/// pin rejects client-side, before the request leaves the container. The
/// small segment therefore comes from the broker's own `LogConfig`, which a
/// topic that overrides nothing inherits. Kafka floors its broker-level
/// `log.segment.bytes` at 1 MiB too (`SERVER_CONFIG_DEF`), but that is the
/// validator on a parsed broker config; this is a harness constructing the
/// struct directly, which is how Kafka's own tests get a small segment.
///
/// At this size a produce of a couple of hundred ~30-byte records seals
/// several segments, which is what gives the tier-copy path repeated work.
pub(crate) const TIERED_SEGMENT_SIZE: krabka_units::ByteSize = krabka_units::bytes(2048);

/// Create a KIP-405 tiered topic and wait for the config overrides to propagate
/// into the partition's `LogConfig`.
///
/// This function pairs [`TIERED_SEGMENT_SIZE`], inherited from the broker
/// default, with `local.retention.bytes=1` on the topic, so a small produce
/// batch seals several segments and the broker evicts every copied segment
/// from local disk at once. Later reads must then go through the remote tier.
///
/// The function waits up to 10 s for `ReplicatorSupervisor::reconcile` to
/// apply the topic's overrides to the live partition. Without this gate, the
/// producer's first batches land in a partition with
/// `remote_storage_enable=false` and no local-retention bound, and nothing
/// triggers the tier-copy path. See `compact_log_cleaner_round_trip` for the
/// same pattern.
///
/// # Panics
///
/// If the broker was not started with `log_config.segment_size` set to
/// [`TIERED_SEGMENT_SIZE`]: the produce would then fill one 1 GiB segment and
/// seal nothing.
pub(crate) async fn create_tiered_topic(broker: &krabka_broker::BrokerHandle, topic: &str) {
    // Uses the KIP-405-aware `cp-kafka:7.8.8` image — older clients' `TopicCommand`
    // validates `--config` keys client-side and rejects `remote.storage.enable` /
    // `local.retention.bytes` before the request leaves the container.
    docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--config",
            "remote.storage.enable=true",
            "--config",
            "local.retention.bytes=1",
            "--config",
            "retention.bytes=-1",
            "--config",
            "retention.ms=-1",
            "--bootstrap-server",
            broker0_advertised(),
        ],
    );

    // Only the overrides the JVM CLI actually sent are worth waiting for; the
    // segment size is inherited the moment the partition exists.
    let cfg = crate::jvm_acceptance::wait_jvm_log_config(
        broker,
        topic,
        |cfg| cfg.remote_storage_enable && cfg.local_retention_size == Some(krabka_units::bytes(1)),
        |within| {
            assert!(
                within,
                "tiered-storage topic config never propagated within 10s; saw {:?}",
                broker.partition_log_config_for_test(topic, 0)
            );
        },
    )
    .await;
    assert!(
        cfg.segment_size == TIERED_SEGMENT_SIZE,
        "the tiered harness must boot its broker with log_config.segment_size = \
         {TIERED_SEGMENT_SIZE:?}; the partition inherited {:?}",
        cfg.segment_size
    );
}

/// Stream `n` records with the format `record-NNNN` into `topic` through the
/// JVM console producer.
///
/// This function forces per-record batches with `batch.size=1` and
/// `linger.ms=0`, so the broker rolls segments at [`TIERED_SEGMENT_SIZE`].
/// Without that, the JVM producer collects everything into one large batch
/// and writes it into a single segment. Nothing then triggers a segment
/// roll, and the tier-copy path gets no work.
pub(crate) fn produce_records(topic: &str, n: usize) {
    let mut payload = String::with_capacity(n * 12);
    for i in 0..n {
        use std::fmt::Write as _;
        let _ = writeln!(payload, "record-{i:04}");
    }
    let mut child_command = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE,
        args: &[
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            topic,
            "--producer-property",
            "batch.size=1",
            "--producer-property",
            "linger.ms=0",
            "--producer-property",
            "max.in.flight.requests.per.connection=1",
        ],
        input: crate::support::ContainerInput::Attached,
        ..Default::default()
    });
    let producer_out = crate::support::jvm_stdin_output(&mut child_command, payload.as_bytes());
    assert!(
        producer_out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&producer_out.stderr)
    );
}

/// Poll `mc ls --recursive local/<bucket>` until it lists at least
/// `min_log_objects` entries whose path ends with `.log`, then return the
/// full listing.
///
/// The poll runs at 500 ms intervals for up to 20 s (40 iterations). It
/// panics if the listing never reaches the threshold.
pub(crate) async fn wait_for_minio_segments(bucket: &str, min_log_objects: usize) -> String {
    let mut bucket_listing = String::new();
    let mut copied_log_objects = 0usize;
    for _ in 0..40 {
        // intentional: bounded poll of an external process (MinIO via `mc ls`);
        // no krabka metric reflects object arrival in the bucket.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        bucket_listing = minio_list_objects(bucket);
        copied_log_objects = bucket_listing
            .lines()
            .filter(|line| {
                std::path::Path::new(line)
                    .extension()
                    .is_some_and(|extension| extension == "log")
            })
            .count();
        if copied_log_objects >= min_log_objects {
            return bucket_listing;
        }
    }
    panic!(
        "expected ≥{min_log_objects} segment `.log` objects in MinIO; \
         saw {copied_log_objects}. Bucket listing:\n{bucket_listing}"
    );
}

/// The artifact suffixes a KIP-405 copy writes for every segment, and that
/// `krabka_restore::verify_segment` requires before it will restore one.
///
/// `.txnindex` is absent for a segment with no aborted transaction, so it is
/// not on this list. The order matches the order
/// `S3RemoteStorage::copy_segment_objects` uploads them in: `.log` lands
/// first, which is exactly why counting `.log` objects does not mean a
/// segment is fully copied.
const SEGMENT_ARTIFACTS: [&str; 5] = [
    ".log",
    ".index",
    ".timeindex",
    ".snapshot",
    ".leader_epoch_checkpoint",
];

/// The key each `mc ls --recursive` line names. It is the last
/// whitespace-separated field of the line, after the date, size and class.
fn listed_key(line: &str) -> Option<&str> {
    line.split_whitespace().last()
}

/// Group `listing`'s keys by segment and report `(complete, seen)`: how many
/// segments hold every entry of [`SEGMENT_ARTIFACTS`], and how many segments
/// the listing mentions at all.
fn segment_completeness(listing: &str) -> (usize, usize) {
    let mut segments: std::collections::BTreeMap<&str, std::collections::BTreeSet<&str>> =
        std::collections::BTreeMap::new();
    for key in listing.lines().filter_map(listed_key) {
        // The suffixes share no common tail -- `.timeindex` does not end with
        // `.index` -- so at most one matches, and the stem before it names the
        // segment.
        if let Some(suffix) = SEGMENT_ARTIFACTS
            .iter()
            .find(|suffix| key.ends_with(**suffix))
        {
            let stem = &key[..key.len() - suffix.len()];
            segments.entry(stem).or_default().insert(suffix);
        }
    }
    let complete = segments
        .values()
        .filter(|found| found.len() == SEGMENT_ARTIFACTS.len())
        .count();
    (complete, segments.len())
}

/// Poll `mc ls --recursive local/<bucket>` until the archive holds at least
/// `min_segments` FULLY copied segments and no partly copied one, then return
/// the listing.
///
/// [`wait_for_minio_segments`] counts `.log` objects, which is enough for a
/// test that reads the archive back through the broker that wrote it: a
/// half-copied segment is simply not read. A caller that restores the bucket
/// with no broker behind it needs more, because
/// `krabka_restore::verify_segment` raises `TornCopy` for a segment missing
/// any of [`SEGMENT_ARTIFACTS`], and `copy_segment_objects` uploads the `.log`
/// before them.
///
/// The wait also requires the listing to stop changing, because the copy task
/// is spawned detached and `BrokerHandle::shutdown` cancels its token without
/// joining it: a caller that shuts the source cluster down mid-copy leaves a
/// torn segment in the bucket forever. The producer has already finished by
/// the time this runs, so the set of sealed segments is finite, and a listing
/// that is complete and unchanged for longer than one `RemoteLogManager` tick
/// means the copy task has run out of work -- a terminal condition, not a
/// guess at how long a copy takes.
///
/// The poll runs at 500 ms intervals for up to 30 s. It panics if the archive
/// never settles.
pub(crate) async fn wait_for_settled_minio_segments(bucket: &str, min_segments: usize) -> String {
    // Identical consecutive listings that count as settled. The tiered
    // harness runs the `RemoteLogManager` at a 1 s tick and copies one segment
    // per partition per tick, so three unchanged 500 ms polls span a whole
    // tick that started no new copy.
    const STABLE_POLLS: usize = 3;

    let mut previous = String::new();
    let mut stable = 0usize;
    let mut listing = String::new();
    let mut complete = 0usize;
    let mut seen = 0usize;
    for _ in 0..60 {
        // intentional: bounded poll of an external process (MinIO via `mc ls`);
        // no krabka metric reflects object arrival in the bucket.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        listing = minio_list_objects(bucket);
        (complete, seen) = segment_completeness(&listing);
        stable = if listing == previous { stable + 1 } else { 0 };
        if complete >= min_segments && complete == seen && stable >= STABLE_POLLS {
            return listing;
        }
        previous.clone_from(&listing);
    }
    panic!(
        "expected ≥{min_segments} fully copied segments in MinIO and no torn copy, settled; \
         saw {complete} complete of {seen} after {stable} unchanged polls. \
         Bucket listing:\n{listing}"
    );
}

/// Consume up to `max` records from `topic` (partition 0, from-beginning)
/// with the JVM console consumer. Returns the number of non-empty output
/// lines.
///
/// `bootstrap_host_port` is the Kafka bootstrap address that is visible from
/// inside the Docker container, for example an allocated port.
/// Single-broker callers should pass `broker0_advertised()`.
pub(crate) fn consume_records(
    topic: &str,
    max: usize,
    timeout_ms: u64,
    bootstrap_host_port: &str,
) -> usize {
    consume_record_values(topic, max, timeout_ms, bootstrap_host_port).len()
}

/// [`consume_records`], returning the record values the consumer printed
/// rather than only how many there were.
///
/// A caller that has to say *which* records came back -- the restore case,
/// which reads a rebuilt cluster and must find the archive's oldest record at
/// the front -- needs the lines themselves. `kafka-console-consumer` prints
/// one record value per line with the default formatter.
pub(crate) fn consume_record_values(
    topic: &str,
    max: usize,
    timeout_ms: u64,
    bootstrap_host_port: &str,
) -> Vec<String> {
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TIERED,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap_host_port,
            "--topic",
            topic,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &max.to_string(),
            "--timeout-ms",
            &timeout_ms.to_string(),
        ],
    );
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect()
}
