//! KIP-405 tiered storage against `MinIO`: segment upload and the remote read
//! that follows local eviction, the KIP-950 disable path through
//! `kafka-configs`, remote-log metadata survival across a broker restart, and a
//! point-in-time restore of the bucket read back by the JVM consumer.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use {
    assert2::{assert, check},
    jvm_acceptance::*,
    krabka_broker::Broker,
};

// 200 records of ~30 bytes seal several segments at TIERED_SEGMENT_SIZE.
const RECORDS: usize = 200;

type MinioTopic = (
    u16,
    MinioContainer,
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    krabka_broker::BrokerConfig,
);

async fn start_minio_topic(topic: &str, rlmm: krabka_broker::RlmmKind) -> MinioTopic {
    let port = minio_port();
    let minio = MinioContainer::start();
    minio_make_bucket(MINIO_BUCKET);
    let s3 = mutable_minio_config(port);
    let (broker, dir, config) = start_host_broker_with_minio_tier(s3, rlmm).await;
    nc_check_connectivity();
    create_tiered_topic(&broker, topic).await;
    produce_records(topic, RECORDS);
    (port, minio, broker, dir, config)
}

// Same multi-thread caveat as `console_producer_round_trip`: blocking
// `Command::output()` calls would starve the broker accept loop on a
// single-threaded runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn tiered_storage_round_trip_through_minio() {
    const TOPIC: &str = "krabka-tiered-minio-itest";

    let (_minio_port, _minio, broker, _dir, _cfg) =
        start_minio_topic(TOPIC, krabka_broker::RlmmKind::InMemory).await;

    // Give the `RemoteLogManager` enough ticks (1 s interval) to (a) copy
    // every sealed segment to MinIO and (b) run the local-retention pass.
    // Each tick handles one segment per partition, so ≥ `RECORDS / batch`
    // ticks plus a margin for the slowest mc handshake — 8 s in practice.
    wait_for_minio_segments(MINIO_BUCKET, 2).await;

    // Consume from offset 0. Older offsets only exist in MinIO at this
    // point (their local segments were dropped by local_retention_pass),
    // so the JVM consumer transparently exercises the remote-read path.
    // Spot-check a sample across the offset range — the very first records
    // are guaranteed to come from MinIO because their segment was evicted
    // before consume started.
    let consumed = consume_records(TOPIC, RECORDS, 20_000, broker0_advertised());
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records from remote tier, got {consumed}"
    );

    broker.shutdown().await;
    // `_minio` is dropped here; the container is removed via `docker rm -f`.
}

/// KIP-950 through the stock tool: `kafka-configs` refuses the bare
/// `remote.storage.enable=false`, and accepts the flip that comes with
/// `remote.log.delete.on.disable=true`. The bucket is empty afterwards and
/// the partition's log start offset has risen to the local log start, so no
/// consumer is left pointing at a segment the flip erased.
///
/// The refusal is the half a runbook hits first: Kafka answers it with
/// `InvalidConfigurationException` and names both ways out, and an operator
/// who reads only "it failed" would otherwise try the same thing again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn tiered_storage_disable_needs_delete_on_disable() {
    const TOPIC: &str = "krabka-tiered-disable-itest";

    let (_minio_port, _minio, broker, _dir, _cfg) =
        start_minio_topic(TOPIC, krabka_broker::RlmmKind::InMemory).await;
    wait_for_minio_segments(MINIO_BUCKET, 2).await;

    let bootstrap = broker0_advertised();
    let alter = |config: &str| -> std::process::Output {
        docker_run_kafka_tool_allowing_failure(&[
            "kafka-configs",
            "--bootstrap-server",
            bootstrap,
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            TOPIC,
            "--add-config",
            config,
        ])
    };

    let refused = alter("remote.storage.enable=false");
    assert!(
        !refused.status.success(),
        "the bare flip must be refused: {}",
        tool_output(&refused)
    );
    let refusal = tool_output(&refused);
    check!(
        refusal.contains("remote.log.delete.on.disable=true"),
        "the refusal must name the delete-on-disable way out: {refusal}"
    );
    check!(
        refusal.contains("remote.log.copy.disable=true"),
        "the refusal must name the read-only tier: {refusal}"
    );

    let accepted = alter("remote.storage.enable=false,remote.log.delete.on.disable=true");
    assert!(
        accepted.status.success(),
        "the flip with delete-on-disable must be accepted: {}",
        tool_output(&accepted)
    );

    // The remote-log manager erases the tier on its next sweep, and raises
    // the log start offset in the same pass.
    //
    // intentional poll: the sweep runs on its own 1 s timer, and the bucket
    // is the only signal that it has run.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut listing = minio_list_objects(MINIO_BUCKET);
    while listing.contains(TOPIC) && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        listing = minio_list_objects(MINIO_BUCKET);
    }
    check!(
        !listing.contains(TOPIC),
        "the disabled topic's segments must be gone from the bucket: {listing}"
    );
    check!(
        broker
            .partition_log_start_for_test(TOPIC, 0)
            .is_some_and(|start| start > 0),
        "the log start offset must have risen to the local log start"
    );

    broker.shutdown().await;
}

// ---------------------------------------------------------------------------
// Topic-backed RLMM durability test (KIP-405 S3 + durable RLMM restart).
//
// Boots with `RlmmKind::TopicBacked`, produces+tiers records, restarts the
// broker against the same `log.dir` (using `BootstrapMode::Rejoin` to skip
// re-initialization), then consumes from offset 0. All records must come
// back — proving `__remote_log_metadata` + snapshot durability across a
// broker restart.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn tiered_storage_topic_rlmm_survives_restart() {
    const TOPIC: &str = "krabka-tiered-restart-itest";

    // Boot with the durable topic-backed RLMM.
    //
    // `bootstrap` is left empty: the broker auto-derives the RLMM metadata
    // client's bootstrap address from its own PLAINTEXT listener via
    // `loopback_bootstrap` (0.0.0.0:9092 → 127.0.0.1:9092). This exercises
    // the fix that makes empty bootstrap work for plaintext single-broker
    // setups without an explicit address. `snapshot_dir` is left empty; the
    // broker derives it from `log.dir` at startup.
    let (_minio_port, _minio, broker, _dir, config) = start_minio_topic(
        TOPIC,
        krabka_broker::RlmmKind::TopicBacked(krabka_broker::KafkaRlmmConfig {
            bootstrap: String::new(),
            num_partitions: 5,
            replication: 1,
            min_isr: 1,
            snapshot_interval: krabka_units::secs(2),
            snapshot_dir: std::path::PathBuf::new(),
            security: None,
            ..krabka_broker::KafkaRlmmConfig::default()
        }),
    )
    .await;

    // Wait for ≥2 segment `.log` objects to appear in MinIO: that means at
    // least two sealed segments have been copied and the local-retention pass
    // has run (evicting them from disk).
    wait_for_minio_segments(MINIO_BUCKET, 2).await;

    // intentional: give the RLMM snapshot task at least one cycle
    // (snapshot_interval=2s) so the on-disk snapshot has a chance to flush
    // before we pull the plug. The snapshot flush has no awaiter/metric. Even
    // if the snapshot hasn't flushed, recovery still succeeds via
    // `__remote_log_metadata` topic replay — the snapshot is only an
    // optimisation that avoids replaying the full topic on startup.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // -----------------------------------------------------------------------
    // RESTART: shut down the broker and re-start against the same log.dir.
    //
    // `BootstrapMode::Rejoin` replays the existing on-disk raft log rather
    // than re-initializing a fresh cluster — the correct mode for restarts.
    // -----------------------------------------------------------------------
    eprintln!("KRABKA[test] shutting down broker for restart test");
    broker.shutdown().await;
    eprintln!("KRABKA[test] broker shut down; restarting with Rejoin mode");

    let mut restart_config = config;
    restart_config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    // `BootstrapMode::Rejoin` replays the existing on-disk raft log rather
    // than re-initializing a fresh cluster — the correct mode for restarts.
    let broker = Broker::start(restart_config).await.expect("restart broker");
    nc_check_connectivity();

    eprintln!("KRABKA[test] broker restarted; consuming from offset 0");

    // Consume from offset 0 post-restart. Older offsets only exist in MinIO;
    // the RLMM must recover its metadata from __remote_log_metadata + snapshot.
    let consumer_out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TIERED, &[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        &RECORDS.to_string(),
        "--timeout-ms",
        "30000",
    ]);
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    let consumed = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    eprintln!("KRABKA[test] consumed {consumed} records post-restart");

    // Spot-check a sample across the offset range.
    for i in [0usize, 1, 50, 100, 150, RECORDS - 1] {
        let needle = format!("record-{i:04}");
        assert!(
            stdout.contains(&needle),
            "consumer missing {needle} post-restart; partial output:\n{}",
            stdout.chars().take(2_000).collect::<String>()
        );
    }
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records from remote tier after restart, got {consumed}"
    );

    broker.shutdown().await;
    // `_minio` is dropped here; `_dir` (log.dir) is also dropped — cleanup.
}

// ---------------------------------------------------------------------------
// KFC-3 point-in-time restore, read back by the JVM console consumer.
//
// The other suites in this file archive segments and read them back through
// the broker that wrote them. This one throws that broker away: after the
// segments are in MinIO it shuts the cluster down, points `krabka-restore` at
// the same bucket, and boots a broker on the directory the restore built. The
// JVM consumer then reads that cluster with `--from-beginning` and no
// knowledge that it is not the original.
//
// That is the client-visible half of KFC-3's "Offsets Are the Contract": a
// stock consumer, not a krabka test client, gets the archived records back at
// the archived offsets. `crates/restore/tests/roundtrip/consume.rs` makes the
// same claim against krabka's own client and checks the exact wire answers;
// this one proves a JVM client is satisfied by them.
// ---------------------------------------------------------------------------

/// The `--archive-s3-*` flags that point `krabka-restore` at the same `MinIO`
/// bucket the broker archived into.
fn restore_argv(minio_port: u16, log_dir: &std::path::Path) -> Vec<String> {
    [
        "krabka-restore",
        "--archive-s3-bucket",
        MINIO_BUCKET,
        "--archive-s3-region",
        "us-east-1",
        "--archive-s3-endpoint",
        &format!("http://127.0.0.1:{minio_port}"),
        "--archive-s3-access-key-id",
        MINIO_ACCESS_KEY,
        "--archive-s3-secret-access-key",
        MINIO_SECRET_KEY,
        "--archive-s3-allow-http",
        "--log-dir",
        &log_dir.display().to_string(),
        "--node-id",
        "1",
        "--standalone",
        "--controller-listener",
        controller_addr_0(),
    ]
    .iter()
    .map(|arg| (*arg).to_owned())
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn restored_cluster_serves_the_jvm_console_consumer() {
    const TOPIC: &str = "krabka-tiered-restore-itest";

    let (minio_port, _minio, broker, source_dir, _cfg) =
        start_minio_topic(TOPIC, krabka_broker::RlmmKind::InMemory).await;
    // Not `wait_for_minio_segments`: that returns as soon as the `.log`
    // objects appear, and `copy_segment_objects` uploads the `.log` before the
    // indexes, producer snapshot and leader-epoch checkpoint that
    // `verify_segment` requires. Shutting the source cluster down on such a
    // listing leaves a torn segment the restore then refuses. This waits for
    // whole segments and for the copy task to run out of work.
    wait_for_settled_minio_segments(MINIO_BUCKET, 2).await;

    // The source cluster is gone from here on: the restore reads the bucket,
    // and nothing else. Dropping its log directory is what makes that true --
    // a restored broker that somehow still read local state would fail rather
    // than quietly pass.
    broker.shutdown().await;
    drop(source_dir);

    let target = tempfile::tempdir().expect("restore target parent");
    let log_dir = target.path().join("restored");
    let report = krabka_restore::restore(
        &<krabka_restore::Cli as clap::Parser>::try_parse_from(restore_argv(minio_port, &log_dir))
            .expect("valid restore command line")
            .args,
    )
    .await
    .expect("restore the MinIO archive");

    // What the archive actually held. Only sealed, copied segments reach the
    // bucket, so this is at most `RECORDS` -- the active segment's tail was
    // never tiered, exactly as KIP-405 defines the copy path.
    let restored: usize = report
        .partitions
        .iter()
        .flat_map(|partition| &partition.segments)
        .map(|segment| usize::try_from(segment.records_kept).expect("small count"))
        .sum();
    assert!(
        restored > 0 && restored <= RECORDS,
        "restore recovered {restored} records from the bucket, expected 1..={RECORDS}"
    );

    // Boot a broker on the restore's output. `start_host_broker_with` builds
    // its own tempdir; `log_dir` replaces it, and the tempdir it returns is
    // dropped unused.
    let restored_log_dir = log_dir.clone();
    let (restored_broker, _unused_dir) = start_host_broker_with(move |config| {
        config.log_dir = restored_log_dir;
    })
    .await;
    nc_check_connectivity();

    let consumed = consume_record_values(TOPIC, restored, 30_000, broker0_advertised());
    check!(
        consumed.len() == restored,
        "expected the {restored} archived records from the restored cluster, got {}",
        consumed.len()
    );
    // The archive's first record is the partition's first record: the copy
    // path tiers segments oldest-first, so offset 0 is in the bucket whenever
    // anything is. `--from-beginning` on the restored cluster must therefore
    // start there and run forward without a gap.
    let expected: Vec<String> = (0..restored).map(|i| format!("record-{i:04}")).collect();
    check!(consumed == expected);

    restored_broker.shutdown().await;
    // `_minio` is dropped here; the container is removed via `docker rm -f`.
}

fn mutable_minio_config(port: u16) -> krabka_remote_storage::S3Config {
    krabka_remote_storage::S3Config {
        bucket: MINIO_BUCKET.into(),
        region: "us-east-1".into(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.into()),
        secret_access_key: Some(MINIO_SECRET_KEY.into()),
        allow_http: true,
        // Force multipart on these small segment fixtures; MinIO permits small parts.
        multipart_threshold: 4 * 1024,
        multipart_chunk_size: 1024,
        conditional_put: false,
        checksum_sha256: false,
        ..krabka_remote_storage::S3Config::default()
    }
}
