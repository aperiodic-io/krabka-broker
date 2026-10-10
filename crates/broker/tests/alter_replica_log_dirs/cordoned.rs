//! KIP-1066 cordoned log directories over the wire, on the two-directory
//! broker: `cordoned.log.dirs` set through `IncrementalAlterConfigs`, the
//! `DescribeLogDirs` v5 `is_cordoned` flag, and the `AlterReplicaLogDirs`
//! refusal of a cordoned destination.
//!
//! Each expectation is what `apache/kafka:4.3.1` answers with
//! `log.dirs=/tmp/d1,/tmp/d2`: `kafka-log-dirs --describe` shows the flag,
//! and `kafka-reassign-partitions` moving a replica into a cordoned directory
//! fails with `InvalidReplicaAssignmentException` (39).

use std::{
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::alter_replica_log_dirs_response::{
        AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
        AlterReplicaLogDirsResponse,
    },
};

use crate::{
    harness::start_two_dir_broker,
    wire::{alter_replica_log_dirs, describe_log_dirs_at, set_broker_config},
};

/// `(log_dir, is_cordoned)` for every directory `DescribeLogDirs` reports at
/// `version`, canonicalized so a symbolic-link temp root compares equal.
async fn cordoned_flags(addr: SocketAddr, version: i16) -> Vec<(std::path::PathBuf, bool)> {
    let mut flags: Vec<_> = describe_log_dirs_at(addr, version)
        .await
        .results
        .into_iter()
        .map(|result| {
            let dir = std::fs::canonicalize(&result.log_dir)
                .unwrap_or_else(|_| std::path::PathBuf::from(&result.log_dir));
            (dir, result.is_cordoned)
        })
        .collect();
    flags.sort();
    flags
}

/// Wait until `DescribeLogDirs` v5 reports `want`. The dynamic config reaches
/// the broker's cordoned set through the metadata image, so the flag follows
/// the committed alter by an image publication.
async fn wait_for_flags(addr: SocketAddr, want: &[(std::path::PathBuf, bool)]) {
    let mut want = want.to_vec();
    want.sort();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let got = cordoned_flags(addr, 5).await;
        if got == want {
            return;
        }
        assert!(Instant::now() <= deadline, "flags never converged: {got:?}");
        // intentional: the cordoned set is broker-local state behind an image
        // watcher, and `DescribeLogDirs` is its only wire observable.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn canonical(dir: &Path) -> std::path::PathBuf {
    std::fs::canonicalize(dir).unwrap()
}

fn move_result(topic: &str, error_code: i16) -> AlterReplicaLogDirsResponse {
    AlterReplicaLogDirsResponse {
        throttle_time_ms: 0,
        results: vec![AlterReplicaLogDirTopicResult {
            topic_name: topic.to_owned(),
            partitions: vec![AlterReplicaLogDirPartitionResult {
                partition_index: 0,
                error_code,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }],
        unknown_tagged_fields: UnknownTaggedFields(vec![]),
    }
}

/// The issue's table: cordon the second directory, then `DescribeLogDirs` v5
/// flags it and v4 has no field to carry the flag; a move into it is 39, and
/// the same move succeeds once it is uncordoned.
#[tokio::test]
async fn a_cordoned_dir_is_flagged_and_refuses_replica_moves() {
    let (handle, primary, extra, addr) = crate::two_dir_topic::start(crate::two_dir_topic::Setup {
        client_id: crate::wire::CLIENT_ID,
        ..Default::default()
    })
    .await;
    let (first, second) = (canonical(primary.path()), canonical(extra.path()));
    let extra_str = std::fs::canonicalize(extra.path())
        .unwrap()
        .display()
        .to_string();
    // The broker names its directories by their configured path, which is
    // what a `cordoned.log.dirs` entry must match.
    let configured = extra.path().display().to_string();

    wait_for_flags(addr, &[(first.clone(), false), (second.clone(), false)]).await;
    assert!(set_broker_config(addr, 1, "cordoned.log.dirs", &configured).await == (0, None));
    wait_for_flags(addr, &[(first.clone(), false), (second.clone(), true)]).await;

    let v4 = cordoned_flags(addr, 4).await;
    let mut uncordoned = vec![(first.clone(), false), (second.clone(), false)];
    uncordoned.sort();
    check!(v4 == uncordoned);

    let refused = alter_replica_log_dirs(addr, Path::new(&extra_str), "t", vec![0]).await;
    check!(refused == move_result("t", 39));

    assert!(set_broker_config(addr, 1, "cordoned.log.dirs", "").await == (0, None));
    wait_for_flags(addr, &[(first, false), (second, false)]).await;
    let accepted = alter_replica_log_dirs(addr, Path::new(&extra_str), "t", vec![0]).await;
    check!(accepted == move_result("t", 0));

    handle.shutdown().await;
}

/// `kafka-configs --alter --add-config cordoned.log.dirs=/nope` on
/// `apache/kafka:4.3.1` fails with `InvalidRequestException` and Kafka's
/// `requirement failed` message, and nothing is cordoned.
#[tokio::test]
async fn an_unknown_cordoned_dir_is_refused() {
    let (handle, _primary, _extra, addr) = start_two_dir_broker().await;
    let refused = set_broker_config(addr, 1, "cordoned.log.dirs", "/nope").await;
    check!(
        refused
            == (
                42,
                Some(
                    "requirement failed: All entries in cordoned.log.dirs must be present in \
                     log.dirs or log.dir. Missing entries : /nope"
                        .to_owned()
                )
            )
    );
    handle.shutdown().await;
}
