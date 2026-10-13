// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by compaction.rs / elect_leaders.rs.

//! End-to-end test of JBOD, multi-log-dir, and `DescribeLogDirs` (KIP-113).
//!
//! It boots a single broker with two log directories, creates a 6-partition
//! topic, and asserts:
//!   1. the partition data is spread across both directories, from
//!      least-loaded placement, and
//!   2. `DescribeLogDirs` reports one result per directory, and the union of
//!      those results covers every partition and matches what is on disk.

mod kafka_wire;

mod support;
#[path = "support/two_dir_topic.rs"]
mod two_dir_topic;

use {
    crate::{
        support::{storage::count_partition_dirs, wire::HeaderEncoding as RequestHeaderEncoding},
        two_dir_topic::{PartitionReadiness, start},
    },
    assert2::assert,
    krabka_protocol::owned::{
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
    std::net::SocketAddr,
    tokio::net::TcpStream,
};

const CLIENT_ID: &str = "krabka-jbod-test";

async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    kafka_wire::exchange()
        .stream(&mut stream)
        .request(&DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        })
        .api_key(35)
        .version(4)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .call()
        .await
        .unwrap()
}

/// Count `topic-partition` subdirs for `topic` directly under `dir`.
fn count_topic_dirs(dir: &std::path::Path, topic: &str) -> usize {
    count_partition_dirs(std::fs::read_dir(dir).unwrap(), topic, true)
}

#[tokio::test]
async fn partitions_spread_across_dirs_and_describe_log_dirs_reports_them() {
    let n: i32 = 6;
    let (handle, primary, extra, addr) = start()
        .client_id(CLIENT_ID)
        .partitions(n)
        .readiness(PartitionReadiness::LocalWriterPresent)
        .call()
        .await;

    // 1. Placement spread: both directories hold at least one partition of `t`.
    let in_primary = count_topic_dirs(primary.path(), "t");
    let in_extra = count_topic_dirs(extra.path(), "t");
    assert!(
        in_primary + in_extra == usize::try_from(n).unwrap(),
        "all partitions on disk"
    );
    assert!(
        in_primary > 0 && in_extra > 0,
        "partitions must spread across both dirs: primary={in_primary} extra={in_extra}"
    );

    // 2. DescribeLogDirs reports one result per configured dir, and the
    //    union of `t` partitions across results is the full 0..n set.
    let resp = describe_log_dirs(addr).await;
    assert!(resp.error_code == 0);
    assert!(resp.results.len() == 2, "one result per log dir");

    let mut reported: Vec<i32> = Vec::new();
    for result in &resp.results {
        assert!(result.error_code == 0);
        for topic in &result.topics {
            if topic.name == "t" {
                for p in &topic.partitions {
                    reported.push(p.partition_index);
                    assert!(p.partition_size >= 0);
                    assert!(!p.is_future_key);
                }
            }
        }
    }
    reported.sort_unstable();
    assert!(
        reported == (0..n).collect::<Vec<_>>(),
        "all partitions reported"
    );

    handle.shutdown().await;
}
