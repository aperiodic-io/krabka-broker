//! The rejection paths of `AlterReplicaLogDirs`: a target that is not a
//! configured `log.dirs` entry, a replica this broker does not host, and a
//! principal without Cluster Alter.
//!
//! Each scenario asserts the Kafka error code per requested partition, because
//! that per-partition row is what the JVM admin client reads.

use assert2::assert;
use krabka_broker::Broker;

use crate::{harness::start_two_dir_broker, wire::alter_replica_log_dirs};

#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_target() {
    let (handle, _primary, _extra, addr) =
        crate::two_dir_topic::start(crate::two_dir_topic::Setup {
            client_id: crate::wire::CLIENT_ID,
            ..Default::default()
        })
        .await;

    let bogus = tempfile::tempdir().unwrap();
    let resp = alter_replica_log_dirs(addr, bogus.path(), "t", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic in response");
    // 57 == LOG_DIR_NOT_FOUND
    assert!(topic.partitions[0].error_code == 57);

    handle.shutdown().await;
}

#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_replica() {
    let (handle, _primary, extra, addr) = start_two_dir_broker().await;

    // Don't create the topic — naming a partition we don't host
    // should return REPLICA_NOT_AVAILABLE.
    let resp = alter_replica_log_dirs(addr, extra.path(), "missing", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "missing")
        .expect("topic in response");
    // 9 == REPLICA_NOT_AVAILABLE
    assert!(topic.partitions[0].error_code == 9);

    handle.shutdown().await;
}

/// Boot a two-dir broker with `SimpleAclAuthorizer`, no super-users, and
/// no ACL grants, so every `authorize()` returns Deny. Cluster.Alter on
/// `AlterReplicaLogDirs` (`api_key` 34) must come back as
/// `CLUSTER_AUTHORIZATION_FAILED` for every listed partition.
#[tokio::test]
async fn alter_replica_log_dirs_denied_without_cluster_alter() {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = crate::support::storage::two_dir_config(primary.path(), extra.path());
    // Default authorizer for `for_tests` is AllowAll; swap in a deny-
    // everything SimpleAclAuthorizer (empty super-users + empty ACL
    // image) so the Cluster Alter gate engages.
    cfg.super_users = std::collections::HashSet::new();
    crate::support::acl::use_simple_acl_authorizer(&mut cfg);
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    // We never create the topic — irrelevant; the ACL gate fires
    // before the partition lookup, so the per-partition row is
    // CLUSTER_AUTHORIZATION_FAILED regardless of whether the replica
    // exists locally.
    let resp = alter_replica_log_dirs(addr, extra.path(), "t", vec![0, 1]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic in response");
    assert!(topic.partitions.len() == 2);
    for p in &topic.partitions {
        // 31 == CLUSTER_AUTHORIZATION_FAILED
        assert!(
            p.error_code == 31,
            "partition {} must be denied, got {}",
            p.partition_index,
            p.error_code
        );
    }

    handle.shutdown().await;
}
