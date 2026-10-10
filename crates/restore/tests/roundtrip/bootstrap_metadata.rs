//! The metadata a restore writes beside the partition data: the cluster id in
//! `meta.properties`, and the topics and partitions the archive held in
//! `bootstrap.records.bin`.
//!
//! Reading those records back needs `wincode`, which this crate cannot name,
//! so the checks here are the count of records the seeding added and the raw
//! bytes of the archived topic ids -- a long enough argument to keep the
//! metadata claim in a file of its own.

use std::path::Path;

use assert2::{assert, check};
use krabka_broker::BrokerConfig;
use krabka_metadata::{
    AclEntry, AclOperation, MetadataImage, MetadataRecord, NodeId, PatternType, PermissionType,
    ResourceType, TopicConfigRecord, TopicRecord,
};
use krabka_protocol::owned::describe_acls_request::DescribeAclsRequest;
use uuid::Uuid;

use crate::fixture::{Fixture, build_fixture};

krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// 3. Read back the restored metadata: `meta.properties` names the cluster id
/// `format_target` chose and the target node, and `bootstrap.records.bin` carries the
/// archive's topics and partitions -- with the SAME topic id the archive
/// used, not a freshly generated one.
#[tokio::test]
async fn restored_bootstrap_metadata_carries_the_archived_topic_ids_and_partition_counts() {
    let fixture = build_fixture();
    let restored = crate::restored::fresh_restore(fixture.archive_root.path()).await;
    check!(restored.report.cluster_id == restored.cluster_id);

    let meta = krabka_format::MetaProperties::read(&restored.log_dir)
        .expect("meta.properties reads")
        .expect("the restore formats the target");
    check!(meta.directory_id.is_some());
    check!(
        krabka_format::MetaProperties {
            directory_id: None,
            ..meta
        } == krabka_format::MetaProperties {
            cluster_id: krabka_format::ClusterId(restored.cluster_id),
            node_id: 1,
            directory_id: None,
        }
    );

    // `bootstrap.records.bin` is a length-prefixed stream of
    // `serde_wincode::SerdeCompat<MetadataRecord>` payloads (see
    // `crates/format/src/format.rs` and
    // `crates/format/tests/seeded_records.rs`), and `wincode`/`serde-wincode`
    // are dependencies of `krabka-format` alone, not of `krabka-restore` --
    // this crate (and so this test, which may only touch this one file) has
    // no `use` path to either crate. Record survival is checked two ways
    // instead: the record *count* the restore's seeded topics and
    // partitions must have added, against a baseline format with the same
    // target flags and no seeding; and the archived topic id's raw bytes,
    // which a `Uuid` serializes to verbatim (as a 16-byte tuple, with no
    // framing of its own, per the `uuid` crate's `Serialize` impl for a
    // non-self-describing format) in any binary serde format, wincode
    // included.
    let baseline = tempfile::tempdir().expect("baseline tempdir");
    let baseline_dir = baseline.path().join("formatted");
    let baseline_code = krabka_format::run_from_args([
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        baseline_dir.display().to_string(),
        "--cluster-id".to_owned(),
        Uuid::new_v4().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        "127.0.0.1:9093".to_owned(),
    ])
    .await;
    check!(baseline_code == 0);

    let record_count = |dir: &Path| -> u64 {
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.join("bootstrap.json")).expect("bootstrap.json"),
        )
        .expect("bootstrap.json is JSON");
        manifest["record_count"]
            .as_u64()
            .expect("record_count is a JSON number")
    };
    let extra_records =
        u64::try_from(Fixture::topic_count() + fixture.partitions().len()).expect("small count");
    check!(record_count(&restored.log_dir) == record_count(&baseline_dir) + extra_records);

    let bin = std::fs::read(restored.log_dir.join("bootstrap.records.bin"))
        .expect("bootstrap.records.bin");
    for topic_id in [fixture.orders_id, fixture.payments_id] {
        let topic_id_bytes: [u8; 16] = topic_id.into_bytes();
        check!(
            bin.windows(16).any(|window| window == topic_id_bytes),
            "topic id {topic_id} not found in bootstrap.records.bin"
        );
    }
    for topic in ["orders", "payments"] {
        check!(
            bin.windows(topic.len())
                .any(|window| window == topic.as_bytes()),
            "topic name {topic:?} not found in bootstrap.records.bin"
        );
    }
}

/// Restored metadata must reach the broker's public admin APIs, not merely
/// survive as bytes in the bootstrap file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_snapshot_reaches_describe_configs_and_describe_acls() {
    const ORDERS: &str = "orders";
    let fixture = build_fixture();
    let mut image = MetadataImage::new(Uuid::new_v4());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: ORDERS.to_owned(),
        topic_id: fixture.orders_id,
        partitions: 2,
        replication_factor: 1,
    }));
    for partition in 0..2 {
        image.apply(&MetadataRecord::V1Partition(single_replica_partition(
            ORDERS,
            krabka_ids::PartitionIndex(partition),
            NodeId(1),
        )));
    }
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: ORDERS.to_owned(),
        overrides: maplit::btreemap! {"cleanup.policy".to_owned() => "compact".to_owned()},
    }));
    image.apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: ORDERS.to_owned(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".to_owned(),
        host: "*".to_owned(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }));

    let snapshot_dir = tempfile::tempdir().expect("snapshot tempdir");
    let snapshot_path = snapshot_dir
        .path()
        .join("00000000000000000042-0000000001.checkpoint");
    std::fs::write(
        &snapshot_path,
        krabka_raft::serialize_metadata_snapshot(&image, 1_700_000_000_000)
            .expect("serialize metadata snapshot"),
    )
    .expect("write metadata snapshot");

    let (_target, log_dir, mut controller, report) = crate::restored::restore_for_controller(
        fixture.archive_root.path(),
        &["--metadata-snapshot", &snapshot_path.display().to_string()],
    )
    .await;
    check!(report.metadata.topic_configs == 1);
    check!(report.metadata.access_control_entries == 1);

    // `DescribeAcls` answers `SECURITY_DISABLED` on a cluster with no
    // authorizer, as Kafka does, so the restored bindings are only reachable
    // through a broker that has one. The plaintext client arrives as
    // `ANONYMOUS`, which the authorizer's own super-user set lets past the
    // cluster-describe gate in front of the listing.
    let mut config = BrokerConfig::for_tests(log_dir);
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        std::iter::once("ANONYMOUS".to_owned()).collect(),
    ));
    let broker = controller.start(config).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("restore-metadata-test")
        .build()
        .await
        .expect("client");

    crate::topic_configuration::check_topic_configuration(
        &client,
        ORDERS,
        "cleanup.policy",
        "compact",
    )
    .await;

    let acls = client
        .send(DescribeAclsRequest {
            resource_type_filter: 2,
            resource_name_filter: Some(ORDERS.to_owned()),
            pattern_type_filter: 3,
            principal_filter: None,
            host_filter: None,
            operation: 1,
            permission_type: 1,
            ..Default::default()
        })
        .await
        .expect("DescribeAcls");
    assert!(acls.error_code == 0, "DescribeAcls failed: {acls:?}");
    check!(acls.resources.iter().any(|resource| {
        resource.resource_name == ORDERS
            && resource
                .acls
                .iter()
                .any(|acl| acl.principal == "User:alice")
    }));

    broker.shutdown().await;
}
