//! The ACL gates on `AddOffsetsToTxn` (#679) and `WriteTxnMarkers` (#680).
//!
//! Every request goes through the dispatch registry, so a test also fails when
//! one of the apis goes back to a dispatch kind that gets no principal.

use std::sync::Arc;

use assert2::check;
use krabka_log::Offset;
use krabka_protocol::owned::{
    add_offsets_to_txn_request::{self, AddOffsetsToTxnRequest},
    add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    write_txn_markers_request::{
        self, WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
    },
    write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
        WriteTxnMarkersResponse,
    },
};

use crate::{
    codes,
    test_support::{
        GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer, principal,
        request_context, start_broker_no_audit_with,
    },
};

/// Kafka's `KafkaApis.handleAddOffsetsToTxnRequest` checks `Write` on the
/// transactional id, then `Read` on the group. The first denial is the
/// answer, and the transaction coordinator does not see the request.
#[tokio::test]
async fn add_offsets_to_txn_checks_transactional_id_write_then_group_read() {
    let (handle, _dir) =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(GrantsInPrincipalName)).await;
    let broker = handle.broker_arc_for_test();

    // The transactional id is not known to the coordinator, so a request
    // that passes both gates gets the coordinator's answer for it.
    let coordinator_answer = codes::NOT_COORDINATOR;
    let cases = [
        ("none", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        ("Group:Read", codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        ("TransactionalId:Write", codes::GROUP_AUTHORIZATION_FAILED),
        ("TransactionalId:Write+Group:Read", coordinator_answer),
    ];

    let address = peer();
    for version in [
        add_offsets_to_txn_request::MIN_VERSION,
        add_offsets_to_txn_request::MAX_VERSION,
    ] {
        for (grants, error_code) in cases {
            let user = principal(grants);
            let ctx = request_context(&user, &address, "add-offsets-authorization");
            let request = AddOffsetsToTxnRequest {
                transactional_id: "unknown-transactional-id".to_string(),
                producer_id: 7,
                producer_epoch: 0,
                group_id: "offsets-group".to_string(),
                ..Default::default()
            };
            let bytes = dispatch_context(
                &broker,
                add_offsets_to_txn_request::API_KEY,
                version,
                &encode_request(&request, version),
                &ctx,
            )
            .await;
            check!(
                decode_response::<AddOffsetsToTxnResponse>(&bytes, version)
                    == AddOffsetsToTxnResponse {
                        error_code,
                        ..Default::default()
                    },
                "v{version} {grants}"
            );
        }
    }
    handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleWriteTxnMarkersRequest` allows a principal that
/// holds `Alter` or `ClusterAction` on the cluster. Any other principal gets
/// `CLUSTER_AUTHORIZATION_FAILED` on every requested partition, and no marker
/// reaches the log.
#[tokio::test]
async fn write_txn_markers_needs_cluster_alter_or_cluster_action() {
    const TOPIC: &str = "orders";

    let (handle, dir) =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(GrantsInPrincipalName)).await;
    let broker = handle.broker_arc_for_test();
    let local = super::write_txn_markers::test_support::open_partition(
        &broker,
        dir.path(),
        crate::test_support::StandalonePartitionSetup {
            topic: TOPIC,
            partition: krabka_ids::PartitionIndex(1),
            ..Default::default()
        },
    );
    // A marker appends only to a partition this broker leads.
    local
        .install_replication_target(None, broker.config.node_id.0, 0)
        .await;

    let request = |producer_id| WriteTxnMarkersRequest {
        markers: vec![
            WritableTxnMarker {
                producer_id,
                producer_epoch: 0,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: TOPIC.to_string(),
                    partition_indexes: vec![1, 2],
                    ..Default::default()
                }],
                ..Default::default()
            },
            WritableTxnMarker {
                producer_id: producer_id + 1,
                producer_epoch: 0,
                transaction_result: false,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: "elsewhere".to_string(),
                    partition_indexes: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let response = |producer_id, local_row: i16, remote_row: i16| WriteTxnMarkersResponse {
        markers: vec![
            WritableTxnMarkerResult {
                producer_id,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: TOPIC.to_string(),
                    partitions: vec![
                        WritableTxnMarkerPartitionResult {
                            partition_index: 1,
                            error_code: local_row,
                            ..Default::default()
                        },
                        WritableTxnMarkerPartitionResult {
                            partition_index: 2,
                            error_code: remote_row,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
            WritableTxnMarkerResult {
                producer_id: producer_id + 1,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "elsewhere".to_string(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: remote_row,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let refused = codes::CLUSTER_AUTHORIZATION_FAILED;
    // (grants, the local row, the rows of partitions this broker does not
    // host, whether one marker reaches the local log)
    let cases = [
        ("none", refused, refused, false),
        ("Topic:Write+TransactionalId:Write", refused, refused, false),
        (
            "Cluster:Alter",
            codes::NONE,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            true,
        ),
        (
            "Cluster:ClusterAction",
            codes::NONE,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            true,
        ),
    ];

    let address = peer();
    let version = write_txn_markers_request::MAX_VERSION;
    for (case, (grants, local_row, remote_row, written)) in cases.into_iter().enumerate() {
        let user = principal(grants);
        let ctx = request_context(&user, &address, "write-txn-markers-authorization");
        let producer_id = 100 + i64::try_from(case).expect("case index") * 10;
        let before = local.log_end_offset();
        let bytes = dispatch_context(
            &broker,
            write_txn_markers_request::API_KEY,
            version,
            &encode_request(&request(producer_id), version),
            &ctx,
        )
        .await;
        check!(
            decode_response::<WriteTxnMarkersResponse>(&bytes, version)
                == response(producer_id, local_row, remote_row),
            "{grants}"
        );
        let expected_end = if written {
            Offset(before.0 + 1)
        } else {
            before
        };
        check!(local.log_end_offset() == expected_end, "{grants}");
    }
    handle.shutdown().await;
}
