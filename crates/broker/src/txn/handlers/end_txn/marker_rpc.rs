//! The remote leg of the marker fan-out: the `WriteTxnMarkers` request that the
//! transaction coordinator sends to a partition leader on another broker, the
//! request it builds from the transaction's partitions, and the per-partition
//! result check it applies to the response.

use std::collections::HashMap;

use krabka_metadata::NodeId;
use krabka_protocol::owned::{
    write_txn_markers_request::{
        WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
    },
    write_txn_markers_response::WriteTxnMarkersResponse,
};

use super::markers::{MarkerDispatchContext, MarkerFanOut};
use crate::{
    error::BrokerError,
    txn::{
        marker::{MarkerCodeClass, MarkerType, classify_marker_code},
        state::{TopicPartition, TxnEntry},
    },
};

/// Send a `WriteTxnMarkersRequest` to a remote broker that leads one or more
/// of the transaction's partitions.
///
/// Dials through the shared
/// [`InterBrokerClient`](crate::network::client::InterBrokerClient) so the connection
/// terminates TLS and runs the SASL client handshake whenever the
/// inter-broker listener demands them. A one-shot
/// `krabka_client_core::Client` per call would carry no TLS
/// connector and no inter-broker credentials. Marker fan-out would then
/// succeed only against a PLAINTEXT inter-broker listener, and it would
/// silently break transactions that span remote-led partitions on any
/// secured cluster.
///
/// ## Coordinator epoch
///
/// The caller resolves the current `__transaction_state` partition leader epoch
/// from the metadata image and stamps it on every marker.
pub(super) async fn send_write_txn_markers(
    context: MarkerDispatchContext<'_>,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
) -> MarkerFanOut {
    match write_remote_markers(context, leader_node, entry, marker_type, tps).await {
        Ok(response) => validate_marker_response(entry, tps, &response),
        Err(error) => MarkerFanOut {
            written: Vec::new(),
            failure: Some(error),
        },
    }
}

/// Dials `leader_node` and sends it the markers for `tps`.
// cargo-mutants: an I/O-only wrapper with no in-process signal. It dials a remote
// broker through the shared `InterBrokerClient` and sends one
// `WriteTxnMarkersRequest`; no test in this process can build the connection, so
// every mutant of the dial-and-send sequence survives unobserved. The request it
// sends and the reply check are mutation-tested on their own.
#[cfg_attr(test, mutants::skip)]
async fn write_remote_markers(
    context: MarkerDispatchContext<'_>,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
) -> Result<WriteTxnMarkersResponse, BrokerError> {
    let MarkerDispatchContext {
        node_id: my_node_id,
        coordinator_epoch,
        image,
        inter_broker_client,
        inter_broker_protocol,
        inter_broker_listener_name,
        inter_broker_server_name,
        ..
    } = context;
    let Some(broker_info) = image.broker(leader_node) else {
        return Err(BrokerError::Txn(format!(
            "EndTxn: leader node {leader_node} not found in metadata image"
        )));
    };

    // Prefer the leader's inter-broker listener endpoint when it has projected
    // one onto its registration record; fall back to the legacy top-level
    // host/port. Mirrors the resolution in the replicator supervisor and
    // heartbeat client — the marker RPC must target the same listener whose
    // protocol we dial with.
    let (host, port) =
        crate::broker::registered_listener_endpoint(broker_info, inter_broker_listener_name);
    let host = host.to_owned();

    let req = build_write_txn_markers_request(entry, marker_type, tps, coordinator_epoch);

    let opts = krabka_client_core::ConnectionOptions {
        client_id: format!("krabka-broker-txn-{my_node_id}"),
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = inter_broker_client
        .connect_as_connection(
            &host,
            port,
            inter_broker_protocol,
            inter_broker_server_name,
            opts,
        )
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: connect to {host}:{port}: {e}")))?;

    // `Connection::send` negotiates the wire version from the broker-advertised
    // ApiVersions table established during connect.
    let resp = conn
        .send(req)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: WriteTxnMarkers to {host}:{port}: {e}")))?;

    conn.close();
    Ok(resp)
}

fn build_write_txn_markers_request(
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
    coordinator_epoch: i32,
) -> WriteTxnMarkersRequest {
    // Group tps by topic for the nested WritableTxnMarkerTopic structure.
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for tp in tps {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition.get());
    }

    let topics: Vec<WritableTxnMarkerTopic> = by_topic
        .into_iter()
        .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
            name,
            partition_indexes,
            ..Default::default()
        })
        .collect();

    WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
            producer_epoch: entry.producer_epoch,
            transaction_result: marker_type == MarkerType::Commit,
            topics,
            coordinator_epoch,
            // Kafka's `TransactionMarkerChannelManager` stamps the
            // transaction's client transaction version, which decides the
            // receiving leader's marker epoch rule. The level is 0 to 3.
            transaction_version: i8::try_from(entry.client_transaction_version).unwrap_or(i8::MAX),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Splits the requested partitions into those that leave the transaction's
/// pending set and the most severe failure, per Kafka's
/// `TransactionMarkerRequestCompletionHandler`. A partition leaves when the
/// leader wrote its marker, and also when the leader answered
/// `UNSUPPORTED_FOR_MESSAGE_FORMAT` or `UNSUPPORTED_VERSION`: the producer
/// could not have written there either, so Kafka calls
/// `TransactionMetadata.removePartition` for it without a marker.
fn validate_marker_response(
    entry: &TxnEntry,
    tps: &[TopicPartition],
    response: &WriteTxnMarkersResponse,
) -> MarkerFanOut {
    let mut outcome = MarkerFanOut::default();
    let Some(marker) = response
        .markers
        .iter()
        .find(|marker| marker.producer_id == entry.producer_id.get())
    else {
        outcome.fail(BrokerError::Txn(format!(
            "WriteTxnMarkers response omitted producer {}",
            entry.producer_id.get()
        )));
        return outcome;
    };
    for tp in tps {
        let result = marker
            .topics
            .iter()
            .find(|topic| topic.name == tp.topic)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_index == tp.partition.get())
            });
        match result {
            None => outcome.fail(BrokerError::Txn(format!(
                "WriteTxnMarkers response omitted {}-{}",
                tp.topic,
                tp.partition.get()
            ))),
            Some(result)
                if matches!(
                    classify_marker_code(result.error_code),
                    MarkerCodeClass::Written | MarkerCodeClass::Dropped
                ) =>
            {
                outcome.written.push(tp.clone());
            }
            Some(result) => outcome.fail(BrokerError::MarkerWriteRefused {
                code: result.error_code,
                message: format!(
                    "WriteTxnMarkers failed for {}-{} with error code {}",
                    tp.topic,
                    tp.partition.get(),
                    result.error_code
                ),
            }),
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord,
    };
    use krabka_protocol::owned::write_txn_markers_response::{
        WritableTxnMarkerPartitionResult, WritableTxnMarkerResult, WritableTxnMarkerTopicResult,
    };
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        codes,
        network::client::InterBrokerClient,
        txn::handlers::end_txn::test_support::{marker_entry, plaintext_client, tps},
    };

    fn marker_response(codes_by_partition: &[(i32, i16)]) -> WriteTxnMarkersResponse {
        WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 7,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "t".to_string(),
                    partitions: codes_by_partition
                        .iter()
                        .map(
                            |&(partition_index, error_code)| WritableTxnMarkerPartitionResult {
                                partition_index,
                                error_code,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn partition(index: i32) -> TopicPartition {
        TopicPartition {
            topic: "t".to_string(),
            partition: krabka_ids::PartitionIndex(index),
        }
    }

    /// How a fan-out attempt failed: `None` when it did not, and the refused
    /// code for a `MarkerWriteRefused`, `-1` for any other error.
    fn failure_code(outcome: &MarkerFanOut) -> Option<i16> {
        outcome.failure.as_ref().map(|error| match error {
            BrokerError::MarkerWriteRefused { code, .. } => *code,
            _ => -1,
        })
    }

    /// #852, #882: every partition the leader acknowledged, or answered with a
    /// code Kafka drops the partition for, is reported as written even when
    /// another partition in the same request is refused, so the coordinator
    /// removes exactly those from the transaction.
    #[test]
    fn marker_response_reports_each_acknowledged_partition() {
        let entry = marker_entry();
        let requested = vec![partition(0), partition(1)];
        // (label, response, partitions written, failure code)
        let cases = [
            (
                "both acknowledged",
                marker_response(&[(0, codes::NONE), (1, codes::NONE)]),
                vec![partition(0), partition(1)],
                None,
            ),
            (
                "one refused",
                marker_response(&[(0, codes::NONE), (1, codes::NOT_LEADER_OR_FOLLOWER)]),
                vec![partition(0)],
                Some(codes::NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "one answered UNSUPPORTED_FOR_MESSAGE_FORMAT",
                marker_response(&[(0, codes::NONE), (1, 43)]),
                vec![partition(0), partition(1)],
                None,
            ),
            (
                "one answered UNSUPPORTED_VERSION",
                marker_response(&[(0, codes::UNSUPPORTED_VERSION), (1, codes::NONE)]),
                vec![partition(0), partition(1)],
                None,
            ),
            (
                "one dropped, one refused",
                marker_response(&[
                    (0, codes::UNSUPPORTED_VERSION),
                    (1, codes::REQUEST_TIMED_OUT),
                ]),
                vec![partition(0)],
                Some(codes::REQUEST_TIMED_OUT),
            ),
            (
                "one answered an unexpected code",
                marker_response(&[(0, codes::NONE), (1, codes::CORRUPT_MESSAGE)]),
                vec![partition(0)],
                Some(codes::CORRUPT_MESSAGE),
            ),
            (
                "one omitted",
                marker_response(&[(1, codes::NONE)]),
                vec![partition(1)],
                Some(-1),
            ),
            (
                "producer omitted",
                WriteTxnMarkersResponse::default(),
                vec![],
                Some(-1),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, response, written, failure) in cases {
            let outcome = validate_marker_response(&entry, &requested, &response);
            actual.push((label, failure_code(&outcome), outcome.written));
            expected.push((label, failure, written));
        }
        assert!(actual == expected);
    }

    #[test]
    fn marker_request_uses_current_coordinator_epoch() {
        let request =
            build_write_txn_markers_request(&marker_entry(), MarkerType::Abort, &tps(), 42);

        assert!(request.markers.len() == 1);
        assert!(request.markers[0].coordinator_epoch == 42);
    }

    /// #876: Kafka's `TransactionMarkerChannelManager` stamps the
    /// transaction's client transaction version on every marker it sends.
    #[test]
    fn marker_request_carries_the_transaction_version() {
        let mut entry = marker_entry();
        entry.producer_epoch = 3;
        entry.client_transaction_version = 2;

        let request = build_write_txn_markers_request(&entry, MarkerType::Commit, &tps(), 5);

        assert!(
            request
                == WriteTxnMarkersRequest {
                    markers: vec![WritableTxnMarker {
                        producer_id: 7,
                        producer_epoch: 3,
                        transaction_result: true,
                        topics: vec![WritableTxnMarkerTopic {
                            name: "t".to_string(),
                            partition_indexes: vec![0],
                            ..Default::default()
                        }],
                        coordinator_epoch: 5,
                        transaction_version: 2,
                        ..Default::default()
                    }],
                    ..Default::default()
                }
        );
    }

    async fn send_test_markers(
        image: &MetadataImage,
        leader: NodeId,
        listener_name: &str,
    ) -> MarkerFanOut {
        let client = plaintext_client();
        let entry = marker_entry();
        let partitions = tps();
        send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: listener_name,
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            leader,
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await
    }

    /// Leader node absent from the metadata image → descriptive `Txn` error,
    /// and no dial.
    #[tokio::test]
    async fn errors_when_leader_node_missing_from_image() {
        let image = MetadataImage::default();
        let err = send_test_markers(&image, NodeId(99), "PLAINTEXT")
            .await
            .failure
            .expect("missing leader must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("not found")),
            "unexpected error: {err:?}"
        );
    }

    /// Leader resolves to its inter-broker endpoint, but the address is
    /// unreachable → the dial fails and the error names the resolved
    /// `host:port` (the endpoint, not the top-level fallback).
    #[tokio::test]
    async fn errors_when_inter_broker_endpoint_unreachable() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                port: 9,
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    // Discard port: refuses connections immediately.
                    port: 9,
                    protocol: ListenerProtocol::Plaintext,
                }],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
            .await
            .failure
            .expect("unreachable endpoint must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "unexpected error: {err:?}"
        );
    }

    /// No endpoint matches the inter-broker listener name → fall back to the
    /// record's top-level `host`/`port`. Still unreachable, so the dial fails
    /// against the fallback address.
    #[tokio::test]
    async fn falls_back_to_top_level_host_port_when_no_matching_endpoint() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                port: 9,
                // Endpoint exists but under a different listener name, so the
                // `find(name == inter_broker_listener_name)` misses.
                endpoints: vec![BrokerEndpoint {
                    name: "SOMETHING_ELSE".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 65000,
                    protocol: ListenerProtocol::Plaintext,
                }],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
            .await
            .failure
            .expect("unreachable fallback must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "expected fallback to top-level 127.0.0.1:9, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn remote_marker_dispatch_dials_with_configured_server_name() {
        use std::sync::Arc;

        use tokio::net::TcpListener;
        use tokio_rustls::{
            LazyConfigAcceptor,
            rustls::{ClientConfig, RootCertStore, server::Acceptor},
        };

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind TLS ClientHello capture listener");
        let port = listener
            .local_addr()
            .expect("capture listener address")
            .port();
        let capture = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept marker dial");
            let handshake = LazyConfigAcceptor::new(Acceptor::default(), stream)
                .await
                .expect("parse marker dial ClientHello");
            handshake.client_hello().server_name().map(str::to_owned)
        });

        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                port,
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    port,
                    protocol: ListenerProtocol::Ssl,
                }],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            },
        ));
        let tls = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let client =
            InterBrokerClient::new(Some(tokio_rustls::TlsConnector::from(Arc::new(tls))), None);
        let entry = marker_entry();
        let partitions = tps();
        let result = send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Ssl,
                inter_broker_listener_name: "INTERNAL",
                inter_broker_server_name: "broker.internal",
                group_coordinator: None,
            },
            NodeId(2),
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await;

        assert!(
            result.failure.is_some(),
            "capture server intentionally stops after ClientHello"
        );
        assert!(
            capture.await.expect("join ClientHello capture").as_deref() == Some("broker.internal")
        );
    }
}
