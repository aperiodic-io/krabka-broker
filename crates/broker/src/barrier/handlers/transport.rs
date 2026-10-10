//! The sending side of `WriteBarrierMarkers`, api key 1014.
//!
//! [`InterBrokerMarkerWriter`] is the leg of a marker fan-out that leaves this
//! broker. The coordinator groups the pending targets of an epoch by their
//! current leader, and it hands each remote group to this writer. The writer
//! sends one request to that leader and returns the offset of every marker the
//! leader placed.
//!
//! [`write_markers`][crate::barrier::handlers::write_markers] is the receiving
//! side.
//!
//! # The dialer
//!
//! The writer dials through the shared
//! [`InterBrokerClient`][crate::network::client::InterBrokerClient], so the
//! connection terminates TLS and runs the SASL client handshake whenever the
//! inter-broker listener needs them. A one-shot client per call would carry no
//! TLS connector and no inter-broker credentials, and the fan-out would then
//! work only against a PLAINTEXT inter-broker listener.
//!
//! The endpoint comes from the leader's registration record. The writer takes
//! the endpoint whose name matches the inter-broker listener, and it falls back
//! to the top-level host and port of the record. `EndTxn` resolves the marker
//! endpoint the same way.
//!
//! # Failure
//!
//! A failed connection, a failed request, and a per-partition error code all
//! leave the target out of the returned placements. The fan-out keeps that
//! target pending and retries it until the injection deadline runs out.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use krabka_ids::{NodeId, PartitionIndex};
use krabka_log::Offset;
use krabka_metadata::{BrokerRegistrationRecord, MetadataImage};
use krabka_protocol::krabka::barrier::{
    WritableBarrierPartition, WritableBarrierTopic, WriteBarrierMarkersRequest,
    WriteBarrierMarkersResponse,
};
use krabka_security::ListenerProtocol;
use tracing::warn;

use crate::{
    barrier::{
        injection::{MarkerPlacement, RemoteMarkerWriter},
        marker::BarrierMarker,
        state::TargetPartition,
    },
    codes,
    error::BrokerError,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
};

/// A [`RemoteMarkerWriter`] over the inter-broker listener.
pub(crate) struct InterBrokerMarkerWriter {
    node_id: NodeId,
    controller: Arc<dyn MetadataSource>,
    client: Arc<InterBrokerClient>,
    listener_protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

impl InterBrokerMarkerWriter {
    pub(crate) fn new(
        node_id: NodeId,
        controller: Arc<dyn MetadataSource>,
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
    ) -> Self {
        Self {
            node_id,
            controller,
            client,
            listener_protocol,
            listener_name,
            server_name,
        }
    }
}

#[async_trait]
impl RemoteMarkerWriter for InterBrokerMarkerWriter {
    async fn write_markers(
        &self,
        leader: NodeId,
        marker: &BarrierMarker,
        targets: &[TargetPartition],
    ) -> Result<Vec<MarkerPlacement>, BrokerError> {
        let image = self.controller.current_image();
        let Some(broker_info) = image.broker(leader) else {
            return Err(BrokerError::Replication(format!(
                "WriteBarrierMarkers: leader node {leader} is not in the metadata image"
            )));
        };
        let (host, port) = endpoint_of(broker_info, &self.listener_name);

        let options = krabka_client_core::ConnectionOptions {
            client_id: format!("krabka-broker-barrier-{}", self.node_id),
            ..krabka_client_core::ConnectionOptions::default()
        };
        let connection = self
            .client
            .connect_as_connection(
                &host,
                port,
                self.listener_protocol,
                &self.server_name,
                options,
            )
            .await
            .map_err(|error| {
                BrokerError::Replication(format!(
                    "WriteBarrierMarkers: connect to {host}:{port}: {error}"
                ))
            })?;

        // `Connection::send` negotiates the version from the table that the
        // peer advertised at connect. A krabka-private api key is in no such
        // table, and the negotiation then settles on version 0, which is the
        // only version this message has.
        let response = connection
            .send(build_request(marker, targets, &image))
            .await
            .map_err(|error| {
                BrokerError::Replication(format!(
                    "WriteBarrierMarkers: request to {host}:{port}: {error}"
                ))
            });
        connection.close();

        Ok(placements(&response?))
    }
}

/// The inter-broker host and port of a leader.
///
/// The named listener wins. A registration record that projects no such
/// endpoint falls back to its top-level host and port.
fn endpoint_of(broker_info: &BrokerRegistrationRecord, listener_name: &str) -> (String, u16) {
    broker_info
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == listener_name)
        .map_or_else(
            || (broker_info.host.clone(), broker_info.port),
            |endpoint| (endpoint.host.clone(), endpoint.port),
        )
}

/// The malformed epoch used when this image has no partition record. The
/// receiver fences it, and the next fan-out attempt reads a fresh image.
const NO_EXPECTED_LEADER_EPOCH: i32 = -1;

/// The request that asks one leader to mark every target it leads.
/// `image` supplies the leader epoch of each target. The receiving broker
/// refuses a partition whose epoch has moved past it, so a request built
/// against a stale view cannot write a false epoch into a marker header.
fn build_request(
    marker: &BarrierMarker,
    targets: &[TargetPartition],
    image: &MetadataImage,
) -> WriteBarrierMarkersRequest {
    let mut by_topic: BTreeMap<&str, Vec<WritableBarrierPartition>> = BTreeMap::new();
    for target in targets {
        by_topic
            .entry(target.topic.as_str())
            .or_default()
            .push(WritableBarrierPartition {
                partition: target.partition.get(),
                // The partition is absent from this image, so this broker has
                // no generation to assert. The receiver fences -1.
                expected_leader_epoch: image
                    .partition(&target.topic, target.partition.get())
                    .map_or(NO_EXPECTED_LEADER_EPOCH, |record| record.leader_epoch.get()),
                ..WritableBarrierPartition::default()
            });
    }
    WriteBarrierMarkersRequest {
        group: marker.group.clone(),
        epoch: marker.epoch,
        triggered_at: marker.triggered_at,
        topics: by_topic
            .into_iter()
            .map(|(topic, partitions)| WritableBarrierTopic {
                topic: topic.to_owned(),
                partitions,
                ..WritableBarrierTopic::default()
            })
            .collect(),
        ..WriteBarrierMarkersRequest::default()
    }
}

/// The markers that the leader placed.
///
/// A partition row with a non-zero error code carries no offset, so it stays
/// out of the result and the fan-out retries it.
fn placements(response: &WriteBarrierMarkersResponse) -> Vec<MarkerPlacement> {
    let mut out = Vec::new();
    for topic in &response.topics {
        for partition in &topic.partitions {
            if partition.error_code == codes::NONE {
                out.push(MarkerPlacement {
                    target: TargetPartition {
                        topic: topic.topic.clone(),
                        partition: PartitionIndex(partition.partition),
                    },
                    offset: Offset(partition.offset),
                });
            } else {
                warn!(
                    topic = topic.topic,
                    partition = partition.partition,
                    error_code = partition.error_code,
                    "the leader refused a barrier marker"
                );
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {

    use assert2::check;
    use krabka_ids::PartitionIndex;
    use krabka_log::Offset;
    use krabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataImage};
    use krabka_protocol::krabka::barrier::{
        WritableBarrierPartition, WritableBarrierTopic, WriteBarrierMarkersRequest,
        WriteBarrierMarkersResponse, WrittenBarrierPartition, WrittenBarrierTopic,
    };
    use krabka_security::ListenerProtocol;

    use super::{build_request, endpoint_of, placements};
    use crate::{
        barrier::{injection::MarkerPlacement, marker::BarrierMarker, state::TargetPartition},
        codes,
    };

    fn marker() -> BarrierMarker {
        BarrierMarker {
            group: "orders-cut".to_owned(),
            epoch: 4,
            triggered_at: 1_724_500_000_000,
        }
    }

    fn at(topic: &str, partition: i32) -> TargetPartition {
        TargetPartition {
            topic: topic.to_owned(),
            partition: PartitionIndex(partition),
        }
    }

    fn registration(endpoints: Vec<BrokerEndpoint>) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            host: "legacy.example".to_owned(),
            endpoints,
            ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
        }
    }

    fn endpoint(name: &str, host: &str, port: u16) -> BrokerEndpoint {
        BrokerEndpoint {
            name: name.to_owned(),
            host: host.to_owned(),
            port,
            protocol: ListenerProtocol::Plaintext,
        }
    }

    /// One requested partition, carrying the malformed missing-image epoch.
    fn part(partition: i32) -> WritableBarrierPartition {
        WritableBarrierPartition {
            partition,
            ..WritableBarrierPartition::default()
        }
    }

    #[test]
    fn one_request_groups_every_target_under_its_topic() {
        let targets = vec![
            at("orders", 2),
            at("payments", 0),
            at("orders", 0),
            at("orders", 1),
        ];
        let expected = WriteBarrierMarkersRequest {
            group: "orders-cut".to_owned(),
            epoch: 4,
            triggered_at: 1_724_500_000_000,
            topics: vec![
                WritableBarrierTopic {
                    topic: "orders".to_owned(),
                    partitions: vec![part(2), part(0), part(1)],
                    ..WritableBarrierTopic::default()
                },
                WritableBarrierTopic {
                    topic: "payments".to_owned(),
                    partitions: vec![part(0)],
                    ..WritableBarrierTopic::default()
                },
            ],
            ..WriteBarrierMarkersRequest::default()
        };
        // No partition is in this image, so every row carries -1 and the
        // receiver will fence it.
        let image = MetadataImage::default();
        check!(build_request(&marker(), &targets, &image) == expected);
    }

    #[test]
    fn a_response_returns_the_offset_of_every_marker_the_leader_placed() {
        let response = WriteBarrierMarkersResponse {
            topics: vec![WrittenBarrierTopic {
                topic: "orders".to_owned(),
                partitions: vec![
                    WrittenBarrierPartition {
                        partition: 0,
                        error_code: codes::NONE,
                        offset: 77,
                        ..WrittenBarrierPartition::default()
                    },
                    WrittenBarrierPartition {
                        partition: 1,
                        error_code: codes::NOT_LEADER_OR_FOLLOWER,
                        offset: -1,
                        ..WrittenBarrierPartition::default()
                    },
                    WrittenBarrierPartition {
                        partition: 2,
                        error_code: codes::FENCED_LEADER_EPOCH,
                        offset: -1,
                        ..WrittenBarrierPartition::default()
                    },
                ],
                ..WrittenBarrierTopic::default()
            }],
            ..WriteBarrierMarkersResponse::default()
        };
        let expected = vec![MarkerPlacement {
            target: at("orders", 0),
            offset: Offset(77),
        }];
        check!(placements(&response) == expected);
    }

    #[test]
    fn an_empty_response_places_no_marker() {
        check!(placements(&WriteBarrierMarkersResponse::default()) == Vec::new());
    }

    #[test]
    fn the_endpoint_of_a_leader_prefers_the_inter_broker_listener() {
        let record = registration(vec![
            endpoint("PLAINTEXT", "public.example", 9092),
            endpoint("INTERNAL", "internal.example", 9093),
        ]);
        check!(endpoint_of(&record, "INTERNAL") == ("internal.example".to_owned(), 9093));
    }

    #[test]
    fn a_leader_with_no_such_listener_falls_back_to_the_top_level_address() {
        let cases: &[(Vec<BrokerEndpoint>, &str)] = &[
            (Vec::new(), "INTERNAL"),
            (vec![endpoint("PLAINTEXT", "public.example", 9092)], "SSL"),
        ];
        for (endpoints, listener) in cases {
            let record = registration(endpoints.clone());
            check!(
                endpoint_of(&record, listener) == ("legacy.example".to_owned(), 9092),
                "{listener}"
            );
        }
    }
}
