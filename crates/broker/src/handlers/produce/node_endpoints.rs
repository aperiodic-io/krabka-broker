//! KIP-951 `NodeEndpoints` for the `Produce` response: the address of every
//! node a partition row names in its `CurrentLeader` hint.
//!
//! The hint alone is a node id and an epoch. A Java producer's `Sender` hands
//! the response's `nodeEndpoints()` to `Metadata.updatePartitionLeadership`,
//! and with an empty array it cannot resolve the new leader's address. It logs
//! that the leader node is not known and falls back to a full `Metadata`
//! round-trip, which is exactly the round-trip KIP-951 removes.

use {
    crate::handlers::leader_endpoints,
    krabka_protocol::owned::produce_response::{NodeEndpoint, TopicProduceResponse},
};

/// The endpoint of every node named by a `CurrentLeader` hint in `topics`, one
/// entry per node id and ordered by it.
///
/// `connection_listener_name` is the listener the request arrived on. Kafka
/// answers with the advertised address of that listener, so a client on the
/// external listener gets the external `host:port`, and this resolves each
/// node through the same [`crate::handlers::metadata::pick_endpoint_host_port`]
/// selection that `Metadata` and `DescribeCluster` project a broker with.
///
/// A node the image does not know contributes no entry: there is no address to
/// advertise for it, and Kafka likewise skips a leader that its metadata cache
/// cannot resolve to a live node.
pub(super) fn produce_node_endpoints(
    image: &krabka_metadata::MetadataImage,
    connection_listener_name: &str,
    inter_broker_listener_name: &str,
    topics: &[TopicProduceResponse],
) -> Vec<NodeEndpoint> {
    leader_endpoints(
        image,
        connection_listener_name,
        inter_broker_listener_name,
        topics
            .iter()
            .flat_map(|topic| topic.partition_responses.iter())
            .map(|partition| partition.current_leader.leader_id),
    )
}

#[cfg(test)]
crate::handlers::node_endpoints::test_support::endpoint_tests!(
    produce_response,
    produce_node_endpoints,
    TopicProduceResponse,
    name,
    partition_responses,
    PartitionProduceResponse,
    index
);
