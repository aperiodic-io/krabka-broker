//! KIP-951 `NodeEndpoints` for the `Fetch` response: the address of every node
//! a partition row names in its `CurrentLeader` hint.
//!
//! A Java consumer feeds the array to `Metadata.updatePartitionLeadership`
//! alongside the hint. Without it the hint names a node whose address the
//! client cannot resolve, and the consumer falls back to the full `Metadata`
//! round-trip that KIP-951 exists to remove.

use krabka_protocol::owned::fetch_response::{FetchableTopicResponse, NodeEndpoint};

use crate::handlers::leader_endpoints;

/// The endpoint of every node named by a `CurrentLeader` hint in `responses`,
/// one entry per node id and ordered by it.
///
/// `connection_listener_name` is the listener the request arrived on. Kafka
/// answers with the advertised address of that listener, so a consumer on the
/// external listener gets the external `host:port`, and this resolves each
/// node through the same [`crate::handlers::metadata::pick_endpoint_host_port`]
/// selection that `Metadata` and `DescribeCluster` project a broker with.
///
/// A node the image does not know contributes no entry: there is no address to
/// advertise for it, and Kafka likewise skips a leader that its metadata cache
/// cannot resolve to a live node.
pub(super) fn fetch_node_endpoints(
    image: &krabka_metadata::MetadataImage,
    connection_listener_name: &str,
    inter_broker_listener_name: &str,
    responses: &[FetchableTopicResponse],
) -> Vec<NodeEndpoint> {
    leader_endpoints(
        image,
        connection_listener_name,
        inter_broker_listener_name,
        responses
            .iter()
            .flat_map(|topic| topic.partitions.iter())
            .map(|partition| partition.current_leader.leader_id),
    )
}

#[cfg(test)]
crate::handlers::node_endpoints::test_support::endpoint_tests!(
    fetch_response,
    fetch_node_endpoints,
    FetchableTopicResponse,
    topic,
    partitions,
    PartitionData,
    partition_index
);
