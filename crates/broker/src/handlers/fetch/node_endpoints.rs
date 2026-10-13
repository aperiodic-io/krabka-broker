//! KIP-951 `NodeEndpoints` for the `Fetch` response: the address of every node
//! a partition row names in its `CurrentLeader` hint.
//!
//! A Java consumer feeds the array to `Metadata.updatePartitionLeadership`
//! alongside the hint. Without it the hint names a node whose address the
//! client cannot resolve, and the consumer falls back to the full `Metadata`
//! round-trip that KIP-951 exists to remove.

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
pub(super) use crate::handlers::node_endpoints::topic_endpoints as fetch_node_endpoints;

#[cfg(test)]
mod tests {
    use {
        crate::{
            codes,
            handlers::node_endpoints::test_support::{check_endpoints, hinted_partitions},
        },
        krabka_protocol::owned::fetch_response::{
            FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
        },
    };

    #[test]
    fn one_entry_per_hinted_node_on_the_connection_listener() {
        check_endpoints(
            |image, listener, leader_ids| {
                let response = FetchableTopicResponse {
                    topic: "orders".to_owned(),
                    partitions: hinted_partitions(leader_ids, |hint| PartitionData {
                        partition_index: hint.index,
                        error_code: codes::NOT_LEADER_OR_FOLLOWER,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: hint.leader_id,
                            leader_epoch: 7,
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                super::fetch_node_endpoints(image, listener, "INTERNAL", &[response])
            },
            |expected| expected.fetch,
        );
    }
}
