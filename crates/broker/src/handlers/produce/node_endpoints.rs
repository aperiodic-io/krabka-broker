//! KIP-951 `NodeEndpoints` for the `Produce` response: the address of every
//! node a partition row names in its `CurrentLeader` hint.
//!
//! The hint alone is a node id and an epoch. A Java producer's `Sender` hands
//! the response's `nodeEndpoints()` to `Metadata.updatePartitionLeadership`,
//! and with an empty array it cannot resolve the new leader's address. It logs
//! that the leader node is not known and falls back to a full `Metadata`
//! round-trip, which is exactly the round-trip KIP-951 removes.

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
pub(super) use crate::handlers::node_endpoints::topic_endpoints as produce_node_endpoints;

#[cfg(test)]
mod tests {
    use {
        crate::{
            codes,
            handlers::node_endpoints::test_support::{check_endpoints, hinted_partitions},
        },
        krabka_protocol::owned::produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
        },
    };

    #[test]
    fn one_entry_per_hinted_node_on_the_connection_listener() {
        check_endpoints(
            |image, listener, leader_ids| {
                let response = TopicProduceResponse {
                    name: "orders".to_owned(),
                    partition_responses: hinted_partitions(leader_ids, |hint| {
                        PartitionProduceResponse {
                            index: hint.index,
                            error_code: codes::NOT_LEADER_OR_FOLLOWER,
                            current_leader: LeaderIdAndEpoch {
                                leader_id: hint.leader_id,
                                leader_epoch: 7,
                                ..Default::default()
                            },
                            ..Default::default()
                        }
                    }),
                    ..Default::default()
                };
                super::produce_node_endpoints(image, listener, "INTERNAL", &[response])
            },
            |expected| expected.produce,
        );
    }
}
