//! KIP-951 endpoint projection shared by Fetch, Produce and the share RPCs.

use {crate::handlers::metadata::pick_endpoint_host_port, krabka_metadata::MetadataImage};

#[cfg(test)]
pub(crate) mod test_support;

/// One `NodeEndpoints` entry, in the shape the four responses share.
#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::NodeEndpoints)]
#[node_endpoints(targets(
    krabka_protocol::owned::fetch_response::NodeEndpoint,
    krabka_protocol::owned::produce_response::NodeEndpoint,
    krabka_protocol::owned::share_acknowledge_response::NodeEndpoint,
    krabka_protocol::owned::share_fetch_response::NodeEndpoint,
))]
pub(crate) struct LeaderEndpoint {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}

/// The endpoint of every node in `leader_ids`, once per node and ordered by
/// node id.
///
/// Each node resolves on `connection_listener_name`, the listener the request
/// arrived on, through the same selection that `Metadata` uses. A negative id
/// and a node that the image does not know contribute nothing, as Kafka's
/// `getAliveBrokerNode` finds no node for them.
#[must_use]
pub(crate) fn leader_endpoints<R: From<LeaderEndpoint>>(
    image: &MetadataImage,
    connection_listener_name: &str,
    inter_broker_listener_name: &str,
    leader_ids: impl IntoIterator<Item = i32>,
) -> Vec<R> {
    let mut node_ids: Vec<i32> = leader_ids.into_iter().filter(|id| *id >= 0).collect();
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
        .into_iter()
        .filter_map(|node_id| {
            let record = image.broker(krabka_metadata::NodeId(u64::try_from(node_id).ok()?))?;
            let (host, port) = pick_endpoint_host_port(
                record,
                connection_listener_name,
                inter_broker_listener_name,
            );
            Some(
                LeaderEndpoint {
                    node_id,
                    host,
                    port,
                    rack: record.rack.clone(),
                }
                .into(),
            )
        })
        .collect()
}
