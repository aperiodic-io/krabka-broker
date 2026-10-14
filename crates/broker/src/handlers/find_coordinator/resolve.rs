//! Turning a coordinator key into the broker that owns it.
//!
//! Every key type reduces to the same question: which partition of the state
//! topic holds this key, and which broker leads that partition. This module
//! holds that lookup, the `Coordinator` rows it produces for a resolved and an
//! unavailable partition, and the parser for the share-coordinator's composite
//! key.

use {
    crate::{broker::Broker, codes, host_port::parse_advertised_host_port, txn::bootstrap::TOPIC},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_protocol::owned::find_coordinator_response::Coordinator,
};

/// What a coordinator lookup resolves against: one metadata image, the brokers
/// that are fenced or dead in it, and the listener the request arrived on.
pub(super) struct ResolveTarget<'a> {
    pub(super) image: &'a krabka_metadata::MetadataImage,
    /// From [`crate::handlers::offline_replicas::unavailable_brokers`].
    pub(super) unavailable: &'a std::collections::HashSet<u64>,
    /// This broker's node id.
    pub(super) local_node: krabka_metadata::NodeId,
    /// This broker's advertised `host:port` on the request's listener.
    pub(super) advertised: &'a str,
    /// The name of the listener the request arrived on.
    pub(super) listener: &'a str,
}

pub(super) fn resolve_transaction_keys(
    broker: &Broker,
    target: &ResolveTarget<'_>,
    keys: Vec<String>,
) -> Vec<Coordinator> {
    keys.into_iter()
        .map(|key| {
            let partition = broker.txn_coordinator.partition_for(&key).get();
            resolve_partition_coordinator(target, TOPIC, partition, key)
        })
        .collect()
}

/// The leader of `state_topic`'s `partition` as a `Coordinator` row for `key`.
///
/// Kafka's `getCoordinator` answers with
/// `metadataCache.getAliveBrokerNode(leaderId, listenerName)`: the leader must
/// be registered, unfenced, and have an endpoint for the request's listener.
/// Anything else is `COORDINATOR_NOT_AVAILABLE` with `Node.noNode()`. The
/// local broker answers with its own advertised address for the listener, the
/// one it registered.
pub(super) fn resolve_partition_coordinator(
    target: &ResolveTarget<'_>,
    state_topic: &str,
    partition: i32,
    key: String,
) -> Coordinator {
    let Some(record) = target.image.partition(state_topic, partition) else {
        return unavailable_coordinator(key);
    };
    let leader = record.leader;
    let Some(registration) = target.image.broker(leader) else {
        return unavailable_coordinator(key);
    };
    if target.unavailable.contains(&leader.0) {
        return unavailable_coordinator(key);
    }
    let (host, port) = if leader == target.local_node {
        let (host, port) = parse_advertised_host_port(target.advertised);
        (host, i32::from(port))
    } else {
        let Some(endpoint) = registration
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == target.listener)
        else {
            return unavailable_coordinator(key);
        };
        (endpoint.host.clone(), i32::from(endpoint.port))
    };
    Coordinator {
        key,
        node_id: wire_node_id(leader),
        host,
        port,
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    }
}

/// The `nodeId` a coordinator's [`NodeId`](krabka_metadata::NodeId) projects to
/// on the wire.
///
/// `FindCoordinatorResponse.nodeId` is an int32, and so is the `nodeId` every
/// registration path validates, so the conversion cannot fail for a broker the
/// controller has registered as a partition leader. `-1` is the sentinel that
/// pairs with `COORDINATOR_NOT_AVAILABLE` if one ever did.
// cargo-mutants: an unobservable sentinel. No registration this broker can hold
// carries a node id above `i32::MAX`, so nothing a test constructs reaches the
// `-1` arm and no mutation of it changes an observable byte. Only the fallback
// is skipped: `resolve_partition_coordinator` and the handler above it stay in
// the sweep, because every other branch they take is wire-visible.
#[cfg_attr(test, mutants::skip)]
fn wire_node_id(leader: krabka_metadata::NodeId) -> i32 { i32::try_from(leader.0).unwrap_or(-1) }

pub(super) fn unavailable_coordinator(key: String) -> Coordinator {
    super::response::no_node_row(key, codes::COORDINATOR_NOT_AVAILABLE)
}

/// Parse a share-coordinator key `"{group}:{topicId}:{partition}"` into its
/// `(group, topic_id, partition)` parts.
///
/// A group id can itself contain `:`, so the function reads the partition and
/// the topic-id from the right. It returns `None` for a malformed partition int,
/// a malformed topic-id UUID, or missing segments.
pub(super) fn parse_share_key(key: &str) -> Option<(&str, uuid::Uuid, i32)> {
    let (rest, partition_str) = key.rsplit_once(':')?;
    let (group, topic_str) = rest.rsplit_once(':')?;
    let partition: i32 = partition_str.parse().ok()?;
    if group.trim().is_empty() || partition < 0 || topic_str.len() != 22 {
        return None;
    }
    let topic_bytes: [u8; 16] = URL_SAFE_NO_PAD.decode(topic_str).ok()?.try_into().ok()?;
    if URL_SAFE_NO_PAD.encode(topic_bytes) != topic_str {
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(topic_bytes);
    Some((group, topic_id, partition))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    const TOPIC_ID: &str = "BQUFBQUFBQUFBQUFBQUFBQ";

    /// `wire_node_id` is the `-1` sentinel fallback the sweep skips: a node id
    /// the controller can register always projects to itself.
    #[test]
    fn a_registered_leader_projects_to_its_own_node_id() {
        assert!(wire_node_id(krabka_metadata::NodeId(42)) == 42);
    }

    #[test]
    fn share_key_parser_accepts_kafka_uuid_and_colons_in_group() {
        let key = format!("group:with:colon:{TOPIC_ID}:7");
        let (group, topic_id, partition) = parse_share_key(&key).expect("valid share key");
        assert!(group == "group:with:colon");
        assert!(topic_id == uuid::Uuid::from_bytes([5; 16]));
        assert!(partition == 7);
    }

    #[yare::parameterized(
        empty_group = { format!(":{TOPIC_ID}:0") },
        whitespace_group = { format!("   :{TOPIC_ID}:0") },
        negative_partition = { format!("group:{TOPIC_ID}:-1") },
        non_integer_partition = { format!("group:{TOPIC_ID}:not-an-int") },
        overflowing_partition = { format!("group:{TOPIC_ID}:2147483648") },
        noncanonical_topic_id = { "group:05050505-0505-0505-0505-050505050505:0".into() },
        padded_topic_id = { format!("group:{TOPIC_ID}=:0") },
        invalid_topic_id = { "group:not-base64-not-uuid:0".into() },
    )]
    fn share_key_parser_rejects_noncanonical_and_invalid_fields(key: String) {
        assert2::assert!(parse_share_key(&key).is_none(), "accepted {key:?}");
    }
}
