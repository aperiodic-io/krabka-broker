//! Voter placement helpers for diskless WAL quorums.

use krabka_metadata::{BrokerRegistrationRecord, NodeId};

/// Selects the WAL voters from distinct configured racks. An incomplete
/// result makes the caller fail closed instead of weakening the AZ-loss
/// durability guarantee.
pub(crate) fn select_voters(
    brokers: impl IntoIterator<Item = BrokerRegistrationRecord>,
    local_node: NodeId,
    voters: usize,
) -> Vec<NodeId> {
    let mut brokers = brokers
        .into_iter()
        .map(|broker| (broker.node_id, broker.rack))
        .collect::<Vec<_>>();
    brokers.sort_by_key(|(node_id, _)| node_id.0);
    select_voters_from_sorted_racks(&brokers, local_node, voters)
}

/// Select voters from broker/rack pairs already sorted by node id.
pub(crate) fn select_voters_from_sorted_racks(
    brokers: &[(NodeId, Option<String>)],
    local_node: NodeId,
    voters: usize,
) -> Vec<NodeId> {
    if voters == 0 {
        return Vec::new();
    }

    // Map exact rack-string equality to compact primitive IDs for the verified
    // selector. Brokers without a rack are deliberately absent.
    let mut rack_names: Vec<&str> = Vec::new();
    let mut candidates: Vec<(u64, u64)> = Vec::new();
    for (node_id, rack) in brokers {
        let Some(rack) = rack.as_deref() else {
            continue;
        };
        let rack_id = rack_names
            .iter()
            .position(|known| *known == rack)
            .unwrap_or_else(|| {
                rack_names.push(rack);
                rack_names.len() - 1
            });
        candidates.push((
            node_id.0,
            u64::try_from(rack_id).expect("rack index fits u64"),
        ));
    }

    krabka_verified::wal::select_wal_voters(&candidates, local_node.0, voters)
        .into_iter()
        .map(|(node, _)| NodeId(node))
        .collect()
}

#[cfg(test)]
mod tests {
    use krabka_metadata::BrokerEndpoint;
    use krabka_security::ListenerProtocol;

    use super::*;

    #[test]
    fn placement_prefers_rack_distinct_voters_with_local_first() {
        let selected = select_voters(
            [
                broker(3, Some("c")),
                broker(1, Some("a")),
                broker(2, Some("b")),
                broker(4, Some("a")),
            ],
            NodeId(1),
            3,
        );

        assert2::assert!((selected) == (vec![NodeId(1), NodeId(2), NodeId(3)]));
    }

    #[test]
    fn placement_refuses_to_weaken_the_rack_failure_budget() {
        let selected = select_voters(
            [broker(1, Some("a")), broker(2, Some("a")), broker(3, None)],
            NodeId(1),
            3,
        );

        assert2::assert!((selected) == (vec![NodeId(1)]));
    }

    #[test]
    fn placement_does_not_invent_an_unregistered_local_voter() {
        let selected = select_voters([broker(2, Some("a")), broker(3, Some("b"))], NodeId(1), 2);

        assert2::assert!(selected.is_empty());
    }

    #[test]
    fn placement_requires_a_rack_for_the_leader() {
        let selected = select_voters(
            [broker(1, None), broker(2, Some("a")), broker(3, Some("b"))],
            NodeId(1),
            3,
        );

        assert2::assert!(selected.is_empty());
    }

    fn broker(id: u64, rack: Option<&str>) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            host: format!("broker-{id}"),
            rack: rack.map(str::to_string),
            endpoints: vec![BrokerEndpoint {
                name: "INTERNAL".into(),
                host: format!("broker-{id}"),
                port: 19092,
                protocol: ListenerProtocol::Plaintext,
            }],
            ..crate::test_support::broker_registration(krabka_raft::NodeId(id))
        }
    }
}
