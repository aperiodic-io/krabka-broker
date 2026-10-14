//! The two-way cut rests on telling the dialer of each connection apart by
//! its client id. These cases pin the names a broker gives its own clients to
//! the node they belong to, and every other name to no node.

use {crate::support::linked_cluster::peer_of_client_id, assert2::assert};

#[test]
fn a_broker_client_id_names_the_node_that_dials() {
    let cases: [(&str, Option<u64>); 14] = [
        ("krabka-broker-2-replicator", Some(2)),
        ("krabka-broker-1-controller", Some(1)),
        ("krabka-broker-3-heartbeat", Some(3)),
        ("krabka-broker-3-isr", Some(3)),
        ("krabka-broker-2-observer", Some(2)),
        ("krabka-broker-1-writer", Some(1)),
        ("krabka-broker-txn-3", Some(3)),
        ("krabka-broker-barrier-1", Some(1)),
        ("krabka-consumer-lag-2", Some(2)),
        // Not a node of a three-node cluster.
        ("krabka-broker-4-replicator", None),
        ("krabka-broker-0-replicator", None),
        // Not a broker's own client.
        ("krabka", None),
        ("krabka-auto-join", None),
        ("dedup-partition", None),
    ];
    for (client_id, node) in cases {
        assert!(peer_of_client_id(client_id) == node, "{client_id}");
    }
}
