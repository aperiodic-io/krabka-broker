//! Shared KIP-951 examples, exercised through each response type's handler.

use std::fmt::Debug;

use assert2::assert;
use krabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord};

/// Instantiate the same KIP-951 response examples for the two wire shapes.
/// The expected rows are built independently of the production converter.
macro_rules! endpoint_tests {
    ($wire:ident, $project:ident, $topic:ident, $name:ident, $parts:ident, $part:ident, $index:ident) => {
        #[cfg(test)]
        mod tests {
            use krabka_protocol::owned::$wire::{LeaderIdAndEpoch, NodeEndpoint, $part, $topic};

            fn responses(leader_ids: &[i32]) -> Vec<$topic> {
                vec![$topic {
                    $name: "orders".to_string(),
                    $parts: leader_ids
                        .iter()
                        .enumerate()
                        .map(|(index, leader_id)| $part {
                            $index: i32::try_from(index).expect("test row count fits an i32"),
                            error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
                            current_leader: LeaderIdAndEpoch {
                                leader_id: *leader_id,
                                leader_epoch: 7,
                                ..Default::default()
                            },
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }]
            }

            #[test]
            fn one_entry_per_hinted_node_on_the_connection_listener() {
                crate::handlers::node_endpoints::test_support::check_endpoints(
                    |image, listener, leader_ids| {
                        super::$project(image, listener, "INTERNAL", &responses(leader_ids))
                    },
                    |node_id, host, port, rack| NodeEndpoint {
                        node_id,
                        host: host.to_string(),
                        port,
                        rack: rack.map(ToString::to_string),
                        ..Default::default()
                    },
                );
            }
        }
    };
}

pub(crate) use endpoint_tests;

fn endpoint(name: &str, host: &str, port: u16) -> BrokerEndpoint {
    BrokerEndpoint {
        name: name.to_string(),
        host: host.to_string(),
        port,
        protocol: krabka_security::ListenerProtocol::Plaintext,
    }
}

/// Two registered brokers, each advertising an internal and an external
/// address, and only node 2 carrying a rack.
fn image() -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for (node_id, rack) in [(1_u64, None), (2, Some("rack-b".to_string()))] {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: format!("legacy-{node_id}"),
                port: 1000,
                rack,
                endpoints: vec![
                    endpoint("INTERNAL", &format!("internal-{node_id}"), 9092),
                    endpoint("EXTERNAL", &format!("external-{node_id}"), 9093),
                ],
                ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
            },
        ));
    }
    image
}

/// Checks listener selection, missing leaders, ordering and deduplication.
pub(crate) fn check_endpoints<R: Debug + PartialEq>(
    project: impl Fn(&MetadataImage, &str, &[i32]) -> Vec<R>,
    expected: impl Fn(i32, &str, i32, Option<&str>) -> R,
) {
    let image = image();
    for (name, listener, leader_ids, want) in [
        ("no hint at all", "EXTERNAL", vec![-1, -1], Vec::<R>::new()),
        (
            "the external client gets the external addresses",
            "EXTERNAL",
            vec![1, 2],
            vec![
                expected(1, "external-1", 9093, None),
                expected(2, "external-2", 9093, Some("rack-b")),
            ],
        ),
        (
            "the internal client gets the internal addresses",
            "INTERNAL",
            vec![2, 1],
            vec![
                expected(1, "internal-1", 9092, None),
                expected(2, "internal-2", 9092, Some("rack-b")),
            ],
        ),
        (
            "many rows naming one node collapse to one entry",
            "EXTERNAL",
            vec![2, 2, 2, -1],
            vec![expected(2, "external-2", 9093, Some("rack-b"))],
        ),
        (
            "a node the image does not know contributes nothing",
            "EXTERNAL",
            vec![9],
            Vec::new(),
        ),
        (
            "an unknown listener falls back to the inter-broker one",
            "NONESUCH",
            vec![1],
            vec![expected(1, "internal-1", 9092, None)],
        ),
    ] {
        let got = project(&image, listener, &leader_ids);
        assert!(got == want, "{name}: got {got:?}, want {want:?}");
    }
}
