//! Shared KIP-951 examples, exercised through each response type's handler.

use {
    crate::test_support::broker_registration,
    assert2::assert,
    krabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        fetch_response::NodeEndpoint as FetchEndpoint,
        produce_response::NodeEndpoint as ProduceEndpoint,
    },
    std::fmt::Debug,
};

/// One independently constructed partition hint in a wire response.
pub(crate) struct PartitionHint {
    pub(crate) index: i32,
    pub(crate) leader_id: i32,
}

pub(crate) fn hinted_partitions<P>(
    leader_ids: &[i32],
    make: impl Fn(PartitionHint) -> P,
) -> Vec<P> {
    leader_ids
        .iter()
        .enumerate()
        .map(|(index, &leader_id)| {
            make(PartitionHint {
                index: i32::try_from(index).expect("test row count fits an i32"),
                leader_id,
            })
        })
        .collect()
}

/// Independent expected wire rows; the production endpoint converter is not used.
pub(crate) struct ExpectedEndpoints {
    pub(crate) fetch: FetchEndpoint,
    pub(crate) produce: ProduceEndpoint,
}

fn expected_endpoint(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> ExpectedEndpoints {
    ExpectedEndpoints {
        fetch: FetchEndpoint {
            node_id,
            host: host.to_owned(),
            port,
            rack: rack.map(ToOwned::to_owned),
            ..Default::default()
        },
        produce: ProduceEndpoint {
            node_id,
            host: host.to_owned(),
            port,
            rack: rack.map(ToOwned::to_owned),
            ..Default::default()
        },
    }
}

struct BrokerCase {
    node_id: u64,
    rack: Option<String>,
}

struct EndpointCase<R> {
    name: &'static str,
    listener: &'static str,
    leader_ids: Vec<i32>,
    want: Vec<R>,
}

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
    for BrokerCase { node_id, rack } in [
        BrokerCase {
            node_id: 1,
            rack: None,
        },
        BrokerCase {
            node_id: 2,
            rack: Some("rack-b".to_string()),
        },
    ] {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                host: format!("legacy-{node_id}"),
                port: 1000,
                rack,
                endpoints: vec![
                    endpoint("INTERNAL", &format!("internal-{node_id}"), 9092),
                    endpoint("EXTERNAL", &format!("external-{node_id}"), 9093),
                ],
                ..broker_registration(krabka_raft::NodeId(node_id))
            },
        ));
    }
    image
}

/// Checks listener selection, missing leaders, ordering and deduplication.
pub(crate) fn check_endpoints<R: Debug + PartialEq>(
    project: impl Fn(&MetadataImage, &str, &[i32]) -> Vec<R>,
    select_expected: impl Fn(ExpectedEndpoints) -> R,
) {
    let image = image();
    let expected = |node_id, host: &str, port, rack: Option<&str>| {
        select_expected(expected_endpoint(node_id, host, port, rack))
    };
    for case in [
        EndpointCase {
            name: "no hint at all",
            listener: "EXTERNAL",
            leader_ids: vec![-1, -1],
            want: Vec::<R>::new(),
        },
        EndpointCase {
            name: "the external client gets the external addresses",
            listener: "EXTERNAL",
            leader_ids: vec![1, 2],
            want: vec![
                expected(1, "external-1", 9093, None),
                expected(2, "external-2", 9093, Some("rack-b")),
            ],
        },
        EndpointCase {
            name: "the internal client gets the internal addresses",
            listener: "INTERNAL",
            leader_ids: vec![2, 1],
            want: vec![
                expected(1, "internal-1", 9092, None),
                expected(2, "internal-2", 9092, Some("rack-b")),
            ],
        },
        EndpointCase {
            name: "many rows naming one node collapse to one entry",
            listener: "EXTERNAL",
            leader_ids: vec![2, 2, 2, -1],
            want: vec![expected(2, "external-2", 9093, Some("rack-b"))],
        },
        EndpointCase {
            name: "a node the image does not know contributes nothing",
            listener: "EXTERNAL",
            leader_ids: vec![9],
            want: Vec::new(),
        },
        EndpointCase {
            name: "an unknown listener falls back to the inter-broker one",
            listener: "NONESUCH",
            leader_ids: vec![1],
            want: vec![expected(1, "internal-1", 9092, None)],
        },
    ] {
        let got = project(&image, case.listener, &case.leader_ids);
        assert!(
            got == case.want,
            "{}: got {got:?}, want {:?}",
            case.name,
            case.want
        );
    }
}
