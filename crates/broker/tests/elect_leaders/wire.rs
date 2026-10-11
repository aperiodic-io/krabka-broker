//! The PLAINTEXT wire helpers that this suite drives `ElectLeaders` through:
//! the client id every request carries, the typed `ElectLeaders` driver, and the `CreateTopics` call that materialises the partition whose
//! leader the tests then elect.
//!
//! The compatibility shim of the authorizer maps an empty `super_users` list
//! and zero ACLs to Allow, so these helpers need no SASL handshake. The
//! `SASL_PLAINTEXT` flavours of the same two drivers live in `sasl`, next to
//! the authorization test that is the only user of a SASL listener here.

use std::net::SocketAddr;

use assert2::assert;
use krabka_protocol::owned::elect_leaders_request::ElectLeadersRequest;
use tokio::net::TcpStream;

use crate::{
    kafka_wire,
    support::{admin::ElectionKind, topics::AssignedTopicSetup},
};

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-elect-test";

/// Drives `ElectLeaders` over a fresh PLAINTEXT connection.
///
/// The compat shim of the authorizer maps no `super_users` and no ACLs to
/// Allow, so this request passes without SASL. This function asserts that the
/// top-level `error_code == 0`. It returns the per-partition
/// `(partition_id, error_code)` rows for the topic named `topic`.
/// Sends `ElectLeaders` with a null topic list -- the "every partition in the
/// cluster" shape -- and returns every `(topic, partition, error_code)` row
/// the broker answered with.
pub async fn drive_elect_all_partitions(
    addr: SocketAddr,
    kind: ElectionKind,
) -> Vec<(String, i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = ElectLeadersRequest {
        election_type: kind.wire_code(),
        topic_partitions: None,
        timeout_ms: 30_000,
        ..Default::default()
    };
    let resp = kafka_wire::elect_leaders_response(&mut stream, &req, CLIENT_ID).await;

    assert!(
        resp.error_code == 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    resp.replica_election_results
        .into_iter()
        .flat_map(|result| {
            let topic = result.topic;
            result
                .partition_result
                .into_iter()
                .map(move |p| (topic.clone(), p.partition_id, p.error_code))
        })
        .collect()
}

/// Creates a topic on a PLAINTEXT broker.
///
/// The compat shim of the authorizer lets the request through because there
/// are no `super_users` and no ACLs.
///
/// The topic has one partition on `replicas`, in that order, so the tests know
/// which broker leads it and which one they can elect: an automatic placement
/// starts at a random broker.
pub async fn create_topic_plaintext(addr: SocketAddr, topic: AssignedTopicSetup) {
    kafka_wire::create_assigned_topic_plaintext(
        addr,
        kafka_wire::AssignedTopicExchangeSetup {
            client_id: CLIENT_ID,
            topic,
        },
    )
    .await;
}
pub use crate::kafka_wire::elect_leaders_plaintext as drive_elect_leaders;
