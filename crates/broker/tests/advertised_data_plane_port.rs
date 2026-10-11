//! A `for_tests` broker that asks for an OS-assigned data-plane port must
//! register the port it actually bound, not the `:0` it was configured with.
//!
//! `register_broker` used to submit this node's `BrokerRegistrationRecord`
//! before the data-plane listener bound, so a `:0` config published port 0
//! into the metadata image. Every other broker's `Metadata` response, and
//! every KIP-annotated admin call that routes through it (`ListGroups`,
//! `DescribeCluster`, ...), then named this broker's own dead endpoint.

mod support;

use assert2::assert;
use krabka_protocol::owned::metadata_response::MetadataResponse;

use crate::support::{
    client::connect_client, discovery::topic_metadata_request, standalone_broker,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_port_zero_broker_registers_its_bound_port() {
    let (_dir, broker) = standalone_broker().await;
    let bound_port = i32::from(broker.listen_addr().port());
    assert!(bound_port != 0, "the broker bound an OS-assigned port");

    let client = connect_client(broker.listen_addr().to_string(), None).await;
    let resp: MetadataResponse = client
        .send(topic_metadata_request(Some(vec![])))
        .await
        .unwrap();

    assert!(resp.brokers.len() == 1);
    assert!(
        resp.brokers[0].port == bound_port,
        "Metadata advertised port {}, the broker actually bound {bound_port}: {:?}",
        resp.brokers[0].port,
        resp.brokers[0],
    );

    broker.shutdown().await;
}
