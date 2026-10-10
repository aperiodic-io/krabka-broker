//! The client side of the suite: a bootstrapped `Client`, the topic every test
//! writes to, and the `acks=all` produce whose partition-level error code is
//! what most of the claims are asserted on.

use std::time::{Duration, Instant};

use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::{N_RECORDS, TOPIC, support::client::connect_owned};

pub async fn client_at(addr: &str) -> Client {
    connect_owned(addr.to_string(), "stretch-cluster-test", "client build").await
}

/// Create `TOPIC` with one partition and rf=3, and return its id.
pub async fn create_topic(client: &Client) -> WireUuid {
    crate::support::client::create_replicated_topic(client, TOPIC).await
}

/// The partition-level error code of one `acks=all` produce.
pub async fn produce_once(client: &Client, topic_id: WireUuid, timeout_ms: i32) -> i16 {
    let resp = client
        .send(crate::support::produce::batch_request(
            crate::support::client::value_batch(crate::support::client::ValueBatchSetup {
                records: crate::support::client::ValueRecordCount(N_RECORDS),
            }),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: (TOPIC).into(),
                topic_id,
                timeout: crate::support::produce::ProduceTimeoutMillis(timeout_ms),
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        ))
        .await
        .expect("Produce round-trip");
    resp.responses[0].partition_responses[0].error_code
}

/// `acks=all` against `addr`, retried until it commits or the bound expires.
///
/// The retry covers only the window in which the surviving replicas have not
/// yet been dropped from the ISR — the leader answers `REQUEST_TIMED_OUT` or
/// `NOT_ENOUGH_REPLICAS` until the controller commits the shrink. It never
/// turns a persistent refusal into a pass: the last code is what the caller
/// asserts on.
pub async fn produce_until_committed(addr: &str, topic_id: WireUuid) -> i16 {
    let client = client_at(addr).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut code = produce_once(&client, topic_id, 5_000).await;
    while code != codes::NONE && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        code = produce_once(&client, topic_id, 5_000).await;
    }
    code
}

pub async fn initialize_topic(addr: &str, handles: [&krabka_broker::BrokerHandle; 3]) -> WireUuid {
    let client = client_at(addr).await;
    let topic_id = create_topic(&client).await;
    for handle in handles {
        crate::within(
            "the partition reaches every node",
            handle.wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    crate::view::wait_for_leader_and_isr(
        handles[0],
        "the initial three-replica ISR",
        1,
        &[1, 2, crate::WITNESS],
    )
    .await;
    topic_id
}

/// Initialize the three sites in their configured node order.
pub async fn initialize_sites<'a>(
    addr: String,
    handle: impl Fn(usize) -> &'a krabka_broker::BrokerHandle,
) -> WireUuid {
    initialize_topic(
        &addr,
        [
            handle(crate::NODE_A),
            handle(crate::NODE_B),
            handle(crate::NODE_C),
        ],
    )
    .await
}
