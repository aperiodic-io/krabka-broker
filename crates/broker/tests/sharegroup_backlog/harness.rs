//! The pieces both backlog cases share: the cluster lock that serialises them,
//! the HTTP scrape of the broker's `/metrics` endpoint, and the `CreateTopics`
//! and `Produce` drivers that put records behind a share group.
//!
//! The scrape is written against a raw `TcpStream` rather than an HTTP client
//! because the assertion is on the exposition text itself, one
//! `krabka_broker_share_group_backlog` line with its labels.

use {
    crate::support::{
        client::http_get,
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::RecordBatch},
    std::{net::SocketAddr, sync::OnceLock},
    tokio::sync::Mutex,
};

pub const TOPIC: &str = "backlog-itest";

pub fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub async fn scrape(addr: SocketAddr) -> String {
    let response = http_get(addr, "/metrics", false).await;
    let body = response.find("\r\n\r\n").map_or(0, |at| at + 4);
    response[body..].to_owned()
}

pub async fn create_topic(client: &Client, partitions: i32, replication_factor: i16) {
    let response = client
        .send(create_topic_request(
            creatable_topic()
                .name((TOPIC).into())
                .partitions(partitions)
                .replicas(replication_factor)
                .call(),
        ))
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0, "{response:?}");
}

pub async fn produce_five(client: &Client, topic_id: uuid::Uuid) {
    let records = (0..5)
        .map(|offset| value_record(offset, Some(bytes::Bytes::from_static(b"work"))))
        .collect();
    let response = client
        .send(batch_request(
            RecordBatch {
                last_offset_delta: 4,
                ..batch_from_records(records)
            },
            SinglePartitionProduceSetup {
                topic: (TOPIC).into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                ..SinglePartitionProduceSetup::replicated()
            },
        ))
        .await
        .unwrap();
    assert!(
        response.responses[0].partition_responses[0].error_code == 0,
        "{response:?}"
    );
}
