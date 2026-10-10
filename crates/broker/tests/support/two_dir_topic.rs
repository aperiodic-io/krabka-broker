//! Ready topic fixtures for the three two-directory integration binaries.
use std::net::SocketAddr;

use krabka_broker::BrokerHandle;
use tempfile::TempDir;

pub(crate) use crate::support::storage::PartitionReadiness;

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct Setup<'a> {
    #[default("krabka-jbod-test")]
    pub client_id: &'a str,
    #[default("t")]
    pub topic: &'a str,
    #[default(crate::support::topics::TopicPartitionCount(1))]
    pub partitions: crate::support::topics::TopicPartitionCount,
    pub readiness: PartitionReadiness,
}

pub(crate) async fn create_and_wait(handle: &BrokerHandle, address: SocketAddr, setup: Setup<'_>) {
    crate::kafka_wire::create_configured_topic_plaintext(
        address,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: setup.client_id,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: setup.topic.into(),
                partitions: setup.partitions,
                ..Default::default()
            },
        },
    )
    .await;
    for partition in 0..setup.partitions.0 {
        match setup.readiness {
            PartitionReadiness::MetadataPublished => {
                handle
                    .wait_until_partition_present(setup.topic, partition)
                    .await;
            }
            PartitionReadiness::LocalWriterPresent => {
                handle
                    .wait_until_local_log_end_offset(setup.topic, partition, 0)
                    .await;
            }
        }
    }
}

pub(crate) async fn start(setup: Setup<'_>) -> (BrokerHandle, TempDir, TempDir, SocketAddr) {
    let (handle, primary, extra, address) = crate::support::storage::start_two_dir_broker().await;
    create_and_wait(&handle, address, setup).await;
    (handle, primary, extra, address)
}
