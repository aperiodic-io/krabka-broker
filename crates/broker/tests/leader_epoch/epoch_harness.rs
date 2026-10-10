//! The single-broker fixture the epoch tests share: booting one broker,
//! creating a one-partition topic on it, resolving that topic's id, and
//! building the one-record batch they produce.
//!
//! Four of the five tests in this suite drive one broker and differ only in
//! what they do to its epoch afterwards, so the setup lives here instead of
//! being repeated beside each of them.

use bytes::Bytes;
use krabka_broker::BrokerHandle;
use krabka_protocol::records::RecordBatch;

pub use crate::support::boot_single;
pub(crate) use crate::support::topic_id_for;
use crate::support::{
    client::connect_client,
    records::value_record,
    topics::{creatable_topic, create_topic_request},
};

pub(crate) async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str) {
    let client = connect_client(bootstrap.to_string(), None).await;
    let _ = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (name).into(),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    broker.wait_until_partition_present(name, 0).await;
}

pub(crate) async fn set_leader_epoch(broker: &BrokerHandle, name: &str, epoch: i32) {
    // Keep reconciliation on the same epoch as the appends under test.
    let mut partition = broker
        .partition_record_for_test(name, 0)
        .expect("partition");
    partition.leader_epoch = krabka_metadata::LeaderEpoch(epoch);
    let leader = partition.leader;
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(partition))
        .await
        .expect("set leader epoch");
    broker
        .wait_until_local_partition_leader(name, 0, leader)
        .await;
}

pub(crate) fn record(value: &str) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.records
        .push(value_record(0, Some(Bytes::from(value.to_string()))));
    b.last_offset_delta = 0;
    b
}
