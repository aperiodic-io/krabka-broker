//! The visible half of the role: the witness registers with its rack, takes a
//! replica and an ISR seat of an rf=3 partition, and is still never handed to a
//! consumer as a read replica.
//!
//! `Metadata.brokers[]` is what every admin tool resolves replica ids through,
//! so a witness missing from it would break `kafka-topics` and
//! `kafka-reassign-partitions` output. The KIP-392 check belongs with it: a
//! same-rack in-ISR replica is exactly the candidate the rack-aware selector
//! reaches for, which makes this the case where the redirect ban has to hold.

use {
    crate::{
        LEADER_ID, SITE_A, SITE_B, SITE_C, TOPIC, WITNESS_ID, cluster_lock,
        witness_cluster::{client_at, shutdown, start_stretch_cluster, wait_for_witness_isr},
        witness_wire::{PartitionView, consumer_fetch, create_topic, partition_view},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_protocol::owned::metadata_request::MetadataRequest,
    std::collections::BTreeMap,
};

/// The witness registers like any other broker — rack included — and it takes
/// a replica and an ISR seat of an rf=3 partition. A consumer that names the
/// witness site is not redirected to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_is_a_visible_isr_member_that_serves_no_reads() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    let client = client_at(&cluster[0].1.listen_addr.to_string()).await;
    let topic_id = create_topic(&client).await;
    wait_for_witness_isr(&cluster, TOPIC).await;

    // The witness must stay resolvable: `kafka-topics` and
    // `kafka-reassign-partitions` render replica ids through this list, and its
    // rack is what `kafka-reassign-partitions --generate` reads to keep a
    // reassignment inside a site.
    let resp = client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata for the broker list");
    let racks: BTreeMap<i32, Option<String>> = resp
        .brokers
        .iter()
        .map(|broker| (broker.node_id, broker.rack.clone()))
        .collect();
    check!(
        racks
            == maplit::btreemap! {
            1 => Some(SITE_A.to_string()),
            2 => Some(SITE_B.to_string()),
            WITNESS_ID => Some(SITE_C.to_string())},
        "every broker, the witness included, is in Metadata.brokers[] with its rack"
    );

    check!(
        partition_view(&client).await
            == PartitionView {
                error_code: codes::NONE,
                partition_index: 0,
                leader_id: LEADER_ID,
                replica_nodes: vec![1, 2, WITNESS_ID],
                isr_nodes: maplit::btreeset! {1, 2, WITNESS_ID},
                offline_replicas: vec![],
            },
        "the witness is a replica and an ISR member; the leader is in the preferred site"
    );

    // KIP-392: the witness is an in-ISR same-rack replica for a `site-c`
    // consumer, which is exactly the case in which the rack-aware selector
    // would pick it. It must not: a witness serves no client reads.
    let redirected = client
        .send(consumer_fetch(topic_id, SITE_C))
        .await
        .expect("consumer Fetch to the leader with client.rack=site-c");
    check!(
        redirected.responses[0].partitions[0].preferred_read_replica == -1,
        "a consumer in the witness site must not be redirected to the witness"
    );

    shutdown(cluster).await;
}
