//! The closed half of the role: a client `Produce` or consumer `Fetch` that
//! reaches the witness is refused with `NOT_LEADER_OR_FOLLOWER`, while the
//! witness's own follower fetch keeps replicating.
//!
//! The two halves have to be asserted together. A witness that refused
//! everything would be no ISR member at all, and it is the ISR seat that keeps
//! `acks=all` writable after a site loss, so the test checks the local log and
//! the ISR right after the refusals.
//!
//! Both refusals also carry the KIP-951 redirect, and this is the one place
//! where the whole of it is observable on a live cluster: the refusing node is
//! never the leader, so the `NodeEndpoints` entry has to come out of the
//! metadata image rather than out of the answering broker's own config.

use std::collections::BTreeSet;

use assert2::check;
use krabka_broker::codes;

use crate::{
    N_RECORDS, SITE_C, TOPIC, cluster_lock, within,
    witness_cluster::{client_at, shutdown, start_stretch_cluster, wait_for_witness_isr},
    witness_wire::{consumer_fetch, create_topic, produce_error, produce_response},
};

/// The listener every client in this suite connects on. A KIP-951
/// `NodeEndpoints` entry advertises the address of the listener the request
/// arrived on, so this is the endpoint name the refusals must answer with.
const PLAINTEXT_LISTENER: &str = "PLAINTEXT";

/// A client `Produce` and a consumer `Fetch` that reach the witness are
/// refused, while replication to that same witness keeps advancing: its local
/// log reaches the produced offset and it stays in the ISR.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_refuses_client_traffic_while_replication_advances() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    let leader = client_at(&cluster[0].1.listen_addr.to_string()).await;
    let topic_id = create_topic(&leader).await;
    wait_for_witness_isr(&cluster, TOPIC).await;

    check!(
        produce_error(&leader, topic_id, N_RECORDS).await == codes::NONE,
        "acks=all commits on the leader with all three sites up"
    );

    // Replication to the witness advances: it holds the records. This is a
    // FOLLOWER fetch the witness itself issued, so it is the half of the fetch
    // path that must keep working.
    let witness_handle = &cluster[2].0;
    within(
        "the witness replicates every record",
        witness_handle.wait_until_local_log_end_offset(TOPIC, 0, i64::from(N_RECORDS)),
    )
    .await;

    let witness = client_at(&cluster[2].1.listen_addr.to_string()).await;

    // The address both refusals must advertise for the leader, read out of the
    // image of the broker that answers them. That broker is the witness, which
    // never leads, so nothing here can come from its own listener config.
    let image = cluster[2].0.controller_image_for_test();
    let leader = image
        .partition(TOPIC, 0)
        .expect("the witness image knows the partition")
        .leader;
    let registration = image.broker(leader).expect("the leader is registered");
    let advertised = registration
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == PLAINTEXT_LISTENER)
        .expect("the leader advertises a PLAINTEXT endpoint");
    let leader_id = i32::try_from(leader.0).expect("broker ids fit an i32");

    let produced = produce_response(&witness, topic_id, N_RECORDS).await;
    check!(
        produced.responses[0].partition_responses[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Produce to the witness is refused"
    );
    let expected_endpoint = krabka_protocol::owned::produce_response::NodeEndpoint {
        node_id: leader_id,
        port: i32::from(advertised.port),
        host: advertised.host.clone(),
        rack: registration.rack.clone(),
        ..Default::default()
    };
    check!(
        produced.node_endpoints == [expected_endpoint],
        "the refused Produce carries the leader's endpoint (KIP-951): {:?}",
        produced.node_endpoints
    );

    let fetched = witness
        .send(consumer_fetch(topic_id, SITE_C))
        .await
        .expect("consumer Fetch to the witness");
    check!(
        fetched.responses[0].partitions[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Fetch to the witness is refused"
    );
    check!(
        fetched.responses[0].partitions[0].current_leader.leader_id == leader_id,
        "the refused Fetch names the leader (KIP-951): {:?}",
        fetched.responses[0].partitions[0].current_leader
    );
    check!(
        fetched.node_endpoints
            == vec![krabka_protocol::owned::fetch_response::NodeEndpoint {
                node_id: leader_id,
                host: advertised.host.clone(),
                port: i32::from(advertised.port),
                rack: registration.rack.clone(),
                ..Default::default()
            }],
        "the refused Fetch carries the leader's endpoint (KIP-951): {:?}",
        fetched.node_endpoints
    );

    // The refusals are client-facing only. The witness is still a full ISR
    // member, which is what keeps `min.insync.replicas=2` satisfiable when a
    // data site is lost.
    let isr: BTreeSet<u64> = cluster[0]
        .0
        .partition_isr_for_test(TOPIC, 0)
        .expect("the leader knows the partition")
        .into_iter()
        .collect();
    check!(
        isr == maplit::btreeset! {1, 2, 3},
        "the witness stays in the ISR after refusing client traffic"
    );

    shutdown(cluster).await;
}
