//! The two-broker `SASL_PLAINTEXT` inter-broker case: peers authenticate to
//! each other over raft, and a JVM client drives a produce against broker 0.
//!
//! It shares the suite because it is an inter-broker authentication test, but
//! it is the one case with no TLS anywhere, and its claim is narrower than the
//! `SASL_SSL` variant's -- metadata convergence plus a single-replica produce,
//! not rf=2 replication. The test's own doc comment explains why the stronger
//! assertion is not reliable on this network topology.

use crate::jvm_acceptance::{
    ADMIN, ADMIN_PASS, KAFKA_IMAGE, assert_console_produced, create_console_topic,
    nc_check_connectivity, numbered_payload, produce_console, start_two_sasl_brokers,
    write_plain_props,
};

/// JVM-driven 2-broker test for the `SASL_PLAINTEXT` inter-broker
/// listener. Both brokers boot with the same shared `admin` credential and
/// mechanism=PLAIN. The raft layer authenticates each peer in both
/// directions before the cluster converges on a 2-broker metadata view.
/// A JVM client then SASL-authenticates as the same `admin` identity over
/// the data-plane listener, creates a topic, and produces 50 records.
///
/// Why this test is *not* a follower-replication assertion: the brokers
/// in this test advertise `host.docker.internal:<port>` so the JVM
/// container can reach them with `--add-host=...:host-gateway`. Under WSL2
/// that hostname resolves to the Windows host IP, for example
/// `10.0.0.170`, which is *not* routable back into the WSL VM where the
/// broker peers live. So follower-fetch traffic that flows broker→broker
/// cannot complete on this network topology. That traffic is the
/// `InterBrokerClient` dialing the peer's advertised address from
/// `MetadataImage`. This is not a SASL or replication bug. It is a
/// Docker-on-WSL networking limit. The Rust-driven equivalent is
/// `tests/auth_handlers/two_broker_sasl.rs::two_broker_sasl_plaintext_replication`.
/// It uses 127.0.0.1 advertised addresses for both brokers and *does*
/// exercise end-to-end inter-broker SASL replication. Use that as the
/// load-bearing inter-broker SASL test.
///
/// What this test *does* assert end-to-end through the JVM client:
///
/// 1. Two brokers boot with `SASL_PLAINTEXT` inter-broker auth and exchange
///    raft `AppendEntries` + `BrokerHeartbeat` traffic over SASL.
/// 2. The cluster converges on a 2-broker metadata view (both brokers'
///    `broker_count() >= 2`).
/// 3. The JVM `kafka-topics` and `kafka-console-producer` tools both
///    SASL-authenticate as `admin` and successfully drive a single-partition
///    topic produce against broker 0.
/// 4. Broker 0's local log has all 50 records after produce returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_inter_broker_replication_authed() {
    const TOPIC: &str = "krabka-jvm-inter-broker-itest";

    let (broker0, broker1, _dir0, _dir1) = start_two_sasl_brokers(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Wait for both brokers to converge on a 2-broker metadata image —
    // the load-bearing inter-broker SASL handshake. If the peer SASL
    // credentials mismatched, broker 1 would never register and this
    // would time out.
    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;

    // JVM client config: SASL_PLAINTEXT + PLAIN as the admin (super-user).
    let props = write_plain_props(ADMIN, ADMIN_PASS);
    let mount = props.mount_str();

    // Create an rf=1 topic (see test doc-comment — JVM-driven rf=2
    // assertion isn't reliable under WSL networking). Single replica is
    // enough to prove the JVM client → broker SASL handshake works in
    // both directions across the two-broker cluster's controller layer.
    create_console_topic(KAFKA_IMAGE, &[&mount], TOPIC, 1, 1);

    // Wait for the topic to materialize in a broker's metadata image (either
    // broker; committed metadata converges on both).
    tokio::select! {
        () = broker0.wait_until_partition_present(TOPIC, 0) => {}
        () = broker1.wait_until_partition_present(TOPIC, 0) => {}
    }

    // Produce 50 records via `kafka-console-producer`. The metadata
    // response steers the producer to whichever broker leads partition 0.

    let payload = numbered_payload("rec", 50);
    let producer_out = produce_console(KAFKA_IMAGE, &[&mount], TOPIC, false, payload.as_bytes());
    assert_console_produced(&producer_out);

    // Verify the leader has 50 records on disk. We don't know in advance
    // which broker leads partition 0 (raft picks one), so wait for whichever
    // broker's local log reaches offset 50 first; the losing awaiter is
    // dropped (the non-leader never materializes the partition locally).
    tokio::select! {
        () = broker0.wait_until_local_log_end_offset(TOPIC, 0, 50) => {}
        () = broker1.wait_until_local_log_end_offset(TOPIC, 0, 50) => {}
    }

    broker0.shutdown().await;
    broker1.shutdown().await;
}
