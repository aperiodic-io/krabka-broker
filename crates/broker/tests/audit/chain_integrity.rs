//! The tamper-evidence apparatus over the audit records: the signed
//! checkpoints, and the hash chain those checkpoints sign.
//!
//! A chain is evidence only while it is unbroken. One case drives the
//! checkpoint cadence down to one record and looks for the signed checkpoint
//! on the topic. The other restarts the broker on the same data directory and
//! asserts that the second boot carried the sequence numbers on rather than
//! resetting them to zero.

use crate::{
    support,
    support::topics::{creatable_topic, create_topic_request},
};

/// Verifies the checkpoint path. The broker is configured with an audit
/// signing key and a checkpoint cadence of `every_n = 1`. A `CreateTopics`
/// request must then put a `checkpoint` record on the audit topic with the
/// expected `key_id`.
#[tokio::test]
async fn signed_checkpoints_appear_on_audit_topic() {
    use ring::signature::Ed25519KeyPair;

    // Generate a key, write it to a temp file, start a broker configured to use it.
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
    let keydir = tempfile::tempdir().unwrap();
    let keypath = keydir.path().join("audit.pk8");
    std::fs::write(&keypath, pkcs8.as_ref()).unwrap();

    // Start a broker with audit signing + a tiny checkpoint cadence (every 1 record).
    let p = support::start_with_audit_key(&keypath, "k-test", 1).await;

    // Cause some audit events (a create succeeds; super-user path).
    let audit_before = p.broker.metrics().audit_events.get();
    let _ = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("cp-topic").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();

    // Wait for the create's chained record AND its signed checkpoint to be
    // durable: with `every_n = 1`, each audit event triggers a checkpoint, so
    // the counter advances by 2 (chained record + checkpoint) per create.
    p.broker
        .wait_for_metrics("audit checkpoint written", |m| {
            m.audit_events.get() >= audit_before + 2
        })
        .await;

    let recs = support::wait_for_audit_record(&p.client, "signed checkpoint", |j| {
        j["type"] == "checkpoint" && j["key_id"] == "k-test"
    })
    .await;
    let saw_checkpoint = recs
        .iter()
        .any(|j| j["type"] == "checkpoint" && j["key_id"] == "k-test");
    assert2::check!(saw_checkpoint);

    p.broker.shutdown().await;
}

/// Verifies that the audit hash-chain sequence numbers are contiguous, and
/// that none repeats, across a broker restart. That shows that chain recovery
/// worked, and that the second boot did NOT reset the chain to seq 0.
#[tokio::test]
async fn audit_chain_continues_across_restart() {
    let dir = tempfile::tempdir().unwrap();

    // First boot: generate some audit events, then shut down cleanly.
    {
        let (broker, _client) = boot_with_audited_topic(
            dir.path(),
            crate::support::topics::CreateTopicSetup {
                topic: "r1",
                ..Default::default()
            },
        )
        .await;
        broker.shutdown().await;
    }

    // Second boot on the SAME data dir: more events.
    let (broker, client) = boot_with_audited_topic(
        dir.path(),
        crate::support::topics::CreateTopicSetup {
            topic: "r2",
            ..Default::default()
        },
    )
    .await;

    // Consume the audit topic and assert seqs are a contiguous, duplicate-free
    // chain (recovery worked — no reset to 0 on the second boot).
    let seqs = support::wait_for_audit_seq_count(&client, 4).await;
    assert2::check!(seqs.len() >= 4); // 2 BrokerStarted + 2 CreateTopics (at least)
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert2::check!(sorted.len() == seqs.len()); // no duplicate seqs
    assert2::check!(sorted == (0..seqs.len() as u64).collect::<Vec<_>>()); // contiguous from 0

    broker.shutdown().await;
}

/// Await the exact topic-creation audit event on each boot before shutdown or consumption.
async fn boot_with_audited_topic(
    directory: &std::path::Path,
    setup: crate::support::topics::CreateTopicSetup<'_>,
) -> (krabka_broker::BrokerHandle, krabka_client_core::Client) {
    let (broker, client) = support::start_with_dir(directory).await;
    let audit_before = broker.metrics().audit_events.get();
    let _ = client
        .send(crate::support::topics::configured_topic_request(setup))
        .await
        .unwrap();
    broker
        .wait_for_metrics("audit event written", |metrics| {
            metrics.audit_events.get() > audit_before
        })
        .await;
    (broker, client)
}
