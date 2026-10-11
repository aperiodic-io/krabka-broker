// rustc 1.95 clippy ICEs on this file in the same places as throttle.rs /
// client_quotas.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
//! Broker-level integration test for (user, client-id) tuple quota
//! end-to-end enforcement.
//!
//! Tests:
//! 1. `tuple_quota_throttles_only_matching_client_id`. It sets
//!    `(user=alice, client-id=app-x) producer_byte_rate=1024`. A produce of
//!    about 4 KB as (alice, app-x) must give `throttle_time_ms > 0`. A produce
//!    of about 4 KB as (alice, other) must give `throttle_time_ms == 0`,
//!    because no quota matches.
//!
//! This test covers the end-to-end behaviour: the Produce handler must forward
//! `ctx.client_id` to the quota lookup instead of "". If it forwards "", the
//! tuple lookup never matches and `throttle_time_ms` is 0 in both cases.
//!
//! The test is gated to non-Windows, to match the multi-broker test convention
//! established by the existing integration suites.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `tuple_quota_enforcement/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
mod kafka_wire;
mod support;
#[path = "tuple_quota_enforcement/tuple_quota_cluster.rs"]
mod tuple_quota_cluster;
#[path = "tuple_quota_enforcement/tuple_quota_drivers.rs"]
mod tuple_quota_drivers;

/// The default client id request headers in this suite carry. The tuple-quota
/// test sends a second produce under another id, which the broker reads for the
/// quota lookup.
const CLIENT_ID: &str = "krabka-tuple-quota-test";

use assert2::assert;

use crate::{
    kafka_wire::{PlainAuthenticationSetup, quotas::QuotaAlterSetup},
    tuple_quota_cluster::{
        create_topic_as_admin, seed_alice_write_acl, seed_compat_shim_disable_acl,
        start_single_broker_sasl_plaintext_with_users, wait_partition_exists,
    },
    tuple_quota_drivers::{await_authorized_produce, drive_alter_client_quotas_sasl},
};

// ─────────────────────────────────────────────────────────────────────────────
// Integration test
// ─────────────────────────────────────────────────────────────────────────────

/// Sets `(user=alice, client-id=app-x) producer_byte_rate=1024`.
///
/// * A produce of about 4 KB as (alice, app-x) gives `throttle_time_ms > 0`,
///   because the tuple matches.
/// * A produce of about 4 KB as (alice, other) gives `throttle_time_ms == 0`,
///   because no quota matches.
///
/// The second assertion verifies that the tuple quota does NOT fire on an
/// unmatched `client_id`, that is, that no `(user=alice)` fallback quota is
/// set.
///
/// This test covers the end-to-end behaviour: the Produce handler must pass
/// `ctx.client_id` to the quota lookup. If it passes `""`, no tuple quota ever
/// matches and both produces return `throttle_time_ms == 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tuple_quota_throttles_only_matching_client_id() {
    let admin_password = uuid::Uuid::new_v4().to_string();
    let alice_password = uuid::Uuid::new_v4().to_string();
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[
            ("admin", admin_password.as_str()),
            ("alice", alice_password.as_str()),
        ],
    )
    .await;

    // Seed ACL entries so the authorizer engages (compat shim disabled) and
    // alice can Write to the topic.
    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(
        addr,
        kafka_wire::SaslTopicSetup {
            client_id: CLIENT_ID,
            password: admin_password.as_bytes(),
            topic: support::topics::ConfiguredTopicSetup {
                name: "tuple-quota-topic".into(),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    wait_partition_exists(&handle, "tuple-quota-topic", 0).await;
    seed_alice_write_acl(&handle, "tuple-quota-topic").await;

    // Set tuple quota: (user=alice, client-id=app-x) producer_byte_rate=1024.
    // Rate = 1024 bytes/sec, burst = 1 second at rate = 1024 bytes free.
    // Producing 4 KB = 4096 bytes means ~3072 bytes over budget → throttle fires.
    // No (user=alice)-only quota is set, so (alice, other) has no quota at all.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        vec![(
            vec![
                ("user".into(), Some("alice".into())),
                ("client-id".into(), Some("app-x".into())),
            ],
            vec![("producer_byte_rate".into(), 1024.0, false)],
        )],
        &QuotaAlterSetup {
            authentication: PlainAuthenticationSetup {
                client_id: CLIENT_ID,
                user: "admin",
                password: admin_password.as_bytes().to_vec(),
            },
            ..Default::default()
        },
    )
    .await;
    assert!(
        alter_resp.len() == 1,
        "one entry in AlterClientQuotas response"
    );
    assert!(
        alter_resp[0].1 == 0,
        "AlterClientQuotas must succeed; error_code={}",
        alter_resp[0].1
    );

    // Await until the quota appears in the metadata image (absorb raft latency).
    //
    // `MetadataImage` canonicalizes EntityKey by sorting entries alphabetically
    // by entity_type, so the stored key has "client-id" before "user".
    handle
        .wait_for_image(|img| {
            let key: krabka_metadata::EntityKey = vec![
                ("client-id".into(), Some("app-x".into())),
                ("user".into(), Some("alice".into())),
            ];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("producer_byte_rate"))
                == Some(&1024.0)
        })
        .await;

    // ── Case 1: (alice, app-x) — tuple matches, must throttle ────────────────
    //
    // Retry loop: TOPIC_AUTHORIZATION_FAILED (29) can fire if the alice Write
    // ACL hasn't propagated to the handler's image snapshot yet.
    let matching_resp = await_authorized_produce(addr, alice_password.as_bytes(), "app-x").await;

    let part = &matching_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce (alice, app-x) must succeed; error_code={}",
        part.error_code
    );
    assert!(
        matching_resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0 for (alice, app-x) with producer_byte_rate=1024 \
         and 4 KB payload; got {} — T3 may not have merged yet (T3 wires ctx.client_id \
         into the quota call site)",
        matching_resp.throttle_time_ms
    );

    // ── Case 2: (alice, other) — no quota match, must NOT throttle ────────────
    //
    // A fresh TCP connection means the token bucket starts from scratch, so
    // there is no residual debt from Case 1.  No (user=alice)-only quota exists,
    // so the produce must complete with throttle_time_ms == 0.
    let non_matching_resp =
        await_authorized_produce(addr, alice_password.as_bytes(), "other").await;

    let part = &non_matching_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce (alice, other) must succeed; error_code={}",
        part.error_code
    );
    assert!(
        non_matching_resp.throttle_time_ms == 0,
        "expected throttle_time_ms == 0 for (alice, other) — no tuple or user-only \
         quota is set for this client_id; got {}",
        non_matching_resp.throttle_time_ms
    );

    handle.shutdown().await;
}
