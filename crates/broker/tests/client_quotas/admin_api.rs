//! Tests for the client-quota admin surface: an `AlterClientQuotas` value
//! becomes visible in the metadata image and in `DescribeClientQuotas`, and a
//! principal that is neither a super-user nor ACL-authorized is refused.

use std::time::{Duration, Instant};

use assert2::assert;

use super::{
    cluster::{seed_compat_shim_disable_acl, start_single_broker_sasl_plaintext_with_users},
    quota_admin::{drive_alter_client_quotas_sasl, drive_describe_client_quotas_sasl},
};
use crate::{
    CLIENT_ID,
    cluster::wait_alice_quota,
    kafka_wire::{
        PlainAuthenticationSetup,
        quotas::{QuotaAlterSetup, QuotaDescribeSetup, UserQuotaSetup, alter_user_quota},
    },
};

/// Test 1: `AlterClientQuotas` sets `(user=alice) producer_byte_rate=1024`.
///
/// The value then appears in the metadata image and in
/// `DescribeClientQuotas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_then_describe_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Alter: set producer_byte_rate=1024 for (user=alice).
    let alter_resp = alter_user_quota(
        addr,
        UserQuotaSetup {
            authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
            ..Default::default()
        },
    )
    .await;
    assert!(alter_resp.len() == 1, "one entry in response");
    assert!(
        alter_resp[0].1 == 0,
        "alter should succeed; error_code={}",
        alter_resp[0].1
    );

    // Await until the quota is visible in the committed metadata image.
    wait_alice_quota(&handle, "producer_byte_rate", 1024.0).await;

    // Describe: fetch back the quota.
    let desc = drive_describe_client_quotas_sasl(
        addr,
        vec![("user".into(), 2 /* ANY */, None)],
        &QuotaDescribeSetup {
            authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
            ..Default::default()
        },
    )
    .await;

    let pbr = desc
        .iter()
        .find(|(entity, _)| {
            entity
                .iter()
                .any(|(t, n)| t == "user" && n.as_deref() == Some("alice"))
        })
        .and_then(|(_, values)| {
            values
                .iter()
                .find(|(k, _)| k == "producer_byte_rate")
                .map(|(_, v)| *v)
        });
    assert!(
        pbr == Some(1024.0),
        "expected producer_byte_rate=1024 from describe; got {desc:?}"
    );

    handle.shutdown().await;
}

/// Test 5: a non-super-user cannot alter client quotas.
///
/// alice is authenticated, is not a super-user, and has no ACLs. alice sends
/// `AlterClientQuotas`. Every entry must carry
/// `CLUSTER_AUTHORIZATION_FAILED (31)`.
///
/// The test seeds a dummy ACL first. The dummy ACL disables the compat shim,
/// which allows every operation while the image holds no ACLs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed dummy ACL to disable compat shim.
    seed_compat_shim_disable_acl(&handle).await;

    // Retry until the shim is provably off: alice should receive 31 on every
    // AlterClientQuotas entry, not 0 (which would mean the shim allowed it).
    let deadline = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_alter_client_quotas_sasl(
            addr,
            vec![(
                vec![("user".into(), Some("alice".into()))],
                vec![("producer_byte_rate".into(), 999.0, false)],
            )],
            &QuotaAlterSetup {
                authentication: PlainAuthenticationSetup {
                    client_id: CLIENT_ID,
                    password: ("alice-secret").as_bytes().to_vec(),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        // CLUSTER_AUTHORIZATION_FAILED = 31.
        if r.iter().all(|(_, ec)| *ec == 31) {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "compat shim still active after 5s; got {r:?}"
        );
        // real-time wait (not a progress poll): retry cadence between network AlterClientQuotas attempts (shim disable), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    assert!(
        resp.iter().all(|(_, ec)| *ec == 31),
        "all entries must carry CLUSTER_AUTHORIZATION_FAILED (31); got {resp:?}"
    );
}
