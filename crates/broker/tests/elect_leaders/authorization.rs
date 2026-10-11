//! The authorization gate on `ElectLeaders`: a principal with credentials but
//! no Cluster Alter ACL is denied per requested partition.
//!
//! This is the only scenario in the suite that runs on a `SASL_PLAINTEXT`
//! listener. The authorizer needs a named principal to deny, and the test also
//! seeds one throw-away ACL so the compatibility shim, which allows everything
//! while `image.acls` is empty, is out of the way.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::Broker;
use krabka_security::SaslMechanism;

use crate::{
    kafka_wire::{PlainAuthenticationSetup, SaslTopicSetup},
    sasl::{create_topic_sasl_plain, drive_elect_leaders_sasl_plain},
    support::{
        acl::{TopicAclSetup, topic_acl_record, use_simple_acl_authorizer},
        admin::ElectionSetup,
        sasl::sasl_temp_config,
        topics::ConfiguredTopicSetup,
    },
    wait::wait_partition_exists,
    wire::CLIENT_ID,
};

/// Single-broker SASL/PLAIN cluster.
///
/// alice authenticates with PLAIN credentials but has **no** ACLs. The test
/// seeds a dummy ACL to disable the compat shim, which allows every operation
/// while `image.acls` is empty. alice then sends `ElectLeaders Preferred` for
/// topic "foo-auth-test" partition 0. Each row must carry
/// `CLUSTER_AUTHORIZATION_FAILED (31)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_without_acl_denied() {
    // Build a single-broker SASL_PLAINTEXT config.
    // admin is the super-user so the compat shim stays off once an ACL
    // exists; alice has credentials but no ACLs.
    let (_log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_string(), "admin-secret".to_string());
    cfg.plain_credentials
        .insert("alice".to_string(), "alice-secret".to_string());
    // The controller listener is PLAINTEXT, so this broker's own heartbeat
    // arrives as ANONYMOUS; like a Kafka inter-broker principal it needs
    // `ClusterAction`, or the broker never unfences and places no replica.
    cfg.super_users = ["admin".to_string(), "ANONYMOUS".to_string()]
        .into_iter()
        .collect();
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; default is `AllowAllAuthorizer`.
    use_simple_acl_authorizer(&mut cfg);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Create the topic as admin (rf=1 fine for a single-broker cluster).
    create_topic_sasl_plain(
        addr,
        SaslTopicSetup {
            client_id: CLIENT_ID,
            topic: ConfiguredTopicSetup {
                name: ("foo-auth-test").into(),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    wait_partition_exists(&handle, "foo-auth-test", 0).await;

    // Seed a dummy ACL so the compat shim is disabled. The ACL itself
    // is irrelevant — any non-empty `image.acls` flips the shim off and
    // forces the authorizer to evaluate every request.
    handle
        .submit_metadata_record_for_test(topic_acl_record(TopicAclSetup {
            topic: "__compat_shim_disable__",
            principal: "User:admin",
            ..Default::default()
        }))
        .await
        .expect("seed dummy ACL");

    // `submit_metadata_record_for_test` blocks until the raft entry is
    // committed and the state machine applies it to the image, so the ACL
    // is guaranteed to be in the image before we proceed. A small extra
    // wait absorbs any race on very slow CI runners.
    // intentional: defensive barrier for ACL visibility to the authorizer;
    // no ACL-image awaiter/metric exists, and the retry loop below is the
    // real convergence gate.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drive ElectLeaders Preferred as alice. Because the compat shim is
    // now off (image.acls is non-empty) and alice has no Cluster Alter
    // ACL, the handler must return CLUSTER_AUTHORIZATION_FAILED (31)
    // for every requested partition.
    //
    // If the shim were still active we'd see error_code=0 (allowed).
    // Retry up to 5s to absorb raft apply latency on slow runners.
    let deadline_auth = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_elect_leaders_sasl_plain(
            addr,
            ElectionSetup {
                topic: ("foo-auth-test").to_string(),
                ..Default::default()
            },
            &PlainAuthenticationSetup {
                client_id: CLIENT_ID,
                password: (b"alice-secret").to_vec(),
                ..Default::default()
            },
        )
        .await;
        // If we see 31, the shim is off and we're done.
        if r.iter().all(|(_, ec)| *ec == 31) {
            break r;
        }
        assert!(
            Instant::now() <= deadline_auth,
            "ACL shim still active or wrong error after 5s; got {r:?}"
        );
        // intentional: backoff between bounded RPC-response retries that
        // re-drive the SASL ElectLeaders wire path to observe the authorizer's
        // decision; the awaited state is on the request path, not in the
        // metadata image, and has no metric.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    // Per-row error code must be 31 (CLUSTER_AUTHORIZATION_FAILED).
    assert!(
        resp == vec![(0, 31)],
        "expected CLUSTER_AUTHORIZATION_FAILED (31) for alice; got {resp:?}"
    );
}
